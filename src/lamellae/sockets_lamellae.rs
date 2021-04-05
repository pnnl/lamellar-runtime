use crate::lamellae::Lamellae;
use crate::lamellar_alloc::{BTreeAlloc, LamellarAlloc};
use crate::lamellar_request::InternalReq;
use crate::runtime::Arch;
use crate::runtime::LAMELLAR_RT;
use crate::runtime::{Cmd, LamellarAny, LamellarClosure, Msg, RetType, SCHEDULER};
use crate::schedulers::Scheduler;
use crate::utils::{parse_localhost, parse_slurm, ser_closure};

use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

const MEM_SIZE: usize = 1 * 1024 * 1024 * 1024;

struct MsgQueues {
    pe_tx: Vec<futures::sync::mpsc::UnboundedSender<std::vec::Vec<u8>>>,
    pe_rx: HashMap<usize, futures::sync::mpsc::UnboundedReceiver<std::vec::Vec<u8>>>,
    tx_q: Vec<crossbeam::channel::Sender<Vec<u8>>>,
}

#[derive(Clone, Debug)]
pub(crate) struct Counters {
    pub(crate) send_req_cnt: Arc<AtomicUsize>,
    pub(crate) recv_req_cnt: Arc<AtomicUsize>,
    return_req_cnt: Arc<AtomicUsize>,
    data_cnt: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct BarrierCnts {
    send: Arc<AtomicUsize>,
    recv: Arc<AtomicUsize>,
    recv2: Arc<AtomicUsize>,
}

pub(crate) struct Runtime {
    arch: Arch,
    port: usize,
    thread_start_id: usize,
    msg_queues: MsgQueues,
    pub(crate) counters: Counters,
    barrier_cnts: BarrierCnts,
    threads: Arc<Mutex<Vec<std::thread::JoinHandle<()>>>>,
    senders: Arc<RwLock<std::collections::HashMap<usize, Arc<Mutex<TcpStream>>>>>,
    receivers: Arc<RwLock<std::collections::HashMap<usize, Arc<Mutex<TcpStream>>>>>,
    global_mem_base_address: Arc<RwLock<usize>>, //need this protected for when we eventually have a (rofi)realloc...
    mem_alloc: BTreeAlloc,
    mem_pool: Box<[u8]>,
    active: Arc<AtomicBool>,
    valid: Arc<AtomicBool>,
}

impl Lamellae for Runtime {
    fn init(&mut self) -> Arch {
        self.arch = match std::env::var("LAMELLAR_LOCAL") {
            Ok(_) => parse_localhost(),
            Err(_) => parse_slurm(),
        };
        self.mem_alloc.init(0, MEM_SIZE);
        *self.global_mem_base_address.write() = self.mem_pool.as_ptr() as usize;
        if self.arch.num_pes > 1 { 
            for i in 0..self.arch.pe_addrs.len() {
                self.arch.pe_addrs[i] += ".ibnet";
            }
        }

        self.create_msg_queues();
        let mut rx_q: Vec<crossbeam::channel::Receiver<Vec<u8>>> = Vec::new();
        for _i in 0..self.arch.num_pes {
            let (t, r) = crossbeam::channel::unbounded();
            rx_q.push(r);
            self.msg_queues.tx_q.push(t);
        }
        let core_ids = core_affinity::get_core_ids().unwrap();
        for tid in 0..2 {
            let rs_qs = rx_q.clone();
            let senders = self.senders.clone();
            let receivers = self.receivers.clone();
            let arch = self.arch.clone();
            let port = self.port;
            let me = self.arch.my_pe;
            let active = self.active.clone();
            // let valid = self.valid.clone();

            let core_id = core_ids[tid + self.thread_start_id].clone();
            let mut threads = self.threads.lock();
            threads.push(std::thread::spawn(move || {
                core_affinity::set_for_current(core_id);
                if tid == 0 {
                    start_server(port, arch.clone(), receivers.clone());
                    recv_thread(port, arch, receivers, active);
                } else if tid % 2 == 0 {
                    while receivers.read().len() < arch.num_pes {
                        std::thread::sleep(std::time::Duration::from_millis(500));
                    }

                    recv_thread(port, arch, receivers, active);
                } else {
                    send_thread(rs_qs, senders, me, active);
                }
            }));
        }

        // println!("starting servers");
        std::thread::sleep(std::time::Duration::from_millis(15000)); //sleep to allow servers on all nodes to start up
        self.connect_to_pes(self.port, self.senders.clone(),self.valid.clone());
        let mut cnt = 0;
        while self.receivers.read().len() < self.arch.num_pes && self.valid.load(Ordering::SeqCst){
            std::thread::yield_now();
            cnt += 1;
            if cnt % 10000000 == 0 {
                println!(
                    "{:?} {:?} {:?} {:?}",
                    self.arch.my_pe,
                    self.receivers.read().len(),
                    self.senders.read().len(),
                    self.arch.num_pes
                )
            }
        }
        if !self.valid.load(Ordering::SeqCst){
            panic!("error setting up connections");
        }
        std::thread::sleep(std::time::Duration::from_millis(5000)); //sleep to allow all other connections to finish connections
        return self.arch.clone();
    }

    fn finit(&self) {
        self.barrier();
        std::thread::sleep(std::time::Duration::from_millis(1000));
        self.active.store(false, Ordering::SeqCst);
        // std::thread::sleep(std::time::Duration::from_millis(1000));
        // for i in 0..1 {
            for i in 0..self.msg_queues.tx_q.len() {
                let _res = self.msg_queues.tx_q[i].send(Vec::<u8>::new());
            }
        // }
        // let mut threads = self.threads.lock();
        // while !threads.is_empty() {
        //     if let Some(thread) = threads.pop() {
        //         if let Ok(_) = thread.join() {
        //             println!("thread joined");
        //         } else {
        //             println!("error joining thread");
        //         }
        //     } else {
        //         println!("no threads left");
        //     }
        // }
    }

    fn barrier(&self) {
        let msg = Msg {
            cmd: Cmd::Barrier,
            src: self.arch.my_pe as u16,
            id: 0,
            return_data: false,
        };
        let func = ser_closure(FnOnce!(
            [] move || {
                LAMELLAR_RT.lamellae.barrier_cnts.recv.fetch_add(1, Ordering::SeqCst);
                (RetType::Closure,Some(ser_closure(
                    FnOnce!([] move || -> (RetType,Option<std::vec::Vec<u8>>){
                    LAMELLAR_RT.lamellae.barrier_cnts.send.fetch_add(1,Ordering::SeqCst);
                    (RetType::Barrier,None)
                }))))
            }
        ));
        let payload = (msg, func);
        self.send_to_all(crate::serialize(&payload)).unwrap();
        while self.barrier_cnts.recv.load(Ordering::SeqCst) < self.arch.num_pes-1 //account for myself
            || self.barrier_cnts.send.load(Ordering::SeqCst) < self.arch.num_pes-1
        //account for myself
        {
            std::thread::yield_now();
        }
        self.barrier_cnts.recv.store(0, Ordering::SeqCst);
        self.barrier_cnts.send.store(0, Ordering::SeqCst);
        let msg = Msg {
            cmd: Cmd::Barrier,
            src: self.arch.my_pe as u16,
            id: 0,
            return_data: false,
        };
        let func = ser_closure(FnOnce!(
            [] move ||  -> (RetType,Option<std::vec::Vec<u8>>){
                LAMELLAR_RT.lamellae.barrier_cnts.recv2.fetch_add(1, Ordering::SeqCst);
                (RetType::Barrier,None)
            }
        ));
        let payload = (msg, func);

        self.send_to_all(crate::serialize(&payload)).unwrap();

        while self.barrier_cnts.recv2.load(Ordering::SeqCst) < self.arch.num_pes - 1 {
            std::thread::yield_now();
        }
        self.barrier_cnts.recv2.store(0, Ordering::SeqCst);
    }

    fn put<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    >(
        &self,
        pe: usize,
        src: &[T],
        dst: usize,
    ) {
        if pe != self.arch.my_pe {
            let dst_addr = dst - *self.global_mem_base_address.read();

            let src_ptr = src.as_ptr() as usize;
            let src_size = src.len();
            let func = Box::new(move || {
                let src = unsafe { std::slice::from_raw_parts(src_ptr as *const T, src_size) };
                let src_data = src.to_vec();
                ser_closure(FnOnce!(
                    [dst_addr,src_data] move || -> (RetType,Option<std::vec::Vec<u8>>) {
                        let dst_addr = LAMELLAR_RT.lamellae.base_addr() + dst_addr;
                        unsafe { std::ptr::copy_nonoverlapping(src_data.as_ptr(), dst_addr as *mut T, src_data.len()); }
                        (RetType::Put,None)
                    }
                ))
            }) as LamellarClosure;
            let my_any: LamellarAny = Box::new(func);
            let ireq = InternalReq {
                data_tx: LAMELLAR_RT.nohandle_ireq.data_tx.clone(),
                cnt: LAMELLAR_RT.nohandle_ireq.cnt.clone(),
                start: LAMELLAR_RT.nohandle_ireq.start.clone(),
                size: 0,
                active: LAMELLAR_RT.nohandle_ireq.active.clone(),
            };
            let msg = Msg {
                cmd: Cmd::PutReq,
                src: self.arch.my_pe as u16,
                id: 0,
                return_data: false,
            };
            SCHEDULER.submit_req(pe, msg, ireq, my_any);
        } else {
            unsafe {
                std::ptr::copy_nonoverlapping(src.as_ptr(), dst as *mut T, src.len());
            }
        }
    }
    fn put_all<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    >(
        &self,
        src: &[T],
        dst: usize,
    ) {
        for i in 0..self.arch.my_pe {
            self.put(i, src, dst);
        }
        for i in self.arch.my_pe + 1..self.arch.num_pes {
            self.put(i, src, dst);
        }
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), dst as *mut T, src.len());
        }
    }

    fn get<T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + 'static >(
        &self,
        pe: usize,
        src: usize,
        dst: &mut [T],
    ) {
        if pe != self.arch.my_pe {
            let size = dst.len();
            let dst_addr = dst.as_ptr() as usize;
            let src_addr = src - *self.global_mem_base_address.read();
            let func = ser_closure(FnOnce!(
                [size,dst_addr,src_addr] move || {
                    let dst_addr = dst_addr ;
                    let src_addr = LAMELLAR_RT.lamellae.base_addr() + src_addr;
                    let src_data = unsafe { std::slice::from_raw_parts(src_addr as *const T,size).to_vec() };
                    (RetType::Closure,Some(ser_closure(
                        FnOnce!([src_data,dst_addr,] move || -> (RetType,Option<std::vec::Vec<u8>>){
                            unsafe {
                                std::ptr::copy_nonoverlapping(src_data.as_ptr(), dst_addr as *mut T, src_data.len());
                            }
                        (RetType::Get,None)
                    }))))
                }
            ));
            let msg = Msg {
                cmd: Cmd::GetReq,
                src: self.arch.my_pe as u16,
                id: 0,
                return_data: false,
            };
            let payload = (msg, func);
            self.send_to_pe(pe, crate::serialize(&payload)).unwrap();
        } else {
            unsafe {
                std::ptr::copy_nonoverlapping(src as *mut T, dst.as_mut_ptr(), dst.len());
            }
        }
    }

    fn alloc(&self, size: usize) -> Option<usize> {
        Some(
            *self.global_mem_base_address.read()
                + self
                    .mem_alloc
                    .try_malloc(size)
                    .expect("lamellar unable to allocate memory"),
        )
        // TODO handle out of memory
    }
    fn free(&self, addr: usize) {
        self.mem_alloc
            .free(addr - *self.global_mem_base_address.read())
    }

    fn base_addr(&self) -> usize {
        *self.global_mem_base_address.read()
    }

    fn send_to_pe(&self, pe: usize, data: std::vec::Vec<u8>) {
        self.counters
            .data_cnt
            .fetch_add(data.len(), Ordering::SeqCst);

        self.msg_queues.tx_q[pe].send(data).expect("error in send");
    }

    fn send_to_all(&self, data: std::vec::Vec<u8>) {
        self.counters
            .data_cnt
            .fetch_add(data.len() * self.arch.num_pes, Ordering::SeqCst);
        for p in 0..self.arch.my_pe {
            self.msg_queues.tx_q[p]
                .send(data.clone())
                .expect("error in send");
        }
        for p in self.arch.my_pe + 1..self.arch.num_pes {
            self.msg_queues.tx_q[p]
                .send(data.clone())
                .expect("error in send");
        }
    }

    fn MB_sent(&self) -> f64 {
        self.counters.data_cnt.load(Ordering::SeqCst) as f64 / 1_000_000.0
    }

    fn print_stats(&self) {
        println!("sockets_lamellae: {:?}", self.counters);
    }
}

impl Runtime {
    pub(crate) fn new(port: usize, thread_start_id: usize) -> Runtime {
        Runtime {
            arch: Arch {
                my_pe: 0,
                num_pes: 0,
                pe_addrs: Vec::new(),
                job_id: 0,
            },
            port: port,
            thread_start_id: thread_start_id,
            msg_queues: MsgQueues {
                pe_tx: Vec::new(),
                pe_rx: HashMap::new(),
                tx_q: Vec::new(),
            },
            counters: Counters {
                send_req_cnt: Arc::new(AtomicUsize::new(0)),
                recv_req_cnt: Arc::new(AtomicUsize::new(0)),
                return_req_cnt: Arc::new(AtomicUsize::new(0)),
                data_cnt: Arc::new(AtomicUsize::new(0)),
            },
            barrier_cnts: BarrierCnts {
                send: Arc::new(AtomicUsize::new(0)),
                recv: Arc::new(AtomicUsize::new(0)),
                recv2: Arc::new(AtomicUsize::new(0)),
            },
            threads: Arc::new(Mutex::new(Vec::new())),
            senders: Arc::new(RwLock::new(std::collections::HashMap::new())),
            receivers: Arc::new(RwLock::new(std::collections::HashMap::new())),
            global_mem_base_address: Arc::new(RwLock::new(0)),
            mem_alloc: BTreeAlloc::new("mem".to_string()),
            mem_pool: vec![0; MEM_SIZE].into_boxed_slice(),
            active: Arc::new(AtomicBool::new(true)),
            valid: Arc::new(AtomicBool::new(true)),
        }
    }

    fn create_msg_queues(&mut self) {
        for pe in 0..self.arch.num_pes {
            let (s, r) = futures::sync::mpsc::unbounded();
            self.msg_queues.pe_tx.push(s);
            self.msg_queues.pe_rx.insert(pe, r);
        }
    }

    fn connect_to_pes(
        &mut self,
        port: usize,
        senders_lock: Arc<RwLock<std::collections::HashMap<usize, Arc<Mutex<TcpStream>>>>>,
        valid: Arc<AtomicBool>,
    ) {
        let mut senders = senders_lock.write();
        for pe in 0..self.arch.num_pes {
            if !senders.contains_key(&pe) {
                let addr_port: String =
                    self.arch.pe_addrs[pe].to_owned() + ":" + &(port + pe).to_string();
                let addr_port_2 = addr_port.clone();
                let addr: Vec<_> = addr_port
                    .to_socket_addrs()
                    .expect("unable to resolve domain")
                    .collect();
                let stream = std::net::TcpStream::connect(&addr[0]);
                match stream {
                    Ok(mut stream) => {
                        let mut buffer = self.arch.my_pe.to_ne_bytes();
                        stream
                            .write_all(&buffer)
                            .expect("error client sending initial handshake");
                        if let Ok(_) = stream.read_exact(&mut buffer)
                        // .expect("error client receiving initial handshake");
                        {
                        } else {
                            println!("error {:?}", addr_port_2);
                            panic!("error client receiving initial handshake");
                        }

                        let r_pe = usize::from_ne_bytes(buffer);
                        if pe == r_pe {
                            senders.entry(pe).or_insert(Arc::new(Mutex::new(stream)));
                        } else {
                            println! {"bad connection?"}
                        }
                    }
                    Err(_) => {
                        println!("connection failed {:?}",&addr[0]);
                        valid.store(false,Ordering::SeqCst);
                    }
                }
            }
        }
    }
}

fn recv_msg(mut stream: &TcpStream) -> Option<Vec<u8>> {
    let mut size_buff = 0usize.to_ne_bytes();
    let size = size_buff.len();
    let mut num = 0;
    while num < size {
        num += match stream.read(&mut size_buff[num..]) {
            Ok(val) => val,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                if num == 0 {
                    return None;
                }
                0
            }
            Err(e) => {
                println!("error in network read {:?}", e);
                0
            }
        }
    }
    let size = usize::from_ne_bytes(size_buff);
    let mut data: Vec<u8> = vec![0; size];
    num = 0;
    while num < size {
        num += match stream.read(&mut data[num..]) {
            Ok(val) => val,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => 0,
            Err(e) => {
                println!("error in network read {:?}", e);
                0
            }
        }
    }
    Some(data)
}

fn recv_thread(
    _port: usize,
    _arch: Arch,
    receivers_lock: Arc<RwLock<std::collections::HashMap<usize, Arc<Mutex<TcpStream>>>>>,
    active: Arc<AtomicBool>,
) {
    let receivers = receivers_lock.read();
    while active.load(Ordering::SeqCst) {
        for (_pe, recv) in receivers.iter() {
            let mut size_buf = 0usize.to_ne_bytes();
            let mut lock = recv.try_lock();
            if let Some(ref mut conn) = lock {
                if let Some(data) = recv_msg(conn) {
                    LAMELLAR_RT
                        .lamellae
                        .counters
                        .recv_req_cnt
                        .fetch_add(1, Ordering::SeqCst);
                    let mut i = 0;
                    while i < data.len() {
                        let (buf, d) = data[i..].split_at(8);
                        for i in 0..8 {
                            size_buf[i] = buf[i];
                        }
                        let size = usize::from_ne_bytes(size_buf);
                        i += 8;
                        SCHEDULER.submit_work(d[..size].to_vec());
                        i += size;
                    }
                }
            }
            drop(lock);
            std::thread::yield_now();
        }
    }
}

fn send_data(data: Vec<u8>, sender: &Arc<Mutex<TcpStream>>) {
    let s: usize = data.len() + 8usize;
    let mut size = s.to_ne_bytes();
    let mut conn = sender.lock();
    conn.write(&size)
        .expect("error sending message + header size");
    size = data.len().to_ne_bytes();
    conn.write(&size).expect("error sending message size");
    conn.write_all(&data).expect("error sending message");
}

fn send_thread(
    rx_q: Vec<crossbeam::channel::Receiver<Vec<u8>>>,
    senders_lock: Arc<RwLock<std::collections::HashMap<usize, Arc<Mutex<TcpStream>>>>>,
    me: usize,
    active: Arc<AtomicBool>,
) {
    let mut senders = senders_lock.read();
    let num_pes = rx_q.len();
    while senders.len() < rx_q.len() {
        drop(senders);
        std::thread::yield_now();
        senders = senders_lock.read();
    }

    let mut agg_buf = std::collections::HashMap::new();
    for i in 0..num_pes {
        agg_buf.insert(i, vec![]);
    }
    let mut agg_buf_size: Vec<usize> = vec![0; num_pes];
    let min_buf_size: usize = 1000;
    let mut active_cnt = rx_q.len();

    let mut sel = crossbeam::Select::new();
    for r in &rx_q {
        sel.recv(r);
    }
    while active.load(Ordering::SeqCst) || active_cnt > 0 {
        let oper = sel.select();
        let dst = oper.index();
        let res = oper.recv(&rx_q[dst]);

        if let Ok(data) = res {
            if data.len() > 0 {
                LAMELLAR_RT
                    .lamellae
                    .counters
                    .send_req_cnt
                    .fetch_add(1, Ordering::SeqCst);

                if agg_buf_size[dst] == 0 && data.len() > min_buf_size {
                    send_data(data, &senders[&dst]);
                } else {
                    agg_buf_size[dst] += data.len();
                    if let Some(buf) = agg_buf.get_mut(&dst) {
                        buf.push(data);
                    } else {
                        println!("ERROR dst {:?} buf not found!", dst);
                    }

                    if agg_buf_size[dst] > min_buf_size || rx_q[dst].len() == 0 {
                        let msg = Msg {
                            cmd: Cmd::AggregationReq,
                            src: me as u16,
                            id: 0,
                            return_data: false,
                        };
                        let agg_buf_tmp = agg_buf.remove(&dst).unwrap();
                        let func = ser_closure(FnMut!(
                            [agg_buf_tmp] move || -> (RetType,Option<std::vec::Vec<u8>>){
                                while let Some(data) = agg_buf_tmp.pop(){
                                    SCHEDULER.submit_work(data);
                                }
                                (RetType::Unit,None)
                            }
                        ));
                        let payload = (msg, func);
                        send_data(crate::serialize(&payload), &senders[&dst]).unwrap();
                        agg_buf.insert(dst, vec![]);
                        agg_buf_size[dst] = 0;
                    }
                }
            } else {
                active_cnt -= 1;
                println!("{:?} activecnt {:?}",me,active_cnt);
                active_cnt=0;
            }
        }
    }
}

fn start_server(
    port: usize,
    arch: Arch,
    receivers_lock: Arc<RwLock<std::collections::HashMap<usize, Arc<Mutex<TcpStream>>>>>,
) {
    let mut receivers = receivers_lock.write();
    let num_pes = arch.num_pes;
    let mut addr_port: String =
        arch.pe_addrs[arch.my_pe].to_owned() + ":" + &(port + arch.my_pe).to_string();

    println!("my_pe {:?} addr_port: {:?}", arch.my_pe, addr_port);

    let mut addr: Vec<_> = addr_port
        .to_socket_addrs()
        .expect("unable to resolve domain")
        .collect();

    //create local copy of work queue sender
    let mut cnt = 0;
    let mut list_try = std::net::TcpListener::bind(&addr[0]);
    while let Err(_err) = list_try {
        cnt += 1;
        if cnt > 1000 {
            cnt = 0;
        }
        addr_port = arch.pe_addrs[arch.my_pe].to_owned()
            + ":"
            + &(port + arch.my_pe + cnt * num_pes).to_string();
        addr = addr_port
            .to_socket_addrs()
            .expect("unable to resolve domain")
            .collect();
        list_try = std::net::TcpListener::bind(&addr[0]);
    }
    let listener = list_try.expect(&format!("error on tcp bind {:?}", &addr[0]));

    // Pull out a stream of sockets for incoming connections

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut buffer = [0; 8];
                stream
                    .read_exact(&mut buffer)
                    .expect("error server receiving initial handshake");
                let pe = usize::from_ne_bytes(buffer);
                buffer = arch.my_pe.to_ne_bytes();
                stream
                    .write_all(&buffer)
                    .expect("error server sending handshake");
                stream
                    .set_nonblocking(true)
                    .expect("set_nonblocking call failed");
                receivers.entry(pe).or_insert(Arc::new(Mutex::new(stream)));
                if receivers.len() >= arch.num_pes {
                    break;
                }
            }
            Err(_e) => { /* connection failed */ }
        }
    }
}

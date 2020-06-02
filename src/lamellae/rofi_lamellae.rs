use crate::lamellae::Lamellae;
use crate::lamellar_alloc::{BTreeAlloc, LamellarAlloc, ObjAlloc};
use crate::rofi_api::*;
use crate::runtime::Arch;
use crate::runtime::{Cmd, Msg, RetType, LAMELLAR_RT, SCHEDULER};
use crate::schedulers::Scheduler;
use crate::utils::ser_closure;

use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
// use std::time::Instant;

const CMD_Q_LEN: usize = 10000;
const NUM_REQ_SLOTS: usize = 100000;
const DATA_SIZE: usize = 100_000_000;
const MEM_SIZE: usize = 10 * 1024 * 1024 * 1024;
const FINI_STATUS: RofiReqStatus = RofiReqStatus::Fini;

lazy_static! {
    static ref DATA_COUNTS: Vec<AtomicUsize> = {
        let mut temp = Vec::new();
        for _i in 0..NUM_REQ_SLOTS {
            temp.push(AtomicUsize::new(0));
        }
        temp
    };
}

#[repr(u8)]
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq)]
pub(crate) enum RofiReqStatus {
    None,
    Init,
    Tx,
    Fini,
}

impl RofiReqStatus {
    fn as_bytes(&self) -> &[u8] {
        let pointer = self as *const RofiReqStatus as *const u8;
        let size = std::mem::size_of::<RofiReqStatus>();
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(pointer, size) };
        slice
    }
}

#[repr(C, align(1))]
#[derive(Clone, Copy, Debug)]
struct RofiCmd {
    daddr: usize, // std::ffi::c_void,
    dsize: usize,
    data_hash: usize,
    cmd_hash: usize,
    status_idx: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct RofiReqInfo {
    cmd: RofiCmd,
    cmd_idx: usize,
    data_cnt_idx: usize,
    dst: usize,
    id: usize,
}
unsafe impl Send for RofiReqInfo {}

#[derive(Clone)]
struct RofiReqBuffer<T: LamellarAlloc> {
    alloc: T,
    cmd_allocs: Vec<T>,
    req_info: Arc<Mutex<Box<[RofiReqInfo]>>>,
    rofi_base_addr: Arc<RwLock<usize>>,
    status_base_addr: usize,
    cmd_base_addr: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct Counters {
    pub(crate) send_req_cnt: Arc<AtomicUsize>,
    pub(crate) recv_req_cnt: Arc<AtomicUsize>,
    rofi_send: Vec<Arc<AtomicUsize>>,
    rofi_recv: Vec<Arc<AtomicUsize>>,
    return_req_cnt: Vec<Arc<AtomicUsize>>,
    data_cnt: Arc<AtomicUsize>,
}

pub(crate) struct Runtime {
    arch: Arch,
    pub(crate) counters: Counters,
    threads: Arc<Mutex<Vec<std::thread::JoinHandle<()>>>>,
    mem_alloc: BTreeAlloc, //manages full memory space allocated from rofi - contains both runtime and user structures
    data_allocs: BTreeAlloc, // manages memory space reserved for am buffers (in rofi memory)
    req_buffer: RofiReqBuffer<ObjAlloc>, // am status slots (in rofi memory)
    data_counts_alloc: ObjAlloc, // multi message counters (in lamellar memory)
    tx_q: Vec<crossbeam::channel::Sender<Vec<u8>>>,
    rofi_base_address: Arc<RwLock<usize>>, //need this protected for when we eventually have a (rofi)realloc...
    active: Arc<AtomicBool>,
    // put_mutex: Arc<Mutex<bool>>,
}

impl RofiCmd {
    fn new(daddr: usize, dsize: usize, data_hash: usize, status_idx: u32) -> RofiCmd {
        let mut cmd = RofiCmd {
            daddr: daddr,
            dsize: dsize,
            data_hash: data_hash,
            cmd_hash: 0,
            status_idx: status_idx,
        };
        cmd.cmd_hash = cmd.calc_cmd_hash();
        cmd
    }
    fn as_bytes(&self) -> &[u8] {
        let pointer = self as *const RofiCmd as *const u8;
        let size = std::mem::size_of::<RofiCmd>();
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(pointer, size) };
        slice
    }

    fn update(&mut self, daddr: usize, dsize: usize, data_hash: usize, status_idx: u32) {
        self.daddr = daddr;
        self.dsize = dsize;
        self.data_hash = data_hash;
        self.status_idx = status_idx;
        self.cmd_hash = self.calc_cmd_hash();
    }
    fn clear(&mut self) {
        self.daddr = 0;
        self.dsize = 0;
        self.data_hash = 0;
        self.cmd_hash = 0;
        self.status_idx = 0;
    }
    fn calc_cmd_hash(&self) -> usize {
        self.daddr as usize
            + self.dsize as usize
            + self.data_hash as usize
            + self.status_idx as usize
    }
}

impl RofiReqInfo {
    fn new(id: usize) -> RofiReqInfo {
        RofiReqInfo {
            cmd: RofiCmd::new(0, 0, 0, 0),
            cmd_idx: 0, //remote index
            data_cnt_idx: 0,
            dst: 0,
            id: id,
        }
    }
    fn update(&mut self, cmd: RofiCmd, cmd_idx: usize, data_cnt_idx: usize, dst: usize, id: usize) {
        self.cmd
            .update(cmd.daddr, cmd.dsize, cmd.data_hash, cmd.status_idx);
        self.cmd_idx = cmd_idx;
        self.data_cnt_idx = data_cnt_idx;
        self.dst = dst;
        self.id = id;
    }
    fn clear(&mut self) {
        self.cmd.clear();
        self.cmd_idx = 0;
        self.data_cnt_idx = 0;
        self.dst = 0;
        self.id = 0;
    }
}

impl<T: LamellarAlloc> RofiReqBuffer<T> {
    fn new(rofi_base_addr: Arc<RwLock<usize>>) -> RofiReqBuffer<T> {
        RofiReqBuffer {
            alloc: T::new("req_buffer".to_string()), //location of req_info locally...
            cmd_allocs: vec![],                      //location in remote nodes...
            req_info: Arc::new(Mutex::new(
                // vec![RofiReqInfo::new(0); NUM_REQ_SLOTS].into_boxed_slice(),
                Box::new([RofiReqInfo::new(0); NUM_REQ_SLOTS]),
            )),
            rofi_base_addr: rofi_base_addr,
            status_base_addr: 0,
            cmd_base_addr: 0,
        }
    }

    fn init(
        &mut self,
        rofi_alloc: &mut impl LamellarAlloc,
        my_pe: usize,
        num_pes: usize,
        num_entries: usize,
    ) {
        self.status_base_addr =
            rofi_alloc.malloc(num_entries * std::mem::size_of::<RofiReqStatus>());
        self.cmd_base_addr = rofi_alloc.malloc(CMD_Q_LEN * num_pes * std::mem::size_of::<RofiCmd>());
        self.alloc.init(0, num_entries);
        if num_pes > 1 {
            let num_cmds_slots_per_pe = CMD_Q_LEN; // / (num_pes - 1);
            for i in 0..num_pes {
                // !!! note we create an empty allocation for my_pe
                // we short circuit local requests so the allocation for my_pe is never used
                // this implies we need to do some
                let start_idx = if i < my_pe {
                    (my_pe - 1) * num_cmds_slots_per_pe
                } else {
                    my_pe * num_cmds_slots_per_pe
                };

                let mut alloc = T::new("cmd".to_owned() + &i.to_string());
                if i != my_pe {
                    alloc.init(start_idx, num_cmds_slots_per_pe);
                } else {
                    alloc.init(0, 0);
                }
                self.cmd_allocs.push(alloc);
            }
        }
    }

    fn add_request(
        &mut self,
        daddr: usize,
        dsize: usize,
        dhash: usize,
        data_cnt_idx: usize,
        dst: usize,
        id: usize,
        status: RofiReqStatus,
    ) -> (usize, *const RofiCmd, usize) {
        let idx = self.alloc.malloc(1); //location to store request locally
        let cmd_idx = self //location to write into remotely
            .cmd_allocs
            .get_mut(dst)
            .expect("cmd allocs dst out of bounds")
            .malloc(1);
        let mut req_i = self.req_info.lock();
        let cmd = RofiCmd::new(daddr, dsize, dhash, idx as u32);
        req_i[idx].update(cmd, cmd_idx, data_cnt_idx, dst, id);
        let cmd_ptr = &(req_i[idx].cmd) as *const RofiCmd;
        drop(req_i);
        unsafe {
            *((*self.rofi_base_addr.read() + self.status_base_addr + idx) as *mut RofiReqStatus) =
                status
        };
        (self.get_cmd_addr(cmd_idx), cmd_ptr, cmd_idx)
    }

    fn get_status(&self, idx: usize) -> RofiReqStatus {
        unsafe { *(self.get_status_addr(idx) as *const RofiReqStatus) }
    }

    fn get_status_addr(&self, idx: usize) -> usize {
        *self.rofi_base_addr.read()
            + self.status_base_addr
            + idx * std::mem::size_of::<RofiReqStatus>()
    }

    fn get_cmd(&self, idx: usize) -> RofiCmd {
        unsafe { *(self.get_cmd_addr(idx) as *const RofiCmd) }
    }

    fn clear_cmd(&mut self, idx: usize) {
        unsafe { &(*(self.get_cmd_addr(idx) as *mut RofiCmd)).clear() };
    }

    fn get_cmd_addr(&self, idx: usize) -> usize {
        *self.rofi_base_addr.read() + self.cmd_base_addr + idx * std::mem::size_of::<RofiCmd>()
    }

    fn free_request(&mut self, idx: usize) -> RofiReqInfo {
        let mut req_info = self.req_info.lock();
        let req = req_info
            .get(idx)
            .expect("req info entry out of bounds")
            .clone();
        req_info[idx].clear(); //the local request
        drop(req_info);
        assert_eq!(idx, req.cmd.status_idx as usize);
        self.cmd_allocs
            .get_mut(req.dst)
            .expect("cmd allocs dst out of bounds")
            .free(req.cmd_idx as usize); //free my slot on the remote node
        unsafe { *(self.get_status_addr(idx) as *mut RofiReqStatus) = RofiReqStatus::None };
        self.alloc.free(idx); //free my local slot
        req
    }
}

impl Lamellae for Runtime {
    fn init(&mut self) -> Arch {
        // let period = time::Duration::from_millis(5000);
        rofi_init().expect("unable to initialze rofi");
        rofi_barrier();
        self.arch.num_pes = rofi_get_size();
        self.arch.my_pe = rofi_get_id();
        unsafe {
            let mut base_ptr: *mut usize = std::ptr::null_mut();
            let base_ptr_ptr = &mut base_ptr as *mut _;
            if rofisys::rofi_alloc(MEM_SIZE, 0x0, base_ptr_ptr as *mut *mut std::ffi::c_void) != 0 {
                panic!("unable to allocate memory region");
            }
            *self.rofi_base_address.write() = base_ptr as usize;
            // println!(
            //     "num_pes {} my_pe {} base_addr{:p} {:x}",
            //     self.arch.num_pes,
            //     self.arch.my_pe,
            //     base_ptr,
            //     *self.rofi_base_address.read()
            // );
        }

        //initialize (rofi) memory and set up lamellae tx buffers and cmd q's
        self.mem_alloc.init(0, MEM_SIZE);
        self.data_allocs
            .init(self.mem_alloc.malloc(DATA_SIZE), DATA_SIZE);
        self.req_buffer.init(
            &mut self.mem_alloc,
            self.arch.my_pe,
            self.arch.num_pes,
            NUM_REQ_SLOTS,
        );
        self.data_counts_alloc.init(0, NUM_REQ_SLOTS);
        let mut rx_q: Vec<crossbeam::channel::Receiver<Vec<u8>>> = Vec::new();
        for _i in 0..self.arch.num_pes {
            self.counters.rofi_recv.push(Arc::new(AtomicUsize::new(0)));
            self.counters.rofi_send.push(Arc::new(AtomicUsize::new(0)));
            self.counters
                .return_req_cnt
                .push(Arc::new(AtomicUsize::new(0)));
            // note we still create a channel for my_pe... we will use it to handle send_to_all requests
            let (t, r) = crossbeam::channel::unbounded();
            rx_q.push(r);
            self.tx_q.push(t);
        }

        let rx_qs = rx_q.clone();
        let my_pe = self.arch.my_pe;
        let mut data_allocs = self.data_allocs.clone();
        let mut req_buffer = self.req_buffer.clone();
        let mut data_counts_alloc = self.data_counts_alloc.clone();
        let mut rofi_base_addr = self.rofi_base_address.clone();
        let mut active = self.active.clone();
        let core_ids = core_affinity::get_core_ids().unwrap();
        let mut core_id = core_ids[0].clone();
        let mut threads = self.threads.lock();
        let mut counters = self.counters.clone();
        // let mut put_mutex = self.put_mutex.clone();
        if self.arch.num_pes > 1 {
            threads.push(std::thread::spawn(move || {
                core_affinity::set_for_current(core_id);
                recv_thread(
                    active,
                    my_pe,
                    data_allocs,
                    req_buffer,
                    data_counts_alloc,
                    rofi_base_addr,
                    counters,
                    // put_mutex,
                );
            }));
            data_allocs = self.data_allocs.clone();
            req_buffer = self.req_buffer.clone();
            data_counts_alloc = self.data_counts_alloc.clone();
            rofi_base_addr = self.rofi_base_address.clone();
            active = self.active.clone();
            core_id = core_ids[1].clone();
            counters = self.counters.clone();
            // put_mutex = self.put_mutex.clone();
            threads.push(std::thread::spawn(move || {
                core_affinity::set_for_current(core_id);
                send_thread(
                    active,
                    rx_qs,
                    data_allocs,
                    req_buffer,
                    data_counts_alloc,
                    rofi_base_addr,
                    my_pe,
                    counters,
                    // put_mutex,
                );
            }));
        }
        self.barrier();
        // std::thread::sleep(period);
        return self.arch.clone();
    }

    fn finit(&self) {
        self.barrier();
        std::thread::sleep(std::time::Duration::from_millis(1000));
        self.active.store(false, Ordering::SeqCst);
        if self.arch.num_pes > 1 {
            for pe in 0..self.tx_q.len() {
                let _res = self.tx_q[pe].send(Vec::<u8>::new());
            }
            self.barrier();
            let mut threads = self.threads.lock();

            while !threads.is_empty() {
                if let Some(thread) = threads.pop() {
                    if let Ok(_) = thread.join() {
                        // println!("thread joined");
                    } else {
                        println!("error joining thread");
                    }
                } else {
                    // println!("no threads left");
                }
            }
            // println!("all threads joined, waiting for other ranks");
            self.barrier();
        }
        rofi_finit().expect("error shutting down rofi");
        // println!("pe: {} {:?}", self.arch.my_pe, self.counters);
        // println!("finit finished");
    }

    fn barrier(&self) {
        rofi_barrier();
    }

    fn send_to_pe(&self, pe: usize, data: std::vec::Vec<u8>) {
        self.counters
            .data_cnt
            .fetch_add(data.len(), Ordering::SeqCst);
        self.tx_q[pe].send(data).expect("error in send");
    }

    fn send_to_all(&self, data: std::vec::Vec<u8>) {
        self.counters
            .data_cnt
            .fetch_add(data.len() * self.arch.num_pes - 1, Ordering::SeqCst);

        // since we will short circuit local requests we can use the my_pe channel as the one for send_to_all requests
        self.tx_q[self.arch.my_pe]
            .send(data)
            .expect("error in send")
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
            unsafe { rofi_put(src, dst, pe).expect("error in rofi put") };
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

    fn get<T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + 'static>(
        &self,
        pe: usize,
        src: usize,
        dst: &mut [T],
    ) {
        if pe != self.arch.my_pe {
            unsafe { 
                let ret = rofi_get(src, dst, pe);//.expect("error in rofi get") 
                if let Err(_) = ret {
                    println!("Error in get from {:?} src {:x} base_addr{:x} size{:x}",pe,src,*self.rofi_base_address.read(),dst.len());
                    panic!();
                }
            };
        } else {
            unsafe {
                std::ptr::copy_nonoverlapping(src as *const T, dst.as_mut_ptr(), dst.len());
            }
        }
    }

    fn alloc(&self, size: usize) -> Option<usize> {
        Some(
            *self.rofi_base_address.read()
                + self
                    .mem_alloc
                    .try_malloc(size)
                    .expect("unable to allocate memory"),
        )
        // TODO handle out of memory
    }
    fn free(&self, addr: usize) {
        self.mem_alloc.free(addr - *self.rofi_base_address.read())
    }
    fn base_addr(&self) -> usize {
        *self.rofi_base_address.read()
    }

    fn MB_sent(&self) -> f64 {
        self.counters.data_cnt.load(Ordering::SeqCst) as f64 / 1_000_000.0 as f64
    }

    fn print_stats(&self) {
        println!("rofi_lamellae: {:?}", self.counters);
    }
}

impl Runtime {
    pub(crate) fn new() -> Runtime {
        let base_addr = Arc::new(RwLock::new(0));
        Runtime {
            arch: Arch {
                my_pe: 0,
                num_pes: 0,
                pe_addrs: Vec::new(),
                job_id: 0,
            },
            counters: Counters {
                send_req_cnt: Arc::new(AtomicUsize::new(0)),
                recv_req_cnt: Arc::new(AtomicUsize::new(0)),
                rofi_send: Vec::new(),
                rofi_recv: Vec::new(),
                return_req_cnt: Vec::new(),
                data_cnt: Arc::new(AtomicUsize::new(0)),
            },
            threads: Arc::new(Mutex::new(Vec::new())),
            mem_alloc: BTreeAlloc::new("mem".to_string()),
            data_allocs: BTreeAlloc::new("data".to_string()),
            req_buffer: RofiReqBuffer::<ObjAlloc>::new(base_addr.clone()),
            data_counts_alloc: ObjAlloc::new("data_counts".to_string()),
            tx_q: Vec::new(),
            rofi_base_address: base_addr,
            active: Arc::new(AtomicBool::new(true)),
            // put_mutex: Arc::new(Mutex::new(false)),
        }
    }
}

fn send_data(
    data: Vec<u8>,
    data_allocs: &mut impl LamellarAlloc,
    req_buffer: &mut RofiReqBuffer<impl LamellarAlloc>,
    data_counts_alloc: &mut impl LamellarAlloc,
    rofi_base_addr: &Arc<RwLock<usize>>,
    _me: usize,
    dsts: &[usize],
    id: usize,
    counters: &mut Counters,
    _timers: &mut (f64, f64, f64, f64),
    // put_mutex: &Arc<Mutex<bool>>,
) -> Vec<usize> {
    // allocate memory in our local data segment
    // let mut timer = Instant::now();
    let daddr = data_allocs.malloc(data.len());
    // timers.0 += timer.elapsed().as_secs_f64();
    unsafe {
        std::ptr::copy_nonoverlapping(
            data.as_ptr(),
            (*rofi_base_addr.read() + daddr) as *mut u8,
            data.len(),
        );
    }
    //-----------------

    // allocate a data_count object
    // timer = Instant::now();
    let data_cnt_idx = data_counts_alloc.malloc(1);
    DATA_COUNTS[data_cnt_idx].fetch_add(dsts.len(), Ordering::SeqCst);
    // timers.1 += timer.elapsed().as_secs_f64();
    //----------------------
    let dhash = data.iter().fold(0usize, |a, b| a + (*b as usize));
    let dsize = data.len();
    let mut status_indices = vec![];
    for dst in dsts {
        // timer = Instant::now();
        let (dst_cmd_addr, cmd_msg_ptr, _cmd_idx) = req_buffer.add_request(
            daddr,
            dsize,
            dhash,
            data_cnt_idx,
            *dst,
            id,
            RofiReqStatus::Init,
        );
        counters.rofi_send[*dst].fetch_add(1, Ordering::SeqCst);
        // timers.2 += timer.elapsed().as_secs_f64();
        // timer = Instant::now();

        unsafe {
            status_indices.push((*cmd_msg_ptr).status_idx as usize);
        }
        let src_cmd_slice = unsafe { (*cmd_msg_ptr).as_bytes() };
        // let lock = put_mutex.lock();
        unsafe { rofi_put(src_cmd_slice, dst_cmd_addr, *dst).expect("error in rofi put") };
        // drop(lock);
        // timers.3 += timer.elapsed().as_secs_f64();
    }
    status_indices
}

fn send_thread(
    active: Arc<AtomicBool>,
    rx_q: Vec<crossbeam::channel::Receiver<Vec<u8>>>,
    mut data_allocs: impl LamellarAlloc,
    mut req_buffer: RofiReqBuffer<impl LamellarAlloc>,
    mut data_counts_alloc: impl LamellarAlloc,
    rofi_base_addr: Arc<RwLock<usize>>,
    me: usize,
    mut counters: Counters,
    // put_mutex: Arc<Mutex<bool>>,
) {
    let mut sel = crossbeam::Select::new();
    for r in &rx_q {
        sel.recv(r);
    }
    let mut timers = (0f64, 0f64, 0f64, 0f64);
    let num_pes = LAMELLAR_RT.arch.num_pes;
    let mut remote_pes = (0..me).collect::<Vec<usize>>();
    remote_pes.extend((me + 1)..num_pes);

    let mut agg_buf = std::collections::HashMap::new();
    for i in 0..(num_pes + 1) {
        agg_buf.insert(i, vec![]);
    }
    let mut agg_buf_size: Vec<usize> = vec![0; num_pes + 1];
    let min_buf_size: usize = 1000;
    let mut active_cnt = rx_q.len();

    while active.load(Ordering::SeqCst) || active_cnt > 0 {
        let oper = sel.select();
        let dst = oper.index();
        let res = oper.recv(&rx_q[dst]);
        let dsts: Vec<usize> = if dst != me {
            let mut v = Vec::<usize>::new();
            v.push(dst);
            v
        } else {
            remote_pes.clone()
        };
        if let Ok(data) = res {
            if data.len() > 0 && data.len() < DATA_SIZE {
                LAMELLAR_RT
                    .lamellae
                    .counters
                    .send_req_cnt
                    .fetch_add(1, Ordering::SeqCst);

                if agg_buf_size[dst] == 0 && data.len() > min_buf_size {
                    send_data(
                        data,
                        &mut data_allocs,
                        &mut req_buffer,
                        &mut data_counts_alloc,
                        &rofi_base_addr,
                        me,
                        &dsts,
                        0,
                        &mut counters,
                        &mut timers,
                        // &put_mutex,
                    );
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
                        send_data(
                            bincode::serialize(&payload).unwrap(),
                            &mut data_allocs,
                            &mut req_buffer,
                            &mut data_counts_alloc,
                            &rofi_base_addr,
                            me,
                            &dsts,
                            0,
                            &mut counters,
                            &mut timers,
                            // &put_mutex,
                        );

                        agg_buf.insert(dst, vec![]);
                        agg_buf_size[dst] = 0;
                    }
                }
            } else if data.len() >= DATA_SIZE {
                panic!(
                    "ERROR: Can not currently send active messages over {:?} bytes long",
                    DATA_SIZE
                );
            } else {
                active_cnt -= 1;
            }
        }
    }
    // println!("pe: {} send thread timers: {:?}", me, timers);
}

fn recv_thread(
    active: Arc<AtomicBool>,
    my_pe: usize,
    data_allocs: impl LamellarAlloc,
    mut req_buffer: RofiReqBuffer<impl LamellarAlloc>,
    data_counts_alloc: impl LamellarAlloc,
    rofi_base_addr: Arc<RwLock<usize>>,
    counters: Counters,
    // put_mutex: Arc<Mutex<bool>>,
) {
    let _timers = (0f64, 0f64, 0f64, 0f64, 0f64, 0f64, 0f64);
    let mut temp_data_map = std::collections::HashMap::new();
    let num_cmds_slots_per_pe = CMD_Q_LEN; // / (LAMELLAR_RT.arch.num_pes - 1);
    while active.load(Ordering::SeqCst) {
        for entry in 0..CMD_Q_LEN * LAMELLAR_RT.arch.num_pes {
            // let mut timer = Instant::now();
            let cmd = req_buffer.get_cmd(entry);
            // timers.0 += timer.elapsed().as_secs_f64();
            if cmd.dsize != 0 && cmd.cmd_hash == cmd.calc_cmd_hash() {
                let mut src = entry / num_cmds_slots_per_pe;
                //account for not allocating cmd slots for my_pe
                if src >= my_pe {
                    src += 1;
                }
                if !temp_data_map.contains_key(&entry) {
                    //we havent yet processed this request
                    counters.rofi_recv[src].fetch_add(1, Ordering::SeqCst);
                    let mut data = vec![0_u8; cmd.dsize];
                    // timer = Instant::now();
                    let data_buf = data.as_mut_slice();
                    // let lock = put_mutex.lock();
                    // unsafe {
                        rofi_iget(*rofi_base_addr.read() + cmd.daddr, data_buf, src)
                            .expect("error in getting rofi_lamellar get");
                    // };
                    let (data_ptr, data_len, data_cap) = data.into_raw_parts();
                    // drop(lock);
                    
                    // timers.1 += timer.elapsed().as_secs_f64();
                    temp_data_map.insert(entry, (data_ptr, data_len, data_cap));
                } else {
                    // hopefully  our get request has finished...
                    if let Some(data_parts) = temp_data_map.remove(&entry) {
                        let data = unsafe {
                            Vec::from_raw_parts(data_parts.0, data_parts.1, data_parts.2)
                        };
                        if cmd.data_hash == data.iter().fold(0usize, |a, b| a + (*b as usize)) {
                            req_buffer.clear_cmd(entry);
                            
                            // let lock = put_mutex.lock();
                            unsafe {
                                rofi_put(
                                    FINI_STATUS.as_bytes(),
                                    req_buffer.get_status_addr(cmd.status_idx as usize),
                                    src,
                                )
                                .expect("rofi error in status put");
                            }
                            SCHEDULER.submit_work(data);
                        // drop(lock);
                        // timers.2 += timer.elapsed().as_secs_f64();
                        } else {
                            let (data_ptr, data_len, data_cap) = data.into_raw_parts();
                            temp_data_map.insert(entry, (data_ptr, data_len, data_cap));
                        }
                    } else {
                        println!("ERROR data buf not found for entry: {:?}", entry);
                    }
                }
            }
        }

        for entry in 0..NUM_REQ_SLOTS {
            // let mut timer = Instant::now();
            let status = req_buffer.get_status(entry);
            // timers.3 += timer.elapsed().as_secs_f64();
            if status == RofiReqStatus::Fini {
                // timer = Instant::now();
                let req_info = req_buffer.free_request(entry);
                // timers.4 += timer.elapsed().as_secs_f64();

                let current_count =
                    DATA_COUNTS[req_info.data_cnt_idx].fetch_sub(1, Ordering::SeqCst);
                counters.return_req_cnt[req_info.dst].fetch_add(1, Ordering::SeqCst);
                if current_count == 1 {
                    //we can't free the data allocation of idx allocation until all requests accessing this data have completed
                    // timer = Instant::now();
                    data_allocs.free(req_info.cmd.daddr);
                    // timers.5 += timer.elapsed().as_secs_f64();
                    // timer = Instant::now();
                    data_counts_alloc.free(req_info.data_cnt_idx);
                    // timers.6 += timer.elapsed().as_secs_f64();
                }
            }
        }
    }
    // println!("pe: {} recv thread timers: {:?}", my_pe, timers);
}

use crate::lamellae::{AllocationType, Backend, Lamellae, LamellaeAM, LamellaeRDMA};
use crate::lamellar_alloc::{BTreeAlloc, LamellarAlloc};
use crate::lamellar_arch::LamellarArchRT;
use crate::schedulers::SchedulerQueue;
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use log::trace;
// use parking_lot::Mutex;
// use raw_sync::locks;
// use raw_sync::locks::*;
use shared_memory::*;
// use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
// use std::time::Duration;
use std::env;

use crossbeam::utils::{Backoff, CachePadded};

const MEM_SIZE: usize = 10 * 1024 * 1024 * 1024;
const MAGIC: u8 = 0b10100101;
//race condition between the tail being advanced and the data being written.
struct CircleBuffer {
    data: *mut u8,
    len: *const usize,
    head: *mut usize,
    tail: *mut CachePadded<AtomicUsize>,
    pop_lock: *mut CachePadded<AtomicUsize>,
}

unsafe impl Sync for CircleBuffer {}
unsafe impl Send for CircleBuffer {}

impl CircleBuffer {
    fn new(mem: *mut u8, len: usize) -> CircleBuffer {
        //  println!("Creating CircleBuffer {:?}",mem);
        let buff = unsafe {
            let temp = mem.add(std::mem::size_of::<u8>() * len) as *mut usize;
            *temp = len;
            let buff = CircleBuffer {
                data: mem, //std::slice::from_raw_parts_mut(mem,len),
                len: mem.add(std::mem::size_of::<u8>() * len) as *const usize,
                head: mem.add(std::mem::size_of::<u8>() * len + std::mem::size_of::<usize>())
                    as *mut usize,
                tail: mem.add(std::mem::size_of::<u8>() * len + 2 * std::mem::size_of::<usize>())
                    as *mut CachePadded<AtomicUsize>,
                pop_lock: mem.add(
                    std::mem::size_of::<u8>() * len
                        + 2 * std::mem::size_of::<usize>()
                        + std::mem::size_of::<CachePadded<AtomicUsize>>(),
                ) as *mut CachePadded<AtomicUsize>,
            };
            *buff.head = 0;
            (*buff.tail).store(0, Ordering::SeqCst);
            (*buff.pop_lock).store(0, Ordering::SeqCst);
            buff
        };
        // unsafe {
        //     // let s = std::slice::from_raw_parts(buff.data as *const u8, *buff.len);
        //     // println!("before{:?} ",s);
        //     // println!("{:?}  {:?} {:?} {:?} {:?} {:?} {:?}",buff.data,len,*buff.len,*buff.head,*buff.tail,(*buff.push_lock).load(Ordering::SeqCst),(*buff.pop_lock).load(Ordering::SeqCst));
        // }
        buff
    }
    fn attach(mem: *mut u8, len: usize) -> CircleBuffer {
        // println!("attaching CircleBuffer");
        let buff = unsafe {
            CircleBuffer {
                data: mem, //std::slice::from_raw_parts_mut(mem,len),
                len: mem.add(std::mem::size_of::<u8>() * len) as *const usize,
                head: mem.add(std::mem::size_of::<u8>() * len + std::mem::size_of::<usize>())
                    as *mut usize,
                tail: mem.add(std::mem::size_of::<u8>() * len + 2 * std::mem::size_of::<usize>())
                    as *mut CachePadded<AtomicUsize>,
                pop_lock: mem.add(
                    std::mem::size_of::<u8>() * len
                        + 2 * std::mem::size_of::<usize>()
                        + std::mem::size_of::<CachePadded<AtomicUsize>>(),
                ) as *mut CachePadded<AtomicUsize>,
            }
        };
        unsafe {
            while *buff.len == 0 {
                std::thread::yield_now()
            }
        }
        //     let s = std::slice::from_raw_parts(buff.data as *const u8, *buff.len);
        //     // println!("before{:?} ",s);
        //     // println!("{:?}  {:?} {:?} {:?} {:?} {:?} {:?}",buff.data,len,*buff.len,*buff.head,*buff.tail,(*buff.push_lock).load(Ordering::SeqCst),(*buff.pop_lock).load(Ordering::SeqCst));
        // }
        buff
    }
    fn size_of(len: usize) -> usize {
        std::mem::size_of::<u8>() * len
            + 2 * std::mem::size_of::<usize>()
            + 2 * std::mem::size_of::<CachePadded<AtomicUsize>>()
    }
    unsafe fn enqueue(&self, mut tail: usize, data: *const u8, len: usize) {
        let backoff = Backoff::new();
        // let orig_tail = tail;
        let i_tail = tail % *self.len;
        if i_tail + len < *self.len {
            while (tail + len) - *self.head > *self.len {
                backoff.snooze();
            } // change this to async_std...
            std::ptr::copy_nonoverlapping(data, self.data.add(i_tail), len);
        } else {
            // println!("going to wrap! {:?} {:?} {:?} {:?}",*self.head,orig_tail,orig_tail+len,*self.len);

            let buf_remaining = *self.len - i_tail;
            while (tail + buf_remaining) - *self.head > *self.len {
                backoff.snooze();
            } //change this to async_std...
            std::ptr::copy_nonoverlapping(data, self.data.add(i_tail), buf_remaining);
            tail += buf_remaining;
            let data_remaining = len - buf_remaining;
            while (tail + data_remaining) - *self.head > *self.len {
                backoff.snooze();
            }
            // println!("wrap! {:?} {:?} {:?} {:?}",*self.head,orig_tail,tail+data_remaining,*self.len);
            std::ptr::copy_nonoverlapping(data.add(buf_remaining), self.data, data_remaining);
        }
    }
    fn push(&self, data: &[u8]) {
        unsafe {
            let backoff = Backoff::new();
            let data_len = data.len();
            let len_bytes = &data_len as *const _ as *const u8;
            let total_len = std::mem::size_of::<u8>() + std::mem::size_of::<usize>() + data_len;
            let mut old_tail = (*self.tail).load(Ordering::Relaxed);
            loop {
                let new_tail = old_tail + total_len;
                match (*self.tail).compare_exchange(
                    old_tail,
                    new_tail,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(x) => old_tail = x,
                }
                backoff.spin();
            }
            self.enqueue(
                old_tail + std::mem::size_of::<u8>(),
                len_bytes,
                std::mem::size_of::<usize>(),
            );
            self.enqueue(
                old_tail + std::mem::size_of::<u8>() + std::mem::size_of::<usize>(),
                data.as_ptr(),
                data_len,
            );
            self.enqueue(old_tail, &[MAGIC] as *const u8, std::mem::size_of::<u8>())
            // let s = std::slice::from_raw_parts(self.data as *const u8, *self.len);
            // println!("push after{:?} ",s);
            // println!(
            //     "pushed {:?} {:?} {:?} {:?} {:?} {:?} {:?} {:?}",
            //     self.data,
            //     *self.len,
            //     *self.head,
            //     *self.tail,
            //     *self.wrap,
            //     (*self.push_lock).load(Ordering::SeqCst),
            //     (*self.pop_lock).load(Ordering::SeqCst),
            //     (*self.wrap_cnt).load(Ordering::SeqCst)
            // );
        }
    }

    unsafe fn dequeue(&self, len: usize) -> Vec<u8> {
        let i_head = *self.head % *self.len;
        if i_head + len < *self.len {
            let data = std::slice::from_raw_parts(self.data.add(i_head), len);
            let data_vec = data.to_vec();
            let zeros = vec![0u8; len];
            std::ptr::copy_nonoverlapping(zeros.as_ptr() as *const u8, self.data.add(i_head), len);
            *self.head += len;

            data_vec
        } else {
            let buf_remaining = *self.len - i_head;
            let data = std::slice::from_raw_parts(self.data.add(i_head), buf_remaining);
            let mut data_vec = data.to_vec();
            let data_remaining = len - buf_remaining;
            data_vec.extend_from_slice(std::slice::from_raw_parts(self.data, data_remaining));
            *self.head += len;
            data_vec
        }
    }

    fn pop(&self) -> Option<Vec<u8>> {
        unsafe {
            let backoff = Backoff::new();
            // println!("popping {:?} {:?} {:?} {:?} {:?} {:?} {:?}",self.data,*self.len,*self.head,*self.tail,*self.wrap, (*self.push_lock).load(Ordering::SeqCst),(*self.pop_lock).load(Ordering::SeqCst));
            while (*self.pop_lock)
                .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                backoff.spin();
            }
            // let s = std::slice::from_raw_parts(self.data as *const u8, *self.len);
            let res = if (*self.tail).load(Ordering::Relaxed)
                > *self.head + std::mem::size_of::<usize>() + std::mem::size_of::<u8>()
            {
                let i_head = *self.head % *self.len;
                if *self.data.add(i_head) == MAGIC {
                    *self.data.add(i_head) = 0;
                    *self.head += 1;
                    //there is a message present
                    let data_len = *(self.dequeue(std::mem::size_of::<usize>()).as_ptr() as *const _
                        as *const usize);
                    // println!("data_len {:?} {:?} {:?} {:?} {:?} {:?} {:?} {:?}",data_len,self.data,*self.len,*self.head,*self.tail,*self.wrap, (*self.push_lock).load(Ordering::SeqCst),(*self.pop_lock).load(Ordering::SeqCst));
                    Some(self.dequeue(data_len))
                } else {
                    None
                }
            } else {
                None
            };
            (*self.pop_lock).store(0, Ordering::SeqCst);
            // let s = std::slice::from_raw_parts(self.data as *const u8, *self.len);
            // println!("pop after{:?} ",s);
            // println!(
            //     "popped {:?} {:?} {:?} {:?} {:?} {:?} {:?} {:?}",
            //     self.data,
            //     *self.len,
            //     *self.head,
            //     *self.tail,
            //     *self.wrap,
            //     (*self.push_lock).load(Ordering::SeqCst),
            //     (*self.pop_lock).load(Ordering::SeqCst),
            //     (*self.wrap_cnt).load(Ordering::SeqCst)
            // );
            res
        }
    }
    fn clear(&self) {
        unsafe {
            let zeros = vec![0u8; *self.len];
            std::ptr::copy_nonoverlapping(zeros.as_ptr() as *const u8, self.data, *self.len);
            *self.head = 0;
            (*self.tail).store(0, Ordering::SeqCst);
            (*self.pop_lock).store(0, Ordering::SeqCst);
        }
    }
}

struct MyShmem {
    data: *mut u8,
    _shmem: Shmem,
}

impl MyShmem {
    fn as_ptr(&self) -> *mut u8 {
        self.data
    }

    // fn is_owner(&self) -> bool {
    //     self.shmem.is_owner()
    // }
}

fn attach_to_shmem(size: usize, id: &str, header: usize, create: bool) -> MyShmem {
    let size = size + std::mem::size_of::<usize>();
    let shmem_id = "lamellar_".to_owned() + &(size.to_string() + "_" + id);
    // let m = if create {
    let m = match ShmemConf::new().size(size).os_id(shmem_id.clone()).create() {
        Ok(m) => {
            println!("created {:?}", shmem_id);

            if create {
                let zeros = vec![0u8; size];
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        zeros.as_ptr() as *const u8,
                        m.as_ptr() as *mut u8,
                        size,
                    );
                    *(m.as_ptr() as *mut _ as *mut usize) = header;
                }
            }
            m
        }
        Err(ShmemError::LinkExists) | Err(ShmemError::MappingIdExists) => {
            match ShmemConf::new().os_id(shmem_id.clone()).open() {
                Ok(m) => {
                    println!("attached {:?}", shmem_id);
                    if create {
                        let zeros = vec![0u8; size];
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                zeros.as_ptr() as *const u8,
                                m.as_ptr() as *mut u8,
                                size,
                            );
                            *(m.as_ptr() as *mut _ as *mut usize) = header;
                        }
                        unsafe {
                            println!(
                                "updated {:?} {:?}",
                                shmem_id,
                                *(m.as_ptr() as *const _ as *const usize)
                            );
                        }
                    }
                    m
                }
                Err(r) => panic!("unable to attach to shared memory {:?} {:?}", shmem_id, r),
            }
        }
        Err(e) => panic!("unable to create shared memory {:?} {:?}", shmem_id, e),
    };
    while (unsafe { *(m.as_ptr() as *const _ as *const usize) } != header) {
        std::thread::yield_now()
    }
    unsafe {
        println!(
            "shmem inited {:?} {:?}",
            shmem_id,
            *(m.as_ptr() as *const _ as *const usize)
        );
    }

    unsafe {
        MyShmem {
            data: m.as_ptr().add(std::mem::size_of::<usize>()),
            _shmem: m,
        }
    }
}

struct ShmemBuffer {
    buff: CircleBuffer,
    _shmem: MyShmem,
}

unsafe impl Sync for ShmemBuffer {}
unsafe impl Send for ShmemBuffer {}

impl ShmemBuffer {
    fn new(size: usize, id: &str, header: usize, create: bool) -> ShmemBuffer {
        // println!(
        //     "Creating ShmemBuffer {:?} {:?} {:?}",
        //     hostname::get().unwrap(),
        //     size,
        //     id
        // );

        let buff_size = CircleBuffer::size_of(size);
        let shmem = attach_to_shmem(buff_size, id, header, create);
        let buff = //unsafe {
            if create {
                let base_ptr = shmem.as_ptr();
                let buff = CircleBuffer::new(base_ptr, size);
                buff
            } else {
                let base_ptr = shmem.as_ptr();
                let buff = CircleBuffer::attach(base_ptr, size);
                buff
            };
        // };
        ShmemBuffer {
            buff: buff,
            _shmem: shmem,
        }
    }
    fn push(&self, data: &[u8]) {
        self.buff.push(data);
    }

    fn pop(&self) -> Option<Vec<u8>> {
        self.buff.pop()
    }
    fn clear(&self) {
        self.buff.clear()
    }

    // async fn a_push(&mut self, data: &[u8]) {
    //     println!("outer mypush");
    //     // let buff_guard = self.mutex.lock().unwrap();
    //     println!("after lock");
    //     self.buff.a_push(data).await;
    // }

    // async fn a_pop(&mut self) -> Option<Vec<u8>> {
    //     // let buff_guard = self.mutex.lock().unwrap();
    //     self.buff.a_pop().await
    // }
}

impl std::fmt::Debug for ShmemBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shmembuff")
    }
}

struct ShmemBarrier {
    my_pe: usize,
    num_pes: usize,
    cnt: AtomicUsize,
    buff: *mut usize, //could we do  a slice from raw parts here?
    _shmem: MyShmem,
}

unsafe impl Sync for ShmemBarrier {}
unsafe impl Send for ShmemBarrier {}

impl ShmemBarrier {
    fn new(my_pe: usize, num_pes: usize, header: usize, create: bool) -> ShmemBarrier {
        let shmem = attach_to_shmem(
            num_pes * std::mem::size_of::<usize>(),
            "barrier",
            header,
            create,
        );
        let barrier = ShmemBarrier {
            my_pe: my_pe,
            num_pes: num_pes,
            cnt: AtomicUsize::new(1),
            buff: shmem.as_ptr() as *mut _ as *mut usize,
            _shmem: shmem,
        };
        unsafe {
            *barrier.buff = 0;
        }
        barrier
    }
    fn exec(&self) {
        let backoff = Backoff::new();
        unsafe {
            for pe in 0..self.num_pes {
                while *(self.buff.add(pe)) < self.cnt.load(Ordering::SeqCst) - 1 {
                    backoff.spin();
                }
            }
            *(self.buff.add(self.my_pe)) = self.cnt.load(Ordering::SeqCst);
            for pe in 0..self.num_pes {
                while *(self.buff.add(pe)) < self.cnt.load(Ordering::SeqCst) {
                    backoff.spin();
                }
            }
        }
        // let cnt = self.cnt.fetch_add(1,Ordering::SeqCst);
        // println!("[{:?}] shmem barrier cnt {:?}",self.my_pe,cnt);
    }
}

impl std::fmt::Debug for ShmemBarrier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cnt: {:?} ", self.cnt)
    }
}

struct IOThread {
    thread: Option<std::thread::JoinHandle<()>>,
    am: Arc<ShmemLamellaeAM>,
    buffer_meta: (usize, String),
    shmem_header: usize,
    active: Arc<AtomicBool>,
}

impl IOThread {
    fn run(&mut self, scheduler: Arc<dyn SchedulerQueue>) {
        let am = self.am.clone();
        let buffer_size = self.buffer_meta.0;
        let buffer_id = self.buffer_meta.1.clone();
        let header = self.shmem_header;
        let active = self.active.clone();
        self.thread = Some(std::thread::spawn(move || {
            let buffer = ShmemBuffer::new(buffer_size, &buffer_id, header, false);
            let backoff = Backoff::new();
            active.store(true, Ordering::SeqCst);
            while active.load(Ordering::SeqCst) {
                if let Some(data) = buffer.pop() {
                    // println!("got something! {:?}",data.len());
                    scheduler.submit_work(data.to_vec(), am.clone());
                } else {
                    backoff.snooze();
                    // std::thread::yield_now();
                }
            }
        }));
    }
    fn shutdown(&mut self) {
        trace!("[{:}] shutting down io thread", self.am.my_pe);
        self.thread
            .take()
            .expect("error joining io thread")
            .join()
            .expect("error joinging io thread");
        trace!("[{:}] shut down io thread", self.am.my_pe);
    }
}

pub(crate) struct ShmemLamellae {
    am: Arc<ShmemLamellaeAM>,
    rdma: Arc<ShmemLamellaeRDMA>,
    threads: Vec<IOThread>,
    my_pe: usize,
    num_pes: usize,
    active: Arc<AtomicBool>,
}

//#[prof]
impl ShmemLamellae {
    pub(crate) fn new() -> ShmemLamellae {
        let num_locales = match env::var("SLURM_NNODES") {
            Ok(val) => val.parse::<usize>().unwrap(),
            Err(_e) => {
                println!("[LAMELLAR] WARNING: currently only supports slurm envrionments, falling back to single node");
                1
            }
        };
        let num_global_pes = match env::var("PMI_SIZE") {
            Ok(val) => val.parse::<usize>().unwrap(),
            Err(_e) => {
                println!("[LAMELLAR] WARNING: currently only supports slurm envrionments, falling back to single node");
                1
            }
        };
        let num_pes = num_global_pes / num_locales;

        let my_pe = match env::var("SLURM_LOCALID") {
            Ok(val) => val.parse::<usize>().unwrap(),
            Err(_e) => {
                println!("[LAMELLAR] WARNING: currently only supports slurm envrionments, falling back to single node");
                0
            }
        };

        let job_id = match env::var("SLURM_JOBID") {
            Ok(val) => val.parse::<usize>().unwrap(),
            Err(_e) => {
                println!("[LAMELLAR] WARNING: currently only supports slurm envrionments, falling back to single node");
                0
            }
        };

        // println!("{:?} {:?} {:?} {:?}",num_locales,num_global_pes,num_pes,my_pe);
        let am = Arc::new(ShmemLamellaeAM::new(
            my_pe,
            num_pes,
            100000000,
            job_id,
            my_pe == 0,
        ));

        let rdma = ShmemLamellaeRDMA::new(my_pe, num_pes, job_id, my_pe == 0);
        //

        let active = Arc::new(AtomicBool::new(false));

        let mut lam = ShmemLamellae {
            am: am.clone(),
            rdma: Arc::new(rdma),
            threads: Vec::new(),
            my_pe: my_pe,
            num_pes: num_pes,
            active: active.clone(),
        };
        let thread = IOThread {
            thread: None,
            am: am.clone(),
            buffer_meta: (100000000, my_pe.to_string() + "_buffer"),
            shmem_header: job_id,
            active: active.clone(),
        };
        lam.threads.push(thread);

        // let temp = std::process::id();
        // let data = crate::serialize(&temp).unwrap();
        // lam.buffer.push(&data);
        // std::thread::sleep(Duration::from_secs(10));
        // println!("popped! {:?}", lam.buffer.pop());
        // std::thread::sleep(Duration::from_secs(10));
        lam.am.barrier();
        println!("created ShmemLamellae");
        lam
    }
}

//#[prof]
impl Lamellae for ShmemLamellae {
    fn init_fabric(&mut self) -> (usize, usize) {
        (self.num_pes, self.my_pe)
    }
    fn init_lamellae(&mut self, scheduler: Arc<dyn SchedulerQueue>) {
        for thread in &mut self.threads {
            thread.run(scheduler.clone());
        }
        while !self.active.load(Ordering::SeqCst) {
            //make this a usize if more than one thread...
            std::thread::yield_now();
        }
        self.am.barrier();
        println!("done init");
    }
    fn finit(&self) {}
    fn barrier(&self) {
        self.am.barrier();
    }
    fn backend(&self) -> Backend {
        Backend::Local
    }
    fn get_am(&self) -> Arc<dyn LamellaeAM> {
        self.am.clone()
    }
    fn get_rdma(&self) -> Arc<dyn LamellaeRDMA> {
        self.rdma.clone()
    }
    #[allow(non_snake_case)]
    fn MB_sent(&self) -> f64 {
        0.0f64
    }
    fn print_stats(&self) {}
}

#[derive(Debug)]
pub(crate) struct ShmemLamellaeAM {
    buffers: Vec<ShmemBuffer>,
    barrier: ShmemBarrier,
    my_pe: usize,
}

impl ShmemLamellaeAM {
    fn new(
        my_pe: usize,
        num_pes: usize,
        buf_size: usize,
        header: usize,
        create: bool,
    ) -> ShmemLamellaeAM {
        let mut buffers = Vec::new();
        for pe in 0..num_pes {
            buffers.push(ShmemBuffer::new(
                buf_size,
                &(pe.to_string() + "_buffer"),
                header,
                create,
            ));
        }
        buffers[my_pe].clear();
        ShmemLamellaeAM {
            buffers: buffers,
            barrier: ShmemBarrier::new(my_pe, num_pes, header, create),
            my_pe: my_pe,
        }
    }
}

//#[prof]
impl LamellaeAM for ShmemLamellaeAM {
    fn send_to_pe(&self, pe: usize, data: std::vec::Vec<u8>) {
        // println!("{:?} sending to {:?}",self.my_pe, pe);
        self.buffers[pe].push(&data);
    }
    fn send_to_all(&self, _data: std::vec::Vec<u8>) {}
    fn send_to_pes(&self, pe: Option<usize>, team: Arc<LamellarArchRT>, data: std::vec::Vec<u8>) {
        if let Some(pe) = pe {
            // println!("{:?} sending to {:?} {:?}",self.my_pe, pe, data.len());
            self.buffers[pe].push(&data);
        } else {
            for dst in team.team_iter() {
                if dst != self.my_pe {
                    // println!("{:?} sending to {:?}",self.my_pe, dst);
                    self.buffers[dst].push(&data);
                }
            }
        }
    }
    fn barrier(&self) {
        self.barrier.exec();
    }
    fn backend(&self) -> Backend {
        Backend::Local
    }
}

pub(crate) struct ShmemLamellaeRDMA {
    alloc: BTreeAlloc,
    my_base_addr: usize,
    global_base_addr: usize,
    seg_size: usize,
    my_pe: usize,
    num_pes: usize,
    _shmem: MyShmem,
}

unsafe impl Sync for ShmemLamellaeRDMA {}
unsafe impl Send for ShmemLamellaeRDMA {}

impl ShmemLamellaeRDMA {
    fn new(my_pe: usize, num_pes: usize, header: usize, create: bool) -> ShmemLamellaeRDMA {
        let shmem = attach_to_shmem(MEM_SIZE, "rdma", header, create);
        let global_base = shmem.as_ptr() as usize;
        let seg_size = MEM_SIZE / num_pes;
        let my_base = global_base + seg_size * my_pe;
        // println!("gba: {:?} mba: {:?} ss: {:?}",global_base,my_base,seg_size);
        let mut rdma = ShmemLamellaeRDMA {
            alloc: BTreeAlloc::new("shmem_mem".to_string()),
            my_base_addr: my_base,
            global_base_addr: global_base,
            seg_size: seg_size,
            my_pe: my_pe,
            num_pes: num_pes,
            _shmem: shmem,
        };
        rdma.alloc.init(0, seg_size);
        rdma
    }
}

//#[prof]
impl LamellaeRDMA for ShmemLamellaeRDMA {
    fn put(&self, pe: usize, src_addr: &[u8], dst: usize) {
        let offset = dst - self.my_base_addr;
        let dst_addr = offset + self.global_base_addr + self.seg_size * pe;

        println!(
            "[{:?}] put: {:?} {:?} {:?} {:?} {:?} len {:?}",
            self.my_pe,
            offset,
            self.seg_size * pe + offset,
            dst_addr,
            dst,
            self.my_base_addr,
            src_addr.len()
        );
        unsafe {
            // println!("[{:?}] memcopy {:?}",pe,src_addr.as_ptr());
            std::ptr::copy_nonoverlapping(src_addr.as_ptr(), dst_addr as *mut u8, src_addr.len());
        }
    }
    fn iput(&self, pe: usize, src_addr: &[u8], dst: usize) {
        let offset = dst - self.my_base_addr;
        let dst_addr = offset + self.global_base_addr + self.seg_size * pe;
        println!(
            "[{:?}] put: {:?} {:?} {:?} {:?} {:?} len {:?}",
            self.my_pe,
            offset,
            self.seg_size * pe + offset,
            dst_addr,
            dst,
            self.my_base_addr,
            src_addr.len()
        );
        unsafe {
            // println!("[{:?}] memcopy {:?}",pe,src_addr.as_ptr());
            std::ptr::copy_nonoverlapping(src_addr.as_ptr(), dst_addr as *mut u8, src_addr.len());
        }
    }
    fn put_all(&self, src_addr: &[u8], dst: usize) {
        let offset = dst - self.my_base_addr;

        for pe in 0..self.num_pes {
            let dst_addr = offset + self.global_base_addr + self.seg_size * pe;
            unsafe {
                // println!("[{:?}] memcopy {:?}",pe,src_addr.as_ptr());
                std::ptr::copy_nonoverlapping(
                    src_addr.as_ptr(),
                    dst_addr as *mut u8,
                    src_addr.len(),
                );
            }
        }
    }
    fn get(&self, pe: usize, src: usize, dst_addr: &mut [u8]) {
        let offset = src - self.my_base_addr;
        let src_addr = offset + self.global_base_addr + self.seg_size * pe;

        unsafe {
            // println!("[{:?}] memcopy {:?}",pe,src_addr.as_ptr());
            std::ptr::copy_nonoverlapping(
                src_addr as *mut u8,
                dst_addr.as_mut_ptr(),
                dst_addr.len(),
            );
        }
    }
    fn rt_alloc(&self, size: usize) -> Option<usize> {
        if let Some(addr) = self.alloc.try_malloc(size) {
            println!("[{:?}] addr: {:?} size: {:?}", self.my_pe, addr, size);
            Some(addr)
        } else {
            None
        }
    }
    fn rt_free(&self, addr: usize) {
        self.alloc.free(addr)
    }
    fn alloc(&self, size: usize, _alloc: AllocationType) -> Option<usize> {
        if let Some(addr) = self.alloc.try_malloc(size) {
            println!("[{:?}] addr: {:?} size: {:?}", self.my_pe, addr, size);
            Some(addr)
        } else {
            None
        }
    }
    fn free(&self, addr: usize) {
        self.alloc.free(addr)
    }
    fn base_addr(&self) -> usize {
        self.my_base_addr
    }
    fn local_addr(&self, _pe: usize, _remote_addr: usize) -> usize {
        _remote_addr
    }
    fn remote_addr(&self, _pe: usize, _local_addr: usize) -> usize {
        _local_addr
    }
    fn mype(&self) -> usize {
        self.my_pe
    }
}

//#[prof]
impl Drop for ShmemLamellae {
    fn drop(&mut self) {
        self.active.store(false, Ordering::SeqCst);
        while let Some(mut iothread) = self.threads.pop() {
            iothread.shutdown();
        }
        trace!("[{:?}] RofiLamellae Dropping", 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use std::time::Duration;
    #[test]
    fn circle_buf_push_pop() {
        let data_len = 1000;
        let mem_len = CircleBuffer::size_of(data_len);
        let mem = (vec![0u8; mem_len]).into_boxed_slice();

        let mem_ptr = Box::into_raw(mem) as *mut u8; //mem.as_mut_ptr() as *mut u8;

        let buff = CircleBuffer::new(mem_ptr, data_len);
        for i in 0..10 {
            let data = crate::serialize(&(i as usize)).unwrap();
            println!("{:?}", data);
            buff.push(&data);
        }
        for i in 0..10 {
            let ser = buff.pop().expect("error in popping");
            println!("{:?}", ser);
            let data: usize = crate::deserialize(&ser).unwrap();
            println!("{:?} {:?}", data, i);
            assert_eq!(data, i);
        }
    }

    #[test]
    fn circle_buf_wrap() {
        let data_len = 16;
        let mem_len = CircleBuffer::size_of(data_len);
        let mem = (vec![0u8; mem_len]).into_boxed_slice();

        let mem_ptr = Box::into_raw(mem) as *mut u8; //mem.as_mut_ptr() as *mut u8;

        let buff = CircleBuffer::new(mem_ptr, data_len);
        let t1 = std::thread::spawn(move || {
            let data = crate::serialize(&(1 as u32)).unwrap();
            buff.push(&data);
            buff.push(&data);
        });
        std::thread::sleep(Duration::from_secs(1));
        let buff = CircleBuffer::attach(mem_ptr, data_len);
        let t2 = std::thread::spawn(move || {
            let ser = buff.pop().expect("error in popping");
            // println!("ser: {:?}", ser);
            let data: u32 = crate::deserialize(&ser).unwrap();
            println!("data: {:?}", data);
            assert_eq!(data, 1 as u32);
            std::thread::sleep(Duration::from_secs(1));
            let ser = buff.pop().expect("error in popping");
            // println!("ser: {:?}", ser);

            let data: u32 = crate::deserialize(&ser).unwrap();
            println!("data: {:?}", data);
            // assert_eq!(data, 1 as u32);
        });
        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[test]
    fn circle_buf_stress() {
        //Test against crossbeam
        let data_len = 1000000;
        let mem_len = CircleBuffer::size_of(data_len);
        let mem = (vec![0u8; mem_len]).into_boxed_slice();

        let mem_ptr = Box::into_raw(mem) as *mut u8; //mem.as_mut_ptr() as *mut u8;
        let mut threads = Vec::new();
        let buff = CircleBuffer::new(mem_ptr, data_len);
        let start = std::time::Instant::now();
        //
        let t = std::thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let vec_data = (0..255).map(|x| x).collect::<Vec<u8>>();

            for _i in 0..100000 {
                let j = rng.gen_range(0, 256);
                let data = crate::serialize(&vec_data[0..j]).unwrap();
                buff.push(&data);
            }
        });
        threads.push(t);
        for _i in 0..4 {
            let buff = CircleBuffer::attach(mem_ptr, data_len);
            let t = std::thread::spawn(move || {
                let mut rng = rand::thread_rng();
                let vec_data = (0..255).map(|x| x).collect::<Vec<u8>>();
                for _i in 0..100000 {
                    let j = rng.gen_range(0, 256);
                    let data = crate::serialize(&vec_data[0..j]).unwrap();
                    buff.push(&data);
                }
            });
            threads.push(t);
        }
        for _i in 0..5 {
            let buff = CircleBuffer::attach(mem_ptr, data_len);
            let t = std::thread::spawn(move || {
                let mut i = 0;
                while i < 100000 {
                    if let Some(ser) = buff.pop() {
                        let data: Vec<u8> = crate::deserialize(&ser).unwrap();
                        if data.len() > 0 {
                            let max = data.len() - 1;
                            assert_eq!(
                                data.iter().map(|x| *x as usize).sum::<usize>(),
                                ((max as f64 / 2.0) * ((max + 1) as f64)) as usize,
                                "max = {:?} i={:?}",
                                max,
                                i
                            );
                        }
                        i += 1;
                    }
                }
            });
            threads.push(t);
        }

        for t in threads {
            t.join().unwrap();
        }
        let time = start.elapsed().as_secs_f64();

        println!("time: {:?}", time);
    }

    #[test]
    fn circle_buf_stress_xbeam_ch() {
        //Test against crossbeam
        let data_len = 1000000;
        let (s, r) = crossbeam::channel::bounded(data_len);
        let start = std::time::Instant::now();
        let s_clone = s.clone();

        let mut threads = Vec::new();
        let t = std::thread::spawn(move || {
            for _i in 0..300000 {
                // let data = crate::serialize(&(1 as u32)).unwrap();
                s_clone.send(1 as u32).unwrap();
            }
        });
        threads.push(t);
        for _i in 0..4 {
            let s_clone = s.clone();
            let t = std::thread::spawn(move || {
                for _i in 0..300000 {
                    // let data = crate::serialize(&(1 as u32)).unwrap();
                    s_clone.send(1 as u32).unwrap();
                }
            });
            threads.push(t);
        }
        for _i in 0..5 {
            let r_clone = r.clone();
            let t = std::thread::spawn(move || {
                let mut i = 0;
                while i < 300000 {
                    if let Ok(ser) = r_clone.recv() {
                        // let data: u32 = crate::deserialize(&ser).unwrap();
                        assert_eq!(ser, 1 as u32);
                        i += 1;
                    }
                }
            });
            threads.push(t);
        }

        for t in threads {
            t.join().unwrap();
        }
        let time = start.elapsed().as_secs_f64();

        println!("time: {:?}", time);
    }

    #[test]
    fn circle_buf_stress_xbeam_q() {
        //Test against crossbeam
        let data_len = 1000000;
        let q = Arc::new(crossbeam::queue::ArrayQueue::new(data_len));
        let start = std::time::Instant::now();
        let q_clone = q.clone();

        let mut threads = Vec::new();
        let t = std::thread::spawn(move || {
            for _i in 0..300000 {
                // let data = crate::serialize(&(1 as u32)).unwrap();
                while q_clone.push(1 as u32).is_err() {
                    std::thread::yield_now()
                }
            }
        });
        threads.push(t);
        for _i in 0..4 {
            let q_clone = q.clone();
            let t = std::thread::spawn(move || {
                for _i in 0..300000 {
                    while q_clone.push(1 as u32).is_err() {
                        std::thread::yield_now()
                    }
                }
            });
            threads.push(t);
        }
        for _i in 0..5 {
            let q_clone = q.clone();
            let t = std::thread::spawn(move || {
                let mut i = 0;
                while i < 300000 {
                    if let Ok(data) = q_clone.pop() {
                        assert_eq!(data, 1 as u32);
                        i += 1;
                    }
                }
            });
            threads.push(t);
        }

        for t in threads {
            t.join().unwrap();
        }
        let time = start.elapsed().as_secs_f64();

        println!("time: {:?}", time);
    }
    #[test]
    fn shmem_buffer_mutex() {
        // let t1 = std::thread::spawn(|| {
        //     let shm = ShmemBuffer::new(10);
        //     let buff_guard = shm.mutex.lock().unwrap();
        //     println!("grabbing lock");
        //     std::thread::sleep(Duration::from_secs(10));
        //     println!("dropping lock");
        //     drop(buff_guard);
        // });
        // let t2 = std::thread::spawn(|| {
        //     let shm = ShmemBuffer::new(10);
        //     let buff_guard = shm.mutex.lock().unwrap();
        //     println!("grabbing lock");
        //     std::thread::sleep(Duration::from_secs(10));
        //     println!("dropping lock");
        //     drop(buff_guard);
        // });
        // t1.join().unwrap();
        // t2.join().unwrap();
    }

    #[test]
    fn shmem_buffer_stress() {
        //Test against crossbeam
        let data_len = 1000000;
        let mut threads = Vec::new();
        let buff = ShmemBuffer::new(data_len, "test", 9999, true);
        let start = std::time::Instant::now();
        let t = std::thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let vec_data = (0..255).map(|x| x).collect::<Vec<u8>>();
            for _i in 0..100000 {
                let j = rng.gen_range(0, 256);
                let data = crate::serialize(&vec_data[0..j]).unwrap();
                buff.push(&data);
            }
        });
        threads.push(t);
        for _i in 0..4 {
            let buff = ShmemBuffer::new(data_len, "test", 9999, false);
            let t = std::thread::spawn(move || {
                let mut rng = rand::thread_rng();
                let vec_data = (0..255).map(|x| x).collect::<Vec<u8>>();
                for _i in 0..100000 {
                    let j = rng.gen_range(0, 256);
                    let data = crate::serialize(&vec_data[0..j]).unwrap();
                    buff.push(&data);
                }
            });
            threads.push(t);
        }
        for _i in 0..5 {
            let buff = ShmemBuffer::new(data_len, "test", 9999, false);
            let t = std::thread::spawn(move || {
                let mut i = 0;
                while i < 100000 {
                    if let Some(ser) = buff.pop() {
                        let data: Vec<u8> = crate::deserialize(&ser).unwrap();
                        if data.len() > 0 {
                            let max = data.len() - 1;
                            assert_eq!(
                                data.iter().map(|x| *x as usize).sum::<usize>(),
                                ((max as f64 / 2.0) * ((max + 1) as f64)) as usize,
                                "max = {:?} i={:?}",
                                max,
                                i
                            );
                        }
                        i += 1;
                    }
                }
            });
            threads.push(t);
        }

        for t in threads {
            t.join().unwrap();
        }
        let time = start.elapsed().as_secs_f64();

        println!("time: {:?}", time);
    }

    #[test]
    fn shmem_barrier() {
        // let num_pes = 10;
        // let mut threads = Vec::new();
        // for pe in 0..num_pes{
        //     let t = std::thread::spawn(move || {
        //         let mut barrier = ShmemBarrier::new(pe,num_pes);
        //         println!("pe {:?} before barrier1",pe);
        //         barrier.exec();
        //         std::thread::sleep(Duration::from_secs(pe as u64));
        //         println!("pe {:?} before barrier2",pe);
        //         barrier.exec();
        //         println!("pe {:?} before barrier3",pe);
        //         barrier.exec();
        //         println!("pe {:?} done",pe);
        //     });
        //     threads.push(t);
        // }
        // for t in threads {
        //     t.join().unwrap();
        // }
    }
}

use crate::{
    config,
    lamellae::comm::{CommInfo, CommProgress, CommShutdown},
    lamellar_alloc::{BTreeAlloc, LamellarAlloc},
    Backend,
};

use super::{
    fabric::{ShmemAlloc, ShmemAllocator},
    CommandQueue,
};

use parking_lot::RwLock;

use std::env;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct ShmemComm {
    //size of my segment
    pub(crate) runtime_allocs: RwLock<Vec<(Arc<ShmemAlloc>, BTreeAlloc)>>, //runtime allocations
    _init: AtomicBool,
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    pub(crate) put_amt: Arc<AtomicUsize>,
    pub(crate) get_amt: Arc<AtomicUsize>,
    pub(crate) allocator: ShmemAllocator,
}

pub(crate) static SHMEM_SIZE: AtomicUsize = AtomicUsize::new(4 * 1024 * 1024 * 1024);
const RT_MEM: usize = 100 * 1024 * 1024;
impl ShmemComm {
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new() -> ShmemComm {
        let num_pes = match env::var("LAMELLAR_NUM_PES") {
            Ok(val) => val.parse::<usize>().unwrap(),
            Err(_e) => 1,
        };
        let my_pe = match env::var("LAMELLAR_PE_ID") {
            Ok(val) => val.parse::<usize>().unwrap(),
            Err(_e) => 0,
        };
        let job_id = match env::var("LAMELLAR_JOB_ID") {
            Ok(val) => val.parse::<usize>().unwrap(),
            Err(_e) => 0,
        };
        if let Some(size) = config().heap_size {
            SHMEM_SIZE.store(size, Ordering::SeqCst);
        }

        let cmd_q_mem = CommandQueue::mem_per_pe() * num_pes;
        let total_mem = cmd_q_mem + RT_MEM + SHMEM_SIZE.load(Ordering::SeqCst);
        let mem_per_pe = total_mem; // / num_pes;

        let allocator = ShmemAllocator::new(num_pes, my_pe, job_id);
        let alloc = unsafe {
            allocator.alloc(
                mem_per_pe,
                std::mem::align_of::<u8>(),
                &(0..num_pes).collect::<Vec<_>>(),
            )
        };

        let shmem = ShmemComm {
            runtime_allocs: RwLock::new(vec![(
                alloc.clone(),
                BTreeAlloc::new("shmem_rt_mem".to_string()),
            )]),
            _init: AtomicBool::new(true),
            num_pes: num_pes,
            my_pe: my_pe,
            put_amt: Arc::new(AtomicUsize::new(0)),
            get_amt: Arc::new(AtomicUsize::new(0)),
            allocator: allocator,
        };
        shmem.runtime_allocs.write()[0]
            .1
            .init(alloc.start(), mem_per_pe);
        shmem
    }
}

impl CommShutdown for ShmemComm {
    //TODO perform cleanups of the shared memory if possible
    fn force_shutdown(&self) {
        unimplemented!();
    }
}

impl CommProgress for ShmemComm {
    fn flush(&self) {}
    fn wait(&self) {}
    #[tracing::instrument(skip_all, level = "debug")]
    fn barrier(&self) {
        unsafe { self.allocator.barrier() };
    }
}

impl CommInfo for ShmemComm {
    fn my_pe(&self) -> usize {
        self.my_pe
    }
    fn num_pes(&self) -> usize {
        self.num_pes
    }
    fn backend(&self) -> Backend {
        Backend::Shmem
    }
    fn atomic_avail<T: 'static>(&self) -> bool {
        let id = std::any::TypeId::of::<T>();

        if id == std::any::TypeId::of::<u8>() {
            true
        } else if id == std::any::TypeId::of::<u16>() {
            true
        } else if id == std::any::TypeId::of::<u32>() {
            true
        } else if id == std::any::TypeId::of::<u64>() {
            true
        } else if id == std::any::TypeId::of::<i8>() {
            true
        } else if id == std::any::TypeId::of::<i16>() {
            true
        } else if id == std::any::TypeId::of::<i32>() {
            true
        } else if id == std::any::TypeId::of::<i64>() {
            true
        } else if id == std::any::TypeId::of::<usize>() {
            true
        } else if id == std::any::TypeId::of::<isize>() {
            true
        } else {
            false
        }
    }
    fn MB_sent(&self) -> f64 {
        (self.put_amt.load(Ordering::SeqCst) + self.get_amt.load(Ordering::SeqCst)) as f64
            / 1_000_000.0
    }
}

impl Drop for ShmemComm {
    #[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
        // let allocs = self.alloc.read();
        // for alloc in allocs.iter(){
        //     println!("dropping shmem -- memory in use {:?}", alloc.occupied());
        // }
        // if self.occupied() > 0 {
        //     println!("dropping shmem -- memory in use {:?}", self.occupied());
        // }
        if self.runtime_allocs.read().len() > 1 {
            println!("[LAMELLAR INFO] {:?} additional rt memory pools were allocated, performance may be increased using a larger initial pool, set using the LAMELLAR_HEAP_SIZE envrionment variable. Current initial size = {:?}",self.runtime_allocs.read().len()-1, SHMEM_SIZE.load(Ordering::SeqCst));
        }
    }
}

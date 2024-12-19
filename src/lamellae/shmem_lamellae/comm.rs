use crate::config;

use crate::lamellar_alloc::{BTreeAlloc, LamellarAlloc};

use futures_util::Future;
use parking_lot::RwLock;

use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct ShmemComm {
    // _shmem: MyShmem, //the global handle
    pub(crate) base_address: Arc<RwLock<usize>>, //start address of my segment
    _size: usize,                                //size of my segment
    alloc: RwLock<Vec<BTreeAlloc>>,
    _init: AtomicBool,
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    alloc_lock: Arc<
        RwLock<(
            HashMap<
                usize, //local addr
                (
                    MyShmem,
                    usize,
                    HashMap<
                        //share mem segment, per pe size,
                        usize, //global pe id
                        (usize, usize),
                    >,
                ), // remote addr, relative index (for subteams)
            >,
            ShmemAlloc,
        )>,
    >,
}

static SHMEM_SIZE: AtomicUsize = AtomicUsize::new(4 * 1024 * 1024 * 1024);
const RT_MEM: usize = 100 * 1024 * 1024;
impl ShmemComm {
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

        // let mem_per_pe = SHMEM_SIZE.load(Ordering::SeqCst)/num_pes;
        let cmd_q_mem = CommandQueue::mem_per_pe() * num_pes;
        let total_mem = cmd_q_mem + RT_MEM + SHMEM_SIZE.load(Ordering::SeqCst);
        let mem_per_pe = total_mem; // / num_pes;

        let alloc = ShmemAlloc::new(num_pes, my_pe, job_id);
        // let shmem = attach_to_shmem(SHMEM_SIZE.load(Ordering::SeqCst),"main",job_id,my_pe==0);

        // let (shmem,index) =unsafe {alloc.alloc(mem_per_pe,0..num_pes)};
        let (shmem, _index, addrs) = unsafe { alloc.alloc(mem_per_pe, 0..num_pes) };
        let addr = shmem.as_ptr() as usize + mem_per_pe * my_pe;

        let mut allocs_map = HashMap::new();
        let mut pe_map = HashMap::new();
        for pe in 0..num_pes {
            if addrs[pe] > 0 {
                pe_map.insert(pe, (addrs[pe], pe));
            }
        }
        allocs_map.insert(addr, (shmem, mem_per_pe, pe_map));
        // alloc.0.insert(addr,(ret,size))
        let shmem = ShmemComm {
            // _shmem: shmem,
            base_address: Arc::new(RwLock::new(addr)),
            _size: mem_per_pe,
            alloc: RwLock::new(vec![BTreeAlloc::new("shmem".to_string())]),
            _init: AtomicBool::new(true),
            num_pes: num_pes,
            my_pe: my_pe,
            alloc_lock: Arc::new(RwLock::new((allocs_map, alloc))),
        };
        shmem.alloc.write()[0].init(addr, mem_per_pe);
        shmem
    }

    pub(crate) fn heap_size() -> usize {
        SHMEM_SIZE.load(Ordering::SeqCst)
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
    fn barrier(&self) {
        let alloc = self.alloc_lock.write();
        unsafe {
            alloc.1.alloc(1, 0..self.num_pes);
        }
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
}

impl Drop for ShmemComm {
    fn drop(&mut self) {
        // let allocs = self.alloc.read();
        // for alloc in allocs.iter(){
        //     println!("dropping shmem -- memory in use {:?}", alloc.occupied());
        // }
        if self.occupied() > 0 {
            println!("dropping shmem -- memory in use {:?}", self.occupied());
        }
        if self.alloc.read().len() > 1 {
            println!("[LAMELLAR INFO] {:?} additional rt memory pools were allocated, performance may be increased using a larger initial pool, set using the LAMELLAR_HEAP_SIZE envrionment variable. Current initial size = {:?}",self.alloc.read().len()-1, SHMEM_SIZE.load(Ordering::SeqCst));
        }
    }
}

use crate::{
    config,
    lamellae::{
        comm::{
            atomic::atomic_type_supported, AtomicOp, CommInfo, CommMem, CommProgress, CommShutdown,
        },
        CollectiveOpKind,
    },
    env_var::HeapMode,
    lamellar_alloc::{BTreeAlloc, LamellarAlloc},
    Backend,
};

use super::fabric::{ShmemOptAlloc, ShmemOptAllocator};

#[cfg(feature = "pmi")]
use pmi::pmi::{Pmi, PmiBuilder};

use parking_lot::RwLock;

use std::env;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::trace;

#[derive(Debug)]
pub(crate) struct ShmemOptComm {
    //size of my segment
    pub(crate) runtime_allocs: RwLock<Vec<(ShmemOptAlloc, BTreeAlloc)>>, //runtime allocations
    // symmetric reservation backing every rt pool; pools are carved from it locally
    pub(crate) rt_fabric: ShmemOptAlloc,
    pub(crate) rt_cursor: AtomicUsize, // bytes of rt_fabric handed to pools so far
    _init: AtomicBool,
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    pub(crate) put_amt: Arc<AtomicUsize>,
    pub(crate) get_amt: Arc<AtomicUsize>,
    pub(crate) allocator: ShmemOptAllocator,
}

pub(crate) static SHMEM_SIZE: AtomicUsize = AtomicUsize::new(4 * 1024 * 1024 * 1024);
const RT_MEM: usize = 100 * 1024 * 1024;
impl ShmemOptComm {
    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new() -> ShmemOptComm {
        #[cfg(feature = "pmi")]
        let pmi_info = PmiBuilder::init().ok();

        #[cfg(feature = "pmi")]
        let (num_pes, my_pe, job_id) = if let Some(ref pmi) = pmi_info {
            (pmi.ranks().len(), pmi.rank(), pmi.job_id())
        } else {
            Self::pe_info_from_env()
        };

        #[cfg(not(feature = "pmi"))]
        let (num_pes, my_pe, job_id) = Self::pe_info_from_env();

        if let Some(size) = config().heap_size {
            SHMEM_SIZE.store(size, Ordering::SeqCst);
        }

        // AM traffic lives in the mailbox segment, no command queue carve-out
        let total_mem = RT_MEM + SHMEM_SIZE.load(Ordering::SeqCst);

        let allocator = ShmemOptAllocator::new(num_pes, my_pe, job_id);
        let region_len = allocator.heap().region_len();
        // dynamic mode reserves a large sparse range up front so pool growth is a local
        // cursor bump (no collective, no alloc_task round trip); untouched pages cost nothing
        let rt_len = if config().heap_mode == HeapMode::Static {
            total_mem
        } else {
            std::cmp::max(total_mem, region_len / 4)
        };
        assert!(
            rt_len < region_len,
            "LAMELLAR_HEAP_SIZE ({total_mem} bytes) does not fit in the shmem region ({region_len} bytes), increase LAMELLAR_SHMEM_REGION"
        );
        let rt_fabric = unsafe {
            allocator.alloc(
                rt_len,
                std::mem::align_of::<u8>(),
                &(0..num_pes).collect::<Vec<_>>(),
            )
        };

        #[cfg(feature = "pmi")]
        if let Some(ref pmi) = pmi_info {
            pmi.barrier(false)
                .expect("PMI barrier failed during shmem init");
        }

        let mut rt_pool = BTreeAlloc::new("shmem_rt_mem".to_string());
        rt_pool.init(rt_fabric.start(), total_mem);
        ShmemOptComm {
            runtime_allocs: RwLock::new(vec![(rt_fabric.clone(), rt_pool)]),
            rt_fabric,
            rt_cursor: AtomicUsize::new(total_mem),
            _init: AtomicBool::new(true),
            num_pes: num_pes,
            my_pe: my_pe,
            put_amt: Arc::new(AtomicUsize::new(0)),
            get_amt: Arc::new(AtomicUsize::new(0)),
            allocator: allocator,
        }
    }

    /// Add a local rt pool of at least `min_size` bytes from the rt reservation.
    /// Returns false once the reservation is exhausted.
    pub(crate) fn grow_rt_pool(&self, min_size: usize) -> bool {
        let mut allocs = self.runtime_allocs.write();
        let cursor = self.rt_cursor.load(Ordering::Relaxed);
        let remaining = self.rt_fabric.num_bytes() - cursor;
        let size = std::cmp::min(
            std::cmp::max(min_size * 2, SHMEM_SIZE.load(Ordering::SeqCst)),
            remaining,
        );
        if size < min_size || size == 0 {
            return false;
        }
        let mut pool = BTreeAlloc::new("shmem_rt_mem".to_string());
        pool.init(self.rt_fabric.start() + cursor, size);
        allocs.push((self.rt_fabric.clone(), pool));
        self.rt_cursor.store(cursor + size, Ordering::Relaxed);
        trace!("grew rt heap by {size} bytes ({} pools)", allocs.len());
        true
    }

    pub(crate) fn rt_room(&self, size: usize) -> bool {
        config().heap_mode != HeapMode::Static
            && self.rt_fabric.num_bytes() - self.rt_cursor.load(Ordering::Relaxed) >= size
    }

    fn pe_info_from_env() -> (usize, usize, usize) {
        let num_pes = match env::var("LAMELLAR_NUM_PES") {
            Ok(val) => val.parse::<usize>().unwrap(),
            Err(_) => 1,
        };
        let my_pe = match env::var("LAMELLAR_PE_ID") {
            Ok(val) => val.parse::<usize>().unwrap(),
            Err(_) => 0,
        };
        let job_id = match env::var("LAMELLAR_JOB_ID") {
            Ok(val) => val.parse::<usize>().unwrap(),
            Err(_) => 0,
        };
        (num_pes, my_pe, job_id)
    }
}

impl CommShutdown for ShmemOptComm {
    //TODO perform cleanups of the shared memory if possible
    fn force_shutdown(&self) {
        unimplemented!();
    }
}

impl CommProgress for ShmemOptComm {
    fn flush_all(&self) {}
    fn wait_all(&self) {}
    //#[tracing::instrument(skip_all, level = "debug")]
    fn barrier(&self) {
        unsafe { self.allocator.barrier() };
    }
}

impl CommInfo for ShmemOptComm {
    fn my_pe(&self) -> usize {
        self.my_pe
    }
    fn num_pes(&self) -> usize {
        self.num_pes
    }
    fn backend(&self) -> Backend {
        Backend::ShmemOpt
    }
    fn atomic_avail<T: 'static>(&self) -> bool {
        atomic_type_supported::<T>()
    }
    fn atomic_op_avail<T: 'static>(&self, _op: AtomicOp<T>) -> bool {
        atomic_type_supported::<T>()
    }
    fn MB_sent(&self) -> f64 {
        (self.put_amt.load(Ordering::SeqCst) + self.get_amt.load(Ordering::SeqCst)) as f64
            / 1_000_000.0
    }
    fn collective_avail<T: 'static>(&self, op: CollectiveOpKind) -> bool {
        match op {
            CollectiveOpKind::Barrier => true,
            _ => atomic_type_supported::<T>(),
        }
    }
}

impl Drop for ShmemOptComm {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop ShmemOptComm");
        // peers may already be gone; skip cross-PE offset recycling from here on
        self.allocator.heap().set_shutting_down();
        // let allocs = self.alloc.read();
        // for alloc in allocs.iter(){
        //     println!("dropping shmem -- memory in use {:?}", alloc.occupied());
        // }
        if self.mem_occupied() > 0 {
            println!("dropping shmem -- memory in use {:?}", self.mem_occupied());
        }
        if self.runtime_allocs.read().len() > 1 {
            println!("[LAMELLAR INFO] {:?} additional rt memory pools were allocated, performance may be increased using a larger initial pool, set using the LAMELLAR_HEAP_SIZE envrionment variable. Current initial size = {:?}",self.runtime_allocs.read().len()-1, SHMEM_SIZE.load(Ordering::SeqCst));
            self.print_pools();
        }
        trace!(target: "drop", "end drop ShmemOptComm");
    }
}

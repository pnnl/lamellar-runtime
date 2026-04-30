use crate::{
    config,
    lamellae::{
        comm::{AtomicOp, CommInfo, CommMem, CommProgress, CommShutdown},
        AllocationType, CollectiveOpKind,
    },
    lamellar_alloc::{BTreeAlloc, LamellarAlloc},
    Backend,
};

use super::{fabric::*, CommandQueue};

use tracing::{debug, trace};

use parking_lot::RwLock;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct UcxMtComm {
    pub(crate) ucx: Arc<UcxWorld>,
    pub(crate) runtime_allocs: RwLock<Vec<(UcxMtAlloc, BTreeAlloc)>>, //runtime allocations
    // pub(crate) fabric_allocs: RwLock<HashMap<usize, CommAlloc>>,
    _init: AtomicBool,
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    pub(crate) put_amt: Arc<AtomicUsize>,
    // pub(crate) put_cnt: Arc<AtomicUsize>,
    pub(crate) get_amt: Arc<AtomicUsize>,
    // pub(crate) get_cnt: Arc<AtomicUsize>,
}

pub(crate) static HEAP_SIZE: AtomicUsize = AtomicUsize::new(4 * 1024 * 1024 * 1024);
const RT_MEM: usize = 100 * 1024 * 1024;
impl UcxMtComm {
    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new(num_threads: usize) -> UcxMtComm {
        if let Some(size) = config().heap_size {
            HEAP_SIZE.store(size, Ordering::SeqCst);
        }
        let ucx = Arc::new(UcxWorld::new(num_threads));
        trace!("ucx initialized: {:?}", ucx);

        ucx.barrier();
        let num_pes = ucx.num_pes;
        let cmd_q_mem = CommandQueue::mem_per_pe() * num_pes;
        let total_mem = cmd_q_mem + RT_MEM + HEAP_SIZE.load(Ordering::SeqCst);
        // let mem_per_pe = total_mem; // / num_pes;

        let alloc_info = ucx.alloc(
            total_mem,
            std::mem::align_of::<u8>(),
            AllocationType::Global,
        );
        // println!("rt alloc info: {:?}", alloc_info);

        let ucx_comm = UcxMtComm {
            ucx: ucx.clone(),
            runtime_allocs: RwLock::new(vec![(
                alloc_info.clone(),
                BTreeAlloc::new("ucx_c_mem".to_string()),
            )]),
            // fabric_allocs: RwLock::new(HashMap::new()),
            _init: AtomicBool::new(true),
            num_pes: num_pes,
            my_pe: ucx.my_pe,
            put_amt: Arc::new(AtomicUsize::new(0)),
            // put_cnt: Arc::new(AtomicUsize::new(0)),
            get_amt: Arc::new(AtomicUsize::new(0)),
            // get_cnt: Arc::new(AtomicUsize::new(0)),
        };
        ucx_comm.runtime_allocs.write()[0]
            .1
            .init(alloc_info.start().into(), total_mem);
        ucx_comm
    }
}

impl CommShutdown for UcxMtComm {
    fn force_shutdown(&self) {}
}

impl CommProgress for UcxMtComm {
    fn flush_all(&self) {
        self.ucx.progress_all();
    }
    fn thread_flush(&self) {
        self.ucx.thread_progress();
    }
    fn wait_all(&self) {
        self.ucx.wait_all();
    }
    fn thread_wait(&self) {
        self.ucx.thread_wait();
    }
    //#[tracing::instrument(skip_all, level = "debug")]
    fn barrier(&self) {
        self.ucx.barrier();
    }
}

impl CommInfo for UcxMtComm {
    fn my_pe(&self) -> usize {
        self.my_pe
    }
    fn num_pes(&self) -> usize {
        self.num_pes
    }
    fn backend(&self) -> Backend {
        Backend::Ucx
    }
    fn atomic_avail<T: 'static>(&self) -> bool {
        self.ucx.atomic_avail::<T>()
    }
    fn atomic_op_avail<T: 'static>(&self, op: AtomicOp<T>) -> bool {
        self.ucx.atomic_op_avail(&op)
    }
    fn MB_sent(&self) -> f64 {
        (self.put_amt.load(Ordering::SeqCst) + self.get_amt.load(Ordering::SeqCst)) as f64
            / 1_000_000.0
    }
    
    fn collective_avail<T: 'static>(&self,op: CollectiveOpKind) -> bool {
        self.ucx.collective_avail::<T>(op)
    }
}

impl Drop for UcxMtComm {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop UcxMtComm");
        // println!("dropping ucx comm");
        if self.mem_occupied() > 0 {
            println!("dropping ucx -- memory in use {:?}", self.mem_occupied());
        }
        // self.exchange_buffer.take();
        if self.runtime_allocs.read().len() > 1 {
            println!("[LAMELLAR INFO] {:?} additional rt memory pools were allocated, performance may be increased using a larger initial pool, set using the LAMELLAR_HEAP_SIZE envrionment variable. Current initial size = {:?}",self.runtime_allocs.read().len()-1, HEAP_SIZE.load(Ordering::SeqCst));
            self.print_pools();
        }

        self.runtime_allocs.write().clear();

        // for (alloc, _) in self.runtime_allocs.write().drain(..) {
        //     self.ucx.free_alloc(&alloc);
        // }

        // let _ = self.ucx.clear_allocs();
        let world_ref_count = Arc::strong_count(&self.ucx);
        debug!(
            "Dropping UcxMtComm: ucx world ref count: {}",
            world_ref_count
        );
        let _ = self.ucx.barrier();
        trace!(target: "drop", "end drop UcxMtComm");
    }
}

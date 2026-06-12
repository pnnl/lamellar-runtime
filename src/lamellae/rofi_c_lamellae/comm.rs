use crate::{
    config,
    lamellae::{
        comm::atomic::AtomicOp,
        comm::{
            CommInfo, CommMem,
            CommProgress, CommShutdown,
        },
        AllocationType,
    },
    lamellar_alloc::{BTreeAlloc, LamellarAlloc},
    Backend,
};

use super::{fabric::*, rofi::*, CommandQueue};

use parking_lot::RwLock;
use tracing::trace;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct RofiCComm {
    pub(crate) rofi_c: Arc<RofiC>,
    pub(crate) runtime_allocs: RwLock<Vec<(RofiCAlloc, BTreeAlloc)>>, //runtime allocations
    _init: AtomicBool,
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    pub(crate) put_amt: Arc<AtomicUsize>,
    #[allow(dead_code)]
    pub(crate) put_cnt: Arc<AtomicUsize>,
    pub(crate) get_amt: Arc<AtomicUsize>,
    #[allow(dead_code)]
    pub(crate) get_cnt: Arc<AtomicUsize>,
}

pub(crate) static HEAP_SIZE: AtomicUsize = AtomicUsize::new(4 * 1024 * 1024 * 1024);
const RT_MEM: usize = 100 * 1024 * 1024;
impl RofiCComm {
    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new(provider: &str, domain: &str) -> RofiCComm {
        if let Some(size) = config().heap_size {
            HEAP_SIZE.store(size, Ordering::SeqCst);
        }
        let rofi_c =
            RofiC::new(Some(provider), Some(domain)).expect("Rofi-C initialization failed");
        trace!("rofi-c initialized: {:?}", rofi_c);

        let _ = rofi_c.barrier();
        let num_pes = rofi_c.num_pes;
        let cmd_q_mem = CommandQueue::mem_per_pe() * num_pes;
        let total_mem = cmd_q_mem + RT_MEM + HEAP_SIZE.load(Ordering::SeqCst);

        let alloc_info = rofi_c
            .alloc(
                total_mem,
                AllocationType::Global,
                std::mem::align_of::<u8>(),
            )
            .expect("rofi rt alloc failed");
        let mut first_alloc = BTreeAlloc::new("rofi_c_rt_mem".to_string());
        first_alloc.init(alloc_info.start(), total_mem);

        let rofi_c_comm = RofiCComm {
            rofi_c: rofi_c.clone(),
            runtime_allocs: RwLock::new(vec![(alloc_info.clone(), first_alloc)]),
            _init: AtomicBool::new(true),
            num_pes,
            my_pe: rofi_c.my_pe,
            put_amt: Arc::new(AtomicUsize::new(0)),
            put_cnt: Arc::new(AtomicUsize::new(0)),
            get_amt: Arc::new(AtomicUsize::new(0)),
            get_cnt: Arc::new(AtomicUsize::new(0)),
        };
        rofi_c_comm
    }

    #[allow(dead_code)]
    pub(crate) fn heap_size() -> usize {
        HEAP_SIZE.load(Ordering::SeqCst)
    }
}

impl CommShutdown for RofiCComm {
    fn force_shutdown(&self) {}
}

impl CommProgress for RofiCComm {
    fn flush_all(&self) {
        let _ = self.rofi_c.progress_all();
    }
    fn wait_all(&self) {
        let _ = self.rofi_c.wait_all();
    }
    //#[tracing::instrument(skip_all, level = "debug")]
    fn barrier(&self) {
        let _ = self.rofi_c.barrier();
    }
}

impl CommInfo for RofiCComm {
    fn my_pe(&self) -> usize {
        self.my_pe
    }
    fn num_pes(&self) -> usize {
        self.num_pes
    }
    fn backend(&self) -> Backend {
        Backend::RofiC
    }
    fn MB_sent(&self) -> f64 {
        (self.put_amt.load(Ordering::SeqCst) + self.get_amt.load(Ordering::SeqCst)) as f64
            / 1_000_000.0
    }
    fn atomic_avail<T: 'static>(&self) -> bool
    where
        Self: Sized,
    {
        self.rofi_c.atomic_avail::<T>()
    }
    fn atomic_op_avail<T: 'static>(&self, op: AtomicOp<T>) -> bool {
        self.rofi_c.atomic_op_avail(&op)
    }
}

impl Drop for RofiCComm {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop RofiCComm");
        if self.mem_occupied() > 0 {
            println!("dropping rofi_c -- memory in use {:?}", self.mem_occupied());
        }
        if self.runtime_allocs.read().len() > 1 {
            println!("[LAMELLAR INFO] {:?} additional rt memory pools were allocated, performance may be increased using a larger initial pool, set using the LAMELLAR_HEAP_SIZE envrionment variable. Current initial size = {:?}",self.runtime_allocs.read().len()-1, HEAP_SIZE.load(Ordering::SeqCst));
            self.print_pools();
        }
        self.runtime_allocs.write().clear();
        let world_ref_count = Arc::strong_count(&self.rofi_c);
        trace!(
            "dropping rofi_c comm, rofi_c world ref count {:?}",
            world_ref_count
        );
        let _ = self.rofi_c.barrier();
        let _ = rofi_c_finit();
        trace!(target: "drop", "end drop RofiCComm");
    }
}

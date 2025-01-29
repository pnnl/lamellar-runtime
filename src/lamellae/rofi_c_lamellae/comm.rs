use crate::{
    config,
    lamellae::{AllocationType,comm::{CommInfo, CommProgress, CommShutdown,CommAlloc,CommAllocType,CommMem}},
    lamellar_alloc::{BTreeAlloc, LamellarAlloc},
    Backend,
};

use super::{
    fabric::*,
    CommandQueue,
};

use parking_lot::RwLock;

use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct RofiCComm {
    // _rofi_c: MyRofiC, //the global handle
    pub(crate) base_address: Arc<RwLock<usize>>, //start address of my segment
    pub(crate) runtime_allocs: RwLock<Vec<BTreeAlloc>>,//runtime allocations
    pub(crate) fabric_allocs: RwLock<HashMap<usize, CommAlloc>>,
    _init: AtomicBool,
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    pub(crate) put_amt: Arc<AtomicUsize>,
    pub(crate) put_cnt: Arc<AtomicUsize>,
    pub(crate) get_amt: Arc<AtomicUsize>,
    pub(crate) get_cnt: Arc<AtomicUsize>
}

pub(crate) static ROFI_SIZE: AtomicUsize = AtomicUsize::new(4 * 1024 * 1024 * 1024);
const RT_MEM: usize = 100 * 1024 * 1024;
impl RofiCComm {
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new(provider: &str, domain: &str) -> RofiCComm {
        if let Some(size) = config().heap_size {
            ROFI_SIZE.store(size, Ordering::SeqCst);
        }
        rofi_c_init(provider, domain).expect("error in rofi_c init");

        rofi_c_barrier();
        let num_pes = rofi_c_get_size();
        let cmd_q_mem = CommandQueue::mem_per_pe() * num_pes;
        let total_mem = cmd_q_mem + RT_MEM + ROFI_SIZE.load(Ordering::SeqCst);
        let mem_per_pe = total_mem; // / num_pes;

        let addr = rofi_c_alloc(total_mem, AllocationType::Global).expect("error in rofi_c_alloc") as usize;
        let rofi_c = RofiCComm {
            base_address: Arc::new(RwLock::new(addr)),
            runtime_allocs: RwLock::new(vec![BTreeAlloc::new("rofi_c_mem".to_string())]),
            fabric_allocs: RwLock::new(HashMap::new()),
            _init: AtomicBool::new(true),
            num_pes: num_pes,
            my_pe: rofi_c_get_id(),
            put_amt: Arc::new(AtomicUsize::new(0)),
            put_cnt: Arc::new(AtomicUsize::new(0)),
            get_amt: Arc::new(AtomicUsize::new(0)),
            get_cnt: Arc::new(AtomicUsize::new(0)),
        };
        rofi_c.runtime_allocs.write()[0].init(addr, total_mem);
        rofi_c.fabric_allocs.write().insert(addr, CommAlloc{addr, size: mem_per_pe, alloc_type: CommAllocType::Fabric});
        rofi_c
    }

    pub(crate) fn heap_size() -> usize {
        ROFI_SIZE.load(Ordering::SeqCst)
    }
}

impl CommShutdown for RofiCComm {
    fn force_shutdown(&self) {
        let _res = rofi_c_finit();
    }
}

impl CommProgress for RofiCComm {
    fn flush(&self) {
        if rofi_c_flush() != 0 {
            println!("rofi_c flush error");
        }
    }
    fn wait(&self) {
        if rofi_c_wait() != 0 {
            println!("rofi_c wait error");
        }
    }
    #[tracing::instrument(skip_all, level = "debug")]
    fn barrier(&self) {
        rofi_c_barrier();
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
}

impl Drop for RofiCComm {
    #[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
        if self.mem_occupied() > 0 {
            println!("dropping rofi_c -- memory in use {:?}", self.mem_occupied());
        }
        if self.runtime_allocs.read().len() > 1 {
            println!("[LAMELLAR INFO] {:?} additional rt memory pools were allocated, performance may be increased using a larger initial pool, set using the LAMELLAR_HEAP_SIZE envrionment variable. Current initial size = {:?}",self.runtime_allocs.read().len()-1, ROFI_SIZE.load(Ordering::SeqCst));
        }
        for (addr,_alloc) in self.fabric_allocs.read().iter(){
            rofi_c_release(*addr);
        }
        rofi_c_barrier();
        let _res = rofi_c_finit();
    }
}

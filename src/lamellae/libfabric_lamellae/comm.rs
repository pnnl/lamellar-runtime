use crate::{
    config,
    lamellae::{
        comm::{CommInfo, CommMem, CommProgress, CommShutdown},
        AllocationType,
    },
    lamellar_alloc::{BTreeAlloc, LamellarAlloc},
    Backend,
};

use super::{fabric::*, CommandQueue};

use tracing::trace;

use parking_lot::RwLock;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct LibfabricComm {
    pub(crate) ofi: Arc<Ofi>,
    pub(crate) runtime_allocs: RwLock<Vec<(Arc<AllocInfo>, BTreeAlloc)>>, //runtime allocations
    // pub(crate) fabric_allocs: RwLock<HashMap<usize, CommAlloc>>,
    _init: AtomicBool,
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    pub(crate) put_amt: Arc<AtomicUsize>,
    pub(crate) put_cnt: Arc<AtomicUsize>,
    pub(crate) get_amt: Arc<AtomicUsize>,
    pub(crate) get_cnt: Arc<AtomicUsize>,
}

pub(crate) static HEAP_SIZE: AtomicUsize = AtomicUsize::new(4 * 1024 * 1024 * 1024);
const RT_MEM: usize = 100 * 1024 * 1024;
impl LibfabricComm {
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new(provider: Option<&str>,
        domain: Option<&str>,) -> LibfabricComm {
        if let Some(size) = config().heap_size {
            HEAP_SIZE.store(size, Ordering::SeqCst);
        }
        let ofi = Ofi::new(provider,domain).expect("error in ofi init");
        trace!("ofi initialized: {:?}", ofi);

        ofi.barrier();
        let num_pes = ofi.num_pes;
        let cmd_q_mem = CommandQueue::mem_per_pe() * num_pes;
        let total_mem = cmd_q_mem + RT_MEM + HEAP_SIZE.load(Ordering::SeqCst);
        let mem_per_pe = total_mem; // / num_pes;

        let alloc_info = ofi
            .alloc(total_mem, AllocationType::Global)
            .expect("error in ofi alloc");

        let lib_fabric_comm = LibfabricComm {
            ofi: ofi.clone(),
            runtime_allocs: RwLock::new(vec![(
                alloc_info.clone(),
                BTreeAlloc::new("libfabric_c_mem".to_string()),
            )]),
            // fabric_allocs: RwLock::new(HashMap::new()),
            _init: AtomicBool::new(true),
            num_pes: num_pes,
            my_pe: ofi.my_pe,
            put_amt: Arc::new(AtomicUsize::new(0)),
            put_cnt: Arc::new(AtomicUsize::new(0)),
            get_amt: Arc::new(AtomicUsize::new(0)),
            get_cnt: Arc::new(AtomicUsize::new(0)),
        };
        lib_fabric_comm.runtime_allocs.write()[0]
            .1
            .init(alloc_info.start(), total_mem);
        // lib_fabric_comm.fabric_allocs.write().insert(
        //     addr,
        //     CommAlloc {
        //         addr,
        //         size: mem_per_pe,
        //         alloc_type: CommAllocType::Fabric,
        //     },
        // );
        lib_fabric_comm
    }

    pub(crate) fn heap_size() -> usize {
        HEAP_SIZE.load(Ordering::SeqCst)
    }
}

impl CommShutdown for LibfabricComm {
    fn force_shutdown(&self) {}
}

impl CommProgress for LibfabricComm {
    fn flush(&self) {
        if let Err(e) = self.ofi.progress() {
            println!("libfabric flush error: {}", e);
        }
    }
    fn wait(&self) {
        if let Err(e) = self.ofi.wait_all() {
            println!("libfabric wait error");
        }
    }
    #[tracing::instrument(skip_all, level = "debug")]
    fn barrier(&self) {
        self.ofi.barrier().expect("error in libfabric barrier");
    }
}

impl CommInfo for LibfabricComm {
    fn my_pe(&self) -> usize {
        self.my_pe
    }
    fn num_pes(&self) -> usize {
        self.num_pes
    }
    fn backend(&self) -> Backend {
        Backend::Libfabric
    }
    fn MB_sent(&self) -> f64 {
        (self.put_amt.load(Ordering::SeqCst) + self.get_amt.load(Ordering::SeqCst)) as f64
            / 1_000_000.0
    }
}

impl Drop for LibfabricComm {
    #[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
        if self.mem_occupied() > 0 {
            println!(
                "dropping libfabric -- memory in use {:?}",
                self.mem_occupied()
            );
        }
        if self.runtime_allocs.read().len() > 1 {
            println!("[LAMELLAR INFO] {:?} additional rt memory pools were allocated, performance may be increased using a larger initial pool, set using the LAMELLAR_HEAP_SIZE envrionment variable. Current initial size = {:?}",self.runtime_allocs.read().len()-1, HEAP_SIZE.load(Ordering::SeqCst));
        }
        //maybe we want to implement an ofi finit function which will free all resources or something
        // for (addr, _alloc) in self.fabric_allocs.write().drain(..) {
        //     self.ofi.free(addr).expect("error in ofi free");
        // }
        self.ofi.clear_allocs();
        self.ofi.barrier();
    }
}

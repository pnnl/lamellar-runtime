use crate::{
    config,
    lamellae::{
        comm::{background_progress_due, AtomicOp, CommInfo, CommMem, CommProgress, CommShutdown},
        AllocationType, CollectiveOpKind,
    },
    lamellar_alloc::{BTreeAlloc, LamellarAlloc},
    Backend,
};

use super::{fabric::*, CommandQueue};

use tracing::trace;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct LibfabricSysOptComm {
    pub(crate) ofi: Arc<OptOfi>,
    // runtime_allocs now lives on OptOfi itself (see OptOfi::runtime_allocs) --
    // LibfabricSysOptAlloc/OneSidedLibfabricSysOptAlloc have no path back to
    // LibfabricSysOptComm, only to OptOfi, so the pool moved there.
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
impl LibfabricSysOptComm {
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new(provider: Option<&str>, domain: Option<&str>) -> LibfabricSysOptComm {
        if let Some(size) = config().heap_size {
            HEAP_SIZE.store(size, Ordering::SeqCst);
        }
        let ofi = OptOfi::new(provider, domain).expect("error in ofi init");
        trace!("ofi initialized: {:?}", ofi);

        ofi.barrier()
            .expect("error in libfabric-sys barrier during init");
        ofi.init_barrier().expect("error initializing barrier");
        let num_pes = ofi.num_pes;
        let cmd_q_mem = CommandQueue::mem_per_pe() * num_pes;
        let total_mem = cmd_q_mem + RT_MEM + HEAP_SIZE.load(Ordering::SeqCst);
        // let mem_per_pe = total_mem; // / num_pes;

        let alloc_info = ofi
            .alloc(
                total_mem,
                AllocationType::Global,
                std::mem::align_of::<u8>(),
            )
            .expect("error in ofi alloc");
        trace!(
            "ofi allocated memory: addr={:?}, len={:?}",
            alloc_info.start(),
            alloc_info.num_bytes()
        );
        let mut initial_pool = BTreeAlloc::new("libfabric_sys_rt_mem".to_string());
        initial_pool.init(alloc_info.start(), total_mem);
        ofi.runtime_allocs.write().push((alloc_info.clone(), initial_pool));

        let lib_fabric_comm = LibfabricSysOptComm {
            ofi: ofi.clone(),
            // fabric_allocs: RwLock::new(HashMap::new()),
            _init: AtomicBool::new(true),
            num_pes: num_pes,
            my_pe: ofi.my_pe,
            put_amt: Arc::new(AtomicUsize::new(0)),
            // put_cnt: Arc::new(AtomicUsize::new(0)),
            get_amt: Arc::new(AtomicUsize::new(0)),
            // get_cnt: Arc::new(AtomicUsize::new(0)),
        };
        trace!("lib_fabric_comm initialized: {:?}", lib_fabric_comm);
        lib_fabric_comm
    }

    // pub(crate) fn heap_size() -> usize {
    //     HEAP_SIZE.load(Ordering::SeqCst)
    // }
}

impl CommShutdown for LibfabricSysOptComm {
    fn force_shutdown(&self) {}
}

impl CommProgress for LibfabricSysOptComm {
    fn flush_all(&self) {
        self.ofi.progress_all();
    }
    fn thread_flush(&self) {
        self.ofi.thread_progress();
    }
    /// See [`background_progress_due`]; a collective in flight also keeps progress prompt.
    fn background_flush(&self) {
        if self.ofi.collective_in_flight() || background_progress_due() {
            self.thread_flush();
        }
    }
    fn wait_all(&self) {
        self.ofi.wait_all();
    }
    fn thread_wait(&self) {
        self.ofi.thread_wait();
    }
    #[tracing::instrument(skip_all, level = "debug")]
    fn barrier(&self) {
        self.ofi.barrier().expect("error in libfabric-sys barrier");
    }
}

impl CommInfo for LibfabricSysOptComm {
    fn my_pe(&self) -> usize {
        self.my_pe
    }
    fn num_pes(&self) -> usize {
        self.num_pes
    }
    fn backend(&self) -> Backend {
        Backend::LibfabricSysOpt
    }
    fn atomic_avail<T: 'static>(&self) -> bool {
        self.ofi.atomic_avail::<T>()
    }
    fn atomic_op_avail<T: 'static>(&self, op: AtomicOp<T>) -> bool {
        self.ofi.atomic_op_avail::<T>(op)
    }
    fn MB_sent(&self) -> f64 {
        (self.put_amt.load(Ordering::SeqCst) + self.get_amt.load(Ordering::SeqCst)) as f64
            / 1_000_000.0
    }
    fn collective_avail<T: 'static>(&self, op: CollectiveOpKind) -> bool {
        self.ofi.collective_avail::<T>(op)
    }
}

impl Drop for LibfabricSysOptComm {
    #[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
        trace!("dropping libfabric-sys comm");
        if self.mem_occupied() > 0 {
            println!(
                "dropping libfabric-sys -- memory in use {:?}",
                self.mem_occupied()
            );
        }
        if self.ofi.runtime_allocs.read().len() > 1 {
            println!("[LAMELLAR INFO] {:?} additional rt memory pools were allocated, performance may be increased using a larger initial pool, set using the LAMELLAR_HEAP_SIZE envrionment variable. Current initial size = {:?}",self.ofi.runtime_allocs.read().len()-1, HEAP_SIZE.load(Ordering::SeqCst));
            self.print_pools();
        }
        let fallbacks = self.ofi.staging_fallbacks();
        if fallbacks > 0 {
            println!(
                "[LAMELLAR INFO][{}] {} RDMA ops fell back to unregistered heap buffers because the registered memory pool was exhausted; consider increasing LAMELLAR_HEAP_SIZE",
                self.my_pe, fallbacks
            );
        }
        // Breaks the OptOfi <-> LibfabricSysOptAlloc reference cycle created by
        // storing rt-pool allocs on OptOfi itself (mirrors clear_allocs() below
        // for the analogous alloc_manager cycle) -- without this, OptOfi would
        // never be freed.
        self.ofi.runtime_allocs.write().clear();
        //maybe we want to implement an ofi finit function which will free all resources or something
        // for (addr, _alloc) in self.fabric_allocs.write().drain(..) {
        //     self.ofi.free(addr).expect("error in ofi free");
        // }
        // Drain outstanding completions on both sides *before* the barrier below --
        // the barrier is the cross-rank sync point that guarantees the peer has also
        // finished draining before anyone proceeds to close_fid() the fabric
        // resources. This must happen while barrier_impl is still the hardware
        // collective/manual barrier (i.e. before clear_barrier() below tears it
        // down) -- OptOfi::drop used to redo this same drain+sync with a raw PMI
        // fence as a fallback since by then clear_barrier() had already run, but a
        // second/extra raw PMIx_Fence call risked an unequal collective-call-count
        // hang across ranks (confirmed live). Doing the one sync here, while the
        // already-proven-working hardware barrier is still available, removes the
        // need for that fallback entirely.
        self.ofi.wait_all();
        let _ = self.ofi.barrier();
        self.ofi.clear_barrier();
        let _ = self.ofi.clear_allocs();
        // Wait (briefly, as a safety margin -- not for correctness of what follows)
        // for any other lingering Arc<OptOfi> clone to drop, so nothing else is
        // still actively using ofi when we tear it down below.
        while Arc::strong_count(&self.ofi) > 1 {
            std::thread::yield_now();
        }
        // Explicitly drive the raw PMIx_Fence + fid closes ourselves, on this
        // thread, rather than relying on the implicit Arc<OptOfi> field-drop
        // below to trigger it -- that implicit drop only runs on THIS thread if
        // no other thread races in a fresh clone+drop between our check above and
        // the field-drop below (measured: it can, even after the count==1 check).
        // final_teardown() is idempotent (guarded), so whichever thread ends up
        // running the eventual Arc<OptOfi>::drop (this one, or a racing one) is a
        // harmless no-op once this explicit call has already run.
        self.ofi.final_teardown();

        trace!(
            "libfabric-sys comm dropped ofi count: {:?}",
            Arc::strong_count(&self.ofi)
        );
    }
}

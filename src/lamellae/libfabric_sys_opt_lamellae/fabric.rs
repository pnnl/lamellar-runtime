use std::{
    collections::HashMap,
    env,
    mem::MaybeUninit,
    ops::{Range, RangeFrom, RangeFull, RangeTo},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    task::{Poll, Waker},
};

use libc::{sysconf, _SC_PAGESIZE, _SC_PHYS_PAGES};
use libfabric_sys;
use parking_lot::{Mutex, RwLock};
use pmi::{Pmi, PmiBuilder};
use tracing::{debug, trace};

fn node_total_memory_bytes() -> Option<u64> {
    let pages = unsafe { sysconf(_SC_PHYS_PAGES) };
    let page_size = unsafe { sysconf(_SC_PAGESIZE) };
    if pages <= 0 || page_size <= 0 {
        return None;
    }

    let pages = pages as u64;
    let page_size = page_size as u64;
    pages.checked_mul(page_size)
}

use crate::{
    config,
    lamellae::{
        calc_alloc_padding_size_align,
        collective::{
            AllReduceOp, ReduceOp, RootOrSliceMut, RootSrcOrSliceMut, RootSrcSliceOrNone,
        },
        decode_padding, decode_ref_count, decode_ref_count_and_padding, decrement_ref_count,
        encode_ref_count_and_padding, get_ref_count, increment_ref_count, AllocError, AllocResult,
        AllocationType, AtomicOp, CollectiveOpKind, CommAlloc, CommAllocAddr, CommAllocInner,
        FabricError, FabricResult,
    },
    lamellar_alloc::{BTreeAlloc, LamellarAlloc},
};

#[cfg(feature = "enable-on-node-shmem")]
use crate::lamellae::shmem_utils::{attach_shmem_segment, ShmemSegment};

unsafe fn close_fid(name: &str, fid: *mut libfabric_sys::fid) {
    if fid.is_null() {
        return;
    }
    let ret = libfabric_sys::inlined_fi_close(fid);
    if ret != 0 {
        eprintln!("Error closing libfabric-sys {name}: {ret}");
    }
}

struct RawMcGroup {
    mc: *mut libfabric_sys::fid_mc,
    av_set: *mut libfabric_sys::fid_av_set,
    coll_addr: u64,
}

unsafe impl Send for RawMcGroup {}
unsafe impl Sync for RawMcGroup {}

impl Drop for RawMcGroup {
    fn drop(&mut self) {
        unsafe {
            close_fid("multicast group", &mut (*self.mc).fid);
            close_fid("address-vector set", &mut (*self.av_set).fid);
        }
    }
}

enum BarrierImpl {
    Uninit,
    Collective(Arc<RawMcGroup>),
    Manual(LibfabricSysOptAlloc, AtomicUsize),
    Pmi(Arc<dyn Pmi>),
}

#[derive(Clone, Copy)]
enum AtomicOpKind {
    Min,
    Max,
    Sum,
    Prod,
    BitOr,
    BitXor,
    BitAnd,
    Read,
    Write,
    Cas,
}

/// Per-op completion signal for the ticket-based (Stage 2) completion model. One `Ticket` is
/// created per libfabric RMA/atomic op that requests `FI_COMPLETION`; its address is leaked
/// (via [`TicketCtx`]) into that op's context so the CQ drain path can reclaim it and mark it
/// done without a global counter. Distinct from the older `coll_cnt_issued`/`coll_cnt_completed`
/// scheme, which collectives still use unchanged.
pub(crate) struct Ticket {
    done: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl Ticket {
    fn new() -> Arc<Self> {
        Arc::new(Ticket {
            done: AtomicBool::new(false),
            waker: Mutex::new(None),
        })
    }

    /// A ticket for an op that never needs to touch the network (e.g. the local-copy or
    /// same-node-shmem fast paths) — already satisfied, so `block`/`poll` return immediately.
    fn new_done() -> Arc<Self> {
        Arc::new(Ticket {
            done: AtomicBool::new(true),
            waker: Mutex::new(None),
        })
    }

    pub(crate) fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    /// Registers `waker` to be woken on completion, then re-checks `done` to catch completion
    /// that landed between the caller's own check and this registration (missed-wakeup guard).
    pub(crate) fn register_waker(&self, waker: &Waker) -> bool {
        *self.waker.lock() = Some(waker.clone());
        self.is_done()
    }

    /// Self-driven: takes `comm_group.lock` blocking (same pattern as the baseline
    /// `libfabric_sys_lamellae`'s `wait_for_cntr`) and calls `progress()` itself until this
    /// ticket completes. No cross-thread wake needed — this thread is the one making progress.
    pub(crate) fn block(&self, ofi: &OptOfi) {
        let _guard = ofi.comm_group.lock.lock();
        while !self.done.load(Ordering::Acquire) {
            ofi.comm_group.progress();
            std::thread::yield_now();
        }
    }

    /// Called from `progress()` once this ticket's op has completed.
    fn complete(&self) {
        self.done.store(true, Ordering::Release);
        if let Some(waker) = self.waker.lock().take() {
            waker.wake();
        }
    }
}

/// Wraps a leaked `Arc<Ticket>` pointer together with the `fi_context` storage the provider
/// requires (domain hints set `mode = FI_CONTEXT`) for any op issued through a msg-variant call
/// with `FI_COMPLETION` set. `fi_ctx` is the first field so the pointer passed to libfabric as
/// the op's `context` and the pointer returned in the CQ entry's `op_context` are both exactly
/// this struct's address — the provider only ever reads/writes the first `sizeof(fi_context)`
/// (32) bytes, per the FI_CONTEXT mode contract, leaving `ticket` untouched.
#[repr(C)]
struct TicketCtx {
    // fi_context2 (64B), not fi_context (32B): the chosen info's mode is zeroed, so nothing
    // pins the provider stack to FI_CONTEXT; any layer using the FI_CONTEXT2-sized scratch
    // would otherwise overrun this small heap box into `ticket` / the next chunk.
    fi_ctx: libfabric_sys::fi_context2,
    ticket: *const Ticket,
}

impl TicketCtx {
    /// Leaks `ticket` into a new context pointer suitable for passing as an op's `context`
    /// argument. Must be paired with exactly one [`Self::reclaim`] call (done by the CQ drain
    /// path) or the `Ticket` (and the small `TicketCtx` box) leaks forever.
    fn leak(ticket: Arc<Ticket>) -> *mut std::ffi::c_void {
        let boxed = Box::new(TicketCtx {
            fi_ctx: libfabric_sys::fi_context2 {
                internal: [std::ptr::null_mut(); 8],
            },
            ticket: Arc::into_raw(ticket),
        });
        Box::into_raw(boxed) as *mut std::ffi::c_void
    }

    /// Reverses [`Self::leak`]: reclaims the `Box<TicketCtx>` and the `Arc<Ticket>` it carried.
    /// `ctx` must be a pointer produced by `leak` and not already reclaimed.
    unsafe fn reclaim(ctx: *mut std::ffi::c_void) -> Arc<Ticket> {
        let boxed = Box::from_raw(ctx as *mut TicketCtx);
        Arc::from_raw(boxed.ticket)
    }
}

pub(crate) struct CommGroup {
    mapped_addresses: Vec<u64>,
    ep: *mut libfabric_sys::fid_ep,
    cq: *mut libfabric_sys::fid_cq,
    put_cntr: *mut libfabric_sys::fid_cntr,
    get_cntr: *mut libfabric_sys::fid_cntr,
    av: *mut libfabric_sys::fid_av,
    eq: *mut libfabric_sys::fid_eq,
    info_entry: *mut libfabric_sys::fi_info,
    put_cnt: AtomicU64,
    get_cnt: AtomicU64,
    coll_cnt_issued: AtomicU64,
    coll_cnt_completed: AtomicU64,
    barrier_impl: RwLock<BarrierImpl>,
    lock: Mutex<()>,
    // Serializes blocking `post_collective` issue + completion wait per PE. Completion is
    // matched by count (`coll_cnt_completed >= cnt`), which is only exact with one
    // collective in flight: otherwise another thread's barrier/allgather completing could
    // release this waiter early, and e.g. `collective_exchange_mr_info` would free its
    // allgather buffers while the coll provider still writes into them.
    coll_lock: Mutex<()>,
    // Serializes `fi_join_collective` issue + `wait_for_join_event` per PE. Separate from
    // `lock` so a join waiter never holds the CQ lock across its whole wait (the old
    // starvation hang). Without it, concurrent joins on one PE drain the shared EQ and
    // one waiter can consume (and previously dropped) another's FI_JOIN_COMPLETE.
    join_lock: Mutex<()>,
    // FI_JOIN_COMPLETE contexts read by a waiter they didn't belong to. Belt-and-braces
    // with `join_lock`; checked before each EQ read.
    pending_join_events: Mutex<Vec<usize>>,
}

unsafe impl Send for CommGroup {}
unsafe impl Sync for CommGroup {}

impl CommGroup {
    fn wait_for_join_event(&self, ctx: *mut std::ffi::c_void) {
        // `fi_eq_read` targets `self.eq`, a separate fabric object from the CQ -- it needs
        // no lock of its own. Only the `self.progress()` call below touches the CQ, so it
        // alone needs `self.lock`, and it must take it via `try_lock` (not a blocking
        // `.lock()`) -- another thread may be holding it for its own blocking wait (e.g.
        // `Ticket::block`/`wait_for_cntr`), and losing that race here should just skip this
        // iteration's progress and retry next time around rather than stall this join.
        loop {
            {
                let mut pending = self.pending_join_events.lock();
                if let Some(i) = pending.iter().position(|c| *c == ctx as usize) {
                    pending.swap_remove(i);
                    return;
                }
            }
            let mut event_entry = MaybeUninit::<libfabric_sys::fi_eq_entry>::uninit();
            let mut event_type = 0;
            let ret = unsafe {
                libfabric_sys::inlined_fi_eq_read(
                    self.eq,
                    &mut event_type,
                    event_entry.as_mut_ptr() as *mut libc::c_void,
                    std::mem::size_of::<libfabric_sys::fi_eq_cm_entry>(),
                    0,
                )
            };

            if ret >= 0 {
                let entry = unsafe { event_entry.assume_init() };
                if event_type == libfabric_sys::FI_JOIN_COMPLETE {
                    if entry.context == ctx {
                        return;
                    }
                    self.pending_join_events.lock().push(entry.context as usize);
                } else {
                    eprintln!(
                        "[LAMELLAR WARNING] unexpected EQ event {} while waiting for join",
                        event_type
                    );
                }
            } else if ret != -(libfabric_sys::FI_EAGAIN as isize) {
                panic!("Error reading EQ: {}", ret);
            }

            if let Some(_lock) = self.lock.try_lock() {
                self.progress();
            }
            std::thread::yield_now();
        }
    }

    /// Reads a single CQ entry and dispatches by its `op_context`: a null context means the
    /// completion belongs to the old collective counting scheme (`coll_cnt_issued`/
    /// `coll_cnt_completed`), unchanged from before Stage 2. A non-null context is a
    /// [`TicketCtx`] leaked by a ticket-based (put/get/atomic Future) op — reclaim it and mark
    /// the ticket done. Every caller of `progress()` already holds `self.lock` (or, for the
    /// poll variants, opportunistically `try_lock`s it), so this never races another CQ read.
    ///
    /// A blocking `fi_cq_sread`-based variant of this was tried and reproducibly hung real-HW
    /// `am_latency` runs on the verbs provider -- the same class of problem already documented
    /// on `wait_for_cntr` for `fi_cntr_wait`: this provider's
    /// blocking wait primitives are not reliable here even with a bounded timeout. Reverted;
    /// stick to non-blocking `fi_cq_read`, self-driven by whichever thread is waiting.
    fn progress(&self) {
        let mut cq_entry = MaybeUninit::<libfabric_sys::fi_cq_entry>::uninit();
        let ret = unsafe {
            libfabric_sys::inlined_fi_cq_read(
                self.cq,
                cq_entry.as_mut_ptr() as *mut libc::c_void,
                1,
            )
        };

        if ret >= 0 {
            let entry = unsafe { cq_entry.assume_init() };
            if entry.op_context.is_null() {
                self.coll_cnt_completed.fetch_add(1, Ordering::SeqCst);
            } else {
                let ticket = unsafe { TicketCtx::reclaim(entry.op_context) };
                ticket.complete();
            }
        } else if ret != -(libfabric_sys::FI_EAGAIN as isize) {
            panic!("Error reading CQ: {} ({})", ret, self.decode_cq_error());
        }
    }

    /// Drains and decodes a pending CQ error entry via `fi_cq_readerr`/`fi_cq_strerror`,
    /// mirroring rofi's error-reporting helpers (`rofi_transport_ctx_check_err` et al.,
    /// transport.c). Called only from panic paths after a non-EAGAIN error was observed,
    /// so failure to read a useful entry still degrades to *some* message.
    fn decode_cq_error(&self) -> String {
        let mut err_entry = MaybeUninit::<libfabric_sys::fi_cq_err_entry>::uninit();
        let ret =
            unsafe { libfabric_sys::inlined_fi_cq_readerr(self.cq, err_entry.as_mut_ptr(), 0) };
        if ret <= 0 {
            return format!("fi_cq_readerr failed: {}", ret);
        }
        let entry = unsafe { err_entry.assume_init() };
        let msg = unsafe {
            let ptr = libfabric_sys::inlined_fi_cq_strerror(
                self.cq,
                entry.prov_errno,
                entry.err_data,
                std::ptr::null_mut(),
                0,
            );
            if ptr.is_null() {
                "<no error string>".to_string()
            } else {
                std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned()
            }
        };
        format!(
            "err={} prov_errno={} ({})",
            entry.err, entry.prov_errno, msg
        )
    }

    pub(crate) fn wait_all(&self) {
        trace!("wait_all");
        self.wait_for_tx_cntr();
        trace!("wait_all put done");
        self.wait_for_rx_cntr();
        trace!("wait_all get done");
        self.wait_for_collectives();
        trace!("wait_all collective done");
        trace!("wait_all done");
    }

    fn wait_for_tx_cntr(&self) {
        self.wait_for_cntr(&self.put_cnt, self.put_cntr, "tx");
    }

    fn wait_for_rx_cntr(&self) {
        self.wait_for_cntr(&self.get_cnt, self.get_cntr, "rx");
    }

    // Neither loop below holds `self.lock` across the whole wait -- only around the
    // `self.progress()` calls that actually touch the CQ. A blocking `.lock()` held for
    // the duration would compete unfairly against another thread's own blocking wait
    // (e.g. `Ticket::block`/`wait_for_cntr`) and can starve for the full wait (observed
    // live: a multi-minute hang in the analogous `wait_for_join_event` case, fixed the
    // same way -- try_lock and just skip progress this iteration on contention).
    fn wait_for_collectives(&self) {
        let mut prev_expected_cnt = self.coll_cnt_issued.load(Ordering::SeqCst);
        let mut old_cnt = self.coll_cnt_completed.load(Ordering::SeqCst);
        let mut expected_cnt = self.coll_cnt_issued.load(Ordering::SeqCst);
        let mut cur_cnt = self.coll_cnt_completed.load(Ordering::SeqCst);

        while cur_cnt < expected_cnt || prev_expected_cnt < expected_cnt || cur_cnt != old_cnt {
            prev_expected_cnt = expected_cnt;
            old_cnt = cur_cnt;
            if let Some(_lock) = self.lock.try_lock() {
                self.progress();
            }
            while self.coll_cnt_completed.load(Ordering::SeqCst)
                < self.coll_cnt_issued.load(Ordering::SeqCst)
            {
                if let Some(_lock) = self.lock.try_lock() {
                    self.progress();
                }
                std::thread::yield_now();
            }

            cur_cnt = self.coll_cnt_completed.load(Ordering::SeqCst);
            expected_cnt = self.coll_cnt_issued.load(Ordering::SeqCst);
            std::thread::yield_now();
        }
    }

    /// Every `fi_cntr_read`/`fi_cntr_readerr`/`fi_cntr_wait` runs `cntr->progress` ->
    /// `ep->progress`, which for rxm with FI_COLLECTIVE is `rxm_ep_progress_coll`
    /// (libfabric 1.22 rxm_ep.c/util_cntr.c) -- it walks and frees the coll provider's
    /// unlocked work queues exactly like `fi_cq_read` does. So cntr access must hold
    /// `self.lock` like `progress()`; unlocked reads raced the CQ path and double-freed
    /// coll work items (glibc heap corruption). Returns None on lock contention.
    fn try_progress_read_cntr(&self, cntr: *mut libfabric_sys::fid_cntr) -> Option<u64> {
        let _lock = self.lock.try_lock()?;
        self.progress();
        Some(unsafe { libfabric_sys::inlined_fi_cntr_read(cntr) as u64 })
    }

    fn read_cntr_locked(&self, cntr: *mut libfabric_sys::fid_cntr) -> u64 {
        let _lock = self.lock.lock();
        unsafe { libfabric_sys::inlined_fi_cntr_read(cntr) as u64 }
    }

    // Pure polling: no `fi_cntr_wait` (it would have to hold `self.lock` for its whole
    // timeout, starving other waiters -- the old starvation hang). Whoever gets the lock
    // progresses the CQ and reads the cntr; everyone else yields and retries.
    fn wait_for_cntr(&self, pending: &AtomicU64, cntr: *mut libfabric_sys::fid_cntr, _dir: &str) {
        let mut prev_expected_cnt = pending.load(Ordering::SeqCst);
        let mut old_cnt = self.read_cntr_locked(cntr);
        let mut expected_cnt = pending.load(Ordering::SeqCst);
        let mut cur_cnt = self.read_cntr_locked(cntr);

        while cur_cnt < expected_cnt || prev_expected_cnt < expected_cnt || cur_cnt != old_cnt {
            prev_expected_cnt = expected_cnt;
            old_cnt = cur_cnt;

            loop {
                if let Some(c) = self.try_progress_read_cntr(cntr) {
                    cur_cnt = c;
                    break;
                }
                std::thread::yield_now();
            }
            expected_cnt = pending.load(Ordering::SeqCst);
            std::thread::yield_now();
        }
    }

    // Non-blocking, single-poll check. `cntr` is a bulk completion COUNT shared by every
    // issuer on this CommGroup, not a per-request handle, so completion order across issuers
    // is not guaranteed. Must read `cntr` (completions) first, then `pending` (issued count)
    // second, fresh every call: since `pending` only grows, sampling it strictly after the
    // cntr read guarantees the target is >= true issued-count at read time, so `completed >=
    // issued` proves every op issued as of this call (including this poll's own, since it was
    // issued before this call started) has completed — regardless of completion order. Never
    // cache/persist `pending` across polls and compare a stale target to a fresh cntr read.
    fn poll_wait_for_cntr(
        &self,
        pending: &AtomicU64,
        cntr: *mut libfabric_sys::fid_cntr,
    ) -> Poll<()> {
        let completed = match self.try_progress_read_cntr(cntr) {
            Some(c) => c,
            None => return Poll::Pending,
        };
        let issued = pending.load(Ordering::SeqCst);
        if completed >= issued {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    fn poll_wait_for_tx_cntr(&self) -> Poll<()> {
        self.poll_wait_for_cntr(&self.put_cnt, self.put_cntr)
    }

    fn poll_wait_for_rx_cntr(&self) -> Poll<()> {
        self.poll_wait_for_cntr(&self.get_cnt, self.get_cntr)
    }

    // Same fresh-read-order requirement as poll_wait_for_cntr: completed-count first,
    // issued-count second.
    fn poll_wait_for_collectives(&self) -> Poll<()> {
        if let Some(_lock) = self.lock.try_lock() {
            self.progress();
        }
        let completed = self.coll_cnt_completed.load(Ordering::SeqCst);
        let issued = self.coll_cnt_issued.load(Ordering::SeqCst);
        if completed >= issued {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    /// Unlike RMA issue (Stage 3), the collective issue call itself MUST hold `self.lock`:
    /// collectives are implemented by the coll peer provider (libfabric 1.22 `prov/coll`),
    /// whose issue path (`coll_progress_work`) and progress path (`coll_ep_progress`, run
    /// inside `fi_cq_read`) both mutate the endpoint's `coll_ready_queue`/op work queues
    /// with no locking of their own, `FI_THREAD_SAFE` notwithstanding. Every `progress()`
    /// caller already holds `self.lock`, so holding it here serializes the two. Only the
    /// issue is locked; the completion wait below still uses `try_lock` progress. An
    /// `FI_EAGAIN` retry self-progresses under the same held lock.
    fn post_collective<F>(&self, blocking: bool, mut fun: F)
    where
        F: FnMut() -> isize,
    {
        // Non-blocking issues take it too (released right after issue) so no new
        // collective can slip in while a blocking waiter holds it; `issued == cnt` for the
        // whole wait, making `completed >= cnt` exact.
        let mut coll_guard = Some(self.coll_lock.lock());
        let cnt = self.coll_cnt_issued.fetch_add(1, Ordering::SeqCst) + 1;
        loop {
            let ret = {
                let _lock = self.lock.lock();
                let ret = fun();
                if ret == -(libfabric_sys::FI_EAGAIN as isize) {
                    self.progress();
                }
                ret
            };
            if ret >= 0 {
                break;
            } else if ret == -(libfabric_sys::FI_EAGAIN as isize) {
                std::thread::yield_now();
            } else {
                panic!("Error posting collective: {}", ret);
            }
        }
        if !blocking {
            coll_guard.take();
        }
        if blocking {
            loop {
                if self.coll_cnt_completed.load(Ordering::SeqCst) >= cnt {
                    break;
                }
                if let Some(_lock) = self.lock.try_lock() {
                    self.progress();
                }
                std::thread::yield_now();
            }
        }
    }

    /// Issue for FI_INJECT-class ops (`fi_inject_write`, `fi_inject_atomic`). No CQ entry
    /// is generated, but the bound write counter still increments: with a counter bound,
    /// rxm (libfabric 1.22 `rxm_ep_inject_write`/`rxm_ep_rma_inject_common`) always takes
    /// the emulated-inject path (tx buf + `fi_writemsg(FI_COMPLETION)`) and bumps CNTR_WR in
    /// `rxm_finish_rma`; inject atomics bump it on the atomic response. So these are counted
    /// in `put_cnt` like any other write, which is what lets `wait_all`/`thread_wait` cover
    /// them (previously uncounted, so a waiter could return with these still in flight).
    ///
    /// Counting discipline for `put_cnt`/`get_cnt` (all post_* paths): reserve the count
    /// *before* issuing, never resync it from the hardware counter. Every op bumping a
    /// counter is counted, so `cntr <= cnt` always holds and `cntr >= cnt` means every op
    /// issued so far has completed. The old post-issue `fetch_add` + `fetch_max(cntr)`
    /// resync could strand waiters: an op completing before its own `fetch_add` lets another
    /// thread's `fetch_max` lift `cnt` to the counter, and the late `fetch_add` then pushes
    /// `cnt` one past anything the counter will ever reach.
    fn post_put_inject<F>(&self, mut fun: F)
    where
        F: FnMut() -> isize,
    {
        self.put_cnt.fetch_add(1, Ordering::SeqCst);
        loop {
            let ret = fun();
            if ret == 0 {
                break;
            } else if ret == -(libfabric_sys::FI_EAGAIN as isize) {
                if let Some(_lock) = self.lock.try_lock() {
                    self.progress();
                }
                std::thread::yield_now();
            } else {
                panic!("Error posting put: {}", ret);
            }
        }
    }

    /// See [`Self::post_collective`] for the Stage 3 lock/progress rationale.
    fn post_put<F>(&self, blocking: bool, mut fun: F) -> u64
    where
        F: FnMut() -> isize,
    {
        // Reserve before issue -- see `post_put_inject` for the counting discipline.
        let cnt = self.put_cnt.fetch_add(1, Ordering::SeqCst) + 1;
        loop {
            let ret = fun();
            if ret == 0 {
                break;
            } else if ret == -(libfabric_sys::FI_EAGAIN as isize) {
                if let Some(_lock) = self.lock.try_lock() {
                    self.progress();
                }
                std::thread::yield_now();
            } else {
                panic!("Error posting put: {}", ret);
            }
        }
        if blocking {
            // Must go through wait_for_cntr, never a raw cntr read/wait: cntr access
            // drives coll progress and needs `self.lock` (see try_progress_read_cntr).
            self.wait_for_cntr(&self.put_cnt, self.put_cntr, "tx");
        }
        cnt
    }

    /// See [`Self::post_collective`] for the Stage 3 lock/progress rationale.
    fn post_get<F>(&self, blocking: bool, mut fun: F) -> u64
    where
        F: FnMut() -> isize,
    {
        // Reserve before issue -- see `post_put_inject` for the counting discipline.
        let new_cnt = self.get_cnt.fetch_add(1, Ordering::SeqCst) + 1;
        loop {
            let ret = fun();
            if ret >= 0 {
                break;
            } else if ret == -(libfabric_sys::FI_EAGAIN as isize) {
                if let Some(_lock) = self.lock.try_lock() {
                    self.progress();
                }
                std::thread::yield_now();
            } else {
                panic!("Error posting get: {}", ret);
            }
        }
        trace!(target: "libfabric-sys", "done posting get {}", new_cnt);

        if blocking {
            // See post_put's comment above: must go through wait_for_cntr.
            self.wait_for_cntr(&self.get_cnt, self.get_cntr, "rx");
        }
        new_cnt
    }

    /// Issue path for ticket-based ops (msg-variant RMA/atomic calls with `FI_COMPLETION` set).
    /// The op's own completion is tracked via the `Ticket` leaked into its context and
    /// reclaimed by `progress()`. The hardware cntr still increments for these ops (the cntr
    /// bind is per-EP-per-capability, not call-variant), so they are also counted in the
    /// matching software count -- `pending` is `put_cnt` for writes/non-fetching atomics
    /// (CNTR_WR) and `get_cnt` for reads/fetch/compare atomics (CNTR_RD) -- which keeps
    /// `wait_all`/`poll_wait_for_*_cntr` exact and lets them cover ticketed ops too. See
    /// `post_put_inject` for the counting discipline.
    /// See [`Self::post_collective`] for the Stage 3 lock/progress rationale.
    fn post_ticketed<F>(&self, pending: &AtomicU64, mut fun: F)
    where
        F: FnMut() -> isize,
    {
        pending.fetch_add(1, Ordering::SeqCst);
        loop {
            let ret = fun();
            if ret >= 0 {
                return;
            } else if ret == -(libfabric_sys::FI_EAGAIN as isize) {
                if let Some(_lock) = self.lock.try_lock() {
                    self.progress();
                }
                std::thread::yield_now();
            } else {
                panic!("Error posting ticketed op: {}", ret);
            }
        }
    }
}

#[derive(Clone)]
pub(crate) enum LibfabricSysOptMem {
    Mmap(Arc<memmap::MmapMut>),
    #[cfg(feature = "enable-on-node-shmem")]
    Shmem(Arc<ShmemSegment>),
}

impl LibfabricSysOptMem {
    fn as_ptr(&self) -> *mut u8 {
        match self {
            LibfabricSysOptMem::Mmap(mem) => mem.as_ptr() as *mut u8,
            #[cfg(feature = "enable-on-node-shmem")]
            LibfabricSysOptMem::Shmem(segment) => segment.base_ptr(),
        }
    }

    fn len(&self) -> usize {
        match self {
            LibfabricSysOptMem::Mmap(mem) => mem.len(),
            #[cfg(feature = "enable-on-node-shmem")]
            LibfabricSysOptMem::Shmem(segment) => segment.len(),
        }
    }

    #[allow(dead_code)]
    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }

    #[allow(dead_code)]
    fn as_mut_slice(&self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_ptr(), self.len()) }
    }
}

pub(crate) struct OptOfi {
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    #[cfg(feature = "enable-on-node-shmem")]
    same_node_pes: Vec<bool>,
    #[cfg(feature = "enable-on-node-shmem")]
    disable_on_node_shmem: bool,
    #[cfg(feature = "enable-on-node-shmem")]
    job_id: usize,
    domain: *mut libfabric_sys::fid_domain,
    #[allow(dead_code)] // WIP: held for ownership/lifetime, not yet read
    fabric: *mut libfabric_sys::fid_fabric,
    _my_pmi: Arc<dyn Pmi>,
    alloc_manager: Arc<AllocInfoManager>,
    comm_group: CommGroup,
    /// Runtime (rt_alloc) sub-allocation pool, shared by any `LibfabricSysOptAlloc`
    /// reachable via its `ofi: Arc<OptOfi>` field. Lives here (rather than on
    /// LibfabricSysOptComm) so that `LibfabricSysOptAlloc`/`OneSidedLibfabricSysOptAlloc` --
    /// which have no other path back to LibfabricSysOptComm -- can still reach an
    /// already-registered scratch pool (e.g. from inside `get_buffer`).
    ///
    /// NOTE: each `LibfabricSysOptAlloc` stored here holds its own `Arc<OptOfi>`
    /// clone pointing back at this very `OptOfi`, i.e. this field creates a
    /// genuine Arc reference cycle (mirrors the existing `alloc_manager` cycle
    /// broken via `clear_allocs()`). It MUST be `.clear()`-ed during teardown
    /// (see `Drop for LibfabricSysOptComm`) or `OptOfi` would never be freed.
    pub(crate) runtime_allocs: RwLock<Vec<(LibfabricSysOptAlloc, BTreeAlloc)>>,
    /// Whether the provider supports `FI_ALLGATHER` (probed once at startup via
    /// `fi_query_collective`). When true, `exchange_mr_info` uses the OFI collective
    /// path (`collective_exchange_mr_info`); otherwise it falls back to PMI
    /// (`manual_exchange_mr_info`). Mirrors `libfabric_sys_lamellae::Ofi::mr_exchange_via_collective`.
    mr_exchange_via_collective: bool,
    /// Multicast groups used by `collective_exchange_mr_info` and by every allocation's
    /// `mcast_group`, one per distinct PE set, joined on first use and reused after. Must be cached: the
    /// coll provider (libfabric 1.22 `prov/coll`) hands out group ids from a fixed 256-entry
    /// per-endpoint mask on `fi_join_collective` and never returns them on `fi_close`, so a
    /// join per allocation exhausts the id space after ~254 allocations (later joins then all get
    /// id 256, and marking it writes one byte past the 32-byte mask). Cleared in
    /// `clear_barrier` (before the endpoint is closed).
    mr_exchange_groups: Mutex<HashMap<Vec<usize>, Arc<RawMcGroup>>>,
    /// `LAMELLAR_RDMA_STAGING` (default on): heap-backed RDMA local buffers are
    /// staged through `runtime_allocs` so rxm never registers them on the fly.
    staging_enabled: bool,
    staging_fallbacks: AtomicUsize,
    staging_warned: AtomicBool,
    /// Guards `final_teardown` so it runs exactly once no matter who calls it:
    /// LibfabricSysOptComm::drop calls it explicitly (on a known-good thread),
    /// and Drop::drop below calls it too as a fallback for any other path that
    /// drops the last Arc<OptOfi> without going through comm.rs first.
    teardown_done: AtomicBool,
    /// Vestigial: was a contention signal for an earlier `Ticket::block()` design (now
    /// self-driven unconditionally, see `Ticket::block()`). Declared/initialized but no
    /// longer read anywhere.
    #[allow(dead_code)]
    blocked_count: AtomicUsize,
}

unsafe impl Send for OptOfi {}
unsafe impl Sync for OptOfi {}

impl std::fmt::Debug for OptOfi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptOfi")
            .field("num_pes", &self.num_pes)
            .field("my_pe", &self.my_pe)
            .finish()
    }
}

impl OptOfi {
    pub(crate) fn new(provider: Option<&str>, domain: Option<&str>) -> FabricResult<Arc<Self>> {
        let my_pmi = Arc::new(PmiBuilder::init().map_err(|e| {
            eprintln!("Error initializing PMI: {:?}", e);
            FabricError::InitError(1)
        })?);

        let num_pes = my_pmi.ranks().len();
        if env::var_os("FI_UNIVERSE").is_none() {
            env::set_var("FI_UNIVERSE", num_pes.to_string());
        }

        if env::var_os("FI_MR_CACHE_MAX_SIZE").is_none() {
            if let Some(total_bytes) = node_total_memory_bytes() {
                env::set_var("FI_MR_CACHE_MAX_SIZE", total_bytes.to_string());
            } else {
                eprintln!(
                    "Warning: unable to determine total system memory for FI_MR_CACHE_MAX_SIZE"
                );
            }
        }

        #[cfg(feature = "enable-on-node-shmem")]
        let disable_on_node_shmem = config().disable_on_node_shmem.unwrap_or(false);
        #[cfg(feature = "enable-on-node-shmem")]
        let mut same_node_pes = vec![false; num_pes];
        #[cfg(feature = "enable-on-node-shmem")]
        if !disable_on_node_shmem {
            for pe in my_pmi.ranks_on_node(my_pmi.node()) {
                if pe < same_node_pes.len() {
                    same_node_pes[pe] = true;
                }
            }
        }
        #[cfg(feature = "enable-on-node-shmem")]
        let job_id = my_pmi.job_id();

        let info = unsafe {
            let hints = libfabric_sys::inlined_fi_allocinfo();
            (*hints).caps = (libfabric_sys::FI_RMA
                | libfabric_sys::FI_ATOMIC
                | libfabric_sys::FI_COLLECTIVE) as u64;
            (*hints).mode = libfabric_sys::FI_CONTEXT;
            (*(*hints).ep_attr).type_ = libfabric_sys::fi_ep_type_FI_EP_RDM;
            (*(*hints).domain_attr).threading = libfabric_sys::fi_threading_FI_THREAD_SAFE;
            // (*(*hints).domain_attr).control_progress = libfabric_sys::fi_progress_FI_PROGRESS_AUTO as u8;
            (*(*hints).domain_attr).data_progress = libfabric_sys::fi_progress_FI_PROGRESS_MANUAL;
            // verbs doesn't need FI_MR_ENDPOINT (that's for rxd/shm-style providers);
            // bind_and_enable_mr_if_required() below stays as a no-op guard regardless.
            (*(*hints).domain_attr).mr_mode = (libfabric_sys::FI_MR_PROV_KEY
                | libfabric_sys::FI_MR_VIRT_ADDR
                | libfabric_sys::FI_MR_ALLOCATED)
                as i32;
            (*(*hints).domain_attr).resource_mgmt = libfabric_sys::fi_resource_mgmt_FI_RM_ENABLED;
            (*(*hints).tx_attr).tclass = libfabric_sys::FI_TC_LOW_LATENCY;
            (*(*hints).tx_attr).op_flags = (libfabric_sys::FI_DELIVERY_COMPLETE) as u64;
            (*hints).addr_format = libfabric_sys::FI_FORMAT_UNSPEC;
            let version = 1 << 16 | 22;
            let mut c_info = MaybeUninit::<*mut libfabric_sys::fi_info>::uninit();
            let ret = libfabric_sys::fi_getinfo(
                version,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                0,
                hints,
                c_info.as_mut_ptr(),
            );
            if ret != 0 {
                libfabric_sys::fi_freeinfo(hints);
                eprintln!("Error querying libfabric providers: {ret}");
                Err(FabricError::InitError(-ret as u32))?;
            }
            let info = c_info.assume_init();
            // verbs-only backend: no provider-fallback ceremony, always require "verbs".
            let _ = provider;
            let mut curr_info = info;
            while !curr_info.is_null() {
                if !std::ffi::CStr::from_ptr((*curr_info).fabric_attr.as_ref().unwrap().prov_name)
                    .to_str()
                    .unwrap()
                    .split(';')
                    .any(|p| p == "verbs")
                {
                    curr_info = (*curr_info).next;
                    continue;
                }
                if !domain.is_none()
                    && !std::ffi::CStr::from_ptr((*curr_info).domain_attr.as_ref().unwrap().name)
                        .to_str()
                        .unwrap()
                        .split(';')
                        .any(|d| d == domain.unwrap())
                {
                    curr_info = (*curr_info).next;
                    continue;
                }
                break;
            }
            if curr_info.is_null() {
                libfabric_sys::fi_freeinfo(hints);
                libfabric_sys::fi_freeinfo(info);
                eprintln!("No libfabric \"verbs\" provider available (domain={domain:?})");
                Err(FabricError::InitError(libfabric_sys::FI_ENODATA))?;
            }
            let ret: *mut libfabric_sys::fi_info = libfabric_sys::fi_dupinfo(curr_info);
            libfabric_sys::fi_freeinfo(hints);
            libfabric_sys::fi_freeinfo(info);
            if ret.is_null() {
                eprintln!("Error duplicating selected libfabric provider info");
                Err(FabricError::InitError(libfabric_sys::FI_ENOMEM))?;
            }

            let base_caps = (libfabric_sys::FI_RMA
                | libfabric_sys::FI_WRITE
                | libfabric_sys::FI_READ
                | libfabric_sys::FI_REMOTE_WRITE
                | libfabric_sys::FI_REMOTE_READ
                | libfabric_sys::FI_ATOMIC
                | libfabric_sys::FI_COLLECTIVE) as u64;
            (*ret).caps = base_caps;
            (*ret).mode = 0;
            (*(*ret).tx_attr).mode = 0;
            (*(*ret).tx_attr).size = 1024;
            (*(*ret).tx_attr).caps = base_caps;
            (*(*ret).rx_attr).mode = 0;
            (*(*ret).rx_attr).size = 1024;
            (*(*ret).rx_attr).caps = (libfabric_sys::FI_RECV | libfabric_sys::FI_COLLECTIVE) as u64;

            ret
        };

        let fabric = unsafe {
            let mut fabric = MaybeUninit::<*mut libfabric_sys::fid_fabric>::uninit();
            let ret = libfabric_sys::fi_fabric(
                (*info).fabric_attr,
                fabric.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if ret != 0 {
                eprintln!("Error creating fabric: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            fabric.assume_init()
        };

        let domain = unsafe {
            let mut domain = MaybeUninit::<*mut libfabric_sys::fid_domain>::uninit();
            let ret = libfabric_sys::inlined_fi_domain(
                fabric,
                info,
                domain.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if ret != 0 {
                eprintln!("Error creating domain: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            domain.assume_init()
        };

        let eq = unsafe {
            let mut eq = MaybeUninit::<*mut libfabric_sys::fid_eq>::uninit();
            let mut eq_attr = libfabric_sys::fi_eq_attr {
                size: 0,
                flags: 0,
                wait_obj: libfabric_sys::fi_wait_obj_FI_WAIT_UNSPEC,
                signaling_vector: 0,
                wait_set: std::ptr::null_mut(),
            };

            let ret = libfabric_sys::inlined_fi_eq_open(
                fabric,
                &mut eq_attr,
                eq.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if ret != 0 {
                eprintln!("Error creating EQ: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            eq.assume_init()
        };

        let cq = unsafe {
            let mut cq = MaybeUninit::<*mut libfabric_sys::fid_cq>::uninit();
            let mut cq_attr = libfabric_sys::fi_cq_attr {
                size: (*(*info).rx_attr).size,
                flags: 0,
                format: libfabric_sys::fi_cq_format_FI_CQ_FORMAT_CONTEXT,
                wait_obj: libfabric_sys::fi_wait_obj_FI_WAIT_UNSPEC,
                signaling_vector: 0,
                wait_cond: libfabric_sys::fi_cq_wait_cond_FI_CQ_COND_NONE,
                wait_set: std::ptr::null_mut(),
            };
            let ret = libfabric_sys::inlined_fi_cq_open(
                domain,
                &mut cq_attr,
                cq.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if ret != 0 {
                eprintln!("Error creating CQ: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            cq.assume_init()
        };

        let av = unsafe {
            let mut av = MaybeUninit::<*mut libfabric_sys::fid_av>::uninit();
            let mut av_attr = libfabric_sys::fi_av_attr {
                type_: libfabric_sys::fi_av_type_FI_AV_UNSPEC,
                rx_ctx_bits: 0,
                count: 0,
                ep_per_node: 0,
                name: std::ptr::null(),
                map_addr: std::ptr::null_mut(),
                flags: 0,
            };
            let ret = libfabric_sys::inlined_fi_av_open(
                domain,
                &mut av_attr,
                av.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if ret != 0 {
                eprintln!("Error creating AV: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            av.assume_init()
        };

        let put_cntr = unsafe {
            let mut cntr = MaybeUninit::<*mut libfabric_sys::fid_cntr>::uninit();
            let mut cntr_attr = libfabric_sys::fi_cntr_attr {
                events: libfabric_sys::fi_cntr_events_FI_CNTR_EVENTS_COMP,
                wait_obj: libfabric_sys::fi_wait_obj_FI_WAIT_UNSPEC,
                wait_set: std::ptr::null_mut(),
                flags: 0,
            };
            let ret = libfabric_sys::inlined_fi_cntr_open(
                domain,
                &mut cntr_attr,
                cntr.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if ret != 0 {
                eprintln!("Error creating put counter: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            cntr.assume_init()
        };

        let get_cntr = unsafe {
            let mut cntr = MaybeUninit::<*mut libfabric_sys::fid_cntr>::uninit();
            let mut cntr_attr = libfabric_sys::fi_cntr_attr {
                events: libfabric_sys::fi_cntr_events_FI_CNTR_EVENTS_COMP,
                wait_obj: libfabric_sys::fi_wait_obj_FI_WAIT_UNSPEC,
                wait_set: std::ptr::null_mut(),
                flags: 0,
            };
            let ret = libfabric_sys::inlined_fi_cntr_open(
                domain,
                &mut cntr_attr,
                cntr.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if ret != 0 {
                eprintln!("Error creating put counter: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            cntr.assume_init()
        };

        let ep = unsafe {
            let mut ep = MaybeUninit::<*mut libfabric_sys::fid_ep>::uninit();
            let ret = libfabric_sys::inlined_fi_endpoint(
                domain,
                info,
                ep.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if ret != 0 {
                eprintln!("Error creating endpoint: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            ep.assume_init()
        };

        unsafe {
            let ret = libfabric_sys::inlined_fi_ep_bind(ep, &mut (*eq).fid, 0);
            if ret != 0 {
                eprintln!("Error binding EQ: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            let flags = (libfabric_sys::FI_TRANSMIT | libfabric_sys::FI_RECV) as u64
                | libfabric_sys::FI_SELECTIVE_COMPLETION;
            let ret = libfabric_sys::inlined_fi_ep_bind(ep, &mut (*cq).fid, flags);
            if ret != 0 {
                eprintln!("Error binding CQ: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            let ret = libfabric_sys::inlined_fi_ep_bind(ep, &mut (*av).fid, 0);
            if ret != 0 {
                eprintln!("Error binding AV: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            let ret = libfabric_sys::inlined_fi_ep_bind(
                ep,
                &mut (*put_cntr).fid,
                libfabric_sys::FI_WRITE as u64,
            );
            if ret != 0 {
                eprintln!("Error binding put counter: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            let ret = libfabric_sys::inlined_fi_ep_bind(
                ep,
                &mut (*get_cntr).fid,
                libfabric_sys::FI_READ as u64,
            );
            if ret != 0 {
                eprintln!("Error binding get counter: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
        }

        unsafe {
            let ret = libfabric_sys::inlined_fi_enable(ep);
            if ret != 0 {
                eprintln!("Error enabling endpoint: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
        }
        let address_bytes = unsafe {
            let mut len = 0;
            let ret =
                libfabric_sys::inlined_fi_getname(&mut (*ep).fid, std::ptr::null_mut(), &mut len);
            if ret != -(libfabric_sys::FI_ETOOSMALL as i32) {
                eprintln!("Error getting endpoint name: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            let mut addr = vec![0u8; len];
            let ret = libfabric_sys::inlined_fi_getname(
                &mut (*ep).fid,
                addr.as_mut_ptr().cast(),
                &mut len,
            );
            if ret != 0 {
                eprintln!("Error getting endpoint name: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            addr
        };

        my_pmi
            .put(&format!("epname"), &address_bytes)
            .expect("PMI put failed during epname exchange");
        my_pmi
            .exchange()
            .expect("PMI exchange failed during epname exchange");

        let unmapped_addresses: Vec<Vec<u8>> = my_pmi
            .ranks()
            .iter()
            .map(|r| {
                my_pmi
                    .get(&format!("epname"), &r)
                    .expect("PMI get failed during epname exchange")
            })
            .collect();

        let mapped_addresses = unsafe {
            let mut mapped_addresses = vec![0u64; unmapped_addresses.len()];
            let total_size = unmapped_addresses
                .iter()
                .fold(0, |acc, unmapped_addresses| acc + unmapped_addresses.len());
            let mut serialized: Vec<u8> = Vec::with_capacity(total_size);
            for a in unmapped_addresses {
                serialized.extend(a.iter())
            }
            let ret = libfabric_sys::inlined_fi_av_insert(
                av,
                serialized.as_mut_ptr().cast(),
                mapped_addresses.len(),
                mapped_addresses.as_mut_ptr(),
                0,
                std::ptr::null_mut(),
            );
            if ret < 0 {
                eprintln!("Error inserting addresses into AV: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            } else if ret as usize != mapped_addresses.len() {
                eprintln!(
                    "Error inserting addresses into AV: number of addresses inserted = {}; number of addresses given = {}",
                    ret,
                    mapped_addresses.len()
                );
                Err(FabricError::InitError(ret as u32))?;
            }
            mapped_addresses
        };

        let comm_group = CommGroup {
            mapped_addresses,
            ep,
            cq,
            put_cntr,
            get_cntr,
            coll_cnt_completed: AtomicU64::new(0),
            coll_cnt_issued: AtomicU64::new(0),
            av,
            eq,
            info_entry: info,
            put_cnt: AtomicU64::new(0),
            get_cnt: AtomicU64::new(0),
            lock: Mutex::new(()),
            coll_lock: Mutex::new(()),
            join_lock: Mutex::new(()),
            pending_join_events: Mutex::new(Vec::new()),
            barrier_impl: RwLock::new(BarrierImpl::Pmi(my_pmi.clone())),
        };

        // Probed like `libfabric_sys_lamellae`. The PMI fallback is expensive here: every
        // `PMIx_Commit` re-sends every `mr_info_{N}` key this process ever put, so per-alloc
        // cost grows with the number of allocations made so far. Verbs (via the coll peer
        // provider) does support FI_ALLGATHER; earlier hangs on this path came from issuing
        // join/allgather without `comm_group.lock` (the coll provider's work queues are
        // unlocked, see `post_collective`) and from joining a new group per allocation (see
        // `mr_exchange_groups`), both fixed.
        let mr_exchange_via_collective = {
            let data_type = rust_type_to_fi_type::<u64>().expect("u64 must map to an fi_datatype");
            let mut attr = libfabric_sys::fi_collective_attr {
                op: 0,
                datatype: data_type,
                datatype_attr: libfabric_sys::fi_atomic_attr { count: 0, size: 0 },
                max_members: 0,
                mode: 0,
            };
            (unsafe {
                libfabric_sys::inlined_fi_query_collective(
                    domain,
                    libfabric_sys::fi_collective_op_FI_ALLGATHER,
                    &mut attr,
                    0,
                )
            }) == 0
        };

        let alloc_manager = AllocInfoManager::new();
        let ofi = Arc::new(Self {
            num_pes,
            my_pe: my_pmi.rank(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_pes,
            #[cfg(feature = "enable-on-node-shmem")]
            disable_on_node_shmem,
            #[cfg(feature = "enable-on-node-shmem")]
            job_id,
            _my_pmi: my_pmi,
            // info_entry,
            fabric: fabric,
            domain,
            alloc_manager: Arc::new(alloc_manager),
            comm_group,
            runtime_allocs: RwLock::new(Vec::new()),
            mr_exchange_via_collective,
            mr_exchange_groups: Mutex::new(HashMap::new()),
            staging_enabled: config().rdma_staging.unwrap_or(true),
            staging_fallbacks: AtomicUsize::new(0),
            staging_warned: AtomicBool::new(false),
            teardown_done: AtomicBool::new(false),
            blocked_count: AtomicUsize::new(0),
        });

        Ok(ofi)
    }

    fn atomic_avail_inner<T: 'static>(&self) -> bool {
        let cg = &self.comm_group;
        let data_type = if let Some(dt) = rust_type_to_fi_type::<T>() {
            dt
        } else {
            return false;
        };
        let mut count: usize = 0;
        let mut avail = true;
        let ret = unsafe {
            libfabric_sys::inlined_fi_atomicvalid(
                cg.ep,
                data_type,
                libfabric_sys::fi_op_FI_SUM,
                &mut count as *mut usize,
            )
        };
        avail &= if ret != 0 { false } else { true };

        let ret = unsafe {
            libfabric_sys::inlined_fi_fetch_atomicvalid(
                cg.ep,
                data_type,
                libfabric_sys::fi_op_FI_ATOMIC_READ,
                &mut count as *mut usize,
            )
        };
        avail &= if ret != 0 { false } else { true };

        let ret = unsafe {
            libfabric_sys::inlined_fi_compare_atomicvalid(
                cg.ep,
                data_type,
                libfabric_sys::fi_op_FI_CSWAP,
                &mut count as *mut usize,
            )
        };
        avail &= if ret != 0 { false } else { true };
        avail
    }

    fn atomic_op_avail_inner<T: 'static>(&self, op: AtomicOpKind) -> bool {
        let cg = &self.comm_group;
        let data_type = if let Some(dt) = rust_type_to_fi_type::<T>() {
            dt
        } else {
            return false;
        };
        let mut count: usize = 0;
        let fi_op = match op {
            AtomicOpKind::Min => libfabric_sys::fi_op_FI_MIN,
            AtomicOpKind::Max => libfabric_sys::fi_op_FI_MAX,
            AtomicOpKind::Sum => libfabric_sys::fi_op_FI_SUM,
            AtomicOpKind::Prod => libfabric_sys::fi_op_FI_PROD,
            AtomicOpKind::BitAnd => libfabric_sys::fi_op_FI_BAND,
            AtomicOpKind::BitOr => libfabric_sys::fi_op_FI_BOR,
            AtomicOpKind::BitXor => libfabric_sys::fi_op_FI_BXOR,
            AtomicOpKind::Read => libfabric_sys::fi_op_FI_ATOMIC_READ,
            AtomicOpKind::Write => libfabric_sys::fi_op_FI_ATOMIC_WRITE,
            AtomicOpKind::Cas => libfabric_sys::fi_op_FI_CSWAP,
        };

        let mut avail = true;
        if matches!(op, AtomicOpKind::Cas) {
            let ret = unsafe {
                libfabric_sys::inlined_fi_compare_atomicvalid(
                    cg.ep,
                    data_type,
                    fi_op,
                    &mut count as *mut usize,
                )
            };
            avail &= if ret != 0 { false } else { true };
        } else {
            let ret = unsafe {
                libfabric_sys::inlined_fi_atomicvalid(
                    cg.ep,
                    data_type,
                    fi_op,
                    &mut count as *mut usize,
                )
            };
            avail &= if ret != 0 { false } else { true };

            let ret = unsafe {
                libfabric_sys::inlined_fi_fetch_atomicvalid(
                    cg.ep,
                    data_type,
                    fi_op,
                    &mut count as *mut usize,
                )
            };
            avail &= if ret != 0 { false } else { true };
        }
        avail
    }

    pub(crate) fn atomic_avail<T: 'static>(&self) -> bool {
        let id = std::any::TypeId::of::<T>();
        if id == std::any::TypeId::of::<u8>() {
            self.atomic_avail_inner::<u8>()
        } else if id == std::any::TypeId::of::<u16>() {
            self.atomic_avail_inner::<u16>()
        } else if id == std::any::TypeId::of::<u32>() {
            self.atomic_avail_inner::<u32>()
        } else if id == std::any::TypeId::of::<u64>() {
            self.atomic_avail_inner::<u64>()
        } else if id == std::any::TypeId::of::<i8>() {
            self.atomic_avail_inner::<i8>()
        } else if id == std::any::TypeId::of::<i16>() {
            self.atomic_avail_inner::<i16>()
        } else if id == std::any::TypeId::of::<i32>() {
            self.atomic_avail_inner::<i32>()
        } else if id == std::any::TypeId::of::<i64>() {
            self.atomic_avail_inner::<i64>()
        } else if id == std::any::TypeId::of::<usize>() {
            self.atomic_avail_inner::<usize>()
        } else if id == std::any::TypeId::of::<isize>() {
            self.atomic_avail_inner::<isize>()
        } else {
            false
        }
    }

    pub(crate) fn atomic_op_avail<T: 'static>(&self, op: AtomicOp<T>) -> bool {
        let op_kind = match op {
            AtomicOp::Min(_) | AtomicOp::FetchMin(_) => AtomicOpKind::Min,
            AtomicOp::Max(_) | AtomicOp::FetchMax(_) => AtomicOpKind::Max,
            AtomicOp::Sum(_) | AtomicOp::FetchSum(_) => AtomicOpKind::Sum,
            AtomicOp::Sub(_) | AtomicOp::FetchSub(_) => AtomicOpKind::Sum, // Sub can be implemented as Add with negative value
            AtomicOp::Prod(_) | AtomicOp::FetchProd(_) => AtomicOpKind::Prod,
            AtomicOp::BitOr(_) | AtomicOp::FetchBitOr(_) => AtomicOpKind::BitOr,
            AtomicOp::BitXor(_) | AtomicOp::FetchBitXor(_) => AtomicOpKind::BitXor,
            AtomicOp::BitAnd(_) | AtomicOp::FetchBitAnd(_) => AtomicOpKind::BitAnd,
            AtomicOp::Read(_) => AtomicOpKind::Read,
            AtomicOp::Write(_) => AtomicOpKind::Write,
            AtomicOp::Cas => AtomicOpKind::Cas,
        };

        let id = std::any::TypeId::of::<T>();
        if id == std::any::TypeId::of::<u8>() {
            self.atomic_op_avail_inner::<u8>(op_kind)
        } else if id == std::any::TypeId::of::<u16>() {
            self.atomic_op_avail_inner::<u16>(op_kind)
        } else if id == std::any::TypeId::of::<u32>() {
            self.atomic_op_avail_inner::<u32>(op_kind)
        } else if id == std::any::TypeId::of::<u64>() {
            self.atomic_op_avail_inner::<u64>(op_kind)
        } else if id == std::any::TypeId::of::<i8>() {
            self.atomic_op_avail_inner::<i8>(op_kind)
        } else if id == std::any::TypeId::of::<i16>() {
            self.atomic_op_avail_inner::<i16>(op_kind)
        } else if id == std::any::TypeId::of::<i32>() {
            self.atomic_op_avail_inner::<i32>(op_kind)
        } else if id == std::any::TypeId::of::<i64>() {
            self.atomic_op_avail_inner::<i64>(op_kind)
        } else if id == std::any::TypeId::of::<usize>() {
            self.atomic_op_avail_inner::<usize>(op_kind)
        } else if id == std::any::TypeId::of::<isize>() {
            self.atomic_op_avail_inner::<isize>(op_kind)
        } else {
            false
        }
    }

    pub(crate) fn collective_avail<T: 'static>(&self, op: CollectiveOpKind) -> bool {
        let (fi_op, reduce_op) = match op {
            CollectiveOpKind::Barrier => (libfabric_sys::fi_collective_op_FI_BARRIER, None),
            CollectiveOpKind::Broadcast => (libfabric_sys::fi_collective_op_FI_BROADCAST, None),
            CollectiveOpKind::AllToAll => (libfabric_sys::fi_collective_op_FI_ALLTOALL, None),
            CollectiveOpKind::AllReduce(reduce_op) => (
                libfabric_sys::fi_collective_op_FI_ALLREDUCE,
                Some(reduce_op),
            ),
            CollectiveOpKind::AllGather => (libfabric_sys::fi_collective_op_FI_ALLGATHER, None),
            CollectiveOpKind::ReduceScatter(reduce_op) => (
                libfabric_sys::fi_collective_op_FI_REDUCE_SCATTER,
                Some(reduce_op),
            ),
            CollectiveOpKind::Reduce(reduce_op) => {
                (libfabric_sys::fi_collective_op_FI_REDUCE, Some(reduce_op))
            }
            CollectiveOpKind::Scatter => (libfabric_sys::fi_collective_op_FI_SCATTER, None),
            CollectiveOpKind::Gather => (libfabric_sys::fi_collective_op_FI_GATHER, None),
        };

        let data_type = if let Some(dt) = rust_type_to_fi_type::<T>() {
            dt
        } else {
            return false;
        };
        let mut attr = libfabric_sys::fi_collective_attr {
            op: 0,
            datatype: data_type,
            datatype_attr: libfabric_sys::fi_atomic_attr { count: 0, size: 0 },
            max_members: 0,
            mode: 0,
        };

        if let Some(reduce_op) = reduce_op {
            let reduce_fi_op = match reduce_op {
                ReduceOp::Min => libfabric_sys::fi_op_FI_MIN,
                ReduceOp::Max => libfabric_sys::fi_op_FI_MAX,
                ReduceOp::Sum => libfabric_sys::fi_op_FI_SUM,
                ReduceOp::Prod => libfabric_sys::fi_op_FI_PROD,
                ReduceOp::BitAnd => libfabric_sys::fi_op_FI_BAND,
                ReduceOp::BitOr => libfabric_sys::fi_op_FI_BOR,
                ReduceOp::BitXor => libfabric_sys::fi_op_FI_BXOR,
            };
            attr.op = reduce_fi_op;
        }
        return unsafe {
            libfabric_sys::inlined_fi_query_collective(self.domain, fi_op, &mut attr, 0)
        } == 0;
    }

    /// Returns the cached multicast group for `pes`, joining it on first use. Allocations must
    /// go through this rather than `create_mc_group`: see `mr_exchange_groups` (cid exhaustion).
    fn cached_mc_group(&self, pes: &[usize]) -> Arc<RawMcGroup> {
        self.mr_exchange_groups
            .lock()
            .entry(pes.to_vec())
            .or_insert_with(|| self.create_mc_group(pes))
            .clone()
    }

    fn create_mc_group(&self, pes: &[usize]) -> Arc<RawMcGroup> {
        let cg = &self.comm_group;
        let mut av_set_attr = libfabric_sys::fi_av_set_attr {
            flags: 0,
            count: pes.len(),
            start_addr: cg.mapped_addresses[pes[0]],
            end_addr: cg.mapped_addresses[pes[0]],
            stride: 1,
            comm_key_size: 0,
            comm_key: std::ptr::null_mut(),
        };
        let av_set = unsafe {
            let mut av_set = MaybeUninit::<*mut libfabric_sys::fid_av_set>::uninit();
            let res = libfabric_sys::inlined_fi_av_set(
                cg.av,
                &mut av_set_attr,
                av_set.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if res != 0 {
                panic!("Error creating AV set: {}", res);
            }
            av_set.assume_init()
        };

        for pe in pes.iter().skip(1) {
            let ret = unsafe {
                libfabric_sys::inlined_fi_av_set_insert(av_set, cg.mapped_addresses[*pe])
            };
            if ret < 0 {
                panic!("Error inserting address into AV set: {}", ret);
            }
        }

        let mut ctx = libfabric_sys::fi_context2 {
            internal: [std::ptr::null_mut(); 8],
        };

        let av_set_addr = unsafe {
            let mut addr: u64 = 0;
            let ret = libfabric_sys::inlined_fi_av_set_addr(av_set, &mut addr);
            if ret != 0 {
                panic!("Error getting AV set address: {}", ret);
            }
            addr
        };
        // One join in flight per PE: held across issue and completion wait.
        let _join_guard = cg.join_lock.lock();
        let mc = unsafe {
            let mut mc = MaybeUninit::<*mut libfabric_sys::fid_mc>::uninit();
            // Join issue queues coll-provider work unlocked -- serialize it against
            // `progress()` exactly like `post_collective` does.
            let ret = {
                let _lock = cg.lock.lock();
                libfabric_sys::inlined_fi_join_collective(
                    cg.ep,
                    av_set_addr,
                    av_set,
                    0,
                    mc.as_mut_ptr(),
                    (&mut ctx) as *mut libfabric_sys::fi_context2 as *mut libc::c_void,
                )
            };
            if ret != 0 {
                panic!("Error registering memory for MC group: {}", ret);
            }
            cg.wait_for_join_event(
                (&mut ctx) as *mut libfabric_sys::fi_context2 as *mut libc::c_void,
            );
            mc.assume_init()
        };
        Arc::new(RawMcGroup {
            mc,
            av_set,
            coll_addr: av_set_addr,
        })
    }

    fn build_local_mr_info_bytes(&self, mem: &[u8], mr: *mut libfabric_sys::fid_mr) -> Vec<u8> {
        let addr = if unsafe { (*(*self.comm_group.info_entry).domain_attr).mr_mode }
            & (libfabric_sys::FI_MR_VIRT_ADDR | libfabric_sys::fi_mr_mode_FI_MR_BASIC) as i32
            != 0
        {
            mem.as_ptr() as u64
        } else {
            0u64
        };

        let addr_size = std::mem::size_of_val(mem);

        let mut key_bytes = unsafe {
            let mut base_addr = addr;
            let mut key_size = (*(*self.comm_group.info_entry).domain_attr).mr_key_size;
            let mut raw_key = vec![0u8; key_size + std::mem::size_of::<u64>()];
            if (*(*self.comm_group.info_entry).domain_attr).mr_mode
                & (libfabric_sys::FI_MR_RAW as i32)
                != 0
            {
                let err = libfabric_sys::inlined_fi_mr_raw_attr(
                    mr,
                    &mut base_addr,
                    raw_key.as_mut_ptr().cast(),
                    &mut key_size,
                    0,
                );

                if err != 0 {
                    panic!("Error getting raw MR key: {}", err);
                }

                let raw_key_len = raw_key.len();
                raw_key[raw_key_len - std::mem::size_of::<u64>()..].copy_from_slice(
                    std::slice::from_raw_parts(&base_addr as *const u64 as *const u8, 8),
                );
                raw_key
            } else {
                let key = libfabric_sys::inlined_fi_mr_key(mr);
                if key == u64::MAX {
                    panic!("Error getting MR key: {}", key);
                }

                let mut bytes = vec![0; std::mem::size_of::<u64>() + std::mem::size_of::<usize>()];
                bytes[..std::mem::size_of::<u64>()].copy_from_slice(std::slice::from_raw_parts(
                    &key as *const u64 as *const u8,
                    std::mem::size_of::<u64>(),
                ));
                bytes[std::mem::size_of::<u64>()..].copy_from_slice(std::slice::from_raw_parts(
                    &base_addr as *const u64 as *const u8,
                    std::mem::size_of::<usize>(),
                ));

                bytes
            }
        };

        key_bytes.extend(unsafe {
            std::slice::from_raw_parts(
                &addr_size as *const usize as *const u8,
                std::mem::size_of::<usize>(),
            )
        });

        key_bytes
    }

    fn decode_remote_mem_info(&self, chunk: &[u8]) -> RemoteMemAddressInfo {
        let addr_len = unsafe {
            *(chunk[chunk.len() - std::mem::size_of::<usize>()..].as_ptr() as *const u64)
        };
        let key_addr = unsafe {
            *(chunk[chunk.len() - std::mem::size_of::<usize>() - std::mem::size_of::<u64>()
                ..chunk.len() - std::mem::size_of::<usize>()]
                .as_ptr() as *const u64)
        };
        let mut key = chunk
            [..chunk.len() - std::mem::size_of::<usize>() - std::mem::size_of::<u64>()]
            .to_vec();

        if unsafe { (*(*self.comm_group.info_entry).domain_attr).mr_mode }
            & (libfabric_sys::FI_MR_RAW as i32)
            != 0
        {
            let mut mapped_key = 0u64;
            let err = unsafe {
                libfabric_sys::inlined_fi_mr_map_raw(
                    self.domain,
                    key_addr,
                    key.as_mut_ptr(),
                    key.len(),
                    &mut mapped_key,
                    0,
                )
            };
            if err != 0 {
                panic!("Error mapping raw MR key: {}", err);
            }
            RemoteMemAddressInfo {
                mem_address: key_addr as *const u8,
                key: mapped_key,
                len: addr_len as usize,
            }
        } else {
            let mut key_mapped = 0u64;
            unsafe {
                std::slice::from_raw_parts_mut(&mut key_mapped as *mut u64 as *mut u8, 8)
                    .copy_from_slice(&key)
            };
            RemoteMemAddressInfo {
                mem_address: key_addr as *const u8,
                key: key_mapped,
                len: addr_len as usize,
            }
        }
    }

    /// Dispatches to the OFI collective (`FI_ALLGATHER`) path when the provider
    /// supports it, else the PMI fallback. Mirrors `libfabric_sys_lamellae::Ofi::exchange_mr_info`.
    fn exchange_mr_info(
        &self,
        pes: &[usize],
        mem: &[u8],
        mr: *mut libfabric_sys::fid_mr,
        mr_key_id: u64,
    ) -> HashMap<usize, RemoteMemAddressInfo> {
        if self.mr_exchange_via_collective {
            self.collective_exchange_mr_info(pes, mem, mr)
        } else {
            self.manual_exchange_mr_info(pes, mem, mr, mr_key_id)
        }
    }

    /// `FI_ALLGATHER`-based MR-info exchange: no PMI KVS growth, no per-call
    /// fence -- just one multicast-group allgather. Preferred whenever the
    /// provider supports it (see `mr_exchange_via_collective`).
    fn collective_exchange_mr_info(
        &self,
        pes: &[usize],
        mem: &[u8],
        mr: *mut libfabric_sys::fid_mr,
    ) -> HashMap<usize, RemoteMemAddressInfo> {
        // Held across a first-use join: every member reaches its first exchange over `pes`
        // at the same point in the (already collectively ordered) alloc sequence.
        let mcast_group = self.cached_mc_group(pes);
        let cg = &self.comm_group;

        let key_bytes = self.build_local_mr_info_bytes(mem, mr);

        let mut all_mem_info_bytes = vec![0u8; key_bytes.len() * pes.len()];
        cg.post_collective(true, || unsafe {
            libfabric_sys::inlined_fi_allgather(
                cg.ep,
                key_bytes.as_ptr().cast(),
                key_bytes.len(),
                std::ptr::null_mut(),
                all_mem_info_bytes.as_mut_ptr().cast(),
                std::ptr::null_mut(),
                mcast_group.coll_addr,
                rust_type_to_fi_type::<u8>().unwrap(),
                0,
                std::ptr::null_mut(),
            )
        });

        all_mem_info_bytes
            .chunks_exact(key_bytes.len())
            .enumerate()
            .map(|(pe, chunk)| (pes[pe], self.decode_remote_mem_info(chunk)))
            .collect()
    }

    /// PMI-based fallback for exchanging MR info when the provider doesn't
    /// support `FI_ALLGATHER` (`mr_exchange_via_collective == false`). Reuses
    /// the put/get pattern already used for endpoint-address discovery in
    /// `OptOfi::new`. `mr_key_id` must be unique per exchanged allocation;
    /// callers pass the same key used to register the MR (already unique via
    /// `AllocInfoManager::next_key`).
    ///
    /// Uses `barrier(false)` (no `PMIX_COLLECT_DATA`) instead of the generic
    /// `Pmi::exchange()` -- `exchange()` hardcodes `collect_data=true`, which
    /// makes `PMIx_Fence` gather/broadcast the *entire* accumulated PMIx KVS
    /// namespace on every call. Since `mr_key_id` is never reused, that KVS
    /// only grows across the run, so `collect_data=true` here made per-call
    /// cost scale with the total number of allocations ever made, not just
    /// this one -- the root cause of the iteration-to-iteration slowdown seen
    /// on this backend. `barrier(false)` only synchronizes; each PE still
    /// lazily fetches exactly the keys it needs via the `get()` calls below.
    fn manual_exchange_mr_info(
        &self,
        pes: &[usize],
        mem: &[u8],
        mr: *mut libfabric_sys::fid_mr,
        mr_key_id: u64,
    ) -> HashMap<usize, RemoteMemAddressInfo> {
        let local_bytes = self.build_local_mr_info_bytes(mem, mr);
        let key = format!("mr_info_{}", mr_key_id);

        self._my_pmi
            .put(&key, &local_bytes)
            .expect("PMI put failed during MR-info exchange");
        self._my_pmi
            .barrier(false)
            .expect("PMI barrier failed during MR-info exchange");

        pes.iter()
            .map(|&pe| {
                let bytes = self
                    ._my_pmi
                    .get(&key, &pe)
                    .expect("PMI get failed during MR-info exchange");
                (pe, self.decode_remote_mem_info(&bytes))
            })
            .collect()
    }

    pub(crate) fn init_barrier(self: &Arc<OptOfi>) -> FabricResult<()> {
        let mut coll_attr = libfabric_sys::fi_collective_attr {
            op: 0,
            datatype: 0,
            datatype_attr: libfabric_sys::fi_atomic_attr { count: 0, size: 0 },
            max_members: 0,
            mode: 0,
        };

        let err = unsafe {
            libfabric_sys::inlined_fi_query_collective(
                self.domain,
                libfabric_sys::fi_collective_op_FI_BARRIER,
                &mut coll_attr,
                0,
            )
        };

        if err != 0 {
            let all_pes: Vec<_> = (0..self.num_pes).collect();
            let barrier_size = all_pes.len() * std::mem::size_of::<usize>();
            let barrier_addr = self
                .sub_alloc(&all_pes, barrier_size, std::mem::align_of::<usize>())
                .map_err(|e| {
                    if let AllocError::FabricAllocationError(err_no) = e {
                        FabricError::BarrierError(err_no as u32)
                    } else {
                        FabricError::BarrierError(u32::MAX)
                    }
                })?;

            *self.comm_group.barrier_impl.write() =
                BarrierImpl::Manual(barrier_addr, AtomicUsize::new(0));
            Ok(())
        } else {
            let all_pes: Vec<_> = (0..self.num_pes).collect();
            let mcast_group = self.create_mc_group(&all_pes);
            *self.comm_group.barrier_impl.write() = BarrierImpl::Collective(mcast_group);
            Ok(())
        }
    }

    pub(crate) fn clear_barrier(&self) {
        let mut barrier_impl = self.comm_group.barrier_impl.write();
        *barrier_impl = BarrierImpl::Uninit;
        self.mr_exchange_groups.lock().clear();
    }

    pub(crate) fn alloc(
        self: &Arc<OptOfi>,
        size: usize,
        alloc: AllocationType,
        align: usize,
    ) -> AllocResult<LibfabricSysOptAlloc> {
        match alloc {
            AllocationType::Sub(pes) => self.sub_alloc(&pes, size, align),
            AllocationType::Global => self.full_alloc(size, align),
            _ => return Err(AllocError::UnexpectedAllocationType(alloc)),
        }
    }

    /// Non-collective: sub-allocates from the pool of already-mmap'd,
    /// already-registered `runtime_allocs` entries (created once, collectively,
    /// at startup / via `alloc_pool()`). Safe to call from a single PE without
    /// any other PE's participation. Returns `Err(AllocError::OutOfMemoryError)`
    /// if no pool entry has room -- callers must not treat this as fatal, only
    /// as "pool exhausted, fall back to some other allocation strategy."
    pub(crate) fn rt_alloc(
        self: &Arc<OptOfi>,
        size: usize,
        align: usize,
    ) -> AllocResult<LibfabricSysOptAlloc> {
        // add space for ref count
        let (padding, size, align) = calc_alloc_padding_size_align(size, align);

        let allocs = self.runtime_allocs.read();
        for (inner_alloc, alloc) in allocs.iter() {
            if let Some(addr) = alloc.try_malloc(size, align) {
                let alloc =
                    inner_alloc.rt_alloc(alloc.clone(), addr - inner_alloc.start(), padding, size)?;
                trace!(
                    target: "libfabric-sys",
                    "new rt alloc (OptOfi pool): 0x{:x}-0x{:x} {} {} {:?}",
                    addr,
                    addr + size,
                    addr - inner_alloc.start(),
                    size,
                    alloc,
                );
                return Ok(alloc);
            }
        }
        Err(AllocError::OutOfMemoryError(size))
    }

    /// Registered scratch memory for an RDMA op whose local buffer would
    /// otherwise be plain heap/stack memory. With verbs;ofi_rxm the provider
    /// registers such buffers on the fly through the MR cache, and a cached
    /// entry can go stale when the heap range is recycled -- a GET then lands
    /// in old physical pages. A sub-range of our already-registered pool is a
    /// pure cache hit, so staging removes both the staleness and the per-op
    /// registration cost.
    ///
    /// `None` means "use the caller's own buffer" (staging disabled, empty
    /// op, or pool exhausted after letting in-flight ops drain).
    pub(crate) fn staging_alloc(
        self: &Arc<OptOfi>,
        bytes: usize,
        align: usize,
    ) -> Option<LibfabricSysOptAlloc> {
        if !self.staging_enabled || bytes == 0 {
            return None;
        }
        for attempt in 0..3 {
            match self.rt_alloc(bytes, align) {
                Ok(alloc) => return Some(alloc),
                Err(AllocError::OutOfMemoryError(_)) if attempt < 2 => {
                    self.progress_all();
                }
                Err(_) => break,
            }
        }
        self.staging_fallbacks.fetch_add(1, Ordering::Relaxed);
        if !self.staging_warned.swap(true, Ordering::Relaxed) {
            eprintln!(
                "[LAMELLAR WARNING][{}] registered staging pool exhausted (needed {} bytes); RDMA op falling back to unregistered heap memory, results may be affected by libfabric MR-cache staleness. Remedies: increase LAMELLAR_HEAP_SIZE, verify your results, or set FI_MR_CACHE_MAX_COUNT=0 (slow).",
                self.my_pe, bytes
            );
        }
        None
    }

    pub(crate) fn staging_fallbacks(&self) -> usize {
        self.staging_fallbacks.load(Ordering::Relaxed)
    }

    pub(crate) fn inject_size(&self) -> usize {
        unsafe { (*(*self.comm_group.info_entry).tx_attr).inject_size }
    }

    /// Returns the MR descriptor of the `runtime_allocs` (staging pool)
    /// entry that actually backs `addr`, or `fallback` if `addr` doesn't
    /// fall inside any pool sub-allocation. Under `FI_MR_ENDPOINT`, the
    /// provider validates the local-buffer desc against its real backing
    /// MR; a staged buffer's real MR is the pool's, not the target alloc's
    /// (`self.mr_desc`, the old always-used value) -- passing the wrong one
    /// causes SIGBUS at real network scale.
    pub(crate) fn local_desc_for(
        &self,
        addr: usize,
        fallback: *mut std::ffi::c_void,
    ) -> *mut std::ffi::c_void {
        let allocs = self.runtime_allocs.read();
        for (alloc, _) in allocs.iter() {
            let start = alloc.start();
            if addr >= start && addr < start + alloc.num_bytes() {
                return alloc.mr_desc;
            }
        }
        fallback
    }

    /// Binds and enables a newly registered MR when the negotiated domain
    /// requires endpoint-bound memory regions (`FI_MR_ENDPOINT`). No-op for
    /// providers that don't require it (everything reachable today).
    unsafe fn bind_and_enable_mr_if_required(
        &self,
        mr: *mut libfabric_sys::fid_mr,
    ) -> AllocResult<()> {
        let mr_mode = (*(*self.comm_group.info_entry).domain_attr).mr_mode as u32;
        if mr_mode & libfabric_sys::FI_MR_ENDPOINT == 0 {
            return Ok(());
        }
        let ret = libfabric_sys::inlined_fi_mr_bind(mr, &mut (*self.comm_group.ep).fid, 0);
        if ret != 0 {
            eprintln!("Error binding memory region to endpoint: {}", ret);
            close_fid("memory region", &mut (*mr).fid);
            Err(AllocError::FabricAllocationError(-ret as i32))?;
        }
        let ret = libfabric_sys::inlined_fi_mr_enable(mr);
        if ret != 0 {
            eprintln!("Error enabling memory region: {}", ret);
            close_fid("memory region", &mut (*mr).fid);
            Err(AllocError::FabricAllocationError(-ret as i32))?;
        }
        Ok(())
    }

    fn full_alloc(
        self: &Arc<OptOfi>,
        data_size: usize,
        align: usize,
    ) -> AllocResult<LibfabricSysOptAlloc> {
        //add space for ref count and padding to align it
        let (padding, size, _align) = calc_alloc_padding_size_align(data_size, align);

        // Align to page boundaries
        let aligned_size = if (self.alloc_manager.page_size() - 1) & size != 0 {
            (size + self.alloc_manager.page_size()) & !(self.alloc_manager.page_size() - 1)
        } else {
            size
        };

        trace!(target: "libfabric-sys", "Full Allocating aligned size: {} aligned", aligned_size);
        #[cfg(not(feature = "enable-on-node-shmem"))]
        let (mem, mem_base_ptr) = {
            let mmap = memmap::MmapOptions::new()
                .len(aligned_size)
                .map_anon()
                .map_err(|_| AllocError::OutOfMemoryError(aligned_size))?;
            // anonymous mmap pages are already zero-filled by the kernel
            let mem_base_ptr = mmap.as_ptr() as *mut u8;
            (LibfabricSysOptMem::Mmap(Arc::new(mmap)), mem_base_ptr)
        };

        #[cfg(feature = "enable-on-node-shmem")]
        let (mem, mem_base_ptr, same_node_bases, same_node_segments) = if self.disable_on_node_shmem
        {
            let mmap = memmap::MmapOptions::new()
                .len(aligned_size)
                .map_anon()
                .map_err(|_| AllocError::OutOfMemoryError(aligned_size))?;
            // anonymous mmap pages are already zero-filled by the kernel
            let mem_base_ptr = mmap.as_ptr() as *mut u8;
            (
                LibfabricSysOptMem::Mmap(Arc::new(mmap)),
                mem_base_ptr,
                vec![None; self.num_pes],
                vec![None; self.num_pes],
            )
        } else {
            let alloc_id = LIBFABRIC_SYS_SHMEM_ALLOC_ID.fetch_add(1, Ordering::SeqCst);
            let shmem_id = format!("libfabric_sys_alloc_{}_pe_{}", alloc_id, self.my_pe);
            let local_segment = Arc::new(attach_shmem_segment(
                self.job_id,
                aligned_size,
                align,
                &shmem_id,
                alloc_id,
                true,
            ));
            let mem_base_ptr = local_segment.base_ptr();
            let mem_slice = unsafe { std::slice::from_raw_parts_mut(mem_base_ptr, aligned_size) };
            mem_slice.fill(0);
            let (mut same_node_bases, same_node_segments) = build_same_node_segments(
                &(0..self.num_pes).collect::<Vec<_>>(),
                &self.same_node_pes,
                self.job_id,
                aligned_size,
                align,
                alloc_id,
                self.my_pe,
            );
            let mut same_node_segments = same_node_segments;
            same_node_bases[self.my_pe] = Some(mem_base_ptr as usize);
            same_node_segments[self.my_pe] = Some(local_segment.clone());
            (
                LibfabricSysOptMem::Shmem(local_segment),
                mem_base_ptr,
                same_node_bases,
                same_node_segments,
            )
        };
        let mem_slice = unsafe { std::slice::from_raw_parts_mut(mem_base_ptr, aligned_size) };

        let mr_key = self.alloc_manager.next_key() as u64;
        let mr = unsafe {
            let mut mr = MaybeUninit::<*mut libfabric_sys::fid_mr>::uninit();
            let ret = libfabric_sys::inlined_fi_mr_reg(
                self.domain,
                mem_base_ptr as *mut libc::c_void,
                aligned_size,
                (libfabric_sys::FI_READ
                    | libfabric_sys::FI_WRITE
                    | libfabric_sys::FI_REMOTE_READ
                    | libfabric_sys::FI_REMOTE_WRITE) as u64,
                0,
                mr_key,
                0,
                mr.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if ret != 0 {
                eprintln!("Error registering memory region: {}", ret);
                Err(AllocError::FabricAllocationError(-ret as i32))?;
            }
            mr.assume_init()
        };

        unsafe { self.bind_and_enable_mr_if_required(mr)? };

        let remote_alloc_infos = self.exchange_mr_info(
            &(0..self.num_pes).collect::<Vec<_>>(),
            mem_slice,
            mr,
            mr_key,
        );

        let mcast_group = self.cached_mc_group(&(0..self.num_pes).collect::<Vec<_>>());

        let alloc = LibfabricSysOptAlloc::new(
            self.clone(),
            mem,
            #[cfg(feature = "enable-on-node-shmem")]
            Arc::new(same_node_bases),
            #[cfg(feature = "enable-on-node-shmem")]
            Arc::new(same_node_segments),
            mr,
            remote_alloc_infos,
            data_size,
            padding,
            self.alloc_manager.clone(),
            Some(mcast_group),
        )
        .map_err(|e| AllocError::FabricAllocationError(e))?;
        self.alloc_manager.insert(alloc.clone());
        Ok(alloc)
    }

    fn sub_alloc(
        self: &Arc<OptOfi>,
        pes: &[usize],
        data_size: usize,
        align: usize,
    ) -> AllocResult<LibfabricSysOptAlloc> {
        //add space for ref count and padding to align it
        let (padding, size, _align) = calc_alloc_padding_size_align(data_size, align);
        // Align to page boundaries
        let aligned_size = if (self.alloc_manager.page_size() - 1) & size != 0 {
            (size + self.alloc_manager.page_size()) & !(self.alloc_manager.page_size() - 1)
        } else {
            size
        };

        trace!(target: "libfabric-sys", "Sub Allocating aligned size: {} aligned pes: {:?}", aligned_size, pes);

        #[cfg(not(feature = "enable-on-node-shmem"))]
        let (mem, mem_base_ptr) = {
            let mmap = memmap::MmapOptions::new()
                .len(aligned_size)
                .map_anon()
                .map_err(|_| AllocError::OutOfMemoryError(aligned_size))?;
            // anonymous mmap pages are already zero-filled by the kernel
            let mem_base_ptr = mmap.as_ptr() as *mut u8;
            (LibfabricSysOptMem::Mmap(Arc::new(mmap)), mem_base_ptr)
        };
        #[cfg(feature = "enable-on-node-shmem")]
        let (mem, mem_base_ptr, same_node_bases, same_node_segments) = if self.disable_on_node_shmem
        {
            let mmap = memmap::MmapOptions::new()
                .len(aligned_size)
                .map_anon()
                .map_err(|_| AllocError::OutOfMemoryError(aligned_size))?;
            // anonymous mmap pages are already zero-filled by the kernel
            let mem_base_ptr = mmap.as_ptr() as *mut u8;
            (
                LibfabricSysOptMem::Mmap(Arc::new(mmap)),
                mem_base_ptr,
                vec![None; self.num_pes],
                vec![None; self.num_pes],
            )
        } else {
            let alloc_id = LIBFABRIC_SYS_SHMEM_ALLOC_ID.fetch_add(1, Ordering::SeqCst);
            let shmem_id = format!("libfabric_sys_alloc_{}_pe_{}", alloc_id, self.my_pe);
            let local_segment = Arc::new(attach_shmem_segment(
                self.job_id,
                aligned_size,
                align,
                &shmem_id,
                alloc_id,
                true,
            ));
            let mem_base_ptr = local_segment.base_ptr();
            let mem_slice = unsafe { std::slice::from_raw_parts_mut(mem_base_ptr, aligned_size) };
            mem_slice.fill(0);
            let (mut same_node_bases, same_node_segments) = build_same_node_segments(
                pes,
                &self.same_node_pes,
                self.job_id,
                aligned_size,
                align,
                alloc_id,
                self.my_pe,
            );
            let mut same_node_segments = same_node_segments;
            same_node_bases[self.my_pe] = Some(mem_base_ptr as usize);
            same_node_segments[self.my_pe] = Some(local_segment.clone());
            (
                LibfabricSysOptMem::Shmem(local_segment),
                mem_base_ptr,
                same_node_bases,
                same_node_segments,
            )
        };
        let mem_slice = unsafe { std::slice::from_raw_parts_mut(mem_base_ptr, aligned_size) };

        let mr_key = self.alloc_manager.next_key() as u64;
        let mr = unsafe {
            let mut mr = MaybeUninit::<*mut libfabric_sys::fid_mr>::uninit();
            let ret = libfabric_sys::inlined_fi_mr_reg(
                self.domain,
                mem_base_ptr as *mut libc::c_void,
                aligned_size,
                (libfabric_sys::FI_READ
                    | libfabric_sys::FI_WRITE
                    | libfabric_sys::FI_REMOTE_READ
                    | libfabric_sys::FI_REMOTE_WRITE) as u64,
                0,
                mr_key,
                0,
                mr.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if ret != 0 {
                eprintln!("Error registering memory region: {}", ret);
                Err(AllocError::FabricAllocationError(-ret as i32))?;
            }
            mr.assume_init()
        };

        unsafe { self.bind_and_enable_mr_if_required(mr)? };

        let remote_alloc_infos = self.exchange_mr_info(pes, mem_slice, mr, mr_key);

        let mcast_group = self.cached_mc_group(pes);

        let alloc = LibfabricSysOptAlloc::new(
            self.clone(),
            mem,
            #[cfg(feature = "enable-on-node-shmem")]
            Arc::new(same_node_bases),
            #[cfg(feature = "enable-on-node-shmem")]
            Arc::new(same_node_segments),
            mr,
            remote_alloc_infos,
            data_size,
            padding,
            self.alloc_manager.clone(),
            Some(mcast_group),
        )
        .map_err(|e| AllocError::FabricAllocationError(e))?;
        self.alloc_manager.insert(alloc.clone());
        Ok(alloc)
    }

    pub(crate) fn get_alloc_from_start_addr(
        &self,
        addr: CommAllocAddr,
    ) -> AllocResult<LibfabricSysOptAlloc> {
        self.alloc_manager.get_alloc_from_start_addr(addr)
    }

    pub(crate) fn clear_allocs(&self) -> Result<(), FabricError> {
        self.alloc_manager.clear();
        Ok(())
    }

    pub(crate) fn barrier(&self) -> Result<(), FabricError> {
        // trace!("Running barrier");
        match &*self.comm_group.barrier_impl.read() {
            BarrierImpl::Uninit => {
                panic!("Barrier is not initialized");
            }
            BarrierImpl::Collective(mcast_group) => {
                let cg = &self.comm_group;
                cg.post_collective(true, || unsafe {
                    libfabric_sys::inlined_fi_barrier(
                        cg.ep,
                        mcast_group.coll_addr,
                        std::ptr::null_mut(),
                    )
                });
                // trace!("Done with barrier");
                Ok(())
            }
            BarrierImpl::Manual(barrier_alloc, barrier_id) => {
                let n = 2usize;
                let pes = (0..self.num_pes).collect::<Vec<_>>();
                let num_pes = pes.len();
                let num_rounds = ((num_pes as f64).log2() / (n as f64).log2()).ceil();
                let my_barrier = barrier_id.fetch_add(1, Ordering::SeqCst);
                for round in 0..num_rounds as usize {
                    for i in 1..=n {
                        let send_pe = (self.my_pe + i * (n + 1).pow(round as u32)) % num_pes;

                        // let dst = barrier_addr + 8 * self.my_pe;
                        unsafe {
                            barrier_alloc.inner_put::<usize>(
                                send_pe,
                                self.my_pe,
                                std::slice::from_ref(&my_barrier),
                                false,
                            );
                        }
                    }

                    let _lock = self.comm_group.lock.lock();
                    for i in 1..=n {
                        let recv_pe = (self.my_pe as i64
                            - i as i64 * (n as i64 + 1).pow(round as u32))
                        .rem_euclid(num_pes as i64);
                        // let barrier_vec = unsafe {
                        //     std::slice::from_raw_parts(barrier_addr as *const usize, num_pes)
                        // };
                        let barrier_vec = unsafe { barrier_alloc.as_mut_slice::<usize>() };
                        while my_barrier > barrier_vec[recv_pe as usize] {
                            self.comm_group.progress();
                            std::thread::yield_now();
                        }
                    }
                }
                Ok(())
            }
            BarrierImpl::Pmi(pmi) => {
                pmi.barrier(true).expect("PMI Barrier failed");
                Ok(())
            }
        }
    }

    pub(crate) fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize {
        self.alloc_manager
            .local_addr(remote_pe, remote_addr)
            .expect(&format!(
                "Local address not found from remote PE {}, remote addr: {:x}",
                remote_pe, remote_addr
            ))
    }

    pub(crate) fn one_sided_alloc_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
        num_bytes: usize,
    ) -> CommAlloc {
        self.alloc_manager.one_sided_alloc_from_remote_pe_and_addr(
            remote_pe,
            remote_addr,
            num_bytes,
        )
    }

    pub(crate) fn local_alloc_and_offset_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
    ) -> Option<(CommAlloc, usize)> {
        self.alloc_manager
            .local_alloc_and_offset_from_remote_pe_and_addr(remote_pe, remote_addr)
    }

    pub(crate) fn remote_addr(&self, pe: usize, local_addr: usize) -> usize {
        self.alloc_manager
            .remote_addr(pe, local_addr)
            .expect(&format!("Remote address not found for PE {}", pe))
    }

    pub(crate) fn wait_all(&self) {
        self.comm_group.wait_all()
    }

    pub(crate) fn thread_wait(&self) {
        self.comm_group.wait_all()
    }

    pub(crate) fn poll_wait_for_tx_cntr(&self) -> Poll<()> {
        self.comm_group.poll_wait_for_tx_cntr()
    }

    pub(crate) fn poll_wait_for_rx_cntr(&self) -> Poll<()> {
        self.comm_group.poll_wait_for_rx_cntr()
    }

    pub(crate) fn poll_wait_for_collectives(&self) -> Poll<()> {
        self.comm_group.poll_wait_for_collectives()
    }

    pub(crate) fn progress_all(&self) {
        let _lock = self.comm_group.lock.lock();
        self.comm_group.progress()
    }

    pub(crate) fn thread_progress(&self) {
        let _lock = self.comm_group.lock.lock();
        self.comm_group.progress()
    }

    /// Raw PMIx_Fence teardown sync + fid closes. Runs exactly once regardless
    /// of caller (guarded by `teardown_done`). Called explicitly from
    /// LibfabricSysOptComm::drop once it has confirmed it holds the last
    /// Arc<OptOfi> reference -- that guarantees this runs on the same thread as
    /// comm's own drop (empirically always the process main thread) rather than
    /// leaving it to whichever thread happens to release the final Arc clone.
    /// libpmix's PMIx_Fence is not safe to call from an arbitrary thread:
    /// measured ~90% hang rate when a non-main thread runs it, vs 0% on main.
    /// Drop::drop below also calls this, as a fallback for any path that drops
    /// the last Arc<OptOfi> without going through comm.rs's explicit call.
    pub(crate) fn final_teardown(&self) {
        if self.teardown_done.swap(true, Ordering::SeqCst) {
            return;
        }
        trace!(target: "drop", "tearing down libfabric-sys OptOfi");
        // Drain first (local, no cross-rank dependency).
        self.comm_group.wait_all();

        // Any collective barrier group must be closed while the endpoint and AV
        // are still alive. Normally LibfabricSysOptComm::drop has already cleared it.
        *self.comm_group.barrier_impl.write() = BarrierImpl::Uninit;

        // Final cross-rank sync point, required here (not optional): the fid closes
        // below are destructive at the verbs/RC level (closing ep/domain/fabric tears
        // down the QP), and a peer that hasn't yet reached its own close sequence can
        // hang inside its own fi_close(ep) waiting on a disconnect handshake against a
        // QP/domain we've already destroyed. Confirmed live: one rank reached "all
        // fids closed" while its peer hung forever on the very first fi_close(ep)
        // call. The mutual hardware barrier in LibfabricSysOptComm::drop happens
        // *before* this and does not cover this window -- clear_barrier/clear_allocs
        // and the Arc<OptOfi> refcount drop to 0 in between are both local-only and
        // can run at very different speeds per rank, so ranks arrive here far apart
        // in time.
        //
        // libpmix's PMIx_Fence is NOT safe to call from an arbitrary thread: measured
        // ~90% hang rate when the thread that drops the last Arc<OptOfi> (and thus
        // runs this fence) is not the process main thread, vs 0% when it is. A stray
        // extra Arc<OptOfi> clone (source not fully identified, but confirmed real --
        // strong_count sometimes reads 2 rather than 1 right before
        // LibfabricSysOptComm's own drop releases its clone) can let a background
        // thread be the one to actually trigger this drop instead of main. The fix
        // is upstream in LibfabricSysOptComm::drop (comm.rs): it busy-waits for
        // strong_count==1 before its own clone drops, guaranteeing whichever thread
        // runs comm's drop is also the one that drives this fence to 0 refs.
        self._my_pmi
            .barrier(false)
            .expect("PMI barrier failed during final OFI teardown sync");

        unsafe {
            close_fid("endpoint", &mut (*self.comm_group.ep).fid);
            close_fid("write counter", &mut (*self.comm_group.put_cntr).fid);
            close_fid("read counter", &mut (*self.comm_group.get_cntr).fid);
            close_fid("completion queue", &mut (*self.comm_group.cq).fid);
            close_fid("event queue", &mut (*self.comm_group.eq).fid);
            close_fid("address vector", &mut (*self.comm_group.av).fid);
            close_fid("domain", &mut (*self.domain).fid);
            close_fid("fabric", &mut (*self.fabric).fid);
            libfabric_sys::fi_freeinfo(self.comm_group.info_entry);
        }
        trace!(target: "drop", "end teardown libfabric-sys OptOfi");
    }
}

impl Drop for OptOfi {
    fn drop(&mut self) {
        self.final_teardown();
    }
}

fn atomic_op_to_fi_atomic_op<T: 'static>(op: &AtomicOp<T>) -> u32 {
    match op {
        AtomicOp::Min(_) | AtomicOp::FetchMin(_) => libfabric_sys::fi_op_FI_MIN,
        AtomicOp::Max(_) | AtomicOp::FetchMax(_) => libfabric_sys::fi_op_FI_MAX,
        AtomicOp::Sum(_) | AtomicOp::FetchSum(_) => libfabric_sys::fi_op_FI_SUM,
        AtomicOp::Sub(_) | AtomicOp::FetchSub(_) => libfabric_sys::fi_op_FI_SUM, // Sub can be implemented as Add with negative value
        AtomicOp::Prod(_) | AtomicOp::FetchProd(_) => libfabric_sys::fi_op_FI_PROD,
        AtomicOp::BitOr(_) | AtomicOp::FetchBitOr(_) => libfabric_sys::fi_op_FI_BOR,
        AtomicOp::BitXor(_) | AtomicOp::FetchBitXor(_) => libfabric_sys::fi_op_FI_BXOR,
        AtomicOp::BitAnd(_) | AtomicOp::FetchBitAnd(_) => libfabric_sys::fi_op_FI_BAND,
        AtomicOp::Read(_) => libfabric_sys::fi_op_FI_ATOMIC_READ,
        AtomicOp::Write(_) => libfabric_sys::fi_op_FI_ATOMIC_WRITE,
        AtomicOp::Cas => libfabric_sys::fi_op_FI_CSWAP,
    }
}

fn rust_type_to_fi_type<T: 'static>() -> Option<u32> {
    let tid = std::any::TypeId::of::<T>();
    if tid == std::any::TypeId::of::<u8>() {
        Some(libfabric_sys::fi_datatype_FI_UINT8)
    } else if tid == std::any::TypeId::of::<u16>() {
        Some(libfabric_sys::fi_datatype_FI_UINT16)
    } else if tid == std::any::TypeId::of::<u32>() {
        Some(libfabric_sys::fi_datatype_FI_UINT32)
    } else if tid == std::any::TypeId::of::<u64>() {
        Some(libfabric_sys::fi_datatype_FI_UINT64)
    } else if tid == std::any::TypeId::of::<usize>() {
        #[cfg(target_pointer_width = "64")]
        {
            Some(libfabric_sys::fi_datatype_FI_UINT64)
        }
        #[cfg(target_pointer_width = "32")]
        {
            Some(libfabric_sys::fi_datatype_FI_UINT32)
        }
    } else if tid == std::any::TypeId::of::<u128>() {
        Some(libfabric_sys::fi_datatype_FI_UINT128)
    } else if tid == std::any::TypeId::of::<i8>() {
        Some(libfabric_sys::fi_datatype_FI_INT8)
    } else if tid == std::any::TypeId::of::<i16>() {
        Some(libfabric_sys::fi_datatype_FI_INT16)
    } else if tid == std::any::TypeId::of::<i32>() {
        Some(libfabric_sys::fi_datatype_FI_INT32)
    } else if tid == std::any::TypeId::of::<i64>() {
        Some(libfabric_sys::fi_datatype_FI_INT64)
    } else if tid == std::any::TypeId::of::<isize>() {
        #[cfg(target_pointer_width = "64")]
        {
            Some(libfabric_sys::fi_datatype_FI_INT64)
        }
        #[cfg(target_pointer_width = "32")]
        {
            Some(libfabric_sys::fi_datatype_FI_INT32)
        }
    } else if tid == std::any::TypeId::of::<i128>() {
        Some(libfabric_sys::fi_datatype_FI_INT128)
    } else if tid == std::any::TypeId::of::<f32>() {
        Some(libfabric_sys::fi_datatype_FI_FLOAT)
    } else if tid == std::any::TypeId::of::<f64>() {
        Some(libfabric_sys::fi_datatype_FI_DOUBLE)
    } else {
        None
    }
}

pub(crate) struct AllocInfoManager {
    pub(crate) mr_info_table: Arc<RwLock<Vec<LibfabricSysOptAlloc>>>,
    mr_next_key: AtomicUsize,
    page_size: usize,
}

impl AllocInfoManager {
    pub(crate) fn new() -> Self {
        Self {
            mr_info_table: Arc::new(RwLock::new(Vec::new())),
            mr_next_key: AtomicUsize::new(0),
            page_size: page_size::get(),
        }
    }

    pub(crate) fn insert(&self, alloc: LibfabricSysOptAlloc) {
        self.mr_info_table.write().push(alloc);
    }

    pub(crate) fn clear(&self) {
        let mut table = self.mr_info_table.write();
        let allocs = table.drain(..).collect::<Vec<_>>();
        drop(table); // we do this because when the allocs are dropped, they may call back into the AllocInfoManager to remove themselves thus deadlocking
        for alloc in allocs {
            trace!(target: "libfabric-sys", "Clearing alloc: {:?}", alloc);
        }
    }

    pub(crate) fn remove_from_alloc(&self, mem_addr: &LibfabricSysOptAlloc) {
        let mut table = self.mr_info_table.write();
        if !table.is_empty() {
            let idx = table
                .iter()
                .position(|e| e.mem.as_ptr() == mem_addr.mem.as_ptr())
                .expect("Error! Invalid memory address");
            table.remove(idx);
        }
    }

    pub(crate) fn get_alloc_from_start_addr(
        &self,
        mem_addr: CommAllocAddr,
    ) -> AllocResult<LibfabricSysOptAlloc> {
        let table = self.mr_info_table.read();
        table
            .iter()
            .find(|e| e.start() == mem_addr.0)
            .cloned()
            .ok_or(AllocError::LocalNotFound(mem_addr))
    }

    pub(crate) fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> Option<usize> {
        let table = self.mr_info_table.read();
        let alloc_info = table
            .iter()
            .find(|x| x.remote_contains(&remote_pe, &remote_addr))?;
        let remote_alloc_info = alloc_info.remote_allocs.get(&remote_pe)?;
        let remote_offset = remote_addr - remote_alloc_info.mem_address as usize;
        Some(alloc_info.start() + remote_offset)
    }

    pub(crate) fn one_sided_alloc_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
        num_bytes: usize,
    ) -> CommAlloc {
        trace!(target: "libfabric-sys",
            "looking for remote_addr: {:x} on pe {:x}",
            remote_pe,
            remote_addr
        );
        let table = self.mr_info_table.read();
        let alloc_info = table
            .iter()
            .find(|x| x.remote_contains(&remote_pe, &remote_addr))
            .expect("Remote address not found in any allocation");
        let remote_alloc_info = alloc_info
            .remote_allocs
            .get(&remote_pe)
            .expect("Remote PE not part of the allocation");
        let remote_offset = remote_addr - remote_alloc_info.mem_address as usize;
        let alloc = alloc_info
            .sub_alloc(remote_offset, num_bytes)
            .expect("Failed to create one-sided allocation from remote PE and address");
        OneSidedLibfabricSysOptAlloc { alloc, remote_pe }.into()
    }

    pub(crate) fn local_alloc_and_offset_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
    ) -> Option<(CommAlloc, usize)> {
        trace!(
            target: "libfabric-sys",
            "looking for remote_addr: {:x} on pr {:x}",
            remote_pe,
            remote_addr
        );
        let table = self.mr_info_table.read();
        let alloc_info = table
            .iter()
            .find(|x| x.remote_contains(&remote_pe, &remote_addr))?;
        let remote_alloc_info = alloc_info.remote_allocs.get(&remote_pe)?;
        let remote_offset = remote_addr - remote_alloc_info.mem_address as usize;
        Some((alloc_info.clone().into(), remote_offset))
    }

    pub(crate) fn remote_addr(&self, remote_pe: usize, local_addr: usize) -> Option<usize> {
        let table = self.mr_info_table.read();
        if let Some(alloc_info) = table.iter().find(|x| x.contains(&local_addr)) {
            if let Some(remote_alloc_info) = alloc_info.remote_allocs.get(&remote_pe) {
                let local_offset = local_addr - alloc_info.start();
                Some(unsafe { remote_alloc_info.mem_address.add(local_offset) as usize })
            } else {
                None
            }
        } else {
            None
        }
    }

    pub(crate) fn page_size(&self) -> usize {
        self.page_size
    }

    pub(crate) fn next_key(&self) -> usize {
        self.mr_next_key.fetch_add(1, Ordering::SeqCst)
    }
}

#[derive(Clone)]
enum AllocTable {
    Fabric(Arc<AllocInfoManager>),
    Runtime(BTreeAlloc, usize, Arc<AllocInfoManager>), //the usize is the offset of the rt_alloc so that we can free it properly if a sub_alloc is the last reference
}

pub(crate) trait MemoryRange {
    fn bounds(&self, len: usize) -> (usize, usize);
}

impl MemoryRange for Range<usize> {
    fn bounds(&self, _len: usize) -> (usize, usize) {
        (self.start, self.end)
    }
}

impl MemoryRange for RangeFrom<usize> {
    fn bounds(&self, len: usize) -> (usize, usize) {
        (self.start, len)
    }
}

impl MemoryRange for RangeFull {
    fn bounds(&self, len: usize) -> (usize, usize) {
        (0, len)
    }
}

impl MemoryRange for RangeTo<usize> {
    fn bounds(&self, _len: usize) -> (usize, usize) {
        (0, self.end)
    }
}

#[derive(Clone)]
pub(crate) struct RemoteMemAddressInfo {
    mem_address: *const u8,
    len: usize,
    key: u64,
}

impl RemoteMemAddressInfo {
    pub(crate) fn mem_address(&self) -> *const u8 {
        self.mem_address
    }

    pub(crate) fn key(&self) -> u64 {
        self.key
    }

    pub(crate) fn contains(&self, addr: &usize) -> bool {
        let start = self.mem_address as usize;
        let end = start + self.len;
        *addr >= start && *addr < end
    }

    pub(crate) unsafe fn sub_region(&self, range: impl MemoryRange) -> Self {
        let (start, end) = range.bounds(self.len);
        // We can create a zero-length sub-region.
        assert!(
            start <= end,
            "Invalid range for remote memory sub-region: start: {}, end: {}",
            start,
            end
        );
        let len = end - start;
        let (start, end) = (
            start * std::mem::size_of::<u8>(),
            end * std::mem::size_of::<u8>(),
        );

        // We can create a zero-length sub-region.
        assert!(
            start <= self.len,
            "Out of bounds access to remote memory sub-region start:{}  len: {}",
            start,
            self.len
        );
        assert!(
            end - 1 < self.len,
            "Out of bounds access to remote memory sub-region start:{}  len: {}",
            start,
            self.len
        );

        Self {
            mem_address: unsafe { self.mem_address.add(start) },
            len,
            key: self.key.clone(),
        }
    }
}

pub(crate) struct LibfabricSysOptAlloc {
    pub(crate) ofi: Arc<OptOfi>,
    mem: LibfabricSysOptMem,
    #[cfg(feature = "enable-on-node-shmem")]
    same_node_bases: Arc<Vec<Option<usize>>>,
    #[cfg(feature = "enable-on-node-shmem")]
    same_node_segments: Arc<Vec<Option<Arc<ShmemSegment>>>>,
    mr: *mut libfabric_sys::fid_mr,
    mr_desc: *mut std::ffi::c_void,
    range: std::ops::Range<usize>,
    remote_allocs: HashMap<usize, RemoteMemAddressInfo>,
    fabric_ref_cnt_offset: usize,
    rt_ref_cnt_offset: usize,
    id: usize,
    alloc_table: AllocTable,
    mcast_group: Option<Arc<RawMcGroup>>,
    pub(crate) print: bool,
}

unsafe impl Send for LibfabricSysOptAlloc {}
unsafe impl Sync for LibfabricSysOptAlloc {}

impl std::fmt::Debug for LibfabricSysOptAlloc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fabric_ref_count = unsafe {
            (&*(self.mem.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize))
                .load(Ordering::SeqCst)
        };

        let mut temp = f.debug_struct("LibfabricSysOptAlloc");
        temp.field("id", &self.id);
        temp.field(
            "addr",
            &format_args!("{:x} - {:x}", self.range.start, self.range.end),
        )
        .field(
            "mem",
            &format_args!("{:?}-{:?}", self.mem.as_ptr(), unsafe {
                self.mem.as_ptr().add(self.mem.len())
            }),
        )
        .field("data_num_bytes", &self.num_bytes())
        .field("my_pe", &self.ofi.my_pe)
        .field("num_pes", &self.ofi.num_pes)
        .field(
            "fabric_ref_cnt_offset",
            &format_args!(
                "{} ({:?}): {}",
                self.fabric_ref_cnt_offset,
                unsafe { self.mem.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize },
                fabric_ref_count
            ),
        );
        if let AllocTable::Runtime(_, _, _) = &self.alloc_table {
            let rt_ref_count = unsafe {
                (&*(self.mem.as_ptr().add(self.rt_ref_cnt_offset) as *const AtomicUsize))
                    .load(Ordering::SeqCst)
            };
            let padding = decode_padding(rt_ref_count);
            let rt_ref_count = decode_ref_count(rt_ref_count);
            temp.field(
                "rt_ref_cnt_offset",
                &format_args!(
                    "{} ({:?}): {}, {}",
                    self.rt_ref_cnt_offset,
                    unsafe { self.mem.as_ptr().add(self.rt_ref_cnt_offset) as *const AtomicUsize },
                    rt_ref_count,
                    padding,
                ),
            );
        }
        temp.finish()
    }
}

impl Clone for LibfabricSysOptAlloc {
    fn clone(&self) -> Self {
        self.increment_fabric_ref_count();
        if let AllocTable::Runtime(_, _, _) = &self.alloc_table {
            self.increment_rt_ref_count();
        }
        get_ref_count(unsafe {
            &*(self.mem.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize)
        });
        trace!(target: "libfabric-sys", "Cloned LibfabricSysOptAlloc: {:?}", self);
        Self {
            ofi: self.ofi.clone(),
            mem: self.mem.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_bases: self.same_node_bases.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_segments: self.same_node_segments.clone(),
            mr: self.mr.clone(),
            mr_desc: unsafe { libfabric_sys::inlined_fi_mr_desc(self.mr) },
            range: self.range.clone(),
            remote_allocs: self.remote_allocs.clone(),
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: self.rt_ref_cnt_offset,
            id: self.id,
            alloc_table: self.alloc_table.clone(),
            mcast_group: self.mcast_group.clone(),
            print: self.print,
        }
    }
}

impl From<LibfabricSysOptAlloc> for CommAlloc {
    fn from(alloc: LibfabricSysOptAlloc) -> Self {
        CommAlloc {
            inner_alloc: Arc::new(CommAllocInner::LibfabricSysOptAlloc(alloc)),
        }
    }
}

static ALLOC_ID: AtomicUsize = AtomicUsize::new(0);
#[cfg(feature = "enable-on-node-shmem")]
static LIBFABRIC_SYS_SHMEM_ALLOC_ID: AtomicUsize = AtomicUsize::new(0);

#[cfg(feature = "enable-on-node-shmem")]
fn build_same_node_segments(
    pes: &[usize],
    same_node_pes: &[bool],
    job_id: usize,
    size: usize,
    align: usize,
    alloc_id: usize,
    my_pe: usize,
) -> (Vec<Option<usize>>, Vec<Option<Arc<ShmemSegment>>>) {
    let mut bases = vec![None; same_node_pes.len()];
    let mut segments = vec![None; same_node_pes.len()];

    for pe in pes {
        if !same_node_pes.get(*pe).copied().unwrap_or(false) || *pe == my_pe {
            continue;
        }
        let shmem_id = format!("libfabric_sys_alloc_{}_pe_{}", alloc_id, pe);
        let segment = attach_shmem_segment(job_id, size, align, &shmem_id, alloc_id, false);
        bases[*pe] = Some(segment.base_ptr() as usize);
        segments[*pe] = Some(Arc::new(segment));
    }

    (bases, segments)
}

impl LibfabricSysOptAlloc {
    unsafe fn negate_atomic_value<OFI>(value: *mut OFI) {
        let num_bytes = std::mem::size_of::<OFI>();
        let mut bytes = vec![0u8; num_bytes];
        std::ptr::copy(value.cast::<u8>(), bytes.as_mut_ptr(), num_bytes);
        for byte in bytes.iter_mut() {
            *byte = !*byte;
        }

        let mut carry: u16 = 1;
        #[cfg(target_endian = "little")]
        for byte in bytes.iter_mut() {
            let sum = *byte as u16 + carry;
            *byte = sum as u8;
            carry = sum >> 8;
            if carry == 0 {
                break;
            }
        }
        #[cfg(target_endian = "big")]
        for byte in bytes.iter_mut().rev() {
            let sum = *byte as u16 + carry;
            *byte = sum as u8;
            carry = sum >> 8;
            if carry == 0 {
                break;
            }
        }

        // let mut result = std::mem::MaybeUninit::<OFI>::uninit();
        std::ptr::copy(bytes.as_ptr(), value.cast::<u8>(), num_bytes);
    }

    fn new(
        ofi: Arc<OptOfi>,
        mem: LibfabricSysOptMem,
        #[cfg(feature = "enable-on-node-shmem")] same_node_bases: Arc<Vec<Option<usize>>>,
        #[cfg(feature = "enable-on-node-shmem")] same_node_segments: Arc<
            Vec<Option<Arc<ShmemSegment>>>,
        >,
        mr: *mut libfabric_sys::fid_mr,
        remote_allocs: HashMap<usize, RemoteMemAddressInfo>,
        num_bytes: usize,
        padding: usize,
        alloc_table: Arc<AllocInfoManager>,
        mcast_group: Option<Arc<RawMcGroup>>,
    ) -> Result<Self, i32> {
        let start = mem.as_ptr() as usize;
        let end = start + num_bytes;
        let id = ALLOC_ID.fetch_add(1, Ordering::SeqCst);
        let ref_cnt_offset = num_bytes + padding;
        let fabric_ref_cnt_offset = num_bytes + padding;
        let encoded = encode_ref_count_and_padding(1, padding);

        let alloc = Self {
            ofi: ofi.clone(),
            mem: mem.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_bases,
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_segments,
            mr,
            mr_desc: unsafe { libfabric_sys::inlined_fi_mr_desc(mr) },
            range: std::ops::Range { start, end },
            remote_allocs: remote_allocs.clone(),
            fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            id,
            alloc_table: AllocTable::Fabric(alloc_table),
            mcast_group,
            print: false,
        };

        unsafe {
            (&*(alloc.mem.as_ptr().add(alloc.fabric_ref_cnt_offset) as *mut AtomicUsize))
                .store(encoded, Ordering::SeqCst);
        }
        debug!(target: "libfabric-sys", "Created LibfabricSysOpt allocation: {:?}", alloc);

        Ok(alloc)
    }

    pub(crate) fn num_pes(&self) -> usize {
        self.remote_allocs.len()
    }

    // we call this function to create a sub-allocation that is tracked as part of a runtime allocation
    // 'len' should already contain the approriate padding and space for the ref count
    pub(crate) fn rt_alloc(
        &self,
        alloc_table: BTreeAlloc,
        offset: usize,
        padding: usize,
        len: usize, //data size + padding + ref count size
    ) -> AllocResult<Self> {
        if offset + len > self.num_bytes() {
            return Err(AllocError::InvalidSubAlloc(offset, len));
        }
        let data_bytes = len - padding - std::mem::size_of::<AtomicUsize>();
        let mut remote_allocs = HashMap::new();
        for (pe, remote_info) in self.remote_allocs.iter() {
            let new_remote_info = unsafe { remote_info.sub_region(offset..offset + len) };
            remote_allocs.insert(*pe, new_remote_info);
        }
        let id = ALLOC_ID.fetch_add(1, Ordering::SeqCst);
        self.increment_fabric_ref_count();
        let ref_cnt_offset = offset + data_bytes + padding;
        let encoded = encode_ref_count_and_padding(1, padding);

        let alloc_manager = match &self.alloc_table {
            AllocTable::Fabric(alloc_manager) => alloc_manager.clone(),
            AllocTable::Runtime(_, _, alloc_manager) => alloc_manager.clone(),
        };

        let alloc = Self {
            ofi: self.ofi.clone(),
            mem: self.mem.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_bases: self.shifted_same_node_bases(offset),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_segments: self.same_node_segments.clone(),
            mr: self.mr.clone(),
            mr_desc: unsafe { libfabric_sys::inlined_fi_mr_desc(self.mr) },
            range: self.range.start + offset..self.range.start + offset + data_bytes,
            remote_allocs: remote_allocs.clone(),
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            id,
            alloc_table: AllocTable::Runtime(alloc_table, self.range.start + offset, alloc_manager),
            mcast_group: self.mcast_group.clone(),
            print: self.print,
        };

        unsafe {
            (&*(alloc.mem.as_ptr().add(alloc.rt_ref_cnt_offset) as *mut AtomicUsize))
                .store(encoded, Ordering::SeqCst);
        }

        debug!(target: "libfabric-sys", "Created LibfabricSysOpt rt-allocation: {:?}", alloc);
        Ok(alloc)
    }

    // This function is used to construct an rt_alloc from a raw sub-allocation
    // typically paired with a call to leak() we dont increment the ref counts as this instance recaptures the leaked instance
    pub(crate) fn as_rt_alloc(self, alloc_table: BTreeAlloc) -> AllocResult<Self> {
        let alloc_manager = match &self.alloc_table {
            AllocTable::Fabric(alloc_manager) => alloc_manager.clone(),
            AllocTable::Runtime(_, _, alloc_manager) => alloc_manager.clone(),
        };

        //since we are recapturing a leaked alloc, the non-rt sub-allocation we are converting should contain the appropriate ref count space at the end of the allocation
        let ref_cnt_offset = ((self.start() - self.mem.as_ptr() as usize) + self.num_bytes())
            - std::mem::size_of::<AtomicUsize>();

        let encoded_ref_count = unsafe {
            (&*(self.mem.as_ptr().add(ref_cnt_offset) as *const AtomicUsize)).load(Ordering::SeqCst)
        };
        let (_rt_ref_cnt, padding) = decode_ref_count_and_padding(encoded_ref_count);

        let alloc = Self {
            ofi: self.ofi.clone(),
            mem: self.mem.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_bases: self.same_node_bases.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_segments: self.same_node_segments.clone(),
            mr: self.mr.clone(),
            mr_desc: unsafe { libfabric_sys::inlined_fi_mr_desc(self.mr) },
            range: self.range.start..self.range.end - padding - std::mem::size_of::<AtomicUsize>(),
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            remote_allocs: self.remote_allocs.clone(),
            id: self.id,
            alloc_table: AllocTable::Runtime(alloc_table, self.range.start, alloc_manager),
            mcast_group: self.mcast_group.clone(),
            print: true,
        };
        get_ref_count(unsafe {
            &*(alloc.mem.as_ptr().add(alloc.fabric_ref_cnt_offset) as *const AtomicUsize)
        });
        debug!(target: "libfabric-sys", "Converted LibfabricSysOpt alloc to rt-alloc: {:?}", alloc);
        Ok(alloc)
    }

    pub(crate) fn sub_alloc(&self, offset: usize, len: usize) -> AllocResult<Self> {
        if offset + len > self.num_bytes() {
            return Err(AllocError::InvalidSubAlloc(offset, len));
        }
        let mut remote_allocs = HashMap::new();
        for (pe, remote_info) in self.remote_allocs.iter() {
            let new_remote_info = unsafe { remote_info.sub_region(offset..offset + len) };
            remote_allocs.insert(*pe, new_remote_info);
        }
        let id = ALLOC_ID.fetch_add(1, Ordering::SeqCst);

        self.increment_fabric_ref_count();
        if let AllocTable::Runtime(_, _, _) = &self.alloc_table {
            self.increment_rt_ref_count();
        }

        let alloc = Self {
            ofi: self.ofi.clone(),
            mem: self.mem.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_bases: self.shifted_same_node_bases(offset),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_segments: self.same_node_segments.clone(),
            mr: self.mr.clone(),
            mr_desc: unsafe { libfabric_sys::inlined_fi_mr_desc(self.mr) },
            range: self.range.start + offset..self.range.start + offset + len,
            remote_allocs,
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: self.rt_ref_cnt_offset, //keep the same ref count offset as the parent allocation if this is actually a rt alloc, it will be updated when converted to a rt_alloc
            id,
            alloc_table: self.alloc_table.clone(),
            mcast_group: self.mcast_group.clone(),
            print: self.print,
        };
        debug!(target: "libfabric-sys", "Created LibfabricSysOpt sub-allocation: {:?}", alloc);
        Ok(alloc)
    }

    pub(crate) fn leak(self) -> Option<CommAllocAddr> {
        match self.alloc_table {
            AllocTable::Fabric(_) => None, //only rt_allocs can be leaked
            AllocTable::Runtime(_, _, _) => {
                self.increment_fabric_ref_count(); //increment the ref count to account for the leaked instance
                let _cnt = self.increment_rt_ref_count(); //increment the ref count to account for the leaked instance
                debug!(target: "libfabric-sys", "Leaking Libfabric-sys rt-allocation: {:?}", self);
                // println!("Leaking allocation {:x} {:?}", self.start(), cnt);
                Some(CommAllocAddr(self.start()))
            }
        }
    }

    pub(crate) fn increment_fabric_ref_count(&self) -> usize {
        let ref_count =
            unsafe { &*(self.mem.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize) };
        increment_ref_count(ref_count)
    }

    pub(crate) fn decrement_fabric_ref_count(&self) -> usize {
        let ref_count =
            unsafe { &*(self.mem.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize) };
        decrement_ref_count(ref_count)
    }

    pub(crate) fn increment_rt_ref_count(&self) -> usize {
        let ref_count =
            unsafe { &*(self.mem.as_ptr().add(self.rt_ref_cnt_offset) as *const AtomicUsize) };
        increment_ref_count(ref_count)
    }
    pub(crate) fn decrement_rt_ref_count(&self) -> usize {
        let ref_count =
            unsafe { &*(self.mem.as_ptr().add(self.rt_ref_cnt_offset) as *const AtomicUsize) };
        decrement_ref_count(ref_count)
    }

    pub(crate) unsafe fn as_mut_slice<T: Copy>(&self) -> &mut [T] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.start() as *mut T,
                self.num_bytes() / std::mem::size_of::<T>(),
            )
        }
    }

    pub(crate) unsafe fn as_slice<T: Copy>(&self) -> &[T] {
        unsafe {
            std::slice::from_raw_parts(
                self.start() as *const T,
                self.num_bytes() / std::mem::size_of::<T>(),
            )
        }
    }

    pub(crate) fn start(&self) -> usize {
        self.range.start
    }
    pub(crate) fn num_bytes(&self) -> usize {
        self.range.end - self.range.start
    }

    #[cfg(feature = "enable-on-node-shmem")]
    fn shifted_same_node_bases(&self, offset: usize) -> Arc<Vec<Option<usize>>> {
        Arc::new(
            self.same_node_bases
                .iter()
                .map(|base| base.map(|addr| addr + offset))
                .collect(),
        )
    }

    #[cfg(feature = "enable-on-node-shmem")]
    fn same_node_addr(&self, pe: usize, offset_bytes: usize) -> Option<CommAllocAddr> {
        self.same_node_bases
            .get(pe)
            .and_then(|base| base.map(|addr| CommAllocAddr(addr + offset_bytes)))
    }

    pub(crate) fn contains(&self, addr: &usize) -> bool {
        trace!(target: "libfabric-sys",
            "Checking if address {:x} is contained in allocation range {:x}-{:x}",
            addr,
            self.range.start,
            self.range.end
        );
        self.range.contains(addr)
    }

    pub(crate) fn remote_contains(&self, remote_id: &usize, addr: &usize) -> bool {
        trace!(target: "libfabric-sys",
            "Checking if remote address {:x} on PE {} is contained in remote allocation for {:?}",
            addr,
            remote_id,
            self,
        );
        match self.remote_allocs.get(remote_id) {
            Some(remote_info) => {
                trace!(target: "libfabric-sys",
                    "Remote PE {} allocation info: {:?} {:?}",
                    remote_id,
                    remote_info.mem_address,
                    unsafe{remote_info.mem_address.add(remote_info.len)}
                );
                remote_info.contains(&addr)
            }
            None => {
                trace!(target: "libfabric-sys",
                    "Remote PE {} is not part of the sub allocation group",
                    remote_id
                );
                for (pe, remote_info) in self.remote_allocs.iter() {
                    trace!(target: "libfabric-sys",
                        "  PE {}: {:?} {:?}",
                        pe,
                        remote_info.mem_address,
                        remote_info.len
                    );
                }
                false
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn remote_info(&self, remote_pe: &usize) -> Option<RemoteMemAddressInfo> {
        self.remote_allocs.get(remote_pe).cloned()
    }

    #[allow(dead_code)]
    pub(crate) fn mr(&self) -> *mut libfabric_sys::fid_mr {
        self.mr.clone()
    }

    pub(crate) unsafe fn inner_put<T: Copy>(
        &self,
        pe: usize,
        offset: usize, //T-sized offset
        src_addr: &[T],
        blocking: bool,
    ) {
        let offset = offset * std::mem::size_of::<T>(); //we allocate memoryregions from libfabric as u8;
        assert!(offset + src_addr.len() * std::mem::size_of::<T>() <= self.num_bytes()); //we use num_bytes instead of mem.len() to allow for sub-allocations,
        if pe == self.ofi.my_pe {
            std::ptr::copy(
                src_addr.as_ptr() as *const u8,
                (self.start() + offset) as *mut u8,
                src_addr.len() * std::mem::size_of::<T>(),
            );
            return;
        }
        #[cfg(feature = "enable-on-node-shmem")]
        if let Some(addr) = self.same_node_addr(pe, offset) {
            std::ptr::copy(
                src_addr.as_ptr() as *const u8,
                addr.as_ptr::<u8>() as *mut u8,
                src_addr.len() * std::mem::size_of::<T>(),
            );
            return;
        }
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));

        let mut remote_dst_addr = remote_alloc_info.mem_address().add(offset);
        debug!(
            "Inner Put: Remote destination address for PE {}: base_addr {:?} offset<T> {} size_of<T> {} len {} {:?}-{:?} {} bytes",
            pe,
            remote_alloc_info.mem_address(),
            offset,
            std::mem::size_of::<T>(),
            src_addr.len(),
            remote_dst_addr,
            remote_dst_addr.add(std::mem::size_of_val(src_addr)),
            std::mem::size_of_val(src_addr)
        );
        let remote_key = remote_alloc_info.key();
        let cg = &self.ofi.comm_group;
        if std::mem::size_of_val(src_addr)
            < (*(*self.ofi.comm_group.info_entry).tx_attr).inject_size
        {
            trace!(
                target: "libfabric-sys",
                "Injecting write to PE {} at address {:?}",
                pe,
                remote_dst_addr
            );
            cg.post_put_inject(|| unsafe {
                libfabric_sys::inlined_fi_inject_write(
                    cg.ep,
                    src_addr.as_ptr().cast(),
                    std::mem::size_of_val(src_addr),
                    cg.mapped_addresses[pe],
                    remote_dst_addr as u64,
                    remote_key,
                )
            });
        } else {
            let mut curr_idx = 0;
            while curr_idx < src_addr.len() {
                let msg_len = std::cmp::min(
                    src_addr.len() - curr_idx,
                    (*(*self.ofi.comm_group.info_entry).ep_attr).max_msg_size
                        / std::mem::size_of::<T>(),
                );

                trace!(
                    target: "libfabric-sys",
                    "Posting write to PE {} at address {:?}",
                    pe,
                    remote_dst_addr,
                );
                let local_desc = self.ofi.local_desc_for(
                    src_addr[curr_idx..curr_idx + msg_len].as_ptr() as usize,
                    self.mr_desc,
                );
                cg.post_put(blocking, || unsafe {
                    libfabric_sys::inlined_fi_write(
                        cg.ep,
                        src_addr[curr_idx..curr_idx + msg_len].as_ptr().cast(),
                        std::mem::size_of_val(&src_addr[curr_idx..curr_idx + msg_len]),
                        local_desc,
                        cg.mapped_addresses[pe],
                        remote_dst_addr as u64,
                        remote_key,
                        std::ptr::null_mut(),
                    )
                });

                remote_dst_addr = remote_dst_addr.add(msg_len * std::mem::size_of::<T>());
                curr_idx += msg_len;
            }
        }
    }

    pub(crate) unsafe fn inner_get<T: Copy>(
        &self,
        pe: usize,
        offset: usize,
        dst_addr: &mut [T],
        blocking: bool,
    ) {
        let offset = offset * std::mem::size_of::<T>(); //we allocate memoryregions from libfabric as u8;
        assert!(offset + dst_addr.len() * std::mem::size_of::<T>() <= self.num_bytes()); //we use num_bytes instead of mem.len() to allow for sub-allocations,
        if pe == self.ofi.my_pe {
            std::ptr::copy(
                (self.start() + offset) as *const u8,
                dst_addr.as_mut_ptr() as *mut u8,
                dst_addr.len() * std::mem::size_of::<T>(),
            );
            return;
        }
        #[cfg(feature = "enable-on-node-shmem")]
        if let Some(addr) = self.same_node_addr(pe, offset) {
            std::ptr::copy(
                addr.as_ptr::<u8>(),
                dst_addr.as_mut_ptr() as *mut u8,
                dst_addr.len() * std::mem::size_of::<T>(),
            );
            return;
        }
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));

        let mut remote_src_addr = remote_alloc_info.mem_address.add(offset);
        let remote_key = remote_alloc_info.key;
        // trace!(
        //     "Inner Get: Remote destination address for PE {}: base_addr {:?} offset<T> {} size_of<T> {} len {} {:?}-{:?} {} bytes",
        //     pe,
        //     remote_alloc_info.mem_address(),
        //     offset,
        //     std::mem::size_of::<T>(),
        //     dst_addr.len(),
        //     remote_src_addr,
        //     remote_src_addr.add(std::mem::size_of_val(dst_addr)),
        //     std::mem::size_of_val(dst_addr)
        // );
        let cg = &self.ofi.comm_group;
        if dst_addr.len()
            < (*(*self.ofi.comm_group.info_entry).ep_attr).max_msg_size / std::mem::size_of::<T>()
        {
            // trace!(
            //     target: "libfabric-sys",
            //     "GET: from PE {} at addr {:?} to local addr {:?} len {} {}",
            //     pe,
            //     remote_src_addr,
            //     dst_addr.as_mut_ptr(),
            //     std::mem::size_of_val(dst_addr),
            //     dst_addr.len(),
            // );
            let local_desc = self
                .ofi
                .local_desc_for(dst_addr.as_ptr() as usize, self.mr_desc);
            cg.post_get(blocking, || unsafe {
                libfabric_sys::inlined_fi_read(
                    cg.ep,
                    dst_addr.as_mut_ptr().cast(),
                    std::mem::size_of_val(dst_addr),
                    local_desc,
                    cg.mapped_addresses[pe],
                    remote_src_addr as u64,
                    remote_key,
                    std::ptr::null_mut(),
                )
            });
        } else {
            let mut curr_idx = 0;

            while curr_idx < dst_addr.len() {
                let msg_len = std::cmp::min(
                    dst_addr.len() - curr_idx,
                    (*(*self.ofi.comm_group.info_entry).ep_attr).max_msg_size
                        / std::mem::size_of::<T>(),
                );
                let local_desc = self.ofi.local_desc_for(
                    dst_addr[curr_idx..curr_idx + msg_len].as_ptr() as usize,
                    self.mr_desc,
                );
                cg.post_get(blocking, || unsafe {
                    trace!(
                        target: "libfabric-sys",
                        "GET: from PE {} at addr {:?} to local addr {:?} len {}",
                        pe,
                        remote_src_addr,
                        &mut dst_addr[curr_idx..curr_idx + msg_len] as *mut [T],
                        msg_len * std::mem::size_of::<T>()
                    );
                    libfabric_sys::inlined_fi_read(
                        cg.ep,
                        dst_addr[curr_idx..curr_idx + msg_len].as_mut_ptr().cast(),
                        std::mem::size_of_val(&dst_addr[curr_idx..curr_idx + msg_len]),
                        local_desc,
                        cg.mapped_addresses[pe],
                        remote_src_addr as u64,
                        remote_key,
                        std::ptr::null_mut(),
                    )
                });
                remote_src_addr = remote_src_addr.add(msg_len * std::mem::size_of::<T>());
                curr_idx += msg_len;
            }
        }
    }

    #[inline(never)]
    pub(crate) unsafe fn inner_get_small<T: Copy>(
        &self,
        pe: usize,
        offset: usize,
        dst_addr: &mut [T],
        blocking: bool,
    ) {
        let offset = offset * std::mem::size_of::<T>(); //we allocate memoryregions from libfabric as u8;
        assert!(offset + dst_addr.len() * std::mem::size_of::<T>() <= self.num_bytes()); //we use num_bytes instead of mem.len() to allow for sub-allocations,
        if pe == self.ofi.my_pe {
            std::ptr::copy(
                (self.start() + offset) as *const u8,
                dst_addr.as_mut_ptr() as *mut u8,
                dst_addr.len() * std::mem::size_of::<T>(),
            );
            return;
        }
        #[cfg(feature = "enable-on-node-shmem")]
        if let Some(addr) = self.same_node_addr(pe, offset) {
            std::ptr::copy(
                addr.as_ptr::<u8>(),
                dst_addr.as_mut_ptr() as *mut u8,
                dst_addr.len() * std::mem::size_of::<T>(),
            );
            return;
        }
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));

        let remote_src_addr = remote_alloc_info.mem_address.add(offset);
        let remote_key = remote_alloc_info.key;
        let cg = &self.ofi.comm_group;
        let local_desc = self
            .ofi
            .local_desc_for(dst_addr.as_ptr() as usize, self.mr_desc);
        cg.post_get(blocking, || unsafe {
            libfabric_sys::inlined_fi_read(
                cg.ep,
                dst_addr.as_mut_ptr().cast(),
                std::mem::size_of_val(dst_addr),
                local_desc,
                cg.mapped_addresses[pe],
                remote_src_addr as u64,
                remote_key,
                std::ptr::null_mut(),
            )
        });
    }

    /// Ticket-based counterpart to [`Self::inner_put`], used only by the Future-returning async
    /// `put`/`put_buffer`/`put_all`/`put_all_buffer` paths (Stage 2). Deliberately skips the
    /// inject fast path: `fi_inject_write` accepts no context and generates no completion, so
    /// there is nothing to leak a [`Ticket`] into — every remote chunk here goes through
    /// `fi_writemsg` with `FI_COMPLETION` regardless of size (single-iov batches that fit in
    /// `inject_size` also set `FI_INJECT`, see below). Stage 4: each `fi_writemsg` call
    /// batches up to `tx_attr.rma_iov_limit` `max_msg_size`-sized segments into one message
    /// (one `Ticket` per *batch*, not per segment) instead of issuing one message per segment —
    /// `src_addr`/the remote destination are both flat contiguous buffers, so a batch is just a
    /// contiguous run of segments described by several iovec/rma_iov entries in one call. Falls
    /// back to the Stage-2 one-segment-per-message shape automatically when the provider reports
    /// `rma_iov_limit <= 1` (verbs often does). Returns one `Ticket` per batch (empty for the
    /// local-copy/same-node-shmem fast paths, already satisfied synchronously).
    pub(crate) unsafe fn inner_put_ticketed<T: Copy>(
        &self,
        pe: usize,
        offset: usize,
        src_addr: &[T],
    ) -> Vec<Arc<Ticket>> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + src_addr.len() * std::mem::size_of::<T>() <= self.num_bytes());
        if pe == self.ofi.my_pe {
            std::ptr::copy(
                src_addr.as_ptr() as *const u8,
                (self.start() + offset) as *mut u8,
                src_addr.len() * std::mem::size_of::<T>(),
            );
            return Vec::new();
        }
        #[cfg(feature = "enable-on-node-shmem")]
        if let Some(addr) = self.same_node_addr(pe, offset) {
            std::ptr::copy(
                src_addr.as_ptr() as *const u8,
                addr.as_ptr::<u8>() as *mut u8,
                src_addr.len() * std::mem::size_of::<T>(),
            );
            return Vec::new();
        }
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));
        let remote_dst_base = remote_alloc_info.mem_address().add(offset);
        let remote_key = remote_alloc_info.key();
        let cg = &self.ofi.comm_group;

        let elem_size = std::mem::size_of::<T>();
        let max_msg_elems = std::cmp::max(1, (*(*cg.info_entry).ep_attr).max_msg_size / elem_size);
        let rma_iov_limit = std::cmp::max(1, (*(*cg.info_entry).tx_attr).rma_iov_limit as usize);

        // Whole src_addr is one contiguous buffer (staged as a single pool block when
        // staged), so one lookup covers every segment in every batch below.
        let local_desc = self
            .ofi
            .local_desc_for(src_addr.as_ptr() as usize, self.mr_desc);

        let mut tickets = Vec::new();
        let mut curr_idx = 0;
        while curr_idx < src_addr.len() {
            let mut iovs = Vec::with_capacity(rma_iov_limit);
            let mut rma_iovs = Vec::with_capacity(rma_iov_limit);
            let mut batch_elems = 0usize;
            while iovs.len() < rma_iov_limit && curr_idx + batch_elems < src_addr.len() {
                let seg_start = curr_idx + batch_elems;
                let seg_len = std::cmp::min(max_msg_elems, src_addr.len() - seg_start);
                let byte_len = seg_len * elem_size;
                iovs.push(libfabric_sys::iovec {
                    iov_base: src_addr.as_ptr().add(seg_start) as *mut std::ffi::c_void,
                    iov_len: byte_len,
                });
                rma_iovs.push(libfabric_sys::fi_rma_iov {
                    addr: remote_dst_base.add(seg_start * elem_size) as u64,
                    len: byte_len,
                    key: remote_key,
                });
                batch_elems += seg_len;
            }
            let mut desc = vec![local_desc; iovs.len()];
            // Without FI_MR_LOCAL rxm registers every source iov through the MR cache,
            // and a cache miss (unregistered heap source) mallocs under the memhooks
            // mm_lock -- which deadlocks against a concurrent free() trimming the heap
            // (arena lock -> madvise/brk hook -> mm_lock). FI_INJECT makes rxm copy
            // small payloads into its own registered tx buffer instead; FI_COMPLETION
            // still delivers a CQ entry carrying our context.
            let mut flags = libfabric_sys::FI_COMPLETION as u64;
            if iovs.len() == 1 && batch_elems * elem_size <= self.ofi.inject_size() {
                flags |= libfabric_sys::FI_INJECT as u64;
            }

            let ticket = Ticket::new();
            let ctx = TicketCtx::leak(ticket.clone());
            let msg = libfabric_sys::fi_msg_rma {
                msg_iov: iovs.as_ptr(),
                desc: desc.as_mut_ptr(),
                iov_count: iovs.len(),
                addr: cg.mapped_addresses[pe],
                rma_iov: rma_iovs.as_ptr(),
                rma_iov_count: rma_iovs.len(),
                context: ctx,
                data: 0,
            };
            cg.post_ticketed(&cg.put_cnt, || unsafe {
                libfabric_sys::inlined_fi_writemsg(cg.ep, &msg, flags)
            });
            tickets.push(ticket);

            curr_idx += batch_elems;
        }
        tickets
    }

    /// Ticket-based counterpart to [`Self::inner_get`] — see [`Self::inner_put_ticketed`] for why
    /// there is no small/inject-sized fast path here either (`fi_read`'s non-msg form has no
    /// inject variant to begin with, but this keeps both sides symmetric: always `fi_readmsg`),
    /// and for the Stage 4 iov-batching shape (one `Ticket`/message per batch of up to
    /// `rma_iov_limit` `max_msg_size` segments, not one per segment).
    pub(crate) unsafe fn inner_get_ticketed<T: Copy>(
        &self,
        pe: usize,
        offset: usize,
        dst_addr: &mut [T],
    ) -> Vec<Arc<Ticket>> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + dst_addr.len() * std::mem::size_of::<T>() <= self.num_bytes());
        if pe == self.ofi.my_pe {
            std::ptr::copy(
                (self.start() + offset) as *const u8,
                dst_addr.as_mut_ptr() as *mut u8,
                dst_addr.len() * std::mem::size_of::<T>(),
            );
            return Vec::new();
        }
        #[cfg(feature = "enable-on-node-shmem")]
        if let Some(addr) = self.same_node_addr(pe, offset) {
            std::ptr::copy(
                addr.as_ptr::<u8>(),
                dst_addr.as_mut_ptr() as *mut u8,
                dst_addr.len() * std::mem::size_of::<T>(),
            );
            return Vec::new();
        }
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));
        let remote_src_base = remote_alloc_info.mem_address.add(offset);
        let remote_key = remote_alloc_info.key;
        let cg = &self.ofi.comm_group;

        let elem_size = std::mem::size_of::<T>();
        let max_msg_elems = std::cmp::max(1, (*(*cg.info_entry).ep_attr).max_msg_size / elem_size);
        let rma_iov_limit = std::cmp::max(1, (*(*cg.info_entry).tx_attr).rma_iov_limit as usize);

        let dst_ptr = dst_addr.as_mut_ptr();
        // Whole dst_addr is one contiguous buffer (staged as a single pool block when
        // staged), so one lookup covers every segment in every batch below.
        let local_desc = self.ofi.local_desc_for(dst_ptr as usize, self.mr_desc);
        let mut tickets = Vec::new();
        let mut curr_idx = 0;
        while curr_idx < dst_addr.len() {
            let mut iovs = Vec::with_capacity(rma_iov_limit);
            let mut rma_iovs = Vec::with_capacity(rma_iov_limit);
            let mut batch_elems = 0usize;
            while iovs.len() < rma_iov_limit && curr_idx + batch_elems < dst_addr.len() {
                let seg_start = curr_idx + batch_elems;
                let seg_len = std::cmp::min(max_msg_elems, dst_addr.len() - seg_start);
                let byte_len = seg_len * elem_size;
                iovs.push(libfabric_sys::iovec {
                    iov_base: dst_ptr.add(seg_start) as *mut std::ffi::c_void,
                    iov_len: byte_len,
                });
                rma_iovs.push(libfabric_sys::fi_rma_iov {
                    addr: remote_src_base.add(seg_start * elem_size) as u64,
                    len: byte_len,
                    key: remote_key,
                });
                batch_elems += seg_len;
            }
            let mut desc = vec![local_desc; iovs.len()];

            let ticket = Ticket::new();
            let ctx = TicketCtx::leak(ticket.clone());
            let msg = libfabric_sys::fi_msg_rma {
                msg_iov: iovs.as_ptr(),
                desc: desc.as_mut_ptr(),
                iov_count: iovs.len(),
                addr: cg.mapped_addresses[pe],
                rma_iov: rma_iovs.as_ptr(),
                rma_iov_count: rma_iovs.len(),
                context: ctx,
                data: 0,
            };
            cg.post_ticketed(&cg.get_cnt, || unsafe {
                libfabric_sys::inlined_fi_readmsg(cg.ep, &msg, libfabric_sys::FI_COMPLETION as u64)
            });
            tickets.push(ticket);

            curr_idx += batch_elems;
        }
        tickets
    }

    pub(crate) fn atomic_op_inner<T: 'static>(
        &self,
        pe: usize,
        offset: usize,
        op: &mut AtomicOp<T>,
        // fi_inject_atomic (below) never generates a completion, so there's nothing
        // for `blocking` to wait on here -- see post_put_inject's doc comment.
        _blocking: bool,
    ) {
        #[cfg(feature = "enable-on-node-shmem")]
        {
            let offset_bytes = offset * std::mem::size_of::<T>();
            if let Some(addr) = self.same_node_addr(pe, offset_bytes) {
                crate::lamellae::comm::atomic::net_atomic_op(op, &addr);
                return;
            }
        }
        let offset = offset * std::mem::size_of::<T>(); //we allocate memoryregions from libfabric as u8;
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes()); //we use num_bytes instead of mem.len() to allow for sub-allocations, offset + 1 because atomics operate on a single element and we verifying we arent missaligned
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));
        let remote_dst_addr = unsafe { remote_alloc_info.mem_address().add(offset) };
        let remote_key = remote_alloc_info.key();

        match op {
            AtomicOp::Sub(src) => unsafe {
                Self::negate_atomic_value(src.as_mut().get_unchecked_mut())
            },
            AtomicOp::FetchMin(_)
            | AtomicOp::FetchMax(_)
            | AtomicOp::FetchSum(_)
            | AtomicOp::FetchSub(_)
            | AtomicOp::FetchProd(_)
            | AtomicOp::FetchBitOr(_)
            | AtomicOp::FetchBitXor(_)
            | AtomicOp::FetchBitAnd(_) => {
                panic!("Fetch atomic ops must use the fetch path")
            }
            AtomicOp::Cas => {
                panic!("Compare atomic ops must use the compare path")
            }
            _ => {}
        };
        let src = op.src();
        let buf = unsafe { std::slice::from_raw_parts(src, 1) };
        // let buf = std::slice::from_ref(std::mem::transmute::<&T, &OFI>(&src));
        let cg = &self.ofi.comm_group;
        let data_type = rust_type_to_fi_type::<T>().expect("Unsupported type for atomic operation");
        let op = atomic_op_to_fi_atomic_op(op);
        cg.post_put_inject(|| unsafe {
            libfabric_sys::inlined_fi_inject_atomic(
                cg.ep,
                buf.as_ptr().cast(),
                buf.len(),
                cg.mapped_addresses[pe],
                remote_dst_addr as u64,
                remote_key,
                data_type,
                op,
            )
        });
    }

    pub(crate) fn atomic_fetch_op_inner<T: 'static>(
        &self,
        pe: usize,
        offset: usize,
        op: &mut AtomicOp<T>,
        result: &mut [T],
        blocking: bool,
    ) {
        #[cfg(feature = "enable-on-node-shmem")]
        {
            let offset_bytes = offset * std::mem::size_of::<T>();
            if let Some(addr) = self.same_node_addr(pe, offset_bytes) {
                crate::lamellae::comm::atomic::net_atomic_fetch_op(op, &addr, result.as_mut_ptr());
                return;
            }
        }

        let offset = offset * std::mem::size_of::<T>(); //we allocate memoryregions from libfabric as u8;
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes()); //we use num_bytes instead of mem.len() to allow for sub-allocations, offset + 1 because atomics operate on a single element and we verifying we arent missaligned
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));
        let remote_dst_addr = unsafe { remote_alloc_info.mem_address().add(offset) };
        let remote_key = remote_alloc_info.key();

        let cg = &self.ofi.comm_group;
        let data_type = rust_type_to_fi_type::<T>().expect("Unsupported type for atomic operation");
        match op {
            AtomicOp::FetchSub(src) => unsafe {
                Self::negate_atomic_value(src.as_mut().get_unchecked_mut())
            },
            AtomicOp::Min(_)
            | AtomicOp::Max(_)
            | AtomicOp::Sum(_)
            | AtomicOp::Sub(_)
            | AtomicOp::Prod(_)
            | AtomicOp::BitOr(_)
            | AtomicOp::BitXor(_)
            | AtomicOp::BitAnd(_) => {
                panic!("Non-fetch atomic ops must use the non-fetch path")
            }
            AtomicOp::Cas => {
                panic!("Compare atomic ops must use the compare path")
            }
            _ => {}
        };
        let src = op.src();
        let buf = unsafe { std::slice::from_raw_parts(src, 1) };
        cg.post_get(blocking, || unsafe {
            libfabric_sys::inlined_fi_fetch_atomic(
                cg.ep,
                buf.as_ptr().cast(),
                buf.len(),
                self.mr_desc,
                result.as_mut_ptr().cast(),
                std::ptr::null_mut(),
                cg.mapped_addresses[pe],
                remote_dst_addr as u64,
                remote_key,
                data_type,
                atomic_op_to_fi_atomic_op(op),
                std::ptr::null_mut(),
            )
        });
    }

    pub(crate) fn atomic_compare_exchange_op_inner<T: 'static + Copy>(
        &self,
        pe: usize,
        offset: usize,
        current: *const T,
        new: *const T,
        result: &mut [T],
        blocking: bool,
    ) {
        #[cfg(feature = "enable-on-node-shmem")]
        {
            let offset_bytes = offset * std::mem::size_of::<T>();
            if let Some(addr) = self.same_node_addr(pe, offset_bytes) {
                result[0] = match crate::lamellae::comm::atomic::net_atomic_compare_exchange(
                    unsafe { *current },
                    unsafe { *new },
                    &addr,
                ) {
                    Ok(old) | Err(old) => old,
                };
                return;
            }
        }

        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));
        let remote_dst_addr = unsafe { remote_alloc_info.mem_address().add(offset) };
        let remote_key = remote_alloc_info.key();

        // let new = *(&new as *const T as *const OFI);
        // let current = *(&current as *const T as *const OFI);
        // let res = &mut *(result as *mut [T] as *mut [OFI]);
        let cg = &self.ofi.comm_group;

        cg.post_get(blocking, || unsafe {
            libfabric_sys::inlined_fi_compare_atomic(
                cg.ep,
                new.cast(),
                1,
                std::ptr::null_mut(),
                current.cast(),
                std::ptr::null_mut(),
                result.as_mut_ptr().cast(),
                std::ptr::null_mut(),
                cg.mapped_addresses[pe],
                remote_dst_addr as u64,
                remote_key,
                rust_type_to_fi_type::<T>().expect("Unsupported type for atomic operation"),
                libfabric_sys::fi_op_FI_CSWAP,
                std::ptr::null_mut(),
            )
        });
    }

    /// Ticket-based counterpart to [`Self::atomic_op_inner`]. As with the put/get ticketed
    /// paths, skips `fi_inject_atomic` (no context, no completion) and always issues via
    /// `fi_atomicmsg` with `FI_COMPLETION`. Atomics operate on a single element, so exactly one
    /// `Ticket` is produced (already-done for the same-node-shmem fast path).
    pub(crate) fn atomic_op_ticketed<T: 'static>(
        &self,
        pe: usize,
        offset: usize,
        op: &mut AtomicOp<T>,
    ) -> Arc<Ticket> {
        #[cfg(feature = "enable-on-node-shmem")]
        {
            let offset_bytes = offset * std::mem::size_of::<T>();
            if let Some(addr) = self.same_node_addr(pe, offset_bytes) {
                crate::lamellae::comm::atomic::net_atomic_op(op, &addr);
                return Ticket::new_done();
            }
        }
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));
        let remote_dst_addr = unsafe { remote_alloc_info.mem_address().add(offset) };
        let remote_key = remote_alloc_info.key();

        match op {
            AtomicOp::Sub(src) => unsafe {
                Self::negate_atomic_value(src.as_mut().get_unchecked_mut())
            },
            AtomicOp::FetchMin(_)
            | AtomicOp::FetchMax(_)
            | AtomicOp::FetchSum(_)
            | AtomicOp::FetchSub(_)
            | AtomicOp::FetchProd(_)
            | AtomicOp::FetchBitOr(_)
            | AtomicOp::FetchBitXor(_)
            | AtomicOp::FetchBitAnd(_) => {
                panic!("Fetch atomic ops must use the fetch path")
            }
            AtomicOp::Cas => {
                panic!("Compare atomic ops must use the compare path")
            }
            _ => {}
        };
        let src = op.src();
        let buf = unsafe { std::slice::from_raw_parts(src, 1) };
        let cg = &self.ofi.comm_group;
        let data_type = rust_type_to_fi_type::<T>().expect("Unsupported type for atomic operation");
        let fi_op = atomic_op_to_fi_atomic_op(op);

        let ticket = Ticket::new();
        let ctx = TicketCtx::leak(ticket.clone());
        let mut desc = [std::ptr::null_mut::<std::ffi::c_void>()];
        let ioc = libfabric_sys::fi_ioc {
            addr: buf.as_ptr() as *mut std::ffi::c_void,
            count: 1,
        };
        let rma_ioc = libfabric_sys::fi_rma_ioc {
            addr: remote_dst_addr as u64,
            count: 1,
            key: remote_key,
        };
        let msg = libfabric_sys::fi_msg_atomic {
            msg_iov: &ioc,
            desc: desc.as_mut_ptr(),
            iov_count: 1,
            addr: cg.mapped_addresses[pe],
            rma_iov: &rma_ioc,
            rma_iov_count: 1,
            datatype: data_type,
            op: fi_op,
            context: ctx,
            data: 0,
        };
        cg.post_ticketed(&cg.put_cnt, || unsafe {
            libfabric_sys::inlined_fi_atomicmsg(cg.ep, &msg, libfabric_sys::FI_COMPLETION as u64)
        });
        ticket
    }

    /// Ticket-based counterpart to [`Self::atomic_fetch_op_inner`]; see
    /// [`Self::atomic_op_ticketed`] for the inject-skip rationale.
    pub(crate) fn atomic_fetch_op_ticketed<T: 'static>(
        &self,
        pe: usize,
        offset: usize,
        op: &mut AtomicOp<T>,
        result: &mut [T],
    ) -> Arc<Ticket> {
        #[cfg(feature = "enable-on-node-shmem")]
        {
            let offset_bytes = offset * std::mem::size_of::<T>();
            if let Some(addr) = self.same_node_addr(pe, offset_bytes) {
                crate::lamellae::comm::atomic::net_atomic_fetch_op(op, &addr, result.as_mut_ptr());
                return Ticket::new_done();
            }
        }

        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));
        let remote_dst_addr = unsafe { remote_alloc_info.mem_address().add(offset) };
        let remote_key = remote_alloc_info.key();

        let cg = &self.ofi.comm_group;
        let data_type = rust_type_to_fi_type::<T>().expect("Unsupported type for atomic operation");
        match op {
            AtomicOp::FetchSub(src) => unsafe {
                Self::negate_atomic_value(src.as_mut().get_unchecked_mut())
            },
            AtomicOp::Min(_)
            | AtomicOp::Max(_)
            | AtomicOp::Sum(_)
            | AtomicOp::Sub(_)
            | AtomicOp::Prod(_)
            | AtomicOp::BitOr(_)
            | AtomicOp::BitXor(_)
            | AtomicOp::BitAnd(_) => {
                panic!("Non-fetch atomic ops must use the non-fetch path")
            }
            AtomicOp::Cas => {
                panic!("Compare atomic ops must use the compare path")
            }
            _ => {}
        };
        let src = op.src();
        let buf = unsafe { std::slice::from_raw_parts(src, 1) };
        let fi_op = atomic_op_to_fi_atomic_op(op);

        let ticket = Ticket::new();
        let ctx = TicketCtx::leak(ticket.clone());
        let mut desc = [std::ptr::null_mut::<std::ffi::c_void>()];
        let mut result_desc = [std::ptr::null_mut::<std::ffi::c_void>()];
        let ioc = libfabric_sys::fi_ioc {
            addr: buf.as_ptr() as *mut std::ffi::c_void,
            count: 1,
        };
        let rma_ioc = libfabric_sys::fi_rma_ioc {
            addr: remote_dst_addr as u64,
            count: 1,
            key: remote_key,
        };
        let mut result_ioc = libfabric_sys::fi_ioc {
            addr: result.as_mut_ptr() as *mut std::ffi::c_void,
            count: 1,
        };
        let msg = libfabric_sys::fi_msg_atomic {
            msg_iov: &ioc,
            desc: desc.as_mut_ptr(),
            iov_count: 1,
            addr: cg.mapped_addresses[pe],
            rma_iov: &rma_ioc,
            rma_iov_count: 1,
            datatype: data_type,
            op: fi_op,
            context: ctx,
            data: 0,
        };
        cg.post_ticketed(&cg.get_cnt, || unsafe {
            libfabric_sys::inlined_fi_fetch_atomicmsg(
                cg.ep,
                &msg,
                &mut result_ioc,
                result_desc.as_mut_ptr(),
                1,
                libfabric_sys::FI_COMPLETION as u64,
            )
        });
        ticket
    }

    /// Ticket-based counterpart to [`Self::atomic_compare_exchange_op_inner`]; see
    /// [`Self::atomic_op_ticketed`] for the inject-skip rationale.
    pub(crate) fn atomic_compare_exchange_op_ticketed<T: 'static + Copy>(
        &self,
        pe: usize,
        offset: usize,
        current: *const T,
        new: *const T,
        result: &mut [T],
    ) -> Arc<Ticket> {
        #[cfg(feature = "enable-on-node-shmem")]
        {
            let offset_bytes = offset * std::mem::size_of::<T>();
            if let Some(addr) = self.same_node_addr(pe, offset_bytes) {
                result[0] = match crate::lamellae::comm::atomic::net_atomic_compare_exchange(
                    unsafe { *current },
                    unsafe { *new },
                    &addr,
                ) {
                    Ok(old) | Err(old) => old,
                };
                return Ticket::new_done();
            }
        }

        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));
        let remote_dst_addr = unsafe { remote_alloc_info.mem_address().add(offset) };
        let remote_key = remote_alloc_info.key();
        let cg = &self.ofi.comm_group;

        let ticket = Ticket::new();
        let ctx = TicketCtx::leak(ticket.clone());
        let mut desc = [std::ptr::null_mut::<std::ffi::c_void>()];
        let mut compare_desc = [std::ptr::null_mut::<std::ffi::c_void>()];
        let mut result_desc = [std::ptr::null_mut::<std::ffi::c_void>()];
        let ioc = libfabric_sys::fi_ioc {
            addr: new as *mut std::ffi::c_void,
            count: 1,
        };
        let compare_ioc = libfabric_sys::fi_ioc {
            addr: current as *mut std::ffi::c_void,
            count: 1,
        };
        let rma_ioc = libfabric_sys::fi_rma_ioc {
            addr: remote_dst_addr as u64,
            count: 1,
            key: remote_key,
        };
        let mut result_ioc = libfabric_sys::fi_ioc {
            addr: result.as_mut_ptr() as *mut std::ffi::c_void,
            count: 1,
        };
        let msg = libfabric_sys::fi_msg_atomic {
            msg_iov: &ioc,
            desc: desc.as_mut_ptr(),
            iov_count: 1,
            addr: cg.mapped_addresses[pe],
            rma_iov: &rma_ioc,
            rma_iov_count: 1,
            datatype: rust_type_to_fi_type::<T>().expect("Unsupported type for atomic operation"),
            op: libfabric_sys::fi_op_FI_CSWAP,
            context: ctx,
            data: 0,
        };
        cg.post_ticketed(&cg.get_cnt, || unsafe {
            libfabric_sys::inlined_fi_compare_atomicmsg(
                cg.ep,
                &msg,
                &compare_ioc,
                compare_desc.as_mut_ptr(),
                1,
                &mut result_ioc,
                result_desc.as_mut_ptr(),
                1,
                libfabric_sys::FI_COMPLETION as u64,
            )
        });
        ticket
    }

    pub(crate) fn allreduce_inplace_inner<T: 'static>(
        &self,
        op: &AllReduceOp,
        src_and_result: &mut [T],
        blocking: bool,
    ) {
        let dst = unsafe {
            std::slice::from_raw_parts_mut(src_and_result.as_mut_ptr(), src_and_result.len())
        };
        self.allreduce_inner(op, src_and_result, dst, blocking)
    }

    pub(crate) fn allreduce_inner<T: 'static>(
        &self,
        op: &AllReduceOp,
        src: &[T],
        result: &mut [T],
        blocking: bool,
    ) {
        // let res = unsafe {&mut *(result as *mut [T] as *mut [OFI])};
        // let buf = unsafe { std::mem::transmute::<&[T], &[OFI]>(src) };
        let cg = &self.ofi.comm_group;
        let mcast_group = self
            .mcast_group
            .as_ref()
            .expect("No multicast group for allreduce");
        cg.post_collective(blocking, || unsafe {
            libfabric_sys::inlined_fi_allreduce(
                cg.ep,
                src.as_ptr().cast(),
                src.len(),
                std::ptr::null_mut(),
                result.as_mut_ptr().cast(),
                std::ptr::null_mut(),
                mcast_group.coll_addr,
                rust_type_to_fi_type::<T>().expect("Unsupported type for allreduce operation"),
                op.into(),
                0,
                std::ptr::null_mut(),
            )
        });
    }
    // pub(crate) fn reduce_inplace_inner<T: 'static>(
    //     &self,
    //     op: &ReduceOp,
    //     root_pe: Option<usize>,
    //     blocking: bool,
    // )  {
    //     let dst = unsafe {std::slice::from_raw_parts_mut(self.start() as *mut T, self.num_bytes()/std::mem::size_of::<T>())};
    //     // let slice_or_pe = if let Some(root) = root_pe {
    //     //     RootOrSliceMut::NotRoot(root)
    //     // }
    //     // else {
    //     //     RootOrSliceMut::Root(dst)
    //     // };

    //     self.reduce_inner(op, slice_or_pe, blocking)
    // }

    pub(crate) fn allgather_inner<T: 'static>(&self, src: &[T], result: &mut [T], blocking: bool) {
        // let res = unsafe {&mut *(result as *mut [T] as *mut [OFI])};
        // let buf = unsafe { std::mem::transmute::<&[T], &[OFI]>(src) };
        let cg = &self.ofi.comm_group;
        let mcast_group = self
            .mcast_group
            .as_ref()
            .expect("No multicast group for allgather");
        cg.post_collective(blocking, || unsafe {
            libfabric_sys::inlined_fi_allgather(
                cg.ep,
                src.as_ptr().cast(),
                src.len(),
                std::ptr::null_mut(),
                result.as_mut_ptr().cast(),
                std::ptr::null_mut(),
                mcast_group.coll_addr,
                rust_type_to_fi_type::<T>().expect("Unsupported type for allgather operation"),
                0,
                std::ptr::null_mut(),
            )
        });
    }

    pub(crate) fn alltoall_inner<T: 'static>(&self, src: &[T], result: &mut [T], blocking: bool) {
        // let res = unsafe {&mut *(result as *mut [T] as *mut [OFI])};
        // let buf = unsafe { std::mem::transmute::<&[T], &[OFI]>(src) };
        let cg = &self.ofi.comm_group;
        let mcast_group = self
            .mcast_group
            .as_ref()
            .expect("No multicast group for alltoall");
        cg.post_collective(blocking, || unsafe {
            libfabric_sys::inlined_fi_alltoall(
                cg.ep,
                src.as_ptr().cast(),
                src.len(),
                std::ptr::null_mut(),
                result.as_mut_ptr().cast(),
                std::ptr::null_mut(),
                mcast_group.coll_addr,
                rust_type_to_fi_type::<T>().expect("Unsupported type for alltoall operation"),
                0,
                std::ptr::null_mut(),
            )
        });
    }

    pub(crate) fn reduce_inner<T: 'static>(
        &self,
        op: &ReduceOp,
        src: &[T],
        slice_or_pe: RootOrSliceMut<T>,
        blocking: bool,
    ) {
        let cg = &self.ofi.comm_group;
        let mcast_group = self
            .mcast_group
            .as_ref()
            .expect("No multicast group for collective reduce");
        let (result, root_pe) = match slice_or_pe {
            RootOrSliceMut::Root(result) => (Some(result), self.ofi.my_pe),
            RootOrSliceMut::NotRoot(root_pe) => (None, root_pe),
        };

        // let buf = unsafe { std::mem::transmute::<&[T], &[OFI]>(src) };
        let res = match result {
            Some(res) => res,
            None => {
                let res_buf =
                    unsafe { std::slice::from_raw_parts_mut(src.as_ptr() as *mut T, src.len()) }; // if result is None, we are doing an in-place reduce or we reduce on a non-root PE, so we can reuse the source buffer as the destination buffer since it is either the destination or will be ignored by non-root PEs
                res_buf
            }
        };
        cg.post_collective(blocking, || unsafe {
            libfabric_sys::inlined_fi_reduce(
                cg.ep,
                src.as_ptr().cast(),
                src.len(),
                std::ptr::null_mut(),
                res.as_mut_ptr().cast(),
                std::ptr::null_mut(),
                mcast_group.coll_addr,
                cg.mapped_addresses[root_pe],
                rust_type_to_fi_type::<T>().expect("Unsupported type for reduce operation"),
                op.into(),
                0,
                std::ptr::null_mut(),
            )
        });
    }

    pub(crate) fn gather_inner<T: 'static>(
        &self,
        src: &[T],
        slice_or_pe: RootOrSliceMut<T>,
        blocking: bool,
    ) {
        let cg = &self.ofi.comm_group;
        let mcast_group = self
            .mcast_group
            .as_ref()
            .expect("No multicast group for collective reduce");
        let (result, root_pe) = match slice_or_pe {
            RootOrSliceMut::Root(result) => (Some(result), self.ofi.my_pe),
            RootOrSliceMut::NotRoot(root_pe) => (None, root_pe),
        };
        let res = match result {
            Some(res) => res,
            None => {
                let res_buf =
                    unsafe { std::slice::from_raw_parts_mut(src.as_ptr() as *mut T, src.len()) }; // if result is None, we are a non-root PE, so we can reuse the source buffer as the destination buffer since it will be ignored.
                res_buf
            }
        };
        cg.post_collective(blocking, || unsafe {
            libfabric_sys::inlined_fi_gather(
                cg.ep,
                src.as_ptr().cast(),
                src.len(),
                std::ptr::null_mut(),
                res.as_mut_ptr().cast(),
                std::ptr::null_mut(),
                mcast_group.coll_addr,
                cg.mapped_addresses[root_pe],
                rust_type_to_fi_type::<T>().expect("Unsupported type for gather operation"),
                0,
                std::ptr::null_mut(),
            )
        });
    }

    pub(crate) fn broadcast_inner<T: 'static>(
        &self,
        root_src: RootSrcOrSliceMut<T>,
        blocking: bool,
    ) {
        let cg = &self.ofi.comm_group;
        let mcast_group = self
            .mcast_group
            .as_ref()
            .expect("No multicast group for collective reduce");
        let (result, root_pe) = match root_src {
            RootSrcOrSliceMut::Root(src) => (src, self.ofi.my_pe),
            RootSrcOrSliceMut::NotRoot(result, root_pe) => (result, root_pe),
        };
        // let res = unsafe {std::mem::transmute::<&mut [T], &mut [OFI]>(result)};
        cg.post_collective(blocking, || unsafe {
            libfabric_sys::inlined_fi_broadcast(
                cg.ep,
                result.as_mut_ptr().cast(),
                result.len(),
                std::ptr::null_mut(),
                mcast_group.coll_addr,
                cg.mapped_addresses[root_pe],
                rust_type_to_fi_type::<T>().expect("Unsupported type for broadcast operation"),
                0,
                std::ptr::null_mut(),
            )
        });
    }

    pub(crate) fn scatter_inner<T: 'static>(
        &self,
        res: &mut [T],
        src_or_root_pe: RootSrcSliceOrNone<'_, T>,
        blocking: bool,
    ) {
        let cg = &self.ofi.comm_group;
        let mcast_group = self
            .mcast_group
            .as_ref()
            .expect("No multicast group for collective reduce");

        let (src, root_pe) = match src_or_root_pe {
            RootSrcSliceOrNone::Root(src) => (src, self.ofi.my_pe),
            RootSrcSliceOrNone::NotRoot(root_pe) => (
                unsafe { std::slice::from_raw_parts(res.as_ptr(), res.len()) },
                root_pe,
            ),
        };

        cg.post_collective(blocking, || unsafe {
            libfabric_sys::inlined_fi_scatter(
                cg.ep,
                src.as_ptr().cast(),
                src.len(),
                std::ptr::null_mut(),
                res.as_mut_ptr().cast(),
                std::ptr::null_mut(),
                mcast_group.coll_addr,
                cg.mapped_addresses[root_pe],
                rust_type_to_fi_type::<T>().expect("Unsupported type for scatter operation"),
                0,
                std::ptr::null_mut(),
            )
        });
    }

    pub(crate) fn reduce_scatter_inner<T: 'static>(
        &self,
        op: &AllReduceOp,
        src: &[T],
        result: &mut [T],
        blocking: bool,
    ) {
        let cg = &self.ofi.comm_group;
        let mcast_group = self
            .mcast_group
            .as_ref()
            .expect("No multicast group for allreduce");
        cg.post_collective(blocking, || unsafe {
            libfabric_sys::inlined_fi_reduce_scatter(
                cg.ep,
                src.as_ptr().cast(),
                src.len(),
                std::ptr::null_mut(),
                result.as_mut_ptr().cast(),
                std::ptr::null_mut(),
                mcast_group.coll_addr,
                rust_type_to_fi_type::<T>().expect("Unsupported type for reduce_scatter operation"),
                op.into(),
                0,
                std::ptr::null_mut(),
            )
        });
    }

    pub(crate) fn wait(&self) {
        self.ofi.comm_group.wait_all()
    }
}

impl Drop for LibfabricSysOptAlloc {
    fn drop(&mut self) {
        let fabric_ref_count = self.decrement_fabric_ref_count();
        // if self.print {
        //     println!(
        //         "[{:?}] Dropping LibfabricSysOptAlloc: {:x} - {:x} ref_cnt(before drop) {}",
        //         std::thread::current().id(),
        //         self.range.start,
        //         self.range.end,
        //         fabric_ref_count
        //     );
        // }
        debug!(target: "libfabric-sys", "Dropping LibfabricSysOptAlloc: {:x} - {:x} ref_cnt(before drop) {}", self.range.start,self.range.end, fabric_ref_count);

        match &self.alloc_table {
            AllocTable::Fabric(alloc_table) => {
                if fabric_ref_count == 2 {
                    debug!(target: "libfabric-sys", "Dropping fabric LibfabricSysOptAlloc: {:?}", self);
                    alloc_table.remove_from_alloc(self);
                }
            }
            AllocTable::Runtime(rt_alloc_table, addr, fabric_alloc_table) => {
                let rt_ref_count = self.decrement_rt_ref_count();
                // if self.print {
                //     println!(
                //         "[{:?}, {:?}] Freeing runtime LibfabricSysOptAlloc: {:?}",
                //         std::time::Instant::now(),
                //         std::thread::current().id(),
                //         self
                //     );
                // }
                if rt_ref_count == 1 {
                    debug!(target: "libfabric-sys", "Freeing runtime LibfabricSysOptAlloc: {:?}",  self);

                    rt_alloc_table.free(*addr).expect(&format!(
                        "[{:?}] Error removing from runtime alloc table {:x}",
                        std::thread::current().id(),
                        addr
                    ));
                }
                if fabric_ref_count == 2 {
                    debug!(target: "libfabric-sys", "Dropping fabric LibfabricSysOptAlloc from rt LibfabricSysOptAlloc: {:?}", self);
                    fabric_alloc_table.remove_from_alloc(self);
                }
            }
        }

        if fabric_ref_count == 1 {
            // Fire-and-forget (`blocking=false`) puts/gets hold no ref on this alloc, so
            // one may still be in flight here: drain before the MR is closed and the
            // mapping released (else the NIC touches memory glibc may already reuse).
            // Skipped after teardown, when the endpoint/cntrs are already closed.
            if !self.ofi.teardown_done.load(Ordering::SeqCst) {
                self.ofi.comm_group.wait_all();
            }
            // Close provider objects before the backing mmap/shared-memory
            // segment is released by field destruction.
            drop(self.mcast_group.take());
            unsafe {
                close_fid("memory region", &mut (*self.mr).fid);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OneSidedLibfabricSysOptAlloc {
    pub(crate) remote_pe: usize,
    pub(crate) alloc: LibfabricSysOptAlloc,
}

unsafe impl Send for OneSidedLibfabricSysOptAlloc {}
unsafe impl Sync for OneSidedLibfabricSysOptAlloc {}

impl OneSidedLibfabricSysOptAlloc {
    pub(crate) fn num_bytes(&self) -> usize {
        self.alloc.num_bytes()
    }
    pub(crate) fn start(&self) -> usize {
        self.alloc.start()
    }
    pub(crate) fn sub_alloc(&self, offset: usize, len: usize) -> AllocResult<Self> {
        let sub_alloc = self.alloc.sub_alloc(offset, len)?;
        Ok(OneSidedLibfabricSysOptAlloc {
            remote_pe: self.remote_pe,
            alloc: sub_alloc,
        })
    }
}

impl From<OneSidedLibfabricSysOptAlloc> for CommAlloc {
    fn from(alloc: OneSidedLibfabricSysOptAlloc) -> Self {
        CommAlloc {
            inner_alloc: Arc::new(CommAllocInner::OneSidedLibfabricSysOptAlloc(alloc)),
        }
    }
}

// Identical impl also exists in libfabric_sys_lamellae/fabric.rs -- gated out
// here when that backend is also enabled to avoid an E0119 conflicting-impl
// error when both `enable-libfabric-sys` and `enable-libfabric-sys-opt` are
// compiled in together (both wrap the same `libfabric_sys::fi_op` type).
#[cfg(not(feature = "enable-libfabric-sys"))]
impl From<&ReduceOp> for libfabric_sys::fi_op {
    fn from(op: &ReduceOp) -> Self {
        match op {
            ReduceOp::Min => libfabric_sys::fi_op_FI_MIN,
            ReduceOp::Max => libfabric_sys::fi_op_FI_MAX,
            ReduceOp::Sum => libfabric_sys::fi_op_FI_SUM,
            ReduceOp::Prod => libfabric_sys::fi_op_FI_PROD,
            // CollectiveReduceOp::LogicalOr => libfabric_sys::fi_op_FI_LOR,
            // CollectiveReduceOp::LogicalXor => libfabric_sys::fi_op_FI_LXOR,
            // CollectiveReduceOp::LogicalAnd => libfabric_sys::fi_op_FI_LAND,
            ReduceOp::BitOr => libfabric_sys::fi_op_FI_BOR,
            ReduceOp::BitXor => libfabric_sys::fi_op_FI_BXOR,
            ReduceOp::BitAnd => libfabric_sys::fi_op_FI_BAND,
        }
    }
}

#[cfg(not(feature = "enable-libfabric-sys"))]
impl From<&ReduceOp> for &libfabric_sys::fi_op {
    fn from(op: &ReduceOp) -> Self {
        match op {
            ReduceOp::Min => &libfabric_sys::fi_op_FI_MIN,
            ReduceOp::Max => &libfabric_sys::fi_op_FI_MAX,
            ReduceOp::Sum => &libfabric_sys::fi_op_FI_SUM,
            ReduceOp::Prod => &libfabric_sys::fi_op_FI_PROD,
            // CollectiveReduceOp::LogicalOr => &libfabric_sys::fi_op_FI_LOR,
            // CollectiveReduceOp::LogicalXor => &libfabric_sys::fi_op_FI_LXOR,
            // CollectiveReduceOp::LogicalAnd => &libfabric_sys::fi_op_FI_LAND,
            ReduceOp::BitOr => &libfabric_sys::fi_op_FI_BOR,
            ReduceOp::BitXor => &libfabric_sys::fi_op_FI_BXOR,
            ReduceOp::BitAnd => &libfabric_sys::fi_op_FI_BAND,
        }
    }
}

use std::{
    future::Future,
    mem::MaybeUninit,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    task::{Poll, Waker},
};

use event_listener::{Event, EventListener, Listener};
use futures_util::task::AtomicWaker;

// use ucx1_sys::*;

use lamellar_ucx_sys::{
    ucp_atomic_op_nbx, ucp_atomic_op_t, ucp_dt_make_contig, ucp_ep_close_nbx, ucp_ep_create,
    ucp_ep_flush_nbx, ucp_ep_h, ucp_ep_params, ucp_ep_params_field, ucp_err_handler,
    ucp_err_handling_mode_t, ucp_get_nbx, ucp_op_attr_t, ucp_put_nbx, ucp_request_check_status,
    ucp_request_free, ucp_request_param_t, ucp_request_param_t__bindgen_ty_1,
    ucp_request_param_t__bindgen_ty_2, ucs_memory_type, ucs_sock_addr, ucs_status_ptr_t,
    ucs_status_t, UCS_PTR_IS_PTR,
};

use parking_lot::Mutex;

use super::{error::Error, memory_region::RKey, worker::Worker};

use tracing::*;

/// Per-op completion signal for the D1 ticket/callback completion model. One `Ticket` is
/// created for every put/get/atomic nbx call that does not complete inline; a strong ref is
/// leaked into the op's `user_data` (see [`completion_cb`]) so UCX's own callback -- invoked
/// from inside `ucp_worker_progress`, never off-thread -- marks it done and frees the
/// underlying UCX request object. Nothing polls `ucp_request_check_status` for these ops
/// anymore; `is_done`/`register_waker` replace that spin.
struct Ticket {
    done: AtomicBool,
    status: Mutex<ucs_status_t>,
    /// D5: lock-free waker slot (was `Mutex<Option<Waker>>`) -- one less lock on the
    /// register/complete path, which is exactly the completion-signalling hot path this
    /// stage is trying to de-contend.
    waker: AtomicWaker,
    /// Owned scratch memory a call site needed UCX to write into (e.g. a discarded
    /// swap-result reply buffer) that must stay valid for exactly as long as this ticket is
    /// live -- i.e. until `completion_cb` fires, whether or not the local `UcxOptRequest`
    /// was dropped early (see `issue_ticketed`'s doc comment: the leaked `Arc<Ticket>` clone
    /// keeps this alive regardless). Never read back; just needs a stable address.
    _scratch: Option<Box<[u8; 8]>>,
}

impl Ticket {
    fn new(scratch: Option<Box<[u8; 8]>>) -> Arc<Self> {
        Arc::new(Ticket {
            done: AtomicBool::new(false),
            status: Mutex::new(ucs_status_t::UCS_INPROGRESS),
            waker: AtomicWaker::new(),
            _scratch: scratch,
        })
    }

    #[inline]
    fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    #[inline]
    fn status(&self) -> ucs_status_t {
        *self.status.lock()
    }

    /// Registers `waker`, then re-checks `done` to catch completion that landed between the
    /// caller's own check and this registration (missed-wakeup guard).
    fn register_waker(&self, waker: &Waker) -> bool {
        self.waker.register(waker);
        self.is_done()
    }

    /// Invoked exactly once: either by [`completion_cb`] (the op's `ucp_*_nbx` call returned a
    /// real in-progress request pointer) or synchronously by the issuing function itself (the
    /// call returned an immediate error pointer, so the callback will never fire).
    fn complete(&self, status: ucs_status_t) {
        *self.status.lock() = status;
        self.done.store(true, Ordering::Release);
        self.waker.wake();
    }
}

/// Completion callback wired via `UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA` on
/// every put/get/atomic nbx call below. `user_data` is the `Arc<Ticket>` pointer leaked by
/// [`issue`] via `Arc::into_raw` for that specific call; per the UCX contract for a request
/// that returns a real (non-NULL, non-error) pointer, this callback fires at most once, so
/// `Arc::from_raw` here reclaims exactly the one ref `issue` leaked -- no double-free, and no
/// leak as long as every call site that does *not* get a callback invocation (an immediate
/// NULL/error return from `ucp_*_nbx`) reclaims and drops its own leaked ticket instead of
/// relying on this callback (see [`issue`]).
///
/// `ucp_request_free` is called here, inside the callback: this mirrors UCX's own examples
/// (e.g. `ucp_client_server.c`'s send callbacks), which free the request object from within
/// its own completion callback -- a request is safe to free once its completion callback has
/// been invoked.
unsafe extern "C" fn completion_cb(
    request: *mut std::ffi::c_void,
    status: ucs_status_t,
    user_data: *mut std::ffi::c_void,
) {
    let ticket = unsafe { Arc::from_raw(user_data as *const Ticket) };
    ticket.complete(status);
    unsafe { ucp_request_free(request) };
}

/// Completion callback for fire-and-forget ops ([`Endpoint::get_untracked`]): nobody observes the
/// result, so there is no ticket and no `user_data`; the callback only releases the request, which
/// UCX requires once an in-flight `ucp_*_nbx` request completes.
unsafe extern "C" fn free_request_cb(
    request: *mut std::ffi::c_void,
    _status: ucs_status_t,
    _user_data: *mut std::ffi::c_void,
) {
    unsafe { ucp_request_free(request) };
}

/// Local-completion state of a [`UcxOptRequest`], independent of the per-`Endpoint` epoch flush
/// used for remote-completion (see [`EpochFlush`]).
enum LocalState {
    /// No raw UCX request object at all: either the `ucp_*_nbx` call completed inline (`Ok`)
    /// or returned an immediate error pointer before any callback could fire (`Err`).
    Resolved(Result<(), Error>),
    /// A ticket-tracked in-flight request; resolved by [`completion_cb`].
    Pending(Arc<Ticket>),
    /// An untracked in-flight raw UCX request (see [`issue_raw`]): no `Ticket`/callback at all,
    /// just the raw `ucs_status_ptr_t` polled directly with `ucp_request_check_status`, exactly
    /// like plain `ucx_lamellae`. Only produced for the synchronous unmanaged-put fast path,
    /// which always drives this to `Resolved` via `wait()` before the `UcxOptRequest` can be
    /// dropped -- unlike `Pending`, nothing else will ever free this request, so it must never
    /// be dropped while still in this state.
    RawPending(ucs_status_ptr_t),
}

/// Builds the `user_data`-leaking half of every ticket-based nbx call: leaks one strong ref of
/// a fresh [`Ticket`] and hands its raw pointer to `issue`, then resolves the trichotomy UCX's
/// `ucp_*_nbx` routines share (NULL / error pointer / real pending pointer) into a
/// [`LocalState`], reclaiming the leaked ticket itself whenever `completion_cb` will never be
/// invoked for this call.
fn issue_ticketed(
    scratch: Option<Box<[u8; 8]>>,
    issue: impl FnOnce(*mut std::ffi::c_void) -> ucs_status_ptr_t,
) -> LocalState {
    let ticket = Ticket::new(scratch);
    let leaked = Arc::into_raw(ticket.clone()) as *mut std::ffi::c_void;
    let request = issue(leaked);
    if request.is_null() {
        // Inline completion: per the UCX nbx contract, the callback is never invoked in this
        // case, so `completion_cb` will not reclaim `leaked` -- do it ourselves.
        unsafe { drop(Arc::from_raw(leaked as *const Ticket)) };
        LocalState::Resolved(Ok(()))
    } else if UCS_PTR_IS_PTR(request) {
        LocalState::Pending(ticket)
    } else {
        // Immediate error: likewise no callback invocation coming.
        unsafe { drop(Arc::from_raw(leaked as *const Ticket)) };
        LocalState::Resolved(Error::from_ptr(request))
    }
}

/// Issues an nbx call with no completion callback wired at all -- no `UCP_OP_ATTR_FIELD_CALLBACK`,
/// no `UCP_OP_ATTR_FIELD_USER_DATA`, no `Ticket` allocation. Used only by the synchronous
/// unmanaged-put fast path, which always blocks on local completion (via [`UcxOptRequest::wait`])
/// immediately after issuing and so never needs a waker -- exactly the case plain `ucx_lamellae`
/// handles by polling the raw request pointer directly. This skips the per-op `Arc<Ticket>`
/// allocation and callback indirection that [`issue_ticketed`] pays for on every op to support
/// async/managed completion, which the unmanaged case never uses.
fn issue_raw(issue: impl FnOnce() -> ucs_status_ptr_t) -> LocalState {
    let request = issue();
    if request.is_null() {
        LocalState::Resolved(Ok(()))
    } else if UCS_PTR_IS_PTR(request) {
        LocalState::RawPending(request)
    } else {
        LocalState::Resolved(Error::from_ptr(request))
    }
}

/// A managed put/atomic-store's remote-completion wait: which [`Endpoint`] to flush, and the
/// epoch target (see [`EpochFlush`]) that flush must reach before the op is remotely visible.
pub(crate) struct EpochWait {
    endpoint: Arc<Endpoint>,
    target: u64,
    /// D5 fix: true once this wait has claimed driver status for the currently in-flight flush
    /// that will (eventually) satisfy `target`. Consulted on every poll of this SAME `EpochWait`
    /// so a re-poll never re-decides driver status from scratch -- `inflight` being non-`None`
    /// no longer means "someone else owns it", it may mean "I do, from my own last poll" (see
    /// [`EpochFlush`]'s doc comment for why only the driver may ever poll the shared request).
    is_driver: bool,
    /// Parked-on-`settled` registration for a non-driver waiter. Must outlive the poll that
    /// created it: dropping an `EventListener` unregisters it, so a listener held only as a
    /// local would never be woken by the driver's `notify` and the task would hang forever.
    listener: Option<EventListener>,
}

pub(crate) struct UcxOptRequest {
    local: LocalState,
    /// Only set while the request is in flight (needed to drive progress); a request that is already
    /// `Resolved` doesn't clone the worker, so inline-completed ops don't touch the shared refcount.
    worker: Option<Arc<Worker>>,
    epoch_wait: Option<EpochWait>,
}

unsafe impl Sync for UcxOptRequest {}
unsafe impl Send for UcxOptRequest {}

impl UcxOptRequest {
    /// Constructor for the [`issue_ticketed`] result (D1 callback-based completion).
    #[inline]
    fn new_pending_local(local: LocalState, worker: &Arc<Worker>, epoch_wait: Option<EpochWait>) -> Self {
        let worker = match local {
            LocalState::Resolved(_) => None,
            _ => Some(worker.clone()),
        };
        Self {
            local,
            worker,
            epoch_wait,
        }
    }

    /// Drives `self.local` to `Resolved`, returning `Pending{acquired}` if a ticket/raw
    /// request is still in flight. `acquired` says whether *this* call took the worker's
    /// progress lock (D5): callers hedge with a self-rewake/spin only when it's `false`,
    /// since a caller that got the lock already ran a capped `ucp_worker_progress` pass and
    /// can trust the ticket's registered waker (fired by `completion_cb` from whichever
    /// thread eventually processes the completion) instead of hammering progress via re-poll.
    ///
    /// `blocking` selects which of the worker's two progress primitives this call uses:
    /// `false` (async `poll_wait`) uses `try_progress`'s try-lock, since a `poll()` call must
    /// never block; `true` (synchronous `wait`) uses `progress_blocking`'s real blocking
    /// `lock()`, which is required for correctness here -- a pure try-lock-and-spin loop has
    /// no anti-starvation guarantee, and under heavy contention (many threads all spinning
    /// `try_progress` with no queuing) that produced a real livelock (every thread pegged at
    /// 100% CPU, zero forward progress) in an actual np=4 correctness run.
    fn poll_local(&mut self, waker: &Waker, blocking: bool) -> LocalPoll {
        let progress = |worker: &Worker| -> bool {
            if blocking {
                worker.progress_blocking(32);
                true
            } else {
                worker.try_progress(32)
            }
        };
        match &self.local {
            LocalState::Resolved(res) => LocalPoll::Ready(*res),
            LocalState::Pending(ticket) => {
                // Fast path.
                if ticket.is_done() {
                    let status = ticket.status();
                    let res = Error::from_status(status);
                    self.local = LocalState::Resolved(res);
                    return LocalPoll::Ready(res);
                }
                let acquired = progress(self.worker.as_ref().expect("in-flight request without a worker"));
                if ticket.is_done() {
                    let status = ticket.status();
                    let res = Error::from_status(status);
                    self.local = LocalState::Resolved(res);
                    return LocalPoll::Ready(res);
                }
                // Missed-wakeup guard: register, then re-check in case completion_cb ran
                // (on another thread progressing the same worker) between the check above
                // and this registration.
                if ticket.register_waker(waker) {
                    let status = ticket.status();
                    let res = Error::from_status(status);
                    self.local = LocalState::Resolved(res);
                    return LocalPoll::Ready(res);
                }
                LocalPoll::Pending { acquired }
            }
            LocalState::RawPending(request) => {
                // No waker to register here -- there is no callback to wake it, so (like
                // plain `ucx_lamellae`'s equivalent path) this relies on the caller re-polling,
                // which is exactly what `wait()`'s loop below does.
                let acquired = progress(self.worker.as_ref().expect("in-flight request without a worker"));
                let request = *request;
                let status = unsafe { ucp_request_check_status(request as _) };
                if status == ucs_status_t::UCS_INPROGRESS {
                    LocalPoll::Pending { acquired }
                } else {
                    unsafe { ucp_request_free(request as _) };
                    let res = Error::from_status(status);
                    self.local = LocalState::Resolved(res);
                    LocalPoll::Ready(res)
                }
            }
        }
    }

    /// Non-blocking, single-shot-per-call version of `wait`. Caller must keep
    /// polling (e.g. via a waker) until this returns `Poll::Ready`.
    #[inline]
    pub(crate) fn poll_wait(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Error>> {
        match self.poll_local(cx.waker(), false) {
            LocalPoll::Pending { acquired } => {
                // D5/U3: only hedge with an immediate self-rewake when we couldn't take the
                // progress lock (someone else is already progressing this worker). If we did
                // take it and still aren't done, trust the ticket's registered waker instead
                // of hammering the executor with a re-poll that can't make progress anyway.
                if !acquired {
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
            LocalPoll::Ready(Err(e)) => Poll::Ready(Err(e)),
            LocalPoll::Ready(Ok(())) => match &mut self.epoch_wait {
                Some(ew) => {
                    let endpoint = ew.endpoint.clone();
                    endpoint.poll_wait_epoch(ew, cx)
                }
                None => Poll::Ready(Ok(())),
            },
        }
    }

    pub(crate) fn wait(mut self) -> Result<(), Error> {
        // No waker in a blocking wait -- spin `poll_local` with a no-op waker, using the
        // worker's fair blocking progress lock (see `poll_local`'s doc comment for why this
        // must not be the async path's try-lock-and-spin).
        let noop_waker = futures_util::task::noop_waker();
        loop {
            match self.poll_local(&noop_waker, true) {
                LocalPoll::Ready(res) => break res?,
                LocalPoll::Pending { .. } => {}
            }
        }
        if let Some(ew) = &mut self.epoch_wait {
            let endpoint = ew.endpoint.clone();
            endpoint.wait_epoch(ew)?;
        }
        Ok(())
    }
}

/// Result of [`UcxOptRequest::poll_local`] -- see its doc comment for the `acquired` semantics.
enum LocalPoll {
    Ready(Result<(), Error>),
    Pending { acquired: bool },
}

impl Drop for UcxOptRequest {
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop UcxOptRequest");
        // LocalState::Pending is never dropped while still pending from any of this module's
        // call sites: `wait`/`poll_wait` always drive it to `Resolved` first. If a future
        // caller *did* drop a `Pending` ticket early, the leaked ticket ref is still safely
        // reclaimed by `completion_cb` whenever UCX gets around to firing it -- the ticket
        // itself has no borrowed state, so an unread completion is a missed wakeup, not UB.
        //
        // LocalState::RawPending is different: there is no callback to ever free it, so unlike
        // `Pending`, dropping it here would leak the underlying UCX request object. Every
        // call site drives it to `Resolved` via `wait()` before returning, so this should be
        // unreachable; assert it rather than silently leaking if that invariant is ever broken.
        debug_assert!(
            !matches!(self.local, LocalState::RawPending(_)),
            "UcxOptRequest dropped with a RawPending local completion still in flight -- leaked a UCX request"
        );
        trace!(target: "drop", "end drop UcxOptRequest");
    }
}

/// A single `ucp_ep_flush_nbx` issued to reach at least `target` posted ops (see [`EpochFlush`]).
struct InFlightFlush {
    target: u64,
    request: UcxOptRequest,
}

/// D2: per-`Endpoint` remote-completion tracking, replacing the whole-worker flush previously
/// used to wait for managed puts/atomic-stores to become visible on this PE.
///
/// `posted` is incremented by exactly one, strictly *after* `issue_ticketed` returns for the op
/// being reserved (never before) -- so a flush that reads `posted == s` is guaranteed every op
/// with epoch <= s was already posted to the NIC by the time the flush was issued. At most one
/// `ucp_ep_flush_nbx` is ever in flight per endpoint: concurrent waiters coalesce onto it, and
/// once it completes, `flushed` is raised to the snapshot it covered.
///
/// Only the caller that creates an `InFlightFlush` ("the driver") ever polls its `request` --
/// that request's `Ticket` has a single-slot `AtomicWaker`, so a second caller registering its
/// own waker there would silently clobber the driver's registration and be lost forever (this
/// was a real livelock: `put_buffer_test` UnsafeArray np=2 repeatedly coalesces multiple waiters
/// onto the same endpoint's flush). Non-driver callers instead park on `settled`, a proper
/// multi-listener broadcast, and are woken (to recheck `flushed`) whenever the driver resolves
/// its flush, whether that satisfies their own target or not.
#[derive(Default)]
struct EpochFlush {
    posted: AtomicU64,
    flushed: AtomicU64,
    inflight: Mutex<Option<InFlightFlush>>,
    settled: Event,
}

#[derive(Debug)]
pub(crate) struct Endpoint {
    pub(crate) worker: Arc<Worker>,
    pub(crate) handle: ucp_ep_h,
    epoch: EpochFlush,
}

unsafe impl Send for Endpoint {}
unsafe impl Sync for Endpoint {}

impl std::fmt::Debug for EpochFlush {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EpochFlush")
            .field("posted", &self.posted.load(Ordering::Relaxed))
            .field("flushed", &self.flushed.load(Ordering::Relaxed))
            .finish()
    }
}

impl Endpoint {
    pub(crate) fn new(worker: Arc<Worker>, remote_address: &[u8]) -> Result<Arc<Endpoint>, Error> {
        let params = ucp_ep_params {
            field_mask: (ucp_ep_params_field::UCP_EP_PARAM_FIELD_REMOTE_ADDRESS).0 as u64,
            address: remote_address.as_ptr() as *const _ as *mut _,
            flags: 0,
            name: std::ptr::null(),
            conn_request: std::ptr::null_mut(),
            user_data: std::ptr::null_mut(),
            err_handler: ucp_err_handler {
                cb: None,
                arg: std::ptr::null_mut(),
            },
            err_mode: ucp_err_handling_mode_t::UCP_ERR_HANDLING_MODE_NONE,
            sockaddr: ucs_sock_addr {
                addr: std::ptr::null_mut(),
                addrlen: 0,
            },
            local_sockaddr: ucs_sock_addr {
                addr: std::ptr::null_mut(),
                addrlen: 0,
            },
        };
        let mut handle = MaybeUninit::uninit();
        // let lock_handle = worker.lock.lock();
        let status = unsafe { ucp_ep_create(worker.handle, &params, handle.as_mut_ptr()) };
        Error::from_status(status)?;
        // let request_size = worker.request_size();
        // drop(lock_handle);

        let ep = Arc::new(Endpoint {
            worker,
            // request_size,
            handle: unsafe { handle.assume_init() },
            epoch: EpochFlush::default(),
        });
        Ok(ep)
    }

    /// Issues one `ucp_ep_flush_nbx` on this endpoint, ticket-tracked like every other op.
    fn issue_flush(&self) -> UcxOptRequest {
        let local = issue_ticketed(None, |user_data| unsafe {
            ucp_ep_flush_nbx(
                self.handle,
                &ucp_request_param_t {
                    op_attr_mask: ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA as u32,
                    flags: 0,
                    request: std::ptr::null_mut(),
                    cb: ucp_request_param_t__bindgen_ty_1 {
                        send: Some(completion_cb),
                    },
                    datatype: 0,
                    user_data,
                    reply_buffer: std::ptr::null_mut(),
                    memory_type: ucs_memory_type::UCS_MEMORY_TYPE_HOST,
                    recv_info: ucp_request_param_t__bindgen_ty_2 {
                        length: std::ptr::null_mut(),
                    },
                    memh: std::ptr::null_mut(),
                },
            )
        });
        UcxOptRequest::new_pending_local(local, &self.worker, None)
    }

    /// Non-blocking: is epoch `ew.target` on this endpoint remotely visible yet? Coalesces
    /// concurrent callers onto a single in-flight `ucp_ep_flush_nbx` (see [`EpochFlush`]).
    ///
    /// Only the caller that currently holds driver status (`ew.is_driver`) for the in-flight
    /// flush polls its `request` directly; everyone else parks on `settled` instead of racing
    /// to register on the same request's single-slot ticket waker (see [`EpochFlush`]'s doc
    /// comment -- this used to lose a coalesced caller's wakeup permanently).
    fn poll_wait_epoch(&self, ew: &mut EpochWait, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Error>> {
        let target = ew.target;
        loop {
            // A wait that owns the in-flight flush must drive it to completion even if a
            // different flush already raised `flushed` past `target`: returning early would
            // orphan the registered flush, and everyone parked behind it would never wake.
            if !ew.is_driver && self.epoch.flushed.load(Ordering::Acquire) >= target {
                return Poll::Ready(Ok(()));
            }
            if ew.is_driver {
                let mut inflight = self.epoch.inflight.lock();
                let flush = inflight
                    .as_mut()
                    .expect("driver's own InFlightFlush missing from EpochFlush::inflight");
                let flush_target = flush.target;
                match flush.request.poll_wait(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(res) => {
                        // Publish `flushed` BEFORE clearing `inflight`: a waiter that sees
                        // `inflight == None` must also see the flush's coverage, otherwise it
                        // starts a redundant flush that the finishing driver then orphans.
                        if res.is_ok() {
                            self.epoch.flushed.fetch_max(flush_target, Ordering::AcqRel);
                        }
                        inflight.take();
                        drop(inflight);
                        ew.is_driver = false;
                        // Wake every coalesced non-driver waiter so they recheck `flushed`
                        // (whether or not this particular flush satisfies their own target).
                        self.epoch.settled.notify(usize::MAX);
                        match res {
                            Ok(()) => {
                                // loop back: this flush may not have covered `target` if more
                                // ops were posted after it was issued -- re-check and possibly
                                // become driver of a new one.
                            }
                            Err(e) => return Poll::Ready(Err(e)),
                        }
                    }
                }
            } else {
                let mut inflight = self.epoch.inflight.lock();
                if self.epoch.flushed.load(Ordering::Acquire) >= target {
                    return Poll::Ready(Ok(()));
                }
                if inflight.is_none() {
                    let s = self.epoch.posted.load(Ordering::Acquire);
                    let request = self.issue_flush();
                    *inflight = Some(InFlightFlush { target: s, request });
                    ew.is_driver = true;
                    continue; // re-enter the loop and drive it via the `ew.is_driver` branch
                }
                // Someone else already owns the in-flight flush's ticket registration -- do
                // not touch `flush.request` ourselves. Park on the broadcast instead.
                //
                // The listener MUST be registered while `inflight` is still locked: the
                // driver clears `inflight` under this same lock before it notifies, so a
                // listener created after dropping the lock can miss that notification
                // entirely and then sleep with no flush left in flight to wake it.
                //
                // Reuse the listener from a previous poll of this same wait so its waker
                // registration survives between polls (see `EpochWait::listener`).
                let mut listener = match ew.listener.take() {
                    Some(l) => l,
                    None => self.epoch.settled.listen(),
                };
                drop(inflight);
                // Listen-then-recheck: catch a `settled` notification that landed between our
                // check above and this registration.
                if self.epoch.flushed.load(Ordering::Acquire) >= target {
                    return Poll::Ready(Ok(()));
                }
                match Pin::new(&mut listener).poll(cx) {
                    Poll::Ready(()) => continue, // notified -- recheck at the top of the loop
                    Poll::Pending => {
                        ew.listener = Some(listener);
                        return Poll::Pending;
                    }
                }
            }
        }
    }

    /// Blocking twin of [`Endpoint::poll_wait_epoch`]: spins with a no-op waker, exactly like
    /// [`UcxOptRequest::wait`] does for local completion.
    fn wait_epoch(&self, ew: &mut EpochWait) -> Result<(), Error> {
        let noop_waker = futures_util::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&noop_waker);
        loop {
            match self.poll_wait_epoch(ew, &mut cx) {
                Poll::Ready(res) => return res,
                // This is a blocking wait loop, so take a real blocking progress pass here
                // rather than relying on the inner `poll_wait`'s try-lock -- a pure
                // try-lock-and-spin loop has no anti-starvation guarantee under heavy
                // contention (see `Worker::progress_blocking`'s doc comment for the livelock
                // this caused in a real np=4 correctness run).
                Poll::Pending => self.worker.progress_blocking(32),
            }
        }
    }

    /// Stores a contiguous block of data into remote memory.
    /// blocking here means until the input buffer would be reusable
    /// not until the put has completed remotely
    ///
    /// D3: when `managed` is true, the source buffer belongs to a managed array/memregion
    /// allocation that the caller does not need back synchronously (the returned
    /// `UcxOptRequest`'s ticket -- not a synchronous wait here -- is what the caller polls/waits
    /// on for *local* completion, exactly like the pre-existing `managed_put`-gated worker flush
    /// already does for *remote* completion). Unmanaged puts (`managed = false`, e.g. a by-value
    /// put from a stack-local buffer) still block synchronously right here, since the source
    /// buffer may go out of scope the instant this function returns.
    pub(crate) fn put(
        self: &Arc<Self>,
        buf: *const u8,
        size: usize,
        remote_addr: usize,
        rkey: &RKey,
        managed: bool,
    ) -> Option<UcxOptRequest> {
        if !managed {
            // Unmanaged: source buffer may not outlive this call, so we always block on local
            // completion right here, synchronously, and never need a waker. Skip the
            // ticket/callback machinery entirely (no UCP_OP_ATTR_FIELD_CALLBACK/USER_DATA, no
            // per-op Arc<Ticket> allocation) and just poll the raw UCX request directly, exactly
            // like plain `ucx_lamellae` does -- this is the hot path for unmanaged bandwidth
            // benchmarks (e.g. put_bw), where the ticket allocation/locking was pure overhead.
            let local = issue_raw(|| unsafe {
                ucp_put_nbx(
                    self.handle,
                    buf as _,
                    size as _,
                    remote_addr as _,
                    rkey.handle,
                    &ucp_request_param_t {
                        op_attr_mask: ucp_op_attr_t::UCP_OP_ATTR_FIELD_MEMORY_TYPE as u32
                            | ucp_op_attr_t::UCP_OP_ATTR_FLAG_FAST_CMPL as u32,
                        flags: 0,
                        request: std::ptr::null_mut(),
                        cb: ucp_request_param_t__bindgen_ty_1 { send: None },
                        datatype: 0,
                        user_data: std::ptr::null_mut(),
                        reply_buffer: std::ptr::null_mut(),
                        memory_type: ucs_memory_type::UCS_MEMORY_TYPE_HOST,
                        recv_info: ucp_request_param_t__bindgen_ty_2 {
                            length: std::ptr::null_mut(),
                        },
                        memh: std::ptr::null_mut(),
                    } as _,
                )
            });
            UcxOptRequest::new_pending_local(local, &self.worker, None)
                .wait()
                .expect("Failed to wait for UcxOptRequest"); //ensures local buffer can be reused
            return None;
        }

        // Managed: skip the synchronous wait; hand the ticket back so the caller
        // (UcxOptPutFuture/block() in rdma.rs) resolves LOCAL completion lazily via
        // poll_wait/wait, which then falls through to a per-EP epoch flush wait for REMOTE
        // completion (D2). The epoch is reserved *after* `issue_ticketed` below has
        // returned, so the op is guaranteed already posted to the NIC by the time any flush
        // reads `posted`.
        let local = issue_ticketed(None, |user_data| unsafe {
            ucp_put_nbx(
                self.handle,
                buf as _,
                size as _,
                remote_addr as _,
                rkey.handle,
                &ucp_request_param_t {
                    // D5 fix: no FAST_CMPL here. That flag tells UCX this op doesn't need
                    // per-request completion tracking, which is the opposite of what the
                    // callback/Ticket below relies on -- on real network transport (not the
                    // same-node path, which completes inline and never hits this combination)
                    // completion_cb was observed to essentially never fire when FAST_CMPL was
                    // set alongside it, hanging poll_wait/wait forever. See `get` below for the
                    // same fix; `issue_flush` and the atomic ops never set FAST_CMPL and were
                    // never affected.
                    op_attr_mask: ucp_op_attr_t::UCP_OP_ATTR_FIELD_MEMORY_TYPE as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA as u32,
                    flags: 0,
                    request: std::ptr::null_mut(),
                    cb: ucp_request_param_t__bindgen_ty_1 {
                        send: Some(completion_cb),
                    },
                    datatype: 0,
                    user_data,
                    reply_buffer: std::ptr::null_mut(),
                    memory_type: ucs_memory_type::UCS_MEMORY_TYPE_HOST,
                    recv_info: ucp_request_param_t__bindgen_ty_2 {
                        length: std::ptr::null_mut(),
                    },
                    memh: std::ptr::null_mut(),
                } as _,
            )
        });
        let target = self.epoch.posted.fetch_add(1, Ordering::AcqRel) + 1;
        Some(UcxOptRequest::new_pending_local(
            local,
            &self.worker,
            Some(EpochWait {
                endpoint: self.clone(),
                target,
                is_driver: false,
                listener: None,
            }),
        ))
    }

    pub(crate) fn get(
        &self,
        buf: *const u8,
        size: usize,
        remote_addr: usize,
        rkey: &RKey,
    ) -> UcxOptRequest {
        let local = issue_ticketed(None, |user_data| unsafe {
            ucp_get_nbx(
                self.handle,
                buf as _,
                size as _,
                remote_addr as _,
                rkey.handle,
                &ucp_request_param_t {
                    // D5 fix: no FAST_CMPL -- see the matching comment in `put`'s managed path.
                    op_attr_mask: ucp_op_attr_t::UCP_OP_ATTR_FIELD_MEMORY_TYPE as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA as u32,
                    flags: 0,
                    request: std::ptr::null_mut(),
                    cb: ucp_request_param_t__bindgen_ty_1 {
                        send: Some(completion_cb),
                    },
                    datatype: 0,
                    user_data,
                    reply_buffer: std::ptr::null_mut(),
                    memory_type: ucs_memory_type::UCS_MEMORY_TYPE_HOST,
                    recv_info: ucp_request_param_t__bindgen_ty_2 {
                        length: std::ptr::null_mut(),
                    },
                    memh: std::ptr::null_mut(),
                } as _,
            )
        });
        UcxOptRequest::new_pending_local(local, &self.worker, None)
    }
    /// Fire-and-forget get for the unmanaged path: no `Ticket` (no `Arc`, no allocation, nothing the
    /// completion callback writes that the issuer later touches) and no per-op `UcxOptRequest`.
    /// Completion is observed only through the worker flush in `wait_all`. An immediate error is
    /// returned; an asynchronous failure is not reported (same as dropping a ticketed request).
    pub(crate) fn get_untracked(
        &self,
        buf: *const u8,
        size: usize,
        remote_addr: usize,
        rkey: &RKey,
    ) -> Result<(), Error> {
        let request = unsafe {
            ucp_get_nbx(
                self.handle,
                buf as _,
                size as _,
                remote_addr as _,
                rkey.handle,
                &ucp_request_param_t {
                    op_attr_mask: ucp_op_attr_t::UCP_OP_ATTR_FIELD_MEMORY_TYPE as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32,
                    flags: 0,
                    request: std::ptr::null_mut(),
                    cb: ucp_request_param_t__bindgen_ty_1 {
                        send: Some(free_request_cb),
                    },
                    datatype: 0,
                    user_data: std::ptr::null_mut(),
                    reply_buffer: std::ptr::null_mut(),
                    memory_type: ucs_memory_type::UCS_MEMORY_TYPE_HOST,
                    recv_info: ucp_request_param_t__bindgen_ty_2 {
                        length: std::ptr::null_mut(),
                    },
                    memh: std::ptr::null_mut(),
                } as _,
            )
        };
        if request.is_null() || UCS_PTR_IS_PTR(request) {
            Ok(())
        } else {
            Error::from_ptr(request)
        }
    }

    pub(crate) fn atomic_op<T>(
        self: &Arc<Self>,
        op: ucp_atomic_op_t,
        value: *const T,
        remote_addr: usize,
        rkey: &RKey,
        managed: bool,
    ) -> Option<UcxOptRequest> {
        assert!(std::mem::size_of::<T>() == 8 || std::mem::size_of::<T>() == 4);
        // println!("Val: {value:?}");

        let local = issue_ticketed(None, |user_data| unsafe {
            ucp_atomic_op_nbx(
                self.handle,
                op,
                value as _,
                1 as _,
                remote_addr as _,
                rkey.handle,
                &ucp_request_param_t {
                    op_attr_mask: ucp_op_attr_t::UCP_OP_ATTR_FIELD_DATATYPE as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_MEMORY_TYPE as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA as u32,
                    flags: 0,
                    request: std::ptr::null_mut(),
                    cb: ucp_request_param_t__bindgen_ty_1 {
                        send: Some(completion_cb),
                    },
                    datatype: ucp_dt_make_contig(std::mem::size_of::<T>() as _),
                    user_data,
                    reply_buffer: std::ptr::null_mut(),
                    memory_type: ucs_memory_type::UCS_MEMORY_TYPE_HOST,
                    recv_info: ucp_request_param_t__bindgen_ty_2 {
                        length: std::ptr::null_mut(),
                    },
                    memh: std::ptr::null_mut(),
                },
            )
        });
        let epoch_wait = if managed {
            let target = self.epoch.posted.fetch_add(1, Ordering::AcqRel) + 1;
            Some(EpochWait {
                endpoint: self.clone(),
                target,
                is_driver: false,
                listener: None,
            })
        } else {
            None
        };
        let req = UcxOptRequest::new_pending_local(local, &self.worker, epoch_wait);
        if managed {
            Some(req)
        } else {
            None
        }
    }

    pub(crate) fn atomic_get<T>(
        &self,
        zero: *const T,
        reply_buf: *mut T,
        remote_addr: usize,
        rkey: &RKey,
    ) -> UcxOptRequest {
        assert!(std::mem::size_of::<T>() == 8 || std::mem::size_of::<T>() == 4);
        // println!("Val: {value:?}");
        // let zero: MaybeUninit<T> = MaybeUninit::uninit();
        let local = issue_ticketed(None, |user_data| unsafe {
            ucp_atomic_op_nbx(
                self.handle,
                ucp_atomic_op_t::UCP_ATOMIC_OP_ADD,
                zero as _,
                1 as _,
                remote_addr as _,
                rkey.handle,
                &ucp_request_param_t {
                    op_attr_mask: ucp_op_attr_t::UCP_OP_ATTR_FIELD_DATATYPE as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_REPLY_BUFFER as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_MEMORY_TYPE as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA as u32,
                    flags: 0,
                    request: std::ptr::null_mut(),
                    cb: ucp_request_param_t__bindgen_ty_1 {
                        send: Some(completion_cb),
                    },
                    datatype: ucp_dt_make_contig(std::mem::size_of::<T>() as _),
                    user_data,
                    reply_buffer: reply_buf as *mut _,
                    memory_type: ucs_memory_type::UCS_MEMORY_TYPE_HOST,
                    recv_info: ucp_request_param_t__bindgen_ty_2 {
                        length: std::ptr::null_mut(),
                    },
                    memh: std::ptr::null_mut(),
                },
            )
        });
        UcxOptRequest::new_pending_local(local, &self.worker, None)
    }
    pub(crate) fn atomic_swap<T>(
        self: &Arc<Self>,
        // buf: *const u8,
        value: *const T,
        remote_addr: usize,
        rkey: &RKey,
        managed: bool,
    ) -> Option<UcxOptRequest> {
        assert!(std::mem::size_of::<T>() == 8 || std::mem::size_of::<T>() == 4);
        // println!("Val: {value:?}");
        // D5 fix: the swapped-out old value is discarded by every caller (this is only used
        // for AtomicArray's plain "write" op), but UCX still writes it into `reply_buffer` on
        // completion. This used to point at a single process-wide `static`
        // (`ATOMIC_PUT_TMP`), shared by every concurrent atomic_swap across every PE/endpoint
        // in the process -- with `wait_all` posting many of these at once, that's dozens of
        // in-flight `ucp_atomic_op_nbx` calls all writing their completion result into the
        // same 8 bytes simultaneously. Give each call its own scratch buffer instead, kept
        // alive by the `Ticket` (not the `UcxOptRequest`, which can be dropped before
        // completion on the unmanaged path) for exactly as long as the op is in flight.
        let mut scratch: Box<[u8; 8]> = Box::new([0u8; 8]);
        let reply_buf = scratch.as_mut_ptr() as *mut T;
        let local = issue_ticketed(Some(scratch), |user_data| unsafe {
            ucp_atomic_op_nbx(
                self.handle,
                ucp_atomic_op_t::UCP_ATOMIC_OP_SWAP,
                value as _,
                1 as _,
                remote_addr as _,
                rkey.handle,
                &ucp_request_param_t {
                    op_attr_mask: ucp_op_attr_t::UCP_OP_ATTR_FIELD_DATATYPE as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_REPLY_BUFFER as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_MEMORY_TYPE as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA as u32,
                    flags: 0,
                    request: std::ptr::null_mut(),
                    cb: ucp_request_param_t__bindgen_ty_1 {
                        send: Some(completion_cb),
                    },
                    datatype: ucp_dt_make_contig(std::mem::size_of::<T>() as _),
                    user_data,
                    reply_buffer: reply_buf as *mut _,
                    memory_type: ucs_memory_type::UCS_MEMORY_TYPE_HOST,
                    recv_info: ucp_request_param_t__bindgen_ty_2 {
                        length: std::ptr::null_mut(),
                    },
                    memh: std::ptr::null_mut(),
                },
            )
        });
        let epoch_wait = if managed {
            let target = self.epoch.posted.fetch_add(1, Ordering::AcqRel) + 1;
            Some(EpochWait {
                endpoint: self.clone(),
                target,
                is_driver: false,
                listener: None,
            })
        } else {
            None
        };
        let req = UcxOptRequest::new_pending_local(local, &self.worker, epoch_wait);
        if managed {
            Some(req)
        } else {
            None
        }
    }

    pub(crate) fn atomic_compare_swap<T>(
        &self,
        compare: *const T,
        reply_buf: *mut T,
        remote_addr: usize,
        rkey: &RKey,
    ) -> UcxOptRequest {
        assert!(std::mem::size_of::<T>() == 8 || std::mem::size_of::<T>() == 4);
        let local = issue_ticketed(None, |user_data| unsafe {
            ucp_atomic_op_nbx(
                self.handle,
                ucp_atomic_op_t::UCP_ATOMIC_OP_CSWAP,
                compare as _,
                1 as _,
                remote_addr as _,
                rkey.handle,
                &ucp_request_param_t {
                    op_attr_mask: ucp_op_attr_t::UCP_OP_ATTR_FIELD_DATATYPE as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_REPLY_BUFFER as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_MEMORY_TYPE as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA as u32,
                    flags: 0,
                    request: std::ptr::null_mut(),
                    cb: ucp_request_param_t__bindgen_ty_1 {
                        send: Some(completion_cb),
                    },
                    datatype: ucp_dt_make_contig(std::mem::size_of::<T>() as _),
                    user_data,
                    reply_buffer: reply_buf as *mut _,
                    memory_type: ucs_memory_type::UCS_MEMORY_TYPE_HOST,
                    recv_info: ucp_request_param_t__bindgen_ty_2 {
                        length: std::ptr::null_mut(),
                    },
                    memh: std::ptr::null_mut(),
                },
            )
        });
        UcxOptRequest::new_pending_local(local, &self.worker, None)
    }

    pub(crate) fn atomic_fetch_op<T>(
        &self,
        op: ucp_atomic_op_t,
        value: *const T,
        reply_buf: *mut T,
        remote_addr: usize,
        rkey: &RKey,
    ) -> UcxOptRequest {
        assert!(std::mem::size_of::<T>() == 8 || std::mem::size_of::<T>() == 4);
        let local = issue_ticketed(None, |user_data| unsafe {
            ucp_atomic_op_nbx(
                self.handle,
                op,
                value as _,
                1 as _,
                remote_addr as _,
                rkey.handle,
                &ucp_request_param_t {
                    op_attr_mask: ucp_op_attr_t::UCP_OP_ATTR_FIELD_DATATYPE as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_REPLY_BUFFER as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_MEMORY_TYPE as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA as u32,
                    flags: 0,
                    request: std::ptr::null_mut(),
                    cb: ucp_request_param_t__bindgen_ty_1 {
                        send: Some(completion_cb),
                    },
                    datatype: ucp_dt_make_contig(std::mem::size_of::<T>() as _),
                    user_data,
                    reply_buffer: reply_buf as *mut _,
                    memory_type: ucs_memory_type::UCS_MEMORY_TYPE_HOST,
                    recv_info: ucp_request_param_t__bindgen_ty_2 {
                        length: std::ptr::null_mut(),
                    },
                    memh: std::ptr::null_mut(),
                },
            )
        });
        UcxOptRequest::new_pending_local(local, &self.worker, None)
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop Endpoint");
        // println!("dropping endpoint");
        debug!("Dropping Endpoint");
        unsafe {
            let request = ucp_ep_close_nbx(
                self.handle,
                &ucp_request_param_t {
                    op_attr_mask: 0,
                    flags: 0,
                    request: std::ptr::null_mut(),
                    cb: ucp_request_param_t__bindgen_ty_1 { send: None },
                    datatype: 0,
                    user_data: std::ptr::null_mut(),
                    reply_buffer: std::ptr::null_mut(),
                    memory_type: ucs_memory_type::UCS_MEMORY_TYPE_HOST,
                    recv_info: ucp_request_param_t__bindgen_ty_2 {
                        length: std::ptr::null_mut(),
                    },
                    memh: std::ptr::null_mut(),
                },
            );
            if request.is_null() {
                // println!("Close return null");
            } else if UCS_PTR_IS_PTR(request) {
                loop {
                    // Blocking wait (Drop is inherently synchronous): use the fair blocking
                    // lock, not try_progress -- a pure try-lock-and-spin loop has no
                    // anti-starvation guarantee under heavy contention (see
                    // `Worker::progress_blocking`'s doc comment).
                    self.worker.progress_blocking(32);
                    if UCS_PTR_IS_PTR(request) {
                        // println!("Close request in progress");
                        if ucp_request_check_status(request as _) != ucs_status_t::UCS_INPROGRESS {
                            // println!("Close request completed");
                            break;
                        }
                    }
                }
                ucp_request_free(request as _);
                // println!("Close request freed");
            } else {
                // println!("Close error");
                let _ = Error::from_ptr(request);
            }
        };
        trace!(target: "drop", "end drop Endpoint");
    }
}

// unsafe extern "C" fn cb(request: *mut std::ffi::c_void, _status: ucs_status_t) {
//     // This is a placeholder for the callback function.
//     // In practice, you would implement the logic to handle the completion of the request.
//     // For example, you might wake up a thread or signal an event.
//     unsafe { ucp_request_free(request as _) };
// }

use lamellar_ucx_sys::*;
use std::{mem::MaybeUninit, sync::Arc, task::Poll};

use parking_lot::Mutex;

use super::{context::Context, error::Error};

use pmi::pmi::Pmi;

use tracing::*;

#[derive(Debug)]
pub(crate) struct Worker {
    _context: Arc<Context>,
    pub(crate) handle: ucp_worker_h,
    /// D5: our own uncontended try-lock around `ucp_worker_progress`, separate from UCX's
    /// internal `UCS_THREAD_MODE_MULTI` lock. Without this, every pending future on every
    /// thread calls `ucp_worker_progress` on every poll and all of them serialize inside UCX's
    /// lock even though only one caller's call is doing anything useful at a time (U4). A
    /// caller that fails to take this lock knows someone else is already progressing the
    /// worker right now, so it can skip calling progress itself and just wait to be woken.
    progress_lock: Mutex<()>,
}

unsafe impl Sync for Worker {}
unsafe impl Send for Worker {}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) enum FlushState {
    #[default]
    NotIssued,
    Pending(ucs_status_ptr_t),
    Done,
}

unsafe impl Sync for FlushState {}
unsafe impl Send for FlushState {}

impl Worker {
    pub(crate) fn new(context: Arc<Context>) -> Result<Arc<Worker>, Error> {
        let mut params = MaybeUninit::<ucp_worker_params_t>::uninit();
        unsafe {
            (*params.as_mut_ptr()).field_mask =
                ucp_worker_params_field::UCP_WORKER_PARAM_FIELD_THREAD_MODE.0 as _;
            (*params.as_mut_ptr()).thread_mode = ucs_thread_mode_t::UCS_THREAD_MODE_MULTI;
        };
        let mut handle = MaybeUninit::uninit();
        let status =
            unsafe { ucp_worker_create(context.handle, params.as_ptr(), handle.as_mut_ptr()) };
        Error::from_status(status)?;

        // Worker created; take the handle and query the worker to see which
        // attributes UCX actually set (thread mode, max AM header, address flags)
        let h = unsafe { handle.assume_init() };
        let mut wattr = MaybeUninit::<ucp_worker_attr_t>::uninit();
        unsafe {
            (*wattr.as_mut_ptr()).field_mask =
                (ucp_worker_attr_field::UCP_WORKER_ATTR_FIELD_THREAD_MODE
                    | ucp_worker_attr_field::UCP_WORKER_ATTR_FIELD_MAX_AM_HEADER
                    | ucp_worker_attr_field::UCP_WORKER_ATTR_FIELD_ADDRESS_FLAGS)
                    .0 as _;
        }
        let qstatus = unsafe { ucp_worker_query(h, wattr.as_mut_ptr()) };
        match Error::from_status(qstatus) {
            Ok(()) => {
                let wattr = unsafe { wattr.assume_init() };
                debug!(
                    "ucx worker attrs: thread_mode={:?}, address_flags={}, max_am_header={}",
                    wattr.thread_mode, wattr.address_flags, wattr.max_am_header
                );
            }
            Err(e) => debug!("ucp_worker_query failed: {:?}", e),
        }

        Ok(Arc::new(Worker {
            _context: context,
            handle: h,
            progress_lock: Mutex::new(()),
        }))
    }

    // pub(crate) fn request_size(&self) -> usize {
    //     self.context.query().unwrap().request_size as usize
    // }

    #[inline]
    pub(crate) fn progress(&self) -> u32 {
        let res = unsafe { ucp_worker_progress(self.handle) };
        res
    }

    /// D5: try to take this worker's progress lock; if acquired, drain
    /// `ucp_worker_progress` until it returns 0 or `cap` iterations run, then release.
    /// Returns whether the lock was acquired -- callers use this to decide whether to
    /// hedge with a self-rewake/spin, since failing to acquire means someone else is
    /// already progressing this worker right now.
    #[inline]
    pub(crate) fn try_progress(&self, cap: u32) -> bool {
        let Some(_guard) = self.progress_lock.try_lock() else {
            return false;
        };
        for _ in 0..cap {
            if unsafe { ucp_worker_progress(self.handle) } == 0 {
                break;
            }
        }
        true
    }

    /// Blocking twin of `try_progress`, for use only by genuinely blocking (non-async) wait
    /// loops. Unlike `try_progress`'s `try_lock`, this takes a real blocking `lock()`, which
    /// relies on `parking_lot::Mutex`'s built-in anti-starvation handoff for fairness. That
    /// fairness guarantee is what a pure try_lock-and-spin loop lacks: under heavy contention
    /// (many threads spinning `try_progress` with no queuing), a given thread can go
    /// essentially indefinitely without ever winning the race, which showed up as a full
    /// livelock (every thread pegged at 100% CPU, zero forward progress) in a real np=4
    /// correctness run. Async paths (`poll_local`/`poll_wait`) must keep using `try_progress`
    /// since they can't block inside `poll()`; only synchronous blocking callers should call
    /// this.
    #[inline]
    pub(crate) fn progress_blocking(&self, cap: u32) {
        let _guard = self.progress_lock.lock();
        for _ in 0..cap {
            if unsafe { ucp_worker_progress(self.handle) } == 0 {
                break;
            }
        }
    }

    fn flush_params() -> ucp_request_param_t {
        ucp_request_param_t {
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
        }
    }

    /// This routine flushes all outstanding AMO and RMA communications on the worker.
    pub(crate) fn wait_all(&self) -> Result<(), Error> {
        let params = Self::flush_params();
        let request = unsafe { ucp_worker_flush_nbx(self.handle, &params) };
        if request.is_null() {
            Ok(())
        } else if UCS_PTR_IS_PTR(request) {
            loop {
                // Blocking wait: use the fair blocking lock, not try_progress -- a pure
                // try-lock-and-spin loop has no anti-starvation guarantee under heavy
                // contention (see `progress_blocking`'s doc comment).
                self.progress_blocking(32);
                if unsafe { ucp_request_check_status(request as _) }
                    != ucs_status_t::UCS_INPROGRESS
                {
                    break;
                }
            }
            unsafe { ucp_request_free(request as _) };
            Ok(())
        } else {
            Error::from_ptr(request)
        }
    }

    /// Non-blocking, single-shot-per-call version of `wait_all`. Caller owns
    /// `state` across polls and is responsible for re-polling (e.g. via a
    /// waker) until this returns `Poll::Ready`.
    pub(crate) fn poll_wait_all(&self, state: &mut FlushState) -> Poll<Result<(), Error>> {
        loop {
            match state {
                FlushState::Done => return Poll::Ready(Ok(())),
                FlushState::NotIssued => {
                    let params = Self::flush_params();
                    let request = unsafe { ucp_worker_flush_nbx(self.handle, &params) };
                    if request.is_null() {
                        *state = FlushState::Done;
                        return Poll::Ready(Ok(()));
                    } else if UCS_PTR_IS_PTR(request) {
                        *state = FlushState::Pending(request);
                    } else {
                        *state = FlushState::Done;
                        return Poll::Ready(Error::from_ptr(request));
                    }
                }
                FlushState::Pending(request) => {
                    let request = *request;
                    let _ = self.try_progress(32);
                    if unsafe { ucp_request_check_status(request as _) }
                        == ucs_status_t::UCS_INPROGRESS
                    {
                        return Poll::Pending;
                    }
                    unsafe { ucp_request_free(request as _) };
                    *state = FlushState::Done;
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }

    /// Get the address of the worker object.
    ///
    /// This address can be passed to remote instances of the UCP library
    /// in order to connect to this worker.
    pub(crate) fn address(&self) -> Result<WorkerAddress<'_>, Error> {
        let mut handle = MaybeUninit::uninit();
        let mut length = MaybeUninit::uninit();
        let status = unsafe {
            ucp_worker_get_address(self.handle, handle.as_mut_ptr(), length.as_mut_ptr())
        };
        Error::from_status(status)?;

        Ok(WorkerAddress {
            handle: unsafe { handle.assume_init() },
            length: unsafe { length.assume_init() } as usize,
            worker: self,
        })
    }

    /// Exchange the addresses of all `workers` (shards) in one PMI round. Returns
    /// `result[shard][pe]`. Every PE must have created the same number of workers.
    pub(crate) fn exchange_addresses(
        workers: &[Arc<Worker>],
        pmi: Arc<dyn Pmi>,
    ) -> Result<Vec<Vec<Vec<u8>>>, Error> {
        let n = workers.len();
        for (i, w) in workers.iter().enumerate() {
            let addr = w.address().unwrap();
            pmi.put(&format!("worker_address_{i}"), addr.as_ref()).unwrap();
        }
        pmi.put("ucx_num_workers", &(n as u64).to_ne_bytes()).unwrap();
        pmi.exchange().unwrap();
        let num_pes = pmi.ranks().len();
        let mut all = vec![Vec::with_capacity(num_pes); n];
        for pe in 0..num_pes {
            let peer_n = u64::from_ne_bytes(
                pmi.get("ucx_num_workers", &pe).unwrap()[..8].try_into().unwrap(),
            ) as usize;
            assert_eq!(
                peer_n, n,
                "LAMELLAR_UCX_WORKERS mismatch: PE {pe} has {peer_n} workers but this PE has {n}"
            );
            for i in 0..n {
                all[i].push(pmi.get(&format!("worker_address_{i}"), &pe).unwrap());
            }
        }
        Ok(all)
    }

    pub(crate) fn exchange_address(&self, pmi: Arc<dyn Pmi>) -> Result<Vec<Vec<u8>>, Error> {
        let my_address = self.address().unwrap();
        let addr_key = format!("worker_address");
        pmi.put(&addr_key, my_address.as_ref()).unwrap();
        pmi.exchange().unwrap();
        let mut all_addresses = Vec::new();
        for pe in 0..pmi.ranks().len() {
            let res = pmi.get(&addr_key, &pe).unwrap();
            all_addresses.push(res);
        }

        Ok(all_addresses)
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop Worker");
        debug!("dropping worker");
        unsafe { ucp_worker_destroy(self.handle) }
        trace!(target: "drop", "end drop Worker");
    }
}

// extern "C" {
//     static _stderr: *mut FILE;
// }

/// The address of the worker object.
#[derive(Debug)]
pub(crate) struct WorkerAddress<'a> {
    handle: *mut ucp_address_t,
    length: usize,
    worker: &'a Worker,
}

impl<'a> AsRef<[u8]> for WorkerAddress<'a> {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.handle as *const u8, self.length) }
    }
}

impl<'a> Drop for WorkerAddress<'a> {
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop WorkerAddress");
        unsafe { ucp_worker_release_address(self.worker.handle, self.handle) }
        trace!(target: "drop", "end drop WorkerAddress");
    }
}

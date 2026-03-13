use lamellar_ucx_sys::*;
use std::{mem::MaybeUninit, sync::Arc};

use super::{context::Context, error::Error};

use pmi::{pmi::Pmi};

use tracing::*;

#[derive(Debug)]
pub(crate) struct Worker {
    _context: Arc<Context>,
    pub(crate) handle: ucp_worker_h,
}

unsafe impl Sync for Worker {}
unsafe impl Send for Worker {}

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
            // progress_lock: Arc::new(Mutex::new(std::time::Instant::now())),
        }))
    }

    // pub(crate) fn request_size(&self) -> usize {
    //     self.context.query().unwrap().request_size as usize
    // }

    pub(crate) fn progress(&self) -> u32 {
        let res = unsafe { ucp_worker_progress(self.handle) };
        res
    }
    /// This routine flushes all outstanding AMO and RMA communications on the worker.
    pub(crate) fn wait_all(&self) -> Result<(), Error> {
        let params = ucp_request_param_t {
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
        };
        let request = unsafe { ucp_worker_flush_nbx(self.handle, &params) };
        if request.is_null() {
            Ok(())
        } else if UCS_PTR_IS_PTR(request) {
            loop {
                let _res = self.progress();
                if UCS_PTR_IS_PTR(request) {
                    if unsafe { ucp_request_check_status(request as _) }
                        != ucs_status_t::UCS_INPROGRESS
                    {
                        break;
                    }
                }
            }
            unsafe { ucp_request_free(request as _) };
            Ok(())
        } else {
            Error::from_ptr(request)
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

    pub(crate) fn exchange_address(&self, pmi: Arc<dyn Pmi>) -> Result<Vec<Vec<u8>>, Error> {
        let my_address = self.address().unwrap();
        let addr_key = format!("worker_address");
        pmi.put(&addr_key, my_address.as_ref()).unwrap();
        pmi.exchange().unwrap();
        let mut all_addresses = Vec::new();
        for pe in 0..pmi.ranks().len() {
            let res = pmi
                .get(&addr_key, &pe)
                .unwrap();
            all_addresses.push(res);
        }

        Ok(all_addresses)
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        debug!("dropping worker");
        unsafe { ucp_worker_destroy(self.handle) }
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
        unsafe { ucp_worker_release_address(self.worker.handle, self.handle) }
    }
}

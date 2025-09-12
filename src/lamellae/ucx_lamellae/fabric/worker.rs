use parking_lot::Mutex;
use std::{mem::MaybeUninit, sync::Arc};
use ucx1_sys::*;

use super::{context::Context, error::Error};

use pmi::{pmi::Pmi, pmix::PmiX};

#[derive(Debug)]
pub(crate) struct Worker {
    context: Arc<Context>,
    // pub(crate) lock: Mutex<()>,
    pub(crate) handle: ucp_worker_h,
}

unsafe impl Sync for Worker {}
unsafe impl Send for Worker {}

impl Worker {
    pub fn new(context: Arc<Context>) -> Result<Arc<Worker>, Error> {
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

        Ok(Arc::new(Worker {
            context,
            // lock: Mutex::new(()),
            handle: unsafe { handle.assume_init() },
        }))
    }

    pub fn poll(&self) {
        self.progress();
    }

    pub fn request_size(&self) -> usize {
        self.context.query().unwrap().request_size as usize
    }

    pub fn progress(&self) -> u32 {
        // let handle = self.handle.try_lock()?;
        // let lock_handle = self.lock.lock();
        // println!("forcing progress");
        let res = unsafe { ucp_worker_progress(self.handle) };
        // println!("[{:?}] progressed {:?}", std::thread::current().id(), res);
        res
    }
    /// This routine flushes all outstanding AMO and RMA communications on the worker.
    pub fn wait_all(&self) -> Result<(), Error> {
        // if let Some(handle) = self.handle.try_lock() {
        // let lock_handle = self.lock.lock();
        // println!(
        //     "[{:?}] in wait_all got worker lock!",
        //     std::thread::current().id()
        // );
        // let status = unsafe { ucp_worker_flush(self.handle) };
        // assert_eq!(status, ucs_status_t::UCS_OK);
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
            // println!("[{:?}] flush return null", std::thread::current().id());
            Ok(())
        } else if UCS_PTR_IS_PTR(request) {
            // println!(
            //     "[{:?}] flush request in not null",
            //     std::thread::current().id()
            // );
            loop {
                let res = unsafe { ucp_worker_progress(self.handle) };
                // println!(
                //     "[{:?}] flush still in progress {:?}",
                //     std::thread::current().id(),
                //     res
                // );
                if UCS_PTR_IS_PTR(request) {
                    if unsafe { ucp_request_check_status(request as _) }
                        != ucs_status_t::UCS_INPROGRESS
                    {
                        // println!(
                        //     "[{:?}] flush request completed",
                        //     std::thread::current().id()
                        // );
                        break;
                    }
                }
                // println!(
                //     "[{:?}] flush progressing handle addr {:?}",
                //     std::thread::current().id(),
                //     self.handle
                // );
                // let res = unsafe { ucp_worker_progress(self.handle) };
                // println!(
                //     "[{:?}] flush still in progress {:?}",
                //     std::thread::current().id(),
                //     res
                // );
                // std::thread::yield_now();
            }
            // println!("[{:?}] flush free ", std::thread::current().id());
            unsafe { ucp_request_free(request as _) };
            Ok(())
        } else {
            // println!("[{:?}] flush error", std::thread::current().id());
            Error::from_ptr(request)
        }
        // } else {
        //     // println!(
        //     //     "[{:?}] could not lock worker handle",
        //     //     std::thread::current().id()
        //     // );
        //     Ok(false)
        // }
    }

    pub fn print_to_stderr(&self) {
        // let _lock_handle = self.lock.lock();
        unsafe { ucp_worker_print_info(self.handle, stderr) };
    }

    /// Get the address of the worker object.
    ///
    /// This address can be passed to remote instances of the UCP library
    /// in order to connect to this worker.
    pub fn address(&self) -> Result<WorkerAddress<'_>, Error> {
        let mut handle = MaybeUninit::uninit();
        let mut length = MaybeUninit::uninit();
        // let _lock_handle = self.lock.lock();
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

    pub fn exchange_address(&self, pmi: &Arc<PmiX>) -> Result<Vec<Vec<u8>>, Error> {
        let my_address = self.address().unwrap();
        // println!(
        //     "len: {},{} address: {:?}",
        //     my_address.length,
        //     my_address.as_ref().len(),
        //     my_address.as_ref()
        // );
        pmi.put("worker_address", my_address.as_ref()).unwrap();
        pmi.exchange().unwrap();
        let mut all_addresses = Vec::new();
        for pe in 0..pmi.ranks().len() {
            let res = pmi
                .get("worker_address", &my_address.as_ref().len(), &pe)
                .unwrap();
            // let address = unsafe { ep::Address::from_bytes(&res) };
            all_addresses.push(res);
        }

        // for address in all_addresses.iter() {
        //     println!("{:?}", address);
        // }

        Ok(all_addresses)
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        // let _lock_handle = self.lock.lock();
        println!("dropping worker");
        unsafe { ucp_worker_destroy(self.handle) }
    }
}

extern "C" {
    static stderr: *mut FILE;
}

/// The address of the worker object.
#[derive(Debug)]
pub struct WorkerAddress<'a> {
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
        // let handle = self.worker.handle.lock();
        // let lock_handle = self.worker.lock.lock();
        unsafe { ucp_worker_release_address(self.worker.handle, self.handle) }
    }
}

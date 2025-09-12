use std::{
    mem::MaybeUninit,
    sync::{atomic::AtomicUsize, Arc},
};

// use ucx1_sys::*;

use ucx1_sys::{
    ucp_atomic_op_nbx, ucp_atomic_op_t, ucp_dt_make_contig, ucp_ep_close_nbx, ucp_ep_create,
    ucp_ep_flush_nbx, ucp_ep_h, ucp_ep_params, ucp_ep_params_field, ucp_err_handler,
    ucp_err_handling_mode_t, ucp_get_nbx, ucp_op_attr_t, ucp_put_nbx, ucp_request_check_status,
    ucp_request_free, ucp_request_param_t, ucp_request_param_t__bindgen_ty_1,
    ucp_request_param_t__bindgen_ty_2, ucs_memory_type, ucs_sock_addr, ucs_status_ptr_t,
    ucs_status_t, UCS_PTR_IS_PTR,
};

use super::{error::Error, memory_region::RKey, worker::Worker};

pub(crate) struct UcxRequest {
    pub(crate) request: ucs_status_ptr_t,
    pub(crate) worker: Arc<Worker>,
    pub(crate) managed_put: bool,
}

unsafe impl Sync for UcxRequest {}
unsafe impl Send for UcxRequest {}

impl UcxRequest {
    pub(crate) fn new(request: ucs_status_ptr_t, worker: Arc<Worker>, managed_put: bool) -> Self {
        Self {
            request,
            worker,
            managed_put,
        }
    }
    pub(crate) fn wait(mut self) -> Result<(), Error> {
        if self.managed_put {
            self.worker.wait_all();
            Ok(())
        } else {
            if self.request.is_null() {
                // println!("Request is null");
                return Ok(());
            }
            if UCS_PTR_IS_PTR(self.request) {
                // println!("Request is not null");
                loop {
                    let _ = self.worker.progress();
                    if unsafe { ucp_request_check_status(self.request as _) }
                        != ucs_status_t::UCS_INPROGRESS
                    {
                        break;
                    }
                }
                unsafe { ucp_request_free(self.request as _) };
                self.request = std::ptr::null_mut();
                Ok(())
            } else {
                // println!("Request is not valid");
                Error::from_ptr(self.request)
            }
        }
    }
}
//     pub(crate) fn wait(
//         mut self,
//         delete_on_wait_failure: bool,
//     ) -> Result<Option<UcxRequest>, Error> {
//         if self.managed_put {
//             println!(
//                 "[{:?}] waiting for managed put",
//                 std::thread::current().id()
//             );
//             if !self.worker.wait_all().expect("Wait all failed") {
//                 return Ok(Some(self));
//             }
//             return Ok(None);
//         } else {
//             if self.request.is_null() {
//                 println!("[{:?}] UCX Request is null", std::thread::current().id());
//                 return Ok(None);
//             }
//             if UCS_PTR_IS_PTR(self.request) {
//                 println!(
//                     "[{:?}] UCX Request is not null",
//                     std::thread::current().id()
//                 );
//                 loop {
//                     if unsafe { ucp_request_check_status(self.request as _) }
//                         != ucs_status_t::UCS_INPROGRESS
//                     {
//                         break;
//                     }
//                     if self.worker.progress().is_none() {
//                         if delete_on_wait_failure {
//                             unsafe { ucp_request_free(self.request as _) };
//                             self.request = std::ptr::null_mut();
//                         }
//                         return Ok(Some(self));
//                     }
//                     std::thread::yield_now();
//                 }
//                 unsafe { ucp_request_free(self.request as _) };
//                 self.request = std::ptr::null_mut();
//                 Ok(None)
//             } else {
//                 // println!(
//                 //     "[{:?}] UCX Request is not valid",
//                 //     std::thread::current().id()
//                 // );
//                 Error::from_ptr(self.request).map(|_| None)
//             }
//         }
//     }
// }

impl Drop for UcxRequest {
    fn drop(&mut self) {
        if !self.request.is_null() {
            unsafe { ucp_request_free(self.request as _) };
        }
    }
}

#[derive(Debug, Clone)]
pub struct Endpoint {
    pub(crate) worker: Arc<Worker>,
    pub(crate) handle: ucp_ep_h,
    request_size: usize,
}

unsafe impl Send for Endpoint {}
unsafe impl Sync for Endpoint {}

static ATOMIC_PUT_TMP: AtomicUsize = AtomicUsize::new(0);
static ATOMIC_GET_TMP: AtomicUsize = AtomicUsize::new(0);

impl Endpoint {
    pub fn new(worker: Arc<Worker>, remote_address: &[u8]) -> Result<Arc<Endpoint>, Error> {
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
        let request_size = worker.request_size();
        // drop(lock_handle);

        Ok(Arc::new(Endpoint {
            worker,
            request_size,
            handle: unsafe { handle.assume_init() },
        }))
    }

    pub fn ep_wait_all(&self) -> Result<(), Error> {
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
        let request = unsafe { ucp_ep_flush_nbx(self.handle, &params) };
        UcxRequest::new(request, self.worker.clone(), false).wait()
    }

    /// Stores a contiguous block of data into remote memory.
    /// blocking here means until the input buffer would be reusable
    /// not until the put has completed remotely
    pub fn put(
        &self,
        buf: *const u8,
        size: usize,
        remote_addr: usize,
        rkey: &RKey,
        managed: bool,
    ) -> Option<UcxRequest> {
        // unsafe extern "C" fn callback(request: *mut c_void, status: ucs_status_t) {
        //     let request = &mut *(request as *mut Request);
        //     request.waker.wake();
        // }

        let request = unsafe {
            ucp_put_nbx(
                self.handle,
                buf as _,
                size as _,
                remote_addr as _,
                rkey.handle,
                &ucp_request_param_t {
                    op_attr_mask: ucp_op_attr_t::UCP_OP_ATTR_FLAG_FAST_CMPL as u32,
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
        };
        // println!("after inner put");
        UcxRequest::new(request, self.worker.clone(), false).wait(); //ensures local buffer can be reused
        if managed {
            Some(UcxRequest::new(
                std::ptr::null_mut(),
                self.worker.clone(),
                true,
            ))
        } else {
            None
        }

        // UcxRequest::new(request, self.worker.clone(), managed)
    }

    pub fn get(&self, buf: *const u8, size: usize, remote_addr: usize, rkey: &RKey) -> UcxRequest {
        // unsafe extern "C" fn callback(request: *mut c_void, status: ucs_status_t) {
        //     let request = &mut *(request as *mut Request);
        //     request.waker.wake();
        // }
        let request = unsafe {
            ucp_get_nbx(
                self.handle,
                buf as _,
                size as _,
                remote_addr as _,
                rkey.handle,
                &ucp_request_param_t {
                    op_attr_mask: ucp_op_attr_t::UCP_OP_ATTR_FLAG_FAST_CMPL as u32,
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
        };
        UcxRequest::new(request, self.worker.clone(), false)
    }

    pub fn atomic_put<T>(
        &self,
        value: T,
        remote_addr: usize,
        rkey: &RKey,
        managed: bool,
    ) -> Option<UcxRequest> {
        assert!(std::mem::size_of::<T>() == 8 || std::mem::size_of::<T>() == 4);
        // println!("Val: {value:?}");

        let request = unsafe {
            ucp_atomic_op_nbx(
                self.handle,
                ucp_atomic_op_t::UCP_ATOMIC_OP_SWAP,
                &value as *const T as _,
                1 as _,
                remote_addr as _,
                rkey.handle,
                &ucp_request_param_t {
                    op_attr_mask: ucp_op_attr_t::UCP_OP_ATTR_FIELD_DATATYPE as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_REPLY_BUFFER as u32,
                    flags: 0,
                    request: std::ptr::null_mut(),
                    cb: ucp_request_param_t__bindgen_ty_1 { send: None },
                    datatype: ucp_dt_make_contig(std::mem::size_of::<T>() as _),
                    user_data: std::ptr::null_mut(),
                    reply_buffer: &ATOMIC_PUT_TMP as *const _ as *mut _,
                    memory_type: ucs_memory_type::UCS_MEMORY_TYPE_HOST,
                    recv_info: ucp_request_param_t__bindgen_ty_2 {
                        length: std::ptr::null_mut(),
                    },
                    memh: std::ptr::null_mut(),
                },
            )
        };
        let req = UcxRequest::new(request, self.worker.clone(), false);
        if managed {
            Some(req)
        } else {
            None
        }
        // } else {
        //     let mut result: u64 = 0;
        //     let value: u64 = 0;
        //     let request = unsafe {
        //         ucp_atomic_op_nbx(
        //             self.handle,
        //             ucp_atomic_op_t::UCP_ATOMIC_OP_SWAP,
        //             &value as *const u64 as _,
        //             1 as _,
        //             remote_addr as _,
        //             rkey.handle,
        //             &ucp_request_param_t {
        //                 op_attr_mask: ucp_op_attr_t::UCP_OP_ATTR_FIELD_DATATYPE as u32
        //                     | ucp_op_attr_t::UCP_OP_ATTR_FIELD_REPLY_BUFFER as u32,
        //                 flags: 0,
        //                 request: std::ptr::null_mut(),
        //                 cb: ucp_request_param_t__bindgen_ty_1 { send: None },
        //                 datatype: ucp_dt_make_contig(std::mem::size_of::<u64>() as _),
        //                 user_data: std::ptr::null_mut(),
        //                 reply_buffer: &result as *const _ as *mut _,
        //                 memory_type: ucs_memory_type::UCS_MEMORY_TYPE_HOST,
        //                 recv_info: ucp_request_param_t__bindgen_ty_2 {
        //                     length: std::ptr::null_mut(),
        //                 },
        //                 memh: std::ptr::null_mut(),
        //             },
        //         )
        //     };
        //     UcxRequest::new(request, self.worker.clone(), false).wait();
        //     None
        // }
    }

    pub fn atomic_get<T>(&self, reply_buf: *mut T, remote_addr: usize, rkey: &RKey) -> UcxRequest {
        assert!(std::mem::size_of::<T>() == 8 || std::mem::size_of::<T>() == 4);
        // println!("Val: {value:?}");
        let zero: MaybeUninit<T> = MaybeUninit::uninit();
        let request = unsafe {
            ucp_atomic_op_nbx(
                self.handle,
                ucp_atomic_op_t::UCP_ATOMIC_OP_ADD,
                zero.as_ptr() as _,
                1 as _,
                remote_addr as _,
                rkey.handle,
                &ucp_request_param_t {
                    op_attr_mask: ucp_op_attr_t::UCP_OP_ATTR_FIELD_DATATYPE as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_REPLY_BUFFER as u32,
                    flags: 0,
                    request: std::ptr::null_mut(),
                    cb: ucp_request_param_t__bindgen_ty_1 { send: None },
                    datatype: ucp_dt_make_contig(std::mem::size_of::<T>() as _),
                    user_data: std::ptr::null_mut(),
                    reply_buffer: reply_buf as *mut _,
                    memory_type: ucs_memory_type::UCS_MEMORY_TYPE_HOST,
                    recv_info: ucp_request_param_t__bindgen_ty_2 {
                        length: std::ptr::null_mut(),
                    },
                    memh: std::ptr::null_mut(),
                },
            )
        };
        UcxRequest::new(request, self.worker.clone(), false)
    }

    pub fn atomic_swap<T>(
        &self,
        // buf: *const u8,
        value: T,
        reply_buf: *mut T,
        remote_addr: usize,
        rkey: &RKey,
    ) -> UcxRequest {
        assert!(std::mem::size_of::<T>() == 8 || std::mem::size_of::<T>() == 4);
        // println!("Val: {value:?}");
        let zero = 0usize;
        let request = unsafe {
            ucp_atomic_op_nbx(
                self.handle,
                ucp_atomic_op_t::UCP_ATOMIC_OP_SWAP,
                &value as *const T as _,
                1 as _,
                remote_addr as _,
                rkey.handle,
                &ucp_request_param_t {
                    op_attr_mask: ucp_op_attr_t::UCP_OP_ATTR_FIELD_DATATYPE as u32
                        | ucp_op_attr_t::UCP_OP_ATTR_FIELD_REPLY_BUFFER as u32,
                    flags: 0,
                    request: std::ptr::null_mut(),
                    cb: ucp_request_param_t__bindgen_ty_1 { send: None },
                    datatype: ucp_dt_make_contig(std::mem::size_of::<T>() as _),
                    user_data: std::ptr::null_mut(),
                    reply_buffer: reply_buf as *mut _,
                    memory_type: ucs_memory_type::UCS_MEMORY_TYPE_HOST,
                    recv_info: ucp_request_param_t__bindgen_ty_2 {
                        length: std::ptr::null_mut(),
                    },
                    memh: std::ptr::null_mut(),
                },
            )
        };
        UcxRequest::new(request, self.worker.clone(), false)
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        // println!("dropping endpoint");
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
                    let _ = self.worker.progress();
                    if UCS_PTR_IS_PTR(request) {
                        // println!("Close request in progress");
                        if unsafe { ucp_request_check_status(request as _) }
                            != ucs_status_t::UCS_INPROGRESS
                        {
                            // println!("Close request completed");
                            break;
                        }
                    }
                }
                unsafe { ucp_request_free(request as _) };
                // println!("Close request freed");
            } else {
                // println!("Close error");
                let _ = Error::from_ptr(request);
            }
        };
    }
}

unsafe extern "C" fn cb(request: *mut std::ffi::c_void, status: ucs_status_t) {
    // This is a placeholder for the callback function.
    // In practice, you would implement the logic to handle the completion of the request.
    // For example, you might wake up a thread or signal an event.
    unsafe { ucp_request_free(request as _) };
}

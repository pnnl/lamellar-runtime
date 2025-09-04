use std::{ffi::CString, mem::MaybeUninit, sync::Arc};

use ucx1_sys::*;

use pmi::{pmi::Pmi, pmix::PmiX};

use super::{error::Error, worker::Worker};

/// The configuration for UCP application context.
#[derive(Debug)]
pub struct Config {
    handle: *mut ucp_config_t,
}

impl Default for Config {
    fn default() -> Self {
        let mut handle = MaybeUninit::uninit();
        let status =
            unsafe { ucp_config_read(std::ptr::null(), std::ptr::null(), handle.as_mut_ptr()) };
        Error::from_status(status).unwrap();

        Config {
            handle: unsafe { handle.assume_init() },
        }
    }
}

impl Config {
    /// Prints information about the context configuration.
    ///
    /// Including memory domains, transport resources, and other useful
    /// information associated with the context.
    pub fn print_to_stderr(&self) {
        let flags = ucs_config_print_flags_t::UCS_CONFIG_PRINT_CONFIG
            | ucs_config_print_flags_t::UCS_CONFIG_PRINT_DOC
            | ucs_config_print_flags_t::UCS_CONFIG_PRINT_HEADER
            | ucs_config_print_flags_t::UCS_CONFIG_PRINT_HIDDEN;
        let title = CString::new("UCP Configuration").expect("Not a valid CStr");
        unsafe { ucp_config_print(self.handle, stderr, title.as_ptr(), flags) };
    }
}

impl Drop for Config {
    fn drop(&mut self) {
        unsafe { ucp_config_release(self.handle) };
    }
}

/// An object that holds a UCP communication instance's global information.
#[derive(Debug)]
pub struct Context {
    pub(crate) handle: ucp_context_h,
}

// Context is thread safe.
unsafe impl Send for Context {}
unsafe impl Sync for Context {}

impl Context {
    /// Creates and initializes a UCP application context with default configuration.
    pub fn new(pmi: Arc<PmiX>) -> Result<Arc<Self>, Error> {
        Self::new_with_config(&Config::default(), pmi)
    }

    /// Creates and initializes a UCP application context with specified configuration.
    pub fn new_with_config(config: &Config, pmi: Arc<PmiX>) -> Result<Arc<Self>, Error> {
        let features = ucp_feature::UCP_FEATURE_RMA
            | ucp_feature::UCP_FEATURE_AMO64
            | ucp_feature::UCP_FEATURE_AMO32;

        let params = ucp_params_t {
            field_mask: (ucp_params_field::UCP_PARAM_FIELD_FEATURES
                | ucp_params_field::UCP_PARAM_FIELD_ESTIMATED_NUM_EPS
                | ucp_params_field::UCP_PARAM_FIELD_MT_WORKERS_SHARED)
                .0 as u64,
            features: features.0 as u64,
            estimated_num_eps: pmi.ranks().len() as u64,
            mt_workers_shared: 0,
            estimated_num_ppn: 0,
            request_size: 0,
            request_init: None,
            request_cleanup: None,
            tag_sender_mask: 0,
            name: std::ptr::null(),
        };
        let mut handle = MaybeUninit::uninit();
        let status = unsafe {
            ucp_init_version(
                UCP_API_MAJOR,
                UCP_API_MINOR,
                &params,
                config.handle,
                handle.as_mut_ptr(),
            )
        };
        Error::from_status(status)?;

        Ok(Arc::new(Context {
            handle: unsafe { handle.assume_init() },
            // pmi: pmi.clone(),
        }))
    }

    /// Create a `Worker` object.
    pub fn create_worker(self: &Arc<Self>) -> Result<Arc<Worker>, Error> {
        Worker::new(self.clone())
    }

    /// Prints information about the context configuration.
    ///
    /// Including memory domains, transport resources, and
    /// other useful information associated with the context.
    pub fn print_to_stderr(&self) {
        unsafe { ucp_context_print_info(self.handle, stderr) };
    }

    /// Fetches information about the context.
    pub fn query(&self) -> Result<ucp_context_attr, Error> {
        #[allow(invalid_value)]
        #[allow(clippy::uninit_assumed_init)]
        let mut attr = ucp_context_attr {
            field_mask: (ucp_context_attr_field::UCP_ATTR_FIELD_REQUEST_SIZE
                | ucp_context_attr_field::UCP_ATTR_FIELD_THREAD_MODE)
                .0 as u64,
            request_size: 0,
            thread_mode: ucs_thread_mode_t::UCS_THREAD_MODE_LAST,
            memory_types: 0,
            name: [0; 32],
        };
        let status = unsafe { ucp_context_query(self.handle, &mut attr) };
        Error::from_status(status)?;

        Ok(attr)
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        unsafe { ucp_cleanup(self.handle) };
    }
}

extern "C" {
    static stderr: *mut FILE;
}

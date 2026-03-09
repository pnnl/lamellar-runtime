/// A CUDA stream — the unit of ordered asynchronous execution on a GPU.
///
/// Kernels and memcpy operations submitted on the same stream execute in
/// issue order.  Operations on different streams may overlap.
///
/// # Feature gates
/// - Without `cuda`: a zero-cost stub; all operations are synchronous no-ops.
/// - With `cuda`: wraps `cust::stream::Stream`.
pub struct CudaStream {
    #[cfg(feature = "cuda")]
    inner: cust::stream::Stream,

    /// Device that owns this stream.
    device_id: u32,
}

impl CudaStream {
    /// Create a new (non-blocking) stream on `device_id`.
    ///
    /// # Errors
    /// Returns `GpuError` when CUDA stream creation fails.
    pub fn new(device_id: u32) -> Result<Self, crate::gpu::error::GpuError> {
        #[cfg(feature = "cuda")]
        {
            use cust::stream::StreamFlags;
            let inner = cust::stream::Stream::new(StreamFlags::NON_BLOCKING, None)
                .map_err(crate::gpu::error::GpuError::from)?;
            Ok(Self { inner, device_id })
        }
        #[cfg(not(feature = "cuda"))]
        {
            Ok(Self { device_id })
        }
    }

    /// Block the calling CPU thread until all previously enqueued work on
    /// this stream has completed.
    pub fn synchronize(&self) -> Result<(), crate::gpu::error::GpuError> {
        #[cfg(feature = "cuda")]
        {
            self.inner
                .synchronize()
                .map_err(crate::gpu::error::GpuError::from)
        }
        #[cfg(not(feature = "cuda"))]
        {
            Ok(())
        }
    }

    /// Return the device this stream belongs to.
    pub fn device_id(&self) -> u32 {
        self.device_id
    }

    /// Expose the raw `cust::Stream` for passing to cust launch helpers.
    #[cfg(feature = "cuda")]
    pub fn inner(&self) -> &cust::stream::Stream {
        &self.inner
    }
}

// CudaStream is bound to one device/thread but can be sent between Tokio tasks
// on the same PE — the CUDA driver is fine with this.
unsafe impl Send for CudaStream {}
unsafe impl Sync for CudaStream {}

use thiserror::Error;

/// All errors that can arise from GPU operations.
#[derive(Debug, Error)]
pub enum GpuError {
    // ── CUDA driver / runtime ──────────────────────────────────────────────
    /// Returned by the CUDA driver API when an operation fails.
    /// The inner string carries the CUDA error name (e.g. `"CUDA_ERROR_OUT_OF_MEMORY"`).
    #[error("CUDA driver error: {0}")]
    CudaDriver(String),

    /// Device memory allocation failed.
    #[error("CUDA device memory allocation failed: requested {bytes} bytes on device {device}")]
    Alloc { bytes: usize, device: u32 },

    /// Host→device or device→host transfer failed.
    #[error("CUDA memory copy failed: {0}")]
    Memcpy(String),

    // ── Kernel dispatch ────────────────────────────────────────────────────
    /// A kernel with the given name was never registered on this PE.
    #[error("GPU kernel not registered: \"{name}\"")]
    KernelNotFound { name: String },

    /// The kernel launched but returned a non-zero exit code (FFI path).
    #[error("FFI kernel \"{name}\" returned error code {code}")]
    FfiKernelFailed { name: String, code: i32 },

    /// rust-cuda PTX JIT compilation failed.
    #[cfg(feature = "rust-cuda")]
    #[error("PTX JIT compile failed for kernel \"{name}\": {reason}")]
    PtxCompile { name: String, reason: String },

    // ── Array / distribution ───────────────────────────────────────────────
    /// A `DistGpuArray` was used on a PE that never registered its local buffer.
    #[error("DistGpuArray (id={id}) has no local buffer registered on this PE")]
    ArrayNotFound { id: u64 },

    /// A kernel was launched with arguments that don't match the buffer element type.
    #[error("Type mismatch: expected element size {expected} bytes, got {actual}")]
    TypeMismatch { expected: usize, actual: usize },

    // ── Generic ────────────────────────────────────────────────────────────
    #[error("{0}")]
    Other(String),
}

impl GpuError {
    /// Convenience constructor for arbitrary string errors.
    pub fn other(msg: impl Into<String>) -> Self {
        GpuError::Other(msg.into())
    }
}

// Allow converting from the `cust` error type when the `cuda` feature is enabled.
#[cfg(feature = "cuda")]
impl From<cust::error::CudaError> for GpuError {
    fn from(e: cust::error::CudaError) -> Self {
        GpuError::CudaDriver(format!("{e:?}"))
    }
}

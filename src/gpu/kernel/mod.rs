/// GPU kernel abstractions.
///
/// # Calling-convention abstraction
///
/// Both the FFI-C and pure-Rust-GPU paths dispatch through the CUDA Driver API
/// at the same launch site.  `GpuKernel` abstracts that site with a single
/// `launch` method operating on raw device pointers.
use crate::gpu::{error::GpuError, stream::CudaStream, GpuSafe};

pub mod ffi;
pub use ffi::FfiCudaKernel;

#[cfg(feature = "rust-cuda")]
pub mod rust_gpu;
#[cfg(feature = "rust-cuda")]
pub use rust_gpu::RustGpuKernel;

// ─────────────────────────────────────────────────────────────────────────────
// LaunchConfig
// ─────────────────────────────────────────────────────────────────────────────

/// Grid / block / shared-memory parameters for a CUDA kernel launch.
///
/// Serializable so it can be carried inside Lamellar Active Messages.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct LaunchConfig {
    pub grid_dim: (u32, u32, u32),
    pub block_dim: (u32, u32, u32),
    pub shared_mem_bytes: u32,
}

impl LaunchConfig {
    /// 1-D launch covering `n` elements with `block_size` threads per block.
    pub fn simple_1d(n: u32, block_size: u32) -> Self {
        let grid = n.div_ceil(block_size);
        Self {
            grid_dim: (grid, 1, 1),
            block_dim: (block_size, 1, 1),
            shared_mem_bytes: 0,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// GpuKernel trait
// ─────────────────────────────────────────────────────────────────────────────

/// Unified calling convention for GPU kernels, regardless of authoring language.
///
/// Takes a raw mutable device pointer + element count rather than `DeviceBuffer`
/// to avoid ownership questions at dispatch time.
///
/// # Safety
/// `ptr` must point to a valid device allocation of `len` elements of type `T`
/// on the GPU associated with `stream`.
pub trait GpuKernel<T: GpuSafe>: Send + Sync + 'static {
    fn name(&self) -> &str;

    /// Enqueue work on `stream` — must **not** synchronize.
    ///
    /// # Safety
    /// See trait-level docs.
    unsafe fn launch(
        &self,
        ptr: *mut T,
        len: usize,
        config: &LaunchConfig,
        stream: &CudaStream,
    ) -> Result<(), GpuError>;
}

// ─────────────────────────────────────────────────────────────────────────────
// ErasedKernel — type-erased kernel stored in the registry
// ─────────────────────────────────────────────────────────────────────────────

/// Type-erased kernel launcher stored in `KERNEL_REGISTRY`.
///
/// The AM dispatch code doesn't know T; it passes raw bytes and the eraser
/// validates elem_size before casting.
pub(crate) trait ErasedKernel: Send + Sync + 'static {
    fn name(&self) -> &str;

    /// # Safety
    /// `ptr` must be a valid device allocation of `elem_size * len` bytes,
    /// and `elem_size` must match `size_of::<T>()` for the kernel's T.
    unsafe fn launch_erased(
        &self,
        ptr: *mut u8,
        elem_size: usize,
        len: usize,
        config: &LaunchConfig,
        stream: &CudaStream,
    ) -> Result<(), GpuError>;
}

/// Adapts any `GpuKernel<T>` into an `ErasedKernel`.
pub(crate) struct KernelEraser<T: GpuSafe, K: GpuKernel<T>> {
    pub(crate) kernel: K,
    _marker: std::marker::PhantomData<T>,
}

impl<T: GpuSafe, K: GpuKernel<T>> KernelEraser<T, K> {
    pub(crate) fn new(kernel: K) -> Self {
        Self { kernel, _marker: std::marker::PhantomData }
    }
}

impl<T: GpuSafe, K: GpuKernel<T>> ErasedKernel for KernelEraser<T, K> {
    fn name(&self) -> &str {
        self.kernel.name()
    }

    unsafe fn launch_erased(
        &self,
        ptr: *mut u8,
        elem_size: usize,
        len: usize,
        config: &LaunchConfig,
        stream: &CudaStream,
    ) -> Result<(), GpuError> {
        if elem_size != std::mem::size_of::<T>() {
            return Err(GpuError::TypeMismatch {
                expected: std::mem::size_of::<T>(),
                actual: elem_size,
            });
        }
        // SAFETY: caller guarantees alignment and validity for T.
        self.kernel.launch(ptr as *mut T, len, config, stream)
    }
}

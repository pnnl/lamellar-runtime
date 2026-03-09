/// GPU support for Lamellar — distributed GPU arrays with a unified kernel
/// calling convention over both pure-Rust (rust-cuda) and FFI-C CUDA kernels.
///
/// # Module layout
///
/// ```text
/// gpu/
///   mod.rs          ← GpuSafe marker trait + top-level re-exports (this file)
///   error.rs        ← GpuError enum
///   stream.rs       ← CudaStream abstraction
///   buffer.rs       ← DeviceBuffer<T> — owns device memory on one PE
///   kernel/
///     mod.rs        ← GpuKernel<T> trait, LaunchConfig, ErasedKernel
///     ffi.rs        ← FfiCudaKernel<T> — extern "C" launcher
///     rust_gpu.rs   ← RustGpuKernel<T> — rust-cuda/Rust-GPU PTX (feature "rust-cuda")
///   array/
///     mod.rs        ← DistGpuArray<T> — distributed handle
///     local.rs      ← LocalGpuArray<T>, per-PE ARRAY_REGISTRY, KERNEL_REGISTRY
///     dispatch.rs   ← ExecKernelAm, GpuKernelResult (Lamellar AM)
/// ```
///
/// # Feature flags
///
/// | Flag          | What it enables                                                      |
/// |---------------|----------------------------------------------------------------------|
/// | *(none)*      | Trait definitions + host-stub (Vec-backed) DeviceBuffer; no CUDA    |
/// | `cuda`        | Real device memory via `cust`; requires CUDA toolkit                 |
/// | `rust-cuda`   | RustGpuKernel PTX path; implies `cuda`; requires NVVM backend        |
///
/// # Quick start (no CUDA required)
///
/// ```rust,ignore
/// use lamellar_project::gpu::{GpuSafe, kernel::{FfiCudaKernel, LaunchConfig}, array::DistGpuArray};
///
/// // 1. Register your kernel on every PE (before dispatching).
/// gpu::array::register_kernel(FfiCudaKernel::<f32>::new("scale", my_scale_fn));
///
/// // 2. Create the distributed array (collective — all PEs call this).
/// let array_id = gpu::array::next_array_id();
/// let gpu_array = DistGpuArray::<f32>::new(world.clone(), array_id, 1024, 0)?;
///
/// // 3. Launch the kernel on all PEs, await distributed completion.
/// let handle = gpu_array.exec_kernel_all("scale", LaunchConfig::simple_1d(1024, 256));
/// let results = handle.block();
/// assert!(results.iter().all(|r| r.is_ok()));
/// ```
pub mod error;
pub mod stream;
pub mod buffer;
pub mod kernel;
pub mod array;

// ─────────────────────────────────────────────────────────────────────────────
// Global CUDA context — ensures all threads can push the right context
// ─────────────────────────────────────────────────────────────────────────────

/// Process-wide CUDA context created by `init_cuda_context`.
///
/// `cust::quick_init()` creates a driver-API context current only on the
/// calling (main) thread.  Lamellar dispatches AM executors on worker threads
/// that have no context current.  We store the context here and call
/// `ensure_cuda_context()` at the start of every CUDA operation so that any
/// thread can transparently push the context before use.
#[cfg(feature = "cuda")]
static CUDA_CTX: std::sync::OnceLock<std::sync::Arc<cust::context::Context>> =
    std::sync::OnceLock::new();

/// Store the CUDA context produced by `cust::quick_init()` for process-wide use.
///
/// Call this once on the main thread before creating any `DistGpuArray`:
/// ```rust,ignore
/// let ctx = cust::quick_init().expect("CUDA init");
/// lamellar_project::gpu::init_cuda_context(ctx);
/// ```
/// Returns an `Arc` to keep the context alive for the program lifetime.
#[cfg(feature = "cuda")]
pub fn init_cuda_context(ctx: cust::context::Context) -> std::sync::Arc<cust::context::Context> {
    let arc = std::sync::Arc::new(ctx);
    // Ignore duplicate-init; first caller wins.
    let _ = CUDA_CTX.set(arc.clone());
    arc
}

/// Push the stored CUDA context onto the current thread.
///
/// Called automatically by `LocalGpuArray::launch_kernel` and `sync`.
#[cfg(feature = "cuda")]
pub fn ensure_cuda_context() {
    if let Some(ctx) = CUDA_CTX.get() {
        cust::context::CurrentContext::set_current(ctx.as_ref())
            .expect("ensure_cuda_context: cuCtxSetCurrent failed");
    }
}

pub use error::GpuError;
pub use buffer::DeviceBuffer;
pub use stream::CudaStream;
pub use kernel::{GpuKernel, LaunchConfig, FfiCudaKernel};
pub use array::DistGpuArray;

#[cfg(feature = "rust-cuda")]
pub use kernel::RustGpuKernel;

// ─────────────────────────────────────────────────────────────────────────────
// GpuSafe — marker trait for GPU-compatible element types
// ─────────────────────────────────────────────────────────────────────────────

/// Marker trait for types that are safe to use as GPU array elements.
///
/// A type is `GpuSafe` if it is:
/// - `Copy` — elements are scalar values, not heap-allocated structures.
/// - `Send + Sync` — safe to transfer between threads (required by the
///   registry and Lamellar AM machinery).
/// - `'static` — no borrowed references (device memory outlives stack frames).
///
/// # Automatic implementations
/// All primitive numeric types implement `GpuSafe` out of the box.
/// Implement it manually for your own POD structs:
///
/// ```rust
/// #[repr(C)]
/// #[derive(Copy, Clone)]
/// struct Vec4 { x: f32, y: f32, z: f32, w: f32 }
/// impl lamellar_project::gpu::GpuSafe for Vec4 {}
/// ```
/// Without the `cuda` feature the only constraint is `Copy + Send + Sync + 'static`.
/// With the `cuda` feature we additionally require `cust::memory::DeviceCopy` so
/// that `DeviceBuffer<T>` can wrap `cust::memory::DeviceBuffer<T>` directly.
#[cfg(not(feature = "cuda"))]
pub trait GpuSafe: Copy + Send + Sync + 'static {}

#[cfg(feature = "cuda")]
pub trait GpuSafe: Copy + Send + Sync + cust::memory::DeviceCopy + 'static {}

// ── Blanket impls for numeric primitives ─────────────────────────────────────
macro_rules! impl_gpu_safe {
    ($($t:ty),*) => { $(impl GpuSafe for $t {})* }
}
impl_gpu_safe!(
    u8, u16, u32, u64, u128, usize,
    i8, i16, i32, i64, i128, isize,
    f32, f64
);

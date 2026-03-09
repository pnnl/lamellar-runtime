/// Per-PE GPU array state and the two process-global registries.
///
/// # Registry pattern
/// Lamellar AMs can't carry device pointers or Arc<RwLock<...>> across PEs —
/// they're not serializable.  Instead:
/// 1. Each PE registers its `LocalGpuArray` under a `u64` array_id.
/// 2. Kernels are registered by name.
/// 3. AMs carry only (array_id, kernel_name, LaunchConfig) — all serializable.
/// 4. AM exec looks up the local array + kernel from the registries.
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use dashmap::DashMap;
use crate::parking_lot::RwLock;

use crate::gpu::{
    buffer::DeviceBuffer,
    error::GpuError,
    kernel::{ErasedKernel, KernelEraser, GpuKernel, LaunchConfig},
    stream::CudaStream,
    GpuSafe,
};

// ─────────────────────────────────────────────────────────────────────────────
// GpuArrayEntry — object-safe trait for type-erased local array dispatch
// ─────────────────────────────────────────────────────────────────────────────

/// Object-safe interface exposed to the AM dispatch code and to benchmarks.
///
/// `LocalGpuArray<T>` implements this for any `T: GpuSafe`.  The AM doesn't
/// need to know T; it just calls `launch_kernel` and `sync`.
pub trait GpuArrayEntry: Send + Sync + 'static {
    fn len(&self) -> usize;
    fn device_id(&self) -> u32;
    fn launch_kernel(&mut self, name: &str, config: &LaunchConfig) -> Result<(), GpuError>;
    fn sync(&self) -> Result<(), GpuError>;
}

// ─────────────────────────────────────────────────────────────────────────────
// Global array registry
// ─────────────────────────────────────────────────────────────────────────────

static ARRAY_REGISTRY: std::sync::LazyLock<
    DashMap<u64, Arc<RwLock<dyn GpuArrayEntry>>>,
> = std::sync::LazyLock::new(DashMap::new);

static NEXT_ARRAY_ID: AtomicU64 = AtomicU64::new(1);

pub fn next_array_id() -> u64 {
    NEXT_ARRAY_ID.fetch_add(1, Ordering::Relaxed)
}

/// Register a `LocalGpuArray<T>` on this PE under `array_id`.
pub fn register_array<T: GpuSafe + 'static>(array_id: u64, array: LocalGpuArray<T>) {
    let entry: Arc<RwLock<dyn GpuArrayEntry>> = Arc::new(RwLock::new(array));
    ARRAY_REGISTRY.insert(array_id, entry);
}

/// Retrieve the type-erased entry for AM dispatch.
pub fn get_array_entry(
    array_id: u64,
) -> Result<Arc<RwLock<dyn GpuArrayEntry>>, GpuError> {
    ARRAY_REGISTRY
        .get(&array_id)
        .map(|r| Arc::clone(&*r))
        .ok_or(GpuError::ArrayNotFound { id: array_id })
}

pub fn deregister_array(array_id: u64) {
    ARRAY_REGISTRY.remove(&array_id);
}

// ─────────────────────────────────────────────────────────────────────────────
// Global kernel registry
// ─────────────────────────────────────────────────────────────────────────────

static KERNEL_REGISTRY: std::sync::LazyLock<
    DashMap<String, Box<dyn ErasedKernel>>,
> = std::sync::LazyLock::new(DashMap::new);

/// Register a kernel on this PE.  Must be called before any `exec_kernel_*`.
pub fn register_kernel<T, K>(kernel: K)
where
    T: GpuSafe + 'static,
    K: GpuKernel<T> + 'static,
{
    let name = kernel.name().to_owned();
    let erased: Box<dyn ErasedKernel> = Box::new(KernelEraser::<T, K>::new(kernel));
    KERNEL_REGISTRY.insert(name, erased);
}

pub(crate) fn get_kernel(name: &str) -> Result<dashmap::mapref::one::Ref<'static, String, Box<dyn ErasedKernel>>, GpuError> {
    KERNEL_REGISTRY
        .get(name)
        .ok_or_else(|| GpuError::KernelNotFound { name: name.to_owned() })
}

// ─────────────────────────────────────────────────────────────────────────────
// LocalGpuArray
// ─────────────────────────────────────────────────────────────────────────────

/// Per-PE GPU array — device-memory buffer + CUDA stream.
///
/// Never serialized; lives in the ARRAY_REGISTRY behind an Arc<RwLock<dyn GpuArrayEntry>>.
pub struct LocalGpuArray<T: GpuSafe> {
    pub buffer: DeviceBuffer<T>,
    pub stream: CudaStream,
    pub len: usize,
    pub device_id: u32,
    pub pe: usize,
    pub array_id: u64,
}

impl<T: GpuSafe> LocalGpuArray<T> {
    pub fn new(array_id: u64, len: usize, device_id: u32, pe: usize) -> Result<Self, GpuError> {
        let buffer = DeviceBuffer::alloc(len, device_id)?;
        let stream = CudaStream::new(device_id)?;
        Ok(Self { buffer, stream, len, device_id, pe, array_id })
    }
}

impl<T: GpuSafe + 'static> GpuArrayEntry for LocalGpuArray<T> {
    fn len(&self) -> usize { self.len }
    fn device_id(&self) -> u32 { self.device_id }

    fn launch_kernel(&mut self, name: &str, config: &LaunchConfig) -> Result<(), GpuError> {
        // Ensure the CUDA context is current on this thread (the AM may run
        // on a different thread than the one that called cust::quick_init).
        #[cfg(feature = "cuda")]
        crate::gpu::ensure_cuda_context();

        let kernel = get_kernel(name)?;
        let elem_size = std::mem::size_of::<T>();
        unsafe {
            kernel.launch_erased(
                self.buffer.as_device_ptr_mut() as *mut u8,
                elem_size,
                self.len,
                config,
                &self.stream,
            )
        }
    }

    fn sync(&self) -> Result<(), GpuError> {
        #[cfg(feature = "cuda")]
        crate::gpu::ensure_cuda_context();
        self.stream.synchronize()
    }
}

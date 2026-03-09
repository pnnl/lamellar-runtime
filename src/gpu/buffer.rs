use crate::gpu::{error::GpuError, GpuSafe};
use std::marker::PhantomData;

/// Owns a contiguous slab of CUDA device memory holding `len` elements of
/// type `T`.
///
/// # Memory model
/// - One `DeviceBuffer` lives on **one PE**, on **one GPU**.
/// - The device pointer is only valid on the PE/process that allocated it.
///   It must **never** be sent across the network or dereferenced on a
///   different PE (even with GPUDirect, use the explicit transfer API).
///
/// # Feature gates
/// - Without `cuda`: backed by a host-side `Vec<T>` for unit-testing the
///   distributed logic without a GPU present.
/// - With `cuda`: backed by `cust::memory::DeviceBuffer<T>`.
pub struct DeviceBuffer<T: GpuSafe> {
    len: usize,
    device_id: u32,

    #[cfg(feature = "cuda")]
    inner: cust::memory::DeviceBuffer<T>,

    /// Host-side fallback storage (no-cuda builds / unit tests).
    #[cfg(not(feature = "cuda"))]
    inner: Vec<T>,

    _marker: PhantomData<T>,
}

impl<T: GpuSafe> DeviceBuffer<T> {
    // ── Construction ───────────────────────────────────────────────────────

    /// Allocate `len` elements of uninitialized device memory on `device_id`.
    pub fn alloc(len: usize, device_id: u32) -> Result<Self, GpuError> {
        #[cfg(feature = "cuda")]
        {
            // TODO: select the device before allocating when multi-GPU support
            //       is added (cust::device::Device::set_current).
            let inner = unsafe { cust::memory::DeviceBuffer::uninitialized(len) }
                .map_err(|_| GpuError::Alloc {
                    bytes: len * std::mem::size_of::<T>(),
                    device: device_id,
                })?;
            Ok(Self { len, device_id, inner, _marker: PhantomData })
        }
        #[cfg(not(feature = "cuda"))]
        {
            let inner = vec![unsafe { std::mem::zeroed() }; len];
            Ok(Self { len, device_id, inner, _marker: PhantomData })
        }
    }

    /// Allocate and fill with `value`.
    pub fn alloc_filled(len: usize, device_id: u32, value: T) -> Result<Self, GpuError>
    where
        T: Copy,
    {
        let mut buf = Self::alloc(len, device_id)?;
        buf.fill(value)?;
        Ok(buf)
    }

    // ── Metadata ───────────────────────────────────────────────────────────

    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn device_id(&self) -> u32 {
        self.device_id
    }
    pub fn size_bytes(&self) -> usize {
        self.len * std::mem::size_of::<T>()
    }

    // ── Device pointer access (kernel dispatch) ────────────────────────────

    /// Raw device pointer — valid **only on this PE** for passing to kernels.
    ///
    /// # Safety
    /// The pointer must not outlive the `DeviceBuffer`, and must only be
    /// dereferenced from device code running on the same GPU.
    pub unsafe fn as_device_ptr(&self) -> *const T {
        #[cfg(feature = "cuda")]
        {
            // DeviceBuffer<T> derefs to DeviceSlice<T> which has as_device_ptr().
            // DevicePointer::as_ptr() -> *const T  (not as_raw which gives CUdeviceptr/u64).
            self.inner.as_device_ptr().as_ptr()
        }
        #[cfg(not(feature = "cuda"))]
        {
            self.inner.as_ptr()
        }
    }

    /// Mutable raw device pointer.
    pub unsafe fn as_device_ptr_mut(&mut self) -> *mut T {
        #[cfg(feature = "cuda")]
        {
            self.inner.as_device_ptr().as_mut_ptr()
        }
        #[cfg(not(feature = "cuda"))]
        {
            self.inner.as_mut_ptr()
        }
    }

    // ── Host ↔ Device transfers ────────────────────────────────────────────

    /// Copy host slice → device (synchronous).
    pub fn copy_from_host(&mut self, src: &[T]) -> Result<(), GpuError> {
        assert_eq!(src.len(), self.len, "DeviceBuffer::copy_from_host: length mismatch");
        #[cfg(feature = "cuda")]
        {
            // CopyDestination::copy_from is implemented on DeviceSlice<T>,
            // accessible here via DerefMut from DeviceBuffer<T>.
            use cust::memory::CopyDestination;
            self.inner.copy_from(src)
                .map_err(|e| GpuError::Memcpy(format!("{e:?}")))
        }
        #[cfg(not(feature = "cuda"))]
        {
            self.inner.copy_from_slice(src);
            Ok(())
        }
    }

    /// Copy device → host slice (synchronous).
    pub fn copy_to_host(&self, dst: &mut Vec<T>) -> Result<(), GpuError> {
        dst.resize(self.len, unsafe { std::mem::zeroed() });
        #[cfg(feature = "cuda")]
        {
            use cust::memory::CopyDestination;
            self.inner.copy_to(dst.as_mut_slice())
                .map_err(|e| GpuError::Memcpy(format!("{e:?}")))
        }
        #[cfg(not(feature = "cuda"))]
        {
            dst.copy_from_slice(&self.inner);
            Ok(())
        }
    }

    /// Fill every element with `value` (host-driven, synchronous).
    pub fn fill(&mut self, value: T) -> Result<(), GpuError>
    where
        T: Copy,
    {
        let host_data = vec![value; self.len];
        self.copy_from_host(&host_data)
    }
}

// SAFETY: `DeviceBuffer` is bound to one GPU context, but that context is
// tied to the OS process, not a specific thread.  `cust` uses the CUDA
// driver's per-thread current-context model; callers must ensure the right
// context is current before calling kernel-dispatch methods.  For SMP Lamellar
// (one process per PE) this is always satisfied.
unsafe impl<T: GpuSafe> Send for DeviceBuffer<T> {}
unsafe impl<T: GpuSafe> Sync for DeviceBuffer<T> {}

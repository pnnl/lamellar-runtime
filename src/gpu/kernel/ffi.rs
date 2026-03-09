/// FFI-C GPU kernel backend.
///
/// Wraps an `extern "C"` kernel launcher compiled from a `.cu` file.
///
/// # Example C wrapper (`my_kernel.cu`)
/// ```c
/// extern "C" int scale_f32(
///     float* data, size_t len,
///     unsigned gx, unsigned gy, unsigned gz,
///     unsigned bx, unsigned by, unsigned bz,
///     unsigned shared_mem, void* stream
/// ) {
///     scale_kernel<<<dim3(gx,gy,gz), dim3(bx,by,bz), shared_mem, (cudaStream_t)stream>>>(data, len);
///     return (int)cudaGetLastError();
/// }
/// ```
use crate::gpu::{error::GpuError, stream::CudaStream, GpuSafe};
use super::{GpuKernel, LaunchConfig};

/// The canonical FFI kernel launcher signature.
pub type FfiKernelFn<T> = unsafe extern "C" fn(
    ptr: *mut T,
    len: usize,
    grid_x: u32, grid_y: u32, grid_z: u32,
    block_x: u32, block_y: u32, block_z: u32,
    shared_mem: u32,
    stream: *mut core::ffi::c_void,
) -> i32;

/// A [`GpuKernel`] backed by an `extern "C"` launcher.
pub struct FfiCudaKernel<T: GpuSafe> {
    name: String,
    func: FfiKernelFn<T>,
}

impl<T: GpuSafe> FfiCudaKernel<T> {
    pub fn new(name: impl Into<String>, func: FfiKernelFn<T>) -> Self {
        Self { name: name.into(), func }
    }
}

impl<T: GpuSafe> GpuKernel<T> for FfiCudaKernel<T> {
    fn name(&self) -> &str {
        &self.name
    }

    unsafe fn launch(
        &self,
        ptr: *mut T,
        len: usize,
        config: &LaunchConfig,
        stream: &CudaStream,
    ) -> Result<(), GpuError> {
        let (gx, gy, gz) = config.grid_dim;
        let (bx, by, bz) = config.block_dim;

        #[cfg(feature = "cuda")]
        let stream_ptr = stream.inner().as_inner() as *mut core::ffi::c_void;
        #[cfg(not(feature = "cuda"))]
        let stream_ptr = std::ptr::null_mut::<core::ffi::c_void>();

        let ret = (self.func)(
            ptr, len,
            gx, gy, gz,
            bx, by, bz,
            config.shared_mem_bytes,
            stream_ptr,
        );

        if ret != 0 {
            Err(GpuError::FfiKernelFailed { name: self.name.clone(), code: ret })
        } else {
            Ok(())
        }
    }
}

unsafe impl<T: GpuSafe> Send for FfiCudaKernel<T> {}
unsafe impl<T: GpuSafe> Sync for FfiCudaKernel<T> {}

/// Pure-Rust GPU kernel backend (rust-cuda / Rust-GPU toolchain).
///
/// Requires feature `rust-cuda`.  See module-level docs in `kernel/mod.rs`
/// for toolchain requirements and a device crate example.
use crate::gpu::{error::GpuError, stream::CudaStream, GpuSafe};
use super::{GpuKernel, LaunchConfig};
use std::sync::Arc;

/// A [`GpuKernel`] backed by a rust-cuda PTX function.
pub struct RustGpuKernel<T: GpuSafe> {
    name: String,
    func_name: String,
    module: Arc<cust::module::Module>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: GpuSafe> RustGpuKernel<T> {
    /// JIT-compile `ptx_bytes` and retrieve the function named `func_name`.
    pub fn from_ptx(
        display_name: impl Into<String>,
        func_name: impl Into<String>,
        ptx_bytes: &[u8],
    ) -> Result<Self, GpuError> {
        let func_name = func_name.into();
        let module = cust::module::Module::from_ptx(ptx_bytes, &[])
            .map_err(|e| GpuError::PtxCompile {
                name: func_name.clone(),
                reason: format!("{e:?}"),
            })?;
        Ok(Self {
            name: display_name.into(),
            func_name,
            module: Arc::new(module),
            _marker: std::marker::PhantomData,
        })
    }

    pub fn from_module(
        display_name: impl Into<String>,
        func_name: impl Into<String>,
        module: Arc<cust::module::Module>,
    ) -> Self {
        Self {
            name: display_name.into(),
            func_name: func_name.into(),
            module,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T: GpuSafe> GpuKernel<T> for RustGpuKernel<T> {
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
        let func = self
            .module
            .get_function(&self.func_name)
            .map_err(|e| GpuError::CudaDriver(format!("{e:?}")))?;

        let (gx, gy, gz) = config.grid_dim;
        let (bx, by, bz) = config.block_dim;

        cust::launch!(
            func<<<
                (gx, gy, gz),
                (bx, by, bz),
                config.shared_mem_bytes,
                stream.inner()
            >>>(ptr, len)
        )
        .map_err(|e| GpuError::CudaDriver(format!("{e:?}")))
    }
}

unsafe impl<T: GpuSafe> Send for RustGpuKernel<T> {}
unsafe impl<T: GpuSafe> Sync for RustGpuKernel<T> {}

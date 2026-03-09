/// Distributed GPU array — one LocalGpuArray per Lamellar PE.
pub mod dispatch;
pub mod local;

pub use dispatch::{ExecKernelAm, GpuKernelResult};
pub use local::{
    deregister_array, get_array_entry, next_array_id, register_array, register_kernel,
    GpuArrayEntry, LocalGpuArray,
};

use crate::ActiveMessaging;

use crate::gpu::{error::GpuError, kernel::LaunchConfig, GpuSafe};

/// Distributed GPU array handle.
///
/// Clone and cheap to pass around on the creating PE.  The actual device
/// memory lives in per-PE `LocalGpuArray<T>` objects in the process-local
/// ARRAY_REGISTRY.
///
/// NOTE: `team` is skipped during serde, so this type can be serialized
/// and placed in an `#[lamellar::AmData]` struct — but `exec_kernel_*`
/// will panic if called on a deserialized copy (team = None).  Use the
/// array_id + ExecKernelAm directly from within AMs instead.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(bound = "")]   // suppress PhantomData<T> serialization bound
pub struct DistGpuArray<T: GpuSafe> {
    pub array_id: u64,
    pub global_len: usize,
    pub len_per_pe: usize,
    pub num_pes: usize,
    pub root_pe: usize,

    #[serde(skip)]
    pub(crate) team: Option<crate::LamellarWorld>,

    _marker: std::marker::PhantomData<T>,
}

impl<T: GpuSafe + 'static> DistGpuArray<T> {
    /// **Collective.** All PEs call this with the same `array_id`.
    pub fn new(
        world: &crate::LamellarWorld,
        array_id: u64,
        len_per_pe: usize,
        device_id: u32,
    ) -> Result<Self, GpuError> {
        let my_pe = world.my_pe();
        let num_pes = world.num_pes();
        let local = LocalGpuArray::<T>::new(array_id, len_per_pe, device_id, my_pe)?;
        register_array(array_id, local);
        Ok(Self {
            array_id,
            global_len: len_per_pe * num_pes,
            len_per_pe,
            num_pes,
            root_pe: my_pe,
            team: Some(world.clone()),
            _marker: std::marker::PhantomData,
        })
    }

    /// Launch `kernel_name` on every PE.
    ///
    /// Returns a `LamellarTask` that resolves to `Vec<GpuKernelResult>` once
    /// ALL PEs have finished executing the kernel and synced their CUDA stream.
    pub fn exec_kernel_all(
        &self,
        kernel_name: impl Into<String>,
        config: LaunchConfig,
    ) -> crate::LamellarTask<Vec<GpuKernelResult>> {
        let world = self.team.as_ref().expect("DistGpuArray::exec_kernel_all: no team");
        world.exec_am_all(ExecKernelAm {
            array_id: self.array_id,
            kernel_name: kernel_name.into(),
            config,
        }).spawn()
    }

    /// Launch on a single PE.
    pub fn exec_kernel_pe(
        &self,
        pe: usize,
        kernel_name: impl Into<String>,
        config: LaunchConfig,
    ) -> crate::LamellarTask<GpuKernelResult> {
        let world = self.team.as_ref().expect("DistGpuArray: no team");
        world.exec_am_pe(pe, ExecKernelAm {
            array_id: self.array_id,
            kernel_name: kernel_name.into(),
            config,
        }).spawn()
    }

    /// Synchronize the local PE's CUDA stream.
    ///
    /// Call this after `exec_kernel_all().block()` returns to ensure the GPU
    /// has finished all kernel work on this PE.  Must be called from a
    /// non-async context (it blocks the calling thread until the GPU is idle).
    pub fn sync_local(&self) -> Result<(), GpuError> {
        let entry = get_array_entry(self.array_id)?;
        let guard = entry.read();
        guard.sync()
    }

    pub fn array_id(&self) -> u64 { self.array_id }
    pub fn global_len(&self) -> usize { self.global_len }
    pub fn len_per_pe(&self) -> usize { self.len_per_pe }
    pub fn num_pes(&self) -> usize { self.num_pes }
}

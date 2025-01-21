use super::{CommAllocAddr, CommSlice};
use crate::{
    active_messaging::AMCounters,
    lamellae::{local_lamellae::rdma::LocalFuture, shmem_lamellae::rdma::ShmemFuture, Scheduler},
    Dist, LamellarTask, LamellarTeamRT,
};

use enum_dispatch::enum_dispatch;
use futures_util::Future;
use pin_project::pin_project;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

pub(crate) trait Remote: Copy + Send + 'static {}
impl<T: Copy + Send + 'static> Remote for T {}

/// A task handle for raw RMDA (put/get) operation
#[pin_project(project = RdmaHandleProj)]
pub enum RdmaHandle<T> {
    #[cfg(feature = "rofi")]
    Rofi(#[pin] RofiFuture),
    #[cfg(feature = "enable-rofi-rust")]
    RofiRust(#[pin] RofiRustFuture),
    #[cfg(feature = "enable-rofi-rust")]
    RofiRustAsync(#[pin] RofiRustAsyncFuture),
    #[cfg(feature = "enable-libfabric")]
    LibFab(#[pin] LibFabFuture),
    #[cfg(feature = "enable-libfabric")]
    LibFabAsync(#[pin] LibFabAsyncFuture),
    Shmem(#[pin] ShmemFuture<T>),
    Local(#[pin] LocalFuture<T>),
}

impl<T: Remote> RdmaHandle<T> {
    /// This method will block the calling thread until the associated Array RDMA Operation completes
    pub(crate) fn block(self, scheduler: &Arc<Scheduler>, outstanding_reqs: Vec<Arc<AMCounters>>) {
        match self {
            #[cfg(feature = "rofi")]
            RdmaHandle::Rofi(f) => f.block(),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaHandle::RofiRust(f) => f.block(),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaHandle::RofiRustAsync(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            RdmaHandle::LibFab(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            RdmaHandle::LibFabAsync(f) => f.block(),
            RdmaHandle::Shmem(f) => f.block(),
            RdmaHandle::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub(crate) fn spawn(
        self,
        scheduler: &Arc<Scheduler>,
        outstanding_reqs: Vec<Arc<AMCounters>>,
    ) -> LamellarTask<()> {
        match self {
            #[cfg(feature = "rofi")]
            RdmaHandle::Rofi(f) => f.spawn(),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaHandle::RofiRust(f) => f.spawn(),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaHandle::RofiRustAsync(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            RdmaHandle::LibFab(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            RdmaHandle::LibFabAsync(f) => f.spawn(),
            RdmaHandle::Shmem(f) => f.spawn(scheduler, outstanding_reqs),
            RdmaHandle::Local(f) => f.spawn(scheduler, outstanding_reqs),
        }
    }
}

impl<T: Remote> Future for RdmaHandle<T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            #[cfg(feature = "rofi")]
            RdmaHandleProj::Rofi(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaHandleProj::RofiRust(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaHandleProj::RofiRustAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            RdmaHandleProj::LibFab(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            RdmaHandleProj::LibFabAsync(f) => f.poll(cx),
            RdmaHandleProj::Shmem(f) => f.poll(cx),
            RdmaHandleProj::Local(f) => f.poll(cx),
        }
    }
}

#[enum_dispatch]
pub(crate) trait CommRdma {
    fn put<T: Remote>(&self, pe: usize, src: CommSlice<T>, dst: CommAllocAddr) -> RdmaHandle<T>;
    fn put_all<T: Remote>(&self, src: CommSlice<T>, dst: CommAllocAddr) -> RdmaHandle<T>;
    fn get<T: Remote>(&self, pe: usize, src: CommAllocAddr, dst: CommSlice<T>) -> RdmaHandle<T>;
}

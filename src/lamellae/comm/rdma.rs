use super::{CommAllocAddr, CommSlice};
#[cfg(feature = "rofi-c")]
use crate::lamellae::rofi_c_lamellae::rdma::RofiCFuture;
use crate::{
    active_messaging::AMCounters,
    lamellae::{local_lamellae::rdma::LocalFuture, shmem_lamellae::rdma::ShmemFuture, Scheduler},
    LamellarTask,
};

#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabric_lamellae::rdma::LibfabricFuture;

use enum_dispatch::enum_dispatch;
use futures_util::Future;
use pin_project::pin_project;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

pub trait Remote: Copy + Send + 'static {}
impl<T: Copy + Send + 'static> Remote for T {}

/// A task handle for raw RMDA (put/get) operation
#[must_use = " RdmaHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct RdmaHandle<T> {
    #[pin]
    pub(crate) future: RdmaFuture<T>,
}

#[pin_project(project = RdmaFutureProj)]
pub(crate) enum RdmaFuture<T> {
    #[cfg(feature = "rofi-c")]
    RofiC(#[pin] RofiCFuture<T>),
    #[cfg(feature = "enable-rofi-rust")]
    RofiRust(#[pin] RofiRustFuture),
    #[cfg(feature = "enable-rofi-rust")]
    RofiRustAsync(#[pin] RofiRustAsyncFuture),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricFuture<T>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricAsync(#[pin] LibfabricAsyncFuture),
    Shmem(#[pin] ShmemFuture<T>),
    Local(#[pin] LocalFuture<T>),
}

impl<T: Remote> RdmaHandle<T> {
    /// This method will block the calling thread until the associated Array RDMA Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "rofi-c")]
            RdmaFuture::RofiC(f) => f.block(),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaFuture::RofiRust(f) => f.block(),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaFuture::RofiRustAsync(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            RdmaFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // RdmaFuture::LibfabricAsync(f) => f.block(),
            RdmaFuture::Shmem(f) => f.block(),
            RdmaFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "rofi-c")]
            RdmaFuture::RofiC(f) => f.spawn(),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaFuture::RofiRust(f) => f.spawn(),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaFuture::RofiRustAsync(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            RdmaFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // RdmaFuture::LibfabricAsync(f) => f.spawn(),
            RdmaFuture::Shmem(f) => f.spawn(),
            RdmaFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote> Future for RdmaHandle<T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "rofi-c")]
            RdmaFutureProj::RofiC(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaFutureProj::RofiRust(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaFutureProj::RofiRustAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            RdmaFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // RdmaFutureProj::LibfabricAsync(f) => f.poll(cx),
            RdmaFutureProj::Shmem(f) => f.poll(cx),
            RdmaFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[enum_dispatch]
pub(crate) trait CommRdma {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        src: CommSlice<T>,
        dst: CommAllocAddr,
    ) -> RdmaHandle<T>;
    fn put2<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        src: T,
        dst: CommAllocAddr,
    ) -> RdmaHandle<T>;
    fn put_test<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        src: T,
        dst: CommAllocAddr,
    ); //-> RdmaHandle<T>;
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: CommSlice<T>,
        dst: CommAllocAddr,
    ) -> RdmaHandle<T>;
    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        src: CommAllocAddr,
        dst: CommSlice<T>,
    ) -> RdmaHandle<T>;
    fn get_test<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        src: CommAllocAddr,
    ) -> T {
        let data: std::mem::MaybeUninit<T> = std::mem::MaybeUninit::uninit();
        unsafe { data.assume_init() }
    }
}

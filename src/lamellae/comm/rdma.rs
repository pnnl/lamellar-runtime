#[cfg(feature = "rofi-c")]
use crate::lamellae::rofi_c_lamellae::rdma::RofiCFuture;
use crate::{
    active_messaging::AMCounters,
    lamellae::{
        local_lamellae::rdma::{LocalAllocAtFuture, LocalAllocFuture},
        shmem_lamellae::rdma::{ShmemAtFuture, ShmemFuture},
        CommSlice, Scheduler,
    },
    memregion::MemregionRdmaInputInner,
    LamellarTask,
};

#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabric_lamellae::rdma::{LibfabricAllocAtFuture, LibfabricAllocFuture};
#[cfg(feature = "enable-ucx")]
use crate::lamellae::ucx_lamellae::rdma::{UcxAllocAtFuture, UcxAllocFuture};

use enum_dispatch::enum_dispatch;
use futures_util::Future;
use pin_project::pin_project;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

pub trait Remote: Copy + Sync + Send + 'static {}
impl<T: Copy + Sync + Send + 'static> Remote for T {}

/// A task handle for raw RMDA (put/get) operation
#[must_use = " RdmaHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct RdmaHandle<T: Remote> {
    #[pin]
    pub(crate) future: RdmaFuture<T>,
}

#[pin_project(project = RdmaFutureProj)]
pub(crate) enum RdmaFuture<T: Remote> {
    #[cfg(feature = "rofi-c")]
    RofiC(#[pin] RofiCFuture<T>),
    #[cfg(feature = "enable-rofi-rust")]
    RofiRust(#[pin] RofiRustFuture),
    #[cfg(feature = "enable-rofi-rust")]
    RofiRustAsync(#[pin] RofiRustAsyncFuture),
    #[cfg(feature = "enable-libfabric")]
    LibfabricAlloc(#[pin] LibfabricAllocFuture<T>),
    #[cfg(feature = "enable-ucx")]
    UcxAlloc(#[pin] UcxAllocFuture<T>),
    Shmem(#[pin] ShmemFuture<T>),
    // Local(#[pin] LocalFuture<T>),
    LocalAlloc(#[pin] LocalAllocFuture<T>),
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
            RdmaFuture::LibfabricAlloc(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            RdmaFuture::UcxAlloc(f) => f.block(),
            RdmaFuture::Shmem(f) => f.block(),
            RdmaFuture::LocalAlloc(f) => f.block(),
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
            RdmaFuture::LibfabricAlloc(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            RdmaFuture::UcxAlloc(f) => f.spawn(),
            RdmaFuture::Shmem(f) => f.spawn(),
            RdmaFuture::LocalAlloc(f) => f.spawn(),
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
            RdmaFutureProj::LibfabricAlloc(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            RdmaFutureProj::UcxAlloc(f) => f.poll(cx),
            RdmaFutureProj::Shmem(f) => f.poll(cx),
            RdmaFutureProj::LocalAlloc(f) => f.poll(cx),
        }
    }
}

#[must_use = " RdmaGetHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct RdmaGetHandle<T> {
    #[pin]
    pub(crate) future: RdmaGetFuture<T>,
}

#[pin_project(project = RdmaGetFutureProj)]
pub(crate) enum RdmaGetFuture<T> {
    #[cfg(feature = "rofi-c")]
    RofiC(#[pin] RofiCFuture<T>),
    #[cfg(feature = "enable-rofi-rust")]
    RofiRust(#[pin] RofiRustFuture),
    #[cfg(feature = "enable-rofi-rust")]
    RofiRustAsync(#[pin] RofiRustAsyncFuture),
    #[cfg(feature = "enable-libfabric")]
    LibfabricAlloc(#[pin] LibfabricAllocAtFuture<T>),
    #[cfg(feature = "enable-ucx")]
    UcxAlloc(#[pin] UcxAllocAtFuture<T>),
    Shmem(#[pin] ShmemAtFuture<T>),
    LocalAlloc(#[pin] LocalAllocAtFuture<T>),
}

impl<T: Remote> RdmaGetHandle<T> {
    /// This method will block the calling thread until the associated Array RDMA Operation completes
    pub fn block(self) -> T {
        match self.future {
            #[cfg(feature = "rofi-c")]
            RdmaGetFuture::RofiC(f) => f.block(),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaGetFuture::RofiRust(f) => f.block(),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaGetFuture::RofiRustAsync(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            RdmaGetFuture::LibfabricAlloc(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            RdmaGetFuture::UcxAlloc(f) => f.block(),
            RdmaGetFuture::Shmem(f) => f.block(),
            RdmaGetFuture::LocalAlloc(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<T> {
        match self.future {
            #[cfg(feature = "rofi-c")]
            RdmaGetFuture::RofiC(f) => f.spawn(),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaGetFuture::RofiRust(f) => f.spawn(),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaGetFuture::RofiRustAsync(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            RdmaGetFuture::LibfabricAlloc(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            RdmaGetFuture::UcxAlloc(f) => f.spawn(),
            RdmaGetFuture::Shmem(f) => f.spawn(),
            RdmaGetFuture::LocalAlloc(f) => f.spawn(),
        }
    }
}

impl<T: Remote> Future for RdmaGetHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "rofi-c")]
            RdmaGetFutureProj::RofiC(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaGetFutureProj::RofiRust(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaGetFutureProj::RofiRustAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            RdmaGetFutureProj::LibfabricAlloc(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            RdmaGetFutureProj::UcxAlloc(f) => f.poll(cx),
            RdmaGetFutureProj::Shmem(f) => f.poll(cx),
            RdmaGetFutureProj::LocalAlloc(f) => f.poll(cx),
        }
    }
}

#[enum_dispatch]
pub(crate) trait CommAllocRdma {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T>;
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize);
    fn put_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T>;
    fn put_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    );
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        offset: usize,
    ) -> RdmaHandle<T>;
    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize);
    fn put_all_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) -> RdmaHandle<T>;
    fn put_all_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    );
    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T>;
    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<Vec<T>>;
    fn get_into_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
        dst: CommSlice<T>,
    ) -> RdmaGetHandle<Vec<T>>;
    fn get_into_buffer_unmanaged<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
        dst: CommSlice<T>,
    );
}

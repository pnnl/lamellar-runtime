#[cfg(feature = "enable-libfabric-async")]
use crate::lamellae::libfabric_async_lamellae::rdma::{
    LibfabricAsyncGetBufferFuture, LibfabricAsyncGetFuture, LibfabricAsyncGetIntoBufferFuture,
    LibfabricAsyncPutFuture,
};
#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabric_lamellae::rdma::{
    LibfabricGetBufferFuture, LibfabricGetFuture, LibfabricGetIntoBufferFuture, LibfabricPutFuture,
};
#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabric_lamellae_mt::rdma::{
    LibfabricMtGetBufferFuture, LibfabricMtGetFuture, LibfabricMtGetIntoBufferFuture,
    LibfabricMtPutFuture,
};
#[cfg(feature = "enable-rofi-c")]
use crate::lamellae::rofi_c_lamellae::rdma::{
    RofiCGetBufferFuture, RofiCGetFuture, RofiCGetIntoBufferFuture, RofiCPutFuture,
};
#[cfg(feature = "enable-ucx")]
use crate::lamellae::ucx_lamellae::rdma::{
    UcxGetBufferFuture, UcxGetFuture, UcxGetIntoBufferFuture, UcxPutFuture,
};
#[cfg(feature = "enable-ucx")]
use crate::lamellae::ucx_lamellae_mt::rdma::{
    UcxMtGetBufferFuture, UcxMtGetFuture, UcxMtGetIntoBufferFuture, UcxMtPutFuture,
};
use crate::{
    active_messaging::AMCounters,
    lamellae::{
        local_lamellae::rdma::{
            LocalFuture, LocalGetBufferFuture, LocalGetFuture, LocalGetIntoBufferFuture,
        },
        shmem_lamellae::rdma::{
            ShmemFuture, ShmemGetBufferFuture, ShmemGetFuture, ShmemGetIntoBufferFuture,
        },
        Scheduler,
    },
    memregion::{AsLamellarBuffer, LamellarBuffer, MemregionRdmaInputInner},
    LamellarTask,
};

use enum_dispatch::enum_dispatch;
use futures_util::Future;
use pin_project::pin_project;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

pub trait Remote: Copy + Sync + Send + Default + 'static {}
impl<T: Copy + Sync + Send + Default + 'static> Remote for T {}

/// A task handle for raw RMDA (put/get) operation
#[must_use = " RdmaHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct RdmaHandle<T: Remote> {
    #[pin]
    pub(crate) future: RdmaPutFuture<T>,
}

#[pin_project(project = RdmaPutFutureProj)]
pub(crate) enum RdmaPutFuture<T: Remote> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricPutFuture<T>),
    #[cfg(feature = "enable-libfabric")]
    LibfabricMt(#[pin] LibfabricMtPutFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncPutFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxPutFuture<T>),
    #[cfg(feature = "enable-ucx")]
    UcxMt(#[pin] UcxMtPutFuture<T>),
    #[cfg(feature = "enable-rofi-c")]
    RofiC(#[pin] RofiCPutFuture<T>),
    Shmem(#[pin] ShmemFuture<T>),
    // Local(#[pin] LocalFuture<T>),
    Local(#[pin] LocalFuture<T>),
}

impl<T: Remote> RdmaHandle<T> {
    /// This method will block the calling thread until the associated Array RDMA Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            RdmaPutFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            RdmaPutFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            RdmaPutFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            RdmaPutFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            RdmaPutFuture::UcxMt(f) => f.block(),
            #[cfg(feature = "enable-rofi-c")]
            RdmaPutFuture::RofiC(f) => f.block(),
            RdmaPutFuture::Shmem(f) => f.block(),
            RdmaPutFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            RdmaPutFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            RdmaPutFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            RdmaPutFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            RdmaPutFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            RdmaPutFuture::UcxMt(f) => f.spawn(),
            #[cfg(feature = "enable-rofi-c")]
            RdmaPutFuture::RofiC(f) => f.spawn(),
            RdmaPutFuture::Shmem(f) => f.spawn(),
            RdmaPutFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote> Future for RdmaHandle<T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            RdmaPutFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            RdmaPutFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            RdmaPutFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            RdmaPutFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            RdmaPutFutureProj::UcxMt(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-c")]
            RdmaPutFutureProj::RofiC(f) => f.poll(cx),
            RdmaPutFutureProj::Shmem(f) => f.poll(cx),
            RdmaPutFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[must_use = " RdmaGetHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct RdmaGetHandle<T: Remote> {
    #[pin]
    pub(crate) future: RdmaGetFuture<T>,
}

#[pin_project(project = RdmaGetFutureProj)]
pub(crate) enum RdmaGetFuture<T: Remote> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricGetFuture<T>),
    #[cfg(feature = "enable-libfabric")]
    LibfabricMt(#[pin] LibfabricMtGetFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncGetFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxGetFuture<T>),
    #[cfg(feature = "enable-ucx")]
    UcxMt(#[pin] UcxMtGetFuture<T>),
    #[cfg(feature = "enable-rofi-c")]
    RofiC(#[pin] RofiCGetFuture<T>),
    Shmem(#[pin] ShmemGetFuture<T>),
    Local(#[pin] LocalGetFuture<T>),
}

impl<T: Remote> RdmaGetHandle<T> {
    /// This method will block the calling thread until the associated Array RDMA Operation completes
    pub fn block(self) -> T {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            RdmaGetFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            RdmaGetFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            RdmaGetFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            RdmaGetFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            RdmaGetFuture::UcxMt(f) => f.block(),
            #[cfg(feature = "enable-rofi-c")]
            RdmaGetFuture::RofiC(f) => f.block(),
            RdmaGetFuture::Shmem(f) => f.block(),
            RdmaGetFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<T> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            RdmaGetFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            RdmaGetFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            RdmaGetFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            RdmaGetFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            RdmaGetFuture::UcxMt(f) => f.spawn(),
            #[cfg(feature = "enable-rofi-c")]
            RdmaGetFuture::RofiC(f) => f.spawn(),
            RdmaGetFuture::Shmem(f) => f.spawn(),
            RdmaGetFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote> Future for RdmaGetHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            RdmaGetFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            RdmaGetFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            RdmaGetFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            RdmaGetFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            RdmaGetFutureProj::UcxMt(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-c")]
            RdmaGetFutureProj::RofiC(f) => f.poll(cx),
            RdmaGetFutureProj::Shmem(f) => f.poll(cx),
            RdmaGetFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[must_use = " RdmaGetHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct RdmaGetBufferHandle<T: Remote> {
    #[pin]
    pub(crate) future: RdmaGetBufferFuture<T>,
}

#[pin_project(project = RdmaGetBufFutureProj)]
pub(crate) enum RdmaGetBufferFuture<T: Remote> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricGetBufferFuture<T>),
    #[cfg(feature = "enable-libfabric")]
    LibfabricMt(#[pin] LibfabricMtGetBufferFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncGetBufferFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxGetBufferFuture<T>),
    #[cfg(feature = "enable-ucx")]
    UcxMt(#[pin] UcxMtGetBufferFuture<T>),
    #[cfg(feature = "enable-rofi-c")]
    RofiC(#[pin] RofiCGetBufferFuture<T>),
    Shmem(#[pin] ShmemGetBufferFuture<T>),
    Local(#[pin] LocalGetBufferFuture<T>),
}

impl<T: Remote> RdmaGetBufferHandle<T> {
    /// This method will block the calling thread until the associated Array RDMA Operation completes
    pub fn block(self) -> Vec<T> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            RdmaGetBufferFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            RdmaGetBufferFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            RdmaGetBufferFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            RdmaGetBufferFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            RdmaGetBufferFuture::UcxMt(f) => f.block(),
            #[cfg(feature = "enable-rofi-c")]
            RdmaGetBufferFuture::RofiC(f) => f.block(),
            RdmaGetBufferFuture::Shmem(f) => f.block(),
            RdmaGetBufferFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<Vec<T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            RdmaGetBufferFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            RdmaGetBufferFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            RdmaGetBufferFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            RdmaGetBufferFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            RdmaGetBufferFuture::UcxMt(f) => f.spawn(),
            #[cfg(feature = "enable-rofi-c")]
            RdmaGetBufferFuture::RofiC(f) => f.spawn(),
            RdmaGetBufferFuture::Shmem(f) => f.spawn(),
            RdmaGetBufferFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote> Future for RdmaGetBufferHandle<T> {
    type Output = Vec<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            RdmaGetBufFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            RdmaGetBufFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            RdmaGetBufFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            RdmaGetBufFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            RdmaGetBufFutureProj::UcxMt(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-c")]
            RdmaGetBufFutureProj::RofiC(f) => f.poll(cx),
            RdmaGetBufFutureProj::Shmem(f) => f.poll(cx),
            RdmaGetBufFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[must_use = " RdmaHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct RdmaGetIntoBufferHandle<T: Remote, B: AsLamellarBuffer<T>> {
    #[pin]
    pub(crate) future: RdmaGetIntoBufferFuture<T, B>,
}

#[pin_project(project = RdmaGetIntoBufferFutureProj)]
pub(crate) enum RdmaGetIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricGetIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric")]
    LibfabricMt(#[pin] LibfabricMtGetIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncGetIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxGetIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx")]
    UcxMt(#[pin] UcxMtGetIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-rofi-c")]
    RofiC(#[pin] RofiCGetIntoBufferFuture<T, B>),
    Shmem(#[pin] ShmemGetIntoBufferFuture<T, B>),
    // Local(#[pin] LocalFuture<T>),
    Local(#[pin] LocalGetIntoBufferFuture<T, B>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> RdmaGetIntoBufferHandle<T, B> {
    /// This method will block the calling thread until the associated Array RDMA Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            RdmaGetIntoBufferFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            RdmaGetIntoBufferFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            RdmaGetIntoBufferFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            RdmaGetIntoBufferFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            RdmaGetIntoBufferFuture::UcxMt(f) => f.block(),
            #[cfg(feature = "enable-rofi-c")]
            RdmaGetIntoBufferFuture::RofiC(f) => f.block(),
            RdmaGetIntoBufferFuture::Shmem(f) => f.block(),
            RdmaGetIntoBufferFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            RdmaGetIntoBufferFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            RdmaGetIntoBufferFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            RdmaGetIntoBufferFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            RdmaGetIntoBufferFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            RdmaGetIntoBufferFuture::UcxMt(f) => f.spawn(),
            #[cfg(feature = "enable-rofi-c")]
            RdmaGetIntoBufferFuture::RofiC(f) => f.spawn(),
            RdmaGetIntoBufferFuture::Shmem(f) => f.spawn(),
            RdmaGetIntoBufferFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for RdmaGetIntoBufferHandle<T, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            RdmaGetIntoBufferFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            RdmaGetIntoBufferFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            RdmaGetIntoBufferFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            RdmaGetIntoBufferFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            RdmaGetIntoBufferFutureProj::UcxMt(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-c")]
            RdmaGetIntoBufferFutureProj::RofiC(f) => f.poll(cx),
            RdmaGetIntoBufferFutureProj::Shmem(f) => f.poll(cx),
            RdmaGetIntoBufferFutureProj::Local(f) => f.poll(cx),
        }
    }
}

// Note that offsets and lengths are with respect to number of elements, not bytes
// the internal lamellae implementations will convert to bytes as needed
// e.g. offset of 1 for a type T of size 8 bytes means an offset of 8 bytes
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
    fn put_blocking<T: Remote>(&self, scheduler: &Arc<Scheduler>, src: T, pe: usize, offset: usize);
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
    fn blocking_get<T: Remote>(&self, scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T;
    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> RdmaGetBufferHandle<T>;
    fn blocking_get_buffer<T: Remote>(&self, scheduler: &Arc<Scheduler>, pe: usize, offset: usize, len: usize) -> Vec<T>;
    fn get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) -> RdmaGetIntoBufferHandle<T, B>;
    fn blocking_get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    );
    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    );
}

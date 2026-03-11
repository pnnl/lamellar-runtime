#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabric_lamellae::collective::{
    LibfabricCollectiveAllReduceFuture,
    LibfabricCollectiveAllReduceIntoBufferFuture,
    LibfabricCollectiveAllReduceInPlaceFuture,
    LibfabricCollectiveReduceFuture,
    LibfabricCollectiveReduceInPlaceFuture,
    LibfabricCollectiveReduceIntoBufferFuture,
    LibfabricCollectiveGatherFuture,
    LibfabricCollectiveGatherIntoBufferFuture,
    LibfabricCollectiveAllGatherFuture,
    LibfabricCollectiveAllGatherIntoBufferFuture,
    LibfabricCollectiveAllBroadcastFuture,
    LibfabricCollectiveAllBroadcastIntoBufferFuture,
    LibfabricCollectiveBroadcastFuture,
    LibfabricCollectiveBroadcastIntoBufferFuture,
    LibfabricCollectiveScatterFuture,
    LibfabricCollectiveScatterIntoBufferFuture,
    LibfabricCollectiveReduceScatterFuture,
    LibfabricCollectiveReduceScatterIntoBufferFuture
};

#[cfg(feature = "enable-ucx")]
use crate::lamellae::ucx_lamellae::collective::{
    UcxCollectiveAllBroadcastFuture,
    UcxCollectiveAllBroadcastIntoBufferFuture,
    UcxCollectiveAllGatherFuture,
    UcxCollectiveAllGatherIntoBufferFuture,
    UcxCollectiveAllReduceFuture,
    UcxCollectiveAllReduceInPlaceFuture,
    UcxCollectiveAllReduceIntoBufferFuture,
    UcxCollectiveBroadcastFuture,
    UcxCollectiveBroadcastIntoBufferFuture,
    UcxCollectiveGatherFuture,
    UcxCollectiveGatherIntoBufferFuture,
    UcxCollectiveReduceFuture,
    UcxCollectiveReduceInPlaceFuture,
    UcxCollectiveReduceIntoBufferFuture,
    UcxCollectiveReduceScatterFuture,
    UcxCollectiveReduceScatterIntoBufferFuture,
    UcxCollectiveScatterFuture,
    UcxCollectiveScatterIntoBufferFuture,
};

use crate::{
    active_messaging::AMCounters, memregion::MemregionRdmaInputInner, scheduler::Scheduler, AsLamellarBuffer, LamellarBuffer, LamellarTask, MemregionRdmaInput, Remote
};

use futures_util::Future;
use pin_project::pin_project;
use std::{
    pin::Pin, sync::Arc, task::{Context, Poll}
};

#[must_use = " CollectiveAllReduceOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveAllReduceOpHandle<T: Remote> {
    #[pin]
    pub(crate) future: CollectiveAllReduceOpFuture<T>,
}

#[pin_project(project = CollectiveAllReduceOpFutureProj)]
pub(crate) enum CollectiveAllReduceOpFuture<T: Remote> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveAllReduceFuture<T>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllReduceFuture<T>),
    // Shmem(#[pin] ShmemAtomicFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote> CollectiveAllReduceOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Vec<T> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            // #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceOpFuture::Ucx(f) => f.block(),
            // AtomicFetchOpFuture::Shmem(f) => f.block(),
            // AtomicFetchOpFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<Vec<T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceOpFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceOpFuture::Ucx(f) => f.spawn(),
            // AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            // AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote> Future for CollectiveAllReduceOpHandle<T> {
    type Output = Vec<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceOpFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFutureProj::LibfabricMt(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[must_use = " CollectiveAllReduceIntoBufferOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveAllReduceIntoBufferOpHandle<T: Remote, B: AsLamellarBuffer<T>> {
    #[pin]
    pub(crate) future: CollectiveAllReduceIntoBufferOpFuture<T, B>,
}

#[pin_project(project = CollectiveAllReduceIntoBufferOpFutureProj)]
pub(crate) enum CollectiveAllReduceIntoBufferOpFuture<T: Remote, B: AsLamellarBuffer<T>> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveAllReduceIntoBufferFuture<T, B>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllReduceIntoBufferFuture<T, B>),
    // Shmem(#[pin] ShmemAtomicFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveAllReduceIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceIntoBufferOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceIntoBufferOpFuture::Ucx(f) => f.block(),
            // AtomicFetchOpFuture::Shmem(f) => f.block(),
            // AtomicFetchOpFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceIntoBufferOpFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceIntoBufferOpFuture::Ucx(f) => f.spawn(),
            // AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            // AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for CollectiveAllReduceIntoBufferOpHandle<T, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceIntoBufferOpFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFutureProj::LibfabricMt(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[must_use = " CollectiveAllReduceInPlaceOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveAllReduceInPlaceOpHandle<T: Remote, B: AsLamellarBuffer<T>>  {
    #[pin]
    pub(crate) future: CollectiveAllReduceInPlaceOpFuture<T, B>,
}

#[pin_project(project = CollectiveAllReduceInPlaceOpFutureProj)]
pub(crate) enum CollectiveAllReduceInPlaceOpFuture<T: Remote, B: AsLamellarBuffer<T>>  {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveAllReduceInPlaceFuture<T, B>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllReduceInPlaceFuture<T, B>),
    // Shmem(#[pin] ShmemAtomicFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveAllReduceInPlaceOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceInPlaceOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceInPlaceOpFuture::Ucx(f) => f.block(),
            // AtomicFetchOpFuture::Shmem(f) => f.block(),
            // AtomicFetchOpFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceInPlaceOpFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceInPlaceOpFuture::Ucx(f) => f.spawn(),
            // AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            // AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for CollectiveAllReduceInPlaceOpHandle<T, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceInPlaceOpFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFutureProj::LibfabricMt(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceInPlaceOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}


#[must_use = " CollectiveReduceOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveReduceOpHandle<T: Remote> {
    #[pin]
    pub(crate) future: CollectiveReduceOpFuture<T>,
}


#[pin_project(project = CollectiveReduceOpFutureProj)]
pub(crate) enum CollectiveReduceOpFuture<T: Remote> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveReduceFuture<T>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveReduceFuture<T>),
    // Shmem(#[pin] ShmemAtomicFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote> CollectiveReduceOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Option<Vec<T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceOpFuture::Ucx(f) => f.block(),
            // AtomicFetchOpFuture::Shmem(f) => f.block(),
            // AtomicFetchOpFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<Option<Vec<T>>> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceOpFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceOpFuture::Ucx(f) => f.spawn(),
            // AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            // AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote> Future for CollectiveReduceOpHandle<T> {
    type Output = Option<Vec<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceOpFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFutureProj::LibfabricMt(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[must_use = " CollectiveReduceIntoBufferOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveReduceIntoBufferOpHandle<T: Remote, B: AsLamellarBuffer<T>> {
    #[pin]
    pub(crate) future: CollectiveReduceIntoBufferOpFuture<T, B>,
}


#[pin_project(project = CollectiveReduceIntoBufferOpFutureProj)]
pub(crate) enum CollectiveReduceIntoBufferOpFuture<T: Remote, B: AsLamellarBuffer<T>> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveReduceIntoBufferFuture<T, B>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveReduceIntoBufferFuture<T, B>),
    // Shmem(#[pin] ShmemAtomicFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveReduceIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self)  {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceIntoBufferOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceIntoBufferOpFuture::Ucx(f) => f.block(),
            // AtomicFetchOpFuture::Shmem(f) => f.block(),
            // AtomicFetchOpFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceIntoBufferOpFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceIntoBufferOpFuture::Ucx(f) => f.spawn(),
            // AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            // AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for CollectiveReduceIntoBufferOpHandle<T, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceIntoBufferOpFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFutureProj::LibfabricMt(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[must_use = " CollectiveReduceInPlaceOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveReduceInPlaceOpHandle<T> {
    #[pin]
    pub(crate) future: CollectiveReduceInPlaceOpFuture<T>,
}


#[pin_project(project = CollectiveReduceInPlaceOpFutureProj)]
pub(crate) enum CollectiveReduceInPlaceOpFuture<T> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveReduceInPlaceFuture<T>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveReduceInPlaceFuture<T>),
    // Shmem(#[pin] ShmemAtomicFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

// impl<T: Remote> CollectiveReduceInPlaceOpHandle<T> {
//     /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
//     pub fn block(self) {
//         match self.future {
//             #[cfg(feature = "enable-libfabric")]
//             CollectiveReduceInPlaceOpFuture::Libfabric(f) => f.block(),
//             // #[cfg(feature = "enable-libfabric")]
//             // AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
//             // #[cfg(feature = "enable-libfabric-async")]
//             // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
//             // #[cfg(feature = "enable-ucx")]
//             // AtomicFetchOpFuture::Ucx(f) => f.block(),
//             // AtomicFetchOpFuture::Shmem(f) => f.block(),
//             // AtomicFetchOpFuture::Local(f) => f.block(),
//         }
//     }

//     /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
//     /// initiating the remote operation.
//     ///
//     /// This function returns a handle that can be used to wait for the operation to complete
//     #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
//     pub fn spawn(self) -> LamellarTask<()> {
//         match self.future {
//             #[cfg(feature = "enable-libfabric")]
//             CollectiveReduceInPlaceOpFuture::Libfabric(f) => f.spawn(),
//             // #[cfg(feature = "enable-libfabric")]
//             // AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
//             // #[cfg(feature = "enable-libfabric-async")]
//             // AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
//             // #[cfg(feature = "enable-ucx")]
//             // AtomicFetchOpFuture::Ucx(f) => f.spawn(),
//             // AtomicFetchOpFuture::Shmem(f) => f.spawn(),
//             // AtomicFetchOpFuture::Local(f) => f.spawn(),
//         }
//     }
// }

impl<T: Remote> Future for CollectiveReduceInPlaceOpHandle<T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceInPlaceOpFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFutureProj::LibfabricMt(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceInPlaceOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}


#[must_use = " CollectiveAllGatherOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveAllGatherOpHandle<T: Remote> {
    #[pin]
    pub(crate) future: CollectiveAllGatherOpFuture<T>,
}

#[pin_project(project = CollectiveAllGatherOpFutureProj)]
pub(crate) enum CollectiveAllGatherOpFuture<T: Remote> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveAllGatherFuture<T>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllGatherFuture<T>),
    // Shmem(#[pin] ShmemAtomicFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote> CollectiveAllGatherOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Vec<T> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllGatherOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllGatherOpFuture::Ucx(f) => f.block(),
            // AtomicFetchOpFuture::Shmem(f) => f.block(),
            // AtomicFetchOpFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<Vec<T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllGatherOpFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllGatherOpFuture::Ucx(f) => f.spawn(),
            // AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            // AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote> Future for CollectiveAllGatherOpHandle<T> {
    type Output = Vec<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllGatherOpFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFutureProj::LibfabricMt(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllGatherOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[must_use = " CollectiveAllGatherIntoBufferOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveAllGatherIntoBufferOpHandle<T: Remote, B: AsLamellarBuffer<T>> {
    #[pin]
    pub(crate) future: CollectiveAllGatherIntoBufferOpFuture<T, B>,
}

#[pin_project(project = CollectiveAllGatherIntoBufferOpFutureProj)]
pub(crate) enum CollectiveAllGatherIntoBufferOpFuture<T: Remote, B: AsLamellarBuffer<T>> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveAllGatherIntoBufferFuture<T, B>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllGatherIntoBufferFuture<T, B>),
    // Shmem(#[pin] ShmemAtomicFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveAllGatherIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllGatherIntoBufferOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllGatherIntoBufferOpFuture::Ucx(f) => f.block(),
            // AtomicFetchOpFuture::Shmem(f) => f.block(),
            // AtomicFetchOpFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllGatherIntoBufferOpFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllGatherIntoBufferOpFuture::Ucx(f) => f.spawn(),
            // AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            // AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for CollectiveAllGatherIntoBufferOpHandle<T, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllGatherIntoBufferOpFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFutureProj::LibfabricMt(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllGatherIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[must_use = " CollectiveGatherOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveGatherOpHandle<T: Remote> {
    #[pin]
    pub(crate) future: CollectiveGatherOpFuture<T>,
}


#[pin_project(project = CollectiveGatherOpFutureProj)]
pub(crate) enum CollectiveGatherOpFuture<T: Remote> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveGatherFuture<T>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveGatherFuture<T>),
    // Shmem(#[pin] ShmemAtomicFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote> CollectiveGatherOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Option<Vec<T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveGatherOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveGatherOpFuture::Ucx(f) => f.block(),
            // AtomicFetchOpFuture::Shmem(f) => f.block(),
            // AtomicFetchOpFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<Option<Vec<T>>> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveGatherOpFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveGatherOpFuture::Ucx(f) => f.spawn(),
            // AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            // AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote> Future for CollectiveGatherOpHandle<T> {
    type Output = Option<Vec<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            CollectiveGatherOpFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFutureProj::LibfabricMt(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveGatherOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[must_use = " CollectiveGatherIntoBufferOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveGatherIntoBufferOpHandle<T: Remote, B: AsLamellarBuffer<T>> {
    #[pin]
    pub(crate) future: CollectiveGatherIntoBufferOpFuture<T, B>,
}


#[pin_project(project = CollectiveGatherIntoBufferOpFutureProj)]
pub(crate) enum CollectiveGatherIntoBufferOpFuture<T: Remote, B: AsLamellarBuffer<T>> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveGatherIntoBufferFuture<T, B>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveGatherIntoBufferFuture<T, B>),
    // Shmem(#[pin] ShmemAtomicFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveGatherIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self)  {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveGatherIntoBufferOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveGatherIntoBufferOpFuture::Ucx(f) => f.block(),
            // AtomicFetchOpFuture::Shmem(f) => f.block(),
            // AtomicFetchOpFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveGatherIntoBufferOpFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveGatherIntoBufferOpFuture::Ucx(f) => f.spawn(),
            // AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            // AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for CollectiveGatherIntoBufferOpHandle<T, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            CollectiveGatherIntoBufferOpFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFutureProj::LibfabricMt(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveGatherIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}


#[must_use = " CollectiveAllBroadcastOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveAllBroadcastOpHandle<T: Remote> {
    #[pin]
    pub(crate) future: CollectiveAllBroadcastOpFuture<T>,
}

#[pin_project(project = CollectiveAllBroadcastOpFutureProj)]
pub(crate) enum CollectiveAllBroadcastOpFuture<T: Remote> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveAllBroadcastFuture<T>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllBroadcastFuture<T>),
    // Shmem(#[pin] ShmemAtomicFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote> CollectiveAllBroadcastOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Vec<T> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllBroadcastOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllBroadcastOpFuture::Ucx(f) => f.block(),
            // AtomicFetchOpFuture::Shmem(f) => f.block(),
            // AtomicFetchOpFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<Vec<T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllBroadcastOpFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllBroadcastOpFuture::Ucx(f) => f.spawn(),
            // AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            // AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote> Future for CollectiveAllBroadcastOpHandle<T> {
    type Output = Vec<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllBroadcastOpFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFutureProj::LibfabricMt(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllBroadcastOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[must_use = " CollectiveAllBroadcastIntoBufferOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveAllBroadcastIntoBufferOpHandle<T: Remote, B: AsLamellarBuffer<T>> {
    #[pin]
    pub(crate) future: CollectiveAllBroadcastIntoBufferOpFuture<T, B>,
}

#[pin_project(project = CollectiveAllBroadcastIntoBufferOpFutureProj)]
pub(crate) enum CollectiveAllBroadcastIntoBufferOpFuture<T: Remote, B: AsLamellarBuffer<T>> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveAllBroadcastIntoBufferFuture<T, B>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllBroadcastIntoBufferFuture<T, B>),
    // Shmem(#[pin] ShmemAtomicFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveAllBroadcastIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllBroadcastIntoBufferOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllBroadcastIntoBufferOpFuture::Ucx(f) => f.block(),
            // AtomicFetchOpFuture::Shmem(f) => f.block(),
            // AtomicFetchOpFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllBroadcastIntoBufferOpFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllBroadcastIntoBufferOpFuture::Ucx(f) => f.spawn(),
            // AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            // AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for CollectiveAllBroadcastIntoBufferOpHandle<T, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllBroadcastIntoBufferOpFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFutureProj::LibfabricMt(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllBroadcastIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[must_use = " CollectiveBroadcastOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveBroadcastOpHandle<T: Remote> {
    #[pin]
    pub(crate) future: CollectiveBroadcastOpFuture<T>,
}

#[pin_project(project = CollectiveBroadcastOpFutureProj)]
pub(crate) enum CollectiveBroadcastOpFuture<T: Remote> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveBroadcastFuture<T>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveBroadcastFuture<T>),
    // Shmem(#[pin] ShmemAtomicFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote> CollectiveBroadcastOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Option<Vec<T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveBroadcastOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveBroadcastOpFuture::Ucx(f) => f.block(),
            // AtomicFetchOpFuture::Shmem(f) => f.block(),
            // AtomicFetchOpFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<Option<Vec<T>>> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveBroadcastOpFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveBroadcastOpFuture::Ucx(f) => f.spawn(),
            // AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            // AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote> Future for CollectiveBroadcastOpHandle<T> {
    type Output = Option<Vec<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            CollectiveBroadcastOpFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFutureProj::LibfabricMt(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveBroadcastOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[must_use = " CollectiveBroadcastIntoBufferOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveBroadcastIntoBufferOpHandle<T: Remote, B: AsLamellarBuffer<T>> {
    #[pin]
    pub(crate) future: CollectiveBroadcastIntoBufferOpFuture<T, B>,
}

#[pin_project(project = CollectiveBroadcastIntoBufferOpFutureProj)]
pub(crate) enum CollectiveBroadcastIntoBufferOpFuture<T: Remote, B: AsLamellarBuffer<T>> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveBroadcastIntoBufferFuture<T, B>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveBroadcastIntoBufferFuture<T, B>),
    // Shmem(#[pin] ShmemAtomicFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveBroadcastIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveBroadcastIntoBufferOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveBroadcastIntoBufferOpFuture::Ucx(f) => f.block(),
            // AtomicFetchOpFuture::Shmem(f) => f.block(),
            // AtomicFetchOpFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveBroadcastIntoBufferOpFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveBroadcastIntoBufferOpFuture::Ucx(f) => f.spawn(),
            // AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            // AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for CollectiveBroadcastIntoBufferOpHandle<T, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            CollectiveBroadcastIntoBufferOpFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFutureProj::LibfabricMt(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveBroadcastIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}


#[must_use = " CollectiveScatterOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveScatterOpHandle<T: Remote> {
    #[pin]
    pub(crate) future: CollectiveScatterOpFuture<T>,
}

#[pin_project(project = CollectiveScatterOpFutureProj)]
pub(crate) enum CollectiveScatterOpFuture<T: Remote> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveScatterFuture<T>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveScatterFuture<T>),
    // Shmem(#[pin] ShmemAtomicFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote> CollectiveScatterOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Vec<T> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveScatterOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveScatterOpFuture::Ucx(f) => f.block(),
            // AtomicFetchOpFuture::Shmem(f) => f.block(),
            // AtomicFetchOpFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<Vec<T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveScatterOpFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveScatterOpFuture::Ucx(f) => f.spawn(),
            // AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            // AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote> Future for CollectiveScatterOpHandle<T> {
    type Output = Vec<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            CollectiveScatterOpFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFutureProj::LibfabricMt(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveScatterOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[must_use = " CollectiveScatterIntoBufferOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveScatterIntoBufferOpHandle<T: Remote, B: AsLamellarBuffer<T>> {
    #[pin]
    pub(crate) future: CollectiveScatterIntoBufferOpFuture<T, B>,
}

#[pin_project(project = CollectiveScatterIntoBufferOpFutureProj)]
pub(crate) enum CollectiveScatterIntoBufferOpFuture<T: Remote, B: AsLamellarBuffer<T>> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveScatterIntoBufferFuture<T, B>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveScatterIntoBufferFuture<T, B>),
    // Shmem(#[pin] ShmemAtomicFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveScatterIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveScatterIntoBufferOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveScatterIntoBufferOpFuture::Ucx(f) => f.block(),
            // AtomicFetchOpFuture::Shmem(f) => f.block(),
            // AtomicFetchOpFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveScatterIntoBufferOpFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveScatterIntoBufferOpFuture::Ucx(f) => f.spawn(),
            // AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            // AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for CollectiveScatterIntoBufferOpHandle<T, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            CollectiveScatterIntoBufferOpFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFutureProj::LibfabricMt(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveScatterIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}


#[must_use = " CollectiveReduceScatterOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveReduceScatterOpHandle<T: Remote> {
    #[pin]
    pub(crate) future: CollectiveReduceScatterOpFuture<T>,
}

#[pin_project(project = CollectiveReduceScatterOpFutureProj)]
pub(crate) enum CollectiveReduceScatterOpFuture<T: Remote> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveReduceScatterFuture<T>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveReduceScatterFuture<T>),
    // Shmem(#[pin] ShmemAtomicFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote> CollectiveReduceScatterOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Vec<T> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceScatterOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceScatterOpFuture::Ucx(f) => f.block(),
            // AtomicFetchOpFuture::Shmem(f) => f.block(),
            // AtomicFetchOpFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<Vec<T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceScatterOpFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceScatterOpFuture::Ucx(f) => f.spawn(),
            // AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            // AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote> Future for CollectiveReduceScatterOpHandle<T> {
    type Output = Vec<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceScatterOpFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFutureProj::LibfabricMt(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceScatterOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[must_use = " CollectiveReduceScatterIntoBufferOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveReduceScatterIntoBufferOpHandle<T: Remote, B: AsLamellarBuffer<T>> {
    #[pin]
    pub(crate) future: CollectiveReduceScatterIntoBufferOpFuture<T, B>,
}

#[pin_project(project = CollectiveReduceScatterIntoBufferOpFutureProj)]
pub(crate) enum CollectiveReduceScatterIntoBufferOpFuture<T: Remote, B: AsLamellarBuffer<T>> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveReduceScatterIntoBufferFuture<T, B>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveReduceScatterIntoBufferFuture<T, B>),
    // Shmem(#[pin] ShmemAtomicFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceScatterIntoBufferOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceScatterIntoBufferOpFuture::Ucx(f) => f.block(),
            // AtomicFetchOpFuture::Shmem(f) => f.block(),
            // AtomicFetchOpFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceScatterIntoBufferOpFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceScatterIntoBufferOpFuture::Ucx(f) => f.spawn(),
            // AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            // AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for CollectiveReduceScatterIntoBufferOpHandle<T, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceScatterIntoBufferOpFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFutureProj::LibfabricMt(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceScatterIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}



#[derive(Clone)]
pub(crate) enum ReduceOp {
    Min,
    Max,
    Sum,
    Prod,
    BitOr,
    BitXor,
    BitAnd,
}

pub(crate) type AllReduceOp = ReduceOp;

pub(crate) enum RootOrBuffer<T> {
    Root(Vec<T>), 
    NotRoot(usize) 
}


pub enum BroadcastInput<T:Remote> {
    Root(MemregionRdmaInput<T>),
    NotRoot(usize, usize)
}

pub(crate) enum BroadcastInputInner<T:Remote> {
    Root(MemregionRdmaInputInner<T>),
    NotRoot(usize, usize)
}


impl<T: Remote> From<BroadcastInput<T>> for BroadcastInputInner<T> {
    fn from(value: BroadcastInput<T>) -> Self {
        match value {
            BroadcastInput::Root(memregion_rdma_input) => BroadcastInputInner::Root(memregion_rdma_input.into()),
            BroadcastInput::NotRoot(len,  root_pe) => BroadcastInputInner::NotRoot(len, root_pe),
        }
    }
}

impl<T: Remote> BroadcastInput<T> {
    pub fn root(src: impl Into<MemregionRdmaInput<T>>) -> Self {
        Self::Root(src.into())
    }

    pub fn not_root(len: usize, root_pe: usize) -> Self {
        Self::NotRoot(len, root_pe)
    }
}

pub(crate) enum RootSrcOrBuffer<T: Remote> {
    Root(MemregionRdmaInputInner<T>), 
    NotRoot(Vec<T>, usize) 
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<RootSrcOrLamellarBuffer<T, B>> for RootSrcOrLamellarBufferInner<T, B> {
    fn from(value: RootSrcOrLamellarBuffer<T, B>) -> Self {
        match value {
            RootSrcOrLamellarBuffer::Root(memregion_rdma_input) => RootSrcOrLamellarBufferInner::Root(memregion_rdma_input.into()),
            RootSrcOrLamellarBuffer::NotRoot(lamellar_buffer, root_pe) => RootSrcOrLamellarBufferInner::NotRoot(lamellar_buffer, root_pe),
        }
    }
}

pub enum RootSrcOrLamellarBufferInner<T: Remote, B: AsLamellarBuffer<T>> {
    Root(MemregionRdmaInputInner<T>), 
    NotRoot(LamellarBuffer<T, B>, usize) 
}

pub enum RootSrcOrLamellarBuffer<T: Remote, B: AsLamellarBuffer<T>> {
    Root(MemregionRdmaInput<T>), 
    NotRoot(LamellarBuffer<T, B>, usize) 
}

pub enum ScatterInput<T: Remote> {
    Root(MemregionRdmaInput<T>, usize),
    NotRoot(usize, usize)
}


impl<T: Remote> ScatterInput<T> {
    pub fn root(src: impl Into<MemregionRdmaInput<T>>, chunk_size: usize) -> Self {
        Self::Root(src.into(), chunk_size)
    }

    pub fn not_root(len: usize, root_pe: usize) -> Self {
        Self::NotRoot(len, root_pe)
    }
}

pub(crate) enum ScatterInputInner<T: Remote> {
    Root(MemregionRdmaInputInner<T>),
    NotRoot(usize)
}


impl<T: Remote> From<ScatterInput<T>> for ScatterInputInner<T> {
    fn from(value: ScatterInput<T>) -> Self {
        match value {
            ScatterInput::Root(memregion_rdma_input, _) => ScatterInputInner::Root(memregion_rdma_input.into()),
            ScatterInput::NotRoot(len,  root_pe) => ScatterInputInner::NotRoot(root_pe),
        }
    }
}


pub(crate) enum RootSrcSliceOrNone<'a, T> {
    Root(&'a [T]), 
    NotRoot(usize) 
}


pub(crate) enum RootSrcOrSliceMut<'a, T> {
    Root(&'a [T]), 
    NotRoot(&'a mut [T], usize) 
}

impl<T: Remote>  RootSrcOrBuffer<T> {
    pub(crate) fn as_mut_slice<'a>(&'a mut self) -> RootSrcOrSliceMut<'a, T> {
        match self {
            RootSrcOrBuffer::Root(memregion_in) => RootSrcOrSliceMut::Root(memregion_in.as_slice()),
            RootSrcOrBuffer::NotRoot(vec, pe) => RootSrcOrSliceMut::NotRoot(vec, *pe)
        }
    }
}

impl<T: Remote>  ScatterInputInner<T> {
    pub(crate) fn as_slice<'a>(&'a self) -> RootSrcSliceOrNone<'a, T> {
        match self {
            ScatterInputInner::Root(memregion_in) => RootSrcSliceOrNone::Root(memregion_in.as_slice()),
            ScatterInputInner::NotRoot(pe) => RootSrcSliceOrNone::NotRoot(*pe)
        }
    }
}



impl<T> RootOrBuffer<T> {
    pub(crate) fn as_mut_slice<'a>(&'a mut self) -> RootOrSliceMut<'a, T> {
        match self {
            RootOrBuffer::Root(items) => RootOrSliceMut::Root(items),
            RootOrBuffer::NotRoot(pe) => RootOrSliceMut::NotRoot(*pe),
        }
    }
}


pub enum RootOrLamellarBuffer<T: Remote, B: AsLamellarBuffer<T>> {
    Root(LamellarBuffer<T, B>),
    NotRoot(usize),
}




impl<T: Remote, B: AsLamellarBuffer<T>> RootOrLamellarBuffer<T, B> {
    pub(crate) fn as_mut_slice<'a>(&'a mut self) -> RootOrSliceMut<'a, T> {
        match self {
            RootOrLamellarBuffer::Root(lamellar_buffer) => RootOrSliceMut::Root(lamellar_buffer.as_mut_slice()),
            RootOrLamellarBuffer::NotRoot(pe) => RootOrSliceMut::NotRoot(*pe),
        }
    }
} 

impl<T: Remote, B: AsLamellarBuffer<T>> RootSrcOrLamellarBufferInner<T, B> {
    pub(crate) fn as_mut_slice<'a>(&'a mut self) -> RootSrcOrSliceMut<'a, T> {
        match self {
            RootSrcOrLamellarBufferInner::Root(memregion_in) => RootSrcOrSliceMut::Root(memregion_in.as_slice()),
            RootSrcOrLamellarBufferInner::NotRoot(lamellar_buffer, pe) => RootSrcOrSliceMut::NotRoot(lamellar_buffer.as_mut_slice(), *pe),
        }
    }
} 

pub(crate) enum RootOrSliceMut<'a, T> {
    Root(&'a mut [T]), 
    NotRoot(usize) 
}

impl std::fmt::Debug for ReduceOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReduceOp::Min => write!(f, "Min"),
            ReduceOp::Max => write!(f, "Max"),
            ReduceOp::Sum => write!(f, "Sum"),
            ReduceOp::Prod => write!(f, "Prod"),
            ReduceOp::BitOr => write!(f, "BitOr"),
            ReduceOp::BitXor => write!(f, "BitXor"),
            ReduceOp::BitAnd => write!(f, "BitAnd"),
        }
    }
}

pub(crate) trait CommAllocCollectiveAllReduce {
    fn reduce_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        op: ReduceOp,
    ) -> CollectiveAllReduceOpHandle<T>;
    
    fn reduce_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        op: ReduceOp,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceIntoBufferOpHandle<T, B>;

    fn reduce_all_in_place<T: Remote, B: AsLamellarBuffer<T>>(
        &self, // TODO: This should probably take a multiple reference to self.
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src_and_dst: LamellarBuffer<T, B>,
        op: ReduceOp,
    ) -> CollectiveAllReduceInPlaceOpHandle<T, B>;
}

pub(crate) trait CommAllocCollectiveReduce {
    fn reduce<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        src: impl Into<MemregionRdmaInputInner<T>>,
        root_pe: usize,
    ) -> CollectiveReduceOpHandle<T>;
    
    fn reduce_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        src: impl Into<MemregionRdmaInputInner<T>>,
        root_or_buffer: RootOrLamellarBuffer<T, B>
    ) -> CollectiveReduceIntoBufferOpHandle<T, B>;
    
    // fn reduce_in_place<T: Remote>(
    //     &self,
    //     scheduler: &Arc<Scheduler>,
    //     counters: Vec<Arc<AMCounters>>,
    //     op: ReduceOp,
    //     root_pe: usize,
    // ) -> CollectiveReduceInPlaceOpHandle<T>;
}

pub(crate) trait CommAllocCollectiveAllGather {
    fn gather_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
    ) -> CollectiveAllGatherOpHandle<T>;
    fn gather_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllGatherIntoBufferOpHandle<T, B>;
}

pub(crate) trait CommAllocCollectiveGather {
    fn gather<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        root_pe: usize,
    ) -> CollectiveGatherOpHandle<T>;
    fn gather_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        root_or_buffer: RootOrLamellarBuffer<T, B>
    ) -> CollectiveGatherIntoBufferOpHandle<T, B>;
}

pub(crate) trait CommAllocCollectiveAllBroadcast {
    fn broadcast_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
    ) -> CollectiveAllBroadcastOpHandle<T>;
    fn broadcast_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllBroadcastIntoBufferOpHandle<T, B>;
}

pub(crate) trait CommAllocCollectiveBroadcast {
    fn broadcast<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src_or_root_pe: BroadcastInput<T>,
    ) -> CollectiveBroadcastOpHandle<T>;
    fn broadcast_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        dst: RootSrcOrLamellarBuffer<T, B>,
    ) -> CollectiveBroadcastIntoBufferOpHandle<T, B>;
}

pub(crate) trait CommAllocCollectiveScatter {
    fn scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src_or_root_pe: ScatterInput<T>,
    ) -> CollectiveScatterOpHandle<T>;
    fn scatter_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        dst: LamellarBuffer<T, B>,
        src_or_root_pe: ScatterInput<T>,
    ) -> CollectiveScatterIntoBufferOpHandle<T, B>;
}

pub(crate) trait CommAllocCollectiveReduceScatter {
    fn reduce_scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        src: impl Into<MemregionRdmaInputInner<T>>,
        len: usize,
    ) -> CollectiveReduceScatterOpHandle<T>;
    
    fn reduce_scatter_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        src: impl Into<MemregionRdmaInputInner<T>>,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveReduceScatterIntoBufferOpHandle<T, B>;

    // fn reduce_scatter_in_place<T: Remote, B: AsLamellarBuffer<T>>(
    //     &self, // TODO: This should probably take a multiple reference to self.
    //     scheduler: &Arc<Scheduler>,
    //     counters: Vec<Arc<AMCounters>>,
    //     src_and_dst: LamellarBuffer<T, B>,
    //     op: ReduceOp,
    // ) -> CollectiveAllReduceInPlaceOpHandle<T, B>;
}
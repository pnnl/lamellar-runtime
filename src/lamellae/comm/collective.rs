use crate::{AsLamellarBuffer, LamellarBuffer, LamellarTask, Remote, active_messaging::AMCounters, scheduler::Scheduler};
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
    LibfabricCollectiveAllToAllFuture,
    LibfabricCollectiveAllToAllIntoBufferFuture,
    LibfabricCollectiveBroadcastFuture,
    LibfabricCollectiveBroadcastIntoBufferFuture,
    LibfabricCollectiveScatterFuture,
    LibfabricCollectiveScatterIntoBufferFuture,
    LibfabricCollectiveReduceScatterFuture,
    LibfabricCollectiveReduceScatterIntoBufferFuture
};
#[cfg(feature = "enable-libfabric-mt")]
use crate::lamellae::libfabric_lamellae_mt::collective::{
    LibfabricMtCollectiveAllReduceFuture,
    LibfabricMtCollectiveAllReduceIntoBufferFuture,
    LibfabricMtCollectiveAllReduceInPlaceFuture,
    LibfabricMtCollectiveReduceFuture,
    LibfabricMtCollectiveReduceInPlaceFuture,
    LibfabricMtCollectiveReduceIntoBufferFuture,
    LibfabricMtCollectiveGatherFuture,
    LibfabricMtCollectiveGatherIntoBufferFuture,
    LibfabricMtCollectiveAllGatherFuture,
    LibfabricMtCollectiveAllGatherIntoBufferFuture,
    LibfabricMtCollectiveAllToAllFuture,
    LibfabricMtCollectiveAllToAllIntoBufferFuture,
    LibfabricMtCollectiveBroadcastFuture,
    LibfabricMtCollectiveBroadcastIntoBufferFuture,
    LibfabricMtCollectiveScatterFuture,
    LibfabricMtCollectiveScatterIntoBufferFuture,
    LibfabricMtCollectiveReduceScatterFuture,
    LibfabricMtCollectiveReduceScatterIntoBufferFuture
};

#[cfg(feature = "enable-libfabric-async")]
use crate::lamellae::libfabric_async_lamellae::collective::{
    LibfabricAsyncCollectiveAllReduceFuture,
    LibfabricAsyncCollectiveAllReduceIntoBufferFuture,
    LibfabricAsyncCollectiveAllReduceInPlaceFuture,
    LibfabricAsyncCollectiveReduceFuture,
    LibfabricAsyncCollectiveReduceInPlaceFuture,
    LibfabricAsyncCollectiveReduceIntoBufferFuture,
    LibfabricAsyncCollectiveGatherFuture,
    LibfabricAsyncCollectiveGatherIntoBufferFuture,
    LibfabricAsyncCollectiveAllGatherFuture,
    LibfabricAsyncCollectiveAllGatherIntoBufferFuture,
    LibfabricAsyncCollectiveAllToAllFuture,
    LibfabricAsyncCollectiveAllToAllIntoBufferFuture,
    LibfabricAsyncCollectiveBroadcastFuture,
    LibfabricAsyncCollectiveBroadcastIntoBufferFuture,
    LibfabricAsyncCollectiveScatterFuture,
    LibfabricAsyncCollectiveScatterIntoBufferFuture,
    LibfabricAsyncCollectiveReduceScatterFuture,
    LibfabricAsyncCollectiveReduceScatterIntoBufferFuture
};

#[cfg(feature = "enable-ucx")]
use crate::lamellae::ucx_lamellae::collective::{
    UcxCollectiveAllToAllFuture,
    UcxCollectiveAllToAllIntoBufferFuture,
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

#[cfg(feature = "enable-ucx-mt")]
use crate::lamellae::ucx_lamellae_mt::collective::{
    UcxMtCollectiveAllToAllFuture,
    UcxMtCollectiveAllToAllIntoBufferFuture,
    UcxMtCollectiveAllGatherFuture,
    UcxMtCollectiveAllGatherIntoBufferFuture,
    UcxMtCollectiveAllReduceFuture,
    UcxMtCollectiveAllReduceInPlaceFuture,
    UcxMtCollectiveAllReduceIntoBufferFuture,
    UcxMtCollectiveBroadcastFuture,
    UcxMtCollectiveBroadcastIntoBufferFuture,
    UcxMtCollectiveGatherFuture,
    UcxMtCollectiveGatherIntoBufferFuture,
    UcxMtCollectiveReduceFuture,
    UcxMtCollectiveReduceInPlaceFuture,
    UcxMtCollectiveReduceIntoBufferFuture,
    UcxMtCollectiveReduceScatterFuture,
    UcxMtCollectiveReduceScatterIntoBufferFuture,
    UcxMtCollectiveScatterFuture,
    UcxMtCollectiveScatterIntoBufferFuture,
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
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtCollectiveAllReduceFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveAllReduceFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllReduceFuture<T>),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtCollectiveAllReduceFuture<T>),
    // Shmem(#[pin] ShmemCollectiveAllReduceFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote> CollectiveAllReduceOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Vec<T> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllReduceOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllReduceOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllReduceOpFuture::UcxMt(f) => f.block(),
            // CollectiveAllReduceOpFuture::Shmem(f) => f.block(),
            // CollectiveAllReduceOpFuture::Local(f) => f.block(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllReduceOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllReduceOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllReduceOpFuture::UcxMt(f) => f.spawn(),
            // CollectiveAllReduceOpFuture::Shmem(f) => f.spawn(),
            // CollectiveAllReduceOpFuture::Local(f) => f.spawn(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllReduceOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllReduceOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllReduceOpFutureProj::UcxMt(f) => f.poll(cx),
            // CollectiveAllReduceOpFutureProj::Shmem(f) => f.poll(cx),
            // CollectiveAllReduceOpFutureProj::Local(f) => f.poll(cx),
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
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtCollectiveAllReduceIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveAllReduceIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllReduceIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtCollectiveAllReduceIntoBufferFuture<T, B>),
    // Shmem(#[pin] ShmemCollectiveAllReduceIntoBufferFuture<T, B>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveAllReduceIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceIntoBufferOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllReduceIntoBufferOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllReduceIntoBufferOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceIntoBufferOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllReduceIntoBufferOpFuture::UcxMt(f) => f.block(),
            // CollectiveAllReduceIntoBufferOpFuture::Shmem(f) => f.block(),
            // CollectiveAllReduceIntoBufferOpFuture::Local(f) => f.block(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllReduceIntoBufferOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllReduceIntoBufferOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceIntoBufferOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllReduceIntoBufferOpFuture::UcxMt(f) => f.spawn(),
            // CollectiveAllReduceIntoBufferOpFuture::Shmem(f) => f.spawn(),
            // CollectiveAllReduceIntoBufferOpFuture::Local(f) => f.spawn(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllReduceIntoBufferOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllReduceIntoBufferOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllReduceIntoBufferOpFutureProj::UcxMt(f) => f.poll(cx),
            // CollectiveAllReduceIntoBufferOpFutureProj::Shmem(f) => f.poll(cx),
            // CollectiveAllReduceIntoBufferOpFutureProj::Local(f) => f.poll(cx),
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
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtCollectiveAllReduceInPlaceFuture<T, B>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveAllReduceInPlaceFuture<T, B>), // we can reuse the IntoBuffer future for the InPlace operation since the buffer is provided by the caller in both cases    
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllReduceInPlaceFuture<T, B>),  
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtCollectiveAllReduceInPlaceFuture<T, B>),  
    // Shmem(#[pin] ShmemCollectiveAllReduceInPlaceFuture<T, B>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveAllReduceInPlaceOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceInPlaceOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllReduceInPlaceOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllReduceInPlaceOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceInPlaceOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllReduceInPlaceOpFuture::UcxMt(f) => f.block(),
            // CollectiveAllReduceInPlaceOpFuture::Shmem(f) => f.block(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllReduceInPlaceOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllReduceInPlaceOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceInPlaceOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllReduceInPlaceOpFuture::UcxMt(f) => f.spawn(),
            // CollectiveAllReduceInPlaceOpFuture::Shmem(f) => f.spawn(),
            // CollectiveAllReduceInPlaceOpFuture::Local(f) => f.spawn(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllReduceInPlaceOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllReduceInPlaceOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceInPlaceOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllReduceInPlaceOpFutureProj::UcxMt(f) => f.poll(cx),
            // CollectiveAllReduceInPlaceOpFutureProj::Shmem(f) => f.poll(cx),
            // CollectiveAllReduceInPlaceOpFutureProj::Local(f) => f.poll(cx),
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
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtCollectiveReduceFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveReduceFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveReduceFuture<T>),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtCollectiveReduceFuture<T>),
    // Shmem(#[pin] ShmemCollectiveReduceFuture<T>),
    // Local(#[pin] LocalCollectiveReduceFuture<T>),
}

impl<T: Remote> CollectiveReduceOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Option<Vec<T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveReduceOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveReduceOpFuture::UcxMt(f) => f.block(),
            // CollectiveReduceOpFuture::Shmem(f) => f.block(),
            // CollectiveReduceOpFuture::Local(f) => f.block(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveReduceOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveReduceOpFuture::UcxMt(f) => f.spawn(),
            // CollectiveReduceOpFuture::Shmem(f) => f.spawn(),
            // CollectiveReduceOpFuture::Local(f) => f.spawn(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveReduceOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveReduceOpFutureProj::UcxMt(f) => f.poll(cx),
            // CollectiveReduceOpFutureProj::Shmem(f) => f.poll(cx),
            // CollectiveReduceOpFutureProj::Local(f) => f.poll(cx),
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
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtCollectiveReduceIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveReduceIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveReduceIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtCollectiveReduceIntoBufferFuture<T, B>),
    // Shmem(#[pin] ShmemCollectiveReduceIntoBufferFuture<T, B>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveReduceIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self)  {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceIntoBufferOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveReduceIntoBufferOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceIntoBufferOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceIntoBufferOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveReduceIntoBufferOpFuture::UcxMt(f) => f.block(),
            // CollectiveReduceIntoBufferOpFuture::Shmem(f) => f.block(),
            // CollectiveReduceIntoBufferOpFuture::Local(f) => f.block(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveReduceIntoBufferOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceIntoBufferOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceIntoBufferOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveReduceIntoBufferOpFuture::UcxMt(f) => f.spawn(),
            // CollectiveReduceIntoBufferOpFuture::Shmem(f) => f.spawn(),
            // CollectiveReduceIntoBufferOpFuture::Local(f) => f.spawn(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveReduceIntoBufferOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceIntoBufferOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveReduceIntoBufferOpFutureProj::UcxMt(f) => f.poll(cx),
            // CollectiveReduceIntoBufferOpFutureProj::Shmem(f) => f.poll(cx),
            // CollectiveReduceIntoBufferOpFutureProj::Local(f) => f.poll(cx),
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
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtCollectiveReduceInPlaceFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveReduceInPlaceFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveReduceInPlaceFuture<T>),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtCollectiveReduceInPlaceFuture<T>),
    // Shmem(#[pin] ShmemCollectiveReduceInPlaceFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

// impl<T: Remote> CollectiveReduceInPlaceOpHandle<T> {
//     /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
//     pub fn block(self) {
//         match self.future {
//             #[cfg(feature = "enable-libfabric")]
//             CollectiveReduceInPlaceOpFuture::Libfabric(f) => f.block(),
//             #[cfg(feature = "enable-libfabric")]
//             CollectiveReduceInPlaceOpFuture::LibfabricMt(f) => f.block(),
//             #[cfg(feature = "enable-libfabric-async")]
//             CollectiveReduceInPlaceOpFuture::LibfabricAsync(f) => f.block(),
//             #[cfg(feature = "enable-ucx")]
//             CollectiveReduceInPlaceOpFuture::Ucx(f) => f.block(),
//             // CollectiveReduceInPlaceOpFuture::Shmem(f) => f.block(),
//             // CollectiveReduceInPlaceOpFuture::Local(f) => f.block(),
//         }
//     }

//     /// This method will spawn the associated (raw) CollectiveReduceInPlaceOp Operation on the work queue,
//     /// initiating the remote operation.
//     ///
//     /// This function returns a handle that can be used to wait for the operation to complete
//     #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
//     pub fn spawn(self) -> LamellarTask<()> {
//         match self.future {
//             #[cfg(feature = "enable-libfabric")]
//             CollectiveReduceInPlaceOpFuture::Libfabric(f) => f.spawn(),
//             #[cfg(feature = "enable-libfabric")]
//             CollectiveReduceInPlaceOpFuture::LibfabricMt(f) => f.spawn(),
//             #[cfg(feature = "enable-libfabric-async")]
//             CollectiveReduceInPlaceOpFuture::LibfabricAsync(f) => f.spawn(),
//             #[cfg(feature = "enable-ucx")]
//             CollectiveReduceInPlaceOpFuture::Ucx(f) => f.spawn(),
//             // CollectiveReduceInPlaceOpFuture::Shmem(f) => f.spawn(),
//             // CollectiveReduceInPlaceOpFuture::Local(f) => f.spawn(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveReduceInPlaceOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceInPlaceOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceInPlaceOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveReduceInPlaceOpFutureProj::UcxMt(f) => f.poll(cx),
            // CollectiveReduceInPlaceOpFutureProj::Shmem(f) => f.poll(cx),
            // CollectiveReduceInPlaceOpFutureProj::Local(f) => f.poll(cx),
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
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtCollectiveAllGatherFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveAllGatherFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllGatherFuture<T>),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtCollectiveAllGatherFuture<T>),
    //Shmem(#[pin] ShmemCollectiveAllGatherFuture<T>),    
    // Local(#[pin] LocalCollectiveAllGatherFuture<T>),
}

impl<T: Remote> CollectiveAllGatherOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Vec<T> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllGatherOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllGatherOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllGatherOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllGatherOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllGatherOpFuture::UcxMt(f) => f.block(),
            // CollectiveAllGatherOpFuture::Shmem(f) => f.block(),
            // CollectiveAllGatherOpFuture::Local(f) => f.block(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllGatherOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllGatherOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllGatherOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllGatherOpFuture::UcxMt(f) => f.spawn(),
            // CollectiveAllGatherOpFuture::Shmem(f) => f.spawn(),
            // CollectiveAllGatherOpFuture::Local(f) => f.spawn(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllGatherOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllGatherOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllGatherOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllGatherOpFutureProj::UcxMt(f) => f.poll(cx),
            // CollectiveAllGatherOpFutureProj::Shmem(f) => f.poll(cx),
            // CollectiveAllGatherOpFutureProj::Local(f) => f.poll(cx),
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
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtCollectiveAllGatherIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveAllGatherIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllGatherIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtCollectiveAllGatherIntoBufferFuture<T, B>),
    // Shmem(#[pin] ShmemCollectiveAllGatherIntoBufferFuture<T, B>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveAllGatherIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllGatherIntoBufferOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllGatherIntoBufferOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllGatherIntoBufferOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllGatherIntoBufferOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllGatherIntoBufferOpFuture::UcxMt(f) => f.block(),
            // CollectiveAllGatherIntoBufferOpFuture::Shmem(f) => f.block(),
            // CollectiveAllGatherIntoBufferOpFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) CollectiveAllGatherIntoBufferOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllGatherIntoBufferOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllGatherIntoBufferOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllGatherIntoBufferOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllGatherIntoBufferOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllGatherIntoBufferOpFuture::UcxMt(f) => f.spawn(),
            // CollectiveAllGatherIntoBufferOpFuture::Shmem(f) => f.spawn(),
            // CollectiveAllGatherIntoBufferOpFuture::Local(f) => f.spawn(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllGatherIntoBufferOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllGatherIntoBufferOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllGatherIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllGatherIntoBufferOpFutureProj::UcxMt(f) => f.poll(cx),
            // CollectiveAllGatherIntoBufferOpFutureProj::Shmem(f) => f.poll(cx),
            // CollectiveAllGatherIntoBufferOpFutureProj::Local(f) => f.poll(cx),
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
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtCollectiveGatherFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveGatherFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveGatherFuture<T>),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtCollectiveGatherFuture<T>),
    // Shmem(#[pin] ShmemCollectiveGatherFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote> CollectiveGatherOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Option<Vec<T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveGatherOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveGatherOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveGatherOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveGatherOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveGatherOpFuture::UcxMt(f) => f.block(),
            // CollectiveGatherOpFuture::Shmem(f) => f.block(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveGatherOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveGatherOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveGatherOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveGatherOpFuture::UcxMt(f) => f.spawn(),
            // CollectiveGatherOpFuture::Shmem(f) => f.spawn(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveGatherOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveGatherOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveGatherOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveGatherOpFutureProj::UcxMt(f) => f.poll(cx),
            // CollectiveGatherOpFutureProj::Shmem(f) => f.poll(cx),
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
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtCollectiveGatherIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveGatherIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveGatherIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtCollectiveGatherIntoBufferFuture<T, B>),
    // Shmem(#[pin] ShmemCollectiveGatherIntoBufferFuture<T, B>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveGatherIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self)  {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveGatherIntoBufferOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveGatherIntoBufferOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveGatherIntoBufferOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveGatherIntoBufferOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveGatherIntoBufferOpFuture::UcxMt(f) => f.block(),
            // CollectiveGatherIntoBufferOpFuture::Shmem(f) => f.block(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveGatherIntoBufferOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveGatherIntoBufferOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveGatherIntoBufferOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveGatherIntoBufferOpFuture::UcxMt(f) => f.spawn(),
            // CollectiveGatherIntoBufferOpFuture::Shmem(f) => f.spawn(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveGatherIntoBufferOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveGatherIntoBufferOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveGatherIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveGatherIntoBufferOpFutureProj::UcxMt(f) => f.poll(cx),
            // CollectiveGatherIntoBufferOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}


#[must_use = " CollectiveAllBroadcastOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveAllToAllOpHandle<T: Remote> {
    #[pin]
    pub(crate) future: CollectiveAllToAllOpFuture<T>,
}

#[pin_project(project = CollectiveAllToAllOpFutureProj)]
pub(crate) enum CollectiveAllToAllOpFuture<T: Remote> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveAllToAllFuture<T>),
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtCollectiveAllToAllFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveAllToAllFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllToAllFuture<T>),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtCollectiveAllToAllFuture<T>),
    // Shmem(#[pin] ShmemCollectiveAllBroadcastFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote> CollectiveAllToAllOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Vec<T> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllToAllOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllToAllOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllToAllOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllToAllOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllToAllOpFuture::UcxMt(f) => f.block(),
            // CollectiveAllBroadcastOpFuture::Shmem(f) => f.block(),
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
            CollectiveAllToAllOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllToAllOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllToAllOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllToAllOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllToAllOpFuture::UcxMt(f) => f.spawn(),
            // CollectiveAllBroadcastOpFuture::Shmem(f) => f.spawn(),
            // AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote> Future for CollectiveAllToAllOpHandle<T> {
    type Output = Vec<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllToAllOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllToAllOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllToAllOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllToAllOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllToAllOpFutureProj::UcxMt(f) => f.poll(cx),
            // CollectiveAllBroadcastOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[must_use = " CollectiveAllBroadcastIntoBufferOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveAllToAllIntoBufferOpHandle<T: Remote, B: AsLamellarBuffer<T>> {
    #[pin]
    pub(crate) future: CollectiveAllToAllIntoBufferOpFuture<T, B>,
}

#[pin_project(project = CollectiveAllToAllIntoBufferOpFutureProj)]
pub(crate) enum CollectiveAllToAllIntoBufferOpFuture<T: Remote, B: AsLamellarBuffer<T>> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveAllToAllIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtCollectiveAllToAllIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveAllToAllIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllToAllIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtCollectiveAllToAllIntoBufferFuture<T, B>),
    // Shmem(#[pin] ShmemCollectiveAllBroadcastIntoBufferFuture<T, B>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveAllToAllIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllToAllIntoBufferOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllToAllIntoBufferOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllToAllIntoBufferOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllToAllIntoBufferOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllToAllIntoBufferOpFuture::UcxMt(f) => f.block(),
            // CollectiveAllBroadcastIntoBufferOpFuture::Shmem(f) => f.block(),
            // CollectiveAllBroadcastIntoBufferOpFuture::Local(f) => f.block(),
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
            CollectiveAllToAllIntoBufferOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllToAllIntoBufferOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllToAllIntoBufferOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllToAllIntoBufferOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllToAllIntoBufferOpFuture::UcxMt(f) => f.spawn(),
            // CollectiveAllBroadcastIntoBufferOpFuture::Shmem(f) => f.spawn(),
            // CollectiveAllBroadcastIntoBufferOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for CollectiveAllToAllIntoBufferOpHandle<T, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllToAllIntoBufferOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveAllToAllIntoBufferOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllToAllIntoBufferOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllToAllIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveAllToAllIntoBufferOpFutureProj::UcxMt(f) => f.poll(cx),
            // CollectiveAllBroadcastIntoBufferOpFutureProj::Shmem(f) => f.poll(cx),
            // CollectiveAllBroadcastIntoBufferOpFutureProj::Local(f) => f.poll(cx),
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
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtCollectiveBroadcastFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveBroadcastFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveBroadcastFuture<T>),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtCollectiveBroadcastFuture<T>),
    // Shmem(#[pin] ShmemCollectiveBroadcastFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote> CollectiveBroadcastOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Option<Vec<T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveBroadcastOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveBroadcastOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveBroadcastOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveBroadcastOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveBroadcastOpFuture::UcxMt(f) => f.block(),
            // CollectiveBroadcastOpFuture::Shmem(f) => f.block(),
            // CollectiveBroadcastOpFuture::Local(f) => f.block(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveBroadcastOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveBroadcastOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveBroadcastOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveBroadcastOpFuture::UcxMt(f) => f.spawn(),
            // CollectiveBroadcastOpFuture::Shmem(f) => f.spawn(),
            // CollectiveBroadcastOpFuture::Local(f) => f.spawn(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveBroadcastOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveBroadcastOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveBroadcastOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveBroadcastOpFutureProj::UcxMt(f) => f.poll(cx),
            // CollectiveBroadcastOpFutureProj::Shmem(f) => f.poll(cx),
            // CollectiveBroadcastOpFutureProj::Local(f) => f.poll(cx),
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
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtCollectiveBroadcastIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveBroadcastIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveBroadcastIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtCollectiveBroadcastIntoBufferFuture<T, B>),
    // Shmem(#[pin] ShmemCollectiveBroadcastIntoBufferFuture<T, B>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveBroadcastIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveBroadcastIntoBufferOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveBroadcastIntoBufferOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveBroadcastIntoBufferOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveBroadcastIntoBufferOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveBroadcastIntoBufferOpFuture::UcxMt(f) => f.block(),
            // CollectiveBroadcastIntoBufferOpFuture::Shmem(f) => f.block(),
            // CollectiveBroadcastIntoBufferOpFuture::Local(f) => f.block(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveBroadcastIntoBufferOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveBroadcastIntoBufferOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveBroadcastIntoBufferOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveBroadcastIntoBufferOpFuture::UcxMt(f) => f.spawn(),
            // CollectiveBroadcastIntoBufferOpFuture::Shmem(f) => f.spawn(),
            // CollectiveBroadcastIntoBufferOpFuture::Local(f) => f.spawn(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveBroadcastIntoBufferOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveBroadcastIntoBufferOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveBroadcastIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveBroadcastIntoBufferOpFutureProj::UcxMt(f) => f.poll(cx),
            // CollectiveBroadcastIntoBufferOpFutureProj::Shmem(f) => f.poll(cx),
            // CollectiveBroadcastIntoBufferOpFutureProj::Local(f) => f.poll(cx),
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
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtCollectiveScatterFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveScatterFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveScatterFuture<T>),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtCollectiveScatterFuture<T>),
    // Shmem(#[pin] ShmemCollectiveScatterFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote> CollectiveScatterOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Vec<T> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveScatterOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveScatterOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveScatterOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveScatterOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveScatterOpFuture::UcxMt(f) => f.block(),
            // CollectiveScatterOpFuture::Shmem(f) => f.block(),
            // CollectiveScatterOpFuture::Local(f) => f.block(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveScatterOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveScatterOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveScatterOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveScatterOpFuture::UcxMt(f) => f.spawn(),
            // CollectiveScatterOpFuture::Shmem(f) => f.spawn(),
            // CollectiveScatterOpFuture::Local(f) => f.spawn(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveScatterOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveScatterOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveScatterOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveScatterOpFutureProj::UcxMt(f) => f.poll(cx),
            // CollectiveScatterOpFutureProj::Shmem(f) => f.poll(cx),
            // CollectiveScatterOpFutureProj::Local(f) => f.poll(cx),
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
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtCollectiveScatterIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveScatterIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveScatterIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtCollectiveScatterIntoBufferFuture<T, B>),
    // Shmem(#[pin] ShmemCollectiveScatterIntoBufferFuture<T, B>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveScatterIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveScatterIntoBufferOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveScatterIntoBufferOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveScatterIntoBufferOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveScatterIntoBufferOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveScatterIntoBufferOpFuture::UcxMt(f) => f.block(),
            // CollectiveScatterIntoBufferOpFuture::Shmem(f) => f.block(),
            // CollectiveScatterIntoBufferOpFuture::Local(f) => f.block(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveScatterIntoBufferOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveScatterIntoBufferOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveScatterIntoBufferOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveScatterIntoBufferOpFuture::UcxMt(f) => f.spawn(),
            // CollectiveScatterIntoBufferOpFuture::Shmem(f) => f.spawn(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveScatterIntoBufferOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveScatterIntoBufferOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveScatterIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveScatterIntoBufferOpFutureProj::UcxMt(f) => f.poll(cx),
            // CollectiveScatterIntoBufferOpFutureProj::Shmem(f) => f.poll(cx),
            // CollectiveScatterIntoBufferOpFutureProj::Local(f) => f.poll(cx),
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
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtCollectiveReduceScatterFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveReduceScatterFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveReduceScatterFuture<T>),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtCollectiveReduceScatterFuture<T>),
    // Shmem(#[pin] ShmemCollectiveReduceScatterFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote> CollectiveReduceScatterOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Vec<T> {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceScatterOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveReduceScatterOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceScatterOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceScatterOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveReduceScatterOpFuture::UcxMt(f) => f.block(),
            // CollectiveReduceScatterOpFuture::Shmem(f) => f.block(),
            // CollectiveReduceScatterOpFuture::Local(f) => f.block(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveReduceScatterOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceScatterOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceScatterOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveReduceScatterOpFuture::UcxMt(f) => f.spawn(),
            // CollectiveReduceScatterOpFuture::Shmem(f) => f.spawn(),
            // CollectiveReduceScatterOpFuture::Local(f) => f.spawn(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveReduceScatterOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceScatterOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceScatterOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveReduceScatterOpFutureProj::UcxMt(f) => f.poll(cx),
            // CollectiveReduceScatterOpFutureProj::Shmem(f) => f.poll(cx),
            // CollectiveReduceScatterOpFutureProj::Local(f) => f.poll(cx),
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
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtCollectiveReduceScatterIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveReduceScatterIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveReduceScatterIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtCollectiveReduceScatterIntoBufferFuture<T, B>),
    // Shmem(#[pin] ShmemCollectiveReduceScatterIntoBufferFuture<T, B>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceScatterIntoBufferOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveReduceScatterIntoBufferOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceScatterIntoBufferOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceScatterIntoBufferOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveReduceScatterIntoBufferOpFuture::UcxMt(f) => f.block(),
            // CollectiveReduceScatterIntoBufferOpFuture::Shmem(f) => f.block(),
            // CollectiveReduceScatterIntoBufferOpFuture::Local(f) => f.block(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveReduceScatterIntoBufferOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceScatterIntoBufferOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceScatterIntoBufferOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveReduceScatterIntoBufferOpFuture::UcxMt(f) => f.spawn(),
            // CollectiveReduceScatterIntoBufferOpFuture::Shmem(f) => f.spawn(),
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
            #[cfg(feature = "enable-libfabric-mt")]
            CollectiveReduceScatterIntoBufferOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceScatterIntoBufferOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceScatterIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            CollectiveReduceScatterIntoBufferOpFutureProj::UcxMt(f) => f.poll(cx),
            // CollectiveReduceScatterIntoBufferOpFutureProj::Shmem(f) => f.poll(cx),
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


pub enum BroadcastInput {
    Root(usize),
    NotRoot(usize)
}

pub(crate) enum BroadcastInputInner {
    Root(usize),
    NotRoot(usize)
}


impl From<BroadcastInput> for BroadcastInputInner {
    fn from(value: BroadcastInput) -> Self {
        match value {
            BroadcastInput::Root(index) => BroadcastInputInner::Root(index),
            BroadcastInput::NotRoot(root_pe) => BroadcastInputInner::NotRoot(root_pe),
        }
    }
}

impl BroadcastInput {
    pub fn root(index: usize) -> Self {
        Self::Root(index)
    }

    pub fn not_root(root_pe: usize) -> Self {
        Self::NotRoot(root_pe)
    }
}

pub(crate) enum RootSrcOrBuffer<T: Remote> {
    Root(usize), 
    NotRoot(Vec<T>, usize) 
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<RootSrcOrLamellarBuffer<T, B>> for RootSrcOrLamellarBufferInner<T, B> {
    fn from(value: RootSrcOrLamellarBuffer<T, B>) -> Self {
        match value {
            RootSrcOrLamellarBuffer::Root(index) => RootSrcOrLamellarBufferInner::Root(index),
            RootSrcOrLamellarBuffer::NotRoot(lamellar_buffer, root_pe) => RootSrcOrLamellarBufferInner::NotRoot(lamellar_buffer, root_pe),
        }
    }
}

pub(crate) enum RootSrcOrLamellarBufferInner<T: Remote, B: AsLamellarBuffer<T>> {
    Root(usize), 
    NotRoot(LamellarBuffer<T, B>, usize) 
}

pub enum RootSrcOrLamellarBuffer<T: Remote, B: AsLamellarBuffer<T>> {
    Root(usize), 
    NotRoot(LamellarBuffer<T, B>, usize) 
}

pub enum ScatterInput {
    Root(usize),
    NotRoot(usize)
}


impl ScatterInput {
    pub fn root(index: usize) -> Self {
        Self::Root(index)
    }

    pub fn not_root(root_pe: usize) -> Self {
        Self::NotRoot(root_pe)
    }
}

pub(crate) enum ScatterInputInner {
    Root(usize),
    NotRoot(usize)
}


impl From<ScatterInput> for ScatterInputInner {
    fn from(value: ScatterInput) -> Self {
        match value {
            ScatterInput::Root(index) => ScatterInputInner::Root(index),
            ScatterInput::NotRoot(root_pe) => ScatterInputInner::NotRoot(root_pe),
        }
    }
}


pub(crate) enum RootSrcSliceOrNone<'a, T> {
    Root(&'a [T]), 
    NotRoot(usize) 
}


pub(crate) enum RootSrcOrSliceMut<'a, T> {
    Root(&'a mut [T]), 
    NotRoot(&'a mut [T], usize) 
}

impl<T: Remote>  RootSrcOrBuffer<T> {
    pub(crate) fn as_mut_slice<'a>(&'a mut self, alloc_slice: &'a mut [T], len: usize) -> RootSrcOrSliceMut<'a, T> {
        match self {
            RootSrcOrBuffer::Root(index) => RootSrcOrSliceMut::Root(&mut alloc_slice[*index..*index + len]),
            RootSrcOrBuffer::NotRoot(vec, pe) => {RootSrcOrSliceMut::NotRoot(vec, *pe)}
        }
    }
}

impl ScatterInputInner {
    pub(crate) fn as_slice<'a, T>(&'a self, alloc_slice: &'a [T], len: usize) -> RootSrcSliceOrNone<'a, T> {
        match self {
            ScatterInputInner::Root(index) => RootSrcSliceOrNone::Root(&alloc_slice[*index..*index + len]),
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
    pub(crate) fn as_mut_slice<'a>(&'a mut self, alloc_slice: &'a mut [T], len: usize) -> RootSrcOrSliceMut<'a, T> {
        match self {
            RootSrcOrLamellarBufferInner::Root(index) => RootSrcOrSliceMut::Root(&mut alloc_slice[*index..*index + len]),
            RootSrcOrLamellarBufferInner::NotRoot(lamellar_buffer, pe) => {
                let buf_slice = lamellar_buffer.as_mut_slice();
                if buf_slice.len() < len {
                    panic!("RootSrcOrLamellarBufferInner::NotRoot buffer size {} is smaller than required len {}", buf_slice.len(), len);
                }
                RootSrcOrSliceMut::NotRoot(&mut buf_slice[..len], *pe)
            },
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
        index: usize,
        len: usize,
        op: ReduceOp,
    ) -> CollectiveAllReduceOpHandle<T>;
    
    fn reduce_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
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
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveReduceOpHandle<T>;
    
    fn reduce_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        index: usize,
        len: usize,
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
        index: usize,
        len: usize,
    ) -> CollectiveAllGatherOpHandle<T>;
    fn gather_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllGatherIntoBufferOpHandle<T, B>;
}

pub(crate) trait CommAllocCollectiveGather {
    fn gather<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveGatherOpHandle<T>;
    fn gather_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
        root_or_buffer: RootOrLamellarBuffer<T, B>
    ) -> CollectiveGatherIntoBufferOpHandle<T, B>;
}

pub(crate) trait CommAllocCollectiveAllToAll {
    fn alltoall<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllToAllOpHandle<T>;
    fn alltoall_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllToAllIntoBufferOpHandle<T, B>;
}

pub(crate) trait CommAllocCollectiveBroadcast {
    fn broadcast<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src_or_root_pe: BroadcastInput,
        len: usize,
    ) -> CollectiveBroadcastOpHandle<T>;
    fn broadcast_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        dst: RootSrcOrLamellarBuffer<T, B>,
        len: usize,
    ) -> CollectiveBroadcastIntoBufferOpHandle<T, B>;
}

pub(crate) trait CommAllocCollectiveScatter {
    fn scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterOpHandle<T>;
    fn scatter_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        dst: LamellarBuffer<T, B>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterIntoBufferOpHandle<T, B>;
}

pub(crate) trait CommAllocCollectiveReduceScatter {
    fn reduce_scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        index: usize,
        len: usize,
    ) -> CollectiveReduceScatterOpHandle<T>;
    
    fn reduce_scatter_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        index: usize,
        len: usize,
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
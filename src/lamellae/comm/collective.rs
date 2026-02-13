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
};

use crate::{
    active_messaging::AMCounters, scheduler::Scheduler, AsLamellarBuffer, LamellarBuffer, LamellarTask, Remote
};

use futures_util::Future;
use pin_project::pin_project;
use std::{
    pin::Pin,
    sync::{atomic::*, Arc},
    task::{Context, Poll},
};

#[must_use = " CollectiveAllReduceOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveAllReduceOpHandle<T> {
    #[pin]
    pub(crate) future: CollectiveAllReduceOpFuture<T>,
}

#[pin_project(project = CollectiveAllReduceOpFutureProj)]
pub(crate) enum CollectiveAllReduceOpFuture<T> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveAllReduceFuture<T>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    // #[cfg(feature = "enable-ucx")]
    // Ucx(#[pin] UcxAtomicFuture<T>),
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
            // AtomicFetchOpFuture::Ucx(f) => f.block(),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFuture::Ucx(f) => f.spawn(),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFutureProj::Ucx(f) => f.poll(cx),
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
    // #[cfg(feature = "enable-ucx")]
    // Ucx(#[pin] UcxAtomicFuture<T>),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFuture::Ucx(f) => f.block(),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFuture::Ucx(f) => f.spawn(),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[must_use = " CollectiveAllReduceInPlaceOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveAllReduceInPlaceOpHandle<T> {
    #[pin]
    pub(crate) future: CollectiveAllReduceInPlaceOpFuture<T>,
}

#[pin_project(project = CollectiveAllReduceInPlaceOpFutureProj)]
pub(crate) enum CollectiveAllReduceInPlaceOpFuture<T> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveAllReduceInPlaceFuture<T>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    // #[cfg(feature = "enable-ucx")]
    // Ucx(#[pin] UcxAtomicFuture<T>),
    // Shmem(#[pin] ShmemAtomicFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote> CollectiveAllReduceInPlaceOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceInPlaceOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFuture::Ucx(f) => f.block(),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFuture::Ucx(f) => f.spawn(),
            // AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            // AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote> Future for CollectiveAllReduceInPlaceOpHandle<T> {
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}


#[must_use = " CollectiveReduceOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveReduceOpHandle<T> {
    #[pin]
    pub(crate) future: CollectiveReduceOpFuture<T>,
}


#[pin_project(project = CollectiveReduceOpFutureProj)]
pub(crate) enum CollectiveReduceOpFuture<T> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveReduceFuture<T>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    // #[cfg(feature = "enable-ucx")]
    // Ucx(#[pin] UcxAtomicFuture<T>),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFuture::Ucx(f) => f.block(),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFuture::Ucx(f) => f.spawn(),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFutureProj::Ucx(f) => f.poll(cx),
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
    // #[cfg(feature = "enable-ucx")]
    // Ucx(#[pin] UcxAtomicFuture<T>),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFuture::Ucx(f) => f.block(),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFuture::Ucx(f) => f.spawn(),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFutureProj::Ucx(f) => f.poll(cx),
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
    // #[cfg(feature = "enable-ucx")]
    // Ucx(#[pin] UcxAtomicFuture<T>),
    // Shmem(#[pin] ShmemAtomicFuture<T>),
    // Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote> CollectiveReduceInPlaceOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceInPlaceOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFuture::Ucx(f) => f.block(),
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
            CollectiveReduceInPlaceOpFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric-async")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFuture::Ucx(f) => f.spawn(),
            // AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            // AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}


#[must_use = " CollectiveAllGatherOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveAllGatherOpHandle<T> {
    #[pin]
    pub(crate) future: CollectiveAllGatherOpFuture<T>,
}

#[pin_project(project = CollectiveAllGatherOpFutureProj)]
pub(crate) enum CollectiveAllGatherOpFuture<T> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveAllGatherFuture<T>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    // #[cfg(feature = "enable-ucx")]
    // Ucx(#[pin] UcxAtomicFuture<T>),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFuture::Ucx(f) => f.block(),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFuture::Ucx(f) => f.spawn(),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFutureProj::Ucx(f) => f.poll(cx),
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
    // #[cfg(feature = "enable-ucx")]
    // Ucx(#[pin] UcxAtomicFuture<T>),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFuture::Ucx(f) => f.block(),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFuture::Ucx(f) => f.spawn(),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFutureProj::Ucx(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            // AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[must_use = " CollectiveGatherOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveGatherOpHandle<T> {
    #[pin]
    pub(crate) future: CollectiveGatherOpFuture<T>,
}


#[pin_project(project = CollectiveGatherOpFutureProj)]
pub(crate) enum CollectiveGatherOpFuture<T> {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveGatherFuture<T>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric-async")]
    // LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    // #[cfg(feature = "enable-ucx")]
    // Ucx(#[pin] UcxAtomicFuture<T>),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFuture::Ucx(f) => f.block(),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFuture::Ucx(f) => f.spawn(),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFutureProj::Ucx(f) => f.poll(cx),
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
    // #[cfg(feature = "enable-ucx")]
    // Ucx(#[pin] UcxAtomicFuture<T>),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFuture::Ucx(f) => f.block(),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFuture::Ucx(f) => f.spawn(),
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
            // #[cfg(feature = "enable-ucx")]
            // AtomicFetchOpFutureProj::Ucx(f) => f.poll(cx),
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
        op: ReduceOp,
    ) -> CollectiveAllReduceOpHandle<T>;
    
    fn reduce_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceIntoBufferOpHandle<T, B>;

    fn reduce_all_in_place<T: Remote>(
        &self, // TODO: This should probably take a multiple reference to self.
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
    ) -> CollectiveAllReduceInPlaceOpHandle<T>;
}

pub(crate) trait CommAllocCollectiveReduce {
    fn reduce<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        root_pe: usize,
    ) -> CollectiveReduceOpHandle<T>;
    
    fn reduce_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        root_or_buffer: RootOrLamellarBuffer<T, B>
    ) -> CollectiveReduceIntoBufferOpHandle<T, B>;
    
    fn reduce_in_place<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        root_pe: usize,
    ) -> CollectiveReduceInPlaceOpHandle<T>;
}

pub(crate) trait CommAllocCollectiveAllGather {
    fn gather_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
    ) -> CollectiveAllGatherOpHandle<T>;
    fn gather_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllGatherIntoBufferOpHandle<T, B>;
}

pub(crate) trait CommAllocCollectiveGather {
    fn gather<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        root_pe: usize,
    ) -> CollectiveGatherOpHandle<T>;
    fn gather_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        root_or_buffer: RootOrLamellarBuffer<T, B>
    ) -> CollectiveGatherIntoBufferOpHandle<T, B>;
}
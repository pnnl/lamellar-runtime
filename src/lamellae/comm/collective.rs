#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabric_lamellae::collective::{
    LibfabricCollectiveAllGatherFuture, LibfabricCollectiveAllGatherIntoBufferFuture,
    LibfabricCollectiveAllReduceFuture, LibfabricCollectiveAllReduceInPlaceFuture,
    LibfabricCollectiveAllReduceIntoBufferFuture, LibfabricCollectiveAllToAllFuture,
    LibfabricCollectiveAllToAllIntoBufferFuture, LibfabricCollectiveBroadcastFuture,
    LibfabricCollectiveBroadcastIntoBufferFuture, LibfabricCollectiveGatherFuture,
    LibfabricCollectiveGatherIntoBufferFuture, LibfabricCollectiveReduceFuture,
    LibfabricCollectiveReduceIntoBufferFuture, LibfabricCollectiveReduceScatterFuture,
    LibfabricCollectiveReduceScatterIntoBufferFuture, LibfabricCollectiveScatterFuture,
    LibfabricCollectiveScatterIntoBufferFuture,
};
#[cfg(feature = "enable-libfabric-sys")]
use crate::lamellae::libfabric_sys_lamellae::collective::{
    LibfabricSysCollectiveAllGatherFuture, LibfabricSysCollectiveAllGatherIntoBufferFuture,
    LibfabricSysCollectiveAllReduceFuture, LibfabricSysCollectiveAllReduceInPlaceFuture,
    LibfabricSysCollectiveAllReduceIntoBufferFuture, LibfabricSysCollectiveAllToAllFuture,
    LibfabricSysCollectiveAllToAllIntoBufferFuture, LibfabricSysCollectiveBroadcastFuture,
    LibfabricSysCollectiveBroadcastIntoBufferFuture, LibfabricSysCollectiveGatherFuture,
    LibfabricSysCollectiveGatherIntoBufferFuture, LibfabricSysCollectiveReduceFuture,
    LibfabricSysCollectiveReduceIntoBufferFuture, LibfabricSysCollectiveReduceScatterFuture,
    LibfabricSysCollectiveReduceScatterIntoBufferFuture, LibfabricSysCollectiveScatterFuture,
    LibfabricSysCollectiveScatterIntoBufferFuture,
};
use crate::{
    active_messaging::AMCounters, scheduler::Scheduler, AsLamellarBuffer, LamellarBuffer,
    LamellarTask, Remote,
};

#[cfg(feature = "enable-libfabric-async")]
use crate::lamellae::libfabric_async_lamellae::collective::{
    LibfabricAsyncCollectiveAllGatherFuture, LibfabricAsyncCollectiveAllGatherIntoBufferFuture,
    LibfabricAsyncCollectiveAllReduceFuture, LibfabricAsyncCollectiveAllReduceInPlaceFuture,
    LibfabricAsyncCollectiveAllReduceIntoBufferFuture, LibfabricAsyncCollectiveAllToAllFuture,
    LibfabricAsyncCollectiveAllToAllIntoBufferFuture, LibfabricAsyncCollectiveBroadcastFuture,
    LibfabricAsyncCollectiveBroadcastIntoBufferFuture, LibfabricAsyncCollectiveGatherFuture,
    LibfabricAsyncCollectiveGatherIntoBufferFuture, LibfabricAsyncCollectiveReduceFuture,
    LibfabricAsyncCollectiveReduceIntoBufferFuture, LibfabricAsyncCollectiveReduceScatterFuture,
    LibfabricAsyncCollectiveReduceScatterIntoBufferFuture, LibfabricAsyncCollectiveScatterFuture,
    LibfabricAsyncCollectiveScatterIntoBufferFuture,
};

#[cfg(feature = "enable-ucx")]
use crate::lamellae::ucx_lamellae::collective::{
    UcxCollectiveAllGatherFuture, UcxCollectiveAllGatherIntoBufferFuture,
    UcxCollectiveAllReduceFuture, UcxCollectiveAllReduceInPlaceFuture,
    UcxCollectiveAllReduceIntoBufferFuture, UcxCollectiveAllToAllFuture,
    UcxCollectiveAllToAllIntoBufferFuture, UcxCollectiveBroadcastFuture,
    UcxCollectiveBroadcastIntoBufferFuture, UcxCollectiveGatherFuture,
    UcxCollectiveGatherIntoBufferFuture, UcxCollectiveReduceFuture,
    UcxCollectiveReduceIntoBufferFuture, UcxCollectiveReduceScatterFuture,
    UcxCollectiveReduceScatterIntoBufferFuture, UcxCollectiveScatterFuture,
    UcxCollectiveScatterIntoBufferFuture,
};

use crate::lamellae::shmem_lamellae::collective::{
    ShmemCollectiveAllGatherFuture, ShmemCollectiveAllGatherIntoBufferFuture,
    ShmemCollectiveAllReduceFuture, ShmemCollectiveAllReduceInPlaceFuture,
    ShmemCollectiveAllReduceIntoBufferFuture, ShmemCollectiveAllToAllFuture,
    ShmemCollectiveAllToAllIntoBufferFuture, ShmemCollectiveBroadcastFuture,
    ShmemCollectiveBroadcastIntoBufferFuture, ShmemCollectiveGatherFuture,
    ShmemCollectiveGatherIntoBufferFuture, ShmemCollectiveReduceFuture,
    ShmemCollectiveReduceIntoBufferFuture, ShmemCollectiveReduceScatterFuture,
    ShmemCollectiveReduceScatterIntoBufferFuture, ShmemCollectiveScatterFuture,
    ShmemCollectiveScatterIntoBufferFuture,
};

use crate::lamellae::local_lamellae::collective::{
    LocalCollectiveAllGatherFuture, LocalCollectiveAllGatherIntoBufferFuture,
    LocalCollectiveAllReduceFuture, LocalCollectiveAllReduceInPlaceFuture,
    LocalCollectiveAllReduceIntoBufferFuture, LocalCollectiveAllToAllFuture,
    LocalCollectiveAllToAllIntoBufferFuture, LocalCollectiveBroadcastFuture,
    LocalCollectiveBroadcastIntoBufferFuture, LocalCollectiveGatherFuture,
    LocalCollectiveGatherIntoBufferFuture, LocalCollectiveReduceFuture,
    LocalCollectiveReduceIntoBufferFuture, LocalCollectiveReduceScatterFuture,
    LocalCollectiveReduceScatterIntoBufferFuture, LocalCollectiveScatterFuture,
    LocalCollectiveScatterIntoBufferFuture,
};

use futures_util::Future;
use pin_project::pin_project;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

#[must_use = " CollectiveAllReduceOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveAllReduceOpHandle<T: Remote> {
    #[pin]
    pub(crate) future: CollectiveAllReduceOpFuture<T>,
}

#[pin_project(project = CollectiveAllReduceOpFutureProj)]
pub(crate) enum CollectiveAllReduceOpFuture<T: Remote> {
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(#[pin] LibfabricSysCollectiveAllReduceFuture<T>),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveAllReduceFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveAllReduceFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllReduceFuture<T>),
    Shmem(#[pin] ShmemCollectiveAllReduceFuture<T>),
    Local(#[pin] LocalCollectiveAllReduceFuture<T>),
    #[allow(dead_code)]
    _Phantom(std::marker::PhantomData<T>),
}

impl<T: Remote> CollectiveAllReduceOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Vec<T> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllReduceOpFuture::LibfabricSys(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllReduceOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceOpFuture::Ucx(f) => f.block(),
            CollectiveAllReduceOpFuture::Shmem(f) => f.block(),
            CollectiveAllReduceOpFuture::Local(f) => f.block(),
            _ => unreachable!(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<Vec<T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllReduceOpFuture::LibfabricSys(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllReduceOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceOpFuture::Ucx(f) => f.spawn(),
            CollectiveAllReduceOpFuture::Shmem(f) => f.spawn(),
            CollectiveAllReduceOpFuture::Local(f) => f.spawn(),
            _ => unreachable!(),
        }
    }
}

impl<T: Remote> Future for CollectiveAllReduceOpHandle<T> {
    type Output = Vec<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllReduceOpFutureProj::LibfabricSys(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllReduceOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceOpFutureProj::Ucx(f) => f.poll(cx),
            CollectiveAllReduceOpFutureProj::Shmem(f) => f.poll(cx),
            CollectiveAllReduceOpFutureProj::Local(f) => f.poll(cx),
            _ => unreachable!(),
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
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(#[pin] LibfabricSysCollectiveAllReduceIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveAllReduceIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveAllReduceIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllReduceIntoBufferFuture<T, B>),
    Shmem(#[pin] ShmemCollectiveAllReduceIntoBufferFuture<T, B>),
    Local(#[pin] LocalCollectiveAllReduceIntoBufferFuture<T, B>),
    #[allow(dead_code)]
    _Phantom(std::marker::PhantomData<(T, B)>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveAllReduceIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllReduceIntoBufferOpFuture::LibfabricSys(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceIntoBufferOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllReduceIntoBufferOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceIntoBufferOpFuture::Ucx(f) => f.block(),
            CollectiveAllReduceIntoBufferOpFuture::Shmem(f) => f.block(),
            CollectiveAllReduceIntoBufferOpFuture::Local(f) => f.block(),
            _ => unreachable!(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllReduceIntoBufferOpFuture::LibfabricSys(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceIntoBufferOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllReduceIntoBufferOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceIntoBufferOpFuture::Ucx(f) => f.spawn(),
            CollectiveAllReduceIntoBufferOpFuture::Shmem(f) => f.spawn(),
            CollectiveAllReduceIntoBufferOpFuture::Local(f) => f.spawn(),
            _ => unreachable!(),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for CollectiveAllReduceIntoBufferOpHandle<T, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllReduceIntoBufferOpFutureProj::LibfabricSys(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceIntoBufferOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllReduceIntoBufferOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            CollectiveAllReduceIntoBufferOpFutureProj::Shmem(f) => f.poll(cx),
            CollectiveAllReduceIntoBufferOpFutureProj::Local(f) => f.poll(cx),
            _ => unreachable!(),
        }
    }
}

#[must_use = " CollectiveAllReduceInPlaceOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveAllReduceInPlaceOpHandle<T: Remote, B: AsLamellarBuffer<T>> {
    #[pin]
    pub(crate) future: CollectiveAllReduceInPlaceOpFuture<T, B>,
}

#[pin_project(project = CollectiveAllReduceInPlaceOpFutureProj)]
pub(crate) enum CollectiveAllReduceInPlaceOpFuture<T: Remote, B: AsLamellarBuffer<T>> {
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(#[pin] LibfabricSysCollectiveAllReduceInPlaceFuture<T, B>),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveAllReduceInPlaceFuture<T, B>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveAllReduceInPlaceFuture<T, B>), // we can reuse the IntoBuffer future for the InPlace operation since the buffer is provided by the caller in both cases
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllReduceInPlaceFuture<T, B>),
    Shmem(#[pin] ShmemCollectiveAllReduceInPlaceFuture<T, B>),
    Local(#[pin] LocalCollectiveAllReduceInPlaceFuture<T, B>),
    #[allow(dead_code)]
    _Phantom(std::marker::PhantomData<(T, B)>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveAllReduceInPlaceOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllReduceInPlaceOpFuture::LibfabricSys(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceInPlaceOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllReduceInPlaceOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceInPlaceOpFuture::Ucx(f) => f.block(),
            CollectiveAllReduceInPlaceOpFuture::Shmem(f) => f.block(),
            CollectiveAllReduceInPlaceOpFuture::Local(f) => f.block(),
            _ => unreachable!(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllReduceInPlaceOpFuture::LibfabricSys(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceInPlaceOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllReduceInPlaceOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceInPlaceOpFuture::Ucx(f) => f.spawn(),
            CollectiveAllReduceInPlaceOpFuture::Shmem(f) => f.spawn(),
            CollectiveAllReduceInPlaceOpFuture::Local(f) => f.spawn(),
            _ => unreachable!(),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for CollectiveAllReduceInPlaceOpHandle<T, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllReduceInPlaceOpFutureProj::LibfabricSys(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllReduceInPlaceOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllReduceInPlaceOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllReduceInPlaceOpFutureProj::Ucx(f) => f.poll(cx),
            CollectiveAllReduceInPlaceOpFutureProj::Shmem(f) => f.poll(cx),
            CollectiveAllReduceInPlaceOpFutureProj::Local(f) => f.poll(cx),
            _ => unreachable!(),
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
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(#[pin] LibfabricSysCollectiveReduceFuture<T>),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveReduceFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveReduceFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveReduceFuture<T>),
    Shmem(#[pin] ShmemCollectiveReduceFuture<T>),
    Local(#[pin] LocalCollectiveReduceFuture<T>),
    #[allow(dead_code)]
    _Phantom(std::marker::PhantomData<T>),
}

impl<T: Remote> CollectiveReduceOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Option<Vec<T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveReduceOpFuture::LibfabricSys(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceOpFuture::Ucx(f) => f.block(),
            CollectiveReduceOpFuture::Shmem(f) => f.block(),
            CollectiveReduceOpFuture::Local(f) => f.block(),
            _ => unreachable!(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<Option<Vec<T>>> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveReduceOpFuture::LibfabricSys(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceOpFuture::Ucx(f) => f.spawn(),
            CollectiveReduceOpFuture::Shmem(f) => f.spawn(),
            CollectiveReduceOpFuture::Local(f) => f.spawn(),
            _ => unreachable!(),
        }
    }
}

impl<T: Remote> Future for CollectiveReduceOpHandle<T> {
    type Output = Option<Vec<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveReduceOpFutureProj::LibfabricSys(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceOpFutureProj::Ucx(f) => f.poll(cx),
            CollectiveReduceOpFutureProj::Shmem(f) => f.poll(cx),
            CollectiveReduceOpFutureProj::Local(f) => f.poll(cx),
            _ => unreachable!(),
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
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(#[pin] LibfabricSysCollectiveReduceIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveReduceIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveReduceIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveReduceIntoBufferFuture<T, B>),
    Shmem(#[pin] ShmemCollectiveReduceIntoBufferFuture<T, B>),
    Local(#[pin] LocalCollectiveReduceIntoBufferFuture<T, B>),
    #[allow(dead_code)]
    _Phantom(std::marker::PhantomData<(T, B)>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveReduceIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveReduceIntoBufferOpFuture::LibfabricSys(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceIntoBufferOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceIntoBufferOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceIntoBufferOpFuture::Ucx(f) => f.block(),
            CollectiveReduceIntoBufferOpFuture::Shmem(f) => f.block(),
            CollectiveReduceIntoBufferOpFuture::Local(f) => f.block(),
            _ => unreachable!(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveReduceIntoBufferOpFuture::LibfabricSys(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceIntoBufferOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceIntoBufferOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceIntoBufferOpFuture::Ucx(f) => f.spawn(),
            CollectiveReduceIntoBufferOpFuture::Shmem(f) => f.spawn(),
            CollectiveReduceIntoBufferOpFuture::Local(f) => f.spawn(),
            _ => unreachable!(),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for CollectiveReduceIntoBufferOpHandle<T, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveReduceIntoBufferOpFutureProj::LibfabricSys(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceIntoBufferOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceIntoBufferOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            CollectiveReduceIntoBufferOpFutureProj::Shmem(f) => f.poll(cx),
            CollectiveReduceIntoBufferOpFutureProj::Local(f) => f.poll(cx),
            _ => unreachable!(),
        }
    }
}

// TODO: reduce_in_place never wired up for any backend (no live caller anywhere in the crate)
// #[must_use = " CollectiveReduceInPlaceOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
// #[pin_project]
// pub(crate) struct CollectiveReduceInPlaceOpHandle<T> {
//     #[pin]
//     pub(crate) future: CollectiveReduceInPlaceOpFuture<T>,
// }
//
//
// #[pin_project(project = CollectiveReduceInPlaceOpFutureProj)]
// pub(crate) enum CollectiveReduceInPlaceOpFuture<T> {
//     #[cfg(feature = "enable-libfabric-sys")]
//     LibfabricSys(#[pin] LibfabricSysCollectiveReduceInPlaceFuture<T>),
//     #[cfg(feature = "enable-libfabric")]
//     Libfabric(#[pin] LibfabricCollectiveReduceInPlaceFuture<T>),
//     #[cfg(feature = "enable-libfabric-mt")]
//     LibfabricMt(#[pin] LibfabricMtCollectiveReduceInPlaceFuture<T>),
//     #[cfg(feature = "enable-libfabric-async")]
//     LibfabricAsync(#[pin] LibfabricAsyncCollectiveReduceInPlaceFuture<T>),
//     #[cfg(feature = "enable-ucx")]
//     Ucx(#[pin] UcxCollectiveReduceInPlaceFuture<T>),
//     #[cfg(feature = "enable-ucx-mt")]
//     UcxMt(#[pin] UcxMtCollectiveReduceInPlaceFuture<T>),
//     // Shmem(#[pin] ShmemCollectiveReduceInPlaceFuture<T>),
//     // Local(#[pin] LocalAtomicFuture<T>),
//     _Phantom(std::marker::PhantomData<T>),
// }
//
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
//
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
//
// impl<T: Remote> Future for CollectiveReduceInPlaceOpHandle<T> {
//     type Output = ();
//
//     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         let this = self.project();
//         match this.future.project() {
//             #[cfg(feature = "enable-libfabric-sys")]
//             CollectiveReduceInPlaceOpFutureProj::LibfabricSys(f) => f.poll(cx),
//             #[cfg(feature = "enable-libfabric")]
//             CollectiveReduceInPlaceOpFutureProj::Libfabric(f) => f.poll(cx),
//             #[cfg(feature = "enable-libfabric-mt")]
//             CollectiveReduceInPlaceOpFutureProj::LibfabricMt(f) => f.poll(cx),
//             #[cfg(feature = "enable-libfabric-async")]
//             CollectiveReduceInPlaceOpFutureProj::LibfabricAsync(f) => f.poll(cx),
//             #[cfg(feature = "enable-ucx")]
//             CollectiveReduceInPlaceOpFutureProj::Ucx(f) => f.poll(cx),
//             #[cfg(feature = "enable-ucx-mt")]
//             CollectiveReduceInPlaceOpFutureProj::UcxMt(f) => f.poll(cx),
//             // CollectiveReduceInPlaceOpFutureProj::Shmem(f) => f.poll(cx),
//             // CollectiveReduceInPlaceOpFutureProj::Local(f) => f.poll(cx),
//             _ => unreachable!(),
//         }
//     }
// }

#[must_use = " CollectiveAllGatherOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct CollectiveAllGatherOpHandle<T: Remote> {
    #[pin]
    pub(crate) future: CollectiveAllGatherOpFuture<T>,
}

#[pin_project(project = CollectiveAllGatherOpFutureProj)]
pub(crate) enum CollectiveAllGatherOpFuture<T: Remote> {
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(#[pin] LibfabricSysCollectiveAllGatherFuture<T>),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveAllGatherFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveAllGatherFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllGatherFuture<T>),
    Shmem(#[pin] ShmemCollectiveAllGatherFuture<T>),
    Local(#[pin] LocalCollectiveAllGatherFuture<T>),
    #[allow(dead_code)]
    _Phantom(std::marker::PhantomData<T>),
}

impl<T: Remote> CollectiveAllGatherOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Vec<T> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllGatherOpFuture::LibfabricSys(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllGatherOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllGatherOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllGatherOpFuture::Ucx(f) => f.block(),
            CollectiveAllGatherOpFuture::Shmem(f) => f.block(),
            CollectiveAllGatherOpFuture::Local(f) => f.block(),
            _ => unreachable!(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<Vec<T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllGatherOpFuture::LibfabricSys(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllGatherOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllGatherOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllGatherOpFuture::Ucx(f) => f.spawn(),
            CollectiveAllGatherOpFuture::Shmem(f) => f.spawn(),
            CollectiveAllGatherOpFuture::Local(f) => f.spawn(),
            _ => unreachable!(),
        }
    }
}

impl<T: Remote> Future for CollectiveAllGatherOpHandle<T> {
    type Output = Vec<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllGatherOpFutureProj::LibfabricSys(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllGatherOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllGatherOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllGatherOpFutureProj::Ucx(f) => f.poll(cx),
            CollectiveAllGatherOpFutureProj::Shmem(f) => f.poll(cx),
            CollectiveAllGatherOpFutureProj::Local(f) => f.poll(cx),
            _ => unreachable!(),
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
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(#[pin] LibfabricSysCollectiveAllGatherIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveAllGatherIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveAllGatherIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllGatherIntoBufferFuture<T, B>),
    Shmem(#[pin] ShmemCollectiveAllGatherIntoBufferFuture<T, B>),
    Local(#[pin] LocalCollectiveAllGatherIntoBufferFuture<T, B>),
    #[allow(dead_code)]
    _Phantom(std::marker::PhantomData<(T, B)>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveAllGatherIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllGatherIntoBufferOpFuture::LibfabricSys(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllGatherIntoBufferOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllGatherIntoBufferOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllGatherIntoBufferOpFuture::Ucx(f) => f.block(),
            CollectiveAllGatherIntoBufferOpFuture::Shmem(f) => f.block(),
            CollectiveAllGatherIntoBufferOpFuture::Local(f) => f.block(),
            _ => unreachable!(),
        }
    }

    /// This method will spawn the associated (raw) CollectiveAllGatherIntoBufferOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllGatherIntoBufferOpFuture::LibfabricSys(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllGatherIntoBufferOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllGatherIntoBufferOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllGatherIntoBufferOpFuture::Ucx(f) => f.spawn(),
            CollectiveAllGatherIntoBufferOpFuture::Shmem(f) => f.spawn(),
            CollectiveAllGatherIntoBufferOpFuture::Local(f) => f.spawn(),
            _ => unreachable!(),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for CollectiveAllGatherIntoBufferOpHandle<T, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllGatherIntoBufferOpFutureProj::LibfabricSys(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllGatherIntoBufferOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllGatherIntoBufferOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllGatherIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            CollectiveAllGatherIntoBufferOpFutureProj::Shmem(f) => f.poll(cx),
            CollectiveAllGatherIntoBufferOpFutureProj::Local(f) => f.poll(cx),
            _ => unreachable!(),
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
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(#[pin] LibfabricSysCollectiveGatherFuture<T>),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveGatherFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveGatherFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveGatherFuture<T>),
    Shmem(#[pin] ShmemCollectiveGatherFuture<T>),
    Local(#[pin] LocalCollectiveGatherFuture<T>),
    #[allow(dead_code)]
    _Phantom(std::marker::PhantomData<T>),
}

impl<T: Remote> CollectiveGatherOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Option<Vec<T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveGatherOpFuture::LibfabricSys(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveGatherOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveGatherOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveGatherOpFuture::Ucx(f) => f.block(),
            CollectiveGatherOpFuture::Shmem(f) => f.block(),
            CollectiveGatherOpFuture::Local(f) => f.block(),
            _ => unreachable!(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<Option<Vec<T>>> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveGatherOpFuture::LibfabricSys(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveGatherOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveGatherOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveGatherOpFuture::Ucx(f) => f.spawn(),
            CollectiveGatherOpFuture::Shmem(f) => f.spawn(),
            CollectiveGatherOpFuture::Local(f) => f.spawn(),
            _ => unreachable!(),
        }
    }
}

impl<T: Remote> Future for CollectiveGatherOpHandle<T> {
    type Output = Option<Vec<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveGatherOpFutureProj::LibfabricSys(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            CollectiveGatherOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveGatherOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveGatherOpFutureProj::Ucx(f) => f.poll(cx),
            CollectiveGatherOpFutureProj::Shmem(f) => f.poll(cx),
            CollectiveGatherOpFutureProj::Local(f) => f.poll(cx),
            _ => unreachable!(),
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
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(#[pin] LibfabricSysCollectiveGatherIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveGatherIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveGatherIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveGatherIntoBufferFuture<T, B>),
    Shmem(#[pin] ShmemCollectiveGatherIntoBufferFuture<T, B>),
    Local(#[pin] LocalCollectiveGatherIntoBufferFuture<T, B>),
    #[allow(dead_code)]
    _Phantom(std::marker::PhantomData<(T, B)>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveGatherIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveGatherIntoBufferOpFuture::LibfabricSys(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveGatherIntoBufferOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveGatherIntoBufferOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveGatherIntoBufferOpFuture::Ucx(f) => f.block(),
            CollectiveGatherIntoBufferOpFuture::Shmem(f) => f.block(),
            CollectiveGatherIntoBufferOpFuture::Local(f) => f.block(),
            _ => unreachable!(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveGatherIntoBufferOpFuture::LibfabricSys(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveGatherIntoBufferOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveGatherIntoBufferOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveGatherIntoBufferOpFuture::Ucx(f) => f.spawn(),
            CollectiveGatherIntoBufferOpFuture::Shmem(f) => f.spawn(),
            CollectiveGatherIntoBufferOpFuture::Local(f) => f.spawn(),
            _ => unreachable!(),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for CollectiveGatherIntoBufferOpHandle<T, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveGatherIntoBufferOpFutureProj::LibfabricSys(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            CollectiveGatherIntoBufferOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveGatherIntoBufferOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveGatherIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            CollectiveGatherIntoBufferOpFutureProj::Shmem(f) => f.poll(cx),
            CollectiveGatherIntoBufferOpFutureProj::Local(f) => f.poll(cx),
            _ => unreachable!(),
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
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(#[pin] LibfabricSysCollectiveAllToAllFuture<T>),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveAllToAllFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveAllToAllFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllToAllFuture<T>),
    Shmem(#[pin] ShmemCollectiveAllToAllFuture<T>),
    Local(#[pin] LocalCollectiveAllToAllFuture<T>),
    #[allow(dead_code)]
    _Phantom(std::marker::PhantomData<T>),
}

impl<T: Remote> CollectiveAllToAllOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Vec<T> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllToAllOpFuture::LibfabricSys(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllToAllOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllToAllOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllToAllOpFuture::Ucx(f) => f.block(),
            CollectiveAllToAllOpFuture::Shmem(f) => f.block(),
            CollectiveAllToAllOpFuture::Local(f) => f.block(),
            _ => unreachable!(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<Vec<T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllToAllOpFuture::LibfabricSys(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllToAllOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllToAllOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllToAllOpFuture::Ucx(f) => f.spawn(),
            CollectiveAllToAllOpFuture::Shmem(f) => f.spawn(),
            CollectiveAllToAllOpFuture::Local(f) => f.spawn(),
            _ => unreachable!(),
        }
    }
}

impl<T: Remote> Future for CollectiveAllToAllOpHandle<T> {
    type Output = Vec<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllToAllOpFutureProj::LibfabricSys(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllToAllOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllToAllOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllToAllOpFutureProj::Ucx(f) => f.poll(cx),
            CollectiveAllToAllOpFutureProj::Shmem(f) => f.poll(cx),
            CollectiveAllToAllOpFutureProj::Local(f) => f.poll(cx),
            _ => unreachable!(),
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
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(#[pin] LibfabricSysCollectiveAllToAllIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveAllToAllIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveAllToAllIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveAllToAllIntoBufferFuture<T, B>),
    Shmem(#[pin] ShmemCollectiveAllToAllIntoBufferFuture<T, B>),
    Local(#[pin] LocalCollectiveAllToAllIntoBufferFuture<T, B>),
    #[allow(dead_code)]
    _Phantom(std::marker::PhantomData<(T, B)>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveAllToAllIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllToAllIntoBufferOpFuture::LibfabricSys(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllToAllIntoBufferOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllToAllIntoBufferOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllToAllIntoBufferOpFuture::Ucx(f) => f.block(),
            CollectiveAllToAllIntoBufferOpFuture::Shmem(f) => f.block(),
            CollectiveAllToAllIntoBufferOpFuture::Local(f) => f.block(),
            _ => unreachable!(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllToAllIntoBufferOpFuture::LibfabricSys(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllToAllIntoBufferOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllToAllIntoBufferOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllToAllIntoBufferOpFuture::Ucx(f) => f.spawn(),
            CollectiveAllToAllIntoBufferOpFuture::Shmem(f) => f.spawn(),
            CollectiveAllToAllIntoBufferOpFuture::Local(f) => f.spawn(),
            _ => unreachable!(),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for CollectiveAllToAllIntoBufferOpHandle<T, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveAllToAllIntoBufferOpFutureProj::LibfabricSys(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            CollectiveAllToAllIntoBufferOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveAllToAllIntoBufferOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveAllToAllIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            CollectiveAllToAllIntoBufferOpFutureProj::Shmem(f) => f.poll(cx),
            CollectiveAllToAllIntoBufferOpFutureProj::Local(f) => f.poll(cx),
            _ => unreachable!(),
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
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(#[pin] LibfabricSysCollectiveBroadcastFuture<T>),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveBroadcastFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveBroadcastFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveBroadcastFuture<T>),
    Shmem(#[pin] ShmemCollectiveBroadcastFuture<T>),
    Local(#[pin] LocalCollectiveBroadcastFuture<T>),
    #[allow(dead_code)]
    _Phantom(std::marker::PhantomData<T>),
}

impl<T: Remote> CollectiveBroadcastOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Option<Vec<T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveBroadcastOpFuture::LibfabricSys(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveBroadcastOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveBroadcastOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveBroadcastOpFuture::Ucx(f) => f.block(),
            CollectiveBroadcastOpFuture::Shmem(f) => f.block(),
            CollectiveBroadcastOpFuture::Local(f) => f.block(),
            _ => unreachable!(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<Option<Vec<T>>> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveBroadcastOpFuture::LibfabricSys(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveBroadcastOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveBroadcastOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveBroadcastOpFuture::Ucx(f) => f.spawn(),
            CollectiveBroadcastOpFuture::Shmem(f) => f.spawn(),
            CollectiveBroadcastOpFuture::Local(f) => f.spawn(),
            _ => unreachable!(),
        }
    }
}

impl<T: Remote> Future for CollectiveBroadcastOpHandle<T> {
    type Output = Option<Vec<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveBroadcastOpFutureProj::LibfabricSys(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            CollectiveBroadcastOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveBroadcastOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveBroadcastOpFutureProj::Ucx(f) => f.poll(cx),
            CollectiveBroadcastOpFutureProj::Shmem(f) => f.poll(cx),
            CollectiveBroadcastOpFutureProj::Local(f) => f.poll(cx),
            _ => unreachable!(),
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
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(#[pin] LibfabricSysCollectiveBroadcastIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveBroadcastIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveBroadcastIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveBroadcastIntoBufferFuture<T, B>),
    Shmem(#[pin] ShmemCollectiveBroadcastIntoBufferFuture<T, B>),
    Local(#[pin] LocalCollectiveBroadcastIntoBufferFuture<T, B>),
    #[allow(dead_code)]
    _Phantom(std::marker::PhantomData<(T, B)>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveBroadcastIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveBroadcastIntoBufferOpFuture::LibfabricSys(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveBroadcastIntoBufferOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveBroadcastIntoBufferOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveBroadcastIntoBufferOpFuture::Ucx(f) => f.block(),
            CollectiveBroadcastIntoBufferOpFuture::Shmem(f) => f.block(),
            CollectiveBroadcastIntoBufferOpFuture::Local(f) => f.block(),
            _ => unreachable!(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveBroadcastIntoBufferOpFuture::LibfabricSys(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveBroadcastIntoBufferOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveBroadcastIntoBufferOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveBroadcastIntoBufferOpFuture::Ucx(f) => f.spawn(),
            CollectiveBroadcastIntoBufferOpFuture::Shmem(f) => f.spawn(),
            CollectiveBroadcastIntoBufferOpFuture::Local(f) => f.spawn(),
            _ => unreachable!(),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for CollectiveBroadcastIntoBufferOpHandle<T, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveBroadcastIntoBufferOpFutureProj::LibfabricSys(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            CollectiveBroadcastIntoBufferOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveBroadcastIntoBufferOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveBroadcastIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            CollectiveBroadcastIntoBufferOpFutureProj::Shmem(f) => f.poll(cx),
            CollectiveBroadcastIntoBufferOpFutureProj::Local(f) => f.poll(cx),
            _ => unreachable!(),
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
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(#[pin] LibfabricSysCollectiveScatterFuture<T>),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveScatterFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveScatterFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveScatterFuture<T>),
    Shmem(#[pin] ShmemCollectiveScatterFuture<T>),
    Local(#[pin] LocalCollectiveScatterFuture<T>),
    #[allow(dead_code)]
    _Phantom(std::marker::PhantomData<T>),
}

impl<T: Remote> CollectiveScatterOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Vec<T> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveScatterOpFuture::LibfabricSys(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveScatterOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveScatterOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveScatterOpFuture::Ucx(f) => f.block(),
            CollectiveScatterOpFuture::Shmem(f) => f.block(),
            CollectiveScatterOpFuture::Local(f) => f.block(),
            _ => unreachable!(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<Vec<T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveScatterOpFuture::LibfabricSys(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveScatterOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveScatterOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveScatterOpFuture::Ucx(f) => f.spawn(),
            CollectiveScatterOpFuture::Shmem(f) => f.spawn(),
            CollectiveScatterOpFuture::Local(f) => f.spawn(),
            _ => unreachable!(),
        }
    }
}

impl<T: Remote> Future for CollectiveScatterOpHandle<T> {
    type Output = Vec<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveScatterOpFutureProj::LibfabricSys(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            CollectiveScatterOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveScatterOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveScatterOpFutureProj::Ucx(f) => f.poll(cx),
            CollectiveScatterOpFutureProj::Shmem(f) => f.poll(cx),
            CollectiveScatterOpFutureProj::Local(f) => f.poll(cx),
            _ => unreachable!(),
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
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(#[pin] LibfabricSysCollectiveScatterIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveScatterIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveScatterIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveScatterIntoBufferFuture<T, B>),
    Shmem(#[pin] ShmemCollectiveScatterIntoBufferFuture<T, B>),
    Local(#[pin] LocalCollectiveScatterIntoBufferFuture<T, B>),
    #[allow(dead_code)]
    _Phantom(std::marker::PhantomData<(T, B)>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveScatterIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveScatterIntoBufferOpFuture::LibfabricSys(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveScatterIntoBufferOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveScatterIntoBufferOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveScatterIntoBufferOpFuture::Ucx(f) => f.block(),
            CollectiveScatterIntoBufferOpFuture::Shmem(f) => f.block(),
            CollectiveScatterIntoBufferOpFuture::Local(f) => f.block(),
            _ => unreachable!(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveScatterIntoBufferOpFuture::LibfabricSys(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveScatterIntoBufferOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveScatterIntoBufferOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveScatterIntoBufferOpFuture::Ucx(f) => f.spawn(),
            CollectiveScatterIntoBufferOpFuture::Shmem(f) => f.spawn(),
            CollectiveScatterIntoBufferOpFuture::Local(f) => f.spawn(),
            _ => unreachable!(),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for CollectiveScatterIntoBufferOpHandle<T, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveScatterIntoBufferOpFutureProj::LibfabricSys(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            CollectiveScatterIntoBufferOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveScatterIntoBufferOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveScatterIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            CollectiveScatterIntoBufferOpFutureProj::Shmem(f) => f.poll(cx),
            CollectiveScatterIntoBufferOpFutureProj::Local(f) => f.poll(cx),
            _ => unreachable!(),
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
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(#[pin] LibfabricSysCollectiveReduceScatterFuture<T>),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveReduceScatterFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveReduceScatterFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveReduceScatterFuture<T>),
    Shmem(#[pin] ShmemCollectiveReduceScatterFuture<T>),
    Local(#[pin] LocalCollectiveReduceScatterFuture<T>),
    #[allow(dead_code)]
    _Phantom(std::marker::PhantomData<T>),
}

impl<T: Remote> CollectiveReduceScatterOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> Vec<T> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveReduceScatterOpFuture::LibfabricSys(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceScatterOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceScatterOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceScatterOpFuture::Ucx(f) => f.block(),
            CollectiveReduceScatterOpFuture::Shmem(f) => f.block(),
            CollectiveReduceScatterOpFuture::Local(f) => f.block(),
            _ => unreachable!(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<Vec<T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveReduceScatterOpFuture::LibfabricSys(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceScatterOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceScatterOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceScatterOpFuture::Ucx(f) => f.spawn(),
            CollectiveReduceScatterOpFuture::Shmem(f) => f.spawn(),
            CollectiveReduceScatterOpFuture::Local(f) => f.spawn(),
            _ => unreachable!(),
        }
    }
}

impl<T: Remote> Future for CollectiveReduceScatterOpHandle<T> {
    type Output = Vec<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveReduceScatterOpFutureProj::LibfabricSys(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceScatterOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceScatterOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceScatterOpFutureProj::Ucx(f) => f.poll(cx),
            CollectiveReduceScatterOpFutureProj::Shmem(f) => f.poll(cx),
            CollectiveReduceScatterOpFutureProj::Local(f) => f.poll(cx),
            _ => unreachable!(),
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
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(#[pin] LibfabricSysCollectiveReduceScatterIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricCollectiveReduceScatterIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncCollectiveReduceScatterIntoBufferFuture<T, B>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxCollectiveReduceScatterIntoBufferFuture<T, B>),
    Shmem(#[pin] ShmemCollectiveReduceScatterIntoBufferFuture<T, B>),
    Local(#[pin] LocalCollectiveReduceScatterIntoBufferFuture<T, B>),
    #[allow(dead_code)]
    _Phantom(std::marker::PhantomData<(T, B)>),
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveReduceScatterIntoBufferOpFuture::LibfabricSys(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceScatterIntoBufferOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceScatterIntoBufferOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceScatterIntoBufferOpFuture::Ucx(f) => f.block(),
            CollectiveReduceScatterIntoBufferOpFuture::Shmem(f) => f.block(),
            CollectiveReduceScatterIntoBufferOpFuture::Local(f) => f.block(),
            _ => unreachable!(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveReduceScatterIntoBufferOpFuture::LibfabricSys(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceScatterIntoBufferOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceScatterIntoBufferOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceScatterIntoBufferOpFuture::Ucx(f) => f.spawn(),
            CollectiveReduceScatterIntoBufferOpFuture::Shmem(f) => f.spawn(),
            CollectiveReduceScatterIntoBufferOpFuture::Local(f) => f.spawn(),
            _ => unreachable!(),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for CollectiveReduceScatterIntoBufferOpHandle<T, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric-sys")]
            CollectiveReduceScatterIntoBufferOpFutureProj::LibfabricSys(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            CollectiveReduceScatterIntoBufferOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            CollectiveReduceScatterIntoBufferOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            CollectiveReduceScatterIntoBufferOpFutureProj::Ucx(f) => f.poll(cx),
            CollectiveReduceScatterIntoBufferOpFutureProj::Shmem(f) => f.poll(cx),
            CollectiveReduceScatterIntoBufferOpFutureProj::Local(f) => f.poll(cx),
            _ => unreachable!(),
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

#[allow(dead_code)] // used only by libfabric/ucx-family fabric.rs, feature-gated out of default build
pub(crate) type AllReduceOp = ReduceOp;

pub(crate) enum RootOrBuffer<T> {
    Root(Vec<T>),
    NotRoot(usize),
}

/// Input type for broadcast collective operations, indicating whether this PE is the root (source) or not.
pub enum BroadcastInput {
    /// This PE is the root; `usize` is the local array index of the data to broadcast.
    Root(usize),
    /// This PE is not the root; `usize` is the PE index of the root.
    NotRoot(usize),
}

#[allow(dead_code)] // WIP: broadcast inner representation not yet consumed
pub(crate) enum BroadcastInputInner {
    Root(usize),
    NotRoot(usize),
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
    /// Construct a [`BroadcastInput`] for the root PE, providing the local array `index` of the data to broadcast.
    pub fn root(index: usize) -> Self {
        Self::Root(index)
    }

    /// Construct a [`BroadcastInput`] for a non-root PE, providing the `root_pe` index.
    pub fn not_root(root_pe: usize) -> Self {
        Self::NotRoot(root_pe)
    }
}

pub(crate) enum RootSrcOrBuffer<T: Remote> {
    Root(usize),
    NotRoot(Vec<T>, usize),
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<RootSrcOrLamellarBuffer<T, B>>
    for RootSrcOrLamellarBufferInner<T, B>
{
    fn from(value: RootSrcOrLamellarBuffer<T, B>) -> Self {
        match value {
            RootSrcOrLamellarBuffer::Root(index) => RootSrcOrLamellarBufferInner::Root(index),
            RootSrcOrLamellarBuffer::NotRoot(lamellar_buffer, root_pe) => {
                RootSrcOrLamellarBufferInner::NotRoot(lamellar_buffer, root_pe)
            }
        }
    }
}

pub(crate) enum RootSrcOrLamellarBufferInner<T: Remote, B: AsLamellarBuffer<T>> {
    Root(usize),
    NotRoot(LamellarBuffer<T, B>, usize),
}

/// Input type for scatter/broadcast operations where the root PE provides a source index and non-root PEs provide a destination buffer.
pub enum RootSrcOrLamellarBuffer<T: Remote, B: AsLamellarBuffer<T>> {
    /// Root PE: local array index of the source data.
    Root(usize),
    /// Non-root PE: destination buffer and root PE index.
    NotRoot(LamellarBuffer<T, B>, usize),
}

/// Input type for scatter collective operations, indicating whether this PE is the root (source) or not.
pub enum ScatterInput {
    /// This PE is the root; `usize` is the local array index of the data to scatter.
    Root(usize),
    /// This PE is not the root; `usize` is the PE index of the root.
    NotRoot(usize),
}

impl ScatterInput {
    /// Construct a [`ScatterInput`] for the root PE, providing the local array `index` of the data to scatter.
    pub fn root(index: usize) -> Self {
        Self::Root(index)
    }

    /// Construct a [`ScatterInput`] for a non-root PE, providing the `root_pe` index.
    pub fn not_root(root_pe: usize) -> Self {
        Self::NotRoot(root_pe)
    }
}

pub(crate) enum ScatterInputInner {
    Root(usize),
    NotRoot(usize),
}

impl From<ScatterInput> for ScatterInputInner {
    fn from(value: ScatterInput) -> Self {
        match value {
            ScatterInput::Root(index) => ScatterInputInner::Root(index),
            ScatterInput::NotRoot(root_pe) => ScatterInputInner::NotRoot(root_pe),
        }
    }
}

#[allow(dead_code)] // consumed only by libfabric/ucx-family fabric.rs, feature-gated out of default build
pub(crate) enum RootSrcSliceOrNone<'a, T> {
    Root(&'a [T]),
    NotRoot(usize),
}

#[allow(dead_code)] // consumed only by libfabric/ucx-family fabric.rs, feature-gated out of default build
pub(crate) enum RootSrcOrSliceMut<'a, T> {
    Root(&'a mut [T]),
    NotRoot(&'a mut [T], usize),
}

impl<T: Remote> RootSrcOrBuffer<T> {
    #[allow(dead_code)] // consumed only by libfabric/ucx-family fabric.rs, feature-gated out of default build
    pub(crate) fn as_mut_slice<'a>(
        &'a mut self,
        alloc_slice: &'a mut [T],
        len: usize,
    ) -> RootSrcOrSliceMut<'a, T> {
        match self {
            RootSrcOrBuffer::Root(index) => {
                RootSrcOrSliceMut::Root(&mut alloc_slice[*index..*index + len])
            }
            RootSrcOrBuffer::NotRoot(vec, pe) => RootSrcOrSliceMut::NotRoot(vec, *pe),
        }
    }
}

impl ScatterInputInner {
    #[allow(dead_code)] // consumed only by libfabric/ucx-family fabric.rs, feature-gated out of default build
    pub(crate) fn as_slice<'a, T>(
        &'a self,
        alloc_slice: &'a [T],
        len: usize,
    ) -> RootSrcSliceOrNone<'a, T> {
        match self {
            ScatterInputInner::Root(index) => {
                RootSrcSliceOrNone::Root(&alloc_slice[*index..*index + len])
            }
            ScatterInputInner::NotRoot(pe) => RootSrcSliceOrNone::NotRoot(*pe),
        }
    }
}

impl<T> RootOrBuffer<T> {
    #[allow(dead_code)] // consumed only by libfabric/ucx-family fabric.rs, feature-gated out of default build
    pub(crate) fn as_mut_slice<'a>(&'a mut self) -> RootOrSliceMut<'a, T> {
        match self {
            RootOrBuffer::Root(items) => RootOrSliceMut::Root(items),
            RootOrBuffer::NotRoot(pe) => RootOrSliceMut::NotRoot(*pe),
        }
    }
}

/// Input type for gather-to-root collective operations, providing the root PE's destination buffer or a non-root PE's root index.
pub enum RootOrLamellarBuffer<T: Remote, B: AsLamellarBuffer<T>> {
    /// This PE is the root; buffer to gather results into.
    Root(LamellarBuffer<T, B>),
    /// This PE is not the root; `usize` is the PE index of the root.
    NotRoot(usize),
}

impl<T: Remote, B: AsLamellarBuffer<T>> RootOrLamellarBuffer<T, B> {
    #[allow(dead_code)] // consumed only by libfabric/ucx-family fabric.rs, feature-gated out of default build
    pub(crate) fn as_mut_slice<'a>(&'a mut self) -> RootOrSliceMut<'a, T> {
        match self {
            RootOrLamellarBuffer::Root(lamellar_buffer) => {
                RootOrSliceMut::Root(lamellar_buffer.as_mut_slice())
            }
            RootOrLamellarBuffer::NotRoot(pe) => RootOrSliceMut::NotRoot(*pe),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> RootSrcOrLamellarBufferInner<T, B> {
    #[allow(dead_code)] // consumed only by libfabric/ucx-family fabric.rs, feature-gated out of default build
    pub(crate) fn as_mut_slice<'a>(
        &'a mut self,
        alloc_slice: &'a mut [T],
        len: usize,
    ) -> RootSrcOrSliceMut<'a, T> {
        match self {
            RootSrcOrLamellarBufferInner::Root(index) => {
                RootSrcOrSliceMut::Root(&mut alloc_slice[*index..*index + len])
            }
            RootSrcOrLamellarBufferInner::NotRoot(lamellar_buffer, pe) => {
                let buf_slice = lamellar_buffer.as_mut_slice();
                if buf_slice.len() < len {
                    panic!("RootSrcOrLamellarBufferInner::NotRoot buffer size {} is smaller than required len {}", buf_slice.len(), len);
                }
                RootSrcOrSliceMut::NotRoot(&mut buf_slice[..len], *pe)
            }
        }
    }
}

#[allow(dead_code)] // consumed only by libfabric/ucx-family fabric.rs, feature-gated out of default build
pub(crate) enum RootOrSliceMut<'a, T> {
    Root(&'a mut [T]),
    NotRoot(usize),
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
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        op: ReduceOp,
    ) -> CollectiveAllReduceOpHandle<T>;

    fn reduce_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        op: ReduceOp,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceIntoBufferOpHandle<T, B>;

    fn reduce_all_in_place<T: Remote, B: AsLamellarBuffer<T>>(
        &self, // TODO: This should probably take a multiple reference to self.
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src_and_dst: LamellarBuffer<T, B>,
        op: ReduceOp,
    ) -> CollectiveAllReduceInPlaceOpHandle<T, B>;
}

pub(crate) trait CommAllocCollectiveReduce {
    fn reduce<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: ReduceOp,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveReduceOpHandle<T>;

    fn reduce_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: ReduceOp,
        index: usize,
        len: usize,
        root_or_buffer: RootOrLamellarBuffer<T, B>,
    ) -> CollectiveReduceIntoBufferOpHandle<T, B>;

    // fn reduce_in_place<T: Remote>(
    //     &self,
    //     scheduler: &Arc<Scheduler>,
    //     counters: Option<Arc<[Arc<AMCounters>]>>,
    //     op: ReduceOp,
    //     root_pe: usize,
    // ) -> CollectiveReduceInPlaceOpHandle<T>;
}

pub(crate) trait CommAllocCollectiveAllGather {
    fn gather_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllGatherOpHandle<T>;
    fn gather_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllGatherIntoBufferOpHandle<T, B>;
}

pub(crate) trait CommAllocCollectiveGather {
    fn gather<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveGatherOpHandle<T>;
    fn gather_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        root_or_buffer: RootOrLamellarBuffer<T, B>,
    ) -> CollectiveGatherIntoBufferOpHandle<T, B>;
}

pub(crate) trait CommAllocCollectiveAllToAll {
    fn alltoall<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllToAllOpHandle<T>;
    fn alltoall_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllToAllIntoBufferOpHandle<T, B>;
}

pub(crate) trait CommAllocCollectiveBroadcast {
    fn broadcast<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src_or_root_pe: BroadcastInput,
        len: usize,
    ) -> CollectiveBroadcastOpHandle<T>;
    fn broadcast_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        dst: RootSrcOrLamellarBuffer<T, B>,
        len: usize,
    ) -> CollectiveBroadcastIntoBufferOpHandle<T, B>;
}

pub(crate) trait CommAllocCollectiveScatter {
    fn scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterOpHandle<T>;
    fn scatter_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        dst: LamellarBuffer<T, B>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterIntoBufferOpHandle<T, B>;
}

pub(crate) trait CommAllocCollectiveReduceScatter {
    fn reduce_scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: ReduceOp,
        index: usize,
        len: usize,
    ) -> CollectiveReduceScatterOpHandle<T>;

    fn reduce_scatter_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: ReduceOp,
        index: usize,
        len: usize,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveReduceScatterIntoBufferOpHandle<T, B>;

    // fn reduce_scatter_in_place<T: Remote, B: AsLamellarBuffer<T>>(
    //     &self, // TODO: This should probably take a multiple reference to self.
    //     scheduler: &Arc<Scheduler>,
    //     counters: Option<Arc<[Arc<AMCounters>]>>,
    //     src_and_dst: LamellarBuffer<T, B>,
    //     op: ReduceOp,
    // ) -> CollectiveAllReduceInPlaceOpHandle<T, B>;
}

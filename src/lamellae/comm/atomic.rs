#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabric_lamellae::atomic::{
    LibfabricAtomicFetchFuture, LibfabricAtomicFuture,
};
use crate::{
    array::operations::handle::ResultOpState,
    lamellae::{
        local_lamellae::atomic::{LocalAtomicFetchFuture, LocalAtomicFuture},
        shmem_lamellae::atomic::{ShmemAtomicFetchFuture, ShmemAtomicFuture},
        CommAllocAddr, CommSlice,
    },
    LamellarTask,
};

use super::{AMCounters, Remote, Scheduler};

use async_std::fs::write;
use enum_dispatch::enum_dispatch;
use futures_util::Future;
use pin_project::pin_project;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
pub(crate) trait NetworkAtomic {
    fn supported() -> bool {
        false
    }
}
impl NetworkAtomic for u8 {
    fn supported() -> bool {
        true
    }
}
impl NetworkAtomic for u16 {
    fn supported() -> bool {
        true
    }
}
impl NetworkAtomic for u32 {
    fn supported() -> bool {
        true
    }
}
impl NetworkAtomic for u64 {
    fn supported() -> bool {
        true
    }
}
impl NetworkAtomic for usize {
    fn supported() -> bool {
        true
    }
}
impl NetworkAtomic for i8 {
    fn supported() -> bool {
        true
    }
}
impl NetworkAtomic for i16 {
    fn supported() -> bool {
        true
    }
}
impl NetworkAtomic for i32 {
    fn supported() -> bool {
        true
    }
}
impl NetworkAtomic for i64 {
    fn supported() -> bool {
        true
    }
}
impl NetworkAtomic for isize {
    fn supported() -> bool {
        true
    }
}

impl NetworkAtomic for () {}

impl<T> NetworkAtomic for &T {}

/// A task handle for raw RMDA (put/get) operation
#[must_use = " AtomicOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct AtomicOpHandle<T> {
    #[pin]
    pub(crate) future: AtomicOpFuture<T>,
}

#[pin_project(project = AtomicOpFutureProj)]
pub(crate) enum AtomicOpFuture<T> {
    #[cfg(feature = "rofi-c")]
    RofiC(#[pin] RofiCFuture<T>),
    #[cfg(feature = "enable-rofi-rust")]
    RofiRust(#[pin] RofiRustFuture),
    #[cfg(feature = "enable-rofi-rust")]
    RofiRustAsync(#[pin] RofiRustAsyncFuture),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricAtomicFuture<T>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricAsync(#[pin] LibfabricAsyncFuture),
    Shmem(#[pin] ShmemAtomicFuture<T>),
    Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Send + 'static> AtomicOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "rofi-c")]
            AtomicOpFuture::RofiC(f) => f.block(),
            #[cfg(feature = "enable-rofi-rust")]
            AtomicOpFuture::RofiRust(f) => f.block(),
            #[cfg(feature = "enable-rofi-rust")]
            AtomicOpFuture::RofiRustAsync(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            AtomicOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicOpFuture::LibfabricAsync(f) => f.block(),
            AtomicOpFuture::Shmem(f) => f.block(),
            AtomicOpFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) AtomicOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        match self.future {
            #[cfg(feature = "rofi-c")]
            AtomicOpFuture::RofiC(f) => f.spawn(),
            #[cfg(feature = "enable-rofi-rust")]
            AtomicOpFuture::RofiRust(f) => f.spawn(),
            #[cfg(feature = "enable-rofi-rust")]
            AtomicOpFuture::RofiRustAsync(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            AtomicOpFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicOpFuture::LibfabricAsync(f) => f.spawn(),
            AtomicOpFuture::Shmem(f) => f.spawn(),
            AtomicOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Send + 'static> Future for AtomicOpHandle<T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "rofi-c")]
            AtomicOpFutureProj::RofiC(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-rust")]
            AtomicOpFutureProj::RofiRust(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-rust")]
            AtomicOpFutureProj::RofiRustAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            AtomicOpFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            AtomicOpFutureProj::Shmem(f) => f.poll(cx),
            AtomicOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[must_use = " AtomicFetchOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct AtomicFetchOpHandle<T> {
    #[pin]
    pub(crate) future: AtomicFetchOpFuture<T>,
}

#[pin_project(project = AtomicFetchOpFutureProj)]
pub(crate) enum AtomicFetchOpFuture<T> {
    #[cfg(feature = "rofi-c")]
    RofiC(#[pin] RofiCFuture<T>),
    #[cfg(feature = "enable-rofi-rust")]
    RofiRust(#[pin] RofiRustFuture),
    #[cfg(feature = "enable-rofi-rust")]
    RofiRustAsync(#[pin] RofiRustAsyncFuture),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricAtomicFetchFuture<T>),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricAsync(#[pin] LibfabricAsyncFuture),
    Shmem(#[pin] ShmemAtomicFetchFuture<T>),
    Local(#[pin] LocalAtomicFetchFuture<T>),
}

impl<T: Send + 'static> AtomicFetchOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> T {
        match self.future {
            #[cfg(feature = "rofi-c")]
            AtomicFetchOpFuture::RofiC(f) => f.block(),
            #[cfg(feature = "enable-rofi-rust")]
            AtomicFetchOpFuture::RofiRust(f) => f.block(),
            #[cfg(feature = "enable-rofi-rust")]
            AtomicFetchOpFuture::RofiRustAsync(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            AtomicFetchOpFuture::Libfabric(f) => f.block(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            AtomicFetchOpFuture::Shmem(f) => f.block(),
            AtomicFetchOpFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) AtomicFetchOp Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<T> {
        match self.future {
            #[cfg(feature = "rofi-c")]
            AtomicFetchOpFuture::RofiC(f) => f.spawn(),
            #[cfg(feature = "enable-rofi-rust")]
            AtomicFetchOpFuture::RofiRust(f) => f.spawn(),
            #[cfg(feature = "enable-rofi-rust")]
            AtomicFetchOpFuture::RofiRustAsync(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            AtomicFetchOpFuture::Libfabric(f) => f.spawn(),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Send + 'static> Future for AtomicFetchOpHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "rofi-c")]
            AtomicFetchOpFutureProj::RofiC(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-rust")]
            AtomicFetchOpFutureProj::RofiRust(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-rust")]
            AtomicFetchOpFutureProj::RofiRustAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            AtomicFetchOpFutureProj::Libfabric(f) => f.poll(cx),
            // #[cfg(feature = "enable-libfabric")]
            // AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[derive(Clone)]
pub(crate) enum AtomicOp<T> {
    Min(T),
    Max(T),
    Sum(T),
    Prod(T),
    LogicalOr(T),
    LogicalXor(T),
    LogicalAnd(T),
    BitOr(T),
    BitXor(T),
    BitAnd(T),
    Read,
    Write(T),
    Cas(T, T),
}

impl<T> std::fmt::Debug for AtomicOp<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AtomicOp::Min(_) => write!(f, "Min"),
            AtomicOp::Max(_) => write!(f, "Max"),
            AtomicOp::Sum(_) => write!(f, "Sum"),
            AtomicOp::Prod(_) => write!(f, "Prod"),
            AtomicOp::LogicalOr(_) => write!(f, "LogicalOr"),
            AtomicOp::LogicalXor(_) => write!(f, "LogicalXor"),
            AtomicOp::LogicalAnd(_) => write!(f, "LogicalAnd"),
            AtomicOp::BitOr(_) => write!(f, "BitOr"),
            AtomicOp::BitXor(_) => write!(f, "BitXor"),
            AtomicOp::BitAnd(_) => write!(f, "BitAnd"),
            AtomicOp::Read => write!(f, "Read"),
            AtomicOp::Write(_) => write!(f, "Write"),
            AtomicOp::Cas(_, _) => write!(f, "Cas"),
        }
    }
}

impl<T> AtomicOp<T> {
    pub(crate) fn src(&self) -> Option<&T> {
        match self {
            AtomicOp::Min(slice)
            | AtomicOp::Max(slice)
            | AtomicOp::Sum(slice)
            | AtomicOp::Prod(slice)
            | AtomicOp::LogicalOr(slice)
            | AtomicOp::LogicalXor(slice)
            | AtomicOp::LogicalAnd(slice)
            | AtomicOp::BitOr(slice)
            | AtomicOp::BitXor(slice)
            | AtomicOp::BitAnd(slice)
            | AtomicOp::Write(slice)
            | AtomicOp::Cas(slice, _) => Some(slice),
            AtomicOp::Read => None,
        }
    }

    pub(crate) fn dst(&self) -> Option<&T> {
        match self {
            AtomicOp::Cas(_, slice) => Some(slice),
            _ => None,
        }
    }
}

pub(crate) trait CommAtomic {
    fn atomic_avail<T: 'static>(&self) -> bool
    where
        Self: Sized;

    fn atomic_op<T>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: CommAllocAddr,
    ) -> AtomicOpHandle<T>;
    fn atomic_fetch_op<T>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: CommAllocAddr,
    ) -> AtomicFetchOpHandle<T>;
}

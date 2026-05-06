#[cfg(feature = "enable-libfabric-async")]
use crate::lamellae::libfabric_async_lamellae::atomic::{
    LibfabricAsyncAtomicCompareExchangeFuture, LibfabricAsyncAtomicFetchFuture,
    LibfabricAsyncAtomicFuture,
};
#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabric_lamellae::atomic::{
    LibfabricAtomicCompareExchangeFuture, LibfabricAtomicFetchFuture, LibfabricAtomicFuture,
};
#[cfg(feature = "enable-libfabric-mt")]
use crate::lamellae::libfabric_lamellae_mt::atomic::{
    LibfabricMtAtomicCompareExchangeFuture, LibfabricMtAtomicFetchFuture, LibfabricMtAtomicFuture,
};
#[cfg(feature="enable-libfabric-sys")]
use crate::lamellae::libfabric_sys_lamellae::atomic::{LibfabricSysAtomicCompareExchangeFuture, LibfabricSysAtomicFetchFuture, LibfabricSysAtomicFuture};
#[cfg(feature = "enable-rofi-c")]
use crate::lamellae::rofi_c_lamellae::atomic::RofiCAtomicFuture;
#[cfg(feature = "enable-rofi-c")]
use crate::lamellae::rofi_c_lamellae::atomic::{
    RofiCAtomicCompareExchangeFuture, RofiCAtomicFetchFuture,
};
#[cfg(feature = "enable-ucx")]
use crate::lamellae::ucx_lamellae::atomic::UcxAtomicCompareExchangeFuture;
#[cfg(feature = "enable-ucx")]
use crate::lamellae::ucx_lamellae::atomic::{UcxAtomicFetchFuture, UcxAtomicFuture};
#[cfg(feature = "enable-ucx-mt")]
use crate::lamellae::ucx_lamellae_mt::atomic::UcxMtAtomicCompareExchangeFuture;
#[cfg(feature = "enable-ucx-mt")]
use crate::lamellae::ucx_lamellae_mt::atomic::{UcxMtAtomicFetchFuture, UcxMtAtomicFuture};
use crate::{
    active_messaging::AMCounters,
    lamellae::{
        local_lamellae::atomic::{
            LocalAtomicCompareExchangeFuture, LocalAtomicFetchFuture, LocalAtomicFuture,
        },
        shmem_lamellae::atomic::{
            ShmemAtomicCompareExchangeFuture, ShmemAtomicFetchFuture, ShmemAtomicFuture,
        },
        CommAllocAddr,
    },
    scheduler::Scheduler,
    LamellarTask, Remote,
};

use futures_util::Future;
pub(crate) fn atomic_type_supported<T: 'static>() -> bool {
    let type_id = std::any::TypeId::of::<T>();
    type_id == std::any::TypeId::of::<u8>()
        || type_id == std::any::TypeId::of::<u16>()
        || type_id == std::any::TypeId::of::<u32>()
        || type_id == std::any::TypeId::of::<u64>()
        || type_id == std::any::TypeId::of::<usize>()
        || type_id == std::any::TypeId::of::<i8>()
        || type_id == std::any::TypeId::of::<i16>()
        || type_id == std::any::TypeId::of::<i32>()
        || type_id == std::any::TypeId::of::<i64>()
        || type_id == std::any::TypeId::of::<isize>()
}

use pin_project::pin_project;
use std::{
    cmp::PartialEq,
    pin::Pin,
    sync::{atomic::*, Arc},
    task::{Context, Poll},
};
// pub(crate) trait NetworkAtomic {
//     fn supported() -> bool {
//         false
//     }
// }
// impl NetworkAtomic for u8 {
//     fn supported() -> bool {
//         true
//     }
// }
// impl NetworkAtomic for u16 {
//     fn supported() -> bool {
//         true
//     }
// }
// impl NetworkAtomic for u32 {
//     fn supported() -> bool {
//         true
//     }
// }
// impl NetworkAtomic for u64 {
//     fn supported() -> bool {
//         true
//     }
// }
// impl NetworkAtomic for usize {
//     fn supported() -> bool {
//         true
//     }
// }
// impl NetworkAtomic for i8 {
//     fn supported() -> bool {
//         true
//     }
// }
// impl NetworkAtomic for i16 {
//     fn supported() -> bool {
//         true
//     }
// }
// impl NetworkAtomic for i32 {
//     fn supported() -> bool {
//         true
//     }
// }
// impl NetworkAtomic for i64 {
//     fn supported() -> bool {
//         true
//     }
// }
// impl NetworkAtomic for isize {
//     fn supported() -> bool {
//         true
//     }
// }

// impl NetworkAtomic for () {}

// impl<T> NetworkAtomic for &T {}

/// A task handle for raw RMDA (put/get) operation
#[must_use = " AtomicOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct AtomicOpHandle<T> {
    #[pin]
    pub(crate) future: AtomicOpFuture<T>,
}

#[pin_project(project = AtomicOpFutureProj)]
pub(crate) enum AtomicOpFuture<T> {
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(#[pin] LibfabricSysAtomicFuture<T>),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricAtomicFuture<T>),
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxAtomicFuture<T>),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtAtomicFuture<T>),
    #[cfg(feature = "enable-rofi-c")]
    RofiC(#[pin] RofiCAtomicFuture<T>),
    Shmem(#[pin] ShmemAtomicFuture<T>),
    Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote> AtomicOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            AtomicOpFuture::LibfabricSys(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            AtomicOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-mt")]
            AtomicOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            AtomicOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            AtomicOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx-mt")]
            AtomicOpFuture::UcxMt(f) => f.block(),
            #[cfg(feature = "enable-rofi-c")]
            AtomicOpFuture::RofiC(f) => f.block(),
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
            #[cfg(feature = "enable-libfabric-sys")]
            AtomicOpFuture::LibfabricSys(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            AtomicOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-mt")]
            AtomicOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            AtomicOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            AtomicOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx-mt")]
            AtomicOpFuture::UcxMt(f) => f.spawn(),
            #[cfg(feature = "enable-rofi-c")]
            AtomicOpFuture::RofiC(f) => f.spawn(),
            AtomicOpFuture::Shmem(f) => f.spawn(),
            AtomicOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote> Future for AtomicOpHandle<T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric-sys")]
            AtomicOpFutureProj::LibfabricSys(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            AtomicOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-mt")]
            AtomicOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            AtomicOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            AtomicOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            AtomicOpFutureProj::UcxMt(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-c")]
            AtomicOpFutureProj::RofiC(f) => f.poll(cx),
            AtomicOpFutureProj::Shmem(f) => f.poll(cx),
            AtomicOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}

/// A task handle for a remote atomic fetch-and-modify operation (e.g. `fetch_add`, `fetch_and`, etc.)
/// that returns the previous value of the modified element.
///
/// Can be awaited as a `Future`, [spawned][AtomicFetchOpHandle::spawn] onto the work queue,
/// or [blocked on][AtomicFetchOpHandle::block] from a synchronous context.
#[must_use = " AtomicFetchOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct AtomicFetchOpHandle<T> {
    #[pin]
    pub(crate) future: AtomicFetchOpFuture<T>,
}

#[pin_project(project = AtomicFetchOpFutureProj)]
pub(crate) enum AtomicFetchOpFuture<T> {
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(#[pin] LibfabricSysAtomicFetchFuture<T>),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricAtomicFetchFuture<T>),
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtAtomicFetchFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncAtomicFetchFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxAtomicFetchFuture<T>),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtAtomicFetchFuture<T>),
    #[cfg(feature = "enable-rofi-c")]
    RofiC(#[pin] RofiCAtomicFetchFuture<T>),
    Shmem(#[pin] ShmemAtomicFetchFuture<T>),
    Local(#[pin] LocalAtomicFetchFuture<T>),
}

impl<T: Remote> AtomicFetchOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> T {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            AtomicFetchOpFuture::LibfabricSys(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            AtomicFetchOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-mt")]
            AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            AtomicFetchOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx-mt")]
            AtomicFetchOpFuture::UcxMt(f) => f.block(),
            #[cfg(feature = "enable-rofi-c")]
            AtomicFetchOpFuture::RofiC(f) => f.block(),
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
            #[cfg(feature = "enable-libfabric-sys")]
            AtomicFetchOpFuture::LibfabricSys(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            AtomicFetchOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-mt")]
            AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            AtomicFetchOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx-mt")]
            AtomicFetchOpFuture::UcxMt(f) => f.spawn(),
            #[cfg(feature = "enable-rofi-c")]
            AtomicFetchOpFuture::RofiC(f) => f.spawn(),
            AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote> Future for AtomicFetchOpHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric-sys")]
            AtomicFetchOpFutureProj::LibfabricSys(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            AtomicFetchOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-mt")]
            AtomicFetchOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            AtomicFetchOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            AtomicFetchOpFutureProj::UcxMt(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-c")]
            AtomicFetchOpFutureProj::RofiC(f) => f.poll(cx),
            AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}

/// A task handle for a remote atomic compare-and-exchange operation.
///
/// Resolves to `Ok(previous_value)` if the exchange succeeded (the current value matched `old`)
/// or `Err(current_value)` if it failed (the current value did not match `old`).
///
/// Can be awaited as a `Future`, [spawned][AtomicCompareExchangeOpHandle::spawn] onto the work queue,
/// or [blocked on][AtomicCompareExchangeOpHandle::block] from a synchronous context.
#[must_use = " AtomicCompareExchangeOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub struct AtomicCompareExchangeOpHandle<T> {
    #[pin]
    pub(crate) future: AtomicCompareExchangeFuture<T>,
}

#[pin_project(project = AtomicCompareExchangeFutureProj)]
pub(crate) enum AtomicCompareExchangeFuture<T> {
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(#[pin] LibfabricSysAtomicCompareExchangeFuture<T>),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricAtomicCompareExchangeFuture<T>),
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(#[pin] LibfabricMtAtomicCompareExchangeFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncAtomicCompareExchangeFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxAtomicCompareExchangeFuture<T>),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(#[pin] UcxMtAtomicCompareExchangeFuture<T>),
    #[cfg(feature = "enable-rofi-c")]
    RofiC(#[pin] RofiCAtomicCompareExchangeFuture<T>),
    Shmem(#[pin] ShmemAtomicCompareExchangeFuture<T>),
    Local(#[pin] LocalAtomicCompareExchangeFuture<T>),
}

impl<T: Remote + PartialEq> AtomicCompareExchangeOpHandle<T> {
    /// This method will block the calling thread until the associated AtomicCompareExchange Operation completes.
    ///
    /// Returns `Ok(previous_value)` if the exchange succeeded, or `Err(current_value)` if it failed.
    pub fn block(self) -> Result<T, T> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            AtomicCompareExchangeFuture::LibfabricSys(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            AtomicCompareExchangeFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric-mt")]
            AtomicCompareExchangeFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            AtomicCompareExchangeFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            AtomicCompareExchangeFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx-mt")]
            AtomicCompareExchangeFuture::UcxMt(f) => f.block(),
            #[cfg(feature = "enable-rofi-c")]
            AtomicCompareExchangeFuture::RofiC(f) => f.block(),
            AtomicCompareExchangeFuture::Shmem(f) => f.block(),
            AtomicCompareExchangeFuture::Local(f) => f.block(),
        }
    }

    /// This method will spawn the associated (raw) AtomicCompareExchange Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete.
    /// Resolves to `Ok(previous_value)` on success or `Err(current_value)` on failure.
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<Result<T, T>> {
        match self.future {
            #[cfg(feature = "enable-libfabric-sys")]
            AtomicCompareExchangeFuture::LibfabricSys(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric")]
            AtomicCompareExchangeFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-mt")]
            AtomicCompareExchangeFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            AtomicCompareExchangeFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            AtomicCompareExchangeFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx-mt")]
            AtomicCompareExchangeFuture::UcxMt(f) => f.spawn(),
            #[cfg(feature = "enable-rofi-c")]
            AtomicCompareExchangeFuture::RofiC(f) => f.spawn(),
            AtomicCompareExchangeFuture::Shmem(f) => f.spawn(),
            AtomicCompareExchangeFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Remote + PartialEq> Future for AtomicCompareExchangeOpHandle<T> {
    type Output = Result<T, T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric-sys")]
            AtomicCompareExchangeFutureProj::LibfabricSys(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            AtomicCompareExchangeFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-mt")]
            AtomicCompareExchangeFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            AtomicCompareExchangeFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            AtomicCompareExchangeFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx-mt")]
            AtomicCompareExchangeFutureProj::UcxMt(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-c")]
            AtomicCompareExchangeFutureProj::RofiC(f) => f.poll(cx),
            AtomicCompareExchangeFutureProj::Shmem(f) => f.poll(cx),
            AtomicCompareExchangeFutureProj::Local(f) => f.poll(cx),
        }
    }
}

// we need to pin box the values in the AtomicOp because these will be passed as input to the remote operation, and their address need to remain stable for the duration of the operation,
#[derive(Clone)]
pub(crate) enum AtomicOp<T> {
    // Non-fetch operations
    Min(Pin<Box<T>>),
    Max(Pin<Box<T>>),
    Sum(Pin<Box<T>>),
    Sub(Pin<Box<T>>), // backends only expose a sum/add op, we expose subtraction via negating the value and using sum/add
    Prod(Pin<Box<T>>),
    BitOr(Pin<Box<T>>),
    BitXor(Pin<Box<T>>),
    BitAnd(Pin<Box<T>>),
    Write(Pin<Box<T>>),
    // Fetch operations (return the old value)
    FetchMin(Pin<Box<T>>),
    FetchMax(Pin<Box<T>>),
    FetchSum(Pin<Box<T>>),
    FetchSub(Pin<Box<T>>),
    FetchProd(Pin<Box<T>>),
    FetchBitOr(Pin<Box<T>>),
    FetchBitXor(Pin<Box<T>>),
    FetchBitAnd(Pin<Box<T>>),
    Read(Pin<Box<T>>), // ucx requires us to do a fetch-add with an addend of 0 in order to do a remote read, so we can use the FetchSum variant with an addend of 0 for that case, but we want to be able to distinguish that from an actual fetch-add operation where the addend is 0, so we have a separate Read variant for that case
    // Compare-and-swap
    Cas(Pin<Box<T>>, Pin<Box<T>>),
}

impl<T> std::fmt::Debug for AtomicOp<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AtomicOp::Min(_) => write!(f, "Min"),
            AtomicOp::Max(_) => write!(f, "Max"),
            AtomicOp::Sum(_) => write!(f, "Sum"),
            AtomicOp::Sub(_) => write!(f, "Sub"),
            AtomicOp::Prod(_) => write!(f, "Prod"),
            AtomicOp::BitOr(_) => write!(f, "BitOr"),
            AtomicOp::BitXor(_) => write!(f, "BitXor"),
            AtomicOp::BitAnd(_) => write!(f, "BitAnd"),
            AtomicOp::Write(_) => write!(f, "Write"),
            AtomicOp::FetchMin(_) => write!(f, "FetchMin"),
            AtomicOp::FetchMax(_) => write!(f, "FetchMax"),
            AtomicOp::FetchSum(_) => write!(f, "FetchSum"),
            AtomicOp::FetchSub(_) => write!(f, "FetchSub"),
            AtomicOp::FetchProd(_) => write!(f, "FetchProd"),
            AtomicOp::FetchBitOr(_) => write!(f, "FetchBitOr"),
            AtomicOp::FetchBitXor(_) => write!(f, "FetchBitXor"),
            AtomicOp::FetchBitAnd(_) => write!(f, "FetchBitAnd"),
            AtomicOp::Read(_) => write!(f, "Read"),
            AtomicOp::Cas(_, _) => write!(f, "Cas"),
        }
    }
}

impl<T> AtomicOp<T> {
    pub(crate) fn src(&self) -> *const T {
        match self {
            AtomicOp::Min(val)
            | AtomicOp::Max(val)
            | AtomicOp::Sum(val)
            | AtomicOp::Sub(val)
            | AtomicOp::Prod(val)
            | AtomicOp::BitOr(val)
            | AtomicOp::BitXor(val)
            | AtomicOp::BitAnd(val)
            | AtomicOp::Write(val)
            | AtomicOp::FetchMin(val)
            | AtomicOp::FetchMax(val)
            | AtomicOp::FetchSum(val)
            | AtomicOp::FetchSub(val)
            | AtomicOp::FetchProd(val)
            | AtomicOp::FetchBitOr(val)
            | AtomicOp::FetchBitXor(val)
            | AtomicOp::FetchBitAnd(val)
            | AtomicOp::Read(val)
            | AtomicOp::Cas(val, _) => val.as_ref().get_ref(),
        }
    }
}

pub(crate) trait CommAllocAtomic {
    fn atomic_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T>;
    fn atomic_op_blocking<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    );
    fn atomic_op_unmanaged<T: Remote>(&self, op: AtomicOp<T>, pe: usize, offset: usize);
    fn atomic_op_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T>;
    fn atomic_op_all_unmanaged<T: Remote>(&self, op: AtomicOp<T>, offset: usize);
    fn atomic_fetch_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicFetchOpHandle<T>;
    fn atomic_fetch_op_blocking<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> T;
    fn atomic_compare_exchange<T: Remote + PartialEq>(
        &self,
        _scheduler: &Arc<Scheduler>,
        _counters: Vec<Arc<AMCounters>>,
        _current: T,
        _new: T,
        _pe: usize,
        _offset: usize,
    ) -> AtomicCompareExchangeOpHandle<T> {
        panic!("atomic_compare_exchange not supported for this backend")
    }
    fn atomic_compare_exchange_blocking<T: Remote + PartialEq>(
        &self,
        _scheduler: &Arc<Scheduler>,
        _current: T,
        _new: T,
        _pe: usize,
        _offset: usize,
    ) -> Result<T, T> {
        panic!("atomic_compare_exchange_blocking not supported for this backend")
    }
}

pub(crate) trait AsAtomic: Copy + std::fmt::Debug {
    fn load(&self) -> Self;
    fn store(&mut self, val: Self);
    fn swap(&mut self, val: Self) -> Self;
    fn fetch_add(&mut self, val: Self) -> Self;
    fn fetch_mul(&mut self, val: Self) -> Self;
    fn fetch_sub(&mut self, val: Self) -> Self;
    fn fetch_and(&mut self, val: Self) -> Self;
    fn fetch_or(&mut self, val: Self) -> Self;
    fn fetch_xor(&mut self, val: Self) -> Self;
    fn fetch_max(&mut self, val: Self) -> Self;
    fn fetch_min(&mut self, val: Self) -> Self;
    fn compare_exchange(&mut self, current: Self, new: Self) -> Result<Self, Self>;
}

//create a macro the implements AsAtomic for all the primitive integer types
macro_rules! impl_as_atomic {
    ($(($t:ty,$a:ty)),*) => {
        $(
            impl AsAtomic for $t {
                fn load(&self) -> Self {
                    let atomic = unsafe { &*(self as *const $t as *const $a) };
                    atomic.load(Ordering::SeqCst)
                }
                fn store(&mut self, val: Self) {
                    let atomic = unsafe { &*(self as *mut $t as *mut $a) };
                    atomic.store(val, Ordering::SeqCst);
                    // println!("store called {:?} {:?}", val, atomic as *const $a);
                }
                fn swap(&mut self, val: Self) -> Self {
                    let atomic = unsafe { &*(self as *mut $t as *mut $a) };
                    atomic.swap(val, Ordering::SeqCst)
                }
                fn fetch_add(&mut self, val: Self) -> Self {
                    let atomic = unsafe { &*(self as *mut $t as *mut $a) };
                    atomic.fetch_add(val, Ordering::SeqCst)
                }
                fn fetch_mul(&mut self, val: Self) -> Self {
                    let atomic = unsafe { &*(self as *mut $t as *mut $a) };
                    let mut cur = atomic.load(Ordering::SeqCst);
                    let mut new = cur * val;
                    while atomic
                        .compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                    {
                        std::thread::yield_now();
                        cur = atomic.load(Ordering::SeqCst);
                        new = cur * val;
                    }
                    cur
                }
                fn fetch_sub(&mut self, val: Self) -> Self {
                    let atomic = unsafe { &*(self as *mut $t as *mut $a) };
                    atomic.fetch_sub(val, Ordering::SeqCst)
                }
                fn fetch_and(&mut self, val: Self) -> Self {
                    let atomic = unsafe { &*(self as *mut $t as *mut $a) };
                    atomic.fetch_and(val, Ordering::SeqCst)
                }
                fn fetch_or(&mut self, val: Self) -> Self {
                    let atomic = unsafe { &*(self as *mut $t as *mut $a) };
                    atomic.fetch_or(val, Ordering::SeqCst)
                }
                fn fetch_xor(&mut self, val: Self) -> Self {
                    let atomic = unsafe { &*(self as *mut $t as *mut $a) };
                    atomic.fetch_xor(val, Ordering::SeqCst)
                }
                fn fetch_max(&mut self, val: Self) -> Self {
                    let atomic = unsafe { &*(self as *mut $t as *mut $a) };
                    atomic.fetch_max(val, Ordering::SeqCst)
                }
                fn fetch_min(&mut self, val: Self) -> Self {
                    let atomic = unsafe { &*(self as *mut $t as *mut $a) };
                    atomic.fetch_min(val, Ordering::SeqCst)
                }
                fn compare_exchange(&mut self, current: Self, new: Self) -> Result<Self, Self> {
                    let atomic = unsafe { &*(self as *mut $t as *mut $a) };
                    atomic.compare_exchange(current, new, Ordering::SeqCst, Ordering::SeqCst)
                }
            }
        )*
    };
}

impl_as_atomic!(
    (u8, AtomicU8),
    (u16, AtomicU16),
    (u32, AtomicU32),
    (u64, AtomicU64),
    (usize, AtomicUsize),
    (i8, AtomicI8),
    (i16, AtomicI16),
    (i32, AtomicI32),
    (i64, AtomicI64),
    (isize, AtomicIsize)
);

//TODO maybe I need to change this to mutable reference? so that the compiler knows we are changing the data?

pub(crate) fn net_atomic_op<T: 'static>(op: &AtomicOp<T>, dst_addr: &CommAllocAddr) {
    // println!("net_atomic_op called dst_addr: {:x}", dst_addr);
    unsafe {
        if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
            // println!("im here u8");
            typed_atomic_op::<u8, T>(op, &*(dst_addr.as_ptr() as *const u8))
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
            // println!("im here u16");
            typed_atomic_op::<u16, T>(op, &*(dst_addr.as_ptr() as *const u16))
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
            typed_atomic_op::<u32, T>(op, &*(dst_addr.as_ptr() as *const u32))
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
            typed_atomic_op::<u64, T>(op, &*(dst_addr.as_ptr() as *const u64))
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
            typed_atomic_op::<usize, T>(op, &*(dst_addr.as_ptr() as *const usize))
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
            typed_atomic_op::<i8, T>(op, &*(dst_addr.as_ptr() as *const i8))
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
            // println!("im here i16");
            typed_atomic_op::<i16, T>(op, &*(dst_addr.as_ptr() as *const i16))
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
            typed_atomic_op::<i32, T>(op, &*(dst_addr.as_ptr() as *const i32))
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
            // println!("im here i64");
            typed_atomic_op::<i64, T>(op, &*(dst_addr.as_ptr() as *const i64))
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
            typed_atomic_op::<isize, T>(op, &*(dst_addr.as_ptr() as *const isize))
        } else {
            panic!("Unsupported atomic operation type");
        }
    }
}

pub(crate) fn net_atomic_fetch_op<T: 'static>(
    op: &AtomicOp<T>,
    dst_addr: &CommAllocAddr,
    result: *mut T,
) {
    unsafe {
        // println!("dst_addr: {:?}", (dst_addr.as_ptr() as *const usize).read());
        if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
            typed_atomic_fetch_op::<u8, T>(op, &*(dst_addr.as_ptr() as *const u8), result)
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
            typed_atomic_fetch_op::<u16, T>(op, &*(dst_addr.as_ptr() as *const u16), result)
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
            typed_atomic_fetch_op::<u32, T>(op, &*(dst_addr.as_ptr() as *const u32), result)
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
            typed_atomic_fetch_op::<u64, T>(op, &*(dst_addr.as_ptr() as *const u64), result)
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
            typed_atomic_fetch_op::<usize, T>(op, &*(dst_addr.as_ptr() as *const usize), result)
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
            typed_atomic_fetch_op::<i8, T>(op, &*(dst_addr.as_ptr() as *const i8), result)
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
            typed_atomic_fetch_op::<i16, T>(op, &*(dst_addr.as_ptr() as *const i16), result)
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
            typed_atomic_fetch_op::<i32, T>(op, &*(dst_addr.as_ptr() as *const i32), result)
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
            typed_atomic_fetch_op::<i64, T>(op, &*(dst_addr.as_ptr() as *const i64), result)
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
            typed_atomic_fetch_op::<isize, T>(op, &*(dst_addr.as_ptr() as *const isize), result)
        } else {
            panic!("Unsupported atomic operation type");
        }
    }
}

pub(crate) fn net_atomic_compare_exchange<T: Copy + 'static>(
    current: T,
    new: T,
    dst_addr: &CommAllocAddr,
) -> Result<T, T> {
    unsafe {
        if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
            typed_atomic_compare_exchange::<u8, T>(current, new, &*(dst_addr.as_ptr() as *const u8))
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
            typed_atomic_compare_exchange::<u16, T>(
                current,
                new,
                &*(dst_addr.as_ptr() as *const u16),
            )
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
            typed_atomic_compare_exchange::<u32, T>(
                current,
                new,
                &*(dst_addr.as_ptr() as *const u32),
            )
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
            typed_atomic_compare_exchange::<u64, T>(
                current,
                new,
                &*(dst_addr.as_ptr() as *const u64),
            )
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
            typed_atomic_compare_exchange::<usize, T>(
                current,
                new,
                &*(dst_addr.as_ptr() as *const usize),
            )
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
            typed_atomic_compare_exchange::<i8, T>(current, new, &*(dst_addr.as_ptr() as *const i8))
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
            typed_atomic_compare_exchange::<i16, T>(
                current,
                new,
                &*(dst_addr.as_ptr() as *const i16),
            )
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
            typed_atomic_compare_exchange::<i32, T>(
                current,
                new,
                &*(dst_addr.as_ptr() as *const i32),
            )
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
            typed_atomic_compare_exchange::<i64, T>(
                current,
                new,
                &*(dst_addr.as_ptr() as *const i64),
            )
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
            typed_atomic_compare_exchange::<isize, T>(
                current,
                new,
                &*(dst_addr.as_ptr() as *const isize),
            )
        } else {
            panic!("Unsupported atomic operation type");
        }
    }
}

unsafe fn typed_atomic_op<A: AsAtomic, T>(op: &AtomicOp<T>, dst: *const A) {
    let op = std::mem::transmute::<&AtomicOp<T>, &AtomicOp<A>>(op);
    match op {
        AtomicOp::Write(val) => {
            (&mut *(dst as *mut A)).store(**val);
        }
        AtomicOp::Sum(val) => {
            (&mut *(dst as *mut A)).fetch_add(**val);
        }
        AtomicOp::Sub(val) => {
            (&mut *(dst as *mut A)).fetch_sub(**val);
        }
        AtomicOp::Prod(val) => {
            (&mut *(dst as *mut A)).fetch_mul(**val);
        }
        AtomicOp::BitOr(val) => {
            (&mut *(dst as *mut A)).fetch_or(**val);
        }
        AtomicOp::BitXor(val) => {
            (&mut *(dst as *mut A)).fetch_xor(**val);
        }
        AtomicOp::BitAnd(val) => {
            (&mut *(dst as *mut A)).fetch_and(**val);
        }
        AtomicOp::Read(_) => {
            panic!("Read atomic op not supported in this context");
        }
        AtomicOp::Cas(_, _) => {
            panic!("Cas atomic op not supported in this context");
        }
        AtomicOp::Min(_) | AtomicOp::Max(_) => {
            panic!("Min/Max atomic ops not supported in this context");
        }
        AtomicOp::FetchMin(_)
        | AtomicOp::FetchMax(_)
        | AtomicOp::FetchSum(_)
        | AtomicOp::FetchSub(_)
        | AtomicOp::FetchProd(_)
        | AtomicOp::FetchBitOr(_)
        | AtomicOp::FetchBitXor(_)
        | AtomicOp::FetchBitAnd(_) => {
            panic!("Fetch atomic ops must use the fetch path");
        }
    }
}

unsafe fn typed_atomic_fetch_op<A: AsAtomic, T>(op: &AtomicOp<T>, dst: *const A, result: *mut T) {
    let op = std::mem::transmute::<&AtomicOp<T>, &AtomicOp<A>>(op);
    let res = match op {
        AtomicOp::FetchMin(val) => (&mut *(dst as *mut A)).fetch_min(**val),
        AtomicOp::FetchMax(val) => (&mut *(dst as *mut A)).fetch_max(**val),
        AtomicOp::FetchSum(val) => (&mut *(dst as *mut A)).fetch_add(**val),
        AtomicOp::FetchSub(val) => (&mut *(dst as *mut A)).fetch_sub(**val),
        AtomicOp::FetchProd(val) => (&mut *(dst as *mut A)).fetch_mul(**val),
        AtomicOp::FetchBitOr(val) => (&mut *(dst as *mut A)).fetch_or(**val),
        AtomicOp::FetchBitXor(val) => (&mut *(dst as *mut A)).fetch_xor(**val),
        AtomicOp::FetchBitAnd(val) => (&mut *(dst as *mut A)).fetch_and(**val),
        AtomicOp::Read(_) => (&*dst).load(),
        AtomicOp::Write(val) => (&mut *(dst as *mut A)).swap(**val),
        AtomicOp::Cas(_, _) => panic!("Cas atomic op not supported in this context"),
        // Reject non-fetch variants in fetch path
        AtomicOp::Min(_)
        | AtomicOp::Max(_)
        | AtomicOp::Sum(_)
        | AtomicOp::Sub(_)
        | AtomicOp::Prod(_)
        | AtomicOp::BitOr(_)
        | AtomicOp::BitXor(_)
        | AtomicOp::BitAnd(_) => {
            panic!("Non-fetch atomic ops must use the non-fetch path");
        }
    };
    (result as *mut A).write(res);
}

unsafe fn typed_atomic_compare_exchange<A: AsAtomic, T: Copy>(
    current: T,
    new: T,
    dst: *const A,
) -> Result<T, T> {
    let current = *(&current as *const T as *const A);
    let new = *(&new as *const T as *const A);
    match (&mut *(dst as *mut A)).compare_exchange(current, new) {
        Ok(old) => Ok(*(&old as *const A as *const T)),
        Err(old) => Err(*(&old as *const A as *const T)),
    }
}

//TODO compare and swap

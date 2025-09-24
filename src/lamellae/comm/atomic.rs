#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabric_lamellae::atomic::{
    LibfabricAtomicFetchFuture, LibfabricAtomicFuture,
};
#[cfg(feature = "enable-ucx")]
use crate::lamellae::ucx_lamellae::atomic::{UcxAtomicFetchFuture, UcxAtomicFuture};
use crate::{
    lamellae::{
        local_lamellae::atomic::{LocalAtomicFetchFuture, LocalAtomicFuture},
        shmem_lamellae::atomic::{ShmemAtomicFetchFuture, ShmemAtomicFuture},
        CommAllocAddr,
    },
    LamellarTask,
};

use super::{AMCounters, Scheduler};

use futures_util::Future;
use pin_project::pin_project;
use std::{
    pin::Pin,
    sync::{atomic::*, Arc},
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
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxAtomicFuture<T>),
    Shmem(#[pin] ShmemAtomicFuture<T>),
    Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Copy + Send + 'static> AtomicOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            AtomicOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            AtomicOpFuture::Ucx(f) => f.block(),
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
            #[cfg(feature = "enable-libfabric")]
            AtomicOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            AtomicOpFuture::Ucx(f) => f.spawn(),
            AtomicOpFuture::Shmem(f) => f.spawn(),
            AtomicOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Copy + Send + 'static> Future for AtomicOpHandle<T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            AtomicOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            AtomicOpFutureProj::Ucx(f) => f.poll(cx),
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
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricAtomicFetchFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxAtomicFetchFuture<T>),
    Shmem(#[pin] ShmemAtomicFetchFuture<T>),
    Local(#[pin] LocalAtomicFetchFuture<T>),
}

impl<T: Copy + Send + 'static> AtomicFetchOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> T {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            AtomicFetchOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            AtomicFetchOpFuture::Ucx(f) => f.block(),
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
            #[cfg(feature = "enable-libfabric")]
            AtomicFetchOpFuture::Libfabric(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            AtomicFetchOpFuture::Ucx(f) => f.spawn(),
            AtomicFetchOpFuture::Shmem(f) => f.spawn(),
            AtomicFetchOpFuture::Local(f) => f.spawn(),
        }
    }
}

impl<T: Copy + Send + 'static> Future for AtomicFetchOpHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.future.project() {
            #[cfg(feature = "enable-libfabric")]
            AtomicFetchOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            AtomicFetchOpFutureProj::Ucx(f) => f.poll(cx),
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

pub(crate) trait CommAllocAtomic {
    fn atomic_op<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T>;
    fn atomic_op_unmanaged<T: Copy + 'static>(&self, op: AtomicOp<T>, pe: usize, offset: usize);
    fn atomic_op_all<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T>;
    fn atomic_op_all_unmanaged<T: Copy + 'static>(&self, op: AtomicOp<T>, offset: usize);
    fn atomic_fetch_op<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicFetchOpHandle<T>;
}

pub(crate) trait AsAtomic: Copy {
    fn load(&self) -> Self;
    fn store(&self, val: Self);
    fn swap(&self, val: Self) -> Self;
    fn fetch_add(&self, val: Self) -> Self;
    fn fetch_sub(&self, val: Self) -> Self;
    fn fetch_and(&self, val: Self) -> Self;
    fn fetch_nand(&self, val: Self) -> Self;
    fn fetch_or(&self, val: Self) -> Self;
    fn fetch_xor(&self, val: Self) -> Self;
    fn fetch_max(&self, val: Self) -> Self;
    fn fetch_min(&self, val: Self) -> Self;
    fn compare_exchange(&self, current: Self, new: Self) -> Result<Self, Self>
    where
        Self: Sized;
}

//create a macro the implements AsAtomic for all the primitive integer types
macro_rules! impl_as_atomic {
    ($(($t:ty,$a:ty)),*) => {
        $(
            impl AsAtomic for $t {
                fn load(&self) -> Self {
                   unsafe{ (*(self as *const $t as *const $a)).load(Ordering::SeqCst) }
                }
                fn store(&self, val: Self) {
                    unsafe{ (*(self as *const $t as *const $a)).store(val, Ordering::SeqCst) }
                }
                fn swap(&self, val: Self) -> Self {
                    unsafe{ (*(self as *const $t as *const $a)).swap(val, Ordering::SeqCst) }
                }
                fn fetch_add(&self, val: Self) -> Self {
                    unsafe{ (*(self as *const $t as *const $a)).fetch_add(val, Ordering::SeqCst) }
                }
                fn fetch_sub(&self, val: Self) -> Self {
                    unsafe{ (*(self as *const $t as *const $a)).fetch_sub(val, Ordering::SeqCst) }
                }
                fn fetch_and(&self, val: Self) -> Self {
                   unsafe{ (*(self as *const $t as *const $a)).fetch_and(val, Ordering::SeqCst) }
                }
                fn fetch_nand(&self, val: Self) -> Self {
                    unsafe{ (*(self as *const $t as *const $a)).fetch_nand(val, Ordering::SeqCst) }
                }
                fn fetch_or(&self, val: Self) -> Self {
                    unsafe{ (*(self as *const $t as *const $a)).fetch_or(val, Ordering::SeqCst) }
                }
                fn fetch_xor(&self, val: Self) -> Self {
                   unsafe{ (*(self as *const $t as *const $a)).fetch_xor(val, Ordering::SeqCst) }
                }
                fn fetch_max(&self, val: Self) -> Self {
                    unsafe{ (*(self as *const $t as *const $a)).fetch_max(val, Ordering::SeqCst) }
                }
                fn fetch_min(&self, val: Self) -> Self {
                    unsafe{ (*(self as *const $t as *const $a)).fetch_min(val, Ordering::SeqCst) }
                }
                fn compare_exchange(&self, current: Self, new: Self) -> Result<Self, Self> {
                    unsafe{ (*(self as *const $t as *const $a)).compare_exchange(current, new, Ordering::SeqCst , Ordering::Relaxed) }
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

pub(crate) fn net_atomic_op<T: 'static>(op: &AtomicOp<T>, dst_addr: &CommAllocAddr) {
    unsafe {
        if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
            typed_atomic_op::<u8, T>(op, &*(dst_addr.as_ptr() as *const u8))
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
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
            typed_atomic_op::<i16, T>(op, &*(dst_addr.as_ptr() as *const i16))
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
            typed_atomic_op::<i32, T>(op, &*(dst_addr.as_ptr() as *const i32))
        } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
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

unsafe fn typed_atomic_op<A: AsAtomic, T>(op: &AtomicOp<T>, dst: &A) {
    let op = std::mem::transmute::<&AtomicOp<T>, &AtomicOp<A>>(op);
    match op {
        AtomicOp::Write(val) => {
            dst.store(*val);
        }
        _ => {
            unimplemented!()
        }
    }
}

unsafe fn typed_atomic_fetch_op<A: AsAtomic, T>(op: &AtomicOp<T>, dst: &A, result: *mut T) {
    let op = std::mem::transmute::<&AtomicOp<T>, &AtomicOp<A>>(op);
    let res = match op {
        AtomicOp::Read => dst.load(),
        AtomicOp::Write(val) => dst.swap(*val),
        _ => {
            unimplemented!()
        }
    };
    (result as *mut A).write(res);
}

#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabric_lamellae::atomic::{
    LibfabricAtomicFetchFuture, LibfabricAtomicFuture,
};
#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabric_lamellae_mt::atomic::{
    LibfabricMtAtomicFetchFuture, LibfabricMtAtomicFuture,
};
#[cfg(feature = "enable-libfabric-async")]
use crate::lamellae::libfabric_async_lamellae::atomic::{
    LibfabricAsyncAtomicFetchFuture, LibfabricAsyncAtomicFuture,
};
#[cfg(feature = "enable-ucx")]
use crate::lamellae::ucx_lamellae::atomic::{UcxAtomicFetchFuture, UcxAtomicFuture};
#[cfg(feature = "enable-ucx")]
use crate::lamellae::ucx_lamellae_mt::atomic::{UcxMtAtomicFetchFuture, UcxMtAtomicFuture};
use crate::{
    active_messaging::AMCounters,
    lamellae::{
        local_lamellae::atomic::{LocalAtomicFetchFuture, LocalAtomicFuture},
        shmem_lamellae::atomic::{ShmemAtomicFetchFuture, ShmemAtomicFuture},
        CommAllocAddr,
    },
    scheduler::Scheduler,
    LamellarTask, Remote,
};

use futures_util::Future;
use pin_project::pin_project;
use std::{
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
    #[cfg(feature = "enable-libfabric")]
    Libfabric(#[pin] LibfabricAtomicFuture<T>),
    #[cfg(feature = "enable-libfabric")]
    LibfabricMt(#[pin] LibfabricMtAtomicFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxAtomicFuture<T>),
    #[cfg(feature = "enable-ucx")]
    UcxMt(#[pin] UcxMtAtomicFuture<T>),
    Shmem(#[pin] ShmemAtomicFuture<T>),
    Local(#[pin] LocalAtomicFuture<T>),
}

impl<T: Remote> AtomicOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicOp Operation completes
    pub fn block(self) {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            AtomicOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            AtomicOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            AtomicOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            AtomicOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            AtomicOpFuture::UcxMt(f) => f.block(),
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
            #[cfg(feature = "enable-libfabric")]
            AtomicOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            AtomicOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            AtomicOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            AtomicOpFuture::UcxMt(f) => f.spawn(),
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
            #[cfg(feature = "enable-libfabric")]
            AtomicOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            AtomicOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            AtomicOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            AtomicOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            AtomicOpFutureProj::UcxMt(f) => f.poll(cx),
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
    #[cfg(feature = "enable-libfabric")]
    LibfabricMt(#[pin] LibfabricMtAtomicFetchFuture<T>),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(#[pin] LibfabricAsyncAtomicFetchFuture<T>),
    #[cfg(feature = "enable-ucx")]
    Ucx(#[pin] UcxAtomicFetchFuture<T>),
    #[cfg(feature = "enable-ucx")]
    UcxMt(#[pin] UcxMtAtomicFetchFuture<T>),
    Shmem(#[pin] ShmemAtomicFetchFuture<T>),
    Local(#[pin] LocalAtomicFetchFuture<T>),
}

impl<T: Remote> AtomicFetchOpHandle<T> {
    /// This method will block the calling thread until the associated Array AtomicFetchOp Operation completes
    pub fn block(self) -> T {
        match self.future {
            #[cfg(feature = "enable-libfabric")]
            AtomicFetchOpFuture::Libfabric(f) => f.block(),
            #[cfg(feature = "enable-libfabric")]
            AtomicFetchOpFuture::LibfabricMt(f) => f.block(),
            #[cfg(feature = "enable-libfabric-async")]
            AtomicFetchOpFuture::LibfabricAsync(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            AtomicFetchOpFuture::Ucx(f) => f.block(),
            #[cfg(feature = "enable-ucx")]
            AtomicFetchOpFuture::UcxMt(f) => f.block(),
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
            #[cfg(feature = "enable-libfabric")]
            AtomicFetchOpFuture::LibfabricMt(f) => f.spawn(),
            #[cfg(feature = "enable-libfabric-async")]
            AtomicFetchOpFuture::LibfabricAsync(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            AtomicFetchOpFuture::Ucx(f) => f.spawn(),
            #[cfg(feature = "enable-ucx")]
            AtomicFetchOpFuture::UcxMt(f) => f.spawn(),
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
            #[cfg(feature = "enable-libfabric")]
            AtomicFetchOpFutureProj::Libfabric(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            AtomicFetchOpFutureProj::LibfabricMt(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric-async")]
            AtomicFetchOpFutureProj::LibfabricAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            AtomicFetchOpFutureProj::Ucx(f) => f.poll(cx),
            #[cfg(feature = "enable-ucx")]
            AtomicFetchOpFutureProj::UcxMt(f) => f.poll(cx),
            AtomicFetchOpFutureProj::Shmem(f) => f.poll(cx),
            AtomicFetchOpFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum AtomicOp<T> {
    Min(T),
    Max(T),
    Sum(T),
    BitOr(T),
    BitXor(T),
    BitAnd(T),
    BitNand(T),
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
            AtomicOp::BitOr(_) => write!(f, "BitOr"),
            AtomicOp::BitXor(_) => write!(f, "BitXor"),
            AtomicOp::BitAnd(_) => write!(f, "BitAnd"),
            AtomicOp::BitNand(_) => write!(f, "BitNand"),
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
            | AtomicOp::BitOr(slice)
            | AtomicOp::BitXor(slice)
            | AtomicOp::BitAnd(slice)
            | AtomicOp::BitNand(slice)
            | AtomicOp::Write(slice)
            | AtomicOp::Cas(slice, _) => Some(slice),
            AtomicOp::Read => None,
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
    fn blocking_atomic_fetch_op<T: Remote>(&self, op: AtomicOp<T>, pe: usize, offset: usize) -> T;
}

pub(crate) trait AsAtomic: Copy + std::fmt::Debug {
    fn load(&self) -> Self;
    fn store(&mut self, val: Self);
    fn swap(&mut self, val: Self) -> Self;
    fn fetch_add(&mut self, val: Self) -> Self;
    // fn fetch_sub(&mut self, val: Self) -> Self;
    fn fetch_and(&mut self, val: Self) -> Self;
    fn fetch_nand(&mut self, val: Self) -> Self;
    fn fetch_or(&mut self, val: Self) -> Self;
    fn fetch_xor(&mut self, val: Self) -> Self;
    fn fetch_max(&mut self, val: Self) -> Self;
    fn fetch_min(&mut self, val: Self) -> Self;
    // fn compare_exchange(&mut self, current: Self, new: Self) -> Result<Self, Self>
    // where
    //     Self: Sized;
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
                // fn fetch_sub(&mut self, val: Self) -> Self {
                //     let atomic = unsafe { &*(self as *mut $t as *mut $a) };
                //     atomic.fetch_sub(val, Ordering::SeqCst)
                // }
                fn fetch_and(&mut self, val: Self) -> Self {
                    let atomic = unsafe { &*(self as *mut $t as *mut $a) };
                    atomic.fetch_and(val, Ordering::SeqCst)
                }
                fn fetch_nand(&mut self, val: Self) -> Self {
                    let atomic = unsafe { &*(self as *mut $t as *mut $a) };
                    atomic.fetch_nand(val, Ordering::SeqCst)
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
                // fn compare_exchange(&mut self, current: Self, new: Self) -> Result<Self, Self> {
                //     let atomic = unsafe { &*(self as *mut $t as *mut $a) };
                //     atomic.compare_exchange(current, new, Ordering::SeqCst , Ordering::Relaxed)
                // }
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
    println!("net_atomic_op called dst_addr: {:x}", dst_addr);
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

unsafe fn typed_atomic_op<A: AsAtomic, T>(op: &AtomicOp<T>, dst: *const A) {
    let op = std::mem::transmute::<&AtomicOp<T>, &AtomicOp<A>>(op);
    match op {
        AtomicOp::Write(val) => {
            (&mut *(dst as *mut A)).store(*val);
        }
        AtomicOp::Sum(val) => {
            (&mut *(dst as *mut A)).fetch_add(*val);
        }
        AtomicOp::BitOr(val) => {
            (&mut *(dst as *mut A)).fetch_or(*val);
        }
        AtomicOp::BitXor(val) => {
            (&mut *(dst as *mut A)).fetch_xor(*val);
        }
        AtomicOp::BitAnd(val) => {
            (&mut *(dst as *mut A)).fetch_and(*val);
        }
        AtomicOp::BitNand(val) => {
            (&mut *(dst as *mut A)).fetch_nand(*val);
        }
        AtomicOp::Read => {
            panic!("Read atomic op not supported in this context");
        }
        AtomicOp::Cas(_, _) => {
            panic!("Cas atomic op not supported in this context");
        }
        AtomicOp::Min(_) | AtomicOp::Max(_) => {
            panic!("Min/Max atomic ops not supported in this context");
        }
    }
}

unsafe fn typed_atomic_fetch_op<A: AsAtomic, T>(op: &AtomicOp<T>, dst: *const A, result: *mut T) {
    let op = std::mem::transmute::<&AtomicOp<T>, &AtomicOp<A>>(op);
    let res = match op {
        AtomicOp::Min(val) => (&mut *(dst as *mut A)).fetch_min(*val),
        AtomicOp::Max(val) => (&mut *(dst as *mut A)).fetch_max(*val),
        AtomicOp::Sum(val) => (&mut *(dst as *mut A)).fetch_add(*val),
        AtomicOp::BitOr(val) => (&mut *(dst as *mut A)).fetch_or(*val),
        AtomicOp::BitXor(val) => (&mut *(dst as *mut A)).fetch_xor(*val),
        AtomicOp::BitAnd(val) => (&mut *(dst as *mut A)).fetch_and(*val),
        AtomicOp::BitNand(val) => (&mut *(dst as *mut A)).fetch_nand(*val),
        AtomicOp::Read => (&*dst).load(),
        AtomicOp::Write(val) => (&mut *(dst as *mut A)).swap(*val),
        AtomicOp::Cas(_, _) => panic!("Cas atomic op not supported in this context"),
    };
    (result as *mut A).write(res);
}

//TODO compare and swap

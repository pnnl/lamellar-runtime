use crate::{
    active_messaging::{AMCounters, SyncSend},
    lamellae::{
        comm::atomic::{
            AtomicFetchOpFuture, AtomicFetchOpHandle, AtomicOp, AtomicOpFuture, AtomicOpHandle,
            CommAtomic,
        },
        CommAllocAddr, Remote,
    },
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::{comm::ShmemComm, Scheduler};

use pin_project::{pin_project, pinned_drop};
use std::{
    future::Future,
    mem::MaybeUninit,
    pin::Pin,
    sync::atomic::{
        AtomicI16, AtomicI32, AtomicI64, AtomicI8, AtomicIsize, AtomicU16, AtomicU32, AtomicU64,
        AtomicU8, AtomicUsize, Ordering,
    },
    sync::Arc,
    task::{Context, Poll},
};

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemAtomicFuture<T> {
    pub(super) op: AtomicOp<T>,
    pub(super) dst: CommAllocAddr,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

trait AsAtomic: Copy {
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

fn net_atomic_op<T: 'static>(op: &AtomicOp<T>, dst_addr: &CommAllocAddr) {
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

fn net_atomic_fetch_op<T: 'static>(op: &AtomicOp<T>, dst_addr: &CommAllocAddr, result: *mut T) {
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

impl<T: 'static> ShmemAtomicFuture<T> {
    pub(crate) fn block(mut self) {
        net_atomic_op(&self.op, &self.dst);
        self.spawned = true;
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        net_atomic_op(&self.op, &self.dst);
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.spawn_task(async {}, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for ShmemAtomicFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<ShmemAtomicFuture<T>> for AtomicOpHandle<T> {
    fn from(f: ShmemAtomicFuture<T>) -> AtomicOpHandle<T> {
        AtomicOpHandle {
            future: AtomicOpFuture::Shmem(f),
        }
    }
}

impl<T: 'static> Future for ShmemAtomicFuture<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            net_atomic_op(&self.op, &self.dst);
            *self.project().spawned = true;
        } else {
        }
        // rofi_c_wait();

        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemAtomicFetchFuture<T> {
    pub(super) op: AtomicOp<T>,
    pub(super) dst: CommAllocAddr,
    pub(super) result: MaybeUninit<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Send + 'static> ShmemAtomicFetchFuture<T> {
    fn exec_op(&self) {}
    pub(crate) fn block(mut self) -> T {
        net_atomic_fetch_op(&self.op, &self.dst, self.result.as_mut_ptr());
        self.spawned = true;
        unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(&mut self.result, &mut res);
            res.assume_init()
        }
        // Ok(())
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        net_atomic_fetch_op(&self.op, &self.dst, self.result.as_mut_ptr());

        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move {
                unsafe {
                    let mut res = MaybeUninit::uninit();
                    std::mem::swap(&mut self.result, &mut res);
                    res.assume_init()
                }
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for ShmemAtomicFetchFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<ShmemAtomicFetchFuture<T>> for AtomicFetchOpHandle<T> {
    fn from(f: ShmemAtomicFetchFuture<T>) -> AtomicFetchOpHandle<T> {
        AtomicFetchOpHandle {
            future: AtomicFetchOpFuture::Shmem(f),
        }
    }
}

impl<T: Send + 'static> Future for ShmemAtomicFetchFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            let res_ptr = self.result.as_mut_ptr();
            net_atomic_fetch_op(&self.op, &self.dst, res_ptr);

            *self.as_mut().project().spawned = true;
        } else {
        }
        // rofi_c_wait();

        Poll::Ready(unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(&mut self.result, &mut res);
            res.assume_init()
        })
    }
}

impl CommAtomic for ShmemComm {
    fn atomic_avail<T: 'static>(&self) -> bool {
        let id = std::any::TypeId::of::<T>();

        if id == std::any::TypeId::of::<u8>() {
            true
        } else if id == std::any::TypeId::of::<u16>() {
            true
        } else if id == std::any::TypeId::of::<u32>() {
            true
        } else if id == std::any::TypeId::of::<u64>() {
            true
        } else if id == std::any::TypeId::of::<i8>() {
            true
        } else if id == std::any::TypeId::of::<i16>() {
            true
        } else if id == std::any::TypeId::of::<i32>() {
            true
        } else if id == std::any::TypeId::of::<i64>() {
            true
        } else if id == std::any::TypeId::of::<usize>() {
            true
        } else if id == std::any::TypeId::of::<isize>() {
            true
        } else {
            false
        }
    }
    fn atomic_op<T>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: CommAllocAddr,
    ) -> AtomicOpHandle<T> {
        let alloc = self.alloc_lock.read();
        for (addr, (shmem, size, addrs)) in alloc.0.iter() {
            if shmem.contains(remote_addr.into()) {
                let real_remote_base = shmem.base_addr() + size * addrs[&pe].1;
                let real_remote_addr = real_remote_base + (remote_addr - *addr);
                return ShmemAtomicFuture {
                    op: op,
                    dst: real_remote_addr,
                    spawned: false,
                    scheduler: scheduler.clone(),
                    counters,
                }
                .into();
            }
        }
        panic!("shmem segment invalid for atomic op");
    }
    fn atomic_fetch_op<T>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: CommAllocAddr,
    ) -> AtomicFetchOpHandle<T> {
        let alloc = self.alloc_lock.read();
        for (addr, (shmem, size, addrs)) in alloc.0.iter() {
            if shmem.contains(remote_addr.into()) {
                let real_remote_base = shmem.base_addr() + size * addrs[&pe].1;
                let real_remote_addr = real_remote_base + (remote_addr - *addr);
                return ShmemAtomicFetchFuture {
                    op,
                    dst: real_remote_addr,
                    result: MaybeUninit::uninit(),
                    spawned: false,
                    scheduler: scheduler.clone(),
                    counters,
                }
                .into();
            }
        }
        panic!("shmem segment invalid for atomic op");
    }
}

use crate::{
    active_messaging::AMCounters,
    lamellae::{
        comm::atomic::{
            AtomicCompareExchangeFuture, AtomicCompareExchangeOpHandle, AtomicFetchOpFuture,
            AtomicFetchOpHandle, AtomicOp, AtomicOpFuture, AtomicOpHandle,
        },
        shmem_opt_lamellae::fabric::{OneSidedShmemOptAlloc, ShmemOptAlloc},
        CommAllocAddr, CommAllocAtomic,
    },
    warnings::RuntimeWarning,
    LamellarTask, Remote,
};

use super::Scheduler;

use pin_project::{pin_project, pinned_drop};
use std::{
    future::Future,
    mem::MaybeUninit,
    pin::Pin,
    sync::{
        atomic::{
            AtomicI16, AtomicI32, AtomicI64, AtomicI8, AtomicIsize, AtomicU16, AtomicU32,
            AtomicU64, AtomicU8, AtomicUsize, Ordering,
        },
        Arc,
    },
    task::{Context, Poll},
};

// Direct load/store atomics on the symmetric heap. RMW ops are AcqRel, loads Acquire, stores
// Release: nothing here backs the CommandQueue's flag protocol (that goes through put_unmanaged,
// see rdma.rs), so SeqCst buys nothing. On x86 only Write changes codegen (mov instead of xchg).
trait ShmAtomic: Copy {
    unsafe fn op(op: &AtomicOp<Self>, dst: *mut Self);
    unsafe fn fetch_op(op: &AtomicOp<Self>, dst: *mut Self) -> Self;
    unsafe fn cas(current: Self, new: Self, dst: *mut Self) -> Result<Self, Self>;
}

macro_rules! impl_shm_atomic {
    ($(($t:ty, $a:ty)),*) => {
        $(
            impl ShmAtomic for $t {
                #[inline(always)]
                unsafe fn op(op: &AtomicOp<$t>, dst: *mut $t) {
                    let a = &*(dst as *const $a);
                    match op {
                        AtomicOp::Write(v) => a.store(**v, Ordering::Release),
                        AtomicOp::Sum(v) => { a.fetch_add(**v, Ordering::AcqRel); }
                        AtomicOp::Sub(v) => { a.fetch_sub(**v, Ordering::AcqRel); }
                        AtomicOp::Prod(v) => { fetch_mul(a, **v); }
                        AtomicOp::BitOr(v) => { a.fetch_or(**v, Ordering::AcqRel); }
                        AtomicOp::BitXor(v) => { a.fetch_xor(**v, Ordering::AcqRel); }
                        AtomicOp::BitAnd(v) => { a.fetch_and(**v, Ordering::AcqRel); }
                        AtomicOp::Min(v) => { a.fetch_min(**v, Ordering::AcqRel); }
                        AtomicOp::Max(v) => { a.fetch_max(**v, Ordering::AcqRel); }
                        AtomicOp::Read(_) => panic!("Read atomic op not supported in this context"),
                        AtomicOp::Cas => panic!("Cas atomic op not supported in this context"),
                        _ => panic!("Fetch atomic ops must use the fetch path"),
                    }
                }
                #[inline(always)]
                unsafe fn fetch_op(op: &AtomicOp<$t>, dst: *mut $t) -> $t {
                    let a = &*(dst as *const $a);
                    match op {
                        AtomicOp::FetchSum(v) => a.fetch_add(**v, Ordering::AcqRel),
                        AtomicOp::FetchSub(v) => a.fetch_sub(**v, Ordering::AcqRel),
                        AtomicOp::FetchProd(v) => fetch_mul(a, **v),
                        AtomicOp::FetchBitOr(v) => a.fetch_or(**v, Ordering::AcqRel),
                        AtomicOp::FetchBitXor(v) => a.fetch_xor(**v, Ordering::AcqRel),
                        AtomicOp::FetchBitAnd(v) => a.fetch_and(**v, Ordering::AcqRel),
                        AtomicOp::FetchMin(v) => a.fetch_min(**v, Ordering::AcqRel),
                        AtomicOp::FetchMax(v) => a.fetch_max(**v, Ordering::AcqRel),
                        AtomicOp::Read(_) => a.load(Ordering::Acquire),
                        AtomicOp::Write(v) => a.swap(**v, Ordering::AcqRel),
                        AtomicOp::Cas => panic!("Cas atomic op not supported in this context"),
                        _ => panic!("Non-fetch atomic ops must use the non-fetch path"),
                    }
                }
                #[inline(always)]
                unsafe fn cas(current: $t, new: $t, dst: *mut $t) -> Result<$t, $t> {
                    (&*(dst as *const $a)).compare_exchange(
                        current,
                        new,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                }
            }
        )*
        // no native fetch_mul: CAS loop, spinning (the line is local, a retry is ~ns)
        trait FetchMul<T> { fn fetch_mul_cas(&self, v: T) -> T; }
        $(
            impl FetchMul<$t> for $a {
                #[inline(always)]
                fn fetch_mul_cas(&self, v: $t) -> $t {
                    let mut cur = self.load(Ordering::Acquire);
                    loop {
                        match self.compare_exchange_weak(
                            cur,
                            cur.wrapping_mul(v),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(old) => return old,
                            Err(actual) => {
                                cur = actual;
                                std::hint::spin_loop();
                            }
                        }
                    }
                }
            }
        )*
    };
}

#[inline(always)]
fn fetch_mul<T, A: FetchMul<T>>(a: &A, v: T) -> T {
    a.fetch_mul_cas(v)
}

impl_shm_atomic!(
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

// Monomorphized dispatch: the TypeId compares are constants per T, so only one arm survives.
macro_rules! with_int_type {
    ($T:ty, $A:ident => $body:expr) => {{
        use std::any::TypeId;
        let id = TypeId::of::<$T>();
        if id == TypeId::of::<u8>() {
            type $A = u8;
            $body
        } else if id == TypeId::of::<u16>() {
            type $A = u16;
            $body
        } else if id == TypeId::of::<u32>() {
            type $A = u32;
            $body
        } else if id == TypeId::of::<u64>() {
            type $A = u64;
            $body
        } else if id == TypeId::of::<usize>() {
            type $A = usize;
            $body
        } else if id == TypeId::of::<i8>() {
            type $A = i8;
            $body
        } else if id == TypeId::of::<i16>() {
            type $A = i16;
            $body
        } else if id == TypeId::of::<i32>() {
            type $A = i32;
            $body
        } else if id == TypeId::of::<i64>() {
            type $A = i64;
            $body
        } else if id == TypeId::of::<isize>() {
            type $A = isize;
            $body
        } else {
            panic!("Unsupported atomic operation type")
        }
    }};
}

#[inline]
fn shm_atomic_op<T: Remote>(op: &AtomicOp<T>, dst: &CommAllocAddr) {
    // T == A in the taken arm, so the casts are identities
    with_int_type!(T, A => unsafe {
        A::op(
            &*(op as *const AtomicOp<T> as *const AtomicOp<A>),
            dst.as_mut_ptr::<A>(),
        )
    })
}

#[inline]
fn shm_atomic_fetch_op<T: Remote>(op: &AtomicOp<T>, dst: &CommAllocAddr) -> T {
    with_int_type!(T, A => unsafe {
        let old = A::fetch_op(
            &*(op as *const AtomicOp<T> as *const AtomicOp<A>),
            dst.as_mut_ptr::<A>(),
        );
        std::mem::transmute_copy::<A, T>(&old)
    })
}

#[inline]
fn shm_atomic_compare_exchange<T: Remote>(current: T, new: T, dst: &CommAllocAddr) -> Result<T, T> {
    with_int_type!(T, A => unsafe {
        match A::cas(
            std::mem::transmute_copy::<T, A>(&current),
            std::mem::transmute_copy::<T, A>(&new),
            dst.as_mut_ptr::<A>(),
        ) {
            Ok(old) => Ok(std::mem::transmute_copy::<A, T>(&old)),
            Err(old) => Err(std::mem::transmute_copy::<A, T>(&old)),
        }
    })
}

pub(super) enum AtomicDst {
    One(CommAllocAddr), // the common case: no Vec allocation
    All(Vec<CommAllocAddr>),
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptAtomicFuture<T> {
    pub(super) op: AtomicOp<T>,
    pub(super) dst: AtomicDst,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) spawned: bool,
}

impl<T: Remote> ShmemOptAtomicFuture<T> {
    fn exec_op(&mut self) {
        match &self.dst {
            AtomicDst::One(dst) => shm_atomic_op(&self.op, dst),
            AtomicDst::All(dsts) => {
                for dst in dsts {
                    shm_atomic_op(&self.op, dst);
                }
            }
        }
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        // the op is complete once issued: hand back a finished task, no executor round-trip
        self.exec_op();
        self.scheduler.ready_task(())
    }
}

#[pinned_drop]
impl<T> PinnedDrop for ShmemOptAtomicFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<ShmemOptAtomicFuture<T>> for AtomicOpHandle<T> {
    fn from(f: ShmemOptAtomicFuture<T>) -> AtomicOpHandle<T> {
        AtomicOpHandle {
            future: AtomicOpFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote> Future for ShmemOptAtomicFuture<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        // no field is structurally pinned
        let this = unsafe { self.get_unchecked_mut() };
        if !this.spawned {
            this.exec_op();
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptAtomicFetchFuture<T> {
    pub(super) op: AtomicOp<T>,
    pub(super) dst: CommAllocAddr,
    pub(super) result: MaybeUninit<T>, // written by exec_op; T: Copy so never needs dropping
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) spawned: bool,
}

impl<T: Remote> ShmemOptAtomicFetchFuture<T> {
    fn exec_op(&mut self) {
        self.result = MaybeUninit::new(shm_atomic_fetch_op(&self.op, &self.dst));
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> T {
        self.exec_op();
        unsafe { self.result.assume_init() }
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_op();
        self.scheduler.ready_task(unsafe { self.result.assume_init() })
    }
}

#[pinned_drop]
impl<T> PinnedDrop for ShmemOptAtomicFetchFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<ShmemOptAtomicFetchFuture<T>> for AtomicFetchOpHandle<T> {
    fn from(f: ShmemOptAtomicFetchFuture<T>) -> AtomicFetchOpHandle<T> {
        AtomicFetchOpHandle {
            future: AtomicFetchOpFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote> Future for ShmemOptAtomicFetchFuture<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        if !this.spawned {
            this.exec_op();
        }
        Poll::Ready(unsafe { this.result.assume_init() })
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptAtomicCompareExchangeFuture<T> {
    pub(super) dst: CommAllocAddr,
    current: T,
    new: T,
    result: Option<Result<T, T>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) spawned: bool,
}

impl<T: Remote> ShmemOptAtomicCompareExchangeFuture<T> {
    fn exec_op(&mut self) {
        self.result = Some(shm_atomic_compare_exchange(
            self.current,
            self.new,
            &self.dst,
        ));
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Result<T, T> {
        self.exec_op();
        self.result
            .take()
            .expect("compare_exchange result should be set")
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Result<T, T>> {
        self.exec_op();
        let result = self
            .result
            .take()
            .expect("compare_exchange result should be set");
        self.scheduler.ready_task(result)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for ShmemOptAtomicCompareExchangeFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<ShmemOptAtomicCompareExchangeFuture<T>> for AtomicCompareExchangeOpHandle<T> {
    fn from(f: ShmemOptAtomicCompareExchangeFuture<T>) -> AtomicCompareExchangeOpHandle<T> {
        AtomicCompareExchangeOpHandle {
            future: AtomicCompareExchangeFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote> Future for ShmemOptAtomicCompareExchangeFuture<T> {
    type Output = Result<T, T>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        if !this.spawned {
            this.exec_op();
        }
        Poll::Ready(
            this.result
                .take()
                .expect("compare_exchange result should be set"),
        )
    }
}

impl CommAllocAtomic for ShmemOptAlloc {
    fn atomic_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        _counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.pe_base_offset(pe);
        let remote_dst_addr = remote_dst_base + offset;
        ShmemOptAtomicFuture {
            op: op,
            dst: AtomicDst::One(CommAllocAddr(remote_dst_addr)),
            spawned: false,
            scheduler: scheduler.clone(),
        }
        .into()
    }
    fn atomic_op_blocking<T: Remote + 'static>(
        &self,
        _scheduler: &Arc<Scheduler>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.pe_base_offset(pe);
        let remote_dst_addr = remote_dst_base + offset;
        shm_atomic_op(&op, &CommAllocAddr(remote_dst_addr));
    }
    fn atomic_op_unmanaged<T: Remote + 'static>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.pe_base_offset(pe);
        let remote_dst_addr = remote_dst_base + offset;
        shm_atomic_op(&op, &CommAllocAddr(remote_dst_addr));
    }
    fn atomic_op_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        _counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_addrs: Vec<CommAllocAddr> = (0..self.num_pes())
            .map(|pe| {
                let remote_dst_base = self.pe_base_offset_rel(pe);
                CommAllocAddr(remote_dst_base + offset)
            })
            .collect();
        ShmemOptAtomicFuture {
            op: op,
            dst: AtomicDst::All(remote_dst_addrs),
            spawned: false,
            scheduler: scheduler.clone(),
        }
        .into()
    }
    fn atomic_op_all_unmanaged<T: Remote + 'static>(&self, op: AtomicOp<T>, offset: usize) {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        for pe in 0..self.num_pes() {
            let remote_dst_base = self.pe_base_offset_rel(pe);
            let remote_dst_addr = remote_dst_base + offset;
            shm_atomic_op(&op, &CommAllocAddr(remote_dst_addr));
        }
    }
    fn atomic_fetch_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        _counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicFetchOpHandle<T> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.pe_base_offset(pe);
        let remote_dst_addr = remote_dst_base + offset;
        ShmemOptAtomicFetchFuture {
            op,
            dst: CommAllocAddr(remote_dst_addr),
            result: MaybeUninit::uninit(),
            spawned: false,
            scheduler: scheduler.clone(),
        }
        .into()
    }
    fn atomic_fetch_op_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> T {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.pe_base_offset(pe);
        let remote_dst_addr = remote_dst_base + offset;
        shm_atomic_fetch_op(&op, &CommAllocAddr(remote_dst_addr))
    }
    fn atomic_compare_exchange<T: Remote + PartialEq>(
        &self,
        scheduler: &Arc<Scheduler>,
        _counters: Option<Arc<[Arc<AMCounters>]>>,
        current: T,
        new: T,
        pe: usize,
        offset: usize,
    ) -> AtomicCompareExchangeOpHandle<T> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.pe_base_offset(pe);
        let remote_dst_addr = remote_dst_base + offset;
        ShmemOptAtomicCompareExchangeFuture {
            dst: CommAllocAddr(remote_dst_addr),
            current,
            new,
            result: None,
            spawned: false,
            scheduler: scheduler.clone(),
        }
        .into()
    }
    fn atomic_compare_exchange_blocking<T: Remote + PartialEq>(
        &self,
        _scheduler: &Arc<Scheduler>,
        current: T,
        new: T,
        pe: usize,
        offset: usize,
    ) -> Result<T, T> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.pe_base_offset(pe);
        let remote_dst_addr = remote_dst_base + offset;
        shm_atomic_compare_exchange(current, new, &CommAllocAddr(remote_dst_addr))
    }
}

impl CommAllocAtomic for OneSidedShmemOptAlloc {
    fn atomic_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        _counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedShmemOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.start();
        let remote_dst_addr = remote_dst_base + offset;
        ShmemOptAtomicFuture {
            op: op,
            dst: AtomicDst::One(CommAllocAddr(remote_dst_addr)),
            spawned: false,
            scheduler: scheduler.clone(),
        }
        .into()
    }
    fn atomic_op_blocking<T: Remote + 'static>(
        &self,
        _scheduler: &Arc<Scheduler>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedShmemOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.start();
        let remote_dst_addr = remote_dst_base + offset;
        shm_atomic_op(&op, &CommAllocAddr(remote_dst_addr));
    }
    fn atomic_op_unmanaged<T: Remote + 'static>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedShmemOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.start();
        let remote_dst_addr = remote_dst_base + offset;
        shm_atomic_op(&op, &CommAllocAddr(remote_dst_addr));
    }
    fn atomic_op_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        self.atomic_op(scheduler, counters, op, self.remote_pe, offset)
    }
    fn atomic_op_all_unmanaged<T: Remote + 'static>(&self, op: AtomicOp<T>, offset: usize) {
        self.atomic_op_unmanaged(op, self.remote_pe, offset)
    }
    fn atomic_fetch_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        _counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicFetchOpHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "atomic fetch op called on OneSidedShmemOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.start();
        let remote_dst_addr = remote_dst_base + offset;
        ShmemOptAtomicFetchFuture {
            op,
            dst: CommAllocAddr(remote_dst_addr),
            result: MaybeUninit::uninit(),
            spawned: false,
            scheduler: scheduler.clone(),
        }
        .into()
    }
    fn atomic_fetch_op_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> T {
        assert_eq!(
            pe, self.remote_pe,
            "blocking atomic fetch op called on OneSidedShmemOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.start();
        let remote_dst_addr = remote_dst_base + offset;
        shm_atomic_fetch_op(&op, &CommAllocAddr(remote_dst_addr))
    }
    fn atomic_compare_exchange<T: Remote + PartialEq>(
        &self,
        scheduler: &Arc<Scheduler>,
        _counters: Option<Arc<[Arc<AMCounters>]>>,
        current: T,
        new: T,
        pe: usize,
        offset: usize,
    ) -> AtomicCompareExchangeOpHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "atomic compare exchange called on OneSidedShmemOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_addr = self.start() + offset;
        ShmemOptAtomicCompareExchangeFuture {
            dst: CommAllocAddr(remote_dst_addr),
            current,
            new,
            result: None,
            spawned: false,
            scheduler: scheduler.clone(),
        }
        .into()
    }
    fn atomic_compare_exchange_blocking<T: Remote + PartialEq>(
        &self,
        _scheduler: &Arc<Scheduler>,
        current: T,
        new: T,
        pe: usize,
        offset: usize,
    ) -> Result<T, T> {
        assert_eq!(
            pe, self.remote_pe,
            "blocking atomic compare exchange called on OneSidedShmemOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_addr = self.start() + offset;
        shm_atomic_compare_exchange(current, new, &CommAllocAddr(remote_dst_addr))
    }
}

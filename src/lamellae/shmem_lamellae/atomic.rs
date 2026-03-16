use crate::{
    active_messaging::AMCounters,
    lamellae::{
        comm::atomic::{
            AtomicCompareExchangeFuture, AtomicCompareExchangeOpHandle, AtomicFetchOpFuture,
            AtomicFetchOpHandle, AtomicOp, AtomicOpFuture, AtomicOpHandle,
        },
        net_atomic_compare_exchange, net_atomic_fetch_op, net_atomic_op,
        shmem_lamellae::fabric::{OneSidedShmemAlloc, ShmemAlloc},
        CommAllocAddr, CommAllocAtomic,
    },
    warnings::RuntimeWarning,
    LamellarTask, Remote,
};

use super::Scheduler;

use pin_project::{pin_project, pinned_drop};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemAtomicFuture<T> {
    pub(super) op: AtomicOp<T>,
    pub(super) dst: Vec<CommAllocAddr>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: 'static> ShmemAtomicFuture<T> {
    pub(crate) fn block(mut self) {
        for dst in &self.dst {
            net_atomic_op(&self.op, dst);
        }
        self.spawned = true;
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        for dst in &self.dst {
            net_atomic_op(&self.op, dst);
        }
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
            for dst in &self.dst {
                net_atomic_op(&self.op, dst);
            }
            *self.project().spawned = true;
        } else {
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemAtomicFetchFuture<T> {
    pub(super) op: AtomicOp<T>,
    pub(super) dst: CommAllocAddr,
    pub(super) result: Box<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> ShmemAtomicFetchFuture<T> {
    pub(crate) fn block(mut self) -> T {
        net_atomic_fetch_op(&self.op, &self.dst, self.result.as_mut() as *mut T);
        self.spawned = true;
        *self.result
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        net_atomic_fetch_op(&self.op, &self.dst, self.result.as_mut() as *mut T);
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler
            .clone()
            .spawn_task(async move { *self.result }, counters)
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

impl<T: Remote> Future for ShmemAtomicFetchFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            let res_ptr = self.result.as_mut() as *mut T;
            net_atomic_fetch_op(&self.op, &self.dst, res_ptr);
            *self.as_mut().project().spawned = true;
        }
        Poll::Ready(*self.result)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemAtomicCompareExchangeFuture<T> {
    pub(super) dst: CommAllocAddr,
    current: T,
    new: T,
    result: Option<Result<T, T>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> ShmemAtomicCompareExchangeFuture<T> {
    fn exec_op(&mut self) {
        self.result = Some(net_atomic_compare_exchange(self.current, self.new, &self.dst));
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Result<T, T> {
        self.exec_op();
        self.result.take().expect("compare_exchange result should be set")
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Result<T, T>> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move { self.result.take().expect("compare_exchange result should be set") },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for ShmemAtomicCompareExchangeFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<ShmemAtomicCompareExchangeFuture<T>> for AtomicCompareExchangeOpHandle<T> {
    fn from(f: ShmemAtomicCompareExchangeFuture<T>) -> AtomicCompareExchangeOpHandle<T> {
        AtomicCompareExchangeOpHandle {
            future: AtomicCompareExchangeFuture::Shmem(f),
        }
    }
}

impl<T: Remote> Future for ShmemAtomicCompareExchangeFuture<T> {
    type Output = Result<T, T>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(self.result.take().expect("compare_exchange result should be set"))
    }
}

impl CommAllocAtomic for ShmemAlloc {
    fn atomic_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        // println!("atomic_op called on ShmemAlloc for pe: {} {:?}", pe, self);
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.pe_base_offset(pe);
        let remote_dst_addr = remote_dst_base + offset;
        ShmemAtomicFuture {
            op: op,
            dst: vec![CommAllocAddr(remote_dst_addr)],
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn atomic_op_blocking<T: Remote + 'static>(&self, _scheduler: &Arc<Scheduler>, op: AtomicOp<T>, pe: usize, offset: usize) {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.pe_base_offset(pe);
        let remote_dst_addr = remote_dst_base + offset;
        net_atomic_op(&op, &CommAllocAddr(remote_dst_addr));
    }
    fn atomic_op_unmanaged<T: Remote + 'static>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.pe_base_offset(pe);
        let remote_dst_addr = remote_dst_base + offset;
        net_atomic_op(&op, &CommAllocAddr(remote_dst_addr));
    }
    fn atomic_op_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_addrs: Vec<CommAllocAddr> = (0..self.num_pes())
            .map(|pe| {
                let remote_dst_base = self.pe_base_offset(pe);
                CommAllocAddr(remote_dst_base + offset)
            })
            .collect();
        ShmemAtomicFuture {
            op: op,
            dst: remote_dst_addrs,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn atomic_op_all_unmanaged<T: Remote + 'static>(&self, op: AtomicOp<T>, offset: usize) {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        for pe in 0..self.num_pes() {
            let remote_dst_base = self.pe_base_offset(pe);
            let remote_dst_addr = remote_dst_base + offset;
            net_atomic_op(&op, &CommAllocAddr(remote_dst_addr));
        }
    }
    fn atomic_fetch_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicFetchOpHandle<T> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.pe_base_offset(pe);
        let remote_dst_addr = remote_dst_base + offset;
        ShmemAtomicFetchFuture {
            op,
            dst: CommAllocAddr(remote_dst_addr),
            result: Box::new(T::default()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn atomic_fetch_op_blocking<T: Remote>(&self, _scheduler: &Arc<Scheduler>, op: AtomicOp<T>, pe: usize, offset: usize) -> T {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.pe_base_offset(pe);
        let remote_dst_addr = remote_dst_base + offset;
        let mut result = T::default();
        net_atomic_fetch_op(&op, &CommAllocAddr(remote_dst_addr), &mut result as *mut T);
        result
    }
    fn atomic_compare_exchange<T: Remote + PartialEq>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        current: T,
        new: T,
        pe: usize,
        offset: usize,
    ) -> AtomicCompareExchangeOpHandle<T> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.pe_base_offset(pe);
        let remote_dst_addr = remote_dst_base + offset;
        ShmemAtomicCompareExchangeFuture {
            dst: CommAllocAddr(remote_dst_addr),
            current,
            new,
            result: None,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
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
        net_atomic_compare_exchange(current, new, &CommAllocAddr(remote_dst_addr))
    }
}

impl CommAllocAtomic for OneSidedShmemAlloc {
    fn atomic_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedShmemAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.start();
        let remote_dst_addr = remote_dst_base + offset;
        ShmemAtomicFuture {
            op: op,
            dst: vec![CommAllocAddr(remote_dst_addr)],
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn atomic_op_blocking<T: Remote + 'static>(&self, _scheduler: &Arc<Scheduler>, op: AtomicOp<T>, pe: usize, offset: usize) {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedShmemAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.start();
        let remote_dst_addr = remote_dst_base + offset;
        net_atomic_op(&op, &CommAllocAddr(remote_dst_addr));
    }
    fn atomic_op_unmanaged<T: Remote + 'static>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedShmemAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.start();
        let remote_dst_addr = remote_dst_base + offset;
        net_atomic_op(&op, &CommAllocAddr(remote_dst_addr));
    }
    fn atomic_op_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
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
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicFetchOpHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "atomic fetch op called on OneSidedShmemAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.start();
        let remote_dst_addr = remote_dst_base + offset;
        ShmemAtomicFetchFuture {
            op,
            dst: CommAllocAddr(remote_dst_addr),
            result: Box::new(T::default()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn atomic_fetch_op_blocking<T: Remote>(&self, _scheduler: &Arc<Scheduler>, op: AtomicOp<T>, pe: usize, offset: usize) -> T {
        assert_eq!(
            pe, self.remote_pe,
            "blocking atomic fetch op called on OneSidedShmemAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_base = self.start();
        let remote_dst_addr = remote_dst_base + offset;
        let mut result = T::default();
        net_atomic_fetch_op(&op, &CommAllocAddr(remote_dst_addr), &mut result as *mut T);
        result
    }
    fn atomic_compare_exchange<T: Remote + PartialEq>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        current: T,
        new: T,
        pe: usize,
        offset: usize,
    ) -> AtomicCompareExchangeOpHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "atomic compare exchange called on OneSidedShmemAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_addr = self.start() + offset;
        ShmemAtomicCompareExchangeFuture {
            dst: CommAllocAddr(remote_dst_addr),
            current,
            new,
            result: None,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
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
            "blocking atomic compare exchange called on OneSidedShmemAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_dst_addr = self.start() + offset;
        net_atomic_compare_exchange(current, new, &CommAllocAddr(remote_dst_addr))
    }
}

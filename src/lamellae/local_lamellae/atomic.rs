use crate::{
    active_messaging::AMCounters,
    lamellae::{
        comm::atomic::{
            AtomicCompareExchangeFuture, AtomicCompareExchangeOpHandle, AtomicFetchOpFuture,
            AtomicFetchOpHandle, AtomicOp, AtomicOpFuture, AtomicOpHandle,
        },
        local_lamellae::comm::LocalAlloc,
        net_atomic_compare_exchange, net_atomic_fetch_op, net_atomic_op, CommAllocAddr,
        CommAllocAtomic,
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
pub(crate) struct LocalAtomicFuture<T> {
    alloc: Arc<LocalAlloc>,
    offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: 'static> LocalAtomicFuture<T> {
    fn exec_op(&mut self) {
        assert!(self.offset < unsafe { self.alloc.as_mut_slice::<T>().len() });
        net_atomic_op(&self.op, &CommAllocAddr(self.alloc.start() + self.offset));
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.spawn_task(async {}, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LocalAtomicFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<LocalAtomicFuture<T>> for AtomicOpHandle<T> {
    fn from(f: LocalAtomicFuture<T>) -> AtomicOpHandle<T> {
        AtomicOpHandle {
            future: AtomicOpFuture::Local(f),
        }
    }
}

impl<T: 'static> Future for LocalAtomicFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalAtomicFetchFuture<T> {
    alloc: Arc<LocalAlloc>,
    offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(super) result: Box<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LocalAtomicFetchFuture<T> {
    fn exec_op(&mut self) {
        assert!(self.offset < unsafe { self.alloc.as_mut_slice::<T>().len() });
        net_atomic_fetch_op(
            &self.op,
            &CommAllocAddr(self.alloc.start() + self.offset),
            self.result.as_mut(),
        );

        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> T {
        self.exec_op();
        *self.result
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler
            .clone()
            .spawn_task(async move { *self.result }, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LocalAtomicFetchFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<LocalAtomicFetchFuture<T>> for AtomicFetchOpHandle<T> {
    fn from(f: LocalAtomicFetchFuture<T>) -> AtomicFetchOpHandle<T> {
        AtomicFetchOpHandle {
            future: AtomicFetchOpFuture::Local(f),
        }
    }
}

impl<T: Remote> Future for LocalAtomicFetchFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(*self.result)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalAtomicCompareExchangeFuture<T> {
    alloc: Arc<LocalAlloc>,
    offset: usize,
    current: T,
    new: T,
    result: Option<Result<T, T>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LocalAtomicCompareExchangeFuture<T> {
    fn exec_op(&mut self) {
        assert!(self.offset < unsafe { self.alloc.as_mut_slice::<T>().len() });
        self.result = Some(net_atomic_compare_exchange(
            self.current,
            self.new,
            &CommAllocAddr(self.alloc.start() + self.offset),
        ));
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
impl<T> PinnedDrop for LocalAtomicCompareExchangeFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<LocalAtomicCompareExchangeFuture<T>> for AtomicCompareExchangeOpHandle<T> {
    fn from(f: LocalAtomicCompareExchangeFuture<T>) -> AtomicCompareExchangeOpHandle<T> {
        AtomicCompareExchangeOpHandle {
            future: AtomicCompareExchangeFuture::Local(f),
        }
    }
}

impl<T: Remote> Future for LocalAtomicCompareExchangeFuture<T> {
    type Output = Result<T, T>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(self.result.take().expect("compare_exchange result should be set"))
    }
}

impl CommAllocAtomic for Arc<LocalAlloc> {
    fn atomic_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        _pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        LocalAtomicFuture {
            alloc: self.clone(),
            offset,
            op,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }
    fn atomic_op_blocking<T: Remote>(&self, _scheduler: &Arc<Scheduler>, op: AtomicOp<T>, _pe: usize, offset: usize) {
        assert!(offset < unsafe { self.as_mut_slice::<T>().len() });
        net_atomic_op(&op, &CommAllocAddr(self.start() + offset));
    }
    fn atomic_op_unmanaged<T: Remote>(&self, op: AtomicOp<T>, _pe: usize, offset: usize) {
        assert!(offset < unsafe { self.as_mut_slice::<T>().len() });
        net_atomic_op(&op, &CommAllocAddr(self.start() + offset));
    }
    fn atomic_op_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        LocalAtomicFuture {
            alloc: self.clone(),
            offset,
            op,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }
    fn atomic_op_all_unmanaged<T: Remote>(&self, op: AtomicOp<T>, offset: usize) {
        assert!(offset < unsafe { self.as_mut_slice::<T>().len() });
        net_atomic_op(&op, &CommAllocAddr(self.start() + offset));
    }
    fn atomic_fetch_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        _pe: usize,
        offset: usize,
    ) -> AtomicFetchOpHandle<T> {
        LocalAtomicFetchFuture {
            alloc: self.clone(),
            offset,
            op,
            result: Box::new(T::default()),
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }
    fn atomic_fetch_op_blocking<T: Remote>(&self, _scheduler: &Arc<Scheduler>, op: AtomicOp<T>, _pe: usize, offset: usize) -> T {
        assert!(offset < unsafe { self.as_mut_slice::<T>().len() });
        let mut result = T::default();
        net_atomic_fetch_op(&op, &CommAllocAddr(self.start() + offset), &mut result);
        result
    }
    fn atomic_compare_exchange<T: Remote + PartialEq>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        current: T,
        new: T,
        _pe: usize,
        offset: usize,
    ) -> AtomicCompareExchangeOpHandle<T> {
        LocalAtomicCompareExchangeFuture {
            alloc: self.clone(),
            offset,
            current,
            new,
            result: None,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }
    fn atomic_compare_exchange_blocking<T: Remote + PartialEq>(
        &self,
        _scheduler: &Arc<Scheduler>,
        current: T,
        new: T,
        _pe: usize,
        offset: usize,
    ) -> Result<T, T> {
        assert!(offset < unsafe { self.as_mut_slice::<T>().len() });
        net_atomic_compare_exchange(current, new, &CommAllocAddr(self.start() + offset))
    }
}

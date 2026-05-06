use crate::{
    active_messaging::AMCounters,
    lamellae::comm::atomic::{
        AtomicCompareExchangeFuture, AtomicCompareExchangeOpHandle, AtomicFetchOpFuture,
        AtomicFetchOpHandle, AtomicOp, AtomicOpFuture, AtomicOpHandle,
        CommAllocAtomic,
    },
    warnings::RuntimeWarning,
    LamellarTask, Remote,
};

use super::{
    fabric::{LibfabricSysAlloc, OneSidedLibfabricSysAlloc},
    Scheduler,
};

use pin_project::{pin_project, pinned_drop};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tracing::trace;

fn compare_exchange_result<T: PartialEq>(old: T, current: T) -> Result<T, T> {
    if old == current {
        Ok(old)
    } else {
        Err(old)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysAtomicFuture<T> {
    pub(crate) alloc: LibfabricSysAlloc,
    pub(super) remote_pes: Vec<usize>,
    pub(crate) offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Send + 'static> LibfabricSysAtomicFuture<T> {
    fn exec_op(&mut self) {
        trace!(
            "performing atomic op: {:?} offset: {:?} ",
            self.op,
            self.offset
        );
        for pe in &self.remote_pes {
            LibfabricSysAlloc::atomic_op_inner(&self.alloc, *pe, self.offset, &self.op, false)
                .unwrap();
        }
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        let ofi = self.alloc.ofi.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { ofi.wait_all().unwrap() }, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricSysAtomicFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<LibfabricSysAtomicFuture<T>> for AtomicOpHandle<T> {
    fn from(f: LibfabricSysAtomicFuture<T>) -> AtomicOpHandle<T> {
        AtomicOpHandle {
            future: AtomicOpFuture::LibfabricSys(f),
        }
    }
}

impl<T: Send + 'static> Future for LibfabricSysAtomicFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysAtomicFetchFuture<T> {
    pub(crate) alloc: LibfabricSysAlloc,
    pub(super) remote_pe: usize,
    pub(crate) offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) result: Box<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricSysAtomicFetchFuture<T> {
    fn exec_op(&mut self) {
        let result_ptr = self.result.as_mut() as *mut T;
        trace!(
            "performing atomic op: {:?} offset: {:?} result ptr: {:?} ",
            self.op,
            self.offset,
            result_ptr
        );
        LibfabricSysAlloc::atomic_fetch_op_inner(
            &self.alloc,
            self.remote_pe,
            self.offset,
            &self.op,
            std::slice::from_mut(self.result.as_mut()),
            false,
        )
        .unwrap();
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> T {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
        *self.result
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_op();

        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricSysAtomicFetchFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<LibfabricSysAtomicFetchFuture<T>> for AtomicFetchOpHandle<T> {
    fn from(f: LibfabricSysAtomicFetchFuture<T>) -> AtomicFetchOpHandle<T> {
        AtomicFetchOpHandle {
            future: AtomicFetchOpFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysAtomicFetchFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();

        Poll::Ready(*self.result)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysAtomicCompareExchangeFuture<T> {
    pub(crate) alloc: LibfabricSysAlloc,
    pub(super) remote_pe: usize,
    pub(crate) offset: usize,
    current: T,
    new: T,
    pub(crate) result: Box<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote + PartialEq> LibfabricSysAtomicCompareExchangeFuture<T> {
    fn exec_op(&mut self) {
        LibfabricSysAlloc::atomic_compare_exchange_op_inner(
            &self.alloc,
            self.remote_pe,
            self.offset,
            self.current,
            self.new,
            std::slice::from_mut(self.result.as_mut()),
            false,
        )
        .unwrap();
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Result<T, T> {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
        compare_exchange_result(*self.result, self.current)
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Result<T, T>> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricSysAtomicCompareExchangeFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<LibfabricSysAtomicCompareExchangeFuture<T>> for AtomicCompareExchangeOpHandle<T> {
    fn from(f: LibfabricSysAtomicCompareExchangeFuture<T>) -> AtomicCompareExchangeOpHandle<T> {
        AtomicCompareExchangeOpHandle {
            future: AtomicCompareExchangeFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote + PartialEq> Future for LibfabricSysAtomicCompareExchangeFuture<T> {
    type Output = Result<T, T>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        Poll::Ready(compare_exchange_result(*self.result, self.current))
    }
}

impl CommAllocAtomic for LibfabricSysAlloc {
    fn atomic_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        LibfabricSysAtomicFuture {
            alloc: self.clone(),
            remote_pes: vec![pe],
            offset,
            op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn atomic_op_blocking<T: Remote>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        LibfabricSysAlloc::atomic_op_inner(self, pe, offset, &op, true).unwrap();
    }
    fn atomic_op_unmanaged<T: Remote + 'static>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        LibfabricSysAlloc::atomic_op_inner(self, pe, offset, &op, false).unwrap();
    }
    fn atomic_op_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        LibfabricSysAtomicFuture {
            alloc: self.clone(),
            remote_pes: (0..self.num_pes()).collect(),
            offset,
            op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn atomic_op_all_unmanaged<T: Remote + 'static>(&self, op: AtomicOp<T>, offset: usize) {
        for pe in 0..self.num_pes() {
            LibfabricSysAlloc::atomic_op_inner(self, pe, offset, &op, false).unwrap();
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
        LibfabricSysAtomicFetchFuture {
            alloc: self.clone(),
            remote_pe: pe,
            offset,
            op: op,
            result: Box::new(T::default()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn blocking_atomic_fetch_op<T: Remote>(&self, op: AtomicOp<T>, pe: usize, offset: usize) -> T {
        let mut result = T::default();
        let mut_result_slice = std::slice::from_mut(&mut result);
        LibfabricSysAlloc::atomic_fetch_op_inner(self, pe, offset, &op, mut_result_slice, true)
            .unwrap();
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
        LibfabricSysAtomicCompareExchangeFuture {
            alloc: self.clone(),
            remote_pe: pe,
            offset,
            current,
            new,
            result: Box::new(T::default()),
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }
    fn blocking_atomic_compare_exchange<T: Remote + PartialEq>(
        &self,
        current: T,
        new: T,
        pe: usize,
        offset: usize,
    ) -> Result<T, T> {
        let mut result = T::default();
        LibfabricSysAlloc::atomic_compare_exchange_op_inner(
            self,
            pe,
            offset,
            current,
            new,
            std::slice::from_mut(&mut result),
            true,
        )
        .unwrap();
        compare_exchange_result(result, current)
    }
}

impl CommAllocAtomic for OneSidedLibfabricSysAlloc {
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
            "atomic op called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricSysAtomicFuture {
            alloc: self.alloc.clone(),
            remote_pes: vec![pe],
            offset,
            op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn atomic_op_blocking<T: Remote>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricSysAlloc::atomic_op_inner(&self.alloc, pe, offset, &op, true).unwrap();
    }

    fn atomic_op_unmanaged<T: Remote + 'static>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricSysAlloc::atomic_op_inner(&self.alloc, pe, offset, &op, false).unwrap();
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
        self.atomic_op_unmanaged(op, self.remote_pe, offset);
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
            "atomic fetch op called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricSysAtomicFetchFuture {
            alloc: self.alloc.clone(),
            remote_pe: pe,
            offset,
            op: op,
            result: Box::new(T::default()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn blocking_atomic_fetch_op<T: Remote>(&self, op: AtomicOp<T>, pe: usize, offset: usize) -> T {
        assert_eq!(
            pe, self.remote_pe,
            "blocking atomic fetch op called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut result = T::default();
        let mut_result_slice = std::slice::from_mut(&mut result);
        LibfabricSysAlloc::atomic_fetch_op_inner(&self.alloc, pe, offset, &op, mut_result_slice, true)
            .unwrap();
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
            "atomic compare exchange called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricSysAtomicCompareExchangeFuture {
            alloc: self.alloc.clone(),
            remote_pe: pe,
            offset,
            current,
            new,
            result: Box::new(T::default()),
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }
    fn blocking_atomic_compare_exchange<T: Remote + PartialEq>(
        &self,
        current: T,
        new: T,
        pe: usize,
        offset: usize,
    ) -> Result<T, T> {
        assert_eq!(
            pe, self.remote_pe,
            "blocking atomic compare exchange called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut result = T::default();
        LibfabricSysAlloc::atomic_compare_exchange_op_inner(
            &self.alloc,
            pe,
            offset,
            current,
            new,
            std::slice::from_mut(&mut result),
            true,
        )
        .unwrap();
        compare_exchange_result(result, current)
    }
}

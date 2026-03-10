use crate::{
    active_messaging::AMCounters,
    lamellae::comm::atomic::{
        AtomicFetchOpFuture, AtomicFetchOpHandle, AtomicOp, AtomicOpFuture, AtomicOpHandle,
        CommAllocAtomic,
    },
    warnings::RuntimeWarning,
    LamellarTask, Remote,
};

use super::{
    fabric::{LibfabricAlloc, OneSidedLibfabricAlloc},
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

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAtomicFuture<T> {
    pub(crate) alloc: LibfabricAlloc,
    pub(super) remote_pes: Vec<usize>,
    pub(crate) offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Send + 'static> LibfabricAtomicFuture<T> {
    fn exec_op(&mut self) {
        trace!(
            "performing atomic op: {:?} offset: {:?} ",
            self.op,
            self.offset
        );
        for pe in &self.remote_pes {
            LibfabricAlloc::atomic_op_inner(&self.alloc, *pe, self.offset, &self.op, false)
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
impl<T> PinnedDrop for LibfabricAtomicFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<LibfabricAtomicFuture<T>> for AtomicOpHandle<T> {
    fn from(f: LibfabricAtomicFuture<T>) -> AtomicOpHandle<T> {
        AtomicOpHandle {
            future: AtomicOpFuture::Libfabric(f),
        }
    }
}

impl<T: Send + 'static> Future for LibfabricAtomicFuture<T> {
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
pub(crate) struct LibfabricAtomicFetchFuture<T> {
    pub(crate) alloc: LibfabricAlloc,
    pub(super) remote_pe: usize,
    pub(crate) offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) result: Box<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricAtomicFetchFuture<T> {
    fn exec_op(&mut self) {
        let result_ptr = self.result.as_mut() as *mut T;
        trace!(
            "performing atomic op: {:?} offset: {:?} result ptr: {:?} ",
            self.op,
            self.offset,
            result_ptr
        );
        LibfabricAlloc::atomic_fetch_op_inner(
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
impl<T> PinnedDrop for LibfabricAtomicFetchFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<LibfabricAtomicFetchFuture<T>> for AtomicFetchOpHandle<T> {
    fn from(f: LibfabricAtomicFetchFuture<T>) -> AtomicFetchOpHandle<T> {
        AtomicFetchOpHandle {
            future: AtomicFetchOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricAtomicFetchFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();

        Poll::Ready(*self.result)
    }
}

impl CommAllocAtomic for LibfabricAlloc {
    fn atomic_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        LibfabricAtomicFuture {
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
        LibfabricAlloc::atomic_op_inner(self, pe, offset, &op, true).unwrap();
    }
    fn atomic_op_unmanaged<T: Remote + 'static>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        LibfabricAlloc::atomic_op_inner(self, pe, offset, &op, false).unwrap();
    }
    fn atomic_op_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        LibfabricAtomicFuture {
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
            LibfabricAlloc::atomic_op_inner(self, pe, offset, &op, false).unwrap();
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
        LibfabricAtomicFetchFuture {
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
        LibfabricAlloc::atomic_fetch_op_inner(self, pe, offset, &op, mut_result_slice, true)
            .unwrap();
        result
    }
}

impl CommAllocAtomic for OneSidedLibfabricAlloc {
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
            "atomic op called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricAtomicFuture {
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
            "atomic op called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricAlloc::atomic_op_inner(&self.alloc, pe, offset, &op, true).unwrap();
    }

    fn atomic_op_unmanaged<T: Remote + 'static>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricAlloc::atomic_op_inner(&self.alloc, pe, offset, &op, false).unwrap();
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
            "atomic fetch op called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricAtomicFetchFuture {
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
            "blocking atomic fetch op called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut result = T::default();
        let mut_result_slice = std::slice::from_mut(&mut result);
        LibfabricAlloc::atomic_fetch_op_inner(&self.alloc, pe, offset, &op, mut_result_slice, true)
            .unwrap();
        result
    }
}

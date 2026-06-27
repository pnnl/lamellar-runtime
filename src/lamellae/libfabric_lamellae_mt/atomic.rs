use crate::{
    active_messaging::AMCounters,
    lamellae::comm::atomic::{
        AtomicCompareExchangeFuture, AtomicCompareExchangeOpHandle, AtomicFetchOpFuture,
        AtomicFetchOpHandle, AtomicOp, AtomicOpFuture, AtomicOpHandle, CommAllocAtomic,
    },
    warnings::RuntimeWarning,
    LamellarTask, Remote,
};

use super::{
    fabric::{LibfabricMtAlloc, OneSidedLibfabricMtAlloc},
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
pub(crate) struct LibfabricMtAtomicFuture<T> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(super) remote_pes: Vec<usize>,
    pub(crate) offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Send + 'static> LibfabricMtAtomicFuture<T> {
    fn exec_op(&mut self) {
        trace!(
            "performing atomic op: {:?} offset: {:?} ",
            self.op,
            self.offset
        );
        for pe in &self.remote_pes {
            LibfabricMtAlloc::atomic_op_inner(&self.alloc, *pe, self.offset, &mut self.op, false)
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
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(
            async move {
                self.alloc.ofi.wait_all().unwrap();
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricMtAtomicFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<LibfabricMtAtomicFuture<T>> for AtomicOpHandle<T> {
    fn from(f: LibfabricMtAtomicFuture<T>) -> AtomicOpHandle<T> {
        AtomicOpHandle {
            future: AtomicOpFuture::LibfabricMt(f),
        }
    }
}

impl<T: Send + 'static> Future for LibfabricMtAtomicFuture<T> {
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
pub(crate) struct LibfabricMtAtomicFetchFuture<T> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(super) remote_pe: usize,
    pub(crate) offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) result: Box<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricMtAtomicFetchFuture<T> {
    fn exec_op(&mut self) {
        let result_ptr = self.result.as_mut() as *mut T;
        trace!(
            "performing atomic op: {:?} offset: {:?} result ptr: {:?} ",
            self.op,
            self.offset,
            result_ptr
        );
        LibfabricMtAlloc::atomic_fetch_op_inner(
            &self.alloc,
            self.remote_pe,
            self.offset,
            &mut self.op,
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

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricMtAtomicFetchFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<LibfabricMtAtomicFetchFuture<T>> for AtomicFetchOpHandle<T> {
    fn from(f: LibfabricMtAtomicFetchFuture<T>) -> AtomicFetchOpHandle<T> {
        AtomicFetchOpHandle {
            future: AtomicFetchOpFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricMtAtomicFetchFuture<T> {
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
pub(crate) struct LibfabricMtAtomicCompareExchangeFuture<T> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(super) remote_pe: usize,
    pub(crate) offset: usize,
    current: Pin<Box<T>>,
    new: Pin<Box<T>>,
    pub(crate) result: Box<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote + PartialEq> LibfabricMtAtomicCompareExchangeFuture<T> {
    fn exec_op(&mut self) {
        LibfabricMtAlloc::atomic_compare_exchange_op_inner(
            &self.alloc,
            self.remote_pe,
            self.offset,
            self.current.as_ref().get_ref() as *const T,
            self.new.as_ref().get_ref() as *const T,
            std::slice::from_mut(self.result.as_mut()),
            false,
        )
        .unwrap();
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Result<T, T> {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
        compare_exchange_result(*self.result, *self.current)
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Result<T, T>> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricMtAtomicCompareExchangeFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<LibfabricMtAtomicCompareExchangeFuture<T>> for AtomicCompareExchangeOpHandle<T> {
    fn from(f: LibfabricMtAtomicCompareExchangeFuture<T>) -> AtomicCompareExchangeOpHandle<T> {
        AtomicCompareExchangeOpHandle {
            future: AtomicCompareExchangeFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote + PartialEq> Future for LibfabricMtAtomicCompareExchangeFuture<T> {
    type Output = Result<T, T>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        Poll::Ready(compare_exchange_result(*self.result, *self.current))
    }
}

impl CommAllocAtomic for LibfabricMtAlloc {
    fn atomic_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        LibfabricMtAtomicFuture {
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
    fn atomic_op_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        let mut op = op;
        LibfabricMtAlloc::atomic_op_inner(self, pe, offset, &mut op, true).unwrap();
    }
    fn atomic_op_unmanaged<T: Remote + 'static>(
        &self,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        LibfabricMtAlloc::atomic_op_inner(self, pe, offset, &mut op, false).unwrap();
    }
    fn atomic_op_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        LibfabricMtAtomicFuture {
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
    fn atomic_op_all_unmanaged<T: Remote + 'static>(&self, mut op: AtomicOp<T>, offset: usize) {
        for pe in 0..self.num_pes() {
            LibfabricMtAlloc::atomic_op_inner(self, pe, offset, &mut op, false).unwrap();
        }
    }
    fn atomic_fetch_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicFetchOpHandle<T> {
        LibfabricMtAtomicFetchFuture {
            alloc: self.clone(),
            remote_pe: pe,
            offset,
            op: op,
            result: Box::new(unsafe { std::mem::zeroed() }),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn atomic_compare_exchange<T: Remote + PartialEq>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        current: T,
        new: T,
        pe: usize,
        offset: usize,
    ) -> AtomicCompareExchangeOpHandle<T> {
        LibfabricMtAtomicCompareExchangeFuture {
            alloc: self.clone(),
            remote_pe: pe,
            offset,
            current: Box::pin(current),
            new: Box::pin(new),
            result: Box::new(unsafe { std::mem::zeroed() }),
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
        pe: usize,
        offset: usize,
    ) -> Result<T, T> {
        let mut result: T = unsafe { std::mem::zeroed() };
        LibfabricMtAlloc::atomic_compare_exchange_op_inner(
            self,
            pe,
            offset,
            &current,
            &new,
            std::slice::from_mut(&mut result),
            true,
        )
        .unwrap();
        compare_exchange_result(result, current)
    }
    fn atomic_fetch_op_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> T {
        let mut result: T = unsafe { std::mem::zeroed() };
        let mut_result_slice = std::slice::from_mut(&mut result);
        LibfabricMtAlloc::atomic_fetch_op_inner(self, pe, offset, &mut op, mut_result_slice, true)
            .unwrap();
        result
    }
}

impl CommAllocAtomic for OneSidedLibfabricMtAlloc {
    fn atomic_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedLibfabricMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricMtAtomicFuture {
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
    fn atomic_op_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedLibfabricMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricMtAlloc::atomic_op_inner(&self.alloc, pe, offset, &mut op, true).unwrap();
    }
    fn atomic_op_unmanaged<T: Remote + 'static>(
        &self,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedLibfabricMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricMtAlloc::atomic_op_inner(&self.alloc, pe, offset, &mut op, false).unwrap();
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
        self.atomic_op_unmanaged(op, self.remote_pe, offset);
    }
    fn atomic_fetch_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicFetchOpHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "atomic fetch op called on OneSidedLibfabricMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricMtAtomicFetchFuture {
            alloc: self.alloc.clone(),
            remote_pe: pe,
            offset,
            op: op,
            result: Box::new(unsafe { std::mem::zeroed() }),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn atomic_compare_exchange<T: Remote + PartialEq>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        current: T,
        new: T,
        pe: usize,
        offset: usize,
    ) -> AtomicCompareExchangeOpHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "atomic compare exchange called on OneSidedLibfabricMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricMtAtomicCompareExchangeFuture {
            alloc: self.alloc.clone(),
            remote_pe: pe,
            offset,
            current: Box::pin(current),
            new: Box::pin(new),
            result: Box::new(unsafe { std::mem::zeroed() }),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn atomic_fetch_op_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> T {
        assert_eq!(
            pe, self.remote_pe,
            "blocking atomic fetch op called on OneSidedLibfabricMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut result: T = unsafe { std::mem::zeroed() };
        let mut_result_slice = std::slice::from_mut(&mut result);
        LibfabricMtAlloc::atomic_fetch_op_inner(
            &self.alloc,
            pe,
            offset,
            &mut op,
            mut_result_slice,
            true,
        )
        .unwrap();
        result
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
            "blocking atomic compare exchange called on OneSidedLibfabricMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut result: T = unsafe { std::mem::zeroed() };
        LibfabricMtAlloc::atomic_compare_exchange_op_inner(
            &self.alloc,
            pe,
            offset,
            &current,
            &new,
            std::slice::from_mut(&mut result),
            true,
        )
        .unwrap();
        compare_exchange_result(result, current)
    }
}

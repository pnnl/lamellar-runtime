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
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
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
            LibfabricSysAlloc::atomic_op_inner(&self.alloc, *pe, self.offset, &mut self.op, false)
        }
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all()
    }
    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
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
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match self.alloc.ofi.poll_wait_for_tx_cntr() {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(()) => {}
        }
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
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
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
            &mut self.op,
            std::slice::from_mut(self.result.as_mut()),
            false,
        );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> T {
        self.exec_op();
        self.alloc.ofi.wait_all();
        *self.result
    }

    pub(crate) fn spawn(self) -> LamellarTask<T> {
        let counters = self.counters.clone();
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
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match self.alloc.ofi.poll_wait_for_rx_cntr() {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(()) => {}
        }

        Poll::Ready(*self.result)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysAtomicCompareExchangeFuture<T> {
    pub(crate) alloc: LibfabricSysAlloc,
    pub(super) remote_pe: usize,
    pub(crate) offset: usize,
    current: Pin<Box<T>>,
    new: Pin<Box<T>>,
    pub(crate) result: Box<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}
impl<T: Remote + PartialEq> LibfabricSysAtomicCompareExchangeFuture<T> {
    fn exec_op(&mut self) {
        LibfabricSysAlloc::atomic_compare_exchange_op_inner(
            &self.alloc,
            self.remote_pe,
            self.offset,
            self.current.as_ref().get_ref() as *const T,
            self.new.as_ref().get_ref() as *const T,
            std::slice::from_mut(self.result.as_mut()),
            false,
        );
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Result<T, T> {
        self.exec_op();
        self.alloc.ofi.wait_all();
        compare_exchange_result(*self.result, *self.current)
    }

    pub(crate) fn spawn(self) -> LamellarTask<Result<T, T>> {
        let counters = self.counters.clone();
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

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match self.alloc.ofi.poll_wait_for_rx_cntr() {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(()) => {}
        }
        Poll::Ready(compare_exchange_result(*self.result, *self.current))
    }
}

impl CommAllocAtomic for LibfabricSysAlloc {
    fn atomic_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
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
    fn atomic_op_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        LibfabricSysAlloc::atomic_op_inner(self, pe, offset, &mut op, true);
    }
    fn atomic_op_unmanaged<T: Remote + 'static>(
        &self,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        LibfabricSysAlloc::atomic_op_inner(self, pe, offset, &mut op, false);
    }
    fn atomic_op_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
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
    fn atomic_op_all_unmanaged<T: Remote + 'static>(&self, mut op: AtomicOp<T>, offset: usize) {
        for pe in 0..self.num_pes() {
            LibfabricSysAlloc::atomic_op_inner(self, pe, offset, &mut op, false);
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
        LibfabricSysAtomicFetchFuture {
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
    fn atomic_fetch_op_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> T {
        let mut result: T = unsafe { std::mem::zeroed() };
        let mut_result_slice = std::slice::from_mut(&mut result);
        LibfabricSysAlloc::atomic_fetch_op_inner(self, pe, offset, &mut op, mut_result_slice, true);
        result
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
        LibfabricSysAtomicCompareExchangeFuture {
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
        LibfabricSysAlloc::atomic_compare_exchange_op_inner(
            self,
            pe,
            offset,
            &current as *const T,
            &new as *const T,
            std::slice::from_mut(&mut result),
            true,
        );
        compare_exchange_result(result, current)
    }
}

impl CommAllocAtomic for OneSidedLibfabricSysAlloc {
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
    fn atomic_op_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricSysAlloc::atomic_op_inner(&self.alloc, pe, offset, &mut op, true);
    }

    fn atomic_op_unmanaged<T: Remote + 'static>(
        &self,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricSysAlloc::atomic_op_inner(&self.alloc, pe, offset, &mut op, false);
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
            "atomic fetch op called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricSysAtomicFetchFuture {
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
    fn atomic_fetch_op_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> T {
        assert_eq!(
            pe, self.remote_pe,
            "blocking atomic fetch op called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut result: T = unsafe { std::mem::zeroed() };
        let mut_result_slice = std::slice::from_mut(&mut result);
        LibfabricSysAlloc::atomic_fetch_op_inner(
            &self.alloc,
            pe,
            offset,
            &mut op,
            mut_result_slice,
            true,
        );
        result
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
            "atomic compare exchange called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricSysAtomicCompareExchangeFuture {
            alloc: self.alloc.clone(),
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
        assert_eq!(
            pe, self.remote_pe,
            "blocking atomic compare exchange called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut result: T = unsafe { std::mem::zeroed() };
        LibfabricSysAlloc::atomic_compare_exchange_op_inner(
            &self.alloc,
            pe,
            offset,
            &current as *const T,
            &new as *const T,
            std::slice::from_mut(&mut result),
            true,
        );
        compare_exchange_result(result, current)
    }
}

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
    fabric::{OneSidedUcxAlloc, UcxAlloc, UcxRequest},
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
pub(crate) struct UcxAtomicFuture<T> {
    pub(crate) alloc: UcxAlloc,
    pub(super) remote_pes: Vec<usize>,
    pub(crate) offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    pub(crate) request: Option<UcxRequest>,
}

impl<T: Remote + Send + 'static> UcxAtomicFuture<T> {
    fn exec_op(&mut self) {
        trace!(
            "performing atomic op: {:?} offset: {:?} ",
            self.op,
            self.offset
        );
        for pe in &self.remote_pes {
            self.request = UcxAlloc::inner_atomic_op(&self.alloc, *pe, self.offset, false, &self.op, true);
        }
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        if let Some(request) = self.request.take() {
            request.wait().expect("Failed to wait for UcxRequest");
        }
        self.spawned = true;
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move {
                if let Some(request) = self.request.take() {
                    request.wait().expect("Failed to wait for UcxRequest");
                }
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for UcxAtomicFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<UcxAtomicFuture<T>> for AtomicOpHandle<T> {
    fn from(f: UcxAtomicFuture<T>) -> AtomicOpHandle<T> {
        AtomicOpHandle {
            future: AtomicOpFuture::Ucx(f),
        }
    }
}

impl<T: Remote + Send + 'static> Future for UcxAtomicFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
            self.spawned = true;
        }
        if let Some(request) = self.request.take() {
            request.wait().expect("Failed to wait for UcxRequest");
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxAtomicFetchFuture<T> {
    pub(crate) alloc: UcxAlloc,
    pub(super) remote_pe: usize,
    pub(crate) offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) result: Box<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    pub(crate) request: Option<UcxRequest>,
}

impl<T: Remote + Send + 'static> UcxAtomicFetchFuture<T> {
    fn exec_op(&mut self) {
        trace!(
            "performing atomic op: {:?} offset: {:?} ",
            self.op,
            self.offset
        );
        self.request = UcxAlloc::inner_atomic_fetch_op(
            &self.alloc,
            self.remote_pe,
            self.offset,
            false,
            &self.op,
            std::slice::from_mut(self.result.as_mut()),
        );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> T {
        self.exec_op();
        if let Some(request) = self.request.take() {
            request.wait().expect("Failed to wait for UcxRequest");
        }
        *self.result
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move {
                if let Some(request) = self.request.take() {
                    request.wait().expect("Failed to wait for UcxRequest");
                }
                *self.result
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for UcxAtomicFetchFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<UcxAtomicFetchFuture<T>> for AtomicFetchOpHandle<T> {
    fn from(f: UcxAtomicFetchFuture<T>) -> AtomicFetchOpHandle<T> {
        AtomicFetchOpHandle {
            future: AtomicFetchOpFuture::Ucx(f),
        }
    }
}

impl<T: Remote + Send + 'static> Future for UcxAtomicFetchFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        if let Some(request) = self.request.take() {
            request.wait().expect("Failed to wait for UcxRequest");
        }
        Poll::Ready(*self.result)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxAtomicCompareExchangeFuture<T> {
    pub(crate) alloc: UcxAlloc,
    pub(super) remote_pe: usize,
    pub(crate) offset: usize,
    pub(super) current: T,
    pub(super) new: T,
    pub(crate) result: Box<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    pub(crate) request: Option<UcxRequest>,
}

impl<T: Remote + Send + PartialEq + 'static> UcxAtomicCompareExchangeFuture<T> {
    fn exec_op(&mut self) {
        // Pre-initialize result with `new` (UCX CSWAP uses reply_buf as both Z input and result output)
        *self.result = self.new;
        self.request = UcxAlloc::inner_atomic_compare_exchange_op(
            &self.alloc,
            self.remote_pe,
            self.offset,
            false,
            self.current,
            std::slice::from_mut(self.result.as_mut()),
        );
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Result<T, T> {
        self.exec_op();
        if let Some(request) = self.request.take() {
            request.wait().expect("Failed to wait for UcxRequest");
        }
        compare_exchange_result(*self.result, self.current)
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Result<T, T>> {
        self.exec_op();
        let current = self.current;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move {
                if let Some(request) = self.request.take() {
                    request.wait().expect("Failed to wait for UcxRequest");
                }
                compare_exchange_result(*self.result, current)
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for UcxAtomicCompareExchangeFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<UcxAtomicCompareExchangeFuture<T>> for AtomicCompareExchangeOpHandle<T> {
    fn from(f: UcxAtomicCompareExchangeFuture<T>) -> AtomicCompareExchangeOpHandle<T> {
        AtomicCompareExchangeOpHandle {
            future: AtomicCompareExchangeFuture::Ucx(f),
        }
    }
}

impl<T: Remote + Send + PartialEq + 'static> Future for UcxAtomicCompareExchangeFuture<T> {
    type Output = Result<T, T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        if let Some(request) = self.request.take() {
            request.wait().expect("Failed to wait for UcxRequest");
        }
        Poll::Ready(compare_exchange_result(*self.result, self.current))
    }
}

impl CommAllocAtomic for UcxAlloc {
    fn atomic_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        UcxAtomicFuture {
            alloc: self.clone(),
            remote_pes: vec![pe],
            offset,
            op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
        }
        .into()
    }
    fn atomic_op_blocking<T: Remote>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        UcxAlloc::inner_atomic_op(self, pe, offset, true, &op, true);
    }
    fn atomic_op_unmanaged<T: Remote + 'static>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        UcxAlloc::inner_atomic_op(self, pe, offset, false, &op, false);
    }
    fn atomic_op_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        let pes = (0..self.num_pes).collect();
        UcxAtomicFuture {
            alloc: self.clone(),
            remote_pes: pes,
            offset,
            op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
        }
        .into()
    }
    fn atomic_op_all_unmanaged<T: Remote + 'static>(&self, op: AtomicOp<T>, offset: usize) {
        for pe in 0..self.num_pes {
            UcxAlloc::inner_atomic_op(self, pe, offset, false, &op, false);
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
        UcxAtomicFetchFuture {
            alloc: self.clone(),
            remote_pe: pe,
            offset,
            op: op,
            result: Box::new(T::default()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
        }
        .into()
    }

    fn blocking_atomic_fetch_op<T: Remote>(&self, op: AtomicOp<T>, pe: usize, offset: usize) -> T {
        let mut result = T::default();
        UcxAlloc::inner_atomic_fetch_op(
            self,
            pe,
            offset,
            true,
            &op,
            std::slice::from_mut(&mut result),
        );
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
        UcxAtomicCompareExchangeFuture {
            alloc: self.clone(),
            remote_pe: pe,
            offset,
            current,
            new,
            result: Box::new(new),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
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
        let mut result = new;
        UcxAlloc::inner_atomic_compare_exchange_op(
            self,
            pe,
            offset,
            true,
            current,
            std::slice::from_mut(&mut result),
        );
        compare_exchange_result(result, current)
    }
}

impl CommAllocAtomic for OneSidedUcxAlloc {
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
            "atomic op called on OneSidedUcxAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxAtomicFuture {
            alloc: self.alloc.clone(),
            remote_pes: vec![pe],
            offset,
            op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
        }
        .into()
    }
    fn atomic_op_blocking<T: Remote>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedUcxAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxAlloc::inner_atomic_op(&self.alloc, pe, offset, true, &op, true);
    }
    fn atomic_op_unmanaged<T: Remote + 'static>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedUcxAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxAlloc::inner_atomic_op(&self.alloc, pe, offset, false, &op, false);
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
            "atomic fetch op called on OneSidedUcxAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxAtomicFetchFuture {
            alloc: self.alloc.clone(),
            remote_pe: pe,
            offset,
            op: op,
            result: Box::new(T::default()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
        }
        .into()
    }

    fn blocking_atomic_fetch_op<T: Remote>(&self, op: AtomicOp<T>, pe: usize, offset: usize) -> T {
        assert_eq!(
            pe, self.remote_pe,
            "atomic fetch op called on OneSidedUcxAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut result = T::default();
        UcxAlloc::inner_atomic_fetch_op(
            &self.alloc,
            pe,
            offset,
            true,
            &op,
            std::slice::from_mut(&mut result),
        );
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
            "atomic compare exchange called on OneSidedUcxAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxAtomicCompareExchangeFuture {
            alloc: self.alloc.clone(),
            remote_pe: pe,
            offset,
            current,
            new,
            result: Box::new(new),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
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
            "blocking atomic compare exchange called on OneSidedUcxAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut result = new;
        UcxAlloc::inner_atomic_compare_exchange_op(
            &self.alloc,
            pe,
            offset,
            true,
            current,
            std::slice::from_mut(&mut result),
        );
        compare_exchange_result(result, current)
    }
}

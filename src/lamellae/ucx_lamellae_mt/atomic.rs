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
    fabric::{OneSidedUcxMtAlloc, UcxMtAlloc, UcxRequest},
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
pub(crate) struct UcxMtAtomicFuture<T> {
    pub(crate) alloc: UcxMtAlloc,
    pub(super) remote_pes: Vec<usize>,
    pub(crate) offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
    pub(crate) request: Option<UcxRequest>,
}

impl<T: Remote + Send + 'static> UcxMtAtomicFuture<T> {
    fn exec_op(&mut self) {
        trace!(
            "performing atomic op: {:?} offset: {:?} ",
            self.op,
            self.offset
        );
        for pe in &self.remote_pes {
            self.request = UcxMtAlloc::inner_atomic_op(
                &self.alloc,
                *pe,
                self.offset,
                false,
                &mut self.op,
                true,
            );
        }
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        if let Some(request) = self.request.take() {
            request.wait().expect("Failed to wait for UcxRequest");
        } else {
            self.alloc.wait_all();
        }
        self.spawned = true;
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        self.spawned = true;
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(
            async move {
                if let Some(request) = self.request.take() {
                    request.wait().expect("Failed to wait for UcxRequest");
                } else {
                    self.alloc.wait_all();
                }
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for UcxMtAtomicFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<UcxMtAtomicFuture<T>> for AtomicOpHandle<T> {
    fn from(f: UcxMtAtomicFuture<T>) -> AtomicOpHandle<T> {
        AtomicOpHandle {
            future: AtomicOpFuture::UcxMt(f),
        }
    }
}

impl<T: Remote + Send + 'static> Future for UcxMtAtomicFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
            self.spawned = true;
        }
        if let Some(request) = self.request.take() {
            request.wait().expect("Failed to wait for UcxRequest");
        } else {
            self.alloc.wait_all();
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtAtomicFetchFuture<T> {
    pub(crate) alloc: UcxMtAlloc,
    pub(super) remote_pe: usize,
    pub(crate) offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) result: Box<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
    pub(crate) request: Option<UcxRequest>,
}

impl<T: Remote + Send + 'static> UcxMtAtomicFetchFuture<T> {
    fn exec_op(&mut self) {
        trace!(
            "performing atomic op: {:?} offset: {:?} ",
            self.op,
            self.offset
        );
        self.request = UcxMtAlloc::inner_atomic_fetch_op(
            &self.alloc,
            self.remote_pe,
            self.offset,
            false,
            &mut self.op,
            std::slice::from_mut(self.result.as_mut()),
        );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> T {
        self.exec_op();
        if let Some(request) = self.request.take() {
            request.wait().expect("Failed to wait for UcxRequest");
        } else {
            self.alloc.wait_all();
        }
        *self.result
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(
            async move {
                if let Some(request) = self.request.take() {
                    request.wait().expect("Failed to wait for UcxRequest");
                } else {
                    self.alloc.wait_all();
                }
                *self.result
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for UcxMtAtomicFetchFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<UcxMtAtomicFetchFuture<T>> for AtomicFetchOpHandle<T> {
    fn from(f: UcxMtAtomicFetchFuture<T>) -> AtomicFetchOpHandle<T> {
        AtomicFetchOpHandle {
            future: AtomicFetchOpFuture::UcxMt(f),
        }
    }
}

impl<T: Remote + Send + 'static> Future for UcxMtAtomicFetchFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        if let Some(request) = self.request.take() {
            request.wait().expect("Failed to wait for UcxRequest");
        } else {
            self.alloc.wait_all();
        }
        Poll::Ready(*self.result)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtAtomicCompareExchangeFuture<T> {
    pub(crate) alloc: UcxMtAlloc,
    pub(super) remote_pe: usize,
    pub(crate) offset: usize,
    pub(super) current: Pin<Box<T>>,
    pub(super) new: T,
    pub(crate) result: Box<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
    pub(crate) request: Option<UcxRequest>,
}

impl<T: Remote + Send + PartialEq + 'static> UcxMtAtomicCompareExchangeFuture<T> {
    fn exec_op(&mut self) {
        // Pre-initialize result with `new` (UCX CSWAP uses reply_buf as both Z input and result output)
        *self.result = self.new;
        self.request = UcxMtAlloc::inner_atomic_compare_exchange_op(
            &self.alloc,
            self.remote_pe,
            self.offset,
            false,
            self.current.as_ref().get_ref(),
            std::slice::from_mut(self.result.as_mut()),
        );
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Result<T, T> {
        self.exec_op();
        if let Some(request) = self.request.take() {
            request.wait().expect("Failed to wait for UcxRequest");
        } else {
            self.alloc.wait_all();
        }
        compare_exchange_result(*self.result, *self.current)
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Result<T, T>> {
        self.exec_op();
        let counters = self.counters.clone();
        let alloc = self.alloc.clone();
        self.scheduler.clone().spawn_task(
            async move {
                if let Some(request) = self.request.take() {
                    request.wait().expect("Failed to wait for UcxRequest");
                } else {
                    alloc.wait_all();
                }
                compare_exchange_result(*self.result, *self.current)
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for UcxMtAtomicCompareExchangeFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<UcxMtAtomicCompareExchangeFuture<T>> for AtomicCompareExchangeOpHandle<T> {
    fn from(f: UcxMtAtomicCompareExchangeFuture<T>) -> AtomicCompareExchangeOpHandle<T> {
        AtomicCompareExchangeOpHandle {
            future: AtomicCompareExchangeFuture::UcxMt(f),
        }
    }
}

impl<T: Remote + Send + PartialEq + 'static> Future for UcxMtAtomicCompareExchangeFuture<T> {
    type Output = Result<T, T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        if let Some(request) = self.request.take() {
            request.wait().expect("Failed to wait for UcxRequest");
        } else {
            self.alloc.wait_all();
        }
        Poll::Ready(compare_exchange_result(*self.result, *self.current))
    }
}

impl CommAllocAtomic for UcxMtAlloc {
    fn atomic_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        UcxMtAtomicFuture {
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
    fn atomic_op_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        UcxMtAlloc::inner_atomic_op(self, pe, offset, true, &mut op, true);
    }
    fn atomic_op_unmanaged<T: Remote + 'static>(
        &self,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        UcxMtAlloc::inner_atomic_op(self, pe, offset, false, &mut op, false);
    }
    fn atomic_op_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        let pes = (0..self.num_pes).collect();
        UcxMtAtomicFuture {
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
    fn atomic_op_all_unmanaged<T: Remote + 'static>(&self, mut op: AtomicOp<T>, offset: usize) {
        for pe in 0..self.num_pes {
            UcxMtAlloc::inner_atomic_op(self, pe, offset, false, &mut op, false);
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
        UcxMtAtomicFetchFuture {
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

    fn atomic_fetch_op_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> T {
        let mut result = T::default();
        UcxMtAlloc::inner_atomic_fetch_op(
            self,
            pe,
            offset,
            true,
            &mut op,
            std::slice::from_mut(&mut result),
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
        UcxMtAtomicCompareExchangeFuture {
            alloc: self.clone(),
            remote_pe: pe,
            offset,
            current: Box::pin(current),
            new,
            result: Box::new(new),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
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
        let mut result = new;
        UcxMtAlloc::inner_atomic_compare_exchange_op(
            self,
            pe,
            offset,
            true,
            &current,
            std::slice::from_mut(&mut result),
        );
        compare_exchange_result(result, current)
    }
}

impl CommAllocAtomic for OneSidedUcxMtAlloc {
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
            "atomic op called on OneSidedUcxMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxMtAtomicFuture {
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
    fn atomic_op_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedUcxMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxMtAlloc::inner_atomic_op(&self.alloc, pe, offset, true, &mut op, true);
    }
    fn atomic_op_unmanaged<T: Remote + 'static>(
        &self,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedUcxMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxMtAlloc::inner_atomic_op(&self.alloc, pe, offset, false, &mut op, false);
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
            "atomic fetch op called on OneSidedUcxMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxMtAtomicFetchFuture {
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

    fn atomic_fetch_op_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> T {
        assert_eq!(
            pe, self.remote_pe,
            "atomic fetch op called on OneSidedUcxMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut result = T::default();
        UcxMtAlloc::inner_atomic_fetch_op(
            &self.alloc,
            pe,
            offset,
            true,
            &mut op,
            std::slice::from_mut(&mut result),
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
            "atomic compare exchange called on OneSidedUcxMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxMtAtomicCompareExchangeFuture {
            alloc: self.alloc.clone(),
            remote_pe: pe,
            offset,
            current: Box::pin(current),
            new,
            result: Box::new(new),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
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
            "blocking atomic compare exchange called on OneSidedUcxMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut result = new;
        UcxMtAlloc::inner_atomic_compare_exchange_op(
            &self.alloc,
            pe,
            offset,
            true,
            &current,
            std::slice::from_mut(&mut result),
        );
        compare_exchange_result(result, current)
    }
}

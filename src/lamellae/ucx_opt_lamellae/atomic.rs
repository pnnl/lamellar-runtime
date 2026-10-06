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
    fabric::{AllocFlushState, OneSidedUcxOptAlloc, UcxOptAlloc, UcxOptRequest},
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
pub(crate) struct UcxOptAtomicFuture<T> {
    pub(crate) alloc: UcxOptAlloc,
    pub(super) remote_pes: Vec<usize>,
    pub(crate) offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
    pub(crate) request: Option<UcxOptRequest>,
    pub(crate) flush_state: AllocFlushState,
}

impl<T: Remote + Send + 'static> UcxOptAtomicFuture<T> {
    fn exec_op(&mut self) {
        trace!(
            "performing atomic op: {:?} offset: {:?} ",
            self.op,
            self.offset
        );
        for pe in &self.remote_pes {
            self.request =
                UcxOptAlloc::inner_atomic_op(&self.alloc, *pe, self.offset, false, &mut self.op, true);
        }
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        if let Some(request) = self.request.take() {
            request.wait().expect("Failed to wait for UcxOptRequest");
        } else {
            self.alloc.wait_all();
        }
        self.spawned = true;
    }
    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for UcxOptAtomicFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<UcxOptAtomicFuture<T>> for AtomicOpHandle<T> {
    fn from(f: UcxOptAtomicFuture<T>) -> AtomicOpHandle<T> {
        AtomicOpHandle {
            future: AtomicOpFuture::UcxOpt(f),
        }
    }
}

impl<T: Remote + Send + 'static> Future for UcxOptAtomicFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
            self.spawned = true;
        }
        let this = self.project();
        if let Some(request) = this.request.as_mut() {
            match request.poll_wait(cx) {
                // D5/U3: `poll_wait` already self-rewakes when it couldn't take the progress
                // lock, and otherwise trusts the ticket's registered waker.
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => panic!("ucx atomic op failed: {:?}", e),
            }
        } else {
            match this.alloc.poll_wait_all(this.flush_state) {
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => panic!("ucx atomic op failed: {:?}", e),
            }
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptAtomicFetchFuture<T> {
    pub(crate) alloc: UcxOptAlloc,
    pub(super) remote_pe: usize,
    pub(crate) offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) result: Box<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
    pub(crate) request: Option<UcxOptRequest>,
    pub(crate) flush_state: AllocFlushState,
}

impl<T: Remote + Send + 'static> UcxOptAtomicFetchFuture<T> {
    fn exec_op(&mut self) {
        trace!(
            "performing atomic op: {:?} offset: {:?} ",
            self.op,
            self.offset
        );
        self.request = UcxOptAlloc::inner_atomic_fetch_op(
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
            request.wait().expect("Failed to wait for UcxOptRequest");
        } else {
            self.alloc.wait_all();
        }
        *self.result
    }

    pub(crate) fn spawn(self) -> LamellarTask<T> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for UcxOptAtomicFetchFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<UcxOptAtomicFetchFuture<T>> for AtomicFetchOpHandle<T> {
    fn from(f: UcxOptAtomicFetchFuture<T>) -> AtomicFetchOpHandle<T> {
        AtomicFetchOpHandle {
            future: AtomicFetchOpFuture::UcxOpt(f),
        }
    }
}

impl<T: Remote + Send + 'static> Future for UcxOptAtomicFetchFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        if let Some(request) = this.request.as_mut() {
            match request.poll_wait(cx) {
                // D5/U3: `poll_wait` already self-rewakes when it couldn't take the progress
                // lock, and otherwise trusts the ticket's registered waker.
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => panic!("ucx atomic fetch op failed: {:?}", e),
            }
        } else {
            match this.alloc.poll_wait_all(this.flush_state) {
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => panic!("ucx atomic fetch op failed: {:?}", e),
            }
        }
        Poll::Ready(**this.result)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptAtomicCompareExchangeFuture<T> {
    pub(crate) alloc: UcxOptAlloc,
    pub(super) remote_pe: usize,
    pub(crate) offset: usize,
    pub(super) current: Pin<Box<T>>,
    pub(super) new: T,
    pub(crate) result: Box<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
    pub(crate) request: Option<UcxOptRequest>,
    pub(crate) flush_state: AllocFlushState,
}

impl<T: Remote + Send + PartialEq + 'static> UcxOptAtomicCompareExchangeFuture<T> {
    fn exec_op(&mut self) {
        // Pre-initialize result with `new` (UCX CSWAP uses reply_buf as both Z input and result output)
        *self.result = self.new;
        self.request = UcxOptAlloc::inner_atomic_compare_exchange_op(
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
            request.wait().expect("Failed to wait for UcxOptRequest");
        } else {
            self.alloc.wait_all();
        }
        compare_exchange_result(*self.result, *self.current)
    }

    pub(crate) fn spawn(self) -> LamellarTask<Result<T, T>> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for UcxOptAtomicCompareExchangeFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<UcxOptAtomicCompareExchangeFuture<T>> for AtomicCompareExchangeOpHandle<T> {
    fn from(f: UcxOptAtomicCompareExchangeFuture<T>) -> AtomicCompareExchangeOpHandle<T> {
        AtomicCompareExchangeOpHandle {
            future: AtomicCompareExchangeFuture::UcxOpt(f),
        }
    }
}

impl<T: Remote + Send + PartialEq + 'static> Future for UcxOptAtomicCompareExchangeFuture<T> {
    type Output = Result<T, T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        if let Some(request) = this.request.as_mut() {
            match request.poll_wait(cx) {
                // D5/U3: `poll_wait` already self-rewakes when it couldn't take the progress
                // lock, and otherwise trusts the ticket's registered waker.
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => panic!("ucx atomic compare exchange failed: {:?}", e),
            }
        } else {
            match this.alloc.poll_wait_all(this.flush_state) {
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => panic!("ucx atomic compare exchange failed: {:?}", e),
            }
        }
        Poll::Ready(compare_exchange_result(**this.result, **this.current))
    }
}

impl CommAllocAtomic for UcxOptAlloc {
    fn atomic_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        UcxOptAtomicFuture {
            alloc: self.clone(),
            remote_pes: vec![pe],
            offset,
            op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
            flush_state: AllocFlushState::default(),
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
        UcxOptAlloc::inner_atomic_op(self, pe, offset, true, &mut op, true);
    }
    fn atomic_op_unmanaged<T: Remote + 'static>(
        &self,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        UcxOptAlloc::inner_atomic_op(self, pe, offset, false, &mut op, false);
    }
    fn atomic_op_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        let pes = (0..self.num_pes).collect();
        UcxOptAtomicFuture {
            alloc: self.clone(),
            remote_pes: pes,
            offset,
            op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
            flush_state: AllocFlushState::default(),
        }
        .into()
    }
    fn atomic_op_all_unmanaged<T: Remote + 'static>(&self, mut op: AtomicOp<T>, offset: usize) {
        for pe in 0..self.num_pes {
            UcxOptAlloc::inner_atomic_op(self, pe, offset, false, &mut op, false);
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
        UcxOptAtomicFetchFuture {
            alloc: self.clone(),
            remote_pe: pe,
            offset,
            op: op,
            result: Box::new(unsafe { std::mem::zeroed() }),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
            flush_state: AllocFlushState::default(),
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
        UcxOptAlloc::inner_atomic_fetch_op(
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
        UcxOptAtomicCompareExchangeFuture {
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
            flush_state: AllocFlushState::default(),
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
        UcxOptAlloc::inner_atomic_compare_exchange_op(
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

impl CommAllocAtomic for OneSidedUcxOptAlloc {
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
            "atomic op called on OneSidedUcxOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxOptAtomicFuture {
            alloc: self.alloc.clone(),
            remote_pes: vec![pe],
            offset,
            op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
            flush_state: AllocFlushState::default(),
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
            "atomic op called on OneSidedUcxOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxOptAlloc::inner_atomic_op(&self.alloc, pe, offset, true, &mut op, true);
    }
    fn atomic_op_unmanaged<T: Remote + 'static>(
        &self,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedUcxOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxOptAlloc::inner_atomic_op(&self.alloc, pe, offset, false, &mut op, false);
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
            "atomic fetch op called on OneSidedUcxOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxOptAtomicFetchFuture {
            alloc: self.alloc.clone(),
            remote_pe: pe,
            offset,
            op: op,
            result: Box::new(unsafe { std::mem::zeroed() }),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
            flush_state: AllocFlushState::default(),
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
            "atomic fetch op called on OneSidedUcxOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut result: T = unsafe { std::mem::zeroed() };
        UcxOptAlloc::inner_atomic_fetch_op(
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
            "atomic compare exchange called on OneSidedUcxOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxOptAtomicCompareExchangeFuture {
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
            flush_state: AllocFlushState::default(),
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
            "blocking atomic compare exchange called on OneSidedUcxOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut result = new;
        UcxOptAlloc::inner_atomic_compare_exchange_op(
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

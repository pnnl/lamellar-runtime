use crate::{
    active_messaging::AMCounters,
    lamellae::{
        collective::{
            BroadcastInput, CollectiveAllGatherIntoBufferOpFuture,
            CollectiveAllGatherIntoBufferOpHandle, CollectiveAllGatherOpFuture,
            CollectiveAllGatherOpHandle, CollectiveAllReduceInPlaceOpFuture,
            CollectiveAllReduceInPlaceOpHandle, CollectiveAllReduceIntoBufferOpFuture,
            CollectiveAllReduceIntoBufferOpHandle, CollectiveAllReduceOpFuture,
            CollectiveAllReduceOpHandle, CollectiveAllToAllIntoBufferOpFuture,
            CollectiveAllToAllIntoBufferOpHandle, CollectiveAllToAllOpFuture,
            CollectiveAllToAllOpHandle, CollectiveBroadcastIntoBufferOpFuture,
            CollectiveBroadcastIntoBufferOpHandle, CollectiveBroadcastOpFuture,
            CollectiveBroadcastOpHandle, CollectiveGatherIntoBufferOpFuture,
            CollectiveGatherIntoBufferOpHandle, CollectiveGatherOpFuture, CollectiveGatherOpHandle,
            CollectiveReduceIntoBufferOpFuture, CollectiveReduceIntoBufferOpHandle,
            CollectiveReduceOpFuture, CollectiveReduceOpHandle,
            CollectiveReduceScatterIntoBufferOpFuture, CollectiveReduceScatterIntoBufferOpHandle,
            CollectiveReduceScatterOpFuture, CollectiveReduceScatterOpHandle,
            CollectiveScatterIntoBufferOpFuture, CollectiveScatterIntoBufferOpHandle,
            CollectiveScatterOpFuture, CollectiveScatterOpHandle, CommAllocCollectiveAllGather,
            CommAllocCollectiveAllReduce, CommAllocCollectiveAllToAll,
            CommAllocCollectiveBroadcast, CommAllocCollectiveGather, CommAllocCollectiveReduce,
            CommAllocCollectiveReduceScatter, CommAllocCollectiveScatter, ReduceOp, RootOrBuffer,
            RootOrLamellarBuffer, RootSrcOrBuffer, RootSrcOrLamellarBuffer,
            RootSrcOrLamellarBufferInner, ScatterInput, ScatterInputInner,
        },
        ucx_opt_lamellae::ucc::UccRequest,
    },
    // memregion::MemregionRdmaInputInner,
    scheduler::Scheduler,
    warnings::RuntimeWarning,
    AsLamellarBuffer,
    LamellarBuffer,
    LamellarTask,
    Remote,
};

use super::fabric::UcxOptAlloc;
use pin_project::{pin_project, pinned_drop};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptCollectiveAllReduceFuture<T: Remote> {
    pub(crate) alloc: UcxOptAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxOptCollectiveAllReduceFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        let req = self
            .alloc
            .allreduce_inner(&self.op, src, &mut self.result, false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc
            .wait_ucc_request(self.req.as_ref().unwrap())
            .unwrap();
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        res
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for UcxOptCollectiveAllReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxOptCollectiveAllReduceFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxOptCollectiveAllReduceFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        match this.alloc.poll_wait_ucc_request(this.req.as_ref().unwrap()) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => panic!("ucx collective allreduce failed: {:?}", e),
        }
        let mut res = Vec::new();
        std::mem::swap(this.result, &mut res);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptCollectiveAllReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxOptAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxOptCollectiveAllReduceIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        let req = self
            .alloc
            .allreduce_inner(&self.op, src, self.result.as_mut_slice(), false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc
            .wait_ucc_request(self.req.as_ref().unwrap())
            .unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for UcxOptCollectiveAllReduceIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxOptCollectiveAllReduceIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxOptCollectiveAllReduceIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        match this.alloc.poll_wait_ucc_request(this.req.as_ref().unwrap()) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => panic!("ucx collective allreduce into buffer failed: {:?}", e),
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptCollectiveAllReduceInPlaceFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxOptAlloc,
    pub(super) op: ReduceOp,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxOptCollectiveAllReduceInPlaceFuture<T, B> {
    fn exec_op(&mut self) {
        let req = self
            .alloc
            .allreduce_inplace_inner(&self.op, self.result.as_mut_slice(), false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc
            .wait_ucc_request(self.req.as_ref().unwrap())
            .unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxOptCollectiveAllReduceInPlaceFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxOptCollectiveAllReduceInPlaceFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxOptCollectiveAllReduceInPlaceFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        match this.alloc.poll_wait_ucc_request(this.req.as_ref().unwrap()) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => panic!("ucx collective allreduce in place failed: {:?}", e),
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptCollectiveReduceFuture<T: Remote> {
    pub(crate) alloc: UcxOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(super) op: ReduceOp,
    pub(crate) target: RootOrBuffer<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxOptCollectiveReduceFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        let req = self
            .alloc
            .reduce_inner(&self.op, src, self.target.as_mut_slice(), false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        self.alloc
            .wait_ucc_request(self.req.as_ref().unwrap())
            .unwrap();
        match &mut self.target {
            RootOrBuffer::Root(res) => {
                let mut out = Vec::new();
                std::mem::swap(&mut out, res);
                Some(out)
            }
            RootOrBuffer::NotRoot(_) => None,
        }
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Option<Vec<T>>> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for UcxOptCollectiveReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxOptCollectiveReduceFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxOptCollectiveReduceFuture<T> {
    type Output = Option<Vec<T>>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        match this.alloc.poll_wait_ucc_request(this.req.as_ref().unwrap()) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => panic!("ucx collective reduce failed: {:?}", e),
        }
        match this.target {
            RootOrBuffer::Root(res) => {
                let mut out = Vec::new();
                std::mem::swap(&mut out, res);
                Poll::Ready(Some(out))
            }
            RootOrBuffer::NotRoot(_) => Poll::Ready(None),
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptCollectiveReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(super) op: ReduceOp,
    pub(crate) target: RootOrLamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxOptCollectiveReduceIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        let req = self
            .alloc
            .reduce_inner(&self.op, src, self.target.as_mut_slice(), false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc
            .wait_ucc_request(self.req.as_ref().unwrap())
            .unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxOptCollectiveReduceIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxOptCollectiveReduceIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxOptCollectiveReduceIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        match this.alloc.poll_wait_ucc_request(this.req.as_ref().unwrap()) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => panic!("ucx collective reduce into buffer failed: {:?}", e),
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptCollectiveReduceInPlaceFuture<T> {
    pub(crate) alloc: UcxOptAlloc,
    pub(super) op: ReduceOp,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
    phantom: std::marker::PhantomData<T>,
}

impl<T: Remote> UcxOptCollectiveReduceInPlaceFuture<T> {
    #[allow(dead_code)] // WIP: reduce-in-place not yet wired up in ucx
    pub(crate) fn block(mut self) {
        self.spawned = true;
    }

    #[allow(dead_code)] // WIP: reduce-in-place not yet wired up in ucx
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.spawned = true;
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for UcxOptCollectiveReduceInPlaceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxOptCollectiveReduceInPlaceFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxOptCollectiveReduceInPlaceFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.spawned = true;
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptCollectiveAllGatherFuture<T: Remote> {
    pub(crate) alloc: UcxOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxOptCollectiveAllGatherFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        let req = self
            .alloc
            .allgather_inner(src, &mut self.result, false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc
            .wait_ucc_request(self.req.as_ref().unwrap())
            .unwrap();
        let mut out = Vec::new();
        std::mem::swap(&mut out, &mut self.result);
        out
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for UcxOptCollectiveAllGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxOptCollectiveAllGatherFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxOptCollectiveAllGatherFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        match this.alloc.poll_wait_ucc_request(this.req.as_ref().unwrap()) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => panic!("ucx collective allgather failed: {:?}", e),
        }
        let mut out = Vec::new();
        std::mem::swap(&mut out, this.result);
        Poll::Ready(out)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptCollectiveAllGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxOptCollectiveAllGatherIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        let req = self
            .alloc
            .allgather_inner(src, self.result.as_mut_slice(), false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc
            .wait_ucc_request(self.req.as_ref().unwrap())
            .unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for UcxOptCollectiveAllGatherIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxOptCollectiveAllGatherIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxOptCollectiveAllGatherIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        match this.alloc.poll_wait_ucc_request(this.req.as_ref().unwrap()) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => panic!("ucx collective allgather into buffer failed: {:?}", e),
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptCollectiveGatherFuture<T: Remote> {
    pub(crate) alloc: UcxOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) target: RootOrBuffer<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxOptCollectiveGatherFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        let req = self
            .alloc
            .gather_inner(src, self.target.as_mut_slice(), false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        self.alloc
            .wait_ucc_request(self.req.as_ref().unwrap())
            .unwrap();
        match &mut self.target {
            RootOrBuffer::Root(res) => {
                let mut out = Vec::new();
                std::mem::swap(&mut out, res);
                Some(out)
            }
            RootOrBuffer::NotRoot(_) => None,
        }
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Option<Vec<T>>> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for UcxOptCollectiveGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxOptCollectiveGatherFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxOptCollectiveGatherFuture<T> {
    type Output = Option<Vec<T>>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        match this.alloc.poll_wait_ucc_request(this.req.as_ref().unwrap()) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => panic!("ucx collective gather failed: {:?}", e),
        }
        match this.target {
            RootOrBuffer::Root(res) => {
                let mut out = Vec::new();
                std::mem::swap(&mut out, res);
                Poll::Ready(Some(out))
            }
            RootOrBuffer::NotRoot(_) => Poll::Ready(None),
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptCollectiveGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) target: RootOrLamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxOptCollectiveGatherIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        let req = self
            .alloc
            .gather_inner(src, self.target.as_mut_slice(), false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc
            .wait_ucc_request(self.req.as_ref().unwrap())
            .unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxOptCollectiveGatherIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxOptCollectiveGatherIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxOptCollectiveGatherIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        match this.alloc.poll_wait_ucc_request(this.req.as_ref().unwrap()) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => panic!("ucx collective gather into buffer failed: {:?}", e),
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptCollectiveAllToAllFuture<T: Remote> {
    pub(crate) alloc: UcxOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxOptCollectiveAllToAllFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        let req = self
            .alloc
            .alltoall_inner(src, &mut self.result, false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc
            .wait_ucc_request(self.req.as_ref().unwrap())
            .unwrap();
        let mut out = Vec::new();
        std::mem::swap(&mut out, &mut self.result);
        out
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for UcxOptCollectiveAllToAllFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxOptCollectiveAlToAllFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxOptCollectiveAllToAllFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        match this.alloc.poll_wait_ucc_request(this.req.as_ref().unwrap()) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => panic!("ucx collective alltoall failed: {:?}", e),
        }
        let mut out = Vec::new();
        std::mem::swap(&mut out, this.result);
        Poll::Ready(out)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptCollectiveAllToAllIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxOptCollectiveAllToAllIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        let req = self
            .alloc
            .alltoall_inner(src, self.result.as_mut_slice(), false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc
            .wait_ucc_request(self.req.as_ref().unwrap())
            .unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxOptCollectiveAllToAllIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxOptCollectiveAllToAllIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxOptCollectiveAllToAllIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        match this.alloc.poll_wait_ucc_request(this.req.as_ref().unwrap()) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => panic!("ucx collective alltoall into buffer failed: {:?}", e),
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptCollectiveBroadcastFuture<T: Remote> {
    pub(crate) alloc: UcxOptAlloc,
    pub(crate) target: RootSrcOrBuffer<T>,
    pub(crate) len: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxOptCollectiveBroadcastFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice() };
        let req = self
            .alloc
            .broadcast_inner(self.target.as_mut_slice(alloc_slice, self.len), false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        self.alloc
            .wait_ucc_request(self.req.as_ref().unwrap())
            .unwrap();
        match &mut self.target {
            RootSrcOrBuffer::Root(_) => None,
            RootSrcOrBuffer::NotRoot(items, _) => {
                let mut out = Vec::new();
                std::mem::swap(items, &mut out);
                Some(out)
            }
        }
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Option<Vec<T>>> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for UcxOptCollectiveBroadcastFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxOptCollectiveBroadcastFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxOptCollectiveBroadcastFuture<T> {
    type Output = Option<Vec<T>>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        match this.alloc.poll_wait_ucc_request(this.req.as_ref().unwrap()) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => panic!("ucx collective broadcast failed: {:?}", e),
        }
        match this.target {
            RootSrcOrBuffer::Root(_) => Poll::Ready(None),
            RootSrcOrBuffer::NotRoot(items, _) => {
                let mut out = Vec::new();
                std::mem::swap(items, &mut out);
                Poll::Ready(Some(out))
            }
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptCollectiveBroadcastIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxOptAlloc,
    pub(crate) target: RootSrcOrLamellarBufferInner<T, B>,
    pub(crate) len: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxOptCollectiveBroadcastIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice() };
        let req = self
            .alloc
            .broadcast_inner(self.target.as_mut_slice(alloc_slice, self.len), false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc
            .wait_ucc_request(self.req.as_ref().unwrap())
            .unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for UcxOptCollectiveBroadcastIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxOptCollectiveBroadcastIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxOptCollectiveBroadcastIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        match this.alloc.poll_wait_ucc_request(this.req.as_ref().unwrap()) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => panic!("ucx collective broadcast into buffer failed: {:?}", e),
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptCollectiveScatterFuture<T: Remote> {
    pub(crate) alloc: UcxOptAlloc,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    src_or_root_pe: ScatterInputInner,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxOptCollectiveScatterFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        let req = self
            .alloc
            .scatter_inner(
                &mut self.result,
                self.src_or_root_pe.as_slice(alloc_slice, self.len),
                false,
            )
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc
            .wait_ucc_request(self.req.as_ref().unwrap())
            .unwrap();
        let mut out = Vec::new();
        std::mem::swap(&mut out, &mut self.result);
        out
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for UcxOptCollectiveScatterFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxOptCollectiveScatterFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxOptCollectiveScatterFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        match this.alloc.poll_wait_ucc_request(this.req.as_ref().unwrap()) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => panic!("ucx collective scatter failed: {:?}", e),
        }
        let mut out = Vec::new();
        std::mem::swap(&mut out, this.result);
        Poll::Ready(out)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptCollectiveScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxOptAlloc,
    pub(crate) len: usize,
    src_or_root_pe: ScatterInputInner,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxOptCollectiveScatterIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        let req = self
            .alloc
            .scatter_inner(
                self.result.as_mut_slice(),
                self.src_or_root_pe.as_slice(alloc_slice, self.len),
                false,
            )
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc
            .wait_ucc_request(self.req.as_ref().unwrap())
            .unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxOptCollectiveScatterIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxOptCollectiveScatterIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxOptCollectiveScatterIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        match this.alloc.poll_wait_ucc_request(this.req.as_ref().unwrap()) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => panic!("ucx collective scatter into buffer failed: {:?}", e),
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptCollectiveReduceScatterFuture<T: Remote> {
    pub(crate) alloc: UcxOptAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxOptCollectiveReduceScatterFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        let src = &alloc_slice[self.index..self.index + self.len];
        let req = self
            .alloc
            .reduce_scatter_inner(&self.op, src, &mut self.result, false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc
            .wait_ucc_request(self.req.as_ref().unwrap())
            .unwrap();
        let mut out = Vec::new();
        std::mem::swap(&mut out, &mut self.result);
        out
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for UcxOptCollectiveReduceScatterFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxOptCollectiveReduceScatterFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxOptCollectiveReduceScatterFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        match this.alloc.poll_wait_ucc_request(this.req.as_ref().unwrap()) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => panic!("ucx collective reduce_scatter failed: {:?}", e),
        }
        let mut out = Vec::new();
        std::mem::swap(&mut out, this.result);
        Poll::Ready(out)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptCollectiveReduceScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxOptAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxOptCollectiveReduceScatterIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        let src = &alloc_slice[self.index..self.index + self.len];
        let req = self
            .alloc
            .reduce_scatter_inner(&self.op, src, self.result.as_mut_slice(), false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc
            .wait_ucc_request(self.req.as_ref().unwrap())
            .unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for UcxOptCollectiveReduceScatterIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxOptCollectiveReduceScatterIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for UcxOptCollectiveReduceScatterIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        match this.alloc.poll_wait_ucc_request(this.req.as_ref().unwrap()) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => {
                panic!("ucx collective reduce_scatter into buffer failed: {:?}", e)
            }
        }
        Poll::Ready(())
    }
}

impl CommAllocCollectiveAllReduce for UcxOptAlloc {
    fn reduce_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        op: ReduceOp,
    ) -> CollectiveAllReduceOpHandle<T> {
        CollectiveAllReduceOpHandle {
            future: CollectiveAllReduceOpFuture::UcxOpt(UcxOptCollectiveAllReduceFuture {
                alloc: self.clone(),
                op,
                index,
                len,
                result: (0..len).map(|_| unsafe { std::mem::zeroed() }).collect(),
                scheduler: scheduler.clone(),
                counters,
                spawned: false,
                req: None,
            }),
        }
    }

    fn reduce_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        op: ReduceOp,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        CollectiveAllReduceIntoBufferOpHandle {
            future: CollectiveAllReduceIntoBufferOpFuture::UcxOpt(
                UcxOptCollectiveAllReduceIntoBufferFuture {
                    alloc: self.clone(),
                    op,
                    index,
                    len,
                    result: dst,
                    scheduler: scheduler.clone(),
                    counters,
                    req: None,
                    spawned: false,
                },
            ),
        }
    }

    fn reduce_all_in_place<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        source_and_dst: LamellarBuffer<T, B>,
        op: ReduceOp,
    ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        CollectiveAllReduceInPlaceOpHandle {
            future: CollectiveAllReduceInPlaceOpFuture::UcxOpt(UcxOptCollectiveAllReduceInPlaceFuture {
                alloc: self.clone(),
                op,
                result: source_and_dst,
                scheduler: scheduler.clone(),
                counters,
                req: None,
                spawned: false,
            }),
        }
    }
}

impl CommAllocCollectiveReduce for UcxOptAlloc {
    fn reduce<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: ReduceOp,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveReduceOpHandle<T> {
        let target = if root_pe != self.my_pe {
            RootOrBuffer::NotRoot(root_pe)
        } else {
            RootOrBuffer::Root((0..len).map(|_| unsafe { std::mem::zeroed() }).collect())
        };
        CollectiveReduceOpHandle {
            future: CollectiveReduceOpFuture::UcxOpt(UcxOptCollectiveReduceFuture {
                alloc: self.clone(),
                index,
                len,
                op,
                target,
                scheduler: scheduler.clone(),
                counters,
                req: None,
                spawned: false,
            }),
        }
    }

    fn reduce_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: ReduceOp,
        index: usize,
        len: usize,
        root_or_buffer: RootOrLamellarBuffer<T, B>,
    ) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        CollectiveReduceIntoBufferOpHandle {
            future: CollectiveReduceIntoBufferOpFuture::UcxOpt(UcxOptCollectiveReduceIntoBufferFuture {
                alloc: self.clone(),
                index,
                len,
                op,
                target: root_or_buffer,
                scheduler: scheduler.clone(),
                counters,
                req: None,
                spawned: false,
            }),
        }
    }
}

impl CommAllocCollectiveAllGather for UcxOptAlloc {
    fn gather_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllGatherOpHandle<T> {
        CollectiveAllGatherOpHandle {
            future: CollectiveAllGatherOpFuture::UcxOpt(UcxOptCollectiveAllGatherFuture {
                alloc: self.clone(),
                index,
                len,
                result: (0..len * self.num_pes)
                    .map(|_| unsafe { std::mem::zeroed() })
                    .collect(),
                scheduler: scheduler.clone(),
                counters,
                req: None,
                spawned: false,
            }),
        }
    }

    fn gather_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllGatherIntoBufferOpHandle<T, B> {
        CollectiveAllGatherIntoBufferOpHandle {
            future: CollectiveAllGatherIntoBufferOpFuture::UcxOpt(
                UcxOptCollectiveAllGatherIntoBufferFuture {
                    alloc: self.clone(),
                    index,
                    len,
                    result: dst,
                    scheduler: scheduler.clone(),
                    counters,
                    req: None,
                    spawned: false,
                },
            ),
        }
    }
}

impl CommAllocCollectiveGather for UcxOptAlloc {
    fn gather<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveGatherOpHandle<T> {
        let target = if root_pe != self.my_pe {
            RootOrBuffer::NotRoot(root_pe)
        } else {
            RootOrBuffer::Root(
                (0..len * self.num_pes)
                    .map(|_| unsafe { std::mem::zeroed() })
                    .collect(),
            )
        };

        CollectiveGatherOpHandle {
            future: CollectiveGatherOpFuture::UcxOpt(UcxOptCollectiveGatherFuture {
                alloc: self.clone(),
                index,
                len,
                target,
                scheduler: scheduler.clone(),
                counters,
                req: None,
                spawned: false,
            }),
        }
    }

    fn gather_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        root_or_buffer: RootOrLamellarBuffer<T, B>,
    ) -> CollectiveGatherIntoBufferOpHandle<T, B> {
        CollectiveGatherIntoBufferOpHandle {
            future: CollectiveGatherIntoBufferOpFuture::UcxOpt(UcxOptCollectiveGatherIntoBufferFuture {
                alloc: self.clone(),
                index,
                len,
                target: root_or_buffer,
                scheduler: scheduler.clone(),
                counters,
                req: None,
                spawned: false,
            }),
        }
    }
}

impl CommAllocCollectiveAllToAll for UcxOptAlloc {
    fn alltoall<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllToAllOpHandle<T> {
        CollectiveAllToAllOpHandle {
            future: CollectiveAllToAllOpFuture::UcxOpt(UcxOptCollectiveAllToAllFuture {
                alloc: self.clone(),
                index,
                result: (0..len * self.num_pes)
                    .map(|_| unsafe { std::mem::zeroed() })
                    .collect(),
                len,
                scheduler: scheduler.clone(),
                counters,
                req: None,
                spawned: false,
            }),
        }
    }

    fn alltoall_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllToAllIntoBufferOpHandle<T, B> {
        CollectiveAllToAllIntoBufferOpHandle {
            future: CollectiveAllToAllIntoBufferOpFuture::UcxOpt(
                UcxOptCollectiveAllToAllIntoBufferFuture {
                    alloc: self.clone(),
                    index,
                    len,
                    result: dst,
                    scheduler: scheduler.clone(),
                    counters,
                    req: None,
                    spawned: false,
                },
            ),
        }
    }
}

impl CommAllocCollectiveBroadcast for UcxOptAlloc {
    fn broadcast<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src_or_pe: BroadcastInput,
        len: usize,
    ) -> CollectiveBroadcastOpHandle<T> {
        let target = match src_or_pe {
            BroadcastInput::Root(index) => RootSrcOrBuffer::Root(index),
            BroadcastInput::NotRoot(root_pe) => RootSrcOrBuffer::NotRoot(
                (0..len).map(|_| unsafe { std::mem::zeroed() }).collect(),
                root_pe,
            ),
        };

        CollectiveBroadcastOpHandle {
            future: CollectiveBroadcastOpFuture::UcxOpt(UcxOptCollectiveBroadcastFuture {
                alloc: self.clone(),
                target,
                len,
                scheduler: scheduler.clone(),
                counters,
                req: None,
                spawned: false,
            }),
        }
    }

    fn broadcast_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        root_or_buffer: RootSrcOrLamellarBuffer<T, B>,
        len: usize,
    ) -> CollectiveBroadcastIntoBufferOpHandle<T, B> {
        CollectiveBroadcastIntoBufferOpHandle {
            future: CollectiveBroadcastIntoBufferOpFuture::UcxOpt(
                UcxOptCollectiveBroadcastIntoBufferFuture {
                    alloc: self.clone(),
                    target: root_or_buffer.into(),
                    len,
                    scheduler: scheduler.clone(),
                    counters,
                    req: None,
                    spawned: false,
                },
            ),
        }
    }
}

impl CommAllocCollectiveScatter for UcxOptAlloc {
    fn scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterOpHandle<T> {
        CollectiveScatterOpHandle {
            future: CollectiveScatterOpFuture::UcxOpt(UcxOptCollectiveScatterFuture {
                alloc: self.clone(),
                len,
                result: (0..len).map(|_| unsafe { std::mem::zeroed() }).collect(),
                src_or_root_pe: src_or_root_pe.into(),
                scheduler: scheduler.clone(),
                counters,
                req: None,
                spawned: false,
            }),
        }
    }

    fn scatter_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        result: LamellarBuffer<T, B>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterIntoBufferOpHandle<T, B> {
        CollectiveScatterIntoBufferOpHandle {
            future: CollectiveScatterIntoBufferOpFuture::UcxOpt(
                UcxOptCollectiveScatterIntoBufferFuture {
                    alloc: self.clone(),
                    len,
                    src_or_root_pe: src_or_root_pe.into(),
                    result,
                    scheduler: scheduler.clone(),
                    counters,
                    req: None,
                    spawned: false,
                },
            ),
        }
    }
}

impl CommAllocCollectiveReduceScatter for UcxOptAlloc {
    fn reduce_scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: ReduceOp,
        index: usize,
        len: usize,
    ) -> CollectiveReduceScatterOpHandle<T> {
        CollectiveReduceScatterOpHandle {
            future: CollectiveReduceScatterOpFuture::UcxOpt(UcxOptCollectiveReduceScatterFuture {
                alloc: self.clone(),
                op,
                index,
                len,
                result: (0..len / self.num_pes)
                    .map(|_| unsafe { std::mem::zeroed() })
                    .collect(),
                scheduler: scheduler.clone(),
                counters,
                req: None,
                spawned: false,
            }),
        }
    }

    fn reduce_scatter_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: ReduceOp,
        index: usize,
        len: usize,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        CollectiveReduceScatterIntoBufferOpHandle {
            future: CollectiveReduceScatterIntoBufferOpFuture::UcxOpt(
                UcxOptCollectiveReduceScatterIntoBufferFuture {
                    alloc: self.clone(),
                    op,
                    index,
                    len,
                    result: dst,
                    scheduler: scheduler.clone(),
                    counters,
                    spawned: false,
                    req: None,
                },
            ),
        }
    }
}

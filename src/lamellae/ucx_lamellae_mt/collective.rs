use crate::{
    active_messaging::AMCounters,
    lamellae::{collective::{
        BroadcastInput, CollectiveAllToAllIntoBufferOpFuture,
        CollectiveAllToAllIntoBufferOpHandle, CollectiveAllToAllOpFuture,
        CollectiveAllToAllOpHandle, CollectiveAllGatherIntoBufferOpFuture,
        CollectiveAllGatherIntoBufferOpHandle, CollectiveAllGatherOpFuture,
        CollectiveAllGatherOpHandle, CollectiveAllReduceInPlaceOpFuture,
        CollectiveAllReduceInPlaceOpHandle, CollectiveAllReduceIntoBufferOpFuture,
        CollectiveAllReduceIntoBufferOpHandle, CollectiveAllReduceOpFuture,
        CollectiveAllReduceOpHandle, CollectiveBroadcastIntoBufferOpFuture,
        CollectiveBroadcastIntoBufferOpHandle, CollectiveBroadcastOpFuture,
        CollectiveBroadcastOpHandle, CollectiveGatherIntoBufferOpFuture,
        CollectiveGatherIntoBufferOpHandle, CollectiveGatherOpFuture,
        CollectiveGatherOpHandle, CollectiveReduceIntoBufferOpFuture,
        CollectiveReduceIntoBufferOpHandle, CollectiveReduceOpFuture,
        CollectiveReduceOpHandle, CollectiveReduceScatterIntoBufferOpFuture,
        CollectiveReduceScatterIntoBufferOpHandle, CollectiveReduceScatterOpFuture,
        CollectiveReduceScatterOpHandle, CollectiveScatterIntoBufferOpFuture,
        CollectiveScatterIntoBufferOpHandle, CollectiveScatterOpFuture,
        CollectiveScatterOpHandle, CommAllocCollectiveAllToAll,
        CommAllocCollectiveAllGather, CommAllocCollectiveAllReduce,
        CommAllocCollectiveBroadcast, CommAllocCollectiveGather, CommAllocCollectiveReduce,
        CommAllocCollectiveReduceScatter, CommAllocCollectiveScatter, ReduceOp, RootOrBuffer,
        RootOrLamellarBuffer, RootSrcOrBuffer, RootSrcOrLamellarBuffer,
        RootSrcOrLamellarBufferInner, ScatterInput, ScatterInputInner,
    }, ucx_lamellae_mt::ucc::UccRequest},
    scheduler::Scheduler,
    warnings::RuntimeWarning,
    AsLamellarBuffer, LamellarBuffer, LamellarTask, Remote,
};

use super::fabric::UcxMtAlloc;
use pin_project::{pin_project, pinned_drop};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtCollectiveAllReduceFuture<T: Remote> {
    pub(crate) alloc: UcxMtAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxMtCollectiveAllReduceFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len] };
        let req = self.alloc
            .allreduce_inner(
                &self.op, 
                src, 
                &mut self.result, 
                false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        res
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for UcxMtCollectiveAllReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxMtCollectiveAllReduceFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxMtCollectiveAllReduceFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtCollectiveAllReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxMtAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxMtCollectiveAllReduceIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len] };
        let req = self
            .alloc
            .allreduce_inner(
                &self.op, 
                src, 
                self.result.as_mut_slice(), 
                false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxMtCollectiveAllReduceIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxMtCollectiveAllReduceIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxMtCollectiveAllReduceIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtCollectiveAllReduceInPlaceFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxMtAlloc,
    pub(super) op: ReduceOp,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxMtCollectiveAllReduceInPlaceFuture<T, B> {
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
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxMtCollectiveAllReduceInPlaceFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxMtCollectiveAllReduceInPlaceFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxMtCollectiveAllReduceInPlaceFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtCollectiveReduceFuture<T: Remote> {
    pub(crate) alloc: UcxMtAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(super) op: ReduceOp,
    pub(crate) target: RootOrBuffer<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxMtCollectiveReduceFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len] };
        let req = self
            .alloc
            .reduce_inner(&self.op, src, self.target.as_mut_slice(), false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
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
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for UcxMtCollectiveReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxMtCollectiveReduceFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxMtCollectiveReduceFuture<T> {
    type Output = Option<Vec<T>>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        match &mut self.target {
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
pub(crate) struct UcxMtCollectiveReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxMtAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(super) op: ReduceOp,
    pub(crate) target: RootOrLamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxMtCollectiveReduceIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len] };
        let req = self
            .alloc
            .reduce_inner(&self.op, src, self.target.as_mut_slice(), false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxMtCollectiveReduceIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxMtCollectiveReduceIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxMtCollectiveReduceIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtCollectiveReduceInPlaceFuture<T> {
    pub(crate) alloc: UcxMtAlloc,
    pub(super) op: ReduceOp,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    phantom: std::marker::PhantomData<T>,
}

impl<T: Remote> UcxMtCollectiveReduceInPlaceFuture<T> {
    pub(crate) fn block(mut self) {
        self.spawned = true;
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for UcxMtCollectiveReduceInPlaceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxMtCollectiveReduceInPlaceFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxMtCollectiveReduceInPlaceFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.spawned = true;
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtCollectiveAllGatherFuture<T: Remote> {
    pub(crate) alloc: UcxMtAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxMtCollectiveAllGatherFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len] };
        let req = self
            .alloc
            .allgather_inner(src, &mut self.result, false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        let mut out = Vec::new();
        std::mem::swap(&mut out, &mut self.result);
        out
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for UcxMtCollectiveAllGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxMtCollectiveAllGatherFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxMtCollectiveAllGatherFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        let mut out = Vec::new();
        std::mem::swap(&mut out, &mut self.result);
        Poll::Ready(out)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtCollectiveAllGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxMtAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxMtCollectiveAllGatherIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len] };
        let req = self
            .alloc
            .allgather_inner(src, self.result.as_mut_slice(), false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxMtCollectiveAllGatherIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxMtCollectiveAllGatherIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxMtCollectiveAllGatherIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtCollectiveGatherFuture<T: Remote> {
    pub(crate) alloc: UcxMtAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) target: RootOrBuffer<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxMtCollectiveGatherFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len] };
        let req = self
            .alloc
            .gather_inner(src, self.target.as_mut_slice(), false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
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
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for UcxMtCollectiveGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxMtCollectiveGatherFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxMtCollectiveGatherFuture<T> {
    type Output = Option<Vec<T>>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        match &mut self.target {
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
pub(crate) struct UcxMtCollectiveGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxMtAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) target: RootOrLamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxMtCollectiveGatherIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len] };
        let req = self
            .alloc
            .gather_inner(src, self.target.as_mut_slice(), false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxMtCollectiveGatherIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxMtCollectiveGatherIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxMtCollectiveGatherIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtCollectiveAllToAllFuture<T: Remote> {
    pub(crate) alloc: UcxMtAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxMtCollectiveAllToAllFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len] };
        let req = self
            .alloc
            .alltoall_inner(src, &mut self.result, false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        let mut out = Vec::new();
        std::mem::swap(&mut out, &mut self.result);
        out
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for UcxMtCollectiveAllToAllFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxMtCollectiveAllToAllFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxMtCollectiveAllToAllFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        let mut out = Vec::new();
        std::mem::swap(&mut out, &mut self.result);
        Poll::Ready(out)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtCollectiveAllToAllIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxMtAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxMtCollectiveAllToAllIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len] };
        let req = self
            .alloc
            .alltoall_inner(src, self.result.as_mut_slice(), false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxMtCollectiveAllToAllIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxMtCollectiveAllToAllIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxMtCollectiveAllToAllIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtCollectiveBroadcastFuture<T: Remote> {
    pub(crate) alloc: UcxMtAlloc,
    pub(crate) target: RootSrcOrBuffer<T>,
    pub(crate) len: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxMtCollectiveBroadcastFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice() };
        let req = self.alloc.broadcast_inner(self.target.as_mut_slice(alloc_slice, self.len), false).unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
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
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for UcxMtCollectiveBroadcastFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxMtCollectiveBroadcastFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxMtCollectiveBroadcastFuture<T> {
    type Output = Option<Vec<T>>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        match &mut self.target {
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
pub(crate) struct UcxMtCollectiveBroadcastIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxMtAlloc,
    pub(crate) target: RootSrcOrLamellarBufferInner<T, B>,
    pub(crate) len: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxMtCollectiveBroadcastIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice() };
        let req = self.alloc.broadcast_inner(self.target.as_mut_slice(alloc_slice, self.len), false).unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxMtCollectiveBroadcastIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxMtCollectiveBroadcastIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxMtCollectiveBroadcastIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtCollectiveScatterFuture<T: Remote> {
    pub(crate) alloc: UcxMtAlloc,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    src_or_root_pe: ScatterInputInner,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxMtCollectiveScatterFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        let req = self
            .alloc
            .scatter_inner(&mut self.result, self.src_or_root_pe.as_slice(alloc_slice, self.len), false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        let mut out = Vec::new();
        std::mem::swap(&mut out, &mut self.result);
        out
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for UcxMtCollectiveScatterFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxMtCollectiveScatterFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxMtCollectiveScatterFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        let mut out = Vec::new();
        std::mem::swap(&mut out, &mut self.result);
        Poll::Ready(out)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtCollectiveScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxMtAlloc,
    pub(crate) len: usize,
    src_or_root_pe: ScatterInputInner,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxMtCollectiveScatterIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        let req = self
            .alloc
            .scatter_inner(self.result.as_mut_slice(), self.src_or_root_pe.as_slice(alloc_slice, self.len), false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxMtCollectiveScatterIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxMtCollectiveScatterIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxMtCollectiveScatterIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtCollectiveReduceScatterFuture<T: Remote> {
    pub(crate) alloc: UcxMtAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxMtCollectiveReduceScatterFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        let src_len = self.len * self.alloc.num_pes;
        let src = &alloc_slice[self.index..self.index + src_len];
        let req = self
            .alloc
            .reduce_scatter_inner(&self.op, src, &mut self.result, false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        let mut out = Vec::new();
        std::mem::swap(&mut out, &mut self.result);
        out
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for UcxMtCollectiveReduceScatterFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxMtCollectiveReduceScatterFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxMtCollectiveReduceScatterFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        let mut out = Vec::new();
        std::mem::swap(&mut out, &mut self.result);
        Poll::Ready(out)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtCollectiveReduceScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxMtAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxMtCollectiveReduceScatterIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        let src_len = self.len * self.alloc.num_pes;
        let src = &alloc_slice[self.index..self.index + src_len];
        let req = self
            .alloc
            .reduce_scatter_inner(&self.op, src, self.result.as_mut_slice(), false)
            .unwrap();
        self.req = req;
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxMtCollectiveReduceScatterIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxMtCollectiveReduceScatterIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxMtCollectiveReduceScatterIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        Poll::Ready(())
    }
}

impl CommAllocCollectiveAllReduce for UcxMtAlloc {
    fn reduce_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
        op: ReduceOp,
    ) -> CollectiveAllReduceOpHandle<T> {
        CollectiveAllReduceOpHandle {
            future: CollectiveAllReduceOpFuture::UcxMt(UcxMtCollectiveAllReduceFuture {
                alloc: self.clone(),
                op,
                index,
                len,
                result: vec![T::default(); len],
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
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
        op: ReduceOp,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        CollectiveAllReduceIntoBufferOpHandle {
            future: CollectiveAllReduceIntoBufferOpFuture::UcxMt(
                UcxMtCollectiveAllReduceIntoBufferFuture {
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
        counters: Vec<Arc<AMCounters>>,
        source_and_dst: LamellarBuffer<T, B>,
        op: ReduceOp,
    ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        CollectiveAllReduceInPlaceOpHandle {
            future: CollectiveAllReduceInPlaceOpFuture::UcxMt(UcxMtCollectiveAllReduceInPlaceFuture {
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

impl CommAllocCollectiveReduce for UcxMtAlloc {
    fn reduce<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveReduceOpHandle<T> {
        let target = if root_pe != self.my_pe {
            RootOrBuffer::NotRoot(root_pe)
        } else {
            RootOrBuffer::Root(vec![T::default(); len])
        };
        CollectiveReduceOpHandle {
            future: CollectiveReduceOpFuture::UcxMt(UcxMtCollectiveReduceFuture {
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
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        index: usize,
        len: usize,
        root_or_buffer: RootOrLamellarBuffer<T, B>,
    ) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        CollectiveReduceIntoBufferOpHandle {
            future: CollectiveReduceIntoBufferOpFuture::UcxMt(UcxMtCollectiveReduceIntoBufferFuture {
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

impl CommAllocCollectiveAllGather for UcxMtAlloc {
    fn gather_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllGatherOpHandle<T> {
        CollectiveAllGatherOpHandle {
            future: CollectiveAllGatherOpFuture::UcxMt(UcxMtCollectiveAllGatherFuture {
                alloc: self.clone(),
                index,
                len,
                result: vec![T::default(); len * self.num_pes],
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
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllGatherIntoBufferOpHandle<T, B> {
        CollectiveAllGatherIntoBufferOpHandle {
            future: CollectiveAllGatherIntoBufferOpFuture::UcxMt(UcxMtCollectiveAllGatherIntoBufferFuture {
                alloc: self.clone(),
                index,
                len,
                result: dst,
                scheduler: scheduler.clone(),
                counters,
                req: None,
                spawned: false,
            }),
        }
    }
}

impl CommAllocCollectiveGather for UcxMtAlloc {
    fn gather<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveGatherOpHandle<T> {
        let target = if root_pe != self.my_pe {
            RootOrBuffer::NotRoot(root_pe)
        } else {
            RootOrBuffer::Root(vec![T::default(); len * self.num_pes])
        };

        CollectiveGatherOpHandle {
            future: CollectiveGatherOpFuture::UcxMt(UcxMtCollectiveGatherFuture {
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
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
        root_or_buffer: RootOrLamellarBuffer<T, B>,
    ) -> CollectiveGatherIntoBufferOpHandle<T, B> {
        CollectiveGatherIntoBufferOpHandle {
            future: CollectiveGatherIntoBufferOpFuture::UcxMt(UcxMtCollectiveGatherIntoBufferFuture {
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

impl CommAllocCollectiveAllToAll for UcxMtAlloc {
    fn alltoall<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllToAllOpHandle<T> {
        CollectiveAllToAllOpHandle {
            future: CollectiveAllToAllOpFuture::UcxMt(UcxMtCollectiveAllToAllFuture {
                alloc: self.clone(),
                index,
                result: vec![T::default(); len * self.num_pes],
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
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllToAllIntoBufferOpHandle<T, B> {
        CollectiveAllToAllIntoBufferOpHandle {
            future: CollectiveAllToAllIntoBufferOpFuture::UcxMt(
                UcxMtCollectiveAllToAllIntoBufferFuture {
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

impl CommAllocCollectiveBroadcast for UcxMtAlloc {
    fn broadcast<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src_or_pe: BroadcastInput,
        len: usize,
    ) -> CollectiveBroadcastOpHandle<T> {
        let target = match src_or_pe {
            BroadcastInput::Root(index) => {
                RootSrcOrBuffer::Root(index)
            }
            BroadcastInput::NotRoot(root_pe) => {
                RootSrcOrBuffer::NotRoot(vec![T::default(); len], root_pe)
            }
        };

        CollectiveBroadcastOpHandle {
            future: CollectiveBroadcastOpFuture::UcxMt(UcxMtCollectiveBroadcastFuture {
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
        counters: Vec<Arc<AMCounters>>,
        root_or_buffer: RootSrcOrLamellarBuffer<T, B>,
        len: usize,
    ) -> CollectiveBroadcastIntoBufferOpHandle<T, B> {
        CollectiveBroadcastIntoBufferOpHandle {
            future: CollectiveBroadcastIntoBufferOpFuture::UcxMt(UcxMtCollectiveBroadcastIntoBufferFuture {
                alloc: self.clone(),
                target: root_or_buffer.into(),
                len,
                scheduler: scheduler.clone(),
                counters,
                req: None,
                spawned: false,
            }),
        }
    }
}

impl CommAllocCollectiveScatter for UcxMtAlloc {
    fn scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterOpHandle<T> {
        CollectiveScatterOpHandle {
            future: CollectiveScatterOpFuture::UcxMt(UcxMtCollectiveScatterFuture {
                alloc: self.clone(),
                len,
                result: vec![T::default(); len],
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
        counters: Vec<Arc<AMCounters>>,
        result: LamellarBuffer<T, B>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterIntoBufferOpHandle<T, B> {
        CollectiveScatterIntoBufferOpHandle {
            future: CollectiveScatterIntoBufferOpFuture::UcxMt(UcxMtCollectiveScatterIntoBufferFuture {
                alloc: self.clone(),
                len,
                src_or_root_pe: src_or_root_pe.into(),
                result,
                scheduler: scheduler.clone(),
                counters,
                req: None,
                spawned: false,
            }),
        }
    }
}

impl CommAllocCollectiveReduceScatter for UcxMtAlloc {
    fn reduce_scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        index: usize,
        len: usize,
    ) -> CollectiveReduceScatterOpHandle<T> {
        CollectiveReduceScatterOpHandle {
            future: CollectiveReduceScatterOpFuture::UcxMt(UcxMtCollectiveReduceScatterFuture {
                alloc: self.clone(),
                op,
                index,
                len,
                result: vec![T::default(); len / self.num_pes],
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
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        index: usize,
        len: usize,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        CollectiveReduceScatterIntoBufferOpHandle {
            future: CollectiveReduceScatterIntoBufferOpFuture::UcxMt(
                UcxMtCollectiveReduceScatterIntoBufferFuture {
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

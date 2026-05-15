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
    }, ucx_lamellae::ucc::UccRequest},
    // memregion::MemregionRdmaInputInner,
    scheduler::Scheduler,
    warnings::RuntimeWarning,
    AsLamellarBuffer, LamellarBuffer, LamellarTask, Remote,
};

use super::fabric::UcxAlloc;
use pin_project::{pin_project, pinned_drop};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

#[pin_project(PinnedDrop)]
pub(crate) struct UcxCollectiveAllReduceFuture<T: Remote> {
    pub(crate) alloc: UcxAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxCollectiveAllReduceFuture<T> {
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
impl<T: Remote> PinnedDrop for UcxCollectiveAllReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxCollectiveAllReduceFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxCollectiveAllReduceFuture<T> {
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
pub(crate) struct UcxCollectiveAllReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxCollectiveAllReduceIntoBufferFuture<T, B> {
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
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxCollectiveAllReduceIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxCollectiveAllReduceIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxCollectiveAllReduceIntoBufferFuture<T, B> {
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
pub(crate) struct UcxCollectiveAllReduceInPlaceFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxAlloc,
    pub(super) op: ReduceOp,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxCollectiveAllReduceInPlaceFuture<T, B> {
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
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxCollectiveAllReduceInPlaceFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxCollectiveAllReduceInPlaceFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxCollectiveAllReduceInPlaceFuture<T, B> {
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
pub(crate) struct UcxCollectiveReduceFuture<T: Remote> {
    pub(crate) alloc: UcxAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(super) op: ReduceOp,
    pub(crate) target: RootOrBuffer<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxCollectiveReduceFuture<T> {
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
impl<T: Remote> PinnedDrop for UcxCollectiveReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxCollectiveReduceFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxCollectiveReduceFuture<T> {
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
pub(crate) struct UcxCollectiveReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(super) op: ReduceOp,
    pub(crate) target: RootOrLamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxCollectiveReduceIntoBufferFuture<T, B> {
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
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxCollectiveReduceIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxCollectiveReduceIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxCollectiveReduceIntoBufferFuture<T, B> {
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
pub(crate) struct UcxCollectiveReduceInPlaceFuture<T> {
    pub(crate) alloc: UcxAlloc,
    pub(super) op: ReduceOp,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    phantom: std::marker::PhantomData<T>,
}

impl<T: Remote> UcxCollectiveReduceInPlaceFuture<T> {
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
impl<T> PinnedDrop for UcxCollectiveReduceInPlaceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxCollectiveReduceInPlaceFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxCollectiveReduceInPlaceFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.spawned = true;
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxCollectiveAllGatherFuture<T: Remote> {
    pub(crate) alloc: UcxAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxCollectiveAllGatherFuture<T> {
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
impl<T: Remote> PinnedDrop for UcxCollectiveAllGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxCollectiveAllGatherFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxCollectiveAllGatherFuture<T> {
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
pub(crate) struct UcxCollectiveAllGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxCollectiveAllGatherIntoBufferFuture<T, B> {
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
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxCollectiveAllGatherIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxCollectiveAllGatherIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxCollectiveAllGatherIntoBufferFuture<T, B> {
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
pub(crate) struct UcxCollectiveGatherFuture<T: Remote> {
    pub(crate) alloc: UcxAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) target: RootOrBuffer<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxCollectiveGatherFuture<T> {
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
impl<T: Remote> PinnedDrop for UcxCollectiveGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxCollectiveGatherFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxCollectiveGatherFuture<T> {
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
pub(crate) struct UcxCollectiveGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) target: RootOrLamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxCollectiveGatherIntoBufferFuture<T, B> {
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
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxCollectiveGatherIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxCollectiveGatherIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxCollectiveGatherIntoBufferFuture<T, B> {
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
pub(crate) struct UcxCollectiveAllToAllFuture<T: Remote> {
    pub(crate) alloc: UcxAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxCollectiveAllToAllFuture<T> {
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
impl<T: Remote> PinnedDrop for UcxCollectiveAllToAllFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxCollectiveAlToAllFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxCollectiveAllToAllFuture<T> {
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
pub(crate) struct UcxCollectiveAllToAllIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxCollectiveAllToAllIntoBufferFuture<T, B> {
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
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxCollectiveAllToAllIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxCollectiveAllToAllIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxCollectiveAllToAllIntoBufferFuture<T, B> {
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
pub(crate) struct UcxCollectiveBroadcastFuture<T: Remote> {
    pub(crate) alloc: UcxAlloc,
    pub(crate) target: RootSrcOrBuffer<T>,
    pub(crate) len: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxCollectiveBroadcastFuture<T> {
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
impl<T: Remote> PinnedDrop for UcxCollectiveBroadcastFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxCollectiveBroadcastFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxCollectiveBroadcastFuture<T> {
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
pub(crate) struct UcxCollectiveBroadcastIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxAlloc,
    pub(crate) target: RootSrcOrLamellarBufferInner<T, B>,
    pub(crate) len: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxCollectiveBroadcastIntoBufferFuture<T, B> {
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
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxCollectiveBroadcastIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxCollectiveBroadcastIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxCollectiveBroadcastIntoBufferFuture<T, B> {
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
pub(crate) struct UcxCollectiveScatterFuture<T: Remote> {
    pub(crate) alloc: UcxAlloc,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    src_or_root_pe: ScatterInputInner,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxCollectiveScatterFuture<T> {
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
impl<T: Remote> PinnedDrop for UcxCollectiveScatterFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxCollectiveScatterFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxCollectiveScatterFuture<T> {
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
pub(crate) struct UcxCollectiveScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxAlloc,
    pub(crate) len: usize,
    src_or_root_pe: ScatterInputInner,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxCollectiveScatterIntoBufferFuture<T, B> {
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
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxCollectiveScatterIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxCollectiveScatterIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxCollectiveScatterIntoBufferFuture<T, B> {
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
pub(crate) struct UcxCollectiveReduceScatterFuture<T: Remote> {
    pub(crate) alloc: UcxAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote> UcxCollectiveReduceScatterFuture<T> {
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
impl<T: Remote> PinnedDrop for UcxCollectiveReduceScatterFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxCollectiveReduceScatterFuture").print();
        }
    }
}

impl<T: Remote> Future for UcxCollectiveReduceScatterFuture<T> {
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
pub(crate) struct UcxCollectiveReduceScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: UcxAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    req: Option<UccRequest>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxCollectiveReduceScatterIntoBufferFuture<T, B> {
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
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxCollectiveReduceScatterIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a UcxCollectiveReduceScatterIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxCollectiveReduceScatterIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.wait_ucc_request(self.req.as_ref().unwrap()).unwrap();
        Poll::Ready(())
    }
}

impl CommAllocCollectiveAllReduce for UcxAlloc {
    fn reduce_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
        op: ReduceOp,
    ) -> CollectiveAllReduceOpHandle<T> {
        CollectiveAllReduceOpHandle {
            future: CollectiveAllReduceOpFuture::Ucx(UcxCollectiveAllReduceFuture {
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
            future: CollectiveAllReduceIntoBufferOpFuture::Ucx(
                UcxCollectiveAllReduceIntoBufferFuture {
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
            future: CollectiveAllReduceInPlaceOpFuture::Ucx(UcxCollectiveAllReduceInPlaceFuture {
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

impl CommAllocCollectiveReduce for UcxAlloc {
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
            future: CollectiveReduceOpFuture::Ucx(UcxCollectiveReduceFuture {
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
            future: CollectiveReduceIntoBufferOpFuture::Ucx(UcxCollectiveReduceIntoBufferFuture {
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

impl CommAllocCollectiveAllGather for UcxAlloc {
    fn gather_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllGatherOpHandle<T> {
        CollectiveAllGatherOpHandle {
            future: CollectiveAllGatherOpFuture::Ucx(UcxCollectiveAllGatherFuture {
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
            future: CollectiveAllGatherIntoBufferOpFuture::Ucx(UcxCollectiveAllGatherIntoBufferFuture {
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

impl CommAllocCollectiveGather for UcxAlloc {
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
            future: CollectiveGatherOpFuture::Ucx(UcxCollectiveGatherFuture {
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
            future: CollectiveGatherIntoBufferOpFuture::Ucx(UcxCollectiveGatherIntoBufferFuture {
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

impl CommAllocCollectiveAllToAll for UcxAlloc {
    fn alltoall<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllToAllOpHandle<T> {
        CollectiveAllToAllOpHandle {
            future: CollectiveAllToAllOpFuture::Ucx(UcxCollectiveAllToAllFuture {
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
            future: CollectiveAllToAllIntoBufferOpFuture::Ucx(
                UcxCollectiveAllToAllIntoBufferFuture {
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

impl CommAllocCollectiveBroadcast for UcxAlloc {
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
            future: CollectiveBroadcastOpFuture::Ucx(UcxCollectiveBroadcastFuture {
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
            future: CollectiveBroadcastIntoBufferOpFuture::Ucx(UcxCollectiveBroadcastIntoBufferFuture {
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

impl CommAllocCollectiveScatter for UcxAlloc {
    fn scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterOpHandle<T> {
        CollectiveScatterOpHandle {
            future: CollectiveScatterOpFuture::Ucx(UcxCollectiveScatterFuture {
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
            future: CollectiveScatterIntoBufferOpFuture::Ucx(UcxCollectiveScatterIntoBufferFuture {
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

impl CommAllocCollectiveReduceScatter for UcxAlloc {
    fn reduce_scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        index: usize,
        len: usize,
    ) -> CollectiveReduceScatterOpHandle<T> {
        CollectiveReduceScatterOpHandle {
            future: CollectiveReduceScatterOpFuture::Ucx(UcxCollectiveReduceScatterFuture {
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
            future: CollectiveReduceScatterIntoBufferOpFuture::Ucx(
                UcxCollectiveReduceScatterIntoBufferFuture {
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

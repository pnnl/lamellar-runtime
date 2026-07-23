use std::{future::Future, pin::Pin, sync::Arc, task::{Context, Poll}};

use pin_project::{pin_project, pinned_drop};

use crate::{
    active_messaging::AMCounters,
    lamellae::{
        collective::{
            CollectiveAllGatherIntoBufferOpFuture, CollectiveAllGatherIntoBufferOpHandle,
            CollectiveAllGatherOpFuture, CollectiveAllGatherOpHandle,
            CollectiveAllReduceInPlaceOpFuture, CollectiveAllReduceInPlaceOpHandle,
            CollectiveAllReduceIntoBufferOpFuture, CollectiveAllReduceIntoBufferOpHandle,
            CollectiveAllReduceOpFuture, CollectiveAllReduceOpHandle,
            CollectiveAllToAllIntoBufferOpFuture, CollectiveAllToAllIntoBufferOpHandle,
            CollectiveAllToAllOpFuture, CollectiveAllToAllOpHandle,
            CollectiveBroadcastIntoBufferOpFuture, CollectiveBroadcastIntoBufferOpHandle,
            CollectiveBroadcastOpFuture, CollectiveBroadcastOpHandle,
            CollectiveGatherIntoBufferOpFuture, CollectiveGatherIntoBufferOpHandle,
            CollectiveGatherOpFuture, CollectiveGatherOpHandle,
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
            RootSrcOrLamellarBufferInner, ScatterInputInner,
        },
        local_lamellae::comm::LocalAlloc,
    },
    scheduler::Scheduler,
    warnings::RuntimeWarning,
    AsLamellarBuffer, BroadcastInput, LamellarBuffer, LamellarTask, Remote, ScatterInput,
};

#[pin_project(PinnedDrop)]
pub(crate) struct LocalCollectiveAllReduceFuture<T: Remote> {
    pub(crate) alloc: Arc<LocalAlloc>,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LocalCollectiveAllReduceFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice::<T>() };
        let my_slice = &alloc_slice[self.index..self.index + self.len];
        self.result.copy_from_slice(my_slice);
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
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
impl<T: Remote> PinnedDrop for LocalCollectiveAllReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LocalCollectiveAllReduceFuture").print();
        }
    }
}

impl<T: Remote> From<LocalCollectiveAllReduceFuture<T>> for CollectiveAllReduceOpHandle<T> {
    fn from(f: LocalCollectiveAllReduceFuture<T>) -> CollectiveAllReduceOpHandle<T> {
        CollectiveAllReduceOpHandle {
            future: CollectiveAllReduceOpFuture::Local(f),
        }
    }
}

impl<T: Remote> Future for LocalCollectiveAllReduceFuture<T> {
    type Output = Vec<T>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let mut res = Vec::new();
        std::mem::swap(&mut res, &mut self.result);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalCollectiveAllReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: Arc<LocalAlloc>,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LocalCollectiveAllReduceIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice::<T>() };
        let my_slice = &alloc_slice[self.index..self.index + self.len];
        self.result.as_mut_slice().copy_from_slice(my_slice);
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LocalCollectiveAllReduceIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LocalCollectiveAllReduceIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LocalCollectiveAllReduceIntoBufferFuture<T, B>>
    for CollectiveAllReduceIntoBufferOpHandle<T, B>
{
    fn from(
        f: LocalCollectiveAllReduceIntoBufferFuture<T, B>,
    ) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        CollectiveAllReduceIntoBufferOpHandle {
            future: CollectiveAllReduceIntoBufferOpFuture::Local(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LocalCollectiveAllReduceIntoBufferFuture<T, B> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalCollectiveAllReduceInPlaceFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LocalCollectiveAllReduceInPlaceFuture<T, B> {
    fn exec_op(&mut self) {
        // Single PE: reducing a value against itself is the identity, buffer is already correct.
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LocalCollectiveAllReduceInPlaceFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LocalCollectiveAllReduceInPlaceFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LocalCollectiveAllReduceInPlaceFuture<T, B>>
    for CollectiveAllReduceInPlaceOpHandle<T, B>
{
    fn from(
        f: LocalCollectiveAllReduceInPlaceFuture<T, B>,
    ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        CollectiveAllReduceInPlaceOpHandle {
            future: CollectiveAllReduceInPlaceOpFuture::Local(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LocalCollectiveAllReduceInPlaceFuture<T, B> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalCollectiveReduceFuture<T: Remote> {
    pub(crate) alloc: Arc<LocalAlloc>,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) target: RootOrBuffer<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LocalCollectiveReduceFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice::<T>() };
        let my_slice = &alloc_slice[self.index..self.index + self.len];
        match &mut self.target {
            RootOrBuffer::Root(result) => {
                result.copy_from_slice(my_slice);
            }
            RootOrBuffer::NotRoot(_) => {}
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        match &mut self.target {
            RootOrBuffer::Root(r) => {
                let mut res = Vec::new();
                std::mem::swap(&mut res, r);
                Some(res)
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
impl<T: Remote> PinnedDrop for LocalCollectiveReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LocalCollectiveReduceFuture").print();
        }
    }
}

impl<T: Remote> From<LocalCollectiveReduceFuture<T>> for CollectiveReduceOpHandle<T> {
    fn from(f: LocalCollectiveReduceFuture<T>) -> CollectiveReduceOpHandle<T> {
        CollectiveReduceOpHandle {
            future: CollectiveReduceOpFuture::Local(f),
        }
    }
}

impl<T: Remote> Future for LocalCollectiveReduceFuture<T> {
    type Output = Option<Vec<T>>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match &mut self.target {
            RootOrBuffer::Root(r) => {
                let mut res = Vec::new();
                std::mem::swap(&mut res, r);
                Poll::Ready(Some(res))
            }
            RootOrBuffer::NotRoot(_) => Poll::Ready(None),
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalCollectiveReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: Arc<LocalAlloc>,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) target: RootOrLamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LocalCollectiveReduceIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice::<T>() };
        let my_slice = &alloc_slice[self.index..self.index + self.len];
        match &mut self.target {
            RootOrLamellarBuffer::Root(result) => {
                result.as_mut_slice().copy_from_slice(my_slice);
            }
            RootOrLamellarBuffer::NotRoot(_) => {}
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LocalCollectiveReduceIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LocalCollectiveReduceIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LocalCollectiveReduceIntoBufferFuture<T, B>>
    for CollectiveReduceIntoBufferOpHandle<T, B>
{
    fn from(
        f: LocalCollectiveReduceIntoBufferFuture<T, B>,
    ) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        CollectiveReduceIntoBufferOpHandle {
            future: CollectiveReduceIntoBufferOpFuture::Local(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LocalCollectiveReduceIntoBufferFuture<T, B> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalCollectiveAllGatherFuture<T: Remote> {
    pub(crate) alloc: Arc<LocalAlloc>,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LocalCollectiveAllGatherFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice::<T>() };
        let my_slice = &alloc_slice[self.index..self.index + self.len];
        self.result.copy_from_slice(my_slice);
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        let mut res = Vec::new();
        std::mem::swap(&mut res, &mut self.result);
        res
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LocalCollectiveAllGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LocalCollectiveAllGatherFuture").print();
        }
    }
}

impl<T: Remote> From<LocalCollectiveAllGatherFuture<T>> for CollectiveAllGatherOpHandle<T> {
    fn from(f: LocalCollectiveAllGatherFuture<T>) -> CollectiveAllGatherOpHandle<T> {
        CollectiveAllGatherOpHandle {
            future: CollectiveAllGatherOpFuture::Local(f),
        }
    }
}

impl<T: Remote> Future for LocalCollectiveAllGatherFuture<T> {
    type Output = Vec<T>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let mut res = Vec::new();
        std::mem::swap(&mut res, &mut self.result);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalCollectiveAllGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: Arc<LocalAlloc>,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LocalCollectiveAllGatherIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice::<T>() };
        let my_slice = &alloc_slice[self.index..self.index + self.len];
        self.result.as_mut_slice().copy_from_slice(my_slice);
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LocalCollectiveAllGatherIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LocalCollectiveAllGatherIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LocalCollectiveAllGatherIntoBufferFuture<T, B>>
    for CollectiveAllGatherIntoBufferOpHandle<T, B>
{
    fn from(
        f: LocalCollectiveAllGatherIntoBufferFuture<T, B>,
    ) -> CollectiveAllGatherIntoBufferOpHandle<T, B> {
        CollectiveAllGatherIntoBufferOpHandle {
            future: CollectiveAllGatherIntoBufferOpFuture::Local(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LocalCollectiveAllGatherIntoBufferFuture<T, B> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalCollectiveGatherFuture<T: Remote> {
    pub(crate) alloc: Arc<LocalAlloc>,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) target: RootOrBuffer<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LocalCollectiveGatherFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice::<T>() };
        let my_slice = &alloc_slice[self.index..self.index + self.len];
        match &mut self.target {
            RootOrBuffer::Root(result) => {
                result.copy_from_slice(my_slice);
            }
            RootOrBuffer::NotRoot(_) => {}
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        match &mut self.target {
            RootOrBuffer::Root(r) => {
                let mut res = Vec::new();
                std::mem::swap(&mut res, r);
                Some(res)
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
impl<T: Remote> PinnedDrop for LocalCollectiveGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LocalCollectiveGatherFuture").print();
        }
    }
}

impl<T: Remote> From<LocalCollectiveGatherFuture<T>> for CollectiveGatherOpHandle<T> {
    fn from(f: LocalCollectiveGatherFuture<T>) -> CollectiveGatherOpHandle<T> {
        CollectiveGatherOpHandle {
            future: CollectiveGatherOpFuture::Local(f),
        }
    }
}

impl<T: Remote> Future for LocalCollectiveGatherFuture<T> {
    type Output = Option<Vec<T>>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match &mut self.target {
            RootOrBuffer::Root(r) => {
                let mut res = Vec::new();
                std::mem::swap(&mut res, r);
                Poll::Ready(Some(res))
            }
            RootOrBuffer::NotRoot(_) => Poll::Ready(None),
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalCollectiveGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: Arc<LocalAlloc>,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) target: RootOrLamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LocalCollectiveGatherIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice::<T>() };
        let my_slice = &alloc_slice[self.index..self.index + self.len];
        match &mut self.target {
            RootOrLamellarBuffer::Root(result) => {
                result.as_mut_slice().copy_from_slice(my_slice);
            }
            RootOrLamellarBuffer::NotRoot(_) => {}
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LocalCollectiveGatherIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LocalCollectiveGatherIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LocalCollectiveGatherIntoBufferFuture<T, B>>
    for CollectiveGatherIntoBufferOpHandle<T, B>
{
    fn from(
        f: LocalCollectiveGatherIntoBufferFuture<T, B>,
    ) -> CollectiveGatherIntoBufferOpHandle<T, B> {
        CollectiveGatherIntoBufferOpHandle {
            future: CollectiveGatherIntoBufferOpFuture::Local(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LocalCollectiveGatherIntoBufferFuture<T, B> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalCollectiveAllToAllFuture<T: Remote> {
    pub(crate) alloc: Arc<LocalAlloc>,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LocalCollectiveAllToAllFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice::<T>() };
        let my_slice = &alloc_slice[self.index..self.index + self.len];
        self.result.copy_from_slice(my_slice);
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        let mut res = Vec::new();
        std::mem::swap(&mut res, &mut self.result);
        res
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LocalCollectiveAllToAllFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LocalCollectiveAllToAllFuture").print();
        }
    }
}

impl<T: Remote> From<LocalCollectiveAllToAllFuture<T>> for CollectiveAllToAllOpHandle<T> {
    fn from(f: LocalCollectiveAllToAllFuture<T>) -> CollectiveAllToAllOpHandle<T> {
        CollectiveAllToAllOpHandle {
            future: CollectiveAllToAllOpFuture::Local(f),
        }
    }
}

impl<T: Remote> Future for LocalCollectiveAllToAllFuture<T> {
    type Output = Vec<T>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let mut res = Vec::new();
        std::mem::swap(&mut res, &mut self.result);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalCollectiveAllToAllIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: Arc<LocalAlloc>,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LocalCollectiveAllToAllIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice::<T>() };
        let my_slice = &alloc_slice[self.index..self.index + self.len];
        self.result.as_mut_slice().copy_from_slice(my_slice);
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LocalCollectiveAllToAllIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LocalCollectiveAllToAllIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LocalCollectiveAllToAllIntoBufferFuture<T, B>>
    for CollectiveAllToAllIntoBufferOpHandle<T, B>
{
    fn from(
        f: LocalCollectiveAllToAllIntoBufferFuture<T, B>,
    ) -> CollectiveAllToAllIntoBufferOpHandle<T, B> {
        CollectiveAllToAllIntoBufferOpHandle {
            future: CollectiveAllToAllIntoBufferOpFuture::Local(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LocalCollectiveAllToAllIntoBufferFuture<T, B> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalCollectiveBroadcastFuture<T: Remote> {
    pub(crate) target: RootSrcOrBuffer<T>,
    pub(crate) len: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LocalCollectiveBroadcastFuture<T> {
    fn exec_op(&mut self) {
        // Single PE: this PE is always the root, and non-root scratch has nothing to fetch from.
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        match &mut self.target {
            RootSrcOrBuffer::Root(_) => None,
            RootSrcOrBuffer::NotRoot(items, _) => {
                let mut res = Vec::new();
                std::mem::swap(items, &mut res);
                Some(res)
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
impl<T: Remote> PinnedDrop for LocalCollectiveBroadcastFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LocalCollectiveBroadcastFuture").print();
        }
    }
}

impl<T: Remote> From<LocalCollectiveBroadcastFuture<T>> for CollectiveBroadcastOpHandle<T> {
    fn from(f: LocalCollectiveBroadcastFuture<T>) -> CollectiveBroadcastOpHandle<T> {
        CollectiveBroadcastOpHandle {
            future: CollectiveBroadcastOpFuture::Local(f),
        }
    }
}

impl<T: Remote> Future for LocalCollectiveBroadcastFuture<T> {
    type Output = Option<Vec<T>>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match &mut self.target {
            RootSrcOrBuffer::Root(_) => Poll::Ready(None),
            RootSrcOrBuffer::NotRoot(items, _) => {
                let mut res = Vec::new();
                std::mem::swap(items, &mut res);
                Poll::Ready(Some(res))
            }
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalCollectiveBroadcastIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) target: RootSrcOrLamellarBufferInner<T, B>,
    pub(crate) len: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LocalCollectiveBroadcastIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LocalCollectiveBroadcastIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LocalCollectiveBroadcastIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LocalCollectiveBroadcastIntoBufferFuture<T, B>>
    for CollectiveBroadcastIntoBufferOpHandle<T, B>
{
    fn from(
        f: LocalCollectiveBroadcastIntoBufferFuture<T, B>,
    ) -> CollectiveBroadcastIntoBufferOpHandle<T, B> {
        CollectiveBroadcastIntoBufferOpHandle {
            future: CollectiveBroadcastIntoBufferOpFuture::Local(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LocalCollectiveBroadcastIntoBufferFuture<T, B> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalCollectiveScatterFuture<T: Remote> {
    pub(crate) alloc: Arc<LocalAlloc>,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) src_or_root_pe: ScatterInputInner,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LocalCollectiveScatterFuture<T> {
    fn exec_op(&mut self) {
        if let ScatterInputInner::Root(index) = self.src_or_root_pe {
            let alloc_slice = unsafe { self.alloc.as_mut_slice::<T>() };
            self.result
                .copy_from_slice(&alloc_slice[index..index + self.len]);
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        let mut res = Vec::new();
        std::mem::swap(&mut res, &mut self.result);
        res
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LocalCollectiveScatterFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LocalCollectiveScatterFuture").print();
        }
    }
}

impl<T: Remote> From<LocalCollectiveScatterFuture<T>> for CollectiveScatterOpHandle<T> {
    fn from(f: LocalCollectiveScatterFuture<T>) -> CollectiveScatterOpHandle<T> {
        CollectiveScatterOpHandle {
            future: CollectiveScatterOpFuture::Local(f),
        }
    }
}

impl<T: Remote> Future for LocalCollectiveScatterFuture<T> {
    type Output = Vec<T>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let mut res = Vec::new();
        std::mem::swap(&mut res, &mut self.result);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalCollectiveScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: Arc<LocalAlloc>,
    pub(crate) len: usize,
    pub(crate) src_or_root_pe: ScatterInputInner,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LocalCollectiveScatterIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        if let ScatterInputInner::Root(index) = self.src_or_root_pe {
            let alloc_slice = unsafe { self.alloc.as_mut_slice::<T>() };
            self.result
                .as_mut_slice()
                .copy_from_slice(&alloc_slice[index..index + self.len]);
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LocalCollectiveScatterIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LocalCollectiveScatterIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LocalCollectiveScatterIntoBufferFuture<T, B>>
    for CollectiveScatterIntoBufferOpHandle<T, B>
{
    fn from(
        f: LocalCollectiveScatterIntoBufferFuture<T, B>,
    ) -> CollectiveScatterIntoBufferOpHandle<T, B> {
        CollectiveScatterIntoBufferOpHandle {
            future: CollectiveScatterIntoBufferOpFuture::Local(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LocalCollectiveScatterIntoBufferFuture<T, B> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalCollectiveReduceScatterFuture<T: Remote> {
    pub(crate) alloc: Arc<LocalAlloc>,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LocalCollectiveReduceScatterFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice::<T>() };
        let my_slice = &alloc_slice[self.index..self.index + self.len];
        self.result.copy_from_slice(my_slice);
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        let mut res = Vec::new();
        std::mem::swap(&mut res, &mut self.result);
        res
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LocalCollectiveReduceScatterFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LocalCollectiveReduceScatterFuture").print();
        }
    }
}

impl<T: Remote> From<LocalCollectiveReduceScatterFuture<T>> for CollectiveReduceScatterOpHandle<T> {
    fn from(f: LocalCollectiveReduceScatterFuture<T>) -> CollectiveReduceScatterOpHandle<T> {
        CollectiveReduceScatterOpHandle {
            future: CollectiveReduceScatterOpFuture::Local(f),
        }
    }
}

impl<T: Remote> Future for LocalCollectiveReduceScatterFuture<T> {
    type Output = Vec<T>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let mut res = Vec::new();
        std::mem::swap(&mut res, &mut self.result);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalCollectiveReduceScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: Arc<LocalAlloc>,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LocalCollectiveReduceScatterIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice::<T>() };
        let my_slice = &alloc_slice[self.index..self.index + self.len];
        self.result.as_mut_slice().copy_from_slice(my_slice);
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LocalCollectiveReduceScatterIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LocalCollectiveReduceScatterIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LocalCollectiveReduceScatterIntoBufferFuture<T, B>>
    for CollectiveReduceScatterIntoBufferOpHandle<T, B>
{
    fn from(
        f: LocalCollectiveReduceScatterIntoBufferFuture<T, B>,
    ) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        CollectiveReduceScatterIntoBufferOpHandle {
            future: CollectiveReduceScatterIntoBufferOpFuture::Local(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LocalCollectiveReduceScatterIntoBufferFuture<T, B>
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

impl CommAllocCollectiveAllReduce for Arc<LocalAlloc> {
    fn reduce_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        _op: ReduceOp,
    ) -> CollectiveAllReduceOpHandle<T> {
        LocalCollectiveAllReduceFuture {
            alloc: self.clone(),
            index,
            len,
            result: (0..len).map(|_| unsafe { std::mem::zeroed() }).collect(),
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }

    fn reduce_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        _op: ReduceOp,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        LocalCollectiveAllReduceIntoBufferFuture {
            alloc: self.clone(),
            index,
            len,
            result: dst,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }

    fn reduce_all_in_place<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src_and_dst: LamellarBuffer<T, B>,
        _op: ReduceOp,
    ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        LocalCollectiveAllReduceInPlaceFuture {
            result: src_and_dst,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }
}

impl CommAllocCollectiveReduce for Arc<LocalAlloc> {
    fn reduce<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        _op: ReduceOp,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveReduceOpHandle<T> {
        let target = if root_pe != 0 {
            RootOrBuffer::NotRoot(root_pe)
        } else {
            RootOrBuffer::Root((0..len).map(|_| unsafe { std::mem::zeroed() }).collect())
        };
        LocalCollectiveReduceFuture {
            alloc: self.clone(),
            index,
            len,
            target,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }

    fn reduce_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        _op: ReduceOp,
        index: usize,
        len: usize,
        root_or_buffer: RootOrLamellarBuffer<T, B>,
    ) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        LocalCollectiveReduceIntoBufferFuture {
            alloc: self.clone(),
            index,
            len,
            target: root_or_buffer,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }
}

impl CommAllocCollectiveAllGather for Arc<LocalAlloc> {
    fn gather_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllGatherOpHandle<T> {
        LocalCollectiveAllGatherFuture {
            alloc: self.clone(),
            index,
            len,
            result: (0..len).map(|_| unsafe { std::mem::zeroed() }).collect(),
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }

    fn gather_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllGatherIntoBufferOpHandle<T, B> {
        LocalCollectiveAllGatherIntoBufferFuture {
            alloc: self.clone(),
            index,
            len,
            result: dst,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }
}

impl CommAllocCollectiveGather for Arc<LocalAlloc> {
    fn gather<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveGatherOpHandle<T> {
        let target = if root_pe != 0 {
            RootOrBuffer::NotRoot(root_pe)
        } else {
            RootOrBuffer::Root((0..len).map(|_| unsafe { std::mem::zeroed() }).collect())
        };
        LocalCollectiveGatherFuture {
            alloc: self.clone(),
            index,
            len,
            target,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }

    fn gather_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        root_or_buffer: RootOrLamellarBuffer<T, B>,
    ) -> CollectiveGatherIntoBufferOpHandle<T, B> {
        LocalCollectiveGatherIntoBufferFuture {
            alloc: self.clone(),
            index,
            len,
            target: root_or_buffer,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }
}

impl CommAllocCollectiveAllToAll for Arc<LocalAlloc> {
    fn alltoall<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllToAllOpHandle<T> {
        LocalCollectiveAllToAllFuture {
            alloc: self.clone(),
            index,
            len,
            result: (0..len).map(|_| unsafe { std::mem::zeroed() }).collect(),
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }

    fn alltoall_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllToAllIntoBufferOpHandle<T, B> {
        LocalCollectiveAllToAllIntoBufferFuture {
            alloc: self.clone(),
            index,
            len,
            result: dst,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }
}

impl CommAllocCollectiveBroadcast for Arc<LocalAlloc> {
    fn broadcast<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src_or_root_pe: BroadcastInput,
        len: usize,
    ) -> CollectiveBroadcastOpHandle<T> {
        let target = match src_or_root_pe {
            BroadcastInput::Root(index) => RootSrcOrBuffer::Root(index),
            BroadcastInput::NotRoot(root) => RootSrcOrBuffer::NotRoot(
                (0..len).map(|_| unsafe { std::mem::zeroed() }).collect(),
                root,
            ),
        };
        LocalCollectiveBroadcastFuture {
            target,
            len,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }

    fn broadcast_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        dst: RootSrcOrLamellarBuffer<T, B>,
        len: usize,
    ) -> CollectiveBroadcastIntoBufferOpHandle<T, B> {
        LocalCollectiveBroadcastIntoBufferFuture {
            target: dst.into(),
            len,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }
}

impl CommAllocCollectiveScatter for Arc<LocalAlloc> {
    fn scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterOpHandle<T> {
        LocalCollectiveScatterFuture {
            alloc: self.clone(),
            len,
            src_or_root_pe: src_or_root_pe.into(),
            result: (0..len).map(|_| unsafe { std::mem::zeroed() }).collect(),
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }

    fn scatter_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        dst: LamellarBuffer<T, B>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterIntoBufferOpHandle<T, B> {
        LocalCollectiveScatterIntoBufferFuture {
            alloc: self.clone(),
            len,
            src_or_root_pe: src_or_root_pe.into(),
            result: dst,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }
}

impl CommAllocCollectiveReduceScatter for Arc<LocalAlloc> {
    fn reduce_scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        _op: ReduceOp,
        index: usize,
        len: usize,
    ) -> CollectiveReduceScatterOpHandle<T> {
        LocalCollectiveReduceScatterFuture {
            alloc: self.clone(),
            index,
            len,
            result: (0..len).map(|_| unsafe { std::mem::zeroed() }).collect(),
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }

    fn reduce_scatter_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        _op: ReduceOp,
        index: usize,
        len: usize,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        LocalCollectiveReduceScatterIntoBufferFuture {
            alloc: self.clone(),
            index,
            len,
            result: dst,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }
}

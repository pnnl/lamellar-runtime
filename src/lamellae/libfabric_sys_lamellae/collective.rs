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
            CommAllocCollectiveReduceScatter, CommAllocCollectiveScatter, RootOrBuffer,
            RootOrLamellarBuffer, RootSrcOrBuffer, RootSrcOrLamellarBuffer,
            RootSrcOrLamellarBufferInner, ScatterInput, ScatterInputInner,
        },
        comm::collective::ReduceOp,
    },
    warnings::RuntimeWarning,
    AsLamellarBuffer, LamellarBuffer, LamellarTask, Remote,
};

use super::{fabric::LibfabricSysAlloc, Scheduler};

use pin_project::{pin_project, pinned_drop};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysCollectiveAllReduceFuture<T: Remote> {
    pub(crate) alloc: LibfabricSysAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricSysCollectiveAllReduceFuture<T> {
    fn exec_op(&mut self) {
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        LibfabricSysAlloc::allreduce_inner(&self.alloc, &self.op, src, &mut self.result, false);

        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc.ofi.wait_all();
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
impl<T: Remote> PinnedDrop for LibfabricSysCollectiveAllReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysCollectiveAllReduceFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricSysCollectiveAllReduceFuture<T>> for CollectiveAllReduceOpHandle<T> {
    fn from(f: LibfabricSysCollectiveAllReduceFuture<T>) -> CollectiveAllReduceOpHandle<T> {
        CollectiveAllReduceOpHandle {
            future: CollectiveAllReduceOpFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysCollectiveAllReduceFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all();
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysCollectiveAllReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>>
{
    pub(crate) alloc: LibfabricSysAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricSysCollectiveAllReduceIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        LibfabricSysAlloc::allreduce_inner(
            &self.alloc,
            &self.op,
            src,
            self.result.as_mut_slice(),
            false,
        );

        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricSysCollectiveAllReduceIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysCollectiveAllReduceIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricSysCollectiveAllReduceIntoBufferFuture<T, B>>
    for CollectiveAllReduceIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricSysCollectiveAllReduceIntoBufferFuture<T, B>,
    ) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        CollectiveAllReduceIntoBufferOpHandle {
            future: CollectiveAllReduceIntoBufferOpFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricSysCollectiveAllReduceIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysCollectiveAllReduceInPlaceFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricSysAlloc,
    pub(super) op: ReduceOp,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
    phantom: std::marker::PhantomData<T>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricSysCollectiveAllReduceInPlaceFuture<T, B> {
    fn exec_op(&mut self) {
        // println!(
        //     "performing collective reduce op: {:?} in place ",
        //     self.op,
        // );

        LibfabricSysAlloc::allreduce_inplace_inner::<T>(
            &self.alloc,
            &self.op,
            self.result.as_mut_slice(),
            false,
        );

        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricSysCollectiveAllReduceInPlaceFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysCollectiveAllReduceInPlaceFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricSysCollectiveAllReduceInPlaceFuture<T, B>>
    for CollectiveAllReduceInPlaceOpHandle<T, B>
{
    fn from(
        f: LibfabricSysCollectiveAllReduceInPlaceFuture<T, B>,
    ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        CollectiveAllReduceInPlaceOpHandle {
            future: CollectiveAllReduceInPlaceOpFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricSysCollectiveAllReduceInPlaceFuture<T, B>
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysCollectiveReduceFuture<T: Remote> {
    pub(crate) alloc: LibfabricSysAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(super) op: ReduceOp,
    pub(crate) target: RootOrBuffer<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricSysCollectiveReduceFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };

        LibfabricSysAlloc::reduce_inner(
            &self.alloc,
            &self.op,
            src,
            self.target.as_mut_slice(),
            false,
        );

        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        self.alloc.ofi.wait_all();
        match &mut self.target {
            RootOrBuffer::Root(res) => {
                let mut res_vec = Vec::new();
                std::mem::swap(&mut res_vec, res);
                Some(res_vec)
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
impl<T: Remote> PinnedDrop for LibfabricSysCollectiveReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysCollectiveReduceFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricSysCollectiveReduceFuture<T>> for CollectiveReduceOpHandle<T> {
    fn from(f: LibfabricSysCollectiveReduceFuture<T>) -> CollectiveReduceOpHandle<T> {
        CollectiveReduceOpHandle {
            future: CollectiveReduceOpFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysCollectiveReduceFuture<T> {
    type Output = Option<Vec<T>>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all();
        match &mut self.target {
            RootOrBuffer::Root(res) => {
                let mut res_vec = Vec::new();
                std::mem::swap(&mut res_vec, res);
                Poll::Ready(Some(res_vec))
            }
            RootOrBuffer::NotRoot(_) => Poll::Ready(None),
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysCollectiveReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricSysAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(super) op: ReduceOp,
    pub(crate) target: RootOrLamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricSysCollectiveReduceIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };

        LibfabricSysAlloc::reduce_inner(
            &self.alloc,
            &self.op,
            src,
            self.target.as_mut_slice(),
            false,
        );

        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricSysCollectiveReduceIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysCollectiveReduceIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricSysCollectiveReduceIntoBufferFuture<T, B>>
    for CollectiveReduceIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricSysCollectiveReduceIntoBufferFuture<T, B>,
    ) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        CollectiveReduceIntoBufferOpHandle {
            future: CollectiveReduceIntoBufferOpFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricSysCollectiveReduceIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysCollectiveReduceInPlaceFuture<T> {
    pub(crate) alloc: LibfabricSysAlloc,
    pub(super) op: ReduceOp,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
    root_pe: Option<usize>,
    phantom: std::marker::PhantomData<T>,
}

// impl<T: Remote> LibfabricSysCollectiveReduceInPlaceFuture<T> {
//     fn exec_op(&mut self) {
//         println!(
//             "performing collective reduce op: {:?} in place ",
//             self.op,
//         );

//         LibfabricSysAlloc::reduce_inplace_inner::<T>(
//             &self.alloc,
//             &self.op,
//             self.root_pe.clone(),
//             false,
//         )
//         .unwrap();
//
//         println!(
//             "collective reduce op: {:?} initiated",
//             self.op,
//         );
//         self.spawned = true;
//     }
//     pub(crate) fn block(self) {
//         self.exec_op();
//         self.alloc.ofi.wait_all();
//     }

//     pub(crate) fn spawn(self) -> LamellarTask<()> {
//         self.exec_op();

//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.clone().spawn_task(self, counters)
//     }
// }

#[pinned_drop]
impl<T> PinnedDrop for LibfabricSysCollectiveReduceInPlaceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysCollectiveReduceInPlaceFuture").print();
        }
    }
}

// TODO: reduce_in_place never wired up for any backend, CollectiveReduceInPlaceOpHandle/Future commented out in comm/collective.rs
// impl<T> From<LibfabricSysCollectiveReduceInPlaceFuture<T>> for CollectiveReduceInPlaceOpHandle<T> {
//     fn from(f: LibfabricSysCollectiveReduceInPlaceFuture<T>) -> CollectiveReduceInPlaceOpHandle<T> {
//         CollectiveReduceInPlaceOpHandle {
//             future: CollectiveReduceInPlaceOpFuture::LibfabricSys(f),
//         }
//     }
// }

impl<T: Remote> Future for LibfabricSysCollectiveReduceInPlaceFuture<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        // if !self.spawned { // TODO: FIX
        //     self.exec_op();
        // }
        self.alloc.ofi.wait_all();
        Poll::Ready(())
    }
}

impl CommAllocCollectiveAllReduce for LibfabricSysAlloc {
    fn reduce_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        op: ReduceOp,
    ) -> CollectiveAllReduceOpHandle<T> {
        LibfabricSysCollectiveAllReduceFuture {
            alloc: self.clone(),
            op: op,
            index,
            len,
            result: (0..len).map(|_| unsafe { std::mem::zeroed() }).collect(),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
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
        LibfabricSysCollectiveAllReduceIntoBufferFuture {
            alloc: self.clone(),
            op: op,
            index,
            len,
            result: dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn reduce_all_in_place<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        source_and_dst: LamellarBuffer<T, B>,
        op: ReduceOp,
    ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        LibfabricSysCollectiveAllReduceInPlaceFuture {
            alloc: self.clone(),
            op: op,
            result: source_and_dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            phantom: std::marker::PhantomData,
        }
        .into()
    }
}

impl CommAllocCollectiveReduce for LibfabricSysAlloc {
    fn reduce<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: ReduceOp,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveReduceOpHandle<T> {
        let target = if root_pe != self.ofi.my_pe {
            RootOrBuffer::NotRoot(root_pe)
        } else {
            RootOrBuffer::Root((0..len).map(|_| unsafe { std::mem::zeroed() }).collect())
        };
        LibfabricSysCollectiveReduceFuture {
            alloc: self.clone(),
            index,
            len,
            op: op,
            target,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
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
        LibfabricSysCollectiveReduceIntoBufferFuture {
            alloc: self.clone(),
            op: op,
            index,
            len,
            target: root_or_buffer,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    // fn reduce_in_place<T: Remote>( // TODO: Fix
    //     &self,
    //     scheduler: &Arc<Scheduler>,
    //     counters: Option<Arc<[Arc<AMCounters>]>>,
    //     op: ReduceOp,
    //     root_pe: usize,
    // ) -> CollectiveReduceInPlaceOpHandle<T> {
    //     let root =  if root_pe != self.ofi.my_pe {
    //         Some(root_pe)
    //     }
    //     else {
    //         None
    //     };
    //     LibfabricSysCollectiveReduceInPlaceFuture {
    //         alloc: self.clone(),
    //         op: op,
    //         spawned: false,
    //         scheduler: scheduler.clone(),
    //         counters,
    //         root_pe: root,
    //         phantom: std::marker::PhantomData,
    //     }.into()
    // }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysCollectiveAllGatherFuture<T: Remote> {
    pub(crate) alloc: LibfabricSysAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricSysCollectiveAllGatherFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        //println!(
        //     "performing collective gather result ptr: {:?} ",
        //     result_ptr
        // );
        LibfabricSysAlloc::allgather_inner(&self.alloc, src, &mut self.result, false);

        // println!(
        //     "collective all gather initiated",
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc.ofi.wait_all();
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
impl<T: Remote> PinnedDrop for LibfabricSysCollectiveAllGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysCollectiveAllGatherFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricSysCollectiveAllGatherFuture<T>> for CollectiveAllGatherOpHandle<T> {
    fn from(f: LibfabricSysCollectiveAllGatherFuture<T>) -> CollectiveAllGatherOpHandle<T> {
        CollectiveAllGatherOpHandle {
            future: CollectiveAllGatherOpFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysCollectiveAllGatherFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all();
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysCollectiveAllGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>>
{
    pub(crate) alloc: LibfabricSysAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricSysCollectiveAllGatherIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        LibfabricSysAlloc::allgather_inner(&self.alloc, src, self.result.as_mut_slice(), false);

        // println!(
        //     "collective gather initiated",
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricSysCollectiveAllGatherIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysCollectiveAllGatherIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricSysCollectiveAllGatherIntoBufferFuture<T, B>>
    for CollectiveAllGatherIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricSysCollectiveAllGatherIntoBufferFuture<T, B>,
    ) -> CollectiveAllGatherIntoBufferOpHandle<T, B> {
        CollectiveAllGatherIntoBufferOpHandle {
            future: CollectiveAllGatherIntoBufferOpFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricSysCollectiveAllGatherIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysCollectiveGatherFuture<T: Remote> {
    pub(crate) alloc: LibfabricSysAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) target: RootOrBuffer<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricSysCollectiveGatherFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };

        LibfabricSysAlloc::gather_inner(&self.alloc, src, self.target.as_mut_slice(), false);

        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        self.alloc.ofi.wait_all();
        match &mut self.target {
            RootOrBuffer::Root(res) => {
                let mut res_vec = Vec::new();
                std::mem::swap(&mut res_vec, res);
                Some(res_vec)
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
impl<T: Remote> PinnedDrop for LibfabricSysCollectiveGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysCollectiveGatherFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricSysCollectiveGatherFuture<T>> for CollectiveGatherOpHandle<T> {
    fn from(f: LibfabricSysCollectiveGatherFuture<T>) -> CollectiveGatherOpHandle<T> {
        CollectiveGatherOpHandle {
            future: CollectiveGatherOpFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysCollectiveGatherFuture<T> {
    type Output = Option<Vec<T>>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all();
        match &mut self.target {
            RootOrBuffer::Root(res) => {
                let mut res_vec = Vec::new();
                std::mem::swap(&mut res_vec, res);
                Poll::Ready(Some(res_vec))
            }
            RootOrBuffer::NotRoot(_) => Poll::Ready(None),
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysCollectiveGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricSysAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) target: RootOrLamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricSysCollectiveGatherIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };

        LibfabricSysAlloc::gather_inner(&self.alloc, src, self.target.as_mut_slice(), false);

        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricSysCollectiveGatherIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysCollectiveGatherIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricSysCollectiveGatherIntoBufferFuture<T, B>>
    for CollectiveGatherIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricSysCollectiveGatherIntoBufferFuture<T, B>,
    ) -> CollectiveGatherIntoBufferOpHandle<T, B> {
        CollectiveGatherIntoBufferOpHandle {
            future: CollectiveGatherIntoBufferOpFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricSysCollectiveGatherIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysCollectiveAllToAllFuture<T: Remote> {
    pub(crate) alloc: LibfabricSysAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricSysCollectiveAllToAllFuture<T> {
    fn exec_op(&mut self) {
        // let result_ptr = self.result.as_mut_ptr();
        // println!(
        //     "performing collective gather result ptr: {:?} ",
        //     result_ptr
        // );
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        LibfabricSysAlloc::alltoall_inner(&self.alloc, src, &mut self.result, false);

        // println!(
        //     "collective all gather initiated",
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc.ofi.wait_all();
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
impl<T: Remote> PinnedDrop for LibfabricSysCollectiveAllToAllFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysCollectiveAllToAllFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricSysCollectiveAllToAllFuture<T>> for CollectiveAllToAllOpHandle<T> {
    fn from(f: LibfabricSysCollectiveAllToAllFuture<T>) -> CollectiveAllToAllOpHandle<T> {
        CollectiveAllToAllOpHandle {
            future: CollectiveAllToAllOpFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysCollectiveAllToAllFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all();
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysCollectiveAllToAllIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>>
{
    pub(crate) alloc: LibfabricSysAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricSysCollectiveAllToAllIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        LibfabricSysAlloc::alltoall_inner(&self.alloc, src, self.result.as_mut_slice(), false);
        // println!(
        //     "collective gather initiated",
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricSysCollectiveAllToAllIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysCollectiveAllToAllIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricSysCollectiveAllToAllIntoBufferFuture<T, B>>
    for CollectiveAllToAllIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricSysCollectiveAllToAllIntoBufferFuture<T, B>,
    ) -> CollectiveAllToAllIntoBufferOpHandle<T, B> {
        CollectiveAllToAllIntoBufferOpHandle {
            future: CollectiveAllToAllIntoBufferOpFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricSysCollectiveAllToAllIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysCollectiveBroadcastFuture<T: Remote> {
    pub(crate) alloc: LibfabricSysAlloc,
    pub(crate) target: RootSrcOrBuffer<T>,
    pub(crate) len: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricSysCollectiveBroadcastFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice() };
        // let result_ptr = self.result.as_mut_ptr();
        // println!(
        //     "performing collective broadcast result ptr: {:?} ",
        //     result_ptr
        // );
        LibfabricSysAlloc::broadcast_inner(
            &self.alloc,
            self.target.as_mut_slice(alloc_slice, self.len),
            false,
        );

        // println!(
        //     "collective broadcast initiated",
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        self.alloc.ofi.wait_all();
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
impl<T: Remote> PinnedDrop for LibfabricSysCollectiveBroadcastFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysCollectiveBroadcastFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricSysCollectiveBroadcastFuture<T>> for CollectiveBroadcastOpHandle<T> {
    fn from(f: LibfabricSysCollectiveBroadcastFuture<T>) -> CollectiveBroadcastOpHandle<T> {
        CollectiveBroadcastOpHandle {
            future: CollectiveBroadcastOpFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysCollectiveBroadcastFuture<T> {
    type Output = Option<Vec<T>>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all();
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
pub(crate) struct LibfabricSysCollectiveBroadcastIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>>
{
    pub(crate) alloc: LibfabricSysAlloc,
    pub(crate) target: RootSrcOrLamellarBufferInner<T, B>,
    pub(crate) len: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricSysCollectiveBroadcastIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice() };
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        LibfabricSysAlloc::broadcast_inner(
            &self.alloc,
            self.target.as_mut_slice(alloc_slice, self.len),
            false,
        );
        // println!(
        //     "collective gather initiated",
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricSysCollectiveBroadcastIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysCollectiveBroadcastIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricSysCollectiveBroadcastIntoBufferFuture<T, B>>
    for CollectiveBroadcastIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricSysCollectiveBroadcastIntoBufferFuture<T, B>,
    ) -> CollectiveBroadcastIntoBufferOpHandle<T, B> {
        CollectiveBroadcastIntoBufferOpHandle {
            future: CollectiveBroadcastIntoBufferOpFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricSysCollectiveBroadcastIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysCollectiveScatterFuture<T: Remote> {
    pub(crate) alloc: LibfabricSysAlloc,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    src_or_root_pe: ScatterInputInner,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricSysCollectiveScatterFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        // let result_ptr = self.result.as_mut_ptr();
        // println!(
        //     "performing collective broadcast result ptr: {:?} ",
        //     result_ptr
        // );
        LibfabricSysAlloc::scatter_inner(
            &self.alloc,
            &mut self.result,
            self.src_or_root_pe.as_slice(alloc_slice, self.len),
            false,
        );

        // println!(
        //     "collective scatter initiated",
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc.ofi.wait_all();
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
impl<T: Remote> PinnedDrop for LibfabricSysCollectiveScatterFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysCollectiveScatterFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricSysCollectiveScatterFuture<T>> for CollectiveScatterOpHandle<T> {
    fn from(f: LibfabricSysCollectiveScatterFuture<T>) -> CollectiveScatterOpHandle<T> {
        CollectiveScatterOpHandle {
            future: CollectiveScatterOpFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysCollectiveScatterFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all();
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysCollectiveScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricSysAlloc,
    pub(crate) len: usize,
    src_or_root_pe: ScatterInputInner,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricSysCollectiveScatterIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        LibfabricSysAlloc::scatter_inner(
            &self.alloc,
            self.result.as_mut_slice(),
            self.src_or_root_pe.as_slice(alloc_slice, self.len),
            false,
        );

        // println!(
        //     "collective gather initiated",
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricSysCollectiveScatterIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysCollectiveScatterIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricSysCollectiveScatterIntoBufferFuture<T, B>>
    for CollectiveScatterIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricSysCollectiveScatterIntoBufferFuture<T, B>,
    ) -> CollectiveScatterIntoBufferOpHandle<T, B> {
        CollectiveScatterIntoBufferOpHandle {
            future: CollectiveScatterIntoBufferOpFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricSysCollectiveScatterIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysCollectiveReduceScatterFuture<T: Remote> {
    pub(crate) alloc: LibfabricSysAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricSysCollectiveReduceScatterFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        let src = &alloc_slice[self.index..self.index + self.len];
        // let result_ptr = self.result.as_mut_ptr();
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        LibfabricSysAlloc::reduce_scatter_inner(
            &self.alloc,
            &self.op,
            src,
            &mut self.result,
            false,
        );

        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc.ofi.wait_all();
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
impl<T: Remote> PinnedDrop for LibfabricSysCollectiveReduceScatterFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysCollectiveReduceScatterFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricSysCollectiveReduceScatterFuture<T>>
    for CollectiveReduceScatterOpHandle<T>
{
    fn from(f: LibfabricSysCollectiveReduceScatterFuture<T>) -> CollectiveReduceScatterOpHandle<T> {
        CollectiveReduceScatterOpHandle {
            future: CollectiveReduceScatterOpFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysCollectiveReduceScatterFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all();
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysCollectiveReduceScatterIntoBufferFuture<
    T: Remote,
    B: AsLamellarBuffer<T>,
> {
    pub(crate) alloc: LibfabricSysAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricSysCollectiveReduceScatterIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        let src = &alloc_slice[self.index..self.index + self.len];
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        LibfabricSysAlloc::reduce_scatter_inner(
            &self.alloc,
            &self.op,
            src,
            self.result.as_mut_slice(),
            false,
        );

        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricSysCollectiveReduceScatterIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysCollectiveReduceScatterIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>>
    From<LibfabricSysCollectiveReduceScatterIntoBufferFuture<T, B>>
    for CollectiveReduceScatterIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricSysCollectiveReduceScatterIntoBufferFuture<T, B>,
    ) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        CollectiveReduceScatterIntoBufferOpHandle {
            future: CollectiveReduceScatterIntoBufferOpFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricSysCollectiveReduceScatterIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all();
        Poll::Ready(())
    }
}

impl CommAllocCollectiveAllGather for LibfabricSysAlloc {
    fn gather_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllGatherOpHandle<T> {
        LibfabricSysCollectiveAllGatherFuture {
            alloc: self.clone(),
            index,
            len,
            result: (0..len * self.num_pes())
                .map(|_| unsafe { std::mem::zeroed() })
                .collect(),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
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
        LibfabricSysCollectiveAllGatherIntoBufferFuture {
            alloc: self.clone(),
            index,
            len,
            result: dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
}

impl CommAllocCollectiveGather for LibfabricSysAlloc {
    fn gather<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveGatherOpHandle<T> {
        let target = if root_pe != self.ofi.my_pe {
            RootOrBuffer::NotRoot(root_pe)
        } else {
            RootOrBuffer::Root(
                (0..len * self.num_pes())
                    .map(|_| unsafe { std::mem::zeroed() })
                    .collect(),
            )
        };
        LibfabricSysCollectiveGatherFuture {
            alloc: self.clone(),
            index,
            len,
            target,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
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
        LibfabricSysCollectiveGatherIntoBufferFuture {
            alloc: self.clone(),
            index,
            len,
            target: root_or_buffer,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
}

impl CommAllocCollectiveAllToAll for LibfabricSysAlloc {
    fn alltoall<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllToAllOpHandle<T> {
        LibfabricSysCollectiveAllToAllFuture {
            alloc: self.clone(),
            index,
            len,
            result: (0..len * self.num_pes())
                .map(|_| unsafe { std::mem::zeroed() })
                .collect(),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
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
        LibfabricSysCollectiveAllToAllIntoBufferFuture {
            alloc: self.clone(),
            index,
            len,
            result: dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
}

impl CommAllocCollectiveBroadcast for LibfabricSysAlloc {
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

        LibfabricSysCollectiveBroadcastFuture {
            alloc: self.clone(),
            target,
            len,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn broadcast_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        root_or_buffer: RootSrcOrLamellarBuffer<T, B>,
        len: usize,
    ) -> CollectiveBroadcastIntoBufferOpHandle<T, B> {
        LibfabricSysCollectiveBroadcastIntoBufferFuture {
            alloc: self.clone(),
            target: root_or_buffer.into(),
            len,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
}

impl CommAllocCollectiveScatter for LibfabricSysAlloc {
    fn scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterOpHandle<T> {
        LibfabricSysCollectiveScatterFuture {
            alloc: self.clone(),
            len,
            src_or_root_pe: src_or_root_pe.into(),
            result: (0..len).map(|_| unsafe { std::mem::zeroed() }).collect(),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn scatter_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        result: LamellarBuffer<T, B>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterIntoBufferOpHandle<T, B> {
        LibfabricSysCollectiveScatterIntoBufferFuture {
            alloc: self.clone(),
            len,
            result: result,
            src_or_root_pe: src_or_root_pe.into(),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
}

impl CommAllocCollectiveReduceScatter for LibfabricSysAlloc {
    fn reduce_scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: ReduceOp,
        index: usize,
        len: usize,
    ) -> CollectiveReduceScatterOpHandle<T> {
        LibfabricSysCollectiveReduceScatterFuture {
            alloc: self.clone(),
            op: op,
            index,
            len,
            result: (0..len / self.num_pes())
                .map(|_| unsafe { std::mem::zeroed() })
                .collect(),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
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
        LibfabricSysCollectiveReduceScatterIntoBufferFuture {
            alloc: self.clone(),
            op: op,
            index,
            len,
            result: dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    // fn reduce_scatter_in_place<T: Remote, B: AsLamellarBuffer<T>>(
    //     &self,
    //     scheduler: &Arc<Scheduler>,
    //     counters: Option<Arc<[Arc<AMCounters>]>>,
    //     source_and_dst: LamellarBuffer<T, B>,
    //     op: ReduceOp,
    // ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {

    //     LibfabricSysCollectiveAllReduceInPlaceFuture {
    //         alloc: self.clone(),
    //         op: op,
    //         result: source_and_dst,
    //         spawned: false,
    //         scheduler: scheduler.clone(),
    //         counters,
    //         phantom: std::marker::PhantomData,
    //     }.into()
    // }
}

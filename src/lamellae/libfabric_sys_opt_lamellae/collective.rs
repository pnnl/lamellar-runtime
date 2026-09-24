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

use super::{fabric::LibfabricSysOptAlloc, Scheduler};

use pin_project::{pin_project, pinned_drop};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysOptCollectiveAllReduceFuture<T: Remote> {
    pub(crate) alloc: LibfabricSysOptAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricSysOptCollectiveAllReduceFuture<T> {
    fn exec_op(&mut self) {
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        LibfabricSysOptAlloc::allreduce_inner(&self.alloc, &self.op, src, &mut self.result, false);

        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || {
            self.alloc.ofi.wait_all();
            let mut res = Vec::new();
            std::mem::swap(&mut self.result, &mut res);
            res
        })
    }

    pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricSysOptCollectiveAllReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysOptCollectiveAllReduceFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricSysOptCollectiveAllReduceFuture<T>>
    for CollectiveAllReduceOpHandle<T>
{
    fn from(f: LibfabricSysOptCollectiveAllReduceFuture<T>) -> CollectiveAllReduceOpHandle<T> {
        CollectiveAllReduceOpHandle {
            future: CollectiveAllReduceOpFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysOptCollectiveAllReduceFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match self.alloc.ofi.poll_wait_for_collectives() {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(()) => {}
        }
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysOptCollectiveAllReduceIntoBufferFuture<
    T: Remote,
    B: AsLamellarBuffer<T>,
> {
    pub(crate) alloc: LibfabricSysOptAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricSysOptCollectiveAllReduceIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        LibfabricSysOptAlloc::allreduce_inner(
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
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || self.alloc.ofi.wait_all())
    }

    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricSysOptCollectiveAllReduceIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysOptCollectiveAllReduceIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>>
    From<LibfabricSysOptCollectiveAllReduceIntoBufferFuture<T, B>>
    for CollectiveAllReduceIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricSysOptCollectiveAllReduceIntoBufferFuture<T, B>,
    ) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        CollectiveAllReduceIntoBufferOpHandle {
            future: CollectiveAllReduceIntoBufferOpFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricSysOptCollectiveAllReduceIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match self.alloc.ofi.poll_wait_for_collectives() {
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
pub(crate) struct LibfabricSysOptCollectiveAllReduceInPlaceFuture<T: Remote, B: AsLamellarBuffer<T>>
{
    pub(crate) alloc: LibfabricSysOptAlloc,
    pub(super) op: ReduceOp,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
    phantom: std::marker::PhantomData<T>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricSysOptCollectiveAllReduceInPlaceFuture<T, B> {
    fn exec_op(&mut self) {
        // println!(
        //     "performing collective reduce op: {:?} in place ",
        //     self.op,
        // );

        LibfabricSysOptAlloc::allreduce_inplace_inner::<T>(
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
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || self.alloc.ofi.wait_all())
    }

    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricSysOptCollectiveAllReduceInPlaceFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysOptCollectiveAllReduceInPlaceFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricSysOptCollectiveAllReduceInPlaceFuture<T, B>>
    for CollectiveAllReduceInPlaceOpHandle<T, B>
{
    fn from(
        f: LibfabricSysOptCollectiveAllReduceInPlaceFuture<T, B>,
    ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        CollectiveAllReduceInPlaceOpHandle {
            future: CollectiveAllReduceInPlaceOpFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricSysOptCollectiveAllReduceInPlaceFuture<T, B>
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match self.alloc.ofi.poll_wait_for_collectives() {
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
pub(crate) struct LibfabricSysOptCollectiveReduceFuture<T: Remote> {
    pub(crate) alloc: LibfabricSysOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(super) op: ReduceOp,
    pub(crate) target: RootOrBuffer<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricSysOptCollectiveReduceFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };

        LibfabricSysOptAlloc::reduce_inner(
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
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || {
            self.alloc.ofi.wait_all();
            match &mut self.target {
                RootOrBuffer::Root(res) => {
                    let mut res_vec = Vec::new();
                    std::mem::swap(&mut res_vec, res);
                    Some(res_vec)
                }
                RootOrBuffer::NotRoot(_) => None,
            }
        })
    }

    pub(crate) fn spawn(self) -> LamellarTask<Option<Vec<T>>> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricSysOptCollectiveReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysOptCollectiveReduceFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricSysOptCollectiveReduceFuture<T>> for CollectiveReduceOpHandle<T> {
    fn from(f: LibfabricSysOptCollectiveReduceFuture<T>) -> CollectiveReduceOpHandle<T> {
        CollectiveReduceOpHandle {
            future: CollectiveReduceOpFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysOptCollectiveReduceFuture<T> {
    type Output = Option<Vec<T>>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match self.alloc.ofi.poll_wait_for_collectives() {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(()) => {}
        }
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
pub(crate) struct LibfabricSysOptCollectiveReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>>
{
    pub(crate) alloc: LibfabricSysOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(super) op: ReduceOp,
    pub(crate) target: RootOrLamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricSysOptCollectiveReduceIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };

        LibfabricSysOptAlloc::reduce_inner(
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
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || self.alloc.ofi.wait_all())
    }

    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricSysOptCollectiveReduceIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysOptCollectiveReduceIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricSysOptCollectiveReduceIntoBufferFuture<T, B>>
    for CollectiveReduceIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricSysOptCollectiveReduceIntoBufferFuture<T, B>,
    ) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        CollectiveReduceIntoBufferOpHandle {
            future: CollectiveReduceIntoBufferOpFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricSysOptCollectiveReduceIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match self.alloc.ofi.poll_wait_for_collectives() {
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
pub(crate) struct LibfabricSysOptCollectiveReduceInPlaceFuture<T> {
    pub(crate) alloc: LibfabricSysOptAlloc,
    pub(super) op: ReduceOp,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
    root_pe: Option<usize>,
    phantom: std::marker::PhantomData<T>,
}

// impl<T: Remote> LibfabricSysOptCollectiveReduceInPlaceFuture<T> {
//     fn exec_op(&mut self) {
//         println!(
//             "performing collective reduce op: {:?} in place ",
//             self.op,
//         );

//         LibfabricSysOptAlloc::reduce_inplace_inner::<T>(
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
impl<T> PinnedDrop for LibfabricSysOptCollectiveReduceInPlaceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysOptCollectiveReduceInPlaceFuture").print();
        }
    }
}

// TODO: reduce_in_place never wired up for any backend, CollectiveReduceInPlaceOpHandle/Future commented out in comm/collective.rs
// impl<T> From<LibfabricSysOptCollectiveReduceInPlaceFuture<T>> for CollectiveReduceInPlaceOpHandle<T> {
//     fn from(f: LibfabricSysOptCollectiveReduceInPlaceFuture<T>) -> CollectiveReduceInPlaceOpHandle<T> {
//         CollectiveReduceInPlaceOpHandle {
//             future: CollectiveReduceInPlaceOpFuture::LibfabricSysOpt(f),
//         }
//     }
// }

impl<T: Remote> Future for LibfabricSysOptCollectiveReduceInPlaceFuture<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // if !self.spawned { // TODO: FIX
        //     self.exec_op();
        // }
        match self.alloc.ofi.poll_wait_for_collectives() {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(()) => {}
        }
        Poll::Ready(())
    }
}

impl CommAllocCollectiveAllReduce for LibfabricSysOptAlloc {
    fn reduce_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        op: ReduceOp,
    ) -> CollectiveAllReduceOpHandle<T> {
        LibfabricSysOptCollectiveAllReduceFuture {
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
        LibfabricSysOptCollectiveAllReduceIntoBufferFuture {
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
        LibfabricSysOptCollectiveAllReduceInPlaceFuture {
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

impl CommAllocCollectiveReduce for LibfabricSysOptAlloc {
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
        LibfabricSysOptCollectiveReduceFuture {
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
        LibfabricSysOptCollectiveReduceIntoBufferFuture {
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
    //     LibfabricSysOptCollectiveReduceInPlaceFuture {
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
pub(crate) struct LibfabricSysOptCollectiveAllGatherFuture<T: Remote> {
    pub(crate) alloc: LibfabricSysOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricSysOptCollectiveAllGatherFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        //println!(
        //     "performing collective gather result ptr: {:?} ",
        //     result_ptr
        // );
        LibfabricSysOptAlloc::allgather_inner(&self.alloc, src, &mut self.result, false);

        // println!(
        //     "collective all gather initiated",
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || {
            self.alloc.ofi.wait_all();
            let mut res = Vec::new();
            std::mem::swap(&mut self.result, &mut res);
            res
        })
    }

    pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricSysOptCollectiveAllGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysOptCollectiveAllGatherFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricSysOptCollectiveAllGatherFuture<T>>
    for CollectiveAllGatherOpHandle<T>
{
    fn from(f: LibfabricSysOptCollectiveAllGatherFuture<T>) -> CollectiveAllGatherOpHandle<T> {
        CollectiveAllGatherOpHandle {
            future: CollectiveAllGatherOpFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysOptCollectiveAllGatherFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match self.alloc.ofi.poll_wait_for_collectives() {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(()) => {}
        }
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysOptCollectiveAllGatherIntoBufferFuture<
    T: Remote,
    B: AsLamellarBuffer<T>,
> {
    pub(crate) alloc: LibfabricSysOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricSysOptCollectiveAllGatherIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        LibfabricSysOptAlloc::allgather_inner(&self.alloc, src, self.result.as_mut_slice(), false);

        // println!(
        //     "collective gather initiated",
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || self.alloc.ofi.wait_all())
    }

    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricSysOptCollectiveAllGatherIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysOptCollectiveAllGatherIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>>
    From<LibfabricSysOptCollectiveAllGatherIntoBufferFuture<T, B>>
    for CollectiveAllGatherIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricSysOptCollectiveAllGatherIntoBufferFuture<T, B>,
    ) -> CollectiveAllGatherIntoBufferOpHandle<T, B> {
        CollectiveAllGatherIntoBufferOpHandle {
            future: CollectiveAllGatherIntoBufferOpFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricSysOptCollectiveAllGatherIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match self.alloc.ofi.poll_wait_for_collectives() {
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
pub(crate) struct LibfabricSysOptCollectiveGatherFuture<T: Remote> {
    pub(crate) alloc: LibfabricSysOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) target: RootOrBuffer<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricSysOptCollectiveGatherFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };

        LibfabricSysOptAlloc::gather_inner(&self.alloc, src, self.target.as_mut_slice(), false);

        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || {
            self.alloc.ofi.wait_all();
            match &mut self.target {
                RootOrBuffer::Root(res) => {
                    let mut res_vec = Vec::new();
                    std::mem::swap(&mut res_vec, res);
                    Some(res_vec)
                }
                RootOrBuffer::NotRoot(_) => None,
            }
        })
    }

    pub(crate) fn spawn(self) -> LamellarTask<Option<Vec<T>>> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricSysOptCollectiveGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysOptCollectiveGatherFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricSysOptCollectiveGatherFuture<T>> for CollectiveGatherOpHandle<T> {
    fn from(f: LibfabricSysOptCollectiveGatherFuture<T>) -> CollectiveGatherOpHandle<T> {
        CollectiveGatherOpHandle {
            future: CollectiveGatherOpFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysOptCollectiveGatherFuture<T> {
    type Output = Option<Vec<T>>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match self.alloc.ofi.poll_wait_for_collectives() {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(()) => {}
        }
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
pub(crate) struct LibfabricSysOptCollectiveGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>>
{
    pub(crate) alloc: LibfabricSysOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) target: RootOrLamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricSysOptCollectiveGatherIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };

        LibfabricSysOptAlloc::gather_inner(&self.alloc, src, self.target.as_mut_slice(), false);

        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || self.alloc.ofi.wait_all())
    }

    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricSysOptCollectiveGatherIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysOptCollectiveGatherIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricSysOptCollectiveGatherIntoBufferFuture<T, B>>
    for CollectiveGatherIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricSysOptCollectiveGatherIntoBufferFuture<T, B>,
    ) -> CollectiveGatherIntoBufferOpHandle<T, B> {
        CollectiveGatherIntoBufferOpHandle {
            future: CollectiveGatherIntoBufferOpFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricSysOptCollectiveGatherIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match self.alloc.ofi.poll_wait_for_collectives() {
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
pub(crate) struct LibfabricSysOptCollectiveAllToAllFuture<T: Remote> {
    pub(crate) alloc: LibfabricSysOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricSysOptCollectiveAllToAllFuture<T> {
    fn exec_op(&mut self) {
        // let result_ptr = self.result.as_mut_ptr();
        // println!(
        //     "performing collective gather result ptr: {:?} ",
        //     result_ptr
        // );
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        LibfabricSysOptAlloc::alltoall_inner(&self.alloc, src, &mut self.result, false);

        // println!(
        //     "collective all gather initiated",
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || {
            self.alloc.ofi.wait_all();
            let mut res = Vec::new();
            std::mem::swap(&mut self.result, &mut res);
            res
        })
    }

    pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricSysOptCollectiveAllToAllFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysOptCollectiveAllToAllFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricSysOptCollectiveAllToAllFuture<T>> for CollectiveAllToAllOpHandle<T> {
    fn from(f: LibfabricSysOptCollectiveAllToAllFuture<T>) -> CollectiveAllToAllOpHandle<T> {
        CollectiveAllToAllOpHandle {
            future: CollectiveAllToAllOpFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysOptCollectiveAllToAllFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match self.alloc.ofi.poll_wait_for_collectives() {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(()) => {}
        }
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysOptCollectiveAllToAllIntoBufferFuture<
    T: Remote,
    B: AsLamellarBuffer<T>,
> {
    pub(crate) alloc: LibfabricSysOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricSysOptCollectiveAllToAllIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        LibfabricSysOptAlloc::alltoall_inner(&self.alloc, src, self.result.as_mut_slice(), false);
        // println!(
        //     "collective gather initiated",
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || self.alloc.ofi.wait_all())
    }

    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricSysOptCollectiveAllToAllIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysOptCollectiveAllToAllIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>>
    From<LibfabricSysOptCollectiveAllToAllIntoBufferFuture<T, B>>
    for CollectiveAllToAllIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricSysOptCollectiveAllToAllIntoBufferFuture<T, B>,
    ) -> CollectiveAllToAllIntoBufferOpHandle<T, B> {
        CollectiveAllToAllIntoBufferOpHandle {
            future: CollectiveAllToAllIntoBufferOpFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricSysOptCollectiveAllToAllIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match self.alloc.ofi.poll_wait_for_collectives() {
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
pub(crate) struct LibfabricSysOptCollectiveBroadcastFuture<T: Remote> {
    pub(crate) alloc: LibfabricSysOptAlloc,
    pub(crate) target: RootSrcOrBuffer<T>,
    pub(crate) len: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricSysOptCollectiveBroadcastFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice() };
        // let result_ptr = self.result.as_mut_ptr();
        // println!(
        //     "performing collective broadcast result ptr: {:?} ",
        //     result_ptr
        // );
        LibfabricSysOptAlloc::broadcast_inner(
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
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || {
            self.alloc.ofi.wait_all();
            match &mut self.target {
                RootSrcOrBuffer::Root(_) => None,
                RootSrcOrBuffer::NotRoot(items, _) => {
                    let mut res = Vec::new();
                    std::mem::swap(items, &mut res);
                    Some(res)
                }
            }
        })
    }

    pub(crate) fn spawn(self) -> LamellarTask<Option<Vec<T>>> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricSysOptCollectiveBroadcastFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysOptCollectiveBroadcastFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricSysOptCollectiveBroadcastFuture<T>>
    for CollectiveBroadcastOpHandle<T>
{
    fn from(f: LibfabricSysOptCollectiveBroadcastFuture<T>) -> CollectiveBroadcastOpHandle<T> {
        CollectiveBroadcastOpHandle {
            future: CollectiveBroadcastOpFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysOptCollectiveBroadcastFuture<T> {
    type Output = Option<Vec<T>>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match self.alloc.ofi.poll_wait_for_collectives() {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(()) => {}
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
pub(crate) struct LibfabricSysOptCollectiveBroadcastIntoBufferFuture<
    T: Remote,
    B: AsLamellarBuffer<T>,
> {
    pub(crate) alloc: LibfabricSysOptAlloc,
    pub(crate) target: RootSrcOrLamellarBufferInner<T, B>,
    pub(crate) len: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricSysOptCollectiveBroadcastIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice() };
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        LibfabricSysOptAlloc::broadcast_inner(
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
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || self.alloc.ofi.wait_all())
    }

    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricSysOptCollectiveBroadcastIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysOptCollectiveBroadcastIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>>
    From<LibfabricSysOptCollectiveBroadcastIntoBufferFuture<T, B>>
    for CollectiveBroadcastIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricSysOptCollectiveBroadcastIntoBufferFuture<T, B>,
    ) -> CollectiveBroadcastIntoBufferOpHandle<T, B> {
        CollectiveBroadcastIntoBufferOpHandle {
            future: CollectiveBroadcastIntoBufferOpFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricSysOptCollectiveBroadcastIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match self.alloc.ofi.poll_wait_for_collectives() {
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
pub(crate) struct LibfabricSysOptCollectiveScatterFuture<T: Remote> {
    pub(crate) alloc: LibfabricSysOptAlloc,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    src_or_root_pe: ScatterInputInner,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricSysOptCollectiveScatterFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        // let result_ptr = self.result.as_mut_ptr();
        // println!(
        //     "performing collective broadcast result ptr: {:?} ",
        //     result_ptr
        // );
        LibfabricSysOptAlloc::scatter_inner(
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
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || {
            self.alloc.ofi.wait_all();
            let mut res = Vec::new();
            std::mem::swap(&mut self.result, &mut res);
            res
        })
    }

    pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricSysOptCollectiveScatterFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysOptCollectiveScatterFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricSysOptCollectiveScatterFuture<T>> for CollectiveScatterOpHandle<T> {
    fn from(f: LibfabricSysOptCollectiveScatterFuture<T>) -> CollectiveScatterOpHandle<T> {
        CollectiveScatterOpHandle {
            future: CollectiveScatterOpFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysOptCollectiveScatterFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match self.alloc.ofi.poll_wait_for_collectives() {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(()) => {}
        }
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysOptCollectiveScatterIntoBufferFuture<
    T: Remote,
    B: AsLamellarBuffer<T>,
> {
    pub(crate) alloc: LibfabricSysOptAlloc,
    pub(crate) len: usize,
    src_or_root_pe: ScatterInputInner,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricSysOptCollectiveScatterIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        LibfabricSysOptAlloc::scatter_inner(
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
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || self.alloc.ofi.wait_all())
    }

    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricSysOptCollectiveScatterIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysOptCollectiveScatterIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricSysOptCollectiveScatterIntoBufferFuture<T, B>>
    for CollectiveScatterIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricSysOptCollectiveScatterIntoBufferFuture<T, B>,
    ) -> CollectiveScatterIntoBufferOpHandle<T, B> {
        CollectiveScatterIntoBufferOpHandle {
            future: CollectiveScatterIntoBufferOpFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricSysOptCollectiveScatterIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match self.alloc.ofi.poll_wait_for_collectives() {
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
pub(crate) struct LibfabricSysOptCollectiveReduceScatterFuture<T: Remote> {
    pub(crate) alloc: LibfabricSysOptAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricSysOptCollectiveReduceScatterFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        let src = &alloc_slice[self.index..self.index + self.len];
        // let result_ptr = self.result.as_mut_ptr();
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        LibfabricSysOptAlloc::reduce_scatter_inner(
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
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || {
            self.alloc.ofi.wait_all();
            let mut res = Vec::new();
            std::mem::swap(&mut self.result, &mut res);
            res
        })
    }

    pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricSysOptCollectiveReduceScatterFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricSysOptCollectiveReduceScatterFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricSysOptCollectiveReduceScatterFuture<T>>
    for CollectiveReduceScatterOpHandle<T>
{
    fn from(
        f: LibfabricSysOptCollectiveReduceScatterFuture<T>,
    ) -> CollectiveReduceScatterOpHandle<T> {
        CollectiveReduceScatterOpHandle {
            future: CollectiveReduceScatterOpFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysOptCollectiveReduceScatterFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match self.alloc.ofi.poll_wait_for_collectives() {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(()) => {}
        }
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysOptCollectiveReduceScatterIntoBufferFuture<
    T: Remote,
    B: AsLamellarBuffer<T>,
> {
    pub(crate) alloc: LibfabricSysOptAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>>
    LibfabricSysOptCollectiveReduceScatterIntoBufferFuture<T, B>
{
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        let src = &alloc_slice[self.index..self.index + self.len];
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        LibfabricSysOptAlloc::reduce_scatter_inner(
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
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || self.alloc.ofi.wait_all())
    }

    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricSysOptCollectiveReduceScatterIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle(
                "a LibfabricSysOptCollectiveReduceScatterIntoBufferFuture",
            )
            .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>>
    From<LibfabricSysOptCollectiveReduceScatterIntoBufferFuture<T, B>>
    for CollectiveReduceScatterIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricSysOptCollectiveReduceScatterIntoBufferFuture<T, B>,
    ) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        CollectiveReduceScatterIntoBufferOpHandle {
            future: CollectiveReduceScatterIntoBufferOpFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricSysOptCollectiveReduceScatterIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match self.alloc.ofi.poll_wait_for_collectives() {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(()) => {}
        }
        Poll::Ready(())
    }
}

impl CommAllocCollectiveAllGather for LibfabricSysOptAlloc {
    fn gather_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllGatherOpHandle<T> {
        LibfabricSysOptCollectiveAllGatherFuture {
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
        LibfabricSysOptCollectiveAllGatherIntoBufferFuture {
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

impl CommAllocCollectiveGather for LibfabricSysOptAlloc {
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
        LibfabricSysOptCollectiveGatherFuture {
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
        LibfabricSysOptCollectiveGatherIntoBufferFuture {
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

impl CommAllocCollectiveAllToAll for LibfabricSysOptAlloc {
    fn alltoall<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllToAllOpHandle<T> {
        LibfabricSysOptCollectiveAllToAllFuture {
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
        LibfabricSysOptCollectiveAllToAllIntoBufferFuture {
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

impl CommAllocCollectiveBroadcast for LibfabricSysOptAlloc {
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

        LibfabricSysOptCollectiveBroadcastFuture {
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
        LibfabricSysOptCollectiveBroadcastIntoBufferFuture {
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

impl CommAllocCollectiveScatter for LibfabricSysOptAlloc {
    fn scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterOpHandle<T> {
        LibfabricSysOptCollectiveScatterFuture {
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
        LibfabricSysOptCollectiveScatterIntoBufferFuture {
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

impl CommAllocCollectiveReduceScatter for LibfabricSysOptAlloc {
    fn reduce_scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: ReduceOp,
        index: usize,
        len: usize,
    ) -> CollectiveReduceScatterOpHandle<T> {
        LibfabricSysOptCollectiveReduceScatterFuture {
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
        LibfabricSysOptCollectiveReduceScatterIntoBufferFuture {
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

    //     LibfabricSysOptCollectiveAllReduceInPlaceFuture {
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

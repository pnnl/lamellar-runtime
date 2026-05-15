use crate::{
    active_messaging::AMCounters, lamellae::{collective::{BroadcastInput, CollectiveAllToAllIntoBufferOpFuture, CollectiveAllToAllIntoBufferOpHandle, CollectiveAllToAllOpFuture, CollectiveAllToAllOpHandle, CollectiveAllGatherIntoBufferOpFuture, CollectiveAllGatherIntoBufferOpHandle, CollectiveAllGatherOpFuture, CollectiveAllGatherOpHandle, CollectiveAllReduceInPlaceOpFuture, CollectiveAllReduceInPlaceOpHandle, CollectiveAllReduceIntoBufferOpFuture, CollectiveAllReduceIntoBufferOpHandle, CollectiveAllReduceOpFuture, CollectiveAllReduceOpHandle, CollectiveBroadcastIntoBufferOpFuture, CollectiveBroadcastIntoBufferOpHandle, CollectiveBroadcastOpFuture, CollectiveBroadcastOpHandle, CollectiveGatherIntoBufferOpFuture, CollectiveGatherIntoBufferOpHandle, CollectiveGatherOpFuture, CollectiveGatherOpHandle, CollectiveReduceInPlaceOpFuture, CollectiveReduceInPlaceOpHandle, CollectiveReduceIntoBufferOpFuture, CollectiveReduceIntoBufferOpHandle, CollectiveReduceOpFuture, CollectiveReduceOpHandle, CollectiveReduceScatterIntoBufferOpFuture, CollectiveReduceScatterIntoBufferOpHandle, CollectiveReduceScatterOpFuture, CollectiveReduceScatterOpHandle, CollectiveScatterIntoBufferOpFuture, CollectiveScatterIntoBufferOpHandle, CollectiveScatterOpFuture, CollectiveScatterOpHandle, CommAllocCollectiveAllToAll, CommAllocCollectiveAllGather, CommAllocCollectiveAllReduce, CommAllocCollectiveBroadcast, CommAllocCollectiveGather, CommAllocCollectiveReduce, CommAllocCollectiveReduceScatter, CommAllocCollectiveScatter, RootOrBuffer, RootOrLamellarBuffer, RootSrcOrBuffer, RootSrcOrLamellarBuffer, RootSrcOrLamellarBufferInner, ScatterInput, ScatterInputInner}, comm::collective::ReduceOp}, warnings::RuntimeWarning, AsLamellarBuffer, LamellarBuffer, LamellarTask, Remote
};


use super::{
    fabric::{LibfabricMtAlloc},
    Scheduler,
};

use pin_project::{pin_project, pinned_drop};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtCollectiveAllReduceFuture<T: Remote> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricMtCollectiveAllReduceFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        LibfabricMtAlloc::allreduce_inner(
            &self.alloc,
            &self.op,
            src,
            &mut self.result,
            false,
        )
        .unwrap();
        
        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
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
impl<T: Remote> PinnedDrop for LibfabricMtCollectiveAllReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricMtCollectiveAllReduceFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricMtCollectiveAllReduceFuture<T>> for CollectiveAllReduceOpHandle<T> {
    fn from(f: LibfabricMtCollectiveAllReduceFuture<T>) -> CollectiveAllReduceOpHandle<T> {
        CollectiveAllReduceOpHandle {
            future: CollectiveAllReduceOpFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricMtCollectiveAllReduceFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        Poll::Ready(res)
    }
}


#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtCollectiveAllReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricMtCollectiveAllReduceIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        LibfabricMtAlloc::allreduce_inner(
            &self.alloc,
            &self.op,
            src,
            self.result.as_mut_slice(),
            false,
        )
        .unwrap();
        
        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricMtCollectiveAllReduceIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricMtCollectiveAllReduceIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricMtCollectiveAllReduceIntoBufferFuture<T, B>> for CollectiveAllReduceIntoBufferOpHandle<T, B> {
    fn from(f: LibfabricMtCollectiveAllReduceIntoBufferFuture<T, B>) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        CollectiveAllReduceIntoBufferOpHandle {
            future: CollectiveAllReduceIntoBufferOpFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricMtCollectiveAllReduceIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        Poll::Ready(())
    }
}


#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtCollectiveAllReduceInPlaceFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(super) op: ReduceOp,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    phantom: std::marker::PhantomData<T>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricMtCollectiveAllReduceInPlaceFuture<T, B> {
    fn exec_op(&mut self) {
        // println!(
        //     "performing collective reduce op: {:?} in place ",
        //     self.op,
        // );

        LibfabricMtAlloc::allreduce_inplace_inner::<T>(
            &self.alloc,
            &self.op,
            self.result.as_mut_slice(),
            false,
        )
        .unwrap();
        
        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricMtCollectiveAllReduceInPlaceFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricMtCollectiveAllReduceInPlaceFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricMtCollectiveAllReduceInPlaceFuture<T, B>> for CollectiveAllReduceInPlaceOpHandle<T, B> {
    fn from(f: LibfabricMtCollectiveAllReduceInPlaceFuture<T, B>) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        CollectiveAllReduceInPlaceOpHandle {
            future: CollectiveAllReduceInPlaceOpFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricMtCollectiveAllReduceInPlaceFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtCollectiveReduceFuture<T: Remote> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(super) op: ReduceOp,
    pub(crate) target: RootOrBuffer<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}



impl<T: Remote> LibfabricMtCollectiveReduceFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        
        LibfabricMtAlloc::reduce_inner(
            &self.alloc,
            &self.op,
            src,
            self.target.as_mut_slice(),
            false,
        )
        .unwrap();
        
        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
        match &mut self.target {
            RootOrBuffer::Root(res) => {
                let mut res_vec = Vec::new();
                std::mem::swap(&mut res_vec, res);
                Some(res_vec)
            },
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
impl<T: Remote> PinnedDrop for LibfabricMtCollectiveReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricMtCollectiveReduceFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricMtCollectiveReduceFuture<T>> for CollectiveReduceOpHandle<T> {
    fn from(f: LibfabricMtCollectiveReduceFuture<T>) -> CollectiveReduceOpHandle<T> {
        CollectiveReduceOpHandle {
            future: CollectiveReduceOpFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricMtCollectiveReduceFuture<T> {
    type Output = Option<Vec<T>>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        match &mut self.target {
            RootOrBuffer::Root(res) => {
                let mut res_vec = Vec::new();
                std::mem::swap(&mut res_vec, res);
                Poll::Ready(Some(res_vec))
            },
            RootOrBuffer::NotRoot(_) => Poll::Ready(None),
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtCollectiveReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(super) op: ReduceOp,
    pub(crate) target: RootOrLamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}



impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricMtCollectiveReduceIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        
        LibfabricMtAlloc::reduce_inner(
            &self.alloc,
            &self.op,
            src,
            self.target.as_mut_slice(),
            false,
        )
        .unwrap();
        
        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self)  {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricMtCollectiveReduceIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricMtCollectiveReduceIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricMtCollectiveReduceIntoBufferFuture<T, B>> for CollectiveReduceIntoBufferOpHandle<T, B> {
    fn from(f: LibfabricMtCollectiveReduceIntoBufferFuture<T, B>) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        CollectiveReduceIntoBufferOpHandle {
            future: CollectiveReduceIntoBufferOpFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricMtCollectiveReduceIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtCollectiveReduceInPlaceFuture<T> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(super) op: ReduceOp,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    root_pe: Option<usize>,
    phantom: std::marker::PhantomData<T>,
}

// impl<T: Remote> LibfabricMtCollectiveReduceInPlaceFuture<T> {
//     fn exec_op(&mut self) {
//         println!(
//             "performing collective reduce op: {:?} in place ",
//             self.op,
//         );

//         LibfabricMtAlloc::reduce_inplace_inner::<T>(
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
//     pub(crate) fn block(mut self) {
//         self.exec_op();
//         self.alloc.ofi.wait_all().unwrap();
//     }

//     pub(crate) fn spawn(mut self) -> LamellarTask<()> {
//         self.exec_op();

//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.clone().spawn_task(self, counters)
//     }
// }

#[pinned_drop]
impl<T> PinnedDrop for LibfabricMtCollectiveReduceInPlaceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricMtCollectiveReduceInPlaceFuture").print();
        }
    }
}

impl<T> From<LibfabricMtCollectiveReduceInPlaceFuture<T>> for CollectiveReduceInPlaceOpHandle<T> {
    fn from(f: LibfabricMtCollectiveReduceInPlaceFuture<T>) -> CollectiveReduceInPlaceOpHandle<T> {
        CollectiveReduceInPlaceOpHandle {
            future: CollectiveReduceInPlaceOpFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricMtCollectiveReduceInPlaceFuture<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        // if !self.spawned { // TODO: FIX
        //     self.exec_op();
        // }
        self.alloc.ofi.wait_all().unwrap();
        Poll::Ready(())
    }
}

impl CommAllocCollectiveAllReduce for LibfabricMtAlloc {
    fn reduce_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
        op: ReduceOp,
    ) -> CollectiveAllReduceOpHandle<T> {

        LibfabricMtCollectiveAllReduceFuture {
            alloc: self.clone(),
            op: op,
            index,
            len,
            result: vec![T::default(); len],
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
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
        
        LibfabricMtCollectiveAllReduceIntoBufferFuture {
            alloc: self.clone(),
            op: op,
            index,
            len,
            result: dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }.into()
    }
    fn reduce_all_in_place<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        source_and_dst: LamellarBuffer<T, B>,
        op: ReduceOp,
    ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {

        LibfabricMtCollectiveAllReduceInPlaceFuture {
            alloc: self.clone(),
            op: op,
            result: source_and_dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            phantom: std::marker::PhantomData,
        }.into()
    }
}


impl CommAllocCollectiveReduce for LibfabricMtAlloc {
    fn reduce<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveReduceOpHandle<T> {
        let target =
            if root_pe != self.ofi.my_pe {
                RootOrBuffer::NotRoot(root_pe)
            }
            else {
                RootOrBuffer::Root(vec![T::default(); len])
            };
        LibfabricMtCollectiveReduceFuture {
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
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        index: usize,
        len: usize,
        root_or_buffer: RootOrLamellarBuffer<T, B>
    ) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        
        LibfabricMtCollectiveReduceIntoBufferFuture {
            alloc: self.clone(),
            op: op,
            index,
            len,
            target: root_or_buffer,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }.into()
    }
    // fn reduce_in_place<T: Remote>( // TODO: Fix
    //     &self,
    //     scheduler: &Arc<Scheduler>,
    //     counters: Vec<Arc<AMCounters>>,
    //     op: ReduceOp,
    //     root_pe: usize,
    // ) -> CollectiveReduceInPlaceOpHandle<T> {
    //     let root =  if root_pe != self.ofi.my_pe {
    //         Some(root_pe)
    //     }
    //     else {
    //         None
    //     };
    //     LibfabricMtCollectiveReduceInPlaceFuture {
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
pub(crate) struct LibfabricMtCollectiveAllGatherFuture<T: Remote> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricMtCollectiveAllGatherFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        LibfabricMtAlloc::allgather_inner(
            &self.alloc,
            src,
            &mut self.result,
            false,
        )
        .unwrap();
        
        // println!(
        //     "collective all gather initiated",
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
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
impl<T: Remote> PinnedDrop for LibfabricMtCollectiveAllGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricMtCollectiveAllGatherFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricMtCollectiveAllGatherFuture<T>> for CollectiveAllGatherOpHandle<T> {
    fn from(f: LibfabricMtCollectiveAllGatherFuture<T>) -> CollectiveAllGatherOpHandle<T> {
        CollectiveAllGatherOpHandle {
            future: CollectiveAllGatherOpFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricMtCollectiveAllGatherFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        Poll::Ready(res)
    }
}


#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtCollectiveAllGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricMtCollectiveAllGatherIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        LibfabricMtAlloc::allgather_inner(
            &self.alloc,
            src,
            self.result.as_mut_slice(),
            false,
        )
        .unwrap();
        
        // println!(
        //     "collective gather initiated",
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricMtCollectiveAllGatherIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricMtCollectiveAllGatherIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricMtCollectiveAllGatherIntoBufferFuture<T, B>> for CollectiveAllGatherIntoBufferOpHandle<T, B> {
    fn from(f: LibfabricMtCollectiveAllGatherIntoBufferFuture<T, B>) -> CollectiveAllGatherIntoBufferOpHandle<T, B> {
        CollectiveAllGatherIntoBufferOpHandle {
            future: CollectiveAllGatherIntoBufferOpFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricMtCollectiveAllGatherIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        Poll::Ready(())
    }
}


#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtCollectiveGatherFuture<T: Remote> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) target: RootOrBuffer<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}



impl<T: Remote> LibfabricMtCollectiveGatherFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        
        LibfabricMtAlloc::gather_inner(
            &self.alloc,
            src,
            self.target.as_mut_slice(),
            false,
        )
        .unwrap();
        
        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
        match &mut self.target {
            RootOrBuffer::Root(res) => {
                let mut res_vec = Vec::new();
                std::mem::swap(&mut res_vec, res);
                Some(res_vec)
            },
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
impl<T: Remote> PinnedDrop for LibfabricMtCollectiveGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricMtCollectiveGatherFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricMtCollectiveGatherFuture<T>> for CollectiveGatherOpHandle<T> {
    fn from(f: LibfabricMtCollectiveGatherFuture<T>) -> CollectiveGatherOpHandle<T> {
        CollectiveGatherOpHandle {
            future: CollectiveGatherOpFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricMtCollectiveGatherFuture<T> {
    type Output = Option<Vec<T>>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        match &mut self.target {
            RootOrBuffer::Root(res) => {
                let mut res_vec = Vec::new();
                std::mem::swap(&mut res_vec, res);
                Poll::Ready(Some(res_vec))
            },
            RootOrBuffer::NotRoot(_) => Poll::Ready(None),
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtCollectiveGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) target: RootOrLamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}



impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricMtCollectiveGatherIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        
        LibfabricMtAlloc::gather_inner(
            &self.alloc,
            src,
            self.target.as_mut_slice(),
            false,
        )
        .unwrap();
        
        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self)  {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricMtCollectiveGatherIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricMtCollectiveGatherIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricMtCollectiveGatherIntoBufferFuture<T, B>> for CollectiveGatherIntoBufferOpHandle<T, B> {
    fn from(f: LibfabricMtCollectiveGatherIntoBufferFuture<T, B>) -> CollectiveGatherIntoBufferOpHandle<T, B> {
        CollectiveGatherIntoBufferOpHandle {
            future: CollectiveGatherIntoBufferOpFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricMtCollectiveGatherIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        Poll::Ready(())
    }
}


#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtCollectiveAllToAllFuture<T: Remote> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricMtCollectiveAllToAllFuture<T> {
    fn exec_op(&mut self) {
        // let result_ptr = self.result.as_mut_ptr();
        // println!(
        //     "performing collective gather result ptr: {:?} ",
        //     result_ptr
        // );
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len] };
        LibfabricMtAlloc::alltoall_inner(
            &self.alloc,
            src,
            &mut self.result,
            false,
        )
        .unwrap();
        
        // println!(
        //     "collective all gather initiated",
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
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
impl<T: Remote> PinnedDrop for LibfabricMtCollectiveAllToAllFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricMtCollectiveAllToAllFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricMtCollectiveAllToAllFuture<T>> for CollectiveAllToAllOpHandle<T> {
    fn from(f: LibfabricMtCollectiveAllToAllFuture<T>) -> CollectiveAllToAllOpHandle<T> {
        CollectiveAllToAllOpHandle {
            future: CollectiveAllToAllOpFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricMtCollectiveAllToAllFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        Poll::Ready(res)
    }
}


#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtCollectiveAllToAllIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricMtCollectiveAllToAllIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len] };
        LibfabricMtAlloc::alltoall_inner(
            &self.alloc,
            src,
            self.result.as_mut_slice(),
            false,
        )
        .unwrap();
        // println!(
        //     "collective gather initiated",
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricMtCollectiveAllToAllIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricMtCollectiveAllToAllIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricMtCollectiveAllToAllIntoBufferFuture<T, B>> for CollectiveAllToAllIntoBufferOpHandle<T, B> {
    fn from(f: LibfabricMtCollectiveAllToAllIntoBufferFuture<T, B>) -> CollectiveAllToAllIntoBufferOpHandle<T, B> {
        CollectiveAllToAllIntoBufferOpHandle {
            future: CollectiveAllToAllIntoBufferOpFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricMtCollectiveAllToAllIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtCollectiveBroadcastFuture<T: Remote> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(crate) target: RootSrcOrBuffer<T> ,
    pub(crate) len: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricMtCollectiveBroadcastFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice() };
        // let result_ptr = self.result.as_mut_ptr();
        // println!(
        //     "performing collective broadcast result ptr: {:?} ",
        //     result_ptr
        // );
        LibfabricMtAlloc::broadcast_inner(
            &self.alloc,
            self.target.as_mut_slice(alloc_slice, self.len),
            false,
        )
        .unwrap();
        
        // println!(
        //     "collective broadcast initiated",
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
        match &mut self.target {
            RootSrcOrBuffer::Root(_) => None,
            RootSrcOrBuffer::NotRoot(items, _) => {
                let mut res = Vec::new();
                std::mem::swap(items, &mut res);
                Some(res)
            },
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
impl<T: Remote> PinnedDrop for LibfabricMtCollectiveBroadcastFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricMtCollectiveBroadcastFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricMtCollectiveBroadcastFuture<T>> for CollectiveBroadcastOpHandle<T> {
    fn from(f: LibfabricMtCollectiveBroadcastFuture<T>) -> CollectiveBroadcastOpHandle<T> {
        CollectiveBroadcastOpHandle {
            future: CollectiveBroadcastOpFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricMtCollectiveBroadcastFuture<T> {
    type Output = Option<Vec<T>>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        match &mut self.target {
            RootSrcOrBuffer::Root(_) => Poll::Ready(None),
            RootSrcOrBuffer::NotRoot(items, _) => {
                let mut res = Vec::new();
                std::mem::swap(items, &mut res);
                Poll::Ready(Some(res))
            },
        }
    }
}


#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtCollectiveBroadcastIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(crate) target: RootSrcOrLamellarBufferInner<T, B>,
    pub(crate) len: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricMtCollectiveBroadcastIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice() };
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        LibfabricMtAlloc::broadcast_inner(
            &self.alloc,
            self.target.as_mut_slice(alloc_slice, self.len),
            false,
        )
        .unwrap();
        // println!(
        //     "collective gather initiated",
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricMtCollectiveBroadcastIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricMtCollectiveBroadcastIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricMtCollectiveBroadcastIntoBufferFuture<T, B>> for CollectiveBroadcastIntoBufferOpHandle<T, B> {
    fn from(f: LibfabricMtCollectiveBroadcastIntoBufferFuture<T, B>) -> CollectiveBroadcastIntoBufferOpHandle<T, B> {
        CollectiveBroadcastIntoBufferOpHandle {
            future: CollectiveBroadcastIntoBufferOpFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricMtCollectiveBroadcastIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtCollectiveScatterFuture<T: Remote> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(crate) len: usize,
    pub(crate) result: Vec<T> ,
    src_or_root_pe: ScatterInputInner,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricMtCollectiveScatterFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        // let result_ptr = self.result.as_mut_ptr();
        // println!(
        //     "performing collective broadcast result ptr: {:?} ",
        //     result_ptr
        // );
        LibfabricMtAlloc::scatter_inner(
            &self.alloc,
            &mut self.result,
            self.src_or_root_pe.as_slice(alloc_slice, self.len),
            false,
        )
        .unwrap();
        
        
        // println!(
        //     "collective scatter initiated",
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
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
impl<T: Remote> PinnedDrop for LibfabricMtCollectiveScatterFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricMtCollectiveScatterFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricMtCollectiveScatterFuture<T>> for CollectiveScatterOpHandle<T> {
    fn from(f: LibfabricMtCollectiveScatterFuture<T>) -> CollectiveScatterOpHandle<T> {
        CollectiveScatterOpHandle {
            future: CollectiveScatterOpFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricMtCollectiveScatterFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        Poll::Ready(res)
    }
}


#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtCollectiveScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(crate) len: usize,
    src_or_root_pe: ScatterInputInner,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricMtCollectiveScatterIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        LibfabricMtAlloc::scatter_inner(
            &self.alloc,
            self.result.as_mut_slice(),
            self.src_or_root_pe.as_slice(alloc_slice, self.len),
            false,
        )
        .unwrap();
        
        // println!(
        //     "collective gather initiated",
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricMtCollectiveScatterIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricMtCollectiveScatterIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricMtCollectiveScatterIntoBufferFuture<T, B>> for CollectiveScatterIntoBufferOpHandle<T, B> {
    fn from(f: LibfabricMtCollectiveScatterIntoBufferFuture<T, B>) -> CollectiveScatterIntoBufferOpHandle<T, B> {
        CollectiveScatterIntoBufferOpHandle {
            future: CollectiveScatterIntoBufferOpFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricMtCollectiveScatterIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        Poll::Ready(())
    }
}


#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtCollectiveReduceScatterFuture<T: Remote> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricMtCollectiveReduceScatterFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        let src = &alloc_slice[self.index..self.index + self.len];
        // let result_ptr = self.result.as_mut_ptr();
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        LibfabricMtAlloc::reduce_scatter_inner(
            &self.alloc,
            &self.op,
            src,
            &mut self.result,
            false,
        )
        .unwrap();
        
        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
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
impl<T: Remote> PinnedDrop for LibfabricMtCollectiveReduceScatterFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricMtCollectiveReduceScatterFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricMtCollectiveReduceScatterFuture<T>> for CollectiveReduceScatterOpHandle<T> {
    fn from(f: LibfabricMtCollectiveReduceScatterFuture<T>) -> CollectiveReduceScatterOpHandle<T> {
        CollectiveReduceScatterOpHandle {
            future: CollectiveReduceScatterOpFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricMtCollectiveReduceScatterFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        Poll::Ready(res)
    }
}


#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtCollectiveReduceScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricMtAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricMtCollectiveReduceScatterIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        let src = &alloc_slice[self.index..self.index + self.len];
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        LibfabricMtAlloc::reduce_scatter_inner(
            &self.alloc,
            &self.op,
            src,
            self.result.as_mut_slice(),
            false,
        )
        .unwrap();
        
        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricMtCollectiveReduceScatterIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricMtCollectiveReduceScatterIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricMtCollectiveReduceScatterIntoBufferFuture<T, B>> for CollectiveReduceScatterIntoBufferOpHandle<T, B> {
    fn from(f: LibfabricMtCollectiveReduceScatterIntoBufferFuture<T, B>) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        CollectiveReduceScatterIntoBufferOpHandle {
            future: CollectiveReduceScatterIntoBufferOpFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricMtCollectiveReduceScatterIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        Poll::Ready(())
    }
}


impl CommAllocCollectiveAllGather for LibfabricMtAlloc {
    fn gather_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllGatherOpHandle<T> {
        LibfabricMtCollectiveAllGatherFuture {
            alloc: self.clone(),
            index,
            len,
            result: vec![T::default(); len * self.num_pes()],
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn gather_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllGatherIntoBufferOpHandle<T, B> {
        
        LibfabricMtCollectiveAllGatherIntoBufferFuture {
            alloc: self.clone(),
            index,
            len,
            result: dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }.into()
    }
}

impl CommAllocCollectiveGather for LibfabricMtAlloc {
    fn gather<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveGatherOpHandle<T> {
        let target =
            if root_pe != self.ofi.my_pe {
                RootOrBuffer::NotRoot(root_pe)
            }
            else {
                RootOrBuffer::Root(vec![T::default(); len * self.num_pes()])
            };
        LibfabricMtCollectiveGatherFuture {
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
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
        root_or_buffer: RootOrLamellarBuffer<T, B>
    ) -> CollectiveGatherIntoBufferOpHandle<T, B> {
        
        LibfabricMtCollectiveGatherIntoBufferFuture {
            alloc: self.clone(),
            index,
            len,
            target: root_or_buffer,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }.into()
    }
}

impl CommAllocCollectiveAllToAll for LibfabricMtAlloc {
    fn alltoall<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllToAllOpHandle<T> {
        LibfabricMtCollectiveAllToAllFuture {
            alloc: self.clone(),
            index,
            len,
            result: vec![T::default(); len * self.num_pes()],
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn alltoall_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        index: usize,
        len: usize,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllToAllIntoBufferOpHandle<T, B> {
        
        LibfabricMtCollectiveAllToAllIntoBufferFuture {
            alloc: self.clone(),
            index,
            len,
            result: dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }.into()
    }
}


impl CommAllocCollectiveBroadcast for LibfabricMtAlloc {
    fn broadcast<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src_or_pe: BroadcastInput,
        len: usize,
    ) -> CollectiveBroadcastOpHandle<T> {
        let target = match src_or_pe {
            BroadcastInput::Root(index) => RootSrcOrBuffer::Root(index),
            BroadcastInput::NotRoot(root_pe) => RootSrcOrBuffer::NotRoot(vec![T::default(); len], root_pe),
        };
        // let target =
        //     if root_pe != self.ofi.my_pe {
        //         BroadcastInput::NotRoot(vec![T::default(); self.num_bytes()/std::mem::size_of::<T>()], root_pe)
        //     }
        //     else {
        //         BroadcastInput::Root()
        //     };
        LibfabricMtCollectiveBroadcastFuture {
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
        counters: Vec<Arc<AMCounters>>,
        root_or_buffer: RootSrcOrLamellarBuffer<T, B>,
        len: usize,
    ) -> CollectiveBroadcastIntoBufferOpHandle<T, B> {
        
        LibfabricMtCollectiveBroadcastIntoBufferFuture {
            alloc: self.clone(),
            target: root_or_buffer.into(),
            len,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }.into()
    }
}


impl CommAllocCollectiveScatter for LibfabricMtAlloc {
    fn scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterOpHandle<T> {
        LibfabricMtCollectiveScatterFuture {
            alloc: self.clone(),
            len,
            src_or_root_pe: src_or_root_pe.into(),
            result: vec![T::default(); len],
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn scatter_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        result: LamellarBuffer<T, B>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterIntoBufferOpHandle<T, B> {
        
        LibfabricMtCollectiveScatterIntoBufferFuture {
            alloc: self.clone(),
            len,
            result: result,
            src_or_root_pe: src_or_root_pe.into(),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }.into()
    }
}

impl CommAllocCollectiveReduceScatter for LibfabricMtAlloc {
    fn reduce_scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        index: usize, 
        len: usize,
    ) -> CollectiveReduceScatterOpHandle<T> {
        LibfabricMtCollectiveReduceScatterFuture {
            alloc: self.clone(),
            op: op,
            index,
            len,
            result: vec![T::default(); len / self.num_pes()],
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
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
        LibfabricMtCollectiveReduceScatterIntoBufferFuture {
            alloc: self.clone(),
            op: op,
            index,
            len,
            result: dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }.into()
    }
    // fn reduce_scatter_in_place<T: Remote, B: AsLamellarBuffer<T>>(
    //     &self,
    //     scheduler: &Arc<Scheduler>,
    //     counters: Vec<Arc<AMCounters>>,
    //     source_and_dst: LamellarBuffer<T, B>,
    //     op: ReduceOp,
    // ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {

    //     LibfabricMtCollectiveAllReduceInPlaceFuture {
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

use crate::{
    active_messaging::AMCounters, lamellae::{collective::{BroadcastInput, CollectiveAllToAllIntoBufferOpFuture, CollectiveAllToAllIntoBufferOpHandle, CollectiveAllToAllOpFuture, CollectiveAllToAllOpHandle, CollectiveAllGatherIntoBufferOpFuture, CollectiveAllGatherIntoBufferOpHandle, CollectiveAllGatherOpFuture, CollectiveAllGatherOpHandle, CollectiveAllReduceInPlaceOpFuture, CollectiveAllReduceInPlaceOpHandle, CollectiveAllReduceIntoBufferOpFuture, CollectiveAllReduceIntoBufferOpHandle, CollectiveAllReduceOpFuture, CollectiveAllReduceOpHandle, CollectiveBroadcastIntoBufferOpFuture, CollectiveBroadcastIntoBufferOpHandle, CollectiveBroadcastOpFuture, CollectiveBroadcastOpHandle, CollectiveGatherIntoBufferOpFuture, CollectiveGatherIntoBufferOpHandle, CollectiveGatherOpFuture, CollectiveGatherOpHandle, CollectiveReduceIntoBufferOpFuture, CollectiveReduceIntoBufferOpHandle, CollectiveReduceOpFuture, CollectiveReduceOpHandle, CollectiveReduceScatterIntoBufferOpFuture, CollectiveReduceScatterIntoBufferOpHandle, CollectiveReduceScatterOpFuture, CollectiveReduceScatterOpHandle, CollectiveScatterIntoBufferOpFuture, CollectiveScatterIntoBufferOpHandle, CollectiveScatterOpFuture, CollectiveScatterOpHandle, CommAllocCollectiveAllToAll, CommAllocCollectiveAllGather, CommAllocCollectiveAllReduce, CommAllocCollectiveBroadcast, CommAllocCollectiveGather, CommAllocCollectiveReduce, CommAllocCollectiveReduceScatter, CommAllocCollectiveScatter, RootOrBuffer, RootOrLamellarBuffer, RootSrcOrBuffer, RootSrcOrLamellarBuffer, RootSrcOrLamellarBufferInner, ScatterInput, ScatterInputInner}, comm::collective::ReduceOp}, warnings::RuntimeWarning, AsLamellarBuffer, LamellarBuffer, LamellarTask, Remote
};

use super::{
    fabric::{LibfabricAlloc},
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
pub(crate) struct LibfabricCollectiveAllReduceFuture<T: Remote> {
    pub(crate) alloc: LibfabricAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricCollectiveAllReduceFuture<T> {
    fn exec_op(&mut self) {
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        LibfabricAlloc::allreduce_inner(
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

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricCollectiveAllReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveAllReduceFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricCollectiveAllReduceFuture<T>> for CollectiveAllReduceOpHandle<T> {
    fn from(f: LibfabricCollectiveAllReduceFuture<T>) -> CollectiveAllReduceOpHandle<T> {
        CollectiveAllReduceOpHandle {
            future: CollectiveAllReduceOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricCollectiveAllReduceFuture<T> {
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
pub(crate) struct LibfabricCollectiveAllReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricCollectiveAllReduceIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        LibfabricAlloc::allreduce_inner(
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

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricCollectiveAllReduceIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveAllReduceIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricCollectiveAllReduceIntoBufferFuture<T, B>> for CollectiveAllReduceIntoBufferOpHandle<T, B> {
    fn from(f: LibfabricCollectiveAllReduceIntoBufferFuture<T, B>) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        CollectiveAllReduceIntoBufferOpHandle {
            future: CollectiveAllReduceIntoBufferOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricCollectiveAllReduceIntoBufferFuture<T, B> {
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
pub(crate) struct LibfabricCollectiveAllReduceInPlaceFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricAlloc,
    pub(super) op: ReduceOp,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
    phantom: std::marker::PhantomData<T>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricCollectiveAllReduceInPlaceFuture<T, B> {
    fn exec_op(&mut self) {
        // println!(
        //     "performing collective reduce op: {:?} in place ",
        //     self.op,
        // );

        LibfabricAlloc::allreduce_inplace_inner::<T>(
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

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricCollectiveAllReduceInPlaceFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveAllReduceInPlaceFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricCollectiveAllReduceInPlaceFuture<T, B>> for CollectiveAllReduceInPlaceOpHandle<T, B> {
    fn from(f: LibfabricCollectiveAllReduceInPlaceFuture<T, B>) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        CollectiveAllReduceInPlaceOpHandle {
            future: CollectiveAllReduceInPlaceOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricCollectiveAllReduceInPlaceFuture<T, B> {
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
pub(crate) struct LibfabricCollectiveReduceFuture<T: Remote> {
    pub(crate) alloc: LibfabricAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(super) op: ReduceOp,
    pub(crate) target: RootOrBuffer<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}



impl<T: Remote> LibfabricCollectiveReduceFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        
        LibfabricAlloc::reduce_inner(
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

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricCollectiveReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveReduceFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricCollectiveReduceFuture<T>> for CollectiveReduceOpHandle<T> {
    fn from(f: LibfabricCollectiveReduceFuture<T>) -> CollectiveReduceOpHandle<T> {
        CollectiveReduceOpHandle {
            future: CollectiveReduceOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricCollectiveReduceFuture<T> {
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
pub(crate) struct LibfabricCollectiveReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(super) op: ReduceOp,
    pub(crate) target: RootOrLamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}



impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricCollectiveReduceIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        
        LibfabricAlloc::reduce_inner(
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

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricCollectiveReduceIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveReduceIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricCollectiveReduceIntoBufferFuture<T, B>> for CollectiveReduceIntoBufferOpHandle<T, B> {
    fn from(f: LibfabricCollectiveReduceIntoBufferFuture<T, B>) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        CollectiveReduceIntoBufferOpHandle {
            future: CollectiveReduceIntoBufferOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricCollectiveReduceIntoBufferFuture<T, B> {
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
pub(crate) struct LibfabricCollectiveReduceInPlaceFuture<T> {
    pub(crate) alloc: LibfabricAlloc,
    pub(super) op: ReduceOp,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
    root_pe: Option<usize>,
    phantom: std::marker::PhantomData<T>,
}

// impl<T: Remote> LibfabricCollectiveReduceInPlaceFuture<T> {
//     fn exec_op(&mut self) {
//         println!(
//             "performing collective reduce op: {:?} in place ",
//             self.op,
//         );

//         LibfabricAlloc::reduce_inplace_inner::<T>(
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
//         self.alloc.ofi.wait_all().unwrap();
//     }

//     pub(crate) fn spawn(self) -> LamellarTask<()> {
//         self.exec_op();

//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.clone().spawn_task(self, counters)
//     }
// }

#[pinned_drop]
impl<T> PinnedDrop for LibfabricCollectiveReduceInPlaceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveReduceInPlaceFuture").print();
        }
    }
}

// TODO: reduce_in_place never wired up for any backend, CollectiveReduceInPlaceOpHandle/Future commented out in comm/collective.rs
// impl<T> From<LibfabricCollectiveReduceInPlaceFuture<T>> for CollectiveReduceInPlaceOpHandle<T> {
//     fn from(f: LibfabricCollectiveReduceInPlaceFuture<T>) -> CollectiveReduceInPlaceOpHandle<T> {
//         CollectiveReduceInPlaceOpHandle {
//             future: CollectiveReduceInPlaceOpFuture::Libfabric(f),
//         }
//     }
// }

impl<T: Remote> Future for LibfabricCollectiveReduceInPlaceFuture<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        // if !self.spawned { // TODO: FIX
        //     self.exec_op();
        // }
        self.alloc.ofi.wait_all().unwrap();
        Poll::Ready(())
    }
}

impl CommAllocCollectiveAllReduce for LibfabricAlloc {
    fn reduce_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        op: ReduceOp,
    ) -> CollectiveAllReduceOpHandle<T> {

        LibfabricCollectiveAllReduceFuture {
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
        
        LibfabricCollectiveAllReduceIntoBufferFuture {
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
        counters: Option<Arc<[Arc<AMCounters>]>>,
        source_and_dst: LamellarBuffer<T, B>,
        op: ReduceOp,
    ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {

        LibfabricCollectiveAllReduceInPlaceFuture {
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


impl CommAllocCollectiveReduce for LibfabricAlloc {
    fn reduce<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
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
                RootOrBuffer::Root((0..len).map(|_| unsafe { std::mem::zeroed() }).collect())
            };
        LibfabricCollectiveReduceFuture {
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
        root_or_buffer: RootOrLamellarBuffer<T, B>
    ) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        
        LibfabricCollectiveReduceIntoBufferFuture {
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
    //     LibfabricCollectiveReduceInPlaceFuture {
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
pub(crate) struct LibfabricCollectiveAllGatherFuture<T: Remote> {
    pub(crate) alloc: LibfabricAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricCollectiveAllGatherFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        //println!(
        //     "performing collective gather result ptr: {:?} ",
        //     result_ptr
        // );
        LibfabricAlloc::allgather_inner(
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

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricCollectiveAllGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveAllGatherFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricCollectiveAllGatherFuture<T>> for CollectiveAllGatherOpHandle<T> {
    fn from(f: LibfabricCollectiveAllGatherFuture<T>) -> CollectiveAllGatherOpHandle<T> {
        CollectiveAllGatherOpHandle {
            future: CollectiveAllGatherOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricCollectiveAllGatherFuture<T> {
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
pub(crate) struct LibfabricCollectiveAllGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricCollectiveAllGatherIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        LibfabricAlloc::allgather_inner(
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

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricCollectiveAllGatherIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveAllGatherIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricCollectiveAllGatherIntoBufferFuture<T, B>> for CollectiveAllGatherIntoBufferOpHandle<T, B> {
    fn from(f: LibfabricCollectiveAllGatherIntoBufferFuture<T, B>) -> CollectiveAllGatherIntoBufferOpHandle<T, B> {
        CollectiveAllGatherIntoBufferOpHandle {
            future: CollectiveAllGatherIntoBufferOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricCollectiveAllGatherIntoBufferFuture<T, B> {
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
pub(crate) struct LibfabricCollectiveGatherFuture<T: Remote> {
    pub(crate) alloc: LibfabricAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) target: RootOrBuffer<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}



impl<T: Remote> LibfabricCollectiveGatherFuture<T> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        
        LibfabricAlloc::gather_inner(
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

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricCollectiveGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveGatherFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricCollectiveGatherFuture<T>> for CollectiveGatherOpHandle<T> {
    fn from(f: LibfabricCollectiveGatherFuture<T>) -> CollectiveGatherOpHandle<T> {
        CollectiveGatherOpHandle {
            future: CollectiveGatherOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricCollectiveGatherFuture<T> {
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
pub(crate) struct LibfabricCollectiveGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) target: RootOrLamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}



impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricCollectiveGatherIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        
        LibfabricAlloc::gather_inner(
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

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricCollectiveGatherIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveGatherIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricCollectiveGatherIntoBufferFuture<T, B>> for CollectiveGatherIntoBufferOpHandle<T, B> {
    fn from(f: LibfabricCollectiveGatherIntoBufferFuture<T, B>) -> CollectiveGatherIntoBufferOpHandle<T, B> {
        CollectiveGatherIntoBufferOpHandle {
            future: CollectiveGatherIntoBufferOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricCollectiveGatherIntoBufferFuture<T, B> {
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
pub(crate) struct LibfabricCollectiveAllToAllFuture<T: Remote> {
    pub(crate) alloc: LibfabricAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricCollectiveAllToAllFuture<T> {
    fn exec_op(&mut self) {
        // let result_ptr = self.result.as_mut_ptr();
        // println!(
        //     "performing collective gather result ptr: {:?} ",
        //     result_ptr
        // );
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len] };
        LibfabricAlloc::alltoall_inner(
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

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricCollectiveAllToAllFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveAllToAllFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricCollectiveAllToAllFuture<T>> for CollectiveAllToAllOpHandle<T> {
    fn from(f: LibfabricCollectiveAllToAllFuture<T>) -> CollectiveAllToAllOpHandle<T> {
        CollectiveAllToAllOpHandle {
            future: CollectiveAllToAllOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricCollectiveAllToAllFuture<T> {
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
pub(crate) struct LibfabricCollectiveAllToAllIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricCollectiveAllToAllIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len] };
        LibfabricAlloc::alltoall_inner(
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

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricCollectiveAllToAllIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveAllToAllIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricCollectiveAllToAllIntoBufferFuture<T, B>> for CollectiveAllToAllIntoBufferOpHandle<T, B> {
    fn from(f: LibfabricCollectiveAllToAllIntoBufferFuture<T, B>) -> CollectiveAllToAllIntoBufferOpHandle<T, B> {
        CollectiveAllToAllIntoBufferOpHandle {
            future: CollectiveAllToAllIntoBufferOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricCollectiveAllToAllIntoBufferFuture<T, B> {
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
pub(crate) struct LibfabricCollectiveBroadcastFuture<T: Remote> {
    pub(crate) alloc: LibfabricAlloc,
    pub(crate) target: RootSrcOrBuffer<T> ,
    pub(crate) len: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricCollectiveBroadcastFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice() };
        // let result_ptr = self.result.as_mut_ptr();
        // println!(
        //     "performing collective broadcast result ptr: {:?} ",
        //     result_ptr
        // );
        LibfabricAlloc::broadcast_inner(
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

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricCollectiveBroadcastFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveBroadcastFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricCollectiveBroadcastFuture<T>> for CollectiveBroadcastOpHandle<T> {
    fn from(f: LibfabricCollectiveBroadcastFuture<T>) -> CollectiveBroadcastOpHandle<T> {
        CollectiveBroadcastOpHandle {
            future: CollectiveBroadcastOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricCollectiveBroadcastFuture<T> {
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
pub(crate) struct LibfabricCollectiveBroadcastIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricAlloc,
    pub(crate) target: RootSrcOrLamellarBufferInner<T, B>,
    pub(crate) len: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricCollectiveBroadcastIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice() };
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        LibfabricAlloc::broadcast_inner(
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

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricCollectiveBroadcastIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveBroadcastIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricCollectiveBroadcastIntoBufferFuture<T, B>> for CollectiveBroadcastIntoBufferOpHandle<T, B> {
    fn from(f: LibfabricCollectiveBroadcastIntoBufferFuture<T, B>) -> CollectiveBroadcastIntoBufferOpHandle<T, B> {
        CollectiveBroadcastIntoBufferOpHandle {
            future: CollectiveBroadcastIntoBufferOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricCollectiveBroadcastIntoBufferFuture<T, B> {
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
pub(crate) struct LibfabricCollectiveScatterFuture<T: Remote> {
    pub(crate) alloc: LibfabricAlloc,
    pub(crate) len: usize,
    pub(crate) result: Vec<T> ,
    src_or_root_pe: ScatterInputInner,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricCollectiveScatterFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        // let result_ptr = self.result.as_mut_ptr();
        // println!(
        //     "performing collective broadcast result ptr: {:?} ",
        //     result_ptr
        // );
        LibfabricAlloc::scatter_inner(
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

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricCollectiveScatterFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveScatterFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricCollectiveScatterFuture<T>> for CollectiveScatterOpHandle<T> {
    fn from(f: LibfabricCollectiveScatterFuture<T>) -> CollectiveScatterOpHandle<T> {
        CollectiveScatterOpHandle {
            future: CollectiveScatterOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricCollectiveScatterFuture<T> {
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
pub(crate) struct LibfabricCollectiveScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricAlloc,
    pub(crate) len: usize,
    src_or_root_pe: ScatterInputInner,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricCollectiveScatterIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        LibfabricAlloc::scatter_inner(
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

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricCollectiveScatterIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveScatterIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricCollectiveScatterIntoBufferFuture<T, B>> for CollectiveScatterIntoBufferOpHandle<T, B> {
    fn from(f: LibfabricCollectiveScatterIntoBufferFuture<T, B>) -> CollectiveScatterIntoBufferOpHandle<T, B> {
        CollectiveScatterIntoBufferOpHandle {
            future: CollectiveScatterIntoBufferOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricCollectiveScatterIntoBufferFuture<T, B> {
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
pub(crate) struct LibfabricCollectiveReduceScatterFuture<T: Remote> {
    pub(crate) alloc: LibfabricAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricCollectiveReduceScatterFuture<T> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        let src = &alloc_slice[self.index..self.index + self.len];
        // let result_ptr = self.result.as_mut_ptr();
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        LibfabricAlloc::reduce_scatter_inner(
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

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricCollectiveReduceScatterFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveReduceScatterFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricCollectiveReduceScatterFuture<T>> for CollectiveReduceScatterOpHandle<T> {
    fn from(f: LibfabricCollectiveReduceScatterFuture<T>) -> CollectiveReduceScatterOpHandle<T> {
        CollectiveReduceScatterOpHandle {
            future: CollectiveReduceScatterOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricCollectiveReduceScatterFuture<T> {
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
pub(crate) struct LibfabricCollectiveReduceScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricCollectiveReduceScatterIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        let src = &alloc_slice[self.index..self.index + self.len];
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        LibfabricAlloc::reduce_scatter_inner(
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

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricCollectiveReduceScatterIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveReduceScatterIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricCollectiveReduceScatterIntoBufferFuture<T, B>> for CollectiveReduceScatterIntoBufferOpHandle<T, B> {
    fn from(f: LibfabricCollectiveReduceScatterIntoBufferFuture<T, B>) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        CollectiveReduceScatterIntoBufferOpHandle {
            future: CollectiveReduceScatterIntoBufferOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricCollectiveReduceScatterIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        Poll::Ready(())
    }
}


impl CommAllocCollectiveAllGather for LibfabricAlloc {
    fn gather_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllGatherOpHandle<T> {
        LibfabricCollectiveAllGatherFuture {
            alloc: self.clone(),
            index,
            len,
            result: (0..len * self.num_pes()).map(|_| unsafe { std::mem::zeroed() }).collect(),
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
        
        LibfabricCollectiveAllGatherIntoBufferFuture {
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

impl CommAllocCollectiveGather for LibfabricAlloc {
    fn gather<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveGatherOpHandle<T> {
        let target =
            if root_pe != self.ofi.my_pe {
                RootOrBuffer::NotRoot(root_pe)
            }
            else {
                RootOrBuffer::Root((0..len * self.num_pes()).map(|_| unsafe { std::mem::zeroed() }).collect())
            };
        LibfabricCollectiveGatherFuture {
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
        root_or_buffer: RootOrLamellarBuffer<T, B>
    ) -> CollectiveGatherIntoBufferOpHandle<T, B> {
        
        LibfabricCollectiveGatherIntoBufferFuture {
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

impl CommAllocCollectiveAllToAll for LibfabricAlloc {
    fn alltoall<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllToAllOpHandle<T> {
        LibfabricCollectiveAllToAllFuture {
            alloc: self.clone(),
            index,
            len,
            result: (0..len * self.num_pes()).map(|_| unsafe { std::mem::zeroed() }).collect(),
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
        
        LibfabricCollectiveAllToAllIntoBufferFuture {
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


impl CommAllocCollectiveBroadcast for LibfabricAlloc {
    fn broadcast<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src_or_pe: BroadcastInput,
        len: usize,
    ) -> CollectiveBroadcastOpHandle<T> {
        let target = match src_or_pe {
            BroadcastInput::Root(index) => RootSrcOrBuffer::Root(index),
            BroadcastInput::NotRoot(root_pe) => RootSrcOrBuffer::NotRoot((0..len).map(|_| unsafe { std::mem::zeroed() }).collect(), root_pe),
        };

        LibfabricCollectiveBroadcastFuture {
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
        
        LibfabricCollectiveBroadcastIntoBufferFuture {
            alloc: self.clone(),
            target: root_or_buffer.into(),
            len,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }.into()
    }
}


impl CommAllocCollectiveScatter for LibfabricAlloc {
    fn scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterOpHandle<T> {
        LibfabricCollectiveScatterFuture {
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
        
        LibfabricCollectiveScatterIntoBufferFuture {
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

impl CommAllocCollectiveReduceScatter for LibfabricAlloc {
    fn reduce_scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: ReduceOp,
        index: usize, 
        len: usize,
    ) -> CollectiveReduceScatterOpHandle<T> {
        LibfabricCollectiveReduceScatterFuture {
            alloc: self.clone(),
            op: op,
            index,
            len,
            result: (0..len / self.num_pes()).map(|_| unsafe { std::mem::zeroed() }).collect(),
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
        LibfabricCollectiveReduceScatterIntoBufferFuture {
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
    //     counters: Option<Arc<[Arc<AMCounters>]>>,
    //     source_and_dst: LamellarBuffer<T, B>,
    //     op: ReduceOp,
    // ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {

    //     LibfabricCollectiveAllReduceInPlaceFuture {
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

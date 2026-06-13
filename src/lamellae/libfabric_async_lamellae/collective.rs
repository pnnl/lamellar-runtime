use crate::{
    active_messaging::AMCounters, lamellae::{collective::{BroadcastInput, CollectiveAllToAllIntoBufferOpFuture, CollectiveAllToAllIntoBufferOpHandle, CollectiveAllToAllOpFuture, CollectiveAllToAllOpHandle, CollectiveAllGatherIntoBufferOpFuture, CollectiveAllGatherIntoBufferOpHandle, CollectiveAllGatherOpFuture, CollectiveAllGatherOpHandle, CollectiveAllReduceInPlaceOpFuture, CollectiveAllReduceInPlaceOpHandle, CollectiveAllReduceIntoBufferOpFuture, CollectiveAllReduceIntoBufferOpHandle, CollectiveAllReduceOpFuture, CollectiveAllReduceOpHandle, CollectiveBroadcastIntoBufferOpFuture, CollectiveBroadcastIntoBufferOpHandle, CollectiveBroadcastOpFuture, CollectiveBroadcastOpHandle, CollectiveGatherIntoBufferOpFuture, CollectiveGatherIntoBufferOpHandle, CollectiveGatherOpFuture, CollectiveGatherOpHandle, CollectiveReduceInPlaceOpFuture, CollectiveReduceInPlaceOpHandle, CollectiveReduceIntoBufferOpFuture, CollectiveReduceIntoBufferOpHandle, CollectiveReduceOpFuture, CollectiveReduceOpHandle, CollectiveReduceScatterIntoBufferOpFuture, CollectiveReduceScatterIntoBufferOpHandle, CollectiveReduceScatterOpFuture, CollectiveReduceScatterOpHandle, CollectiveScatterIntoBufferOpFuture, CollectiveScatterIntoBufferOpHandle, CollectiveScatterOpFuture, CollectiveScatterOpHandle, CommAllocCollectiveAllToAll, CommAllocCollectiveAllGather, CommAllocCollectiveAllReduce, CommAllocCollectiveBroadcast, CommAllocCollectiveGather, CommAllocCollectiveReduce, CommAllocCollectiveReduceScatter, CommAllocCollectiveScatter, RootOrBuffer, RootOrLamellarBuffer, RootSrcOrBuffer, RootSrcOrLamellarBuffer, RootSrcOrLamellarBufferInner, ScatterInput, ScatterInputInner}, comm::collective::ReduceOp}, warnings::RuntimeWarning, AsLamellarBuffer, LamellarBuffer, LamellarTask, Remote
};

use super::{
    fabric::{LibfabricAsyncAlloc},
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
pub(crate) struct LibfabricAsyncCollectiveAllReduceFuture<T: Remote> {
    fut_data: Option<CollectiveReduceData<T>>,
    fut: Option<Pin<Box<dyn Future<Output=Vec<T>> + Send>>>,
}

struct CollectiveReduceData<T> {
    pub(crate) alloc: LibfabricAsyncAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    #[allow(dead_code)] // WIP: spawned tracking not yet used in libfabric-async
    pub(crate) spawned: bool,
}

impl<T: Remote> CollectiveReduceData<T> {
    async fn exec_op(mut self) -> Vec<T> {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        LibfabricAsyncAlloc::allreduce_inner(
            &self.alloc,
            &self.op,
            src,
            &mut self.result,
        )
        .await
        .unwrap();
        self.result
    }

    pub(crate) fn block(self) -> Vec<T> {
        self.scheduler.clone().block_on(async move { self.exec_op().await })
    }

    pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(async move { self.exec_op().await }, counters)
    }
}

impl<T: Remote> LibfabricAsyncCollectiveAllReduceFuture<T> {
    pub(crate) fn block(self) -> Vec<T> {
        let fut_data = self.fut_data.take().unwrap();
        fut_data.block()
    }

    pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
        let fut_data = self.fut_data.take().unwrap();
        fut_data.spawn()
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricAsyncCollectiveAllReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveAllReduceFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricAsyncCollectiveAllReduceFuture<T>> for CollectiveAllReduceOpHandle<T> {
    fn from(f: LibfabricAsyncCollectiveAllReduceFuture<T>) -> CollectiveAllReduceOpHandle<T> {
        CollectiveAllReduceOpHandle {
            future: CollectiveAllReduceOpFuture::LibfabricAsync(f),
        }
    }
}

impl<T: Remote> Future for LibfabricAsyncCollectiveAllReduceFuture<T> {
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => {
                fut.as_mut().poll(cx)
            },
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                let fut = Box::pin(fut_data.exec_op());
                mut_self.fut = Some(fut);
                Poll::Pending
            }
        }
    }
}

struct CollectiveReduceIntoBufferData<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricAsyncAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    #[allow(dead_code)] // WIP: spawned tracking not yet used in libfabric-async
    pub(crate) spawned: bool,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncCollectiveAllReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    fut_data: Option<CollectiveReduceIntoBufferData<T, B>>,
    fut: Option<Pin<Box<dyn Future<Output=()> + Send>>>,
}

// impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricAsyncCollectiveAllReduceIntoBufferFuture<T, B> {
//     fn exec_op(&mut self) {
//         // println!(
//         //     "performing collective reduce op: {:?} result ptr: {:?} ",
//         //     self.op,
//         //     result_ptr
//         // );
//         let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
//         LibfabricAsyncAlloc::allreduce_inner(
//             &self.alloc,
//             &self.op,
//             src,
//             self.result.as_mut_slice(),
//             false,
//         )
//         .unwrap();
        
//         // println!(
//         //     "collective reduce op: {:?} initiated",
//         //     self.op,
//         // );
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

// #[pinned_drop]
// impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricAsyncCollectiveAllReduceIntoBufferFuture<T, B> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveAllReduceIntoBufferFuture").print();
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricAsyncCollectiveAllReduceIntoBufferFuture<T, B>> for CollectiveAllReduceIntoBufferOpHandle<T, B> {
//     fn from(f: LibfabricAsyncCollectiveAllReduceIntoBufferFuture<T, B>) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
//         CollectiveAllReduceIntoBufferOpHandle {
//             future: CollectiveAllReduceIntoBufferOpFuture::LibfabricAsync(f),
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricAsyncCollectiveAllReduceIntoBufferFuture<T, B> {
//     type Output = ();
//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         self.alloc.ofi.wait_all().unwrap();
//         Poll::Ready(())
//     }
// }


// #[pin_project(PinnedDrop)]
// pub(crate) struct LibfabricAsyncCollectiveAllReduceInPlaceFuture<T: Remote, B: AsLamellarBuffer<T>> {
//     pub(crate) alloc: LibfabricAsyncAlloc,
//     pub(super) op: ReduceOp,
//     pub(crate) result: LamellarBuffer<T, B>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
//     phantom: std::marker::PhantomData<T>,
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricAsyncCollectiveAllReduceInPlaceFuture<T, B> {
//     fn exec_op(&mut self) {
//         // println!(
//         //     "performing collective reduce op: {:?} in place ",
//         //     self.op,
//         // );

//         LibfabricAsyncAlloc::allreduce_inplace_inner::<T>(
//             &self.alloc,
//             &self.op,
//             self.result.as_mut_slice(),
//             false,
//         )
//         .unwrap();
        
//         // println!(
//         //     "collective reduce op: {:?} initiated",
//         //     self.op,
//         // );
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

// #[pinned_drop]
// impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricAsyncCollectiveAllReduceInPlaceFuture<T, B> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveAllReduceInPlaceFuture").print();
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricAsyncCollectiveAllReduceInPlaceFuture<T, B>> for CollectiveAllReduceInPlaceOpHandle<T, B> {
//     fn from(f: LibfabricAsyncCollectiveAllReduceInPlaceFuture<T, B>) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
//         CollectiveAllReduceInPlaceOpHandle {
//             future: CollectiveAllReduceInPlaceOpFuture::LibfabricAsync(f),
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricAsyncCollectiveAllReduceInPlaceFuture<T, B> {
//     type Output = ();
//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         self.alloc.ofi.wait_all().unwrap();
//         Poll::Ready(())
//     }
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct LibfabricAsyncCollectiveReduceFuture<T: Remote> {
//     pub(crate) alloc: LibfabricAsyncAlloc,
//     pub(crate) index: usize,
//     pub(crate) len: usize,
//     pub(super) op: ReduceOp,
//     pub(crate) target: RootOrBuffer<T>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }



// impl<T: Remote> LibfabricAsyncCollectiveReduceFuture<T> {
//     fn exec_op(&mut self) {
//         let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        
//         LibfabricAsyncAlloc::reduce_inner(
//             &self.alloc,
//             &self.op,
//             src,
//             self.target.as_mut_slice(),
//             false,
//         )
//         .unwrap();
        
//         // println!(
//         //     "collective reduce op: {:?} initiated",
//         //     self.op,
//         // );
//         self.spawned = true;
//     }
//     pub(crate) fn block(self) -> Option<Vec<T>> {
//         self.exec_op();
//         self.alloc.ofi.wait_all().unwrap();
//         match &mut self.target {
//             RootOrBuffer::Root(res) => {
//                 let mut res_vec = Vec::new();
//                 std::mem::swap(&mut res_vec, res);
//                 Some(res_vec)
//             },
//             RootOrBuffer::NotRoot(_) => None,
//         }
//     }

//     pub(crate) fn spawn(self) -> LamellarTask<Option<Vec<T>>> {
//         self.exec_op();

//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.clone().spawn_task(self, counters)
//     }
// }

// #[pinned_drop]
// impl<T: Remote> PinnedDrop for LibfabricAsyncCollectiveReduceFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveReduceFuture").print();
//         }
//     }
// }

// impl<T: Remote> From<LibfabricAsyncCollectiveReduceFuture<T>> for CollectiveReduceOpHandle<T> {
//     fn from(f: LibfabricAsyncCollectiveReduceFuture<T>) -> CollectiveReduceOpHandle<T> {
//         CollectiveReduceOpHandle {
//             future: CollectiveReduceOpFuture::LibfabricAsync(f),
//         }
//     }
// }

// impl<T: Remote> Future for LibfabricAsyncCollectiveReduceFuture<T> {
//     type Output = Option<Vec<T>>;
//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         self.alloc.ofi.wait_all().unwrap();
//         match &mut self.target {
//             RootOrBuffer::Root(res) => {
//                 let mut res_vec = Vec::new();
//                 std::mem::swap(&mut res_vec, res);
//                 Poll::Ready(Some(res_vec))
//             },
//             RootOrBuffer::NotRoot(_) => Poll::Ready(None),
//         }
//     }
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct LibfabricAsyncCollectiveReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
//     pub(crate) alloc: LibfabricAsyncAlloc,
//     pub(crate) index: usize,
//     pub(crate) len: usize,
//     pub(super) op: ReduceOp,
//     pub(crate) target: RootOrLamellarBuffer<T, B>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }



// impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricAsyncCollectiveReduceIntoBufferFuture<T, B> {
//     fn exec_op(&mut self) {
//         let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        
//         LibfabricAsyncAlloc::reduce_inner(
//             &self.alloc,
//             &self.op,
//             src,
//             self.target.as_mut_slice(),
//             false,
//         )
//         .unwrap();
        
//         // println!(
//         //     "collective reduce op: {:?} initiated",
//         //     self.op,
//         // );
//         self.spawned = true;
//     }
//     pub(crate) fn block(self)  {
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

// #[pinned_drop]
// impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricAsyncCollectiveReduceIntoBufferFuture<T, B> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveReduceIntoBufferFuture").print();
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricAsyncCollectiveReduceIntoBufferFuture<T, B>> for CollectiveReduceIntoBufferOpHandle<T, B> {
//     fn from(f: LibfabricAsyncCollectiveReduceIntoBufferFuture<T, B>) -> CollectiveReduceIntoBufferOpHandle<T, B> {
//         CollectiveReduceIntoBufferOpHandle {
//             future: CollectiveReduceIntoBufferOpFuture::LibfabricAsync(f),
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricAsyncCollectiveReduceIntoBufferFuture<T, B> {
//     type Output = ();
//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         self.alloc.ofi.wait_all().unwrap();
//         Poll::Ready(())
//     }
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct LibfabricAsyncCollectiveReduceInPlaceFuture<T> {
//     pub(crate) alloc: LibfabricAsyncAlloc,
//     pub(super) op: ReduceOp,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
//     root_pe: Option<usize>,
//     phantom: std::marker::PhantomData<T>,
// }

// // impl<T: Remote> LibfabricAsyncCollectiveReduceInPlaceFuture<T> {
// //     fn exec_op(&mut self) {
// //         println!(
// //             "performing collective reduce op: {:?} in place ",
// //             self.op,
// //         );

// //         LibfabricAsyncAlloc::reduce_inplace_inner::<T>(
// //             &self.alloc,
// //             &self.op,
// //             self.root_pe.clone(),
// //             false,
// //         )
// //         .unwrap();
// //         
// //         println!(
// //             "collective reduce op: {:?} initiated",
// //             self.op,
// //         );
// //         self.spawned = true;
// //     }
// //     pub(crate) fn block(self) {
// //         self.exec_op();
// //         self.alloc.ofi.wait_all().unwrap();
// //     }

// //     pub(crate) fn spawn(self) -> LamellarTask<()> {
// //         self.exec_op();

// //         let mut counters = Vec::new();
// //         std::mem::swap(&mut counters, &mut self.counters);
// //         self.scheduler.clone().spawn_task(self, counters)
// //     }
// // }

// #[pinned_drop]
// impl<T> PinnedDrop for LibfabricAsyncCollectiveReduceInPlaceFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveReduceInPlaceFuture").print();
//         }
//     }
// }

// impl<T> From<LibfabricAsyncCollectiveReduceInPlaceFuture<T>> for CollectiveReduceInPlaceOpHandle<T> {
//     fn from(f: LibfabricAsyncCollectiveReduceInPlaceFuture<T>) -> CollectiveReduceInPlaceOpHandle<T> {
//         CollectiveReduceInPlaceOpHandle {
//             future: CollectiveReduceInPlaceOpFuture::LibfabricAsync(f),
//         }
//     }
// }

// impl<T: Remote> Future for LibfabricAsyncCollectiveReduceInPlaceFuture<T> {
//     type Output = ();
//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         // if !self.spawned { // TODO: FIX
//         //     self.exec_op();
//         // }
//         self.alloc.ofi.wait_all().unwrap();
//         Poll::Ready(())
//     }
// }

// impl CommAllocCollectiveAllReduce for LibfabricAsyncAlloc {
//     fn reduce_all<T: Remote>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Option<Arc<[Arc<AMCounters>]>>,
//         index: usize,
//         len: usize,
//         op: ReduceOp,
//     ) -> CollectiveAllReduceOpHandle<T> {
        
//         CollectiveAllReduceOpHandle
//         LibfabricAsyncCollectiveAllReduceFuture {
//             // alloc: self.clone(),
//             // op: op,
//             // index,
//             // len,
//             result: vec![T::default(); len],
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//         }
//         .into()
//     }
//     fn reduce_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Option<Arc<[Arc<AMCounters>]>>,
//         index: usize,
//         len: usize,
//         op: ReduceOp,
//         dst: LamellarBuffer<T, B>,
//     ) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        
//         LibfabricAsyncCollectiveAllReduceIntoBufferFuture {
//             alloc: self.clone(),
//             op: op,
//             index,
//             len,
//             result: dst,
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//         }.into()
//     }
//     fn reduce_all_in_place<T: Remote, B: AsLamellarBuffer<T>>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Option<Arc<[Arc<AMCounters>]>>,
//         source_and_dst: LamellarBuffer<T, B>,
//         op: ReduceOp,
//     ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {

//         LibfabricAsyncCollectiveAllReduceInPlaceFuture {
//             alloc: self.clone(),
//             op: op,
//             result: source_and_dst,
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//             phantom: std::marker::PhantomData,
//         }.into()
//     }
// }


// impl CommAllocCollectiveReduce for LibfabricAsyncAlloc {
//     fn reduce<T: Remote>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Option<Arc<[Arc<AMCounters>]>>,
//         op: ReduceOp,
//         index: usize,
//         len: usize,
//         root_pe: usize,
//     ) -> CollectiveReduceOpHandle<T> {
//         let target =
//             if root_pe != self.ofi.my_pe {
//                 RootOrBuffer::NotRoot(root_pe)
//             }
//             else {
//                 RootOrBuffer::Root(vec![T::default(); len])
//             };
//         LibfabricAsyncCollectiveReduceFuture {
//             alloc: self.clone(),
//             index,
//             len,
//             op: op,
//             target,
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//         }
//         .into()
//     }
//     fn reduce_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Option<Arc<[Arc<AMCounters>]>>,
//         op: ReduceOp,
//         index: usize,
//         len: usize,
//         root_or_buffer: RootOrLamellarBuffer<T, B>
//     ) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        
//         LibfabricAsyncCollectiveReduceIntoBufferFuture {
//             alloc: self.clone(),
//             op: op,
//             index,
//             len,
//             target: root_or_buffer,
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//         }.into()
//     }
//     // fn reduce_in_place<T: Remote>( // TODO: Fix
//     //     &self,
//     //     scheduler: &Arc<Scheduler>,
//     //     counters: Option<Arc<[Arc<AMCounters>]>>,
//     //     op: ReduceOp,
//     //     root_pe: usize,
//     // ) -> CollectiveReduceInPlaceOpHandle<T> {
//     //     let root =  if root_pe != self.ofi.my_pe {
//     //         Some(root_pe)
//     //     }
//     //     else {
//     //         None
//     //     };
//     //     LibfabricAsyncCollectiveReduceInPlaceFuture {
//     //         alloc: self.clone(),
//     //         op: op,
//     //         spawned: false,
//     //         scheduler: scheduler.clone(),
//     //         counters,
//     //         root_pe: root,
//     //         phantom: std::marker::PhantomData,
//     //     }.into()
//     // }
// }


// #[pin_project(PinnedDrop)]
// pub(crate) struct LibfabricAsyncCollectiveAllGatherFuture<T: Remote> {
//     pub(crate) alloc: LibfabricAsyncAlloc,
//     pub(crate) index: usize,
//     pub(crate) len: usize,
//     pub(crate) result: Vec<T>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Remote> LibfabricAsyncCollectiveAllGatherFuture<T> {
//     fn exec_op(&mut self) {
//         let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
//         let result_ptr = self.result.as_mut_ptr();
//         //println!(
//         //     "performing collective gather result ptr: {:?} ",
//         //     result_ptr
//         // );
//         LibfabricAsyncAlloc::allgather_inner(
//             &self.alloc,
//             src,
//             &mut self.result,
//             false,
//         )
//         .unwrap();
        
//         // println!(
//         //     "collective all gather initiated",
//         // );
//         self.spawned = true;
//     }
//     pub(crate) fn block(self) -> Vec<T> {
//         self.exec_op();
//         self.alloc.ofi.wait_all().unwrap();
//         let mut res = Vec::new();
//         std::mem::swap(&mut self.result, &mut res);
//         res
//     }

//     pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
//         self.exec_op();

//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.clone().spawn_task(self, counters)
//     }
// }

// #[pinned_drop]
// impl<T: Remote> PinnedDrop for LibfabricAsyncCollectiveAllGatherFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveAllGatherFuture").print();
//         }
//     }
// }

// impl<T: Remote> From<LibfabricAsyncCollectiveAllGatherFuture<T>> for CollectiveAllGatherOpHandle<T> {
//     fn from(f: LibfabricAsyncCollectiveAllGatherFuture<T>) -> CollectiveAllGatherOpHandle<T> {
//         CollectiveAllGatherOpHandle {
//             future: CollectiveAllGatherOpFuture::LibfabricAsync(f),
//         }
//     }
// }

// impl<T: Remote> Future for LibfabricAsyncCollectiveAllGatherFuture<T> {
//     type Output = Vec<T>;
//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         self.alloc.ofi.wait_all().unwrap();
//         let mut res = Vec::new();
//         std::mem::swap(&mut self.result, &mut res);
//         Poll::Ready(res)
//     }
// }


// #[pin_project(PinnedDrop)]
// pub(crate) struct LibfabricAsyncCollectiveAllGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
//     pub(crate) alloc: LibfabricAsyncAlloc,
//     pub(crate) index: usize,
//     pub(crate) len: usize,
//     pub(crate) result: LamellarBuffer<T, B>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricAsyncCollectiveAllGatherIntoBufferFuture<T, B> {
//     fn exec_op(&mut self) {
//         let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
//         // println!(
//         //     "performing collective reduce op: {:?} result ptr: {:?} ",
//         //     self.op,
//         //     result_ptr
//         // );
//         LibfabricAsyncAlloc::allgather_inner(
//             &self.alloc,
//             src,
//             self.result.as_mut_slice(),
//             false,
//         )
//         .unwrap();
        
//         // println!(
//         //     "collective gather initiated",
//         // );
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

// #[pinned_drop]
// impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricAsyncCollectiveAllGatherIntoBufferFuture<T, B> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveAllGatherIntoBufferFuture").print();
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricAsyncCollectiveAllGatherIntoBufferFuture<T, B>> for CollectiveAllGatherIntoBufferOpHandle<T, B> {
//     fn from(f: LibfabricAsyncCollectiveAllGatherIntoBufferFuture<T, B>) -> CollectiveAllGatherIntoBufferOpHandle<T, B> {
//         CollectiveAllGatherIntoBufferOpHandle {
//             future: CollectiveAllGatherIntoBufferOpFuture::LibfabricAsync(f),
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricAsyncCollectiveAllGatherIntoBufferFuture<T, B> {
//     type Output = ();
//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         self.alloc.ofi.wait_all().unwrap();
//         Poll::Ready(())
//     }
// }


// #[pin_project(PinnedDrop)]
// pub(crate) struct LibfabricAsyncCollectiveGatherFuture<T: Remote> {
//     pub(crate) alloc: LibfabricAsyncAlloc,
//     pub(crate) index: usize,
//     pub(crate) len: usize,
//     pub(crate) target: RootOrBuffer<T>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }



// impl<T: Remote> LibfabricAsyncCollectiveGatherFuture<T> {
//     fn exec_op(&mut self) {
//         let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        
//         LibfabricAsyncAlloc::gather_inner(
//             &self.alloc,
//             src,
//             self.target.as_mut_slice(),
//             false,
//         )
//         .unwrap();
        
//         // println!(
//         //     "collective reduce op: {:?} initiated",
//         //     self.op,
//         // );
//         self.spawned = true;
//     }
//     pub(crate) fn block(self) -> Option<Vec<T>> {
//         self.exec_op();
//         self.alloc.ofi.wait_all().unwrap();
//         match &mut self.target {
//             RootOrBuffer::Root(res) => {
//                 let mut res_vec = Vec::new();
//                 std::mem::swap(&mut res_vec, res);
//                 Some(res_vec)
//             },
//             RootOrBuffer::NotRoot(_) => None,
//         }
//     }

//     pub(crate) fn spawn(self) -> LamellarTask<Option<Vec<T>>> {
//         self.exec_op();

//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.clone().spawn_task(self, counters)
//     }
// }

// #[pinned_drop]
// impl<T: Remote> PinnedDrop for LibfabricAsyncCollectiveGatherFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveGatherFuture").print();
//         }
//     }
// }

// impl<T: Remote> From<LibfabricAsyncCollectiveGatherFuture<T>> for CollectiveGatherOpHandle<T> {
//     fn from(f: LibfabricAsyncCollectiveGatherFuture<T>) -> CollectiveGatherOpHandle<T> {
//         CollectiveGatherOpHandle {
//             future: CollectiveGatherOpFuture::LibfabricAsync(f),
//         }
//     }
// }

// impl<T: Remote> Future for LibfabricAsyncCollectiveGatherFuture<T> {
//     type Output = Option<Vec<T>>;
//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         self.alloc.ofi.wait_all().unwrap();
//         match &mut self.target {
//             RootOrBuffer::Root(res) => {
//                 let mut res_vec = Vec::new();
//                 std::mem::swap(&mut res_vec, res);
//                 Poll::Ready(Some(res_vec))
//             },
//             RootOrBuffer::NotRoot(_) => Poll::Ready(None),
//         }
//     }
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct LibfabricAsyncCollectiveGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
//     pub(crate) alloc: LibfabricAsyncAlloc,
//     pub(crate) index: usize,
//     pub(crate) len: usize,
//     pub(crate) target: RootOrLamellarBuffer<T, B>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }



// impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricAsyncCollectiveGatherIntoBufferFuture<T, B> {
//     fn exec_op(&mut self) {
//         let src = unsafe { &self.alloc.as_slice()[self.index..self.index+self.len]};
        
//         LibfabricAsyncAlloc::gather_inner(
//             &self.alloc,
//             src,
//             self.target.as_mut_slice(),
//             false,
//         )
//         .unwrap();
        
//         // println!(
//         //     "collective reduce op: {:?} initiated",
//         //     self.op,
//         // );
//         self.spawned = true;
//     }
//     pub(crate) fn block(self)  {
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

// #[pinned_drop]
// impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricAsyncCollectiveGatherIntoBufferFuture<T, B> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveGatherIntoBufferFuture").print();
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricAsyncCollectiveGatherIntoBufferFuture<T, B>> for CollectiveGatherIntoBufferOpHandle<T, B> {
//     fn from(f: LibfabricAsyncCollectiveGatherIntoBufferFuture<T, B>) -> CollectiveGatherIntoBufferOpHandle<T, B> {
//         CollectiveGatherIntoBufferOpHandle {
//             future: CollectiveGatherIntoBufferOpFuture::LibfabricAsync(f),
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricAsyncCollectiveGatherIntoBufferFuture<T, B> {
//     type Output = ();
//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         self.alloc.ofi.wait_all().unwrap();
//         Poll::Ready(())
//     }
// }


// #[pin_project(PinnedDrop)]
// pub(crate) struct LibfabricAsyncCollectiveAllBroadcastFuture<T: Remote> {
//     pub(crate) alloc: LibfabricAsyncAlloc,
//     pub(crate) src: MemregionRdmaInputInner<T>,
//     pub(crate) result: Vec<T>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Remote> LibfabricAsyncCollectiveAllBroadcastFuture<T> {
//     fn exec_op(&mut self) {
//         // let result_ptr = self.result.as_mut_ptr();
//         // println!(
//         //     "performing collective gather result ptr: {:?} ",
//         //     result_ptr
//         // );
//         LibfabricAsyncAlloc::alltoall_inner(
//             &self.alloc,
//             self.src.as_slice(),
//             &mut self.result,
//             false,
//         )
//         .unwrap();
        
//         // println!(
//         //     "collective all gather initiated",
//         // );
//         self.spawned = true;
//     }
//     pub(crate) fn block(self) -> Vec<T> {
//         self.exec_op();
//         self.alloc.ofi.wait_all().unwrap();
//         let mut res = Vec::new();
//         std::mem::swap(&mut self.result, &mut res);
//         res
//     }

//     pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
//         self.exec_op();

//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.clone().spawn_task(self, counters)
//     }
// }

// #[pinned_drop]
// impl<T: Remote> PinnedDrop for LibfabricAsyncCollectiveAllBroadcastFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveAllBroadcastFuture").print();
//         }
//     }
// }

// impl<T: Remote> From<LibfabricAsyncCollectiveAllBroadcastFuture<T>> for CollectiveAllBroadcastOpHandle<T> {
//     fn from(f: LibfabricAsyncCollectiveAllBroadcastFuture<T>) -> CollectiveAllBroadcastOpHandle<T> {
//         CollectiveAllBroadcastOpHandle {
//             future: CollectiveAllBroadcastOpFuture::LibfabricAsync(f),
//         }
//     }
// }

// impl<T: Remote> Future for LibfabricAsyncCollectiveAllBroadcastFuture<T> {
//     type Output = Vec<T>;
//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         self.alloc.ofi.wait_all().unwrap();
//         let mut res = Vec::new();
//         std::mem::swap(&mut self.result, &mut res);
//         Poll::Ready(res)
//     }
// }


// #[pin_project(PinnedDrop)]
// pub(crate) struct LibfabricAsyncCollectiveAllBroadcastIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
//     pub(crate) alloc: LibfabricAsyncAlloc,
//     pub(crate) src: MemregionRdmaInputInner<T>,
//     pub(crate) result: LamellarBuffer<T, B>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricAsyncCollectiveAllBroadcastIntoBufferFuture<T, B> {
//     fn exec_op(&mut self) {
//         // println!(
//         //     "performing collective reduce op: {:?} result ptr: {:?} ",
//         //     self.op,
//         //     result_ptr
//         // );
//         LibfabricAsyncAlloc::alltoall_inner(
//             &self.alloc,
//             self.src.as_slice(),
//             self.result.as_mut_slice(),
//             false,
//         )
//         .unwrap();
//         // println!(
//         //     "collective gather initiated",
//         // );
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

// #[pinned_drop]
// impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricAsyncCollectiveAllBroadcastIntoBufferFuture<T, B> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveAllBroadcastIntoBufferFuture").print();
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricAsyncCollectiveAllBroadcastIntoBufferFuture<T, B>> for CollectiveAllBroadcastIntoBufferOpHandle<T, B> {
//     fn from(f: LibfabricAsyncCollectiveAllBroadcastIntoBufferFuture<T, B>) -> CollectiveAllBroadcastIntoBufferOpHandle<T, B> {
//         CollectiveAllBroadcastIntoBufferOpHandle {
//             future: CollectiveAllBroadcastIntoBufferOpFuture::LibfabricAsync(f),
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricAsyncCollectiveAllBroadcastIntoBufferFuture<T, B> {
//     type Output = ();
//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         self.alloc.ofi.wait_all().unwrap();
//         Poll::Ready(())
//     }
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct LibfabricAsyncCollectiveBroadcastFuture<T: Remote> {
//     pub(crate) alloc: LibfabricAsyncAlloc,
//     pub(crate) target: RootSrcOrBuffer<T> ,
//     pub(crate) len: usize,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Remote> LibfabricAsyncCollectiveBroadcastFuture<T> {
//     fn exec_op(&mut self) {
//         let alloc_slice = unsafe { self.alloc.as_slice() };
//         // let result_ptr = self.result.as_mut_ptr();
//         // println!(
//         //     "performing collective broadcast result ptr: {:?} ",
//         //     result_ptr
//         // );
//         LibfabricAsyncAlloc::broadcast_inner(
//             &self.alloc,
//             self.target.as_mut_slice(alloc_slice, self.len),
//             false,
//         )
//         .unwrap();
        
//         // println!(
//         //     "collective broadcast initiated",
//         // );
//         self.spawned = true;
//     }
//     pub(crate) fn block(self) -> Option<Vec<T>> {
//         self.exec_op();
//         self.alloc.ofi.wait_all().unwrap();
//         match &mut self.target {
//             RootSrcOrBuffer::Root(_) => None,
//             RootSrcOrBuffer::NotRoot(items, _) => {
//                 let mut res = Vec::new();
//                 std::mem::swap(items, &mut res);
//                 Some(res)
//             },
//         }
//     }

//     pub(crate) fn spawn(self) -> LamellarTask<Option<Vec<T>>> {
//         self.exec_op();

//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.clone().spawn_task(self, counters)
//     }
// }

// #[pinned_drop]
// impl<T: Remote> PinnedDrop for LibfabricAsyncCollectiveBroadcastFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveBroadcastFuture").print();
//         }
//     }
// }

// impl<T: Remote> From<LibfabricAsyncCollectiveBroadcastFuture<T>> for CollectiveBroadcastOpHandle<T> {
//     fn from(f: LibfabricAsyncCollectiveBroadcastFuture<T>) -> CollectiveBroadcastOpHandle<T> {
//         CollectiveBroadcastOpHandle {
//             future: CollectiveBroadcastOpFuture::LibfabricAsync(f),
//         }
//     }
// }

// impl<T: Remote> Future for LibfabricAsyncCollectiveBroadcastFuture<T> {
//     type Output = Option<Vec<T>>;
//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         self.alloc.ofi.wait_all().unwrap();
//         match &mut self.target {
//             RootSrcOrBuffer::Root(_) => Poll::Ready(None),
//             RootSrcOrBuffer::NotRoot(items, _) => {
//                 let mut res = Vec::new();
//                 std::mem::swap(items, &mut res);
//                 Poll::Ready(Some(res))
//             },
//         }
//     }
// }


// #[pin_project(PinnedDrop)]
// pub(crate) struct LibfabricAsyncCollectiveBroadcastIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
//     pub(crate) alloc: LibfabricAsyncAlloc,
//     pub(crate) target: RootSrcOrLamellarBufferInner<T, B>,
//     pub(crate) len: usize,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricAsyncCollectiveBroadcastIntoBufferFuture<T, B> {
//     fn exec_op(&mut self) {
//         let alloc_slice = unsafe { self.alloc.as_slice() };
//         // println!(
//         //     "performing collective reduce op: {:?} result ptr: {:?} ",
//         //     self.op,
//         //     result_ptr
//         // );
//         LibfabricAsyncAlloc::broadcast_inner(
//             &self.alloc,
//             self.target.as_mut_slice(alloc_slice, self.len),
//             false,
//         )
//         .unwrap();
//         // println!(
//         //     "collective gather initiated",
//         // );
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

// #[pinned_drop]
// impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricAsyncCollectiveBroadcastIntoBufferFuture<T, B> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveBroadcastIntoBufferFuture").print();
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricAsyncCollectiveBroadcastIntoBufferFuture<T, B>> for CollectiveBroadcastIntoBufferOpHandle<T, B> {
//     fn from(f: LibfabricAsyncCollectiveBroadcastIntoBufferFuture<T, B>) -> CollectiveBroadcastIntoBufferOpHandle<T, B> {
//         CollectiveBroadcastIntoBufferOpHandle {
//             future: CollectiveBroadcastIntoBufferOpFuture::LibfabricAsync(f),
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricAsyncCollectiveBroadcastIntoBufferFuture<T, B> {
//     type Output = ();
//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         self.alloc.ofi.wait_all().unwrap();
//         Poll::Ready(())
//     }
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct LibfabricAsyncCollectiveScatterFuture<T: Remote> {
//     pub(crate) alloc: LibfabricAsyncAlloc,
//     pub(crate) len: usize,
//     pub(crate) result: Vec<T> ,
//     src_or_root_pe: ScatterInputInner,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Remote> LibfabricAsyncCollectiveScatterFuture<T> {
//     fn exec_op(&mut self) {
//         let alloc_slice = unsafe { self.alloc.as_slice() };
//         // let result_ptr = self.result.as_mut_ptr();
//         // println!(
//         //     "performing collective broadcast result ptr: {:?} ",
//         //     result_ptr
//         // );
//         LibfabricAsyncAlloc::scatter_inner(
//             &self.alloc,
//             &mut self.result,
//             self.src_or_root_pe.as_slice(alloc_slice, self.len),
//             false,
//         )
//         .unwrap();
        
        
//         // println!(
//         //     "collective scatter initiated",
//         // );
//         self.spawned = true;
//     }
//     pub(crate) fn block(self) -> Vec<T> {
//         self.exec_op();
//         self.alloc.ofi.wait_all().unwrap();
//         let mut res = Vec::new();
//         std::mem::swap(&mut self.result, &mut res);
//         res
//     }

//     pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
//         self.exec_op();

//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.clone().spawn_task(self, counters)
//     }
// }

// #[pinned_drop]
// impl<T: Remote> PinnedDrop for LibfabricAsyncCollectiveScatterFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveScatterFuture").print();
//         }
//     }
// }

// impl<T: Remote> From<LibfabricAsyncCollectiveScatterFuture<T>> for CollectiveScatterOpHandle<T> {
//     fn from(f: LibfabricAsyncCollectiveScatterFuture<T>) -> CollectiveScatterOpHandle<T> {
//         CollectiveScatterOpHandle {
//             future: CollectiveScatterOpFuture::LibfabricAsync(f),
//         }
//     }
// }

// impl<T: Remote> Future for LibfabricAsyncCollectiveScatterFuture<T> {
//     type Output = Vec<T>;
//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         self.alloc.ofi.wait_all().unwrap();
//         let mut res = Vec::new();
//         std::mem::swap(&mut self.result, &mut res);
//         Poll::Ready(res)
//     }
// }


// // #[pin_project(PinnedDrop)]
// // pub(crate) struct LibfabricAsyncCollectiveScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
// //     pub(crate) alloc: LibfabricAsyncAlloc,
// //     pub(crate) len: usize,
// //     src_or_root_pe: ScatterInputInner,
// //     pub(crate) result: LamellarBuffer<T, B>,
// //     pub(crate) scheduler: Arc<Scheduler>,
// //     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
// //     pub(crate) spawned: bool,
// // }

// // impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricAsyncCollectiveScatterIntoBufferFuture<T, B> {
// //     fn exec_op(&mut self) {
// //         let alloc_slice = unsafe { self.alloc.as_slice() };
// //         // println!(
// //         //     "performing collective reduce op: {:?} result ptr: {:?} ",
// //         //     self.op,
// //         //     result_ptr
// //         // );
// //         LibfabricAsyncAlloc::scatter_inner(
// //             &self.alloc,
// //             self.result.as_mut_slice(),
// //             self.src_or_root_pe.as_slice(alloc_slice, self.len),
// //             false,
// //         )
// //         .unwrap();
        
// //         // println!(
// //         //     "collective gather initiated",
// //         // );
// //         self.spawned = true;
// //     }
// //     pub(crate) fn block(self) {
// //         self.exec_op();
// //         self.alloc.ofi.wait_all().unwrap();
// //     }

// //     pub(crate) fn spawn(self) -> LamellarTask<()> {
// //         self.exec_op();

// //         let mut counters = Vec::new();
// //         std::mem::swap(&mut counters, &mut self.counters);
// //         self.scheduler.clone().spawn_task(self, counters)
// //     }
// // }

// // #[pinned_drop]
// // impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricAsyncCollectiveScatterIntoBufferFuture<T, B> {
// //     fn drop(self: Pin<&mut Self>) {
// //         if !self.spawned {
// //             RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveScatterIntoBufferFuture").print();
// //         }
// //     }
// // }

// // impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricAsyncCollectiveScatterIntoBufferFuture<T, B>> for CollectiveScatterIntoBufferOpHandle<T, B> {
// //     fn from(f: LibfabricAsyncCollectiveScatterIntoBufferFuture<T, B>) -> CollectiveScatterIntoBufferOpHandle<T, B> {
// //         CollectiveScatterIntoBufferOpHandle {
// //             future: CollectiveScatterIntoBufferOpFuture::LibfabricAsync(f),
// //         }
// //     }
// // }

// // impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricAsyncCollectiveScatterIntoBufferFuture<T, B> {
// //     type Output = ();
// //     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
// //         if !self.spawned {
// //             self.exec_op();
// //         }
// //         self.alloc.ofi.wait_all().unwrap();
// //         Poll::Ready(())
// //     }
// // }


// // #[pin_project(PinnedDrop)]
// // pub(crate) struct LibfabricAsyncCollectiveReduceScatterFuture<T: Remote> {
// //     pub(crate) alloc: LibfabricAsyncAlloc,
// //     pub(super) op: ReduceOp,
// //     pub(crate) index: usize,
// //     pub(crate) len: usize,
// //     pub(crate) result: Vec<T>,
// //     pub(crate) scheduler: Arc<Scheduler>,
// //     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
// //     pub(crate) spawned: bool,
// // }

// // impl<T: Remote> LibfabricAsyncCollectiveReduceScatterFuture<T> {
// //     fn exec_op(&mut self) {
// //         let alloc_slice = unsafe { self.alloc.as_slice() };
// //         let src = &alloc_slice[self.index..self.index + self.len];
// //         // let result_ptr = self.result.as_mut_ptr();
// //         // println!(
// //         //     "performing collective reduce op: {:?} result ptr: {:?} ",
// //         //     self.op,
// //         //     result_ptr
// //         // );
// //         LibfabricAsyncAlloc::reduce_scatter_inner(
// //             &self.alloc,
// //             &self.op,
// //             src,
// //             &mut self.result,
// //             false,
// //         )
// //         .unwrap();
        
// //         // println!(
// //         //     "collective reduce op: {:?} initiated",
// //         //     self.op,
// //         // );
// //         self.spawned = true;
// //     }
// //     pub(crate) fn block(self) -> Vec<T> {
// //         self.exec_op();
// //         self.alloc.ofi.wait_all().unwrap();
// //         let mut res = Vec::new();
// //         std::mem::swap(&mut self.result, &mut res);
// //         res
// //     }

// //     pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
// //         self.exec_op();

// //         let mut counters = Vec::new();
// //         std::mem::swap(&mut counters, &mut self.counters);
// //         self.scheduler.clone().spawn_task(self, counters)
// //     }
// // }

// // #[pinned_drop]
// // impl<T: Remote> PinnedDrop for LibfabricAsyncCollectiveReduceScatterFuture<T> {
// //     fn drop(self: Pin<&mut Self>) {
// //         if !self.spawned {
// //             RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveReduceScatterFuture").print();
// //         }
// //     }
// // }

// // impl<T: Remote> From<LibfabricAsyncCollectiveReduceScatterFuture<T>> for CollectiveReduceScatterOpHandle<T> {
// //     fn from(f: LibfabricAsyncCollectiveReduceScatterFuture<T>) -> CollectiveReduceScatterOpHandle<T> {
// //         CollectiveReduceScatterOpHandle {
// //             future: CollectiveReduceScatterOpFuture::LibfabricAsync(f),
// //         }
// //     }
// // }

// // impl<T: Remote> Future for LibfabricAsyncCollectiveReduceScatterFuture<T> {
// //     type Output = Vec<T>;
// //     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
// //         if !self.spawned {
// //             self.exec_op();
// //         }
// //         self.alloc.ofi.wait_all().unwrap();
// //         let mut res = Vec::new();
// //         std::mem::swap(&mut self.result, &mut res);
// //         Poll::Ready(res)
// //     }
// // }


// // #[pin_project(PinnedDrop)]
// // pub(crate) struct LibfabricAsyncCollectiveReduceScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
// //     pub(crate) alloc: LibfabricAsyncAlloc,
// //     pub(super) op: ReduceOp,
// //     pub(crate) index: usize,
// //     pub(crate) len: usize,
// //     pub(crate) result: LamellarBuffer<T, B>,
// //     pub(crate) scheduler: Arc<Scheduler>,
// //     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
// //     pub(crate) spawned: bool,
// // }

// // impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricAsyncCollectiveReduceScatterIntoBufferFuture<T, B> {
// //     fn exec_op(&mut self) {
// //         let alloc_slice = unsafe { self.alloc.as_slice() };
// //         let src = &alloc_slice[self.index..self.index + self.len];
// //         // println!(
// //         //     "performing collective reduce op: {:?} result ptr: {:?} ",
// //         //     self.op,
// //         //     result_ptr
// //         // );
// //         LibfabricAsyncAlloc::reduce_scatter_inner(
// //             &self.alloc,
// //             &self.op,
// //             src,
// //             self.result.as_mut_slice(),
// //             false,
// //         )
// //         .unwrap();
        
// //         // println!(
// //         //     "collective reduce op: {:?} initiated",
// //         //     self.op,
// //         // );
// //         self.spawned = true;
// //     }
// //     pub(crate) fn block(self) {
// //         self.exec_op();
// //         self.alloc.ofi.wait_all().unwrap();
// //     }

// //     pub(crate) fn spawn(self) -> LamellarTask<()> {
// //         self.exec_op();

// //         let mut counters = Vec::new();
// //         std::mem::swap(&mut counters, &mut self.counters);
// //         self.scheduler.clone().spawn_task(self, counters)
// //     }
// // }

// // #[pinned_drop]
// // impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricAsyncCollectiveReduceScatterIntoBufferFuture<T, B> {
// //     fn drop(self: Pin<&mut Self>) {
// //         if !self.spawned {
// //             RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveReduceScatterIntoBufferFuture").print();
// //         }
// //     }
// // }

// // impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricAsyncCollectiveReduceScatterIntoBufferFuture<T, B>> for CollectiveReduceScatterIntoBufferOpHandle<T, B> {
// //     fn from(f: LibfabricAsyncCollectiveReduceScatterIntoBufferFuture<T, B>) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
// //         CollectiveReduceScatterIntoBufferOpHandle {
// //             future: CollectiveReduceScatterIntoBufferOpFuture::LibfabricAsync(f),
// //         }
// //     }
// // }

// // impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricAsyncCollectiveReduceScatterIntoBufferFuture<T, B> {
// //     type Output = ();
// //     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
// //         if !self.spawned {
// //             self.exec_op();
// //         }
// //         self.alloc.ofi.wait_all().unwrap();
// //         Poll::Ready(())
// //     }
// // }


// // impl CommAllocCollectiveAllGather for LibfabricAsyncAlloc {
// //     fn gather_all<T: Remote>(
// //         &self,
// //         scheduler: &Arc<Scheduler>,
// //         counters: Option<Arc<[Arc<AMCounters>]>>,
// //         index: usize,
// //         len: usize,
// //     ) -> CollectiveAllGatherOpHandle<T> {
// //         LibfabricAsyncCollectiveAllGatherFuture {
// //             alloc: self.clone(),
// //             index,
// //             len,
// //             result: vec![T::default(); len * self.num_pes()],
// //             spawned: false,
// //             scheduler: scheduler.clone(),
// //             counters,
// //         }
// //         .into()
// //     }
// //     fn gather_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
// //         &self,
// //         scheduler: &Arc<Scheduler>,
// //         counters: Option<Arc<[Arc<AMCounters>]>>,
// //         index: usize,
// //         len: usize,
// //         dst: LamellarBuffer<T, B>,
// //     ) -> CollectiveAllGatherIntoBufferOpHandle<T, B> {
        
// //         LibfabricAsyncCollectiveAllGatherIntoBufferFuture {
// //             alloc: self.clone(),
// //             index,
// //             len,
// //             result: dst,
// //             spawned: false,
// //             scheduler: scheduler.clone(),
// //             counters,
// //         }.into()
// //     }
// // }

// // impl CommAllocCollectiveGather for LibfabricAsyncAlloc {
// //     fn gather<T: Remote>(
// //         &self,
// //         scheduler: &Arc<Scheduler>,
// //         counters: Option<Arc<[Arc<AMCounters>]>>,
// //         index: usize,
// //         len: usize,
// //         root_pe: usize,
// //     ) -> CollectiveGatherOpHandle<T> {
// //         let target =
// //             if root_pe != self.ofi.my_pe {
// //                 RootOrBuffer::NotRoot(root_pe)
// //             }
// //             else {
// //                 RootOrBuffer::Root(vec![T::default(); len * self.num_pes()])
// //             };
// //         LibfabricAsyncCollectiveGatherFuture {
// //             alloc: self.clone(),
// //             index,
// //             len,
// //             target,
// //             spawned: false,
// //             scheduler: scheduler.clone(),
// //             counters,
// //         }
// //         .into()
// //     }
// //     fn gather_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
// //         &self,
// //         scheduler: &Arc<Scheduler>,
// //         counters: Option<Arc<[Arc<AMCounters>]>>,
// //         index: usize,
// //         len: usize,
// //         root_or_buffer: RootOrLamellarBuffer<T, B>
// //     ) -> CollectiveGatherIntoBufferOpHandle<T, B> {
        
// //         LibfabricAsyncCollectiveGatherIntoBufferFuture {
// //             alloc: self.clone(),
// //             index,
// //             len,
// //             target: root_or_buffer,
// //             spawned: false,
// //             scheduler: scheduler.clone(),
// //             counters,
// //         }.into()
// //     }
// // }

// // impl CommAllocCollectiveAllBroadcast for LibfabricAsyncAlloc {
// //     fn broadcast_all<T: Remote>(
// //         &self,
// //         scheduler: &Arc<Scheduler>,
// //         counters: Option<Arc<[Arc<AMCounters>]>>,
// //         src: impl Into<MemregionRdmaInputInner<T>>,
// //     ) -> CollectiveAllBroadcastOpHandle<T> {
// //         let memregion_in = src.into();
// //         let len = memregion_in.len();
// //         LibfabricAsyncCollectiveAllBroadcastFuture {
// //             alloc: self.clone(),
// //             src: memregion_in,
// //             result: vec![T::default(); len * self.num_pes()],
// //             spawned: false,
// //             scheduler: scheduler.clone(),
// //             counters,
// //         }
// //         .into()
// //     }
// //     fn broadcast_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
// //         &self,
// //         scheduler: &Arc<Scheduler>,
// //         counters: Option<Arc<[Arc<AMCounters>]>>,
// //         src: impl Into<MemregionRdmaInputInner<T>>,
// //         dst: LamellarBuffer<T, B>,
// //     ) -> CollectiveAllBroadcastIntoBufferOpHandle<T, B> {
        
// //         LibfabricAsyncCollectiveAllBroadcastIntoBufferFuture {
// //             alloc: self.clone(),
// //             src: src.into(),
// //             result: dst,
// //             spawned: false,
// //             scheduler: scheduler.clone(),
// //             counters,
// //         }.into()
// //     }
// // }


// // impl CommAllocCollectiveBroadcast for LibfabricAsyncAlloc {
// //     fn broadcast<T: Remote>(
// //         &self,
// //         scheduler: &Arc<Scheduler>,
// //         counters: Option<Arc<[Arc<AMCounters>]>>,
// //         src_or_pe: BroadcastInput,
// //         len: usize,
// //     ) -> CollectiveBroadcastOpHandle<T> {
// //         let target = match src_or_pe {
// //             BroadcastInput::Root(index) => RootSrcOrBuffer::Root(index),
// //             BroadcastInput::NotRoot(root_pe) => RootSrcOrBuffer::NotRoot(vec![T::default(); len], root_pe),
// //         };
// //         // let target =
// //         //     if root_pe != self.ofi.my_pe {
// //         //         BroadcastInput::NotRoot(vec![T::default(); self.num_bytes()/std::mem::size_of::<T>()], root_pe)
// //         //     }
// //         //     else {
// //         //         BroadcastInput::Root()
// //         //     };
// //         LibfabricAsyncCollectiveBroadcastFuture {
// //             alloc: self.clone(),
// //             target,
// //             len,
// //             spawned: false,
// //             scheduler: scheduler.clone(),
// //             counters,
// //         }
// //         .into()
// //     }
// //     fn broadcast_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
// //         &self,
// //         scheduler: &Arc<Scheduler>,
// //         counters: Option<Arc<[Arc<AMCounters>]>>,
// //         root_or_buffer: RootSrcOrLamellarBuffer<T, B>,
// //         len: usize,
// //     ) -> CollectiveBroadcastIntoBufferOpHandle<T, B> {
        
// //         LibfabricAsyncCollectiveBroadcastIntoBufferFuture {
// //             alloc: self.clone(),
// //             target: root_or_buffer.into(),
// //             len,
// //             spawned: false,
// //             scheduler: scheduler.clone(),
// //             counters,
// //         }.into()
// //     }
// // }


// // impl CommAllocCollectiveScatter for LibfabricAsyncAlloc {
// //     fn scatter<T: Remote>(
// //         &self,
// //         scheduler: &Arc<Scheduler>,
// //         counters: Option<Arc<[Arc<AMCounters>]>>,
// //         src_or_root_pe: ScatterInput,
// //         len: usize,
// //     ) -> CollectiveScatterOpHandle<T> {
// //         LibfabricAsyncCollectiveScatterFuture {
// //             alloc: self.clone(),
// //             len,
// //             src_or_root_pe: src_or_root_pe.into(),
// //             result: vec![T::default(); len/ self.num_pes()],
// //             spawned: false,
// //             scheduler: scheduler.clone(),
// //             counters,
// //         }
// //         .into()
// //     }
// //     fn scatter_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
// //         &self,
// //         scheduler: &Arc<Scheduler>,
// //         counters: Option<Arc<[Arc<AMCounters>]>>,
// //         result: LamellarBuffer<T, B>,
// //         src_or_root_pe: ScatterInput,
// //         len: usize,
// //     ) -> CollectiveScatterIntoBufferOpHandle<T, B> {
        
// //         LibfabricAsyncCollectiveScatterIntoBufferFuture {
// //             alloc: self.clone(),
// //             len,
// //             result: result,
// //             src_or_root_pe: src_or_root_pe.into(),
// //             spawned: false,
// //             scheduler: scheduler.clone(),
// //             counters,
// //         }.into()
// //     }
// // }

// // impl CommAllocCollectiveReduceScatter for LibfabricAsyncAlloc {
// //     fn reduce_scatter<T: Remote>(
// //         &self,
// //         scheduler: &Arc<Scheduler>,
// //         counters: Option<Arc<[Arc<AMCounters>]>>,
// //         op: ReduceOp,
// //         index: usize, 
// //         len: usize,
// //     ) -> CollectiveReduceScatterOpHandle<T> {
// //         LibfabricAsyncCollectiveReduceScatterFuture {
// //             alloc: self.clone(),
// //             op: op,
// //             index,
// //             len,
// //             result: vec![T::default(); len / self.num_pes()],
// //             spawned: false,
// //             scheduler: scheduler.clone(),
// //             counters,
// //         }
// //         .into()
// //     }
// //     fn reduce_scatter_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
// //         &self,
// //         scheduler: &Arc<Scheduler>,
// //         counters: Option<Arc<[Arc<AMCounters>]>>,
// //         op: ReduceOp,
// //         index: usize,
// //         len: usize,
// //         dst: LamellarBuffer<T, B>,
// //     ) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
// //         LibfabricAsyncCollectiveReduceScatterIntoBufferFuture {
// //             alloc: self.clone(),
// //             op: op,
// //             index,
// //             len,
// //             result: dst,
// //             spawned: false,
// //             scheduler: scheduler.clone(),
// //             counters,
// //         }.into()
// //     }
// //     // fn reduce_scatter_in_place<T: Remote, B: AsLamellarBuffer<T>>(
// //     //     &self,
// //     //     scheduler: &Arc<Scheduler>,
// //     //     counters: Option<Arc<[Arc<AMCounters>]>>,
// //     //     source_and_dst: LamellarBuffer<T, B>,
// //     //     op: ReduceOp,
// //     // ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {

// //     //     LibfabricAsyncCollectiveAllReduceInPlaceFuture {
// //     //         alloc: self.clone(),
// //     //         op: op,
// //     //         result: source_and_dst,
// //     //         spawned: false,
// //     //         scheduler: scheduler.clone(),
// //     //         counters,
// //     //         phantom: std::marker::PhantomData,
// //     //     }.into()
// //     // }
// // }

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveReduceIntoBufferData<T, B> {
    async fn exec_op(mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        LibfabricAsyncAlloc::allreduce_inner(&self.alloc, &self.op, src, self.result.as_mut_slice())
            .await
            .unwrap();
    }

    pub(crate) fn block(self) {
        self.scheduler.clone().block_on(async move { self.exec_op().await })
    }

    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.exec_op().await }, counters)
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricAsyncCollectiveAllReduceIntoBufferFuture<T, B> {
    pub(crate) fn block(self) {
        let fut_data = self.fut_data.take().unwrap();
        fut_data.block()
    }

    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let fut_data = self.fut_data.take().unwrap();
        fut_data.spawn()
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricAsyncCollectiveAllReduceIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveAllReduceIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricAsyncCollectiveAllReduceIntoBufferFuture<T, B>>
    for CollectiveAllReduceIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricAsyncCollectiveAllReduceIntoBufferFuture<T, B>,
    ) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        CollectiveAllReduceIntoBufferOpHandle {
            future: CollectiveAllReduceIntoBufferOpFuture::LibfabricAsync(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricAsyncCollectiveAllReduceIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

struct CollectiveAllReduceInPlaceData<T: Remote, B: AsLamellarBuffer<T>> {
    alloc: LibfabricAsyncAlloc,
    op: ReduceOp,
    result: LamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncCollectiveAllReduceInPlaceFuture<T: Remote, B: AsLamellarBuffer<T>> {
    fut_data: Option<CollectiveAllReduceInPlaceData<T, B>>,
    fut: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveAllReduceInPlaceData<T, B> {
    async fn exec_op(mut self) {
        LibfabricAsyncAlloc::allreduce_inplace_inner::<T>(&self.alloc, &self.op, self.result.as_mut_slice())
            .await
            .unwrap();
    }

    fn block(self) {
        self.scheduler.clone().block_on(async move { self.exec_op().await })
    }

    fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.exec_op().await }, counters)
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricAsyncCollectiveAllReduceInPlaceFuture<T, B> {
    pub(crate) fn block(self) {
        self.fut_data.take().unwrap().block()
    }

    pub(crate) fn spawn(self) -> LamellarTask<()> {
        self.fut_data.take().unwrap().spawn()
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricAsyncCollectiveAllReduceInPlaceFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveAllReduceInPlaceFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricAsyncCollectiveAllReduceInPlaceFuture<T, B>>
    for CollectiveAllReduceInPlaceOpHandle<T, B>
{
    fn from(
        f: LibfabricAsyncCollectiveAllReduceInPlaceFuture<T, B>,
    ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        CollectiveAllReduceInPlaceOpHandle {
            future: CollectiveAllReduceInPlaceOpFuture::LibfabricAsync(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricAsyncCollectiveAllReduceInPlaceFuture<T, B>
{
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

struct CollectiveReduceDataToRoot<T: Remote> {
    alloc: LibfabricAsyncAlloc,
    index: usize,
    len: usize,
    op: ReduceOp,
    target: RootOrBuffer<T>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncCollectiveReduceFuture<T: Remote> {
    fut_data: Option<CollectiveReduceDataToRoot<T>>,
    fut: Option<Pin<Box<dyn Future<Output = Option<Vec<T>>> + Send>>>,
}

impl<T: Remote> CollectiveReduceDataToRoot<T> {
    async fn exec_op(mut self) -> Option<Vec<T>> {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        LibfabricAsyncAlloc::reduce_inner(&self.alloc, &self.op, src, self.target.as_mut_slice())
            .await
            .unwrap();
        match &mut self.target {
            RootOrBuffer::Root(res) => {
                let mut res_vec = Vec::new();
                std::mem::swap(&mut res_vec, res);
                Some(res_vec)
            }
            RootOrBuffer::NotRoot(_) => None,
        }
    }

    fn block(self) -> Option<Vec<T>> {
        self.scheduler
            .clone()
            .block_on(async move { self.exec_op().await })
    }

    fn spawn(self) -> LamellarTask<Option<Vec<T>>> {
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.exec_op().await }, counters)
    }
}

impl<T: Remote> LibfabricAsyncCollectiveReduceFuture<T> {
    pub(crate) fn block(self) -> Option<Vec<T>> {
        self.fut_data.take().unwrap().block()
    }

    pub(crate) fn spawn(self) -> LamellarTask<Option<Vec<T>>> {
        self.fut_data.take().unwrap().spawn()
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricAsyncCollectiveReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveReduceFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricAsyncCollectiveReduceFuture<T>> for CollectiveReduceOpHandle<T> {
    fn from(f: LibfabricAsyncCollectiveReduceFuture<T>) -> CollectiveReduceOpHandle<T> {
        CollectiveReduceOpHandle {
            future: CollectiveReduceOpFuture::LibfabricAsync(f),
        }
    }
}

impl<T: Remote> Future for LibfabricAsyncCollectiveReduceFuture<T> {
    type Output = Option<Vec<T>>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

struct CollectiveReduceIntoBufferDataToRoot<T: Remote, B: AsLamellarBuffer<T>> {
    alloc: LibfabricAsyncAlloc,
    index: usize,
    len: usize,
    op: ReduceOp,
    target: RootOrLamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncCollectiveReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    fut_data: Option<CollectiveReduceIntoBufferDataToRoot<T, B>>,
    fut: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveReduceIntoBufferDataToRoot<T, B> {
    async fn exec_op(mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        LibfabricAsyncAlloc::reduce_inner(&self.alloc, &self.op, src, self.target.as_mut_slice())
            .await
            .unwrap();
    }

    fn block(self) {
        self.scheduler.clone().block_on(async move { self.exec_op().await })
    }

    fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.exec_op().await }, counters)
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricAsyncCollectiveReduceIntoBufferFuture<T, B> {
    pub(crate) fn block(self) {
        self.fut_data.take().unwrap().block()
    }

    pub(crate) fn spawn(self) -> LamellarTask<()> {
        self.fut_data.take().unwrap().spawn()
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricAsyncCollectiveReduceIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveReduceIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricAsyncCollectiveReduceIntoBufferFuture<T, B>>
    for CollectiveReduceIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricAsyncCollectiveReduceIntoBufferFuture<T, B>,
    ) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        CollectiveReduceIntoBufferOpHandle {
            future: CollectiveReduceIntoBufferOpFuture::LibfabricAsync(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricAsyncCollectiveReduceIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}


#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncCollectiveReduceInPlaceFuture<T> {
    pub(crate) alloc: LibfabricAsyncAlloc,
    pub(super) op: ReduceOp,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
    root_pe: Option<usize>,
    phantom: std::marker::PhantomData<T>,
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricAsyncCollectiveReduceInPlaceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveReduceInPlaceFuture").print();
        }
    }
}

impl<T> From<LibfabricAsyncCollectiveReduceInPlaceFuture<T>> for CollectiveReduceInPlaceOpHandle<T> {
    fn from(f: LibfabricAsyncCollectiveReduceInPlaceFuture<T>) -> CollectiveReduceInPlaceOpHandle<T> {
        CollectiveReduceInPlaceOpHandle {
            future: CollectiveReduceInPlaceOpFuture::LibfabricAsync(f),
        }
    }
}

impl<T: Remote> Future for LibfabricAsyncCollectiveReduceInPlaceFuture<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        todo!("fix collective reduce inplace future");
        // if !self.spawned { // TODO: FIX
        //     self.exec_op();
        // }
        // Poll::Ready(())
    }
}

struct CollectiveAllGatherData<T: Remote> {
    alloc: LibfabricAsyncAlloc,
    index: usize,
    len: usize,
    result: Vec<T>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncCollectiveAllGatherFuture<T: Remote> {
    fut_data: Option<CollectiveAllGatherData<T>>,
    fut: Option<Pin<Box<dyn Future<Output = Vec<T>> + Send>>>,
}

impl<T: Remote> CollectiveAllGatherData<T> {
    async fn exec_op(mut self) -> Vec<T> {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        LibfabricAsyncAlloc::allgather_inner(&self.alloc, src, &mut self.result)
            .await
            .unwrap();
        self.result
    }

    fn block(self) -> Vec<T> {
        self.scheduler
            .clone()
            .block_on(async move { self.exec_op().await })
    }

    fn spawn(self) -> LamellarTask<Vec<T>> {
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.exec_op().await }, counters)
    }
}

impl<T: Remote> LibfabricAsyncCollectiveAllGatherFuture<T> {
    pub(crate) fn block(self) -> Vec<T> {
        self.fut_data.take().unwrap().block()
    }

    pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
        self.fut_data.take().unwrap().spawn()
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricAsyncCollectiveAllGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveAllGatherFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricAsyncCollectiveAllGatherFuture<T>> for CollectiveAllGatherOpHandle<T> {
    fn from(f: LibfabricAsyncCollectiveAllGatherFuture<T>) -> CollectiveAllGatherOpHandle<T> {
        CollectiveAllGatherOpHandle {
            future: CollectiveAllGatherOpFuture::LibfabricAsync(f),
        }
    }
}

impl<T: Remote> Future for LibfabricAsyncCollectiveAllGatherFuture<T> {
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

struct CollectiveAllGatherIntoBufferData<T: Remote, B: AsLamellarBuffer<T>> {
    alloc: LibfabricAsyncAlloc,
    index: usize,
    len: usize,
    result: LamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncCollectiveAllGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    fut_data: Option<CollectiveAllGatherIntoBufferData<T, B>>,
    fut: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveAllGatherIntoBufferData<T, B> {
    async fn exec_op(mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        LibfabricAsyncAlloc::allgather_inner(&self.alloc, src, self.result.as_mut_slice())
            .await
            .unwrap();
    }

    fn block(self) {
        self.scheduler.clone().block_on(async move { self.exec_op().await })
    }

    fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.exec_op().await }, counters)
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricAsyncCollectiveAllGatherIntoBufferFuture<T, B> {
    pub(crate) fn block(self) {
        self.fut_data.take().unwrap().block()
    }

    pub(crate) fn spawn(self) -> LamellarTask<()> {
        self.fut_data.take().unwrap().spawn()
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricAsyncCollectiveAllGatherIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveAllGatherIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricAsyncCollectiveAllGatherIntoBufferFuture<T, B>>
    for CollectiveAllGatherIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricAsyncCollectiveAllGatherIntoBufferFuture<T, B>,
    ) -> CollectiveAllGatherIntoBufferOpHandle<T, B> {
        CollectiveAllGatherIntoBufferOpHandle {
            future: CollectiveAllGatherIntoBufferOpFuture::LibfabricAsync(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricAsyncCollectiveAllGatherIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

struct CollectiveGatherData<T: Remote> {
    alloc: LibfabricAsyncAlloc,
    index: usize,
    len: usize,
    target: RootOrBuffer<T>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncCollectiveGatherFuture<T: Remote> {
    fut_data: Option<CollectiveGatherData<T>>,
    fut: Option<Pin<Box<dyn Future<Output = Option<Vec<T>>> + Send>>>,
}

impl<T: Remote> CollectiveGatherData<T> {
    async fn exec_op(mut self) -> Option<Vec<T>> {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        LibfabricAsyncAlloc::gather_inner(&self.alloc, src, self.target.as_mut_slice())
            .await
            .unwrap();
        match &mut self.target {
            RootOrBuffer::Root(res) => {
                let mut res_vec = Vec::new();
                std::mem::swap(&mut res_vec, res);
                Some(res_vec)
            }
            RootOrBuffer::NotRoot(_) => None,
        }
    }

    fn block(self) -> Option<Vec<T>> {
        self.scheduler
            .clone()
            .block_on(async move { self.exec_op().await })
    }

    fn spawn(self) -> LamellarTask<Option<Vec<T>>> {
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.exec_op().await }, counters)
    }
}

impl<T: Remote> LibfabricAsyncCollectiveGatherFuture<T> {
    pub(crate) fn block(self) -> Option<Vec<T>> {
        self.fut_data.take().unwrap().block()
    }

    pub(crate) fn spawn(self) -> LamellarTask<Option<Vec<T>>> {
        self.fut_data.take().unwrap().spawn()
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricAsyncCollectiveGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveGatherFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricAsyncCollectiveGatherFuture<T>> for CollectiveGatherOpHandle<T> {
    fn from(f: LibfabricAsyncCollectiveGatherFuture<T>) -> CollectiveGatherOpHandle<T> {
        CollectiveGatherOpHandle {
            future: CollectiveGatherOpFuture::LibfabricAsync(f),
        }
    }
}

impl<T: Remote> Future for LibfabricAsyncCollectiveGatherFuture<T> {
    type Output = Option<Vec<T>>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

struct CollectiveGatherIntoBufferData<T: Remote, B: AsLamellarBuffer<T>> {
    alloc: LibfabricAsyncAlloc,
    index: usize,
    len: usize,
    target: RootOrLamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncCollectiveGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    fut_data: Option<CollectiveGatherIntoBufferData<T, B>>,
    fut: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveGatherIntoBufferData<T, B> {
    async fn exec_op(mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        LibfabricAsyncAlloc::gather_inner(&self.alloc, src, self.target.as_mut_slice())
            .await
            .unwrap();
    }

    fn block(self) {
        self.scheduler.clone().block_on(async move { self.exec_op().await })
    }

    fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.exec_op().await }, counters)
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricAsyncCollectiveGatherIntoBufferFuture<T, B> {
    pub(crate) fn block(self) {
        self.fut_data.take().unwrap().block()
    }

    pub(crate) fn spawn(self) -> LamellarTask<()> {
        self.fut_data.take().unwrap().spawn()
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricAsyncCollectiveGatherIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveGatherIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricAsyncCollectiveGatherIntoBufferFuture<T, B>>
    for CollectiveGatherIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricAsyncCollectiveGatherIntoBufferFuture<T, B>,
    ) -> CollectiveGatherIntoBufferOpHandle<T, B> {
        CollectiveGatherIntoBufferOpHandle {
            future: CollectiveGatherIntoBufferOpFuture::LibfabricAsync(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricAsyncCollectiveGatherIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

struct CollectiveAllToAllData<T: Remote> {
    alloc: LibfabricAsyncAlloc,
    index: usize,
    len: usize,
    result: Vec<T>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncCollectiveAllToAllFuture<T: Remote> {
    fut_data: Option<CollectiveAllToAllData<T>>,
    fut: Option<Pin<Box<dyn Future<Output = Vec<T>> + Send>>>,
}

impl<T: Remote> CollectiveAllToAllData<T> {
    async fn exec_op(mut self) -> Vec<T> {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        LibfabricAsyncAlloc::alltoall_inner(&self.alloc, src, &mut self.result)
            .await
            .unwrap();
        self.result
    }

    fn block(self) -> Vec<T> {
        self.scheduler
            .clone()
            .block_on(async move { self.exec_op().await })
    }

    fn spawn(self) -> LamellarTask<Vec<T>> {
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.exec_op().await }, counters)
    }
}

impl<T: Remote> LibfabricAsyncCollectiveAllToAllFuture<T> {
    pub(crate) fn block(self) -> Vec<T> {
        self.fut_data.take().unwrap().block()
    }

    pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
        self.fut_data.take().unwrap().spawn()
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricAsyncCollectiveAllToAllFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveAllToAllFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricAsyncCollectiveAllToAllFuture<T>> for CollectiveAllToAllOpHandle<T> {
    fn from(f: LibfabricAsyncCollectiveAllToAllFuture<T>) -> CollectiveAllToAllOpHandle<T> {
        CollectiveAllToAllOpHandle {
            future: CollectiveAllToAllOpFuture::LibfabricAsync(f),
        }
    }
}

impl<T: Remote> Future for LibfabricAsyncCollectiveAllToAllFuture<T> {
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

struct CollectiveAllToAllIntoBufferData<T: Remote, B: AsLamellarBuffer<T>> {
    alloc: LibfabricAsyncAlloc,
    index: usize,
    len: usize,
    result: LamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncCollectiveAllToAllIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    fut_data: Option<CollectiveAllToAllIntoBufferData<T, B>>,
    fut: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveAllToAllIntoBufferData<T, B> {
    async fn exec_op(mut self) {
        let src = unsafe { &self.alloc.as_slice()[self.index..self.index + self.len] };
        LibfabricAsyncAlloc::alltoall_inner(&self.alloc, src, self.result.as_mut_slice())
            .await
            .unwrap();
    }

    fn block(self) {
        self.scheduler.clone().block_on(async move { self.exec_op().await })
    }

    fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.exec_op().await }, counters)
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricAsyncCollectiveAllToAllIntoBufferFuture<T, B> {
    pub(crate) fn block(self) {
        self.fut_data.take().unwrap().block()
    }

    pub(crate) fn spawn(self) -> LamellarTask<()> {
        self.fut_data.take().unwrap().spawn()
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricAsyncCollectiveAllToAllIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveAllToAllIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricAsyncCollectiveAllToAllIntoBufferFuture<T, B>>
    for CollectiveAllToAllIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricAsyncCollectiveAllToAllIntoBufferFuture<T, B>,
    ) -> CollectiveAllToAllIntoBufferOpHandle<T, B> {
        CollectiveAllToAllIntoBufferOpHandle {
            future: CollectiveAllToAllIntoBufferOpFuture::LibfabricAsync(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricAsyncCollectiveAllToAllIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

struct CollectiveBroadcastData<T: Remote> {
    alloc: LibfabricAsyncAlloc,
    target: RootSrcOrBuffer<T>,
    len: usize,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncCollectiveBroadcastFuture<T: Remote> {
    fut_data: Option<CollectiveBroadcastData<T>>,
    fut: Option<Pin<Box<dyn Future<Output = Option<Vec<T>>> + Send>>>,
}

impl<T: Remote> CollectiveBroadcastData<T> {
    async fn exec_op(mut self) -> Option<Vec<T>> {
        let alloc_slice = unsafe { self.alloc.as_mut_slice() };
        LibfabricAsyncAlloc::broadcast_inner(
            &self.alloc,
            self.target.as_mut_slice(alloc_slice, self.len),
        )
        .await
        .unwrap();
        match &mut self.target {
            RootSrcOrBuffer::Root(_) => None,
            RootSrcOrBuffer::NotRoot(items, _) => {
                let mut res = Vec::new();
                std::mem::swap(items, &mut res);
                Some(res)
            }
        }
    }

    fn block(self) -> Option<Vec<T>> {
        self.scheduler
            .clone()
            .block_on(async move { self.exec_op().await })
    }

    fn spawn(self) -> LamellarTask<Option<Vec<T>>> {
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.exec_op().await }, counters)
    }
}

impl<T: Remote> LibfabricAsyncCollectiveBroadcastFuture<T> {
    pub(crate) fn block(self) -> Option<Vec<T>> {
        self.fut_data.take().unwrap().block()
    }

    pub(crate) fn spawn(self) -> LamellarTask<Option<Vec<T>>> {
        self.fut_data.take().unwrap().spawn()
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricAsyncCollectiveBroadcastFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveBroadcastFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricAsyncCollectiveBroadcastFuture<T>> for CollectiveBroadcastOpHandle<T> {
    fn from(f: LibfabricAsyncCollectiveBroadcastFuture<T>) -> CollectiveBroadcastOpHandle<T> {
        CollectiveBroadcastOpHandle {
            future: CollectiveBroadcastOpFuture::LibfabricAsync(f),
        }
    }
}

impl<T: Remote> Future for LibfabricAsyncCollectiveBroadcastFuture<T> {
    type Output = Option<Vec<T>>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

struct CollectiveBroadcastIntoBufferData<T: Remote, B: AsLamellarBuffer<T>> {
    alloc: LibfabricAsyncAlloc,
    target: RootSrcOrLamellarBufferInner<T, B>,
    len: usize,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncCollectiveBroadcastIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    fut_data: Option<CollectiveBroadcastIntoBufferData<T, B>>,
    fut: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveBroadcastIntoBufferData<T, B> {
    async fn exec_op(mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice() };
        LibfabricAsyncAlloc::broadcast_inner(
            &self.alloc,
            self.target.as_mut_slice(alloc_slice, self.len),
        )
        .await
        .unwrap();
    }

    fn block(self) {
        self.scheduler.clone().block_on(async move { self.exec_op().await })
    }

    fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.exec_op().await }, counters)
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricAsyncCollectiveBroadcastIntoBufferFuture<T, B> {
    pub(crate) fn block(self) {
        self.fut_data.take().unwrap().block()
    }

    pub(crate) fn spawn(self) -> LamellarTask<()> {
        self.fut_data.take().unwrap().spawn()
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricAsyncCollectiveBroadcastIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveBroadcastIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricAsyncCollectiveBroadcastIntoBufferFuture<T, B>>
    for CollectiveBroadcastIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricAsyncCollectiveBroadcastIntoBufferFuture<T, B>,
    ) -> CollectiveBroadcastIntoBufferOpHandle<T, B> {
        CollectiveBroadcastIntoBufferOpHandle {
            future: CollectiveBroadcastIntoBufferOpFuture::LibfabricAsync(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricAsyncCollectiveBroadcastIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

struct CollectiveScatterData<T: Remote> {
    alloc: LibfabricAsyncAlloc,
    len: usize,
    result: Vec<T>,
    src_or_root_pe: ScatterInputInner,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncCollectiveScatterFuture<T: Remote> {
    fut_data: Option<CollectiveScatterData<T>>,
    fut: Option<Pin<Box<dyn Future<Output = Vec<T>> + Send>>>,
}

impl<T: Remote> CollectiveScatterData<T> {
    async fn exec_op(mut self) -> Vec<T> {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        LibfabricAsyncAlloc::scatter_inner(
            &self.alloc,
            &mut self.result,
            self.src_or_root_pe.as_slice(alloc_slice, self.len),
        )
        .await
        .unwrap();
        self.result
    }

    fn block(self) -> Vec<T> {
        self.scheduler
            .clone()
            .block_on(async move { self.exec_op().await })
    }

    fn spawn(self) -> LamellarTask<Vec<T>> {
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.exec_op().await }, counters)
    }
}

impl<T: Remote> LibfabricAsyncCollectiveScatterFuture<T> {
    pub(crate) fn block(self) -> Vec<T> {
        self.fut_data.take().unwrap().block()
    }

    pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
        self.fut_data.take().unwrap().spawn()
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricAsyncCollectiveScatterFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveScatterFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricAsyncCollectiveScatterFuture<T>> for CollectiveScatterOpHandle<T> {
    fn from(f: LibfabricAsyncCollectiveScatterFuture<T>) -> CollectiveScatterOpHandle<T> {
        CollectiveScatterOpHandle {
            future: CollectiveScatterOpFuture::LibfabricAsync(f),
        }
    }
}

impl<T: Remote> Future for LibfabricAsyncCollectiveScatterFuture<T> {
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

struct CollectiveScatterIntoBufferData<T: Remote, B: AsLamellarBuffer<T>> {
    alloc: LibfabricAsyncAlloc,
    len: usize,
    src_or_root_pe: ScatterInputInner,
    result: LamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncCollectiveScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    fut_data: Option<CollectiveScatterIntoBufferData<T, B>>,
    fut: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveScatterIntoBufferData<T, B> {
    async fn exec_op(mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        LibfabricAsyncAlloc::scatter_inner(
            &self.alloc,
            self.result.as_mut_slice(),
            self.src_or_root_pe.as_slice(alloc_slice, self.len),
        )
        .await
        .unwrap();
    }

    fn block(self) {
        self.scheduler.clone().block_on(async move { self.exec_op().await })
    }

    fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.exec_op().await }, counters)
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricAsyncCollectiveScatterIntoBufferFuture<T, B> {
    pub(crate) fn block(self) {
        self.fut_data.take().unwrap().block()
    }

    pub(crate) fn spawn(self) -> LamellarTask<()> {
        self.fut_data.take().unwrap().spawn()
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricAsyncCollectiveScatterIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveScatterIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricAsyncCollectiveScatterIntoBufferFuture<T, B>>
    for CollectiveScatterIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricAsyncCollectiveScatterIntoBufferFuture<T, B>,
    ) -> CollectiveScatterIntoBufferOpHandle<T, B> {
        CollectiveScatterIntoBufferOpHandle {
            future: CollectiveScatterIntoBufferOpFuture::LibfabricAsync(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricAsyncCollectiveScatterIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

struct CollectiveReduceScatterData<T: Remote> {
    alloc: LibfabricAsyncAlloc,
    op: ReduceOp,
    index: usize,
    len: usize,
    result: Vec<T>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncCollectiveReduceScatterFuture<T: Remote> {
    fut_data: Option<CollectiveReduceScatterData<T>>,
    fut: Option<Pin<Box<dyn Future<Output = Vec<T>> + Send>>>,
}

impl<T: Remote> CollectiveReduceScatterData<T> {
    async fn exec_op(mut self) -> Vec<T> {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        let src = &alloc_slice[self.index..self.index + self.len];
        LibfabricAsyncAlloc::reduce_scatter_inner(&self.alloc, &self.op, src, &mut self.result)
            .await
            .unwrap();
        self.result
    }

    fn block(self) -> Vec<T> {
        self.scheduler
            .clone()
            .block_on(async move { self.exec_op().await })
    }

    fn spawn(self) -> LamellarTask<Vec<T>> {
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.exec_op().await }, counters)
    }
}

impl<T: Remote> LibfabricAsyncCollectiveReduceScatterFuture<T> {
    pub(crate) fn block(self) -> Vec<T> {
        self.fut_data.take().unwrap().block()
    }

    pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
        self.fut_data.take().unwrap().spawn()
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricAsyncCollectiveReduceScatterFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveReduceScatterFuture").print();
        }
    }
}

impl<T: Remote> From<LibfabricAsyncCollectiveReduceScatterFuture<T>>
    for CollectiveReduceScatterOpHandle<T>
{
    fn from(f: LibfabricAsyncCollectiveReduceScatterFuture<T>) -> CollectiveReduceScatterOpHandle<T> {
        CollectiveReduceScatterOpHandle {
            future: CollectiveReduceScatterOpFuture::LibfabricAsync(f),
        }
    }
}

impl<T: Remote> Future for LibfabricAsyncCollectiveReduceScatterFuture<T> {
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

struct CollectiveReduceScatterIntoBufferData<T: Remote, B: AsLamellarBuffer<T>> {
    alloc: LibfabricAsyncAlloc,
    op: ReduceOp,
    index: usize,
    len: usize,
    result: LamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncCollectiveReduceScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    fut_data: Option<CollectiveReduceScatterIntoBufferData<T, B>>,
    fut: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> CollectiveReduceScatterIntoBufferData<T, B> {
    async fn exec_op(mut self) {
        let alloc_slice = unsafe { self.alloc.as_slice() };
        let src = &alloc_slice[self.index..self.index + self.len];
        LibfabricAsyncAlloc::reduce_scatter_inner(
            &self.alloc,
            &self.op,
            src,
            self.result.as_mut_slice(),
        )
        .await
        .unwrap();
    }

    fn block(self) {
        self.scheduler.clone().block_on(async move { self.exec_op().await })
    }

    fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.exec_op().await }, counters)
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricAsyncCollectiveReduceScatterIntoBufferFuture<T, B> {
    pub(crate) fn block(self) {
        self.fut_data.take().unwrap().block()
    }

    pub(crate) fn spawn(self) -> LamellarTask<()> {
        self.fut_data.take().unwrap().spawn()
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for LibfabricAsyncCollectiveReduceScatterIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncCollectiveReduceScatterIntoBufferFuture")
                .print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>>
    From<LibfabricAsyncCollectiveReduceScatterIntoBufferFuture<T, B>>
    for CollectiveReduceScatterIntoBufferOpHandle<T, B>
{
    fn from(
        f: LibfabricAsyncCollectiveReduceScatterIntoBufferFuture<T, B>,
    ) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        CollectiveReduceScatterIntoBufferOpHandle {
            future: CollectiveReduceScatterIntoBufferOpFuture::LibfabricAsync(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for LibfabricAsyncCollectiveReduceScatterIntoBufferFuture<T, B>
{
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

impl CommAllocCollectiveAllReduce for LibfabricAsyncAlloc {
    fn reduce_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        op: ReduceOp,
    ) -> CollectiveAllReduceOpHandle<T> {
        LibfabricAsyncCollectiveAllReduceFuture {
            fut_data: Some(CollectiveReduceData {
                alloc: self.clone(),
                op,
                index,
                len,
                result: vec![T::default(); len],
                spawned: false,
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
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
        LibfabricAsyncCollectiveAllReduceIntoBufferFuture {
            fut_data: Some(CollectiveReduceIntoBufferData {
                alloc: self.clone(),
                op,
                index,
                len,
                result: dst,
                spawned: false,
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
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
        LibfabricAsyncCollectiveAllReduceInPlaceFuture {
            fut_data: Some(CollectiveAllReduceInPlaceData {
                alloc: self.clone(),
                op,
                result: source_and_dst,
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
        }
        .into()
    }
}

impl CommAllocCollectiveReduce for LibfabricAsyncAlloc {
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
            RootOrBuffer::Root(vec![T::default(); len])
        };
        LibfabricAsyncCollectiveReduceFuture {
            fut_data: Some(CollectiveReduceDataToRoot {
                alloc: self.clone(),
                index,
                len,
                op,
                target,
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
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
        LibfabricAsyncCollectiveReduceIntoBufferFuture {
            fut_data: Some(CollectiveReduceIntoBufferDataToRoot {
                alloc: self.clone(),
                index,
                len,
                op,
                target: root_or_buffer,
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
        }
        .into()
    }
}

impl CommAllocCollectiveAllGather for LibfabricAsyncAlloc {
    fn gather_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllGatherOpHandle<T> {
        LibfabricAsyncCollectiveAllGatherFuture {
            fut_data: Some(CollectiveAllGatherData {
                alloc: self.clone(),
                index,
                len,
                result: vec![T::default(); len * self.num_pes()],
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
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
        LibfabricAsyncCollectiveAllGatherIntoBufferFuture {
            fut_data: Some(CollectiveAllGatherIntoBufferData {
                alloc: self.clone(),
                index,
                len,
                result: dst,
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
        }
        .into()
    }
}

impl CommAllocCollectiveGather for LibfabricAsyncAlloc {
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
            RootOrBuffer::Root(vec![T::default(); len * self.num_pes()])
        };
        LibfabricAsyncCollectiveGatherFuture {
            fut_data: Some(CollectiveGatherData {
                alloc: self.clone(),
                index,
                len,
                target,
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
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
        LibfabricAsyncCollectiveGatherIntoBufferFuture {
            fut_data: Some(CollectiveGatherIntoBufferData {
                alloc: self.clone(),
                index,
                len,
                target: root_or_buffer,
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
        }
        .into()
    }
}

impl CommAllocCollectiveAllToAll for LibfabricAsyncAlloc {
    fn alltoall<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllToAllOpHandle<T> {
        LibfabricAsyncCollectiveAllToAllFuture {
            fut_data: Some(CollectiveAllToAllData {
                alloc: self.clone(),
                index,
                len,
                result: vec![T::default(); len * self.num_pes()],
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
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
        LibfabricAsyncCollectiveAllToAllIntoBufferFuture {
            fut_data: Some(CollectiveAllToAllIntoBufferData {
                alloc: self.clone(),
                index,
                len,
                result: dst,
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
        }
        .into()
    }
}

impl CommAllocCollectiveBroadcast for LibfabricAsyncAlloc {
    fn broadcast<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src_or_pe: BroadcastInput,
        len: usize,
    ) -> CollectiveBroadcastOpHandle<T> {
        let target = match src_or_pe {
            BroadcastInput::Root(index) => RootSrcOrBuffer::Root(index),
            BroadcastInput::NotRoot(root_pe) => {
                RootSrcOrBuffer::NotRoot(vec![T::default(); len], root_pe)
            }
        };
        LibfabricAsyncCollectiveBroadcastFuture {
            fut_data: Some(CollectiveBroadcastData {
                alloc: self.clone(),
                target,
                len,
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
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
        LibfabricAsyncCollectiveBroadcastIntoBufferFuture {
            fut_data: Some(CollectiveBroadcastIntoBufferData {
                alloc: self.clone(),
                target: root_or_buffer.into(),
                len,
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
        }
        .into()
    }
}

impl CommAllocCollectiveScatter for LibfabricAsyncAlloc {
    fn scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterOpHandle<T> {
        LibfabricAsyncCollectiveScatterFuture {
            fut_data: Some(CollectiveScatterData {
                alloc: self.clone(),
                len,
                src_or_root_pe: src_or_root_pe.into(),
                result: vec![T::default(); len],
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
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
        LibfabricAsyncCollectiveScatterIntoBufferFuture {
            fut_data: Some(CollectiveScatterIntoBufferData {
                alloc: self.clone(),
                len,
                src_or_root_pe: src_or_root_pe.into(),
                result,
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
        }
        .into()
    }
}

impl CommAllocCollectiveReduceScatter for LibfabricAsyncAlloc {
    fn reduce_scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: ReduceOp,
        index: usize,
        len: usize,
    ) -> CollectiveReduceScatterOpHandle<T> {
        LibfabricAsyncCollectiveReduceScatterFuture {
            fut_data: Some(CollectiveReduceScatterData {
                alloc: self.clone(),
                op,
                index,
                len,
                result: vec![T::default(); len / self.num_pes()],
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
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
        LibfabricAsyncCollectiveReduceScatterIntoBufferFuture {
            fut_data: Some(CollectiveReduceScatterIntoBufferData {
                alloc: self.clone(),
                op,
                index,
                len,
                result: dst,
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
        }
        .into()
    }
}

// use std::{future::Future, ops::Add, pin::Pin, sync::Arc, task::{Context, Poll}};

// use pin_project::{pin_project, pinned_drop};

// use crate::{active_messaging::AMCounters, array::iterator::distributed_iterator::reduce, lamellae::{collective::{CollectiveAllBroadcastIntoBufferOpFuture, CollectiveAllBroadcastIntoBufferOpHandle, CollectiveAllBroadcastOpFuture, CollectiveAllBroadcastOpHandle, CollectiveAllGatherIntoBufferOpFuture, CollectiveAllGatherIntoBufferOpHandle, CollectiveAllGatherOpFuture, CollectiveAllGatherOpHandle, CollectiveAllReduceInPlaceOpFuture, CollectiveAllReduceInPlaceOpHandle, CollectiveAllReduceIntoBufferOpFuture, CollectiveAllReduceIntoBufferOpHandle, CollectiveAllReduceOpFuture, CollectiveAllReduceOpHandle, CollectiveBroadcastIntoBufferOpFuture, CollectiveBroadcastIntoBufferOpHandle, CollectiveBroadcastOpFuture, CollectiveBroadcastOpHandle, CollectiveGatherIntoBufferOpFuture, CollectiveGatherIntoBufferOpHandle, CollectiveGatherOpFuture, CollectiveGatherOpHandle, CollectiveReduceIntoBufferOpFuture, CollectiveReduceIntoBufferOpHandle, CollectiveReduceOpFuture, CollectiveReduceOpHandle, CollectiveReduceScatterIntoBufferOpFuture, CollectiveReduceScatterIntoBufferOpHandle, CollectiveReduceScatterOpFuture, CollectiveReduceScatterOpHandle, CollectiveScatterIntoBufferOpFuture, CollectiveScatterIntoBufferOpHandle, CollectiveScatterOpFuture, CollectiveScatterOpHandle, CommAllocCollectiveAllGather, CommAllocCollectiveAllReduce, CommAllocCollectiveBroadcast, CommAllocCollectiveGather, CommAllocCollectiveReduce, CommAllocCollectiveReduceScatter, CommAllocCollectiveScatter, ReduceOp, RootOrBuffer, RootOrLamellarBuffer, RootSrcOrBuffer, RootSrcOrLamellarBuffer, RootSrcOrLamellarBufferInner, ScatterInputInner}, net_atomic_op, shmem_lamellae::fabric::ShmemAlloc, AtomicOp, CommAllocAddr}, memregion::MemregionRdmaInputInner, scheduler::Scheduler, warnings::RuntimeWarning, AsLamellarBuffer, BroadcastInput, LamellarBuffer, LamellarTask, Remote, ScatterInput};

// fn reduce_to_root<T: Remote>(root: usize, alloc: &ShmemAlloc, result: Option<&mut [T]>, index: usize, len: usize, op: &ReduceOp) -> Option<usize>{
//     let alloc_slice = unsafe { alloc.as_mut_slice() };
//     let my_slice = &mut alloc_slice[index..index + len]; 
//     let my_ptr = my_slice.as_mut_ptr() as *mut T;

//     if alloc.my_alloc_pe == root {
//         alloc.coll_set_index(root, index);
//         alloc.coll_inc_barrier();
        
//         while alloc.coll_get_barrier() < alloc.num_pes() {
//             std::thread::yield_now();
//         }
        
//         if let Some(result) = result {
//             result.copy_from_slice(my_slice);
//         }
//         None
//     }
//     else {
//         while alloc.coll_get_barrier() == 0 {
//             std::thread::yield_now();
//         }

//         let remote_dst_address = alloc.pe_base_offset(root) + alloc.coll_index(root) * std::mem::size_of::<T>();
//         let mut remote_dest = CommAllocAddr(remote_dst_address);
//         for s in my_slice {
//             let op = reduce_to_atomic_op::<T>(*s, op);
//             net_atomic_op(&op, &remote_dest);
//             remote_dest = remote_dest.add(std::mem::size_of::<T>());
//         }
//         alloc.coll_inc_barrier();

//         Some(remote_dst_address)
//     }
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct ShmemCollectiveAllReduceFuture<T: Remote> {
//     pub(crate) alloc: ShmemAlloc,
//     pub(super) op: ReduceOp,
//     pub(crate) index: usize,
//     pub(crate) len: usize,
//     pub(crate) result: Vec<T>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// fn reduce_to_atomic_op<T>(val: T, op: &ReduceOp) -> AtomicOp<T> {
//     match op {
//         ReduceOp::Sum => AtomicOp::Sum(val),
//         ReduceOp::Prod => AtomicOp::Prod(val),
//         ReduceOp::BitOr => AtomicOp::BitOr(val),
//         ReduceOp::BitXor => AtomicOp::BitXor(val),
//         ReduceOp::BitAnd => AtomicOp::BitAnd(val),
//         _ => unimplemented!("unsupported reduce op for shmem collective allreduce"),
//     }
// }

// impl<T: Remote> ShmemCollectiveAllReduceFuture<T> {
//     fn exec_op(&mut self) {
//         let alloc_slice = unsafe { self.alloc.as_mut_slice() };
//         let my_slice = &mut alloc_slice[self.index..self.index + self.len]; 
//         let mut to_replace: Vec<T> = vec![T::default(); my_slice.len()];
//         to_replace.copy_from_slice(my_slice);
//         let remote_dst_address = reduce_to_root(0, &self.alloc, Some(&mut self.result[..]), self.index, self.len, &self.op);
//         if self.alloc.my_alloc_pe == 0 {
//             self.alloc.coll_inc_barrier();
            
//             while self.alloc.coll_get_barrier() != 2 * self.alloc.num_pes() {
//                 std::thread::yield_now();
//             }

//             self.alloc.coll_reset_barrier();
//             my_slice.copy_from_slice(&to_replace);
//         }
//         else {
//             while self.alloc.coll_get_barrier() < self.alloc.num_pes() {
//                 std::thread::yield_now();
//             }

//             let remote_dst_slice = unsafe {std::slice::from_raw_parts(remote_dst_address.unwrap() as *const T, self.len)};

//             self.result.copy_from_slice(remote_dst_slice);
//             self.alloc.coll_inc_barrier();
//             while self.alloc.coll_get_barrier() >= self.alloc.num_pes() {
//                 std::thread::yield_now();
//             }
//         }
//         self.spawned = true;
//     }

//     pub(crate) fn block(self) -> Vec<T> {
//         self.exec_op();
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
// impl<T: Remote> PinnedDrop for ShmemCollectiveAllReduceFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a ShmemCollectiveAllReduceFuture").print();
//         }
//     }
// }

// impl<T: Remote> From<ShmemCollectiveAllReduceFuture<T>> for CollectiveAllReduceOpHandle<T> {
//     fn from(f: ShmemCollectiveAllReduceFuture<T>) -> CollectiveAllReduceOpHandle<T> {
//         CollectiveAllReduceOpHandle {
//             future: CollectiveAllReduceOpFuture::Shmem(f),
//         }
//     }
// }

// impl<T: Remote> Future for ShmemCollectiveAllReduceFuture<T> {
//     type Output = Vec<T>;

//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         let mut res = Vec::new();
//         std::mem::swap(&mut self.result, &mut res);
//         Poll::Ready(res)
//     }
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct ShmemCollectiveAllReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
//     pub(crate) alloc: ShmemAlloc,
//     pub(super) op: ReduceOp,
//     pub(crate) index: usize,
//     pub(crate) len: usize,
//     pub(crate) result: LamellarBuffer<T, B>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> ShmemCollectiveAllReduceIntoBufferFuture<T, B> {
//     fn exec_op(&mut self) {
//         let alloc_slice = unsafe { self.alloc.as_mut_slice() };
//         let my_slice = &mut alloc_slice[self.index..self.index + self.len]; 
//         let mut to_replace: Vec<T> = vec![T::default(); my_slice.len()];
//         to_replace.copy_from_slice(my_slice);
//         let remote_dst_address = reduce_to_root(0, &self.alloc, Some(&mut self.result.as_mut_slice()), self.index, self.len, &self.op);
//         if self.alloc.my_alloc_pe == 0 {
//             self.alloc.coll_inc_barrier();
            
//             while self.alloc.coll_get_barrier() != 2 * self.alloc.num_pes() {
//                 std::thread::yield_now();
//             }

//             self.alloc.coll_reset_barrier();
//             my_slice.copy_from_slice(&to_replace);
//         }
//         else {
//             while self.alloc.coll_get_barrier() < self.alloc.num_pes() {
//                 std::thread::yield_now();
//             }

//             let remote_dst_slice = unsafe {std::slice::from_raw_parts(remote_dst_address.unwrap() as *const T, self.len)};

//             self.result.as_mut_slice().copy_from_slice(remote_dst_slice);
//             self.alloc.coll_inc_barrier();
//             while self.alloc.coll_get_barrier() >= self.alloc.num_pes() {
//                 std::thread::yield_now();
//             }
//         }
//         self.spawned = true;
//     }

//     pub(crate) fn block(self)  {
//         self.exec_op();
//     }

//     pub(crate) fn spawn(self) -> LamellarTask<()> {
//         self.exec_op();

//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.clone().spawn_task(self, counters)
//     }
// }

// #[pinned_drop]
// impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for ShmemCollectiveAllReduceIntoBufferFuture<T, B> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a ShmemCollectiveAllReduceIntoBufferFuture").print();
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> From<ShmemCollectiveAllReduceIntoBufferFuture<T, B>> for CollectiveAllReduceIntoBufferOpHandle<T, B> {
//     fn from(f: ShmemCollectiveAllReduceIntoBufferFuture<T, B>) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
//         CollectiveAllReduceIntoBufferOpHandle {
//             future: CollectiveAllReduceIntoBufferOpFuture::Shmem(f),
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> Future for ShmemCollectiveAllReduceIntoBufferFuture<T, B> {
//     type Output = ();

//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         Poll::Ready(())
//     }
// }


// #[pin_project(PinnedDrop)]
// pub(crate) struct ShmemCollectiveAllReduceInPlaceFuture<T: Remote, B: AsLamellarBuffer<T>> {
//     pub(crate) alloc: ShmemAlloc,
//     pub(super) op: ReduceOp,
//     pub(crate) result: LamellarBuffer<T, B>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
//     phantom: std::marker::PhantomData<T>,
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> ShmemCollectiveAllReduceInPlaceFuture<T, B> {
//     fn exec_op(&mut self) {
//         unimplemented!("shmem collective allreduce in place is not yet implemented");
//         // let alloc_slice = unsafe { self.alloc.as_mut_slice() };
//         // let my_slice = &mut alloc_slice[self.index..self.index + self.len]; 
//         // let mut to_replace: Vec<T> = vec![T::default(); my_slice.len()];
//         // to_replace.copy_from_slice(my_slice);
//         // let remote_dst_address = reduce_to_root(0, &self.alloc, Some(&mut self.result.as_mut_slice()), self.index, self.len, &self.op);
//         // if self.alloc.my_alloc_pe == 0 {
//         //     self.alloc.coll_inc_barrier();
            
//         //     while self.alloc.coll_get_barrier() != 2 * self.alloc.num_pes() {
//         //         std::thread::yield_now();
//         //     }
//         //     println!("PE 0 passed 2nd barrier");
//         //     println!("PE 0 barrier: {}", self.alloc.coll_get_barrier());

//         //     self.alloc.coll_reset_barrier();
//         //     my_slice.copy_from_slice(&to_replace);
//         // }
//         // else {
//         //     while self.alloc.coll_get_barrier() < self.alloc.num_pes() {
//         //         std::thread::yield_now();
//         //     }

//         //     let remote_dst_slice = unsafe {std::slice::from_raw_parts(remote_dst_address.unwrap() as *const T, self.len)};

//         //     self.result.as_mut_slice().copy_from_slice(remote_dst_slice);
//         //     self.alloc.coll_inc_barrier();
//         //     while self.alloc.coll_get_barrier() >=  {
//         //         std::thread::yield_now();
//         //     }
//         // }
//         // self.spawned = true;
//     }

//     pub(crate) fn block(self)  {
//         self.exec_op();
//     }

//     pub(crate) fn spawn(self) -> LamellarTask<()> {
//         self.exec_op();

//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.clone().spawn_task(self, counters)
//     }
// }

// #[pinned_drop]
// impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for ShmemCollectiveAllReduceInPlaceFuture<T, B> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a ShmemCollectiveAllReduceInPlaceFuture").print();
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> From<ShmemCollectiveAllReduceInPlaceFuture<T, B>> for CollectiveAllReduceInPlaceOpHandle<T, B> {
//     fn from(f: ShmemCollectiveAllReduceInPlaceFuture<T, B>) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
//         CollectiveAllReduceInPlaceOpHandle {
//             future: CollectiveAllReduceInPlaceOpFuture::Shmem(f),
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> Future for ShmemCollectiveAllReduceInPlaceFuture<T, B> {
//     type Output = ();

//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         Poll::Ready(())
//     }
// }


// #[pin_project(PinnedDrop)]
// pub(crate) struct ShmemCollectiveReduceFuture<T: Remote> {
//     pub(crate) alloc: ShmemAlloc,
//     pub(crate) index: usize,
//     pub(crate) len: usize,
//     pub(super) op: ReduceOp,
//     pub(crate) target: RootOrBuffer<T>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Remote> ShmemCollectiveReduceFuture<T> {
//     fn exec_op(&mut self) {
//         let alloc_slice = unsafe { self.alloc.as_mut_slice() };
//         let my_slice = &mut alloc_slice[self.index..self.index + self.len]; 
//         let mut to_replace: Vec<T> = vec![T::default(); my_slice.len()];
//         to_replace.copy_from_slice(my_slice);
//         match &mut self.target {
//             RootOrBuffer::Root(result) => {
//                 let remote_dst_address = reduce_to_root(self.alloc.my_alloc_pe, &self.alloc, Some(&mut result[..]), self.index, self.len, &self.op);
//                 self.alloc.coll_inc_barrier();
//                 while self.alloc.coll_get_barrier() < 2 * self.alloc.num_pes() {
//                     std::thread::yield_now();
//                 }
//                 self.alloc.coll_reset_barrier();
//                 my_slice.copy_from_slice(&to_replace);
//             },
//             RootOrBuffer::NotRoot(root) => {
//                 let remote_dst_address = reduce_to_root::<T>(*root, &self.alloc, None, self.index, self.len, &self.op);
//                 // self.alloc.coll_inc_barrier();
//                 while self.alloc.coll_get_barrier() < self.alloc.num_pes() {
//                     std::thread::yield_now();
//                 }
//                 self.alloc.coll_inc_barrier();

//                 while self.alloc.coll_get_barrier() >= self.alloc.num_pes() {
//                     std::thread::yield_now();
//                 }
//             },
//         }
//         self.spawned = true;
//     }

//     pub(crate) fn block(self) -> Option<Vec<T>> {
//         self.exec_op();
//         match &mut self.target {
//             RootOrBuffer::Root(r) => {
//                 let mut res = Vec::new();
//                 std::mem::swap(&mut res, r);
//                 Some(res)
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
// impl<T: Remote> PinnedDrop for ShmemCollectiveReduceFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a ShmemCollectiveReduceFuture").print();
//         }
//     }
// }

// impl<T: Remote> From<ShmemCollectiveReduceFuture<T>> for CollectiveReduceOpHandle<T> {
//     fn from(f: ShmemCollectiveReduceFuture<T>) -> CollectiveReduceOpHandle<T> {
//         CollectiveReduceOpHandle {
//             future: CollectiveReduceOpFuture::Shmem(f),
//         }
//     }
// }

// impl<T: Remote> Future for ShmemCollectiveReduceFuture<T> {
//     type Output = Option<Vec<T>>;

//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         match &mut self.target {
//             RootOrBuffer::Root(r) => {
//                 let mut res = Vec::new();
//                 std::mem::swap(&mut res, r);
//                 Poll::Ready(Some(res))
//             },
//             RootOrBuffer::NotRoot(_) => Poll::Ready(None),
//         }
//     }
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct ShmemCollectiveReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
//     pub(crate) alloc: ShmemAlloc,
//     pub(crate) index: usize,
//     pub(crate) len: usize,
//     pub(super) op: ReduceOp,
//     pub(crate) target: RootOrLamellarBuffer<T, B>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> ShmemCollectiveReduceIntoBufferFuture<T, B> {
//     fn exec_op(&mut self) {
//         let alloc_slice = unsafe { self.alloc.as_mut_slice() };
//         let my_slice = &mut alloc_slice[self.index..self.index + self.len]; 
//         let mut to_replace: Vec<T> = vec![T::default(); my_slice.len()];
//         to_replace.copy_from_slice(my_slice);
//         match &mut self.target {
//             RootOrLamellarBuffer::Root(result) => {
//                 let remote_dst_address = reduce_to_root(self.alloc.my_alloc_pe, &self.alloc, Some(result.as_mut_slice()), self.index, self.len, &self.op);
//                 // self.alloc.coll_inc_barrier();
//                 while self.alloc.coll_get_barrier() < 2 * self.alloc.num_pes() {
//                     std::thread::yield_now();
//                 }
//                 self.alloc.coll_reset_barrier();
//                 my_slice.copy_from_slice(&to_replace);
//             },
//             RootOrLamellarBuffer::NotRoot(root) => {
//                 let remote_dst_address = reduce_to_root::<T>(*root, &self.alloc, None, self.index, self.len, &self.op);
//                 // self.alloc.coll_inc_barrier();
//                 while self.alloc.coll_get_barrier() < self.alloc.num_pes() {
//                     std::thread::yield_now();
//                 }
//                 self.alloc.coll_inc_barrier();

//                 while self.alloc.coll_get_barrier() >= self.alloc.num_pes() {
//                     std::thread::yield_now();
//                 }
//             },
//         }
//         self.spawned = true;
//     }

//     pub(crate) fn block(self)  {
//         self.exec_op();
//     }

//     pub(crate) fn spawn(self) -> LamellarTask<()> {
//         self.exec_op();

//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.clone().spawn_task(self, counters)
//     }
// }

// #[pinned_drop]
// impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for ShmemCollectiveReduceIntoBufferFuture<T, B> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a ShmemCollectiveReduceIntoBufferFuture").print();
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> From<ShmemCollectiveReduceIntoBufferFuture<T, B>> for CollectiveReduceIntoBufferOpHandle<T, B> {
//     fn from(f: ShmemCollectiveReduceIntoBufferFuture<T, B>) -> CollectiveReduceIntoBufferOpHandle<T, B> {
//         CollectiveReduceIntoBufferOpHandle {
//             future: CollectiveReduceIntoBufferOpFuture::Shmem(f),
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> Future for ShmemCollectiveReduceIntoBufferFuture<T, B> {
//     type Output = ();

//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         Poll::Ready(())
//     }
// }
// fn gather<T: Remote>(root: usize, alloc: &ShmemAlloc, result: Option<&mut [T]>, index: usize, len: usize) {
//     let alloc_slice = unsafe { alloc.as_mut_slice() };
//     let my_slice = &mut alloc_slice[index..index + len]; 
//     let my_ptr = my_slice.as_mut_ptr() as *mut T;
//     alloc.coll_set_index(alloc.my_alloc_pe, index);
//     alloc.coll_inc_barrier();
//     while alloc.coll_get_barrier() < alloc.num_pes() {
//         std::thread::yield_now();
//     }

//     if let Some(result) = result {
//         for pe in 0..alloc.num_pes() {
//             let remote_src_address = alloc.pe_base_offset(pe) + alloc.coll_index(pe) * std::mem::size_of::<T>();
//             let remote_src_slice = unsafe {std::slice::from_raw_parts(remote_src_address as *const T, len)};
//             result[(pe * len)..((pe + 1) * len)].copy_from_slice(remote_src_slice);
//         }
//     }

//     alloc.coll_inc_barrier();
    
//     if alloc.my_alloc_pe == root {
//         while alloc.coll_get_barrier() < 2 * alloc.num_pes() {
//             std::thread::yield_now();
//         }
//         alloc.coll_reset_barrier();
//     }
//     else {
//         while alloc.coll_get_barrier() >= alloc.num_pes() {
//             std::thread::yield_now();
//         }
//     }
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct ShmemCollectiveAllGatherFuture<T: Remote> {
//     pub(crate) alloc: ShmemAlloc,
//     pub(crate) index: usize,
//     pub(crate) len: usize,
//     pub(crate) result: Vec<T>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Remote> ShmemCollectiveAllGatherFuture<T> {
//     fn exec_op(&mut self) {
//         gather(0, &self.alloc, Some(&mut self.result), self.index, self.len);
//         self.spawned = true;
//     }

//     pub(crate) fn block(self) -> Vec<T> {
//         self.exec_op();
//         let mut res = Vec::new();
//         std::mem::swap(&mut res, &mut self.result);

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
// impl<T: Remote> PinnedDrop for ShmemCollectiveAllGatherFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a ShmemCollectiveAllGatherFuture").print();
//         }
//     }
// }

// impl<T: Remote> From<ShmemCollectiveAllGatherFuture<T>> for CollectiveAllGatherOpHandle<T> {
//     fn from(f: ShmemCollectiveAllGatherFuture<T>) -> CollectiveAllGatherOpHandle<T> {
//         CollectiveAllGatherOpHandle {
//             future: CollectiveAllGatherOpFuture::Shmem(f),
//         }
//     }
// }

// impl<T: Remote> Future for ShmemCollectiveAllGatherFuture<T> {
//     type Output = Vec<T>;

//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         let mut res = Vec::new();
//         std::mem::swap(&mut res, &mut self.result);
//         Poll::Ready(res)
//     }
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct ShmemCollectiveAllGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
//     pub(crate) alloc: ShmemAlloc,
//     pub(crate) index: usize,
//     pub(crate) len: usize,
//     pub(crate) result: LamellarBuffer<T, B>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> ShmemCollectiveAllGatherIntoBufferFuture<T, B> {
//     fn exec_op(&mut self) {
//         gather(0, &self.alloc, Some(self.result.as_mut_slice()), self.index, self.len);
//         self.spawned = true;

//     }

//     pub(crate) fn block(self)  {
//         self.exec_op();
//     }

//     pub(crate) fn spawn(self) -> LamellarTask<()> {
//         self.exec_op();

//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.clone().spawn_task(self, counters)
//     }
// }

// #[pinned_drop]
// impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for ShmemCollectiveAllGatherIntoBufferFuture<T, B> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a ShmemCollectiveAllGatherIntoBufferFuture").print();
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> From<ShmemCollectiveAllGatherIntoBufferFuture<T, B>> for CollectiveAllGatherIntoBufferOpHandle<T, B> {
//     fn from(f: ShmemCollectiveAllGatherIntoBufferFuture<T, B>) -> CollectiveAllGatherIntoBufferOpHandle<T, B> {
//         CollectiveAllGatherIntoBufferOpHandle {
//             future: CollectiveAllGatherIntoBufferOpFuture::Shmem(f),
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> Future for ShmemCollectiveAllGatherIntoBufferFuture<T, B> {
//     type Output = ();

//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         Poll::Ready(())
//     }
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct ShmemCollectiveGatherFuture<T: Remote> {
//     pub(crate) alloc: ShmemAlloc,
//     pub(crate) index: usize,
//     pub(crate) len: usize,
//     pub(crate) target: RootOrBuffer<T>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Remote> ShmemCollectiveGatherFuture<T> {
//     fn exec_op(&mut self) {
//         match &mut self.target {
//             RootOrBuffer::Root(result) => {
//                 gather(self.alloc.my_alloc_pe, &self.alloc, Some(result.as_mut_slice()), self.index, self.len);
//             },
//             RootOrBuffer::NotRoot(root) => {
//                 gather::<T>(*root, &self.alloc, None, self.index, self.len);
//             },
//         }
//         self.spawned = true;
//     }

//     pub(crate) fn block(self) -> Option<Vec<T>> {
//         self.exec_op();
//         match &mut self.target {
//             RootOrBuffer::Root(r) => {
//                 let mut res = Vec::new();
//                 std::mem::swap(&mut res, r);
//                 Some(res)
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
// impl<T: Remote> PinnedDrop for ShmemCollectiveGatherFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a ShmemCollectiveGatherFuture").print();
//         }
//     }
// }

// impl<T: Remote> From<ShmemCollectiveGatherFuture<T>> for CollectiveGatherOpHandle<T> {
//     fn from(f: ShmemCollectiveGatherFuture<T>) -> CollectiveGatherOpHandle<T> {
//         CollectiveGatherOpHandle {
//             future: CollectiveGatherOpFuture::Shmem(f),
//         }
//     }
// }

// impl<T: Remote> Future for ShmemCollectiveGatherFuture<T> {
//     type Output = Option<Vec<T>>;

//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         match &mut self.target {
//             RootOrBuffer::Root(r) => {
//                 let mut res = Vec::new();
//                 std::mem::swap(&mut res, r);
//                 Poll::Ready(Some(res))
//             },
//             RootOrBuffer::NotRoot(_) => Poll::Ready(None),
//         }
//     }
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct ShmemCollectiveGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
//     pub(crate) alloc: ShmemAlloc,
//     pub(crate) index: usize,
//     pub(crate) len: usize,
//     pub(crate) target: RootOrLamellarBuffer<T, B>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }


// impl<T: Remote, B: AsLamellarBuffer<T>> ShmemCollectiveGatherIntoBufferFuture<T, B> {
//     fn exec_op(&mut self) {
//         match &mut self.target {
//             RootOrLamellarBuffer::Root(result) => {
//                 gather(self.alloc.my_alloc_pe, &self.alloc, Some(result.as_mut_slice()), self.index, self.len);
//             },
//             RootOrLamellarBuffer::NotRoot(root) => {
//                 gather::<T>(*root, &self.alloc, None, self.index, self.len);
//             },
//         }
//         self.spawned = true;
//     }

//     pub(crate) fn block(self)  {
//         self.exec_op();
//     }

//     pub(crate) fn spawn(self) -> LamellarTask<()> {
//         self.exec_op();

//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.clone().spawn_task(self, counters)
//     }
// }

// #[pinned_drop]
// impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for ShmemCollectiveGatherIntoBufferFuture<T, B> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a ShmemCollectiveGatherIntoBufferFuture").print();
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> From<ShmemCollectiveGatherIntoBufferFuture<T, B>> for CollectiveGatherIntoBufferOpHandle<T, B> {
//     fn from(f: ShmemCollectiveGatherIntoBufferFuture<T, B>) -> CollectiveGatherIntoBufferOpHandle<T, B> {
//         CollectiveGatherIntoBufferOpHandle {
//             future: CollectiveGatherIntoBufferOpFuture::Shmem(f),
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> Future for ShmemCollectiveGatherIntoBufferFuture<T, B> {
//     type Output = ();

//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         Poll::Ready(())
//     }
// }


// #[pin_project(PinnedDrop)]
// pub(crate) struct ShmemCollectiveAllBroadcastFuture<T: Remote> {
//     pub(crate) alloc: ShmemAlloc,
//     pub(crate) src: MemregionRdmaInputInner<T>,
//     pub(crate) result: Vec<T>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Remote> ShmemCollectiveAllBroadcastFuture<T> {
//     fn exec_op(&mut self) {
//         unimplemented!("shmem collective allbroadcast is not yet implemented");
//     }

//     pub(crate) fn block(self) -> Vec<T> {
//         self.exec_op();
//         let mut res = Vec::new();
//         std::mem::swap(&mut res, &mut self.result);
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
// impl<T: Remote> PinnedDrop for ShmemCollectiveAllBroadcastFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a ShmemCollectiveAllBroadcastFuture").print();
//         }
//     }
// }

// impl<T: Remote> From<ShmemCollectiveAllBroadcastFuture<T>> for CollectiveAllBroadcastOpHandle<T> {
//     fn from(f: ShmemCollectiveAllBroadcastFuture<T>) -> CollectiveAllBroadcastOpHandle<T> {
//         CollectiveAllBroadcastOpHandle {
//             future: CollectiveAllBroadcastOpFuture::Shmem(f),
//         }
//     }
// }

// impl<T: Remote> Future for ShmemCollectiveAllBroadcastFuture<T> {
//     type Output = Vec<T>;

//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         let mut res = Vec::new();
//         std::mem::swap(&mut res, &mut self.result);
//         Poll::Ready(res)
//     }
// }


// #[pin_project(PinnedDrop)]
// pub(crate) struct ShmemCollectiveAllBroadcastIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
//     pub(crate) alloc: ShmemAlloc,
//     pub(crate) src: MemregionRdmaInputInner<T>,
//     pub(crate) result: LamellarBuffer<T, B>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> ShmemCollectiveAllBroadcastIntoBufferFuture<T, B> {
//     fn exec_op(&mut self) {
//         unimplemented!("shmem collective allbroadcast into buffer is not yet implemented");
//     }

//     pub(crate) fn block(self)  {
//         self.exec_op();
//     }

//     pub(crate) fn spawn(self) -> LamellarTask<()> {
//         self.exec_op();

//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.clone().spawn_task(self, counters)
//     }
// }

// #[pinned_drop]
// impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for ShmemCollectiveAllBroadcastIntoBufferFuture<T, B> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a ShmemCollectiveAllBroadcastIntoBufferFuture").print();
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> From<ShmemCollectiveAllBroadcastIntoBufferFuture<T, B>> for CollectiveAllBroadcastIntoBufferOpHandle<T, B> {
//     fn from(f: ShmemCollectiveAllBroadcastIntoBufferFuture<T, B>) -> CollectiveAllBroadcastIntoBufferOpHandle<T, B> {
//         CollectiveAllBroadcastIntoBufferOpHandle {
//             future: CollectiveAllBroadcastIntoBufferOpFuture::Shmem(f),
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> Future for ShmemCollectiveAllBroadcastIntoBufferFuture<T, B> {
//     type Output = ();

//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         Poll::Ready(())
//     }
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct ShmemCollectiveBroadcastFuture<T: Remote> {
//     pub(crate) alloc: ShmemAlloc,
//     pub(crate) target: RootSrcOrBuffer<T> ,
//     pub(crate) len: usize,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// fn broadcast_root_side(alloc: &ShmemAlloc, root_index: usize, len: usize) {
//     alloc.coll_set_index(alloc.my_alloc_pe, root_index);
//     alloc.coll_inc_barrier();
//     while alloc.coll_get_barrier() < alloc.num_pes() {
//         std::thread::yield_now();
//     }
//     alloc.coll_inc_barrier();
//     while alloc.coll_get_barrier() < 2 * alloc.num_pes() {
//         std::thread::yield_now();
//     }

//     alloc.coll_reset_barrier();
// }

// fn broadcast_non_root_side<T: Remote>(alloc: &ShmemAlloc, root_index: usize, len: usize, res: &mut[T]) {
//     while(alloc.coll_get_barrier() == 0) {
//         std::thread::yield_now();
//     }

//     let remote_src_address = alloc.pe_base_offset(root_index) + alloc.coll_index(root_index) * std::mem::size_of::<T>();

//     let remote_src_slice = unsafe {std::slice::from_raw_parts(remote_src_address as *const T, len)};
//     res.copy_from_slice(remote_src_slice);
//     alloc.coll_inc_barrier();
//     while alloc.coll_get_barrier() < alloc.num_pes() {
//         std::thread::yield_now();
//     }
//     alloc.coll_inc_barrier();

//     while alloc.coll_get_barrier() >= alloc.num_pes() {
//         std::thread::yield_now();
//     }
// }

// impl<T: Remote> ShmemCollectiveBroadcastFuture<T> {
//     fn exec_op(&mut self) {
//         match self.target {
//             RootSrcOrBuffer::Root(index) => {
//                 broadcast_root_side(&self.alloc, index, self.len);
//             },
//             RootSrcOrBuffer::NotRoot(ref mut res, root) => {
//                 broadcast_non_root_side(&self.alloc, root, self.len, res.as_mut_slice());
//             },
//         }
//         self.spawned = true;
//     }

//     pub(crate) fn block(self) -> Option<Vec<T>>  {
//         self.exec_op();
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
// impl<T: Remote> PinnedDrop for ShmemCollectiveBroadcastFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a ShmemCollectiveBroadcastFuture").print();
//         }
//     }
// }

// impl<T: Remote> From<ShmemCollectiveBroadcastFuture<T>> for CollectiveBroadcastOpHandle<T> {
//     fn from(f: ShmemCollectiveBroadcastFuture<T>) -> CollectiveBroadcastOpHandle<T> {
//         CollectiveBroadcastOpHandle {
//             future: CollectiveBroadcastOpFuture::Shmem(f),
//         }
//     }
// }

// impl<T: Remote> Future for ShmemCollectiveBroadcastFuture<T> {
//     type Output = Option<Vec<T>>;

//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
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
// pub(crate) struct ShmemCollectiveBroadcastIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
//     pub(crate) alloc: ShmemAlloc,
//     pub(crate) target: RootSrcOrLamellarBufferInner<T, B>,
//     pub(crate) len: usize,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> ShmemCollectiveBroadcastIntoBufferFuture<T, B> {
//     fn exec_op(&mut self) {
//         match self.target {
//             RootSrcOrLamellarBufferInner::Root(index) => {
//                 broadcast_root_side(&self.alloc, index, self.len);
//             },
//             RootSrcOrLamellarBufferInner::NotRoot(ref mut res, root) => {
//                 broadcast_non_root_side(&self.alloc, root, self.len, res.as_mut_slice());
//             },
//         }
//         self.spawned = true;
//     }

//     pub(crate) fn block(self)  {
//         self.exec_op();
//     }

//     pub(crate) fn spawn(self) -> LamellarTask<()> {
//         self.exec_op();

//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.clone().spawn_task(self, counters)
//     }
// }

// #[pinned_drop]
// impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for ShmemCollectiveBroadcastIntoBufferFuture<T, B> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a ShmemCollectiveBroadcastIntoBufferFuture").print();
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> From<ShmemCollectiveBroadcastIntoBufferFuture<T, B>> for CollectiveBroadcastIntoBufferOpHandle<T, B> {
//     fn from(f: ShmemCollectiveBroadcastIntoBufferFuture<T, B>) -> CollectiveBroadcastIntoBufferOpHandle<T, B> {
//         CollectiveBroadcastIntoBufferOpHandle {
//             future: CollectiveBroadcastIntoBufferOpFuture::Shmem(f),
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> Future for ShmemCollectiveBroadcastIntoBufferFuture<T, B> {
//     type Output = ();

//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         Poll::Ready(())
//     }
// }

// fn scatter_root_side<T: Remote>(alloc: &ShmemAlloc, index: usize, len: usize, res: &mut[T]) {
//     let alloc_slice = unsafe { alloc.as_mut_slice() };
//     let my_slice = &mut alloc_slice[index..index + len];
//     res.copy_from_slice(my_slice);
//     alloc.coll_set_index(alloc.my_alloc_pe, index);
//     alloc.coll_inc_barrier();
//     while alloc.coll_get_barrier() < alloc.num_pes() {
//         std::thread::yield_now();
//     }
//     alloc.coll_inc_barrier();
//     while alloc.coll_get_barrier() < 2 * alloc.num_pes() {
//         std::thread::yield_now();
//     }

//     alloc.coll_reset_barrier();
// }

// fn scatter_non_root_side<T: Remote>(alloc: &ShmemAlloc, root_index: usize, len: usize, res: &mut[T]) {
//     while(alloc.coll_get_barrier() == 0) {
//         std::thread::yield_now();
//     }

//     let remote_src_address = alloc.pe_base_offset(root_index) + alloc.coll_index(root_index) * std::mem::size_of::<T>();

//     let remote_src_slice = unsafe {std::slice::from_raw_parts(remote_src_address as *const T, len * alloc.num_pes())};
//     res.copy_from_slice(&remote_src_slice[alloc.my_alloc_pe * len..(alloc.my_alloc_pe + 1) * len]);
//     alloc.coll_inc_barrier();
//     while alloc.coll_get_barrier() < alloc.num_pes() {
//         std::thread::yield_now();
//     }
//     alloc.coll_inc_barrier();

//     while alloc.coll_get_barrier() >= alloc.num_pes() {
//         std::thread::yield_now();
//     }
// }


// #[pin_project(PinnedDrop)]
// pub(crate) struct ShmemCollectiveScatterFuture<T: Remote> {
//     pub(crate) alloc: ShmemAlloc,
//     pub(crate) len: usize,
//     pub(crate) result: Vec<T> ,
//     src_or_root_pe: ScatterInputInner,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Remote> ShmemCollectiveScatterFuture<T> {
//     fn exec_op(&mut self) {
//         match self.src_or_root_pe {
//             ScatterInputInner::Root(index) => {
//                 scatter_root_side(&self.alloc, index, self.len, &mut self.result);
//             },
//             ScatterInputInner::NotRoot(root) => {
//                 scatter_non_root_side(&self.alloc, root, self.len, &mut self.result);
//             },
//         }
//         self.spawned = true;
//     }

//     pub(crate) fn block(self) -> Vec<T> {
//         self.exec_op();
//         let mut res = Vec::new();
//         std::mem::swap(&mut res, &mut self.result);
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
// impl<T: Remote> PinnedDrop for ShmemCollectiveScatterFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a ShmemCollectiveScatterFuture").print();
//         }
//     }
// }

// impl<T: Remote> From<ShmemCollectiveScatterFuture<T>> for CollectiveScatterOpHandle<T> {
//     fn from(f: ShmemCollectiveScatterFuture<T>) -> CollectiveScatterOpHandle<T> {
//         CollectiveScatterOpHandle {
//             future: CollectiveScatterOpFuture::Shmem(f),
//         }
//     }
// }

// impl<T: Remote> Future for ShmemCollectiveScatterFuture<T> {
//     type Output = Vec<T>;

//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         let mut res = Vec::new();
//         std::mem::swap(&mut res, &mut self.result);
//         Poll::Ready(res)
//     }
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct ShmemCollectiveScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
//     pub(crate) alloc: ShmemAlloc,
//     pub(crate) len: usize,
//     src_or_root_pe: ScatterInputInner,
//     pub(crate) result: LamellarBuffer<T, B>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> ShmemCollectiveScatterIntoBufferFuture<T, B> {
//     fn exec_op(&mut self) {
//         match self.src_or_root_pe {
//             ScatterInputInner::Root(index) => {
//                 scatter_root_side(&self.alloc, index, self.len, self.result.as_mut_slice());
//             },
//             ScatterInputInner::NotRoot(root) => {
//                 scatter_non_root_side(&self.alloc, root, self.len, self.result.as_mut_slice());
//             },
//         }
//         self.spawned = true;
//     }

//     pub(crate) fn block(self)  {
//         self.exec_op();
//     }

//     pub(crate) fn spawn(self) -> LamellarTask<()> {
//         self.exec_op();

//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.clone().spawn_task(self, counters)
//     }
// }

// #[pinned_drop]
// impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for ShmemCollectiveScatterIntoBufferFuture<T, B> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a ShmemCollectiveScatterIntoBufferFuture").print();
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> From<ShmemCollectiveScatterIntoBufferFuture<T, B>> for CollectiveScatterIntoBufferOpHandle<T, B> {
//     fn from(f: ShmemCollectiveScatterIntoBufferFuture<T, B>) -> CollectiveScatterIntoBufferOpHandle<T, B> {
//         CollectiveScatterIntoBufferOpHandle {
//             future: CollectiveScatterIntoBufferOpFuture::Shmem(f),
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> Future for ShmemCollectiveScatterIntoBufferFuture<T, B> {
//     type Output = ();

//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         Poll::Ready(())
//     }
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct ShmemCollectiveReduceScatterFuture<T: Remote> {
//     pub(crate) alloc: ShmemAlloc,
//     pub(super) op: ReduceOp,
//     pub(crate) index: usize,
//     pub(crate) len: usize,
//     pub(crate) result: Vec<T>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Remote> ShmemCollectiveReduceScatterFuture<T> {
//     fn exec_op(&mut self) {
//         let alloc_slice = unsafe { self.alloc.as_mut_slice() };
//         let my_slice = &mut alloc_slice[self.index..self.index + self.len]; 
//         let mut to_replace: Vec<T> = vec![T::default(); my_slice.len()];
//         to_replace.copy_from_slice(my_slice);
//         let chunk_size = self.len / self.alloc.num_pes();
//         let remote_dst_address = reduce_to_root::<T>(0, &self.alloc, None, self.index, self.len, &self.op);
//         if self.alloc.my_alloc_pe == 0 {
//             self.result.copy_from_slice(&my_slice[self.alloc.my_alloc_pe * chunk_size..(self.alloc.my_alloc_pe + 1) * chunk_size]);
//             self.alloc.coll_inc_barrier();
            
//             while self.alloc.coll_get_barrier() != 2 * self.alloc.num_pes() {
//                 std::thread::yield_now();
//             }

//             self.alloc.coll_reset_barrier();
//             my_slice.copy_from_slice(&to_replace);
//         }
//         else {
//             while self.alloc.coll_get_barrier() < self.alloc.num_pes() {
//                 std::thread::yield_now();
//             }

//             let remote_dst_slice = unsafe {std::slice::from_raw_parts(remote_dst_address.unwrap() as *const T, self.len)};

//             self.result.copy_from_slice(&remote_dst_slice[self.alloc.my_alloc_pe * chunk_size..(self.alloc.my_alloc_pe + 1) * chunk_size]);
//             self.alloc.coll_inc_barrier();
//             while self.alloc.coll_get_barrier() >= self.alloc.num_pes() {
//                 std::thread::yield_now();
//             }
//         }
//         self.spawned = true;
//     }

//     pub(crate) fn block(self) -> Vec<T> {
//         self.exec_op();
//         let mut res = Vec::new();
//         std::mem::swap(&mut res, &mut self.result);
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
// impl<T: Remote> PinnedDrop for ShmemCollectiveReduceScatterFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {  
//             RuntimeWarning::DroppedHandle("a ShmemCollectiveReduceScatterFuture").print();
//         }
//     }
// }

// impl<T: Remote> From<ShmemCollectiveReduceScatterFuture<T>> for CollectiveReduceScatterOpHandle<T> {
//     fn from(f: ShmemCollectiveReduceScatterFuture<T>) -> CollectiveReduceScatterOpHandle<T> {
//         CollectiveReduceScatterOpHandle {
//             future: CollectiveReduceScatterOpFuture::Shmem(f),
//         }
//     }
// }

// impl<T: Remote> Future for ShmemCollectiveReduceScatterFuture<T> {
//     type Output = Vec<T>;

//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         let mut res = Vec::new();
//         std::mem::swap(&mut res, &mut self.result);
//         Poll::Ready(res)
//     }
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct ShmemCollectiveReduceScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
//     pub(crate) alloc: ShmemAlloc,
//     pub(super) op: ReduceOp,
//     pub(crate) index: usize,
//     pub(crate) len: usize,
//     pub(crate) result: LamellarBuffer<T, B>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> ShmemCollectiveReduceScatterIntoBufferFuture<T, B> {
//     fn exec_op(&mut self) {
//         let alloc_slice = unsafe { self.alloc.as_mut_slice() };
//         let my_slice = &mut alloc_slice[self.index..self.index + self.len]; 
//         let mut to_replace: Vec<T> = vec![T::default(); my_slice.len()];
//         let chunk_size = self.len / self.alloc.num_pes();
//         to_replace.copy_from_slice(my_slice);
//         let remote_dst_address = reduce_to_root::<T>(0, &self.alloc, None, self.index, self.len, &self.op);
//         if self.alloc.my_alloc_pe == 0 {
//             self.result.as_mut_slice().copy_from_slice(&my_slice[self.alloc.my_alloc_pe * chunk_size..(self.alloc.my_alloc_pe + 1) * chunk_size]);
//             self.alloc.coll_inc_barrier();
            
//             while self.alloc.coll_get_barrier() != 2 * self.alloc.num_pes() {
//                 std::thread::yield_now();
//             }

//             self.alloc.coll_reset_barrier();
//             my_slice.copy_from_slice(&to_replace);
//         }
//         else {
//             while self.alloc.coll_get_barrier() < self.alloc.num_pes() {
//                 std::thread::yield_now();
//             }

//             let remote_dst_slice = unsafe {std::slice::from_raw_parts(remote_dst_address.unwrap() as *const T, self.len)};
//             self.result.as_mut_slice().copy_from_slice(&remote_dst_slice[self.alloc.my_alloc_pe * chunk_size..(self.alloc.my_alloc_pe + 1) * chunk_size]);
//             self.alloc.coll_inc_barrier();
//             while self.alloc.coll_get_barrier() >= self.alloc.num_pes() {
//                 std::thread::yield_now();
//             }
//         }
//     }

//     pub(crate) fn block(self)  {
//         self.exec_op();
//     }

//     pub(crate) fn spawn(self) -> LamellarTask<()> {
//         self.exec_op();

//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.clone().spawn_task(self, counters)
//     }
// }

// #[pinned_drop]
// impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for ShmemCollectiveReduceScatterIntoBufferFuture<T, B> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a ShmemCollectiveReduceScatterIntoBufferFuture").print();
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> From<ShmemCollectiveReduceScatterIntoBufferFuture<T, B>> for CollectiveReduceScatterIntoBufferOpHandle<T, B> {
//     fn from(f: ShmemCollectiveReduceScatterIntoBufferFuture<T, B>) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
//         CollectiveReduceScatterIntoBufferOpHandle {
//             future: CollectiveReduceScatterIntoBufferOpFuture::Shmem(f),
//         }
//     }
// }

// impl<T: Remote, B: AsLamellarBuffer<T>> Future for ShmemCollectiveReduceScatterIntoBufferFuture<T, B> {
//     type Output = ();

//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         Poll::Ready(())
//     }
// }

// impl CommAllocCollectiveAllReduce for ShmemAlloc {
//     fn reduce_all<T: crate::Remote>(
//         &self,
//         scheduler: &std::sync::Arc<crate::scheduler::Scheduler>,
//         counters: Vec<std::sync::Arc<crate::active_messaging::AMCounters>>,
//         index: usize,
//         len: usize,
//         op: crate::lamellae::collective::ReduceOp,
//     ) -> crate::lamellae::collective::CollectiveAllReduceOpHandle<T> {

//         ShmemCollectiveAllReduceFuture {
//             alloc: self.clone(),
//             op,
//             index,
//             len,
//             result: vec![T::default(); len],
//             scheduler: scheduler.clone(),
//             counters,
//             spawned: false,
//         }
//         .into()
//     }

//     fn reduce_all_into_buffer<T: crate::Remote, B: crate::AsLamellarBuffer<T>> (
//         &self,
//         scheduler: &std::sync::Arc<crate::scheduler::Scheduler>,
//         counters: Vec<std::sync::Arc<crate::active_messaging::AMCounters>>,
//         index: usize,
//         len: usize,
//         op: crate::lamellae::collective::ReduceOp,
//         dst: crate::LamellarBuffer<T, B>,
//     ) -> crate::lamellae::collective::CollectiveAllReduceIntoBufferOpHandle<T, B> {
//         ShmemCollectiveAllReduceIntoBufferFuture {
//             alloc: self.clone(),
//             op,
//             index,
//             len,
//             result: dst,
//             scheduler: scheduler.clone(),
//             counters,
//             spawned: false,
//         }
//         .into()
//     }

//     fn reduce_all_in_place<T: crate::Remote, B: crate::AsLamellarBuffer<T>>(
//         &self, // TODO: This should probably take a multiple reference to self.
//         scheduler: &std::sync::Arc<crate::scheduler::Scheduler>,
//         counters: Vec<std::sync::Arc<crate::active_messaging::AMCounters>>,
//         src_and_dst: crate::LamellarBuffer<T, B>,
//         op: crate::lamellae::collective::ReduceOp,
//     ) -> crate::lamellae::collective::CollectiveAllReduceInPlaceOpHandle<T, B> {
//         ShmemCollectiveAllReduceInPlaceFuture {
//             alloc: self.clone(),
//             op,
//             result: src_and_dst,
//             scheduler: scheduler.clone(),
//             counters,
//             spawned: false,
//             phantom: std::marker::PhantomData,
//         }
//         .into()
//     }
// }

// impl CommAllocCollectiveReduce for ShmemAlloc {
//     fn reduce<T: crate::Remote>(
//         &self,
//         scheduler: &std::sync::Arc<crate::scheduler::Scheduler>,
//         counters: Vec<std::sync::Arc<crate::active_messaging::AMCounters>>,
//         op: crate::lamellae::collective::ReduceOp,
//         index: usize,
//         len: usize,
//         root_pe: usize,
//     ) -> crate::lamellae::collective::CollectiveReduceOpHandle<T> {
//         let target =
//             if root_pe != self.my_alloc_pe {
//                 RootOrBuffer::NotRoot(root_pe)
//             }
//             else {
//                 RootOrBuffer::Root(vec![T::default(); len])
//             };
//         ShmemCollectiveReduceFuture {
//             alloc: self.clone(),
//             index,
//             len,
//             op,
//             target,
//             scheduler: scheduler.clone(),
//             counters,
//             spawned: false,
//         }
//         .into()
//     }
    
//     fn reduce_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Option<Arc<[Arc<AMCounters>]>>,
//         op: ReduceOp,
//         index: usize,
//         len: usize,
//         root_or_buffer: crate::lamellae::collective::RootOrLamellarBuffer<T, B>
//     ) -> crate::lamellae::collective::CollectiveReduceIntoBufferOpHandle<T, B> {
//         ShmemCollectiveReduceIntoBufferFuture {
//             alloc: self.clone(),
//             index,
//             len,
//             op,
//             target: root_or_buffer,
//             scheduler: scheduler.clone(),
//             counters,
//             spawned: false,
//         }
//         .into()
//     }
// }

// impl CommAllocCollectiveBroadcast for ShmemAlloc {
//     fn broadcast<T: Remote>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Option<Arc<[Arc<AMCounters>]>>,
//         src_or_pe: BroadcastInput,
//         len: usize,
//     ) -> CollectiveBroadcastOpHandle<T> {
//         let target = match src_or_pe {
//                 BroadcastInput::Root(index) => RootSrcOrBuffer::Root(index),
//                 BroadcastInput::NotRoot(root) => RootSrcOrBuffer::NotRoot(vec![T::default(); len], root),
//             };
//         ShmemCollectiveBroadcastFuture {
//             alloc: self.clone(),
//             target,
//             len,
//             scheduler: scheduler.clone(),
//             counters,
//             spawned: false,
//         }
//         .into()
//     }
    
//     fn broadcast_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Option<Arc<[Arc<AMCounters>]>>,
//         root_or_buffer: RootSrcOrLamellarBuffer<T, B>,
//         len: usize,
//     ) -> CollectiveBroadcastIntoBufferOpHandle<T, B> {
        
//         ShmemCollectiveBroadcastIntoBufferFuture {
//             alloc: self.clone(),
//             target: root_or_buffer.into(),
//             len,
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//         }.into()
//     }
// }

// impl CommAllocCollectiveGather for ShmemAlloc {
//     fn gather<T: Remote>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Option<Arc<[Arc<AMCounters>]>>,
//         index: usize,
//         len: usize,
//         root_pe: usize,
//     ) -> CollectiveGatherOpHandle<T> {
//         let target =
//             if root_pe != self.my_alloc_pe {
//                 RootOrBuffer::NotRoot(root_pe)
//             }
//             else {
//                 RootOrBuffer::Root(vec![T::default(); len * self.num_pes()])
//             };
//         ShmemCollectiveGatherFuture {
//             alloc: self.clone(),
//             index,
//             len,
//             target,
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//         }
//         .into()
//     }
//     fn gather_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Option<Arc<[Arc<AMCounters>]>>,
//         index: usize,
//         len: usize,
//         root_or_buffer: RootOrLamellarBuffer<T, B>
//     ) -> CollectiveGatherIntoBufferOpHandle<T, B> {
        
//         ShmemCollectiveGatherIntoBufferFuture {
//             alloc: self.clone(),
//             index,
//             len,
//             target: root_or_buffer,
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//         }.into()
//     }
// }


// impl CommAllocCollectiveAllGather for ShmemAlloc {
//     fn gather_all<T: Remote>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Option<Arc<[Arc<AMCounters>]>>,
//         index: usize,
//         len: usize,
//     ) -> CollectiveAllGatherOpHandle<T> {
//         ShmemCollectiveAllGatherFuture {
//             alloc: self.clone(),
//             index,
//             len,
//             result: vec![T::default(); len * self.num_pes()],
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//         }
//         .into()
//     }
//     fn gather_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Option<Arc<[Arc<AMCounters>]>>,
//         index: usize,
//         len: usize,
//         dst: LamellarBuffer<T, B>,
//     ) -> CollectiveAllGatherIntoBufferOpHandle<T, B> {
        
//         ShmemCollectiveAllGatherIntoBufferFuture {
//             alloc: self.clone(),
//             index,
//             len,
//             result: dst,
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//         }.into()
//     }
// }

// impl CommAllocCollectiveScatter for ShmemAlloc {
//     fn scatter<T: Remote>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Option<Arc<[Arc<AMCounters>]>>,
//         src_or_root_pe: ScatterInput,
//         len: usize,
//     ) -> CollectiveScatterOpHandle<T> {
//         ShmemCollectiveScatterFuture {
//             alloc: self.clone(),
//             len,
//             src_or_root_pe: src_or_root_pe.into(),
//             result: vec![T::default(); len],
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//         }
//         .into()
//     }
//     fn scatter_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Option<Arc<[Arc<AMCounters>]>>,
//         result: LamellarBuffer<T, B>,
//         src_or_root_pe: ScatterInput,
//         len: usize,
//     ) -> CollectiveScatterIntoBufferOpHandle<T, B> {
        
//         ShmemCollectiveScatterIntoBufferFuture {
//             alloc: self.clone(),
//             len,
//             result: result,
//             src_or_root_pe: src_or_root_pe.into(),
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//         }.into()
//     }
// }

// impl CommAllocCollectiveReduceScatter for ShmemAlloc {
//     fn reduce_scatter<T: Remote>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Option<Arc<[Arc<AMCounters>]>>,
//         op: ReduceOp,
//         index: usize, 
//         len: usize,
//     ) -> CollectiveReduceScatterOpHandle<T> {
//         ShmemCollectiveReduceScatterFuture {
//             alloc: self.clone(),
//             op: op,
//             index,
//             len,
//             result: vec![T::default(); len / self.num_pes()],
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//         }
//         .into()
//     }
//     fn reduce_scatter_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Option<Arc<[Arc<AMCounters>]>>,
//         op: ReduceOp,
//         index: usize,
//         len: usize,
//         dst: LamellarBuffer<T, B>,
//     ) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
//         ShmemCollectiveReduceScatterIntoBufferFuture {
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
// }

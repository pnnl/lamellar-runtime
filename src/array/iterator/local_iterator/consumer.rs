pub(crate) mod collect;
pub(crate) mod count;
pub(crate) mod for_each;
pub(crate) mod reduce;
pub(crate) mod sum;

pub(crate) use collect::*;
pub(crate) use count::*;
pub(crate) use for_each::*;
pub(crate) use reduce::*;
pub(crate) use sum::*;

// use crate::active_messaging::LamellarArcLocalAm;
// use crate::lamellar_request::LamellarRequest;
// use crate::lamellar_team::LamellarTeamRT;
// use crate::array::iterator::local_iterator::{LocalIterator,IterRequest,Monotonic};

// use std::sync::Arc;
// use std::sync::atomic::{AtomicUsize,Ordering};
// use std::pin::Pin;
// use parking_lot::Mutex;
// use rand::thread_rng;
// use rand::prelude::SliceRandom;

// #[derive(Clone, Debug)]
// pub(crate) struct IterWorkStealer {
//     pub(crate) range: Arc<Mutex<(usize, usize)>>, //start, end
// }

// impl IterWorkStealer {
//     fn set_range(&self, start: usize, end: usize) {
//         let mut range = self.range.lock();
//         range.0 = start;
//         range.1 = end;
//     }

//     fn next(&self) -> Option<usize> {
//         let mut range = self.range.lock();
//         let index = range.0;
//         range.0 += 1;
//         if range.0 <= range.1 {
//             Some(index)
//         } else {
//             None
//         }
//     }
//     fn set_done(&self) {
//         let mut range = self.range.lock();
//         range.0 = range.1;
//     }

//     fn steal(&self) -> Option<(usize, usize)> {
//         let mut range = self.range.lock();
//         let start = range.0;
//         let end = range.1;
//         if end > start && end - start > 2 {
//             let new_end = (start + end) / 2;
//             range.1 = new_end;
//             Some((new_end, end))
//         } else {
//             None
//         }
//     }
// }

// #[derive(Clone, Debug)]
// pub(crate) enum IterSchedule{
//     Static(usize,usize),
//     Dynamic(Arc<AtomicUsize>,usize),
//     Chunk(Vec<(usize, usize)>, Arc<AtomicUsize>,),
//     WorkStealing(IterWorkStealer, Vec<IterWorkStealer>)
// }

// impl IterSchedule {
//     fn init_iter<I: LocalIterator>(&self, iter: I) -> IterScheduleIter<I> {
//         match self {
//             IterSchedule::Static( start, end) => {
//                 IterScheduleIter::Static(iter.init(*start,end-start))
//             }
//             IterSchedule::Dynamic(cur_i, max_i) => {
//                 IterScheduleIter::Dynamic(iter, cur_i.clone(), *max_i)
//             }
//             IterSchedule::Chunk(ranges, range_i) => {
//                 IterScheduleIter::Chunk(iter.init(0,0), ranges.clone(),range_i.clone())
//             }
//             IterSchedule::WorkStealing( range, siblings) => {
//                 let (start, end) = *range.range.lock();
//                 IterScheduleIter::WorkStealing(iter.init(start, end-start), range.clone(), siblings.clone())
//             }
//         }
//     }
//     fn monotonic_iter<I: LocalIterator>(&self, iter: I) -> IterScheduleIter<Monotonic<I>> {
//         match self {
//             IterSchedule::Static(start, end) => {
//                 IterScheduleIter::Static(iter.monotonic().init(*start,end-start))
//             }
//             IterSchedule::Dynamic(cur_i, max_i) => {
//                 IterScheduleIter::Dynamic(iter.monotonic(), cur_i.clone(), *max_i)
//             }
//             IterSchedule::Chunk(ranges, range_i) => {
//                 IterScheduleIter::Chunk(iter.monotonic().init(0,0), ranges.clone(),range_i.clone())
//             }
//             IterSchedule::WorkStealing(range, siblings) => {
//                 let (start, end) = *range.range.lock();
//                 IterScheduleIter::WorkStealing(iter.monotonic().init(start, end-start), range.clone(), siblings.clone())            }
//         }
//     }
// }

// pub(crate) enum IterScheduleIter<I>{
//     Static(I),
//     Dynamic(I,Arc<AtomicUsize>,usize),
//     Chunk(I,Vec<(usize, usize)>, Arc<AtomicUsize>),
//     WorkStealing(I,IterWorkStealer, Vec<IterWorkStealer>)
// }

// impl<I: LocalIterator> Iterator for IterScheduleIter<I> {
//     type Item = I::Item;
//     fn next(&mut self) -> Option<Self::Item> {
//         match self {
//             IterScheduleIter::Static(iter) => {
//                 iter.next()
//             }
//             IterScheduleIter::Dynamic(iter, cur_i, max_i) => {
//                 let mut ci = cur_i.fetch_add(1, Ordering::Relaxed);
//                 while ci < *max_i {
//                     // println!("ci {:?} maxi {:?} {:?}", ci, *max_i, std::thread::current().id());
//                     *iter = iter.init(ci,1);
//                     if let Some(elem) = iter.next() {
//                         return Some(elem);
//                     }
//                     ci = cur_i.fetch_add(1, Ordering::Relaxed);
//                 }
//                 None
//             }
//             IterScheduleIter::Chunk(iter, ranges, range_i) => {
//                 let mut next = iter.next();
//                 // println!("next {:?} {:?}", next.is_none(), std::thread::current().id());
//                 if next.is_none(){
//                     let ri = range_i.fetch_add(1, Ordering::Relaxed);
//                     // println!("range {:?} {:?}", ri, std::thread::current().id());
//                     if ri < ranges.len() {
//                         *iter = iter.init(ranges[ri].0, ranges[ri].1-ranges[ri].0);
//                         next = iter.next();
//                     }
//                 }
//                 next
//             }
//             IterScheduleIter::WorkStealing(iter, range, siblings) => {
//                 let mut inner_next = |iter: &mut I| {
//                     while let Some(ri) = range.next(){
//                         *iter = iter.init(ri,1);
//                         if let Some(elem) = iter.next() {
//                             return Some(elem);
//                         }
//                         // else{
//                         //     range.set_done();
//                         // }
//                     }
//                     None
//                 };
//                 let mut next = inner_next(iter);
//                 if next.is_none() {
//                     let mut rng = thread_rng();
//                     let mut workers = (0..siblings.len()).collect::<Vec<usize>>();
//                     workers.shuffle(&mut rng);
//                     if let Some(worker) = workers.pop() {
//                         if let Some((start, end)) = siblings[worker].steal() {
//                             *iter = iter.init(start, end - start);
//                             range.set_range(start, end);
//                             next = inner_next(iter);
//                         }
//                     }
//                 }
//                 next
//             }
//         }
//     }
// }

// pub(crate) trait IterConsumer{
//     type AmOutput;
//     type Output;
//     fn into_am(&self, schedule: IterSchedule) -> LamellarArcLocalAm;
//     fn create_handle(self, team: Pin<Arc<LamellarTeamRT>>, reqs: Vec<Box<dyn LamellarRequest<Output = Self::AmOutput>>>) -> Box<dyn IterRequest<Output = Self::Output>>;
//     fn max_elems(&self, in_elems: usize) -> usize;
// }

// // #[derive(Clone, Debug)]
// // pub(crate) enum IterConsumer<I,A,T,F,R>{
// //     Collect(Distribution,PhantomData<A>),
// //     Count,
// //     ForEach(F),
// //     Reduce(R),
// // }

// // impl<I,A,T,F,R> IterConsumer<I,A,T,F,R> where
// //     I: LocalIterator + 'static,
// //     I::Item: SyncSend,
// //     A: From<UnsafeArray<T>> + SyncSend,
// //     T: Dist + ArrayOps
// //     F: Fn(I::Item) + SyncSend + Clone + 'static,
// //     R: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,{

// //     fn into_am<Am>(self, schedule: IterSchedule<I>) -> Am
// //     where
// //         A: LamellarActiveMessage + LocalAM + 'static,{
// //         match self {
// //             IterConsumer::Collect(_) => {
// //                 CollectAm{
// //                     schedule
// //                 }
// //             }
// //             IterConsumer::Count => {
// //                 CountAm{
// //                     schedule
// //                 }
// //             }
// //             IterConsumer::ForEach(op) => {
// //                 ForEachAm{
// //                     op,
// //                     schedule,
// //                 }
// //             }
// //             IterConsumer::Reduce(op) => {
// //                 ReduceAm{
// //                     op,
// //                     schedule,
// //                 }
// //             }
// //         }
// //     }

// //     fn create_handle<O>(self, team: Pin<Arc<LamellarTeamRT>>, reqs: Vec<Box<dyn LamellarRequest<Output = O>>) -> IterConsumerHandle<A,T,F,R>{
// //         match self {
// //             IterConsumer::Collect(dist,phantom) => {
// //                 IterConsumerHandle::Collect(LocalIterCollectHandle{
// //                     reqs: reqs,
// //                     distribution: dist,
// //                     team: team,
// //                     _phantom: phantom,
// //                 })
// //             }
// //             IterConsumer::Count => {
// //                 IterConsumerHandle::Count(LocalIterCountHandle{
// //                     reqs: reqs,
// //                 })
// //             }
// //             IterConsumer::ForEach(_) => {
// //                 IterConsumerHandle::ForEach(LocalIterForEachHandle{
// //                     reqs: reqs,
// //                 })
// //             }
// //             IterConsumer::Reduce(op) => {
// //                 IterConsumerHandle::Reduce(LocalIterReduceHandle::<I::Item,R>{
// //                     reqs:reqs,
// //                     op: op
// //                 })
// //             }
// //         }
// //     }
// // }

// // pub(crate) enum IterConsumerHandle<A,T,F>{
// //     Collect(LocalIterCollectHandle<T,A>),
// //     Count(LocalIterCountHandle),
// //     ForEach(LocalIterForEachHandle),
// //     Reduce(LocalIterReduceHandle<T,F>)
// // }

// // #[async_trait]
// // impl<A,T,F> IterConsumerHandle<I,A,T,F,R> where
// //     A: From<UnsafeArray<T>> + SyncSend,
// //     T: Dist + ArrayOps
// //     F: Fn(I::Item) + SyncSend + Clone + 'static,
// //     R: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,{

use crate::active_messaging::SyncSend;
use crate::array::iterator::distributed_iterator::*;
use crate::array::iterator::private::Sealed;
use crate::array::r#unsafe::{UnsafeArray, UnsafeArrayInner};
use crate::array::{ArrayOps, AsyncTeamFrom, Distribution, InnerArray};
use crate::lamellar_request::LamellarRequest;

use crate::array::iterator::Schedule;
use crate::lamellar_team::LamellarTeamRT;
use crate::memregion::Dist;

use core::marker::PhantomData;
use futures_util::Future;
use paste::paste;
use std::pin::Pin;
use std::sync::Arc;

impl<T> InnerArray for UnsafeArray<T> {
    fn as_inner(&self) -> &UnsafeArrayInner {
        &self.inner
    }
}

impl InnerArray for UnsafeArrayInner {
    fn as_inner(&self) -> &UnsafeArrayInner {
        &self
    }
}

impl<T: Dist> DistIteratorLauncher for UnsafeArray<T> {}

macro_rules! consumer_impl {
    ($name:ident<$($generics:ident),*>($($arg:ident : $arg_ty:ty),*); [$return_type:ident$(<$($ret_gen:ty),*>)?]; [$($bounds:tt)+]; [$($am:tt)*]; [$(-> $($blocking_ret:tt)*)?] ) => {
        paste! {
            fn $name<$($generics),*>(&self, $($arg : $arg_ty),*) -> $return_type$(<$($ret_gen),*>)?
            where
            $($bounds)+
            {

                self.[<$name _with_schedule>](Schedule::Static, $($arg),*)
            }


            fn [<$name _with_schedule >]<$($generics),*>(
                &self,
                sched: Schedule,
                $($arg : $arg_ty),*
            ) ->   $return_type$(<$($ret_gen),*>)?
            where
                $($bounds)+
            {
                let am = $($am)*;
                let barrier = self.barrier_handle();
                let inner = self.clone();
                let reqs_future = Box::pin(async move{match sched {
                    Schedule::Static => inner.sched_static(am),
                    Schedule::Dynamic => inner.sched_dynamic(am),
                    Schedule::Chunk(size) => inner.sched_chunk(am,size),
                    Schedule::Guided => inner.sched_guided(am),
                    Schedule::WorkStealing => inner.sched_work_stealing(am),
                }});
                $return_type::new(barrier,reqs_future,self)
            }

            fn [<blocking_ $name>]<$($generics),*>(&self, $($arg : $arg_ty),*) $(-> $($blocking_ret)*)?
            where
            $($bounds)+
            {

                self.[<blocking_ $name _with_schedule>](Schedule::Static, $($arg),*)
            }


            fn [<blocking_ $name _with_schedule >]<$($generics),*>(
                &self,
                sched: Schedule,
                $($arg : $arg_ty),*
            ) $(-> $($blocking_ret)*)?
            where
                $($bounds)+
            {
                let am = $($am)*;
                self.data.team.barrier.tasking_barrier();
                let inner = self.clone();
                let reqs = match sched {
                    Schedule::Static => inner.sched_static(am),
                    Schedule::Dynamic => inner.sched_dynamic(am),
                    Schedule::Chunk(size) => inner.sched_chunk(am,size),
                    Schedule::Guided => inner.sched_guided(am),
                    Schedule::WorkStealing => inner.sched_work_stealing(am),
                };
                reqs.blocking_wait()
            }
        }
    };
}

impl DistIteratorLauncher for UnsafeArrayInner {
    // type Inner = Self;
    fn global_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        // println!("global index cs:{:?}",chunk_size);
        if chunk_size == 1 {
            self.global_index_from_local(index)
        } else {
            Some(self.global_index_from_local(index * chunk_size)? / chunk_size)
        }
    }

    fn subarray_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        if chunk_size == 1 {
            self.subarray_index_from_local(index)
        } else {
            Some(self.subarray_index_from_local(index * chunk_size)? / chunk_size)
        }
    }

    consumer_impl!(
    for_each<I, F>(iter: &I, op: F); 
    [DistIterForEachHandle];
    [I: DistributedIterator + 'static, F: Fn(I::Item) + SyncSend + Clone + 'static];
    [
        ForEach {
            iter: iter.iter_clone(Sealed),
            op,
        }
    ];
    []);

    consumer_impl!(
        for_each_async<I, F, Fut>(iter: &I, op: F); 
        [DistIterForEachHandle];
        [I: DistributedIterator + 'static, F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static, Fut: Future<Output = ()> + Send + 'static];
        [
            ForEachAsync {
                iter: iter.iter_clone(Sealed),
                op,
            }
        ];
        []
    );

    // fn for_each<I, F>(&self, iter: &I, op: F) -> DistIterForEachHandle
    // where
    //     I: DistributedIterator + 'static,
    //     F: Fn(I::Item) + SyncSend + Clone + 'static,
    // {
    //     self.for_each_with_schedule(Schedule::Static, iter, op)
    // }

    // fn for_each_with_schedule<I, F>(
    //     &self,
    //     sched: Schedule,
    //     iter: &I,
    //     op: F,
    // ) -> DistIterForEachHandle
    // where
    //     I: DistributedIterator + 'static,
    //     F: Fn(I::Item) + SyncSend + Clone + 'static,
    // {
    //     let for_each = ForEach {
    //         iter: iter.iter_clone(Sealed),
    //         op,
    //     };
    //     self.team().barrier();
    //     match sched {
    //         Schedule::Static => self.sched_static(for_each),
    //         Schedule::Dynamic => self.sched_dynamic(for_each),
    //         Schedule::Chunk(size) => self.sched_chunk(for_each, size),
    //         Schedule::Guided => self.sched_guided(for_each),
    //         Schedule::WorkStealing => self.sched_work_stealing(for_each),
    //     }
    // }

    // fn for_each_async<I, F, Fut>(&self, iter: &I, op: F) -> DistIterForEachHandle
    // where
    //     I: DistributedIterator + 'static,
    //     F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    //     Fut: Future<Output = ()> + Send + 'static,
    // {
    //     self.for_each_async_with_schedule(Schedule::Static, iter, op)
    // }

    // fn for_each_async_with_schedule<I, F, Fut>(
    //     &self,
    //     sched: Schedule,
    //     iter: &I,
    //     op: F,
    // ) -> DistIterForEachHandle
    // where
    //     I: DistributedIterator + 'static,
    //     F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    //     Fut: Future<Output = ()> + Send + 'static,
    // {
    //     let for_each = ForEachAsync {
    //         iter: iter.iter_clone(Sealed),
    //         op,
    //     };
    //     self.team().barrier();
    //     match sched {
    //         Schedule::Static => self.sched_static(for_each),
    //         Schedule::Dynamic => self.sched_dynamic(for_each),
    //         Schedule::Chunk(size) => self.sched_chunk(for_each, size),
    //         Schedule::Guided => self.sched_guided(for_each),
    //         Schedule::WorkStealing => self.sched_work_stealing(for_each),
    //     }
    // }

    consumer_impl!(
        reduce<I, F>( iter: &I, op: F); 
        [DistIterReduceHandle<I::Item, F>];
        [I: DistributedIterator + 'static, I::Item: Dist + ArrayOps, F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static];
        [
            Reduce {
                iter: iter.iter_clone(Sealed),
                op,
            }
        ];
        [-> Option<I::Item>]);

    // consumer_impl!(
    //     reduce_async<I, T,F>( iter: &I, op: F); 
    //     [DistIterReduceHandle<T, F>];
    //     [I: DistributedIterator + 'static, I::Item: Future<Output = T> + Send + 'static, T: Dist + Send + ArrayOps, F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,];
    //     [
    //         ReduceAsync {
    //             iter: iter.iter_clone(Sealed),
    //             op,
    //             // _phantom: PhantomData,
    //         }
    //     ];
    //     [-> Option<T>]);

    // fn reduce<I, F>(&self, iter: &I, op: F) -> DistIterReduceHandle<I::Item, F>
    // where
    //     I: DistributedIterator + 'static,
    //     I::Item: Dist + ArrayOps,
    //     F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
    // {
    //     self.reduce_with_schedule(Schedule::Static, iter, op)
    // }

    // fn reduce_with_schedule<I, F>(
    //     &self,
    //     sched: Schedule,
    //     iter: &I,
    //     op: F,
    // ) -> DistIterReduceHandle<I::Item, F>
    // where
    //     I: DistributedIterator + 'static,
    //     I::Item: Dist + ArrayOps,
    //     F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
    // {
    //     let reduce = Reduce {
    //         iter: iter.iter_clone(Sealed),
    //         op,
    //     };
    //     match sched {
    //         Schedule::Static => self.sched_static(reduce),
    //         Schedule::Dynamic => self.sched_dynamic(reduce),
    //         Schedule::Chunk(size) => self.sched_chunk(reduce, size),
    //         Schedule::Guided => self.sched_guided(reduce),
    //         Schedule::WorkStealing => self.sched_work_stealing(reduce),
    //     }
    // }

    consumer_impl!(
        collect<I, A>( iter: &I, d: Distribution); 
        [DistIterCollectHandle<I::Item, A>];
        [I: DistributedIterator + 'static, I::Item: Dist + ArrayOps,  A: AsyncTeamFrom<(Vec<I::Item>, Distribution)> + SyncSend + Clone + 'static,];
        [
            Collect {
                iter: iter.iter_clone(Sealed).monotonic(),
                distribution: d,
                _phantom: PhantomData,
            }
        ];
        [-> A]);
    consumer_impl!(
        collect_async<I, A, B>( iter: &I, d: Distribution); 
        [DistIterCollectHandle<B, A>];
        [I: DistributedIterator + 'static, I::Item: Future<Output = B> + Send + 'static,B: Dist + ArrayOps,A: AsyncTeamFrom<(Vec<B>, Distribution)> + SyncSend + Clone + 'static,];
        [
            CollectAsync {
                iter: iter.iter_clone(Sealed).monotonic(),
                distribution: d,
                _phantom: PhantomData,
            }
        ];
        [-> A]);

    // fn collect<I, A>(&self, iter: &I, d: Distribution) -> DistIterCollectHandle<I::Item, A>
    // where
    //     I: DistributedIterator + 'static,
    //     I::Item: Dist + ArrayOps,
    //     A: AsyncTeamFrom<(Vec<I::Item>, Distribution)> + SyncSend + Clone + 'static,
    // {
    //     self.collect_with_schedule(Schedule::Static, iter, d)
    // }

    // fn collect_with_schedule<I, A>(
    //     &self,
    //     sched: Schedule,
    //     iter: &I,
    //     d: Distribution,
    // ) -> DistIterCollectHandle<I::Item, A>
    // where
    //     I: DistributedIterator + 'static,
    //     I::Item: Dist + ArrayOps,
    //     A: AsyncTeamFrom<(Vec<I::Item>, Distribution)> + SyncSend + Clone + 'static,
    // {
    //     let collect = Collect {
    //         iter: iter.iter_clone(Sealed).monotonic(),
    //         distribution: d,
    //         _phantom: PhantomData,
    //     };
    //     match sched {
    //         Schedule::Static => self.sched_static(collect),
    //         Schedule::Dynamic => self.sched_dynamic(collect),
    //         Schedule::Chunk(size) => self.sched_chunk(collect, size),
    //         Schedule::Guided => self.sched_guided(collect),
    //         Schedule::WorkStealing => self.sched_work_stealing(collect),
    //     }
    // }

    // fn collect_async<I, A, B>(&self, iter: &I, d: Distribution) -> DistIterCollectHandle<B, A>
    // where
    //     I: DistributedIterator,
    //     I::Item: Future<Output = B> + Send + 'static,
    //     B: Dist + ArrayOps,
    //     A: AsyncTeamFrom<(Vec<B>, Distribution)> + SyncSend + Clone + 'static,
    // {
    //     self.collect_async_with_schedule(Schedule::Static, iter, d)
    // }

    // fn collect_async_with_schedule<I, A, B>(
    //     &self,
    //     sched: Schedule,
    //     iter: &I,
    //     d: Distribution,
    // ) -> DistIterCollectHandle<B, A>
    // where
    //     I: DistributedIterator,
    //     I::Item: Future<Output = B> + Send + 'static,
    //     B: Dist + ArrayOps,
    //     A: AsyncTeamFrom<(Vec<B>, Distribution)> + SyncSend + Clone + 'static,
    // {
    //     let collect = CollectAsync {
    //         iter: iter.iter_clone(Sealed).monotonic(),
    //         distribution: d,
    //         _phantom: PhantomData,
    //     };
    //     match sched {
    //         Schedule::Static => self.sched_static(collect),
    //         Schedule::Dynamic => self.sched_dynamic(collect),
    //         Schedule::Chunk(size) => self.sched_chunk(collect, size),
    //         Schedule::Guided => self.sched_guided(collect),
    //         Schedule::WorkStealing => self.sched_work_stealing(collect),
    //     }
    // }
    consumer_impl!(
        count<I>( iter: &I); 
        [DistIterCountHandle];
        [I: DistributedIterator + 'static ];
        [
            Count {
                iter: iter.iter_clone(Sealed),
            }
        ];
        [-> usize]);
    // fn count<I>(&self, iter: &I) -> DistIterCountHandle
    // where
    //     I: DistributedIterator + 'static,
    // {
    //     self.count_with_schedule(Schedule::Static, iter)
    // }

    // fn count_with_schedule<I>(&self, sched: Schedule, iter: &I) -> DistIterCountHandle
    // where
    //     I: DistributedIterator + 'static,
    // {
    //     let count = Count {
    //         iter: iter.iter_clone(Sealed),
    //     };
    //     match sched {
    //         Schedule::Static => self.sched_static(count),
    //         Schedule::Dynamic => self.sched_dynamic(count),
    //         Schedule::Chunk(size) => self.sched_chunk(count, size),
    //         Schedule::Guided => self.sched_guided(count),
    //         Schedule::WorkStealing => self.sched_work_stealing(count),
    //     }
    // }

    consumer_impl!(
        sum<I>(iter: &I); 
        [DistIterSumHandle<I::Item>];
        [I: DistributedIterator + 'static, I::Item: Dist + ArrayOps + std::iter::Sum, ];
        [
            Sum {
                iter: iter.iter_clone(Sealed),
            }
        ];
        [-> I::Item]);

    // fn sum<I>(&self, iter: &I) -> DistIterSumHandle<I::Item>
    // where
    //     I: DistributedIterator + 'static,
    //     I::Item: Dist + ArrayOps + std::iter::Sum,
    // {
    //     self.sum_with_schedule(Schedule::Static, iter)
    // }

    // fn sum_with_schedule<I>(&self, sched: Schedule, iter: &I) -> DistIterSumHandle<I::Item>
    // where
    //     I: DistributedIterator + 'static,
    //     I::Item: Dist + ArrayOps + std::iter::Sum,
    // {
    //     let sum = Sum {
    //         iter: iter.iter_clone(Sealed),
    //     };
    //     match sched {
    //         Schedule::Static => self.sched_static(sum),
    //         Schedule::Dynamic => self.sched_dynamic(sum),
    //         Schedule::Chunk(size) => self.sched_chunk(sum, size),
    //         Schedule::Guided => self.sched_guided(sum),
    //         Schedule::WorkStealing => self.sched_work_stealing(sum),
    //     }
    // }

    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.data.team.clone()
    }
}

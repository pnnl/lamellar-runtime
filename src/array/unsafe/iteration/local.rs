use crate::active_messaging::SyncSend;
use crate::array::iterator::local_iterator::*;
use crate::array::iterator::private::*;
use crate::array::r#unsafe::{UnsafeArray, UnsafeArrayInner};
use crate::array::{ArrayOps, AsyncTeamFrom, Distribution};

use crate::array::iterator::Schedule;
use crate::lamellar_team::LamellarTeamRT;
use crate::memregion::Dist;

use core::marker::PhantomData;
use futures_util::Future;
use paste::paste;
use std::pin::Pin;
use std::sync::Arc;

impl<T: Dist> LocalIteratorLauncher for UnsafeArray<T> {}

macro_rules! consumer_impl {
    ($name:ident<$($generics:ident),*>($($arg:ident : $arg_ty:ty),*); [$return_type:ident$(<$($ret_gen:ty),*>)?]; [$($bounds:tt)+]; [$($am:tt)*]; [$($lock:tt)*] ) => {
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
                self.data.team.team_counters.inc_send_req(1);
                self.data.team.world_counters.inc_send_req(1);
                self.data.task_group.counters.inc_send_req(1);
                let lock =  $($lock)*;
                let inner = self.clone();
                let reqs_future = Box::pin(async move{
                    let reqs = match sched {
                        Schedule::Static => inner.sched_static(am),
                        Schedule::Dynamic => inner.sched_dynamic(am),
                        Schedule::Chunk(size) => inner.sched_chunk(am,size),
                        Schedule::Guided => inner.sched_guided(am),
                        Schedule::WorkStealing => inner.sched_work_stealing(am),
                    };
                    inner.data.team.team_counters.inc_launched(1);
                    inner.data.team.world_counters.inc_launched(1);
                    inner.data.task_group.counters.inc_launched(1);
                    reqs
                });
                $return_type::new(lock,reqs_future,self)
            }
        }
    };
}

impl LocalIteratorLauncher for UnsafeArrayInner {
    fn local_global_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        // println!("global index cs:{:?}",chunk_size);
        if chunk_size == 1 {
            self.global_index_from_local(index)
        } else {
            Some(self.global_index_from_local(index * chunk_size)? / chunk_size)
        }
    }

    fn local_subarray_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        if chunk_size == 1 {
            self.subarray_index_from_local(index)
        } else {
            Some(self.subarray_index_from_local(index * chunk_size)? / chunk_size)
        }
    }

    consumer_impl!(
        for_each<I, F>(iter: &I, op: F);
        [LocalIterForEachHandle];
        [I: LocalIterator + 'static, F: Fn(I::Item) + SyncSend + Clone + 'static];
        [
            ForEach {
                iter: iter.iter_clone(Sealed),
                op,
            }
        ];
        [iter.lock_if_needed(Sealed)]
    );

    consumer_impl!(
        for_each_async<I, F, Fut>(iter: &I, op: F);
        [LocalIterForEachHandle];
        [I: LocalIterator + 'static, F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static, Fut: Future<Output = ()> + Send + 'static];
        [
            ForEachAsync {
                iter: iter.iter_clone(Sealed),
                op,
            }
        ];
        [iter.lock_if_needed(Sealed)]
    );

    consumer_impl!(
        reduce<I, F>( iter: &I, op: F);
        [LocalIterReduceHandle<I::Item, F>];
        [I: LocalIterator + 'static, I::Item: SyncSend + Copy, F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static];
        [
            Reduce {
                iter: iter.iter_clone(Sealed),
                op,
            }
        ];
        [iter.lock_if_needed(Sealed)]
    );

    consumer_impl!(
        collect<I, A>( iter: &I, d: Distribution);
        [LocalIterCollectHandle<I::Item, A>];
        [I: LocalIterator + 'static, I::Item: Dist + ArrayOps,  A: AsyncTeamFrom<(Vec<I::Item>, Distribution)> + SyncSend + Clone + 'static,];
        [
            Collect {
                iter: iter.iter_clone(Sealed).monotonic(),
                distribution: d,
                _phantom: PhantomData,
            }
        ];
        [iter.lock_if_needed(Sealed)]
    );

    consumer_impl!(
        collect_async<I, A, B>( iter: &I, d: Distribution);
        [LocalIterCollectHandle<B, A>];
        [I: LocalIterator + 'static, I::Item: Future<Output = B> + Send + 'static,B: Dist + ArrayOps,A: AsyncTeamFrom<(Vec<B>, Distribution)> + SyncSend + Clone + 'static,];
        [
            CollectAsync {
                iter: iter.iter_clone(Sealed).monotonic(),
                distribution: d,
                _phantom: PhantomData,
            }
        ];
        [iter.lock_if_needed(Sealed)]
    );

    consumer_impl!(
        count<I>( iter: &I);
        [LocalIterCountHandle];
        [I: LocalIterator + 'static ];
        [
            Count {
                iter: iter.iter_clone(Sealed),
            }
        ];
        [iter.lock_if_needed(Sealed)]
    );

    consumer_impl!(
        sum<I>(iter: &I);
        [LocalIterSumHandle<I::Item>];
        [I: LocalIterator + 'static, I::Item: SyncSend + for<'a> std::iter::Sum<&'a I::Item> + std::iter::Sum<I::Item>  , ];
        [
            Sum {
                iter: iter.iter_clone(Sealed),
            }
        ];
        [iter.lock_if_needed(Sealed)]
    );

    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.data.team.clone()
    }
}

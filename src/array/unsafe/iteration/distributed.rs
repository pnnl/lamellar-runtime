use crate::active_messaging::SyncSend;
use crate::array::iterator::distributed_iterator::*;
use crate::array::iterator::private::Sealed;
use crate::array::iterator::Schedule;
use crate::array::r#unsafe::{UnsafeArray, UnsafeArrayInner};
use crate::array::{ArrayOps, AsyncTeamFrom, Distribution, InnerArray};
use crate::lamellar_env::LamellarEnv;
use crate::memregion::Dist;
use crate::LamellarTeam;

use core::marker::PhantomData;
use futures_util::Future;
use paste::paste;
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
                // set req counters so that wait all works
                self.data.team.team_counters.inc_send_req(1);
                self.data.team.world_counters.inc_send_req(1);
                self.data.task_group.counters.inc_send_req(1);


                let lock =  $($lock)*;
                let inner = self.clone();
                let reqs_future = Box::pin(async move{

                    // println!("barrier id {:?} entering dist iter sched {:?} {:?} {:?}",barrier_id, inner.data.team.team_counters.outstanding_reqs.load(Ordering::SeqCst), inner.data.team.world_counters.outstanding_reqs.load(Ordering::SeqCst), inner.data.task_group.counters.outstanding_reqs.load(Ordering::SeqCst));
                    let reqs = match sched {
                        Schedule::Static => inner.sched_static(am),
                        Schedule::Dynamic => inner.sched_dynamic(am),
                        Schedule::Chunk(size) => inner.sched_chunk(am,size),
                        Schedule::Guided => inner.sched_guided(am),
                        Schedule::WorkStealing => inner.sched_work_stealing(am),
                    };

                    // increment launch counters to match req countersk
                    inner.data.team.team_counters.inc_launched(1);
                    inner.data.team.world_counters.inc_launched(1);
                    inner.data.task_group.counters.inc_launched(1);
                    // println!("barrier id {:?} done with dist iter sched {:?} {:?} {:?}",barrier_id,inner.data.team.team_counters.outstanding_reqs.load(Ordering::SeqCst), inner.data.team.world_counters.outstanding_reqs.load(Ordering::SeqCst), inner.data.task_group.counters.outstanding_reqs.load(Ordering::SeqCst));
                    reqs
                });
                $return_type::new(lock,reqs_future,self)
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
        [iter.lock_if_needed(Sealed)]
    );

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
        [iter.lock_if_needed(Sealed)]
    );

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
        [iter.lock_if_needed(Sealed)]
    );

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
        [iter.lock_if_needed(Sealed)]
    );
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
        [iter.lock_if_needed(Sealed)]
    );

    consumer_impl!(
        count<I>( iter: &I);
        [DistIterCountHandle];
        [I: DistributedIterator + 'static ];
        [
            Count {
                iter: iter.iter_clone(Sealed),
            }
        ];
        [iter.lock_if_needed(Sealed)]
    );

    consumer_impl!(
        sum<I>(iter: &I);
        [DistIterSumHandle<I::Item>];
        [I: DistributedIterator + 'static, I::Item: Dist + ArrayOps + std::iter::Sum, ];
        [
            Sum {
                iter: iter.iter_clone(Sealed),
            }
        ];
        [iter.lock_if_needed(Sealed)]
    );

    fn team(&self) -> Arc<LamellarTeam> {
        self.data.team.team()
    }
}

use crate::active_messaging::SyncSend;
use crate::array::iterator::distributed_iterator::*;
use crate::array::iterator::private::Sealed;
use crate::array::r#unsafe::{UnsafeArray, UnsafeArrayInner};
use crate::array::{ArrayOps, AsyncTeamFrom, Distribution, InnerArray};
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
    ($name:ident<$($generics:ident),*>($($arg:ident : $arg_ty:ty),*); [$return_type:ident$(<$($ret_gen:ty),*>)?]; [$($bounds:tt)+]; [$($am:tt)*]; [ $($blocking_ret:tt)*] ) => {
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

            // fn [<spawn_ $name>]<$($generics),*>(&self, $($arg : $arg_ty),*) -> LamellarTask<$($blocking_ret)*>
            // where
            // $($bounds)+
            // {

            //     self.[<spawn_ $name _with_schedule>](Schedule::Static, $($arg),*)
            // }


            // fn [<spawn_ $name _with_schedule >]<$($generics),*>(
            //     &self,
            //     sched: Schedule,
            //     $($arg : $arg_ty),*
            // ) -> LamellarTask<$($blocking_ret)*>
            // where
            //     $($bounds)+
            // {
            //     self.data.team.scheduler.spawn_task(self.[<$name _with_schedule>](sched, $($arg),*))
            // }

            // fn [<blocking_ $name>]<$($generics),*>(&self, $($arg : $arg_ty),*) -> $($blocking_ret)*
            // where
            // $($bounds)+
            // {

            //     self.[<blocking_ $name _with_schedule>](Schedule::Static, $($arg),*)
            // }


            // fn [<blocking_ $name _with_schedule >]<$($generics),*>(
            //     &self,
            //     sched: Schedule,
            //     $($arg : $arg_ty),*
            // ) -> $($blocking_ret)*
            // where
            //     $($bounds)+
            // {
            //     if std::thread::current().id() != *crate::MAIN_THREAD {
            //         let name = stringify!{$name};
            //         let msg = format!("
            //             [LAMELLAR WARNING] You are calling `blocking_{name}[_with_schedule]` from within an async context which may lead to deadlock, it is recommended that you use `{name}[_with_schedule]().await;` instead! 
            //             Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {:?}", std::backtrace::Backtrace::capture()
            //         );
            //         if let Some(val) = config().blocking_call_warning {
            //             if val {
            //                 println!("{msg}");
            //             }
            //         } else {
            //             println!("{msg}");
            //         }
            //     }
            //     let am = $($am)*;
            //     self.data.team.barrier.tasking_barrier();
            //     let inner = self.clone();
            //     let reqs = match sched {
            //         Schedule::Static => inner.sched_static(am),
            //         Schedule::Dynamic => inner.sched_dynamic(am),
            //         Schedule::Chunk(size) => inner.sched_chunk(am,size),
            //         Schedule::Guided => inner.sched_guided(am),
            //         Schedule::WorkStealing => inner.sched_work_stealing(am),
            //     };
            //     reqs.blocking_wait()
            // }
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
    [()]);

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
        [()]
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
        [Option<I::Item>]);

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
        [A]);
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
        [A]);

    consumer_impl!(
        count<I>( iter: &I); 
        [DistIterCountHandle];
        [I: DistributedIterator + 'static ];
        [
            Count {
                iter: iter.iter_clone(Sealed),
            }
        ];
        [usize]);

    consumer_impl!(
        sum<I>(iter: &I); 
        [DistIterSumHandle<I::Item>];
        [I: DistributedIterator + 'static, I::Item: Dist + ArrayOps + std::iter::Sum, ];
        [
            Sum {
                iter: iter.iter_clone(Sealed),
            }
        ];
        [I::Item]);

    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.data.team.clone()
    }
}

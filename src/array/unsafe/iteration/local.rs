use crate::active_messaging::SyncSend;
use crate::array::iterator::local_iterator::*;
use crate::array::r#unsafe::UnsafeArray;
use crate::array::{LamellarArray,Distribution,ArrayOps,TeamFrom};

use crate::memregion::Dist;
use crate::array::iterator::Schedule;
use crate::lamellar_team::LamellarTeamRT;

use core::marker::PhantomData;
use futures::Future;
use std::pin::Pin;
use std::sync::Arc;


impl<T: Dist> LocalIteratorLauncher for UnsafeArray<T> {
    fn local_global_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        // println!("global index cs:{:?}",chunk_size);
        if chunk_size == 1 {
            self.inner.global_index_from_local(index)
        } else {
            Some(self.inner.global_index_from_local(index * chunk_size)? / chunk_size)
        }
    }

    fn local_subarray_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        if chunk_size == 1 {
            self.inner.subarray_index_from_local(index)
        } else {
            Some(self.inner.subarray_index_from_local(index * chunk_size)? / chunk_size)
        }
    }

    fn local_for_each<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        self.local_for_each_with_schedule(Schedule::Static, iter, op)
    }

    fn local_for_each_with_schedule<I, F>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        let for_each = ForEach{
            iter: iter.clone(),
            op,
        };
        match sched {
            Schedule::Static => self.sched_static(for_each ),
            Schedule::Dynamic => self.sched_dynamic(for_each),
            Schedule::Chunk(size) => self.sched_chunk(for_each, size),
            Schedule::Guided => self.sched_guided(for_each),
            Schedule::WorkStealing => self.sched_work_stealing(for_each),
        }
    }

    fn local_for_each_async<I, F, Fut>(
        &self,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.local_for_each_async_with_schedule(Schedule::Static, iter, op)
    }

    fn local_for_each_async_with_schedule<I, F, Fut>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let for_each = ForEachAsync{
            iter: iter.clone(),
            op: op.clone(),
        };
        match sched {
            Schedule::Static => self.sched_static(for_each ),
            Schedule::Dynamic => self.sched_dynamic(for_each),
            Schedule::Chunk(size) => self.sched_chunk(for_each, size),
            Schedule::Guided => self.sched_guided(for_each),
            Schedule::WorkStealing => self.sched_work_stealing(for_each),
        }
    }

    fn local_reduce<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = Option<I::Item>> + Send>>
    where
        I: LocalIterator + 'static,
        I::Item: SyncSend,
        F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
    {
        self.local_reduce_with_schedule(Schedule::Static, iter, op)
    }

    fn local_reduce_with_schedule<I, F>(&self, sched: Schedule, iter: &I, op: F) -> Pin<Box<dyn Future<Output = Option<I::Item>> + Send>>
    where
        I: LocalIterator + 'static,
        I::Item: SyncSend,
        F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
    {
        let reduce = Reduce{
            iter: iter.clone(),
            op,
        };
        match sched {
            Schedule::Static => self.sched_static(reduce ),
            Schedule::Dynamic => self.sched_dynamic(reduce),
            Schedule::Chunk(size) => self.sched_chunk(reduce, size),
            Schedule::Guided => self.sched_guided(reduce),
            Schedule::WorkStealing => self.sched_work_stealing(reduce),
        }
    }

    // fn local_reduce_async<I, F, Fut>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = Option<I::Item>> + Send>>
    // where
    //     I: LocalIterator + 'static,
    //     I::Item: SyncSend,
    //     F: Fn(I::Item, I::Item) -> Fut + SyncSend + Clone + 'static,
    //     Fut: Future<Output = I::Item> + SyncSend + Clone + 'static
    // {
    //     self.local_reduce_async_with_schedule(Schedule::Static, iter, op)
    // }

    // fn local_reduce_async_with_schedule<I, F, Fut>(&self, sched: Schedule, iter: &I, op: F) -> Pin<Box<dyn Future<Output = Option<I::Item>> + Send>>
    // where
    //     I: LocalIterator + 'static,
    //     I::Item: SyncSend,
    //     F: Fn(I::Item, I::Item) -> Fut + SyncSend + Clone + 'static,
    //     Fut: Future<Output = I::Item> + SyncSend + Clone + 'static,
    // {
    //     let reduce = ReduceAsync{
    //         iter: iter.clone(),
    //         op,
    //         _phantom: PhantomData,
    //     };
    //     match sched {
    //         Schedule::Static => self.sched_static(reduce ),
    //         Schedule::Dynamic => self.sched_dynamic(reduce),
    //         Schedule::Chunk(size) => self.sched_chunk(reduce, size),
    //         Schedule::Guided => self.sched_guided(reduce),
    //         Schedule::WorkStealing => self.sched_work_stealing(reduce),
    //     }
    // }

    fn local_collect<I, A>(&self, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I:  LocalIterator + 'static,
        I::Item: Dist + ArrayOps,
        A: for<'a>  TeamFrom<(&'a Vec<I::Item>,Distribution)> + SyncSend + Clone + 'static,
    {
        self.local_collect_with_schedule(Schedule::Static,iter,d)
    }

    fn local_collect_with_schedule<I, A>(&self, sched: Schedule, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I:  LocalIterator + 'static,
        I::Item: Dist + ArrayOps,
        A: for<'a>  TeamFrom<(&'a Vec<I::Item>,Distribution)> + SyncSend + Clone + 'static,
    {
        let collect = Collect{
            iter: iter.clone().monotonic(),
            distribution: d,
            _phantom: PhantomData,
        };
        match sched {
            Schedule::Static => self.sched_static(collect ),
            Schedule::Dynamic => self.sched_dynamic(collect),
            Schedule::Chunk(size) => self.sched_chunk(collect, size),
            Schedule::Guided => self.sched_guided(collect),
            Schedule::WorkStealing => self.sched_work_stealing(collect),
        }
    }

    // fn local_collect_async<I, A, B>(&self, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    // where
    //     I:  LocalIterator + 'static,
    //    I::Item: Future<Output = B> + Send  + 'static,
    //     B: Dist + ArrayOps,
    //     A: From<UnsafeArray<B>> + SyncSend  + Clone +  'static,
    // {
    //     self.local_collect_async_with_schedule(Schedule::Static,iter,d)
    // }

    // fn local_collect_async_with_schedule<I, A, B>(&self, sched: Schedule, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    // where
    //     I:  LocalIterator + 'static,
    //    I::Item: Future<Output = B> + Send  + 'static,
    //     B: Dist + ArrayOps,
    //     A: From<UnsafeArray<B>> + SyncSend  + Clone +  'static,
    // {
    //     let collect = CollectAsync{
    //         iter: iter.clone(),
    //         distribution: d,
    //         _phantom: PhantomData,
    //     };
    //     match sched {
    //         Schedule::Static => self.sched_static(collect ),
    //         Schedule::Dynamic => self.sched_dynamic(collect),
    //         Schedule::Chunk(size) => self.sched_chunk(collect, size),
    //         Schedule::Guided => self.sched_guided(collect),
    //         Schedule::WorkStealing => self.sched_work_stealing(collect),
    //     }
    // }

    fn local_count<I>(&self, iter: &I) -> Pin<Box<dyn Future<Output = usize> + Send>>
    where
        I:  LocalIterator + 'static
    {
        self.local_count_with_schedule(Schedule::Static,iter)
    }

    fn local_count_with_schedule<I>(&self, sched: Schedule, iter: &I) -> Pin<Box<dyn Future<Output = usize> + Send>>
    where
        I:  LocalIterator + 'static,
    {
        let count = Count{
            iter: iter.clone(),
        };
        match sched {
            Schedule::Static => self.sched_static(count ),
            Schedule::Dynamic => self.sched_dynamic(count),
            Schedule::Chunk(size) => self.sched_chunk(count, size),
            Schedule::Guided => self.sched_guided(count),
            Schedule::WorkStealing => self.sched_work_stealing(count),
        }
    }

    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.inner.data.team.clone()
    }
}

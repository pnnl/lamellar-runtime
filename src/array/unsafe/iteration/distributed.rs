use crate::active_messaging::SyncSend;
use crate::array::iterator::distributed_iterator::*;
use crate::array::r#unsafe::UnsafeArray;
use crate::array::{LamellarArray,Distribution,ArrayOps,TeamFrom};

use crate::memregion::Dist;
use crate::array::iterator::Schedule;
use crate::lamellar_team::LamellarTeamRT;

use core::marker::PhantomData;
use futures::Future;
use std::pin::Pin;
use std::sync::Arc;

impl<T: Dist> DistIteratorLauncher for UnsafeArray<T> {
    fn global_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        // println!("global index cs:{:?}",chunk_size);
        if chunk_size == 1 {
            self.inner.global_index_from_local(index)
        } else {
            Some(self.inner.global_index_from_local(index * chunk_size)? / chunk_size)
        }
    }

    fn subarray_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        if chunk_size == 1 {
            self.inner.subarray_index_from_local(index)
        } else {
            Some(self.inner.subarray_index_from_local(index * chunk_size)? / chunk_size)
        }
    }

    // fn subarray_pe_and_offset_for_global_index(&self, index: usize, chunk_size: usize) -> Option<(usize,usize)> {
    //     if chunk_size == 1 {
    //         Some(self.calc_pe_and_offset(index))
    //     } else {
    //         Some(self.calc_pe_and_offset(index * chunk_size)? / chunk_size)
    //     }
    // }

    fn for_each<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        self.for_each_with_schedule(Schedule::Static, iter, op)
    }

    fn for_each_with_schedule<I, F>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        let for_each = ForEach{
            iter: iter.clone(),
            op,
        };
        self.barrier();
        match sched {
            Schedule::Static => self.sched_static(for_each),
            Schedule::Dynamic => self.sched_dynamic(for_each),
            Schedule::Chunk(size) => self.sched_chunk(for_each, size),
            Schedule::Guided => self.sched_guided(for_each),
            Schedule::WorkStealing => self.sched_work_stealing(for_each),
        }
    }

    fn for_each_async<I, F, Fut>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.for_each_async_with_schedule(Schedule::Static, iter, op)
    }

    fn for_each_async_with_schedule<I, F, Fut>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let for_each = ForEachAsync{
            iter: iter.clone(),
            op,
        };
        self.barrier();
        match sched {
            Schedule::Static => self.sched_static(for_each),
            Schedule::Dynamic => self.sched_dynamic(for_each),
            Schedule::Chunk(size) => self.sched_chunk(for_each, size),
            Schedule::Guided => self.sched_guided(for_each),
            Schedule::WorkStealing => self.sched_work_stealing(for_each),
        }
    }

    fn collect<I, A>(&self, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: DistributedIterator + 'static,
        I::Item: Dist + ArrayOps,
        A: for<'a>  TeamFrom<(&'a Vec<I::Item>,Distribution)> + SyncSend + Clone + 'static,
    {
        self.collect_with_schedule(Schedule::Static,iter,d)
    }

    fn collect_with_schedule<I, A>(&self, sched: Schedule, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: DistributedIterator + 'static,
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

    fn collect_async<I, A, B>(
        &self,
        iter: &I,
        d: Distribution,
    ) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: DistributedIterator,
       I::Item: Future<Output = B> + Send  + 'static,
        B: Dist + ArrayOps,
        A: for<'a> TeamFrom<(&'a Vec<B>,Distribution)> + SyncSend  + Clone +  'static,
    {
        self.collect_async_with_schedule(Schedule::Static,iter,d)
    }

    fn collect_async_with_schedule<I, A, B>(
        &self,
        sched: Schedule,
        iter: &I,
        d: Distribution,
    ) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: DistributedIterator,
       I::Item: Future<Output = B> + Send  + 'static,
        B: Dist + ArrayOps,
        A: for<'a> TeamFrom<(&'a Vec<B>,Distribution)> + SyncSend  + Clone +  'static,
    {
        let collect = CollectAsync{
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

    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.inner.data.team.clone()
    }
}



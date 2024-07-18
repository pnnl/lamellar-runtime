use crate::active_messaging::SyncSend;
use crate::array::iterator::local_iterator::*;
use crate::array::iterator::private::*;
use crate::array::r#unsafe::UnsafeArray;
use crate::array::{ArrayOps, AsyncTeamFrom, Distribution};

use crate::array::iterator::Schedule;
use crate::lamellar_team::LamellarTeamRT;
use crate::memregion::Dist;

use core::marker::PhantomData;
use futures_util::Future;
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

    fn for_each<I, F>(&self, iter: &I, op: F) -> LocalIterForEachHandle
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        self.for_each_with_schedule(Schedule::Static, iter, op)
    }

    fn for_each_with_schedule<I, F>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> LocalIterForEachHandle
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        let for_each = ForEach {
            iter: iter.iter_clone(Sealed),
            op,
        };
        match sched {
            Schedule::Static => self.inner.sched_static(for_each),
            Schedule::Dynamic => self.inner.sched_dynamic(for_each),
            Schedule::Chunk(size) => self.inner.sched_chunk(for_each, size),
            Schedule::Guided => self.inner.sched_guided(for_each),
            Schedule::WorkStealing => self.inner.sched_work_stealing(for_each),
        }
    }

    fn for_each_async<I, F, Fut>(&self, iter: &I, op: F) -> LocalIterForEachHandle
    where
        I: LocalIterator + 'static,
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
    ) -> LocalIterForEachHandle
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let for_each = ForEachAsync {
            iter: iter.iter_clone(Sealed),
            op: op.clone(),
        };
        match sched {
            Schedule::Static => self.inner.sched_static(for_each),
            Schedule::Dynamic => self.inner.sched_dynamic(for_each),
            Schedule::Chunk(size) => self.inner.sched_chunk(for_each, size),
            Schedule::Guided => self.inner.sched_guided(for_each),
            Schedule::WorkStealing => self.inner.sched_work_stealing(for_each),
        }
    }

    fn reduce<I, F>(&self, iter: &I, op: F) -> LocalIterReduceHandle<I::Item, F>
    where
        I: LocalIterator + 'static,
        I::Item: SyncSend,
        F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
    {
        self.reduce_with_schedule(Schedule::Static, iter, op)
    }

    fn reduce_with_schedule<I, F>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> LocalIterReduceHandle<I::Item, F>
    where
        I: LocalIterator + 'static,
        I::Item: SyncSend,
        F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
    {
        let reduce = Reduce {
            iter: iter.iter_clone(Sealed),
            op,
        };
        match sched {
            Schedule::Static => self.inner.sched_static(reduce),
            Schedule::Dynamic => self.inner.sched_dynamic(reduce),
            Schedule::Chunk(size) => self.inner.sched_chunk(reduce, size),
            Schedule::Guided => self.inner.sched_guided(reduce),
            Schedule::WorkStealing => self.inner.sched_work_stealing(reduce),
        }
    }

    fn collect<I, A>(&self, iter: &I, d: Distribution) -> LocalIterCollectHandle<I::Item, A>
    where
        I: LocalIterator + 'static,
        I::Item: Dist + ArrayOps,
        A: AsyncTeamFrom<(Vec<I::Item>, Distribution)> + SyncSend + Clone + 'static,
    {
        self.collect_with_schedule(Schedule::Static, iter, d)
    }

    fn collect_with_schedule<I, A>(
        &self,
        sched: Schedule,
        iter: &I,
        d: Distribution,
    ) -> LocalIterCollectHandle<I::Item, A>
    where
        I: LocalIterator + 'static,
        I::Item: Dist + ArrayOps,
        A: AsyncTeamFrom<(Vec<I::Item>, Distribution)> + SyncSend + Clone + 'static,
    {
        let collect = Collect {
            iter: iter.iter_clone(Sealed).monotonic(),
            distribution: d,
            _phantom: PhantomData,
        };
        match sched {
            Schedule::Static => self.inner.sched_static(collect),
            Schedule::Dynamic => self.inner.sched_dynamic(collect),
            Schedule::Chunk(size) => self.inner.sched_chunk(collect, size),
            Schedule::Guided => self.inner.sched_guided(collect),
            Schedule::WorkStealing => self.inner.sched_work_stealing(collect),
        }
    }

    fn count<I>(&self, iter: &I) -> LocalIterCountHandle
    where
        I: LocalIterator + 'static,
    {
        self.count_with_schedule(Schedule::Static, iter)
    }

    fn count_with_schedule<I>(&self, sched: Schedule, iter: &I) -> LocalIterCountHandle
    where
        I: LocalIterator + 'static,
    {
        let count = Count {
            iter: iter.iter_clone(Sealed),
        };
        match sched {
            Schedule::Static => self.inner.sched_static(count),
            Schedule::Dynamic => self.inner.sched_dynamic(count),
            Schedule::Chunk(size) => self.inner.sched_chunk(count, size),
            Schedule::Guided => self.inner.sched_guided(count),
            Schedule::WorkStealing => self.inner.sched_work_stealing(count),
        }
    }

    fn sum<I>(&self, iter: &I) -> LocalIterSumHandle<I::Item>
    where
        I: LocalIterator + 'static,
        I::Item: SyncSend + std::iter::Sum,
    {
        self.sum_with_schedule(Schedule::Static, iter)
    }

    fn sum_with_schedule<I>(&self, sched: Schedule, iter: &I) -> LocalIterSumHandle<I::Item>
    where
        I: LocalIterator + 'static,
        I::Item: SyncSend + std::iter::Sum,
    {
        let sum = Sum {
            iter: iter.iter_clone(Sealed),
        };
        match sched {
            Schedule::Static => self.inner.sched_static(sum),
            Schedule::Dynamic => self.inner.sched_dynamic(sum),
            Schedule::Chunk(size) => self.inner.sched_chunk(sum, size),
            Schedule::Guided => self.inner.sched_guided(sum),
            Schedule::WorkStealing => self.inner.sched_work_stealing(sum),
        }
    }

    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.inner.data.team.clone()
    }
}

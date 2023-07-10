use crate::array::read_only::*;

use crate::array::iterator::distributed_iterator::{DistIter,DistIteratorLauncher,IndexedDistributedIterator,DistributedIterator};
use crate::array::iterator::local_iterator::{LocalIter,LocalIteratorLauncher,IndexedLocalIterator,LocalIterator};
use crate::array::iterator::one_sided_iterator::OneSidedIter;
use crate::array::iterator::{LamellarArrayIterators, Schedule};
use crate::array::*;
use crate::memregion::Dist;

impl<T: Dist> LamellarArrayIterators<T> for ReadOnlyArray<T> {
    // type Array = ReadOnlyArray<T>;
    type DistIter = DistIter<'static, T, Self>;
    type LocalIter = LocalIter<'static, T, Self>;
    type OnesidedIter = OneSidedIter<'static, T, Self>;
    fn dist_iter(&self) -> Self::DistIter {
        DistIter::new(self.clone().into(), 0, 0)
    }

    fn local_iter(&self) -> Self::LocalIter {
        LocalIter::new(self.clone().into(), 0, 0)
    }

    fn onesided_iter(&self) -> Self::OnesidedIter {
        OneSidedIter::new(self.clone().into(), self.array.team_rt().clone(), 1)
    }

    fn buffered_onesided_iter(&self, buf_size: usize) -> Self::OnesidedIter {
        OneSidedIter::new(
            self.clone().into(),
            self.array.team_rt().clone(),
            std::cmp::min(buf_size, self.len()),
        )
    }
}

impl<T: Dist> DistIteratorLauncher for ReadOnlyArray<T> {
    fn global_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        self.array.global_index_from_local(index, chunk_size)
    }

    fn subarray_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        self.array.subarray_index_from_local(index, chunk_size)
    }

    // fn subarray_pe_and_offset_for_global_index(&self, index: usize, chunk_size: usize) -> Option<(usize,usize)> {
    //     self.array.subarray_pe_and_offset_for_global_index(index, chunk_size)
    // }

    fn for_each<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        DistIteratorLauncher::for_each(&self.array, iter, op)
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
        DistIteratorLauncher::for_each_with_schedule(&self.array, sched, iter, op)
    }
    fn for_each_async<I, F, Fut>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        DistIteratorLauncher::for_each_async(&self.array,iter, op)
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
        DistIteratorLauncher::for_each_async_with_schedule(&self.array, sched, iter, op)
    }

    fn collect<I, A>(&self, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: DistributedIterator + 'static,
        I::Item: Dist + ArrayOps,
        A: for<'a>  TeamFrom<(&'a Vec<I::Item>,Distribution)> + SyncSend + Clone + 'static,
    {
        DistIteratorLauncher::collect(&self.array, iter, d)
    }

    fn collect_with_schedule<I, A>(&self,sched: Schedule, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: DistributedIterator + 'static,
        I::Item: Dist + ArrayOps,
        A: for<'a>  TeamFrom<(&'a Vec<I::Item>,Distribution)> + SyncSend + Clone + 'static,
    {
        DistIteratorLauncher::collect_with_schedule(&self.array, sched, iter, d)
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
        DistIteratorLauncher::collect_async(&self.array, iter, d)
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
        DistIteratorLauncher::collect_async_with_schedule(&self.array, sched,iter, d)
    }
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team_rt().clone()
    }
}

impl<T: Dist> LocalIteratorLauncher for ReadOnlyArray<T> {
    fn local_global_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        self.array.local_global_index_from_local(index, chunk_size)
    }

    fn local_subarray_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        self.array
            .local_subarray_index_from_local(index, chunk_size)
    }

    fn for_each<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        LocalIteratorLauncher::for_each(&self.array,iter, op)
    }
    fn for_each_with_schedule<I, F>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        LocalIteratorLauncher::for_each_with_schedule(&self.array,sched, iter, op)
    }
    fn for_each_async<I, F, Fut>(
        &self,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        LocalIteratorLauncher::for_each_async(&self.array,iter, op)
    }
    fn for_each_async_with_schedule<I, F, Fut>(
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
        LocalIteratorLauncher::for_each_async_with_schedule(&self.array, sched, iter, op)
    }

    fn reduce<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = Option<I::Item>> + Send>>
    where
        I: LocalIterator + 'static,
        I::Item: SyncSend,
        F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
    {
        LocalIteratorLauncher::reduce(&self.array, iter, op)
    }

    fn reduce_with_schedule<I, F>(&self, sched: Schedule, iter: &I, op: F) -> Pin<Box<dyn Future<Output = Option<I::Item>> + Send>>
    where
        I: LocalIterator + 'static,
        I::Item: SyncSend,
        F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
    {
        LocalIteratorLauncher::reduce_with_schedule(&self.array, sched, iter, op)
    }

    // fn reduce_async<I, F, Fut>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = Option<I::Item>> + Send>>
    // where
    //     I: LocalIterator + 'static,
    //     I::Item: SyncSend,
    //     F: Fn(I::Item, I::Item) -> Fut + SyncSend + Clone + 'static,
    //     Fut: Future<Output = I::Item> + SyncSend + Clone + 'static
    // {
    //     self.array.reduce_async(iter, op)
    // }

    // fn reduce_async_with_schedule<I, F, Fut>(&self, sched: Schedule, iter: &I, op: F) -> Pin<Box<dyn Future<Output = Option<I::Item>> + Send>>
    // where
    //     I: LocalIterator + 'static,
    //     I::Item: SyncSend,
    //     F: Fn(I::Item, I::Item) -> Fut + SyncSend + Clone + 'static,
    //     Fut: Future<Output = I::Item> + SyncSend + Clone + 'static
    // {
    //     self.array.reduce_async_with_schedule(sched, iter, op)
    // }

    fn collect<I, A>(&self, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: LocalIterator + 'static,
        I::Item: Dist + ArrayOps,
        A: for<'a>  TeamFrom<(&'a Vec<I::Item>,Distribution)> + SyncSend + Clone + 'static,
    {
        LocalIteratorLauncher::collect(&self.array, iter, d)
    }

    fn collect_with_schedule<I, A>(&self, sched: Schedule, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: LocalIterator + 'static,
        I::Item: Dist + ArrayOps,
        A: for<'a>  TeamFrom<(&'a Vec<I::Item>,Distribution)> + SyncSend + Clone + 'static,
    {
        LocalIteratorLauncher::collect_with_schedule(&self.array, sched, iter, d)
    }

// fn collect_async<I, A, B>(
    //     &self,
    //     iter: &I,
    //     d: Distribution,
    // ) -> Pin<Box<dyn Future<Output = A> + Send>>
    // where
    //     I: LocalIterator + 'static,
    //    I::Item: Future<Output = B> + Send  + 'static,
    //     B: Dist + ArrayOps,
    //     A: From<UnsafeArray<B>> + SyncSend  + Clone +  'static,
    // {
    //     self.array.collect_async(iter, d)
    // }

    // fn collect_async_with_schedule<I, A, B>(
    //     &self,
    //     sched: Schedule,
    //     iter: &I,
    //     d: Distribution,
    // ) -> Pin<Box<dyn Future<Output = A> + Send>>
    // where
    //     I: LocalIterator + 'static,
    //    I::Item: Future<Output = B> + Send  + 'static,
    //     B: Dist + ArrayOps,
    //     A: From<UnsafeArray<B>> + SyncSend  + Clone +  'static,
    // {
    //     self.array.collect_async_with_schedule(sched, iter, d)
    // }

    fn count<I>(&self, iter: &I) -> Pin<Box<dyn Future<Output = usize> + Send>>
    where
        I: LocalIterator + 'static
    {
        LocalIteratorLauncher::count(&self.array, iter)
    }
    
    fn count_with_schedule<I>(&self, sched: Schedule, iter: &I) -> Pin<Box<dyn Future<Output = usize> + Send>>
    where
        I: LocalIterator + 'static
    {
        LocalIteratorLauncher::count_with_schedule(&self.array, sched, iter)
    }

    fn sum<I>(&self, iter: &I) -> Pin<Box<dyn Future<Output = I::Item> + Send>>
    where
        I: LocalIterator + 'static,
        I::Item: SyncSend + std::iter::Sum,
    {
        LocalIteratorLauncher::sum(&self.array, iter)
    }
    
    fn sum_with_schedule<I>(&self, sched: Schedule, iter: &I) -> Pin<Box<dyn Future<Output = I::Item> + Send>>
    where
        I: LocalIterator + 'static,
        I::Item: SyncSend + std::iter::Sum,
    {
        LocalIteratorLauncher::sum_with_schedule(&self.array, sched, iter)
    }

    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team_rt().clone()
    }
}

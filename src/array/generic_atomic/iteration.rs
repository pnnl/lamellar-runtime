use crate::array::generic_atomic::*;
use crate::array::iterator::distributed_iterator::{
    DistIteratorLauncher, DistributedIterator, IndexedDistributedIterator,
};
use crate::array::iterator::local_iterator::{LocalIterator, LocalIteratorLauncher};
use crate::array::iterator::one_sided_iterator::OneSidedIter;
use crate::array::iterator::{
    private::*, LamellarArrayIterators, LamellarArrayMutIterators, Schedule,
};
use crate::array::*;
use crate::memregion::Dist;
// use parking_lot::{
//     lock_api::{RwLockReadGuardArc, RwLockWriteGuardArc},
//     RawRwLock,
// };

#[doc(hidden)]
#[derive(Clone)]
pub struct GenericAtomicDistIter<T: Dist> {
    data: GenericAtomicArray<T>,
    cur_i: usize,
    end_i: usize,
}

impl<T: Dist> IterClone for GenericAtomicDistIter<T> {
    fn iter_clone(&self, _: Sealed) -> Self {
        GenericAtomicDistIter {
            data: self.data.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
        }
    }
}

impl<T: Dist> std::fmt::Debug for GenericAtomicDistIter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GenericAtomicDistIter{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

#[doc(hidden)]
#[derive(Clone)]
pub struct GenericAtomicLocalIter<T: Dist> {
    data: GenericAtomicArray<T>,
    cur_i: usize,
    end_i: usize,
}

impl<T: Dist> IterClone for GenericAtomicLocalIter<T> {
    fn iter_clone(&self, _: Sealed) -> Self {
        GenericAtomicLocalIter {
            data: self.data.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
        }
    }
}

impl<T: Dist> std::fmt::Debug for GenericAtomicLocalIter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GenericAtomicLocalIter{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

impl<T: Dist> DistributedIterator for GenericAtomicDistIter<T> {
    type Item = GenericAtomicElement<T>;
    type Array = GenericAtomicArray<T>;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
        GenericAtomicDistIter {
            data: self.data.clone(),
            cur_i: std::cmp::min(start_i, max_i),
            end_i: std::cmp::min(start_i + cnt, max_i),
        }
    }
    fn array(&self) -> Self::Array {
        self.data.clone()
    }
    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_i < self.end_i {
            self.cur_i += 1;
            Some(GenericAtomicElement {
                array: self.data.clone(),
                local_index: self.cur_i - 1,
            })
        } else {
            None
        }
    }
    fn elems(&self, in_elems: usize) -> usize {
        in_elems
    }
    // fn global_index(&self, index: usize) -> Option<usize> {
    //     let g_index = self.data.global_index_from_local(index, 1);
    //     g_index
    // }
    // fn subarray_index(&self, index: usize) -> Option<usize> {
    //     let g_index = self.data.subarray_index_from_local(index, 1);
    //     g_index
    // }
    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}
impl<T: Dist> IndexedDistributedIterator for GenericAtomicDistIter<T> {
    fn iterator_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.subarray_index_from_local(index, 1);
        g_index
    }
}

impl<T: Dist> LocalIterator for GenericAtomicLocalIter<T> {
    type Item = GenericAtomicElement<T>;
    type Array = GenericAtomicArray<T>;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("init generic_atomic start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?} {:?}",start_i,cnt, start_i+cnt,max_i,std::thread::current().id());
        GenericAtomicLocalIter {
            data: self.data.clone(),
            cur_i: std::cmp::min(start_i, max_i),
            end_i: std::cmp::min(start_i + cnt, max_i),
        }
    }
    fn array(&self) -> Self::Array {
        self.data.clone()
    }
    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_i < self.end_i {
            self.cur_i += 1;
            Some(GenericAtomicElement {
                array: self.data.clone(),
                local_index: self.cur_i - 1,
            })
        } else {
            None
        }
    }
    fn elems(&self, in_elems: usize) -> usize {
        in_elems
    }

    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

impl<T: Dist> LamellarArrayIterators<T> for GenericAtomicArray<T> {
    // type Array = GenericAtomicArray<T>;
    type DistIter = GenericAtomicDistIter<T>;
    type LocalIter = GenericAtomicLocalIter<T>;
    type OnesidedIter = OneSidedIter<'static, T, Self>;
    fn dist_iter(&self) -> Self::DistIter {
        GenericAtomicDistIter {
            data: self.clone(),
            cur_i: 0,
            end_i: 0,
        }
    }

    fn local_iter(&self) -> Self::LocalIter {
        GenericAtomicLocalIter {
            data: self.clone(),
            cur_i: 0,
            end_i: 0,
        }
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

impl<T: Dist> LamellarArrayMutIterators<T> for GenericAtomicArray<T> {
    type DistIter = GenericAtomicDistIter<T>;
    type LocalIter = GenericAtomicLocalIter<T>;

    fn dist_iter_mut(&self) -> Self::DistIter {
        GenericAtomicDistIter {
            data: self.clone(),
            cur_i: 0,
            end_i: 0,
        }
    }

    fn local_iter_mut(&self) -> Self::LocalIter {
        GenericAtomicLocalIter {
            data: self.clone(),
            cur_i: 0,
            end_i: 0,
        }
    }
}

impl<T: Dist> DistIteratorLauncher for GenericAtomicArray<T> {
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
        DistIteratorLauncher::for_each_async(&self.array, iter, op)
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

    fn reduce<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = Option<I::Item>> + Send>>
    where
        I: DistributedIterator + 'static,
        I::Item: Dist + ArrayOps,
        F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
    {
        DistIteratorLauncher::reduce(&self.array, iter, op)
    }

    fn reduce_with_schedule<I, F>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = Option<I::Item>> + Send>>
    where
        I: DistributedIterator + 'static,
        I::Item: Dist + ArrayOps,
        F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
    {
        DistIteratorLauncher::reduce_with_schedule(&self.array, sched, iter, op)
    }

    fn collect<I, A>(&self, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: DistributedIterator + 'static,
        I::Item: Dist + ArrayOps,
        A: for<'a> TeamFrom<(&'a Vec<I::Item>, Distribution)> + SyncSend + Clone + 'static,
    {
        DistIteratorLauncher::collect(&self.array, iter, d)
    }

    fn collect_with_schedule<I, A>(
        &self,
        sched: Schedule,
        iter: &I,
        d: Distribution,
    ) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: DistributedIterator + 'static,
        I::Item: Dist + ArrayOps,
        A: for<'a> TeamFrom<(&'a Vec<I::Item>, Distribution)> + SyncSend + Clone + 'static,
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
        I::Item: Future<Output = B> + Send + 'static,
        B: Dist + ArrayOps,
        A: for<'a> TeamFrom<(&'a Vec<B>, Distribution)> + SyncSend + Clone + 'static,
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
        I::Item: Future<Output = B> + Send + 'static,
        B: Dist + ArrayOps,
        A: for<'a> TeamFrom<(&'a Vec<B>, Distribution)> + SyncSend + Clone + 'static,
    {
        DistIteratorLauncher::collect_async_with_schedule(&self.array, sched, iter, d)
    }

    fn count<I>(&self, iter: &I) -> Pin<Box<dyn Future<Output = usize> + Send>>
    where
        I: DistributedIterator + 'static,
    {
        DistIteratorLauncher::count(&self.array, iter)
    }

    fn count_with_schedule<I>(
        &self,
        sched: Schedule,
        iter: &I,
    ) -> Pin<Box<dyn Future<Output = usize> + Send>>
    where
        I: DistributedIterator + 'static,
    {
        DistIteratorLauncher::count_with_schedule(&self.array, sched, iter)
    }

    fn sum<I>(&self, iter: &I) -> Pin<Box<dyn Future<Output = I::Item> + Send>>
    where
        I: DistributedIterator + 'static,
        I::Item: Dist + ArrayOps + std::iter::Sum,
    {
        DistIteratorLauncher::sum(&self.array, iter)
    }

    fn sum_with_schedule<I>(
        &self,
        sched: Schedule,
        iter: &I,
    ) -> Pin<Box<dyn Future<Output = I::Item> + Send>>
    where
        I: DistributedIterator + 'static,
        I::Item: Dist + ArrayOps + std::iter::Sum,
    {
        DistIteratorLauncher::sum_with_schedule(&self.array, sched, iter)
    }

    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team_rt().clone()
    }
}

impl<T: Dist> LocalIteratorLauncher for GenericAtomicArray<T> {
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
        LocalIteratorLauncher::for_each(&self.array, iter, op)
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
        LocalIteratorLauncher::for_each_with_schedule(&self.array, sched, iter, op)
    }
    fn for_each_async<I, F, Fut>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        LocalIteratorLauncher::for_each_async(&self.array, iter, op)
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

    fn reduce_with_schedule<I, F>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = Option<I::Item>> + Send>>
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
        A: for<'a> TeamFrom<(&'a Vec<I::Item>, Distribution)> + SyncSend + Clone + 'static,
    {
        LocalIteratorLauncher::collect(&self.array, iter, d)
    }

    fn collect_with_schedule<I, A>(
        &self,
        sched: Schedule,
        iter: &I,
        d: Distribution,
    ) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: LocalIterator + 'static,
        I::Item: Dist + ArrayOps,
        A: for<'a> TeamFrom<(&'a Vec<I::Item>, Distribution)> + SyncSend + Clone + 'static,
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
        I: LocalIterator + 'static,
    {
        LocalIteratorLauncher::count(&self.array, iter)
    }

    fn count_with_schedule<I>(
        &self,
        sched: Schedule,
        iter: &I,
    ) -> Pin<Box<dyn Future<Output = usize> + Send>>
    where
        I: LocalIterator + 'static,
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

    fn sum_with_schedule<I>(
        &self,
        sched: Schedule,
        iter: &I,
    ) -> Pin<Box<dyn Future<Output = I::Item> + Send>>
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

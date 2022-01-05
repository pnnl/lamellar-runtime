use crate::array::collective_atomic::*;

use crate::array::iterator::distributed_iterator::{
    DistIter, DistIterMut, DistIteratorLauncher, DistributedIterator, ForEach, ForEachAsync,
};
use crate::array::iterator::serial_iterator::LamellarArrayIter;
use crate::array::*;
use crate::memregion::Dist;

impl<T: Dist> CollectiveAtomicArray<T> {
    pub fn dist_iter(&self) -> DistIter<'static, T, CollectiveAtomicArray<T>> {
        DistIter::new(self.clone().into(), 0, 0)
    }

    pub fn dist_iter_mut(&self) -> DistIterMut<'static, T, CollectiveAtomicArray<T>> {
        DistIterMut::new(self.clone().into(), 0, 0)
    }

    pub fn ser_iter(&self) -> LamellarArrayIter<'_, T, CollectiveAtomicArray<T>> {
        LamellarArrayIter::new(self.clone().into(), self.array.team().clone(), 1)
    }

    pub fn buffered_iter(&self, buf_size: usize) -> LamellarArrayIter<'_, T, CollectiveAtomicArray<T>> {
        LamellarArrayIter::new(
            self.clone().into(),
            self.array.team().clone(),
            std::cmp::min(buf_size, self.len()),
        )
    }
}

impl<T: Dist> DistIteratorLauncher for CollectiveAtomicArray<T> {
    fn global_index_from_local(&self, index: usize, chunk_size: usize) -> usize {
        self.array.global_index_from_local(index, chunk_size)
    }

    fn for_each<I, F>(&self, iter: &I, op: F)
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + Sync + Send + Clone + 'static,
    {
        self.array.for_each(iter, op)
    }
    fn for_each_async<I, F, Fut>(&self, iter: &I, op: F)
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + Clone + 'static,
    {
        self.array.for_each_async(iter, op)
    }
}

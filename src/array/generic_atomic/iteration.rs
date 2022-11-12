use crate::array::generic_atomic::*;

use crate::array::iterator::distributed_iterator::*;
use crate::array::iterator::local_iterator::*;
use crate::array::iterator::one_sided_iterator::OneSidedIter;
use crate::array::iterator::Schedule;
// use crate::array::private::LamellarArrayPrivate;
use crate::array::*;
use crate::memregion::Dist;
// use parking_lot::{
//     lock_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard},
//     RawRwLock,
// };

#[doc(hidden)]
#[derive(Clone)]
pub struct GenericAtomicDistIter<T: Dist> {
    data: GenericAtomicArray<T>,
    cur_i: usize,
    end_i: usize,
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

// impl<T: Dist> GenericAtomicDistIter<T> {
//     pub(crate) fn new(data: GenericAtomicArray<T>, cur_i: usize, cnt: usize) -> Self {
//         // println!("new dist iter {:?} {:? } {:?}",cur_i, cnt, cur_i+cnt);
//         GenericAtomicDistIter {
//             data,
//             cur_i,
//             end_i: cur_i + cnt,
//         }
//     }
// }
// impl<T: Dist + 'static> GenericAtomicDistIter<T> {
//     pub fn for_each<F>(&self, op: F) -> DistIterForEachHandle
//     where
//         F: Fn(GenericAtomicElement<T>) + SyncSend + Clone + 'static,
//     {
//         self.data.clone().for_each(self, op)
//     }
//     pub fn for_each_async<F, Fut>(&self, op: F) -> DistIterForEachHandle
//     where
//         F: Fn(GenericAtomicElement<T>) -> Fut + SyncSend + Clone + 'static,
//         Fut: Future<Output = ()> + SyncSend + 'static,
//     {
//         self.data.clone().for_each_async(self, op)
//     }
// }

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
        // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
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

impl<T: Dist + 'static> GenericAtomicArray<T> {
    pub fn dist_iter(&self) -> GenericAtomicDistIter<T> {
        GenericAtomicDistIter {
            data: self.clone(),
            cur_i: 0,
            end_i: 0,
        }
    }

    pub fn dist_iter_mut(&self) -> GenericAtomicDistIter<T> {
        GenericAtomicDistIter {
            data: self.clone(),
            cur_i: 0,
            end_i: 0,
        }
    }

    pub fn local_iter(&self) -> GenericAtomicLocalIter<T> {
        GenericAtomicLocalIter {
            data: self.clone(),
            cur_i: 0,
            end_i: 0,
        }
    }

    pub fn local_iter_mut(&self) -> GenericAtomicLocalIter<T> {
        GenericAtomicLocalIter {
            data: self.clone(),
            cur_i: 0,
            end_i: 0,
        }
    }

    pub fn onesided_iter(&self) -> OneSidedIter<'_, T, GenericAtomicArray<T>> {
        OneSidedIter::new(self.clone().into(), self.array.team().clone(), 1)
    }

    pub fn buffered_onesided_iter(
        &self,
        buf_size: usize,
    ) -> OneSidedIter<'_, T, GenericAtomicArray<T>> {
        OneSidedIter::new(
            self.clone().into(),
            self.array.team().clone(),
            std::cmp::min(buf_size, self.len()),
        )
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
        self.array.for_each(iter, op)
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
        self.array.for_each_with_schedule(sched, iter, op)
    }
    fn for_each_async<I, F, Fut>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.array.for_each_async(iter, op)
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
        self.array.for_each_async_with_schedule(sched, iter, op)
    }

    fn collect<I, A>(&self, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: DistributedIterator + 'static,
        I::Item: Dist,
        A: From<UnsafeArray<I::Item>> + SyncSend + 'static,
    {
        self.array.collect(iter, d)
    }
    fn collect_async<I, A, B>(
        &self,
        iter: &I,
        d: Distribution,
    ) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: DistributedIterator + 'static,
        I::Item: Future<Output = B> + Send + 'static,
        B: Dist,
        A: From<UnsafeArray<B>> + SyncSend + 'static,
    {
        self.array.collect_async(iter, d)
    }
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team().clone()
    }
}

impl<T: Dist> LocalIteratorLauncher for GenericAtomicArray<T> {
    fn local_global_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        self.array.local_global_index_from_local(index, chunk_size)
    }

    fn local_subarray_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        self.array.local_subarray_index_from_local(index, chunk_size)
    }

    fn local_for_each<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        self.array.local_for_each(iter, op)
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
        self.array.local_for_each_with_schedule(sched, iter, op)
    }
    fn local_for_each_async<I, F, Fut>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.array.local_for_each_async(iter, op)
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
        self.array.local_for_each_async_with_schedule(sched, iter, op)
    }

    // fn local_collect<I, A>(&self, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    // where
    //     I: LocalIterator + 'static,
    //     I::Item: Dist,
    //     A: From<UnsafeArray<I::Item>> + SyncSend + 'static,
    // {
    //     self.array.local_collect(iter, d)
    // }
    // fn local_collect_async<I, A, B>(
    //     &self,
    //     iter: &I,
    //     d: Distribution,
    // ) -> Pin<Box<dyn Future<Output = A> + Send>>
    // where
    //     I: LocalIterator + 'static,
    //     I::Item: Future<Output = B> + Send + 'static,
    //     B: Dist,
    //     A: From<UnsafeArray<B>> + SyncSend + 'static,
    // {
    //     self.array.local_collect_async(iter, d)
    // }
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team().clone()
    }
}

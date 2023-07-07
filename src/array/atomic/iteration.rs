use crate::array::atomic::*;

use crate::array::iterator::distributed_iterator::{
    DistIteratorLauncher, DistributedIterator, IndexedDistributedIterator,
};
use crate::array::iterator::local_iterator::{IndexedLocalIterator, LocalIterator};
use crate::array::iterator::one_sided_iterator::OneSidedIter;
use crate::array::iterator::{LamellarArrayIterators, LamellarArrayMutIterators};
use crate::array::*;
use crate::memregion::Dist;

#[derive(Clone)]
pub struct AtomicDistIter<T: Dist> {
    //dont need a AtomicDistIterMut in this case as any updates to inner elements are atomic
    data: AtomicArray<T>,
    cur_i: usize,
    end_i: usize,
}

impl<T: Dist> std::fmt::Debug for AtomicDistIter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AtomicDistIter{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

impl<T: Dist> AtomicDistIter<T> {
    pub(crate) fn new(data: AtomicArray<T>, cur_i: usize, cnt: usize) -> Self {
        // println!("new dist iter {:?} {:? } {:?}",cur_i, cnt, cur_i+cnt);
        AtomicDistIter {
            data,
            cur_i,
            end_i: cur_i + cnt,
        }
    }
}

#[derive(Clone)]
pub struct AtomicLocalIter<T: Dist> {
    //dont need a AtomicDistIterMut in this case as any updates to inner elements are atomic
    data: AtomicArray<T>,
    cur_i: usize,
    end_i: usize,
}

impl<T: Dist> std::fmt::Debug for AtomicLocalIter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AtomicLocalIter{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

impl<T: Dist> AtomicLocalIter<T> {
    pub(crate) fn new(data: AtomicArray<T>, cur_i: usize, cnt: usize) -> Self {
        // println!("new dist iter {:?} {:? } {:?}",cur_i, cnt, cur_i+cnt);
        AtomicLocalIter {
            data,
            cur_i,
            end_i: cur_i + cnt,
        }
    }
}

impl<T: Dist> DistributedIterator for AtomicDistIter<T> {
    type Item = AtomicElement<T>;
    type Array = AtomicArray<T>;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
        // println!("num_elems_local: {:?}",self.data.num_elems_local());
        AtomicDistIter {
            data: self.data.clone(),
            cur_i: std::cmp::min(start_i, max_i),
            end_i: std::cmp::min(start_i + cnt, max_i),
        }
    }
    fn array(&self) -> Self::Array {
        self.data.clone()
    }
    fn next(&mut self) -> Option<Self::Item> {
        // println!("{:?} {:?}",self.cur_i,self.end_i);
        if self.cur_i < self.end_i {
            self.cur_i += 1;
            self.data.get_element(self.cur_i - 1)
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
impl<T: Dist> IndexedDistributedIterator for AtomicDistIter<T> {
    fn iterator_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.subarray_index_from_local(index, 1);
        g_index
    }
}

impl<T: Dist> LocalIterator for AtomicLocalIter<T> {
    type Item = AtomicElement<T>;
    type Array = AtomicArray<T>;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("init atomic start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?} {:?}",start_i,cnt, start_i+cnt,max_i,std::thread::current().id());

        AtomicLocalIter {
            data: self.data.clone(),
            cur_i: std::cmp::min(start_i, max_i),
            end_i: std::cmp::min(start_i + cnt, max_i),
        }
    }
    fn array(&self) -> Self::Array {
        self.data.clone()
    }
    fn next(&mut self) -> Option<Self::Item> {
        // println!("{:?} {:?} {:?} {:?}",self.cur_i,self.end_i,self.cur_i < self.end_i,std::thread::current().id());
    
        if self.cur_i < self.end_i {
            self.cur_i += 1;
            self.data.get_element(self.cur_i - 1)
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

impl<T: Dist + 'static> IndexedLocalIterator for AtomicLocalIter<T> {
    fn iterator_index(&self, index: usize) -> Option<usize> {
        if index < self.data.len() {
            Some(index) //everyone at this point as calculated the actual index (cause we are local only) so just return it
        } else {
            None
        }
    }
}

impl<T: Dist> LamellarArrayIterators<T> for AtomicArray<T> {
    // type Array = AtomicArray<T>;
    type DistIter = AtomicDistIter<T>;
    type LocalIter = AtomicLocalIter<T>;
    type OnesidedIter = OneSidedIter<'static, T, Self>;
    fn dist_iter(&self) -> Self::DistIter {
        AtomicDistIter::new(self.clone(), 0, 0)
    }

    fn local_iter(&self) -> Self::LocalIter {
        AtomicLocalIter::new(self.clone(), 0, 0)
    }

    fn onesided_iter(&self) -> Self::OnesidedIter {
        OneSidedIter::new(self.clone().into(), LamellarArray::team(self).clone(), 1)
    }

    fn buffered_onesided_iter(&self, buf_size: usize) -> Self::OnesidedIter {
        OneSidedIter::new(
            self.clone().into(),
            LamellarArray::team(self).clone(),
            std::cmp::min(buf_size, self.len()),
        )
    }
}

impl<T: Dist> LamellarArrayMutIterators<T> for AtomicArray<T> {
    type DistIter = AtomicDistIter<T>;
    type LocalIter = AtomicLocalIter<T>;

    fn dist_iter_mut(&self) -> Self::DistIter {
        AtomicDistIter::new(self.clone(), 0, 0)
    }

    fn local_iter_mut(&self) -> Self::LocalIter {
        AtomicLocalIter::new(self.clone(), 0, 0)
    }
}

// impl<T: Dist> DistIteratorLauncher for AtomicArray<T> {
//     fn global_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
//         self.data.global_index_from_local(index, chunk_size)
//     }

//     fn subarray_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
//         self.data.subarray_index_from_local(index, chunk_size)
//     }

//     fn for_each<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
//     where
//         I: DistributedIterator + 'static,
//         F: Fn(I::Item) + SyncSend + Clone + 'static,
//     {
//         self.data.for_each(iter, op)
//     }
//     fn for_each_with_schedule<I, F>(
//         &self,
//         sched: Schedule,
//         iter: &I,
//         op: F,
//     ) -> Pin<Box<dyn Future<Output = ()> + Send>>
//     where
//         I: DistributedIterator + 'static,
//         F: Fn(I::Item) + SyncSend + Clone + 'static,
//     {
//         self.data.for_each_with_schedule(sched, iter, op)
//     }
//     fn for_each_async<I, F, Fut>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
//     where
//         I: DistributedIterator + 'static,
//         F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
//         Fut: Future<Output = ()> + Send + 'static,
//     {
//         self.data.for_each_async(iter, op)
//     }
//     fn for_each_async_with_schedule<I, F, Fut>(
//         &self,
//         sched: Schedule,
//         iter: &I,
//         op: F,
//     ) -> Pin<Box<dyn Future<Output = ()> + Send>>
//     where
//         I: DistributedIterator + 'static,
//         F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
//         Fut: Future<Output = ()> + Send + 'static,
//     {
//         self.data.for_each_async_with_schedule(sched, iter, op)
//     }

//     fn collect<I, A>(&self, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
//     where
//         I: DistributedIterator + 'static,
//         I::Item: Dist + ArrayOps,
//         A: for<'a>  TeamFrom<(&'a Vec<I::Item>,Distribution)> + SyncSend + Clone + 'static,
//     {
//         self.data.collect(iter, d)
//     }
//     fn collect_async<I, A, B>(
//         &self,
//         iter: &I,
//         d: Distribution,
//     ) -> Pin<Box<dyn Future<Output = A> + Send>>
//     where
//         I: DistributedIterator + 'static,
//         I::Item: Future<Output = B> + SyncSend + Clone + 'static,
//         B: Dist + ArrayOps,
//         A: From<UnsafeArray<B>> + SyncSend  + Clone +  'static,
//     {
//         self.data.collect_async(iter, d)
//     }
//     fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
//         self.data.team().clone()
//     }
// }

// impl<T: Dist> LocalIteratorLauncher for AtomicArray<T> {
//     fn local_global_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
//         self.data.local_global_index_from_local(index, chunk_size)
//     }

//     fn local_subarray_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
//         self.data.local_subarray_index_from_local(index, chunk_size)
//     }

//     fn local_for_each<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
//     where
//         I: LocalIterator + 'static,
//         F: Fn(I::Item) + SyncSend + Clone + 'static,
//     {
//         self.data.local_for_each(iter, op)
//     }
//     fn local_for_each_with_schedule<I, F>(
//         &self,
//         sched: Schedule,
//         iter: &I,
//         op: F,
//     ) -> Pin<Box<dyn Future<Output = ()> + Send>>
//     where
//         I: LocalIterator + 'static,
//         F: Fn(I::Item) + SyncSend + Clone + 'static,
//     {
//         self.data.local_for_each_with_schedule(sched, iter, op)
//     }
//     fn local_for_each_async<I, F, Fut>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
//     where
//         I: LocalIterator + 'static,
//         F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
//         Fut: Future<Output = ()> + Send + 'static,
//     {
//         self.data.local_for_each_async(iter, op)
//     }
//     fn local_for_each_async_with_schedule<I, F, Fut>(
//         &self,
//         sched: Schedule,
//         iter: &I,
//         op: F,
//     ) -> Pin<Box<dyn Future<Output = ()> + Send>>
//     where
//         I: LocalIterator + 'static,
//         F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
//         Fut: Future<Output = ()> + Send + 'static,
//     {
//         self.data.local_for_each_async_with_schedule(sched, iter, op)
//     }

//     // fn local_collect<I, A>(&self, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
//     // where
//     //     I: LocalIterator + 'static,
//     //     I::Item: Dist + ArrayOps,
//     //     A: for<'a>  TeamFrom<(&'a Vec<I::Item>,Distribution)> + SyncSend + Clone + 'static,
//     // {
//     //     self.data.local_collect(iter, d)
//     // }
//     // fn local_collect_async<I, A, B>(
//     //     &self,
//     //     iter: &I,
//     //     d: Distribution,
//     // ) -> Pin<Box<dyn Future<Output = A> + Send>>
//     // where
//     //     I: LocalIterator + 'static,
//     //     I::Item: Future<Output = B> + SyncSend + Clone + 'static,
//     //     B: Dist + ArrayOps,
//     //     A: From<UnsafeArray<B>> + SyncSend  + Clone +  'static,
//     // {
//     //     self.data.local_collect_async(iter, d)
//     // }

//     fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
//         self.data.team().clone()
//     }
// }

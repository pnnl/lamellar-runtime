use crate::array::collective_atomic::*;

use crate::array::iterator::distributed_iterator::{
     DistIteratorLauncher, DistributedIterator
};
use crate::array::iterator::serial_iterator::LamellarArrayIter;
use crate::array::private::LamellarArrayPrivate;
use crate::array::*;
use crate::memregion::Dist;
use parking_lot::{RawRwLock, lock_api::{ArcRwLockReadGuard,ArcRwLockWriteGuard}};


#[derive(Clone)]
pub struct CollectiveAtomicDistIter<'a, T: Dist> {
    data: CollectiveAtomicArray<T>,
    lock: Arc<ArcRwLockReadGuard<RawRwLock, Box<()>>>,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'a T>,
}


// impl<'a,T: Dist> CollectiveAtomicDistIter<'a, T> {
//     pub(crate) fn new(data: CollectiveAtomicArray<T>,lock: Arc<RwLockReadGuard<'a, Box<()>>>, cur_i: usize, cnt: usize) -> Self {
//         // println!("new dist iter {:?} {:? } {:?}",cur_i, cnt, cur_i+cnt);
//         CollectiveAtomicDistIter {
//             data,
//             lock,
//             cur_i,
//             end_i: cur_i + cnt,
//         }
//     }
// }
impl<T: Dist + 'static> CollectiveAtomicDistIter<'static, T> {
    pub fn for_each<F>(self, op: F)
    where
        F: Fn(&T) + Sync + Send + Clone + 'static,
    {
        self.data.clone().for_each(self, op);
    }
    pub fn for_each_async<F, Fut>(&self, op: F)
    where
        F: Fn(&T) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + Clone + 'static,
    {
        self.data.clone().for_each_async(self, op);
    }
}

impl<'a, T: Dist + 'a > DistributedIterator for CollectiveAtomicDistIter<'a, T> {
    type Item = &'a T;
    type Array = CollectiveAtomicArray<T>;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
        CollectiveAtomicDistIter {
            data: self.data.clone(),
            lock: self.lock.clone(),
            cur_i: std::cmp::min(start_i, max_i),
            end_i: std::cmp::min(start_i + cnt, max_i),
            _marker: PhantomData,
        }
    }
    fn array(&self) -> Self::Array {
        self.data.clone()
    }
    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_i < self.end_i {
            self.cur_i += 1;
            unsafe {
                self.data.array
                    .local_as_ptr()
                    .offset((self.cur_i - 1) as isize)
                    .as_ref()
            }
        } else {
            None
        }
    }
    fn elems(&self, in_elems: usize) -> usize {
        in_elems
    }
    fn global_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.global_index_from_local(index, 1);
        g_index
    }
    fn subarray_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.subarray_index_from_local(index,1); 
        g_index
    }
    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

#[derive(Clone)]
pub struct CollectiveAtomicDistIterMut<'a, T: Dist> {
    data: CollectiveAtomicArray<T>,
    lock: Arc<ArcRwLockWriteGuard<RawRwLock, Box<()>>>,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'a T>,
}

// impl<'a, T: Dist> CollectiveAtomicDistIterMut<'a, T> {
//     pub(crate) fn new(data: CollectiveAtomicArray<T>,lock: Arc<RwLockWriteGuard<'a, Box<()>>>, cur_i: usize, cnt: usize) -> Self {
//         // println!("new dist iter {:?} {:? } {:?}",cur_i, cnt, cur_i+cnt);
//         CollectiveAtomicDistIterMut {
//             data,
//             lock,
//             cur_i,
//             end_i: cur_i + cnt,
//         }
//     }
// }
impl<T: Dist +'static> CollectiveAtomicDistIterMut<'static, T> {
    pub fn for_each<F>(self, op: F)
    where
        F: Fn(&mut T) + Sync + Send + Clone + 'static,
    {
        self.data.clone().for_each(self, op);
    }
    pub fn for_each_async<F, Fut>(&self, op: F)
    where
        F: Fn(&mut T) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + Clone + 'static,
    {
        self.data.clone().for_each_async(self, op);
    }
}

impl<'a, T: Dist + 'a> DistributedIterator for CollectiveAtomicDistIterMut<'a, T> {
    type Item = &'a mut T;
    type Array = CollectiveAtomicArray<T>;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
        CollectiveAtomicDistIterMut {
            data: self.data.clone(),
            lock: self.lock.clone(),
            cur_i: std::cmp::min(start_i, max_i),
            end_i: std::cmp::min(start_i + cnt, max_i),
            _marker: PhantomData,
        }
    }
    fn array(&self) -> Self::Array {
        self.data.clone()
    }
    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_i < self.end_i {
            self.cur_i += 1;
            unsafe {
                Some(
                    &mut *self
                        .data.array
                        .local_as_mut_ptr()
                        .offset((self.cur_i - 1) as isize),
                )
            }
        } else {
            None
        }
    }
    fn elems(&self, in_elems: usize) -> usize {
        in_elems
    }
    fn global_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.global_index_from_local(index, 1);
        g_index
    }
    fn subarray_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.subarray_index_from_local(index,1); //not sure if this works...
                                                  // println!("enumerate index: {:?} global_index {:?}", index,g_index);
        g_index
    }
    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

impl< T: Dist + 'static> CollectiveAtomicArray<T> {
    pub fn dist_iter(&self) -> CollectiveAtomicDistIter<'static,T> {
        let lock = Arc::new(self.lock.read());
        self.barrier();
        // CollectiveAtomicDistIter::new(self.clone(), lock, 0, 0)
        CollectiveAtomicDistIter {
            data: self.clone(),
            lock: lock,
            cur_i: 0,
            end_i: 0,
            _marker: PhantomData,
        }
    }

    pub fn dist_iter_mut(&self) -> CollectiveAtomicDistIterMut<'static,T> {
        let lock = Arc::new(self.lock.write());
        self.barrier();
        // CollectiveAtomicDistIterMut::new(self.clone(), lock, 0, 0)
        CollectiveAtomicDistIterMut {
            data: self.clone(),
            lock: lock,
            cur_i: 0,
            end_i: 0,
            _marker: PhantomData,
        }
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
    fn global_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        self.array.global_index_from_local(index, chunk_size)
    }

    fn subarray_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        self.array.subarray_index_from_local(index, chunk_size)
    }

    fn for_each<I, F>(&self, iter: I, op: F)
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

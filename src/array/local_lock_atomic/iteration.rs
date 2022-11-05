use crate::array::local_lock_atomic::*;

// use crate::array::iterator::distributed_iterator::{DistIteratorLauncher, DistributedIterator};
use crate::array::iterator::distributed_iterator::*;
use crate::array::iterator::serial_iterator::LamellarArrayIter;
use crate::array::private::LamellarArrayPrivate;
use crate::array::*;
use crate::memregion::Dist;
use parking_lot::{
    lock_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard},
    RawRwLock,
};

#[derive(Clone)]
pub struct LocalLockAtomicDistIter<'a, T: Dist> {
    data: LocalLockAtomicArray<T>,
    lock: Arc<ArcRwLockReadGuard<RawRwLock, Box<()>>>,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'a T>,
}

impl<'a, T: Dist> std::fmt::Debug for LocalLockAtomicDistIter<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LocalLockAtomicDistIter{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

// impl<'a,T: Dist> LocalLockAtomicDistIter<'a, T> {
//     pub(crate) fn new(data: LocalLockAtomicArray<T>,lock: Arc<RwLockReadGuard<'a, Box<()>>>, cur_i: usize, cnt: usize) -> Self {
//         // println!("new dist iter {:?} {:? } {:?}",cur_i, cnt, cur_i+cnt);
//         LocalLockAtomicDistIter {
//             data,
//             lock,
//             cur_i,
//             end_i: cur_i + cnt,
//         }
//     }
// }
// impl<T: Dist + 'static> LocalLockAtomicDistIter<'static, T> {
//     pub fn for_each<F>(&self, op: F)
//     where
//         F: Fn(&T) + SyncSend  + Clone + 'static,
//     {
//         self.data.clone().for_each(self, op);
//     }
//     pub fn for_each_async<F, Fut>(&self, op: F)
//     where
//         F: Fn(&T) -> Fut + SyncSend  + Clone + 'static,
//         Fut: Future<Output = ()> + Send  + Clone + 'static,
//     {
//         self.data.clone().for_each_async(self, op);
//     }
// }

impl<T: Dist + 'static> DistributedIterator for LocalLockAtomicDistIter<'static, T> {
    type Item = &'static T;
    type Array = LocalLockAtomicArray<T>;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
        LocalLockAtomicDistIter {
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
                self.data
                    .array
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
        let g_index = self.data.subarray_index_from_local(index, 1);
        g_index
    }
    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

#[derive(Clone)]
pub struct LocalLockAtomicDistIterMut<'a, T: Dist> {
    data: LocalLockAtomicArray<T>,
    lock: Arc<ArcRwLockWriteGuard<RawRwLock, Box<()>>>,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'a T>,
}

impl<'a, T: Dist> std::fmt::Debug for LocalLockAtomicDistIterMut<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LocalLockAtomicDistIterMut{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

// impl <'a, T:Dist> Drop for LocalLockAtomicDistIterMut<'a, T> {
//     fn drop(&mut self){
//         println!("dist_iter thread {:?} drop lock",std::thread::current().id());
//     }
// }

// impl<'a, T: Dist> LocalLockAtomicDistIterMut<'a, T> {
//     pub(crate) fn new(data: LocalLockAtomicArray<T>,lock: Arc<RwLockWriteGuard<'a, Box<()>>>, cur_i: usize, cnt: usize) -> Self {
//         // println!("new dist iter {:?} {:? } {:?}",cur_i, cnt, cur_i+cnt);
//         LocalLockAtomicDistIterMut {
//             data,
//             lock,
//             cur_i,
//             end_i: cur_i + cnt,
//         }
//     }
// }
// impl<T: Dist + 'static> LocalLockAtomicDistIterMut<'static, T> {
//     pub fn for_each<F>(&self, op: F)
//     where
//         F: Fn(&mut T) + SyncSend  + Clone + 'static,
//     {
//         self.data.clone().for_each(self, op);
//     }
//     pub fn for_each_async<F, Fut>(&self, op: F)
//     where
//         F: Fn(&mut T) -> Fut + SyncSend  + Clone + 'static,
//         Fut: Future<Output = ()> + SyncSend +  'static,
//     {
//         self.data.clone().for_each_async(self, op);
//     }
// }

impl<T: Dist + 'static> DistributedIterator for LocalLockAtomicDistIterMut<'static, T> {
    type Item = &'static mut T;
    type Array = LocalLockAtomicArray<T>;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
        LocalLockAtomicDistIterMut {
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
                        .data
                        .array
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
        let g_index = self.data.subarray_index_from_local(index, 1); //not sure if this works...
                                                                     // println!("enumerate index: {:?} global_index {:?}", index,g_index);
        g_index
    }
    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

impl<T: Dist + 'static> LocalLockAtomicArray<T> {
    pub fn dist_iter(&self) -> LocalLockAtomicDistIter<'static, T> {
        let lock = Arc::new(self.lock.read());
        // self.barrier();
        // LocalLockAtomicDistIter::new(self.clone(), lock, 0, 0)
        LocalLockAtomicDistIter {
            data: self.clone(),
            lock: lock,
            cur_i: 0,
            end_i: 0,
            _marker: PhantomData,
        }
    }

    pub fn dist_iter_mut(&self) -> LocalLockAtomicDistIterMut<'static, T> {
        let lock = Arc::new(self.lock.write());
        // println!("dist_iter thread {:?} got lock",std::thread::current().id());
        // self.barrier();
        // LocalLockAtomicDistIterMut::new(self.clone(), lock, 0, 0)
        LocalLockAtomicDistIterMut {
            data: self.clone(),
            lock: lock,
            cur_i: 0,
            end_i: 0,
            _marker: PhantomData,
        }
    }

    pub fn ser_iter(&self) -> LamellarArrayIter<'_, T, LocalLockAtomicArray<T>> {
        LamellarArrayIter::new(self.clone().into(), self.array.team().clone(), 1)
    }

    pub fn buffered_iter(
        &self,
        buf_size: usize,
    ) -> LamellarArrayIter<'_, T, LocalLockAtomicArray<T>> {
        LamellarArrayIter::new(
            self.clone().into(),
            self.array.team().clone(),
            std::cmp::min(buf_size, self.len()),
        )
    }
}

impl<T: Dist> DistIteratorLauncher for LocalLockAtomicArray<T> {
    fn global_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        self.array.global_index_from_local(index, chunk_size)
    }

    fn subarray_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        self.array.subarray_index_from_local(index, chunk_size)
    }

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

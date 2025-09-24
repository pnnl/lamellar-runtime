use parking_lot::Mutex;

use crate::array::global_lock_atomic::*;

use crate::array::iterator::distributed_iterator::*;
use crate::array::iterator::local_iterator::*;
use crate::array::iterator::one_sided_iterator::OneSidedIter;
use crate::array::iterator::{
    private::{InnerIter, Sealed},
    LamellarArrayIterators, LamellarArrayMutIterators,
};
use crate::array::private::LamellarArrayPrivate;
use crate::array::r#unsafe::private::UnsafeArrayInner;
use crate::array::*;
use crate::memregion::Dist;

use self::iterator::IterLockFuture;

impl<T> InnerArray for GlobalLockArray<T> {
    fn as_inner(&self) -> &UnsafeArrayInner {
        &self.array.inner
    }
}

//#[doc(hidden)]
#[derive(Clone)]
pub struct GlobalLockDistIter<T: Dist> {
    data: GlobalLockArray<T>,
    lock: Arc<Mutex<Option<GlobalRwDarcReadGuard<()>>>>,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'static T>,
}

impl<T: Dist> InnerIter for GlobalLockDistIter<T> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        if self.lock.lock().is_none() {
            let lock_handle = self.data.lock.read();
            let lock = self.lock.clone();

            Some(Box::pin(async move {
                *lock.lock() = Some(lock_handle.await);
            }))
        } else {
            None
        }
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        GlobalLockDistIter {
            data: self.data.clone(),
            lock: self.lock.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
            _marker: PhantomData,
        }
    }
}

impl<T: Dist> std::fmt::Debug for GlobalLockDistIter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GlobalLockDistIter{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

//#[doc(hidden)]
#[derive(Clone)]
pub struct GlobalLockLocalIter<T: Dist> {
    data: GlobalLockArray<T>,
    lock: Arc<Mutex<Option<GlobalRwDarcReadGuard<()>>>>,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'static T>,
}

impl<T: Dist> InnerIter for GlobalLockLocalIter<T> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        if self.lock.lock().is_none() {
            let lock_handle = self.data.lock.read();
            let lock = self.lock.clone();
            Some(Box::pin(async move {
                *lock.lock() = Some(lock_handle.await);
            }))
        } else {
            None
        }
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        GlobalLockLocalIter {
            data: self.data.clone(),
            lock: self.lock.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
            _marker: PhantomData,
        }
    }
}

impl<T: Dist> std::fmt::Debug for GlobalLockLocalIter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GlobalLockLocalIter{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

impl<T: Dist + 'static> DistributedIterator for GlobalLockDistIter<T> {
    type Item = &'static T;
    type Array = GlobalLockArray<T>;
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
        GlobalLockDistIter {
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
    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}
impl<T: Dist + 'static> IndexedDistributedIterator for GlobalLockDistIter<T> {
    fn iterator_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.subarray_index_from_local(index, 1);
        g_index
    }
}

impl<T: Dist + 'static> LocalIterator for GlobalLockLocalIter<T> {
    type Item = &'static T;
    type Array = GlobalLockArray<T>;
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
        GlobalLockLocalIter {
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

    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

impl<T: Dist + 'static> IndexedLocalIterator for GlobalLockLocalIter<T> {
    fn iterator_index(&self, index: usize) -> Option<usize> {
        if index < self.data.len() {
            Some(index) //everyone at this point as calculated the actual index (cause we are local only) so just return it
        } else {
            None
        }
    }
}

pub struct GlobalLockDistIterMut<T: Dist> {
    data: GlobalLockArray<T>,
    lock: Arc<Mutex<Option<GlobalRwDarcCollectiveWriteGuard<()>>>>,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'static T>,
}

impl<T: Dist> InnerIter for GlobalLockDistIterMut<T> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        if self.lock.lock().is_none() {
            let lock_handle = self.data.lock.collective_write();
            let lock = self.lock.clone();

            Some(Box::pin(async move {
                *lock.lock() = Some(lock_handle.await);
            }))
        } else {
            None
        }
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        GlobalLockDistIterMut {
            data: self.data.clone(),
            lock: self.lock.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
            _marker: PhantomData,
        }
    }
}

impl<T: Dist> std::fmt::Debug for GlobalLockDistIterMut<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GlobalLockDistIterMut{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

pub struct GlobalLockLocalIterMut<T: Dist> {
    data: GlobalLockArray<T>,
    lock: Arc<Mutex<Option<GlobalRwDarcWriteGuard<()>>>>,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'static T>,
}

impl<T: Dist> InnerIter for GlobalLockLocalIterMut<T> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        if self.lock.lock().is_none() {
            let lock_handle = self.data.lock.write();
            let lock = self.lock.clone();

            Some(Box::pin(async move {
                *lock.lock() = Some(lock_handle.await);
            }))
        } else {
            None
        }
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        GlobalLockLocalIterMut {
            data: self.data.clone(),
            lock: self.lock.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
            _marker: PhantomData,
        }
    }
}

impl<T: Dist> std::fmt::Debug for GlobalLockLocalIterMut<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GlobalLockLocalIterMut{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

impl<T: Dist + 'static> DistributedIterator for GlobalLockDistIterMut<T> {
    type Item = &'static mut T;
    type Array = GlobalLockArray<T>;
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
        GlobalLockDistIterMut {
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

    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

impl<T: Dist + 'static> IndexedDistributedIterator for GlobalLockDistIterMut<T> {
    fn iterator_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.subarray_index_from_local(index, 1);
        g_index
    }
}

impl<T: Dist + 'static> LocalIterator for GlobalLockLocalIterMut<T> {
    type Item = &'static mut T;
    type Array = GlobalLockArray<T>;
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
        GlobalLockLocalIterMut {
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

    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

impl<T: Dist + 'static> IndexedLocalIterator for GlobalLockLocalIterMut<T> {
    fn iterator_index(&self, index: usize) -> Option<usize> {
        if index < self.data.len() {
            Some(index) //everyone at this point as calculated the actual index (cause we are local only) so just return it
        } else {
            None
        }
    }
}

impl<T: Dist> LamellarArrayIterators<T> for GlobalLockReadGuard<T> {
    // type Array = GlobalLockArray<T>;
    type DistIter = GlobalLockDistIter<T>;
    type LocalIter = GlobalLockLocalIter<T>;
    type OnesidedIter = OneSidedIter<T, GlobalLockArray<T>>;

    fn dist_iter(&self) -> Self::DistIter {
        GlobalLockDistIter {
            data: self.array.clone(),
            lock: Arc::new(Mutex::new(Some(self.lock_guard.clone()))),
            cur_i: 0,
            end_i: 0,
            _marker: PhantomData,
        }
    }

    fn local_iter(&self) -> Self::LocalIter {
        GlobalLockLocalIter {
            data: self.array.clone(),
            lock: Arc::new(Mutex::new(Some(self.lock_guard.clone()))),
            cur_i: 0,
            end_i: 0,
            _marker: PhantomData,
        }
    }

    fn onesided_iter(&self) -> Self::OnesidedIter {
        OneSidedIter::new(&self.array, 1)
    }

    fn buffered_onesided_iter(&self, buf_size: usize) -> Self::OnesidedIter {
        OneSidedIter::new(&self.array, std::cmp::min(buf_size, self.array.len()))
    }
}

impl<T: Dist> LamellarArrayIterators<T> for GlobalLockArray<T> {
    type DistIter = GlobalLockDistIter<T>;
    type LocalIter = GlobalLockLocalIter<T>;
    type OnesidedIter = OneSidedIter<T, GlobalLockArray<T>>;

    fn dist_iter(&self) -> Self::DistIter {
        GlobalLockDistIter {
            data: self.clone(),
            lock: Arc::new(Mutex::new(None)),
            cur_i: 0,
            end_i: 0,
            _marker: PhantomData,
        }
    }

    fn local_iter(&self) -> Self::LocalIter {
        GlobalLockLocalIter {
            data: self.clone(),
            lock: Arc::new(Mutex::new(None)),
            cur_i: 0,
            end_i: 0,
            _marker: PhantomData,
        }
    }

    fn onesided_iter(&self) -> Self::OnesidedIter {
        OneSidedIter::new(self, 1)
    }

    fn buffered_onesided_iter(&self, buf_size: usize) -> Self::OnesidedIter {
        OneSidedIter::new(self, std::cmp::min(buf_size, self.array.len()))
    }
}

impl<T: Dist> LamellarArrayMutIterators<T> for GlobalLockArray<T> {
    type DistIter = GlobalLockDistIterMut<T>;
    type LocalIter = GlobalLockLocalIterMut<T>;

    fn dist_iter_mut(&self) -> Self::DistIter {
        GlobalLockDistIterMut {
            data: self.clone(),
            lock: Arc::new(Mutex::new(None)),
            cur_i: 0,
            end_i: 0,
            _marker: PhantomData,
        }
    }

    fn local_iter_mut(&self) -> Self::LocalIter {
        GlobalLockLocalIterMut {
            data: self.clone(),
            lock: Arc::new(Mutex::new(None)),
            cur_i: 0,
            end_i: 0,
            _marker: PhantomData,
        }
    }
}

impl<T: Dist> DistIteratorLauncher for GlobalLockArray<T> {}

impl<T: Dist> LocalIteratorLauncher for GlobalLockArray<T> {}

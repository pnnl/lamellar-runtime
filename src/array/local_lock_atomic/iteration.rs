use crate::array::iterator::distributed_iterator::*;
use crate::array::iterator::local_iterator::*;
use crate::array::iterator::one_sided_iterator::OneSidedIter;
use crate::array::iterator::{private::*, LamellarArrayIterators, LamellarArrayMutIterators};
use crate::array::local_lock_atomic::*;
use crate::array::private::LamellarArrayPrivate;
use crate::array::r#unsafe::private::UnsafeArrayInner;
use crate::array::*;
use crate::darc::local_rw_darc::LocalRwDarcWriteGuard;
use crate::memregion::Dist;

impl<T> InnerArray for LocalLockArray<T> {
    fn as_inner(&self) -> &UnsafeArrayInner {
        &self.array.inner
    }
}

//#[doc(hidden)]
#[derive(Clone)]
pub struct LocalLockDistIter<'a, T: Dist> {
    data: LocalLockArray<T>,
    lock: Arc<LocalRwDarcReadGuard<()>>,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'a T>,
}

impl<'a, T: Dist> IterClone for LocalLockDistIter<'a, T> {
    fn iter_clone(&self, _: Sealed) -> Self {
        LocalLockDistIter {
            data: self.data.clone(),
            lock: self.lock.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
            _marker: PhantomData,
        }
    }
}

impl<'a, T: Dist> std::fmt::Debug for LocalLockDistIter<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LocalLockDistIter{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

//#[doc(hidden)]
#[derive(Clone)]
pub struct LocalLockLocalIter<'a, T: Dist> {
    data: LocalLockArray<T>,
    lock: Arc<LocalRwDarcReadGuard<()>>,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'a T>,
}

impl<'a, T: Dist> IterClone for LocalLockLocalIter<'a, T> {
    fn iter_clone(&self, _: Sealed) -> Self {
        LocalLockLocalIter {
            data: self.data.clone(),
            lock: self.lock.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
            _marker: PhantomData,
        }
    }
}

impl<'a, T: Dist> std::fmt::Debug for LocalLockLocalIter<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LocalLockLocalIter{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

impl<T: Dist + 'static> DistributedIterator for LocalLockDistIter<'static, T> {
    type Item = &'static T;
    type Array = LocalLockArray<T>;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
        LocalLockDistIter {
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
impl<T: Dist + 'static> IndexedDistributedIterator for LocalLockDistIter<'static, T> {
    fn iterator_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.subarray_index_from_local(index, 1);
        g_index
    }
}

impl<T: Dist + 'static> LocalIterator for LocalLockLocalIter<'static, T> {
    type Item = &'static T;
    type Array = LocalLockArray<T>;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
        LocalLockLocalIter {
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

impl<T: Dist + 'static> IndexedLocalIterator for LocalLockLocalIter<'static, T> {
    fn iterator_index(&self, index: usize) -> Option<usize> {
        if index < self.data.len() {
            Some(index) //everyone at this point as calculated the actual index (cause we are local only) so just return it
        } else {
            None
        }
    }
}

pub struct LocalLockDistIterMut<'a, T: Dist> {
    data: LocalLockArray<T>,
    lock: Arc<LocalRwDarcWriteGuard<()>>,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'a T>,
}

impl<'a, T: Dist> IterClone for LocalLockDistIterMut<'a, T> {
    fn iter_clone(&self, _: Sealed) -> Self {
        LocalLockDistIterMut {
            data: self.data.clone(),
            lock: self.lock.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
            _marker: PhantomData,
        }
    }
}

impl<'a, T: Dist> std::fmt::Debug for LocalLockDistIterMut<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LocalLockDistIterMut{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

pub struct LocalLockLocalIterMut<'a, T: Dist> {
    data: LocalLockArray<T>,
    lock: Arc<LocalRwDarcWriteGuard<()>>,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'a T>,
}

impl<'a, T: Dist> IterClone for LocalLockLocalIterMut<'a, T> {
    fn iter_clone(&self, _: Sealed) -> Self {
        LocalLockLocalIterMut {
            data: self.data.clone(),
            lock: self.lock.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
            _marker: PhantomData,
        }
    }
}

impl<'a, T: Dist> std::fmt::Debug for LocalLockLocalIterMut<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LocalLockLocalIterMut{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

impl<T: Dist + 'static> DistributedIterator for LocalLockDistIterMut<'static, T> {
    type Item = &'static mut T;
    type Array = LocalLockArray<T>;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
        LocalLockDistIterMut {
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

impl<T: Dist + 'static> IndexedDistributedIterator for LocalLockDistIterMut<'static, T> {
    fn iterator_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.subarray_index_from_local(index, 1);
        g_index
    }
}

impl<T: Dist + 'static> LocalIterator for LocalLockLocalIterMut<'static, T> {
    type Item = &'static mut T;
    type Array = LocalLockArray<T>;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
        LocalLockLocalIterMut {
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

impl<T: Dist + 'static> IndexedLocalIterator for LocalLockLocalIterMut<'static, T> {
    fn iterator_index(&self, index: usize) -> Option<usize> {
        if index < self.data.len() {
            Some(index) //everyone at this point as calculated the actual index (cause we are local only) so just return it
        } else {
            None
        }
    }
}

impl<T: Dist> LamellarArrayIterators<T> for LocalLockArray<T> {
    // type Array = LocalLockArray<T>;
    type DistIter = LocalLockDistIter<'static, T>;
    type LocalIter = LocalLockLocalIter<'static, T>;
    type OnesidedIter = OneSidedIter<'static, T, Self>;

    fn dist_iter(&self) -> Self::DistIter {
        // let the_array: LocalLockArray<T> = self.clone();
        let lock: LocalRwDarc<()> = self.lock.clone();
        let lock = Arc::new(self.array.block_on(async move { lock.read().await }));
        self.barrier();
        LocalLockDistIter {
            data: self.clone(),
            lock: lock,
            cur_i: 0,
            end_i: 0,
            _marker: PhantomData,
        }
    }

    fn local_iter(&self) -> Self::LocalIter {
        let lock: LocalRwDarc<()> = self.lock.clone();
        let lock = Arc::new(self.array.block_on(async move { lock.read().await }));
        LocalLockLocalIter {
            data: self.clone(),
            lock: lock,
            cur_i: 0,
            end_i: 0,
            _marker: PhantomData,
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

impl<T: Dist> LamellarArrayMutIterators<T> for LocalLockArray<T> {
    type DistIter = LocalLockDistIterMut<'static, T>;
    type LocalIter = LocalLockLocalIterMut<'static, T>;

    fn dist_iter_mut(&self) -> Self::DistIter {
        let lock: LocalRwDarc<()> = self.lock.clone();
        let lock = Arc::new(self.array.block_on(async move { lock.write().await }));
        // self.barrier();
        // println!("dist_iter thread {:?} got lock",std::thread::current().id());
        LocalLockDistIterMut {
            data: self.clone(),
            lock: lock,
            cur_i: 0,
            end_i: 0,
            _marker: PhantomData,
        }
    }

    fn local_iter_mut(&self) -> Self::LocalIter {
        // println!("trying to get write lock for iter");
        let lock: LocalRwDarc<()> = self.lock.clone();
        let lock = Arc::new(self.array.block_on(async move { lock.write().await }));
        // println!("got write lock for iter");
        LocalLockLocalIterMut {
            data: self.clone(),
            lock: lock,
            cur_i: 0,
            end_i: 0,
            _marker: PhantomData,
        }
    }
}

impl<T: Dist> DistIteratorLauncher for LocalLockArray<T> {}

impl<T: Dist> LocalIteratorLauncher for LocalLockArray<T> {}

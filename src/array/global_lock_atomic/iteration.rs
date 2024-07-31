use crate::array::global_lock_atomic::*;

use crate::array::iterator::distributed_iterator::*;
use crate::array::iterator::local_iterator::*;
use crate::array::iterator::one_sided_iterator::OneSidedIter;
use crate::array::iterator::{
    private::{IterClone, Sealed},
    LamellarArrayIterators, LamellarArrayMutIterators,
};
use crate::array::private::LamellarArrayPrivate;
use crate::array::r#unsafe::private::UnsafeArrayInner;
use crate::array::*;
use crate::memregion::Dist;

impl<T> InnerArray for GlobalLockArray<T> {
    fn as_inner(&self) -> &UnsafeArrayInner {
        &self.array.inner
    }
}

//#[doc(hidden)]
#[derive(Clone)]
pub struct GlobalLockDistIter<T: Dist> {
    array_guard: GlobalLockReadGuard<T>,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'static T>,
}

impl<T: Dist> IterClone for GlobalLockDistIter<T> {
    fn iter_clone(&self, _: Sealed) -> Self {
        GlobalLockDistIter {
            array_guard: self.array_guard.clone(),
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
            self.array_guard.array.len(),
            self.cur_i,
            self.end_i
        )
    }
}

//#[doc(hidden)]
#[derive(Clone)]
pub struct GlobalLockLocalIter<T: Dist> {
    array_guard: GlobalLockReadGuard<T>,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'static T>,
}

impl<T: Dist> IterClone for GlobalLockLocalIter<T> {
    fn iter_clone(&self, _: Sealed) -> Self {
        GlobalLockLocalIter {
            array_guard: self.array_guard.clone(),
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
            self.array_guard.array.len(),
            self.cur_i,
            self.end_i
        )
    }
}

impl<T: Dist + 'static> DistributedIterator for GlobalLockDistIter<T> {
    type Item = &'static T;
    type Array = GlobalLockArray<T>;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.array_guard.array.num_elems_local();
        // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
        GlobalLockDistIter {
            array_guard: self.array_guard.clone(),
            cur_i: std::cmp::min(start_i, max_i),
            end_i: std::cmp::min(start_i + cnt, max_i),
            _marker: PhantomData,
        }
    }
    fn array(&self) -> Self::Array {
        self.array_guard.array.clone()
    }
    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_i < self.end_i {
            self.cur_i += 1;
            unsafe {
                self.array_guard
                    .array
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
        let g_index = self.array_guard.array.subarray_index_from_local(index, 1);
        g_index
    }
}

impl<T: Dist + 'static> LocalIterator for GlobalLockLocalIter<T> {
    type Item = &'static T;
    type Array = GlobalLockArray<T>;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.array_guard.array.num_elems_local();
        // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
        GlobalLockLocalIter {
            array_guard: self.array_guard.clone(),
            cur_i: std::cmp::min(start_i, max_i),
            end_i: std::cmp::min(start_i + cnt, max_i),
            _marker: PhantomData,
        }
    }
    fn array(&self) -> Self::Array {
        self.array_guard.array.clone()
    }
    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_i < self.end_i {
            self.cur_i += 1;
            unsafe {
                self.array_guard
                    .array
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
        if index < self.array_guard.array.len() {
            Some(index) //everyone at this point as calculated the actual index (cause we are local only) so just return it
        } else {
            None
        }
    }
}

pub struct GlobalLockDistIterMut<T: Dist> {
    data: GlobalLockArray<T>,
    lock: Arc<GlobalRwDarcCollectiveWriteGuard<()>>,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'static T>,
}

impl<T: Dist> IterClone for GlobalLockDistIterMut<T> {
    fn iter_clone(&self, _: Sealed) -> Self {
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
    lock: Arc<GlobalRwDarcWriteGuard<()>>,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'static T>,
}

impl<T: Dist> IterClone for GlobalLockLocalIterMut<T> {
    fn iter_clone(&self, _: Sealed) -> Self {
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
    fn init(&self, start_i: usize, cnt: usize) -> Self {
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
    fn init(&self, start_i: usize, cnt: usize) -> Self {
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
    type OnesidedIter = OneSidedIter<'static, T, GlobalLockArray<T>>;

    fn dist_iter(&self) -> Self::DistIter {
        GlobalLockDistIter {
            array_guard: self.clone(),
            cur_i: 0,
            end_i: 0,
            _marker: PhantomData,
        }
    }

    fn local_iter(&self) -> Self::LocalIter {
        GlobalLockLocalIter {
            array_guard: self.clone(),
            cur_i: 0,
            end_i: 0,
            _marker: PhantomData,
        }
    }

    fn onesided_iter(&self) -> Self::OnesidedIter {
        OneSidedIter::new(self.array.clone().into(), self.array.team_rt().clone(), 1)
    }

    fn buffered_onesided_iter(&self, buf_size: usize) -> Self::OnesidedIter {
        OneSidedIter::new(
            self.array.clone().into(),
            self.array.team_rt().clone(),
            std::cmp::min(buf_size, self.array.len()),
        )
    }
}

impl<T: Dist> LamellarArrayMutIterators<T> for GlobalLockArray<T> {
    type DistIter = GlobalLockDistIterMut<T>;
    type LocalIter = GlobalLockLocalIterMut<T>;

    fn dist_iter_mut(&self) -> Self::DistIter {
        let lock: GlobalRwDarc<()> = self.lock.clone();
        let lock = Arc::new(
            self.array
                .block_on(async move { lock.collective_write().await }),
        );
        // self.barrier();
        // println!("dist_iter thread {:?} got lock",std::thread::current().id());
        GlobalLockDistIterMut {
            data: self.clone(),
            lock: lock,
            cur_i: 0,
            end_i: 0,
            _marker: PhantomData,
        }
    }

    fn local_iter_mut(&self) -> Self::LocalIter {
        let lock: GlobalRwDarc<()> = self.lock.clone();
        let lock = Arc::new(self.array.block_on(async move { lock.write().await }));
        GlobalLockLocalIterMut {
            data: self.clone(),
            lock: lock,
            cur_i: 0,
            end_i: 0,
            _marker: PhantomData,
        }
    }
}

impl<T: Dist> DistIteratorLauncher for GlobalLockArray<T> {}

impl<T: Dist> LocalIteratorLauncher for GlobalLockArray<T> {}

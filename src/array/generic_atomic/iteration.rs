use crate::array::generic_atomic::*;

use crate::array::iterator::distributed_iterator::*;
use crate::array::iterator::local_iterator::*;
use crate::array::iterator::one_sided_iterator::OneSidedIter;
use crate::array::iterator::{private::*, LamellarArrayIterators, LamellarArrayMutIterators};
use crate::array::r#unsafe::private::UnsafeArrayInner;
use crate::array::*;
use crate::memregion::Dist;

use self::iterator::IterLockFuture;
// use parking_lot::{
//     lock_api::{RwLockReadGuardArc, RwLockWriteGuardArc},
//     RawRwLock,
// };

impl<T> InnerArray for GenericAtomicArray<T> {
    fn as_inner(&self) -> &UnsafeArrayInner {
        &self.array.inner
    }
}

//#[doc(hidden)]
#[derive(Clone)]
pub struct GenericAtomicDistIter<T: Dist> {
    data: GenericAtomicArray<T>,
    cur_i: usize,
    end_i: usize,
}

impl<T: Dist> InnerIter for GenericAtomicDistIter<T> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
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

//#[doc(hidden)]
#[derive(Clone)]
pub struct GenericAtomicLocalIter<T: Dist> {
    data: GenericAtomicArray<T>,
    cur_i: usize,
    end_i: usize,
}

impl<T: Dist> InnerIter for GenericAtomicLocalIter<T> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
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
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Self {
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
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Self {
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
    type OnesidedIter = OneSidedIter<T, Self>;
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
        OneSidedIter::new(self, 1)
    }

    fn buffered_onesided_iter(&self, buf_size: usize) -> Self::OnesidedIter {
        OneSidedIter::new(self, std::cmp::min(buf_size, self.len()))
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

impl<T: Dist> DistIteratorLauncher for GenericAtomicArray<T> {}

impl<T: Dist> LocalIteratorLauncher for GenericAtomicArray<T> {}

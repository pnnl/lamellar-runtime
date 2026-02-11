use crate::array::atomic::*;

use crate::array::iterator::distributed_iterator::*;
use crate::array::iterator::local_iterator::*;
use crate::array::iterator::one_sided_iterator::OneSidedIter;
use crate::array::iterator::{
    private::{InnerIter, Sealed},
    LamellarArrayIterators, LamellarArrayMutIterators,
};
use crate::array::r#unsafe::private::UnsafeArrayInner;
use crate::array::*;
use crate::memregion::Dist;

use self::iterator::IterLockFuture;

impl<T: Dist> InnerArray for AtomicArray<T> {
    fn as_inner(&self) -> &UnsafeArrayInner {
        match &self {
            AtomicArray::NativeAtomicArray(a) => a.as_inner(),
            AtomicArray::GenericAtomicArray(a) => a.as_inner(),
            AtomicArray::NetworkAtomicArray(a) => a.as_inner(),
        }
    }
}

#[derive(Clone)]
pub struct AtomicDistIter<T: Dist> {
    //dont need a AtomicDistIterMut in this case as any updates to inner elements are atomic
    data: AtomicArray<T>,
    cur_i: usize,
    end_i: usize,
}

impl<T: Dist> InnerIter for AtomicDistIter<T> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        AtomicDistIter {
            data: self.data.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
        }
    }
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

impl<T: Dist> InnerIter for AtomicLocalIter<T> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        AtomicLocalIter {
            data: self.data.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
        }
    }
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
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Self {
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
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Self {
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
    type OnesidedIter = OneSidedIter<T, Self>;
    fn dist_iter(&self) -> Self::DistIter {
        AtomicDistIter::new(self.clone(), 0, 0)
    }

    fn local_iter(&self) -> Self::LocalIter {
        AtomicLocalIter::new(self.clone(), 0, 0)
    }

    fn onesided_iter(&self) -> Self::OnesidedIter {
        OneSidedIter::new(self, 1)
    }

    fn buffered_onesided_iter(&self, buf_size: usize) -> Self::OnesidedIter {
        OneSidedIter::new(self, std::cmp::min(buf_size, self.len()))
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

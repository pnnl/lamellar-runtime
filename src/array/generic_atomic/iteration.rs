use std::ops::{
    AddAssign, BitAndAssign, BitOrAssign, BitXorAssign, DivAssign, MulAssign, RemAssign, ShlAssign,
    ShrAssign, SubAssign,
};

use parking_lot::Mutex;

use crate::{
    array::{
        iterator::{
            distributed_iterator::DistIteratorLauncher,
            local_iterator::LocalIteratorLauncher,
            one_sided_iterator::OneSidedIter,
            private::{InnerIter, Sealed},
            IterLockFuture,
        },
        private::LamellarArrayPrivate,
        r#unsafe::private::UnsafeArrayInner,
        ElementArithmeticOps, ElementBitWiseOps, ElementShiftOps, InnerArray,
    },
    memregion::Dist,
    DistributedIterator, GenericAtomicArray, IndexedDistributedIterator, LamellarArray,
    LamellarArrayIterators, LamellarArrayMutIterators, LocalIterator, Remote,
};

impl<T: Remote> InnerArray for GenericAtomicArray<T> {
    fn as_inner(&self) -> &UnsafeArrayInner {
        &self.array.inner
    }
}

// //#[doc(hidden)]
// #[derive(Clone)]
// pub struct GenericAtomicDistIter<T: Dist> {
//     data: GenericAtomicArray<T>,
//     cur_i: usize,
//     end_i: usize,
// }

// impl<T: Dist> InnerIter for GenericAtomicDistIter<T> {
//     fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
//         None
//     }
//     fn iter_clone(&self, _s: Sealed) -> Self {
//         GenericAtomicDistIter {
//             data: self.data.clone(),
//             cur_i: self.cur_i,
//             end_i: self.end_i,
//         }
//     }
// }

// impl<T: Dist> std::fmt::Debug for GenericAtomicDistIter<T> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(
//             f,
//             "GenericAtomicDistIter{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
//             self.data.len(),
//             self.cur_i,
//             self.end_i
//         )
//     }
// }

// //#[doc(hidden)]
// #[derive(Clone)]
// pub struct GenericAtomicLocalIter<T: Dist> {
//     data: GenericAtomicArray<T>,
//     cur_i: usize,
//     end_i: usize,
// }

// impl<T: Dist> InnerIter for GenericAtomicLocalIter<T> {
//     fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
//         None
//     }
//     fn iter_clone(&self, _s: Sealed) -> Self {
//         GenericAtomicLocalIter {
//             data: self.data.clone(),
//             cur_i: self.cur_i,
//             end_i: self.end_i,
//         }
//     }
// }

// impl<T: Dist> std::fmt::Debug for GenericAtomicLocalIter<T> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(
//             f,
//             "GenericAtomicLocalIter{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
//             self.data.len(),
//             self.cur_i,
//             self.end_i
//         )
//     }
// }

// impl<T: Dist> DistributedIterator for GenericAtomicDistIter<T> {
//     type Item = GenericAtomicElement<T>;
//     type Array = GenericAtomicArray<T>;
//     fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Self {
//         let max_i = self.data.num_elems_local();
//         // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
//         GenericAtomicDistIter {
//             data: self.data.clone(),
//             cur_i: std::cmp::min(start_i, max_i),
//             end_i: std::cmp::min(start_i + cnt, max_i),
//         }
//     }
//     fn array(&self) -> Self::Array {
//         self.data.clone()
//     }
//     fn next(&mut self) -> Option<Self::Item> {
//         if self.cur_i < self.end_i {
//             self.cur_i += 1;
//             Some(GenericAtomicElement {
//                 array: self.data.clone(),
//                 local_index: self.cur_i - 1,
//             })
//         } else {
//             None
//         }
//     }
//     fn elems(&self, in_elems: usize) -> usize {
//         in_elems
//     }
//     fn advance_index(&mut self, count: usize) {
//         self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
//     }
// }
// impl<T: Dist> IndexedDistributedIterator for GenericAtomicDistIter<T> {
//     fn iterator_index(&self, index: usize) -> Option<usize> {
//         let g_index = self.data.subarray_index_from_local(index, 1);
//         g_index
//     }
// }

// impl<T: Dist> LocalIterator for GenericAtomicLocalIter<T> {
//     type Item = GenericAtomicElement<T>;
//     type Array = GenericAtomicArray<T>;
//     fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Self {
//         let max_i = self.data.num_elems_local();
//         // println!("init generic_atomic start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?} {:?}",start_i,cnt, start_i+cnt,max_i,std::thread::current().id());
//         GenericAtomicLocalIter {
//             data: self.data.clone(),
//             cur_i: std::cmp::min(start_i, max_i),
//             end_i: std::cmp::min(start_i + cnt, max_i),
//         }
//     }
//     fn array(&self) -> Self::Array {
//         self.data.clone()
//     }
//     fn next(&mut self) -> Option<Self::Item> {
//         if self.cur_i < self.end_i {
//             self.cur_i += 1;
//             Some(GenericAtomicElement {
//                 array: self.data.clone(),
//                 local_index: self.cur_i - 1,
//             })
//         } else {
//             None
//         }
//     }
//     fn elems(&self, in_elems: usize) -> usize {
//         in_elems
//     }

//     fn advance_index(&mut self, count: usize) {
//         self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
//     }
// }

// impl<T: Dist> LamellarArrayIterators<T> for GenericAtomicArray<T> {
//     // type Array = GenericAtomicArray<T>;
//     type DistIter = GenericAtomicDistIter<T>;
//     type LocalIter = GenericAtomicLocalIter<T>;
//     type OnesidedIter = OneSidedIter<T, Self>;
//     fn dist_iter(&self) -> Self::DistIter {
//         GenericAtomicDistIter {
//             data: self.clone(),
//             cur_i: 0,
//             end_i: 0,
//         }
//     }

//     fn local_iter(&self) -> Self::LocalIter {
//         GenericAtomicLocalIter {
//             data: self.clone(),
//             cur_i: 0,
//             end_i: 0,
//         }
//     }

//     fn onesided_iter(&self) -> Self::OnesidedIter {
//         OneSidedIter::new(self, 1)
//     }

//     fn buffered_onesided_iter(&self, buf_size: usize) -> Self::OnesidedIter {
//         OneSidedIter::new(self, std::cmp::min(buf_size, self.len()))
//     }
// }

// impl<T: Dist> LamellarArrayMutIterators<T> for GenericAtomicArray<T> {
//     type DistIter = GenericAtomicDistIter<T>;
//     type LocalIter = GenericAtomicLocalIter<T>;

//     fn dist_iter_mut(&self) -> Self::DistIter {
//         GenericAtomicDistIter {
//             data: self.clone(),
//             cur_i: 0,
//             end_i: 0,
//         }
//     }

//     fn local_iter_mut(&self) -> Self::LocalIter {
//         GenericAtomicLocalIter {
//             data: self.clone(),
//             cur_i: 0,
//             end_i: 0,
//         }
//     }
// }

// impl<T: Dist> DistIteratorLauncher for GenericAtomicArray<T> {}

// impl<T: Dist> LocalIteratorLauncher for GenericAtomicArray<T> {}

/// A zero-clone element for framework iterators over `GenericAtomicArray`.
///
/// Stores raw pointers into Darc-backed heap memory (stable addresses) plus
/// a reference to the per-element mutex.  The lifetime `'a` is tracked only
/// through `PhantomData` so we can safely use `'static` when the iterator
/// keeps the Darc alive — the same technique used by `LocalLockLocalIter`.
///
/// All mutation goes through `ptr::read` + local copy + `ptr::write` to avoid
/// the `invalid_reference_casting` lint that fires when writing through
/// `*mut T` derived from a `&T`.
#[doc(hidden)]
pub struct GenericAtomicDistIterElement<'a, T: Dist> {
    pub(crate) lock: &'a Mutex<()>,
    pub(crate) value: *const T,
    _marker: std::marker::PhantomData<&'a T>,
}

impl<'a, T: Dist> GenericAtomicDistIterElement<'a, T> {
    pub fn load(&self) -> T {
        let _lock = self.lock.lock();
        unsafe { std::ptr::read(self.value) }
    }
    pub fn store(&self, val: T) {
        let _lock = self.lock.lock();
        unsafe { std::ptr::write(self.value as *mut T, val) }
    }
    pub fn swap(&self, val: T) -> T {
        let _lock = self.lock.lock();
        unsafe {
            let old = std::ptr::read(self.value);
            std::ptr::write(self.value as *mut T, val);
            old
        }
    }
}

impl<'a, T: ElementArithmeticOps> GenericAtomicDistIterElement<'a, T> {
    pub fn fetch_add(&self, val: T) -> T {
        let _lock = self.lock.lock();
        unsafe {
            let old = std::ptr::read(self.value);
            let mut new_val = old;
            new_val += val;
            std::ptr::write(self.value as *mut T, new_val);
            old
        }
    }
    pub fn fetch_sub(&self, val: T) -> T {
        let _lock = self.lock.lock();
        unsafe {
            let old = std::ptr::read(self.value);
            let mut new_val = old;
            new_val -= val;
            std::ptr::write(self.value as *mut T, new_val);
            old
        }
    }
    pub fn fetch_mul(&self, val: T) -> T {
        let _lock = self.lock.lock();
        unsafe {
            let old = std::ptr::read(self.value);
            let mut new_val = old;
            new_val *= val;
            std::ptr::write(self.value as *mut T, new_val);
            old
        }
    }
    pub fn fetch_div(&self, val: T) -> T {
        let _lock = self.lock.lock();
        unsafe {
            let old = std::ptr::read(self.value);
            let mut new_val = old;
            new_val /= val;
            std::ptr::write(self.value as *mut T, new_val);
            old
        }
    }
    pub fn fetch_rem(&self, val: T) -> T {
        let _lock = self.lock.lock();
        unsafe {
            let old = std::ptr::read(self.value);
            let mut new_val = old;
            new_val %= val;
            std::ptr::write(self.value as *mut T, new_val);
            old
        }
    }
}

impl<'a, T: Dist + std::cmp::Eq> GenericAtomicDistIterElement<'a, T> {
    pub fn compare_exchange(&self, current: T, new: T) -> Result<T, T> {
        let _lock = self.lock.lock();
        let current_val = unsafe { std::ptr::read(self.value) };
        if current_val == current {
            unsafe { std::ptr::write(self.value as *mut T, new) }
            Ok(current_val)
        } else {
            Err(current_val)
        }
    }
}

impl<'a, T: Dist + std::cmp::PartialEq + std::cmp::PartialOrd + std::ops::Sub<Output = T>>
    GenericAtomicDistIterElement<'a, T>
{
    pub fn compare_exchange_epsilon(&self, current: T, new: T, eps: T) -> Result<T, T> {
        let _lock = self.lock.lock();
        let current_val = unsafe { std::ptr::read(self.value) };
        let same = if current_val > current {
            current_val - current < eps
        } else {
            current - current_val < eps
        };
        if same {
            unsafe { std::ptr::write(self.value as *mut T, new) }
            Ok(current_val)
        } else {
            Err(current_val)
        }
    }
}

impl<'a, T: ElementBitWiseOps + 'static> GenericAtomicDistIterElement<'a, T> {
    pub fn fetch_and(&self, val: T) -> T {
        let _lock = self.lock.lock();
        unsafe {
            let old = std::ptr::read(self.value);
            let mut new_val = old;
            new_val &= val;
            std::ptr::write(self.value as *mut T, new_val);
            old
        }
    }
    pub fn fetch_or(&self, val: T) -> T {
        let _lock = self.lock.lock();
        unsafe {
            let old = std::ptr::read(self.value);
            let mut new_val = old;
            new_val |= val;
            std::ptr::write(self.value as *mut T, new_val);
            old
        }
    }
    pub fn fetch_xor(&self, val: T) -> T {
        let _lock = self.lock.lock();
        unsafe {
            let old = std::ptr::read(self.value);
            let mut new_val = old;
            new_val ^= val;
            std::ptr::write(self.value as *mut T, new_val);
            old
        }
    }
}

impl<'a, T: ElementShiftOps + 'static> GenericAtomicDistIterElement<'a, T> {
    pub fn fetch_shl(&self, val: T) -> T {
        let _lock = self.lock.lock();
        unsafe {
            let old = std::ptr::read(self.value);
            let mut new_val = old;
            new_val <<= val;
            std::ptr::write(self.value as *mut T, new_val);
            old
        }
    }
    pub fn fetch_shr(&self, val: T) -> T {
        let _lock = self.lock.lock();
        unsafe {
            let old = std::ptr::read(self.value);
            let mut new_val = old;
            new_val >>= val;
            std::ptr::write(self.value as *mut T, new_val);
            old
        }
    }
}

impl<'a, T: Dist + ElementArithmeticOps> AddAssign<T> for GenericAtomicDistIterElement<'a, T> {
    fn add_assign(&mut self, val: T) {
        let _lock = self.lock.lock();
        unsafe {
            let mut tmp = std::ptr::read(self.value);
            tmp += val;
            std::ptr::write(self.value as *mut T, tmp);
        }
    }
}
impl<'a, T: Dist + ElementArithmeticOps> SubAssign<T> for GenericAtomicDistIterElement<'a, T> {
    fn sub_assign(&mut self, val: T) {
        let _lock = self.lock.lock();
        unsafe {
            let mut tmp = std::ptr::read(self.value);
            tmp -= val;
            std::ptr::write(self.value as *mut T, tmp);
        }
    }
}
impl<'a, T: Dist + ElementArithmeticOps> MulAssign<T> for GenericAtomicDistIterElement<'a, T> {
    fn mul_assign(&mut self, val: T) {
        let _lock = self.lock.lock();
        unsafe {
            let mut tmp = std::ptr::read(self.value);
            tmp *= val;
            std::ptr::write(self.value as *mut T, tmp);
        }
    }
}
impl<'a, T: Dist + ElementArithmeticOps> DivAssign<T> for GenericAtomicDistIterElement<'a, T> {
    fn div_assign(&mut self, val: T) {
        let _lock = self.lock.lock();
        unsafe {
            let mut tmp = std::ptr::read(self.value);
            tmp /= val;
            std::ptr::write(self.value as *mut T, tmp);
        }
    }
}
impl<'a, T: Dist + ElementArithmeticOps> RemAssign<T> for GenericAtomicDistIterElement<'a, T> {
    fn rem_assign(&mut self, val: T) {
        let _lock = self.lock.lock();
        unsafe {
            let mut tmp = std::ptr::read(self.value);
            tmp %= val;
            std::ptr::write(self.value as *mut T, tmp);
        }
    }
}
impl<'a, T: Dist + ElementBitWiseOps> BitAndAssign<T> for GenericAtomicDistIterElement<'a, T> {
    fn bitand_assign(&mut self, val: T) {
        let _lock = self.lock.lock();
        unsafe {
            let mut tmp = std::ptr::read(self.value);
            tmp &= val;
            std::ptr::write(self.value as *mut T, tmp);
        }
    }
}
impl<'a, T: Dist + ElementBitWiseOps> BitOrAssign<T> for GenericAtomicDistIterElement<'a, T> {
    fn bitor_assign(&mut self, val: T) {
        let _lock = self.lock.lock();
        unsafe {
            let mut tmp = std::ptr::read(self.value);
            tmp |= val;
            std::ptr::write(self.value as *mut T, tmp);
        }
    }
}
impl<'a, T: Dist + ElementBitWiseOps> BitXorAssign<T> for GenericAtomicDistIterElement<'a, T> {
    fn bitxor_assign(&mut self, val: T) {
        let _lock = self.lock.lock();
        unsafe {
            let mut tmp = std::ptr::read(self.value);
            tmp ^= val;
            std::ptr::write(self.value as *mut T, tmp);
        }
    }
}
impl<'a, T: Dist + ElementShiftOps> ShlAssign<T> for GenericAtomicDistIterElement<'a, T> {
    fn shl_assign(&mut self, val: T) {
        let _lock = self.lock.lock();
        unsafe {
            let mut tmp = std::ptr::read(self.value);
            tmp.shl_assign(val);
            std::ptr::write(self.value as *mut T, tmp);
        }
    }
}
impl<'a, T: Dist + ElementShiftOps> ShrAssign<T> for GenericAtomicDistIterElement<'a, T> {
    fn shr_assign(&mut self, val: T) {
        let _lock = self.lock.lock();
        unsafe {
            let mut tmp = std::ptr::read(self.value);
            tmp.shr_assign(val);
            std::ptr::write(self.value as *mut T, tmp);
        }
    }
}

impl<'a, T: Dist + std::fmt::Debug> std::fmt::Debug for GenericAtomicDistIterElement<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _lock = self.lock.lock();
        let current_val = unsafe { std::ptr::read(self.value) };
        write!(f, "{current_val:?}")
    }
}

// Safety: GenericAtomicDistIterElement<'static, T> is Send because:
// - lock: &'static Mutex<()> is Send + Sync
// - value: *const T with PhantomData<&'static T> is Send when T: Sync (guaranteed by T: Dist)
unsafe impl<T: Dist> Send for GenericAtomicDistIterElement<'_, T> {}
unsafe impl<T: Dist> Sync for GenericAtomicDistIterElement<'_, T> {}

/// Construct a `GenericAtomicDistIterElement<'static, T>` for a given local index.
///
/// Safety: `data` must outlive the returned element. Both pointers are into
/// Darc-backed heap memory that stays valid as long as any clone of `data` is alive.
/// The iterator keeps `self.data` (a clone) alive for the whole iteration.
pub(crate) unsafe fn element_at_local_index<T: Dist>(
    data: &GenericAtomicArray<T>,
    local_i: usize,
) -> GenericAtomicDistIterElement<'static, T> {
    let lock_i = data
        .array
        .inner
        .pe_full_offset_for_local_index(data.array.inner.data.my_pe, local_i)
        .expect("invalid local index");
    let lock: &'static Mutex<()> = &*(&data.locks[lock_i] as *const Mutex<()>);
    let value: *const T = data.local_as_ptr().add(local_i);
    GenericAtomicDistIterElement {
        lock,
        value,
        _marker: std::marker::PhantomData,
    }
}

#[derive(Clone)]
pub struct GenericAtomicDistTestIter<T: Dist> {
    data: GenericAtomicArray<T>,
    cur_i: usize,
    end_i: usize,
}

impl<T: Dist> InnerIter for GenericAtomicDistTestIter<T> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        GenericAtomicDistTestIter {
            data: self.data.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
        }
    }
}

impl<T: Dist> std::fmt::Debug for GenericAtomicDistTestIter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GenericAtomicDistTestIter{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

#[derive(Clone)]
pub struct GenericAtomicLocalTestIter<T: Dist> {
    data: GenericAtomicArray<T>,
    cur_i: usize,
    end_i: usize,
}

impl<T: Dist> InnerIter for GenericAtomicLocalTestIter<T> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        GenericAtomicLocalTestIter {
            data: self.data.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
        }
    }
}

impl<T: Dist> std::fmt::Debug for GenericAtomicLocalTestIter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GenericAtomicLocalTestIter{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

impl<T: Dist + 'static> DistributedIterator for GenericAtomicDistTestIter<T> {
    type Item = GenericAtomicDistIterElement<'static, T>;
    type Array = GenericAtomicArray<T>;
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Self {
        let max_i = self.data.num_elems_local();
        GenericAtomicDistTestIter {
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
            let local_i = self.cur_i;
            self.cur_i += 1;
            Some(unsafe { element_at_local_index(&self.data, local_i) })
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

impl<T: Dist + 'static> IndexedDistributedIterator for GenericAtomicDistTestIter<T> {
    fn iterator_index(&self, index: usize) -> Option<usize> {
        self.data.subarray_index_from_local(index, 1)
    }
}

impl<T: Dist + 'static> LocalIterator for GenericAtomicLocalTestIter<T> {
    type Item = GenericAtomicDistIterElement<'static, T>;
    type Array = GenericAtomicArray<T>;
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Self {
        let max_i = self.data.num_elems_local();
        GenericAtomicLocalTestIter {
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
            let local_i = self.cur_i;
            self.cur_i += 1;
            Some(unsafe { element_at_local_index(&self.data, local_i) })
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
    type DistIter = GenericAtomicDistTestIter<T>;
    type LocalIter = GenericAtomicLocalTestIter<T>;
    type OnesidedIter = OneSidedIter<T, Self>;
    fn dist_iter(&self) -> Self::DistIter {
        GenericAtomicDistTestIter {
            data: self.clone(),
            cur_i: 0,
            end_i: 0,
        }
    }

    fn local_iter(&self) -> Self::LocalIter {
        GenericAtomicLocalTestIter {
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
    type DistIter = GenericAtomicDistTestIter<T>;
    type LocalIter = GenericAtomicLocalTestIter<T>;

    fn dist_iter_mut(&self) -> Self::DistIter {
        GenericAtomicDistTestIter {
            data: self.clone(),
            cur_i: 0,
            end_i: 0,
        }
    }

    fn local_iter_mut(&self) -> Self::LocalIter {
        GenericAtomicLocalTestIter {
            data: self.clone(),
            cur_i: 0,
            end_i: 0,
        }
    }
}

impl<T: Dist> DistIteratorLauncher for GenericAtomicArray<T> {}

impl<T: Dist> LocalIteratorLauncher for GenericAtomicArray<T> {}

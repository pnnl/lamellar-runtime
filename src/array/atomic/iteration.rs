use std::ops::{
    AddAssign, BitAndAssign, BitOrAssign, BitXorAssign, DivAssign, MulAssign, RemAssign, ShlAssign,
    ShrAssign, SubAssign,
};

use crate::array::atomic::*;
use crate::array::generic_atomic::iteration::{
    element_at_local_index as generic_element_at, GenericAtomicDistIterElement,
};
use crate::array::iterator::distributed_iterator::*;
use crate::array::iterator::local_iterator::*;
use crate::array::iterator::one_sided_iterator::OneSidedIter;
use crate::array::iterator::{
    private::{InnerIter, Sealed},
    LamellarArrayIterators, LamellarArrayMutIterators,
};
use crate::array::native_atomic::iteration::NativeAtomicDistIterElement;
use crate::array::network_atomic::iteration::NetworkAtomicDistIterElement;
use crate::array::private::LamellarArrayPrivate;
use crate::array::r#unsafe::private::UnsafeArrayInner;
use crate::array::*;
use crate::array::{ElementArithmeticOps, ElementBitWiseOps, ElementShiftOps};
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

/// Zero-clone element yielded by `AtomicDistIter` and `AtomicLocalIter`.
///
/// Dispatches to the appropriate per-variant element type based on which
/// `AtomicArray` variant backs the iterator.
pub enum AtomicDistIterElement<'a, T: Dist> {
    Native(NativeAtomicDistIterElement<'a, T>),
    Generic(GenericAtomicDistIterElement<'a, T>),
    Network(NetworkAtomicDistIterElement<'a, T>),
}

unsafe impl<T: Dist> Send for AtomicDistIterElement<'_, T> {}
unsafe impl<T: Dist> Sync for AtomicDistIterElement<'_, T> {}

impl<'a, T: Dist> AtomicDistIterElement<'a, T> {
    /// Returns the current value of the element.
    ///
    /// `AtomicDistIterElement` is yielded by [`AtomicArray::dist_iter_mut`].
    /// Use [`AtomicArray`] to obtain elements; do not construct this type directly.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// let _ = array.dist_iter_mut().for_each(|elem| {
    ///     let val = elem.load();
    /// }).block();
    ///```
    pub fn load(&self) -> T {
        match self {
            AtomicDistIterElement::Native(e) => e.load(),
            AtomicDistIterElement::Generic(e) => e.load(),
            AtomicDistIterElement::Network(e) => e.load(),
        }
    }
    /// Stores `val` into the element atomically.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// let _ = array.dist_iter_mut().for_each(|elem| {
    ///     elem.store(42);
    /// }).block();
    ///```
    pub fn store(&self, val: T) {
        match self {
            AtomicDistIterElement::Native(e) => e.store(val),
            AtomicDistIterElement::Generic(e) => e.store(val),
            AtomicDistIterElement::Network(e) => e.store(val),
        }
    }
    /// Atomically stores `val` and returns the previous value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// let _ = array.dist_iter_mut().for_each(|elem| {
    ///     let old = elem.swap(42);
    /// }).block();
    ///```
    pub fn swap(&self, val: T) -> T {
        match self {
            AtomicDistIterElement::Native(e) => e.swap(val),
            AtomicDistIterElement::Generic(e) => e.swap(val),
            AtomicDistIterElement::Network(e) => e.swap(val),
        }
    }
}

impl<'a, T: Dist + std::cmp::Eq> AtomicDistIterElement<'a, T> {
    /// Atomically replaces the element with `new` if it equals `current`; returns old value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// let _ = array.dist_iter_mut().for_each(|elem| {
    ///     let _ = elem.compare_exchange(0, 42);
    /// }).block();
    ///```
    pub fn compare_exchange(&self, current: T, new: T) -> Result<T, T> {
        match self {
            AtomicDistIterElement::Native(e) => e.compare_exchange(current, new),
            AtomicDistIterElement::Generic(e) => e.compare_exchange(current, new),
            AtomicDistIterElement::Network(e) => e.compare_exchange(current, new),
        }
    }
}

impl<'a, T: Dist + std::cmp::PartialEq + std::cmp::PartialOrd + std::ops::Sub<Output = T>>
    AtomicDistIterElement<'a, T>
{
    /// Atomically replaces element with `new` if `|current - elem| <= eps`; returns old value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<f64> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// let _ = array.dist_iter_mut().for_each(|elem| {
    ///     let _ = elem.compare_exchange_epsilon(0.0, 1.0, 1e-9);
    /// }).block();
    ///```
    pub fn compare_exchange_epsilon(&self, current: T, new: T, eps: T) -> Result<T, T> {
        match self {
            AtomicDistIterElement::Native(e) => e.compare_exchange_epsilon(current, new, eps),
            AtomicDistIterElement::Generic(e) => e.compare_exchange_epsilon(current, new, eps),
            AtomicDistIterElement::Network(e) => e.compare_exchange_epsilon(current, new, eps),
        }
    }
}

impl<'a, T: Dist + ElementArithmeticOps> AtomicDistIterElement<'a, T> {
    /// Atomically adds `val` and returns the previous value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// let _ = array.dist_iter_mut().for_each(|elem| {
    ///     let old = elem.fetch_add(1);
    /// }).block();
    ///```
    pub fn fetch_add(&self, val: T) -> T {
        match self {
            AtomicDistIterElement::Native(e) => e.fetch_add(val),
            AtomicDistIterElement::Generic(e) => e.fetch_add(val),
            AtomicDistIterElement::Network(e) => e.fetch_add(val),
        }
    }
    /// Atomically subtracts `val` and returns the previous value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// let _ = array.dist_iter_mut().for_each(|elem| {
    ///     let old = elem.fetch_sub(1);
    /// }).block();
    ///```
    pub fn fetch_sub(&self, val: T) -> T {
        match self {
            AtomicDistIterElement::Native(e) => e.fetch_sub(val),
            AtomicDistIterElement::Generic(e) => e.fetch_sub(val),
            AtomicDistIterElement::Network(e) => e.fetch_sub(val),
        }
    }
    /// Atomically multiplies by `val` and returns the previous value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// let _ = array.dist_iter_mut().for_each(|elem| {
    ///     let old = elem.fetch_mul(2);
    /// }).block();
    ///```
    pub fn fetch_mul(&self, val: T) -> T {
        match self {
            AtomicDistIterElement::Native(e) => e.fetch_mul(val),
            AtomicDistIterElement::Generic(e) => e.fetch_mul(val),
            AtomicDistIterElement::Network(e) => e.fetch_mul(val),
        }
    }
    /// Atomically divides by `val` and returns the previous value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// let _ = array.dist_iter_mut().for_each(|elem| {
    ///     let old = elem.fetch_div(2);
    /// }).block();
    ///```
    pub fn fetch_div(&self, val: T) -> T {
        match self {
            AtomicDistIterElement::Native(e) => e.fetch_div(val),
            AtomicDistIterElement::Generic(e) => e.fetch_div(val),
            AtomicDistIterElement::Network(e) => e.fetch_div(val),
        }
    }
    /// Atomically computes remainder by `val` and returns the previous value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// let _ = array.dist_iter_mut().for_each(|elem| {
    ///     let old = elem.fetch_rem(3);
    /// }).block();
    ///```
    pub fn fetch_rem(&self, val: T) -> T {
        match self {
            AtomicDistIterElement::Native(e) => e.fetch_rem(val),
            AtomicDistIterElement::Generic(e) => e.fetch_rem(val),
            AtomicDistIterElement::Network(e) => e.fetch_rem(val),
        }
    }
}

impl<'a, T: Dist + ElementBitWiseOps + 'static> AtomicDistIterElement<'a, T> {
    /// Atomically bitwise-ANDs with `val` and returns the previous value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// let _ = array.dist_iter_mut().for_each(|elem| {
    ///     let old = elem.fetch_and(0xFF);
    /// }).block();
    ///```
    pub fn fetch_and(&self, val: T) -> T {
        match self {
            AtomicDistIterElement::Native(e) => e.fetch_and(val),
            AtomicDistIterElement::Generic(e) => e.fetch_and(val),
            AtomicDistIterElement::Network(e) => e.fetch_and(val),
        }
    }
    /// Atomically bitwise-ORs with `val` and returns the previous value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// let _ = array.dist_iter_mut().for_each(|elem| {
    ///     let old = elem.fetch_or(0x01);
    /// }).block();
    ///```
    pub fn fetch_or(&self, val: T) -> T {
        match self {
            AtomicDistIterElement::Native(e) => e.fetch_or(val),
            AtomicDistIterElement::Generic(e) => e.fetch_or(val),
            AtomicDistIterElement::Network(e) => e.fetch_or(val),
        }
    }
    /// Atomically bitwise-XORs with `val` and returns the previous value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// let _ = array.dist_iter_mut().for_each(|elem| {
    ///     let old = elem.fetch_xor(0x01);
    /// }).block();
    ///```
    pub fn fetch_xor(&self, val: T) -> T {
        match self {
            AtomicDistIterElement::Native(e) => e.fetch_xor(val),
            AtomicDistIterElement::Generic(e) => e.fetch_xor(val),
            AtomicDistIterElement::Network(e) => e.fetch_xor(val),
        }
    }
}

impl<'a, T: Dist + ElementShiftOps + 'static> AtomicDistIterElement<'a, T> {
    /// Atomically left-shifts by `val` and returns the previous value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// let _ = array.dist_iter_mut().for_each(|elem| {
    ///     let old = elem.fetch_shl(1);
    /// }).block();
    ///```
    pub fn fetch_shl(&self, val: T) -> T {
        match self {
            AtomicDistIterElement::Native(e) => e.fetch_shl(val),
            AtomicDistIterElement::Generic(e) => e.fetch_shl(val),
            AtomicDistIterElement::Network(e) => e.fetch_shl(val),
        }
    }
    /// Atomically right-shifts by `val` and returns the previous value.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, 100, Distribution::Block).block();
    /// let _ = array.dist_iter_mut().for_each(|elem| {
    ///     let old = elem.fetch_shr(1);
    /// }).block();
    ///```
    pub fn fetch_shr(&self, val: T) -> T {
        match self {
            AtomicDistIterElement::Native(e) => e.fetch_shr(val),
            AtomicDistIterElement::Generic(e) => e.fetch_shr(val),
            AtomicDistIterElement::Network(e) => e.fetch_shr(val),
        }
    }
}

impl<'a, T: Dist + ElementArithmeticOps> AddAssign<T> for AtomicDistIterElement<'a, T> {
    fn add_assign(&mut self, val: T) {
        self.fetch_add(val);
    }
}
impl<'a, T: Dist + ElementArithmeticOps> SubAssign<T> for AtomicDistIterElement<'a, T> {
    fn sub_assign(&mut self, val: T) {
        self.fetch_sub(val);
    }
}
impl<'a, T: Dist + ElementArithmeticOps> MulAssign<T> for AtomicDistIterElement<'a, T> {
    fn mul_assign(&mut self, val: T) {
        self.fetch_mul(val);
    }
}
impl<'a, T: Dist + ElementArithmeticOps> DivAssign<T> for AtomicDistIterElement<'a, T> {
    fn div_assign(&mut self, val: T) {
        self.fetch_div(val);
    }
}
impl<'a, T: Dist + ElementArithmeticOps> RemAssign<T> for AtomicDistIterElement<'a, T> {
    fn rem_assign(&mut self, val: T) {
        self.fetch_rem(val);
    }
}
impl<'a, T: Dist + ElementBitWiseOps> BitAndAssign<T> for AtomicDistIterElement<'a, T> {
    fn bitand_assign(&mut self, val: T) {
        self.fetch_and(val);
    }
}
impl<'a, T: Dist + ElementBitWiseOps> BitOrAssign<T> for AtomicDistIterElement<'a, T> {
    fn bitor_assign(&mut self, val: T) {
        self.fetch_or(val);
    }
}
impl<'a, T: Dist + ElementBitWiseOps> BitXorAssign<T> for AtomicDistIterElement<'a, T> {
    fn bitxor_assign(&mut self, val: T) {
        self.fetch_xor(val);
    }
}
impl<'a, T: Dist + ElementShiftOps> ShlAssign<T> for AtomicDistIterElement<'a, T> {
    fn shl_assign(&mut self, val: T) {
        self.fetch_shl(val);
    }
}
impl<'a, T: Dist + ElementShiftOps> ShrAssign<T> for AtomicDistIterElement<'a, T> {
    fn shr_assign(&mut self, val: T) {
        self.fetch_shr(val);
    }
}

impl<'a, T: Dist + std::fmt::Debug> std::fmt::Debug for AtomicDistIterElement<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AtomicDistIterElement::Native(e) => write!(f, "{e:?}"),
            AtomicDistIterElement::Generic(e) => write!(f, "{e:?}"),
            AtomicDistIterElement::Network(e) => write!(f, "{e:?}"),
        }
    }
}

#[derive(Clone)]
pub struct AtomicDistIter<T: Dist> {
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
        AtomicDistIter {
            data,
            cur_i,
            end_i: cur_i + cnt,
        }
    }
}

#[derive(Clone)]
pub struct AtomicLocalIter<T: Dist> {
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
        AtomicLocalIter {
            data,
            cur_i,
            end_i: cur_i + cnt,
        }
    }
}

fn next_atomic_element<T: Dist>(
    data: &AtomicArray<T>,
    local_i: usize,
) -> AtomicDistIterElement<'static, T> {
    match data {
        AtomicArray::NativeAtomicArray(arr) => {
            let value = unsafe { arr.local_as_ptr().add(local_i) };
            AtomicDistIterElement::Native(NativeAtomicDistIterElement {
                value,
                orig_t: arr.orig_t,
                _marker: std::marker::PhantomData,
            })
        }
        AtomicArray::GenericAtomicArray(arr) => {
            AtomicDistIterElement::Generic(unsafe { generic_element_at(arr, local_i) })
        }
        AtomicArray::NetworkAtomicArray(arr) => {
            let value = unsafe { arr.local_as_ptr().add(local_i) };
            AtomicDistIterElement::Network(NetworkAtomicDistIterElement {
                value,
                orig_t: arr.orig_t,
                _marker: std::marker::PhantomData,
            })
        }
    }
}

impl<T: Dist + 'static> DistributedIterator for AtomicDistIter<T> {
    type Item = AtomicDistIterElement<'static, T>;
    type Array = AtomicArray<T>;
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Self {
        let max_i = self.data.num_elems_local();
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
        if self.cur_i < self.end_i {
            let local_i = self.cur_i;
            self.cur_i += 1;
            Some(next_atomic_element(&self.data, local_i))
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

impl<T: Dist + 'static> IndexedDistributedIterator for AtomicDistIter<T> {
    fn iterator_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.subarray_index_from_local(index, 1);
        g_index
    }
}

impl<T: Dist + 'static> LocalIterator for AtomicLocalIter<T> {
    type Item = AtomicDistIterElement<'static, T>;
    type Array = AtomicArray<T>;
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Self {
        let max_i = self.data.num_elems_local();
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
        if self.cur_i < self.end_i {
            let local_i = self.cur_i;
            self.cur_i += 1;
            Some(next_atomic_element(&self.data, local_i))
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
            Some(index)
        } else {
            None
        }
    }
}

impl<T: Dist> LamellarArrayIterators<T> for AtomicArray<T> {
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

pub(crate) mod operations;
pub(crate) mod iteration;
mod rdma;
use crate::array::atomic::AtomicElement;
use crate::array::generic_atomic::operations::BUFOPS;
use crate::array::private::LamellarArrayPrivate;
use crate::array::r#unsafe::{UnsafeByteArray, UnsafeByteArrayWeak};
use crate::array::*;
use crate::darc::Darc;
use crate::darc::DarcMode;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::Dist;
use parking_lot::{Mutex, MutexGuard};
use serde::ser::SerializeSeq;
use std::any::TypeId;
// use std::ops::{Deref, DerefMut};

use std::ops::{AddAssign, BitAndAssign, BitOrAssign, DivAssign, MulAssign, SubAssign};

#[doc(hidden)]
pub struct GenericAtomicElement<T> {
    array: GenericAtomicArray<T>,
    local_index: usize,
}


impl<T: Dist> From<GenericAtomicElement<T>> for AtomicElement<T> {
    fn from(element: GenericAtomicElement<T>) -> AtomicElement<T> {
        AtomicElement::GenericAtomicElement(element)
    }
}

impl<T: Dist> GenericAtomicElement<T> {
    pub fn load(&self) -> T {
        let _lock = self.array.lock_index(self.local_index);
        unsafe { self.array.__local_as_mut_slice()[self.local_index] }
    }
    pub fn store(&self, val: T) {
        let _lock = self.array.lock_index(self.local_index);
        unsafe {
            self.array.__local_as_mut_slice()[self.local_index] = val;
        }
    }
    pub fn swap(&self, val: T) -> T {
        let _lock = self.array.lock_index(self.local_index);
        unsafe {
            let old = self.array.__local_as_mut_slice()[self.local_index];
            self.array.__local_as_mut_slice()[self.local_index] = val;
            old
        }
    }
}
impl<T: ElementArithmeticOps> GenericAtomicElement<T> {
    pub fn fetch_add(&self, val: T) -> T {
        let _lock = self.array.lock_index(self.local_index);
        unsafe {
            let old = self.array.__local_as_mut_slice()[self.local_index];
            self.array.__local_as_mut_slice()[self.local_index] += val;
            old
        }
    }
    pub fn fetch_sub(&self, val: T) -> T {
        let _lock = self.array.lock_index(self.local_index);
        unsafe {
            let old = self.array.__local_as_mut_slice()[self.local_index];
            self.array.__local_as_mut_slice()[self.local_index] -= val;
            old
        }
    }
    pub fn fetch_mul(&self, val: T) -> T {
        let _lock = self.array.lock_index(self.local_index);
        unsafe {
            let old = self.array.__local_as_mut_slice()[self.local_index];
            self.array.__local_as_mut_slice()[self.local_index] *= val;
            old
        }
    }
    pub fn fetch_div(&self, val: T) -> T {
        let _lock = self.array.lock_index(self.local_index);
        unsafe {
            let old = self.array.__local_as_mut_slice()[self.local_index];
            self.array.__local_as_mut_slice()[self.local_index] /= val;
            old
        }
    }
}

impl<T: Dist + std::cmp::Eq> GenericAtomicElement<T> {
    pub fn compare_exchange(&self, current: T, new: T) -> Result<T, T> {
        let _lock = self.array.lock_index(self.local_index);
        let current_val = unsafe { self.array.__local_as_mut_slice()[self.local_index] };
        if current_val == current {
            unsafe {
                self.array.__local_as_mut_slice()[self.local_index] = new;
            }
            Ok(current_val)
        } else {
            Err(current_val)
        }
    }
}
impl<T: Dist + std::cmp::PartialEq + std::cmp::PartialOrd + std::ops::Sub<Output = T>>
    GenericAtomicElement<T>
{
    pub fn compare_exchange_epsilon(&self, current: T, new: T, eps: T) -> Result<T, T> {
        let _lock = self.array.lock_index(self.local_index);
        let current_val = unsafe { self.array.__local_as_mut_slice()[self.local_index] };
        let same = if current_val > current {
            current_val - current < eps
        } else {
            current - current_val < eps
        };
        if same {
            unsafe {
                self.array.__local_as_mut_slice()[self.local_index] = new;
            }
            Ok(current_val)
        } else {
            Err(current_val)
        }
    }
}

impl<T: ElementBitWiseOps + 'static> GenericAtomicElement<T> {
    pub fn fetch_and(&self, val: T) -> T {
        let _lock = self.array.lock_index(self.local_index);
        unsafe {
            let old = self.array.__local_as_mut_slice()[self.local_index];
            self.array.__local_as_mut_slice()[self.local_index] &= val;
            old
        }
    }
    pub fn fetch_or(&self, val: T) -> T {
        let _lock = self.array.lock_index(self.local_index);
        unsafe {
            let old = self.array.__local_as_mut_slice()[self.local_index];
            self.array.__local_as_mut_slice()[self.local_index] |= val;
            old
        }
    }
}

impl<T: Dist + ElementArithmeticOps> AddAssign<T> for GenericAtomicElement<T> {
    fn add_assign(&mut self, val: T) {
        // self.add(val)
        let _lock = self.array.lock_index(self.local_index);
        unsafe { self.array.__local_as_mut_slice()[self.local_index] += val }
    }
}

impl<T: Dist + ElementArithmeticOps> SubAssign<T> for GenericAtomicElement<T> {
    fn sub_assign(&mut self, val: T) {
        let _lock = self.array.lock_index(self.local_index);
        unsafe { self.array.__local_as_mut_slice()[self.local_index] -= val }
    }
}

impl<T: Dist + ElementArithmeticOps> MulAssign<T> for GenericAtomicElement<T> {
    fn mul_assign(&mut self, val: T) {
        let _lock = self.array.lock_index(self.local_index);
        unsafe { self.array.__local_as_mut_slice()[self.local_index] *= val }
    }
}

impl<T: Dist + ElementArithmeticOps> DivAssign<T> for GenericAtomicElement<T> {
    fn div_assign(&mut self, val: T) {
        let _lock = self.array.lock_index(self.local_index);
        unsafe { self.array.__local_as_mut_slice()[self.local_index] /= val }
    }
}

impl<T: Dist + ElementBitWiseOps> BitAndAssign<T> for GenericAtomicElement<T> {
    fn bitand_assign(&mut self, val: T) {
        let _lock = self.array.lock_index(self.local_index);
        unsafe { self.array.__local_as_mut_slice()[self.local_index] &= val }
    }
}

impl<T: Dist + ElementBitWiseOps> BitOrAssign<T> for GenericAtomicElement<T> {
    fn bitor_assign(&mut self, val: T) {
        let _lock = self.array.lock_index(self.local_index);
        unsafe { self.array.__local_as_mut_slice()[self.local_index] |= val }
    }
}

impl<T: Dist + std::fmt::Debug> std::fmt::Debug for GenericAtomicElement<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _lock = self.array.lock_index(self.local_index);
        let current_val = unsafe { self.array.__local_as_mut_slice()[self.local_index] };
        write!(f,"{current_val:?}")
    }
}

/// A variant of an [AtomicArray] providing atomic access for any type that implements [Dist][crate::memregion::Dist].
///
/// Atomicity is gauranteed by constructing a 1-Byte mutex for each element in the array.
///
/// Generally any operation on this array type will be performed via an internal runtime Active Message, i.e. direct RDMA operations are not allowed
#[lamellar_impl::AmDataRT(Clone, Debug)]
pub struct GenericAtomicArray<T> {
    locks: Darc<Vec<Mutex<()>>>,
    pub(crate) array: UnsafeArray<T>,
}

#[doc(hidden)]
#[lamellar_impl::AmDataRT(Clone, Debug)]
pub struct GenericAtomicByteArray {
    locks: Darc<Vec<Mutex<()>>>,
    pub(crate) array: UnsafeByteArray,
}

impl GenericAtomicByteArray {
    #[doc(hidden)]
    pub fn lock_index(&self, index: usize) -> MutexGuard<()> {
        let index = self
            .array
            .inner
            .pe_full_offset_for_local_index(self.array.inner.data.my_pe, index)
            .expect("invalid local index");
        self.locks[index].lock()
    }

    #[doc(hidden)]
    pub fn downgrade(array: &GenericAtomicByteArray) -> GenericAtomicByteArrayWeak {
        GenericAtomicByteArrayWeak {
            locks: array.locks.clone(),
            array: UnsafeByteArray::downgrade(&array.array),
        }
    }
}

#[doc(hidden)]
#[lamellar_impl::AmLocalDataRT(Clone, Debug)]
pub struct GenericAtomicByteArrayWeak {
    locks: Darc<Vec<Mutex<()>>>,
    pub(crate) array: UnsafeByteArrayWeak,
}

impl GenericAtomicByteArrayWeak {
    #[doc(hidden)]
    pub fn upgrade(&self) -> Option<GenericAtomicByteArray> {
        Some(GenericAtomicByteArray {
            locks: self.locks.clone(),
            array: self.array.upgrade()?,
        })
    }
}

#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct GenericAtomicLocalData<T: Dist> {
    pub(crate) array: GenericAtomicArray<T>,
    start_index: usize,
    end_index: usize,
}

#[doc(hidden)]
#[derive(Debug)]
pub struct GenericAtomicLocalDataIter<T: Dist> {
    array: GenericAtomicArray<T>,
    index: usize,
    end_index: usize,
}

impl<T: Dist> GenericAtomicLocalData<T> {
    pub fn at(&self, index: usize) -> GenericAtomicElement<T> {
        GenericAtomicElement {
            array: self.array.clone(),
            local_index: index,
        }
    }

    pub fn get_mut(&self, index: usize) -> Option<GenericAtomicElement<T>> {
        Some(GenericAtomicElement {
            array: self.array.clone(),
            local_index: index,
        })
    }

    pub fn len(&self) -> usize {
        unsafe { self.array.__local_as_mut_slice().len() }
    }

    pub fn iter(&self) -> GenericAtomicLocalDataIter<T> {
        GenericAtomicLocalDataIter {
            array: self.array.clone(),
            index: self.start_index,
            end_index: self.end_index,
        }
    }

    pub fn sub_data(&self, start_index: usize, end_index: usize) -> GenericAtomicLocalData<T> {
        GenericAtomicLocalData {
            array: self.array.clone(),
            start_index: start_index,
            end_index: std::cmp::min(end_index, self.array.num_elems_local()),
        }
    }
}

impl<T: Dist + serde::Serialize> serde::Serialize for GenericAtomicLocalData<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_seq(Some(self.len()))?;
        for i in 0..self.len() {
            s.serialize_element(&self.at(i).load())?;
        }
        s.end()
    }
}

impl<T: Dist> IntoIterator for GenericAtomicLocalData<T> {
    type Item = GenericAtomicElement<T>;
    type IntoIter = GenericAtomicLocalDataIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        GenericAtomicLocalDataIter {
            array: self.array,
            index: self.start_index,
            end_index: self.end_index,
        }
    }
}

impl<T: Dist> Iterator for GenericAtomicLocalDataIter<T> {
    type Item = GenericAtomicElement<T>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.end_index {
            let index = self.index;
            self.index += 1;
            Some(GenericAtomicElement {
                array: self.array.clone(),
                local_index: index,
            })
        } else {
            None
        }
    }
}

impl<T: Dist + std::default::Default> GenericAtomicArray<T> {
    pub(crate) fn new<U: Clone + Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> GenericAtomicArray<T> {
        // println!("new generic_atomic array");
        let array = UnsafeArray::new(team.clone(), array_size, distribution);
        let mut vec = vec![];
        for _i in 0..array.num_elems_local() {
            vec.push(Mutex::new(()));
        }
        let locks = Darc::new(team, vec).unwrap();

        if let Some(func) = BUFOPS.get(&TypeId::of::<T>()) {
            let mut op_bufs = array.inner.data.op_buffers.write();
            let bytearray = GenericAtomicByteArray {
                locks: locks.clone(),
                array: array.clone().into(),
            };

            for pe in 0..op_bufs.len() {
                op_bufs[pe] = func(GenericAtomicByteArray::downgrade(&bytearray));
            }
            // println!("{}", op_bufs.len());
        }

        GenericAtomicArray {
            locks: locks,
            array: array,
        }
    }
}

impl<T: Dist> GenericAtomicArray<T> {
    pub(crate) fn get_element(&self, index: usize) -> Option<GenericAtomicElement<T>> {
        if index > unsafe{self.__local_as_slice().len()}{//We are only directly accessing the local slice for its len
            Some(
                GenericAtomicElement {
                    array: self.clone(),
                    local_index: index,
                }
            )
        }
        else {
            None
        }
    }
}

impl<T: Dist> GenericAtomicArray<T> {
    // pub fn wait_all(&self) {
    //     self.array.wait_all();
    // }
    // pub fn barrier(&self) {
    //     self.array.barrier();
    // }

    // pub fn block_on<F>(&self, f: F) -> F::Output
    // where
    //     F: Future,
    // {
    //     self.array.block_on(f)
    // }

    // pub(crate) fn num_elems_local(&self) -> usize {
    //     self.array.num_elems_local()
    // }

    pub fn use_distribution(self, distribution: Distribution) -> Self {
        GenericAtomicArray {
            locks: self.locks.clone(),
            array: self.array.use_distribution(distribution),
        }
    }

    // pub fn num_pes(&self) -> usize {
    //     self.array.num_pes()
    // }

    // #[doc(hidden)]
    // pub fn pe_for_dist_index(&self, index: usize) -> Option<usize> {
    //     self.array.pe_for_dist_index(index)
    // }

    // #[doc(hidden)]
    // pub fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> Option<usize> {
    //     self.array.pe_offset_for_dist_index(pe, index)
    // }

    // // pub(crate) fn subarray_index_from_local(&self, index: usize) -> Option<usize> {
    // //     self.array.inner.subarray_index_from_local(index)
    // // }

    // pub fn len(&self) -> usize {
    //     self.array.len()
    // }

    pub fn local_data(&self) -> GenericAtomicLocalData<T> {
        GenericAtomicLocalData {
            array: self.clone(),
            start_index: 0,
            end_index: self.array.num_elems_local(),
        }
    }

    pub fn mut_local_data(&self) -> GenericAtomicLocalData<T> {
        GenericAtomicLocalData {
            array: self.clone(),
            start_index: 0,
            end_index: self.array.num_elems_local(),
        }
    }

    #[doc(hidden)]
    pub unsafe fn __local_as_slice(&self) -> &[T] {
        self.array.local_as_mut_slice()
    }
    #[doc(hidden)]
    pub unsafe fn __local_as_mut_slice(&self) -> &mut [T] {
        self.array.local_as_mut_slice()
    }

    // pub fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self {
    //     GenericAtomicArray {
    //         locks: self.locks.clone(),
    //         array: self.array.sub_array(range),
    //     }
    // }

    pub fn into_unsafe(self) -> UnsafeArray<T> {
        // println!("generic into_unsafe");
        self.array.into()
    }

    // pub fn into_local_only(self) -> LocalOnlyArray<T> {
    //     // println!("generic into_local_only");
    //     self.array.into()
    // }

    pub fn into_read_only(self) -> ReadOnlyArray<T> {
        // println!("generic into_read_only");
        self.array.into()
    }

    pub fn into_local_lock(self) -> LocalLockArray<T> {
        // println!("generic into_local_lock");
        self.array.into()
    }

    #[doc(hidden)]
    pub fn lock_index(&self, index: usize) -> MutexGuard<()> {
        // if let Some(ref locks) = *self.locks {
        //     let start_index = (index * std::mem::size_of::<T>()) / self.orig_t_size;
        //     let end_index = ((index + 1) * std::mem::size_of::<T>()) / self.orig_t_size;
        //     let mut guards = vec![];
        //     for i in start_index..end_index {
        //         guards.push(locks[i].lock())
        //     }
        //     Some(guards)
        // } else {
        //     None
        // }
        // println!("trying to lock {:?}",index);
        let index = self
            .array
            .inner
            .pe_full_offset_for_local_index(self.array.inner.data.my_pe, index)
            .expect("invalid local index");
        self.locks[index].lock()
    }
}

impl<T: Dist + 'static> GenericAtomicArray<T> {
    pub fn into_atomic(self) -> GenericAtomicArray<T> {
        // println!("generic into_atomic");
        self.array.into()
    }
}

impl<T: Dist> From<UnsafeArray<T>> for GenericAtomicArray<T> {
    fn from(array: UnsafeArray<T>) -> Self {
        // println!("generic from unsafe array");
        array.block_on_outstanding(DarcMode::GenericAtomicArray);
        let mut vec = vec![];
        for _i in 0..array.num_elems_local() {
            vec.push(Mutex::new(()));
        }
        let locks = Darc::new(array.team(), vec).unwrap();
        if let Some(func) = BUFOPS.get(&TypeId::of::<T>()) {
            let bytearray = GenericAtomicByteArray {
                locks: locks.clone(),
                array: array.clone().into(),
            };
            let mut op_bufs = array.inner.data.op_buffers.write();
            for _pe in 0..array.inner.data.num_pes {
                op_bufs.push(func(GenericAtomicByteArray::downgrade(&bytearray)))
            }
        }
        GenericAtomicArray {
            locks: locks,
            array: array,
        }
    }
}

impl<T: Dist> From<GenericAtomicArray<T>> for GenericAtomicByteArray {
    fn from(array: GenericAtomicArray<T>) -> Self {
        GenericAtomicByteArray {
            locks: array.locks.clone(),
            array: array.array.into(),
        }
    }
}

impl<T: Dist> From<GenericAtomicArray<T>> for LamellarByteArray {
    fn from(array: GenericAtomicArray<T>) -> Self {
        LamellarByteArray::GenericAtomicArray(GenericAtomicByteArray {
            locks: array.locks.clone(),
            array: array.array.into(),
        })
    }
}

impl<T: Dist> From<GenericAtomicArray<T>> for AtomicByteArray {
    fn from(array: GenericAtomicArray<T>) -> Self {
        AtomicByteArray::GenericAtomicByteArray(GenericAtomicByteArray {
            locks: array.locks.clone(),
            array: array.array.into(),
        })
    }
}
impl<T: Dist> From<GenericAtomicByteArray> for GenericAtomicArray<T> {
    fn from(array: GenericAtomicByteArray) -> Self {
        GenericAtomicArray {
            locks: array.locks.clone(),
            array: array.array.into(),
        }
    }
}
impl<T: Dist> From<GenericAtomicByteArray> for AtomicArray<T> {
    fn from(array: GenericAtomicByteArray) -> Self {
        GenericAtomicArray {
            locks: array.locks.clone(),
            array: array.array.into(),
        }
        .into()
    }
}

impl<T: Dist> private::ArrayExecAm<T> for GenericAtomicArray<T> {
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team_rt().clone()
    }
    fn team_counters(&self) -> Arc<AMCounters> {
        self.array.team_counters()
    }
}

impl<T: Dist> private::LamellarArrayPrivate<T> for GenericAtomicArray<T> {
    fn inner_array(&self) -> &UnsafeArray<T> {
        &self.array
    }
    fn local_as_ptr(&self) -> *const T {
        self.array.local_as_mut_ptr()
    }
    fn local_as_mut_ptr(&self) -> *mut T {
        self.array.local_as_mut_ptr()
    }
    fn pe_for_dist_index(&self, index: usize) -> Option<usize> {
        self.array.pe_for_dist_index(index)
    }
    fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> Option<usize> {
        self.array.pe_offset_for_dist_index(pe, index)
    }
    unsafe fn into_inner(self) -> UnsafeArray<T> {
        self.array
    }
}

impl<T: Dist> LamellarArray<T> for GenericAtomicArray<T> {
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team_rt().clone()
    }
    fn my_pe(&self) -> usize {
        self.array.my_pe()
    }
    fn num_pes(&self) -> usize {
        self.array.num_pes()
    }
    fn len(&self) -> usize {
        self.array.len()
    }
    fn num_elems_local(&self) -> usize {
        self.array.num_elems_local()
    }
    fn barrier(&self) {
        self.array.barrier();
    }
    fn wait_all(&self) {
        self.array.wait_all()
        // println!("done in wait all {:?}",std::time::SystemTime::now());
    }
    fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future {
            self.array.block_on(f)
    }
    fn pe_and_offset_for_global_index(&self, index: usize) -> Option<(usize, usize)> {
        self.array.pe_and_offset_for_global_index(index)
    }
}

impl<T: Dist> LamellarWrite for GenericAtomicArray<T> {}
impl<T: Dist> LamellarRead for GenericAtomicArray<T> {}

impl<T: Dist> SubArray<T> for GenericAtomicArray<T> {
    type Array = GenericAtomicArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array {
        GenericAtomicArray {
            locks: self.locks.clone(),
            array: self.array.sub_array(range),
        }
    }
    fn global_index(&self, sub_index: usize) -> usize {
        self.array.global_index(sub_index)
    }
}

impl<T: Dist + std::fmt::Debug> GenericAtomicArray<T> {
    pub fn print(&self) {
        self.array.print();
    }
}

impl<T: Dist + std::fmt::Debug> ArrayPrint<T> for GenericAtomicArray<T> {
    fn print(&self) {
        self.array.print()
    }
}

impl<T: Dist + AmDist + 'static,> LamellarArrayReduce<T> for GenericAtomicArray<T> {
    fn reduce(&self, op: &str) -> Pin<Box<dyn Future<Output = T>>> {
        self.array.reduce_data(op,self.clone().into()).into_future()
    }
}
impl<T: Dist + AmDist + ElementArithmeticOps + 'static,> LamellarArrayArithmeticReduce<T> for GenericAtomicArray<T> {
    fn sum(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.reduce("sum")
    }
    fn prod(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.reduce("prod")
    }
}
impl<T: Dist + AmDist + ElementComparePartialEqOps + 'static,> LamellarArrayCompareReduce<T> for GenericAtomicArray<T> {
    fn max(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.reduce("max")
    }
    fn min(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.reduce("min")
    }
}

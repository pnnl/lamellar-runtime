mod iteration;
pub(crate) mod operations;
pub(crate) mod rdma;
pub use rdma::{AtomicArrayGet, AtomicArrayPut};

use crate::array::generic_atomic::GenericAtomicElement;
use crate::array::native_atomic::NativeAtomicElement;
use crate::array::private::LamellarArrayPrivate;
use crate::array::*;
// use crate::darc::{Darc, DarcMode};
use crate::lamellar_team::IntoLamellarTeam;
use crate::memregion::Dist;
use std::any::TypeId;
use std::collections::HashSet;
// use std::sync::atomic::Ordering;
// use std::sync::Arc;

lazy_static! {
    pub(crate) static ref NATIVE_ATOMICS: HashSet<TypeId> = {
        let mut map = HashSet::new();
        map.insert(TypeId::of::<u8>());
        map.insert(TypeId::of::<u16>());
        map.insert(TypeId::of::<u32>());
        map.insert(TypeId::of::<u64>());
        map.insert(TypeId::of::<usize>());
        map.insert(TypeId::of::<i8>());
        map.insert(TypeId::of::<i16>());
        map.insert(TypeId::of::<i32>());
        map.insert(TypeId::of::<i64>());
        map.insert(TypeId::of::<isize>());
        map
    };
}

use std::ops::{AddAssign, BitAndAssign, BitOrAssign, DivAssign, MulAssign, SubAssign};

// #[doc(hidden)]
/// An abstraction of an atomic element either via language supported Atomic integer types or through the use of an accompanying mutex.
///
/// This type is returned when iterating over an AtomicArray as well as when accessing local elements through an [AtomicLocalData] handle.
pub enum AtomicElement<T: Dist> {
    NativeAtomicElement(NativeAtomicElement<T>),
    GenericAtomicElement(GenericAtomicElement<T>),
}

impl<T: Dist> AtomicElement<T> {
    /// Atomically read the value of this element
    ///
    /// Note: for native atomic types, [SeqCst][std::sync::atomic::Ordering::SeqCst] ordering is used
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.local_data();
    /// println!("PE{my_pe} elem: {:?}",local_data.at(10).load());
    ///
    /// # let array2: AtomicArray<f32>  = AtomicArray::new(&world,100,Distribution::Block); // test genericatomic
    /// # let local_data = array2.local_data();
    /// # println!("PE{my_pe} elem: {:?}",local_data.at(10).load());
    ///```
    pub fn load(&self) -> T {
        match self {
            AtomicElement::NativeAtomicElement(array) => array.load(),
            AtomicElement::GenericAtomicElement(array) => array.load(),
        }
    }

    /// Atomically store `val` into this element
    ///
    /// Note: for native atomic types, [SeqCst][std::sync::atomic::Ordering::SeqCst] ordering is used
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.local_data();
    /// local_data.at(10).store(19);
    ///
    /// # let array2: AtomicArray<f32>  = AtomicArray::new(&world,100,Distribution::Block); // test genericatomic
    /// # let local_data = array2.local_data();
    /// # local_data.at(10).store(19.0);
    ///```
    pub fn store(&self, val: T) {
        match self {
            AtomicElement::NativeAtomicElement(array) => array.store(val),
            AtomicElement::GenericAtomicElement(array) => array.store(val),
        }
    }

    /// Atomically swap `val` with the current value of this element, returning the swaped value
    ///
    /// Note: for native atomic types, [SeqCst][std::sync::atomic::Ordering::SeqCst] ordering is used
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.local_data();
    /// let old_val = local_data.at(10).swap(19);
    ///
    /// # let array2: AtomicArray<f32>  = AtomicArray::new(&world,100,Distribution::Block); // test genericatomic
    /// # let local_data = array2.local_data();
    /// # let old_val = local_data.at(10).swap(19.0);
    ///```
    pub fn swap(&self, val: T) -> T {
        match self {
            AtomicElement::NativeAtomicElement(array) => array.swap(val),
            AtomicElement::GenericAtomicElement(array) => array.swap(val),
        }
    }
}

impl<T: ElementArithmeticOps> AtomicElement<T> {
    /// Atomically add `val` to the current value, returning the previous value
    ///
    /// Note: for native atomic types, [SeqCst][std::sync::atomic::Ordering::SeqCst] ordering is used
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.local_data();
    /// let old_val = local_data.at(10).fetch_add(19);
    ///
    /// # let array2: AtomicArray<f32>  = AtomicArray::new(&world,100,Distribution::Block); // test genericatomic
    /// # let local_data = array2.local_data();
    /// # let old_val = local_data.at(10).fetch_add(19.0);
    ///```
    pub fn fetch_add(&self, val: T) -> T {
        match self {
            AtomicElement::NativeAtomicElement(array) => array.fetch_add(val),
            AtomicElement::GenericAtomicElement(array) => array.fetch_add(val),
        }
    }
    /// Atomically subtracts `val` from the current value, returning the previous value
    ///
    /// Note: for native atomic types, [SeqCst][std::sync::atomic::Ordering::SeqCst] ordering is used
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.local_data();
    /// let old_val = local_data.at(10).fetch_sub(19);
    ///
    /// # let array2: AtomicArray<f32>  = AtomicArray::new(&world,100,Distribution::Block); // test genericatomic
    /// # let local_data = array2.local_data();
    /// # let old_val = local_data.at(10).fetch_sub(19.0);
    ///```
    pub fn fetch_sub(&self, val: T) -> T {
        match self {
            AtomicElement::NativeAtomicElement(array) => array.fetch_sub(val),
            AtomicElement::GenericAtomicElement(array) => array.fetch_sub(val),
        }
    }

    /// Atomically multiplies `val` with the current value, returning the previous value
    ///
    /// Note: for native atomic types, [SeqCst][std::sync::atomic::Ordering::SeqCst] ordering is used
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.local_data();
    /// let old_val = local_data.at(10).fetch_mul(19);
    ///
    /// # let array2: AtomicArray<f32>  = AtomicArray::new(&world,100,Distribution::Block); // test genericatomic
    /// # let local_data = array2.local_data();
    /// # let old_val = local_data.at(10).fetch_mul(19.0);
    ///```
    pub fn fetch_mul(&self, val: T) -> T {
        match self {
            AtomicElement::NativeAtomicElement(array) => array.fetch_mul(val),
            AtomicElement::GenericAtomicElement(array) => array.fetch_mul(val),
        }
    }

    /// Atomically divides the current value by `val`, returning the previous value
    ///
    /// Note: for native atomic types, [SeqCst][std::sync::atomic::Ordering::SeqCst] ordering is used
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.local_data();
    /// let old_val = local_data.at(10).fetch_div(19);
    ///
    /// # let array2: AtomicArray<f32>  = AtomicArray::new(&world,100,Distribution::Block); // test genericatomic
    /// # let local_data = array2.local_data();
    /// # let old_val = local_data.at(10).fetch_div(19.0);
    ///```
    pub fn fetch_div(&self, val: T) -> T {
        match self {
            AtomicElement::NativeAtomicElement(array) => array.fetch_div(val),
            AtomicElement::GenericAtomicElement(array) => array.fetch_div(val),
        }
    }
}

impl<T: Dist + std::cmp::Eq> AtomicElement<T> {
    /// Stores the `new` value into this element if the current value is the same as `current`.
    ///
    /// the return value is a result indicating whether the new value was written into the element and contains the previous value.
    /// On success this previous value is gauranteed to be equal to `current`
    ///
    /// Note: for native atomic types, [SeqCst][std::sync::atomic::Ordering::SeqCst] ordering is used
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.local_data();
    /// let result = local_data.at(10).compare_exchange(19,10);
    ///```
    pub fn compare_exchange(&self, current: T, new: T) -> Result<T, T> {
        match self {
            AtomicElement::NativeAtomicElement(array) => array.compare_exchange(current, new),
            AtomicElement::GenericAtomicElement(array) => array.compare_exchange(current, new),
        }
    }
}

impl<T: Dist + std::cmp::PartialEq + std::cmp::PartialOrd + std::ops::Sub<Output = T>>
    AtomicElement<T>
{
    /// Stores the `new` value into this element if the current value is the same as `current` plus or minus `epslion`.
    ///
    /// e.g. ``` if current - epsilon < array[index] && array[index] < current + epsilon { array[index] = new }```
    ///
    /// the return value is a result indicating whether the new value was written into the element and contains the previous value.
    /// On success this previous value is gauranteed to be within epsilon of `current`
    ///
    /// Note: for native atomic types, [SeqCst][std::sync::atomic::Ordering::SeqCst] ordering is used
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.local_data();
    /// let result = local_data.at(10).compare_exchange_epsilon(19,10,1);
    ///
    /// # let array2: AtomicArray<f32>  = AtomicArray::new(&world,100,Distribution::Block); // test genericatomic
    /// # let local_data = array2.local_data();
    /// # let result = local_data.at(10).compare_exchange_epsilon(19.0,10.0,0.1);
    ///```
    pub fn compare_exchange_epsilon(&self, current: T, new: T, eps: T) -> Result<T, T> {
        match self {
            AtomicElement::NativeAtomicElement(array) => {
                array.compare_exchange_epsilon(current, new, eps)
            }
            AtomicElement::GenericAtomicElement(array) => {
                array.compare_exchange_epsilon(current, new, eps)
            }
        }
    }
}

impl<T: ElementBitWiseOps + 'static> AtomicElement<T> {
    /// Atomically performs a bitwise and of `val` and the current value, returning the previous value
    ///
    /// Note: for native atomic types, [SeqCst][std::sync::atomic::Ordering::SeqCst] ordering is used
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.local_data();
    /// let old_val = local_data.at(10).fetch_and(0b0011);
    ///```
    pub fn fetch_and(&self, val: T) -> T {
        match self {
            AtomicElement::NativeAtomicElement(array) => array.fetch_and(val),
            AtomicElement::GenericAtomicElement(array) => array.fetch_and(val),
        }
    }
    /// Atomically performs a bitwise and of `val` and the current value, returning the previous value
    ///
    /// Note: for native atomic types, [SeqCst][std::sync::atomic::Ordering::SeqCst] ordering is used
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.local_data();
    /// let old_val = local_data.at(10).fetch_or(0b0011);
    ///```
    pub fn fetch_or(&self, val: T) -> T {
        match self {
            AtomicElement::NativeAtomicElement(array) => array.fetch_or(val),
            AtomicElement::GenericAtomicElement(array) => array.fetch_or(val),
        }
    }
}

impl<T: Dist + ElementArithmeticOps> AddAssign<T> for AtomicElement<T> {
    fn add_assign(&mut self, val: T) {
        match self {
            AtomicElement::NativeAtomicElement(array) => array.add_assign(val),
            AtomicElement::GenericAtomicElement(array) => array.add_assign(val),
        }
    }
}

impl<T: Dist + ElementArithmeticOps> SubAssign<T> for AtomicElement<T> {
    fn sub_assign(&mut self, val: T) {
        match self {
            AtomicElement::NativeAtomicElement(array) => array.sub_assign(val),
            AtomicElement::GenericAtomicElement(array) => array.sub_assign(val),
        }
    }
}

impl<T: Dist + ElementArithmeticOps> MulAssign<T> for AtomicElement<T> {
    fn mul_assign(&mut self, val: T) {
        match self {
            AtomicElement::NativeAtomicElement(array) => array.mul_assign(val),
            AtomicElement::GenericAtomicElement(array) => array.mul_assign(val),
        }
    }
}

impl<T: Dist + ElementArithmeticOps> DivAssign<T> for AtomicElement<T> {
    fn div_assign(&mut self, val: T) {
        match self {
            AtomicElement::NativeAtomicElement(array) => array.div_assign(val),
            AtomicElement::GenericAtomicElement(array) => array.div_assign(val),
        }
    }
}

impl<T: Dist + ElementBitWiseOps> BitAndAssign<T> for AtomicElement<T> {
    fn bitand_assign(&mut self, val: T) {
        match self {
            AtomicElement::NativeAtomicElement(array) => array.bitand_assign(val),
            AtomicElement::GenericAtomicElement(array) => array.bitand_assign(val),
        }
    }
}

impl<T: Dist + ElementBitWiseOps> BitOrAssign<T> for AtomicElement<T> {
    fn bitor_assign(&mut self, val: T) {
        match self {
            AtomicElement::NativeAtomicElement(array) => array.bitor_assign(val),
            AtomicElement::GenericAtomicElement(array) => array.bitor_assign(val),
        }
    }
}

impl<T: Dist + std::fmt::Debug> std::fmt::Debug for AtomicElement<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AtomicElement::NativeAtomicElement(array) => array.fmt(f),
            AtomicElement::GenericAtomicElement(array) => array.fmt(f),
        }
    }
}

///A safe abstraction of a distributed array, providing read/write access protect by atomic elements
///
/// If the type of the Array is an integer type (U8, usize, i32, i16, etc.) the array will use the appropriate Atomic* type underneath.
///
/// If it is any other type `T: Dist` then the array will construct a mutex for each element in the array to manage access.
///
/// All access to the individual elements in this array type are protected either via a language/compiler supported atomic type or by a mutex,
/// as such there can be many concurrent threads modifying the array at any given time.
///
/// Generally any operation on this array type will be performed via an internal runtime Active Message, i.e. direct RDMA operations are not allowed
#[enum_dispatch(LamellarArray<T>,LamellarArrayInternalGet<T>,LamellarArrayInternalPut<T>,ArrayExecAm<T>,LamellarArrayPrivate<T>,DistIteratorLauncher,LocalIteratorLauncher)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[serde(bound = "T: Dist + serde::Serialize + serde::de::DeserializeOwned + 'static")]
pub enum AtomicArray<T: Dist> {
    NativeAtomicArray(NativeAtomicArray<T>),
    GenericAtomicArray(GenericAtomicArray<T>),
}

impl<T: Dist + 'static> crate::active_messaging::DarcSerde for AtomicArray<T> {
    fn ser(&self, num_pes: usize, darcs: &mut Vec<RemotePtr>){
        match self {
            AtomicArray::NativeAtomicArray(array) => array.ser(num_pes,darcs),
            AtomicArray::GenericAtomicArray(array) => array.ser(num_pes,darcs),
        }
    }
    fn des(&self, cur_pe: Result<usize, crate::IdError>) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.des(cur_pe),
            AtomicArray::GenericAtomicArray(array) => array.des(cur_pe),
        }
    }
}

impl<T: Dist> SubArray<T> for AtomicArray<T> {
    type Array = AtomicArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.sub_array(range).into(),
            AtomicArray::GenericAtomicArray(array) => array.sub_array(range).into(),
        }
    }
    fn global_index(&self, sub_index: usize) -> usize {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.global_index(sub_index).into(),
            AtomicArray::GenericAtomicArray(array) => array.global_index(sub_index).into(),
        }
    }
}

#[doc(hidden)]
#[enum_dispatch]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum AtomicByteArray {
    NativeAtomicByteArray(NativeAtomicByteArray),
    GenericAtomicByteArray(GenericAtomicByteArray),
}

impl AtomicByteArray {
    #[doc(hidden)]
    pub fn downgrade(array: &AtomicByteArray) -> AtomicByteArrayWeak {
        match array {
            AtomicByteArray::NativeAtomicByteArray(array) => {
                AtomicByteArrayWeak::NativeAtomicByteArrayWeak(NativeAtomicByteArray::downgrade(
                    array,
                ))
            }
            AtomicByteArray::GenericAtomicByteArray(array) => {
                AtomicByteArrayWeak::GenericAtomicByteArrayWeak(GenericAtomicByteArray::downgrade(
                    array,
                ))
            }
        }
    }
}

impl crate::active_messaging::DarcSerde for AtomicByteArray {
    fn ser(&self, num_pes: usize, darcs: &mut Vec<RemotePtr>) {
        match self {
            AtomicByteArray::NativeAtomicByteArray(array) => array.ser(num_pes,darcs),
            AtomicByteArray::GenericAtomicByteArray(array) => array.ser(num_pes,darcs),
        }
    }

    fn des(&self, cur_pe: Result<usize, crate::IdError>) {
        match self {
            AtomicByteArray::NativeAtomicByteArray(array) => array.des(cur_pe),
            AtomicByteArray::GenericAtomicByteArray(array) => array.des(cur_pe),
        }
    }
}

#[doc(hidden)]
#[enum_dispatch]
#[derive(Clone)]
pub enum AtomicByteArrayWeak {
    NativeAtomicByteArrayWeak(NativeAtomicByteArrayWeak),
    GenericAtomicByteArrayWeak(GenericAtomicByteArrayWeak),
}

impl AtomicByteArrayWeak {
    #[doc(hidden)]
    pub fn upgrade(&self) -> Option<AtomicByteArray> {
        match self {
            AtomicByteArrayWeak::NativeAtomicByteArrayWeak(array) => {
                Some(AtomicByteArray::NativeAtomicByteArray(array.upgrade()?))
            }
            AtomicByteArrayWeak::GenericAtomicByteArrayWeak(array) => {
                Some(AtomicByteArray::GenericAtomicByteArray(array.upgrade()?))
            }
        }
    }
}

/// Provides access to a PEs local data to provide "local" indexing while maintaining safety guarantees of the array type.
///
/// It may be useful (albeit incorrect) to think of this as a slice of the PEs local data.
pub struct AtomicLocalData<T: Dist> {
    pub(crate) array: AtomicArray<T>,
}

/// An iterator over the elements in an [AtomicLocalData]
pub struct AtomicLocalDataIter<T: Dist> {
    array: AtomicArray<T>,
    index: usize,
}

impl<T: Dist> AtomicLocalData<T> {
    /// Returns the element specified by `index`
    ///
    /// Indexing is local to each PE.
    ///
    /// # Panics
    /// Panics if `index` is out of bounds
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.local_data();
    ///
    /// let first_local_val = local_data.at(0);
    ///```
    pub fn at(&self, index: usize) -> AtomicElement<T> {
        let Some(val) = self.array.get_element(index) else {
            panic!("AtomicLocalData index {index} out of bounds");
        };
        val
    }

    /// Returns the element specified by `index`, returns `None` otherwise
    ///
    /// Indexing is local to each PE.
    ///
    /// # Examples
    /// Assume 4 PE system
    ///```no_run
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.local_data();
    ///
    /// let first_local_val = local_data.get_mut(0).unwrap(); //local data length is 25
    ///```
    pub fn get_mut(&self, index: usize) -> Option<AtomicElement<T>> {
        self.array.get_element(index)
    }

    /// Returns the number of local elements on the PE
    ///
    /// # Examples
    /// Assume 4 PE system
    ///```no_run
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.local_data();
    ///
    /// assert_eq!(25,local_data.len());
    ///```
    pub fn len(&self) -> usize {
        unsafe { self.array.__local_as_mut_slice().len() }
    }

    /// Returns an [Iterator] over the elements in the local data
    ///
    /// # Examples
    /// Assume 4 PE system
    ///```no_run
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.local_data();
    ///
    /// for elem in local_data.iter() {
    ///    println!("elem {:?}",elem.load());
    /// }
    ///```
    pub fn iter(&self) -> AtomicLocalDataIter<T> {
        AtomicLocalDataIter {
            array: self.array.clone(),
            index: 0,
        }
    }
}

impl<T: Dist> IntoIterator for AtomicLocalData<T> {
    type Item = AtomicElement<T>;
    type IntoIter = AtomicLocalDataIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        AtomicLocalDataIter {
            array: self.array,
            index: 0,
        }
    }
}

impl<T: Dist> Iterator for AtomicLocalDataIter<T> {
    type Item = AtomicElement<T>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.array.num_elems_local() {
            let index = self.index;
            self.index += 1;
            self.array.get_element(index)
        } else {
            None
        }
    }
}

//#[prof]
impl<T: Dist + std::default::Default + 'static> AtomicArray<T> {
    #[doc(alias = "Collective")]
    /// Construct a new AtomicArray with a length of `array_size` whose data will be layed out with the provided `distribution` on the PE's specified by the `team`.
    /// `team` is commonly a [LamellarWorld][crate::LamellarWorld] or [LamellarTeam][crate::LamellarTeam] (instance or reference). 
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `team` to enter the constructor call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<f32> = AtomicArray::new(&world,100,Distribution::Cyclic);
    pub fn new<U: Clone + Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> AtomicArray<T> {
        // println!("new atomic array");
        if NATIVE_ATOMICS.contains(&TypeId::of::<T>()) {
            NativeAtomicArray::new_internal(team, array_size, distribution).into()
        } else {
            GenericAtomicArray::new(team, array_size, distribution).into()
        }
    }
}
impl<T: Dist + 'static> AtomicArray<T> {
    pub(crate) fn get_element(&self, index: usize) -> Option<AtomicElement<T>> {
        match self {
            AtomicArray::NativeAtomicArray(array) => Some(array.get_element(index)?.into()),
            AtomicArray::GenericAtomicArray(array) => Some(array.get_element(index)?.into()),
        }
    }
}

impl<T: Dist> AtomicArray<T> {
    #[doc(alias("One-sided", "onesided"))]
    /// Change the distribution this array handle uses to index into the data of the array.
    ///
    /// # One-sided Operation
    /// This is a one-sided call and does not redistribute or modify the actual data, it simply changes how the array is indexed for this particular handle.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    /// // do something interesting... or not
    /// let block_view = array.clone().use_distribution(Distribution::Block);
    ///```
    pub fn use_distribution(self, distribution: Distribution) -> Self {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.use_distribution(distribution).into(),
            AtomicArray::GenericAtomicArray(array) => array.use_distribution(distribution).into(),
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the calling PE's local data as an [AtomicLocalData], which allows safe access to local elements.   
    ///
    /// Because each element is Atomic, this handle to the local data can be used to both read and write individual elements safely.
    ///
    /// # One-sided Operation
    /// Only returns local data on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.local_data();
    /// println!("PE{my_pe} local_data[0]: {:?}",local_data.at(0).load());
    ///```
    pub fn local_data(&self) -> AtomicLocalData<T> {
        AtomicLocalData {
            array: self.clone(),
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the calling PE's local data as an [AtomicLocalData], which allows safe mutable access to local elements.   
    ///
    /// Because each element is Atomic, this handle to the local data can be used to both read and write individual elements safely.
    ///
    /// # One-sided Operation
    /// Only returns local data on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.local_data();
    /// println!("PE{my_pe} local_data[0]: {:?}",local_data.at(0).load());
    ///```
    pub fn mut_local_data(&self) -> AtomicLocalData<T> {
        AtomicLocalData {
            array: self.clone(),
        }
    }

    #[doc(hidden)]
    pub unsafe fn __local_as_slice(&self) -> &[T] {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.__local_as_slice(),
            AtomicArray::GenericAtomicArray(array) => array.__local_as_slice(),
        }
    }
    #[doc(hidden)]
    pub unsafe fn __local_as_mut_slice(&self) -> &mut [T] {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.__local_as_mut_slice(),
            AtomicArray::GenericAtomicArray(array) => array.__local_as_mut_slice(),
        }
    }

    #[doc(alias = "Collective")]
    /// Convert this AtomicArray into an [UnsafeArray][crate::array::UnsafeArray]
    ///
    /// This is a collective and blocking function which will only return when there is at most a single reference on each PE
    /// to this Array, and that reference is currently calling this function.
    ///
    /// When it returns, it is gauranteed that there are only  `UnsafeArray` handles to the underlying data
    ///
    /// Note, that while this call itself is safe, and `UnsafeArray` unsurprisingly is not safe and thus you need to tread very carefully
    /// doing any operations with the resulting array.
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `array` to enter the call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let unsafe_array = array.into_unsafe();
    ///```
    ///
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let array1 = array.clone();
    /// let slice = array1.local_data();
    ///
    /// // no borrows to this specific instance (array) so it can enter the "into_unsafe" call
    /// // but array1 will not be dropped until after 'slice' is dropped.
    /// // Given the ordering of these calls we will get stuck in "into_unsafe" as it
    /// // waits for the reference count to go down to "1" (but we will never be able to drop slice/array1).
    /// let unsafe_array = array.into_unsafe();
    /// unsafe_array.print();
    /// println!("{:?}",slice.at(0).load());
    ///```
    pub fn into_unsafe(self) -> UnsafeArray<T> {
        // println!("atomic into_unsafe");
        match self {
            AtomicArray::NativeAtomicArray(array) => array.into(),
            AtomicArray::GenericAtomicArray(array) => array.into(),
        }
    }
    // pub fn into_local_only(self) -> LocalOnlyArray<T> {
    //     // println!("atomic into_local_only");
    //     match self {
    //         AtomicArray::NativeAtomicArray(array) => array.array.into(),
    //         AtomicArray::GenericAtomicArray(array) => array.array.into(),
    //     }
    // }

    #[doc(alias = "Collective")]
    /// Convert this AtomicArray into a (safe) [ReadOnlyArray][crate::array::ReadOnlyArray]
    ///
    /// This is a collective and blocking function which will only return when there is at most a single reference on each PE
    /// to this Array, and that reference is currently calling this function.
    ///
    /// When it returns, it is gauranteed that there are only `ReadOnlyArray` handles to the underlying data
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `array` to enter the call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let read_only_array = array.into_read_only();
    ///```
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let array1 = array.clone();
    /// let slice = unsafe {array1.local_data()};
    ///
    /// // no borrows to this specific instance (array) so it can enter the "into_read_only" call
    /// // but array1 will not be dropped until after mut_slice is dropped.
    /// // Given the ordering of these calls we will get stuck in "into_read_only" as it
    /// // waits for the reference count to go down to "1" (but we will never be able to drop slice/array1).
    /// let read_only_array = array.into_read_only();
    /// read_only_array.print();
    /// println!("{:?}",slice.at(0).load());
    ///```
    pub fn into_read_only(self) -> ReadOnlyArray<T> {
        // println!("atomic into_read_only");
        match self {
            AtomicArray::NativeAtomicArray(array) => array.array.into(),
            AtomicArray::GenericAtomicArray(array) => array.array.into(),
        }
    }

    #[doc(alias = "Collective")]
    /// Convert this AtomicArray into a (safe) [LocalLockArray][crate::array::LocalLockArray]
    ///
    /// This is a collective and blocking function which will only return when there is at most a single reference on each PE
    /// to this Array, and that reference is currently calling this function.
    ///
    /// When it returns, it is gauranteed that there are only `LocalLockArray` handles to the underlying data
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `array` to enter the call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_lock_array = array.into_local_lock();
    ///```
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let array1 = array.clone();
    /// let slice = unsafe {array1.local_data()};
    ///
    /// // no borrows to this specific instance (array) so it can enter the "into_local_lock" call
    /// // but array1 will not be dropped until after mut_slice is dropped.
    /// // Given the ordering of these calls we will get stuck in "into_local_lock" as it
    /// // waits for the reference count to go down to "1" (but we will never be able to drop slice/array1).
    /// let local_lock_array = array.into_local_lock();
    /// local_lock_array.print();
    /// println!("{:?}",slice.at(0).load());
    ///```
    pub fn into_local_lock(self) -> LocalLockArray<T> {
        // println!("atomic into_local_lock");
        match self {
            AtomicArray::NativeAtomicArray(array) => array.array.into(),
            AtomicArray::GenericAtomicArray(array) => array.array.into(),
        }
    }
}

impl<T: Dist + 'static> From<UnsafeArray<T>> for AtomicArray<T> {
    fn from(array: UnsafeArray<T>) -> Self {
        // println!("Converting from UnsafeArray to AtomicArray");
        if NATIVE_ATOMICS.contains(&TypeId::of::<T>()) {
            NativeAtomicArray::from(array).into()
        } else {
            GenericAtomicArray::from(array).into()
        }
    }
}

// impl<T: Dist + 'static> From<LocalOnlyArray<T>> for AtomicArray<T> {
//     fn from(array: LocalOnlyArray<T>) -> Self {
//         // println!("Converting from LocalOnlyArray to AtomicArray");
//         unsafe { array.into_inner().into() }
//     }
// }

impl<T: Dist + 'static> From<ReadOnlyArray<T>> for AtomicArray<T> {
    fn from(array: ReadOnlyArray<T>) -> Self {
        // println!("Converting from ReadOnlyArray to AtomicArray");
        unsafe { array.into_inner().into() }
    }
}
impl<T: Dist + 'static> From<LocalLockArray<T>> for AtomicArray<T> {
    fn from(array: LocalLockArray<T>) -> Self {
        // println!("Converting from LocalLockArray to AtomicArray");
        unsafe { array.into_inner().into() }
    }
}

impl<T: Dist> From<AtomicArray<T>> for AtomicByteArray {
    fn from(array: AtomicArray<T>) -> Self {
        match array {
            AtomicArray::NativeAtomicArray(array) => array.into(),
            AtomicArray::GenericAtomicArray(array) => array.into(),
        }
    }
}

impl<T: Dist> From<AtomicArray<T>> for LamellarByteArray {
    fn from(array: AtomicArray<T>) -> Self {
        match array {
            AtomicArray::NativeAtomicArray(array) => array.into(),
            AtomicArray::GenericAtomicArray(array) => array.into(),
        }
    }
}

impl<T: Dist> From<AtomicByteArray> for AtomicArray<T> {
    fn from(array: AtomicByteArray) -> Self {
        match array {
            AtomicByteArray::NativeAtomicByteArray(array) => array.into(),
            AtomicByteArray::GenericAtomicByteArray(array) => array.into(),
        }
    }
}

impl<T: Dist + AmDist + 'static> LamellarArrayReduce<T>
    for AtomicArray<T>
{
    fn reduce(&self,reduction: &str) -> Pin<Box<dyn Future<Output = T>>> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.reduce(reduction),
            AtomicArray::GenericAtomicArray(array) => array.reduce(reduction),
        }
    }
}

impl<T: Dist + AmDist + ElementArithmeticOps + 'static> LamellarArrayArithmeticReduce<T>
    for AtomicArray<T>
{
    fn sum(&self) -> Pin<Box<dyn Future<Output = T>>> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.sum(),
            AtomicArray::GenericAtomicArray(array) => array.sum(),
        }
    }
    fn prod(&self) -> Pin<Box<dyn Future<Output = T>>> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.prod(),
            AtomicArray::GenericAtomicArray(array) => array.prod(),
        }
    }
}
impl<T: Dist + AmDist + ElementComparePartialEqOps + 'static> LamellarArrayCompareReduce<T>
    for AtomicArray<T>
{
    fn max(&self) -> Pin<Box<dyn Future<Output = T>>> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.max(),
            AtomicArray::GenericAtomicArray(array) => array.max(),
        }
    }
    fn min(&self) -> Pin<Box<dyn Future<Output = T>>> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.min(),
            AtomicArray::GenericAtomicArray(array) => array.min(),
        }
    }
}

impl<T: Dist> LamellarWrite for AtomicArray<T> {}
impl<T: Dist> LamellarRead for AtomicArray<T> {}

impl<T: Dist + std::fmt::Debug> AtomicArray<T> {
    pub fn print(&self) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.print(),
            AtomicArray::GenericAtomicArray(array) => array.print(),
        }
    }
}

impl<T: Dist + std::fmt::Debug> ArrayPrint<T> for AtomicArray<T> {
    fn print(&self) {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.print(),
            AtomicArray::GenericAtomicArray(array) => array.print(),
        }
    }
}

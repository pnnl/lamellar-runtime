#[cfg(not(feature = "non-buffered-array-ops"))]
pub(crate) mod buffered_operations;
mod iteration;
#[cfg(not(feature = "non-buffered-array-ops"))]
// pub(crate) use buffered_operations as operations;
pub(crate) mod rdma;
pub use rdma::{AtomicArrayGet, AtomicArrayPut};

// use crate::array::atomic::buffered_operations::BUFOPS;
// use crate::array::private::{ArrayExecAm};
// use crate::array::r#unsafe::UnsafeByteArray;
use crate::array::*;
use crate::array::native_atomic::NativeAtomicElement;
use crate::array::atomic2::Atomic2Element;
// use crate::darc::{Darc, DarcMode};
use crate::lamellar_team::{IntoLamellarTeam};
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

// pub trait AtomicOps {
//     type Atomic;
//     fn as_native_atomic(&self) -> &Self::Atomic;
//     fn fetch_add(&self, val: Self) -> Self;
//     fn fetch_sub(&mut self, val: Self) -> Self;
//     fn fetch_mul(&mut self, val: Self) -> Self;
//     fn fetch_div(&mut self, val: Self) -> Self;
//     fn fetch_bit_and(&mut self, val: Self) -> Self;
//     fn fetch_bit_or(&mut self, val: Self) -> Self;
//     fn compare_exchange(&mut self, current: Self, new: Self) -> Result<Self, Self>
//     where
//         Self: Sized;
    // fn load(&mut self) -> Self;
//     fn store(&mut self, val: Self);
//     fn swap(&mut self, val: Self) -> Self;
// }


use std::ops::{AddAssign, BitAndAssign, BitOrAssign, DivAssign, MulAssign, SubAssign};


pub enum AtomicElement<T: Dist>{
    NativeAtomicElement(NativeAtomicElement<T>),
    Atomic2Element(Atomic2Element<T>),
}

impl <T: Dist> AtomicElement<T>{
    pub fn load(&self) -> T {
        match self{
            AtomicElement::NativeAtomicElement(array) => array.load(),
            AtomicElement::Atomic2Element(array) => array.load(),
        }
    }
    pub fn store(&self, val: T) {
        match self{
            AtomicElement::NativeAtomicElement(array) => array.store(val),
            AtomicElement::Atomic2Element(array) => array.store(val),
        }
    }
}

impl<T: Dist + ElementArithmeticOps> AddAssign<T> for AtomicElement<T> {
    fn add_assign(&mut self, val: T) {
        match self{
            AtomicElement::NativeAtomicElement(array) => array.add_assign(val),
            AtomicElement::Atomic2Element(array) => array.add_assign(val),
        }
    }
}

impl<T: Dist + ElementArithmeticOps> SubAssign<T> for AtomicElement<T> {
    fn sub_assign(&mut self, val: T) {
        match self{
            AtomicElement::NativeAtomicElement(array) => array.sub_assign(val),
            AtomicElement::Atomic2Element(array) => array.sub_assign(val),
        }
    }
}

impl<T: Dist + ElementArithmeticOps> MulAssign<T> for AtomicElement<T> {
    fn mul_assign(&mut self, val: T) {
        match self{
            AtomicElement::NativeAtomicElement(array) => array.mul_assign(val),
            AtomicElement::Atomic2Element(array) => array.mul_assign(val),
        }
    }
}

impl<T: Dist + ElementArithmeticOps> DivAssign<T> for AtomicElement<T> {
    fn div_assign(&mut self, val: T) {
        match self{
            AtomicElement::NativeAtomicElement(array) => array.div_assign(val),
            AtomicElement::Atomic2Element(array) => array.div_assign(val),
        }
    }
}

impl<T: Dist + ElementBitWiseOps> BitAndAssign<T> for AtomicElement<T> {
    fn bitand_assign(&mut self, val: T) {
        match self{
            AtomicElement::NativeAtomicElement(array) => array.bitand_assign(val),
            AtomicElement::Atomic2Element(array) => array.bitand_assign(val),
        }
    }
}

impl<T: Dist + ElementBitWiseOps> BitOrAssign<T> for AtomicElement<T> {
    fn bitor_assign(&mut self, val: T) {
        match self{
            AtomicElement::NativeAtomicElement(array) => array.bitor_assign(val),
            AtomicElement::Atomic2Element(array) => array.bitor_assign(val),
        }
    }
}


#[enum_dispatch(LamellarArray<T>,LamellarArrayGet<T>,LamellarArrayPut<T>,ArrayExecAm<T>,LamellarArrayPrivate<T>,DistIteratorLauncher,)]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(bound = "T: Dist + serde::Serialize + serde::de::DeserializeOwned + 'static")]
pub enum AtomicArray<T: Dist>{
    NativeAtomicArray(NativeAtomicArray<T>),
    Atomic2Array(Atomic2Array<T>),
}

impl<T: Dist + 'static> crate::DarcSerde for AtomicArray<T> {
    fn ser(&self, num_pes: usize, cur_pe: Result<usize, crate::IdError>) {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.ser(num_pes, cur_pe),
            AtomicArray::Atomic2Array(array) => array.ser(num_pes, cur_pe),
        }
    }
    fn des(&self, cur_pe: Result<usize, crate::IdError>) {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.des(cur_pe),
            AtomicArray::Atomic2Array(array) => array.des(cur_pe),
        }
    }
}


#[enum_dispatch]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum AtomicByteArray{
    NativeAtomicByteArray(NativeAtomicByteArray),
    Atomic2ByteArray(Atomic2ByteArray),
}

impl crate::DarcSerde for AtomicByteArray {
    fn ser(&self, num_pes: usize, cur_pe: Result<usize, crate::IdError>) {
        match self{
            AtomicByteArray::NativeAtomicByteArray(array) => array.ser(num_pes, cur_pe),
            AtomicByteArray::Atomic2ByteArray(array) => array.ser(num_pes, cur_pe),
        }
    }

    fn des(&self, cur_pe: Result<usize, crate::IdError>) {
        match self{
            AtomicByteArray::NativeAtomicByteArray(array) => array.des(cur_pe),
            AtomicByteArray::Atomic2ByteArray(array) => array.des(cur_pe),
        }
    }
}



pub struct AtomicLocalData<T: Dist> {
    array: AtomicArray<T>,
}

pub struct AtomicLocalDataIter<T: Dist> {
    array: AtomicArray<T>,
    index: usize,
}

impl<T: Dist> AtomicLocalData<T> {
    pub fn at(&self, index: usize) -> AtomicElement<T> {
        self.array.get_element(index)
    }

    pub fn get_mut(&self, index: usize) -> Option<AtomicElement<T>> {
        Some(self.array.get_element(index))
    }

    pub fn len(&self) -> usize {
        unsafe { self.array.__local_as_mut_slice().len() }
    }

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
            Some(self.array.get_element(index))
        } else {
            None
        }
    }
}


//#[prof]
impl<T: Dist + std::default::Default + 'static> AtomicArray<T> {
    
    pub fn new<U: Clone + Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> AtomicArray<T> {
        if NATIVE_ATOMICS.contains(&TypeId::of::<T>()){
            NativeAtomicArray::new_internal(team,array_size,distribution).into()
        }
        else{
            Atomic2Array::new(team,array_size,distribution).into()
        }
        
    }
}
impl<T: Dist + 'static> AtomicArray<T> {

    pub(crate) fn get_element(&self, index: usize) -> AtomicElement<T>{
        match self{
            AtomicArray::NativeAtomicArray(array) => array.get_element(index).into() ,
            AtomicArray::Atomic2Array(array) => array.get_element(index).into() ,
        }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub fn wait_all(&self) {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.wait_all() ,
            AtomicArray::Atomic2Array(array) => array.wait_all() ,
        }
    }
    pub fn barrier(&self) {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.barrier() ,
            AtomicArray::Atomic2Array(array) => array.barrier() ,
        }
    }
    pub(crate) fn num_elems_local(&self) -> usize {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.num_elems_local() ,
            AtomicArray::Atomic2Array(array) => array.num_elems_local() ,
        }
    }

    pub fn use_distribution(self, distribution: Distribution) -> Self {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.use_distribution(distribution).into() ,
            AtomicArray::Atomic2Array(array) => array.use_distribution(distribution).into() ,
        }
    }

    pub fn num_pes(&self) -> usize {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.num_pes() ,
            AtomicArray::Atomic2Array(array) => array.num_pes() ,
        }
    }

    #[doc(hidden)]
    pub fn pe_for_dist_index(&self, index: usize) -> Option<usize> {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.pe_for_dist_index(index) ,
            AtomicArray::Atomic2Array(array) => array.pe_for_dist_index(index) ,
        }
    }

    #[doc(hidden)]
    pub fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> Option<usize> {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.pe_offset_for_dist_index(pe,index) ,
            AtomicArray::Atomic2Array(array) => array.pe_offset_for_dist_index(pe,index) ,
        }
    }

    pub fn len(&self) -> usize {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.len() ,
            AtomicArray::Atomic2Array(array) => array.len() ,
        }
    }

    pub fn local_data(&self) -> AtomicLocalData<T> {
        AtomicLocalData {
            array: self.clone(),
        }
    }

    pub fn mut_local_data(&self) -> AtomicLocalData<T> {
        AtomicLocalData {
            array: self.clone(),
        }
    }

    #[doc(hidden)]
    pub unsafe fn __local_as_slice(&self) -> &[T] {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.__local_as_slice() ,
            AtomicArray::Atomic2Array(array) => array.__local_as_slice() ,
        }
    }
    #[doc(hidden)]
    pub unsafe fn __local_as_mut_slice(&self) -> &mut [T] {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.__local_as_mut_slice() ,
            AtomicArray::Atomic2Array(array) => array.__local_as_mut_slice() ,
        }
    }
    pub fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> AtomicArray<T> {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.sub_array(range).into() ,
            AtomicArray::Atomic2Array(array) => array.sub_array(range).into() ,
        }
    }
    pub fn into_unsafe(self) -> UnsafeArray<T> {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.into() ,
            AtomicArray::Atomic2Array(array) => array.into() ,
        }
    }
    pub fn into_local_only(self) -> LocalOnlyArray<T> {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.array.into() ,
            AtomicArray::Atomic2Array(array) => array.array.into() ,
        }
    }
    pub fn into_read_only(self) -> ReadOnlyArray<T> {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.array.into() ,
            AtomicArray::Atomic2Array(array) => array.array.into() ,
        }
    }
    pub fn into_local_lock_atomic(self) -> LocalLockAtomicArray<T> {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.array.into() ,
            AtomicArray::Atomic2Array(array) => array.array.into() ,
        }
    }
    pub fn into_atomic2(self) -> Atomic2Array<T> {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.array.into() ,
            AtomicArray::Atomic2Array(array) => array,
        }
    }
}

impl<T: Dist + 'static> From<UnsafeArray<T>> for AtomicArray<T> {
    fn from(array: UnsafeArray<T>) -> Self {
        if NATIVE_ATOMICS.contains(&TypeId::of::<T>()){
            NativeAtomicArray::from(array).into()
        }
        else{
            Atomic2Array::from(array).into()
        }
    }
}

impl<T: Dist> From<AtomicArray<T>> for AtomicByteArray {
    fn from(array: AtomicArray<T>) -> Self {
        match array{
            AtomicArray::NativeAtomicArray(array) => array.into() ,
            AtomicArray::Atomic2Array(array) => array.into() ,
        }
    }
}

impl<T: Dist> From<AtomicByteArray> for AtomicArray<T> {
    fn from(array: AtomicByteArray) -> Self {
        match array{
            AtomicByteArray::NativeAtomicByteArray(array) => array.into(),
            AtomicByteArray::Atomic2ByteArray(array) => array.into(),
        }
    }
}

impl<T: Dist + serde::Serialize + serde::de::DeserializeOwned + 'static> AtomicArray<T> {
    pub fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.reduce(op),
            AtomicArray::Atomic2Array(array) => array.reduce(op),
        }
    }
    pub fn sum(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.reduce("sum"),
            AtomicArray::Atomic2Array(array) => array.reduce("sum"),
        }
    }
    pub fn prod(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.reduce("prod"),
            AtomicArray::Atomic2Array(array) => array.reduce("prod"),
        }
    }
    pub fn max(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.reduce("max") ,
            AtomicArray::Atomic2Array(array) => array.reduce("max") ,
        }
    }
}

// impl<T: Dist> private::ArrayExecAm<T> for AtomicArray<T> {
//     fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
//         self.array.team().clone()
//     }
//     fn team_counters(&self) -> Arc<AMCounters> {
//         self.array.team_counters()
//     }
// }

// impl<T: Dist> private::LamellarArrayPrivate<T> for AtomicArray<T> {
//     fn local_as_ptr(&self) -> *const T {
//         self.array.local_as_mut_ptr()
//     }
//     fn local_as_mut_ptr(&self) -> *mut T {
//         self.array.local_as_mut_ptr()
//     }
//     fn pe_for_dist_index(&self, index: usize) -> Option<usize> {
//         self.array.pe_for_dist_index(index)
//     }
//     fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> Option<usize> {
//         self.array.pe_offset_for_dist_index(pe, index)
//     }
//     unsafe fn into_inner(self) -> UnsafeArray<T> {
//         self.array
//     }
// }


// impl<T: Dist> LamellarArray<T> for AtomicArray<T> {
//     fn my_pe(&self) -> usize {
//         self.array.my_pe()
//     }
//     fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
//         self.array.team().clone()
//     }
//     fn num_elems_local(&self) -> usize {
//         self.num_elems_local()
//     }
//     fn len(&self) -> usize {
//         self.len()
//     }
//     fn barrier(&self) {
//         self.barrier();
//     }
//     fn wait_all(&self) {
//         self.array.wait_all()
//         // println!("done in wait all {:?}",std::time::SystemTime::now());
//     }
//     fn pe_and_offset_for_global_index(&self, index: usize) -> Option<(usize, usize)> {
//         self.array.pe_and_offset_for_global_index(index)
//     }
// }

impl<T: Dist> LamellarWrite for AtomicArray<T> {}
impl<T: Dist> LamellarRead for AtomicArray<T> {}


// impl<T: Dist> SubArray<T> for AtomicArray<T> {
//     type Array = AtomicArray<T>;
//     fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array {
//         self.sub_array(range).into()
//     }
//     fn global_index(&self, sub_index: usize) -> usize {
//         self.array.global_index(sub_index)
//     }
// }

// impl<T: Dist + std::fmt::Debug> AtomicArray<T> {
//     pub fn print(&self) {
//         self.array.print();
//     }
// }

// impl<T: Dist + std::fmt::Debug> ArrayPrint<T> for AtomicArray<T> {
//     fn print(&self) {
//         self.array.print()
//     }
// }



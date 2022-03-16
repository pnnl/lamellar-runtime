#[cfg(not(feature = "non-buffered-array-ops"))]
pub(crate) mod buffered_operations;
mod iteration;
#[cfg(not(feature = "non-buffered-array-ops"))]
pub(crate) use buffered_operations as operations;
mod rdma;
use crate::array::atomic2::operations::BUFOPS;
use crate::array::private::LamellarArrayPrivate;
use crate::array::r#unsafe::UnsafeByteArray;
use crate::array::atomic::AtomicElement;
use crate::array::*;
use crate::darc::Darc;
use crate::darc::DarcMode;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::Dist;
use parking_lot::{
    Mutex,MutexGuard
};
use std::any::TypeId;
// use std::ops::{Deref, DerefMut};



use std::ops::{AddAssign, BitAndAssign, BitOrAssign, DivAssign, MulAssign, SubAssign};
pub struct Atomic2Element<T: Dist> {
    array: Atomic2Array<T>,
    local_index: usize,
}

impl <T: Dist> From<Atomic2Element<T>> for AtomicElement<T>{
    fn from(element: Atomic2Element<T>) -> AtomicElement<T>{
        AtomicElement::Atomic2Element(element)
    }
}

impl <T: Dist> Atomic2Element<T>{
    pub fn load(&self) -> T {
        let _lock =self.array.lock_index(self.local_index);
        unsafe {
            self.array.__local_as_mut_slice()[self.local_index]
        }
    }
    pub fn store(&self, val: T) {
        let _lock =self.array.lock_index(self.local_index);
        unsafe {
            self.array.__local_as_mut_slice()[self.local_index] = val;
        }
    }
}
//todo does this work on sub arrays?
impl<T: Dist + ElementArithmeticOps> AddAssign<T> for Atomic2Element<T> {
    fn add_assign(&mut self, val: T) {
        // self.add(val)
        let _lock =self.array.lock_index(self.local_index);
        unsafe {
            self.array.__local_as_mut_slice()[self.local_index] += val
        }
    }
}

impl<T: Dist + ElementArithmeticOps> SubAssign<T> for Atomic2Element<T> {
    fn sub_assign(&mut self, val: T) {
        let _lock =self.array.lock_index(self.local_index);
        unsafe {
            self.array.__local_as_mut_slice()[self.local_index] -= val
        }
    }
}

impl<T: Dist + ElementArithmeticOps> MulAssign<T> for Atomic2Element<T> {
    fn mul_assign(&mut self, val: T) {
        let _lock =self.array.lock_index(self.local_index);
        unsafe {
            self.array.__local_as_mut_slice()[self.local_index] *= val
        }
    }
}

impl<T: Dist + ElementArithmeticOps> DivAssign<T> for Atomic2Element<T> {
    fn div_assign(&mut self, val: T) {
        let _lock =self.array.lock_index(self.local_index);
        unsafe {
            self.array.__local_as_mut_slice()[self.local_index] /= val
        }
    }
}

impl<T: Dist + ElementBitWiseOps> BitAndAssign<T> for Atomic2Element<T> {
    fn bitand_assign(&mut self, val: T) {
        let _lock =self.array.lock_index(self.local_index);
        unsafe {
            self.array.__local_as_mut_slice()[self.local_index] &= val
        }
    }
}

impl<T: Dist + ElementBitWiseOps> BitOrAssign<T> for Atomic2Element<T> {
    fn bitor_assign(&mut self, val: T) {
        let _lock =self.array.lock_index(self.local_index);
        unsafe {
            self.array.__local_as_mut_slice()[self.local_index] |= val
        }
    }
}


#[lamellar_impl::AmDataRT(Clone)]
pub struct Atomic2Array<T: Dist> {
    locks: Darc<Vec<Mutex<()>>>,
    pub(crate) array: UnsafeArray<T>,
}

#[lamellar_impl::AmDataRT(Clone)]
pub struct Atomic2ByteArray {
    locks: Darc<Vec<Mutex<()>>>,
    pub(crate) array: UnsafeByteArray,
}

impl Atomic2ByteArray{
    #[doc(hidden)]
    pub fn lock_index(&self, index: usize) -> MutexGuard<()> {
        self.locks[index].lock()
    }
}

pub struct Atomic2LocalData<T: Dist> {
    array: Atomic2Array<T>,
}

pub struct Atomic2LocalDataIter<T: Dist> {
    array: Atomic2Array<T>,
    index: usize,
}

impl<T: Dist> Atomic2LocalData<T> {
    pub fn at(&self, index: usize) -> Atomic2Element<T> {
        Atomic2Element {
            array: self.array.clone(),
            local_index: index,
        }
    }

    pub fn get_mut(&self, index: usize) -> Option<Atomic2Element<T>> {
        Some(Atomic2Element {
            array: self.array.clone(),
            local_index: index,
        })
    }

    pub fn len(&self) -> usize {
        unsafe { self.array.__local_as_mut_slice().len() }
    }

    pub fn iter(&self) -> Atomic2LocalDataIter<T> {
        Atomic2LocalDataIter {
            array: self.array.clone(),
            index: 0,
        }
    }
}

impl<T: Dist> IntoIterator for Atomic2LocalData<T> {
    type Item = Atomic2Element<T>;
    type IntoIter = Atomic2LocalDataIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        Atomic2LocalDataIter {
            array: self.array,
            index: 0,
        }
    }
}

impl<T: Dist> Iterator for Atomic2LocalDataIter<T> {
    type Item = Atomic2Element<T>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.array.num_elems_local() {
            let index = self.index;
            self.index += 1;
            Some(Atomic2Element {
                array: self.array.clone(),
                local_index: index,
            })
        } else {
            None
        }
    }
}

impl<T: Dist + std::default::Default> Atomic2Array<T> {
    //Sync + Send + Copy  == Dist
    pub fn new<U: Clone + Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> Atomic2Array<T> {
        let array = UnsafeArray::new(team.clone(), array_size, distribution);
        let mut vec = vec![];
        for _i in 0..array.num_elems_local(){
            vec.push(Mutex::new(()));
        }
        let locks = Darc::new(team, vec).unwrap();

        if let Some(func) = BUFOPS.get(&TypeId::of::<T>()) {
            let mut op_bufs = array.inner.data.op_buffers.write();
            let bytearray = Atomic2ByteArray {
                locks: locks.clone(),
                array: array.clone().into(),
            };

            for pe in 0..op_bufs.len() {
                op_bufs[pe] = func(bytearray.clone());
            }
        }

        Atomic2Array {
            locks: locks,
            array: array,
        }
    }
}

impl<T: Dist > Atomic2Array<T> {
    pub(crate) fn get_element(&self, index: usize) -> Atomic2Element<T>{
        Atomic2Element{
            array: self.clone(),
            local_index: index,
        }
    }
}

impl<T: Dist> Atomic2Array<T> {
    pub fn wait_all(&self) {
        self.array.wait_all();
    }
    pub fn barrier(&self) {
        self.array.barrier();
    }
    pub(crate) fn num_elems_local(&self) -> usize {
        self.array.num_elems_local()
    }

    pub fn use_distribution(self, distribution: Distribution) -> Self {
        Atomic2Array {
            locks: self.locks.clone(),
            array: self.array.use_distribution(distribution),
        }
    }

    pub fn num_pes(&self) -> usize {
        self.array.num_pes()
    }

    #[doc(hidden)]
    pub fn pe_for_dist_index(&self, index: usize) -> Option<usize> {
        self.array.pe_for_dist_index(index)
    }

    #[doc(hidden)]
    pub fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> Option<usize> {
        self.array.pe_offset_for_dist_index(pe, index)
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }


    pub fn local_data(&self) -> Atomic2LocalData<T> {
        Atomic2LocalData {
            array: self.clone(),
        }
    }

    pub fn mut_local_data(&self) -> Atomic2LocalData<T> {
        Atomic2LocalData {
            array: self.clone(),
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

    pub fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self {
        Atomic2Array {
            locks: self.locks.clone(),
            array: self.array.sub_array(range),
        }
    }

    pub fn into_unsafe(self) -> UnsafeArray<T> {
        self.array.into()
    }

    pub fn into_local_only(self) -> LocalOnlyArray<T> {
        self.array.into()
    }

    pub fn into_read_only(self) -> ReadOnlyArray<T> {
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
        self.locks[index].lock()
    }
}

impl<T: Dist + 'static> Atomic2Array<T> {
    pub fn into_atomic(self) -> Atomic2Array<T> {
        self.array.into()
    }
}

impl<T: Dist> From<UnsafeArray<T>> for Atomic2Array<T> {
    fn from(array: UnsafeArray<T>) -> Self {
        array.block_on_outstanding(DarcMode::Atomic2Array);
        let mut vec = vec![];
        for _i in 0..array.num_elems_local(){
            vec.push(Mutex::new(()));
        }
        let locks = Darc::new(array.team(), vec).unwrap();
        if let Some(func) = BUFOPS.get(&TypeId::of::<T>()) {
            let bytearray = Atomic2ByteArray {
                locks: locks.clone(),
                array: array.clone().into(),
            };
            let mut op_bufs = array.inner.data.op_buffers.write();
            for _pe in 0..array.inner.data.num_pes {
                op_bufs.push(func(bytearray.clone()))
            }
        }
        Atomic2Array {
            locks: locks,
            array: array,
        }
    }
}

impl<T: Dist> From<Atomic2Array<T>> for Atomic2ByteArray {
    fn from(array: Atomic2Array<T>) -> Self {
        Atomic2ByteArray {
            locks: array.locks.clone(),
            array: array.array.into(),
        }
    }
}
impl<T: Dist> From<Atomic2Array<T>> for AtomicByteArray {
    fn from(array: Atomic2Array<T>) -> Self {
        AtomicByteArray::Atomic2ByteArray(Atomic2ByteArray {
            locks: array.locks.clone(),
            array: array.array.into(),
        })
    }
}
impl<T: Dist> From<Atomic2ByteArray> for Atomic2Array<T> {
    fn from(array: Atomic2ByteArray) -> Self {
        Atomic2Array {
            locks: array.locks.clone(),
            array: array.array.into(),
        }
    }
}
impl<T: Dist> From<Atomic2ByteArray> for AtomicArray<T> {
    fn from(array: Atomic2ByteArray) -> Self {
        Atomic2Array {
            locks: array.locks.clone(),
            array: array.array.into(),
        }.into()
    }
}

impl<T: Dist> private::ArrayExecAm<T> for Atomic2Array<T> {
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team().clone()
    }
    fn team_counters(&self) -> Arc<AMCounters> {
        self.array.team_counters()
    }
}

impl<T: Dist> private::LamellarArrayPrivate<T> for Atomic2Array<T> {
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

impl<T: Dist> LamellarArray<T> for Atomic2Array<T> {
    fn my_pe(&self) -> usize {
        self.array.my_pe()
    }
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team().clone()
    }
    fn num_elems_local(&self) -> usize {
        self.num_elems_local()
    }
    fn len(&self) -> usize {
        self.len()
    }
    fn barrier(&self) {
        self.barrier();
    }
    fn wait_all(&self) {
        self.array.wait_all()
        // println!("done in wait all {:?}",std::time::SystemTime::now());
    }
    fn pe_and_offset_for_global_index(&self, index: usize) -> Option<(usize, usize)> {
        self.array.pe_and_offset_for_global_index(index)
    }
}

impl<T: Dist> LamellarWrite for Atomic2Array<T> {}
impl<T: Dist> LamellarRead for Atomic2Array<T> {}

impl<T: Dist> SubArray<T> for Atomic2Array<T> {
    type Array = Atomic2Array<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array {
        self.sub_array(range).into()
    }
    fn global_index(&self, sub_index: usize) -> usize {
        self.array.global_index(sub_index)
    }
}

impl<T: Dist + std::fmt::Debug> Atomic2Array<T> {
    pub fn print(&self) {
        self.array.print();
    }
}

impl<T: Dist + std::fmt::Debug> ArrayPrint<T> for Atomic2Array<T> {
    fn print(&self) {
        self.array.print()
    }
}


impl<T: Dist + AmDist + 'static> Atomic2Array<T> {
    pub fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.array.reduce(op)
    }
    pub fn sum(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.array.reduce("sum")
    }
    pub fn prod(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.array.reduce("prod")
    }
    pub fn max(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.array.reduce("max")
    }
}

// impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> LamellarArrayReduce<T>
//     for Atomic2Array<T>
// {
//     fn get_reduction_op(&self, op: String) -> LamellarArcAm {
//         self.array.get_reduction_op(op)
//     }
//     fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         self.reduce(op)
//     }
//     fn sum(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         self.sum()
//     }
//     fn max(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         self.max()
//     }
//     fn prod(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         self.prod()
//     }
// }

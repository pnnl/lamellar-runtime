mod iteration;
pub(crate) mod operations;
pub (crate) mod rdma;
pub use rdma::{AtomicArrayPut,AtomicArrayGet};

// use crate::array::iterator::distributed_iterator::{
//     DistIter, DistIterMut, DistIteratorLauncher, DistributedIterator,
// };
use crate::array::r#unsafe::UnsafeByteArray;
use crate::array::iterator::serial_iterator::LamellarArrayIter;
use crate::array::*;
use crate::array::private::LamellarArrayPrivate;
use crate::darc::{Darc, DarcMode};
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::Dist;
use core::marker::PhantomData;
use parking_lot::{Mutex, MutexGuard};
use std::any::TypeId;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;



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


mod atomic_private {
    use parking_lot::Mutex;
    pub trait LocksInit {
        fn init(&self, local_len: usize) -> Option<Vec<Mutex<()>>>;
    }
}

pub trait AtomicOps {
    type Atomic;
    fn as_atomic(&self) -> &Self::Atomic;
    fn fetch_add(&self, val: Self) -> Self;
    fn fetch_sub(&mut self, val: Self) -> Self;
    fn fetch_mul(&mut self, val: Self) -> Self;
    fn fetch_div(&mut self, val: Self) -> Self;
    fn fetch_bit_and(&mut self, val: Self) -> Self;
    fn fetch_bit_or(&mut self, val: Self) -> Self;
    fn compare_exchange(&mut self, current: Self, new: Self) -> Result<Self,Self> where Self: Sized;
    fn load(&mut self) -> Self;
    fn store(&mut self, val: Self);
    fn swap(&mut self, val: Self) -> Self;
}

macro_rules! impl_atomic_ops{
    { $A:ty, $B:ty } => {
        impl AtomicOps for $A {
            // there is an equivalent call in nightly rust
            // Self::Atomic::from_mut()... we will switch to that once stablized;
            type Atomic = $B;
            fn as_atomic(&self) -> &Self::Atomic{
                use std::mem::align_of;
                let [] = [(); align_of::<$B>() - align_of::<$A>()];
                // SAFETY:
                //  - the mutable reference guarantees unique ownership.
                //  - the alignment of `$int_type` and `Self` is the
                //    same, as promised by $cfg_align and verified above.
                unsafe { &*(self as *const $A as *mut $A as *mut Self::Atomic) }
            }
            fn fetch_add(&self, val: Self) ->Self {
                self.as_atomic().fetch_add(val, Ordering::SeqCst)
            }
            fn fetch_sub(&mut self, val: Self,) ->Self{
                self.as_atomic().fetch_sub(val, Ordering::SeqCst)
            }
            fn fetch_mul(&mut self, val: Self,) -> Self{
                let mut cur = self.as_atomic().load(Ordering::SeqCst);
                let mut new = cur*val;
                while self.compare_exchange(cur,new).is_err(){
                    std::thread::yield_now();
                    cur = self.as_atomic().load(Ordering::SeqCst);
                    new = cur*val;
                }
                cur
            }
            fn fetch_div(&mut self, val: Self,) -> Self{
                let mut cur = self.as_atomic().load(Ordering::SeqCst);
                let mut new = cur/val;
                while self.compare_exchange(cur,new).is_err(){
                    std::thread::yield_now();
                    cur = self.as_atomic().load(Ordering::SeqCst);
                    new = cur/val;
                }
                cur
            }
            fn fetch_bit_and(&mut self, val: Self,) -> Self{
                // println!("fetch_bit_and: {:?}",val);
                self.as_atomic().fetch_and(val,Ordering::SeqCst)
            }
            fn fetch_bit_or(&mut self, val: Self,) -> Self{
                // println!("fetch_bit_or: {:?}",val);
                self.as_atomic().fetch_or(val,Ordering::SeqCst)
            }
            fn compare_exchange(&mut self, current: Self, new: Self) -> Result<Self,Self> {
                self.as_atomic().compare_exchange(current,new,Ordering::SeqCst,Ordering::SeqCst)
            }
            fn load(&mut self) ->Self{
                self.as_atomic().load(Ordering::SeqCst)
            }
            fn store(&mut self, val: Self){
                self.as_atomic().store(val,Ordering::SeqCst)
            }
            fn swap(&mut self, val: Self) ->Self{
                self.as_atomic().swap(val, Ordering::SeqCst)
            }
        }
    }
}

use std::sync::atomic::AtomicI8;
impl_atomic_ops! {i8,AtomicI8}
use std::sync::atomic::AtomicI16;
impl_atomic_ops! {i16,AtomicI16}
use std::sync::atomic::AtomicI32;
impl_atomic_ops! {i32,AtomicI32}
use std::sync::atomic::AtomicI64;
impl_atomic_ops! {i64,AtomicI64}
use std::sync::atomic::AtomicIsize;
impl_atomic_ops! {isize,AtomicIsize}
use std::sync::atomic::AtomicU8;
impl_atomic_ops! {u8,AtomicU8}
use std::sync::atomic::AtomicU16;
impl_atomic_ops! {u16,AtomicU16}
use std::sync::atomic::AtomicU32;
impl_atomic_ops! {u32,AtomicU32}
use std::sync::atomic::AtomicU64;
impl_atomic_ops! {u64,AtomicU64}
use std::sync::atomic::AtomicUsize;
impl_atomic_ops! {usize,AtomicUsize}

#[lamellar_impl::AmDataRT(Clone)]
pub struct AtomicArray<T: Dist> {
    locks: Darc<Option<Vec<Mutex<()>>>>,
    orig_t_size: usize,
    pub(crate) array: UnsafeArray<T>,
}

#[lamellar_impl::AmDataRT(Clone)]
pub struct AtomicByteArray{
    locks: Darc<Option<Vec<Mutex<()>>>>,
    orig_t_size: usize,
    pub(crate) array: UnsafeByteArray,
}

//#[prof]
impl<T: Dist + std::default::Default + 'static> AtomicArray<T> {
    //Sync + Send + Copy  == Dist
    pub fn new<U: Clone + Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> AtomicArray<T> {
        let array = UnsafeArray::new(team.clone(), array_size, distribution);
        let locks = if NATIVE_ATOMICS.get(&TypeId::of::<T>()).is_some() {
            None
        } else {
            let mut vec = vec![];
            for _i in 0..array.num_elems_local() {
                vec.push(Mutex::new(()));
            }
            Some(vec)
        };
        AtomicArray {
            locks: Darc::new(team, locks).unwrap(),
            orig_t_size: std::mem::size_of::<T>(),
            array: array,
        }
    }
}

impl<T: Dist> AtomicArray<T> {
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
        AtomicArray {
            locks: self.locks.clone(),
            orig_t_size: self.orig_t_size,
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
    pub fn pe_offset_for_dist_index(&self, pe: usize, index: usize) ->  Option<usize> {
        self.array.pe_offset_for_dist_index(pe, index)
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }
    #[doc(hidden)]
    pub unsafe fn local_as_slice(&self) -> &[T] {
        self.array.local_as_mut_slice()
    }
    #[doc(hidden)]
    pub unsafe fn local_as_mut_slice(&self) -> &mut [T] {
        self.array.local_as_mut_slice()
    }
    pub fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> AtomicArray<T> {
        AtomicArray {
            locks: self.locks.clone(),
            orig_t_size: self.orig_t_size,
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
    pub fn lock_index(&self, index: usize) -> Option<Vec<MutexGuard<()>>> {
        if let Some(ref locks) = *self.locks {
            let start_index = (index * std::mem::size_of::<T>()) / self.orig_t_size;
            let end_index = ((index + 1) * std::mem::size_of::<T>()) / self.orig_t_size;
            let mut guards = vec![];
            for i in start_index..end_index {
                guards.push(locks[i].lock())
            }
            Some(guards)
        } else {
            None
        }
    }
}

impl<T: Dist + 'static> From<UnsafeArray<T>> for AtomicArray<T> {
    fn from(array: UnsafeArray<T>) -> Self{
        // let array = array.into_inner();
        array.block_on_outstanding(DarcMode::AtomicArray);
        let locks = if NATIVE_ATOMICS.get(&TypeId::of::<T>()).is_some() {
            None
        } else {
            let mut vec = vec![];
            for _i in 0..array.num_elems_local() {
                vec.push(Mutex::new(()));
            }
            Some(vec)
        };
        AtomicArray {
            locks: Darc::new(array.team(), locks).unwrap(),
            orig_t_size: std::mem::size_of::<T>(),
            array: array,
        }
    }
}

impl <T: Dist> From<AtomicArray<T>> for AtomicByteArray{
    fn from(array: AtomicArray<T>) -> Self {
        AtomicByteArray {
            locks: array.locks.clone(),
            orig_t_size: array.orig_t_size,
            array: array.array.into(),
        }
    }
}
impl <T: Dist> From<AtomicByteArray> for AtomicArray<T>{
    fn from(array: AtomicByteArray) -> Self {
        AtomicArray {
            locks: array.locks.clone(),
            orig_t_size: array.orig_t_size,
            array: array.array.into(),
        }
    }
}

// impl <T: Dist + serde::Serialize + serde::de::DeserializeOwned + 'static> AtomicArray<T> {
//     pub fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         self.array.reduce(op)
//     }
//     pub fn sum(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         self.array.reduce("sum")
//     }
//     pub fn prod(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         self.array.reduce("prod")
//     }
//     pub fn max(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         self.array.reduce("max")
//     }
// }

impl<T: Dist> private::ArrayExecAm<T> for AtomicArray<T> {
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team().clone()
    }
    fn team_counters(&self) ->Arc<AMCounters>{
        self.array.team_counters()
    }
}

impl<T: Dist> private::LamellarArrayPrivate<T> for AtomicArray<T> {
    fn local_as_ptr(&self) -> *const T {
        self.array.local_as_mut_ptr()
    }
    fn local_as_mut_ptr(&self) -> *mut T {
        self.array.local_as_mut_ptr()
    }
    fn pe_for_dist_index(&self, index: usize) -> Option<usize> {
        self.array.pe_for_dist_index(index)
    }
    fn pe_offset_for_dist_index(&self, pe: usize, index: usize) ->  Option<usize> {
        self.array.pe_offset_for_dist_index(pe, index)
    }
    unsafe fn into_inner(self) -> UnsafeArray<T>{
        self.array
    }
}

impl<T: Dist> LamellarArray<T> for AtomicArray<T> {
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
}
impl<T: Dist> LamellarWrite for AtomicArray<T> {}
impl<T: Dist> LamellarRead for AtomicArray<T> {}

impl<T: Dist> SubArray<T> for AtomicArray<T> {
    type Array = AtomicArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array {
        self.sub_array(range).into()
    }
    fn global_index(&self, sub_index: usize) -> usize {
        self.array.global_index(sub_index)
    }
}

impl<T: Dist + std::fmt::Debug> AtomicArray<T> {
    pub fn print(&self) {
        self.array.print();
    }
}

impl<T: Dist + std::fmt::Debug> ArrayPrint<T> for AtomicArray<T> {
    fn print(&self) {
        self.array.print()
    }
}



// impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> LamellarArrayReduce<T>
//     for AtomicArray<T>
// {

//     fn get_reduction_op(&self, op: String) -> LamellarArcAm {
//         // unsafe {
//         REDUCE_OPS
//             .get(&(std::any::TypeId::of::<T>(), op))
//             .expect("unexpected reduction type")(
//             self.clone().to_base_inner::<u8>().into(),
//             self.inner.team.num_pes(),
//         )
//         // }
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

// impl<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> IntoIterator
//     for &'a AtomicArray<T>
// {
//     type Item = &'a T;
//     type IntoIter = SerialIteratorIter<LamellarArrayIter<'a, T>>;
//     fn into_iter(self) -> Self::IntoIter {
//         SerialIteratorIter {
//             iter: self.ser_iter(),
//         }
//     }
// }

// impl < T> Drop for AtomicArray<T>{
//     fn drop(&mut self){
//         println!("dropping array!!!");
//     }
// }

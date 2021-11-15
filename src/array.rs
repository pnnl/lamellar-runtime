use crate::{active_messaging::*, LamellarTeamRT}; //{ActiveMessaging,AMCounters,Cmd,Msg,LamellarAny,LamellarLocal};
                                                  // use crate::lamellae::Lamellae;
                                                  // use crate::lamellar_arch::LamellarArchRT;
use crate::lamellar_request::LamellarRequest;
use crate::memregion::{
    local::LocalMemoryRegion, shared::SharedMemoryRegion, Dist, LamellarMemoryRegion,
};

use enum_dispatch::enum_dispatch;
use futures_lite::Future;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) mod r#unsafe;
pub use r#unsafe::UnsafeArray;
pub(crate) mod read_only;
pub use read_only::ReadOnlyArray;

pub mod iterator;
pub use iterator::distributed_iterator::DistributedIterator;
// use iterator::distributed_iterator::{DistIter, DistIterMut, DistIteratorLauncher};
// use iterator::serial_iterator::LamellarArrayIter;
pub use iterator::serial_iterator::{SerialIterator, SerialIteratorIter};

pub(crate) type ReduceGen =
    fn(LamellarReadArray<u8>, usize) -> Arc<dyn RemoteActiveMessage + Send + Sync>;

lazy_static! {
    pub(crate) static ref REDUCE_OPS: HashMap<(std::any::TypeId, String), ReduceGen> = {
        let mut temp = HashMap::new();
        for reduction_type in crate::inventory::iter::<ReduceKey> {
            temp.insert(
                (reduction_type.id.clone(), reduction_type.name.clone()),
                reduction_type.gen,
            );
        }
        temp
    };
}

pub struct ReduceKey {
    pub id: std::any::TypeId,
    pub name: String,
    pub gen: ReduceGen,
}
crate::inventory::collect!(ReduceKey);

lamellar_impl::generate_reductions_for_type_rt!(u8, u16, u32, u64, u128, usize);
lamellar_impl::generate_reductions_for_type_rt!(i8, i16, i32, i64, i128, isize);
lamellar_impl::generate_reductions_for_type_rt!(f32, f64);



#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug)]
pub enum Distribution {
    Block,
    Cyclic,
}

// pub enum Array {
//     Unsafe,
// }

#[derive(Hash, std::cmp::PartialEq, std::cmp::Eq, Clone)]
pub enum ArrayOp {
    Put,
    Get,
    Add,
}
#[derive(serde::Serialize, serde::Deserialize, Clone)]
enum ArrayOpInput {
    Add(usize, Vec<u8>),
}

#[enum_dispatch(RegisteredMemoryRegion<T>, SubRegion<T>, MyFrom<T>)]
#[derive(Clone)]
pub enum LamellarArrayInput<T: Dist + Clone + 'static> {
    LamellarMemRegion(LamellarMemoryRegion<T>),
    SharedMemRegion(SharedMemoryRegion<T>),
    LocalMemRegion(LocalMemoryRegion<T>),
    // Unsafe(UnsafeArray<T>),
    // Vec(Vec<T>),
}

impl<T: Dist + Clone + 'static> MyFrom<&T> for LamellarArrayInput<T> {
    fn my_from(val: &T, team: &Arc<LamellarTeamRT>) -> Self {
        let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(1);
        unsafe {
            buf.as_mut_slice().unwrap()[0] = val.clone();
        }
        LamellarArrayInput::LocalMemRegion(buf)
    }
}

impl<T: Dist + Clone + 'static> MyFrom<T> for LamellarArrayInput<T> {
    fn my_from(val: T, team: &Arc<LamellarTeamRT>) -> Self {
        let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(1);
        unsafe {
            buf.as_mut_slice().unwrap()[0] = val;
        }
        LamellarArrayInput::LocalMemRegion(buf)
    }
}

// impl<T: Dist + Clone + 'static> MyFrom<T> for LamellarArrayInput<T> {
//     fn my_from(val: T, team: &Arc<LamellarTeamRT>) -> Self {
//         let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(1);
//         unsafe {
//             buf.as_mut_slice().unwrap()[0] = val;
//         }
//         LamellarArrayInput::LocalMemRegion(buf)
//     }
// }

pub trait MyFrom<T: ?Sized> {
    fn my_from(val: T, team: &Arc<LamellarTeamRT>) -> Self;
}

pub trait MyInto<T: ?Sized> {
    fn my_into(self, team: &Arc<LamellarTeamRT>) -> T;
}

impl<T, U> MyInto<U> for T
where
    U: MyFrom<T>,
{
    fn my_into(self, team: &Arc<LamellarTeamRT>) -> U {
        U::my_from(self, team)
    }
}

pub trait ArrayOps<T> {
    fn add(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>>;
}

#[enum_dispatch]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(bound = "T: Dist + Clone + 'static")]
pub enum LamellarReadArray<T: Dist + Clone + 'static> {
    UnsafeArray(UnsafeArray<T>),
    ReadOnlyArray(ReadOnlyArray<T>)
}

#[enum_dispatch]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(bound = "T: Dist + Clone + 'static")]
pub enum LamellarWriteArray<T: Dist + Clone + 'static> {
    UnsafeArray(UnsafeArray<T>)
}

#[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
pub trait LamellarArray<T: Dist + Clone + 'static>: Sync + Send + Clone{
    fn team(&self) -> Arc<LamellarTeamRT>;
    fn local_as_ptr(&self) -> *const T;
    fn local_as_mut_ptr(&self) -> *mut T;
    fn num_elems_local(&self) -> usize;
    fn len(&self) -> usize;
    fn barrier(&self);
    fn wait_all(&self);
    // /// Returns a distributed iterator for the LamellarArray
    // /// must be called accross all pes containing data in the array
    // /// iteration on a pe only occurs on the data which is locally present
    // /// with all pes iterating concurrently
    // /// blocking: true
    // pub fn dist_iter(&self) -> DistIter<'static, T>;

    // /// Returns a distributed iterator for the LamellarArray
    // /// must be called accross all pes containing data in the array
    // /// iteration on a pe only occurs on the data which is locally present
    // /// with all pes iterating concurrently
    // pub fn dist_iter_mut(&self) -> DistIterMut<'static, T>;

    // /// Returns an iterator for the LamellarArray, all iteration occurs on the PE
    // /// where this was called, data that is not local to the PE is automatically
    // /// copied and transferred
    // pub fn ser_iter(&self) -> LamellarArrayIter<'_, T> ;

    // /// Returns an iterator for the LamellarArray, all iteration occurs on the PE
    // /// where this was called, data that is not local to the PE is automatically
    // /// copied and transferred, array data is buffered to more efficiently make
    // /// use of network buffers
    // pub fn buffered_iter(&self, buf_size: usize) -> LamellarArrayIter<'_, T> ;
}

pub trait SubArray<T: Dist + Clone + 'static>: LamellarArray<T> {
    type Array: LamellarArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) ->Self::Array;
}

#[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
pub trait LamellarArrayRead<T: Dist + Clone + 'static>: LamellarArray<T>
{
    fn get<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U);
    fn at(&self, index: usize) -> T;
}

#[enum_dispatch(LamellarWriteArray<T>)]
pub trait LamellarArrayWrite<T: Dist + Clone + 'static>: LamellarArray<T>
{
    fn put<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U); 
}

pub trait LamellarArrayReduce<T>: LamellarArrayRead<T>
where
    T: Dist + Clone + 'static,
{
    fn get_reduction_op(&self, op: String) -> LamellarArcAm;
    fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
    fn sum(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
    fn max(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
    fn prod(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
}


// impl<'a, T: Dist  + 'static> IntoIterator
//     for &'a LamellarArray<T>
// {
//     type Item = &'a T;
//     type IntoIter = SerialIteratorIter<LamellarArrayIter<'a, T>>;
//     fn into_iter(self) -> Self::IntoIter {
//         SerialIteratorIter {
//             iter: self.ser_iter(),
//         }
//     }
// }

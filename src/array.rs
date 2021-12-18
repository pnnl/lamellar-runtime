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
use std::pin::Pin;
use std::sync::Arc;

pub(crate) mod r#unsafe;
pub use r#unsafe::{operations::UnsafeArrayAdd, UnsafeArray};
pub(crate) mod read_only;
pub use read_only::ReadOnlyArray;
pub(crate) mod local_only;
pub use local_only::LocalOnlyArray;
pub(crate) mod atomic;
pub use atomic::{AtomicArray, AtomicArrayAdd, AtomicOps};

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

#[derive(Hash, std::cmp::PartialEq, std::cmp::Eq, Clone)]
pub enum ArrayOpCmd {
    Put,
    PutAm,
    Get(bool), //bool true == immediate, false = async
    GetAm,
}

pub trait ArrayOp {} //essentially a marker trait than signifys a type has been registered for distributed ArrayOps

pub trait ArrayOps<T: Dist + std::ops::AddAssign> {
    fn add(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>>;
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
enum ArrayOpInput {
    Add(usize, Vec<u8>),
}

pub trait ArrayAdd<T: Dist + std::ops::AddAssign> {
    fn dist_add(
        &self,
        index: usize,
        func: LamellarArcAm,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync>;
    fn local_add(&self, index: usize, val: T);
}

#[enum_dispatch(RegisteredMemoryRegion<T>, SubRegion<T>, MyFrom<T>,MemoryRegionRDMA<T>,AsBase)]
#[derive(Clone)]
pub enum LamellarArrayInput<T: Dist> {
    LamellarMemRegion(LamellarMemoryRegion<T>),
    SharedMemRegion(SharedMemoryRegion<T>), //when used as input/output we are only using the local data
    LocalMemRegion(LocalMemoryRegion<T>),
    // Unsafe(UnsafeArray<T>),
    // Vec(Vec<T>),
}

pub trait LamellarWrite {}
pub trait LamellarRead {}

impl<T: Dist> LamellarRead for T {}

impl<T: Dist> MyFrom<&T> for LamellarArrayInput<T> {
    fn my_from(val: &T, team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(1);
        unsafe {
            buf.as_mut_slice().unwrap()[0] = val.clone();
        }
        LamellarArrayInput::LocalMemRegion(buf)
    }
}

impl<T: Dist> MyFrom<T> for LamellarArrayInput<T> {
    fn my_from(val: T, team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(1);
        unsafe {
            buf.as_mut_slice().unwrap()[0] = val;
        }
        LamellarArrayInput::LocalMemRegion(buf)
    }
}

// impl<T: AmDist+ Clone + 'static> MyFrom<T> for LamellarArrayInput<T> {
//     fn my_from(val: T, team: &Arc<LamellarTeamRT>) -> Self {
//         let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(1);
//         unsafe {
//             buf.as_mut_slice().unwrap()[0] = val;
//         }
//         LamellarArrayInput::LocalMemRegion(buf)
//     }
// }

pub trait MyFrom<T: ?Sized> {
    fn my_from(val: T, team: &Pin<Arc<LamellarTeamRT>>) -> Self;
}

pub trait MyInto<T: ?Sized> {
    fn my_into(self, team: &Pin<Arc<LamellarTeamRT>>) -> T;
}

impl<T, U> MyInto<U> for T
where
    U: MyFrom<T>,
{
    fn my_into(self, team: &Pin<Arc<LamellarTeamRT>>) -> U {
        U::my_from(self, team)
    }
}

impl<T: Dist> MyFrom<&LamellarArrayInput<T>> for LamellarArrayInput<T> {
    fn my_from(lai: &LamellarArrayInput<T>, _team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        lai.clone()
    }
}

#[enum_dispatch]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(bound = "T: Dist + serde::Serialize + serde::de::DeserializeOwned")]
pub enum LamellarReadArray<T: Dist> {
    UnsafeArray(UnsafeArray<T>),
    ReadOnlyArray(ReadOnlyArray<T>),
    AtomicArray(AtomicArray<T>),
}

#[enum_dispatch]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(bound = "T: Dist + serde::Serialize + serde::de::DeserializeOwned")]
pub enum LamellarWriteArray<T: Dist> {
    UnsafeArray(UnsafeArray<T>),
    AtomicArray(AtomicArray<T>),
}

pub(crate) mod private {
    use crate::array::{
        AtomicArray, LamellarReadArray, LamellarWriteArray, ReadOnlyArray, UnsafeArray,
    };
    use crate::memregion::Dist;
    use enum_dispatch::enum_dispatch;
    #[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
    pub trait LamellarArrayPrivate<T: Dist> {
        // fn my_pe(&self) -> usize;
        fn local_as_ptr(&self) -> *const T;
        fn local_as_mut_ptr(&self) -> *mut T;
        fn pe_for_dist_index(&self, index: usize) -> usize;
        fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> usize;
    }
}

#[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
pub trait LamellarArray<T: Dist>: private::LamellarArrayPrivate<T> {
    fn team(&self) -> Pin<Arc<LamellarTeamRT>>;
    fn my_pe(&self) -> usize;
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

// pub trait ArrayIterator{

// }

pub trait SubArray<T: Dist>: LamellarArray<T> {
    type Array: LamellarArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array;
    fn global_index(&self, sub_index: usize) -> usize;
}

#[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
pub trait LamellarArrayRead<T: Dist>: LamellarArray<T> + Sync + Send {
    // this is non blocking call
    // the runtime does not manage checking for completion of message transmission
    // the user is responsible for ensuring the buffer remains valid
    unsafe fn get_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
        &self,
        index: usize,
        buf: U,
    );

    // a safe synchronous call that blocks untils the data as all been transfered
    fn iget<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U);

    // blocking call that gets the value stored and the provided index
    fn iat(&self, index: usize) -> T;

    // we also plan to implement safe asyncronous versions of iget and iat
    // the apis would be something like:
    // fn get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U) -> LamellarRequest<U>;
    // fn at(&self, index: usize) -> LamellarRequest<T>;
}

#[enum_dispatch(LamellarWriteArray<T>)]
pub trait LamellarArrayWrite<T: Dist>: LamellarArray<T> + Sync + Send {
    // non blocking put
    // runtime provides no mechansim for checking when the data has finished being written
    // buf can immediately be reused after this call returns
    // unsafe fn put_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U);
    // blocking ops
    // fn iput<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(&self, index: usize, buf: U);
    // fn iswap(&self, index: usize, val: T) -> T;
    //async ops
    // fn put<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(&self, index: usize, buf: U) -> LamellarRequest<()>;
    // fn swap(&self, index: usize, val: T) -> LamellarRequest<T>;
}

// #[enum_dispatch(LamellarArithmeticOps<T>)]
// pub trait LamellarArithmeticOps<T: Dist + std::ops::AddAssing + std::ops::SubAssing + std::ops::MulAssign + std::ops::DivAssign>: LamellarArrayWrite{
//     // blocking ops
//     fn iadd(&self, index: usize, val: T);
//     fn ifetch_add(&self, index: usize, val: T) ->T;
//     fn isub(&self, index: usize, val: T);
//     fn ifetch_sub(&self, index: usize, val: T) ->T;
//     fn imul(&self, index: usize, val: T);
//     fn ifetch_mul(&self, index: usize, val: T) ->T;
//     fn idiv(&self, index: usize, val: T);
//     fn ifetch_div(&self, index: usize, val: T) ->T;
//     //async ops
//     fn add(&self, index: usize, val: T) -> LamellarRequest<()>;
//     fn fetch_add(&self, index: usize, val: T) -> LamellarRequest<T>;
//     fn sub(&self, index: usize, val: T) -> LamellarRequest<()>;
//     fn fetch_sub(&self, index: usize, val: T) ->LamellarRequest<T>;
//     fn mul(&self, index: usize, val: T) -> LamellarRequest<()>;
//     fn fetch_mul(&self, index: usize, val: T) -> LamellarRequest<T>;
//     fn div(&self, index: usize, val: T) -> LamellarRequest<()>;
//     fn fetch_div(&self, index: usize, val: T) -> LamellarRequest<T>;
// }

// pub trait LamellarLocalOps<T: Dist + Add + Sub + Mul + Div>: LamellarArithmeticOps{
//     fn local_add(&self, index: usize, val: T) -> T;
//     ...

// pub trait LamellarRemoteOps<T: Dist + Add + Sub + Mul + Div>: LamellarArithmeticOps{
//     fn remote_add(&self, index: usize, val: T) -> LamellarRequest<T>;
//     ...
// }

pub trait ArrayPrint<T: Dist + std::fmt::Debug>: LamellarArray<T> {
    fn print(&self);
}

pub trait LamellarArrayReduce<T>: LamellarArrayRead<T>
where
    T: Dist + serde::Serialize + serde::de::DeserializeOwned,
{
    fn get_reduction_op(&self, op: String) -> LamellarArcAm;
    fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
    fn sum(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
    fn max(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
    fn prod(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
}

// impl<'a, T: AmDist + 'static> IntoIterator
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

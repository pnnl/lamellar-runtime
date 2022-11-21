//! LamellarArrays provide a safe and highlevel abstraction of a distributed array.
//! 
//! By distributed, we mean that the memory backing the array is physically located on multiple distributed PEs in the system.
//!
//! # Features
//!  - [Safety](#safety)
//!  - [Multiple array types](#multiple-array-types)
//!  - RDMA like `put` and `get` APIs 
//!  - Element Wise operations (e.g. `add`, `fetch_add`, `or`, `compare_exchange`, etc.)
//!  - Batched operations (`batch_add`, `batch_fetch_add`, etc.)
//!  - Distributed and Onesided Iteration (`dist_iter`, `dist_iter_mut`, `onesided_iter`, etc.)
//!  - Distributed Reductions (e.g. `reduce`),
//!  - [Block or Cyclic memory layouts](crate::array::Distribution)
//!  - [Sub Arrays](crate::array::SubArray)
//!  - [Type conversion](#type-conversion)
//!
//! # Safety
//! Array Data Lifetimes: LamellarArrays are built upon [Darcs][crate::darc::Darc] (Distributed Atomic Reference Counting Pointers) and as such have distributed lifetime management.
//! This means that as long as a single reference to an array exists anywhere in the distributed system, the data for the entire array will remain valid on every PE (even though a given PE may have dropped all its local references).
//! While the compiler handles lifetimes within the context of a single PE, our distributed lifetime management relies on "garbage collecting active messages" to ensure all remote references have been accounted for.  
//!
//! # Multiple array types
//! We provide several array types, each with their own saftey gaurantees with respect to how data is accessed (further detail can be found in the documentation for each type)
//!  - [UnsafeArray]: No safety gaurantees - PEs are free to read/write to anywhere in the array with no access control
//!  - [ReadOnlyArray]: No write access is permitted, and thus PEs are free to read from anywhere in the array with no access control
//!  - [AtomicArray]: Each Element is atomic (either instrisically or enforced via the runtime)
//!      - NativeAtomicArray: utilizes the language atomic types e.g AtomicUsize, AtomicI8, etc.
//!      - GenericAtomicArray: Each element is protected by a 1-byte mutex
//!  - [LocalLockArray]: The data on each PE is protected by a local RwLock
//! 
//! # Type conversion
//! We offer a variety of methods to convert between array types.
//! - `into_atomic`, `into_read_only`, etc., convert between disributed array types.
//! - `collect` and `collect_async` provide functionality analogous to the [collect](https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.collect) method for Rust iterators
//! - Some types are specially designed to serve as inputs to batched operations.
//!   Conversion to these types is supported by a variety of functions:
//!   `local_data`, `read_local_data`, `write_local_data`, etc. convert to slices and other data types.  See [OpInput](crate::array::OpInput) for details.
use crate::lamellar_request::LamellarRequest;
use crate::memregion::{
    one_sided::OneSidedMemoryRegion,
    shared::SharedMemoryRegion,
    Dist,
    LamellarMemoryRegion,
    // RemoteMemoryRegion,
};
use crate::{active_messaging::*, LamellarTeamRT};
// use crate::Darc;
use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
use futures_lite::Future;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

// use serde::de::DeserializeOwned;

/// This macro automatically derives various LamellarArray "Op" traits for user defined types
/// 
/// The following "Op" traits will be implemented:
/// - [AccessOps][crate::array::operations::AccessOps]
/// - [ArithmeticOps][crate::array::operations::ArithmeticOps]
/// - [BitWiseOps][crate::array::operations::BitWiseOps]
/// - [CompareExchangeEpsilonOps][crate::array::operations::CompareExchangeEpsilonOps]
/// - [CompareExchangeOps][crate::array::operations::CompareExchangeOps]
/// 
/// The required trait bounds can be found by viewing each "Op" traits documentation.
pub use lamellar_impl::ArrayOps;

pub mod prelude;

pub(crate) mod r#unsafe;
pub use r#unsafe::{
    operations::UnsafeArrayOpBuf, UnsafeArray, UnsafeByteArray, UnsafeByteArrayWeak,
};
pub(crate) mod read_only;
pub use read_only::{ReadOnlyArray, ReadOnlyArrayOpBuf, ReadOnlyByteArray, ReadOnlyByteArrayWeak};

// pub(crate) mod local_only;
// pub use local_only::LocalOnlyArray;

pub(crate) mod atomic;
pub use atomic::{
    // operations::{AtomicArrayOp, AtomicArrayOpBuf},
    AtomicArray,
    AtomicByteArray, //AtomicOps
    AtomicByteArrayWeak,
    AtomicLocalData,
};

pub(crate) mod generic_atomic;
pub use generic_atomic::{
    operations::GenericAtomicArrayOpBuf, GenericAtomicArray, GenericAtomicByteArray,
    GenericAtomicByteArrayWeak, GenericAtomicLocalData,
};

pub(crate) mod native_atomic;
pub use native_atomic::{
    operations::NativeAtomicArrayOpBuf, NativeAtomicArray, NativeAtomicByteArray,
    NativeAtomicByteArrayWeak, NativeAtomicLocalData,
};

pub(crate) mod local_lock_atomic;
pub use local_lock_atomic::{
    operations::LocalLockArrayOpBuf, LocalLockArray, LocalLockByteArray,
    LocalLockByteArrayWeak, LocalLockLocalData,
};

pub mod iterator;
pub use iterator::distributed_iterator::DistributedIterator;
pub use iterator::local_iterator::LocalIterator;
pub use iterator::one_sided_iterator::OneSidedIterator;

pub(crate) mod operations;
pub use operations::*;

pub(crate) type ReduceGen = fn(LamellarByteArray, usize) -> LamellarArcAm;

lazy_static! {
    pub(crate) static ref REDUCE_OPS: HashMap<(std::any::TypeId, &'static str), ReduceGen> = {
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

#[doc(hidden)]
pub struct ReduceKey {
    pub id: std::any::TypeId,
    pub name: &'static str,
    pub gen: ReduceGen,
}
crate::inventory::collect!(ReduceKey);

// lamellar_impl::generate_reductions_for_type_rt!(true, u8,usize);
// lamellar_impl::generate_ops_for_type_rt!(true, true, u8,usize);
impl Dist for bool {}

lamellar_impl::generate_reductions_for_type_rt!(true, u8, u16, u32, u64, usize);
lamellar_impl::generate_reductions_for_type_rt!(false, u128);
lamellar_impl::generate_ops_for_type_rt!(true, true, u8, u16, u32, u64, usize);
lamellar_impl::generate_ops_for_type_rt!(true, false, u128);

lamellar_impl::generate_reductions_for_type_rt!(true, i8, i16, i32, i64, isize);
lamellar_impl::generate_reductions_for_type_rt!(false, i128);
lamellar_impl::generate_ops_for_type_rt!(true, true, i8, i16, i32, i64, isize);
lamellar_impl::generate_ops_for_type_rt!(true, false, i128);

lamellar_impl::generate_reductions_for_type_rt!(false, f32, f64);
lamellar_impl::generate_ops_for_type_rt!(false, false, f32, f64);

/// Specifies the distributed data layout of a LamellarArray
///
/// Block: The indicies of the elements on each PE are sequential
///
/// Cyclic: The indicies of the elements on each PE have a stride equal to the number of PEs associated with the array
///
/// # Examples
/// assume we have 4 PEs
/// ## Block
///```
/// let block_array = LamellarArray::new(world,12,Distribution::Block);
/// block array index location  = PE0 [0,1,2,3],  PE1 [4,5,6,7],  PE2 [8,9,10,11], PE3 [12,13,14,15]
///```
/// ## Cyclic
///```
/// let cyclic_array = LamellarArray::new(world,12,Distribution::Cyclic);
/// cyclic array index location = PE0 [0,4,8,12], PE1 [1,5,9,13], PE2 [2,6,10,14], PE3 [3,7,11,15]
///```
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
pub enum Distribution {
    Block,
    Cyclic,
}

#[doc(hidden)]
#[derive(Hash, std::cmp::PartialEq, std::cmp::Eq, Clone)]
pub enum ArrayRdmaCmd {
    Put,
    PutAm,
    Get(bool), //bool true == immediate, false = async
    GetAm,
}

#[doc(hidden)]
#[async_trait]
pub trait LamellarArrayRequest: Sync + Send {
    type Output;
    async fn into_future(mut self: Box<Self>) -> Self::Output;
    fn wait(self: Box<Self>) -> Self::Output;
}

struct ArrayRdmaHandle {
    reqs: Vec<Box<dyn LamellarRequest<Output = ()>>>,
}
#[async_trait]
impl LamellarArrayRequest for ArrayRdmaHandle {
    type Output = ();
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        for req in self.reqs.drain(0..) {
            req.into_future().await;
        }
        ()
    }
    fn wait(mut self: Box<Self>) -> Self::Output {
        for req in self.reqs.drain(0..) {
            req.get();
        }
        ()
    }
}

struct ArrayRdmaAtHandle<T: Dist> {
    reqs: Vec<Box<dyn LamellarRequest<Output = ()>>>,
    buf: OneSidedMemoryRegion<T>,
}
#[async_trait]
impl<T: Dist> LamellarArrayRequest for ArrayRdmaAtHandle<T> {
    type Output = T;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        for req in self.reqs.drain(0..) {
            req.into_future().await;
        }
        unsafe { self.buf.as_slice().unwrap()[0] }
    }
    fn wait(mut self: Box<Self>) -> Self::Output {
        for req in self.reqs.drain(0..) {
            req.get();
        }
        unsafe { self.buf.as_slice().unwrap()[0] }
    }
}

#[enum_dispatch(RegisteredMemoryRegion<T>, SubRegion<T>, MyFrom<T>,MemoryRegionRDMA<T>,AsBase)]
#[derive(Clone, Debug)]
pub enum LamellarArrayInput<T: Dist> {
    LamellarMemRegion(LamellarMemoryRegion<T>),
    SharedMemRegion(SharedMemoryRegion<T>), //when used as input/output we are only using the local data
    LocalMemRegion(OneSidedMemoryRegion<T>),
    // UnsafeArray(UnsafeArray<T>),
}

#[doc(hidden)]
pub trait LamellarWrite {}

#[doc(hidden)]
pub trait LamellarRead {}

impl<T: Dist> LamellarRead for T {}

impl<T: Dist> MyFrom<&T> for LamellarArrayInput<T> {
    fn my_from(val: &T, team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let buf: OneSidedMemoryRegion<T> = team.alloc_one_sided_mem_region(1);
        unsafe {
            buf.as_mut_slice().unwrap()[0] = val.clone();
        }
        LamellarArrayInput::LocalMemRegion(buf)
    }
}

impl<T: Dist> MyFrom<T> for LamellarArrayInput<T> {
    fn my_from(val: T, team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let buf: OneSidedMemoryRegion<T> = team.alloc_one_sided_mem_region(1);
        unsafe {
            buf.as_mut_slice().unwrap()[0] = val;
        }
        LamellarArrayInput::LocalMemRegion(buf)
    }
}

impl<T: Dist> MyFrom<Vec<T>> for LamellarArrayInput<T> {
    fn my_from(vals: Vec<T>, team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let buf: OneSidedMemoryRegion<T> = team.alloc_one_sided_mem_region(vals.len());
        unsafe {
            std::ptr::copy_nonoverlapping(vals.as_ptr(), buf.as_mut_ptr().unwrap(), vals.len());
        }
        LamellarArrayInput::LocalMemRegion(buf)
    }
}
impl<T: Dist> MyFrom<&Vec<T>> for LamellarArrayInput<T> {
    fn my_from(vals: &Vec<T>, team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let buf: OneSidedMemoryRegion<T> = team.alloc_one_sided_mem_region(vals.len());
        unsafe {
            std::ptr::copy_nonoverlapping(vals.as_ptr(), buf.as_mut_ptr().unwrap(), vals.len());
        }
        LamellarArrayInput::LocalMemRegion(buf)
    }
}

// impl<T: AmDist+ Clone + 'static> MyFrom<T> for LamellarArrayInput<T> {
//     fn my_from(val: T, team: &Arc<LamellarTeamRT>) -> Self {
//         let buf: OneSidedMemoryRegion<T> = team.alloc_one_sided_mem_region(1);
//         unsafe {
//             buf.as_mut_slice().unwrap()[0] = val;
//         }
//         LamellarArrayInput::LocalMemRegion(buf)
//     }
// }

#[doc(hidden)]
pub trait MyFrom<T: ?Sized> {
    fn my_from(val: T, team: &Pin<Arc<LamellarTeamRT>>) -> Self;
}

#[doc(hidden)]
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

/// Represents the array types that allow Read operations
#[enum_dispatch]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(bound = "T: Dist + serde::Serialize + serde::de::DeserializeOwned + 'static")]
pub enum LamellarReadArray<T: Dist + 'static> {
    UnsafeArray(UnsafeArray<T>),
    ReadOnlyArray(ReadOnlyArray<T>),
    AtomicArray(AtomicArray<T>),
    LocalLockArray(LocalLockArray<T>),
}

#[doc(hidden)]
#[enum_dispatch]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum LamellarByteArray {
    //we intentially do not include "byte" in the variant name to ease construciton in the proc macros
    UnsafeArray(UnsafeByteArray),
    ReadOnlyArray(ReadOnlyByteArray),
    AtomicArray(AtomicByteArray),
    NativeAtomicArray(NativeAtomicByteArray),
    GenericAtomicArray(GenericAtomicByteArray),
    LocalLockArray(LocalLockByteArray),
}

impl<T: Dist + 'static> crate::active_messaging::DarcSerde for LamellarReadArray<T> {
    fn ser(&self, num_pes: usize) {
        // println!("in shared ser");
        match self {
            LamellarReadArray::UnsafeArray(array) => array.ser(num_pes),
            LamellarReadArray::ReadOnlyArray(array) => array.ser(num_pes),
            LamellarReadArray::AtomicArray(array) => array.ser(num_pes),
            LamellarReadArray::LocalLockArray(array) => array.ser(num_pes),
        }
    }
    fn des(&self, cur_pe: Result<usize, crate::IdError>) {
        // println!("in shared des");
        match self {
            LamellarReadArray::UnsafeArray(array) => array.des(cur_pe),
            LamellarReadArray::ReadOnlyArray(array) => array.des(cur_pe),
            LamellarReadArray::AtomicArray(array) => array.des(cur_pe),
            LamellarReadArray::LocalLockArray(array) => array.des(cur_pe),
        }
    }
}


/// Represents the array types that allow write  operations
#[enum_dispatch]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(bound = "T: Dist + serde::Serialize + serde::de::DeserializeOwned")]
pub enum LamellarWriteArray<T: Dist> {
    UnsafeArray(UnsafeArray<T>),
    AtomicArray(AtomicArray<T>),
    LocalLockArray(LocalLockArray<T>),
}

impl<T: Dist + 'static> crate::active_messaging::DarcSerde for LamellarWriteArray<T> {
    fn ser(&self, num_pes: usize) {
        // println!("in shared ser");
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.ser(num_pes),
            LamellarWriteArray::AtomicArray(array) => array.ser(num_pes),
            LamellarWriteArray::LocalLockArray(array) => array.ser(num_pes),
        }
    }
    fn des(&self, cur_pe: Result<usize, crate::IdError>) {
        // println!("in shared des");
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.des(cur_pe),
            LamellarWriteArray::AtomicArray(array) => array.des(cur_pe),
            LamellarWriteArray::LocalLockArray(array) => array.des(cur_pe),
        }
    }
}

pub(crate) mod private {
    use crate::active_messaging::*;
    use crate::array::{
        AtomicArray, /*NativeAtomicArray, GenericAtomicArray,*/ LamellarReadArray,
        LamellarWriteArray, LocalLockArray, ReadOnlyArray, UnsafeArray,
    };
    use crate::lamellar_request::{LamellarMultiRequest, LamellarRequest};
    use crate::memregion::Dist;
    use crate::LamellarTeamRT;
    use enum_dispatch::enum_dispatch;
    use std::pin::Pin;
    use std::sync::Arc;
    #[doc(hidden)]
    #[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
    pub trait LamellarArrayPrivate<T: Dist> {
        // // fn my_pe(&self) -> usize;
        fn inner_array(&self) -> &UnsafeArray<T>;
        fn local_as_ptr(&self) -> *const T;
        fn local_as_mut_ptr(&self) -> *mut T;
        fn pe_for_dist_index(&self, index: usize) -> Option<usize>;
        fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> Option<usize>;
        unsafe fn into_inner(self) -> UnsafeArray<T>;
    }

    #[doc(hidden)]
    #[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
    pub(crate) trait ArrayExecAm<T: Dist> {
        fn team(&self) -> Pin<Arc<LamellarTeamRT>>;
        fn team_counters(&self) -> Arc<AMCounters>;
        fn exec_am_local<F>(&self, am: F) -> Box<dyn LamellarRequest<Output = F::Output>>
        where
            F: LamellarActiveMessage + LocalAM + 'static,
        {
            self.team().exec_am_local_tg(am, Some(self.team_counters()))
        }
        fn exec_am_pe<F>(&self, pe: usize, am: F) -> Box<dyn LamellarRequest<Output = F::Output>>
        where
            F: RemoteActiveMessage + LamellarAM + AmDist,
        {
            self.team()
                .exec_am_pe_tg(pe, am, Some(self.team_counters()))
        }
        fn exec_arc_am_pe<F>(
            &self,
            pe: usize,
            am: LamellarArcAm,
        ) -> Box<dyn LamellarRequest<Output = F>>
        where
            F: AmDist,
        {
            self.team()
                .exec_arc_am_pe(pe, am, Some(self.team_counters()))
        }
        fn exec_am_all<F>(&self, am: F) -> Box<dyn LamellarMultiRequest<Output = F::Output>>
        where
            F: RemoteActiveMessage + LamellarAM + AmDist,
        {
            self.team().exec_am_all_tg(am, Some(self.team_counters()))
        }
    }
}

/// Represents a distributed array, providing some convenience functions for getting simple information about the array
/// This is intended for use within the runtime, but needs to be public due to its use in Proc Macros
#[doc(hidden)]
#[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
pub trait LamellarArray<T: Dist>: private::LamellarArrayPrivate<T> {
    /// Returns the team used to construct this array, the PEs in the team represent the same PEs which have a slice of data of the array
    fn team(&self) -> Pin<Arc<LamellarTeamRT>>;
    /// Return the current PE of the calling thread
    fn my_pe(&self) -> usize;
    /// Return the number of elements of the array local to this PE
    fn num_elems_local(&self) -> usize;
    /// Return the total number of elements in the array
    fn len(&self) -> usize;

    /// Global synchronization method which blocks calling thread until all PEs in the owning Array data have entered the barrier
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder.build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic);
    /// 
    /// array.barrier();
    ///```
    fn barrier(&self);

    /// blocks calling thread until all remote tasks (e.g. element wise operations)
    /// initiated by the calling PE have completed.
    ///
    /// Note: this is not a distributed synchronization primitive (i.e. it has no knowledge of a Remote PEs tasks)
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder.build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// for i in 0..100{
    ///     array.add(i,1);
    /// }
    /// array.wait_all(); //block until the previous add operations have finished
    ///```
    fn wait_all(&self);

    /// Run a future to completion on the current thread
    ///
    /// This function will block the caller until the given future has completed, the future is executed within the Lamellar threadpool
    ///
    /// Users can await any future, including those returned from lamellar remote operations
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder.build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let request = array.fetch_add(10,1000); //fetch index 10 and add 1000 to it 
    /// let result = array.block_on(request); //block until am has executed
    /// // we also could have used world.block_on() or team.block_on()
    ///```
    fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future;

    /// given a global index, calculate the PE and offset on that PE where the element actually resides.
    /// Returns None if the index is Out of bounds
    fn pe_and_offset_for_global_index(&self, index: usize) -> Option<(usize, usize)>;

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
    // pub fn onesided_iter(&self) -> OneSidedIter<'_, T> ;

    // /// Returns an iterator for the LamellarArray, all iteration occurs on the PE
    // /// where this was called, data that is not local to the PE is automatically
    // /// copied and transferred, array data is buffered to more efficiently make
    // /// use of network buffers
    // pub fn buffered_onesided_iter(&self, buf_size: usize) -> OneSidedIter<'_, T> ;
}


/// Sub arrays are contiguous subsets of the elements of an array.
///
/// A sub array increments the parent arrays reference count, so the same lifetime guarantees apply to the subarray
/// 
/// There can exist mutliple subarrays to the same parent array and creating sub arrays are onesided operations
pub trait SubArray<T: Dist>: LamellarArray<T> {
    type Array: LamellarArray<T>;
    /// Given a range of indices, construct a sub array representing the elements in that range
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array;
    
    /// Convert a sub array based index into the index space of the original array
    fn global_index(&self, sub_index: usize) -> usize;
}

#[doc(hidden)]
#[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
pub trait LamellarArrayGet<T: Dist + 'static>: LamellarArray<T> {

    // async get
    // get data from self and write into buf
    fn get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
        &self,
        index: usize,
        dst: U,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;

    // blocking call that gets the value stored and the provided index
    fn at(&self, index: usize) -> Pin<Box<dyn Future<Output = T> + Send>>;
}

#[doc(hidden)]
#[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
pub trait LamellarArrayInternalGet<T: Dist + 'static>: LamellarArray<T> {
    fn internal_get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
        &self,
        index: usize,
        dst: U,
    ) -> Box<dyn LamellarArrayRequest<Output = ()>>;

    // blocking call that gets the value stored and the provided index
    fn internal_at(&self, index: usize) -> Box<dyn LamellarArrayRequest<Output = T>>;
}

#[doc(hidden)]
#[enum_dispatch(LamellarWriteArray<T>)]
pub trait LamellarArrayPut<T: Dist>: LamellarArray<T> {
    //put data from buf into self
    fn put<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(
        &self,
        index: usize,
        src: U,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

#[doc(hidden)]
#[enum_dispatch(LamellarWriteArray<T>)]
pub(crate) trait LamellarArrayInternalPut<T: Dist>: LamellarArray<T> {
    //put data from buf into self
    fn internal_put<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(
        &self,
        index: usize,
        src: U,
    ) -> Box<dyn LamellarArrayRequest<Output = ()>>;
}

#[doc(hidden)]
pub trait ArrayPrint<T: Dist + std::fmt::Debug>: LamellarArray<T> {
    fn print(&self);
}

// #[enum_dispatch(LamellarWriteArray<T>,LamellarReadArray<T>)]
pub trait LamellarArrayReduce<T>: LamellarArrayGet<T>
where
    T: Dist + AmDist + 'static,
{
    fn get_reduction_op(&self, op: &str) -> LamellarArcAm;
    fn reduce(&self, op: &str) -> Pin<Box<dyn Future<Output = T>>>;
    fn sum(&self) -> Pin<Box<dyn Future<Output = T>>>;
    fn max(&self) -> Pin<Box<dyn Future<Output = T>>>;
    fn prod(&self) -> Pin<Box<dyn Future<Output = T>>>;
}

impl<T: Dist + AmDist + 'static> LamellarWriteArray<T> {
    pub fn reduce(&self, op: &str) -> Pin<Box<dyn Future<Output = T>>> {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.reduce(op),
            LamellarWriteArray::AtomicArray(array) => array.reduce(op),
            LamellarWriteArray::LocalLockArray(array) => array.reduce(op),
        }
    }
    pub fn sum(&self) -> Pin<Box<dyn Future<Output = T>>> {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.sum(),
            LamellarWriteArray::AtomicArray(array) => array.sum(),
            LamellarWriteArray::LocalLockArray(array) => array.sum(),
        }
    }
    pub fn max(&self) -> Pin<Box<dyn Future<Output = T>>> {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.max(),
            LamellarWriteArray::AtomicArray(array) => array.max(),
            LamellarWriteArray::LocalLockArray(array) => array.max(),
        }
    }
    pub fn prod(&self) -> Pin<Box<dyn Future<Output = T>>> {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.prod(),
            LamellarWriteArray::AtomicArray(array) => array.prod(),
            LamellarWriteArray::LocalLockArray(array) => array.prod(),
        }
    }
}

impl<T: Dist + AmDist + 'static> LamellarReadArray<T> {
    pub fn reduce(&self, op: &str) -> Pin<Box<dyn Future<Output = T>>> {
        match self {
            LamellarReadArray::UnsafeArray(array) => array.reduce(op),
            LamellarReadArray::AtomicArray(array) => array.reduce(op),
            LamellarReadArray::LocalLockArray(array) => array.reduce(op),
            LamellarReadArray::ReadOnlyArray(array) => array.reduce(op),
        }
    }
    pub fn sum(&self) -> Pin<Box<dyn Future<Output = T>>> {
        match self {
            LamellarReadArray::UnsafeArray(array) => array.sum(),
            LamellarReadArray::AtomicArray(array) => array.sum(),
            LamellarReadArray::LocalLockArray(array) => array.sum(),
            LamellarReadArray::ReadOnlyArray(array) => array.sum(),
        }
    }
    pub fn max(&self) -> Pin<Box<dyn Future<Output = T>>> {
        match self {
            LamellarReadArray::UnsafeArray(array) => array.max(),
            LamellarReadArray::AtomicArray(array) => array.max(),
            LamellarReadArray::LocalLockArray(array) => array.max(),
            LamellarReadArray::ReadOnlyArray(array) => array.max(),
        }
    }
    pub fn prod(&self) -> Pin<Box<dyn Future<Output = T>>> {
        match self {
            LamellarReadArray::UnsafeArray(array) => array.prod(),
            LamellarReadArray::AtomicArray(array) => array.prod(),
            LamellarReadArray::LocalLockArray(array) => array.prod(),
            LamellarReadArray::ReadOnlyArray(array) => array.prod(),
        }
    }
}

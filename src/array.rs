//! LamellarArrays provide a safe and high-level abstraction of a distributed array.
//!
//! By distributed, we mean that the memory backing the array is physically located on multiple distributed PEs in the system.
//!
//! # Features
//!
//! **Features**  include
//!  - [Safety](#safety)
//!  - [Multiple array types](#multiple-array-types)
//!  - RDMA like [put][crate::array::LamellarArrayPut] and  [get][crate::array::LamellarArrayGet] APIs
//!  - [Block][crate::array::Distribution::Block] or [Cyclic][crate::array::Distribution::Cyclic] data layouts
//!
//! **Tools to work with arrays** include
//!  - [Conversion](#type-conversion) between different array types and other data structures
//!  - Element-wise operations (e.g. [load/store][crate::array::AccessOps], [add][crate::array::ArithmeticOps], [fetch_and][crate::array::BitWiseOps], [compare_exchange][crate::array::CompareExchangeOps], etc)
//!  - Batched operations ([batch_add][crate::array::ArithmeticOps], [batch_fetch_add][crate::array::ArithmeticOps], etc.)
//!  - [Distributed][crate::array::iterator::distributed_iterator], [Local][crate::array::iterator::local_iterator], and [Onesided][crate::array::iterator::one_sided_iterator] Iteration
//!  - [Distributed Reductions][crate::array::LamellarArrayReduce]
//!  - [Sub Arrays][crate::array::SubArray]
//!
//! # Examples
//!
//! Lamellar provides a variety of [examples](https://github.com/pnnl/lamellar-runtime/tree/master/examples/array_examples) for common tasks, e.g. distributed iteration.
//!
//! # Safety
//! Array Data Lifetimes: LamellarArrays are built upon [Darcs][crate::darc::Darc] (Distributed Atomic Reference Counting Pointers) and as such have distributed lifetime management.
//! This means that as long as a single reference to an array exists anywhere in the distributed system, the data for the entire array will remain valid on every PE (even though a given PE may have dropped all its local references).
//! While the compiler handles lifetimes within the context of a single PE, our distributed lifetime management relies on "garbage collecting active messages" to ensure all remote references have been accounted for.  
//!
//! # Multiple array types
//! We provide several array types, each with their own saftey gaurantees with respect to how data is accessed (further details can be found in the documentation for each type)
//!  - [UnsafeArray]: No safety gaurantees - PEs are free to read/write to anywhere in the array with no access control
//!  - [ReadOnlyArray]: No write access is permitted, and thus PEs are free to read from anywhere in the array with no access control
//!  - [AtomicArray]: Each Element is atomic (either instrinsically or enforced via the runtime)
//!      - [NativeAtomicArray]: utilizes the language atomic types e.g AtomicUsize, AtomicI8, etc.
//!      - [GenericAtomicArray]: Each element is protected by a 1-byte mutex
//!  - [LocalLockArray]: The data on each PE is protected by a local RwLock
//!  - [GlobalLockArray]: The data on each PE is protected by a global RwLock
//!
//! # Type conversion
//! Lamellar offers a variety of methods to convert between different array types and other data structures.
//! - `into_atomic`, `into_read_only`, etc., convert between disributed array types.
//! - `collect` and `collect_async` provide functionality analogous to the [collect](https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.collect) method for Rust iterators
//! - We also provided access directly to the underlying local data of an array using functions (and container types) that preserve the safety guarantees of a given array type
//!     -`local_data`, `read_local_data`, `write_local_data`, etc. convert to slices and other data types.
//!     - Consequently, these functions can be used to create valid inputs for batched operations,  see [OpInput] for details.
//! ```
//! use lamellar::array::prelude::*;
//!
//! // define an length-10 array of type UnsafeArray<usize>
//! let world = LamellarWorldBuilder::new().build();
//! let array =  UnsafeArray::<usize>::new(&world, 10,Distribution::Block);
//!
//! // convert between array types    
//! let array = array.into_local_lock(); // LocalLockArray
//! let array = array.into_global_lock(); // GlobalLockArray
//! let array = array.into_atomic(); // AtomicArray
//! let array = array.into_read_only(); // ReadOnlyArray
//!
//! // get a reference to the underlying slice: &[usize]
//! let local_data = array.local_data();
//!
//! // export to Vec<usize>
//! let vec = array.local_data().to_vec();
//! ```
use crate::barrier::BarrierHandle;
use crate::lamellar_env::LamellarEnv;
use crate::memregion::{
    one_sided::OneSidedMemoryRegion,
    shared::SharedMemoryRegion,
    Dist,
    LamellarMemoryRegion,
    RegisteredMemoryRegion, // RemoteMemoryRegion,
};
use crate::scheduler::LamellarTask;
use crate::{active_messaging::*, LamellarTeam, LamellarTeamRT};

// use crate::Darc;
use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
use futures_util::Future;
// use parking_lot::Mutex;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;

// use serde::de::DeserializeOwned;

/// This macro automatically derives various LamellarArray "Op" traits for user defined types
///
/// The following "Op" traits are automatically implemented:
/// - [AccessOps]
/// - [ReadOnlyOps]
///
/// Additionally, it is possible to pass any of the following as a list to [ArrayOps] to derive the associated traits
/// - `Arithmetic` -- [ArithmeticOps]
///     - requires [AddAssign][std::ops::AddAssign], [SubAssign][std::ops::SubAssign], [MulAssign][std::ops::MulAssign], [DivAssign][std::ops::DivAssign], [RemAssign][std::ops::RemAssign] to be implemented on your data type
/// - `Bitwise` -- [BitWiseOps]
///     - requires [BitAndAssign][std::ops::BitAndAssign], [BitOrAssign][std::ops::BitOrAssign], [BitXorAssign][std::ops::BitXorAssign] to be implemented on your data type
/// - `CompEx` -- [CompareExchangeOps]
///     - requires [PartialEq], [PartialOrd] to be implemented on your data type
/// - `CompExEps` -- [CompareExchangeEpsilonOps]
///     - requires [PartialEq], [PartialOrd] to be implemented on your data type
/// - `Shift` -- [ShiftOps]
///     - requires [ShlAssign][std::ops::ShlAssign], [ShrAssign][std::ops::ShrAssign] to be implemented on you data type
///
/// Alternatively, if you plan to derive all the above traits you can simply supply `All` as the single argument to [ArrayOps]
pub use lamellar_impl::ArrayOps;

// //#[doc(hidden)]

/// The prelude contains all the traits and macros that are required to use the array types
pub mod prelude;

pub(crate) mod r#unsafe;
pub use r#unsafe::{
    local_chunks::{UnsafeLocalChunks, UnsafeLocalChunksMut},
    operations::{
        multi_val_multi_idx_ops, multi_val_single_idx_ops, single_val_multi_idx_ops,
        BatchReturnType,
    },
    UnsafeArray, UnsafeByteArray, UnsafeByteArrayWeak,
};
pub(crate) mod read_only;
pub use read_only::{ReadOnlyArray, ReadOnlyByteArray, ReadOnlyByteArrayWeak, ReadOnlyLocalChunks};

pub(crate) mod atomic;
pub use atomic::{AtomicArray, AtomicByteArray, AtomicByteArrayWeak, AtomicLocalData};

pub(crate) mod generic_atomic;
pub use generic_atomic::{
    GenericAtomicArray, GenericAtomicByteArray, GenericAtomicByteArrayWeak, GenericAtomicLocalData,
};

pub(crate) mod native_atomic;
pub use native_atomic::{
    NativeAtomicArray, NativeAtomicByteArray, NativeAtomicByteArrayWeak, NativeAtomicLocalData,
};

pub(crate) mod local_lock_atomic;
pub use local_lock_atomic::{
    LocalLockArray, LocalLockByteArray, LocalLockByteArrayWeak, LocalLockLocalChunks,
    LocalLockLocalChunksMut, LocalLockLocalData, LocalLockMutLocalData, LocalLockReadGuard,
    LocalLockWriteGuard,
};

pub(crate) mod global_lock_atomic;
pub use global_lock_atomic::{
    GlobalLockArray, GlobalLockByteArray, GlobalLockByteArrayWeak, GlobalLockLocalData,
    GlobalLockMutLocalData, GlobalLockReadGuard, GlobalLockWriteGuard,
};

pub mod iterator;
// //#[doc(hidden)]
pub use iterator::distributed_iterator::DistributedIterator;
// //#[doc(hidden)]
pub use iterator::local_iterator::LocalIterator;
// //#[doc(hidden)]
pub use iterator::one_sided_iterator::OneSidedIterator;

pub(crate) mod operations;
pub use operations::*;

pub(crate) mod handle;
pub use handle::*;

pub(crate) type ReduceGen = fn(LamellarByteArray, usize) -> LamellarArcAm;

lazy_static! {
    pub(crate) static ref REDUCE_OPS: HashMap<(std::any::TypeId, &'static str), ReduceGen> = {
        let mut temp = HashMap::new();
        for reduction_type in crate::inventory::iter::<ReduceKey> {
            temp.insert(
                ((reduction_type.id)(), reduction_type.name),
                reduction_type.gen,
            );
        }
        temp
    };
}

type ReduceIdGen = fn() -> std::any::TypeId;
#[doc(hidden)]
pub struct ReduceKey {
    pub id: ReduceIdGen,
    pub name: &'static str,
    pub gen: ReduceGen,
}
crate::inventory::collect!(ReduceKey);

// impl Dist for bool {}
// lamellar_impl::generate_reductions_for_type_rt!(true, u8, usize);
// lamellar_impl::generate_ops_for_type_rt!(true, true, true, u8, usize);

// lamellar_impl::generate_reductions_for_type_rt!(false, f64);
// lamellar_impl::generate_ops_for_type_rt!(false, false, false, f64);
lamellar_impl::generate_reductions_for_type_rt!(false, u128);
lamellar_impl::generate_ops_for_type_rt!(true, false, true, u128);

lamellar_impl::generate_reductions_for_type_rt!(true, u8, u16, u32, u64, usize);
lamellar_impl::generate_reductions_for_type_rt!(false, u128);
lamellar_impl::generate_ops_for_type_rt!(true, true, true, u8, u16, u32, u64, usize);
lamellar_impl::generate_ops_for_type_rt!(true, false, true, u128);

lamellar_impl::generate_reductions_for_type_rt!(true, i8, i16, i32, i64, isize);
lamellar_impl::generate_reductions_for_type_rt!(false, i128);
lamellar_impl::generate_ops_for_type_rt!(true, true, true, i8, i16, i32, i64, isize);
lamellar_impl::generate_ops_for_type_rt!(true, false, true, i128);

lamellar_impl::generate_reductions_for_type_rt!(false, f32, f64);
lamellar_impl::generate_ops_for_type_rt!(false, false, false, f32, f64);

lamellar_impl::generate_ops_for_bool_rt!();

impl<T: Dist + ArrayOps> Dist for Option<T> {}
impl<T: Dist + ArrayOps> ArrayOps for Option<T> {}
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
/// use lamellar::array::prelude::*;
/// let world = LamellarWorldBuilder::new().build();
/// let block_array = AtomicArray::<usize>::new(world,12,Distribution::Block);
/// //block array index location  = PE0 [0,1,2,3],  PE1 [4,5,6,7],  PE2 [8,9,10,11], PE3 [12,13,14,15]
///```
/// ## Cyclic
///```
/// use lamellar::array::prelude::*;
/// let world = LamellarWorldBuilder::new().build();
/// let cyclic_array = AtomicArray::<usize>::new(world,12,Distribution::Cyclic);
/// //cyclic array index location = PE0 [0,4,8,12], PE1 [1,5,9,13], PE2 [2,6,10,14], PE3 [3,7,11,15]
///```
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
pub enum Distribution {
    /// The indicies of the elements on each PE are sequential
    Block,
    /// The indicies of the elements on each PE have a stride equal to the number of PEs associated with the array
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

/// Registered memory regions that can be used as input to various LamellarArray RDMA operations.
// #[enum_dispatch(RegisteredMemoryRegion<T>, SubRegion<T>, TeamFrom<T>,MemoryRegionRDMA<T>,AsBase)]
#[derive(Clone, Debug)]
pub enum LamellarArrayRdmaInput<T: Dist> {
    /// Variant contiaining a memory region whose local data can be used as an input buffer
    LamellarMemRegion(LamellarMemoryRegion<T>),
    /// Variant contiaining a shared memory region whose local data can be used as an input buffer
    SharedMemRegion(SharedMemoryRegion<T>), //when used as input/output we are only using the local data
    /// Variant contiaining a onessided memory region that can be used as an input buffer
    LocalMemRegion(OneSidedMemoryRegion<T>),
    // UnsafeArray(UnsafeArray<T>),
}
impl<T: Dist> LamellarRead for LamellarArrayRdmaOutput<T> {}

/// Registered memory regions that can be used as output to various LamellarArray RDMA operations.
// #[enum_dispatch(RegisteredMemoryRegion<T>, SubRegion<T>, TeamFrom<T>,MemoryRegionRDMA<T>,AsBase)]
#[derive(Clone, Debug)]
pub enum LamellarArrayRdmaOutput<T: Dist> {
    /// Variant contiaining a memory region whose local data can be used as an output buffer
    LamellarMemRegion(LamellarMemoryRegion<T>),
    /// Variant contiaining a shared memory region whose local data can be used as an output buffer
    SharedMemRegion(SharedMemoryRegion<T>), //when used as input/output we are only using the local data
    /// Variant contiaining a onessided memory region that can be used as an output buffer
    LocalMemRegion(OneSidedMemoryRegion<T>),
    // UnsafeArray(UnsafeArray<T>),
}

impl<T: Dist> LamellarWrite for LamellarArrayRdmaOutput<T> {}

/// Trait for types that can be used as output to various LamellarArray RDMA operations.
pub trait LamellarWrite {}

/// Trait for types that can be used as input to various LamellarArray RDMA operations.
pub trait LamellarRead {}

// impl<T: Dist> LamellarRead for T {}
impl<T: Dist> LamellarRead for &T {}

impl<T: Dist> LamellarRead for Vec<T> {}
impl<T: Dist> LamellarRead for &Vec<T> {}
impl<T: Dist> LamellarRead for &[T] {}

impl<T: Dist> TeamFrom<&T> for LamellarArrayRdmaInput<T> {
    /// Constructs a single element [OneSidedMemoryRegion] and copies `val` into it
    fn team_from(val: &T, team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let buf: OneSidedMemoryRegion<T> = team.alloc_one_sided_mem_region(1);
        unsafe {
            buf.as_mut_slice().expect("Data should exist on PE")[0] = val.clone();
        }
        LamellarArrayRdmaInput::LocalMemRegion(buf)
    }
}

impl<T: Dist> TeamFrom<T> for LamellarArrayRdmaInput<T> {
    /// Constructs a single element [OneSidedMemoryRegion] and copies `val` into it
    fn team_from(val: T, team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let buf: OneSidedMemoryRegion<T> = team.alloc_one_sided_mem_region(1);
        unsafe {
            buf.as_mut_slice().expect("Data should exist on PE")[0] = val;
        }
        LamellarArrayRdmaInput::LocalMemRegion(buf)
    }
}

impl<T: Dist> TeamFrom<Vec<T>> for LamellarArrayRdmaInput<T> {
    /// Constructs a [OneSidedMemoryRegion] equal in length to `vals` and copies `vals` into it
    fn team_from(vals: Vec<T>, team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let buf: OneSidedMemoryRegion<T> = team.alloc_one_sided_mem_region(vals.len());
        unsafe {
            std::ptr::copy_nonoverlapping(
                vals.as_ptr(),
                buf.as_mut_ptr().expect("Data should exist on PE"),
                vals.len(),
            );
        }
        LamellarArrayRdmaInput::LocalMemRegion(buf)
    }
}
impl<T: Dist> TeamFrom<&Vec<T>> for LamellarArrayRdmaInput<T> {
    /// Constructs a [OneSidedMemoryRegion] equal in length to `vals` and copies `vals` into it
    fn team_from(vals: &Vec<T>, team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let buf: OneSidedMemoryRegion<T> = team.alloc_one_sided_mem_region(vals.len());
        unsafe {
            std::ptr::copy_nonoverlapping(
                vals.as_ptr(),
                buf.as_mut_ptr().expect("Data should exist on PE"),
                vals.len(),
            );
        }
        LamellarArrayRdmaInput::LocalMemRegion(buf)
    }
}
impl<T: Dist> TeamFrom<&[T]> for LamellarArrayRdmaInput<T> {
    /// Constructs a [OneSidedMemoryRegion] equal in length to `vals` and copies `vals` into it
    fn team_from(vals: &[T], team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let buf: OneSidedMemoryRegion<T> = team.alloc_one_sided_mem_region(vals.len());
        unsafe {
            std::ptr::copy_nonoverlapping(
                vals.as_ptr(),
                buf.as_mut_ptr().expect("Data should exist on PE"),
                vals.len(),
            );
        }
        LamellarArrayRdmaInput::LocalMemRegion(buf)
    }
}

impl<T: Dist> TeamFrom<&LamellarArrayRdmaInput<T>> for LamellarArrayRdmaInput<T> {
    fn team_from(lai: &LamellarArrayRdmaInput<T>, _team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        lai.clone()
    }
}

impl<T: Dist> TeamFrom<&LamellarArrayRdmaOutput<T>> for LamellarArrayRdmaOutput<T> {
    fn team_from(lao: &LamellarArrayRdmaOutput<T>, _team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        lao.clone()
    }
}

impl<T: Clone> TeamFrom<(&Vec<T>, Distribution)> for Vec<T> {
    fn team_from(vals: (&Vec<T>, Distribution), _team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        vals.0.to_vec()
    }
}

impl<T: Clone> TeamFrom<(Vec<T>, Distribution)> for Vec<T> {
    fn team_from(vals: (Vec<T>, Distribution), _team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        vals.0.to_vec()
    }
}

impl<T: Dist> TeamTryFrom<&T> for LamellarArrayRdmaInput<T> {
    fn team_try_from(val: &T, team: &Pin<Arc<LamellarTeamRT>>) -> Result<Self, anyhow::Error> {
        Ok(LamellarArrayRdmaInput::team_from(val, team))
    }
}

impl<T: Dist> TeamTryFrom<T> for LamellarArrayRdmaInput<T> {
    fn team_try_from(val: T, team: &Pin<Arc<LamellarTeamRT>>) -> Result<Self, anyhow::Error> {
        Ok(LamellarArrayRdmaInput::team_from(val, team))
    }
}

impl<T: Dist> TeamTryFrom<Vec<T>> for LamellarArrayRdmaInput<T> {
    fn team_try_from(val: Vec<T>, team: &Pin<Arc<LamellarTeamRT>>) -> Result<Self, anyhow::Error> {
        if val.len() == 0 {
            Err(anyhow::anyhow!(
                "Trying to create an empty LamellarArrayRdmaInput"
            ))
        } else {
            Ok(LamellarArrayRdmaInput::team_from(val, team))
        }
    }
}

impl<T: Dist> TeamTryFrom<&Vec<T>> for LamellarArrayRdmaInput<T> {
    fn team_try_from(val: &Vec<T>, team: &Pin<Arc<LamellarTeamRT>>) -> Result<Self, anyhow::Error> {
        if val.len() == 0 {
            Err(anyhow::anyhow!(
                "Trying to create an empty LamellarArrayRdmaInput"
            ))
        } else {
            Ok(LamellarArrayRdmaInput::team_from(val, team))
        }
    }
}

impl<T: Dist> TeamTryFrom<&[T]> for LamellarArrayRdmaInput<T> {
    fn team_try_from(val: &[T], team: &Pin<Arc<LamellarTeamRT>>) -> Result<Self, anyhow::Error> {
        if val.len() == 0 {
            Err(anyhow::anyhow!(
                "Trying to create an empty LamellarArrayRdmaInput"
            ))
        } else {
            Ok(LamellarArrayRdmaInput::team_from(val, team))
        }
    }
}

impl<T: Dist> TeamTryFrom<&LamellarArrayRdmaInput<T>> for LamellarArrayRdmaInput<T> {
    fn team_try_from(
        lai: &LamellarArrayRdmaInput<T>,
        _team: &Pin<Arc<LamellarTeamRT>>,
    ) -> Result<Self, anyhow::Error> {
        Ok(lai.clone())
    }
}

impl<T: Dist> TeamTryFrom<&LamellarArrayRdmaOutput<T>> for LamellarArrayRdmaOutput<T> {
    fn team_try_from(
        lao: &LamellarArrayRdmaOutput<T>,
        _team: &Pin<Arc<LamellarTeamRT>>,
    ) -> Result<Self, anyhow::Error> {
        Ok(lao.clone())
    }
}

impl<T: Clone> TeamTryFrom<(&Vec<T>, Distribution)> for Vec<T> {
    fn team_try_from(
        vals: (&Vec<T>, Distribution),
        _team: &Pin<Arc<LamellarTeamRT>>,
    ) -> Result<Self, anyhow::Error> {
        Ok(vals.0.to_vec())
    }
}

// #[async_trait]
// impl<T: Clone> AsyncTeamFrom<(&Vec<T>, Distribution)> for Vec<T> {
//     async fn team_from(vals: (&Vec<T>, Distribution), _team: &Pin<Arc<LamellarTeamRT>>) -> Self {
//         vals.0.to_vec()
//     }
// }

// #[async_trait]
impl<T: Dist + ArrayOps> AsyncTeamFrom<(Vec<T>, Distribution)> for Vec<T> {
    async fn team_from(input: (Vec<T>, Distribution), _team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        input.0
    }
}

#[async_trait]
/// Provides the same abstraction as the `From` trait in the standard language, but with a `team` parameter so that lamellar memory regions can be allocated
/// and to be used within an async context
pub(crate) trait AsyncInto<T>: Sized {
    async fn async_into(self) -> T;
}

#[async_trait]
/// Provides the same abstraction as the `From` trait in the standard language, but with a `team` parameter so that lamellar memory regions can be allocated
/// and to be used within an async context
pub(crate) trait AsyncFrom<T>: Sized {
    async fn async_from(val: T) -> Self;
}

// AsyncFrom implies AsyncInto
#[async_trait]
impl<T, U> AsyncInto<U> for T
where
    T: Send,
    U: AsyncFrom<T>,
{
    /// Calls `U::from(self).await`.
    ///
    /// That is, this conversion is whatever the implementation of
    /// <code>[AsyncFrom]&lt;T&gt; for U</code> chooses to do.
    #[inline]
    async fn async_into(self) -> U {
        U::async_from(self).await
    }
}

// AsyncFrom (and thus Into) is reflexive
// #[async_trait]
// impl<T> AsyncFrom<T> for T
// where
//     T: Send,
// {
//     /// Returns the argument unchanged.
//     #[inline(always)]
//     async fn async_from(t: T) -> T {
//         t
//     }
// }

/// Provides the same abstraction as the `From` trait in the standard language, but with a `team` parameter so that lamellar memory regions can be allocated
pub trait TeamFrom<T: ?Sized> {
    /// Converts to this type from the input type
    fn team_from(val: T, team: &Pin<Arc<LamellarTeamRT>>) -> Self;
}

// #[async_trait]
/// Provides the same abstraction as the `From` trait in the standard language, but with a `team` parameter so that lamellar memory regions can be allocated
/// and to be used within an async context
pub trait AsyncTeamFrom<T: ?Sized>: TeamFrom<T> + Sized {
    /// Converts to this type from the input type
    fn team_from(val: T, team: &Pin<Arc<LamellarTeamRT>>) -> impl Future<Output = Self> + Send;
}

/// Provides the same abstraction as the `TryFrom` trait in the standard language, but with a `team` parameter so that lamellar memory regions can be allocated
pub trait TeamTryFrom<T: ?Sized> {
    /// Trys to convert to this type from the input type
    fn team_try_from(val: T, team: &Pin<Arc<LamellarTeamRT>>) -> Result<Self, anyhow::Error>
    where
        Self: Sized;
}
/// Provides the same abstraction as the `Into` trait in the standard language, but with a `team` parameter so that lamellar memory regions can be allocated
pub trait TeamInto<T: ?Sized> {
    /// converts this type into the (usually inferred) input type
    fn team_into(self, team: &Pin<Arc<LamellarTeamRT>>) -> T;
}

/// Provides the same abstraction as the `Into` trait in the standard language, but with a `team` parameter so that lamellar memory regions can be allocated to be used within an async context
#[async_trait]
pub trait AsyncTeamInto<T: ?Sized> {
    /// converts this type into the (usually inferred) input type
    async fn team_into(self, team: &Pin<Arc<LamellarTeamRT>>) -> T;
}

/// Provides the same abstraction as the `TryInto` trait in the standard language, but with a `team` parameter so that lamellar memory regions can be allocated

pub trait TeamTryInto<T>: Sized {
    /// Trys to convert this type into the (usually inferred) input type
    fn team_try_into(self, team: &Pin<Arc<LamellarTeamRT>>) -> Result<T, anyhow::Error>;
}

impl<T, U> TeamInto<U> for T
where
    U: TeamFrom<T>,
{
    fn team_into(self, team: &Pin<Arc<LamellarTeamRT>>) -> U {
        U::team_from(self, team)
    }
}

#[async_trait]
impl<T: Send, U> AsyncTeamInto<U> for T
where
    U: AsyncTeamFrom<T>,
{
    async fn team_into(self, team: &Pin<Arc<LamellarTeamRT>>) -> U {
        <U as AsyncTeamFrom<T>>::team_from(self, team).await
    }
}

impl<T, U> TeamTryInto<U> for T
where
    U: TeamTryFrom<T>,
{
    fn team_try_into(self, team: &Pin<Arc<LamellarTeamRT>>) -> Result<U, anyhow::Error> {
        U::team_try_from(self, team)
    }
}

/// Represents the array types that allow Read operations
#[enum_dispatch]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(bound = "T: Dist + serde::Serialize + serde::de::DeserializeOwned + 'static")]
pub enum LamellarReadArray<T: Dist + 'static> {
    ///
    UnsafeArray(UnsafeArray<T>),
    ///
    ReadOnlyArray(ReadOnlyArray<T>),
    ///
    AtomicArray(AtomicArray<T>),
    ///
    LocalLockArray(LocalLockArray<T>),
    ///
    GlobalLockArray(GlobalLockArray<T>),
}

#[doc(hidden)]
#[enum_dispatch]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum LamellarByteArray {
    //we intentially do not include "byte" in the variant name to ease construciton in the proc macros
    //#[doc(hidden)]
    UnsafeArray(UnsafeByteArray),
    //#[doc(hidden)]
    ReadOnlyArray(ReadOnlyByteArray),
    //#[doc(hidden)]
    AtomicArray(AtomicByteArray),
    //#[doc(hidden)]
    NativeAtomicArray(NativeAtomicByteArray),
    //#[doc(hidden)]
    GenericAtomicArray(GenericAtomicByteArray),
    //#[doc(hidden)]
    LocalLockArray(LocalLockByteArray),
    //#[doc(hidden)]
    GlobalLockArray(GlobalLockByteArray),
}

impl LamellarByteArray {
    pub fn type_id(&self) -> std::any::TypeId {
        match self {
            LamellarByteArray::UnsafeArray(_) => std::any::TypeId::of::<UnsafeByteArray>(),
            LamellarByteArray::ReadOnlyArray(_) => std::any::TypeId::of::<ReadOnlyByteArray>(),
            LamellarByteArray::AtomicArray(_) => std::any::TypeId::of::<AtomicByteArray>(),
            LamellarByteArray::NativeAtomicArray(_) => {
                std::any::TypeId::of::<NativeAtomicByteArray>()
            }
            LamellarByteArray::GenericAtomicArray(_) => {
                std::any::TypeId::of::<GenericAtomicByteArray>()
            }
            LamellarByteArray::LocalLockArray(_) => std::any::TypeId::of::<LocalLockByteArray>(),
            LamellarByteArray::GlobalLockArray(_) => std::any::TypeId::of::<GlobalLockByteArray>(),
        }
    }

    pub(crate) fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        match self {
            LamellarByteArray::UnsafeArray(array) => array.inner.data.team(),
            LamellarByteArray::ReadOnlyArray(array) => array.array.inner.data.team(),
            LamellarByteArray::AtomicArray(array) => array.team(),
            LamellarByteArray::NativeAtomicArray(array) => array.array.inner.data.team(),
            LamellarByteArray::GenericAtomicArray(array) => array.array.inner.data.team(),
            LamellarByteArray::LocalLockArray(array) => array.array.inner.data.team(),
            LamellarByteArray::GlobalLockArray(array) => array.array.inner.data.team(),
        }
    }
}

impl<T: Dist + 'static> crate::active_messaging::DarcSerde for LamellarReadArray<T> {
    fn ser(&self, num_pes: usize, darcs: &mut Vec<RemotePtr>) {
        // println!("in shared ser");
        match self {
            LamellarReadArray::UnsafeArray(array) => array.ser(num_pes, darcs),
            LamellarReadArray::ReadOnlyArray(array) => array.ser(num_pes, darcs),
            LamellarReadArray::AtomicArray(array) => array.ser(num_pes, darcs),
            LamellarReadArray::LocalLockArray(array) => array.ser(num_pes, darcs),
            LamellarReadArray::GlobalLockArray(array) => array.ser(num_pes, darcs),
        }
    }
    fn des(&self, cur_pe: Result<usize, crate::IdError>) {
        // println!("in shared des");
        match self {
            LamellarReadArray::UnsafeArray(array) => array.des(cur_pe),
            LamellarReadArray::ReadOnlyArray(array) => array.des(cur_pe),
            LamellarReadArray::AtomicArray(array) => array.des(cur_pe),
            LamellarReadArray::LocalLockArray(array) => array.des(cur_pe),
            LamellarReadArray::GlobalLockArray(array) => array.des(cur_pe),
        }
    }
}

impl<T: Dist> ActiveMessaging for LamellarReadArray<T> {
    type SinglePeAmHandle<R: AmDist> = AmHandle<R>;
    type MultiAmHandle<R: AmDist> = MultiAmHandle<R>;
    type LocalAmHandle<L> = LocalAmHandle<L>;
    fn exec_am_all<F>(&self, am: F) -> Self::MultiAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        match self {
            LamellarReadArray::UnsafeArray(array) => array.exec_am_all(am),
            LamellarReadArray::ReadOnlyArray(array) => array.exec_am_all(am),
            LamellarReadArray::AtomicArray(array) => array.exec_am_all(am),
            LamellarReadArray::LocalLockArray(array) => array.exec_am_all(am),
            LamellarReadArray::GlobalLockArray(array) => array.exec_am_all(am),
        }
    }
    fn exec_am_pe<F>(&self, pe: usize, am: F) -> Self::SinglePeAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        match self {
            LamellarReadArray::UnsafeArray(array) => array.exec_am_pe(pe, am),
            LamellarReadArray::ReadOnlyArray(array) => array.exec_am_pe(pe, am),
            LamellarReadArray::AtomicArray(array) => array.exec_am_pe(pe, am),
            LamellarReadArray::LocalLockArray(array) => array.exec_am_pe(pe, am),
            LamellarReadArray::GlobalLockArray(array) => array.exec_am_pe(pe, am),
        }
    }
    fn exec_am_local<F>(&self, am: F) -> Self::LocalAmHandle<F::Output>
    where
        F: LamellarActiveMessage + LocalAM + 'static,
    {
        match self {
            LamellarReadArray::UnsafeArray(array) => array.exec_am_local(am),
            LamellarReadArray::ReadOnlyArray(array) => array.exec_am_local(am),
            LamellarReadArray::AtomicArray(array) => array.exec_am_local(am),
            LamellarReadArray::LocalLockArray(array) => array.exec_am_local(am),
            LamellarReadArray::GlobalLockArray(array) => array.exec_am_local(am),
        }
    }
    fn wait_all(&self) {
        match self {
            LamellarReadArray::UnsafeArray(array) => array.wait_all(),
            LamellarReadArray::ReadOnlyArray(array) => array.wait_all(),
            LamellarReadArray::AtomicArray(array) => array.wait_all(),
            LamellarReadArray::LocalLockArray(array) => array.wait_all(),
            LamellarReadArray::GlobalLockArray(array) => array.wait_all(),
        }
    }
    fn await_all(&self) -> impl Future<Output = ()> + Send {
        let fut: Pin<Box<dyn Future<Output = ()> + Send>> = match self {
            LamellarReadArray::UnsafeArray(array) => Box::pin(array.await_all()),
            LamellarReadArray::ReadOnlyArray(array) => Box::pin(array.await_all()),
            LamellarReadArray::AtomicArray(array) => Box::pin(array.await_all()),
            LamellarReadArray::LocalLockArray(array) => Box::pin(array.await_all()),
            LamellarReadArray::GlobalLockArray(array) => Box::pin(array.await_all()),
        };
        fut
    }
    fn barrier(&self) {
        match self {
            LamellarReadArray::UnsafeArray(array) => array.barrier(),
            LamellarReadArray::ReadOnlyArray(array) => array.barrier(),
            LamellarReadArray::AtomicArray(array) => array.barrier(),
            LamellarReadArray::LocalLockArray(array) => array.barrier(),
            LamellarReadArray::GlobalLockArray(array) => array.barrier(),
        }
    }
    fn async_barrier(&self) -> BarrierHandle {
        match self {
            LamellarReadArray::UnsafeArray(array) => array.async_barrier(),
            LamellarReadArray::ReadOnlyArray(array) => array.async_barrier(),
            LamellarReadArray::AtomicArray(array) => array.async_barrier(),
            LamellarReadArray::LocalLockArray(array) => array.async_barrier(),
            LamellarReadArray::GlobalLockArray(array) => array.async_barrier(),
        }
    }
    fn spawn<F: Future>(&self, f: F) -> LamellarTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        match self {
            LamellarReadArray::UnsafeArray(array) => array.spawn(f),
            LamellarReadArray::ReadOnlyArray(array) => array.spawn(f),
            LamellarReadArray::AtomicArray(array) => array.spawn(f),
            LamellarReadArray::LocalLockArray(array) => array.spawn(f),
            LamellarReadArray::GlobalLockArray(array) => array.spawn(f),
        }
    }
    fn block_on<F: Future>(&self, f: F) -> F::Output {
        match self {
            LamellarReadArray::UnsafeArray(array) => array.block_on(f),
            LamellarReadArray::ReadOnlyArray(array) => array.block_on(f),
            LamellarReadArray::AtomicArray(array) => array.block_on(f),
            LamellarReadArray::LocalLockArray(array) => array.block_on(f),
            LamellarReadArray::GlobalLockArray(array) => array.block_on(f),
        }
    }
    fn block_on_all<I>(&self, iter: I) -> Vec<<<I as IntoIterator>::Item as Future>::Output>
    where
        I: IntoIterator,
        <I as IntoIterator>::Item: Future + Send + 'static,
        <<I as IntoIterator>::Item as Future>::Output: Send,
    {
        match self {
            LamellarReadArray::UnsafeArray(array) => array.block_on_all(iter),
            LamellarReadArray::ReadOnlyArray(array) => array.block_on_all(iter),
            LamellarReadArray::AtomicArray(array) => array.block_on_all(iter),
            LamellarReadArray::LocalLockArray(array) => array.block_on_all(iter),
            LamellarReadArray::GlobalLockArray(array) => array.block_on_all(iter),
        }
    }
}

/// Represents the array types that allow write  operations
#[enum_dispatch]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(bound = "T: Dist + serde::Serialize + serde::de::DeserializeOwned")]
pub enum LamellarWriteArray<T: Dist> {
    ///
    UnsafeArray(UnsafeArray<T>),
    ///
    AtomicArray(AtomicArray<T>),
    ///
    LocalLockArray(LocalLockArray<T>),
    ///
    GlobalLockArray(GlobalLockArray<T>),
}

impl<T: Dist + 'static> crate::active_messaging::DarcSerde for LamellarWriteArray<T> {
    fn ser(&self, num_pes: usize, darcs: &mut Vec<RemotePtr>) {
        // println!("in shared ser");
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.ser(num_pes, darcs),
            LamellarWriteArray::AtomicArray(array) => array.ser(num_pes, darcs),
            LamellarWriteArray::LocalLockArray(array) => array.ser(num_pes, darcs),
            LamellarWriteArray::GlobalLockArray(array) => array.ser(num_pes, darcs),
        }
    }
    fn des(&self, cur_pe: Result<usize, crate::IdError>) {
        // println!("in shared des");
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.des(cur_pe),
            LamellarWriteArray::AtomicArray(array) => array.des(cur_pe),
            LamellarWriteArray::LocalLockArray(array) => array.des(cur_pe),
            LamellarWriteArray::GlobalLockArray(array) => array.des(cur_pe),
        }
    }
}

impl<T: Dist> ActiveMessaging for LamellarWriteArray<T> {
    type SinglePeAmHandle<R: AmDist> = AmHandle<R>;
    type MultiAmHandle<R: AmDist> = MultiAmHandle<R>;
    type LocalAmHandle<L> = LocalAmHandle<L>;
    fn exec_am_all<F>(&self, am: F) -> Self::MultiAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.exec_am_all(am),
            LamellarWriteArray::AtomicArray(array) => array.exec_am_all(am),
            LamellarWriteArray::LocalLockArray(array) => array.exec_am_all(am),
            LamellarWriteArray::GlobalLockArray(array) => array.exec_am_all(am),
        }
    }
    fn exec_am_pe<F>(&self, pe: usize, am: F) -> Self::SinglePeAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.exec_am_pe(pe, am),
            LamellarWriteArray::AtomicArray(array) => array.exec_am_pe(pe, am),
            LamellarWriteArray::LocalLockArray(array) => array.exec_am_pe(pe, am),
            LamellarWriteArray::GlobalLockArray(array) => array.exec_am_pe(pe, am),
        }
    }
    fn exec_am_local<F>(&self, am: F) -> Self::LocalAmHandle<F::Output>
    where
        F: LamellarActiveMessage + LocalAM + 'static,
    {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.exec_am_local(am),
            LamellarWriteArray::AtomicArray(array) => array.exec_am_local(am),
            LamellarWriteArray::LocalLockArray(array) => array.exec_am_local(am),
            LamellarWriteArray::GlobalLockArray(array) => array.exec_am_local(am),
        }
    }
    fn wait_all(&self) {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.wait_all(),
            LamellarWriteArray::AtomicArray(array) => array.wait_all(),
            LamellarWriteArray::LocalLockArray(array) => array.wait_all(),
            LamellarWriteArray::GlobalLockArray(array) => array.wait_all(),
        }
    }
    fn await_all(&self) -> impl Future<Output = ()> + Send {
        let fut: Pin<Box<dyn Future<Output = ()> + Send>> = match self {
            LamellarWriteArray::UnsafeArray(array) => Box::pin(array.await_all()),
            LamellarWriteArray::AtomicArray(array) => Box::pin(array.await_all()),
            LamellarWriteArray::LocalLockArray(array) => Box::pin(array.await_all()),
            LamellarWriteArray::GlobalLockArray(array) => Box::pin(array.await_all()),
        };
        fut
    }
    fn barrier(&self) {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.barrier(),
            LamellarWriteArray::AtomicArray(array) => array.barrier(),
            LamellarWriteArray::LocalLockArray(array) => array.barrier(),
            LamellarWriteArray::GlobalLockArray(array) => array.barrier(),
        }
    }
    fn async_barrier(&self) -> BarrierHandle {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.async_barrier(),
            LamellarWriteArray::AtomicArray(array) => array.async_barrier(),
            LamellarWriteArray::LocalLockArray(array) => array.async_barrier(),
            LamellarWriteArray::GlobalLockArray(array) => array.async_barrier(),
        }
    }
    fn spawn<F: Future>(&self, f: F) -> LamellarTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.spawn(f),
            LamellarWriteArray::AtomicArray(array) => array.spawn(f),
            LamellarWriteArray::LocalLockArray(array) => array.spawn(f),
            LamellarWriteArray::GlobalLockArray(array) => array.spawn(f),
        }
    }
    fn block_on<F: Future>(&self, f: F) -> F::Output {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.block_on(f),
            LamellarWriteArray::AtomicArray(array) => array.block_on(f),
            LamellarWriteArray::LocalLockArray(array) => array.block_on(f),
            LamellarWriteArray::GlobalLockArray(array) => array.block_on(f),
        }
    }
    fn block_on_all<I>(&self, iter: I) -> Vec<<<I as IntoIterator>::Item as Future>::Output>
    where
        I: IntoIterator,
        <I as IntoIterator>::Item: Future + Send + 'static,
        <<I as IntoIterator>::Item as Future>::Output: Send,
    {
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.block_on_all(iter),
            LamellarWriteArray::AtomicArray(array) => array.block_on_all(iter),
            LamellarWriteArray::LocalLockArray(array) => array.block_on_all(iter),
            LamellarWriteArray::GlobalLockArray(array) => array.block_on_all(iter),
        }
    }
}

// private sealed trait
#[doc(hidden)]
pub trait InnerArray: Sized {
    fn as_inner(&self) -> &r#unsafe::private::UnsafeArrayInner;
}

pub(crate) mod private {
    use crate::active_messaging::*;
    use crate::array::{
        AtomicArray, GenericAtomicArray, GlobalLockArray, LamellarByteArray, LamellarReadArray,
        LamellarWriteArray, LocalLockArray, NativeAtomicArray, ReadOnlyArray, UnsafeArray,
    };
    use crate::memregion::Dist;
    use crate::LamellarTeamRT;
    use enum_dispatch::enum_dispatch;
    use std::pin::Pin;
    use std::sync::Arc;
    //#[doc(hidden)]
    #[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
    pub trait LamellarArrayPrivate<T: Dist>: Clone {
        // // fn my_pe(&self) -> usize;
        fn inner_array(&self) -> &UnsafeArray<T>;
        fn local_as_ptr(&self) -> *const T;
        fn local_as_mut_ptr(&self) -> *mut T;
        fn pe_for_dist_index(&self, index: usize) -> Option<usize>;
        fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> Option<usize>;
        unsafe fn into_inner(self) -> UnsafeArray<T>;
        fn as_lamellar_byte_array(&self) -> LamellarByteArray;
    }

    //#[doc(hidden)]
    #[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
    pub(crate) trait ArrayExecAm<T: Dist> {
        fn team(&self) -> Pin<Arc<LamellarTeamRT>>;
        fn team_counters(&self) -> Arc<AMCounters>;
        fn exec_am_local_tg<F>(&self, am: F) -> LocalAmHandle<F::Output>
        where
            F: LamellarActiveMessage + LocalAM + 'static,
        {
            self.team().exec_am_local_tg(am, Some(self.team_counters()))
        }
        fn exec_am_pe_tg<F>(&self, pe: usize, am: F) -> AmHandle<F::Output>
        where
            F: RemoteActiveMessage + LamellarAM + AmDist,
        {
            self.team()
                .exec_am_pe_tg(pe, am, Some(self.team_counters()))
        }
        // fn exec_arc_am_pe<F>(&self, pe: usize, am: LamellarArcAm) -> AmHandle<F>
        // where
        //     F: AmDist,
        // {
        //     self.team()
        //         .exec_arc_am_pe(pe, am, Some(self.team_counters()))
        // }
        fn exec_am_all_tg<F>(&self, am: F) -> MultiAmHandle<F::Output>
        where
            F: RemoteActiveMessage + LamellarAM + AmDist,
        {
            self.team().exec_am_all_tg(am, Some(self.team_counters()))
        }
    }
}

/// Represents a distributed array, providing some convenience functions for getting simple information about the array.
/// This is mostly intended for use within the runtime (specifically for use in Proc Macros) but the available functions may be useful to endusers as well.
#[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
pub trait LamellarArray<T: Dist>: private::LamellarArrayPrivate<T> + ActiveMessaging {
    #[doc(alias("One-sided", "onesided"))]
    /// Returns the team used to construct this array, the PEs in the team represent the same PEs which have a slice of data of the array
    ///
    /// # One-sided Operation
    /// the result is returned only on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let a_team = array.team();
    ///```
    fn team_rt(&self) -> Pin<Arc<LamellarTeamRT>>; //todo turn this into Arc<LamellarTeam>

    // #[doc(alias("One-sided", "onesided"))]
    // /// Return the current PE of the calling thread
    // ///
    // /// # One-sided Operation
    // /// the result is returned only on the calling PE
    // ///
    // /// # Examples
    // ///```
    // /// use lamellar::array::prelude::*;
    // /// let world = LamellarWorldBuilder::new().build();
    // /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    // ///
    // /// assert_eq!(world.my_pe(),array.my_pe());
    // ///```
    // fn my_pe(&self) -> usize;

    // #[doc(alias("One-sided", "onesided"))]
    // /// Return the number of PEs containing data for this array
    // ///
    // /// # One-sided Operation
    // /// the result is returned only on the calling PE
    // ///
    // /// # Examples
    // ///```
    // /// use lamellar::array::prelude::*;
    // /// let world = LamellarWorldBuilder::new().build();
    // /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    // ///
    // /// assert_eq!(world.num_pes(),array.num_pes());
    // ///```
    // fn num_pes(&self) -> usize;

    #[doc(alias("One-sided", "onesided"))]
    /// Return the total number of elements in this array
    ///
    /// # One-sided Operation
    /// the result is returned only on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic);
    ///
    /// assert_eq!(100,array.len());
    ///```
    fn len(&self) -> usize;

    #[doc(alias("One-sided", "onesided"))]
    /// Return the number of elements of the array local to this PE
    ///
    /// # One-sided Operation
    /// the result is returned only on the calling PE
    ///
    /// # Examples
    /// Assume a 4 PE system
    ///```no_run //assert is for 4 PEs
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<i8> = ReadOnlyArray::new(&world,100,Distribution::Cyclic);
    ///
    /// assert_eq!(25,array.num_elems_local());
    ///```
    fn num_elems_local(&self) -> usize;

    /// Change the distribution this array handle uses to index into the data of the array.
    ///
    /// This is a one-sided call and does not redistribute the actual data, it simply changes how the array is indexed for this particular handle.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic);
    /// // do something interesting... or not
    /// let block_view = array.clone().use_distribution(Distribution::Block);
    ///```
    // fn use_distribution(self, distribution: Distribution) -> Self;

    // #[doc(alias = "Collective")]
    // /// Global synchronization method which blocks calling thread until all PEs in the owning Array data have entered the barrier
    // ///
    // /// # Collective Operation
    // /// Requires all PEs associated with the array to enter the barrier, otherwise deadlock will occur
    // ///
    // /// # Examples
    // ///```
    // /// use lamellar::array::prelude::*;
    // /// let world = LamellarWorldBuilder::new().build();
    // /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic);
    // ///
    // /// array.barrier();
    // ///```
    // fn barrier(&self);

    // #[doc(alias("One-sided", "onesided"))]
    // /// blocks calling thread until all remote tasks (e.g. element wise operations)
    // /// initiated by the calling PE have completed.
    // ///
    // /// # One-sided Operation
    // /// this is not a distributed synchronization primitive (i.e. it has no knowledge of a Remote PEs tasks), the calling thread will only wait for tasks
    // /// to finish that were initiated by the calling PE itself
    // ///
    // /// # Examples
    // ///```
    // /// use lamellar::array::prelude::*;
    // /// let world = LamellarWorldBuilder::new().build();
    // /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    // ///
    // /// for i in 0..100{
    // ///     array.add(i,1);
    // /// }
    // /// array.wait_all(); //block until the previous add operations have finished
    // ///```
    // fn wait_all(&self);

    // #[doc(alias("One-sided", "onesided"))]
    // /// Run a future to completion on the current thread
    // ///
    // /// This function will block the caller until the given future has completed, the future is executed within the Lamellar threadpool
    // ///
    // /// Users can await any future, including those returned from lamellar remote operations
    // ///
    // /// # One-sided Operation
    // /// this is not a distributed synchronization primitive and only blocks the calling thread until the given future has completed on the calling PE
    // ///
    // /// # Examples
    // ///```
    // /// use lamellar::array::prelude::*;
    // /// let world = LamellarWorldBuilder::new().build();
    // /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    // ///
    // /// let request = array.fetch_add(10,1000); //fetch index 10 and add 1000 to it
    // /// let result = array.block_on(request); //block until am has executed
    // /// // we also could have used world.block_on() or team.block_on()
    // ///```
    // fn block_on<F: Future>(&self, f: F) -> F::Output;

    #[doc(alias("One-sided", "onesided"))]
    /// Given a global index, calculate the PE and offset on that PE where the element actually resides.
    /// Returns None if the index is Out of bounds
    ///
    /// # One-sided Operation
    /// the result is returned only on the calling PE
    ///
    /// # Examples
    /// assume we have 4 PEs
    /// ## Block
    ///```no_run //assert is for 4 PEs
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let block_array: UnsafeArray<usize> = UnsafeArray::new(&world,16,Distribution::Block);
    /// // block array index location  = PE0 [0,1,2,3],  PE1 [4,5,6,7],  PE2 [8,9,10,11], PE3 [12,13,14,15]
    /// let  Some((pe,offset)) = block_array.pe_and_offset_for_global_index(6) else { panic!("out of bounds");};
    /// assert_eq!((pe,offset) ,(1,2));
    ///```
    /// ## Cyclic
    ///```no_run //assert is for 4 PEs
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let cyclic_array: UnsafeArray<usize> = UnsafeArray::new(world,16,Distribution::Cyclic);
    /// // cyclic array index location = PE0 [0,4,8,12], PE1 [1,5,9,13], PE2 [2,6,10,14], PE3 [3,7,11,15]
    /// let Some((pe,offset)) = cyclic_array.pe_and_offset_for_global_index(6) else { panic!("out of bounds");};
    /// assert_eq!((pe,offset) ,(2,1));
    ///```
    fn pe_and_offset_for_global_index(&self, index: usize) -> Option<(usize, usize)>;

    #[doc(alias("One-sided", "onesided"))]
    /// Given a PE, return the global index of the first element on that PE
    /// Returns None if no data exists on that PE
    ///
    /// # One-sided Operation
    /// the result is returned only on the calling PE
    ///
    /// # Examples
    /// assume we have 4 PEs
    /// ## Block
    ///```no_run //assert is for 4 PEs
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let block_array: UnsafeArray<usize> = UnsafeArray::new(&world,16,Distribution::Block);
    /// // block array index location  = PE0 [0,1,2,3],  PE1 [4,5,6,7],  PE2 [8,9,10,11], PE3 [12,13,14,15]
    /// let index = block_array.first_global_index_for_pe(0).unwrap();
    /// assert_eq!(index , 0);
    /// let index = block_array.first_global_index_for_pe(1).unwrap();
    /// assert_eq!(index , 4);
    /// let index = block_array.first_global_index_for_pe(2).unwrap();
    /// assert_eq!(index , 8);
    /// let index = block_array.first_global_index_for_pe(3).unwrap();
    /// assert_eq!(index , 12);
    ///```
    /// ## Cyclic
    ///```no_run //assert is for 4 PEs
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let cyclic_array: UnsafeArray<usize> = UnsafeArray::new(world,16,Distribution::Cyclic);
    /// // cyclic array index location = PE0 [0,4,8,12], PE1 [1,5,9,13], PE2 [2,6,10,14], PE3 [3,7,11,15]
    /// let Some((pe,offset)) = cyclic_array.pe_and_offset_for_global_index(6) else { panic!("out of bounds");};
    /// let index = block_array.first_global_index_for_pe(0).unwrap();
    /// assert_eq!(index , 0);
    /// let index = block_array.first_global_index_for_pe(1).unwrap();
    /// assert_eq!(index , 1);
    /// let index = block_array.first_global_index_for_pe(2).unwrap();
    /// assert_eq!(index , 2);
    /// let index = block_array.first_global_index_for_pe(3).unwrap();
    /// assert_eq!(index , 3);
    ///```
    fn first_global_index_for_pe(&self, pe: usize) -> Option<usize>;

    #[doc(alias("One-sided", "onesided"))]
    /// Given a PE, return the global index of the first element on that PE
    /// Returns None if no data exists on that PE
    ///
    /// # One-sided Operation
    /// the result is returned only on the calling PE
    ///
    /// # Examples
    /// assume we have 4 PEs
    /// ## Block
    ///```no_run //assert is for 4 PEs
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let block_array: UnsafeArray<usize> = UnsafeArray::new(&world,16,Distribution::Block);
    /// // block array index location  = PE0 [0,1,2,3],  PE1 [4,5,6,7],  PE2 [8,9,10,11], PE3 [12,13,14,15]
    /// let index = block_array.last_global_index_for_pe(0).unwrap();
    /// assert_eq!(index , 3);
    /// let index = block_array.last_global_index_for_pe(1).unwrap();
    /// assert_eq!(index , 7);
    /// let index = block_array.last_global_index_for_pe(2).unwrap();
    /// assert_eq!(index , 11);
    /// let index = block_array.last_global_index_for_pe(3).unwrap();
    /// assert_eq!(index , 15);
    ///```
    /// ## Cyclic
    ///```no_run //assert is for 4 PEs
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let cyclic_array: UnsafeArray<usize> = UnsafeArray::new(world,16,Distribution::Cyclic);
    /// // cyclic array index location = PE0 [0,4,8,12], PE1 [1,5,9,13], PE2 [2,6,10,14], PE3 [3,7,11,15]
    /// let Some((pe,offset)) = cyclic_array.pe_and_offset_for_global_index(6) else { panic!("out of bounds");};
    /// let index = block_array.last_global_index_for_pe(0).unwrap();
    /// assert_eq!(index , 12);
    /// let index = block_array.last_global_index_for_pe(1).unwrap();
    /// assert_eq!(index , 13);
    /// let index = block_array.last_global_index_for_pe(2).unwrap();
    /// assert_eq!(index , 14);
    /// let index = block_array.last_global_index_for_pe(3).unwrap();
    /// assert_eq!(index , 15);
    ///```
    fn last_global_index_for_pe(&self, pe: usize) -> Option<usize>;

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
    #[doc(hidden)]
    type Array: LamellarArray<T>;
    #[doc(alias("One-sided", "onesided"))]
    /// Create a sub array of this UnsafeArray which consists of the elements specified by the range
    ///
    /// Note: it is possible that the subarray does not contain any data on this PE
    ///
    ///
    /// # One-sided Operation
    /// this does not affect how data in the array is distributed, nor require communication/coordination with other PEs in the array,
    /// rather it creates a handle on the calling PE which only has access to the elements in the specified range.
    ///
    /// # Panic
    /// This call will panic if the end of the range exceeds the size of the array.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let sub_array = array.sub_array(25..75);
    ///```
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array;

    #[doc(alias("One-sided", "onesided"))]
    /// Given an index with respect to the SubArray, return the index with respect to original array.
    ///
    /// # One-sided Operation
    /// the result is returned only on the calling PE
    ///
    /// # Panic
    /// This call will panic if the end of the range exceeds the size of the array.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let sub_array = array.sub_array(25..75);
    /// assert_eq!(25,sub_array.global_index(0));
    ///```
    fn global_index(&self, sub_index: usize) -> usize;
}

/// Interface defining low level APIs for copying data from an array into a buffer or local variable
pub trait LamellarArrayGet<T: Dist>: LamellarArrayInternalGet<T> {
    #[doc(alias("One-sided", "onesided"))]
    /// Performs an (active message based) "Get" of the data in this array starting at the provided `index` into the specified `dst`
    ///
    /// The length of the Get is dictated by the length of the buffer.
    ///
    /// This call returns a future that can be awaited to determine when the `get` has finished
    ///
    /// # Warning
    /// This is a low-level API, unless you are very confident in low level distributed memory access it is highly recommended
    /// you use a safe Array type and utilize the LamellarArray load/store operations instead.
    ///
    /// # Safety
    /// when using this call we need to think about safety in terms of the array and the destination buffer
    /// ## Arrays
    /// - [UnsafeArray] - always unsafe as there are no protections on the arrays data.
    /// - [AtomicArray] - technically safe, but potentially not what you want, `loads` of individual elements are atomic, but a copy of a range of elements its not atomic (we iterate through the range copying each element individually)
    /// - [LocalLockArray] - always safe as we grab a local read lock before transfering the data (preventing any modifcation from happening on the array)
    /// - [ReadOnlyArray] - always safe, read only arrays are never modified.
    /// ## Destination Buffer
    /// - [SharedMemoryRegion] - always unsafe as there are no guarantees that there may be other local and remote readers/writers.
    /// - [OneSidedMemoryRegion] - always unsafe as there are no guarantees that there may be other local and remote readers/writers.
    ///
    /// # One-sided Operation
    /// the remote transfer is initiated by the calling PE
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][AmHandle::spawn] or [blocked on][AmHandle::block]
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array = LocalLockArray::<usize>::new(&world,12,Distribution::Block);
    /// let buf = world.alloc_one_sided_mem_region::<usize>(12);
    /// array.dist_iter_mut().enumerate().for_each(|(i,elem)| *elem = i); //we will used this val as completion detection
    /// unsafe { // we just created buf and have not shared it so free to mutate safely
    ///     for elem in buf.as_mut_slice()
    ///                          .expect("we just created it so we know its local") { //initialize mem_region
    ///         *elem = buf.len();
    ///     }
    /// }
    /// array.wait_all();
    /// array.barrier();
    /// println!("PE{my_pe} array data: {:?}",unsafe{buf.as_slice().unwrap()});
    /// if my_pe == 0 { //only perfrom the transfer from one PE
    ///     println!();
    ///      unsafe { world.block_on(array.get(0,&buf))}; //safe because we have not shared buf, and we block immediately on the request
    /// }
    /// println!("PE{my_pe} buf data: {:?}",unsafe{buf.as_slice().unwrap()});
    ///
    ///```
    /// Possible output on A 4 PE system (ordering with respect to PEs may change)
    ///```text
    /// PE0: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE1: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE2: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE3: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    ///
    /// PE1: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE2: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE3: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE0: buf data [0,1,2,3,4,5,6,7,8,9,10,11] //we only did the "get" on PE0, also likely to be printed last since the other PEs do not wait for PE0 in this example
    ///```
    #[must_use = "this function is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it "]
    unsafe fn get<U: TeamTryInto<LamellarArrayRdmaOutput<T>> + LamellarWrite>(
        &self,
        index: usize,
        dst: U,
    ) -> ArrayRdmaHandle;

    #[doc(alias("One-sided", "onesided"))]
    /// Retrieves the element in this array located at the specified `index`
    ///
    /// This call returns a future that can be awaited to retrieve to requested element
    ///
    /// # Safety
    /// when using this call we need to think about safety in terms of the array type
    /// ## Arrays
    /// - [UnsafeArray] - always unsafe as there are no protections on the arrays data.
    /// - [AtomicArray] - always safe as loads of a single element are atomic
    /// - [LocalLockArray] - always safe as we grab a local read lock before transfering the data (preventing any modifcation from happening on the array)
    /// - [ReadOnlyArray] - always safe, read only arrays are never modified.
    ///
    /// # One-sided Operation
    /// the remote transfer is initiated by the calling PE
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][ArrayRdmaHandle::spawn] or [blocked on][ArrayRdmaHandle::block]
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    /// let array = LocalLockArray::<usize>::new(&world,12,Distribution::Block);
    /// array.dist_iter_mut().enumerate().for_each(move |(i,elem)| *elem = my_pe); //we will used this val as completion detection
    /// array.wait_all();
    /// array.barrier();
    /// println!("PE{my_pe} array data: {:?}",array.read_local_data());
    /// let index = ((my_pe+1)%num_pes) * array.num_elems_local(); // get first index on PE to the right (with wrap arround)
    /// let at_req = array.at(index);
    /// let val = array.block_on(at_req);
    /// println!("PE{my_pe} array[{index}] = {val}");
    ///```
    /// Possible output on A 4 PE system (ordering with respect to PEs may change)
    ///```text
    /// PE0: buf data [0,0,0]
    /// PE1: buf data [1,1,1]
    /// PE2: buf data [2,2,2]
    /// PE3: buf data [3,3,3]
    ///
    /// PE0: array[3] = 1
    /// PE1: array[6] = 2
    /// PE2: array[9] = 3
    /// PE3: array[0] = 0
    ///```
    #[must_use = "this function is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it "]
    fn at(&self, index: usize) -> ArrayRdmaAtHandle<T>;
}

#[doc(hidden)]
#[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
pub trait LamellarArrayInternalGet<T: Dist>: LamellarArray<T> {
    unsafe fn internal_get<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        index: usize,
        dst: U,
    ) -> ArrayRdmaHandle;

    // blocking call that gets the value stored and the provided index
    unsafe fn internal_at(&self, index: usize) -> ArrayRdmaAtHandle<T>;
}

/// Interface defining low level APIs for copying data from a buffer or local variable into this array
pub trait LamellarArrayPut<T: Dist>: LamellarArrayInternalPut<T> {
    #[doc(alias("One-sided", "onesided"))]
    /// Performs an (active message based) "Put" of the data in the specified `src` buffer into this array starting from the provided `index`
    ///
    /// The length of the Put is dictated by the length of the `src` buffer.
    ///
    /// This call returns a future that can be awaited to determine when the `put` has finished
    ///
    /// # Warning
    /// This is a low-level API, unless you are very confident in low level distributed memory access it is highly recommended
    /// you use a safe Array type and utilize the LamellarArray load/store operations instead.
    ///
    ///
    /// # Safety
    /// when using this call we need to think about safety in terms of the array and the source buffer
    ///
    /// ## Arrays
    /// - [UnsafeArray] - always unsafe as there are no protections on the arrays data.
    /// - [AtomicArray] - technically safe, but potentially not what you want, `stores` of individual elements are atomic, but writing to a range of elements its not atomic overall (we iterate through the range writing to each element individually)
    /// - [LocalLockArray] - always safe as we grab a local write lock before writing the data (ensuring mutual exclusitivity when modifying the array)
    /// ## Source Buffer
    /// - [SharedMemoryRegion] - always unsafe as there are no guarantees that there may be other local and remote readers/writers
    /// - [OneSidedMemoryRegion] - always unsafe as there are no guarantees that there may be other local and remote readers/writers
    /// - `Vec`,`T` - always safe as ownership is transfered to the `Put`
    /// - `&Vec`, `&T` - always safe as these are immutable borrows
    ///
    /// # One-sided Operation
    /// the remote transfer is initiated by the calling PE
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][ArrayRdmaHandle::spawn] or [blocked on][ArrayRdmaHandle::block]
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array = LocalLockArray::<usize>::new(&world,12,Distribution::Block);
    /// let buf = world.alloc_one_sided_mem_region::<usize>(12);
    /// let len = buf.len();
    /// array.dist_iter_mut().for_each(move |elem| *elem = len); //we will used this val as completion detection
    ///
    /// //Safe as we are this is the only reference to buf   
    /// unsafe {
    ///     for (i,elem) in buf.as_mut_slice()
    ///                       .expect("we just created it so we know its local")
    ///                       .iter_mut()
    ///                        .enumerate(){ //initialize mem_region
    ///       *elem = i;
    ///     }
    /// }
    /// array.wait_all();
    /// array.barrier();
    /// println!("PE{my_pe} array data: {:?}",array.local_data());
    /// if my_pe == 0 { //only perfrom the transfer from one PE
    ///     array.block_on( unsafe {  array.put(0,&buf) } );
    ///     println!();
    /// }
    /// array.barrier(); //block other PEs until PE0 has finised "putting" the data
    ///    
    /// println!("PE{my_pe} array data: {:?}",array.local_data());
    ///     
    ///
    ///```
    /// Possible output on A 4 PE system (ordering with respect to PEs may change)
    ///```text
    /// PE0: array data [12,12,12]
    /// PE1: array data [12,12,12]
    /// PE2: array data [12,12,12]
    /// PE3: array data [12,12,12]
    ///
    /// PE0: array data [0,1,2]
    /// PE1: array data [3,4,5]
    /// PE2: array data [6,7,8]
    /// PE3: array data [9,10,11]
    ///```
    #[must_use = "this function is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it "]
    unsafe fn put<U: TeamTryInto<LamellarArrayRdmaInput<T>> + LamellarRead>(
        &self,
        index: usize,
        src: U,
    ) -> ArrayRdmaHandle;
}

#[doc(hidden)]
#[enum_dispatch(LamellarWriteArray<T>)]
pub trait LamellarArrayInternalPut<T: Dist>: LamellarArray<T> {
    //put data from buf into self
    unsafe fn internal_put<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        index: usize,
        src: U,
    ) -> ArrayRdmaHandle;
}

/// An interfacing allowing for conveiniently printing the data contained within a lamellar array
pub trait ArrayPrint<T: Dist + std::fmt::Debug>: LamellarArray<T> {
    #[doc(alias = "Collective")]
    /// Print the data within a lamellar array
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the array to enter the print call otherwise deadlock will occur (i.e. barriers are being called internally)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let block_array = AtomicArray::<usize>::new(&world,100,Distribution::Block);
    /// let cyclic_array = AtomicArray::<usize>::new(&world,100,Distribution::Block);
    ///
    /// block_array.dist_iter().zip(cyclic_array.dist_iter()).enumerate().for_each(move |i,(a,b)| {
    ///     a.store(i);
    ///     b.store(i);
    /// });
    /// block_array.print();
    /// println!();
    /// cyclic_array.print();
    ///```
    fn print(&self);
}

// pub(crate) trait LamellarArrayReduceInner<T>: LamellarArrayInternalGet<T>
// where
//     T: Dist + AmDist + 'static,
// {
//     fn get_reduction_op(&self, op: &str) -> LamellarArcAm;
//     fn reduce_data(&self, func: LamellarArcAm) -> Box<dyn LamellarRequest<Output = T>>;
//     fn reduce_req(&self, op: &str) -> Box<dyn LamellarRequest<Output = T>>;
// }

/// An interface for performing distributed reductions accross a lamellar array.
///
/// This trait exposes a few common reductions implemented by the runtime
/// as well as the ability the launch user defined reductions that have been registered with the runtime at compile time
///
/// Please see the documentation for the [register_reduction] procedural macro for
/// more details and examples on how to create your own reductions.
///
/// Currently these are one sided reductions, meaning the calling PE will initiate the reduction, and launch the appropriate Active Messages
/// with out requiring synchronization with the other PEs
///
/// We plan to expose a collective reductions (e.g. reduce_all) in a future release, as well as support for broadcast operations.
///
/// # Safety
/// This trait is only implelemted by the safe array types, for UnsafeArray we expose unsafe APIs of the functions.
///
/// One thing to consider is that due to being a one sided reduction, safety is only gauranteed with respect to Atomicity of individual elements,
/// not with respect to the entire global array. This means that while one PE is performing a reduction, other PEs can atomically update their local
/// elements. While this is technically safe with respect to the integrity of an indivdual element (and with respect to the compiler),
/// it may not be your desired behavior.
///
/// To be clear this behavior is not an artifact of lamellar, but rather the language itself,
/// for example if you have an `Arc<Vec<AtomicUsize>>` shared on multiple threads, you could safely update the elements from each thread,
/// but performing a reduction could result in safe but non deterministic results.
///
/// In Lamellar converting to a [ReadOnlyArray] before the reduction is a straightforward workaround to enusre the data is not changing during the reduction.
///
/// # Examples
/// We provide a series of examples illustrating the above issues
///```
/// use lamellar::array::prelude::*;
/// let world = LamellarWorldBuilder::new().build();
/// let array = AtomicArray::<usize>::new(&world,1000000,Distribution::Block);
/// use rand::Rng;
///
/// let array_clone = array.clone();
/// array.local_iter().for_each(move |_| {
///     let index = rand::thread_rng().gen_range(0..array_clone.len());
///     array_clone.add(index,1); //randomly at one to an element in the array.
/// });
/// let sum = array.block_on(array.sum()); // atomic updates still possibly happening, output non deterministic
/// println!("sum {sum}");
///```
/// Waiting for local operations to finish not enough by itself
///```
/// use lamellar::array::prelude::*;
/// use rand::Rng;
/// let world = LamellarWorldBuilder::new().build();
/// let array = AtomicArray::<usize>::new(&world,1000000,Distribution::Block);
/// let array_clone = array.clone();
/// let req = array.local_iter().for_each(move |_| {
///     let index = rand::thread_rng().gen_range(0..array_clone.len());
///     array_clone.add(index,1); //randomly at one to an element in the array.
/// });
/// array.block_on(req);// this is not sufficient, we also need to "wait_all" as each "add" call is another request
/// array.wait_all();
/// let sum = array.block_on(array.sum()); // atomic updates still possibly happening (on remote nodes), output non deterministic
/// println!("sum {sum}");
///```
/// Need to add a barrier after local operations on all PEs have finished
///```
/// use lamellar::array::prelude::*;
/// use rand::Rng;
/// let world = LamellarWorldBuilder::new().build();
/// let num_pes = world.num_pes();
/// let array = AtomicArray::<usize>::new(&world,1000000,Distribution::Block);
/// let array_clone = array.clone();
/// let req = array.local_iter().for_each(move |_| {
///     let index = rand::thread_rng().gen_range(0..array_clone.len());
///     array_clone.add(index,1); //randomly at one to an element in the array.
/// });
/// array.block_on(req);// this is not sufficient, we also need to "wait_all" as each "add" call is another request
/// array.wait_all();
/// array.barrier();
/// let sum = array.block_on(array.sum()); // No updates occuring anywhere anymore so we have a deterministic result
/// assert_eq!(array.len()*num_pes,sum);
///```
/// Alternatively we can convert our AtomicArray into a ReadOnlyArray before the reduction
/// ```
/// use lamellar::array::prelude::*;
/// use rand::Rng;
/// let world = LamellarWorldBuilder::new().build();
/// let num_pes = world.num_pes();
/// let array = AtomicArray::<usize>::new(&world,1000000,Distribution::Block);
/// let array_clone = array.clone();
/// let req = array.local_iter().for_each(move |_| {
///     let index = rand::thread_rng().gen_range(0..array_clone.len());
///     array_clone.add(index,1); //randomly at one to an element in the array.
/// });
/// let array = array.into_read_only(); //only returns once there is a single reference remaining on each PE
/// let sum = array.block_on(array.sum()); // No updates occuring anywhere anymore so we have a deterministic result
/// assert_eq!(array.len()*num_pes,sum);
///```
/// Finally we are inlcuding a `Arc<Vec<AtomicUsize>>` highlightin the same issue
///```
/// use std::sync::atomic::{AtomicUsize,Ordering};
/// use std::sync::Arc;
/// use std::thread;

/// use rand::prelude::*;
/// use std::time::Duration;

/// let  mut data = vec![];
/// for _i in 0..1000{
///     data.push(AtomicUsize::new(0));
/// }
/// let shared_data = Arc::new(data);
/// for _i in 0..4{
///     let shared_data = shared_data.clone();
///     thread::spawn ( move ||{
///         let mut rng = rand::thread_rng();
///         for _i in 0..10000{
///             let index = rng.gen_range(0..shared_data.len());
///             shared_data[index].fetch_add(1,Ordering::SeqCst);
///         }
///     });
/// }
/// let mut sum = shared_data.iter().map(|elem| elem.load(Ordering::SeqCst)).reduce(|sum,item| sum+item).expect("iter has more than one element");
/// println!{"sum {sum:?}"};
/// while sum < 40000 {
///     sum = shared_data.iter().map(|elem| elem.load(Ordering::SeqCst)).reduce(|sum,item| sum+item).expect("iter has more than one element");
///     println!{"sum {sum:?}"};
/// }
///```
pub trait LamellarArrayReduce<T>: LamellarArrayInternalGet<T>
where
    T: Dist + AmDist + 'static,
{
    /// The Handle type returned by the reduce operation
    type Handle;
    #[doc(alias("One-sided", "onesided"))]
    /// Perform a reduction on the entire distributed array, returning the value to the calling PE.
    ///
    /// Please see the documentation for the [register_reduction] procedural macro for
    /// more details and examples on how to create your own reductions.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Reduce` active messages on the other PEs associated with the array.
    /// the returned reduction result is only available on the calling PE  
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// use rand::Rng;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = AtomicArray::<usize>::new(&world,1000000,Distribution::Block);
    /// let array_clone = array.clone();
    /// let req = array.local_iter().for_each(move |_| {
    ///     let index = rand::thread_rng().gen_range(0..array_clone.len());
    ///     array_clone.add(index,1); //randomly at one to an element in the array.
    /// });
    /// let array = array.into_read_only(); //only returns once there is a single reference remaining on each PE
    /// let sum = array.block_on(array.reduce("sum")); // equivalent to calling array.sum()
    /// assert_eq!(array.len()*num_pes,sum);
    ///```
    fn reduce(&self, reduction: &str) -> Self::Handle;
}

/// Interface for common arithmetic based reductions
// pub trait LamellarArrayArithmeticReduce<T>: LamellarArrayReduce<T>
// where
//     T: Dist + AmDist + ElementArithmeticOps + 'static,
// {
//     #[doc(alias("One-sided", "onesided"))]
//     /// Perform a sum reduction on the entire distributed array, returning the value to the calling PE.
//     ///
//     /// This equivalent to `reduce("sum")`.
//     ///
//     /// # One-sided Operation
//     /// The calling PE is responsible for launching `Sum` active messages on the other PEs associated with the array.
//     /// the returned sum reduction result is only available on the calling PE
//     ///
//     /// # Examples
//     /// ```
//     /// use lamellar::array::prelude::*;
//     /// use rand::Rng;
//     /// let world = LamellarWorldBuilder::new().build();
//     /// let num_pes = world.num_pes();
//     /// let array = AtomicArray::<usize>::new(&world,1000000,Distribution::Block);
//     /// let array_clone = array.clone();
//     /// let req = array.local_iter().for_each(move |_| {
//     ///     let index = rand::thread_rng().gen_range(0..array_clone.len());
//     ///     array_clone.add(index,1); //randomly at one to an element in the array.
//     /// });
//     /// let array = array.into_read_only(); //only returns once there is a single reference remaining on each PE
//     /// let sum = array.block_on(array.sum());
//     /// assert_eq!(array.len()*num_pes,sum);
//     ///```
//     fn sum(&self) -> Self::Handle;

//     #[doc(alias("One-sided", "onesided"))]
//     /// Perform a production reduction on the entire distributed array, returning the value to the calling PE.
//     ///
//     /// This equivalent to `reduce("prod")`.
//     ///
//     /// # One-sided Operation
//     /// The calling PE is responsible for launching `Prod` active messages on the other PEs associated with the array.
//     /// the returned prod reduction result is only available on the calling PE
//     ///
//     /// # Examples
//     /// ```
//     /// use lamellar::array::prelude::*;
//     /// let world = LamellarWorldBuilder::new().build();
//     /// let num_pes = world.num_pes();
//     /// let array = AtomicArray::<usize>::new(&world,10,Distribution::Block);
//     /// let req = array.dist_iter().enumerate().for_each(move |(i,elem)| {
//     ///     elem.store(i+1);
//     /// });
//     /// array.wait_all();
//     /// array.barrier();
//     /// let prod =  array.block_on(array.prod());
//     /// assert_eq!((1..=array.len()).product::<usize>(),prod);
//     ///```
//     fn prod(&self) -> Self::Handle;
// }

/// Interface for common compare based reductions
// pub trait LamellarArrayCompareReduce<T>: LamellarArrayReduce<T>
// where
//     T: Dist + AmDist + ElementComparePartialEqOps + 'static,
// {
//     #[doc(alias("One-sided", "onesided"))]
//     /// Find the max element in the entire destributed array, returning to the calling PE
//     ///
//     /// This equivalent to `reduce("max")`.
//     ///
//     /// # One-sided Operation
//     /// The calling PE is responsible for launching `Max` active messages on the other PEs associated with the array.
//     /// the returned max reduction result is only available on the calling PE
//     ///
//     /// # Examples
//     /// ```
//     /// use lamellar::array::prelude::*;
//     /// let world = LamellarWorldBuilder::new().build();
//     /// let num_pes = world.num_pes();
//     /// let array = AtomicArray::<usize>::new(&world,10,Distribution::Block);
//     /// let req = array.dist_iter().enumerate().for_each(move |(i,elem)| elem.store(i*2));
//     /// let array = array.into_read_only(); //only returns once there is a single reference remaining on each PE
//     /// let max = array.block_on(array.max());
//     /// assert_eq!((array.len()-1)*2,max);
//     ///```
//     fn max(&self) -> Self::Handle;

//     #[doc(alias("One-sided", "onesided"))]
//     /// Find the min element in the entire destributed array, returning to the calling PE
//     ///
//     /// This equivalent to `reduce("min")`.
//     ///
//     /// # One-sided Operation
//     /// The calling PE is responsible for launching `Min` active messages on the other PEs associated with the array.
//     /// the returned min reduction result is only available on the calling PE
//     ///
//     /// # Examples
//     /// ```
//     /// use lamellar::array::prelude::*;
//     /// let world = LamellarWorldBuilder::new().build();
//     /// let num_pes = world.num_pes();
//     /// let array = AtomicArray::<usize>::new(&world,10,Distribution::Block);
//     /// let req = array.dist_iter().enumerate().for_each(move |(i,elem)| elem.store(i*2));
//     /// let array = array.into_read_only(); //only returns once there is a single reference remaining on each PE
//     /// let min = array.block_on(array.min());
//     /// assert_eq!(0,min);
//     ///```
//     fn min(&self) -> Self::Handle;
// }

/// This procedural macro is used to enable the execution of user defined reductions on LamellarArrays.
///
/// The general form of using this macro is:
/// ```register_reduction!(name,closure,type1,type2,...)```
/// - `name` is how the reduction will be registered with runtime and used to launch the reduction
/// - `closure` is the user defined reduction and takes the form of:
///     - ```FnMut(T, T -> T```
/// - `type1`, `type2`,... are the types for which we would like this reduction to work for
///     - reductions get implemented as [Active Messages][crate::active_messaging] and as such must use concrete types (no generics) to register correctly
///
/// The procedural macro will appropriately construct various implmentation so that the safety guarantees of each lamellary array type are maintained.
///
/// # Panics
/// This will panic at Runtime initialization if the name of the reduction is duplicated.
///
/// # Examples
/// Recreating the "Sum" reduction
/// ```
/// use lamellar::array::prelude::*;
/// use rand::Rng;
///
/// register_reduction!(
///     my_sum, // the name of our new reduction
///     |acc,elem| acc+elem , //the reduction closure
///     usize, // will be implementd for usize,f32, and i32
///     f32,
///     i32,
/// );
/// let world = LamellarWorldBuilder::new().build();
/// let num_pes = world.num_pes();
/// let array = AtomicArray::<usize>::new(&world,1000000,Distribution::Block);
/// let array_clone = array.clone();
/// let req = array.local_iter().for_each(move |_| {
///     let index = rand::thread_rng().gen_range(0..array_clone.len());
///     array_clone.add(index,1); //randomly at one to an element in the array.
/// });
/// let array = array.into_read_only(); //only returns once there is a single reference remaining on each PE
/// let sum = array.block_on(array.sum());
/// let my_sum = array.block_on(array.reduce("my_sum")); //pass a &str containing the reduction to use
/// assert_eq!(sum,my_sum);
///```
pub use lamellar_impl::register_reduction;

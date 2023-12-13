//! Memory regions are unsafe low-level abstractions around shared memory segments that have been allocated by a lamellae provider.
//!
//! These memory region APIs provide the functionality to perform RDMA operations on the shared memory segments, and are at the core
//! of how the Runtime communicates in a distributed environment (or using shared memory when using the `shmem` backend).
//!
//! # Warning
//! This is a low-level module, unless you are very comfortable/confident in low level distributed memory (and even then) it is highly recommended you use the [LamellarArrays][crate::array] and [Active Messaging][crate::active_messaging] interfaces to perform distributed communications and computation.
use crate::active_messaging::{AmDist, RemotePtr};
use crate::array::{
    LamellarArrayRdmaInput, LamellarArrayRdmaOutput, LamellarRead, LamellarWrite, TeamFrom,
};
use crate::lamellae::{AllocationType, Backend, Lamellae, LamellaeComm, LamellaeRDMA};
use crate::lamellar_team::{LamellarTeam, LamellarTeamRT};
use crate::LamellarEnv;
use core::marker::PhantomData;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[doc(hidden)]
pub mod prelude;

pub(crate) mod shared;
pub use shared::SharedMemoryRegion;

pub(crate) mod one_sided;
pub use one_sided::OneSidedMemoryRegion;

use enum_dispatch::enum_dispatch;

/// This error occurs when you are trying to directly access data locally on a PE through a memregion handle,
/// but that PE does not contain any data for that memregion
///
/// This can occur when tryin to get the local data from a [OneSidedMemoryRegion] on any PE but the one which created it.
///
/// It can also occur if a subteam creates a shared memory region, and then a PE that does not exist in the team tries to access local data directly.
///
/// In both these cases the solution would be to use the memregion handle to perfrom a `get` operation, transferring the data from a remote node into a local buffer.
#[derive(Debug, Clone)]
pub struct MemNotLocalError;

/// A Result type for LamellarMemoryRegion Operations
pub type MemResult<T> = Result<T, MemNotLocalError>;

impl std::fmt::Display for MemNotLocalError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "mem region not local",)
    }
}

impl std::error::Error for MemNotLocalError {}

/// Trait representing types that can be used in remote operations
///
/// as well as [Copy] so we can perform bitwise copies
pub trait Dist:
    AmDist + Sync + Send + Copy + serde::ser::Serialize + serde::de::DeserializeOwned + 'static
// AmDist + Copy
{
}
// impl<T: Send  + Copy + std::fmt::Debug + 'static>
//     Dist for T
// {
// }

#[doc(hidden)]
#[enum_dispatch(RegisteredMemoryRegion<T>, MemRegionId, AsBase, MemoryRegionRDMA<T>, RTMemoryRegionRDMA<T>, LamellarEnv)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[serde(bound = "T: Dist + serde::Serialize + serde::de::DeserializeOwned")]
pub enum LamellarMemoryRegion<T: Dist> {
    Shared(SharedMemoryRegion<T>),
    Local(OneSidedMemoryRegion<T>),
    // Unsafe(UnsafeArray<T>),
}

// This could be useful for if we want to transfer the actual data instead of the pointer
// impl<T: Dist + serde::Serialize> LamellarMemoryRegion<T> {
//     #[tracing::instrument(skip_all)]
//     pub(crate) fn serialize_local_data<S>(
//         mr: &LamellarMemoryRegion<T>,
//         s: S,
//     ) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer,
//     {
//         match mr {
//             LamellarMemoryRegion::Shared(mr) => mr.serialize_local_data(s),
//             LamellarMemoryRegion::Local(mr) => mr.serialize_local_data(s),
//             // LamellarMemoryRegion::Unsafe(mr) => mr.serialize_local_data(s),
//         }
//     }
// }

impl<T: Dist> crate::active_messaging::DarcSerde for LamellarMemoryRegion<T> {
    #[tracing::instrument(skip_all)]
    fn ser(&self, num_pes: usize, darcs: &mut Vec<RemotePtr>) {
        // println!("in shared ser");
        match self {
            LamellarMemoryRegion::Shared(mr) => mr.ser(num_pes, darcs),
            LamellarMemoryRegion::Local(mr) => mr.ser(num_pes, darcs),
            // LamellarMemoryRegion::Unsafe(mr) => mr.ser(num_pes,darcs),
        }
    }
    #[tracing::instrument(skip_all)]
    fn des(&self, cur_pe: Result<usize, crate::IdError>) {
        // println!("in shared des");
        match self {
            LamellarMemoryRegion::Shared(mr) => mr.des(cur_pe),
            LamellarMemoryRegion::Local(mr) => mr.des(cur_pe),
            // LamellarMemoryRegion::Unsafe(mr) => mr.des(cur_pe),
        }
        // self.mr.print();
    }
}

impl<T: Dist> LamellarMemoryRegion<T> {
    #[tracing::instrument(skip_all)]
    pub unsafe fn as_mut_slice(&self) -> MemResult<&mut [T]> {
        match self {
            LamellarMemoryRegion::Shared(memregion) => memregion.as_mut_slice(),
            LamellarMemoryRegion::Local(memregion) => memregion.as_mut_slice(),
            // LamellarMemoryRegion::Unsafe(memregion) => memregion.as_mut_slice(),
        }
    }

    #[tracing::instrument(skip_all)]
    pub unsafe fn as_slice(&self) -> MemResult<&[T]> {
        match self {
            LamellarMemoryRegion::Shared(memregion) => memregion.as_slice(),
            LamellarMemoryRegion::Local(memregion) => memregion.as_slice(),
            // LamellarMemoryRegion::Unsafe(memregion) => memregion.as_slice(),
        }
    }

    // #[tracing::instrument(skip_all)]
    // pub fn sub_region<R: std::ops::RangeBounds<usize>>(&self, range: R) -> LamellarMemoryRegion<T> {
    //     match self {
    //         LamellarMemoryRegion::Shared(memregion) => memregion.sub_region(range).into(),
    //         LamellarMemoryRegion::Local(memregion) => memregion.sub_region(range).into(),
    //         // LamellarMemoryRegion::Unsafe(memregion) => memregion.sub_region(range).into(),
    //     }
    // }
}
impl<T: Dist> SubRegion<T> for LamellarMemoryRegion<T> {
    type Region = LamellarMemoryRegion<T>;
    fn sub_region<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Region {
        match self {
            LamellarMemoryRegion::Shared(memregion) => memregion.sub_region(range).into(),
            LamellarMemoryRegion::Local(memregion) => memregion.sub_region(range).into(),
        }
    }
}

impl<T: Dist> From<LamellarArrayRdmaOutput<T>> for LamellarMemoryRegion<T> {
    #[tracing::instrument(skip_all)]
    fn from(output: LamellarArrayRdmaOutput<T>) -> Self {
        match output {
            LamellarArrayRdmaOutput::LamellarMemRegion(mr) => mr,
            LamellarArrayRdmaOutput::SharedMemRegion(mr) => mr.into(),
            LamellarArrayRdmaOutput::LocalMemRegion(mr) => mr.into(),
        }
    }
}

impl<T: Dist> From<LamellarArrayRdmaInput<T>> for LamellarMemoryRegion<T> {
    #[tracing::instrument(skip_all)]
    fn from(input: LamellarArrayRdmaInput<T>) -> Self {
        match input {
            LamellarArrayRdmaInput::LamellarMemRegion(mr) => mr,
            LamellarArrayRdmaInput::SharedMemRegion(mr) => mr.into(),
            LamellarArrayRdmaInput::LocalMemRegion(mr) => mr.into(),
        }
    }
}

impl<T: Dist> From<&LamellarMemoryRegion<T>> for LamellarArrayRdmaInput<T> {
    #[tracing::instrument(skip_all)]
    fn from(mr: &LamellarMemoryRegion<T>) -> Self {
        LamellarArrayRdmaInput::LamellarMemRegion(mr.clone())
    }
}

impl<T: Dist> TeamFrom<&LamellarMemoryRegion<T>> for LamellarArrayRdmaInput<T> {
    #[tracing::instrument(skip_all)]
    fn team_from(mr: &LamellarMemoryRegion<T>, _team: &std::pin::Pin<Arc<LamellarTeamRT>>) -> Self {
        LamellarArrayRdmaInput::LamellarMemRegion(mr.clone())
    }
}

impl<T: Dist> TeamFrom<LamellarMemoryRegion<T>> for LamellarArrayRdmaInput<T> {
    #[tracing::instrument(skip_all)]
    fn team_from(mr: LamellarMemoryRegion<T>, _team: &std::pin::Pin<Arc<LamellarTeamRT>>) -> Self {
        LamellarArrayRdmaInput::LamellarMemRegion(mr)
    }
}

impl<T: Dist> From<&LamellarMemoryRegion<T>> for LamellarArrayRdmaOutput<T> {
    #[tracing::instrument(skip_all)]
    fn from(mr: &LamellarMemoryRegion<T>) -> Self {
        LamellarArrayRdmaOutput::LamellarMemRegion(mr.clone())
    }
}

impl<T: Dist> TeamFrom<&LamellarMemoryRegion<T>> for LamellarArrayRdmaOutput<T> {
    #[tracing::instrument(skip_all)]
    fn team_from(mr: &LamellarMemoryRegion<T>, _team: &std::pin::Pin<Arc<LamellarTeamRT>>) -> Self {
        LamellarArrayRdmaOutput::LamellarMemRegion(mr.clone())
    }
}

impl<T: Dist> TeamFrom<LamellarMemoryRegion<T>> for LamellarArrayRdmaOutput<T> {
    #[tracing::instrument(skip_all)]
    fn team_from(mr: LamellarMemoryRegion<T>, _team: &std::pin::Pin<Arc<LamellarTeamRT>>) -> Self {
        LamellarArrayRdmaOutput::LamellarMemRegion(mr)
    }
}

/// An  abstraction for a memory region that has been registered with the underlying lamellae (network provider)
/// allowing for RDMA operations.
///
/// Memory Regions are low-level unsafe abstraction not really intended for use in higher-level applications
///
/// # Warning
/// Unless you are very confident in low level distributed memory access it is highly recommended you utilize the
/// [LamellarArray][crate::array::LamellarArray] interface to construct and interact with distributed memory.
#[enum_dispatch]
pub trait RegisteredMemoryRegion<T: Dist> {
    #[doc(alias("One-sided", "onesided"))]
    /// The length (in number of elements of `T`) of the local segment of the memory region (i.e. not the global length of the memory region)  
    ///
    /// # One-sided Operation
    /// the result is returned only on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1000);
    /// assert_eq!(mem_region.len(),1000);
    ///```
    fn len(&self) -> usize;
    #[doc(hidden)]
    fn addr(&self) -> MemResult<usize>;

    #[doc(alias("One-sided", "onesided"))]
    /// Return a slice of the local (to the calling PE) data of the memory region
    ///
    /// Returns an error if the PE does not contain any local data associated with this memory region
    ///
    /// # Safety
    /// this call is always unsafe as there is no gaurantee that there do not exist mutable references elsewhere in the distributed system.
    ///
    /// # One-sided Operation
    /// the result is returned only on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1000);
    /// let slice = unsafe{mem_region.as_slice().expect("PE is part of the world team")};
    ///```
    unsafe fn as_slice(&self) -> MemResult<&[T]>;

    #[doc(alias("One-sided", "onesided"))]
    /// Return a reference to the local (to the calling PE) element located by the provided index
    ///
    /// Returns an error if the index is out of bounds or the PE does not contain any local data associated with this memory region
    ///
    /// # Safety
    /// this call is always unsafe as there is no gaurantee that there do not exist mutable references elsewhere in the distributed system.
    ///
    /// # One-sided Operation
    /// the result is returned only on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1000);
    /// let val = unsafe{mem_region.at(999).expect("PE is part of the world team")};
    ///```
    unsafe fn at(&self, index: usize) -> MemResult<&T>;

    #[doc(alias("One-sided", "onesided"))]
    /// Return a mutable slice of the local (to the calling PE) data of the memory region
    ///
    /// Returns an error if the PE does not contain any local data associated with this memory region
    ///
    /// # Safety
    /// this call is always unsafe as there is no gaurantee that there do not exist other mutable references elsewhere in the distributed system.
    ///
    /// # One-sided Operation
    /// the result is returned only on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1000);
    /// let slice =unsafe { mem_region.as_mut_slice().expect("PE is part of the world team")};
    ///```
    unsafe fn as_mut_slice(&self) -> MemResult<&mut [T]>;

    #[doc(alias("One-sided", "onesided"))]
    /// Return a ptr to the local (to the calling PE) data of the memory region
    ///
    /// Returns an error if the PE does not contain any local data associated with this memory region
    ///
    /// # Safety
    /// this call is always unsafe as there is no gaurantee that there do not exist mutable references elsewhere in the distributed system.
    ///
    /// # One-sided Operation
    /// the result is returned only on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1000);
    /// let ptr = unsafe { mem_region.as_ptr().expect("PE is part of the world team")};
    ///```
    unsafe fn as_ptr(&self) -> MemResult<*const T>;

    #[doc(alias("One-sided", "onesided"))]
    /// Return a mutable ptr to the local (to the calling PE) data of the memory region
    ///
    /// Returns an error if the PE does not contain any local data associated with this memory region
    ///
    /// # Safety
    /// this call is always unsafe as there is no gaurantee that there do not exist mutable references elsewhere in the distributed system.
    ///
    /// # One-sided Operation
    /// the result is returned only on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1000);
    /// let ptr = unsafe { mem_region.as_mut_ptr().expect("PE is part of the world team")};
    ///```
    unsafe fn as_mut_ptr(&self) -> MemResult<*mut T>;
}

#[enum_dispatch]
pub(crate) trait MemRegionId {
    fn id(&self) -> usize;
}

// RegisteredMemoryRegion<T>, MemRegionId, AsBase, SubRegion<T>, MemoryRegionRDMA<T>, RTMemoryRegionRDMA<T>
// we seperate SubRegion and AsBase out as their own traits
// because we want MemRegion to impl RegisteredMemoryRegion (so that it can be used in Shared + Local)
// but MemRegion should not return LamellarMemoryRegions directly (as both SubRegion and AsBase require)
// we will implement seperate functions for MemoryRegion itself.
#[doc(hidden)]
#[enum_dispatch]
pub trait SubRegion<T: Dist> {
    type Region: RegisteredMemoryRegion<T> + MemoryRegionRDMA<T>;
    #[doc(alias("One-sided", "onesided"))]
    /// Create a sub region of this RegisteredMemoryRegion using the provided range
    ///
    /// # One-sided Operation
    /// the result is returned only on the calling PE
    ///
    /// # Panics
    /// panics if the end range is larger than the length of the memory region
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(100);
    ///
    /// let sub_region = mem_region.sub_region(30..70);
    ///```
    fn sub_region<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Region;
}

#[enum_dispatch]
pub(crate) trait AsBase {
    unsafe fn to_base<B: Dist>(self) -> LamellarMemoryRegion<B>;
}

/// The Inteface for exposing RDMA operations on a memory region. These provide the actual mechanism for performing a transfer.
#[enum_dispatch]
pub trait MemoryRegionRDMA<T: Dist> {
    #[doc(alias("One-sided", "onesided"))]
    /// "Puts" (copies) data from a local memory location into a remote memory location on the specified PE
    ///
    /// The data buffer may not be safe to upon return from this call, currently the user is responsible for completion detection,
    /// or you may use the similar blocking_put call (with a potential performance penalty);
    ///
    /// # Safety
    /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    /// Additionally, when this call returns the underlying fabric provider may or may not have already copied the data buffer
    ///
    /// # One-sided Operation
    /// the calling PE initaites the remote transfer
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let dst_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes*10);
    /// let src_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(10);
    /// unsafe{ for elem in dst_mem_region.as_mut_slice().expect("PE in world team") {*elem = num_pes;}}
    /// unsafe{ for elem in src_mem_region.as_mut_slice().expect("PE in world team") {*elem = my_pe;}}
    ///
    /// for pe in 0..num_pes{
    ///    unsafe{dst_mem_region.put(pe,my_pe*src_mem_region.len(),&src_mem_region)};
    /// }
    /// unsafe {
    ///     let dst_slice = dst_mem_region.as_slice().expect("PE in world team");
    ///     for (i,elem) in dst_slice.iter().enumerate(){
    ///         let pe = i / &src_mem_region.len();
    ///         while *elem == num_pes{
    ///             std::thread::yield_now();
    ///         }
    ///         assert_eq!(pe,*elem);
    ///     }      
    /// }
    ///```
    unsafe fn put<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, index: usize, data: U);

    #[doc(alias("One-sided", "onesided"))]
    /// Blocking "Puts" (copies) data from a local memory location into a remote memory location on the specified PE.
    ///
    /// This function blocks until the data in the data buffer has been transfered out of this PE, this does not imply that it has arrived at the remote destination though
    ///
    /// # Safety
    /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    ///
    /// # One-sided Operation
    /// the calling PE initaites the remote transfer
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let dst_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes*10);
    /// let src_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(10);
    /// unsafe{ for elem in dst_mem_region.as_mut_slice().expect("PE in world team") {*elem = num_pes;}}
    /// unsafe{ for elem in src_mem_region.as_mut_slice().expect("PE in world team") {*elem = my_pe;}}
    ///
    /// for pe in 0..num_pes{
    ///    unsafe{dst_mem_region.blocking_put(pe,my_pe*src_mem_region.len(),&src_mem_region)};
    /// }
    /// unsafe {
    ///     let dst_slice = dst_mem_region.as_slice().expect("PE in world team");
    ///     for (i,elem) in dst_slice.iter().enumerate(){
    ///         let pe = i / &src_mem_region.len();
    ///         while *elem == num_pes{
    ///             std::thread::yield_now();
    ///         }
    ///         assert_eq!(pe,*elem);
    ///     }      
    /// }
    ///```
    unsafe fn blocking_put<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    );

    /// "Puts" (copies) data from a local memory location into a remote memory location on all PEs containing the memory region
    ///
    /// This is similar to broadcast
    ///
    /// The data buffer may not be safe to upon return from this call, currently the user is responsible for completion detection
    ///
    /// # Safety
    /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    /// Additionally, when this call returns the underlying fabric provider may or may not have already copied the data buffer
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let dst_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes*10);
    /// let src_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(10);
    /// unsafe{ for elem in dst_mem_region.as_mut_slice().expect("PE in world team") {*elem = num_pes;}}
    /// unsafe{ for elem in src_mem_region.as_mut_slice().expect("PE in world team") {*elem = my_pe;}}
    ///
    /// unsafe{dst_mem_region.put_all(my_pe*src_mem_region.len(),&src_mem_region)};
    ///
    /// unsafe {
    ///     let dst_slice = dst_mem_region.as_slice().expect("PE in world team");
    ///     for (i,elem) in dst_slice.iter().enumerate(){
    ///         let pe = i / &src_mem_region.len();
    ///         while *elem == num_pes{
    ///             std::thread::yield_now();
    ///         }
    ///         assert_eq!(pe,*elem);
    ///     }      
    /// }
    ///```
    unsafe fn put_all<U: Into<LamellarMemoryRegion<T>>>(&self, index: usize, data: U);

    #[doc(alias("One-sided", "onesided"))]
    /// "Gets" (copies) data from remote memory location on the specified PE into the provided data buffer.
    /// After calling this function, the data may or may not have actually arrived into the data buffer.
    /// The user is responsible for transmission termination detection
    ///
    /// # Safety
    /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    /// Additionally, when this call returns the underlying fabric provider may or may not have already copied data into the data buffer.
    ///
    /// # One-sided Operation
    /// the calling PE initaites the remote transfer
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let src_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(10);
    /// let dst_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes*10);
    ///
    /// unsafe{ for elem in src_mem_region.as_mut_slice().expect("PE in world team") {*elem = my_pe;}}
    /// unsafe{ for elem in dst_mem_region.as_mut_slice().expect("PE in world team") {*elem = num_pes;}}
    ///
    /// for pe in 0..num_pes{
    ///     let start_i = pe*src_mem_region.len();
    ///     let end_i = start_i+src_mem_region.len();
    ///     unsafe{src_mem_region.get_unchecked(pe,0,dst_mem_region.sub_region(start_i..end_i))};
    /// }
    ///
    /// unsafe {
    ///     let dst_slice = dst_mem_region.as_slice().expect("PE in world team");
    ///     for (i,elem) in dst_slice.iter().enumerate(){
    ///         let pe = i / &src_mem_region.len();
    ///         while *elem == num_pes{
    ///             std::thread::yield_now();
    ///         }
    ///         assert_eq!(pe,*elem);
    ///     }      
    /// }
    ///```
    unsafe fn get_unchecked<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    );

    #[doc(alias("One-sided", "onesided"))]
    /// Blocking "Gets" (copies) data from remote memory location on the specified PE into the provided data buffer.
    /// After calling this function, the data is guaranteed to be placed in the data buffer
    ///
    /// # Safety
    /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    ///
    /// # One-sided Operation
    /// the calling PE initaites the remote transfer
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let src_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(10);
    /// let dst_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes*10);
    ///
    /// unsafe{ for elem in src_mem_region.as_mut_slice().expect("PE in world team") {*elem = my_pe;}}
    /// unsafe{ for elem in dst_mem_region.as_mut_slice().expect("PE in world team") {*elem = num_pes;}}
    ///
    /// for pe in 0..num_pes{
    ///     let start_i = pe*src_mem_region.len();
    ///     let end_i = start_i+src_mem_region.len();
    ///     unsafe{src_mem_region.blocking_get(pe,0,dst_mem_region.sub_region(start_i..end_i))};
    /// }
    ///
    /// unsafe {
    ///     let dst_slice = dst_mem_region.as_slice().expect("PE in world team");
    ///     for (i,elem) in dst_slice.iter().enumerate(){
    ///         let pe = i / &src_mem_region.len();
    ///         assert_eq!(pe,*elem);
    ///     }      
    /// }
    ///```
    unsafe fn blocking_get<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    );
}

#[enum_dispatch]
pub(crate) trait RTMemoryRegionRDMA<T: Dist> {
    unsafe fn put_slice(&self, pe: usize, index: usize, data: &[T]);
    unsafe fn blocking_get_slice(&self, pe: usize, index: usize, data: &mut [T]);
}

impl<T: Dist> Hash for LamellarMemoryRegion<T> {
    #[tracing::instrument(skip_all)]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state);
    }
}

impl<T: Dist> PartialEq for LamellarMemoryRegion<T> {
    #[tracing::instrument(skip_all)]
    fn eq(&self, other: &LamellarMemoryRegion<T>) -> bool {
        self.id() == other.id()
    }
}

impl<T: Dist> Eq for LamellarMemoryRegion<T> {}

impl<T: Dist> LamellarWrite for LamellarMemoryRegion<T> {}
impl<T: Dist> LamellarWrite for &LamellarMemoryRegion<T> {}
impl<T: Dist> LamellarRead for LamellarMemoryRegion<T> {}
impl<T: Dist> LamellarRead for &LamellarMemoryRegion<T> {}

#[derive(Copy, Clone)]
pub(crate) enum Mode {
    Local,
    Remote,
    Shared,
}

// this is not intended to be accessed directly by a user
// it will be wrapped in either a shared region or local region
// in shared regions its wrapped in a darc which allows us to send
// to different nodes, in local its wrapped in Arc (we dont currently support sending to other nodes)
// for local we would probably need to develop something like a one-sided initiated darc...
pub(crate) struct MemoryRegion<T: Dist> {
    addr: usize,
    pe: usize,
    size: usize,
    num_bytes: usize,
    backend: Backend,
    rdma: Arc<dyn LamellaeRDMA>,
    mode: Mode,
    phantom: PhantomData<T>,
}

impl<T: Dist> MemoryRegion<T> {
    #[tracing::instrument(skip_all)]
    pub(crate) fn new(
        size: usize, //number of elements of type T
        lamellae: Arc<Lamellae>,
        alloc: AllocationType,
    ) -> MemoryRegion<T> {
        if let Ok(memreg) = MemoryRegion::try_new(size, lamellae, alloc) {
            memreg
        } else {
            unsafe { std::ptr::null_mut::<i32>().write(1) };
            panic!("out of memory")
        }
    }
    #[tracing::instrument(skip_all)]
    pub(crate) fn try_new(
        size: usize, //number of elements of type T
        lamellae: Arc<Lamellae>,
        alloc: AllocationType,
    ) -> Result<MemoryRegion<T>, anyhow::Error> {
        // println!(
        //     "creating new lamellar memory region {:?}",
        //     size * std::mem::size_of::<T>()
        // );
        let mut mode = Mode::Shared;
        let addr = if size > 0 {
            if let AllocationType::Local = alloc {
                mode = Mode::Local;
                lamellae.rt_alloc(size * std::mem::size_of::<T>(), std::mem::align_of::<T>())?
            } else {
                lamellae.alloc(
                    size * std::mem::size_of::<T>(),
                    alloc,
                    std::mem::align_of::<T>(),
                )? //did we call team barrer before this?
            }
        } else {
            return Err(anyhow::anyhow!("cant have negative sized memregion"));
        };
        let temp = MemoryRegion {
            addr: addr,
            pe: lamellae.my_pe(),
            size: size,
            num_bytes: size * std::mem::size_of::<T>(),
            backend: lamellae.backend(),
            rdma: lamellae,
            mode: mode,
            phantom: PhantomData,
        };
        // println!(
        //     "new memregion {:x} {:x}",
        //     temp.addr,
        //     size * std::mem::size_of::<T>()
        // );
        Ok(temp)
    }
    #[tracing::instrument(skip_all)]
    pub(crate) fn from_remote_addr(
        addr: usize,
        pe: usize,
        size: usize,
        lamellae: Arc<Lamellae>,
    ) -> Result<MemoryRegion<T>, anyhow::Error> {
        Ok(MemoryRegion {
            addr: addr,
            pe: pe,
            size: size,
            num_bytes: size * std::mem::size_of::<T>(),
            backend: lamellae.backend(),
            rdma: lamellae,
            mode: Mode::Remote,
            phantom: PhantomData,
        })
    }

    #[allow(dead_code)]
    #[tracing::instrument(skip_all)]
    pub(crate) unsafe fn to_base<B: Dist>(self) -> MemoryRegion<B> {
        //this is allowed as we consume the old object..
        assert_eq!(
            self.num_bytes % std::mem::size_of::<B>(),
            0,
            "Error converting memregion to new base, does not align"
        );
        MemoryRegion {
            addr: self.addr, //TODO: out of memory...
            pe: self.pe,
            size: self.num_bytes / std::mem::size_of::<B>(),
            num_bytes: self.num_bytes,
            backend: self.backend,
            rdma: self.rdma.clone(),
            mode: self.mode,
            phantom: PhantomData,
        }
    }

    // }

    // impl<T: AmDist+ 'static> MemoryRegionRDMA<T> for MemoryRegion<T> {
    /// copy data from local memory location into a remote memory location
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - address (which is "registered" with network device) of local input buffer that will be put into the remote memory
    /// the data buffer may not be safe to upon return from this call, currently the user is responsible for completion detection,
    /// or you may use the similar iput call (with a potential performance penalty);
    #[tracing::instrument(skip_all)]
    pub(crate) unsafe fn put<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        //todo make return a result?
        let data = data.into();
        if (index + data.len()) * std::mem::size_of::<R>() <= self.num_bytes {
            let num_bytes = data.len() * std::mem::size_of::<R>();
            if let Ok(ptr) = data.as_ptr() {
                let bytes = std::slice::from_raw_parts(ptr as *const u8, num_bytes);
                self.rdma
                    .put(pe, bytes, self.addr + index * std::mem::size_of::<R>())
            } else {
                panic!("ERROR: put data src is not local");
            }
        } else {
            println!(
                "mem region bytes: {:?} sizeof elem {:?} len {:?}",
                self.num_bytes,
                std::mem::size_of::<T>(),
                self.size
            );
            println!(
                "data bytes: {:?} sizeof elem {:?} len {:?} index: {:?}",
                data.len() * std::mem::size_of::<R>(),
                std::mem::size_of::<R>(),
                data.len(),
                index
            );
            panic!("index out of bounds");
        }
    }

    /// copy data from local memory location into a remote memory localtion
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - address (which is "registered" with network device) of local input buffer that will be put into the remote memory
    /// the data buffer is free to be reused upon return of this function.
    #[tracing::instrument(skip_all)]
    pub(crate) unsafe fn blocking_put<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        //todo make return a result?
        let data = data.into();
        if (index + data.len()) * std::mem::size_of::<R>() <= self.num_bytes {
            let num_bytes = data.len() * std::mem::size_of::<R>();
            if let Ok(ptr) = data.as_ptr() {
                let bytes = std::slice::from_raw_parts(ptr as *const u8, num_bytes);
                self.rdma
                    .iput(pe, bytes, self.addr + index * std::mem::size_of::<R>())
            } else {
                panic!("ERROR: put data src is not local");
            }
        } else {
            println!("{:?} {:?} {:?}", self.size, index, data.len());
            panic!("index out of bounds");
        }
    }

    #[tracing::instrument(skip_all)]
    pub(crate) unsafe fn put_all<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        index: usize,
        data: U,
    ) {
        let data = data.into();
        if (index + data.len()) * std::mem::size_of::<R>() <= self.num_bytes {
            let num_bytes = data.len() * std::mem::size_of::<R>();
            if let Ok(ptr) = data.as_ptr() {
                let bytes = std::slice::from_raw_parts(ptr as *const u8, num_bytes);
                self.rdma
                    .put_all(bytes, self.addr + index * std::mem::size_of::<R>());
            } else {
                panic!("ERROR: put data src is not local");
            }
        } else {
            panic!("index out of bounds");
        }
    }

    //TODO: once we have a reliable asynchronos get wait mechanism, we return a request handle,
    //data probably needs to be referenced count or lifespan controlled so we know it exists when the get trys to complete
    //in the handle drop method we will wait until the request completes before dropping...  ensuring the data has a place to go
    /// copy data from remote memory location into provided data buffer
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - address (which is "registered" with network device) of destination buffer to store result of the get
    #[tracing::instrument(skip_all)]
    pub(crate) unsafe fn get_unchecked<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        let data = data.into();
        if (index + data.len()) * std::mem::size_of::<R>() <= self.num_bytes {
            let num_bytes = data.len() * std::mem::size_of::<R>();
            if let Ok(ptr) = data.as_mut_ptr() {
                let bytes = std::slice::from_raw_parts_mut(ptr as *mut u8, num_bytes);
                // println!("getting {:?} {:?} {:?} {:?} {:?} {:?} {:?}",pe,index,std::mem::size_of::<R>(),data.len(), num_bytes,self.size, self.num_bytes);
                self.rdma
                    .get(pe, self.addr + index * std::mem::size_of::<R>(), bytes);
            //(remote pe, src, dst)
            // println!("getting {:?} {:?} [{:?}] {:?} {:?} {:?}",pe,self.addr + index * std::mem::size_of::<T>(),index,data.addr(),data.len(),num_bytes);
            } else {
                panic!("ERROR: get data dst is not local");
            }
        } else {
            println!("{:?} {:?} {:?}", self.size, index, data.len(),);
            panic!("index out of bounds");
        }
    }

    /// copy data from remote memory location into provided data buffer
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - address (which is "registered" with network device) of destination buffer to store result of the get
    ///    data will be present within the buffer once this returns.
    #[tracing::instrument(skip_all)]
    pub(crate) unsafe fn blocking_get<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        let data = data.into();
        if (index + data.len()) * std::mem::size_of::<R>() <= self.num_bytes {
            let num_bytes = data.len() * std::mem::size_of::<R>();
            if let Ok(ptr) = data.as_mut_ptr() {
                let bytes = std::slice::from_raw_parts_mut(ptr as *mut u8, num_bytes);
                // println!("getting {:?} {:?} {:?} {:?} {:?} {:?} {:?}",pe,index,std::mem::size_of::<R>(),data.len(), num_bytes,self.size, self.num_bytes);
                self.rdma
                    .iget(pe, self.addr + index * std::mem::size_of::<R>(), bytes);
            //(remote pe, src, dst)
            // println!("getting {:?} {:?} [{:?}] {:?} {:?} {:?}",pe,self.addr + index * std::mem::size_of::<T>(),index,data.addr(),data.len(),num_bytes);
            } else {
                panic!("ERROR: get data dst is not local");
            }
        } else {
            println!("{:?} {:?} {:?}", self.size, index, data.len(),);
            panic!("index out of bounds");
        }
    }

    //we must ensure the the slice will live long enough and that it already exsists in registered memory
    #[tracing::instrument(skip_all)]
    pub(crate) unsafe fn put_slice<R: Dist>(&self, pe: usize, index: usize, data: &[R]) {
        //todo make return a result?
        if (index + data.len()) * std::mem::size_of::<R>() <= self.num_bytes {
            let num_bytes = data.len() * std::mem::size_of::<R>();
            let bytes = std::slice::from_raw_parts(data.as_ptr() as *const u8, num_bytes);
            // println!(
            //     "mem region len: {:?} index: {:?} data len{:?} num_bytes {:?}  from {:?} to {:x} ({:x} [{:?}])",
            //     self.size,
            //     index,
            //     data.len(),
            //     num_bytes,
            //     data.as_ptr(),
            //     self.addr,
            //     self.addr + index * std::mem::size_of::<T>(),
            //     pe,
            // );
            self.rdma
                .put(pe, bytes, self.addr + index * std::mem::size_of::<R>())
        } else {
            println!(
                "mem region len: {:?} index: {:?} data len{:?}",
                self.size,
                index,
                data.len()
            );
            panic!("index out of bounds");
        }
    }
    /// copy data from remote memory location into provided data buffer
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - address (which is "registered" with network device) of destination buffer to store result of the get
    ///    data will be present within the buffer once this returns.
    #[tracing::instrument(skip_all)]
    pub(crate) unsafe fn blocking_get_slice<R: Dist>(
        &self,
        pe: usize,
        index: usize,
        data: &mut [R],
    ) {
        // let data = data.into();
        if (index + data.len()) * std::mem::size_of::<R>() <= self.num_bytes {
            let num_bytes = data.len() * std::mem::size_of::<R>();
            let bytes = std::slice::from_raw_parts_mut(data.as_mut_ptr() as *mut u8, num_bytes);
            // println!("getting {:?} {:?} {:?} {:?} {:?} {:?} {:?}",pe,index,std::mem::size_of::<R>(),data.len(), num_bytes,self.size, self.num_bytes);

            self.rdma
                .iget(pe, self.addr + index * std::mem::size_of::<R>(), bytes);
            //(remote pe, src, dst)
            // println!("getting {:?} {:?} [{:?}] {:?} {:?} {:?}",pe,self.addr + index * std::mem::size_of::<T>(),index,data.addr(),data.len(),num_bytes);
        } else {
            println!("{:?} {:?} {:?}", self.size, index, data.len(),);
            panic!("index out of bounds");
        }
    }

    #[allow(dead_code)]
    #[tracing::instrument(skip_all)]
    pub(crate) unsafe fn fill_from_remote_addr<R: Dist>(
        &self,
        my_index: usize,
        pe: usize,
        addr: usize,
        len: usize,
    ) {
        if (my_index + len) * std::mem::size_of::<R>() <= self.num_bytes {
            let num_bytes = len * std::mem::size_of::<R>();
            let my_offset = self.addr + my_index * std::mem::size_of::<R>();
            let bytes = std::slice::from_raw_parts_mut(my_offset as *mut u8, num_bytes);
            let local_addr = self.rdma.local_addr(pe, addr);
            self.rdma.iget(pe, local_addr, bytes);
        } else {
            println!(
                "mem region len: {:?} index: {:?} data len{:?}",
                self.size, my_index, len
            );
            panic!("index out of bounds");
        }
    }

    #[allow(dead_code)]
    #[tracing::instrument(skip_all)]
    pub(crate) fn len(&self) -> usize {
        self.size
    }

    #[tracing::instrument(skip_all)]
    pub(crate) fn addr(&self) -> MemResult<usize> {
        Ok(self.addr)
    }

    #[tracing::instrument(skip_all)]
    pub(crate) fn casted_at<R: Dist>(&self, index: usize) -> MemResult<&R> {
        if self.addr != 0 {
            let num_bytes = self.size * std::mem::size_of::<T>();
            assert_eq!(
                num_bytes % std::mem::size_of::<R>(),
                0,
                "Error converting memregion to new base, does not align"
            );
            Ok(unsafe {
                &std::slice::from_raw_parts(
                    self.addr as *const R,
                    num_bytes / std::mem::size_of::<R>(),
                )[index]
            })
        } else {
            Err(MemNotLocalError {})
        }
    }

    #[tracing::instrument(skip_all)]
    pub(crate) fn as_slice(&self) -> MemResult<&[T]> {
        if self.addr != 0 {
            Ok(unsafe { std::slice::from_raw_parts(self.addr as *const T, self.size) })
        } else {
            Ok(&[])
        }
    }
    #[tracing::instrument(skip_all)]
    pub(crate) fn as_casted_slice<R: Dist>(&self) -> MemResult<&[R]> {
        if self.addr != 0 {
            let num_bytes = self.size * std::mem::size_of::<T>();
            assert_eq!(
                num_bytes % std::mem::size_of::<R>(),
                0,
                "Error converting memregion to new base, does not align"
            );
            Ok(unsafe {
                std::slice::from_raw_parts(
                    self.addr as *const R,
                    num_bytes / std::mem::size_of::<R>(),
                )
            })
        } else {
            Ok(&[])
        }
    }
    #[tracing::instrument(skip_all)]
    pub(crate) unsafe fn as_mut_slice(&self) -> MemResult<&mut [T]> {
        if self.addr != 0 {
            Ok(std::slice::from_raw_parts_mut(
                self.addr as *mut T,
                self.size,
            ))
        } else {
            Ok(&mut [])
        }
    }
    #[tracing::instrument(skip_all)]
    pub(crate) unsafe fn as_casted_mut_slice<R: Dist>(&self) -> MemResult<&mut [R]> {
        if self.addr != 0 {
            let num_bytes = self.size * std::mem::size_of::<T>();
            assert_eq!(
                num_bytes % std::mem::size_of::<R>(),
                0,
                "Error converting memregion to new base, does not align"
            );
            Ok(std::slice::from_raw_parts_mut(
                self.addr as *mut R,
                num_bytes / std::mem::size_of::<R>(),
            ))
        } else {
            Ok(&mut [])
        }
    }
    #[allow(dead_code)]
    #[tracing::instrument(skip_all)]
    pub(crate) fn as_ptr(&self) -> MemResult<*const T> {
        Ok(self.addr as *const T)
    }
    #[allow(dead_code)]
    #[tracing::instrument(skip_all)]
    pub(crate) fn as_casted_ptr<R: Dist>(&self) -> MemResult<*const R> {
        Ok(self.addr as *const R)
    }
    #[allow(dead_code)]
    #[tracing::instrument(skip_all)]
    pub(crate) fn as_mut_ptr(&self) -> MemResult<*mut T> {
        Ok(self.addr as *mut T)
    }
    #[allow(dead_code)]
    #[tracing::instrument(skip_all)]
    pub(crate) fn as_casted_mut_ptr<R: Dist>(&self) -> MemResult<*mut R> {
        Ok(self.addr as *mut R)
    }
}

impl<T: Dist> MemRegionId for MemoryRegion<T> {
    #[tracing::instrument(skip_all)]
    fn id(&self) -> usize {
        self.addr //probably should be key
    }
}

/// The interface for allocating shared and onesided memory regions
pub trait RemoteMemoryRegion {
    #[doc(alias = "Collective")]
    /// Allocate a shared memory region from the asymmetric heap.
    /// There will be `size` number of `T` elements on each PE.
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `array` to enter the call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    fn alloc_shared_mem_region<T: Dist + std::marker::Sized>(
        &self,
        size: usize,
    ) -> SharedMemoryRegion<T>;

    #[doc(alias("One-sided", "onesided"))]
    /// Allocate a one-sided memory region from the internal lamellar heap.
    /// This region only exists on the calling PE, but the returned handle can be
    /// sent to other PEs allowing remote access to the region.
    /// There will be `size` number of `T` elements on the calling PE.
    ///
    /// # One-sided Operation
    /// the calling PE will allocate the memory region locally, without intervention from the other PEs.
    ///
    fn alloc_one_sided_mem_region<T: Dist + std::marker::Sized>(
        &self,
        size: usize,
    ) -> OneSidedMemoryRegion<T>;
}

impl<T: Dist> Drop for MemoryRegion<T> {
    #[tracing::instrument(skip_all)]
    fn drop(&mut self) {
        // println!("trying to dropping mem region {:?}",self);
        if self.addr != 0 {
            match self.mode {
                Mode::Local => self.rdma.rt_free(self.addr), // - self.rdma.base_addr());
                Mode::Shared => self.rdma.free(self.addr),
                Mode::Remote => {}
            }
        }
        // println!("dropping mem region {:?}",self);
    }
}

impl<T: Dist> std::fmt::Debug for MemoryRegion<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // write!(f, "{:?}", slice)
        write!(
            f,
            "addr {:#x} size {:?} backend {:?}", // cnt: {:?}",
            self.addr,
            self.size,
            self.backend,
            // self.cnt.load(Ordering::SeqCst)
        )
    }
}

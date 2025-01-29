//! Memory regions are unsafe low-level abstractions around shared memory segments that have been allocated by a lamellae provider.
//!
//! These memory region APIs provide the functionality to perform RDMA operations on the shared memory segments, and are at the core
//! of how the Runtime communicates in a distributed environment (or using shared memory when using the `shmem` backend).
//!
//! # Warning
//! This is a low-level module, unless you are very comfortable/confident in low level distributed memory (and even then) it is highly recommended you use the [LamellarArrays][crate::array] and [Active Messaging][crate::active_messaging] interfaces to perform distributed communications and computation.
use crate::{
    active_messaging::{AMCounters, AmDist, RemotePtr},
    array::{
        LamellarArrayRdmaInput, LamellarArrayRdmaOutput, LamellarRead, LamellarWrite, TeamFrom,
        TeamTryFrom,
    },
    lamellae::{
        AllocationType, Backend, CommAlloc, CommAllocAddr, CommAllocType, CommAtomic, CommInfo,
        CommMem, CommRdma, CommSlice, Lamellae, RdmaHandle,
    },
    lamellar_team::{LamellarTeam, LamellarTeamRT},
    scheduler::Scheduler,
    LamellarEnv,
};
use core::marker::PhantomData;
use std::sync::Arc;
use std::{
    hash::{Hash, Hasher},
    pin::Pin,
};

//#[doc(hidden)]
/// Prelude for using the [LamellarMemoryRegion] module
pub mod prelude;

pub(crate) mod shared;
pub use shared::SharedMemoryRegion;

pub(crate) mod one_sided;
pub use one_sided::OneSidedMemoryRegion;

pub(crate) mod handle;
use handle::{FallibleSharedMemoryRegionHandle, SharedMemoryRegionHandle};

use enum_dispatch::enum_dispatch;
use tracing::{debug, trace};

/// This error occurs when you are trying to directly access data locally on a PE through a memregion handle,
/// but that PE does not contain any data for that memregion
///
/// This can occur when tryin to get the local data from a [OneSidedMemoryRegion] on any PE but the one which created it.
///
/// It can also occur if a subteam creates a shared memory region, and then a PE that does not exist in the team tries to access local data directly.
///
/// In both these cases the solution would be to use the memregion handle to perfrom a `get` operation, transferring the data from a remote node into a local buffer.
#[derive(Debug, Clone, Copy)]
pub enum MemRegionError {
    MemNotLocalError,
    MemNotAlignedError,
}

impl std::fmt::Display for MemRegionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            MemRegionError::MemNotLocalError => write!(
                f,
                "trying to access the local data of a mem region that is remote",
            ),
            MemRegionError::MemNotAlignedError => {
                write!(f, "trying to convert a mem region to a non aligned type",)
            }
        }
    }
}

impl std::error::Error for MemRegionError {}

/// A Result type for LamellarMemoryRegion Operations
pub type MemResult<T> = Result<T, MemRegionError>;

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

//#[doc(hidden)]
/// Enum used to expose common methods for all registered memory regions
// #[enum_dispatch(RegisteredMemoryRegion<T>, MemRegionId, AsBase, MemoryRegionRDMA<T>, RTMemoryRegionRDMA<T>, LamellarEnv)]
#[enum_dispatch(RegisteredMemoryRegion<T>, MemoryRegionRDMA<T>,RTMemoryRegionRDMA<T>,MemRegionId, AsBase,LamellarEnv)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[serde(bound = "T: Dist + serde::Serialize + serde::de::DeserializeOwned")]
pub enum LamellarMemoryRegion<T: Dist> {
    ///
    Shared(SharedMemoryRegion<T>),
    ///
    Local(OneSidedMemoryRegion<T>),
    // Unsafe(UnsafeArray<T>),
}

// This could be useful for if we want to transfer the actual data instead of the pointer
// impl<T: Dist + serde::Serialize> LamellarMemoryRegion<T> {
//     #[tracing::instrument(skip_all, level = "debug")]
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
    #[tracing::instrument(skip_all, level = "debug")]
    fn ser(&self, num_pes: usize, darcs: &mut Vec<RemotePtr>) {
        // println!("in shared ser");
        match self {
            LamellarMemoryRegion::Shared(mr) => mr.ser(num_pes, darcs),
            LamellarMemoryRegion::Local(mr) => mr.ser(num_pes, darcs),
            // LamellarMemoryRegion::Unsafe(mr) => mr.ser(num_pes,darcs),
        }
    }
    #[tracing::instrument(skip_all, level = "debug")]
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
    #[tracing::instrument(skip_all, level = "debug")]
    /// If the memory region contains local data, return it as a mutable slice
    /// else return a 0 length slice
    pub unsafe fn as_mut_slice(&self) -> &mut [T] {
        match self {
            LamellarMemoryRegion::Shared(memregion) => memregion.as_mut_slice(),
            LamellarMemoryRegion::Local(memregion) => memregion.as_mut_slice(),
            // LamellarMemoryRegion::Unsafe(memregion) => memregion.as_mut_slice(),
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    /// if the memory region contains local data, return it as a slice
    /// else return a 0 length slice
    pub unsafe fn as_slice(&self) -> &[T] {
        match self {
            LamellarMemoryRegion::Shared(memregion) => memregion.as_slice(),
            LamellarMemoryRegion::Local(memregion) => memregion.as_slice(),
            // LamellarMemoryRegion::Unsafe(memregion) => memregion.as_slice(),
        }
    }

    // #[tracing::instrument(skip_all, level = "debug")]
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
    #[tracing::instrument(skip_all, level = "debug")]
    fn from(output: LamellarArrayRdmaOutput<T>) -> Self {
        match output {
            LamellarArrayRdmaOutput::LamellarMemRegion(mr) => mr,
            LamellarArrayRdmaOutput::SharedMemRegion(mr) => mr.into(),
            LamellarArrayRdmaOutput::LocalMemRegion(mr) => mr.into(),
        }
    }
}

impl<T: Dist> From<LamellarArrayRdmaInput<T>> for LamellarMemoryRegion<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn from(input: LamellarArrayRdmaInput<T>) -> Self {
        match input {
            LamellarArrayRdmaInput::LamellarMemRegion(mr) => mr,
            LamellarArrayRdmaInput::SharedMemRegion(mr) => mr.into(),
            LamellarArrayRdmaInput::LocalMemRegion(mr) => mr.into(),
        }
    }
}

impl<T: Dist> From<&LamellarMemoryRegion<T>> for LamellarArrayRdmaInput<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn from(mr: &LamellarMemoryRegion<T>) -> Self {
        LamellarArrayRdmaInput::LamellarMemRegion(mr.clone())
    }
}

impl<T: Dist> TeamFrom<&LamellarMemoryRegion<T>> for LamellarArrayRdmaInput<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn team_from(mr: &LamellarMemoryRegion<T>, _team: &Arc<LamellarTeam>) -> Self {
        LamellarArrayRdmaInput::LamellarMemRegion(mr.clone())
    }
}

impl<T: Dist> TeamFrom<LamellarMemoryRegion<T>> for LamellarArrayRdmaInput<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn team_from(mr: LamellarMemoryRegion<T>, _team: &Arc<LamellarTeam>) -> Self {
        LamellarArrayRdmaInput::LamellarMemRegion(mr)
    }
}

impl<T: Dist> TeamTryFrom<&LamellarMemoryRegion<T>> for LamellarArrayRdmaInput<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn team_try_from(
        mr: &LamellarMemoryRegion<T>,
        _team: &Arc<LamellarTeam>,
    ) -> Result<Self, anyhow::Error> {
        Ok(LamellarArrayRdmaInput::LamellarMemRegion(mr.clone()))
    }
}

impl<T: Dist> TeamTryFrom<LamellarMemoryRegion<T>> for LamellarArrayRdmaInput<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn team_try_from(
        mr: LamellarMemoryRegion<T>,
        _team: &Arc<LamellarTeam>,
    ) -> Result<Self, anyhow::Error> {
        Ok(LamellarArrayRdmaInput::LamellarMemRegion(mr))
    }
}

impl<T: Dist> From<&LamellarMemoryRegion<T>> for LamellarArrayRdmaOutput<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn from(mr: &LamellarMemoryRegion<T>) -> Self {
        LamellarArrayRdmaOutput::LamellarMemRegion(mr.clone())
    }
}

impl<T: Dist> TeamFrom<&LamellarMemoryRegion<T>> for LamellarArrayRdmaOutput<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn team_from(mr: &LamellarMemoryRegion<T>, _team: &Arc<LamellarTeam>) -> Self {
        LamellarArrayRdmaOutput::LamellarMemRegion(mr.clone())
    }
}

impl<T: Dist> TeamFrom<LamellarMemoryRegion<T>> for LamellarArrayRdmaOutput<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn team_from(mr: LamellarMemoryRegion<T>, _team: &Arc<LamellarTeam>) -> Self {
        LamellarArrayRdmaOutput::LamellarMemRegion(mr)
    }
}

impl<T: Dist> TeamTryFrom<&LamellarMemoryRegion<T>> for LamellarArrayRdmaOutput<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn team_try_from(
        mr: &LamellarMemoryRegion<T>,
        _team: &Arc<LamellarTeam>,
    ) -> Result<Self, anyhow::Error> {
        Ok(LamellarArrayRdmaOutput::LamellarMemRegion(mr.clone()))
    }
}

impl<T: Dist> TeamTryFrom<LamellarMemoryRegion<T>> for LamellarArrayRdmaOutput<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn team_try_from(
        mr: LamellarMemoryRegion<T>,
        _team: &Arc<LamellarTeam>,
    ) -> Result<Self, anyhow::Error> {
        Ok(LamellarArrayRdmaOutput::LamellarMemRegion(mr))
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
pub(crate) trait RegisteredMemoryRegion<T: Dist> {
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
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1000).block();
    /// assert_eq!(mem_region.len(),1000);
    ///```
    fn len(&self) -> usize;

    //TODO: move this function to a private trait or private method
    #[doc(hidden)]
    fn addr(&self) -> MemResult<usize>;

    #[doc(alias("One-sided", "onesided"))]
    /// Return a slice of the local (to the calling PE) data of the memory region
    ///
    /// Returns a 0-length slice if the PE does not contain any local data associated with this memory region
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
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1000).block();
    /// let slice = unsafe{mem_region.as_slice().expect("PE is part of the world team")};
    ///```
    unsafe fn as_slice(&self) -> &[T];

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
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1000).block();
    /// let val = unsafe{mem_region.at(999).expect("PE is part of the world team")};
    ///```
    unsafe fn at(&self, index: usize) -> MemResult<&T>;

    #[doc(alias("One-sided", "onesided"))]
    /// Return a mutable slice of the local (to the calling PE) data of the memory region
    ///
    /// Returns a 0-length slice if the PE does not contain any local data associated with this memory region
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
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1000).block();
    /// let slice =unsafe { mem_region.as_mut_slice().expect("PE is part of the world team")};
    ///```
    unsafe fn as_mut_slice(&self) -> &mut [T];

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
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1000).block();
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
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1000).block();
    /// let ptr = unsafe { mem_region.as_mut_ptr().expect("PE is part of the world team")};
    ///```
    unsafe fn as_mut_ptr(&self) -> MemResult<*mut T>;
    unsafe fn as_comm_slice(&self) -> MemResult<CommSlice<T>>;
    unsafe fn comm_addr(&self) -> MemResult<CommAllocAddr>;
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
//#[doc(hidden)]

/// Trait for creating subregions of a memory region
pub trait SubRegion<T: Dist> {
    #[doc(hidden)]
    type Region:  MemoryRegionRDMA<T>;
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
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(100).block();
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
    /// let dst_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes*10).block();
    /// let src_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(10).block();
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
    unsafe fn put<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) -> RdmaHandle<T>;

    // #[doc(alias("One-sided", "onesided"))]
    // /// Blocking "Puts" (copies) data from a local memory location into a remote memory location on the specified PE.
    // ///
    // /// This function blocks until the data in the data buffer has been transfered out of this PE, this does not imply that it has arrived at the remote destination though
    // ///
    // /// # Safety
    // /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    // ///
    // /// # One-sided Operation
    // /// the calling PE initaites the remote transfer
    // ///
    // /// # Examples
    // ///```
    // /// use lamellar::memregion::prelude::*;
    // ///
    // /// let world = LamellarWorldBuilder::new().build();
    // /// let my_pe = world.my_pe();
    // /// let num_pes = world.num_pes();
    // ///
    // /// let dst_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes*10).block();
    // /// let src_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(10).block();
    // /// unsafe{ for elem in dst_mem_region.as_mut_slice().expect("PE in world team") {*elem = num_pes;}}
    // /// unsafe{ for elem in src_mem_region.as_mut_slice().expect("PE in world team") {*elem = my_pe;}}
    // ///
    // /// for pe in 0..num_pes{
    // ///    unsafe{dst_mem_region.blocking_put(pe,my_pe*src_mem_region.len(),&src_mem_region)};
    // /// }
    // /// unsafe {
    // ///     let dst_slice = dst_mem_region.as_slice().expect("PE in world team");
    // ///     for (i,elem) in dst_slice.iter().enumerate(){
    // ///         let pe = i / &src_mem_region.len();
    // ///         while *elem == num_pes{
    // ///             std::thread::yield_now();
    // ///         }
    // ///         assert_eq!(pe,*elem);
    // ///     }
    // /// }
    // ///```
    // unsafe fn blocking_put<U: Into<LamellarMemoryRegion<T>>>(
    //     &self,
    //     pe: usize,
    //     index: usize,
    //     data: U,
    // );

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
    /// let dst_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes*10).block();
    /// let src_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(10).block();
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
    unsafe fn put_all<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        index: usize,
        data: U,
    ) -> RdmaHandle<T>;

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
    /// let src_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(10).block();
    /// let dst_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes*10).block();
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
    ) -> RdmaHandle<T>;

    // #[doc(alias("One-sided", "onesided"))]
    // /// Blocking "Gets" (copies) data from remote memory location on the specified PE into the provided data buffer.
    // /// After calling this function, the data is guaranteed to be placed in the data buffer
    // ///
    // /// # Safety
    // /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    // ///
    // /// # One-sided Operation
    // /// the calling PE initaites the remote transfer
    // ///
    // /// # Examples
    // ///```
    // /// use lamellar::memregion::prelude::*;
    // ///
    // /// let world = LamellarWorldBuilder::new().build();
    // /// let my_pe = world.my_pe();
    // /// let num_pes = world.num_pes();
    // ///
    // /// let src_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(10).block();
    // /// let dst_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes*10).block();
    // ///
    // /// unsafe{ for elem in src_mem_region.as_mut_slice().expect("PE in world team") {*elem = my_pe;}}
    // /// unsafe{ for elem in dst_mem_region.as_mut_slice().expect("PE in world team") {*elem = num_pes;}}
    // ///
    // /// for pe in 0..num_pes{
    // ///     let start_i = pe*src_mem_region.len();
    // ///     let end_i = start_i+src_mem_region.len();
    // ///     unsafe{src_mem_region.blocking_get(pe,0,dst_mem_region.sub_region(start_i..end_i))};
    // /// }
    // ///
    // /// unsafe {
    // ///     let dst_slice = dst_mem_region.as_slice().expect("PE in world team");
    // ///     for (i,elem) in dst_slice.iter().enumerate(){
    // ///         let pe = i / &src_mem_region.len();
    // ///         assert_eq!(pe,*elem);
    // ///     }
    // /// }
    // ///```
    // unsafe fn blocking_get<U: Into<LamellarMemoryRegion<T>>>(
    //     &self,
    //     pe: usize,
    //     index: usize,
    //     data: U,
    // );
}

#[enum_dispatch]
pub(crate) trait RTMemoryRegionRDMA<T: Dist> {
    unsafe fn put_comm_slice(&self, pe: usize, index: usize, data: CommSlice<T>) -> RdmaHandle<T>;
    unsafe fn get_comm_slice(&self, pe: usize, index: usize, data: CommSlice<T>) -> RdmaHandle<T>;
}

impl<T: Dist> Hash for LamellarMemoryRegion<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state);
    }
}

impl<T: Dist> PartialEq for LamellarMemoryRegion<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn eq(&self, other: &LamellarMemoryRegion<T>) -> bool {
        self.id() == other.id()
    }
}

impl<T: Dist> Eq for LamellarMemoryRegion<T> {}

impl<T: Dist> LamellarWrite for LamellarMemoryRegion<T> {}
impl<T: Dist> LamellarWrite for &LamellarMemoryRegion<T> {}
impl<T: Dist> LamellarRead for LamellarMemoryRegion<T> {}
impl<T: Dist> LamellarRead for &LamellarMemoryRegion<T> {}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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
    alloc: CommAlloc,
    num_elems: usize,
    pe: usize,
    backend: Backend,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    rdma: Arc<Lamellae>,
    mode: Mode,
    phantom: PhantomData<T>,
}

impl<T: Dist> MemoryRegion<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new(
        num_elems: usize, //number of elements of type T
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        lamellae: &Arc<Lamellae>,
        alloc: AllocationType,
    ) -> MemoryRegion<T> {
        if let Ok(memreg) = MemoryRegion::try_new(num_elems, scheduler, counters, lamellae, alloc) {
            memreg
        } else {
            unsafe { std::ptr::null_mut::<i32>().write(1) };
            panic!("out of memory")
        }
    }
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn try_new(
        num_elems: usize, //number of elements of type T
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        lamellae: &Arc<Lamellae>,
        alloc: AllocationType,
    ) -> Result<MemoryRegion<T>, anyhow::Error> {
        // println!(
        //     "creating new lamellar memory region size: {:?} align: {:?}",
        //     size * std::mem::size_of::<T>(),
        //     std::mem::align_of::<T>()
        // );
        let mut mode = Mode::Shared;
        let alloc = if num_elems > 0 {
            if let AllocationType::Local = alloc {
                mode = Mode::Local;
                lamellae.comm().rt_alloc(
                    num_elems * std::mem::size_of::<T>(),
                    std::mem::align_of::<T>(),
                )?
            } else {
                lamellae.comm().alloc(
                    num_elems * std::mem::size_of::<T>(),
                    alloc,
                    std::mem::align_of::<T>(),
                )
                .block()? //did we call team barrer before this?
            }
        } else {
            println!(
                "cant have zero sized memregion {:?}",
                std::backtrace::Backtrace::capture()
            );
            panic!("cant have zero sized memregion");
            // return Err(anyhow::anyhow!("cant have negative sized memregion"));
        };
        let temp = MemoryRegion {
            alloc,
            pe: lamellae.comm().my_pe(),
            num_elems,
            scheduler: scheduler.clone(),
            counters: counters,
            backend: lamellae.comm().backend(),
            rdma: lamellae.clone(),
            mode: mode,
            phantom: PhantomData,
        };
        trace!(
            "new memregion alloc {:?}",
            temp.alloc,
        );
        Ok(temp)
    }
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn from_remote_addr(
        addr: CommAllocAddr,
        pe: usize,
        num_elems: usize,
        team: Pin<Arc<LamellarTeamRT>>,
        lamellae: Arc<Lamellae>,
    ) -> Result<MemoryRegion<T>, anyhow::Error> {
        Ok(MemoryRegion {
            alloc: CommAlloc {
                addr: addr.into(),
                size: num_elems * std::mem::size_of::<T>(),
                alloc_type: CommAllocType::Remote,
            },
            pe: pe,
            num_elems,
            scheduler: team.scheduler.clone(),
            counters: team.counters(),
            backend: lamellae.comm().backend(),
            rdma: lamellae,
            mode: Mode::Remote,
            phantom: PhantomData,
        })
    }

    #[allow(dead_code)]
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) unsafe fn to_base<B: Dist>(mut self) -> MemoryRegion<B> {
        //this is allowed as we consume the old object..
        assert_eq!(
            self.alloc.size % std::mem::size_of::<B>(),
            0,
            "Error converting memregion to new base, does not align"
        );
        // MemoryRegion {
        //     addr: self.addr, //TODO: out of memory...
        //     pe: self.pe,
        //     size: self.num_bytes / std::mem::size_of::<B>(),
        //     num_bytes: self.num_bytes,
        //     backend: self.backend,
        //     rdma: self.rdma.comm().clone(),
        //     mode: self.mode,
        //     phantom: PhantomData,
        // }
        self.num_elems = self.alloc.size / std::mem::size_of::<B>();
        std::mem::transmute(self) //we do this because other wise self gets dropped and frees the underlying data (we could also set addr to 0 in self)
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
    #[tracing::instrument(skip(self,data), level = "debug")]
    pub(crate) unsafe fn put<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) -> RdmaHandle<R> {
        trace!("put memregion {:?} index: {:?}", self.alloc, index);
        let data = data.into();
        if (index + data.len()) * std::mem::size_of::<R>() <= self.alloc.size && data.len() > 0 {
            if let Ok(data_slice) = data.as_comm_slice() {
                self.rdma.comm().put(
                    &self.scheduler,
                    self.counters.clone(),
                    pe,
                    data_slice,
                    self.alloc.comm_addr() + index * std::mem::size_of::<R>(),
                )
            } else {
                panic!("ERROR: put data src is not local");
            }
        } else {
            println!(
                "mem region bytes: {:?} sizeof elem {:?} len {:?}",
                self.alloc.size,
                std::mem::size_of::<T>(),
                self.num_elems
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

    // /// copy data from local memory location into a remote memory localtion
    // ///
    // /// # Arguments
    // ///
    // /// * `pe` - id of remote PE to grab data from
    // /// * `index` - offset into the remote memory window
    // /// * `data` - address (which is "registered" with network device) of local input buffer that will be put into the remote memory
    // /// the data buffer is free to be reused upon return of this function.
    // #[tracing::instrument(skip_all, level = "debug")]
    // pub(crate) unsafe fn blocking_put<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
    //     &self,
    //     pe: usize,
    //     index: usize,
    //     data: U,
    // ) {
    //     //todo make return a result?
    //     let data = data.into();
    //     if (index + data.len()) * std::mem::size_of::<R>() <= self.num_bytes {
    //         let num_bytes = data.len() * std::mem::size_of::<R>();
    //         if let Ok(ptr) = data.as_ptr() {
    //             let bytes = std::slice::from_raw_parts(ptr as *const u8, num_bytes);
    //             self.rdma
    //                 .comm().iput(pe, bytes, self.addr + index * std::mem::size_of::<R>())
    //         } else {
    //             panic!("ERROR: put data src is not local");
    //         }
    //     } else {
    //         println!("{:?} {:?} {:?}", self.size, index, data.len());
    //         panic!("index out of bounds");
    //     }
    // }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) unsafe fn put_all<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        index: usize,
        data: U,
    ) -> RdmaHandle<R> {
        let data = data.into();
        if (index + data.len()) * std::mem::size_of::<R>() <= self.alloc.size && data.len() > 0 {
            if let Ok(data_slice) = data.as_comm_slice() {
                self.rdma.comm().put_all(
                    &self.scheduler,
                    self.counters.clone(),
                    data_slice,
                    self.alloc.comm_addr() + index * std::mem::size_of::<R>(),
                )
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
    #[tracing::instrument(skip(self, data), level = "debug")]
    pub(crate) unsafe fn get_unchecked<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) -> RdmaHandle<R> {
        trace!("get memregion {:?} index: {:?}", self.alloc, index);
        let data = data.into();
        if (index + data.len()) * std::mem::size_of::<R>() <= self.alloc.size && data.len() > 0 {
            if let Ok(data_slice) = data.as_comm_slice() {
                self.rdma.comm().get(
                    &self.scheduler,
                    self.counters.clone(),
                    pe,
                    self.alloc.comm_addr() + index * std::mem::size_of::<R>(),
                    data_slice,
                )
            } else {
                panic!("ERROR: get data dst is not local");
            }
        } else {
            println!("{:?} {:?} {:?}", self.alloc.size, index, data.len(),);
            panic!("index out of bounds");
        }
    }

    // /// copy data from remote memory location into provided data buffer
    // ///
    // /// # Arguments
    // ///
    // /// * `pe` - id of remote PE to grab data from
    // /// * `index` - offset into the remote memory window
    // /// * `data` - address (which is "registered" with network device) of destination buffer to store result of the get
    // ///    data will be present within the buffer once this returns.
    // #[tracing::instrument(skip_all, level = "debug")]
    // pub(crate) unsafe fn blocking_get<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
    //     &self,
    //     pe: usize,
    //     index: usize,
    //     data: U,
    // ) {
    //     let data = data.into();
    //     if (index + data.len()) * std::mem::size_of::<R>() <= self.num_bytes {
    //         let num_bytes = data.len() * std::mem::size_of::<R>();
    //         if let Ok(ptr) = data.as_mut_ptr() {
    //             let bytes = std::slice::from_raw_parts_mut(ptr as *mut u8, num_bytes);
    //             // println!(
    //             //     "getting {:?} {:?} {:?} {:?} {:?} {:?} {:?}",
    //             //     pe,
    //             //     index,
    //             //     std::mem::size_of::<R>(),
    //             //     data.len(),
    //             //     num_bytes,
    //             //     self.size,
    //             //     self.num_bytes
    //             // );
    //             self.rdma
    //                 .comm().iget(pe, self.addr + index * std::mem::size_of::<R>(), bytes);
    //         //(remote pe, src, dst)
    //         // println!("getting {:?} {:?} [{:?}] {:?} {:?} {:?}",pe,self.addr + index * std::mem::size_of::<T>(),index,data.addr(),data.len(),num_bytes);
    //         } else {
    //             panic!("ERROR: get data dst is not local");
    //         }
    //     } else {
    //         println!("{:?} {:?} {:?}", self.size, index, data.len(),);
    //         panic!("index out of bounds");
    //     }
    // }

    //we must ensure the the slice will live long enough and that it already exsists in registered memory
    #[tracing::instrument(skip(self, data), level = "debug")]
    pub(crate) unsafe fn put_comm_slice<R: Dist>(
        &self,
        pe: usize,
        index: usize,
        data: CommSlice<R>,
    ) -> RdmaHandle<R> {
        trace!(
            "put commslice memregion {:?} index: {:?} {:?} {:?}",
            self.alloc,
            index,
            data.addr,
            data.len()
        );
        if (index + data.len()) * std::mem::size_of::<R>() <= self.alloc.size && data.len() > 0 {
            // let num_bytes = data.len() * std::mem::size_of::<R>();
            // let bytes = std::slice::from_raw_parts(data.as_ptr() as *const u8, num_bytes);
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
            self.rdma.comm().put(
                &self.scheduler,
                self.counters.clone(),
                pe,
                data,
                self.alloc.comm_addr() + index * std::mem::size_of::<R>(),
            )
        } else {
            println!(
                "mem region len: {:?} index: {:?} data len{:?}",
                self.num_elems,
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
    // #[tracing::instrument(skip_all)]
    pub(crate) unsafe fn get_comm_slice<R: Dist>(
        &self,
        pe: usize,
        index: usize,
        data: CommSlice<R>,
    ) -> RdmaHandle<R> {
        // let data = data.into();
        if (index + data.len()) * std::mem::size_of::<R>() <= self.alloc.size && data.len() > 0 {
            // let num_bytes = data.len() * std::mem::size_of::<R>();
            // let bytes = std::slice::from_raw_parts_mut(data.as_mut_ptr() as *mut u8, num_bytes);
            // println!("getting {:?} {:?} {:?} {:?} {:?} {:?} {:?}",pe,index,std::mem::size_of::<R>(),data.len(), num_bytes,self.size, self.num_bytes);

            self.rdma.comm().get(
                &self.scheduler,
                self.counters.clone(),
                pe,
                self.alloc.comm_addr() + index * std::mem::size_of::<R>(),
                data,
            )
            //(remote pe, src, dst)
            // println!("getting {:?} {:?} [{:?}] {:?} {:?} {:?}",pe,self.addr + index * std::mem::size_of::<T>(),index,data.addr(),data.len(),num_bytes);
        } else {
            println!("{:?} {:?} {:?}", self.alloc.size, index, data.len(),);
            panic!("index out of bounds");
        }
    }

    /// atomically (element-wise)copy data from local memory location into a remote memory localtion
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - address (which is "registered" with network device) of local input buffer that will be put into the remote memory
    /// the data buffer is free to be reused upon return of this function.
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) unsafe fn atomic_store<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        // //todo make return a result?
        // let data = data.into();
        // if (index + data.len()) * std::mem::size_of::<R>() <= self.num_bytes {
        //     self.rdma.comm().atomic_op(
        //         AtomcicOp::Store,
        //         pe,
        //         data.as_slice(),
        //         self.addr + index * std::mem::size_of::<R>(),
        //     )
        // } else {
        //     println!("{:?} {:?} {:?}", self.size, index, data.len());
        //     panic!("index out of bounds");
        // }
    }

    /// atomically (element-wise)copy data from local memory location into a remote memory localtion
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - address (which is "registered" with network device) of local input buffer that will be put into the remote memory
    /// the data buffer is free to be reused upon return of this function.
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) unsafe fn iatomic_store<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        //todo make return a result?
        // let data = data.into();
        // if (index + data.len()) * std::mem::size_of::<R>() <= self.num_bytes {
        //     self.rdma.comm().atomic_store(
        //         pe,
        //         data.as_slice(),
        //         self.addr + index * std::mem::size_of::<R>(),
        //     )
        // } else {
        //     println!("{:?} {:?} {:?}", self.size, index, data.len());
        //     panic!("index out of bounds");
        // }
    }

    /// atomically(element-wise) copy data from remote memory location into provided data buffer
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - address (which is "registered" with network device) of destination buffer to store result of the get
    ///    data will be present within the buffer once this returns.
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn atomic_load<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        // let data = data.into();
        // if (index + data.len()) * std::mem::size_of::<R>() <= self.num_bytes {
        //     self.rdma
        //         .atomic_load(pe, self.addr + index * std::mem::size_of::<R>(), unsafe {
        //             data.as_mut_slice()
        //         })
        // } else {
        //     println!("{:?} {:?} {:?}", self.size, index, data.len());
        //     panic!("index out of bounds");
        // }
    }

    /// atomically(element-wise) copy data from remote memory location into provided data buffer
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - address (which is "registered" with network device) of destination buffer to store result of the get
    ///    data will be present within the buffer once this returns.
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn iatomic_load<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        // let data = data.into();
        // if (index + data.len()) * std::mem::size_of::<R>() <= self.num_bytes {
        //     self.rdma
        //         .comm().iatomic_load(pe, self.addr + index * std::mem::size_of::<R>(), unsafe {
        //             data.as_mut_slice()
        //         })
        // } else {
        //     println!("{:?} {:?} {:?}", self.size, index, data.len());
        //     panic!("index out of bounds");
        // }
    }

    /// atomically (element-wise)copy data from local memory location into a remote memory localtion
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - address (which is "registered" with network device) of local input buffer that will be put into the remote memory
    /// the data buffer is free to be reused upon return of this function.
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) unsafe fn atomic_swap<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        pe: usize,
        index: usize,
        operand: U,
        result: U,
    ) {
        //todo make return a result?
        // let operand = operand.into();
        // let result = result.into();
        // if (index + operand.len()) * std::mem::size_of::<R>() <= self.num_bytes {
        //     self.rdma.comm().atomic_swap(
        //         pe,
        //         operand.as_slice(),
        //         self.addr + index * std::mem::size_of::<R>(),
        //         result.as_mut_slice(),
        //     )
        // } else {
        //     println!("{:?} {:?} {:?}", self.size, index, result.len());
        //     panic!("index out of bounds");
        // }
    }

    /// atomically (element-wise)copy data from local memory location into a remote memory localtion
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - address (which is "registered" with network device) of local input buffer that will be put into the remote memory
    /// the data buffer is free to be reused upon return of this function.
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) unsafe fn iatomic_swap<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        pe: usize,
        index: usize,
        operand: U,
        result: U,
    ) {
        //todo make return a result?
        // let operand = operand.into();
        // let result = result.into();
        // if (index + operand.len()) * std::mem::size_of::<R>() <= self.num_bytes {
        //     self.rdma.comm().iatomic_swap(
        //         pe,
        //         operand.as_slice(),
        //         self.addr() + index * std::mem::size_of::<R>(),
        //         result.as_mut_slice(),
        //     )
        // } else {
        //     println!("{:?} {:?} {:?}", self.alloc.size, index, result.len());
        //     panic!("index out of bounds");
        // }
    }

    // #[allow(dead_code)]
    #[tracing::instrument(skip_all, level = "debug")]
    // pub(crate) unsafe fn fill_from_remote_addr<R: Dist>(
    //     &self,
    //     my_index: usize,
    //     pe: usize,
    //     addr: usize,
    //     len: usize,
    // ) {
    //     if (my_index + len) * std::mem::size_of::<R>() <= self.num_bytes {
    //         let num_bytes = len * std::mem::size_of::<R>();
    //         let my_offset = self.addr + my_index * std::mem::size_of::<R>();
    //         let bytes = std::slice::from_raw_parts_mut(my_offset as *mut u8, num_bytes);
    //         let local_addr = self.rdma.comm().local_addr(pe, addr);
    //         self.rdma.comm().iget(pe, local_addr, bytes);
    //     } else {
    //         println!(
    //             "mem region len: {:?} index: {:?} data len{:?}",
    //             self.size, my_index, len
    //         );
    //         panic!("index out of bounds");
    //     }
    // }

    #[allow(dead_code)]
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn len(&self) -> usize {
        self.alloc.size
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn addr(&self) -> MemResult<usize> {
        if self.mode == Mode::Remote {
            return Err(MemRegionError::MemNotLocalError);
        }
        Ok(self.alloc.addr)
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn casted_at<R: Dist>(&self, index: usize) -> MemResult<&R> {
        if self.mode == Mode::Remote {
            return Err(MemRegionError::MemNotLocalError);
        }
        let num_bytes = self.alloc.size * std::mem::size_of::<T>();
        assert_eq!(
            num_bytes % std::mem::size_of::<R>(),
            0,
            "Error converting memregion to new base, does not align"
        );
        Ok(unsafe {
            &std::slice::from_raw_parts(
                self.alloc.addr as *const R,
                num_bytes / std::mem::size_of::<R>(),
            )[index]
        })
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn as_slice(&self) -> &[T] {
        unsafe { self.as_mut_slice() }
    }
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn as_casted_slice<R: Dist>(&self) -> MemResult<&[R]> {
        unsafe { Ok(self.as_casted_mut_slice()?) }
    }
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) unsafe fn as_mut_slice(&self) -> &mut [T] {
        if self.mode == Mode::Remote {
            return &mut [];
        }
        trace!("alloc {:?} len {:?} alighnment {:?} is address alligned {:?}" , self.alloc,self.alloc.size / std::mem::size_of::<T>(), std::mem::align_of::<T>(), self.alloc.addr % std::mem::align_of::<T>());
        std::slice::from_raw_parts_mut(
            self.alloc.addr as *mut T,
            self.alloc.size / std::mem::size_of::<T>(),
        )
    }
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) unsafe fn as_casted_mut_slice<R: Dist>(&self) -> MemResult<&mut [R]> {
        if self.mode == Mode::Remote {
            return Ok(&mut []);
        }
        if self.alloc.size % std::mem::size_of::<R>() != 0 {
            return Err(MemRegionError::MemNotAlignedError);
        }
        Ok(std::slice::from_raw_parts_mut(
            self.alloc.addr as *mut R,
            self.alloc.size / std::mem::size_of::<R>(),
        ))
    }
    // #[allow(dead_code)]
    // #[tracing::instrument(skip_all, level = "debug")]
    // pub(crate) fn as_ptr(&self) -> MemResult<*const T> {
    //     Ok(self.addr as *const T)
    // }
    // #[allow(dead_code)]
    // #[tracing::instrument(skip_all, level = "debug")]
    // pub(crate) fn as_casted_ptr<R: Dist>(&self) -> MemResult<*const R> {
    //     Ok(self.addr as *const R)
    // }
    // #[allow(dead_code)]
    // #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn as_mut_ptr(&self) -> MemResult<*mut T> {
        if self.mode == Mode::Remote {
            return Err(MemRegionError::MemNotLocalError);
        }
        unsafe { Ok(self.alloc.as_mut_ptr()) }
    }
    // #[allow(dead_code)]
    // #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn as_casted_mut_ptr<R: Dist>(&self) -> MemResult<*mut R> {
        if self.mode == Mode::Remote {
            return Err(MemRegionError::MemNotLocalError);
        }
        unsafe { Ok(self.alloc.as_mut_ptr()) }
    }

    pub(crate) unsafe fn as_comm_slice(&self) -> MemResult<CommSlice<T>> {
        if self.mode == Mode::Remote {
            return Err(MemRegionError::MemNotLocalError);
        }
        Ok(self.alloc.as_slice())
    }
    pub(crate) unsafe fn as_casted_comm_slice<R: Dist>(&self) -> MemResult<CommSlice<R>> {
        if self.mode == Mode::Remote {
            return Err(MemRegionError::MemNotLocalError);
        }
        Ok(self.alloc.as_slice())
    }
    pub(crate) unsafe fn comm_addr(&self) -> MemResult<CommAllocAddr> {
        if self.mode == Mode::Remote {
            return Err(MemRegionError::MemNotLocalError);
        }
        Ok(self.alloc.comm_addr())
    }
}

impl<T: Dist> MemRegionId for MemoryRegion<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn id(&self) -> usize {
        self.alloc.addr //probably should be key
    }
}

/// The interface for allocating shared and onesided memory regions
pub trait RemoteMemoryRegion {
    #[doc(alias = "Collective")]
    /// Allocate a shared memory region from the asymmetric heap.
    /// There will be `size` number of `T` elements on each PE.
    ///
    /// Note: If there is not enough memory in the lamellar heap on the calling PE
    /// this call will trigger a "heap grow" operation (initiated and handled by the runtime),
    /// this behavior can be disabled by setting the env variable "LAMELLAR_HEAP_MODE=static",
    /// in which case this call will cause a panic if there is not enough memory.
    ///
    /// Alternatively, you can use the `try_alloc_shared_mem_region` method which returns
    /// a `Result` and allows you to handle the error case when there is not enough memory.
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `array` to enter the call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    fn alloc_shared_mem_region<T: Dist + std::marker::Sized>(
        &self,
        size: usize,
    ) -> SharedMemoryRegionHandle<T>;

    #[doc(alias = "Collective")]
    /// Allocate a shared memory region from the asymmetric heap.
    /// There will be `size` number of `T` elements on each PE.
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `array` to enter the call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    fn try_alloc_shared_mem_region<T: Dist + std::marker::Sized>(
        &self,
        size: usize,
    ) -> FallibleSharedMemoryRegionHandle<T>;

    #[doc(alias("One-sided", "onesided"))]
    /// Allocate a one-sided memory region from the internal lamellar heap.
    /// This region only exists on the calling PE, but the returned handle can be
    /// sent to other PEs allowing remote access to the region.
    /// There will be `size` number of `T` elements on the calling PE.
    ///
    /// Note: If there is not enough memory in the lamellar heap on the calling PE
    /// this call will trigger a "heap grow" operation (initiated and handled by the runtime),
    /// this behavior can be disabled by setting the env variable "LAMELLAR_HEAP_MODE=static",
    /// in which case this call will cause a panic if there is not enough memory.
    ///
    /// Alternatively, you can use the `try_alloc_one_sided_mem_region` method which returns
    /// a `Result` and allows you to handle the error case when there is not enough memory.
    ///
    /// # One-sided Operation
    /// the calling PE will allocate the memory region locally, without intervention from the other PEs.
    ///
    fn alloc_one_sided_mem_region<T: Dist + std::marker::Sized>(
        &self,
        size: usize,
    ) -> OneSidedMemoryRegion<T>;

    #[doc(alias("One-sided", "onesided"))]
    /// Allocate a one-sided memory region from the internal lamellar heap.
    /// This region only exists on the calling PE, but the returned handle can be
    /// sent to other PEs allowing remote access to the region.
    /// There will be `size` number of `T` elements on the calling PE.
    ///
    /// # One-sided Operation
    /// the calling PE will allocate the memory region locally, without intervention from the other PEs.
    ///
    fn try_alloc_one_sided_mem_region<T: Dist + std::marker::Sized>(
        &self,
        size: usize,
    ) -> Result<OneSidedMemoryRegion<T>, anyhow::Error>;
}

impl<T: Dist> Drop for MemoryRegion<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
        // println!("trying to dropping mem region {:?}",self);

        match self.mode {
            Mode::Local => self.rdma.comm().rt_free(self.alloc.clone()), // - self.rdma.comm().base_addr());
            Mode::Shared => self.rdma.comm().free(self.alloc.clone()),
            Mode::Remote => {}
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
            self.alloc.addr,
            self.alloc.size,
            self.backend,
            // self.cnt.load(Ordering::SeqCst)
        )
    }
}

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
        AllocationType, AtomicFetchOpHandle, AtomicOp, AtomicOpHandle, Backend, CommAlloc,
        CommAllocAddr, CommAllocAtomic, CommAllocInner, CommAllocRdma, CommAllocType, CommInfo,
        CommMem, CommProgress, CommSlice, Lamellae, RdmaGetBufferHandle, RdmaGetHandle,
        RdmaGetIntoBufferHandle, RdmaHandle, Remote,
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

pub(crate) mod input;
pub use input::MemregionRdmaInput;
pub(crate) use input::MemregionRdmaInputInner;

pub(crate) mod buffer;
pub use buffer::{AsLamellarBuffer, LamellarBuffer};

use enum_dispatch::enum_dispatch;
use tracing::trace;

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
    AmDist + Remote + Sync + serde::ser::Serialize + serde::de::DeserializeOwned + Default
// AmDist + Copy
{
}

// pub struct LamellarRdmaOutput{
//     buffer: MemregionRdmaOutputInner,
//     byte_alloc:
// }
// pub(crate) enum MemregionRdmaOutputInner {}

//#[doc(hidden)]
/// Enum used to expose common methods for all registered memory regions
// #[enum_dispatch(RegisteredMemoryRegion<T>, MemRegionId, AsBase, MemoryRegionRDMA<T>, RTMemoryRegionRDMA<T>, LamellarEnv)]
#[enum_dispatch(RegisteredMemoryRegion<T>, MemoryRegionRDMA<T>,RTMemoryRegionRDMA<T>,MemRegionId, AsBase,LamellarEnv)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[serde(bound = "T: Remote + serde::Serialize + serde::de::DeserializeOwned")]
pub enum LamellarMemoryRegion<T: Remote> {
    ///
    Shared(SharedMemoryRegion<T>),
    ///
    Local(OneSidedMemoryRegion<T>),
    // Unsafe(UnsafeArray<T>),
}

// This could be useful for if we want to transfer the actual data instead of the pointer
// impl<T: Remote + serde::Serialize> LamellarMemoryRegion<T> {
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

impl<T: Remote> crate::active_messaging::DarcSerde for LamellarMemoryRegion<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn ser(&self, num_pes: usize, darcs: &mut Vec<RemotePtr>) {
        // println!("in shared ser");
        match self {
            LamellarMemoryRegion::Shared(mr) => mr.ser(num_pes, darcs),
            LamellarMemoryRegion::Local(mr) => mr.ser(num_pes, darcs),
            // LamellarMemoryRegion::Unsafe(mr) => mr.ser(num_pes,darcs),
        }
    }
    // #[tracing::instrument(skip_all, level = "debug")]
    // fn des(&self, cur_pe: Result<usize, crate::IdError>) {
    //     // println!("in shared des");
    //     match self {
    //         LamellarMemoryRegion::Shared(mr) => mr.des(cur_pe),
    //         LamellarMemoryRegion::Local(mr) => mr.des(cur_pe),
    //         // LamellarMemoryRegion::Unsafe(mr) => mr.des(cur_pe),
    //     }
    //     // self.mr.print();
    // }
}

impl<T: Remote> LamellarMemoryRegion<T> {
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
impl<T: Remote> SubRegion<T> for LamellarMemoryRegion<T> {
    fn sub_region<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self {
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
            LamellarArrayRdmaInput::Owned(_) | LamellarArrayRdmaInput::OwnedVec(_) => {
                panic!("Owned values are not supported")
            }
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
pub(crate) trait RegisteredMemoryRegion<T: Remote> {
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
    fn addr(&self) -> MemResult<CommAllocAddr>;

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
pub trait SubRegion<T: Remote> {
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
    fn sub_region<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self;
}

#[enum_dispatch]
pub(crate) trait AsBase {
    unsafe fn to_base<B: Dist>(self) -> LamellarMemoryRegion<B>;
}

// #[enum_dispatch]
// pub trait MemoryRegionRDMA<T: Remote> {

// }

/// The Inteface for exposing RDMA operations on a memory region. These provide the actual mechanism for performing a transfer.

#[enum_dispatch]
pub(crate) trait RTMemoryRegionRDMA<T: Remote> {
    #[doc(alias("One-sided", "onesided"))]
    unsafe fn put(&self, pe: usize, index: usize, data: T) -> RdmaHandle<T>;

    #[doc(alias("One-sided", "onesided"))]
    unsafe fn put_unmanaged(&self, pe: usize, index: usize, data: T);

    #[doc(alias("One-sided", "onesided"))]
    /// "Puts" (copies) data from a local memory location into a remote memory location on the specified PE
    ///
    /// The data buffer may not be safe to upon return from this call, a handle is returned that can be used to check for completion,
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
    unsafe fn put_buffer(
        &self,
        pe: usize,
        index: usize,
        data: impl Into<MemregionRdmaInputInner<T>>,
    ) -> RdmaHandle<T>;

    #[doc(alias("One-sided", "onesided"))]
    /// "Puts" (copies) data from a local memory location into a remote memory location on the specified PE
    ///
    /// The data buffer may not be safe to upon return from this call, no handle is returned so the user may ensure completion
    /// via calling `wait_all` on the underlying memory region, lamellarworld or team.
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
    ///    unsafe{dst_mem_region.put_unmanaged(pe,my_pe*src_mem_region.len(),&src_mem_region)};
    /// }
    /// dst_mem_region.wait_all();
    ///```
    unsafe fn put_buffer_unmanaged(
        &self,
        pe: usize,
        index: usize,
        data: impl Into<MemregionRdmaInputInner<T>>,
    );

    unsafe fn put_all(&self, index: usize, data: T) -> RdmaHandle<T>;

    unsafe fn put_all_unmanaged(&self, index: usize, data: T);

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
    unsafe fn put_all_buffer(
        &self,
        index: usize,
        data: impl Into<MemregionRdmaInputInner<T>>,
    ) -> RdmaHandle<T>;

    unsafe fn put_all_buffer_unmanaged(
        &self,
        index: usize,
        data: impl Into<MemregionRdmaInputInner<T>>,
    );

    #[doc(alias("One-sided", "onesided"))]
    /// "At" (copies) data from remote memory location on the specified PE and returns it.
    /// After calling this function, a handle is returned that the user can use to retrieve the result.
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
    ///
    /// unsafe{ for elem in src_mem_region.as_mut_slice().expect("PE in world team") {*elem = my_pe;}}
    ///
    /// let result = src_mem_region.at(1,5).block();
    ///```
    unsafe fn get(&self, pe: usize, index: usize) -> RdmaGetHandle<T>;

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
    ///     unsafe{src_mem_region.get(pe,0,dst_mem_region.sub_region(start_i..end_i))};
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
    unsafe fn get_buffer(&self, pe: usize, index: usize, len: usize) -> RdmaGetBufferHandle<T>;

    unsafe fn get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) -> RdmaGetIntoBufferHandle<T, B>;

    unsafe fn get_into_buffer_unmanaged<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        index: usize,
        data: LamellarBuffer<T, B>,
    );

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
    // unsafe fn put_comm_slice(&self, pe: usize, index: usize, data: CommSlice<T>) -> RdmaHandle<T>;
    // unsafe fn get_comm_slice(&self, pe: usize, index: usize, data: CommSlice<T>) -> RdmaHandle<T>;
}

impl<T: Remote> Hash for LamellarMemoryRegion<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state);
    }
}

impl<T: Remote> PartialEq for LamellarMemoryRegion<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn eq(&self, other: &LamellarMemoryRegion<T>) -> bool {
        self.id() == other.id()
    }
}

impl<T: Remote> Eq for LamellarMemoryRegion<T> {}

impl<T: Remote> LamellarWrite for LamellarMemoryRegion<T> {}
impl<T: Remote> LamellarWrite for &LamellarMemoryRegion<T> {}
impl<T: Remote> LamellarRead for LamellarMemoryRegion<T> {}
impl<T: Remote> LamellarRead for &LamellarMemoryRegion<T> {}

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
pub(crate) struct MemoryRegion<T: Remote> {
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

impl<T: Remote> MemoryRegion<T> {
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
        trace!(
            "creating new lamellar memory region size: {:?} align: {:?}",
            num_elems * std::mem::size_of::<T>(),
            std::mem::align_of::<T>()
        );
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
                )? //did we call team barrer before this?
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
        trace!("new memregion alloc {:?}", temp.alloc,);
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
                inner_alloc: CommAllocInner::Raw(addr.into(), num_elems * std::mem::size_of::<T>()),
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
            self.alloc.num_bytes() % std::mem::size_of::<B>(),
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
        self.num_elems = self.alloc.num_bytes() / std::mem::size_of::<B>();
        std::mem::transmute(self) //we do this because other wise self gets dropped and frees the underlying data (we could also set addr to 0 in self)
    }

    pub(crate) unsafe fn put<R: Remote>(&self, pe: usize, index: usize, data: R) -> RdmaHandle<R> {
        if std::any::type_name::<R>() != std::any::type_name::<T>() {
            println!("cant put of type {:?} into memregion of type {:?} (use to_base to convert the memregion to the correct base type)",std::any::type_name::<R>(),std::any::type_name::<T>());
        }
        trace!("put memregion {:?} index: {:?}", self.alloc, index);
        self.alloc
            .inner_alloc
            .put(&self.scheduler, self.counters.clone(), data, pe, index)
    }

    pub(crate) unsafe fn put_unmanaged<R: Remote>(&self, pe: usize, index: usize, data: R) {
        if std::any::type_name::<R>() != std::any::type_name::<T>() {
            println!("cant put unmanaged of type {:?} into memregion of type {:?} (use to_base to convert the memregion to the correct base type)",std::any::type_name::<R>(),std::any::type_name::<T>());
        }
        trace!(
            "put unmanaged memregion {:?} index: {:?}",
            self.alloc,
            index
        );
        self.alloc.inner_alloc.put_unmanaged(data, pe, index)
    }

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
    #[tracing::instrument(skip(self, data), level = "debug")]
    pub(crate) unsafe fn put_buffer<R: Remote>(
        &self,
        pe: usize,
        index: usize,
        data: impl Into<MemregionRdmaInputInner<R>>,
    ) -> RdmaHandle<R> {
        if std::any::type_name::<R>() != std::any::type_name::<T>() {
            println!("cant put buffer of type {:?} into memregion of type {:?} (use to_base to convert the memregion to the correct base type)",std::any::type_name::<R>(),std::any::type_name::<T>());
        }
        trace!("put buffer memregion {:?} index: {:?}", self.alloc, index);
        let data = data.into();
        self.alloc
            .inner_alloc
            .put_buffer(&self.scheduler, self.counters.clone(), data, pe, index)
    }

    pub(crate) unsafe fn put_buffer_unmanaged<R: Remote>(
        &self,
        pe: usize,
        index: usize,
        data: impl Into<MemregionRdmaInputInner<R>>,
    ) {
        if std::any::type_name::<R>() != std::any::type_name::<T>() {
            println!("cant put unmanaged buffer of type {:?} into memregion of type {:?} (use to_base to convert the memregion to the correct base type)",std::any::type_name::<R>(),std::any::type_name::<T>());
        }
        trace!("put buffer memregion {:?} index: {:?}", self.alloc, index);
        let data = data.into();
        self.alloc.inner_alloc.put_buffer_unmanaged(data, pe, index)
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) unsafe fn put_all<R: Remote>(&self, offset: usize, data: R) -> RdmaHandle<R> {
        if std::any::type_name::<R>() != std::any::type_name::<T>() {
            println!("cant put all of type {:?} into memregion of type {:?} (use to_base to convert the memregion to the correct base type)",std::any::type_name::<R>(),std::any::type_name::<T>());
        }
        trace!("put all memregion {:?} index: {:?}", self.alloc, offset);
        self.alloc
            .inner_alloc
            .put_all(&self.scheduler, self.counters.clone(), data, offset)
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) unsafe fn put_all_unmanaged<R: Remote>(&self, offset: usize, data: R) {
        if std::any::type_name::<R>() != std::any::type_name::<T>() {
            println!("cant put all unmanaged of type {:?} into memregion of type {:?} (use to_base to convert the memregion to the correct base type)",std::any::type_name::<R>(),std::any::type_name::<T>());
        }
        trace!(
            "put all unmanaged memregion {:?} index: {:?}",
            self.alloc,
            offset
        );
        self.alloc.inner_alloc.put_all_unmanaged(data, offset);
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) unsafe fn put_all_buffer<R: Remote>(
        &self,
        offset: usize,
        data: impl Into<MemregionRdmaInputInner<R>>,
    ) -> RdmaHandle<R> {
        if std::any::type_name::<R>() != std::any::type_name::<T>() {
            println!("cant put all buffer of type {:?} into memregion of type {:?} (use to_base to convert the memregion to the correct base type)",std::any::type_name::<R>(),std::any::type_name::<T>());
        }
        trace!(
            "put all buffer memregion {:?} index: {:?}",
            self.alloc,
            offset
        );
        let data = data.into();

        self.alloc
            .inner_alloc
            .put_all_buffer(&self.scheduler, self.counters.clone(), data, offset)
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) unsafe fn put_all_buffer_unmanaged<R: Remote>(
        &self,
        offset: usize,
        data: impl Into<MemregionRdmaInputInner<R>>,
    ) {
        if std::any::type_name::<R>() != std::any::type_name::<T>() {
            println!("cant put all unmanaged buffer of type {:?} into memregion of type {:?} (use to_base to convert the memregion to the correct base type)",std::any::type_name::<R>(),std::any::type_name::<T>());
        }
        trace!(
            "put all buffer unmanaged memregion {:?} index: {:?}",
            self.alloc,
            offset
        );
        let data = data.into();

        self.alloc
            .inner_alloc
            .put_all_buffer_unmanaged(data, offset);
    }

    pub(crate) unsafe fn get<R: Remote>(&self, pe: usize, index: usize) -> RdmaGetHandle<R> {
        if std::any::type_name::<R>() != std::any::type_name::<T>() {
            println!("cant get of type {:?} from memregion of type {:?} (use to_base to convert the memregion to the correct base type)",std::any::type_name::<R>(),std::any::type_name::<T>());
        }
        trace!("get memregion {:?} index: {:?}", self.alloc, index);

        self.alloc
            .inner_alloc
            .get(&self.scheduler, self.counters.clone(), pe, index)
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
    pub(crate) unsafe fn get_buffer<R: Remote>(
        &self,
        pe: usize,
        index: usize,
        len: usize,
    ) -> RdmaGetBufferHandle<R> {
        if std::any::type_name::<R>() != std::any::type_name::<T>() {
            println!("cant get buffer of type {:?} from memregion of type {:?} (use to_base to convert the memregion to the correct base type)",std::any::type_name::<R>(),std::any::type_name::<T>());
        }
        trace!(
            "get buffer memregion pe: {:?} index: {:?} num_elems: {:?} alloc {:?}",
            pe,
            index,
            len,
            self.alloc
        );

        self.alloc
            .inner_alloc
            .get_buffer(&self.scheduler, self.counters.clone(), pe, index, len)
    }

    pub(crate) unsafe fn get_into_buffer<R: Remote, B: AsLamellarBuffer<R>>(
        &self,
        pe: usize,
        index: usize,
        data: LamellarBuffer<R, B>,
    ) -> RdmaGetIntoBufferHandle<R, B> {
        if std::any::type_name::<R>() != std::any::type_name::<T>() {
            println!("cant get into buffer of type {:?} from memregion of type {:?} (use to_base to convert the memregion to the correct base type)",std::any::type_name::<R>(),std::any::type_name::<T>());
        }
        trace!(
            "get into buffer memregion {:?} index: {:?}",
            self.alloc,
            index
        );

        self.alloc.inner_alloc.get_into_buffer(
            &self.scheduler,
            self.counters.clone(),
            pe,
            index,
            data,
        )
    }

    pub(crate) unsafe fn get_into_buffer_unmanaged<R: Remote, B: AsLamellarBuffer<R>>(
        &self,
        pe: usize,
        index: usize,
        data: LamellarBuffer<R, B>,
    ) {
        if std::any::type_name::<R>() != std::any::type_name::<T>() {
            println!("cant get into unmanaged buffer of type {:?} from memregion of type {:?} (use to_base to convert the memregion to the correct base type)",std::any::type_name::<R>(),std::any::type_name::<T>());
        }
        trace!(
            "get into buffer unmanaged memregion {:?} index: {:?}",
            self.alloc,
            index
        );

        self.alloc
            .inner_alloc
            .get_into_buffer_unmanaged(pe, index, data);
    }

    pub(crate) fn atomic_op<R: Remote>(
        &self,
        pe: usize,
        index: usize,
        op: AtomicOp<R>,
    ) -> AtomicOpHandle<R> {
        trace!("atomic_op memregion {:?} index: {:?}", self.alloc, index);
        self.alloc
            .inner_alloc
            .atomic_op(&self.scheduler, self.counters.clone(), op, pe, index)
    }
    pub(crate) fn atomic_op_unmanaged<R: Remote>(&self, pe: usize, index: usize, op: AtomicOp<R>) {
        trace!(
            "atomic_op unmanaged memregion {:?} index: {:?}",
            self.alloc,
            index
        );
        self.alloc.inner_alloc.atomic_op_unmanaged(op, pe, index)
    }

    pub(crate) fn atomic_op_all<R: Remote>(
        &self,
        offset: usize,
        op: AtomicOp<R>,
    ) -> AtomicOpHandle<R> {
        trace!(
            "atomic_op_all memregion {:?} index: {:?}",
            self.alloc,
            offset
        );
        self.alloc
            .inner_alloc
            .atomic_op_all(&self.scheduler, self.counters.clone(), op, offset)
    }
    pub(crate) fn atomic_op_all_unmanaged<R: Remote>(&self, offset: usize, op: AtomicOp<R>) {
        trace!(
            "atomic_op_all unmanaged memregion {:?} index: {:?}",
            self.alloc,
            offset
        );
        self.alloc.inner_alloc.atomic_op_all_unmanaged(op, offset)
    }

    pub(crate) fn atomic_fetch_op<R: Remote>(
        &self,
        pe: usize,
        index: usize,
        op: AtomicOp<R>,
    ) -> AtomicFetchOpHandle<R> {
        trace!(
            "atomic_fetch_op memregion {:?} index: {:?}",
            self.alloc,
            index
        );
        self.alloc.inner_alloc.atomic_fetch_op(
            &self.scheduler,
            self.counters.clone(),
            op,
            pe,
            index,
        )
    }

    pub(crate) fn wait_all(&self) {
        self.rdma.comm().wait();
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn addr(&self) -> MemResult<CommAllocAddr> {
        if self.mode == Mode::Remote {
            return Err(MemRegionError::MemNotLocalError);
        }
        Ok(self.alloc.inner_alloc.addr())
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn casted_at<R: Remote>(&self, index: usize) -> MemResult<&R> {
        if self.mode == Mode::Remote {
            return Err(MemRegionError::MemNotLocalError);
        }
        let num_bytes = self.alloc.num_bytes();
        assert_eq!(
            num_bytes % std::mem::size_of::<R>(),
            0,
            "Error converting memregion to new base, does not align"
        );
        Ok(unsafe {
            &std::slice::from_raw_parts(self.alloc.as_ptr(), num_bytes / std::mem::size_of::<R>())
                [index]
        })
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn as_slice(&self) -> &[T] {
        unsafe { self.as_mut_slice() }
    }
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn as_casted_slice<R: Remote>(&self) -> MemResult<&[R]> {
        unsafe { Ok(self.as_casted_mut_slice()?) }
    }
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) unsafe fn as_mut_slice(&self) -> &mut [T] {
        if self.mode == Mode::Remote {
            return &mut [];
        }
        // trace!(
        //     "as_mut_slice memregion {:?} num_elems: {:?} size: {:?} calced elems: {:?}",
        //     self.alloc,
        //     self.num_elems,
        //     self.alloc.num_bytes(),
        //     self.alloc.num_bytes() / std::mem::size_of::<T>()
        // );
        std::slice::from_raw_parts_mut(
            self.alloc.as_mut_ptr(),
            self.alloc.num_bytes() / std::mem::size_of::<T>(),
        )
    }
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) unsafe fn as_casted_mut_slice<R: Remote>(&self) -> MemResult<&mut [R]> {
        if self.mode == Mode::Remote {
            return Ok(&mut []);
        }
        if self.alloc.num_bytes() % std::mem::size_of::<R>() != 0 {
            return Err(MemRegionError::MemNotAlignedError);
        }
        Ok(std::slice::from_raw_parts_mut(
            self.alloc.as_mut_ptr(),
            self.alloc.num_bytes() / std::mem::size_of::<R>(),
        ))
    }
    // #[allow(dead_code)]
    // #[tracing::instrument(skip_all, level = "debug")]
    // pub(crate) fn as_ptr(&self) -> MemResult<*const T> {
    //     Ok(self.addr as *const T)
    // }
    // #[allow(dead_code)]
    // #[tracing::instrument(skip_all, level = "debug")]
    // pub(crate) fn as_casted_ptr<R: Remote>(&self) -> MemResult<*const R> {
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
    pub(crate) fn as_casted_mut_ptr<R: Remote>(&self) -> MemResult<*mut R> {
        if self.mode == Mode::Remote {
            return Err(MemRegionError::MemNotLocalError);
        }
        unsafe { Ok(self.alloc.as_mut_ptr()) }
    }

    pub(crate) unsafe fn as_comm_slice(&self) -> MemResult<CommSlice<T>> {
        if self.mode == Mode::Remote {
            return Err(MemRegionError::MemNotLocalError);
        }
        Ok(self.alloc.as_comm_slice())
    }
    pub(crate) unsafe fn as_casted_comm_slice<R: Remote>(&self) -> MemResult<CommSlice<R>> {
        if self.mode == Mode::Remote {
            return Err(MemRegionError::MemNotLocalError);
        }
        Ok(self.alloc.as_comm_slice())
    }
    pub(crate) unsafe fn comm_addr(&self) -> MemResult<CommAllocAddr> {
        if self.mode == Mode::Remote {
            return Err(MemRegionError::MemNotLocalError);
        }
        Ok(self.alloc.comm_addr())
    }
}

impl<T: Remote> MemRegionId for MemoryRegion<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn id(&self) -> usize {
        self.alloc.inner_alloc.addr().into()
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
    fn alloc_shared_mem_region<T: Remote + std::marker::Sized>(
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
    fn try_alloc_shared_mem_region<T: Remote + std::marker::Sized>(
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
    fn alloc_one_sided_mem_region<T: Remote + std::marker::Sized>(
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
    fn try_alloc_one_sided_mem_region<T: Remote + std::marker::Sized>(
        &self,
        size: usize,
    ) -> Result<OneSidedMemoryRegion<T>, anyhow::Error>;
}

impl<T: Remote> Drop for MemoryRegion<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
        // println!("trying to dropping mem region {:?}",self);

        match self.mode {
            Mode::Local => self.rdma.comm().rt_free(self.alloc.clone()),
            Mode::Shared => self.rdma.comm().free(self.alloc.clone()),
            Mode::Remote => {}
        }
        // println!("dropping mem region {:?}",self);
    }
}

impl<T: Remote> std::fmt::Debug for MemoryRegion<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // write!(f, "{:?}", slice)
        write!(
            f,
            "addr {:#x} size {:?} backend {:?}", // cnt: {:?}",
            self.alloc.comm_addr(),
            self.alloc.num_bytes(),
            self.backend,
            // self.cnt.load(Ordering::SeqCst)
        )
    }
}

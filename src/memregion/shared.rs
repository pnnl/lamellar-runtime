use crate::active_messaging::RemotePtr;
use crate::array::{LamellarRead, LamellarWrite, TeamTryFrom};
use crate::darc::Darc;
use crate::lamellae::collective::{CollectiveAllReduceInPlaceOpHandle, CollectiveAllReduceIntoBufferOpHandle, CollectiveBroadcastIntoBufferOpHandle, CollectiveBroadcastOpHandle, CollectiveGatherIntoBufferOpHandle, CollectiveGatherOpHandle};
use crate::lamellae::{AllocationType, LamellaeUtil, RdmaGetBufferHandle, RdmaGetIntoBufferHandle};
use crate::{memregion::*, LamellarEnv, LamellarTeam};

// use crate::active_messaging::AmDist;
use core::marker::PhantomData;
// use serde::ser::Serialize;
use std::sync::Arc;

use std::ops::Bound;
use tracing::trace;

/// A Shared Memory Region is a [RemoteMemoryRegion] that has only been allocated on multiple PEs.
///
/// The memory region provides RDMA access to any PE which has a handle to the region.
///
/// SharedMemoryRegions implement distributed reference counting, so their handles can be sent along in active messages
/// to other Remote PE's, and it is gauranteed that the memory regions on each PE will remain valid as long as a single reference
/// exists on any PE anywhere in the distributed system (even if the original allocating PEs drops all local references to the memory region)
///
/// SharedMemoryRegions are constructed using either the LamellarWorld instance or a LamellarTeam instance.
///
/// Memory Regions are low-level unsafe abstraction not really intended for use in higher-level applications
///
/// # Warning
/// Unless you are very confident in low level distributed memory access it is highly recommended you utilize the
/// [LamellarArray][crate::array::LamellarArray]  interface to construct and interact with distributed memory.
///
/// # Examples
///
///
///```
/// use lamellar::memregion::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
///
/// let world_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1000).block();
/// ```
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct SharedMemoryRegion<T: Remote> {
    pub(crate) mr: Darc<MemoryRegion<u8>>,
    sub_region_offset: usize,
    sub_region_size: usize,
    phantom: PhantomData<T>,
}

impl<T: Remote> LamellarEnv for SharedMemoryRegion<T> {
    fn my_pe(&self) -> usize {
        self.mr.team().my_pe()
    }
    fn num_pes(&self) -> usize {
        self.mr.team().num_pes()
    }
    fn num_threads_per_pe(&self) -> usize {
        self.mr.team().num_threads_per_pe()
    }
    fn world(&self) -> Arc<LamellarTeam> {
        self.mr.team().world()
    }
    fn team(&self) -> Arc<LamellarTeam> {
        self.mr.team().team()
    }
}

impl<T: Remote> crate::active_messaging::DarcSerde for SharedMemoryRegion<T> {
    fn ser(&self, num_pes: usize, darcs: &mut Vec<RemotePtr>) {
        self.mr.serialize_update_cnts(num_pes);
        darcs.push(RemotePtr::NetworkDarc(self.mr.clone().into()));
    }
}

impl<T: Remote> SharedMemoryRegion<T> {
    // pub(crate) fn new(
    //     size: usize,
    //     team: Darc<LamellarTeamRT>,
    //     alloc: AllocationType,
    // ) -> SharedMemoryRegionHandle<T> {
    //     SharedMemoryRegion::try_new(size, team, alloc).expect("Out of memory")
    // }
    pub(crate) fn lamellae(
        &self,
    ) -> Arc<crate::lamellae::Lamellae> {
        self.mr.rdma.clone()
    }

    pub(crate) fn new(
        size: usize,
        team: Darc<LamellarTeamRT>,
        alloc: AllocationType,
    ) -> SharedMemoryRegionHandle<T> {
        // println!("creating new shared mem region {:?} {:?}",size,alloc);

        SharedMemoryRegionHandle {
            team: team.clone(),
            launched: false,
            creation_future: Box::pin(async move {
                team.async_barrier().await;
                let mut mr_t = MemoryRegion::<T>::try_new(
                    size,
                    &team.scheduler,
                    team.counters(),
                    &team.lamellae,
                    alloc.clone(),
                );
                while let Err(_e) = mr_t {
                    async_std::task::yield_now().await;
                    team.lamellae
                        .request_new_alloc(size * std::mem::size_of::<T>())
                        .await;
                    mr_t = MemoryRegion::try_new(
                        size,
                        &team.scheduler,
                        team.counters(),
                        &team.lamellae,
                        alloc.clone(),
                    );
                }

                let mr = unsafe {
                    mr_t.expect("enough memory should have been allocated")
                        .to_base::<u8>()
                };
                SharedMemoryRegion {
                    mr: Darc::async_try_new_with_drop(
                        team.clone(),
                        mr,
                        crate::darc::DarcMode::Darc,
                        None,
                    )
                    .await
                    .expect("memregions can only be created on a member of the team"),
                    sub_region_offset: 0,
                    sub_region_size: size,
                    phantom: PhantomData,
                }
            }),
        }
    }

    pub(crate) fn try_new(
        size: usize,
        team: Darc<LamellarTeamRT>,
        alloc: AllocationType,
    ) -> FallibleSharedMemoryRegionHandle<T> {
        // println!("creating new shared mem region {:?} {:?}",size,alloc);

        FallibleSharedMemoryRegionHandle {
            team: team.clone(),
            launched: false,
            creation_future: Box::pin(async move {
                team.async_barrier().await;
                let mr_t: MemoryRegion<T> = MemoryRegion::try_new(
                    size,
                    &team.scheduler,
                    team.counters(),
                    &team.lamellae,
                    alloc,
                )?;
                let mr = unsafe { mr_t.to_base::<u8>() };
                let res: Result<SharedMemoryRegion<T>, anyhow::Error> = Ok(SharedMemoryRegion {
                    mr: Darc::async_try_new_with_drop(
                        team.clone(),
                        mr,
                        crate::darc::DarcMode::Darc,
                        None,
                    )
                    .await
                    .expect("memregions can only be created on a member of the team"),
                    sub_region_offset: 0,
                    sub_region_size: size,
                    phantom: PhantomData,
                });
                res
            }),
        }
    }

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
    pub unsafe fn as_slice(&self) -> &[T] {
        RegisteredMemoryRegion::as_slice(self)
    }

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
    pub unsafe fn as_mut_slice(&self) -> &mut [T] {
        RegisteredMemoryRegion::as_mut_slice(self)
    }

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
    pub unsafe fn as_ptr(&self) -> MemResult<*const T> {
        RegisteredMemoryRegion::as_ptr(self)
    }

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
    pub unsafe fn as_mut_ptr(&self) -> MemResult<*mut T> {
        RegisteredMemoryRegion::as_mut_ptr(self)
    }

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
    pub fn sub_region<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self {
        SubRegion::sub_region(self, range)
    }

    /// Return the length of the memory region
    pub fn len(&self) -> usize {
        self.sub_region_size
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Initiates a remote write of a single element to the given `pe` at `index` in this shared memory region.
    ///
    /// Returns an [`RdmaHandle`] representing the in-flight transfer. The transfer is not
    /// guaranteed complete until the handle is driven via `.await`, `spawn()`, or `block()`.
    ///
    /// # Safety
    /// This call is always unsafe because mutual exclusivity is not enforced — other PEs
    /// may read or write overlapping locations concurrently. The fabric provider may not
    /// have copied `data` by the time this call returns.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer; the target PE is not notified.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes).block();
    /// unsafe {
    ///     mem_region.put(my_pe, my_pe, my_pe).block();
    /// }
    /// world.wait_all();
    /// world.barrier();
    ///```
    pub unsafe fn put(&self, pe: usize, index: usize, data: T) -> RdmaHandle<T> {
        RTMemoryRegionRDMA::<T>::put(self, pe, index, data)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Writes a single element to the given `pe` at `index` and **blocks** until the transfer is complete.
    ///
    /// Unlike [`put`][Self::put], this call does not return until the data has been delivered;
    /// no handle is needed for completion detection.
    ///
    /// # Safety
    /// This call is always unsafe because mutual exclusivity is not enforced — other PEs
    /// may read or write overlapping locations concurrently.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer; the target PE is not notified.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes).block();
    /// unsafe {
    ///     mem_region.put_blocking(my_pe, my_pe, my_pe);
    /// }
    /// world.barrier();
    ///```
    pub unsafe fn put_blocking(&self, pe: usize, index: usize, data: T) {
        RTMemoryRegionRDMA::<T>::put_blocking(self, pe, index, data)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Initiates a remote write of a single element to the given `pe` **without** tracking the
    /// transfer in the runtime's completion bookkeeping.
    ///
    /// The caller is entirely responsible for ensuring the transfer is complete before accessing
    /// the destination. [`wait_all`][Self::wait_all] and [`barrier`][crate::LamellarEnv::barrier]
    /// do **not** track unmanaged operations.
    ///
    /// # Safety
    /// This call is always unsafe because mutual exclusivity is not enforced and there is no
    /// runtime-tracked handle for completion detection.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer; the target PE is not notified.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes).block();
    /// unsafe {
    ///     mem_region.put_unmanaged(my_pe, my_pe, my_pe);
    /// }
    /// // caller is responsible for ensuring completion
    /// world.barrier();
    ///```
    pub unsafe fn put_unmanaged(&self, pe: usize, index: usize, data: T) {
        RTMemoryRegionRDMA::<T>::put_unmanaged(self, pe, index, data)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Initiates a remote write of a contiguous buffer to the given `pe` at `index` in this shared memory region.
    ///
    /// Accepts any type that implements `Into<MemregionRdmaInput<T>>`, including
    /// [`OneSidedMemoryRegion<T>`], [`SharedMemoryRegion<T>`], and slices thereof.
    ///
    /// Returns an [`RdmaHandle`] representing the in-flight transfer. The data buffer must
    /// not be dropped or mutated until the transfer completes.
    ///
    /// # Safety
    /// This call is always unsafe because:
    /// * Mutual exclusivity is not enforced — other PEs may concurrently access the destination.
    /// * Multi-element transfers are not collectively atomic.
    /// * The fabric provider may not have copied the source by the time this call returns.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer; the target PE is not notified.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let src: OneSidedMemoryRegion<usize> = world.alloc_one_sided_mem_region(10);
    /// let dst: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes * 10).block();
    /// unsafe {
    ///     for (i, elem) in src.as_mut_slice().expect("PE just allocated").iter_mut().enumerate() {
    ///         *elem = my_pe * 10 + i;
    ///     }
    ///     dst.put_buffer(my_pe, my_pe * 10, src).block();
    /// }
    /// world.wait_all();
    /// world.barrier();
    ///```
    pub unsafe fn put_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) -> RdmaHandle<T> {
        RTMemoryRegionRDMA::<T>::put_buffer(self, pe, index, data.into())
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Initiates a remote write of a contiguous buffer to the given `pe` **without** tracking
    /// the transfer in the runtime's completion bookkeeping.
    ///
    /// The caller is entirely responsible for ensuring the transfer is complete before accessing
    /// either the source or destination.
    ///
    /// # Safety
    /// This call is always unsafe because:
    /// * Mutual exclusivity is not enforced — other PEs may concurrently access the destination.
    /// * Multi-element transfers are not collectively atomic.
    /// * There is no runtime-tracked handle; the caller must arrange for completion detection.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer; the target PE is not notified.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let src: OneSidedMemoryRegion<usize> = world.alloc_one_sided_mem_region(10);
    /// let dst: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes * 10).block();
    /// unsafe {
    ///     for (i, elem) in src.as_mut_slice().expect("PE just allocated").iter_mut().enumerate() {
    ///         *elem = my_pe * 10 + i;
    ///     }
    ///     dst.put_buffer_unmanaged(my_pe, my_pe * 10, src);
    /// }
    /// world.barrier();
    ///```
    pub unsafe fn put_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        RTMemoryRegionRDMA::<T>::put_buffer_unmanaged(self, pe, index, data.into())
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Broadcasts a single element to **all** PEs at `index` in this shared memory region.
    ///
    /// Returns an [`RdmaHandle`] representing the in-flight transfer.
    ///
    /// # Safety
    /// This call is always unsafe because mutual exclusivity is not enforced — other PEs
    /// may read or write overlapping locations concurrently.
    ///
    /// # One-sided Operation
    /// The calling PE initiates transfers to all PEs; remote PEs are not notified.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1).block();
    /// if my_pe == 0 {
    ///     unsafe { mem_region.put_all(0, 42usize).block(); }
    /// }
    /// world.wait_all();
    /// world.barrier();
    ///```
    pub unsafe fn put_all(&self, index: usize, data: T) -> RdmaHandle<T> {
        RTMemoryRegionRDMA::<T>::put_all(self, index, data)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Broadcasts a single element to **all** PEs at `index` **without** tracking the transfers
    /// in the runtime's completion bookkeeping.
    ///
    /// The caller is entirely responsible for ensuring all transfers are complete.
    ///
    /// # Safety
    /// This call is always unsafe because mutual exclusivity is not enforced and there is no
    /// runtime-tracked handle for completion detection.
    ///
    /// # One-sided Operation
    /// The calling PE initiates transfers to all PEs; remote PEs are not notified.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1).block();
    /// if my_pe == 0 {
    ///     unsafe { mem_region.put_all_unmanaged(0, 42usize); }
    /// }
    /// world.barrier();
    ///```
    pub unsafe fn put_all_unmanaged(&self, index: usize, data: T) {
        RTMemoryRegionRDMA::<T>::put_all_unmanaged(self, index, data)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Broadcasts a contiguous buffer to **all** PEs at `index` in this shared memory region.
    ///
    /// Returns an [`RdmaHandle`] representing the in-flight transfers.
    ///
    /// # Safety
    /// This call is always unsafe because:
    /// * Mutual exclusivity is not enforced — other PEs may concurrently access the destination.
    /// * Multi-element transfers are not collectively atomic.
    /// * The fabric provider may not have copied the source by the time this call returns.
    ///
    /// # One-sided Operation
    /// The calling PE initiates transfers to all PEs; remote PEs are not notified.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// let src: OneSidedMemoryRegion<usize> = world.alloc_one_sided_mem_region(10);
    /// let dst: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(10).block();
    /// unsafe {
    ///     for (i, elem) in src.as_mut_slice().expect("PE just allocated").iter_mut().enumerate() {
    ///         *elem = i;
    ///     }
    ///     if my_pe == 0 {
    ///         dst.put_all_buffer(0, src).block();
    ///     }
    /// }
    /// world.wait_all();
    /// world.barrier();
    ///```
    pub unsafe fn put_all_buffer<U: Into<MemregionRdmaInput<T>>>(
        &self,
        index: usize,
        data: U,
    ) -> RdmaHandle<T> {
        RTMemoryRegionRDMA::<T>::put_all_buffer(self, index, data.into())
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Broadcasts a contiguous buffer to **all** PEs at `index` **without** tracking the transfers
    /// in the runtime's completion bookkeeping.
    ///
    /// The caller is entirely responsible for ensuring all transfers are complete.
    ///
    /// # Safety
    /// This call is always unsafe because:
    /// * Mutual exclusivity is not enforced — other PEs may concurrently access the destination.
    /// * Multi-element transfers are not collectively atomic.
    /// * There is no runtime-tracked handle; the caller must arrange for completion detection.
    ///
    /// # One-sided Operation
    /// The calling PE initiates transfers to all PEs; remote PEs are not notified.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// let src: OneSidedMemoryRegion<usize> = world.alloc_one_sided_mem_region(10);
    /// let dst: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(10).block();
    /// unsafe {
    ///     for (i, elem) in src.as_mut_slice().expect("PE just allocated").iter_mut().enumerate() {
    ///         *elem = i;
    ///     }
    ///     if my_pe == 0 {
    ///         dst.put_all_buffer_unmanaged(0, src);
    ///     }
    /// }
    /// world.barrier();
    ///```
    pub unsafe fn put_all_buffer_unmanaged<U: Into<MemregionRdmaInput<T>>>(
        &self,
        index: usize,
        data: U,
    ) {
        RTMemoryRegionRDMA::<T>::put_all_buffer_unmanaged(self, index, data.into());
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Fetches a single element from the given `pe` at `index` in this shared memory region.
    ///
    /// Returns an [`RdmaGetHandle`] whose `block()` / `.await` resolves to `T`.
    ///
    /// # Safety
    /// This call is always unsafe because mutual exclusivity is not enforced — other PEs
    /// may be writing to the source location concurrently.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer; the target PE is not notified.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes).block();
    /// unsafe {
    ///     mem_region.put(my_pe, my_pe, my_pe).block();
    ///     world.barrier();
    ///     let val = mem_region.get(my_pe, my_pe).block();
    ///     assert_eq!(val, my_pe);
    /// }
    ///```
    pub unsafe fn get(&self, pe: usize, index: usize) -> RdmaGetHandle<T> {
        RTMemoryRegionRDMA::<T>::get(self, pe, index)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Fetches a contiguous slice of `len` elements from the given `pe` at `index`.
    ///
    /// Returns an [`RdmaGetBufferHandle`] whose `block()` / `.await` resolves to a `Vec<T>`.
    ///
    /// # Safety
    /// This call is always unsafe because:
    /// * Mutual exclusivity is not enforced — other PEs may concurrently write to the source.
    /// * Multi-element transfers are not collectively atomic.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer; the target PE is not notified.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes * 10).block();
    /// unsafe {
    ///     for (i, elem) in mem_region.as_mut_slice().expect("PE just allocated").iter_mut().enumerate() {
    ///         *elem = i;
    ///     }
    ///     let data: Vec<usize> = mem_region.get_buffer(my_pe, my_pe * 10, 10).block();
    ///     assert_eq!(data.len(), 10);
    /// }
    ///```
    pub unsafe fn get_buffer(&self, pe: usize, index: usize, len: usize) -> RdmaGetBufferHandle<T> {
        RTMemoryRegionRDMA::<T>::get_buffer(self, pe, index, len)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Fetches data from the given `pe` at `index` into a caller-supplied [`LamellarBuffer`].
    ///
    /// Returns an [`RdmaGetIntoBufferHandle`] whose `block()` / `.await` resolves to `()`;
    /// the fetched data is available through the [`LamellarBuffer`] after completion.
    ///
    /// Use [`LamellarBuffer::from_vec`] to wrap a `Vec<T>` as the destination, or
    /// [`LamellarBuffer::from_one_sided_memory_region`] to use a pinned RDMA-registered buffer.
    ///
    /// # Safety
    /// This call is always unsafe because:
    /// * Mutual exclusivity is not enforced — other PEs may concurrently write to the source.
    /// * Multi-element transfers are not collectively atomic.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer; the target PE is not notified.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes * 10).block();
    /// unsafe {
    ///     for (i, elem) in mem_region.as_mut_slice().expect("PE just allocated").iter_mut().enumerate() {
    ///         *elem = i;
    ///     }
    ///     let buf = LamellarBuffer::from_vec(vec![0usize; 10]);
    ///     mem_region.get_into_buffer(my_pe, my_pe * 10, buf).block();
    /// }
    ///```
    pub unsafe fn get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) -> RdmaGetIntoBufferHandle<T, B> {
        RTMemoryRegionRDMA::<T>::get_into_buffer(self, pe, index, data)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Fetches data from the given `pe` into a caller-supplied [`LamellarBuffer`] **without**
    /// tracking the transfer in the runtime's completion bookkeeping.
    ///
    /// The caller is entirely responsible for ensuring the transfer is complete before reading
    /// the buffer.
    ///
    /// # Safety
    /// This call is always unsafe because:
    /// * Mutual exclusivity is not enforced — other PEs may concurrently write to the source.
    /// * Multi-element transfers are not collectively atomic.
    /// * There is no runtime-tracked handle; the caller must arrange for completion detection.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the remote transfer; the target PE is not notified.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes * 10).block();
    /// unsafe {
    ///     for (i, elem) in mem_region.as_mut_slice().expect("PE just allocated").iter_mut().enumerate() {
    ///         *elem = i;
    ///     }
    ///     let buf = LamellarBuffer::from_vec(vec![0usize; 10]);
    ///     mem_region.get_into_buffer_unmanaged(my_pe, my_pe * 10, buf);
    /// }
    /// // caller is responsible for ensuring completion before reading the buffer
    /// world.barrier();
    ///```
    pub unsafe fn get_into_buffer_unmanaged<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) {
        RTMemoryRegionRDMA::<T>::get_into_buffer_unmanaged(self, pe, index, data);
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Atomically stores `val` at `index` on the given `pe` using a network-level atomic write.
    ///
    /// Returns an [`AtomicOpHandle`] representing the in-flight operation.
    ///
    /// # Safety
    /// This call is unsafe because the caller must ensure `index` is within bounds and that
    /// the surrounding memory access pattern does not violate invariants.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the atomic operation; the target PE is not notified.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes).block();
    /// unsafe {
    ///     mem_region.atomic_store(my_pe, my_pe, my_pe).block();
    /// }
    /// world.wait_all();
    /// world.barrier();
    ///```
    pub unsafe fn atomic_store(&self, pe: usize, index: usize, val: T) -> AtomicOpHandle<T> {
        self.mr.as_base::<T>().atomic_op(
            pe,
            self.sub_region_offset + index,
            AtomicOp::Write(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Atomically stores `val` at `index` on the given `pe` **without** tracking the operation
    /// in the runtime's completion bookkeeping.
    ///
    /// The caller is entirely responsible for ensuring the operation is complete before
    /// reading the destination.
    ///
    /// # Safety
    /// This call is unsafe because the caller must ensure `index` is within bounds and there
    /// is no runtime-tracked handle for completion detection.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the atomic operation; the target PE is not notified.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes).block();
    /// unsafe {
    ///     mem_region.atomic_store_unmanaged(my_pe, my_pe, my_pe);
    /// }
    /// world.barrier();
    ///```
    pub unsafe fn atomic_store_unmanaged(&self, pe: usize, index: usize, val: T) {
        //we need to do the offsetting here since we are going directly through the inner alloc

        self.mr.as_base::<T>().atomic_op_unmanaged(
            pe,
            self.sub_region_offset + index,
            AtomicOp::Write(Box::pin(val)),
        );
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Atomically loads the element at `index` on the given `pe`.
    ///
    /// Returns an [`AtomicFetchOpHandle`] whose `block()` / `.await` resolves to the fetched `T`.
    ///
    /// # Safety
    /// This call is unsafe because the caller must ensure `index` is within bounds.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the atomic operation; the target PE is not notified.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes).block();
    /// unsafe {
    ///     mem_region.atomic_store(my_pe, my_pe, my_pe).block();
    ///     let val = mem_region.atomic_load(my_pe, my_pe).block();
    ///     assert_eq!(val, my_pe);
    /// }
    ///```
    pub unsafe fn atomic_load(&self, pe: usize, index: usize) -> AtomicFetchOpHandle<T> {
        // let res = MaybeUninit::uninit().assume_init();
        self.mr.as_base::<T>().atomic_fetch_op(
            pe,
            self.sub_region_offset + index,
            AtomicOp::Read(Box::pin(std::mem::zeroed())),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Atomically swaps the element at `index` on the given `pe` with `val`, returning the
    /// previous value.
    ///
    /// Returns an [`AtomicFetchOpHandle`] whose `block()` / `.await` resolves to the old `T`.
    ///
    /// # Safety
    /// This call is unsafe because the caller must ensure `index` is within bounds.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the atomic operation; the target PE is not notified.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes).block();
    /// unsafe {
    ///     mem_region.atomic_store(my_pe, my_pe, my_pe).block();
    ///     let old = mem_region.atomic_swap(my_pe, my_pe, 0usize).block();
    ///     assert_eq!(old, my_pe);
    /// }
    ///```
    pub unsafe fn atomic_swap(&self, pe: usize, index: usize, val: T) -> AtomicFetchOpHandle<T> {
        // let res = MaybeUninit::uninit().assume_init();
        self.mr.as_base::<T>().atomic_fetch_op(
            pe,
            self.sub_region_offset + index,
            AtomicOp::Write(Box::pin(val)),
        )
    }
    pub unsafe fn min_all(&self, index: usize, len: usize) -> CollectiveAllReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all(index, len, ReduceOp::Min)
    }
    pub unsafe fn max_all(&self, index: usize, len: usize) -> CollectiveAllReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all(index, len, ReduceOp::Max)
    }
    pub unsafe fn sum_all(&self, index: usize, len: usize) -> CollectiveAllReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all(index, len, ReduceOp::Sum)
    }
    pub unsafe fn prod_all(&self, index: usize, len: usize) -> CollectiveAllReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all(index, len, ReduceOp::Prod)
    }
    pub unsafe fn bit_or_all(&self, index: usize, len: usize) -> CollectiveAllReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all(index, len, ReduceOp::BitOr)
    }
    pub unsafe fn bit_xor_all(&self, index: usize, len: usize) -> CollectiveAllReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all(index, len, ReduceOp::BitXor)
    }
    pub unsafe fn bit_and_all(&self, index: usize, len: usize) -> CollectiveAllReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all(index, len, ReduceOp::BitAnd)
    }
    pub unsafe fn min_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::Min, buffer)
    }
    pub unsafe fn max_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::Max, buffer)
    }
    pub unsafe fn sum_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::Sum, buffer)
    }
    pub unsafe fn prod_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::Prod, buffer)
    }
    pub unsafe fn bit_or_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::BitOr, buffer)
    }
    pub unsafe fn bit_xor_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::BitXor, buffer)
    }
    pub unsafe fn bit_and_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::BitAnd, buffer)
    }
    pub unsafe fn min_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::Min)
    }
    pub unsafe fn max_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
        .as_base::<T>()
        .reduce_all_in_place(src_and_dst, ReduceOp::Max)
    }
    pub unsafe fn sum_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::Sum)
    }
    pub unsafe fn prod_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::Prod)
    }
    pub unsafe fn bit_or_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::BitOr)
    }
    pub unsafe fn bit_xor_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::BitXor)
    }
    pub unsafe fn bit_and_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T, B>) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::BitAnd)
    }
    pub unsafe fn min_at_pe(&self, index: usize, len: usize, root_pe: usize) -> CollectiveReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce(ReduceOp::Min, index, len, root_pe)
    }
            pub unsafe fn max_at_pe(&self, index: usize, len: usize, root_pe: usize) -> CollectiveReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
        .as_base::<T>()
        .reduce(ReduceOp::Max, index, len, root_pe)
    }
    pub unsafe fn sum_at_pe(&self, index: usize, len: usize, root_pe: usize) -> CollectiveReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce(ReduceOp::Sum, index, len, root_pe)
    }
            pub unsafe fn prod_at_pe(&self, index: usize, len: usize, root_pe: usize) -> CollectiveReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce(ReduceOp::Prod, index, len, root_pe)
    }
            pub unsafe fn bit_or_at_pe(&self, index: usize, len: usize, root_pe: usize) -> CollectiveReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce(ReduceOp::BitOr, index, len, root_pe)
    }
            pub unsafe fn bit_xor_at_pe(&self, index: usize, len: usize, root_pe: usize) -> CollectiveReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce(ReduceOp::BitXor, index, len, root_pe)
    }
            pub unsafe fn bit_and_at_pe(&self, index: usize, len: usize, root_pe: usize) -> CollectiveReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce(ReduceOp::BitAnd, index, len, root_pe)
    }
            pub unsafe fn min_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce_into_buffer(ReduceOp::Min, index, len, target)
    }
            pub unsafe fn max_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
        .as_base::<T>()
        .reduce_into_buffer(ReduceOp::Max, index, len, target)
    }
    pub unsafe fn sum_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce_into_buffer(ReduceOp::Sum, index, len, target)
    }
            pub unsafe fn prod_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce_into_buffer(ReduceOp::Prod, index, len, target)
    }
            pub unsafe fn bit_or_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce_into_buffer(ReduceOp::BitOr, index, len, target)
    }
            pub unsafe fn bit_xor_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce_into_buffer(ReduceOp::BitXor, index, len, target)
    }
            pub unsafe fn bit_and_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce_into_buffer(ReduceOp::BitAnd, index, len, target)
    }
    // pub unsafe fn min_at_pe_in_place(&self, root_pe: usize) -> CollectiveReduceInPlaceOpHandle<T> {
    //     // let slice = self.as_slice();
    //     self.mr
    //         .as_base::<T>()
    //         .reduce_in_place(ReduceOp::Min, root_pe)
    // }
    // pub unsafe fn max_at_pe_in_place(&self, root_pe: usize) -> CollectiveReduceInPlaceOpHandle<T> {
    //     // let slice = self.as_slice();
    //     self.mr
    //     .as_base::<T>()
    //     .reduce_in_place(ReduceOp::Max, root_pe)
    // }
    // pub unsafe fn sum_at_pe_in_place(&self, root_pe: usize) -> CollectiveReduceInPlaceOpHandle<T> {
    //     // let slice = self.as_slice();
    //     self.mr
    //         .as_base::<T>()
    //         .reduce_in_place(ReduceOp::Sum, root_pe)
    // }
    // pub unsafe fn prod_at_pe_in_place(&self, root_pe: usize) -> CollectiveReduceInPlaceOpHandle<T> {
    //     // let slice = self.as_slice();
    //     self.mr
    //         .as_base::<T>()
    //         .reduce_in_place(ReduceOp::Prod, root_pe)
    // }
    // pub unsafe fn bit_or_at_pe_in_place(&self, root_pe: usize) -> CollectiveReduceInPlaceOpHandle<T> {
    //     // let slice = self.as_slice();
    //     self.mr
    //         .as_base::<T>()
    //         .reduce_in_place(ReduceOp::BitOr, root_pe)
    // }
    // pub unsafe fn bit_xor_at_pe_in_place(&self, root_pe: usize) -> CollectiveReduceInPlaceOpHandle<T> {
    //     // let slice = self.as_slice();
    //     self.mr
    //         .as_base::<T>()
    //         .reduce_in_place(ReduceOp::BitXor, root_pe)
    // }
    // pub unsafe fn bit_and_at_pe_in_place(&self, root_pe: usize) -> CollectiveReduceInPlaceOpHandle<T> {
    //     // let slice = self.as_slice();
    //     self.mr
    //         .as_base::<T>()
    //         .reduce_in_place(ReduceOp::BitAnd, root_pe)
    // }
        pub unsafe fn gather_all(&self, index: usize, len: usize) -> CollectiveAllGatherOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .gather_all(index, len)
    }
        pub unsafe fn gather_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> CollectiveAllGatherIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .gather_all_into_buffer(index, len, buffer)
    }
        pub unsafe fn gather_at_pe(&self, index: usize, len: usize, root_pe: usize) -> CollectiveGatherOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .gather(index, len, root_pe)
    }
        pub unsafe fn gather_at_pe_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, root_or_buffer: RootOrLamellarBuffer<T, B>) -> CollectiveGatherIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .gather_into_buffer(index, len, root_or_buffer)
    }
    pub unsafe fn broadcast_all(&self,  index:usize, len: usize) -> CollectiveAllToAllOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .alltoall(index, len)
    }
    pub unsafe fn broadcast_all_into_buffer<B: AsLamellarBuffer<T>>(&self,  index:usize, len: usize, buffer: LamellarBuffer<T, B>) -> CollectiveAllToAllIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .alltoall_into_buffer(index, len, buffer)
    }
        pub unsafe fn broadcast_from_pe(&self, src_or_root_pe: BroadcastInput, len: usize) -> CollectiveBroadcastOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .broadcast(src_or_root_pe, len)
    }
        pub unsafe fn broadcast_from_pe_into_buffer<B: AsLamellarBuffer<T>> (&self, root_or_buffer: RootSrcOrLamellarBuffer<T, B>, len: usize) -> CollectiveBroadcastIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .broadcast_into_buffer(root_or_buffer, len)
    }
        pub unsafe fn scatter_from_pe(&self, src_or_root_pe: ScatterInput, len: usize) -> CollectiveScatterOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .scatter(src_or_root_pe, len)
    }
        pub unsafe fn scatter_from_pe_into_buffer<B: AsLamellarBuffer<T>> (&self, buffer: LamellarBuffer<T, B>, src_or_root_pe: ScatterInput, len: usize) -> CollectiveScatterIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .scatter_into_buffer(buffer, src_or_root_pe, len)
    }
    pub unsafe fn min_scatter(&self, index: usize, len: usize) -> CollectiveReduceScatterOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce_scatter(ReduceOp::Min, index, len)
    }
            pub unsafe fn max_scatter(&self, index: usize, len: usize) -> CollectiveReduceScatterOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce_scatter(ReduceOp::Max, index, len)
    }
            pub unsafe fn sum_scatter(&self, index: usize, len: usize) -> CollectiveReduceScatterOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce_scatter(ReduceOp::Sum, index, len)
    }
            pub unsafe fn prod_scatter(&self, index: usize, len: usize) -> CollectiveReduceScatterOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce_scatter(ReduceOp::Prod, index, len)
    }
            pub unsafe fn bit_or_scatter(&self, index: usize, len: usize) -> CollectiveReduceScatterOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce_scatter(ReduceOp::BitOr, index, len)
    }
            pub unsafe fn bit_xor_scatter(&self, index: usize, len: usize) -> CollectiveReduceScatterOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce_scatter(ReduceOp::BitXor, index, len)
    }
            pub unsafe fn bit_and_scatter(&self, index: usize, len: usize) -> CollectiveReduceScatterOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce_scatter(ReduceOp::BitAnd, index, len)
    }
            pub unsafe fn min_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce_scatter_into_buffer(ReduceOp::Min, index, len, buffer)
    }
            pub unsafe fn max_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
        .as_base::<T>()
        .reduce_scatter_into_buffer(ReduceOp::Max, index, len, buffer)
    }
    pub unsafe fn sum_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce_scatter_into_buffer(ReduceOp::Sum, index, len, buffer)
    }
            pub unsafe fn prod_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce_scatter_into_buffer(ReduceOp::Prod, index, len, buffer)
    }
            pub unsafe fn bit_or_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce_scatter_into_buffer(ReduceOp::BitOr, index, len, buffer)
    }
            pub unsafe fn bit_xor_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce_scatter_into_buffer(ReduceOp::BitXor, index, len, buffer)
    }
            pub unsafe fn bit_and_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
                .reduce_scatter_into_buffer(ReduceOp::BitAnd, index, len, buffer)
    }

    /// Blocks until all outstanding RDMA operations issued by this PE on this memory region
    /// have completed.
    pub fn wait_all(&self) {
        self.mr.wait_all();
    }
}

// This could be useful for if we want to transfer the actual data instead of the pointer
// impl<T: Remote + serde::Serialize> SharedMemoryRegion<T> {
//     pub(crate) fn serialize_local_data<S>(&self, s: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer,
//     {
//         unsafe { self.as_slice().unwrap().serialize(s) }
//     }
// }

//account for subregion stuff
impl<T: Remote> RegisteredMemoryRegion<T> for SharedMemoryRegion<T> {
    fn len(&self) -> usize {
        self.sub_region_size
    }
    fn addr(&self) -> MemResult<CommAllocAddr> {
        let addr = self.mr.addr()?;
        Ok(addr + self.sub_region_offset * std::mem::size_of::<T>())
    }
    unsafe fn as_slice(&self) -> &[T] {
        self.as_mut_slice()
    }
    unsafe fn as_mut_slice(&self) -> &mut [T] {
        let slice = self
            .mr
            .as_casted_mut_slice::<T>()
            .expect("should be aligned");
        if slice.len() >= self.sub_region_size + self.sub_region_offset {
            &mut slice[self.sub_region_offset..(self.sub_region_offset + self.sub_region_size)]
        } else {
            &mut slice[self.sub_region_offset..]
        }
    }
    unsafe fn as_ptr(&self) -> MemResult<*const T> {
        self.addr().map(|addr| addr.as_ptr())
    }
    unsafe fn as_mut_ptr(&self) -> MemResult<*mut T> {
        self.addr().map(|addr| addr.as_mut_ptr())
    }
}

impl<T: Remote> MemRegionId for SharedMemoryRegion<T> {
    fn id(&self) -> usize {
        self.mr.id()
    }
}

impl<T: Remote> SubRegion<T> for SharedMemoryRegion<T> {
    fn sub_region<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self {
        let start = match range.start_bound() {
            //inclusive
            Bound::Included(idx) => *idx,
            Bound::Excluded(idx) => *idx + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            //exclusive
            Bound::Included(idx) => *idx + 1,
            Bound::Excluded(idx) => *idx,
            Bound::Unbounded => self.sub_region_size,
        };
        if end > self.sub_region_size {
            panic!(
                "subregion range ({:?}-{:?}) exceeds size of memregion {:?}",
                start, end, self.sub_region_size
            );
        }
        // println!("shared subregion: {:?} {:?} {:?}",start,end,(end-start));
        SharedMemoryRegion {
            mr: self.mr.clone(),
            sub_region_offset: self.sub_region_offset + start,
            sub_region_size: (end - start),
            phantom: PhantomData,
        }
    }
}

impl<T: Remote> RTMemoryRegionRDMA<T> for SharedMemoryRegion<T> {
    unsafe fn put(&self, pe: usize, index: usize, data: T) -> RdmaHandle<T> {
        self.mr
            .as_base::<T>()
            .put(pe, self.sub_region_offset + index, data)
    }
    unsafe fn put_blocking(&self, pe: usize, index: usize, data: T) {
        self.mr
            .as_base::<T>()
            .put_blocking(pe, self.sub_region_offset + index, data)
    }
    unsafe fn put_unmanaged(&self, pe: usize, index: usize, data: T) {
        self.mr
            .alloc
            .inner_alloc
            .put_unmanaged(data, pe, self.sub_region_offset + index);
    }
    unsafe fn put_buffer(
        &self,
        pe: usize,
        index: usize,
        data: impl Into<MemregionRdmaInputInner<T>>,
    ) -> RdmaHandle<T> {
        self.mr
            .as_base::<T>()
            .put_buffer(pe, self.sub_region_offset + index, data)
    }
    unsafe fn put_buffer_unmanaged(
        &self,
        pe: usize,
        index: usize,
        data: impl Into<MemregionRdmaInputInner<T>>,
    ) {
        self.mr
            .alloc
            .inner_alloc
            .put_buffer_unmanaged(data, pe, self.sub_region_offset + index);
    }
    unsafe fn put_all(&self, index: usize, data: T) -> RdmaHandle<T> {
        self.mr
            .as_base::<T>()
            .put_all(self.sub_region_offset + index, data)
    }
    unsafe fn put_all_unmanaged(&self, index: usize, data: T) {
        self.mr
            .alloc
            .inner_alloc
            .put_all_unmanaged(data, self.sub_region_offset + index);
    }
    unsafe fn put_all_buffer(
        &self,
        index: usize,
        data: impl Into<MemregionRdmaInputInner<T>>,
    ) -> RdmaHandle<T> {
        self.mr
            .as_base::<T>()
            .put_all_buffer(self.sub_region_offset + index, data)
    }
    unsafe fn put_all_buffer_unmanaged(
        &self,
        index: usize,
        data: impl Into<MemregionRdmaInputInner<T>>,
    ) {
        self.mr
            .alloc
            .inner_alloc
            .put_all_buffer_unmanaged(data, self.sub_region_offset + index);
    }

    unsafe fn get(&self, pe: usize, index: usize) -> RdmaGetHandle<T> {
        self.mr
            .as_base::<T>()
            .get(pe, self.sub_region_offset + index)
    }
    unsafe fn get_buffer(&self, pe: usize, index: usize, len: usize) -> RdmaGetBufferHandle<T> {
        self.mr
            .as_base::<T>()
            .get_buffer(pe, self.sub_region_offset + index, len)
    }
    unsafe fn get_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) -> RdmaGetIntoBufferHandle<T, B> {
        self.mr
            .as_base::<T>()
            .get_into_buffer(pe, self.sub_region_offset + index, data)
    }
    unsafe fn get_into_buffer_unmanaged<B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        index: usize,
        data: LamellarBuffer<T, B>,
    ) {
        self.mr
            .as_base::<T>()
            .get_into_buffer_unmanaged(pe, self.sub_region_offset + index, data);
    }
    // unsafe fn put_comm_slice(&self, pe: usize, index: usize, data: CommSlice<T>) -> RdmaHandle<T> {
    //     self.mr
    //         .put_comm_slice(pe, self.sub_region_offset + index, data)
    // }
    // unsafe fn get_comm_slice(&self, pe: usize, index: usize, data: CommSlice<T>) -> RdmaHandle<T> {
    //     // println!("iget_slice {:?} {:?}",pe,self.sub_region_offset + index);
    //     self.mr
    //         .get_comm_slice(pe, self.sub_region_offset + index, data)
    // }
}

impl<T: Remote> std::fmt::Debug for SharedMemoryRegion<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{:?}] shared mem region:  {:?} ", self.mr.pe, self.mr,)
    }
}

impl<T: Remote> LamellarWrite for SharedMemoryRegion<T> {}
impl<T: Remote> LamellarRead for SharedMemoryRegion<T> {}

impl<T: Remote> From<&SharedMemoryRegion<T>> for LamellarMemoryRegion<T> {
    fn from(smr: &SharedMemoryRegion<T>) -> Self {
        LamellarMemoryRegion::Shared(smr.clone())
    }
}

impl<T: Dist> From<&SharedMemoryRegion<T>> for LamellarArrayRdmaOutput<T> {
    fn from(smr: &SharedMemoryRegion<T>) -> Self {
        // println!("from");
        LamellarArrayRdmaOutput::SharedMemRegion(smr.clone())
    }
}

impl<T: Dist> TeamFrom<&SharedMemoryRegion<T>> for LamellarArrayRdmaOutput<T> {
    fn team_from(smr: &SharedMemoryRegion<T>, _team: &Arc<LamellarTeam>) -> Self {
        LamellarArrayRdmaOutput::SharedMemRegion(smr.clone())
    }
}

impl<T: Dist> From<&SharedMemoryRegion<T>> for LamellarArrayRdmaInput<T> {
    fn from(smr: &SharedMemoryRegion<T>) -> Self {
        // println!("from");
        LamellarArrayRdmaInput::SharedMemRegion(smr.clone())
    }
}

impl<T: Dist> TeamFrom<&SharedMemoryRegion<T>> for LamellarArrayRdmaInput<T> {
    fn team_from(smr: &SharedMemoryRegion<T>, _team: &Arc<LamellarTeam>) -> Self {
        LamellarArrayRdmaInput::SharedMemRegion(smr.clone())
    }
}

impl<T: Dist> TeamTryFrom<&SharedMemoryRegion<T>> for LamellarArrayRdmaOutput<T> {
    fn team_try_from(
        smr: &SharedMemoryRegion<T>,
        _team: &Arc<LamellarTeam>,
    ) -> Result<Self, anyhow::Error> {
        Ok(LamellarArrayRdmaOutput::SharedMemRegion(smr.clone()))
    }
}

impl<T: Dist> TeamTryFrom<&SharedMemoryRegion<T>> for LamellarArrayRdmaInput<T> {
    fn team_try_from(
        smr: &SharedMemoryRegion<T>,
        _team: &Arc<LamellarTeam>,
    ) -> Result<Self, anyhow::Error> {
        Ok(LamellarArrayRdmaInput::SharedMemRegion(smr.clone()))
    }
}

impl<T: Remote> Drop for SharedMemoryRegion<T> {
    fn drop(&mut self) {
        trace!(target: "drop", "drop SharedMemoryRegion");
    }
}

use crate::active_messaging::RemotePtr;
use crate::array::{LamellarRead, LamellarWrite, TeamTryFrom};
use crate::darc::Darc;
use crate::lamellae::collective::{
    CollectiveAllReduceInPlaceOpHandle, CollectiveAllReduceIntoBufferOpHandle,
    CollectiveBroadcastIntoBufferOpHandle, CollectiveBroadcastOpHandle,
    CollectiveGatherIntoBufferOpHandle, CollectiveGatherOpHandle,
};
use crate::lamellae::{
    AllocationType, AtomicCompareExchangeOpHandle, LamellaeUtil, RdmaGetBufferHandle,
    RdmaGetIntoBufferHandle,
};
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
    pub(crate) fn lamellae(&self) -> Arc<crate::lamellae::Lamellae> {
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
    /// let slice = unsafe{mem_region.as_slice()};
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
    /// let slice =unsafe { mem_region.as_mut_slice()};
    ///```
    pub unsafe fn as_mut_slice(&self) -> &mut [T] {
        RegisteredMemoryRegion::as_mut_slice(self)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Copy `src` into the local (to the calling PE) data of the memory region.
    ///
    /// Equivalent to `unsafe { mem_region.as_mut_slice().copy_from_slice(src) }`, except that on
    /// backends whose RDMA `get` is not itself ordered against local stores (e.g. the shmem
    /// backend, where a "get" is just a cross-process pointer read with no NIC-enforced
    /// completion ordering) this also issues the fence needed to make the write visible to a
    /// remote PE that reads this region via RDMA. Prefer this over `as_mut_slice().copy_from_slice()`
    /// whenever the buffer may be read remotely.
    ///
    /// # Safety
    /// this call is always unsafe as there is no gaurantee that there do not exist other mutable references elsewhere in the distributed system.
    ///
    /// # One-sided Operation
    /// the result is only written on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1000).block();
    /// unsafe { mem_region.local_copy_from_slice(&[0usize; 1000]) };
    ///```
    pub unsafe fn local_copy_from_slice(&self, src: &[T]) {
        self.as_mut_slice().copy_from_slice(src);
        if let crate::lamellae::Lamellae::Shmem(_) = &*self.lamellae() {
            std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
        }
        #[cfg(feature = "enable-shmem-opt")]
        if let crate::lamellae::Lamellae::ShmemOpt(_) = &*self.lamellae() {
            std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
        }
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
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1000).block();
    /// assert_eq!(mem_region.len(), 1000);
    ///```
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
    /// Initiates a remote write of a single element to the given `pe` without returning a
    /// handle for it.
    ///
    /// The transfer cannot be tracked individually and is not counted in the runtime's request
    /// counters, but [`wait_all`][Self::wait_all] flushes the communication layer: once it
    /// returns, every earlier unmanaged put from this PE has completed. Until then the
    /// destination must not be read.
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
    ///     for (i, elem) in src.as_mut_slice().iter_mut().enumerate() {
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
    /// Initiates a remote write of a contiguous buffer to the given `pe` without returning a
    /// handle for it.
    ///
    /// The transfer cannot be tracked individually and is not counted in the runtime's request
    /// counters, but [`wait_all`][Self::wait_all] flushes the communication layer: once it
    /// returns, every earlier unmanaged put from this PE has completed. Until then the source
    /// must stay valid and unmodified (some backends have copied it by the time this call
    /// returns, others have not) and the destination must not be read.
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
    ///     for (i, elem) in src.as_mut_slice().iter_mut().enumerate() {
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
    /// Broadcasts a single element to **all** PEs at `index` without returning a handle for the
    /// transfers.
    ///
    /// The transfers cannot be tracked individually, but [`wait_all`][Self::wait_all] flushes
    /// the communication layer: once it returns, every earlier unmanaged put from this PE has
    /// completed.
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
    ///     for (i, elem) in src.as_mut_slice().iter_mut().enumerate() {
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
    /// Broadcasts a contiguous buffer to **all** PEs at `index` without returning a handle for
    /// the transfers.
    ///
    /// The transfers cannot be tracked individually, but [`wait_all`][Self::wait_all] flushes
    /// the communication layer: once it returns, every earlier unmanaged put from this PE has
    /// completed. Until then the source must stay valid and unmodified.
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
    ///     for (i, elem) in src.as_mut_slice().iter_mut().enumerate() {
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
    ///     for (i, elem) in mem_region.as_mut_slice().iter_mut().enumerate() {
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
    ///     for (i, elem) in mem_region.as_mut_slice().iter_mut().enumerate() {
    ///         *elem = i;
    ///     }
    ///     let buf = LamellarBuffer::from_vec(&world, vec![0usize; 10]);
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
    ///     for (i, elem) in mem_region.as_mut_slice().iter_mut().enumerate() {
    ///         *elem = i;
    ///     }
    ///     let buf = LamellarBuffer::from_vec(&world, vec![0usize; 10]);
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
    #[doc(alias("One-sided", "onesided"))]
    /// Atomically adds `val` into the element at `index` on the given `pe`, returning the
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
    ///     let old = mem_region.atomic_fetch_add(my_pe, my_pe, my_pe + 1).block();
    ///     assert_eq!(old, my_pe);
    ///     assert_eq!(mem_region.atomic_load(my_pe, my_pe).block(), my_pe + (my_pe + 1));
    /// }
    ///```
    pub unsafe fn atomic_fetch_add(&self, pe: usize, index: usize, val: T) -> AtomicFetchOpHandle<T> {
        self.mr.as_base::<T>().atomic_fetch_op(
            pe,
            self.sub_region_offset + index,
            AtomicOp::FetchSum(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Atomically bitwise-ANDs `val` into the element at `index` on the given `pe`, returning the
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
    ///     let old = mem_region.atomic_fetch_bit_and(my_pe, my_pe, usize::MAX).block();
    ///     assert_eq!(old, my_pe);
    ///     assert_eq!(mem_region.atomic_load(my_pe, my_pe).block(), my_pe);
    /// }
    ///```
    pub unsafe fn atomic_fetch_bit_and(&self, pe: usize, index: usize, val: T) -> AtomicFetchOpHandle<T> {
        self.mr.as_base::<T>().atomic_fetch_op(
            pe,
            self.sub_region_offset + index,
            AtomicOp::FetchBitAnd(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Atomically bitwise-ORs `val` into the element at `index` on the given `pe`, returning the
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
    ///     let old = mem_region.atomic_fetch_bit_or(my_pe, my_pe, 1 << 8).block();
    ///     assert_eq!(old, my_pe);
    ///     assert_eq!(mem_region.atomic_load(my_pe, my_pe).block(), my_pe | (1 << 8));
    /// }
    ///```
    pub unsafe fn atomic_fetch_bit_or(&self, pe: usize, index: usize, val: T) -> AtomicFetchOpHandle<T> {
        self.mr.as_base::<T>().atomic_fetch_op(
            pe,
            self.sub_region_offset + index,
            AtomicOp::FetchBitOr(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Atomically bitwise-XORs `val` into the element at `index` on the given `pe`, returning the
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
    ///     let old = mem_region.atomic_fetch_bit_xor(my_pe, my_pe, usize::MAX).block();
    ///     assert_eq!(old, my_pe);
    ///     assert_eq!(mem_region.atomic_load(my_pe, my_pe).block(), !my_pe);
    /// }
    ///```
    pub unsafe fn atomic_fetch_bit_xor(&self, pe: usize, index: usize, val: T) -> AtomicFetchOpHandle<T> {
        self.mr.as_base::<T>().atomic_fetch_op(
            pe,
            self.sub_region_offset + index,
            AtomicOp::FetchBitXor(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Blocking form of [`atomic_store`](Self::atomic_store): atomically stores `val` into the element at `index` on `pe`.
    ///
    /// Completes the operation before returning, without creating a handle.
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
    ///     mem_region.atomic_store_blocking(my_pe, my_pe, my_pe);
    ///     assert_eq!(mem_region.atomic_load_blocking(my_pe, my_pe), my_pe);
    /// }
    ///```
    pub unsafe fn atomic_store_blocking(&self, pe: usize, index: usize, val: T) {
        self.mr.as_base::<T>().atomic_op_blocking(pe, self.sub_region_offset + index, AtomicOp::Write(Box::pin(val)))
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Blocking form of [`atomic_load`](Self::atomic_load): atomically loads the element at `index` on `pe`.
    ///
    /// Completes the operation before returning, without creating a handle.
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
    ///     mem_region.atomic_store_blocking(my_pe, my_pe, my_pe);
    ///     assert_eq!(mem_region.atomic_load_blocking(my_pe, my_pe), my_pe);
    /// }
    ///```
    pub unsafe fn atomic_load_blocking(&self, pe: usize, index: usize) -> T {
        self.mr.as_base::<T>().atomic_fetch_op_blocking(
            pe,
            self.sub_region_offset + index,
            AtomicOp::Read(Box::pin(std::mem::zeroed())),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Blocking form of [`atomic_swap`](Self::atomic_swap): atomically swaps `val` into the element at `index` on `pe`, returning the old value.
    ///
    /// Completes the operation before returning, without creating a handle.
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
    ///     mem_region.atomic_store_blocking(my_pe, my_pe, my_pe);
    ///     assert_eq!(mem_region.atomic_swap_blocking(my_pe, my_pe, 0), my_pe);
    /// }
    ///```
    pub unsafe fn atomic_swap_blocking(&self, pe: usize, index: usize, val: T) -> T {
        self.mr.as_base::<T>().atomic_fetch_op_blocking(
            pe,
            self.sub_region_offset + index,
            AtomicOp::Write(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Blocking form of [`atomic_fetch_add`](Self::atomic_fetch_add): fetch-and-add on the element at `index` on `pe`, returning the old value.
    ///
    /// Completes the operation before returning, without creating a handle.
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
    ///     mem_region.atomic_store_blocking(my_pe, my_pe, my_pe);
    ///     assert_eq!(mem_region.atomic_fetch_add_blocking(my_pe, my_pe, 1), my_pe);
    /// }
    ///```
    pub unsafe fn atomic_fetch_add_blocking(&self, pe: usize, index: usize, val: T) -> T {
        self.mr.as_base::<T>().atomic_fetch_op_blocking(
            pe,
            self.sub_region_offset + index,
            AtomicOp::FetchSum(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Blocking form of [`atomic_fetch_bit_and`](Self::atomic_fetch_bit_and): fetch-and-and on the element at `index` on `pe`, returning the old value.
    ///
    /// Completes the operation before returning, without creating a handle.
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
    ///     mem_region.atomic_store_blocking(my_pe, my_pe, my_pe);
    ///     assert_eq!(mem_region.atomic_fetch_bit_and_blocking(my_pe, my_pe, usize::MAX), my_pe);
    /// }
    ///```
    pub unsafe fn atomic_fetch_bit_and_blocking(&self, pe: usize, index: usize, val: T) -> T {
        self.mr.as_base::<T>().atomic_fetch_op_blocking(
            pe,
            self.sub_region_offset + index,
            AtomicOp::FetchBitAnd(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Blocking form of [`atomic_fetch_bit_or`](Self::atomic_fetch_bit_or): fetch-and-or on the element at `index` on `pe`, returning the old value.
    ///
    /// Completes the operation before returning, without creating a handle.
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
    ///     mem_region.atomic_store_blocking(my_pe, my_pe, my_pe);
    ///     assert_eq!(mem_region.atomic_fetch_bit_or_blocking(my_pe, my_pe, 0), my_pe);
    /// }
    ///```
    pub unsafe fn atomic_fetch_bit_or_blocking(&self, pe: usize, index: usize, val: T) -> T {
        self.mr.as_base::<T>().atomic_fetch_op_blocking(
            pe,
            self.sub_region_offset + index,
            AtomicOp::FetchBitOr(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Blocking form of [`atomic_fetch_bit_xor`](Self::atomic_fetch_bit_xor): fetch-and-xor on the element at `index` on `pe`, returning the old value.
    ///
    /// Completes the operation before returning, without creating a handle.
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
    ///     mem_region.atomic_store_blocking(my_pe, my_pe, my_pe);
    ///     assert_eq!(mem_region.atomic_fetch_bit_xor_blocking(my_pe, my_pe, 0), my_pe);
    /// }
    ///```
    pub unsafe fn atomic_fetch_bit_xor_blocking(&self, pe: usize, index: usize, val: T) -> T {
        self.mr.as_base::<T>().atomic_fetch_op_blocking(
            pe,
            self.sub_region_offset + index,
            AtomicOp::FetchBitXor(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Atomically adds `val` to the element at `index` on the given `pe`.
    ///
    /// Returns an [`AtomicOpHandle`] that must be `block()`ed, awaited or `spawn()`ed.
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
    ///     mem_region.atomic_store(my_pe, my_pe, 6).block();
    ///     mem_region.atomic_add(my_pe, my_pe, 3).block();
    ///     assert_eq!(mem_region.atomic_load(my_pe, my_pe).block(), 9);
    /// }
    ///```
    pub unsafe fn atomic_add(&self, pe: usize, index: usize, val: T) -> AtomicOpHandle<T> {
        self.mr.as_base::<T>().atomic_op(
            pe,
            self.sub_region_offset + index,
            AtomicOp::Sum(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Blocking form of [`atomic_add`](Self::atomic_add): completes before returning.
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
    ///     mem_region.atomic_store(my_pe, my_pe, 6).block();
    ///     mem_region.atomic_add_blocking(my_pe, my_pe, 3);
    ///     assert_eq!(mem_region.atomic_load_blocking(my_pe, my_pe), 9);
    /// }
    ///```
    pub unsafe fn atomic_add_blocking(&self, pe: usize, index: usize, val: T) {
        self.mr.as_base::<T>().atomic_op_blocking(
            pe,
            self.sub_region_offset + index,
            AtomicOp::Sum(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Unmanaged form of [`atomic_add`](Self::atomic_add): the operation is issued without a handle
    /// and is not tracked; synchronize with a barrier or `wait_all` before relying on the result.
    ///
    /// # Safety
    /// This call is unsafe because the caller must ensure `index` is within bounds.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the atomic operation; the target PE is not notified.
    ///
    pub unsafe fn atomic_add_unmanaged(&self, pe: usize, index: usize, val: T) {
        self.mr.as_base::<T>().atomic_op_unmanaged(
            pe,
            self.sub_region_offset + index,
            AtomicOp::Sum(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Atomically subtracts `val` from the element at `index` on the given `pe`.
    ///
    /// Returns an [`AtomicOpHandle`] that must be `block()`ed, awaited or `spawn()`ed.
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
    ///     mem_region.atomic_store(my_pe, my_pe, 6).block();
    ///     mem_region.atomic_sub(my_pe, my_pe, 3).block();
    ///     assert_eq!(mem_region.atomic_load(my_pe, my_pe).block(), 3);
    /// }
    ///```
    pub unsafe fn atomic_sub(&self, pe: usize, index: usize, val: T) -> AtomicOpHandle<T> {
        self.mr.as_base::<T>().atomic_op(
            pe,
            self.sub_region_offset + index,
            AtomicOp::Sub(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Blocking form of [`atomic_sub`](Self::atomic_sub): completes before returning.
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
    ///     mem_region.atomic_store(my_pe, my_pe, 6).block();
    ///     mem_region.atomic_sub_blocking(my_pe, my_pe, 3);
    ///     assert_eq!(mem_region.atomic_load_blocking(my_pe, my_pe), 3);
    /// }
    ///```
    pub unsafe fn atomic_sub_blocking(&self, pe: usize, index: usize, val: T) {
        self.mr.as_base::<T>().atomic_op_blocking(
            pe,
            self.sub_region_offset + index,
            AtomicOp::Sub(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Unmanaged form of [`atomic_sub`](Self::atomic_sub): the operation is issued without a handle
    /// and is not tracked; synchronize with a barrier or `wait_all` before relying on the result.
    ///
    /// # Safety
    /// This call is unsafe because the caller must ensure `index` is within bounds.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the atomic operation; the target PE is not notified.
    ///
    pub unsafe fn atomic_sub_unmanaged(&self, pe: usize, index: usize, val: T) {
        self.mr.as_base::<T>().atomic_op_unmanaged(
            pe,
            self.sub_region_offset + index,
            AtomicOp::Sub(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Atomically subtracts `val` from the element at `index` on the given `pe`, returning the previous value.
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
    ///     mem_region.atomic_store(my_pe, my_pe, 6).block();
    ///     assert_eq!(mem_region.atomic_fetch_sub(my_pe, my_pe, 3).block(), 6);
    ///     assert_eq!(mem_region.atomic_load(my_pe, my_pe).block(), 3);
    /// }
    ///```
    pub unsafe fn atomic_fetch_sub(&self, pe: usize, index: usize, val: T) -> AtomicFetchOpHandle<T> {
        self.mr.as_base::<T>().atomic_fetch_op(
            pe,
            self.sub_region_offset + index,
            AtomicOp::FetchSub(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Blocking form of [`atomic_fetch_sub`](Self::atomic_fetch_sub).
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
    ///     mem_region.atomic_store(my_pe, my_pe, 6).block();
    ///     assert_eq!(mem_region.atomic_fetch_sub_blocking(my_pe, my_pe, 3), 6);
    /// }
    ///```
    pub unsafe fn atomic_fetch_sub_blocking(&self, pe: usize, index: usize, val: T) -> T {
        self.mr.as_base::<T>().atomic_fetch_op_blocking(
            pe,
            self.sub_region_offset + index,
            AtomicOp::FetchSub(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Atomically multiplies by `val` the element at `index` on the given `pe`.
    ///
    /// Returns an [`AtomicOpHandle`] that must be `block()`ed, awaited or `spawn()`ed.
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
    ///     mem_region.atomic_store(my_pe, my_pe, 6).block();
    ///     mem_region.atomic_mul(my_pe, my_pe, 3).block();
    ///     assert_eq!(mem_region.atomic_load(my_pe, my_pe).block(), 18);
    /// }
    ///```
    pub unsafe fn atomic_mul(&self, pe: usize, index: usize, val: T) -> AtomicOpHandle<T> {
        self.mr.as_base::<T>().atomic_op(
            pe,
            self.sub_region_offset + index,
            AtomicOp::Prod(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Blocking form of [`atomic_mul`](Self::atomic_mul): completes before returning.
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
    ///     mem_region.atomic_store(my_pe, my_pe, 6).block();
    ///     mem_region.atomic_mul_blocking(my_pe, my_pe, 3);
    ///     assert_eq!(mem_region.atomic_load_blocking(my_pe, my_pe), 18);
    /// }
    ///```
    pub unsafe fn atomic_mul_blocking(&self, pe: usize, index: usize, val: T) {
        self.mr.as_base::<T>().atomic_op_blocking(
            pe,
            self.sub_region_offset + index,
            AtomicOp::Prod(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Unmanaged form of [`atomic_mul`](Self::atomic_mul): the operation is issued without a handle
    /// and is not tracked; synchronize with a barrier or `wait_all` before relying on the result.
    ///
    /// # Safety
    /// This call is unsafe because the caller must ensure `index` is within bounds.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the atomic operation; the target PE is not notified.
    ///
    pub unsafe fn atomic_mul_unmanaged(&self, pe: usize, index: usize, val: T) {
        self.mr.as_base::<T>().atomic_op_unmanaged(
            pe,
            self.sub_region_offset + index,
            AtomicOp::Prod(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Atomically multiplies by `val` the element at `index` on the given `pe`, returning the previous value.
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
    ///     mem_region.atomic_store(my_pe, my_pe, 6).block();
    ///     assert_eq!(mem_region.atomic_fetch_mul(my_pe, my_pe, 3).block(), 6);
    ///     assert_eq!(mem_region.atomic_load(my_pe, my_pe).block(), 18);
    /// }
    ///```
    pub unsafe fn atomic_fetch_mul(&self, pe: usize, index: usize, val: T) -> AtomicFetchOpHandle<T> {
        self.mr.as_base::<T>().atomic_fetch_op(
            pe,
            self.sub_region_offset + index,
            AtomicOp::FetchProd(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Blocking form of [`atomic_fetch_mul`](Self::atomic_fetch_mul).
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
    ///     mem_region.atomic_store(my_pe, my_pe, 6).block();
    ///     assert_eq!(mem_region.atomic_fetch_mul_blocking(my_pe, my_pe, 3), 6);
    /// }
    ///```
    pub unsafe fn atomic_fetch_mul_blocking(&self, pe: usize, index: usize, val: T) -> T {
        self.mr.as_base::<T>().atomic_fetch_op_blocking(
            pe,
            self.sub_region_offset + index,
            AtomicOp::FetchProd(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Atomically bitwise-ANDs `val` into the element at `index` on the given `pe`.
    ///
    /// Returns an [`AtomicOpHandle`] that must be `block()`ed, awaited or `spawn()`ed.
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
    ///     mem_region.atomic_store(my_pe, my_pe, 6).block();
    ///     mem_region.atomic_bit_and(my_pe, my_pe, 3).block();
    ///     assert_eq!(mem_region.atomic_load(my_pe, my_pe).block(), 2);
    /// }
    ///```
    pub unsafe fn atomic_bit_and(&self, pe: usize, index: usize, val: T) -> AtomicOpHandle<T> {
        self.mr.as_base::<T>().atomic_op(
            pe,
            self.sub_region_offset + index,
            AtomicOp::BitAnd(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Blocking form of [`atomic_bit_and`](Self::atomic_bit_and): completes before returning.
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
    ///     mem_region.atomic_store(my_pe, my_pe, 6).block();
    ///     mem_region.atomic_bit_and_blocking(my_pe, my_pe, 3);
    ///     assert_eq!(mem_region.atomic_load_blocking(my_pe, my_pe), 2);
    /// }
    ///```
    pub unsafe fn atomic_bit_and_blocking(&self, pe: usize, index: usize, val: T) {
        self.mr.as_base::<T>().atomic_op_blocking(
            pe,
            self.sub_region_offset + index,
            AtomicOp::BitAnd(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Unmanaged form of [`atomic_bit_and`](Self::atomic_bit_and): the operation is issued without a handle
    /// and is not tracked; synchronize with a barrier or `wait_all` before relying on the result.
    ///
    /// # Safety
    /// This call is unsafe because the caller must ensure `index` is within bounds.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the atomic operation; the target PE is not notified.
    ///
    pub unsafe fn atomic_bit_and_unmanaged(&self, pe: usize, index: usize, val: T) {
        self.mr.as_base::<T>().atomic_op_unmanaged(
            pe,
            self.sub_region_offset + index,
            AtomicOp::BitAnd(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Atomically bitwise-ORs `val` into the element at `index` on the given `pe`.
    ///
    /// Returns an [`AtomicOpHandle`] that must be `block()`ed, awaited or `spawn()`ed.
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
    ///     mem_region.atomic_store(my_pe, my_pe, 6).block();
    ///     mem_region.atomic_bit_or(my_pe, my_pe, 3).block();
    ///     assert_eq!(mem_region.atomic_load(my_pe, my_pe).block(), 7);
    /// }
    ///```
    pub unsafe fn atomic_bit_or(&self, pe: usize, index: usize, val: T) -> AtomicOpHandle<T> {
        self.mr.as_base::<T>().atomic_op(
            pe,
            self.sub_region_offset + index,
            AtomicOp::BitOr(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Blocking form of [`atomic_bit_or`](Self::atomic_bit_or): completes before returning.
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
    ///     mem_region.atomic_store(my_pe, my_pe, 6).block();
    ///     mem_region.atomic_bit_or_blocking(my_pe, my_pe, 3);
    ///     assert_eq!(mem_region.atomic_load_blocking(my_pe, my_pe), 7);
    /// }
    ///```
    pub unsafe fn atomic_bit_or_blocking(&self, pe: usize, index: usize, val: T) {
        self.mr.as_base::<T>().atomic_op_blocking(
            pe,
            self.sub_region_offset + index,
            AtomicOp::BitOr(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Unmanaged form of [`atomic_bit_or`](Self::atomic_bit_or): the operation is issued without a handle
    /// and is not tracked; synchronize with a barrier or `wait_all` before relying on the result.
    ///
    /// # Safety
    /// This call is unsafe because the caller must ensure `index` is within bounds.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the atomic operation; the target PE is not notified.
    ///
    pub unsafe fn atomic_bit_or_unmanaged(&self, pe: usize, index: usize, val: T) {
        self.mr.as_base::<T>().atomic_op_unmanaged(
            pe,
            self.sub_region_offset + index,
            AtomicOp::BitOr(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Atomically bitwise-XORs `val` into the element at `index` on the given `pe`.
    ///
    /// Returns an [`AtomicOpHandle`] that must be `block()`ed, awaited or `spawn()`ed.
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
    ///     mem_region.atomic_store(my_pe, my_pe, 6).block();
    ///     mem_region.atomic_bit_xor(my_pe, my_pe, 3).block();
    ///     assert_eq!(mem_region.atomic_load(my_pe, my_pe).block(), 5);
    /// }
    ///```
    pub unsafe fn atomic_bit_xor(&self, pe: usize, index: usize, val: T) -> AtomicOpHandle<T> {
        self.mr.as_base::<T>().atomic_op(
            pe,
            self.sub_region_offset + index,
            AtomicOp::BitXor(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Blocking form of [`atomic_bit_xor`](Self::atomic_bit_xor): completes before returning.
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
    ///     mem_region.atomic_store(my_pe, my_pe, 6).block();
    ///     mem_region.atomic_bit_xor_blocking(my_pe, my_pe, 3);
    ///     assert_eq!(mem_region.atomic_load_blocking(my_pe, my_pe), 5);
    /// }
    ///```
    pub unsafe fn atomic_bit_xor_blocking(&self, pe: usize, index: usize, val: T) {
        self.mr.as_base::<T>().atomic_op_blocking(
            pe,
            self.sub_region_offset + index,
            AtomicOp::BitXor(Box::pin(val)),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Unmanaged form of [`atomic_bit_xor`](Self::atomic_bit_xor): the operation is issued without a handle
    /// and is not tracked; synchronize with a barrier or `wait_all` before relying on the result.
    ///
    /// # Safety
    /// This call is unsafe because the caller must ensure `index` is within bounds.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the atomic operation; the target PE is not notified.
    ///
    pub unsafe fn atomic_bit_xor_unmanaged(&self, pe: usize, index: usize, val: T) {
        self.mr.as_base::<T>().atomic_op_unmanaged(
            pe,
            self.sub_region_offset + index,
            AtomicOp::BitXor(Box::pin(val)),
        )
    }

    /// Performs a collective all-reduce minimum over `len` elements starting at `index`.
    /// Returns a [`CollectiveAllReduceOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = my_pe + 1;
    ///     mem_region.min_all(0, num_pes).block();
    /// }
    ///```
    pub unsafe fn min_all(&self, index: usize, len: usize) -> CollectiveAllReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all(self.sub_region_offset + index, len, ReduceOp::Min)
    }
    /// Performs a collective all-reduce maximum over `len` elements starting at `index`.
    /// Returns a [`CollectiveAllReduceOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = my_pe + 1;
    ///     mem_region.max_all(0, num_pes).block();
    /// }
    ///```
    pub unsafe fn max_all(&self, index: usize, len: usize) -> CollectiveAllReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all(self.sub_region_offset + index, len, ReduceOp::Max)
    }
    /// Performs a collective all-reduce sum over `len` elements starting at `index`.
    /// Returns a [`CollectiveAllReduceOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = my_pe + 1;
    ///     mem_region.sum_all(0, num_pes).block();
    /// }
    ///```
    pub unsafe fn sum_all(&self, index: usize, len: usize) -> CollectiveAllReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all(self.sub_region_offset + index, len, ReduceOp::Sum)
    }
    /// Performs a collective all-reduce product over `len` elements starting at `index`.
    /// Returns a [`CollectiveAllReduceOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = my_pe + 1;
    ///     mem_region.prod_all(0, num_pes).block();
    /// }
    ///```
    pub unsafe fn prod_all(&self, index: usize, len: usize) -> CollectiveAllReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all(self.sub_region_offset + index, len, ReduceOp::Prod)
    }
    /// Performs a collective all-reduce bitwise OR over `len` elements starting at `index`.
    /// Returns a [`CollectiveAllReduceOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = 1 << my_pe;
    ///     mem_region.bit_or_all(0, num_pes).block();
    /// }
    ///```
    pub unsafe fn bit_or_all(&self, index: usize, len: usize) -> CollectiveAllReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all(self.sub_region_offset + index, len, ReduceOp::BitOr)
    }
    /// Performs a collective all-reduce bitwise XOR over `len` elements starting at `index`.
    /// Returns a [`CollectiveAllReduceOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = 1 << my_pe;
    ///     mem_region.bit_xor_all(0, num_pes).block();
    /// }
    ///```
    pub unsafe fn bit_xor_all(&self, index: usize, len: usize) -> CollectiveAllReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all(self.sub_region_offset + index, len, ReduceOp::BitXor)
    }
    /// Performs a collective all-reduce bitwise AND over `len` elements starting at `index`.
    /// Returns a [`CollectiveAllReduceOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = usize::MAX;
    ///     mem_region.bit_and_all(0, num_pes).block();
    /// }
    ///```
    pub unsafe fn bit_and_all(&self, index: usize, len: usize) -> CollectiveAllReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all(self.sub_region_offset + index, len, ReduceOp::BitAnd)
    }
    /// Performs a collective all-reduce minimum, storing results into `buffer`.
    /// Returns a [`CollectiveAllReduceIntoBufferOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; num_pes]);
    /// unsafe {
    ///     mem_region.as_mut_slice()[my_pe] = my_pe + 1;
    ///     mem_region.min_all_into_buffer(0, num_pes, buf).block();
    /// }
    ///```
    pub unsafe fn min_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_into_buffer(self.sub_region_offset + index, len, ReduceOp::Min, buffer)
    }
    /// Performs a collective all-reduce maximum, storing results into `buffer`.
    /// Returns a [`CollectiveAllReduceIntoBufferOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; num_pes]);
    /// unsafe {
    ///     mem_region.as_mut_slice()[my_pe] = my_pe + 1;
    ///     mem_region.max_all_into_buffer(0, num_pes, buf).block();
    /// }
    ///```
    pub unsafe fn max_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_into_buffer(self.sub_region_offset + index, len, ReduceOp::Max, buffer)
    }
    /// Performs a collective all-reduce sum, storing results into `buffer`.
    /// Returns a [`CollectiveAllReduceIntoBufferOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; num_pes]);
    /// unsafe {
    ///     mem_region.as_mut_slice()[my_pe] = my_pe + 1;
    ///     mem_region.sum_all_into_buffer(0, num_pes, buf).block();
    /// }
    ///```
    pub unsafe fn sum_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_into_buffer(self.sub_region_offset + index, len, ReduceOp::Sum, buffer)
    }
    /// Performs a collective all-reduce product, storing results into `buffer`.
    /// Returns a [`CollectiveAllReduceIntoBufferOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; num_pes]);
    /// unsafe {
    ///     mem_region.as_mut_slice()[my_pe] = my_pe + 1;
    ///     mem_region.prod_all_into_buffer(0, num_pes, buf).block();
    /// }
    ///```
    pub unsafe fn prod_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_into_buffer(self.sub_region_offset + index, len, ReduceOp::Prod, buffer)
    }
    /// Performs a collective all-reduce bitwise OR, storing results into `buffer`.
    /// Returns a [`CollectiveAllReduceIntoBufferOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; num_pes]);
    /// unsafe {
    ///     mem_region.as_mut_slice()[my_pe] = 1 << my_pe;
    ///     mem_region.bit_or_all_into_buffer(0, num_pes, buf).block();
    /// }
    ///```
    pub unsafe fn bit_or_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_into_buffer(self.sub_region_offset + index, len, ReduceOp::BitOr, buffer)
    }
    /// Performs a collective all-reduce bitwise XOR, storing results into `buffer`.
    /// Returns a [`CollectiveAllReduceIntoBufferOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; num_pes]);
    /// unsafe {
    ///     mem_region.as_mut_slice()[my_pe] = 1 << my_pe;
    ///     mem_region.bit_xor_all_into_buffer(0, num_pes, buf).block();
    /// }
    ///```
    pub unsafe fn bit_xor_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_into_buffer(self.sub_region_offset + index, len, ReduceOp::BitXor, buffer)
    }
    /// Performs a collective all-reduce bitwise AND, storing results into `buffer`.
    /// Returns a [`CollectiveAllReduceIntoBufferOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; num_pes]);
    /// unsafe {
    ///     mem_region.as_mut_slice()[my_pe] = usize::MAX;
    ///     mem_region.bit_and_all_into_buffer(0, num_pes, buf).block();
    /// }
    ///```
    pub unsafe fn bit_and_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_into_buffer(self.sub_region_offset + index, len, ReduceOp::BitAnd, buffer)
    }
    /// Performs an in-place collective all-reduce minimum using `src_and_dst` as both source and destination.
    /// Returns a [`CollectiveAllReduceInPlaceOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    /// let buf = LamellarBuffer::from_vec(&world, vec![my_pe + 1; num_pes]);
    /// unsafe {
    ///     mem_region.min_all_in_place(buf).block();
    /// }
    ///```
    pub unsafe fn min_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::Min)
    }
    /// Performs an in-place collective all-reduce maximum using `src_and_dst` as both source and destination.
    /// Returns a [`CollectiveAllReduceInPlaceOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    /// let buf = LamellarBuffer::from_vec(&world, vec![my_pe + 1; num_pes]);
    /// unsafe {
    ///     mem_region.max_all_in_place(buf).block();
    /// }
    ///```
    pub unsafe fn max_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::Max)
    }
    /// Performs an in-place collective all-reduce sum using `src_and_dst` as both source and destination.
    /// Returns a [`CollectiveAllReduceInPlaceOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    /// let buf = LamellarBuffer::from_vec(&world, vec![my_pe + 1; num_pes]);
    /// unsafe {
    ///     mem_region.sum_all_in_place(buf).block();
    /// }
    ///```
    pub unsafe fn sum_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::Sum)
    }
    /// Performs an in-place collective all-reduce product using `src_and_dst` as both source and destination.
    /// Returns a [`CollectiveAllReduceInPlaceOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    /// let buf = LamellarBuffer::from_vec(&world, vec![my_pe + 1; num_pes]);
    /// unsafe {
    ///     mem_region.prod_all_in_place(buf).block();
    /// }
    ///```
    pub unsafe fn prod_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::Prod)
    }
    /// Performs an in-place collective all-reduce bitwise OR using `src_and_dst` as both source and destination.
    /// Returns a [`CollectiveAllReduceInPlaceOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    /// let buf = LamellarBuffer::from_vec(&world, vec![1usize << my_pe; num_pes]);
    /// unsafe {
    ///     mem_region.bit_or_all_in_place(buf).block();
    /// }
    ///```
    pub unsafe fn bit_or_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::BitOr)
    }
    /// Performs an in-place collective all-reduce bitwise XOR using `src_and_dst` as both source and destination.
    /// Returns a [`CollectiveAllReduceInPlaceOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    /// let buf = LamellarBuffer::from_vec(&world, vec![1usize << my_pe; num_pes]);
    /// unsafe {
    ///     mem_region.bit_xor_all_in_place(buf).block();
    /// }
    ///```
    pub unsafe fn bit_xor_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::BitXor)
    }
    /// Performs an in-place collective all-reduce bitwise AND using `src_and_dst` as both source and destination.
    /// Returns a [`CollectiveAllReduceInPlaceOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    /// let buf = LamellarBuffer::from_vec(&world, vec![usize::MAX; num_pes]);
    /// unsafe {
    ///     mem_region.bit_and_all_in_place(buf).block();
    /// }
    ///```
    pub unsafe fn bit_and_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::BitAnd)
    }
    /// Performs a collective reduce minimum to `root_pe`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = my_pe + 1;
    ///     mem_region.min_at_pe(0, num_pes, 0).block();
    /// }
    ///```
    pub unsafe fn min_at_pe(
        &self,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce(ReduceOp::Min, index, len, root_pe)
    }
    /// Performs a collective reduce maximum to `root_pe`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = my_pe + 1;
    ///     mem_region.max_at_pe(0, num_pes, 0).block();
    /// }
    ///```
    pub unsafe fn max_at_pe(
        &self,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce(ReduceOp::Max, index, len, root_pe)
    }
    /// Performs a collective reduce sum to `root_pe`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = my_pe + 1;
    ///     mem_region.sum_at_pe(0, num_pes, 0).block();
    /// }
    ///```
    pub unsafe fn sum_at_pe(
        &self,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce(ReduceOp::Sum, index, len, root_pe)
    }
    /// Performs a collective reduce product to `root_pe`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = my_pe + 1;
    ///     mem_region.prod_at_pe(0, num_pes, 0).block();
    /// }
    ///```
    pub unsafe fn prod_at_pe(
        &self,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce(ReduceOp::Prod, index, len, root_pe)
    }
    /// Performs a collective reduce bitwise OR to `root_pe`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = 1 << my_pe;
    ///     mem_region.bit_or_at_pe(0, num_pes, 0).block();
    /// }
    ///```
    pub unsafe fn bit_or_at_pe(
        &self,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce(ReduceOp::BitOr, index, len, root_pe)
    }
    /// Performs a collective reduce bitwise XOR to `root_pe`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = 1 << my_pe;
    ///     mem_region.bit_xor_at_pe(0, num_pes, 0).block();
    /// }
    ///```
    pub unsafe fn bit_xor_at_pe(
        &self,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce(ReduceOp::BitXor, index, len, root_pe)
    }
    /// Performs a collective reduce bitwise AND to `root_pe`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = usize::MAX;
    ///     mem_region.bit_and_at_pe(0, num_pes, 0).block();
    /// }
    ///```
    pub unsafe fn bit_and_at_pe(
        &self,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveReduceOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce(ReduceOp::BitAnd, index, len, root_pe)
    }
    /// Performs a collective reduce minimum to `root_pe`, storing results into `target`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = my_pe + 1;
    ///     let target = if my_pe == 0 {
    ///         lamellar::RootOrLamellarBuffer::Root(LamellarBuffer::from_vec(&world, vec![0usize; num_pes]))
    ///     } else {
    ///         lamellar::RootOrLamellarBuffer::NotRoot(0)
    ///     };
    ///     mem_region.min_at_pe_into_buffer(0, num_pes, target).block();
    /// }
    ///```
    pub unsafe fn min_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::Min, index, len, target)
    }
    /// Performs a collective reduce maximum to `root_pe`, storing results into `target`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = my_pe + 1;
    ///     let target = if my_pe == 0 {
    ///         lamellar::RootOrLamellarBuffer::Root(LamellarBuffer::from_vec(&world, vec![0usize; num_pes]))
    ///     } else {
    ///         lamellar::RootOrLamellarBuffer::NotRoot(0)
    ///     };
    ///     mem_region.max_at_pe_into_buffer(0, num_pes, target).block();
    /// }
    ///```
    pub unsafe fn max_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::Max, index, len, target)
    }
    /// Performs a collective reduce sum to `root_pe`, storing results into `target`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = my_pe + 1;
    ///     let target = if my_pe == 0 {
    ///         lamellar::RootOrLamellarBuffer::Root(LamellarBuffer::from_vec(&world, vec![0usize; num_pes]))
    ///     } else {
    ///         lamellar::RootOrLamellarBuffer::NotRoot(0)
    ///     };
    ///     mem_region.sum_at_pe_into_buffer(0, num_pes, target).block();
    /// }
    ///```
    pub unsafe fn sum_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::Sum, index, len, target)
    }
    /// Performs a collective reduce product to `root_pe`, storing results into `target`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = my_pe + 1;
    ///     let target = if my_pe == 0 {
    ///         lamellar::RootOrLamellarBuffer::Root(LamellarBuffer::from_vec(&world, vec![0usize; num_pes]))
    ///     } else {
    ///         lamellar::RootOrLamellarBuffer::NotRoot(0)
    ///     };
    ///     mem_region.prod_at_pe_into_buffer(0, num_pes, target).block();
    /// }
    ///```
    pub unsafe fn prod_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::Prod, index, len, target)
    }
    /// Performs a collective reduce bitwise OR to `root_pe`, storing results into `target`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = 1 << my_pe;
    ///     let target = if my_pe == 0 {
    ///         lamellar::RootOrLamellarBuffer::Root(LamellarBuffer::from_vec(&world, vec![0usize; num_pes]))
    ///     } else {
    ///         lamellar::RootOrLamellarBuffer::NotRoot(0)
    ///     };
    ///     mem_region.bit_or_at_pe_into_buffer(0, num_pes, target).block();
    /// }
    ///```
    pub unsafe fn bit_or_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::BitOr, index, len, target)
    }
    /// Performs a collective reduce bitwise XOR to `root_pe`, storing results into `target`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = 1 << my_pe;
    ///     let target = if my_pe == 0 {
    ///         lamellar::RootOrLamellarBuffer::Root(LamellarBuffer::from_vec(&world, vec![0usize; num_pes]))
    ///     } else {
    ///         lamellar::RootOrLamellarBuffer::NotRoot(0)
    ///     };
    ///     mem_region.bit_xor_at_pe_into_buffer(0, num_pes, target).block();
    /// }
    ///```
    pub unsafe fn bit_xor_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::BitXor, index, len, target)
    }
    /// Performs a collective reduce bitwise AND to `root_pe`, storing results into `target`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = usize::MAX;
    ///     let target = if my_pe == 0 {
    ///         lamellar::RootOrLamellarBuffer::Root(LamellarBuffer::from_vec(&world, vec![0usize; num_pes]))
    ///     } else {
    ///         lamellar::RootOrLamellarBuffer::NotRoot(0)
    ///     };
    ///     mem_region.bit_and_at_pe_into_buffer(0, num_pes, target).block();
    /// }
    ///```
    pub unsafe fn bit_and_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> CollectiveReduceIntoBufferOpHandle<T, B> {
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
    /// Performs a collective all-gather.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = my_pe;
    ///     let gathered: Vec<usize> = mem_region.gather_all(0, 1).block();
    ///     assert_eq!(gathered.len(), num_pes);
    /// }
    ///```
    pub unsafe fn gather_all(&self, index: usize, len: usize) -> CollectiveAllGatherOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .gather_all(self.sub_region_offset + index, len)
    }
    /// Performs a collective all-gather, storing results into `buffer`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; num_pes]);
    /// unsafe {
    ///     mem_region.as_mut_slice()[my_pe] = my_pe;
    ///     mem_region.gather_all_into_buffer(0, 1, buf).block();
    /// }
    ///```
    pub unsafe fn gather_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> CollectiveAllGatherIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .gather_all_into_buffer(self.sub_region_offset + index, len, buffer)
    }
    /// Performs a collective gather to a single PE.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = my_pe;
    ///     if my_pe == 0 {
    ///         let gathered: Vec<usize> = mem_region.gather_at_pe(0, 1, 0).block().expect("PE is root");
    ///         assert_eq!(gathered.len(), num_pes);
    ///     } else {
    ///         mem_region.gather_at_pe(0, 1, 0).block();
    ///     }
    /// }
    ///```
    pub unsafe fn gather_at_pe(
        &self,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveGatherOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .gather(self.sub_region_offset + index, len, root_pe)
    }
    /// Performs a collective gather to a single PE, storing results into `target`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = my_pe;
    ///     let root_or_buffer = if my_pe == 0 {
    ///         lamellar::RootOrLamellarBuffer::Root(LamellarBuffer::from_vec(&world, vec![0usize; num_pes]))
    ///     } else {
    ///         lamellar::RootOrLamellarBuffer::NotRoot(0)
    ///     };
    ///     mem_region.gather_at_pe_into_buffer(0, 1, root_or_buffer).block();
    /// }
    ///```
    pub unsafe fn gather_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        root_or_buffer: RootOrLamellarBuffer<T, B>,
    ) -> CollectiveGatherIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .gather_into_buffer(self.sub_region_offset + index, len, root_or_buffer)
    }
    /// Broadcasts `len` elements starting at `index` from the root PE to all PEs.
    /// Returns a [`CollectiveAllToAllOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     mem_region.as_mut_slice()[my_pe] = my_pe;
    ///     mem_region.broadcast_all(0, 1).block();
    /// }
    ///```
    pub unsafe fn broadcast_all(&self, index: usize, len: usize) -> CollectiveAllToAllOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .alltoall(self.sub_region_offset + index, len)
    }
    /// Broadcasts `len` elements starting at `index` from the root PE to all PEs, storing results into `buffer`.
    /// Returns a [`CollectiveAllToAllIntoBufferOpHandle`] that must be `block()`ed or `spawn()`ed.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; num_pes]);
    /// unsafe {
    ///     mem_region.as_mut_slice()[my_pe] = my_pe;
    ///     mem_region.broadcast_all_into_buffer(0, 1, buf).block();
    /// }
    ///```
    pub unsafe fn broadcast_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> CollectiveAllToAllIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .alltoall_into_buffer(self.sub_region_offset + index, len, buffer)
    }
    /// Performs a collective broadcast from a root PE.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1).block();
    /// unsafe {
    ///     let input = if my_pe == 0 {
    ///         lamellar::BroadcastInput::root(0)
    ///     } else {
    ///         lamellar::BroadcastInput::not_root(0)
    ///     };
    ///     mem_region.broadcast_from_pe(input, 1).block();
    /// }
    ///```
    pub unsafe fn broadcast_from_pe(
        &self,
        src_or_root_pe: BroadcastInput,
        len: usize,
    ) -> CollectiveBroadcastOpHandle<T> {
        // let slice = self.as_slice();
        let src_or_root_pe = match src_or_root_pe {
            BroadcastInput::Root(index) => BroadcastInput::Root(self.sub_region_offset + index),
            BroadcastInput::NotRoot(root_pe) => BroadcastInput::NotRoot(root_pe),
        };
        self.mr.as_base::<T>().broadcast(src_or_root_pe, len)
    }
    /// Performs a collective broadcast from a root PE, storing results into `buffer`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1).block();
    /// unsafe {
    ///     let root_or_buffer = if my_pe == 0 {
    ///         lamellar::RootSrcOrLamellarBuffer::Root(0)
    ///     } else {
    ///         lamellar::RootSrcOrLamellarBuffer::NotRoot(LamellarBuffer::from_vec(&world, vec![0usize; 1]), 0)
    ///     };
    ///     mem_region.broadcast_from_pe_into_buffer(root_or_buffer, 1).block();
    /// }
    ///```
    pub unsafe fn broadcast_from_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        root_or_buffer: RootSrcOrLamellarBuffer<T, B>,
        len: usize,
    ) -> CollectiveBroadcastIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        let root_or_buffer = match root_or_buffer {
            RootSrcOrLamellarBuffer::Root(index) => {
                RootSrcOrLamellarBuffer::Root(self.sub_region_offset + index)
            }
            RootSrcOrLamellarBuffer::NotRoot(buffer, root_pe) => {
                RootSrcOrLamellarBuffer::NotRoot(buffer, root_pe)
            }
        };
        self.mr
            .as_base::<T>()
            .broadcast_into_buffer(root_or_buffer, len)
    }
    /// Performs a collective scatter from a root PE.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    ///     if my_pe == 0 {
    ///         let slice = mem_region.as_mut_slice();
    ///         for i in 0..num_pes { slice[i] = i; }
    ///     }
    ///     let input = if my_pe == 0 {
    ///         lamellar::ScatterInput::root(0)
    ///     } else {
    ///         lamellar::ScatterInput::not_root(0)
    ///     };
    ///     mem_region.scatter_from_pe(input, 1).block();
    /// }
    ///```
    pub unsafe fn scatter_from_pe(
        &self,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterOpHandle<T> {
        // let slice = self.as_slice();
        self.mr.as_base::<T>().scatter(src_or_root_pe, len)
    }
    /// Performs a collective scatter from a root PE, storing results into `buffer`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
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
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; 1]);
    /// unsafe {
    ///     if my_pe == 0 {
    ///         let slice = mem_region.as_mut_slice();
    ///         for i in 0..num_pes { slice[i] = i; }
    ///     }
    ///     let input = if my_pe == 0 {
    ///         lamellar::ScatterInput::root(0)
    ///     } else {
    ///         lamellar::ScatterInput::not_root(0)
    ///     };
    ///     mem_region.scatter_from_pe_into_buffer(buf, input, 1).block();
    /// }
    ///```
    pub unsafe fn scatter_from_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        buffer: LamellarBuffer<T, B>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .scatter_into_buffer(buffer, src_or_root_pe, len)
    }
    /// Performs a collective reduce-scatter minimum.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes * num_pes).block();
    /// unsafe {
    ///     let slice = mem_region.as_mut_slice();
    ///     for i in 0..num_pes * num_pes { slice[i] = my_pe + i + 1; }
    ///     mem_region.min_scatter(0, num_pes).block();
    /// }
    ///```
    pub unsafe fn min_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> CollectiveReduceScatterOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_scatter(ReduceOp::Min, index, len)
    }
    /// Performs a collective reduce-scatter maximum.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes * num_pes).block();
    /// unsafe {
    ///     let slice = mem_region.as_mut_slice();
    ///     for i in 0..num_pes * num_pes { slice[i] = my_pe + i + 1; }
    ///     mem_region.max_scatter(0, num_pes).block();
    /// }
    ///```
    pub unsafe fn max_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> CollectiveReduceScatterOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_scatter(ReduceOp::Max, index, len)
    }
    /// Performs a collective reduce-scatter sum.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes * num_pes).block();
    /// unsafe {
    ///     let slice = mem_region.as_mut_slice();
    ///     for i in 0..num_pes * num_pes { slice[i] = my_pe + i + 1; }
    ///     mem_region.sum_scatter(0, num_pes).block();
    /// }
    ///```
    pub unsafe fn sum_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> CollectiveReduceScatterOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_scatter(ReduceOp::Sum, index, len)
    }
    /// Performs a collective reduce-scatter product.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes * num_pes).block();
    /// unsafe {
    ///     let slice = mem_region.as_mut_slice();
    ///     for i in 0..num_pes * num_pes { slice[i] = my_pe + i + 1; }
    ///     mem_region.prod_scatter(0, num_pes).block();
    /// }
    ///```
    pub unsafe fn prod_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> CollectiveReduceScatterOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_scatter(ReduceOp::Prod, index, len)
    }
    /// Performs a collective reduce-scatter bitwise OR.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes * num_pes).block();
    /// unsafe {
    ///     let slice = mem_region.as_mut_slice();
    ///     for i in 0..num_pes * num_pes { slice[i] = 1 << (my_pe + i); }
    ///     mem_region.bit_or_scatter(0, num_pes).block();
    /// }
    ///```
    pub unsafe fn bit_or_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> CollectiveReduceScatterOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_scatter(ReduceOp::BitOr, index, len)
    }
    /// Performs a collective reduce-scatter bitwise XOR.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes * num_pes).block();
    /// unsafe {
    ///     let slice = mem_region.as_mut_slice();
    ///     for i in 0..num_pes * num_pes { slice[i] = 1 << (my_pe + i); }
    ///     mem_region.bit_xor_scatter(0, num_pes).block();
    /// }
    ///```
    pub unsafe fn bit_xor_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> CollectiveReduceScatterOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_scatter(ReduceOp::BitXor, index, len)
    }
    /// Performs a collective reduce-scatter bitwise AND.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes * num_pes).block();
    /// unsafe {
    ///     let slice = mem_region.as_mut_slice();
    ///     for i in 0..num_pes * num_pes { slice[i] = usize::MAX; }
    ///     mem_region.bit_and_scatter(0, num_pes).block();
    /// }
    ///```
    pub unsafe fn bit_and_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> CollectiveReduceScatterOpHandle<T> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_scatter(ReduceOp::BitAnd, index, len)
    }
    /// Performs a collective reduce-scatter minimum, storing results into `buffer`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes * num_pes).block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; num_pes]);
    /// unsafe {
    ///     let slice = mem_region.as_mut_slice();
    ///     for i in 0..num_pes * num_pes { slice[i] = my_pe + i + 1; }
    ///     mem_region.min_scatter_into_buffer(0, num_pes, buf).block();
    /// }
    ///```
    pub unsafe fn min_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::Min, index, len, buffer)
    }
    /// Performs a collective reduce-scatter maximum, storing results into `buffer`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes * num_pes).block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; num_pes]);
    /// unsafe {
    ///     let slice = mem_region.as_mut_slice();
    ///     for i in 0..num_pes * num_pes { slice[i] = my_pe + i + 1; }
    ///     mem_region.max_scatter_into_buffer(0, num_pes, buf).block();
    /// }
    ///```
    pub unsafe fn max_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::Max, index, len, buffer)
    }
    /// Performs a collective reduce-scatter sum, storing results into `buffer`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes * num_pes).block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; num_pes]);
    /// unsafe {
    ///     let slice = mem_region.as_mut_slice();
    ///     for i in 0..num_pes * num_pes { slice[i] = my_pe + i + 1; }
    ///     mem_region.sum_scatter_into_buffer(0, num_pes, buf).block();
    /// }
    ///```
    pub unsafe fn sum_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::Sum, index, len, buffer)
    }
    /// Performs a collective reduce-scatter product, storing results into `buffer`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes * num_pes).block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; num_pes]);
    /// unsafe {
    ///     let slice = mem_region.as_mut_slice();
    ///     for i in 0..num_pes * num_pes { slice[i] = my_pe + i + 1; }
    ///     mem_region.prod_scatter_into_buffer(0, num_pes, buf).block();
    /// }
    ///```
    pub unsafe fn prod_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::Prod, index, len, buffer)
    }
    /// Performs a collective reduce-scatter bitwise OR, storing results into `buffer`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes * num_pes).block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; num_pes]);
    /// unsafe {
    ///     let slice = mem_region.as_mut_slice();
    ///     for i in 0..num_pes * num_pes { slice[i] = 1 << (my_pe + i); }
    ///     mem_region.bit_or_scatter_into_buffer(0, num_pes, buf).block();
    /// }
    ///```
    pub unsafe fn bit_or_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::BitOr, index, len, buffer)
    }
    /// Performs a collective reduce-scatter bitwise XOR, storing results into `buffer`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes * num_pes).block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; num_pes]);
    /// unsafe {
    ///     let slice = mem_region.as_mut_slice();
    ///     for i in 0..num_pes * num_pes { slice[i] = 1 << (my_pe + i); }
    ///     mem_region.bit_xor_scatter_into_buffer(0, num_pes, buf).block();
    /// }
    ///```
    pub unsafe fn bit_xor_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::BitXor, index, len, buffer)
    }
    /// Performs a collective reduce-scatter bitwise AND, storing results into `buffer`.
    ///
    /// # Safety
    /// `index + len` must not exceed the local segment length. Concurrent mutation of the same elements is undefined behavior.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(num_pes * num_pes).block();
    /// let buf = LamellarBuffer::from_vec(&world, vec![0usize; num_pes]);
    /// unsafe {
    ///     let slice = mem_region.as_mut_slice();
    ///     for i in 0..num_pes * num_pes { slice[i] = usize::MAX; }
    ///     mem_region.bit_and_scatter_into_buffer(0, num_pes, buf).block();
    /// }
    ///```
    pub unsafe fn bit_and_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        // let slice = self.as_slice();
        self.mr
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::BitAnd, index, len, buffer)
    }

    /// Blocks until all outstanding RDMA operations issued by this PE on this memory region
    /// have completed.
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
    ///     let _ = mem_region.put(my_pe, my_pe, my_pe);
    /// }
    /// mem_region.wait_all();
    /// world.barrier();
    ///```
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

impl<T: Remote> SharedMemoryRegion<T> {
    #[doc(alias("One-sided", "onesided"))]
    /// Blocking "put" of `len` elements from the local memory at `src` into this region at `index` on `pe`.
    ///
    /// `src` is ordinary caller memory (a stack array, a `Vec`, a global); it need not come from Lamellar.
    /// The runtime does not register it with the network, so a backend that needs a registered source stages
    /// it through an internal buffer (or sends it inline when small). On return `src` may be reused, but the
    /// data is not guaranteed to be visible on `pe` yet: call [`wait_all`](Self::wait_all) for that.
    ///
    /// # Safety
    /// `src` must be valid for reads of `len` elements, and `index + len` must be within this region.
    /// Mutual exclusivity with other readers and writers is not enforced.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the transfer; `pe` is not notified.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let mem_region: SharedMemoryRegion<u32> = world.alloc_shared_mem_region(num_pes).block();
    /// let local = [my_pe as u32 + 1];
    /// unsafe {
    ///     mem_region.put_ptr_blocking(my_pe, my_pe, local.as_ptr(), 1);
    ///     mem_region.wait_all();
    ///     let mut back = [0u32];
    ///     mem_region.get_into_ptr_blocking(my_pe, my_pe, back.as_mut_ptr(), 1);
    ///     assert_eq!(back, local);
    /// }
    ///```
    pub unsafe fn put_ptr_blocking(&self, pe: usize, index: usize, src: *const T, len: usize) {
        if len == 0 {
            return;
        }
        debug_assert!(index + len <= self.sub_region_size);
        self.mr
            .as_base::<T>()
            .put_buffer(
                pe,
                self.sub_region_offset + index,
                MemregionRdmaInputInner::unregistered_raw(src, len),
            )
            .block();
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Unmanaged form of [`put_ptr_blocking`](Self::put_ptr_blocking): the transfer is issued and tracked by no
    /// handle. `src` must stay valid and unmodified until [`wait_all`](Self::wait_all) (or another fence on
    /// this PE) has completed.
    ///
    /// # Safety
    /// As [`put_ptr_blocking`](Self::put_ptr_blocking), plus the lifetime requirement on `src` above.
    pub unsafe fn put_ptr_unmanaged(&self, pe: usize, index: usize, src: *const T, len: usize) {
        if len == 0 {
            return;
        }
        debug_assert!(index + len <= self.sub_region_size);
        self.mr.as_base::<T>().put_buffer_unmanaged(
            pe,
            self.sub_region_offset + index,
            MemregionRdmaInputInner::unregistered_raw(src, len),
        );
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Blocking "get" of `len` elements from this region at `index` on `pe` into the local memory at `dst`.
    ///
    /// `dst` is ordinary caller memory; the runtime does not register it, so a backend that needs a registered
    /// destination stages the transfer. When this returns the data is in `dst`.
    ///
    /// # Safety
    /// `dst` must be valid for writes of `len` elements and not accessed by anything else during the call, and
    /// `index + len` must be within this region.
    ///
    /// # One-sided Operation
    /// The calling PE initiates the transfer; `pe` is not notified. See [`put_ptr_blocking`](Self::put_ptr_blocking)
    /// for an example.
    pub unsafe fn get_into_ptr_blocking(&self, pe: usize, index: usize, dst: *mut T, len: usize) {
        if len == 0 {
            return;
        }
        debug_assert!(index + len <= self.sub_region_size);
        let buf = LamellarBuffer::from_unregistered_raw(dst, len, self.mr.rdma.clone());
        self.mr
            .as_base::<T>()
            .blocking_get_into_buffer(pe, self.sub_region_offset + index, buf);
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Unmanaged form of [`get_into_ptr_blocking`](Self::get_into_ptr_blocking): `dst` must stay valid, and must not be
    /// read or written, until [`wait_all`](Self::wait_all) (or another fence on this PE) has completed.
    ///
    /// # Safety
    /// As [`get_into_ptr_blocking`](Self::get_into_ptr_blocking), plus the lifetime requirement on `dst` above.
    pub unsafe fn get_into_ptr_unmanaged(&self, pe: usize, index: usize, dst: *mut T, len: usize) {
        if len == 0 {
            return;
        }
        debug_assert!(index + len <= self.sub_region_size);
        let buf = LamellarBuffer::from_unregistered_raw(dst, len, self.mr.rdma.clone());
        self.mr
            .as_base::<T>()
            .get_into_buffer_unmanaged(pe, self.sub_region_offset + index, buf);
    }
}

impl<T: Remote> SharedMemoryRegion<T> {
    /// Reinterprets this region (or sub-region) as a region of `U`, returning
    /// [`MemRegionError::MemNotAlignedError`] if the view would not be valid for `U`.
    ///
    /// The result covers exactly the same bytes; indices into it count `U`s, so sub-regions and
    /// atomics on the new region address `U`-sized slots. The view is valid when, for this region
    /// or sub-region:
    /// * `size_of::<U>()` is non-zero and divides both its byte offset and its byte length, and
    /// * the start of the allocation on this PE is a multiple of `align_of::<U>()`.
    ///
    /// Symmetric allocations are placed at the same offset on every PE, so the allocation-alignment
    /// check done on the calling PE stands for all of them. The check is skipped on a PE that holds
    /// no data for the region (a sub-team allocation seen from outside the team).
    ///
    /// The alignment an allocation gets is the one requested for its element type, so a region
    /// allocated as `u8` is not guaranteed to be suitable for wider types. Allocating as the widest
    /// type needed (and casting down to `u8` where bytes are wanted) always is.
    ///
    /// # Safety
    /// This call is unsafe because the bytes of the region are reinterpreted: every bit pattern
    /// that is read through the new view must be a valid `U`, and the caller must not create
    /// overlapping typed accesses that race.
    ///
    /// # Examples
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// // allocated as u64 so that it is aligned for u64
    /// let words: SharedMemoryRegion<u64> = world.alloc_shared_mem_region(4).block();
    /// unsafe {
    ///     let bytes: SharedMemoryRegion<u8> = words.try_cast().unwrap();
    ///     assert_eq!(bytes.len(), 32);
    ///     // a byte offset that is not a multiple of 8 cannot be viewed as u64
    ///     assert!(bytes.sub_region(1..9).try_cast::<u64>().is_err());
    ///     let back: SharedMemoryRegion<u64> = bytes.sub_region(8..24).try_cast().unwrap();
    ///     assert_eq!(back.len(), 2);
    ///     back.atomic_store(my_pe, 0, 7).block();
    ///     assert_eq!(words.atomic_load(my_pe, 1).block(), 7);
    /// }
    ///```
    pub unsafe fn try_cast<U: Remote>(&self) -> Result<SharedMemoryRegion<U>, MemRegionError> {
        let (t_size, u_size) = (std::mem::size_of::<T>(), std::mem::size_of::<U>());
        if u_size == 0 {
            return Err(MemRegionError::MemNotAlignedError);
        }
        let byte_offset = self.sub_region_offset * t_size;
        let byte_len = self.sub_region_size * t_size;
        if byte_offset % u_size != 0 || byte_len % u_size != 0 {
            return Err(MemRegionError::MemNotAlignedError);
        }
        if self.mr.mode != Mode::Remote {
            let base: usize = self.mr.alloc.inner_alloc.addr().into();
            if base % std::mem::align_of::<U>() != 0 {
                return Err(MemRegionError::MemNotAlignedError);
            }
        }
        Ok(SharedMemoryRegion {
            mr: self.mr.clone(),
            sub_region_offset: byte_offset / u_size,
            sub_region_size: byte_len / u_size,
            phantom: PhantomData,
        })
    }

    /// Like [`try_cast`](Self::try_cast), but panics if the view would not be valid for `U`.
    ///
    /// # Safety
    /// See [`try_cast`](Self::try_cast).
    pub unsafe fn cast<U: Remote>(&self) -> SharedMemoryRegion<U> {
        self.try_cast::<U>()
            .expect("memory region is not suitably sized or aligned for the requested type")
    }
}

impl<T: Remote + PartialEq> SharedMemoryRegion<T> {
    #[doc(alias("One-sided", "onesided"))]
    /// Atomically compares the element at `index` on the given `pe` with `current` and, if they
    /// are equal, replaces it with `new`.
    ///
    /// Returns an [`AtomicCompareExchangeOpHandle`] whose `block()` / `.await` resolves to
    /// `Ok(previous)` if the exchange happened and `Err(actual)` if it did not.
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
    ///     mem_region.atomic_store(my_pe, my_pe, 5).block();
    ///     assert_eq!(mem_region.atomic_compare_exchange(my_pe, my_pe, 5, 6).block(), Ok(5));
    ///     assert_eq!(mem_region.atomic_compare_exchange(my_pe, my_pe, 5, 7).block(), Err(6));
    /// }
    ///```
    pub unsafe fn atomic_compare_exchange(
        &self,
        pe: usize,
        index: usize,
        current: T,
        new: T,
    ) -> AtomicCompareExchangeOpHandle<T> {
        self.mr
            .as_base::<T>()
            .atomic_compare_exchange(pe, self.sub_region_offset + index, current, new)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Blocking form of [`atomic_compare_exchange`](Self::atomic_compare_exchange).
    ///
    /// # Safety
    /// This call is unsafe because the caller must ensure `index` is within bounds.
    pub unsafe fn atomic_compare_exchange_blocking(
        &self,
        pe: usize,
        index: usize,
        current: T,
        new: T,
    ) -> Result<T, T> {
        self.mr
            .as_base::<T>()
            .atomic_compare_exchange_blocking(pe, self.sub_region_offset + index, current, new)
    }
}

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

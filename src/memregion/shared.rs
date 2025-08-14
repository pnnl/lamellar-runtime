use crate::active_messaging::RemotePtr;
use crate::array::{LamellarRead, LamellarWrite, TeamTryFrom};
use crate::darc::Darc;
use crate::lamellae::AllocationType;
use crate::{memregion::*, LamellarEnv, LamellarTeam};

// use crate::active_messaging::AmDist;
use core::marker::PhantomData;
// use serde::ser::Serialize;
use std::pin::Pin;
use std::sync::Arc;

use std::ops::Bound;

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
pub struct SharedMemoryRegion<T: Dist> {
    pub(crate) mr: Darc<MemoryRegion<u8>>,
    sub_region_offset: usize,
    sub_region_size: usize,
    phantom: PhantomData<T>,
}

impl<T: Dist> LamellarEnv for SharedMemoryRegion<T> {
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

impl<T: Dist> crate::active_messaging::DarcSerde for SharedMemoryRegion<T> {
    fn ser(&self, num_pes: usize, darcs: &mut Vec<RemotePtr>) {
        // println!("in shared ser");
        self.mr.serialize_update_cnts(num_pes);
        darcs.push(RemotePtr::NetworkDarc(self.mr.clone().into()));
    }
    // fn des(&self, cur_pe: Result<usize, crate::IdError>) {
    //     // println!("in shared des");
    //     // match cur_pe {
    //     //     Ok(_) => {
    //     //         self.mr.deserialize_update_cnts();
    //     //     }
    //     //     Err(err) => {
    //     //         panic!("can only access darcs within team members ({:?})", err);
    //     //     }
    //     // }
    //     // self.mr.print();
    // }
}

impl<T: Dist> SharedMemoryRegion<T> {
    // pub(crate) fn new(
    //     size: usize,
    //     team: Pin<Arc<LamellarTeamRT>>,
    //     alloc: AllocationType,
    // ) -> SharedMemoryRegionHandle<T> {
    //     SharedMemoryRegion::try_new(size, team, alloc).expect("Out of memory")
    // }

    pub(crate) fn new(
        size: usize,
        team: Pin<Arc<LamellarTeamRT>>,
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
                        .comm()
                        .alloc_pool(size * std::mem::size_of::<T>());
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
        team: Pin<Arc<LamellarTeamRT>>,
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
    fn sub_region<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self {
        SubRegion::sub_region(self, range)
    }

    /// Return the length of the memory region
    pub fn len(&self) -> usize {
        self.sub_region_size
    }
}

// This could be useful for if we want to transfer the actual data instead of the pointer
// impl<T: Dist + serde::Serialize> SharedMemoryRegion<T> {
//     pub(crate) fn serialize_local_data<S>(&self, s: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer,
//     {
//         unsafe { self.as_slice().unwrap().serialize(s) }
//     }
// }

//account for subregion stuff
impl<T: Dist> RegisteredMemoryRegion<T> for SharedMemoryRegion<T> {
    fn len(&self) -> usize {
        self.sub_region_size
    }
    fn addr(&self) -> MemResult<CommAllocAddr> {
        let addr = self.mr.addr()?;
        Ok(addr + self.sub_region_offset * std::mem::size_of::<T>())
    }
    unsafe fn at(&self, index: usize) -> MemResult<&T> {
        self.mr.casted_at::<T>(index)
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
    unsafe fn as_comm_slice(&self) -> MemResult<CommSlice<T>> {
        let slice = self.mr.as_casted_comm_slice()?;
        Ok(
            slice
                .sub_slice(self.sub_region_offset..(self.sub_region_offset + self.sub_region_size)),
        )
    }
    // unsafe fn as_casted_comm_slice<R:Dist>(&self) -> MemResult<CommSlice<R>> {
    //     let mut slice = self.mr.as_casted_comm_slice()?;
    //     Ok(slice.sub_slice(self.sub_region_offset..(self.sub_region_offset + self.sub_region_size)))
    // }
    unsafe fn comm_addr(&self) -> MemResult<CommAllocAddr> {
        let addr = self.mr.comm_addr()?;
        Ok(addr + self.sub_region_offset * std::mem::size_of::<T>())
    }
}

impl<T: Dist> MemRegionId for SharedMemoryRegion<T> {
    fn id(&self) -> usize {
        self.mr.id()
    }
}

impl<T: Dist> SubRegion<T> for SharedMemoryRegion<T> {
    type Region = SharedMemoryRegion<T>;
    fn sub_region<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Region {
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

impl<T: Dist> AsBase for SharedMemoryRegion<T> {
    unsafe fn to_base<B: Dist>(self) -> LamellarMemoryRegion<B> {
        let u8_offset = self.sub_region_offset * std::mem::size_of::<T>();
        let u8_size = self.sub_region_size * std::mem::size_of::<T>();
        // println!("to_base");
        SharedMemoryRegion {
            mr: self.mr.clone(),
            sub_region_offset: u8_offset / std::mem::size_of::<B>(),
            sub_region_size: u8_size / std::mem::size_of::<B>(),
            phantom: PhantomData,
        }
        .into()
    }
}

impl<T: Dist> MemoryRegionRDMA<T> for SharedMemoryRegion<T> {
    unsafe fn put<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) -> RdmaHandle<T> {
        self.mr.put(pe, self.sub_region_offset + index, data)
    }
    // unsafe fn blocking_put<U: Into<LamellarMemoryRegion<T>>>(
    //     &self,
    //     pe: usize,
    //     index: usize,
    //     data: U,
    // ) {
    //     self.mr
    //         .blocking_put(pe, self.sub_region_offset + index, data);
    // }
    unsafe fn put_all<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        index: usize,
        data: U,
    ) -> RdmaHandle<T> {
        self.mr.put_all(self.sub_region_offset + index, data)
    }
    unsafe fn get_unchecked<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) -> RdmaHandle<T> {
        self.mr
            .get_unchecked(pe, self.sub_region_offset + index, data)
    }
    // unsafe fn blocking_get<U: Into<LamellarMemoryRegion<T>>>(
    //     &self,
    //     pe: usize,
    //     index: usize,
    //     data: U,
    // ) {
    //     self.mr
    //         .blocking_get(pe, self.sub_region_offset + index, data);
    // }
}

impl<T: Dist> RTMemoryRegionRDMA<T> for SharedMemoryRegion<T> {
    unsafe fn put_comm_slice(&self, pe: usize, index: usize, data: CommSlice<T>) -> RdmaHandle<T> {
        self.mr
            .put_comm_slice(pe, self.sub_region_offset + index, data)
    }
    unsafe fn get_comm_slice(&self, pe: usize, index: usize, data: CommSlice<T>) -> RdmaHandle<T> {
        // println!("iget_slice {:?} {:?}",pe,self.sub_region_offset + index);
        self.mr
            .get_comm_slice(pe, self.sub_region_offset + index, data)
    }
}

impl<T: Dist> std::fmt::Debug for SharedMemoryRegion<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{:?}] shared mem region:  {:?} ", self.mr.pe, self.mr,)
    }
}

impl<T: Dist> LamellarWrite for SharedMemoryRegion<T> {}
impl<T: Dist> LamellarRead for SharedMemoryRegion<T> {}

impl<T: Dist> From<&SharedMemoryRegion<T>> for LamellarMemoryRegion<T> {
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

// impl<T: Dist> Drop for SharedMemoryRegion<T> {
//     fn drop(&mut self) {
//         println!("dropping shared memory region");
//     }
// }

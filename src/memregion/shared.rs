use crate::array::{LamellarRead, LamellarWrite};
use crate::active_messaging::RemotePtr;
use crate::darc::Darc;
use crate::lamellae::AllocationType;
use crate::memregion::*;

// use crate::active_messaging::AmDist;
use core::marker::PhantomData;
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
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
/// let world_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1000);
/// ```
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct SharedMemoryRegion<T: Dist> {
    pub(crate) mr: Darc<MemoryRegion<u8>>,
    sub_region_offset: usize,
    sub_region_size: usize,
    phantom: PhantomData<T>,
}

impl<T: Dist> crate::active_messaging::DarcSerde for SharedMemoryRegion<T> {
    fn ser(&self, num_pes: usize, darcs: &mut Vec<RemotePtr>) {
        // println!("in shared ser");
        self.mr.serialize_update_cnts(num_pes);
        darcs.push(RemotePtr::NetworkDarc(self.mr.clone().into()));
    }
    fn des(&self, cur_pe: Result<usize, crate::IdError>) {
        // println!("in shared des");
        match cur_pe {
            Ok(_) => {
                self.mr.deserialize_update_cnts();
            }
            Err(err) => {
                panic!("can only access darcs within team members ({:?})", err);
            }
        }
        // self.mr.print();
    }
}

impl<T: Dist> SharedMemoryRegion<T> {
    pub(crate) fn new(
        size: usize,
        team: Pin<Arc<LamellarTeamRT>>,
        alloc: AllocationType,
    ) -> SharedMemoryRegion<T> {
        SharedMemoryRegion::try_new(size, team, alloc).expect("Out of memory")
    }

    pub(crate) fn try_new(
        size: usize,
        team: Pin<Arc<LamellarTeamRT>>,
        alloc: AllocationType,
    ) -> Result<SharedMemoryRegion<T>, anyhow::Error> {
        // println!("creating new shared mem region {:?} {:?}",size,alloc);
        Ok(SharedMemoryRegion {
            mr: Darc::try_new(
                team.clone(),
                MemoryRegion::try_new(
                    size * std::mem::size_of::<T>(),
                    team.lamellae.clone(),
                    alloc,
                )?,
                crate::darc::DarcMode::Darc,
            )
            .expect("memregions can only be created on a member of the team"),
            sub_region_offset: 0,
            sub_region_size: size,
            phantom: PhantomData,
        })
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
    fn addr(&self) -> MemResult<usize> {
        if let Ok(addr) = self.mr.addr() {
            Ok(addr + self.sub_region_offset * std::mem::size_of::<T>())
        } else {
            Err(MemNotLocalError {})
        }
    }
    unsafe fn at(&self, index: usize) -> MemResult<&T> {
        self.mr.casted_at::<T>(index)
    }
    unsafe fn as_slice(&self) -> MemResult<&[T]> {
        if let Ok(slice) = self.mr.as_casted_slice::<T>() {
            Ok(&slice[self.sub_region_offset..(self.sub_region_offset + self.sub_region_size)])
        } else {
            Err(MemNotLocalError {})
        }
    }
    unsafe fn as_mut_slice(&self) -> MemResult<&mut [T]> {
        if let Ok(slice) = self.mr.as_casted_mut_slice::<T>() {
            Ok(&mut slice[self.sub_region_offset..(self.sub_region_offset + self.sub_region_size)])
        } else {
            Err(MemNotLocalError {})
        }
    }
    unsafe fn as_ptr(&self) -> MemResult<*const T> {
        if let Ok(addr) = self.addr() {
            Ok(addr as *const T)
        } else {
            Err(MemNotLocalError {})
        }
    }
    unsafe fn as_mut_ptr(&self) -> MemResult<*mut T> {
        if let Ok(addr) = self.addr() {
            Ok(addr as *mut T)
        } else {
            Err(MemNotLocalError {})
        }
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

//#[prof]
impl<T: Dist> MemoryRegionRDMA<T> for SharedMemoryRegion<T> {
    unsafe fn put<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, index: usize, data: U) {
        self.mr.put(pe, self.sub_region_offset + index, data);
    }
    unsafe fn blocking_put<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        self.mr
            .blocking_put(pe, self.sub_region_offset + index, data);
    }
    unsafe fn put_all<U: Into<LamellarMemoryRegion<T>>>(&self, index: usize, data: U) {
        self.mr.put_all(self.sub_region_offset + index, data);
    }
    unsafe fn get_unchecked<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        self.mr
            .get_unchecked(pe, self.sub_region_offset + index, data);
    }
    unsafe fn blocking_get<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        self.mr
            .blocking_get(pe, self.sub_region_offset + index, data);
    }
}

impl<T: Dist> RTMemoryRegionRDMA<T> for SharedMemoryRegion<T> {
    unsafe fn put_slice(&self, pe: usize, index: usize, data: &[T]) {
        self.mr.put_slice(pe, self.sub_region_offset + index, data)
    }
    unsafe fn blocking_get_slice(&self, pe: usize, index: usize, data: &mut [T]) {
        // println!("iget_slice {:?} {:?}",pe,self.sub_region_offset + index);
        self.mr
            .blocking_get_slice(pe, self.sub_region_offset + index, data)
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
    fn team_from(smr: &SharedMemoryRegion<T>, _team: &std::pin::Pin<Arc<LamellarTeamRT>>) -> Self {
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
    fn team_from(smr: &SharedMemoryRegion<T>, _team: &std::pin::Pin<Arc<LamellarTeamRT>>) -> Self {
        LamellarArrayRdmaInput::SharedMemRegion(smr.clone())
    }
}

//#[prof]
// impl<T: Dist> Drop for SharedMemoryRegion<T> {
//     fn drop(&mut self) {
//         println!("dropping shared memory region");
//     }
// }

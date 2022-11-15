use crate::array::{LamellarRead, LamellarWrite};
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
/// use lamellar::{SharedMemoryRegion, RemoteMemoryRegion};
///
/// let world_mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1000);
/// let team_mem_region: SharedMemoryRegion<usize> = some_team.alloc_shared_mem_region(1000);
/// ```
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct SharedMemoryRegion<T: Dist> {
    pub(crate) mr: Darc<MemoryRegion<u8>>,
    sub_region_offset: usize,
    sub_region_size: usize,
    phantom: PhantomData<T>,
}

impl<T: Dist> crate::active_messaging::DarcSerde for SharedMemoryRegion<T> {
    fn ser(&self, num_pes: usize) {
        // println!("in shared ser");
        self.mr.serialize_update_cnts(num_pes);
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
    /// The length (in number of elements of `T`) of the local segment of the memory region (i.e. not the global length of the memory region)  
    ///
    /// # Example
    ///```
    /// use lamellar::SharedMemoryRegion;
    ///
    /// let mem_region: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(1000);
    /// assert_eq!(mem_region.len(),1000)
    pub fn len(&self) -> usize {
        RegisteredMemoryRegion::<T>::len(self)
    }

    /// "Puts" (copies) `data` from a local memory location into a this shared memory region at the provided offset on the specified PE
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - A LamellarMemoryRegion (either shared or onesided) that has data local to this PE,
    /// the data buffer may not be safe to upon return from this call, currently the user is responsible for completion detection,
    /// or you may use the similar iput call (with a potential performance penalty);
    ///
    /// # Safety
    /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    /// Additionally, when this call returns the underlying fabric provider may or may not have already copied the data buffer
    ///
    /// # Panics
    /// Panics if "data" does not have any local data on this PE
    /// Panics if index is out of bounds
    /// Panics if PE is out of bounds
    pub unsafe fn put<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, offset: usize, data: U) {
        MemoryRegionRDMA::<T>::put(self, pe, offset, data);
    }

    /// Blocking "Puts" (copies) data from a local memory location into a remote memory location on the specified PE.
    ///
    /// This function blocks until the data in the data buffer has been transfered out of this PE, this does not imply that it has arrived at the remote destination though
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - A LamellarMemoryRegion (either shared or onesided) that has data local to this PE,
    /// the data buffer is free to be reused upon return of this function.
    ///
    /// # Safety
    /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    ///
    /// # Panics
    /// Panics if "data" does not have any local data on this PE
    /// Panics if index is out of bounds
    /// Panics if PE is out of bounds
    pub unsafe fn blocking_put<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        MemoryRegionRDMA::<T>::blocking_put(self, pe, index, data);
    }

    /// "Puts" (copies) data from a local memory location into a remote memory location on all PEs containing the memory region
    ///
    /// This is similar to broad cast
    ///
    /// # Arguments
    ///
    /// * `index` - offset into the remote memory window
    /// * `data` - A LamellarMemoryRegion (either shared or onesided) that has data local to this PE,
    /// the data buffer may not be safe to upon return from this call, currently the user is responsible for completion detection,
    /// or you may use the similar iput call (with a potential performance penalty);
    ///
    /// # Safety
    /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    /// Additionally, when this call returns the underlying fabric provider may or may not have already copied the data buffer
    ///
    /// # Panics
    /// Panics if "data" does not have any local data on this PE
    /// Panics if index is out of bounds
    pub unsafe fn put_all<U: Into<LamellarMemoryRegion<T>>>(&self, index: usize, data: U) {
        MemoryRegionRDMA::<T>::put_all(self, index, data);
    }

    /// "Gets" (copies) data from remote memory location on the specified PE into the provided data buffer.
    /// After calling this function, the data may or may not have actually arrived into the data buffer.
    /// The user is responsible for transmission termination detection
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - A LamellarMemoryRegion (either shared or onesided) that has data local to this PE
    ///
    /// # Safety
    /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    /// Additionally, when this call returns the underlying fabric provider may or may not have already copied data into the data buffer.
    ///
    /// # Panics
    /// Panics if "data" does not have any local data on this PE
    /// Panics if index is out of bounds
    /// Panics if PE is out of bounds
    pub unsafe fn get_unchecked<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        MemoryRegionRDMA::<T>::get_unchecked(self, pe, index, data);
    }

    /// Blocking "Gets" (copies) data from remote memory location on the specified PE into the provided data buffer.
    /// After calling this function, the data is guaranteed to be placed in the data buffer
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - A LamellarMemoryRegion (either shared or onesided) that has data local to this PE
    ///
    /// # Safety
    /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    ///
    /// # Panics
    /// Panics if "data" does not have any local data on this PE
    /// Panics if index is out of bounds
    /// Panics if PE is out of bounds
    pub unsafe fn blocking_get<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        MemoryRegionRDMA::<T>::blocking_get(self, pe, index, data);
    }

    /// Create a sub region of this OneSidedMemoryRegion using the provided range
    ///
    /// # Panics
    /// panics if the end range is larger than the length of the memory region
    pub fn sub_region<R: std::ops::RangeBounds<usize>>(&self, range: R) -> LamellarMemoryRegion<T> {
        SubRegion::<T>::sub_region(self, range)
    }

    /// Return a slice of the local (to the calling PE) data of the memory region
    ///
    /// Returns an error if the PE does not contain any local data associated with this memory region
    ///
    /// # Safety
    /// this call is always unsafe as there is no gaurantee that there do not exist mutable references elsewhere in the distributed system.
    pub unsafe fn as_slice(&self) -> MemResult<&[T]> {
        RegisteredMemoryRegion::<T>::as_slice(self)
    }

    /// Return a mutable slice of the local (to the calling PE) data of the memory region
    ///
    /// Returns an error if the PE does not contain any local data associated with this memory region
    ///
    /// # Safety
    /// this call is always unsafe as there is no gaurantee that there do not exist other mutable references elsewhere in the distributed system.
    pub unsafe fn as_mut_slice(&self) -> MemResult<&mut [T]> {
        RegisteredMemoryRegion::<T>::as_mut_slice(self)
    }

    /// Return a ptr to the local (to the calling PE) data of the memory region
    ///
    /// Returns an error if the PE does not contain any local data associated with this memory region
    ///
    /// # Safety
    /// this call is always unsafe as there is no gaurantee that there do not exist mutable references elsewhere in the distributed system.
    pub unsafe fn as_ptr(&self) -> MemResult<*const T> {
        RegisteredMemoryRegion::<T>::as_ptr(self)
    }

    /// Return a mutable ptr to the local (to the calling PE) data of the memory region
    ///
    /// Returns an error if the PE does not contain any local data associated with this memory region
    ///
    /// # Safety
    /// this call is always unsafe as there is no gaurantee that there do not exist mutable references elsewhere in the distributed system.
    pub unsafe fn as_mut_ptr(&self) -> MemResult<*mut T> {
        RegisteredMemoryRegion::<T>::as_mut_ptr(self)
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
    fn sub_region<R: std::ops::RangeBounds<usize>>(&self, range: R) -> LamellarMemoryRegion<T> {
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
        .into()
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

impl<T: Dist> From<&SharedMemoryRegion<T>> for LamellarArrayInput<T> {
    fn from(smr: &SharedMemoryRegion<T>) -> Self {
        // println!("from");
        LamellarArrayInput::SharedMemRegion(smr.clone())
    }
}

impl<T: Dist> MyFrom<&SharedMemoryRegion<T>> for LamellarArrayInput<T> {
    fn my_from(smr: &SharedMemoryRegion<T>, _team: &std::pin::Pin<Arc<LamellarTeamRT>>) -> Self {
        LamellarArrayInput::SharedMemRegion(smr.clone())
    }
}

//#[prof]
// impl<T: Dist> Drop for SharedMemoryRegion<T> {
//     fn drop(&mut self) {
//         println!("dropping shared memory region");
//     }
// }

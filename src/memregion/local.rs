use crate::lamellae::local_lamellae::Local;
use crate::lamellae::{AllocationType, Backend, Lamellae, LamellaeComm, LamellaeRDMA};
use crate::memregion::*;
// use crate::lamellar_array::{LamellarLocalArray};
use core::marker::PhantomData;
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use std::ops::{Bound, RangeBounds};

// #[derive(serde::Serialize, serde::Deserialize, Clone)]
#[derive(Clone)]
pub struct LocalMemoryRegion<T: Dist + 'static> {
    mr: Arc<MemoryRegion<u8>>,
    pe: usize,
    sub_region_offset: usize,
    sub_region_size: usize,
    phantom: PhantomData<T>
}

impl<T: Dist + 'static> LocalMemoryRegion<T> {
    pub(crate) fn new(size: usize, lamellae: Arc<Lamellae>) -> LocalMemoryRegion<T> {
        let mr = Arc::new(MemoryRegion::new(size*std::mem::size_of::<T>(), lamellae, AllocationType::Local));
        let pe = mr.pe;
        LocalMemoryRegion { 
            mr: mr, 
            pe: pe, 
            sub_region_offset: 0,
            sub_region_size: size, 
            phantom: PhantomData, 
        }
    }
    pub fn len(&self) -> usize {
        RegisteredMemoryRegion::<T>::len(self)
    }
    pub unsafe fn put<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, index: usize, data: U) {
        MemoryRegionRDMA::<T>::put(self, pe, index, data);
    }
    pub fn iput<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, index: usize, data: U) {
        MemoryRegionRDMA::<T>::iput(self, pe, index, data);
    }
    pub unsafe fn put_all<U: Into<LamellarMemoryRegion<T>>>(&self, index: usize, data: U) {
        MemoryRegionRDMA::<T>::put_all(self, index, data);
    }
    pub unsafe fn get<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, index: usize, data: U) {
        MemoryRegionRDMA::<T>::get(self, pe, index, data);
    }
    pub fn sub_region<R: std::ops::RangeBounds<usize>>(&self, range: R) -> LamellarMemoryRegion<T> {
        SubRegion::<T>::sub_region(self, range)
    }
    pub fn as_slice(&self) -> MemResult<&[T]> {
        RegisteredMemoryRegion::<T>::as_slice(self)
    }
    pub unsafe fn as_mut_slice(&self) -> MemResult<&mut [T]> {
        RegisteredMemoryRegion::<T>::as_mut_slice(self)
    }
    pub fn as_ptr(&self) -> MemResult<*const T> {
        RegisteredMemoryRegion::<T>::as_ptr(self)
    }
    pub fn as_mut_ptr(&self) -> MemResult<*mut T> {
        RegisteredMemoryRegion::<T>::as_mut_ptr(self)
    }
}

//account for subregion stuff
impl<T: Dist + 'static> RegisteredMemoryRegion<T> for LocalMemoryRegion<T> {
    fn len(&self) -> usize {
        self.sub_region_size
    }
    fn addr(&self) -> MemResult<usize> {
        if self.pe == self.mr.pe {
            if let Ok(addr) = self.mr.addr(){
                Ok(addr + self.sub_region_offset * std::mem::size_of::<T>())
            }
            else{
                Err(MemNotLocalError {})
            }
        } else {
            Err(MemNotLocalError {})
        }
    }

    fn as_slice(&self) -> MemResult<&[T]> {
        if self.pe == self.mr.pe {
            if let Ok(slice) = self.mr.as_casted_slice::<T>(){
                Ok(&slice[self.sub_region_offset..(self.sub_region_offset+self.sub_region_size)])
            }
            else{
                Err(MemNotLocalError {})
            }
        } else {
            Err(MemNotLocalError {})
        }
    }
    unsafe fn as_mut_slice(&self) -> MemResult<&mut [T]> {
        if self.pe == self.mr.pe {
            if let Ok(slice) = self.mr.as_casted_mut_slice::<T>(){
                Ok(&mut slice[self.sub_region_offset..(self.sub_region_offset+self.sub_region_size)])
            }
            else{
                Err(MemNotLocalError {})
            }
        } else {
            Err(MemNotLocalError {})
        }
    }
    fn as_ptr(&self) -> MemResult<*const T> {
        if self.pe == self.mr.pe {
            if let Ok(addr) = self.addr(){
                Ok(addr as *const T)
            } 
            else{
                Err(MemNotLocalError{})
            }
        } else {
            Err(MemNotLocalError {})
        }
    }
    fn as_mut_ptr(&self) -> MemResult<*mut T> {
        if self.pe == self.mr.pe {
            if let Ok(addr) = self.addr(){
                Ok(addr as *mut T)
            } 
            else{
                Err(MemNotLocalError{})
            }
        } else {
            Err(MemNotLocalError {})
        }
    }
}

impl<T: Dist + 'static> MemRegionId for LocalMemoryRegion<T> {
    fn id(&self) -> usize {
        self.mr.id()
    }
}

//fixme
impl<T: Dist + 'static> SubRegion<T> for LocalMemoryRegion<T> {
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

        // println!("local subregion: {:?} {:?} {:?}",start,end,(end-start));
        LocalMemoryRegion {
            mr: self.mr.clone(),
            pe: self.pe,
            sub_region_offset: self.sub_region_offset + start,
            sub_region_size: (end - start),
            phantom: PhantomData,
        }
        .into()

    }
}

//fixme
impl<T: Dist + 'static> AsBase for LocalMemoryRegion<T> {
    unsafe fn as_base<B: Dist + 'static>(self) -> LamellarMemoryRegion<B> {

        let u8_offset = self.sub_region_offset * std::mem::size_of::<T>();
        let u8_size = self.sub_region_size * std::mem::size_of::<T>();
        LocalMemoryRegion {
            mr: self.mr.clone(),
            pe: self.pe,
            sub_region_offset: u8_offset/std::mem::size_of::<B>(), 
            sub_region_size: u8_size/std::mem::size_of::<B>(), 
            phantom: PhantomData,
        }
        .into()
    }
}

impl<T: Dist + 'static> MemoryRegionRDMA<T> for LocalMemoryRegion<T> {
    unsafe fn put<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, index: usize, data: U) {
        if self.pe == pe {
            self.mr.put(pe, self.sub_region_offset + index, data);
            // self.mr.put(pe, index, data);
        } else {
            panic!(
                "trying to put to PE {:?} which does not contain data (pe with data =  {:?})",
                pe, self.pe
            );
            // Err(MemNotLocalError {})
        }
    }
    fn iput<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, index: usize, data: U) {
        if self.pe == pe {
            self.mr.iput(pe, self.sub_region_offset + index, data);
        // self.mr.iput(pe, index, data);
        } else {
            panic!(
                "trying to put to PE {:?} which does not contain data (pe with data =  {:?})",
                pe, self.pe
            );
            // Err(MemNotLocalError {})
        }
    }
    unsafe fn put_all<U: Into<LamellarMemoryRegion<T>>>(&self, index: usize, data: U) {
        self.mr.put_all(self.sub_region_offset + index, data);
        // self.mr.put_all(index, data);
    }
    unsafe fn get<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, index: usize, data: U) {
        if self.pe == pe {
            self.mr.get(pe, self.sub_region_offset + index, data);
        // self.mr.get(pe, index, data);
        } else {
            panic!(
                "trying to get from PE {:?} which does not contain data (pe with data =  {:?})",
                pe, self.pe
            );
            // Err(MemNotLocalError {})
        }
    }
}

impl<T: Dist + 'static> RTMemoryRegionRDMA<T> for LocalMemoryRegion<T> {
    unsafe fn put_slice(&self, pe: usize, index: usize, data: &[T]) {
        if self.pe == pe {
            self.mr.put_slice(pe, self.sub_region_offset + index, data)
        // self.mr.put_slice(pe, index, data)
        } else {
            panic!(
                "trying to put to PE {:?} which does not contain data (pe with data =  {:?})",
                pe, self.pe
            );
            // Err(MemNotLocalError {})
        }
    }
}

impl<T: Dist + 'static> std::fmt::Debug for LocalMemoryRegion<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // let slice = unsafe { std::slice::from_raw_parts(self.addr as *const T, self.size) };
        // write!(f, "{:?}", slice)
        write!(f, "[{:?}] local mem region:  {:?} ", self.pe, self.mr,)
    }
}

impl<T: Dist + 'static> From<&LocalMemoryRegion<T>> for LamellarArrayInput<T> {
    fn from(smr: &LocalMemoryRegion<T>) -> Self {
        LamellarArrayInput::LocalMemRegion(smr.clone())
    }
}

impl<T: Dist + 'static> MyFrom<&LocalMemoryRegion<T>> for LamellarArrayInput<T> {
    fn my_from(smr: &LocalMemoryRegion<T>, _team: &Arc<LamellarTeam>) -> Self {
        LamellarArrayInput::LocalMemRegion(smr.clone())
    }
}

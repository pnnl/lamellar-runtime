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

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct LocalMemoryRegion<T: Dist + 'static> {
    mr: MemoryRegion<T>,
    pe: usize,
}

impl<T: Dist + 'static> LocalMemoryRegion<T> {
    pub(crate) fn new(size: usize, lamellae: Arc<Lamellae>) -> LocalMemoryRegion<T> {
        let mr = MemoryRegion::new(size, lamellae, AllocationType::Local);
        let pe = mr.pe;
        LocalMemoryRegion { mr: mr, pe: pe }
    }
    pub fn len(&self) -> usize {
        RegisteredMemoryRegion::<T>::len(self)
    }
    unsafe fn put<U: Into<LamellarMemoryRegion<T>>>(&self, index: usize, data: U) {
        self.mr.put(self.pe, index, data);
    }
    fn iput<U: Into<LamellarMemoryRegion<T>>>(&self, index: usize, data: U) {
        self.mr.iput(self.pe, index, data);
    }
    unsafe fn put_all<U: Into<LamellarMemoryRegion<T>>>(&self, index: usize, data: U) {
        self.mr.put_all(index, data);
    }
    unsafe fn get<U: Into<LamellarMemoryRegion<T>>>(&self, index: usize, data: U) {
        self.mr.get(self.pe, index, data);
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

impl<T: Dist + 'static> RegisteredMemoryRegion<T> for LocalMemoryRegion<T> {
    fn len(&self) -> usize {
        self.mr.len()
    }
    fn addr(&self) -> MemResult<usize> {
        if self.pe == self.mr.pe {
            self.mr.addr()
        } else {
            Err(MemNotLocalError {})
        }
    }

    fn as_slice(&self) -> MemResult<&[T]> {
        if self.pe == self.mr.pe {
            self.mr.as_slice()
        } else {
            Err(MemNotLocalError {})
        }
    }
    unsafe fn as_mut_slice(&self) -> MemResult<&mut [T]> {
        if self.pe == self.mr.pe {
            self.mr.as_mut_slice()
        } else {
            Err(MemNotLocalError {})
        }
    }
    fn as_ptr(&self) -> MemResult<*const T> {
        if self.pe == self.mr.pe {
            self.mr.as_ptr()
        } else {
            Err(MemNotLocalError {})
        }
    }
    fn as_mut_ptr(&self) -> MemResult<*mut T> {
        if self.pe == self.mr.pe {
            self.mr.as_mut_ptr()
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

impl<T: Dist + 'static> SubRegion<T> for LocalMemoryRegion<T> {
    fn sub_region<R: std::ops::RangeBounds<usize>>(&self, range: R) -> LamellarMemoryRegion<T> {
        LocalMemoryRegion {
            mr: self.mr.sub_region(range),
            pe: self.pe,
        }
        .into()
    }
}

impl<T: Dist + 'static> AsBase for LocalMemoryRegion<T> {
    unsafe fn as_base<B: Dist + 'static>(self) -> LamellarMemoryRegion<B> {
        LocalMemoryRegion {
            mr: self.mr.as_base::<B>(),
            pe: self.pe,
        }
        .into()
    }
}

impl<T: Dist + 'static> MemoryRegionRDMA<T> for LocalMemoryRegion<T> {
    unsafe fn put<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, index: usize, data: U) {
        if self.pe == pe {
            self.mr.put(pe, index, data);
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
            self.mr.iput(pe, index, data);
        } else {
            panic!(
                "trying to put to PE {:?} which does not contain data (pe with data =  {:?})",
                pe, self.pe
            );
            // Err(MemNotLocalError {})
        }
    }
    unsafe fn put_all<U: Into<LamellarMemoryRegion<T>>>(&self, index: usize, data: U) {
        self.mr.put_all(index, data);
    }
    unsafe fn get<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, index: usize, data: U) {
        if self.pe == pe {
            self.mr.get(pe, index, data);
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
            self.mr.put_slice(pe, index, data)
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
    fn my_from(smr: &LocalMemoryRegion<T>, _team: &LamellarTeam) -> Self {
        LamellarArrayInput::LocalMemRegion(smr.clone())
    }
}

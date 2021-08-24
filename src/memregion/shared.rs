use crate::darc::Darc;
use crate::lamellae::{AllocationType, Backend, Lamellae, LamellaeComm, LamellaeRDMA};
use crate::memregion::*;
// use crate::LamellarTeam;
// use crate::TEAMS;
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

// #[derive(serde::Serialize, serde::Deserialize)]
// #[serde(
//     into = "__NetworkMemoryRegion<T>",
//     from = "__NetworkMemoryRegion<T>"
// )]

// #[derive(serde::Serialize, serde::Deserialize, Clone)]
#[lamellar_impl::AmDataRT(Clone)]
// #[serde(bound(serialize="T: serde::Serialize", deserialize="T: serde::Deserialize<'de>"))]
pub struct SharedMemoryRegion<T: Dist + 'static> {
    
    mr: Darc<MemoryRegion<u8>>,
    sub_region_offset: usize,
    sub_region_size: usize,
    phantom: PhantomData<T>,
}




impl<T: Dist + 'static> SharedMemoryRegion<T> {
    pub(crate) fn new(
        size: usize,
        // lamellae: Arc<Lamellae>,
        team: Arc<LamellarTeam>,
        alloc: AllocationType,
    ) -> SharedMemoryRegion<T> {
        SharedMemoryRegion {
            mr: Darc::new(
                team.clone(),
                MemoryRegion::new(size, team.team.lamellae.clone(), alloc),
            )
            .expect("memregions can only be created on a member of the team"),
            sub_region_offset: 0,
            sub_region_size: 0,
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

impl<T: Dist + 'static> RegisteredMemoryRegion<T> for SharedMemoryRegion<T> {
    fn len(&self) -> usize {
        self.mr.len()
    }
    fn addr(&self) -> MemResult<usize> {
        self.mr.addr()
    }
    fn as_slice(&self) -> MemResult<&[T]> {
        self.mr.as_casted_slice::<T>()
    }
    unsafe fn as_mut_slice(&self) -> MemResult<&mut [T]> {
        self.mr.as_casted_mut_slice::<T>()
    }
    fn as_ptr(&self) -> MemResult<*const T> {
        self.mr.as_casted_ptr::<T>()
    }
    fn as_mut_ptr(&self) -> MemResult<*mut T> {
        self.mr.as_casted_mut_ptr::<T>()
    }
}

impl<T: Dist + 'static> MemRegionId for SharedMemoryRegion<T> {
    fn id(&self) -> usize {
        self.mr.id()
    }
}

impl<T: Dist + 'static> SubRegion<T> for SharedMemoryRegion<T> {
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
        SharedMemoryRegion {
            mr: self.mr.clone(),
            sub_region_offset: self.sub_region_offset + start,
            sub_region_size: (end - start),
            phantom: PhantomData,
        }
        .into()
        // SharedMemoryRegion {
        //     mr: self.mr.sub_region(range),
        // }
        // .into()
    }
}

impl<T: Dist + 'static> AsBase for SharedMemoryRegion<T> {
    unsafe fn as_base<B: Dist + 'static>(self) -> LamellarMemoryRegion<B> {
        SharedMemoryRegion {
            mr: self.mr.clone(),
            sub_region_offset: self.sub_region_offset,
            sub_region_size: self.sub_region_size,
            phantom: PhantomData,
        }
        .into()
        // // SharedMemoryRegion {
        // //     mr: self.mr.as_base::<B>(),
        // // }
        // // .into()
    }
}

//#[prof]
impl<T: Dist + 'static> MemoryRegionRDMA<T> for SharedMemoryRegion<T> {
    unsafe fn put<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, index: usize, data: U) {
        // let len = data.clone().into().len();
        // if index + len > self.sub_region_size {
        //     println!("{:?} {:?} {:?}", self.sub_region_size, index, len);
        //     panic!("sub_region index out of bounds");
        // }
        // self.mr.put(pe, self.sub_region_offset + index, data);
        self.mr.put(pe, index, data);
    }
    fn iput<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, index: usize, data: U) {
        // let len = data.clone().into().len();
        // if index + len > self.sub_region_size {
        //     println!("{:?} {:?} {:?}", self.sub_region_size, index, len);
        //     panic!("sub_region index out of bounds");
        // }
        // self.mr.iput(pe, self.sub_region_offset + index, data);
        self.mr.iput(pe, index, data);
    }
    unsafe fn put_all<U: Into<LamellarMemoryRegion<T>>>(&self, index: usize, data: U) {
        // let len = data.clone().into().len();
        // if index + len > self.sub_region_size {
        //     println!("{:?} {:?} {:?}", self.sub_region_size, index, len);
        //     panic!("sub_region index out of bounds");
        // }
        // self.mr.put_all(self.sub_region_offset + index, data);
        self.mr.put_all(index, data);
    }
    unsafe fn get<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, index: usize, data: U) {
        // let len = data.clone().into().len();
        // if index + len > self.sub_region_size {
        //     println!("{:?} {:?} {:?}", self.sub_region_size, index, len);
        //     panic!("sub_region index out of bounds");
        // }
        // self.mr.get(pe, self.sub_region_offset + index, data);
        self.mr.get(pe, index, data);
    }
}

impl<T: Dist + 'static> RTMemoryRegionRDMA<T> for SharedMemoryRegion<T> {
    unsafe fn put_slice(&self, pe: usize, index: usize, data: &[T]) {
        // if index + data.len() > self.sub_region_size {
        //     println!("{:?} {:?} {:?}", self.sub_region_size, index, data.len());
        //     panic!("sub_region index out of bounds");
        // }
        // self.mr.put_slice(pe, self.sub_region_offset + index, data)
        self.mr.put_slice(pe, index, data)
    }
}

impl<T: Dist + 'static> std::fmt::Debug for SharedMemoryRegion<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // let slice = unsafe { std::slice::from_raw_parts(self.addr as *const T, self.size) };
        // write!(f, "{:?}", slice)
        write!(f, "[{:?}] shared mem region:  {:?} ", self.mr.pe, self.mr,)
    }
}

impl<T: Dist + 'static> From<&SharedMemoryRegion<T>> for LamellarArrayInput<T> {
    fn from(smr: &SharedMemoryRegion<T>) -> Self {
        LamellarArrayInput::SharedMemRegion(smr.clone())
    }
}

impl<T: Dist + 'static> MyFrom<&SharedMemoryRegion<T>> for LamellarArrayInput<T> {
    fn my_from(smr: &SharedMemoryRegion<T>, _team: &Arc<LamellarTeam>) -> Self {
        LamellarArrayInput::SharedMemRegion(smr.clone())
    }
}

// //#[prof]
// impl<T: Dist + 'static> Drop for SharedMemoryRegion<T> {
//     fn drop(&mut self) {
//         let cnt = self.cnt.fetch_sub(1, Ordering::SeqCst);
//         // //println!("drop: {:?} {:?}",self,cnt);

//         if cnt == 1 {
//             ACTIVE.remove(self.backend);
//             // println!("trying to dropping mem region {:?}",self);
//             if self.addr != 0{
//                 if self.local {
//                     self.rdma.rt_free(self.addr - self.rdma.base_addr()); // - self.rdma.base_addr());
//                 } else {
//                     self.rdma.free(self.addr);
//                 }
//             }
//             // ACTIVE.print();
//             //println!("dropping mem region {:?}",self);
//         }
//     }
// }

//#[prof]
// impl<T: Dist + 'static> std::fmt::Debug for SharedMemoryRegion<T> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         // let slice = unsafe { std::slice::from_raw_parts(self.addr as *const T, self.size) };
//         // write!(f, "{:?}", slice)
//         write!(
//             f,
//             "addr {:#x} size {:?} backend {:?} cnt: {:?}",
//             self.addr,
//             self.size,
//             self.backend,
//             self.cnt.load(Ordering::SeqCst)
//         )
//     }
// }

// //#[prof]
// impl<T: Dist + 'static> std::fmt::Debug
//     for __NetworkMemoryRegion<T>
// {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         // let slice = unsafe { std::slice::from_raw_parts(self.addr as *const T, self.size) };
//         // write!(f, "{:?}", slice)
//         write!(
//             f,
//             "pe {:?} size {:?} backend {:?} ",
//             self.pe, self.size, self.backend,
//         )
//     }
// }

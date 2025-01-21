pub(crate) mod atomic;
pub(crate) mod error;
pub(crate) mod rdma;

pub(crate) use atomic::*;
pub(crate) use error::*;
pub(crate) use rdma::*;

use super::Backend;

// use crate::LamellarMemoryRegion;
use crate::{
    Serialize, Deserialize,
    lamellae::{
        local_lamellae::comm::LocalComm, shmem_lamellae::comm::ShmemComm, AllocationType,
        SerializedData
    },
};
#[cfg(feature = "enable-rofi")]
use crate::lamellae::rofi::rofi_comm::*;
#[cfg(feature = "enable-libfabric")]
use crate::lamellae::{
    libfabric::libfabric_comm::*, libfabric_async::libfabric_async_comm::*, LibFabAsyncData,
};
#[cfg(feature = "enable-rofi-rust")]
use crate::lamellae::{
    rofi_rust::rofi_rust_comm::*, rofi_rust_async::rofi_rust_async_comm::*, RofiRustAsyncData,
    RofiRustData,
};

use enum_dispatch::enum_dispatch;
use std::sync::Arc;
use derive_more::{Add, Sub,From, Into};

// use super::LamellaeRDMA;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CmdQStatus {
    Active = 1,
    Finished = 2,
    ShuttingDown = 3,
    Panic = 4,
}

#[enum_dispatch(CommMem,CommRdma,CommShutdown,CommInfo,CommProgress)]
#[derive(Debug)]
pub(crate) enum Comm {
    #[cfg(feature = "rofi")]
    Rofi(RofiComm),
    #[cfg(feature = "enable-rofi-rust")]
    RofiRust(RofiRustComm),
    #[cfg(feature = "enable-rofi-rust")]
    RofiRustAsync(RofiRustAsyncComm),
    #[cfg(feature = "enable-libfabric")]
    LibFab(LibFabComm),
    #[cfg(feature = "enable-libfabric")]
    LibFabAsync(LibFabAsyncComm),
    Shmem(ShmemComm),
    Local(LocalComm),
}

impl Comm {
    pub(crate) fn new_serialized_data(
        self: &Arc<Comm>,
        size: usize,
    ) -> Result<SerializedData, anyhow::Error> {
        SerializedData::new(self.clone(), size)
    }
}

impl CommAtomic for Comm {
    fn atomic_avail<T>(&self) -> bool {
        false
    }
    fn atomic_op<T: NetworkAtomic>(&self, _op: AtomicOp<T>,_pe: usize, _remote_addr: usize) -> RdmaHandle<T> {
        match self {
            Comm::Shmem(comm) => comm.atomic_op(_op,_pe,_remote_addr),
            Comm::Local(comm) => comm.atomic_op(_op,_pe,_remote_addr),
        }
    }
    fn atomic_fetch_op<T: NetworkAtomic>(&self, _op: AtomicOp<T>,_pe: usize, _remote_addr: usize, _result: &mut [T]) -> RdmaHandle<T> {
        match self {
            Comm::Shmem(comm) => comm.atomic_fetch_op(_op,_pe,_remote_addr,_result),
            Comm::Local(comm) => comm.atomic_fetch_op(_op,_pe,_remote_addr,_result),
        }
    }
}

#[enum_dispatch]
pub(crate) trait CommShutdown {
    fn force_shutdown(&self);
}

#[derive(Debug,Clone)]
pub(crate) struct  CommAlloc{
    pub(crate) addr: usize,
    pub(crate) size: usize,
    pub(crate) alloc_type: CommAllocType,
}

// unsafe impl Send for CommAlloc {}
// unsafe impl Sync for CommAlloc {}


impl CommAlloc{
    pub(crate) fn byte_add(&self, offset: usize) -> CommAllocAddr {
        debug_assert!(self.addr + offset < self.size);
        CommAllocAddr(self.addr + offset)
    }
    pub(crate) fn as_slice<T>(&self) -> CommSlice<T> {
        CommSlice{
            addr: CommAllocAddr(self.addr),
            size: self.size/std::mem::size_of::<T>(),
            _phantom: std::marker::PhantomData
        }
    }
    pub(crate) fn slice_at_offset<T>(&self, offset: usize, size: usize) -> CommSlice<T> {
        debug_assert!(self.addr + offset < self.size && self.addr + offset + size < self.size);
        CommSlice{
            addr: CommAllocAddr(self.addr + offset),
            size,
            _phantom: std::marker::PhantomData
        }
    }
    pub(crate) unsafe fn as_ptr<T>(&self) -> *const T {
        self.addr as *const T
    }
    pub(crate) unsafe fn as_mut_ptr<T>(&self) -> *mut T {
        self.addr as *mut T
    }
    pub(crate) fn comm_addr(&self) -> CommAllocAddr {
        CommAllocAddr(self.addr)
    }
}

#[derive(Debug,Copy,Clone,Eq, PartialEq, PartialOrd, Ord)]
pub(crate) enum CommAllocType {
    RtHeap,
    Fabric,
    Remote,
}
// unsafe impl Send for CommAllocType {}
// unsafe impl Sync for CommAllocType {}

#[derive(Debug,Copy,Clone,Add,Sub,From,Into,PartialEq,Eq,PartialOrd,Ord,Serialize,Deserialize)]
pub(crate) struct  CommAllocAddr(pub(crate) usize);

impl std::fmt::LowerHex for CommAllocAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:x}", self.0)
    }
}
// unsafe impl Send for CommAllocAddr {}
// unsafe impl Sync for CommAllocAddr {}

impl CommAllocAddr {
    pub(crate) unsafe fn as_ptr<T>(&self) -> *const T {
        self.0 as *const T
    }
    pub(crate) unsafe fn as_mut_ptr<T>(&self) -> *mut T {
        self.0 as *mut T
    }

    pub(crate) unsafe fn as_ref<T>(&self) -> Option<&T> {
       self.as_ptr::<T>().as_ref()
    }

    pub(crate) unsafe fn as_mut<T>(&self) -> Option<&mut T> {
        self.as_mut_ptr::<T>().as_mut()
    }
}

impl std::ops::Add<usize> for CommAllocAddr {
    type Output = CommAllocAddr;
    fn add(self, rhs: usize) -> Self::Output {
        CommAllocAddr(self.0 + rhs)
    }
}
impl std::ops::Add<usize> for &CommAllocAddr {
    type Output = CommAllocAddr;
    fn add(self, rhs: usize) -> Self::Output {
        CommAllocAddr(self.0 + rhs)
    }
}

impl std::convert::AsRef<CommAllocAddr> for CommAllocAddr {
    fn as_ref(&self) -> &CommAllocAddr {
        self
    }
}


#[derive(Debug,Copy,Clone)]
pub(crate) struct CommSlice<T>{
    addr: CommAllocAddr,
    size: usize,
    _phantom: std::marker::PhantomData<T>,
}


// unsafe impl<T> Send for CommSlice<T> {}
// unsafe impl<T> Sync for CommSlice<T> {}


impl <T> CommSlice<T>{
    pub(crate) fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.addr.as_ptr(), self.size) }
    }
    pub(crate) fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.addr.as_mut_ptr(), self.size) }
    }
    pub(crate) fn sub_slice(&self,range: impl std::ops::RangeBounds<usize>) -> Self {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&index) => index,
            std::ops::Bound::Excluded(&index) => index + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&index) => index + 1,
            std::ops::Bound::Excluded(&index) => index,
            std::ops::Bound::Unbounded => self.len(),
        };
        debug_assert!(start <= end);
        debug_assert!(end <= self.len());
        CommSlice{
            addr: self.addr + start,
            size: end - start,
            _phantom: std::marker::PhantomData
        }
        
    }
    pub(crate) fn as_mut_ptr(&self) -> *mut T {
        unsafe {self.addr.as_mut_ptr()}
    }
    pub(crate) fn as_ptr(&self) -> *const T {
        unsafe { self.addr.as_ptr() }
    }
    pub(crate) fn len(&self) -> usize {
        self.size
    }
    pub(crate) fn addr(&self) -> usize {
        self.addr.into()
    }

    
    pub(crate) fn index_addr(&self, index: usize) -> CommAllocAddr {
        debug_assert!(index < self.size);
        self.addr + index
    }

    pub(crate) unsafe fn from_raw_parts(data: *const T, len: usize) -> Self {
        CommSlice{
            addr: CommAllocAddr(data as usize),
            size: len,
            _phantom: std::marker::PhantomData
        }
    }

    pub(crate) unsafe fn from_slice(slice: &[T]) -> Self {
        CommSlice::from_raw_parts(slice.as_ptr(), slice.len())
    }

    pub(crate) fn contains<Addr: AsRef<CommAllocAddr>>(&self, addr: Addr) -> bool {
        let addr = addr.as_ref();
        &self.addr <= addr && addr < &(self.addr + self.size)
    }
}



impl<T> std::ops::Deref for CommSlice<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.addr.as_ptr(), self.size) }
    }
}

impl<T> std::ops::DerefMut for CommSlice<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.addr.as_mut_ptr(), self.size) }
    }
}





#[enum_dispatch]
pub(crate) trait CommMem {
    fn alloc(&self, size: usize, alloc: AllocationType, align: usize) -> error::AllocResult<CommAlloc>;
    fn free(&self, alloc: CommAlloc);
    fn rt_alloc(&self, size: usize, align: usize) -> error::AllocResult<CommAlloc>;
    fn rt_check_alloc(&self, size: usize, align: usize) -> bool;
    fn rt_free(&self, alloc: CommAlloc);
    fn mem_occupied(&self) -> usize;
    fn alloc_pool(&self, min_size: usize);
    fn num_pool_allocs(&self) -> usize;
    fn print_pools(&self);
    fn base_addr(&self) -> CommAllocAddr;
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> CommAllocAddr;
    fn remote_addr(&self, remote_pe: usize, local_addr: usize) -> CommAllocAddr;
    fn local_alloc(&self, remote_pe: usize, remote_addr: CommAllocAddr) -> error::AllocResult<CommAlloc>;
}

#[enum_dispatch]
pub(crate) trait CommProgress {
    fn flush(&self);
    fn wait(&self);
    fn barrier(&self);
}

#[enum_dispatch]
pub(crate) trait CommInfo {
    fn my_pe(&self) -> usize;
    fn num_pes(&self) -> usize;
    fn backend(&self) -> Backend;
    fn MB_sent(&self) -> f64;
}

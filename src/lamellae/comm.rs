pub(crate) mod atomic;
pub(crate) mod error;
pub(crate) mod rdma;

pub(crate) use atomic::*;
pub(crate) use error::*;
pub(crate) use rdma::*;
use tracing::trace;

use super::Backend;

// use crate::LamellarMemoryRegion;
#[cfg(feature = "rofi-c")]
use crate::lamellae::rofi_c_lamellae::comm::RofiCComm;
#[cfg(feature = "enable-libfabric")]
use crate::lamellae::{
    libfabric::libfabric_comm::*, libfabric_async::libfabric_async_comm::*, LibFabAsyncData,
};
#[cfg(feature = "enable-rofi-rust")]
use crate::lamellae::{
    rofi_rust::rofi_rust_comm::*, rofi_rust_async::rofi_rust_async_comm::*, RofiRustAsyncData,
    RofiRustData,
};
use crate::{
    active_messaging::AMCounters,
    lamellae::{
        local_lamellae::comm::LocalComm, shmem_lamellae::comm::ShmemComm, AllocationType,
        SerializedData,
    },
    scheduler::Scheduler,
    Deserialize, Serialize,
};

use derive_more::{Add, From, Into, Sub};
use enum_dispatch::enum_dispatch;
use std::sync::Arc;

// use super::LamellaeRDMA;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CmdQStatus {
    Active = 1,
    Finished = 2,
    ShuttingDown = 3,
    Panic = 4,
}

#[enum_dispatch(CommMem, CommRdma, CommShutdown, CommInfo, CommProgress)]
#[derive(Debug)]
pub(crate) enum Comm {
    #[cfg(feature = "rofi-c")]
    RofiC(RofiCComm),
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
    #[tracing::instrument(skip(self), level = "debug")]
    pub(crate) fn new_serialized_data(
        self: &Arc<Comm>,
        size: usize,
    ) -> Result<SerializedData, anyhow::Error> {
        // trace!("new serialized data");
        SerializedData::new(self.clone(), size)
    }
}

impl CommAtomic for Comm {
    fn atomic_avail<T>(&self) -> bool {
        false
    }
    fn atomic_op<T: NetworkAtomic>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        _op: AtomicOp<T>,
        _pe: usize,
        _remote_addr: usize,
    ) -> RdmaHandle<T> {
        match self {
            #[cfg(feature = "rofi-c")]
            Comm::RofiC(comm) => comm.atomic_op(scheduler, counters, _op, _pe, _remote_addr),
            Comm::Shmem(comm) => comm.atomic_op(scheduler, counters, _op, _pe, _remote_addr),
            Comm::Local(comm) => comm.atomic_op(scheduler, counters, _op, _pe, _remote_addr),
        }
    }
    fn atomic_fetch_op<T: NetworkAtomic>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        _op: AtomicOp<T>,
        _pe: usize,
        _remote_addr: usize,
        _result: &mut [T],
    ) -> RdmaHandle<T> {
        match self {
            #[cfg(feature = "rofi-c")]
            Comm::RofiC(comm) => comm.atomic_fetch_op(scheduler, counters, _op, _pe, _remote_addr, _result),
            Comm::Shmem(comm) => {
                comm.atomic_fetch_op(scheduler, counters, _op, _pe, _remote_addr, _result)
            }
            Comm::Local(comm) => {
                comm.atomic_fetch_op(scheduler, counters, _op, _pe, _remote_addr, _result)
            }
        }
    }
}

#[enum_dispatch]
pub(crate) trait CommShutdown {
    fn force_shutdown(&self);
}

#[derive(Clone)]
pub(crate) struct CommAlloc {
    pub(crate) addr: usize,
    pub(crate) size: usize,
    pub(crate) alloc_type: CommAllocType,
}
impl std::fmt::Debug for CommAlloc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CommAlloc {{ addr: {:x}, size: {:?}, alloc_type: {:?} }}",
            self.addr, self.size, self.alloc_type
        )
    }
}

// unsafe impl Send for CommAlloc {}
// unsafe impl Sync for CommAlloc {}

impl CommAlloc {
    pub(crate) fn byte_add(&self, offset: usize) -> CommAllocAddr {
        debug_assert!(offset < self.size);
        CommAllocAddr(self.addr + offset)
    }
    #[tracing::instrument(skip(self), level = "debug")]
    pub(crate) fn as_slice<T>(&self) -> CommSlice<T> {
        CommSlice {
            addr: CommAllocAddr(self.addr),
            size: self.size / std::mem::size_of::<T>(),
            _phantom: std::marker::PhantomData,
        }
    }
    #[tracing::instrument( level = "debug")]
    pub(crate) fn slice_at_byte_offset<T>(&self, offset: usize, num_elems: usize) -> CommSlice<T> {
        trace!(
            "{:?} offset: {}  num_elems: {} bytes: {}",
            self,
            offset,
            num_elems,
            num_elems * std::mem::size_of::<T>()
        );
        debug_assert!(
            offset < self.size && offset + num_elems * std::mem::size_of::<T>() <= self.size
        );
        CommSlice {
            addr: CommAllocAddr(self.addr + offset),
            size: num_elems,
            _phantom: std::marker::PhantomData,
        }
    }
    
    pub(crate) unsafe fn as_ptr<T>(&self) -> *const T {
        self.addr as *const T
    }
    pub(crate) unsafe fn as_mut_ptr<T>(&self) -> *mut T {
        self.addr as *mut T
    }
    pub(crate) unsafe fn as_ref<T>(&self) -> Option<&T> {
        self.as_ptr::<T>().as_ref()
    }

    pub(crate) unsafe fn as_mut<T>(&self) -> Option<&mut T> {
        self.as_mut_ptr::<T>().as_mut()
    }
    pub(crate) fn comm_addr(&self) -> CommAllocAddr {
        CommAllocAddr(self.addr)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub(crate) enum CommAllocType {
    RtHeap,
    Fabric,
    Remote,
}
// unsafe impl Send for CommAllocType {}
// unsafe impl Sync for CommAllocType {}

#[derive(
    Copy, Clone, Add, Sub, From, Into, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub(crate) struct CommAllocAddr(pub(crate) usize);

impl Into<usize> for &CommAllocAddr {
    fn into(self) -> usize {
        self.0
    }
}

impl std::fmt::Debug for CommAllocAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:x}", self.0)
    }
}

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

#[derive(Debug, Copy, Clone)]
pub(crate) struct CommSlice<T> {
    pub(crate) addr: CommAllocAddr,
    size: usize,
    _phantom: std::marker::PhantomData<T>,
}

// unsafe impl<T> Send for CommSlice<T> {}
// unsafe impl<T> Sync for CommSlice<T> {}

impl<T> CommSlice<T> {
    pub(crate) fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.addr.as_ptr(), self.size) }
    }
    pub(crate) fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.addr.as_mut_ptr(), self.size) }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn sub_slice(&self, range: impl std::ops::RangeBounds<usize>) -> Self {
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
        // trace!("subslice start: {} end: {} new addr: {:?} new size: {}", start, end, self.addr + start*std::mem::size_of::<T>(), end - start);
        CommSlice {
            addr: self.addr + start*std::mem::size_of::<T>(),
            size: end - start,
            _phantom: std::marker::PhantomData,
        }
    }
    pub(crate) fn as_mut_ptr(&self) -> *mut T {
        unsafe { self.addr.as_mut_ptr() }
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
        self.addr + index*std::mem::size_of::<T>()
    }

    pub(crate) unsafe fn from_raw_parts(data: *const T, len: usize) -> Self {
        CommSlice {
            addr: CommAllocAddr(data as usize),
            size: len,
            _phantom: std::marker::PhantomData,
        }
    }

    pub(crate) unsafe fn from_slice(slice: &[T]) -> Self {
        CommSlice::from_raw_parts(slice.as_ptr(), slice.len())
    }

    pub(crate) fn contains<Addr: AsRef<CommAllocAddr>>(&self, addr: Addr) -> bool {
        let addr = addr.as_ref();
        &self.addr <= addr && addr < &(self.addr + self.size)
    }

    pub(crate) fn num_bytes(&self) -> usize {
        self.size * std::mem::size_of::<T>()
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
    fn alloc(
        &self,
        size: usize,
        alloc: AllocationType,
        align: usize,
    ) -> error::AllocResult<CommAlloc>;
    fn free(&self, alloc: CommAlloc);
    fn rt_alloc(&self, size: usize, align: usize) -> error::AllocResult<CommAlloc>;
    fn rt_check_alloc(&self, size: usize, align: usize) -> bool;
    fn rt_free(&self, alloc: CommAlloc);
    fn mem_occupied(&self) -> usize;
    fn alloc_pool(&self, min_size: usize);
    fn num_pool_allocs(&self) -> usize;
    fn print_pools(&self);
    fn base_addr(&self) -> CommAllocAddr;
    // this translates a remote address to a local address
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> CommAllocAddr;
    // this translates a local address to a remote address
    fn remote_addr(&self, remote_pe: usize, local_addr: usize) -> CommAllocAddr;
    // this checks for an allocation at the given address
    fn get_alloc(&self,  addr: CommAllocAddr) -> error::AllocResult<CommAlloc>;
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
    #[allow(non_snake_case)]
    fn MB_sent(&self) -> f64;
}

// pub(crate) struct CommOpHandle<'a, T = ()> {
//     fut: Pin<Box<dyn Future<Output =T> + Send + 'a> >
// }

// impl<'a, T> CommOpHandle<'a, T> {
//     pub(crate) fn new(fut: impl Future<Output =T> + Send + 'a) -> Self {
//         Self {
//             fut: Box::pin(fut)
//         }
//     }

//     pub(crate) fn block(self) -> T{
//         #[cfg(feature="tokio-executor")]
//         return Handle::current().block_on(async {self.fut.await});
//         #[cfg(not(feature="tokio-executor"))]
//         return block_on(async {self.fut.await});
//     }
// }

// impl<'a, T> Future for CommOpHandle<'a, T> {
//         type Output = T;
//     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         let mut this = self.get_mut();
//         let guard = ready!(this.fut.as_mut().poll(cx));
//         Poll::Ready(guard)
//     }
// }
pub(crate) mod alloc;
pub(crate) mod atomic;
pub(crate) mod error;
pub(crate) mod rdma;
pub(crate) mod slice;

pub(crate) use alloc::*;
pub(crate) use atomic::*;
pub(crate) use error::*;
pub(crate) use rdma::*;
pub(crate) use slice::*;

pub use rdma::Remote;

use super::Backend;

// use crate::LamellarMemoryRegion;
#[cfg(feature = "rofi-c")]
use crate::lamellae::rofi_c_lamellae::comm::RofiCComm;
// #[cfg(feature = "enable-libfabric")]
// use crate::lamellae::{
//     libfabric::libfabric_comm::*, libfabric_async::libfabric_async_comm::*, LibfabricAsyncData,
// };
#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabric_lamellae::comm::LibfabricComm;
// #[cfg(feature = "enable-rofi-rust")]
// use crate::lamellae::{
//     rofi_rust::rofi_rust_comm::*, rofi_rust_async::rofi_rust_async_comm::*, RofiRustAsyncData,
//     RofiRustData,
// };

#[cfg(feature = "enable-ucx")]
use crate::lamellae::ucx_lamellae::comm::UcxComm;
use crate::lamellae::{
    local_lamellae::comm::LocalComm, shmem_lamellae::comm::ShmemComm, AllocationType,
    SerializedData,
};

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

#[enum_dispatch(CommMem, CommShutdown, CommInfo, CommProgress)]
#[derive(Debug)]
pub(crate) enum Comm {
    #[cfg(feature = "enable-libfabric")]
    Libfabric(LibfabricComm),
    #[cfg(feature = "enable-ucx")]
    Ucx(UcxComm),
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricAsync(LibfabricAsyncComm),
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

// impl CommAtomic for Comm {
//     fn atomic_avail<T: 'static>(&self) -> bool {
//         match self {
//             #[cfg(feature = "rofi-c")]
//             Comm::RofiC(comm) => comm.atomic_avail::<T>(),
//             Comm::Shmem(comm) => comm.atomic_avail::<T>(),
//             Comm::Local(comm) => comm.atomic_avail::<T>(),
//             #[cfg(feature = "enable-libfabric")]
//             Comm::Libfabric(comm) => comm.atomic_avail::<T>(),
//             #[cfg(feature = "enable-ucx")]
//             Comm::Ucx(comm) => comm.atomic_avail::<T>(),
//         }
//     }
//     fn atomic_op<T: Copy>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Vec<Arc<AMCounters>>,
//         op: AtomicOp<T>,
//         pe: usize,
//         remote_alloc: CommAllocInner,
//         offset: usize,
//     ) -> AtomicOpHandle<T> {
//         match self {
//             #[cfg(feature = "rofi-c")]
//             Comm::RofiC(comm) => comm.atomic_op(scheduler, counters, op, pe, remote_alloc),
//             Comm::Shmem(comm) => comm.atomic_op(scheduler, counters, op, pe, remote_alloc, offset),
//             Comm::Local(comm) => comm.atomic_op(scheduler, counters, op, pe, remote_alloc, offset),
//             #[cfg(feature = "enable-libfabric")]
//             Comm::Libfabric(comm) => {
//                 comm.atomic_op(scheduler, counters, op, pe, remote_alloc, offset)
//             }
//             #[cfg(feature = "enable-ucx")]
//             Comm::Ucx(comm) => comm.atomic_op(scheduler, counters, op, pe, remote_alloc, offset),
//         }
//     }
//     fn atomic_fetch_op<T: Copy>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Vec<Arc<AMCounters>>,
//         op: AtomicOp<T>,
//         pe: usize,
//         remote_alloc: CommAllocInner,
//         offset: usize,
//     ) -> AtomicFetchOpHandle<T> {
//         match self {
//             #[cfg(feature = "rofi-c")]
//             Comm::RofiC(comm) => comm.atomic_fetch_op(scheduler, counters, op, pe, remote_addr),
//             Comm::Shmem(comm) => {
//                 comm.atomic_fetch_op(scheduler, counters, op, pe, remote_alloc, offset)
//             }
//             Comm::Local(comm) => {
//                 comm.atomic_fetch_op(scheduler, counters, op, pe, remote_alloc, offset)
//             }
//             #[cfg(feature = "enable-libfabric")]
//             Comm::Libfabric(comm) => {
//                 comm.atomic_fetch_op(scheduler, counters, op, pe, remote_alloc, offset)
//             }
//             #[cfg(feature = "enable-ucx")]
//             Comm::Ucx(comm) => {
//                 comm.atomic_fetch_op(scheduler, counters, op, pe, remote_alloc, offset)
//             }
//         }
//     }
// }

#[enum_dispatch]
pub(crate) trait CommShutdown {
    fn force_shutdown(&self);
}

#[enum_dispatch]
pub(crate) trait CommMem {
    fn alloc(
        &self,
        size: usize,
        alloc: AllocationType,
        align: usize,
    ) -> error::AllocResult<CommAlloc>;

    fn rt_alloc(&self, size: usize, align: usize) -> error::AllocResult<CommAlloc>;
    fn rt_check_alloc(&self, size: usize, align: usize) -> bool;
    fn mem_occupied(&self) -> usize;
    fn alloc_pool(&self, min_size: usize);
    fn num_pool_allocs(&self) -> usize;
    fn print_pools(&self);
    // this translates a remote address to a local address
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> CommAllocAddr;

    // this creates a CommAlloc from a remote PE and remote address that represents a one-sided allocation
    // we can only perform rdma operations to remote_pe using this allocation
    fn one_sided_alloc_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
        num_bytes: usize,
    ) -> CommAlloc;

    // this translates a remote address to its local allocation + offset within that allocation
    // we need this to support onesided allocations that arrive at remote node
    fn local_alloc_and_offset_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
    ) -> (CommAlloc, usize);

    // think of this as Box::from_raw, it takes a raw remote address and returns the CommAlloc that contains it
    // this does not not increment the ref count of the CommAlloc, this should be matched to a alloc.leak() call typically
    // we need this to enable appropriate freeing of serialized active message data after it has been processed at a remote PE
    fn local_rt_alloc_from_addr(&self, addr: usize) -> error::AllocResult<CommAlloc>;
    // this translates a local address to a remote address
    fn remote_addr(&self, remote_pe: usize, local_addr: usize) -> CommAllocAddr;
    // this checks for an allocation at the given address
    fn get_alloc(&self, addr: CommAllocAddr) -> error::AllocResult<CommAlloc>;
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
    fn atomic_avail<T: 'static>(&self) -> bool
    where
        Self: Sized;
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

pub(crate) mod alloc;
pub(crate) mod atomic;
pub(crate) mod error;
pub(crate) mod rdma;
pub(crate) mod slice;
pub(crate) mod collective;

pub(crate) use alloc::*;
pub(crate) use atomic::*;
pub(crate) use error::*;
pub(crate) use rdma::*;
pub(crate) use slice::*;

use super::Backend;
pub use rdma::Remote;

#[cfg(feature = "enable-libfabric-sys")]
use crate::lamellae::libfabric_sys_lamellae::comm::LibfabricSysComm;

#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabric_lamellae::comm::LibfabricComm;

#[cfg(feature = "enable-libfabric-mt")]
use crate::lamellae::libfabric_lamellae_mt::comm::LibfabricMtComm;

#[cfg(feature = "enable-libfabric-async")]
use crate::lamellae::libfabric_async_lamellae::comm::LibfabricAsyncComm;

#[cfg(feature = "enable-rofi-c")]
use crate::lamellae::rofi_c_lamellae::comm::RofiCComm;
#[cfg(feature = "enable-ucx")]
use crate::lamellae::ucx_lamellae::comm::UcxComm;
#[cfg(feature = "enable-ucx-mt")]
use crate::lamellae::ucx_lamellae_mt::comm::UcxMtComm;
use crate::lamellae::{
    collective::ReduceOp, local_lamellae::comm::LocalComm, shmem_lamellae::comm::ShmemComm, AllocationType, SerializedData
};

use enum_dispatch::enum_dispatch;
use std::sync::Arc;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CmdQStatus {
    Active = 1,
    Finished = 2,
    ShuttingDown = 3,
    Panic = 4,
}

pub(crate) enum CollectiveOpKind {
    Barrier,
    Broadcast,
    AllToAll,
    AllReduce(ReduceOp),
    AllGather,
    ReduceScatter(ReduceOp),
    Reduce(ReduceOp),
    Scatter,
    Gather,
}

#[enum_dispatch(CommMem, CommShutdown, CommInfo, CommProgress)]
#[derive(Debug)]
pub(crate) enum Comm {
    #[cfg(feature = "enable-libfabric-sys")]
    LibfabricSys(LibfabricSysComm),
    #[cfg(feature = "enable-libfabric")]
    Libfabric(LibfabricComm),
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt(LibfabricMtComm),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync(LibfabricAsyncComm),
    #[cfg(feature = "enable-ucx")]
    Ucx(UcxComm),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt(UcxMtComm),
    #[cfg(feature = "enable-rofi-c")]
    RofiC(RofiCComm),
    Shmem(ShmemComm),
    Local(LocalComm),
}

impl Comm {
    //#[tracing::instrument(skip(self), level = "debug")]
    pub(crate) fn new_serialized_data(
        self: &Arc<Comm>,
        size: usize,
    ) -> Result<SerializedData, anyhow::Error> {
        // trace!("new serialized data");
        SerializedData::new(self.clone(), size)
    }
}

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

    // think of this as Box::from_raw, it takes a raw local address and returns the CommAlloc that contains it
    // this does not not increment the ref count of the CommAlloc, this should be matched to a alloc.leak() call typically
    // we need this to enable appropriate freeing of serialized active message data after it has been processed at a remote PE
    // or for use in reconstructing a Darc<LamellarTeamRT> from a raw pointer
    fn local_rt_alloc_from_local_addr(&self, addr: usize) -> error::AllocResult<CommAlloc>;

    // this translates a local address to a remote address
    fn remote_addr(&self, remote_pe: usize, local_addr: usize) -> CommAllocAddr;

    // this checks for an allocation at the given address and returns it if it exists incrementing the ref count
    fn get_alloc_cloned(&self, addr: CommAllocAddr) -> error::AllocResult<CommAlloc>;
}

#[enum_dispatch]
pub(crate) trait CommProgress {
    fn flush_all(&self);
    fn thread_flush(&self) {
        self.flush_all();
    }
    fn wait_all(&self);
    fn thread_wait(&self) {
        self.wait_all();
    }
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
    fn atomic_op_avail<T: 'static>(&self, _op: AtomicOp<T>) -> bool
    where
        Self: Sized,
    {
        self.atomic_avail::<T>()
    }
    fn collective_avail<T: 'static>(&self, op: CollectiveOpKind) -> bool;

    #[allow(non_snake_case)]
    fn MB_sent(&self) -> f64;
}

pub(crate) mod atomic;
pub(crate) mod error;
pub(crate) mod rdma;

pub(crate) use atomic::*;
pub(crate) use error::*;
pub(crate) use rdma::*;

pub use rdma::Remote;

use tracing::trace;

use super::Backend;

// use crate::LamellarMemoryRegion;
#[cfg(feature = "rofi-c")]
use crate::lamellae::rofi_c_lamellae::comm::RofiCComm;
// #[cfg(feature = "enable-libfabric")]
// use crate::lamellae::{
//     libfabric::libfabric_comm::*, libfabric_async::libfabric_async_comm::*, LibfabricAsyncData,
// };
#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabric_lamellae::{comm::LibfabricComm, fabric::LibfabricAlloc};
// #[cfg(feature = "enable-rofi-rust")]
// use crate::lamellae::{
//     rofi_rust::rofi_rust_comm::*, rofi_rust_async::rofi_rust_async_comm::*, RofiRustAsyncData,
//     RofiRustData,
// };

#[cfg(feature = "enable-ucx")]
use crate::lamellae::ucx_lamellae::{comm::UcxComm, fabric::UcxAlloc};
use crate::{
    active_messaging::AMCounters,
    lamellae::{
        local_lamellae::comm::{LocalAlloc, LocalComm},
        shmem_lamellae::{comm::ShmemComm, fabric::ShmemAlloc},
        AllocationType, SerializedData,
    },
    memregion::MemregionRdmaInputInner,
    scheduler::Scheduler,
    Deserialize, Serialize,
};

use derive_more::{Add, Into, Sub};
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
    #[cfg(feature = "rofi-c")]
    RofiC(RofiCComm),
    #[cfg(feature = "enable-rofi-rust")]
    RofiRust(RofiRustComm),
    #[cfg(feature = "enable-rofi-rust")]
    RofiRustAsync(RofiRustAsyncComm),
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

#[derive(Clone, Debug)]
pub(crate) enum CommAllocInner {
    Raw(usize, usize), //address, size
    LocalAlloc(Arc<LocalAlloc>),
    ShmemAlloc(Arc<ShmemAlloc>),
    #[cfg(feature = "enable-libfabric")]
    LibfabricAlloc(Arc<LibfabricAlloc>),
    #[cfg(feature = "enable-ucx")]
    UcxAlloc(Arc<UcxAlloc>),
}

impl CommAllocInner {
    pub(crate) fn addr(&self) -> CommAllocAddr {
        match self {
            CommAllocInner::Raw(addr, _) => CommAllocAddr(*addr),
            CommAllocInner::LocalAlloc(inner_alloc) => CommAllocAddr(inner_alloc.start()),
            CommAllocInner::ShmemAlloc(inner_alloc) => CommAllocAddr(inner_alloc.start()),
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => CommAllocAddr(inner_alloc.start()),
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => CommAllocAddr(inner_alloc.start()),
        }
    }
    pub(crate) fn size(&self) -> usize {
        match self {
            CommAllocInner::Raw(_, size) => *size,
            CommAllocInner::LocalAlloc(inner_alloc) => inner_alloc.num_bytes(),
            CommAllocInner::ShmemAlloc(inner_alloc) => inner_alloc.num_bytes(),
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => inner_alloc.num_bytes(),
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => inner_alloc.num_bytes(),
        }
    }
    pub(crate) fn sub_alloc(&self, offset: usize, size: usize) -> CommAllocInner {
        trace!("sub_alloc offset: {} size: {}", offset, size);
        debug_assert!(offset + size <= self.size());
        match self {
            CommAllocInner::Raw(addr, _) => CommAllocInner::Raw(*addr + offset, size),
            CommAllocInner::LocalAlloc(inner_alloc) => CommAllocInner::LocalAlloc(
                inner_alloc
                    .sub_alloc(offset, size)
                    .expect("Invalid sub allocation"),
            ),
            CommAllocInner::ShmemAlloc(inner_alloc) => CommAllocInner::ShmemAlloc(
                inner_alloc
                    .sub_alloc(offset, size)
                    .expect("Invalid sub allocation"),
            ),
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => CommAllocInner::LibfabricAlloc(
                inner_alloc
                    .sub_alloc(offset, size)
                    .expect("Invalid sub allocation"),
            ),
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => CommAllocInner::UcxAlloc(
                inner_alloc
                    .sub_alloc(offset, size)
                    .expect("Invalid sub allocation"),
            ),
        }
    }
    pub(crate) fn contains(&self, addr: &usize) -> bool {
        let my_addr = self.addr().into();
        my_addr <= *addr && *addr < my_addr
    }
}

impl CommAllocRdma for CommAllocInner {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::put(inner_alloc, scheduler, counters, src, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::put(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::put(inner_alloc, scheduler, counters, src, pe, offset)
            }
        }
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::put_unmanaged(inner_alloc, src, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::put_unmanaged(inner_alloc, src, pe, offset)
            }
        }
    }
    fn put_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer(inner_alloc, scheduler, counters, src, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer(inner_alloc, scheduler, counters, src, pe, offset)
            }
        }
    }
    fn put_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer_unmanaged(inner_alloc, src, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer_unmanaged(inner_alloc, src, pe, offset)
            }
        }
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        offset: usize,
    ) -> RdmaHandle<T> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::put_all(inner_alloc, scheduler, counters, src, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_all(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_all(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::put_all(inner_alloc, scheduler, counters, src, offset)
            }
        }
    }
    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize) {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::put_all_unmanaged(inner_alloc, src, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_all_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_all_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::put_all_unmanaged(inner_alloc, src, offset)
            }
        }
    }
    fn put_all_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) -> RdmaHandle<T> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer(inner_alloc, scheduler, counters, src, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer(inner_alloc, scheduler, counters, src, offset)
            }
        }
    }
    fn put_all_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer_unmanaged(inner_alloc, src, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer_unmanaged(inner_alloc, src, offset)
            }
        }
    }

    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::get(inner_alloc, scheduler, counters, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::get(inner_alloc, scheduler, counters, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::get(inner_alloc, scheduler, counters, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::get(inner_alloc, scheduler, counters, pe, offset)
            }
        }
    }

    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
        dst: CommSlice<T>,
    ) -> RdmaHandle<T> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::get_buffer(inner_alloc, scheduler, counters, pe, offset, dst)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::get_buffer(inner_alloc, scheduler, counters, pe, offset, dst)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::get_buffer(inner_alloc, scheduler, counters, pe, offset, dst)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::get_buffer(inner_alloc, scheduler, counters, pe, offset, dst)
            }
        }
    }
}

impl CommAllocAtomic for CommAllocInner {
    fn atomic_op<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                inner_alloc.atomic_op(scheduler, counters, op, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                inner_alloc.atomic_op(scheduler, counters, op, pe, offset)
            }
        }
    }
    fn atomic_op_unmanaged<T: Copy + 'static>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                inner_alloc.atomic_op_unmanaged(op, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_op_unmanaged(op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_op_unmanaged(op, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                inner_alloc.atomic_op_unmanaged(op, pe, offset)
            }
        }
    }
    fn atomic_op_all<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all(scheduler, counters, op, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all(scheduler, counters, op, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all(scheduler, counters, op, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all(scheduler, counters, op, offset)
            }
        }
    }
    fn atomic_op_all_unmanaged<T: Copy + 'static>(&self, op: AtomicOp<T>, offset: usize) {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all_unmanaged(op, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all_unmanaged(op, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all_unmanaged(op, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all_unmanaged(op, offset)
            }
        }
    }
    fn atomic_fetch_op<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicFetchOpHandle<T> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            }
        }
    }
}

// impl Into<CommAllocAddr> for &CommAllocInner {
//     fn into(self) -> CommAllocAddr {
//         match self {
//             CommAllocInner::Raw(addr) => CommAllocAddr(*addr),
//             #[cfg(feature = "enable-libfabric")]
//             CommAllocInner::LibfabricAlloc(info) => CommAllocAddr(info.addr),
//         }
//     }
// }

// impl Into<CommAllocAddr> for CommAllocInner {
//     fn into(self) -> CommAllocAddr {
//         match self {
//             CommAllocInner::Raw(addr) => CommAllocAddr(addr),
//             #[cfg(feature = "enable-libfabric")]
//             CommAllocInner::LibfabricAlloc(info) => CommAllocAddr(info.addr),
//         }
//     }
// }

// impl std::ops::Deref for CommAllocInner {
//     type Target = usize;
//     fn deref(&self) -> &Self::Target {
//         match self {
//             CommAllocInner::Raw(addr) => addr,
//             #[cfg(feature = "enable-libfabric")]
//             CommAllocInner::LibfabricAlloc(info) => &info.addr,
//         }
//     }
// }

#[derive(Clone)]
pub(crate) struct CommAlloc {
    // pub(crate) addr: usize,
    // pub(crate) size: usize,
    pub(crate) inner_alloc: CommAllocInner,
    pub(crate) alloc_type: CommAllocType,
}
impl std::fmt::Debug for CommAlloc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CommAlloc {{ addr: {:x}, size: {:?}, alloc_type: {:?} }}",
            self.inner_alloc.addr(),
            self.num_bytes(),
            self.alloc_type
        )
    }
}

// unsafe impl Send for CommAlloc {}
// unsafe impl Sync for CommAlloc {}

impl CommAlloc {
    pub(crate) fn byte_add(&self, offset: usize) -> CommAllocAddr {
        debug_assert!(offset < self.num_bytes());
        self.inner_alloc.addr() + offset
    }
    #[tracing::instrument(skip(self), level = "debug")]
    pub(crate) fn as_comm_slice<T>(&self) -> CommSlice<T> {
        CommSlice {
            inner_alloc: self.inner_alloc.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
    #[tracing::instrument(level = "debug")]
    pub(crate) fn comm_slice_at_byte_offset<T>(
        &self,
        offset: usize,
        num_elems: usize,
    ) -> CommSlice<T> {
        trace!(
            "{:?} offset: {}  num_elems: {} bytes: {}",
            self,
            offset,
            num_elems,
            num_elems * std::mem::size_of::<T>()
        );
        debug_assert!(
            offset < self.num_bytes()
                && offset + num_elems * std::mem::size_of::<T>() <= self.num_bytes()
        );
        CommSlice {
            inner_alloc: self
                .inner_alloc
                .sub_alloc(offset, num_elems * std::mem::size_of::<T>()),
            _phantom: std::marker::PhantomData,
        }
    }

    pub(crate) unsafe fn as_ptr<T>(&self) -> *const T {
        self.inner_alloc.addr().as_ptr::<T>()
    }
    pub(crate) unsafe fn as_mut_ptr<T>(&self) -> *mut T {
        self.inner_alloc.addr().as_mut_ptr::<T>()
    }
    pub(crate) unsafe fn as_ref<T>(&self) -> Option<&T> {
        self.as_ptr::<T>().as_ref()
    }

    // pub(crate) unsafe fn as_mut<T>(&self) -> Option<&mut T> {
    //     self.as_mut_ptr::<T>().as_mut()
    // }
    pub(crate) fn comm_addr(&self) -> CommAllocAddr {
        self.inner_alloc.addr()
    }
    pub(crate) fn num_bytes(&self) -> usize {
        self.inner_alloc.size()
    }
    // pub(crate) fn contains(&self, addr: &usize) -> bool {
    //     self.inner_alloc.contains(addr)
    // }
    // pub(crate) fn calc_offset(&self, addr: &usize) -> CommAllocAddr {
    //     CommAllocAddr(*addr - *self.inner_alloc.addr())
    // }
}

impl CommAllocRdma for CommAlloc {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        self.inner_alloc.put(scheduler, counters, src, pe, offset)
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        self.inner_alloc.put_unmanaged(src, pe, offset)
    }
    fn put_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        self.inner_alloc
            .put_buffer(scheduler, counters, src, pe, offset)
    }
    fn put_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) {
        self.inner_alloc.put_buffer_unmanaged(src, pe, offset)
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        offset: usize,
    ) -> RdmaHandle<T> {
        self.inner_alloc.put_all(scheduler, counters, src, offset)
    }
    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize) {
        self.inner_alloc.put_all_unmanaged(src, offset)
    }
    fn put_all_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) -> RdmaHandle<T> {
        self.inner_alloc
            .put_all_buffer(scheduler, counters, src, offset)
    }
    fn put_all_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) {
        self.inner_alloc.put_all_buffer_unmanaged(src, offset)
    }
    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
        dst: CommSlice<T>,
    ) -> RdmaHandle<T> {
        self.inner_alloc
            .get_buffer(scheduler, counters, pe, offset, dst)
    }
    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        self.inner_alloc.get(scheduler, counters, pe, offset)
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

#[derive(Copy, Clone, Add, Sub, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct CommAllocAddr(pub(crate) usize);

impl Into<usize> for &CommAllocAddr {
    fn into(self) -> usize {
        self.0
    }
}

impl Into<usize> for CommAllocAddr {
    fn into(self) -> usize {
        self.0
    }
}

impl Into<CommAllocAddr> for usize {
    fn into(self) -> CommAllocAddr {
        CommAllocAddr(self)
    }
}

impl std::ops::Deref for CommAllocAddr {
    type Target = usize;
    fn deref(&self) -> &Self::Target {
        &self.0
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

    // pub(crate) unsafe fn as_mut<T>(&self) -> Option<&mut T> {
    //     self.as_mut_ptr::<T>().as_mut()
    // }
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

impl std::ops::Sub<usize> for CommAllocAddr {
    type Output = CommAllocAddr;
    fn sub(self, rhs: usize) -> Self::Output {
        CommAllocAddr(self.0 - rhs)
    }
}
impl std::ops::Sub<usize> for &CommAllocAddr {
    type Output = CommAllocAddr;
    fn sub(self, rhs: usize) -> Self::Output {
        CommAllocAddr(self.0 - rhs)
    }
}

impl std::convert::AsRef<CommAllocAddr> for CommAllocAddr {
    fn as_ref(&self) -> &CommAllocAddr {
        self
    }
}

impl std::convert::AsRef<usize> for CommAllocAddr {
    fn as_ref(&self) -> &usize {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CommSlice<T> {
    pub(crate) inner_alloc: CommAllocInner,
    _phantom: std::marker::PhantomData<T>,
}

// unsafe impl<T> Send for CommSlice<T> {}
// unsafe impl<T> Sync for CommSlice<T> {}

impl<T> CommSlice<T> {
    pub(crate) fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }
    // pub(crate) fn as_mut_slice(&mut self) -> &mut [T] {
    //     unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len()) }
    // }
    pub(crate) unsafe fn as_casted_slice<R>(&self) -> Option<&[R]> {
        let len = self.len() * std::mem::size_of::<T>() / std::mem::size_of::<R>();
        if len * std::mem::size_of::<R>() != self.len() * std::mem::size_of::<T>() {
            return None; // size mismatch
        }
        let ptr = self.as_mut_ptr() as *mut R;
        Some(std::slice::from_raw_parts_mut(ptr, len))
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
        trace!(
            "subslice start: {} end: {} new size: {} ({} {})",
            start,
            end,
            end - start,
            start * std::mem::size_of::<T>(),
            (end - start) * std::mem::size_of::<T>()
        );
        CommSlice {
            inner_alloc: self.inner_alloc.sub_alloc(
                start * std::mem::size_of::<T>(),
                (end - start) * std::mem::size_of::<T>(),
            ),
            _phantom: std::marker::PhantomData,
        }
    }
    pub(crate) fn as_mut_ptr(&self) -> *mut T {
        unsafe { self.inner_alloc.addr().as_mut_ptr() }
    }
    pub(crate) fn as_ptr(&self) -> *const T {
        unsafe { self.inner_alloc.addr().as_ptr() }
    }
    pub(crate) fn len(&self) -> usize {
        self.inner_alloc.size() / std::mem::size_of::<T>()
    }
    pub(crate) fn usize_addr(&self) -> usize {
        self.inner_alloc.addr().into()
    }

    pub(crate) fn index_addr(&self, index: usize) -> CommAllocAddr {
        debug_assert!(index < self.inner_alloc.size());
        self.inner_alloc.addr() + index * std::mem::size_of::<T>()
    }

    pub(crate) unsafe fn from_raw_parts(data: *const T, len: usize) -> Self {
        CommSlice {
            inner_alloc: CommAllocInner::Raw(data as usize, len * std::mem::size_of::<T>()),
            _phantom: std::marker::PhantomData,
        }
    }

    pub(crate) unsafe fn from_slice(slice: &[T]) -> Self {
        CommSlice::from_raw_parts(slice.as_ptr(), slice.len())
    }

    pub(crate) fn contains(&self, addr: &usize) -> bool {
        self.inner_alloc.contains(&addr)
    }

    // pub(crate) fn num_bytes(&self) -> usize {
    //     self.info.size() * std::mem::size_of::<T>()
    // }
}

impl<T> CommAllocRdma for CommSlice<T> {
    fn put<U: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: U,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<U> {
        self.inner_alloc.put(
            scheduler,
            counters,
            src,
            pe,
            offset * std::mem::size_of::<U>(),
        )
    }
    fn put_unmanaged<U: Remote>(&self, src: U, pe: usize, offset: usize) {
        self.inner_alloc
            .put_unmanaged(src, pe, offset * std::mem::size_of::<U>())
    }
    fn put_buffer<U: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<U>>,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<U> {
        self.inner_alloc.put_buffer(
            scheduler,
            counters,
            src,
            pe,
            offset * std::mem::size_of::<U>(),
        )
    }
    fn put_buffer_unmanaged<U: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<U>>,
        pe: usize,
        offset: usize,
    ) {
        self.inner_alloc
            .put_buffer_unmanaged(src, pe, offset * std::mem::size_of::<U>())
    }

    fn put_all<U: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: U,
        offset: usize,
    ) -> RdmaHandle<U> {
        self.inner_alloc
            .put_all(scheduler, counters, src, offset * std::mem::size_of::<U>())
    }
    fn put_all_unmanaged<U: Remote>(&self, src: U, offset: usize) {
        self.inner_alloc
            .put_all_unmanaged(src, offset * std::mem::size_of::<U>())
    }
    fn put_all_buffer<U: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<U>>,
        offset: usize,
    ) -> RdmaHandle<U> {
        self.inner_alloc
            .put_all_buffer(scheduler, counters, src, offset * std::mem::size_of::<U>())
    }
    fn put_all_buffer_unmanaged<U: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<U>>,
        offset: usize,
    ) {
        self.inner_alloc
            .put_all_buffer_unmanaged(src, offset * std::mem::size_of::<U>())
    }
    fn get<U: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<U> {
        self.inner_alloc
            .get(scheduler, counters, pe, offset * std::mem::size_of::<U>())
    }
    fn get_buffer<U: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
        dst: CommSlice<U>,
    ) -> RdmaHandle<U> {
        self.inner_alloc.get_buffer(
            scheduler,
            counters,
            pe,
            offset * std::mem::size_of::<U>(),
            dst,
        )
    }
}

impl<T> std::ops::Deref for CommSlice<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }
}

impl<T> std::ops::DerefMut for CommSlice<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len()) }
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

    // TODO: we probably want the CommAlloc to handle freeing on Drop... do we have the appropriate lifetime info for this?
    fn free(&self, alloc: CommAlloc);
    fn rt_alloc(&self, size: usize, align: usize) -> error::AllocResult<CommAlloc>;
    fn rt_check_alloc(&self, size: usize, align: usize) -> bool;
    // TODO: we probably want the CommAlloc to handle freeing on Drop... do we have the appropriate lifetime info for this?
    fn rt_free(&self, alloc: CommAlloc);
    fn mem_occupied(&self) -> usize;
    fn alloc_pool(&self, min_size: usize);
    fn num_pool_allocs(&self) -> usize;
    fn print_pools(&self);
    // this translates a remote address to a local address
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> CommAllocAddr;
    // this translates a remote address to its local allocation + offset within that allocation
    fn local_alloc_and_offset_from_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
    ) -> (CommAlloc, usize);
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

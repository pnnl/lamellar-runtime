pub(crate) mod atomic;
pub(crate) mod error;
pub(crate) mod rdma;

use super::Backend;
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
use crate::lamellae::{shmem_lamellae::comm::ShmemComm, AllocationType, SerializedData};
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

// #[enum_dispatch(LamellaeRDMA, CommOps)]
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
}

impl Comm {
    pub(crate) fn new_serialized_data(
        self: &Arc<Comm>,
        size: usize,
    ) -> Result<SerializedData, anyhow::Error> {
        match self.as_ref() {
            #[cfg(feature = "rofi")]
            Comm::Rofi(_) => Ok(RofiData::new(self.clone(), size)?.into()),
            #[cfg(feature = "enable-rofi-rust")]
            Comm::RofiRust(_) => Ok(RofiRustData::new(self.clone(), size)?.into()),
            #[cfg(feature = "enable-rofi-rust")]
            Comm::RofiRustAsync(_) => Ok(RofiRustAsyncData::new(self.clone(), size)?.into()),
            #[cfg(feature = "enable-libfabric")]
            Comm::LibFab(_) => Ok(LibFabData::new(self.clone(), size)?.into()),
            #[cfg(feature = "enable-libfabric")]
            Comm::LibFabAsync(_) => Ok(LibFabAsyncData::new(self.clone(), size)?.into()),
            Comm::Shmem(_) => Ok(ShmemData::new(self.clone(), size)?.into()),
        }
    }
}

pub(crate) trait CommShutdown {
    fn force_shutdown(&self);
}

pub(crate) trait CommMem {
    fn alloc(&self, size: usize, alloc: AllocationType, align: usize) -> error::AllocResult<usize>;
    fn free(&self, addr: usize);
    fn rt_alloc(&self, size: usize, align: usize) -> error::AllocResult<usize>;
    fn rt_check_alloc(&self, size: usize, align: usize) -> bool;
    fn rt_free(&self, addr: usize);
    fn mem_occupied(&self) -> usize;
    fn alloc_pool(&self, min_size: usize);
    fn num_pool_allocs(&self) -> usize;
    fn print_pools(&self);
    fn base_addr(&self) -> usize;
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize;
    fn remote_addr(&self, remote_pe: usize, local_addr: usize) -> usize;
}

pub(crate) trait CommProgress {
    fn flush(&self);
    fn wait(&self);
    fn barrier(&self);
}

pub(crate) trait CommInfo {
    fn my_pe(&self) -> usize;
    fn num_pes(&self) -> usize;
    fn backend(&self) -> Backend;
}

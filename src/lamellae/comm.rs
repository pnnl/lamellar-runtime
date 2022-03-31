#[cfg(feature = "enable-rofi")]
use crate::lamellae::rofi::rofi_comm::*;
use crate::lamellae::shmem::shmem_comm::*;
use crate::lamellae::{AllocationType, SerializedData};
// use crate::lamellae::shmem::ShmemComm;

use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub enum AllocError {
    OutOfMemoryError(usize),
    IdError(usize),
}

impl std::fmt::Display for AllocError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            AllocError::OutOfMemoryError(size) => {
                write!(f, "not enough memory for to allocate {} bytes", size)
            }
            AllocError::IdError(pe) => {
                write!(f, "pe {} must be part of team of sub allocation", pe)
            }
        }
    }
}

impl std::error::Error for AllocError {}

pub(crate) type AllocResult<T> = Result<T, AllocError>;

#[cfg(feature = "enable-rofi")]
#[derive(Debug, Clone, Copy)]
pub enum TxError {
    GetError,
}
#[cfg(feature = "enable-rofi")]
impl std::fmt::Display for TxError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TxError::GetError => {
                write!(f, "error performing get")
            }
        }
    }
}
#[cfg(feature = "enable-rofi")]
impl std::error::Error for TxError {}
#[cfg(feature = "enable-rofi")]
pub(crate) type TxResult<T> = Result<T, TxError>;

pub(crate) trait Remote: Copy + Send + Sync {}
impl<T: Copy + Send + Sync> Remote for T {}

#[enum_dispatch(CommOps)]
pub(crate) enum Comm {
    #[cfg(feature = "enable-rofi")]
    Rofi(RofiComm),
    Shmem(ShmemComm),
}

impl Comm {
    pub(crate) fn new_serialized_data(
        self: &Arc<Comm>,
        size: usize,
    ) -> Result<SerializedData, anyhow::Error> {
        match self.as_ref() {
            #[cfg(feature = "enable-rofi")]
            Comm::Rofi(_) => Ok(RofiData::new(self.clone(), size)?.into()),
            Comm::Shmem(_) => Ok(ShmemData::new(self.clone(), size)?.into()),
        }
    }
}

#[async_trait]
#[enum_dispatch]
pub(crate) trait CommOps {
    fn my_pe(&self) -> usize;
    fn num_pes(&self) -> usize;
    fn barrier(&self);
    fn occupied(&self) -> usize;
    fn num_pool_allocs(&self) -> usize;
    fn print_pools(&self);
    fn alloc_pool(&self, min_size: usize);
    fn rt_alloc(&self, size: usize) -> AllocResult<usize>;
    fn rt_check_alloc(&self, size: usize) -> bool;
    fn rt_free(&self, addr: usize);
    fn alloc(&self, size: usize, alloc: AllocationType) -> AllocResult<usize>;
    fn free(&self, addr: usize);
    fn base_addr(&self) -> usize;
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize;
    fn remote_addr(&self, pe: usize, local_addr: usize) -> usize;
    fn put<T: Remote>(&self, pe: usize, src_addr: &[T], dst_addr: usize);
    fn iput<T: Remote>(&self, pe: usize, src_addr: &[T], dst_addr: usize);
    fn put_all<T: Remote>(&self, src_addr: &[T], dst_addr: usize);
    fn get<T: Remote>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]);
    fn iget<T: Remote>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]);
    fn iget_relative<T: Remote>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]);
    #[allow(non_snake_case)]
    fn MB_sent(&self) -> f64;
}

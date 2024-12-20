#[cfg(feature = "rofi")]
use crate::lamellae::rofi::rofi_comm::*;
use crate::lamellae::shmem::shmem_comm::*;
use crate::lamellae::{AllocationType, SerializedData};
// use crate::lamellae::shmem::ShmemComm;

use async_trait::async_trait;
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

// impl Into<u8> for CmdQStatus {
//     fn into(self) -> u8 {
//         self as u8
//     }
// }
// const ACTIVE: u8 = 0;
// const FINISHED: u8 = 1;
// const SHUTING_DOWN: u8 = 2;
// const PANIC: u8 = 3;

#[derive(Debug, Clone, Copy)]
pub(crate) enum AllocError {
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

#[cfg(feature = "rofi")]
#[derive(Debug, Clone, Copy)]
pub(crate) enum TxError {
    GetError,
}
#[cfg(feature = "rofi")]
impl std::fmt::Display for TxError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TxError::GetError => {
                write!(f, "error performing get")
            }
        }
    }
}
#[cfg(feature = "rofi")]
impl std::error::Error for TxError {}
#[cfg(feature = "rofi")]
pub(crate) type TxResult<T> = Result<T, TxError>;

pub(crate) trait Remote: Copy {}
impl<T: Copy> Remote for T {}

#[enum_dispatch(CommOps)]
#[derive(Debug)]
pub(crate) enum Comm {
    #[cfg(feature = "rofi")]
    Rofi(RofiComm),
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
    fn rt_alloc(&self, size: usize, align: usize) -> AllocResult<usize>;
    fn rt_check_alloc(&self, size: usize, align: usize) -> bool;
    fn rt_free(&self, addr: usize);
    fn alloc(&self, size: usize, alloc: AllocationType) -> AllocResult<usize>;
    fn free(&self, addr: usize);
    fn base_addr(&self) -> usize;
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize;
    fn remote_addr(&self, pe: usize, local_addr: usize) -> usize;
    fn flush(&self);
    fn put<T: Remote>(&self, pe: usize, src_addr: &[T], dst_addr: usize);
    fn iput<T: Remote>(&self, pe: usize, src_addr: &[T], dst_addr: usize);
    fn put_all<T: Remote>(&self, src_addr: &[T], dst_addr: usize);
    fn get<T: Remote>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]);
    fn iget<T: Remote>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]);
    // fn iget_relative<T: Remote>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]);
    #[allow(non_snake_case)]
    #[allow(dead_code)]
    fn MB_sent(&self) -> f64;
    fn force_shutdown(&self);
}

#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabric_rs::libfabric_sync::libfabric_comm::*;
#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabric_rs::libfabric_async::libfabric_async_comm::*;
#[cfg(feature = "enable-rofi")]
use crate::lamellae::rofi::rofi_comm::*;
#[cfg(feature = "enable-rofi-rust")]
use crate::lamellae::rofi_rust::rofi_rust_comm::*;
#[cfg(feature = "enable-rofi-rust")]
use crate::lamellae::rofi_rust_async::rofi_rust_async_comm::*;
use crate::lamellae::shmem::shmem_comm::*;
#[cfg(feature = "enable-libfabric")]
use crate::lamellae::LibFabAsyncData;
#[cfg(feature = "enable-rofi-rust")]
use crate::lamellae::RofiRustAsyncData;
#[cfg(feature = "enable-rofi-rust")]
use crate::lamellae::RofiRustData;
use crate::lamellae::Scheduler;
use crate::lamellae::{AllocationType, SerializedData};
// use crate::lamellae::shmem::ShmemComm;
use futures_util::{ready, Future};
use std::task::{Context, Poll};
use std::pin::Pin;
#[cfg(feature="tokio-executor")]
use tokio::runtime::Handle;
#[cfg(not(feature="tokio-executor"))]
use async_std::task::block_on;
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

#[cfg(feature = "enable-rofi")]
#[derive(Debug, Clone, Copy)]
pub(crate) enum TxError {
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

pub(crate) trait Remote: Copy {}
impl<T: Copy> Remote for T {}

#[enum_dispatch(CommOps)]
#[derive(Debug)]
pub(crate) enum Comm {
    #[cfg(feature = "enable-rofi")]
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
            #[cfg(feature = "enable-rofi")]
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

    pub(crate) fn set_scheduler(&self, scheduler: Arc<Scheduler>) {
        match self {
            #[cfg(feature = "enable-rofi")]
            Comm::Rofi(comm) => comm.set_scheduler(scheduler),
            #[cfg(feature = "enable-rofi-rust")]
            Comm::RofiRust(comm) => comm.set_scheduler(scheduler),
            #[cfg(feature = "enable-rofi-rust")]
            Comm::RofiRustAsync(comm) => comm.set_scheduler(scheduler),
            #[cfg(feature = "enable-libfabric")]
            Comm::LibFab(comm) => comm.set_scheduler(scheduler),
            #[cfg(feature = "enable-libfabric")]
            Comm::LibFabAsync(comm) => comm.set_scheduler(scheduler),
            Comm::Shmem(comm) => comm.set_scheduler(scheduler),
        }
    }
}

#[enum_dispatch]
pub(crate) trait CommOps {
    fn my_pe(&self) -> usize;
    fn num_pes(&self) -> usize;
    #[must_use="You must .block() or .await the returned handle in order for this function to execute"]
    fn barrier<'a>(&'a self) -> CommOpHandle<'a>;
    fn occupied(&self) -> usize;
    fn num_pool_allocs(&self) -> usize;
    fn print_pools(&self);
    #[must_use="You must .block() or .await the returned handle in order for this function to execute"]
    fn alloc_pool<'a>(&'a self, min_size: usize) -> CommOpHandle<'a>;
    fn rt_alloc(&self, size: usize, align: usize) -> AllocResult<usize>;
    fn rt_check_alloc(&self, size: usize, align: usize) -> bool;
    fn rt_free(&self, addr: usize);
    #[must_use="You must .block() or .await the returned handle in order for this function to execute"]
    fn alloc<'a>(&'a self, size: usize, alloc: AllocationType) -> CommOpHandle<'a, AllocResult<usize>>;
    fn free(&self, addr: usize);
    fn base_addr(&self) -> usize;
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize;
    fn remote_addr(&self, pe: usize, local_addr: usize) -> usize;
    fn flush(&self);
    #[must_use="You must .block() or .await the returned handle in order for this function to execute"]
    fn put<'a, T: Remote + Sync>(&'a self, pe: usize, src_addr: &'a [T], dst_addr: usize) -> CommOpHandle<'a> ;
    fn iput<T: Remote>(&self, pe: usize, src_addr: &[T], dst_addr: usize);
    #[must_use="You must .block() or .await the returned handle in order for this function to execute"]
    fn put_all<'a, T: Remote + Sync>(&'a self, src_addr: &'a [T], dst_addr: usize)-> CommOpHandle<'a>;
    #[must_use="You must .block() or .await the returned handle in order for this function to execute"]
    fn get<'a, T: Remote + Sync + Send>(&'a self, pe: usize, src_addr: usize, dst_addr: &'a mut [T]) -> CommOpHandle<'a>;
    fn iget<'a, T: Remote + Sync + Send>(&'a self, pe: usize, src_addr: usize, dst_addr: &'a mut [T]);
    // fn iget_relative<T: Remote>(& self, pe: usize, src_addr: usize, dst_addr: &mut [T]);
    #[allow(non_snake_case)]
    fn MB_sent(&self) -> f64;
    fn force_shutdown(&self);
}

pub struct CommOpHandle<'a, T = ()> {
    fut: Pin<Box<dyn Future<Output =T> + Send + 'a> >,
    scheduler: Arc<crate::lamellae::Scheduler>,
}

impl<'a, T> CommOpHandle<'a, T> {
    pub(crate) fn new(fut: impl Future<Output =T> + Send + 'a, scheduler: Arc<crate::lamellae::Scheduler>) -> Self {
        Self {
            fut: Box::pin(fut),
            scheduler: scheduler.clone(),
        }
    }

    pub fn block(self) -> T {
        // #[cfg(feature="tokio-executor")]
        // return Handle::current().block_on(async {self.fut.await});
        // #[cfg(not(feature="tokio-executor"))]
        // return block_on(async {self.fut.await});
        self.scheduler.block_on(async {self.fut.await})
    }
    
    // pub(crate) fn spawn(self) -> async_std::task::JoinHandle<T> {
    //     async_std::task::spawn(async {self.fut.await})
    // }


}

impl<'a, T> Future for CommOpHandle<'a, T> {
        type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let guard = ready!(this.fut.as_mut().poll(cx));
        Poll::Ready(guard)
    }
}
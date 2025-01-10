use super::{CommSlice,CommAllocAddr,error::RdmaResult};
use crate::{LamellarMemoryRegion,lamellae::{local_lamellae::rdma::LocalFuture, shmem_lamellae::rdma::ShmemFuture}};

use enum_dispatch::enum_dispatch;
use pin_project::pin_project;
use futures_util::{Future, FutureExt};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

pub(crate) trait Remote: Copy {}
impl<T: Copy> Remote for T {}

#[pin_project(project = RdmaFutureProj)]
pub(crate) enum RdmaFuture {
    #[cfg(feature = "rofi")]
    Rofi(#[pin] RofiFuture),
    #[cfg(feature = "enable-rofi-rust")]
    RofiRust(#[pin] RofiRustFuture),
    #[cfg(feature = "enable-rofi-rust")]
    RofiRustAsync(#[pin] RofiRustAsyncFuture),
    #[cfg(feature = "enable-libfabric")]
    LibFab(#[pin] LibFabFuture),
    #[cfg(feature = "enable-libfabric")]
    LibFabAsync(#[pin] LibFabAsyncFuture),
    Shmem(#[pin] ShmemFuture),
    Local(#[pin] LocalFuture),
}

impl Future for RdmaFuture {
    type Output = RdmaResult;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            #[cfg(feature = "rofi")]
            RdmaFutureProj::Rofi(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaFutureProj::RofiRust(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaFutureProj::RofiRustAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            RdmaFutureProj::LibFab(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            RdmaFutureProj::LibFabAsync(f) => f.poll(cx),
            RdmaFutureProj::Shmem(f) => f.poll(cx),
            RdmaFutureProj::Local(f) => f.poll(cx),
        }
    }
}

#[enum_dispatch]
pub(crate) trait CommRdma {
    fn put<T: Remote>(&self, pe: usize, src: CommSlice<T>, dst: CommAllocAddr) -> RdmaFuture;
    fn put_all<T: Remote>(&self, src: CommSlice<T>, dst: CommAllocAddr) -> RdmaFuture;
    fn get<T: Remote>(&self, pe: usize, src: CommAllocAddr, dst: CommSlice<T>) -> RdmaFuture;
}

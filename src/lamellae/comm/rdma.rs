use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::Future;

use crate::lamellae::{local_lamellae::rdma::LocalFuture, shmem_lamellae::rdma::ShmemFuture};

use super::error::RdmaResult;

pub(crate) trait Remote: Copy {}
impl<T: Copy> Remote for T {}

pub(crate) enum RdmaFuture {
    #[cfg(feature = "rofi")]
    Rofi(RofiFuture),
    #[cfg(feature = "enable-rofi-rust")]
    RofiRust(RofiRustFuture),
    #[cfg(feature = "enable-rofi-rust")]
    RofiRustAsync(RofiRustAsyncFuture),
    #[cfg(feature = "enable-libfabric")]
    LibFab(LibFabFuture),
    #[cfg(feature = "enable-libfabric")]
    LibFabAsync(LibFabAsyncFuture),
    Shmem(ShmemFuture),
    Local(LocalFuture),
}

impl Future for RdmaFuture {
    type Output = RdmaResult;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            #[cfg(feature = "rofi")]
            RdmaFuture::Rofi(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaFuture::RofiRust(f) => f.poll(cx),
            #[cfg(feature = "enable-rofi-rust")]
            RdmaFuture::RofiRustAsync(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            RdmaFuture::LibFab(f) => f.poll(cx),
            #[cfg(feature = "enable-libfabric")]
            RdmaFuture::LibFabAsync(f) => f.poll(cx),
            RdmaFuture::Shmem(f) => f.poll(cx),
            RdmaFuture::Local(f) => f.poll(cx),
        }
    }
}

pub(crate) trait CommRdma {
    fn put<T: Remote>(&self, pe: usize, src: &[T], dst: usize) -> RdmaFuture;
    fn put_all<T: Remote>(&self, src: &[T], dst: usize) -> RdmaFuture;
    fn get<T: Remote>(&self, pe: usize, src: usize, dst: &mut [T]) -> RdmaFuture;
}

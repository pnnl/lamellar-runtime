use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::Future;

use crate::lamellae::{comm::{
    error::RdmaResult,
    rdma::{CommRdma, RdmaFuture, Remote},
}, CommAllocAddr, CommSlice};

use super::comm::ShmemComm;

pub(crate) struct ShmemFuture {}

impl From<ShmemFuture> for RdmaFuture{
    fn from(f: ShmemFuture) -> RdmaFuture {
        RdmaFuture::Shmem(f)
    }
}

impl Future for ShmemFuture {
    type Output = RdmaResult;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(()))
    }
}

impl CommRdma for ShmemComm {
    fn put<T: Remote>(&self, pe: usize, src: CommSlice<T>, remote_addr: CommAllocAddr) -> RdmaFuture {
        let alloc = self.alloc_lock.read();
        for (addr, (shmem, size, addrs)) in alloc.0.iter() {
            if shmem.contains(remote_addr.0) {
                let real_dst_base = shmem.base_addr() + size * addrs[&pe].1;
                let real_dst_addr = real_dst_base + (remote_addr.0 - addr);
                unsafe {
                    std::ptr::copy_nonoverlapping(src.as_ptr(), real_dst_addr as *mut T, src.len());
                }
                break;
            }
        }
        ShmemFuture {}.into()
    }
    fn put_all<T: Remote>(&self, src: CommSlice<T>, dst: CommAllocAddr) -> RdmaFuture {
        // self.put_all(src, dst)
        ShmemFuture {}.into()
    }
    fn get<T: Remote>(&self, pe: usize, src: CommAllocAddr, dst: CommSlice<T>) -> RdmaFuture {
        // self.get(pe, src, dst)
        ShmemFuture {}.into()
    }
}

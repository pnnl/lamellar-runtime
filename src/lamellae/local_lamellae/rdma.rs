use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::Future;

use crate::{LamellarMemoryRegion,lamellae::comm::{
    error::RdmaResult,
    rdma::{CommRdma, RdmaFuture, Remote},
    CommAllocAddr, CommSlice
}};

use super::comm::LocalComm;

pub(crate) struct LocalFuture {}


impl Future for LocalFuture {
    type Output = RdmaResult;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(()))
    }
}

impl From<LocalFuture> for RdmaFuture{
    fn from(f: LocalFuture) -> RdmaFuture {
        RdmaFuture::Local(f)
    }
}

impl CommRdma for LocalComm {
    fn put<T: Remote>(&self, pe: usize, src: CommSlice<T>, dst: CommAllocAddr) -> RdmaFuture {
        let src_addr = src.addr();
        let dst = dst.into();
            if !((src_addr <= dst
            && dst < src_addr + src.len()) //dst start overlaps src
            || (src_addr <= dst + src.len()
            && dst + src.len() < src_addr + src.len()))
            //dst end overlaps src
            {
                unsafe {
                    std::ptr::copy_nonoverlapping(src.as_ptr(), dst as *mut T, src.len());
                }
            } else {
                unsafe {
                    std::ptr::copy(src.as_ptr(), dst as *mut T, src.len());
                }
            }
        LocalFuture {}.into()
    }
    fn put_all<T: Remote>(&self, src: CommSlice<T>, dst: CommAllocAddr) -> RdmaFuture {
        let src_addr = src.addr();
        let dst = dst.into();
        if !((src_addr<= dst
            && dst < src_addr+ src.len()) //dst start overlaps src
            || (src_addr <= dst + src.len()
            && dst + src.len() < src_addr + src.len()))
        //dst end overlaps src
        {
                unsafe {
                    std::ptr::copy_nonoverlapping(src.as_ptr(), dst as *mut T, src.len());
                }
        } else {
                unsafe {
                    std::ptr::copy(src.as_ptr(), dst as *mut T, src.len());
                }
        }
        LocalFuture {}.into()
    }
    fn get<T: Remote>(&self, pe: usize, src: CommAllocAddr, dst: CommSlice<T>) -> RdmaFuture {
        let dst_addr = dst.addr();
        let src = src.into();
        if !((dst_addr <= src && src <dst_addr + dst.len())
            || (dst_addr <= src + dst.len()
                && src + dst.len() < dst_addr + dst.len()))
        {
            unsafe {
                std::ptr::copy_nonoverlapping(src as *mut T, dst.as_mut_ptr(), dst.len());
            }
        } else {
            unsafe {
                std::ptr::copy(src as *mut T, dst.as_mut_ptr(), dst.len());
            }
        }
        LocalFuture {}.into()
    }
}

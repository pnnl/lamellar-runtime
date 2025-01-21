use std::{
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    task::{Context, Poll},
};

use futures_util::Future;

use crate::{
    active_messaging::AMCounters,
    lamellae::{
        comm::rdma::{CommRdma, RdmaHandle, Remote},
        CommAllocAddr, CommSlice,
    },
    LamellarTask,
};

use super::{comm::ShmemComm, Scheduler};

pub(super) enum Op<T> {
    Put(usize, CommSlice<T>, CommAllocAddr),
    PutAll(CommSlice<T>, Vec<(usize, CommAllocAddr)>),
    Get(usize, CommAllocAddr, CommSlice<T>),
    Atomic,
}
pub(crate) struct ShmemFuture<T> {
    pub(crate) op: Op<T>,
}

impl<T: Remote> ShmemFuture<T> {
    fn inner_put(&self, pe: usize, src: &CommSlice<T>, dst: &CommAllocAddr) {
        if !(src.contains(dst) || src.contains(dst + src.len())) {
            unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), src.len()) };
        } else {
            unsafe {
                std::ptr::copy(src.as_ptr(), dst.as_mut_ptr(), src.len());
            }
        }
    }
    fn inner_put_all(&self, src: &CommSlice<T>, dsts: &Vec<(usize, CommAllocAddr)>) {
        for (pe, dst) in dsts {
            self.inner_put(*pe, &src, dst);
        }
    }
    fn inner_get(&self, pe: usize, src: &CommAllocAddr, dst: &CommSlice<T>) {
        if !(dst.contains(src) || dst.contains(src + dst.len())) {
            unsafe {
                std::ptr::copy_nonoverlapping(src.as_mut_ptr(), dst.as_mut_ptr(), dst.len());
            }
        } else {
            unsafe {
                std::ptr::copy(src.as_mut_ptr(), dst.as_mut_ptr(), dst.len());
            }
        }
    }
    fn exec_op(&self) {
        match &self.op {
            Op::Put(pe, src, dst) => {
                self.inner_put(*pe, src, dst);
            }
            Op::PutAll(src, dst) => {
                self.inner_put_all(src, dst);
            }
            Op::Get(pe, src, dst) => {
                self.inner_get(*pe, src, dst);
            }
            Op::Atomic => {}
        }
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        // Ok(())
    }
    pub(crate) fn spawn(
        mut self,
        scheduler: &Arc<Scheduler>,
        outstanding_reqs: Vec<Arc<AMCounters>>,
    ) -> LamellarTask<()> {
        scheduler.spawn_task(self, outstanding_reqs)
    }
}

impl<T: Remote> From<ShmemFuture<T>> for RdmaHandle<T> {
    fn from(f: ShmemFuture<T>) -> RdmaHandle<T> {
        RdmaHandle::Shmem(f)
    }
}

impl<T: Remote> Future for ShmemFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.exec_op();
        Poll::Ready(())
    }
}

impl CommRdma for ShmemComm {
    fn put<T: Remote>(
        &self,
        pe: usize,
        src: CommSlice<T>,
        remote_addr: CommAllocAddr,
    ) -> RdmaHandle<T> {
        self.put_amt
            .fetch_add(src.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
        let alloc = self.alloc_lock.read();
        for (addr, (shmem, size, addrs)) in alloc.0.iter() {
            if shmem.contains(remote_addr.0) {
                let real_dst_base = shmem.base_addr() + size * addrs[&pe].1;
                let real_dst_addr = real_dst_base + (remote_addr.0 - addr);
                return ShmemFuture {
                    op: Op::Put(pe, src, real_dst_addr),
                }
                .into();
            }
        }
        panic!("shmem segment invalid for put");
    }
    fn put_all<T: Remote>(&self, src: CommSlice<T>, remote_addr: CommAllocAddr) -> RdmaHandle<T> {
        self.put_amt.fetch_add(
            src.len() * std::mem::size_of::<T>() * self.num_pes,
            Ordering::SeqCst,
        );
        let alloc = self.alloc_lock.read();
        for (addr, (shmem, size, addrs)) in alloc.0.iter() {
            if shmem.contains(remote_addr.0) {
                let pe_addrs = (0..self.num_pes)
                    .map(|pe| {
                        let real_dst_base = shmem.base_addr() + size * addrs[&pe].1;
                        let real_dst_addr = real_dst_base + (remote_addr.0 - addr);
                        (pe, real_dst_addr)
                    })
                    .collect::<Vec<_>>();
                return ShmemFuture {
                    op: Op::PutAll(src, pe_addrs),
                }
                .into();
            }
        }
        panic!("shmem segment invalid for put all");
    }
    fn get<T: Remote>(
        &self,
        pe: usize,
        src_addr: CommAllocAddr,
        dst: CommSlice<T>,
    ) -> RdmaHandle<T> {
        self.get_amt
            .fetch_add(dst.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
        let alloc = self.alloc_lock.read();
        for (addr, (shmem, size, addrs)) in alloc.0.iter() {
            if shmem.contains(src_addr.0) {
                let real_src_base = shmem.base_addr() + size * addrs[&pe].1;
                let real_src_addr = real_src_base + (src_addr.0 - addr);
                return ShmemFuture {
                    op: Op::Get(pe, real_src_addr, dst),
                }
                .into();
            }
        }
        panic!("shmem segment invalid for get");
    }
}

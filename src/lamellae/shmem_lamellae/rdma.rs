use std::{
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    task::{Context, Poll},
};

use futures_util::Future;
use pin_project::{pin_project, pinned_drop};

use crate::{
    active_messaging::AMCounters,
    lamellae::{
        comm::rdma::{CommRdma, RdmaFuture, RdmaHandle, Remote},
        CommAllocAddr, CommSlice,
    },
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::{comm::ShmemComm, Scheduler};

pub(super) enum Op<T> {
    Put(CommSlice<T>, CommAllocAddr),
    PutAll(CommSlice<T>, Vec<CommAllocAddr>),
    Get(CommAllocAddr, CommSlice<T>),
    Atomic,
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemFuture<T> {
    pub(crate) op: Op<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> ShmemFuture<T> {
    fn inner_put(&self, src: &CommSlice<T>, dst: &CommAllocAddr) {
        if !(src.contains(dst) || src.contains(dst + src.len())) {
            unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), src.len()) };
        } else {
            unsafe {
                std::ptr::copy(src.as_ptr(), dst.as_mut_ptr(), src.len());
            }
        }
    }
    fn inner_put_all(&self, src: &CommSlice<T>, dsts: &Vec<CommAllocAddr>) {
        for dst in dsts {
            self.inner_put(&src, dst);
        }
    }
    fn inner_get(&self, src: &CommAllocAddr, dst: &CommSlice<T>) {
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
            Op::Put(src, dst) => {
                self.inner_put(src, dst);
            }
            Op::PutAll(src, dst) => {
                self.inner_put_all(src, dst);
            }
            Op::Get(src, dst) => {
                self.inner_get(src, dst);
            }
            Op::Atomic => {}
        }
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.spawned = true;
        // Ok(())
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.spawn_task(async {}, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for ShmemFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<ShmemFuture<T>> for RdmaHandle<T> {
    fn from(f: ShmemFuture<T>) -> RdmaHandle<T> {
        RdmaHandle {
            future: RdmaFuture::Shmem(f),
        }
    }
}

impl<T: Remote> Future for ShmemFuture<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
            *self.project().spawned = true;
        }
        Poll::Ready(())
    }
}

impl CommRdma for ShmemComm {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
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
                    op: Op::Put(src, real_dst_addr),
                    spawned: false,
                    scheduler: scheduler.clone(),
                    counters,
                }
                .into();
            }
        }
        panic!("shmem segment invalid for put");
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: CommSlice<T>,
        remote_addr: CommAllocAddr,
    ) -> RdmaHandle<T> {
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
                        real_dst_addr
                    })
                    .collect::<Vec<_>>();
                return ShmemFuture {
                    op: Op::PutAll(src, pe_addrs),
                    spawned: false,
                    scheduler: scheduler.clone(),
                    counters,
                }
                .into();
            }
        }
        panic!("shmem segment invalid for put all");
    }
    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
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
                    op: Op::Get(real_src_addr, dst),
                    spawned: false,
                    scheduler: scheduler.clone(),
                    counters,
                }
                .into();
            }
        }
        panic!("shmem segment invalid for get");
    }
}

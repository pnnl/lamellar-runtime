use std::{
    mem::MaybeUninit,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_util::Future;
use pin_project::{pin_project, pinned_drop};
use tracing::trace;

use crate::{
    active_messaging::AMCounters,
    lamellae::{
        comm::rdma::{RdmaFuture, RdmaHandle, Remote},
        shmem_lamellae::fabric::ShmemAlloc,
        CommAllocAddr, CommAllocRdma, CommSlice, RdmaAtFuture, RdmaGetHandle,
    },
    memregion::MemregionRdmaInputInner,
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::Scheduler;

pub(super) enum Op<T: Remote> {
    Put(T, CommAllocAddr),
    PutBuf(MemregionRdmaInputInner<T>, CommAllocAddr),
    PutAll(T, Vec<CommAllocAddr>),
    PutAllBuf(MemregionRdmaInputInner<T>, Vec<CommAllocAddr>),
    Get(CommAllocAddr, CommSlice<T>),
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemFuture<T: Remote> {
    pub(super) op: Op<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> ShmemFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn inner_put_buf(&self, src: &MemregionRdmaInputInner<T>, dst: &CommAllocAddr) {
        trace!(
            "putting src: {:?} dst: {:?} len: {} num bytes {}",
            src.as_ptr(),
            dst,
            src.len(),
            src.len() * std::mem::size_of::<T>()
        );
        if !(src.contains(dst) || src.contains(&(dst + src.len()))) {
            unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), src.len()) };
        } else {
            unsafe {
                std::ptr::copy(src.as_ptr(), dst.as_mut_ptr(), src.len());
            }
        }
    }
    #[tracing::instrument(skip_all, level = "debug")]
    fn inner_get(&self, src: &CommAllocAddr, dst: &CommSlice<T>) {
        trace!(
            "getting src: {:?} dst: {:?} len: {}",
            src,
            dst.usize_addr(),
            dst.len()
        );
        if !(dst.contains(src) || dst.contains(&(src + dst.len()))) {
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
            Op::Put(src, dst) => unsafe {
                dst.as_mut_ptr::<T>().write(*src);
            },
            Op::PutBuf(src, dst) => {
                self.inner_put_buf(src, dst);
            }
            Op::PutAll(src, dsts) => {
                for dst in dsts {
                    unsafe { dst.as_mut_ptr::<T>().write(*src) };
                }
            }
            Op::PutAllBuf(src, dsts) => {
                for dst in dsts {
                    self.inner_put_buf(src, dst);
                }
            }
            Op::Get(src, dst) => {
                self.inner_get(src, dst);
            }
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
impl<T: Remote> PinnedDrop for ShmemFuture<T> {
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

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemAtFuture<T> {
    pub(super) src: CommAllocAddr,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    pub(crate) result: MaybeUninit<T>,
}

impl<T: Remote> ShmemAtFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        trace!("getting src: {:?} ", self.src);
        unsafe {
            self.result
                .as_mut_ptr()
                .write(self.src.as_ptr::<T>().read())
        };
    }

    pub(crate) fn block(mut self) -> T {
        self.exec_at();
        self.spawned = true;
        unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(&mut self.result, &mut res);
            res.assume_init()
        }
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_at();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.spawn_task(
            async move {
                unsafe {
                    let mut res = MaybeUninit::uninit();
                    std::mem::swap(&mut self.result, &mut res);
                    res.assume_init()
                }
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for ShmemAtFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<ShmemAtFuture<T>> for RdmaGetHandle<T> {
    fn from(f: ShmemAtFuture<T>) -> RdmaGetHandle<T> {
        RdmaGetHandle {
            future: RdmaAtFuture::Shmem(f),
        }
    }
}

impl<T: Remote> Future for ShmemAtFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        let this = self.project();
        *this.spawned = true;
        // rofi_c_wait();

        Poll::Ready(unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(this.result, &mut res);
            res.assume_init()
        })
    }
}

impl CommAllocRdma for Arc<ShmemAlloc> {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        ShmemFuture {
            op: Op::Put(src.into(), CommAllocAddr(self.pe_base_offset(pe) + offset)),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        let dst = CommAllocAddr(self.pe_base_offset(pe) + offset);
        unsafe {
            dst.as_mut_ptr::<T>().write(src);
        }
    }
    fn put_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        ShmemFuture {
            op: Op::PutBuf(src.into(), CommAllocAddr(self.pe_base_offset(pe) + offset)),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn put_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) {
        let dst = CommAllocAddr(self.pe_base_offset(pe) + offset);
        let src = src.into();
        if !(src.contains(&dst) || src.contains(&(dst + src.len()))) {
            unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), src.len()) };
        } else {
            unsafe {
                std::ptr::copy(src.as_ptr(), dst.as_mut_ptr(), src.len());
            }
        }
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        offset: usize,
    ) -> RdmaHandle<T> {
        let pe_addrs = (0..self.num_pes())
            .map(|pe| {
                let real_dst_base = self.pe_base_offset(pe);
                let real_dst_addr = real_dst_base + offset;
                CommAllocAddr(real_dst_addr)
            })
            .collect::<Vec<_>>();
        ShmemFuture {
            op: Op::PutAll(src, pe_addrs),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize) {
        for pe in 0..self.num_pes() {
            let real_dst_base = self.pe_base_offset(pe);
            let real_dst_addr = real_dst_base + offset;
            let dst = CommAllocAddr(real_dst_addr);
            unsafe {
                dst.as_mut_ptr::<T>().write(src);
            }
        }
    }
    fn put_all_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) -> RdmaHandle<T> {
        let pe_addrs = (0..self.num_pes())
            .map(|pe| {
                let real_dst_base = self.pe_base_offset(pe);
                let real_dst_addr = real_dst_base + offset;
                CommAllocAddr(real_dst_addr)
            })
            .collect::<Vec<_>>();
        ShmemFuture {
            op: Op::PutAllBuf(src.into(), pe_addrs),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn put_all_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) {
        let src = src.into();
        for pe in 0..self.num_pes() {
            let real_dst_base = self.pe_base_offset(pe);
            let real_dst_addr = real_dst_base + offset;
            let dst = CommAllocAddr(real_dst_addr);
            if !(src.contains(&dst) || src.contains(&(dst + src.len()))) {
                unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), src.len()) };
            } else {
                unsafe {
                    std::ptr::copy(src.as_ptr(), dst.as_mut_ptr(), src.len());
                }
            }
        }
    }
    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
        dst: CommSlice<T>,
    ) -> RdmaHandle<T> {
        ShmemFuture {
            op: Op::Get(CommAllocAddr(self.pe_base_offset(pe) + offset), dst),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        let remote_src_base = self.pe_base_offset(pe);
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        ShmemAtFuture {
            src: remote_src_addr,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: MaybeUninit::uninit(),
        }
        .into()
    }
}

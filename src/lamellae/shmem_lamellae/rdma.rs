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
        comm::rdma::{RdmaHandle, RdmaPutFuture, Remote},
        shmem_lamellae::fabric::ShmemAlloc,
        CommAllocAddr, CommAllocRdma, RdmaGetBufferFuture, RdmaGetBufferHandle, RdmaGetFuture,
        RdmaGetHandle, RdmaGetIntoBufferFuture, RdmaGetIntoBufferHandle,
    },
    memregion::{AsLamellarBuffer, LamellarBuffer, MemregionRdmaInputInner},
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::Scheduler;

pub(super) enum Op<T: Remote> {
    Put(T, CommAllocAddr),
    PutBuf(MemregionRdmaInputInner<T>, CommAllocAddr),
    PutAll(T, Vec<CommAllocAddr>),
    PutAllBuf(MemregionRdmaInputInner<T>, Vec<CommAllocAddr>),
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemFuture<T: Remote> {
    op: Op<T>,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
}

impl<T: Remote> ShmemFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn inner_put_buf(src: &MemregionRdmaInputInner<T>, dst: &CommAllocAddr) {
        trace!(
            "putting src: {:?} dst: {:?} len: {} num bytes {}",
            src.as_ptr(),
            dst,
            src.len(),
            src.len() * std::mem::size_of::<T>()
        );
        let dst_slice = unsafe { std::slice::from_raw_parts_mut(dst.as_mut_ptr::<T>(), src.len()) };
        dst_slice.copy_from_slice(src.as_slice());
    }

    fn exec_op(&mut self) {
        match &mut self.op {
            Op::Put(src, dst) => unsafe {
                dst.as_mut_ptr::<T>().write(*src);
            },
            Op::PutBuf(src, dst) => {
                ShmemFuture::inner_put_buf(src, dst);
            }
            Op::PutAll(src, dsts) => {
                for dst in dsts {
                    unsafe { dst.as_mut_ptr::<T>().write(*src) };
                }
            }
            Op::PutAllBuf(src, dsts) => {
                for dst in dsts {
                    ShmemFuture::inner_put_buf(src, dst);
                }
            }
        }
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.spawned = true;
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
            future: RdmaPutFuture::Shmem(f),
        }
    }
}

impl<T: Remote> Future for ShmemFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
            *self.project().spawned = true;
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemGetFuture<T> {
    src: CommAllocAddr,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
    result: MaybeUninit<T>,
}

impl<T: Remote> ShmemGetFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        trace!("getting src: {:?} ", self.src);
        unsafe { self.result.write(self.src.as_ptr::<T>().read()) };
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
impl<T> PinnedDrop for ShmemGetFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<ShmemGetFuture<T>> for RdmaGetHandle<T> {
    fn from(f: ShmemGetFuture<T>) -> RdmaGetHandle<T> {
        RdmaGetHandle {
            future: RdmaGetFuture::Shmem(f),
        }
    }
}

impl<T: Remote> Future for ShmemGetFuture<T> {
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
#[pin_project(PinnedDrop)]
pub(crate) struct ShmemGetBufferFuture<T> {
    src: CommAllocAddr,
    len: usize,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
    result: MaybeUninit<Vec<T>>,
}

impl<T: Remote> ShmemGetBufferFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        trace!("getting src: {:?} ", self.src);
        unsafe {
            let mut dst = Vec::<T>::with_capacity(self.len);
            dst.set_len(self.len);
            let src_slice = std::slice::from_raw_parts(self.src.as_ptr::<T>(), self.len);
            dst.copy_from_slice(src_slice);
            self.result.write(dst);
        }
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_at();
        self.spawned = true;
        unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(&mut self.result, &mut res);
            res.assume_init()
        }
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_at();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
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
impl<T> PinnedDrop for ShmemGetBufferFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<ShmemGetBufferFuture<T>> for RdmaGetBufferHandle<T> {
    fn from(f: ShmemGetBufferFuture<T>) -> RdmaGetBufferHandle<T> {
        RdmaGetBufferHandle {
            future: RdmaGetBufferFuture::Shmem(f),
        }
    }
}

impl<T: Remote> Future for ShmemGetBufferFuture<T> {
    type Output = Vec<T>;
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

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemGetIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    src: CommAllocAddr,
    buffer: LamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> ShmemGetIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let dst = self.buffer.as_mut_slice();
        let src_slice = unsafe { std::slice::from_raw_parts(self.src.as_ptr::<T>(), dst.len()) };
        dst.copy_from_slice(src_slice);
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.spawned = true;
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
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for ShmemGetIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<ShmemGetIntoBufferFuture<T, B>>
    for RdmaGetIntoBufferHandle<T, B>
{
    fn from(f: ShmemGetIntoBufferFuture<T, B>) -> RdmaGetIntoBufferHandle<T, B> {
        RdmaGetIntoBufferHandle {
            future: RdmaGetIntoBufferFuture::Shmem(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for ShmemGetIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
            *self.project().spawned = true;
        }
        Poll::Ready(())
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
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        ShmemFuture {
            op: Op::Put(src.into(), CommAllocAddr(self.pe_base_offset(pe) + offset)),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
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
        let offset = offset * std::mem::size_of::<T>();
        let src = src.into();
        assert!(offset + src.len() * std::mem::size_of::<T>() <= self.num_bytes());
        ShmemFuture {
            op: Op::PutBuf(src, CommAllocAddr(self.pe_base_offset(pe) + offset)),
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
        let offset = offset * std::mem::size_of::<T>();
        let src = src.into();
        assert!(offset + src.len() * std::mem::size_of::<T>() <= self.num_bytes());
        let dst = CommAllocAddr(self.pe_base_offset(pe) + offset);
        if !(src.contains(&dst) || src.contains(&(dst + src.num_bytes()))) {
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
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
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
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
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
        let offset = offset * std::mem::size_of::<T>();
        let src = src.into();
        assert!(offset + src.len() * std::mem::size_of::<T>() <= self.num_bytes());
        let pe_addrs = (0..self.num_pes())
            .map(|pe| {
                let real_dst_base = self.pe_base_offset(pe);
                let real_dst_addr = real_dst_base + offset;
                CommAllocAddr(real_dst_addr)
            })
            .collect::<Vec<_>>();
        ShmemFuture {
            op: Op::PutAllBuf(src, pe_addrs),
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
        let offset = offset * std::mem::size_of::<T>();
        let src = src.into();
        assert!(offset + src.len() * std::mem::size_of::<T>() <= self.num_bytes());
        for pe in 0..self.num_pes() {
            let real_dst_base = self.pe_base_offset(pe);
            let real_dst_addr = real_dst_base + offset;
            let dst = CommAllocAddr(real_dst_addr);
            if !(src.contains(&dst) || src.contains(&(dst + src.num_bytes()))) {
                unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), src.len()) };
            } else {
                unsafe {
                    std::ptr::copy(src.as_ptr(), dst.as_mut_ptr(), src.len());
                }
            }
        }
    }

    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.pe_base_offset(pe);
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        ShmemGetFuture {
            src: remote_src_addr,

            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: MaybeUninit::uninit(),
        }
        .into()
    }

    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> RdmaGetBufferHandle<T> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + len * std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.pe_base_offset(pe);
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        ShmemGetBufferFuture {
            src: remote_src_addr,
            len,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: MaybeUninit::uninit(),
        }
        .into()
    }

    fn get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) -> RdmaGetIntoBufferHandle<T, B> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + dst.len() * std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.pe_base_offset(pe);
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        ShmemGetIntoBufferFuture {
            src: remote_src_addr,
            buffer: dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }

    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + dst.len() * std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.pe_base_offset(pe);
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        let src_slice =
            unsafe { std::slice::from_raw_parts(remote_src_addr.as_ptr::<T>(), dst.len()) };

        dst.as_mut_slice().copy_from_slice(src_slice);
    }
}

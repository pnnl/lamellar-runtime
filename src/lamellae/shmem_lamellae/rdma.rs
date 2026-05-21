use std::{
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
        shmem_lamellae::fabric::{OneSidedShmemAlloc, ShmemAlloc},
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
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
}

impl<T: Remote> ShmemFuture<T> {
    //#[tracing::instrument(skip_all, level = "debug")]
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

        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
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
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemGetFuture<T> {
    src: CommAllocAddr,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    result: Box<T>,
}

impl<T: Remote> ShmemGetFuture<T> {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        trace!("getting src: {:?} ", self.src);
        unsafe {
            *self.result = self.src.as_ptr::<T>().read();
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> T {
        self.exec_at();
        *self.result
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_at();
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { *self.result }, counters)
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
        Poll::Ready(*self.result)
    }
}
#[pin_project(PinnedDrop)]
pub(crate) struct ShmemGetBufferFuture<T> {
    src: CommAllocAddr,
    len: usize,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    result: Vec<T>,
}

impl<T: Remote> ShmemGetBufferFuture<T> {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        trace!("getting src: {:?} ", self.src);
        unsafe {
            let src_slice = std::slice::from_raw_parts(self.src.as_ptr::<T>(), self.len);
            self.result.copy_from_slice(src_slice);
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_at();
        std::mem::take(&mut self.result)
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_at();
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { std::mem::take(&mut self.result) }, counters)
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
        Poll::Ready(std::mem::take(&mut self.result))
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemGetIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    src: CommAllocAddr,
    buffer: LamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> ShmemGetIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let dst = self.buffer.as_mut_slice();
        let src_slice = unsafe { std::slice::from_raw_parts(self.src.as_ptr::<T>(), dst.len()) };
        dst.copy_from_slice(src_slice);
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
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
        }
        Poll::Ready(())
    }
}

impl CommAllocRdma for ShmemAlloc {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
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
    fn put_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        src: T,
        pe: usize,
        offset: usize,
    ) {
        self.put_unmanaged(src, pe, offset)
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
        counters: Option<Arc<[Arc<AMCounters>]>>,
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
        counters: Option<Arc<[Arc<AMCounters>]>>,
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
        counters: Option<Arc<[Arc<AMCounters>]>>,
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
        counters: Option<Arc<[Arc<AMCounters>]>>,
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
            result: Box::new(T::default()),
        }
        .into()
    }

    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.pe_base_offset(pe);
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        unsafe { remote_src_addr.as_ptr::<T>().read() }
    }

    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
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
            result: vec![T::default(); len],
        }
        .into()
    }
    fn blocking_get_buffer<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> Vec<T> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + len * std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.pe_base_offset(pe);
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        let mut dst = vec![T::default(); len];
        unsafe {
            let src_slice = std::slice::from_raw_parts(remote_src_addr.as_ptr::<T>(), len);
            dst.copy_from_slice(src_slice);
        }
        dst
    }

    fn get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
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

    fn blocking_get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        _scheduler: &Arc<Scheduler>,
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

impl CommAllocRdma for OneSidedShmemAlloc {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "put called on OneSidedShmemAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        ShmemFuture {
            op: Op::Put(src.into(), CommAllocAddr(self.start() + offset)),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn put_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        src: T,
        pe: usize,
        offset: usize,
    ) {
        self.put_unmanaged(src, pe, offset)
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        assert_eq!(
            pe, self.remote_pe,
            "put_unmanaged called on OneSidedShmemAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let dst = CommAllocAddr(self.start() + offset);
        unsafe {
            dst.as_mut_ptr::<T>().write(src);
        }
    }
    fn put_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "put_buffer called on OneSidedShmemAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        let src = src.into();
        assert!(offset + src.len() * std::mem::size_of::<T>() <= self.num_bytes());
        ShmemFuture {
            op: Op::PutBuf(src, CommAllocAddr(self.start() + offset)),
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
        assert_eq!(pe, self.remote_pe, "put_buffer_unmanaged called on OneSidedShmemAlloc with incorrect pe: {} expected pe: {}", pe, self.remote_pe);
        let offset = offset * std::mem::size_of::<T>();
        let src = src.into();
        assert!(offset + src.len() * std::mem::size_of::<T>() <= self.num_bytes());
        let dst = CommAllocAddr(self.start() + offset);
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
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        offset: usize,
    ) -> RdmaHandle<T> {
        self.put(scheduler, counters, src, self.remote_pe, offset)
    }
    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize) {
        self.put_unmanaged(src, self.remote_pe, offset);
    }
    fn put_all_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) -> RdmaHandle<T> {
        self.put_buffer(scheduler, counters, src, self.remote_pe, offset)
    }
    fn put_all_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) {
        self.put_buffer_unmanaged(src, self.remote_pe, offset);
    }

    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "get called on OneSidedShmemAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.start();
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        ShmemGetFuture {
            src: remote_src_addr,

            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: Box::new(T::default()),
        }
        .into()
    }

    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        assert_eq!(
            pe, self.remote_pe,
            "blocking_get called on OneSidedShmemAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.start();
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        unsafe { remote_src_addr.as_ptr::<T>().read() }
    }

    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> RdmaGetBufferHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "get_buffer called on OneSidedShmemAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + len * std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.start();
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        ShmemGetBufferFuture {
            src: remote_src_addr,
            len,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: vec![T::default(); len],
        }
        .into()
    }
    fn blocking_get_buffer<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> Vec<T> {
        assert_eq!(
            pe, self.remote_pe,
            "blocking_get_buffer called on OneSidedShmemAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + len * std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.start();
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        unsafe {
            let mut dst = vec![T::default(); len];
            let src_slice = std::slice::from_raw_parts(remote_src_addr.as_ptr::<T>(), len);
            dst.copy_from_slice(src_slice);
            dst
        }
    }

    fn get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) -> RdmaGetIntoBufferHandle<T, B> {
        assert_eq!(
            pe, self.remote_pe,
            "get_into_buffer called on OneSidedShmemAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + dst.len() * std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.start();
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

    fn blocking_get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        _scheduler: &Arc<Scheduler>,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        assert_eq!(
            pe, self.remote_pe,
            "blocking_get_into_buffer called on OneSidedShmemAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + dst.len() * std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.start();
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        let src_slice =
            unsafe { std::slice::from_raw_parts(remote_src_addr.as_ptr::<T>(), dst.len()) };

        dst.as_mut_slice().copy_from_slice(src_slice);
    }

    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        assert_eq!(pe, self.remote_pe, "get_into_buffer_unmanaged called on OneSidedShmemAlloc with incorrect pe: {} expected pe: {}", pe, self.remote_pe);
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + dst.len() * std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.start();
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        let src_slice =
            unsafe { std::slice::from_raw_parts(remote_src_addr.as_ptr::<T>(), dst.len()) };

        dst.as_mut_slice().copy_from_slice(src_slice);
    }
}

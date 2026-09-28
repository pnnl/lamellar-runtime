use std::{
    mem::MaybeUninit,
    pin::Pin,
    sync::{
        atomic::{fence, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use futures_util::Future;
use pin_project::{pin_project, pinned_drop};
use tracing::trace;

use crate::{
    active_messaging::AMCounters,
    lamellae::{
        comm::rdma::{RdmaHandle, RdmaPutFuture, Remote},
        shmem_opt_lamellae::fabric::{OneSidedShmemOptAlloc, ShmemOptAlloc},
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
pub(crate) struct ShmemOptFuture<T: Remote> {
    op: Op<T>,
    scheduler: Arc<Scheduler>,
    spawned: bool,
}

impl<T: Remote> ShmemOptFuture<T> {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn inner_put_buf(src: &MemregionRdmaInputInner<T>, dst: &CommAllocAddr) {
        trace!(
            "putting src: {:?} dst: {:?} len: {} num bytes {}",
            src.as_ptr(),
            dst,
            src.len(),
            src.len() * std::mem::size_of::<T>()
        );
        unsafe {
            std::ptr::copy_nonoverlapping(
                src.as_ptr() as *const u8,
                dst.as_mut_ptr::<T>() as *mut u8,
                src.len() * std::mem::size_of::<T>(),
            )
        };
    }

    fn exec_op(&mut self) {
        match &mut self.op {
            Op::Put(src, dst) => unsafe {
                dst.as_mut_ptr::<T>().write_unaligned(*src);
            },
            Op::PutBuf(src, dst) => {
                ShmemOptFuture::inner_put_buf(src, dst);
            }
            Op::PutAll(src, dsts) => {
                for dst in dsts {
                    unsafe { dst.as_mut_ptr::<T>().write_unaligned(*src) };
                }
            }
            Op::PutAllBuf(src, dsts) => {
                for dst in dsts {
                    ShmemOptFuture::inner_put_buf(src, dst);
                }
            }
        }
        // Order the payload before any later completion signal (a store). Release suffices here:
        // only the *_unmanaged paths back the CommandQueue's Dekker-style flag protocol, which
        // needs StoreLoad and keeps SeqCst. On x86 this is a compiler barrier only.
        fence(Ordering::Release);

        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        // a store is complete once issued: hand back a finished task, no executor round-trip
        self.exec_op();
        self.scheduler.ready_task(())
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for ShmemOptFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<ShmemOptFuture<T>> for RdmaHandle<T> {
    fn from(f: ShmemOptFuture<T>) -> RdmaHandle<T> {
        RdmaHandle {
            future: RdmaPutFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote> Future for ShmemOptFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptGetFuture<T> {
    src: CommAllocAddr,
    alloc: Option<super::fabric::ShmemOptAlloc>,
    scheduler: Arc<Scheduler>,
    spawned: bool,
    result: MaybeUninit<T>, // written by exec_at; T: Copy so never needs dropping
}

impl<T: Remote> ShmemOptGetFuture<T> {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        trace!("getting src: {:?} ", self.src);
        // Whatever told us the data is ready was a prior load (flag/CmdMsg); Acquire keeps this
        // read after it, pairing with the writer's Release/SeqCst fence.
        fence(Ordering::Acquire);
        self.result = MaybeUninit::new(unsafe { self.src.as_ptr::<T>().read_unaligned() });
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> T {
        self.exec_at();
        unsafe { self.result.assume_init() }
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_at();
        self.scheduler.ready_task(unsafe { self.result.assume_init() })
    }
}

#[pinned_drop]
impl<T> PinnedDrop for ShmemOptGetFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<ShmemOptGetFuture<T>> for RdmaGetHandle<T> {
    fn from(f: ShmemOptGetFuture<T>) -> RdmaGetHandle<T> {
        RdmaGetHandle {
            future: RdmaGetFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote> Future for ShmemOptGetFuture<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        // no field is structurally pinned (T may be !Unpin, but it is only ever copied)
        let this = unsafe { self.get_unchecked_mut() };
        if !this.spawned {
            this.exec_at();
        }
        Poll::Ready(unsafe { this.result.assume_init() })
    }
}
#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptGetBufferFuture<T> {
    src: CommAllocAddr,
    alloc: Option<super::fabric::ShmemOptAlloc>,
    len: usize,
    scheduler: Arc<Scheduler>,
    spawned: bool,
    result: Vec<T>, // capacity `len`, length set once exec_at has filled it
}

impl<T: Remote> ShmemOptGetBufferFuture<T> {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        trace!("getting src: {:?} ", self.src);
        fence(Ordering::Acquire);
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.src.as_ptr::<T>(),
                self.result.as_mut_ptr(),
                self.len,
            );
            self.result.set_len(self.len);
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_at();
        std::mem::take(&mut self.result)
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_at();
        let result = std::mem::take(&mut self.result);
        self.scheduler.ready_task(result)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for ShmemOptGetBufferFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<ShmemOptGetBufferFuture<T>> for RdmaGetBufferHandle<T> {
    fn from(f: ShmemOptGetBufferFuture<T>) -> RdmaGetBufferHandle<T> {
        RdmaGetBufferHandle {
            future: RdmaGetBufferFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote> Future for ShmemOptGetBufferFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        Poll::Ready(std::mem::take(&mut self.result))
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptGetIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    src: CommAllocAddr,
    alloc: Option<super::fabric::ShmemOptAlloc>,
    buffer: LamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> ShmemOptGetIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        fence(Ordering::Acquire);
        let dst = self.buffer.as_mut_slice();
        unsafe {
            std::ptr::copy(
                self.src.as_ptr::<T>() as *const u8,
                dst.as_mut_ptr() as *mut u8,
                dst.len() * std::mem::size_of::<T>(),
            );
        }
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        self.scheduler.ready_task(())
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for ShmemOptGetIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<ShmemOptGetIntoBufferFuture<T, B>>
    for RdmaGetIntoBufferHandle<T, B>
{
    fn from(f: ShmemOptGetIntoBufferFuture<T, B>) -> RdmaGetIntoBufferHandle<T, B> {
        RdmaGetIntoBufferHandle {
            future: RdmaGetIntoBufferFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for ShmemOptGetIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

impl CommAllocRdma for ShmemOptAlloc {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        _counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        ShmemOptFuture {
            op: Op::Put(src.into(), CommAllocAddr(self.pe_base_offset(pe) + offset)),
            spawned: false,
            scheduler: scheduler.clone(),
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
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let dst = CommAllocAddr(self.pe_base_offset(pe) + offset);
        unsafe {
            dst.as_mut_ptr::<T>().write_unaligned(src);
        }
        fence(Ordering::Release);
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let dst = CommAllocAddr(self.pe_base_offset(pe) + offset);
        unsafe {
            dst.as_mut_ptr::<T>().write_unaligned(src);
        }
        fence(Ordering::SeqCst);
    }
    fn put_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        _counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        let offset = offset * std::mem::size_of::<T>();
        let src = src.into();
        assert!(offset + src.len() * std::mem::size_of::<T>() <= self.num_bytes());
        ShmemOptFuture {
            op: Op::PutBuf(src, CommAllocAddr(self.pe_base_offset(pe) + offset)),
            spawned: false,
            scheduler: scheduler.clone(),
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
        fence(Ordering::SeqCst);
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        _counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        offset: usize,
    ) -> RdmaHandle<T> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let pe_addrs = (0..self.num_pes())
            .map(|pe| {
                let real_dst_base = self.pe_base_offset_rel(pe);
                let real_dst_addr = real_dst_base + offset;
                CommAllocAddr(real_dst_addr)
            })
            .collect::<Vec<_>>();
        ShmemOptFuture {
            op: Op::PutAll(src, pe_addrs),
            spawned: false,
            scheduler: scheduler.clone(),
        }
        .into()
    }
    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize) {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        for pe in 0..self.num_pes() {
            let real_dst_base = self.pe_base_offset_rel(pe);
            let real_dst_addr = real_dst_base + offset;
            let dst = CommAllocAddr(real_dst_addr);
            unsafe {
                dst.as_mut_ptr::<T>().write_unaligned(src);
            }
        }
        fence(Ordering::SeqCst);
    }
    fn put_all_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        _counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) -> RdmaHandle<T> {
        let offset = offset * std::mem::size_of::<T>();
        let src = src.into();
        assert!(offset + src.len() * std::mem::size_of::<T>() <= self.num_bytes());
        let pe_addrs = (0..self.num_pes())
            .map(|pe| {
                let real_dst_base = self.pe_base_offset_rel(pe);
                let real_dst_addr = real_dst_base + offset;
                CommAllocAddr(real_dst_addr)
            })
            .collect::<Vec<_>>();
        ShmemOptFuture {
            op: Op::PutAllBuf(src, pe_addrs),
            spawned: false,
            scheduler: scheduler.clone(),
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
            let real_dst_base = self.pe_base_offset_rel(pe);
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
        fence(Ordering::SeqCst);
    }

    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        _counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.pe_base_offset(pe);
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        ShmemOptGetFuture {
            src: remote_src_addr,
            alloc: Some(self.clone()),
            spawned: false,
            scheduler: scheduler.clone(),
            result: MaybeUninit::uninit(),
        }
        .into()
    }

    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.pe_base_offset(pe);
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        fence(Ordering::Acquire);
        unsafe { remote_src_addr.as_ptr::<T>().read_unaligned() }
    }

    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        _counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> RdmaGetBufferHandle<T> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + len * std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.pe_base_offset(pe);
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        ShmemOptGetBufferFuture {
            src: remote_src_addr,
            alloc: Some(self.clone()),
            len,
            spawned: false,
            scheduler: scheduler.clone(),
            result: Vec::with_capacity(len),
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
        let mut dst: Vec<T> = Vec::with_capacity(len);
        fence(Ordering::Acquire);
        unsafe {
            std::ptr::copy_nonoverlapping(remote_src_addr.as_ptr::<T>(), dst.as_mut_ptr(), len);
            dst.set_len(len);
        }
        dst
    }

    fn get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        _counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) -> RdmaGetIntoBufferHandle<T, B> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + dst.len() * std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.pe_base_offset(pe);
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        ShmemOptGetIntoBufferFuture {
            src: remote_src_addr,
            alloc: Some(self.clone()),
            buffer: dst,
            spawned: false,
            scheduler: scheduler.clone(),
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
        fence(Ordering::Acquire);
        unsafe {
            std::ptr::copy(
                remote_src_addr.as_ptr::<T>() as *const u8,
                dst.as_mut_slice().as_mut_ptr() as *mut u8,
                dst.len() * std::mem::size_of::<T>(),
            );
        }
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
        fence(Ordering::SeqCst);
        unsafe {
            std::ptr::copy(
                remote_src_addr.as_ptr::<T>() as *const u8,
                dst.as_mut_slice().as_mut_ptr() as *mut u8,
                dst.len() * std::mem::size_of::<T>(),
            );
        }
    }
}

impl CommAllocRdma for OneSidedShmemOptAlloc {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        _counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "put called on OneSidedShmemOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        ShmemOptFuture {
            op: Op::Put(src.into(), CommAllocAddr(self.start() + offset)),
            spawned: false,
            scheduler: scheduler.clone(),
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
        assert_eq!(
            pe, self.remote_pe,
            "put_blocking called on OneSidedShmemOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let dst = CommAllocAddr(self.start() + offset);
        unsafe {
            dst.as_mut_ptr::<T>().write_unaligned(src);
        }
        fence(Ordering::Release);
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        assert_eq!(
            pe, self.remote_pe,
            "put_unmanaged called on OneSidedShmemOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let dst = CommAllocAddr(self.start() + offset);
        unsafe {
            dst.as_mut_ptr::<T>().write_unaligned(src);
        }
        fence(Ordering::SeqCst);
    }
    fn put_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        _counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "put_buffer called on OneSidedShmemOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        let src = src.into();
        assert!(offset + src.len() * std::mem::size_of::<T>() <= self.num_bytes());
        ShmemOptFuture {
            op: Op::PutBuf(src, CommAllocAddr(self.start() + offset)),
            spawned: false,
            scheduler: scheduler.clone(),
        }
        .into()
    }
    fn put_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) {
        assert_eq!(pe, self.remote_pe, "put_buffer_unmanaged called on OneSidedShmemOptAlloc with incorrect pe: {} expected pe: {}", pe, self.remote_pe);
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
        fence(Ordering::SeqCst);
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
        _counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "get called on OneSidedShmemOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.start();
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        ShmemOptGetFuture {
            src: remote_src_addr,
            alloc: Some(self.alloc.clone()),
            spawned: false,
            scheduler: scheduler.clone(),
            result: MaybeUninit::uninit(),
        }
        .into()
    }

    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        assert_eq!(
            pe, self.remote_pe,
            "blocking_get called on OneSidedShmemOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.start();
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        fence(Ordering::Acquire);
        unsafe { remote_src_addr.as_ptr::<T>().read_unaligned() }
    }

    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        _counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> RdmaGetBufferHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "get_buffer called on OneSidedShmemOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + len * std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.start();
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        ShmemOptGetBufferFuture {
            src: remote_src_addr,
            alloc: Some(self.alloc.clone()),
            len,
            spawned: false,
            scheduler: scheduler.clone(),
            result: Vec::with_capacity(len),
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
            "blocking_get_buffer called on OneSidedShmemOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + len * std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.start();
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        let mut dst: Vec<T> = Vec::with_capacity(len);
        fence(Ordering::Acquire);
        unsafe {
            std::ptr::copy_nonoverlapping(remote_src_addr.as_ptr::<T>(), dst.as_mut_ptr(), len);
            dst.set_len(len);
        }
        dst
    }

    fn get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        _counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) -> RdmaGetIntoBufferHandle<T, B> {
        assert_eq!(
            pe, self.remote_pe,
            "get_into_buffer called on OneSidedShmemOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + dst.len() * std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.start();
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        ShmemOptGetIntoBufferFuture {
            src: remote_src_addr,
            alloc: Some(self.alloc.clone()),
            buffer: dst,
            spawned: false,
            scheduler: scheduler.clone(),
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
            "blocking_get_into_buffer called on OneSidedShmemOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + dst.len() * std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.start();
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        fence(Ordering::Acquire);
        unsafe {
            std::ptr::copy(
                remote_src_addr.as_ptr::<T>() as *const u8,
                dst.as_mut_slice().as_mut_ptr() as *mut u8,
                dst.len() * std::mem::size_of::<T>(),
            );
        }
    }

    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        assert_eq!(pe, self.remote_pe, "get_into_buffer_unmanaged called on OneSidedShmemOptAlloc with incorrect pe: {} expected pe: {}", pe, self.remote_pe);
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + dst.len() * std::mem::size_of::<T>() <= self.num_bytes());
        let remote_src_base = self.start();
        let remote_src_addr = CommAllocAddr(remote_src_base + offset);
        fence(Ordering::SeqCst);
        unsafe {
            std::ptr::copy(
                remote_src_addr.as_ptr::<T>() as *const u8,
                dst.as_mut_slice().as_mut_ptr() as *mut u8,
                dst.len() * std::mem::size_of::<T>(),
            );
        }
    }
}

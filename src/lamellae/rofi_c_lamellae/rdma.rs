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
        comm::rdma::{
            CommAllocRdma, RdmaGetBufferFuture, RdmaGetBufferHandle, RdmaGetFuture, RdmaGetHandle,
            RdmaGetIntoBufferFuture, RdmaGetIntoBufferHandle, RdmaHandle, RdmaPutFuture, Remote,
        },
        CommAlloc, CommAllocAddr, CommAllocInner,
    },
    memregion::{AsLamellarBuffer, LamellarBuffer, MemregionRdmaInputInner},
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::{fabric::*, rofi::*, Scheduler};

// Mirror libfabric_lamellae's structure: separate futures for put/get/get_buffer/get_into_buffer.

pub(super) enum AllocOp<T: Remote> {
    Put(usize, T),
    PutBuf(usize, MemregionRdmaInputInner<T>),
    PutAll(Vec<usize>, T),
    PutAllBuf(Vec<usize>, MemregionRdmaInputInner<T>),
}

#[pin_project(PinnedDrop)]
pub(crate) struct RofiCPutFuture<T: Remote> {
    my_pe: usize,
    addr: CommAllocAddr,
    offset: usize,
    op: AllocOp<T>,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
}

impl<T: Remote> RofiCPutFuture<T> {
    fn exec_put_val(&self, pe: usize, val: &T) {
        trace!("rofi_c put val to pe {} addr: {:?}", pe, self.addr);
        let dst = (self.addr.0 + self.offset * std::mem::size_of::<T>()) as usize;
        if pe != self.my_pe {
            unsafe { rofi_c_put(std::slice::from_ref(val), dst, pe).expect("rofi_c_put failed") }
        } else {
            unsafe { std::ptr::copy(val as *const T, dst as *mut T, 1) }
        }
    }
    fn exec_put_buf(&self, pe: usize, src: &MemregionRdmaInputInner<T>) {
        trace!("rofi_c put buf to pe {} addr: {:?}", pe, self.addr);
        let dst = (self.addr.0 + self.offset * std::mem::size_of::<T>()) as usize;
        if pe != self.my_pe {
            unsafe { rofi_c_put(src.as_slice(), dst, pe).expect("rofi_c_put failed") }
        } else {
            unsafe { std::ptr::copy(src.as_ptr(), dst as *mut T, src.len()) }
        }
    }

    fn exec_op(&mut self) {
        match &self.op {
            AllocOp::Put(pe, src) => self.exec_put_val(*pe, src),
            AllocOp::PutBuf(pe, src) => self.exec_put_buf(*pe, src),
            AllocOp::PutAll(pes, src) => {
                for pe in pes {
                    self.exec_put_val(*pe, src);
                }
            }
            AllocOp::PutAllBuf(pes, src) => {
                for pe in pes {
                    self.exec_put_buf(*pe, src);
                }
            }
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        rofi_c_wait();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.spawn_task(
            async move {
                rofi_c_wait();
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for RofiCPutFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<RofiCPutFuture<T>> for RdmaHandle<T> {
    fn from(f: RofiCPutFuture<T>) -> RdmaHandle<T> {
        RdmaHandle {
            future: RdmaPutFuture::RofiC(f),
        }
    }
}

impl<T: Remote> Future for RofiCPutFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        rofi_c_wait();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct RofiCGetFuture<T: Remote> {
    my_pe: usize,
    addr: CommAllocAddr,
    pe: usize,
    offset: usize,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
    issued_network: bool,
    result: Box<T>,
}

impl<T: Remote> RofiCGetFuture<T> {
    fn exec_at(&mut self) {
        trace!("rofi_c get at: {:?} {:?}", self.pe, self.offset);
        let src = (self.addr.0 + self.offset * std::mem::size_of::<T>()) as usize;
        if self.pe == self.my_pe {
            unsafe {
                std::ptr::copy(src as *const T, &mut *self.result as *mut T, 1)
            }
            self.issued_network = false;
        } else {
            unsafe {
                rofi_c_get(src, std::slice::from_mut(&mut *self.result), self.pe)
                    .expect("rofi_c_get failed")
            };
            self.issued_network = true;
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> T {
        self.exec_at();
        if self.issued_network {
            rofi_c_wait();
        }
        *self.result
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_at();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for RofiCGetFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaGetHandle").print();
        }
    }
}

impl<T: Remote> From<RofiCGetFuture<T>> for RdmaGetHandle<T> {
    fn from(f: RofiCGetFuture<T>) -> RdmaGetHandle<T> {
        RdmaGetHandle {
            future: RdmaGetFuture::RofiC(f),
        }
    }
}

impl<T: Remote> Future for RofiCGetFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        if self.issued_network {
            rofi_c_wait();
        }
        Poll::Ready(*self.result)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct RofiCGetBufferFuture<T: Remote> {
    my_pe: usize,
    addr: CommAllocAddr,
    pe: usize,
    offset: usize,
    len: usize,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
    issued_network: bool,
    result: Vec<T>,
}

impl<T: Remote> RofiCGetBufferFuture<T> {
    fn exec_at(&mut self) {
        trace!("rofi_c get buffer at: {:?} {:?}", self.pe, self.offset);
        let src = (self.addr.0 + self.offset * std::mem::size_of::<T>()) as usize;
        if self.pe == self.my_pe {
            unsafe {
                std::ptr::copy(src as *const T, self.result.as_mut_ptr(), self.len)
            }
            self.issued_network = false;
        } else {
            unsafe { rofi_c_get(src, &mut self.result, self.pe).expect("rofi_c_get failed") };
            self.issued_network = true;
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_at();
        if self.issued_network {
            rofi_c_wait();
        }
        std::mem::take(&mut self.result)
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_at();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for RofiCGetBufferFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaGetBufferHandle").print();
        }
    }
}

impl<T: Remote> From<RofiCGetBufferFuture<T>> for RdmaGetBufferHandle<T> {
    fn from(f: RofiCGetBufferFuture<T>) -> RdmaGetBufferHandle<T> {
        RdmaGetBufferHandle {
            future: RdmaGetBufferFuture::RofiC(f),
        }
    }
}

impl<T: Remote> Future for RofiCGetBufferFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        if self.issued_network {
            rofi_c_wait();
        }
        Poll::Ready(std::mem::take(&mut self.result))
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct RofiCGetIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    my_pe: usize,
    addr: CommAllocAddr,
    pe: usize,
    offset: usize,
    dst: LamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
    issued_network: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> RofiCGetIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let src = (self.addr.0 + self.offset * std::mem::size_of::<T>()) as usize;
        if self.pe == self.my_pe {
            unsafe {
                std::ptr::copy(
                    src as *const T,
                    self.dst.as_mut_slice().as_mut_ptr(),
                    self.dst.len(),
                )
            }
            self.issued_network = false;
        } else {
            unsafe { rofi_c_get(src, self.dst.as_mut_slice(), self.pe).expect("rofi_c_get failed") };
            self.issued_network = true;
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        if self.issued_network {
            rofi_c_wait();
        }
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let issued_network = self.issued_network;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        let scheduler = self.scheduler.clone();
        scheduler.spawn_task(
            async move {
                if issued_network {
                    rofi_c_wait();
                }
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for RofiCGetIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaGetIntoBufferHandle").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<RofiCGetIntoBufferFuture<T, B>>
    for RdmaGetIntoBufferHandle<T, B>
{
    fn from(f: RofiCGetIntoBufferFuture<T, B>) -> RdmaGetIntoBufferHandle<T, B> {
        RdmaGetIntoBufferHandle {
            future: RdmaGetIntoBufferFuture::RofiC(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for RofiCGetIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        if self.issued_network {
            rofi_c_wait();
        }
        Poll::Ready(())
    }
}

// Note: the actual wiring between CommAllocInner::Raw and these RofiC futures
// requires adding a RofiC-specific CommAllocInner variant or routing calls
// from `CommAllocInner::Raw`. That change lives in shared modules; here we
// only provide the backend futures and conversions.

impl CommAllocRdma for crate::lamellae::rofi_c_lamellae::fabric::RofiCAlloc {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        RofiCPutFuture {
            my_pe: self.my_pe,
            addr: CommAllocAddr(self.start()),
            offset,
            op: AllocOp::Put(pe, src),
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }

    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {

        let dst = (self.start() + offset * std::mem::size_of::<T>()) as usize;
        if pe != self.my_pe {
            trace!("rofi_c put unmanaged val to pe {} addr: {:x?}", pe, self.start());
            unsafe { rofi_c_put(std::slice::from_ref(&src), dst, pe).expect("rofi_c_put failed") }
        } else {
            trace!("rofi_c put unmanaged val locally addr: {:x?}", self.start());
            unsafe { std::ptr::copy(&src as *const T, dst as *mut T, 1) }
        }
    }

    fn put_blocking<T: Remote>(&self, _scheduler: &Arc<Scheduler>, src: T, pe: usize, offset: usize) {
        let dst = (self.start() + offset * std::mem::size_of::<T>()) as usize;
        if pe != self.my_pe {
            trace!("rofi_c put blocking val to pe {} addr: {:x?}", pe, self.start());
            unsafe { rofi_c_put(std::slice::from_ref(&src), dst, pe).expect("rofi_c_put failed") }
            rofi_c_wait();
        } else {
            trace!("rofi_c put blocking val locally addr: {:x?}", self.start());
            unsafe { std::ptr::copy(&src as *const T, dst as *mut T, 1) }
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
        RofiCPutFuture {
            my_pe: self.my_pe,
            addr: CommAllocAddr(self.start()),
            offset,
            op: AllocOp::PutBuf(pe, src.into()),
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }

    fn put_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) {
        let src = src.into();
        let dst = (self.start() + offset * std::mem::size_of::<T>()) as usize;
        if pe != self.my_pe {
            unsafe { rofi_c_put(src.as_slice(), dst, pe).expect("rofi_c_put failed") }
        } else {
            unsafe { std::ptr::copy(src.as_ptr(), dst as *mut T, src.len()) }
        }
    }

    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        offset: usize,
    ) -> RdmaHandle<T> {
        RofiCPutFuture {
            my_pe: self.my_pe,
            addr: CommAllocAddr(self.start()),
            offset,
            op: AllocOp::PutAll((0..self.num_pes).collect(), src),
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }

    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize) {
        for pe in 0..self.num_pes {
            let dst = (self.start() + offset * std::mem::size_of::<T>()) as usize;
            if pe != self.my_pe {
                unsafe {
                    rofi_c_put(std::slice::from_ref(&src), dst, pe)
                        .expect("rofi_c_put failed")
                }
            } else {
                unsafe { std::ptr::copy(&src as *const T, dst as *mut T, 1) }
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
        RofiCPutFuture {
            my_pe: self.my_pe,
            addr: CommAllocAddr(self.start()),
            offset,
            op: AllocOp::PutAllBuf((0..self.num_pes).collect(), src.into()),
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }

    fn put_all_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) {
        let src = src.into();
        for pe in 0..self.num_pes {
            let dst = (self.start() + offset * std::mem::size_of::<T>()) as usize;
            if pe != self.my_pe {
                unsafe { rofi_c_put(src.as_slice(), dst, pe).expect("rofi_c_put failed") }
            } else {
                unsafe { std::ptr::copy(src.as_ptr(), dst as *mut T, src.len()) }
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
        RofiCGetFuture {
            my_pe: self.my_pe,
            addr: CommAllocAddr(self.start()),
            pe,
            offset,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
            issued_network: false,
            result: Box::new(T::default()),
        }
        .into()
    }

    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        let mut val: T = T::default();
        let val_slice = std::slice::from_mut(&mut val);
        let src = (self.start() + offset * std::mem::size_of::<T>()) as usize;
        if pe == self.my_pe {
            unsafe { std::ptr::copy(src as *const T, &mut val as *mut T, 1) }
        } else {
            unsafe {
                rofi_c_get(src, val_slice, pe).expect("rofi_c_get failed")
            };
            rofi_c_wait();
        }
        val
    }

    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> RdmaGetBufferHandle<T> {
        RofiCGetBufferFuture {
            my_pe: self.my_pe,
            addr: CommAllocAddr(self.start()),
            pe,
            offset,
            len,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
            issued_network: false,
            result: vec![T::default(); len],
        }
        .into()
    }

    fn blocking_get_buffer<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize, len: usize) -> Vec<T> {
        let mut dst = vec![T::default(); len];
        let src = (self.start() + offset * std::mem::size_of::<T>()) as usize;
        if pe == self.my_pe {
            unsafe { std::ptr::copy(src as *const T, dst.as_mut_ptr(), len) }
        } else {
            unsafe { rofi_c_get(src, &mut dst, pe).expect("rofi_c_get failed") };
            rofi_c_wait();
        }
        dst
    }

    fn get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) -> RdmaGetIntoBufferHandle<T, B> {
        RofiCGetIntoBufferFuture {
            my_pe: self.my_pe,
            addr: CommAllocAddr(self.start()),
            pe,
            offset,
            dst,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
            issued_network: false,
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
        let src = (self.start() + offset * std::mem::size_of::<T>()) as usize;
        if pe == self.my_pe {
            unsafe {
                std::ptr::copy(
                    src as *const T,
                    dst.as_mut_slice().as_mut_ptr(),
                    dst.len(),
                )
            }
        } else {
            unsafe {
                rofi_c_get(src, dst.as_mut_slice(), pe).expect("rofi_c_get failed");
            }
            rofi_c_wait();
        }
    }

    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        let src = (self.start() + offset * std::mem::size_of::<T>()) as usize;
        if pe == self.my_pe {
            unsafe {
                std::ptr::copy(
                    src as *const T,
                    dst.as_mut_slice().as_mut_ptr(),
                    dst.len(),
                )
            }
        } else {
            unsafe {
                rofi_c_get(src, dst.as_mut_slice(), pe).expect("rofi_c_get failed");
            }
        }
    }
}

impl CommAllocRdma for crate::lamellae::rofi_c_lamellae::fabric::OneSidedRofiCAlloc {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        self.alloc.put(scheduler, counters, src, pe, offset)
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        self.alloc.put_unmanaged(src, pe, offset)
    }
    fn put_blocking<T: Remote>(&self, _scheduler: &Arc<Scheduler>, src: T, pe: usize, offset: usize) {
        self.alloc.put_blocking(_scheduler, src, pe, offset)
    }
    fn put_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        self.alloc.put_buffer(scheduler, counters, src, pe, offset)
    }
    fn put_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) {
        self.alloc.put_buffer_unmanaged(src, pe, offset)
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        offset: usize,
    ) -> RdmaHandle<T> {
        self.alloc.put_all(scheduler, counters, src, offset)
    }
    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize) {
        self.alloc.put_all_unmanaged(src, offset)
    }
    fn put_all_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) -> RdmaHandle<T> {
        self.alloc.put_all_buffer(scheduler, counters, src, offset)
    }
    fn put_all_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) {
        self.alloc.put_all_buffer_unmanaged(src, offset)
    }
    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        self.alloc.get(scheduler, counters, pe, offset)
    }
    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        self.alloc.blocking_get(_scheduler, pe, offset)
    }
    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> RdmaGetBufferHandle<T> {
        self.alloc.get_buffer(scheduler, counters, pe, offset, len)
    }
    fn blocking_get_buffer<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize, len: usize) -> Vec<T> {
        self.alloc.blocking_get_buffer(_scheduler, pe, offset, len)
    }
    fn get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) -> RdmaGetIntoBufferHandle<T, B> {
        self.alloc
            .get_into_buffer(scheduler, counters, pe, offset, dst)
    }
    fn blocking_get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        _scheduler: &Arc<Scheduler>,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) {
        self.alloc.blocking_get_into_buffer(_scheduler, pe, offset, dst)
    }
    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) {
        self.alloc.get_into_buffer_unmanaged(pe, offset, dst)
    }
}

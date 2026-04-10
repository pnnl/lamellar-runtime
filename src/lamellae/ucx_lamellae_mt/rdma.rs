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
            RdmaGetBufferFuture, RdmaGetBufferHandle, RdmaGetFuture, RdmaGetHandle,
            RdmaGetIntoBufferFuture, RdmaGetIntoBufferHandle, RdmaHandle, RdmaPutFuture, Remote,
        },
        CommAllocRdma,
    },
    memregion::{AsLamellarBuffer, LamellarBuffer, MemregionRdmaInputInner},
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::{
    fabric::{OneSidedUcxMtAlloc, UcxMtAlloc, UcxRequest},
    Scheduler,
};

#[derive(Clone)]
pub(super) enum AllocOp<T: Remote> {
    Put(usize, T),
    PutBuf(usize, MemregionRdmaInputInner<T>),
    PutAll(Vec<usize>, T),
    PutAllBuf(Vec<usize>, MemregionRdmaInputInner<T>),
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtPutFuture<T: Remote> {
    my_pe: usize,
    alloc: UcxMtAlloc,
    offset: usize,
    op: AllocOp<T>,
    local_op: bool,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
    request: Option<UcxRequest>,
}

impl<T: Remote> UcxMtPutFuture<T> {
    fn inner_put(&mut self, pe: usize, src: T) {
        if pe != self.my_pe {
            self.request = unsafe {
                UcxMtAlloc::put_inner(
                    &self.alloc,
                    pe,
                    self.offset,
                    std::slice::from_ref(&src),
                    false,
                    true,
                )
            };
        } else {
            let dst = (self.alloc.start() + self.offset) as *mut T;
            unsafe { dst.write(src) };
        }
    }
    fn inner_put_buf(&mut self, pe: usize, src: &MemregionRdmaInputInner<T>) {
        trace!(
            "putting src: {:?} dst: {:?} len: {} num bytes {}",
            src.as_ptr(),
            self.alloc.start() + self.offset,
            src.len(),
            src.len() * std::mem::size_of::<T>()
        );
        if pe != self.my_pe {
            self.request = unsafe {
                UcxMtAlloc::put_inner(&self.alloc, pe, self.offset, src.as_slice(), false, true)
            };
        } else {
            self.alloc.as_mut_slice()[self.offset..self.offset + src.len()]
                .copy_from_slice(src.as_slice());
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn inner_put_all(&mut self, pes: &Vec<usize>, src: T) {
        for pe in pes {
            self.inner_put(*pe, src);
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn inner_put_all_buf(&mut self, pes: &Vec<usize>, src: &MemregionRdmaInputInner<T>) {
        for pe in pes {
            self.inner_put_buf(*pe, src);
        }
    }

    fn exec_op(&mut self) {
        match self.op.clone() {
            AllocOp::Put(pe, src) => {
                self.inner_put(pe, src);
            }
            AllocOp::PutBuf(pe, src) => {
                self.inner_put_buf(pe, &src);
            }
            AllocOp::PutAll(pes, src) => {
                self.inner_put_all(&pes, src);
            }
            AllocOp::PutAllBuf(pes, src) => {
                self.inner_put_all_buf(&pes, &src);
            }
        }
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        if let Some(request) = self.request.take() {
            request.wait().expect("ucx put failed");
        } else if !self.local_op {
            self.alloc.wait_all();
        }
        self.spawned = true;
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move {
                if let Some(request) = self.request.take() {
                    request.wait().expect("ucx put failed");
                } else if !self.local_op {
                    self.alloc.wait_all();
                }
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for UcxMtPutFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<UcxMtPutFuture<T>> for RdmaHandle<T> {
    fn from(f: UcxMtPutFuture<T>) -> RdmaHandle<T> {
        RdmaHandle {
            future: RdmaPutFuture::UcxMt(f),
        }
    }
}

impl<T: Remote> Future for UcxMtPutFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        *this.spawned = true;
        if let Some(request) = this.request.take() {
            let _ = request.wait().expect("ucx put failed");
        } else if !*this.local_op {
            this.alloc.wait_all();
        }

        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtGetFuture<T> {
    alloc: UcxMtAlloc,
    pe: usize,
    offset: usize,
    local_op: bool,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
    result: Box<T>,
    request: Option<UcxRequest>,
}

impl<T: Remote> UcxMtGetFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        unsafe {
            self.request = self.alloc.inner_get(
                self.pe,
                self.offset,
                false,
                std::slice::from_mut(&mut *self.result),
            );
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> T {
        self.exec_at();
        if let Some(request) = self.request.take() {
            request.wait().expect("ucx get failed");
        } else if !self.local_op {
            self.alloc.wait_all();
        }
        *self.result
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_at();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move {
                if let Some(request) = self.request.take() {
                    request.wait().expect("ucx get failed");
                } else if !self.local_op {
                    self.alloc.wait_all();
                }
                *self.result
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for UcxMtGetFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<UcxMtGetFuture<T>> for RdmaGetHandle<T> {
    fn from(f: UcxMtGetFuture<T>) -> RdmaGetHandle<T> {
        RdmaGetHandle {
            future: RdmaGetFuture::UcxMt(f),
        }
    }
}

impl<T: Remote> Future for UcxMtGetFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        let this = self.project();
        if let Some(request) = this.request.take() {
            request.wait().expect("ucx get failed");
        } else if !*this.local_op {
            this.alloc.wait_all();
        }

        Poll::Ready(**this.result)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtGetBufferFuture<T> {
    alloc: UcxMtAlloc,
    pe: usize,
    offset: usize,
    len: usize,
    local_op: bool,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
    result: Vec<T>,
    request: Option<UcxRequest>,
}

impl<T: Remote> UcxMtGetBufferFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn exec_get(&mut self) {
        unsafe {
            self.request = self
                .alloc
                .inner_get(self.pe, self.offset, false, &mut self.result);
        }
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_get();
        self.spawned = true;
        if let Some(request) = self.request.take() {
            request.wait().expect("ucx get buffer failed");
        } else if !self.local_op {
            self.alloc.wait_all();
        }
        std::mem::take(&mut self.result)
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_get();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move {
                if let Some(request) = self.request.take() {
                    request.wait().expect("ucx get buffer failed");
                } else if !self.local_op {
                    self.alloc.wait_all();
                }
                std::mem::take(&mut self.result)
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for UcxMtGetBufferFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<UcxMtGetBufferFuture<T>> for RdmaGetBufferHandle<T> {
    fn from(f: UcxMtGetBufferFuture<T>) -> RdmaGetBufferHandle<T> {
        RdmaGetBufferHandle {
            future: RdmaGetBufferFuture::UcxMt(f),
        }
    }
}

impl<T: Remote> Future for UcxMtGetBufferFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_get();
        }
        let this = self.project();
        *this.spawned = true;
        if let Some(request) = this.request.take() {
            request.wait().expect("ucx get buffer failed");
        } else if !*this.local_op {
            this.alloc.wait_all();
        }

        Poll::Ready(std::mem::take(this.result))
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxMtGetIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    my_pe: usize,
    alloc: UcxMtAlloc,
    pe: usize,
    offset: usize,
    local_op: bool,
    dst: LamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
    request: Option<UcxRequest>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxMtGetIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        if self.pe != self.my_pe {
            self.request = unsafe {
                UcxMtAlloc::inner_get(
                    &self.alloc,
                    self.pe,
                    self.offset,
                    false,
                    self.dst.as_mut_slice(),
                )
            };
        } else {
            let len = self.dst.len();
            self.dst
                .as_mut_slice()
                .copy_from_slice(&self.alloc.as_mut_slice()[self.offset..(self.offset + len)])
        }
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        if let Some(request) = self.request.take() {
            request.wait().expect("ucx get into buffer failed");
        } else if !self.local_op {
            self.alloc.wait_all();
        }
        self.spawned = true;
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        let request = self.request.take();
        let alloc = self.alloc.clone();
        let local_op = self.local_op;
        self.scheduler.clone().spawn_task(
            async move {
                match request {
                    Some(request) => request.wait().expect("ucx get failed"),
                    None => {
                        if !local_op {
                            alloc.wait_all()
                        }
                    }
                };
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxMtGetIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<UcxMtGetIntoBufferFuture<T, B>>
    for RdmaGetIntoBufferHandle<T, B>
{
    fn from(f: UcxMtGetIntoBufferFuture<T, B>) -> RdmaGetIntoBufferHandle<T, B> {
        RdmaGetIntoBufferHandle {
            future: RdmaGetIntoBufferFuture::UcxMt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxMtGetIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        *this.spawned = true;
        if let Some(request) = this.request.take() {
            request.wait().expect("ucx get into buffer failed");
        } else if !*this.local_op {
            this.alloc.wait_all();
        }

        Poll::Ready(())
    }
}

impl CommAllocRdma for UcxMtAlloc {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        //  self.put_amt
        //     .fetch_add(src.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
        trace!(
            "putting to dst: {:?} offset<T>: {:?} final addr{:x} len: 1 num bytes {}",
            self.start(),
            offset,
            self.start() + offset,
            std::mem::size_of::<T>()
        );
        let local_op = pe == self.my_pe;
        UcxMtPutFuture {
            my_pe: self.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::Put(pe, src.into()),
            local_op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
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
        if pe != self.my_pe {
            let _ = unsafe {
                UcxMtAlloc::put_inner(&self, pe, offset, std::slice::from_ref(&src), true, true)
            };
        } else {
            self.as_mut_slice()[offset] = src;
        }
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        trace!(
            "put unmanaged to dst: {:x} offset<T>: {:?} final addr {:x} len: 1 num bytes {}",
            self.start(),
            offset,
            self.start() + offset,
            std::mem::size_of::<T>()
        );
        if pe != self.my_pe {
            // for ucx put operation waiting on the request simply ensures the input buffer is free to reuse
            // not that the operation has completed on the remote side
            let _ = unsafe {
                UcxMtAlloc::put_inner(&self, pe, offset, std::slice::from_ref(&src), false, false)
            };
        } else {
            self.as_mut_slice()[offset] = src;
            // let dst = (self.start() + offset) as *mut T;
            // unsafe { dst.write(src) };
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
        //  self.put_amt
        //     .fetch_add(src.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
        let local_op = pe == self.my_pe;
        UcxMtPutFuture {
            my_pe: self.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::PutBuf(pe, src.into()),
            local_op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
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
        trace!(
            "putting src: {:?} dst: {:?} len: {} num bytes {}",
            src.as_ptr(),
            self.start() + offset,
            src.len(),
            src.len() * std::mem::size_of::<T>()
        );
        if pe != self.my_pe {
            // for ucx put operation waiting on the request simply ensures the input buffer is free to reuse
            // not that the operation has completed on the remote side
            let _ =
                unsafe { UcxMtAlloc::put_inner(&self, pe, offset, src.as_slice(), false, false) };
        } else {
            self.as_mut_slice()[offset..offset + src.len()].copy_from_slice(src.as_slice());
            // let dst = self.start() + offset;
            // if !(src.contains(&dst) || src.contains(&(dst + src.len()))) {
            //     unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst as *mut T, src.len()) };
            // } else {
            //     unsafe {
            //         std::ptr::copy(src.as_ptr(), dst as *mut T, src.len());
            //     }
            // }
        }
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        offset: usize,
    ) -> RdmaHandle<T> {
        let pes = (0..self.num_pes).collect();
        UcxMtPutFuture {
            my_pe: self.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::PutAll(pes, src),
            local_op: false,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
        }
        .into()
    }
    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize) {
        let pes = (0..self.num_pes).collect::<Vec<usize>>();
        for pe in pes {
            if pe != self.my_pe {
                // for ucx put operation waiting on the request simply ensures the input buffer is free to reuse
                // not that the operation has completed on the remote side
                unsafe {
                    UcxMtAlloc::put_inner(
                        &self,
                        pe,
                        offset,
                        std::slice::from_ref(&src),
                        false,
                        false,
                    )
                };
            } else {
                self.as_mut_slice()[offset] = src;
                // let dst = (self.start() + offset) as *mut T;
                // unsafe { dst.write(src) };
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
        let pes = (0..self.num_pes).collect();
        UcxMtPutFuture {
            my_pe: self.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::PutAllBuf(pes, src.into()),
            local_op: false,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
        }
        .into()
    }
    fn put_all_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) {
        let src = src.into();
        let pes = (0..self.num_pes).collect::<Vec<usize>>();
        for pe in pes {
            trace!(
                "putting src: {:?} dst: {:?} len: {} num bytes {}",
                src.as_ptr(),
                self.start() + offset,
                src.len(),
                src.len() * std::mem::size_of::<T>()
            );
            if pe != self.my_pe {
                // for ucx put operation waiting on the request simply ensures the input buffer is free to reuse
                // not that the operation has completed on the remote side
                let _ = unsafe {
                    UcxMtAlloc::put_inner(&self, pe, offset, src.as_slice(), false, false)
                };
            } else {
                self.as_mut_slice()[offset..offset + src.len()].copy_from_slice(src.as_slice());
                // let dst = self.start() + offset;
                // if !(src.contains(&dst) || src.contains(&(dst + src.len()))) {
                //     unsafe {
                //         std::ptr::copy_nonoverlapping(src.as_ptr(), dst as *mut T, src.len())
                //     };
                // } else {
                //     unsafe {
                //         std::ptr::copy(src.as_ptr(), dst as *mut T, src.len());
                //     }
                // }
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
        let local_op = pe == self.my_pe;
        UcxMtGetFuture {
            alloc: self.clone(),
            pe,
            offset,
            local_op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: Box::new(T::default()),
            request: None,
        }
        .into()
    }

    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        let mut val = T::default();
        let val_slice = std::slice::from_mut(&mut val);
        let _ = unsafe { self.inner_get(pe, offset, true, val_slice) };
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
        trace!(
            "get buffer ucxalloc pe: {:?} index: {:?} num_elems: {:?} alloc {:?}",
            pe,
            offset,
            len,
            self
        );
        let local_op = pe == self.my_pe;
        UcxMtGetBufferFuture {
            alloc: self.clone(),
            pe,
            offset,
            len,
            local_op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: vec![T::default(); len],
            request: None,
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
        let mut buf = vec![T::default(); len];
        let _ = unsafe { self.inner_get(pe, offset, true, buf.as_mut_slice()) };
        buf
    }

    fn get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) -> RdmaGetIntoBufferHandle<T, B> {
        let local_op = pe == self.my_pe;
        UcxMtGetIntoBufferFuture {
            my_pe: self.my_pe,
            alloc: self.clone(),
            pe,
            offset,
            local_op,
            dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
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
        if pe != self.my_pe {
            let _ = unsafe { self.inner_get(pe, offset, true, dst.as_mut_slice()) };
        } else {
            let len = dst.len();
            dst.as_mut_slice()
                .copy_from_slice(&self.as_mut_slice()[offset..(offset + len)]);
        }
    }
    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        if pe != self.my_pe {
            let _ = unsafe { UcxMtAlloc::inner_get(&self, pe, offset, false, dst.as_mut_slice()) };
        } else {
            let len = dst.len();
            dst.as_mut_slice()
                .copy_from_slice(&self.as_mut_slice()[offset..(offset + len)]);
        }
    }
}

impl CommAllocRdma for OneSidedUcxMtAlloc {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "put called on OneSidedUcxMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );

        UcxMtPutFuture {
            my_pe: self.alloc.my_pe,
            alloc: self.alloc.clone(),
            offset,
            op: AllocOp::Put(pe, src.into()),
            local_op: pe == self.alloc.my_pe,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
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
            "put_blocking called on OneSidedUcxMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        if pe != self.alloc.my_pe {
            let _ = unsafe {
                UcxMtAlloc::put_inner(
                    &self.alloc,
                    pe,
                    offset,
                    std::slice::from_ref(&src),
                    true,
                    true,
                )
            };
        } else {
            self.alloc.as_mut_slice()[offset] = src;
        }
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        assert_eq!(
            pe, self.remote_pe,
            "put_unmanaged called on OneSidedUcxMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        if pe != self.alloc.my_pe {
            // for ucx put operation waiting on the request simply ensures the input buffer is free to reuse
            // not that the operation has completed on the remote side
            let _ = unsafe {
                UcxMtAlloc::put_inner(
                    &self.alloc,
                    pe,
                    offset,
                    std::slice::from_ref(&src),
                    false,
                    false,
                )
            };
        } else {
            self.alloc.as_mut_slice()[offset] = src;
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
        assert_eq!(
            pe, self.remote_pe,
            "put_buffer called on OneSidedUcxMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );

        UcxMtPutFuture {
            my_pe: self.alloc.my_pe,
            alloc: self.alloc.clone(),
            offset,
            op: AllocOp::PutBuf(pe, src.into()),
            local_op: pe == self.alloc.my_pe,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
        }
        .into()
    }
    fn put_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) {
        assert_eq!(
            pe, self.remote_pe,
            "put_buffer_unmanaged called on OneSidedUcxMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let src = src.into();
        if pe != self.alloc.my_pe {
            // for ucx put operation waiting on the request simply ensures the input buffer is free to reuse
            // not that the operation has completed on the remote side
            let _ = unsafe {
                UcxMtAlloc::put_inner(&self.alloc, pe, offset, src.as_slice(), false, false)
            };
        } else {
            self.alloc.as_mut_slice()[offset..offset + src.len()].copy_from_slice(src.as_slice());
        }
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        offset: usize,
    ) -> RdmaHandle<T> {
        self.put(scheduler, counters, src, self.remote_pe, offset)
    }
    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize) {
        self.put_unmanaged(src, self.remote_pe, offset)
    }
    fn put_all_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
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
        self.put_buffer_unmanaged(src, self.remote_pe, offset)
    }

    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "get called on OneSidedUcxMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxMtGetFuture {
            alloc: self.alloc.clone(),
            pe,
            offset,
            local_op: pe == self.alloc.my_pe,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: Box::new(T::default()),
            request: None,
        }
        .into()
    }

    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        assert_eq!(
            pe, self.remote_pe,
            "blocking_get called on OneSidedUcxMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut val = T::default();
        let val_slice = std::slice::from_mut(&mut val);
        let _ = unsafe { self.alloc.inner_get(pe, offset, true, val_slice) };
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
        assert_eq!(
            pe, self.remote_pe,
            "get_buffer called on OneSidedUcxMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxMtGetBufferFuture {
            alloc: self.alloc.clone(),
            pe,
            offset,
            len,
            local_op: pe == self.alloc.my_pe,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: vec![T::default(); len],
            request: None,
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
            "blocking_get_buffer called on OneSidedUcxMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut dst = vec![T::default(); len];
        unsafe {
            dst.set_len(len);
            if pe != self.alloc.my_pe {
                let _ = self.alloc.inner_get(pe, offset, true, dst.as_mut_slice());
            } else {
                dst.as_mut_slice()
                    .copy_from_slice(&self.alloc.as_mut_slice()[offset..(offset + len)]);
            }
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
        assert_eq!(
            pe, self.remote_pe,
            "get_into_buffer called on OneSidedUcxMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxMtGetIntoBufferFuture {
            my_pe: self.alloc.my_pe,
            alloc: self.alloc.clone(),
            pe,
            offset,
            local_op: pe == self.alloc.my_pe,
            dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
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
            "blocking_get_into_buffer called on OneSidedUcxMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        if pe != self.alloc.my_pe {
            let _ =
                unsafe { UcxMtAlloc::inner_get(&self.alloc, pe, offset, true, dst.as_mut_slice()) };
        } else {
            let len = dst.len();
            dst.as_mut_slice()
                .copy_from_slice(&self.alloc.as_mut_slice()[offset..(offset + len)]);
        }
    }
    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        assert_eq!(
            pe, self.remote_pe,
            "get_into_buffer_unmanaged called on OneSidedUcxMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        if pe != self.alloc.my_pe {
            let _ = unsafe {
                UcxMtAlloc::inner_get(&self.alloc, pe, offset, false, dst.as_mut_slice())
            };
        } else {
            let len = dst.len();
            dst.as_mut_slice()
                .copy_from_slice(&self.alloc.as_mut_slice()[offset..(offset + len)]);
        }
    }
}

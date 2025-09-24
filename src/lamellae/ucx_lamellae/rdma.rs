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
        comm::rdma::{
            RdmaGetBufferFuture, RdmaGetBufferHandle, RdmaGetFuture, RdmaGetHandle,
            RdmaGetIntoBufferFuture, RdmaGetIntoBufferHandle, RdmaHandle, RdmaPutFuture, Remote,
        },
        CommAllocRdma, CommSlice,
    },
    memregion::{AsLamellarBuffer, LamellarBuffer, MemregionRdmaInputInner},
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::{
    fabric::{UcxAlloc, UcxRequest},
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
pub(crate) struct UcxPutFuture<T: Remote> {
    my_pe: usize,
    alloc: Arc<UcxAlloc>,
    offset: usize,
    op: AllocOp<T>,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
    request: Option<UcxRequest>,
}

impl<T: Remote> UcxPutFuture<T> {
    fn inner_put(&mut self, pe: usize, src: T) {
        if pe != self.my_pe {
            self.request = unsafe {
                UcxAlloc::put_inner(
                    &self.alloc,
                    pe,
                    self.offset,
                    std::slice::from_ref(&src),
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
            self.request =
                unsafe { UcxAlloc::put_inner(&self.alloc, pe, self.offset, src.as_slice(), true) };
        } else {
            self.alloc.as_mut_slice()[self.offset..self.offset + src.len()]
                .copy_from_slice(src.as_slice());
            // let dst = self.alloc.start() + self.offset;
            // if !(src.contains(&dst) || src.contains(&(dst + src.len()))) {
            //     unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst as *mut T, src.len()) };
            // } else {
            //     unsafe {
            //         std::ptr::copy(src.as_ptr(), dst as *mut T, src.len());
            //     }
            // }
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
            request.wait();
        } else {
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
                    request.wait();
                } else {
                    self.alloc.wait_all();
                }
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for UcxPutFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<UcxPutFuture<T>> for RdmaHandle<T> {
    fn from(f: UcxPutFuture<T>) -> RdmaHandle<T> {
        RdmaHandle {
            future: RdmaPutFuture::Ucx(f),
        }
    }
}

impl<T: Remote> Future for UcxPutFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        *this.spawned = true;
        if let Some(request) = this.request.take() {
            request.wait();
        } else {
            this.alloc.wait_all();
        }

        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxGetFuture<T> {
    alloc: Arc<UcxAlloc>,
    pe: usize,
    offset: usize,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
    result: MaybeUninit<T>,
    request: Option<UcxRequest>,
}

impl<T: Remote> UcxGetFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        unsafe {
            self.request = Some(self.alloc.inner_get(
                self.pe,
                self.offset,
                std::slice::from_raw_parts_mut(self.result.as_mut_ptr(), 1),
            ));
        }
    }

    pub(crate) fn block(mut self) -> T {
        self.exec_at();
        self.spawned = true;
        if let Some(request) = self.request.take() {
            request.wait();
        } else {
            self.alloc.wait_all();
        }
        unsafe { self.result.assume_init() }
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_at();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move {
                if let Some(request) = self.request.take() {
                    request.wait();
                } else {
                    self.alloc.wait_all();
                }
                unsafe { self.result.assume_init() }
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for UcxGetFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<UcxGetFuture<T>> for RdmaGetHandle<T> {
    fn from(f: UcxGetFuture<T>) -> RdmaGetHandle<T> {
        RdmaGetHandle {
            future: RdmaGetFuture::Ucx(f),
        }
    }
}

impl<T: Remote> Future for UcxGetFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        let this = self.project();
        *this.spawned = true;
        if let Some(request) = this.request.take() {
            request.wait();
        } else {
            this.alloc.wait_all();
        }

        Poll::Ready(unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(this.result, &mut res);
            res.assume_init()
        })
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxGetBufferFuture<T> {
    alloc: Arc<UcxAlloc>,
    pe: usize,
    offset: usize,
    len: usize,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
    result: MaybeUninit<Vec<T>>,
    request: Option<UcxRequest>,
}

impl<T: Remote> UcxGetBufferFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        unsafe {
            let mut dst = Vec::<T>::with_capacity(self.len);
            dst.set_len(self.len);
            self.request = Some(
                self.alloc
                    .inner_get(self.pe, self.offset, dst.as_mut_slice()),
            );
            self.result.write(dst);
        }
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_at();
        self.spawned = true;
        if let Some(request) = self.request.take() {
            request.wait();
        } else {
            self.alloc.wait_all();
        }
        let mut res = MaybeUninit::uninit();
        std::mem::swap(&mut self.result, &mut res);
        unsafe { res.assume_init() }
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_at();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move {
                if let Some(request) = self.request.take() {
                    request.wait();
                } else {
                    self.alloc.wait_all();
                }
                let mut res = MaybeUninit::uninit();
                std::mem::swap(&mut self.result, &mut res);
                unsafe { res.assume_init() }
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for UcxGetBufferFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<UcxGetBufferFuture<T>> for RdmaGetBufferHandle<T> {
    fn from(f: UcxGetBufferFuture<T>) -> RdmaGetBufferHandle<T> {
        RdmaGetBufferHandle {
            future: RdmaGetBufferFuture::Ucx(f),
        }
    }
}

impl<T: Remote> Future for UcxGetBufferFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        let this = self.project();
        *this.spawned = true;
        if let Some(request) = this.request.take() {
            request.wait();
        } else {
            this.alloc.wait_all();
        }

        Poll::Ready(unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(this.result, &mut res);
            res.assume_init()
        })
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxGetIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    my_pe: usize,
    alloc: Arc<UcxAlloc>,
    pe: usize,
    offset: usize,
    dst: LamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
    request: Option<UcxRequest>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxGetIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        if self.pe != self.my_pe {
            self.request = Some(unsafe {
                UcxAlloc::inner_get(&self.alloc, self.pe, self.offset, self.dst.as_mut_slice())
            });
        } else {
            let len = self.dst.len();
            unsafe {
                self.dst
                    .as_mut_slice()
                    .copy_from_slice(&self.alloc.as_mut_slice()[self.offset..(self.offset + len)])
            };
            // let src = self.alloc.start() + self.offset;
            // if !(dst.contains(&src) || dst.contains(&(src + dst.len()))) {
            //     unsafe {
            //         std::ptr::copy_nonoverlapping(src as *const T, dst.as_mut_ptr(), dst.len());
            //     }
            // } else {
            //     unsafe {
            //         std::ptr::copy(src as *const T, dst.as_mut_ptr(), dst.len());
            //     }
            // }
        }
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        if let Some(request) = self.request.take() {
            request.wait();
        } else {
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
        self.scheduler.clone().spawn_task(
            async move {
                match request {
                    Some(request) => request.wait().expect("ucx get failed"),
                    None => alloc.wait_all(),
                };
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxGetIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<UcxGetIntoBufferFuture<T, B>>
    for RdmaGetIntoBufferHandle<T, B>
{
    fn from(f: UcxGetIntoBufferFuture<T, B>) -> RdmaGetIntoBufferHandle<T, B> {
        RdmaGetIntoBufferHandle {
            future: RdmaGetIntoBufferFuture::Ucx(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxGetIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        *this.spawned = true;
        if let Some(request) = this.request.take() {
            request.wait();
        } else {
            this.alloc.wait_all();
        }

        Poll::Ready(())
    }
}

impl CommAllocRdma for Arc<UcxAlloc> {
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
        UcxPutFuture {
            my_pe: self.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::Put(pe, src.into()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
        }
        .into()
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        if pe != self.my_pe {
            // for ucx put operation waiting on the request simply ensures the input buffer is free to reuse
            // not that the operation has completed on the remote side
            unsafe { UcxAlloc::put_inner(&self, pe, offset, std::slice::from_ref(&src), false) };
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

        UcxPutFuture {
            my_pe: self.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::PutBuf(pe, src.into()),
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
            unsafe { UcxAlloc::put_inner(&self, pe, offset, src.as_slice(), false) };
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
        UcxPutFuture {
            my_pe: self.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::PutAll(pes, src),
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
                    UcxAlloc::put_inner(&self, pe, offset, std::slice::from_ref(&src), false)
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
        UcxPutFuture {
            my_pe: self.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::PutAllBuf(pes, src.into()),
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
                unsafe { UcxAlloc::put_inner(&self, pe, offset, src.as_slice(), false) };
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
        UcxGetFuture {
            alloc: self.clone(),
            pe,
            offset,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: MaybeUninit::uninit(),
            request: None,
        }
        .into()
    }

    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        len: usize,
        pe: usize,
        offset: usize,
    ) -> RdmaGetBufferHandle<T> {
        UcxGetBufferFuture {
            alloc: self.clone(),
            pe,
            offset,
            len,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: MaybeUninit::uninit(),
            request: None,
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
        UcxGetIntoBufferFuture {
            my_pe: self.my_pe,
            alloc: self.clone(),
            pe,
            offset,
            dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
        }
        .into()
    }
    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        if pe != self.my_pe {
            let _ = unsafe { UcxAlloc::inner_get(&self, pe, offset, dst.as_mut_slice()) };
        } else {
            let len = dst.len();
            unsafe {
                dst.as_mut_slice()
                    .copy_from_slice(&self.as_mut_slice()[offset..(offset + len)]);
            }
            // let src = self.alloc.start() + self.offset;
            // if !(dst.contains(&src) || dst.contains(&(src + dst.len()))) {
            //     unsafe {
            //         std::ptr::copy_nonoverlapping(src as *const T, dst.as_mut_ptr(), dst.len());
            //     }
            // } else {
            //     unsafe {
            //         std::ptr::copy(src as *const T, dst.as_mut_ptr(), dst.len());
            //     }
            // }
        }
    }
}

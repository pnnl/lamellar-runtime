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
    fabric::{OneSidedUcxAlloc, UcxAlloc, UcxRequest},
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
    alloc: UcxAlloc,
    offset: usize,
    op: AllocOp<T>,
    local_op: bool,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    request: Option<UcxRequest>,
}

impl<T: Remote> UcxPutFuture<T> {
    fn inner_put(&mut self, pe: usize, src: T) {
        self.request = unsafe {
            UcxAlloc::put_inner(
                &self.alloc,
                pe,
                self.offset,
                std::slice::from_ref(&src),
                false,
                true,
            )
        };
    }
    fn inner_put_buf(&mut self, pe: usize, src: &MemregionRdmaInputInner<T>) {
        trace!(
            "putting src: {:?} dst: {:?} len: {} num bytes {}",
            src.as_ptr(),
            self.alloc.start() + self.offset,
            src.len(),
            src.len() * std::mem::size_of::<T>()
        );
        self.request = unsafe {
            UcxAlloc::put_inner(&self.alloc, pe, self.offset, src.as_slice(), false, true)
        };
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn inner_put_all(&mut self, pes: &Vec<usize>, src: T) {
        for pe in pes {
            self.inner_put(*pe, src);
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
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
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(
            async move {
                if let Some(mut request) = self.request.take() {
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
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
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
pub(crate) struct UcxGetFuture<T> {
    alloc: UcxAlloc,
    pe: usize,
    offset: usize,
    local_op: bool,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    result: Box<T>,
    request: Option<UcxRequest>,
}

impl<T: Remote> UcxGetFuture<T> {
    //#[tracing::instrument(skip_all, level = "debug")]
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
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(
            async move {
                if let Some(mut request) = self.request.take() {
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
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
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
pub(crate) struct UcxGetBufferFuture<T> {
    alloc: UcxAlloc,
    pe: usize,
    offset: usize,
    len: usize,
    local_op: bool,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    result: Vec<T>,
    request: Option<UcxRequest>,
}

impl<T: Remote> UcxGetBufferFuture<T> {
    //#[tracing::instrument(skip_all, level = "debug")]
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
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(
            async move {
                if let Some(mut request) = self.request.take() {
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
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
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
pub(crate) struct UcxGetIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    alloc: UcxAlloc,
    pe: usize,
    offset: usize,
    local_op: bool,
    dst: LamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    request: Option<UcxRequest>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxGetIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        self.request = unsafe {
            UcxAlloc::inner_get(
                &self.alloc,
                self.pe,
                self.offset,
                false,
                self.dst.as_mut_slice(),
            )
        };
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
        let counters = self.counters.clone();
        let request = self.request.take();
        let alloc = self.alloc.clone();
        let local_op = self.local_op;
        self.scheduler.clone().spawn_task(
            async move {
                match request {
                    Some(mut request) => request.wait().expect("ucx get failed"),
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
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
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

impl CommAllocRdma for UcxAlloc {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
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
        UcxPutFuture {
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
        let _ = unsafe {
            UcxAlloc::put_inner(&self, pe, offset, std::slice::from_ref(&src), true, true)
        };
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        trace!(
            "put unmanaged to dst: {:x} offset<T>: {:?} final addr {:x} len: 1 num bytes {}",
            self.start(),
            offset,
            self.start() + offset,
            std::mem::size_of::<T>()
        );
        // for ucx put operation waiting on the request simply ensures the input buffer is free to reuse
        // not that the operation has completed on the remote side
        let _ = unsafe {
            UcxAlloc::put_inner(&self, pe, offset, std::slice::from_ref(&src), false, false)
        };
    }
    fn put_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        //  self.put_amt
        //     .fetch_add(src.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
        let local_op = pe == self.my_pe;
        UcxPutFuture {
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
        // for ucx put operation waiting on the request simply ensures the input buffer is free to reuse
        // not that the operation has completed on the remote side
        let _ = unsafe { UcxAlloc::put_inner(&self, pe, offset, src.as_slice(), false, false) };
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        offset: usize,
    ) -> RdmaHandle<T> {
        let pes = (0..self.num_pes).collect();
        UcxPutFuture {
            alloc: self.clone(),
            offset,
            op: AllocOp::PutAll(pes, src),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            local_op: false,
            request: None,
        }
        .into()
    }
    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize) {
        let pes = (0..self.num_pes).collect::<Vec<usize>>();
        for pe in pes {
            // for ucx put operation waiting on the request simply ensures the input buffer is free to reuse
            // not that the operation has completed on the remote side
            let _ = unsafe {
                UcxAlloc::put_inner(&self, pe, offset, std::slice::from_ref(&src), false, false)
            };
        }
    }
    fn put_all_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) -> RdmaHandle<T> {
        let pes = (0..self.num_pes).collect();
        UcxPutFuture {
            alloc: self.clone(),
            offset,
            op: AllocOp::PutAllBuf(pes, src.into()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            local_op: false,
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
            // for ucx put operation waiting on the request simply ensures the input buffer is free to reuse
            // not that the operation has completed on the remote side
            let _ = unsafe { UcxAlloc::put_inner(&self, pe, offset, src.as_slice(), false, false) };
        }
    }

    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        let local_op = pe == self.my_pe;
        UcxGetFuture {
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
        counters: Option<Arc<[Arc<AMCounters>]>>,
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
        UcxGetBufferFuture {
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
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) -> RdmaGetIntoBufferHandle<T, B> {
        let local_op = pe == self.my_pe;
        UcxGetIntoBufferFuture {
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
        let _ = unsafe { self.inner_get(pe, offset, true, dst.as_mut_slice()) };
    }
    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        let _ = unsafe { UcxAlloc::inner_get(&self, pe, offset, false, dst.as_mut_slice()) };
    }
}

impl CommAllocRdma for OneSidedUcxAlloc {
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
            "put called on OneSidedUcxAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );

        UcxPutFuture {
            alloc: self.alloc.clone(),
            offset,
            op: AllocOp::Put(pe, src.into()),
            local_op: false,
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
            "put_blocking called on OneSidedUcxAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let _ = unsafe {
            UcxAlloc::put_inner(
                &self.alloc,
                pe,
                offset,
                std::slice::from_ref(&src),
                true,
                true,
            )
        };
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        assert_eq!(
            pe, self.remote_pe,
            "put_unmanaged called on OneSidedUcxAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        // for ucx put operation waiting on the request simply ensures the input buffer is free to reuse
        // not that the operation has completed on the remote side
        let _ = unsafe {
            UcxAlloc::put_inner(
                &self.alloc,
                pe,
                offset,
                std::slice::from_ref(&src),
                false,
                false,
            )
        };
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
            "put_buffer called on OneSidedUcxAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );

        UcxPutFuture {
            alloc: self.alloc.clone(),
            offset,
            op: AllocOp::PutBuf(pe, src.into()),
            local_op: false,
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
            "put_buffer_unmanaged called on OneSidedUcxAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let src = src.into();
        // for ucx put operation waiting on the request simply ensures the input buffer is free to reuse
        // not that the operation has completed on the remote side
        let _ =
            unsafe { UcxAlloc::put_inner(&self.alloc, pe, offset, src.as_slice(), false, false) };
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
        self.put_unmanaged(src, self.remote_pe, offset)
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
        self.put_buffer_unmanaged(src, self.remote_pe, offset)
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
            "get called on OneSidedUcxAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxGetFuture {
            alloc: self.alloc.clone(),
            pe,
            offset,
            local_op: false,
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
            "blocking_get called on OneSidedUcxAlloc with incorrect pe: {} expected pe: {}",
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
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> RdmaGetBufferHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "get_buffer called on OneSidedUcxAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxGetBufferFuture {
            alloc: self.alloc.clone(),
            pe,
            offset,
            len,
            local_op: false,
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
            "blocking_get_buffer called on OneSidedUcxAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut dst = vec![T::default(); len];
        unsafe {
            dst.set_len(len);
            let _ = self.alloc.inner_get(pe, offset, true, dst.as_mut_slice());
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
        assert_eq!(
            pe, self.remote_pe,
            "get_into_buffer called on OneSidedUcxAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxGetIntoBufferFuture {
            alloc: self.alloc.clone(),
            pe,
            offset,
            local_op: false,
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
            "blocking_get_into_buffer called on OneSidedUcxAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let _ = unsafe { UcxAlloc::inner_get(&self.alloc, pe, offset, true, dst.as_mut_slice()) };
    }
    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        assert_eq!(
            pe, self.remote_pe,
            "get_into_buffer_unmanaged called on OneSidedUcxAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let _ = unsafe { UcxAlloc::inner_get(&self.alloc, pe, offset, false, dst.as_mut_slice()) };
    }
}

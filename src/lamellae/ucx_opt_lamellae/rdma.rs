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
    fabric::{AllocFlushState, OneSidedUcxOptAlloc, UcxOptAlloc, UcxOptRequest},
    Scheduler,
};

#[derive(Clone)]
pub(super) enum AllocOp<T: Remote> {
    // Scalar sources are boxed: a ticketed put reads the source at DMA-completion
    // time, which happens after `exec_op` returns and the future has potentially
    // been moved (into `spawn_task`) while the op is in flight -- an inline `T`
    // would leave the NIC reading a stale, already-popped stack frame.
    Put(usize, Box<T>),
    PutBuf(usize, MemregionRdmaInputInner<T>),
    PutAll(Vec<usize>, Box<T>),
    PutAllBuf(Vec<usize>, MemregionRdmaInputInner<T>),
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptPutFuture<T: Remote> {
    alloc: UcxOptAlloc,
    offset: usize,
    op: AllocOp<T>,
    local_op: bool,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    // One entry per issued RDMA op. `PutAll`/`PutAllBuf` issue one op per destination
    // PE, and every op's completion must be tracked -- a single `Option` slot here
    // previously dropped (and thus never awaited) every PE's request but the last.
    requests: Vec<UcxOptRequest>,
    flush_state: AllocFlushState,
}

impl<T: Remote> UcxOptPutFuture<T> {
    // `src` is a raw pointer into the `Box<T>` owned by `self.op`, which lives as
    // long as this future does -- never a by-value copy that could be popped off
    // some other stack frame before the (possibly deferred) local completion.
    fn inner_put(&mut self, pe: usize, src: *const T) {
        if let Some(request) = unsafe {
            UcxOptAlloc::put_inner(
                &self.alloc,
                pe,
                self.offset,
                std::slice::from_raw_parts(src, 1),
                false,
                true,
            )
        } {
            self.requests.push(request);
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
        if let Some(request) =
            unsafe { UcxOptAlloc::put_inner(&self.alloc, pe, self.offset, src.as_slice(), false, true) }
        {
            self.requests.push(request);
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn inner_put_all(&mut self, pes: &[usize], src: *const T) {
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
        match &self.op {
            AllocOp::Put(pe, src) => {
                let pe = *pe;
                let ptr: *const T = &**src;
                self.inner_put(pe, ptr);
            }
            AllocOp::PutAll(pes, src) => {
                let pes = pes.clone();
                let ptr: *const T = &**src;
                self.inner_put_all(&pes, ptr);
            }
            AllocOp::PutBuf(pe, src) => {
                let pe = *pe;
                let src = src.clone();
                self.inner_put_buf(pe, &src);
            }
            AllocOp::PutAllBuf(pes, src) => {
                let pes = pes.clone();
                let src = src.clone();
                self.inner_put_all_buf(&pes, &src);
            }
        }
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        if !self.requests.is_empty() {
            for request in self.requests.drain(..) {
                request.wait().expect("ucx put failed");
            }
        } else if !self.local_op {
            self.alloc.wait_all();
        }
        self.spawned = true;
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        self.spawned = true;
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for UcxOptPutFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<UcxOptPutFuture<T>> for RdmaHandle<T> {
    fn from(f: UcxOptPutFuture<T>) -> RdmaHandle<T> {
        RdmaHandle {
            future: RdmaPutFuture::UcxOpt(f),
        }
    }
}

impl<T: Remote> Future for UcxOptPutFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        *this.spawned = true;
        if !this.requests.is_empty() {
            let mut pending = false;
            this.requests.retain_mut(|request| match request.poll_wait(cx) {
                Poll::Pending => {
                    pending = true;
                    true
                }
                Poll::Ready(Ok(())) => false,
                Poll::Ready(Err(e)) => panic!("ucx put failed: {:?}", e),
            });
            if pending {
                // D5/U3: `poll_wait` already self-rewakes when it couldn't take the progress
                // lock, and otherwise trusts the ticket's registered waker -- don't hammer the
                // executor with an unconditional rewake on top of that here.
                return Poll::Pending;
            }
        } else if !*this.local_op {
            match this.alloc.poll_wait_all(this.flush_state) {
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => panic!("ucx put failed: {:?}", e),
            }
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptGetFuture<T> {
    alloc: UcxOptAlloc,
    pe: usize,
    offset: usize,
    local_op: bool,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    result: Box<T>,
    request: Option<UcxOptRequest>,
    flush_state: AllocFlushState,
}

impl<T: Remote> UcxOptGetFuture<T> {
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
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for UcxOptGetFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<UcxOptGetFuture<T>> for RdmaGetHandle<T> {
    fn from(f: UcxOptGetFuture<T>) -> RdmaGetHandle<T> {
        RdmaGetHandle {
            future: RdmaGetFuture::UcxOpt(f),
        }
    }
}

impl<T: Remote> Future for UcxOptGetFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        let this = self.project();
        if let Some(request) = this.request.as_mut() {
            match request.poll_wait(cx) {
                // D5/U3: `poll_wait` already self-rewakes when it couldn't take the progress
                // lock, and otherwise trusts the ticket's registered waker.
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => panic!("ucx get failed: {:?}", e),
            }
        } else if !*this.local_op {
            match this.alloc.poll_wait_all(this.flush_state) {
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => panic!("ucx get failed: {:?}", e),
            }
        }

        Poll::Ready(**this.result)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptGetBufferFuture<T> {
    alloc: UcxOptAlloc,
    pe: usize,
    offset: usize,
    len: usize,
    local_op: bool,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    result: Vec<T>,
    request: Option<UcxOptRequest>,
    flush_state: AllocFlushState,
}

impl<T: Remote> UcxOptGetBufferFuture<T> {
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
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for UcxOptGetBufferFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<UcxOptGetBufferFuture<T>> for RdmaGetBufferHandle<T> {
    fn from(f: UcxOptGetBufferFuture<T>) -> RdmaGetBufferHandle<T> {
        RdmaGetBufferHandle {
            future: RdmaGetBufferFuture::UcxOpt(f),
        }
    }
}

impl<T: Remote> Future for UcxOptGetBufferFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_get();
        }
        let this = self.project();
        *this.spawned = true;
        if let Some(request) = this.request.as_mut() {
            match request.poll_wait(cx) {
                // D5/U3: `poll_wait` already self-rewakes when it couldn't take the progress
                // lock, and otherwise trusts the ticket's registered waker.
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => panic!("ucx get buffer failed: {:?}", e),
            }
        } else if !*this.local_op {
            match this.alloc.poll_wait_all(this.flush_state) {
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => panic!("ucx get buffer failed: {:?}", e),
            }
        }

        Poll::Ready(std::mem::take(this.result))
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxOptGetIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    alloc: UcxOptAlloc,
    pe: usize,
    offset: usize,
    local_op: bool,
    dst: LamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    request: Option<UcxOptRequest>,
    flush_state: AllocFlushState,
}

impl<T: Remote, B: AsLamellarBuffer<T>> UcxOptGetIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        self.request = unsafe {
            UcxOptAlloc::inner_get(
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
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for UcxOptGetIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<UcxOptGetIntoBufferFuture<T, B>>
    for RdmaGetIntoBufferHandle<T, B>
{
    fn from(f: UcxOptGetIntoBufferFuture<T, B>) -> RdmaGetIntoBufferHandle<T, B> {
        RdmaGetIntoBufferHandle {
            future: RdmaGetIntoBufferFuture::UcxOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for UcxOptGetIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        *this.spawned = true;
        if let Some(request) = this.request.as_mut() {
            match request.poll_wait(cx) {
                // D5/U3: `poll_wait` already self-rewakes when it couldn't take the progress
                // lock, and otherwise trusts the ticket's registered waker.
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => panic!("ucx get into buffer failed: {:?}", e),
            }
        } else if !*this.local_op {
            match this.alloc.poll_wait_all(this.flush_state) {
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => panic!("ucx get into buffer failed: {:?}", e),
            }
        }

        Poll::Ready(())
    }
}

impl CommAllocRdma for UcxOptAlloc {
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
        UcxOptPutFuture {
            alloc: self.clone(),
            offset,
            op: AllocOp::Put(pe, Box::new(src)),
            local_op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            requests: Vec::new(),
            flush_state: AllocFlushState::default(),
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
            UcxOptAlloc::put_inner(&self, pe, offset, std::slice::from_ref(&src), true, true)
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
            UcxOptAlloc::put_inner(&self, pe, offset, std::slice::from_ref(&src), false, false)
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
        UcxOptPutFuture {
            alloc: self.clone(),
            offset,
            op: AllocOp::PutBuf(pe, src.into()),
            local_op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            requests: Vec::new(),
            flush_state: AllocFlushState::default(),
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
        // Waiting on the request only ensures the input buffer is free to reuse, not that the put has
        // completed on the remote side. Registered sources of at least UNMANAGED_NOWAIT_MIN_BYTES are not
        // even waited on: they must stay valid until the next `wait_all`, which drains the put.
        let _ = unsafe { UcxOptAlloc::put_inner_unmanaged(&self, pe, offset, src.as_slice(), src.is_registered()) };
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        offset: usize,
    ) -> RdmaHandle<T> {
        let pes = (0..self.num_pes).collect();
        UcxOptPutFuture {
            alloc: self.clone(),
            offset,
            op: AllocOp::PutAll(pes, Box::new(src)),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            local_op: false,
            requests: Vec::new(),
            flush_state: AllocFlushState::default(),
        }
        .into()
    }
    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize) {
        let pes = (0..self.num_pes).collect::<Vec<usize>>();
        for pe in pes {
            // for ucx put operation waiting on the request simply ensures the input buffer is free to reuse
            // not that the operation has completed on the remote side
            let _ = unsafe {
                UcxOptAlloc::put_inner(&self, pe, offset, std::slice::from_ref(&src), false, false)
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
        UcxOptPutFuture {
            alloc: self.clone(),
            offset,
            op: AllocOp::PutAllBuf(pes, src.into()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            local_op: false,
            requests: Vec::new(),
            flush_state: AllocFlushState::default(),
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
            // Waiting on the request only ensures the input buffer is free to reuse, not that the put has
            // completed on the remote side. Registered sources of at least UNMANAGED_NOWAIT_MIN_BYTES are not
            // even waited on: they must stay valid until the next `wait_all`, which drains the put.
            let _ = unsafe { UcxOptAlloc::put_inner_unmanaged(&self, pe, offset, src.as_slice(), src.is_registered()) };
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
        UcxOptGetFuture {
            alloc: self.clone(),
            pe,
            offset,
            local_op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: Box::new(unsafe { std::mem::zeroed() }),
            request: None,
            flush_state: AllocFlushState::default(),
        }
        .into()
    }

    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        let mut val: T = unsafe { std::mem::zeroed() };
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
        UcxOptGetBufferFuture {
            alloc: self.clone(),
            pe,
            offset,
            len,
            local_op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: (0..len).map(|_| unsafe { std::mem::zeroed() }).collect(),
            request: None,
            flush_state: AllocFlushState::default(),
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
        let mut buf: Vec<T> = (0..len).map(|_| unsafe { std::mem::zeroed() }).collect();
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
        UcxOptGetIntoBufferFuture {
            alloc: self.clone(),
            pe,
            offset,
            local_op,
            dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
            flush_state: AllocFlushState::default(),
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
        unsafe { UcxOptAlloc::inner_get_unmanaged(&self, pe, offset, dst.as_mut_slice()) };
    }
}

impl CommAllocRdma for OneSidedUcxOptAlloc {
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
            "put called on OneSidedUcxOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );

        UcxOptPutFuture {
            alloc: self.alloc.clone(),
            offset,
            op: AllocOp::Put(pe, Box::new(src)),
            local_op: false,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            requests: Vec::new(),
            flush_state: AllocFlushState::default(),
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
            "put_blocking called on OneSidedUcxOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let _ = unsafe {
            UcxOptAlloc::put_inner(
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
            "put_unmanaged called on OneSidedUcxOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        // for ucx put operation waiting on the request simply ensures the input buffer is free to reuse
        // not that the operation has completed on the remote side
        let _ = unsafe {
            UcxOptAlloc::put_inner(
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
            "put_buffer called on OneSidedUcxOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );

        UcxOptPutFuture {
            alloc: self.alloc.clone(),
            offset,
            op: AllocOp::PutBuf(pe, src.into()),
            local_op: false,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            requests: Vec::new(),
            flush_state: AllocFlushState::default(),
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
            "put_buffer_unmanaged called on OneSidedUcxOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let src = src.into();
        // Waiting on the request only ensures the input buffer is free to reuse, not that the put has
        // completed on the remote side. Registered sources of at least UNMANAGED_NOWAIT_MIN_BYTES are not
        // even waited on: they must stay valid until the next `wait_all`, which drains the put.
        let _ = unsafe {
            UcxOptAlloc::put_inner_unmanaged(&self.alloc, pe, offset, src.as_slice(), src.is_registered())
        };
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
            "get called on OneSidedUcxOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxOptGetFuture {
            alloc: self.alloc.clone(),
            pe,
            offset,
            local_op: false,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: Box::new(unsafe { std::mem::zeroed() }),
            request: None,
            flush_state: AllocFlushState::default(),
        }
        .into()
    }

    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        assert_eq!(
            pe, self.remote_pe,
            "blocking_get called on OneSidedUcxOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut val: T = unsafe { std::mem::zeroed() };
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
            "get_buffer called on OneSidedUcxOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxOptGetBufferFuture {
            alloc: self.alloc.clone(),
            pe,
            offset,
            len,
            local_op: false,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: (0..len).map(|_| unsafe { std::mem::zeroed() }).collect(),
            request: None,
            flush_state: AllocFlushState::default(),
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
            "blocking_get_buffer called on OneSidedUcxOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut dst: Vec<T> = (0..len).map(|_| unsafe { std::mem::zeroed() }).collect();
        let _ = unsafe { self.alloc.inner_get(pe, offset, true, dst.as_mut_slice()) };
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
            "get_into_buffer called on OneSidedUcxOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        UcxOptGetIntoBufferFuture {
            alloc: self.alloc.clone(),
            pe,
            offset,
            local_op: false,
            dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
            flush_state: AllocFlushState::default(),
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
            "blocking_get_into_buffer called on OneSidedUcxOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let _ = unsafe { UcxOptAlloc::inner_get(&self.alloc, pe, offset, true, dst.as_mut_slice()) };
    }
    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        assert_eq!(
            pe, self.remote_pe,
            "get_into_buffer_unmanaged called on OneSidedUcxOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        unsafe { UcxOptAlloc::inner_get_unmanaged(&self.alloc, pe, offset, dst.as_mut_slice()) };
    }
}

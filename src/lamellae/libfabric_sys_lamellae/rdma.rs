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
    fabric::{LibfabricSysAlloc, OneSidedLibfabricSysAlloc},
    Scheduler,
};

pub(super) enum AllocOp<T: Remote> {
    Put(usize, T),
    PutBuf(usize, MemregionRdmaInputInner<T>),
    PutAll(Vec<usize>, T),
    PutAllBuf(Vec<usize>, MemregionRdmaInputInner<T>),
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysPutFuture<T: Remote> {
    my_pe: usize,
    alloc: LibfabricSysAlloc,
    offset: usize,
    op: AllocOp<T>,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
}

impl<T: Remote> LibfabricSysPutFuture<T> {
    fn inner_put(&self, pe: usize, src: &T) {
        trace!(
            "putting src: {:x} dst: {:x} len: {} num bytes {}",
            src as *const T as usize,
            self.alloc.start() + self.offset,
            1,
            std::mem::size_of::<T>()
        );
        unsafe {
            LibfabricSysAlloc::inner_put(
                &self.alloc,
                pe,
                self.offset,
                std::slice::from_ref(src),
                false,
            );
        };
    }
    fn inner_put_buf(&self, pe: usize, src: &MemregionRdmaInputInner<T>) {
        unsafe {
            LibfabricSysAlloc::inner_put(&self.alloc, pe, self.offset, src.as_slice(), false);
        };
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn inner_put_all(&self, pes: &Vec<usize>, src: &T) {
        for pe in pes {
            self.inner_put(*pe, src);
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn inner_put_all_buf(&self, pes: &Vec<usize>, src: &MemregionRdmaInputInner<T>) {
        for pe in pes {
            self.inner_put_buf(*pe, src);
        }
    }

    fn exec_op(&mut self) {
        match &self.op {
            AllocOp::Put(pe, src) => {
                self.inner_put(*pe, src);
            }
            AllocOp::PutBuf(pe, src) => {
                self.inner_put_buf(*pe, src);
            }
            AllocOp::PutAll(pes, src) => {
                self.inner_put_all(pes, src);
            }
            AllocOp::PutAllBuf(pes, src) => {
                self.inner_put_all_buf(pes, src);
            }
        }

        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all();
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler
            .clone()
            .spawn_task(async move { self.alloc.ofi.wait_all() }, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricSysPutFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<LibfabricSysPutFuture<T>> for RdmaHandle<T> {
    fn from(f: LibfabricSysPutFuture<T>) -> RdmaHandle<T> {
        RdmaHandle {
            future: RdmaPutFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysPutFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all();

        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysGetFuture<T> {
    alloc: LibfabricSysAlloc,
    pe: usize,
    offset: usize,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
    result: Box<T>,
}

impl<T: Remote> LibfabricSysGetFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        trace!("getting at: {:?} {:?} ", self.pe, self.offset);
        unsafe {
            self.alloc
                .inner_get(
                    self.pe,
                    self.offset,
                    std::slice::from_mut(&mut *self.result),
                    false,
                );
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> T {
        self.exec_at();
        self.alloc.ofi.wait_all();
        // unsafe { self.result.assume_init_read() }
        *self.result
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_at();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move {
                self.alloc.ofi.wait_all();
                *self.result
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricSysGetFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<LibfabricSysGetFuture<T>> for RdmaGetHandle<T> {
    fn from(f: LibfabricSysGetFuture<T>) -> RdmaGetHandle<T> {
        RdmaGetHandle {
            future: RdmaGetFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysGetFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        let this = self.project();
        this.alloc.ofi.wait_all();

        // Poll::Ready(unsafe { this.result.assume_init_read() })
        Poll::Ready(**this.result)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysGetBufferFuture<T> {
    alloc: LibfabricSysAlloc,
    pe: usize,
    offset: usize,
    len: usize,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
    result: Vec<T>,
}

impl<T: Remote> LibfabricSysGetBufferFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        trace!("getting at: {:?} {:?} ", self.pe, self.offset);
        unsafe {
            self.alloc
                .inner_get(self.pe, self.offset, &mut self.result, false);
            self.spawned = true;
        }
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_at();

        self.alloc.ofi.wait_all();
        std::mem::take(&mut self.result)
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_at();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move {
                self.alloc.ofi.wait_all();
                std::mem::take(&mut self.result)
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricSysGetBufferFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<LibfabricSysGetBufferFuture<T>> for RdmaGetBufferHandle<T> {
    fn from(f: LibfabricSysGetBufferFuture<T>) -> RdmaGetBufferHandle<T> {
        RdmaGetBufferHandle {
            future: RdmaGetBufferFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysGetBufferFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        let this = self.project();
        this.alloc.ofi.wait_all();
        Poll::Ready(std::mem::take(this.result))
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysGetIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    my_pe: usize,
    alloc: LibfabricSysAlloc,
    pe: usize,
    offset: usize,
    dst: LamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricSysGetIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        unsafe {
            LibfabricSysAlloc::inner_get(
                &self.alloc,
                self.pe,
                self.offset,
                self.dst.as_mut_slice(),
                false,
            );
        };
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all();
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        let ofi = self.alloc.ofi.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { ofi.wait_all() }, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricSysGetIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricSysGetIntoBufferFuture<T, B>>
    for RdmaGetIntoBufferHandle<T, B>
{
    fn from(f: LibfabricSysGetIntoBufferFuture<T, B>) -> RdmaGetIntoBufferHandle<T, B> {
        RdmaGetIntoBufferHandle {
            future: RdmaGetIntoBufferFuture::LibfabricSys(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricSysGetIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all();
        Poll::Ready(())
    }
}

impl CommAllocRdma for LibfabricSysAlloc {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        LibfabricSysPutFuture {
            my_pe: self.ofi.my_pe,
            alloc: self.clone(),
            offset: offset,
            op: AllocOp::Put(pe, src),
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
        unsafe {
            LibfabricSysAlloc::inner_put(&self, pe, offset, std::slice::from_ref(&src), true)
        };
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        trace!(
            "put unamanaged dst: {pe}  offset: {offset} size_of<T> {}",
            std::mem::size_of::<T>()
        );
        unsafe {
            LibfabricSysAlloc::inner_put(&self, pe, offset, std::slice::from_ref(&src), false);
        };
    }
    fn put_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        LibfabricSysPutFuture {
            my_pe: self.ofi.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::PutBuf(pe, src.into()),
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
        let src = src.into();

        unsafe {
            LibfabricSysAlloc::inner_put(&self, pe, offset, src.as_slice(), false);
        };
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        offset: usize,
    ) -> RdmaHandle<T> {
        let pes = (0..self.num_pes()).collect();
        LibfabricSysPutFuture {
            my_pe: self.ofi.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::PutAll(pes, src.into()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize) {
        for pe in 0..self.num_pes() {
            unsafe {
                LibfabricSysAlloc::inner_put(&self, pe, offset, std::slice::from_ref(&src), false);
            };
        }
    }
    fn put_all_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) -> RdmaHandle<T> {
        let pes = (0..self.num_pes()).collect();
        LibfabricSysPutFuture {
            my_pe: self.ofi.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::PutAllBuf(pes, src.into()),
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
            unsafe {
                LibfabricSysAlloc::inner_put(&self, pe, offset, src.as_slice(), false);
            };
        }
    }

    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        LibfabricSysGetFuture {
            alloc: self.clone(),
            pe,
            offset,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: Box::new(T::default()),
            // local_op: pe == self.ofi.my_pe,
        }
        .into()
    }
    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        let mut val = T::default();
        let val_slice = std::slice::from_mut(&mut val);
        unsafe {
            LibfabricSysAlloc::inner_get_small(self, pe, offset, val_slice, true)
        };
        val
        // let mut result = T::default();
        // let mut_result_slice = std::slice::from_mut(&mut result);
        // LibfabricAlloc::atomic_fetch_op_inner(
        //     self,
        //     pe,
        //     offset,
        //     &crate::lamellae::AtomicOp::Read,
        //     mut_result_slice,
        //     true,
        // )
        // .unwrap();
        // result
    }
    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> RdmaGetBufferHandle<T> {
        LibfabricSysGetBufferFuture {
            alloc: self.clone(),
            pe,
            offset,
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
        let mut dst = vec![T::default(); len];
        unsafe {
            self.inner_get(pe, offset, &mut dst, true)
        };
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
        LibfabricSysGetIntoBufferFuture {
            my_pe: self.ofi.my_pe,
            alloc: self.clone(),
            pe,
            offset,
            dst,
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
        unsafe {
            LibfabricSysAlloc::inner_get(&self, pe, offset, dst.as_mut_slice(), true)
        };
    }

    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        unsafe {
            LibfabricSysAlloc::inner_get(&self, pe, offset, dst.as_mut_slice(), false);
        };
    }
}

impl CommAllocRdma for OneSidedLibfabricSysAlloc {
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
            "put called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );

        LibfabricSysPutFuture {
            my_pe: self.alloc.ofi.my_pe,
            alloc: self.alloc.clone(),
            offset: offset,
            op: AllocOp::Put(pe, src),
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
        assert_eq!(
            pe, self.remote_pe,
            "put_blocking called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        unsafe {
            LibfabricSysAlloc::inner_put(&self.alloc, pe, offset, std::slice::from_ref(&src), true)
        };
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        assert_eq!(
            pe, self.remote_pe,
            "put_unmanaged called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        unsafe {
            LibfabricSysAlloc::inner_put(&self.alloc, pe, offset, std::slice::from_ref(&src), false);
        };
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
            "put_buffer called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );

        LibfabricSysPutFuture {
            my_pe: self.alloc.ofi.my_pe,
            alloc: self.alloc.clone(),
            offset,
            op: AllocOp::PutBuf(pe, src.into()),
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
        assert_eq!(
            pe, self.remote_pe,
            "put_buffer_unmanaged called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let src = src.into();
        unsafe {
            LibfabricSysAlloc::inner_put(&self.alloc, pe, offset, src.as_slice(), false);
        };
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
        self.put_unmanaged(src, self.remote_pe, offset);
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
        self.put_buffer_unmanaged(src, self.remote_pe, offset);
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
            "get called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricSysGetFuture {
            alloc: self.alloc.clone(),
            pe,
            offset,
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
            "blocking_get called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut val = T::default();
        let val_slice = std::slice::from_mut(&mut val);
        unsafe {
            LibfabricSysAlloc::inner_get_small(&self.alloc, pe, offset, val_slice, true)
        };
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
            "get_buffer called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricSysGetBufferFuture {
            alloc: self.alloc.clone(),
            pe,
            offset,
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
            "blocking_get_buffer called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut dst = vec![T::default(); len];
        unsafe {
            self.alloc
                .inner_get(pe, offset, &mut dst, true)
        };
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
            "get_into_buffer called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricSysGetIntoBufferFuture {
            my_pe: self.alloc.ofi.my_pe,
            alloc: self.alloc.clone(),
            pe,
            offset,
            dst,
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
            "blocking_get_into_buffer called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        unsafe {
            LibfabricSysAlloc::inner_get(&self.alloc, pe, offset, dst.as_mut_slice(), true)
        };
    }
    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        assert_eq!(pe, self.remote_pe, "get_into_buffer_unmanaged called on OneSidedLibfabricSysAlloc with incorrect pe: {} expected pe: {}", pe, self.remote_pe);
        unsafe {
            LibfabricSysAlloc::inner_get(&self.alloc, pe, offset, dst.as_mut_slice(), false);
        };
    }
}

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
    fabric::{LibfabricAlloc, OneSidedLibfabricAlloc},
    Scheduler,
};

pub(super) enum AllocOp<T: Remote> {
    Put(usize, T),
    PutBuf(usize, MemregionRdmaInputInner<T>),
    PutAll(Vec<usize>, T),
    PutAllBuf(Vec<usize>, MemregionRdmaInputInner<T>),
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricPutFuture<T: Remote> {
    my_pe: usize,
    alloc: LibfabricAlloc,
    offset: usize,
    op: AllocOp<T>,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
}

impl<T: Remote> LibfabricPutFuture<T> {
    fn inner_put(&self, pe: usize, src: &T) {
        trace!(
            "putting src: {:x} dst: {:x} len: {} num bytes {}",
            src as *const T as usize,
            self.alloc.start() + self.offset,
            1,
            std::mem::size_of::<T>()
        );
        unsafe {
            LibfabricAlloc::inner_put(
                &self.alloc,
                pe,
                self.offset,
                std::slice::from_ref(src),
                false,
            )
            .expect("error in put")
        };
    }
    fn inner_put_buf(&self, pe: usize, src: &MemregionRdmaInputInner<T>) {
        unsafe {
            LibfabricAlloc::inner_put(&self.alloc, pe, self.offset, src.as_slice(), false)
                .expect("error in put_buf")
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
        self.alloc.ofi.wait_all().unwrap();
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler
            .clone()
            .spawn_task(async move { self.alloc.ofi.wait_all().unwrap() }, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricPutFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<LibfabricPutFuture<T>> for RdmaHandle<T> {
    fn from(f: LibfabricPutFuture<T>) -> RdmaHandle<T> {
        RdmaHandle {
            future: RdmaPutFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricPutFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();

        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricGetFuture<T> {
    alloc: LibfabricAlloc,
    pe: usize,
    offset: usize,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
    result: Box<T>,
}

impl<T: Remote> LibfabricGetFuture<T> {
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
                )
                .expect("error in get");
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> T {
        self.exec_at();
        self.alloc.ofi.wait_all().unwrap();
        // unsafe { self.result.assume_init_read() }
        *self.result
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_at();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move {
                self.alloc.ofi.wait_all().unwrap();
                *self.result
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricGetFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<LibfabricGetFuture<T>> for RdmaGetHandle<T> {
    fn from(f: LibfabricGetFuture<T>) -> RdmaGetHandle<T> {
        RdmaGetHandle {
            future: RdmaGetFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricGetFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        let this = self.project();
        this.alloc.ofi.wait_all().unwrap();

        // Poll::Ready(unsafe { this.result.assume_init_read() })
        Poll::Ready(**this.result)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricGetBufferFuture<T> {
    alloc: LibfabricAlloc,
    pe: usize,
    offset: usize,
    len: usize,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
    result: Vec<T>,
}

impl<T: Remote> LibfabricGetBufferFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        trace!("getting at: {:?} {:?} ", self.pe, self.offset);
        unsafe {
            self.alloc
                .inner_get(self.pe, self.offset, &mut self.result, false)
                .expect("error in get_buffer");
            self.spawned = true;
        }
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_at();

        self.alloc.ofi.wait_all().unwrap();
        std::mem::take(&mut self.result)
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_at();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move {
                self.alloc.ofi.wait_all().unwrap();
                std::mem::take(&mut self.result)
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricGetBufferFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<LibfabricGetBufferFuture<T>> for RdmaGetBufferHandle<T> {
    fn from(f: LibfabricGetBufferFuture<T>) -> RdmaGetBufferHandle<T> {
        RdmaGetBufferHandle {
            future: RdmaGetBufferFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricGetBufferFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        let this = self.project();
        this.alloc.ofi.wait_all().unwrap();
        Poll::Ready(std::mem::take(this.result))
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricGetIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    my_pe: usize,
    alloc: LibfabricAlloc,
    pe: usize,
    offset: usize,
    dst: LamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricGetIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        unsafe {
            LibfabricAlloc::inner_get(
                &self.alloc,
                self.pe,
                self.offset,
                self.dst.as_mut_slice(),
                false,
            )
            .expect("error in get_into_buffer");
        };
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        let ofi = self.alloc.ofi.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { ofi.wait_all().unwrap() }, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricGetIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricGetIntoBufferFuture<T, B>>
    for RdmaGetIntoBufferHandle<T, B>
{
    fn from(f: LibfabricGetIntoBufferFuture<T, B>) -> RdmaGetIntoBufferHandle<T, B> {
        RdmaGetIntoBufferHandle {
            future: RdmaGetIntoBufferFuture::Libfabric(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricGetIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_all().unwrap();
        Poll::Ready(())
    }
}

impl CommAllocRdma for LibfabricAlloc {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        LibfabricPutFuture {
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
    fn put_blocking<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        unsafe {
            LibfabricAlloc::inner_put(&self, pe, offset, std::slice::from_ref(&src), true)
                .expect("error in put_blocking")
        };
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        trace!(
            "put unamanaged dst: {pe}  offset: {offset} size_of<T> {}",
            std::mem::size_of::<T>()
        );
        unsafe {
            LibfabricAlloc::inner_put(&self, pe, offset, std::slice::from_ref(&src), false)
                .expect("error in put_unmanaged")
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
        LibfabricPutFuture {
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
            LibfabricAlloc::inner_put(&self, pe, offset, src.as_slice(), false)
                .expect("error in put_buffer_unmanaged")
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
        LibfabricPutFuture {
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
                LibfabricAlloc::inner_put(&self, pe, offset, std::slice::from_ref(&src), false)
                    .expect("error in put_all_unmanaged")
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
        LibfabricPutFuture {
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
                LibfabricAlloc::inner_put(&self, pe, offset, src.as_slice(), false)
                    .expect("error in put_all_buffer_unmanaged")
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
        LibfabricGetFuture {
            alloc: self.clone(),
            pe,
            offset,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: Box::new(T::default()),
        }
        .into()
    }

    fn blocking_get<T: Remote>(&self, pe: usize, offset: usize) -> T {
        let mut val = T::default();
        let val_slice = std::slice::from_mut(&mut val);
        unsafe {
            LibfabricAlloc::inner_get_small(self, pe, offset, val_slice, true)
                .expect("error in blocking_get")
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
        LibfabricGetBufferFuture {
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
    fn blocking_get_buffer<T: Remote>(&self, pe: usize, offset: usize, len: usize) -> Vec<T> {
        let mut dst = vec![T::default(); len];
        unsafe {
            self.inner_get(pe, offset, &mut dst, true)
                .expect("error in blocking_get_buffer")
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
        LibfabricGetIntoBufferFuture {
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
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        unsafe {
            LibfabricAlloc::inner_get(&self, pe, offset, dst.as_mut_slice(), true)
                .expect("error in blocking_get_into_buffer")
        };
    }

    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        unsafe {
            LibfabricAlloc::inner_get(&self, pe, offset, dst.as_mut_slice(), false)
                .expect("error in get_into_buffer_unmanaged")
        };
    }
}

impl CommAllocRdma for OneSidedLibfabricAlloc {
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
            "put called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );

        LibfabricPutFuture {
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
    fn put_blocking<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        assert_eq!(
            pe, self.remote_pe,
            "put_blocking called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        unsafe {
            LibfabricAlloc::inner_put(&self.alloc, pe, offset, std::slice::from_ref(&src), true)
                .expect("error in put_blocking")
        };
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        assert_eq!(
            pe, self.remote_pe,
            "put_unmanaged called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        unsafe {
            LibfabricAlloc::inner_put(&self.alloc, pe, offset, std::slice::from_ref(&src), false)
                .expect("error in put_unmanaged")
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
            "put_buffer called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );

        LibfabricPutFuture {
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
            "put_buffer_unmanaged called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let src = src.into();
        unsafe {
            LibfabricAlloc::inner_put(&self.alloc, pe, offset, src.as_slice(), false)
                .expect("error in put_buffer_unmanaged")
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
            "get called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricGetFuture {
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
    fn blocking_get<T: Remote>(&self, pe: usize, offset: usize) -> T {
        assert_eq!(
            pe, self.remote_pe,
            "blocking_get called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut val = T::default();
        let val_slice = std::slice::from_mut(&mut val);
        unsafe {
            LibfabricAlloc::inner_get_small(&self.alloc, pe, offset, val_slice, true)
                .expect("error in blocking_get")
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
            "get_buffer called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricGetBufferFuture {
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
    fn blocking_get_buffer<T: Remote>(&self, pe: usize, offset: usize, len: usize) -> Vec<T> {
        assert_eq!(
            pe, self.remote_pe,
            "blocking_get_buffer called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut dst = vec![T::default(); len];
        unsafe {
            self.alloc
                .inner_get(pe, offset, &mut dst, true)
                .expect("error in blocking_get_buffer");
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
            "get_into_buffer called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricGetIntoBufferFuture {
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
            LibfabricAlloc::inner_get(&self.alloc, pe, offset, dst.as_mut_slice(), true)
                .expect("error in blocking_get_into_buffer")
        };
    }
    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        assert_eq!(pe, self.remote_pe, "get_into_buffer_unmanaged called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}", pe, self.remote_pe);
        unsafe {
            LibfabricAlloc::inner_get(&self.alloc, pe, offset, dst.as_mut_slice(), false)
                .expect("error in get_into_buffer_unmanaged")
        };
    }
}

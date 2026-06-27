use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_util::Future;
use pin_project::{pin_project, pinned_drop};

use crate::{
    active_messaging::AMCounters,
    lamellae::{
        comm::rdma::{RdmaHandle, RdmaPutFuture, Remote},
        local_lamellae::comm::LocalAlloc,
        CommAllocRdma, RdmaGetBufferFuture, RdmaGetBufferHandle, RdmaGetFuture, RdmaGetHandle,
        RdmaGetIntoBufferFuture, RdmaGetIntoBufferHandle,
    },
    memregion::{AsLamellarBuffer, LamellarBuffer, MemregionRdmaInputInner},
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::Scheduler;

pub(super) enum AllocOp<T: Remote> {
    Put(T),
    PutBuf(MemregionRdmaInputInner<T>), //for local lamellae put_all is equivalent to put
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalFuture<T: Remote> {
    alloc: Arc<LocalAlloc>,
    index: usize,
    op: AllocOp<T>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
}

impl<T: Remote> LocalFuture<T> {
    fn exec_op(&mut self) {
        match &mut self.op {
            AllocOp::Put(src) => unsafe {
                let alloc_slice = self.alloc.as_mut_slice();
                assert!(self.index < alloc_slice.len());
                alloc_slice[self.index] = *src;
            },
            AllocOp::PutBuf(src) => unsafe {
                let alloc_slice = self.alloc.as_mut_slice();
                assert!(self.index + src.len() <= alloc_slice.len());
                alloc_slice[self.index..(self.index + src.len())].copy_from_slice(src.as_slice());
            },
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
impl<T: Remote> PinnedDrop for LocalFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> Future for LocalFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

impl<T: Remote> From<LocalFuture<T>> for RdmaHandle<T> {
    fn from(f: LocalFuture<T>) -> RdmaHandle<T> {
        RdmaHandle {
            future: RdmaPutFuture::Local(f),
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalGetFuture<T> {
    alloc: Arc<LocalAlloc>,
    index: usize,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    result: Box<T>,
}

impl<T: Remote> LocalGetFuture<T> {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        unsafe {
            let alloc_slice = self.alloc.as_mut_slice();
            assert!(self.index < alloc_slice.len());
            *self.result = alloc_slice[self.index];
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
impl<T> PinnedDrop for LocalGetFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> Future for LocalGetFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        Poll::Ready(*self.result)
    }
}

impl<T: Remote> From<LocalGetFuture<T>> for RdmaGetHandle<T> {
    fn from(f: LocalGetFuture<T>) -> RdmaGetHandle<T> {
        RdmaGetHandle {
            future: RdmaGetFuture::Local(f),
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalGetBufferFuture<T> {
    alloc: Arc<LocalAlloc>,
    index: usize,
    len: usize,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    result: Vec<T>,
}

impl<T: Remote> LocalGetBufferFuture<T> {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        let alloc_slice = unsafe { self.alloc.as_mut_slice() };
        assert!(self.index + self.len <= alloc_slice.len());
        self.result
            .as_mut_slice()
            .copy_from_slice(&alloc_slice[self.index..(self.index + self.len)]);

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
impl<T> PinnedDrop for LocalGetBufferFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> Future for LocalGetBufferFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        Poll::Ready(std::mem::take(&mut self.result))
    }
}

impl<T: Remote> From<LocalGetBufferFuture<T>> for RdmaGetBufferHandle<T> {
    fn from(f: LocalGetBufferFuture<T>) -> RdmaGetBufferHandle<T> {
        RdmaGetBufferHandle {
            future: RdmaGetBufferFuture::Local(f),
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalGetIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    alloc: Arc<LocalAlloc>,
    index: usize,
    buffer: LamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LocalGetIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        let len = self.buffer.len();
        let alloc_slice = unsafe { self.alloc.as_mut_slice() };
        assert!(self.index + len <= alloc_slice.len());

        self.buffer
            .as_mut_slice()
            .copy_from_slice(&alloc_slice[self.index..(self.index + len)]);

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
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LocalGetIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LocalGetIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LocalGetIntoBufferFuture<T, B>>
    for RdmaGetIntoBufferHandle<T, B>
{
    fn from(f: LocalGetIntoBufferFuture<T, B>) -> RdmaGetIntoBufferHandle<T, B> {
        RdmaGetIntoBufferHandle {
            future: RdmaGetIntoBufferFuture::Local(f),
        }
    }
}

impl CommAllocRdma for Arc<LocalAlloc> {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        _pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        // self.put_amt
        //     .fetch_add(src.len() * std::mem::size_of::<T>(), Ordering::SeqCst);

        LocalFuture {
            alloc: self.clone(),
            index: offset, //no need to go to bytes since local lamellae, we can reason in number of T
            op: AllocOp::Put(src.into()),
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
        _pe: usize,
        offset: usize,
    ) {
        let dst = unsafe { self.as_mut_ptr::<T>().add(offset) };
        unsafe {
            dst.write(src);
        }
    }
    fn put_unmanaged<T: Remote>(&self, src: T, _pe: usize, offset: usize) {
        let dst = unsafe { self.as_mut_ptr::<T>().add(offset) };
        unsafe {
            dst.write(src);
        }
    }
    fn put_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        _pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        // self.put_amt
        //     .fetch_add(src.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
        LocalFuture {
            alloc: self.clone(),
            index: offset, //no need to go to bytes since local lamellae, we can reason in number of T
            op: AllocOp::PutBuf(src.into()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn put_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        _pe: usize,
        offset: usize,
    ) {
        let src = src.into();
        let alloc_slice = unsafe { self.as_mut_slice() };
        assert!(offset + src.len() <= alloc_slice.len());
        alloc_slice[offset..(offset + src.len())].copy_from_slice(src.as_slice());
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        offset: usize,
    ) -> RdmaHandle<T> {
        // self.put_amt.fetch_add(
        //     src.len() * std::mem::size_of::<T>() * self.num_pes,
        //     Ordering::SeqCst,
        // );
        LocalFuture {
            alloc: self.clone(),
            index: offset, //no need to go to bytes since local lamellae, we can reason in number of T
            op: AllocOp::Put(src),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize) {
        let alloc_slice = unsafe { self.as_mut_slice() };
        assert!(offset < alloc_slice.len());
        alloc_slice[offset] = src;
    }
    fn put_all_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) -> RdmaHandle<T> {
        // self.put_amt.fetch_add(
        //     src.len() * std::mem::size_of::<T>() * self.num_pes,
        //     Ordering::SeqCst,
        // );
        LocalFuture {
            alloc: self.clone(),
            index: offset, //no need to go to bytes since local lamellae, we can reason in number of T
            op: AllocOp::PutBuf(src.into()),
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
        let alloc_slice = unsafe { self.as_mut_slice() };
        assert!(offset + src.len() <= alloc_slice.len());
        alloc_slice[offset..(offset + src.len())].copy_from_slice(src.as_slice());
    }
    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        _pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        // self.get_amt
        //     .fetch_add(std::mem::size_of::<T>(), Ordering::SeqCst);
        LocalGetFuture {
            alloc: self.clone(),
            index: offset, //no need to go to bytes since local lamellae, we can reason in number of T
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: Box::new(unsafe { std::mem::zeroed() }),
        }
        .into()
    }

    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, _pe: usize, offset: usize) -> T {
        assert!(offset < unsafe { self.as_mut_slice::<T>().len() });
        unsafe { self.as_mut_slice::<T>()[offset] }
    }
    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        _pe: usize,
        offset: usize,
        len: usize,
    ) -> RdmaGetBufferHandle<T> {
        LocalGetBufferFuture {
            alloc: self.clone(),
            index: offset, //no need to go to bytes since local lamellae, we can reason in number of T
            len,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: (0..len).map(|_| unsafe { std::mem::zeroed() }).collect(),
        }
        .into()
    }
    fn blocking_get_buffer<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        _pe: usize,
        offset: usize,
        len: usize,
    ) -> Vec<T> {
        let alloc_slice = unsafe { self.as_mut_slice() };
        assert!(offset + len <= alloc_slice.len());
        alloc_slice[offset..(offset + len)].to_vec()
    }
    fn get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        _pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) -> RdmaGetIntoBufferHandle<T, B> {
        LocalGetIntoBufferFuture {
            alloc: self.clone(),
            index: offset, //no need to go to bytes since local lamellae, we can reason in number of T
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
        _pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        let alloc_slice = unsafe { self.as_mut_slice() };
        assert!(offset + dst.len() <= alloc_slice.len());
        let len = dst.len();
        dst.as_mut_slice()
            .copy_from_slice(&alloc_slice[offset..(offset + len)]);
    }
    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        _pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        let alloc_slice = unsafe { self.as_mut_slice() };
        assert!(offset + dst.len() <= alloc_slice.len());
        let len = dst.len();
        dst.as_mut_slice()
            .copy_from_slice(&alloc_slice[offset..(offset + len)]);
    }
}

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
        CommAllocAddr, CommAllocRdma,
    },
    memregion::{AsLamellarBuffer, LamellarBuffer, MemregionRdmaInputInner},
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::{fabric::LibfabricAlloc, Scheduler};

pub(super) enum AllocOp<T: Remote> {
    Put(usize, T),
    PutBuf(usize, MemregionRdmaInputInner<T>),
    PutAll(Vec<usize>, T),
    PutAllBuf(Vec<usize>, MemregionRdmaInputInner<T>),
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricPutFuture<T: Remote> {
    my_pe: usize,
    alloc: Arc<LibfabricAlloc>,
    offset: usize,
    op: AllocOp<T>,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
}

impl<T: Remote> LibfabricPutFuture<T> {
    fn inner_put(&self, pe: usize, src: &T) {
        trace!(
            "putting src: {:?} dst: {:?} len: {} num bytes {}",
            src as *const T as usize,
            self.alloc.start() + self.offset,
            1,
            std::mem::size_of::<T>()
        );
        if pe != self.my_pe {
            unsafe {
                LibfabricAlloc::inner_put(
                    &self.alloc,
                    pe,
                    self.offset,
                    std::slice::from_ref(src),
                    false,
                )
            };
        } else {
            unsafe {
                self.alloc.as_mut_slice()[self.offset] = *src;
            }
            // let dst = CommAllocAddr(self.alloc.start() + self.offset* std::mem::size_of::<T>());
            // unsafe { dst.as_mut_ptr::<T>().write(*src) };
        }
    }
    fn inner_put_buf(&self, pe: usize, src: &MemregionRdmaInputInner<T>) {
        if pe != self.my_pe {
            unsafe {
                LibfabricAlloc::inner_put(&self.alloc, pe, self.offset, src.as_slice(), false)
            };
        } else {
            unsafe {
                self.alloc.as_mut_slice()[self.offset..self.offset + src.len()]
                    .copy_from_slice(src.as_slice());
            }
            // let dst = self.alloc.start() + self.offset * std::mem::size_of::<T>();
            // if !(src.contains(&dst) || src.contains(&(dst + src.num_bytes()))) {
            //     unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst as *mut T, src.len()) };
            // } else {
            //     unsafe {
            //         std::ptr::copy(src.as_ptr(), dst as *mut T, src.len());
            //     }
            // }
        }
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

    fn exec_op(&self) {
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
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
        self.spawned = true;
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        // let ofi = self.alloc.ofi.clone();
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
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
            self.alloc.ofi.wait_all().unwrap();
            *self.project().spawned = true;
        } else {
            self.alloc.ofi.wait_all().unwrap();
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricGetFuture<T> {
    alloc: Arc<LibfabricAlloc>,
    pe: usize,
    offset: usize,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
    result: MaybeUninit<T>,
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
                    std::slice::from_raw_parts_mut(self.result.as_mut_ptr(), 1),
                    false,
                )
                .expect("error in get");
        }
    }

    pub(crate) fn block(mut self) -> T {
        self.exec_at();
        self.spawned = true;
        self.alloc.ofi.wait_all().unwrap();
        unsafe { self.result.assume_init() }
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_at();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move {
                self.alloc.ofi.wait_all().unwrap();
                unsafe { self.result.assume_init() }
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
        *this.spawned = true;
        this.alloc.ofi.wait_all().unwrap();

        Poll::Ready(unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(this.result, &mut res);
            res.assume_init()
        })
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricGetBufferFuture<T> {
    alloc: Arc<LibfabricAlloc>,
    pe: usize,
    offset: usize,
    len: usize,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
    result: MaybeUninit<Vec<T>>,
}

impl<T: Remote> LibfabricGetBufferFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        trace!("getting at: {:?} {:?} ", self.pe, self.offset);
        unsafe {
            let mut dst = Vec::<T>::with_capacity(self.len);
            dst.set_len(self.len);
            self.alloc
                .inner_get(self.pe, self.offset, dst.as_mut_slice(), false);
            self.result.write(dst);
        }
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_at();
        self.spawned = true;
        self.alloc.ofi.wait_all().unwrap();
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
                self.alloc.ofi.wait_all().unwrap();
                let mut res = MaybeUninit::uninit();
                std::mem::swap(&mut self.result, &mut res);
                unsafe { res.assume_init() }
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
        *this.spawned = true;
        this.alloc.ofi.wait_all().unwrap();

        Poll::Ready(unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(this.result, &mut res);
            res.assume_init()
        })
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricGetIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    my_pe: usize,
    alloc: Arc<LibfabricAlloc>,
    pe: usize,
    offset: usize,
    dst: LamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Vec<Arc<AMCounters>>,
    spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricGetIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        if self.pe != self.my_pe {
            unsafe {
                LibfabricAlloc::inner_get(
                    &self.alloc,
                    self.pe,
                    self.offset,
                    self.dst.as_mut_slice(),
                    false,
                )
            };
        } else {
            let len = self.dst.len();
            unsafe {
                self.dst
                    .as_mut_slice()
                    .copy_from_slice(&self.alloc.as_mut_slice()[self.offset..self.offset + len])
            };
        }
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
        self.spawned = true;
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        self.spawned = true;
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
            self.alloc.ofi.wait_all().unwrap();
            *self.project().spawned = true;
        } else {
            self.alloc.ofi.wait_all().unwrap();
        }
        Poll::Ready(())
    }
}

impl CommAllocRdma for Arc<LibfabricAlloc> {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        // self.put_amt
        //     .fetch_add(std::mem::size_of::<T>(), Ordering::SeqCst);

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
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        // self.put_amt
        //     .fetch_add(std::mem::size_of::<T>(), Ordering::SeqCst);

        trace!(
            "put unamanaged dst: {pe}  offset: {offset} size_of<T> {}",
            std::mem::size_of::<T>()
        );
        if pe != self.ofi.my_pe {
            unsafe {
                LibfabricAlloc::inner_put(&self, pe, offset, std::slice::from_ref(&src), false)
            };
        } else {
            unsafe {
                trace!(
                    "put unmanaged local copy {:?} {:?}",
                    self.as_mut_slice::<T>().as_ptr(),
                    self.as_mut_slice::<T>().as_ptr().add(offset)
                )
            };
            unsafe { self.as_mut_slice::<T>()[offset] = src };
            // let dst = CommAllocAddr(self.start() + offset);
            // unsafe { dst.as_mut_ptr::<T>().write(src) };
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

        // self.put_amt
        //     .fetch_add(src.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
        if pe != self.ofi.my_pe {
            unsafe { LibfabricAlloc::inner_put(&self, pe, offset, src.as_slice(), false) };
        } else {
            unsafe {
                self.as_mut_slice::<T>()[offset..offset + src.len()].copy_from_slice(src.as_slice())
            };
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
            if pe != self.ofi.my_pe {
                unsafe {
                    LibfabricAlloc::inner_put(&self, pe, offset, std::slice::from_ref(&src), false)
                };
            } else {
                let dst = CommAllocAddr(self.start() + offset);
                unsafe { dst.as_mut_ptr::<T>().write(src) };
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
            if pe != self.ofi.my_pe {
                unsafe { LibfabricAlloc::inner_put(&self, pe, offset, src.as_slice(), false) };
            } else {
                let dst = self.start() + offset;

                if !(src.contains(&dst) || src.contains(&(dst + src.len()))) {
                    unsafe {
                        std::ptr::copy_nonoverlapping(src.as_ptr(), dst as *mut T, src.len())
                    };
                } else {
                    unsafe {
                        std::ptr::copy(src.as_ptr(), dst as *mut T, src.len());
                    }
                }
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
        LibfabricGetFuture {
            alloc: self.clone(),
            pe,
            offset,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: MaybeUninit::uninit(),
        }
        .into()
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
            result: MaybeUninit::uninit(),
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
    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        if pe != self.ofi.my_pe {
            unsafe { LibfabricAlloc::inner_get(&self, pe, offset, dst.as_mut_slice(), false) };
        } else {
            let len = dst.len();
            unsafe {
                dst.as_mut_slice()
                    .copy_from_slice(&self.as_mut_slice::<T>()[offset..offset + len]);
            }
        }
    }
}

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
    alloc: LibfabricAlloc,
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
    alloc: LibfabricAlloc,
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
            let mut dst = vec![T::default(); self.len];
            // let dst_mut_slice = std::slice::from_raw_parts_mut(dst.as_mut_ptr(), self.len);

            // dst.set_len(self.len);
            self.alloc
                .inner_get(self.pe, self.offset, &mut dst, false)
                .expect("error in get_buffer");
            // dst.set_len(self.len);
            // let dst = std::mem::transmute::<Vec<MaybeUninit<T>>, Vec<T>>(dst);
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
        // if self.pe != self.my_pe {
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
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        trace!(
            "put unamanaged dst: {pe}  offset: {offset} size_of<T> {}",
            std::mem::size_of::<T>()
        );
        if pe != self.ofi.my_pe {
            unsafe {
                LibfabricAlloc::inner_put(&self, pe, offset, std::slice::from_ref(&src), false)
                    .expect("error in put_unmanaged")
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
            if pe != self.ofi.my_pe {
                unsafe {
                    LibfabricAlloc::inner_put(&self, pe, offset, std::slice::from_ref(&src), false)
                        .expect("error in put_all_unmanaged")
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
                unsafe {
                    LibfabricAlloc::inner_put(&self, pe, offset, src.as_slice(), false)
                        .expect("error in put_all_buffer_unmanaged")
                };
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
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        assert_eq!(
            pe, self.remote_pe,
            "put_unmanaged called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        if pe != self.alloc.ofi.my_pe {
            unsafe {
                LibfabricAlloc::inner_put(
                    &self.alloc,
                    pe,
                    offset,
                    std::slice::from_ref(&src),
                    false,
                )
                .expect("error in put_unmanaged")
            };
        } else {
            unsafe { self.alloc.as_mut_slice::<T>()[offset] = src };
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

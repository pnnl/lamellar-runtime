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
        CommAllocAddr, CommAllocRdma,
    },
    memregion::{AsLamellarBuffer, LamellarBuffer, MemregionRdmaInputInner},
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::{
    fabric::{LibfabricMtAlloc, OneSidedLibfabricMtAlloc},
    Scheduler,
};

pub(super) enum AllocOp<T: Remote> {
    Put(usize, T),
    PutBuf(usize, MemregionRdmaInputInner<T>),
    PutAll(Vec<usize>, T),
    PutAllBuf(Vec<usize>, MemregionRdmaInputInner<T>),
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtPutFuture<T: Remote> {
    my_pe: usize,
    alloc: LibfabricMtAlloc,
    offset: usize,
    op: AllocOp<T>,
    local_op: bool,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
}

impl<T: Remote> LibfabricMtPutFuture<T> {
    fn inner_put(&self, pe: usize, src: &T) {
        trace!(
            "putting src: {:x} dst: {:x} len: {} num bytes {}",
            src as *const T as usize,
            self.alloc.start() + self.offset,
            1,
            std::mem::size_of::<T>()
        );
        unsafe {
            LibfabricMtAlloc::inner_put(
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
            LibfabricMtAlloc::inner_put(&self.alloc, pe, self.offset, src.as_slice(), false)
                .expect("error in put_buf")
        };
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn inner_put_all(&self, pes: &Vec<usize>, src: &T) {
        for pe in pes {
            self.inner_put(*pe, src);
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
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
        if !self.local_op {
            self.alloc.ofi.wait_all().unwrap();
        }
    }
    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricMtPutFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<LibfabricMtPutFuture<T>> for RdmaHandle<T> {
    fn from(f: LibfabricMtPutFuture<T>) -> RdmaHandle<T> {
        RdmaHandle {
            future: RdmaPutFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricMtPutFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        if !self.local_op {
            match self.alloc.ofi.poll_wait_for_tx_cntr() {
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(()) => {}
            }
        }

        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtGetFuture<T> {
    alloc: LibfabricMtAlloc,
    pe: usize,
    offset: usize,
    local_op: bool,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    result: Box<T>,
}

impl<T: Remote> LibfabricMtGetFuture<T> {
    //#[tracing::instrument(skip_all, level = "debug")]
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
        if !self.local_op {
            self.alloc.ofi.wait_all().unwrap();
        }
        // unsafe { self.result.assume_init_read() }
        *self.result
    }
    pub(crate) fn spawn(self) -> LamellarTask<T> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricMtGetFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<LibfabricMtGetFuture<T>> for RdmaGetHandle<T> {
    fn from(f: LibfabricMtGetFuture<T>) -> RdmaGetHandle<T> {
        RdmaGetHandle {
            future: RdmaGetFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricMtGetFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        let this = self.project();
        if !*this.local_op {
            match this.alloc.ofi.poll_wait_for_rx_cntr() {
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(()) => {}
            }
        }

        // Poll::Ready(unsafe { this.result.assume_init_read() })
        Poll::Ready(**this.result)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtGetBufferFuture<T> {
    alloc: LibfabricMtAlloc,
    pe: usize,
    offset: usize,
    len: usize,
    local_op: bool,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    result: Vec<T>,
}

impl<T: Remote> LibfabricMtGetBufferFuture<T> {
    //#[tracing::instrument(skip_all, level = "debug")]
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

        if !self.local_op {
            self.alloc.ofi.wait_all().unwrap();
        }
        std::mem::take(&mut self.result)
    }
    pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricMtGetBufferFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<LibfabricMtGetBufferFuture<T>> for RdmaGetBufferHandle<T> {
    fn from(f: LibfabricMtGetBufferFuture<T>) -> RdmaGetBufferHandle<T> {
        RdmaGetBufferHandle {
            future: RdmaGetBufferFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricMtGetBufferFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        if !self.local_op {
            match self.alloc.ofi.poll_wait_for_rx_cntr() {
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(()) => {}
            }
        }
        Poll::Ready(std::mem::take(&mut self.result))
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricMtGetIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    my_pe: usize,
    alloc: LibfabricMtAlloc,
    pe: usize,
    offset: usize,
    dst: LamellarBuffer<T, B>,
    local_op: bool,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricMtGetIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        unsafe {
            LibfabricMtAlloc::inner_get(
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
        if !self.local_op {
            self.alloc.ofi.wait_all().unwrap();
        }
    }
    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricMtGetIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricMtGetIntoBufferFuture<T, B>>
    for RdmaGetIntoBufferHandle<T, B>
{
    fn from(f: LibfabricMtGetIntoBufferFuture<T, B>) -> RdmaGetIntoBufferHandle<T, B> {
        RdmaGetIntoBufferHandle {
            future: RdmaGetIntoBufferFuture::LibfabricMt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricMtGetIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        if !self.local_op {
            match self.alloc.ofi.poll_wait_for_rx_cntr() {
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Ready(()) => {}
            }
        }
        Poll::Ready(())
    }
}

impl CommAllocRdma for LibfabricMtAlloc {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        let local_op = pe == self.ofi.my_pe;
        LibfabricMtPutFuture {
            my_pe: self.ofi.my_pe,
            alloc: self.clone(),
            offset: offset,
            op: AllocOp::Put(pe, src),
            local_op,
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
        if pe != self.ofi.my_pe {
            unsafe {
                LibfabricMtAlloc::inner_put(&self, pe, offset, std::slice::from_ref(&src), true)
                    .expect("error in put_blocking")
            };
        } else {
            unsafe { self.as_mut_slice::<T>()[offset] = src };
        }
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        trace!(
            "put unamanaged dst: {pe}  offset: {offset} size_of<T> {}",
            std::mem::size_of::<T>()
        );
        if pe != self.ofi.my_pe {
            unsafe {
                LibfabricMtAlloc::inner_put(&self, pe, offset, std::slice::from_ref(&src), false)
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
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        let local_op = pe == self.ofi.my_pe;
        LibfabricMtPutFuture {
            my_pe: self.ofi.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::PutBuf(pe, src.into()),
            local_op,
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
            LibfabricMtAlloc::inner_put(&self, pe, offset, src.as_slice(), false)
                .expect("error in put_buffer_unmanaged")
        };
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        offset: usize,
    ) -> RdmaHandle<T> {
        let pes = (0..self.num_pes()).collect();
        LibfabricMtPutFuture {
            my_pe: self.ofi.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::PutAll(pes, src.into()),
            local_op: false,
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
                    LibfabricMtAlloc::inner_put(
                        &self,
                        pe,
                        offset,
                        std::slice::from_ref(&src),
                        false,
                    )
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
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) -> RdmaHandle<T> {
        let pes = (0..self.num_pes()).collect();
        LibfabricMtPutFuture {
            my_pe: self.ofi.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::PutAllBuf(pes, src.into()),
            local_op: false,
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
                    LibfabricMtAlloc::inner_put(&self, pe, offset, src.as_slice(), false)
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
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        let local_op = pe == self.ofi.my_pe;
        LibfabricMtGetFuture {
            alloc: self.clone(),
            pe,
            offset,
            local_op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: Box::new(unsafe { std::mem::zeroed() }),
        }
        .into()
    }

    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        let mut val: T = unsafe { std::mem::zeroed() };
        let val_slice = std::slice::from_mut(&mut val);
        unsafe {
            LibfabricMtAlloc::inner_get_small(self, pe, offset, val_slice, true)
                .expect("error in blocking_get")
        };
        val
        // let mut result = T::default();
        // let mut_result_slice = std::slice::from_mut(&mut result);
        // LibfabricMtAlloc::atomic_fetch_op_inner(
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
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> RdmaGetBufferHandle<T> {
        let local_op = pe == self.ofi.my_pe;
        LibfabricMtGetBufferFuture {
            alloc: self.clone(),
            pe,
            offset,
            len,
            local_op,
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
        pe: usize,
        offset: usize,
        len: usize,
    ) -> Vec<T> {
        let mut dst: Vec<T> = (0..len).map(|_| unsafe { std::mem::zeroed() }).collect();
        unsafe {
            self.inner_get(pe, offset, &mut dst, true)
                .expect("error in blocking_get_buffer")
        };
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
        let local_op = pe == self.ofi.my_pe;
        LibfabricMtGetIntoBufferFuture {
            my_pe: self.ofi.my_pe,
            alloc: self.clone(),
            pe,
            offset,
            dst,
            local_op,
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
            LibfabricMtAlloc::inner_get(&self, pe, offset, dst.as_mut_slice(), true)
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
            LibfabricMtAlloc::inner_get(&self, pe, offset, dst.as_mut_slice(), false)
                .expect("error in get_into_buffer_unmanaged")
        };
    }
}

impl CommAllocRdma for OneSidedLibfabricMtAlloc {
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
            "put called on OneSidedLibfabricMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let local_op = pe == self.alloc.ofi.my_pe;
        LibfabricMtPutFuture {
            my_pe: self.alloc.ofi.my_pe,
            alloc: self.alloc.clone(),
            offset: offset,
            op: AllocOp::Put(pe, src),
            local_op,
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
            "put_blocking called on OneSidedLibfabricMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        if pe != self.alloc.ofi.my_pe {
            unsafe {
                LibfabricMtAlloc::inner_put(
                    &self.alloc,
                    pe,
                    offset,
                    std::slice::from_ref(&src),
                    true,
                )
                .expect("error in put_blocking")
            };
        } else {
            unsafe { self.alloc.as_mut_slice::<T>()[offset] = src };
        }
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        assert_eq!(
            pe, self.remote_pe,
            "put_unmanaged called on OneSidedLibfabricMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        if pe != self.alloc.ofi.my_pe {
            unsafe {
                LibfabricMtAlloc::inner_put(
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
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "put_buffer called on OneSidedLibfabricMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let local_op = pe == self.alloc.ofi.my_pe;
        LibfabricMtPutFuture {
            my_pe: self.alloc.ofi.my_pe,
            alloc: self.alloc.clone(),
            offset,
            op: AllocOp::PutBuf(pe, src.into()),
            local_op,
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
            "put_buffer_unmanaged called on OneSidedLibfabricMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let src = src.into();
        unsafe {
            LibfabricMtAlloc::inner_put(&self.alloc, pe, offset, src.as_slice(), false)
                .expect("error in put_buffer_unmanaged")
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
        self.put_unmanaged(src, self.remote_pe, offset);
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
        self.put_buffer_unmanaged(src, self.remote_pe, offset);
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
            "get called on OneSidedLibfabricMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricMtGetFuture {
            alloc: self.alloc.clone(),
            pe,
            offset,
            local_op: false,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: Box::new(unsafe { std::mem::zeroed() }),
        }
        .into()
    }
    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        assert_eq!(
            pe, self.remote_pe,
            "blocking_get called on OneSidedLibfabricMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut val: T = unsafe { std::mem::zeroed() };
        let val_slice = std::slice::from_mut(&mut val);
        unsafe {
            LibfabricMtAlloc::inner_get(&self.alloc, pe, offset, val_slice, true)
                .expect("error in blocking_get")
        };
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
            "get_buffer called on OneSidedLibfabricMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricMtGetBufferFuture {
            alloc: self.alloc.clone(),
            pe,
            offset,
            len,
            local_op: false,
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
        pe: usize,
        offset: usize,
        len: usize,
    ) -> Vec<T> {
        assert_eq!(
            pe, self.remote_pe,
            "blocking_get_buffer called on OneSidedLibfabricMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut dst: Vec<T> = (0..len).map(|_| unsafe { std::mem::zeroed() }).collect();
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
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) -> RdmaGetIntoBufferHandle<T, B> {
        assert_eq!(
            pe, self.remote_pe,
            "get_into_buffer called on OneSidedLibfabricMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricMtGetIntoBufferFuture {
            my_pe: self.alloc.ofi.my_pe,
            alloc: self.alloc.clone(),
            pe,
            offset,
            dst,
            local_op: false,
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
            "blocking_get_into_buffer called on OneSidedLibfabricMtAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        unsafe {
            LibfabricMtAlloc::inner_get(&self.alloc, pe, offset, dst.as_mut_slice(), true)
                .expect("error in blocking_get_into_buffer")
        };
    }
    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        assert_eq!(pe, self.remote_pe, "get_into_buffer_unmanaged called on OneSidedLibfabricMtAlloc with incorrect pe: {} expected pe: {}", pe, self.remote_pe);
        unsafe {
            LibfabricMtAlloc::inner_get(&self.alloc, pe, offset, dst.as_mut_slice(), false)
                .expect("error in get_into_buffer_unmanaged")
        };
    }
}

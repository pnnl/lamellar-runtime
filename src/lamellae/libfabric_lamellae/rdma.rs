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
        comm::rdma::{RdmaAtFuture, RdmaFuture, RdmaGetHandle, RdmaHandle, Remote},
        CommAllocAddr, CommAllocRdma, CommSlice,
    },
    memregion::MemregionRdmaInputInner,
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::{fabric::LibfabricAlloc, Scheduler};

pub(super) enum AllocOp<T: Remote> {
    Put(usize, T),
    PutBuf(usize, MemregionRdmaInputInner<T>),
    PutAll(Vec<usize>, T),
    PutAllBuf(Vec<usize>, MemregionRdmaInputInner<T>),
    Get(usize, CommSlice<T>),
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAllocFuture<T: Remote> {
    my_pe: usize,
    pub(crate) alloc: Arc<LibfabricAlloc>,
    pub(crate) offset: usize,
    pub(super) op: AllocOp<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricAllocFuture<T> {
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
            let dst = CommAllocAddr(self.alloc.start() + self.offset);
            unsafe { dst.as_mut_ptr::<T>().write(*src) };
        }
    }
    fn inner_put_buf(&self, pe: usize, src: &MemregionRdmaInputInner<T>) {
        if pe != self.my_pe {
            unsafe {
                LibfabricAlloc::inner_put(&self.alloc, pe, self.offset, src.as_slice(), false)
            };
        } else {
            let dst = self.alloc.start() + self.offset;
            if !(src.contains(&dst) || src.contains(&(dst + src.len()))) {
                unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst as *mut T, src.len()) };
            } else {
                unsafe {
                    std::ptr::copy(src.as_ptr(), dst as *mut T, src.len());
                }
            }
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
    fn inner_get(&self, pe: usize, mut dst: CommSlice<T>) {
        trace!(
            "getting src: {:?} dst: {:?} len: {}",
            self.alloc.start() + self.offset,
            dst.usize_addr(),
            dst.len()
        );
        if pe != self.my_pe {
            unsafe { LibfabricAlloc::get(&self.alloc, pe, self.offset, &mut dst, false) };
        } else {
            let src = self.alloc.start() + self.offset;
            if !(dst.contains(&src) || dst.contains(&(src + dst.len()))) {
                unsafe {
                    std::ptr::copy_nonoverlapping(src as *const T, dst.as_mut_ptr(), dst.len());
                }
            } else {
                unsafe {
                    std::ptr::copy(src as *const T, dst.as_mut_ptr(), dst.len());
                }
            }
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
            AllocOp::Get(pe, dst) => {
                self.inner_get(*pe, dst.clone());
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
impl<T: Remote> PinnedDrop for LibfabricAllocFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<LibfabricAllocFuture<T>> for RdmaHandle<T> {
    fn from(f: LibfabricAllocFuture<T>) -> RdmaHandle<T> {
        RdmaHandle {
            future: RdmaFuture::LibfabricAlloc(f),
        }
    }
}

impl<T: Remote> Future for LibfabricAllocFuture<T> {
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
pub(crate) struct LibfabricAllocAtFuture<T> {
    pub(crate) alloc: Arc<LibfabricAlloc>,
    pub(crate) pe: usize,
    pub(crate) offset: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    pub(crate) result: MaybeUninit<T>,
}

impl<T: Remote> LibfabricAllocAtFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        trace!("getting at: {:?} {:?} ", self.pe, self.offset);
        unsafe {
            self.alloc.inner_get(
                self.pe,
                self.offset,
                std::slice::from_raw_parts_mut(self.result.as_mut_ptr(), 1),
                false,
            )
        };
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
impl<T> PinnedDrop for LibfabricAllocAtFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<LibfabricAllocAtFuture<T>> for RdmaGetHandle<T> {
    fn from(f: LibfabricAllocAtFuture<T>) -> RdmaGetHandle<T> {
        RdmaGetHandle {
            future: RdmaAtFuture::LibfabricAlloc(f),
        }
    }
}

impl<T: Remote> Future for LibfabricAllocAtFuture<T> {
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
        LibfabricAllocFuture {
            my_pe: self.ofi.my_pe,
            alloc: self.clone(),
            offset,
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
        if pe != self.ofi.my_pe {
            unsafe {
                LibfabricAlloc::inner_put(&self, pe, offset, std::slice::from_ref(&src), false)
            };
        } else {
            let dst = CommAllocAddr(self.start() + offset);
            unsafe { dst.as_mut_ptr::<T>().write(src) };
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

        LibfabricAllocFuture {
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
            let dst = self.start() + offset;
            if !(src.contains(&dst) || src.contains(&(dst + src.len()))) {
                unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst as *mut T, src.len()) };
            } else {
                unsafe {
                    std::ptr::copy(src.as_ptr(), dst as *mut T, src.len());
                }
            }
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
        LibfabricAllocFuture {
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
        LibfabricAllocFuture {
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

    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
        dst: CommSlice<T>,
    ) -> RdmaHandle<T> {
        LibfabricAllocFuture {
            my_pe: self.ofi.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::Get(pe, dst),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        LibfabricAllocAtFuture {
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
}

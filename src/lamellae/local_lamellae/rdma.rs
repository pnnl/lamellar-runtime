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
        comm::{
            rdma::{RdmaFuture, RdmaHandle, Remote},
            CommSlice,
        },
        local_lamellae::comm::LocalAlloc,
        CommAllocRdma, RdmaAtFuture, RdmaGetHandle,
    },
    memregion::MemregionRdmaInput,
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::Scheduler;

pub(super) enum AllocOp<T: Remote> {
    Put(T),
    PutBuf(MemregionRdmaInput<T>), //for local lamellae put_all is equivalent to put
    Get(CommSlice<T>),
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalAllocFuture<T: Remote> {
    alloc: Arc<LocalAlloc>,
    offset: usize,
    pub(super) op: AllocOp<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LocalAllocFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn inner_put_buf(&self, src: &MemregionRdmaInput<T>) {
        let dst = unsafe { self.alloc.as_mut_ptr::<T>().add(self.offset) };
        // let src = src.as_slice();
        trace!(
            "putting src: {:?} dst: {:?} len: {} num bytes {}",
            src.as_ptr(),
            dst,
            src.len(),
            src.len() * std::mem::size_of::<T>()
        );
        if !(src.contains(&dst.addr()) || src.contains(&(dst.addr() + src.len()))) {
            unsafe {
                std::ptr::copy_nonoverlapping(src.as_ptr(), dst, src.len());
            }
        } else {
            unsafe {
                std::ptr::copy(src.as_ptr(), dst, src.len());
            }
        }
    }
    #[tracing::instrument(skip_all, level = "debug")]
    fn inner_get(&self, dst: &CommSlice<T>) {
        let src = unsafe { self.alloc.as_mut_ptr::<T>().add(self.offset) };
        trace!(
            "getting src: {:?} dst: {:?} len: {}",
            src,
            dst.usize_addr(),
            dst.len()
        );
        if !(dst.contains(&src.addr()) || dst.contains(&(src.addr() + dst.len()))) {
            unsafe {
                std::ptr::copy_nonoverlapping(src, dst.as_mut_ptr(), dst.len());
            }
        } else {
            unsafe {
                std::ptr::copy(src, dst.as_mut_ptr(), dst.len());
            }
        }
    }
    fn exec_op(&self) {
        match &self.op {
            AllocOp::Put(src) => unsafe {
                let dst = self.alloc.as_mut_ptr::<T>().add(self.offset);
                dst.write(*src);
            },
            AllocOp::PutBuf(src) => {
                self.inner_put_buf(src);
            }
            AllocOp::Get(dst) => {
                self.inner_get(dst);
            }
        }
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.spawned = true;
        // Ok(())
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.spawn_task(async {}, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LocalAllocFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> Future for LocalAllocFuture<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
            *self.project().spawned = true;
        }
        Poll::Ready(())
    }
}

impl<T: Remote> From<LocalAllocFuture<T>> for RdmaHandle<T> {
    fn from(f: LocalAllocFuture<T>) -> RdmaHandle<T> {
        RdmaHandle {
            future: RdmaFuture::LocalAlloc(f),
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalAllocAtFuture<T> {
    alloc: Arc<LocalAlloc>,
    offset: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    pub(crate) result: MaybeUninit<T>,
}

impl<T: Remote> LocalAllocAtFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        let src = unsafe { self.alloc.as_mut_ptr::<T>().add(self.offset) };
        unsafe {
            self.result.as_mut_ptr().write(src.read());
        }
    }

    pub(crate) fn block(mut self) -> T {
        self.exec_at();
        self.spawned = true;
        unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(&mut self.result, &mut res);
            res.assume_init()
        }
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_at();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move {
                unsafe {
                    let mut res = MaybeUninit::uninit();
                    std::mem::swap(&mut self.result, &mut res);
                    res.assume_init()
                }
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LocalAllocAtFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> Future for LocalAllocAtFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        let this = self.project();
        *this.spawned = true;
        // rofi_c_wait();

        Poll::Ready(unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(this.result, &mut res);
            res.assume_init()
        })
    }
}

impl<T: Remote> From<LocalAllocAtFuture<T>> for RdmaGetHandle<T> {
    fn from(f: LocalAllocAtFuture<T>) -> RdmaGetHandle<T> {
        RdmaGetHandle {
            future: RdmaAtFuture::LocalAlloc(f),
        }
    }
}

impl CommAllocRdma for Arc<LocalAlloc> {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        _pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        // self.put_amt
        //     .fetch_add(src.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
        LocalAllocFuture {
            alloc: self.clone(),
            offset,
            op: AllocOp::Put(src.into()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
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
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInput<T>>,
        _pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        // self.put_amt
        //     .fetch_add(src.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
        LocalAllocFuture {
            alloc: self.clone(),
            offset,
            op: AllocOp::PutBuf(src.into()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn put_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInput<T>>,
        _pe: usize,
        offset: usize,
    ) {
        let dst = unsafe { self.as_mut_ptr::<T>().add(offset) };
        let src = src.into();
        if !(src.contains(&dst.addr()) || src.contains(&(dst.addr() + src.len()))) {
            unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst, src.len()) };
        } else {
            unsafe {
                std::ptr::copy(src.as_ptr(), dst, src.len());
            }
        }
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInput<T>>,
        offset: usize,
    ) -> RdmaHandle<T> {
        // self.put_amt.fetch_add(
        //     src.len() * std::mem::size_of::<T>() * self.num_pes,
        //     Ordering::SeqCst,
        // );
        LocalAllocFuture {
            alloc: self.clone(),
            offset,
            op: AllocOp::PutBuf(src.into()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        _pe: usize,
        offset: usize,
        dst: CommSlice<T>,
    ) -> RdmaHandle<T> {
        // self.get_amt
        //     .fetch_add(dst.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
        LocalAllocFuture {
            alloc: self.clone(),
            offset,
            op: AllocOp::Get(dst),
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
        _pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        // self.get_amt
        //     .fetch_add(std::mem::size_of::<T>(), Ordering::SeqCst);
        LocalAllocAtFuture {
            alloc: self.clone(),
            offset,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: MaybeUninit::uninit(),
        }
        .into()
    }
}

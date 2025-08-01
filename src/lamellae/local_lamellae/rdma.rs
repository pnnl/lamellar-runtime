use std::{
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    task::{Context, Poll},
};

use futures_util::Future;
use pin_project::{pin_project, pinned_drop};
use tracing::trace;

use crate::{
    active_messaging::AMCounters,
    lamellae::comm::{
        rdma::{CommRdma, RdmaFuture, RdmaHandle, Remote},
        CommAllocAddr, CommSlice,
    },
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::{comm::LocalComm, Scheduler};

pub(super) enum Op<T> {
    Put(CommSlice<T>, CommAllocAddr), //for local lamellae put_all is equivalent to put
    Get(CommAllocAddr, CommSlice<T>),
    Atomic,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalFuture<T> {
    pub(super) op: Op<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LocalFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn inner_put(&self, src: &CommSlice<T>, dst: &CommAllocAddr) {
        trace!(
            "putting src: {:?} dst: {:?} len: {} num bytes {}",
            src.usize_addr(),
            dst,
            src.len(),
            src.len() * std::mem::size_of::<T>()
        );
        if !(src.contains(dst) || src.contains(&(dst + src.len()))) {
            unsafe {
                std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), src.len());
            }
        } else {
            unsafe {
                std::ptr::copy(src.as_ptr(), dst.as_mut_ptr(), src.len());
            }
        }
    }
    #[tracing::instrument(skip_all, level = "debug")]
    fn inner_get(&self, src: &CommAllocAddr, dst: &CommSlice<T>) {
        trace!(
            "getting src: {:?} dst: {:?} len: {}",
            src,
            dst.usize_addr(),
            dst.len()
        );
        if !(dst.contains(src) || dst.contains(&(src + dst.len()))) {
            unsafe {
                std::ptr::copy_nonoverlapping(src.as_mut_ptr(), dst.as_mut_ptr(), dst.len());
            }
        } else {
            unsafe {
                std::ptr::copy(src.as_mut_ptr(), dst.as_mut_ptr(), dst.len());
            }
        }
    }
    fn exec_op(&self) {
        match &self.op {
            Op::Put(src, dst) => {
                self.inner_put(src, dst);
            }
            Op::Get(src, dst) => {
                self.inner_get(src, dst);
            }
            Op::Atomic => {}
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
impl<T> PinnedDrop for LocalFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> Future for LocalFuture<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
            *self.project().spawned = true;
        }
        Poll::Ready(())
    }
}

impl<T: Remote> From<LocalFuture<T>> for RdmaHandle<T> {
    fn from(f: LocalFuture<T>) -> RdmaHandle<T> {
        RdmaHandle {
            future: RdmaFuture::Local(f),
        }
    }
}

impl CommRdma for LocalComm {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        _pe: usize,
        src: CommSlice<T>,
        dst: CommAllocAddr,
    ) -> RdmaHandle<T> {
        self.put_amt
            .fetch_add(src.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
        LocalFuture {
            op: Op::Put(src, dst),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: CommSlice<T>,
        dst: CommAllocAddr,
    ) -> RdmaHandle<T> {
        self.put_amt.fetch_add(
            src.len() * std::mem::size_of::<T>() * self.num_pes,
            Ordering::SeqCst,
        );
        LocalFuture {
            op: Op::Put(src, dst),
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
        src: CommAllocAddr,
        dst: CommSlice<T>,
    ) -> RdmaHandle<T> {
        self.get_amt
            .fetch_add(dst.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
        LocalFuture {
            op: Op::Get(src, dst),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
}

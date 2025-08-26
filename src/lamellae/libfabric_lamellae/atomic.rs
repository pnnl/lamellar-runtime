use crate::{
    active_messaging::AMCounters,
    lamellae::comm::{
        atomic::{
            AtomicFetchOpFuture, AtomicFetchOpHandle, AtomicOp, AtomicOpFuture, AtomicOpHandle,
            CommAtomic, NetworkAtomic,
        },
        rdma::RdmaHandle,
        CommAllocAddr, Remote,
    },
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::{comm::LibfabricComm, fabric::Ofi, Scheduler};

use pin_project::{pin_project, pinned_drop};
use std::{
    future::Future,
    mem::MaybeUninit,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tracing::trace;

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAtomicFuture<T> {
    pub(crate) my_pe: usize,
    pub(crate) ofi: Arc<Ofi>,
    pub(super) remote_pe: usize,
    pub(super) op: AtomicOp<T>,
    pub(super) dst: CommAllocAddr,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Send + 'static> LibfabricAtomicFuture<T> {
    fn exec_op(&self) {
        trace!("performing atomic op: {:?} dst: {:?} ", self.op, self.dst);
        unsafe {
            self.ofi
                .atomic_op(self.remote_pe, &self.op, &self.dst)
                .unwrap()
        };
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.ofi.wait_all().unwrap();
        self.spawned = true;
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        // let ofi = self.ofi.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.ofi.wait_all().unwrap() }, counters)
        // self.scheduler.clone().spawn_task(async move {}, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricAtomicFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<LibfabricAtomicFuture<T>> for AtomicOpHandle<T> {
    fn from(f: LibfabricAtomicFuture<T>) -> AtomicOpHandle<T> {
        AtomicOpHandle {
            future: AtomicOpFuture::Libfabric(f),
        }
    }
}

impl<T: Send + 'static> Future for LibfabricAtomicFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
            self.spawned = true;
        }
        self.ofi.wait_all().unwrap();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAtomicFetchFuture<T> {
    pub(crate) my_pe: usize,
    pub(crate) ofi: Arc<Ofi>,
    pub(super) remote_pe: usize,
    pub(super) op: AtomicOp<T>,
    pub(super) dst: CommAllocAddr,
    pub(crate) result: MaybeUninit<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Send + 'static> LibfabricAtomicFetchFuture<T> {
    fn exec_op(&mut self) {
        trace!("performing atomic op: {:?} dst: {:?} ", self.op, self.dst);
        unsafe {
            self.ofi
                .atomic_fetch_op(
                    self.remote_pe,
                    &self.op,
                    &self.dst,
                    std::slice::from_mut(&mut *self.result.as_mut_ptr()),
                )
                .unwrap()
        };
    }
    pub(crate) fn block(mut self) -> T {
        self.exec_op();
        self.ofi.wait_all().unwrap();
        self.spawned = true;
        unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(&mut self.result, &mut res);
            res.assume_init()
        }
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_op();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move {
                self.ofi.wait_all().unwrap();
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
impl<T> PinnedDrop for LibfabricAtomicFetchFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<LibfabricAtomicFetchFuture<T>> for AtomicFetchOpHandle<T> {
    fn from(f: LibfabricAtomicFetchFuture<T>) -> AtomicFetchOpHandle<T> {
        AtomicFetchOpHandle {
            future: AtomicFetchOpFuture::Libfabric(f),
        }
    }
}

impl<T: Send + 'static> Future for LibfabricAtomicFetchFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
            self.spawned = true;
        }
        self.ofi.wait_all().unwrap();
        Poll::Ready(unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(&mut self.result, &mut res);
            res.assume_init()
        })
    }
}

impl CommAtomic for LibfabricComm {
    fn atomic_avail<T: 'static>(&self) -> bool {
        self.ofi.atomic_avail::<T>()
    }
    fn atomic_op<T>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: CommAllocAddr,
    ) -> AtomicOpHandle<T> {
        LibfabricAtomicFuture {
            my_pe: self.my_pe,
            ofi: self.ofi.clone(),
            remote_pe: pe,
            op,
            dst: remote_addr,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn atomic_fetch_op<T>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: CommAllocAddr,
    ) -> AtomicFetchOpHandle<T> {
        LibfabricAtomicFetchFuture {
            my_pe: self.my_pe,
            ofi: self.ofi.clone(),
            remote_pe: pe,
            op: op,
            dst: remote_addr,
            result: MaybeUninit::uninit(),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
}

use crate::{
    active_messaging::AMCounters,
    lamellae::comm::atomic::{
        AtomicFetchOpFuture, AtomicFetchOpHandle, AtomicOp, AtomicOpFuture, AtomicOpHandle,
        CommAllocAtomic,
    },
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::{fabric::LibfabricAlloc, Scheduler};

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
pub(crate) struct LibfabricAllocAtomicFuture<T> {
    pub(crate) alloc: Arc<LibfabricAlloc>,
    pub(super) remote_pes: Vec<usize>,
    pub(crate) offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Send + 'static> LibfabricAllocAtomicFuture<T> {
    fn exec_op(&self) {
        trace!(
            "performing atomic op: {:?} offset: {:?} ",
            self.op,
            self.offset
        );
        for pe in &self.remote_pes {
            unsafe { LibfabricAlloc::atomic_op(&self.alloc, *pe, self.offset, &self.op).unwrap() };
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
impl<T> PinnedDrop for LibfabricAllocAtomicFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<LibfabricAllocAtomicFuture<T>> for AtomicOpHandle<T> {
    fn from(f: LibfabricAllocAtomicFuture<T>) -> AtomicOpHandle<T> {
        AtomicOpHandle {
            future: AtomicOpFuture::LibfabricAlloc(f),
        }
    }
}

impl<T: Send + 'static> Future for LibfabricAllocAtomicFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
            self.spawned = true;
        }
        self.alloc.ofi.wait_all().unwrap();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAllocAtomicFetchFuture<T> {
    pub(crate) alloc: Arc<LibfabricAlloc>,
    pub(super) remote_pe: usize,
    pub(crate) offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) result: MaybeUninit<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Send + 'static> LibfabricAllocAtomicFetchFuture<T> {
    fn exec_op(&mut self) {
        trace!(
            "performing atomic op: {:?} offset: {:?} ",
            self.op,
            self.offset
        );
        unsafe {
            LibfabricAlloc::atomic_fetch_op(
                &self.alloc,
                self.remote_pe,
                self.offset,
                &self.op,
                std::slice::from_mut(&mut *self.result.as_mut_ptr()),
            )
            .unwrap()
        };
    }
    pub(crate) fn block(mut self) -> T {
        self.exec_op();
        self.alloc.ofi.wait_all().unwrap();
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
                self.alloc.ofi.wait_all().unwrap();
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
impl<T> PinnedDrop for LibfabricAllocAtomicFetchFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<LibfabricAllocAtomicFetchFuture<T>> for AtomicFetchOpHandle<T> {
    fn from(f: LibfabricAllocAtomicFetchFuture<T>) -> AtomicFetchOpHandle<T> {
        AtomicFetchOpHandle {
            future: AtomicFetchOpFuture::LibfabricAlloc(f),
        }
    }
}

impl<T: Send + 'static> Future for LibfabricAllocAtomicFetchFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
            self.spawned = true;
        }
        self.alloc.ofi.wait_all().unwrap();
        Poll::Ready(unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(&mut self.result, &mut res);
            res.assume_init()
        })
    }
}

impl CommAllocAtomic for Arc<LibfabricAlloc> {
    fn atomic_op<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        LibfabricAllocAtomicFuture {
            alloc: self.clone(),
            remote_pes: vec![pe],
            offset,
            op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn atomic_op_unmanaged<T: Copy + 'static>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        unsafe {
            LibfabricAlloc::atomic_op(self, pe, offset, &op).unwrap();
        }
    }
    fn atomic_op_all<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        LibfabricAllocAtomicFuture {
            alloc: self.clone(),
            remote_pes: (0..self.num_pes()).collect(),
            offset,
            op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn atomic_op_all_unmanaged<T: Copy + 'static>(&self, op: AtomicOp<T>, offset: usize) {
        for pe in 0..self.num_pes() {
            unsafe {
                LibfabricAlloc::atomic_op(self, pe, offset, &op).unwrap();
            }
        }
    }
    fn atomic_fetch_op<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicFetchOpHandle<T> {
        LibfabricAllocAtomicFetchFuture {
            alloc: self.clone(),
            remote_pe: pe,
            offset,
            op: op,
            result: MaybeUninit::uninit(),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
}

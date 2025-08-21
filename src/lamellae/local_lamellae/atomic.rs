use crate::{
    active_messaging::AMCounters,
    lamellae::{
        comm::atomic::{
            AtomicFetchOpFuture, AtomicFetchOpHandle, AtomicOp, AtomicOpFuture, AtomicOpHandle,
            CommAtomic,
        },
        CommAllocAddr, Remote,
    },
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::{comm::LocalComm, Scheduler};

use pin_project::{pin_project, pinned_drop};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

#[pin_project(PinnedDrop)]
pub(crate) struct LocalAtomicFuture<T> {
    pub(super) op: AtomicOp<T>,
    pub(super) dst: CommAllocAddr,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T> LocalAtomicFuture<T> {
    fn exec_op(&self) {}
    pub(crate) fn block(mut self) {
        self.exec_op();
        // rofi_c_wait();
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
impl<T> PinnedDrop for LocalAtomicFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<LocalAtomicFuture<T>> for AtomicOpHandle<T> {
    fn from(f: LocalAtomicFuture<T>) -> AtomicOpHandle<T> {
        AtomicOpHandle {
            future: AtomicOpFuture::Local(f),
        }
    }
}

impl<T> Future for LocalAtomicFuture<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
            *self.project().spawned = true;
        } else {
        }
        // rofi_c_wait();

        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalAtomicFetchFuture<T> {
    pub(super) op: AtomicOp<T>,
    pub(super) dst: CommAllocAddr,
    pub(super) result: Option<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Send + 'static> LocalAtomicFetchFuture<T> {
    fn exec_op(&self) {}
    pub(crate) fn block(mut self) -> T {
        self.exec_op();
        // rofi_c_wait();
        self.spawned = true;
        self.result.take().expect("Result should be set")
        // Ok(())
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_op();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move { self.result.take().expect("Result should be set") },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LocalAtomicFetchFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<LocalAtomicFetchFuture<T>> for AtomicFetchOpHandle<T> {
    fn from(f: LocalAtomicFetchFuture<T>) -> AtomicFetchOpHandle<T> {
        AtomicFetchOpHandle {
            future: AtomicFetchOpFuture::Local(f),
        }
    }
}

impl<T: Send + 'static> Future for LocalAtomicFetchFuture<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        *this.spawned = true;
        // rofi_c_wait();

        Poll::Ready(this.result.take().expect("Result should be set"))
    }
}

impl CommAtomic for LocalComm {
    fn atomic_avail<T: 'static>(&self) -> bool {
        false
    }
    fn atomic_op<T>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: CommAllocAddr,
    ) -> AtomicOpHandle<T> {
        LocalAtomicFuture {
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
        result: T,
    ) -> AtomicFetchOpHandle<T> {
        LocalAtomicFetchFuture {
            op,
            dst: remote_addr,
            result: Some(result),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
}

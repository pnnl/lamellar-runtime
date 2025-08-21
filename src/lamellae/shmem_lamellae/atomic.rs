use crate::{
    active_messaging::{AMCounters, SyncSend},
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

use super::{comm::ShmemComm, Scheduler};

use pin_project::{pin_project, pinned_drop};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemAtomicFuture<T> {
    pub(super) op: AtomicOp<T>,
    pub(super) dst: CommAllocAddr,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T> ShmemAtomicFuture<T> {
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
impl<T> PinnedDrop for ShmemAtomicFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<ShmemAtomicFuture<T>> for AtomicOpHandle<T> {
    fn from(f: ShmemAtomicFuture<T>) -> AtomicOpHandle<T> {
        AtomicOpHandle {
            future: AtomicOpFuture::Shmem(f),
        }
    }
}

impl<T> Future for ShmemAtomicFuture<T> {
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
pub(crate) struct ShmemAtomicFetchFuture<T> {
    pub(super) op: AtomicOp<T>,
    pub(super) dst: CommAllocAddr,
    pub(super) result: Option<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Send + 'static> ShmemAtomicFetchFuture<T> {
    fn exec_op(&self) {}
    pub(crate) fn block(mut self) -> T {
        self.exec_op();
        // rofi_c_wait();
        self.spawned = true;
        self.result.take().expect("Result should not be None")
        // Ok(())
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_op();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move { self.result.take().expect("Result should not be None") },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for ShmemAtomicFetchFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<ShmemAtomicFetchFuture<T>> for AtomicFetchOpHandle<T> {
    fn from(f: ShmemAtomicFetchFuture<T>) -> AtomicFetchOpHandle<T> {
        AtomicFetchOpHandle {
            future: AtomicFetchOpFuture::Shmem(f),
        }
    }
}

impl<T: Send + 'static> Future for ShmemAtomicFetchFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
            *self.as_mut().project().spawned = true;
        } else {
        }
        // rofi_c_wait();

        Poll::Ready(self.result.take().expect("Result should not be None"))
    }
}

impl CommAtomic for ShmemComm {
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
        ShmemAtomicFuture {
            op: op,
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
        ShmemAtomicFetchFuture {
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

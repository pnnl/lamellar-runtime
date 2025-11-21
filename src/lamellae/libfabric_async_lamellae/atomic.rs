use std::{future::Future, mem::MaybeUninit, pin::Pin, sync::Arc, task::Poll};
use parking_lot::Mutex;
use pin_project::{pin_project, pinned_drop};
use tracing::trace;

use crate::{active_messaging::AMCounters, lamellae::{libfabric_async_lamellae::fabric::{LibfabricAsyncAlloc, OneSidedLibfabricAsyncAlloc}, AtomicFetchOpFuture, AtomicOp, AtomicOpFuture, CommAllocAtomic}, scheduler::Scheduler, warnings::RuntimeWarning, AtomicFetchOpHandle, AtomicOpHandle, LamellarTask};
use crate::lamellae::CommAllocAddr;


struct AtomicFetchOpFutureData<T> {
    pub(crate) alloc: LibfabricAsyncAlloc,
    pub(super) remote_pe: usize,
    pub(crate) offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) result: MaybeUninit<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncAtomicFetchFuture<T> {
    fut_data: Option<AtomicFetchOpFutureData<T>>,
    fut: Option<Pin<Box<dyn std::future::Future<Output = T> + Send>>>,
    // pub(crate) alloc: LibfabricAsyncAlloc,
    // pub(super) remote_pe: usize,
    // pub(crate) offset: usize,
    // pub(super) op: AtomicOp<T>,
    // pub(crate) result: MaybeUninit<T>,
    // pub(crate) scheduler: Arc<Scheduler>,
    // pub(crate) counters: Vec<Arc<AMCounters>>,
    // pub(crate) spawned: bool,
}

impl<T: Copy + Send + 'static> LibfabricAsyncAtomicFetchFuture<T> {
    pub(crate) fn block(mut self) -> T {
        let fut_data = self.fut_data.take().unwrap();
        fut_data.block()
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        let fut_data = self.fut_data.take().unwrap();
        fut_data.spawn()
    }
}

impl<T: Copy + Send + 'static> AtomicFetchOpFutureData<T> {
    async fn exec_op(mut self) -> T {
        trace!(
            "performing atomic op: {:?} offset: {:?} ",
            self.op,
            self.offset
        );
        unsafe {
            LibfabricAsyncAlloc::atomic_fetch_op_inner(
                &self.alloc,
                self.remote_pe,
                self.offset,
                self.op.clone(),
                std::slice::from_mut(&mut *self.result.as_mut_ptr()),
            )
            .await
            .unwrap()
        };
        unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(&mut self.result, &mut res);
            res.assume_init()
        }
    }
    pub(crate) fn block(mut self) -> T {
        // self.spawned = true;
        self.scheduler
            .clone()
            .block_on(async move {
                self.exec_op().await
            })
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        // self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler
            .clone()
            .spawn_task(async move {
                self.exec_op().await
            }, counters)
    }
}

impl<T: Copy + Send + 'static> Future for LibfabricAsyncAtomicFetchFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let mut mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => {
                fut.as_mut().poll(cx)
            }
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            },
        }
    }
}
    


#[pinned_drop]
impl<T> PinnedDrop for LibfabricAsyncAtomicFetchFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        // if !self.spawned {
        //     RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        // }
    }
}

impl<T> From<LibfabricAsyncAtomicFetchFuture<T>> for AtomicFetchOpHandle<T> {
    fn from(f: LibfabricAsyncAtomicFetchFuture<T>) -> AtomicFetchOpHandle<T> {
        AtomicFetchOpHandle {
            future: AtomicFetchOpFuture::LibfabricAsync(f),
        }
    }
}

struct AtomicOpFutureData<T> {
    pub(crate) alloc: LibfabricAsyncAlloc,
    pub(super) remote_pes: Vec<usize>,
    pub(crate) offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
}


#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncAtomicFuture<T> {
    fut_data: Option<AtomicOpFutureData<T>>,
    fut: Option<Pin<Box<dyn std::future::Future<Output = ()> + Send>>>,
    // pub(crate) alloc: LibfabricAsyncAlloc,
    // pub(super) remote_pes: Vec<usize>,
    // pub(crate) offset: usize,
    // pub(super) op: AtomicOp<T>,
    // pub(crate) scheduler: Arc<Scheduler>,
    // pub(crate) counters: Vec<Arc<AMCounters>>,
    // pub(crate) spawned: bool,
}


impl<T: Copy + Send + 'static> LibfabricAsyncAtomicFuture<T> {
    pub(crate) fn block(mut self) {
        let fut_data = self.fut_data.take().unwrap();
        fut_data.block()
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        let fut_data = self.fut_data.take().unwrap();
        fut_data.spawn()
    }
}

impl<T: Copy + Send + 'static> AtomicOpFutureData<T> {
    async fn exec_op(self) {
        trace!(
            "performing atomic op: {:?} offset: {:?} ",
            self.op,
            self.offset
        );
        // let op = self.op.lock();
        for pe in &self.remote_pes {
            LibfabricAsyncAlloc::atomic_op_inner(&self.alloc, *pe, self.offset, self.op.clone()).await.unwrap();
        }
    }
    pub(crate) fn block(mut self) {
        // self.spawned = true;
        self.scheduler
            .clone()
            .block_on(async move {
                self.exec_op().await;
            });
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        // self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler
            .clone()
            .spawn_task(async move {
                self.exec_op().await;
            }, counters)
    }
}


impl<T: Copy + Send + 'static> Future for LibfabricAsyncAtomicFuture<T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let mut mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => {
                fut.as_mut().poll(cx)
            }
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            },
        }
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricAsyncAtomicFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        // if !self.spawned {
        //     RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        // }
    }
}


impl<T> From<LibfabricAsyncAtomicFuture<T>> for AtomicOpHandle<T> {
    fn from(f: LibfabricAsyncAtomicFuture<T>) -> AtomicOpHandle<T> {
        AtomicOpHandle {
            future: AtomicOpFuture::LibfabricAsync(f),
        }
    }
}


impl CommAllocAtomic for LibfabricAsyncAlloc {

    fn atomic_op<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        LibfabricAsyncAtomicFuture {
            fut_data: Some(AtomicOpFutureData {
            alloc: self.clone(),
            remote_pes: vec![pe],
            offset,
            op,
            // spawned: false,
            scheduler: scheduler.clone(),
            counters,
            }),
            fut: None,
        }
        .into()
    }
    fn atomic_op_unmanaged<T: Copy + 'static>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        LibfabricAsyncAlloc::atomic_op_inner_unmanaged(self, pe, offset, &op).unwrap();
    }
    fn atomic_op_all<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        LibfabricAsyncAtomicFuture {
            fut_data: Some(AtomicOpFutureData {
            alloc: self.clone(),
            remote_pes: (0..self.num_pes()).collect(),
            offset,
            op,
            // spawned: false,
            scheduler: scheduler.clone(),
            counters,
            }),
            fut: None,
        }
        .into()
    }
    fn atomic_op_all_unmanaged<T: Copy + 'static>(&self, op: AtomicOp<T>, offset: usize) {
        for pe in 0..self.num_pes() {
            LibfabricAsyncAlloc::atomic_op_inner_unmanaged(self, pe, offset, &op).unwrap();
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
        LibfabricAsyncAtomicFetchFuture {
            fut_data: Some(AtomicFetchOpFutureData {
            alloc: self.clone(),
            remote_pe: pe,
            offset,
            op,
            result: MaybeUninit::uninit(),
            // spawned: false,
            scheduler: scheduler.clone(),
            counters,
            }),
            fut: None,
        }
        .into()
    }
}

impl CommAllocAtomic for OneSidedLibfabricAsyncAlloc {
    fn atomic_op<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedLibfabricAsyncAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricAsyncAtomicFuture {
            fut_data: Some(AtomicOpFutureData {
            alloc: self.alloc.clone(),
            remote_pes: vec![pe],
            offset,
            op,
            // spawned: false,
            scheduler: scheduler.clone(),
            counters,
            }),
            fut: None,
        }
        .into()
    }
    fn atomic_op_unmanaged<T: Copy + 'static>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedLibfabricAsyncAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricAsyncAlloc::atomic_op_inner_unmanaged(&self.alloc, pe, offset, &op).unwrap();
    }
    fn atomic_op_all<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        self.atomic_op(scheduler, counters, op, self.remote_pe, offset)
    }
    fn atomic_op_all_unmanaged<T: Copy + 'static>(&self, op: AtomicOp<T>, offset: usize) {
        self.atomic_op_unmanaged(op, self.remote_pe, offset);
    }
    fn atomic_fetch_op<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicFetchOpHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "atomic fetch op called on OneSidedLibfabricAsyncAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricAsyncAtomicFetchFuture {
            fut_data: Some(AtomicFetchOpFutureData {
            alloc: self.alloc.clone(),
            remote_pe: pe,
            offset,
            op,
            result: MaybeUninit::uninit(),
            // spawned: false,
            scheduler: scheduler.clone(),
            counters,
            }),
            fut: None,
        }
        .into()
    }
}

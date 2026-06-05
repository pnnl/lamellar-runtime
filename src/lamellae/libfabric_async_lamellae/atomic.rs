use pin_project::{pin_project, pinned_drop};
use std::{future::Future, pin::Pin, sync::Arc, task::Poll};
use tracing::trace;

use crate::{
    active_messaging::AMCounters,
    lamellae::{
        libfabric_async_lamellae::fabric::{LibfabricAsyncAlloc, OneSidedLibfabricAsyncAlloc},
        AtomicCompareExchangeFuture, AtomicFetchOpFuture, AtomicOp, AtomicOpFuture,
        CommAllocAtomic,
    },
    scheduler::Scheduler,
    AtomicCompareExchangeOpHandle, AtomicFetchOpHandle, AtomicOpHandle, LamellarTask, Remote,
};

fn compare_exchange_result<T: PartialEq>(old: T, current: T) -> Result<T, T> {
    if old == current {
        Ok(old)
    } else {
        Err(old)
    }
}

struct AtomicFetchOpFutureData<T> {
    pub(crate) alloc: LibfabricAsyncAlloc,
    pub(super) remote_pe: usize,
    pub(crate) offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) result: Box<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncAtomicFetchFuture<T> {
    fut_data: Option<AtomicFetchOpFutureData<T>>,
    fut: Option<Pin<Box<dyn std::future::Future<Output = T> + Send>>>,
}

impl<T: Remote + Send + 'static> LibfabricAsyncAtomicFetchFuture<T> {
    pub(crate) fn block(mut self) -> T {
        let fut_data = self.fut_data.take().unwrap();
        fut_data.block()
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        let fut_data = self.fut_data.take().unwrap();
        fut_data.spawn()
    }
}

impl<T: Remote + Send + 'static> AtomicFetchOpFutureData<T> {
    async fn exec_op(mut self) -> T {
        trace!(
            "performing atomic op: {:?} offset: {:?} ",
            self.op,
            self.offset
        );
            LibfabricAsyncAlloc::atomic_fetch_op_inner(
                &self.alloc,
                self.remote_pe,
                self.offset,
                &mut self.op,
                std::slice::from_mut(self.result.as_mut()),
            )
            .await
            .unwrap();
        
        *self.result
    }
    pub(crate) fn block(self) -> T {
        // self.spawned = true;
        self.scheduler
            .clone()
            .block_on(async move { self.exec_op().await })
    }

    pub(crate) fn spawn(self) -> LamellarTask<T> {
        // self.spawned = true;
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.exec_op().await }, counters)
    }
}

impl<T: Remote + Send + 'static> Future for LibfabricAsyncAtomicFetchFuture<T> {
    type Output = T;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
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
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
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
    // pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    // pub(crate) spawned: bool,
}

impl<T: Remote + Send + 'static> LibfabricAsyncAtomicFuture<T> {
    pub(crate) fn block(mut self) {
        let fut_data = self.fut_data.take().unwrap();
        fut_data.block()
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        let fut_data = self.fut_data.take().unwrap();
        fut_data.spawn()
    }
}

impl<T: Remote + Send + 'static> AtomicOpFutureData<T> {
    async fn exec_op(mut self) {
        trace!(
            "performing atomic op: {:?} offset: {:?} ",
            self.op,
            self.offset
        );
        // let op = self.op.lock();
        for pe in &self.remote_pes {
            LibfabricAsyncAlloc::atomic_op_inner(&self.alloc, *pe, self.offset, &mut self.op)
                .await
                .unwrap();
        }
    }
    pub(crate) fn block(self) {
        // self.spawned = true;
        self.scheduler.clone().block_on(async move {
            self.exec_op().await;
        });
    }
    pub(crate) fn spawn(self) -> LamellarTask<()> {
        // self.spawned = true;
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(
            async move {
                self.exec_op().await;
            },
            counters,
        )
    }
}

impl<T: Remote + Send + 'static> Future for LibfabricAsyncAtomicFuture<T> {
    type Output = ();

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
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

struct AtomicCompareExchangeFutureData<T> {
    pub(crate) alloc: LibfabricAsyncAlloc,
    pub(super) remote_pe: usize,
    pub(crate) offset: usize,
    current: Pin<Box<T>>,
    new: Pin<Box<T>>,
    pub(crate) result: Box<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncAtomicCompareExchangeFuture<T> {
    fut_data: Option<AtomicCompareExchangeFutureData<T>>,
    fut: Option<Pin<Box<dyn std::future::Future<Output = Result<T, T>> + Send>>>,
}

impl<T: Remote + Send + PartialEq + 'static> AtomicCompareExchangeFutureData<T> {
    async fn exec_op(mut self) -> Result<T, T> {
            LibfabricAsyncAlloc::atomic_compare_exchange_op_inner(
                &self.alloc,
                self.remote_pe,
                self.offset,
                self.current.as_ref().get_ref(),
                self.new.as_ref().get_ref(),
                std::slice::from_mut(self.result.as_mut()),
            )
            .await
            .unwrap();
        compare_exchange_result(*self.result, *self.current)
    }
    pub(crate) fn block(self) -> Result<T, T> {
        self.scheduler
            .clone()
            .block_on(async move { self.exec_op().await })
    }
    pub(crate) fn spawn( self) -> LamellarTask<Result<T, T>> {
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.exec_op().await }, counters)
    }
}

impl<T: Remote + Send + PartialEq + 'static> LibfabricAsyncAtomicCompareExchangeFuture<T> {
    pub(crate) fn block(mut self) -> Result<T, T> {
        let fut_data = self.fut_data.take().unwrap();
        fut_data.block()
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Result<T, T>> {
        let fut_data = self.fut_data.take().unwrap();
        fut_data.spawn()
    }
}

impl<T: Remote + Send + PartialEq + 'static> Future
    for LibfabricAsyncAtomicCompareExchangeFuture<T>
{
    type Output = Result<T, T>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut.as_mut() {
            Some(fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricAsyncAtomicCompareExchangeFuture<T> {
    fn drop(self: Pin<&mut Self>) {}
}

impl<T> From<LibfabricAsyncAtomicCompareExchangeFuture<T>> for AtomicCompareExchangeOpHandle<T> {
    fn from(f: LibfabricAsyncAtomicCompareExchangeFuture<T>) -> AtomicCompareExchangeOpHandle<T> {
        AtomicCompareExchangeOpHandle {
            future: AtomicCompareExchangeFuture::LibfabricAsync(f),
        }
    }
}

impl CommAllocAtomic for LibfabricAsyncAlloc {
    fn atomic_op<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
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
    fn atomic_op_unmanaged<T: Remote + 'static>(
        &self,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        LibfabricAsyncAlloc::atomic_op_inner_unmanaged(self, pe, offset, &mut op).unwrap();
    }
    fn atomic_op_all<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
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
    fn atomic_op_all_unmanaged<T: Remote + 'static>(&self, mut op: AtomicOp<T>, offset: usize) {
        for pe in 0..self.num_pes() {
            LibfabricAsyncAlloc::atomic_op_inner_unmanaged(self, pe, offset, &mut op).unwrap();
        }
    }
    fn atomic_fetch_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
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
                result: Box::new(T::default()),
                // spawned: false,
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
        }
        .into()
    }
    fn atomic_compare_exchange<T: Remote + PartialEq>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        current: T,
        new: T,
        pe: usize,
        offset: usize,
    ) -> AtomicCompareExchangeOpHandle<T> {
        LibfabricAsyncAtomicCompareExchangeFuture {
            fut_data: Some(AtomicCompareExchangeFutureData {
                alloc: self.clone(),
                remote_pe: pe,
                offset,
                current: Box::pin(current),
                new: Box::pin(new),
                result: Box::new(T::default()),
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
        }
        .into()
    }
    fn atomic_fetch_op_blocking<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> T {
        let mut result = T::default();
        scheduler.clone().block_on(async {
                LibfabricAsyncAlloc::atomic_fetch_op_inner(
                    self,
                    pe,
                    offset,
                    &mut op,
                    std::slice::from_mut(&mut result),
                )
                .await
                .unwrap();
        });
        result
    }
    fn atomic_op_blocking<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        scheduler.clone().block_on(async {
            LibfabricAsyncAlloc::atomic_op_inner(self, pe, offset, &mut op)
                .await
                .unwrap();
        });
    }
    fn atomic_compare_exchange_blocking<T: Remote + PartialEq>(
        &self,
        scheduler: &Arc<Scheduler>,
        current: T,
        new: T,
        pe: usize,
        offset: usize,
    ) -> Result<T, T> {
        let mut result = T::default();
        scheduler.clone().block_on(async {
                LibfabricAsyncAlloc::atomic_compare_exchange_op_inner(
                    self,
                    pe,
                    offset,
                    &current,
                    &new,
                    std::slice::from_mut(&mut result),
                )
                .await
                .unwrap();
        });
        compare_exchange_result(result, current)
    }
}

impl CommAllocAtomic for OneSidedLibfabricAsyncAlloc {
    fn atomic_op<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
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
    fn atomic_op_unmanaged<T: Remote + 'static>(
        &self,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        assert_eq!(
            pe, self.remote_pe,
            "atomic op called on OneSidedLibfabricAsyncAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricAsyncAlloc::atomic_op_inner_unmanaged(&self.alloc, pe, offset, &mut op).unwrap();
    }
    fn atomic_op_blocking<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        assert_eq!(
            pe, self.remote_pe,
            "atomic_op_blocking called on OneSidedLibfabricAsyncAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        scheduler.clone().block_on(async {
            LibfabricAsyncAlloc::atomic_op_inner(&self.alloc, pe, offset, &mut op)
                .await
                .unwrap();
        });
    }
    fn atomic_op_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        self.atomic_op(scheduler, counters, op, self.remote_pe, offset)
    }
    fn atomic_op_all_unmanaged<T: Remote + 'static>(&self, op: AtomicOp<T>, offset: usize) {
        self.atomic_op_unmanaged(op, self.remote_pe, offset);
    }
    fn atomic_fetch_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
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
                result: Box::new(T::default()),
                // spawned: false,
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
        }
        .into()
    }
    fn atomic_compare_exchange<T: Remote + PartialEq>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        current: T,
        new: T,
        pe: usize,
        offset: usize,
    ) -> AtomicCompareExchangeOpHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "atomic compare exchange called on OneSidedLibfabricAsyncAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricAsyncAtomicCompareExchangeFuture {
            fut_data: Some(AtomicCompareExchangeFutureData {
                alloc: self.alloc.clone(),
                remote_pe: pe,
                offset,
                current: Box::pin(current),
                new: Box::pin(new),
                result: Box::new(T::default()),
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
        }
        .into()
    }
    fn atomic_fetch_op_blocking<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> T {
        assert_eq!(pe, self.remote_pe, "atomic_fetch_op_blocking called on OneSidedLibfabricAsyncAlloc with incorrect pe: {} expected pe: {}", pe, self.remote_pe);
        let mut result = T::default();
        scheduler.clone().block_on(async {
                LibfabricAsyncAlloc::atomic_fetch_op_inner(
                    &self.alloc,
                    pe,
                    offset,
                    &mut op,
                    std::slice::from_mut(&mut result),
                )
                .await
                .unwrap();
        });
        result
    }
    fn atomic_compare_exchange_blocking<T: Remote + PartialEq>(
        &self,
        scheduler: &Arc<Scheduler>,
        current: T,
        new: T,
        pe: usize,
        offset: usize,
    ) -> Result<T, T> {
        assert_eq!(
            pe, self.remote_pe,
            "blocking atomic compare exchange called on OneSidedLibfabricAsyncAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut result = T::default();
        scheduler.clone().block_on(async {
                LibfabricAsyncAlloc::atomic_compare_exchange_op_inner(
                    &self.alloc,
                    pe,
                    offset,
                    &current,
                    &new,
                    std::slice::from_mut(&mut result),
                )
                .await
                .unwrap();
        });
        compare_exchange_result(result, current)
    }
}

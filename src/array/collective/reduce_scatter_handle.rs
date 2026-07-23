use futures_util::Future;
use pin_project::pin_project;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::array::LamellarByteArray;
use crate::lamellae::comm::collective::CollectiveReduceScatterOpHandle;
use crate::warnings::RuntimeWarning;
use crate::{
    active_messaging::AMCounters, lamellae::collective::CollectiveReduceScatterIntoBufferOpHandle,
    scheduler::Scheduler, AsLamellarBuffer, Dist, LamellarTask,
};

#[pin_project]
pub struct ArrayCollectiveReduceScatterHandle<T: Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveReduceScatterState<T>,
    pub(crate) spawned: bool,
}

#[pin_project]
pub(crate) struct CollectiveReduceScatterManualOpHandle<T: Dist> {
    #[pin]
    pub(crate) future: Pin<Box<dyn Future<Output = Vec<T>> + Send>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
}

impl<T: Dist> Future for CollectiveReduceScatterManualOpHandle<T> {
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.future.poll(cx)
    }
}

impl<T: Dist> CollectiveReduceScatterManualOpHandle<T> {
    pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }

    pub(crate) fn block(self) -> Vec<T> {
        self.scheduler.clone().block_on(self)
    }
}

#[pin_project(project = ArrayCollectiveReduceScatterStateProj)]
pub(crate) enum ArrayCollectiveReduceScatterState<T: Dist> {
    CollectiveReduceScatter(#[pin] CollectiveReduceScatterOpHandle<T>),
    CollectiveReduceScatterManual(#[pin] CollectiveReduceScatterManualOpHandle<T>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist> ArrayCollectiveReduceScatterHandle<T> {
    /// Spawn the collective reduce_scatter operation.
    ///
    /// # Collective Operation
    /// All PEs must participate; collective barriers used internally.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.reduce_scatter_init(lamellar::array::operations::Sum);
    /// let task = handle.spawn();
    ///```
    ///
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Vec<T>> {
        let task = match self.state {
            ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req) => req.spawn(),
            ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    /// Block until the collective reduce_scatter operation completes.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.reduce_scatter_init(lamellar::array::operations::Sum);
    /// let result = handle.block();
    ///```
    pub fn block(mut self) -> Vec<T> {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveReduceScatterHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req) => req.block(),
            ArrayCollectiveReduceScatterState::CollectiveReduceScatterManual(req) => req.block(),
        }
    }
}

impl<T: Dist> Future for ArrayCollectiveReduceScatterHandle<T> {
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveReduceScatterStateProj::CollectiveReduceScatter(req) => req.poll(cx),
            ArrayCollectiveReduceScatterStateProj::CollectiveReduceScatterManual(req) => {
                req.poll(cx)
            }
        }
    }
}

#[pin_project]
pub struct ArrayCollectiveReduceScatterIntoBufferHandle<T: Dist, B: AsLamellarBuffer<T>> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveReduceScatterIntoBufferState<T, B>,
    pub(crate) spawned: bool,
}

#[pin_project]
pub(crate) struct CollectiveReduceScatterIntoBufferManualOpHandle {
    #[pin]
    pub(crate) future: Pin<Box<dyn Future<Output = ()> + Send>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
}

impl Future for CollectiveReduceScatterIntoBufferManualOpHandle {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.future.poll(cx)
    }
}

impl CollectiveReduceScatterIntoBufferManualOpHandle {
    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }

    pub(crate) fn block(self) {
        self.scheduler.clone().block_on(self)
    }
}

#[pin_project(project = ArrayCollectiveReduceScatterIntoBufferStateProj)]
pub(crate) enum ArrayCollectiveReduceScatterIntoBufferState<T: Dist, B: AsLamellarBuffer<T>> {
    CollectiveReduceScatterIntoBuffer(#[pin] CollectiveReduceScatterIntoBufferOpHandle<T, B>),
    CollectiveReduceScatterIntoBufferManual(#[pin] CollectiveReduceScatterIntoBufferManualOpHandle),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist, B: AsLamellarBuffer<T>> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
    /// Spawn the collective reduce_scatter_into_buffer operation.
    ///
    /// # Collective Operation
    /// All PEs must participate; collective barriers used internally.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let buf = world.alloc_shared_mem::<usize>(100).block();
    /// let handle = array.reduce_scatter_into_buffer_init(lamellar::array::operations::Sum, &buf);
    /// let task = handle.spawn();
    ///```
    ///
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<()> {
        let task = match self.state {
            ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(req) => req.spawn(),
            ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    /// Block until the collective reduce_scatter_into_buffer operation completes.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let buf = world.alloc_shared_mem::<usize>(100).block();
    /// let handle = array.reduce_scatter_into_buffer_init(lamellar::array::operations::Sum, &buf);
    /// let result = handle.block();
    ///```
    pub fn block(mut self) {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveReduceScatterIntoBufferHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(req) => req.block(),
            ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBufferManual(req) => req.block(),
        }
    }
}

impl<T: Dist, B: AsLamellarBuffer<T>> Future
    for ArrayCollectiveReduceScatterIntoBufferHandle<T, B>
{
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveReduceScatterIntoBufferStateProj::CollectiveReduceScatterIntoBuffer(req) => {
                req.poll(cx)
            },
            ArrayCollectiveReduceScatterIntoBufferStateProj::CollectiveReduceScatterIntoBufferManual(req) => {
                req.poll(cx)
            }
        }
    }
}

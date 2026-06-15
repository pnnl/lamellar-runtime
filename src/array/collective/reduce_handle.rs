use futures_util::Future;
use pin_project::pin_project;
use std::{
    pin::Pin, sync::Arc, task::{Context, Poll}
};

use crate::{AsLamellarBuffer, Dist, LamellarTask, active_messaging::AMCounters, lamellae::collective::{CollectiveAllReduceInPlaceOpHandle, CollectiveAllReduceIntoBufferOpHandle, CollectiveReduceInPlaceOpHandle, CollectiveReduceIntoBufferOpHandle, CollectiveReduceOpHandle}, scheduler::Scheduler};
use crate::array::LamellarByteArray;
use crate::lamellae::comm::collective::CollectiveAllReduceOpHandle;
use crate::warnings::RuntimeWarning;

#[pin_project]
pub struct ArrayCollectiveAllReduceHandle<T: Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveAllReduceState<T>,
    pub(crate) spawned: bool,
}

#[must_use = " CollectiveAllReduceManualOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub(crate) struct CollectiveAllReduceManualOpHandle<T: Dist> {
    #[pin] pub(crate) future: Pin<Box<dyn Future<Output = Vec<T>> + Send>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
}

impl <T: Dist> Future for CollectiveAllReduceManualOpHandle<T> {
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.future.poll(cx)
    }
}

impl<T: Dist> CollectiveAllReduceManualOpHandle<T> {
    pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }

    pub(crate) fn block(self) -> Vec<T> {
        self.scheduler.clone().block_on(self)
    }
}



#[pin_project(project = ArrayCollectiveAllReduceStateProj)]
pub(crate) enum ArrayCollectiveAllReduceState<T: Dist> {
    CollectiveAllReduce(#[pin] CollectiveAllReduceOpHandle<T>),
    CollectiveAllReduceManual(#[pin] CollectiveAllReduceManualOpHandle<T>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist> ArrayCollectiveAllReduceHandle<T> {
    /// Spawn the collective allreduce operation.
    ///
    /// # Collective Operation
    /// All PEs must participate; collective barriers used internally.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.allreduce_init(lamellar::array::operations::Sum);
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
            ArrayCollectiveAllReduceState::CollectiveAllReduce(req) => req.spawn(),
            ArrayCollectiveAllReduceState::CollectiveAllReduceManual(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    /// Block until the collective allreduce operation completes.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.allreduce_init(lamellar::array::operations::Sum);
    /// let result = handle.block();
    ///```
    pub fn block(mut self) -> Vec<T> {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveAllReduceHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveAllReduceState::CollectiveAllReduce(req) => req.block(),
            ArrayCollectiveAllReduceState::CollectiveAllReduceManual(req) => req.block(),
        }
    }
}


impl<T: Dist> Future for ArrayCollectiveAllReduceHandle<T> {
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveAllReduceStateProj::CollectiveAllReduce(req) => {
                req.poll(cx)
            }
            ArrayCollectiveAllReduceStateProj::CollectiveAllReduceManual(req) => {
                req.poll(cx)
            }
        }
    }
}

#[pin_project]
pub struct ArrayCollectiveAllReduceIntoBufferHandle<T: Dist, B: AsLamellarBuffer<T>> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveAllReduceIntoBufferState<T, B>,
    pub(crate) spawned: bool,
}


#[must_use = " CollectiveAllReduceIntoBufferManualOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub(crate) struct CollectiveAllReduceIntoBufferManualOpHandle {
    #[pin] pub(crate) future: Pin<Box<dyn Future<Output = ()> + Send>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
}

impl Future for CollectiveAllReduceIntoBufferManualOpHandle {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.future.poll(cx)
    }
}

impl CollectiveAllReduceIntoBufferManualOpHandle {
    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }

    pub(crate) fn block(self) {
        self.scheduler.clone().block_on(self)
    }
}


#[pin_project(project = ArrayCollectiveAllReduceIntoBufferStateProj)]
pub(crate) enum ArrayCollectiveAllReduceIntoBufferState<T: Dist, B: AsLamellarBuffer<T>> {
    CollectiveAllReduceIntoBuffer(#[pin] CollectiveAllReduceIntoBufferOpHandle<T, B>),
    CollectiveAllReduceIntoBufferManual(#[pin] CollectiveAllReduceIntoBufferManualOpHandle),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist, B: AsLamellarBuffer<T>> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
    /// Spawn the collective allreduce_into_buffer operation.
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
    /// let handle = array.allreduce_into_buffer_init(lamellar::array::operations::Sum, &buf);
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
            ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req) => req.spawn(),
            ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    /// Block until the collective allreduce_into_buffer operation completes.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let buf = world.alloc_shared_mem::<usize>(100).block();
    /// let handle = array.allreduce_into_buffer_init(lamellar::array::operations::Sum, &buf);
    /// let result = handle.block();
    ///```
    pub fn block(mut self)  {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveAllReduceIntoBufferHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req) => req.block(),
            ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBufferManual(req) => req.block(),
        }
    }
}


impl<T: Dist, B: AsLamellarBuffer<T>> Future for ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveAllReduceIntoBufferStateProj::CollectiveAllReduceIntoBuffer(req) => {
                req.poll(cx)
            }
            ArrayCollectiveAllReduceIntoBufferStateProj::CollectiveAllReduceIntoBufferManual(req) => {
                req.poll(cx)
            }
        }
    }
}

#[pin_project]
pub struct ArrayCollectiveAllReduceInPlaceHandle<T: Dist, B: AsLamellarBuffer<T>> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveAllReduceInPlaceState<T, B>,
    pub(crate) spawned: bool,
}

#[pin_project(project = ArrayCollectiveAllReduceInPlaceStateProj)]
pub(crate) enum ArrayCollectiveAllReduceInPlaceState<T: Dist, B: AsLamellarBuffer<T>> {
    CollectiveAllReduceInPlace(#[pin] CollectiveAllReduceInPlaceOpHandle<T, B>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist, B: AsLamellarBuffer<T>> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
    /// Spawn the collective allreduce_in_place operation.
    ///
    /// # Collective Operation
    /// All PEs must participate; collective barriers used internally.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let buf = world.alloc_shared_mem::<usize>(100).block();
    /// let handle = buf.allreduce_in_place_init(lamellar::array::operations::Sum);
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
            ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    /// Block until the collective allreduce_in_place operation completes.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let buf = world.alloc_shared_mem::<usize>(100).block();
    /// let handle = buf.allreduce_in_place_init(lamellar::array::operations::Sum);
    /// let result = handle.block();
    ///```
    pub fn block(mut self) {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveAllReduceInPlaceHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req) => req.block(),
        }
    }
}


impl<T: Dist, B: AsLamellarBuffer<T>> Future for ArrayCollectiveAllReduceInPlaceHandle<T, B> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveAllReduceInPlaceStateProj::CollectiveAllReduceInPlace(req) => {
                req.poll(cx)
            }
        }
    }
}

#[pin_project]
pub struct ArrayCollectiveReduceHandle<T: Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveReduceState<T>,
    pub(crate) spawned: bool,
}

#[pin_project]
pub(crate) struct CollectiveReduceManualOpHandle<T: Dist> {
    #[pin]
    pub(crate) future: Pin<Box<dyn Future<Output = Option<Vec<T>>> + Send>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
}


impl<T: Dist> Future for CollectiveReduceManualOpHandle<T> {
    type Output = Option<Vec<T>>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.future.poll(cx)
    }
}

impl<T: Dist> CollectiveReduceManualOpHandle<T> {
    pub(crate) fn spawn(self) -> LamellarTask<Option<Vec<T>>> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }

    pub(crate) fn block(self) -> Option<Vec<T>> {
        self.scheduler.clone().block_on(self)
    }
}

#[pin_project(project = ArrayCollectiveReduceStateProj)]
pub(crate) enum ArrayCollectiveReduceState<T: Dist> {
    CollectiveReduce(#[pin] CollectiveReduceOpHandle<T>),
    CollectiveReduceManual(#[pin] CollectiveReduceManualOpHandle<T>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist> ArrayCollectiveReduceHandle<T> {
    /// Spawn the collective reduce operation.
    ///
    /// # Collective Operation
    /// All PEs must participate; collective barriers used internally.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.reduce_init(0, lamellar::array::operations::Sum);
    /// let task = handle.spawn();
    ///```
    ///
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Option<Vec<T>>> {
        let task = match self.state {
            ArrayCollectiveReduceState::CollectiveReduce(req) => req.spawn(),
            ArrayCollectiveReduceState::CollectiveReduceManual(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    /// Block until the collective reduce operation completes.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.reduce_init(0, lamellar::array::operations::Sum);
    /// let result = handle.block();
    ///```
    pub fn block(mut self) -> Option<Vec<T>> {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveReduceHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveReduceState::CollectiveReduce(req) => req.block(),
            ArrayCollectiveReduceState::CollectiveReduceManual(req) => req.block(),
        }
    }
}


impl<T: Dist> Future for ArrayCollectiveReduceHandle<T> {
    type Output = Option<Vec<T>>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveReduceStateProj::CollectiveReduce(req) => {
                req.poll(cx)
            },
            ArrayCollectiveReduceStateProj::CollectiveReduceManual(req) => {
                req.poll(cx)
            }
        }
    }
}

#[pin_project]
pub struct ArrayCollectiveReduceIntoBufferHandle<T: Dist, B: AsLamellarBuffer<T>> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveReduceIntoBufferState<T, B>,
    pub(crate) spawned: bool,
}


#[pin_project]
pub(crate) struct CollectiveReduceIntoBufferManualOpHandle {
    #[pin]
    pub(crate) future: Pin<Box<dyn Future<Output = ()> + Send>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
}


impl Future for CollectiveReduceIntoBufferManualOpHandle {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.future.poll(cx)
    }
}

impl CollectiveReduceIntoBufferManualOpHandle {
    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }

    pub(crate) fn block(self) {
        self.scheduler.clone().block_on(self)
    }
}

#[pin_project(project = ArrayCollectiveReduceIntoBufferStateProj)]
pub(crate) enum ArrayCollectiveReduceIntoBufferState<T: Dist, B: AsLamellarBuffer<T>> {
    CollectiveReduceIntoBuffer(#[pin] CollectiveReduceIntoBufferOpHandle<T, B>),
    CollectiveReduceIntoBufferManual(#[pin] CollectiveReduceIntoBufferManualOpHandle),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist, B: AsLamellarBuffer<T>> ArrayCollectiveReduceIntoBufferHandle<T, B> {
    /// Spawn the collective reduce_into_buffer operation.
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
    /// let handle = array.reduce_into_buffer_init(0, lamellar::array::operations::Sum, &buf);
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
            ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req) => req.spawn(),
            ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    /// Block until the collective reduce_into_buffer operation completes.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let buf = world.alloc_shared_mem::<usize>(100).block();
    /// let handle = array.reduce_into_buffer_init(0, lamellar::array::operations::Sum, &buf);
    /// let result = handle.block();
    ///```
    pub fn block(mut self) {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveReduceIntoBufferHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req) => req.block(),
            ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBufferManual(req) => req.block(),

        }
    }
}


impl<T: Dist, B: AsLamellarBuffer<T>> Future for ArrayCollectiveReduceIntoBufferHandle<T, B> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveReduceIntoBufferStateProj::CollectiveReduceIntoBuffer(req) => {
                req.poll(cx)
            }
            ArrayCollectiveReduceIntoBufferStateProj::CollectiveReduceIntoBufferManual(req) => {
                req.poll(cx)
            }
        }
    }
}

#[pin_project]
pub(crate) struct ArrayCollectiveReduceInPlaceHandle<T: Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveReduceInPlaceState<T>,
    pub(crate) spawned: bool,
}

#[pin_project(project = ArrayCollectiveReduceInPlaceStateProj)]
pub(crate) enum ArrayCollectiveReduceInPlaceState<T: Dist> {
    #[allow(dead_code)]
    CollectiveReduceInPlace(#[pin] CollectiveReduceInPlaceOpHandle<T>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

// impl<T: Dist> ArrayCollectiveReduceInPlaceHandle<T> {
//     /// This method will spawn the associated Array RDMA Operation on the work queue,
//     /// initiating the remote operation.
//     ///
//     /// This function returns a handle that can be used to wait for the operation to complete
//     #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
//     pub fn spawn(self) -> LamellarTask<()> {
//         let task = match self.state {
//             ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req) => req.spawn(),
//         };
//         self.spawned = true;
//         task
//     }
//     pub fn block(self) {
//         RuntimeWarning::BlockingCall(
//             "ArrayCollectiveReduceInPlaceHandle::block",
//             "<handle>.spawn() or <handle>.await",
//         )
//         .print();
//         self.spawned = true;
//         match self.state {
//             ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req) => req.block(),
//         }
//     }
// }


impl<T: Dist> Future for ArrayCollectiveReduceInPlaceHandle<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveReduceInPlaceStateProj::CollectiveReduceInPlace(req) => {
                req.poll(cx)
            }
        }
    }
}


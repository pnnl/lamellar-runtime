use std::{future::Future, pin::Pin, sync::Arc, task::{Context, Poll}};

use pin_project::pin_project;

use crate::{AsLamellarBuffer, Dist, LamellarTask, active_messaging::AMCounters, array::LamellarByteArray, lamellae::collective::{CollectiveAllToAllIntoBufferOpHandle, CollectiveAllToAllOpHandle, CollectiveBroadcastIntoBufferOpHandle, CollectiveBroadcastOpHandle, CollectiveScatterIntoBufferOpHandle, CollectiveScatterOpHandle}, scheduler::Scheduler, warnings::RuntimeWarning};


#[pin_project]
pub struct ArrayCollectiveAllToAllHandle<T: Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveAllToAllState<T>,
    pub(crate) spawned: bool,
}

#[must_use = " CollectiveAllToAllManualOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub(crate) struct CollectiveAllToAllManualOpHandle<T: Dist> {
    #[pin]
    pub(crate) future: Pin<Box<dyn Future<Output = Vec<T>> + Send>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
}

impl<T: Dist> Future for CollectiveAllToAllManualOpHandle<T> {
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.future.poll(cx)
    }
}

impl<T: Dist> CollectiveAllToAllManualOpHandle<T> {
    pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }

    pub(crate) fn block(self) -> Vec<T> {
        self.scheduler.clone().block_on(self)
    }
}


#[pin_project(project = ArrayCollectiveAllToAllStateProj)]
pub(crate) enum ArrayCollectiveAllToAllState<T: Dist> {
    CollectiveAllToAll(#[pin] CollectiveAllToAllOpHandle<T>),
    CollectiveAllToAllManual(#[pin] CollectiveAllToAllManualOpHandle<T>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist> ArrayCollectiveAllToAllHandle<T> {
    /// Spawn the collective all-to-all operation on the work queue.
    ///
    /// # Collective Operation
    /// All PEs must participate in this call; collective barriers are used internally.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.alltoall_init::<usize>();
    /// let task = handle.spawn();
    ///```
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Vec<T>> {
        let task = match self.state {
            ArrayCollectiveAllToAllState::CollectiveAllToAll(req) => req.spawn(),
            ArrayCollectiveAllToAllState::CollectiveAllToAllManual(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    /// Block until the collective operation completes and return results.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.alltoall_init::<usize>();
    /// let result = handle.block();
    ///```
    pub fn block(mut self) -> Vec<T> {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveAllToAllHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveAllToAllState::CollectiveAllToAll(req) => req.block(),
            ArrayCollectiveAllToAllState::CollectiveAllToAllManual(req) => req.block(),
        }
    }
}


impl<T: Dist> Future for ArrayCollectiveAllToAllHandle<T> {
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveAllToAllStateProj::CollectiveAllToAll(req) => {
                req.poll(cx)
            }
            ArrayCollectiveAllToAllStateProj::CollectiveAllToAllManual(req) => {
                req.poll(cx)
            }
        }
    }
}

#[pin_project]
pub struct ArrayCollectiveAllToAllIntoBufferHandle<T: Dist, B: AsLamellarBuffer<T>> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveAllToAllIntoBufferState<T, B>,
    pub(crate) spawned: bool,
}

#[pin_project]
pub(crate) struct CollectiveAllToAllIntoBufferManualOpHandle {
    #[pin]
    pub(crate) future: Pin<Box<dyn Future<Output = ()> + Send>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
}

impl Future for CollectiveAllToAllIntoBufferManualOpHandle {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.future.poll(cx)
    }
}

impl CollectiveAllToAllIntoBufferManualOpHandle {
    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }

    pub(crate) fn block(self) -> () {
        self.scheduler.clone().block_on(self)
    }
}


#[pin_project(project = ArrayCollectiveAllToAllIntoBufferStateProj)]
pub(crate) enum ArrayCollectiveAllToAllIntoBufferState<T: Dist, B: AsLamellarBuffer<T>> {
    CollectiveAllToAllIntoBuffer(#[pin] CollectiveAllToAllIntoBufferOpHandle<T, B>),
    CollectiveAllToAllIntoBufferManual(#[pin] CollectiveAllToAllIntoBufferManualOpHandle),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist, B: AsLamellarBuffer<T>> ArrayCollectiveAllToAllIntoBufferHandle<T, B> {
    /// Spawn the collective all-to-all operation writing into a provided buffer.
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
    /// let handle = array.alltoall_init_into_buffer::<usize>(&buf);
    /// let task = handle.spawn();
    ///```
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<()> {
        let task = match self.state {
            ArrayCollectiveAllToAllIntoBufferState::CollectiveAllToAllIntoBuffer(req) => req.spawn(),
            ArrayCollectiveAllToAllIntoBufferState::CollectiveAllToAllIntoBufferManual(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    /// Block until the collective operation completes into a buffer.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let buf = world.alloc_shared_mem::<usize>(100).block();
    /// let handle = array.alltoall_init_into_buffer::<usize>(&buf);
    /// handle.block();
    ///```
    pub fn block(mut self)  {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveAllToAllIntoBufferHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveAllToAllIntoBufferState::CollectiveAllToAllIntoBuffer(req) => req.block(),
            ArrayCollectiveAllToAllIntoBufferState::CollectiveAllToAllIntoBufferManual(req) => req.block(),
        }
    }
}


impl<T: Dist, B: AsLamellarBuffer<T>> Future for ArrayCollectiveAllToAllIntoBufferHandle<T, B> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveAllToAllIntoBufferStateProj::CollectiveAllToAllIntoBuffer(req) => {
                req.poll(cx)
            },
            ArrayCollectiveAllToAllIntoBufferStateProj::CollectiveAllToAllIntoBufferManual(req) => {
                req.poll(cx)
            }
        }
    }
}


#[pin_project]
pub struct ArrayCollectiveBroadcastHandle<T: Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveBroadcastState<T>,
    pub(crate) spawned: bool,
}

#[must_use = " CollectiveBroadcastManualOpHandle: 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project]
pub(crate) struct CollectiveBroadcastManualOpHandle<T: Dist> {
    #[pin]
    pub(crate) future: Pin<Box<dyn Future<Output = Option<Vec<T>>> + Send>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
}

impl<T: Dist> Future for CollectiveBroadcastManualOpHandle<T> {
    type Output = Option<Vec<T>>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.future.poll(cx)
    }
}

impl<T: Dist> CollectiveBroadcastManualOpHandle<T> {
    pub(crate) fn spawn(self) -> LamellarTask<Option<Vec<T>>> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }

    pub(crate) fn block(self) -> Option<Vec<T>> {
        self.scheduler.clone().block_on(self)
    }
}


#[pin_project(project = ArrayCollectiveBroadcastStateProj)]
pub(crate) enum ArrayCollectiveBroadcastState<T: Dist> {
    CollectiveBroadcast(#[pin] CollectiveBroadcastOpHandle<T>),
    CollectiveBroadcastManual(#[pin] CollectiveBroadcastManualOpHandle<T>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist> ArrayCollectiveBroadcastHandle<T> {
    /// Spawn the collective broadcast operation.
    ///
    /// # Collective Operation
    /// All PEs must participate; collective barriers used internally.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.broadcast_init::<usize>(0);
    /// let task = handle.spawn();
    ///```
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Option<Vec<T>>> {
        let task = match self.state {
            ArrayCollectiveBroadcastState::CollectiveBroadcast(req) => req.spawn(),
            ArrayCollectiveBroadcastState::CollectiveBroadcastManual(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    /// Block until the collective broadcast operation completes.
    ///
    /// Returns the broadcast data only on non-root PEs; root PEs receive `None`.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.broadcast_init::<usize>(0);
    /// let result = handle.block();
    ///```
    pub fn block(mut self) -> Option<Vec<T>> {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveBroadcastHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveBroadcastState::CollectiveBroadcast(req) => req.block(),
            ArrayCollectiveBroadcastState::CollectiveBroadcastManual(req) => req.block(),
        }
    }
}


impl<T: Dist> Future for ArrayCollectiveBroadcastHandle<T> {
    type Output = Option<Vec<T>>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveBroadcastStateProj::CollectiveBroadcast(req) => {
                req.poll(cx)
            },
            ArrayCollectiveBroadcastStateProj::CollectiveBroadcastManual(req) => {
                req.poll(cx)
            }
        }
    }
}


#[pin_project]
pub(crate) struct CollectiveBroadcastIntoBufferManualOpHandle {
    #[pin]
    pub(crate) future: Pin<Box<dyn Future<Output = ()> + Send>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
}

impl Future for CollectiveBroadcastIntoBufferManualOpHandle {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.future.poll(cx)
    }
}

impl CollectiveBroadcastIntoBufferManualOpHandle {
    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }

    pub(crate) fn block(self) -> () {
        self.scheduler.clone().block_on(self)
    }
}


#[pin_project]
pub struct ArrayCollectiveBroadcastIntoBufferHandle<T: Dist, B: AsLamellarBuffer<T>> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveBroadcastIntoBufferState<T, B>,
    pub(crate) spawned: bool,
}

#[pin_project(project = ArrayCollectiveBroadcastIntoBufferStateProj)]
pub(crate) enum ArrayCollectiveBroadcastIntoBufferState<T: Dist, B: AsLamellarBuffer<T>> {
    CollectiveBroadcastIntoBuffer(#[pin] CollectiveBroadcastIntoBufferOpHandle<T, B>),
    CollectiveBroadcastIntoBufferManual(#[pin] CollectiveBroadcastIntoBufferManualOpHandle),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist, B: AsLamellarBuffer<T>> ArrayCollectiveBroadcastIntoBufferHandle<T, B> {
    /// Spawn the collective broadcast operation writing into a provided buffer.
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
    /// let handle = array.broadcast_init_into_buffer::<usize>(&buf, 0);
    /// let task = handle.spawn();
    ///```
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<()> {
        let task = match self.state {
            ArrayCollectiveBroadcastIntoBufferState::CollectiveBroadcastIntoBuffer(req) => req.spawn(),
            ArrayCollectiveBroadcastIntoBufferState::CollectiveBroadcastIntoBufferManual(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    /// Block until the collective broadcast operation completes into a buffer.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let buf = world.alloc_shared_mem::<usize>(100).block();
    /// let handle = array.broadcast_init_into_buffer::<usize>(&buf, 0);
    /// handle.block();
    ///```
    pub fn block(mut self)  {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveBroadcastIntoBufferHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveBroadcastIntoBufferState::CollectiveBroadcastIntoBuffer(req) => req.block(),
            ArrayCollectiveBroadcastIntoBufferState::CollectiveBroadcastIntoBufferManual(req) => req.block(),
        }
    }
}


impl<T: Dist, B: AsLamellarBuffer<T>> Future for ArrayCollectiveBroadcastIntoBufferHandle<T, B> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveBroadcastIntoBufferStateProj::CollectiveBroadcastIntoBuffer(req) => {
                req.poll(cx)
            },
            ArrayCollectiveBroadcastIntoBufferStateProj::CollectiveBroadcastIntoBufferManual(req) => {
                req.poll(cx)
            }
        }
    }
}

#[pin_project]
pub(crate) struct CollectiveScatterManualOpHandle<T: Dist> {
    #[pin]
    pub(crate) future: Pin<Box<dyn Future<Output = Vec<T>> + Send>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
}

impl<T: Dist> Future for CollectiveScatterManualOpHandle<T> {
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.future.poll(cx)
    }
}

impl<T: Dist> CollectiveScatterManualOpHandle<T> {
    pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }

    pub(crate) fn block(self) -> Vec<T> {
        self.scheduler.clone().block_on(self)
    }
}

#[pin_project]
pub(crate) struct CollectiveScatterIntoBufferManualOpHandle {
    #[pin]
    pub(crate) future: Pin<Box<dyn Future<Output = ()> + Send>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
}

impl Future for CollectiveScatterIntoBufferManualOpHandle {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.future.poll(cx)
    }
}

impl CollectiveScatterIntoBufferManualOpHandle {
    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }

    pub(crate) fn block(self) -> () {
        self.scheduler.clone().block_on(self)
    }
}



#[pin_project]
pub struct ArrayCollectiveScatterHandle<T: Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveScatterState<T>,
    pub(crate) spawned: bool,
}

#[pin_project(project = ArrayCollectiveScatterStateProj)]
pub(crate) enum ArrayCollectiveScatterState<T: Dist> {
    CollectiveScatter(#[pin] CollectiveScatterOpHandle<T>),
    CollectiveScatterManual(#[pin] CollectiveScatterManualOpHandle<T>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist> ArrayCollectiveScatterHandle<T> {
    /// Spawn the collective scatter operation.
    ///
    /// # Collective Operation
    /// All PEs must participate; collective barriers used internally.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.scatter_init::<usize>(0);
    /// let task = handle.spawn();
    ///```
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Vec<T>> {
        let task = match self.state {
            ArrayCollectiveScatterState::CollectiveScatter(req) => req.spawn(),
            ArrayCollectiveScatterState::CollectiveScatterManual(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    /// Block until the collective scatter operation completes.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.scatter_init::<usize>(0);
    /// let result = handle.block();
    ///```
    pub fn block(mut self) -> Vec<T> {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveScatterHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveScatterState::CollectiveScatter(req) => req.block(),
            ArrayCollectiveScatterState::CollectiveScatterManual(req) => req.block(),
        }
    }
}


impl<T: Dist> Future for ArrayCollectiveScatterHandle<T> {
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveScatterStateProj::CollectiveScatter(req) => {
                req.poll(cx)
            },
            ArrayCollectiveScatterStateProj::CollectiveScatterManual(req) => {
                req.poll(cx)
            }
        }
    }
}

#[pin_project]
pub struct ArrayCollectiveScatterIntoBufferHandle<T: Dist, B: AsLamellarBuffer<T>> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveScatterIntoBufferState<T, B>,
    pub(crate) spawned: bool,
}

#[pin_project(project = ArrayCollectiveScatterIntoBufferStateProj)]
pub(crate) enum ArrayCollectiveScatterIntoBufferState<T: Dist, B: AsLamellarBuffer<T>> {
    CollectiveScatterIntoBuffer(#[pin] CollectiveScatterIntoBufferOpHandle<T, B>),
    CollectiveScatterIntoBufferManual(#[pin] CollectiveScatterIntoBufferManualOpHandle),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist, B: AsLamellarBuffer<T>> ArrayCollectiveScatterIntoBufferHandle<T, B> {
    /// Spawn the collective scatter operation writing into a provided buffer.
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
    /// let handle = array.scatter_init_into_buffer::<usize>(&buf, 0);
    /// let task = handle.spawn();
    ///```
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<()> {
        let task = match self.state {
            ArrayCollectiveScatterIntoBufferState::CollectiveScatterIntoBuffer(req) => req.spawn(),
            ArrayCollectiveScatterIntoBufferState::CollectiveScatterIntoBufferManual(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    /// Block until the collective scatter operation completes into a buffer.
    ///
    /// # Examples
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let buf = world.alloc_shared_mem::<usize>(100).block();
    /// let handle = array.scatter_init_into_buffer::<usize>(&buf, 0);
    /// handle.block();
    ///```
    pub fn block(mut self)  {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveScatterIntoBufferHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveScatterIntoBufferState::CollectiveScatterIntoBuffer(req) => req.block(),
            ArrayCollectiveScatterIntoBufferState::CollectiveScatterIntoBufferManual(req) => req.block(),
        }
    }
}


impl<T: Dist, B: AsLamellarBuffer<T>> Future for ArrayCollectiveScatterIntoBufferHandle<T, B> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveScatterIntoBufferStateProj::CollectiveScatterIntoBuffer(req) => {
                req.poll(cx)
            },
            ArrayCollectiveScatterIntoBufferStateProj::CollectiveScatterIntoBufferManual(req) => {
                req.poll(cx)
            }
        }
    }
}

use std::{future::Future, pin::Pin, task::{Context, Poll}};

use pin_project::pin_project;

use crate::{array::LamellarByteArray, lamellae::collective::{CollectiveAllGatherIntoBufferOpHandle, CollectiveAllGatherOpHandle, CollectiveGatherIntoBufferOpHandle, CollectiveGatherOpHandle}, warnings::RuntimeWarning, AsLamellarBuffer, Dist, LamellarTask};


#[pin_project]
pub struct ArrayCollectiveAllGatherHandle<T: Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveAllGatherState<T>,
    pub(crate) spawned: bool,
}

#[pin_project(project = ArrayCollectiveAllGatherStateProj)]
pub(crate) enum ArrayCollectiveAllGatherState<T: Dist> {
    CollectiveAllGather(#[pin] CollectiveAllGatherOpHandle<T>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist> ArrayCollectiveAllGatherHandle<T> {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Vec<T>> {
        let task = match self.state {
            ArrayCollectiveAllGatherState::CollectiveAllGather(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    pub fn block(mut self) -> Vec<T> {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveAllGatherHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveAllGatherState::CollectiveAllGather(req) => req.block(),
        }
    }
}


impl<T: Dist> Future for ArrayCollectiveAllGatherHandle<T> {
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveAllGatherStateProj::CollectiveAllGather(req) => {
                req.poll(cx)
            }
        }
    }
}

#[pin_project]
pub struct ArrayCollectiveAllGatherIntoBufferHandle<T: Dist, B: AsLamellarBuffer<T>> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveAllGatherIntoBufferState<T, B>,
    pub(crate) spawned: bool,
}

#[pin_project(project = ArrayCollectiveAllGatherIntoBufferStateProj)]
pub(crate) enum ArrayCollectiveAllGatherIntoBufferState<T: Dist, B: AsLamellarBuffer<T>> {
    CollectiveAllGatherIntoBuffer(#[pin] CollectiveAllGatherIntoBufferOpHandle<T, B>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist, B: AsLamellarBuffer<T>> ArrayCollectiveAllGatherIntoBufferHandle<T, B> {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<()> {
        let task = match self.state {
            ArrayCollectiveAllGatherIntoBufferState::CollectiveAllGatherIntoBuffer(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    pub fn block(mut self)  {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveAllGatherIntoBufferHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveAllGatherIntoBufferState::CollectiveAllGatherIntoBuffer(req) => req.block(),
        }
    }
}


impl<T: Dist, B: AsLamellarBuffer<T>> Future for ArrayCollectiveAllGatherIntoBufferHandle<T, B> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveAllGatherIntoBufferStateProj::CollectiveAllGatherIntoBuffer(req) => {
                req.poll(cx)
            }
        }
    }
}


#[pin_project]
pub struct ArrayCollectiveGatherHandle<T: Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveGatherState<T>,
    pub(crate) spawned: bool,
}

#[pin_project(project = ArrayCollectiveGatherStateProj)]
pub(crate) enum ArrayCollectiveGatherState<T: Dist> {
    CollectiveGather(#[pin] CollectiveGatherOpHandle<T>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist> ArrayCollectiveGatherHandle<T> {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Option<Vec<T>>> {
        let task = match self.state {
            ArrayCollectiveGatherState::CollectiveGather(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    pub fn block(mut self) -> Option<Vec<T>> {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveGatherHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveGatherState::CollectiveGather(req) => req.block(),
        }
    }
}


impl<T: Dist> Future for ArrayCollectiveGatherHandle<T> {
    type Output = Option<Vec<T>>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveGatherStateProj::CollectiveGather(req) => {
                req.poll(cx)
            }
        }
    }
}

#[pin_project]
pub struct ArrayCollectiveGatherIntoBufferHandle<T: Dist, B: AsLamellarBuffer<T>> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveGatherIntoBufferState<T, B>,
    pub(crate) spawned: bool,
}

#[pin_project(project = ArrayCollectiveGatherIntoBufferStateProj)]
pub(crate) enum ArrayCollectiveGatherIntoBufferState<T: Dist, B: AsLamellarBuffer<T>> {
    CollectiveGatherIntoBuffer(#[pin] CollectiveGatherIntoBufferOpHandle<T, B>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist, B: AsLamellarBuffer<T>> ArrayCollectiveGatherIntoBufferHandle<T, B> {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<()> {
        let task = match self.state {
            ArrayCollectiveGatherIntoBufferState::CollectiveGatherIntoBuffer(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    pub fn block(mut self) {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveGatherIntoBufferHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveGatherIntoBufferState::CollectiveGatherIntoBuffer(req) => req.block(),
        }
    }
}


impl<T: Dist, B: AsLamellarBuffer<T>> Future for ArrayCollectiveGatherIntoBufferHandle<T, B> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveGatherIntoBufferStateProj::CollectiveGatherIntoBuffer(req) => {
                req.poll(cx)
            }
        }
    }
}
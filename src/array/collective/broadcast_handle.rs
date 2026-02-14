use std::{future::Future, pin::Pin, task::{Context, Poll}};

use pin_project::pin_project;

use crate::{array::LamellarByteArray, lamellae::collective::{CollectiveAllBroadcastIntoBufferOpHandle, CollectiveAllBroadcastOpHandle, CollectiveBroadcastIntoBufferOpHandle, CollectiveBroadcastOpHandle, CollectiveScatterIntoBufferOpHandle, CollectiveScatterOpHandle}, warnings::RuntimeWarning, AsLamellarBuffer, Dist, LamellarTask};


#[pin_project]
pub struct ArrayCollectiveAllBroadcastHandle<T: Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveAllBroadcastState<T>,
    pub(crate) spawned: bool,
}

#[pin_project(project = ArrayCollectiveAllBroadcastStateProj)]
pub(crate) enum ArrayCollectiveAllBroadcastState<T: Dist> {
    CollectiveAllBroadcast(#[pin] CollectiveAllBroadcastOpHandle<T>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist> ArrayCollectiveAllBroadcastHandle<T> {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Vec<T>> {
        let task = match self.state {
            ArrayCollectiveAllBroadcastState::CollectiveAllBroadcast(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    pub fn block(mut self) -> Vec<T> {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveAllBroadcastHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveAllBroadcastState::CollectiveAllBroadcast(req) => req.block(),
        }
    }
}


impl<T: Dist> Future for ArrayCollectiveAllBroadcastHandle<T> {
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveAllBroadcastStateProj::CollectiveAllBroadcast(req) => {
                req.poll(cx)
            }
        }
    }
}

#[pin_project]
pub struct ArrayCollectiveAllBroadcastIntoBufferHandle<T: Dist, B: AsLamellarBuffer<T>> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveAllBroadcastIntoBufferState<T, B>,
    pub(crate) spawned: bool,
}

#[pin_project(project = ArrayCollectiveAllBroadcastIntoBufferStateProj)]
pub(crate) enum ArrayCollectiveAllBroadcastIntoBufferState<T: Dist, B: AsLamellarBuffer<T>> {
    CollectiveAllBroadcastIntoBuffer(#[pin] CollectiveAllBroadcastIntoBufferOpHandle<T, B>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist, B: AsLamellarBuffer<T>> ArrayCollectiveAllBroadcastIntoBufferHandle<T, B> {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<()> {
        let task = match self.state {
            ArrayCollectiveAllBroadcastIntoBufferState::CollectiveAllBroadcastIntoBuffer(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    pub fn block(mut self)  {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveAllBroadcastIntoBufferHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveAllBroadcastIntoBufferState::CollectiveAllBroadcastIntoBuffer(req) => req.block(),
        }
    }
}


impl<T: Dist, B: AsLamellarBuffer<T>> Future for ArrayCollectiveAllBroadcastIntoBufferHandle<T, B> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveAllBroadcastIntoBufferStateProj::CollectiveAllBroadcastIntoBuffer(req) => {
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

#[pin_project(project = ArrayCollectiveBroadcastStateProj)]
pub(crate) enum ArrayCollectiveBroadcastState<T: Dist> {
    CollectiveBroadcast(#[pin] CollectiveBroadcastOpHandle<T>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist> ArrayCollectiveBroadcastHandle<T> {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Option<Vec<T>>> {
        let task = match self.state {
            ArrayCollectiveBroadcastState::CollectiveBroadcast(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    pub fn block(mut self) -> Option<Vec<T>> {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveBroadcastHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveBroadcastState::CollectiveBroadcast(req) => req.block(),
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
            }
        }
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
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist, B: AsLamellarBuffer<T>> ArrayCollectiveBroadcastIntoBufferHandle<T, B> {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<()> {
        let task = match self.state {
            ArrayCollectiveBroadcastIntoBufferState::CollectiveBroadcastIntoBuffer(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    pub fn block(mut self)  {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveBroadcastIntoBufferHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveBroadcastIntoBufferState::CollectiveBroadcastIntoBuffer(req) => req.block(),
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
            }
        }
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
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist> ArrayCollectiveScatterHandle<T> {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Vec<T>> {
        let task = match self.state {
            ArrayCollectiveScatterState::CollectiveScatter(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    pub fn block(mut self) -> Vec<T> {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveScatterHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveScatterState::CollectiveScatter(req) => req.block(),
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
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist, B: AsLamellarBuffer<T>> ArrayCollectiveScatterIntoBufferHandle<T, B> {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<()> {
        let task = match self.state {
            ArrayCollectiveScatterIntoBufferState::CollectiveScatterIntoBuffer(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    pub fn block(mut self)  {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveScatterIntoBufferHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveScatterIntoBufferState::CollectiveScatterIntoBuffer(req) => req.block(),
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
            }
        }
    }
}

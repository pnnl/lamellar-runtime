use futures_util::Future;
use pin_project::pin_project;
use std::{
    pin::Pin,
    sync::{atomic::*, Arc},
    task::{Context, Poll},
};

use crate::{lamellae::collective::{CollectiveAllReduceInPlaceOpHandle, CollectiveAllReduceIntoBufferOpHandle, CollectiveReduceInPlaceOpHandle, CollectiveReduceIntoBufferOpHandle, CollectiveReduceOpHandle}, AsLamellarBuffer, Dist, LamellarTask};
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

#[pin_project(project = ArrayCollectiveAllReduceStateProj)]
pub(crate) enum ArrayCollectiveAllReduceState<T: Dist> {
    CollectiveAllReduce(#[pin] CollectiveAllReduceOpHandle<T>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist> ArrayCollectiveAllReduceHandle<T> {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Vec<T>> {
        let task = match self.state {
            ArrayCollectiveAllReduceState::CollectiveAllReduce(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    pub fn block(mut self) -> Vec<T> {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveAllReduceHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveAllReduceState::CollectiveAllReduce(req) => req.block(),
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

#[pin_project(project = ArrayCollectiveAllReduceIntoBufferStateProj)]
pub(crate) enum ArrayCollectiveAllReduceIntoBufferState<T: Dist, B: AsLamellarBuffer<T>> {
    CollectiveAllReduceIntoBuffer(#[pin] CollectiveAllReduceIntoBufferOpHandle<T, B>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist, B: AsLamellarBuffer<T>> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<()> {
        let task = match self.state {
            ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    pub fn block(mut self)  {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveAllReduceIntoBufferHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req) => req.block(),
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
        }
    }
}

#[pin_project]
pub struct ArrayCollectiveAllReduceInPlaceHandle<T: Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveAllReduceInPlaceState<T>,
    pub(crate) spawned: bool,
}

#[pin_project(project = ArrayCollectiveAllReduceInPlaceStateProj)]
pub(crate) enum ArrayCollectiveAllReduceInPlaceState<T: Dist> {
    CollectiveAllReduceInPlace(#[pin] CollectiveAllReduceInPlaceOpHandle<T>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist> ArrayCollectiveAllReduceInPlaceHandle<T> {
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


impl<T: Dist> Future for ArrayCollectiveAllReduceInPlaceHandle<T> {
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

#[pin_project(project = ArrayCollectiveReduceStateProj)]
pub(crate) enum ArrayCollectiveReduceState<T: Dist> {
    CollectiveReduce(#[pin] CollectiveReduceOpHandle<T>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist> ArrayCollectiveReduceHandle<T> {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Option<Vec<T>>> {
        let task = match self.state {
            ArrayCollectiveReduceState::CollectiveReduce(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    pub fn block(mut self) -> Option<Vec<T>> {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveReduceHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveReduceState::CollectiveReduce(req) => req.block(),
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

#[pin_project(project = ArrayCollectiveReduceIntoBufferStateProj)]
pub(crate) enum ArrayCollectiveReduceIntoBufferState<T: Dist, B: AsLamellarBuffer<T>> {
    CollectiveReduceIntoBuffer(#[pin] CollectiveReduceIntoBufferOpHandle<T, B>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist, B: AsLamellarBuffer<T>> ArrayCollectiveReduceIntoBufferHandle<T, B> {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<()> {
        let task = match self.state {
            ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    pub fn block(mut self) {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveReduceIntoBufferHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req) => req.block(),
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
        }
    }
}

#[pin_project]
pub struct ArrayCollectiveReduceInPlaceHandle<T: Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveReduceInPlaceState<T>,
    pub(crate) spawned: bool,
}

#[pin_project(project = ArrayCollectiveReduceInPlaceStateProj)]
pub(crate) enum ArrayCollectiveReduceInPlaceState<T: Dist> {
    CollectiveReduceInPlace(#[pin] CollectiveReduceInPlaceOpHandle<T>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist> ArrayCollectiveReduceInPlaceHandle<T> {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<()> {
        let task = match self.state {
            ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    pub fn block(mut self) {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveReduceInPlaceHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req) => req.block(),
        }
    }
}


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

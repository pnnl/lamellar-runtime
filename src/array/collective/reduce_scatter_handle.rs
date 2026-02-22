use futures_util::Future;
use pin_project::pin_project;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::{lamellae::collective::{CollectiveReduceScatterIntoBufferOpHandle, CollectiveReduceInPlaceOpHandle, CollectiveReduceIntoBufferOpHandle, CollectiveReduceOpHandle}, AsLamellarBuffer, Dist, LamellarTask};
use crate::array::LamellarByteArray;
use crate::lamellae::comm::collective::CollectiveReduceScatterOpHandle;
use crate::warnings::RuntimeWarning;

#[pin_project]
pub struct ArrayCollectiveReduceScatterHandle<T: Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayCollectiveReduceScatterState<T>,
    pub(crate) spawned: bool,
}

#[pin_project(project = ArrayCollectiveReduceScatterStateProj)]
pub(crate) enum ArrayCollectiveReduceScatterState<T: Dist> {
    CollectiveReduceScatter(#[pin] CollectiveReduceScatterOpHandle<T>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist> ArrayCollectiveReduceScatterHandle<T> {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Vec<T>> {
        let task = match self.state {
            ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    pub fn block(mut self) -> Vec<T> {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveReduceScatterHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req) => req.block(),
        }
    }
}


impl<T: Dist> Future for ArrayCollectiveReduceScatterHandle<T> {
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveReduceScatterStateProj::CollectiveReduceScatter(req) => {
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

#[pin_project(project = ArrayCollectiveReduceScatterIntoBufferStateProj)]
pub(crate) enum ArrayCollectiveReduceScatterIntoBufferState<T: Dist, B: AsLamellarBuffer<T>> {
    CollectiveReduceScatterIntoBuffer(#[pin] CollectiveReduceScatterIntoBufferOpHandle<T, B>),
    // LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    // RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // // LoadOp(ArrayFetchOpHandle<T>),
    // RdmaGet(RdmaGetHandle<T>),
    // AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist, B: AsLamellarBuffer<T>> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<()> {
        let task = match self.state {
            ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    pub fn block(mut self)  {
        RuntimeWarning::BlockingCall(
            "ArrayCollectiveReduceScatterIntoBufferHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(req) => req.block(),
        }
    }
}


impl<T: Dist, B: AsLamellarBuffer<T>> Future for ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            ArrayCollectiveReduceScatterIntoBufferStateProj::CollectiveReduceScatterIntoBuffer(req) => {
                req.poll(cx)
            }
        }
    }
}

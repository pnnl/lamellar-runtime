use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    active_messaging::{LocalAmHandle, MultiAmHandle},
    array::{ArrayOpHandle, LamellarByteArray},
    warnings::RuntimeWarning,
    AmHandle, AtomicOpHandle, Dist, LamellarTask, RdmaHandle,
};

pub struct ArrayRdmaPutHandle<T: Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    pub(crate) state: ArrayRdmaPutState<T>,
    pub(crate) spawned: bool,
}

pub(crate) enum ArrayRdmaPutState<T: Dist> {
    LocalAmPut(LocalAmHandle<()>),     //Am is initiated as a local am
    RemoteAmPut(AmHandle<()>),         //Am is initiated as a remote am
    RemoteAmPutAll(MultiAmHandle<()>), //Am is initiated as a remote am to all PEs
    StoreOp(ArrayOpHandle<T>),
    RdmaPut(RdmaHandle<T>),
    AtomicPut(AtomicOpHandle<T>),
    MultiRdmaPut(Vec<RdmaHandle<T>>),
}

impl<T: Dist> ArrayRdmaPutHandle<T> {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<()> {
        let task = match self.state {
            ArrayRdmaPutState::LocalAmPut(req) => req.spawn(),
            ArrayRdmaPutState::RemoteAmPut(req) => req.spawn(),
            ArrayRdmaPutState::RemoteAmPutAll(req) => {
                let req = req.spawn();
                self.array.team().spawn(async move {
                    req.await;
                })
            }
            ArrayRdmaPutState::StoreOp(req) => req.spawn(),
            ArrayRdmaPutState::RdmaPut(req) => req.spawn(),
            ArrayRdmaPutState::AtomicPut(req) => req.spawn(),
            ArrayRdmaPutState::MultiRdmaPut(ref mut reqs) => {
                let tasks = reqs.drain(..).map(|req| req.spawn()).collect::<Vec<_>>();
                self.array.team().spawn(async move {
                    for task in tasks {
                        task.await;
                    }
                })
            }
        };
        self.spawned = true;
        task
    }
    pub fn block(mut self) {
        RuntimeWarning::BlockingCall(
            "ArrayRdmaHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayRdmaPutState::LocalAmPut(req) => req.block(),
            ArrayRdmaPutState::RemoteAmPut(req) => req.block(),
            ArrayRdmaPutState::RemoteAmPutAll(req) => {
                req.block();
            }
            ArrayRdmaPutState::StoreOp(req) => req.block(),
            ArrayRdmaPutState::RdmaPut(req) => req.block(),
            ArrayRdmaPutState::AtomicPut(req) => req.block(),
            ArrayRdmaPutState::MultiRdmaPut(ref mut reqs) => {
                let tasks = reqs.drain(..).map(|req| req.spawn()).collect::<Vec<_>>();
                self.array.team().block_on(async move {
                    for task in tasks {
                        task.await;
                    }
                })
            }
        }
    }
}

impl<T: Dist> Future for ArrayRdmaPutHandle<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let res = match &mut this.state {
            ArrayRdmaPutState::LocalAmPut(req) => {
                if let Poll::Ready(_) = Pin::new(req).poll(cx) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
            ArrayRdmaPutState::RemoteAmPut(req) => {
                if let Poll::Ready(_) = Pin::new(req).poll(cx) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
            ArrayRdmaPutState::RemoteAmPutAll(req) => {
                if let Poll::Ready(_) = Pin::new(req).poll(cx) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
            ArrayRdmaPutState::StoreOp(req) => {
                if let Poll::Ready(_) = Pin::new(req).poll(cx) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
            ArrayRdmaPutState::RdmaPut(req) => {
                if let Poll::Ready(_) = Pin::new(req).poll(cx) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
            ArrayRdmaPutState::AtomicPut(req) => {
                if let Poll::Ready(_) = Pin::new(req).poll(cx) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
            ArrayRdmaPutState::MultiRdmaPut(reqs) => {
                reqs.retain_mut(|req| {
                    if let Poll::Ready(_) = Pin::new(req).poll(cx) {
                        false
                    } else {
                        true
                    }
                });
                if reqs.is_empty() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
        };
        this.spawned = true;
        res
    }
}

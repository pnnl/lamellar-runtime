use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    array::LamellarByteArray, warnings::RuntimeWarning, AmHandle, AtomicOpHandle, Dist,
    LamellarTask, RdmaHandle,
};

pub struct ArrayRdmaHandle2<T: Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    pub(crate) state: ArrayRdmaState<T>,
    pub(crate) spawned: bool,
}

pub(crate) enum ArrayRdmaState<T: Dist> {
    AmPut(AmHandle<()>),
    RdmaPut(RdmaHandle<T>),
    AtomicPut(AtomicOpHandle<T>),
}

impl<T: Dist> ArrayRdmaHandle2<T> {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<()> {
        let task = match self.state {
            ArrayRdmaState::AmPut(req) => req.spawn(),
            ArrayRdmaState::RdmaPut(req) => req.spawn(),
            ArrayRdmaState::AtomicPut(req) => req.spawn(),
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
            ArrayRdmaState::AmPut(req) => req.block(),
            ArrayRdmaState::RdmaPut(req) => req.block(),
            ArrayRdmaState::AtomicPut(req) => req.block(),
        }
    }
}

impl<T: Dist> Future for ArrayRdmaHandle2<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.state {
            ArrayRdmaState::AmPut(req) => {
                if let Poll::Ready(v) = Pin::new(req).poll(cx) {
                    this.spawned = true;
                    Poll::Ready(v)
                } else {
                    Poll::Pending
                }
            }
            ArrayRdmaState::RdmaPut(req) => {
                if let Poll::Ready(v) = Pin::new(req).poll(cx) {
                    this.spawned = true;
                    Poll::Ready(v)
                } else {
                    Poll::Pending
                }
            }
            ArrayRdmaState::AtomicPut(req) => {
                if let Poll::Ready(v) = Pin::new(req).poll(cx) {
                    this.spawned = true;
                    Poll::Ready(v)
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

pub trait LamellarRdmaPut<T: Dist> {
    unsafe fn new_put(&self, index: usize, data: T) -> ArrayRdmaHandle2<T>;
    unsafe fn new_put_unmanaged(&self, index: usize, data: T);
    // fn put_buffer<U: TeamTryInto<LamellarArrayRdmaOutput<T>>>(
    //     &self,
    //     index: usize,
    //     buf: U,
    // ) -> RdmaFuture<T>;
    // fn put_buffer_unmanaged<U: TeamTryInto<LamellarArrayRdmaOutput<T>>>(
    //     &self,
    //     index: usize,
    //     buf: U,
    // ) -> RdmaFuture<T>;
    // fn put_pe_buffer<U: TeamTryInto<LamellarArrayRdmaOutput<T>>>(
    //     &self,
    //     pe: usize,
    //     index: usize,
    //     buf: U,
    // ) -> RdmaFuture<T>;
    // fn put_pe_buffer_unmanaged<U: TeamTryInto<LamellarArrayRdmaOutput<T>>>(
    //     &self,
    //     pe: usize,
    //     index: usize,
    //     buf: U,
    // ) -> RdmaFuture<T>;
    // fn put_pe(&self, pe: usize, index: usize, data: T) -> RdmaFuture<T>;
    // fn put_pe_unmanaged(&self, pe: usize, index: usize, data: T) -> RdmaFuture<T>;
    // todo put all
}

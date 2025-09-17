use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    active_messaging::{LocalAmHandle, MultiAmHandle},
    array::{ArrayOpHandle, LamellarByteArray},
    memregion::MemregionRdmaInputInner,
    warnings::RuntimeWarning,
    AmHandle, AtomicOpHandle, Dist, LamellarTask, RdmaHandle,
};

pub struct ArrayRdmaHandle2<T: Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    pub(crate) state: ArrayRdmaState<T>,
    pub(crate) spawned: bool,
}

pub(crate) enum ArrayRdmaState<T: Dist> {
    LocalAmPut(LocalAmHandle<()>),     //Am is initiated as a local am
    RemoteAmPut(AmHandle<()>),         //Am is initiated as a remote am
    RemoteAmPutAll(MultiAmHandle<()>), //Am is initiated as a remote am to all PEs
    StoreOp(ArrayOpHandle<T>),
    RdmaPut(RdmaHandle<T>),
    AtomicPut(AtomicOpHandle<T>),
    MultiRdmaPut(Vec<RdmaHandle<T>>),
}

impl<T: Dist> ArrayRdmaHandle2<T> {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<()> {
        let task = match self.state {
            ArrayRdmaState::LocalAmPut(req) => req.spawn(),
            ArrayRdmaState::RemoteAmPut(req) => req.spawn(),
            ArrayRdmaState::RemoteAmPutAll(req) => {
                let req = req.spawn();
                self.array.team().spawn(async move {
                    req.await;
                })
            }
            ArrayRdmaState::StoreOp(req) => req.spawn(),
            ArrayRdmaState::RdmaPut(req) => req.spawn(),
            ArrayRdmaState::AtomicPut(req) => req.spawn(),
            ArrayRdmaState::MultiRdmaPut(ref mut reqs) => {
                let mut tasks = Vec::with_capacity(reqs.len());
                for req in reqs.drain(..) {
                    tasks.push(req.spawn());
                }
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
            ArrayRdmaState::LocalAmPut(req) => req.block(),
            ArrayRdmaState::RemoteAmPut(req) => req.block(),
            ArrayRdmaState::RemoteAmPutAll(req) => {
                req.block();
            }
            ArrayRdmaState::StoreOp(req) => req.block(),
            ArrayRdmaState::RdmaPut(req) => req.block(),
            ArrayRdmaState::AtomicPut(req) => req.block(),
            ArrayRdmaState::MultiRdmaPut(ref mut reqs) => {
                for req in reqs.drain(..) {
                    req.block();
                }
            }
        }
    }
}

impl<T: Dist> Future for ArrayRdmaHandle2<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let res = match &mut this.state {
            ArrayRdmaState::LocalAmPut(req) => {
                if let Poll::Ready(_) = Pin::new(req).poll(cx) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
            ArrayRdmaState::RemoteAmPut(req) => {
                if let Poll::Ready(_) = Pin::new(req).poll(cx) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
            ArrayRdmaState::RemoteAmPutAll(req) => {
                if let Poll::Ready(_) = Pin::new(req).poll(cx) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
            ArrayRdmaState::StoreOp(req) => {
                if let Poll::Ready(_) = Pin::new(req).poll(cx) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
            ArrayRdmaState::RdmaPut(req) => {
                if let Poll::Ready(_) = Pin::new(req).poll(cx) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
            ArrayRdmaState::AtomicPut(req) => {
                if let Poll::Ready(_) = Pin::new(req).poll(cx) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
            ArrayRdmaState::MultiRdmaPut(reqs) => {
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

// All functions marked unsafe as it will be up to
// the implementing Array to determine the final saftely
// exposed to the user.
pub(crate) trait LamellarRdmaPut<T: Dist> {
    unsafe fn put(&self, index: usize, data: T) -> ArrayRdmaHandle2<T>;
    unsafe fn put_unmanaged(&self, index: usize, data: T);
    unsafe fn put_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaHandle2<T>;
    unsafe fn put_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        index: usize,
        buf: U,
    );
    unsafe fn put_pe(&self, pe: usize, offset: usize, data: T) -> ArrayRdmaHandle2<T>;
    unsafe fn put_pe_unmanaged(&self, pe: usize, offset: usize, data: T);
    unsafe fn put_pe_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaHandle2<T>;
    unsafe fn put_pe_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        pe: usize,
        offset: usize,
        buf: U,
    );
    unsafe fn put_all(&self, offset: usize, data: T) -> ArrayRdmaHandle2<T>;
    unsafe fn put_all_unmanaged(&self, offset: usize, data: T);
    unsafe fn put_all_buffer<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
    ) -> ArrayRdmaHandle2<T>;
    unsafe fn put_all_buffer_unmanaged<U: Into<MemregionRdmaInputInner<T>>>(
        &self,
        offset: usize,
        buf: U,
    );

    // todo put all
}

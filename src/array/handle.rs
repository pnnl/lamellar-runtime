use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use pin_project::{pin_project, pinned_drop};

use crate::{
    active_messaging::{AmHandle, LocalAmHandle},
    array::LamellarByteArray,
    lamellar_request::LamellarRequest,
    scheduler::LamellarTask,
    warnings::RuntimeWarning,
    Dist, OneSidedMemoryRegion, RegisteredMemoryRegion,
};

/// a task handle for an array rdma (put/get) operation
pub struct ArrayRdmaHandle {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    pub(crate) reqs: VecDeque<AmHandle<()>>,
    pub(crate) spawned: bool,
}

impl Drop for ArrayRdmaHandle {
    fn drop(&mut self) {
        if !self.spawned {
            RuntimeWarning::disable_warnings();
            for _ in self.reqs.drain(0..) {}
            RuntimeWarning::enable_warnings();
            RuntimeWarning::DroppedHandle("an ArrayRdmaHandle").print();
        }
    }
}

impl ArrayRdmaHandle {
    /// This method will spawn the associated Array RDMA Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        self.array.team().spawn(self)
    }

    /// This method will block the calling thread until the associated Array RDMA Operation completes
    pub fn block(self) -> () {
        RuntimeWarning::BlockingCall(
            "ArrayRdmaHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.array.team().block_on(self)
    }
}

impl LamellarRequest for ArrayRdmaHandle {
    fn blocking_wait(mut self) -> Self::Output {
        self.spawned = true;
        for req in self.reqs.drain(0..) {
            req.blocking_wait();
        }
    }
    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        let mut ready = true;
        for req in self.reqs.iter_mut() {
            ready &= req.ready_or_set_waker(waker);
        }
        self.spawned = true;
        ready
    }
    fn val(&self) -> Self::Output {
        for req in self.reqs.iter() {
            req.val();
        }
    }
}

impl Future for ArrayRdmaHandle {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            for req in self.reqs.iter_mut() {
                req.ready_or_set_waker(cx.waker());
            }
            self.spawned = true;
        }
        while let Some(mut req) = self.reqs.pop_front() {
            if !req.ready_or_set_waker(cx.waker()) {
                self.reqs.push_front(req);
                return Poll::Pending;
            }
        }
        Poll::Ready(())
    }
}

/// a task handle for an array rdma 'at' operation
#[pin_project(PinnedDrop)]
pub struct ArrayRdmaAtHandle<T: Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    pub(crate) req: Option<LocalAmHandle<()>>,
    pub(crate) buf: OneSidedMemoryRegion<T>,
    pub(crate) spawned: bool,
}

#[pinned_drop]
impl<T: Dist> PinnedDrop for ArrayRdmaAtHandle<T> {
    fn drop(mut self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::disable_warnings();
            let _ = self.req.take();
            RuntimeWarning::enable_warnings();
            RuntimeWarning::DroppedHandle("an ArrayRdmaAtHandle").print();
        }
    }
}

impl<T: Dist> ArrayRdmaAtHandle<T> {
    /// This method will spawn the associated Array RDMA at Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion and retrieve the result. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<T> {
        self.spawned = true;
        self.array.team().spawn(self)
    }

    /// This method will block the calling thread until the associated Array RDMA at Operation completes
    pub fn block(mut self) -> T {
        self.spawned = true;
        RuntimeWarning::BlockingCall(
            "ArrayRdmaAtHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.array.team().block_on(self)
    }
}

impl<T: Dist> LamellarRequest for ArrayRdmaAtHandle<T> {
    fn blocking_wait(mut self) -> Self::Output {
        self.spawned = true;
        if let Some(req) = self.req.take() {
            req.blocking_wait();
        }
        // match self.req {
        //     Some(req) => req.blocking_wait(),
        //     None => {} //this means we did a blocking_get (With respect to RDMA) on either Unsafe or ReadOnlyArray so data is here
        // }
        unsafe { self.buf.as_slice().expect("Data should exist on PE")[0] }
    }
    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        self.spawned = true;
        if let Some(req) = &mut self.req {
            req.ready_or_set_waker(waker)
        } else {
            true
        }
    }
    fn val(&self) -> Self::Output {
        unsafe { self.buf.as_slice().expect("Data should exist on PE")[0] }
    }
}

impl<T: Dist> Future for ArrayRdmaAtHandle<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match &mut this.req {
            Some(req) => {
                if !req.ready_or_set_waker(cx.waker()) {
                    return Poll::Pending;
                }
            }
            None => {} //this means we did a blocking_get (With respect to RDMA) on either Unsafe or ReadOnlyArray so data is here
        }
        Poll::Ready(unsafe { this.buf.as_slice().expect("Data should exist on PE")[0] })
    }
}

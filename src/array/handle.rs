use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use pin_project::pin_project;

use crate::{
    active_messaging::{AmHandle, LocalAmHandle},
    array::LamellarByteArray,
    lamellar_request::LamellarRequest,
    scheduler::LamellarTask,
    Dist, OneSidedMemoryRegion, RegisteredMemoryRegion,
};

/// a task handle for an array rdma (put/get) operation
pub struct ArrayRdmaHandle {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    pub(crate) reqs: VecDeque<AmHandle<()>>,
}

impl ArrayRdmaHandle {
    pub fn spawn(self) -> LamellarTask<()> {
        self.array.team().spawn_task(self)
    }

    pub fn block(self) -> () {
        self.array.team().block_on(self)
    }
}

impl LamellarRequest for ArrayRdmaHandle {
    fn blocking_wait(mut self) -> Self::Output {
        for req in self.reqs.drain(0..) {
            req.blocking_wait();
        }
    }
    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        let mut ready = true;
        for req in self.reqs.iter_mut() {
            ready &= req.ready_or_set_waker(waker);
        }
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
#[pin_project]
pub struct ArrayRdmaAtHandle<T: Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    pub(crate) req: Option<LocalAmHandle<()>>,
    pub(crate) buf: OneSidedMemoryRegion<T>,
}

impl<T: Dist> ArrayRdmaAtHandle<T> {
    pub fn spawn(self) -> LamellarTask<T> {
        self.array.team().spawn_task(self)
    }

    pub fn block(self) -> T {
        self.array.team().block_on(self)
    }
}

impl<T: Dist> LamellarRequest for ArrayRdmaAtHandle<T> {
    fn blocking_wait(self) -> Self::Output {
        match self.req {
            Some(req) => req.blocking_wait(),
            None => {} //this means we did a blocking_get (With respect to RDMA) on either Unsafe or ReadOnlyArray so data is here
        }
        unsafe { self.buf.as_slice().expect("Data should exist on PE")[0] }
    }
    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
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

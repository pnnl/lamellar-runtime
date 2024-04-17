use crate::{
    array::{AmDist, LamellarByteArray},
    lamellar_request::LamellarRequest,
    AmHandle,
};

use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use pin_project::pin_project;

pub struct ArrayBatchOpHandle {
    pub(crate) _array: LamellarByteArray, //prevents prematurely performing a local drop
    pub(crate) reqs: VecDeque<(AmHandle<()>, Vec<usize>)>,
}

pub type ArrayOpHandle = ArrayBatchOpHandle;

impl LamellarRequest for ArrayBatchOpHandle {
    fn blocking_wait(mut self) -> Self::Output {
        for req in self.reqs.drain(0..) {
            req.0.blocking_wait();
        }
    }
    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        let mut ready = true;
        for req in self.reqs.iter_mut() {
            ready &= req.0.ready_or_set_waker(waker);
        }
        ready
    }
    fn val(&self) -> Self::Output {
        for req in self.reqs.iter() {
            req.0.val();
        }
    }
}

impl Future for ArrayBatchOpHandle {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        while let Some(mut req) = self.reqs.pop_front() {
            if !req.0.ready_or_set_waker(cx.waker()) {
                self.reqs.push_front(req);
                return Poll::Pending;
            }
        }
        Poll::Ready(())
    }
}

pub struct ArrayFetchOpHandle<R: AmDist> {
    pub(crate) _array: LamellarByteArray, //prevents prematurely performing a local drop
    pub(crate) req: AmHandle<Vec<R>>,
}

impl<R: AmDist> LamellarRequest for ArrayFetchOpHandle<R> {
    fn blocking_wait(mut self) -> Self::Output {
        self.req
            .blocking_wait()
            .pop()
            .expect("should have a single request")
    }
    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        self.req.ready_or_set_waker(waker)
    }
    fn val(&self) -> Self::Output {
        self.req.val().pop().expect("should have a single request")
    }
}

impl<R: AmDist> Future for ArrayFetchOpHandle<R> {
    type Output = R;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.req.ready_or_set_waker(cx.waker()) {
            return Poll::Ready(self.req.val().pop().expect("should have a single request"));
        }
        Poll::Pending
    }
}

#[pin_project]
pub struct ArrayFetchBatchOpHandle<R: AmDist> {
    pub(crate) _array: LamellarByteArray, //prevents prematurely performing a local drop
    pub(crate) reqs: VecDeque<(AmHandle<Vec<R>>, Vec<usize>)>,
    results: Vec<R>,
}

impl<R: AmDist> From<ArrayFetchBatchOpHandle<R>> for ArrayFetchOpHandle<R> {
    fn from(mut req: ArrayFetchBatchOpHandle<R>) -> Self {
        Self {
            _array: req._array,
            req: req.reqs.pop_front().unwrap().0,
        }
    }
}

impl<R: AmDist> ArrayFetchBatchOpHandle<R> {
    pub(crate) fn new(
        array: LamellarByteArray,
        reqs: VecDeque<(AmHandle<Vec<R>>, Vec<usize>)>,
        max_index: usize,
    ) -> Self {
        let mut results = Vec::with_capacity(max_index);
        unsafe {
            results.set_len(max_index);
        }
        Self {
            _array: array,
            reqs,
            results,
        }
    }
}

impl<R: AmDist> LamellarRequest for ArrayFetchBatchOpHandle<R> {
    fn blocking_wait(mut self) -> Self::Output {
        for req in self.reqs.drain(0..) {
            let mut res = req.0.blocking_wait();
            for (val, idx) in res.drain(..).zip(req.1.iter()) {
                self.results[*idx] = val;
            }
        }
        std::mem::take(&mut self.results)
    }
    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        let mut ready = true;
        for req in self.reqs.iter_mut() {
            ready &= req.0.ready_or_set_waker(waker);
        }
        ready
    }
    fn val(&self) -> Self::Output {
        let mut results = Vec::with_capacity(self.results.len());
        unsafe {
            results.set_len(self.results.len());
        }
        for req in &self.reqs {
            let mut res = req.0.val();
            for (val, idx) in res.drain(..).zip(req.1.iter()) {
                results[*idx] = val;
            }
        }
        results
    }
}

impl<R: AmDist> Future for ArrayFetchBatchOpHandle<R> {
    type Output = Vec<R>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        while let Some(mut req) = this.reqs.pop_front() {
            if !req.0.ready_or_set_waker(cx.waker()) {
                this.reqs.push_front(req);
                return Poll::Pending;
            } else {
                let mut res = req.0.val();
                for (val, idx) in res.drain(..).zip(req.1.iter()) {
                    this.results[*idx] = val;
                }
            }
        }
        Poll::Ready(std::mem::take(&mut this.results))
    }
}

pub struct ArrayResultOpHandle<R: AmDist> {
    pub(crate) _array: LamellarByteArray, //prevents prematurely performing a local drop
    pub(crate) req: AmHandle<Vec<Result<R, R>>>,
}

impl<R: AmDist> LamellarRequest for ArrayResultOpHandle<R> {
    fn blocking_wait(self) -> Self::Output {
        self.req
            .blocking_wait()
            .pop()
            .expect("should have a single request")
    }
    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        self.req.ready_or_set_waker(waker)
    }
    fn val(&self) -> Self::Output {
        self.req.val().pop().expect("should have a single request")
    }
}

impl<R: AmDist> Future for ArrayResultOpHandle<R> {
    type Output = Result<R, R>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.req.ready_or_set_waker(cx.waker()) {
            return Poll::Ready(self.req.val().pop().expect("should have a single request"));
        }
        Poll::Pending
    }
}

#[pin_project]
pub struct ArrayResultBatchOpHandle<R: AmDist> {
    pub(crate) _array: LamellarByteArray, //prevents prematurely performing a local drop
    pub(crate) reqs: VecDeque<(AmHandle<Vec<Result<R, R>>>, Vec<usize>)>,
    results: Vec<Result<R, R>>,
}

impl<R: AmDist> From<ArrayResultBatchOpHandle<R>> for ArrayResultOpHandle<R> {
    fn from(mut req: ArrayResultBatchOpHandle<R>) -> Self {
        Self {
            _array: req._array,
            req: req.reqs.pop_front().unwrap().0,
        }
    }
}

impl<R: AmDist> ArrayResultBatchOpHandle<R> {
    pub(crate) fn new(
        array: LamellarByteArray,
        reqs: VecDeque<(AmHandle<Vec<Result<R, R>>>, Vec<usize>)>,
        max_index: usize,
    ) -> Self {
        let mut results = Vec::with_capacity(max_index);
        unsafe {
            results.set_len(max_index);
        }
        Self {
            _array: array,
            reqs,
            results,
        }
    }
}

impl<R: AmDist> LamellarRequest for ArrayResultBatchOpHandle<R> {
    fn blocking_wait(mut self) -> Self::Output {
        for req in self.reqs.drain(0..) {
            let mut res = req.0.blocking_wait();
            for (val, idx) in res.drain(..).zip(req.1.iter()) {
                self.results[*idx] = val;
            }
        }
        std::mem::take(&mut self.results)
    }
    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        let mut ready = true;
        for req in self.reqs.iter_mut() {
            ready &= req.0.ready_or_set_waker(waker);
        }
        ready
    }
    fn val(&self) -> Self::Output {
        let mut results = Vec::with_capacity(self.results.len());
        unsafe {
            results.set_len(self.results.len());
        }
        for req in &self.reqs {
            let mut res = req.0.val();
            for (val, idx) in res.drain(..).zip(req.1.iter()) {
                results[*idx] = val;
            }
        }
        results
    }
}

impl<R: AmDist> Future for ArrayResultBatchOpHandle<R> {
    type Output = Vec<Result<R, R>>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        while let Some(mut req) = this.reqs.pop_front() {
            if !req.0.ready_or_set_waker(cx.waker()) {
                this.reqs.push_front(req);
                return Poll::Pending;
            } else {
                let mut res = req.0.val();
                for (val, idx) in res.drain(..).zip(req.1.iter()) {
                    this.results[*idx] = val;
                }
            }
        }
        Poll::Ready(std::mem::take(&mut this.results))
    }
}

use crate::{
    array::{AmDist, LamellarByteArray},
    lamellae::{AtomicFetchOpHandle, AtomicOpHandle, RdmaGetHandle, Remote},
    lamellar_request::LamellarRequest,
    scheduler::LamellarTask,
    warnings::RuntimeWarning,
    AmHandle, Dist, RdmaHandle,
};

use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::{ready, Future};

use pin_project::{pin_project, pinned_drop};

// /// a task handle for a single array operation that doesnt return any values
// pub type ArrayOpHandle<T> = ArrayBatchOpHandle;
#[must_use = "Array operation handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called. Ignoring the resulting value with 'let _ = ...' will cause the operation to NOT BE executed."]
#[pin_project(PinnedDrop)]
pub struct ArrayOpHandle<T: Remote> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: OpState<T>,
}

#[pin_project(project = OpStateProj)]
pub(crate) enum OpState<T: Remote> {
    Am(#[pin] AmHandle<()>),
    Network(#[pin] AtomicOpHandle<T>),
    Rdma(#[pin] RdmaHandle<T>),
    Spawned,
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for ArrayOpHandle<T> {
    fn drop(mut self: Pin<&mut Self>) {
        match self.state {
            OpState::Am(_) | OpState::Network(_) | OpState::Rdma(_) => {
                RuntimeWarning::DroppedHandle("an ArrayOpHandle").print();
            }
            _ => {}
        }
    }
}

impl<T: Dist> ArrayOpHandle<T> {
    /// This method will spawn the associated Array Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<()> {
        let old_state = std::mem::replace(&mut self.state, OpState::Spawned);
        match old_state {
            OpState::Am(am_handle) => am_handle.spawn(),
            OpState::Rdma(op_handle) => op_handle.spawn(),
            OpState::Network(op_handle) => op_handle.spawn(),
            _ => panic!("ArrayOpHandle should already have been spawned"),
        }
    }
    /// This method will block the calling thread until the associated Array Operation completes
    pub fn block(mut self) -> () {
        RuntimeWarning::BlockingCall(
            "ArrayBatchOpHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        let old_state = std::mem::replace(&mut self.state, OpState::Spawned);
        match old_state {
            OpState::Am(am_handle) => {
                am_handle.block();
            }
            OpState::Rdma(op_handle) => {
                op_handle.block();
            }
            OpState::Network(op_handle) => {
                op_handle.block();
            }
            OpState::Spawned => {
                // already completed
            }
        }
    }
}

impl<T: Dist> Future for ArrayOpHandle<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();
        let res = match this.state.project() {
            OpStateProj::Am(op_handle) => op_handle.poll(cx),
            OpStateProj::Network(op_handle) => return op_handle.poll(cx),
            OpStateProj::Rdma(op_handle) => return op_handle.poll(cx),
            _ => Poll::Ready(()),
        };
        if res.is_ready() {
            self.state = OpState::Spawned;
        }
        res
    }
}

/// a task handle for a batched array operation that doesnt return any values
#[must_use = "Array operation handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called. Ignoring the resulting value with 'let _ = ...' will cause the operation to NOT BE executed."]
#[pin_project(PinnedDrop)]
pub struct ArrayBatchOpHandle {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: BatchOpState,
}

#[pin_project(project = BatchOpStateProj)]
pub(crate) enum BatchOpState {
    Reqs(#[pin] VecDeque<(AmHandle<()>, Vec<usize>)>),
    // Network(#[pin] AtomicOpHandle<T>),
    // Rdma(#[pin] RdmaHandle<T>),
    Launched(#[pin] VecDeque<(LamellarTask<()>, Vec<usize>)>),
    Completed,
}

#[pinned_drop]
impl PinnedDrop for ArrayBatchOpHandle {
    fn drop(mut self: Pin<&mut Self>) {
        if let BatchOpState::Reqs(reqs) = &mut self.state {
            RuntimeWarning::disable_warnings();
            for _ in reqs.drain(0..) {}
            RuntimeWarning::enable_warnings();
            RuntimeWarning::DroppedHandle("an ArrayBatchOpHandle").print();
        }
    }
}

impl ArrayBatchOpHandle {
    /// This method will spawn the associated Array Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<()> {
        let old_state = std::mem::replace(&mut self.state, BatchOpState::Completed);
        match old_state {
            BatchOpState::Reqs(mut reqs) => {
                let launched = reqs
                    .drain(..)
                    .map(|(am, res)| (am.spawn(), res))
                    .collect::<VecDeque<(LamellarTask<()>, Vec<usize>)>>();
                self.state = BatchOpState::Launched(launched);
                self.array.team().spawn(self)
            }
            // BatchOpState::Rdma(op_handle) => op_handle.spawn(),
            // BatchOpState::Network(op_handle) => op_handle.spawn(),
            _ => panic!("ArrayBatchOpHandle should already have been spawned"),
        }
    }
    /// This method will block the calling thread until the associated Array Operation completes
    pub fn block(mut self) -> () {
        RuntimeWarning::BlockingCall(
            "ArrayBatchOpHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        let old_state = std::mem::replace(&mut self.state, BatchOpState::Completed);
        match old_state {
            BatchOpState::Reqs(mut reqs) => {
                let launched = reqs
                    .drain(..)
                    .map(|(am, res)| (am.spawn(), res))
                    .collect::<VecDeque<(LamellarTask<()>, Vec<usize>)>>();
                self.state = BatchOpState::Launched(launched);
                self.array.team().block_on(self)
            }
            // BatchOpState::Rdma(op_handle) => {
            //     op_handle.block();
            // }
            // BatchOpState::Network(op_handle) => {
            //     op_handle.block();
            // }
            BatchOpState::Launched(_reqs) => self.array.team().block_on(self),
            BatchOpState::Completed => {
                // already completed
            }
        }
    }
}

impl Future for ArrayBatchOpHandle {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let BatchOpState::Reqs(reqs) = &mut self.state {
            let launched = reqs
                .drain(..)
                .map(|(am, res)| (am.spawn(), res))
                .collect::<VecDeque<(LamellarTask<()>, Vec<usize>)>>();
            self.state = BatchOpState::Launched(launched);
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        if let BatchOpState::Launched(reqs) = &mut self.state {
            while let Some(mut req) = reqs.pop_front() {
                if Future::poll(Pin::new(&mut req.0), cx).is_pending() {
                    reqs.push_front(req);
                    return Poll::Pending;
                }
            }
            return Poll::Ready(());
        }
        Poll::Ready(())
        // let this = self.project();
        // match this.state.project() {
        //     BatchOpStateProj::Network(op_handle) => {
        //         return op_handle.poll(cx);
        //     }
        //     BatchOpStateProj::Rdma(op_handle) => {
        //         return op_handle.poll(cx);
        //     }
        //     _ => Poll::Ready(()),
        // }
    }
}

/// a task handle for a single array operation that returns a value
#[must_use = "Array operation handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called. Ignoring the resulting value with 'let _ = ...' will cause the operation to NOT BE executed."]
#[pin_project]
pub struct ArrayFetchOpHandle<R: Dist> {
    //AmHandle triggers Handle Dropped warning
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: FetchOpState<R>,
    // pub(crate) req: AmHandle<Vec<R>>,
}

#[pin_project(project = FetchOpStateProj)]
pub(crate) enum FetchOpState<R> {
    Req(#[pin] AmHandle<Vec<R>>),
    Rdma(#[pin] RdmaGetHandle<R>),
    Network(#[pin] AtomicFetchOpHandle<R>),
    AmLaunched(#[pin] LamellarTask<Vec<R>>),
}

impl<R: Dist> ArrayFetchOpHandle<R> {
    /// This method will spawn the associated Array Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<R> {
        match self.state {
            FetchOpState::Req(req) => {
                self.state = FetchOpState::AmLaunched(req.spawn());
                self.array.team().spawn(self)
            }
            FetchOpState::Rdma(op_handle) => op_handle.spawn(),
            FetchOpState::Network(op_handle) => op_handle.spawn(),
            _ => panic!("ArrayBatchOpHandle should already have been spawned"),
        }
    }

    /// This method will block the calling thread until the associated Array Operation completes
    pub fn block(self) -> R {
        RuntimeWarning::BlockingCall(
            "ArrayFetchOpHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        match self.state {
            FetchOpState::Req(req) => req.block().pop().expect("should have a single request"),
            FetchOpState::Rdma(op_handle) => op_handle.block(),
            FetchOpState::Network(op_handle) => op_handle.block(),
            FetchOpState::AmLaunched(req) => {
                req.block().pop().expect("should have a single request")
            }
        }
    }
}

impl<R: Dist> Future for ArrayFetchOpHandle<R> {
    type Output = R;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.state.project() {
            FetchOpStateProj::Req(req) => {
                let mut result = ready!(req.poll(cx));
                return Poll::Ready(result.pop().unwrap());
            }
            FetchOpStateProj::Rdma(req) => req.poll(cx),
            FetchOpStateProj::Network(req) => req.poll(cx),
            FetchOpStateProj::AmLaunched(req) => {
                let mut result = ready!(req.poll(cx));
                return Poll::Ready(result.pop().unwrap());
            }
        }
    }
}

/// a task handle for a batched array operation that return values
#[pin_project(PinnedDrop)]
#[must_use = "Array operation handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called. Ignoring the resulting value with 'let _ = ...' will cause the operation to NOT BE executed."]
pub struct ArrayFetchBatchOpHandle<R: AmDist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    // pub(crate) reqs: VecDeque<(AmHandle<Vec<R>>, Vec<usize>)>,
    pub(crate) state: FetchBatchOpState<R>,
    results: Vec<R>,
}

pub(crate) enum FetchBatchOpState<R> {
    Reqs(VecDeque<(AmHandle<Vec<R>>, Vec<usize>)>),
    Launched(VecDeque<(LamellarTask<Vec<R>>, Vec<usize>)>),
}

#[pinned_drop]
impl<R: AmDist> PinnedDrop for ArrayFetchBatchOpHandle<R> {
    fn drop(self: Pin<&mut Self>) {
        let mut this = self.project();
        if let FetchBatchOpState::Reqs(reqs) = &mut this.state {
            RuntimeWarning::disable_warnings();
            for _ in reqs.drain(0..) {}
            RuntimeWarning::enable_warnings();
            RuntimeWarning::DroppedHandle("an ArrayFetchBatchOpHandle").print();
        }
    }
}

impl<R: AmDist> ArrayFetchBatchOpHandle<R> {
    /// This method will spawn the associated Array Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Vec<R>> {
        match &mut self.state {
            FetchBatchOpState::Reqs(reqs) => {
                let launched = reqs
                    .drain(..)
                    .map(|(am, res)| (am.spawn(), res))
                    .collect::<VecDeque<(LamellarTask<Vec<R>>, Vec<usize>)>>();
                self.state = FetchBatchOpState::Launched(launched);
                self.array.team().spawn(self)
            }
            _ => panic!("ArrayFetchBatchOpHandle should already have been spawned"),
        }
    }

    /// This method will block the calling thread until the associated Array Operation completes
    pub fn block(mut self) -> Vec<R> {
        RuntimeWarning::BlockingCall(
            "ArrayFetchBatchOpHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        match &mut self.state {
            FetchBatchOpState::Reqs(reqs) => {
                let launched = reqs
                    .drain(..)
                    .map(|(am, res)| (am.spawn(), res))
                    .collect::<VecDeque<(LamellarTask<Vec<R>>, Vec<usize>)>>();
                self.state = FetchBatchOpState::Launched(launched);
                self.array.team().block_on(self)
            }
            FetchBatchOpState::Launched(_) => self.array.team().block_on(self),
        }
    }
}

impl<R: Dist> From<ArrayFetchBatchOpHandle<R>> for ArrayFetchOpHandle<R> {
    fn from(mut req: ArrayFetchBatchOpHandle<R>) -> Self {
        let handle = match &mut req.state {
            FetchBatchOpState::Reqs(reqs) => Self {
                array: req.array.clone(),
                state: FetchOpState::Req(reqs.pop_front().unwrap().0),
            },
            FetchBatchOpState::Launched(reqs) => Self {
                array: req.array.clone(),
                state: FetchOpState::AmLaunched(reqs.pop_front().unwrap().0),
            },
        };
        req.state = FetchBatchOpState::Launched(VecDeque::new());
        handle
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
            array: array,
            state: FetchBatchOpState::Reqs(reqs),
            results,
        }
    }
}

impl<R: AmDist> Future for ArrayFetchBatchOpHandle<R> {
    type Output = Vec<R>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match &mut this.state {
            FetchBatchOpState::Reqs(reqs) => {
                let launched = reqs
                    .drain(..)
                    .map(|(am, res)| (am.spawn(), res))
                    .collect::<VecDeque<(LamellarTask<Vec<R>>, Vec<usize>)>>();
                *this.state = FetchBatchOpState::Launched(launched);
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            FetchBatchOpState::Launched(reqs) => {
                while let Some(mut req) = reqs.pop_front() {
                    match Future::poll(Pin::new(&mut req.0), cx) {
                        Poll::Pending => {
                            reqs.push_front(req);
                            return Poll::Pending;
                        }
                        Poll::Ready(mut res) => {
                            for (val, idx) in res.drain(..).zip(req.1.iter()) {
                                this.results[*idx] = val;
                            }
                        }
                    }
                }
            }
        }
        Poll::Ready(std::mem::take(&mut this.results))
    }
}

/// a task handle for a single array operation that returns a result
#[must_use = "Array operation handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called. Ignoring the resulting value with 'let _ = ...' will cause the operation to NOT BE executed."]
pub struct ArrayResultOpHandle<R: AmDist> {
    // dropped handle triggered by AmHandle
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    pub(crate) state: ResultOpState<R>,
}

pub(crate) enum ResultOpState<R: AmDist> {
    Req(AmHandle<Vec<Result<R, R>>>),
    Launched(LamellarTask<Vec<Result<R, R>>>),
}

impl<R: AmDist> ArrayResultOpHandle<R> {
    /// This method will spawn the associated Array Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Result<R, R>> {
        match self.state {
            ResultOpState::Req(req) => {
                self.state = ResultOpState::Launched(req.spawn());
                self.array.team().spawn(self)
            }
            _ => panic!("ArrayResultOpHandle should already have been spawned"),
        }
    }

    /// This method will block the calling thread until the associated Array Operation completes
    pub fn block(mut self) -> Result<R, R> {
        RuntimeWarning::BlockingCall(
            "ArrayResultOpHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        match self.state {
            ResultOpState::Req(req) => {
                self.state = ResultOpState::Launched(req.spawn());
                self.array.team().block_on(self)
            }
            ResultOpState::Launched(ref _req) => self.array.team().block_on(self),
        }
    }
}

impl<R: AmDist> Future for ArrayResultOpHandle<R> {
    type Output = Result<R, R>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.state {
            ResultOpState::Req(req) => {
                if req.ready_or_set_waker(cx.waker()) {
                    return Poll::Ready(req.val().pop().expect("should have a single request"));
                }
            }
            ResultOpState::Launched(req) => {
                if let Poll::Ready(mut res) = Future::poll(Pin::new(req), cx) {
                    return Poll::Ready(res.pop().expect("should have a single request"));
                }
            }
        }
        Poll::Pending
    }
}

/// a task handle for a batched array operation that returns results
#[pin_project(PinnedDrop)]
#[must_use = "Array operation handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called. Ignoring the resulting value with 'let _ = ...' will cause the operation to NOT BE executed."]
pub struct ArrayResultBatchOpHandle<R: AmDist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    pub(crate) state: BatchResultOpState<R>, //reqs: ,
    results: Vec<Result<R, R>>,
}

pub(crate) enum BatchResultOpState<R> {
    Reqs(VecDeque<(AmHandle<Vec<Result<R, R>>>, Vec<usize>)>),
    Launched(VecDeque<(LamellarTask<Vec<Result<R, R>>>, Vec<usize>)>),
}

#[pinned_drop]
impl<R: AmDist> PinnedDrop for ArrayResultBatchOpHandle<R> {
    fn drop(self: Pin<&mut Self>) {
        let mut this = self.project();
        if let BatchResultOpState::Reqs(reqs) = &mut this.state {
            RuntimeWarning::disable_warnings();
            for _ in reqs.drain(0..) {}
            RuntimeWarning::enable_warnings();
            RuntimeWarning::DroppedHandle("an ArrayResultBatchOpHandle").print();
        }
    }
}

impl<R: AmDist> ArrayResultBatchOpHandle<R> {
    /// This method will spawn the associated Array Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Vec<Result<R, R>>> {
        match &mut self.state {
            BatchResultOpState::Reqs(reqs) => {
                let launched = reqs
                    .drain(..)
                    .map(|(am, res)| (am.spawn(), res))
                    .collect::<VecDeque<(LamellarTask<Vec<Result<R, R>>>, Vec<usize>)>>();
                self.state = BatchResultOpState::Launched(launched);
                self.array.team().spawn(self)
            }
            _ => panic!("ArrayFetchBatchOpHandle should already have been spawned"),
        }
    }

    /// This method will block the calling thread until the associated Array Operation completes
    pub fn block(mut self) -> Vec<Result<R, R>> {
        RuntimeWarning::BlockingCall(
            "ArrayResultBatchOpHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        match &mut self.state {
            BatchResultOpState::Reqs(reqs) => {
                let launched = reqs
                    .drain(..)
                    .map(|(am, res)| (am.spawn(), res))
                    .collect::<VecDeque<(LamellarTask<Vec<Result<R, R>>>, Vec<usize>)>>();
                self.state = BatchResultOpState::Launched(launched);
                self.array.team().block_on(self)
            }
            BatchResultOpState::Launched(_) => self.array.team().block_on(self),
        }
    }
}

impl<R: AmDist> From<ArrayResultBatchOpHandle<R>> for ArrayResultOpHandle<R> {
    fn from(mut req: ArrayResultBatchOpHandle<R>) -> Self {
        let handle = match &mut req.state {
            BatchResultOpState::Reqs(reqs) => Self {
                array: req.array.clone(),
                state: ResultOpState::Req(reqs.pop_front().unwrap().0),
            },
            BatchResultOpState::Launched(reqs) => Self {
                array: req.array.clone(),
                state: ResultOpState::Launched(reqs.pop_front().unwrap().0),
            },
        };
        req.state = BatchResultOpState::Launched(VecDeque::new());
        handle
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
            array: array,
            state: BatchResultOpState::Reqs(reqs),
            results,
        }
    }
}

impl<R: AmDist> Future for ArrayResultBatchOpHandle<R> {
    type Output = Vec<Result<R, R>>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match &mut this.state {
            BatchResultOpState::Reqs(reqs) => {
                // println!("launching sub ams");
                let launched = reqs
                    .drain(..)
                    .map(|(am, res)| (am.spawn(), res))
                    .collect::<VecDeque<(LamellarTask<Vec<Result<R, R>>>, Vec<usize>)>>();
                *this.state = BatchResultOpState::Launched(launched);
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            BatchResultOpState::Launched(reqs) => {
                // println!("polling sub ams");
                while let Some(mut req) = reqs.pop_front() {
                    match Future::poll(Pin::new(&mut req.0), cx) {
                        Poll::Pending => {
                            reqs.push_front(req);
                            return Poll::Pending;
                        }
                        Poll::Ready(mut res) => {
                            // println!("res: {:?}", res.len());
                            for (val, idx) in res.drain(..).zip(req.1.iter()) {
                                this.results[*idx] = val;
                            }
                        }
                    }
                }
            }
        }
        Poll::Ready(std::mem::take(&mut this.results))
    }
}

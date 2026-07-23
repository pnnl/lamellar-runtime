use crate::{
    array::{AmDist, LamellarByteArray},
    lamellae::{
        AtomicCompareExchangeOpHandle, AtomicFetchOpHandle, AtomicOpHandle, RdmaGetHandle, Remote,
    },
    memregion::one_sided::OneSidedMemoryRegion,
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

/// a task handle for a single array operation that doesnt return any values
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
    /// Spawn the array operation.
    ///
    /// This method will spawn the associated Array Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.put(0, 42); // or other op like get, local_iter_mut, etc
    /// let task = handle.spawn();
    /// ```
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

    /// Block until the array operation completes.
    ///
    /// This method will block the calling thread until the associated Array Operation completes
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.put(0, 42);
    /// handle.block();
    /// ```
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
        let this = self.as_mut().project();
        let res = match this.state.project() {
            OpStateProj::Am(op_handle) => op_handle.poll(cx),
            OpStateProj::Network(op_handle) => op_handle.poll(cx),
            OpStateProj::Rdma(op_handle) => op_handle.poll(cx),
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
    /// Spawn the array operation.
    ///
    /// This method will spawn the associated Array Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.put(0, 42); // or other op like get, local_iter_mut, etc
    /// let task = handle.spawn();
    /// ```
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

    /// Block until the array operation completes.
    ///
    /// This method will block the calling thread until the associated Array Operation completes
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.put(0, 42);
    /// handle.block();
    /// ```
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

//  fn bytes_to_vec_t<T: Copy>(bytes: Vec<u8>) -> Vec<T> {
//         let elem_size = std::mem::size_of::<T>();
//         if elem_size == 0 || bytes.is_empty() {
//             return Vec::new();
//         }
//         let len = bytes.len() / elem_size;
//         let cap = bytes.capacity() / elem_size;
//         let ptr = bytes.as_ptr() as *mut T;
//         std::mem::forget(bytes);
//         unsafe { Vec::from_raw_parts(ptr, len, cap) }
//     }

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

//need to add a Memregion Variant
#[pin_project(project = FetchOpStateProj)]
pub(crate) enum FetchOpState<R: Remote> {
    // Req(#[pin] AmHandle<Vec<R>>),
    // ByteReq(#[pin] AmHandle<Vec<u8>>),
    OneSidedMemoryRegionReq(#[pin] AmHandle<OneSidedMemoryRegion<u8>>),
    Rdma(#[pin] RdmaGetHandle<R>, Option<OneSidedMemoryRegion<u8>>),
    Network(#[pin] AtomicFetchOpHandle<R>),
    AmLaunched(#[pin] LamellarTask<Vec<R>>),
    // ByteLaunched(#[pin] LamellarTask<Vec<u8>>),
    OneSidedMemoryRegionLaunched(#[pin] LamellarTask<OneSidedMemoryRegion<u8>>),
}

impl<R: Dist> ArrayFetchOpHandle<R> {
    /// Spawn the array operation.
    ///
    /// This method will spawn the associated Array Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.put(0, 42); // or other op like get, local_iter_mut, etc
    /// let task = handle.spawn();
    /// ```
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<R> {
        match self.state {
            // FetchOpState::Req(req) => {
            //     self.state = FetchOpState::AmLaunched(req.spawn());
            //     self.array.team().spawn(self)
            // }
            // FetchOpState::ByteReq(req) => {
            //     self.state = FetchOpState::ByteLaunched(req.spawn());
            //     self.array.team().spawn(self)
            // }
            FetchOpState::OneSidedMemoryRegionReq(op_handle) => {
                self.state = FetchOpState::OneSidedMemoryRegionLaunched(op_handle.spawn());
                self.array.team().spawn(self)
            }
            FetchOpState::Rdma(op_handle, mem_region) => {
                let task = op_handle.spawn();
                drop(mem_region);
                task
            }
            FetchOpState::Network(op_handle) => op_handle.spawn(),
            _ => panic!("ArrayBatchOpHandle should already have been spawned"),
        }
    }

    /// Block until the array operation completes.
    ///
    /// This method will block the calling thread until the associated Array Operation completes
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.get(0);
    /// let result = handle.block();
    /// ```
    pub fn block(self) -> R {
        RuntimeWarning::BlockingCall(
            "ArrayFetchOpHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        match self.state {
            // FetchOpState::Req(req) => req.block().pop().expect("should have a single request"),
            // FetchOpState::ByteReq(req) => {
            //     bytes_to_vec_t::<R>(req.block()).pop().expect("should have a single request")
            // }
            FetchOpState::OneSidedMemoryRegionReq(req) => {
                let mem_region = req.block();
                let data = unsafe { mem_region.clone().to_base::<R>().get(0).block() };
                data
            }
            FetchOpState::Rdma(op_handle, mem_region) => {
                let data = op_handle.block();
                drop(mem_region);
                data
            }
            FetchOpState::Network(op_handle) => op_handle.block(),
            FetchOpState::AmLaunched(req) => {
                req.block().pop().expect("should have a single request")
            }
            // FetchOpState::ByteLaunched(req) => {
            //     bytes_to_vec_t::<R>(req.block()).pop().expect("should have a single request")
            // }
            FetchOpState::OneSidedMemoryRegionLaunched(req) => {
                let mem_region = req.block();
                let data = unsafe { mem_region.clone().to_base::<R>().get(0).block() };
                data
            }
        }
    }
}

impl<R: Dist> Future for ArrayFetchOpHandle<R> {
    type Output = R;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match this.state.as_mut().project() {
            // FetchOpStateProj::Req(req) => {
            //     let mut result = ready!(req.poll(cx));
            //     return Poll::Ready(result.pop().unwrap());
            // }
            // FetchOpStateProj::ByteReq(req) => {
            //     let bytes = ready!(req.poll(cx));
            //     return Poll::Ready(bytes_to_vec_t::<R>(bytes).pop().unwrap());
            // }
            FetchOpStateProj::OneSidedMemoryRegionReq(req) => {
                let mem_region = ready!(req.poll(cx));
                let rdma = unsafe { mem_region.clone().to_base::<R>().get(0) };
                this.state.set(FetchOpState::Rdma(rdma, Some(mem_region)));
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            FetchOpStateProj::Rdma(req, _mem_region) => req.poll(cx),
            FetchOpStateProj::Network(req) => req.poll(cx),
            FetchOpStateProj::AmLaunched(req) => {
                let mut result = ready!(req.poll(cx));
                return Poll::Ready(result.pop().unwrap());
            }
            // FetchOpStateProj::ByteLaunched(req) => {
            //     let bytes = ready!(req.poll(cx));
            //     return Poll::Ready(bytes_to_vec_t::<R>(bytes).pop().unwrap());
            // }
            FetchOpStateProj::OneSidedMemoryRegionLaunched(req) => {
                let mem_region = ready!(req.poll(cx));
                let rdma = unsafe { mem_region.clone().to_base::<R>().get(0) };
                this.state.set(FetchOpState::Rdma(rdma, Some(mem_region)));
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
    }
}

/// a task handle for a batched array operation that return values
#[pin_project(PinnedDrop)]
#[must_use = "Array operation handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called. Ignoring the resulting value with 'let _ = ...' will cause the operation to NOT BE executed."]
pub struct ArrayFetchBatchOpHandle<R: AmDist + Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    // pub(crate) reqs: VecDeque<(AmHandle<Vec<R>>, Vec<usize>)>,
    pub(crate) state: FetchBatchOpState<R>,
    results: Vec<R>,
}

pub(crate) enum FetchBatchOpState<R: AmDist + Dist> {
    // Reqs(VecDeque<(AmHandle<Vec<R>>, Vec<usize>)>),
    // ByteReqs(VecDeque<(AmHandle<Vec<u8>>, Vec<usize>)>),
    OneSidedMemoryRegionReqs(VecDeque<(AmHandle<OneSidedMemoryRegion<u8>>, Vec<usize>)>),
    Launched(VecDeque<(LamellarTask<Vec<R>>, Vec<usize>)>),
    // ByteLaunched(VecDeque<(LamellarTask<Vec<u8>>, Vec<usize>)>),
    OneSidedMemoryRegionLaunched(
        VecDeque<(
            Option<LamellarTask<OneSidedMemoryRegion<u8>>>,
            Option<LamellarTask<Vec<R>>>,
            Vec<usize>,
            Option<OneSidedMemoryRegion<u8>>,
        )>,
    ),
}

#[pinned_drop]
impl<R: AmDist + Dist> PinnedDrop for ArrayFetchBatchOpHandle<R> {
    fn drop(self: Pin<&mut Self>) {
        let mut this = self.project();
        match &mut this.state {
            // FetchBatchOpState::Reqs(reqs) => {
            //     RuntimeWarning::disable_warnings();
            //     for _ in reqs.drain(0..) {}
            //     RuntimeWarning::enable_warnings();
            //     RuntimeWarning::DroppedHandle("an ArrayFetchBatchOpHandle").print();
            // }
            // FetchBatchOpState::ByteReqs(reqs) => {
            //     RuntimeWarning::disable_warnings();
            //     for _ in reqs.drain(0..) {}
            //     RuntimeWarning::enable_warnings();
            //     RuntimeWarning::DroppedHandle("an ArrayFetchBatchOpHandle").print();
            // }
            FetchBatchOpState::OneSidedMemoryRegionReqs(reqs) => {
                RuntimeWarning::disable_warnings();
                for _ in reqs.drain(0..) {}
                RuntimeWarning::enable_warnings();
                RuntimeWarning::DroppedHandle("an ArrayFetchBatchOpHandle").print();
            }
            _ => {}
        }
    }
}

impl<R: AmDist + Dist> ArrayFetchBatchOpHandle<R> {
    /// Spawn the array operation.
    ///
    /// This method will spawn the associated Array Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.put(0, 42); // or other op like get, local_iter_mut, etc
    /// let task = handle.spawn();
    /// ```
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Vec<R>> {
        match &mut self.state {
            // FetchBatchOpState::Reqs(reqs) => {
            //     let launched = reqs
            //         .drain(..)
            //         .map(|(am, res)| (am.spawn(), res))
            //         .collect::<VecDeque<(LamellarTask<Vec<R>>, Vec<usize>)>>();
            //     self.state = FetchBatchOpState::Launched(launched);
            //     self.array.team().spawn(self)
            // }
            // FetchBatchOpState::ByteReqs(reqs) => {
            //     let launched = reqs
            //         .drain(..)
            //         .map(|(am, res)| (am.spawn(), res))
            //         .collect::<VecDeque<(LamellarTask<Vec<u8>>, Vec<usize>)>>();
            //     self.state = FetchBatchOpState::ByteLaunched(launched);
            //     self.array.team().spawn(self)
            // }
            FetchBatchOpState::OneSidedMemoryRegionReqs(reqs) => {
                let launched = reqs
                    .drain(..)
                    .map(|(am, res)| (Some(am.spawn()), None, res, None))
                    .collect::<VecDeque<(
                        Option<LamellarTask<OneSidedMemoryRegion<u8>>>,
                        Option<LamellarTask<Vec<R>>>,
                        Vec<usize>,
                        Option<OneSidedMemoryRegion<u8>>,
                    )>>();
                self.state = FetchBatchOpState::OneSidedMemoryRegionLaunched(launched);
                self.array.team().spawn(self)
            }
            _ => panic!("ArrayFetchBatchOpHandle should already have been spawned"),
        }
    }

    /// Block until the array operation completes.
    ///
    /// This method will block the calling thread until the associated Array Operation completes
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.get(0);
    /// let result = handle.block();
    /// ```
    pub fn block(mut self) -> Vec<R> {
        RuntimeWarning::BlockingCall(
            "ArrayFetchBatchOpHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        match &mut self.state {
            // FetchBatchOpState::Reqs(reqs) => {
            //     let launched = reqs
            //         .drain(..)
            //         .map(|(am, res)| (am.spawn(), res))
            //         .collect::<VecDeque<(LamellarTask<Vec<R>>, Vec<usize>)>>();
            //     self.state = FetchBatchOpState::Launched(launched);
            //     self.array.team().block_on(self)
            // }
            // FetchBatchOpState::ByteReqs(reqs) => {
            //     let launched = reqs
            //         .drain(..)
            //         .map(|(am, res)| (am.spawn(), res))
            //         .collect::<VecDeque<(LamellarTask<Vec<u8>>, Vec<usize>)>>();
            //     self.state = FetchBatchOpState::ByteLaunched(launched);
            //     self.array.team().block_on(self)
            // }
            FetchBatchOpState::OneSidedMemoryRegionReqs(reqs) => {
                let launched = reqs
                    .drain(..)
                    .map(|(am, res)| (Some(am.spawn()), None, res, None))
                    .collect::<VecDeque<(
                        Option<LamellarTask<OneSidedMemoryRegion<u8>>>,
                        Option<LamellarTask<Vec<R>>>,
                        Vec<usize>,
                        Option<OneSidedMemoryRegion<u8>>,
                    )>>();
                self.state = FetchBatchOpState::OneSidedMemoryRegionLaunched(launched);
                self.array.team().block_on(self)
            }
            FetchBatchOpState::Launched(_) | FetchBatchOpState::OneSidedMemoryRegionLaunched(_) => {
                self.array.team().block_on(self)
            }
        }
    }
}

impl<R: Dist> From<ArrayFetchBatchOpHandle<R>> for ArrayFetchOpHandle<R> {
    fn from(mut req: ArrayFetchBatchOpHandle<R>) -> Self {
        let handle = match &mut req.state {
            // FetchBatchOpState::Reqs(reqs) => Self {
            //     array: req.array.clone(),
            //     state: FetchOpState::Req(reqs.pop_front().unwrap().0),
            // },
            // FetchBatchOpState::ByteReqs(reqs) => Self {
            //     array: req.array.clone(),
            //     state: FetchOpState::ByteReq(reqs.pop_front().unwrap().0),
            // },
            FetchBatchOpState::OneSidedMemoryRegionReqs(reqs) => Self {
                array: req.array.clone(),
                state: FetchOpState::OneSidedMemoryRegionReq(reqs.pop_front().unwrap().0),
            },
            FetchBatchOpState::Launched(reqs) => Self {
                array: req.array.clone(),
                state: FetchOpState::AmLaunched(reqs.pop_front().unwrap().0),
            },
            // FetchBatchOpState::ByteLaunched(reqs) => Self {
            //     array: req.array.clone(),
            //     state: FetchOpState::ByteLaunched(reqs.pop_front().unwrap().0),
            // },
            FetchBatchOpState::OneSidedMemoryRegionLaunched(reqs) => Self {
                array: req.array.clone(),
                state: FetchOpState::OneSidedMemoryRegionLaunched(
                    reqs.pop_front()
                        .unwrap()
                        .0
                        .expect("Expected a launched task"),
                ),
            },
        };
        req.state = FetchBatchOpState::Launched(VecDeque::new());
        handle
    }
}

impl<R: AmDist + Dist> ArrayFetchBatchOpHandle<R> {
    // pub(crate) fn new(
    //     array: LamellarByteArray,
    //     reqs: VecDeque<(AmHandle<Vec<R>>, Vec<usize>)>,
    //     max_index: usize,
    // ) -> Self {
    //     let mut results = Vec::with_capacity(max_index);
    //     unsafe {
    //         results.set_len(max_index);
    //     }
    //     Self {
    //         array,
    //         state: FetchBatchOpState::Reqs(reqs),
    //         results,
    //     }
    // }

    pub(crate) fn new_one_sided_memory_region(
        array: LamellarByteArray,
        reqs: VecDeque<(AmHandle<OneSidedMemoryRegion<u8>>, Vec<usize>)>,
        max_index: usize,
    ) -> Self {
        let mut results = Vec::with_capacity(max_index);
        unsafe {
            results.set_len(max_index);
        }
        Self {
            array,
            state: FetchBatchOpState::OneSidedMemoryRegionReqs(reqs),
            results,
        }
    }

    // pub(crate) fn new_bytes(
    //     array: LamellarByteArray,
    //     reqs: VecDeque<(AmHandle<Vec<u8>>, Vec<usize>)>,
    //     max_index: usize,
    // ) -> Self {
    //     let mut results = Vec::with_capacity(max_index);
    //     unsafe {
    //         results.set_len(max_index);
    //     }
    //     Self {
    //         array,
    //         state: FetchBatchOpState::ByteReqs(reqs),
    //         results,
    //     }
    // }
}

impl<R: AmDist + Dist> Future for ArrayFetchBatchOpHandle<R> {
    type Output = Vec<R>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match &mut this.state {
            // FetchBatchOpState::Reqs(reqs) => {
            //     let launched = reqs
            //         .drain(..)
            //         .map(|(am, res)| (am.spawn(), res))
            //         .collect::<VecDeque<(LamellarTask<Vec<R>>, Vec<usize>)>>();
            //     *this.state = FetchBatchOpState::Launched(launched);
            //     cx.waker().wake_by_ref();
            //     return Poll::Pending;
            // }
            // FetchBatchOpState::ByteReqs(reqs) => {
            //     let launched = reqs
            //         .drain(..)
            //         .map(|(am, res)| (am.spawn(), res))
            //         .collect::<VecDeque<(LamellarTask<Vec<u8>>, Vec<usize>)>>();
            //     *this.state = FetchBatchOpState::ByteLaunched(launched);
            //     cx.waker().wake_by_ref();
            //     return Poll::Pending;
            // }
            FetchBatchOpState::OneSidedMemoryRegionReqs(reqs) => {
                let launched = reqs
                    .drain(..)
                    .map(|(am, res)| (Some(am.spawn()), None, res, None))
                    .collect::<VecDeque<(
                        Option<LamellarTask<OneSidedMemoryRegion<u8>>>,
                        Option<LamellarTask<Vec<R>>>,
                        Vec<usize>,
                        Option<OneSidedMemoryRegion<u8>>,
                    )>>();
                *this.state = FetchBatchOpState::OneSidedMemoryRegionLaunched(launched);
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
            // FetchBatchOpState::ByteLaunched(reqs) => {
            //     while let Some(mut req) = reqs.pop_front() {
            //         match Future::poll(Pin::new(&mut req.0), cx) {
            //             Poll::Pending => {
            //                 reqs.push_front(req);
            //                 return Poll::Pending;
            //             }
            //             Poll::Ready(bytes) => {
            //                 let mut res = bytes_to_vec_t::<R>(bytes);
            //                 for (val, idx) in res.drain(..).zip(req.1.iter()) {
            //                     this.results[*idx] = val;
            //                 }
            //             }
            //         }
            //     }
            // }
            FetchBatchOpState::OneSidedMemoryRegionLaunched(reqs) => {
                while let Some(mut req) = reqs.pop_front() {
                    if let Some(mem_region) = req.0.as_mut() {
                        if let Poll::Ready(mem_region) = Future::poll(Pin::new(mem_region), cx) {
                            let num_bytes = mem_region.len();
                            let data_task = unsafe {
                                mem_region
                                    .clone()
                                    .to_base::<R>()
                                    .get_buffer(0, num_bytes / std::mem::size_of::<R>())
                                    .spawn()
                            };
                            req.0 = None;
                            req.1 = Some(data_task);
                            req.3 = Some(mem_region);
                            cx.waker().wake_by_ref();
                        }
                        reqs.push_front(req);
                        return Poll::Pending;
                    } else if let Some(rdma) = req.1.as_mut() {
                        match Future::poll(Pin::new(rdma), cx) {
                            Poll::Pending => {
                                reqs.push_front(req);
                                return Poll::Pending;
                            }
                            Poll::Ready(data) => {
                                drop(req.3.take());
                                for (val, idx) in data.into_iter().zip(req.2.iter()) {
                                    this.results[*idx] = val;
                                }
                            }
                        }
                    } else {
                        panic!("Both OneSidedMemoryRegion and Rdma are None");
                    }
                }
            }
        }
        Poll::Ready(std::mem::take(&mut this.results))
    }
}

// fn bytes_to_result_vec<T: Dist>( bytes: Vec<u8>) -> Vec<Result<T, T>> {
//     crate::deserialize(&bytes, true).expect("failed to deserialize result vec")
// }

/// a task handle for a single array operation that returns a result
#[must_use = "Array operation handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called. Ignoring the resulting value with 'let _ = ...' will cause the operation to NOT BE executed."]
#[pin_project]
pub struct ArrayResultOpHandle<R: Dist + PartialEq> {
    // dropped handle triggered by AmHandle
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ResultOpState<R>,
}

#[pin_project(project = ResultOpStateProj)]
pub(crate) enum ResultOpState<R: Remote + PartialEq> {
    // Req(AmHandle<Vec<Result<R, R>>>),
    // ByteReq(AmHandle<Vec<u8>>),
    OneSidedMemoryRegionReq(#[pin] AmHandle<OneSidedMemoryRegion<u8>>),
    Rdma(
        #[pin] RdmaGetHandle<Result<R, R>>,
        Option<OneSidedMemoryRegion<u8>>,
    ),
    Network(#[pin] AtomicCompareExchangeOpHandle<R>),
    Launched(#[pin] LamellarTask<Vec<Result<R, R>>>),
    // ByteLaunched(LamellarTask<Vec<u8>>),
    OneSidedMemoryRegionLaunched(#[pin] LamellarTask<OneSidedMemoryRegion<u8>>),
}

impl<R: Dist + PartialEq> ArrayResultOpHandle<R> {
    /// Spawn the array operation.
    ///
    /// This method will spawn the associated Array Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.put(0, 42); // or other op like get, local_iter_mut, etc
    /// let task = handle.spawn();
    /// ```
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Result<R, R>> {
        match self.state {
            // ResultOpState::Req(req) => {
            //     self.state = ResultOpState::Launched(req.spawn());
            //     self.array.team().spawn(self)
            // }
            // ResultOpState::ByteReq(req) => {
            //     self.state = ResultOpState::ByteLaunched(req.spawn());
            //     self.array.team().spawn(self)
            // }
            ResultOpState::OneSidedMemoryRegionReq(op_handle) => {
                self.state = ResultOpState::OneSidedMemoryRegionLaunched(op_handle.spawn());
                self.array.team().spawn(self)
            }
            ResultOpState::Rdma(op_handle, mem_region) => {
                let task = op_handle.spawn();
                drop(mem_region);
                task
            }
            ResultOpState::Network(handle) => handle.spawn(),
            _ => panic!("ArrayResultOpHandle should already have been spawned"),
        }
    }

    /// Block until the array operation completes.
    ///
    /// This method will block the calling thread until the associated Array Operation completes
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.compare_exchange(0, 42, 0);
    /// let result = handle.block();
    /// ```
    pub fn block(mut self) -> Result<R, R> {
        RuntimeWarning::BlockingCall(
            "ArrayResultOpHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        match self.state {
            // ResultOpState::ByteReq(req) => {
            //     self.state = ResultOpState::ByteLaunched(req.spawn());
            //     self.array.team().block_on(self)
            // }
            // ResultOpState::Req(req) => {
            //     self.state = ResultOpState::Launched(req.spawn());
            //     self.array.team().block_on(self)
            // }
            ResultOpState::OneSidedMemoryRegionReq(op_handle) => {
                self.state = ResultOpState::OneSidedMemoryRegionLaunched(op_handle.spawn());
                self.array.team().block_on(self)
            }
            ResultOpState::Rdma(op_handle, mem_region) => {
                let data = op_handle.block();
                drop(mem_region);
                data
            }
            ResultOpState::Network(handle) => handle.block(),
            ResultOpState::Launched(ref _req) => self.array.team().block_on(self),
            // ResultOpState::ByteLaunched(ref _req) => {
            //     self.array.team().block_on(self)
            // }
            ResultOpState::OneSidedMemoryRegionLaunched(ref _req) => {
                self.array.team().block_on(self)
            }
        }
    }
}

impl<R: Dist + PartialEq> Future for ArrayResultOpHandle<R> {
    type Output = Result<R, R>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match this.state.as_mut().project() {
            // ResultOpStateProj::Req(req) => {
            //     if req.ready_or_set_waker(cx.waker()) {
            //         return Poll::Ready(req.val().pop().expect("should have a single request"));
            //     }
            // }
            // ResultOpStateProj::ByteReq(req) => {
            //     if req.ready_or_set_waker(cx.waker()) {
            //         let bytes = req.val();
            //         return Poll::Ready(
            //             bytes_to_result_vec::<R>(bytes).pop().expect("should have a single request"),
            //         );
            //     }
            // }
            ResultOpStateProj::OneSidedMemoryRegionReq(req) => {
                let mem_region = ready!(req.poll(cx));
                let rdma = unsafe { mem_region.clone().to_base::<Result<R, R>>().get(0) };
                this.state.set(ResultOpState::Rdma(rdma, Some(mem_region)));
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            ResultOpStateProj::Rdma(req, _mem_region) => req.poll(cx),
            ResultOpStateProj::Network(req) => req.poll(cx),
            ResultOpStateProj::Launched(req) => {
                let mut res = ready!(req.poll(cx));
                Poll::Ready(res.pop().expect("should have a single request"))
            }
            // ResultOpStateProj::ByteLaunched(req) => {
            //     if let Poll::Ready(bytes) = Future::poll(Pin::new(req), cx) {
            //         return Poll::Ready(
            //             bytes_to_result_vec::<R>(bytes).pop().expect("should have a single request"),
            //         );
            //     }
            // }
            ResultOpStateProj::OneSidedMemoryRegionLaunched(req) => {
                let mem_region = ready!(req.poll(cx));
                let rdma = unsafe { mem_region.clone().to_base::<Result<R, R>>().get(0) };
                this.state.set(ResultOpState::Rdma(rdma, Some(mem_region)));
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
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
    // Reqs(VecDeque<(AmHandle<Vec<Result<R, R>>>, Vec<usize>)>),
    // ByteReqs(VecDeque<(AmHandle<Vec<u8>>, Vec<usize>)>),
    OneSidedMemoryRegionReqs(VecDeque<(AmHandle<OneSidedMemoryRegion<u8>>, Vec<usize>)>),
    Launched(VecDeque<(LamellarTask<Vec<Result<R, R>>>, Vec<usize>)>),
    // ByteLaunched(VecDeque<(LamellarTask<Vec<u8>>, Vec<usize>)>),
    OneSidedMemoryRegionLaunched(
        VecDeque<(
            Option<LamellarTask<OneSidedMemoryRegion<u8>>>,
            Option<LamellarTask<Vec<Result<R, R>>>>,
            Vec<usize>,
            Option<OneSidedMemoryRegion<u8>>,
        )>,
    ),
}

#[pinned_drop]
impl<R: AmDist> PinnedDrop for ArrayResultBatchOpHandle<R> {
    fn drop(self: Pin<&mut Self>) {
        let mut this = self.project();
        match &mut this.state {
            // BatchResultOpState::Reqs(reqs) => {
            //     RuntimeWarning::disable_warnings();
            //     for _ in reqs.drain(0..) {}
            //     RuntimeWarning::enable_warnings();
            //     RuntimeWarning::DroppedHandle("an ArrayResultBatchOpHandle").print();
            // }
            // BatchResultOpState::ByteReqs(reqs) => {
            //     RuntimeWarning::disable_warnings();
            //     for _ in reqs.drain(0..) {}
            //     RuntimeWarning::enable_warnings();
            //     RuntimeWarning::DroppedHandle("an ArrayResultBatchOpHandle").print();
            // }
            BatchResultOpState::OneSidedMemoryRegionReqs(reqs) => {
                RuntimeWarning::disable_warnings();
                for _ in reqs.drain(0..) {}
                RuntimeWarning::enable_warnings();
                RuntimeWarning::DroppedHandle("an ArrayResultBatchOpHandle").print();
            }
            _ => {}
        }
    }
}

impl<R: AmDist + Dist> ArrayResultBatchOpHandle<R> {
    /// Spawn the array operation.
    ///
    /// This method will spawn the associated Array Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.put(0, 42); // or other op like get, local_iter_mut, etc
    /// let task = handle.spawn();
    /// ```
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Vec<Result<R, R>>> {
        match &mut self.state {
            // BatchResultOpState::Reqs(reqs) => {
            //     let launched = reqs
            //         .drain(..)
            //         .map(|(am, res)| (am.spawn(), res))
            //         .collect::<VecDeque<(LamellarTask<Vec<Result<R, R>>>, Vec<usize>)>>();
            //     self.state = BatchResultOpState::Launched(launched);
            //     self.array.team().spawn(self)
            // }
            // BatchResultOpState::ByteReqs(reqs) => {
            //     let launched = reqs
            //         .drain(..)
            //         .map(|(am, res)| (am.spawn(), res))
            //         .collect::<VecDeque<(LamellarTask<Vec<u8>>, Vec<usize>)>>();
            //     self.state = BatchResultOpState::ByteLaunched(launched);
            //     self.array.team().spawn(self)
            // }
            BatchResultOpState::OneSidedMemoryRegionReqs(reqs) => {
                let launched = reqs
                    .drain(..)
                    .map(|(am, res)| (Some(am.spawn()), None, res, None))
                    .collect::<VecDeque<(
                        Option<LamellarTask<OneSidedMemoryRegion<u8>>>,
                        Option<LamellarTask<Vec<Result<R, R>>>>,
                        Vec<usize>,
                        Option<OneSidedMemoryRegion<u8>>,
                    )>>();
                self.state = BatchResultOpState::OneSidedMemoryRegionLaunched(launched);
                self.array.team().spawn(self)
            }
            _ => panic!("ArrayFetchBatchOpHandle should already have been spawned"),
        }
    }

    /// Block until the array operation completes.
    ///
    /// This method will block the calling thread until the associated Array Operation completes
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, 100, Distribution::Block).block();
    /// let handle = array.compare_exchange(0, 42, 0);
    /// let result = handle.block();
    /// ```
    pub fn block(mut self) -> Vec<Result<R, R>> {
        RuntimeWarning::BlockingCall(
            "ArrayResultBatchOpHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        match &mut self.state {
            // BatchResultOpState::Reqs(reqs) => {
            //     let launched = reqs
            //         .drain(..)
            //         .map(|(am, res)| (am.spawn(), res))
            //         .collect::<VecDeque<(LamellarTask<Vec<Result<R, R>>>, Vec<usize>)>>();
            //     self.state = BatchResultOpState::Launched(launched);
            //     self.array.team().block_on(self)
            // }
            // BatchResultOpState::ByteReqs(reqs) => {
            //     let launched = reqs
            //         .drain(..)
            //         .map(|(am, res)| (am.spawn(), res))
            //         .collect::<VecDeque<(LamellarTask<Vec<u8>>, Vec<usize>)>>();
            //     self.state = BatchResultOpState::ByteLaunched(launched);
            //     self.array.team().block_on(self)
            // }
            BatchResultOpState::OneSidedMemoryRegionReqs(reqs) => {
                let launched = reqs
                    .drain(..)
                    .map(|(am, res)| (Some(am.spawn()), None, res, None))
                    .collect::<VecDeque<(
                        Option<LamellarTask<OneSidedMemoryRegion<u8>>>,
                        Option<LamellarTask<Vec<Result<R, R>>>>,
                        Vec<usize>,
                        Option<OneSidedMemoryRegion<u8>>,
                    )>>();
                self.state = BatchResultOpState::OneSidedMemoryRegionLaunched(launched);
                self.array.team().block_on(self)
            }
            BatchResultOpState::Launched(_)
            | BatchResultOpState::OneSidedMemoryRegionLaunched(_) => {
                self.array.team().block_on(self)
            }
        }
    }
}

impl<R: Dist + PartialEq> From<ArrayResultBatchOpHandle<R>> for ArrayResultOpHandle<R> {
    fn from(mut req: ArrayResultBatchOpHandle<R>) -> Self {
        let handle = match &mut req.state {
            // BatchResultOpState::Reqs(reqs) => Self {
            //     array: req.array.clone(),
            //     state: ResultOpState::Req(reqs.pop_front().unwrap().0),
            // },
            // BatchResultOpState::ByteReqs(reqs) => Self {
            //     array: req.array.clone(),
            //     state: ResultOpState::ByteReq(reqs.pop_front().unwrap().0),
            // },
            BatchResultOpState::OneSidedMemoryRegionReqs(reqs) => Self {
                array: req.array.clone(),
                state: ResultOpState::OneSidedMemoryRegionReq(reqs.pop_front().unwrap().0),
            },
            BatchResultOpState::Launched(reqs) => Self {
                array: req.array.clone(),
                state: ResultOpState::Launched(reqs.pop_front().unwrap().0),
            },
            // BatchResultOpState::ByteLaunched(reqs) => Self {
            //     array: req.array.clone(),
            //     state: ResultOpState::ByteLaunched(reqs.pop_front().unwrap().0),
            // },
            BatchResultOpState::OneSidedMemoryRegionLaunched(reqs) => Self {
                array: req.array.clone(),
                state: ResultOpState::OneSidedMemoryRegionLaunched(
                    reqs.pop_front()
                        .unwrap()
                        .0
                        .expect("Expected a launched task"),
                ),
            },
        };
        req.state = BatchResultOpState::Launched(VecDeque::new());
        handle
    }
}

impl<R: AmDist> ArrayResultBatchOpHandle<R> {
    // pub(crate) fn new(
    //     array: LamellarByteArray,
    //     reqs: VecDeque<(AmHandle<Vec<Result<R, R>>>, Vec<usize>)>,
    //     max_index: usize,
    // ) -> Self {
    //     let mut results = Vec::with_capacity(max_index);
    //     unsafe {
    //         results.set_len(max_index);
    //     }
    //     Self {
    //         array,
    //         state: BatchResultOpState::Reqs(reqs),
    //         results,
    //     }
    // }

    pub(crate) fn new_one_sided_memory_region(
        array: LamellarByteArray,
        reqs: VecDeque<(AmHandle<OneSidedMemoryRegion<u8>>, Vec<usize>)>,
        max_index: usize,
    ) -> Self {
        let mut results = Vec::with_capacity(max_index);
        unsafe {
            results.set_len(max_index);
        }
        Self {
            array,
            state: BatchResultOpState::OneSidedMemoryRegionReqs(reqs),
            results,
        }
    }

    // pub(crate) fn new_bytes(
    //     array: LamellarByteArray,
    //     reqs: VecDeque<(AmHandle<Vec<u8>>, Vec<usize>)>,
    //     max_index: usize,
    // ) -> Self {
    //     let mut results = Vec::with_capacity(max_index);
    //     unsafe {
    //         results.set_len(max_index);
    //     }
    //     Self {
    //         array,
    //         state: BatchResultOpState::ByteReqs(reqs),
    //         results,
    //     }
    // }
}

impl<R: AmDist + Dist> Future for ArrayResultBatchOpHandle<R> {
    type Output = Vec<Result<R, R>>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match &mut this.state {
            // BatchResultOpState::Reqs(reqs) => {
            //     let launched = reqs
            //         .drain(..)
            //         .map(|(am, res)| (am.spawn(), res))
            //         .collect::<VecDeque<(LamellarTask<Vec<Result<R, R>>>, Vec<usize>)>>();
            //     *this.state = BatchResultOpState::Launched(launched);
            //     cx.waker().wake_by_ref();
            //     return Poll::Pending;
            // }
            // BatchResultOpState::ByteReqs(reqs) => {
            //     let launched = reqs
            //         .drain(..)
            //         .map(|(am, res)| (am.spawn(), res))
            //         .collect::<VecDeque<(LamellarTask<Vec<u8>>, Vec<usize>)>>();
            //     *this.state = BatchResultOpState::ByteLaunched(launched);
            //     cx.waker().wake_by_ref();
            //     return Poll::Pending;
            // }
            BatchResultOpState::OneSidedMemoryRegionReqs(reqs) => {
                let launched = reqs
                    .drain(..)
                    .map(|(am, res)| (Some(am.spawn()), None, res, None))
                    .collect::<VecDeque<(
                        Option<LamellarTask<OneSidedMemoryRegion<u8>>>,
                        Option<LamellarTask<Vec<Result<R, R>>>>,
                        Vec<usize>,
                        Option<OneSidedMemoryRegion<u8>>,
                    )>>();
                *this.state = BatchResultOpState::OneSidedMemoryRegionLaunched(launched);
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            BatchResultOpState::Launched(reqs) => {
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
            // BatchResultOpState::ByteLaunched(reqs) => {
            //     while let Some(mut req) = reqs.pop_front() {
            //         match Future::poll(Pin::new(&mut req.0), cx) {
            //             Poll::Pending => {
            //                 reqs.push_front(req);
            //                 return Poll::Pending;
            //             }
            //             Poll::Ready(bytes) => {
            //                 let mut res = bytes_to_result_vec::<R>(bytes);
            //                 for (val, idx) in res.drain(..).zip(req.1.iter()) {
            //                     this.results[*idx] = val;
            //                 }
            //             }
            //         }
            //     }
            // }
            BatchResultOpState::OneSidedMemoryRegionLaunched(reqs) => {
                while let Some(mut req) = reqs.pop_front() {
                    if let Some(mem_region) = req.0.as_mut() {
                        if let Poll::Ready(mem_region) = Future::poll(Pin::new(mem_region), cx) {
                            let num_bytes = mem_region.len();
                            let data_task = unsafe {
                                mem_region
                                    .clone()
                                    .to_base::<Result<R, R>>()
                                    .get_buffer(0, num_bytes / std::mem::size_of::<Result<R, R>>())
                                    .spawn()
                            };
                            req.0 = None;
                            req.1 = Some(data_task);
                            req.3 = Some(mem_region);
                            cx.waker().wake_by_ref();
                        }
                        reqs.push_front(req);
                        return Poll::Pending;
                    } else if let Some(rdma) = req.1.as_mut() {
                        match Future::poll(Pin::new(rdma), cx) {
                            Poll::Pending => {
                                reqs.push_front(req);
                                return Poll::Pending;
                            }
                            Poll::Ready(data) => {
                                drop(req.3.take());
                                for (val, idx) in data.into_iter().zip(req.2.iter()) {
                                    this.results[*idx] = val;
                                }
                            }
                        }
                    } else {
                        panic!("Both OneSidedMemoryRegion and Rdma are None");
                    }
                }
            }
        }
        Poll::Ready(std::mem::take(&mut this.results))
    }
}

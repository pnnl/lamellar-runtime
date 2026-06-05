use core::panic;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::{
    stream::{Collect, FuturesOrdered},
    StreamExt,
};
use pin_project::pin_project;

use crate::{
    active_messaging::LocalAmHandle,
    array::LamellarByteArray,
    lamellae::{RdmaGetBufferHandle, RdmaGetHandle, RdmaGetIntoBufferHandle},
    memregion::{AsLamellarBuffer, LamellarBuffer},
    warnings::RuntimeWarning,
    AmHandle, AtomicFetchOpHandle, Dist, LamellarTask, OneSidedMemoryRegion,
};

/// Handle returned by single-element array RDMA get operations.
///
/// Resolves to `T` when driven to completion via `spawn()`, `block()`, or `.await`.
/// Dropping this handle without doing so will only ensure completion if
/// [`wait_all`][crate::LamellarArray::wait_all] or a barrier is subsequently called.
pub struct ArrayRdmaGetHandle<T: Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    pub(crate) state: ArrayRdmaGetState<T>,
    pub(crate) spawned: bool,
}

pub(crate) enum ArrayRdmaGetState<T: Dist> {
    LocalAmGet(LocalAmHandle<T>),   //Am is initiated as a local am
    RemoteAmGet(AmHandle<Vec<u8>>), //Am is initiated as a remote am
    // LoadOp(ArrayFetchOpHandle<T>),
    RdmaGet(RdmaGetHandle<T>),
    AtomicGet(AtomicFetchOpHandle<T>),
}

impl<T: Dist> ArrayRdmaGetHandle<T> {
    /// Enqueues the get operation on the runtime work queue and returns a [`LamellarTask<T>`]
    /// that resolves to the retrieved element when the transfer is complete.
    ///
    /// Prefer `spawn()` or `.await` over `block()` inside async contexts to avoid stalling
    /// the executor.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world, num_pes, Distribution::Block).block();
    ///
    /// let handle = array.get(0);
    /// let task = handle.spawn();
    /// // do other work …
    /// let val = task.block();
    /// println!("PE{my_pe} got array[0] = {val}");
    ///```
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<T> {
        let task = match self.state {
            ArrayRdmaGetState::LocalAmGet(req) => req.spawn(),
            ArrayRdmaGetState::RemoteAmGet(req) => {
                let task = req.spawn();
                self.array.team().spawn(async move {
                    let data = task.await;
                    // println!("data: {:?}", data);
                    if data.len() != std::mem::size_of::<T>() {
                        panic!(
                            "Remote AM get returned incorrect number of bytes {:?} {:?}",
                            data.len(),
                            std::mem::size_of::<T>()
                        );
                    }
                    unsafe { std::ptr::read_unaligned(data.as_ptr() as *const T) }
                })
            }
            // ArrayRdmaGetState::LoadOp(req) => req.spawn(),
            ArrayRdmaGetState::RdmaGet(req) => req.spawn(),
            ArrayRdmaGetState::AtomicGet(req) => req.spawn(),
        };
        self.spawned = true;
        task
    }
    /// Blocks the current thread until the get transfer is complete and returns the retrieved
    /// element.
    ///
    /// Emits a [`RuntimeWarning`][crate::warnings::RuntimeWarning] when called inside an async
    /// context; use `spawn()` or `.await` there instead.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world, num_pes, Distribution::Block).block();
    ///
    /// let val = array.get(0).block();
    /// println!("PE{my_pe} got array[0] = {val}");
    ///```
    pub fn block(mut self) -> T {
        RuntimeWarning::BlockingCall(
            "ArrayRdmaHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayRdmaGetState::LocalAmGet(req) => req.block(),
            ArrayRdmaGetState::RemoteAmGet(req) => {
                let data = req.block();
                if data.len() != std::mem::size_of::<T>() {
                    panic!("Remote AM get returned incorrect number of bytes");
                }
                unsafe { std::ptr::read_unaligned(data.as_ptr() as *const T) }
            }
            // ArrayRdmaGetState::LoadOp(req) => req.block(),
            ArrayRdmaGetState::RdmaGet(req) => req.block(),
            ArrayRdmaGetState::AtomicGet(req) => req.block(),
        }
    }
}

impl<T: Dist> Future for ArrayRdmaGetHandle<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let res = match &mut this.state {
            ArrayRdmaGetState::LocalAmGet(req) => Pin::new(req).poll(cx),
            ArrayRdmaGetState::RemoteAmGet(req) => match Pin::new(req).poll(cx) {
                Poll::Ready(data) => {
                    if data.len() != std::mem::size_of::<T>() {
                        panic!("Remote AM get returned incorrect number of bytes");
                    }
                    let value = unsafe { std::ptr::read_unaligned(data.as_ptr() as *const T) };
                    Poll::Ready(value)
                }
                Poll::Pending => Poll::Pending,
            },
            // ArrayRdmaGetState::LoadOp(req) => Pin::new(req).poll(cx),
            ArrayRdmaGetState::RdmaGet(req) => Pin::new(req).poll(cx),
            ArrayRdmaGetState::AtomicGet(req) => Pin::new(req).poll(cx),
        };
        this.spawned = true;
        res
    }
}

/// Handle returned by multi-element array RDMA get operations that allocate their own output buffer.
///
/// Resolves to `Vec<T>` when driven to completion via `spawn()`, `block()`, or `.await`.
/// Dropping this handle without doing so will only ensure completion if
/// [`wait_all`][crate::LamellarArray::wait_all] or a barrier is subsequently called.
#[pin_project]
pub struct ArrayRdmaGetBufferHandle<T: Dist> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayRdmaGetBufferState<T>,
    pub(crate) spawned: bool,
}

#[pin_project(project = ArrayRdmaGetBufferStateProj)]
pub(crate) enum ArrayRdmaGetBufferState<T: Dist> {
    LocalAmGet(#[pin] LocalAmHandle<Vec<T>>), //Am is initiated as a local am
    RemoteAmGet(#[pin] AmHandle<()>, OneSidedMemoryRegion<T>), //Am is initiated as a remote am
    RdmaGet(#[pin] RdmaGetBufferHandle<T>),
    MultiRdmaBlockGet(#[pin] Collect<FuturesOrdered<RdmaGetBufferHandle<T>>, Vec<Vec<T>>>),
    MultiRdmaCyclicGet(#[pin] Collect<FuturesOrdered<RdmaGetBufferHandle<T>>, Vec<Vec<T>>>),
}

impl<T: Dist> ArrayRdmaGetBufferHandle<T> {
    /// Enqueues the get operation on the runtime work queue and returns a
    /// [`LamellarTask<Vec<T>>`] that resolves to the retrieved elements when the transfer is
    /// complete.
    ///
    /// Prefer `spawn()` or `.await` over `block()` inside async contexts to avoid stalling
    /// the executor.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world, num_pes * 10, Distribution::Block).block();
    ///
    /// let handle = array.get_buffer(0, 5);
    /// let task = handle.spawn();
    /// // do other work …
    /// let data = task.block();
    /// println!("PE{my_pe} elements[0..5]: {:?}", data);
    ///```
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Vec<T>> {
        let task = match self.state {
            ArrayRdmaGetBufferState::LocalAmGet(req) => req.spawn(),
            ArrayRdmaGetBufferState::RemoteAmGet(req, mr) => {
                let task = req.spawn();
                self.array.team().spawn(async move {
                    let _ = task.await;
                    unsafe { mr.as_slice().to_vec() }
                })
            }
            ArrayRdmaGetBufferState::RdmaGet(req) => req.spawn(),
            ArrayRdmaGetBufferState::MultiRdmaBlockGet(ref mut reqs) => {
                let mut tasks = FuturesOrdered::new().collect();
                std::mem::swap(&mut tasks, reqs);
                self.array.team().spawn(async move {
                    let data = tasks.await;
                    data.into_iter().flatten().collect()
                })
            }
            ArrayRdmaGetBufferState::MultiRdmaCyclicGet(ref mut reqs) => {
                let mut tasks = FuturesOrdered::new().collect();
                std::mem::swap(&mut tasks, reqs);
                self.array.team().spawn(async move {
                    // let mut results = Vec::with_capacity(tasks.len());
                    let results = tasks.await;
                    let num_elems = results.iter().map(|data| data.len()).sum();
                    let mut dst = vec![T::default(); num_elems];
                    let results_len = results.len();
                    for (k, data) in results.into_iter().enumerate() {
                        for (i, v) in data.into_iter().enumerate() {
                            dst[i * results_len + k] = v;
                        }
                    }
                    dst
                })
            }
        };
        self.spawned = true;
        task
    }
    /// Blocks the current thread until the get transfer is complete and returns the retrieved
    /// elements as `Vec<T>`.
    ///
    /// Emits a [`RuntimeWarning`][crate::warnings::RuntimeWarning] when called inside an async
    /// context; use `spawn()` or `.await` there instead.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world, num_pes * 10, Distribution::Block).block();
    ///
    /// let data = array.get_buffer(0, 5).block();
    /// println!("PE{my_pe} elements[0..5]: {:?}", data);
    ///```
    pub fn block(mut self) -> Vec<T> {
        RuntimeWarning::BlockingCall(
            "ArrayRdmaHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayRdmaGetBufferState::LocalAmGet(req) => req.block(),
            ArrayRdmaGetBufferState::RemoteAmGet(req, mr) => {
                req.block();
                unsafe { mr.as_slice().to_vec() }
            }
            ArrayRdmaGetBufferState::RdmaGet(req) => req.block(),
            ArrayRdmaGetBufferState::MultiRdmaBlockGet(ref mut reqs) => {
                let mut tasks = FuturesOrdered::new().collect();
                std::mem::swap(&mut tasks, reqs);
                self.array.team().block_on(async move {
                    let data = tasks.await;
                    data.into_iter().flatten().collect()
                })
            }
            ArrayRdmaGetBufferState::MultiRdmaCyclicGet(ref mut reqs) => {
                let mut tasks = FuturesOrdered::new().collect();
                std::mem::swap(&mut tasks, reqs);
                self.array.team().block_on(async move {
                    let results = tasks.await;
                    let results_len = results.len();
                    let num_elems = results.iter().map(|data| data.len()).sum();
                    let mut dst = vec![T::default(); num_elems];

                    //results are ordered based on the pe where the first index was located, so we need to interleave them
                    for (k, data) in results.into_iter().enumerate() {
                        for (i, v) in data.into_iter().enumerate() {
                            dst[i * results_len + k] = v;
                        }
                    }
                    dst
                })
            }
        }
    }
}

impl<T: Dist> Future for ArrayRdmaGetBufferHandle<T> {
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let res = match this.state.project() {
            ArrayRdmaGetBufferStateProj::LocalAmGet(req) => req.poll(cx),
            ArrayRdmaGetBufferStateProj::RemoteAmGet(req, mr) => match req.poll(cx) {
                Poll::Ready(_) => {
                    let data = unsafe { mr.as_slice().to_vec() };
                    Poll::Ready(data)
                }
                Poll::Pending => Poll::Pending,
            },
            ArrayRdmaGetBufferStateProj::RdmaGet(req) => req.poll(cx),
            ArrayRdmaGetBufferStateProj::MultiRdmaBlockGet(reqs) => match reqs.poll(cx) {
                Poll::Ready(data) => {
                    let data = data.into_iter().flatten().collect();
                    Poll::Ready(data)
                }
                Poll::Pending => Poll::Pending,
            },
            ArrayRdmaGetBufferStateProj::MultiRdmaCyclicGet(reqs) => {
                match reqs.poll(cx) {
                    Poll::Ready(mut results) => {
                        let num_elems = results.iter().map(|data| data.len()).sum();
                        let results_len = results.len();
                        let mut dst = vec![T::default(); num_elems];
                        for (k, data) in results.drain(..).enumerate() {
                            for (i, v) in data.into_iter().enumerate() {
                                dst[i * results_len + k] = v;
                            }
                        }
                        Poll::Ready(dst)
                    } //continue to process below
                    Poll::Pending => return Poll::Pending,
                }
            }
        };
        *this.spawned = true;
        res
    }
}

/// Handle returned by multi-element array RDMA get operations that write into a caller-supplied
/// [`LamellarBuffer`].
///
/// Resolves to `()` when driven to completion via `spawn()`, `block()`, or `.await`; the
/// transferred data is available in the original buffer afterwards.
/// Dropping this handle without doing so will only ensure completion if
/// [`wait_all`][crate::LamellarArray::wait_all] or a barrier is subsequently called.
#[pin_project]
pub struct ArrayRdmaGetIntoBufferHandle<T: Dist, B: AsLamellarBuffer<T>> {
    pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
    #[pin]
    pub(crate) state: ArrayRdmaGetIntoBufferState<T, B>,
    pub(crate) spawned: bool,
    // pub(crate) dst: LamellarBuffer<T, B>, //keep a reference to the dst buffer to ensure it is not dropped too early
}

#[pin_project(project = ArrayRdmaGetIntoBufferStateProj)]
pub(crate) enum ArrayRdmaGetIntoBufferState<T: Dist, B: AsLamellarBuffer<T>> {
    LocalAmGet(#[pin] LocalAmHandle<()>), //Am is initiated as a local am
    RemoteAmGet(LamellarBuffer<T, B>, #[pin] AmHandle<Vec<u8>>), //Am is initiated as a remote am
    RdmaGet(#[pin] RdmaGetIntoBufferHandle<T, B>),
    MultiRdmaBlockGet(#[pin] Collect<FuturesOrdered<RdmaGetIntoBufferHandle<T, B>>, Vec<()>>),
    MultiRdmaCyclicGet(
        LamellarBuffer<T, B>,
        #[pin] Collect<FuturesOrdered<RdmaGetBufferHandle<T>>, Vec<Vec<T>>>,
    ),
}

impl<T: Dist, B: AsLamellarBuffer<T> + 'static> ArrayRdmaGetIntoBufferHandle<T, B> {
    /// Enqueues the get operation on the runtime work queue and returns a [`LamellarTask<()>`]
    /// that resolves when the transfer into the caller-supplied buffer is complete.
    ///
    /// The buffer passed to the originating `get_into_buffer*` call will contain the results
    /// after this task completes. Prefer `spawn()` or `.await` over `block()` inside async
    /// contexts to avoid stalling the executor.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world, num_pes * 10, Distribution::Block).block();
    ///
    /// let dst: Vec<usize> = vec![0usize; 5];
    /// let buf = LamellarBuffer::from_vec(dst);
    /// let handle = array.get_into_buffer(0, buf);
    /// let task = handle.spawn();
    /// // do other work …
    /// let buf = task.block();
    /// // buf is the LamellarBuffer returned from the inner task — use try_unwrap to recover the Vec
    ///```
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<()> {
        let task = match self.state {
            ArrayRdmaGetIntoBufferState::LocalAmGet(req) => req.spawn(),
            ArrayRdmaGetIntoBufferState::RemoteAmGet(mut buf, req) => {
                let task = req.spawn();
                self.array.team().spawn(async move {
                    let data = task.await;
                    let buf_slice = buf.as_mut_slice();
                    let buf_slice_u8 = unsafe {
                        std::slice::from_raw_parts_mut(
                            buf_slice.as_mut_ptr() as *mut u8,
                            buf_slice.len() * std::mem::size_of::<T>(),
                        )
                    };
                    buf_slice_u8.copy_from_slice(&data);
                })
            }
            ArrayRdmaGetIntoBufferState::RdmaGet(req) => req.spawn(),
            ArrayRdmaGetIntoBufferState::MultiRdmaBlockGet(ref mut reqs) => {
                let mut tasks = FuturesOrdered::new().collect();
                std::mem::swap(&mut tasks, reqs);
                self.array.team().spawn(async move {
                    tasks.await;
                })
            }
            ArrayRdmaGetIntoBufferState::MultiRdmaCyclicGet(ref mut dst, ref mut reqs) => {
                let mut tasks = FuturesOrdered::new().collect();
                std::mem::swap(&mut tasks, reqs);
                let mut tmp_dst = dst.split_off(0); //poor mans clone
                self.array.team().spawn(async move {
                    let results = tasks.await;
                    let dst_slice = tmp_dst.as_mut_slice();
                    let results_len = results.len();
                    // let mut k = 0;
                    for (k, data) in results.into_iter().enumerate() {
                        for (i, v) in data.into_iter().enumerate() {
                            dst_slice[i * results_len + k] = v;
                        }
                    }
                })
            }
        };
        self.spawned = true;
        task
    }
    /// Blocks the current thread until the get transfer into the caller-supplied buffer is
    /// complete.
    ///
    /// The buffer passed to the originating `get_into_buffer*` call will contain the results
    /// after this returns. Emits a [`RuntimeWarning`][crate::warnings::RuntimeWarning] when
    /// called inside an async context; use `spawn()` or `.await` there instead.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    ///
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world, num_pes * 10, Distribution::Block).block();
    ///
    /// let dst: Vec<usize> = vec![0usize; 5];
    /// let buf = LamellarBuffer::from_vec(dst);
    /// let buf = array.get_into_buffer(0, buf).block();
    /// let result = buf.try_unwrap().expect("no other references exist");
    /// println!("PE{my_pe} elements[0..5]: {:?}", result);
    ///```
    pub fn block(mut self) {
        RuntimeWarning::BlockingCall(
            "ArrayRdmaHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.spawned = true;
        match self.state {
            ArrayRdmaGetIntoBufferState::LocalAmGet(req) => req.block(),
            ArrayRdmaGetIntoBufferState::RemoteAmGet(mut buf, req) => {
                let data = req.block();
                let buf_slice = buf.as_mut_slice();
                let buf_slice_u8 = unsafe {
                    std::slice::from_raw_parts_mut(
                        buf_slice.as_mut_ptr() as *mut u8,
                        buf_slice.len() * std::mem::size_of::<T>(),
                    )
                };
                buf_slice_u8.copy_from_slice(&data);
            }
            ArrayRdmaGetIntoBufferState::RdmaGet(req) => req.block(),
            ArrayRdmaGetIntoBufferState::MultiRdmaBlockGet(ref mut reqs) => {
                let mut tasks = FuturesOrdered::new().collect();
                std::mem::swap(&mut tasks, reqs);
                self.array.team().block_on(async move {
                    tasks.await;
                });
            }
            ArrayRdmaGetIntoBufferState::MultiRdmaCyclicGet(ref mut dst, ref mut reqs) => {
                let mut tasks = FuturesOrdered::new().collect();
                std::mem::swap(&mut tasks, reqs);
                let mut tmp_dst = dst.split_off(0); //poor mans clone
                self.array.team().block_on(async move {
                    let results = tasks.await;
                    let dst_slice = tmp_dst.as_mut_slice();
                    let results_len = results.len();
                    for (k, data) in results.into_iter().enumerate() {
                        for (i, v) in data.into_iter().enumerate() {
                            dst_slice[i * results_len + k] = v;
                        }
                    }
                });
            }
        }
    }
}

impl<T: Dist, B: AsLamellarBuffer<T>> Future for ArrayRdmaGetIntoBufferHandle<T, B> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let res = match this.state.project() {
            ArrayRdmaGetIntoBufferStateProj::LocalAmGet(req) => match req.poll(cx) {
                Poll::Ready(_) => Poll::Ready(()),
                Poll::Pending => Poll::Pending,
            },
            ArrayRdmaGetIntoBufferStateProj::RemoteAmGet(buf, req) => match req.poll(cx) {
                Poll::Ready(data) => {
                    let buf_slice = buf.as_mut_slice();
                    let buf_slice_u8 = unsafe {
                        std::slice::from_raw_parts_mut(
                            buf_slice.as_mut_ptr() as *mut u8,
                            buf_slice.len() * std::mem::size_of::<T>(),
                        )
                    };
                    buf_slice_u8.copy_from_slice(&data);
                    Poll::Ready(())
                }
                Poll::Pending => Poll::Pending,
            },
            ArrayRdmaGetIntoBufferStateProj::RdmaGet(req) => match req.poll(cx) {
                Poll::Ready(_) => Poll::Ready(()),
                Poll::Pending => Poll::Pending,
            },
            ArrayRdmaGetIntoBufferStateProj::MultiRdmaBlockGet(reqs) => match reqs.poll(cx) {
                Poll::Ready(_) => Poll::Ready(()),
                Poll::Pending => Poll::Pending,
            },
            ArrayRdmaGetIntoBufferStateProj::MultiRdmaCyclicGet(dst, reqs) => match reqs.poll(cx) {
                Poll::Ready(mut results) => {
                    let results_len = results.len();
                    let dst_slice = dst.as_mut_slice();
                    for (k, data) in results.drain(..).enumerate() {
                        for (i, v) in data.into_iter().enumerate() {
                            dst_slice[i * results_len + k] = v;
                        }
                    }
                    Poll::Ready(())
                }
                Poll::Pending => Poll::Pending,
            },
        };
        *this.spawned = true;
        res
    }
}

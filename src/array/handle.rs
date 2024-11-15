use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use pin_project::{pin_project, pinned_drop};

use crate::{
    active_messaging::{AmHandle, LocalAmHandle},
    array::LamellarByteArray,
    lamellar_request::LamellarRequest,
    scheduler::LamellarTask,
    warnings::RuntimeWarning,
    Dist, LamellarTeamRT, OneSidedMemoryRegion, RegisteredMemoryRegion,
};

use super::{AtomicArray, GlobalLockArray, LocalLockArray, ReadOnlyArray, UnsafeArray};

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
    pub fn spawn(mut self) -> LamellarTask<()> {
        for req in self.reqs.iter_mut() {
            req.launch();
        }
        self.spawned = true;
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
    fn launch(&mut self) -> Self::Output {
        for req in self.reqs.iter_mut() {
            req.launch();
        }
        self.spawned = true;
    }
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
        if let Some(req) = &mut self.req {
            req.launch();
        }
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
    fn launch(&mut self) {
        if let Some(req) = &mut self.req {
            req.launch();
        }
        self.spawned = true;
    }
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
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.spawned = true;
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

#[must_use = " Array 'into' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)]
#[doc(alias = "Collective")]
/// This is a handle representing the operation of changing from a some other LamellarArray type into an [UnsafeArray].
/// This handled must either be awaited in an async context or blocked on in a non-async context for the operation to be performed.
/// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the Arrays's team, only returning once every PE in the team has completed the call.
///
/// Furthermore, the handle will not return while any additional references outside of the one making this call exist on each PE. It is not possible for the
/// pointed to object to respresented as more than one array type simultaneously (on any PE).
///
/// # Collective Operation
/// Requires all PEs associated with the `darc` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
///
/// # Examples
/// ```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
///
/// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
/// let unsafe_array = array.into_unsafe().block();
/// /* alternatively something like the following is valid as well
/// let unsafe_array = world.block_on(async move{
///     array.into_unsafe().await
/// })
///  */
/// ```
pub struct IntoUnsafeArrayHandle<T> {
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) launched: bool,
    #[pin]
    pub(crate) outstanding_future: Pin<Box<dyn Future<Output = UnsafeArray<T>> + Send>>,
}

#[pinned_drop]
impl<T> PinnedDrop for IntoUnsafeArrayHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            RuntimeWarning::DroppedHandle("a IntoUnsafeArrayHandle").print();
        }
    }
}

impl<T: Dist + 'static> IntoUnsafeArrayHandle<T> {
    /// Used to drive the cconversion of another LamellarArray into an [UnsafeArray]
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
    /// let unsafe_array = array.into_unsafe().block();
    pub fn block(mut self) -> UnsafeArray<T> {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "IntoUnsafeArrayHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.team.clone().block_on(self)
    }

    /// This method will spawn the conversion  into the UnsafeArray on the work queue.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block).block();
    /// let unsafe_array_task = array.into_unsafe().spawn();
    /// let unsafe_array = unsafe_array_task.block();
    /// ```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(mut self) -> LamellarTask<UnsafeArray<T>> {
        self.launched = true;
        self.team.clone().spawn(self)
    }
}

impl<T> Future for IntoUnsafeArrayHandle<T> {
    type Output = UnsafeArray<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        match this.outstanding_future.as_mut().poll(cx) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(array) => Poll::Ready(array),
        }
    }
}

#[must_use = " Array 'into' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)]
#[doc(alias = "Collective")]
/// This is a handle representing the operation of changing from a some other LamellarArray type into an [AtomicArray].
/// This handled must either be awaited in an async context or blocked on in a non-async context for the operation to be performed.
/// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the Arrays's team, only returning once every PE in the team has completed the call.
///
/// Furthermore, the handle will not return while any additional references outside of the one making this call exist on each PE. It is not possible for the
/// pointed to object to respresented as more than one array type simultaneously (on any PE).
///
/// # Collective Operation
/// Requires all PEs associated with the `darc` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
///
/// # Examples
/// ```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
///
/// let array = GlobalLockArray::<usize>::new(&world,100, Distribution::Block).block();
/// let atomic_array = array.into_atomic().block();
/// /* alternatively something like the following is valid as well
/// let atomic_array = world.block_on(async move{
///     array.into_unsafe().await
/// })
///  */
/// ```
pub struct IntoAtomicArrayHandle<T: Dist> {
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) launched: bool,
    #[pin]
    pub(crate) outstanding_future: Pin<Box<dyn Future<Output = AtomicArray<T>> + Send>>,
}

#[pinned_drop]
impl<T: Dist> PinnedDrop for IntoAtomicArrayHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            RuntimeWarning::DroppedHandle("a IntoAtomicArrayHandle").print();
        }
    }
}

impl<T: Dist + 'static> IntoAtomicArrayHandle<T> {
    /// Used to drive the cconversion of another LamellarArray into an [AtomicArray]
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let array = GlobalLockArray::<usize>::new(&world,100, Distribution::Block).block();
    /// let atomic_array = array.into_atomic().block();
    pub fn block(mut self) -> AtomicArray<T> {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "IntoAtomicArrayHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.team.clone().block_on(self)
    }

    /// This method will spawn the conversion  into the AtomicArray on the work queue.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = GlobalLockArray::<usize>::new(&world,100, Distribution::Block).block();
    /// let atomic_array_task = array.into_atomic().spawn();
    /// let atomic_array = atomic_array_task.block();
    /// ```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(mut self) -> LamellarTask<AtomicArray<T>> {
        self.launched = true;
        self.team.clone().spawn(self)
    }
}

impl<T: Dist> Future for IntoAtomicArrayHandle<T> {
    type Output = AtomicArray<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        match this.outstanding_future.as_mut().poll(cx) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(array) => Poll::Ready(array),
        }
    }
}

#[must_use = " Array 'into' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)]
#[doc(alias = "Collective")]
/// This is a handle representing the operation of changing from a some other LamellarArray type into an [LocalLockArray].
/// This handled must either be awaited in an async context or blocked on in a non-async context for the operation to be performed.
/// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the Arrays's team, only returning once every PE in the team has completed the call.
///
/// Furthermore, the handle will not return while any additional references outside of the one making this call exist on each PE. It is not possible for the
/// pointed to object to respresented as more than one array type simultaneously (on any PE).
///
/// # Collective Operation
/// Requires all PEs associated with the `darc` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
///
/// # Examples
/// ```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
///
/// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
/// let local_lock_array = array.into_local_lock().block();
/// /* alternatively something like the following is valid as well
/// let local_lock_array = world.block_on(async move{
///     array.into_unsafe().await
/// })
///  */
/// ```
pub struct IntoLocalLockArrayHandle<T: Dist> {
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) launched: bool,
    #[pin]
    pub(crate) outstanding_future: Pin<Box<dyn Future<Output = LocalLockArray<T>> + Send>>,
}

#[pinned_drop]
impl<T: Dist> PinnedDrop for IntoLocalLockArrayHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            RuntimeWarning::DroppedHandle("a IntoLocalLockArrayHandle").print();
        }
    }
}

impl<T: Dist + 'static> IntoLocalLockArrayHandle<T> {
    /// Used to drive the cconversion of another LamellarArray into an [LocalLockArray]
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
    /// let local_lock_array = array.into_local_lock().block();
    pub fn block(mut self) -> LocalLockArray<T> {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "IntoLocalLockArrayHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.team.clone().block_on(self)
    }

    /// This method will spawn the conversion  into the LocalLockArray on the work queue.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
    /// let local_lock_array_task = array.into_local_lock().spawn();
    /// let local_lock_array = local_lock_array_task.block();
    /// ```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(mut self) -> LamellarTask<LocalLockArray<T>> {
        self.launched = true;
        self.team.clone().spawn(self)
    }
}

impl<T: Dist> Future for IntoLocalLockArrayHandle<T> {
    type Output = LocalLockArray<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        match this.outstanding_future.as_mut().poll(cx) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(array) => Poll::Ready(array),
        }
    }
}

#[must_use = " Array 'into' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)]
#[doc(alias = "Collective")]
/// This is a handle representing the operation of changing from a some other LamellarArray type into an [GlobalLockArray].
/// This handled must either be awaited in an async context or blocked on in a non-async context for the operation to be performed.
/// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the Arrays's team, only returning once every PE in the team has completed the call.
///
/// Furthermore, the handle will not return while any additional references outside of the one making this call exist on each PE. It is not possible for the
/// pointed to object to respresented as more than one array type simultaneously (on any PE).
///
/// # Collective Operation
/// Requires all PEs associated with the `darc` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
///
/// # Examples
/// ```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
///
/// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
/// let global_lock_array = array.into_global_lock().block();
/// /* alternatively something like the following is valid as well
/// let global_lock_array = world.block_on(async move{
///     array.into_unsafe().await
/// })
///  */
/// ```
pub struct IntoGlobalLockArrayHandle<T: Dist> {
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) launched: bool,
    #[pin]
    pub(crate) outstanding_future: Pin<Box<dyn Future<Output = GlobalLockArray<T>> + Send>>,
}

#[pinned_drop]
impl<T: Dist> PinnedDrop for IntoGlobalLockArrayHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            RuntimeWarning::DroppedHandle("a IntoGlobalLockArrayHandle").print();
        }
    }
}

impl<T: Dist + 'static> IntoGlobalLockArrayHandle<T> {
    /// Used to drive the cconversion of another LamellarArray into an [GlobalLockArray]
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
    /// let global_lock_array = array.into_global_lock().block();
    pub fn block(mut self) -> GlobalLockArray<T> {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "IntoGlobalLockArrayHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.team.clone().block_on(self)
    }

    /// This method will spawn the conversion  into the GlobalLockArray on the work queue.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
    /// let global_lock_array_task = array.into_global_lock().spawn();
    /// let global_lock_array = global_lock_array_task.block();
    /// ```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(mut self) -> LamellarTask<GlobalLockArray<T>> {
        self.launched = true;
        self.team.clone().spawn(self)
    }
}

impl<T: Dist> Future for IntoGlobalLockArrayHandle<T> {
    type Output = GlobalLockArray<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        match this.outstanding_future.as_mut().poll(cx) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(array) => Poll::Ready(array),
        }
    }
}

#[must_use = " Array 'into' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)]
#[doc(alias = "Collective")]
/// This is a handle representing the operation of changing from a some other LamellarArray type into an [ReadOnlyArray].
/// This handled must either be awaited in an async context or blocked on in a non-async context for the operation to be performed.
/// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the Arrays's team, only returning once every PE in the team has completed the call.
///
/// Furthermore, the handle will not return while any additional references outside of the one making this call exist on each PE. It is not possible for the
/// pointed to object to respresented as more than one array type simultaneously (on any PE).
///
/// # Collective Operation
/// Requires all PEs associated with the `darc` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
///
/// # Examples
/// ```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
///
/// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
/// let read_only_array = array.into_read_only().block();
/// /* alternatively something like the following is valid as well
/// let read_only_array = world.block_on(async move{
///     array.into_unsafe().await
/// })
///  */
/// ```
pub struct IntoReadOnlyArrayHandle<T: Dist> {
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) launched: bool,
    #[pin]
    pub(crate) outstanding_future: Pin<Box<dyn Future<Output = ReadOnlyArray<T>> + Send>>,
}

#[pinned_drop]
impl<T: Dist> PinnedDrop for IntoReadOnlyArrayHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            RuntimeWarning::DroppedHandle("a IntoReadOnlyArrayHandle").print();
        }
    }
}

impl<T: Dist + 'static> IntoReadOnlyArrayHandle<T> {
    /// Used to drive the cconversion of another LamellarArray into an [ReadOnlyArray]
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
    /// let read_only_array = array.into_read_only().block();
    pub fn block(mut self) -> ReadOnlyArray<T> {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "IntoReadOnlyArrayHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.team.clone().block_on(self)
    }

    /// This method will spawn the conversion  into the ReadOnlyArray on the work queue.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
    /// let read_only_array_task = array.into_read_only().spawn();
    /// let read_only_array = read_only_array_task.block();
    /// ```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(mut self) -> LamellarTask<ReadOnlyArray<T>> {
        self.launched = true;
        self.team.clone().spawn(self)
    }
}

impl<T: Dist> Future for IntoReadOnlyArrayHandle<T> {
    type Output = ReadOnlyArray<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        match this.outstanding_future.as_mut().poll(cx) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(array) => Poll::Ready(array),
        }
    }
}

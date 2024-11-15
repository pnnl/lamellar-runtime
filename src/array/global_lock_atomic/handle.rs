use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::darc::handle::{
    GlobalRwDarcCollectiveWriteHandle, GlobalRwDarcReadHandle, GlobalRwDarcWriteHandle,
};
use crate::scheduler::LamellarTask;
use crate::warnings::RuntimeWarning;
use crate::GlobalLockArray;
use crate::{Dist, LamellarTeamRT};

use futures_util::Future;
use pin_project::{pin_project, pinned_drop};

use super::{
    ArrayOps, GlobalLockCollectiveMutLocalData, GlobalLockLocalData, GlobalLockMutLocalData,
    GlobalLockReadGuard, GlobalLockWriteGuard,
};

#[must_use = " GlobalLockArray 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)]
#[doc(alias = "Collective")]
/// This is a handle representing the operation of creating a new [GlobalLockArray].
/// This handled must either be awaited in an async context or blocked on in a non-async context for the operation to be performed.
/// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the GlobalLockArray's team, only returning once every PE in the team has completed the call.
///
/// # Collective Operation
/// Requires all PEs associated with the `GlobalLockArray` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
///
/// # Examples
/// ```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
///
/// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic).block();
/// ```
pub struct GlobalLockArrayHandle<T: Dist + ArrayOps + 'static> {
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) launched: bool,
    #[pin]
    pub(crate) creation_future: Pin<Box<dyn Future<Output = GlobalLockArray<T>> + Send>>,
}

#[pinned_drop]
impl<T: Dist + ArrayOps + 'static> PinnedDrop for GlobalLockArrayHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            RuntimeWarning::DroppedHandle("a GlobalLockArrayHandle").print();
        }
    }
}

impl<T: Dist + ArrayOps + 'static> GlobalLockArrayHandle<T> {
    /// Used to drive creation of a new GlobalLockArray
    /// # Examples
    ///
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic).block();
    pub fn block(mut self) -> GlobalLockArray<T> {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "GlobalLockArrayHandle::block",
            "<handle>.spawn() or<handle>.await",
        )
        .print();
        self.team.clone().block_on(self)
    }

    /// This method will spawn the creation of the GlobalLockArray on the work queue
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// /// # Examples
    ///
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array_task = GlobalLockArray::<usize>::new(&world,100,Distribution::Cyclic).spawn();
    /// // do some other work
    /// let array = array_task.block();
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(mut self) -> LamellarTask<GlobalLockArray<T>> {
        self.launched = true;
        self.team.clone().spawn(self)
    }
}

impl<T: Dist + ArrayOps + 'static> Future for GlobalLockArrayHandle<T> {
    type Output = GlobalLockArray<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        this.creation_future.as_mut().poll(cx)
    }
}

#[must_use = "GlobalLockArray lock handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project] //unused drop warning triggered by GlobalRwDarcReadHandle
/// Handle used to retrieve the aquired read lock of a GlobalLockArray
///
/// This handle must be awaited or blocked on to acquire the lock
///
/// Once awaited/blocked the handle will not return while any writer currently has access to the lock, but there may be other readers
///
/// Returns an RAII guard which will drop the read access of the wrlock when dropped
/// # Examples
///```
/// use lamellar::array::prelude::*;
/// let world = LamellarWorldBuilder::new().build();
/// let my_pe = world.my_pe();
/// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic).block();
/// let handle = array.read_lock();
/// let task = world.spawn(async move {
///     let read_lock = handle.await;
///     //do interesting work
/// });
/// array.read_lock().block();
/// task.block();
///```
pub struct GlobalLockReadHandle<T> {
    pub(crate) array: GlobalLockArray<T>,
    #[pin]
    pub(crate) lock_handle: GlobalRwDarcReadHandle<()>,
}

impl<T: Dist> GlobalLockReadHandle<T> {
    pub(crate) fn new(array: GlobalLockArray<T>) -> Self {
        Self {
            array: array.clone(),
            lock_handle: array.lock.read(),
        }
    }
    /// Handle used to retrieve the aquired read lock of a GlobalLockArray within a non async context
    ///
    /// Returns an RAII guard which will drop the read access of the wrlock when dropped
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic).block();
    /// let handle = array.read_lock();
    /// let guard = handle.block();
    ///```
    pub fn block(self) -> GlobalLockReadGuard<T> {
        RuntimeWarning::BlockingCall(
            "GlobalLockReadHandle::block",
            "<handle>.spawn() or<handle>.await",
        )
        .print();

        self.array.lock.darc.team().scheduler.block_on(self)
    }

    /// This method will spawn the associated active message to capture the lock on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic).block();
    /// let handle = array.read_lock();
    /// let task = handle.spawn(); // initiate getting the read lock
    /// // do other work
    /// let guard = task.block();
    ///```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(self) -> LamellarTask<GlobalLockReadGuard<T>> {
        self.array.lock.darc.team().spawn(self)
    }
}

impl<T: Dist> Future for GlobalLockReadHandle<T> {
    type Output = GlobalLockReadGuard<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.lock_handle.poll(cx) {
            Poll::Ready(val) => Poll::Ready(GlobalLockReadGuard {
                array: this.array.clone(),
                lock_guard: val,
            }),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[must_use = "GlobalLockArray lock handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project] //unused drop warning triggered by GlobalRwDarcReadHandle
/// Handle used to retrieve the aquired local data [GlobalLockLocalData] of  a GlobalLockArray
///
/// This handle must be awaited or blocked on to acquire the lock
///
/// Once awaited/blocked the handle will not return while any writer currently has access to the lock, but there may be other readers
///
/// Returns an RAII guard which will drop the read access of the wrlock when dropped
/// # Examples
///```
/// use lamellar::array::prelude::*;
/// let world = LamellarWorldBuilder::new().build();
/// let my_pe = world.my_pe();
///
/// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic).block();
/// let handle = array.read_local_data();
/// world.spawn(async move {
///     let  local_data = handle.await;
///     println!("PE{my_pe}, local_data: {:?}", local_data);
/// });
/// let mut local_data = array.read_local_data().block();
/// println!("PE{my_pe}, local_data: {:?}", local_data);
///```
pub struct GlobalLockLocalDataHandle<T: Dist> {
    pub(crate) array: GlobalLockArray<T>,
    pub(crate) start_index: usize,
    pub(crate) end_index: usize,
    #[pin]
    pub(crate) lock_handle: GlobalRwDarcReadHandle<()>,
}

impl<T: Dist> GlobalLockLocalDataHandle<T> {
    /// Handle used to retrieve the aquired local data [GlobalLockLocalData] of a GlobalLockArray within a non async context
    ///
    /// Returns an RAII guard which will drop the write access of the wrlock when dropped
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic).block();
    /// let handle = array.read_local_data();
    /// let  local_data = handle.block();
    /// println!("local data: {:?}",local_data);
    ///```
    pub fn block(self) -> GlobalLockLocalData<T> {
        RuntimeWarning::BlockingCall(
            "GlobalLockLocalDataHandle::block",
            "<handle>.spawn() or<handle>.await",
        )
        .print();

        self.array.lock.darc.team().scheduler.block_on(self)
    }
    /// This method will spawn the associated active message to capture the lock and data on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic).block();
    /// let handle = array.read_local_data();
    /// let task = handle.spawn(); // initiate getting the read lock
    /// // do other work
    /// let  local_data = task.block();
    /// println!("local data: {:?}",local_data);
    ///```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(self) -> LamellarTask<GlobalLockLocalData<T>> {
        self.array.lock.darc.team().spawn(self)
    }
}

impl<T: Dist> Future for GlobalLockLocalDataHandle<T> {
    type Output = GlobalLockLocalData<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.lock_handle.poll(cx) {
            Poll::Ready(val) => Poll::Ready(GlobalLockLocalData {
                array: this.array.clone(),
                start_index: *this.start_index,
                end_index: *this.end_index,
                // lock: self.lock.clone(),
                lock_guard: val,
            }),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[must_use = "GlobalLockArray lock handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project] //unused drop warning triggered by GlobalRwDarcWriteHandle
/// Handle used to retrieve the aquired write lock of a GlobalLockArray
///
/// This handle must be awaited or blocked on to acquire the lock
///
/// Once awaited/blocked the handle will not return while any readers or writer currently has access to the lock
///
/// Returns an RAII guard which will drop the read access of the wrlock when dropped
/// # Examples
///```
/// use lamellar::array::prelude::*;
/// let world = LamellarWorldBuilder::new().build();
/// let my_pe = world.my_pe();
/// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic).block();
/// let handle = array.write_lock();
/// let task = world.spawn(async move {
///     let write_lock = handle.await;
///     //do interesting work
/// });
/// array.write_lock().block();
/// task.block();
///```
pub struct GlobalLockWriteHandle<T> {
    pub(crate) array: GlobalLockArray<T>,
    #[pin]
    pub(crate) lock_handle: GlobalRwDarcWriteHandle<()>,
}

impl<T: Dist> GlobalLockWriteHandle<T> {
    pub(crate) fn new(array: GlobalLockArray<T>) -> Self {
        Self {
            array: array.clone(),
            lock_handle: array.lock.write(),
        }
    }
    /// Handle used to retrieve the aquired write lock of a GlobalLockArray within a non async context
    ///
    /// Returns an RAII guard which will drop the write access of the wrlock when dropped
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic).block();
    /// let handle = array.write_lock();
    /// let guard = handle.block();
    ///```
    pub fn block(self) -> GlobalLockWriteGuard<T> {
        RuntimeWarning::BlockingCall(
            "GlobalLockWriteHandle::block",
            "<handle>.spawn() or<handle>.await",
        )
        .print();

        self.array.lock.darc.team().scheduler.block_on(self)
    }

    /// This method will spawn the associated active message to capture the lock  on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic).block();
    /// let handle = array.write_lock();
    /// let task = handle.spawn(); // initiate getting the read lock
    /// // do other work
    /// let guard = task.block();
    ///```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(self) -> LamellarTask<GlobalLockWriteGuard<T>> {
        self.array.lock.darc.team().spawn(self)
    }
}

impl<T: Dist> Future for GlobalLockWriteHandle<T> {
    type Output = GlobalLockWriteGuard<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.lock_handle.poll(cx) {
            Poll::Ready(val) => Poll::Ready(GlobalLockWriteGuard {
                array: this.array.clone(),
                lock_guard: val,
            }),
            Poll::Pending => Poll::Pending,
        }
    }
}
#[must_use = "GlobalLockArray lock handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project] //unused drop warning triggered by GlobalRwDarcWriteHandle
/// Handle used to retrieve the aquired mutable local data [GlobalLockMutLocalData] of  a GlobalLockArray
///
/// This handle must be awaited or blocked on to acquire the lock
///
/// Once awaited/blocked the handle will not return while any readers or writer currently has access to the lock
///
/// Returns an RAII guard which will drop the read access of the wrlock when dropped
/// # Examples
///```
/// use lamellar::array::prelude::*;
/// let world = LamellarWorldBuilder::new().build();
/// let my_pe = world.my_pe();
///
/// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic).block();
/// let handle = array.write_local_data();
/// world.spawn(async move {
///     let mut local_data = handle.await;
///     local_data.iter_mut().for_each(|elem| *elem += my_pe);
/// });
/// let mut local_data = array.write_local_data().block();
/// local_data.iter_mut().for_each(|elem| *elem += my_pe);
///```
pub struct GlobalLockMutLocalDataHandle<T: Dist> {
    pub(crate) array: GlobalLockArray<T>,
    pub(crate) start_index: usize,
    pub(crate) end_index: usize,
    #[pin]
    pub(crate) lock_handle: GlobalRwDarcWriteHandle<()>,
}

impl<T: Dist> GlobalLockMutLocalDataHandle<T> {
    /// Handle used to retrieve the aquired mutable local data [GlobalLockMutLocalData] of a GlobalLockArray within a non async context
    ///
    /// Returns an RAII guard which will drop the write access of the wrlock when dropped
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic).block();
    /// let handle = array.write_local_data();
    /// let mut local_data = handle.block();
    /// local_data.iter_mut().for_each(|elem| *elem += my_pe);
    ///```
    pub fn block(self) -> GlobalLockMutLocalData<T> {
        RuntimeWarning::BlockingCall(
            "GlobalLockMutLocalDataHandle::block",
            "<handle>.spawn() or<handle>.await",
        )
        .print();

        self.array.lock.darc.team().scheduler.block_on(self)
    }

    /// This method will spawn the associated active message to capture the lock and data on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic).block();
    /// let handle = array.write_local_data();
    /// let task = handle.spawn(); // initiate getting the read lock
    /// // do other work
    /// let mut local_data = task.block();
    /// local_data.iter_mut().for_each(|elem| *elem += my_pe);
    ///```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(self) -> LamellarTask<GlobalLockMutLocalData<T>> {
        self.array.lock.darc.team().spawn(self)
    }
}

impl<T: Dist> Future for GlobalLockMutLocalDataHandle<T> {
    type Output = GlobalLockMutLocalData<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.lock_handle.poll(cx) {
            Poll::Ready(val) => Poll::Ready(GlobalLockMutLocalData {
                array: this.array.clone(),
                start_index: *this.start_index,
                end_index: *this.end_index,
                // lock: self.lock.clone(),
                lock_guard: val,
            }),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[must_use = "GlobalLockArray lock handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project] //unused drop warning triggered by GlobalRwDarcCollectiveWriteHandle
/// Handle used to retrieve the aquired mutable local data [GlobalLockMutLocalData] of a GlobalLockArray with all PEs collectively accessing their local data
///
/// This handle must be awaited or blocked on to acquire the lock
///
/// Once awaited/blocked the handle will not return while any readers or writer currently has access to the lock
///
/// Returns an RAII guard which will drop the read access of the wrlock when dropped
/// # Examples
///```
/// use lamellar::array::prelude::*;
/// let world = LamellarWorldBuilder::new().build();
/// let my_pe = world.my_pe();
///
/// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic).block();
/// let handle = array.collective_write_local_data();
/// world.block_on(async move {
///     let mut local_data = handle.await;
///     local_data.iter_mut().for_each(|elem| *elem += my_pe);
/// });
/// let mut local_data = array.collective_write_local_data().block();
/// local_data.iter_mut().for_each(|elem| *elem += my_pe);
///```
pub struct GlobalLockCollectiveMutLocalDataHandle<T: Dist> {
    pub(crate) array: GlobalLockArray<T>,
    pub(crate) start_index: usize,
    pub(crate) end_index: usize,
    #[pin]
    pub(crate) lock_handle: GlobalRwDarcCollectiveWriteHandle<()>,
}

impl<T: Dist> GlobalLockCollectiveMutLocalDataHandle<T> {
    /// Handle used to retrieve the aquired mutable local data [GlobalLockMutLocalData] of a GlobalLockArray within a non async context
    /// with all PEs collectively accessing their local data
    ///
    /// Returns an RAII guard which will drop the write access of the wrlock when dropped
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic).block();
    /// let handle = array.collective_write_local_data();
    /// let mut local_data = handle.block();
    /// local_data.iter_mut().for_each(|elem| *elem += my_pe);
    ///```
    pub fn block(self) -> GlobalLockCollectiveMutLocalData<T> {
        RuntimeWarning::BlockingCall(
            "GlobalLockCollectiveMutLocalData::block",
            "<handle>.spawn() or<handle>.await",
        )
        .print();

        self.array.lock.darc.team().scheduler.block_on(self)
    }

    /// This method will spawn the associated active message to capture the lock and data on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic).block();
    /// let handle = array.collective_write_local_data();
    /// let task = handle.spawn(); // initiate getting the read lock
    /// // do other work
    /// let mut local_data = task.block();
    /// local_data.iter_mut().for_each(|elem| *elem += my_pe);
    ///```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(self) -> LamellarTask<GlobalLockCollectiveMutLocalData<T>> {
        self.array.lock.darc.team().spawn(self)
    }
}

impl<T: Dist> Future for GlobalLockCollectiveMutLocalDataHandle<T> {
    type Output = GlobalLockCollectiveMutLocalData<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.lock_handle.poll(cx) {
            Poll::Ready(val) => Poll::Ready(GlobalLockCollectiveMutLocalData {
                array: this.array.clone(),
                start_index: *this.start_index,
                end_index: *this.end_index,
                // lock: self.lock.clone(),
                _lock_guard: val,
            }),
            Poll::Pending => Poll::Pending,
        }
    }
}

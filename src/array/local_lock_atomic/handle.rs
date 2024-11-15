use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::darc::handle::{LocalRwDarcReadHandle, LocalRwDarcWriteHandle};
use crate::scheduler::LamellarTask;
use crate::warnings::RuntimeWarning;
use crate::LocalLockArray;
use crate::{Dist, LamellarTeamRT};

use futures_util::Future;
use pin_project::{pin_project, pinned_drop};

use super::{
    ArrayOps, LocalLockLocalChunks, LocalLockLocalChunksMut, LocalLockLocalData,
    LocalLockMutLocalData, LocalLockReadGuard, LocalLockWriteGuard,
};

#[must_use = " LocalLockArray 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)]
#[doc(alias = "Collective")]
/// This is a handle representing the operation of creating a new [LocalLockArray].
/// This handled must either be awaited in an async context or blocked on in a non-async context for the operation to be performed.
/// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the LocalLockArray's team, only returning once every PE in the team has completed the call.
///
/// # Collective Operation
/// Requires all PEs associated with the `LocalLockArray` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
///
/// # Examples
/// ```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
///
/// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic).block();
/// ```
pub struct LocalLockArrayHandle<T: Dist + ArrayOps + 'static> {
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) launched: bool,
    #[pin]
    pub(crate) creation_future: Pin<Box<dyn Future<Output = LocalLockArray<T>> + Send>>,
}

#[pinned_drop]
impl<T: Dist + ArrayOps + 'static> PinnedDrop for LocalLockArrayHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            RuntimeWarning::DroppedHandle("a LocalLockArrayHandle").print();
        }
    }
}

impl<T: Dist + ArrayOps + 'static> LocalLockArrayHandle<T> {
    /// Used to drive creation of a new LocalLockArray
    /// # Examples
    ///
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic).block();
    pub fn block(mut self) -> LocalLockArray<T> {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "LocalLockArrayHandle::block",
            "<handle>.spawn() or<handle>.await",
        )
        .print();
        self.team.clone().block_on(self)
    }

    /// This method will spawn the creation of the LocalLockArray on the work queue
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// /// # Examples
    ///
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array_task = LocalLockArray::<usize>::new(&world,100,Distribution::Cyclic).spawn();
    /// // do some other work
    /// let array = array_task.block();
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(mut self) -> LamellarTask<LocalLockArray<T>> {
        self.launched = true;
        self.team.clone().spawn(self)
    }
}

impl<T: Dist + ArrayOps + 'static> Future for LocalLockArrayHandle<T> {
    type Output = LocalLockArray<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        this.creation_future.as_mut().poll(cx)
    }
}

#[must_use = "LocalLockArray lock handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)] //unused drop warning triggered by LocalRwDarcReadHandle
/// Handle used to retrieve the aquired read lock of a LocalLockArray
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
/// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic).block();
/// let handle = array.read_lock();
/// let task = world.spawn(async move {
///     let read_lock = handle.await;
///     //do interesting work
/// });
/// array.read_lock().block();
/// task.block();
///```
pub struct LocalLockReadHandle<T> {
    pub(crate) array: LocalLockArray<T>,
    #[pin]
    pub(crate) lock_handle: LocalRwDarcReadHandle<()>,
}

#[pinned_drop]
impl<T> PinnedDrop for LocalLockReadHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.lock_handle.launched {
            RuntimeWarning::DroppedHandle("a LocalLockReadHandle").print();
        }
    }
}

impl<T: Dist> LocalLockReadHandle<T> {
    pub(crate) fn new(array: LocalLockArray<T>) -> Self {
        Self {
            array: array.clone(),
            lock_handle: array.lock.read(),
        }
    }

    /// Blocks the calling thread to retrieve the aquired read lock of a LocalLockArray within a non async context
    ///
    /// Returns an RAII guard which will drop the read access of the wrlock when dropped
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic).block();
    /// let handle = array.read_lock();
    /// let guard = handle.block();
    ///```
    pub fn block(self) -> LocalLockReadGuard<T> {
        RuntimeWarning::BlockingCall(
            "LocalLockReadHandle::block",
            "<handle>.spawn() or<handle>.await",
        )
        .print();
        self.array.lock.darc.team().scheduler.block_on(self)
    }

    /// This method will spawn the associated active message to capture the lock on the work queue,
    /// initiating the operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic).block();
    /// let handle = array.read_lock();
    /// let task = handle.spawn(); // initiate getting the read lock
    /// // do other work
    /// let guard = task.block();
    ///```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(self) -> LamellarTask<LocalLockReadGuard<T>> {
        self.array.lock.darc.team().spawn(self)
    }
}

impl<T: Dist> Future for LocalLockReadHandle<T> {
    type Output = LocalLockReadGuard<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.lock_handle.poll(cx) {
            Poll::Ready(val) => Poll::Ready(LocalLockReadGuard {
                array: this.array.clone(),
                lock_guard: Arc::new(val),
            }),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[must_use = "LocalLockArray lock handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)] //unused drop warning triggered by LocalRwDarcReadHandle
/// Handle used to retrieve the aquired local data [LocalLockLocalData] of  a LocalLockArray
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
/// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic).block();
/// let handle = array.read_local_data();
/// world.spawn(async move {
///     let  local_data = handle.await;
///     println!("PE{my_pe}, local_data: {:?}", local_data);
/// });
/// let mut local_data = array.read_local_data().block();
/// println!("PE{my_pe}, local_data: {:?}", local_data);
///```
pub struct LocalLockLocalDataHandle<T: Dist> {
    pub(crate) array: LocalLockArray<T>,
    pub(crate) start_index: usize,
    pub(crate) end_index: usize,
    #[pin]
    pub(crate) lock_handle: LocalRwDarcReadHandle<()>,
}

#[pinned_drop]
impl<T: Dist> PinnedDrop for LocalLockLocalDataHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.lock_handle.launched {
            RuntimeWarning::DroppedHandle("a LocalLockLocalDataHandle").print();
        }
    }
}

impl<T: Dist> LocalLockLocalDataHandle<T> {
    /// Blocks the calling thread to retrieve the aquired local data [LocalLockLocalData] of a LocalLockArray within a non async context
    ///
    /// Returns an RAII guard which will drop the write access of the wrlock when dropped
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic).block();
    /// let handle = array.read_local_data();
    /// let  local_data = handle.block();
    /// println!("local data: {:?}",local_data);
    ///```
    pub fn block(self) -> LocalLockLocalData<T> {
        RuntimeWarning::BlockingCall(
            "LocalLockLocalDataHandle::block",
            "<handle>.spawn() or<handle>.await",
        )
        .print();
        self.array.lock.darc.team().scheduler.block_on(self)
    }

    /// This method will spawn the associated active message to capture the lock and data on the work queue,
    /// initiating the operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic).block();
    /// let handle = array.read_local_data();
    /// let task = handle.spawn(); // initiate getting the read lock
    /// // do other work
    /// let  local_data = task.block();
    /// println!("local data: {:?}",local_data);
    ///```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(self) -> LamellarTask<LocalLockLocalData<T>> {
        self.array.lock.darc.team().spawn(self)
    }
}

impl<T: Dist> Future for LocalLockLocalDataHandle<T> {
    type Output = LocalLockLocalData<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.lock_handle.poll(cx) {
            Poll::Ready(val) => Poll::Ready(LocalLockLocalData {
                array: this.array.clone(),
                start_index: *this.start_index,
                end_index: *this.end_index,
                // lock: self.lock.clone(),
                lock_guard: Arc::new(val),
            }),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[must_use = "LocalLockArray lock handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)] //unused drop warning triggered by LocalRwDarcWriteHandle
/// Handle used to retrieve the aquired write lock of a LocalLockArray
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
/// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic).block();
/// let handle = array.write_lock();
/// let task = world.spawn(async move {
///     let write_lock = handle.await;
///     //do interesting work
/// });
/// array.write_lock().block();
/// task.block();
///```
pub struct LocalLockWriteHandle<T> {
    pub(crate) array: LocalLockArray<T>,
    #[pin]
    pub(crate) lock_handle: LocalRwDarcWriteHandle<()>,
}

#[pinned_drop]
impl<T> PinnedDrop for LocalLockWriteHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.lock_handle.launched {
            RuntimeWarning::DroppedHandle("a LocalRwDarcWriteHandle").print();
        }
    }
}

impl<T: Dist> LocalLockWriteHandle<T> {
    pub(crate) fn new(array: LocalLockArray<T>) -> Self {
        Self {
            array: array.clone(),
            lock_handle: array.lock.write(),
        }
    }
    /// Blocks the calling thread to retrieve the aquired write lock of a LocalLockArray within a non async context
    ///
    /// Returns an RAII guard which will drop the write access of the wrlock when dropped
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic).block();
    /// let handle = array.write_lock();
    /// handle.block();
    ///```
    pub fn block(self) -> LocalLockWriteGuard<T> {
        RuntimeWarning::BlockingCall(
            "LocalLockWriteHandle::block",
            "<handle>.spawn() or<handle>.await",
        )
        .print();

        self.array.lock.darc.team().scheduler.block_on(self)
    }

    /// This method will spawn the associated active message to capture the lock on the work queue,
    /// initiating the operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic).block();
    /// let handle = array.write_lock();
    /// let task = handle.spawn(); // initiate getting the write lock
    /// //do other work
    /// task.block();
    ///```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(self) -> LamellarTask<LocalLockWriteGuard<T>> {
        self.array.lock.darc.team().spawn(self)
    }
}

impl<T: Dist> Future for LocalLockWriteHandle<T> {
    type Output = LocalLockWriteGuard<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.lock_handle.poll(cx) {
            Poll::Ready(val) => Poll::Ready(LocalLockWriteGuard {
                array: this.array.clone(),
                lock_guard: val,
            }),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[must_use = "LocalLockArray lock handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)] // unused drop warning triggered by LocalRwDarcWriteHandle
/// Handle used to retrieve the aquired mutable local data [LocalLockMutLocalData] of  a LocalLockArray
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
/// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic).block();
/// let handle = array.write_local_data();
/// world.spawn(async move {
///     let mut local_data = handle.await;
///     local_data.iter_mut().for_each(|elem| *elem += my_pe);
/// });
/// let mut local_data = array.write_local_data().block();
/// local_data.iter_mut().for_each(|elem| *elem += my_pe);
///```
pub struct LocalLockMutLocalDataHandle<T: Dist> {
    pub(crate) array: LocalLockArray<T>,
    pub(crate) start_index: usize,
    pub(crate) end_index: usize,
    #[pin]
    pub(crate) lock_handle: LocalRwDarcWriteHandle<()>,
}

#[pinned_drop]
impl<T: Dist> PinnedDrop for LocalLockMutLocalDataHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.lock_handle.launched {
            RuntimeWarning::DroppedHandle("a LocalLockMutLocalDataHandle").print();
        }
    }
}

impl<T: Dist> LocalLockMutLocalDataHandle<T> {
    /// Blocks the calling thread to retrieve the aquired mutable local data [LocalLockMutLocalData] of a LocalLockArray within a non async context
    ///
    /// Returns an RAII guard which will drop the write access of the wrlock when dropped
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic).block();
    /// let handle = array.write_local_data();
    /// let mut local_data = handle.block();
    /// local_data.iter_mut().for_each(|elem| *elem += my_pe);
    ///```
    pub fn block(self) -> LocalLockMutLocalData<T> {
        RuntimeWarning::BlockingCall(
            "LocalLockMutLocalDataHandle::block",
            "<handle>.spawn() or<handle>.await",
        )
        .print();

        self.array.lock.darc.team().scheduler.block_on(self)
    }

    /// This method will spawn the associated active message to capture the lock and data on the work queue,
    /// initiating the operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic).block();
    /// let handle = array.write_local_data();
    /// let task = handle.spawn(); // initiate getting the write lock
    /// //do other work
    /// let mut local_data = task.block();
    /// local_data.iter_mut().for_each(|elem| *elem += my_pe);
    ///```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(self) -> LamellarTask<LocalLockMutLocalData<T>> {
        self.array.lock.darc.team().spawn(self)
    }
}

impl<T: Dist> Future for LocalLockMutLocalDataHandle<T> {
    type Output = LocalLockMutLocalData<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.lock_handle.poll(cx) {
            Poll::Ready(val) => Poll::Ready(LocalLockMutLocalData {
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

#[must_use = "LocalLockArray lock handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)] //unused drop warning triggered by LocalRwDarcReadHandle
/// Constructs a handle for immutably iterating over fixed sized chunks(slices) of the local data of this array.
/// This handle must be either await'd in an async context or block'd in an non-async context.
/// Awaiting or blocking will not return until the read lock has been acquired.
///
/// the returned iterator is a lamellar [LocalIterator] and also captures a read lock on the local data.
///
/// # Examples
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array: LocalLockArray<usize> = LocalLockArray::new(&world,40,Distribution::Block).block();
/// let my_pe = world.my_pe();
/// //block in a non-async context
/// let _ = array.read_local_chunks(5).block().enumerate().for_each(move|(i,chunk)| {
///     println!("PE: {my_pe} i: {i} chunk: {chunk:?}");
/// }).block();
///
/// //await in an async context
/// world.block_on(async move {
///     let _ = array.read_local_chunks(5).await.enumerate().for_each(move|(i,chunk)| {
///         println!("PE: {my_pe} i: {i} chunk: {chunk:?}");
///     }).await;
/// });
///
/// ```
pub struct LocalLockLocalChunksHandle<T> {
    pub(crate) chunk_size: usize,
    pub(crate) index: usize,     //global index within the array local data
    pub(crate) end_index: usize, //global index within the array local data
    pub(crate) array: LocalLockArray<T>,
    #[pin]
    pub(crate) lock_handle: LocalRwDarcReadHandle<()>,
}

#[pinned_drop]
impl<T> PinnedDrop for LocalLockLocalChunksHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.lock_handle.launched {
            RuntimeWarning::DroppedHandle("a LocalLockLocalChunksHandle").print();
        }
    }
}

impl<T: Dist> LocalLockLocalChunksHandle<T> {
    /// Blocks the calling thread to retrieve the aquired immutable local chunks iterator of a LocalLockArray within a non async context
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,40,Distribution::Block).block();
    /// let my_pe = world.my_pe();
    /// //block in a non-async context
    /// let _ = array.read_local_chunks(5).block().enumerate().for_each(move|(i,chunk)| {
    ///     println!("PE: {my_pe} i: {i} chunk: {chunk:?}");
    /// }).block();
    ///```
    pub fn block(self) -> LocalLockLocalChunks<T> {
        RuntimeWarning::BlockingCall(
            "LocalLockLocalChunksHandle::block",
            "<handle>.spawn() or<handle>.await",
        )
        .print();

        self.array.lock.darc.team().scheduler.block_on(self)
    }

    /// This method will spawn the associated active message to capture the lock and data on the work queue,
    /// initiating the operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,40,Distribution::Block).block();
    /// let my_pe = world.my_pe();
    /// //block in a non-async context
    /// let iter_task = array.read_local_chunks(5).block().enumerate().for_each(move|(i,chunk)| {
    ///     println!("PE: {my_pe} i: {i} chunk: {chunk:?}");
    /// }).spawn();//initiate the iteration
    /// // do other work
    /// iter_task.block();
    ///```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(self) -> LamellarTask<LocalLockLocalChunks<T>> {
        self.array.lock.darc.team().spawn(self)
    }
}

impl<T: Dist> Future for LocalLockLocalChunksHandle<T> {
    type Output = LocalLockLocalChunks<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.lock_handle.poll(cx) {
            Poll::Ready(val) => Poll::Ready(LocalLockLocalChunks {
                chunk_size: *this.chunk_size,
                index: *this.index, //global index within the array local data
                end_index: *this.end_index, //global index within the array local data
                array: this.array.clone(),
                lock_guard: Arc::new(val),
            }),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[must_use = "LocalLockArray lock handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)] // unused drop warning triggered by LocalRwDarcWriteHandle
/// A handle for mutably iterating over fixed sized chunks(slices) of the local data of this array.
/// This handle must be either await'd in an async context or block'd in an non-async context.
/// Awaiting or blocking will not return until the write lock has been acquired.
///
/// the returned iterator is a lamellar [LocalIterator] and also captures a write lock on the local data.
///
/// # Examples
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array: LocalLockArray<usize> = LocalLockArray::new(&world,40,Distribution::Block).block();
/// let my_pe = world.my_pe();
/// let _ = array.write_local_chunks(5).block().enumerate().for_each(move|(i, mut chunk)| {
///         for elem in chunk.iter_mut() {
///             *elem = i;
///         }
///     }).block();
/// world.block_on(async move {
///     let _ = array.write_local_chunks(5).await.enumerate().for_each(move|(i, mut chunk)| {
///         for elem in chunk.iter_mut() {
///             *elem = i;
///         }
///     }).await;
/// });
/// ```
pub struct LocalLockLocalChunksMutHandle<T> {
    pub(crate) chunk_size: usize,
    pub(crate) index: usize,     //global index within the array local data
    pub(crate) end_index: usize, //global index within the array local data
    pub(crate) array: LocalLockArray<T>,
    #[pin]
    pub(crate) lock_handle: LocalRwDarcWriteHandle<()>,
}

#[pinned_drop]
impl<T> PinnedDrop for LocalLockLocalChunksMutHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.lock_handle.launched {
            RuntimeWarning::DroppedHandle("a LocalLockLocalChunksMutHandle").print();
        }
    }
}

impl<T: Dist> LocalLockLocalChunksMutHandle<T> {
    /// Blocks the calling thread to retrieve the aquired mutable local chunks iterator of a LocalLockArray within a non async context
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,40,Distribution::Block).block();
    /// let my_pe = world.my_pe();
    /// //block in a non-async context
    /// let _ = array.write_local_chunks(5).block().enumerate().for_each(move|(i, mut chunk)| {
    ///         for elem in chunk.iter_mut() {
    ///             *elem = i;
    ///         }
    ///     }).block();
    ///```
    pub fn block(self) -> LocalLockLocalChunksMut<T> {
        RuntimeWarning::BlockingCall(
            "LocalLockLocalChunksMutHandle::block",
            "<handle>.spawn() or<handle>.await",
        )
        .print();

        self.array.lock.darc.team().scheduler.block_on(self)
    }

    /// This method will spawn the associated active message to capture the lock and data on the work queue,
    /// initiating the operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,40,Distribution::Block).block();
    /// let my_pe = world.my_pe();
    /// //block in a non-async context
    /// let iter_task = array.write_local_chunks(5).block().enumerate().for_each(move|(i, mut chunk)| {
    ///         for elem in chunk.iter_mut() {
    ///             *elem = i;
    ///         }
    ///     }).spawn(); // initiate the iteration
    /// // do other work
    /// iter_task.block();
    ///```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(self) -> LamellarTask<LocalLockLocalChunksMut<T>> {
        self.array.lock.darc.team().spawn(self)
    }
}

impl<T: Dist> Future for LocalLockLocalChunksMutHandle<T> {
    type Output = LocalLockLocalChunksMut<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.lock_handle.poll(cx) {
            Poll::Ready(val) => Poll::Ready(LocalLockLocalChunksMut {
                chunk_size: *this.chunk_size,
                index: *this.index, //global index within the array local data
                end_index: *this.end_index, //global index within the array local data
                array: this.array.clone(),
                lock_guard: Arc::new(val),
            }),
            Poll::Pending => Poll::Pending,
        }
    }
}

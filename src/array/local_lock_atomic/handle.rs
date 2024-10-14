use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::config;
use crate::darc::handle::{LocalRwDarcReadHandle, LocalRwDarcWriteHandle};
use crate::Dist;
use crate::LocalLockArray;

use futures_util::Future;
use pin_project::pin_project;

use super::{LocalLockLocalData, LocalLockMutLocalData, LocalLockReadGuard, LocalLockWriteGuard};

#[must_use]
#[pin_project]
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
/// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
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

impl<T: Dist> LocalLockReadHandle<T> {
    pub(crate) fn new(array: LocalLockArray<T>) -> Self {
        Self {
            array: array.clone(),
            lock_handle: array.lock.read(),
        }
    }

    /// Handle used to retrieve the aquired read lock of a LocalLockArray within a non async context
    ///
    /// Returns an RAII guard which will drop the read access of the wrlock when dropped
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    /// let handle = array.read_lock();
    /// let guard = handle.block();
    ///```
    pub fn block(self) -> LocalLockReadGuard<T> {
        let msg = format!("
                [LAMELLAR WARNING] You are calling `LocalLockReadHandle::block` from within an async context which may lead to deadlock, it is recommended that you use `.await;` instead!
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture()
            );
        match config().blocking_call_warning {
            Some(val) if val => println!("{msg}"),
            _ => println!("{msg}"),
        }

        self.array.lock.darc.team().scheduler.block_on(self)
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

#[must_use]
#[pin_project]
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
/// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
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

impl<T: Dist> LocalLockLocalDataHandle<T> {
    /// Handle used to retrieve the aquired local data [LocalLockLocalData] of a LocalLockArray within a non async context
    ///
    /// Returns an RAII guard which will drop the write access of the wrlock when dropped
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    /// let handle = array.read_local_data();
    /// let  local_data = handle.block();
    /// println!("local data: {:?}",local_data);
    ///```
    pub fn block(self) -> LocalLockLocalData<T> {
        let msg = format!("
                [LAMELLAR WARNING] You are calling `LocalLockLocalDataHandle::block` from within an async context which may lead to deadlock, it is recommended that you use `.await;` instead!
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture()
            );
        match config().blocking_call_warning {
            Some(val) if val => println!("{msg}"),
            _ => println!("{msg}"),
        }

        self.array.lock.darc.team().scheduler.block_on(self)
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

#[must_use]
#[pin_project]
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
/// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
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

impl<T: Dist> LocalLockWriteHandle<T> {
    pub(crate) fn new(array: LocalLockArray<T>) -> Self {
        Self {
            array: array.clone(),
            lock_handle: array.lock.write(),
        }
    }
    /// Handle used to retrieve the aquired write lock of a LocalLockArray within a non async context
    ///
    /// Returns an RAII guard which will drop the write access of the wrlock when dropped
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    /// let handle = array.write_lock();
    /// handle.block();
    ///```
    pub fn block(self) -> LocalLockWriteGuard<T> {
        let msg = format!("
                [LAMELLAR WARNING] You are calling `LocalLockWriteHandle::block` from within an async context which may lead to deadlock, it is recommended that you use `.await;` instead!
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture()
            );
        match config().blocking_call_warning {
            Some(val) if val => println!("{msg}"),
            _ => println!("{msg}"),
        }

        self.array.lock.darc.team().scheduler.block_on(self)
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

#[must_use]
#[pin_project]
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
/// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
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

impl<T: Dist> LocalLockMutLocalDataHandle<T> {
    /// Handle used to retrieve the aquired mutable local data [LocalLockMutLocalData] of a LocalLockArray within a non async context
    ///
    /// Returns an RAII guard which will drop the write access of the wrlock when dropped
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    /// let handle = array.write_local_data();
    /// let mut local_data = handle.block();
    /// local_data.iter_mut().for_each(|elem| *elem += my_pe);
    ///```
    pub fn block(self) -> LocalLockMutLocalData<T> {
        let msg = format!("
                [LAMELLAR WARNING] You are calling `LocalLockLocalDataHandle::block` from within an async context which may lead to deadlock, it is recommended that you use `.await;` instead!
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture()
            );
        match config().blocking_call_warning {
            Some(val) if val => println!("{msg}"),
            _ => println!("{msg}"),
        }

        self.array.lock.darc.team().scheduler.block_on(self)
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

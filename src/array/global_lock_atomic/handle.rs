use std::pin::Pin;
use std::task::{Context, Poll};

use crate::config;
use crate::darc::handle::{
    GlobalRwDarcCollectiveWriteHandle, GlobalRwDarcReadHandle, GlobalRwDarcWriteHandle,
};
use crate::Dist;
use crate::GlobalLockArray;

use futures_util::Future;
use pin_project::pin_project;

use super::{
    GlobalLockCollectiveMutLocalData, GlobalLockLocalData, GlobalLockMutLocalData,
    GlobalLockReadGuard, GlobalLockWriteGuard,
};

#[must_use]
#[pin_project]
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
/// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    /// let handle = array.read_lock();
    /// let guard = handle.block();
    ///```
    pub fn block(self) -> GlobalLockReadGuard<T> {
        let msg = format!("
                [LAMELLAR WARNING] You are calling `GlobalLockReadHandle::block` from within an async context which may lead to deadlock, it is recommended that you use `.await;` instead!
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture()
            );
        match config().blocking_call_warning {
            Some(val) if val => println!("{msg}"),
            _ => println!("{msg}"),
        }

        self.array.lock.darc.team().scheduler.block_on(self)
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

#[must_use]
#[pin_project]
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
/// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    /// let handle = array.read_local_data();
    /// let  local_data = handle.block();
    /// println!("local data: {:?}",local_data);
    ///```
    pub fn block(self) -> GlobalLockLocalData<T> {
        let msg = format!("
                [LAMELLAR WARNING] You are calling `GlobalLockLocalDataHandle::block` from within an async context which may lead to deadlock, it is recommended that you use `.await;` instead!
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture()
            );
        match config().blocking_call_warning {
            Some(val) if val => println!("{msg}"),
            _ => println!("{msg}"),
        }

        self.array.lock.darc.team().scheduler.block_on(self)
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

#[must_use]
#[pin_project]
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
/// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    /// let handle = array.write_lock();
    /// handle.block();
    ///```
    pub fn block(self) -> GlobalLockWriteGuard<T> {
        let msg = format!("
                [LAMELLAR WARNING] You are calling `GlobalLockWriteHandle::block` from within an async context which may lead to deadlock, it is recommended that you use `.await;` instead!
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture()
            );
        match config().blocking_call_warning {
            Some(val) if val => println!("{msg}"),
            _ => println!("{msg}"),
        }

        self.array.lock.darc.team().scheduler.block_on(self)
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

#[must_use]
#[pin_project]
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
/// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    /// let handle = array.write_local_data();
    /// let mut local_data = handle.block();
    /// local_data.iter_mut().for_each(|elem| *elem += my_pe);
    ///```
    pub fn block(self) -> GlobalLockMutLocalData<T> {
        let msg = format!("
                [LAMELLAR WARNING] You are calling `GlobalLockLocalDataHandle::block` from within an async context which may lead to deadlock, it is recommended that you use `.await;` instead!
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture()
            );
        match config().blocking_call_warning {
            Some(val) if val => println!("{msg}"),
            _ => println!("{msg}"),
        }

        self.array.lock.darc.team().scheduler.block_on(self)
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

#[must_use]
#[pin_project]
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
/// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    /// let handle = array.collective_write_local_data();
    /// let mut local_data = handle.block();
    /// local_data.iter_mut().for_each(|elem| *elem += my_pe);
    ///```
    pub fn block(self) -> GlobalLockCollectiveMutLocalData<T> {
        let msg = format!("
                [LAMELLAR WARNING] You are calling `GlobalLockCollectiveMutLocalDataHandle::block` from within an async context which may lead to deadlock, it is recommended that you use `.await;` instead!
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture()
            );
        match config().blocking_call_warning {
            Some(val) if val => println!("{msg}"),
            _ => println!("{msg}"),
        }

        self.array.lock.darc.team().scheduler.block_on(self)
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

use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::darc::local_rw_darc::{LocalRwDarc, LocalRwDarcReadGuard};
use crate::lamellar_request::LamellarRequest;
use crate::AmHandle;
use crate::{config, GlobalRwDarc};

use async_lock::{RwLockReadGuardArc, RwLockWriteGuardArc};
use futures_util::{ready, Future};
use pin_project::pin_project;

use super::global_rw_darc::{
    GlobalRwDarcCollectiveWriteGuard, GlobalRwDarcReadGuard, GlobalRwDarcWriteGuard,
};
use super::local_rw_darc::LocalRwDarcWriteGuard;

#[pin_project(project = StateProj)]
enum State<T> {
    Init,
    TryingRead(#[pin] Pin<Box<dyn Future<Output = RwLockReadGuardArc<T>> + Send + 'static>>),
    TryingWrite(#[pin] Pin<Box<dyn Future<Output = RwLockWriteGuardArc<T>> + Send + 'static>>),
}

#[must_use]
#[pin_project]
/// Handle used to retrieve the aquired read lock from a LocalRwDarc
///
/// This handle must be awaited or blocked on to acquire the lock
///
/// Once awaited/blocked the handle will not return while any writer currently has access to the lock, but there may be other readers
///
/// Returns an RAII guard which will drop the read access of the wrlock when dropped
/// # Examples
///
///```
/// use lamellar::darc::prelude::*;
/// use lamellar::active_messaging::*;
///
/// #[lamellar::AmData(Clone)]
/// struct DarcAm {
///     counter: LocalRwDarc<usize>, //each pe has a local atomicusize
/// }
///
/// #[lamellar::am]
/// impl LamellarAm for DarcAm {
///     async fn exec(self) {
///         let counter_handle = self.counter.read();
///         let counter = counter_handle.await; // await until we get the read lock
///         println!("the current counter value on pe {} = {}",lamellar::current_pe,*counter);
///     }
///  }
/// //-------------
///
/// let world = LamellarWorldBuilder::new().build();
/// let my_pe = world.my_pe();
/// let counter = LocalRwDarc::new(&world, 0).unwrap();
/// let _ = world.exec_am_all(DarcAm {counter: counter.clone()}).spawn();
/// let handle = counter.read();
/// let guard = handle.block(); //block until we get the read lock
/// println!("the current counter value on pe {} main thread = {}",my_pe,*guard);
/// drop(guard); //release the lock
/// world.wait_all(); // wait for my active message to return
/// world.barrier(); //at this point all updates will have been performed
///
///```
pub struct LocalRwDarcReadHandle<T: 'static> {
    darc: LocalRwDarc<T>,
    #[pin]
    state: State<T>,
}

impl<T: Sync + Send> LocalRwDarcReadHandle<T> {
    pub(crate) fn new(darc: LocalRwDarc<T>) -> Self {
        Self {
            darc,
            state: State::Init,
        }
    }
    /// Used to retrieve the aquired read lock from a LocalRwDarc within a non async context
    ///
    /// Returns an RAII guard which will drop the read access of the wrlock when dropped
    /// # Examples
    ///
    ///```
    /// use lamellar::darc::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let counter = LocalRwDarc::new(&world, 0).unwrap();
    /// let handle = counter.read();
    /// let guard = handle.block(); //block until we get the read lock
    /// println!("the current counter value on pe {} main thread = {}",my_pe,*guard);
    ///
    ///```
    pub fn block(self) -> LocalRwDarcReadGuard<T> {
        let msg = format!("
                [LAMELLAR WARNING] You are calling `LocalRwDarcReadHandle::block` from within an async context which may lead to deadlock, it is recommended that you use `.await;` instead!
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture()
            );
        match config().blocking_call_warning {
            Some(val) if val => println!("{msg}"),
            _ => println!("{msg}"),
        }

        let inner_darc = self.darc.darc.clone();

        let guard = self
            .darc
            .darc
            .team()
            .clone()
            .block_on(async move { inner_darc.read_arc().await });
        LocalRwDarcReadGuard {
            darc: self.darc,
            lock: guard,
        }
    }
}

impl<T: Sync + Send> Future for LocalRwDarcReadHandle<T> {
    type Output = LocalRwDarcReadGuard<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner_darc = self.darc.darc.clone();
        let mut this = self.project();
        match this.state.as_mut().project() {
            StateProj::Init => {
                let lock = Box::pin(async move { inner_darc.read_arc().await });
                *this.state = State::TryingRead(lock);
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            StateProj::TryingRead(lock) => {
                let guard = ready!(lock.poll(cx));
                Poll::Ready(LocalRwDarcReadGuard {
                    darc: this.darc.clone(),
                    lock: guard,
                })
            }
            _ => unreachable!(),
        }
    }
}

#[must_use]
#[pin_project]
/// Handle used to retrieve the aquired write lock from a LocalRwDarc
///
/// This handle must be awaited or blocked on to acquire the lock
///
/// Once awaited/blocked the handle will not return while any readers or writer currently has access to the lock
///
/// Returns an RAII guard which will drop the write access of the wrlock when dropped
/// # Examples
///
///```
/// use lamellar::darc::prelude::*;
/// use lamellar::active_messaging::*;
///
/// #[lamellar::AmData(Clone)]
/// struct DarcAm {
///     counter: LocalRwDarc<usize>, //each pe has a local atomicusize
/// }
///
/// #[lamellar::am]
/// impl LamellarAm for DarcAm {
///     async fn exec(self) {
///         let counter_handle = self.counter.write();
///         let mut counter = counter_handle.await; // await until we get the write lock
///         *counter += 1;
///     }
///  }
/// //-------------
///
/// let world = LamellarWorldBuilder::new().build();
/// let my_pe = world.my_pe();
/// let counter = LocalRwDarc::new(&world, 0).unwrap();
/// let _ = world.exec_am_all(DarcAm {counter: counter.clone()}).spawn();
/// let handle = counter.write();
/// let mut guard = handle.block(); //block until we get the write lock
/// *guard += my_pe;
/// drop(guard); //release the lock
/// world.wait_all(); // wait for my active message to return
/// world.barrier(); //at this point all updates will have been performed
///```
pub struct LocalRwDarcWriteHandle<T: 'static> {
    darc: LocalRwDarc<T>,
    #[pin]
    state: State<T>,
}

impl<T: Sync + Send> LocalRwDarcWriteHandle<T> {
    pub(crate) fn new(darc: LocalRwDarc<T>) -> Self {
        Self {
            darc,
            state: State::Init,
        }
    }
    /// used to retrieve the aquired write lock from a LocalRwDarc within a non async context
    ///
    /// Returns an RAII guard which will drop the write access of the wrlock when dropped
    /// # Examples
    ///
    ///```
    /// use lamellar::darc::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let counter = LocalRwDarc::new(&world, 0).unwrap();
    /// let handle = counter.write();
    /// let mut guard = handle.block(); //block until we get the write lock
    /// *guard += my_pe;
    ///```
    pub fn block(self) -> LocalRwDarcWriteGuard<T> {
        let msg = format!("
                [LAMELLAR WARNING] You are calling `LocalRwDarcWriteHandle::block` from within an async context which may lead to deadlock, it is recommended that you use `.await;` instead!
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture()
            );
        match config().blocking_call_warning {
            Some(val) if val => println!("{msg}"),
            _ => println!("{msg}"),
        }

        let inner_darc = self.darc.darc.clone();

        let guard = self
            .darc
            .darc
            .team()
            .clone()
            .block_on(async move { inner_darc.write_arc().await });
        LocalRwDarcWriteGuard {
            darc: self.darc,
            lock: guard,
        }
    }
}

impl<T: Sync + Send> Future for LocalRwDarcWriteHandle<T> {
    type Output = LocalRwDarcWriteGuard<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner_darc = self.darc.darc.clone();
        let mut this = self.project();
        match this.state.as_mut().project() {
            StateProj::Init => {
                let lock = Box::pin(async move { inner_darc.write_arc().await });
                *this.state = State::TryingWrite(lock);
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            StateProj::TryingWrite(lock) => {
                let guard = ready!(lock.poll(cx));
                Poll::Ready(LocalRwDarcWriteGuard {
                    darc: this.darc.clone(),
                    lock: guard,
                })
            }
            _ => unreachable!(),
        }
    }
}

#[must_use]
#[pin_project]
/// Handle used to retrieve the aquired read lock from a GlobalRwDarc
///
/// This handle must be awaited or blocked on to acquire the lock
///
/// Once awaited/blocked the handle will not return while any writer currently has access to the lock, but there may be other readers
///
/// Returns an RAII guard which will drop the read access of the wrlock when dropped
/// # Examples
///
///```
/// use lamellar::darc::prelude::*;
/// use lamellar::active_messaging::*;
///
/// #[lamellar::AmData(Clone)]
/// struct DarcAm {
///     counter: GlobalRwDarc<usize>, //each pe has a local atomicusize
/// }
///
/// #[lamellar::am]
/// impl LamellarAm for DarcAm {
///     async fn exec(self) {
///         let counter_handle = self.counter.read();
///         let counter = counter_handle.await; // await until we get the write lock
///         println!("the current counter value on pe {} = {}",lamellar::current_pe,*counter);
///     }
///  }
/// //-------------
///
/// let world = LamellarWorldBuilder::new().build();
/// let my_pe = world.my_pe();
/// let counter = GlobalRwDarc::new(&world, 0).unwrap();
/// let _ = world.exec_am_all(DarcAm {counter: counter.clone()}).spawn();
/// let handle = counter.read();
/// let guard = handle.block(); //block until we get the write lock
/// println!("the current counter value on pe {} main thread = {}",my_pe,*guard);
/// drop(guard); //release the lock
/// world.wait_all(); // wait for my active message to return
/// world.barrier(); //at this point all updates will have been performed
///
///```
pub struct GlobalRwDarcReadHandle<T: 'static> {
    pub(crate) darc: GlobalRwDarc<T>,
    #[pin]
    pub(crate) lock_am: AmHandle<()>,
}

impl<T: Sync + Send> GlobalRwDarcReadHandle<T> {
    /// Used to retrieve the aquired read lock from a GlobalRwDarc within a non async context
    ///
    /// Returns an RAII guard which will drop the read access of the wrlock when dropped
    /// # Examples
    ///
    ///```
    /// use lamellar::darc::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let counter = GlobalRwDarc::new(&world, 0).unwrap();
    /// let handle = counter.read();
    /// let guard = handle.block(); //block until we get the write lock
    /// println!("the current counter value on pe {} main thread = {}",my_pe,*guard);
    ///```
    pub fn block(self) -> GlobalRwDarcReadGuard<T> {
        let msg = format!("
                [LAMELLAR WARNING] You are calling `GlobalRwDarcReadHandle::block` from within an async context which may lead to deadlock, it is recommended that you use `.await;` instead!
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture()
            );
        match config().blocking_call_warning {
            Some(val) if val => println!("{msg}"),
            _ => println!("{msg}"),
        }

        let _ = self.lock_am.blocking_wait();
        GlobalRwDarcReadGuard {
            darc: self.darc.clone(),
            marker: PhantomData,
            local_cnt: Arc::new(AtomicUsize::new(1)),
        }
    }
}

impl<T: Sync + Send> Future for GlobalRwDarcReadHandle<T> {
    type Output = GlobalRwDarcReadGuard<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        ready!(this.lock_am.poll(cx));
        Poll::Ready(GlobalRwDarcReadGuard {
            darc: this.darc.clone(),
            marker: PhantomData,
            local_cnt: Arc::new(AtomicUsize::new(1)),
        })
    }
}

#[must_use]
#[pin_project]
/// Handle used to retrieve the aquired write lock from a GlobalRwDarc
///
/// This handle must be awaited or blocked on to acquire the lock
///
/// Once awaited/blocked the handle will not return while any readers orwriter currently has access to the lock
///
/// Returns an RAII guard which will drop the write access of the wrlock when dropped
/// # Examples
///
///```
/// use lamellar::darc::prelude::*;
/// use lamellar::active_messaging::*;
///
/// #[lamellar::AmData(Clone)]
/// struct DarcAm {
///     counter: GlobalRwDarc<usize>, //each pe has a local atomicusize
/// }
///
/// #[lamellar::am]
/// impl LamellarAm for DarcAm {
///     async fn exec(self) {
///         let counter_handle = self.counter.write();
///         let mut counter = counter_handle.await; // await until we get the write lock
///         *counter += 1;
///     }
///  }
/// //-------------
///
/// let world = LamellarWorldBuilder::new().build();
/// let my_pe = world.my_pe();
/// let counter = GlobalRwDarc::new(&world, 0).unwrap();
/// let _ = world.exec_am_all(DarcAm {counter: counter.clone()}).spawn();
/// let handle = counter.write();
/// let mut guard = handle.block(); //block until we get the write lock
/// *guard += my_pe;
/// drop(guard); //release the lock
/// world.wait_all(); // wait for my active message to return
/// world.barrier(); //at this point all updates will have been performed
///
///```
pub struct GlobalRwDarcWriteHandle<T: 'static> {
    pub(crate) darc: GlobalRwDarc<T>,
    #[pin]
    pub(crate) lock_am: AmHandle<()>,
}

impl<T: Sync + Send> GlobalRwDarcWriteHandle<T> {
    /// Used to retrieve the aquired write lock from a GlobalRwDarc within a non async context
    ///
    /// Returns an RAII guard which will drop the read access of the wrlock when dropped
    /// # Examples
    ///
    ///```
    /// use lamellar::darc::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let counter = GlobalRwDarc::new(&world, 0).unwrap();
    /// let handle = counter.write();
    /// let mut guard = handle.block(); //block until we get the write lock
    /// *guard += my_pe;
    ///```
    pub fn block(self) -> GlobalRwDarcWriteGuard<T> {
        let msg = format!("
                [LAMELLAR WARNING] You are calling `GlobalRwDarcWriteHandle::block` from within an async context which may lead to deadlock, it is recommended that you use `.await;` instead!
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture()
            );
        match config().blocking_call_warning {
            Some(val) if val => println!("{msg}"),
            _ => println!("{msg}"),
        }

        let _ = self.lock_am.blocking_wait();
        GlobalRwDarcWriteGuard {
            darc: self.darc.clone(),
            marker: PhantomData,
        }
    }
}

impl<T: Sync + Send> Future for GlobalRwDarcWriteHandle<T> {
    type Output = GlobalRwDarcWriteGuard<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        ready!(this.lock_am.poll(cx));
        Poll::Ready(GlobalRwDarcWriteGuard {
            darc: this.darc.clone(),
            marker: PhantomData,
        })
    }
}

#[must_use]
#[pin_project]
/// Handle used to retrieve the aquired collective write lock from a GlobalRwDarc
///
/// This handle must be awaited or blocked on to actually acquire the lock
///
/// Once awaited/blocked the handle will not return while any readers or non collective writer currently has access to the lock.
/// Further the handle will not return until all PEs have acquired the lock
///
/// Returns an RAII guard which will drop the collective write access of the wrlock when dropped
/// # Examples
///
///```
/// use lamellar::darc::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let my_pe = world.my_pe();
///
/// let counter = GlobalRwDarc::new(&world, 0).unwrap();
/// let handle = counter.collective_write();
/// let mut guard = handle.block(); // this will block until all PEs have acquired the lock
/// *guard += my_pe;
///```
pub struct GlobalRwDarcCollectiveWriteHandle<T: 'static> {
    pub(crate) darc: GlobalRwDarc<T>,
    pub(crate) collective_cnt: usize,
    #[pin]
    pub(crate) lock_am: AmHandle<()>,
}

impl<T: Sync + Send> GlobalRwDarcCollectiveWriteHandle<T> {
    /// Used to retrieve the aquired collective write lock from a GlobalRwDarc within a non async context
    ///
    /// Returns an RAII guard which will drop the read access of the wrlock when dropped
    /// # Examples
    ///
    ///```
    /// use lamellar::darc::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let counter = GlobalRwDarc::new(&world, 0).unwrap();
    /// let handle = counter.collective_write();
    /// let mut guard = handle.block(); //block until we get the write lock
    /// *guard += my_pe;
    pub fn block(self) -> GlobalRwDarcCollectiveWriteGuard<T> {
        let msg = format!("
                [LAMELLAR WARNING] You are calling `GlobalRwDarcCollectiveWriteHandle::block` from within an async context which may lead to deadlock, it is recommended that you use `.await;` instead!
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture()
            );
        match config().blocking_call_warning {
            Some(val) if val => println!("{msg}"),
            _ => println!("{msg}"),
        }

        let _ = self.lock_am.blocking_wait();
        GlobalRwDarcCollectiveWriteGuard {
            darc: self.darc.clone(),
            collective_cnt: self.collective_cnt,
            marker: PhantomData,
        }
    }
}

impl<T: Sync + Send> Future for GlobalRwDarcCollectiveWriteHandle<T> {
    type Output = GlobalRwDarcCollectiveWriteGuard<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        ready!(this.lock_am.poll(cx));
        Poll::Ready(GlobalRwDarcCollectiveWriteGuard {
            darc: this.darc.clone(),
            collective_cnt: *this.collective_cnt,
            marker: PhantomData,
        })
    }
}

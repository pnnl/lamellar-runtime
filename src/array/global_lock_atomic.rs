mod iteration;
pub(crate) mod operations;
mod rdma;
use crate::array::private::ArrayExecAm;
use crate::array::private::LamellarArrayPrivate;
use crate::array::r#unsafe::{UnsafeByteArray, UnsafeByteArrayWeak};
use crate::array::*;
use crate::barrier::BarrierHandle;
use crate::config;
use crate::darc::global_rw_darc::{
    GlobalRwDarc, GlobalRwDarcCollectiveWriteGuard, GlobalRwDarcReadGuard, GlobalRwDarcWriteGuard,
};
use crate::darc::DarcMode;
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::Dist;
use crate::scheduler::LamellarTask;

use pin_project::pin_project;

use std::ops::{Deref, DerefMut};
use std::task::{Context, Poll, Waker};

/// A safe abstraction of a distributed array, providing read/write access protected by locks.
///
/// This array type protects access to its data by mainitaining a `RwLock` on each PE that contains data.
///
/// Whenever a thread wants access to the arrays data it must first grab the either a read lock or the write lock (depending on the access type), before being able to proceed.
///
/// An important characteristic of the array type is that each PE manages access to its own data and has no knowledge of the state of the other PEs.
/// This means that while there can only ever be a single 'writer' at a time on each PE, there may exist multiple 'writers' at any given time globally.
///
/// Generally any operation on this array type will be performed via an internal runtime Active Message.
/// Direct RDMA operations can occur if the appropriate lock is held.
#[lamellar_impl::AmDataRT(Clone, Debug)]
pub struct GlobalLockArray<T> {
    pub(crate) lock: GlobalRwDarc<()>,
    pub(crate) array: UnsafeArray<T>,
}

#[doc(hidden)]
#[lamellar_impl::AmDataRT(Clone, Debug)]
pub struct GlobalLockByteArray {
    lock: GlobalRwDarc<()>,
    pub(crate) array: UnsafeByteArray,
}

impl GlobalLockByteArray {
    pub fn downgrade(array: &GlobalLockByteArray) -> GlobalLockByteArrayWeak {
        GlobalLockByteArrayWeak {
            lock: array.lock.clone(),
            array: UnsafeByteArray::downgrade(&array.array),
        }
    }
}

#[doc(hidden)]
#[lamellar_impl::AmLocalDataRT(Clone, Debug)]
pub struct GlobalLockByteArrayWeak {
    lock: GlobalRwDarc<()>,
    pub(crate) array: UnsafeByteArrayWeak,
}

impl GlobalLockByteArrayWeak {
    pub fn upgrade(&self) -> Option<GlobalLockByteArray> {
        Some(GlobalLockByteArray {
            lock: self.lock.clone(),
            array: self.array.upgrade()?,
        })
    }
}

/// Provides mutable access to a PEs local data to provide "local" indexing while maintaining safety guarantees of the array type.
///
/// This derefences down to a `&mut [T]`.
///
/// This struct is a Write Lock guard, meaning that during its lifetime, an instance will have exclusive write access to the underlying local data on the PE
/// (allowing for the safe deref into `&mut [T]`), preventing any other local or remote access.
///
/// When the instance is dropped the lock is released.
#[derive(Debug)]
pub struct GlobalLockMutLocalData<T: Dist> {
    pub(crate) array: GlobalLockArray<T>,
    start_index: usize,
    end_index: usize,
    lock_guard: GlobalRwDarcWriteGuard<()>,
}

// impl<T: Dist> Drop for GlobalLockMutLocalData<T>{
//     fn drop(&mut self){
//         println!("release lock! {:?} {:?}",std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
//     }
// }

impl<T: Dist> Deref for GlobalLockMutLocalData<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        unsafe { &self.array.array.local_as_mut_slice()[self.start_index..self.end_index] }
    }
}
impl<T: Dist> DerefMut for GlobalLockMutLocalData<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut self.array.array.local_as_mut_slice()[self.start_index..self.end_index] }
    }
}

/// Provides collective mutable access to each PEs local data to provide "local" indexing while maintaining safety guarantees of the array type.
///
/// This derefences down to a `&mut [T]`.
///
/// This struct is a Collective Write Lock guard, meaning that during its lifetime, each PE will have exclusive write access to the underlying local data on the PE
/// (allowing for the safe deref into `&mut [T]`), preventing any other local or remote access.
///
/// When each PE drops its instance, the lock is release.
#[derive(Debug)]
pub struct GlobalLockCollectiveMutLocalData<T: Dist> {
    pub(crate) array: GlobalLockArray<T>,
    start_index: usize,
    end_index: usize,
    _lock_guard: GlobalRwDarcCollectiveWriteGuard<()>,
}

// impl<T: Dist> Drop for GlobalLockCollectiveMutLocalData<T>{
//     fn drop(&mut self){
//         println!("release lock! {:?} {:?}",std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
//     }
// }

impl<T: Dist> Deref for GlobalLockCollectiveMutLocalData<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        unsafe { &self.array.array.local_as_mut_slice()[self.start_index..self.end_index] }
    }
}
impl<T: Dist> DerefMut for GlobalLockCollectiveMutLocalData<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut self.array.array.local_as_mut_slice()[self.start_index..self.end_index] }
    }
}

/// Provides immutable access to a PEs local data to provide "local" indexing while maintaining safety guarantees of the array type.
///
/// This derefences down to a `&[T]`.
///
/// This struct is a Read Lock guard, meaning that during its lifetime, an instance will have shared read access to the underlying local data on the PE
/// (allowing for the safe deref into `&[T]`), preventing any local or remote write access.
///
/// When the instance is dropped the lock is released.
pub struct GlobalLockLocalData<T: Dist> {
    pub(crate) array: GlobalLockArray<T>,
    // lock: GlobalRwDarc<()>,
    start_index: usize,
    end_index: usize,
    lock_guard: GlobalRwDarcReadGuard<()>,
}

impl<T: Dist + std::fmt::Debug> std::fmt::Debug for GlobalLockLocalData<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.deref())
    }
}

impl<T: Dist> Clone for GlobalLockLocalData<T> {
    fn clone(&self) -> Self {
        GlobalLockLocalData {
            array: self.array.clone(),
            start_index: self.start_index,
            end_index: self.end_index,
            // lock: self.lock.clone(),
            lock_guard: self.lock_guard.clone(),
        }
    }
}

impl<T: Dist> Deref for GlobalLockLocalData<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        unsafe { &self.array.array.local_as_slice()[self.start_index..self.end_index] }
    }
}

// impl<T: Dist> Drop for GlobalLockLocalData<T> {
//     fn drop(&mut self) {
//         println!(
//             "release GlobalLockLocalData lock! {:?} ",
//             std::thread::current().id(),
//         );
//     }
// }

impl<T: Dist> GlobalLockLocalData<T> {
    /// Convert into a smaller sub range of the local data, the original read lock is transfered to the new sub data to mainitain safety guarantees
    ///
    /// # Examples
    /// Assume 4 PEs
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.blocking_read_local_data();
    /// let sub_data = local_data.clone().into_sub_data(10,20); // clone() essentially increases the references to the read lock by 1.
    /// assert_eq!(local_data[10],sub_data[0]);
    ///```
    pub fn into_sub_data(self, start: usize, end: usize) -> GlobalLockLocalData<T> {
        GlobalLockLocalData {
            array: self.array.clone(),
            start_index: start,
            end_index: end,
            // lock: self.lock,
            lock_guard: self.lock_guard,
        }
    }
}

impl<T: Dist + serde::Serialize> serde::Serialize for GlobalLockLocalData<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        unsafe { &self.array.array.local_as_mut_slice()[self.start_index..self.end_index] }
            .serialize(serializer)
    }
}

pub struct GlobalLockLocalDataIter<'a, T: Dist> {
    data: &'a [T],
    index: usize,
}

impl<'a, T: Dist> Iterator for GlobalLockLocalDataIter<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.data.len() {
            self.index += 1;
            Some(&self.data[self.index - 1])
        } else {
            None
        }
    }
}

impl<'a, T: Dist> IntoIterator for &'a GlobalLockLocalData<T> {
    type Item = &'a T;
    type IntoIter = GlobalLockLocalDataIter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        GlobalLockLocalDataIter {
            data: unsafe {
                &self.array.array.local_as_mut_slice()[self.start_index..self.end_index]
            },
            index: 0,
        }
    }
}

/// Captures a read lock on the array, allowing immutable access to the underlying data
#[derive(Clone)]
pub struct GlobalLockReadGuard<T: Dist> {
    pub(crate) array: GlobalLockArray<T>,
    lock_guard: GlobalRwDarcReadGuard<()>,
}

impl<T: Dist> GlobalLockReadGuard<T> {
    /// Access the underlying local data through the read lock
    pub fn local_data(&self) -> GlobalLockLocalData<T> {
        GlobalLockLocalData {
            array: self.array.clone(),
            start_index: 0,
            end_index: self.array.num_elems_local(),
            // lock: self.lock.clone(),
            lock_guard: self.lock_guard.clone(),
        }
    }
}

/// Captures a write lock on the array, allowing mutable access to the underlying data
pub struct GlobalLockWriteGuard<T: Dist> {
    pub(crate) array: GlobalLockArray<T>,
    lock_guard: GlobalRwDarcWriteGuard<()>,
}

impl<T: Dist> From<GlobalLockMutLocalData<T>> for GlobalLockWriteGuard<T> {
    fn from(data: GlobalLockMutLocalData<T>) -> Self {
        GlobalLockWriteGuard {
            array: data.array,
            lock_guard: data.lock_guard,
        }
    }
}

impl<T: Dist> GlobalLockWriteGuard<T> {
    /// Access the underlying local data through the write lock
    pub fn local_data(self) -> GlobalLockMutLocalData<T> {
        GlobalLockMutLocalData {
            array: self.array.clone(),
            start_index: 0,
            end_index: self.array.num_elems_local(),
            lock_guard: self.lock_guard,
        }
    }
}

impl<T: Dist + ArrayOps + std::default::Default> GlobalLockArray<T> {
    #[doc(alias = "Collective")]
    /// Construct a new GlobalLockArray with a length of `array_size` whose data will be layed out with the provided `distribution` on the PE's specified by the `team`.
    /// `team` is commonly a [LamellarWorld][crate::LamellarWorld] or [LamellarTeam][crate::LamellarTeam] (instance or reference).
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `team` to enter the constructor call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    pub fn new<U: Clone + Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> GlobalLockArray<T> {
        let array = UnsafeArray::new(team.clone(), array_size, distribution);
        let lock = GlobalRwDarc::new(team, ()).unwrap();

        GlobalLockArray {
            lock: lock,
            array: array,
        }
    }
}

impl<T: Dist> GlobalLockArray<T> {
    #[doc(alias("One-sided", "onesided"))]
    /// Change the distribution this array handle uses to index into the data of the array.
    ///
    /// # One-sided Operation
    /// This is a one-sided call and does not redistribute or modify the actual data, it simply changes how the array is indexed for this particular handle.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    /// // do something interesting... or not
    /// let block_view = array.clone().use_distribution(Distribution::Block);
    ///```
    pub fn use_distribution(self, distribution: Distribution) -> Self {
        GlobalLockArray {
            lock: self.lock.clone(),
            array: self.array.use_distribution(distribution),
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return a global read lock guard on the calling PE
    ///
    /// this function will block the thread until the lock is acquired
    /// Calling within an asynchronous block may lead to deadlock, use [read_lock](self::GlobalLockArray::read_lock) instead.
    ///
    /// # One-sided Operation
    /// Only explictly requires the calling PE, although the global lock may be managed by other PEs
    ///
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let read_lock = array.blocking_read_lock();
    /// //do interesting work
    ///
    ///```
    pub fn blocking_read_lock(&self) -> GlobalLockReadGuard<T> {
        if std::thread::current().id() != *crate::MAIN_THREAD {
            if let Some(val) = config().blocking_call_warning {
                if val {
                    println!("[LAMELLAR WARNING] You are calling `GlobalLockArray::blocking_read_lock` from within an async context which may lead to deadlock, it is recommended that you use `read_lock().await;` instead! 
                    Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture());
                }
            } else {
                println!("[LAMELLAR WARNING] You are calling `GlobalLockArray::blocking_read_lock` from within an async context which may lead to deadlock, it is recommended that you use `read_lock().await;` instead! 
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture());
            }
        }
        let self_clone: GlobalLockArray<T> = self.clone();
        self.block_on(async move {
            GlobalLockReadGuard {
                array: self_clone.clone(),
                lock_guard: self_clone.lock.read().await,
            }
        })
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return a global read lock guard on the calling PE
    ///
    /// this function will block the calling task until the lock is acquired (but not the calling thread)
    ///
    /// # One-sided Operation
    /// Only explictly requires the calling PE, although the global lock may be managed by other PEs
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// world.block_on(async move {
    ///     let read_lock = array.read_lock().await;
    ///     //do interesting work
    /// });
    ///```
    pub async fn read_lock(&self) -> GlobalLockReadGuard<T> {
        GlobalLockReadGuard {
            array: self.clone(),
            lock_guard: self.lock.read().await,
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return a global write lock guard on the calling PE
    ///
    /// this function will block the thread until the lock is acquired
    /// Calling within an asynchronous block may lead to deadlock, use [write_lock](self::GlobalLockArray::write_lock) instead.
    ///
    /// # One-sided Operation
    /// Only explictly requires the calling PE, although the global lock may be managed by other PEs
    ///
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let write_lock = array.blocking_write_lock();
    /// //do interesting work
    ///
    ///```
    pub fn blocking_write_lock(&self) -> GlobalLockWriteGuard<T> {
        if std::thread::current().id() != *crate::MAIN_THREAD {
            if let Some(val) = config().blocking_call_warning {
                if val {
                    println!("[LAMELLAR WARNING] You are calling `GlobalLockArray::blocking_write_lock` from within an async context which may lead to deadlock, it is recommended that you use `write_lock().await;` instead! 
                    Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture());
                }
            } else {
                println!("[LAMELLAR WARNING] You are calling `GlobalLockArray::blocking_write_lock` from within an async context which may lead to deadlock, it is recommended that you use `write_lock().await;` instead! 
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture());
            }
        }
        let self_clone: GlobalLockArray<T> = self.clone();
        self.block_on(async move {
            GlobalLockWriteGuard {
                array: self_clone.clone(),
                lock_guard: self_clone.lock.write().await,
            }
        })
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return a global write lock guard on the calling PE
    ///
    /// this function will block the calling task until the lock is acquired (but not the calling thread)
    ///
    /// # One-sided Operation
    /// Only explictly requires the calling PE, although the global lock may be managed by other PEs
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// world.block_on(async move {
    ///     let write_lock = array.write_lock().await;
    ///     //do interesting work
    /// });
    ///```
    pub async fn write_lock(&self) -> GlobalLockWriteGuard<T> {
        GlobalLockWriteGuard {
            array: self.clone(),
            lock_guard: self.lock.write().await,
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the calling PE's local data as a [GlobalLockLocalData], which allows safe immutable access to local elements.
    ///
    /// Calling this function will result in a local read lock being captured on the array
    ///
    /// This function is blocking and intended to be called from non asynchronous contexts.
    /// Calling within an asynchronous block may lead to deadlock.
    ///
    /// # One-sided Operation
    /// Only returns local data on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.blocking_read_local_data();
    /// println!("PE{my_pe} data: {local_data:?}");
    ///```
    pub fn blocking_read_local_data(&self) -> GlobalLockLocalData<T> {
        if std::thread::current().id() != *crate::MAIN_THREAD {
            if let Some(val) = config().blocking_call_warning {
                if val {
                    println!("[LAMELLAR WARNING] You are calling `GlobalLockArray::blocking_read_local_data` from within an async context which may lead to deadlock, it is recommended that you use `read_local_data().await;` instead! 
                    Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture());
                }
            } else {
                println!("[LAMELLAR WARNING] You are calling `GlobalLockArray::blocking_read_local_data` from within an async context which may lead to deadlock, it is recommended that you use `read_local_data().await;` instead! 
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture());
            }
        }
        let self_clone: GlobalLockArray<T> = self.clone();
        self.block_on(async move {
            GlobalLockLocalData {
                array: self_clone.clone(),
                start_index: 0,
                end_index: self_clone.array.num_elems_local(),
                // lock: self_clone.lock.clone(),
                lock_guard: self_clone.lock.read().await,
            }
        })
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the calling PE's local data as a [GlobalLockLocalData], which allows safe immutable access to local elements.   
    ///
    /// Calling this function will result in a local read lock being captured on the array
    ///
    /// This version is intended to be used within asynchronous contexts to prevent blocking the worker threads
    ///
    /// # One-sided Operation
    /// Only returns local data on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// world.clone().block_on(async move {
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.read_local_data().await;
    /// println!("PE{my_pe} data: {local_data:?}");
    /// });
    ///```
    pub async fn read_local_data(&self) -> GlobalLockLocalData<T> {
        GlobalLockLocalData {
            array: self.clone(),
            start_index: 0,
            end_index: self.array.num_elems_local(),
            // lock: self.lock.clone(),
            lock_guard: self.lock.read().await,
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the calling PE's local data as a [GlobalLockMutLocalData], which allows safe mutable access to local elements.
    ///
    /// Calling this function will result in the global write lock being captured on the array.
    ///.
    /// This function is blocking and intended to be called from non asynchronous contexts.
    /// Calling within an asynchronous block may lead to deadlock.
    ///
    /// # One-sided Operation
    /// Only returns (mutable) local data on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.blocking_write_local_data();
    /// println!("PE{my_pe} data: {local_data:?}");
    ///```
    pub fn blocking_write_local_data(&self) -> GlobalLockMutLocalData<T> {
        if std::thread::current().id() != *crate::MAIN_THREAD {
            if let Some(val) = config().blocking_call_warning {
                if val {
                    println!("[LAMELLAR WARNING] You are calling `GlobalLockArray::blocking_write_local_data` from within an async context which may lead to deadlock, it is recommended that you use `write_local_data().await;` instead! 
                    Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture());
                }
            } else {
                println!("[LAMELLAR WARNING] You are calling `GlobalLockArray::blocking_write_local_data` from within an async context which may lead to deadlock, it is recommended that you use `write_local_data().await;` instead! 
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture());
            }
        }
        let self_clone: GlobalLockArray<T> = self.clone();
        self.block_on(async move {
            let lock = self_clone.lock.write().await;
            let data = GlobalLockMutLocalData {
                array: self_clone.clone(),
                start_index: 0,
                end_index: self_clone.array.num_elems_local(),
                lock_guard: lock,
            };
            // println!("got lock! {:?} {:?}",std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
            data
        })
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the calling PE's local data as a [GlobalLockMutLocalData], which allows safe mutable access to local elements.   
    ///
    /// Calling this function will result in the global write lock being captured on the array
    ///
    /// This version is intended to be used within asynchronous contexts to prevent blocking the worker threads
    ///
    /// # One-sided Operation
    /// Only returns (mutable) local data on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// world.clone().block_on(async move {
    ///     let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    ///     let local_data = array.write_local_data().await;
    ///     println!("PE{my_pe} data: {local_data:?}");
    /// });
    ///```
    pub async fn write_local_data(&self) -> GlobalLockMutLocalData<T> {
        let lock = self.lock.write().await;
        let data = GlobalLockMutLocalData {
            array: self.clone(),
            start_index: 0,
            end_index: self.array.num_elems_local(),
            lock_guard: lock,
        };
        // println!("got lock! {:?} {:?}",std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
        data
    }

    /// Return the calling PE's local data as a [GlobalLockMutLocalData], which allows safe mutable access to local elements.   
    ///
    /// Calling this function will result in the collective write lock being captured on the array
    ///
    /// # Collective Operation
    /// All PEs associated with this array must enter the call, otherwise deadlock will occur.
    /// Upon return every PE will hold a special collective write lock so that they can all access their local data simultaneous
    /// This lock prevents any other access from occuring on the array until it is dropped on all the PEs.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.blocking_collective_write_local_data();
    /// println!("PE{my_pe} data: {local_data:?}");
    ///```
    pub fn blocking_collective_write_local_data(&self) -> GlobalLockCollectiveMutLocalData<T> {
        if std::thread::current().id() != *crate::MAIN_THREAD {
            if let Some(val) = config().blocking_call_warning {
                if val {
                    println!("[LAMELLAR WARNING] You are calling `GlobalLockArray::blocking_collective_write_local_data` from within an async context which may lead to deadlock, it is recommended that you use `collective_write_local_data().await;` instead! 
                    Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture());
                }
            } else {
                println!("[LAMELLAR WARNING] You are calling `GlobalLockArray::blocking_collective_write_local_data` from within an async context which may lead to deadlock, it is recommended that you use `collective_write_local_data().await;` instead! 
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture());
            }
        }
        let self_clone: GlobalLockArray<T> = self.clone();
        self.block_on(async move {
            let lock = self_clone.lock.collective_write().await;
            let data = GlobalLockCollectiveMutLocalData {
                array: self_clone.clone(),
                start_index: 0,
                end_index: self_clone.array.num_elems_local(),
                _lock_guard: lock,
            };
            // println!("got lock! {:?} {:?}",std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
            data
        })
    }

    #[doc(alias("Collective"))]
    /// Return the calling PE's local data as a [GlobalLockMutLocalData], which allows safe mutable access to local elements.   
    ///
    /// Calling this function will result in the collective write lock being captured on the array
    ///
    /// # Collective Operation
    /// All PEs associated with this array must enter the call, otherwise deadlock will occur.
    /// Upon return every PE will hold a special collective write lock so that they can all access their local data simultaneous
    /// This lock prevents any other access from occuring on the array until it is dropped on all the PEs.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// world.clone().block_on(async move {
    ///    let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    ///    let local_data = array.collective_write_local_data().await;
    ///    println!("PE{my_pe} data: {local_data:?}");
    /// });
    ///```
    pub async fn collective_write_local_data(&self) -> GlobalLockCollectiveMutLocalData<T> {
        let lock = self.lock.collective_write().await;
        let data = GlobalLockCollectiveMutLocalData {
            array: self.clone(),
            start_index: 0,
            end_index: self.array.num_elems_local(),
            _lock_guard: lock,
        };
        // println!("got lock! {:?} {:?}",std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
        data
    }

    #[doc(hidden)]
    pub unsafe fn __local_as_slice(&self) -> &[T] {
        self.array.local_as_mut_slice()
    }

    #[doc(alias = "Collective")]
    /// Convert this GlobalLockArray into an [UnsafeArray][crate::array::UnsafeArray]
    ///
    /// This is a collective and blocking function which will only return when there is at most a single reference on each PE
    /// to this Array, and that reference is currently calling this function.
    ///
    /// When it returns, it is gauranteed that there are only  `UnsafeArray` handles to the underlying data
    ///
    /// Note, that while this call itself is safe, and `UnsafeArray` unsurprisingly is not safe and thus you need to tread very carefully
    /// doing any operations with the resulting array.
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `array` to enter the call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let unsafe_array = array.into_unsafe();
    ///```
    ///
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let array1 = array.clone();
    /// let slice = array1.blocking_read_local_data();
    ///
    /// // no borrows to this specific instance (array) so it can enter the "into_unsafe" call
    /// // but array1 will not be dropped until after 'slice' is dropped.
    /// // Given the ordering of these calls we will get stuck in "into_unsafe" as it
    /// // waits for the reference count to go down to "1" (but we will never be able to drop slice/array1).
    /// let unsafe_array = array.into_unsafe();
    /// unsafe_array.print();
    /// println!("{slice:?}");
    pub fn into_unsafe(self) -> UnsafeArray<T> {
        // println!("GlobalLock into_unsafe");
        self.array.into()
    }

    // pub fn into_local_only(self) -> LocalOnlyArray<T> {
    //     // println!("GlobalLock into_local_only");
    //     self.array.into()
    // }

    #[doc(alias = "Collective")]
    /// Convert this GlobalLockArray into a (safe) [ReadOnlyArray][crate::array::ReadOnlyArray]
    ///
    /// This is a collective and blocking function which will only return when there is at most a single reference on each PE
    /// to this Array, and that reference is currently calling this function.
    ///
    /// When it returns, it is gauranteed that there are only `ReadOnlyArray` handles to the underlying data
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `array` to enter the call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let read_only_array = array.into_read_only();
    ///```
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let array1 = array.clone();
    /// let slice = array1.blocking_read_local_data();
    ///
    /// // no borrows to this specific instance (array) so it can enter the "into_read_only" call
    /// // but array1 will not be dropped until after mut_slice is dropped.
    /// // Given the ordering of these calls we will get stuck in "into_read_only" as it
    /// // waits for the reference count to go down to "1" (but we will never be able to drop slice/array1).
    /// let read_only_array = array.into_read_only();
    /// read_only_array.print();
    /// println!("{slice:?}");
    ///```
    pub fn into_read_only(self) -> ReadOnlyArray<T> {
        // println!("GlobalLock into_read_only");
        self.array.into()
    }

    #[doc(alias = "Collective")]
    /// Convert this GlobalLockArray into a (safe) [ReadOnlyArray][crate::array::ReadOnlyArray]
    ///
    /// This is a collective and blocking function which will only return when there is at most a single reference on each PE
    /// to this Array, and that reference is currently calling this function.
    ///
    /// When it returns, it is gauranteed that there are only `ReadOnlyArray` handles to the underlying data
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `array` to enter the call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let read_only_array = array.into_read_only();
    ///```
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let array1 = array.clone();
    /// let slice = array1.blocking_read_local_data();
    ///
    /// // no borrows to this specific instance (array) so it can enter the "into_read_only" call
    /// // but array1 will not be dropped until after mut_slice is dropped.
    /// // Given the ordering of these calls we will get stuck in "into_read_only" as it
    /// // waits for the reference count to go down to "1" (but we will never be able to drop slice/array1).
    /// let read_only_array = array.into_read_only();
    /// read_only_array.print();
    /// println!("{slice:?}");
    ///```
    pub fn into_local_lock(self) -> GlobalLockArray<T> {
        // println!("GlobalLock into_read_only");
        self.array.into()
    }
}

impl<T: Dist + 'static> GlobalLockArray<T> {
    #[doc(alias = "Collective")]
    /// Convert this GlobalLockArray into a (safe) [AtomicArray][crate::array::AtomicArray]
    ///
    /// This is a collective and blocking function which will only return when there is at most a single reference on each PE
    /// to this Array, and that reference is currently calling this function.
    ///
    /// When it returns, it is gauranteed that there are only `AtomicArray` handles to the underlying data
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `array` to enter the call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let atomic_array = array.into_atomic();
    ///```
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let array1 = array.clone();
    /// let slice = array1.blocking_read_local_data();
    ///
    /// // no borrows to this specific instance (array) so it can enter the "into_atomic" call
    /// // but array1 will not be dropped until after mut_slice is dropped.
    /// // Given the ordering of these calls we will get stuck in "into_atomic" as it
    /// // waits for the reference count to go down to "1" (but we will never be able to drop slice/array1).
    /// let atomic_array = array.into_atomic();
    /// atomic_array.print();
    /// println!("{slice:?}");
    ///```
    pub fn into_atomic(self) -> AtomicArray<T> {
        // println!("GlobalLock into_atomic");
        self.array.into()
    }
}

impl<T: Dist + ArrayOps> TeamFrom<(Vec<T>, Distribution)> for GlobalLockArray<T> {
    fn team_from(input: (Vec<T>, Distribution), team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let (vals, distribution) = input;
        let input = (&vals, distribution);
        let array: UnsafeArray<T> = TeamInto::team_into(input, team);
        array.into()
    }
}

// #[async_trait]
impl<T: Dist + ArrayOps> AsyncTeamFrom<(Vec<T>, Distribution)> for GlobalLockArray<T> {
    async fn team_from(input: (Vec<T>, Distribution), team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let array: UnsafeArray<T> = AsyncTeamInto::team_into(input, team).await;
        array.async_into().await
    }
}

impl<T: Dist> From<UnsafeArray<T>> for GlobalLockArray<T> {
    fn from(array: UnsafeArray<T>) -> Self {
        // println!("GlobalLock from unsafe");
        array.block_on_outstanding(DarcMode::GlobalLockArray);
        let lock = GlobalRwDarc::new(array.team_rt(), ()).unwrap();

        GlobalLockArray {
            lock: lock,
            array: array,
        }
    }
}

#[async_trait]
impl<T: Dist> AsyncFrom<UnsafeArray<T>> for GlobalLockArray<T> {
    async fn async_from(array: UnsafeArray<T>) -> Self {
        // println!("GlobalLock from unsafe");
        array.await_on_outstanding(DarcMode::GlobalLockArray).await;
        let lock = GlobalRwDarc::new(array.team_rt(), ()).unwrap();

        GlobalLockArray {
            lock: lock,
            array: array,
        }
    }
}

// impl<T: Dist> From<LocalOnlyArray<T>> for GlobalLockArray<T> {
//     fn from(array: LocalOnlyArray<T>) -> Self {
//         // println!("GlobalLock from localonly");
//         unsafe { array.into_inner().into() }
//     }
// }

impl<T: Dist> From<AtomicArray<T>> for GlobalLockArray<T> {
    fn from(array: AtomicArray<T>) -> Self {
        // println!("GlobalLock from atomic");
        unsafe { array.into_inner().into() }
    }
}

impl<T: Dist> From<ReadOnlyArray<T>> for GlobalLockArray<T> {
    fn from(array: ReadOnlyArray<T>) -> Self {
        // println!("GlobalLock from readonly");
        unsafe { array.into_inner().into() }
    }
}

impl<T: Dist> From<LocalLockArray<T>> for GlobalLockArray<T> {
    fn from(array: LocalLockArray<T>) -> Self {
        // println!("GlobalLock from LocalLockArray");
        unsafe { array.into_inner().into() }
    }
}

impl<T: Dist> From<GlobalLockArray<T>> for GlobalLockByteArray {
    fn from(array: GlobalLockArray<T>) -> Self {
        GlobalLockByteArray {
            lock: array.lock.clone(),
            array: array.array.into(),
        }
    }
}
impl<T: Dist> From<GlobalLockArray<T>> for LamellarByteArray {
    fn from(array: GlobalLockArray<T>) -> Self {
        LamellarByteArray::GlobalLockArray(GlobalLockByteArray {
            lock: array.lock.clone(),
            array: array.array.into(),
        })
    }
}

impl<T: Dist> From<LamellarByteArray> for GlobalLockArray<T> {
    fn from(array: LamellarByteArray) -> Self {
        if let LamellarByteArray::GlobalLockArray(array) = array {
            array.into()
        } else {
            panic!("Expected LamellarByteArray::GlobalLockArray")
        }
    }
}

impl<T: Dist> From<GlobalLockByteArray> for GlobalLockArray<T> {
    fn from(array: GlobalLockByteArray) -> Self {
        GlobalLockArray {
            lock: array.lock.clone(),
            array: array.array.into(),
        }
    }
}

impl<T: Dist> private::ArrayExecAm<T> for GlobalLockArray<T> {
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team_rt().clone()
    }
    fn team_counters(&self) -> Arc<AMCounters> {
        self.array.team_counters()
    }
}

impl<T: Dist> private::LamellarArrayPrivate<T> for GlobalLockArray<T> {
    fn inner_array(&self) -> &UnsafeArray<T> {
        &self.array
    }
    fn local_as_ptr(&self) -> *const T {
        self.array.local_as_mut_ptr()
    }
    fn local_as_mut_ptr(&self) -> *mut T {
        self.array.local_as_mut_ptr()
    }
    fn pe_for_dist_index(&self, index: usize) -> Option<usize> {
        self.array.pe_for_dist_index(index)
    }
    fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> Option<usize> {
        self.array.pe_offset_for_dist_index(pe, index)
    }
    unsafe fn into_inner(self) -> UnsafeArray<T> {
        self.array
    }
    fn as_lamellar_byte_array(&self) -> LamellarByteArray {
        self.clone().into()
    }
}

impl<T: Dist> ActiveMessaging for GlobalLockArray<T> {
    type SinglePeAmHandle<R: AmDist> = AmHandle<R>;
    type MultiAmHandle<R: AmDist> = MultiAmHandle<R>;
    type LocalAmHandle<L> = LocalAmHandle<L>;
    fn exec_am_all<F>(&self, am: F) -> Self::MultiAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        self.array.exec_am_all_tg(am)
    }
    fn exec_am_pe<F>(&self, pe: usize, am: F) -> Self::SinglePeAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        self.array.exec_am_pe_tg(pe, am)
    }
    fn exec_am_local<F>(&self, am: F) -> Self::LocalAmHandle<F::Output>
    where
        F: LamellarActiveMessage + LocalAM + 'static,
    {
        self.array.exec_am_local_tg(am)
    }
    fn wait_all(&self) {
        self.array.wait_all()
    }
    fn await_all(&self) -> impl Future<Output = ()> + Send {
        self.array.await_all()
    }
    fn barrier(&self) {
        self.array.barrier()
    }
    fn async_barrier(&self) -> BarrierHandle {
        self.array.async_barrier()
    }
    fn spawn<F: Future>(&self, f: F) -> LamellarTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        self.array.spawn(f)
    }
    fn block_on<F: Future>(&self, f: F) -> F::Output {
        self.array.block_on(f)
    }
    fn block_on_all<I>(&self, iter: I) -> Vec<<<I as IntoIterator>::Item as Future>::Output>
    where
        I: IntoIterator,
        <I as IntoIterator>::Item: Future + Send + 'static,
        <<I as IntoIterator>::Item as Future>::Output: Send,
    {
        self.array.block_on_all(iter)
    }
}

impl<T: Dist> LamellarArray<T> for GlobalLockArray<T> {
    fn team_rt(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team_rt().clone()
    }
    // fn my_pe(&self) -> usize {
    //     LamellarArray::my_pe(&self.array)
    // }
    // fn num_pes(&self) -> usize {
    //     LamellarArray::num_pes(&self.array)
    // }
    fn len(&self) -> usize {
        self.array.len()
    }
    fn num_elems_local(&self) -> usize {
        self.array.num_elems_local()
    }
    // fn barrier(&self) {
    //     self.array.barrier();
    // }
    // fn wait_all(&self) {
    //     self.array.wait_all()
    //     // println!("done in wait all {:?}",std::time::SystemTime::now());
    // }
    // fn block_on<F: Future>(&self, f: F) -> F::Output {
    //     self.array.block_on(f)
    // }
    fn pe_and_offset_for_global_index(&self, index: usize) -> Option<(usize, usize)> {
        self.array.pe_and_offset_for_global_index(index)
    }
    fn first_global_index_for_pe(&self, pe: usize) -> Option<usize> {
        self.array.first_global_index_for_pe(pe)
    }

    fn last_global_index_for_pe(&self, pe: usize) -> Option<usize> {
        self.array.last_global_index_for_pe(pe)
    }
}

impl<T: Dist> LamellarEnv for GlobalLockArray<T> {
    fn my_pe(&self) -> usize {
        LamellarEnv::my_pe(&self.array)
    }

    fn num_pes(&self) -> usize {
        LamellarEnv::num_pes(&self.array)
    }
    fn num_threads_per_pe(&self) -> usize {
        self.array.team_rt().num_threads()
    }
    fn world(&self) -> Arc<LamellarTeam> {
        self.array.team_rt().world()
    }
    fn team(&self) -> Arc<LamellarTeam> {
        self.array.team_rt().team()
    }
}

impl<T: Dist> LamellarWrite for GlobalLockArray<T> {}
impl<T: Dist> LamellarRead for GlobalLockArray<T> {}

impl<T: Dist> SubArray<T> for GlobalLockArray<T> {
    type Array = GlobalLockArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array {
        GlobalLockArray {
            lock: self.lock.clone(),
            array: self.array.sub_array(range),
        }
    }
    fn global_index(&self, sub_index: usize) -> usize {
        self.array.global_index(sub_index)
    }
}

impl<T: Dist + std::fmt::Debug> GlobalLockArray<T> {
    #[doc(alias = "Collective")]
    /// Print the data within a lamellar array
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the array to enter the print call otherwise deadlock will occur (i.e. barriers are being called internally)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let block_array = GlobalLockArray::<usize>::new(&world,100,Distribution::Block);
    /// let cyclic_array = GlobalLockArray::<usize>::new(&world,100,Distribution::Block);
    ///
    /// block_array.print();
    /// println!();
    /// cyclic_array.print();
    ///```
    pub fn print(&self) {
        self.barrier();
        let _guard = self.read_local_data();
        self.array.print();
    }
}

impl<T: Dist + std::fmt::Debug> ArrayPrint<T> for GlobalLockArray<T> {
    fn print(&self) {
        self.barrier();
        let _guard = self.read_local_data();
        self.array.print()
    }
}

//#[doc(hidden)]
#[pin_project]
pub struct GlobalLockArrayReduceHandle<T: Dist + AmDist> {
    req: AmHandle<Option<T>>,
    lock_guard: GlobalLockReadGuard<T>,
}

impl<T: Dist + AmDist> GlobalLockArrayReduceHandle<T> {
    /// This method will spawn the associated Array Reduce Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion and retrieve the result. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<Option<T>> {
        self.lock_guard.array.clone().spawn(self)
    }

    /// This method will block the caller until the associated Array Reduce Operation completes
    pub fn block(self) -> Option<T> {
        self.lock_guard.array.clone().block_on(self)
    }
}

impl<T: Dist + AmDist> LamellarRequest for GlobalLockArrayReduceHandle<T> {
    fn blocking_wait(self) -> Self::Output {
        self.req.blocking_wait()
    }
    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        self.req.ready_or_set_waker(waker)
    }
    fn val(&self) -> Self::Output {
        self.req.val()
    }
}

impl<T: Dist + AmDist> Future for GlobalLockArrayReduceHandle<T> {
    type Output = Option<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.req.ready_or_set_waker(cx.waker()) {
            true => Poll::Ready(this.req.val()),
            false => Poll::Pending,
        }
    }
}

impl<T: Dist + AmDist + 'static> GlobalLockReadGuard<T> {
    #[doc(alias("One-sided", "onesided"))]
    /// Perform a reduction on the entire distributed array, returning the value to the calling PE.
    ///
    /// Please see the documentation for the [register_reduction] procedural macro for
    /// more details and examples on how to create your own reductions.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Reduce` active messages on the other PEs associated with the array.
    /// the returned reduction result is only available on the calling PE  
    ///
    /// # Safety
    /// the global read lock ensures atomicity of the entire array, i.e. individual elements can not being modified before the call completes
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][GlobalLockArrayReduceHandle::spawn] or [blocked on][GlobalLockArrayReduceHandle::block]
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// use rand::Rng;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = GlobalLockArray::<usize>::new(&world,10,Distribution::Block);
    /// array.block_on(array.dist_iter_mut().enumerate().for_each(move |(i,elem)| *elem = i*2));
    /// let read_guard = array.blocking_read_lock();
    /// let prod = array.block_on(read_guard.reduce("prod")).expect("array has > 0 elements");
    ///```
    #[must_use = "this function is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it "]
    pub fn reduce(self, op: &str) -> GlobalLockArrayReduceHandle<T> {
        GlobalLockArrayReduceHandle {
            req: self.array.array.reduce_data(op, self.array.clone().into()),
            lock_guard: self,
        }
    }
}
impl<T: Dist + AmDist + ElementArithmeticOps + 'static> GlobalLockReadGuard<T> {
    #[doc(alias("One-sided", "onesided"))]
    /// Perform a sum reduction on the entire distributed array, returning the value to the calling PE.
    ///
    /// This equivalent to `reduce("sum")`.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Sum` active messages on the other PEs associated with the array.
    /// the returned sum reduction result is only available on the calling PE
    ///
    /// # Safety
    /// the global read lock ensures atomicity of the entire array, i.e. individual elements can not being modified before the call completes
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][GlobalLockArrayReduceHandle::spawn] or [blocked on][GlobalLockArrayReduceHandle::block]
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// use rand::Rng;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = GlobalLockArray::<usize>::new(&world,10,Distribution::Block);
    /// array.block_on(array.dist_iter_mut().enumerate().for_each(move |(i,elem)| *elem = i*2));
    /// let read_guard = array.blocking_read_lock();
    /// let sum = array.block_on(read_guard.sum()).expect("array has > 0 elements");
    /// ```
    #[must_use = "this function is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it "]
    pub fn sum(self) -> GlobalLockArrayReduceHandle<T> {
        self.reduce("sum")
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Perform a production reduction on the entire distributed array, returning the value to the calling PE.
    ///
    /// This equivalent to `reduce("prod")`.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Prod` active messages on the other PEs associated with the array.
    /// the returned prod reduction result is only available on the calling PE
    ///
    /// # Safety
    /// the global read lock ensures atomicity of the entire array, i.e. individual elements can not being modified before the call completes
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][GlobalLockArrayReduceHandle::spawn] or [blocked on][GlobalLockArrayReduceHandle::block]
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = GlobalLockArray::<usize>::new(&world,10,Distribution::Block);
    /// array.block_on(array.dist_iter_mut().enumerate().for_each(move |(i,elem)| *elem = i+1));
    /// let read_guard = array.blocking_read_lock();
    /// let prod = array.block_on(read_guard.prod()).expect("array has > 0 elements");
    /// assert_eq!((1..=array.len()).product::<usize>(),prod);
    ///```
    #[must_use = "this function is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it "]
    pub fn prod(self) -> GlobalLockArrayReduceHandle<T> {
        self.reduce("prod")
    }
}
impl<T: Dist + AmDist + ElementComparePartialEqOps + 'static> GlobalLockReadGuard<T> {
    #[doc(alias("One-sided", "onesided"))]
    /// Find the max element in the entire destributed array, returning to the calling PE
    ///
    /// This equivalent to `reduce("max")`.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Max` active messages on the other PEs associated with the array.
    /// the returned max reduction result is only available on the calling PE
    ///
    /// # Safety
    /// the global read lock ensures atomicity of the entire array, i.e. individual elements can not being modified before the call completes
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][GlobalLockArrayReduceHandle::spawn] or [blocked on][GlobalLockArrayReduceHandle::block]
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = GlobalLockArray::<usize>::new(&world,10,Distribution::Block);
    /// array.block_on(array.dist_iter_mut().enumerate().for_each(move |(i,elem)| *elem = i*2));
    /// let read_guard = array.blocking_read_lock();
    /// let max = array.block_on(read_guard.max()).expect("array has > 0 elements");
    /// assert_eq!((array.len()-1)*2,max);
    ///```
    #[must_use = "this function is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it "]
    pub fn max(self) -> GlobalLockArrayReduceHandle<T> {
        self.reduce("max")
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Find the min element in the entire destributed array, returning to the calling PE
    ///
    /// This equivalent to `reduce("min")`.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Min` active messages on the other PEs associated with the array.
    /// the returned min reduction result is only available on the calling PE
    ///
    /// # Safety
    /// the global read lock ensures atomicity of the entire array, i.e. individual elements can not being modified before the call completes
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][GlobalLockArrayReduceHandle::spawn] or [blocked on][GlobalLockArrayReduceHandle::block]
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = GlobalLockArray::<usize>::new(&world,10,Distribution::Block);
    /// array.block_on(array.dist_iter_mut().enumerate().for_each(move |(i,elem)| *elem = i*2));
    /// let read_guard = array.blocking_read_lock();
    /// let min = array.block_on(read_guard.min()).expect("array has > 0 elements");
    /// assert_eq!(0,min);
    ///```
    #[must_use = "this function is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it "]
    pub fn min(self) -> GlobalLockArrayReduceHandle<T> {
        self.reduce("min")
    }
}

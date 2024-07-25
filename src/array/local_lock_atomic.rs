mod iteration;
pub(crate) mod local_chunks;
pub use local_chunks::{LocalLockLocalChunks, LocalLockLocalChunksMut};
pub(crate) mod operations;
mod rdma;
use crate::array::private::ArrayExecAm;
use crate::array::private::LamellarArrayPrivate;
use crate::array::r#unsafe::{UnsafeByteArray, UnsafeByteArrayWeak};
use crate::array::*;
use crate::barrier::BarrierHandle;
use crate::config;
use crate::darc::local_rw_darc::LocalRwDarc;
use crate::darc::DarcMode;
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::Dist;
use crate::scheduler::LamellarTask;

// use parking_lot::{
//     lock_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard},
//     RawRwLock,
// };
use async_lock::{RwLockReadGuardArc, RwLockWriteGuardArc};
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
pub struct LocalLockArray<T> {
    lock: LocalRwDarc<()>,
    pub(crate) array: UnsafeArray<T>,
}

#[doc(hidden)]
#[lamellar_impl::AmDataRT(Clone, Debug)]
pub struct LocalLockByteArray {
    lock: LocalRwDarc<()>,
    pub(crate) array: UnsafeByteArray,
}

impl LocalLockByteArray {
    pub fn downgrade(array: &LocalLockByteArray) -> LocalLockByteArrayWeak {
        LocalLockByteArrayWeak {
            lock: array.lock.clone(),
            array: UnsafeByteArray::downgrade(&array.array),
        }
    }
}

#[doc(hidden)]
#[lamellar_impl::AmLocalDataRT(Clone, Debug)]
pub struct LocalLockByteArrayWeak {
    lock: LocalRwDarc<()>,
    pub(crate) array: UnsafeByteArrayWeak,
}

impl LocalLockByteArrayWeak {
    pub fn upgrade(&self) -> Option<LocalLockByteArray> {
        Some(LocalLockByteArray {
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
pub struct LocalLockMutLocalData<T: Dist> {
    array: LocalLockArray<T>,
    start_index: usize,
    end_index: usize,
    lock_guard: RwLockWriteGuardArc<()>,
}

// impl<T: Dist> Drop for LocalLockMutLocalData<T> {
//     fn drop(&mut self) {
//         // println!("release lock! {:?} {:?}",std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
//     }
// }

impl<T: Dist> Deref for LocalLockMutLocalData<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        unsafe { &self.array.array.local_as_mut_slice()[self.start_index..self.end_index] }
    }
}
impl<T: Dist> DerefMut for LocalLockMutLocalData<T> {
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
#[derive(Debug)]
pub struct LocalLockLocalData<T: Dist> {
    pub(crate) array: LocalLockArray<T>,
    // lock: LocalRwDarc<()>,
    start_index: usize,
    end_index: usize,
    lock_guard: Arc<RwLockReadGuardArc<()>>,
}

impl<'a, T: Dist> Clone for LocalLockLocalData<T> {
    fn clone(&self) -> Self {
        // println!("getting read lock in LocalLockLocalData clone");
        LocalLockLocalData {
            array: self.array.clone(),
            start_index: self.start_index,
            end_index: self.end_index,
            // lock: self.lock.clone(),
            lock_guard: self.lock_guard.clone(),
        }
    }
}

// impl<'a, T: Dist> Drop for LocalLockLocalData<T> {
//     fn drop(&mut self) {
//         println!(
//             "dropping read lock {:?}",
//             std::sync::Arc::strong_count(&self.lock_guard)
//         );
//     }
// }

// impl<'a, T: Dist> Drop for LocalLockMutLocalData<T> {
//     fn drop(&mut self) {
//         println!("dropping write lock");
//     }
// }

impl<'a, T: Dist> LocalLockLocalData<T> {
    /// Convert into a smaller sub range of the local data, the original read lock is transfered to the new sub data to mainitain safety guarantees
    ///
    /// # Examples
    /// Assume 4 PEs
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.read_local_data();
    /// let sub_data = local_data.clone().into_sub_data(10,20); // clone() essentially increases the references to the read lock by 1.
    /// assert_eq!(local_data[10],sub_data[0]);
    ///```
    pub fn into_sub_data(self, start: usize, end: usize) -> LocalLockLocalData<T> {
        // println!("into sub data {:?} {:?}", start, end);
        LocalLockLocalData {
            array: self.array.clone(),
            start_index: start,
            end_index: end,
            // lock: self.lock.clone(),
            lock_guard: self.lock_guard.clone(),
        }
    }
}

impl<'a, T: Dist + serde::Serialize> serde::Serialize for LocalLockLocalData<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        unsafe { &self.array.array.local_as_mut_slice()[self.start_index..self.end_index] }
            .serialize(serializer)
    }
}

pub struct LocalLockLocalDataIter<'a, T: Dist> {
    data: &'a [T],
    index: usize,
}

impl<'a, T: Dist> Iterator for LocalLockLocalDataIter<'a, T> {
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

impl<'a, T: Dist> IntoIterator for &'a LocalLockLocalData<T> {
    type Item = &'a T;
    type IntoIter = LocalLockLocalDataIter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        LocalLockLocalDataIter {
            data: unsafe {
                &self.array.array.local_as_mut_slice()[self.start_index..self.end_index]
            },
            index: 0,
        }
    }
}

impl<T: Dist> Deref for LocalLockLocalData<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        unsafe { &self.array.array.local_as_mut_slice()[self.start_index..self.end_index] }
    }
}

/// Captures a read lock on the array, allowing immutable access to the underlying data
#[derive(Clone)]
pub struct LocalLockReadGuard<T: Dist> {
    pub(crate) array: LocalLockArray<T>,
    lock_guard: Arc<RwLockReadGuardArc<()>>,
}

impl<T: Dist> LocalLockReadGuard<T> {
    /// Access the underlying local data immutably through the read lock
    pub fn local_data(&self) -> LocalLockLocalData<T> {
        LocalLockLocalData {
            array: self.array.clone(),
            start_index: 0,
            end_index: self.array.num_elems_local(),
            // lock: self.lock.clone(),
            lock_guard: self.lock_guard.clone(),
        }
    }
}

/// Captures a write lock on the array, allowing mutable access to the underlying data
pub struct LocalLockWriteGuard<T: Dist> {
    pub(crate) array: LocalLockArray<T>,
    lock_guard: RwLockWriteGuardArc<()>,
}

impl<T: Dist> From<LocalLockMutLocalData<T>> for LocalLockWriteGuard<T> {
    fn from(data: LocalLockMutLocalData<T>) -> Self {
        LocalLockWriteGuard {
            array: data.array,
            lock_guard: data.lock_guard,
        }
    }
}

impl<T: Dist> LocalLockWriteGuard<T> {
    /// Access the underlying local data mutably through the write lock
    pub fn local_data(self) -> LocalLockMutLocalData<T> {
        LocalLockMutLocalData {
            array: self.array.clone(),
            start_index: 0,
            end_index: self.array.num_elems_local(),
            lock_guard: self.lock_guard,
        }
    }
}

impl<T: Dist + ArrayOps + std::default::Default> LocalLockArray<T> {
    #[doc(alias = "Collective")]
    /// Construct a new LocalLockArray with a length of `array_size` whose data will be layed out with the provided `distribution` on the PE's specified by the `team`.
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
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    pub fn new<U: Clone + Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> LocalLockArray<T> {
        let array = UnsafeArray::new(team.clone(), array_size, distribution);
        array.block_on_outstanding(DarcMode::LocalLockArray);
        let lock = LocalRwDarc::new(team, ()).unwrap();

        LocalLockArray {
            lock: lock,
            array: array,
        }
    }
}

impl<T: Dist> LocalLockArray<T> {
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
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    /// // do something interesting... or not
    /// let block_view = array.clone().use_distribution(Distribution::Block);
    ///```
    pub fn use_distribution(self, distribution: Distribution) -> Self {
        LocalLockArray {
            lock: self.lock.clone(),
            array: self.array.use_distribution(distribution),
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the calling PE's local read lock guard
    ///
    /// this function will block the thread until the lock is acquired
    /// Calling within an asynchronous block may lead to deadlock, use [read_lock](self::LocalLockArray::read_lock) instead.
    ///
    /// # One-sided Operation
    /// Only explictly requires the calling PE
    ///
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array:LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let read_lock = array.blocking_read_lock();
    /// //do interesting work
    ///
    pub fn blocking_read_lock(&self) -> LocalLockReadGuard<T> {
        if std::thread::current().id() != *crate::MAIN_THREAD {
            if let Some(val) = config().blocking_call_warning {
                if val {
                    println!("[LAMELLAR WARNING] You are calling `LocalLockArray::blocking_read_lock` from within an async context which may lead to deadlock, it is recommended that you use `read_lock().await;` instead! 
                    Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture());
                }
            } else {
                println!("[LAMELLAR WARNING] You are calling `LocalLockArray::blocking_read_lock` from within an async context which may lead to deadlock, it is recommended that you use `read_lock().await;` instead! 
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture());
            }
        }
        let self_clone: LocalLockArray<T> = self.clone();
        self.block_on(async move {
            LocalLockReadGuard {
                array: self_clone.clone(),
                lock_guard: Arc::new(self_clone.lock.read().await),
            }
        })
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the calling PE's local read lock
    ///
    /// this function will block the calling task until the lock is acquired (but not the calling thread)
    ///
    /// # One-sided Operation
    /// Only explictly requires the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// world.block_on(async move {
    ///     let read_lock = array.read_lock().await;
    ///     //do interesting work
    /// });
    ///```
    pub async fn read_lock(&self) -> LocalLockReadGuard<T> {
        LocalLockReadGuard {
            array: self.clone(),
            lock_guard: Arc::new(self.lock.read().await),
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the calling PE's local write lock guard
    ///
    /// this function will block the thread until the lock is acquired
    /// Calling within an asynchronous block may lead to deadlock, use [write_lock](self::LocalLockArray::write_lock) instead.
    ///
    /// # One-sided Operation
    /// Only explictly requires the calling PE
    ///
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array:LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let write_lock = array.blocking_write_lock();
    /// //do interesting work
    ///
    pub fn blocking_write_lock(&self) -> LocalLockWriteGuard<T> {
        if std::thread::current().id() != *crate::MAIN_THREAD {
            if let Some(val) = config().blocking_call_warning {
                if val {
                    println!("[LAMELLAR WARNING] You are calling `LocalLockArray::blocking_write_lock` from within an async context which may lead to deadlock, it is recommended that you use `write_lock().await;` instead! 
                    Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture());
                }
            } else {
                println!("[LAMELLAR WARNING] You are calling `LocalLockArray::blocking_write_lock` from within an async context which may lead to deadlock, it is recommended that you use `write_lock().await;` instead! 
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture());
            }
        }
        let self_clone: LocalLockArray<T> = self.clone();
        self.block_on(async move {
            LocalLockWriteGuard {
                array: self_clone.clone(),
                lock_guard: self_clone.lock.write().await,
            }
        })
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the calling PE's local write lock
    ///
    /// this function will block the calling task until the lock is acquired (but not the calling thread)
    ///
    /// # One-sided Operation
    /// Only explictly requires the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// world.block_on(async move {
    ///     let write_lock = array.write_lock().await;
    ///     //do interesting work
    /// });
    ///```
    pub async fn write_lock(&self) -> LocalLockWriteGuard<T> {
        LocalLockWriteGuard {
            array: self.clone(),
            lock_guard: self.lock.write().await,
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the calling PE's local data as a [LocalLockLocalData], which allows safe immutable access to local elements.
    ///
    /// Calling this function will result in a local read lock being captured on the array
    ///
    /// # One-sided Operation
    /// Only returns local data on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.blocking_read_local_data();
    /// println!("PE{my_pe} data: {local_data:?}");
    ///```
    pub fn blocking_read_local_data(&self) -> LocalLockLocalData<T> {
        if std::thread::current().id() != *crate::MAIN_THREAD {
            if let Some(val) = config().blocking_call_warning {
                if val {
                    println!("[LAMELLAR WARNING] You are calling `LocalLockArray::blocking_read_local_data` from within an async context which may lead to deadlock, it is recommended that you use `read_local_data().await;` instead! 
                    Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture());
                }
            } else {
                println!("[LAMELLAR WARNING] You are calling `LocalLockArray::blocking_read_local_data` from within an async context which may lead to deadlock, it is recommended that you use `read_local_data().await;` instead! 
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture());
            }
        }
        let self_clone: LocalLockArray<T> = self.clone();
        self.block_on(async move {
            LocalLockLocalData {
                array: self_clone.clone(),
                // lock: self_clone.lock.clone(),
                start_index: 0,
                end_index: self_clone.num_elems_local(),
                lock_guard: Arc::new(self_clone.lock.read().await),
            }
        })
    }

    /// Return the calling PE's local data as a [LocalLockLocalData], which allows safe immutable access to local elements.   
    ///
    /// Calling this function will result in a local read lock being captured on the array
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
    ///     let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    ///     let local_data = array.read_local_data().await;
    ///     println!("PE{my_pe} data: {local_data:?}");
    /// });
    ///```
    pub async fn read_local_data(&self) -> LocalLockLocalData<T> {
        // println!("getting read lock in read_local_local");
        LocalLockLocalData {
            array: self.clone(),
            // lock: self.lock.clone(),
            start_index: 0,
            end_index: self.num_elems_local(),
            lock_guard: Arc::new(self.lock.read().await),
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the calling PE's local data as a [LocalLockMutLocalData], which allows safe mutable access to local elements.
    ///
    /// Calling this function will result in the local write lock being captured on the array
    ///
    /// # One-sided Operation
    /// Only returns (mutable) local data on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.blocking_write_local_data();
    /// println!("PE{my_pe} data: {local_data:?}");
    ///```
    pub fn blocking_write_local_data(&self) -> LocalLockMutLocalData<T> {
        if std::thread::current().id() != *crate::MAIN_THREAD {
            if let Some(val) = config().blocking_call_warning {
                if val {
                    println!("[LAMELLAR WARNING] You are calling `LocalLockArray::blocking_write_local_data` from within an async context which may lead to deadlock, it is recommended that you use `write_local_data().await;` instead! 
                    Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture());
                }
            } else {
                println!("[LAMELLAR WARNING] You are calling `LocalLockArray::blocking_write_local_data` from within an async context which may lead to deadlock, it is recommended that you use `write_local_data().await;` instead! 
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture());
            }
        }
        let self_clone: LocalLockArray<T> = self.clone();
        self.block_on(async move {
            let lock = self_clone.lock.write().await;
            let data = LocalLockMutLocalData {
                array: self_clone.clone(),
                start_index: 0,
                end_index: self_clone.num_elems_local(),
                lock_guard: lock,
            };
            // println!("got lock! {:?} {:?}",std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
            data
        })
    }

    #[doc(alias("One-sided", "onesided"))]
    /// TODO: UPDATE
    /// Return the calling PE's local data as a [LocalLockMutLocalData], which allows safe mutable access to local elements.   
    ///
    /// Calling this function will result in the local write lock being captured on the array
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
    ///     let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    ///     let local_data = array.write_local_data().await;
    ///     println!("PE{my_pe} data: {local_data:?}");
    /// });
    ///```
    pub async fn write_local_data(&self) -> LocalLockMutLocalData<T> {
        // println!("getting write lock in write_local_data");
        let lock = self.lock.write().await;
        let data = LocalLockMutLocalData {
            array: self.clone(),
            start_index: 0,
            end_index: self.num_elems_local(),
            lock_guard: lock,
        };
        // println!("got lock! {:?} {:?}",std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
        data
    }

    #[doc(hidden)]
    pub unsafe fn __local_as_slice(&self) -> &[T] {
        self.array.local_as_mut_slice()
    }

    #[doc(alias = "Collective")]
    /// Convert this LocalLockArray into an [UnsafeArray][crate::array::UnsafeArray]
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
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
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
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let array1 = array.clone();
    /// let slice = array1.local_data();
    ///
    /// // no borrows to this specific instance (array) so it can enter the "into_unsafe" call
    /// // but array1 will not be dropped until after 'slice' is dropped.
    /// // Given the ordering of these calls we will get stuck in "into_unsafe" as it
    /// // waits for the reference count to go down to "1" (but we will never be able to drop slice/array1).
    /// let unsafe_array = array.into_unsafe();
    /// unsafe_array.print();
    /// println!("{slice:?}");
    pub fn into_unsafe(self) -> UnsafeArray<T> {
        // println!("locallock into_unsafe");
        self.array.into()
    }

    // pub fn into_local_only(self) -> LocalOnlyArray<T> {
    //     // println!("locallock into_local_only");
    //     self.array.into()
    // }

    #[doc(alias = "Collective")]
    /// Convert this LocalLockArray into a (safe) [ReadOnlyArray][crate::array::ReadOnlyArray]
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
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let read_only_array = array.into_read_only();
    ///```
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let array1 = array.clone();
    /// let slice = unsafe {array1.local_data()};
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
        // println!("locallock into_read_only");
        self.array.into()
    }

    #[doc(alias = "Collective")]
    /// Convert this LocalLockArray into a (safe) [GlobalLockArray][crate::array::GlobalLockArray]
    ///
    /// This is a collective and blocking function which will only return when there is at most a single reference on each PE
    /// to this Array, and that reference is currently calling this function.
    ///
    /// When it returns, it is gauranteed that there are only `GlobalLockArray` handles to the underlying data
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `array` to enter the call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let global_lock_array = array.into_global_lock();
    ///```
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let array1 = array.clone();
    /// let slice = unsafe {array1.local_data()};
    ///
    /// // no borrows to this specific instance (array) so it can enter the "into_global_lock" call
    /// // but array1 will not be dropped until after mut_slice is dropped.
    /// // Given the ordering of these calls we will get stuck in "into_global_lock" as it
    /// // waits for the reference count to go down to "1" (but we will never be able to drop slice/array1).
    /// let global_lock_array = array.into_global_lock();
    /// global_lock_array.print();
    /// println!("{slice:?}");
    ///```
    pub fn into_global_lock(self) -> GlobalLockArray<T> {
        // println!("readonly into_global_lock");
        self.array.into()
    }
}

impl<T: Dist + 'static> LocalLockArray<T> {
    #[doc(alias = "Collective")]
    /// Convert this LocalLockArray into a (safe) [AtomicArray][crate::array::AtomicArray]
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
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let atomic_array = array.into_atomic();
    ///```
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let array1 = array.clone();
    /// let slice = unsafe {array1.local_data()};
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
        // println!("locallock into_atomic");
        self.array.into()
    }
}

impl<T: Dist + ArrayOps> TeamFrom<(Vec<T>, Distribution)> for LocalLockArray<T> {
    fn team_from(input: (Vec<T>, Distribution), team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let (vals, distribution) = input;
        let input = (&vals, distribution);
        let array: UnsafeArray<T> = TeamInto::team_into(input, team);
        array.into()
    }
}

// #[async_trait]
impl<T: Dist + ArrayOps> AsyncTeamFrom<(Vec<T>, Distribution)> for LocalLockArray<T> {
    async fn team_from(input: (Vec<T>, Distribution), team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let array: UnsafeArray<T> = AsyncTeamInto::team_into(input, team).await;
        array.async_into().await
    }
}

impl<T: Dist> From<UnsafeArray<T>> for LocalLockArray<T> {
    fn from(array: UnsafeArray<T>) -> Self {
        // println!("locallock from unsafe");
        array.block_on_outstanding(DarcMode::LocalLockArray);
        let lock = LocalRwDarc::new(array.team_rt(), ()).unwrap();

        LocalLockArray {
            lock: lock,
            array: array,
        }
    }
}

#[async_trait]
impl<T: Dist> AsyncFrom<UnsafeArray<T>> for LocalLockArray<T> {
    async fn async_from(array: UnsafeArray<T>) -> Self {
        // println!("locallock from unsafe");
        array.await_on_outstanding(DarcMode::LocalLockArray).await;
        let lock = LocalRwDarc::new(array.team_rt(), ()).unwrap();

        LocalLockArray {
            lock: lock,
            array: array,
        }
    }
}

// impl<T: Dist> From<LocalOnlyArray<T>> for LocalLockArray<T> {
//     fn from(array: LocalOnlyArray<T>) -> Self {
//         // println!("locallock from localonly");
//         unsafe { array.into_inner().into() }
//     }
// }

impl<T: Dist> From<AtomicArray<T>> for LocalLockArray<T> {
    fn from(array: AtomicArray<T>) -> Self {
        // println!("locallock from atomic");
        unsafe { array.into_inner().into() }
    }
}

impl<T: Dist> From<ReadOnlyArray<T>> for LocalLockArray<T> {
    fn from(array: ReadOnlyArray<T>) -> Self {
        // println!("locallock from readonly");
        unsafe { array.into_inner().into() }
    }
}

impl<T: Dist> From<GlobalLockArray<T>> for LocalLockArray<T> {
    fn from(array: GlobalLockArray<T>) -> Self {
        // println!("LocalLockArray from GlobalLockArray");
        unsafe { array.into_inner().into() }
    }
}

impl<T: Dist> From<LocalLockArray<T>> for LocalLockByteArray {
    fn from(array: LocalLockArray<T>) -> Self {
        LocalLockByteArray {
            lock: array.lock.clone(),
            array: array.array.into(),
        }
    }
}
impl<T: Dist> From<LocalLockArray<T>> for LamellarByteArray {
    fn from(array: LocalLockArray<T>) -> Self {
        LamellarByteArray::LocalLockArray(LocalLockByteArray {
            lock: array.lock.clone(),
            array: array.array.into(),
        })
    }
}

impl<T: Dist> From<LamellarByteArray> for LocalLockArray<T> {
    fn from(array: LamellarByteArray) -> Self {
        if let LamellarByteArray::LocalLockArray(array) = array {
            array.into()
        } else {
            panic!("Expected LamellarByteArray::LocalLockArray")
        }
    }
}

impl<T: Dist> From<LocalLockByteArray> for LocalLockArray<T> {
    fn from(array: LocalLockByteArray) -> Self {
        LocalLockArray {
            lock: array.lock.clone(),
            array: array.array.into(),
        }
    }
}

impl<T: Dist> private::ArrayExecAm<T> for LocalLockArray<T> {
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team_rt().clone()
    }
    fn team_counters(&self) -> Arc<AMCounters> {
        self.array.team_counters()
    }
}

impl<T: Dist> private::LamellarArrayPrivate<T> for LocalLockArray<T> {
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

impl<T: Dist> ActiveMessaging for LocalLockArray<T> {
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

impl<T: Dist> LamellarArray<T> for LocalLockArray<T> {
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

impl<T: Dist> LamellarEnv for LocalLockArray<T> {
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

impl<T: Dist> LamellarWrite for LocalLockArray<T> {}
impl<T: Dist> LamellarRead for LocalLockArray<T> {}

impl<T: Dist> SubArray<T> for LocalLockArray<T> {
    type Array = LocalLockArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array {
        LocalLockArray {
            lock: self.lock.clone(),
            array: self.array.sub_array(range),
        }
    }
    fn global_index(&self, sub_index: usize) -> usize {
        self.array.global_index(sub_index)
    }
}

impl<T: Dist + std::fmt::Debug> LocalLockArray<T> {
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
    /// let block_array = LocalLockArray::<usize>::new(&world,100,Distribution::Block);
    /// let cyclic_array = LocalLockArray::<usize>::new(&world,100,Distribution::Block);
    ///
    /// block_array.print();
    /// println!();
    /// cyclic_array.print();
    ///```
    pub fn print(&self) {
        self.array.print();
    }
}

impl<T: Dist + std::fmt::Debug> ArrayPrint<T> for LocalLockArray<T> {
    fn print(&self) {
        self.array.print()
    }
}

//#[doc(hidden)]
#[pin_project]
pub struct LocalLockArrayReduceHandle<T: Dist + AmDist> {
    req: AmHandle<Option<T>>,
    lock_guard: Arc<RwLockReadGuardArc<()>>,
}

impl<T: Dist + AmDist> LamellarRequest for LocalLockArrayReduceHandle<T> {
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

impl<T: Dist + AmDist> Future for LocalLockArrayReduceHandle<T> {
    type Output = Option<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.req.ready_or_set_waker(cx.waker()) {
            true => Poll::Ready(this.req.val()),
            false => Poll::Pending,
        }
    }
}

impl<T: Dist + AmDist + 'static> LocalLockReadGuard<T> {
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
    /// the local read lock ensures atomicity of only the local portion of the array, I.e. elements on a PE wont change while the operation is being executed on that PE
    /// Atomicity of data on remote PEs is only guaranteed while the remote operation is executing on the remote PE (once it has captured that PEs local lock).
    /// Remote data can change before and after the overall operation has completed.
    ///
    /// Lamellar converting to a [ReadOnlyArray] or [GlobalLockArray] before the reduction is a straightforward workaround to enusre the data is not changing during the reduction.
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// use rand::Rng;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = LocalLockArray::<usize>::new(&world,10,Distribution::Block);
    /// array.block_on(array.dist_iter().enumerate().for_each(move |(i,elem)| elem.store(i*2)));
    /// let read_guard = array.blocking_read_lock();
    /// let prod = array.block_on(read_guard.reduce("prod"));
    ///```
    pub fn reduce(self, op: &str) -> LocalLockArrayReduceHandle<T> {
        LocalLockArrayReduceHandle {
            req: self.array.array.reduce_data(op, self.array.clone().into()),
            lock_guard: self.lock_guard.clone(),
        }
    }

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
    /// the local read lock ensures atomicity of only the local portion of the array, I.e. elements on a PE wont change while the operation is being executed on that PE
    /// Atomicity of data on remote PEs is only guaranteed while the remote operation is executing on the remote PE (once it has captured that PEs local lock).
    /// Remote data can change before and after the overall operation has completed.
    ///
    /// Lamellar converting to a [ReadOnlyArray] or [GlobalLockArray] before the reduction is a straightforward workaround to enusre the data is not changing during the reduction.
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// use rand::Rng;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = LocalLockArray::<usize>::new(&world,10,Distribution::Block);
    /// array.block_on(array.dist_iter().enumerate().for_each(move |(i,elem)| elem.store(i*2)));
    /// let read_guard = array.blocking_read_lock();
    /// let prod = read_guard.blocking_reduce("prod");
    ///```
    pub fn blocking_reduce(self, op: &str) -> Option<T> {
        if std::thread::current().id() != *crate::MAIN_THREAD {
            let msg = format!("
                [LAMELLAR WARNING] You are calling `LocalLockArray::blocking_reduce` from within an async context which may lead to deadlock, it is recommended that you use `reduce(...).await;` instead! 
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture()
            );
            match config().blocking_call_warning {
                Some(val) if val => println!("{msg}"),
                _ => println!("{msg}"),
            }
        }
        self.array.block_on(LocalLockArrayReduceHandle {
            req: self.array.array.reduce_data(op, self.array.clone().into()),
            lock_guard: self.lock_guard.clone(),
        })
    }
}
impl<T: Dist + AmDist + ElementArithmeticOps + 'static> LocalLockReadGuard<T> {
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
    /// the local read lock ensures atomicity of only the local portion of the array, I.e. elements on a PE wont change while the operation is being executed on that PE
    /// Atomicity of data on remote PEs is only guaranteed while the remote operation is executing on the remote PE (once it has captured that PEs local lock).
    /// Remote data can change before and after the overall operation has completed.
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// use rand::Rng;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = LocalLockArray::<usize>::new(&world,10,Distribution::Block);
    /// array.block_on(array.dist_iter().enumerate().for_each(move |(i,elem)| elem.store(i*2)));
    /// let read_guard = array.blocking_read_lock();
    /// let sum = array.block_on(read_guard.sum());
    /// ```
    pub fn sum(self) -> LocalLockArrayReduceHandle<T> {
        self.reduce("sum")
    }
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
    /// the local read lock ensures atomicity of only the local portion of the array, I.e. elements on a PE wont change while the operation is being executed on that PE
    /// Atomicity of data on remote PEs is only guaranteed while the remote operation is executing on the remote PE (once it has captured that PEs local lock).
    /// Remote data can change before and after the overall operation has completed.
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// use rand::Rng;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = LocalLockArray::<usize>::new(&world,10,Distribution::Block);
    /// array.block_on(array.dist_iter().enumerate().for_each(move |(i,elem)| elem.store(i*2)));
    /// let read_guard = array.blocking_read_lock();
    /// let sum = read_guard.blocking_sum();
    /// ```
    pub fn blocking_sum(self) -> Option<T> {
        self.blocking_reduce("sum")
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
    /// the local read lock ensures atomicity of only the local portion of the array, I.e. elements on a PE wont change while the operation is being executed on that PE
    /// Atomicity of data on remote PEs is only guaranteed while the remote operation is executing on the remote PE (once it has captured that PEs local lock).
    /// Remote data can change before and after the overall operation has completed.
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = LocalLockArray::<usize>::new(&world,10,Distribution::Block);
    /// array.block_on(array.dist_iter().enumerate().for_each(move |(i,elem)| elem.store(i*2)));
    /// let read_guard = array.blocking_read_lock();
    /// let prod = array.block_on(read_guard.prod());
    /// assert_eq!((1..=array.len()).product::<usize>(),prod);
    ///```
    pub fn prod(self) -> LocalLockArrayReduceHandle<T> {
        self.reduce("prod")
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
    /// the local read lock ensures atomicity of only the local portion of the array, I.e. elements on a PE wont change while the operation is being executed on that PE
    /// Atomicity of data on remote PEs is only guaranteed while the remote operation is executing on the remote PE (once it has captured that PEs local lock).
    /// Remote data can change before and after the overall operation has completed.
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = LocalLockArray::<usize>::new(&world,10,Distribution::Block);
    /// array.block_on(array.dist_iter().enumerate().for_each(move |(i,elem)| elem.store(i*2)));
    /// let read_guard = array.blocking_read_lock();
    /// let prod = read_guard.blocking_prod();
    /// assert_eq!((1..=array.len()).product::<usize>(),prod);
    ///```
    pub fn blocking_prod(self) -> Option<T> {
        self.blocking_reduce("prod")
    }
}
impl<T: Dist + AmDist + ElementComparePartialEqOps + 'static> LocalLockReadGuard<T> {
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
    /// the local read lock ensures atomicity of only the local portion of the array, I.e. elements on a PE wont change while the operation is being executed on that PE
    /// Atomicity of data on remote PEs is only guaranteed while the remote operation is executing on the remote PE (once it has captured that PEs local lock).
    /// Remote data can change before and after the overall operation has completed.
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = LocalLockArray::<usize>::new(&world,10,Distribution::Block);
    /// array.block_on(array.dist_iter().enumerate().for_each(move |(i,elem)| elem.store(i*2)));
    /// let read_guard = array.blocking_read_lock();
    /// let max = array.block_on(read_guard.max());
    /// assert_eq!((array.len()-1)*2,max);
    ///```
    pub fn max(self) -> LocalLockArrayReduceHandle<T> {
        self.reduce("max")
    }

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
    /// the local read lock ensures atomicity of only the local portion of the array, I.e. elements on a PE wont change while the operation is being executed on that PE
    /// Atomicity of data on remote PEs is only guaranteed while the remote operation is executing on the remote PE (once it has captured that PEs local lock).
    /// Remote data can change before and after the overall operation has completed.
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = LocalLockArray::<usize>::new(&world,10,Distribution::Block);
    /// array.block_on(array.dist_iter().enumerate().for_each(move |(i,elem)| elem.store(i*2)));
    /// let read_guard = array.blocking_read_lock();
    /// let max = read_guard.blocking_max();
    /// assert_eq!((array.len()-1)*2,max);
    ///```
    pub fn blocking_max(self) -> Option<T> {
        self.blocking_reduce("max")
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
    /// the local read lock ensures atomicity of only the local portion of the array, I.e. elements on a PE wont change while the operation is being executed on that PE
    /// Atomicity of data on remote PEs is only guaranteed while the remote operation is executing on the remote PE (once it has captured that PEs local lock).
    /// Remote data can change before and after the overall operation has completed.
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = LocalLockArray::<usize>::new(&world,10,Distribution::Block);
    /// array.block_on(array.dist_iter().enumerate().for_each(move |(i,elem)| elem.store(i*2)));
    /// let read_guard = array.blocking_read_lock();
    /// let min = array.block_on(read_guard.min());
    /// assert_eq!(0,min);
    ///```
    pub fn min(self) -> LocalLockArrayReduceHandle<T> {
        self.reduce("min")
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
    /// the local read lock ensures atomicity of only the local portion of the array, I.e. elements on a PE wont change while the operation is being executed on that PE
    /// Atomicity of data on remote PEs is only guaranteed while the remote operation is executing on the remote PE (once it has captured that PEs local lock).
    /// Remote data can change before and after the overall operation has completed.
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = LocalLockArray::<usize>::new(&world,10,Distribution::Block);
    /// array.block_on(array.dist_iter().enumerate().for_each(move |(i,elem)| elem.store(i*2)));
    /// let read_guard = array.blocking_read_lock();
    /// let min = read_guard.blocking_min();
    /// assert_eq!(0,min);
    ///```
    pub fn blocking_min(self) -> Option<T> {
        self.blocking_reduce("min")
    }
}

// impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> LamellarArrayReduce<T>
//     for LocalLockArray<T>
// {
//     fn get_reduction_op(&self, op: String) -> LamellarArcAm {
//         self.array.get_reduction_op(op)
//     }
//     fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T>  > {
//         self.reduce(op)
//     }
//     fn sum(&self) -> Box<dyn LamellarRequest<Output = T>  > {
//         self.sum()
//     }
//     fn max(&self) -> Box<dyn LamellarRequest<Output = T>  > {
//         self.max()
//     }
//     fn prod(&self) -> Box<dyn LamellarRequest<Output = T>  > {
//         self.prod()
//     }
// }

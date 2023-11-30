mod iteration;
pub(crate) mod operations;
mod rdma;
use crate::array::private::LamellarArrayPrivate;
use crate::array::r#unsafe::{UnsafeByteArray, UnsafeByteArrayWeak};
use crate::array::*;
use crate::darc::global_rw_darc::{
    GlobalRwDarc, GlobalRwDarcCollectiveWriteGuard, GlobalRwDarcReadGuard, GlobalRwDarcWriteGuard,
};
use crate::darc::DarcMode;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::Dist;
use std::ops::{Deref, DerefMut};

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
    lock: GlobalRwDarc<()>,
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
pub struct GlobalLockMutLocalData<'a, T: Dist> {
    data: &'a mut [T],
    _index: usize,
    _lock_guard: GlobalRwDarcWriteGuard<()>,
}

// impl<T: Dist> Drop for GlobalLockMutLocalData<'_, T>{
//     fn drop(&mut self){
//         println!("release lock! {:?} {:?}",std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
//     }
// }

impl<T: Dist> Deref for GlobalLockMutLocalData<'_, T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        self.data
    }
}
impl<T: Dist> DerefMut for GlobalLockMutLocalData<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data
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
pub struct GlobalLockCollectiveMutLocalData<'a, T: Dist> {
    data: &'a mut [T],
    _index: usize,
    _lock_guard: GlobalRwDarcCollectiveWriteGuard<()>,
}

// impl<T: Dist> Drop for GlobalLockCollectiveMutLocalData<'_, T>{
//     fn drop(&mut self){
//         println!("release lock! {:?} {:?}",std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
//     }
// }

impl<T: Dist> Deref for GlobalLockCollectiveMutLocalData<'_, T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        self.data
    }
}
impl<T: Dist> DerefMut for GlobalLockCollectiveMutLocalData<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data
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
pub struct GlobalLockLocalData<'a, T: Dist> {
    pub(crate) array: GlobalLockArray<T>,
    pub(crate) data: &'a [T],
    index: usize,
    lock: GlobalRwDarc<()>,
    lock_guard: GlobalRwDarcReadGuard<()>,
}

impl<'a, T: Dist + std::fmt::Debug> std::fmt::Debug for GlobalLockLocalData<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.data)
    }
}

impl<'a, T: Dist> Clone for GlobalLockLocalData<'a, T> {
    fn clone(&self) -> Self {
        GlobalLockLocalData {
            array: self.array.clone(),
            data: self.data,
            index: self.index,
            lock: self.lock.clone(),
            lock_guard: self.lock_guard.clone(),
        }
    }
}

impl<'a, T: Dist> GlobalLockLocalData<'a, T> {
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
    /// let local_data = array.read_local_data();
    /// let sub_data = local_data.clone().into_sub_data(10,20); // clone() essentially increases the references to the read lock by 1.
    /// assert_eq!(local_data[10],sub_data[0]);
    ///```
    pub fn into_sub_data(self, start: usize, end: usize) -> GlobalLockLocalData<'a, T> {
        GlobalLockLocalData {
            array: self.array.clone(),
            data: &self.data[start..end],
            index: 0,
            lock: self.lock,
            lock_guard: self.lock_guard,
        }
    }
}

impl<'a, T: Dist + serde::Serialize> serde::Serialize for GlobalLockLocalData<'a, T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.data.serialize(serializer)
    }
}

impl<'a, T: Dist> Iterator for GlobalLockLocalData<'a, T> {
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

impl<T: Dist> Deref for GlobalLockLocalData<'_, T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.data
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

    // #[doc(alias("One-sided", "onesided"))]
    // /// Return the calling PE's local data as a [GlobalLockLocalData], which allows safe immutable access to local elements.
    // ///
    // /// Calling this function will result in a local read lock being captured on the array
    // ///
    // /// This function is blocking and intended to be called from non asynchronous contexts.
    // /// Calling within an asynchronous block may lead to deadlock.
    // ///
    // /// # One-sided Operation
    // /// Only returns local data on the calling PE
    // ///
    // /// # Examples
    // ///```
    // /// use lamellar::array::prelude::*;
    // /// let world = LamellarWorldBuilder::new().build();
    // /// let my_pe = world.my_pe();
    // /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    // ///
    // /// let local_data = array.read_local_data();
    // /// println!("PE{my_pe} data: {local_data:?}");
    // ///```
    // pub fn read_local_data(&self) -> GlobalLockLocalData<'_, T> {
    //     GlobalLockLocalData {
    //         array: self.clone(),
    //         data: unsafe { self.array.local_as_mut_slice() },
    //         index: 0,
    //         lock: self.lock.clone(),
    //         lock_guard: self.lock.read(),
    //     }
    // }

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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.block_on(array.read_local_data());
    /// println!("PE{my_pe} data: {local_data:?}");
    ///```
    pub async fn read_local_data(&self) -> GlobalLockLocalData<'_, T> {
        GlobalLockLocalData {
            array: self.clone(),
            data: unsafe { self.array.local_as_mut_slice() },
            index: 0,
            lock: self.lock.clone(),
            lock_guard: self.lock.read().await,
        }
    }

    // #[doc(alias("One-sided", "onesided"))]
    // /// Return the calling PE's local data as a [GlobalLockMutLocalData], which allows safe mutable access to local elements.
    // ///
    // /// Calling this function will result in the global write lock being captured on the array.
    // ///.
    // /// This function is blocking and intended to be called from non asynchronous contexts.
    // /// Calling within an asynchronous block may lead to deadlock.
    // ///
    // /// # One-sided Operation
    // /// Only returns (mutable) local data on the calling PE
    // ///
    // /// # Examples
    // ///```
    // /// use lamellar::array::prelude::*;
    // /// let world = LamellarWorldBuilder::new().build();
    // /// let my_pe = world.my_pe();
    // /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    // ///
    // /// let local_data = array.write_local_data();
    // /// println!("PE{my_pe} data: {local_data:?}");
    // ///```
    // pub fn write_local_data(&self) -> GlobalLockMutLocalData<'_, T> {
    //     let lock = self.lock.write();
    //     let data = GlobalLockMutLocalData {
    //         data: unsafe { self.array.local_as_mut_slice() },
    //         _index: 0,
    //         _lock_guard: lock,
    //     };
    //     // println!("got lock! {:?} {:?}",std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
    //     data
    // }

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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.block_on(array.write_local_data());
    /// println!("PE{my_pe} data: {local_data:?}");
    ///```
    pub async fn write_local_data(&self) -> GlobalLockMutLocalData<'_, T> {
        let lock = self.lock.write().await;
        let data = GlobalLockMutLocalData {
            data: unsafe { self.array.local_as_mut_slice() },
            _index: 0,
            _lock_guard: lock,
        };
        // println!("got lock! {:?} {:?}",std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
        data
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
    /// let array: GlobalLockArray<usize> = GlobalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_data = array.block_on(array.collective_write_local_data());
    /// println!("PE{my_pe} data: {local_data:?}");
    ///```
    pub async fn collective_write_local_data(&self) -> GlobalLockCollectiveMutLocalData<'_, T> {
        let lock = self.lock.collective_write().await;
        let data = GlobalLockCollectiveMutLocalData {
            data: unsafe { self.array.local_as_mut_slice() },
            _index: 0,
            _lock_guard: lock,
        };
        // println!("got lock! {:?} {:?}",std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
        data
    }

    // #[doc(hidden)] //todo create a custom macro to emit a warning saying use read_local_slice/write_local_slice intead
    pub(crate) async fn local_as_slice(&self) -> GlobalLockLocalData<'_, T> {
        let the_lock = self.lock.read().await;
        GlobalLockLocalData {
            array: self.clone(),
            data: unsafe { self.array.local_as_mut_slice() },
            index: 0,
            lock: self.lock.clone(),
            lock_guard: the_lock,
        }
    }
    // #[doc(hidden)]
    // pub unsafe fn local_as_mut_slice(&self) -> &mut [T] {
    //     self.array.local_as_mut_slice()
    // }

    // #[doc(hidden)]
    pub(crate) async fn local_as_mut_slice(&self) -> GlobalLockMutLocalData<'_, T> {
        let the_lock = self.lock.write().await;
        let lock = GlobalLockMutLocalData {
            data: unsafe { self.array.local_as_mut_slice() },
            _index: 0,
            _lock_guard: the_lock,
        };
        // println!("have lla write lock");
        // println!("got lock! {:?} {:?}",std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
        lock
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the calling PE's local data as a [GlobalLockLocalData], which allows safe immutable access to local elements.   
    ///
    /// Calling this function will result in a local read lock being captured on the array
    ///
    /// While this call is safe, it may be more clear to use the [read_local_data()][GlobalLockArray::read_local_data] function.
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
    /// let local_data = array.block_on(array.local_data());
    /// println!("PE{my_pe} data: {local_data:?}");
    ///```
    pub async fn local_data(&self) -> GlobalLockLocalData<'_, T> {
        self.local_as_slice().await
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the calling PE's local data as a [GlobalLockMutLocalData], which allows safe immutable access to local elements.   
    ///
    /// Calling this function will result in a local read lock being captured on the array
    ///
    /// While this call is safe, it may be more clear to use the [write_local_data()][GlobalLockArray::write_local_data] function.
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
    /// let local_data = array.block_on(array.mut_local_data());
    /// println!("PE{my_pe} data: {local_data:?}");
    ///```
    pub async fn mut_local_data(&self) -> GlobalLockMutLocalData<'_, T> {
        self.local_as_mut_slice().await
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
    pub fn into_local_lock(self) -> LocalLockArray<T> {
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
        // println!("GlobalLock into_atomic");
        self.array.into()
    }
}

impl<T: Dist + ArrayOps> TeamFrom<(Vec<T>, Distribution)> for GlobalLockArray<T> {
    fn team_from(input: (Vec<T>, Distribution), team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let (vals, distribution) = input;
        let input = (&vals, distribution);
        let array: UnsafeArray<T> = input.team_into(team);
        array.into()
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
    fn barrier(&self) {
        self.array.barrier();
    }
    fn wait_all(&self) {
        self.array.wait_all()
        // println!("done in wait all {:?}",std::time::SystemTime::now());
    }
    fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future,
    {
        self.array.block_on(f)
    }
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

#[doc(hidden)]
pub struct GlobalLockArrayReduceHandle<T: Dist + AmDist> {
    req: Box<dyn LamellarRequest<Output = T>>,
    _lock_guard: GlobalRwDarcReadGuard<()>,
}

#[async_trait]
impl<T: Dist + AmDist> LamellarRequest for GlobalLockArrayReduceHandle<T> {
    type Output = T;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        self.req.into_future().await
    }
    fn get(&self) -> Self::Output {
        self.req.get()
    }
}

impl<T: Dist + AmDist + 'static> LamellarArrayReduce<T> for GlobalLockArray<T> {
    fn reduce(&self, op: &str) -> Pin<Box<dyn Future<Output = T>>> {
        let lock = self.array.block_on(self.lock.read());
        Box::new(GlobalLockArrayReduceHandle {
            req: self.array.reduce_data(op, self.clone().into()),
            _lock_guard: lock,
        })
        .into_future()
    }
}
impl<T: Dist + AmDist + ElementArithmeticOps + 'static> LamellarArrayArithmeticReduce<T>
    for GlobalLockArray<T>
{
    fn sum(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.reduce("sum")
    }
    fn prod(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.reduce("prod")
    }
}
impl<T: Dist + AmDist + ElementComparePartialEqOps + 'static> LamellarArrayCompareReduce<T>
    for GlobalLockArray<T>
{
    fn max(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.reduce("max")
    }
    fn min(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.reduce("min")
    }
}

// impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> LamellarArrayReduce<T>
//     for GlobalLockArray<T>
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

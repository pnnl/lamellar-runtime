use core::marker::PhantomData;
use parking_lot::RwLock;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::darc::local_rw_darc::LocalRwDarc;
use crate::darc::{Darc, DarcInner, DarcMode, __NetworkDarc};
use crate::lamellae::{LamellaeComm, LamellaeRDMA};
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::lamellar_world::LAMELLAES;
use crate::IdError;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
enum LockType {
    Read,
    Write,
}

#[derive(Debug)]
pub(crate) struct DistRwLock<T> {
    readers: AtomicUsize,
    writer: AtomicUsize,
    // local_cnt: AtomicUsize, //eventually we can do an optimization potentially where if we already have the global lock and another local request comes in we keep it (although this could cause starvation)
    // local_state: Mutex<Option<LockType>>,
    team: std::pin::Pin<Arc<LamellarTeamRT>>,
    data: std::cell::UnsafeCell<T>,
}

unsafe impl<T: Send> Send for DistRwLock<T> {}
unsafe impl<T: Sync> Sync for DistRwLock<T> {}

/// # Safety
///
/// The lock is assumed to be allocated in rdma able memory (so we can look up the corresponding address on a remote pe).
/// since this is intended to be wrapped by a Darc there should be no issues
/// usage outside of a darc is undefined
impl<T> DistRwLock<T> {
    pub(crate) fn new<U: Into<IntoLamellarTeam>>(data: T, team: U) -> DistRwLock<T> {
        let team = team.into().team.clone();
        DistRwLock {
            readers: AtomicUsize::new(0),
            writer: AtomicUsize::new(team.num_pes),
            // local_cnt: AtomicUsize::new(0),
            // local_state: Mutex::new(None),
            team: team,
            data: std::cell::UnsafeCell::new(data),
        }
    }
    fn into_inner(self) -> T {
        self.data.into_inner()
    }
}
impl<T> DistRwLock<T> {
    async fn async_reader_lock(&self, _pe: usize) {
        loop {
            while self.writer.load(Ordering::SeqCst) != self.team.num_pes {
                async_std::task::yield_now().await;
            }
            // println!("\t{:?} inc read count {:?} {:?}",pe,self.readers.load(Ordering::SeqCst),self.writer.load(Ordering::SeqCst));
            self.readers.fetch_add(1, Ordering::SeqCst);
            if self.writer.load(Ordering::SeqCst) == self.team.num_pes {
                break;
            }
            self.readers.fetch_sub(1, Ordering::SeqCst);
            // println!("\t{:?} writers exist dec read count {:?} {:?}",pe,self.readers.load(Ordering::SeqCst),self.writer.load(Ordering::SeqCst));
        }
        // println!("\t{:?} read locked {:?} {:?}",pe,self.readers.load(Ordering::SeqCst),self.writer.load(Ordering::SeqCst));
    }
    async fn async_writer_lock(&self, pe: usize) {
        while let Err(_) =
            self.writer
                .compare_exchange(self.team.num_pes, pe, Ordering::SeqCst, Ordering::SeqCst)
        {
            async_std::task::yield_now().await;
        }
        // println!("\t{:?} write lock checking for readers {:?} {:?}",pe,self.readers.load(Ordering::SeqCst),self.writer.load(Ordering::SeqCst));
        while self.readers.load(Ordering::SeqCst) != 0 {
            async_std::task::yield_now().await;
        }
        // println!("\t{:?} write locked {:?} {:?}",pe,self.readers.load(Ordering::SeqCst),self.writer.load(Ordering::SeqCst));
    }

    /// # Safety
    ///
    /// The lock must be held when calling this method.
    unsafe fn reader_unlock(&self, _pe: usize) {
        // println!("\t{:?} reader unlocking  {:?} {:?}",pe,self.readers.load(Ordering::SeqCst),self.writer.load(Ordering::SeqCst));
        self.readers.fetch_sub(1, Ordering::SeqCst);
        // println!("\t{:?} reader unlocked  {:?} {:?}",pe,self.readers.load(Ordering::SeqCst),self.writer.load(Ordering::SeqCst));
    }
    /// # Safety
    ///
    /// The lock must be held when calling this method.
    unsafe fn writer_unlock(&self, pe: usize) {
        // println!("\t{:?} writer unlocking {:?} {:?}",pe,self.readers.load(Ordering::SeqCst),self.writer.load(Ordering::SeqCst));
        if let Err(val) =
            self.writer
                .compare_exchange(pe, self.team.num_pes, Ordering::SeqCst, Ordering::SeqCst)
        {
            panic!(
                "should not be trying to unlock another pes lock {:?} {:?}",
                pe, val
            );
        }
        // println!("\t{:?} writer unlocked {:?} {:?}",pe,self.readers.load(Ordering::SeqCst),self.writer.load(Ordering::SeqCst));
    }
}

#[lamellar_impl::AmDataRT(Debug)]
struct LockAm {
    rwlock_addr: usize,
    orig_pe: usize, //with respect to the team
    lock_type: LockType,
}
#[lamellar_impl::rt_am]
impl LamellarAM for LockAm {
    async fn exec() {
        let lock = {
            let rwlock = unsafe { &*(self.rwlock_addr as *mut DarcInner<DistRwLock<()>>) }; //we dont actually care about the "type" we wrap here, we just need access to the meta data for the darc

            match self.lock_type {
                LockType::Read => {
                    futures::future::Either::Left(rwlock.item().async_reader_lock(self.orig_pe))
                }
                LockType::Write => {
                    futures::future::Either::Right(rwlock.item().async_writer_lock(self.orig_pe))
                }
            }
        };
        lock.await;
        // println!("finished lock am");
    }
}

#[lamellar_impl::AmDataRT(Debug)]
struct UnlockAm {
    rwlock_addr: usize,
    orig_pe: usize, //with respect to the team
    lock_type: LockType,
}
#[lamellar_impl::rt_am]
impl LamellarAM for UnlockAm {
    async fn exec() {
        let rwlock = unsafe { &*(self.rwlock_addr as *mut DarcInner<DistRwLock<()>>) }; //we dont actually care about the "type" we wrap here, we just need access to the meta data for the darc
        unsafe {
            match self.lock_type {
                LockType::Read => rwlock.item().reader_unlock(self.orig_pe),
                LockType::Write => rwlock.item().writer_unlock(self.orig_pe),
            }
        }
    }
}

pub struct GlobalRwDarcReadGuard<'a, T: 'static> {
    rwlock: Darc<DistRwLock<T>>,
    marker: PhantomData<&'a mut T>,
}
impl<'a, T: 'a> Deref for GlobalRwDarcReadGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { &*self.rwlock.data.get() }
    }
}

impl<'a, T: 'a> Drop for GlobalRwDarcReadGuard<'a, T> {
    fn drop(&mut self) {
        // println!("dropping read guard");
        let inner = self.rwlock.inner();
        let team = inner.team();
        let remote_rwlock_addr = team.lamellae.remote_addr(
            0,
            inner as *const DarcInner<DistRwLock<T>> as *const () as usize,
        );
        team.exec_am_pe_tg(
            0,
            UnlockAm {
                rwlock_addr: remote_rwlock_addr,
                orig_pe: team.team_pe.expect("darcs cant exist on non team members"),
                lock_type: LockType::Read,
            },
            Some(inner.am_counters()),
        );
    }
}

//TODO update this so that we print locked if data is locked...
impl<T: fmt::Debug> fmt::Debug for GlobalRwDarcReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unsafe { fmt::Debug::fmt(&self.rwlock.data.get().as_ref(), f) }
    }
}

pub struct GlobalRwDarcWriteGuard<'a, T: 'static> {
    rwlock: Darc<DistRwLock<T>>,
    marker: PhantomData<&'a mut T>,
}

impl<'a, T: 'a> Deref for GlobalRwDarcWriteGuard<'a, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        unsafe { &*self.rwlock.data.get() }
    }
}

impl<'a, T: 'a> DerefMut for GlobalRwDarcWriteGuard<'a, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.rwlock.data.get() }
    }
}

impl<'a, T: 'a> Drop for GlobalRwDarcWriteGuard<'a, T> {
    fn drop(&mut self) {
        // println!("dropping write guard");
        let inner = self.rwlock.inner();
        let team = inner.team();
        let remote_rwlock_addr = team.lamellae.remote_addr(
            0,
            inner as *const DarcInner<DistRwLock<T>> as *const () as usize,
        );
        team.exec_am_pe_tg(
            0,
            UnlockAm {
                rwlock_addr: remote_rwlock_addr,
                orig_pe: team.team_pe.expect("darcs cant exist on non team members"),
                lock_type: LockType::Write,
            },
            Some(inner.am_counters()),
        );
    }
}

impl<T: fmt::Debug> fmt::Debug for GlobalRwDarcWriteGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unsafe { fmt::Debug::fmt(&self.rwlock.data.get().as_ref(), f) }
    }
}

/// A global read-write `Darc`
///
/// A single global read-write lock is associated with the `GlobalRwDarc`.
/// Whenever the interior object is accessed (on any PE) the global lock is required to be aquired.
/// When a thread aquires a Write lock it is guaranteed to the only thread with access to
/// the interior object across the entire distributed environment. When a thread aquires a Read lock
/// it may be one of many threads accross the distributed envrionment with access, but none of them will have mutable access.
/// NOTE: Grabbing the lock is a distributed operation and can come with a significant performance penalty
/// - Contrast with a `LocalRwDarc`, where each local PE has a local lock.
/// - Contrast with a `Darc`, which also has local ownership but does not
///   allow modification unless the wrapped object itself provides it, e.g.
///   `AtomicUsize` or `Mutex<..>`.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct GlobalRwDarc<T: 'static> {
    // pub(crate) darc: Darc<RwLock<Box<(T,MyRwLock)>>>,
    #[serde(
        serialize_with = "globalrw_serialize2",
        deserialize_with = "globalrw_from_ndarc2"
    )]
    pub(crate) darc: Darc<DistRwLock<T>>,
}

unsafe impl<T: Send> Send for GlobalRwDarc<T> {}
unsafe impl<T: Sync> Sync for GlobalRwDarc<T> {}

impl<T> crate::active_messaging::DarcSerde for GlobalRwDarc<T> {
    fn ser(&self, num_pes: usize) {
        // println!("in global rw darc ser");
        // match cur_pe {
        //     Ok(cur_pe) => {
        self.darc.serialize_update_cnts(num_pes);
        //     }
        //     Err(err) => {
        //         panic!("can only access darcs within team members ({:?})", err);
        //     }
        // }
    }
    fn des(&self, cur_pe: Result<usize, IdError>) {
        match cur_pe {
            Ok(_) => {
                self.darc.deserialize_update_cnts();
            }
            Err(err) => {
                panic!("can only access darcs within team members ({:?})", err);
            }
        }
    }
}

impl<T> GlobalRwDarc<T> {
    fn inner(&self) -> &DarcInner<DistRwLock<T>> {
        self.darc.inner()
    }

    #[doc(hidden)]
    pub fn serialize_update_cnts(&self, cnt: usize) {
        // println!("serialize darc cnts");
        // if self.darc.src_pe == cur_pe{
        self.inner()
            .dist_cnt
            .fetch_add(cnt, std::sync::atomic::Ordering::SeqCst);
        // }
        // self.print();
        // println!("done serialize darc cnts");
    }

    #[doc(hidden)]
    pub fn deserialize_update_cnts(&self) {
        // println!("deserialize darc? cnts");
        // if self.darc.src_pe != cur_pe{
        self.inner().inc_pe_ref_count(self.darc.src_pe, 1); // we need to increment by 2 cause bincode calls the serialize function twice when serializing...
                                                            // }
                                                            // self.inner().local_cnt.fetch_add(1, Ordering::SeqCst);
                                                            // self.print();
                                                            // println!("done deserialize darc cnts");
    }

    #[doc(hidden)]
    pub fn print(&self) {
        let rel_addr =
            unsafe { self.darc.inner as usize - (*self.inner().team).lamellae.base_addr() };
        println!(
            "--------\norig: {:?} {:?} (0x{:x}) {:?}\n--------",
            self.darc.src_pe,
            self.darc.inner,
            rel_addr,
            self.inner()
        );
    }

    /// Launches an active message to gather a global read lock associated with this GlobalRwDarc.
    ///
    /// The current task will be blocked until the lock has been acquired.
    ///
    /// This function will not return while any writer currently has access to the lock, but there may be other readers
    ///
    /// Returns an RAII guard which will drop the read access of the wrlock when dropped
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for creating and transfering the active message which aquires the lock.
    /// Once aquired this specific instance of the read lock will only be held by the calling PE (until it is dropped)
    /// Other PEs may have separately aquired read locks as well.
    ///
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
    ///         let counter = self.counter.async_read().await; // await until we get the write lock
    ///         println!("the current counter value on pe {} = {}",lamellar::current_pe,*counter);
    ///     }
    ///  }
    /// //-------------
    /// 
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// let counter = GlobalRwDarc::new(&world, 0).unwrap();
    /// world.exec_am_all(DarcAm {counter: counter.clone()});
    /// let guard = world.block_on(counter.async_read());
    /// println!("the current counter value on pe {} main thread = {}",my_pe,*guard);
    /// drop(guard); //release the
    /// world.wait_all(); // wait for my active message to return
    /// world.barrier(); //at this point all updates will have been performed
    ///```
    pub async fn async_read(&self) -> GlobalRwDarcReadGuard<'_, T> {
        // println!("async read");
        let inner = self.inner();
        let team = inner.team();
        let remote_rwlock_addr = team.lamellae.remote_addr(
            0,
            inner as *const DarcInner<DistRwLock<T>> as *const () as usize,
        );
        team.exec_am_pe_tg(
            0,
            LockAm {
                rwlock_addr: remote_rwlock_addr,
                orig_pe: team.team_pe.expect("darcs cant exist on non team members"),
                lock_type: LockType::Read,
            },
            Some(inner.am_counters()),
        )
        .into_future()
        .await;
        GlobalRwDarcReadGuard {
            rwlock: self.darc.clone(),
            marker: PhantomData,
        }
        // inner.item().read(remote_rwlock_addr)
    }

    /// Launches an active message to gather the global write lock associated with this GlobalRwDarc.
    ///
    /// The current task will be blocked until the lock has been acquired.
    ///
    /// This function will not return while another writer or any readers currently have access to the lock
    ///
    /// Returns an RAII guard which will drop the write access of the wrlock when dropped
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for creating and transfering the active message which aquires the lock.
    /// Once aquired the lock will only be held by the calling PE (until it is dropped)
    ///
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
    ///         let mut counter = self.counter.async_write().await; // await until we get the write lock
    ///         *counter += 1; // although we have the global lock, we are still only modifying the data local to this PE
    ///     }
    ///  }
    /// //-------------
    /// 
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// let counter = GlobalRwDarc::new(&world, 0).unwrap();
    /// world.exec_am_all(DarcAm {counter: counter.clone()});
    /// let mut guard = world.block_on(counter.async_write());
    /// *guard += my_pe;
    /// drop(guard); //release the
    /// world.wait_all(); // wait for my active message to return
    /// world.barrier(); //at this point all updates will have been performed
    ///```
    pub async fn async_write(&self) -> GlobalRwDarcWriteGuard<'_, T> {
        // println!("async write");
        let inner = self.inner();
        let team = inner.team();
        let remote_rwlock_addr = team.lamellae.remote_addr(
            0,
            inner as *const DarcInner<DistRwLock<T>> as *const () as usize,
        );
        team.exec_am_pe_tg(
            0,
            LockAm {
                rwlock_addr: remote_rwlock_addr,
                orig_pe: team.team_pe.expect("darcs cant exist on non team members"),
                lock_type: LockType::Write,
            },
            Some(inner.am_counters()),
        )
        .into_future()
        .await;
        GlobalRwDarcWriteGuard {
            rwlock: self.darc.clone(),
            marker: PhantomData,
        }
    }

    /// Launches an active message to gather a global read lock associated with this GlobalRwDarc.
    ///
    /// The current THREAD will be blocked until the lock has been acquired.
    ///
    /// This function will not return while any writer currently has access to the lock, but there may be other readers
    ///
    /// Returns an RAII guard which will drop the read access of the wrlock when dropped
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for creating and transfering the active message which aquires the lock.
    /// Once aquired this specific instance of the read lock will only be held by the calling PE (until it is dropped)
    /// Other PEs may have separately aquired read locks as well.
    ///
    ///
    /// # Note
    /// Do not use this function in an asynchronous context (i.e. a Lamellar Active message), instead use [GlobalRwDarc::async_read]
    ///
    /// # Examples
    ///```
    /// use lamellar::darc::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// let counter = GlobalRwDarc::new(&world, 0).unwrap();
    /// // do interesting work
    /// let guard = counter.read(); //blocks current thread until aquired
    /// println!("the current counter value on pe {} main thread = {}",my_pe,*guard);
    ///```
    pub fn read(&self) -> GlobalRwDarcReadGuard<'_, T> {
        // println!("read");
        let inner = self.inner();
        let team = inner.team();
        let remote_rwlock_addr = team.lamellae.remote_addr(
            0,
            inner as *const DarcInner<DistRwLock<T>> as *const () as usize,
        );
        team.exec_am_pe_tg(
            0,
            LockAm {
                rwlock_addr: remote_rwlock_addr,
                orig_pe: team.team_pe.expect("darcs cant exist on non team members"),
                lock_type: LockType::Read,
            },
            Some(inner.am_counters()),
        )
        .get();
        GlobalRwDarcReadGuard {
            rwlock: self.darc.clone(),
            marker: PhantomData,
        }
    }

    /// Launches an active message to gather a global write lock associated with this GlobalRwDarc.
    ///
    /// The current THREAD will be blocked until the lock has been acquired.
    ///
    /// This function will not return while another writer or any readers currently have access to the lock
    ///
    /// Returns an RAII guard which will drop the write access of the wrlock when dropped
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for creating and transfering the active message which aquires the lock.
    /// Once aquired the lock will only be held by the calling PE (until it is dropped)
    ///
    /// # Note
    /// Do not use this function in an asynchronous context (i.e. a Lamellar Active message), instead use [GlobalRwDarc::async_write]
    ///
    /// # Examples
    ///```
    /// use lamellar::darc::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///
    /// let counter = GlobalRwDarc::new(&world, 0).unwrap();
    /// // do interesting work
    /// let mut guard = counter.write(); //blocks current thread until aquired
    /// *guard += my_pe;
    ///```
    pub fn write(&self) -> GlobalRwDarcWriteGuard<'_, T> {
        // println!("write");
        let inner = self.inner();
        let team = inner.team();
        let remote_rwlock_addr = team.lamellae.remote_addr(
            0,
            inner as *const DarcInner<DistRwLock<T>> as *const () as usize,
        );
        team.exec_am_pe_tg(
            0,
            LockAm {
                rwlock_addr: remote_rwlock_addr,
                orig_pe: team.team_pe.expect("darcs cant exist on non team members"),
                lock_type: LockType::Write,
            },
            Some(inner.am_counters()),
        )
        .get();
        GlobalRwDarcWriteGuard {
            rwlock: self.darc.clone(),
            marker: PhantomData,
        }
        // inner.item().write(remote_rwlock_addr)
    }
}

impl<T> GlobalRwDarc<T> {
    /// Constructs a new `GlobalRwDarc<T>` on the PEs specified by team.
    ///
    /// This is a blocking collective call amongst all PEs in the team, only returning once every PE in the team has completed the call.
    ///
    /// Returns an error if this PE is not a part of team
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `team` to enter the call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    ///
    /// ```
    /// use lamellar::darc::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let five = GlobalRwDarc::new(&world,5).expect("PE in world team");
    /// ```
    pub fn new<U: Clone + Into<IntoLamellarTeam>>(
        team: U,
        item: T,
    ) -> Result<GlobalRwDarc<T>, IdError> {
        Ok(GlobalRwDarc {
            darc: Darc::try_new(
                team.clone(),
                DistRwLock::new(item, team),
                DarcMode::GlobalRw,
            )?,
        })
    }

    // pub(crate) fn try_new<U: Clone + Into<IntoLamellarTeam>>(
    //     team: U,
    //     item: T,
    // ) -> Result<GlobalRwDarc<T>, IdError> {
    //     Ok(GlobalRwDarc {
    //         darc: Darc::try_new(
    //             team.clone(),
    //             DistRwLock::new(item, team),
    //             DarcMode::GlobalRw,
    //         )?,
    //     })
    // }

    /// Converts this GlobalRwDarc into a regular [Darc]
    ///
    /// This is a blocking collective call amongst all PEs in the GlobalRwDarc's team, only returning once every PE in the team has completed the call.
    ///
    /// Furthermore, this call will block while any additional references outside of the one making this call exist on each PE. It is not possible for the
    /// pointed to object to wrapped by both a Darc and a GlobalRwDarc simultaneously (on any PE).
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `darc` to enter the call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    /// ```
    /// use lamellar::darc::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let five = GlobalRwDarc::new(&world,5).expect("PE in world team");
    /// let five_as_darc = five.into_darc();
    /// ```
    pub fn into_darc(self) -> Darc<T> {
        let inner = self.inner();
        // println!("into_darc");
        // self.print();
        inner.block_on_outstanding(DarcMode::Darc, 0);
        inner.local_cnt.fetch_add(1, Ordering::SeqCst); //we add this here because to account for moving inner into d
        let item = unsafe { Box::from_raw(inner.item as *mut DistRwLock<T>).into_inner() };
        let d = Darc {
            inner: self.darc.inner as *mut DarcInner<T>,
            src_pe: self.darc.src_pe,
            // phantom: PhantomData,
        };
        d.inner_mut().update_item(Box::into_raw(Box::new(item)));
        d
    }

    /// Converts this GlobalRwDarc into a [LocalRwDarc]
    ///
    /// This is a blocking collective call amongst all PEs in the GlobalRwDarc's team, only returning once every PE in the team has completed the call.
    ///
    /// Furthermore, this call will block while any additional references outside of the one making this call exist on each PE. It is not possible for the
    /// pointed to object to wrapped by both a GlobalRwDarc and a LocalRwDarc simultaneously (on any PE).
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `darc` to enter the call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    /// ```
    /// use lamellar::darc::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let five = GlobalRwDarc::new(&world,5).expect("PE in world team");
    /// let five_as_localdarc = five.into_localrw();
    /// ```
    pub fn into_localrw(self) -> LocalRwDarc<T> {
        let inner = self.inner();
        // println!("into_localrw");
        // self.print();
        inner.block_on_outstanding(DarcMode::LocalRw, 0);
        inner.local_cnt.fetch_add(1, Ordering::SeqCst); //we add this here because to account for moving inner into d
        let item = unsafe { Box::from_raw(inner.item as *mut DistRwLock<T>).into_inner() };
        let d = Darc {
            inner: self.darc.inner as *mut DarcInner<Arc<RwLock<Box<T>>>>,
            src_pe: self.darc.src_pe,
            // phantom: PhantomData,
        };
        d.inner_mut()
            .update_item(Box::into_raw(Box::new(Arc::new(RwLock::new(Box::new(
                item,
            ))))));
        LocalRwDarc { darc: d }
    }
}

impl<T> Clone for GlobalRwDarc<T> {
    fn clone(&self) -> Self {
        // self.inner().local_cnt.fetch_add(1,Ordering::SeqCst);
        GlobalRwDarc {
            darc: self.darc.clone(),
        }
    }
}

impl<T: fmt::Display> fmt::Display for GlobalRwDarc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unsafe { fmt::Display::fmt(&self.inner().item().data.get().as_ref().unwrap(), f) }
    }
}

// #[doc(hidden)]
// pub fn globalrw_serialize<S, T>(localrw: &GlobalRwDarc<T>, s: S) -> Result<S::Ok, S::Error>
// where
//     S: Serializer,
// {
//     __NetworkDarc::<T>::from(&localrw.darc).serialize(s)
// }

// #[doc(hidden)]
// pub fn globalrw_from_ndarc<'de, D, T>(deserializer: D) -> Result<GlobalRwDarc<T>, D::Error>
// where
//     D: Deserializer<'de>,
// {
//     let ndarc: __NetworkDarc<T> = Deserialize::deserialize(deserializer)?;
//     // println!("gdarc from net darc");
//     let rwdarc = GlobalRwDarc {
//         darc: Darc::from(ndarc),
//     };
//     // println!("lrwdarc from net darc");
//     // rwdarc.print();
//     Ok(rwdarc)
// }

pub(crate) fn globalrw_serialize2<S, T>(
    globalrw: &Darc<DistRwLock<T>>,
    s: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    __NetworkDarc::<T>::from(globalrw).serialize(s)
}

pub(crate) fn globalrw_from_ndarc2<'de, D, T>(
    deserializer: D,
) -> Result<Darc<DistRwLock<T>>, D::Error>
where
    D: Deserializer<'de>,
{
    let ndarc: __NetworkDarc<T> = Deserialize::deserialize(deserializer)?;
    // println!("gdarc from net darc");
    // rwdarc.print();
    Ok(Darc::from(ndarc))
}

impl<T> From<&Darc<DistRwLock<T>>> for __NetworkDarc<T> {
    fn from(darc: &Darc<DistRwLock<T>>) -> Self {
        // println!("rwdarc to net darc");
        // darc.print();
        let team = &darc.inner().team();
        let ndarc = __NetworkDarc {
            inner_addr: darc.inner as *const u8 as usize,
            backend: team.lamellae.backend(),
            orig_world_pe: team.world_pe,
            orig_team_pe: team.team_pe.expect("darcs only valid on team members"),
            phantom: PhantomData,
        };
        ndarc
    }
}

impl<T> From<__NetworkDarc<T>> for Darc<DistRwLock<T>> {
    fn from(ndarc: __NetworkDarc<T>) -> Self {
        // println!("rwdarc from net darc");

        if let Some(lamellae) = LAMELLAES.read().get(&ndarc.backend) {
            let darc = Darc {
                inner: lamellae.local_addr(ndarc.orig_world_pe, ndarc.inner_addr)
                    as *mut DarcInner<DistRwLock<T>>,
                src_pe: ndarc.orig_team_pe,
                // phantom: PhantomData,
            };
            darc
        } else {
            panic!("unexepected lamellae backend {:?}", &ndarc.backend);
        }
    }
}

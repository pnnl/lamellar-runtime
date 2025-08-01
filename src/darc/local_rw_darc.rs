use async_lock::{RwLock, RwLockReadGuardArc, RwLockWriteGuardArc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::{
    active_messaging::RemotePtr,
    darc::{Darc, DarcInner, DarcMode, __NetworkDarc},
    lamellar_team::IntoLamellarTeam,
    IdError, LamellarEnv, LamellarTeam,
};

use super::handle::LocalRwDarcHandle;
pub(crate) use super::handle::{
    IntoDarcHandle, IntoGlobalRwDarcHandle, LocalRwDarcReadHandle, LocalRwDarcWriteHandle,
};

#[derive(Debug)]
pub struct LocalRwDarcReadGuard<T: 'static> {
    pub(crate) _darc: LocalRwDarc<T>,
    pub(crate) lock: RwLockReadGuardArc<T>,
}

impl<T: fmt::Display> fmt::Display for LocalRwDarcReadGuard<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.lock, f)
    }
}

impl<T> std::ops::Deref for LocalRwDarcReadGuard<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.lock
    }
}

// impl<T> RwDarcGuard<LocalRwDarc<T>> for LocalRwDarcReadGuard<T> {
//     type Guard = RwLockReadGuardArc<T>;
//     fn new(darc: LocalRwDarc<T>, lock_guard: Self::Guard) -> Self {
//         LocalRwDarcReadGuard {
//             darc,
//             lock: lock_guard,
//         }
//     }
// }

#[derive(Debug)]
pub struct LocalRwDarcWriteGuard<T: 'static> {
    pub(crate) _darc: LocalRwDarc<T>,
    pub(crate) lock: RwLockWriteGuardArc<T>,
}

impl<T: fmt::Display> fmt::Display for LocalRwDarcWriteGuard<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.lock, f)
    }
}

impl<T> std::ops::Deref for LocalRwDarcWriteGuard<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.lock
    }
}

impl<T> std::ops::DerefMut for LocalRwDarcWriteGuard<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.lock
    }
}

// impl<T> RwDarcGuard<LocalRwDarc<T>> for LocalRwDarcWriteGuard<T> {
//     type Guard = RwLockWriteGuardArc<T>;
//     fn new(darc: LocalRwDarc<T>, lock_guard: Self::Guard) -> Self {
//         LocalRwDarcWriteGuard {
//             darc,
//             lock: lock_guard,
//         }
//     }
// }

/// A local read-write `Darc`
///
/// Each PE maintains its own local read-write lock associated with the `LocalRwDarc`.
/// Whenever the interior object is accessed on a PE the local lock is required to be aquired.
/// When a thread aquires a Write lock it is guaranteed to the only thread with access to
/// the interior object with respect to the PE it is executing on (no guarantees are made about what is occuring on other PEs).
/// When a thread aquires a Read lock it may be one of many threads on the PE with access, but none of them will have mutable access.
/// - Contrast with a `GlobalRwDarc`, which has a single global lock.
/// - Contrast with a `Darc`, which also has local ownership but does not
///   allow modification unless the wrapped object itself provides it, e.g.
///   `AtomicUsize` or `Mutex<..>`.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct LocalRwDarc<T: 'static> {
    #[serde(
        serialize_with = "localrw_serialize2",
        deserialize_with = "localrw_from_ndarc2"
    )]
    pub(crate) darc: Darc<Arc<RwLock<T>>>, //we need to wrap WrLock in an Arc so we get access to ArcReadGuard and ArcWriteGuard
}

unsafe impl<T: Send> Send for LocalRwDarc<T> {} //we are protecting internally with an WrLock
unsafe impl<T: Send> Sync for LocalRwDarc<T> {} //we are protecting internally with an WrLock

impl<T> LamellarEnv for LocalRwDarc<T> {
    fn my_pe(&self) -> usize {
        self.darc.my_pe()
    }
    fn num_pes(&self) -> usize {
        self.darc.num_pes()
    }
    fn num_threads_per_pe(&self) -> usize {
        self.darc.num_threads_per_pe()
    }
    fn world(&self) -> Arc<LamellarTeam> {
        self.darc.world()
    }
    fn team(&self) -> Arc<LamellarTeam> {
        self.darc.team().team()
    }
}

impl<T> crate::active_messaging::DarcSerde for LocalRwDarc<T> {
    fn ser(&self, num_pes: usize, darcs: &mut Vec<RemotePtr>) {
        // println!("in local darc ser");
        // match cur_pe {
        //     Ok(cur_pe) => {
        self.darc.serialize_update_cnts(num_pes);
        // }
        // Err(err) => {
        //     panic!("can only access darcs within team members ({:?})", err);
        // }
        // }
        darcs.push(RemotePtr::NetworkDarc(self.darc.clone().into()));
    }
    fn des(&self, cur_pe: Result<usize, IdError>) {
        // match cur_pe {
        //     Ok(_) => {
        //         self.darc.deserialize_update_cnts();
        //     }
        //     Err(err) => {
        //         panic!("can only access darcs within team members ({:?})", err);
        //     }
        // }
    }
}

impl<T> LocalRwDarc<T> {
    pub(crate) fn inner(&self) -> &DarcInner<Arc<RwLock<T>>> {
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
        tracing::trace!(
            "localrwdarc[{:?}] deserialize_update_cnts {:?}",
            self.darc.id,
            self.inner()
        );
        self.inner().inc_pe_ref_count(self.darc.src_pe, 1); // we need to increment by 2 cause bincode calls the serialize function twice when serializing...
                                                            // }
        self.inner().local_cnt.fetch_add(1, Ordering::SeqCst);
        // self.print();
        // println!("done deserialize darc cnts");
    }

    #[doc(hidden)]
    pub fn print(&self) {
        println!(
            "--------\norig: {:?} 0x{:x} {:?}\n--------",
            self.darc.src_pe,
            self.darc.inner.addr(),
            self.inner()
        );
    }
}

impl<T: Sync + Send> LocalRwDarc<T> {
    #[doc(alias("One-sided", "onesided"))]
    /// Creates a handle for aquiring a reader lock of this LocalRwDarc local to this PE.
    /// The returned handle must either be await'd `.read().await` within an async context
    /// or it must be blocked on `.read().block()` in a non async context to actually acquire the lock
    ///
    /// After awaiting or blocking on the handle, a RAII guard is returned which will drop the read access of the wrlock when dropped
    ///
    /// # One-sided Operation
    /// The calling PE is only aware of its own local lock and does not require coordination with other PEs
    ///
    /// # Note
    /// the aquired lock is only with respect to this PE, the locks on the other PEs will be in their own states
    ///
    /// # Examples
    ///
    ///```
    /// use lamellar::darc::prelude::*;
    /// use lamellar::active_messaging::prelude::*;
    /// #[lamellar::AmData(Clone)]
    /// struct DarcAm {
    ///     counter: LocalRwDarc<usize>, //each pe has a local atomicusize
    /// }
    ///
    /// #[lamellar::am]
    /// impl LamellarAm for DarcAm {
    ///     async fn exec(self) {
    ///         let counter = self.counter.read().await; //block until we get the write lock
    ///         println!("the current counter value on pe {} = {}",lamellar::current_pe,counter);
    ///     }
    ///  }
    /// //-------------
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let counter = LocalRwDarc::new(&world, 0).block().unwrap();
    /// let _ = world.exec_am_all(DarcAm {counter: counter.clone()}).spawn();
    /// let guard = counter.read().block(); //we can also explicitly block on the lock in a non async context
    /// println!("the current counter value on pe {} main thread = {}",my_pe,*guard);
    ///```
    pub fn read(&self) -> LocalRwDarcReadHandle<T> {
        LocalRwDarcReadHandle::new(self.clone())
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Creates a handle for aquiring a writer lock of this LocalRwDarc local to this PE.
    /// The returned handle must either be await'd `.write().await` within an async context
    /// or it must be blocked on `.write().block()` in a non async context to actually acquire the lock
    ///
    /// After awaiting or blocking on the handle, a RAII guard is returned which will drop the write access of the wrlock when dropped
    ///
    /// # One-sided Operation
    /// The calling PE is only aware of its own local lock and does not require coordination with other PEs
    ///
    /// # Note
    /// the aquired lock is only with respect to this PE, the locks on the other PEs will be in their own states
    ///
    /// # Examples
    ///
    ///```
    /// use lamellar::darc::prelude::*;
    /// use lamellar::active_messaging::prelude::*;
    /// #[lamellar::AmData(Clone)]
    /// struct DarcAm {
    ///     counter: LocalRwDarc<usize>, //each pe has a local atomicusize
    /// }
    ///
    /// #[lamellar::am]
    /// impl LamellarAm for DarcAm {
    ///     async fn exec(self) {
    ///         let mut counter = self.counter.write().await; //block until we get the write lock
    ///         *counter += 1;
    ///     }
    ///  }
    /// //-------------
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let counter = LocalRwDarc::new(&world, 0).block().unwrap();
    /// let _ = world.exec_am_all(DarcAm {counter: counter.clone()}).spawn();
    /// let mut  guard = counter.write().block(); //we can also explicitly block on the lock in a non async context
    /// *guard += my_pe;
    /// println!("the current counter value on pe {} main thread = {}",my_pe,*guard);
    ///```
    pub fn write(&self) -> LocalRwDarcWriteHandle<T> {
        LocalRwDarcWriteHandle::new(self.clone())
    }

    #[doc(alias = "Collective")]
    /// Constructs a new `LocalRwDarc<T>` on the PEs specified by team.
    ///
    /// This is a blocking collective call amongst all PEs in the team, only returning once every PE in the team has completed the call.
    ///
    /// Returns an error if this PE is not a part of team
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `darc` to enter the call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    ///
    /// ```
    /// use lamellar::darc::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let five = LocalRwDarc::new(&world,5).block().expect("PE in world team");
    /// ```
    pub fn new<U: Into<IntoLamellarTeam>>(team: U, item: T) -> LocalRwDarcHandle<T> {
        // Ok(LocalRwDarc {
        //     darc: Darc::try_new(team, Arc::new(RwLock::new(item)), DarcMode::LocalRw)?,
        // })
        let team = team.into().team.clone();
        LocalRwDarcHandle {
            team: team.clone(),
            launched: false,
            creation_future: Box::pin(Darc::async_try_new_with_drop(
                team,
                Arc::new(RwLock::new(item)),
                DarcMode::LocalRw,
                None,
            )),
        }
    }

    // pub(crate) fn try_new<U: Into<IntoLamellarTeam>>(team: U, item: T) -> Result<LocalRwDarc<T>, IdError> {
    //     Ok(LocalRwDarc {
    //         darc: Darc::try_new(
    //             team,
    //             Arc::new(RwLock::new(Box::new(item))),
    //             DarcMode::LocalRw,
    //         )?,
    //     })
    // }

    #[doc(alias = "Collective")]
    /// Converts this LocalRwDarc into a [GlobalRwDarc]
    ///
    /// This returns a handle (which is Future) thats needs to be `awaited` or `blocked` on to perform the operation.
    /// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the Darc's team, only returning once every PE in the team has completed the call.
    ///
    /// Furthermore, the handle will not return while any additional references outside of the one making this call exist on each PE. It is not possible for the
    /// pointed to object to wrapped by both a Darc and a LocalRwDarc simultaneously (on any PE).
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `darc` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    /// ```
    /// use lamellar::darc::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let five = LocalRwDarc::new(&world,5).block().expect("PE in world team");
    /// let five_as_globaldarc = world.block_on(async move {five.into_globalrw().await});
    /// ```
    pub fn into_globalrw(self) -> IntoGlobalRwDarcHandle<T> {
        // let wrapped_inner = WrappedInner {
        //     inner: NonNull::new(self.darc.inner as *mut DarcInner<T>)
        //         .expect("invalid darc pointer"),
        // };
        let inner = self.darc.inner.clone();
        let team = self.darc.inner().team().clone();
        IntoGlobalRwDarcHandle {
            darc: self.into(),
            team,
            launched: false,
            outstanding_future: Box::pin(DarcInner::block_on_outstanding(
                inner,
                DarcMode::GlobalRw,
                0,
            )),
        }
    }
}

impl<T: Send + Sync> LocalRwDarc<T> {
    #[doc(alias = "Collective")]
    /// Converts this LocalRwDarc into a regular [Darc]
    ///
    /// This returns a handle (which is Future) thats needs to be `awaited` or `blocked` on to perform the operation.
    /// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the Darc's team, only returning once every PE in the team has completed the call.
    ///
    /// Furthermore, the handle will not return while any additional references outside of the one making this call exist on each PE. It is not possible for the
    /// pointed to object to wrapped by both a Darc and a LocalRwDarc simultaneously (on any PE).
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `darc` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    /// ```
    /// use lamellar::darc::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let five = LocalRwDarc::new(&world,5).block().expect("PE in world team");
    /// let five_as_darc = five.into_darc().block();
    /// ```
    pub fn into_darc(self) -> IntoDarcHandle<T> {
        // let wrapped_inner = WrappedInner {
        //     inner: NonNull::new(self.darc.inner as *mut DarcInner<T>)
        //         .expect("invalid darc pointer"),
        // };
        let inner = self.darc.inner.clone();
        let team = self.darc.inner().team().clone();
        IntoDarcHandle {
            darc: self.into(),
            team,
            launched: false,
            outstanding_future: Box::pin(async move {
                DarcInner::block_on_outstanding(inner, DarcMode::Darc, 0).await;
            }),
        }
    }
}

impl<T> Clone for LocalRwDarc<T> {
    fn clone(&self) -> Self {
        // self.inner().local_cnt.fetch_add(1,Ordering::SeqCst);
        tracing::trace!(
            "LocalRwDarc[{:?}] Clone {:?}",
            self.darc.id,
            self.darc.inner()
        );
        LocalRwDarc {
            darc: self.darc.clone(),
        }
    }
}

impl<T: fmt::Display + Sync + Send> fmt::Display for LocalRwDarc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let lock: LocalRwDarc<T> = self.clone();
        fmt::Display::fmt(&lock.read().block(), f)
    }
}

// //#[doc(hidden)]
// pub fn localrw_serialize<S, T>(localrw: &LocalRwDarc<T>, s: S) -> Result<S::Ok, S::Error>
// where
//     S: Serializer,
// {
//     __NetworkDarc::<T>::from(&localrw.darc).serialize(s)
// }

// //#[doc(hidden)]
// pub fn localrw_from_ndarc<'de, D, T>(deserializer: D) -> Result<LocalRwDarc<T>, D::Error>
// where
//     D: Deserializer<'de>,
// {
//     // println!("lrwdarc1 from net darc");
//     let ndarc: __NetworkDarc<T> = Deserialize::deserialize(deserializer)?;
//     let rwdarc = LocalRwDarc {
//         darc: Darc::from(ndarc),
//     };
//     // println!("lrwdarc from net darc");
//     // rwdarc.print();
//     Ok(rwdarc)
// }

//#[doc(hidden)]
pub(crate) fn localrw_serialize2<S, T>(
    localrw: &Darc<Arc<RwLock<T>>>,
    s: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // __NetworkDarc::<T>::from(localrw).serialize(s)
    let ndarc = __NetworkDarc::from(localrw);
    // println!("ndarc size {:?} {:?}",crate::serialized_size(&ndarc,false),crate::serialized_size(&ndarc,true));
    ndarc.serialize(s)
}

//#[doc(hidden)]
// #[allow(unreachable_pub)]
pub(crate) fn localrw_from_ndarc2<'de, D, T>(
    deserializer: D,
) -> Result<Darc<Arc<RwLock<T>>>, D::Error>
where
    D: Deserializer<'de>,
{
    tracing::trace!("lrwdarc2 from net darc");
    let ndarc: __NetworkDarc = Deserialize::deserialize(deserializer)?;
    // let rwdarc = LocalRwDarc {
    //     darc: ,
    // };
    // println!("lrwdarc from net darc");
    // println!("ndarc {:?}",ndarc);
    Ok(Darc::from(ndarc))
}

// impl<T> From<Darc<Arc<RwLock<T>>>> for __NetworkDarc {
//     fn from(darc: Darc<Arc<RwLock<T>>>) -> Self {
//         // println!("rwdarc to net darc");
//         // darc.print();
//         let team = &darc.inner().team();
//         let ndarc = __NetworkDarc {
//             inner_addr: darc.inner as *const u8 as usize,
//             backend: team.lamellae.comm().backend(),
//             orig_world_pe: team.world_pe,
//             orig_team_pe: team.team_pe.expect("darcs only valid on team members"),
//         };
//         ndarc
//     }
// }

// impl<T> From<&Darc<Arc<RwLock<T>>>> for __NetworkDarc {
//     fn from(darc: &Darc<Arc<RwLock<T>>>) -> Self {
//         // println!("rwdarc to net darc");
//         // darc.print();
//         let team = &darc.inner().team();
//         let ndarc = __NetworkDarc {
//             inner_addr: darc.inner as *const u8 as usize,
//             backend: team.lamellae.comm().backend(),
//             orig_world_pe: team.world_pe,
//             orig_team_pe: team.team_pe.expect("darcs only valid on team members"),
//         };
//         ndarc
//     }
// }

// impl<T> From<__NetworkDarc> for Darc<Arc<RwLock<T>>> {
//     fn from(ndarc: __NetworkDarc) -> Self {
//         // println!("rwdarc from net darc");

//         if let Some(lamellae) = LAMELLAES.read().get(&ndarc.backend) {
//             let darc = Darc {
//                 inner: lamellae.comm().local_addr(ndarc.orig_world_pe, ndarc.inner_addr)
//                     as *mut DarcInner<Arc<RwLock<T>>>,
//                 src_pe: ndarc.orig_team_pe,
//                 // phantom: PhantomData,
//             };
//             darc
//         } else {
//             panic!("unexepected lamellae backend {:?}", &ndarc.backend);
//         }
//     }
// }

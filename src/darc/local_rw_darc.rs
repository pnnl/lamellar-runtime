use core::marker::PhantomData;
use parking_lot::{
    lock_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard},
    RawRwLock, RwLock,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::darc::global_rw_darc::{DistRwLock, GlobalRwDarc};
use crate::darc::{Darc, DarcInner, DarcMode, __NetworkDarc};
use crate::lamellae::{LamellaeComm, LamellaeRDMA};
use crate::lamellar_team::IntoLamellarTeam;
use crate::lamellar_world::LAMELLAES;
use crate::IdError;

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
    pub(crate) darc: Darc<Arc<RwLock<Box<T>>>>, //we need to wrap WrLock in an Arc so we get access to ArcReadGuard and ArcWriteGuard
}

unsafe impl<T: Send> Send for LocalRwDarc<T> {}
unsafe impl<T: Sync> Sync for LocalRwDarc<T> {}

impl<T> crate::DarcSerde for LocalRwDarc<T> {
    fn ser(&self, num_pes: usize) {
        // println!("in local darc ser");
        // match cur_pe {
        //     Ok(cur_pe) => {
        self.darc.serialize_update_cnts(num_pes);
        // }
        // Err(err) => {
        //     panic!("can only access darcs within team members ({:?})", err);
        // }
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

impl<T> LocalRwDarc<T> {
    fn inner(&self) -> &DarcInner<Arc<RwLock<Box<T>>>> {
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
        self.inner().local_cnt.fetch_add(1, Ordering::SeqCst);
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

    /// Aquires a reader lock of this LocalRwDarc local to this PE.
    ///
    /// The current THREAD will be blocked until the lock has been acquired.
    ///
    /// This function will not return while any writer currentl has access to the lock
    ///
    /// Returns an RAII guard which will drop the read access of the wrlock when dropped
    ///
    /// # Note
    /// the aquired lock is only with respect to this PE, the locks on the other PEs will be in their own states
    ///
    /// # Examples
    ///
    ///```
    /// use lamellar::{ActiveMessaging, LocalRwDarc};
    /// #[lamellar::AmData(Clone)]
    /// struct DarcAm {
    ///     counter: LocalRwDarc<usize>, //each pe has a local atomicusize
    /// }
    ///
    /// #[lamellar::am]
    /// impl LamellarAm for DarcAm {
    ///     async fn exec(self) {
    ///         let counter = self.counter.read(); //block until we get the write lock
    ///         println!("the current counter value on pe {} = {}",lamellar::current_pe,counter);
    ///     }
    ///  }
    /// -------------
    /// let counter = GlobalRwDarc::new(world, 0).unwrap();
    /// world.exec_am_all(DarcAm {counter: counter.clone()});
    /// let guard = world.block_on(counter.async_read());
    /// println!("the current counter value on pe {} main thread = {}",my_pe,guard);
    ///```
    pub fn read(&self) -> ArcRwLockReadGuard<RawRwLock, Box<T>> {
        self.darc.read_arc()
    }

    /// Aquires the writer lock of this LocalRwDarc local to this PE.
    ///
    /// The current THREAD will be blocked until the lock has been acquired.
    ///
    /// This function will not return while another writer or any readers currently have access to the lock
    ///
    /// Returns an RAII guard which will drop the write access of the wrlock when dropped
    ///
    /// # Note
    /// the aquired lock is only with respect to this PE, the locks on the other PEs will be in their own states
    ///
    /// # Examples
    ///
    ///```
    /// use lamellar::{ActiveMessaging, LocalRwDarc};
    /// #[lamellar::AmData(Clone)]
    /// struct DarcAm {
    ///     counter: LocalRwDarc<usize>, //each pe has a local atomicusize
    /// }
    ///
    /// #[lamellar::am]
    /// impl LamellarAm for DarcAm {
    ///     async fn exec(self) {
    ///         let counter = self.counter.write(); //block until we get the write lock
    ///        counter += 1;
    ///     }
    ///  }
    /// -------------
    /// let counter = GlobalRwDarc::new(world, 0).unwrap();
    /// world.exec_am_all(DarcAm {counter: counter.clone()});
    /// let guard = counter.write();
    /// guard += my_pe;
    ///```
    pub fn write(&self) -> ArcRwLockWriteGuard<RawRwLock, Box<T>> {
        self.darc.write_arc()
    }
}

impl<T> LocalRwDarc<T> {
    /// Constructs a new `LocalRwDarc<T>` on the PEs specified by team.
    ///
    /// This is a blocking collective call amongst all PEs in the team, only returning once every PE in the team has completed the call.
    ///
    /// Returns an error if this PE is not a part of team
    ///
    /// # Examples
    ///
    /// ```
    /// use lamellar::LocalRwDarc;
    ///
    /// let five = LocalRwDarc::new(5);
    /// ```
    pub fn new<U: Into<IntoLamellarTeam>>(team: U, item: T) -> Result<LocalRwDarc<T>, IdError> {
        Ok(LocalRwDarc {
            darc: Darc::try_new(
                team,
                Arc::new(RwLock::new(Box::new(item))),
                DarcMode::LocalRw,
            )?,
        })
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

    /// Converts this LocalRwDarc into a regular [Darc]
    ///
    /// This is a blocking collective call amongst all PEs in the LocalRwDarc's team, only returning once every PE in the team has completed the call.
    ///
    /// Furthermore, this call will block while any additional references outside of the one making this call exist on each PE. It is not possible for the
    /// pointed to object to wrapped by both a Darc and a LocalRwDarc simultaneously (on any PE).
    ///
    /// # Example
    /// ```
    /// use lamellar::{Darc,LocalRwDarc};
    ///
    /// let five = LocalRwDarc::new(5);
    /// let five_as_darc = five.into_darc();
    /// ```
    pub fn into_darc(self) -> Darc<T> {
        let inner = self.inner();
        // println!("into_darc");
        // self.print();
        inner.block_on_outstanding(DarcMode::Darc, 0);
        // println!("after block on outstanding");
        inner.local_cnt.fetch_add(1, Ordering::SeqCst); //we add this here because to account for moving inner into d
                                                        // let item = unsafe { Box::from_raw(inner.item as *mut Arc<RwLock<Box<T>>>).into_inner() };
        let mut arc_item =
            unsafe { (*Box::from_raw(inner.item as *mut Arc<RwLock<Box<T>>>)).clone() };

        let item: Box<T> = loop {
            arc_item = match Arc::try_unwrap(arc_item) {
                Ok(item) => break item.into_inner(),
                Err(arc_item) => arc_item,
            };
        };
        let d = Darc {
            inner: self.darc.inner as *mut DarcInner<T>,
            src_pe: self.darc.src_pe,
            // phantom: PhantomData,
        };
        d.inner_mut().update_item(Box::into_raw(item));
        d
    }

    /// Converts this LocalRwDarc into a [GlobalRwDarc]
    ///
    /// This is a blocking collective call amongst all PEs in the LocalRwDarc's team, only returning once every PE in the team has completed the call.
    ///
    /// Furthermore, this call will block while any additional references outside of the one making this call exist on each PE. It is not possible for the
    /// pointed to object to wrapped by both a GlobalRwDarc and a LocalRwDarc simultaneously (on any PE).
    ///
    /// # Example
    /// ```
    /// use lamellar::{GlobalRwDarc,LocalRwDarc};
    ///
    /// let five = LocalRwDarc::new(5);
    /// let five_as_globaldarc = five.into_globalrw();
    /// ```
    pub fn into_globalrw(self) -> GlobalRwDarc<T> {
        let inner = self.inner();
        // println!("into_darc");
        // self.print();
        inner.block_on_outstanding(DarcMode::GlobalRw, 0);
        // println!("after block on outstanding");
        inner.local_cnt.fetch_add(1, Ordering::SeqCst); //we add this here because to account for moving inner into d
        let mut arc_item =
            unsafe { (*Box::from_raw(inner.item as *mut Arc<RwLock<Box<T>>>)).clone() };
        let item: Box<T> = loop {
            arc_item = match Arc::try_unwrap(arc_item) {
                Ok(item) => break item.into_inner(),
                Err(arc_item) => arc_item,
            };
        };
        let d = Darc {
            inner: self.darc.inner as *mut DarcInner<DistRwLock<T>>,
            src_pe: self.darc.src_pe,
            // phantom: PhantomData,
        };
        d.inner_mut()
            .update_item(Box::into_raw(Box::new(DistRwLock::new(
                *item,
                self.inner().team(),
            ))));
        GlobalRwDarc { darc: d }
    }
}

impl<T> Clone for LocalRwDarc<T> {
    fn clone(&self) -> Self {
        // self.inner().local_cnt.fetch_add(1,Ordering::SeqCst);
        LocalRwDarc {
            darc: self.darc.clone(),
        }
    }
}

impl<T: fmt::Display> fmt::Display for LocalRwDarc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self.read(), f)
    }
}

#[doc(hidden)]
pub fn localrw_serialize<S, T>(localrw: &LocalRwDarc<T>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    __NetworkDarc::<T>::from(&localrw.darc).serialize(s)
}

#[doc(hidden)]
pub fn localrw_serialize2<S, T>(
    localrw: &Darc<Arc<RwLock<Box<T>>>>,
    s: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // __NetworkDarc::<T>::from(localrw).serialize(s)
    let ndarc = __NetworkDarc::<T>::from(localrw);
    // println!("ndarc size {:?} {:?}",crate::serialized_size(&ndarc,false),crate::serialized_size(&ndarc,true));
    ndarc.serialize(s)
}

#[doc(hidden)]
pub fn localrw_from_ndarc<'de, D, T>(deserializer: D) -> Result<LocalRwDarc<T>, D::Error>
where
    D: Deserializer<'de>,
{
    // println!("lrwdarc1 from net darc");
    let ndarc: __NetworkDarc<T> = Deserialize::deserialize(deserializer)?;
    let rwdarc = LocalRwDarc {
        darc: Darc::from(ndarc),
    };
    // println!("lrwdarc from net darc");
    // rwdarc.print();
    Ok(rwdarc)
}

#[doc(hidden)]
pub fn localrw_from_ndarc2<'de, D, T>(
    deserializer: D,
) -> Result<Darc<Arc<RwLock<Box<T>>>>, D::Error>
where
    D: Deserializer<'de>,
{
    // println!("lrwdarc2 from net darc");
    let ndarc: __NetworkDarc<T> = Deserialize::deserialize(deserializer)?;
    // let rwdarc = LocalRwDarc {
    //     darc: ,
    // };
    // println!("lrwdarc from net darc");
    // println!("ndarc {:?}",ndarc);
    Ok(Darc::from(ndarc))
}

impl<T> From<Darc<Arc<RwLock<Box<T>>>>> for __NetworkDarc<T> {
    fn from(darc: Darc<Arc<RwLock<Box<T>>>>) -> Self {
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

impl<T> From<&Darc<Arc<RwLock<Box<T>>>>> for __NetworkDarc<T> {
    fn from(darc: &Darc<Arc<RwLock<Box<T>>>>) -> Self {
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

impl<T> From<__NetworkDarc<T>> for Darc<Arc<RwLock<Box<T>>>> {
    fn from(ndarc: __NetworkDarc<T>) -> Self {
        // println!("rwdarc from net darc");

        if let Some(lamellae) = LAMELLAES.read().get(&ndarc.backend) {
            let darc = Darc {
                inner: lamellae.local_addr(ndarc.orig_world_pe, ndarc.inner_addr)
                    as *mut DarcInner<Arc<RwLock<Box<T>>>>,
                src_pe: ndarc.orig_team_pe,
                // phantom: PhantomData,
            };
            darc
        } else {
            panic!("unexepected lamellae backend {:?}", &ndarc.backend);
        }
    }
}

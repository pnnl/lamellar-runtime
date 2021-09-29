use core::marker::PhantomData;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::darc::global_rw_darc::{DistRwLock, GlobalRwDarc};
use crate::darc::{Darc, DarcInner, DarcMode, __NetworkDarc};
use crate::lamellae::{LamellaeComm, LamellaeRDMA};
use crate::lamellar_world::LAMELLAES;
use crate::IdError;
use crate::LamellarTeam;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct LocalRwDarc<T: 'static> {
    #[serde(
        serialize_with = "localrw_serialize2",
        deserialize_with = "localrw_from_ndarc2"
    )]
    pub(crate) darc: Darc<RwLock<Box<T>>>,
}

unsafe impl<T: Sync + Send> Send for LocalRwDarc<T> {}
unsafe impl<T: Sync + Send> Sync for LocalRwDarc<T> {}

impl<T> crate::DarcSerde for LocalRwDarc<T> {
    fn ser(&self, num_pes: usize, cur_pe: Result<usize, IdError>) {
        // println!("in local darc ser");
        match cur_pe {
            Ok(cur_pe) => {
                self.darc.serialize_update_cnts(num_pes, cur_pe);
            }
            Err(err) => {
                panic!("can only access darcs within team members ({:?})", err);
            }
        }
    }
    fn des(&self, cur_pe: Result<usize, IdError>) {
        match cur_pe {
            Ok(cur_pe) => {
                self.darc.deserialize_update_cnts(cur_pe);
            }
            Err(err) => {
                panic!("can only access darcs within team members ({:?})", err);
            }
        }
    }
}

impl<T> LocalRwDarc<T> {
    fn inner(&self) -> &DarcInner<RwLock<Box<T>>> {
        self.darc.inner()
    }

    pub fn serialize_update_cnts(&self, cnt: usize, _cur_pe: usize) {
        // println!("serialize darc cnts");
        // if self.darc.src_pe == cur_pe{
        self.inner()
            .dist_cnt
            .fetch_add(cnt, std::sync::atomic::Ordering::SeqCst);
        // }
        // self.print();
        // println!("done serialize darc cnts");
    }

    pub fn deserialize_update_cnts(&self, _cur_pe: usize) {
        // println!("deserialize darc? cnts");
        // if self.darc.src_pe != cur_pe{
        self.inner().inc_pe_ref_count(self.darc.src_pe, 1); // we need to increment by 2 cause bincode calls the serialize function twice when serializing...
                                                            // }
        self.inner().local_cnt.fetch_add(1, Ordering::SeqCst);
        // self.print();
        // println!("done deserialize darc cnts");
    }

    pub fn print(&self) {
        let rel_addr =
            unsafe { self.darc.inner as usize - (*self.inner().team).team.lamellae.base_addr() };
        println!(
            "--------\norig: {:?} {:?} (0x{:x}) {:?}\n--------",
            self.darc.src_pe,
            self.darc.inner,
            rel_addr,
            self.inner()
        );
    }

    pub fn read(&self) -> RwLockReadGuard<Box<T>> {
        self.darc.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<Box<T>> {
        self.darc.write()
    }
}

impl<T> LocalRwDarc<T> {
    pub fn new(team: Arc<LamellarTeam>, item: T) -> Result<LocalRwDarc<T>, IdError> {
        Ok(LocalRwDarc {
            darc: Darc::try_new(team, RwLock::new(Box::new(item)), DarcMode::LocalRw)?,
        })
    }

    pub fn try_new(team: Arc<LamellarTeam>, item: T) -> Result<LocalRwDarc<T>, IdError> {
        Ok(LocalRwDarc {
            darc: Darc::try_new(team, RwLock::new(Box::new(item)), DarcMode::LocalRw)?,
        })
    }

    pub fn into_darc(self) -> Darc<T> {
        let inner = self.inner();
        // println!("into_darc");
        // self.print();
        inner.block_on_outstanding(DarcMode::Darc);
        // println!("after block on outstanding");
        inner.local_cnt.fetch_add(1, Ordering::SeqCst);
        let item = unsafe { Box::from_raw(inner.item as *mut RwLock<Box<T>>).into_inner() };
        let d = Darc {
            inner: self.darc.inner as *mut DarcInner<T>,
            src_pe: self.darc.src_pe,
            // phantom: PhantomData,
        };
        d.inner_mut().update_item(Box::into_raw(item));
        d
    }

    pub fn into_globalrw(self) -> GlobalRwDarc<T> {
        let inner = self.inner();
        // println!("into_darc");
        // self.print();
        inner.block_on_outstanding(DarcMode::GlobalRw);
        // println!("after block on outstanding");
        inner.local_cnt.fetch_add(1, Ordering::SeqCst);
        let item = unsafe { Box::from_raw(inner.item as *mut RwLock<Box<T>>).into_inner() };
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

pub fn localrw_serialize<S, T>(localrw: &LocalRwDarc<T>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    __NetworkDarc::<T>::from(&localrw.darc).serialize(s)
}

pub fn localrw_serialize2<S, T>(localrw: &Darc<RwLock<Box<T>>>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    __NetworkDarc::<T>::from(localrw).serialize(s)
}

pub fn localrw_from_ndarc<'de, D, T>(deserializer: D) -> Result<LocalRwDarc<T>, D::Error>
where
    D: Deserializer<'de>,
{
    let ndarc: __NetworkDarc<T> = Deserialize::deserialize(deserializer)?;
    // println!("lrwdarc from net darc");
    let rwdarc = LocalRwDarc {
        darc: Darc::from(ndarc),
    };
    // println!("lrwdarc from net darc");
    // rwdarc.print();
    Ok(rwdarc)
}

pub fn localrw_from_ndarc2<'de, D, T>(deserializer: D) -> Result<Darc<RwLock<Box<T>>>, D::Error>
where
    D: Deserializer<'de>,
{
    let ndarc: __NetworkDarc<T> = Deserialize::deserialize(deserializer)?;
    // println!("lrwdarc from net darc");
    // let rwdarc = LocalRwDarc {
    //     darc: ,
    // };
    // println!("lrwdarc from net darc");
    // rwdarc.print();
    Ok(Darc::from(ndarc))
}

impl<T> From<Darc<RwLock<Box<T>>>> for __NetworkDarc<T> {
    fn from(darc: Darc<RwLock<Box<T>>>) -> Self {
        // println!("rwdarc to net darc");
        // darc.print();
        let team = &darc.inner().team().team;
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

impl<T> From<&Darc<RwLock<Box<T>>>> for __NetworkDarc<T> {
    fn from(darc: &Darc<RwLock<Box<T>>>) -> Self {
        // println!("rwdarc to net darc");
        // darc.print();
        let team = &darc.inner().team().team;
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

impl<T> From<__NetworkDarc<T>> for Darc<RwLock<Box<T>>> {
    fn from(ndarc: __NetworkDarc<T>) -> Self {
        // println!("rwdarc from net darc");

        if let Some(lamellae) = LAMELLAES.read().get(&ndarc.backend) {
            let darc = Darc {
                inner: lamellae.local_addr(ndarc.orig_world_pe, ndarc.inner_addr)
                    as *mut DarcInner<RwLock<Box<T>>>,
                src_pe: ndarc.orig_team_pe,
                // phantom: PhantomData,
            };
            darc
        } else {
            panic!("unexepected lamellae backend {:?}", &ndarc.backend);
        }
    }
}

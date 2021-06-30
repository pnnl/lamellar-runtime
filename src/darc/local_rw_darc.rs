
use core::marker::PhantomData;
use parking_lot::{RwLock,RwLockReadGuard, RwLockWriteGuard};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::sync::atomic::Ordering;
use std::sync::Arc;


use crate::lamellar_world::LAMELLAES;
use crate::LamellarTeam;
use crate::lamellae::{LamellaeComm, LamellaeRDMA};
use crate::IdError;
use crate::darc::{Darc,DarcInner,DarcMode,__NetworkDarc};
use crate::darc::global_rw_darc::{GlobalRwDarc,DistRwLock};



#[derive(Debug)]
pub struct LocalRwDarc<T: 'static + ?Sized> {
    pub(crate) darc: Darc<RwLock<Box<T>>>,
}

unsafe impl<T: ?Sized + Sync + Send> Send for LocalRwDarc<T> {}
unsafe impl<T: ?Sized + Sync + Send> Sync for LocalRwDarc<T> {}

impl<T: ?Sized> LocalRwDarc<T> {
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
            phantom: PhantomData,
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
            phantom: PhantomData,
        };
        d.inner_mut().update_item(Box::into_raw(Box::new(DistRwLock::new(*item,self.inner().team()))));
        GlobalRwDarc{
            darc: d
        }
    }
}

impl<T: ?Sized> Clone for LocalRwDarc<T> {
    fn clone(&self) -> Self {
        // self.inner().local_cnt.fetch_add(1,Ordering::SeqCst);
        LocalRwDarc {
            darc: self.darc.clone(),
        }
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for LocalRwDarc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self.read(), f)
    }
}

pub fn localrw_serialize<S, T>(localrw: &LocalRwDarc<T>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: ?Sized,
{
    __NetworkDarc::<T>::from(&localrw.darc).serialize(s)
}

pub fn localrw_from_ndarc<'de, D, T>(deserializer: D) -> Result<LocalRwDarc<T>, D::Error>
where
    D: Deserializer<'de>,
    T: ?Sized,
{
    let ndarc: __NetworkDarc<T> = Deserialize::deserialize(deserializer)?;
    let rwdarc = LocalRwDarc {
        darc: Darc::from(ndarc),
    };
    // println!("lrwdarc from net darc");
    // rwdarc.print();
    Ok(rwdarc)
}

impl<T: ?Sized> From<&Darc<RwLock<Box<T>>>> for __NetworkDarc<T> {
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

impl<T: ?Sized> From<__NetworkDarc<T>> for Darc<RwLock<Box<T>>> {
    fn from(ndarc: __NetworkDarc<T>) -> Self {
        // println!("rwdarc from net darc");

        if let Some(lamellae) = LAMELLAES.read().get(&ndarc.backend) {
            let darc = Darc {
                inner: lamellae.local_addr(ndarc.orig_world_pe, ndarc.inner_addr)
                    as *mut DarcInner<RwLock<Box<T>>>,
                src_pe: ndarc.orig_team_pe,
                phantom: PhantomData,
            };
            // if !DARC_ADDRS.read().contains(&ndarc.inner_addr)
            //     && !DARC_ADDRS.read().contains(&(darc.inner as usize))
            // {
            //     println!(
            //         "wtf! 0x{:x} -- 0x{:x} (0x{:x}) ",
            //         ndarc.inner_addr,
            //         darc.inner as usize,
            //         darc.inner as usize - lamellae.base_addr()
            //     );
            //     for addr in DARC_ADDRS.read().iter() {
            //         println!("0x{:x} (0x{:x}) ", addr, addr - lamellae.base_addr());
            //     }
            // }
            // panic!("just for debugging");
            // darc.print();
            darc
        } else {
            panic!("unexepected lamellae backend {:?}", &ndarc.backend);
        }
    }
}

use core::marker::PhantomData;
use parking_lot::{Mutex, RwLock };
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::sync::atomic::{AtomicUsize,Ordering};
use std::sync::Arc;
use std::ops::{Deref,DerefMut};

use crate::active_messaging::ActiveMessaging;
use crate::lamellar_world::LAMELLAES;
use crate::LamellarTeam;
use crate::lamellae::{LamellaeComm, LamellaeRDMA};
use crate::IdError;
use crate::darc::{Darc,DarcInner,DarcMode,__NetworkDarc};
use crate::darc::local_rw_darc::{LocalRwDarc};

#[derive( serde::Serialize, serde::Deserialize, Debug)]
enum LockType{
    Read,
    Write,
}

#[derive(Debug)]
pub (crate) struct DistRwLock<T: ?Sized>{
    readers: AtomicUsize,
    writer: AtomicUsize,
    local_cnt: AtomicUsize,
    local_state: Mutex<Option<LockType>>,
    team: Arc<LamellarTeam>,
    data: std::cell::UnsafeCell<T>,
}


unsafe impl<T: ?Sized + Sync + Send> Send for DistRwLock<T> {}
unsafe impl<T: ?Sized + Sync + Send> Sync for DistRwLock<T> {}



/// # Safety
///
/// The lock is assumed to be allocated in rdma able memory (so we can look up the corresponding address on a remote pe).
/// since this is intended to be wrapped by a Darc there should be no issues
/// usage outside of a darc is undefined
impl<T> DistRwLock<T>{
    pub (crate) fn new(data: T, team: Arc<LamellarTeam>) ->DistRwLock<T>{
        DistRwLock{
            readers: AtomicUsize::new(0),
            writer: AtomicUsize::new(team.team.num_pes),
            local_cnt: AtomicUsize::new(0),
            local_state: Mutex::new(None),
            team: team,
            data: std::cell::UnsafeCell::new(data),
        }
    }
    fn into_inner(self) -> T {
        self.data.into_inner()
    }
}
impl<T: ?Sized> DistRwLock<T>{
    
    async fn async_reader_lock(&self, _pe: usize) {
        loop{
            while self.writer.load(Ordering::SeqCst) != self.team.team.num_pes { async_std::task::yield_now().await;}
            // println!("\t{:?} inc read count {:?} {:?}",pe,self.readers.load(Ordering::SeqCst),self.writer.load(Ordering::SeqCst));
            self.readers.fetch_add(1,Ordering::SeqCst);
            if self.writer.load(Ordering::SeqCst) == self.team.team.num_pes { break; }
            self.readers.fetch_sub(1,Ordering::SeqCst);
            // println!("\t{:?} writers exist dec read count {:?} {:?}",pe,self.readers.load(Ordering::SeqCst),self.writer.load(Ordering::SeqCst));
        }
        // println!("\t{:?} read locked {:?} {:?}",pe,self.readers.load(Ordering::SeqCst),self.writer.load(Ordering::SeqCst));
    }
    async fn async_writer_lock(&self, pe: usize){
        while let Err(_) = self.writer.compare_exchange(self.team.team.num_pes,pe,Ordering::SeqCst,Ordering::SeqCst){ async_std::task::yield_now().await; }
        // println!("\t{:?} write lock checking for readers {:?} {:?}",pe,self.readers.load(Ordering::SeqCst),self.writer.load(Ordering::SeqCst));
        while self.readers.load(Ordering::SeqCst) != 0 { async_std::task::yield_now().await; }
        // println!("\t{:?} write locked {:?} {:?}",pe,self.readers.load(Ordering::SeqCst),self.writer.load(Ordering::SeqCst));
    }

    /// # Safety
    ///
    /// The lock must be held when calling this method.
    unsafe fn reader_unlock(&self, _pe: usize) {
        // println!("\t{:?} reader unlocking  {:?} {:?}",pe,self.readers.load(Ordering::SeqCst),self.writer.load(Ordering::SeqCst));
        self.readers.fetch_sub(1,Ordering::SeqCst);
        // println!("\t{:?} reader unlocked  {:?} {:?}",pe,self.readers.load(Ordering::SeqCst),self.writer.load(Ordering::SeqCst));
    }
    /// # Safety
    ///
    /// The lock must be held when calling this method.
    unsafe fn writer_unlock(&self, pe: usize){
        // println!("\t{:?} writer unlocking {:?} {:?}",pe,self.readers.load(Ordering::SeqCst),self.writer.load(Ordering::SeqCst));
        if let Err(val) = self.writer.compare_exchange(pe,self.team.team.num_pes,Ordering::SeqCst,Ordering::SeqCst) {
            panic!("should not be trying to unlock another pes lock {:?} {:?}",pe, val);
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
    fn exec() {
        let lock = {
            let rwlock = unsafe { &*(self.rwlock_addr as *mut DarcInner<DistRwLock<()>>) }; //we dont actually care about the "type" we wrap here, we just need access to the meta data for the darc

            match self.lock_type{
                LockType::Read => futures::future::Either::Left(rwlock.item().async_reader_lock(self.orig_pe)),
                LockType::Write => futures::future::Either::Right(rwlock.item().async_writer_lock(self.orig_pe)),
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
    fn exec() {
        let rwlock = unsafe { &*(self.rwlock_addr as *mut DarcInner<DistRwLock<()>>) }; //we dont actually care about the "type" we wrap here, we just need access to the meta data for the darc
        unsafe {
            match self.lock_type{
                LockType::Read => rwlock.item().reader_unlock(self.orig_pe),
                LockType::Write => rwlock.item().writer_unlock(self.orig_pe),
            }
        }
    }
}



pub struct GlobalRwDarcReadGuard<'a, T: 'static + ?Sized>{
    rwlock:  Darc<DistRwLock<T>>, 
    marker: PhantomData<&'a mut T>,
}
impl<'a, T: ?Sized + 'a> Deref for GlobalRwDarcReadGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { & *self.rwlock.data.get() }
    }
}

impl<'a, T: ?Sized + 'a> Drop for GlobalRwDarcReadGuard<'a, T> {
    fn drop(&mut self){
        // println!("dropping read guard");
        let inner =self.rwlock.inner();
        let team = inner.team();
        let remote_rwlock_addr = team.team.lamellae.remote_addr(0,inner as *const DarcInner<DistRwLock<T>> as *const () as usize);
        team.exec_am_pe(0, UnlockAm{rwlock_addr: remote_rwlock_addr, orig_pe: team.team.team_pe.expect("darcs cant exist on non team members"), lock_type: LockType::Read});
    }
}

//TODO update this so that we print locked if data is locked...
impl<T: ?Sized + fmt::Debug> fmt::Debug for GlobalRwDarcReadGuard<'_,T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unsafe{
            fmt::Debug::fmt(&self.rwlock.data.get().as_ref(),f)
        }
    }
}

pub struct GlobalRwDarcWriteGuard<'a, T: 'static + ?Sized>{
    rwlock: Darc<DistRwLock<T>>,
    marker: PhantomData<&'a mut T>,
}

impl<'a, T: ?Sized + 'a> Deref for GlobalRwDarcWriteGuard<'a, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        unsafe { & *self.rwlock.data.get()}
    }
}

impl<'a, T: ?Sized + 'a> DerefMut for GlobalRwDarcWriteGuard<'a, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.rwlock.data.get() }
    }
}

impl<'a, T: ?Sized + 'a> Drop for GlobalRwDarcWriteGuard<'a, T> {
    fn drop(&mut self){
        // println!("dropping write guard");
        let inner =self.rwlock.inner();
        let team = inner.team();
        let remote_rwlock_addr = team.team.lamellae.remote_addr(0,inner as *const DarcInner<DistRwLock<T>> as *const () as usize);
        team.exec_am_pe(0, UnlockAm{rwlock_addr: remote_rwlock_addr, orig_pe: team.team.team_pe.expect("darcs cant exist on non team members"), lock_type: LockType::Write});
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for GlobalRwDarcWriteGuard<'_,T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unsafe{
            fmt::Debug::fmt(&self.rwlock.data.get().as_ref(),f)
        }
    }
}



#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct GlobalRwDarc<T: 'static + ?Sized> {
    // pub(crate) darc: Darc<RwLock<Box<(T,MyRwLock)>>>,
    #[serde(serialize_with = "globalrw_serialize2", deserialize_with = "globalrw_from_ndarc2")]
    pub(crate) darc: Darc<DistRwLock<T>>,
}

unsafe impl<T: ?Sized + Sync + Send> Send for GlobalRwDarc<T> {}
unsafe impl<T: ?Sized + Sync + Send> Sync for GlobalRwDarc<T> {}

impl<T: ?Sized> crate::DarcSerde for GlobalRwDarc<T> {
    fn ser(&self, num_pes: usize, cur_pe: Result<usize, IdError>) {
        println!("in global rw darc ser");
        match cur_pe{
            Ok(cur_pe) => {self.darc.serialize_update_cnts(num_pes,cur_pe);},
            Err(err) =>  {panic!("can only access darcs within team members ({:?})",err);}
        }
    }
    fn des(&self, cur_pe: Result<usize, IdError>) {
        match cur_pe{
            Ok(cur_pe) => {self.darc.deserialize_update_cnts(cur_pe);},
            Err(err) => {panic!("can only access darcs within team members ({:?})",err);}
        } 
    }
}

impl<T: ?Sized> GlobalRwDarc<T> {
    fn inner(&self) -> &DarcInner<DistRwLock<T>> {
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

   
    pub async fn async_read(&self) -> GlobalRwDarcReadGuard<'_,T> {
        // println!("async read");
        let inner = self.inner();
        let team = inner.team();
        let remote_rwlock_addr = team.team.lamellae.remote_addr(0,inner as *const DarcInner<DistRwLock<T>> as *const () as usize);
        team.exec_am_pe(0, LockAm{rwlock_addr: remote_rwlock_addr, orig_pe: team.team.team_pe.expect("darcs cant exist on non team members"), lock_type: LockType::Read}).into_future().await;
        GlobalRwDarcReadGuard{
            rwlock: self.darc.clone(),
            marker: PhantomData,
        }
        // inner.item().read(remote_rwlock_addr)
    }

    pub async fn async_write(&self) -> GlobalRwDarcWriteGuard<'_,T> {
        // println!("async write");
        let inner = self.inner();
        let team = inner.team();
        let remote_rwlock_addr = team.team.lamellae.remote_addr(0,inner as *const DarcInner<DistRwLock<T>> as *const () as usize);
        team.exec_am_pe(0, LockAm{rwlock_addr: remote_rwlock_addr, orig_pe: team.team.team_pe.expect("darcs cant exist on non team members"), lock_type: LockType::Write}).into_future().await;
        GlobalRwDarcWriteGuard{
            rwlock: self.darc.clone(),
            marker: PhantomData,
        }
    }

    pub fn read(&self) -> GlobalRwDarcReadGuard<'_,T> {
        // println!("read");
        let inner = self.inner();
        let team = inner.team();
        let remote_rwlock_addr = team.team.lamellae.remote_addr(0,inner as *const DarcInner<DistRwLock<T>> as *const () as usize);
        team.exec_am_pe(0, LockAm{rwlock_addr: remote_rwlock_addr, orig_pe: team.team.team_pe.expect("darcs cant exist on non team members"), lock_type: LockType::Read}).get();
        GlobalRwDarcReadGuard{
            rwlock: self.darc.clone(),
            marker: PhantomData,
        }
    }

    pub fn write(&self) -> GlobalRwDarcWriteGuard<'_,T> {
        // println!("write");
        let inner = self.inner();
        let team = inner.team();
        let remote_rwlock_addr = team.team.lamellae.remote_addr(0,inner as *const DarcInner<DistRwLock<T>> as *const () as usize);
        team.exec_am_pe(0, LockAm{rwlock_addr: remote_rwlock_addr, orig_pe: team.team.team_pe.expect("darcs cant exist on non team members"), lock_type: LockType::Write}).get();
        GlobalRwDarcWriteGuard{
            rwlock: self.darc.clone(),
            marker: PhantomData,
        }
        // inner.item().write(remote_rwlock_addr)
    }

    
}

impl<T> GlobalRwDarc<T> {
    pub fn new(team: Arc<LamellarTeam>, item: T) -> Result<GlobalRwDarc<T>, IdError> {
        Ok(GlobalRwDarc {
            darc: Darc::try_new(team.clone(), DistRwLock::new(item,team), DarcMode::GlobalRw)?,
        })
        
    }

    pub fn try_new(team: Arc<LamellarTeam>, item: T) -> Result<GlobalRwDarc<T>, IdError> {
        Ok(GlobalRwDarc {
            darc: Darc::try_new(team.clone(), DistRwLock::new(item,team), DarcMode::GlobalRw)?,
        })
    }

    pub fn into_darc(self) -> Darc<T> {
        let inner = self.inner();
        // println!("into_darc");
        // self.print();
        inner.block_on_outstanding(DarcMode::Darc);
        inner.local_cnt.fetch_add(1, Ordering::SeqCst);
        let item = unsafe { Box::from_raw(inner.item as *mut DistRwLock<T>).into_inner() };
        let d = Darc {
            inner: self.darc.inner as *mut DarcInner<T>,
            src_pe: self.darc.src_pe,
            phantom: PhantomData,
        };
        d.inner_mut().update_item(Box::into_raw(Box::new(item)));
        d
    }

    pub fn into_localrw(self) -> LocalRwDarc<T> {
        let inner = self.inner();
        inner.block_on_outstanding(DarcMode::LocalRw);
        inner.local_cnt.fetch_add(1, Ordering::SeqCst);
        let item = unsafe { Box::from_raw(inner.item as *mut DistRwLock<T>).into_inner() };
        let d = Darc {
            inner: self.darc.inner as *mut DarcInner<RwLock<Box<T>>>,
            src_pe: self.darc.src_pe,
            phantom: PhantomData,
        };
        d.inner_mut()
            .update_item(Box::into_raw(Box::new(RwLock::new(Box::new(item)))));
        LocalRwDarc { darc: d }
    }
}

impl<T: ?Sized> Clone for GlobalRwDarc<T> {
    fn clone(&self) -> Self {
        // self.inner().local_cnt.fetch_add(1,Ordering::SeqCst);
        GlobalRwDarc {
            darc: self.darc.clone(),
        }
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for GlobalRwDarc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unsafe{
            fmt::Display::fmt(&self.inner().item().data.get().as_ref().unwrap(), f)
        }
    }
}

pub fn globalrw_serialize<S, T>(localrw: &GlobalRwDarc<T>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: ?Sized,
{
    __NetworkDarc::<T>::from(&localrw.darc).serialize(s)
}

pub fn globalrw_from_ndarc<'de, D, T>(deserializer: D) -> Result<GlobalRwDarc<T>, D::Error>
where
    D: Deserializer<'de>,
    T: ?Sized,
{
    let ndarc: __NetworkDarc<T> = Deserialize::deserialize(deserializer)?;
    // println!("gdarc from net darc");
    let rwdarc = GlobalRwDarc {
        darc: Darc::from(ndarc),
    };
    // println!("lrwdarc from net darc");
    // rwdarc.print();
    Ok(rwdarc)
}

pub(crate) fn globalrw_serialize2<S, T>(globalrw: &Darc<DistRwLock<T>>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: ?Sized,
{
    __NetworkDarc::<T>::from(globalrw).serialize(s)
}

pub(crate) fn globalrw_from_ndarc2<'de, D, T>(deserializer: D) -> Result<Darc<DistRwLock<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: ?Sized,
{
    let ndarc: __NetworkDarc<T> = Deserialize::deserialize(deserializer)?;
    // println!("gdarc from net darc");
    // rwdarc.print();
    Ok(Darc::from(ndarc))
}

impl<T: ?Sized> From<&Darc<DistRwLock<T>>> for __NetworkDarc<T> {
    fn from(darc: &Darc<DistRwLock<T>>) -> Self {
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

impl<T: ?Sized> From<__NetworkDarc<T>> for Darc<DistRwLock<T>> {
    fn from(ndarc: __NetworkDarc<T>) -> Self {
        // println!("rwdarc from net darc");

        if let Some(lamellae) = LAMELLAES.read().get(&ndarc.backend) {
            let darc = Darc {
                inner: lamellae.local_addr(ndarc.orig_world_pe, ndarc.inner_addr)
                    as *mut DarcInner<DistRwLock<T>>,
                src_pe: ndarc.orig_team_pe,
                phantom: PhantomData,
            };
            darc
        } else {
            panic!("unexepected lamellae backend {:?}", &ndarc.backend);
        }
    }
}

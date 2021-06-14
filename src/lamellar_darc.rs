use core::marker::PhantomData;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize,AtomicU8,Ordering};
use std::ops::Deref;
use std::pin::Pin;
use futures::Future;
use serde::{Serializer,Deserializer,Serialize,Deserialize};
use parking_lot::RwLock;

use crate::lamellar_world::LAMELLAES;
use crate::LamellarTeamRT;
use crate::LamellarTeam;
// use crate::LamellarAM;
use crate::lamellae::{AllocationType,Backend,LamellaeRDMA,LamellaeComm};
use crate::active_messaging::ActiveMessaging;
use crate::IdError;

#[lamellar_impl::AmDataRT(Debug)]
struct FinishedAm{
    cnt: usize,
    orig_pe: usize,
    inner_addr: usize, //cant pass the darc itself cause we cant handle generics yet in lamellarAM...
}
#[lamellar_impl::rt_am]
impl LamellarAM for FinishedAm {
    fn exec() {
        // println!("in finished! {:?}",self);
        let inner = unsafe {&*(self.inner_addr as *mut DarcInner<()>)}; //we dont actually care about the "type" we wrap here, we just need access to the meta data for the darc
        inner.dist_cnt.fetch_sub(self.cnt,Ordering::SeqCst);
    }
}

// pub enum DarcType<T: 'static + ?Sized>{
//     ReadOnly(Darc<T>),
//     LocalRw(Darc<RwLock<Box<T>>>),
// }

//i think we just have different DarcTypes

// impl <T>  DarcType<T>{
//     pub fn new_ro(team: Arc<LamellarTeam>, item: T) -> DarcType<T> {
//         DarcType::ReadOnly(Darc::new(team,item))
//     }
//     pub fn new_lrw(team: Arc<LamellarTeam>, item: T)  -> DarcType<T> {
//         DarcType::LocalRw(Darc::new(team,RwLock::new(Box::new(item))))
//     }
//     pub fn into_ro(self) -> DarcType<T>{
//         match self{
//             DarcType::ReadOnly(darc) => DarcType::ReadOnly(darc),
//             DarcType::LocalRw(darc) => {
//                 let inner = darc.inner();
//                 let item = unsafe { Box::from_raw(inner.item as *mut RwLock<Box<T>> ).into_inner() };
//                 let d = Darc{
//                     inner: darc.inner as *mut DarcInner<T>,
//                     orig_pe: darc.orig_pe,
//                     phantom: PhantomData
//                 };
//                 d.inner_mut().update_item(Box::into_raw(item));
//                 DarcType::ReadOnly(d)
//             }
//         }
//     }
//     pub fn into_lrw(self) -> DarcType<T> {
//         match self{
//             DarcType::LocalRw(darc) => DarcType::LocalRw(darc),
//             DarcType::ReadOnly(darc) => {
//                 let inner = darc.inner();
//                 let item = unsafe { Box::from_raw(inner.item as *mut T ) };
//                 let d = Darc{
//                     inner: darc.inner as *mut DarcInner<RwLock<Box<T>>>,
//                     orig_pe: darc.orig_pe,
//                     phantom: PhantomData
//                 };
//                 d.inner_mut().update_item(Box::into_raw(Box::new(RwLock::new(item))));
//                 DarcType::LocalRw(d)
//             }
//         }
//     }
// }

#[repr(C)]
pub struct DarcInner< 
T: ?Sized,
>{
    my_pe: usize, // with respect to LamellarArch used to create this object
    num_pes: usize,// with respect to LamellarArch used to create this object
    local_cnt: AtomicUsize, // cnt of times weve cloned for local access
    dist_cnt: AtomicUsize, // cnt of times weve cloned (serialized) for distributed access
    ref_cnt_addr: usize, // array of cnts for accesses from remote pes
    dropped_addr: usize,
    team: *const LamellarTeam,
    item: *const T,
}
unsafe impl<T: ?Sized + Sync + Send > Send for DarcInner<T> {}
unsafe impl<T: ?Sized + Sync + Send > Sync for DarcInner<T> {}


pub struct Darc<T:'static + ?Sized>{
    inner: *mut DarcInner<T>,
    orig_pe: usize,
    phantom: PhantomData<DarcInner<T>>,
}

unsafe impl<T: ?Sized + Sync + Send > Send for Darc<T> {}
unsafe impl<T: ?Sized + Sync + Send > Sync for Darc<T> {}

impl <T: ?Sized> DarcInner<T>{

    fn team(&self) -> Arc<LamellarTeamRT>{
        unsafe{(*self.team).team.clone() }
    }
    fn inc_pe_ref_count(&self, pe: usize, amt: usize) -> usize { //not sure yet what pe will be (world or team)
        let team_pe = pe; 
        let ref_cnt = unsafe{((self.ref_cnt_addr+team_pe*std::mem::size_of::<AtomicUsize>()) as *mut AtomicUsize).as_ref().unwrap()};
        ref_cnt.fetch_add(amt,Ordering::SeqCst)
    }
    // fn dec_pe_ref_count(&self, pe: usize,amt: usize) -> usize {
    //     // println!("decrementing");
    //     let team_pe = pe; 
    //     let  ref_cnt = unsafe{((self.ref_cnt_addr+team_pe*std::mem::size_of::<AtomicUsize>()) as *mut AtomicUsize).as_ref().unwrap()};
    //     ref_cnt.fetch_sub(amt,Ordering::SeqCst)
    // }

    fn update_item(&mut self, item: *const T){
        self.item = item;
    }

    fn item(&self) -> &T{
        unsafe { &(*self.item) }
    }
    
    fn send_finished(&self) -> Vec<Pin<Box<dyn Future<Output = Option<()>> + Send>>>{
        let ref_cnts =  unsafe{std::slice::from_raw_parts_mut(self.ref_cnt_addr as *mut AtomicUsize,self.num_pes ) };
        let team= self.team();
        let mut reqs=vec![];
        for pe in 0..ref_cnts.len(){
            
            let cnt = ref_cnts[pe].swap(0,Ordering::SeqCst);
            
            if cnt > 0 {
                let my_addr = &*self as *const DarcInner<T>  as usize;
                let pe_addr = team.lamellae.remote_addr( team.arch.world_pe(pe).unwrap(),my_addr);
                // println!("sending finised to {:?} {:?}",pe,cnt);
                reqs.push(team.exec_am_pe(pe,FinishedAm{cnt: cnt, orig_pe: pe, inner_addr: pe_addr }).into_future()); 
            }
        
        }
        reqs
    }
    unsafe fn any_ref_cnt(&self) -> bool{
        let ref_cnts =  std::slice::from_raw_parts_mut(self.ref_cnt_addr as *mut usize ,self.num_pes );//this is potentially a dirty read
        ref_cnts.iter().any(|x| *x >0)
    }
    fn block_on_outstanding(&self){
        while self.dist_cnt.load(Ordering::SeqCst) != 0 || self.local_cnt.load(Ordering::SeqCst) != 1 || unsafe { self.any_ref_cnt()}{
            // if self.local_cnt.load(Ordering::SeqCst) == 1{
            self.send_finished();
            // }
            std::thread::yield_now();
        }
        self.team().barrier();
    }
}

impl<T: ?Sized> fmt::Debug for DarcInner<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        
        write!(
            f,
            "[{:}/{:?}] lc: {:?} dc: {:?}\nref_cnt: {:?}\ndropped {:?}",
            self.my_pe, self.num_pes, self.local_cnt.load(Ordering::SeqCst), self.dist_cnt.load(Ordering::SeqCst),
            unsafe {&std::slice::from_raw_parts_mut(self.ref_cnt_addr as *mut usize,self.num_pes )},
            unsafe {&std::slice::from_raw_parts_mut(self.dropped_addr as *mut u8,self.num_pes )}
        )
    }
}

impl <T: ?Sized> Darc< T>{
    fn inner(&self) -> &DarcInner< T> {
        unsafe{self.inner.as_ref().unwrap()}
    }
    fn inner_mut(&self) -> &mut DarcInner< T> {
        unsafe{self.inner.as_mut().unwrap()}
    }
    fn ref_cnts_as_mut_slice(&self) -> &mut [usize] {
        let inner = self.inner();
        unsafe{std::slice::from_raw_parts_mut(inner.ref_cnt_addr as *mut usize,inner.num_pes ) }
    }
    fn dropped_as_mut_slice(&self) -> &mut [u8]{
        let inner = self.inner();
        unsafe{std::slice::from_raw_parts_mut(inner.dropped_addr as *mut u8,inner.num_pes ) }
    }

    pub fn serialize_update_cnts(&self, cnt: usize){
        self.inner().dist_cnt.fetch_add(cnt,std::sync::atomic::Ordering::SeqCst);
    }

    pub fn deserialize_update_cnts(&self){
        self.inner().inc_pe_ref_count(self.orig_pe,1);// we need to increment by 2 cause bincode calls the serialize function twice when serializing...
        self.inner().local_cnt.fetch_add(1,Ordering::SeqCst);
    }

    pub fn print(&self){
        println!("--------\norig: {:?} {:?} {:?}\n--------",self.orig_pe,self.inner,self.inner());
    }

    pub fn into_localrw(self) -> LocalRwDarc<T> {
        let inner = self.inner();
        inner.block_on_outstanding();
        inner.local_cnt.fetch_add(1,Ordering::SeqCst);
        let item = unsafe { Box::from_raw(inner.item as *mut T ) };
        let d = Darc{
            inner: self.inner as *mut DarcInner<RwLock<Box<T>>>,
            orig_pe: self.orig_pe,
            phantom: PhantomData
        };
        d.inner_mut().update_item(Box::into_raw(Box::new(RwLock::new(item))));
        LocalRwDarc{darc: d}
    }
    
}

impl <T>  Darc<T>{
    pub fn  new(team: Arc<LamellarTeam>, item: T) -> Darc<T>{
        if let Ok(darc) = Darc::try_new(team,item) {
            darc
        }
        else{
            panic!("Cannot create a team based Darc if not part of the team");
        }
    }

    pub fn  try_new(team: Arc<LamellarTeam>, item: T) -> Result<Darc<T>,IdError>{
        let team_rt = team.team.clone();
        let my_pe = team_rt.team_pe?;
        
        let alloc = if team_rt.num_pes == team_rt.num_world_pes {
            AllocationType::Global
            
        }
        else{
            AllocationType::Sub(team_rt.get_pes())
            
        };
        let size = std::mem::size_of::<DarcInner<T>>() + team_rt.num_pes*std::mem::size_of::<usize>()+team_rt.num_pes;
        let addr =  team_rt.lamellae.alloc(size,alloc).unwrap();
        let temp_team = team.clone();
        let darc_temp=DarcInner{
            my_pe: my_pe,
            num_pes: team_rt.num_pes,
            local_cnt: AtomicUsize::new(1),
            dist_cnt: AtomicUsize::new(0),
            ref_cnt_addr: addr + std::mem::size_of::<DarcInner<T>>(),
            dropped_addr: addr + std::mem::size_of::<DarcInner<T>>() + team_rt.num_pes*std::mem::size_of::<usize>(),
            team: Arc::into_raw(temp_team),
            item: Box::into_raw(Box::new(item)), 
        };
        unsafe{
            std::ptr::copy_nonoverlapping(&darc_temp, addr as *mut DarcInner<T>, 1);
        }
        let d = Darc{
            inner: addr as *mut DarcInner<T>,
            orig_pe: my_pe,
            phantom: PhantomData
        };
        team_rt.barrier();
        Ok(d) 
    }
    
    
}

impl<T: ?Sized> Clone for Darc<T> {
    fn clone(&self) -> Self {
        self.inner().local_cnt.fetch_add(1,Ordering::SeqCst);
        Darc{
            inner: self.inner,
            orig_pe: self.orig_pe,
            phantom: self.phantom
        }
    }
}

impl<T: ?Sized> Deref for Darc<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        unsafe {&*self.inner().item}
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for Darc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}


impl<T: ?Sized + fmt::Debug> fmt::Debug for Darc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

// impl <'a, T> IntoIterator for &'a Darc<T> 
// where
//     &'a T: IntoIterator{
//     type Item = <&'T as IntoIterator>::Item;
//     type IntoIter = <&'a T as IntoIterator>::IntoIter{
//         self.inner().item().into_iter()
//     }
// }


impl<T: 'static + ?Sized> Drop for Darc<T> {
    fn drop(&mut self) {
        let inner = self.inner();
        let cnt = inner.local_cnt.fetch_sub(1,Ordering::SeqCst);
        if cnt == 1{ //we are currently the last local ref, if it increases again it must mean someone else has come in and we can probably let them worry about cleaning up...
            let pe_ref_cnts = self.ref_cnts_as_mut_slice();
            if pe_ref_cnts.iter().any(|&x| x > 0){ //if we have received and accesses from remote pes, send we are finished
                inner.send_finished();
            }
        }
        // println!("in drop");
        // self.print();
        if inner.local_cnt.load(Ordering::SeqCst) == 0{ // we have no more current local references so lets try to launch our garbage collecting am

            let dropped_refs = self.dropped_as_mut_slice();
            let local_dropped = unsafe {(*(((&mut dropped_refs[inner.my_pe]) as *mut u8)as *mut AtomicU8)).compare_exchange(0,1,Ordering::SeqCst,Ordering::SeqCst)};
            if local_dropped == Ok(0){ 
                // println!("launching drop task");
                // self.print();
                inner.team().exec_am_local(DroppedWaitAM{inner_addr: self.inner as *const u8 as usize, dropped_addr: inner.dropped_addr,my_pe: inner.my_pe, num_pes: inner.num_pes , phantom: PhantomData::<T>});
                let rdma = &inner.team().lamellae;
                for pe in inner.team().arch.team_iter(){
                    rdma.put(pe, &dropped_refs[inner.my_pe..=inner.my_pe], inner.dropped_addr + inner.my_pe * std::mem::size_of::<u8>());
                }
            }
        }
    }
}


#[lamellar_impl::AmLocalDataRT]
struct DroppedWaitAM<T: ?Sized>{
    inner_addr: usize,
    dropped_addr: usize,
    my_pe: usize,
    num_pes: usize,
    phantom: PhantomData<T>
}

unsafe impl<T: ?Sized> Send for  DroppedWaitAM<T> {}
unsafe impl<T: ?Sized> Sync for  DroppedWaitAM<T> {}

use std::ptr::NonNull;
struct Wrapper<T: ?Sized> {
    inner: NonNull<DarcInner<T>>
}
unsafe impl<T: ?Sized> Send for Wrapper<T> {}

#[lamellar_impl::rt_am_local]
impl<T: 'static + ?Sized> LamellarAM for DroppedWaitAM<T>{
    fn exec(self){
        // println!("in DroppedWaitAM");
        let dropped_refs =   unsafe{std::slice::from_raw_parts_mut(self.dropped_addr as *mut u8,self.num_pes ) };
        // let mut timeout = std::time::Instant::now();
        for pe in dropped_refs.iter(){
            while *pe != 1 {
                async_std::task::yield_now().await;
                // if timeout.elapsed().as_secs_f64() > 5.0{
                //     // println!("Darc trying to free! {:?} {:?}",self.inner_addr,dropped_refs);
                //     timeout = std::time::Instant::now();
                // }
            }
        }
        // let inner =self.inner_addr as *mut DarcInner<T>; 
        let wrapped = Wrapper{inner: NonNull::new(self.inner_addr as *mut DarcInner<T>).unwrap()};
        // let inner = unsafe {&*wrapped.inner}; //we dont actually care about the "type" we wrap here, we just need access to the meta data for the darc (but still allow async wait cause T is not send)
        unsafe {
            while wrapped.inner.as_ref().dist_cnt.load(Ordering::SeqCst) != 0 ||  wrapped.inner.as_ref().local_cnt.load(Ordering::SeqCst) != 0 {
                if  wrapped.inner.as_ref().local_cnt.load(Ordering::SeqCst) == 0{
                    wrapped.inner.as_ref().send_finished();
                }
                // if timeout.elapsed().as_secs_f64() > 5.0{
                //     // println!("Darc trying to free! {:x} {:?} {:?} {:?}",self.inner_addr,dropped_refs,wrapped.inner.as_ref().local_cnt.load(Ordering::SeqCst),wrapped.inner.as_ref().dist_cnt.load(Ordering::SeqCst) );
                //     timeout = std::time::Instant::now();
                // }
                async_std::task::yield_now().await;
            }
            // let inner = unsafe {&*(self.inner_addr as *mut DarcInner<T>)}; //now we need to true type to deallocate appropriately
            let _item =  Box::from_raw(wrapped.inner.as_ref().item as *mut T) ;
            let team =  Arc::from_raw(wrapped.inner.as_ref().team) ; //return to rust to drop appropriately
            // println!("Darc freed! {:x} {:?}",self.inner_addr,dropped_refs);
            team.team.lamellae.free(self.inner_addr);
        }
        
        
    }
}






#[derive(serde::Deserialize,serde::Serialize)]
pub struct __NetworkDarc<T: ?Sized>{
    inner_addr: usize,
    backend: Backend,
    orig_world_pe: usize,
    orig_team_pe: usize,
    phantom: PhantomData<DarcInner<T>>,
}

pub fn darc_serialize<S,T>(darc: &Darc<T>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: ?Sized,{
    __NetworkDarc::from(darc).serialize(s)
}

pub fn darc_from_ndarc<'de, D, T> (deserializer: D) -> Result<Darc<T>, D::Error>
where
    D: Deserializer<'de>,
    T: ?Sized, {
    let ndarc: __NetworkDarc<T> = Deserialize::deserialize(deserializer)?;
    Ok(Darc::from(ndarc))
}



impl< T: ?Sized > From<&Darc<T>>
    for __NetworkDarc<T>
{
    fn from(darc: &Darc<T>) -> Self {
        let ndarc = __NetworkDarc {
            inner_addr: darc.inner as *const u8 as usize, 
            backend: darc.inner().team().lamellae.backend(),
            orig_world_pe: darc.inner().team().world_pe,
            orig_team_pe: darc.orig_pe,
            phantom: PhantomData
        };
        ndarc
    }
}

impl< T: ?Sized > From<__NetworkDarc<T>>
    for Darc<T>
{
    fn from(ndarc: __NetworkDarc<T>) -> Self {

        if let Some(lamellae) = LAMELLAES.read().get(&ndarc.backend){
            let darc = Darc{
                inner: lamellae.local_addr(ndarc.orig_world_pe,ndarc.inner_addr) as *mut DarcInner<T>,
                orig_pe: ndarc.orig_team_pe,
                phantom: PhantomData
            };
            darc
        }
        else{
            panic!("unexepected lamellae backend {:?}",&ndarc.backend);
        }
    }
}





#[derive(Debug)]
pub struct LocalRwDarc<T:'static + ?Sized>{
    darc: Darc<RwLock<Box<T>>>
}

unsafe impl<T: ?Sized + Sync + Send > Send for LocalRwDarc<T> {}
unsafe impl<T: ?Sized + Sync + Send > Sync for LocalRwDarc<T> {}
use parking_lot::{RwLockReadGuard,RwLockWriteGuard};
impl <T: ?Sized> LocalRwDarc< T>{
    fn inner(&self) -> &DarcInner<RwLock<Box<T>>> {
        self.darc.inner()
    }

    pub fn serialize_update_cnts(&self, cnt: usize){
        self.inner().dist_cnt.fetch_add(cnt,std::sync::atomic::Ordering::SeqCst);
    }

    pub fn deserialize_update_cnts(&self){
        self.inner().inc_pe_ref_count(self.darc.orig_pe,1);// we need to increment by 2 cause bincode calls the serialize function twice when serializing...
        self.inner().local_cnt.fetch_add(1,Ordering::SeqCst);
    }

    pub fn print(&self){
        println!("--------\norig: {:?} {:?}\n--------",self.darc.orig_pe,self.inner());
    }

    pub fn read(&self) -> RwLockReadGuard<Box<T>>{
        self.darc.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<Box<T>>{
        self.darc.write()
    }

    pub fn into_darc(self) -> Darc<T> {
        let inner = self.inner();
        // println!("into_darc");
        // self.print();
        inner.block_on_outstanding();  
        inner.local_cnt.fetch_add(1,Ordering::SeqCst);      
        let item = unsafe { Box::from_raw(inner.item as *mut RwLock<Box<T>> ).into_inner() };
        let d = Darc{
            inner: self.darc.inner as *mut DarcInner<T>,
            orig_pe: self.darc.orig_pe,
            phantom: PhantomData
        };
        d.inner_mut().update_item(Box::into_raw(item));
        d
    }
    
}

impl <T>  LocalRwDarc<T>{
    pub fn  new(team: Arc<LamellarTeam>, item: T) -> LocalRwDarc<T>{
        LocalRwDarc{
            darc: Darc::new(team.clone(),RwLock::new(Box::new(item)))
        }
    }

    pub fn  try_new(team: Arc<LamellarTeam>, item: T) -> Result<LocalRwDarc<T>,IdError>{
        Ok(LocalRwDarc{
            darc: Darc::try_new(team,RwLock::new(Box::new(item)))?
        })
    }    
}

impl<T: ?Sized> Clone for LocalRwDarc<T> {
    fn clone(&self) -> Self {
        // self.inner().local_cnt.fetch_add(1,Ordering::SeqCst);
        LocalRwDarc{
            darc: self.darc.clone()
        }
    }
}


impl<T: ?Sized + fmt::Display> fmt::Display for LocalRwDarc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self.read(), f)
    }
}

pub fn localrw_serialize<S,T>(localrw: &LocalRwDarc<T>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: ?Sized,{
    __NetworkDarc::<T>::from(&localrw.darc).serialize(s)
}

pub fn localrw_from_ndarc<'de, D, T> (deserializer: D) -> Result<LocalRwDarc<T>, D::Error>
where
    D: Deserializer<'de>,
    T: ?Sized, {
    let ndarc: __NetworkDarc<T> = Deserialize::deserialize(deserializer)?;
    Ok(LocalRwDarc{darc: Darc::from(ndarc)})
}

impl< T: ?Sized > From<&Darc<RwLock<Box<T>>>>
    for __NetworkDarc<T>
{
    fn from(darc: &Darc<RwLock<Box<T>>>) -> Self {
        let ndarc = __NetworkDarc {
            inner_addr: darc.inner as *const u8 as usize, 
            backend: darc.inner().team().lamellae.backend(),
            orig_world_pe: darc.inner().team().world_pe,
            orig_team_pe: darc.orig_pe,
            phantom: PhantomData
        };
        ndarc
    }
}

impl< T: ?Sized > From<__NetworkDarc<T>>
    for Darc<RwLock<Box<T>>>
{
    fn from(ndarc: __NetworkDarc<T>) -> Self {

        if let Some(lamellae) = LAMELLAES.read().get(&ndarc.backend){
            let darc = Darc{
                inner: lamellae.local_addr(ndarc.orig_world_pe,ndarc.inner_addr) as *mut DarcInner<RwLock<Box<T>>>,
                orig_pe: ndarc.orig_team_pe,
                phantom: PhantomData
            };
            darc
        }
        else{
            panic!("unexepected lamellae backend {:?}",&ndarc.backend);
        }
    }
}


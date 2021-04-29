use core::marker::PhantomData;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize,AtomicU8,Ordering};
use std::ops::Deref;
use std::pin::Pin;
use futures::Future;
use serde::{Serializer,Deserializer,Serialize,Deserialize};


use crate::lamellar_world::LAMELLAES;
use crate::LamellarTeamRT;
use crate::LamellarTeam;
use crate::LamellarAM;
use crate::lamellae::{AllocationType,Backend};
use crate::active_messaging::ActiveMessaging;
use crate::IdError;

// #[derive(serde::Serialize, serde::Deserialize, Clone)]
#[lamellar_impl::AmDataRT]
struct FinishedAm{
    cnt: usize,
    orig_pe: usize,
    inner_addr: usize, //cant pass the darc itself cause we cant handle generics yet in lamellarAM...
}
#[lamellar_impl::rt_am]
impl LamellarAM for FinishedAm {
    fn exec() {
        let inner = unsafe {&*(self.inner_addr as *mut DarcInner<()>)}; //we dont actually care about the "type" we wrap here, we just need access to the meta data for the darc
        // println!("addr: {:x} {:p}",self.inner_addr,inner);
        inner.dist_cnt.fetch_sub(self.cnt,Ordering::SeqCst);
        // println!("in finished am: dropped {:?} {:?}",self.cnt,inner);
    }
}

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
    team: *const LamellarTeamRT,
    item: *const T,
}
unsafe impl<T: ?Sized + Sync + Send > Send for DarcInner<T> {}
unsafe impl<T: ?Sized + Sync + Send > Sync for DarcInner<T> {}


pub struct Darc<T: ?Sized>{
    inner: *mut DarcInner<T>,
    orig_pe: usize,
    phantom: PhantomData<DarcInner<T>>,
}

unsafe impl<T: ?Sized + Sync + Send > Send for Darc<T> {}
unsafe impl<T: ?Sized + Sync + Send > Sync for Darc<T> {}

impl <T: ?Sized> DarcInner<T>{

    fn team(&self) -> &LamellarTeamRT{
        unsafe{&*self.team }
    }
    fn inc_pe_ref_count(&self, pe: usize, amt: usize) -> usize { //not sure yet what pe will be (world or team)
        // println!("incrementing");
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
    
    fn send_finished(&self) -> Vec<Pin<Box<dyn Future<Output = Option<()>> + Send>>>{
        let ref_cnts =  unsafe{std::slice::from_raw_parts_mut(self.ref_cnt_addr as *mut AtomicUsize,self.num_pes ) };
        let team= self.team();
        let mut reqs=vec![];
        for pe in 0..ref_cnts.len(){
            
            let cnt = ref_cnts[pe].swap(0,Ordering::SeqCst);
            
            if cnt > 0 {
                // println!("pe {:?} ref_cnt {:?} {:?}",pe,cnt,ref_cnts[pe].load(Ordering::SeqCst));
                let my_addr = &*self as *const DarcInner<T>  as usize;
                // println!("my_addr {:x}",my_addr);
                let pe_addr = team.lamellae.get_rdma().remote_addr( team.arch.world_pe(pe).unwrap(),my_addr);
                // println!("pe_addr {:x}",pe_addr);
                reqs.push(team.exec_am_pe(pe,FinishedAm{cnt: cnt, orig_pe: pe, inner_addr: pe_addr }).into_future()); 
            }
        
        }
        reqs
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
    // fn from_inner(inner: *mut DarcInner<T>)->Self{
    //     Self { inner, phantom: PhantomData }
    // }
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
        println!("--------\norig: {:?} {:?}\n--------",self.orig_pe,self.inner());
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
        let team = team.team.clone();
        let my_pe = team.team_pe?;
        let rdma = team.lamellae.get_rdma();
        
        let alloc = if team.num_pes == team.num_world_pes {
            // println!("global alloc");
            AllocationType::Global
            
        }
        else{
            // println!("sub alloc {:?}",team.get_pes());
            AllocationType::Sub(team.get_pes())
            
        };
        let size = std::mem::size_of::<DarcInner<T>>() + team.num_pes*std::mem::size_of::<usize>()+team.num_pes;
        // println!("size of darc: {:?}",size);
        let addr = rdma.alloc(size,alloc).unwrap();
        // println!("DARC Addr: {:x}",addr);
        
        let darc_temp=DarcInner{
            my_pe: my_pe,
            num_pes: team.num_pes,
            local_cnt: AtomicUsize::new(1),
            dist_cnt: AtomicUsize::new(0),
            ref_cnt_addr: addr + std::mem::size_of::<DarcInner<T>>(),
            dropped_addr: addr + std::mem::size_of::<DarcInner<T>>() + team.num_pes*std::mem::size_of::<usize>(),
            team: Arc::into_raw(team.clone()),
            item: Box::into_raw(Box::new(item)), 
            // item: Box::pin(item),
        };
        // darc_temp.inc_local_ref_count();
        // println!("a: {:x} sd: {:x} src: {:x}",addr,std::mem::size_of::<DarcInner<T>>(),team.num_pes*std::mem::size_of::<usize>());
        // println!("{:?} {:?} {:x} {:x}",darc_temp.my_pe,darc_temp.num_pes,darc_temp.ref_cnt_addr,darc_temp.dropped_addr);
        // let memslice = unsafe{std::slice::from_raw_parts(addr as *const u8,size)};
        // println!("before addr: {:x} {:?}",addr,memslice) ;
        unsafe{
            std::ptr::copy_nonoverlapping(&darc_temp, addr as *mut DarcInner<T>, 1);
        }
        // println!("after addr: {:?} {:?}",addr,memslice) ;
        let d = Darc{
            inner: addr as *mut DarcInner<T>,
            orig_pe: my_pe,
            phantom: PhantomData
        };
        // println!("entering barrier");
        // d.print();
        team.barrier();
        // team.barrier();
        // println!("exiting barrier");
        // d.print();
        Ok(d)
        
    }
    
    
}

impl<T: ?Sized> Clone for Darc<T> {
    fn clone(&self) -> Self {
        // println!("cloning");
        // self.print();
        self.inner().local_cnt.fetch_add(1,Ordering::SeqCst);
        Darc{
            inner: self.inner,
            orig_pe: self.orig_pe,
            phantom: self.phantom
        }
    }
}


#[lamellar_impl::AmDataRT]
struct DroppedWaitAM{
    inner_addr: usize,
    dropped_addr: usize,
    my_pe: usize,
    num_pes: usize
}
#[lamellar_impl::rt_am]
impl LamellarAM for DroppedWaitAM{
    fn exec(self){
        // println!("in DroppedWaitAM");
        let dropped_refs =   unsafe{std::slice::from_raw_parts_mut(self.dropped_addr as *mut u8,self.num_pes ) };
        let mut timeout = std::time::Instant::now();
        for pe in dropped_refs.iter(){
            while *pe != 1 {
                async_std::task::yield_now().await;
                if timeout.elapsed().as_secs_f64() > 5.0{
                    // println!("DroppedWaitAM: {:?}",dropped_refs);
                    timeout = std::time::Instant::now();
                }
            }
        }
        // println!("in DarcDropAM");
        let inner = unsafe {&*(self.inner_addr as *mut DarcInner<dyn std::any::Any + Send>)}; //we dont actually care about the "type" we wrap here, we just need access to the meta data for the darc
        //at this point everyone is ready to drop and we can reclaim the memory.
        let team = unsafe { Arc::from_raw(inner.team) }; //return to rust to drop appropriately
        // unsafe {Box::from_raw(inner.item as *mut dyn std::any::Any)};//return to rust to drop appropriately
        team.lamellae.get_rdma().free(self.inner_addr);
        // lamellar::team.exec_am_pe(self.my_pe,DarcDropAM{inner_addr: self.inner_addr});
    }
}

// #[derive(serde::Serialize,serde::Deserialize)]
#[lamellar_impl::AmDataRT]
struct DarcDropAM{
    inner_addr: usize,
}

#[lamellar_impl::rt_am]
impl LamellarAM for DarcDropAM{
    fn exec(self){
        // println!("in DarcDropAM");
        let inner = unsafe {&*(self.inner_addr as *mut DarcInner<dyn std::any::Any + Send>)}; //we dont actually care about the "type" we wrap here, we just need access to the meta data for the darc
        //at this point everyone is ready to drop and we can reclaim the memory.
        let team = unsafe { Arc::from_raw(inner.team) }; //return to rust to drop appropriately
        // unsafe {Box::from_raw(inner.item as *mut dyn std::any::Any)};//return to rust to drop appropriately
        team.lamellae.get_rdma().free(self.inner_addr);
    }
}

impl<T: ?Sized> Drop for Darc<T> {
    fn drop(&mut self) {
        // println!("dropping Darc");
        // self.print();
        let inner = self.inner();
        let cnt = inner.local_cnt.fetch_sub(1,Ordering::SeqCst);
        if cnt == 1{ //we are currently the last local ref, if it increases again it must mean someone else has come in and we can probably let them worry about cleaning up...
            // print!("im the last local: ");
            // self.print();
            let pe_ref_cnts = self.ref_cnts_as_mut_slice();
            if pe_ref_cnts.iter().any(|&x| x > 0){ //if we have received and accesses from remote pes, send we are finished
                inner.send_finished();
                // async_std::task::block_on(futures::future::join_all(inner.send_finished()));
                // print!("sent finished ams: ");
                // self.print();
            }
        }
        if inner.dist_cnt.load(Ordering::SeqCst) == 0 && inner.local_cnt.load(Ordering::SeqCst) == 0{ // we have no more current references so lets try to launch our garbage collecting am

            let dropped_refs = self.dropped_as_mut_slice();
            let local_dropped = unsafe {(*(((&mut dropped_refs[inner.my_pe]) as *mut u8)as *mut AtomicU8)).compare_exchange(0,1,Ordering::SeqCst,Ordering::SeqCst)};
            if local_dropped == Ok(0){ 
                inner.team().exec_am_pe(inner.my_pe,DroppedWaitAM{inner_addr: self.inner as *const u8 as usize, dropped_addr: inner.dropped_addr,my_pe: inner.my_pe, num_pes: inner.num_pes });
                let rdma = inner.team().lamellae.get_rdma();
                for pe in inner.team().arch.team_iter(){
                    // println!("putting dropped {:?} into: {:?}",&dropped_refs[inner.my_pe..=inner.my_pe],pe);
                    rdma.put(pe, &dropped_refs[inner.my_pe..=inner.my_pe], inner.dropped_addr + inner.my_pe * std::mem::size_of::<u8>());
                }
            }
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


// use serde::ser::SerializeStruct;

// NOTE!!!! Based on the internals of bincode 1.3.3
// there is an initial serialized_size call
// followed by the actual serialization, as a result
// we will need to double the dec count for the pe_ref_cnts array
#[derive(serde::Deserialize,serde::Serialize)]
pub struct __NetworkDarc<T: ?Sized>{
    inner_addr: usize,
    backend: Backend,
    orig_world_pe: usize,
    orig_team_pe: usize,
    phantom: PhantomData<DarcInner<T>>,
}

// impl<T: ?Sized> serde::Serialize for __NetworkDarc<T> {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer,
//     {
//         let darc = Darc{
//             inner: self.inner_addr as *mut DarcInner<T>,
//             orig_pe: self.orig_team_pe,
//             phantom: PhantomData
//         };
//         darc.inner().dist_cnt.fetch_add(1,Ordering::SeqCst);

//         // 5 is the number of fields in the struct.
//         let mut state = serializer.serialize_struct("__NetworkDarc", 5)?;
//         state.serialize_field("inner_addr", &self.inner_addr)?;
//         state.serialize_field("backend", &self.backend)?;
//         state.serialize_field("orig_world_pe", &self.orig_world_pe)?;
//         state.serialize_field("orig_team_pe", &self.orig_team_pe)?;
//         state.serialize_field("phantom", &self.phantom)?;
//         state.end()
        

//     }
// }




pub fn darc_serialize<S,T>(darc: &Darc<T>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: ?Sized, {
        __NetworkDarc::from(darc).serialize(s)
        // Serialize::serialize(ndar)?
        // s.serialize(ndarc)
}

pub fn from_ndarc<'de, D, T> (deserializer: D) -> Result<Darc<T>, D::Error>
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
                inner: lamellae.get_rdma().local_addr(ndarc.orig_world_pe,ndarc.inner_addr) as *mut DarcInner<T>,
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

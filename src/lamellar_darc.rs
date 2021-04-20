use core::marker::PhantomData;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize,Ordering};
use std::ops::Deref;

use crate::lamellar_memregion::{LamellarMemoryRegion, RemoteMemoryRegion, RegisteredMemoryRegion};
use crate::LamellarTeamRT;
use crate::LamellarTeam;
use crate::lamellae::{AllocationType,Backend,Lamellae};
use crate::active_messaging::ActiveMessaging;
use crate::IdError;

// pub trait Element: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + Send + Sync + 'static {}
// impl <T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + Send + Sync + 'static > Element for T {}


// pub trait Element: ?Sized {}
// impl <T: ?Sized> Element for T {}
// struct DistRefCnt{
//     addr: usize,
//     len: usize,
// }

#[repr(C)]
pub struct DarcInner<'a, 
T: ?Sized,
>{
    my_pe: usize, // with respect to LamellarArch used to create this object
    num_pes: usize,// with respect to LamellarArch used to create this object
    ref_cnt_addr: usize,
    dropped_addr: usize,
    team: &'a Arc<LamellarTeamRT>,
    item: T,
    
    // // lamellae: Arc<dyn Lamellae + Send + Sync>,
    // // data: LamellarMemoryRegion<usize>, //this is essentially a pointer to inner...
    // phantom: PhantomData<T>,
    // // inner: *mut DarcInner<T>,
}

pub struct Darc<'a,T: ?Sized>{
    inner: *mut DarcInner<'a,T>,
    phantom: PhantomData<DarcInner<'a,T>>,
}

impl <T: ?Sized> DarcInner<'_,T>{
    fn inc_local_ref_count(&self) -> usize{
        // println!("incrementing");
        let ref_cnt = unsafe{((self.ref_cnt_addr+self.my_pe*std::mem::size_of::<AtomicUsize>()) as *mut AtomicUsize).as_ref().unwrap()};
        ref_cnt.fetch_add(1,Ordering::Relaxed)
    }
    fn dec_local_ref_count(&self) -> usize{
        // println!("decrementing");
        let  ref_cnt = unsafe{((self.ref_cnt_addr+self.my_pe*std::mem::size_of::<AtomicUsize>()) as *mut AtomicUsize).as_ref().unwrap()};
        ref_cnt.fetch_sub(1,Ordering::Relaxed)
    }
}

impl <'a,T: ?Sized> Darc<'a, T>{
    fn inner(&self) -> &DarcInner<'_, T> {
        unsafe{self.inner.as_ref().unwrap()}
    }
    fn from_inner(inner: *mut DarcInner<'a,T>)->Self{
        Self { inner, phantom: PhantomData }
    }
    fn ref_cnts_as_mut_slice(&self) -> &mut [usize] {
        let inner = self.inner();
        unsafe{std::slice::from_raw_parts_mut(inner.ref_cnt_addr as *mut usize,inner.num_pes ) }
    }
    fn dropped_as_mut_slice(&self) -> &mut [u8]{
        let inner = self.inner();
        unsafe{std::slice::from_raw_parts_mut(inner.dropped_addr as *mut u8,inner.num_pes ) }
    }
}

impl <'a,T>  Darc<'a,T>{
    pub fn  new(team: &'a Arc<LamellarTeam>, item: T) -> Darc<T>{
        if let Ok(darc) = Darc::try_new(team,item) {
            darc
        }
        else{
            panic!("Cannot create a team based Darc if not part of the team");
        }
    }

    pub fn  try_new(team: &'a Arc<LamellarTeam>, item: T) -> Result<Darc<T>,IdError>{
        let team = &team.team;
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
        
        // let item_box = Box::new(item);
        let darc_temp = Box::new(DarcInner{
            my_pe: my_pe,
            num_pes: team.num_pes,
            ref_cnt_addr: addr + std::mem::size_of::<DarcInner<T>>(),
            dropped_addr: addr + std::mem::size_of::<DarcInner<T>>() + team.num_pes*std::mem::size_of::<usize>(),
            // item: Box::<T>::leak(item_box),
            item: item,
            team: &team,
        });
        darc_temp.inc_local_ref_count();
        // println!("a: {:x} sd: {:x} src: {:x}",addr,std::mem::size_of::<DarcInner<T>>(),team.num_pes*std::mem::size_of::<usize>());
        // println!("{:?} {:?} {:x} {:x}",darc_temp.my_pe,darc_temp.num_pes,darc_temp.ref_cnt_addr,darc_temp.dropped_addr);
        let memslice = unsafe{std::slice::from_raw_parts(addr as *const u8,size)};
        // println!("before addr: {:x} {:?}",addr,memslice) ;
        unsafe{
            std::ptr::copy_nonoverlapping(&*darc_temp, addr as *mut DarcInner<T>, 1);
        }
        // println!("after addr: {:?} {:?}",addr,memslice) ;
        let d = Darc{
            inner: addr as *mut DarcInner<T>,
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
    

   

    pub fn print(&self){
        let inner = self.inner();
        let size = std::mem::size_of::<DarcInner<T>>() + inner.num_pes*std::mem::size_of::<usize>()+inner.num_pes;
        let ref_cnts = self.ref_cnts_as_mut_slice();
        let dropped = self.dropped_as_mut_slice();
        let memslice = unsafe{std::slice::from_raw_parts(self.inner as *const u8,size+20)};
        println!("mem: {:?}",memslice);
        println!("[{:?}/{:?}] ref cnts ({:?}) {:?} dropped ({:?}) {:?}",inner.my_pe,inner.num_pes,ref_cnts.as_ptr(),ref_cnts,dropped.as_ptr(),dropped);
    }
}

impl<T: ?Sized> Clone for Darc<'_,T> {
    fn clone(&self) -> Self {
        // println!("cloning");
        // self.print();
        self.inner().inc_local_ref_count();
        Self::from_inner(self.inner)
    }
}

impl<T: ?Sized> Drop for Darc<'_,T> {
    fn drop(&mut self) {
        // println!("dropping Darc");
        // self.print();
        let inner = self.inner();
        // inner.team.barrier();
        let cnt = inner.dec_local_ref_count();
        // println!("drop cnt: {:?}",cnt);
        if cnt == 1 { //last local reference
            // println!("last drop");
            //at this point we have dropped all local handles to our local data
            let rdma = inner.team.lamellae.get_rdma();
            let dropped = self.dropped_as_mut_slice();
            let temp = rdma.rt_alloc(1).unwrap();
            let temp_slice = unsafe { std::slice::from_raw_parts_mut((temp + rdma.base_addr()) as *mut u8, 1) };
            temp_slice[0]=1;
            // dropped[inner.my_pe]=1;
            // self.print();
            for pe in inner.team.arch.team_iter(){
                println!("putting dropped {:?} into: {:?}",&dropped[inner.my_pe..=inner.my_pe],pe);
                rdma.put(pe, temp_slice, inner.dropped_addr + inner.my_pe * std::mem::size_of::<u8>());
            }
            // self.print();
            for pe in dropped.iter(){
                // println!{"{:?}",pe};
                while *pe != 1 {
                    std::thread::yield_now();
                }
            }
            rdma.rt_free(temp);
        }
        // println!("dropped Darc");
        // self.print();
    }
}

impl<T: ?Sized> Deref for Darc<'_,T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        &self.inner().item
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for Darc<'_,T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}


impl<T: ?Sized + fmt::Debug> fmt::Debug for Darc<'_,T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}


// #[derive(serde::Serialize, serde::Deserialize, Clone, Copy)]
// pub struct __NetworkLamellarDarc<T: std::clone::Clone + Send + Sync + 'static> {
//     // orig_addr: usize,
//     addr: usize,
//     pe: usize,
//     size: usize,
//     backend: Backend,
//     local: bool,
//     phantom: PhantomData<T>,
// }
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy)]
pub struct __NetworkLamellarDarc<'a,T: ?Sized>{
    inner_addr: usize,
    phantom: PhantomData<DarcInner<'a,T>>,
}

//#[prof]
impl<'a, T: ?Sized> From<LamellarDarc<'a,T>>
    for __NetworkLamellarDarc<'a,T>
{
    fn from(reg: LamellarDarc<T>) -> Self {
        let ndarc = __NetworkLamellarDarc {
            inner_addr: reg.inner as usize, 
            phantom: reg.phantom
        };
        // println!("lmr: addr: {:x} pe: {:?} size: {:?} backend {:?}, nlmr: addr: {:x} pe: {:?} size: {:?} backend {:?}",reg.addr,reg.pe,reg.size,reg.backend,nlmr.addr,nlmr.pe,nlmr.size,nlmr.backend);
        ndarc
    }
}



impl<'a, T: ?Sized> From<__NetworkLamellarDarc<'a,T>>
    for LamellarDarc<'a,T>
{
    fn from(reg: __NetworkLamellarDarc<T>) -> Self {
        darc = Darc{
            inner: reg.inner_addr as *mut DarcInner<T>,
            phantom: reg.phantom
        }
        darc
    }
}

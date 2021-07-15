// use crate::active_messaging::*; //{ActiveMessaging,AMCounters,Cmd,Msg,LamellarAny,LamellarLocal};
// use crate::lamellae::Lamellae;
// use crate::lamellar_arch::LamellarArchRT;
use crate::lamellar_memregion::{LamellarMemoryRegion, RemoteMemoryRegion,MemResult};
// use crate::lamellar_request::{AmType, LamellarRequest, LamellarRequestHandle};
use crate::lamellar_team::LamellarTeam;
// use crate::scheduler::{Scheduler,SchedulerQueue};
use crate::darc::Darc;
use crate::array::{LamellarArray,Distribution};
use crate::lamellar_memregion::RegisteredMemoryRegion;

use log::trace;
// use std::hash::{Hash, Hasher};
// use std::ops::{Index, IndexMut, Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive};
use std::ops::RangeBounds;
// use std::any;
// use core::marker::PhantomData;
// use std::collections::HashMap;
// use std::sync::atomic::Ordering;
use std::sync::Arc;
// use std::time::{Duration, Instant};



#[derive(Clone)]
pub struct UnsafeArray<
    T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + Send + Sync + 'static,
> {
    mem_region: Darc<LamellarMemoryRegion<T>>,
    size: usize,
    elem_per_pe: f32,
    team: Arc<LamellarTeam>,
    distribution: Distribution,
}



//#[prof]
impl<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + std::fmt::Debug
            + 'static,
    > UnsafeArray<T>
{
    pub fn new(
        team: Arc<LamellarTeam>,
        array_size: usize,
        distribution: Distribution,
    ) -> UnsafeArray<T> {
        let per_pe_size = (array_size as f32 / team.num_pes() as f32).ceil() as usize;
        let rmr: LamellarMemoryRegion<T> = team.alloc_shared_mem_region(per_pe_size);
        unsafe {
            for elem in rmr.clone().as_base::<u8>().as_mut_slice().unwrap(){
                *elem = 0;
            }
        }
        UnsafeArray {
            mem_region: Darc::new(team.clone(),rmr).expect("trying to create array on non team member"),
            size: array_size,
            elem_per_pe: array_size as f32 / team.num_pes() as f32,
            team: team,
            distribution: distribution
        }
    }

    pub fn get_raw_mem_region(&self) -> &LamellarMemoryRegion<T> {
        &self.mem_region
    }

    fn block_put(&self, index: usize, buf: &impl RegisteredMemoryRegion<Output = T>){
        let start_pe =  (index as f32 / self.elem_per_pe).floor() as usize;
        let end_pe = (((index + buf.len()) as f32) /  self.elem_per_pe).ceil() as usize;
        println!("index: {:?} start_pe {:?} end_pe {:?} buf_len {:?} elem_per_pe {:?}",index,start_pe,end_pe, buf.len(),self.elem_per_pe);
        let mut dist_index = index;
        let mut buf_index = 0;
        for pe in start_pe..end_pe{
            let num_elems_on_pe = (self.elem_per_pe * (pe +1) as f32).round() as usize - (self.elem_per_pe * pe as f32).round() as usize;
            let pe_start_index =  (self.elem_per_pe * pe as f32).round() as usize;
            let offset = dist_index - pe_start_index;
            let len = std::cmp::min(num_elems_on_pe-offset,buf.len() - buf_index);
            if len > 0 {
                println!("pe {:?} offset {:?} range: {:?}-{:?} dist_index {:?} pe_start_index {:?} num_elems {:?} len {:?}", pe, offset, buf_index, buf_index+len, dist_index, pe_start_index, num_elems_on_pe, len);
                unsafe {self.mem_region.put(pe,offset,&buf.sub_region(buf_index..(buf_index+len)));}
                buf_index +=len;
                dist_index += len;
            }
        }
    }

    fn cyclic_put(&self, index: usize, buf: &impl RegisteredMemoryRegion<Output = T> ){
        let num_pes = self.team.num_pes();
        let num_elems_pe = buf.len()/num_pes + 1; //we add plus one to ensure we allocate enough space
        let mut overflow = 0;
        let temp_array = self.team.alloc_local_mem_region::<T>(num_elems_pe);
        let buf_slice = unsafe {buf.as_slice().unwrap()};
        let start_pe = index % num_pes;
        for i in 0..std::cmp::min(buf.len(),num_pes){
            let mut len = 0;
            let mut k = 0;
            // for (j,val) in buf.as_slice().unwrap()[i..].iter().step_by(num_pes).enumerate(){
            for j in (i..buf.len()).step_by(num_pes){
                unsafe {temp_array.put(k,&buf.sub_region(j..=j))};
                k+=1;
            }
            let pe = (start_pe + i) % num_pes;
            let offset = index/num_pes + overflow;
            self.mem_region.iput(pe,offset,&temp_array.sub_region(0..k));
            if pe + 1 == num_pes{
                overflow += 1;
            }
        }
        self.team.free_local_memory_region(temp_array);
    }

    pub fn print(&self){
        self.team.team.barrier();
        for pe in 0..self.team.num_pes(){
            self.team.team.barrier();
            if self.team.team_pe_id().unwrap() == pe{
                println!("[{:?}] {:?}",pe,self.mem_region.as_slice());
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
            
        }
    }
}

impl<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + std::fmt::Debug
            + 'static,
    > LamellarArray<T> for UnsafeArray<T>
{
    fn put(&self, index: usize, buf: &impl RegisteredMemoryRegion<Output=T>){
        match self.distribution{
            Distribution::Block => self.block_put(index,buf),
            Distribution::Cyclic => self.cyclic_put(index,buf),
        }
    }
    // fn put_indirect(self, index: usize, buf: &impl LamellarBuffer<T>){
    //     let pe = match self.distribution{
    //         Distribution::Block => (index as f32 / self.elem_per_pe) as usize,
    //         Distribution::Cyclic => index % self.team.num_pes(),
    //     }
    //     self.mem_region.put(pe,index,buf.to_rmr());
    // }
    // fn get(self, index: usize, buf: &mut impl RegisteredMemoryRegion){

    // }
    // fn get_indirect(self, index: usize, buf: &mut impl LamellarBuffer<T>){

    // } 
}

impl<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    > RegisteredMemoryRegion for UnsafeArray<T>
{
    type Output = T;
    fn len(&self) -> usize {
        self.size
    }
    fn addr(&self) ->MemResult<usize> {
        self.mem_region.addr()
    }
    fn as_slice(&self) -> MemResult<&[T]> {
       self.mem_region.as_slice()
    }
    unsafe fn as_mut_slice(&self) -> MemResult<&mut [T]> {
        self.mem_region.as_mut_slice()
    }
    fn as_ptr(&self) -> MemResult<*const T> {
        self.mem_region.as_ptr()
    }
    fn as_mut_ptr(&self) -> MemResult<*mut T> {
        self.mem_region.as_mut_ptr()
    }
    //todo: fixme!!
    fn sub_region<R: RangeBounds<usize>>(&self, range: R) -> UnsafeArray<T> {
        self.clone()
    }
}



// impl<T, Idx> Index<Idx> for UnsafeArray<T>
// where
//     T: serde::ser::Serialize
//         + serde::de::DeserializeOwned
//         + std::clone::Clone
//         + Send
//         + Sync
//         + 'static,
//     Idx: ArrayIndex<UnsafeArray<T>>,//maybe this should be unsafesubarray?
// {
//     type Output = Idx::Output;

//     fn index(&self, index: Idx) -> &Self::Output {
//         let pe = match self.distribution{
//             Distribution::Block => (index as f32 / self.elem_per_pe) as usize,
//             Distribution::Cyclic => index % self.team.num_pes(),
//         }
//         self.mem_region

//         unsafe { &std::slice::from_raw_parts(self.addr as *mut T, self.size)[index] }
//     }
// }

// impl<T, Idx> IndexMut<Idx> for UnsafeArray<T>
// where
//     T: serde::ser::Serialize
//         + serde::de::DeserializeOwned
//         + std::clone::Clone
//         + Send
//         + Sync
//         + 'static,
//     Idx: std::slice::SliceIndex<[T]>,
// {
//     fn index_mut(&mut self, index: Idx) -> &mut Self::Output {
//         unsafe { &mut std::slice::from_raw_parts_mut(self.addr as *mut T, self.size)[index] }
//     }
// }

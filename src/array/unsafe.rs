// use crate::active_messaging::*; //{ActiveMessaging,AMCounters,Cmd,Msg,LamellarAny,LamellarLocal};
// use crate::lamellae::Lamellae;
// use crate::lamellar_arch::LamellarArchRT;
use crate::memregion::{
    shared::SharedMemoryRegion, AsBase, Dist, MemoryRegionRDMA, RegisteredMemoryRegion,
    RemoteMemoryRegion, SubRegion,
};
// use crate::lamellar_request::{AmType, LamellarRequest, LamellarRequestHandle};
use crate::lamellar_team::LamellarTeam;
// use crate::scheduler::{Scheduler,SchedulerQueue};
use crate::array::{Distribution, LamellarArrayInput, LamellarArrayRDMA, MyInto};
use crate::darc::Darc;
// use crate::lamellar_memregion::RegisteredMemoryRegion;

use log::trace;
// use std::hash::{Hash, Hasher};
use std::ops::RangeBounds;
use std::ops::{
    Index, IndexMut, Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive,
};
// use std::any;
// use core::marker::PhantomData;
// use std::collections::HashMap;
// use std::sync::atomic::Ordering;
use std::sync::Arc;
// use std::time::{Duration, Instant};

#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(into = "__NetworkUnsafeArray<T>", from = "__NetworkUnsafeArray<T>")]
pub struct UnsafeArray<T: Dist + 'static> {
    mem_region: Darc<SharedMemoryRegion<T>>,
    size: usize,      //total array size
    elem_per_pe: f32, //used to evenly distribute elems
    num_elems_local: usize,
    pub(crate) team: Arc<LamellarTeam>,
    distribution: Distribution,
}

enum ArrayOp {
    Put,
    Get,
}

//#[prof]
impl<T: Dist + 'static> UnsafeArray<T> {
    pub fn new(
        team: Arc<LamellarTeam>,
        array_size: usize,
        distribution: Distribution,
    ) -> UnsafeArray<T> {
        let elem_per_pe = array_size as f32 / team.num_pes() as f32;
        let num_elems_local = match distribution {
            Distribution::Block => {
                ((elem_per_pe * (team.team_pe_id().unwrap() + 1) as f32).round()
                    - (elem_per_pe * team.team_pe_id().unwrap() as f32).round())
                    as usize
            }
            Distribution::Cyclic => {
                let rem = array_size % team.num_pes();
                if team.team_pe_id().unwrap() < rem {
                    elem_per_pe as usize + 1
                } else {
                    elem_per_pe as usize
                }
            }
        };
        let per_pe_size = (array_size as f32 / team.num_pes() as f32).ceil() as usize; //we do ceil to ensure enough space an each pe
        println!("{:?} {:?} {:?}", elem_per_pe, num_elems_local, per_pe_size);
        let rmr: SharedMemoryRegion<T> = team.alloc_shared_mem_region(per_pe_size);
        unsafe {
            for elem in rmr.clone().as_base::<u8>().as_mut_slice().unwrap() {
                *elem = 0;
            }
        }
        UnsafeArray {
            mem_region: Darc::new(team.clone(), rmr)
                .expect("trying to create array on non team member"),
            size: array_size,
            elem_per_pe: elem_per_pe,
            num_elems_local: num_elems_local,
            team: team,
            distribution: distribution,
        }
    }

    pub fn get_raw_mem_region(&self) -> &SharedMemoryRegion<T> {
        &self.mem_region
    }

    fn block_op<U: MyInto<LamellarArrayInput<T>>>(&self, op: ArrayOp, index: usize, buf: U) {
        let buf = buf.my_into(&self.team);
        let start_pe = (index as f32 / self.elem_per_pe).floor() as usize;
        let end_pe = (((index + buf.len()) as f32) / self.elem_per_pe).ceil() as usize;
        // println!("index: {:?} start_pe {:?} end_pe {:?} buf_len {:?} elem_per_pe {:?}",index,start_pe,end_pe, buf.len(),self.elem_per_pe);
        let mut dist_index = index;
        let mut buf_index = 0;
        for pe in start_pe..end_pe {
            let num_elems_on_pe = (self.elem_per_pe * (pe + 1) as f32).round() as usize
                - (self.elem_per_pe * pe as f32).round() as usize;
            let pe_start_index = (self.elem_per_pe * pe as f32).round() as usize;
            let offset = dist_index - pe_start_index;
            let len = std::cmp::min(num_elems_on_pe - offset, buf.len() - buf_index);
            if len > 0 {
                // println!("pe {:?} offset {:?} range: {:?}-{:?} dist_index {:?} pe_start_index {:?} num_elems {:?} len {:?}", pe, offset, buf_index, buf_index+len, dist_index, pe_start_index, num_elems_on_pe, len);
                match op {
                    ArrayOp::Put => unsafe {
                        self.mem_region.put(
                            pe,
                            offset,
                            buf.sub_region(buf_index..(buf_index + len)),
                        )
                    },
                    ArrayOp::Get => unsafe {
                        self.mem_region.get(
                            pe,
                            offset,
                            buf.sub_region(buf_index..(buf_index + len)),
                        )
                    },
                }

                buf_index += len;
                dist_index += len;
            }
        }
    }

    fn cyclic_op<U: MyInto<LamellarArrayInput<T>>>(&self, op: ArrayOp, index: usize, buf: U) {
        let buf = buf.my_into(&self.team);
        let my_pe = self.team.team_pe_id().unwrap();
        let num_pes = self.team.num_pes();
        let num_elems_pe = buf.len() / num_pes + 1; //we add plus one to ensure we allocate enough space
        let mut overflow = 0;
        let start_pe = index % num_pes;
        match op {
            ArrayOp::Put => {
                let temp_array = self.team.alloc_local_mem_region::<T>(num_elems_pe);
                for i in 0..std::cmp::min(buf.len(), num_pes) {
                    let mut len = 0;
                    let mut k = 0;
                    let pe = (start_pe + i) % num_pes;
                    let offset = index / num_pes + overflow;
                    for j in (i..buf.len()).step_by(num_pes) {
                        unsafe { temp_array.put(my_pe, k, buf.sub_region(j..=j)) };
                        k += 1;
                    }
                    self.mem_region
                        .iput(pe, offset, temp_array.sub_region(0..k));
                    if pe + 1 == num_pes {
                        overflow += 1;
                    }
                }
                self.team.free_local_memory_region(temp_array);
            }
            ArrayOp::Get => {
                for i in 0..buf.len() {
                    unsafe {
                        self.mem_region.get(
                            (index + i) % num_pes,
                            index + i / num_pes,
                            buf.sub_region(i..=i),
                        )
                    }; //can't do a more optimized get (where we do one get per pe) until rofi supports transfer completion events.
                }
            }
        }
    }
    pub fn put<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U) {
        match self.distribution {
            Distribution::Block => self.block_op(ArrayOp::Put, index, buf),
            Distribution::Cyclic => self.cyclic_op(ArrayOp::Put, index, buf),
        }
    }
    pub fn get<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U) {
        match self.distribution {
            Distribution::Block => self.block_op(ArrayOp::Get, index, buf),
            Distribution::Cyclic => self.cyclic_op(ArrayOp::Get, index, buf),
        }
    }
    pub fn local_as_slice(&self) -> &[T] {
        &self
            .mem_region
            .as_slice()
            .expect("memory doesnt exist on this pe (this should not happen for arrays currently)")
            [0..self.num_elems_local]
    }
}
impl<T: Dist + std::fmt::Debug + 'static> UnsafeArray<T> {
    pub fn print(&self) {
        // println!("print entry barrier()");
        self.team.team.barrier(); //TODO: have barrier accept a string so we can print where we are stalling.
        for pe in 0..self.team.num_pes() {
            // println!("print pe {:?} barrier()",pe);
            self.team.team.barrier();
            if self.team.team_pe_id().unwrap() == pe {
                println!("[{:?}] {:?}", pe, self.local_as_slice());
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        // println!("print exit barrier()");
        // self.team.team.barrier();
    }
}

impl<T: Dist + 'static> LamellarArrayRDMA<T> for UnsafeArray<T> {
    #[inline(always)]
    fn put<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U) {
        self.put(index, buf)
    }
    #[inline(always)]
    fn get<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U) {
        self.get(index, buf)
    }
    #[inline(always)]
    fn local_as_slice(&self) -> &[T] {
        self.local_as_slice()
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

impl<T: Dist + 'static> crate::DarcSerde for UnsafeArray<T> {
    fn ser(&self, num_pes: usize, cur_pe: Result<usize, crate::IdError>) {
        // println!("in unsafearray ser");
        match cur_pe {
            Ok(cur_pe) => {
                self.mem_region.serialize_update_cnts(num_pes, cur_pe);
            }
            Err(err) => {
                panic!("can only access darcs within team members ({:?})", err);
            }
        }
    }
    fn des(&self, cur_pe: Result<usize, crate::IdError>) {
        // println!("in unsafearray des");
        match cur_pe {
            Ok(cur_pe) => {
                self.mem_region.deserialize_update_cnts(cur_pe);
            }
            Err(err) => {
                panic!("can only access darcs within team members ({:?})", err);
            }
        }
    }
}

// pub struct UnsafeElem<T>
// where
//     T: Dist + 'static,
// {
//     val: T,
//     array: UnsafeArray<T>,
// }

// impl<
//         T: serde::ser::Serialize
//             + serde::de::DeserializeOwned
//             + std::clone::Clone
//             + Send
//             + Sync
//             + 'static,
//     > RegisteredMemoryRegion for UnsafeArray<T>
// {
//     fn len(&self) -> usize {
//         self.size
//     }
//     fn addr(&self) -> MemResult<usize> {
//         self.mem_region.addr()
//     }
//     fn as_slice(&self) -> MemResult<&[T]> {
//         self.mem_region.as_slice()
//     }
//     unsafe fn as_mut_slice(&self) -> MemResult<&mut [T]> {
//         self.mem_region.as_mut_slice()
//     }
//     fn as_ptr(&self) -> MemResult<*const T> {
//         self.mem_region.as_ptr()
//     }
//     fn as_mut_ptr(&self) -> MemResult<*mut T> {
//         self.mem_region.as_mut_ptr()
//     }
//     //todo: fixme!!
//     fn sub_region<R: RangeBounds<usize>>(&self, range: R) -> UnsafeArray<T> {
//         self.clone()
//     }
// }

// impl<T, Idx> Index<Idx> for UnsafeArray<T>
// where
//     T: Dist + 'static,
//     Idx: LamellarArrayIndex<UnsafeArray<T>>,
// {
//     type Output = Idx::Output;

//     fn index(&self, index: Idx) -> &Idx::Output {
//         index.index(self)
//     }
// }

// impl <T> LamellarArrayIndex<UnsafeArray<T>> for usize
// where T: Dist + 'static {
//     type Output = T;
//     fn index(self, array: &UnsafeArray<T>) -> &T{
//         let buf: LocalMemoryRegion<T> = array.team.alloc_local_mem_region(1);
//         array.get(self,&buf);
//         &buf.as_slice().unwrap()[0]
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

// #[derive(serde::Serialize, serde::Deserialize, Clone)]
#[lamellar_impl::AmDataRT(Clone)]
pub struct __NetworkUnsafeArray<T: Dist + 'static> {
    mem_region: Darc<SharedMemoryRegion<T>>,
    size: usize,
    elem_per_pe: f32,
    distribution: Distribution,
}

impl<T: Dist + 'static> From<UnsafeArray<T>> for __NetworkUnsafeArray<T> {
    fn from(array: UnsafeArray<T>) -> Self {
        let nua = __NetworkUnsafeArray {
            mem_region: array.mem_region.clone(),
            size: array.size,
            elem_per_pe: array.elem_per_pe, //probably dont need this
            distribution: array.distribution.clone(),
        };
        nua
    }
}

//#[prof]
impl<T: Dist + 'static> From<__NetworkUnsafeArray<T>> for UnsafeArray<T> {
    fn from(array: __NetworkUnsafeArray<T>) -> Self {
        // println!("deserializing network unsafe array");
        // array.mem_region.print();
        let team = array.mem_region.team();
        let elem_per_pe = array.elem_per_pe;
        let num_elems_local = match array.distribution {
            Distribution::Block => {
                ((elem_per_pe * (team.team_pe_id().unwrap() + 1) as f32).round()
                    - (elem_per_pe * team.team_pe_id().unwrap() as f32).round())
                    as usize
            }
            Distribution::Cyclic => {
                let rem = array.size % team.num_pes();
                if team.team_pe_id().unwrap() < rem {
                    elem_per_pe as usize + 1
                } else {
                    elem_per_pe as usize
                }
            }
        };
        UnsafeArray {
            mem_region: array.mem_region.clone(),
            size: array.size,
            elem_per_pe: elem_per_pe,
            num_elems_local: num_elems_local,
            team: team,
            distribution: array.distribution.clone(),
        }
    }
}

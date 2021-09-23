use crate::active_messaging::*; //{ActiveMessaging,AMCounters,Cmd,Msg,LamellarAny,LamellarLocal};
use crate::lamellae::AllocationType;
use crate::lamellar_request::LamellarRequest;
use crate::scheduler::SchedulerQueue;
// use crate::lamellar_arch::LamellarArchRT;
use crate::memregion::{Dist, MemoryRegion, RegisteredMemoryRegion, RemoteMemoryRegion, SubRegion};
// use crate::lamellar_request::{AmType, LamellarRequest, LamellarRequestHandle};
use crate::lamellar_team::LamellarTeam;
// use crate::scheduler::{Scheduler,SchedulerQueue};
use crate::array::*;
// {
//     Distribution, LamellarArray, LamellarArrayInput, LamellarArrayIter, LamellarArrayIterator,
//     LamellarArrayRDMA, LamellarArrayReduce, MyInto, REDUCE_OPS,
// };
use crate::darc::Darc;
use crate::deserialize;
// use crate::lamellar_memregion::RegisteredMemoryRegion;

// use log::trace;
// use std::hash::{Hash, Hasher};
// use std::ops::RangeBounds;
// use std::ops::{
//     Index, IndexMut, Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive,
// };
// use std::any;
// use core::marker::PhantomData;
use std::collections::HashMap;
use parking_lot::RwLock;
use core::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};




struct UnsafeArrayInner {
    mem_region: MemoryRegion<u8>,
    array_counters: Arc<AMCounters>,
    op_map: Arc<RwLock<HashMap<ArrayOp,Box<dyn Fn(&ArrayOpInput)+Sync+Send>>>>,
    pub(crate) team: Arc<LamellarTeam>,
}

//need to calculate num_elems_local dynamically
#[lamellar_impl::AmDataRT(Clone)]
pub struct UnsafeArray<T: Dist + 'static> {
    inner: Darc<UnsafeArrayInner>,
    distribution: Distribution,
    size: usize,      //total array size
    elem_per_pe: f32, //used to evenly distribute elems
    // num_elems_local: usize,
    sub_array_offset: usize,
    sub_array_size: usize,
    phantom: PhantomData<T>,
}

// impl<T: Dist + 'static> crate::DarcSerde for UnsafeArray<T> {
//     fn ser(&self, num_pes: usize, cur_pe: Result<usize, crate::IdError>) {
//         // println!("in unsafearray ser");
//         self.mem_region.ser(num_pes, cur_pe);
//     }
//     fn des(&self, cur_pe: Result<usize, crate::IdError>) {
//         // println!("in unsafearray des");
//         self.mem_region.des(cur_pe);
//     }
// }



//#[prof]
impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> UnsafeArray<T> {
    pub fn new(
        team: Arc<LamellarTeam>,
        array_size: usize,
        distribution: Distribution,
    ) -> UnsafeArray<T> {
        let elem_per_pe = array_size as f32 / team.num_pes() as f32;
        let per_pe_size = (array_size as f32 / team.num_pes() as f32).ceil() as usize; //we do ceil to ensure enough space an each pe
                                                                                       // println!("new unsafe array {:?} {:?} {:?}", elem_per_pe, num_elems_local, per_pe_size);
        let rmr = MemoryRegion::new(
            per_pe_size * std::mem::size_of::<T>(),
            team.team.lamellae.clone(),
            AllocationType::Global,
        );
        // println!("goint to init array");
        unsafe {
            for elem in rmr.as_mut_slice().unwrap() {
                *elem = 0;
            }
        }
        // println!("after array init");
        

        let array = UnsafeArray {
            inner: Darc::new(
                team.clone(),
                UnsafeArrayInner {
                    mem_region: rmr,
                    array_counters: Arc::new(AMCounters::new()),
                    op_map: Arc::new(RwLock::new(HashMap::new())),
                    team: team,
                },
            )
            .expect("trying to create array on non team member"),
            distribution: distribution.clone(),
            size: array_size,
            elem_per_pe: elem_per_pe,
            // num_elems_local: num_elems_local,
            sub_array_offset: 0,
            sub_array_size: array_size,
            phantom: PhantomData,
        };
        
        // println!("dist {:?} size: {:?} elem_per_per {:?} num_elems_local {:?} sub_array_offset {:?} sub_array_size {:?}",
        // distribution,array_size,elem_per_pe,array.num_elems_local(),0,array_size);
        array
    }
    fn num_elems_local(&self) -> usize {
        match self.distribution {
            Distribution::Block => {
                ((self.elem_per_pe * (self.inner.team.team_pe_id().unwrap() + 1) as f32).round()
                    - (self.elem_per_pe * self.inner.team.team_pe_id().unwrap() as f32).round())
                    as usize
            }
            Distribution::Cyclic => {
                let rem = self.size % self.inner.team.num_pes();
                if self.inner.team.team_pe_id().unwrap() < rem {
                    self.elem_per_pe as usize + 1
                } else {
                    self.elem_per_pe as usize
                }
            }
        }
    }

    pub fn num_pes(&self)->usize{
        self.inner.team.num_pes()
    }


    pub fn pe_for_dist_index(&self, index: usize) -> usize{
        match self.distribution {
            Distribution::Block => {
                (index as f32 / self.elem_per_pe).floor() as usize
            },
            Distribution::Cyclic => {
                index %  self.inner.team.num_pes()
            }
        }
    }
    pub fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> usize {
        match self.distribution {
            Distribution::Block => {
                let pe_start_index = (self.elem_per_pe * pe as f32).round() as usize;
                index - pe_start_index
            },
            Distribution::Cyclic => {
                index /  self.inner.team.num_pes()
            }
        }
    }

    fn block_op<U: MyInto<LamellarArrayInput<T>>>(&self, op: ArrayOp, index: usize, buf: U) {
        let buf = buf.my_into(&self.inner.team);
        let start_pe = (index as f32 / self.elem_per_pe).floor() as usize;
        let end_pe = (((index + buf.len()) as f32) / self.elem_per_pe).ceil() as usize;
        // println!(
        //     "index: {:?} start_pe {:?} end_pe {:?} buf_len {:?} ",
        //     index,
        //     start_pe,
        //     end_pe,
        //     buf.len(),
        // );
        let mut dist_index = index;
        let mut buf_index = 0;
        for pe in start_pe..end_pe {
            let num_elems_on_pe = (self.elem_per_pe * (pe + 1) as f32).round() as usize
                - (self.elem_per_pe * pe as f32).round() as usize;
            let pe_start_index = (self.elem_per_pe * pe as f32).round() as usize;
            let offset = dist_index - pe_start_index;
            let len = std::cmp::min(num_elems_on_pe - offset, buf.len() - buf_index);
            if len > 0 {
                println!("pe {:?} offset {:?} range: {:?}-{:?} dist_index {:?} pe_start_index {:?} num_elems {:?} len {:?}", pe, offset, buf_index, buf_index+len, dist_index, pe_start_index, num_elems_on_pe, len);
                match op {
                    ArrayOp::Put => unsafe {
                        self.inner.mem_region.put(
                            pe,
                            offset,
                            buf.sub_region(buf_index..(buf_index + len)),
                        )
                    },
                    ArrayOp::Get => unsafe {
                        self.inner.mem_region.get(
                            pe,
                            offset,
                            buf.sub_region(buf_index..(buf_index + len)),
                        )
                    },
                    _ => {}
                }

                buf_index += len;
                dist_index += len;
            }
        }
    }

    fn len(&self) -> usize {
        self.sub_array_size
    }

    fn cyclic_op<U: MyInto<LamellarArrayInput<T>>>(&self, op: ArrayOp, index: usize, buf: U) {
        let buf = buf.my_into(&self.inner.team);
        let my_pe = self.inner.team.team_pe_id().unwrap();
        let num_pes = self.inner.team.num_pes();
        let num_elems_pe = buf.len() / num_pes + 1; //we add plus one to ensure we allocate enough space
        let mut overflow = 0;
        let start_pe = index % num_pes;
        match op {
            ArrayOp::Put => {
                let temp_array = self.inner.team.alloc_local_mem_region::<T>(num_elems_pe);
                for i in 0..std::cmp::min(buf.len(), num_pes) {
                    // let mut len = 0;
                    let mut k = 0;
                    let pe = (start_pe + i) % num_pes;
                    let offset = index / num_pes + overflow;
                    for j in (i..buf.len()).step_by(num_pes) {
                        unsafe { temp_array.put(my_pe, k, buf.sub_region(j..=j)) };
                        k += 1;
                    }
                    self.inner
                        .mem_region
                        .iput(pe, offset, temp_array.sub_region(0..k));
                    if pe + 1 == num_pes {
                        overflow += 1;
                    }
                }
                self.inner.team.free_local_memory_region(temp_array);
            }
            ArrayOp::Get => { //optimize like we did for put...
                for i in 0..buf.len() {
                    // println!(
                    //     "gindex {:?} pe {:?} pindex {:?} i: {:?}",
                    //     index,
                    //     (index + i) % num_pes,
                    //     (index + i) / num_pes,
                    //     i
                    // );
                    unsafe {
                        self.inner.mem_region.get(
                            (index + i) % num_pes,
                            (index + i) / num_pes,
                            buf.sub_region(i..=i),
                        )
                    }; //can't do a more optimized get (where we do one get per pe) until rofi supports transfer completion events.
                }
            }
            _ => {}
        }
    }
    pub fn put<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U) {
        match self.distribution {
            Distribution::Block => self.block_op(ArrayOp::Put, self.sub_array_offset + index, buf),
            Distribution::Cyclic => {
                self.cyclic_op(ArrayOp::Put, self.sub_array_offset + index, buf)
            }
        }
    }
    pub fn get<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U) {
        match self.distribution {
            Distribution::Block => self.block_op(ArrayOp::Get, self.sub_array_offset + index, buf),
            Distribution::Cyclic => {
                self.cyclic_op(ArrayOp::Get, self.sub_array_offset + index, buf)
            }
        }
    }
    pub fn local_as_slice(&self) -> &[T] {
        self.local_as_mut_slice()
    }
    pub fn local_as_mut_slice(&self) -> &mut [T] {
        // println!("in local_as_slice");
        let slice = unsafe {
            self.inner.mem_region.as_casted_mut_slice::<T>().expect(
            "memory doesnt exist on this pe (this should not happen for arrays currently)",
        )};
        let index = self.sub_array_offset;
        let len = self.sub_array_size;
        let my_pe = self.inner.team.team_pe_id().unwrap();
        let num_pes = self.inner.team.num_pes();
        match self.distribution {
            Distribution::Block => {
                let start_pe = (index as f32 / self.elem_per_pe).floor() as usize;
                let end_pe = (((index + len) as f32) / self.elem_per_pe).ceil() as usize;
                let num_elems_local = self.num_elems_local();
                // println!("start_pe {:?} end_pe {:?}", start_pe, end_pe);
                if my_pe == start_pe || my_pe == end_pe {
                    let start_index = index - (self.elem_per_pe * my_pe as f32).round() as usize;
                    let end_index = if start_index + len > num_elems_local {
                        num_elems_local
                    } else {
                        start_index + len
                    };
                    &mut slice[start_index..end_index]
                } else {
                    &mut slice[0..num_elems_local]
                }
            }
            Distribution::Cyclic => {
                let start_index = index / num_pes + if my_pe >= index % num_pes { 0 } else { 1 };
                let remainder = (index + len) % num_pes;
                let end_index = (index + len) / num_pes
                    + if my_pe < remainder && remainder > 0 {
                        1
                    } else {
                        0
                    };
                &mut slice[start_index..end_index]
            }
        }
    }
    pub fn as_base_inner<B: Dist + 'static>(self) -> UnsafeArray<B> {
        let u8_size = self.size * std::mem::size_of::<T>();
        let b_size = u8_size / std::mem::size_of::<B>();
        // println!("u8size {:?} bsize {:?}", u8_size, b_size);
        let elem_per_pe = b_size as f32 / self.inner.team.num_pes() as f32;
        // println!("elem_per_pe {:?}", elem_per_pe);
        let u8_offset = self.sub_array_offset * std::mem::size_of::<T>();
        let u8_sub_size = self.sub_array_size * std::mem::size_of::<T>();

        // println!(
        //     "old num_elems_local {:?} u8_offset {:?} u8_sub_size {:?}",
        //     self.num_elems_local(),
        //     u8_offset,
        //     u8_sub_size
        // );

        UnsafeArray {
            inner: self.inner.clone(),
            distribution: self.distribution,
            size: b_size,
            elem_per_pe: elem_per_pe,
            // num_elems_local: num_elems_local,
            sub_array_offset: u8_offset / std::mem::size_of::<B>(),
            sub_array_size: u8_sub_size / std::mem::size_of::<B>(),
            phantom: PhantomData,
        }
    }

    pub fn reduce_inner(
        &self,
        func: LamellarArcAm,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        if let Ok(my_pe) = self.inner.team.team_pe_id() {
            self.inner.team.team.exec_am_pe::<T>(
                self.inner.team.clone(),
                my_pe,
                func,
                Some(self.inner.array_counters.clone()),
            )
        } else {
            self.inner.team.team.exec_am_pe::<T>(
                self.inner.team.clone(),
                0,
                func,
                Some(self.inner.array_counters.clone()),
            )
        }
    }

    pub fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.reduce_inner(self.get_reduction_op(op.to_string()))
    }
    pub fn sum(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.reduce("sum")
    }
    pub fn prod(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.reduce("prod")
    }
    pub fn max(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.reduce("max")
    }

    pub(crate) fn local_as_ptr(&self) -> *const T {
        self.inner.mem_region.as_casted_ptr::<T>().unwrap()
    }
    pub(crate) fn local_as_mut_ptr(&self) -> *mut T {
        self.inner.mem_region.as_casted_mut_ptr::<T>().unwrap()
    }

    pub fn iter(&self) -> LamellarArrayIter<'_, T> {
        LamellarArrayIter::new(self.clone().into(), self.inner.team.clone())
    }
    // pub fn dist_iter(&self) -> LamellarArrayDistIter<'_, T> {
    //     LamellarArrayDistIter::new(self.clone().into())
    // }

    pub fn for_each<F>(&self,op: F)
    where F: Fn(&T) + Sync + Send + Clone +  'static{
       let num_workers = match std::env::var("LAMELLAR_THREADS") {
            Ok(n) => n.parse::<usize>().unwrap(),
            Err(_) => 4,
        };
        let num_elems_local = self.num_elems_local();
        let elems_per_thread = num_elems_local as f64/num_workers as f64;
        if let Ok(my_pe) = self.inner.team.team_pe_id() {
            let mut worker = 0;
            while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local{
                self.inner.team.team.exec_am_local(
                    self.inner.team.clone(),
                    ForEach{
                        op: op.clone(),
                        data: self.clone().into(),
                        start_i: (worker as f64 * elems_per_thread).round() as usize,
                        end_i: ((worker+1) as f64 * elems_per_thread).round() as usize,
                    },
                    Some(self.inner.array_counters.clone()),
                );
                worker+=1;
            }
        }
    }
    pub fn for_each_mut<F>(&self,op: F)
    where F: Fn(&mut T) + Sync + Send + Clone +  'static{
       let num_workers = match std::env::var("LAMELLAR_THREADS") {
            Ok(n) => n.parse::<usize>().unwrap(),
            Err(_) => 4,
        };
        let num_elems_local = self.num_elems_local();
        let elems_per_thread = num_elems_local as f64/num_workers as f64;
        if let Ok(my_pe) = self.inner.team.team_pe_id() {
            let mut worker = 0;
            while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local{
                self.inner.team.team.exec_am_local(
                    self.inner.team.clone(),
                    ForEachMut{
                        op: op.clone(),
                        data: self.clone().into(),
                        start_i: (worker as f64 * elems_per_thread).round() as usize,
                        end_i: ((worker+1) as f64 * elems_per_thread).round() as usize,
                    },
                    Some(self.inner.array_counters.clone()),
                );
                worker+=1;
            }
        }
    }
}

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + 'static>
    UnsafeArray<T>
{
    pub fn print(&self) {
        // println!("print entry barrier()");
        self.inner.team.team.barrier(); //TODO: have barrier accept a string so we can print where we are stalling.
        for pe in 0..self.inner.team.num_pes() {
            // println!("print pe {:?} barrier()",pe);
            self.inner.team.team.barrier();
            if self.inner.team.team_pe_id().unwrap() == pe {
                println!("[{:?}] {:?}", pe, self.local_as_slice());
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        // println!("print exit barrier()");
        // self.inner.team.team.barrier();
    }
}
#[lamellar_impl::AmDataRT]
struct AddAm {
    array: UnsafeArray<u8>,
    input: ArrayOpInput,
}

#[lamellar_impl::rt_am]
impl LamellarAM for AddAm{
    fn exec(&self) {
        (self.array.inner.op_map.read().get(&ArrayOp::Add).expect("Did not call array.init_add()"))(&self.input);
    }
}

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + std::ops::AddAssign + 'static>
    UnsafeArray<T>
{
    // pub fn init_add(&self){
    //     let array_clone = self.clone();
    //     let mut op_map = self.inner.op_map.write();
    //     op_map.insert(ArrayOp::Add,Box::new(move |input| {
    //         if let ArrayOpInput::Add(index,bytes) = input{
    //             let val: T = deserialize(&bytes).unwrap();
    //             array_clone.local_as_mut_slice()[*index] += val;
    //         }            
    //     }));
    // }
    // pub fn add(&self,index: usize,val: T) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
    //     let pe = self.pe_for_dist_index(index);
        
    //     self.inner.team.team.exec_am_pe( 
    //         self.inner.team.clone(),
    //         pe,
    //         Arc::new(AddAm{
    //             array: self.clone().as_base::<u8>(),
    //             input: ArrayOpInput::Add(self.pe_offset_for_dist_index(pe,index),crate::serialize(&val).unwrap())
    //         }),
    //         Some(self.inner.array_counters.clone()),
    //     )
    // }
    pub fn dist_add(&self, index: usize, func: LamellarArcAm) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        let pe = self.pe_for_dist_index(index);
        self.inner.team.team.exec_am_pe( 
            self.inner.team.clone(),
            pe,
            func,
            Some(self.inner.array_counters.clone()),
        )
    }
    pub fn local_add(&self, index: usize, val: T) {
        self.local_as_mut_slice()[index] += val;
    }
}


// impl<T:  Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> From<UnsafeArray<T>> for LamellarArray<T> {
//     fn from(v: UnsafeArray<T>) -> LamellarArray<T> {
//         LamellarArray::Unsafe(v)
//     }
// }

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> LamellarArrayRDMA<T>
    for UnsafeArray<T>
{
    #[inline(always)]
    fn len(&self) -> usize {
        self.len()
    }
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
        // println!("rdma local_as_slice");
        self.local_as_slice()
    }
    #[inline(always)]
    fn local_as_mut_slice(&self) -> &mut [T] {
        // println!("rdma local_as_slice");
        self.local_as_mut_slice()
    }
    #[inline(always)]
    fn as_base<B: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static>(
        self,
    ) -> LamellarArray<B> {
        self.as_base_inner::<B>().into()
    }
}

// impl<T: Dist + 'static> Drop for UnsafeArray<T> {
//     fn drop(&mut self) {
//         println!("dropping unsafe array");
//     }
// }

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> LamellarArrayReduce<T>
    for UnsafeArray<T>
{
    fn wait_all(&self) {
        let mut temp_now = Instant::now();
        while self
            .inner
            .array_counters
            .outstanding_reqs
            .load(Ordering::SeqCst)
            > 0
        {
            // std::thread::yield_now();
            self.inner.team.team.scheduler.exec_task(); //mmight as well do useful work while we wait
            if temp_now.elapsed() > Duration::new(60, 0) {
                println!(
                    "in team wait_all mype: {:?} cnt: {:?} {:?}",
                    self.inner.team.team.world_pe,
                    self.inner
                        .array_counters
                        .send_req_cnt
                        .load(Ordering::SeqCst),
                    self.inner
                        .array_counters
                        .outstanding_reqs
                        .load(Ordering::SeqCst),
                    // self.counters.recv_req_cnt.load(Ordering::SeqCst),
                );
                // self.lamellae.print_stats();
                temp_now = Instant::now();
            }
        }
    }
    fn get_reduction_op(&self, op: String) -> LamellarArcAm {
        // unsafe {
        REDUCE_OPS
            .get(&(std::any::TypeId::of::<T>(), op))
            .expect("unexpected reduction type")(
            self.clone().as_base_inner::<u8>().into(),
            self.inner.team.num_pes(),
        )
        // }
    }
    fn reduce(&self, op: &str)  -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.reduce(op)
    }
    fn sum(&self)  -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.sum()
    }
    fn max(&self)  -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.max()
    }
    fn prod(&self)  -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.prod()
    }
}

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static>
    LamellarArrayIterator<T> for UnsafeArray<T>
{
    fn iter(&self) -> LamellarArrayIter<'_, T> {
        self.iter()
    }
    fn dist_iter(&self) -> LamellarArrayDistIter<'_, T> {
        self.dist_iter()
    }
}

// pub struct UnsafeElem<T>
// where
//     T: Dist + 'static,
// {
//     val: T,
//     array: UnsafeArray<T>,
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

// #[lamellar_impl::AmDataRT(Clone)]
// pub struct __NetworkUnsafeArray<T: Dist + 'static> {
//     mem_region: SharedMemoryRegion<T>,
//     size: usize,
//     elem_per_pe: f32,
//     distribution: Distribution,
// }

// impl<T: Dist + 'static> From<UnsafeArray<T>> for __NetworkUnsafeArray<T> {
//     fn from(array: UnsafeArray<T>) -> Self {
//         let nua = __NetworkUnsafeArray {
//             mem_region: array.mem_region.clone(),
//             size: array.size,
//             elem_per_pe: array.elem_per_pe, //probably dont need this
//             distribution: array.distribution.clone(),
//         };
//         nua
//     }
// }

// //#[prof]
// impl<T: Dist + 'static> From<__NetworkUnsafeArray<T>> for UnsafeArray<T> {
//     fn from(array: __NetworkUnsafeArray<T>) -> Self {
//         // println!("deserializing network unsafe array");
//         // array.mem_region.print();
//         let team = array.mem_region.mr.team();
//         let elem_per_pe = array.elem_per_pe;
//         let num_elems_local = match array.distribution {
//             Distribution::Block => {
//                 ((elem_per_pe * (team.team_pe_id().unwrap() + 1) as f32).round()
//                     - (elem_per_pe * team.team_pe_id().unwrap() as f32).round())
//                     as usize
//             }
//             Distribution::Cyclic => {
//                 let rem = array.size % team.num_pes();
//                 if team.team_pe_id().unwrap() < rem {
//                     elem_per_pe as usize + 1
//                 } else {
//                     elem_per_pe as usize
//                 }
//             }
//         };
//         UnsafeArray {
//             mem_region: array.mem_region.clone(),
//             size: array.size,
//             elem_per_pe: elem_per_pe,
//             num_elems_local: num_elems_local,
//             team: team,
//             distribution: array.distribution.clone(),
//         }
//     }
// }

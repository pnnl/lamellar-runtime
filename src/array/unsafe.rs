use crate::active_messaging::*;
use crate::array::iterator::distributed_iterator::{
    DistIter, DistIterMut, DistIteratorLauncher, DistributedIterator, ForEach, ForEachAsync
};
use crate::array::iterator::serial_iterator::LamellarArrayIter;
use crate::array::*;
use crate::darc::{Darc,DarcMode};
use crate::lamellae::AllocationType;
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::{Dist, MemoryRegion, RegisteredMemoryRegion, SubRegion};
use crate::scheduler::SchedulerQueue;
use core::marker::PhantomData;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::ops::Bound;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};


struct UnsafeArrayInner {
    mem_region: MemoryRegion<u8>,
    array_counters: Arc<AMCounters>,
    op_map: Arc<RwLock<HashMap<ArrayOp, Box<dyn Fn(&ArrayOpInput) + Sync + Send>>>>,
    pub(crate) team: Arc<LamellarTeamRT>,
}

//need to calculate num_elems_local dynamically
#[lamellar_impl::AmDataRT(Clone)]
pub struct UnsafeArray<T: Dist + 'static> {
    inner: Darc<UnsafeArrayInner>,
    distribution: Distribution,
    size: usize,      //total array size
    elem_per_pe: f64, //used to evenly distribute elems
    sub_array_offset: usize,
    sub_array_size: usize,
    pub(crate) my_pe: usize,
    phantom: PhantomData<T>,
}

//#[prof]
impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> UnsafeArray<T> {
    pub fn new<U: Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> UnsafeArray<T> {
        let team = team.into().team.clone();
        let elem_per_pe = array_size as f64 / team.num_pes() as f64;
        let per_pe_size = (array_size as f64 / team.num_pes() as f64).ceil() as usize; //we do ceil to ensure enough space an each pe
                                                                                       // println!("new unsafe array {:?} {:?} {:?}", elem_per_pe, num_elems_local, per_pe_size);
        let rmr = MemoryRegion::new(
            per_pe_size * std::mem::size_of::<T>(),
            team.lamellae.clone(),
            AllocationType::Global,
        );
        unsafe {
            for elem in rmr.as_mut_slice().unwrap() {
                *elem = 0;
            }
        }
        let my_pe = team.team_pe_id().unwrap();

        let array = UnsafeArray {
            inner: Darc::try_new(
                team.clone(),
                UnsafeArrayInner {
                    mem_region: rmr,
                    array_counters: Arc::new(AMCounters::new()),
                    op_map: Arc::new(RwLock::new(HashMap::new())),
                    team: team,
                },
                crate::darc::DarcMode::Darc,
            )
            .expect("trying to create array on non team member"),
            distribution: distribution.clone(),
            size: array_size,
            elem_per_pe: elem_per_pe,
            sub_array_offset: 0,
            sub_array_size: array_size,
            my_pe: my_pe,
            phantom: PhantomData,
        };
        array
    }
    pub fn wait_all(&self) {
        <UnsafeArray<T> as LamellarArray<T>>::wait_all(self);
    }
    pub fn barrier(&self) {
        self.inner.team.barrier();
    }
    pub(crate) fn num_elems_local(&self) -> usize {
        match self.distribution {
            Distribution::Block => {
                ((self.elem_per_pe * (self.my_pe + 1) as f64).round()
                    - (self.elem_per_pe * self.my_pe as f64).round()) as usize
            }
            Distribution::Cyclic => {
                let rem = self.size % self.inner.team.num_pes();
                if self.my_pe < rem {
                    self.elem_per_pe as usize + 1
                } else {
                    self.elem_per_pe as usize
                }
            }
        }
    }

    pub fn use_distribution(mut self, distribution: Distribution) -> Self {
        self.distribution = distribution;
        self
    }

    pub fn num_pes(&self) -> usize {
        self.inner.team.num_pes()
    }

    pub fn pe_for_dist_index(&self, index: usize) -> usize {
        match self.distribution {
            Distribution::Block => (index as f64 / self.elem_per_pe).floor() as usize,
            Distribution::Cyclic => index % self.inner.team.num_pes(),
        }
    }
    pub fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> usize {
        match self.distribution {
            Distribution::Block => {
                let pe_start_index = (self.elem_per_pe * pe as f64).round() as usize;
                index - pe_start_index
            }
            Distribution::Cyclic => index / self.inner.team.num_pes(),
        }
    }

    fn block_op<U: MyInto<LamellarArrayInput<T>>>(&self, op: ArrayOp, index: usize, buf: U) {
        let buf = buf.my_into(&self.inner.team);
        let start_pe = (index as f64 / self.elem_per_pe).floor() as usize;
        let end_pe = (((index + buf.len()) as f64) / self.elem_per_pe).ceil() as usize;
        let mut dist_index = index;
        let mut buf_index = 0;
        for pe in start_pe..end_pe {
            let num_elems_on_pe = (self.elem_per_pe * (pe + 1) as f64).round() as usize
                - (self.elem_per_pe * pe as f64).round() as usize;
            let pe_start_index = (self.elem_per_pe * pe as f64).round() as usize;
            let offset = dist_index - pe_start_index;
            let len = std::cmp::min(num_elems_on_pe - offset, buf.len() - buf_index);
            if len > 0 {
                // println!("pe {:?} offset {:?} range: {:?}-{:?} dist_index {:?} pe_start_index {:?} num_elems {:?} len {:?}", pe, offset, buf_index, buf_index+len, dist_index, pe_start_index, num_elems_on_pe, len);
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

    pub fn len(&self) -> usize {
        self.sub_array_size
    }

    fn cyclic_op<U: MyInto<LamellarArrayInput<T>>>(&self, op: ArrayOp, index: usize, buf: U) {
        let buf = buf.my_into(&self.inner.team);
        let my_pe = self.my_pe;
        let num_pes = self.inner.team.num_pes();
        let num_elems_pe = buf.len() / num_pes + 1; //we add plus one to ensure we allocate enough space
        let mut overflow = 0;
        let start_pe = index % num_pes;
        match op {
            ArrayOp::Put => {
                let temp_array = self.inner.team.alloc_local_mem_region::<T>(num_elems_pe);
                for i in 0..std::cmp::min(buf.len(), num_pes) {
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
            }
            ArrayOp::Get => {
                //optimize like we did for put...
                for i in 0..buf.len() {
                    unsafe {
                        self.inner.mem_region.get(
                            (index + i) % num_pes,
                            (index + i) / num_pes,
                            buf.sub_region(i..=i),
                        )
                    };
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
    pub fn at(&self, index: usize) -> T {
        let buf: LocalMemoryRegion<T> = self.team().alloc_local_mem_region(1);
        self.get(index, &buf);
        buf.as_slice().unwrap()[0].clone()
    }
    pub fn local_as_slice(&self) -> &[T] {
        self.local_as_mut_slice()
    }
    pub fn local_as_mut_slice(&self) -> &mut [T] {
        let slice = unsafe {
            self.inner.mem_region.as_casted_mut_slice::<T>().expect(
                "memory doesnt exist on this pe (this should not happen for arrays currently)",
            )
        };
        let index = self.sub_array_offset;
        let len = self.sub_array_size;
        let my_pe = self.my_pe;
        let num_pes = self.inner.team.num_pes();
        match self.distribution {
            Distribution::Block => {
                let start_pe = (index as f64 / self.elem_per_pe).floor() as usize;
                let end_pe = (((index + len) as f64) / self.elem_per_pe).ceil() as usize;
                let num_elems_local = self.num_elems_local();
                if my_pe == start_pe || my_pe == end_pe {
                    let start_index = index - (self.elem_per_pe * my_pe as f64).round() as usize;
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
    pub fn to_base_inner<B: Dist + 'static>(self) -> UnsafeArray<B> {
        let u8_size = self.size * std::mem::size_of::<T>();
        let b_size = u8_size / std::mem::size_of::<B>();
        let elem_per_pe = b_size as f64 / self.inner.team.num_pes() as f64;
        let u8_offset = self.sub_array_offset * std::mem::size_of::<T>();
        let u8_sub_size = self.sub_array_size * std::mem::size_of::<T>();

        UnsafeArray {
            inner: self.inner.clone(),
            distribution: self.distribution,
            size: b_size,
            elem_per_pe: elem_per_pe,
            sub_array_offset: u8_offset / std::mem::size_of::<B>(),
            sub_array_size: u8_sub_size / std::mem::size_of::<B>(),
            my_pe: self.my_pe,
            phantom: PhantomData,
        }
    }

    pub fn reduce_inner(
        &self,
        func: LamellarArcAm,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        if let Ok(my_pe) = self.inner.team.team_pe_id() {
            self.inner.team.exec_arc_am_pe::<T>(
                my_pe,
                func,
                Some(self.inner.array_counters.clone()),
            )
        } else {
            self.inner
                .team
                .exec_arc_am_pe::<T>(0, func, Some(self.inner.array_counters.clone()))
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

    // pub fn local_mem_region(&self) -> &MemoryRegion<T> {
    //     &self.inner.mem_region
    // }

    // pub(crate) fn local_as_ptr(&self) -> *const T {
    //     self.inner.mem_region.as_casted_ptr::<T>().unwrap()
    // }
    pub(crate) fn local_as_mut_ptr(&self) -> *mut T {
        self.inner.mem_region.as_casted_mut_ptr::<T>().unwrap()
    }

    pub fn dist_iter(&self) -> DistIter<'static, T,UnsafeArray<T>> {
        DistIter::new(self.clone().into(), 0, 0)
    }

    pub fn dist_iter_mut(&self) -> DistIterMut<'static, T,UnsafeArray<T>> {
        DistIterMut::new(self.clone().into(), 0, 0)
    }

    pub fn ser_iter(&self) -> LamellarArrayIter<'_, T, UnsafeArray<T>> {
        LamellarArrayIter::new(self.clone().into(), self.inner.team.clone(), 1)
    }

    pub fn buffered_iter(&self, buf_size: usize) -> LamellarArrayIter<'_, T, UnsafeArray<T>> {
        LamellarArrayIter::new(
            self.clone().into(),
            self.inner.team.clone(),
            std::cmp::min(buf_size, self.len()),
        )
    }

    pub fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> UnsafeArray<T> {
        let start = match range.start_bound() {
            //inclusive
            Bound::Included(idx) => *idx,
            Bound::Excluded(idx) => *idx + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            //exclusive
            Bound::Included(idx) => *idx + 1,
            Bound::Excluded(idx) => *idx,
            Bound::Unbounded => self.sub_array_size,
        };
        if end > self.sub_array_size {
            panic!(
                "subregion range ({:?}-{:?}) exceeds size of array {:?}",
                start, end, self.sub_array_size
            );
        }
        UnsafeArray {
            inner: self.inner.clone(),
            distribution: self.distribution,
            size: self.size,
            elem_per_pe: self.elem_per_pe,
            sub_array_offset: self.sub_array_offset + start,
            sub_array_size: (end - start),
            my_pe: self.my_pe,
            phantom: PhantomData,
        }
    }
    pub(crate) fn team(&self) -> Arc<LamellarTeamRT> {
        self.inner.team.clone()
    }

    pub(crate) fn block_on_outstanding(&self, mode: DarcMode){
        self.inner.block_on_outstanding(mode);
    }

    pub fn into_read_only(self) -> ReadOnlyArray<T> {
        self.block_on_outstanding(DarcMode::ReadOnlyArray);
        ReadOnlyArray{
            array: self
        }
    }

    pub fn into_local_only(self) -> LocalOnlyArray<T> {
        self.block_on_outstanding(DarcMode::LocalOnlyArray);
        LocalOnlyArray{
            array: self,
            _unsync: PhantomData
        }
    }
}

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> DistIteratorLauncher
    for UnsafeArray<T>
{
    fn global_index_from_local(&self, index: usize, chunk_size: usize) -> usize {
        // println!("global index cs:{:?}",chunk_size);
        let my_pe = self.my_pe;
        if chunk_size == 1 {
            match self.distribution {
                Distribution::Block => {
                    let pe_start_index = (self.elem_per_pe * my_pe as f64).round() as usize;
                    index + pe_start_index
                }
                Distribution::Cyclic => self.inner.team.num_pes() * index + my_pe,
            }
        } else {
            match self.distribution {
                Distribution::Block => {
                    let num_chunks_per_per = self.elem_per_pe / chunk_size as f64;
                    // println!("{:?} {:?}", self.elem_per_pe,num_chunks_per_per);
                    let start_chunk = (num_chunks_per_per * my_pe as f64).round() as usize;
                    start_chunk + index
                }
                Distribution::Cyclic => self.inner.team.num_pes() * index + my_pe,
            }
        }
    }

    fn for_each<I, F>(&self, iter: &I, op: F)
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + Sync + Send + Clone + 'static,
    {
        if let Ok(_my_pe) = self.inner.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local = iter.elems(self.num_elems_local());
            let elems_per_thread = num_elems_local as f64 / num_workers as f64;
            // println!("num_chunks {:?} chunks_thread {:?}", num_elems_local, elems_per_thread);
            let mut worker = 0;
            while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
                let start_i = (worker as f64 * elems_per_thread).round() as usize;
                let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
                self.inner.team.exec_am_local_tg(
                    ForEach {
                        op: op.clone(),
                        data: iter.clone(),
                        start_i: start_i,
                        end_i: end_i,
                    },
                    Some(self.inner.array_counters.clone()),
                );
                worker += 1;
            }
        }
    }
    fn for_each_async<I, F, Fut>(&self, iter: &I, op: F)
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + 'static,
    {
        if let Ok(_my_pe) = self.inner.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local = iter.elems(self.num_elems_local());
            let elems_per_thread = num_elems_local as f64 / num_workers as f64;
            // println!("num_chunks {:?} chunks_thread {:?}", num_elems_local, elems_per_thread);
            let mut worker = 0;
            while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
                let start_i = (worker as f64 * elems_per_thread).round() as usize;
                let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
                self.inner.team.exec_am_local_tg(
                    ForEachAsync {
                        op: op.clone(),
                        data: iter.clone(),
                        start_i: start_i,
                        end_i: end_i,
                    },
                    Some(self.inner.array_counters.clone()),
                );
                worker += 1;
            }
        }
    }
}

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> private::LamellarArrayPrivate<T>
    for UnsafeArray<T>
{
    fn my_pe(&self) -> usize{
        self.my_pe
    }
    fn local_as_ptr(&self) -> *const T{
        self.local_as_mut_ptr()
    }
    fn local_as_mut_ptr(&self) -> *mut T{
        self.local_as_mut_ptr()
    }

 

    fn pe_for_dist_index(&self, index: usize) -> usize {
        match self.distribution {
            Distribution::Block => (index as f64 / self.elem_per_pe).floor() as usize,
            Distribution::Cyclic => index % self.inner.team.num_pes(),
        }
    }
    fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> usize {
        match self.distribution {
            Distribution::Block => {
                let pe_start_index = (self.elem_per_pe * pe as f64).round() as usize;
                index - pe_start_index
            }
            Distribution::Cyclic => index / self.inner.team.num_pes(),
        }
    }
}


impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> LamellarArray<T>
    for UnsafeArray<T>
{
    fn team(&self) -> Arc<LamellarTeamRT>{
        self.team().clone()
    }
    
    fn num_elems_local(&self) -> usize{
        self.num_elems_local()
    }
    fn len(&self) -> usize{
        self.len()
    }
    fn barrier(&self){
        self.barrier();
    }
    fn wait_all(&self){
        let mut temp_now = Instant::now();
        while self
            .inner
            .array_counters
            .outstanding_reqs
            .load(Ordering::SeqCst)
            > 0
        {
            // std::thread::yield_now();
            self.inner.team.scheduler.exec_task(); //mmight as well do useful work while we wait
            if temp_now.elapsed() > Duration::new(60, 0) {
                println!(
                    "in team wait_all mype: {:?} cnt: {:?} {:?}",
                    self.inner.team.world_pe,
                    self.inner
                        .array_counters
                        .send_req_cnt
                        .load(Ordering::SeqCst),
                    self.inner
                        .array_counters
                        .outstanding_reqs
                        .load(Ordering::SeqCst),
                );
                temp_now = Instant::now();
            }
        }
        // println!("done in wait all {:?}",std::time::SystemTime::now());
    }
    
}
impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> LamellarArrayRead<T>
    for UnsafeArray<T>
{
    fn get<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U){
        self.get(index,buf)
    }
    fn at(&self, index: usize) -> T{
        self.at(index)
    }
}

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> LamellarArrayWrite<T>
    for UnsafeArray<T>
{
    fn put<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U){
        self.put(index,buf)
    }
}

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> SubArray<T>
    for UnsafeArray<T>
{
    type Array=UnsafeArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array {
        self.sub_array(range).into()
    }
}

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static + std::ops::AddAssign> UnsafeArray<T>
where
    UnsafeArray<T>: ArrayOps<T>,
{
    pub fn add(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        // <UnsafeArray<T> as ArrayOps<T>>::add(self, index, val) //this is implemented automatically by a proc macro
        let pe = self.pe_for_dist_index(index);
        let local_index = self.pe_offset_for_dist_index(pe,index);
        if pe == self.my_pe{
            self.local_add(local_index,val);
            None
        }
        else{
            // Some(self.dist_add(
            //     index,
            //     Arc::new (#add_name_am{
            //         data: self.clone(),
            //         local_index: local_index,
            //         val: val,
            //     })
            // ))
            None
        }
    }
}

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + 'static>
    UnsafeArray<T>
{
    pub fn print(&self) {
        self.inner.team.barrier(); //TODO: have barrier accept a string so we can print where we are stalling.
        for pe in 0..self.inner.team.num_pes() {
            self.inner.team.barrier();
            if self.my_pe == pe {
                println!("[pe {:?} data] {:?}", pe, self.local_as_slice());
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
    }
}

// impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + std::ops::AddAssign + 'static,> ArrayOps<T> for UnsafeArray<T> {
//     #[inline(always)]
//     fn add(&self, index: usize, val: T) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
//         self.add(index,val) //this is implemented automatically by a proc macro
//     }
// }
// #[lamellar_impl::AmDataRT]
// struct AddAm {
//     array: UnsafeArray<u8>,
//     input: ArrayOpInput,
// }

// #[lamellar_impl::rt_am]
// impl LamellarAM for AddAm {
//     fn exec(&self) {
//         (self
//             .array
//             .inner
//             .op_map
//             .read()
//             .get(&ArrayOp::Add)
//             .expect("Did not call array.init_add()"))(&self.input);
//     }
// }

impl<
        T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + std::ops::AddAssign + 'static,
    > UnsafeArray<T>
{
    pub fn dist_add(
        &self,
        index: usize,
        func: LamellarArcAm,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        let pe = self.pe_for_dist_index(index);
        self.inner
            .team
            .exec_arc_am_pe(pe, func, Some(self.inner.array_counters.clone()))
    }
    pub fn local_add(&self, index: usize, val: T) {
        self.local_as_mut_slice()[index] += val;
    }
}

// impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> LamellarArrayRDMA<T>
//     for UnsafeArray<T>
// {
//     #[inline(always)]
//     fn len(&self) -> usize {
//         self.len()
//     }
//     #[inline(always)]
//     fn put<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U) {
//         self.put(index, buf)
//     }
//     #[inline(always)]
//     fn get<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U) {
//         self.get(index, buf)
//     }

//     #[inline(always)]
//     fn at(&self, index: usize) -> T {
//         self.at(index)
//     }
//     #[inline(always)]
//     fn local_as_slice(&self) -> &[T] {
//         // println!("rdma local_as_slice");
//         self.local_as_slice()
//     }
//     #[inline(always)]
//     fn local_as_mut_slice(&self) -> &mut [T] {
//         // println!("rdma local_as_slice");
//         self.local_as_mut_slice()
//     }
//     #[inline(always)]
//     fn to_base<B: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static>(
//         self,
//     ) -> LamellarArray<B> {
//         self.to_base_inner::<B>().into()
//     }
// }

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> LamellarArrayReduce<T>
    for UnsafeArray<T>
{
    
    fn get_reduction_op(&self, op: String) -> LamellarArcAm {
        // unsafe {
        REDUCE_OPS
            .get(&(std::any::TypeId::of::<T>(), op))
            .expect("unexpected reduction type")(
            self.clone().to_base_inner::<u8>().into(),
            self.inner.team.num_pes(),
        )
        // }
    }
    fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.reduce(op)
    }
    fn sum(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.sum()
    }
    fn max(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.max()
    }
    fn prod(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.prod()
    }
}

// impl<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> IntoIterator
//     for &'a UnsafeArray<T>
// {
//     type Item = &'a T;
//     type IntoIter = SerialIteratorIter<LamellarArrayIter<'a, T>>;
//     fn into_iter(self) -> Self::IntoIter {
//         SerialIteratorIter {
//             iter: self.ser_iter(),
//         }
//     }
// }

// impl < T> Drop for UnsafeArray<T>{
//     fn drop(&mut self){
//         println!("dropping array!!!");
//     }
// }

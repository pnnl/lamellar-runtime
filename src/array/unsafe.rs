mod iteration;
pub(crate) mod operations;
mod rdma;

use crate::active_messaging::*;
use crate::array::*;
use crate::array::{LamellarRead, LamellarWrite};
use crate::darc::{Darc, DarcMode};
use crate::lamellae::AllocationType;
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::{Dist, MemoryRegion};
use crate::scheduler::SchedulerQueue;
use core::marker::PhantomData;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

struct UnsafeArrayInner {
    mem_region: MemoryRegion<u8>,
    pub(crate) array_counters: Arc<AMCounters>,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) my_pe: usize,
}

//need to calculate num_elems_local dynamically
#[lamellar_impl::AmDataRT(Clone)]
pub struct UnsafeArray<T: Dist> {
    inner: Darc<UnsafeArrayInner>,
    distribution: Distribution,
    size: usize,      //total array size
    elem_per_pe: f64, //used to evenly distribute elems
    sub_array_offset: usize,
    sub_array_size: usize,

    // typeid: TypeId,
    phantom: PhantomData<T>,
}

//#[prof]
impl<T: Dist> UnsafeArray<T> {
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
                    team: team,
                    my_pe: my_pe,
                },
                crate::darc::DarcMode::Darc,
            )
            .expect("trying to create array on non team member"),
            distribution: distribution.clone(),
            size: array_size,
            elem_per_pe: elem_per_pe,
            sub_array_offset: 0,
            sub_array_size: array_size,
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
                ((self.elem_per_pe * (self.inner.my_pe + 1) as f64).round()
                    - (self.elem_per_pe * self.inner.my_pe as f64).round()) as usize
            }
            Distribution::Cyclic => {
                let rem = self.size % self.inner.team.num_pes();
                if self.inner.my_pe < rem {
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

    pub fn len(&self) -> usize {
        self.sub_array_size
    }

    pub unsafe fn local_as_slice(&self) -> &[T] {
        self.local_as_mut_slice()
    }
    pub unsafe fn local_as_mut_slice(&self) -> &mut [T] {
        let slice =
            self.inner.mem_region.as_casted_mut_slice::<T>().expect(
                "memory doesnt exist on this pe (this should not happen for arrays currently)",
            );
        let index = self.sub_array_offset;
        let len = self.sub_array_size;
        let my_pe = self.inner.my_pe;
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

    pub(crate) fn local_as_mut_ptr(&self) -> *mut T {
        self.inner.mem_region.as_casted_mut_ptr::<T>().unwrap()
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
            // my_pe: self.inner.my_pe,
            // typeid: self.typeid,
            phantom: PhantomData,
        }
    }
    pub(crate) fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.inner.team.clone()
    }  
    
    pub(crate) fn block_on_outstanding(&self, mode: DarcMode) {
        self.inner.block_on_outstanding(mode);
    }

    pub fn into_read_only(self) -> ReadOnlyArray<T> {
        self.into()
    }

    pub fn into_local_only(self) -> LocalOnlyArray<T> {
        self.into()
    }

    pub fn into_collective_atomic(self) -> CollectiveAtomicArray<T> {
        self.into()
    }
}

impl<T: Dist + 'static> UnsafeArray<T>{
    pub fn into_atomic(self) -> AtomicArray<T> {
        self.into()
    }
}

impl<T: Dist> From<AtomicArray<T>> for UnsafeArray<T> {
    fn from(array: AtomicArray<T>) -> Self{
        // let array = array.into_inner();
        array.array.block_on_outstanding(DarcMode::UnsafeArray);
        array.array
    }
}

impl<T: Dist> From<CollectiveAtomicArray<T>> for UnsafeArray<T> {
    fn from(array: CollectiveAtomicArray<T>) -> Self{
        array.array.block_on_outstanding(DarcMode::UnsafeArray);
        array.array
    }
}

impl<T: Dist> From<LocalOnlyArray<T>> for UnsafeArray<T> {
    fn from(array: LocalOnlyArray<T>) -> Self{
        array.array.block_on_outstanding(DarcMode::UnsafeArray);
        array.array
    }
}

impl<T: Dist> From<ReadOnlyArray<T>> for UnsafeArray<T> {
    fn from(array: ReadOnlyArray<T>) -> Self{
        array.array.block_on_outstanding(DarcMode::UnsafeArray);
        array.array
    }
}

impl <T: Dist> AsBytes<T,u8> for UnsafeArray<T>{
    type Array = UnsafeArray<u8>;
    #[doc(hidden)]
    unsafe fn as_bytes(&self) -> Self::Array {
        let u8_size = self.size * std::mem::size_of::<T>();
        let b_size = u8_size / std::mem::size_of::<u8>();
        let elem_per_pe = b_size as f64 / self.inner.team.num_pes() as f64;
        let u8_offset = self.sub_array_offset * std::mem::size_of::<T>();
        let u8_sub_size = self.sub_array_size * std::mem::size_of::<T>();

        UnsafeArray {
            inner: self.inner.clone(),
            distribution: self.distribution,
            size: b_size,
            elem_per_pe: elem_per_pe,
            sub_array_offset: u8_offset / std::mem::size_of::<u8>(),
            sub_array_size: u8_sub_size / std::mem::size_of::<u8>(),
            // my_pe: self.inner.my_pe,
            phantom: PhantomData,
        }
    }
}

impl <T: Dist> FromBytes<T,u8> for UnsafeArray<u8>{
    type Array = UnsafeArray<T>;
    #[doc(hidden)]
    unsafe fn from_bytes(self) -> Self::Array {
        let u8_size = self.size * std::mem::size_of::<u8>();
        let b_size = u8_size / std::mem::size_of::<T>();
        let elem_per_pe = b_size as f64 / self.inner.team.num_pes() as f64;
        let u8_offset = self.sub_array_offset * std::mem::size_of::<u8>();
        let u8_sub_size = self.sub_array_size * std::mem::size_of::<u8>();

        UnsafeArray {
            inner: self.inner.clone(),
            distribution: self.distribution,
            size: b_size,
            elem_per_pe: elem_per_pe,
            sub_array_offset: u8_offset / std::mem::size_of::<T>(),
            sub_array_size: u8_sub_size / std::mem::size_of::<T>(),
            // my_pe: self.inner.my_pe,
            // typeid: self.typeid,
            phantom: PhantomData,
        }
    }
}

impl<T: Dist + serde::Serialize + serde::de::DeserializeOwned + 'static> UnsafeArray<T> {
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
}

impl<T: Dist> private::LamellarArrayPrivate<T> for UnsafeArray<T> {
    fn local_as_ptr(&self) -> *const T {
        self.local_as_mut_ptr()
    }
    fn local_as_mut_ptr(&self) -> *mut T {
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

    unsafe fn into_inner(self) -> UnsafeArray<T>{
        self
    }
    
}

impl<T: Dist> LamellarArray<T> for UnsafeArray<T> {
    fn my_pe(&self) -> usize {
        self.inner.my_pe
    }
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.team().clone()
    }

    fn num_elems_local(&self) -> usize {
        self.num_elems_local()
    }
    fn len(&self) -> usize {
        self.len()
    }
    fn barrier(&self) {
        self.barrier();
    }
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

impl<T: Dist> LamellarWrite for UnsafeArray<T> {}
impl<T: Dist> LamellarRead for UnsafeArray<T> {}

impl<T: Dist> SubArray<T> for UnsafeArray<T> {
    type Array = UnsafeArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array {
        self.sub_array(range).into()
    }
    fn global_index(&self, sub_index: usize) -> usize {
        self.sub_array_offset + sub_index
    }
}

impl<T: Dist + std::fmt::Debug> UnsafeArray<T> {
    pub fn print(&self) {
        <UnsafeArray<T> as ArrayPrint<T>>::print(&self);
    }
}

impl<T: Dist + std::fmt::Debug> ArrayPrint<T> for UnsafeArray<T> {
    fn print(&self) {
        self.inner.team.barrier(); //TODO: have barrier accept a string so we can print where we are stalling.
        for pe in 0..self.inner.team.num_pes() {
            self.inner.team.barrier();
            if self.inner.my_pe == pe {
                println!("[pe {:?} data] {:?}", pe, unsafe { self.local_as_slice() });
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
}

impl<T: Dist + serde::Serialize + serde::de::DeserializeOwned + 'static> LamellarArrayReduce<T>
    for UnsafeArray<T>
{
    fn get_reduction_op(&self, op: String) -> LamellarArcAm {
        //do this the same way we did add...
        // unsafe {
        REDUCE_OPS
            .get(&(std::any::TypeId::of::<T>(), op))
            .expect("unexpected reduction type")(
            unsafe { self.clone().as_bytes().into() },
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

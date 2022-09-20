mod iteration;
#[cfg(feature = "non-buffered-array-ops")]
pub(crate) mod operations;

#[cfg(not(feature = "non-buffered-array-ops"))]
pub(crate) mod buffered_operations;
#[cfg(not(feature = "non-buffered-array-ops"))]
pub(crate) use buffered_operations as operations;
mod rdma;

use crate::active_messaging::*;
use crate::array::r#unsafe::operations::BUFOPS;
use crate::array::*;
use crate::array::{LamellarRead, LamellarWrite};
use crate::darc::{Darc, DarcMode, WeakDarc};
use crate::lamellae::AllocationType;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::{Dist, MemoryRegion};
use crate::scheduler::SchedulerQueue;
use crate::LamellarTaskGroup;
use core::marker::PhantomData;
use parking_lot::RwLock;
use std::any::TypeId;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) struct UnsafeArrayData {
    mem_region: MemoryRegion<u8>,
    pub(crate) array_counters: Arc<AMCounters>,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) task_group: Arc<LamellarTaskGroup>,
    pub(crate) my_pe: usize,
    pub(crate) num_pes: usize,
    #[cfg(not(feature = "non-buffered-array-ops"))]
    pub(crate) op_buffers: RwLock<Vec<Arc<dyn BufferOp>>>,
    #[cfg(not(feature = "non-buffered-array-ops"))]
    req_cnt: Arc<AtomicUsize>,
}

impl std::fmt::Debug for UnsafeArrayData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UnsafeArrayData{{ mem_region: {:?}, array_counters: {:?}, team: {:?}, task_group: {:?}, my_pe: {:?}, num_pes: {:?} req_cnt: {:?} }}",
            self.mem_region, self.array_counters, self.team, self.task_group, self.my_pe, self.num_pes, self.req_cnt
        )
    }
}

// impl Drop for UnsafeArrayData {
//     fn drop(&mut self) {
//         // println!("unsafe array data dropping1");
//         self.op_buffers.write().clear();
//     }
// }

//need to calculate num_elems_local dynamically
#[lamellar_impl::AmDataRT(Clone, Debug)]
pub struct UnsafeArray<T> {
    pub(crate) inner: UnsafeArrayInner,
    phantom: PhantomData<T>,
}

#[lamellar_impl::AmDataRT(Clone, Debug)]
pub struct UnsafeByteArray {
    pub(crate) inner: UnsafeArrayInner,
}

impl UnsafeByteArray {
    pub(crate) fn downgrade(array: &UnsafeByteArray) -> UnsafeByteArrayWeak {
        UnsafeByteArrayWeak {
            inner: UnsafeArrayInner::downgrade(&array.inner),
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Clone, Debug)]
pub struct UnsafeByteArrayWeak {
    pub(crate) inner: UnsafeArrayInnerWeak,
}

impl UnsafeByteArrayWeak {
    pub fn upgrade(&self) -> Option<UnsafeByteArray> {
        if let Some(inner) = self.inner.upgrade() {
            Some(UnsafeByteArray { inner })
        } else {
            None
        }
    }
}

#[lamellar_impl::AmDataRT(Clone, Debug)]
pub(crate) struct UnsafeArrayInner {
    pub(crate) data: Darc<UnsafeArrayData>,
    pub(crate) distribution: Distribution,
    // wait: Darc<AtomicUsize>,
    orig_elem_per_pe: f64,
    elem_size: usize, //for bytes array will be size of T, for T array will be 1
    offset: usize,    //relative to size of T
    size: usize,      //relative to size of T
}

#[lamellar_impl::AmLocalDataRT(Clone, Debug)]
pub(crate) struct UnsafeArrayInnerWeak {
    pub(crate) data: WeakDarc<UnsafeArrayData>,
    pub(crate) distribution: Distribution,
    // wait: Darc<AtomicUsize>,
    orig_elem_per_pe: f64,
    elem_size: usize, //for bytes array will be size of T, for T array will be 1
    offset: usize,    //relative to size of T
    size: usize,      //relative to size of T
}

// impl Drop for UnsafeArrayInner {
//     fn drop(&mut self) {
//         // println!("unsafe array inner dropping");
//     }
// }

// impl<T: Dist> Drop for UnsafeArray<T> {
//     fn drop(&mut self) {
//         println!("Dropping unsafe array");
//         // self.wait_all();
//     }
// }

//#[prof]
impl<T: Dist + 'static> UnsafeArray<T> {
    pub fn new<U: Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> UnsafeArray<T> {
        let team = team.into().team.clone();
        let task_group = LamellarTaskGroup::new(team.clone());
        let my_pe = team.team_pe_id().unwrap();
        let num_pes = team.num_pes();
        let elem_per_pe = array_size as f64 / num_pes as f64;
        let per_pe_size = (array_size as f64 / num_pes as f64).ceil() as usize; //we do ceil to ensure enough space an each pe
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

        let data = Darc::try_new_with_drop(
            team.clone(),
            UnsafeArrayData {
                mem_region: rmr,
                array_counters: Arc::new(AMCounters::new()),
                team: team.clone(),
                task_group: Arc::new(task_group),
                my_pe: my_pe,
                num_pes: num_pes,
                // op_buffers: Mutex::new(HashMap::new()),
                op_buffers: RwLock::new(Vec::new()),
                req_cnt: Arc::new(AtomicUsize::new(0)),
            },
            crate::darc::DarcMode::UnsafeArray,
            Some(|data: &mut UnsafeArrayData| {
                // // println!("unsafe array data dropping2");
                data.op_buffers.write().clear();
            }),
        )
        .expect("trying to create array on non team member");
        // let wait = Darc::try_new(
        //     team.clone(),
        //     AtomicUsize::new(0),
        //     crate::darc::DarcMode::Darc,
        // )
        // .expect("trying to create array on non team member");
        let array = UnsafeArray {
            inner: UnsafeArrayInner {
                data: data,
                distribution: distribution.clone(),
                // wait: wait,
                orig_elem_per_pe: elem_per_pe,
                elem_size: std::mem::size_of::<T>(),
                offset: 0,        //relative to size of T
                size: array_size, //relative to size of T
            },
            phantom: PhantomData,
        };
        // println!("new unsafe");
        // unsafe {println!("size {:?} bytes {:?}",array.inner.size, array.inner.data.mem_region.as_mut_slice().unwrap().len())};
        // println!("elem per pe {:?}", elem_per_pe);
        // for i in 0..num_pes{
        //     println!("pe: {:?} {:?}",i,array.inner.num_elems_pe(i));
        // }
        // array.inner.data.print();
        array.create_buffered_ops();
        // println!("after buffered ops");
        // array.inner.data.print();
        array
    }

    fn create_buffered_ops(&self) {
        if let Some(func) = BUFOPS.get(&TypeId::of::<T>()) {
            let mut op_bufs = self.inner.data.op_buffers.write();
            let bytearray: UnsafeByteArray = self.clone().into();
            for _pe in 0..self.inner.data.num_pes {
                op_bufs.push(func(UnsafeByteArray::downgrade(&bytearray)))
            }
        }
    }
    pub fn wait_all(&self) {
        <UnsafeArray<T> as LamellarArray<T>>::wait_all(self);
    }
    pub fn barrier(&self) {
        self.inner.data.team.barrier();
    }

    pub fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future,
    {
        self.inner.data.team.scheduler.block_on(f)
    }

    pub fn use_distribution(mut self, distribution: Distribution) -> Self {
        self.inner.distribution = distribution;
        self
    }

    pub fn num_pes(&self) -> usize {
        self.inner.data.num_pes
    }

    pub fn len(&self) -> usize {
        self.inner.size
    }

    pub fn calc_pe_and_offset(&self, i: usize) -> (usize, usize) {
        if let Some(pe) = self.inner.pe_for_dist_index(i) {
            let local_index = self.inner.pe_offset_for_dist_index(pe, i).unwrap(); //calculated pe above
            (pe, local_index)
        } else {
            panic!(
                "distributed index out of bounds {:?} (len: {:?})",
                i,
                self.len()
            );
        }
    }

    pub unsafe fn local_as_slice(&self) -> &[T] {
        self.local_as_mut_slice()
    }
    pub unsafe fn local_as_mut_slice(&self) -> &mut [T] {
        let u8_slice = self.inner.local_as_mut_slice();
        // println!("u8 slice {:?} u8_len {:?} len {:?}",u8_slice,u8_slice.len(),u8_slice.len()/std::mem::size_of::<T>());
        std::slice::from_raw_parts_mut(
            u8_slice.as_mut_ptr() as *mut T,
            u8_slice.len() / std::mem::size_of::<T>(),
        )
    }

    pub unsafe fn local_data(&self) -> &[T] {
        self.local_as_mut_slice()
    }

    pub unsafe fn mut_local_data(&self) -> &mut [T] {
        self.local_as_mut_slice()
    }

    pub(crate) fn local_as_mut_ptr(&self) -> *mut T {
        let u8_ptr = unsafe { self.inner.local_as_mut_ptr() };
        // self.inner.data.mem_region.as_casted_mut_ptr::<T>().unwrap();
        u8_ptr as *mut T
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
            Bound::Unbounded => self.inner.size,
        };
        if end > self.inner.size {
            panic!(
                "subregion range ({:?}-{:?}) exceeds size of array {:?}",
                start, end, self.inner.size
            );
        }
        // println!("new inner {:?} {:?} {:?} {:?}",start,end,end-start,self.sub_array_offset + start);
        let mut inner = self.inner.clone();
        inner.offset += start;
        inner.size = end - start;
        UnsafeArray {
            inner: inner,
            phantom: PhantomData,
        }
    }

    pub fn sub_array_range(&self) -> std::ops::Range<usize> {
        self.inner.offset..(self.inner.offset + self.inner.size)
    }
    pub(crate) fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.inner.data.team.clone()
    }

    pub(crate) fn block_on_outstanding(&self, mode: DarcMode) {
        self.wait_all();
        // println!("block on outstanding");
        // self.inner.data.print();
        self.inner.data.block_on_outstanding(mode, 0); //self.inner.data.op_buffers.read().len());
        self.inner.data.op_buffers.write().clear();
        // self.inner.data.print();
    }

    pub fn into_read_only(self) -> ReadOnlyArray<T> {
        // println!("unsafe into read only");
        self.into()
    }

    pub fn into_local_only(self) -> LocalOnlyArray<T> {
        // println!("unsafe into local only");
        self.into()
    }

    pub fn into_local_lock_atomic(self) -> LocalLockAtomicArray<T> {
        // println!("unsafe into local lock atomic");
        self.into()
    }

    // pub fn into_generic_atomic(self) -> GenericAtomicArray<T> {
    //     println!("into generic atomic");
    //     self.into()
    // }
}

// impl<T: Dist> UnsafeArray<T> {
//     pub fn into_native_atomic(self) -> NativeAtomicArray<T> {
//         self.into()
//     }
// }

impl<T: Dist + 'static> UnsafeArray<T> {
    pub fn into_atomic(self) -> AtomicArray<T> {
        // println!("unsafe into atomic");
        self.into()
    }
}

// use crate::array::private::LamellarArrayPrivate;
// impl <T: Dist, A: LamellarArrayPrivate<T>> From<A> for UnsafeArray<T>{
//     fn from(array: A) -> Self {
//        let array = array.into_inner();
//        array.block_on_outstanding(DarcMode::UnsafeArray);
//        array.create_buffered_ops();
//        array
//     }
// }

impl<T: Dist> From<AtomicArray<T>> for UnsafeArray<T> {
    fn from(array: AtomicArray<T>) -> Self {
        // println!("unsafe from atomic");
        // array.into_unsafe()
        match array {
            AtomicArray::NativeAtomicArray(array) => UnsafeArray::<T>::from(array),
            AtomicArray::GenericAtomicArray(array) => UnsafeArray::<T>::from(array),
        }
    }
}

impl<T: Dist> From<NativeAtomicArray<T>> for UnsafeArray<T> {
    fn from(array: NativeAtomicArray<T>) -> Self {
        // println!("unsafe from native atomic");
        // let array = array.into_data();
        array.array.block_on_outstanding(DarcMode::UnsafeArray);
        array.array.inner.data.op_buffers.write().clear();
        array.array.create_buffered_ops();
        array.array
    }
}

impl<T: Dist> From<GenericAtomicArray<T>> for UnsafeArray<T> {
    fn from(array: GenericAtomicArray<T>) -> Self {
        // println!("unsafe from generic atomic");
        // let array = array.into_data();
        array.array.block_on_outstanding(DarcMode::UnsafeArray);
        array.array.inner.data.op_buffers.write().clear();
        array.array.create_buffered_ops();
        array.array
    }
}

impl<T: Dist> From<LocalLockAtomicArray<T>> for UnsafeArray<T> {
    fn from(array: LocalLockAtomicArray<T>) -> Self {
        // println!("unsafe from local lock atomic");
        array.array.block_on_outstanding(DarcMode::UnsafeArray);
        array.array.inner.data.op_buffers.write().clear();
        array.array.create_buffered_ops();
        array.array
    }
}

impl<T: Dist> From<LocalOnlyArray<T>> for UnsafeArray<T> {
    fn from(array: LocalOnlyArray<T>) -> Self {
        // println!("unsafe from local only");
        array.array.block_on_outstanding(DarcMode::UnsafeArray);
        array.array.inner.data.op_buffers.write().clear();
        array.array.create_buffered_ops();
        array.array
    }
}

impl<T: Dist> From<ReadOnlyArray<T>> for UnsafeArray<T> {
    fn from(array: ReadOnlyArray<T>) -> Self {
        // println!("unsafe from read only");
        array.array.block_on_outstanding(DarcMode::UnsafeArray);
        array.array.inner.data.op_buffers.write().clear();
        array.array.create_buffered_ops();
        array.array
    }
}

impl<T: Dist> From<UnsafeByteArray> for UnsafeArray<T> {
    fn from(array: UnsafeByteArray) -> Self {
        UnsafeArray {
            inner: array.inner,
            phantom: PhantomData,
        }
    }
}

impl<T: Dist> From<UnsafeArray<T>> for UnsafeByteArray {
    fn from(array: UnsafeArray<T>) -> Self {
        UnsafeByteArray { inner: array.inner }
    }
}

impl<T: Dist> From<UnsafeArray<T>> for LamellarByteArray {
    fn from(array: UnsafeArray<T>) -> Self {
        LamellarByteArray::UnsafeArray(array.into())
    }
}

impl<T: Dist + serde::Serialize + serde::de::DeserializeOwned + 'static> UnsafeArray<T> {
    pub(crate) fn reduce_data(&self, func: LamellarArcAm) -> Box<dyn LamellarRequest<Output = T>> {
        if let Ok(my_pe) = self.inner.data.team.team_pe_id() {
            self.inner.data.team.exec_arc_am_pe::<T>(
                my_pe,
                func,
                Some(self.inner.data.array_counters.clone()),
            )
        } else {
            self.inner.data.team.exec_arc_am_pe::<T>(
                0,
                func,
                Some(self.inner.data.array_counters.clone()),
            )
        }
    }

    pub(crate) fn reduce_req(&self, op: &str) -> Box<dyn LamellarRequest<Output = T>> {
        self.reduce_data(self.get_reduction_op(op.to_string()))
    }
    pub fn reduce(&self, op: &str) -> Pin<Box<dyn Future<Output = T>>> {
        self.reduce_data(self.get_reduction_op(op.to_string()))
            .into_future()
    }
    pub fn sum(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.reduce("sum")
    }
    pub fn prod(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.reduce("prod")
    }
    pub fn max(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.reduce("max")
    }
}

impl<T: Dist> private::ArrayExecAm<T> for UnsafeArray<T> {
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.team().clone()
    }
    fn team_counters(&self) -> Arc<AMCounters> {
        self.inner.data.array_counters.clone()
    }
}
impl<T: Dist> private::LamellarArrayPrivate<T> for UnsafeArray<T> {
    fn inner_array(&self) -> &UnsafeArray<T> {
        self
    }
    fn local_as_ptr(&self) -> *const T {
        self.local_as_mut_ptr()
    }
    fn local_as_mut_ptr(&self) -> *mut T {
        self.local_as_mut_ptr()
    }
    fn pe_for_dist_index(&self, index: usize) -> Option<usize> {
        self.inner.pe_for_dist_index(index)
    }
    fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> Option<usize> {
        self.inner.pe_offset_for_dist_index(pe, index)
    }

    unsafe fn into_inner(self) -> UnsafeArray<T> {
        self
    }
}

impl<T: Dist> LamellarArray<T> for UnsafeArray<T> {
    fn my_pe(&self) -> usize {
        self.inner.data.my_pe
    }
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.team().clone()
    }

    fn num_elems_local(&self) -> usize {
        self.inner.num_elems_local()
    }
    fn len(&self) -> usize {
        self.len()
    }
    fn barrier(&self) {
        self.barrier();
    }
    fn wait_all(&self) {
        let mut temp_now = Instant::now();
        // let mut first = true;
        while self
            .inner
            .data
            .array_counters
            .outstanding_reqs
            .load(Ordering::SeqCst)
            > 0
            || self.inner.data.req_cnt.load(Ordering::SeqCst) > 0
        {
            // std::thread::yield_now();
            self.inner.data.team.scheduler.exec_task(); //mmight as well do useful work while we wait
            if temp_now.elapsed() > Duration::new(60, 0) {
                //|| first{
                println!(
                    "in team wait_all mype: {:?} cnt: {:?} {:?} {:?}",
                    self.inner.data.team.world_pe,
                    self.inner
                        .data
                        .array_counters
                        .send_req_cnt
                        .load(Ordering::SeqCst),
                    self.inner
                        .data
                        .array_counters
                        .outstanding_reqs
                        .load(Ordering::SeqCst),
                    self.inner.data.req_cnt.load(Ordering::SeqCst)
                );
                temp_now = Instant::now();
                // first = false;
            }
        }
        self.inner.data.task_group.wait_all();
        // println!("done in wait all {:?}",std::time::SystemTime::now());
    }
    fn pe_and_offset_for_global_index(&self, index: usize) -> Option<(usize, usize)> {
        let pe = self.inner.pe_for_dist_index(index)?;
        let offset = self.inner.pe_offset_for_dist_index(pe, index)?;
        Some((pe, offset))
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
        self.inner.offset + sub_index
    }
}

impl<T: Dist + std::fmt::Debug> UnsafeArray<T> {
    pub fn print(&self) {
        <UnsafeArray<T> as ArrayPrint<T>>::print(&self);
    }
}

impl<T: Dist + std::fmt::Debug> ArrayPrint<T> for UnsafeArray<T> {
    fn print(&self) {
        self.inner.data.team.barrier(); //TODO: have barrier accept a string so we can print where we are stalling.
        for pe in 0..self.inner.data.team.num_pes() {
            self.inner.data.team.barrier();
            if self.inner.data.my_pe == pe {
                println!("[pe {:?} data] {:?}", pe, unsafe { self.local_as_slice() });
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
}

impl<T: Dist + AmDist + 'static> LamellarArrayReduce<T> for UnsafeArray<T> {
    fn get_reduction_op(&self, op: String) -> LamellarArcAm {
        //do this the same way we did add...
        // unsafe {
        REDUCE_OPS
            .get(&(std::any::TypeId::of::<T>(), op))
            .expect("unexpected reduction type")(
            self.clone().into(),
            self.inner.data.team.num_pes(),
        )
        // }
    }
    fn reduce(&self, op: &str) -> Pin<Box<dyn Future<Output = T>>> {
        self.reduce(op)
    }
    fn sum(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.sum()
    }
    fn max(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.max()
    }
    fn prod(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.prod()
    }
}

impl UnsafeArrayInnerWeak {
    pub fn upgrade(&self) -> Option<UnsafeArrayInner> {
        if let Some(data) = self.data.upgrade() {
            Some(UnsafeArrayInner {
                data: data,
                distribution: self.distribution.clone(),
                orig_elem_per_pe: self.orig_elem_per_pe,
                elem_size: self.elem_size,
                offset: self.offset,
                size: self.size,
            })
        } else {
            None
        }
    }
}

impl UnsafeArrayInner {
    pub(crate) fn downgrade(array: &UnsafeArrayInner) -> UnsafeArrayInnerWeak {
        UnsafeArrayInnerWeak {
            data: Darc::downgrade(&array.data),
            distribution: array.distribution.clone(),
            orig_elem_per_pe: array.orig_elem_per_pe,
            elem_size: array.elem_size,
            offset: array.offset,
            size: array.size,
        }
    }

    //index is relative to (sub)array (i.e. index=0 doesnt necessarily live on pe=0)
    pub(crate) fn pe_for_dist_index(&self, index: usize) -> Option<usize> {
        if self.size > index {
            let global_index = index + self.offset;
            match self.distribution {
                Distribution::Block => {
                    let mut pe = ((global_index) as f64 / self.orig_elem_per_pe).floor() as usize;
                    let end_index = (self.orig_elem_per_pe * (pe + 1) as f64).round() as usize;
                    // println!("pe {:?} size: {:?} index {:?} end_index {:?} global_index {:?}",pe,self.size,index,end_index,global_index);
                    if global_index >= end_index {
                        pe += 1;
                    }
                    Some(pe)
                }
                Distribution::Cyclic => Some(global_index % self.data.num_pes),
            }
        } else {
            None
        }
    }

    //index relative to subarray, return offset relative to subarray
    pub fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> Option<usize> {
        let global_index = self.offset + index;
        let num_elems_local = self.num_elems_pe(pe);
        match self.distribution {
            Distribution::Block => {
                // println!("{:?} {:?} {:?}",pe,index,num_elems_local);
                let pe_start_index = self.start_index_for_pe(pe)?;
                let pe_end_index = pe_start_index + num_elems_local;
                if pe_start_index <= index && index < pe_end_index {
                    Some(index - pe_start_index)
                } else {
                    None
                }
            }
            Distribution::Cyclic => {
                let num_pes = self.data.num_pes;
                if global_index % num_pes == pe {
                    Some(index / num_pes)
                } else {
                    None
                }
            }
        }
    }

    //index relative to subarray, return local offset relative to full array
    // pub fn pe_full_offset_for_dist_index(&self, pe: usize, index: usize) -> Option<usize> {
    //     let global_index = self.offset + index;
    //     println!("{:?} {:?} {:?}",global_index, self.offset, index);
    //     match self.distribution {
    //         Distribution::Block => {
    //             let pe_start_index = (self.orig_elem_per_pe * pe as f64).round() as usize;
    //             let pe_end_index = (self.orig_elem_per_pe * (pe+1) as f64).round() as usize;
    //             println!("{:?} {:?}",pe_start_index,pe_end_index);
    //             if pe_start_index <= global_index && global_index < pe_end_index{
    //                 Some(global_index - pe_start_index)
    //             }
    //             else{
    //                 None
    //             }
    //         }
    //         Distribution::Cyclic => {
    //             let num_pes = self.data.num_pes;
    //             if global_index% num_pes == pe{
    //                 Some(global_index/num_pes)
    //             }
    //             else{
    //                 None
    //             }
    //         }
    //     }
    // }

    //index is local with respect to subarray
    //returns local offset relative to full array
    pub fn pe_full_offset_for_local_index(&self, pe: usize, index: usize) -> Option<usize> {
        // let global_index = self.offset + index;
        let global_index = self.global_index_from_local(index)?;
        // println!("{:?} {:?} {:?}",global_index, self.offset, index);
        match self.distribution {
            Distribution::Block => {
                let pe_start_index = (self.orig_elem_per_pe * pe as f64).round() as usize;
                let pe_end_index = (self.orig_elem_per_pe * (pe + 1) as f64).round() as usize;
                // println!("{:?} {:?}",pe_start_index,pe_end_index);
                if pe_start_index <= global_index && global_index < pe_end_index {
                    Some(global_index - pe_start_index)
                } else {
                    None
                }
            }
            Distribution::Cyclic => {
                let num_pes = self.data.num_pes;
                if global_index % num_pes == pe {
                    Some(global_index / num_pes)
                } else {
                    None
                }
            }
        }
    }

    //index is local with respect to subarray
    //returns index with respect to original full length array
    pub(crate) fn global_index_from_local(&self, index: usize) -> Option<usize> {
        let my_pe = self.data.my_pe;
        match self.distribution {
            Distribution::Block => {
                let global_start = (self.orig_elem_per_pe * my_pe as f64).round() as usize;
                let start = global_start as isize - self.offset as isize;
                if start >= 0 {
                    //the (sub)array starts before my pe
                    if (start as usize) < self.size {
                        //sub(array) exists on my node
                        Some(global_start as usize + index)
                    } else {
                        //sub array does not exist on my node
                        None
                    }
                } else {
                    //inner starts on or after my pe
                    let global_end = (self.orig_elem_per_pe * (my_pe + 1) as f64).round() as usize;
                    if self.offset < global_end {
                        //the (sub)array starts on my pe
                        Some(self.offset + index)
                    } else {
                        //the (sub)array starts after my pe
                        None
                    }
                }
            }
            Distribution::Cyclic => {
                let num_pes = self.data.num_pes;
                let start_pe = self.pe_for_dist_index(0).unwrap();
                let end_pe = self.pe_for_dist_index(self.size - 1).unwrap();

                let mut num_elems = self.size / num_pes;
                // println!("{:?} {:?} {:?} {:?}",num_pes,start_pe,end_pe,num_elems);

                if self.size % num_pes != 0 {
                    //we have leftover elements
                    if start_pe <= end_pe {
                        //no wrap around occurs
                        if start_pe <= my_pe && my_pe <= end_pe {
                            num_elems += 1;
                        }
                    } else {
                        //wrap around occurs
                        if start_pe <= my_pe || my_pe <= end_pe {
                            num_elems += 1;
                        }
                    }
                }
                // println!("{:?} {:?} {:?} {:?}",num_pes,start_pe,end_pe,num_elems);

                if index < num_elems {
                    if start_pe <= my_pe {
                        Some(num_pes * index + self.offset + (my_pe - start_pe))
                    } else {
                        Some(num_pes * index + self.offset + (num_pes - start_pe) + my_pe)
                    }
                } else {
                    None
                }
            }
        }
    }

    //index is local with respect to subarray
    //returns index with respect to subarrayy
    pub(crate) fn subarray_index_from_local(&self, index: usize) -> Option<usize> {
        let my_pe = self.data.my_pe;
        let my_start_index = self.start_index_for_pe(my_pe)?; //None means subarray doesnt exist on this PE
        match self.distribution {
            Distribution::Block => {
                if my_start_index + index < self.size {
                    //local index is in subarray
                    Some(my_start_index + index)
                } else {
                    //local index outside subarray
                    None
                }
            }
            Distribution::Cyclic => {
                let num_pes = self.data.num_pes;
                let num_elems_local = self.num_elems_local();
                if index < num_elems_local {
                    //local index is in subarray
                    Some(my_start_index + num_pes * index)
                } else {
                    //local index outside subarray
                    None
                }
            }
        }
    }

    //return index relative to the subarray
    pub(crate) fn start_index_for_pe(&self, pe: usize) -> Option<usize> {
        match self.distribution {
            Distribution::Block => {
                let global_start = (self.orig_elem_per_pe * pe as f64).round() as usize;
                let start = global_start as isize - self.offset as isize;
                if start >= 0 {
                    //the (sub)array starts before my pe
                    if (start as usize) < self.size {
                        //sub(array) exists on my node
                        Some(start as usize)
                    } else {
                        //sub array does not exist on my node
                        None
                    }
                } else {
                    let global_end = (self.orig_elem_per_pe * (pe + 1) as f64).round() as usize;
                    if self.offset < global_end {
                        //the (sub)array starts on my pe
                        Some(0)
                    } else {
                        //the (sub)array starts after my pe
                        None
                    }
                }
            }
            Distribution::Cyclic => {
                let num_pes = self.data.num_pes;
                if let Some(start_pe) = self.pe_for_dist_index(0) {
                    let temp_len = if self.size < num_pes {
                        //sub array might not exist on my array
                        self.size
                    } else {
                        num_pes
                    };
                    for i in 0..temp_len {
                        if (i + start_pe) % num_pes == pe {
                            return Some(i);
                        }
                    }
                }
                None
            }
        }
    }

    pub(crate) fn num_elems_pe(&self, pe: usize) -> usize {
        match self.distribution {
            Distribution::Block => {
                if let Some(start_i) = self.start_index_for_pe(pe) {
                    //inner starts before or on pe
                    let end_i = if let Some(end_i) = self.start_index_for_pe(pe + 1) {
                        //inner ends after pe
                        end_i
                    } else {
                        //inner ends on pe
                        self.size
                    };
                    // println!("num_elems_pe pe {:?} si {:?} ei {:?}",pe,start_i,end_i);
                    end_i - start_i
                } else {
                    0
                }
            }
            Distribution::Cyclic => {
                let num_pes = self.data.num_pes;
                if let Some(start_pe) = self.pe_for_dist_index(0) {
                    let end_pe = self.pe_for_dist_index(self.size - 1).unwrap(); //inclusive
                    let mut num_elems = self.size / num_pes;
                    if self.size % num_pes != 0 {
                        //we have left over elements
                        if start_pe <= end_pe {
                            //no wrap around occurs
                            if pe >= start_pe && pe <= end_pe {
                                num_elems += 1
                            }
                        } else {
                            //wrap arround occurs
                            if pe >= start_pe || pe <= end_pe {
                                num_elems += 1
                            }
                        }
                    }
                    num_elems
                } else {
                    0
                }
            }
        }
    }
    pub(crate) fn num_elems_local(&self) -> usize {
        self.num_elems_pe(self.data.my_pe)
    }

    pub(crate) unsafe fn local_as_mut_slice(&self) -> &mut [u8] {
        let slice =
            self.data.mem_region.as_casted_mut_slice::<u8>().expect(
                "memory doesnt exist on this pe (this should not happen for arrays currently)",
            );
        // let len = self.size;
        let my_pe = self.data.my_pe;
        let num_pes = self.data.num_pes;
        let num_elems_local = self.num_elems_local();
        match self.distribution {
            Distribution::Block => {
                let start_pe = self.pe_for_dist_index(0).unwrap(); //index is relative to inner
                                                                   // let end_pe = self.pe_for_dist_index(len-1).unwrap();
                                                                   // println!("spe {:?} epe {:?}",start_pe,end_pe);
                let start_index = if my_pe == start_pe {
                    //inner starts on my pe
                    let global_start = (self.orig_elem_per_pe * my_pe as f64).round() as usize;
                    self.offset - global_start
                } else {
                    0
                };
                let end_index = start_index + num_elems_local;
                // println!("nel {:?} sao {:?} as slice si: {:?} ei {:?} elemsize {:?}",num_elems_local,self.offset,start_index,end_index,self.elem_size);
                &mut slice[start_index * self.elem_size..end_index * self.elem_size]
            }
            Distribution::Cyclic => {
                let global_index = self.offset;
                let start_index = global_index / num_pes
                    + if my_pe >= global_index % num_pes {
                        0
                    } else {
                        1
                    };
                let end_index = start_index + num_elems_local;
                // println!("si {:?}  ei {:?}",start_index,end_index);
                &mut slice[start_index * self.elem_size..end_index * self.elem_size]
            }
        }
    }
    pub(crate) unsafe fn local_as_mut_ptr(&self) -> *mut u8 {
        let ptr =
            self.data.mem_region.as_casted_mut_ptr::<u8>().expect(
                "memory doesnt exist on this pe (this should not happen for arrays currently)",
            );
        // let len = self.size;
        let my_pe = self.data.my_pe;
        let num_pes = self.data.num_pes;
        // let num_elems_local = self.num_elems_local();
        match self.distribution {
            Distribution::Block => {
                let start_pe = self.pe_for_dist_index(0).unwrap(); //index is relative to inner
                                                                   // let end_pe = self.pe_for_dist_index(len-1).unwrap();
                                                                   // println!("spe {:?} epe {:?}",start_pe,end_pe);
                let start_index = if my_pe == start_pe {
                    //inner starts on my pe
                    let global_start = (self.orig_elem_per_pe * my_pe as f64).round() as usize;
                    self.offset - global_start
                } else {
                    0
                };

                // println!("nel {:?} sao {:?} as slice si: {:?} ei {:?}",num_elems_local,self.offset,start_index,end_index);
                ptr.offset((start_index * self.elem_size) as isize)
            }
            Distribution::Cyclic => {
                let global_index = self.offset;
                let start_index = global_index / num_pes
                    + if my_pe >= global_index % num_pes {
                        0
                    } else {
                        1
                    };
                // println!("si {:?}  ei {:?}",start_index,end_index);
                ptr.offset((start_index * self.elem_size) as isize)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn pe_for_dist_index() {
        for num_pes in 2..200 {
            println!("num_pes {:?}", num_pes);
            for len in num_pes..2000 {
                let mut elems_per_pe = vec![0; num_pes];
                let mut pe_for_elem = vec![0; len];
                let epp = len as f32 / num_pes as f32;
                let mut cur_elem = 0;
                for pe in 0..num_pes {
                    elems_per_pe[pe] =
                        (((pe + 1) as f32 * epp).round() - (pe as f32 * epp).round()) as usize;
                    for _i in 0..elems_per_pe[pe] {
                        pe_for_elem[cur_elem] = pe;
                        cur_elem += 1;
                    }
                }
                for elem in 0..len {
                    //the actual calculation
                    let mut calc_pe = (((elem) as f32 / epp).floor()) as usize;
                    let end_i = (epp * (calc_pe + 1) as f32).round() as usize;
                    if elem >= end_i {
                        calc_pe += 1;
                    }
                    //--------------------
                    if calc_pe != pe_for_elem[elem] {
                        println!(
                            "npe: {:?} len {:?} e: {:?} eep: {:?} cpe: {:?}  ei {:?}",
                            num_pes,
                            len,
                            elem,
                            epp,
                            ((elem) as f32 / epp),
                            end_i
                        );
                        println!("{:?}", elems_per_pe);
                        println!("{:?}", pe_for_elem);
                    }
                    assert_eq!(calc_pe, pe_for_elem[elem]);
                }
            }
        }
    }
}

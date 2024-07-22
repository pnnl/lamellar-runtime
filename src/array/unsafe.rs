mod iteration;

pub(crate) mod local_chunks;
// pub use local_chunks::{};
pub(crate) mod operations;
mod rdma;

use crate::active_messaging::*;
// use crate::array::r#unsafe::operations::BUFOPS;
use crate::array::private::{ArrayExecAm, LamellarArrayPrivate};
use crate::array::*;
use crate::array::{LamellarRead, LamellarWrite};
use crate::barrier::BarrierHandle;
use crate::darc::{Darc, DarcMode, WeakDarc};
use crate::env_var::config;
use crate::lamellae::AllocationType;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::{Dist, MemoryRegion};
use crate::LamellarTaskGroup;

use core::marker::PhantomData;
use futures_util::{future, StreamExt};
use std::ops::Bound;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub(crate) struct UnsafeArrayData {
    mem_region: MemoryRegion<u8>,
    pub(crate) array_counters: Arc<AMCounters>,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) task_group: Arc<LamellarTaskGroup>,
    pub(crate) my_pe: usize,
    pub(crate) num_pes: usize,
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

/// An unsafe abstraction of a distributed array.
///
/// This array type provides no gaurantees on how it's data is being accessed, either locally or from remote PEs.
///
/// UnsafeArrays are really intended for use in the runtime as the foundation for our safe array types.
///
/// # Warning
/// Unless you are very confident in low level distributed memory access it is highly recommended you utilize the
/// the other LamellarArray types ([AtomicArray], [LocalLockArray], [GlobalLockArray], [ReadOnlyArray]) to construct and interact with distributed memory.
#[lamellar_impl::AmDataRT(Clone, Debug)]
pub struct UnsafeArray<T> {
    pub(crate) inner: UnsafeArrayInner,
    phantom: PhantomData<T>,
}

#[doc(hidden)]
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

#[doc(hidden)]
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

pub(crate) mod private {
    use super::UnsafeArrayData;
    use crate::array::Distribution;
    use crate::darc::Darc;
    #[lamellar_impl::AmDataRT(Clone, Debug)]
    pub struct UnsafeArrayInner {
        pub(crate) data: Darc<UnsafeArrayData>,
        pub(crate) distribution: Distribution,
        pub(crate) orig_elem_per_pe: usize,
        pub(crate) orig_remaining_elems: usize,
        pub(crate) elem_size: usize, //for bytes array will be size of T, for T array will be 1
        pub(crate) offset: usize,    //relative to size of T
        pub(crate) size: usize,      //relative to size of T
        pub(crate) sub: bool,
    }
}
use private::UnsafeArrayInner;

#[lamellar_impl::AmLocalDataRT(Clone, Debug)]
pub(crate) struct UnsafeArrayInnerWeak {
    pub(crate) data: WeakDarc<UnsafeArrayData>,
    pub(crate) distribution: Distribution,
    orig_elem_per_pe: usize,
    orig_remaining_elems: usize,
    elem_size: usize, //for bytes array will be size of T, for T array will be 1
    offset: usize,    //relative to size of T
    size: usize,      //relative to size of T
    sub: bool,
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

impl<T: Dist + ArrayOps + 'static> UnsafeArray<T> {
    #[doc(alias = "Collective")]
    /// Construct a new UnsafeArray with a length of `array_size` whose data will be layed out with the provided `distribution` on the PE's specified by the `team`.
    /// `team` is commonly a [LamellarWorld][crate::LamellarWorld] or [LamellarTeam][crate::LamellarTeam] (instance or reference).
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `team` to enter the constructor call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic);
    pub fn new<U: Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> UnsafeArray<T> {
        let team = team.into().team.clone();
        team.tasking_barrier();
        let task_group = LamellarTaskGroup::new(team.clone());
        let my_pe = team.team_pe_id().unwrap();
        let num_pes = team.num_pes();
        let full_array_size = std::cmp::max(array_size, num_pes);

        let elem_per_pe = full_array_size / num_pes;
        let remaining_elems = full_array_size % num_pes;
        let mut per_pe_size = elem_per_pe;
        if remaining_elems > 0 {
            per_pe_size += 1
        }
        // println!("new unsafe array {:?} {:?}", elem_per_pe, per_pe_size);
        let rmr_t: MemoryRegion<T> =
            MemoryRegion::new(per_pe_size, team.lamellae.clone(), AllocationType::Global);
        // let rmr = MemoryRegion::new(
        //     per_pe_size * std::mem::size_of::<T>(),
        //     team.lamellae.clone(),
        //     AllocationType::Global,
        // );
        // println!("new array {:?}",rmr_t.as_ptr());

        unsafe {
            // for elem in rmr_t.as_mut_slice().expect("data should exist on pe") {
            //     *elem = std::mem::zeroed();
            // }
            if std::mem::needs_drop::<T>() {
                // If `T` needs to be dropped then we have to do this one item at a time, in
                // case one of the intermediate drops does a panic.
                // slice.iter_mut().for_each(write_zeroes);
                panic!("need drop not yet supported");
            } else {
                // Otherwise we can be really fast and just fill everthing with zeros.
                let len = std::mem::size_of_val::<[T]>(
                    rmr_t.as_mut_slice().expect("data should exist on pe"),
                );
                std::ptr::write_bytes(
                    rmr_t.as_mut_ptr().expect("data should exist on pe") as *mut u8,
                    0u8,
                    len,
                )
            }
        }
        let rmr = unsafe { rmr_t.to_base::<u8>() };
        // println!("new array u8 {:?}",rmr.as_ptr());

        let data = Darc::try_new_with_drop(
            team.clone(),
            UnsafeArrayData {
                mem_region: rmr,
                array_counters: Arc::new(AMCounters::new()),
                team: team.clone(),
                task_group: Arc::new(task_group),
                my_pe: my_pe,
                num_pes: num_pes,
                req_cnt: Arc::new(AtomicUsize::new(0)),
            },
            crate::darc::DarcMode::UnsafeArray,
            None,
        )
        .expect("trying to create array on non team member");
        // println!("new unsafe array darc {:?}", data);
        // data.print();
        let array = UnsafeArray {
            inner: UnsafeArrayInner {
                data: data,
                distribution: distribution.clone(),
                // wait: wait,
                orig_elem_per_pe: elem_per_pe,
                orig_remaining_elems: remaining_elems,
                elem_size: std::mem::size_of::<T>(),
                offset: 0,             //relative to size of T
                size: full_array_size, //relative to size of T
                sub: false,
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
        if full_array_size != array_size {
            println!("WARNING: Array size {array_size} is less than number of pes {full_array_size}, each PE will not contain data");
            array.sub_array(0..array_size)
        } else {
            array
        }
        // println!("after buffered ops");
        // array.inner.data.print();
    }

    pub(crate) async fn async_new<U: Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> UnsafeArray<T> {
        let team = team.into().team.clone();
        team.async_barrier().await;
        let task_group = LamellarTaskGroup::new(team.clone());
        let my_pe = team.team_pe_id().unwrap();
        let num_pes = team.num_pes();
        let full_array_size = std::cmp::max(array_size, num_pes);

        let elem_per_pe = full_array_size / num_pes;
        let remaining_elems = full_array_size % num_pes;
        let mut per_pe_size = elem_per_pe;
        if remaining_elems > 0 {
            per_pe_size += 1
        }
        let rmr_t: MemoryRegion<T> =
            MemoryRegion::new(per_pe_size, team.lamellae.clone(), AllocationType::Global);
        // let rmr = MemoryRegion::new(
        //     per_pe_size * std::mem::size_of::<T>(),
        //     team.lamellae.clone(),
        //     AllocationType::Global,
        // );

        unsafe {
            // for elem in rmr_t.as_mut_slice().expect("data should exist on pe") {
            //     *elem = std::mem::zeroed();
            // }
            if std::mem::needs_drop::<T>() {
                // If `T` needs to be dropped then we have to do this one item at a time, in
                // case one of the intermediate drops does a panic.
                // slice.iter_mut().for_each(write_zeroes);
                panic!("need drop not yet supported");
            } else {
                // Otherwise we can be really fast and just fill everthing with zeros.
                let len = std::mem::size_of_val::<[T]>(
                    rmr_t.as_mut_slice().expect("data should exist on pe"),
                );
                std::ptr::write_bytes(
                    rmr_t.as_mut_ptr().expect("data should exist on pe") as *mut u8,
                    0u8,
                    len,
                )
            }
        }
        let rmr = unsafe { rmr_t.to_base::<u8>() };

        let data = Darc::try_new_with_drop(
            team.clone(),
            UnsafeArrayData {
                mem_region: rmr,
                array_counters: Arc::new(AMCounters::new()),
                team: team.clone(),
                task_group: Arc::new(task_group),
                my_pe: my_pe,
                num_pes: num_pes,
                req_cnt: Arc::new(AtomicUsize::new(0)),
            },
            crate::darc::DarcMode::UnsafeArray,
            None,
        )
        .expect("trying to create array on non team member");
        let array = UnsafeArray {
            inner: UnsafeArrayInner {
                data: data,
                distribution: distribution.clone(),
                // wait: wait,
                orig_elem_per_pe: elem_per_pe,
                orig_remaining_elems: remaining_elems,
                elem_size: std::mem::size_of::<T>(),
                offset: 0,             //relative to size of T
                size: full_array_size, //relative to size of T
                sub: false,
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
        if full_array_size != array_size {
            println!("WARNING: Array size {array_size} is less than number of pes {full_array_size}, each PE will not contain data");
            array.sub_array(0..array_size)
        } else {
            array
        }
        // println!("after buffered ops");
        // array.inner.data.print();
    }
}
impl<T: Dist + 'static> UnsafeArray<T> {
    #[doc(alias("One-sided", "onesided"))]
    /// Change the distribution this array handle uses to index into the data of the array.
    ///
    /// # One-sided Operation
    /// This is a one-sided call and does not redistribute the modify actual data, it simply changes how the array is indexed for this particular handle.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic);
    /// // do something interesting... or not
    /// let block_view = array.clone().use_distribution(Distribution::Block);
    ///```
    pub fn use_distribution(mut self, distribution: Distribution) -> Self {
        self.inner.distribution = distribution;
        self
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the calling PE's local data as an immutable slice
    ///
    /// # Safety
    /// Data in UnsafeArrays are always unsafe as there are no protections on how remote PE's may access this PE's local data.
    /// It is also possible to have mutable and immutable references to this arrays data on the same PE
    ///
    /// # One-sided Operation
    /// Only returns local data on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic);
    ///
    /// unsafe {
    ///     let slice = array.local_as_slice();
    ///     println!("PE{my_pe} data: {slice:?}");
    /// }
    ///```
    pub unsafe fn local_as_slice(&self) -> &[T] {
        self.local_as_mut_slice()
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the calling PE's local data as a mutable slice
    ///
    /// # Safety
    /// Data in UnsafeArrays are always unsafe as there are no protections on how remote PE's may access this PE's local data.
    /// It is also possible to have mutable and immutable references to this arrays data on the same PE
    ///
    /// # One-sided Operation
    /// Only returns local data on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic);
    ///
    /// unsafe {
    ///     let slice =  array.local_as_mut_slice();
    ///     for elem in slice{
    ///         *elem += 1;
    ///     }
    /// }
    ///```
    pub unsafe fn local_as_mut_slice(&self) -> &mut [T] {
        let u8_slice = self.inner.local_as_mut_slice();
        // println!("u8 slice {:?} u8_len {:?} len {:?}",u8_slice,u8_slice.len(),u8_slice.len()/std::mem::size_of::<T>());
        std::slice::from_raw_parts_mut(
            u8_slice.as_mut_ptr() as *mut T,
            u8_slice.len() / std::mem::size_of::<T>(),
        )
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the calling PE's local data as an immutable slice
    ///
    /// # Safety
    /// Data in UnsafeArrays are always unsafe as there are no protections on how remote PE's may access this PE's local data.
    /// It is also possible to have mutable and immutable references to this arrays data on the same PE
    ///
    /// # One-sided Operation
    /// Only returns local data on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic);
    /// unsafe {
    ///     let slice = array.local_data();
    ///     println!("PE{my_pe} data: {slice:?}");
    /// }
    ///```
    pub unsafe fn local_data(&self) -> &[T] {
        self.local_as_mut_slice()
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the calling PE's local data as a mutable slice
    ///
    /// # Safety
    /// Data in UnsafeArrays are always unsafe as there are no protections on how remote PE's may access this PE's local data.
    /// It is also possible to have mutable and immutable references to this arrays data on the same PE
    ///
    /// # One-sided Operation
    /// Only returns local data on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic);
    ///
    /// unsafe {
    ///     let slice = array.mut_local_data();
    ///     for elem in slice{
    ///         *elem += 1;
    ///     }
    /// }
    ///```
    pub unsafe fn mut_local_data(&self) -> &mut [T] {
        self.local_as_mut_slice()
    }

    pub(crate) fn local_as_mut_ptr(&self) -> *mut T {
        let u8_ptr = unsafe { self.inner.local_as_mut_ptr() };
        // self.inner.data.mem_region.as_casted_mut_ptr::<T>().unwrap();
        // println!("ptr: {:?} {:?}", u8_ptr, u8_ptr as *const T);
        u8_ptr as *mut T
    }

    /// Return the index range with respect to the original array over which this array handle represents)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic);
    ///
    /// assert_eq!(array.sub_array_range(),(0..100));
    ///
    /// let sub_array = array.sub_array(25..75);
    /// assert_eq!(sub_array.sub_array_range(),(25..75));
    ///```
    pub fn sub_array_range(&self) -> std::ops::Range<usize> {
        self.inner.offset..(self.inner.offset + self.inner.size)
    }

    pub(crate) fn team_rt(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.inner.data.team.clone()
    }

    pub(crate) async fn await_all(&self) {
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
            // self.inner.data.team.flush();
            // self.inner.data.team.scheduler.exec_task(); //mmight as well do useful work while we wait
            async_std::task::yield_now().await;
            if temp_now.elapsed().as_secs_f64() > config().deadlock_timeout {
                //|| first{
                println!(
                    "in array await_all mype: {:?} cnt: {:?} {:?} {:?}",
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
        self.inner.data.task_group.await_all().await;
        // println!("done in wait all {:?}",std::time::SystemTime::now());
    }

    pub(crate) fn block_on_outstanding(&self, mode: DarcMode) {
        self.wait_all();
        // println!("block on outstanding");
        // self.inner.data.print();
        // let the_array: UnsafeArray<T> = self.clone();
        let array_darc = self.inner.data.clone();
        self.team_rt()
            .block_on(array_darc.block_on_outstanding(mode, 1)); //one for this instance of the array
    }

    pub(crate) async fn await_on_outstanding(&self, mode: DarcMode) {
        self.await_all().await;
        // println!("block on outstanding");
        // self.inner.data.print();
        // let the_array: UnsafeArray<T> = self.clone();
        let array_darc = self.inner.data.clone();
        array_darc.block_on_outstanding(mode, 1).await;
    }

    #[doc(alias = "Collective")]
    /// Convert this UnsafeArray into a (safe) [ReadOnlyArray][crate::array::ReadOnlyArray]
    ///
    /// This is a collective and blocking function which will only return when there is at most a single reference on each PE
    /// to this Array, and that reference is currently calling this function.
    ///
    /// When it returns, it is gauranteed that there are only  `ReadOnlyArray` handles to the underlying data
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `array` to enter the call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let read_only_array = array.into_read_only();
    ///```
    ///
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let array1 = array.clone();
    /// let mut_slice = unsafe {array1.local_as_mut_slice()};
    ///
    /// // no borrows to this specific instance (array) so it can enter the "into_read_only" call
    /// // but array1 will not be dropped until after mut_slice is dropped.
    /// // Given the ordering of these calls we will get stuck in "into_read_only" as it
    /// // waits for the reference count to go down to "1" (but we will never be able to drop mut_slice/array1).
    /// let ro_array = array.into_read_only();
    /// ro_array.print();
    /// println!("{mut_slice:?}");
    ///```
    pub fn into_read_only(self) -> ReadOnlyArray<T> {
        // println!("unsafe into read only");
        self.into()
    }

    // pub fn into_local_only(self) -> LocalOnlyArray<T> {
    //     // println!("unsafe into local only");
    //     self.into()
    // }

    #[doc(alias = "Collective")]
    /// Convert this UnsafeArray into a (safe) [LocalLockArray][crate::array::LocalLockArray]
    ///
    /// This is a collective and blocking function which will only return when there is at most a single reference on each PE
    /// to this Array, and that reference is currently calling this function.
    ///
    /// When it returns, it is gauranteed that there are only `LocalLockArray` handles to the underlying data
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `array` to enter the call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_lock_array = array.into_local_lock();
    ///```
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let array1 = array.clone();
    /// let mut_slice = unsafe {array1.local_as_mut_slice()};
    ///
    /// // no borrows to this specific instance (array) so it can enter the "into_local_lock" call
    /// // but array1 will not be dropped until after mut_slice is dropped.
    /// // Given the ordering of these calls we will get stuck in "iinto_local_lock" as it
    /// // waits for the reference count to go down to "1" (but we will never be able to drop mut_slice/array1).
    /// let local_lock_array = array.into_local_lock();
    /// local_lock_array.print();
    /// println!("{mut_slice:?}");
    ///```
    pub fn into_local_lock(self) -> LocalLockArray<T> {
        // println!("unsafe into local lock atomic");
        self.into()
    }

    #[doc(alias = "Collective")]
    /// Convert this UnsafeArray into a (safe) [GlobalLockArray][crate::array::GlobalLockArray]
    ///
    /// This is a collective and blocking function which will only return when there is at most a single reference on each PE
    /// to this Array, and that reference is currently calling this function.
    ///
    /// When it returns, it is gauranteed that there are only `GlobalLockArray` handles to the underlying data
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `array` to enter the call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let global_lock_array = array.into_global_lock();
    ///```
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let array1 = array.clone();
    /// let slice = unsafe {array1.local_data()};
    ///
    /// // no borrows to this specific instance (array) so it can enter the "into_global_lock" call
    /// // but array1 will not be dropped until after mut_slice is dropped.
    /// // Given the ordering of these calls we will get stuck in "into_global_lock" as it
    /// // waits for the reference count to go down to "1" (but we will never be able to drop slice/array1).
    /// let global_lock_array = array.into_global_lock();
    /// global_lock_array.print();
    /// println!("{slice:?}");
    ///```
    pub fn into_global_lock(self) -> GlobalLockArray<T> {
        // println!("readonly into_global_lock");
        self.into()
    }

    pub(crate) fn tasking_barrier(&self) {
        self.inner.data.team.tasking_barrier();
    }

    pub(crate) fn async_barrier(&self) -> impl std::future::Future<Output = ()> + Send + '_ {
        self.inner.data.team.async_barrier()
    }
}

impl<T: Dist + 'static> UnsafeArray<T> {
    #[doc(alias = "Collective")]
    /// Convert this UnsafeArray into a (safe) [AtomicArray][crate::array::AtomicArray]
    ///
    /// This is a collective and blocking function which will only return when there is at most a single reference on each PE
    /// to this Array, and that reference is currently calling this function.
    ///
    /// When it returns, it is gauranteed that there are only `AtomicArray` handles to the underlying data
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `array` to enter the call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let atomic_array = array.into_local_lock();
    ///```
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let array1 = array.clone();
    /// let mut_slice = unsafe {array1.local_as_mut_slice()};
    ///
    /// // no borrows to this specific instance (array) so it can enter the "into_atomic" call
    /// // but array1 will not be dropped until after mut_slice is dropped.
    /// // Given the ordering of these calls we will get stuck in "into_atomic" as it
    /// // waits for the reference count to go down to "1" (but we will never be able to drop mut_slice/array1).
    /// let atomic_array = array.into_local_lock();
    /// atomic_array.print();
    /// println!("{mut_slice:?}");
    ///```
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

impl<T: Dist + ArrayOps> TeamFrom<(Vec<T>, Distribution)> for UnsafeArray<T> {
    fn team_from(input: (Vec<T>, Distribution), team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let (vals, distribution) = input;
        let input = (&vals, distribution);
        TeamInto::team_into(input, team)
    }
}

// #[async_trait]
impl<T: Dist + ArrayOps> AsyncTeamFrom<(Vec<T>, Distribution)> for UnsafeArray<T> {
    async fn team_from(input: (Vec<T>, Distribution), team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let (local_vals, distribution) = input;
        // println!("local_vals len: {:?}", local_vals.len());
        team.async_barrier().await;
        let local_sizes =
            UnsafeArray::<usize>::async_new(team.clone(), team.num_pes, Distribution::Block).await;
        unsafe {
            local_sizes.local_as_mut_slice()[0] = local_vals.len();
        }
        team.async_barrier().await;
        // local_sizes.barrier();
        let mut size = 0;
        let mut my_start = 0;
        let my_pe = team.team_pe.expect("pe not part of team");
        unsafe {
            local_sizes
                .buffered_onesided_iter(team.num_pes)
                .into_stream()
                .enumerate()
                .for_each(|(i, local_size)| {
                    size += local_size;
                    if i < my_pe {
                        my_start += local_size;
                    }
                    future::ready(())
                })
                .await;
        }
        let array = UnsafeArray::<T>::async_new(team.clone(), size, distribution).await;
        if local_vals.len() > 0 {
            unsafe { array.put(my_start, local_vals).await };
        }
        team.async_barrier().await;
        array
    }
}

impl<T: Dist + ArrayOps> TeamFrom<(&Vec<T>, Distribution)> for UnsafeArray<T> {
    fn team_from(input: (&Vec<T>, Distribution), team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        if std::thread::current().id() != *crate::MAIN_THREAD {
            let msg = format!("
                [LAMELLAR WARNING] You are calling `Array::team_from` from within an async context which may lead to deadlock, this is unintended and likely a Runtime bug.
                Please open a github issue at https://github.com/pnnl/lamellar-runtime/issues including a backtrace if possible.
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {:?}", std::backtrace::Backtrace::capture()
            );
            match config().blocking_call_warning {
                Some(val) if val => println!("{msg}"),
                _ => println!("{msg}"),
            }
        }
        let (local_vals, distribution) = input;
        // println!("local_vals len: {:?}", local_vals.len());
        team.tasking_barrier();
        let local_sizes =
            UnsafeArray::<usize>::new(team.clone(), team.num_pes, Distribution::Block);
        unsafe {
            local_sizes.local_as_mut_slice()[0] = local_vals.len();
        }
        local_sizes.barrier();
        let mut size = 0;
        let mut my_start = 0;
        let my_pe = team.team_pe.expect("pe not part of team");
        unsafe {
            local_sizes
                .buffered_onesided_iter(team.num_pes)
                .into_iter()
                .enumerate()
                .for_each(|(i, local_size)| {
                    size += local_size;
                    if i < my_pe {
                        my_start += local_size;
                    }
                });
        }
        let array = UnsafeArray::<T>::new(team.clone(), size, distribution);
        if local_vals.len() > 0 {
            array.block_on(unsafe { array.put(my_start, local_vals) });
        }
        array.barrier();
        array
    }
}

impl<T: Dist> From<AtomicArray<T>> for UnsafeArray<T> {
    fn from(array: AtomicArray<T>) -> Self {
        match array {
            AtomicArray::NativeAtomicArray(array) => UnsafeArray::<T>::from(array),
            AtomicArray::GenericAtomicArray(array) => UnsafeArray::<T>::from(array),
        }
    }
}

impl<T: Dist> From<NativeAtomicArray<T>> for UnsafeArray<T> {
    fn from(array: NativeAtomicArray<T>) -> Self {
        array.array.block_on_outstanding(DarcMode::UnsafeArray);
        array.array
    }
}

impl<T: Dist> From<GenericAtomicArray<T>> for UnsafeArray<T> {
    fn from(array: GenericAtomicArray<T>) -> Self {
        array.array.block_on_outstanding(DarcMode::UnsafeArray);
        array.array
    }
}

impl<T: Dist> From<LocalLockArray<T>> for UnsafeArray<T> {
    fn from(array: LocalLockArray<T>) -> Self {
        array.array.block_on_outstanding(DarcMode::UnsafeArray);
        array.array
    }
}

impl<T: Dist> From<GlobalLockArray<T>> for UnsafeArray<T> {
    fn from(array: GlobalLockArray<T>) -> Self {
        array.array.block_on_outstanding(DarcMode::UnsafeArray);
        array.array
    }
}

impl<T: Dist> From<ReadOnlyArray<T>> for UnsafeArray<T> {
    fn from(array: ReadOnlyArray<T>) -> Self {
        array.array.block_on_outstanding(DarcMode::UnsafeArray);
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

impl<T: Dist> From<&UnsafeByteArray> for UnsafeArray<T> {
    fn from(array: &UnsafeByteArray) -> Self {
        UnsafeArray {
            inner: array.inner.clone(),
            phantom: PhantomData,
        }
    }
}

impl<T: Dist> From<UnsafeArray<T>> for UnsafeByteArray {
    fn from(array: UnsafeArray<T>) -> Self {
        UnsafeByteArray { inner: array.inner }
    }
}

impl<T: Dist> From<&UnsafeArray<T>> for UnsafeByteArray {
    fn from(array: &UnsafeArray<T>) -> Self {
        UnsafeByteArray {
            inner: array.inner.clone(),
        }
    }
}

impl<T: Dist> From<UnsafeArray<T>> for LamellarByteArray {
    fn from(array: UnsafeArray<T>) -> Self {
        LamellarByteArray::UnsafeArray(array.into())
    }
}

impl<T: Dist> From<LamellarByteArray> for UnsafeArray<T> {
    fn from(array: LamellarByteArray) -> Self {
        if let LamellarByteArray::UnsafeArray(array) = array {
            array.into()
        } else {
            panic!("Expected LamellarByteArray::UnsafeArray")
        }
    }
}

impl<T: Dist> ArrayExecAm<T> for UnsafeArray<T> {
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.team_rt().clone()
    }
    fn team_counters(&self) -> Arc<AMCounters> {
        self.inner.data.array_counters.clone()
    }
}
impl<T: Dist> LamellarArrayPrivate<T> for UnsafeArray<T> {
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
        if self.inner.sub {
            self.inner.pe_sub_offset_for_dist_index(pe, index)
        } else {
            self.inner.pe_full_offset_for_dist_index(pe, index)
        }
    }

    unsafe fn into_inner(self) -> UnsafeArray<T> {
        self
    }
    fn as_lamellar_byte_array(&self) -> LamellarByteArray {
        self.clone().into()
    }
}

impl<T: Dist> LamellarArray<T> for UnsafeArray<T> {
    fn team_rt(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.inner.data.team.clone()
    }

    // fn my_pe(&self) -> usize {
    //     self.inner.data.my_pe
    // }

    // fn num_pes(&self) -> usize {
    //     self.inner.data.num_pes
    // }

    fn len(&self) -> usize {
        self.inner.size
    }

    fn num_elems_local(&self) -> usize {
        self.inner.num_elems_local()
    }

    fn barrier(&self) {
        self.inner.data.team.tasking_barrier();
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
            // self.inner.data.team.flush();
            self.inner.data.team.scheduler.exec_task(); //mmight as well do useful work while we wait
            if temp_now.elapsed().as_secs_f64() > config().deadlock_timeout {
                //|| first{
                println!(
                    "in array wait_all mype: {:?} cnt: {:?} {:?} {:?}",
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

    fn block_on<F: Future>(&self, f: F) -> F::Output {
        self.inner.data.team.scheduler.block_on(f)
    }

    //#[tracing::instrument(skip_all)]
    fn pe_and_offset_for_global_index(&self, index: usize) -> Option<(usize, usize)> {
        if self.inner.sub {
            let pe = self.inner.pe_for_dist_index(index)?;
            let offset = self.inner.pe_sub_offset_for_dist_index(pe, index)?;
            Some((pe, offset))
        } else {
            self.inner.full_pe_and_offset_for_global_index(index)
        }
    }

    fn first_global_index_for_pe(&self, pe: usize) -> Option<usize> {
        self.inner.start_index_for_pe(pe)
    }

    fn last_global_index_for_pe(&self, pe: usize) -> Option<usize> {
        self.inner.end_index_for_pe(pe)
    }
}

impl<T: Dist> LamellarEnv for UnsafeArray<T> {
    fn my_pe(&self) -> usize {
        self.inner.data.my_pe
    }

    fn num_pes(&self) -> usize {
        self.inner.data.num_pes
    }

    fn num_threads_per_pe(&self) -> usize {
        self.inner.data.team.num_threads()
    }
    fn world(&self) -> Arc<LamellarTeam> {
        self.inner.data.team.world()
    }
    fn team(&self) -> Arc<LamellarTeam> {
        self.inner.data.team.team()
    }
}

impl<T: Dist> LamellarWrite for UnsafeArray<T> {}
impl<T: Dist> LamellarRead for UnsafeArray<T> {}

impl<T: Dist> SubArray<T> for UnsafeArray<T> {
    type Array = UnsafeArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array {
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
        inner.sub = true;
        UnsafeArray {
            inner: inner,
            phantom: PhantomData,
        }
    }
    fn global_index(&self, sub_index: usize) -> usize {
        self.inner.offset + sub_index
    }
}

impl<T: Dist + std::fmt::Debug> UnsafeArray<T> {
    #[doc(alias = "Collective")]
    /// Print the data within a lamellar array
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the array to enter the print call otherwise deadlock will occur (i.e. barriers are being called internally)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let block_array = UnsafeArray::<usize>::new(&world,100,Distribution::Block);
    /// let cyclic_array = UnsafeArray::<usize>::new(&world,100,Distribution::Block);
    ///
    /// block_array.dist_iter().zip(cyclic_array.dist_iter()).enumerate().for_each(move |i,(a,b)| {
    ///     a.store(i);
    ///     b.store(i);
    /// });
    /// block_array.print();
    /// println!();
    /// cyclic_array.print();
    ///```
    pub fn print(&self) {
        <UnsafeArray<T> as ArrayPrint<T>>::print(&self);
    }
}

impl<T: Dist + std::fmt::Debug> ArrayPrint<T> for UnsafeArray<T> {
    fn print(&self) {
        self.inner.data.team.tasking_barrier(); //TODO: have barrier accept a string so we can print where we are stalling.
        for pe in 0..self.inner.data.team.num_pes() {
            self.inner.data.team.tasking_barrier();
            if self.inner.data.my_pe == pe {
                println!("[pe {:?} data] {:?}", pe, unsafe { self.local_as_slice() });
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
}

impl<T: Dist + AmDist + 'static> UnsafeArray<T> {
    pub(crate) fn get_reduction_op(
        &self,
        op: &str,
        byte_array: LamellarByteArray,
    ) -> LamellarArcAm {
        REDUCE_OPS
            .get(&(std::any::TypeId::of::<T>(), op))
            .expect("unexpected reduction type")(byte_array, self.inner.data.team.num_pes())
    }
    pub(crate) fn reduce_data(
        &self,
        op: &str,
        byte_array: LamellarByteArray,
    ) -> AmHandle<Option<T>> {
        let func = self.get_reduction_op(op, byte_array);
        if let Ok(my_pe) = self.inner.data.team.team_pe_id() {
            self.inner.data.team.exec_arc_am_pe::<Option<T>>(
                my_pe,
                func,
                Some(self.inner.data.array_counters.clone()),
            )
        } else {
            self.inner.data.team.exec_arc_am_pe::<Option<T>>(
                0,
                func,
                Some(self.inner.data.array_counters.clone()),
            )
        }
    }
}

// This is esentially impl LamellarArrayReduce, but we man to explicity have UnsafeArray expose unsafe functions
impl<T: Dist + AmDist + 'static> UnsafeArray<T> {
    #[doc(alias("One-sided", "onesided"))]
    /// Perform a reduction on the entire distributed array, returning the value to the calling PE.
    ///
    /// Please see the documentation for the [register_reduction][lamellar_impl::register_reduction] procedural macro for
    /// more details and examples on how to create your own reductions.
    ///
    /// # Safety
    /// Data in UnsafeArrays are always unsafe as there are no protections on how remote PE's or local threads may access this PE's local data.
    /// Any updates to local data are not guaranteed to be Atomic.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Reduce` active messages on the other PEs associated with the array.
    /// the returned reduction result is only available on the calling PE  
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// use rand::Rng;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = AtomicArray::<usize>::new(&world,1000000,Distribution::Block);
    /// let array_clone = array.clone();
    /// unsafe { // THIS IS NOT SAFE -- we are randomly updating elements, no protections, updates may be lost... DONT DO THIS
    ///     let req = array.local_iter().for_each(move |_| {
    ///         let index = rand::thread_rng().gen_range(0..array_clone.len());
    ///        array_clone.add(index,1); //randomly at one to an element in the array.
    ///     });
    /// }
    /// array.wait_all();
    /// array.barrier();
    /// let sum = array.block_on(array.reduce("sum")); // equivalent to calling array.sum()
    /// //assert_eq!(array.len()*num_pes,sum); // may or may not fail
    ///```
    pub unsafe fn reduce(&self, op: &str) -> AmHandle<Option<T>> {
        self.reduce_data(op, self.clone().into())
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Perform a reduction on the entire distributed array, returning the value to the calling PE.
    ///
    /// Please see the documentation for the [register_reduction][lamellar_impl::register_reduction] procedural macro for
    /// more details and examples on how to create your own reductions.
    ///
    /// # Safety
    /// Data in UnsafeArrays are always unsafe as there are no protections on how remote PE's or local threads may access this PE's local data.
    /// Any updates to local data are not guaranteed to be Atomic.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Reduce` active messages on the other PEs associated with the array.
    /// the returned reduction result is only available on the calling PE  
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// use rand::Rng;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = AtomicArray::<usize>::new(&world,1000000,Distribution::Block);
    /// let array_clone = array.clone();
    /// unsafe { // THIS IS NOT SAFE -- we are randomly updating elements, no protections, updates may be lost... DONT DO THIS
    ///     let req = array.local_iter().for_each(move |_| {
    ///         let index = rand::thread_rng().gen_range(0..array_clone.len());
    ///        array_clone.add(index,1); //randomly at one to an element in the array.
    ///     });
    /// }
    /// array.wait_all();
    /// array.barrier();
    /// let sum = array.blocking_reduce("sum"); // equivalent to calling array.sum()
    /// //assert_eq!(array.len()*num_pes,sum); // may or may not fail
    ///```
    pub unsafe fn blocking_reduce(&self, op: &str) -> Option<T> {
        if std::thread::current().id() != *crate::MAIN_THREAD {
            let msg = format!("
                [LAMELLAR WARNING] You are calling `UnsafeArray::blocking_reduce` from within an async context which may lead to deadlock, it is recommended that you use `reduce(...).await;` instead! 
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {:?}", std::backtrace::Backtrace::capture()
            );
            match config().blocking_call_warning {
                Some(val) if val => println!("{msg}"),
                _ => println!("{msg}"),
            }
        }
        self.block_on(self.reduce_data(op, self.clone().into()))
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Perform a sum reduction on the entire distributed array, returning the value to the calling PE.
    ///
    /// This equivalent to `reduce("sum")`.
    ///
    /// # Safety
    /// Data in UnsafeArrays are always unsafe as there are no protections on how remote PE's or local threads may access this PE's local data.
    /// Any updates to local data are not guaranteed to be Atomic.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Sum` active messages on the other PEs associated with the array.
    /// the returned sum reduction result is only available on the calling PE  
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// use rand::Rng;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = UnsafeArray::<usize>::new(&world,1000000,Distribution::Block);
    /// let array_clone = array.clone();
    /// unsafe { // THIS IS NOT SAFE -- we are randomly updating elements, no protections, updates may be lost... DONT DO THIS
    ///     let req = array.local_iter().for_each(move |_| {
    ///         let index = rand::thread_rng().gen_range(0..array_clone.len());
    ///        array_clone.add(index,1); //randomly at one to an element in the array.
    ///     });
    /// }
    /// array.wait_all();
    /// array.barrier();
    /// let sum = array.block_on(unsafe{array.sum()}); //Safe in this instance as we have ensured no updates are currently happening
    /// // assert_eq!(array.len()*num_pes,sum);//this may or may not fail
    ///```
    pub unsafe fn sum(&self) -> AmHandle<Option<T>> {
        self.reduce("sum")
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Perform a sum reduction on the entire distributed array, returning the value to the calling PE.
    ///
    /// This equivalent to `reduce("sum")`.
    ///
    /// # Safety
    /// Data in UnsafeArrays are always unsafe as there are no protections on how remote PE's or local threads may access this PE's local data.
    /// Any updates to local data are not guaranteed to be Atomic.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Sum` active messages on the other PEs associated with the array.
    /// the returned sum reduction result is only available on the calling PE  
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// use rand::Rng;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = UnsafeArray::<usize>::new(&world,1000000,Distribution::Block);
    /// let array_clone = array.clone();
    /// unsafe { // THIS IS NOT SAFE -- we are randomly updating elements, no protections, updates may be lost... DONT DO THIS
    ///     let req = array.local_iter().for_each(move |_| {
    ///         let index = rand::thread_rng().gen_range(0..array_clone.len());
    ///        array_clone.add(index,1); //randomly at one to an element in the array.
    ///     });
    /// }
    /// array.wait_all();
    /// array.barrier();
    /// let sum = unsafe{array.blocking_sum()};
    ///```
    pub unsafe fn blocking_sum(&self) -> Option<T> {
        self.blocking_reduce("sum")
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Perform a production reduction on the entire distributed array, returning the value to the calling PE.
    ///
    /// This equivalent to `reduce("prod")`.
    ///
    /// # Safety
    /// Data in UnsafeArrays are always unsafe as there are no protections on how remote PE's or local threads may access this PE's local data.
    /// Any updates to local data are not guaranteed to be Atomic.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Prod` active messages on the other PEs associated with the array.
    /// the returned prod reduction result is only available on the calling PE  
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// use rand::Rng;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = UnsafeArray::<usize>::new(&world,10,Distribution::Block);
    /// unsafe {
    ///     let req = array.dist_iter_mut().enumerate().for_each(move |(i,elem)| {
    ///         *elem = i+1;
    ///     });
    /// }
    /// array.print();
    /// array.wait_all();
    /// array.print();
    /// let prod = unsafe{ array.block_on(array.prod())};
    /// assert_eq!((1..=array.len()).product::<usize>(),prod);
    ///```
    pub unsafe fn prod(&self) -> AmHandle<Option<T>> {
        self.reduce("prod")
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Perform a production reduction on the entire distributed array, returning the value to the calling PE.
    ///
    /// This equivalent to `reduce("prod")`.
    ///
    /// # Safety
    /// Data in UnsafeArrays are always unsafe as there are no protections on how remote PE's or local threads may access this PE's local data.
    /// Any updates to local data are not guaranteed to be Atomic.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Prod` active messages on the other PEs associated with the array.
    /// the returned prod reduction result is only available on the calling PE  
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// use rand::Rng;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = UnsafeArray::<usize>::new(&world,10,Distribution::Block);
    /// unsafe {
    ///     let req = array.dist_iter_mut().enumerate().for_each(move |(i,elem)| {
    ///         *elem = i+1;
    ///     });
    /// }
    /// array.print();
    /// array.wait_all();
    /// array.print();
    /// let array = array.into_read_only(); //only returns once there is a single reference remaining on each PE
    /// array.print();
    /// let prod =  unsafe{array.blocking_prod()};
    /// assert_eq!((1..=array.len()).product::<usize>(),prod);
    ///```
    pub unsafe fn blocking_prod(&self) -> Option<T> {
        self.blocking_reduce("prod")
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Find the max element in the entire destributed array, returning to the calling PE
    ///
    /// This equivalent to `reduce("max")`.
    ///
    /// # Safety
    /// Data in UnsafeArrays are always unsafe as there are no protections on how remote PE's or local threads may access this PE's local data.
    /// Any updates to local data are not guaranteed to be Atomic.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Max` active messages on the other PEs associated with the array.
    /// the returned max reduction result is only available on the calling PE  
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = UnsafeArray::<usize>::new(&world,10,Distribution::Block);
    /// let array_clone = array.clone();
    /// unsafe{array.dist_iter_mut().enumerate().for_each(|(i,elem)| *elem = i*2)}; //safe as we are accessing in a data parallel fashion
    /// array.wait_all();
    /// array.barrier();
    /// let max_req = unsafe{array.max()}; //Safe in this instance as we have ensured no updates are currently happening
    /// let max = array.block_on(max_req);
    /// assert_eq!((array.len()-1)*2,max);
    ///```
    pub unsafe fn max(&self) -> AmHandle<Option<T>> {
        self.reduce("max")
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Find the max element in the entire destributed array, returning to the calling PE
    ///
    /// This equivalent to `reduce("max")`.
    ///
    /// # Safety
    /// Data in UnsafeArrays are always unsafe as there are no protections on how remote PE's or local threads may access this PE's local data.
    /// Any updates to local data are not guaranteed to be Atomic.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Max` active messages on the other PEs associated with the array.
    /// the returned max reduction result is only available on the calling PE  
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = UnsafeArray::<usize>::new(&world,10,Distribution::Block);
    /// let array_clone = array.clone();
    /// unsafe{array.dist_iter_mut().enumerate().for_each(|(i,elem)| *elem = i*2)}; //safe as we are accessing in a data parallel fashion
    /// array.wait_all();
    /// array.barrier();
    /// let max = unsafe{array.blocking_max()};
    /// assert_eq!((array.len()-1)*2,max);
    ///```
    pub unsafe fn blocking_max(&self) -> Option<T> {
        self.blocking_reduce("max")
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Find the min element in the entire destributed array, returning to the calling PE
    ///
    /// This equivalent to `reduce("min")`.
    ///
    /// # Safety
    /// Data in UnsafeArrays are always unsafe as there are no protections on how remote PE's or local threads may access this PE's local data.
    /// Any updates to local data are not guaranteed to be Atomic.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Min` active messages on the other PEs associated with the array.
    /// the returned min reduction result is only available on the calling PE  
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = UnsafeArray::<usize>::new(&world,10,Distribution::Block);
    /// let array_clone = array.clone();
    /// unsafe{array.dist_iter_mut().enumerate().for_each(|(i,elem)| *elem = i*2)}; //safe as we are accessing in a data parallel fashion
    /// array.wait_all();
    /// array.barrier();
    /// let min_req = unsafe{array.min()}; //Safe in this instance as we have ensured no updates are currently happening
    /// let min = array.block_on(min_req);
    /// assert_eq!(0,min);
    ///```
    pub unsafe fn min(&self) -> AmHandle<Option<T>> {
        self.reduce("min")
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Find the min element in the entire destributed array, returning to the calling PE
    ///
    /// This equivalent to `reduce("min")`.
    ///
    /// # Safety
    /// Data in UnsafeArrays are always unsafe as there are no protections on how remote PE's or local threads may access this PE's local data.
    /// Any updates to local data are not guaranteed to be Atomic.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Min` active messages on the other PEs associated with the array.
    /// the returned min reduction result is only available on the calling PE  
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = UnsafeArray::<usize>::new(&world,10,Distribution::Block);
    /// let array_clone = array.clone();
    /// unsafe{array.dist_iter_mut().enumerate().for_each(|(i,elem)| *elem = i*2)}; //safe as we are accessing in a data parallel fashion
    /// array.wait_all();
    /// array.barrier();
    /// let min = unsafe{array.blocking_min()};
    /// assert_eq!(0,min);
    ///```
    pub unsafe fn blocking_min(&self) -> Option<T> {
        self.blocking_reduce("min")
    }
}

impl UnsafeArrayInnerWeak {
    pub(crate) fn upgrade(&self) -> Option<UnsafeArrayInner> {
        if let Some(data) = self.data.upgrade() {
            Some(UnsafeArrayInner {
                data: data,
                distribution: self.distribution.clone(),
                orig_elem_per_pe: self.orig_elem_per_pe,
                orig_remaining_elems: self.orig_remaining_elems,
                elem_size: self.elem_size,
                offset: self.offset,
                size: self.size,
                sub: self.sub,
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
            orig_remaining_elems: array.orig_remaining_elems,
            elem_size: array.elem_size,
            offset: array.offset,
            size: array.size,
            sub: array.sub,
        }
    }

    pub(crate) fn full_pe_and_offset_for_global_index(
        &self,
        index: usize,
    ) -> Option<(usize, usize)> {
        if self.size > index {
            let mut global_index = index;
            match self.distribution {
                Distribution::Block => {
                    let rem_index = self.orig_remaining_elems * (self.orig_elem_per_pe + 1);
                    let mut elem_per_pe = self.orig_elem_per_pe;
                    if rem_index < self.size {
                        elem_per_pe += 1;
                    } else {
                        global_index = global_index - rem_index;
                    }
                    let (pe, offset) = if global_index < rem_index {
                        (global_index / elem_per_pe, global_index % elem_per_pe)
                    } else {
                        (
                            rem_index / elem_per_pe
                                + (global_index - rem_index) / self.orig_elem_per_pe,
                            global_index % self.orig_elem_per_pe,
                        )
                    };

                    Some((pe, offset))
                }
                Distribution::Cyclic => {
                    let res = Some((
                        global_index % self.data.num_pes,
                        global_index / self.data.num_pes,
                    ));
                    res
                }
            }
        } else {
            None
        }
    }

    //index is relative to (sub)array (i.e. index=0 doesnt necessarily live on pe=0)
    // //#[tracing::instrument(skip_all)]
    pub(crate) fn pe_for_dist_index(&self, index: usize) -> Option<usize> {
        if self.size > index {
            let mut global_index = index + self.offset;
            match self.distribution {
                Distribution::Block => {
                    let rem_index = self.orig_remaining_elems * (self.orig_elem_per_pe + 1);
                    let mut elem_per_pe = self.orig_elem_per_pe;
                    if rem_index < self.size {
                        elem_per_pe += 1;
                    } else {
                        global_index = global_index - rem_index;
                    }
                    let pe = if global_index < rem_index {
                        global_index / elem_per_pe
                    } else {
                        rem_index / elem_per_pe + (global_index - rem_index) / self.orig_elem_per_pe
                    };
                    Some(pe)
                }
                Distribution::Cyclic => Some(global_index % self.data.num_pes),
            }
        } else {
            None
        }
    }

    //index relative to subarray, return offset relative to subarray
    // //#[tracing::instrument(skip_all)]
    pub(crate) fn pe_full_offset_for_dist_index(&self, pe: usize, index: usize) -> Option<usize> {
        let mut global_index = self.offset + index;

        match self.distribution {
            Distribution::Block => {
                let rem_index = self.orig_remaining_elems * (self.orig_elem_per_pe + 1);
                let mut elem_per_pe = self.orig_elem_per_pe;
                if rem_index < self.size {
                    elem_per_pe += 1;
                } else {
                    global_index = global_index - rem_index;
                }
                let offset = if global_index < rem_index {
                    global_index % elem_per_pe
                } else {
                    global_index % self.orig_elem_per_pe
                };
                Some(offset)
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

    //index relative to subarray, return offset relative to subarray
    pub(crate) fn pe_sub_offset_for_dist_index(&self, pe: usize, index: usize) -> Option<usize> {
        let offset = self.pe_full_offset_for_dist_index(pe, index)?;
        match self.distribution {
            Distribution::Block => {
                if self.offset <= offset {
                    Some(offset - self.offset)
                } else {
                    None
                }
            }
            Distribution::Cyclic => {
                let num_pes = self.data.num_pes;
                if (index + self.offset) % num_pes == pe {
                    Some(index / num_pes)
                } else {
                    None
                }
            }
        }
    }

    //index is local with respect to subarray
    //returns local offset relative to full array
    // //#[tracing::instrument(skip_all)]
    pub(crate) fn pe_full_offset_for_local_index(&self, pe: usize, index: usize) -> Option<usize> {
        let global_index = self.global_index_from_local(index)?;
        match self.distribution {
            Distribution::Block => {
                let pe_start_index = self.global_start_index_for_pe(pe);
                let mut pe_end_index = pe_start_index + self.orig_elem_per_pe;
                if pe < self.orig_remaining_elems {
                    pe_end_index += 1;
                }
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
    // //#[tracing::instrument(skip_all)]
    pub(crate) fn global_index_from_local(&self, index: usize) -> Option<usize> {
        let my_pe = self.data.my_pe;
        match self.distribution {
            Distribution::Block => {
                let global_start = self.global_start_index_for_pe(my_pe);
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
                    let mut global_end = global_start + self.orig_elem_per_pe;
                    if my_pe < self.orig_remaining_elems {
                        global_end += 1;
                    }
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
                let start_pe = match self.pe_for_dist_index(0) {
                    Some(i) => i,
                    None => panic!("index 0 out of bounds for array of length {:?}", self.size),
                };
                let end_pe = match self.pe_for_dist_index(self.size - 1) {
                    Some(i) => i,
                    None => panic!(
                        "index {:?} out of bounds for array of length {:?}",
                        self.size - 1,
                        self.size
                    ),
                };

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
    // //#[tracing::instrument(skip_all)]
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

    // return index relative to the full array
    pub(crate) fn global_start_index_for_pe(&self, pe: usize) -> usize {
        match self.distribution {
            Distribution::Block => {
                let global_start = self.orig_elem_per_pe * pe;
                global_start + std::cmp::min(pe, self.orig_remaining_elems)
            }
            Distribution::Cyclic => pe,
        }
    }
    //return index relative to the subarray
    // //#[tracing::instrument(skip_all)]
    pub(crate) fn start_index_for_pe(&self, pe: usize) -> Option<usize> {
        match self.distribution {
            Distribution::Block => {
                let global_start = self.global_start_index_for_pe(pe);
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
                    let mut global_end = global_start + self.orig_elem_per_pe;
                    if pe < self.orig_remaining_elems {
                        global_end += 1;
                    }
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

    #[allow(dead_code)]
    pub(crate) fn global_end_index_for_pe(&self, pe: usize) -> usize {
        self.global_start_index_for_pe(pe) + self.num_elems_pe(pe)
    }

    //return index relative to the subarray
    // //#[tracing::instrument(skip_all)]
    pub(crate) fn end_index_for_pe(&self, pe: usize) -> Option<usize> {
        let start_i = self.start_index_for_pe(pe)?;
        match self.distribution {
            Distribution::Block => {
                //(sub)array ends on our pe
                if pe == self.pe_for_dist_index(self.size - 1)? {
                    Some(self.size - 1)
                } else {
                    // (sub)array ends on another pe
                    Some(self.start_index_for_pe(pe + 1)? - 1)
                }
            }
            Distribution::Cyclic => {
                let num_elems = self.num_elems_pe(pe);
                let num_pes = self.data.num_pes;
                let end_i = start_i + (num_elems - 1) * num_pes;
                Some(end_i)
            }
        }
    }

    // //#[tracing::instrument(skip_all)]
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
                    let end_pe = match self.pe_for_dist_index(self.size - 1) {
                        Some(i) => i,
                        None => panic!(
                            "index {:?} out of bounds for array of length {:?}",
                            self.size - 1,
                            self.size
                        ),
                    }; //inclusive
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
    // //#[tracing::instrument(skip_all)]
    pub(crate) fn num_elems_local(&self) -> usize {
        self.num_elems_pe(self.data.my_pe)
    }

    // //#[tracing::instrument(skip_all)]
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
                let start_pe = self
                    .pe_for_dist_index(0)
                    .expect("array len should be greater than 0"); //index is relative to inner
                                                                   // let end_pe = self.pe_for_dist_index(len-1).unwrap();
                                                                   // println!("spe {:?} epe {:?}",start_pe,end_pe);
                let start_index = if my_pe == start_pe {
                    //inner starts on my pe
                    let global_start = self.global_start_index_for_pe(my_pe);
                    self.offset - global_start
                } else {
                    0
                };
                let end_index = start_index + num_elems_local;
                // println!(
                //     "nel {:?} sao {:?} as slice si: {:?} ei {:?} elemsize {:?}",
                //     num_elems_local, self.offset, start_index, end_index, self.elem_size
                // );
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

    // //#[tracing::instrument(skip_all)]
    pub(crate) unsafe fn local_as_mut_ptr(&self) -> *mut u8 {
        let ptr =
            self.data.mem_region.as_casted_mut_ptr::<u8>().expect(
                "memory doesnt exist on this pe (this should not happen for arrays currently)",
            );
        // println!("u8 ptr: {:?}", ptr);
        // let len = self.size;
        let my_pe = self.data.my_pe;
        let num_pes = self.data.num_pes;
        // let num_elems_local = self.num_elems_local();
        match self.distribution {
            Distribution::Block => {
                let start_pe = self
                    .pe_for_dist_index(0)
                    .expect("array len should be greater than 0"); //index is relative to inner
                                                                   // let end_pe = self.pe_for_dist_index(len-1).unwrap();
                                                                   // println!("spe {:?} epe {:?}",start_pe,end_pe);
                let start_index = if my_pe == start_pe {
                    //inner starts on my pe
                    let global_start = self.global_start_index_for_pe(my_pe);
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

    fn barrier_handle(&self) -> BarrierHandle {
        self.data.team.barrier.barrier_handle()
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

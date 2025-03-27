pub(crate) mod handle;
pub use handle::ReadOnlyArrayHandle;

mod iteration;
pub(crate) mod local_chunks;
pub use local_chunks::ReadOnlyLocalChunks;
mod rdma;
use crate::array::private::ArrayExecAm;
use crate::array::*;
use crate::barrier::BarrierHandle;
use crate::darc::DarcMode;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::Dist;
use crate::scheduler::LamellarTask;

use std::sync::Arc;

/// A safe abstraction of a distributed array, providing only read access.
#[lamellar_impl::AmDataRT(Clone, Debug)]
pub struct ReadOnlyArray<T> {
    pub(crate) array: UnsafeArray<T>,
}

#[doc(hidden)]
#[lamellar_impl::AmDataRT(Clone, Debug)]
pub struct ReadOnlyByteArray {
    pub(crate) array: UnsafeByteArray,
}
impl ReadOnlyByteArray {
    pub fn downgrade(array: &ReadOnlyByteArray) -> ReadOnlyByteArrayWeak {
        ReadOnlyByteArrayWeak {
            array: UnsafeByteArray::downgrade(&array.array),
        }
    }
    pub fn local_data<T: Dist>(&self) -> &[T] {
        self.array.local_data()
    }
}

#[doc(hidden)]
#[lamellar_impl::AmLocalDataRT(Clone, Debug)]
pub struct ReadOnlyByteArrayWeak {
    pub(crate) array: UnsafeByteArrayWeak,
}

impl ReadOnlyByteArrayWeak {
    pub fn upgrade(&self) -> Option<ReadOnlyByteArray> {
        Some(ReadOnlyByteArray {
            array: self.array.upgrade()?,
        })
    }
}

/// A safe abstraction of a distributed array, providing only read access.
///
/// This array type limits access of its data to `read only`, meaning it is not possible
/// to modify this array locally or from remote PEs.
///
/// Thanks to this gaurantee there is the potential for increased performance when ready remote data in this
/// array type as locking or atomic access is uneeded. For certain operations like `get()` it is possible to
/// directly do an RDMA transfer.
impl<T: Dist + ArrayOps> ReadOnlyArray<T> {
    #[doc(alias = "Collective")]
    /// Construct a new ReadOnlyArray with a length of `array_size` whose data will be layed out with the provided `distribution` on the PE's specified by the `team`.
    /// `team` is commonly a [LamellarWorld][crate::LamellarWorld] or [LamellarTeam][crate::LamellarTeam] (instance or reference).
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `team` to enter the constructor call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// It is not terribly useful to construct a new ReadOnlyArray as you will be unable to modify its data. Rather, it is common to convert
    /// some other array type into a ReadOnlyArray (using `into_read_only()`) after it has been initialized.
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic).block();
    pub fn new<U: Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> ReadOnlyArrayHandle<T> {
        let team = team.into().team.clone();
        ReadOnlyArrayHandle {
            team: team.clone(),
            launched: false,
            creation_future: Box::pin(UnsafeArray::async_new(
                team,
                array_size,
                distribution,
                DarcMode::ReadOnlyArray,
            )),
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Change the distribution this array handle uses to index into the data of the array.
    ///
    /// # One-sided Operation
    /// This is a one-sided call and does not redistribute or modify the actual data, it simply changes how the array is indexed for this particular handle.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic).block();
    /// // do something interesting... or not
    /// let block_view = array.clone().use_distribution(Distribution::Block);
    ///```
    pub fn use_distribution(self, distribution: Distribution) -> Self {
        ReadOnlyArray {
            array: self.array.use_distribution(distribution),
        }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the calling PE's local data as an immutable slice
    ///
    /// Note: this is safe for ReadOnlyArrays because they cannot be modified either remotely or locally
    ///
    /// # One-sided Operation
    /// Only returns local data on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic).block();
    ///
    /// let slice = array.local_as_slice();
    /// println!("PE{my_pe} data: {slice:?}");
    ///```
    pub fn local_as_slice(&self) -> &[T] {
        unsafe { self.array.local_as_mut_slice() }
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the calling PE's local data as an immutable slice
    ///
    /// Note: this is safe for ReadOnlyArrays because they cannot be modified either remotely or locally
    ///
    /// # One-sided Operation
    /// Only returns local data on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic).block();
    ///
    /// let slice = array.local_as_slice();
    /// println!("PE{my_pe} data: {slice:?}");
    ///```
    pub fn local_data(&self) -> &[T] {
        unsafe { self.array.local_as_mut_slice() }
    }

    #[doc(alias = "Collective")]
    /// Convert this ReadOnlyArray into an [UnsafeArray][crate::array::UnsafeArray]
    ///
    /// This is a collective and blocking function which will only return when there is at most a single reference on each PE
    /// to this Array, and that reference is currently calling this function.
    ///
    /// When it returns, it is gauranteed that there are only  `UnsafeArray` handles to the underlying data
    ///
    /// Note, that while this call itself is safe, and `UnsafeArray` unsurprisingly is not safe and thus you need to tread very carefully
    /// doing any operations with the resulting array.
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `array` to enter the call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic).block();
    ///
    /// let unsafe_array = array.into_unsafe().block();
    ///```
    ///
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic).block();
    ///
    /// let array1 = array.clone();
    /// let slice = array1.local_data();
    ///
    /// // no borrows to this specific instance (array) so it can enter the "into_unsafe" call
    /// // but array1 will not be dropped until after 'slice' is dropped.
    /// // Given the ordering of these calls we will get stuck in "into_unsafe" as it
    /// // waits for the reference count to go down to "1" (but we will never be able to drop slice/array1).
    /// let unsafe_array = array.into_unsafe().block();
    /// unsafe_array.print();
    /// println!("{slice:?}");
    ///```
    pub fn into_unsafe(self) -> IntoUnsafeArrayHandle<T> {
        // println!("readonly into_unsafe");
        IntoUnsafeArrayHandle {
            team: self.array.inner.data.team.clone(),
            launched: false,
            outstanding_future: Box::pin(self.async_into()),
        }
    }

    // pub fn into_local_only(self) -> LocalOnlyArray<T> {
    //     // println!("readonly into_local_only");
    //     self.array.into()
    // }

    #[doc(alias = "Collective")]
    /// Convert this ReadOnlyArray into a (safe) [LocalLockArray][crate::array::LocalLockArray]
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic).block();
    ///
    /// let local_lock_array = array.into_local_lock().block();
    ///```
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic).block();
    ///
    /// let array1 = array.clone();
    /// let slice = unsafe {array1.local_data()};
    ///
    /// // no borrows to this specific instance (array) so it can enter the "into_local_lock" call
    /// // but array1 will not be dropped until after mut_slice is dropped.
    /// // Given the ordering of these calls we will get stuck in "into_local_lock" as it
    /// // waits for the reference count to go down to "1" (but we will never be able to drop slice/array1).
    /// let local_lock_array = array.into_local_lock().block();
    /// local_lock_array.print();
    /// println!("{slice:?}");
    ///```
    pub fn into_local_lock(self) -> IntoLocalLockArrayHandle<T> {
        // println!("readonly into_local_lock");
        self.array.into_local_lock()
    }

    #[doc(alias = "Collective")]
    /// Convert this ReadOnlyArray into a (safe) [GlobalLockArray][crate::array::GlobalLockArray]
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic).block();
    ///
    /// let global_lock_array = array.into_global_lock().block();
    ///```
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic).block();
    ///
    /// let array1 = array.clone();
    /// let slice = unsafe {array1.local_data()};
    ///
    /// // no borrows to this specific instance (array) so it can enter the "into_global_lock" call
    /// // but array1 will not be dropped until after mut_slice is dropped.
    /// // Given the ordering of these calls we will get stuck in "into_global_lock" as it
    /// // waits for the reference count to go down to "1" (but we will never be able to drop slice/array1).
    /// let global_lock_array = array.into_global_lock().block();
    /// global_lock_array.print();
    /// println!("{slice:?}");
    ///```
    pub fn into_global_lock(self) -> IntoGlobalLockArrayHandle<T> {
        // println!("readonly into_global_lock");
        self.array.into_global_lock()
    }
}

impl<T: Dist + 'static> ReadOnlyArray<T> {
    #[doc(alias = "Collective")]
    /// Convert this ReadOnlyArray into a (safe) [AtomicArray][crate::array::AtomicArray]
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic).block();
    ///
    /// let atomic_array = array.into_local_lock().block();
    ///```
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic).block();
    ///
    /// let array1 = array.clone();
    /// let slice = array1.local_data();
    ///
    /// // no borrows to this specific instance (array) so it can enter the "into_atomic" call
    /// // but array1 will not be dropped until after slice is dropped.
    /// // Given the ordering of these calls we will get stuck in "into_atomic" as it
    /// // waits for the reference count to go down to "1" (but we will never be able to drop slice/array1).
    /// let atomic_array = array.into_local_lock().block();
    /// atomic_array.print();
    /// println!("{slice:?}");
    ///```
    pub fn into_atomic(self) -> IntoAtomicArrayHandle<T> {
        self.array.into_atomic()
    }
}

// #[async_trait]
impl<T: Dist + ArrayOps> AsyncTeamFrom<(Vec<T>, Distribution)> for ReadOnlyArray<T> {
    async fn team_from(input: (Vec<T>, Distribution), team: &Arc<LamellarTeam>) -> Self {
        let array: UnsafeArray<T> = AsyncTeamInto::team_into(input, team).await;
        array.async_into().await
    }
}

#[async_trait]
impl<T: Dist> AsyncFrom<UnsafeArray<T>> for ReadOnlyArray<T> {
    async fn async_from(array: UnsafeArray<T>) -> Self {
        // println!("readonly from UnsafeArray");
        array.await_on_outstanding(DarcMode::ReadOnlyArray).await;

        ReadOnlyArray { array: array }
    }
}

impl<T: Dist> From<ReadOnlyArray<T>> for ReadOnlyByteArray {
    fn from(array: ReadOnlyArray<T>) -> Self {
        ReadOnlyByteArray {
            array: array.array.into(),
        }
    }
}
impl<T: Dist> From<ReadOnlyByteArray> for ReadOnlyArray<T> {
    fn from(array: ReadOnlyByteArray) -> Self {
        ReadOnlyArray {
            array: array.array.into(),
        }
    }
}
impl<T: Dist> From<ReadOnlyArray<T>> for LamellarByteArray {
    fn from(array: ReadOnlyArray<T>) -> Self {
        LamellarByteArray::ReadOnlyArray(ReadOnlyByteArray {
            array: array.array.into(),
        })
    }
}

impl<T: Dist> From<LamellarByteArray> for ReadOnlyArray<T> {
    fn from(array: LamellarByteArray) -> Self {
        if let LamellarByteArray::ReadOnlyArray(array) = array {
            array.into()
        } else {
            panic!("Expected LamellarByteArray::ReadOnlyArray")
        }
    }
}

impl<T: Dist + AmDist + 'static> ReadOnlyArray<T> {
    #[doc(alias("One-sided", "onesided"))]
    /// Perform a reduction on the entire distributed array, returning the value to the calling PE.
    ///
    /// Please see the documentation for the [register_reduction] procedural macro for
    /// more details and examples on how to create your own reductions.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Reduce` active messages on the other PEs associated with the array.
    /// the returned reduction result is only available on the calling PE
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][AmHandle::spawn] or [blocked on][AmHandle::block]
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// use rand::Rng;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = AtomicArray::<usize>::new(&world,1000000,Distribution::Block).block();
    /// let array_clone = array.clone();
    /// let _ = array.local_iter().for_each(move |_| {
    ///     let index = rand::thread_rng().gen_range(0..array_clone.len());
    ///     let _ = array_clone.add(index,1).spawn(); //randomly at one to an element in the array.
    /// }).block();
    /// array.wait_all();
    /// let array = array.into_read_only().block(); //only returns once there is a single reference remaining on each PE
    /// let sum = array.reduce("sum").block().expect("array len > 0"); // equivalent to calling array.sum()
    /// assert_eq!(array.len()*num_pes,sum);
    ///```
    #[must_use = "this function is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it "]
    pub fn reduce(&self, op: &str) -> AmHandle<Option<T>> {
        self.array.reduce_data(op, self.clone().into())
    }
}
impl<T: Dist + AmDist + ElementArithmeticOps + 'static> ReadOnlyArray<T> {
    #[doc(alias("One-sided", "onesided"))]
    /// Perform a sum reduction on the entire distributed array, returning the value to the calling PE.
    ///
    /// This equivalent to `reduce("sum")`.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Sum` active messages on the other PEs associated with the array.
    /// the returned sum reduction result is only available on the calling PE
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][AmHandle::spawn] or [blocked on][AmHandle::block]
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// use rand::Rng;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = AtomicArray::<usize>::new(&world,1000000,Distribution::Block).block();
    /// let array_clone = array.clone();
    /// let _ = array.local_iter().for_each(move |_| {
    ///     let index = rand::thread_rng().gen_range(0..array_clone.len());
    ///     let _ = array_clone.add(index,1).spawn(); //randomly at one to an element in the array.
    /// }).block();
    /// array.wait_all();
    /// let array = array.into_read_only().block(); //only returns once there is a single reference remaining on each PE
    /// let sum = array.sum().block().expect("array len > 0");
    /// assert_eq!(array.len()*num_pes,sum);
    /// ```
    #[must_use = "this function is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it "]
    pub fn sum(&self) -> AmHandle<Option<T>> {
        self.reduce("sum")
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Perform a production reduction on the entire distributed array, returning the value to the calling PE.
    ///
    /// This equivalent to `reduce("prod")`.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Prod` active messages on the other PEs associated with the array.
    /// the returned prod reduction result is only available on the calling PE
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][AmHandle::spawn] or [blocked on][AmHandle::block]
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = AtomicArray::<usize>::new(&world,10,Distribution::Block).block();
    /// let _ = array.dist_iter().enumerate().for_each(move |(i,elem)| {
    ///     elem.store(i+1);
    /// }).block();
    /// array.wait_all();
    /// let array = array.into_read_only().block(); //only returns once there is a single reference remaining on each PE
    /// let prod = array.prod().block().expect("array len > 0");
    /// assert_eq!((1..=array.len()).product::<usize>(),prod);
    ///```
    #[must_use = "this function is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it "]
    pub fn prod(&self) -> AmHandle<Option<T>> {
        self.reduce("prod")
    }
}
impl<T: Dist + AmDist + ElementComparePartialEqOps + 'static> ReadOnlyArray<T> {
    #[doc(alias("One-sided", "onesided"))]
    /// Find the max element in the entire destributed array, returning to the calling PE
    ///
    /// This equivalent to `reduce("max")`.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Max` active messages on the other PEs associated with the array.
    /// the returned max reduction result is only available on the calling PE
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][AmHandle::spawn] or [blocked on][AmHandle::block]
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = AtomicArray::<usize>::new(&world,10,Distribution::Block).block();
    /// let _ = array.dist_iter().enumerate().for_each(move |(i,elem)| elem.store(i*2)).block();
    /// array.wait_all();
    /// let array = array.into_read_only().block(); //only returns once there is a single reference remaining on each PE
    /// let max = array.max().block().expect("array len > 0");
    /// assert_eq!((array.len()-1)*2,max);
    ///```
    #[must_use = "this function is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it "]
    pub fn max(&self) -> AmHandle<Option<T>> {
        self.reduce("max")
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Find the min element in the entire destributed array, returning to the calling PE
    ///
    /// This equivalent to `reduce("min")`.
    ///
    /// # One-sided Operation
    /// The calling PE is responsible for launching `Min` active messages on the other PEs associated with the array.
    /// the returned min reduction result is only available on the calling PE
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][AmHandle::spawn] or [blocked on][AmHandle::block]
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let array = AtomicArray::<usize>::new(&world,10,Distribution::Block).block();
    /// let _ = array.dist_iter().enumerate().for_each(move |(i,elem)| elem.store(i*2)).block();
    /// array.wait_all();
    /// let array = array.into_read_only().block(); //only returns once there is a single reference remaining on each PE
    /// let min = array.min().block().expect("array len > 0");
    /// assert_eq!(0,min);
    ///```
    #[must_use = "this function is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it "]
    pub fn min(&self) -> AmHandle<Option<T>> {
        self.reduce("min")
    }
}

impl<T: Dist> private::ArrayExecAm<T> for ReadOnlyArray<T> {
    fn team_rt(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team_rt()
    }
    fn team_counters(&self) -> Arc<AMCounters> {
        self.array.team_counters()
    }
}

impl<T: Dist> private::LamellarArrayPrivate<T> for ReadOnlyArray<T> {
    fn inner_array(&self) -> &UnsafeArray<T> {
        &self.array
    }
    fn local_as_ptr(&self) -> *const T {
        self.array.local_as_ptr()
    }
    fn local_as_mut_ptr(&self) -> *mut T {
        self.array.local_as_mut_ptr()
    }
    fn pe_for_dist_index(&self, index: usize) -> Option<usize> {
        self.array.pe_for_dist_index(index)
    }
    fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> Option<usize> {
        self.array.pe_offset_for_dist_index(pe, index)
    }
    unsafe fn into_inner(self) -> UnsafeArray<T> {
        self.array
    }
    fn as_lamellar_byte_array(&self) -> LamellarByteArray {
        self.clone().into()
    }
}

impl<T: Dist> ActiveMessaging for ReadOnlyArray<T> {
    type SinglePeAmHandle<R: AmDist> = AmHandle<R>;
    type MultiAmHandle<R: AmDist> = MultiAmHandle<R>;
    type LocalAmHandle<L> = LocalAmHandle<L>;
    fn exec_am_all<F>(&self, am: F) -> Self::MultiAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        self.array.exec_am_all_tg(am)
    }
    fn exec_am_pe<F>(&self, pe: usize, am: F) -> Self::SinglePeAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        self.array.exec_am_pe_tg(pe, am)
    }
    fn exec_am_local<F>(&self, am: F) -> Self::LocalAmHandle<F::Output>
    where
        F: LamellarActiveMessage + LocalAM + 'static,
    {
        self.array.exec_am_local_tg(am)
    }
    fn wait_all(&self) {
        self.array.wait_all()
    }
    fn await_all(&self) -> impl Future<Output = ()> + Send {
        self.array.await_all()
    }
    fn barrier(&self) {
        self.array.barrier()
    }
    fn async_barrier(&self) -> BarrierHandle {
        self.array.async_barrier()
    }
    fn spawn<F: Future>(&self, f: F) -> LamellarTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        self.array.spawn(f)
    }
    fn block_on<F: Future>(&self, f: F) -> F::Output {
        self.array.block_on(f)
    }
    fn block_on_all<I>(&self, iter: I) -> Vec<<<I as IntoIterator>::Item as Future>::Output>
    where
        I: IntoIterator,
        <I as IntoIterator>::Item: Future + Send + 'static,
        <<I as IntoIterator>::Item as Future>::Output: Send,
    {
        self.array.block_on_all(iter)
    }
}

impl<T: Dist> LamellarArray<T> for ReadOnlyArray<T> {
    // fn team_rt(&self) -> Pin<Arc<LamellarTeamRT>> {
    //     self.array.team_rt()
    // }
    // fn my_pe(&self) -> usize {
    //     LamellarArray::my_pe(&self.array)
    // }
    // fn num_pes(&self) -> usize {
    //     LamellarArray::num_pes(&self.array)
    // }
    fn len(&self) -> usize {
        self.array.len()
    }
    fn num_elems_local(&self) -> usize {
        self.array.num_elems_local()
    }
    // fn barrier(&self) {
    //     self.array.barrier();
    // }

    // fn wait_all(&self) {
    //     self.array.wait_all()
    //     // println!("done in wait all {:?}",std::time::SystemTime::now());
    // }
    // fn block_on<F: Future>(&self, f: F) -> F::Output {
    //     self.array.block_on(f)
    // }
    fn pe_and_offset_for_global_index(&self, index: usize) -> Option<(usize, usize)> {
        self.array.pe_and_offset_for_global_index(index)
    }

    fn first_global_index_for_pe(&self, pe: usize) -> Option<usize> {
        self.array.first_global_index_for_pe(pe)
    }

    fn last_global_index_for_pe(&self, pe: usize) -> Option<usize> {
        self.array.last_global_index_for_pe(pe)
    }
}

impl<T: Dist> LamellarEnv for ReadOnlyArray<T> {
    fn my_pe(&self) -> usize {
        LamellarEnv::my_pe(&self.array)
    }

    fn num_pes(&self) -> usize {
        LamellarEnv::num_pes(&self.array)
    }

    fn num_threads_per_pe(&self) -> usize {
        self.array.team_rt().num_threads()
    }
    fn world(&self) -> Arc<LamellarTeam> {
        self.array.team_rt().world()
    }
    fn team(&self) -> Arc<LamellarTeam> {
        self.array.team_rt().team()
    }
}

impl<T: Dist> SubArray<T> for ReadOnlyArray<T> {
    type Array = ReadOnlyArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array {
        ReadOnlyArray {
            array: self.array.sub_array(range),
        }
    }
    fn global_index(&self, sub_index: usize) -> usize {
        self.array.global_index(sub_index)
    }
}

impl<T: Dist + std::fmt::Debug> ReadOnlyArray<T> {
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
    /// let block_array = ReadOnlyArray::<usize>::new(&world,100,Distribution::Block).block();
    /// let cyclic_array = ReadOnlyArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// block_array.print();
    /// println!();
    /// cyclic_array.print();
    ///```
    pub fn print(&self) {
        self.array.print()
    }
}

impl<T: ElementOps + 'static> ReadOnlyOps<T> for ReadOnlyArray<T> {}

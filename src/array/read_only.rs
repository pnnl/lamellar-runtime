mod iteration;
mod rdma;
use crate::array::private::LamellarArrayPrivate;
use crate::array::*;
use crate::darc::DarcMode;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::memregion::Dist;
use std::any::TypeId;
use std::sync::Arc;

type BufFn = fn(ReadOnlyByteArrayWeak) -> Arc<dyn BufferOp>;

lazy_static! {
    pub(crate) static ref BUFOPS: HashMap<TypeId, BufFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<ReadOnlyArrayOpBuf> {
            map.insert(op.id.clone(), op.op);
        }
        map
    };
}

#[doc(hidden)]
pub struct ReadOnlyArrayOpBuf {
    pub id: TypeId,
    pub op: BufFn,
}

crate::inventory::collect!(ReadOnlyArrayOpBuf);

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
impl<T: Dist> ReadOnlyArray<T> {
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic);
    pub fn new<U: Into<IntoLamellarTeam>>(
        team: U,
        array_size: usize,
        distribution: Distribution,
    ) -> ReadOnlyArray<T> {
        let array = UnsafeArray::new(team, array_size, distribution);
        array.block_on_outstanding(DarcMode::ReadOnlyArray);
        if let Some(func) = BUFOPS.get(&TypeId::of::<T>()) {
            let mut op_bufs = array.inner.data.op_buffers.write();
            let bytearray = ReadOnlyByteArray {
                array: array.clone().into(),
            };

            for _pe in 0..array.num_pes() {
                op_bufs.push(func(ReadOnlyByteArray::downgrade(&bytearray)));
            }
        }
        ReadOnlyArray { array: array }
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic);
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic);
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic);
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let unsafe_array = array.into_unsafe();
    ///```
    ///
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let array1 = array.clone();
    /// let slice = array1.local_data();
    ///
    /// // no borrows to this specific instance (array) so it can enter the "into_unsafe" call
    /// // but array1 will not be dropped until after 'slice' is dropped.
    /// // Given the ordering of these calls we will get stuck in "into_unsafe" as it
    /// // waits for the reference count to go down to "1" (but we will never be able to drop slice/array1).
    /// let unsafe_array = array.into_unsafe();
    /// unsafe_array.print();
    /// println!("{slice:?}");
    ///```
    pub fn into_unsafe(self) -> UnsafeArray<T> {
        // println!("readonly into_unsafe");
        self.array.into()
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let local_lock_array = array.into_local_lock();
    ///```
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let array1 = array.clone();
    /// let slice = unsafe {array1.local_data()};
    ///
    /// // no borrows to this specific instance (array) so it can enter the "into_local_lock" call
    /// // but array1 will not be dropped until after mut_slice is dropped.
    /// // Given the ordering of these calls we will get stuck in "into_local_lock" as it
    /// // waits for the reference count to go down to "1" (but we will never be able to drop slice/array1).
    /// let local_lock_array = array.into_local_lock();
    /// local_lock_array.print();
    /// println!("{slice:?}");
    ///```
    pub fn into_local_lock(self) -> LocalLockArray<T> {
        // println!("readonly into_local_lock");
        self.array.into()
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let atomic_array = array.into_local_lock();
    ///```
    /// # Warning
    /// Because this call blocks there is the possibility for deadlock to occur, as highlighted below:
    ///```no_run
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic);
    ///
    /// let array1 = array.clone();
    /// let slice = array1.local_data();
    ///
    /// // no borrows to this specific instance (array) so it can enter the "into_atomic" call
    /// // but array1 will not be dropped until after slice is dropped.
    /// // Given the ordering of these calls we will get stuck in "into_atomic" as it
    /// // waits for the reference count to go down to "1" (but we will never be able to drop slice/array1).
    /// let atomic_array = array.into_local_lock();
    /// atomic_array.print();
    /// println!("{slice:?}");
    ///```
    pub fn into_atomic(self) -> AtomicArray<T> {
        self.array.into()
    }
}

impl<T: Dist> From<UnsafeArray<T>> for ReadOnlyArray<T> {
    fn from(array: UnsafeArray<T>) -> Self {
        // println!("readonly from UnsafeArray");
        array.block_on_outstanding(DarcMode::ReadOnlyArray);
        if let Some(func) = BUFOPS.get(&TypeId::of::<T>()) {
            let bytearray = ReadOnlyByteArray {
                array: array.clone().into(),
            };
            let mut op_bufs = array.inner.data.op_buffers.write();
            for _pe in 0..array.inner.data.num_pes {
                op_bufs.push(func(ReadOnlyByteArray::downgrade(&bytearray)));
            }
        }
        ReadOnlyArray { array: array }
    }
}

// impl<T: Dist> From<LocalOnlyArray<T>> for ReadOnlyArray<T> {
//     fn from(array: LocalOnlyArray<T>) -> Self {
//         // println!("readonly from LocalOnlyArray");
//         unsafe { array.into_inner().into() }
//     }
// }

impl<T: Dist> From<AtomicArray<T>> for ReadOnlyArray<T> {
    fn from(array: AtomicArray<T>) -> Self {
        // println!("readonly from AtomicArray");
        unsafe { array.into_inner().into() }
    }
}

impl<T: Dist> From<LocalLockArray<T>> for ReadOnlyArray<T> {
    fn from(array: LocalLockArray<T>) -> Self {
        // println!("readonly from LocalLockArray");
        unsafe { array.into_inner().into() }
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

impl<T: Dist + AmDist + 'static> LamellarArrayReduce<T> for ReadOnlyArray<T> {
    fn reduce(&self, op: &str) -> Pin<Box<dyn Future<Output = T>>> {
        self.array
            .reduce_data(op, self.clone().into())
            .into_future()
    }
}
impl<T: Dist + AmDist + ElementArithmeticOps + 'static> LamellarArrayArithmeticReduce<T>
    for ReadOnlyArray<T>
{
    fn sum(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.reduce("sum")
    }
    fn prod(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.reduce("prod")
    }
}
impl<T: Dist + AmDist + ElementComparePartialEqOps + 'static> LamellarArrayCompareReduce<T>
    for ReadOnlyArray<T>
{
    fn max(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.reduce("max")
    }
    fn min(&self) -> Pin<Box<dyn Future<Output = T>>> {
        self.reduce("min")
    }
}

impl<T: Dist> private::ArrayExecAm<T> for ReadOnlyArray<T> {
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team_rt().clone()
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
}

impl<T: Dist> LamellarArray<T> for ReadOnlyArray<T> {
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.array.team_rt().clone()
    }
    fn my_pe(&self) -> usize {
        self.array.my_pe()
    }
    fn num_pes(&self) -> usize {
        self.array.num_pes()
    }
    fn len(&self) -> usize {
        self.array.len()
    }
    fn num_elems_local(&self) -> usize {
        self.array.num_elems_local()
    }
    fn barrier(&self) {
        self.array.barrier();
    }
    fn wait_all(&self) {
        self.array.wait_all()
        // println!("done in wait all {:?}",std::time::SystemTime::now());
    }
    fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future,
    {
        self.array.block_on(f)
    }
    fn pe_and_offset_for_global_index(&self, index: usize) -> Option<(usize, usize)> {
        self.array.pe_and_offset_for_global_index(index)
    }

    fn first_global_index_for_pe(&self, pe: usize) -> Option<usize>{
        self.array.first_global_index_for_pe(pe)
    }

    fn last_global_index_for_pe(&self, pe: usize) -> Option<usize>{
        self.array.last_global_index_for_pe(pe)
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
    pub fn print(&self) {
        self.array.print()
    }
}

impl<T: ElementOps + 'static> ReadOnlyOps<T> for ReadOnlyArray<T> {}

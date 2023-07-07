//! Provides various iterator types for LamellarArrays
pub mod distributed_iterator;
use distributed_iterator::DistributedIterator;
pub mod local_iterator;
use local_iterator::LocalIterator;
pub mod one_sided_iterator;
use one_sided_iterator::OneSidedIterator;
pub mod consumer;
use consumer::IterConsumer;

use crate::memregion::Dist;

use async_trait::async_trait;

#[doc(hidden)]
#[async_trait]
pub trait IterRequest {
    type Output;
    async fn into_future(mut self: Box<Self>) -> Self::Output;
    fn wait(self: Box<Self>) -> Self::Output;
}


/// The Schedule type controls how elements of a LamellarArray are distributed to threads when
/// calling `for_each_with_schedule` on a local or distributed iterator.
///
/// Inspired by then OpenMP schedule parameter
///
/// # Possible Options
/// - Static: Each thread recieves a static range of elements to iterate over, the range length is roughly array.local_data().len()/number of threads on pe
/// - Dynaimc: Each thread processes a single element at a time
/// - Chunk(usize): Each thread prcesses chunk sized range of elements at a time.
/// - Guided: Similar to chunks, but the chunks decrease in size over time
/// - WorkStealing: Intially allocated the same range as static, but allows idle threads to steal work from busy threads
#[derive(Debug, Clone)]
pub enum Schedule {
    Static,
    Dynamic,      //single element
    Chunk(usize), //dynamic but with multiple elements
    Guided,       // chunks that get smaller over time
    WorkStealing, // static initially but other threads can steal
}

/// The interface for creating the various lamellar array iterator types
///
/// This is only implemented for Safe Array types, [UnsafeArray][crate::array::UnsafeArray] directly provides unsafe versions of the same functions
pub trait LamellarArrayIterators<T: Dist> {
    type DistIter: DistributedIterator;
    type LocalIter: LocalIterator;
    type OnesidedIter: OneSidedIterator;

    #[doc(alias = "Collective")]
    /// Create an immutable [DistributedIterator][crate::array::DistributedIterator] for this array
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the array to enter the call otherwise deadlock will occur (i.e. barriers are being called internally)
    /// Throughout execution of the iteration, data movement may occur amongst various PEs
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic);
    ///
    /// world.block_on(
    ///     array.dist_iter().for_each(move |elem| println!("PE{my_pe} elem {elem}"))
    /// );
    ///```
    fn dist_iter(&self) -> Self::DistIter;

    #[doc(alias("One-sided", "onesided"))]
    /// Create an immutable [LocalIterator][crate::array::LocalIterator] for this array
    ///
    /// # One-sided Operation
    /// The iteration is launched and local to only the calling PE.
    /// No data movement from remote PEs is required
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic);
    ///
    /// world.block_on(
    ///     array.local_iter().for_each(move |elem| println!("PE{my_pe} elem {}",elem.load())) // "load" is specific to AtomicArray elements, other types can deref the element directly"
    /// );
    ///```
    fn local_iter(&self) -> Self::LocalIter;

    #[doc(alias("One-sided", "onesided"))]
    /// Create an immutable [OneSidedIterator][crate::array::OneSidedIterator] for this array
    ///
    /// # One-sided Operation
    /// The iteration is launched and local to only the calling PE.
    /// Data movement will occur with the remote PEs to transfer their data to the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Cyclic);
    ///
    /// if my_pe == 0 {
    ///     for elem in array.onesided_iter().into_iter() { //"into_iter()" converts into a standard Rust Iterator
    ///         println!("PE{my_pe} elem {elem}");
    ///     }
    /// }
    ///```
    fn onesided_iter(&self) -> Self::OnesidedIter;

    #[doc(alias("One-sided", "onesided"))]
    /// Create an immutable [OneSidedIterator][crate::array::OneSidedIterator] for this array
    /// which will transfer and buffer `buf_size` elements at a time (to more efficient utilize the underlying lamellae network)
    ///
    /// The buffering is transparent to the user.
    ///
    /// This iterator typcially outperforms the non buffered version.
    ///
    /// # One-sided Operation
    /// The iteration is launched and local to only the calling PE.
    /// Data movement will occur with the remote PEs to transfer their data to the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// if my_pe == 0 {
    ///     for elem in array.buffered_onesided_iter(100).into_iter() { // "into_iter()" converts into a standard Rust Iterator
    ///         println!("PE{my_pe} elem {elem}");
    ///     }
    /// }
    ///```
    fn buffered_onesided_iter(&self, buf_size: usize) -> Self::OnesidedIter;
}

/// The interface for creating the various lamellar array mutable iterator types
///
/// This is only implemented for Safe Array types, [UnsafeArray][crate::array::UnsafeArray] directly provides unsafe versions of the same functions
pub trait LamellarArrayMutIterators<T: Dist> {
    /// Reference to the array being iterated
    // type Array: LamellarArray<T>;
    type DistIter: DistributedIterator;
    type LocalIter: LocalIterator;

    #[doc(alias = "Collective")]
    /// Create a mutable [DistributedIterator][crate::array::DistributedIterator] for this array
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the array to enter the call otherwise deadlock will occur (i.e. barriers are being called internally)
    /// Throughout execution of the iteration, data movement may occur amongst various PEs
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// world.block_on(
    ///     array.dist_iter_mut().for_each(move |elem| *elem = my_pe)
    /// );
    ///```
    fn dist_iter_mut(&self) -> Self::DistIter;

    #[doc(alias("One-sided", "onesided"))]
    /// Create a mutable [LocalIterator][crate::array::LocalIterator] for this array
    ///
    /// # One-sided Operation
    /// The iteration is launched and local to only the calling PE.
    /// No data movement from remote PEs is required
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,100,Distribution::Cyclic);
    ///
    /// world.block_on(
    ///    array.local_iter_mut().for_each(move |elem| *elem = my_pe)
    /// );
    fn local_iter_mut(&self) -> Self::LocalIter;
}

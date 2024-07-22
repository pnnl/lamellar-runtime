//! Distributed (and parallel) iteration of a LamellarArray
//!
//! This module provides distriubuted iteration capabilities for lamellar arrays,
//! where iteration over sub slices of the LamellarArray occurs in parallel on each PE.
//!
//! We try to minimize data movement as much as possible, but the runtime will manage remote transfers
//! as necessary depending on the iterators used.
//!
//! # Examples
//!
//! Examples can be found under [lamellar-runtime/examples/array_examples/](https://github.com/pnnl/lamellar-runtime/tree/master/examples/array_examples)

// mod chunks;
mod enumerate;
mod filter;
mod filter_map;
mod map;
mod monotonic;
mod skip;
mod step_by;
mod take;
// mod zip;

pub(crate) mod consumer;

// use chunks::*;
use enumerate::*;
use filter::*;
use filter_map::*;
use map::*;
use monotonic::*;
use skip::*;
use step_by::*;
use take::*;
// use zip::*;

pub(crate) use consumer::*;

use crate::array::iterator::{private::*, Schedule};
use crate::array::{
    operations::ArrayOps, AsyncTeamFrom, AtomicArray, Distribution, GenericAtomicArray, InnerArray,
    LamellarArray, NativeAtomicArray,
};
use crate::memregion::Dist;
use crate::LamellarTeamRT;

use crate::active_messaging::SyncSend;

use enum_dispatch::enum_dispatch;
use futures_util::Future;
use paste::paste;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

macro_rules! consumer_impl {
    ($name:ident<$($generics:ident),*>($($arg:ident : $arg_ty:ty),*); [$($return_type: tt)*]; [$($bounds:tt)+] ; [$(-> $($blocking_ret:tt)*)? ]) => {
        fn $name<$($generics),*>(&self, $($arg : $arg_ty),*) -> $($return_type)*
        where
           $($bounds)+
        {
            self.as_inner().$name($($arg),*)
        }

        paste! {
            fn [<$name _with_schedule >]<$($generics),*>(
                &self,
                sched: Schedule,
                $($arg : $arg_ty),*
            ) ->  $($return_type)*
            where
                $($bounds)+
            {
                self.as_inner().[<$name _with_schedule>](sched, $($arg),*)
            }

            fn [<blocking_ $name >]<$($generics),*>(
                &self,
                $($arg : $arg_ty),*
            )   $(-> $($blocking_ret)*)?
            where
                $($bounds)+
            {
                self.as_inner().[<blocking_ $name >]($($arg),*)
            }

            fn [<blocking_ $name _with_schedule >]<$($generics),*>(
                &self,
                sched: Schedule,
                $($arg : $arg_ty),*
            )  $(-> $($blocking_ret)*)?
            where
                $($bounds)+
            {
                self.as_inner().[<blocking_ $name _with_schedule>](sched, $($arg),*)
            }
        }
    };
}

#[doc(hidden)]
pub trait DistIteratorLauncher: InnerArray {
    consumer_impl!(
        for_each<I, F>(iter: &I, op: F);
        [DistIterForEachHandle];
        [I: DistributedIterator + 'static, F: Fn(I::Item) + SyncSend + Clone + 'static];
        []
    );
    consumer_impl!(
        for_each_async<I, F, Fut>(iter: &I, op: F); 
        [DistIterForEachHandle];
        [I: DistributedIterator + 'static, F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static, Fut: Future<Output = ()> + Send + 'static];
        []);

    consumer_impl!(
        reduce<I, F>(iter: &I, op: F); 
        [DistIterReduceHandle<I::Item, F>];
        [I: DistributedIterator + 'static, I::Item: Dist + ArrayOps, F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static];
        [-> Option<I::Item>]);

    consumer_impl!(
        collect<I, A>(iter: &I, d: Distribution); 
        [DistIterCollectHandle<I::Item, A>];
        [I: DistributedIterator + 'static, I::Item: Dist + ArrayOps, A: AsyncTeamFrom<(Vec<I::Item>, Distribution)> + SyncSend + Clone + 'static];
        [-> A]);

    consumer_impl!(
        collect_async<I, A, B>(iter: &I, d: Distribution); 
        [DistIterCollectHandle<B, A>];
        [I: DistributedIterator + 'static, I::Item: Future<Output = B> + Send + 'static,B: Dist + ArrayOps,A: AsyncTeamFrom<(Vec<B>, Distribution)> + SyncSend + Clone + 'static,];
        [-> A]);

    consumer_impl!(
        count<I>(iter: &I); 
        [DistIterCountHandle];
        [I: DistributedIterator + 'static ];
        [-> usize]);

    consumer_impl!(
        sum<I>(iter: &I); 
        [DistIterSumHandle<I::Item>];
        [I: DistributedIterator + 'static, I::Item: Dist + ArrayOps + std::iter::Sum, ];
        [-> I::Item]);

    //#[doc(hidden)]
    fn global_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        if chunk_size == 1 {
            self.as_inner().global_index_from_local(index)
        } else {
            Some(
                self.as_inner()
                    .global_index_from_local(index * chunk_size)?
                    / chunk_size,
            )
        }
    }

    //#[doc(hidden)]
    fn subarray_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        if chunk_size == 1 {
            self.as_inner().subarray_index_from_local(index)
        } else {
            Some(
                self.as_inner()
                    .subarray_index_from_local(index * chunk_size)?
                    / chunk_size,
            )
        }
    }

    //#[doc(hidden)]
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.as_inner().team()
    }
}

/// An interface for dealing with distributed iterators (intended as a parallel and distributed version of the standard iterator trait)
///
/// The functions in this trait are available on all distributed iterators.
/// Additonaly functionality can be found in the [IndexedDistributedIterator] trait:
/// these methods are only available for distributed iterators where the number of elements is known in advance (e.g. after invoking `filter` these methods would be unavailable)
pub trait DistributedIterator: SyncSend + IterClone + 'static {
    /// The type of item this distributed iterator produces
    type Item: Send;

    /// The array to which this distributed iterator was created from
    type Array: DistIteratorLauncher;

    /// Internal method used to initalize this distributed iterator to the correct element and correct length.
    fn init(&self, start_i: usize, cnt: usize) -> Self;

    /// Return the original array this distributed iterator belongs too
    fn array(&self) -> Self::Array;

    /// Return the next element in the iterator, otherwise return None
    fn next(&mut self) -> Option<Self::Item>;

    /// Return the maximum number of elements in the iterator
    ///
    /// the actual number of return elements maybe be less (e.g. using a filter iterator)
    fn elems(&self, in_elems: usize) -> usize;

    // /// given a local index return return the corresponding index from the original array
    // fn global_index(&self, index: usize) -> Option<usize>;

    // /// given a local index return the corresponding global subarray index ( or None otherwise)
    // fn subarray_index(&self, index: usize) -> Option<usize>;

    /// advance the internal iterator localtion by count elements
    fn advance_index(&mut self, count: usize);

    /// Applies `op` on each element of this iterator, producing a new iterator with only the elements that gave `true` results
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = LocalLockArray::<usize>::new(&world,8,Distribution::Block);
    /// let my_pe = world.my_pe();
    ///
    /// let init_iter = array.dist_iter_mut().for_each(move|e| *e = my_pe); //initialize array
    /// let filter_iter = array.dist_iter()
    ///                        .enumerate() //we can call enumerate before the filter
    ///                        .filter(|(_,e)| *e%2 == 1).for_each(move|(i,e)| println!("PE: {my_pe} i: {i} elem: {e}"));
    /// world.block_on(async move {
    ///     init_iter.await;
    ///     filter_iter.await;
    /// });
    ///```
    /// Possible output on a 4 PE (1 thread/PE) execution (ordering is likey to be random with respect to PEs)
    ///```text
    /// PE: 1 i: 2 elem: 1
    /// PE: 1 i: 3 elem: 1
    /// PE: 3 i: 6 elem: 3
    /// PE: 3 i: 7 elem: 3
    ///```
    fn filter<F>(self, op: F) -> Filter<Self, F>
    where
        F: Fn(&Self::Item) -> bool + Clone + 'static,
    {
        Filter::new(self, op)
    }

    /// Applies `op` on each element of this iterator to get an `Option`, producing a new iterator with only the elements that return `Some`
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = LocalLockArray::<usize>::new(&world,8,Distribution::Block);
    /// let my_pe = world.my_pe();
    ///
    /// array.dist_iter_mut().for_each(move|e| *e = my_pe); //initialize array
    /// array.wait_all();
    /// let filter_iter = array.dist_iter()
    ///                        .enumerate() //we can call enumerate before the filter
    ///                        .filter_map(|(i,e)| {
    ///     if *e%2 == 0{ Some((i,*e as f32)) }
    ///     else { None }
    /// });
    /// world.block_on(filter_iter.for_each(move|(i,e)| println!("PE: {my_pe} i: {i} elem: {e}")));
    ///```
    /// Possible output on a 4 PE (1 thread/PE) execution (ordering is likey to be random with respect to PEs)
    ///```text
    /// PE: 0 i: 0 elem: 0.0
    /// PE: 0 i: 1 elem: 0.0
    /// PE: 2 i: 4 elem: 2.0
    /// PE: 2 i: 5 elem: 2.0
    ///```
    fn filter_map<F, R>(self, op: F) -> FilterMap<Self, F>
    where
        F: Fn(Self::Item) -> Option<R> + Clone + 'static,
        R: Send + 'static,
    {
        FilterMap::new(self, op)
    }

    /// Applies `op` to each element producing a new iterator with the results
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,8,Distribution::Block);
    /// let my_pe = world.my_pe();
    ///
    /// array.dist_iter().map(|elem| *elem as f64).enumerate().for_each(move|(i,elem)| println!("PE: {my_pe} i: {i} elem: {elem}"));
    /// array.wait_all();
    ///```
    /// Possible output on a 4 PE (1 thread/PE) execution (ordering is likey to be random with respect to PEs)
    ///```text
    /// PE: 0 i: 0 elem: 0.0
    /// PE: 0 i: 1 elem: 0.0
    /// PE: 1 i: 2 elem: 0.0
    /// PE: 1 i: 3 elem: 0.0
    /// PE: 2 i: 4 elem: 0.0
    /// PE: 2 i: 5 elem: 0.0
    /// PE: 3 i: 6 elem: 0.0
    /// PE: 3 i: 7 elem: 0.0
    ///```
    fn map<F, R>(self, op: F) -> Map<Self, F>
    where
        F: Fn(Self::Item) -> R + Clone + 'static,
        R: Send + 'static,
    {
        Map::new(self, op)
    }

    /// Similar to the Enumerate iterator (which can only be applied to `IndexedLocalIterators`), but the yielded indicies are only
    /// guaranteed to be unique and monotonically increasing, they should not be considered to have any relation to the underlying
    /// location of data in the local array.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = LocalLockArray::<usize>::new(&world,16,Distribution::Block);
    /// let my_pe = world.my_pe();
    ///
    /// array.local_iter_mut().for_each(move|e| *e = my_pe); //initialize array
    /// array.wait_all();
    /// let filter_iter = array.local_iter()
    ///                        .enumerate() //we can call enumerate before the filter
    ///                        .filter_map(|(i,e)| {
    ///                             if *e%2 == 0{ Some((i,*e as f32)) }
    ///                             else { None }
    ///                         })
    ///                        .monotonic();
    /// world.block_on(filter_iter.for_each(move|(j,(i,e))| println!("PE: {my_pe} j: {j} i: {i} elem: {e}")));
    ///```
    /// Possible output on a 4 PE (1 thread/PE) execution (ordering is likey to be random with respect to PEs)
    ///```text
    /// PE: 0 j: 0 i: 0 elem: 0.0
    /// PE: 0 j: 1 i: 1 elem: 0.0
    /// PE: 2 j: 0 i: 0 elem: 2.0
    /// PE: 2 j: 1 i: 1 elem: 2.0
    ///```
    fn monotonic(self) -> Monotonic<Self> {
        Monotonic::new(self, 0)
    }

    /// Calls a closure on each element of a Distributed Iterator in parallel and distributed on each PE (which owns data of the iterated array).
    ///
    /// Calling this function invokes an implicit barrier across all PEs in the Array
    ///
    /// This call utilizes the [Schedule::Static][crate::array::iterator::Schedule] policy.
    ///
    /// This function returns a future which can be used to poll for completion of the iteration.
    /// Note calling this function launches the iteration regardless of if the returned future is used or not.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// world.block_on(
    ///     array
    ///         .dist_iter()
    ///         .for_each(move |elem| println!("{:?} {elem}",std::thread::current().id()))
    /// );
    ///```
    #[must_use]
    fn for_each<F>(&self, op: F) -> DistIterForEachHandle
    where
        F: Fn(Self::Item) + SyncSend + Clone + 'static,
    {
        self.array().for_each(self, op)
    }

    /// Calls a closure on each element of a Distributed Iterator in parallel and distributed on each PE (which owns data of the iterated array).
    ///
    /// Calling this function invokes an implicit barrier across all PEs in the Array
    ///
    /// This call utilizes the [Schedule::Static][crate::array::iterator::Schedule] policy.
    ///
    /// The iteration will have been completed by the time this function returns
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// array
    ///     .dist_iter()
    ///     .blocking_for_each(move |elem| println!("{:?} {elem}",std::thread::current().id()))
    /// );
    ///```
    fn blocking_for_each<F>(&self, op: F) 
    where
        F: Fn(Self::Item) + SyncSend + Clone + 'static,
    {
        self.array().blocking_for_each(self, op)
    }

    /// Calls a closure and immediately awaits the result on each element of a Distributed Iterator in parallel and distributed on each PE (which owns data of the iterated array).
    ///
    /// Calling this function invokes an implicit barrier across all PEs in the Array
    ///
    /// The supplied closure must return a future.
    ///
    /// Each thread will only drive a single future at a time.
    ///
    /// This function returns a future which can be used to poll for completion of the iteration.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let iter = array.dist_iter().for_each_async(|elem| async move {
    ///     async_std::task::yield_now().await;
    ///     println!("{:?} {elem}",std::thread::current().id())
    /// });
    /// world.block_on(iter);
    /// ```
    /// essentially the for_each_async call gets converted into (on each thread)
    ///```ignore
    /// for fut in array.iter(){
    ///     fut.await;
    /// }
    ///```
    #[must_use]
    fn for_each_async<F, Fut>(&self, op: F) -> DistIterForEachHandle
    where
        F: Fn(Self::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.array().for_each_async(self, op)
    }

    /// Calls a closure and immediately awaits the result on each element of a Distributed Iterator in parallel and distributed on each PE (which owns data of the iterated array).
    ///
    /// Calling this function invokes an implicit barrier across all PEs in the Array
    ///
    /// The supplied closure must return a future.
    ///
    /// Each thread will only drive a single future at a time.
    ///
    /// Iteration is completed by the time this function returns
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// array.dist_iter().blocking_for_each_async(|elem| async move {
    ///     async_std::task::yield_now().await;
    ///     println!("{:?} {elem}",std::thread::current().id())
    /// });
    /// ```
    /// essentially the for_each_async call gets converted into (on each thread)
    ///```ignore
    /// for fut in array.iter(){
    ///     fut.await;
    /// }
    ///```
    fn blocking_for_each_async<F, Fut>(&self, op: F)
    where
        F: Fn(Self::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.array().blocking_for_each_async(self, op)
    }

    /// Calls a closure on each element of a Distributed Iterator in parallel and distributed on each PE (which owns data of the iterated array) using the specififed [Schedule][crate::array::iterator::Schedule] policy.
    ///
    /// Calling this function invokes an implicit barrier across all PEs in the Array
    ///
    /// This function returns a future which can be used to poll for completion of the iteration.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// array.dist_iter().for_each_with_schedule(Schedule::WorkStealing, |elem| println!("{:?} {elem}",std::thread::current().id()));
    /// array.wait_all();
    ///```
    #[must_use]
    fn for_each_with_schedule<F>(&self, sched: Schedule, op: F) -> DistIterForEachHandle
    where
        F: Fn(Self::Item) + SyncSend + Clone + 'static,
    {
        self.array().for_each_with_schedule(sched, self, op)
    }

    /// Calls a closure on each element of a Distributed Iterator in parallel and distributed on each PE (which owns data of the iterated array) using the specififed [Schedule][crate::array::iterator::Schedule] policy.
    ///
    /// Calling this function invokes an implicit barrier across all PEs in the Array
    ///
    /// Iteration is completed by the time this function returns
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// array.dist_iter().blocking_for_each_with_schedule(Schedule::WorkStealing, |elem| println!("{:?} {elem}",std::thread::current().id()));
    ///```
    fn blocking_for_each_with_schedule<F>(&self, sched: Schedule, op: F)
    where
        F: Fn(Self::Item) + SyncSend + Clone + 'static,
    {
        self.array().blocking_for_each_with_schedule(sched, self, op)
    }

    /// Calls a closure and immediately awaits the result on each element of a Distributed Iterator in parallel and distributed on each PE (which owns data of the iterated array) using the specififed [Schedule][crate::array::iterator::Schedule] policy.
    ///
    /// Calling this function invokes an implicit barrier across all PEs in the Array, after this barrier no further communication is performed
    /// as each PE will only process elements local to itself
    ///
    /// The supplied closure must return a future.
    ///
    /// Each thread will only drive a single future at a time.
    ///
    /// This function returns a future which can be used to poll for completion of the iteration.
    /// Note calling this function launches the iteration regardless of if the returned future is used or not.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// array.dist_iter().for_each_async_with_schedule(Schedule::Chunk(10),|elem| async move {
    ///     async_std::task::yield_now().await;
    ///     println!("{:?} {elem}",std::thread::current().id())
    /// });
    /// array.wait_all();
    ///```
    #[must_use]
    fn for_each_async_with_schedule<F, Fut>(&self, sched: Schedule, op: F) -> DistIterForEachHandle
    where
        F: Fn(Self::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.array().for_each_async_with_schedule(sched, self, op)
    }

    /// Calls a closure and immediately awaits the result on each element of a Distributed Iterator in parallel and distributed on each PE (which owns data of the iterated array) using the specififed [Schedule][crate::array::iterator::Schedule] policy.
    ///
    /// Calling this function invokes an implicit barrier across all PEs in the Array, after this barrier no further communication is performed
    /// as each PE will only process elements local to itself
    ///
    /// The supplied closure must return a future.
    ///
    /// Each thread will only drive a single future at a time.
    ///
    /// Iteration is completed by the time this function returns
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// array.dist_iter().blocking_for_each_async_with_schedule(Schedule::Chunk(10),|elem| async move {
    ///     async_std::task::yield_now().await;
    ///     println!("{:?} {elem}",std::thread::current().id())
    /// });
    ///```
    fn blocking_for_each_async_with_schedule<F, Fut>(&self, sched: Schedule, op: F)
    where
        F: Fn(Self::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.array().blocking_for_each_async_with_schedule(sched, self, op)
    }

    /// Reduces the elements of the dist iterator using the provided closure
    ///
    /// This function returns a future which needs to be driven to completion to retrieve the reduced value.
    ///
    /// This call utilizes the [Schedule::Static][crate::array::iterator::Schedule] policy.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let req = array.dist_iter().reduce(|acc,elem| acc+elem);
    /// let sum = array.block_on(req); //wait on the collect request to get the new array
    ///```
    #[must_use]
    fn reduce<F>(&self, op: F) -> DistIterReduceHandle<Self::Item, F>
    where
        // &'static Self: LocalIterator + 'static,
        Self::Item: Dist + ArrayOps,
        F: Fn(Self::Item, Self::Item) -> Self::Item + SyncSend + Clone + 'static,
    {
        self.array().reduce(self, op)
    }

    /// Reduces the elements of the dist iterator using the provided closure
    ///
    /// The function returns the reduced value
    ///
    /// This call utilizes the [Schedule::Static][crate::array::iterator::Schedule] policy.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let req = array.dist_iter().blocking_reduce(|acc,elem| acc+elem);
    ///```
    fn blocking_reduce<F>(&self, op: F) -> Option<Self::Item>
    where
        // &'static Self: LocalIterator + 'static,
        Self::Item: Dist + ArrayOps,
        F: Fn(Self::Item, Self::Item) -> Self::Item + SyncSend + Clone + 'static,
    {
        self.array().blocking_reduce(self, op)
    }

    /// Reduces the elements of the dist iterator using the provided closure and [Schedule][crate::array::iterator::Schedule] policy
    ///
    /// This function returns a future which needs to be driven to completion to retrieve the  reduced value.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let req = array.dist_iter().reduce_with_schedule(Schedule::Static,|acc,elem| acc+elem);
    /// let sum = array.block_on(req); //wait on the collect request to get the new array
    ///```
    #[must_use]
    fn reduce_with_schedule<F>(&self, sched: Schedule, op: F) -> DistIterReduceHandle<Self::Item, F>
    where
        // &'static Self: LocalIterator + 'static,
        Self::Item: Dist + ArrayOps,
        F: Fn(Self::Item, Self::Item) -> Self::Item + SyncSend + Clone + 'static,
    {
        self.array().reduce_with_schedule(sched, self, op)
    }

    /// Reduces the elements of the dist iterator using the provided closure and [Schedule][crate::array::iterator::Schedule] policy
    ///
    /// This function returns the reduced value.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let req = array.dist_iter().blocking_reduce_with_schedule(Schedule::Static,|acc,elem| acc+elem);//wait on the collect request to get the new array
    ///```
    fn blocking_reduce_with_schedule<F>(&self, sched: Schedule, op: F) -> Option<Self::Item>
    where
        // &'static Self: LocalIterator + 'static,
        Self::Item: Dist + ArrayOps,
        F: Fn(Self::Item, Self::Item) -> Self::Item + SyncSend + Clone + 'static,
    {
        self.array().blocking_reduce_with_schedule(sched, self, op)
    }

    /// Collects the elements of the distributed iterator into a new LamellarArray
    ///
    /// Calling this function invokes an implicit barrier across all PEs in the Array.
    ///
    /// This function returns a future which needs to be driven to completion to retrieve the new LamellarArray.
    /// Calling await on the future will invoke an implicit barrier (allocating the resources for a new array).
    ///
    /// Creating the new array potentially results in data transfers depending on the distribution mode and the fact there is no gaurantee
    /// that each PE will contribute an equal number of elements to the new array, and currently LamellarArrays
    /// distribute data across the PEs as evenly as possible.
    ///
    /// This call utilizes the [Schedule::Static][crate::array::iterator::Schedule] policy.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let req = array.dist_iter()
    ///                .map(|elem| *elem) //because of constraints of collect we need to convert from &usize to usize
    ///                .filter(|elem|  *elem < 10) // (if we didnt do the previous map  we would have needed to do **elem)
    ///                .collect::<AtomicArray<usize>>(Distribution::Block);
    /// let new_array = array.block_on(req); //wait on the collect request to get the new array
    ///```
    #[must_use]
    fn collect<A>(&self, d: Distribution) -> DistIterCollectHandle<Self::Item, A>
    where
        // &'static Self: DistributedIterator + 'static,
        Self::Item: Dist + ArrayOps,
        A: AsyncTeamFrom<(Vec<Self::Item>, Distribution)> + SyncSend + Clone + 'static,
    {
        self.array().collect(self, d)
    }

    /// Collects the elements of the distributed iterator into a new LamellarArray
    ///
    /// Calling this function invokes an implicit barrier across all PEs in the Array.
    ///
    /// This function returns the new LamellarArray upon completion.
    ///
    /// Creating the new array potentially results in data transfers depending on the distribution mode and the fact there is no gaurantee
    /// that each PE will contribute an equal number of elements to the new array, and currently LamellarArrays
    /// distribute data across the PEs as evenly as possible.
    ///
    /// This call utilizes the [Schedule::Static][crate::array::iterator::Schedule] policy.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let new_array = array.dist_iter()
    ///                .map(|elem| *elem) //because of constraints of collect we need to convert from &usize to usize
    ///                .filter(|elem|  *elem < 10) // (if we didnt do the previous map  we would have needed to do **elem)
    ///                .blocking_collect::<AtomicArray<usize>>(Distribution::Block);
    ///```
    fn blocking_collect<A>(&self, d: Distribution) -> A
    where
        // &'static Self: DistributedIterator + 'static,
        Self::Item: Dist + ArrayOps,
        A: AsyncTeamFrom<(Vec<Self::Item>, Distribution)> + SyncSend + Clone + 'static,
    {
        self.array().blocking_collect(self, d)
    }

    /// Collects the elements of the distributed iterator into a new LamellarArray, using the provided [Schedule][crate::array::iterator::Schedule] policy 
    ///
    /// Calling this function invokes an implicit barrier across all PEs in the Array.
    ///
    /// This function returns a future which needs to be driven to completion to retrieve the new LamellarArray.
    /// Calling await on the future will invoke an implicit barrier (allocating the resources for a new array).
    ///
    /// Creating the new array potentially results in data transfers depending on the distribution mode and the fact there is no gaurantee
    /// that each PE will contribute an equal number of elements to the new array, and currently LamellarArrays
    /// distribute data across the PEs as evenly as possible.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let req = array.dist_iter()
    ///                .map(|elem| *elem) //because of constraints of collect we need to convert from &usize to usize
    ///                .filter(|elem|  *elem < 10) // (if we didnt do the previous map  we would have needed to do **elem)
    ///                .collect::<AtomicArray<usize>>(Distribution::Block);
    /// let new_array = array.block_on(req); //wait on the collect request to get the new array
    ///```
    #[must_use]
    fn collect_with_schedule<A>(&self,sched: Schedule, d: Distribution) -> DistIterCollectHandle<Self::Item, A>
    where
        // &'static Self: DistributedIterator + 'static,
        Self::Item: Dist + ArrayOps,
        A: AsyncTeamFrom<(Vec<Self::Item>, Distribution)> + SyncSend + Clone + 'static,
    {
        self.array().collect_with_schedule(sched,self,  d)
    }

    /// Collects the elements of the distributed iterator into a new LamellarArray, using the provided [Schedule][crate::array::iterator::Schedule] policy 
    ///
    /// Calling this function invokes an implicit barrier across all PEs in the Array.
    ///
    /// This function returns the new LamellarArray upon completion.
    ///
    /// Creating the new array potentially results in data transfers depending on the distribution mode and the fact there is no gaurantee
    /// that each PE will contribute an equal number of elements to the new array, and currently LamellarArrays
    /// distribute data across the PEs as evenly as possible.
    ///
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let new_array = array.dist_iter()
    ///                .map(|elem| *elem) //because of constraints of collect we need to convert from &usize to usize
    ///                .filter(|elem|  *elem < 10) // (if we didnt do the previous map  we would have needed to do **elem)
    ///                .blocking_collect_with_scheduler::<AtomicArray<usize>>(Schedule::Dynamic, Distribution::Block);
    ///```
    fn blocking_collect_with_schedule<A>(&self,sched: Schedule, d: Distribution) -> A
    where
        // &'static Self: DistributedIterator + 'static,
        Self::Item: Dist + ArrayOps,
        A: AsyncTeamFrom<(Vec<Self::Item>, Distribution)> + SyncSend + Clone + 'static,
    {
        self.array().blocking_collect_with_schedule(sched,self, d)
    }

    /// Collects the awaited elements of the distributed iterator into a new LamellarArray
    ///
    /// Calling this function invokes an implicit barrier across all PEs in the Array.
    ///
    /// Each element from the iterator must return a Future
    ///
    /// Each thread will only drive a single future at a time.
    ///
    /// This function returns a future which needs to be driven to completion to retrieve the new LamellarArray.
    /// Calling await on the future will invoke an implicit barrier (allocating the resources for a new array).
    ///
    /// Creating the new array potentially results in data transfers depending on the distribution mode and the fact there is no gaurantee
    /// that each PE will contribute an equal number of elements to the new array, and currently LamellarArrays
    /// distribute data across the PEs as evenly as possible.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// // initialize a world and an atomic array
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Block);
    ///
    /// // clone the array; this doesn't duplicate the underlying
    /// // data but it does create a second pointer that we can
    /// // discard when necessary
    /// let array_clone = array.clone();
    ///
    /// // run collect
    /// let req
    ///     = array_clone.dist_iter().map(
    ///         move |elem|
    ///         array_clone
    ///             .fetch_add(elem.load(),1000))
    ///             .collect_async::<ReadOnlyArray<usize>,_>(Distribution::Cyclic);
    /// let _new_array = array.block_on(req);
    ///```
    #[must_use]
    fn collect_async<A, T>(&self, d: Distribution) -> DistIterCollectHandle<T, A>
    where
        // &'static Self: DistributedIterator + 'static,
        T: Dist + ArrayOps,
        Self::Item: Future<Output = T> + Send + 'static,
        A: AsyncTeamFrom<(Vec<T>, Distribution)> + SyncSend + Clone + 'static,
    {
        self.array().collect_async(self, d)
    }

    /// Collects the awaited elements of the distributed iterator into a new LamellarArray
    ///
    /// Calling this function invokes an implicit barrier across all PEs in the Array.
    ///
    /// Each element from the iterator must return a Future
    ///
    /// Each thread will only drive a single future at a time.
    ///
    /// The function returns the new LamellarArray upon completion.
    ///
    /// Creating the new array potentially results in data transfers depending on the distribution mode and the fact there is no gaurantee
    /// that each PE will contribute an equal number of elements to the new array, and currently LamellarArrays
    /// distribute data across the PEs as evenly as possible.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// // initialize a world and an atomic array
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Block);
    ///
    /// // clone the array; this doesn't duplicate the underlying
    /// // data but it does create a second pointer that we can
    /// // discard when necessary
    /// let array_clone = array.clone();
    ///
    /// // run collect
    /// let _new_array
    ///     = array_clone.dist_iter().map(
    ///         move |elem|
    ///         array_clone
    ///             .fetch_add(elem.load(),1000))
    ///             .blocking_collect_async::<ReadOnlyArray<usize>,_>(Distribution::Cyclic);
    ///```
    fn blocking_collect_async<A, T>(&self, d: Distribution) -> A
    where
        // &'static Self: DistributedIterator + 'static,
        T: Dist + ArrayOps,
        Self::Item: Future<Output = T> + Send + 'static,
        A: AsyncTeamFrom<(Vec<T>, Distribution)> + SyncSend + Clone + 'static,
    {
        self.array().blocking_collect_async(self, d)
    }

    /// Collects the awaited elements of the distributed iterator into a new LamellarArray, using the provided [Schedule][crate::array::iterator::Schedule] policy 
    ///
    /// Calling this function invokes an implicit barrier across all PEs in the Array.
    ///
    /// Each element from the iterator must return a Future
    ///
    /// Each thread will only drive a single future at a time.
    ///
    /// This function returns a future which needs to be driven to completion to retrieve the new LamellarArray.
    /// Calling await on the future will invoke an implicit barrier (allocating the resources for a new array).
    ///
    /// Creating the new array potentially results in data transfers depending on the distribution mode and the fact there is no gaurantee
    /// that each PE will contribute an equal number of elements to the new array, and currently LamellarArrays
    /// distribute data across the PEs as evenly as possible.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// // initialize a world and an atomic array
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Block);
    ///
    /// // clone the array; this doesn't duplicate the underlying
    /// // data but it does create a second pointer that we can
    /// // discard when necessary
    /// let array_clone = array.clone();
    ///
    /// // run collect
    /// let req
    ///     = array_clone.dist_iter().map(
    ///         move |elem|
    ///         array_clone
    ///             .fetch_add(elem.load(),1000))
    ///             .collect_async_with_schedule::<ReadOnlyArray<usize>,_>(Scheduler::Dynamic, Distribution::Cyclic);
    /// let _new_array = array.block_on(req);
    ///```
    #[must_use]
    fn collect_async_with_schedule<A, T>(&self, sched: Schedule,   d: Distribution) -> DistIterCollectHandle<T, A>
    where
        // &'static Self: DistributedIterator + 'static,
        T: Dist + ArrayOps,
        Self::Item: Future<Output = T> + Send + 'static,
        A: AsyncTeamFrom<(Vec<T>, Distribution)> + SyncSend + Clone + 'static,
    {
        self.array().collect_async_with_schedule(sched, self, d)
    }

    /// Collects the awaited elements of the distributed iterator into a new LamellarArray,using the provided [Schedule][crate::array::iterator::Schedule] policy 
    ///
    /// Calling this function invokes an implicit barrier across all PEs in the Array.
    ///
    /// Each element from the iterator must return a Future
    ///
    /// Each thread will only drive a single future at a time.
    ///
    /// The function returns the new LamellarArray upon completion.
    ///
    /// Creating the new array potentially results in data transfers depending on the distribution mode and the fact there is no gaurantee
    /// that each PE will contribute an equal number of elements to the new array, and currently LamellarArrays
    /// distribute data across the PEs as evenly as possible.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// // initialize a world and an atomic array
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Block);
    ///
    /// // clone the array; this doesn't duplicate the underlying
    /// // data but it does create a second pointer that we can
    /// // discard when necessary
    /// let array_clone = array.clone();
    ///
    /// // run collect
    /// let _new_array
    ///     = array_clone.dist_iter().map(
    ///         move |elem|
    ///         array_clone
    ///             .fetch_add(elem.load(),1000))
    ///             .blocking_collect_async::<ReadOnlyArray<usize>,_>(Distribution::Cyclic);
    ///```
    fn blocking_collect_async_with_schedule<A, T>(&self, sched: Schedule, d: Distribution) -> A
    where
        // &'static Self: DistributedIterator + 'static,
        T: Dist + ArrayOps,
        Self::Item: Future<Output = T> + Send + 'static,
        A: AsyncTeamFrom<(Vec<T>, Distribution)> + SyncSend + Clone + 'static,
    {
        self.array().blocking_collect_async_with_schedule(sched,self, d)
    }

    /// Counts the number of the elements of the distriubted iterator
    /// 
    /// Calling this function invokes an implicit barrier and distributed reduction across all PEs in the Array.
    ///
    /// This function returns a future which needs to be driven to completion to retrieve count.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let req = array.dist_iter().filter(|elem|  elem < 10).count();
    /// let cnt = array.block_on(req); //wait on the collect request to get the new array
    ///```
    #[must_use]
    fn count(&self) -> DistIterCountHandle {
        self.array().count(self)
    }

    /// Counts the number of the elements of the distributed iterator
    ///
    /// Calling this function invokes an implicit barrier and distributed reduction across all PEs in the Array.
    /// 
    /// This function returns the count upon completion.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let cnt = array.dist_iter().filter(|elem|  elem < 10).blocking_count();
    ///```
    fn blocking_count(&self) -> usize {
        self.array().blocking_count(self)
    }

    /// Counts the number of the elements of the distriubted iterator, using the provided [Schedule][crate::array::iterator::Schedule] policy
    /// 
    /// Calling this function invokes an implicit barrier and distributed reduction across all PEs in the Array.
    ///
    /// This function returns a future which needs to be driven to completion to retrieve count.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let req = array.dist_iter().filter(|elem|  elem < 10).count_with_schedule(Schedule::Dynamic);
    /// let cnt = array.block_on(req); //wait on the collect request to get the new array
    ///```
    fn count_with_schedule(&self, sched: Schedule) -> DistIterCountHandle {
        self.array().count_with_schedule(sched, self)
    }


    /// Counts the number of the elements of the distributed iterator, using the provided [Schedule][crate::array::iterator::Schedule] policy
    ///
    /// Calling this function invokes an implicit barrier and distributed reduction across all PEs in the Array.
    /// 
    /// This function returns the count upon completion.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let cnt = array.dist_iter().filter(|elem|  elem < 10).blocking_count_with_schedule(Schedule::Dynamic);
    ///```
    fn blocking_count_with_schedule(&self, sched: Schedule) -> usize {
        self.array().blocking_count_with_schedule(sched, self)
    }

    /// Sums the elements of the distributed iterator.
    ///
    /// Takes each element, adds them together, and returns the result.
    /// 
    /// Calling this function invokes an implicit barrier and distributed reduction across all PEs in the Array.
    ///
    /// An empty iterator returns the zero value of the type.
    ///
    /// This function returns a future which needs to be driven to completion to retrieve the sum
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let req = array.dist_iter().sum();
    /// let sum = array.block_on(req); //wait on the collect request to get the new array
    ///```
    #[must_use]
    fn sum(&self) -> DistIterSumHandle<Self::Item>
    where
        Self::Item: Dist + ArrayOps + std::iter::Sum,
    {
        self.array().sum(self)
    }

    /// Sums the elements of the distributed iterator.
    ///
    /// Takes each element, adds them together, and returns the result.
    /// 
    /// Calling this function invokes an implicit barrier and distributed reduction across all PEs in the Array.
    ///
    /// An empty iterator returns the zero value of the type.
    ///
    /// This function returns the sum upon completion.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let sum = array.dist_iter().blocking_sum();
    ///```
    fn blocking_sum(&self) -> Self::Item
    where
        Self::Item: Dist + ArrayOps + std::iter::Sum,
    {
        self.array().blocking_sum(self)
    }

    /// Sums the elements of the distributed iterator, using the specified [Schedule][crate::array::iterator::Schedule] policy
    ///
    /// Takes each element, adds them together, and returns the result.
    /// 
    /// Calling this function invokes an implicit barrier and distributed reduction across all PEs in the Array.
    ///
    /// An empty iterator returns the zero value of the type.
    ///
    /// This function returns a future which needs to be driven to completion to retrieve the sum
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let req = array.dist_iter().sum_with_schedule(Schedule::Guided);
    /// let sum = array.block_on(req); //wait on the collect request to get the new array
    ///```
    #[must_use]
    fn sum_with_schedule(&self, sched: Schedule) -> DistIterSumHandle<Self::Item>
    where
        Self::Item: Dist + ArrayOps + std::iter::Sum,
    {
        self.array().sum_with_schedule(sched, self)
    }

    /// Sums the elements of the distributed iterator, using the specified [Schedule][crate::array::iterator::Schedule] policy
    ///
    /// Takes each element, adds them together, and returns the result.
    /// 
    /// Calling this function invokes an implicit barrier and distributed reduction across all PEs in the Array.
    ///
    /// An empty iterator returns the zero value of the type.
    ///
    /// This function returns the sum upon completion.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let sum = array.dist_iter().blocking_sum_with_schedule(Schedule::Guided);
    ///```
    fn blocking_sum_with_schedule(&self, sched: Schedule) -> Self::Item
    where
        Self::Item: Dist + ArrayOps + std::iter::Sum,
    {
        self.array().blocking_sum_with_schedule(sched, self)
    }
}

/// An interface for dealing with distributed iterators which are indexable, meaning it returns an iterator of known length
pub trait IndexedDistributedIterator: DistributedIterator + SyncSend + IterClone + 'static {
    /// yields the global array index along with each element
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,8,Distribution::Block);
    /// let my_pe = world.my_pe();
    ///
    /// array.dist_iter().enumerate().for_each(move|(i,elem)| println!("PE: {my_pe} i: {i} elem: {elem}"));
    /// array.wait_all();
    ///```
    /// Possible output on a 4 PE (1 thread/PE) execution (ordering is likey to be random with respect to PEs)
    ///```text
    /// PE: 0 i: 0 elem: 0
    /// PE: 0 i: 1 elem: 0
    /// PE: 1 i: 2 elem: 0
    /// PE: 1 i: 3 elem: 0
    /// PE: 2 i: 4 elem: 0
    /// PE: 2 i: 5 elem: 0
    /// PE: 3 i: 6 elem: 0
    /// PE: 3 i: 7 elem: 0
    ///```
    fn enumerate(self) -> Enumerate<Self> {
        Enumerate::new(self, 0)
    }

    /// An iterator that skips the first `n` elements
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,8,Distribution::Block);
    /// let my_pe = world.my_pe();
    ///
    /// array.dist_iter().enumerate().skip(3).for_each(move|(i,elem)| println!("PE: {my_pe} i: {i} elem: {elem}"));
    /// array.wait_all();
    ///```
    /// Possible output on a 4 PE (1 thread/PE) execution (ordering is likey to be random with respect to PEs)
    ///```text
    /// PE: 1 i:3 elem: 0
    /// PE: 2 i:4 elem: 0
    /// PE: 2 i:5 elem: 0
    /// PE: 3 i:6 elem: 0
    /// PE: 3 i:7 elem: 0
    ///```
    fn skip(self, n: usize) -> Skip<Self> {
        Skip::new(self, n, 0)
    }

    /// An iterator that steps by `step_size` elements
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,8,Distribution::Block);
    /// let my_pe = world.my_pe();
    ///
    /// array.dist_iter().enumerate().step_by(3).for_each(move|(i,elem)| println!("PE: {my_pe} i: {i} elem: {elem}"));
    /// array.wait_all();
    ///```
    /// Possible output on a 4 PE (1 thread/PE) execution (ordering is likey to be random with respect to PEs)
    ///```text
    /// PE: 0 i: 0 elem: 0
    /// PE: 1 i: 3 elem: 0
    /// PE: 3 i: 6 elem: 0
    ///```
    fn step_by(self, step_size: usize) -> StepBy<Self> {
        StepBy::new(self, step_size, 0)
    }

    /// An iterator that takes the first `n` elements
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,8,Distribution::Block);
    /// let my_pe = world.my_pe();
    ///
    /// array.dist_iter().enumerate().take(3).for_each(move|(i,elem)| println!("PE: {my_pe} i: {i} elem: {elem}"));
    /// array.wait_all();
    ///```
    /// Possible output on a 4 PE (1 thread/PE) execution (ordering is likey to be random with respect to PEs)
    ///```text
    /// PE: 0 i: 0 elem: 0
    /// PE: 0 i: 1 elem: 0
    /// PE: 1 i: 2 elem: 0
    ///```
    fn take(self, n: usize) -> Take<Self> {
        Take::new(self, n, 0)
    }
    // fn zip<I: IndexedDistributedIterator>(self, iter: I) -> Zip<Self, I> {
    //     Zip::new(self, iter)
    // }

    /// given an local index return the corresponding global iterator index ( or None otherwise)
    fn iterator_index(&self, index: usize) -> Option<usize>;
}

/// Immutable LamellarArray distributed iterator
///
/// This struct is created by calling `dist_iter` on any of the LamellarArray types
///
/// # Examples
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = LocalLockArray::<usize>::new(&world,100,Distribution::Block);
///
/// let dist_iter = array.dist_iter().for_each(move |e| println!("{e}"));
/// world.block_on(dist_iter);
///```
#[derive(Clone)]
pub struct DistIter<'a, T: Dist + 'static, A: LamellarArray<T>> {
    data: A,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'a T>,
}

impl<'a, T: Dist, A: LamellarArray<T>> IterClone for DistIter<'a, T, A> {
    fn iter_clone(&self, _: Sealed) -> Self {
        DistIter {
            data: self.data.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
            _marker: PhantomData,
        }
    }
}

impl<'a, T: Dist, A: LamellarArray<T>> std::fmt::Debug for DistIter<'a, T, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DistIter{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

impl<T: Dist, A: LamellarArray<T>> DistIter<'_, T, A> {
    pub(crate) fn new(data: A, cur_i: usize, cnt: usize) -> Self {
        // println!("new dist iter {:?} {:? } {:?}",cur_i, cnt, cur_i+cnt);
        DistIter {
            data,
            cur_i,
            end_i: cur_i + cnt,
            _marker: PhantomData,
        }
    }
}

impl<
        T: Dist + 'static,
        A: LamellarArray<T> + DistIteratorLauncher + SyncSend + Clone + 'static,
    > DistributedIterator for DistIter<'static, T, A>
{
    type Item = &'static T;
    type Array = A;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("{:?} DistIter init {start_i} {cnt} {} {}",std::thread::current().id(), start_i+cnt,max_i);
        DistIter {
            data: self.data.clone(),
            cur_i: std::cmp::min(start_i, max_i),
            end_i: std::cmp::min(start_i + cnt, max_i),
            _marker: PhantomData,
        }
    }
    fn array(&self) -> Self::Array {
        self.data.clone()
    }
    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_i < self.end_i {
            // println!("{:?} DistIter next cur: {:?} end {:?} Some left",std::thread::current().id(),self.cur_i,self.end_i);
            self.cur_i += 1;
            unsafe {
                self.data
                    .local_as_ptr()
                    .offset((self.cur_i - 1) as isize)
                    .as_ref()
            }
        } else {
            // println!("{:?} DistIter next cur: {:?} end {:?} Done",std::thread::current().id(),self.cur_i,self.end_i);
            None
        }
    }
    fn elems(&self, in_elems: usize) -> usize {
        in_elems
    }
    fn advance_index(&mut self, count: usize) {
        // println!("{:?} \t DistIter advance index {} {} {}",std::thread::current().id(),count,self.cur_i + count, self.end_i);
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

impl<
        T: Dist + 'static,
        A: LamellarArray<T> + SyncSend + DistIteratorLauncher + Clone + 'static,
    > IndexedDistributedIterator for DistIter<'static, T, A>
{
    fn iterator_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.subarray_index_from_local(index, 1);
        // println!("{:?} \t DistIter iterator index {index} {g_index:?}",std::thread::current().id());
        g_index
    }
}

/// Mutable LamellarArray distributed iterator
///
/// This struct is created by calling `dist_iter_mut` on any of the [LamellarWriteArray][crate::array::LamellarWriteArray] types
///
/// # Examples
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = LocalLockArray::<usize>::new(&world,100,Distribution::Block);
/// let my_pe = world.my_pe();
/// let dist_iter = array.dist_iter_mut().for_each(move |e| *e = my_pe );
/// world.block_on(dist_iter);
///```
pub struct DistIterMut<'a, T: Dist, A: LamellarArray<T>> {
    data: A,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'a T>,
}

impl<'a, T: Dist, A: LamellarArray<T>> IterClone for DistIterMut<'a, T, A> {
    fn iter_clone(&self, _: Sealed) -> Self {
        DistIterMut {
            data: self.data.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
            _marker: PhantomData,
        }
    }
}

impl<'a, T: Dist, A: LamellarArray<T>> std::fmt::Debug for DistIterMut<'a, T, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DistIterMut{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

impl<T: Dist, A: LamellarArray<T>> DistIterMut<'_, T, A> {
    pub(crate) fn new(data: A, cur_i: usize, cnt: usize) -> Self {
        DistIterMut {
            data,
            cur_i,
            end_i: cur_i + cnt,
            _marker: PhantomData,
        }
    }
}

impl<
        T: Dist + 'static,
        A: LamellarArray<T> + SyncSend + DistIteratorLauncher + Clone + 'static,
    > DistributedIterator for DistIterMut<'static, T, A>
{
    type Item = &'static mut T;
    type Array = A;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("dist iter init {:?} {:?} {:?}",start_i,cnt,max_i);
        DistIterMut {
            data: self.data.clone(),
            cur_i: std::cmp::min(start_i, max_i),
            end_i: std::cmp::min(start_i + cnt, max_i),
            _marker: PhantomData,
        }
    }
    fn array(&self) -> Self::Array {
        self.data.clone()
    }
    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_i < self.end_i {
            self.cur_i += 1;
            unsafe {
                Some(
                    &mut *self
                        .data
                        .local_as_mut_ptr()
                        .offset((self.cur_i - 1) as isize),
                )
            }
        } else {
            None
        }
    }
    fn elems(&self, in_elems: usize) -> usize {
        in_elems
    }

    // fn subarray_index(&self, index: usize) -> Option<usize> {
    //     let g_index = self.data.subarray_index_from_local(index, 1);
    //     g_index
    // }

    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

impl<
        T: Dist + 'static,
        A: LamellarArray<T> + SyncSend + DistIteratorLauncher + Clone + 'static,
    > IndexedDistributedIterator for DistIterMut<'static, T, A>
{
    fn iterator_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.subarray_index_from_local(index, 1);

        g_index
    }
}

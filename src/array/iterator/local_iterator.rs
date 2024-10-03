//! Parallel iteration of a PE's local segment of a LamellarArray
//!
//! This module provides parallel iteration capabilities for the local segments of LamellarArrays,
//! similar to the `ParallelIterators` provided by the (Rayon)<https://docs.rs/rayon/latest/rayon/> crate.
//!
//! These iterators are purely local to the calling PE, no data transfer occurs.
//!
//! # Examples
//!
//! Examples can be found under [lamellar-runtime/examples/array_examples/](https://github.com/pnnl/lamellar-runtime/tree/master/examples/array_examples)
mod chunks;
mod enumerate;
mod filter;
mod filter_map;
mod map;
mod monotonic;
mod skip;
mod step_by;
mod take;
mod zip;

pub(crate) mod consumer;

use chunks::*;
use enumerate::*;
use filter::*;
use filter_map::*;
use map::*;
use monotonic::*;
use skip::*;
use step_by::*;
use take::*;
use zip::*;

pub(crate) use consumer::*;

use crate::array::iterator::{private::*, Schedule};
use crate::array::{operations::ArrayOps, AsyncTeamFrom, Distribution, InnerArray, LamellarArray};
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
    ($name:ident<$($generics:ident),*>($($arg:ident : $arg_ty:ty),*); [$($return_type: tt)*]; [$($bounds:tt)+] ) => {
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
        }
    };
}

#[doc(hidden)]
#[enum_dispatch]
pub trait LocalIteratorLauncher: InnerArray {
    consumer_impl!(
        for_each<I, F>(iter: &I, op: F);
        [LocalIterForEachHandle];
        [I: LocalIterator + 'static, F: Fn(I::Item) + SyncSend + Clone + 'static]
    );
    consumer_impl!(
        for_each_async<I, F, Fut>(iter: &I, op: F);
        [LocalIterForEachHandle];
        [I: LocalIterator + 'static, F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static, Fut: Future<Output = ()> + Send + 'static]);

    consumer_impl!(
        reduce<I, F>(iter: &I, op: F);
        [LocalIterReduceHandle<I::Item, F>];
        [I: LocalIterator + 'static, I::Item: SyncSend + Copy, F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static]);

    consumer_impl!(
        collect<I, A>(iter: &I, d: Distribution);
        [LocalIterCollectHandle<I::Item, A>];
        [I: LocalIterator + 'static, I::Item: Dist + ArrayOps, A: AsyncTeamFrom<(Vec<I::Item>, Distribution)> + SyncSend + Clone + 'static]);

    consumer_impl!(
        collect_async<I, A, B>(iter: &I, d: Distribution);
        [LocalIterCollectHandle<B, A>];
        [I: LocalIterator + 'static, I::Item: Future<Output = B> + Send + 'static,B: Dist + ArrayOps,A: AsyncTeamFrom<(Vec<B>, Distribution)> + SyncSend + Clone + 'static,]);

    consumer_impl!(
        count<I>(iter: &I);
        [LocalIterCountHandle];
        [I: LocalIterator + 'static ]);

    consumer_impl!(
        sum<I>(iter: &I);
        [LocalIterSumHandle<I::Item>];
        [I: LocalIterator + 'static, I::Item: SyncSend +  std::iter::Sum + for<'a> std::iter::Sum<&'a I::Item> , ]);

    //#[doc(hidden)]
    fn local_global_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
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
    fn local_subarray_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
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

/// An interface for dealing with parallel local iterators (intended as the Lamellar version of the Rayon ParellelIterator trait)
///
/// The functions in this trait are available on all local iterators.
/// Additonaly functionality can be found in the [IndexedLocalIterator] trait:
/// these methods are only available for local iterators where the number of elements is known in advance (e.g. after invoking `filter` these methods would be unavailable)
pub trait LocalIterator: SyncSend + IterClone + 'static {
    /// The type of item this local iterator produces
    type Item: Send;

    /// The array to which this local iterator was created from
    type Array: LocalIteratorLauncher;

    /// Internal method used to initalize this local iterator to the correct element and correct length.
    ///
    /// Because we know the number of elements of the array on each PE we can specify the index to start from.
    fn init(&self, start_i: usize, cnt: usize) -> Self;

    /// Return the original array this local iterator belongs too
    fn array(&self) -> Self::Array;

    /// Return the next element in the iterator, otherwise return None
    fn next(&mut self) -> Option<Self::Item>;

    /// Return the maximum number of elements in the iterator
    ///
    /// the actual number of return elements maybe be less (e.g. using a filter iterator)
    fn elems(&self, in_elems: usize) -> usize;

    /// advance the internal iterator localtion by count elements
    fn advance_index(&mut self, count: usize);

    /// Applies `op` on each element of this iterator, producing a new iterator with only the elements that gave `true` results
    ///
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = LocalLockArray::<usize>::new(&world,8,Distribution::Block);
    /// let my_pe = world.my_pe();
    ///
    /// let init_iter = array.local_iter_mut().for_each(move|e| *e = my_pe); //initialize array
    /// let filter_iter = array.local_iter()
    ///                        .enumerate() //we can call enumerate before the filter
    ///                        .filter(|(_,e)| **e%2 == 1).for_each(move|(i,e)| println!("PE: {my_pe} i: {i} elem: {e}"));
    /// world.block_on(async move {
    ///     init_iter.await;
    ///     filter_iter.await;
    /// });
    ///```
    /// Possible output on a 4 PE (1 thread/PE) execution (ordering is likey to be random with respect to PEs)
    ///```text
    /// PE: 1 i: 0 elem: 1
    /// PE: 1 i: 1 elem: 1
    /// PE: 3 i: 0 elem: 3
    /// PE: 3 i: 1 elem: 3
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
    /// array.local_iter_mut().for_each(move|e| *e = my_pe); //initialize array
    /// array.wait_all();
    /// let filter_iter = array.local_iter()
    ///                        .enumerate() //we can call enumerate before the filter
    ///                        .filter_map(|(i,e)| {
    ///     if *e%2 == 0{ Some((i,*e as f32)) }
    ///     else { None }
    /// });
    /// world.block_on(filter_iter.for_each(move|(i,e)| println!("PE: {my_pe} i: {i} elem: {e:?}")));
    ///```
    /// Possible output on a 4 PE (1 thread/PE) execution (ordering is likey to be random with respect to PEs)
    ///```text
    /// PE: 0 i: 0 elem: 0.0
    /// PE: 0 i: 1 elem: 0.0
    /// PE: 2 i: 0 elem: 2.0
    /// PE: 2 i: 1 elem: 2.0
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
    /// array.local_iter().map(|elem| *elem as f64).enumerate().for_each(move|(i,elem)| println!("PE: {my_pe} i: {i} elem: {elem}"));
    /// array.wait_all();
    ///```
    /// Possible output on a 4 PE (1 thread/PE) execution (ordering is likey to be random with respect to PEs)
    ///```text
    /// PE: 0 i: 0 elem: 0.0
    /// PE: 0 i: 1 elem: 0.0
    /// PE: 1 i: 0 elem: 0.0
    /// PE: 1 i: 1 elem: 0.0
    /// PE: 2 i: 0 elem: 0.0
    /// PE: 2 i: 1 elem: 0.0
    /// PE: 3 i: 0 elem: 0.0
    /// PE: 3 i: 1 elem: 0.0
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

    /// Calls a closure on each element of a Local Iterator in parallel on the calling PE (the PE must have some local data of the array).
    ///
    /// This call utilizes the [Schedule::Static][crate::array::iterator::Schedule] policy.
    ///
    /// This function returns a future which can be used to poll for completion of the iteration.
    /// Note calling this function launches the iteration regardless of if the returned future is used or not.
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][LocalIterForEachHandle::spawn] or [blocked on][LocalIterForEachHandle::block]
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// world.block_on(
    ///     array
    ///         .local_iter()
    ///         .for_each(move |elem| println!("{:?} {elem}",std::thread::current().id()))
    /// );
    ///```
    #[must_use = "this iteration adapter is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it."]
    fn for_each<F>(&self, op: F) -> LocalIterForEachHandle
    where
        F: Fn(Self::Item) + SyncSend + Clone + 'static,
    {
        self.array().for_each(self, op)
    }

    /// Calls a closure on each element of a Local Iterator in parallel on the calling PE (the PE must have some local data of the array) using the specififed [Scehedule][crate::array::iterator::Schedule] policy.
    ///
    /// This function returns a future which can be used to poll for completion of the iteration.
    /// Note calling this function launches the iteration regardless of if the returned future is used or not.
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][LocalIterForEachHandle::spawn] or [blocked on][LocalIterForEachHandle::block]
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// array.local_iter().for_each_with_schedule(Schedule::WorkStealing, |elem| println!("{:?} {elem}",std::thread::current().id()));
    /// array.wait_all();
    ///```
    #[must_use = "this iteration adapter is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it."]
    fn for_each_with_schedule<F>(&self, sched: Schedule, op: F) -> LocalIterForEachHandle
    where
        F: Fn(Self::Item) + SyncSend + Clone + 'static,
    {
        self.array().for_each_with_schedule(sched, self, op)
    }

    /// Calls a closure and immediately awaits the result on each element of a Local Iterator in parallel on the calling PE (the PE must have some local data of the array).
    ///
    /// This call utilizes the [Schedule::Static][crate::array::iterator::Schedule] policy.
    ///
    /// The supplied closure must return a future.
    ///
    /// Each thread will only drive a single future at a time.
    ///
    /// This function returns a future which can be used to poll for completion of the iteration.
    /// Note calling this function launches the iteration regardless of if the returned future is used or not.
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][LocalIterForEachHandle::spawn] or [blocked on][LocalIterForEachHandle::block]
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let iter = array.local_iter().for_each_async(|elem| async move {
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
    #[must_use = "this iteration adapter is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it."]
    fn for_each_async<F, Fut>(&self, op: F) -> LocalIterForEachHandle
    where
        F: Fn(Self::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.array().for_each_async(self, op)
    }

    /// Calls a closure on each element of a Local Iterator in parallel on the calling PE (the PE must have some local data of the array) using the specififed [Schedule] policy.
    ///
    /// The supplied closure must return a future.
    ///
    /// Each thread will only drive a single future at a time.
    ///
    /// This function returns a future which can be used to poll for completion of the iteration.
    /// Note calling this function launches the iteration regardless of if the returned future is used or not.
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][LocalIterForEachHandle::spawn] or [blocked on][LocalIterForEachHandle::block]
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// array.local_iter().for_each_async_with_schedule(Schedule::Chunk(10),|elem| async move {
    ///     async_std::task::yield_now().await;
    ///     println!("{:?} {elem}",std::thread::current().id())
    /// });
    /// array.wait_all();
    ///```
    #[must_use = "this iteration adapter is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it."]
    fn for_each_async_with_schedule<F, Fut>(&self, sched: Schedule, op: F) -> LocalIterForEachHandle
    where
        F: Fn(Self::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.array().for_each_async_with_schedule(sched, self, op)
    }

    /// Reduces the elements of the local iterator using the provided closure
    ///
    /// This function returns a future which needs to be driven to completion to retrieve the reduced value.
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][LocalIterReduceHandle::spawn] or [blocked on][LocalIterReduceHandle::block]
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let req = array.local_iter().reduce(|acc,elem| acc+elem);
    /// let sum = array.block_on(req); //wait on the collect request to get the new array
    ///```
    #[must_use = "this iteration adapter is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it."]
    fn reduce<F>(&self, op: F) -> LocalIterReduceHandle<Self::Item, F>
    where
        // &'static Self: LocalIterator + 'static,
        Self::Item: SyncSend + Copy,
        F: Fn(Self::Item, Self::Item) -> Self::Item + SyncSend + Clone + 'static,
    {
        self.array().reduce(self, op)
    }

    /// Reduces the elements of the local iterator using the provided closure and specififed [Schedule] policy
    ///
    /// This function returns a future which needs to be driven to completion to retrieve the reduced value.
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][LocalIterReduceHandle::spawn] or [blocked on][LocalIterReduceHandle::block]
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let req = array.local_iter().reduce_with_schedule(Schedule::Chunk(10),|acc,elem| acc+elem);
    /// let sum = array.block_on(req); //wait on the collect request to get the new array
    ///```
    #[must_use = "this iteration adapter is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it."]
    fn reduce_with_schedule<F>(
        &self,
        sched: Schedule,
        op: F,
    ) -> LocalIterReduceHandle<Self::Item, F>
    where
        // &'static Self: LocalIterator + 'static,
        Self::Item: SyncSend + Copy,
        F: Fn(Self::Item, Self::Item) -> Self::Item + SyncSend + Clone + 'static,
    {
        self.array().reduce_with_schedule(sched, self, op)
    }

    /// Collects the elements of the local iterator into the specified container type
    ///
    /// This function returns a future which needs to be driven to completion to retrieve the new container.
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][LocalIterCollectHandle::spawn] or [blocked on][LocalIterCollectHandle::block]
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Block);
    ///
    /// let array_clone = array.clone();
    /// let req = array.local_iter().map(elem.load()).filter(|elem| elem % 2 == 0).collect::<ReadOnlyArray<usize>>(Distribution::Cyclic);
    /// let new_array = array.block_on(req);
    ///```
    #[must_use = "this iteration adapter is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it."]
    fn collect<A>(&self, d: Distribution) -> LocalIterCollectHandle<Self::Item, A>
    where
        // &'static Self: LocalIterator + 'static,
        Self::Item: Dist + ArrayOps,
        A: AsyncTeamFrom<(Vec<Self::Item>, Distribution)> + SyncSend + Clone + 'static,
    {
        self.array().collect(self, d)
    }

    /// Collects the elements of the local iterator into the specified container type using the specified [Schedule] policy
    ///
    /// This function returns a future which needs to be driven to completion to retrieve the new container.
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][LocalIterCollectHandle::spawn] or [blocked on][LocalIterCollectHandle::block]
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Block);
    ///
    /// let array_clone = array.clone();
    /// let req = array.local_iter().map(elem.load()).filter(|elem| elem % 2 == 0).collect_with_schedule::<ReadOnlyArray<usize>>(Scheduler::WorkStealing,Distribution::Cyclic);
    /// let new_array = array.block_on(req);
    ///```
    #[must_use = "this iteration adapter is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it."]
    fn collect_with_schedule<A>(
        &self,
        sched: Schedule,
        d: Distribution,
    ) -> LocalIterCollectHandle<Self::Item, A>
    where
        // &'static Self: LocalIterator + 'static,
        Self::Item: Dist + ArrayOps,
        A: AsyncTeamFrom<(Vec<Self::Item>, Distribution)> + SyncSend + Clone + 'static,
    {
        self.array().collect_with_schedule(sched, self, d)
    }

    /// Collects the awaited elements of the local iterator into a new LamellarArray
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
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][LocalIterCollectHandle::spawn] or [blocked on][LocalIterCollectHandle::block]
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
    ///     = array_clone.local_iter().map(
    ///         move |elem|
    ///         array_clone
    ///             .fetch_add(elem.load(),1000))
    ///             .collect_async::<ReadOnlyArray<usize>,_>(Distribution::Cyclic);
    /// let _new_array = array.block_on(req);
    ///```
    #[must_use = "this iteration adapter is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it."]
    fn collect_async<A, T>(&self, d: Distribution) -> LocalIterCollectHandle<T, A>
    where
        // &'static Self: DistributedIterator + 'static,
        T: Dist + ArrayOps,
        Self::Item: Future<Output = T> + Send + 'static,
        A: AsyncTeamFrom<(Vec<T>, Distribution)> + SyncSend + Clone + 'static,
    {
        self.array().collect_async(self, d)
    }

    /// Collects the awaited elements of the local iterator into a new LamellarArray, using the provided [Schedule] policy
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
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][LocalIterCollectHandle::spawn] or [blocked on][LocalIterCollectHandle::block]
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
    ///     = array_clone.local_iter().map(
    ///         move |elem|
    ///         array_clone
    ///             .fetch_add(elem.load(),1000))
    ///             .collect_async_with_schedule::<ReadOnlyArray<usize>,_>(Scheduler::Dynamic, Distribution::Cyclic);
    /// let _new_array = array.block_on(req);
    ///```
    #[must_use = "this iteration adapter is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it."]
    fn collect_async_with_schedule<A, T>(
        &self,
        sched: Schedule,
        d: Distribution,
    ) -> LocalIterCollectHandle<T, A>
    where
        // &'static Self: DistributedIterator + 'static,
        T: Dist + ArrayOps,
        Self::Item: Future<Output = T> + Send + 'static,
        A: AsyncTeamFrom<(Vec<T>, Distribution)> + SyncSend + Clone + 'static,
    {
        self.array().collect_async_with_schedule(sched, self, d)
    }

    /// Counts the number of the elements of the local iterator
    ///
    /// This function returns a future which needs to be driven to completion to retrieve the number of elements in the local iterator
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][LocalIterCountHandle::spawn] or [blocked on][LocalIterCountHandle::block]
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let req = array.local_iter().count();
    /// let cnt = array.block_on(req);
    ///```
    #[must_use = "this iteration adapter is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it."]
    fn count(&self) -> LocalIterCountHandle {
        self.array().count(self)
    }

    /// Counts the number of the elements of the local iterator using the provided [Schedule] policy
    ///
    /// This function returns a future which needs to be driven to completion to retrieve the number of elements in the local iterator
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][LocalIterCountHandle::spawn] or [blocked on][LocalIterCountHandle::block]
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let req = array.local_iter().count_with_schedule(Schedule::Dynamic);
    /// let cnt = array.block_on(req);
    ///```
    #[must_use = "this iteration adapter is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it."]
    fn count_with_schedule(&self, sched: Schedule) -> LocalIterCountHandle {
        self.array().count_with_schedule(sched, self)
    }

    /// Sums the elements of the local iterator.
    ///
    /// Takes each element, adds them together, and returns the result.
    ///
    /// An empty iterator returns the zero value of the type.
    ///
    /// This function returns a future which needs to be driven to completion to retrieve the sum
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][LocalIterSumHandle::spawn] or [blocked on][LocalIterSumHandle::block]
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let req = array.local_iter().sum();
    /// let sum = array.block_on(req); //wait on the collect request to get the new array
    ///```
    #[must_use = "this iteration adapter is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it."]
    fn sum(&self) -> LocalIterSumHandle<Self::Item>
    where
        Self::Item: SyncSend + std::iter::Sum + for<'a> std::iter::Sum<&'a Self::Item>,
    {
        self.array().sum(self)
    }

    /// Sums the elements of the local iterator, using the specified [Schedule] policy
    ///
    /// Takes each element, adds them together, and returns the result.
    ///
    /// An empty iterator returns the zero value of the type.
    ///
    /// This function returns a future which needs to be driven to completion to retrieve the sum
    /// # Note
    /// The future retuned by this function is lazy and does nothing unless awaited, [spawned][LocalIterSumHandle::spawn] or [blocked on][LocalIterSumHandle::block]
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let req = array.local_iter().sum_with_schedule(Schedule::Guided);
    /// let sum = array.block_on(req);
    ///```
    #[must_use = "this iteration adapter is lazy and does nothing unless awaited. Either await the returned future, or call 'spawn()' or 'block()' on it."]
    fn sum_with_schedule(&self, sched: Schedule) -> LocalIterSumHandle<Self::Item>
    where
        Self::Item: SyncSend + std::iter::Sum + for<'a> std::iter::Sum<&'a Self::Item>,
    {
        self.array().sum_with_schedule(sched, self)
    }
}

/// An interface for dealing with local iterators which are indexable, meaning it returns an iterator of known length
pub trait IndexedLocalIterator: LocalIterator + SyncSend + IterClone + 'static {
    /// yields the local (to the calling PE) index along with each element
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,8,Distribution::Block);
    /// let my_pe = world.my_pe();
    ///
    /// array.local_iter().enumerate().for_each(move|(i,elem)| println!("PE: {my_pe} i: {i} elem: {elem}"));
    /// array.wait_all();
    ///```
    /// Possible output on a 4 PE (1 thread/PE) execution (ordering is likey to be random with respect to PEs)
    ///```text
    /// PE: 0 i: 0 elem: 0
    /// PE: 0 i: 1 elem: 0
    /// PE: 1 i: 0 elem: 0
    /// PE: 1 i: 1 elem: 0
    /// PE: 2 i: 0 elem: 0
    /// PE: 2 i: 1 elem: 0
    /// PE: 3 i: 0 elem: 0
    /// PE: 3 i: 1 elem: 0
    ///```
    fn enumerate(self) -> Enumerate<Self> {
        Enumerate::new(self, 0)
    }

    /// Split an iterator into fixed-sized chunks
    ///
    /// Returns an iterator that itself returns [Iterator]s over the chunked slices of the array.
    /// If the number of elements is not evenly divisible by `size`, the last chunk may be shorter than `size`
    ///
    /// # Note
    /// If calling this on a LocalLockArray it may be possible to call [blocking_read_local_chunks](crate::array::LocalLockArray::blocking_read_local_chunks), [read_local_chunks](crate::array::LocalLockArray::read_local_chunks)
    /// [blocking_write_local_chunks](crate::array::LocalLockArray::blocking_write_local_chunks), or [write_local_chunks](crate::array::LocalLockArray::blocking_write_local_chunks) for better performance
    ///
    /// If calling this on an UnsafeArray it may be possible to call [local_chunks](crate::array::UnsafeArray::local_chunks) or [local_chunks_mut](crate::array::UnsafeArray::local_chunks_mut)
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,40,Distribution::Block);
    /// let my_pe = world.my_pe();
    ///
    /// array.local_iter().chunks(5).enumerate().for_each(move|(i,chunk)| {
    ///     let chunk_vec: Vec<usize> = chunk.map(|elem| *elem).collect();
    ///     println!("PE: {my_pe} i: {i} chunk: {chunk_vec:?}");
    /// });
    /// array.wait_all();
    /// ```
    /// Possible output on a 4 PE (1 thread/PE) execution (ordering is likey to be random with respect to PEs)
    ///```text
    /// PE: 0 i: 0 chunk: [0, 0, 0, 0, 0]
    /// PE: 0 i: 1 chunk: [0, 0, 0, 0, 0]
    /// PE: 1 i: 0 chunk: [0, 0, 0, 0, 0]
    /// PE: 1 i: 1 chunk: [0, 0, 0, 0, 0]
    /// PE: 2 i: 0 chunk: [0, 0, 0, 0, 0]
    /// PE: 2 i: 1 chunk: [0, 0, 0, 0, 0]
    /// PE: 3 i: 0 chunk: [0, 0, 0, 0, 0]
    /// PE: 3 i: 1 chunk: [0, 0, 0, 0, 0]
    ///```
    fn chunks(self, size: usize) -> Chunks<Self> {
        Chunks::new(self, 0, 0, size)
    }

    // /// Applies `op` to each element producing a new iterator with the results
    // ///
    // /// # Examples
    // ///```
    // /// use lamellar::array::prelude::*;
    // ///
    // /// let world = LamellarWorldBuilder::new().build();
    // /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,8,Distribution::Block);
    // /// let my_pe = world.my_pe();
    // ///
    // /// array.local_iter().map(|elem| *elem as f64).enumerate().for_each(move|(i,elem)| println!("PE: {my_pe} i: {i} elem: {elem}"));
    // /// array.wait_all();
    // ///```
    // /// Possible output on a 4 PE (1 thread/PE) execution (ordering is likey to be random with respect to PEs)
    // ///```text
    // /// PE: 0 i: 0 elem: 0.0
    // /// PE: 0 i: 1 elem: 0.0
    // /// PE: 1 i: 0 elem: 0.0
    // /// PE: 1 i: 1 elem: 0.0
    // /// PE: 2 i: 0 elem: 0.0
    // /// PE: 2 i: 1 elem: 0.0
    // /// PE: 3 i: 0 elem: 0.0
    // /// PE: 3 i: 1 elem: 0.0
    // ///```
    // fn map<F, R>(self, op: F) -> MapIndexed<Self, F>
    // where
    //     F: Fn(Self::Item) -> R + Clone + 'static,
    //     R: Send + 'static,
    // {
    //     MapIndexed::new(self, op)
    // }

    /// An iterator that skips the first `n` elements
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,16,Distribution::Block);
    /// let my_pe = world.my_pe();
    ///
    /// array.local_iter().enumerate().skip(3).for_each(move|(i,elem)| println!("PE: {my_pe} i: {i} elem: {elem}"));
    /// array.wait_all();
    ///```
    /// Possible output on a 4 PE (1 thread/PE) execution (ordering is likey to be random with respect to PEs)
    ///```text
    /// PE: 0 i:3 elem: 0
    /// PE: 1 i:3 elem: 0
    /// PE: 2 i:3 elem: 0
    /// PE: 3 i:3 elem: 0
    ///```
    fn skip(self, count: usize) -> Skip<Self> {
        Skip::new(self, count, 0)
    }

    /// An iterator that steps by `step_size` elements
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,28,Distribution::Block);
    /// let my_pe = world.my_pe();
    ///
    /// array.local_iter().enumerate().step_by(3).for_each(move|(i,elem)| println!("PE: {my_pe} i: {i} elem: {elem}"));
    /// array.wait_all();
    ///```
    /// Possible output on a 4 PE (1 thread/PE) execution (ordering is likey to be random with respect to PEs)
    ///```text
    /// PE: 0 i: 0 elem: 0
    /// PE: 0 i: 3 elem: 0
    /// PE: 0 i: 6 elem: 0
    /// PE: 1 i: 0 elem: 0
    /// PE: 1 i: 3 elem: 0
    /// PE: 1 i: 6 elem: 0
    /// PE: 2 i: 0 elem: 0
    /// PE: 2 i: 3 elem: 0
    /// PE: 2 i: 6 elem: 0
    /// PE: 3 i: 0 elem: 0
    /// PE: 3 i: 3 elem: 0
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
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,16,Distribution::Block);
    /// let my_pe = world.my_pe();
    ///
    /// array.local_iter().enumerate().take(3).for_each(move|(i,elem)| println!("PE: {my_pe} i: {i} elem: {elem}"));
    /// array.wait_all();
    ///```
    /// Possible output on a 4 PE (1 thread/PE) execution (ordering is likey to be random with respect to PEs)
    ///```text
    /// PE: 0 i: 0 elem: 0
    /// PE: 0 i: 1 elem: 0
    /// PE: 0 i: 2 elem: 0
    /// PE: 1 i: 0 elem: 0
    /// PE: 1 i: 1 elem: 0
    /// PE: 1 i: 2 elem: 0
    /// PE: 2 i: 0 elem: 0
    /// PE: 2 i: 1 elem: 0
    /// PE: 2 i: 2 elem: 0
    /// PE: 3 i: 0 elem: 0
    /// PE: 3 i: 1 elem: 0
    /// PE: 3 i: 2 elem: 0
    ///```
    fn take(self, count: usize) -> Take<Self> {
        Take::new(self, count)
    }

    /// Iterates over tuples `(A,B)` where the `A` items are from this iterator and the `B` items are from the iter in the argument.
    /// If the two iterators are of unequal length, the returned iterator will be equal in length to the shorter of the two.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array_A: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,16,Distribution::Block);
    /// let array_B: LocalLockArray<usize> = LocalLockArray::new(&world,12,Distribution::Block);
    /// let my_pe = world.my_pe();
    ///
    /// //initalize array_B
    /// array_B.local_iter_mut().enumerate().for_each(move|(i,elem)| *elem = i);
    /// array_B.wait_all();
    ///
    /// array_A.local_iter().zip(array_B.local_iter()).for_each(move|(elem_A,elem_B)| println!("PE: {my_pe} A: {elem_A} B: {elem_B}"));
    /// array_A.wait_all();
    ///```
    /// Possible output on a 4 PE (1 thread/PE) execution (ordering is likey to be random with respect to PEs)
    ///```text
    /// PE: 0 A: 0 B: 0
    /// PE: 0 A: 0 B: 1
    /// PE: 0 A: 0 B: 2
    /// PE: 1 A: 0 B: 0
    /// PE: 1 A: 0 B: 1
    /// PE: 1 A: 0 B: 2
    /// PE: 2 A: 0 B: 0
    /// PE: 2 A: 0 B: 1
    /// PE: 2 A: 0 B: 2
    /// PE: 3 A: 0 B: 0
    /// PE: 3 A: 0 B: 1
    /// PE: 3 A: 0 B: 2
    ///```
    fn zip<I: IndexedLocalIterator>(self, iter: I) -> Zip<Self, I> {
        Zip::new(self, iter)
    }

    /// given an local index return the corresponding local iterator index ( or None otherwise)
    fn iterator_index(&self, index: usize) -> Option<usize>;
}

/// Immutable LamellarArray local iterator
///
/// This struct is created by calling `local_iter` on any of the LamellarArray types
///
/// # Examples
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block);
///
/// let local_iter = array.local_iter().for_each(move|e| println!("{}",e.load()));
/// world.block_on(local_iter);
///```
#[derive(Clone)]
pub struct LocalIter<'a, T: Dist + 'static, A: LamellarArray<T>> {
    data: A,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'a T>,
}

impl<'a, T: Dist, A: LamellarArray<T>> IterClone for LocalIter<'a, T, A> {
    fn iter_clone(&self, _: Sealed) -> Self {
        LocalIter {
            data: self.data.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
            _marker: PhantomData,
        }
    }
}

impl<'a, T: Dist, A: LamellarArray<T>> std::fmt::Debug for LocalIter<'a, T, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LocalIter{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

impl<T: Dist, A: LamellarArray<T>> LocalIter<'_, T, A> {
    pub(crate) fn new(data: A, cur_i: usize, cnt: usize) -> Self {
        // println!("new dist iter {:?} {:? } {:?}",cur_i, cnt, cur_i+cnt);
        LocalIter {
            data,
            cur_i,
            end_i: cur_i + cnt,
            _marker: PhantomData,
        }
    }
}

impl<
        T: Dist + 'static,
        A: LamellarArray<T> + LocalIteratorLauncher + SyncSend + Clone + 'static,
    > LocalIterator for LocalIter<'static, T, A>
{
    type Item = &'static T;
    type Array = A;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("init local_iterator start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?} {:?}",start_i,cnt, start_i+cnt,max_i,std::thread::current().id());

        // println!("{:?} LocalIter init {start_i} {cnt} {} {}",std::thread::current().id(), start_i+cnt,max_i);
        LocalIter {
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
            // println!("{:?} LocalIter next cur: {:?} end {:?} Some left",std::thread::current().id(),self.cur_i,self.end_i);
            self.cur_i += 1;
            unsafe {
                self.data
                    .local_as_ptr()
                    .offset((self.cur_i - 1) as isize)
                    .as_ref()
            }
        } else {
            None
        }
    }
    fn elems(&self, in_elems: usize) -> usize {
        in_elems
    }
    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

impl<
        T: Dist + 'static,
        A: LamellarArray<T> + SyncSend + LocalIteratorLauncher + Clone + 'static,
    > IndexedLocalIterator for LocalIter<'static, T, A>
{
    fn iterator_index(&self, index: usize) -> Option<usize> {
        // println!("{:?} \t LocalIter iterator index {index} {:?}",std::thread::current().id(),self.cur_i);
        if index < self.data.len() {
            Some(index) //everyone at this point as calculated the actual index (cause we are local only) so just return it
        } else {
            None
        }
    }
}

/// Mutable LamellarArray local iterator
///
/// This struct is created by calling `local_iter_mut` on any of the [LamellarWriteArray][crate::array::LamellarWriteArray] types
///
/// # Examples
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block);
/// let my_pe = world.my_pe();
///
/// let local_iter = array.local_iter_mut().for_each(move|e| e.store(my_pe) );
/// world.block_on(local_iter);
///```
pub struct LocalIterMut<'a, T: Dist, A: LamellarArray<T>> {
    data: A,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'a T>,
}

impl<'a, T: Dist, A: LamellarArray<T>> IterClone for LocalIterMut<'a, T, A> {
    fn iter_clone(&self, _: Sealed) -> Self {
        LocalIterMut {
            data: self.data.clone(),
            cur_i: self.cur_i,
            end_i: self.end_i,
            _marker: PhantomData,
        }
    }
}

impl<'a, T: Dist, A: LamellarArray<T>> std::fmt::Debug for LocalIterMut<'a, T, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LocalIterMut{{ data.len: {:?}, cur_i: {:?}, end_i: {:?} }}",
            self.data.len(),
            self.cur_i,
            self.end_i
        )
    }
}

impl<T: Dist, A: LamellarArray<T>> LocalIterMut<'_, T, A> {
    pub(crate) fn new(data: A, cur_i: usize, cnt: usize) -> Self {
        LocalIterMut {
            data,
            cur_i,
            end_i: cur_i + cnt,
            _marker: PhantomData,
        }
    }
}

impl<
        T: Dist + 'static,
        A: LamellarArray<T> + SyncSend + LocalIteratorLauncher + Clone + 'static,
    > LocalIterator for LocalIterMut<'static, T, A>
{
    type Item = &'static mut T;
    type Array = A;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("{:?} LocalIter init {start_i} {cnt} {} {}",std::thread::current().id(), start_i+cnt,max_i);
        LocalIterMut {
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
    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

impl<
        T: Dist + 'static,
        A: LamellarArray<T> + SyncSend + LocalIteratorLauncher + Clone + 'static,
    > IndexedLocalIterator for LocalIterMut<'static, T, A>
{
    fn iterator_index(&self, index: usize) -> Option<usize> {
        // println!("{:?} \t LocalIterMut iterator index {index} {:?}",std::thread::current().id(),self.cur_i);
        if index < self.data.len() {
            Some(index) //everyone at this point as calculated the actual index (cause we are local only) so just return it
        } else {
            None
        }
    }
}

//! Distributed (and parallel) iteration of a LamellarArray
//!
//! This module provides distriubuted iteration capabilities for lamellar arrays,
//! where iteration over sub slices of the LamellarArray occurs in parallel on each PE.
//!
//! We try to minimize data movement as much as possible, but the runtime will manage remote transfers
//! as necessary depending on the iterators used.

// mod chunks;
mod enumerate;
mod filter;
mod filter_map;
pub(crate) mod for_each;
mod map;
mod skip;
mod step_by;
mod take;
// mod zip;

// use chunks::*;
use enumerate::*;
use filter::*;
use filter_map::*;
use map::*;
use skip::*;
use step_by::*;
use take::*;
// use zip::*;

use crate::array::iterator::one_sided_iterator::OneSidedIterator;
use crate::array::iterator::Schedule;
use crate::array::{
    AtomicArray, Distribution, GenericAtomicArray, LamellarArray, LamellarArrayPut,
    NativeAtomicArray, UnsafeArray,
};
use crate::lamellar_request::LamellarRequest;
use crate::memregion::Dist;
use crate::LamellarTeamRT;

use crate::active_messaging::SyncSend;

use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
use futures::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use rand::seq::SliceRandom;
use rand::thread_rng;

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct Collect<I>
where
    I: DistributedIterator,
{
    pub(crate) data: I,
    pub(crate) start_i: usize,
    pub(crate) end_i: usize,
}

impl<I> std::fmt::Debug for Collect<I>
where
    I: DistributedIterator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Collect {{   start_i: {:?}, end_i: {:?} }}",
            self.start_i, self.end_i
        )
    }
}

#[lamellar_impl::rt_am_local]
impl<I> LamellarAm for Collect<I>
where
    I: DistributedIterator + 'static,
    I::Item: Sync,
{
    async fn exec(&self) -> Vec<I::Item> {
        let mut iter = self.data.init(self.start_i, self.end_i - self.start_i);
        let mut vec = Vec::new();
        while let Some(elem) = iter.next() {
            vec.push(elem);
        }
        vec
    }
}

#[lamellar_impl::AmLocalDataRT(Clone, Debug)]
pub(crate) struct CollectAsync<I, T>
where
    I: DistributedIterator,
    I::Item: Future<Output = T>,
    T: Dist,
{
    pub(crate) data: I,
    pub(crate) start_i: usize,
    pub(crate) end_i: usize,
    pub(crate) _phantom: PhantomData<T>,
}

#[lamellar_impl::rt_am_local]
impl<I, T> LamellarAm for CollectAsync<I, T, Fut>
where
    I: DistributedIterator + 'static,
    I::Item: Future<Output = T> + Send,
    T: Dist,
{
    async fn exec(&self) -> Vec<<I::Item as Future>::Output> {
        let mut iter = self.data.init(self.start_i, self.end_i - self.start_i);
        let mut vec = Vec::new();
        while let Some(elem) = iter.next() {
            let res = elem.await;
            vec.push(res);
        }
        vec
    }
}

#[doc(hidden)]
#[async_trait]
pub trait DistIterRequest {
    type Output;
    async fn into_future(mut self: Box<Self>) -> Self::Output;
    fn wait(self: Box<Self>) -> Self::Output;
}

#[doc(hidden)]
pub struct DistIterForEachHandle {
    pub(crate) reqs: Vec<Box<dyn LamellarRequest<Output = ()>>>,
}

// impl Drop for DistIterForEachHandle {
//     fn drop(&mut self) {
//         println!("dropping DistIterForEachHandle");
//     }
// }

#[doc(hidden)]
#[async_trait]
impl DistIterRequest for DistIterForEachHandle {
    type Output = ();
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        for req in self.reqs.drain(..) {
            req.into_future().await;
        }
    }
    fn wait(mut self: Box<Self>) -> Self::Output {
        for req in self.reqs.drain(..) {
            req.get();
        }
    }
}

#[doc(hidden)]
pub struct DistIterCollectHandle<T: Dist, A: From<UnsafeArray<T>> + SyncSend> {
    pub(crate) reqs: Vec<Box<dyn LamellarRequest<Output = Vec<T>>>>,
    pub(crate) distribution: Distribution,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) _phantom: PhantomData<A>,
}

impl<T: Dist, A: From<UnsafeArray<T>> + SyncSend> DistIterCollectHandle<T, A> {
    fn create_array(&self, local_vals: &Vec<T>) -> A {
        self.team.barrier();
        let local_sizes =
            UnsafeArray::<usize>::new(self.team.clone(), self.team.num_pes, Distribution::Block);
        unsafe {
            local_sizes.local_as_mut_slice()[0] = local_vals.len();
        }
        local_sizes.barrier();
        // local_sizes.print();
        let mut size = 0;
        let mut my_start = 0;
        let my_pe = self.team.team_pe.expect("pe not part of team");
        // local_sizes.print();
        unsafe {
            local_sizes
                .onesided_iter()
                .into_iter()
                .enumerate()
                .for_each(|(i, local_size)| {
                    size += local_size;
                    if i < my_pe {
                        my_start += local_size;
                    }
                });
        }
        // println!("my_start {} size {}", my_start, size);
        let array = UnsafeArray::<T>::new(self.team.clone(), size, self.distribution); //implcit barrier

        // safe because only a single reference to array on each PE
        // we calculate my_start so that each pes local vals are guaranteed to not overwrite another pes values.
        unsafe { array.put(my_start, local_vals) };
        array.into()
    }
}
#[async_trait]
impl<T: Dist, A: From<UnsafeArray<T>> + SyncSend> DistIterRequest for DistIterCollectHandle<T, A> {
    type Output = A;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        let mut local_vals = vec![];
        for req in self.reqs.drain(0..) {
            let v = req.into_future().await;
            local_vals.extend(v);
        }
        self.create_array(&local_vals)
    }
    fn wait(mut self: Box<Self>) -> Self::Output {
        let mut local_vals = vec![];
        for req in self.reqs.drain(0..) {
            let v = req.get();
            local_vals.extend(v);
        }
        self.create_array(&local_vals)
    }
}

#[doc(hidden)]
#[enum_dispatch]
pub trait DistIteratorLauncher {
    fn for_each<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static;

    fn for_each_with_schedule<I, F>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static;

    fn for_each_async<I, F, Fut>(
        &self,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static;

    fn for_each_async_with_schedule<I, F, Fut>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static;

    fn collect<I, A>(&self, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: DistributedIterator + 'static,
        I::Item: Dist,
        A: From<UnsafeArray<I::Item>> + SyncSend + 'static;

    fn collect_async<I, A, B>(
        &self,
        iter: &I,
        d: Distribution,
    ) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: DistributedIterator + 'static,
        I::Item: Future<Output = B> + Send + 'static,
        B: Dist,
        A: From<UnsafeArray<B>> + SyncSend + 'static;

    #[doc(hidden)]
    fn global_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize>;

    #[doc(hidden)]
    fn subarray_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize>;

    // #[doc(hidden)]
    // fn subarray_pe_and_offset_for_global_index(&self, index: usize, chunk_size: usize) -> Option<(usize,usize)>;

    #[doc(hidden)]
    fn team(&self) -> Pin<Arc<LamellarTeamRT>>;
}

/// An interface for dealing with distributed iterators (intended as a parallel and distributed version of the standard iterator trait)
///
/// The functions in this trait are available on all distributed iterators.
/// Additonaly functionality can be found in the [IndexedDistributedIterator] trait:
/// these methods are only available for distributed iterators where the number of elements is known in advance (e.g. after invoking `filter` these methods would be unavailable)
pub trait DistributedIterator: SyncSend + Clone + 'static {
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

    /// Calls a closure on each element of a Distributed Iterator in parallel and distributed on each PE (which owns data of the iterated array).
    ///
    /// Calling this function invokes an implicit barrier across all PEs in the Array
    ///
    /// This call is utilizes the [Schedule::Static][crate::array::iterator::Schedule] policy.
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
    fn for_each<F>(&self, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        F: Fn(Self::Item) + SyncSend + Clone + 'static,
    {
        self.array().for_each(self, op)
    }

    /// Calls a closure and immediately awaits the result on each element of a Distributed Iterator in parallel and distributed on each PE (which owns data of the iterated array).
    ///
    /// Calling this function invokes an implicit barrier across all PEs in the Array
    ///
    /// This call is utilizes the [Schedule::Static][crate::array::iterator::Schedule] policy.
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
    fn for_each_async<F, Fut>(&self, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        F: Fn(Self::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.array().for_each_async(self, op)
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
    fn collect<A>(&self, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        // &'static Self: DistributedIterator + 'static,
        Self::Item: Dist, 
        A: From<UnsafeArray<Self::Item>> + SyncSend + 'static,
    {
        self.array().collect(self, d)
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
    fn collect_async<A, T>(&self, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        // &'static Self: DistributedIterator + 'static,
        T: Dist,
        Self::Item: Future<Output = T> + Send + 'static,
        A: From<UnsafeArray<<Self::Item as Future>::Output>> + SyncSend + 'static,
    {
        self.array().collect_async(self, d)
    }
}

/// An interface for dealing with distributed iterators which are indexable, meaning it returns an iterator of known length
pub trait IndexedDistributedIterator: DistributedIterator + SyncSend + Clone + 'static {
    /// Calls a closure on each element of a Distributed Iterator in parallel and distributed on each PE (which owns data of the iterated array) using the specififed schedule policy.
    ///
    /// Calling this function invokes an implicit barrier across all PEs in the Array
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
    /// array.dist_iter().for_each_with_schedule(Schedule::WorkStealing, |elem| println!("{:?} {elem}",std::thread::current().id()));
    /// array.wait_all();
    ///```
    fn for_each_with_schedule<F>(
        &self,
        sched: Schedule,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        F: Fn(Self::Item) + SyncSend + Clone + 'static,
    {
        self.array().for_each_with_schedule(sched, self, op)
    }

    /// Calls a closure and immediately awaits the result on each element of a Distributed Iterator in parallel and distributed on each PE (which owns data of the iterated array).
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
    fn for_each_async_with_schedule<F, Fut>(
        &self,
        sched: Schedule,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        F: Fn(Self::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.array().for_each_async_with_schedule(sched, self, op)
    }

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
#[derive(Clone)]
pub struct DistIterMut<'a, T: Dist, A: LamellarArray<T>> {
    data: A,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'a T>,
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

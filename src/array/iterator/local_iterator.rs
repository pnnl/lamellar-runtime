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
// pub(crate) mod fold;
pub(crate) mod for_each;
mod map;
pub(crate) mod reduce;
mod skip;
mod step_by;
mod take;
mod zip;

use chunks::*;
use enumerate::*;
use filter::*;
use filter_map::*;
use map::*;
use skip::*;
use step_by::*;
use take::*;
use zip::*;

use crate::array::iterator::one_sided_iterator::OneSidedIterator;
use crate::array::iterator::Schedule;
use crate::array::{AtomicArray, Distribution, LamellarArray, LamellarArrayPut, UnsafeArray,operations::ArrayOps};
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
pub(crate) struct LocalCollect<I>
where
    I: LocalIterator,
{
    pub(crate) data: I,
    pub(crate) start_i: usize,
    pub(crate) end_i: usize,
}

impl<I> std::fmt::Debug for LocalCollect<I>
where
    I: LocalIterator,
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
impl<I> LamellarAm for LocalCollect<I>
where
    I: LocalIterator + 'static,
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
pub(crate) struct LocalCollectAsync<I, T>
where
    I: LocalIterator,
    I::Item: Future<Output = T>,
    T: Dist,
{
    pub(crate) data: I,
    pub(crate) start_i: usize,
    pub(crate) end_i: usize,
    pub(crate) _phantom: PhantomData<T>,
}

#[lamellar_impl::rt_am_local]
impl<I, T> LamellarAm for LocalCollectAsync<I, T, Fut>
where
    I: LocalIterator + 'static,
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
pub trait LocalIterRequest {
    type Output;
    async fn into_future(mut self: Box<Self>) -> Self::Output;
    fn wait(self: Box<Self>) -> Self::Output;
}

#[doc(hidden)]
pub struct LocalIterForEachHandle {
    pub(crate) reqs: Vec<Box<dyn LamellarRequest<Output = ()>>>,
}

// impl Drop for LocalIterForEachHandle {
//     fn drop(&mut self) {
//         println!("dropping LocalIterForEachHandle");
//     }
// }

#[doc(hidden)]
#[async_trait]
impl LocalIterRequest for LocalIterForEachHandle {
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
pub struct LocalIterCollectHandle<T: Dist + ArrayOps, A: From<UnsafeArray<T>> + SyncSend> {
    pub(crate) reqs: Vec<Box<dyn LamellarRequest<Output = Vec<T>>>>,
    pub(crate) distribution: Distribution,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) _phantom: PhantomData<A>,
}

impl<T: Dist + ArrayOps, A: From<UnsafeArray<T>> + SyncSend> LocalIterCollectHandle<T, A> {
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
impl<T: Dist + ArrayOps, A: From<UnsafeArray<T>> + SyncSend> LocalIterRequest
    for LocalIterCollectHandle<T, A>
{
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
pub trait LocalIteratorLauncher {
    fn local_for_each<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static;

    fn local_for_each_with_schedule<I, F>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static;

    fn local_for_each_async<I, F, Fut>(
        &self,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static;

    fn local_for_each_async_with_schedule<I, F, Fut>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static;
    
    fn local_reduce<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = I::Item> + Send>>
    where
        I: LocalIterator + 'static,
        I::Item: SyncSend,
        F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static;

    // fn local_collect<I, A>(&self, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    // where
    //     I: LocalIterator + 'static,
    //     I::Item: Dist,
    //     A: From<UnsafeArray<I::Item>> + SyncSend + 'static;

    // fn local_collect_async<I, A, B>(
    //     &self,
    //     iter: &I,
    //     d: Distribution,
    // ) -> Pin<Box<dyn Future<Output = A> + Send>>
    // where
    //     I: LocalIterator + 'static,
    //     I::Item: Future<Output = B> + Send + 'static,
    //     B: Dist,
    //     A: From<UnsafeArray<B>> + SyncSend + 'static;

    #[doc(hidden)]
    fn local_global_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize>;

    #[doc(hidden)]
    fn local_subarray_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize>;

    #[doc(hidden)]
    fn team(&self) -> Pin<Arc<LamellarTeamRT>>;
}

/// An interface for dealing with parallel local iterators (intended as the Lamellar version of the Rayon ParellelIterator trait)
///
/// The functions in this trait are available on all local iterators.
/// Additonaly functionality can be found in the [IndexedLocalIterator] trait:
/// these methods are only available for local iterators where the number of elements is known in advance (e.g. after invoking `filter` these methods would be unavailable)
pub trait LocalIterator: SyncSend + Clone + 'static {
    /// The type of item this distributed iterator produces
    type Item: Send;

    /// The array to which this distributed iterator was created from
    type Array: LocalIteratorLauncher;

    /// Internal method used to initalize this distributed iterator to the correct element and correct length.
    ///
    /// Because we know the number of elements of the array on each PE we can specify the index to start from.
    fn init(&self, start_i: usize, cnt: usize) -> Self;

    /// Return the original array this distributed iterator belongs too
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
    /// world.block_on(filter_iter.for_each(move|(i,e)| println!("PE: {my_pe} i: {i} elem: {e}")));
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

    /// Calls a closure on each element of a Local Iterator in parallel on the calling PE (the PE must have some local data of the array).
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
    ///         .local_iter()
    ///         .for_each(move |elem| println!("{:?} {elem}",std::thread::current().id()))
    /// );
    ///```
    fn for_each<F>(&self, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        F: Fn(Self::Item) + SyncSend + Clone + 'static,
    {
        self.array().local_for_each(self, op)
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
    ///
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
    fn for_each_async<F, Fut>(&self, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        F: Fn(Self::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.array().local_for_each_async(self, op)
    }

     
    /// Reduces the elements of the local iterator using the provided closure
    ///
    /// This function returns a future which needs to be driven to completion to retrieve the new container.
    ///
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
    fn reduce<F>(&self, op: F) -> Pin<Box<dyn Future<Output = Self::Item> + Send>>
    where
        // &'static Self: LocalIterator + 'static,
        Self::Item: SyncSend,
        F: Fn(Self::Item,Self::Item) -> Self::Item + SyncSend + Clone + 'static,
    {
        self.array().local_reduce(self, op)
    }

    /*
    /// Collects the elements of the local iterator into the specified container type
    ///
    /// This function returns a future which needs to be driven to completion to retrieve the new container.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(&world,100,Distribution::Block);
    ///
    /// let req = array.local_iter().filter(|elem|  elem < 10).collect::<Vec<usize>>(Distribution::Block);
    /// let new_vec = array.block_on(req); //wait on the collect request to get the new array
    ///```
    fn collect<A>(&self, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        // &'static Self: LocalIterator + 'static,
        Self::Item: Dist,
        A: From<UnsafeArray<Self::Item>> + SyncSend + 'static,
    {
        self.array().local_collect(self, d)
    }
    */

    /*
    /// Collects the elements of the local iterator into the specified container type
    /// Each element from the iterator must return a Future
    ///
    /// Each thread will only drive a single future at a time.
    ///
    /// This function returns a future which needs to be driven to completion to retrieve the new container.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Block);
    ///
    /// let array_clone = array.clone();
    /// let req = array.dist_iter().map(|elem|  array_clone.fetch_add(elem,1000)).collect_async::<Vec<usize>>(Distribution::Cyclic);
    /// let new_vec = array.block_on(req);
    fn collect_async<A, T>(&self, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        // &'static Self: LocalIterator + 'static,
        T: Dist,
        Self::Item: Future<Output = T> + Send + 'static,
        A: From<UnsafeArray<<Self::Item as Future>::Output>> + SyncSend + 'static,
    {
        self.array().local_collect_async(self, d)
    }
    */
}

/// An interface for dealing with local iterators which are indexable, meaning it returns an iterator of known length
pub trait IndexedLocalIterator: LocalIterator + SyncSend + Clone + 'static {
    /// Calls a closure on each element of a Local Iterator in parallel on the calling PE (the PE must have some local data of the array) using the specififed schedule policy.
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
    /// array.local_iter().for_each_with_schedule(Schedule::WorkStealing, |elem| println!("{:?} {elem}",std::thread::current().id()));
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
        self.array().local_for_each_with_schedule(sched, self, op)
    }

    /// Calls a closure on each element of a Local Iterator in parallel on the calling PE (the PE must have some local data of the array) using the specififed schedule policy.
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
    /// array.local_iter().for_each_async_with_schedule(Schedule::Chunk(10),|elem| async move {
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
        self.array()
            .local_for_each_async_with_schedule(sched, self, op)
    }

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
    /// array_B.dist_iter_mut().enumerate().for_each(move|(i,elem)| *elem = i);
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
#[derive(Clone)]
pub struct LocalIterMut<'a, T: Dist, A: LamellarArray<T>> {
    data: A,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'a T>,
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

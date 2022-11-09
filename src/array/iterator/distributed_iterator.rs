mod chunks;
mod enumerate;
mod filter;
mod filter_map;
pub(crate) mod for_each;
mod ignore;
mod map;
mod step_by;
mod take;
mod zip;

use chunks::*;
use enumerate::*;
use filter::*;
use filter_map::*;
use ignore::*;
use map::*;
use step_by::*;
use take::*;
use zip::*;

use crate::memregion::Dist;
use crate::LamellarRequest;
use crate::LamellarTeamRT;
// use crate::LamellarArray;
use crate::array::iterator::serial_iterator::SerialIterator;
use crate::array::{
    AtomicArray, Distribution, GenericAtomicArray, LamellarArray, NativeAtomicArray, UnsafeArray,
}; //, LamellarArrayPut, LamellarArrayGet};

use crate::active_messaging::SyncSend;
// use crate::scheduler::SchedulerQueue;

use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
// use futures::{future, Future, StreamExt};
use futures::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use rand::seq::SliceRandom;
use rand::thread_rng;

/// The Schedule type controls how elements of a LamellarArray are distributed to threads when 
/// calling `for_each_with_schedule` on a distributed iterator.
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
        local_sizes
            .ser_iter()
            .into_iter()
            .enumerate()
            .for_each(|(i, local_size)| {
                size += local_size;
                if i < my_pe {
                    my_start += local_size;
                }
            });
        // println!("my_start {} size {}", my_start, size);
        let array = UnsafeArray::<T>::new(self.team.clone(), size, self.distribution); //implcit barrier
        array.put(my_start, local_vals);
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


#[enum_dispatch]
pub trait DistIteratorLauncher {
    /// Calls a closure on each element of a Distributed Iterator in parallel and distributed on each PE (which owns data of the iterated array).
    /// 
    /// Calling this function invokes an implicit barrier across all PEs in the Array, after this barrier no further communication is performed
    /// as each PE will only process elements local to itself
    ///
    /// Currently the static schedule is work distribution policy for the threads
    ///
    /// This function returns a future which can be used to poll for completion of the iteration.
    /// Note calling this function launches the iteration regardless of if the returned future is used or not.
    ///
    /// #Example
    ///```
    /// use lamellar::array::{
    ///     DistributedIterator, Distribution, ReadOnlyArray,
    /// };
    ///
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(...);
    ///
    /// array.dist_iter().for_each(|elem| println!("{:?} {elem}",std::thread::current().id()));
    ///```
    fn for_each<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static;

    /// Calls a closure on each element of a Distributed Iterator in parallel and distributed on each PE (which owns data of the iterated array) using the specififed schedule policy.
    /// 
    /// Calling this function invokes an implicit barrier across all PEs in the Array, after this barrier no further communication is performed
    /// as each PE will only process elements local to itself
    ///
    /// This function returns a future which can be used to poll for completion of the iteration.
    /// Note calling this function launches the iteration regardless of if the returned future is used or not.
    ///
    /// #Example
    ///```
    /// use lamellar::array::{
    ///     iterator::distributed_iterator::Schedule, DistributedIterator, Distribution, ReadOnlyArray,
    /// };
    ///
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(...);
    ///
    /// array.dist_iter().for_each_with_schedule(Schedule::WorkStealing, |elem| println!("{:?} {elem}",std::thread::current().id()));
    ///```
    fn for_each_with_schedule<I, F>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static;

    /// Calls a closure and immediately awaits the result on each element of a Distributed Iterator in parallel and distributed on each PE (which owns data of the iterated array).
    /// 
    /// Calling this function invokes an implicit barrier across all PEs in the Array, after this barrier no further communication is performed
    /// as each PE will only process elements local to itself
    ///
    /// Currently the static schedule is work distribution policy for the threads
    ///
    /// The supplied closure must return a future.
    /// 
    /// Each thread will only drive a single future at a time. 
    ///
    /// This function returns a future which can be used to poll for completion of the iteration.
    /// Note calling this function launches the iteration regardless of if the returned future is used or not.
    ///
    /// #Example
    ///```
    /// use lamellar::array::{
    ///     iterator::distributed_iterator::Schedule, DistributedIterator, Distribution, ReadOnlyArray,
    /// };
    ///
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(...);
    ///
    /// array.dist_iter().for_each_async(|elem| async move { 
    ///     async_std::task::yield_now().await;
    ///     println!("{:?} {elem}",std::thread::current().id())
    /// });
    /// 
    /// // essentially gets converted into (on each thread)
    /// for fut in array.iter(){
    ///     fut.await;
    /// }
    ///```
    fn for_each_async<I, F, Fut>(
        &self,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static;

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
    /// #Example
    ///```
    /// use lamellar::array::{
    ///     DistributedIterator, Distribution, ReadOnlyArray,
    /// };
    ///
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(...);
    ///
    /// array.dist_iter().for_each_with_schedule(Schedule::Chunks(10),|elem| async move { 
    ///     async_std::task::yield_now().await;
    ///     println!("{:?} {elem}",std::thread::current().id())
    /// });
    ///
    /// // essentially gets converted into (on each thread)
    /// for fut in array.iter(){
    ///     fut.await;
    /// }
    ///```
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
    ///```
    /// use lamellar::array::{
    ///     DistributedIterator, Distribution, ReadOnlyArray, AtomicArray
    /// };
    ///
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(...);
    ///
    /// let req = array.dist_iter().filter(|elem|  elem < 10).collect::<AtomicArray<usize>>(Distribution::Block);
    /// let new_array = array.block_on(req);
    ///```
    fn collect<I, A>(&self, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: DistributedIterator + 'static,
        I::Item: Dist,
        A: From<UnsafeArray<I::Item>> + SyncSend + 'static;
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
    ///```
    /// use lamellar::array::{
    ///     DistributedIterator, Distribution, ReadOnlyArray, AtomicArray
    /// };
    ///
    /// let array: AtomicArray<usize> = AtomicArray::new(...);
    /// let array_clone = array.clone();
    /// let req = array.dist_iter().map(|elem|  array_clone.fetch_add(elem,1000)).collect_async::<ReadOnlyArray<usize>>(Distribution::Cyclic);
    /// let new_array = array.block_on(req);
    ///
    /// // collect_asyncessentially gets converted into (on each thread)
    /// let mut data = vec![];
    /// for fut in array.iter(){
    ///     data.push(fut.await);
    /// }
    ///```
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

    #[doc(hidden)]
    fn team(&self) -> Pin<Arc<LamellarTeamRT>>;
}

pub trait DistributedIterator: SyncSend + Clone + 'static {
    type Item: Send;
    type Array: DistIteratorLauncher;
    fn init(&self, start_i: usize, cnt: usize) -> Self;
    fn array(&self) -> Self::Array;
    fn next(&mut self) -> Option<Self::Item>;
    fn elems(&self, in_elems: usize) -> usize;
    fn global_index(&self, index: usize) -> Option<usize>;
    fn subarray_index(&self, index: usize) -> Option<usize>;
    fn advance_index(&mut self, count: usize);

    fn enumerate(self) -> Enumerate<Self> {
        Enumerate::new(self, 0)
    }
    fn filter<F>(self, op: F) -> Filter<Self, F>
    where
        F: Fn(&Self::Item) -> bool + Clone + 'static,
    {
        Filter::new(self, op)
    }
    fn filter_map<F, R>(self, op: F) -> FilterMap<Self, F>
    where
        F: Fn(Self::Item) -> Option<R> + Clone + 'static,
        R: Send + 'static,
    {
        FilterMap::new(self, op)
    }
    fn chunks(self, size: usize) -> Chunks<Self> {
        Chunks::new(self, 0, 0, size)
    }
    fn ignore(self, count: usize) -> Ignore<Self> {
        Ignore::new(self, count)
    }
    fn map<F, R>(self, op: F) -> Map<Self, F>
    where
        F: Fn(Self::Item) -> R + Clone + 'static,
        R: Send + 'static,
    {
        Map::new(self, op)
    }
    fn step_by(self, step_size: usize) -> StepBy<Self> {
        StepBy::new(self, step_size)
    }
    fn take(self, count: usize) -> Take<Self> {
        Take::new(self, count)
    }
    fn zip<I: DistributedIterator>(self, iter: I) -> Zip<Self, I> {
        Zip::new(self, iter)
    }

    /// Calls a closure on each element of a Distributed Iterator in parallel and distributed on each PE (which owns data of the iterated array).
    /// 
    /// Calling this function invokes an implicit barrier across all PEs in the Array, after this barrier no further communication is performed
    /// as each PE will only process elements local to itself
    ///
    /// Currently the static schedule is work distribution policy for the threads
    ///
    /// This function returns a future which can be used to poll for completion of the iteration.
    /// Note calling this function launches the iteration regardless of if the returned future is used or not.
    ///
    /// #Example
    ///```
    /// use lamellar::array::{
    ///     DistributedIterator, Distribution, ReadOnlyArray,
    /// };
    ///
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(...);
    ///
    /// array.dist_iter().for_each(|elem| println!("{:?} {elem}",std::thread::current().id()));
    ///```
    fn for_each<F>(&self, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        F: Fn(Self::Item) + SyncSend + Clone + 'static,
    {
        self.array().for_each(self, op)
    }

    /// Calls a closure on each element of a Distributed Iterator in parallel and distributed on each PE (which owns data of the iterated array) using the specififed schedule policy.
    /// 
    /// Calling this function invokes an implicit barrier across all PEs in the Array, after this barrier no further communication is performed
    /// as each PE will only process elements local to itself
    ///
    /// This function returns a future which can be used to poll for completion of the iteration.
    /// Note calling this function launches the iteration regardless of if the returned future is used or not.
    ///
    /// #Example
    ///```
    /// use lamellar::array::{
    ///     iterator::distributed_iterator::Schedule, DistributedIterator, Distribution, ReadOnlyArray,
    /// };
    ///
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(...);
    ///
    /// array.dist_iter().for_each_with_schedule(Schedule::WorkStealing, |elem| println!("{:?} {elem}",std::thread::current().id()));
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
    /// Currently the static schedule is work distribution policy for the threads
    ///
    /// The supplied closure must return a future.
    /// 
    /// Each thread will only drive a single future at a time. 
    ///
    /// This function returns a future which can be used to poll for completion of the iteration.
    /// Note calling this function launches the iteration regardless of if the returned future is used or not.
    ///
    /// #Example
    ///```
    /// use lamellar::array::{
    ///     iterator::distributed_iterator::Schedule, DistributedIterator, Distribution, ReadOnlyArray,
    /// };
    ///
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(...);
    ///
    /// array.dist_iter().for_each_async(|elem| async move { 
    ///     async_std::task::yield_now().await;
    ///     println!("{:?} {elem}",std::thread::current().id())
    /// });
    /// 
    /// // essentially gets converted into (on each thread)
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
    /// #Example
    ///```
    /// use lamellar::array::{
    ///     DistributedIterator, Distribution, ReadOnlyArray,
    /// };
    ///
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(...);
    ///
    /// array.dist_iter().for_each_with_schedule(Schedule::Chunks(10),|elem| async move { 
    ///     async_std::task::yield_now().await;
    ///     println!("{:?} {elem}",std::thread::current().id())
    /// });
    ///
    /// // essentially gets converted into (on each thread)
    /// for fut in array.iter(){
    ///     fut.await;
    /// }
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
    ///```
    /// use lamellar::array::{
    ///     DistributedIterator, Distribution, ReadOnlyArray, AtomicArray
    /// };
    ///
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(...);
    ///
    /// let req = array.dist_iter().filter(|elem|  elem < 10).collect::<AtomicArray<usize>>(Distribution::Block);
    /// let new_array = array.block_on(req);
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
    ///```
    /// use lamellar::array::{
    ///     DistributedIterator, Distribution, ReadOnlyArray, AtomicArray
    /// };
    ///
    /// let array: AtomicArray<usize> = AtomicArray::new(...);
    /// let array_clone = array.clone();
    /// let req = array.dist_iter().map(|elem|  array_clone.fetch_add(elem,1000)).collect_async::<ReadOnlyArray<usize>>(Distribution::Cyclic);
    /// let new_array = array.block_on(req);
    ///
    /// // collect_asyncessentially gets converted into (on each thread)
    /// let mut data = vec![];
    /// for fut in array.iter(){
    ///     data.push(fut.await);
    /// }
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

// impl<
//         T: Dist + 'static,
//         A: LamellarArray<T> + DistIteratorLauncher + SyncSend + Clone + 'static,
//     > DistIter<'static, T, A>
// {
//     pub fn for_each<F>(&self, op: F)
//     where
//         F: Fn(&T)   + Clone + 'static,
//     {
//         self.data.clone().for_each(self, op);
//     }
//     pub fn for_each_async<F, Fut>(&self, op: F)
//     where
//         F: Fn(&T) -> Fut   + Clone + 'static,
//         Fut: Future<Output = ()>   + Clone + 'static,
//     {
//         self.data.clone().for_each_async(self, op);
//     }
// }

impl<
        T: Dist + 'static,
        A: LamellarArray<T> + DistIteratorLauncher + SyncSend + Clone + 'static,
    > DistributedIterator for DistIter<'static, T, A>
{
    type Item = &'static T;
    type Array = A;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
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
        // println!("dist iter next cur: {:?} end {:?}",self.cur_i,self.end_i);
        if self.cur_i < self.end_i {
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
        // println!("dist iter elems {:?}",in_elems);
        in_elems
    }
    fn global_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.global_index_from_local(index, 1);
        // println!("dist_iter index: {:?} global_index {:?}", index,g_index);
        g_index
    }
    fn subarray_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.subarray_index_from_local(index, 1);
        // println!("dist_iter index: {:?} global_index {:?}", index,g_index);
        g_index
    }
    // fn chunk_size(&self) -> usize {
    //     1
    // }
    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

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

// impl<
//         T: Dist + 'static,
//         A: LamellarArray<T> + SyncSend + DistIteratorLauncher + Clone + 'static,
//     > DistIterMut<'static, T, A>
// {
//     pub fn for_each<F>(&self, op: F)
//     where
//         F: Fn(&mut T)   + Clone + 'static,
//     {
//         self.data.clone().for_each(self, op);
//     }
//     pub fn for_each_async<F, Fut>(&self, op: F)
//     where
//         F: Fn(&mut T) -> Fut   + Clone + 'static,
//         Fut: Future<Output = ()>   + Clone + 'static,
//     {
//         self.data.clone().for_each_async(self, op);
//     }
// }

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
    fn global_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.global_index_from_local(index, 1);
        // println!("dist_iter index: {:?} global_index {:?}", index,g_index);
        g_index
    }
    fn subarray_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.subarray_index_from_local(index, 1);
        g_index
    }
    // fn chunk_size(&self) -> usize {
    //     1
    // }
    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

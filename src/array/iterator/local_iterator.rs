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
use crate::lamellar_request::LamellarRequest;
use crate::LamellarTeamRT;
// use crate::LamellarArray;
use crate::array::iterator::one_sided_iterator::OneSidedIterator;
use crate::array::iterator::Schedule;
use crate::array::{
    AtomicArray, Distribution, LamellarArray, UnsafeArray,
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
pub struct LocalIterCollectHandle<T: Dist, A: From<UnsafeArray<T>> + SyncSend> {
    pub(crate) reqs: Vec<Box<dyn LamellarRequest<Output = Vec<T>>>>,
    pub(crate) distribution: Distribution,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) _phantom: PhantomData<A>,
}

impl<T: Dist, A: From<UnsafeArray<T>> + SyncSend> LocalIterCollectHandle<T, A> {
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
            .onesided_iter()
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
impl<T: Dist, A: From<UnsafeArray<T>> + SyncSend> LocalIterRequest for LocalIterCollectHandle<T, A> {
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
pub trait LocalIteratorLauncher {
    /// Calls a closure on each element of a Local Iterator in parallel and distributed on each PE (which owns data of the iterated array).
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
    ///     LocalIterator, Distribution, ReadOnlyArray,
    /// };
    ///
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(...);
    ///
    /// array.dist_iter().for_each(|elem| println!("{:?} {elem}",std::thread::current().id()));
    ///```
    fn local_for_each<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static;

    /// Calls a closure on each element of a Local Iterator in parallel and distributed on each PE (which owns data of the iterated array) using the specififed schedule policy.
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
    ///     iterator::local_iterator::Schedule, LocalIterator, Distribution, ReadOnlyArray,
    /// };
    ///
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(...);
    ///
    /// array.dist_iter().for_each_with_schedule(Schedule::WorkStealing, |elem| println!("{:?} {elem}",std::thread::current().id()));
    ///```
    fn local_for_each_with_schedule<I, F>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static;

    /// Calls a closure and immediately awaits the result on each element of a Local Iterator in parallel and distributed on each PE (which owns data of the iterated array).
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
    ///     iterator::local_iterator::Schedule, LocalIterator, Distribution, ReadOnlyArray,
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
    fn local_for_each_async<I, F, Fut>(
        &self,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static;

    /// Calls a closure and immediately awaits the result on each element of a Local Iterator in parallel and distributed on each PE (which owns data of the iterated array).
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
    ///     LocalIterator, Distribution, ReadOnlyArray,
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

    /*    
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
    ///     LocalIterator, Distribution, ReadOnlyArray, AtomicArray
    /// };
    ///
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(...);
    ///
    /// let req = array.dist_iter().filter(|elem|  elem < 10).collect::<AtomicArray<usize>>(Distribution::Block);
    /// let new_array = array.block_on(req);
    ///```
    fn local_collect<I, A>(&self, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: LocalIterator + 'static,
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
    ///     LocalIterator, Distribution, ReadOnlyArray, AtomicArray
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
    fn local_collect_async<I, A, B>(
        &self,
        iter: &I,
        d: Distribution,
    ) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: LocalIterator + 'static,
        I::Item: Future<Output = B> + Send + 'static,
        B: Dist,
        A: From<UnsafeArray<B>> + SyncSend + 'static;
    */

    #[doc(hidden)]
    fn local_global_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize>;

    #[doc(hidden)]
    fn local_subarray_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize>;

    #[doc(hidden)]
    fn team(&self) -> Pin<Arc<LamellarTeamRT>>;
}

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

    /// given an index in subarray space return the corresponding index from the original array
    // fn global_index(&self, index: usize) -> Option<usize>;

    /// given an index in the original array space return the corresponding subarray index ( or None otherwise)
    fn subarray_index(&self, index: usize) -> Option<usize>;
    fn advance_index(&mut self, count: usize);

   
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
    

    /// Calls a closure on each element of a Local Iterator in parallel and distributed on each PE (which owns data of the iterated array).
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
    ///     LocalIterator, Distribution, ReadOnlyArray,
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
        self.array().local_for_each(self, op)
    }

     /// Calls a closure and immediately awaits the result on each element of a Local Iterator in parallel and distributed on each PE (which owns data of the iterated array).
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
    ///     iterator::local_iterator::Schedule, LocalIterator, Distribution, ReadOnlyArray,
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
        self.array().local_for_each_async(self, op)
    }

    
    /*
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
    ///     LocalIterator, Distribution, ReadOnlyArray, AtomicArray
    /// };
    ///
    /// let array: ReadOnlyArray<usize> = ReadOnlyArray::new(...);
    ///
    /// let req = array.dist_iter().filter(|elem|  elem < 10).collect::<AtomicArray<usize>>(Distribution::Block);
    /// let new_array = array.block_on(req);
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
    ///     LocalIterator, Distribution, ReadOnlyArray, AtomicArray
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
        // &'static Self: LocalIterator + 'static,
        T: Dist,
        Self::Item: Future<Output = T> + Send + 'static,
        A: From<UnsafeArray<<Self::Item as Future>::Output>> + SyncSend + 'static,
    {
        self.array().local_collect_async(self, d)
    }
    */
}

pub trait IndexedLocalIterator: LocalIterator + SyncSend + Clone + 'static {
    /// Calls a closure on each element of a Local Iterator in parallel and distributed on each PE (which owns data of the iterated array) using the specififed schedule policy.
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
    ///     iterator::local_iterator::Schedule, LocalIterator, Distribution, ReadOnlyArray,
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
        self.array().local_for_each_with_schedule(sched, self, op)
    }
    
    /// Calls a closure and immediately awaits the result on each element of a Local Iterator in parallel and distributed on each PE (which owns data of the iterated array).
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
    ///     LocalIterator, Distribution, ReadOnlyArray,
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
        self.array().local_for_each_async_with_schedule(sched, self, op)
    }

    fn enumerate(self) -> Enumerate<Self> {
        Enumerate::new(self, 0)
    }
    fn chunks(self, size: usize) -> Chunks<Self> {
        Chunks::new(self, 0, 0, size)
    }
    fn map<F, R>(self, op: F) -> Map<Self, F>
    where
        F: Fn(Self::Item) -> R + Clone + 'static,
        R: Send + 'static,
    {
        Map::new(self, op)
    }
    fn ignore(self, count: usize) -> Ignore<Self> {
        Ignore::new(self, count, 0)
    }
    fn step_by(self, step_size: usize) -> StepBy<Self> {
        StepBy::new(self, step_size,0)
    }
    fn take(self, count: usize) -> Take<Self> {
        Take::new(self, count)
    }
    fn zip<I: IndexedLocalIterator>(self, iter: I) -> Zip<Self, I> {
        Zip::new(self, iter)
    }

    /// given an local index return the corresponding local iterator index ( or None otherwise)
    fn iterator_index(&self, index: usize) -> Option<usize>;
}

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

    fn subarray_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.local_subarray_index_from_local(index, 1);
        g_index
    }

    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

impl<
        T: Dist + 'static,
        A: LamellarArray<T> + SyncSend + LocalIteratorLauncher + Clone + 'static,
    > IndexedLocalIterator for LocalIter<'static, T, A> {
        fn iterator_index(&self, index: usize) -> Option<usize> {
            // println!("{:?} \t LocalIter iterator index {index} {:?}",std::thread::current().id(),self.cur_i);
            if index < self.data.len(){
                Some(index) //everyone at this point as calculated the actual index (cause we are local only) so just return it
            }
            else {
                None
            }
        }
}

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

// impl<
//         T: Dist + 'static,
//         A: LamellarArray<T> + SyncSend + LocalIteratorLauncher + Clone + 'static,
//     > LocalIterMut<'static, T, A>
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
    // fn global_index(&self, index: usize) -> Option<usize> {
    //     let g_index = self.data.global_index_from_local(index, 1);
    //     // println!("dist_iter index: {:?} global_index {:?}", index,g_index);
    //     g_index
    // }
    fn subarray_index(&self, index: usize) -> Option<usize> {
        let g_index = self.data.local_subarray_index_from_local(index, 1);
        g_index
    }
    // fn chunk_size(&self) -> usize {
    //     1
    // }
    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

impl<
        T: Dist + 'static,
        A: LamellarArray<T> + SyncSend + LocalIteratorLauncher + Clone + 'static,
    > IndexedLocalIterator for LocalIterMut<'static, T, A> {
        fn iterator_index(&self, index: usize) -> Option<usize> {
            // println!("{:?} \t LocalIterMut iterator index {index} {:?}",std::thread::current().id(),self.cur_i);
            if index < self.data.len(){
                Some(index) //everyone at this point as calculated the actual index (cause we are local only) so just return it
            }
            else {
                None
            }
        }
}

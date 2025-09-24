//! One-sided (i.e. serial, not parallel) iteration of a LamellarArray on a single PE
//!
//! This module provides serial iteration of an entire LamellarArray on the calling PE.
//! The resulting OneSidedIterator can be converted in to standard Iterator, to allow
//! using all the functionality and capabilities those provide.
//!
//! These iterators will automatically transfer data in from Remote PEs as needed
//!
//! # Examples
//!
//! Examples can be found under [lamellar-runtime/examples/array_examples/](https://github.com/pnnl/lamellar-runtime/tree/master/examples/array_examples)
mod chunks;
use chunks::*;

mod skip;
use skip::*;

mod step_by;
use step_by::*;

mod zip;
use zip::*;

//TODO: further test the buffered iter
// mod buffered;
// use buffered::*;

use crate::array::LamellarArray;
use crate::memregion::Dist;
use crate::warnings::RuntimeWarning;
use crate::LamellarTask;
use futures_util::{Future, Stream};
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};

//TODO: Think about an active message based method for transfering data that performs data reducing iterators before sending
// i.e. for something like step_by(N) we know that only every N elements actually needs to get sent...
pub(crate) mod private {
    use crate::array::LamellarRdmaGet;
    use crate::memregion::Dist;
    use crate::{LamellarArray, LamellarEnv};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    pub trait OneSidedIteratorInner {
        /// The type of item self distributed iterator produces
        type Item: Send;

        /// The underlying element type of the Array self iterator belongs to
        type ElemType: Dist + 'static;

        /// The orgininal array that created self iterator
        type Array: LamellarRdmaGet<Self::ElemType>
            + LamellarArray<Self::ElemType>
            + LamellarEnv
            + Send;

        fn init(&mut self);
        /// Return the next element in the iterator, otherwise return None
        fn next(&mut self) -> Option<Self::Item>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;

        /// advance the internal iterator localtion by count elements
        fn advance_index(&mut self, count: usize);

        fn advance_index_pin(self: Pin<&mut Self>, count: usize);

        /// Return the original array self distributed iterator belongs too
        fn array(&self) -> Self::Array;

        /// The size of the returned Item
        fn item_size(&self) -> usize {
            std::mem::size_of::<Self::Item>()
        }
    }
}
/// An interface for dealing with one sided iterators of LamellarArrays
///
/// The functions in self trait are available on all [one-sided iterators](crate::array::iterator::one_sided_iterator)
/// (which run over the data of a distributed array on a single PE).  Typically
/// the provided iterator functions are optimized versions of the standard Iterator equivalents to reduce data movement assoicated with handling distributed arrays
///
/// Additonaly functionality can be found by converting these iterators into Standard Iterators (with potential loss in data movement optimizations)
///
/// Note that currently One Sided Iterators will iterate over the distributed array serially, we are planning a parallel version in a future release.
pub trait OneSidedIterator: private::OneSidedIteratorInner {
    // /// Buffer (fetch/get) the next element in the array into the provided memory region (transferring data from a remote PE if necessary)
    // fn buffered_next(
    //     &mut self,
    //     mem_region: OneSidedMemoryRegion<u8>,
    // ) -> Option<Pin<Box<Future<()>>>>;

    // /// return the first `Self::Item` from a `u8` buffer
    // fn from_mem_region(&self, mem_region: OneSidedMemoryRegion<u8>) -> Option<Self::Item>;

    /// Split an iterator into fixed-sized chunks
    ///
    /// Returns an iterator that returns OneSidedMemoryRegions of the chunked array.
    /// If the number of elements is not evenly divisible by `size`, the last chunk may be shorter than `size`
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = LocalLockArray::<usize>::new(&world,24,Distribution::Block).block();
    /// let my_pe = world.my_pe();
    /// array.dist_iter_mut().for_each(move|e| *e = my_pe).block(); //initialize array using a distributed iterator
    /// if my_pe == 0 {
    ///     for chunk in array.onesided_iter().chunks(5).into_iter() { //convert into a standard Iterator
    ///         // SAFETY: chunk is safe in self instance because self will be the only handle to the memory region,
    ///         // and the runtime has verified that data is already placed in it
    ///         println!("PE: {my_pe} chunk: {:?}",unsafe {chunk.as_slice()});
    ///     }
    /// }
    /// ```
    /// Output on a 4 PE execution
    ///```text
    /// PE: 0 chunk: [0, 0, 0, 0, 0]
    /// PE: 0 chunk: [0, 1, 1, 1, 1]
    /// PE: 0 chunk: [1, 1, 2, 2, 2]
    /// PE: 0 chunk: [2, 2, 2, 3, 3]
    /// PE: 0 chunk: [3, 3, 3, 3]
    ///```
    fn chunks(self, chunk_size: usize) -> Chunks<Self>
    where
        Self: Sized + Send,
    {
        Chunks::new(self, chunk_size)
    }

    /// An iterator that skips the first `n` elements
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = LocalLockArray::<usize>::new(&world,8,Distribution::Block).block();
    /// let my_pe = world.my_pe();
    /// array.dist_iter_mut().for_each(move|e| *e = my_pe).block(); //initialize array using a distributed iterator
    /// if my_pe == 0 {
    ///     for elem in array.onesided_iter().skip(3).into_iter() {  //convert into a standard Iterator
    ///         println!("PE: {my_pe} elem: {elem}");
    ///     }
    /// }
    /// ```
    /// Output on a 4 PE execution
    ///```text
    /// PE: 0 elem: 1
    /// PE: 0 elem: 2
    /// PE: 0 elem: 2
    /// PE: 0 elem: 3
    /// PE: 0 elem: 3
    ///```
    fn skip(self, count: usize) -> Skip<Self>
    where
        Self: Sized + Send,
    {
        Skip::new(self, count)
    }

    /// An iterator that steps by `step_size` elements
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = LocalLockArray::<usize>::new(&world,8,Distribution::Block).block();
    /// let my_pe = world.my_pe();
    /// array.dist_iter_mut().for_each(move|e| *e = my_pe).block(); //initialize array using a distributed iterator
    /// if my_pe == 0 {
    ///     for elem in array.onesided_iter().step_by(3).into_iter() { //convert into a standard Iterator
    ///         println!("PE: {my_pe} elem: {elem}");
    ///     }
    /// }
    ///```
    /// Output on a 4 PE execution
    ///```text
    /// PE: 0 elem: 0
    /// PE: 0 elem: 2
    /// PE: 0 elem: 3
    ///```
    fn step_by(self, step_size: usize) -> StepBy<Self>
    where
        Self: Sized + Send,
    {
        StepBy::new(self, step_size)
    }

    /// Iterates over tuples `(A,B)` where the `A` items are from self iterator and the `B` items are from the iter in the argument.
    /// If the two iterators or of unequal length, the returned iterator will be equal in length to the shorter of the two.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array_A = LocalLockArray::<usize>::new(&world,8,Distribution::Block).block();
    /// let array_B: LocalLockArray<usize> = LocalLockArray::new(&world,12,Distribution::Block).block();
    /// let my_pe = world.my_pe();
    /// //initialize arrays using a distributed iterator
    /// let _ = array_A.dist_iter_mut().for_each(move|e| *e = my_pe).spawn();
    /// let _ = array_B.dist_iter_mut().enumerate().for_each(move|(i,elem)| *elem = i).spawn();
    /// world.wait_all(); // instead of waiting on both arrays in separate calls, just wait for all tasks at the world level
    ///
    /// if my_pe == 0 {
    ///     for (elemA,elemB) in array_A.onesided_iter().zip(array_B.onesided_iter()).into_iter() { //convert into a standard Iterator
    ///         println!("PE: {my_pe} A: {elemA} B: {elemB}");
    ///     }
    /// }
    /// ```
    /// Output on a 4 PE execution
    ///```text
    /// PE: 0 A: 0 B: 0
    /// PE: 0 A: 0 B: 1
    /// PE: 0 A: 1 B: 2
    /// PE: 0 A: 1 B: 3
    /// PE: 0 A: 2 B: 4
    /// PE: 0 A: 2 B: 5
    /// PE: 0 A: 3 B: 6
    /// PE: 0 A: 3 B: 7
    ///```
    fn zip<I>(self, iter: I) -> Zip<Self, I>
    where
        Self: Sized + Send,
        I: OneSidedIterator + Sized + Send,
    {
        Zip::new(self, iter)
    }

    // fn buffered(self, buf_size: usize) -> Buffered<Self>
    // where
    //     Self: Sized + Send,
    // {
    //     Buffered::new(self, buf_size)
    // }

    /// Convert a one-sided iterator into a standard Rust [Iterator], enabling one to use any of the functions available on `Iterator`s
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = LocalLockArray::<usize>::new(&world,8,Distribution::Block).block();
    /// let my_pe = world.my_pe();
    /// array.dist_iter_mut().for_each(move|e| *e = my_pe).block(); //initialize array using a distributed iterator
    /// if my_pe == 0 {
    ///     let sum = array.onesided_iter().into_iter().take(4).map(|elem| *elem as f64).sum::<f64>();
    ///     println!("Sum: {sum}")
    /// }
    /// ```
    ///  Output on a 4 PE execution
    ///```text
    /// Sum: 2.0
    ///```
    fn into_iter(mut self) -> OneSidedIteratorIter<Self>
    where
        Self: Sized + Send,
    {
        RuntimeWarning::BlockingCall("into_iter", "into_stream()").print();

        // println!("Into Iter");
        self.init();
        OneSidedIteratorIter { iter: self }
    }

    /// Convert a one-sided iterator into a standard Rust [Stream] for iteration in async contexts, enabling one to use any of the functions available on `Stream`s
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use futures_util::stream::{StreamExt};
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = LocalLockArray::<usize>::new(&world,8,Distribution::Block).block();
    /// let my_pe = world.my_pe();
    /// let num_pes = world.num_pes();
    /// let _ =array.dist_iter_mut().for_each(move|e| *e = my_pe).spawn(); //initialize array using a distributed iterator
    /// array.wait_all();
    ///
    /// world.block_on (async move {
    ///      if my_pe == 0 {
    ///          let result = array.onesided_iter().into_stream().take(4).map(|elem|*elem as f64).all(|elem|async move{ elem < num_pes as f64});
    ///          assert_eq!(result.await, true);
    ///      }
    ///  });
    /// ```
    fn into_stream(mut self) -> OneSidedStream<Self>
    where
        Self: Sized + Send,
    {
        // println!("Into Stream");
        self.init();
        OneSidedStream { iter: self }
    }
}

/// An immutable standard Rust [Iterator] backed by a [OneSidedIterator](crate::array::iterator::one_sided_iterator).
///
/// This object iterates over data serially on a single PE ; compare with [distributed iterators](crate::array::iterator::distributed_iterator), which iterate over data on all PEs associate with the array.
///
/// This struct is created by calling [into_iter][OneSidedIterator::into_iter] a OneSidedIterator
///
/// # Examples
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block).block();
///
/// let std_iter = array.onesided_iter().into_iter();
/// for e in std_iter {
///     println!("{e}");
/// }
///```
pub struct OneSidedIteratorIter<I> {
    pub(crate) iter: I,
}

impl<I> Iterator for OneSidedIteratorIter<I>
where
    I: OneSidedIterator,
{
    type Item = <I as private::OneSidedIteratorInner>::Item;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// An immutable one sided iterator of a LamellarArray
///
/// This struct is created by calling `onesided_iter` on any of the LamellarArray types
///
/// # Examples
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block).block();
///
/// let one_sided_iter = array.onesided_iter();
///```
#[pin_project]
pub struct OneSidedIter<T: Dist + 'static, A: LamellarArray<T>> {
    array: A,
    // buf_0: OneSidedMemoryRegion<T>,
    // buf_0: LamellarBuffer<T, Vec<T>>,
    buf_size: usize,
    index: usize,
    buf_index: usize,
    // ptr: SendNonNull<T>,
    #[pin]
    state: State<T>,
    // _marker: PhantomData<&'a T>,
}

#[pin_project(project = StateProj)]
pub(crate) enum State<T> {
    // Ready,
    Pending(#[pin] LamellarTask<Vec<T>>),
    Buffered(Vec<T>),
    Finished,
}

impl<T: Dist + 'static, A: LamellarArray<T>> OneSidedIter<T, A> {
    pub(crate) fn new(array: &A, buf_size: usize) -> OneSidedIter<T, A> {
        // let buf_0 = array.team().alloc_one_sided_mem_region(buf_size);
        // potentially unsafe depending on the array type (i.e. UnsafeArray - which requries unsafe to construct an iterator),
        // but safe with respect to the buf_0 as self is the only reference

        // let req = unsafe { array.internal_get(0, &buf_0) };
        // let ptr = unsafe {
        //     SendNonNull(
        //         NonNull::new(buf_0.as_mut_ptr().expect("data should be local"))
        //             .expect("ptr is non null"),
        //     )
        // };
        let iter = OneSidedIter {
            array: array.clone(),
            // buf_0: LamellarBuffer::<T, Vec<T>>::from_vec(vec![Default::default(); buf_size]),
            buf_size,
            index: 0,
            buf_index: 0,
            // ptr: ptr,
            state: State::Finished,
        };

        iter
    }
}

impl<T: Dist + 'static + Clone + Send, A: LamellarArray<T> + Send> OneSidedIterator
    for OneSidedIter<T, A>
{
}

impl<T: Dist + 'static + Clone + Send, A: LamellarArray<T> + Send> private::OneSidedIteratorInner
    for OneSidedIter<T, A>
{
    type ElemType = T;
    type Item = T;
    type Array = A;

    fn init(&mut self) {
        // println!(
        //     "Iter init: index: {:?} buf_len {:?} array_len {:?}",
        //     self.index,
        //     self.buf_0.len(),
        //     self.array.len()
        // );
        // let temp_buf = self.buf_0.split(0);
        let mut req = unsafe { self.array.get_buffer(self.index, self.buf_size) };
        // req.launch();
        self.state = State::Pending(req.spawn());
    }

    fn next(&mut self) -> Option<Self::Item> {
        let mut cur_state = State::Finished;
        std::mem::swap(&mut self.state, &mut cur_state);
        match cur_state {
            State::Pending(req) => {
                // req.blocking_wait();
                let data = req.block();
                // if self.buf_0.try_reset() != true {
                //     panic!("Cannot reset buffer as it is shared");
                // }
                let val = data[0];
                self.state = State::Buffered(data);

                self.index += 1;
                self.buf_index += 1;
                Some(val)
            }
            State::Buffered(mut data) => {
                //once here the we never go back to pending
                if self.index < self.array.len() {
                    if self.buf_index == self.buf_size {
                        //need to get new data
                        self.buf_index = 0;

                        let mut new_data = if self.index + self.buf_size < self.array.len() {
                            // potentially unsafe depending on the array type (i.e. UnsafeArray - which requries unsafe to construct an iterator),
                            // but safe with respect to the buf_0 as we have consumed all its content and this is the only reference
                            // let temp_buf = self.buf_0.split(0);

                            unsafe { self.array.get_buffer(self.index, self.buf_size).block() }
                            // if self.buf_0.try_reset() != true {
                            //     panic!("Cannot reset buffer as it is shared");
                            // }
                        } else {
                            // potentially unsafe depending on the array type (i.e. UnsafeArray - which requries unsafe to construct an iterator),
                            // but safe with respect to the buf_0 as we have consumed all its content and this is the only reference
                            // sub_region is set to the remaining size of the array so we will not have an out of bounds issue

                            unsafe {
                                self.array
                                    .get_buffer(self.index, self.array.len() - self.index)
                                    .block()
                            }
                        };
                        std::mem::swap(&mut data, &mut new_data);
                    }
                    let val = data[self.buf_index];
                    self.state = State::Buffered(data);
                    self.index += 1;
                    self.buf_index += 1;
                    Some(val)
                } else {
                    self.state = State::Finished;
                    None
                }
            }
            State::Finished => None,
        }
    }

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        let res = match this.state.as_mut().project() {
            StateProj::Pending(req) => match req.poll(cx) {
                Poll::Ready(data) => {
                    let val = data[0];
                    *this.state = State::Buffered(data);
                    *this.index += 1;
                    *this.buf_index += 1;
                    Some(val)
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            },
            StateProj::Buffered(data) => {
                if *this.index < this.array.len() {
                    // *this.state = State::Buffered;
                    if *this.buf_index == *this.buf_size {
                        //need to get new data
                        *this.buf_index = 0;
                        let req = if *this.index + *this.buf_size < this.array.len() {
                            unsafe { this.array.get_buffer(*this.index, *this.buf_size).spawn() }
                        } else {
                            unsafe {
                                this.array
                                    .get_buffer(*this.index, this.array.len() - *this.index)
                                    .spawn()
                            }
                        };
                        // req.ready_or_set_waker(cx.waker());
                        *this.state = State::Pending(req);

                        return Poll::Pending;
                    }
                    let val = data[*this.buf_index];
                    *this.index += 1;
                    *this.buf_index += 1;
                    Some(val)
                } else {
                    *this.state = State::Finished;
                    None
                }
            }
            StateProj::Finished => None,
        };
        Poll::Ready(res)
    }

    fn advance_index(&mut self, count: usize) {
        let this = Pin::new(self);
        this.advance_index_pin(count);
    }

    fn advance_index_pin(mut self: Pin<&mut Self>, count: usize) {
        // let this = self.as_mut().project();
        self.index += count;
        self.buf_index += count;
        if self.buf_index == self.buf_size {
            self.buf_index = 0;
            // self.fill_buffer(0);
            if self.index + self.buf_size < self.array.len() {
                // potentially unsafe depending on the array type (i.e. UnsafeArray - which requries unsafe to construct an iterator),
                // but safe with respect to the buf_0 as we have consumed all its content and self is the only reference
                // let temp_buf = self.buf_0.split(0);
                let mut req = unsafe { self.array.get_buffer(self.index, self.buf_size).spawn() };
                // req.launch();
                self.state = State::Pending(req);
            } else {
                // potentially unsafe depending on the array type (i.e. UnsafeArray - which requries unsafe to construct an iterator),
                // but safe with respect to the buf_0 as we have consumed all its content and self is the only reference
                // sub_region is set to the remaining size of the array so we will not have an out of bounds issue
                // let temp_buf = self.buf_0.split(0);
                // let _ = temp_buf.split(self.array.len() - self.index);
                let mut req = unsafe {
                    self.array
                        .get_buffer(self.index, self.array.len() - self.index)
                        .spawn()
                };
                // req.launch();
                self.state = State::Pending(req);
            }
        }
    }
    fn array(&self) -> Self::Array {
        self.array.clone()
    }

    fn item_size(&self) -> usize {
        std::mem::size_of::<T>()
    }
}

/// An immutable standard Rust [Stream] backed by a [OneSidedIterator](crate::array::iterator::one_sided_iterator) for iteration in async contexts.
///
/// This object iterates over data serially on a single PE ; compare with [distributed iterators](crate::array::iterator::distributed_iterator), which iterate over data on all PEs associate with the array.
///
/// This struct is created by calling [into_stream][OneSidedIterator::into_iter] a OneSidedIterator
///
/// # Examples
///```
/// use lamellar::array::prelude::*;
/// use futures_util::stream::StreamExt;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block).block();
/// world.block_on(async move {
///     let mut stream = array.onesided_iter().into_stream();
///     while let Some(e) = stream.next().await {
///         println!("{e}");
///     }
/// });
///```
#[pin_project]
pub struct OneSidedStream<I> {
    #[pin]
    pub(crate) iter: I,
}

impl<I> Stream for OneSidedStream<I>
where
    I: OneSidedIterator,
{
    type Item = <I as private::OneSidedIteratorInner>::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // let me = self.get_mut();
        // println!("OneSidedStream polling");
        let this = self.project();
        let res = this.iter.poll_next(cx);
        match res {
            Poll::Ready(Some(res)) => {
                // println!("OneSidedStream ready");
                Poll::Ready(Some(res))
            }
            Poll::Ready(None) => {
                // println!("OneSidedStream finished");
                Poll::Ready(None)
            }
            Poll::Pending => {
                // println!("OneSidedStream pending");
                Poll::Pending
            }
        }
    }
}

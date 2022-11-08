mod copied_chunks;
use copied_chunks::*;

mod ignore;
// use futures_lite::FutureExt;
use ignore::*;

mod step_by;
use step_by::*;

mod zip;
use zip::*;

mod buffered;
use buffered::*;

use crate::array::LamellarArrayInternalGet;
use crate::array::LamellarArrayRequest;
use crate::memregion::Dist;
use crate::LamellarArray;
use crate::LamellarTeamRT;
use crate::OneSidedMemoryRegion;

use async_trait::async_trait;
// use futures::{ready, Stream};
use pin_project::pin_project;
use std::marker::PhantomData;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
// use std::task::{Context, Poll};

//todo: make an into_stream function
#[async_trait]
// pub trait SerialAsyncIterator {
//     type Item;
//     type ElemType: Dist + 'static;
//     type Array: LamellarArrayInternalGet<Self::ElemType>;
//     async fn async_next(self: Pin<&mut Self>) -> Option<Self::Item>;
// }
pub trait SerialIterator {
    type Item: Send;
    type ElemType: Dist + 'static;
    type Array: LamellarArrayInternalGet<Self::ElemType> + Send;
    fn next(&mut self) -> Option<Self::Item>;
    // async fn async_next(mut self: Pin<&mut Self>) -> Option<Self::Item>;
    fn advance_index(&mut self, count: usize);
    // async fn async_advance_index(mut self: Pin<&mut Self>, count: usize);
    fn array(&self) -> Self::Array;
    fn item_size(&self) -> usize;
    fn buffered_next(
        &mut self,
        mem_region: OneSidedMemoryRegion<u8>,
    ) -> Option<Box<dyn LamellarArrayRequest<Output = ()>>>;
    // async fn async_buffered_next(
    //     mut self: Pin<&mut Self>,
    //     mem_region: OneSidedMemoryRegion<u8>,
    // ) -> Option<Box<dyn LamellarArrayRequest<Output = ()>>>;

    fn from_mem_region(&self, mem_region: OneSidedMemoryRegion<u8>) -> Option<Self::Item>;
    fn copied_chunks(self, chunk_size: usize) -> CopiedChunks<Self>
    where
        Self: Sized + Send,
    {
        CopiedChunks::new(self, chunk_size)
    }
    fn ignore(self, count: usize) -> Ignore<Self>
    where
        Self: Sized + Send,
    {
        Ignore::new(self, count)
    }
    fn step_by(self, step_size: usize) -> StepBy<Self>
    where
        Self: Sized + Send,
    {
        StepBy::new(self, step_size)
    }
    fn zip<I>(self, iter: I) -> Zip<Self, I>
    where
        Self: Sized + Send,
        I: SerialIterator + Sized + Send,
    {
        Zip::new(self, iter)
    }
    fn buffered(self, buf_size: usize) -> Buffered<Self>
    where
        Self: Sized + Send,
    {
        Buffered::new(self, buf_size)
    }
    fn into_iter(self) -> SerialIteratorIter<Self>
    where
        Self: Sized + Send,
    {
        SerialIteratorIter { iter: self }
    }
    // fn into_stream(self) -> SerialIteratorAsyncIter<Self>
    // where
    //     Self: Sized + Send,
    // {
    //     SerialIteratorAsyncIter { iter: self }
    // }
}

pub struct SerialIteratorIter<I> {
    pub(crate) iter: I,
}
impl<I> Iterator for SerialIteratorIter<I>
where
    I: SerialIterator,
{
    type Item = <I as SerialIterator>::Item;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

// #[pin_project]
// pub struct SerialIteratorAsyncIter<I> {
//     #[pin]
//     pub(crate) iter: I,
// }

// impl<I> Stream for SerialIteratorAsyncIter<I>
// where
//     I: SerialIterator + Send,
// {
//     type Item = <I as SerialIterator>::Item;
//     fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         let this = self.project();
//         let res = ready!(this.iter.async_next().poll(cx));
//         Poll::Ready(res)
//     }
// }

struct SendNonNull<T: Dist + 'static>(NonNull<T>);

// This is safe because Lamellar Arrays are allocated from Rofi, and thus cannot be moved
// the pointer will remain valid for the lifetime of the array
unsafe impl<T: Dist + 'static> Send for SendNonNull<T> {}

#[pin_project]
pub struct LamellarArrayIter<'a, T: Dist + 'static, A: LamellarArrayInternalGet<T>> {
    array: A,
    buf_0: OneSidedMemoryRegion<T>,
    // buf_1: OneSidedMemoryRegion<T>,
    index: usize,
    buf_index: usize,
    ptr: SendNonNull<T>,
    _marker: PhantomData<&'a T>,
}

// unsafe impl<'a, T: Dist + 'static, A: LamellarArrayGet<T>> Sync for LamellarArrayIter<'a, T, A> {}
// unsafe impl<'a, T: Dist + 'static, A: LamellarArrayGet<T>> Send for LamellarArrayIter<'a, T, A> {}

impl<'a, T: Dist + 'static, A: LamellarArrayInternalGet<T>> LamellarArrayIter<'a, T, A> {
    pub(crate) fn new(
        array: A,
        team: Pin<Arc<LamellarTeamRT>>,
        buf_size: usize,
    ) -> LamellarArrayIter<'a, T, A> {
        let buf_0 = team.alloc_one_sided_mem_region(buf_size);
        array.internal_get(0, &buf_0).wait();
        let ptr = unsafe { SendNonNull(NonNull::new(buf_0.as_mut_ptr().unwrap()).unwrap()) };
        let iter = LamellarArrayIter {
            array: array,
            buf_0: buf_0,
            // buf_1: team.alloc_one_sided_mem_region(buf_size),
            index: 0,
            buf_index: 0,
            ptr: ptr,
            _marker: PhantomData,
        };
        // iter.fill_buffer(0);

        iter
    }
    // fn fill_buffer(&self, index: usize) {
    //     let end_i = std::cmp::min(index + self.buf_0.len(), self.array.len()) - index;
    //     let buf_0 = self.buf_0.sub_region(..end_i);
    //     let buf_0_u8 = buf_0.clone().to_base::<u8>();
    //     let buf_0_slice = unsafe { buf_0_u8.as_mut_slice().unwrap() };
    //     let buf_1 = self.buf_1.sub_region(..end_i);
    //     let buf_1_u8 = buf_1.clone().to_base::<u8>();
    //     let buf_1_slice = unsafe { buf_1_u8.as_mut_slice().unwrap() };
    //     for i in 0..buf_0_slice.len() {
    //         buf_0_slice[i] = 0;
    //         buf_1_slice[i] = 1;
    //     }
    //     self.array.get(index, &buf_0);
    //     self.array.get(index, &buf_1);
    // }
    // fn spin_for_valid(&self, index: usize) {
    //     let buf_0_temp = self.buf_0.sub_region(index..=index).to_base::<u8>();
    //     let buf_0 = buf_0_temp.as_slice().unwrap();
    //     let buf_1_temp = self.buf_1.sub_region(index..=index).to_base::<u8>();
    //     let buf_1 = buf_1_temp.as_slice().unwrap();
    //     for i in 0..buf_0.len() {
    //         while buf_0[i] != buf_1[i] {
    //             std::thread::yield_now();
    //         }
    //     }
    // }

    // fn check_for_valid(&self, index: usize) -> bool {
    //     let buf_0_temp = self.buf_0.sub_region(index..=index).to_base::<u8>();
    //     let buf_0 = buf_0_temp.as_slice().unwrap();
    //     let buf_1_temp = self.buf_1.sub_region(index..=index).to_base::<u8>();
    //     let buf_1 = buf_1_temp.as_slice().unwrap();
    //     for i in 0..buf_0.len() {
    //         if buf_0[i] != buf_1[i] {
    //             return false;
    //         }
    //     }
    //     true
    // }
}

#[async_trait]
impl<'a, T: Dist + 'static, A: LamellarArrayInternalGet<T> + Clone + Send> SerialIterator
    for LamellarArrayIter<'a, T, A>
{
    type ElemType = T;
    type Item = &'a T;
    type Array = A;
    fn next(&mut self) -> Option<Self::Item> {
        // println!("next {:?} {:?} {:?} {:?}",self.index,self.array.len(),self.buf_index,self.buf_0.len());
        let res = if self.index < self.array.len() {
            if self.buf_index == self.buf_0.len() {
                // println!("need to get new data");
                //need to get new data
                self.buf_index = 0;
                // self.fill_buffer(self.index);
                if self.index + self.buf_0.len() < self.array.len() {
                    self.array.internal_get(self.index, &self.buf_0).wait();
                } else {
                    let sub_region = self.buf_0.sub_region(0..(self.array.len() - self.index));
                    self.array.internal_get(self.index, &sub_region).wait();
                }
            }
            // self.spin_for_valid(self.buf_index);
            self.index += 1;
            self.buf_index += 1;
            unsafe {
                self.ptr
                    .0
                    .as_ptr()
                    .offset(self.buf_index as isize - 1)
                    .as_ref()
            }
        } else {
            None
        };
        res
    }
    // async fn async_next(mut self: Pin<&mut Self>) -> Option<Self::Item> {
    //     let this = self.project();
    //     // println!("async_next serial_iterator");
    //     // println!("nexat {:?} {:?} {:?} {:?}",self.index,self.array.len(),self.buf_index,self.buf_0.len());
    //     let array = this.array.clone();
    //     let res = if *this.index < array.len() {
    //         if *this.buf_index == this.buf_0.len() {
    //             // println!("need to get new data");
    //             //need to get new data
    //             *this.buf_index = 0;
    //             // self.fill_buffer(self.index);
    //             if *this.index + this.buf_0.len() < array.len() {
    //                 let req = array.internal_get(*this.index, &*this.buf_0);
    //                 req.into_future().await;
    //             } else {
    //                 let sub_region = this.buf_0.sub_region(0..(array.len() - *this.index));
    //                 let req = array.internal_get(*this.index, &sub_region);
    //                 req.into_future().await;
    //             }
    //         }
    //         // self.spin_for_valid(self.buf_index);
    //         *this.index += 1;
    //         *this.buf_index += 1;
    //         unsafe {
    //             this.ptr
    //                 .0
    //                 .as_ptr()
    //                 .offset(*this.buf_index as isize - 1)
    //                 .as_ref()
    //         }
    //     } else {
    //         None
    //     };
    //     res
    // }
    fn advance_index(&mut self, count: usize) {
        self.index += count;
        self.buf_index = 0;
        // self.fill_buffer(0);
        self.array.internal_get(self.index, &self.buf_0).wait();
    }
    // async fn async_advance_index(mut self: Pin<&mut Self>, count: usize) {
    //     let this = self.project();
    //     *this.index += count;
    //     *this.buf_index = 0;
    //     // self.fill_buffer(0);
    //     let req = this
    //         .array
    //         .internal_get(*this.index, &*this.buf_0)
    //         .into_future();
    //     req.await;
    // }
    fn array(&self) -> Self::Array {
        self.array.clone()
    }

    fn item_size(&self) -> usize {
        std::mem::size_of::<T>()
    }
    fn buffered_next(
        &mut self,
        mem_region: OneSidedMemoryRegion<u8>,
    ) -> Option<Box<dyn LamellarArrayRequest<Output = ()>>> {
        if self.index < self.array.len() {
            let mem_reg_t = unsafe { mem_region.to_base::<Self::ElemType>() };
            let req = self.array.internal_get(self.index, &mem_reg_t);
            self.index += mem_reg_t.len();
            Some(req)
        } else {
            None
        }
    }
    // async fn async_buffered_next(
    //     mut self: Pin<&mut Self>,
    //     mem_region: OneSidedMemoryRegion<u8>,
    // ) -> Option<Box<dyn LamellarArrayRequest<Output = ()>>> {
    //     if self.index < self.array.len() {
    //         let mem_reg_t = mem_region.to_base::<Self::ElemType>();
    //         let req = self.array.internal_get(self.index, &mem_reg_t);
    //         self.index += mem_reg_t.len();
    //         Some(req)
    //     } else {
    //         None
    //     }
    // }
    fn from_mem_region(&self, mem_region: OneSidedMemoryRegion<u8>) -> Option<Self::Item> {
        unsafe {
            let mem_reg_t = mem_region.to_base::<Self::ElemType>();
            mem_reg_t.as_ptr().unwrap().as_ref()
        }
    }
}

// impl<'a, T: AmDist+ Clone > Iterator
// for LamellarArrayIter<'a, T>
// {
//     type Item = &'a T;
//     fn next(&mut self) -> Option<Self::Item> {
//         <Self as SerialIterator>::next(self)
//     }
// }

// use futures::task::{Context, Poll};
// use futures::Stream;
// use std::pin::Pin;

// impl<'a, T: AmDist+ Clone + Unpin > Stream
// for LamellarArrayIter<'a, T>
// {
// type Item = &'a T;
// fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//     let res = if self.index < self.array.len() {
//         if self.buf_index == self.buf_0.len() {
//             //need to get new data
//             self.buf_index = 0;
//             self.fill_buffer(self.index);
//         }
//         if self.check_for_valid(self.buf_index) {
//             self.index += 1;
//             self.buf_index += 1;
//             Poll::Ready(unsafe {
//                 self.ptr
//                     .as_ptr()
//                     .offset(self.buf_index as isize - 1)
//                     .as_ref()
//             })
//         } else {
//             Poll::Pending
//         }
//     } else {
//         Poll::Ready(None)
//     };
//     res
// }
// }

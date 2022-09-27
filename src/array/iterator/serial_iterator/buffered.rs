use crate::array::iterator::serial_iterator::*;
use crate::array::LamellarArrayRequest;
// use crate::LamellarArray;
// use crate::scheduler::SchedulerQueue;
use crate::LocalMemoryRegion;
use std::collections::VecDeque;
use std::ops::Deref;

use async_trait::async_trait;
// use futures::Future;
use pin_project::pin_project;
#[pin_project]
pub struct Buffered<I>
where
    I: SerialIterator + Send,
{
    #[pin]
    iter: I,
    index: usize,
    buf_index: usize,
    buf_size: usize,
    // buf: LocalMemoryRegion<u8>,
    reqs: VecDeque<
        Option<(
            usize,
            Box<dyn LamellarArrayRequest<Output = ()>>,
            LocalMemoryRegion<u8>,
        )>,
    >,
}

impl<I> Buffered<I>
where
    I: SerialIterator + Send,
{
    pub(crate) fn new(iter: I, buf_size: usize) -> Buffered<I> {
        // let array = iter.array().clone(); //.to_base::<u8>();
        // println!("len: {:?}",array.len());
        // let mem_region = iter.array().team().alloc_local_mem_region(buf_size*iter.item_size());//*iter.array().size_of_elem());
        let mut buf = Buffered {
            iter,
            index: 0,
            buf_index: 0,
            buf_size: buf_size,
            // buf: mem_region,
            reqs: VecDeque::new(),
        };
        for _ in 0..buf.buf_size {
            buf.initiate_buffer();
        }
        buf
    }

    fn initiate_buffer(&mut self) {
        let array = self.iter.array();
        let array_bytes = array.len() * std::mem::size_of::<<Self as SerialIterator>::ElemType>();
        let size = std::cmp::min(self.iter.item_size(), array_bytes - self.buf_index);
        if size > 0 {
            let mem_region = array.team().alloc_local_mem_region(size);
            if let Some(req) = self.iter.buffered_next(mem_region.clone()) {
                self.reqs.push_back(Some((self.buf_index, req, mem_region)));
                self.buf_index += size;
            } else {
                self.reqs.push_back(None);
            }
        }
        // }
    }

    // fn initiate_buffer_async(self: Pin<&mut Self>) {
    //     let mut this = self.project();
    //     let array = this.iter.array();
    //     let array_bytes = array.len() * std::mem::size_of::<<Self as SerialIterator>::ElemType>();
    //     let size = std::cmp::min(this.iter.item_size(), array_bytes - *this.buf_index);
    //     if size > 0 {
    //         let mem_region = array.team().alloc_local_mem_region(size);
    //         if let Some(req) = array
    //             .team()
    //             .scheduler
    //             .block_on(this.iter.as_mut().async_buffered_next(mem_region.clone()))
    //         {
    //             this.reqs
    //                 .push_back(Some((*this.buf_index, req, mem_region)));
    //             *this.buf_index += size;
    //         } else {
    //             this.reqs.push_back(None);
    //         }
    //     }
    //     // }
    // }

    fn wait_on_buffer(&mut self, size: usize) -> Option<LocalMemoryRegion<u8>> {
        let (index, req, mem_region) =
            if let Some((index, req, mem_region)) = self.reqs.pop_front().unwrap() {
                (index, req, mem_region)
            } else {
                return None;
            };
        assert_eq!(mem_region.len(), size);
        assert_eq!(index, self.index);
        req.wait();
        Some(mem_region)
    }

    // fn wait_on_buffer_async(
    //     self: Pin<&mut Self>,
    //     size: usize,
    // ) -> Option<Pin<Box<dyn Future<Output = LocalMemoryRegion<u8>> + Send>>> {
    //     let this = self.project();
    //     let (index, req, mem_region) =
    //         if let Some((index, req, mem_region)) = this.reqs.pop_front().unwrap() {
    //             (index, req.into_future(), mem_region)
    //         } else {
    //             return None;
    //         };
    //     assert_eq!(mem_region.len(), size);
    //     assert_eq!(index, *this.index);
    //     Some(Box::pin(async {
    //         req.await;
    //         mem_region
    //     }))
    // }
}

pub struct BufferedItem<U> {
    item: U,
    _mem_region: LocalMemoryRegion<u8>,
}

impl<U> Deref for BufferedItem<U> {
    type Target = U;
    fn deref(&self) -> &Self::Target {
        &self.item
    }
}

#[async_trait]
// impl<I> SerialAsyncIterator for Buffered<I>
// where
//     I: SerialIterator + SerialAsyncIterator,
// {
//     type ElemType = <I as SerialAsyncIterator>::ElemType;
//     type Item = BufferedItem<<I as SerialAsyncIterator>::Item>;
//     type Array = <I as SerialAsyncIterator>::Array;
//     async fn async_next(self: Pin<&mut Self>) -> Option<Self::Item> {
//         // println!("{:?} {:?}",self.index,self.array.len()/std::mem::size_of::<<Self as SerialIterator>::ElemType>());
//         let array = self.array();
//         let array_bytes = array.len() * std::mem::size_of::<<Self as SerialIterator>::ElemType>();
//         if self.index < array_bytes {
//             let size = std::cmp::min(self.iter.item_size(), array_bytes - self.index);
//             // println!("getting {:?} {:?} {:?} {:?}",self.index,size,self.iter.item_size(),self.buf_index);
//             let mem_region = self.wait_on_buffer_async(size).await?;
//             self.index += size;
//             //if self.index % self.buf_index == 0 {
//             self.initiate_buffer();
//             //}
//             Some(BufferedItem {
//                 item: self.iter.from_mem_region(mem_region.clone())?,
//                 _mem_region: mem_region,
//             })
//         } else {
//             None
//         }
//     }
// }

impl<I> SerialIterator for Buffered<I>
where
    I: SerialIterator + Send,
{
    type ElemType = I::ElemType;
    type Item = BufferedItem<I::Item>;
    type Array = I::Array;
    fn next(&mut self) -> Option<Self::Item> {
        // println!("{:?} {:?}",self.index,self.array.len()/std::mem::size_of::<<Self as SerialIterator>::ElemType>());
        let array = self.array();
        let array_bytes = array.len() * std::mem::size_of::<<Self as SerialIterator>::ElemType>();
        if self.index < array_bytes {
            let size = std::cmp::min(self.iter.item_size(), array_bytes - self.index);
            // println!("getting {:?} {:?} {:?} {:?}",self.index,size,self.iter.item_size(),self.buf_index);
            let mem_region = self.wait_on_buffer(size)?;
            self.index += size;
            //if self.index % self.buf_index == 0 {
            self.initiate_buffer();
            //}
            Some(BufferedItem {
                item: self.iter.from_mem_region(mem_region.clone())?,
                _mem_region: mem_region,
            })
        } else {
            None
        }
    }
    // async fn async_next(mut self: Pin<&mut Self>) -> Option<Self::Item> {
    //     // println!("async_next buffered");
    //     // println!("{:?} {:?}",self.index,self.array.len()/std::mem::size_of::<<Self as SerialIterator>::ElemType>());
    //     // let this = self.as_mut().project();
    //     let array = self.iter.array();
    //     let array_bytes = array.len() * std::mem::size_of::<<Self as SerialIterator>::ElemType>();
    //     if self.index < array_bytes {
    //         let size = std::cmp::min(self.iter.item_size(), array_bytes - self.index);
    //         // println!("getting {:?} {:?} {:?} {:?}",self.index,size,self.iter.item_size(),self.buf_index);
    //         let mem_region = self.as_mut().wait_on_buffer_async(size)?;
    //         let this = self.as_mut().project();
    //         *this.index += size;
    //         //if self.index % self.buf_index == 0 {
    //         self.as_mut().initiate_buffer_async();

    //         let mem_region = mem_region.await;
    //         Some(BufferedItem {
    //             item: self.project().iter.from_mem_region(mem_region.clone())?,
    //             _mem_region: mem_region,
    //         })
    //     } else {
    //         None
    //     }
    // }
    fn advance_index(&mut self, count: usize) {
        // println!("advance_index {:?} {:?} {:?} {:?}",self.index, count, count*self.chunk_size,self.array.len());
        self.iter.advance_index(count);
        // if self.index < self.array.len(){
        //     let size = std::cmp::min(self.chunk_size, self.array.len() - self.index);
        //     self.fill_buffer(0, &self.mem_region.sub_region(..size));
        // }
    }
    // async fn async_advance_index(mut self: Pin<&mut Self>, count: usize) {
    //     self.project().iter.async_advance_index(count).await
    // }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn item_size(&self) -> usize {
        self.iter.item_size()
    }
    //im not actually sure what to do if another buffered iter is called after this one
    fn buffered_next(
        &mut self,
        mem_region: LocalMemoryRegion<u8>,
    ) -> Option<Box<dyn LamellarArrayRequest<Output = ()>>> {
        self.iter.buffered_next(mem_region)
    }

    // async fn async_buffered_next(
    //     mut self: Pin<&mut Self>,
    //     mem_region: LocalMemoryRegion<u8>,
    // ) -> Option<Box<dyn LamellarArrayRequest<Output = ()>>> {
    //     self.project().iter.async_buffered_next(mem_region).await
    // }
    //im not actually sure what to do if another buffered iter is called after this one
    fn from_mem_region(&self, mem_region: LocalMemoryRegion<u8>) -> Option<Self::Item> {
        Some(BufferedItem {
            item: self.iter.from_mem_region(mem_region.clone())?,
            _mem_region: mem_region,
        })
    }
}

// impl<I> Iterator for Buffered<I>
// where
//     I: SerialIterator + Iterator
// {
//     type Item = LocalMemoryRegion<I::ElemType>;
//     fn next(&mut self) -> Option<Self::Item> {
//         <Self as SerialIterator>::next(self)
//     }
// }

// use futures::task::{Context, Poll};
// use futures::Stream;
// use std::pin::Pin;

// impl<I> Stream for Buffered<I>
// where
//     I: SerialIterator + Stream + Unpin
// {
//     type Item = LocalMemoryRegion<I::ElemType>;
//     fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         // println!("{:?} {:?}",self.index,self.array.len()/std::mem::size_of::<<Self as SerialIterator>::ElemType>());
//         println!("async getting {:?} {:?}",self.index,self.chunk_size);
//         if self.index < self.array.len(){
//             let size = std::cmp::min(self.chunk_size, self.array.len() - self.index);
//             // self.fill_buffer(0, &self.mem_region.sub_region(..size));
//             let mem_region: LocalMemoryRegion<I::ElemType> = self.array.team().alloc_local_mem_region(size);
//             self.fill_buffer(101010101, &mem_region);
//             if self.check_for_valid(101010101,&mem_region){
//                 self.index += size;
//                 Poll::Ready(Some(mem_region))
//             }
//             else{
//                 Poll::Pending
//             }
//         }
//         else{
//             Poll::Ready(None)
//         }
//     }
// }

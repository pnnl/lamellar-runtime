use crate::array::iterator::one_sided_iterator::*;
// use crate::array::LamellarArrayRequest;
// use crate::LamellarArray;
use crate::memregion::OneSidedMemoryRegion;
use pin_project::pin_project;

// use async_trait::async_trait;
// use futures::Future;
#[pin_project]
pub struct Chunks<I>
where
    I: OneSidedIterator + Send,
{
    #[pin]
    iter: I,
    index: usize,
    chunk_size: usize,
}

impl<I> Chunks<I>
where
    I: OneSidedIterator + Send,
{
    pub(crate) fn new(iter: I, chunk_size: usize) -> Chunks<I> {
        // let array = iter.array().clone(); //.to_base::<u8>();
        // println!("len: {:?}",array.len());
        // let mem_region = iter.array().team().alloc_one_sided_mem_region(chunk_size);//*iter.array().size_of_elem());
        let chunks = Chunks {
            iter,
            // array,
            // mem_region: mem_region.clone(),
            index: 0,
            chunk_size,
        };
        // chunks.fill_buffer(0,&mem_region);
        chunks
    }

    fn get_buffer(&self, size: usize) -> OneSidedMemoryRegion<<I as OneSidedIterator>::ElemType> {
        let mem_region: OneSidedMemoryRegion<<I as OneSidedIterator>::ElemType> =
            self.array().team().alloc_one_sided_mem_region(size);
        // potentially unsafe depending on the array type (i.e. UnsafeArray - which requries unsafe to construct an iterator), 
        // but safe with respect to the mem_region as this is the only reference
        unsafe {self.array().internal_get(self.index, &mem_region).wait();}
        mem_region
    }

}

impl<I> OneSidedIterator for Chunks<I>
where
    I: OneSidedIterator + Send,
{
    type ElemType = I::ElemType;
    type Item = OneSidedMemoryRegion<I::ElemType>;
    type Array = I::Array;
    fn next(&mut self) -> Option<Self::Item> {
        // println!("{:?} {:?}",self.index,self.array.len()/std::mem::size_of::<<Self as OneSidedIterator>::ElemType>());
        let array = self.array();
        if self.index < array.len() {
            let size = std::cmp::min(self.chunk_size, array.len() - self.index);

            let mem_region = self.get_buffer(size);
            self.index += size;
            Some(mem_region)
        } else {
            None
        }
    }

    fn advance_index(&mut self, count: usize) {
        // println!("advance_index {:?} {:?} {:?} {:?}",self.index, count, count*self.chunk_size,self.array.len());
        self.index += count * self.chunk_size;
    }

    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn item_size(&self) -> usize {
        self.chunk_size * std::mem::size_of::<I::ElemType>()
    }
    // fn buffered_next(
    //     &mut self,
    //     mem_region: OneSidedMemoryRegion<u8>,
    // ) -> Option<Box<dyn LamellarArrayRequest<Output = ()>>> {
    //     let array = self.array();
    //     if self.index < array.len() {
    //         let mem_reg_t = unsafe { mem_region.to_base::<I::ElemType>() };
    //         let req = array.internal_get(self.index, &mem_reg_t);
    //         self.index += mem_reg_t.len();
    //         Some(req)
    //     } else {
    //         None
    //     }
    // }
    // fn from_mem_region(&self, mem_region: OneSidedMemoryRegion<u8>) -> Option<Self::Item> {
    //     let mem_reg_t = unsafe { mem_region.to_base::<I::ElemType>() };
    //     Some(mem_reg_t)
    // }
}

// impl<I> Iterator for Chunks<I>
// where
//     I: OneSidedIterator + Iterator
// {
//     type Item = OneSidedMemoryRegion<I::ElemType>;
//     fn next(&mut self) -> Option<Self::Item> {
//         <Self as OneSidedIterator>::next(self)
//     }
// }

// use futures::task::{Context, Poll};
// use futures::Stream;
// use std::pin::Pin;

// impl<I> Stream for Chunks<I>
// where
//     I: OneSidedIterator + Stream + Unpin
// {
//     type Item = OneSidedMemoryRegion<I::ElemType>;
//     fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         // println!("{:?} {:?}",self.index,self.array.len()/std::mem::size_of::<<Self as OneSidedIterator>::ElemType>());
//         println!("async getting {:?} {:?}",self.index,self.chunk_size);
//         if self.index < self.array.len(){
//             let size = std::cmp::min(self.chunk_size, self.array.len() - self.index);
//             // self.fill_buffer(0, &self.mem_region.sub_region(..size));
//             let mem_region: OneSidedMemoryRegion<I::ElemType> = self.array.team().alloc_one_sided_mem_region(size);
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

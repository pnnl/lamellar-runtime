use crate::array::iterator::serial_iterator::*;
use crate::array::LamellarArrayRequest;
// use crate::LamellarArray;
use crate::LocalMemoryRegion;

pub struct CopiedChunks<I>
where
    I: SerialIterator,
{
    iter: I,
    // array: LamellarArray<I::ElemType>,
    // mem_region: LocalMemoryRegion<I::ElemType>,
    index: usize,
    chunk_size: usize,
}

impl<I> CopiedChunks<I>
where
    I: SerialIterator,
{
    pub(crate) fn new(iter: I, chunk_size: usize) -> CopiedChunks<I> {
        // let array = iter.array().clone(); //.to_base::<u8>();
        // println!("len: {:?}",array.len());
        // let mem_region = iter.array().team().alloc_local_mem_region(chunk_size);//*iter.array().size_of_elem());
        let chunks = CopiedChunks {
            iter,
            // array,
            // mem_region: mem_region.clone(),
            index: 0,
            chunk_size,
        };
        // chunks.fill_buffer(0,&mem_region);
        chunks
    }

    fn get_buffer(&self, size: usize) -> LocalMemoryRegion<I::ElemType> {
        let mem_region: LocalMemoryRegion<I::ElemType> =
            self.array().team().alloc_local_mem_region(size);
        self.array().get(self.index, &mem_region).wait();
        mem_region
    }
}

impl<I> SerialIterator for CopiedChunks<I>
where
    I: SerialIterator,
{
    type ElemType = I::ElemType;
    type Item = LocalMemoryRegion<I::ElemType>;
    type Array = I::Array;
    fn next(&mut self) -> Option<Self::Item> {
        // println!("{:?} {:?}",self.index,self.array.len()/std::mem::size_of::<<Self as SerialIterator>::ElemType>());
        let array = self.array();
        if self.index < array.len() {
            let size = std::cmp::min(self.chunk_size, array.len() - self.index);

            let mem_region = self.get_buffer( size);
            self.index += size;
            Some(mem_region)
        } else {
            None
        }
    }
    fn advance_index(&mut self, count: usize) {
        // println!("advance_index {:?} {:?} {:?} {:?}",self.index, count, count*self.chunk_size,self.array.len());
        self.index += count * self.chunk_size;
        // if self.index < self.array.len(){
        //     let size = std::cmp::min(self.chunk_size, self.array.len() - self.index);
        //     self.fill_buffer(0, &self.mem_region.sub_region(..size));
        // }
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn item_size(&self) -> usize {
        self.chunk_size * std::mem::size_of::<I::ElemType>()
    }
    fn buffered_next(&mut self, mem_region: LocalMemoryRegion<u8>) -> Option<Box<dyn LamellarArrayRequest<Output = ()> + Send + Sync>>{
        let array = self.array();
        if self.index < array.len() {
            let mem_reg_t = mem_region.to_base::<I::ElemType>();
            let req  = array.get(self.index, &mem_reg_t);
            self.index += mem_reg_t.len();
            Some(req)
        } else {
            None
        }
    }
    fn from_mem_region(&self, mem_region: LocalMemoryRegion<u8>) -> Option<Self::Item>{
        unsafe {
            let mem_reg_t = mem_region.to_base::<I::ElemType>();
            Some(mem_reg_t)
        }
    }
}

// impl<I> Iterator for CopiedChunks<I>
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

// impl<I> Stream for CopiedChunks<I>
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

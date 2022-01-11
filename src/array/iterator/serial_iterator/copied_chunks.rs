use crate::array::iterator::serial_iterator::*;
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

    fn get_buffer(&self, _val: u32, size: usize) -> LocalMemoryRegion<I::ElemType> {
        let mem_region: LocalMemoryRegion<I::ElemType> =
            self.array().team().alloc_local_mem_region(size);
        // let buf_u8 = mem_region.clone().to_base::<u32>();
        // let buf_slice = unsafe { buf_u8.as_mut_slice().unwrap() };
        // println!("buf_u8 len {:}",buf_slice.len());
        // for i in 0..buf_slice.len() {
        //     buf_slice[i] = val;
        // }
        self.array().iget(self.index, &mem_region);
        // }

        // fn spin_for_valid(&self, val: u32, buf: &LocalMemoryRegion<I::ElemType>) {
        // let buf_0_temp = self.mem_region.clone().to_base::<u8>();
        // let buf_0 = buf_0_temp.as_slice().unwrap();
        // let buf_1_temp = buf.clone().to_base::<u32>();
        // let buf_1 = buf_1_temp.as_slice().unwrap();

        // let mut start = std::time::Instant::now();
        // for i in 0..buf_slice.len() {
        //     // while buf_0[i] != buf_1[i] {
        //     while buf_slice[i] == val {
        //         std::thread::yield_now();
        //         if start.elapsed().as_secs_f64() > 5.0 {
        //             println!("i: {:?} {:?} {:?}", i, val, buf_slice[i]);
        //             start = std::time::Instant::now();
        //         }
        //     }
        // }
        mem_region
    }
    // fn check_for_valid(&self,val: u32, buf: &LocalMemoryRegion<I::ElemType>) -> bool {
    //     let buf_1_temp = buf.clone().to_base::<u32>();
    //     let buf_1 = buf_1_temp.as_slice().unwrap();
    //     for i in 0..buf_1.len() {
    //         // while buf_0[i] != buf_1[i] {
    //         if buf_1[i] == val{
    //             return false;
    //         }
    //     }
    //     true
    // }
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
            // self.fill_buffer(0, &self.mem_region.sub_region(..size));
            // println!("getting {:?} {:?}",self.index,self.chunk_size);

            let mem_region = self.get_buffer(101010101, size);

            // self.spin_for_valid(101010101, &mem_region);
            self.index += size;
            // if self.index < self.array.len(){
            //     let size = std::cmp::min(self.chunk_size, self.array.len() - self.index);
            //     self.fill_buffer(0, &self.mem_region.sub_region(..size));
            // }
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

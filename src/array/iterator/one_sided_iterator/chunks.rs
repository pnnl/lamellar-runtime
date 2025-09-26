use crate::array::iterator::one_sided_iterator::{private::*, *};
use crate::array::LamellarRdmaGet;

use pin_project::pin_project;

#[pin_project]
pub struct Chunks<I>
where
    I: OneSidedIterator + Send,
{
    #[pin]
    iter: I,
    index: usize,
    chunk_size: usize,
    #[pin]
    state: ChunkState<I::ElemType>,
}

#[pin_project(project = ChunkStateProj)]
enum ChunkState<E: Dist> {
    Pending(#[pin] LamellarTask<Vec<E>>),
    Finished,
}

impl<I> Chunks<I>
where
    I: OneSidedIterator + Send,
{
    pub(crate) fn new(iter: I, chunk_size: usize) -> Chunks<I> {
        // let array = iter.array().clone(); //.to_base::<u8>();
        // println!(" Chunks size: {:?}", chunk_size);

        let chunks = Chunks {
            iter,
            index: 0,
            chunk_size,
            state: ChunkState::Finished,
        };
        chunks
    }
}

impl<I> OneSidedIterator for Chunks<I> where I: OneSidedIterator + Send {}

impl<I> OneSidedIteratorInner for Chunks<I>
where
    I: OneSidedIterator + Send,
{
    type ElemType = I::ElemType;
    type Item = Vec<I::ElemType>;
    type Array = I::Array;

    fn init(&mut self) {
        let array = self.array();
        let size = std::cmp::min(self.chunk_size, array.len() - self.index);
        let new_req = unsafe { array.get_buffer(self.index, size).spawn() };
        self.index += size;
        self.state = ChunkState::Pending(new_req);
    }
    fn next(&mut self) -> Option<Self::Item> {
        let array = self.array();
        let mut cur_state = ChunkState::Finished;
        std::mem::swap(&mut self.state, &mut cur_state);
        match cur_state {
            ChunkState::Pending(req) => {
                // println!("next: index: {:?}", self.index);
                if self.index < array.len() {
                    //prefetch
                    let size = std::cmp::min(self.chunk_size, array.len() - self.index);
                    // println!("prefectching: index: {:?} {:?}", self.index, size);
                    let new_req = unsafe { array.get_buffer(self.index, size).spawn() };
                    self.index += size;
                    self.state = ChunkState::Pending(new_req);
                } else {
                    self.state = ChunkState::Finished;
                }
                Some(req.block())
            }
            ChunkState::Finished => None,
        }
    }

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let array = self.array();
        let mut this = self.as_mut().project();
        // let mut cur_state = ChunkState::Finished;

        // std::mem::swap(&mut *this.state, &mut cur_state);

        match this.state.as_mut().project() {
            ChunkStateProj::Pending(req) => match req.poll(cx) {
                Poll::Ready(mem_region) => {
                    if *this.index < array.len() {
                        //prefetch
                        let size = std::cmp::min(*this.chunk_size, array.len() - *this.index);
                        // println!("prefectching: index: {:?} {:?}", this.index, size);
                        let new_req = unsafe { array.get_buffer(*this.index, size).spawn() };
                        *this.index += size;
                        *this.state = ChunkState::Pending(new_req);
                    } else {
                        *this.state = ChunkState::Finished;
                    }
                    Poll::Ready(Some(mem_region))
                }
                Poll::Pending => Poll::Pending,
            },
            ChunkStateProj::Finished => Poll::Ready(None),
        }
    }

    fn advance_index(&mut self, count: usize) {
        // println!("advance_index {:?} {:?} {:?} {:?}",self.index, count, count*self.chunk_size,self.array.len());
        self.index += count * self.chunk_size;
    }

    fn advance_index_pin(self: Pin<&mut Self>, count: usize) {
        // println!(
        //     "advance_index_pin {:?} {:?} {:?}",
        //     self.index,
        //     count,
        //     count * self.chunk_size,
        // );
        let this = self.project();
        *this.index += count * *this.chunk_size;
        // println!(
        //     "after advance_index_pin {:?} {:?} {:?} ",
        //     *this.index,
        //     count,
        //     count * *this.chunk_size,
        // );
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
    // ) -> Option<ArrayRdmaHandle> {
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

// use futures_util::task::{Context, Poll};
// use futures_util::Stream;
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

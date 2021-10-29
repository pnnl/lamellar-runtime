


mod copied_chunks;
use copied_chunks::*;

mod ignore;
use ignore::*;

use crate::memregion::Dist;
use crate::LamellarTeamRT;
use crate::LamellarArray;
use crate::LocalMemoryRegion;

use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::Arc;

pub trait SerialIterator {
    type ElemType: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static;
    fn advance_index(&mut self, count: usize);
    fn set_index(&mut self,index: usize);
    fn get_index(&self) -> usize;
    fn array(&self) -> LamellarArray<Self::ElemType>;
    fn copied_chunks(self, chunk_size: usize) -> CopiedChunks<Self> where Self: Sized {
        CopiedChunks::new(self,chunk_size)
    }
    fn ignore<I: SerialIterator>(self,count: usize) -> Ignore<Self>  where Self: Sized {
        Ignore::new(self,count)
    }
}

pub struct LamellarArrayIter<
'a,
T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static,
> {
    array: LamellarArray<T>,
    buf_0: LocalMemoryRegion<T>,
    buf_1: LocalMemoryRegion<T>,
    index: usize,
    buf_index: usize,
    ptr: NonNull<T>,
    _marker: PhantomData<&'a T>,
}

impl<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static>
LamellarArrayIter<'a, T>
{
    pub(crate) fn new(
        array: LamellarArray<T>,
        team: Arc<LamellarTeamRT>,
        buf_size: usize,
    ) -> LamellarArrayIter<'a, T> {
        let buf_0 = team.alloc_local_mem_region(buf_size);
        let ptr = NonNull::new(buf_0.as_mut_ptr().unwrap()).unwrap();
        let iter = LamellarArrayIter {
            array: array,
            buf_0: buf_0,
            buf_1: team.alloc_local_mem_region(buf_size),
            index: 0,
            buf_index: 0,
            ptr: ptr,
            _marker: PhantomData,
        };
        iter.fill_buffer(0);
        iter
    }
    fn fill_buffer(&self, index: usize) {
        let end_i = std::cmp::min(index + self.buf_0.len(), self.array.len()) - index;
        let buf_0 = self.buf_0.sub_region(..end_i);
        let buf_0_u8 = buf_0.clone().to_base::<u8>();
        let buf_0_slice = unsafe { buf_0_u8.as_mut_slice().unwrap() };
        let buf_1 = self.buf_1.sub_region(..end_i);
        let buf_1_u8 = buf_1.clone().to_base::<u8>();
        let buf_1_slice = unsafe { buf_1_u8.as_mut_slice().unwrap() };
        for i in 0..buf_0_slice.len() {
            buf_0_slice[i] = 0;
            buf_1_slice[i] = 1;
        }
        self.array.get(index, &buf_0);
        self.array.get(index, &buf_1);
    }
    fn spin_for_valid(&self, index: usize) {
        let buf_0_temp = self.buf_0.sub_region(index..=index).to_base::<u8>();
        let buf_0 = buf_0_temp.as_slice().unwrap();
        let buf_1_temp = self.buf_1.sub_region(index..=index).to_base::<u8>();
        let buf_1 = buf_1_temp.as_slice().unwrap();
        for i in 0..buf_0.len() {
            while buf_0[i] != buf_1[i] {
                std::thread::yield_now();
            }
        }
    }

    fn check_for_valid(&self, index: usize) -> bool {
        let buf_0_temp = self.buf_0.sub_region(index..=index).to_base::<u8>();
        let buf_0 = buf_0_temp.as_slice().unwrap();
        let buf_1_temp = self.buf_1.sub_region(index..=index).to_base::<u8>();
        let buf_1 = buf_1_temp.as_slice().unwrap();
        for i in 0..buf_0.len() {
            if buf_0[i] != buf_1[i] {
                return false;
            }
        }
        true
    }
}

impl<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> SerialIterator for
LamellarArrayIter<'a, T> {
    type ElemType = T;
    fn advance_index(&mut self, count: usize){
        self.index += count;
        self.buf_index = self.index;
        self.fill_buffer(0);
    }
    fn set_index(&mut self, index: usize){
        self.index= index;
        self.buf_index = index;
        self.fill_buffer(0);
    }
    fn get_index(&self) -> usize {
        self.index
    }
    fn array(&self) -> LamellarArray<T> {
        self.array.clone()
    }
}



impl<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> Iterator
for LamellarArrayIter<'a, T>
{
type Item = &'a T;
fn next(&mut self) -> Option<Self::Item> {
    let res = if self.index < self.array.len() {
        if self.buf_index == self.buf_0.len() {
            //need to get new data
            self.buf_index = 0;
            self.fill_buffer(self.index);
        }
        self.spin_for_valid(self.buf_index);
        self.index += 1;
        self.buf_index += 1;
        unsafe {
            self.ptr
                .as_ptr()
                .offset(self.buf_index as isize - 1)
                .as_ref()
        }
    } else {
        None
    };
    res
}
}

use futures::task::{Context, Poll};
use futures::Stream;
use std::pin::Pin;

impl<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + Unpin + 'static> Stream
for LamellarArrayIter<'a, T>
{
type Item = &'a T;
fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let res = if self.index < self.array.len() {
        if self.buf_index == self.buf_0.len() {
            //need to get new data
            self.buf_index = 0;
            self.fill_buffer(self.index);
        }
        if self.check_for_valid(self.buf_index) {
            self.index += 1;
            self.buf_index += 1;
            Poll::Ready(unsafe {
                self.ptr
                    .as_ptr()
                    .offset(self.buf_index as isize - 1)
                    .as_ref()
            })
        } else {
            Poll::Pending
        }
    } else {
        Poll::Ready(None)
    };
    res
}
}


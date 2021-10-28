
mod chunks;
mod enumerate;

use chunks::*;
use enumerate::*;

use crate::memregion::Dist;
use crate::LamellarArray;

use std::marker::PhantomData;
use futures::Future;

pub trait DistIteratorLauncher {
    fn for_each<I, F>(&self, iter: &I, op: F, chunk_size: usize)
    //this really needs to return a task group handle...
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + Sync + Send + Clone + 'static;
    fn for_each_async<I, F, Fut>(&self, iter: &I, op: F, chunk_size: usize)
    //this really needs to return a task group handle...
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + 'static;

    fn global_index_from_local(&self, index: usize, chunk_size: usize) -> usize;
}

pub trait DistributedIterator: Sync + Send + Clone {
    type Item: Sync + Send;
    type Array: DistIteratorLauncher;
    fn init(&self, start_i: usize, end_i: usize) -> Self;
    fn array(&self) -> Self::Array;
    fn next(&mut self) -> Option<Self::Item>;
    
    fn enumerate(self) -> Enumerate<Self> {
        Enumerate::new(self, 0)
    }
    fn chunks(self,size: usize) -> Chunks<Self>{
        Chunks::new(self,0,0,size)
    }
}

pub trait DistIterChunkSize{
    fn chunk_size(&self) -> usize;
}

#[derive(Clone)]
pub struct DistIter<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> {
    data: LamellarArray<T>,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'a T>,
}

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> DistIter<'_, T> {
    pub(crate) fn new(data: LamellarArray<T>, cur_i: usize, end_i: usize,) -> Self {
        DistIter { data, cur_i, end_i, _marker: PhantomData }
    }
}

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> DistIter<'static, T> {
    pub fn for_each<F>(&self, op: F)
    where
        F: Fn(&T) + Sync + Send + Clone + 'static,
    {
        self.data.clone().for_each(self, op, self.chunk_size());
    }
    pub fn for_each_async<F, Fut>(&self, op: F)
    where
        F: Fn(&T) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + 'static,
    {
        self.data.clone().for_each_async(self, op, self.chunk_size());
    }
}

impl<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'a> DistIterChunkSize
for DistIter<'a, T>{
    fn chunk_size(&self) -> usize {
        1
    }
}
impl<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'a> DistributedIterator
    for DistIter<'a, T>
{
    type Item = &'a T;
    type Array = LamellarArray<T>;
    fn init(&self, start_i: usize, end_i: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("dist iter init {:?} {:?} {:?}",start_i,end_i,max_i);
        DistIter {
            data: self.data.clone(),
            cur_i: std::cmp::min(start_i,max_i),
            end_i: std::cmp::min(end_i,max_i),
            _marker: PhantomData,
        }
    }
    fn array(&self) -> Self::Array {
        self.data.clone()
    }
    fn next(&mut self) -> Option<Self::Item> {
        // println!("cur: {:?} end {:?}",self.cur_i,self.end_i);
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
}

#[derive(Clone)]
pub struct DistIterMut<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static>
{
    data: LamellarArray<T>,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'a T>,
}

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> DistIterMut<'_, T> {
    pub(crate) fn new(data: LamellarArray<T>, cur_i: usize, end_i: usize,) -> Self {
        DistIterMut { data, cur_i, end_i, _marker: PhantomData }
    }
}

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static>
    DistIterMut<'static, T>
{
    pub fn for_each<F>(&self, op: F)
    where
        F: Fn(&mut T) + Sync + Send + Clone + 'static,
    {
        self.data.clone().for_each(self, op, self.chunk_size());
    }
    pub fn for_each_async<F, Fut>(&self, op: F)
    where
        F: Fn(&mut T) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + 'static,
    {
        self.data.clone().for_each_async(self, op, self.chunk_size());
    }
}
impl<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'a> DistIterChunkSize
for DistIterMut<'a, T>{
    fn chunk_size(&self) -> usize {
        1
    }
}
impl<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'a> DistributedIterator
    for DistIterMut<'a, T>
{
    type Item = &'a mut T;
    type Array = LamellarArray<T>;
    fn init(&self, start_i: usize, end_i: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("dist iter init {:?} {:?} {:?}",start_i,end_i,max_i);
        DistIterMut {
            data: self.data.clone(),
            cur_i: std::cmp::min(start_i,max_i),
            end_i: std::cmp::min(end_i,max_i),
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
}
mod chunks;
mod enumerate;
mod ignore;
mod step_by;
mod take;

use chunks::*;
use enumerate::*;
use ignore::*;
use step_by::*;
use take::*;

use crate::memregion::Dist;
use crate::LamellarArray;

use futures::Future;
use std::marker::PhantomData;

pub trait DistIteratorLauncher {
    fn for_each<I, F>(&self, iter: &I, op: F)
    //this really needs to return a task group handle...
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + Sync + Send + Clone + 'static;
    fn for_each_async<I, F, Fut>(&self, iter: &I, op: F)
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
    fn init(&self, start_i: usize, cnt: usize) -> Self;
    fn array(&self) -> Self::Array;
    fn next(&mut self) -> Option<Self::Item>;
    fn elems(&self, in_elems: usize) -> usize;
    fn global_index(&self, index: usize) -> usize;
    fn chunk_size(&self) -> usize;
    fn advance_index(&mut self, count: usize);

    fn enumerate(self) -> Enumerate<Self> {
        Enumerate::new(self, 0)
    }
    fn chunks(self, size: usize) -> Chunks<Self> {
        Chunks::new(self, 0, 0, size)
    }
    fn ignore(self, count: usize) -> Ignore<Self> {
        Ignore::new(self, count)
    }
    fn step_by(self, step_size: usize) -> StepBy<Self> {
        StepBy::new(self, step_size)
    }
    fn take(self, count: usize) -> Take<Self> {
        Take::new(self, count)
    }
}

#[derive(Clone)]
pub struct DistIter<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> {
    data: LamellarArray<T>,
    cur_i: usize,
    end_i: usize,
    _marker: PhantomData<&'a T>,
}

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> DistIter<'_, T> {
    pub(crate) fn new(data: LamellarArray<T>, cur_i: usize, cnt: usize) -> Self {
        // println!("new dist iter {:?} {:? } {:?}",cur_i, cnt, cur_i+cnt);
        DistIter {
            data,
            cur_i,
            end_i: cur_i + cnt,
            _marker: PhantomData,
        }
    }
}

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static> DistIter<'static, T> {
    pub fn for_each<F>(&self, op: F)
    where
        F: Fn(&T) + Sync + Send + Clone + 'static,
    {
        self.data.clone().for_each(self, op);
    }
    pub fn for_each_async<F, Fut>(&self, op: F)
    where
        F: Fn(&T) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + 'static,
    {
        self.data.clone().for_each_async(self, op);
    }
}

impl<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'a> DistributedIterator
    for DistIter<'a, T>
{
    type Item = &'a T;
    type Array = LamellarArray<T>;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
        DistIter {
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
        // println!("dist iter next cur: {:?} end {:?}",self.cur_i,self.end_i);
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
    fn elems(&self, in_elems: usize) -> usize {
        // println!("dist iter elems {:?}",in_elems);
        in_elems
    }
    fn global_index(&self, index: usize) -> usize {
        let g_index = self.data.global_index_from_local(index, 1);
        // println!("dist_iter index: {:?} global_index {:?}", index,g_index);
        g_index
    }
    fn chunk_size(&self) -> usize {
        1
    }
    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
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
    pub(crate) fn new(data: LamellarArray<T>, cur_i: usize, cnt: usize) -> Self {
        DistIterMut {
            data,
            cur_i,
            end_i: cur_i + cnt,
            _marker: PhantomData,
        }
    }
}

impl<T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'static>
    DistIterMut<'static, T>
{
    pub fn for_each<F>(&self, op: F)
    where
        F: Fn(&mut T) + Sync + Send + Clone + 'static,
    {
        self.data.clone().for_each(self, op);
    }
    pub fn for_each_async<F, Fut>(&self, op: F)
    where
        F: Fn(&mut T) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + 'static,
    {
        self.data.clone().for_each_async(self, op);
    }
}
impl<'a, T: Dist + serde::ser::Serialize + serde::de::DeserializeOwned + 'a> DistributedIterator
    for DistIterMut<'a, T>
{
    type Item = &'a mut T;
    type Array = LamellarArray<T>;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("dist iter init {:?} {:?} {:?}",start_i,end_i,max_i);
        DistIterMut {
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
    fn global_index(&self, index: usize) -> usize {
        let g_index = self.data.global_index_from_local(index, 1);
        // println!("dist_iter index: {:?} global_index {:?}", index,g_index);
        g_index
    }
    fn chunk_size(&self) -> usize {
        1
    }
    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

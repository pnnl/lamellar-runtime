use crate::array::atomic::*;
use crate::array::atomic::operations::OPS;

use crate::array::iterator::distributed_iterator::{
    DistIter, DistIterMut, DistIteratorLauncher, DistributedIterator, ForEach, ForEachAsync,
};
use crate::array::iterator::serial_iterator::LamellarArrayIter;
use crate::array::*;
use crate::memregion::Dist;

#[derive(Clone)]
pub struct AtomicDistIter< T: Dist> { //dont need a AtomicDistIterMut in this case as any updates to inner elements are atomic
    data: AtomicArray<T>,
    cur_i: usize,
    end_i: usize,
}

impl<T: Dist> AtomicDistIter<T> {
    pub(crate) fn new(data: AtomicArray<T>, cur_i: usize, cnt: usize) -> Self {
        // println!("new dist iter {:?} {:? } {:?}",cur_i, cnt, cur_i+cnt);
        AtomicDistIter {
            data,
            cur_i,
            end_i: cur_i + cnt,
        }
    }
}
impl<T: Dist + 'static> AtomicDistIter<T> {
    pub fn for_each<F>(self, op: F)
    where
        F: Fn(AtomicElement<T>) + Sync + Send + Clone + 'static,
    {
        self.data.clone().for_each(self, op);
    }
    pub fn for_each_async<F, Fut>(&self, op: F)
    where
        F: Fn(AtomicElement<T>) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + Clone + 'static,
    {
        self.data.clone().for_each_async(self, op);
    }
}

impl<T: Dist > DistributedIterator for AtomicDistIter<T> {
    type Item = AtomicElement<T>;
    type Array = AtomicArray<T>;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let max_i = self.data.num_elems_local();
        // println!("init dist iter start_i: {:?} cnt {:?} end_i: {:?} max_i: {:?}",start_i,cnt, start_i+cnt,max_i);
        AtomicDistIter {
            data: self.data.clone(),
            cur_i: std::cmp::min(start_i, max_i),
            end_i: std::cmp::min(start_i + cnt, max_i)
        }
    }
    fn array(&self) -> Self::Array {
        self.data.clone()
    }
    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_i < self.end_i {
            self.cur_i += 1;
            Some(AtomicElement{
                array:self.data.clone(),
                local_index: self.cur_i - 1,
            })
        } else {
            None
        }
    }
    fn elems(&self, in_elems: usize) -> usize {
        in_elems
    }
    fn global_index(&self, index: usize) -> usize {
        let g_index = self.data.global_index_from_local(index, 1);
        g_index
    }
    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

impl<T: Dist> AtomicArray<T> {
    pub fn dist_iter(&self) -> AtomicDistIter<T> {
        AtomicDistIter::new(self.clone(), 0, 0)
    }

    pub fn dist_iter_mut(&self) -> AtomicDistIter<T> {
        AtomicDistIter::new(self.clone(), 0, 0)
    }

    pub fn ser_iter(&self) -> LamellarArrayIter<'_, T, AtomicArray<T>> {
        LamellarArrayIter::new(self.clone().into(), self.array.team().clone(), 1)
    }

    pub fn buffered_iter(&self, buf_size: usize) -> LamellarArrayIter<'_, T, AtomicArray<T>> {
        LamellarArrayIter::new(
            self.clone().into(),
            self.array.team().clone(),
            std::cmp::min(buf_size, self.len()),
        )
    }
}

impl<T: Dist> DistIteratorLauncher for AtomicArray<T> {
    fn global_index_from_local(&self, index: usize, chunk_size: usize) -> usize {
        self.array.global_index_from_local(index, chunk_size)
    }

    fn for_each<I, F>(&self, iter: I, op: F)
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + Sync + Send + Clone + 'static,
    {
        self.array.for_each(iter, op)
    }
    fn for_each_async<I, F, Fut>(&self, iter: &I, op: F)
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + Clone + 'static,
    {
        self.array.for_each_async(iter, op)
    }
}

pub struct AtomicElement<T: Dist>{
    array: AtomicArray<T>,
    local_index: usize,
}

impl<T: Dist + 'static> AtomicElement<T> {
    fn local_op(&self, val: T, op: ArrayOpCmd) -> T{
        if let Some(funcs) = OPS.get(&(op,TypeId::of::<T>())) {
            let mut val = val;
            let array: AtomicArray<u8> = unsafe { self.array.as_bytes() };
            funcs.1(&mut val as *mut T as *mut u8, array, self.local_index);
            val
        }
        else{
            panic!("type has not been registered");
        }
    }
} 

impl<T:ElementOps + 'static> AtomicElement<T>{
    pub fn load(&self) -> T {
        self.local_op(unsafe{self.array.local_as_slice()[0]},ArrayOpCmd::Load)
    }

    pub fn store(&self, val: T) {
        self.local_op(val,ArrayOpCmd::Store);
    }

    pub fn swap(&self, val: T)  -> T {
        self.local_op(val,ArrayOpCmd::Swap)
    }
}


impl<T:ElementArithmeticOps + 'static> AtomicElement<T>{
    pub fn add(
        &self,
        val: T,
    ) {
        self.local_op(val,ArrayOpCmd::Add);
    }
    pub fn fetch_add(
        &self,
        val: T,
    ) -> T {
        self.local_op(val,ArrayOpCmd::FetchAdd)
    }
    pub fn sub(
        &self,
        val: T,
    )  {
        self.local_op(val,ArrayOpCmd::Sub);
    }
    pub fn fetch_sub(
        &self,
        val: T,
    ) -> T {
        self.local_op(val,ArrayOpCmd::FetchSub)
    }
    pub fn mul(
        &self,
        val: T,
    )  {
        self.local_op(val,ArrayOpCmd::Mul);
    }
    pub fn fetch_mul(
        &self,
        val: T,
    ) -> T {
        self.local_op(val,ArrayOpCmd::FetchMul)
    }
    pub fn div(
        &self,
        val: T,
    )  {
        self.local_op(val,ArrayOpCmd::Div);
    }
    pub fn fetch_div(
        &self,
        val: T,
    ) -> T {
        self.local_op(val,ArrayOpCmd::FetchDiv)
    }
}

impl<T:  ElementBitWiseOps + 'static> AtomicElement<T> {
    pub fn bit_and(
        &self,
        val: T,
    )  {
        self.local_op(val,ArrayOpCmd::And);
    }
    pub fn fetch_bit_and(
        &self,
        val: T,
    ) -> T {
        self.local_op(val,ArrayOpCmd::FetchAnd)
    }
    pub fn bit_or(
        &self,
        val: T,
    ) {
        self.local_op(val,ArrayOpCmd::Or);
    }
    pub fn fetch_bit_or(
        &self,
        val: T,
    ) -> T {
        self.local_op(val,ArrayOpCmd::FetchOr)
    }
}


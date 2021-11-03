use crate::array::iterator::distributed_iterator::*;

use futures::Future;

//ignores the first n elements of iterator I per pe (this implys that n * num_pes elements are ignored in total)
#[derive(Clone)]
pub struct Ignore<I> {
    iter: I,
    count: usize,
}

impl<I> Ignore<I>
where
    I: DistributedIterator,
{
    pub(crate) fn new(iter: I, count: usize) -> Ignore<I> {
        // println!("new Ignore {:?} ",count);
        Ignore { iter, count }
    }
}

impl<I> Ignore<I>
where
    I: DistributedIterator + 'static,
{
    pub fn for_each<F>(&self, op: F)
    where
        F: Fn(<I as DistributedIterator>::Item) + Sync + Send + Clone + 'static,
    {
        self.iter.array().for_each(self, op);
    }
    pub fn for_each_async<F, Fut>(&self, op: F)
    where
        F: Fn(<I as DistributedIterator>::Item) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + 'static,
    {
        self.iter.array().for_each_async(self, op);
    }
}

impl<I> DistributedIterator for Ignore<I>
where
    I: DistributedIterator,
{
    type Item = <I as DistributedIterator>::Item;
    type Array = <I as DistributedIterator>::Array;
    fn init(&self, in_start_i: usize, in_cnt: usize) -> Ignore<I> {
        let start_i = std::cmp::max(in_start_i, self.count);
        let end_i = std::cmp::max(start_i + in_cnt, self.count);
        let cnt = std::cmp::min(in_cnt, end_i - start_i);
        // println!("init ignore in start_i: {:?} start_i {:?} in cnt {:?} cnt {:?} in end_i: {:?} end_i: {:?} count {:?}",in_start_i,start_i,in_cnt,cnt,in_start_i+in_cnt,end_i,self.count);
        Ignore::new(self.iter.init(start_i, cnt), self.count)
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        // println!("ignore next");
        self.iter.next()
    }
    fn elems(&self, in_elems: usize) -> usize {
        let in_elems = self.iter.elems(in_elems);
        // println!("ignore elems {:?} {:?}",in_elems,std::cmp::max(0,in_elems-self.count));
        std::cmp::max(0, in_elems - self.count)
    }
    fn global_index(&self, index: usize) -> usize {
        let g_index = self.iter.global_index(index);
        // println!("ignore index: {:?} global_index {:?}", index,g_index);
        g_index
    }
    fn chunk_size(&self) -> usize {
        self.iter.chunk_size()
    }
    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count);
    }
}

use crate::array::iterator::distributed_iterator::*;

use futures::Future;

//ignores the first n elements of iterator I per pe (this implys that n * num_pes elements are ignored in total)
#[derive(Clone)]
pub struct Take<I> {
    iter: I,
    count: usize,
}

impl<I> Take<I>
where
    I: DistributedIterator,
{
    pub(crate) fn new(iter: I, count: usize) -> Take<I> {
        // println!("new Take {:?}",count);
        Take { iter, count }
    }
}

impl<I> Take<I>
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

impl<I> DistributedIterator for Take<I>
where
    I: DistributedIterator,
{
    type Item = <I as DistributedIterator>::Item;
    type Array = <I as DistributedIterator>::Array;
    fn init(&self, start_i: usize, cnt: usize) -> Take<I> {
        // println!("init take start_i: {:?} cnt: {:?} count: {:?}",start_i, cnt,self.count);
        Take::new(
            self.iter.init(start_i, std::cmp::min(cnt, self.count)),
            self.count,
        )
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        // println!("take next");
        self.iter.next()
    }
    fn elems(&self, in_elems: usize) -> usize {
        let in_elems = self.iter.elems(in_elems);
        // println!("take elems {:?} {:?}",in_elems,std::cmp::min(in_elems,self.count));
        std::cmp::min(in_elems, self.count)
    }
    fn global_index(&self, index: usize) -> usize {
        let g_index = self.iter.global_index(index);
        // println!("take index: {:?} global_index {:?}", index,g_index);
        g_index
    }
    fn chunk_size(&self) -> usize {
        self.iter.chunk_size()
    }
    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count);
    }
}

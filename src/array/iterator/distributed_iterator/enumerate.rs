use crate::array::iterator::distributed_iterator::*;

use futures::Future;
#[derive(Clone)]
pub struct Enumerate<I> {
    iter: I,
    count: usize,
}
impl<I> Enumerate<I>
where
    I: DistributedIterator,
{
    pub(crate) fn new(iter: I, count: usize) -> Enumerate<I> {
        // println!("new Enumerate {:?} ",count);
        Enumerate { iter, count }
    }
    
}

impl<I> Enumerate<I>
where
    I: DistributedIterator +DistIterChunkSize+ 'static,
{
    
    pub fn for_each<F>(&self, op: F)
    where
        F: Fn((usize, <I as DistributedIterator>::Item)) + Sync + Send + Clone + 'static,
    {
        self.iter.array().for_each(self, op, self.chunk_size());
    }
    pub fn for_each_async<F, Fut>(&self, op: F)
    where
        F: Fn((usize, <I as DistributedIterator>::Item)) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + 'static,
    {
        self.iter.array().for_each_async(self, op, self.chunk_size());
    }
}

impl<I> DistIterChunkSize for Enumerate<I>
where
    I: DistIterChunkSize,
{
    fn chunk_size(&self) -> usize {
        self.iter.chunk_size()
    }
}

impl<I> DistributedIterator for Enumerate<I>
where
    I: DistributedIterator +DistIterChunkSize,
{
    type Item = (usize, <I as DistributedIterator>::Item);
    type Array = <I as DistributedIterator>::Array;
    fn init(&self, start_i: usize, end_i: usize) -> Enumerate<I> {
        Enumerate::new(self.iter.init(start_i, end_i), start_i)
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        let a = self.iter.next()?;
        let i = self.iter.array().global_index_from_local(self.count,self.chunk_size());
        self.count += 1;
        Some((i, a))
    }
}

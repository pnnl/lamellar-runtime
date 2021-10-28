use crate::array::iterator::distributed_iterator::*;

use futures::Future;

#[derive(Clone)]
pub struct Chunks<I> {
    iter: I,
    cur_i: usize,
    end_i: usize,
    chunk_size: usize,

}

impl<I> Chunks<I>
where
    I: DistributedIterator,
{
    pub(crate) fn new(iter: I, cur_i: usize, end_i: usize,chunk_size: usize) -> Chunks<I> {
        // println!("new Chunks {:?} {:?} {:?}",cur_i, end_i,chunk_size);
        Chunks { iter, cur_i, end_i, chunk_size }
    }
    
}

impl<I> Chunks<I>
where
    I: DistributedIterator + DistIterChunkSize + 'static,
{
    pub fn for_each<F>(&self, op: F)
    where
        F: Fn(Chunk<I>) + Sync + Send + Clone + 'static,
    {
        self.iter.array().for_each(self, op, self.chunk_size());
    }
    pub fn for_each_async<F, Fut>(&self, op: F)
    where
        F: Fn(Chunk<I>) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + 'static,
    {
        self.iter.array().for_each_async(self, op, self.chunk_size());
    }
}

impl<I> DistIterChunkSize for Chunks<I>
where
    I: DistIterChunkSize,
{
    fn chunk_size(&self) -> usize {
        self.iter.chunk_size()*self.chunk_size
    }
}

impl<I> DistributedIterator for Chunks<I>
where
    I: DistributedIterator,
{
    type Item =  Chunk<I>;
    type Array = <I as DistributedIterator>::Array;
    fn init(&self, start_i: usize, end_i: usize) -> Chunks<I> {
        Chunks::new(self.iter.init(start_i*self.chunk_size, end_i*self.chunk_size), start_i, end_i,self.chunk_size)
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_i < self.end_i {
            // let size = std::cmp::min(self.chunk_size, self.end_i-self.cur_i);
            let start_i =self.cur_i*self.chunk_size;
            let iter = self.iter.clone().init(start_i,start_i+self.chunk_size);
            // println!("new Chunk {:?} {:?} {:?}",self.cur_i, self.cur_i+size, size);
            let chunk = Chunk{
                iter: iter
            };
            self.cur_i += 1;
            Some(chunk)
        } else {
            None
        }
    }
}

#[derive(Clone)]
pub struct Chunk<I>{
    iter: I,
}

impl<I> Iterator
    for Chunk<I>
    where
    I: DistributedIterator
{
    type Item = <I as DistributedIterator>::Item;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}
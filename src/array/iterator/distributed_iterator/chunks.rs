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
    pub(crate) fn new(iter: I, cur_i: usize, cnt: usize, chunk_size: usize) -> Chunks<I> {
        // println!("new Chunks {:?} {:?} {:?} {:?}",cur_i ,cnt, cur_i+cnt,chunk_size);
        Chunks {
            iter,
            cur_i,
            end_i: cur_i + cnt,
            chunk_size,
        }
    }
}

impl<I> Chunks<I>
where
    I: DistributedIterator + 'static,
{
    pub fn for_each<F>(self, op: F)
    where
        F: Fn(Chunk<I>) + Sync + Send + Clone + 'static,
    {
        self.iter.array().for_each(self, op);
    }
    pub fn for_each_async<F, Fut>(&self, op: F)
    where
        F: Fn(Chunk<I>) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + Clone + 'static,
    {
        self.iter.array().for_each_async(self, op);
    }
}

impl<I> DistributedIterator for Chunks<I>
where
    I: DistributedIterator,
{
    type Item = Chunk<I>;
    type Array = <I as DistributedIterator>::Array;
    fn init(&self, start_i: usize, cnt: usize) -> Chunks<I> {
        // println!("init chunks start_i: {:?} cnt {:?} end_i: {:?} chunk_size: {:?} chunk_size(): {:?}",start_i,cnt, start_i+cnt,self.chunk_size,self.chunk_size());
        Chunks::new(
            self.iter
                .init(start_i * self.chunk_size, (start_i + cnt) * self.chunk_size),
            start_i,
            cnt,
            self.chunk_size,
        )
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        // println!("chunks next cur_i {:?} end_i {:?}", self.cur_i, self.end_i);
        if self.cur_i < self.end_i {
            // let size = std::cmp::min(self.chunk_size, self.end_i-self.cur_i);
            let start_i = self.cur_i * self.chunk_size;
            let iter = self.iter.clone().init(start_i, self.chunk_size);
            // println!("new Chunk {:?} {:?} {:?} {:?}",self.cur_i, self.end_i, start_i,start_i+self.chunk_size);
            let chunk = Chunk { iter: iter };
            self.cur_i += 1;
            Some(chunk)
        } else {
            // println!("iter done!");
            None
        }
    }
    fn elems(&self, in_elems: usize) -> usize {
        let in_elems = self.iter.elems(in_elems);
        let elems = if in_elems % self.chunk_size > 0 {
            1 + in_elems / self.chunk_size
        } else {
            in_elems / self.chunk_size
        };
        // println!("chunk elems {:?} {:?}",in_elems, elems);
        elems
    }
    fn global_index(&self, index: usize) -> Option<usize> {
        let g_index = self.iter.global_index(index * self.chunk_size)? / self.chunk_size;
        // println!("chunks index: {:?} global_index {:?}", index,g_index);
        Some(g_index)
    }
    fn subarray_index(&self, index: usize) -> Option<usize> {
        let g_index = self.iter.subarray_index(index*self.chunk_size)? /self.chunk_size; //not sure if this works...
                                                  // println!("enumerate index: {:?} global_index {:?}", index,g_index);
        Some(g_index)
    }
    // fn chunk_size(&self) -> usize {
    //     self.iter.chunk_size() * self.chunk_size
    // }
    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

#[derive(Clone)]
pub struct Chunk<I> {
    iter: I,
}

impl<I> Iterator for Chunk<I>
where
    I: DistributedIterator,
{
    type Item = <I as DistributedIterator>::Item;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

use crate::array::iterator::distributed_iterator::*;

use futures::Future;
#[derive(Clone)]
pub struct Zip<A,B> {
    a: A,
    b: B,
}
impl<A,B> Zip<A,B>
where
    A: DistributedIterator, 
    B: DistributedIterator,
{
    pub(crate) fn new(a: A, b: B) -> Zip<A,B> {
        // println!("new Zip {:?} ",count);
        Zip { a, b }
    }
}

impl<A,B> Zip<A,B>
where
    A: DistributedIterator + 'static, 
    B: DistributedIterator + 'static,
{
    pub fn for_each<F>(&self, op: F)
    where
        F: Fn((<A as DistributedIterator>::Item, <B as DistributedIterator>::Item)) + Sync + Send + Clone + 'static,
    {
        self.a.array().for_each(self, op);
    }
    pub fn for_each_async<F, Fut>(&self, op: F)
    where
        F: Fn((<A as DistributedIterator>::Item, <B as DistributedIterator>::Item)) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + 'static,
    {
        self.a.array().for_each_async(self, op);
    }
}

impl<A,B> DistributedIterator for Zip<A,B>
where
    A: DistributedIterator, 
    B: DistributedIterator,
{
    type Item = (<A as DistributedIterator>::Item, <B as DistributedIterator>::Item);
    type Array = <A as DistributedIterator>::Array;
    fn init(&self, start_i: usize, cnt: usize) -> Zip<A,B> {
        // println!("init zip start_i: {:?} cnt {:?} end_i {:?}",start_i, cnt, start_i+cnt );
        Zip::new(self.a.init(start_i, cnt), self.b.init(start_i, cnt))
    }
    fn array(&self) -> Self::Array {
        self.a.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        // println!("zip next");
        let a = self.a.next()?;
        let b = self.b.next()?;
        Some((a, b))
    }
    fn elems(&self, in_elems: usize) -> usize {
        let in_elems = std::cmp::min(self.a.elems(in_elems),self.b.elems(in_elems));
        // println!("enumerate elems {:?}",in_elems);
        in_elems
    }
    fn global_index(&self, index: usize) -> usize {
        let g_index = self.a.global_index(index); //not sure if this works...
        // println!("enumerate index: {:?} global_index {:?}", index,g_index);
        g_index
    }
    // fn chunk_size(&self) -> usize {
    //     self.iter.chunk_size()
    // }
    fn advance_index(&mut self, count: usize) {
        self.a.advance_index(count);
        self.b.advance_index(count);
    }
}

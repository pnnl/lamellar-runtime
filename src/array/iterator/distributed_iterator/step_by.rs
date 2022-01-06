use crate::array::iterator::distributed_iterator::*;

use futures::Future;

//ignores the first n elements of iterator I per pe (this implys that n * num_pes elements are ignored in total)
#[derive(Clone)]
pub struct StepBy<I> {
    iter: I,
    step_size: usize,
}

impl<I> StepBy<I>
where
    I: DistributedIterator,
{
    pub(crate) fn new(iter: I, step_size: usize) -> StepBy<I> {
        // println!("new StepBy {:?} ",step_size);
        StepBy { iter, step_size }
    }
}

impl<I> StepBy<I>
where
    I: DistributedIterator + 'static,
{
    pub fn for_each<F>(self, op: F)
    where
        F: Fn(<I as DistributedIterator>::Item) + Sync + Send + Clone + 'static,
    {
        self.iter.array().for_each(self, op);
    }
    pub fn for_each_async<F, Fut>(&self, op: F)
    where
        F: Fn(<I as DistributedIterator>::Item) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + Clone + 'static,
    {
        self.iter.array().for_each_async(self, op);
    }
}

impl<I> DistributedIterator for StepBy<I>
where
    I: DistributedIterator,
{
    type Item = <I as DistributedIterator>::Item;
    type Array = <I as DistributedIterator>::Array;
    fn init(&self, start_i: usize, cnt: usize) -> StepBy<I> {
        // println!("init step by start_i {:?} cnt {:?} step_size {:?}",start_i, cnt, self.step_size);
        StepBy::new(
            self.iter
                .init(start_i * self.step_size, cnt * self.step_size),
            self.step_size,
        )
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        // println!("step_by next");
        let res = self.iter.next();
        self.iter.advance_index(self.step_size - 1); //-1 cause iter.next() already advanced by 1
        res
    }
    fn elems(&self, in_elems: usize) -> usize {
        let in_elems = self.iter.elems(in_elems);
        // println!("step by elems {:?} {:?} ",in_elems,(in_elems as f32/self.step_size as f32).ceil());
        (in_elems as f32 / self.step_size as f32).ceil() as usize
    }
    fn global_index(&self, index: usize) -> usize {
        let g_index = self.iter.global_index(index * self.step_size) / self.step_size;
        // println!("step_by index: {:?} global_index {:?}", index,g_index);
        g_index
    }
    // fn chunk_size(&self) -> usize {
    //     self.iter.chunk_size()
    // }
    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count);
    }
}

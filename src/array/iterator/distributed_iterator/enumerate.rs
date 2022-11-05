use crate::array::iterator::distributed_iterator::*;

#[derive(Clone, Debug)]
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

// impl<I> Enumerate<I>
// where
//     I: DistributedIterator + 'static,
// {
//     pub fn for_each<F>(&self, op: F)
//     where
//         F: Fn((usize, <I as DistributedIterator>::Item)) + SyncSend  + Clone + 'static,
//     {
//         self.iter.array().for_each(self, op);
//     }
//     pub fn for_each_async<F, Fut>(&self, op: F)
//     where
//         F: Fn((usize, <I as DistributedIterator>::Item)) -> Fut + SyncSend  + Clone + 'static,
//         Fut: Future<Output = ()> + SyncSend  + Clone + 'static,
//     {
//         self.iter.array().for_each_async(self, op);
//     }
// }

impl<I> DistributedIterator for Enumerate<I>
where
    I: DistributedIterator,
{
    type Item = (usize, <I as DistributedIterator>::Item);
    type Array = <I as DistributedIterator>::Array;
    fn init(&self, start_i: usize, cnt: usize) -> Enumerate<I> {
        // println!("init enumerate start_i: {:?} cnt {:?} end_i {:?}",start_i, cnt, start_i+cnt );
        Enumerate::new(self.iter.init(start_i, cnt), start_i)
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        let a = self.iter.next()?;
        let i = self.subarray_index(self.count)?;
        // println!("enumerate next {:?} i: {:?}",self.count,i);
        self.count += 1;
        Some((i, a))
    }

    fn elems(&self, in_elems: usize) -> usize {
        let in_elems = self.iter.elems(in_elems);
        // println!("enumerate elems {:?}",in_elems);
        in_elems
    }
    fn global_index(&self, index: usize) -> Option<usize> {
        let g_index = self.iter.global_index(index);
        // println!("enumerate index: {:?} global_index {:?}", index,g_index);
        g_index
    }
    fn subarray_index(&self, index: usize) -> Option<usize> {
        let g_index = self.iter.subarray_index(index); //not sure if this works...
                                                       // println!("enumerate index: {:?} global_index {:?}", index,g_index);
        g_index
    }
    // fn chunk_size(&self) -> usize {
    //     self.iter.chunk_size()
    // }
    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count);
        self.count += count;
    }
}

use crate::array::iterator::distributed_iterator::*;

#[derive(Clone, Debug)]
pub struct Filter<I, F> {
    iter: I,
    f: F,
}
impl<I, F> Filter<I, F>
where
    I: DistributedIterator,
{
    pub(crate) fn new(iter: I, f: F) -> Filter<I, F> {
        // println!("new Filter {:?} ",count);
        Filter { iter, f }
    }
}

impl<I, F> DistributedIterator for Filter<I, F>
where
    I: DistributedIterator,
    F: FnMut(&I::Item) -> bool + SyncSend + Clone + 'static,
{
    type Item = I::Item;
    type Array = <I as DistributedIterator>::Array;
    fn init(&self, start_i: usize, cnt: usize) -> Filter<I, F> {
        let val = Filter::new(self.iter.init(start_i, cnt), self.f.clone());
        // println!("{:?} Filter init {start_i} {cnt}",std::thread::current().id());
        val
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(next) = self.iter.next() {
            // println!("{:?} Filter next",std::thread::current().id());
            if (self.f)(&next) {
                return Some(next);
            }
        }
        // println!("{:?} Filter done",std::thread::current().id());
        None
    }

    fn elems(&self, in_elems: usize) -> usize {
        let in_elems = self.iter.elems(in_elems);
        in_elems
    }
    // fn global_index(&self, index: usize) -> Option<usize> {
    //     let g_index = self.iter.global_index(index);
    //     // println!("enumerate index: {:?} global_index {:?}", index,g_index);
    //     g_index
    // }
    // fn subarray_index(&self, index: usize) -> Option<usize> {
    //     let g_index = self.iter.subarray_index(index);
    //                                                    // println!("enumerate index: {:?} global_index {:?}", index,g_index);
    //     g_index
    // }
    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count);
        // println!("{:?} \t Filter advance index {count}",std::thread::current().id());
    }
}

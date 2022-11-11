use crate::array::iterator::local_iterator::*;

//ignores the first n elements of iterator I per pe (this implys that n * num_pes elements are ignored in total)
#[derive(Clone, Debug)]
pub struct Take<I> {
    iter: I,
    take_count: usize,
}

impl<I> Take<I>
where
    I: IndexedLocalIterator,
{
    pub(crate) fn new(iter: I, take_count: usize) -> Take<I> {
        Take { iter, take_count }
    }
}

impl<I> LocalIterator for Take<I>
where
    I: IndexedLocalIterator,
{
    type Item = <I as LocalIterator>::Item;
    type Array = <I as LocalIterator>::Array;
    fn init(&self, start_i: usize, cnt: usize) -> Take<I> {
        let start_i = std::cmp::min(start_i,self.take_count);
        let end_i = std::cmp::min(start_i+cnt, self.take_count);
        let len = end_i-start_i;
        Take::new(
            self.iter.init(start_i, len),
            self.take_count,
        )
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
    fn elems(&self, in_elems: usize) -> usize {
        let in_elems = self.iter.elems(in_elems);
        std::cmp::min(in_elems, self.take_count)
    }
    fn subarray_index(&self, index: usize) -> Option<usize> {
        let g_index = self.iter.subarray_index(index); 
        g_index
    }
    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count);
    }
}

impl<I> IndexedLocalIterator for Take<I>
where
    I: IndexedLocalIterator,
{
    fn iterator_index(&self, index: usize) -> Option<usize> {
        let i_index = self.iter.iterator_index(index); 
        // println!("{:?} \t Enumerate iterator index {index} {g_index:?}",std::thread::current().id());
        i_index
    }

}

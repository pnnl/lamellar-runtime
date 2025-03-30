use crate::array::iterator::{local_iterator::*, IterLockFuture};

/// `Take` is an iterator that limits the number of elements returned by an iterator to a specified count.
#[derive(Clone, Debug)]
pub struct Take<I> {
    iter: I,
    take_count: usize,
}

impl<I: InnerIter> InnerIter for Take<I> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        self.iter.lock_if_needed(_s)
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        Take {
            iter: self.iter.iter_clone(Sealed),
            take_count: self.take_count,
        }
    }
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
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Take<I> {
        let start_i = std::cmp::min(start_i, self.take_count);
        let end_i = std::cmp::min(start_i + cnt, self.take_count);
        let len = end_i - start_i;
        Take::new(self.iter.init(start_i, len, _s), self.take_count)
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

use crate::array::iterator::{local_iterator::*, IterLockFuture};

/// `Skip` is an iterator that skips a specified number of elements from the start of an iterator.
#[derive(Clone, Debug)]
pub struct Skip<I> {
    iter: I,
    skip_count: usize,
    skip_offset: usize,
}

impl<I: InnerIter> InnerIter for Skip<I> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        self.iter.lock_if_needed(_s)
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        Skip {
            iter: self.iter.iter_clone(Sealed),
            skip_count: self.skip_count,
            skip_offset: self.skip_offset,
        }
    }
}

impl<I> Skip<I>
where
    I: IndexedLocalIterator,
{
    pub(crate) fn new(iter: I, skip_count: usize, skip_offset: usize) -> Skip<I> {
        // println!("new Skip {:?} ",count);
        Skip {
            iter,
            skip_count,
            skip_offset,
        }
    }
}

impl<I> LocalIterator for Skip<I>
where
    I: IndexedLocalIterator,
{
    type Item = <I as LocalIterator>::Item;
    type Array = <I as LocalIterator>::Array;
    fn init(&self, in_start_i: usize, in_cnt: usize, _s: Sealed) -> Skip<I> {
        let mut iter = self.iter.init(in_start_i, in_cnt, _s);
        let start_i = std::cmp::max(in_start_i, self.skip_count);
        let advance = std::cmp::min(start_i - in_start_i, in_cnt);

        // let end_i = std::cmp::max(in_start_i + in_cnt, self.skip_count);
        // let cnt = std::cmp::min(in_cnt, end_i - start_i);

        iter.advance_index(advance);

        let val = Skip::new(iter, self.skip_count, advance);
        // println!("{:?} Skip init {in_start_i} {start_i} {advance} {in_cnt} {:?}",std::thread::current().id(),self.skip_count);
        val
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        let val = self.iter.next();
        if val.is_some() {
            // println!("{:?} Skip next {:?}",std::thread::current().id(),self.skip_count);
        } else {
            // println!("{:?} Skip done",std::thread::current().id());
        }
        val
    }
    fn elems(&self, in_elems: usize) -> usize {
        let in_elems = self.iter.elems(in_elems);
        std::cmp::max(0, in_elems - self.skip_count)
    }

    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count);
    }
}

impl<I> IndexedLocalIterator for Skip<I>
where
    I: IndexedLocalIterator,
{
    fn iterator_index(&self, index: usize) -> Option<usize> {
        let Some(i_index) = self.iter.iterator_index(index + self.skip_offset) else {
            // println!("{:?} \t Skip iterator index  {index} {} None",std::thread::current().id(),self.skip_offset);
            return None;
        };

        let i_index = i_index as isize - self.skip_count as isize;
        if i_index >= 0 {
            // println!("{:?} \t Skip iterator index  {index} {} {i_index}",std::thread::current().id(),self.skip_offset);
            Some(i_index as usize)
        } else {
            // println!("{:?} \t Skip iterator index  {index} {} None",std::thread::current().id(),self.skip_offset);
            None
        }
    }
}

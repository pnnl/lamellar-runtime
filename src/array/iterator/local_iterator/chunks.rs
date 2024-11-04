use crate::array::iterator::{local_iterator::*, IterLockFuture};

#[derive(Clone, Debug)]
pub struct Chunks<I> {
    iter: I,
    cur_i: usize,
    end_i: usize,
    chunk_size: usize,
}

impl<I: InnerIter> InnerIter for Chunks<I> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        self.iter.lock_if_needed(_s)
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        Chunks {
            iter: self.iter.iter_clone(Sealed),
            cur_i: self.cur_i,
            end_i: self.end_i,
            chunk_size: self.chunk_size,
        }
    }
}

impl<I> Chunks<I>
where
    I: IndexedLocalIterator,
{
    pub(crate) fn new(iter: I, cur_i: usize, cnt: usize, chunk_size: usize) -> Chunks<I> {
        Chunks {
            iter,
            cur_i,
            end_i: cur_i + cnt,
            chunk_size,
        }
    }
}

impl<I> LocalIterator for Chunks<I>
where
    I: IndexedLocalIterator,
{
    type Item = Chunk<I>;
    type Array = <I as LocalIterator>::Array;
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Chunks<I> {
        Chunks::new(
            self.iter.init(
                start_i * self.chunk_size,
                (start_i + cnt) * self.chunk_size,
                _s,
            ),
            start_i,
            cnt,
            self.chunk_size,
        )
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_i < self.end_i {
            let start_i = self.cur_i * self.chunk_size;
            let iter = self
                .iter
                .iter_clone(Sealed)
                .init(start_i, self.chunk_size, Sealed);
            let chunk = Chunk { iter: iter };
            self.cur_i += 1;
            Some(chunk)
        } else {
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
        elems
    }

    fn advance_index(&mut self, count: usize) {
        self.cur_i = std::cmp::min(self.cur_i + count, self.end_i);
    }
}

impl<I> IndexedLocalIterator for Chunks<I>
where
    I: IndexedLocalIterator,
{
    fn iterator_index(&self, index: usize) -> Option<usize> {
        let i_index = self.iter.iterator_index(index);
        i_index
    }
}

#[derive(Clone)]
pub struct Chunk<I> {
    iter: I,
}

impl<I> Iterator for Chunk<I>
where
    I: LocalIterator,
{
    type Item = <I as LocalIterator>::Item;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

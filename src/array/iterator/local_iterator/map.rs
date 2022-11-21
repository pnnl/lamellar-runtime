use crate::array::iterator::local_iterator::*;

#[derive(Clone, Debug)]
pub struct Map<I, F> {
    iter: I,
    f: F,
}
impl<I, F> Map<I, F>
where
    I: IndexedLocalIterator,
{
    pub(crate) fn new(iter: I, f: F) -> Map<I, F> {
        Map { iter, f }
    }
}

impl<B, I, F> LocalIterator for Map<I, F>
where
    I: IndexedLocalIterator,
    F: FnMut(I::Item) -> B + SyncSend + Clone + 'static,
    B: Send,
{
    type Item = B;
    type Array = <I as LocalIterator>::Array;
    fn init(&self, start_i: usize, cnt: usize) -> Map<I, F> {
        Map::new(self.iter.init(start_i, cnt), self.f.clone())
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(&mut self.f)
    }

    fn elems(&self, in_elems: usize) -> usize {
        let in_elems = self.iter.elems(in_elems);
        in_elems
    }

    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count);
    }
}

impl<B, I, F> IndexedLocalIterator for Map<I, F>
where
    I: IndexedLocalIterator,
    F: FnMut(I::Item) -> B + SyncSend + Clone + 'static,
    B: Send,
{
    fn iterator_index(&self, index: usize) -> Option<usize> {
        let i_index = self.iter.iterator_index(index);
        i_index
    }
}

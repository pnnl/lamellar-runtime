use crate::array::iterator::local_iterator::*;

#[derive(Clone, Debug)]
pub struct Enumerate<I> {
    iter: I,
    cur_index: usize,
}
impl<I> Enumerate<I>
where
    I: IndexedLocalIterator,
{
    pub(crate) fn new(iter: I, cur_index: usize) -> Enumerate<I> {
        // println!("new Enumerate {:?} ",count);
        Enumerate { iter, cur_index }
    }
}

impl<I> LocalIterator for Enumerate<I>
where
    I: IndexedLocalIterator,
{
    type Item = (usize, <I as LocalIterator>::Item);
    type Array = <I as LocalIterator>::Array;
    fn init(&self, start_i: usize, cnt: usize) -> Enumerate<I> {
        let val = Enumerate::new(self.iter.init(start_i, cnt), start_i);
        // println!("{:?} Enumerate init {start_i} {cnt} {start_i}",std::thread::current().id());
        val
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(a) = self.iter.next() {
            let i = self.iterator_index(self.cur_index)?;
            // println!("{:?} Enumerate next {:?} i: {:?}",std::thread::current().id(),self.cur_index,i);
            self.cur_index += 1;
            Some((i, a))
        } else {
            // println!("{:?} Enumerate done",std::thread::current().id());
            None
        }
    }

    fn elems(&self, in_elems: usize) -> usize {
        let in_elems = self.iter.elems(in_elems);
        in_elems
    }

    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count);
        self.cur_index += count;
    }
}

impl<I> IndexedLocalIterator for Enumerate<I>
where
    I: IndexedLocalIterator,
{
    fn iterator_index(&self, index: usize) -> Option<usize> {
        let i_index = self.iter.iterator_index(index);
        // println!("{:?} \t Enumerate iterator index {index} {g_index:?}",std::thread::current().id());
        i_index
    }
}

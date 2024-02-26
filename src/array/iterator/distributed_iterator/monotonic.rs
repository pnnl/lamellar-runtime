use crate::array::iterator::distributed_iterator::*;

#[derive(Clone, Debug)]
pub struct Monotonic<I> {
    iter: I,
    cur_index: usize,
}

impl<I: IterClone> IterClone for Monotonic<I> {
    fn iter_clone(&self, _: Sealed) -> Self {
        Monotonic {
            iter: self.iter.iter_clone(Sealed),
            cur_index: self.cur_index,
        }
    }
}

impl<I> Monotonic<I>
where
    I: DistributedIterator,
{
    pub(crate) fn new(iter: I, cur_index: usize) -> Monotonic<I> {
        // println!("new Monotonic {:?} ",count);
        Monotonic { iter, cur_index }
    }
}

impl<I> DistributedIterator for Monotonic<I>
where
    I: DistributedIterator,
{
    type Item = (usize, <I as DistributedIterator>::Item);
    type Array = <I as DistributedIterator>::Array;
    fn init(&self, start_i: usize, cnt: usize) -> Monotonic<I> {
        let val = Monotonic::new(self.iter.init(start_i, cnt), start_i);
        // println!("{:?} Monotonic init {start_i} {cnt} {start_i}",std::thread::current().id());
        val
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(a) = self.iter.next() {
            let i = self.cur_index;
            // println!("{:?} Monotonic next {:?} i: {:?}",std::thread::current().id(),self.cur_index,i);
            self.cur_index += 1;
            Some((i, a))
        } else {
            // println!("{:?} Monotonic done",std::thread::current().id());
            None
        }
    }

    fn elems(&self, in_elems: usize) -> usize {
        let in_elems = self.iter.elems(in_elems);
        in_elems
    }

    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count);
        self.cur_index += 1;
    }
}

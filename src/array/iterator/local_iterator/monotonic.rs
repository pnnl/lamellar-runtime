use crate::array::iterator::{local_iterator::*, IterLockFuture};

#[derive(Clone, Debug)]
pub struct Monotonic<I> {
    iter: I,
    cur_index: usize,
}

impl<I: InnerIter> InnerIter for Monotonic<I> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        self.iter.lock_if_needed(_s)
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        Monotonic {
            iter: self.iter.iter_clone(Sealed),
            cur_index: self.cur_index,
        }
    }
}
impl<I> Monotonic<I>
where
    I: LocalIterator,
{
    pub(crate) fn new(iter: I, cur_index: usize) -> Monotonic<I> {
        // println!("new Monotonic {:?} ",count);
        Monotonic { iter, cur_index }
    }
}

impl<I> LocalIterator for Monotonic<I>
where
    I: LocalIterator,
{
    type Item = (usize, <I as LocalIterator>::Item);
    type Array = <I as LocalIterator>::Array;
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Monotonic<I> {
        let val = Monotonic::new(self.iter.init(start_i, cnt, _s), start_i);
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

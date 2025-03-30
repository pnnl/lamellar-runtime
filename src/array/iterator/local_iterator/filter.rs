use crate::array::iterator::{local_iterator::*, IterLockFuture};


/// `Filter` is an iterator that filters elements based on a predicate function.
#[derive(Clone, Debug)]
pub struct Filter<I, F> {
    iter: I,
    f: F,
}

impl<I: InnerIter, F: Clone> InnerIter for Filter<I, F> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        self.iter.lock_if_needed(_s)
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        Filter {
            iter: self.iter.iter_clone(Sealed),
            f: self.f.clone(),
        }
    }
}

impl<I, F> Filter<I, F>
where
    I: LocalIterator,
{
    pub(crate) fn new(iter: I, f: F) -> Filter<I, F> {
        // println!("new Filter {:?} ",count);
        Filter { iter, f }
    }
}

impl<I, F> LocalIterator for Filter<I, F>
where
    I: LocalIterator,
    F: FnMut(&I::Item) -> bool + SyncSend + Clone + 'static,
{
    type Item = I::Item;
    type Array = <I as LocalIterator>::Array;
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Filter<I, F> {
        // println!("{:?} Filter init before {start_i} {cnt}",std::thread::current().id());
        let val = Filter::new(self.iter.init(start_i, cnt, _s), self.f.clone());
        // println!("{:?} Filter init after {start_i} {cnt}",std::thread::current().id());

        val
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(next) = self.iter.next() {
            // println!("{:?} {:?} Filter next", (self.f)(&next),std::thread::current().id());
            if (self.f)(&next) {
                return Some(next);
            }
        }
        // println!("{:?} Filter done",std::thread::current().id());
        None
    }

    fn elems(&self, in_elems: usize) -> usize {
        let in_elems = self.iter.elems(in_elems);
        // println!("enumerate elems {:?}",in_elems);
        in_elems
    }

    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count);
    }
}

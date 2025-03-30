use crate::array::iterator::{local_iterator::*, IterLockFuture};


/// `Zip` is a combinator that combines two local iterators into a single iterator
/// that yields tuples of items from each of the two iterators.
#[derive(Clone, Debug)]
pub struct Zip<A, B> {
    a: A,
    b: B,
}

impl<A: InnerIter, B: InnerIter> InnerIter for Zip<A, B> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        let futa = self.a.lock_if_needed(_s);
        let futb = self.b.lock_if_needed(_s);
        match (futa, futb) {
            (None, None) => None,
            (Some(futa), None) => Some(futa),
            (None, Some(futb)) => Some(futb),
            (Some(futa), Some(futb)) => Some(Box::pin(async move {
                let _ = futures_util::future::join(futa, futb).await;
            })),
        }
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        Zip {
            a: self.a.iter_clone(Sealed),
            b: self.b.iter_clone(Sealed),
        }
    }
}

impl<A, B> Zip<A, B>
where
    A: IndexedLocalIterator,
    B: IndexedLocalIterator,
{
    pub(crate) fn new(a: A, b: B) -> Zip<A, B> {
        Zip { a, b }
    }
}

impl<A, B> LocalIterator for Zip<A, B>
where
    A: IndexedLocalIterator,
    B: IndexedLocalIterator,
{
    type Item = (<A as LocalIterator>::Item, <B as LocalIterator>::Item);
    type Array = <A as LocalIterator>::Array;
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Zip<A, B> {
        // println!("init zip start_i: {:?} cnt {:?} end_i {:?}",start_i, cnt, start_i+cnt );
        Zip::new(self.a.init(start_i, cnt, _s), self.b.init(start_i, cnt, _s))
    }
    fn array(&self) -> Self::Array {
        self.a.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        // println!("zip next");
        let a = self.a.next()?;
        let b = self.b.next()?;
        Some((a, b))
    }
    fn elems(&self, in_elems: usize) -> usize {
        let in_elems = std::cmp::min(self.a.elems(in_elems), self.b.elems(in_elems));
        // println!("enumerate elems {:?}",in_elems);
        in_elems
    }

    fn advance_index(&mut self, count: usize) {
        self.a.advance_index(count);
        self.b.advance_index(count);
    }
}

impl<A, B> IndexedLocalIterator for Zip<A, B>
where
    A: IndexedLocalIterator,
    B: IndexedLocalIterator,
{
    fn iterator_index(&self, index: usize) -> Option<usize> {
        let i_index = self.a.iterator_index(index);
        i_index
    }
}

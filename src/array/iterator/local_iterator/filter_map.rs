use crate::array::iterator::{local_iterator::*, IterLockFuture};


/// `FilterMap` is an iterator that applies a function to each element of the input iterator,
/// transforming it into a new type, while also filtering out elements that do not satisfy the function.
#[derive(Clone, Debug)]
pub struct FilterMap<I, F> {
    iter: I,
    f: F,
}

impl<I: InnerIter, F: Clone> InnerIter for FilterMap<I, F> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        self.iter.lock_if_needed(_s)
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        FilterMap {
            iter: self.iter.iter_clone(Sealed),
            f: self.f.clone(),
        }
    }
}

impl<I, F> FilterMap<I, F>
where
    I: LocalIterator,
{
    pub(crate) fn new(iter: I, f: F) -> FilterMap<I, F> {
        // println!("new FilterMap {:?} ",count);
        FilterMap { iter, f }
    }
}

// impl<B,I,F> FilterMap<I,F>
// where
//     I: LocalIterator + 'static,
//     F: FnMut(I::Item) -> Option<B> + SyncSend + Clone + 'static,
//     B: Send + 'static
// {
//     pub fn for_each<G>(&self, op: G)
//     where
//         G: Fn(B) + SyncSend  + Clone + 'static,
//     {
//         self.iter.array().for_each(self, op);
//     }
//     pub fn for_each_async<G, Fut>(&self, op: G)
//     where
//         G: Fn(B) -> Fut + SyncSend  + Clone + 'static,
//         Fut: Future<Output = ()> + Send + 'static,
//     {
//         self.iter.array().for_each_async(self, op);
//     }
// }

impl<B, I, F> LocalIterator for FilterMap<I, F>
where
    I: LocalIterator,
    F: FnMut(I::Item) -> Option<B> + SyncSend + Clone + 'static,
    B: Send,
{
    type Item = B;
    type Array = <I as LocalIterator>::Array;
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> FilterMap<I, F> {
        // println!("init enumerate start_i: {:?} cnt {:?} end_i {:?}",start_i, cnt, start_i+cnt );
        FilterMap::new(self.iter.init(start_i, cnt, _s), self.f.clone())
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(next) = self.iter.next() {
            if let Some(next) = (self.f)(next) {
                return Some(next);
            }
        }
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

use crate::array::iterator::{distributed_iterator::*, IterLockFuture};

/// `Enumerate` is an iterator that yields pairs of the current index and the value from the input iterator.
#[derive(Clone, Debug)]
pub struct Enumerate<I> {
    iter: I,
    cur_index: usize,
}

impl<I: InnerIter> InnerIter for Enumerate<I> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        self.iter.lock_if_needed(_s)
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        Enumerate {
            iter: self.iter.iter_clone(Sealed),
            cur_index: self.cur_index,
        }
    }
}

impl<I> Enumerate<I>
where
    I: IndexedDistributedIterator,
{
    pub(crate) fn new(iter: I, cur_index: usize) -> Enumerate<I> {
        // println!("new Enumerate {:?} ",count);
        Enumerate { iter, cur_index }
    }
}

impl<I> DistributedIterator for Enumerate<I>
where
    I: IndexedDistributedIterator,
{
    type Item = (usize, <I as DistributedIterator>::Item);
    type Array = <I as DistributedIterator>::Array;
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Enumerate<I> {
        let iter = self.iter.init(start_i, cnt, _s);
        let val = Enumerate::new(iter, start_i);
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
        // println!("enumerate elems {:?}",in_elems);
        in_elems
    }
    // fn global_index(&self, index: usize) -> Option<usize> {
    //     let g_index = self.iter.global_index(index);
    //     // println!("enumerate index: {:?} global_index {:?}", index,g_index);
    //     g_index
    // }
    // fn subarray_index(&self, index: usize) -> Option<usize> {
    //     let g_index = self.iter.subarray_index(index);
    //                                                    // println!("enumerate index: {:?} global_index {:?}", index,g_index);
    //     g_index
    // }
    // fn chunk_size(&self) -> usize {
    //     self.iter.chunk_size()
    // }
    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count);
        // println!("{:?} \t Enumerate advance index {count}",std::thread::current().id());
        self.cur_index += count;
    }
}

impl<I> IndexedDistributedIterator for Enumerate<I>
where
    I: IndexedDistributedIterator,
{
    fn iterator_index(&self, index: usize) -> Option<usize> {
        let g_index = self.iter.iterator_index(index);
        // println!("{:?} \t Enumerate iterator index {index} {g_index:?}",std::thread::current().id());
        g_index
    }
}

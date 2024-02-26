use crate::array::iterator::distributed_iterator::*;

//skips the first n elements of iterator I per pe (this implys that n * num_pes elements are skipd in total)
#[derive(Clone, Debug)]
pub struct Take<I> {
    iter: I,
    count: usize,
    cur_index: usize,
}

impl<I: IterClone> IterClone for Take<I> {
    fn iter_clone(&self, _: Sealed) -> Self {
        Take {
            iter: self.iter.iter_clone(Sealed),
            count: self.count,
            cur_index: self.cur_index,
        }
    }
}

impl<I> Take<I>
where
    I: IndexedDistributedIterator,
{
    pub(crate) fn new(iter: I, count: usize, start_i: usize) -> Take<I> {
        // println!("new Take {:?}",count);
        Take {
            iter,
            count,
            cur_index: start_i,
        }
    }
}

// impl<I> Take<I>
// where
//     I: DistributedIterator + 'static,
// {
//     pub fn for_each<F>(&self, op: F)
//     where
//         F: Fn(I::Item)   + Clone + 'static,
//     {
//         self.iter.array().for_each(self, op);
//     }
//     pub fn for_each_async<F, Fut>(&self, op: F)
//     where
//         F: Fn(I::Item) -> Fut   + Clone + 'static,
//         Fut: Future<Output = ()>   + Clone + 'static,
//     {
//         self.iter.array().for_each_async(self, op);
//     }
// }

impl<I> DistributedIterator for Take<I>
where
    I: IndexedDistributedIterator,
{
    type Item = <I as DistributedIterator>::Item;
    type Array = <I as DistributedIterator>::Array;
    fn init(&self, start_i: usize, len: usize) -> Take<I> {
        // println!("init take start_i: {:?} cnt: {:?} count: {:?}",start_i, cnt,self.count);
        let val = Take::new(self.iter.init(start_i, len), self.count, start_i);
        // println!("{:?} Take init {start_i} {len} {:?} {start_i}",std::thread::current().id(),self.count);
        val
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        // println!("take next");
        if self.iterator_index(self.cur_index)? < self.count {
            self.cur_index += 1;
            let val = self.iter.next();
            // println!("{:?} Take next ",std::thread::current().id());
            val
        } else {
            // println!("{:?} Take done",std::thread::current().id());
            None
        }
    }
    fn elems(&self, in_elems: usize) -> usize {
        let in_elems = self.iter.elems(in_elems);
        std::cmp::max(0, in_elems)
    }
    // fn global_index(&self, index: usize) -> Option<usize> {
    //     let g_index = self.iter.global_index(index);
    //     // println!("take index: {:?} global_index {:?}", index,g_index);
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
        self.cur_index += count;
        // println!("{:?} \t Take advance index {count}",std::thread::current().id());
    }
}

impl<I> IndexedDistributedIterator for Take<I>
where
    I: IndexedDistributedIterator,
{
    fn iterator_index(&self, index: usize) -> Option<usize> {
        let Some(g_index) = self.iter.iterator_index(index) else {
            // println!("{:?} \t Take advance index {index} None",std::thread::current().id());
            return None;
        };
        if g_index < self.count {
            // println!("{:?} \t Take advance index {index} {g_index}",std::thread::current().id());
            Some(g_index)
        } else {
            // println!("{:?} \t Take advance index {index} None",std::thread::current().id());
            None
        }
    }
}

use crate::array::iterator::{distributed_iterator::*, IterLockFuture};

//skips the first n elements of iterator I per pe (this implys that n * num_pes elements are skipd in total)
#[derive(Clone, Debug)]
pub struct Skip<I> {
    iter: I,
    count: usize,
    skip_index: usize,
}

impl<I: InnerIter> InnerIter for Skip<I> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        Skip {
            iter: self.iter.iter_clone(Sealed),
            count: self.count,
            skip_index: self.skip_index,
        }
    }
}

impl<I> Skip<I>
where
    I: IndexedDistributedIterator,
{
    pub(crate) fn new(iter: I, count: usize, start_i: usize) -> Skip<I> {
        // println!("new Skip {:?} ",count);
        Skip {
            iter,
            count,
            skip_index: start_i,
        }
    }
}

impl<I> DistributedIterator for Skip<I>
where
    I: IndexedDistributedIterator,
{
    type Item = <I as DistributedIterator>::Item;
    type Array = <I as DistributedIterator>::Array;
    fn init(&self, in_start_i: usize, len: usize, _s: Sealed) -> Skip<I> {
        let mut iter = self.iter.init(in_start_i, len, _s);
        let mut skip_index = in_start_i;

        //now we need to see how many elements to skip
        if let Some(mut iterator_index) = iter.iterator_index(skip_index) {
            while (iterator_index as isize - self.count as isize) < 0 {
                skip_index += 1;
                match iter.iterator_index(skip_index) {
                    Some(i) => iterator_index = i,
                    None => {
                        iter.advance_index(len);
                        let val = Skip::new(iter, self.count, skip_index);
                        // println!("{:?} Skip nothing init {skip_index} {len} {:?} {skip_index}",std::thread::current().id(),self.count);
                        return val;
                    } // skip more elements that in iterator
                }
            }
            iter.advance_index(skip_index - in_start_i);
            let val = Skip::new(iter, self.count, skip_index - in_start_i);
            // println!("{:?} Skip init {skip_index} {len} {:?} {}",std::thread::current().id(),self.count,skip_index-in_start_i);
            val
        } else {
            iter.advance_index(len);
            let val = Skip::new(iter, self.count, in_start_i); // nothing to iterate so set len to 0
                                                               // println!("{:?} Skip nothing init {in_start_i} {len} {:?} {in_start_i}",std::thread::current().id(),self.count);
            val
        }
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        let val = self.iter.next();
        // println!("skip next {}",self.skip_index);
        // if val.is_some() {
        //     println!("{:?} Skip next {:?}",std::thread::current().id(),self.skip_index);
        // }
        // else {
        //     println!("{:?} Skip done",std::thread::current().id());
        // }
        val
    }
    fn elems(&self, in_elems: usize) -> usize {
        let in_elems = self.iter.elems(in_elems);
        // println!("skip elems {:?} {:?}",in_elems,std::cmp::max(0,in_elems-self.count));
        std::cmp::max(0, in_elems)
    }
    // fn global_index(&self, index: usize) -> Option<usize> {
    //     let g_index = self.iter.global_index(index);
    //     // println!("skip index: {:?} global_index {:?}", index,g_index);
    //     g_index
    // }
    // fn subarray_index(&self, index: usize) -> Option<usize> {
    //     let g_index = self.iter.subarray_index(index);
    //                                                    // println!("enumerate index: {:?} global_index {:?}", index,g_index);
    //     g_index
    // }

    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count);
        // println!("{:?} \t Skip advance index  {count}",std::thread::current().id());
    }
}

impl<I> IndexedDistributedIterator for Skip<I>
where
    I: IndexedDistributedIterator,
{
    fn iterator_index(&self, index: usize) -> Option<usize> {
        let Some(i_index) = self.iter.iterator_index(index + self.skip_index) else {
            // println!("{:?} \t Skip iterator index  {index} {} None",std::thread::current().id(),self.skip_index);
            return None;
        };

        let i_index = i_index as isize - self.count as isize;
        if i_index >= 0 {
            // println!("{:?} \t Skip iterator index  {index} {} {i_index}",std::thread::current().id(),self.skip_index);
            Some(i_index as usize)
        } else {
            // println!("{:?} \t Skip iterator index  {index} {} None",std::thread::current().id(),self.skip_index);
            None
        }
    }
}

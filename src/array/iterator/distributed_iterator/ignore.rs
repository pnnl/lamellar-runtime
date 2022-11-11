use crate::array::iterator::distributed_iterator::*;

//ignores the first n elements of iterator I per pe (this implys that n * num_pes elements are ignored in total)
#[derive(Clone, Debug)]
pub struct Ignore<I> {
    iter: I,
    count: usize,
    ignore_index: usize,
}

impl<I> Ignore<I>
where
    I: IndexedDistributedIterator,
{
    pub(crate) fn new(iter: I, count: usize, start_i: usize) -> Ignore<I> {
        // println!("new Ignore {:?} ",count);
        Ignore { iter, count, ignore_index: start_i }
    }
}



impl<I> DistributedIterator for Ignore<I>
where
    I: IndexedDistributedIterator,
{
    type Item = <I as DistributedIterator>::Item;
    type Array = <I as DistributedIterator>::Array;
    fn init(&self, in_start_i: usize, len: usize) -> Ignore<I> {
        let mut iter = self.iter.init(in_start_i, len);
        let mut ignore_index = in_start_i;

        //now we need to see how many elements to skip
        if let Some( mut iterator_index) = iter.iterator_index(ignore_index){
            while (iterator_index as isize - self.count as isize) < 0{
                ignore_index += 1;
                match iter.iterator_index(ignore_index) {
                    Some(i) => iterator_index = i,
                    None => {
                        iter.advance_index(len);
                        let val = Ignore::new(iter, self.count, ignore_index);
                        // println!("{:?} Ignore nothing init {ignore_index} {len} {:?} {ignore_index}",std::thread::current().id(),self.count);
                        return val
                    }, // ignore more elements that in iterator
                }
            }
            iter.advance_index(ignore_index-in_start_i);
            let val = Ignore::new(iter, self.count,ignore_index);
            // println!("{:?} Ignore init {ignore_index} {len} {:?} {ignore_index}",std::thread::current().id(),self.count);
            val
        }
        else {
            iter.advance_index(len);
            let val = Ignore::new(iter, self.count,in_start_i); // nothing to iterate so set len to 0
            // println!("{:?} Ignore nothing init {in_start_i} {len} {:?} {in_start_i}",std::thread::current().id(),self.count);
            val
        }
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        
        let val = self.iter.next();
        // println!("ignore next {}",self.ignore_index);
        // if val.is_some() {
        //     println!("{:?} Ignore next {:?}",std::thread::current().id(),self.ignore_index);
        // }
        // else {
        //     println!("{:?} Ignore done",std::thread::current().id());
        // }
        val
    }
    fn elems(&self, in_elems: usize) -> usize {
        let in_elems = self.iter.elems(in_elems);
        // println!("ignore elems {:?} {:?}",in_elems,std::cmp::max(0,in_elems-self.count));
        std::cmp::max(0, in_elems)
    }
    // fn global_index(&self, index: usize) -> Option<usize> {
    //     let g_index = self.iter.global_index(index);
    //     // println!("ignore index: {:?} global_index {:?}", index,g_index);
    //     g_index
    // }
    fn subarray_index(&self, index: usize) -> Option<usize> {
        let g_index = self.iter.subarray_index(index); 
                                                       // println!("enumerate index: {:?} global_index {:?}", index,g_index);
        g_index
    }

    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count);
        // println!("{:?} \t Ignore advance index  {count}",std::thread::current().id());
    }
}

impl<I> IndexedDistributedIterator for Ignore<I>
where
    I: IndexedDistributedIterator,
{
    fn iterator_index(&self, index: usize) -> Option<usize> {
       
        let Some(i_index) = self.iter.iterator_index(index+self.ignore_index) else {
            // println!("{:?} \t Ignore iterator index  {index} {} None",std::thread::current().id(),self.ignore_index);
            return None;
        }; 
       
        let i_index = i_index as isize - self.count as isize;
        if i_index >= 0 {
            // println!("{:?} \t Ignore iterator index  {index} {} {i_index}",std::thread::current().id(),self.ignore_index);
            Some(i_index as usize)
        }  
        else{
            // println!("{:?} \t Ignore iterator index  {index} {} None",std::thread::current().id(),self.ignore_index);
            None
        }
    }
}

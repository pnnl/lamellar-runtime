use crate::array::iterator::distributed_iterator::*;

#[derive(Clone)]
pub struct Filter<I,F> {
    iter: I,
    f: F
}
impl<I, F> Filter<I, F>
where
    I: DistributedIterator,
{
    pub(crate) fn new(iter: I, f: F) -> Filter<I,F> {
        // println!("new Filter {:?} ",count);
        Filter { iter, f }
    }
}

// impl<I,F> Filter<I,F>
// where
//     I: DistributedIterator + 'static,
//     F: FnMut(&I::Item) -> bool + AmLocal + Clone + 'static,
// {
//     pub fn for_each<G>(&self, op: G)
//     where
//         G: Fn(I::Item) + AmLocal  + Clone + 'static,
//     {
//         self.iter.array().for_each(self, op);
//     }
//     pub fn for_each_async<G, Fut>(&self, op: G)
//     where
//         G: Fn(I::Item) -> Fut + AmLocal  + Clone + 'static,
//         Fut: Future<Output = ()> + AmLocal + 'static,
//     {
//         self.iter.array().for_each_async(self, op);
//     }
// }

impl<I,F> DistributedIterator for Filter<I,F>
where
    I: DistributedIterator,
    F: FnMut(&I::Item) -> bool + AmLocal + Clone + 'static, 
{
    type Item = I::Item;
    type Array = <I as DistributedIterator>::Array;
    fn init(&self, start_i: usize, cnt: usize) -> Filter<I,F> {
        // println!("init enumerate start_i: {:?} cnt {:?} end_i {:?}",start_i, cnt, start_i+cnt );
        Filter::new(self.iter.init(start_i, cnt), self.f.clone())
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(next) = self.iter.next(){
            if (self.f)(&next) {
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
    fn global_index(&self, index: usize) -> Option<usize> {
        let g_index = self.iter.global_index(index);
        // println!("enumerate index: {:?} global_index {:?}", index,g_index);
        g_index
    }
    fn subarray_index(&self, index: usize) -> Option<usize> {
        let g_index = self.iter.subarray_index(index); //not sure if this works...
                                                       // println!("enumerate index: {:?} global_index {:?}", index,g_index);
        g_index
    }
    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count);
    }
}

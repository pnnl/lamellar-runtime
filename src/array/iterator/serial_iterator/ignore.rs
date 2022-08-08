use crate::array::iterator::serial_iterator::*;
use crate::array::LamellarArrayRequest;
use crate::LocalMemoryRegion;

pub struct Ignore<I> {
    iter: I,
}

impl<I> Ignore<I>
where
    I: SerialIterator,
{
    pub(crate) fn new(mut iter: I, count: usize) -> Self {
        iter.advance_index(count);
        Ignore { iter }
    }
}

impl<I> SerialIterator for Ignore<I>
where
    I: SerialIterator,
{
    type ElemType = I::ElemType;
    type Item = <I as SerialIterator>::Item;
    type Array = I::Array;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count);
    }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn item_size(&self) -> usize {
        self.iter.item_size()
    }
    fn buffered_next(
        &mut self,
        mem_region: LocalMemoryRegion<u8>,
    ) -> Option<Box<dyn LamellarArrayRequest<Output = ()>>> {
        self.iter.buffered_next(mem_region)
    }
    fn from_mem_region(&self, mem_region: LocalMemoryRegion<u8>) -> Option<Self::Item> {
        self.iter.from_mem_region(mem_region)
    }
}

// impl<I>  Iterator
// for Ignore<I>
// where
//     I: SerialIterator+Iterator,
// {
//     type Item = <I as SerialIterator>::Item;
//     fn next(&mut self) -> Option<Self::Item> {
//         <Self as SerialIterator>::next(self)
//     }
// }

// impl<I> Stream for Ignore<I>
// where
//     I: SerialIterator + Stream + Unpin
// {
//     type Item = <I as Stream>::Item;
//     fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         self.iter.poll_next(cx)
//     }
// }

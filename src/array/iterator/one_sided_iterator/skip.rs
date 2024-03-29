use crate::array::iterator::one_sided_iterator::*;
// use crate::array::LamellarArrayRequest;
// use crate::memregion::OneSidedMemoryRegion;

// use async_trait::async_trait;
use pin_project::pin_project;

#[pin_project]
pub struct Skip<I> {
    #[pin]
    iter: I,
}

impl<I> Skip<I>
where
    I: OneSidedIterator + Send,
{
    pub(crate) fn new(mut iter: I, count: usize) -> Self {
        iter.advance_index(count);
        Skip { iter }
    }
}

impl<I> OneSidedIterator for Skip<I>
where
    I: OneSidedIterator + Send,
{
    type ElemType = I::ElemType;
    type Item = <I as OneSidedIterator>::Item;
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
    // fn buffered_next(
    //     &mut self,
    //     mem_region: OneSidedMemoryRegion<u8>,
    // ) -> Option<Box<dyn LamellarArrayRequest<Output = ()>>> {
    //     self.iter.buffered_next(mem_region)
    // }
    // fn from_mem_region(&self, mem_region: OneSidedMemoryRegion<u8>) -> Option<Self::Item> {
    //     self.iter.from_mem_region(mem_region)
    // }
}

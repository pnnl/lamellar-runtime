use crate::array::iterator::one_sided_iterator::{private::*, *};
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

impl<I> OneSidedIterator for Skip<I> where I: OneSidedIterator + Send {}

impl<I> OneSidedIteratorInner for Skip<I>
where
    I: OneSidedIterator + Send,
{
    type ElemType = I::ElemType;
    type Item = <I as OneSidedIteratorInner>::Item;
    type Array = I::Array;

    fn init(&mut self) {
        self.iter.init()
    }

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().iter.poll_next(cx)
    }

    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count);
    }

    fn advance_index_pin(self: Pin<&mut Self>, count: usize) {
        // println!("skipping {count}");
        self.project().iter.advance_index_pin(count);
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

use crate::array::iterator::one_sided_iterator::*;
// use crate::array::LamellarArrayRequest;
// use crate::memregion::OneSidedMemoryRegion;

// use async_trait::async_trait;
use pin_project::pin_project;

#[pin_project]
pub struct StepBy<I> {
    #[pin]
    iter: I,
    step_size: usize,
}

impl<I> StepBy<I>
where
    I: OneSidedIterator + Send,
{
    pub(crate) fn new(iter: I, step_size: usize) -> Self {
        StepBy { iter, step_size }
    }
}

impl<I> OneSidedIterator for StepBy<I>
where
    I: OneSidedIterator + Send,
{
    type ElemType = I::ElemType;
    type Item = <I as OneSidedIterator>::Item;
    type Array = I::Array;
    fn next(&mut self) -> Option<Self::Item> {
        let res = self.iter.next()?;
        self.iter.advance_index(self.step_size - 1);
        Some(res)
    }
    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count * self.step_size);
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
    //     let res = self.iter.buffered_next(mem_region)?;
    //     self.iter.advance_index(self.step_size - 1);
    //     Some(res)
    // }
    // fn from_mem_region(&self, mem_region: OneSidedMemoryRegion<u8>) -> Option<Self::Item> {
    //     self.iter.from_mem_region(mem_region)
    // }
}

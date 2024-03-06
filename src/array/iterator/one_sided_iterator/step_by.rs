use crate::array::iterator::one_sided_iterator::{private::*, *};
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

impl<I> OneSidedIterator for StepBy<I> where I: OneSidedIterator + Send {}

impl<I> OneSidedIteratorInner for StepBy<I>
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
        let res = self.iter.next()?;
        self.iter.advance_index(self.step_size - 1);
        Some(res)
    }

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.iter.as_mut().poll_next(cx) {
            Poll::Ready(Some(res)) => {
                // println!("step by {:?}", *this.step_size);
                this.iter.advance_index_pin(*this.step_size - 1);
                Poll::Ready(Some(res))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => {
                // println!("step by pending");
                Poll::Pending
            }
        }
    }
    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count * self.step_size);
    }
    fn advance_index_pin(self: Pin<&mut Self>, count: usize) {
        let step_size = self.step_size;
        self.project().iter.advance_index_pin(count * step_size);
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

use crate::array::iterator::one_sided_iterator::*;
use crate::array::LamellarArrayRequest;
use crate::memregion::OneSidedMemoryRegion;

use async_trait::async_trait;
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

#[async_trait]
// impl<I> SerialAsyncIterator for StepBy<I>
// where
//     I: OneSidedIterator + SerialAsyncIterator,
// {
//     type ElemType = <I as SerialAsyncIterator>::ElemType;
//     type Item = <I as SerialAsyncIterator>::Item;
//     type Array = <I as SerialAsyncIterator>::Array;
//     async fn async_next(self: Pin<&mut Self>) -> Option<Self::Item> {
//         let res = self.iter.async_next().await?;
//         self.iter.advance_index(self.step_size - 1);
//         Some(res)
//     }
// }
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
    // async fn async_next(mut self: Pin<&mut Self>) -> Option<Self::Item> {
    //     // println!("async_next step_by");
    //     let this = self.as_mut().project();
    //     let res = this.iter.async_next().await?;
    //     let step_size = *this.step_size;
    //     self.as_mut()
    //         .project()
    //         .iter
    //         .async_advance_index(step_size - 1)
    //         .await;
    //     Some(res)
    // }
    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count * self.step_size);
    }
    // async fn async_advance_index(mut self: Pin<&mut Self>, count: usize) {
    //     let this = self.project();
    //     this.iter.async_advance_index(count * *this.step_size).await
    // }
    fn array(&self) -> Self::Array {
        self.iter.array()
    }
    fn item_size(&self) -> usize {
        self.iter.item_size()
    }
    fn buffered_next(
        &mut self,
        mem_region: OneSidedMemoryRegion<u8>,
    ) -> Option<Box<dyn LamellarArrayRequest<Output = ()>>> {
        let res = self.iter.buffered_next(mem_region)?;
        self.iter.advance_index(self.step_size - 1);
        Some(res)
    }
    // async fn async_buffered_next(
    //     mut self: Pin<&mut Self>,
    //     mem_region: OneSidedMemoryRegion<u8>,
    // ) -> Option<Box<dyn LamellarArrayRequest<Output = ()>>> {
    //     let this = self.as_mut().project();
    //     let res = this.iter.async_buffered_next(mem_region).await?;
    //     let this = self.as_mut().project();
    //     this.iter.async_advance_index(*this.step_size - 1).await;
    //     Some(res)
    // }
    fn from_mem_region(&self, mem_region: OneSidedMemoryRegion<u8>) -> Option<Self::Item> {
        self.iter.from_mem_region(mem_region)
    }
}

// impl<I> Iterator for StepBy<I>
// where
//     I: OneSidedIterator + Iterator,
// {
//     type Item = <I as OneSidedIterator>::Item;
//     fn next(&mut self) -> Option<Self::Item> {
//         <Self as OneSidedIterator>::next(self)
//     }
// }

// impl<I> Stream for StepBy<I>
// where
//     I: OneSidedIterator + Stream + Unpin
// {
//     type Item = <I as Stream>::Item;
//     fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         let res = self.iter.poll_next(cx);
//         let count = self.step_size-1;
//         self.iter.advance_index(count);
//         res
//     }
// }

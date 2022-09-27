use crate::array::iterator::serial_iterator::*;
use crate::array::LamellarArrayRequest;
use crate::LocalMemoryRegion;

use async_trait::async_trait;
use pin_project::pin_project;

#[pin_project]
pub struct Ignore<I> {
    #[pin]
    iter: I,
}

impl<I> Ignore<I>
where
    I: SerialIterator + Send,
{
    pub(crate) fn new(mut iter: I, count: usize) -> Self {
        iter.advance_index(count);
        Ignore { iter }
    }
}

#[async_trait]
// impl<I> SerialAsyncIterator for Ignore<I>
// where
//     I: SerialIterator + SerialAsyncIterator,
// {
//     type ElemType = <I as SerialAsyncIterator>::ElemType;
//     type Item = <I as SerialAsyncIterator>::Item;
//     type Array = <I as SerialAsyncIterator>::Array;
//     async fn async_next(self: Pin<&mut Self>) -> Option<Self::Item> {
//         self.iter.async_next().await
//     }
// }
impl<I> SerialIterator for Ignore<I>
where
    I: SerialIterator + Send,
{
    type ElemType = I::ElemType;
    type Item = <I as SerialIterator>::Item;
    type Array = I::Array;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
    async fn async_next(self: Pin<&mut Self>) -> Option<Self::Item> {
        // println!("async_next ignore");
        let this = self.project();
        this.iter.async_next().await
    }

    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count);
    }
    async fn async_advance_index(mut self: Pin<&mut Self>, count: usize) {
        self.project().iter.async_advance_index(count).await
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
    async fn async_buffered_next(
        mut self: Pin<&mut Self>,
        mem_region: LocalMemoryRegion<u8>,
    ) -> Option<Box<dyn LamellarArrayRequest<Output = ()>>> {
        self.project().iter.async_buffered_next(mem_region).await
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

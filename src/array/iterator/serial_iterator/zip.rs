use crate::array::iterator::serial_iterator::*;
use crate::array::LamellarArrayRequest;
use crate::LocalMemoryRegion;

use async_trait::async_trait;
use futures::join;
use pin_project::pin_project;

struct ZipBufferedReq {
    reqs: Vec<Box<dyn LamellarArrayRequest<Output = ()>>>,
}

#[async_trait]
impl LamellarArrayRequest for ZipBufferedReq {
    type Output = ();
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        for req in self.reqs.drain(0..) {
            req.into_future().await;
        }
        ()
    }
    fn wait(mut self: Box<Self>) -> Self::Output {
        for req in self.reqs.drain(0..) {
            req.wait();
        }
        ()
    }
}

#[pin_project]
pub struct Zip<A, B> {
    #[pin]
    a: A,
    #[pin]
    b: B,
}

impl<A, B> Zip<A, B>
where
    A: SerialIterator + Send,
    B: SerialIterator + Send,
{
    pub(crate) fn new(a: A, b: B) -> Self {
        Zip { a, b }
    }
}

#[async_trait]
// impl<A, B> SerialAsyncIterator for Zip<A, B>
// where
//     A: SerialIterator + SerialAsyncIterator,
//     B: SerialIterator + SerialAsyncIterator,
// {
//     type ElemType = <A as SerialAsyncIterator>::ElemType;
//     type Item = (
//         <A as SerialAsyncIterator>::Item,
//         <B as SerialAsyncIterator>::Item,
//     );
//     type Array = <A as SerialAsyncIterator>::Array;
//     async fn async_next(self: Pin<&mut Self>) -> Option<Self::Item> {
//         let a = self.a.async_next().await?;
//         let b = self.b.async_next().await?;
//         Some((a, b))
//     }
// }
impl<A, B> SerialIterator for Zip<A, B>
where
    A: SerialIterator + Send,
    B: SerialIterator + Send,
{
    type ElemType = A::ElemType;
    type Item = (<A as SerialIterator>::Item, <B as SerialIterator>::Item);
    type Array = A::Array;
    fn next(&mut self) -> Option<Self::Item> {
        let a = self.a.next()?;
        let b = self.b.next()?;
        Some((a, b))
    }
    async fn async_next(self: Pin<&mut Self>) -> Option<Self::Item> {
        // println!("async_next zip");
        let mut this = self.project();
        let a = this.a.async_next(); //.await?;
        let b = this.b.async_next(); //.await?;

        let a_b = join!(a, b);
        Some((a_b.0?, a_b.1?))
    }
    fn advance_index(&mut self, count: usize) {
        self.a.advance_index(count);
        self.b.advance_index(count);
    }
    async fn async_advance_index(mut self: Pin<&mut Self>, count: usize) {
        let this = self.project();
        let a = this.a.async_advance_index(count);
        let b = this.b.async_advance_index(count);
        join!(a, b);
    }
    fn array(&self) -> Self::Array {
        self.a.array()
    }
    fn item_size(&self) -> usize {
        self.a.item_size() + self.b.item_size()
    }
    fn buffered_next(
        &mut self,
        mem_region: LocalMemoryRegion<u8>,
    ) -> Option<Box<dyn LamellarArrayRequest<Output = ()>>> {
        let a_sub_region = mem_region.sub_region(0..self.a.item_size());
        let mut reqs = vec![];
        reqs.push(self.a.buffered_next(a_sub_region)?);
        let b_sub_region =
            mem_region.sub_region(self.a.item_size()..self.a.item_size() + self.b.item_size());

        reqs.push(self.b.buffered_next(b_sub_region)?);
        Some(Box::new(ZipBufferedReq { reqs }))
    }
    async fn async_buffered_next(
        mut self: Pin<&mut Self>,
        mem_region: LocalMemoryRegion<u8>,
    ) -> Option<Box<dyn LamellarArrayRequest<Output = ()>>> {
        let this = self.as_mut().project();
        let a_sub_region = mem_region.sub_region(0..this.a.item_size());
        let mut reqs = vec![];

        reqs.push(this.a.async_buffered_next(a_sub_region).await?);
        let this = self.as_mut().project();
        let b_sub_region =
            mem_region.sub_region(this.a.item_size()..this.a.item_size() + this.b.item_size());

        reqs.push(this.b.async_buffered_next(b_sub_region).await?);
        Some(Box::new(ZipBufferedReq { reqs }))
    }
    fn from_mem_region(&self, mem_region: LocalMemoryRegion<u8>) -> Option<Self::Item> {
        let a_sub_region = mem_region.sub_region(0..self.a.item_size());
        let a = self.a.from_mem_region(a_sub_region)?;
        let b_sub_region =
            mem_region.sub_region(self.a.item_size()..self.a.item_size() + self.b.item_size());
        let b = self.b.from_mem_region(b_sub_region)?;
        Some((a, b))
    }
}

// impl<A, B>  Iterator
// for Zip<A, B>
// where
//     I: SerialIterator+Iterator,
// {
//     type Item = <I as SerialIterator>::Item;
//     fn next(&mut self) -> Option<Self::Item> {
//         <Self as SerialIterator>::next(self)
//     }
// }

// impl<A, B> Stream for Zip<A, B>
// where
//     I: SerialIterator + Stream + Unpin
// {
//     type Item = <I as Stream>::Item;
//     fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         self.iter.poll_next(cx)
//     }
// }

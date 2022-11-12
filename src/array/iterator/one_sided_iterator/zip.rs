use crate::array::iterator::one_sided_iterator::*;
// use crate::array::LamellarArrayRequest;
// use crate::memregion::OneSidedMemoryRegion;

// use async_trait::async_trait;
use pin_project::pin_project;

// struct ZipBufferedReq {
//     reqs: Vec<Box<dyn LamellarArrayRequest<Output = ()>>>,
// }

// impl LamellarArrayRequest for ZipBufferedReq {
//     type Output = ();
//     async fn into_future(mut self: Box<Self>) -> Self::Output {
//         for req in self.reqs.drain(0..) {
//             req.into_future().await;
//         }
//         ()
//     }
//     fn wait(mut self: Box<Self>) -> Self::Output {
//         for req in self.reqs.drain(0..) {
//             req.wait();
//         }
//         ()
//     }
// }

#[pin_project]
pub struct Zip<A, B> {
    #[pin]
    a: A,
    #[pin]
    b: B,
}

impl<A, B> Zip<A, B>
where
    A: OneSidedIterator + Send,
    B: OneSidedIterator + Send,
{
    pub(crate) fn new(a: A, b: B) -> Self {
        Zip { a, b }
    }
}

impl<A, B> OneSidedIterator for Zip<A, B>
where
    A: OneSidedIterator + Send,
    B: OneSidedIterator + Send,
{
    type ElemType = A::ElemType;
    type Item = (<A as OneSidedIterator>::Item, <B as OneSidedIterator>::Item);
    type Array = A::Array;
    fn next(&mut self) -> Option<Self::Item> {
        let a = self.a.next()?;
        let b = self.b.next()?;
        Some((a, b))
    }
    fn advance_index(&mut self, count: usize) {
        self.a.advance_index(count);
        self.b.advance_index(count);
    }
    fn array(&self) -> Self::Array {
        self.a.array()
    }
    fn item_size(&self) -> usize {
        self.a.item_size() + self.b.item_size()
    }
    // fn buffered_next(
    //     &mut self,
    //     mem_region: OneSidedMemoryRegion<u8>,
    // ) -> Option<Box<dyn LamellarArrayRequest<Output = ()>>> {
    //     let a_sub_region = mem_region.sub_region(0..self.a.item_size());
    //     let mut reqs = vec![];
    //     reqs.push(self.a.buffered_next(a_sub_region)?);
    //     let b_sub_region =
    //         mem_region.sub_region(self.a.item_size()..self.a.item_size() + self.b.item_size());

    //     reqs.push(self.b.buffered_next(b_sub_region)?);
    //     Some(Box::new(ZipBufferedReq { reqs }))
    // }
    // fn from_mem_region(&self, mem_region: OneSidedMemoryRegion<u8>) -> Option<Self::Item> {
    //     let a_sub_region = mem_region.sub_region(0..self.a.item_size());
    //     let a = self.a.from_mem_region(a_sub_region)?;
    //     let b_sub_region =
    //         mem_region.sub_region(self.a.item_size()..self.a.item_size() + self.b.item_size());
    //     let b = self.b.from_mem_region(b_sub_region)?;
    //     Some((a, b))
    // }
}

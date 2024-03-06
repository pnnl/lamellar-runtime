use crate::array::iterator::one_sided_iterator::{private::*, *};
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
pub struct Zip<A, B>
where
    A: OneSidedIterator + Send,
    B: OneSidedIterator + Send,
{
    #[pin]
    a: A,
    #[pin]
    b: B,
    state: ZipState<<A as OneSidedIteratorInner>::Item, <B as OneSidedIteratorInner>::Item>,
}

enum ZipState<A, B> {
    Pending,
    Finished,
    AReady(A),
    BReady(B),
}

impl<A, B> Zip<A, B>
where
    A: OneSidedIterator + Send,
    B: OneSidedIterator + Send,
{
    pub(crate) fn new(a: A, b: B) -> Self {
        Zip {
            a,
            b,
            state: ZipState::Pending,
        }
    }
}

impl<A, B> OneSidedIterator for Zip<A, B>
where
    A: OneSidedIterator + Send,
    B: OneSidedIterator + Send,
{
}

impl<A, B> OneSidedIteratorInner for Zip<A, B>
where
    A: OneSidedIterator + Send,
    B: OneSidedIterator + Send,
{
    type ElemType = A::ElemType;
    type Item = (
        <A as OneSidedIteratorInner>::Item,
        <B as OneSidedIteratorInner>::Item,
    );
    type Array = A::Array;

    fn init(&mut self) {
        self.a.init();
        self.b.init();
    }
    fn next(&mut self) -> Option<Self::Item> {
        let a = self.a.next()?;
        let b = self.b.next()?;
        Some((a, b))
    }

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut cur_state = ZipState::Pending;
        let this = self.project();
        std::mem::swap(&mut *this.state, &mut cur_state);

        match cur_state {
            ZipState::Pending => {
                let a = this.a.poll_next(cx);
                let b = this.b.poll_next(cx);
                match (a, b) {
                    (Poll::Ready(a), Poll::Ready(b)) => {
                        if a.is_none() || b.is_none() {
                            *this.state = ZipState::Finished;
                            return Poll::Ready(None);
                        }
                        *this.state = ZipState::Pending;
                        Poll::Ready(Some((a.unwrap(), b.unwrap())))
                    }
                    (Poll::Ready(a), Poll::Pending) => match a {
                        Some(a) => {
                            *this.state = ZipState::AReady(a);
                            Poll::Pending
                        }
                        None => {
                            *this.state = ZipState::Finished;
                            Poll::Ready(None)
                        }
                    },
                    (Poll::Pending, Poll::Ready(b)) => match b {
                        Some(b) => {
                            *this.state = ZipState::BReady(b);
                            Poll::Pending
                        }
                        None => {
                            *this.state = ZipState::Finished;
                            Poll::Ready(None)
                        }
                    },
                    (Poll::Pending, Poll::Pending) => Poll::Pending,
                }
            }
            ZipState::AReady(a) => match this.b.poll_next(cx) {
                Poll::Ready(b) => match b {
                    Some(b) => {
                        *this.state = ZipState::Pending;
                        Poll::Ready(Some((a, b)))
                    }
                    None => {
                        *this.state = ZipState::Finished;
                        Poll::Ready(None)
                    }
                },
                Poll::Pending => Poll::Pending,
            },
            ZipState::BReady(b) => match this.a.poll_next(cx) {
                Poll::Ready(a) => match a {
                    Some(a) => {
                        *this.state = ZipState::Pending;
                        Poll::Ready(Some((a, b)))
                    }
                    None => {
                        *this.state = ZipState::Finished;
                        Poll::Ready(None)
                    }
                },
                Poll::Pending => Poll::Pending,
            },
            ZipState::Finished => Poll::Ready(None),
        }
    }
    fn advance_index(&mut self, count: usize) {
        self.a.advance_index(count);
        self.b.advance_index(count);
    }
    fn advance_index_pin(self: Pin<&mut Self>, count: usize) {
        let this = self.project();
        this.a.advance_index_pin(count);
        this.b.advance_index_pin(count);
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

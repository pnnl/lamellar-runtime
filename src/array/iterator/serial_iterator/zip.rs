use crate::array::iterator::serial_iterator::*;

pub struct Zip<A, B> {
    a: A,
    b: B,
}

impl<A, B> Zip<A, B>
where
    A: SerialIterator,
    B: SerialIterator,
{
    pub(crate) fn new(a: A, b: B) -> Self {
        Zip { a,b }
    }
}

impl<A, B> SerialIterator for Zip<A, B>
where
    A: SerialIterator,
    B: SerialIterator,
{
    type ElemType = A::ElemType;
    type Item = (<A as SerialIterator>::Item,<B as SerialIterator>::Item);
    type Array = A::Array;
    fn next(&mut self) -> Option<Self::Item> {
        let a = self.a.next()?;
        let b = self.b.next()?;
        Some((a,b))
    }
    fn advance_index(&mut self, count: usize) {
        self.a.advance_index(count);
        self.b.advance_index(count);
    }
    fn array(&self) -> Self::Array {
        self.a.array()
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

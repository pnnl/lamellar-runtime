use crate::array::iterator::serial_iterator::*;

pub struct StepBy<I> {
    iter: I,
    step_size: usize,
}

impl<I> StepBy<I>
where
    I: SerialIterator,
{
    pub(crate) fn new(iter: I, step_size: usize) -> Self {
        StepBy { iter, step_size }
    }
}
impl<I> SerialIterator for StepBy<I>
where
    I: SerialIterator,
{
    type ElemType = I::ElemType;
    type Item = <I as SerialIterator>::Item;
    fn next(&mut self) -> Option<Self::Item> {
        let res = self.iter.next()?;
        self.iter.advance_index(self.step_size - 1);
        Some(res)
    }
    fn advance_index(&mut self, count: usize) {
        self.iter.advance_index(count * self.step_size);
    }
    fn array(&self) -> LamellarArray<Self::ElemType> {
        self.iter.array()
    }
}

// impl<I> Iterator for StepBy<I>
// where
//     I: SerialIterator + Iterator,
// {
//     type Item = <I as SerialIterator>::Item;
//     fn next(&mut self) -> Option<Self::Item> {
//         <Self as SerialIterator>::next(self)
//     }
// }

// impl<I> Stream for StepBy<I>
// where
//     I: SerialIterator + Stream + Unpin
// {
//     type Item = <I as Stream>::Item;
//     fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         let res = self.iter.poll_next(cx);
//         let count = self.step_size-1;
//         self.iter.advance_index(count);
//         res
//     }
// }

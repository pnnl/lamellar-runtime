use crate::array::iterator::distributed_iterator::*;
use crate::array::iterator::local_iterator::*;
use crate::array::iterator::one_sided_iterator::OneSidedIter;
use crate::array::iterator::LamellarArrayIterators;
use crate::array::r#unsafe::private::UnsafeArrayInner;
use crate::array::read_only::*;
use crate::array::*;
use crate::memregion::Dist;

impl<T> InnerArray for ReadOnlyArray<T> {
    fn as_inner(&self) -> &UnsafeArrayInner {
        &self.array.inner
    }
}

impl<T: Dist> LamellarArrayIterators<T> for ReadOnlyArray<T> {
    // type Array = ReadOnlyArray<T>;
    type DistIter = DistIter<'static, T, Self>;
    type LocalIter = LocalIter<'static, T, Self>;
    type OnesidedIter = OneSidedIter<T, Self>;
    fn dist_iter(&self) -> Self::DistIter {
        DistIter::new(self.clone().into(), 0, 0)
    }

    fn local_iter(&self) -> Self::LocalIter {
        LocalIter::new(self.clone().into(), 0, 0)
    }

    fn onesided_iter(&self) -> Self::OnesidedIter {
        OneSidedIter::new(self, 1)
    }

    fn buffered_onesided_iter(&self, buf_size: usize) -> Self::OnesidedIter {
        OneSidedIter::new(self, std::cmp::min(buf_size, self.len()))
    }
}

impl<T: Dist> DistIteratorLauncher for ReadOnlyArray<T> {}
impl<T: Dist> LocalIteratorLauncher for ReadOnlyArray<T> {}

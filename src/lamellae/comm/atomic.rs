use crate::{lamellae::comm::rdma::RdmaHandle};

use super::Remote;
pub(crate) trait NetworkAtomic: Remote {}
impl NetworkAtomic for u8 {}
impl NetworkAtomic for u16 {}
impl NetworkAtomic for u32 {}
impl NetworkAtomic for u64 {}
impl NetworkAtomic for usize {}
impl NetworkAtomic for i8 {}
impl NetworkAtomic for i16 {}
impl NetworkAtomic for i32 {}
impl NetworkAtomic for i64 {}
impl NetworkAtomic for isize {}

pub(crate) enum AtomicOp<'a, T: NetworkAtomic> {
    Min(&'a [T]),
    Max(&'a [T]),
    Sum(&'a [T]),
    Prod(&'a [T]),
    LogicalOr(&'a [T]),
    LogicalXor(&'a [T]),
    LogicalAnd(&'a [T]),
    BitOr(&'a [T]),
    BitXor(&'a [T]),
    BitAnd(&'a [T]),
    Read,
    Write(&'a [T]),
    Cas(&'a [T], &'a [T]),
}

pub(crate) trait CommAtomic {
    fn atomic_avail<T>(&self) -> bool
    where
        Self: Sized;

    fn atomic_op<T: NetworkAtomic>(
        &self,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: usize,
    ) -> RdmaHandle<T>;
    fn atomic_fetch_op<T: NetworkAtomic>(
        &self,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: usize,
        result: &mut [T],
    ) -> RdmaHandle<T>;
}

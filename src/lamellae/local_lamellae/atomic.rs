use crate::lamellae::comm::{
    atomic::{AtomicOp, CommAtomic, NetworkAtomic},
    rdma::RdmaHandle,
};

use super::{
    comm::LocalComm,
    rdma::{LocalFuture, Op},
};

impl CommAtomic for LocalComm {
    fn atomic_avail<T>(&self) -> bool {
        false
    }
    fn atomic_op<T: NetworkAtomic>(
        &self,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: usize,
    ) -> RdmaHandle<T> {
        LocalFuture { op: Op::Atomic }.into()
    }
    fn atomic_fetch_op<T: NetworkAtomic>(
        &self,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: usize,
        result: &mut [T],
    ) -> RdmaHandle<T> {
        LocalFuture { op: Op::Atomic }.into()
    }
}

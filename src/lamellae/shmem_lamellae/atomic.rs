use crate::lamellae::comm::{
    atomic::{AtomicOp, CommAtomic, NetworkAtomic},
    rdma::RdmaHandle,
};

use super::{
    comm::ShmemComm,
    rdma::{Op, ShmemFuture},
};

impl CommAtomic for ShmemComm {
    fn atomic_avail<T>(&self) -> bool {
        false
    }
    fn atomic_op<T: NetworkAtomic>(
        &self,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: usize,
    ) -> RdmaHandle<T> {
        ShmemFuture { op: Op::Atomic }.into()
    }
    fn atomic_fetch_op<T: NetworkAtomic>(
        &self,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: usize,
        result: &mut [T],
    ) -> RdmaHandle<T> {
        ShmemFuture { op: Op::Atomic }.into()
    }
}

use crate::lamellae::comm::{
    atomic::{AtomicOp, CommAtomic, NetworkAtomic},
    rdma::RdmaFuture,
};

use super::{comm::ShmemComm, rdma::ShmemFuture};

impl CommAtomic for ShmemComm {
    fn atomic_avail<T>(&self) -> bool {
        false
    }
    fn atomic_op<T: NetworkAtomic>(
        &self,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: usize,
    ) -> RdmaFuture {
        ShmemFuture {}.into()
    }
    fn atomic_fetch_op<T: NetworkAtomic>(
        &self,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: usize,
        result: &mut [T],
    ) -> RdmaFuture {
        ShmemFuture {}.into()
    }
}

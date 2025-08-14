use std::sync::Arc;

use crate::{
    active_messaging::AMCounters,
    lamellae::comm::{
        atomic::{AtomicOp, CommAtomic, NetworkAtomic},
        rdma::RdmaHandle,
    },
};

use super::{
    comm::LibfabricComm,
    rdma::{LibfabricFuture, Op},
    Scheduler,
};

impl CommAtomic for LibfabricComm {
    fn atomic_avail<T>(&self) -> bool {
        false
    }
    fn atomic_op<T: NetworkAtomic>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: usize,
    ) -> RdmaHandle<T> {
        LibfabricFuture {
            my_pe: self.my_pe,
            ofi: self.ofi.clone(),
            op: Op::Atomic,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn atomic_fetch_op<T: NetworkAtomic>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: usize,
        result: &mut [T],
    ) -> RdmaHandle<T> {
        LibfabricFuture {
            my_pe: self.my_pe,
            ofi: self.ofi.clone(),
            op: Op::Atomic,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
}

use crate::{
    active_messaging::AMCounters,
    lamellae::comm::{
        atomic::{AtomicOp, CommAtomic, NetworkAtomic},
        rdma::RdmaHandle,
    },
};

use super::{
    comm::LocalComm,
    rdma::{LocalFuture, Op},
    Scheduler,
};

use std::sync::Arc;

impl CommAtomic for LocalComm {
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
        LocalFuture {
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
        LocalFuture {
            op: Op::Atomic,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
}

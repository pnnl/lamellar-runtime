use std::sync::Arc;

use crate::{
    active_messaging::AMCounters,
    lamellae::comm::{
        atomic::{AtomicOp, CommAtomic, NetworkAtomic},
        rdma::RdmaHandle,
    },
};

use super::{
    comm::RofiCComm,
    rdma::{Op, RofiCFuture},
    Scheduler,
};

impl CommAtomic for RofiCComm {
    fn atomic_avail<T: 'static>(&self) -> bool {
        false
    }
    fn atomic_op<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: usize,
    ) -> RdmaHandle<T> {
        RofiCFuture {
            my_pe: self.my_pe,
            op: Op::Atomic,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn atomic_fetch_op<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: usize,
    ) -> RdmaHandle<T> {
        RofiCFuture {
            my_pe: self.my_pe,
            op: Op::Atomic,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
}

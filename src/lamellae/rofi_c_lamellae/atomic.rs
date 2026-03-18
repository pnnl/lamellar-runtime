use std::sync::Arc;

use crate::active_messaging::AMCounters;
use crate::lamellae::comm::atomic::{
    AtomicFetchOpHandle, AtomicOp, AtomicOpHandle, CommAllocAtomic,
};
use crate::LamellarTask;
use crate::Remote;

use super::{
    fabric::{OneSidedRofiCAlloc, RofiCAlloc},
    Scheduler,
};

impl CommAllocAtomic for RofiCAlloc {
    fn atomic_op<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        _counters: Vec<Arc<AMCounters>>,
        _op: AtomicOp<T>,
        _pe: usize,
        _offset: usize,
    ) -> AtomicOpHandle<T> {
        unimplemented!("atomic operations not implemented for rofi-c backend")
    }

    fn atomic_op_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        _op: AtomicOp<T>,
        _pe: usize,
        _offset: usize,
    ) {
        unimplemented!("atomic operations not implemented for rofi-c backend")
    }

    fn atomic_op_unmanaged<T: Remote>(&self, _op: AtomicOp<T>, _pe: usize, _offset: usize) {
        unimplemented!("atomic operations not implemented for rofi-c backend")
    }

    fn atomic_op_all<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        _counters: Vec<Arc<AMCounters>>,
        _op: AtomicOp<T>,
        _offset: usize,
    ) -> AtomicOpHandle<T> {
        unimplemented!("atomic operations not implemented for rofi-c backend")
    }

    fn atomic_op_all_unmanaged<T: Remote>(&self, _op: AtomicOp<T>, _offset: usize) {
        unimplemented!("atomic operations not implemented for rofi-c backend")
    }

    fn atomic_fetch_op<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        _counters: Vec<Arc<AMCounters>>,
        _op: AtomicOp<T>,
        _pe: usize,
        _offset: usize,
    ) -> AtomicFetchOpHandle<T> {
        unimplemented!("atomic fetch operations not implemented for rofi-c backend")
    }

    fn atomic_fetch_op_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        _op: AtomicOp<T>,
        _pe: usize,
        _offset: usize,
    ) -> T {
        unimplemented!("atomic fetch operations not implemented for rofi-c backend")
    }
}

impl CommAllocAtomic for OneSidedRofiCAlloc {
    fn atomic_op<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        _counters: Vec<Arc<AMCounters>>,
        _op: AtomicOp<T>,
        _pe: usize,
        _offset: usize,
    ) -> AtomicOpHandle<T> {
        unimplemented!("atomic operations not implemented for rofi-c backend")
    }

    fn atomic_op_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        _op: AtomicOp<T>,
        _pe: usize,
        _offset: usize,
    ) {
        unimplemented!("atomic operations not implemented for rofi-c backend")
    }

    fn atomic_op_unmanaged<T: Remote>(&self, _op: AtomicOp<T>, _pe: usize, _offset: usize) {
        unimplemented!("atomic operations not implemented for rofi-c backend")
    }

    fn atomic_op_all<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        _counters: Vec<Arc<AMCounters>>,
        _op: AtomicOp<T>,
        _offset: usize,
    ) -> AtomicOpHandle<T> {
        unimplemented!("atomic operations not implemented for rofi-c backend")
    }

    fn atomic_op_all_unmanaged<T: Remote>(&self, _op: AtomicOp<T>, _offset: usize) {
        unimplemented!("atomic operations not implemented for rofi-c backend")
    }

    fn atomic_fetch_op<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        _counters: Vec<Arc<AMCounters>>,
        _op: AtomicOp<T>,
        _pe: usize,
        _offset: usize,
    ) -> AtomicFetchOpHandle<T> {
        unimplemented!("atomic fetch operations not implemented for rofi-c backend")
    }

    fn atomic_fetch_op_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        _op: AtomicOp<T>,
        _pe: usize,
        _offset: usize,
    ) -> T {
        unimplemented!("atomic fetch operations not implemented for rofi-c backend")
    }
}

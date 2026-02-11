use std::sync::Arc;

use crate::active_messaging::AMCounters;
use crate::lamellae::comm::atomic::{AtomicFetchOpHandle, AtomicOp, AtomicOpHandle, CommAllocAtomic};
use crate::LamellarTask;
use crate::Remote;

use super::{fabric::{RofiCAlloc, OneSidedRofiCAlloc}, Scheduler};

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

    fn blocking_atomic_fetch_op<T: Remote>(&self, _op: AtomicOp<T>, _pe: usize, _offset: usize) -> T {
        unimplemented!("atomic fetch operations not implemented for rofi-c backend")
    }
}

impl CommAllocAtomic for OneSidedRofiCAlloc {
    fn atomic_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        // One-sided alloc should target its remote_pe; validate or forward
        assert_eq!(pe, self.alloc.my_pe, "atomic op called on OneSidedRofiCAlloc with incorrect pe");
        self.alloc.atomic_op(scheduler, counters, op, pe, offset)
    }

    fn atomic_op_unmanaged<T: Remote>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        assert_eq!(pe, self.alloc.my_pe, "atomic op called on OneSidedRofiCAlloc with incorrect pe");
        self.alloc.atomic_op_unmanaged(op, pe, offset)
    }

    fn atomic_op_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        self.alloc.atomic_op_all(scheduler, counters, op, offset)
    }

    fn atomic_op_all_unmanaged<T: Remote>(&self, op: AtomicOp<T>, offset: usize) {
        self.alloc.atomic_op_all_unmanaged(op, offset)
    }

    fn atomic_fetch_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicFetchOpHandle<T> {
        assert_eq!(pe, self.alloc.my_pe, "atomic fetch op called on OneSidedRofiCAlloc with incorrect pe");
        self.alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
    }

    fn blocking_atomic_fetch_op<T: Remote>(&self, op: AtomicOp<T>, pe: usize, offset: usize) -> T {
        assert_eq!(pe, self.alloc.my_pe, "blocking atomic fetch op called on OneSidedRofiCAlloc with incorrect pe");
        self.alloc.blocking_atomic_fetch_op(op, pe, offset)
    }
}

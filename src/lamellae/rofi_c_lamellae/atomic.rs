use std::sync::Arc;

use crate::active_messaging::AMCounters;
use crate::lamellae::comm::atomic::{
    AtomicCompareExchangeFuture, AtomicCompareExchangeOpHandle, AtomicFetchOpFuture,
    AtomicFetchOpHandle, AtomicOp, AtomicOpFuture, AtomicOpHandle, CommAllocAtomic,
};
use crate::warnings::RuntimeWarning;
use crate::LamellarTask;
use crate::Remote;

use super::{
    fabric::{OneSidedRofiCAlloc, RofiCAlloc},
    rofi::{rofi_c_atomic_fetch, rofi_c_atomic_op, rofi_c_compare_atomic},
    Scheduler,
};
use futures_util::Future;
use pin_project::{pin_project, pinned_drop};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

fn exec_rofi_atomic_op<T: Remote + Copy + 'static>(
    alloc: &RofiCAlloc,
    pe: usize,
    offset: usize,
    op: &mut AtomicOp<T>,
) {
    assert!(offset < alloc.num_bytes() / std::mem::size_of::<T>());
    let addr = (alloc.start() + offset * std::mem::size_of::<T>()) as *mut T;
    // if pe == alloc.my_pe {
    //     net_atomic_op(op, &CommAllocAddr(addr as usize));
    //     return;
    // }
    rofi_c_atomic_op(addr, op, pe).expect("rofi-c atomic op failed");
}

fn exec_rofi_atomic_fetch<T: Remote + Copy + 'static>(
    alloc: &RofiCAlloc,
    pe: usize,
    offset: usize,
    op: &mut AtomicOp<T>,
    result: &mut T,
) {
    assert!(offset < alloc.num_bytes() / std::mem::size_of::<T>());
    let addr = (alloc.start() + offset * std::mem::size_of::<T>()) as *mut T;
    // if pe == alloc.my_pe {
    //     net_atomic_fetch_op(op, &CommAllocAddr(addr as usize), result);
    // } else {
    rofi_c_atomic_fetch(addr, op, result, pe).expect("rofi-c atomic fetch failed");
    // }
}

fn exec_rofi_compare_atomic<T: Remote + Copy + PartialEq + 'static>(
    alloc: &RofiCAlloc,
    pe: usize,
    offset: usize,
    current: *const T,
    new: *const T,
    result: &mut T,
) {
    assert!(offset < alloc.num_bytes() / std::mem::size_of::<T>());
    let addr = (alloc.start() + offset * std::mem::size_of::<T>()) as *mut T;
    // if pe == alloc.my_pe {
    //     unsafe {
    //     *result = net_atomic_compare_exchange(*current, *new, &CommAllocAddr(addr as usize))
    //         .unwrap_or_else(|old| old);
    //     }
    // } else {
    rofi_c_compare_atomic(addr, current, new, result, pe).expect("rofi-c compare atomic failed");
    // }
}

#[pin_project(PinnedDrop)]
pub(crate) struct RofiCAtomicFuture<T> {
    alloc: RofiCAlloc,
    remote_pes: Vec<usize>,
    offset: usize,
    op: AtomicOp<T>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    wait_cnt: Option<usize>,
}

impl<T: Remote + Copy + 'static> RofiCAtomicFuture<T> {
    fn exec_op(&mut self) {
        for pe in &self.remote_pes {
            exec_rofi_atomic_op(&self.alloc, *pe, self.offset, &mut self.op);
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.wait().expect("rofi-c atomic wait failed");
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for RofiCAtomicFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<RofiCAtomicFuture<T>> for AtomicOpHandle<T> {
    fn from(f: RofiCAtomicFuture<T>) -> AtomicOpHandle<T> {
        AtomicOpHandle {
            future: AtomicOpFuture::RofiC(f),
        }
    }
}

impl<T: Remote + Copy + 'static> Future for RofiCAtomicFuture<T> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let mut wait_cnt = self.wait_cnt;
        self.alloc.try_wait(&mut wait_cnt);
        self.wait_cnt = wait_cnt;
        if let Some(_my_cnt) = self.wait_cnt {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct RofiCAtomicFetchFuture<T> {
    alloc: RofiCAlloc,
    remote_pe: usize,
    offset: usize,
    op: AtomicOp<T>,
    result: Box<T>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    wait_cnt: Option<usize>,
}

impl<T: Remote + Copy + 'static> RofiCAtomicFetchFuture<T> {
    fn exec_op(&mut self) {
        exec_rofi_atomic_fetch(
            &self.alloc,
            self.remote_pe,
            self.offset,
            &mut self.op,
            self.result.as_mut(),
        );
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> T {
        self.exec_op();
        self.alloc.wait().expect("rofi-c atomic wait failed");
        *self.result
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for RofiCAtomicFetchFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<RofiCAtomicFetchFuture<T>> for AtomicFetchOpHandle<T> {
    fn from(f: RofiCAtomicFetchFuture<T>) -> AtomicFetchOpHandle<T> {
        AtomicFetchOpHandle {
            future: AtomicFetchOpFuture::RofiC(f),
        }
    }
}

impl<T: Remote + Copy + 'static> Future for RofiCAtomicFetchFuture<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let mut wait_cnt = self.wait_cnt;
        self.alloc.try_wait(&mut wait_cnt);
        self.wait_cnt = wait_cnt;
        if let Some(_my_cnt) = self.wait_cnt {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        Poll::Ready(*self.result)
    }
}

fn compare_exchange_result<T: PartialEq>(old: T, current: T) -> Result<T, T> {
    if old == current {
        Ok(old)
    } else {
        Err(old)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct RofiCAtomicCompareExchangeFuture<T> {
    alloc: RofiCAlloc,
    remote_pe: usize,
    offset: usize,
    current: Pin<Box<T>>,
    new: Pin<Box<T>>,
    result: Box<T>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    wait_cnt: Option<usize>,
}

impl<T: Remote + Copy + PartialEq + 'static> RofiCAtomicCompareExchangeFuture<T> {
    fn exec_op(&mut self) {
        exec_rofi_compare_atomic(
            &self.alloc,
            self.remote_pe,
            self.offset,
            self.current.as_ref().get_ref(),
            self.new.as_ref().get_ref(),
            self.result.as_mut(),
        );
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Result<T, T> {
        self.exec_op();
        self.alloc.wait().expect("rofi-c atomic wait failed");
        compare_exchange_result(*self.result, *self.current)
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Result<T, T>> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for RofiCAtomicCompareExchangeFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<RofiCAtomicCompareExchangeFuture<T>> for AtomicCompareExchangeOpHandle<T> {
    fn from(f: RofiCAtomicCompareExchangeFuture<T>) -> AtomicCompareExchangeOpHandle<T> {
        AtomicCompareExchangeOpHandle {
            future: AtomicCompareExchangeFuture::RofiC(f),
        }
    }
}

impl<T: Remote + Copy + PartialEq + 'static> Future for RofiCAtomicCompareExchangeFuture<T> {
    type Output = Result<T, T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let mut wait_cnt = self.wait_cnt;
        self.alloc.try_wait(&mut wait_cnt);
        self.wait_cnt = wait_cnt;
        if let Some(_my_cnt) = self.wait_cnt {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        Poll::Ready(compare_exchange_result(*self.result, *self.current))
    }
}

impl CommAllocAtomic for RofiCAlloc {
    fn atomic_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        RofiCAtomicFuture {
            alloc: self.clone(),
            remote_pes: vec![pe],
            offset,
            op,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
            wait_cnt: None,
        }
        .into()
    }

    fn atomic_op_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        exec_rofi_atomic_op(self, pe, offset, &mut op);
        self.wait().expect("rofi-c atomic wait failed");
    }

    fn atomic_op_unmanaged<T: Remote>(&self, mut op: AtomicOp<T>, pe: usize, offset: usize) {
        exec_rofi_atomic_op(self, pe, offset, &mut op);
    }

    fn atomic_op_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        RofiCAtomicFuture {
            alloc: self.clone(),
            remote_pes: (0..self.num_pes).collect(),
            offset,
            op,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
            wait_cnt: None,
        }
        .into()
    }

    fn atomic_op_all_unmanaged<T: Remote>(&self, mut op: AtomicOp<T>, offset: usize) {
        for pe in 0..self.num_pes {
            exec_rofi_atomic_op(self, pe, offset, &mut op);
        }
    }

    fn atomic_fetch_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicFetchOpHandle<T> {
        RofiCAtomicFetchFuture {
            alloc: self.clone(),
            remote_pe: pe,
            offset,
            op,
            result: Box::new(T::default()),
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
            wait_cnt: None,
        }
        .into()
    }

    fn atomic_fetch_op_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        mut op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> T {
        let mut result = T::default();
        exec_rofi_atomic_fetch(self, pe, offset, &mut op, &mut result);
        self.wait().expect("rofi-c atomic wait failed");
        result
    }

    fn atomic_compare_exchange<T: Remote + PartialEq>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        current: T,
        new: T,
        pe: usize,
        offset: usize,
    ) -> AtomicCompareExchangeOpHandle<T> {
        RofiCAtomicCompareExchangeFuture {
            alloc: self.clone(),
            remote_pe: pe,
            offset,
            current: Box::pin(current),
            new: Box::pin(new),
            result: Box::new(new),
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
            wait_cnt: None,
        }
        .into()
    }

    fn atomic_compare_exchange_blocking<T: Remote + PartialEq>(
        &self,
        _scheduler: &Arc<Scheduler>,
        current: T,
        new: T,
        pe: usize,
        offset: usize,
    ) -> Result<T, T> {
        let mut result = new;
        exec_rofi_compare_atomic(self, pe, offset, &current, &new, &mut result);
        self.wait().expect("rofi-c atomic wait failed");
        compare_exchange_result(result, current)
    }
}

impl CommAllocAtomic for OneSidedRofiCAlloc {
    fn atomic_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        self.alloc.atomic_op(scheduler, counters, op, pe, offset)
    }

    fn atomic_op_blocking<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        self.alloc.atomic_op_blocking(scheduler, op, pe, offset)
    }

    fn atomic_op_unmanaged<T: Remote>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        self.alloc.atomic_op_unmanaged(op, pe, offset)
    }

    fn atomic_op_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
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
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicFetchOpHandle<T> {
        self.alloc
            .atomic_fetch_op(scheduler, counters, op, pe, offset)
    }

    fn atomic_fetch_op_blocking<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> T {
        self.alloc
            .atomic_fetch_op_blocking(scheduler, op, pe, offset)
    }

    fn atomic_compare_exchange<T: Remote + PartialEq>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        current: T,
        new: T,
        pe: usize,
        offset: usize,
    ) -> AtomicCompareExchangeOpHandle<T> {
        self.alloc
            .atomic_compare_exchange(scheduler, counters, current, new, pe, offset)
    }

    fn atomic_compare_exchange_blocking<T: Remote + PartialEq>(
        &self,
        scheduler: &Arc<Scheduler>,
        current: T,
        new: T,
        pe: usize,
        offset: usize,
    ) -> Result<T, T> {
        self.alloc
            .atomic_compare_exchange_blocking(scheduler, current, new, pe, offset)
    }
}

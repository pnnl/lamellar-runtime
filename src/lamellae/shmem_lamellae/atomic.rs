use crate::{
    active_messaging::AMCounters,
    lamellae::{
        comm::atomic::{
            AtomicFetchOpFuture, AtomicFetchOpHandle, AtomicOp, AtomicOpFuture, AtomicOpHandle,
        },
        net_atomic_fetch_op, net_atomic_op,
        shmem_lamellae::fabric::ShmemAlloc,
        CommAllocAddr, CommAllocAtomic,
    },
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::Scheduler;

use pin_project::{pin_project, pinned_drop};
use std::{
    future::Future,
    mem::MaybeUninit,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemAtomicFuture<T> {
    pub(super) op: AtomicOp<T>,
    pub(super) dst: Vec<CommAllocAddr>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: 'static> ShmemAtomicFuture<T> {
    pub(crate) fn block(mut self) {
        for dst in &self.dst {
            net_atomic_op(&self.op, dst);
        }
        self.spawned = true;
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        for dst in &self.dst {
            net_atomic_op(&self.op, dst);
        }
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.spawn_task(async {}, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for ShmemAtomicFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<ShmemAtomicFuture<T>> for AtomicOpHandle<T> {
    fn from(f: ShmemAtomicFuture<T>) -> AtomicOpHandle<T> {
        AtomicOpHandle {
            future: AtomicOpFuture::Shmem(f),
        }
    }
}

impl<T: 'static> Future for ShmemAtomicFuture<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            for dst in &self.dst {
                net_atomic_op(&self.op, dst);
            }
            *self.project().spawned = true;
        } else {
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemAtomicFetchFuture<T> {
    pub(super) op: AtomicOp<T>,
    pub(super) dst: CommAllocAddr,
    pub(super) result: MaybeUninit<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Send + 'static> ShmemAtomicFetchFuture<T> {
    pub(crate) fn block(mut self) -> T {
        net_atomic_fetch_op(&self.op, &self.dst, self.result.as_mut_ptr());
        self.spawned = true;
        unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(&mut self.result, &mut res);
            res.assume_init()
        }
        // Ok(())
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        net_atomic_fetch_op(&self.op, &self.dst, self.result.as_mut_ptr());

        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move {
                unsafe {
                    let mut res = MaybeUninit::uninit();
                    std::mem::swap(&mut self.result, &mut res);
                    res.assume_init()
                }
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for ShmemAtomicFetchFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<ShmemAtomicFetchFuture<T>> for AtomicFetchOpHandle<T> {
    fn from(f: ShmemAtomicFetchFuture<T>) -> AtomicFetchOpHandle<T> {
        AtomicFetchOpHandle {
            future: AtomicFetchOpFuture::Shmem(f),
        }
    }
}

impl<T: Send + 'static> Future for ShmemAtomicFetchFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            let res_ptr = self.result.as_mut_ptr();
            net_atomic_fetch_op(&self.op, &self.dst, res_ptr);

            *self.as_mut().project().spawned = true;
        } else {
        }
        // rofi_c_wait();

        Poll::Ready(unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(&mut self.result, &mut res);
            res.assume_init()
        })
    }
}

impl CommAllocAtomic for Arc<ShmemAlloc> {
    fn atomic_op<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        let remote_dst_base = self.pe_base_offset(pe);
        let remote_dst_addr = remote_dst_base + offset;
        ShmemAtomicFuture {
            op: op,
            dst: vec![CommAllocAddr(remote_dst_addr)],
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn atomic_op_unmanaged<T: Copy + 'static>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        let remote_dst_base = self.pe_base_offset(pe);
        let remote_dst_addr = remote_dst_base + offset;
        net_atomic_op(&op, &CommAllocAddr(remote_dst_addr));
    }
    fn atomic_op_all<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        let remote_dst_addrs: Vec<CommAllocAddr> = (0..self.num_pes())
            .map(|pe| {
                let remote_dst_base = self.pe_base_offset(pe);
                CommAllocAddr(remote_dst_base + offset)
            })
            .collect();
        ShmemAtomicFuture {
            op: op,
            dst: remote_dst_addrs,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn atomic_op_all_unmanaged<T: Copy + 'static>(&self, op: AtomicOp<T>, offset: usize) {
        for pe in 0..self.num_pes() {
            let remote_dst_base = self.pe_base_offset(pe);
            let remote_dst_addr = remote_dst_base + offset;
            net_atomic_op(&op, &CommAllocAddr(remote_dst_addr));
        }
    }
    fn atomic_fetch_op<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicFetchOpHandle<T> {
        let remote_dst_base = self.pe_base_offset(pe);
        let remote_dst_addr = remote_dst_base + offset;
        ShmemAtomicFetchFuture {
            op,
            dst: CommAllocAddr(remote_dst_addr),
            result: MaybeUninit::uninit(),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
}

use crate::{
    active_messaging::AMCounters,
    lamellae::{
        comm::atomic::{
            AtomicFetchOpFuture, AtomicFetchOpHandle, AtomicOp, AtomicOpFuture, AtomicOpHandle,
        },
        local_lamellae::comm::LocalAlloc,
        net_atomic_fetch_op, net_atomic_op, CommAllocAddr, CommAllocAtomic,
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

// #[pin_project(PinnedDrop)]
// pub(crate) struct LocalAtomicFuture<T> {
//     pub(super) op: AtomicOp<T>,
//     pub(super) dst: CommAllocAddr,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Vec<Arc<AMCounters>>,
//     pub(crate) spawned: bool,
// }

// impl<T: 'static> LocalAtomicFuture<T> {
//     pub(crate) fn block(mut self) {
//         net_atomic_op(&self.op, &self.dst);
//         self.spawned = true;
//     }
//     pub(crate) fn spawn(mut self) -> LamellarTask<()> {
//         net_atomic_op(&self.op, &self.dst);
//         self.spawned = true;
//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.spawn_task(async {}, counters)
//     }
// }

// #[pinned_drop]
// impl<T> PinnedDrop for LocalAtomicFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a RdmaHandle").print();
//         }
//     }
// }

// impl<T> From<LocalAtomicFuture<T>> for AtomicOpHandle<T> {
//     fn from(f: LocalAtomicFuture<T>) -> AtomicOpHandle<T> {
//         AtomicOpHandle {
//             future: AtomicOpFuture::Local(f),
//         }
//     }
// }

// impl<T: 'static> Future for LocalAtomicFuture<T> {
//     type Output = ();
//     fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             net_atomic_op(&self.op, &self.dst);
//             *self.project().spawned = true;
//         } else {
//         }
//         Poll::Ready(())
//     }
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct LocalAtomicFetchFuture<T> {
//     pub(super) op: AtomicOp<T>,
//     pub(super) dst: CommAllocAddr,
//     pub(super) result: MaybeUninit<T>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Vec<Arc<AMCounters>>,
//     pub(crate) spawned: bool,
// }

// impl<T: Send + 'static> LocalAtomicFetchFuture<T> {
//     pub(crate) fn block(mut self) -> T {
//         net_atomic_fetch_op(&self.op, &self.dst, self.result.as_mut_ptr());
//         self.spawned = true;
//         unsafe {
//             let mut res = MaybeUninit::uninit();
//             std::mem::swap(&mut self.result, &mut res);
//             res.assume_init()
//         }
//     }

//     pub(crate) fn spawn(mut self) -> LamellarTask<T> {
//         net_atomic_fetch_op(&self.op, &self.dst, self.result.as_mut_ptr());
//         self.spawned = true;
//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.clone().spawn_task(
//             async move {
//                 unsafe {
//                     let mut res = MaybeUninit::uninit();
//                     std::mem::swap(&mut self.result, &mut res);
//                     res.assume_init()
//                 }
//             },
//             counters,
//         )
//     }
// }

// #[pinned_drop]
// impl<T> PinnedDrop for LocalAtomicFetchFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a RdmaHandle").print();
//         }
//     }
// }

// impl<T> From<LocalAtomicFetchFuture<T>> for AtomicFetchOpHandle<T> {
//     fn from(f: LocalAtomicFetchFuture<T>) -> AtomicFetchOpHandle<T> {
//         AtomicFetchOpHandle {
//             future: AtomicFetchOpFuture::Local(f),
//         }
//     }
// }

// impl<T: Send + 'static> Future for LocalAtomicFetchFuture<T> {
//     type Output = T;
//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             let result_ptr =  self.result.as_mut_ptr();
//             net_atomic_fetch_op(&self.op, &self.dst,result_ptr);
//         }
//         let this = self.project();
//         *this.spawned = true;

//         Poll::Ready(unsafe {
//             let mut res = MaybeUninit::uninit();
//             std::mem::swap(this.result, &mut res);
//             res.assume_init()
//         })
//     }
// }

// impl CommAtomic for LocalComm {
//     fn atomic_avail<T: 'static>(&self) -> bool {
//         false
//     }
//     fn atomic_op<T: Copy>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Vec<Arc<AMCounters>>,
//         op: AtomicOp<T>,
//         pe: usize,
//         remote_alloc: CommAllocInner,
//         offset: usize,
//     ) -> AtomicOpHandle<T> {
//         LocalAtomicFuture {
//             op,
//             dst: remote_alloc.addr() + offset,
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//         }
//         .into()
//     }
//     fn atomic_fetch_op<T: Copy>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Vec<Arc<AMCounters>>,
//         op: AtomicOp<T>,
//         pe: usize,
//         remote_alloc: CommAllocInner,
//         offset: usize,
//     ) -> AtomicFetchOpHandle<T> {
//         LocalAtomicFetchFuture {
//             op,
//             dst: remote_alloc.addr() + offset,
//             result: MaybeUninit::uninit(),
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//         }
//         .into()
//     }
// }

#[pin_project(PinnedDrop)]
pub(crate) struct LocalAllocAtomicFuture<T> {
    alloc: Arc<LocalAlloc>,
    offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: 'static> LocalAllocAtomicFuture<T> {
    fn exec_op(&mut self) {
        net_atomic_op(&self.op, &CommAllocAddr(self.alloc.start() + self.offset))
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.spawned = true;
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        // LamellarTask {
        //     task: LamellarTaskInner::Finished(Some(())),
        //     executor: self.scheduler.executor.clone(),
        // }
        self.scheduler.spawn_task(async {}, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LocalAllocAtomicFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<LocalAllocAtomicFuture<T>> for AtomicOpHandle<T> {
    fn from(f: LocalAllocAtomicFuture<T>) -> AtomicOpHandle<T> {
        AtomicOpHandle {
            future: AtomicOpFuture::LocalAlloc(f),
        }
    }
}

impl<T: 'static> Future for LocalAllocAtomicFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
            *self.project().spawned = true;
        } else {
        }
        // rofi_c_wait();

        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LocalAllocAtomicFetchFuture<T> {
    alloc: Arc<LocalAlloc>,
    offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(super) result: MaybeUninit<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Send + 'static> LocalAllocAtomicFetchFuture<T> {
    fn exec_op(&mut self) {
        net_atomic_fetch_op(
            &self.op,
            &CommAllocAddr(self.alloc.start() + self.offset),
            self.result.as_mut_ptr(),
        )
    }
    pub(crate) fn block(mut self) -> T {
        self.exec_op();
        self.spawned = true;
        unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(&mut self.result, &mut res);
            res.assume_init()
        }
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_op();
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
impl<T> PinnedDrop for LocalAllocAtomicFetchFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<LocalAllocAtomicFetchFuture<T>> for AtomicFetchOpHandle<T> {
    fn from(f: LocalAllocAtomicFetchFuture<T>) -> AtomicFetchOpHandle<T> {
        AtomicFetchOpHandle {
            future: AtomicFetchOpFuture::LocalAlloc(f),
        }
    }
}

impl<T: Send + 'static> Future for LocalAllocAtomicFetchFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        *this.spawned = true;
        // rofi_c_wait();

        Poll::Ready(unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(this.result, &mut res);
            res.assume_init()
        })
    }
}

impl CommAllocAtomic for Arc<LocalAlloc> {
    fn atomic_op<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        _pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        LocalAllocAtomicFuture {
            alloc: self.clone(),
            offset,
            op,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }
    fn atomic_op_unmanaged<T: Copy + 'static>(&self, op: AtomicOp<T>, _pe: usize, offset: usize) {
        net_atomic_op(&op, &CommAllocAddr(self.start() + offset));
    }
    fn atomic_fetch_op<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        _pe: usize,
        offset: usize,
    ) -> AtomicFetchOpHandle<T> {
        LocalAllocAtomicFetchFuture {
            alloc: self.clone(),
            offset,
            op,
            result: MaybeUninit::uninit(),
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }
}

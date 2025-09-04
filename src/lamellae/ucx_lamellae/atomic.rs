use crate::{
    active_messaging::AMCounters,
    lamellae::comm::{
        atomic::{
            AtomicFetchOpFuture, AtomicFetchOpHandle, AtomicOp, AtomicOpFuture, AtomicOpHandle,
            CommAllocAtomic,
        },
        CommAllocAddr, CommAllocInner,
    },
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::{
    comm::UcxComm,
    fabric::{UcxAlloc, UcxRequest, UcxWorld},
    Scheduler,
};

use pin_project::{pin_project, pinned_drop};
use std::{
    future::Future,
    mem::MaybeUninit,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tracing::trace;

// #[pin_project(PinnedDrop)]
// pub(crate) struct UcxAtomicFuture<T> {
//     pub(crate) my_pe: usize,
//     pub(crate) ucx: Arc<UcxWorld>,
//     pub(super) remote_pe: usize,
//     pub(super) op: AtomicOp<T>,
//     pub(super) dst: CommAllocAddr,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Vec<Arc<AMCounters>>,
//     pub(crate) spawned: bool,
//     pub(crate) request: Option<UcxRequest>,
// }

// impl<T: Copy + Send + 'static> UcxAtomicFuture<T> {
//     fn exec_op(&mut self) {
//         trace!("performing atomic op: {:?} dst: {:?} ", self.op, self.dst);
//         self.request = Some(unsafe {
//             self.ucx
//                 .atomic_op(self.remote_pe, &self.op, &self.dst)
//         });
//     }
//     pub(crate) fn block(mut self) {
//         self.exec_op();
//         let request = self.request.take().expect("ucx request doesnt exist");
//         request.wait();
//         self.spawned = true;
//     }
//     pub(crate) fn spawn(mut self) -> LamellarTask<()> {
//         self.exec_op();
//         self.spawned = true;
//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         let request = self.request.take().expect("ucx request doesnt exist");
//         // let ucx = self.ucx.clone();
//         self.scheduler
//             .clone()
//             .spawn_task(async move { request.wait(); }, counters)
//         // self.scheduler.clone().spawn_task(async move {}, counters)
//     }
// }

// #[pinned_drop]
// impl<T> PinnedDrop for UcxAtomicFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a RdmaHandle").print();
//         }
//     }
// }

// impl<T> From<UcxAtomicFuture<T>> for AtomicOpHandle<T> {
//     fn from(f: UcxAtomicFuture<T>) -> AtomicOpHandle<T> {
//         AtomicOpHandle {
//             future: AtomicOpFuture::Ucx(f),
//         }
//     }
// }

// impl<T: Copy + Send + 'static> Future for UcxAtomicFuture<T> {
//     type Output = ();
//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//             self.spawned = true;
//         }
//         let request = self.request.take().expect("ucx request doesnt exist");
//         request.wait();
//         Poll::Ready(())
//     }
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct UcxAtomicFetchFuture<T> {
//     pub(crate) my_pe: usize,
//     pub(crate) ucx: Arc<UcxWorld>,
//     pub(super) remote_pe: usize,
//     pub(super) op: AtomicOp<T>,
//     pub(super) dst: CommAllocAddr,
//     pub(crate) result: MaybeUninit<T>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Vec<Arc<AMCounters>>,
//     pub(crate) spawned: bool,
//     pub(crate) request: Option<UcxRequest>,
// }

// impl<T: Copy + Send + 'static> UcxAtomicFetchFuture<T> {
//     fn exec_op(&mut self) {
//         trace!("performing atomic op: {:?} dst: {:?} ", self.op, self.dst);
//         self.request = Some(unsafe {
//             self.ucx
//                 .atomic_fetch_op(
//                     self.remote_pe,
//                     &self.op,
//                     &self.dst,
//                     std::slice::from_mut(&mut *self.result.as_mut_ptr()),
//                 )
//         });
//     }
//     pub(crate) fn block(mut self) -> T {
//         self.exec_op();
//         let request = self.request.take().expect("ucx request doesnt exist");
//         request.wait();
//         self.spawned = true;
//         unsafe {
//             let mut res = MaybeUninit::uninit();
//             std::mem::swap(&mut self.result, &mut res);
//             res.assume_init()
//         }
//     }

//     pub(crate) fn spawn(mut self) -> LamellarTask<T> {
//         self.exec_op();
//         self.spawned = true;
//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);

//         self.scheduler.clone().spawn_task(
//             async move {
//                 let request = self.request.take().expect("ucx request doesnt exist");
//                 request.wait();
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
// impl<T> PinnedDrop for UcxAtomicFetchFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a RdmaHandle").print();
//         }
//     }
// }

// impl<T> From<UcxAtomicFetchFuture<T>> for AtomicFetchOpHandle<T> {
//     fn from(f: UcxAtomicFetchFuture<T>) -> AtomicFetchOpHandle<T> {
//         AtomicFetchOpHandle {
//             future: AtomicFetchOpFuture::Ucx(f),
//         }
//     }
// }

// impl<T: Copy + Send + 'static> Future for UcxAtomicFetchFuture<T> {
//     type Output = T;
//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//             self.spawned = true;
//         }
//         let request = self.request.take().expect("ucx request doesnt exist");
//         request.wait();
//         Poll::Ready(unsafe {
//             let mut res = MaybeUninit::uninit();
//             std::mem::swap(&mut self.result, &mut res);
//             res.assume_init()
//         })
//     }
// }

// impl CommAtomic for UcxComm {
//     fn atomic_avail<T: 'static>(&self) -> bool {
//         self.ucx.atomic_avail::<T>()
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
//         UcxAtomicFuture {
//             my_pe: self.my_pe,
//             ucx: self.ucx.clone(),
//             remote_pe: pe,
//             op,
//             dst: remote_alloc.addr() + offset,
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//             request: None,
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
//         UcxAtomicFetchFuture {
//             my_pe: self.my_pe,
//             ucx: self.ucx.clone(),
//             remote_pe: pe,
//             op: op,
//             dst: remote_alloc.addr() + offset,
//             result: MaybeUninit::uninit(),
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//             request: None,
//         }
//         .into()
//     }
// }

#[pin_project(PinnedDrop)]
pub(crate) struct UcxAllocAtomicFuture<T> {
    pub(crate) alloc: Arc<UcxAlloc>,
    pub(super) remote_pe: usize,
    pub(crate) offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    pub(crate) request: Option<UcxRequest>,
}

impl<T: Copy + Send + 'static> UcxAllocAtomicFuture<T> {
    fn exec_op(&mut self) {
        trace!(
            "performing atomic op: {:?} offset: {:?} ",
            self.op,
            self.offset
        );
        self.request = Some(unsafe {
            UcxAlloc::atomic_op(&self.alloc, self.remote_pe, self.offset, &self.op)
        });
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        let request = self.request.take().expect("ucx request doesnt exist");
        request.wait();
        self.spawned = true;
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        let request = self.request.take().expect("ucx request doesnt exist");
        self.scheduler.clone().spawn_task(
            async move {
                request.wait();
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for UcxAllocAtomicFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<UcxAllocAtomicFuture<T>> for AtomicOpHandle<T> {
    fn from(f: UcxAllocAtomicFuture<T>) -> AtomicOpHandle<T> {
        AtomicOpHandle {
            future: AtomicOpFuture::UcxAlloc(f),
        }
    }
}

impl<T: Copy + Send + 'static> Future for UcxAllocAtomicFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
            self.spawned = true;
        }
        let request = self.request.take().expect("ucx request doesnt exist");
        request.wait();
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxAllocAtomicFetchFuture<T> {
    pub(crate) alloc: Arc<UcxAlloc>,
    pub(super) remote_pe: usize,
    pub(crate) offset: usize,
    pub(super) op: AtomicOp<T>,
    pub(crate) result: MaybeUninit<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    pub(crate) request: Option<UcxRequest>,
}

impl<T: Copy + Send + 'static> UcxAllocAtomicFetchFuture<T> {
    fn exec_op(&mut self) {
        trace!(
            "performing atomic op: {:?} offset: {:?} ",
            self.op,
            self.offset
        );
        self.request = Some(unsafe {
            UcxAlloc::atomic_fetch_op(
                &self.alloc,
                self.remote_pe,
                self.offset,
                &self.op,
                std::slice::from_mut(&mut *self.result.as_mut_ptr()),
            )
        });
    }
    pub(crate) fn block(mut self) -> T {
        self.exec_op();
        let request = self.request.take().expect("ucx request doesnt exist");
        request.wait();
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
                let request = self.request.take().expect("ucx request doesnt exist");
                request.wait();
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
impl<T> PinnedDrop for UcxAllocAtomicFetchFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T> From<UcxAllocAtomicFetchFuture<T>> for AtomicFetchOpHandle<T> {
    fn from(f: UcxAllocAtomicFetchFuture<T>) -> AtomicFetchOpHandle<T> {
        AtomicFetchOpHandle {
            future: AtomicFetchOpFuture::UcxAlloc(f),
        }
    }
}

impl<T: Copy + Send + 'static> Future for UcxAllocAtomicFetchFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
            self.spawned = true;
        }
        let request = self.request.take().expect("ucx request doesnt exist");
        request.wait();
        Poll::Ready(unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(&mut self.result, &mut res);
            res.assume_init()
        })
    }
}

impl CommAllocAtomic for Arc<UcxAlloc> {
    fn atomic_op<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        UcxAllocAtomicFuture {
            alloc: self.clone(),
            remote_pe: pe,
            offset,
            op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
        }
        .into()
    }
    fn atomic_op_unmanaged<T: Copy>(
        &self,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        unsafe { UcxAlloc::atomic_op(self, pe, offset, &op) };
    }
    fn atomic_fetch_op<T: Copy>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicFetchOpHandle<T> {
        UcxAllocAtomicFetchFuture {
            alloc: self.clone(),
            remote_pe: pe,
            offset,
            op: op,
            result: MaybeUninit::uninit(),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
        }
        .into()
    }
}

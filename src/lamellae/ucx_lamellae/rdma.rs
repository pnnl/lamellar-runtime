use std::{
    mem::MaybeUninit,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_util::Future;
use pin_project::{pin_project, pinned_drop};
use tracing::trace;

use crate::{
    active_messaging::AMCounters,
    lamellae::{
        comm::rdma::{RdmaAtFuture, RdmaFuture, RdmaGetHandle, RdmaHandle, Remote},
        CommAllocRdma, CommSlice,
    },
    memregion::MemregionRdmaInput,
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::{
    fabric::{UcxAlloc, UcxRequest},
    Scheduler,
};

#[derive(Clone)]
pub(super) enum AllocOp<T: Remote> {
    Put(usize, T),
    PutBuf(usize, MemregionRdmaInput<T>),
    PutAll(Vec<usize>, MemregionRdmaInput<T>),
    Get(usize, CommSlice<T>),
}

#[pin_project(PinnedDrop)]
pub(crate) struct UcxAllocFuture<T: Remote> {
    my_pe: usize,
    pub(crate) alloc: Arc<UcxAlloc>,
    pub(crate) offset: usize,
    pub(super) op: AllocOp<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    pub(crate) request: Option<UcxRequest>,
}

impl<T: Remote> UcxAllocFuture<T> {
    fn inner_put(&mut self, pe: usize, src: T) {
        if pe != self.my_pe {
            self.request = unsafe {
                UcxAlloc::put_inner(
                    &self.alloc,
                    pe,
                    self.offset,
                    std::slice::from_ref(&src),
                    true,
                )
            };
        } else {
            let dst = (self.alloc.start() + self.offset) as *mut T;
            unsafe { dst.write(src) };
        }
    }
    fn inner_put_buf(&mut self, pe: usize, src: &MemregionRdmaInput<T>) {
        trace!(
            "putting src: {:?} dst: {:?} len: {} num bytes {}",
            src.as_ptr(),
            self.alloc.start() + self.offset,
            src.len(),
            src.len() * std::mem::size_of::<T>()
        );
        if pe != self.my_pe {
            self.request =
                unsafe { UcxAlloc::put_inner(&self.alloc, pe, self.offset, src.as_slice(), true) };
        } else {
            let dst = self.alloc.start() + self.offset;
            if !(src.contains(&dst) || src.contains(&(dst + src.len()))) {
                unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst as *mut T, src.len()) };
            } else {
                unsafe {
                    std::ptr::copy(src.as_ptr(), dst as *mut T, src.len());
                }
            }
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn inner_put_all(&mut self, pes: &Vec<usize>, src: &MemregionRdmaInput<T>) {
        // trace!(
        //     "put all src: {:?} dsts: {:?} len: {}",
        //     src.usize_addr(),
        //     dst,
        //     src.len()
        // );
        for pe in pes {
            self.inner_put_buf(*pe, src);
        }
    }
    fn inner_get(&mut self, pe: usize, mut dst: CommSlice<T>) {
        trace!(
            "getting src: {:?} dst: {:?} len: {}",
            self.alloc.start() + self.offset,
            dst.usize_addr(),
            dst.len()
        );
        if pe != self.my_pe {
            self.request =
                Some(unsafe { UcxAlloc::get(&self.alloc, pe, self.offset, &mut dst, false) });
        } else {
            let src = self.alloc.start() + self.offset;
            if !(dst.contains(&src) || dst.contains(&(src + dst.len()))) {
                unsafe {
                    std::ptr::copy_nonoverlapping(src as *const T, dst.as_mut_ptr(), dst.len());
                }
            } else {
                unsafe {
                    std::ptr::copy(src as *const T, dst.as_mut_ptr(), dst.len());
                }
            }
        }
    }

    fn exec_op(&mut self) {
        match self.op.clone() {
            AllocOp::Put(pe, src) => {
                self.inner_put(pe, src);
            }
            AllocOp::PutBuf(pe, src) => {
                self.inner_put_buf(pe, &src);
            }
            AllocOp::PutAll(pes, src) => {
                self.inner_put_all(&pes, &src);
            }
            AllocOp::Get(pe, dst) => {
                self.inner_get(pe, dst);
            }
        }
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        if let Some(request) = self.request.take() {
            request.wait();
        } else {
            self.alloc.wait_all();
        }
        // if let Some(mut request) = self.request.take() {
        //     while let Some(request2) = request.wait(false).expect("Wait failed") {
        //         request = request2;
        //         std::thread::yield_now();
        //     }
        // } else {
        //     while !self.alloc.wait_all() {
        //         std::thread::yield_now();
        //     }
        // }
        self.spawned = true;
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move {
                if let Some(request) = self.request.take() {
                    request.wait();
                } else {
                    self.alloc.wait_all();
                }
            },
            counters,
        )
        // self.scheduler.clone().spawn_task(
        //     async move {
        //         // println!(
        //         //     "[{:?}] in spawn rdma {:?} {:?} {:?}",
        //         //     std::thread::current().id(),
        //         //     self.alloc,
        //         //     self.offset,
        //         //     self.request.is_some()
        //         // );
        //         if let Some(mut request) = self.request.take() {
        //             while let Some(request2) = request.wait(false).expect("Wait failed") {
        //                 request = request2;
        //                 async_std::task::yield_now().await;
        //             }
        //         } else {
        //             while !self.alloc.wait_all() {
        //                 async_std::task::yield_now().await;
        //             }
        //         }
        //         // println!(
        //         //     "[{:?}] out spawn rdma {:?} {:?} {:?}",
        //         //     std::thread::current().id(),
        //         //     self.alloc,
        //         //     self.offset,
        //         //     self.request.is_some()
        //         // );
        //     },
        //     counters,
        // )
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for UcxAllocFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<UcxAllocFuture<T>> for RdmaHandle<T> {
    fn from(f: UcxAllocFuture<T>) -> RdmaHandle<T> {
        RdmaHandle {
            future: RdmaFuture::UcxAlloc(f),
        }
    }
}

impl<T: Remote> Future for UcxAllocFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let this = self.project();
        *this.spawned = true;
        if let Some(request) = this.request.take() {
            request.wait();
        } else {
            this.alloc.wait_all();
        }

        Poll::Ready(())
    }
}
// impl<T: Remote> Future for UcxAllocFuture<T> {
//     type Output = ();
//     fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             // println!("[{:?}] in poll rdma", std::thread::current().id());
//             self.exec_op();
//         }
//         let this = self.project();
//         *this.spawned = true;
//         if let Some(request) = this.request.take() {
//             if let Some(request) = request.wait(false).expect("Wait failed") {
//                 *this.request = Some(request);
//                 cx.waker().wake_by_ref();
//                 return Poll::Pending;
//             }
//         } else {
//             if !this.alloc.wait_all() {
//                 cx.waker().wake_by_ref();
//                 return Poll::Pending;
//             }
//         }

//         // println!("[{:?}] out poll rdma", std::thread::current().id());
//         Poll::Ready(())
//     }
// }

#[pin_project(PinnedDrop)]
pub(crate) struct UcxAllocAtFuture<T> {
    pub(crate) alloc: Arc<UcxAlloc>,
    pub(crate) pe: usize,
    pub(crate) offset: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    pub(crate) result: MaybeUninit<T>,
    pub(crate) request: Option<UcxRequest>,
}

impl<T: Remote> UcxAllocAtFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        trace!("getting at: {:?} {:?} ", self.pe, self.offset);
        self.request = Some(unsafe {
            self.alloc.inner_get(
                self.pe,
                self.offset,
                std::slice::from_raw_parts_mut(self.result.as_mut_ptr(), 1),
                false,
            )
        });
    }

    pub(crate) fn block(mut self) -> T {
        self.exec_at();
        self.spawned = true;
        // if let Some(mut request) = self.request.take() {
        //     while let Some(request2) = request.wait(false).expect("Wait failed") {
        //         request = request2;
        //         std::thread::yield_now();
        //     }
        // } else {
        //     while !self.alloc.wait_all() {
        //         std::thread::yield_now();
        //     }
        // }
        if let Some(request) = self.request.take() {
            request.wait();
        } else {
            self.alloc.wait_all();
        }
        unsafe { self.result.assume_init() }
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_at();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        // self.scheduler.clone().spawn_task(
        //     async move {
        //         // println!("in spawn at");
        //         if let Some(mut request) = self.request.take() {
        //             while let Some(request2) = request.wait(false).expect("Wait failed") {
        //                 request = request2;
        //                 async_std::task::yield_now().await;
        //             }
        //         } else {
        //             while !self.alloc.wait_all() {
        //                 async_std::task::yield_now().await;
        //             }
        //         }
        //         // println!("out spawn at");
        //         unsafe { self.result.assume_init() }
        //     },
        //     counters,
        // )
        self.scheduler.clone().spawn_task(
            async move {
                if let Some(request) = self.request.take() {
                    request.wait();
                } else {
                    self.alloc.wait_all();
                }
                unsafe { self.result.assume_init() }
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for UcxAllocAtFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<UcxAllocAtFuture<T>> for RdmaGetHandle<T> {
    fn from(f: UcxAllocAtFuture<T>) -> RdmaGetHandle<T> {
        RdmaGetHandle {
            future: RdmaAtFuture::UcxAlloc(f),
        }
    }
}

impl<T: Remote> Future for UcxAllocAtFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        let this = self.project();
        *this.spawned = true;
        if let Some(request) = this.request.take() {
            request.wait();
        } else {
            this.alloc.wait_all();
        }

        Poll::Ready(unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(this.result, &mut res);
            res.assume_init()
        })
    }
}

// impl<T: Remote> Future for UcxAllocAtFuture<T> {
//     type Output = T;
//     fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             // println!("(in poll at");
//             self.exec_at();
//         }
//         let this = self.project();
//         *this.spawned = true;
//         if let Some(request) = this.request.take() {
//             if let Some(request) = request.wait(false).expect("Wait failed") {
//                 *this.request = Some(request);
//                 cx.waker().wake_by_ref();
//                 return Poll::Pending;
//             }
//         } else {
//             if !this.alloc.wait_all() {
//                 cx.waker().wake_by_ref();
//                 return Poll::Pending;
//             }
//         }
//         // println!("out poll at");
//         Poll::Ready(unsafe {
//             let mut res = MaybeUninit::uninit();
//             std::mem::swap(this.result, &mut res);
//             res.assume_init()
//         })
//     }
// }

impl CommAllocRdma for Arc<UcxAlloc> {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        //  self.put_amt
        //     .fetch_add(src.len() * std::mem::size_of::<T>(), Ordering::SeqCst);

        UcxAllocFuture {
            my_pe: self.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::Put(pe, src.into()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
        }
        .into()
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        if pe != self.my_pe {
            // for ucx put operation waiting on the request simply ensures the input buffer is free to reuse
            // not that the operation has completed on the remote side
            unsafe { UcxAlloc::put_inner(&self, pe, offset, std::slice::from_ref(&src), false) };
        } else {
            let dst = (self.start() + offset) as *mut T;
            unsafe { dst.write(src) };
        }
    }
    fn put_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInput<T>>,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        //  self.put_amt
        //     .fetch_add(src.len() * std::mem::size_of::<T>(), Ordering::SeqCst);

        UcxAllocFuture {
            my_pe: self.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::PutBuf(pe, src.into()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
        }
        .into()
    }
    fn put_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInput<T>>,
        pe: usize,
        offset: usize,
    ) {
        let src = src.into();
        trace!(
            "putting src: {:?} dst: {:?} len: {} num bytes {}",
            src.as_ptr(),
            self.start() + offset,
            src.len(),
            src.len() * std::mem::size_of::<T>()
        );
        if pe != self.my_pe {
            // for ucx put operation waiting on the request simply ensures the input buffer is free to reuse
            // not that the operation has completed on the remote side
            unsafe { UcxAlloc::put_inner(&self, pe, offset, src.as_slice(), false) };
        } else {
            let dst = self.start() + offset;
            if !(src.contains(&dst) || src.contains(&(dst + src.len()))) {
                unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst as *mut T, src.len()) };
            } else {
                unsafe {
                    std::ptr::copy(src.as_ptr(), dst as *mut T, src.len());
                }
            }
        }
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: impl Into<MemregionRdmaInput<T>>,
        offset: usize,
    ) -> RdmaHandle<T> {
        let pes = (0..self.num_pes).collect();
        UcxAllocFuture {
            my_pe: self.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::PutAll(pes, src.into()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
        }
        .into()
    }

    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
        dst: CommSlice<T>,
    ) -> RdmaHandle<T> {
        UcxAllocFuture {
            my_pe: self.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::Get(pe, dst),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
        }
        .into()
    }
    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        UcxAllocAtFuture {
            alloc: self.clone(),
            pe,
            offset,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: MaybeUninit::uninit(),
            request: None,
        }
        .into()
    }
}

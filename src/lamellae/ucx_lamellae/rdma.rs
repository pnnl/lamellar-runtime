use std::{
    mem::MaybeUninit,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    task::{Context, Poll},
};

use futures_util::Future;
use pin_project::{pin_project, pinned_drop};
use tracing::trace;

use crate::{
    active_messaging::AMCounters,
    array::LamellarArrayRdmaInput,
    lamellae::{
        comm::rdma::{RdmaAtFuture, RdmaAtHandle, RdmaFuture, RdmaHandle, Remote},
        CommAllocAddr, CommAllocInner, CommAllocRdma, CommSlice,
    },
    memregion::MemregionRdmaInput,
    warnings::RuntimeWarning,
    Dist, LamellarTask,
};

use super::{
    comm::UcxComm,
    fabric::{UcxAlloc, UcxRequest, UcxWorld},
    Scheduler,
};

// #[derive(Clone)]
// pub(super) enum Op<T> {
//     Put(usize, CommSlice<T>, CommAllocAddr),
//     Put2(usize, T, CommAllocAddr),
//     PutTest(usize, T, CommAllocAddr),
//     PutAll(CommSlice<T>, CommAllocAddr, Vec<usize>),
//     Get(usize, CommAllocAddr, CommSlice<T>),
//     // Atomic,
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct UcxFuture<T> {
//     pub(crate) my_pe: usize,
//     pub(crate) ucx: Arc<UcxWorld>,
//     pub(super) op: Op<T>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Vec<Arc<AMCounters>>,
//     pub(crate) spawned: bool,
//     pub(crate) request: Option<UcxRequest>,
// }

// impl<T: Remote> UcxFuture<T> {
//     fn inner_put(&mut self, pe: usize, src: &CommSlice<T>, dst: &CommAllocAddr) {
//         trace!(
//             "putting src: {:?} dst: {:?} len: {} num bytes {}",
//             src.usize_addr(),
//             dst,
//             src.len(),
//             src.len() * std::mem::size_of::<T>()
//         );
//         if pe != self.my_pe {
//             self.request = Some(unsafe { self.ucx.put(pe, src, dst, false) });
//         } else {
//             if !(src.contains(dst) || src.contains(&(dst + src.len()))) {
//                 unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), src.len()) };
//             } else {
//                 unsafe {
//                     std::ptr::copy(src.as_ptr(), dst.as_mut_ptr(), src.len());
//                 }
//             }
//         }
//     }
//     fn inner_put2(&mut self, pe: usize, src: &T, dst: &CommAllocAddr) {
//         self.request = Some(unsafe {
//             self.ucx
//                 .inner_put(pe, std::slice::from_ref(src), *(&dst as &usize), false)
//         });
//     }
//     fn inner_put_test(&mut self, pe: usize, src: &T, dst: &CommAllocAddr) {
//         if pe != self.my_pe {
//             self.request = Some(unsafe {
//                 self.ucx
//                     .inner_put(pe, std::slice::from_ref(src), *(dst as &usize), false)
//             });
//         } else {
//             unsafe {
//                 dst.as_mut_ptr::<T>().write(*src);
//             }
//         }
//     }
//     #[tracing::instrument(skip_all, level = "debug")]
//     fn inner_put_all(&mut self, src: &CommSlice<T>, dst: &CommAllocAddr, pes: &Vec<usize>) {
//         trace!(
//             "put all src: {:?} dsts: {:?} len: {}",
//             src.usize_addr(),
//             dst,
//             src.len()
//         );
//         for pe in pes {
//             self.inner_put(*pe, src, dst);
//         }
//     }
//     #[tracing::instrument(skip_all, level = "debug")]
//     fn inner_get(&mut self, pe: usize, src: &CommAllocAddr, mut dst: CommSlice<T>) {
//         trace!(
//             "getting src: {:?} dst: {:?} len: {}",
//             src,
//             dst.usize_addr(),
//             dst.len()
//         );
//         if pe != self.my_pe {
//             self.request = Some(unsafe { self.ucx.get(pe, src, &mut dst, false) });
//             // unsafe{rucx_c_get(src.into(),  dst.as_mut_slice(), pe).expect("rucx_c_get failed")};
//         } else {
//             if !(dst.contains(src) || dst.contains(&(src + dst.len()))) {
//                 unsafe {
//                     std::ptr::copy_nonoverlapping(src.as_mut_ptr(), dst.as_mut_ptr(), dst.len());
//                 }
//             } else {
//                 unsafe {
//                     std::ptr::copy(src.as_mut_ptr(), dst.as_mut_ptr(), dst.len());
//                 }
//             }
//         }
//     }

//     fn exec_op(&mut self) {
//         match self.op.clone() {
//             Op::Put(pe, src, dst) => {
//                 self.inner_put(pe, &src, &dst);
//             }
//             Op::Put2(pe, src, dst) => {
//                 self.inner_put2(pe, &src, &dst);
//             }
//             Op::PutTest(pe, src, dst) => {
//                 self.inner_put_test(pe, &src, &dst);
//             }
//             Op::PutAll(src, dst, pes) => {
//                 self.inner_put_all(&src, &dst, &pes);
//             }
//             Op::Get(pe, src, dst) => {
//                 self.inner_get(pe, &src, dst.clone());
//             }
//         }
//     }
//     pub(crate) fn block(mut self) {
//         self.exec_op();
//         // rucx_c_wait();
//         if let Some(request) = self.request.take() {
//             request.wait();
//         } else {
//             self.ucx.wait_all();
//         }
//         self.spawned = true;
//         // Ok(())
//     }
//     pub(crate) fn spawn(mut self) -> LamellarTask<()> {
//         self.exec_op();
//         self.spawned = true;
//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);

//         self.scheduler.clone().spawn_task(
//             async move {
//                 if let Some(request) = self.request.take() {
//                     request.wait();
//                 } else {
//                     self.ucx.wait_all();
//                 }
//             },
//             counters,
//         )
//     }
// }

// #[pinned_drop]
// impl<T> PinnedDrop for UcxFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a RdmaHandle").print();
//         }
//     }
// }

// impl<T: Remote> From<UcxFuture<T>> for RdmaHandle<T> {
//     fn from(f: UcxFuture<T>) -> RdmaHandle<T> {
//         RdmaHandle {
//             future: RdmaFuture::Ucx(f),
//         }
//     }
// }

// impl<T: Remote> Future for UcxFuture<T> {
//     type Output = ();
//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_op();
//         }
//         let this = self.project();
//         *this.spawned = true;
//         if let Some(request) = this.request.take() {
//             request.wait();
//         } else {
//             this.ucx.wait_all();
//         }

//         Poll::Ready(())
//     }
// }

// #[pin_project(PinnedDrop)]
// pub(crate) struct UcxAtFuture<T> {
//     pub(crate) pe: usize,
//     pub(crate) ucx: Arc<UcxWorld>,
//     pub(super) src: CommAllocAddr,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) counters: Vec<Arc<AMCounters>>,
//     pub(crate) spawned: bool,
//     pub(crate) result: MaybeUninit<T>,
//     pub(crate) request: Option<UcxRequest>,
// }

// impl<T: Remote> UcxAtFuture<T> {
//     #[tracing::instrument(skip_all, level = "debug")]
//     fn exec_at(&mut self) {
//         trace!("getting src: {:?} ", self.src);
//         self.request = Some(unsafe {
//             self.ucx.inner_get(
//                 self.pe,
//                 *(&self.src as &usize),
//                 std::slice::from_raw_parts_mut(self.result.as_mut_ptr(), 1),
//                 false,
//             )
//         });
//     }

//     pub(crate) fn block(mut self) -> T {
//         self.exec_at();
//         self.spawned = true;
//         if let Some(request) = self.request.take() {
//             request.wait();
//         }
//         unsafe { self.result.assume_init() }
//     }
//     pub(crate) fn spawn(mut self) -> LamellarTask<T> {
//         self.exec_at();
//         self.spawned = true;
//         let mut counters = Vec::new();
//         std::mem::swap(&mut counters, &mut self.counters);
//         self.scheduler.clone().spawn_task(
//             async move {
//                 if let Some(request) = self.request.take() {
//                     request.wait();
//                 }
//                 unsafe { self.result.assume_init() }
//             },
//             counters,
//         )
//     }
// }

// #[pinned_drop]
// impl<T> PinnedDrop for UcxAtFuture<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.spawned {
//             RuntimeWarning::DroppedHandle("a RdmaHandle").print();
//         }
//     }
// }

// impl<T: Remote> From<UcxAtFuture<T>> for RdmaAtHandle<T> {
//     fn from(f: UcxAtFuture<T>) -> RdmaAtHandle<T> {
//         RdmaAtHandle {
//             future: RdmaAtFuture::Ucx(f),
//         }
//     }
// }

// impl<T: Remote> Future for UcxAtFuture<T> {
//     type Output = T;
//     fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
//         if !self.spawned {
//             self.exec_at();
//         }
//         let this = self.project();
//         *this.spawned = true;
//         if let Some(request) = this.request.take() {
//             request.wait();
//         }

//         Poll::Ready(unsafe {
//             let mut res = MaybeUninit::uninit();
//             std::mem::swap(this.result, &mut res);
//             res.assume_init()
//         })
//     }
// }

// impl CommRdma for UcxComm {
//     fn put<T: Remote>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Vec<Arc<AMCounters>>,
//         pe: usize,
//         src: CommSlice<T>, //contains alloc info
//         remote_alloc: CommAllocInner,
//         offset: usize,
//     ) -> RdmaHandle<T> {
//         self.put_amt
//             .fetch_add(src.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
//         UcxFuture {
//             my_pe: self.my_pe,
//             ucx: self.ucx.clone(),
//             op: Op::Put(pe, src, remote_alloc.addr() + offset),
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//             request: None,
//         }
//         .into()
//     }
//     fn put2<T: Remote>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Vec<Arc<AMCounters>>,
//         pe: usize,
//         src: T, //contains alloc info
//         remote_alloc: CommAllocInner,
//         offset: usize,
//     ) -> RdmaHandle<T> {
//         // self.put_amt
//         //     .fetch_add(std::mem::size_of::<T>(), Ordering::SeqCst);
//         UcxFuture {
//             my_pe: self.my_pe,
//             ucx: self.ucx.clone(),
//             op: Op::Put2(pe, src, remote_alloc.addr() + offset),
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//             request: None,
//         }
//         .into()
//     }
//     fn put_test<T: Dist>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Vec<Arc<AMCounters>>,
//         pe: usize,
//         src: LamellarArrayRdmaInput<T>,
//         remote_alloc: CommAllocInner,
//         offset: usize,
//     ) {
//         // if pe != self.my_pe {
//         let remote_addr = remote_alloc.addr() + offset;
//         unsafe {
//             self.ucx
//                 .inner_put(pe, src.as_slice(), *(&remote_addr as &usize), false)
//         };

//         // } else {
//         //     unsafe {
//         //         remote_addr.as_mut_ptr::<T>().write(src);
//         //     }
//         // }
//     }
//     fn put_all<T: Remote>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Vec<Arc<AMCounters>>,
//         src: CommSlice<T>,
//         remote_alloc: CommAllocInner,
//         offset: usize,
//     ) -> RdmaHandle<T> {
//         self.put_amt.fetch_add(
//             src.len() * std::mem::size_of::<T>() * self.num_pes,
//             Ordering::SeqCst,
//         );
//         let pes = (0..self.num_pes).collect();
//         UcxFuture {
//             my_pe: self.my_pe,
//             ucx: self.ucx.clone(),
//             op: Op::PutAll(src, remote_alloc.addr() + offset, pes),
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//             request: None,
//         }
//         .into()
//     }
//     fn get<T: Remote>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Vec<Arc<AMCounters>>,
//         pe: usize,
//         src: CommAllocInner,
//         offset: usize,
//         dst: CommSlice<T>,
//     ) -> RdmaHandle<T> {
//         self.get_amt
//             .fetch_add(dst.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
//         UcxFuture {
//             my_pe: self.my_pe,
//             ucx: self.ucx.clone(),
//             op: Op::Get(pe, src.addr() + offset, dst),
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//             request: None,
//         }
//         .into()
//     }

//     fn get_test<T: Remote>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Vec<Arc<AMCounters>>,
//         pe: usize,
//         src: CommAllocInner,
//         offset: usize,
//     ) -> T {
//         let mut dst: std::mem::MaybeUninit<T> = std::mem::MaybeUninit::uninit();
//         let src_addr = src.addr() + offset;
//         // if pe != self.my_pe {
//         unsafe {
//             self.ucx.inner_get(
//                 pe,
//                 *(&src_addr as &usize),
//                 std::slice::from_mut(&mut *dst.as_mut_ptr()),
//                 true,
//             )
//         }
//         .wait();
//         unsafe { dst.assume_init() }
//         // } else {
//         //     unsafe {
//         //         src_addr.as_mut_ptr::<T>().read()
//         //     }
//         // }
//     }
//     fn at<T: Remote>(
//         &self,
//         scheduler: &Arc<Scheduler>,
//         counters: Vec<Arc<AMCounters>>,
//         pe: usize,
//         src: CommAllocInner,
//         offset: usize,
//     ) -> RdmaAtHandle<T> {
//         self.get_amt
//             .fetch_add(std::mem::size_of::<T>(), Ordering::SeqCst);
//         UcxAtFuture {
//             pe,
//             ucx: self.ucx.clone(),
//             src: src.addr() + offset,
//             spawned: false,
//             scheduler: scheduler.clone(),
//             counters,
//             result: MaybeUninit::uninit(),
//             request: None,
//         }
//         .into()
//     }
// }

#[derive(Clone)]
pub(super) enum AllocOp<T: Remote> {
    Put(usize, MemregionRdmaInput<T>),
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
    fn inner_put(&mut self, pe: usize, src: &MemregionRdmaInput<T>) {
        trace!(
            "putting src: {:?} dst: {:?} len: {} num bytes {}",
            src.as_ptr(),
            self.alloc.start() + self.offset,
            src.len(),
            src.len() * std::mem::size_of::<T>()
        );
        if pe != self.my_pe {
            self.request = Some(unsafe {
                UcxAlloc::put_inner(&self.alloc, pe, self.offset, src.as_slice(), true)
            });
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
            self.inner_put(*pe, src);
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
                self.inner_put(pe, &src);
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

impl<T: Remote> From<UcxAllocAtFuture<T>> for RdmaAtHandle<T> {
    fn from(f: UcxAllocAtFuture<T>) -> RdmaAtHandle<T> {
        RdmaAtHandle {
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

impl CommAllocRdma for Arc<UcxAlloc> {
    fn put<T: Remote>(
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
            op: AllocOp::Put(pe, src.into()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            request: None,
        }
        .into()
    }
    fn put_unmanaged<T: Remote>(
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

    fn get<T: Remote>(
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
    fn at<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        offset: usize,
    ) -> RdmaAtHandle<T> {
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

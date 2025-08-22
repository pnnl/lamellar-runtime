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
    lamellae::{
        comm::rdma::{CommRdma, RdmaAtFuture, RdmaAtHandle, RdmaFuture, RdmaHandle, Remote},
        CommAllocAddr, CommSlice,
    },
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::{comm::LibfabricComm, fabric::Ofi, Scheduler};

pub(super) enum Op<T> {
    Put(usize, CommSlice<T>, CommAllocAddr),
    Put2(usize, T, CommAllocAddr),
    PutTest(usize, T, CommAllocAddr),
    PutAll(CommSlice<T>, CommAllocAddr, Vec<usize>),
    Get(usize, CommAllocAddr, CommSlice<T>),
    // Atomic,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricFuture<T> {
    pub(crate) my_pe: usize,
    pub(crate) ofi: Arc<Ofi>,
    pub(super) op: Op<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> LibfabricFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn inner_put(&self, pe: usize, src: &CommSlice<T>, dst: &CommAllocAddr) {
        trace!(
            "putting src: {:?} dst: {:?} len: {} num bytes {}",
            src.usize_addr(),
            dst,
            src.len(),
            src.len() * std::mem::size_of::<T>()
        );
        if pe != self.my_pe {
            unsafe { self.ofi.put(pe, src, dst, false).unwrap() };
            // unsafe{rofi_c_put(src,  dst.into(), pe).expect("rofi_c_put failed")};
        } else {
            if !(src.contains(dst) || src.contains(&(dst + src.len()))) {
                unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), src.len()) };
            } else {
                unsafe {
                    std::ptr::copy(src.as_ptr(), dst.as_mut_ptr(), src.len());
                }
            }
        }
    }
    fn inner_put2(&self, pe: usize, src: &T, dst: &CommAllocAddr) {
        // trace!(
        //     "putting src: {:?} dst: {:?} len: {} num bytes {}",
        //     src.usize_addr(),
        //     dst,
        //     src.len(),
        //     src.len() * std::mem::size_of::<T>()
        // );
        // if pe != self.my_pe {
        unsafe {
            self.ofi
                .inner_put(pe, std::slice::from_ref(src), *(&dst as &usize), false)
                .unwrap()
        };
        // unsafe{rofi_c_put(src,  dst.into(), pe).expect("rofi_c_put failed")};
        // } else {
        //     if !(src.contains(dst) || src.contains(&(dst + src.len()))) {
        //         unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), src.len()) };
        //     } else {
        //         unsafe {
        //             std::ptr::copy(src.as_ptr(), dst.as_mut_ptr(), src.len());
        //         }
        //     }
        // }
    }
    fn inner_put_test(&self, pe: usize, src: &T, dst: &CommAllocAddr) {
        // trace!(
        //     "putting src: {:?} dst: {:?} len: {} num bytes {}",
        //     src.usize_addr(),
        //     dst,
        //     src.len(),
        //     src.len() * std::mem::size_of::<T>()
        // );
        if pe != self.my_pe {
            unsafe {
                self.ofi
                    .inner_put(pe, std::slice::from_ref(src), *(dst as &usize), false)
                    .unwrap()
            };
            // unsafe{rofi_c_put(src,  dst.into(), pe).expect("rofi_c_put failed")};
        } else {
            // if !(src.contains(dst) || src.contains(&(dst + src.len()))) {
            unsafe {
                dst.as_mut_ptr::<T>().write(*src);
            }
            // unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), 1) };
            // } else {
            //     unsafe {
            //         std::ptr::copy(src.as_ptr(), dst.as_mut_ptr(), 1);
            //     }
            // }
        }
    }
    #[tracing::instrument(skip_all, level = "debug")]
    fn inner_put_all(&self, src: &CommSlice<T>, dst: &CommAllocAddr, pes: &Vec<usize>) {
        trace!(
            "put all src: {:?} dsts: {:?} len: {}",
            src.usize_addr(),
            dst,
            src.len()
        );
        for pe in pes {
            self.inner_put(*pe, src, dst);
        }
    }
    #[tracing::instrument(skip_all, level = "debug")]
    fn inner_get(&self, pe: usize, src: &CommAllocAddr, mut dst: CommSlice<T>) {
        trace!(
            "getting src: {:?} dst: {:?} len: {}",
            src,
            dst.usize_addr(),
            dst.len()
        );
        if pe != self.my_pe {
            unsafe { self.ofi.get(pe, src, &mut dst, false).unwrap() };
            // unsafe{rofi_c_get(src.into(),  dst.as_mut_slice(), pe).expect("rofi_c_get failed")};
        } else {
            if !(dst.contains(src) || dst.contains(&(src + dst.len()))) {
                unsafe {
                    std::ptr::copy_nonoverlapping(src.as_mut_ptr(), dst.as_mut_ptr(), dst.len());
                }
            } else {
                unsafe {
                    std::ptr::copy(src.as_mut_ptr(), dst.as_mut_ptr(), dst.len());
                }
            }
        }
    }
    fn exec_op(&self) {
        match &self.op {
            Op::Put(pe, src, dst) => {
                self.inner_put(*pe, src, dst);
            }
            Op::Put2(pe, src, dst) => {
                self.inner_put2(*pe, src, dst);
            }
            Op::PutTest(pe, src, dst) => {
                self.inner_put_test(*pe, src, dst);
            }
            Op::PutAll(src, dst, pes) => {
                self.inner_put_all(src, dst, pes);
            }
            Op::Get(pe, src, dst) => {
                self.inner_get(*pe, src, dst.clone());
            }
        }
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        // rofi_c_wait();
        self.ofi.wait_all().unwrap();
        self.spawned = true;
        // Ok(())
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        let ofi = self.ofi.clone();
        self.scheduler
            .spawn_task(async move { ofi.wait_all().unwrap() }, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<LibfabricFuture<T>> for RdmaHandle<T> {
    fn from(f: LibfabricFuture<T>) -> RdmaHandle<T> {
        RdmaHandle {
            future: RdmaFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricFuture<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
            self.ofi.wait_all().unwrap();
            *self.project().spawned = true;
        } else {
            self.ofi.wait_all().unwrap();
        }
        // rofi_c_wait();

        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAtFuture<T> {
    pub(crate) pe: usize,
    pub(crate) ofi: Arc<Ofi>,
    pub(super) src: CommAllocAddr,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    pub(crate) result: MaybeUninit<T>,
}

impl<T: Remote> LibfabricAtFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        trace!("getting src: {:?} ", self.src);
        unsafe {
            self.ofi.inner_get(
                self.pe,
                *(&self.src as &usize),
                std::slice::from_raw_parts_mut(self.result.as_mut_ptr(), 1),
                false,
            )
        };
    }

    pub(crate) fn block(mut self) -> T {
        self.exec_at();
        self.spawned = true;
        self.ofi.wait_all().unwrap();
        unsafe { self.result.assume_init() }
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_at();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(
            async move {
                self.ofi.wait_all().unwrap();
                unsafe { self.result.assume_init() }
            },
            counters,
        )
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricAtFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<LibfabricAtFuture<T>> for RdmaAtHandle<T> {
    fn from(f: LibfabricAtFuture<T>) -> RdmaAtHandle<T> {
        RdmaAtHandle {
            future: RdmaAtFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricAtFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        let this = self.project();
        *this.spawned = true;
        this.ofi.wait_all().unwrap();

        Poll::Ready(unsafe {
            let mut res = MaybeUninit::uninit();
            std::mem::swap(this.result, &mut res);
            res.assume_init()
        })
    }
}

impl CommRdma for LibfabricComm {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        src: CommSlice<T>, //contains alloc info
        remote_addr: CommAllocAddr,
    ) -> RdmaHandle<T> {
        self.put_amt
            .fetch_add(src.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
        LibfabricFuture {
            my_pe: self.my_pe,
            ofi: self.ofi.clone(),
            op: Op::Put(pe, src, remote_addr),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn put2<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        src: T, //contains alloc info
        remote_addr: CommAllocAddr,
    ) -> RdmaHandle<T> {
        // self.put_amt
        //     .fetch_add(std::mem::size_of::<T>(), Ordering::SeqCst);
        LibfabricFuture {
            my_pe: self.my_pe,
            ofi: self.ofi.clone(),
            op: Op::Put2(pe, src, remote_addr),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn put_test<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        src: T,
        remote_addr: CommAllocAddr,
    ) {
        // if pe != self.my_pe {
        unsafe {
            self.ofi
                .inner_put(
                    pe,
                    std::slice::from_ref(&src),
                    *(&remote_addr as &usize),
                    false,
                )
                .unwrap()
        };

        // } else {
        //     unsafe {
        //         remote_addr.as_mut_ptr::<T>().write(src);
        //     }
        // }
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        src: CommSlice<T>,
        remote_addr: CommAllocAddr,
    ) -> RdmaHandle<T> {
        self.put_amt.fetch_add(
            src.len() * std::mem::size_of::<T>() * self.num_pes,
            Ordering::SeqCst,
        );
        let pes = (0..self.num_pes).collect();
        LibfabricFuture {
            my_pe: self.my_pe,
            ofi: self.ofi.clone(),
            op: Op::PutAll(src, remote_addr, pes),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        src_addr: CommAllocAddr,
        dst: CommSlice<T>,
    ) -> RdmaHandle<T> {
        self.get_amt
            .fetch_add(dst.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
        LibfabricFuture {
            my_pe: self.my_pe,
            ofi: self.ofi.clone(),
            op: Op::Get(pe, src_addr, dst),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }

    fn get_test<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        src_addr: CommAllocAddr,
    ) -> T {
        let mut dst: std::mem::MaybeUninit<T> = std::mem::MaybeUninit::uninit();

        // if pe != self.my_pe {
        unsafe {
            self.ofi
                .inner_get(
                    pe,
                    *(&src_addr as &usize),
                    std::slice::from_mut(&mut *dst.as_mut_ptr()),
                    true,
                )
                .unwrap()
        };
        unsafe { dst.assume_init() }
        // } else {
        //     unsafe {
        //         src_addr.as_mut_ptr::<T>().read()
        //     }
        // }
    }
    fn at<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        src: CommAllocAddr,
    ) -> RdmaAtHandle<T> {
        self.get_amt
            .fetch_add(std::mem::size_of::<T>(), Ordering::SeqCst);
        LibfabricAtFuture {
            pe,
            ofi: self.ofi.clone(),
            src,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: MaybeUninit::uninit(),
        }
        .into()
    }
}

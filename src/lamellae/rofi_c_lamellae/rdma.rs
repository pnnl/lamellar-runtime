use std::{
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
        comm::rdma::{CommRdma, RdmaFuture, RdmaHandle, Remote},
        CommAllocAddr, CommSlice,
    },
    warnings::RuntimeWarning,
    LamellarTask,
};

use super::{fabric::*,{comm::RofiCComm, Scheduler}};

pub(super) enum Op<T> {
    Put(usize, CommSlice<T>, CommAllocAddr),
    PutAll(CommSlice<T>,CommAllocAddr, Vec<usize>),
    Get(usize, CommAllocAddr, CommSlice<T>),
    Atomic,
}

#[pin_project(PinnedDrop)]
pub(crate) struct RofiCFuture<T> {
    pub(crate) my_pe: usize,
    pub(super) op: Op<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> RofiCFuture<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn inner_put(&self, pe: usize, src: &CommSlice<T>, dst: &CommAllocAddr) {
        trace!(
            "putting src: {:?} dst: {:?} len: {} num bytes {}",
            src.addr,
            dst,
            src.len(),
            src.len() * std::mem::size_of::<T>()
        );
        if pe != self.my_pe {
            unsafe{rofi_c_put(src,  dst.into(), pe).expect("rofi_c_put failed")};
        }
        else{
            if !(src.contains(dst) || src.contains(dst + src.len())) {
                unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), src.len()) };
            } else {
                unsafe {
                    std::ptr::copy(src.as_ptr(), dst.as_mut_ptr(), src.len());
                }
            }
        }
        
    }
    #[tracing::instrument(skip_all, level = "debug")]
    fn inner_put_all(&self, src: &CommSlice<T>, dst: &CommAllocAddr, pes: &Vec<usize>) {
        trace!(
            "put all src: {:?} dsts: {:?} len: {}",
            src.addr,
            dst,
            src.len()
        );
        for pe in pes {
            self.inner_put(*pe,src, dst);
        }
    }
    #[tracing::instrument(skip_all, level = "debug")]
    fn inner_get(&self,pe: usize, src: &CommAllocAddr, mut dst: CommSlice<T>) {
        trace!(
            "getting src: {:?} dst: {:?} len: {}",
            src,
            dst.addr,
            dst.len()
        );
        if pe != self.my_pe {
            unsafe{rofi_c_get(src.into(),  dst.as_mut_slice(), pe).expect("rofi_c_get failed")};
        }
        else{
            if !(dst.contains(src) || dst.contains(src + dst.len())) {
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
            Op::Put(pe,src, dst) => {
                self.inner_put(*pe, src, dst);
            }
            Op::PutAll(src, dst,pes) => {
                self.inner_put_all(src, dst,pes);
            }
            Op::Get(pe,src, dst) => {
                self.inner_get(*pe, src, *dst);
            }
            Op::Atomic => {}
        }
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        rofi_c_wait();
        self.spawned = true;
        // Ok(())
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        self.spawned = true;
        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.spawn_task(async {}, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for RofiCFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<RofiCFuture<T>> for RdmaHandle<T> {
    fn from(f: RofiCFuture<T>) -> RdmaHandle<T> {
        RdmaHandle {
            future: RdmaFuture::RofiC(f),
        }
    }
}

impl<T: Remote> Future for RofiCFuture<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
            *self.project().spawned = true;
        }
        rofi_c_wait();
        Poll::Ready(())
    }
}

impl CommRdma for RofiCComm {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        pe: usize,
        src: CommSlice<T>,
        remote_addr: CommAllocAddr,
    ) -> RdmaHandle<T> {
        self.put_amt
            .fetch_add(src.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
        RofiCFuture {
            my_pe: self.my_pe,
            op: Op::Put(pe,src, remote_addr),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
        
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
        RofiCFuture {
            my_pe: self.my_pe,
            op: Op::PutAll(src, remote_addr,pes),
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
        RofiCFuture {
            my_pe: self.my_pe,
            op: Op::Get(pe,src_addr, dst),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
}

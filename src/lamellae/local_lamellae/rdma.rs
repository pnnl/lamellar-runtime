use std::{
    pin::Pin, sync::{atomic::Ordering, Arc}, task::{Context, Poll}
};

use futures_util::Future;

use crate::{active_messaging::{AMCounters, ActiveMessaging}, lamellae::comm::{
    rdma::{CommRdma, RdmaHandle, Remote},
    CommAllocAddr, CommSlice
}, LamellarTask, LamellarTeamRT};

use super::{comm::LocalComm, Scheduler};

pub(super) enum Op<T>{
    Put( CommSlice<T>, CommAllocAddr), //for local lamellae put_all is equivalent to put
    Get(CommAllocAddr, CommSlice<T>),
    Atomic,
}
pub(crate) struct LocalFuture<T> {
    pub(crate) op: Op<T>,
}

impl<T: Remote> LocalFuture<T> {
    fn inner_put(&self,  src: &CommSlice<T>, dst: &CommAllocAddr) {
        if !(src.contains(dst) || src.contains(dst + src.len())) {
            unsafe {
                std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), src.len());
            }
        } else {
            unsafe {
                std::ptr::copy(src.as_ptr(), dst.as_mut_ptr(), src.len());
            }
        }
    }
    fn inner_get(&self, src: &CommAllocAddr, dst: &CommSlice<T>) {
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
    fn exec_op(&self) {
        match &self.op {
            Op::Put(src,dst) => {
                self.inner_put(src,dst);
            }
            Op::Get(src,dst) => {
                self.inner_get(src,dst);
            }
            Op::Atomic => {}
        }
    }
    pub(crate) fn block( self)  {
        self.exec_op();
        // Ok(())
    }
    pub(crate) fn spawn(mut self,scheduler: &Arc<Scheduler>, outstanding_reqs: Vec<Arc<AMCounters>>) -> LamellarTask<()> {
        scheduler.spawn_task(self,outstanding_reqs)
    }
}


impl<T: Remote> Future for LocalFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.exec_op();
        Poll::Ready(())
    }
}

impl<T: Remote> From<LocalFuture<T>> for RdmaHandle<T>{
    fn from(f: LocalFuture<T>) -> RdmaHandle<T> {
        RdmaHandle::Local(f)
    }
}

impl CommRdma for LocalComm {
    fn put<T: Remote>(&self, _pe: usize, src: CommSlice<T>, dst: CommAllocAddr) -> RdmaHandle<T> {
        self.put_amt.fetch_add(src.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
        LocalFuture {op: Op::Put(src,dst)}.into()
    }
    fn put_all<T: Remote>(&self, src: CommSlice<T>, dst: CommAllocAddr) -> RdmaHandle<T> {
        self.put_amt.fetch_add(src.len() * std::mem::size_of::<T>() * self.num_pes, Ordering::SeqCst);
        LocalFuture {op: Op::Put(src,dst)}.into()
    }
    fn get<T: Remote>(&self, _pe: usize, src: CommAllocAddr, dst: CommSlice<T>) -> RdmaHandle<T> {
        self.get_amt.fetch_add(dst.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
        LocalFuture {op: Op::Get(src,dst)}.into()
    }
}

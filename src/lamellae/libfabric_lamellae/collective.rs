use crate::{
    active_messaging::AMCounters, lamellae::{collective::{CollectiveAllReduceInPlaceOpFuture, CollectiveAllReduceInPlaceOpHandle, CollectiveAllReduceIntoBufferOpFuture, CollectiveAllReduceIntoBufferOpHandle, CollectiveAllReduceOpFuture, CollectiveAllReduceOpHandle, CommAllocCollectiveAllReduce}, comm::collective::ReduceOp}, warnings::RuntimeWarning, AsLamellarBuffer, LamellarBuffer, LamellarTask, Remote
};

use super::{
    fabric::{LibfabricAlloc, OneSidedLibfabricAlloc, CachedContext},
    Scheduler,
};

use pin_project::{pin_project, pinned_drop};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tracing::trace;


#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricCollectiveAllReduceFuture<T> {
    pub(crate) alloc: LibfabricAlloc,
    pub(super) op: ReduceOp,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    pub(crate) ctx: Option<CachedContext>,
}

impl<T: Remote> LibfabricCollectiveAllReduceFuture<T> {
    fn exec_op(&mut self) {
        let result_ptr = self.result.as_mut_ptr();
        println!(
            "performing collective reduce op: {:?} result ptr: {:?} ",
            self.op,
            result_ptr
        );
        let ctx = LibfabricAlloc::allreduce_inner(
            &self.alloc,
            &self.op,
            &mut self.result,
            false,
        )
        .unwrap();
        self.ctx = Some(ctx);
        println!(
            "collective reduce op: {:?} initiated",
            self.op,
        );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        self.alloc.ofi.wait_for_completion(self.ctx.as_ref().unwrap()).unwrap();
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        res
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_op();

        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricCollectiveAllReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveAllReduceFuture").print();
        }
    }
}

impl<T> From<LibfabricCollectiveAllReduceFuture<T>> for CollectiveAllReduceOpHandle<T> {
    fn from(f: LibfabricCollectiveAllReduceFuture<T>) -> CollectiveAllReduceOpHandle<T> {
        CollectiveAllReduceOpHandle {
            future: CollectiveAllReduceOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricCollectiveAllReduceFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_for_completion(self.ctx.as_ref().unwrap()).unwrap();
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        Poll::Ready(res)
    }
}


#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricCollectiveAllReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricAlloc,
    pub(super) op: ReduceOp,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    pub(crate) ctx: Option<CachedContext>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricCollectiveAllReduceIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        // println!(
        //     "performing collective reduce op: {:?} result ptr: {:?} ",
        //     self.op,
        //     result_ptr
        // );
        let ctx = LibfabricAlloc::allreduce_inner(
            &self.alloc,
            &self.op,
            self.result.as_mut_slice(),
            false,
        )
        .unwrap();
        self.ctx = Some(ctx);
        println!(
            "collective reduce op: {:?} initiated",
            self.op,
        );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_for_completion(self.ctx.as_ref().unwrap()).unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricCollectiveAllReduceIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveAllReduceIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricCollectiveAllReduceIntoBufferFuture<T, B>> for CollectiveAllReduceIntoBufferOpHandle<T, B> {
    fn from(f: LibfabricCollectiveAllReduceIntoBufferFuture<T, B>) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        CollectiveAllReduceIntoBufferOpHandle {
            future: CollectiveAllReduceIntoBufferOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricCollectiveAllReduceIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_for_completion(self.ctx.as_ref().unwrap()).unwrap();
        Poll::Ready(())
    }
}


#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricCollectiveAllReduceInPlaceFuture<T> {
    pub(crate) alloc: LibfabricAlloc,
    pub(super) op: ReduceOp,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    pub(crate) ctx: Option<CachedContext>,
    phantom: std::marker::PhantomData<T>,
}

impl<T: Remote> LibfabricCollectiveAllReduceInPlaceFuture<T> {
    fn exec_op(&mut self) {
        println!(
            "performing collective reduce op: {:?} in place ",
            self.op,
        );

        let ctx = LibfabricAlloc::allreduce_inplace_inner::<T>(
            &self.alloc,
            &self.op,
            false,
        )
        .unwrap();
        self.ctx = Some(ctx);
        println!(
            "collective reduce op: {:?} initiated",
            self.op,
        );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        self.alloc.ofi.wait_for_completion(self.ctx.as_ref().unwrap()).unwrap();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricCollectiveAllReduceInPlaceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveAllReduceInPlaceFuture").print();
        }
    }
}

impl<T> From<LibfabricCollectiveAllReduceInPlaceFuture<T>> for CollectiveAllReduceInPlaceOpHandle<T> {
    fn from(f: LibfabricCollectiveAllReduceInPlaceFuture<T>) -> CollectiveAllReduceInPlaceOpHandle<T> {
        CollectiveAllReduceInPlaceOpHandle {
            future: CollectiveAllReduceInPlaceOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricCollectiveAllReduceInPlaceFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_for_completion(self.ctx.as_ref().unwrap()).unwrap();
        Poll::Ready(())
    }
}



impl CommAllocCollectiveAllReduce for LibfabricAlloc {
    fn reduce_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
    ) -> CollectiveAllReduceOpHandle<T> {
        LibfabricCollectiveAllReduceFuture {
            alloc: self.clone(),
            op: op,
            result: vec![T::default(); self.num_bytes()/std::mem::size_of::<T>()],
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            ctx: None,
        }
        .into()
    }
    fn reduce_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        
        LibfabricCollectiveAllReduceIntoBufferFuture {
            alloc: self.clone(),
            op: op,
            result: dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            ctx: None,
        }.into()
    }
    fn reduce_all_in_place<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
    ) -> CollectiveAllReduceInPlaceOpHandle<T> {

        LibfabricCollectiveAllReduceInPlaceFuture {
            alloc: self.clone(),
            op: op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            ctx: None,
            phantom: std::marker::PhantomData,
        }.into()
    }
}
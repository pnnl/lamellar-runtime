use crate::{
    active_messaging::AMCounters, lamellae::{collective::{CollectiveAllReduceInPlaceOpFuture, CollectiveAllReduceInPlaceOpHandle, CollectiveAllReduceIntoBufferOpFuture, CollectiveAllReduceIntoBufferOpHandle, CollectiveAllReduceOpFuture, CollectiveAllReduceOpHandle, CollectiveGatherIntoBufferOpFuture, CollectiveGatherIntoBufferOpHandle, CollectiveGatherOpFuture, CollectiveGatherOpHandle, CollectiveReduceInPlaceOpFuture, CollectiveReduceInPlaceOpHandle, CollectiveReduceIntoBufferOpFuture, CollectiveReduceIntoBufferOpHandle, CollectiveReduceOpFuture, CollectiveReduceOpHandle, CommAllocCollectiveAllReduce, CommAllocCollectiveGather, CommAllocCollectiveReduce, RootOrBuffer, RootOrLamellarBuffer}, comm::collective::ReduceOp}, warnings::RuntimeWarning, AsLamellarBuffer, LamellarBuffer, LamellarTask, Remote
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

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricCollectiveReduceFuture<T> {
    pub(crate) alloc: LibfabricAlloc,
    pub(super) op: ReduceOp,
    pub(crate) target: RootOrBuffer<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    pub(crate) ctx: Option<CachedContext>,
}



impl<T: Remote> LibfabricCollectiveReduceFuture<T> {
    fn exec_op(&mut self) {
        
        let ctx = LibfabricAlloc::reduce_inner(
            &self.alloc,
            &self.op,
            self.target.as_mut_slice(),
            false,
        )
        .unwrap();
        self.ctx = Some(ctx);
        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        self.alloc.ofi.wait_for_completion(self.ctx.as_ref().unwrap()).unwrap();
        match &mut self.target {
            RootOrBuffer::Root(res) => {
                let mut res_vec = Vec::new();
                std::mem::swap(&mut res_vec, res);
                Some(res_vec)
            },
            RootOrBuffer::NotRoot(_) => None,
        }
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Option<Vec<T>>> {
        self.exec_op();

        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricCollectiveReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveReduceFuture").print();
        }
    }
}

impl<T> From<LibfabricCollectiveReduceFuture<T>> for CollectiveReduceOpHandle<T> {
    fn from(f: LibfabricCollectiveReduceFuture<T>) -> CollectiveReduceOpHandle<T> {
        CollectiveReduceOpHandle {
            future: CollectiveReduceOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricCollectiveReduceFuture<T> {
    type Output = Option<Vec<T>>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_for_completion(self.ctx.as_ref().unwrap()).unwrap();
        match &mut self.target {
            RootOrBuffer::Root(res) => {
                let mut res_vec = Vec::new();
                std::mem::swap(&mut res_vec, res);
                Poll::Ready(Some(res_vec))
            },
            RootOrBuffer::NotRoot(_) => Poll::Ready(None),
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricCollectiveReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricAlloc,
    pub(super) op: ReduceOp,
    pub(crate) target: RootOrLamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    pub(crate) ctx: Option<CachedContext>,
}



impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricCollectiveReduceIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        
        let ctx = LibfabricAlloc::reduce_inner(
            &self.alloc,
            &self.op,
            self.target.as_mut_slice(),
            false,
        )
        .unwrap();
        self.ctx = Some(ctx);
        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self)  {
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
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricCollectiveReduceIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveReduceIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricCollectiveReduceIntoBufferFuture<T, B>> for CollectiveReduceIntoBufferOpHandle<T, B> {
    fn from(f: LibfabricCollectiveReduceIntoBufferFuture<T, B>) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        CollectiveReduceIntoBufferOpHandle {
            future: CollectiveReduceIntoBufferOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricCollectiveReduceIntoBufferFuture<T, B> {
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
pub(crate) struct LibfabricCollectiveReduceInPlaceFuture<T> {
    pub(crate) alloc: LibfabricAlloc,
    pub(super) op: ReduceOp,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    pub(crate) ctx: Option<CachedContext>,
    root_pe: Option<usize>,
    phantom: std::marker::PhantomData<T>,
}

impl<T: Remote> LibfabricCollectiveReduceInPlaceFuture<T> {
    fn exec_op(&mut self) {
        println!(
            "performing collective reduce op: {:?} in place ",
            self.op,
        );

        let ctx = LibfabricAlloc::reduce_inplace_inner::<T>(
            &self.alloc,
            &self.op,
            self.root_pe.clone(),
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
impl<T> PinnedDrop for LibfabricCollectiveReduceInPlaceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveReduceInPlaceFuture").print();
        }
    }
}

impl<T> From<LibfabricCollectiveReduceInPlaceFuture<T>> for CollectiveReduceInPlaceOpHandle<T> {
    fn from(f: LibfabricCollectiveReduceInPlaceFuture<T>) -> CollectiveReduceInPlaceOpHandle<T> {
        CollectiveReduceInPlaceOpHandle {
            future: CollectiveReduceInPlaceOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricCollectiveReduceInPlaceFuture<T> {
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


impl CommAllocCollectiveReduce for LibfabricAlloc {
    fn reduce<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        root_pe: usize,
    ) -> CollectiveReduceOpHandle<T> {
        let target =
            if root_pe != self.ofi.my_pe {
                RootOrBuffer::NotRoot(root_pe)
            }
            else {
                RootOrBuffer::Root(vec![T::default(); self.num_bytes()/std::mem::size_of::<T>()])
            };
        LibfabricCollectiveReduceFuture {
            alloc: self.clone(),
            op: op,
            target,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            ctx: None,
        }
        .into()
    }
    fn reduce_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        root_or_buffer: RootOrLamellarBuffer<T, B>
    ) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        
        LibfabricCollectiveReduceIntoBufferFuture {
            alloc: self.clone(),
            op: op,
            target: root_or_buffer,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            ctx: None,
        }.into()
    }
    fn reduce_in_place<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        root_pe: usize,
    ) -> CollectiveReduceInPlaceOpHandle<T> {
        let root =  if root_pe != self.ofi.my_pe {
            Some(root_pe)
        }
        else {
            None
        };
        LibfabricCollectiveReduceInPlaceFuture {
            alloc: self.clone(),
            op: op,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            ctx: None,
            root_pe: root,
            phantom: std::marker::PhantomData,
        }.into()
    }
}


#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricCollectiveGatherFuture<T> {
    pub(crate) alloc: LibfabricAlloc,
    pub(crate) target: RootOrBuffer<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    pub(crate) ctx: Option<CachedContext>,
}



impl<T: Remote> LibfabricCollectiveGatherFuture<T> {
    fn exec_op(&mut self) {
        
        let ctx = LibfabricAlloc::gather_inner(
            &self.alloc,
            self.target.as_mut_slice(),
            false,
        )
        .unwrap();
        self.ctx = Some(ctx);
        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        self.alloc.ofi.wait_for_completion(self.ctx.as_ref().unwrap()).unwrap();
        match &mut self.target {
            RootOrBuffer::Root(res) => {
                let mut res_vec = Vec::new();
                std::mem::swap(&mut res_vec, res);
                Some(res_vec)
            },
            RootOrBuffer::NotRoot(_) => None,
        }
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Option<Vec<T>>> {
        self.exec_op();

        let mut counters = Vec::new();
        std::mem::swap(&mut counters, &mut self.counters);
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricCollectiveGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveGatherFuture").print();
        }
    }
}

impl<T> From<LibfabricCollectiveGatherFuture<T>> for CollectiveGatherOpHandle<T> {
    fn from(f: LibfabricCollectiveGatherFuture<T>) -> CollectiveGatherOpHandle<T> {
        CollectiveGatherOpHandle {
            future: CollectiveGatherOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricCollectiveGatherFuture<T> {
    type Output = Option<Vec<T>>;
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_for_completion(self.ctx.as_ref().unwrap()).unwrap();
        match &mut self.target {
            RootOrBuffer::Root(res) => {
                let mut res_vec = Vec::new();
                std::mem::swap(&mut res_vec, res);
                Poll::Ready(Some(res_vec))
            },
            RootOrBuffer::NotRoot(_) => Poll::Ready(None),
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricCollectiveGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: LibfabricAlloc,
    pub(crate) target: RootOrLamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Vec<Arc<AMCounters>>,
    pub(crate) spawned: bool,
    pub(crate) ctx: Option<CachedContext>,
}



impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricCollectiveGatherIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        
        let ctx = LibfabricAlloc::gather_inner(
            &self.alloc,
            self.target.as_mut_slice(),
            false,
        )
        .unwrap();
        self.ctx = Some(ctx);
        // println!(
        //     "collective reduce op: {:?} initiated",
        //     self.op,
        // );
        self.spawned = true;
    }
    pub(crate) fn block(mut self)  {
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
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricCollectiveGatherIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a LibfabricCollectiveGatherIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricCollectiveGatherIntoBufferFuture<T, B>> for CollectiveGatherIntoBufferOpHandle<T, B> {
    fn from(f: LibfabricCollectiveGatherIntoBufferFuture<T, B>) -> CollectiveGatherIntoBufferOpHandle<T, B> {
        CollectiveGatherIntoBufferOpHandle {
            future: CollectiveGatherIntoBufferOpFuture::Libfabric(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricCollectiveGatherIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        self.alloc.ofi.wait_for_completion(self.ctx.as_ref().unwrap()).unwrap();
        Poll::Ready(())
    }
}

impl CommAllocCollectiveGather for LibfabricAlloc {
    fn gather<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        root_pe: usize,
    ) -> CollectiveGatherOpHandle<T> {
        let target =
            if root_pe != self.ofi.my_pe {
                RootOrBuffer::NotRoot(root_pe)
            }
            else {
                RootOrBuffer::Root(vec![T::default(); self.num_bytes()/std::mem::size_of::<T>() * self.num_pes()])
            };
        LibfabricCollectiveGatherFuture {
            alloc: self.clone(),
            target,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            ctx: None,
        }
        .into()
    }
    fn gather_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        root_or_buffer: RootOrLamellarBuffer<T, B>
    ) -> CollectiveGatherIntoBufferOpHandle<T, B> {
        
        LibfabricCollectiveGatherIntoBufferFuture {
            alloc: self.clone(),
            target: root_or_buffer,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            ctx: None,
        }.into()
    }
}

use std::{
    future::Future,
    mem::MaybeUninit,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tracing::trace;

use pin_project::{pin_project, pinned_drop};

use crate::{
    active_messaging::AMCounters,
    lamellae::{
        libfabric_async_lamellae::fabric::{LibfabricAsyncAlloc, OneSidedLibfabricAsyncAlloc},
        CommAllocAddr, CommAllocRdma, RdmaGetBufferFuture, RdmaGetBufferHandle, RdmaGetFuture,
        RdmaGetHandle, RdmaGetIntoBufferFuture, RdmaGetIntoBufferHandle, RdmaPutFuture,
    },
    memregion::MemregionRdmaInputInner,
    scheduler::Scheduler,
    warnings::RuntimeWarning,
    AsLamellarBuffer, LamellarBuffer, LamellarTask, RdmaHandle, Remote,
};

pub(super) enum AllocOp<T: Remote> {
    Put(usize, T),
    PutBuf(usize, MemregionRdmaInputInner<T>),
    PutAll(Vec<usize>, T),
    PutAllBuf(Vec<usize>, MemregionRdmaInputInner<T>),
}

struct PutFutureData<T: Remote> {
    alloc: LibfabricAsyncAlloc,
    offset: usize,
    op: AllocOp<T>,
}

enum PutState<T: Remote> {
    Created(PutFutureData<T>),
    Spawned(Pin<Box<dyn Future<Output = ()> + Send>>),
    Finished,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncPutFuture<T: Remote> {
    state: PutState<T>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

impl<T: Remote> LibfabricAsyncPutFuture<T> {
    pub(crate) fn block(mut self) {
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || {
            self.scheduler.clone().block_on(async move {
                (&mut self).await;
            });
        })
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        let scheduler = self.scheduler.clone();
        let counters = self.counters.clone();
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match Pin::new(&mut self).poll(&mut cx) {
            Poll::Ready(()) => {
                let done: Pin<Box<dyn Future<Output = ()> + Send>> =
                    Box::pin(std::future::ready(()));
                scheduler.spawn_task(done, counters)
            }
            Poll::Pending => scheduler.spawn_task(self, counters),
        }
    }
}
impl<T: Remote> PutFutureData<T> {
    fn inner_put<'a, 'b>(
        &'a self,
        pe: usize,
        src: &'b [T],
        local_desc: libfabric::mr::MemoryRegionDesc<'b>,
    ) -> impl Future<Output = Result<(), libfabric::error::Error>> + use<'a, 'b, T> {
        trace!(
            "putting src: {:x} dst: {:x} len: {} num bytes {}",
            src.as_ptr() as usize,
            self.alloc.start() + self.offset,
            src.len(),
            std::mem::size_of_val(src)
        );
        unsafe { LibfabricAsyncAlloc::inner_put(&self.alloc, pe, self.offset, src, local_desc) }
    }
    // `Ofi::staging_alloc`: route a large, unregistered source through
    // pre-registered scratch memory so rxm never registers the caller's
    // buffer on the fly. Small/registered sources keep their original
    // fire-and-forget-into-inject semantics.
    async fn exec_op(self) {
        let (src, registered): (&[T], bool) = match &self.op {
            AllocOp::Put(_, src) | AllocOp::PutAll(_, src) => (std::slice::from_ref(src), false),
            AllocOp::PutBuf(_, src) | AllocOp::PutAllBuf(_, src) => {
                (src.as_slice(), src.is_registered())
            }
        };
        let mut staging = None;
        if !registered && std::mem::size_of_val(src) >= self.alloc.ofi.inject_size() {
            if let Some(stage) = self
                .alloc
                .ofi
                .staging_alloc(std::mem::size_of_val(src), std::mem::align_of::<T>())
            {
                unsafe { stage.as_mut_slice::<T>().copy_from_slice(src) };
                staging = Some(stage);
            }
        }
        // Local MR descriptor must match whatever buffer `src` actually
        // points into: the staging pool allocation when staging kicked in,
        // otherwise this array's own allocation.
        let local_desc = match &staging {
            Some(stage) => stage.descriptor(),
            None => self.alloc.descriptor(),
        };
        let src: &[T] = match &staging {
            Some(stage) => unsafe { stage.as_slice::<T>() },
            None => src,
        };
        match &self.op {
            AllocOp::Put(pe, _) | AllocOp::PutBuf(pe, _) => {
                self.inner_put(*pe, src, local_desc)
                    .await
                    .expect("error in put");
            }
            AllocOp::PutAll(pes, _) | AllocOp::PutAllBuf(pes, _) => {
                for pe in pes {
                    self.inner_put(*pe, src, local_desc)
                        .await
                        .expect("error in put");
                }
            }
        }
    }

}

struct GetFutureData<T> {
    alloc: LibfabricAsyncAlloc,
    pe: usize,
    offset: usize,
    result: MaybeUninit<T>,
}

enum GetState<T> {
    Created(GetFutureData<T>),
    Spawned(Pin<Box<dyn Future<Output = T> + Send>>),
    Finished,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncGetFuture<T> {
    state: GetState<T>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

impl<T: Remote> LibfabricAsyncGetFuture<T> {
    pub(crate) fn block(mut self) -> T {
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || {
            self.scheduler
                .clone()
                .block_on(async move { (&mut self).await })
        })
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        let scheduler = self.scheduler.clone();
        let counters = self.counters.clone();
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match Pin::new(&mut self).poll(&mut cx) {
            Poll::Ready(res) => {
                let done: Pin<Box<dyn Future<Output = T> + Send>> =
                    Box::pin(std::future::ready(res));
                scheduler.spawn_task(done, counters)
            }
            Poll::Pending => scheduler.spawn_task(self, counters),
        }
    }
}

impl<T: Remote> GetFutureData<T> {
    //#[tracing::instrument(skip_all, level = "debug")]
    async fn exec_at(mut self) -> T {
        trace!("getting at: {:?} {:?} ", self.pe, self.offset);
        // `Ofi::staging_alloc`: GET into pre-registered scratch, then copy
        // out, so rxm never registers this heap-backed destination.
        let staging = self
            .alloc
            .ofi
            .staging_alloc(std::mem::size_of::<T>(), std::mem::align_of::<T>());
        unsafe {
            match &staging {
                Some(stage) => {
                    self.alloc
                        .inner_get(
                            self.pe,
                            self.offset,
                            stage.as_mut_slice::<T>(),
                            stage.descriptor(),
                        )
                        .await
                        .expect("error in get");
                    self.result.write(stage.as_slice::<T>()[0]);
                }
                None => {
                    let local_desc = self.alloc.descriptor();
                    self.alloc
                        .inner_get(
                            self.pe,
                            self.offset,
                            std::slice::from_raw_parts_mut(self.result.as_mut_ptr(), 1),
                            local_desc,
                        )
                        .await
                        .expect("error in get");
                }
            }
            let mut res = MaybeUninit::uninit();
            std::mem::swap(&mut self.result, &mut res);
            res.assume_init()
        }
    }
}

impl<T: Remote> Future for LibfabricAsyncGetFuture<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match std::mem::replace(&mut mut_self.state, GetState::Finished) {
            GetState::Created(data) => {
                let mut fut: Pin<Box<dyn Future<Output = T> + Send>> = Box::pin(data.exec_at());
                match fut.as_mut().poll(cx) {
                    Poll::Ready(res) => Poll::Ready(res),
                    Poll::Pending => {
                        mut_self.state = GetState::Spawned(fut);
                        Poll::Pending
                    }
                }
            }
            GetState::Spawned(mut fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(res) => Poll::Ready(res),
                Poll::Pending => {
                    mut_self.state = GetState::Spawned(fut);
                    Poll::Pending
                }
            },
            GetState::Finished => panic!("LibfabricAsyncGetFuture polled after completion"),
        }
    }
}

struct GetBufferFutureData<T> {
    alloc: LibfabricAsyncAlloc,
    pe: usize,
    offset: usize,
    len: usize,
    result: MaybeUninit<Vec<T>>,
}

enum GetBufferState<T> {
    Created(GetBufferFutureData<T>),
    Spawned(Pin<Box<dyn Future<Output = Vec<T>> + Send>>),
    Finished,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncGetBufferFuture<T> {
    state: GetBufferState<T>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

impl<T: Remote> LibfabricAsyncGetBufferFuture<T> {
    pub(crate) fn block(mut self) -> Vec<T> {
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || {
            self.scheduler
                .clone()
                .block_on(async move { (&mut self).await })
        })
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        let scheduler = self.scheduler.clone();
        let counters = self.counters.clone();
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match Pin::new(&mut self).poll(&mut cx) {
            Poll::Ready(res) => {
                let done: Pin<Box<dyn Future<Output = Vec<T>> + Send>> =
                    Box::pin(std::future::ready(res));
                scheduler.spawn_task(done, counters)
            }
            Poll::Pending => scheduler.spawn_task(self, counters),
        }
    }
}

impl<T: Remote> GetBufferFutureData<T> {
    //#[tracing::instrument(skip_all, level = "debug")]
    async fn exec_at(mut self) -> Vec<T> {
        trace!("getting at: {:?} {:?} ", self.pe, self.offset);
        // `Ofi::staging_alloc`: GET into pre-registered scratch, then copy
        // out, so rxm never registers this heap-backed destination.
        let staging = self
            .alloc
            .ofi
            .staging_alloc(self.len * std::mem::size_of::<T>(), std::mem::align_of::<T>());
        unsafe {
            let dst = match &staging {
                Some(stage) => {
                    self.alloc
                        .inner_get(
                            self.pe,
                            self.offset,
                            stage.as_mut_slice::<T>(),
                            stage.descriptor(),
                        )
                        .await
                        .expect("error in get_buffer");
                    stage.as_slice::<T>().to_vec()
                }
                None => {
                    let mut dst: Vec<T> = (0..self.len).map(|_| std::mem::zeroed()).collect();
                    let local_desc = self.alloc.descriptor();
                    self.alloc
                        .inner_get(self.pe, self.offset, &mut dst, local_desc)
                        .await
                        .expect("error in get_buffer");
                    dst
                }
            };
            self.result.write(dst);
            let mut res = MaybeUninit::uninit();
            std::mem::swap(&mut self.result, &mut res);
            res.assume_init()
        }
    }
}

impl<T: Remote> Future for LibfabricAsyncGetBufferFuture<T> {
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match std::mem::replace(&mut mut_self.state, GetBufferState::Finished) {
            GetBufferState::Created(data) => {
                let mut fut: Pin<Box<dyn Future<Output = Vec<T>> + Send>> =
                    Box::pin(data.exec_at());
                match fut.as_mut().poll(cx) {
                    Poll::Ready(res) => Poll::Ready(res),
                    Poll::Pending => {
                        mut_self.state = GetBufferState::Spawned(fut);
                        Poll::Pending
                    }
                }
            }
            GetBufferState::Spawned(mut fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(res) => Poll::Ready(res),
                Poll::Pending => {
                    mut_self.state = GetBufferState::Spawned(fut);
                    Poll::Pending
                }
            },
            GetBufferState::Finished => panic!("LibfabricAsyncGetBufferFuture polled after completion"),
        }
    }
}

struct GetIntoBufferFutureData<T: Remote, B: AsLamellarBuffer<T>> {
    alloc: LibfabricAsyncAlloc,
    pe: usize,
    offset: usize,
    dst: LamellarBuffer<T, B>,
}

enum GetIntoBufferState<T: Remote, B: AsLamellarBuffer<T>> {
    Created(GetIntoBufferFutureData<T, B>),
    Spawned(Pin<Box<dyn Future<Output = ()> + Send>>),
    Finished,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncGetIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    state: GetIntoBufferState<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricAsyncGetIntoBufferFuture<T, B> {
    pub(crate) fn block(mut self) {
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || {
            self.scheduler.clone().block_on(async move {
                (&mut self).await;
            });
        })
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        let scheduler = self.scheduler.clone();
        let counters = self.counters.clone();
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match Pin::new(&mut self).poll(&mut cx) {
            Poll::Ready(()) => {
                let done: Pin<Box<dyn Future<Output = ()> + Send>> =
                    Box::pin(std::future::ready(()));
                scheduler.spawn_task(done, counters)
            }
            Poll::Pending => scheduler.spawn_task(self, counters),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> GetIntoBufferFutureData<T, B> {
    async fn exec_op(mut self) {
        // `Ofi::staging_alloc`: only needed when `dst`'s backing store isn't
        // itself registered memory (memregions/CommSlice already are).
        let staging = if self.dst.is_registered() {
            None
        } else {
            self.alloc.ofi.staging_alloc(
                std::mem::size_of_val(self.dst.as_slice()),
                std::mem::align_of::<T>(),
            )
        };
        unsafe {
            match &staging {
                Some(stage) => {
                    LibfabricAsyncAlloc::inner_get(
                        &self.alloc,
                        self.pe,
                        self.offset,
                        stage.as_mut_slice::<T>(),
                        stage.descriptor(),
                    )
                    .await
                    .expect("error in get_into_buffer");
                    self.dst
                        .as_mut_slice()
                        .copy_from_slice(stage.as_slice::<T>());
                }
                None => {
                    let local_desc = self.alloc.descriptor();
                    LibfabricAsyncAlloc::inner_get(
                        &self.alloc,
                        self.pe,
                        self.offset,
                        self.dst.as_mut_slice(),
                        local_desc,
                    )
                    .await
                    .expect("error in get_into_buffer");
                }
            }
        };
    }

}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricAsyncGetIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match std::mem::replace(&mut mut_self.state, GetIntoBufferState::Finished) {
            GetIntoBufferState::Created(data) => {
                let mut fut: Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(data.exec_op());
                match fut.as_mut().poll(cx) {
                    Poll::Ready(()) => Poll::Ready(()),
                    Poll::Pending => {
                        mut_self.state = GetIntoBufferState::Spawned(fut);
                        Poll::Pending
                    }
                }
            }
            GetIntoBufferState::Spawned(mut fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(()) => Poll::Ready(()),
                Poll::Pending => {
                    mut_self.state = GetIntoBufferState::Spawned(fut);
                    Poll::Pending
                }
            },
            GetIntoBufferState::Finished => {
                panic!("LibfabricAsyncGetIntoBufferFuture polled after completion")
            }
        }
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricAsyncPutFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        match &self.state {
            PutState::Created(_) => {
                RuntimeWarning::DroppedHandle("a LibfabricAsyncPutFuture").print();
            }
            PutState::Spawned(_) => {
                eprintln!("[TEMP DEBUG] LibfabricAsyncPutFuture dropped mid-flight (Spawned)");
            }
            PutState::Finished => {}
        }
    }
}

impl<T: Remote> From<LibfabricAsyncPutFuture<T>> for RdmaHandle<T> {
    fn from(f: LibfabricAsyncPutFuture<T>) -> RdmaHandle<T> {
        RdmaHandle {
            future: RdmaPutFuture::LibfabricAsync(f),
        }
    }
}

impl<T: Remote> Future for LibfabricAsyncPutFuture<T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match std::mem::replace(&mut mut_self.state, PutState::Finished) {
            PutState::Created(data) => {
                let mut fut: Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(data.exec_op());
                match fut.as_mut().poll(cx) {
                    Poll::Ready(()) => Poll::Ready(()),
                    Poll::Pending => {
                        mut_self.state = PutState::Spawned(fut);
                        Poll::Pending
                    }
                }
            }
            PutState::Spawned(mut fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(()) => Poll::Ready(()),
                Poll::Pending => {
                    mut_self.state = PutState::Spawned(fut);
                    Poll::Pending
                }
            },
            PutState::Finished => panic!("LibfabricAsyncPutFuture polled after completion"),
        }
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricAsyncGetFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        match &self.state {
            GetState::Created(_) => {
                RuntimeWarning::DroppedHandle("a LibfabricAsyncGetFuture").print();
            }
            GetState::Spawned(_) => {
                eprintln!("[TEMP DEBUG] LibfabricAsyncGetFuture dropped mid-flight (Spawned)");
            }
            GetState::Finished => {}
        }
    }
}

impl<T: Remote> From<LibfabricAsyncGetFuture<T>> for RdmaGetHandle<T> {
    fn from(f: LibfabricAsyncGetFuture<T>) -> RdmaGetHandle<T> {
        RdmaGetHandle {
            future: RdmaGetFuture::LibfabricAsync(f),
        }
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricAsyncGetBufferFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        match &self.state {
            GetBufferState::Created(_) => {
                RuntimeWarning::DroppedHandle("a LibfabricAsyncGetBufferFuture").print();
            }
            GetBufferState::Spawned(_) => {
                eprintln!("[TEMP DEBUG] LibfabricAsyncGetBufferFuture dropped mid-flight (Spawned)");
            }
            GetBufferState::Finished => {}
        }
    }
}

impl<T: Remote> From<LibfabricAsyncGetBufferFuture<T>> for RdmaGetBufferHandle<T> {
    fn from(f: LibfabricAsyncGetBufferFuture<T>) -> RdmaGetBufferHandle<T> {
        RdmaGetBufferHandle {
            future: RdmaGetBufferFuture::LibfabricAsync(f),
        }
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricAsyncGetIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        match &self.state {
            GetIntoBufferState::Created(_) => {
                RuntimeWarning::DroppedHandle("a LibfabricAsyncGetIntoBufferFuture").print();
            }
            GetIntoBufferState::Spawned(_) => {
                eprintln!(
                    "[TEMP DEBUG] LibfabricAsyncGetIntoBufferFuture dropped mid-flight (Spawned)"
                );
            }
            GetIntoBufferState::Finished => {}
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricAsyncGetIntoBufferFuture<T, B>>
    for RdmaGetIntoBufferHandle<T, B>
{
    fn from(f: LibfabricAsyncGetIntoBufferFuture<T, B>) -> RdmaGetIntoBufferHandle<T, B> {
        RdmaGetIntoBufferHandle {
            future: RdmaGetIntoBufferFuture::LibfabricAsync(f),
        }
    }
}

/// PUT with no completion handle. Registered sources and sources below the
/// inject size keep the original fire-and-forget semantics (rxm copies the
/// latter into its own buffers). A larger heap source is staged through
/// registered memory and completed synchronously, since nothing could
/// otherwise keep the staging block alive until the DMA finishes.
fn put_buffer_unmanaged_impl<T: Remote>(
    alloc: &LibfabricAsyncAlloc,
    pes: impl Iterator<Item = usize>,
    offset: usize,
    src: &MemregionRdmaInputInner<T>,
) {
    let slice = src.as_slice();
    if src.is_registered() || std::mem::size_of_val(slice) < alloc.ofi.inject_size() {
        let local_desc = alloc.descriptor();
        for pe in pes {
            unsafe {
                LibfabricAsyncAlloc::inner_put_unmanaged(alloc, pe, offset, slice, local_desc, false)
                    .expect("error in put_buffer_unmanaged")
            };
        }
        return;
    }
    match alloc
        .ofi
        .staging_alloc(std::mem::size_of_val(slice), std::mem::align_of::<T>())
    {
        Some(staging) => {
            unsafe { staging.as_mut_slice::<T>().copy_from_slice(slice) };
            let staged = unsafe { staging.as_slice::<T>() };
            let local_desc = staging.descriptor();
            for pe in pes {
                unsafe {
                    LibfabricAsyncAlloc::inner_put_unmanaged(
                        alloc, pe, offset, staged, local_desc, false,
                    )
                    .expect("error in put_buffer_unmanaged")
                };
            }
            let _ = alloc.ofi.wait_all();
        }
        None => {
            let local_desc = alloc.descriptor();
            for pe in pes {
                unsafe {
                    LibfabricAsyncAlloc::inner_put_unmanaged(alloc, pe, offset, slice, local_desc, false)
                        .expect("error in put_buffer_unmanaged")
                };
            }
        }
    }
}

impl CommAllocRdma for LibfabricAsyncAlloc {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        LibfabricAsyncPutFuture {
            state: PutState::Created(PutFutureData {
                alloc: self.clone(),
                offset,
                op: AllocOp::Put(pe, src),
            }),
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        trace!(
            "put unamanaged dst: {pe}  offset: {offset} size_of<T> {}",
            std::mem::size_of::<T>()
        );
        if pe != self.ofi.my_pe {
            let local_desc = self.descriptor();
            unsafe {
                LibfabricAsyncAlloc::inner_put_unmanaged(
                    &self,
                    pe,
                    offset,
                    std::slice::from_ref(&src),
                    local_desc,
                    std::mem::size_of::<T>() >= self.ofi.inject_size(), // stack src: only inject copies it
                )
                .expect("error in put_unmanaged")
            };
        } else {
            unsafe {
                trace!(
                    "put unmanaged local copy {:?} {:?}",
                    self.as_mut_slice::<T>().as_ptr(),
                    self.as_mut_slice::<T>().as_ptr().add(offset)
                )
            };
            unsafe { self.as_mut_slice::<T>()[offset] = src };
        }
    }
    fn put_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        LibfabricAsyncPutFuture {
            state: PutState::Created(PutFutureData {
                alloc: self.clone(),
                offset,
                op: AllocOp::PutBuf(pe, src.into()),
            }),
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn put_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) {
        let src = src.into();
        put_buffer_unmanaged_impl(self, std::iter::once(pe), offset, &src);
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        offset: usize,
    ) -> RdmaHandle<T> {
        let pes = (0..self.num_pes()).collect();
        LibfabricAsyncPutFuture {
            state: PutState::Created(PutFutureData {
                alloc: self.clone(),
                offset,
                op: AllocOp::PutAll(pes, src.into()),
            }),
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize) {
        let local_desc = self.descriptor();
        for pe in 0..self.num_pes() {
            if pe != self.ofi.my_pe {
                unsafe {
                    LibfabricAsyncAlloc::inner_put_unmanaged(
                        &self,
                        pe,
                        offset,
                        std::slice::from_ref(&src),
                        local_desc,
                        std::mem::size_of::<T>() >= self.ofi.inject_size(), // stack src: only inject copies it
                    )
                    .expect("error in put_all_unmanaged")
                };
            } else {
                let dst = CommAllocAddr(self.start() + offset);
                unsafe { dst.as_mut_ptr::<T>().write(src) };
            }
        }
    }
    fn put_all_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) -> RdmaHandle<T> {
        let pes = (0..self.num_pes()).collect();
        LibfabricAsyncPutFuture {
            state: PutState::Created(PutFutureData {
                alloc: self.clone(),
                offset,
                op: AllocOp::PutAllBuf(pes, src.into()),
            }),
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn put_all_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) {
        // `inner_put_unmanaged` already special-cases pe == my_pe with its
        // own ptr::copy, so routing every pe (including local) through the
        // shared staging-aware helper matches the reference/backend-A
        // template and gets local puts staging coverage for free too.
        let src = src.into();
        put_buffer_unmanaged_impl(self, 0..self.num_pes(), offset, &src);
    }

    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        LibfabricAsyncGetFuture {
            state: GetState::Created(GetFutureData {
                alloc: self.clone(),
                pe,
                offset,
                result: MaybeUninit::uninit(),
            }),
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        let mut result: T = unsafe { std::mem::zeroed() };
        let staging = self
            .ofi
            .staging_alloc(std::mem::size_of::<T>(), std::mem::align_of::<T>());
        match &staging {
            Some(stage) => {
                let local_desc = stage.descriptor();
                _scheduler.clone().block_on(async {
                    unsafe {
                        LibfabricAsyncAlloc::inner_get(
                            self,
                            pe,
                            offset,
                            stage.as_mut_slice::<T>(),
                            local_desc,
                        )
                        .await
                        .expect("error in blocking_get");
                    }
                });
                result = unsafe { stage.as_slice::<T>()[0] };
            }
            None => {
                let local_desc = self.descriptor();
                _scheduler.clone().block_on(async {
                    unsafe {
                        LibfabricAsyncAlloc::inner_get(
                            self,
                            pe,
                            offset,
                            std::slice::from_mut(&mut result),
                            local_desc,
                        )
                        .await
                        .expect("error in blocking_get");
                    }
                });
            }
        }
        result
    }
    fn put_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        src: T,
        pe: usize,
        offset: usize,
    ) {
        let local_desc = self.descriptor();
        _scheduler.clone().block_on(async {
            unsafe {
                LibfabricAsyncAlloc::inner_put(
                    self,
                    pe,
                    offset,
                    std::slice::from_ref(&src),
                    local_desc,
                )
                .await
                .expect("error in blocking_put");
            }
        });
    }
    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> RdmaGetBufferHandle<T> {
        LibfabricAsyncGetBufferFuture {
            state: GetBufferState::Created(GetBufferFutureData {
                alloc: self.clone(),
                pe,
                offset,
                len,
                result: MaybeUninit::uninit(),
            }),
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn blocking_get_buffer<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> Vec<T> {
        let staging = self
            .ofi
            .staging_alloc(len * std::mem::size_of::<T>(), std::mem::align_of::<T>());
        match &staging {
            Some(stage) => {
                let local_desc = stage.descriptor();
                _scheduler.clone().block_on(async {
                    unsafe {
                        LibfabricAsyncAlloc::inner_get(
                            self,
                            pe,
                            offset,
                            stage.as_mut_slice::<T>(),
                            local_desc,
                        )
                        .await
                        .expect("error in blocking_get_buffer");
                    }
                });
                unsafe { stage.as_slice::<T>() }.to_vec()
            }
            None => {
                let mut dst: Vec<T> = (0..len).map(|_| unsafe { std::mem::zeroed() }).collect();
                let local_desc = self.descriptor();
                _scheduler.clone().block_on(async {
                    unsafe {
                        LibfabricAsyncAlloc::inner_get(self, pe, offset, &mut dst, local_desc)
                            .await
                            .expect("error in blocking_get_buffer");
                    }
                });
                dst
            }
        }
    }
    fn get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) -> RdmaGetIntoBufferHandle<T, B> {
        LibfabricAsyncGetIntoBufferFuture {
            state: GetIntoBufferState::Created(GetIntoBufferFutureData {
                alloc: self.clone(),
                pe,
                offset,
                dst,
            }),
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn blocking_get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        _scheduler: &Arc<Scheduler>,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        if dst.is_registered() {
            let local_desc = self.descriptor();
            _scheduler.clone().block_on(async {
                unsafe {
                    LibfabricAsyncAlloc::inner_get(&self, pe, offset, dst.as_mut_slice(), local_desc)
                        .await
                        .expect("error in blocking_get_into_buffer");
                }
            })
        } else {
            let staging = self.ofi.staging_alloc(
                std::mem::size_of_val(dst.as_slice()),
                std::mem::align_of::<T>(),
            );
            match &staging {
                Some(stage) => {
                    let local_desc = stage.descriptor();
                    _scheduler.clone().block_on(async {
                        unsafe {
                            LibfabricAsyncAlloc::inner_get(
                                &self,
                                pe,
                                offset,
                                stage.as_mut_slice::<T>(),
                                local_desc,
                            )
                            .await
                            .expect("error in blocking_get_into_buffer");
                        }
                    });
                    unsafe { dst.as_mut_slice().copy_from_slice(stage.as_slice::<T>()) };
                }
                None => {
                    let local_desc = self.descriptor();
                    _scheduler.clone().block_on(async {
                        unsafe {
                            LibfabricAsyncAlloc::inner_get(
                                &self,
                                pe,
                                offset,
                                dst.as_mut_slice(),
                                local_desc,
                            )
                            .await
                            .expect("error in blocking_get_into_buffer");
                        }
                    })
                }
            }
        }
    }

    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        if dst.is_registered() {
            let local_desc = self.descriptor();
            async_std::task::block_on(async {
                unsafe {
                    LibfabricAsyncAlloc::inner_get_unmanaged(
                        &self,
                        pe,
                        offset,
                        dst.as_mut_slice(),
                        local_desc,
                    )
                    .await
                    .expect("error in get_into_buffer_unmanaged")
                };
            });
            return;
        }
        let staging = self.ofi.staging_alloc(
            std::mem::size_of_val(dst.as_slice()),
            std::mem::align_of::<T>(),
        );
        match &staging {
            Some(stage) => {
                let local_desc = stage.descriptor();
                async_std::task::block_on(async {
                    unsafe {
                        LibfabricAsyncAlloc::inner_get_unmanaged(
                            &self,
                            pe,
                            offset,
                            stage.as_mut_slice::<T>(),
                            local_desc,
                        )
                        .await
                        .expect("error in get_into_buffer_unmanaged")
                    };
                });
                unsafe { dst.as_mut_slice().copy_from_slice(stage.as_slice::<T>()) };
            }
            None => {
                let local_desc = self.descriptor();
                async_std::task::block_on(async {
                    unsafe {
                        LibfabricAsyncAlloc::inner_get_unmanaged(
                            &self,
                            pe,
                            offset,
                            dst.as_mut_slice(),
                            local_desc,
                        )
                        .await
                        .expect("error in get_into_buffer_unmanaged")
                    };
                });
            }
        }
    }
}

impl CommAllocRdma for OneSidedLibfabricAsyncAlloc {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        // assert_eq!(
        //     pe, self.remote_pe,
        //     "put called on OneSidedLibfabricAsyncAlloc with incorrect pe: {} expected pe: {}",
        //     pe, self.remote_pe
        // );

        LibfabricAsyncPutFuture {
            state: PutState::Created(PutFutureData {
                alloc: self.alloc.clone(),
                offset: offset,
                op: AllocOp::Put(pe, src),
            }),
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        // assert_eq!(
        //     pe, self.remote_pe,
        //     "put_unmanaged called on OneSidedLibfabricAsyncAlloc with incorrect pe: {} expected pe: {}",
        //     pe, self.remote_pe
        // );
        if pe != self.alloc.ofi.my_pe {
            let local_desc = self.alloc.descriptor();
            unsafe {
                LibfabricAsyncAlloc::inner_put_unmanaged(
                    &self.alloc,
                    pe,
                    offset,
                    std::slice::from_ref(&src),
                    local_desc,
                    std::mem::size_of::<T>() >= self.alloc.ofi.inject_size(), // stack src: only inject copies it
                )
                .expect("error in put_unmanaged")
            };
        } else {
            unsafe { self.alloc.as_mut_slice::<T>()[offset] = src };
        }
    }
    fn put_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        src: T,
        pe: usize,
        offset: usize,
    ) {
        assert_eq!(
            pe, self.remote_pe,
            "put_blocking called on OneSidedLibfabricAsyncAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let local_desc = self.alloc.descriptor();
        _scheduler.clone().block_on(async {
            unsafe {
                LibfabricAsyncAlloc::inner_put(
                    &self.alloc,
                    pe,
                    offset,
                    std::slice::from_ref(&src),
                    local_desc,
                )
                .await
                .expect("error in OneSided blocking_put");
            }
        });
    }
    fn put_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "put_buffer called on OneSidedLibfabricAsyncAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );

        LibfabricAsyncPutFuture {
            state: PutState::Created(PutFutureData {
                alloc: self.alloc.clone(),
                offset,
                op: AllocOp::PutBuf(pe, src.into()),
            }),
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn put_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) {
        assert_eq!(
            pe, self.remote_pe,
            "put_buffer_unmanaged called on OneSidedLibfabricAsyncAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let src = src.into();
        put_buffer_unmanaged_impl(&self.alloc, std::iter::once(pe), offset, &src);
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        offset: usize,
    ) -> RdmaHandle<T> {
        self.put(scheduler, counters, src, self.remote_pe, offset)
    }
    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize) {
        self.put_unmanaged(src, self.remote_pe, offset);
    }
    fn put_all_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) -> RdmaHandle<T> {
        self.put_buffer(scheduler, counters, src, self.remote_pe, offset)
    }
    fn put_all_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) {
        self.put_buffer_unmanaged(src, self.remote_pe, offset);
    }

    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "get called on OneSidedLibfabricAsyncAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricAsyncGetFuture {
            state: GetState::Created(GetFutureData {
                alloc: self.alloc.clone(),
                pe,
                offset,
                result: MaybeUninit::uninit(),
            }),
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        let mut result: T = unsafe { std::mem::zeroed() };
        let staging = self
            .alloc
            .ofi
            .staging_alloc(std::mem::size_of::<T>(), std::mem::align_of::<T>());
        match &staging {
            Some(stage) => {
                let local_desc = stage.descriptor();
                _scheduler.clone().block_on(async {
                    unsafe {
                        LibfabricAsyncAlloc::inner_get(
                            &self.alloc,
                            pe,
                            offset,
                            stage.as_mut_slice::<T>(),
                            local_desc,
                        )
                        .await
                        .expect("error in OneSided blocking_get");
                    }
                });
                result = unsafe { stage.as_slice::<T>()[0] };
            }
            None => {
                let local_desc = self.alloc.descriptor();
                _scheduler.clone().block_on(async {
                    unsafe {
                        LibfabricAsyncAlloc::inner_get(
                            &self.alloc,
                            pe,
                            offset,
                            std::slice::from_mut(&mut result),
                            local_desc,
                        )
                        .await
                        .expect("error in OneSided blocking_get");
                    }
                });
            }
        }
        result
    }
    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> RdmaGetBufferHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "get_buffer called on OneSidedLibfabricAsyncAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricAsyncGetBufferFuture {
            state: GetBufferState::Created(GetBufferFutureData {
                alloc: self.alloc.clone(),
                pe,
                offset,
                len,
                result: MaybeUninit::uninit(),
            }),
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn blocking_get_buffer<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> Vec<T> {
        let staging = self
            .alloc
            .ofi
            .staging_alloc(len * std::mem::size_of::<T>(), std::mem::align_of::<T>());
        let dst = match &staging {
            Some(stage) => {
                let local_desc = stage.descriptor();
                _scheduler.clone().block_on(async {
                    unsafe {
                        LibfabricAsyncAlloc::inner_get(
                            &self.alloc,
                            pe,
                            offset,
                            stage.as_mut_slice::<T>(),
                            local_desc,
                        )
                        .await
                        .expect("error in OneSided blocking_get_buffer");
                    }
                });
                unsafe { stage.as_slice::<T>().to_vec() }
            }
            None => {
                let mut dst: Vec<T> = (0..len).map(|_| unsafe { std::mem::zeroed() }).collect();
                let local_desc = self.alloc.descriptor();
                _scheduler.clone().block_on(async {
                    unsafe {
                        LibfabricAsyncAlloc::inner_get(&self.alloc, pe, offset, &mut dst, local_desc)
                            .await
                            .expect("error in OneSided blocking_get_buffer");
                    }
                });
                dst
            }
        };
        dst
    }
    fn get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) -> RdmaGetIntoBufferHandle<T, B> {
        assert_eq!(
            pe, self.remote_pe,
            "get_into_buffer called on OneSidedLibfabricAsyncAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricAsyncGetIntoBufferFuture {
            state: GetIntoBufferState::Created(GetIntoBufferFutureData {
                alloc: self.alloc.clone(),
                pe,
                offset,
                dst,
            }),
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn blocking_get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        _scheduler: &Arc<Scheduler>,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        if dst.is_registered() {
            let local_desc = self.alloc.descriptor();
            _scheduler.clone().block_on(async {
                unsafe {
                    LibfabricAsyncAlloc::inner_get(&self.alloc, pe, offset, dst.as_mut_slice(), local_desc)
                        .await
                        .expect("error in OneSided blocking_get_into_buffer");
                }
            })
        } else if let Some(stage) = self.alloc.ofi.staging_alloc(
            std::mem::size_of_val(dst.as_slice()),
            std::mem::align_of::<T>(),
        ) {
            let local_desc = stage.descriptor();
            _scheduler.clone().block_on(async {
                unsafe {
                    LibfabricAsyncAlloc::inner_get(
                        &self.alloc,
                        pe,
                        offset,
                        stage.as_mut_slice::<T>(),
                        local_desc,
                    )
                    .await
                    .expect("error in OneSided blocking_get_into_buffer");
                }
            });
            unsafe { dst.as_mut_slice().copy_from_slice(stage.as_slice::<T>()) };
        } else {
            let local_desc = self.alloc.descriptor();
            _scheduler.clone().block_on(async {
                unsafe {
                    LibfabricAsyncAlloc::inner_get(&self.alloc, pe, offset, dst.as_mut_slice(), local_desc)
                        .await
                        .expect("error in OneSided blocking_get_into_buffer");
                }
            })
        }
    }
    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        assert_eq!(pe, self.remote_pe, "get_into_buffer_unmanaged called on OneSidedLibfabricAsyncAlloc with incorrect pe: {} expected pe: {}", pe, self.remote_pe);
        if dst.is_registered() {
            let local_desc = self.alloc.descriptor();
            async_std::task::block_on(async {
                unsafe {
                    LibfabricAsyncAlloc::inner_get_unmanaged(
                        &self.alloc,
                        pe,
                        offset,
                        dst.as_mut_slice(),
                        local_desc,
                    )
                    .await
                    .expect("error in get_into_buffer_unmanaged")
                };
            });
        } else if let Some(stage) = self.alloc.ofi.staging_alloc(
            std::mem::size_of_val(dst.as_slice()),
            std::mem::align_of::<T>(),
        ) {
            let local_desc = stage.descriptor();
            async_std::task::block_on(async {
                unsafe {
                    LibfabricAsyncAlloc::inner_get_unmanaged(
                        &self.alloc,
                        pe,
                        offset,
                        stage.as_mut_slice::<T>(),
                        local_desc,
                    )
                    .await
                    .expect("error in get_into_buffer_unmanaged")
                };
            });
            unsafe { dst.as_mut_slice().copy_from_slice(stage.as_slice::<T>()) };
        } else {
            let local_desc = self.alloc.descriptor();
            async_std::task::block_on(async {
                unsafe {
                    LibfabricAsyncAlloc::inner_get_unmanaged(
                        &self.alloc,
                        pe,
                        offset,
                        dst.as_mut_slice(),
                        local_desc,
                    )
                    .await
                    .expect("error in get_into_buffer_unmanaged")
                };
            });
        }
    }
}

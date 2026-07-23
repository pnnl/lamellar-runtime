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
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncPutFuture<T: Remote> {
    fut_data: Option<PutFutureData<T>>,
    fut: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<T: Remote> LibfabricAsyncPutFuture<T> {
    pub(crate) fn block(mut self) {
        let data = self.fut_data.take().unwrap();
        data.block()
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        let data = self.fut_data.take().unwrap();
        data.spawn()
    }
}
impl<T: Remote> PutFutureData<T> {
    fn inner_put<'a, 'b>(
        &'a self,
        pe: usize,
        src: &'b T,
    ) -> impl Future<Output = Result<(), libfabric::error::Error>> + use<'a, 'b, T> {
        trace!(
            "putting src: {:x} dst: {:x} len: {} num bytes {}",
            src as *const T as usize,
            self.alloc.start() + self.offset,
            1,
            std::mem::size_of::<T>()
        );
        unsafe {
            LibfabricAsyncAlloc::inner_put(&self.alloc, pe, self.offset, std::slice::from_ref(src))
        }
    }
    fn inner_put_buf<'a, 'b>(
        &'a self,
        pe: usize,
        src: &'b MemregionRdmaInputInner<T>,
    ) -> impl Future<Output = Result<(), libfabric::error::Error>> + use<'a, 'b, T> {
        unsafe { LibfabricAsyncAlloc::inner_put(&self.alloc, pe, self.offset, src.as_slice()) }
    }
    async fn exec_op(self) {
        match &self.op {
            AllocOp::Put(pe, src) => {
                self.inner_put(*pe, src).await.expect("error in put");
            }
            AllocOp::PutBuf(pe, src) => {
                self.inner_put_buf(*pe, src)
                    .await
                    .expect("error in put_buf");
            }
            AllocOp::PutAll(pes, src) => {
                for pe in pes {
                    self.inner_put(*pe, src).await.expect("error in put");
                }
            }
            AllocOp::PutAllBuf(pes, src) => {
                for pe in pes {
                    self.inner_put_buf(*pe, src)
                        .await
                        .expect("error in put_buf");
                }
            }
        }
    }

    pub(crate) fn block(self) {
        self.scheduler.clone().block_on(async move {
            self.exec_op().await;
        });
    }
    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(
            async move {
                self.exec_op().await;
            },
            counters,
        )
    }
}

struct GetFutureData<T> {
    alloc: LibfabricAsyncAlloc,
    pe: usize,
    offset: usize,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    result: MaybeUninit<T>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncGetFuture<T> {
    fut: Option<Pin<Box<dyn Future<Output = T> + Send>>>,
    fut_data: Option<GetFutureData<T>>,
}

impl<T: Remote> LibfabricAsyncGetFuture<T> {
    pub(crate) fn block(mut self) -> T {
        let data = self.fut_data.take().unwrap();
        data.block()
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        let data = self.fut_data.take().unwrap();
        data.spawn()
    }
}

impl<T: Remote> GetFutureData<T> {
    //#[tracing::instrument(skip_all, level = "debug")]
    async fn exec_at(mut self) -> T {
        trace!("getting at: {:?} {:?} ", self.pe, self.offset);
        unsafe {
            self.alloc
                .inner_get(
                    self.pe,
                    self.offset,
                    std::slice::from_raw_parts_mut(self.result.as_mut_ptr(), 1),
                )
                .await
                .expect("error in get");
            let mut res = MaybeUninit::uninit();
            std::mem::swap(&mut self.result, &mut res);
            res.assume_init()
        }
    }

    pub(crate) fn block(self) -> T {
        self.scheduler
            .clone()
            .block_on(async { self.exec_at().await })
    }
    pub(crate) fn spawn(self) -> LamellarTask<T> {
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.exec_at().await }, counters)
    }
}

impl<T: Remote> Future for LibfabricAsyncGetFuture<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut {
            Some(ref mut fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_at()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

struct GetBufferFutureData<T> {
    alloc: LibfabricAsyncAlloc,
    pe: usize,
    offset: usize,
    len: usize,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    result: MaybeUninit<Vec<T>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncGetBufferFuture<T> {
    fut_data: Option<GetBufferFutureData<T>>,
    fut: Option<Pin<Box<dyn Future<Output = Vec<T>> + Send>>>,
}

impl<T: Remote> LibfabricAsyncGetBufferFuture<T> {
    pub(crate) fn block(mut self) -> Vec<T> {
        let data = self.fut_data.take().unwrap();
        data.block()
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        let data = self.fut_data.take().unwrap();
        data.spawn()
    }
}

impl<T: Remote> GetBufferFutureData<T> {
    //#[tracing::instrument(skip_all, level = "debug")]
    async fn exec_at(mut self) -> Vec<T> {
        trace!("getting at: {:?} {:?} ", self.pe, self.offset);
        unsafe {
            let mut dst: Vec<T> = (0..self.len).map(|_| std::mem::zeroed() ).collect();
            // let dst_mut_slice = std::slice::from_raw_parts_mut(dst.as_mut_ptr(), self.len);

            // dst.set_len(self.len);
            self.alloc
                .inner_get(self.pe, self.offset, &mut dst)
                .await
                .expect("error in get_buffer");
            // let dst = std::mem::transmute::<Vec<MaybeUninit<T>>, Vec<T>>(dst);
            self.result.write(dst);
            let mut res = MaybeUninit::uninit();
            std::mem::swap(&mut self.result, &mut res);
            res.assume_init()
        }
    }

    pub(crate) fn block(self) -> Vec<T> {
        self.scheduler
            .clone()
            .block_on(async { self.exec_at().await })
    }
    pub(crate) fn spawn(self) -> LamellarTask<Vec<T>> {
        let counters = self.counters.clone();
        self.scheduler
            .clone()
            .spawn_task(async move { self.exec_at().await }, counters)
    }
}

impl<T: Remote> Future for LibfabricAsyncGetBufferFuture<T> {
    type Output = Vec<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut {
            Some(ref mut fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_at()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

struct GetIntoBufferFutureData<T: Remote, B: AsLamellarBuffer<T>> {
    alloc: LibfabricAsyncAlloc,
    pe: usize,
    offset: usize,
    dst: LamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricAsyncGetIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    fut_data: Option<GetIntoBufferFutureData<T, B>>,
    fut: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricAsyncGetIntoBufferFuture<T, B> {
    pub(crate) fn block(mut self) {
        let data = self.fut_data.take().unwrap();
        data.block()
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        let data = self.fut_data.take().unwrap();
        data.spawn()
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> GetIntoBufferFutureData<T, B> {
    async fn exec_op(mut self) {
        // if self.pe != self.my_pe {
        unsafe {
            LibfabricAsyncAlloc::inner_get(
                &self.alloc,
                self.pe,
                self.offset,
                self.dst.as_mut_slice(),
            )
            .await
            .expect("error in get_into_buffer");
        };
    }

    pub(crate) fn block(self) {
        self.scheduler.clone().block_on(async move {
            self.exec_op().await;
        });
    }
    pub(crate) fn spawn(self) -> LamellarTask<()> {
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(
            async move {
                self.exec_op().await;
            },
            counters,
        )
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricAsyncGetIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        match mut_self.fut {
            Some(ref mut fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricAsyncPutFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncPutFuture").print();
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
        match mut_self.fut {
            Some(ref mut fut) => fut.as_mut().poll(cx),
            None => {
                let fut_data = mut_self.fut_data.take().unwrap();
                mut_self.fut = Some(Box::pin(fut_data.exec_op()));
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricAsyncGetFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncGetFuture").print();
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
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncGetBufferFuture").print();
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
        if self.fut_data.is_some() {
            RuntimeWarning::DroppedHandle("a LibfabricAsyncGetIntoBufferFuture").print();
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
            fut_data: Some(PutFutureData {
                alloc: self.clone(),
                offset,
                op: AllocOp::Put(pe, src),
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
        }
        .into()
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        trace!(
            "put unamanaged dst: {pe}  offset: {offset} size_of<T> {}",
            std::mem::size_of::<T>()
        );
        if pe != self.ofi.my_pe {
            unsafe {
                LibfabricAsyncAlloc::inner_put_unmanaged(
                    &self,
                    pe,
                    offset,
                    std::slice::from_ref(&src),
                    false,
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
            fut_data: Some(PutFutureData {
                alloc: self.clone(),
                offset,
                op: AllocOp::PutBuf(pe, src.into()),
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
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

        unsafe {
            LibfabricAsyncAlloc::inner_put_unmanaged(&self, pe, offset, src.as_slice(), false)
                .expect("error in put_buffer_unmanaged")
        };
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
            fut_data: Some(PutFutureData {
                alloc: self.clone(),
                offset,
                op: AllocOp::PutAll(pes, src.into()),
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
        }
        .into()
    }
    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize) {
        for pe in 0..self.num_pes() {
            if pe != self.ofi.my_pe {
                unsafe {
                    LibfabricAsyncAlloc::inner_put_unmanaged(
                        &self,
                        pe,
                        offset,
                        std::slice::from_ref(&src),
                        false,
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
            fut_data: Some(PutFutureData {
                alloc: self.clone(),
                offset,
                op: AllocOp::PutAllBuf(pes, src.into()),
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
        }
        .into()
    }
    fn put_all_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) {
        let src = src.into();
        for pe in 0..self.num_pes() {
            if pe != self.ofi.my_pe {
                unsafe {
                    LibfabricAsyncAlloc::inner_put_unmanaged(
                        &self,
                        pe,
                        offset,
                        src.as_slice(),
                        false,
                    )
                    .expect("error in put_all_buffer_unmanaged")
                };
            } else {
                let dst = self.start() + offset;

                if !(src.contains(&dst) || src.contains(&(dst + src.len()))) {
                    unsafe {
                        std::ptr::copy_nonoverlapping(src.as_ptr(), dst as *mut T, src.len())
                    };
                } else {
                    unsafe {
                        std::ptr::copy(src.as_ptr(), dst as *mut T, src.len());
                    }
                }
            }
        }
    }

    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        LibfabricAsyncGetFuture {
            fut_data: Some(GetFutureData {
                alloc: self.clone(),
                pe,
                offset,
                scheduler: scheduler.clone(),
                counters,
                result: MaybeUninit::uninit(),
            }),
            fut: None,
        }
        .into()
    }
    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        let mut result: T = unsafe { std::mem::zeroed() };
        _scheduler.clone().block_on(async {
            unsafe {
                LibfabricAsyncAlloc::inner_get(self, pe, offset, std::slice::from_mut(&mut result))
                    .await
                    .expect("error in blocking_get");
            }
        });
        result
    }
    fn put_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        src: T,
        pe: usize,
        offset: usize,
    ) {
        _scheduler.clone().block_on(async {
            unsafe {
                LibfabricAsyncAlloc::inner_put(self, pe, offset, std::slice::from_ref(&src))
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
            fut_data: Some(GetBufferFutureData {
                alloc: self.clone(),
                pe,
                offset,
                len,
                scheduler: scheduler.clone(),
                counters,
                result: MaybeUninit::uninit(),
            }),
            fut: None,
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
        let mut dst: Vec<T> = (0..len).map(|_| unsafe { std::mem::zeroed() }).collect();
        _scheduler.clone().block_on(async {
            unsafe {
                LibfabricAsyncAlloc::inner_get(self, pe, offset, &mut dst)
                    .await
                    .expect("error in blocking_get_buffer");
            }
        });
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
        LibfabricAsyncGetIntoBufferFuture {
            fut_data: Some(GetIntoBufferFutureData {
                alloc: self.clone(),
                pe,
                offset,
                dst,
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
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
        _scheduler.clone().block_on(async {
            unsafe {
                LibfabricAsyncAlloc::inner_get(&self, pe, offset, dst.as_mut_slice())
                    .await
                    .expect("error in blocking_get_into_buffer");
            }
        })
    }
    
    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        async_std::task::block_on(async {
        unsafe {
            LibfabricAsyncAlloc::inner_get_unmanaged(&self, pe, offset, dst.as_mut_slice())
                .await
                .expect("error in get_into_buffer_unmanaged")
        };});

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
            fut_data: Some(PutFutureData {
                alloc: self.alloc.clone(),
                offset: offset,
                op: AllocOp::Put(pe, src),
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
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
            unsafe {
                LibfabricAsyncAlloc::inner_put_unmanaged(
                    &self.alloc,
                    pe,
                    offset,
                    std::slice::from_ref(&src),
                    false,
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
        _scheduler.clone().block_on(async {
            unsafe {
                LibfabricAsyncAlloc::inner_put(&self.alloc, pe, offset, std::slice::from_ref(&src))
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
            fut_data: Some(PutFutureData {
                alloc: self.alloc.clone(),
                offset,
                op: AllocOp::PutBuf(pe, src.into()),
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
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
        unsafe {
            LibfabricAsyncAlloc::inner_put_unmanaged(&self.alloc, pe, offset, src.as_slice(), false)
                .expect("error in put_buffer_unmanaged")
        };
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
            fut_data: Some(GetFutureData {
                alloc: self.alloc.clone(),
                pe,
                offset,
                scheduler: scheduler.clone(),
                counters,
                result: MaybeUninit::uninit(),
            }),
            fut: None,
        }
        .into()
    }
    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        let mut result: T = unsafe { std::mem::zeroed() };
        _scheduler.clone().block_on(async {
            unsafe {
                LibfabricAsyncAlloc::inner_get(
                    &self.alloc,
                    pe,
                    offset,
                    std::slice::from_mut(&mut result),
                )
                .await
                .expect("error in OneSided blocking_get");
            }
        });
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
            fut_data: Some(GetBufferFutureData {
                alloc: self.alloc.clone(),
                pe,
                offset,
                len,
                scheduler: scheduler.clone(),
                counters,
                result: MaybeUninit::uninit(),
            }),
            fut: None,
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
        let mut dst: Vec<T> = (0..len).map(|_| unsafe { std::mem::zeroed() }).collect();
        _scheduler.clone().block_on(async {
            unsafe {
                LibfabricAsyncAlloc::inner_get(&self.alloc, pe, offset, &mut dst)
                    .await
                    .expect("error in OneSided blocking_get_buffer");
            }
        });
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
            fut_data: Some(GetIntoBufferFutureData {
                alloc: self.alloc.clone(),
                pe,
                offset,
                dst,
                scheduler: scheduler.clone(),
                counters,
            }),
            fut: None,
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
        _scheduler.clone().block_on(async {
            unsafe {
                LibfabricAsyncAlloc::inner_get(&self.alloc, pe, offset, dst.as_mut_slice())
                    .await
                    .expect("error in OneSided blocking_get_into_buffer");
            }
        })
    }
    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        assert_eq!(pe, self.remote_pe, "get_into_buffer_unmanaged called on OneSidedLibfabricAsyncAlloc with incorrect pe: {} expected pe: {}", pe, self.remote_pe);
        async_std::task::block_on(async {
        unsafe {
            LibfabricAsyncAlloc::inner_get_unmanaged(
                &self.alloc,
                pe,
                offset,
                dst.as_mut_slice(),
            )
            .await
            .expect("error in get_into_buffer_unmanaged")
        };});
    }
}

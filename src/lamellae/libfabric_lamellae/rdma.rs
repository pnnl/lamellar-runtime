use std::{
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
        comm::rdma::{
            RdmaGetBufferFuture, RdmaGetBufferHandle, RdmaGetFuture, RdmaGetHandle,
            RdmaGetIntoBufferFuture, RdmaGetIntoBufferHandle, RdmaHandle, RdmaPutFuture, Remote,
        },
        CommAllocRdma,
    },
    memregion::{AsLamellarBuffer, LamellarBuffer, MemregionRdmaInputInner},
    warnings::RuntimeWarning,
    LamellarTask,
};


use super::{
    fabric::{LibfabricAlloc, OneSidedLibfabricAlloc},
    Scheduler,
};

pub(super) enum AllocOp<T: Remote> {
    Put(usize, T),
    PutBuf(usize, MemregionRdmaInputInner<T>),
    PutAll(Vec<usize>, T),
    PutAllBuf(Vec<usize>, MemregionRdmaInputInner<T>),
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricPutFuture<T: Remote> {
    my_pe: usize,
    alloc: LibfabricAlloc,
    offset: usize,
    op: AllocOp<T>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    /// Registered copy of an unregistered source that is too large for the
    /// inject path (see `Ofi::staging_alloc`); must outlive the DMA, so it is
    /// held until the future completes.
    staging: Option<LibfabricAlloc>,
    local_op: bool,
    wait_cnt: Option<usize>,
}

impl<T: Remote> LibfabricPutFuture<T> {
    fn inner_put(&self, pe: usize, src: &[T]) {
        trace!(
            "putting src: {:x} dst: {:x} len: {} num bytes {}",
            src.as_ptr() as usize,
            self.alloc.start() + self.offset,
            src.len(),
            std::mem::size_of_val(src)
        );
        unsafe {
            LibfabricAlloc::inner_put(&self.alloc, pe, self.offset, src, false)
                .expect("error in put")
        };
    }

    fn exec_op(&mut self) {
        let (src, registered): (&[T], bool) = match &self.op {
            AllocOp::Put(_, src) | AllocOp::PutAll(_, src) => (std::slice::from_ref(src), false),
            AllocOp::PutBuf(_, src) | AllocOp::PutAllBuf(_, src) => {
                (src.as_slice(), src.is_registered())
            }
        };
        // Below the inject size rxm copies the source into its own buffers;
        // registered sources are DMA'd from where they are.
        if !registered && std::mem::size_of_val(src) >= self.alloc.ofi.inject_size() {
            if let Some(staging) = self
                .alloc
                .ofi
                .staging_alloc(std::mem::size_of_val(src), std::mem::align_of::<T>())
            {
                unsafe { staging.as_mut_slice::<T>().copy_from_slice(src) };
                self.staging = Some(staging);
            }
        }
        let src: &[T] = match &self.staging {
            Some(staging) => unsafe { staging.as_slice::<T>() },
            None => src,
        };
        match &self.op {
            AllocOp::Put(pe, _) | AllocOp::PutBuf(pe, _) => self.inner_put(*pe, src),
            AllocOp::PutAll(pes, _) | AllocOp::PutAllBuf(pes, _) => {
                for pe in pes {
                    self.inner_put(*pe, src);
                }
            }
        }

        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || {
            if !self.local_op {
                self.alloc.ofi.wait_all().unwrap();
            }
        })
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricPutFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<LibfabricPutFuture<T>> for RdmaHandle<T> {
    fn from(f: LibfabricPutFuture<T>) -> RdmaHandle<T> {
        RdmaHandle {
            future: RdmaPutFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricPutFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        if !self.local_op {
            let mut wait_cnt = self.wait_cnt;
            self.alloc.ofi.try_wait(&mut wait_cnt);
            self.wait_cnt = wait_cnt;
            if self.wait_cnt.is_some() {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }

        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricGetFuture<T> {
    alloc: LibfabricAlloc,
    pe: usize,
    offset: usize,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    result: Box<T>,
    staging: Option<LibfabricAlloc>,
    local_op: bool,
    wait_cnt: Option<usize>,
}

impl<T: Remote> LibfabricGetFuture<T> {
    fn new(
        alloc: LibfabricAlloc,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
    ) -> Self {
        let staging = alloc
            .ofi
            .staging_alloc(std::mem::size_of::<T>(), std::mem::align_of::<T>());
        let local_op = pe == alloc.ofi.my_pe;
        LibfabricGetFuture {
            alloc,
            pe,
            offset,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: Box::new(unsafe { std::mem::zeroed() }),
            staging,
            local_op,
            wait_cnt: None,
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        trace!("getting at: {:?} {:?} ", self.pe, self.offset);
        unsafe {
            let dst: &mut [T] = match &self.staging {
                Some(staging) => staging.as_mut_slice::<T>(),
                None => std::slice::from_mut(&mut *self.result),
            };
            self.alloc
                .inner_get(self.pe, self.offset, dst, false)
                .expect("error in get");
        }
        self.spawned = true;
    }

    fn copy_out(&mut self) {
        if let Some(staging) = self.staging.take() {
            *self.result = unsafe { staging.as_slice::<T>()[0] };
        }
    }

    pub(crate) fn block(mut self) -> T {
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || {
            self.exec_at();
            if !self.local_op {
                self.alloc.ofi.wait_all().unwrap();
            }
            self.copy_out();
            *self.result
        })
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<T> {
        self.exec_at();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricGetFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<LibfabricGetFuture<T>> for RdmaGetHandle<T> {
    fn from(f: LibfabricGetFuture<T>) -> RdmaGetHandle<T> {
        RdmaGetHandle {
            future: RdmaGetFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricGetFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }

        if !self.local_op {
            let mut wait_cnt = self.wait_cnt;
            self.alloc.ofi.try_wait(&mut wait_cnt);
            self.wait_cnt = wait_cnt;
            if self.wait_cnt.is_some() {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
        self.copy_out();
        Poll::Ready(*self.result)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricGetBufferFuture<T> {
    alloc: LibfabricAlloc,
    pe: usize,
    offset: usize,
    len: usize,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    /// Empty until completion when `staging` is Some (filled by `copy_out`);
    /// otherwise the zeroed GET destination itself.
    result: Vec<T>,
    /// Registered scratch destination (see `Ofi::staging_alloc`). None =>
    /// GET straight into `result`.
    staging: Option<LibfabricAlloc>,
    local_op: bool,
    wait_cnt: Option<usize>,
}

impl<T: Remote> LibfabricGetBufferFuture<T> {
    fn new(
        alloc: LibfabricAlloc,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> Self {
        let staging = alloc
            .ofi
            .staging_alloc(len * std::mem::size_of::<T>(), std::mem::align_of::<T>());
        // With staging the Vec is filled by copy_out at completion, so skip
        // the zero-fill pass; without it the Vec is the GET destination.
        let result = if staging.is_some() {
            Vec::new()
        } else {
            (0..len).map(|_| unsafe { std::mem::zeroed() }).collect()
        };
        let local_op = pe == alloc.ofi.my_pe;
        LibfabricGetBufferFuture {
            alloc,
            pe,
            offset,
            len,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result,
            staging,
            local_op,
            wait_cnt: None,
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        trace!("getting at: {:?} {:?} ", self.pe, self.offset);
        unsafe {
            let dst: &mut [T] = match &self.staging {
                Some(staging) => staging.as_mut_slice::<T>(),
                None => &mut self.result,
            };
            self.alloc
                .inner_get(self.pe, self.offset, dst, false)
                .expect("error in get_buffer");
            self.spawned = true;
        }
    }

    // Only valid once the GET is known complete; staging drops here and
    // returns its block to the pool.
    fn copy_out(&mut self) {
        if let Some(staging) = self.staging.take() {
            self.result.reserve_exact(self.len);
            self.result
                .extend_from_slice(unsafe { staging.as_slice::<T>() });
        }
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || {
            self.exec_at();

            if !self.local_op {
                self.alloc.ofi.wait_all().unwrap();
            }
            self.copy_out();
            std::mem::take(&mut self.result)
        })
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_at();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T> PinnedDrop for LibfabricGetBufferFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote> From<LibfabricGetBufferFuture<T>> for RdmaGetBufferHandle<T> {
    fn from(f: LibfabricGetBufferFuture<T>) -> RdmaGetBufferHandle<T> {
        RdmaGetBufferHandle {
            future: RdmaGetBufferFuture::Libfabric(f),
        }
    }
}

impl<T: Remote> Future for LibfabricGetBufferFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        if !self.local_op {
            let mut wait_cnt = self.wait_cnt;
            self.alloc.ofi.try_wait(&mut wait_cnt);
            self.wait_cnt = wait_cnt;
            if self.wait_cnt.is_some() {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
        self.copy_out();
        Poll::Ready(std::mem::take(&mut self.result))
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricGetIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    my_pe: usize,
    alloc: LibfabricAlloc,
    pe: usize,
    offset: usize,
    dst: LamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    /// Registered scratch destination, only when `dst`'s backing store is
    /// not itself registered memory (see `Ofi::staging_alloc`).
    staging: Option<LibfabricAlloc>,
    local_op: bool,
    wait_cnt: Option<usize>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricGetIntoBufferFuture<T, B> {
    fn new(
        alloc: LibfabricAlloc,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) -> Self {
        let staging = if dst.is_registered() {
            None
        } else {
            alloc.ofi.staging_alloc(
                std::mem::size_of_val(dst.as_slice()),
                std::mem::align_of::<T>(),
            )
        };
        let my_pe = alloc.ofi.my_pe;
        LibfabricGetIntoBufferFuture {
            my_pe,
            alloc,
            pe,
            offset,
            dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            staging,
            local_op: pe == my_pe,
            wait_cnt: None,
        }
    }

    fn exec_op(&mut self) {
        unsafe {
            let dst: &mut [T] = match &self.staging {
                Some(staging) => staging.as_mut_slice::<T>(),
                None => self.dst.as_mut_slice(),
            };
            LibfabricAlloc::inner_get(&self.alloc, self.pe, self.offset, dst, false)
                .expect("error in get_into_buffer");
        };
        self.spawned = true;
    }

    fn copy_out(&mut self) {
        if let Some(staging) = self.staging.take() {
            self.dst
                .as_mut_slice()
                .copy_from_slice(unsafe { staging.as_slice::<T>() });
        }
    }

    pub(crate) fn block(mut self) {
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || {
            self.exec_op();
            if !self.local_op {
                self.alloc.ofi.wait_all().unwrap();
            }
            self.copy_out();
        })
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricGetIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricGetIntoBufferFuture<T, B>>
    for RdmaGetIntoBufferHandle<T, B>
{
    fn from(f: LibfabricGetIntoBufferFuture<T, B>) -> RdmaGetIntoBufferHandle<T, B> {
        RdmaGetIntoBufferHandle {
            future: RdmaGetIntoBufferFuture::Libfabric(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricGetIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        if !self.local_op {
            let mut wait_cnt = self.wait_cnt;
            self.alloc.ofi.try_wait(&mut wait_cnt);
            self.wait_cnt = wait_cnt;
            if self.wait_cnt.is_some() {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
        self.copy_out();
        Poll::Ready(())
    }
}

/// PUT with no completion handle. Registered sources and sources below the
/// inject size keep the original fire-and-forget semantics (rxm copies the
/// latter into its own buffers). A larger heap source is staged through
/// registered memory and completed synchronously, since nothing could
/// otherwise keep the staging block alive until the DMA finishes.
unsafe fn put_buffer_unmanaged_impl<T: Remote>(
    alloc: &LibfabricAlloc,
    pes: impl Iterator<Item = usize>,
    offset: usize,
    src: &MemregionRdmaInputInner<T>,
    what: &str,
) {
    let slice = src.as_slice();
    if src.is_registered() || std::mem::size_of_val(slice) < alloc.ofi.inject_size() {
        for pe in pes {
            LibfabricAlloc::inner_put(alloc, pe, offset, slice, false).expect(what);
        }
        return;
    }
    match alloc
        .ofi
        .staging_alloc(std::mem::size_of_val(slice), std::mem::align_of::<T>())
    {
        Some(staging) => {
            staging.as_mut_slice::<T>().copy_from_slice(slice);
            let staged = staging.as_slice::<T>();
            for pe in pes {
                LibfabricAlloc::inner_put(alloc, pe, offset, staged, false).expect(what);
            }
            alloc.ofi.wait_all().expect(what);
        }
        None => {
            for pe in pes {
                LibfabricAlloc::inner_put(alloc, pe, offset, slice, false).expect(what);
            }
        }
    }
}

/// Blocking GET into `dst`, routed through registered staging memory when
/// available so the provider never registers `dst` on the fly.
unsafe fn blocking_get_staged<T: Remote>(
    alloc: &LibfabricAlloc,
    pe: usize,
    offset: usize,
    dst: &mut [T],
    small: bool,
    what: &str,
) {
    let staging = alloc
        .ofi
        .staging_alloc(std::mem::size_of_val(dst), std::mem::align_of::<T>());
    let target: &mut [T] = match &staging {
        Some(staging) => staging.as_mut_slice::<T>(),
        None => dst,
    };
    let res = if small {
        LibfabricAlloc::inner_get_small(alloc, pe, offset, target, true)
    } else {
        LibfabricAlloc::inner_get(alloc, pe, offset, target, true)
    };
    res.expect(what);
    if let Some(staging) = staging {
        dst.copy_from_slice(staging.as_slice::<T>());
    }
}

/// GET into a `LamellarBuffer` with no completion handle. A registered
/// backing store (memregions, `CommSlice`) keeps the original fire-and-forget
/// semantics; a heap backing store is staged and completed synchronously,
/// since nothing could otherwise copy the result out later.
unsafe fn get_into_buffer_unmanaged_impl<T: Remote, B: AsLamellarBuffer<T>>(
    alloc: &LibfabricAlloc,
    pe: usize,
    offset: usize,
    dst: &mut LamellarBuffer<T, B>,
) {
    if dst.is_registered() {
        LibfabricAlloc::inner_get(alloc, pe, offset, dst.as_mut_slice(), false)
            .expect("error in get_into_buffer_unmanaged");
    } else {
        blocking_get_staged(
            alloc,
            pe,
            offset,
            dst.as_mut_slice(),
            false,
            "error in get_into_buffer_unmanaged",
        );
    }
}

impl CommAllocRdma for LibfabricAlloc {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        LibfabricPutFuture {
            my_pe: self.ofi.my_pe,
            alloc: self.clone(),
            offset: offset,
            op: AllocOp::Put(pe, src),
            staging: None,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            local_op: pe == self.ofi.my_pe,
            wait_cnt: None,
        }
        .into()
    }
    fn put_blocking<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        src: T,
        pe: usize,
        offset: usize,
    ) {
        unsafe {
            LibfabricAlloc::inner_put(&self, pe, offset, std::slice::from_ref(&src), true)
                .expect("error in put_blocking")
        };
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        trace!(
            "put unamanaged dst: {pe}  offset: {offset} size_of<T> {}",
            std::mem::size_of::<T>()
        );
        unsafe {
            LibfabricAlloc::inner_put(
                &self,
                pe,
                offset,
                std::slice::from_ref(&src),
                std::mem::size_of::<T>() >= self.ofi.inject_size(), // stack src: only inject copies it
            )
            .expect("error in put_unmanaged")
        };
    }
    fn put_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        LibfabricPutFuture {
            my_pe: self.ofi.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::PutBuf(pe, src.into()),
            staging: None,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            local_op: pe == self.ofi.my_pe,
            wait_cnt: None,
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
            put_buffer_unmanaged_impl(
                self,
                std::iter::once(pe),
                offset,
                &src,
                "error in put_buffer_unmanaged",
            )
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
        LibfabricPutFuture {
            my_pe: self.ofi.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::PutAll(pes, src.into()),
            staging: None,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            local_op: false,
            wait_cnt: None,
        }
        .into()
    }
    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize) {
        for pe in 0..self.num_pes() {
            unsafe {
                LibfabricAlloc::inner_put(
                    &self,
                    pe,
                    offset,
                    std::slice::from_ref(&src),
                    std::mem::size_of::<T>() >= self.ofi.inject_size(), // stack src: only inject copies it
                )
                .expect("error in put_all_unmanaged")
            };
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
        LibfabricPutFuture {
            my_pe: self.ofi.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::PutAllBuf(pes, src.into()),
            staging: None,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            local_op: false,
            wait_cnt: None,
        }
        .into()
    }
    fn put_all_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) {
        let src = src.into();
        unsafe {
            put_buffer_unmanaged_impl(
                self,
                0..self.num_pes(),
                offset,
                &src,
                "error in put_all_buffer_unmanaged",
            )
        };
    }

    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        LibfabricGetFuture::new(self.clone(), scheduler, counters, pe, offset).into()
    }

    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        let mut val: T = unsafe { std::mem::zeroed() };
        unsafe {
            blocking_get_staged(
                self,
                pe,
                offset,
                std::slice::from_mut(&mut val),
                true,
                "error in blocking_get",
            )
        };
        val
    }
    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> RdmaGetBufferHandle<T> {
        LibfabricGetBufferFuture::new(self.clone(), scheduler, counters, pe, offset, len).into()
    }
    fn blocking_get_buffer<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> Vec<T> {
        let mut dst: Vec<T> = (0..len).map(|_| unsafe { std::mem::zeroed() }).collect();
        unsafe {
            blocking_get_staged(
                self,
                pe,
                offset,
                &mut dst,
                false,
                "error in blocking_get_buffer",
            )
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
        LibfabricGetIntoBufferFuture::new(self.clone(), scheduler, counters, pe, offset, dst)
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
            unsafe {
                LibfabricAlloc::inner_get(&self, pe, offset, dst.as_mut_slice(), true)
                    .expect("error in blocking_get_into_buffer")
            };
        } else {
            unsafe {
                blocking_get_staged(
                    self,
                    pe,
                    offset,
                    dst.as_mut_slice(),
                    false,
                    "error in blocking_get_into_buffer",
                )
            };
        }
    }

    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        unsafe { get_into_buffer_unmanaged_impl(self, pe, offset, &mut dst) };
    }
}

impl CommAllocRdma for OneSidedLibfabricAlloc {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        assert_eq!(
            pe, self.remote_pe,
            "put called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );

        LibfabricPutFuture {
            my_pe: self.alloc.ofi.my_pe,
            alloc: self.alloc.clone(),
            offset: offset,
            op: AllocOp::Put(pe, src),
            staging: None,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            local_op: pe == self.alloc.ofi.my_pe,
            wait_cnt: None,
        }
        .into()
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
            "put_blocking called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        unsafe {
            LibfabricAlloc::inner_put(&self.alloc, pe, offset, std::slice::from_ref(&src), true)
                .expect("error in put_blocking")
        };
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        assert_eq!(
            pe, self.remote_pe,
            "put_unmanaged called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        unsafe {
            LibfabricAlloc::inner_put(
                &self.alloc,
                pe,
                offset,
                std::slice::from_ref(&src),
                std::mem::size_of::<T>() >= self.alloc.ofi.inject_size(), // stack src: only inject copies it
            )
            .expect("error in put_unmanaged")
        };
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
            "put_buffer called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );

        LibfabricPutFuture {
            my_pe: self.alloc.ofi.my_pe,
            alloc: self.alloc.clone(),
            offset,
            op: AllocOp::PutBuf(pe, src.into()),
            staging: None,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            local_op: pe == self.alloc.ofi.my_pe,
            wait_cnt: None,
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
            "put_buffer_unmanaged called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let src = src.into();
        unsafe {
            put_buffer_unmanaged_impl(
                &self.alloc,
                std::iter::once(pe),
                offset,
                &src,
                "error in put_buffer_unmanaged",
            )
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
            "get called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricGetFuture::new(self.alloc.clone(), scheduler, counters, pe, offset).into()
    }
    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        assert_eq!(
            pe, self.remote_pe,
            "blocking_get called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut val: T = unsafe { std::mem::zeroed() };
        unsafe {
            blocking_get_staged(
                &self.alloc,
                pe,
                offset,
                std::slice::from_mut(&mut val),
                true,
                "error in blocking_get",
            )
        };
        val
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
            "get_buffer called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricGetBufferFuture::new(self.alloc.clone(), scheduler, counters, pe, offset, len)
            .into()
    }
    fn blocking_get_buffer<T: Remote>(
        &self,
        _scheduler: &Arc<Scheduler>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> Vec<T> {
        assert_eq!(
            pe, self.remote_pe,
            "blocking_get_buffer called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut dst: Vec<T> = (0..len).map(|_| unsafe { std::mem::zeroed() }).collect();
        unsafe {
            blocking_get_staged(
                &self.alloc,
                pe,
                offset,
                &mut dst,
                false,
                "error in blocking_get_buffer",
            )
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
            "get_into_buffer called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricGetIntoBufferFuture::new(self.alloc.clone(), scheduler, counters, pe, offset, dst)
            .into()
    }
    fn blocking_get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        _scheduler: &Arc<Scheduler>,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        assert_eq!(
            pe, self.remote_pe,
            "blocking_get_into_buffer called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        if dst.is_registered() {
            unsafe {
                LibfabricAlloc::inner_get(&self.alloc, pe, offset, dst.as_mut_slice(), true)
                    .expect("error in blocking_get_into_buffer")
            };
        } else {
            unsafe {
                blocking_get_staged(
                    &self.alloc,
                    pe,
                    offset,
                    dst.as_mut_slice(),
                    false,
                    "error in blocking_get_into_buffer",
                )
            };
        }
    }
    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        assert_eq!(pe, self.remote_pe, "get_into_buffer_unmanaged called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}", pe, self.remote_pe);
        unsafe { get_into_buffer_unmanaged_impl(&self.alloc, pe, offset, &mut dst) };
    }
}

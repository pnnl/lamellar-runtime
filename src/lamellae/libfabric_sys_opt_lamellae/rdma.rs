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
    fabric::{LibfabricSysOptAlloc, OneSidedLibfabricSysOptAlloc, OptOfi, Ticket},
    Scheduler,
};

/// Blocks on every ticket in `tickets` — used by the synchronous `block()` path.
fn block_all(tickets: &[Arc<Ticket>], ofi: &OptOfi) {
    for t in tickets {
        t.block(ofi);
    }
}

/// Drop guard for futures dropped mid-op (select/timeout/task abort): the NIC may still be
/// reading/writing this future's `result`/`dst`/`staging`/boxed source, which are freed
/// right after `drop` returns. Blocks (self-progressing) until every ticket is done; free
/// when the op already completed normally.
fn wait_in_flight(tickets: &[Arc<Ticket>], ofi: &OptOfi) {
    if tickets.iter().any(|t| !t.is_done()) {
        block_all(tickets, ofi);
    }
}

/// Polls every ticket in `tickets`. Registers `cx`'s waker on the first not-yet-done ticket
/// found (re-checking after registration to catch a completion that raced the check) and
/// returns `Pending` if any ticket is outstanding, else `Ready(())`. Not all tickets need a
/// waker registered on every call — whichever one wakes us will cause `poll` to be called
/// again, and this scan restarts from the front each time.
fn poll_all(tickets: &[Arc<Ticket>], cx: &mut Context<'_>) -> Poll<()> {
    for t in tickets {
        if !t.is_done() {
            if !t.register_waker(cx.waker()) {
                return Poll::Pending;
            }
        }
    }
    Poll::Ready(())
}

// By-value sources are boxed: the ticketed fi_writemsg reads the source at DMA time,
// after `exec_op` returns, and the future itself is moved (into `block_in_place` /
// `spawn_task`) while the op is in flight -- an inline `T` would leave the NIC reading a
// stale stack/moved-from copy.
pub(super) enum AllocOp<T: Remote> {
    Put(usize, Box<T>),
    PutBuf(usize, MemregionRdmaInputInner<T>),
    PutAll(Vec<usize>, Box<T>),
    PutAllBuf(Vec<usize>, MemregionRdmaInputInner<T>),
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysOptPutFuture<T: Remote> {
    my_pe: usize,
    alloc: LibfabricSysOptAlloc,
    offset: usize,
    op: AllocOp<T>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    /// Registered copy of an unregistered source that is too large for the inject
    /// path (see `OptOfi::staging_alloc`). Every ticket for this PUT must be
    /// confirmed done (via `tickets`) before this is dropped, since it is the
    /// buffer the RMA is actually reading from.
    staging: Option<LibfabricSysOptAlloc>,
    tickets: Vec<Arc<Ticket>>,
}

impl<T: Remote> LibfabricSysOptPutFuture<T> {
    fn inner_put(&self, pe: usize, src: &[T]) -> Vec<Arc<Ticket>> {
        trace!(
            "putting src: {:x} dst: {:x} len: {} num bytes {}",
            src.as_ptr() as usize,
            self.alloc.start() + self.offset,
            src.len(),
            std::mem::size_of_val(src)
        );
        unsafe { LibfabricSysOptAlloc::inner_put_ticketed(&self.alloc, pe, self.offset, src) }
    }

    fn exec_op(&mut self) {
        let (src, registered): (&[T], bool) = match &self.op {
            AllocOp::Put(_, src) | AllocOp::PutAll(_, src) => (std::slice::from_ref(&**src), false),
            AllocOp::PutBuf(_, src) | AllocOp::PutAllBuf(_, src) => {
                (src.as_slice(), src.is_registered())
            }
        };
        // Up to the inject size the ticketed fi_writemsg sets FI_INJECT, so rxm
        // copies the source into its own registered tx buffer and an unregistered
        // heap source is safe as-is (see `inner_put_ticketed`).
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
        self.tickets = match &self.op {
            AllocOp::Put(pe, _) | AllocOp::PutBuf(pe, _) => self.inner_put(*pe, src),
            AllocOp::PutAll(pes, _) | AllocOp::PutAllBuf(pes, _) => {
                let mut tickets = Vec::new();
                for pe in pes {
                    tickets.append(&mut self.inner_put(*pe, src));
                }
                tickets
            }
        };
        self.spawned = true;
    }
    pub(crate) fn block(mut self) {
        self.exec_op();
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || {
            block_all(&self.tickets, &self.alloc.ofi);
        })
    }
    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for LibfabricSysOptPutFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
        wait_in_flight(&self.tickets, &self.alloc.ofi);
    }
}

impl<T: Remote> From<LibfabricSysOptPutFuture<T>> for RdmaHandle<T> {
    fn from(f: LibfabricSysOptPutFuture<T>) -> RdmaHandle<T> {
        RdmaHandle {
            future: RdmaPutFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysOptPutFuture<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        poll_all(&self.tickets, cx)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysOptGetFuture<T> {
    alloc: LibfabricSysOptAlloc,
    pe: usize,
    offset: usize,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    result: Box<T>,
    /// Registered scratch destination (see `OptOfi::staging_alloc`); held until
    /// every ticket for this GET is confirmed done, then copied into `result`
    /// (`copy_out`) and dropped back to the pool.
    staging: Option<LibfabricSysOptAlloc>,
    tickets: Vec<Arc<Ticket>>,
}

impl<T: Remote> LibfabricSysOptGetFuture<T> {
    fn new(
        alloc: LibfabricSysOptAlloc,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
    ) -> Self {
        let staging = alloc
            .ofi
            .staging_alloc(std::mem::size_of::<T>(), std::mem::align_of::<T>());
        LibfabricSysOptGetFuture {
            alloc,
            pe,
            offset,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result: Box::new(unsafe { std::mem::zeroed() }),
            staging,
            tickets: Vec::new(),
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        trace!("getting at: {:?} {:?} ", self.pe, self.offset);
        self.tickets = unsafe {
            let dst: &mut [T] = match &self.staging {
                Some(staging) => staging.as_mut_slice::<T>(),
                None => std::slice::from_mut(&mut *self.result),
            };
            self.alloc.inner_get_ticketed(self.pe, self.offset, dst)
        };
        self.spawned = true;
    }

    // Only valid once the GET is known complete; staging drops here and
    // returns its block to the pool.
    fn copy_out(&mut self) {
        if let Some(staging) = self.staging.take() {
            *self.result = unsafe { staging.as_slice::<T>()[0] };
        }
    }

    pub(crate) fn block(mut self) -> T {
        self.exec_at();
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || {
            block_all(&self.tickets, &self.alloc.ofi);
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
impl<T> PinnedDrop for LibfabricSysOptGetFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
        wait_in_flight(&self.tickets, &self.alloc.ofi);
    }
}

impl<T: Remote> From<LibfabricSysOptGetFuture<T>> for RdmaGetHandle<T> {
    fn from(f: LibfabricSysOptGetFuture<T>) -> RdmaGetHandle<T> {
        RdmaGetHandle {
            future: RdmaGetFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysOptGetFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        match poll_all(&self.tickets, cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(()) => {}
        }
        self.copy_out();
        Poll::Ready(*self.result)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysOptGetBufferFuture<T> {
    alloc: LibfabricSysOptAlloc,
    pe: usize,
    offset: usize,
    len: usize,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    /// Empty until completion when `staging` is Some (filled by `copy_out`);
    /// otherwise the zeroed GET destination itself.
    result: Vec<T>,
    /// Registered scratch destination (see `OptOfi::staging_alloc`). None =>
    /// GET straight into `result`.
    staging: Option<LibfabricSysOptAlloc>,
    tickets: Vec<Arc<Ticket>>,
}

impl<T: Remote> LibfabricSysOptGetBufferFuture<T> {
    fn new(
        alloc: LibfabricSysOptAlloc,
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
        LibfabricSysOptGetBufferFuture {
            alloc,
            pe,
            offset,
            len,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            result,
            staging,
            tickets: Vec::new(),
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn exec_at(&mut self) {
        trace!("getting at: {:?} {:?} ", self.pe, self.offset);
        unsafe {
            let dst: &mut [T] = match &self.staging {
                Some(staging) => staging.as_mut_slice::<T>(),
                None => &mut self.result,
            };
            self.tickets = self.alloc.inner_get_ticketed(self.pe, self.offset, dst);
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
        self.exec_at();

        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || {
            block_all(&self.tickets, &self.alloc.ofi);
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
impl<T> PinnedDrop for LibfabricSysOptGetBufferFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
        wait_in_flight(&self.tickets, &self.alloc.ofi);
    }
}

impl<T: Remote> From<LibfabricSysOptGetBufferFuture<T>> for RdmaGetBufferHandle<T> {
    fn from(f: LibfabricSysOptGetBufferFuture<T>) -> RdmaGetBufferHandle<T> {
        RdmaGetBufferHandle {
            future: RdmaGetBufferFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote> Future for LibfabricSysOptGetBufferFuture<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_at();
        }
        match poll_all(&self.tickets, cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(()) => {}
        }
        self.copy_out();
        Poll::Ready(std::mem::take(&mut self.result))
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct LibfabricSysOptGetIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    my_pe: usize,
    alloc: LibfabricSysOptAlloc,
    pe: usize,
    offset: usize,
    dst: LamellarBuffer<T, B>,
    scheduler: Arc<Scheduler>,
    counters: Option<Arc<[Arc<AMCounters>]>>,
    spawned: bool,
    /// Registered scratch destination, only when `dst`'s backing store is not
    /// itself registered memory (see `OptOfi::staging_alloc`).
    staging: Option<LibfabricSysOptAlloc>,
    tickets: Vec<Arc<Ticket>>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> LibfabricSysOptGetIntoBufferFuture<T, B> {
    fn new(
        alloc: LibfabricSysOptAlloc,
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
        LibfabricSysOptGetIntoBufferFuture {
            my_pe,
            alloc,
            pe,
            offset,
            dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            staging,
            tickets: Vec::new(),
        }
    }

    fn exec_op(&mut self) {
        self.tickets = unsafe {
            let dst: &mut [T] = match &self.staging {
                Some(staging) => staging.as_mut_slice::<T>(),
                None => self.dst.as_mut_slice(),
            };
            LibfabricSysOptAlloc::inner_get_ticketed(&self.alloc, self.pe, self.offset, dst)
        };
        self.spawned = true;
    }

    // Only valid once the GET is known complete; staging drops here and
    // returns its block to the pool.
    fn copy_out(&mut self) {
        if let Some(staging) = self.staging.take() {
            self.dst
                .as_mut_slice()
                .copy_from_slice(unsafe { staging.as_slice::<T>() });
        }
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
        let scheduler = self.scheduler.clone();
        scheduler.block_in_place(move || {
            block_all(&self.tickets, &self.alloc.ofi);
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
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for LibfabricSysOptGetIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a RdmaHandle").print();
        }
        wait_in_flight(&self.tickets, &self.alloc.ofi);
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<LibfabricSysOptGetIntoBufferFuture<T, B>>
    for RdmaGetIntoBufferHandle<T, B>
{
    fn from(f: LibfabricSysOptGetIntoBufferFuture<T, B>) -> RdmaGetIntoBufferHandle<T, B> {
        RdmaGetIntoBufferHandle {
            future: RdmaGetIntoBufferFuture::LibfabricSysOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for LibfabricSysOptGetIntoBufferFuture<T, B> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match poll_all(&self.tickets, cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(()) => {}
        }
        self.copy_out();
        Poll::Ready(())
    }
}

/// PUT with no completion handle. Registered sources and sources below the
/// inject size keep the original fire-and-forget semantics (rxm copies the
/// latter into its own buffers). A larger heap source is staged through
/// registered memory and completed synchronously via `wait_all`, since
/// nothing else could keep the staging block alive until the DMA finishes.
unsafe fn put_buffer_unmanaged_impl<T: Remote>(
    alloc: &LibfabricSysOptAlloc,
    pes: impl Iterator<Item = usize>,
    offset: usize,
    src: &MemregionRdmaInputInner<T>,
) {
    let slice = src.as_slice();
    if src.is_registered() || std::mem::size_of_val(slice) < alloc.ofi.inject_size() {
        for pe in pes {
            LibfabricSysOptAlloc::inner_put(alloc, pe, offset, slice, false);
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
                LibfabricSysOptAlloc::inner_put(alloc, pe, offset, staged, false);
            }
            alloc.ofi.wait_all();
        }
        None => {
            for pe in pes {
                LibfabricSysOptAlloc::inner_put(alloc, pe, offset, slice, false);
            }
        }
    }
}

/// Blocking GET into `dst`, routed through registered staging memory when
/// available so the provider never registers `dst` on the fly.
unsafe fn blocking_get_staged<T: Remote>(
    alloc: &LibfabricSysOptAlloc,
    pe: usize,
    offset: usize,
    dst: &mut [T],
    small: bool,
) {
    let staging = alloc
        .ofi
        .staging_alloc(std::mem::size_of_val(dst), std::mem::align_of::<T>());
    let target: &mut [T] = match &staging {
        Some(staging) => staging.as_mut_slice::<T>(),
        None => dst,
    };
    if small {
        LibfabricSysOptAlloc::inner_get_small(alloc, pe, offset, target, true);
    } else {
        LibfabricSysOptAlloc::inner_get(alloc, pe, offset, target, true);
    }
    if let Some(staging) = staging {
        dst.copy_from_slice(staging.as_slice::<T>());
    }
}

/// GET into a `LamellarBuffer` with no completion handle. A registered
/// backing store (memregions, `CommSlice`) keeps the original fire-and-forget
/// semantics; a heap backing store is staged and completed synchronously,
/// since nothing could otherwise copy the result out later.
unsafe fn get_into_buffer_unmanaged_impl<T: Remote, B: AsLamellarBuffer<T>>(
    alloc: &LibfabricSysOptAlloc,
    pe: usize,
    offset: usize,
    dst: &mut LamellarBuffer<T, B>,
) {
    if dst.is_registered() {
        LibfabricSysOptAlloc::inner_get(alloc, pe, offset, dst.as_mut_slice(), false);
    } else {
        blocking_get_staged(alloc, pe, offset, dst.as_mut_slice(), false);
    }
}

impl CommAllocRdma for LibfabricSysOptAlloc {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        LibfabricSysOptPutFuture {
            my_pe: self.ofi.my_pe,
            alloc: self.clone(),
            offset: offset,
            op: AllocOp::Put(pe, Box::new(src)),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            staging: None,
            tickets: Vec::new(),
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
            LibfabricSysOptAlloc::inner_put(&self, pe, offset, std::slice::from_ref(&src), true)
        };
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        trace!(
            "put unamanaged dst: {pe}  offset: {offset} size_of<T> {}",
            std::mem::size_of::<T>()
        );
        unsafe {
            // `src` is a stack local: only the inject path (< inject_size) copies it at
            // post time, so anything larger must complete before we return.
            let blocking = std::mem::size_of::<T>() >= self.ofi.inject_size();
            LibfabricSysOptAlloc::inner_put(&self, pe, offset, std::slice::from_ref(&src), blocking);
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
        LibfabricSysOptPutFuture {
            my_pe: self.ofi.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::PutBuf(pe, src.into()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            staging: None,
            tickets: Vec::new(),
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
            put_buffer_unmanaged_impl(&self, std::iter::once(pe), offset, &src);
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
        LibfabricSysOptPutFuture {
            my_pe: self.ofi.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::PutAll(pes, Box::new(src)),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            staging: None,
            tickets: Vec::new(),
        }
        .into()
    }
    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize) {
        let ofi_inject_size = self.ofi.inject_size();
        for pe in 0..self.num_pes() {
            unsafe {
                LibfabricSysOptAlloc::inner_put(
                    &self,
                    pe,
                    offset,
                    std::slice::from_ref(&src),
                    std::mem::size_of::<T>() >= ofi_inject_size, // stack src: see put_unmanaged
                );
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
        LibfabricSysOptPutFuture {
            my_pe: self.ofi.my_pe,
            alloc: self.clone(),
            offset,
            op: AllocOp::PutAllBuf(pes, src.into()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            staging: None,
            tickets: Vec::new(),
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
            put_buffer_unmanaged_impl(&self, 0..self.num_pes(), offset, &src);
        };
    }

    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        LibfabricSysOptGetFuture::new(self.clone(), scheduler, counters, pe, offset).into()
    }
    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        let mut val: T = unsafe { std::mem::zeroed() };
        let val_slice = std::slice::from_mut(&mut val);
        unsafe { blocking_get_staged(self, pe, offset, val_slice, true) };
        val
        // let mut result = T::default();
        // let mut_result_slice = std::slice::from_mut(&mut result);
        // LibfabricAlloc::atomic_fetch_op_inner(
        //     self,
        //     pe,
        //     offset,
        //     &crate::lamellae::AtomicOp::Read,
        //     mut_result_slice,
        //     true,
        // )
        // .unwrap();
        // result
    }
    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> RdmaGetBufferHandle<T> {
        LibfabricSysOptGetBufferFuture::new(self.clone(), scheduler, counters, pe, offset, len)
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
        unsafe { blocking_get_staged(self, pe, offset, &mut dst, false) };
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
        LibfabricSysOptGetIntoBufferFuture::new(self.clone(), scheduler, counters, pe, offset, dst)
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
                LibfabricSysOptAlloc::inner_get(&self, pe, offset, dst.as_mut_slice(), true)
            };
        } else {
            unsafe { blocking_get_staged(self, pe, offset, dst.as_mut_slice(), false) };
        }
    }

    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        unsafe { get_into_buffer_unmanaged_impl(&self, pe, offset, &mut dst) };
    }
}

impl CommAllocRdma for OneSidedLibfabricSysOptAlloc {
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
            "put called on OneSidedLibfabricSysOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );

        LibfabricSysOptPutFuture {
            my_pe: self.alloc.ofi.my_pe,
            alloc: self.alloc.clone(),
            offset: offset,
            op: AllocOp::Put(pe, Box::new(src)),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            staging: None,
            tickets: Vec::new(),
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
            LibfabricSysOptAlloc::inner_put(
                &self.alloc,
                pe,
                offset,
                std::slice::from_ref(&src),
                true,
            )
        };
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        assert_eq!(
            pe, self.remote_pe,
            "put_unmanaged called on OneSidedLibfabricSysOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let ofi_inject_size = self.alloc.ofi.inject_size();
        unsafe {
            LibfabricSysOptAlloc::inner_put(
                &self.alloc,
                pe,
                offset,
                std::slice::from_ref(&src),
                std::mem::size_of::<T>() >= ofi_inject_size, // stack src: see put_unmanaged
            );
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
            "put_buffer called on OneSidedLibfabricSysOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );

        LibfabricSysOptPutFuture {
            my_pe: self.alloc.ofi.my_pe,
            alloc: self.alloc.clone(),
            offset,
            op: AllocOp::PutBuf(pe, src.into()),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
            staging: None,
            tickets: Vec::new(),
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
            "put_buffer_unmanaged called on OneSidedLibfabricSysOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let src = src.into();
        unsafe {
            put_buffer_unmanaged_impl(&self.alloc, std::iter::once(pe), offset, &src);
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
            "get called on OneSidedLibfabricSysOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricSysOptGetFuture::new(self.alloc.clone(), scheduler, counters, pe, offset).into()
    }
    fn blocking_get<T: Remote>(&self, _scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        assert_eq!(
            pe, self.remote_pe,
            "blocking_get called on OneSidedLibfabricAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut val: T = unsafe { std::mem::zeroed() };
        let val_slice = std::slice::from_mut(&mut val);
        unsafe { blocking_get_staged(&self.alloc, pe, offset, val_slice, true) };
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
            "get_buffer called on OneSidedLibfabricSysOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricSysOptGetBufferFuture::new(self.alloc.clone(), scheduler, counters, pe, offset, len)
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
            "blocking_get_buffer called on OneSidedLibfabricSysOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        let mut dst: Vec<T> = (0..len).map(|_| unsafe { std::mem::zeroed() }).collect();
        unsafe { blocking_get_staged(&self.alloc, pe, offset, &mut dst, false) };
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
            "get_into_buffer called on OneSidedLibfabricSysOptAlloc with incorrect pe: {} expected pe: {}",
            pe, self.remote_pe
        );
        LibfabricSysOptGetIntoBufferFuture::new(
            self.alloc.clone(),
            scheduler,
            counters,
            pe,
            offset,
            dst,
        )
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
                LibfabricSysOptAlloc::inner_get(&self.alloc, pe, offset, dst.as_mut_slice(), true)
            };
        } else {
            unsafe { blocking_get_staged(&self.alloc, pe, offset, dst.as_mut_slice(), false) };
        }
    }
    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        mut dst: LamellarBuffer<T, B>,
    ) {
        assert_eq!(pe, self.remote_pe, "get_into_buffer_unmanaged called on OneSidedLibfabricSysOptAlloc with incorrect pe: {} expected pe: {}", pe, self.remote_pe);
        unsafe { get_into_buffer_unmanaged_impl(&self.alloc, pe, offset, &mut dst) };
    }
}

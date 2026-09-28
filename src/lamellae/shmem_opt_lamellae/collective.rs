use std::{
    any::TypeId,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use pin_project::{pin_project, pinned_drop};

use crate::{
    active_messaging::AMCounters,
    lamellae::{
        collective::{
            CollectiveAllGatherIntoBufferOpFuture, CollectiveAllGatherIntoBufferOpHandle,
            CollectiveAllGatherOpFuture, CollectiveAllGatherOpHandle,
            CollectiveAllReduceInPlaceOpFuture, CollectiveAllReduceInPlaceOpHandle,
            CollectiveAllReduceIntoBufferOpFuture, CollectiveAllReduceIntoBufferOpHandle,
            CollectiveAllReduceOpFuture, CollectiveAllReduceOpHandle,
            CollectiveAllToAllIntoBufferOpFuture, CollectiveAllToAllIntoBufferOpHandle,
            CollectiveAllToAllOpFuture, CollectiveAllToAllOpHandle,
            CollectiveBroadcastIntoBufferOpFuture, CollectiveBroadcastIntoBufferOpHandle,
            CollectiveBroadcastOpFuture, CollectiveBroadcastOpHandle,
            CollectiveGatherIntoBufferOpFuture, CollectiveGatherIntoBufferOpHandle,
            CollectiveGatherOpFuture, CollectiveGatherOpHandle, CollectiveReduceIntoBufferOpFuture,
            CollectiveReduceIntoBufferOpHandle, CollectiveReduceOpFuture, CollectiveReduceOpHandle,
            CollectiveReduceScatterIntoBufferOpFuture, CollectiveReduceScatterIntoBufferOpHandle,
            CollectiveReduceScatterOpFuture, CollectiveReduceScatterOpHandle,
            CollectiveScatterIntoBufferOpFuture, CollectiveScatterIntoBufferOpHandle,
            CollectiveScatterOpFuture, CollectiveScatterOpHandle, CommAllocCollectiveAllGather,
            CommAllocCollectiveAllReduce, CommAllocCollectiveAllToAll,
            CommAllocCollectiveBroadcast, CommAllocCollectiveGather, CommAllocCollectiveReduce,
            CommAllocCollectiveReduceScatter, CommAllocCollectiveScatter, ReduceOp, RootOrBuffer,
            RootOrLamellarBuffer, RootSrcOrBuffer, RootSrcOrLamellarBuffer,
            RootSrcOrLamellarBufferInner, ScatterInputInner,
        },
        shmem_opt_lamellae::fabric::{ShmemOptAlloc, COLL_EAGER_BYTES, COLL_RD_BYTES},
    },
    scheduler::Scheduler,
    warnings::RuntimeWarning,
    AsLamellarBuffer, BroadcastInput, LamellarBuffer, LamellarTask, Remote, ScatterInput,
};

/// Elementwise `acc[i] = acc[i] op src[i]`. Integers wrap, like the atomic ops they replace.
fn local_reduce<T: Remote>(op: &ReduceOp, acc: &mut [T], src: &[T]) {
    macro_rules! int {
        ($t:ty) => {{
            let acc = unsafe { std::slice::from_raw_parts_mut(acc.as_mut_ptr() as *mut $t, acc.len()) };
            let src = unsafe { std::slice::from_raw_parts(src.as_ptr() as *const $t, src.len()) };
            let it = acc.iter_mut().zip(src.iter());
            match op {
                ReduceOp::Sum => it.for_each(|(a, s)| *a = a.wrapping_add(*s)),
                ReduceOp::Prod => it.for_each(|(a, s)| *a = a.wrapping_mul(*s)),
                ReduceOp::Min => it.for_each(|(a, s)| *a = (*a).min(*s)),
                ReduceOp::Max => it.for_each(|(a, s)| *a = (*a).max(*s)),
                ReduceOp::BitOr => it.for_each(|(a, s)| *a |= *s),
                ReduceOp::BitXor => it.for_each(|(a, s)| *a ^= *s),
                ReduceOp::BitAnd => it.for_each(|(a, s)| *a &= *s),
            }
            return;
        }};
    }
    macro_rules! float {
        ($t:ty) => {{
            let acc = unsafe { std::slice::from_raw_parts_mut(acc.as_mut_ptr() as *mut $t, acc.len()) };
            let src = unsafe { std::slice::from_raw_parts(src.as_ptr() as *const $t, src.len()) };
            let it = acc.iter_mut().zip(src.iter());
            match op {
                ReduceOp::Sum => it.for_each(|(a, s)| *a += *s),
                ReduceOp::Prod => it.for_each(|(a, s)| *a *= *s),
                ReduceOp::Min => it.for_each(|(a, s)| *a = a.min(*s)),
                ReduceOp::Max => it.for_each(|(a, s)| *a = a.max(*s)),
                _ => panic!("bitwise reduction on a float type"),
            }
            return;
        }};
    }
    let id = TypeId::of::<T>();
    if id == TypeId::of::<u8>() { int!(u8) }
    if id == TypeId::of::<u16>() { int!(u16) }
    if id == TypeId::of::<u32>() { int!(u32) }
    if id == TypeId::of::<u64>() { int!(u64) }
    if id == TypeId::of::<u128>() { int!(u128) }
    if id == TypeId::of::<usize>() { int!(usize) }
    if id == TypeId::of::<i8>() { int!(i8) }
    if id == TypeId::of::<i16>() { int!(i16) }
    if id == TypeId::of::<i32>() { int!(i32) }
    if id == TypeId::of::<i64>() { int!(i64) }
    if id == TypeId::of::<i128>() { int!(i128) }
    if id == TypeId::of::<isize>() { int!(isize) }
    if id == TypeId::of::<f32>() { float!(f32) }
    if id == TypeId::of::<f64>() { float!(f64) }
    panic!("unsupported reduction type {}", std::any::type_name::<T>());
}

// Chunk the reduction so the accumulator stays in L1 while every peer is folded in.
const REDUCE_CHUNK_BYTES: usize = 16 * 1024;

/// `out = op over all alloc PEs of peer[off..off + out.len()]`, where `base(idx)` is where the
/// `idx`-th PE's data starts (its published index, or its eager slot). The fold order is the
/// same on every PE, so float results match bit for bit. Call only after every peer has arrived.
fn reduce_peers<T: Remote>(
    alloc: &ShmemOptAlloc,
    base: impl Fn(usize) -> *const T,
    off: usize,
    out: &mut [T],
    op: &ReduceOp,
) {
    let peer = |idx: usize, start: usize, n: usize| unsafe {
        std::slice::from_raw_parts(base(idx).add(off + start), n)
    };
    let chunk = std::cmp::max(REDUCE_CHUNK_BYTES / std::cmp::max(std::mem::size_of::<T>(), 1), 1);
    let mut start = 0;
    while start < out.len() {
        let n = std::cmp::min(chunk, out.len() - start);
        let acc = &mut out[start..start + n];
        acc.copy_from_slice(peer(0, start, n));
        for idx in 1..alloc.num_pes() {
            local_reduce(op, acc, peer(idx, start, n));
        }
        start += n;
    }
}

// Synchronization for all collectives below (flags in fabric.rs). Two modes, chosen from the
// op's size, which is the same on every PE:
// - zero copy: arrive publishes this PE's index, readers wait for the arrivals they need and
//   read the input in place, and a PE returns only once every PE that reads its data has
//   marked done. The input memory is never modified.
// - eager (input fits `COLL_EAGER_BYTES`): arrive copies the input into a parity scratch slot,
//   readers read the slots, and nobody waits for done, so the op costs one sync.

#[inline(always)]
fn eager<T>(elems: usize) -> bool {
    elems * std::mem::size_of::<T>() <= COLL_EAGER_BYTES
}

/// This PE's `len` elements at `index`, as bytes for an eager arrive.
#[inline(always)]
fn my_bytes<T>(alloc: &ShmemOptAlloc, index: usize, len: usize) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(
            (alloc.data as *const T).add(index) as *const u8,
            len * std::mem::size_of::<T>(),
        )
    }
}

/// Every PE reduces all peers' `len` elements into `out`.
fn all_reduce<T: Remote>(alloc: &ShmemOptAlloc, index: usize, op: &ReduceOp, out: &mut [T]) {
    let p = alloc.num_pes();
    if alloc.coll_rd_avail() && std::mem::size_of_val(out) <= COLL_RD_BYTES {
        return all_reduce_rd(alloc, index, op, out);
    }
    if eager::<T>(out.len()) {
        let epoch = alloc.coll_eager_arrive(my_bytes::<T>(alloc, index, out.len()));
        alloc.coll_wait_all_arrive(epoch);
        reduce_peers(alloc, |i| alloc.coll_eager_peer::<T>(i, epoch), 0, out, op);
        return;
    }
    if p >= RSAG_MIN_PES && eager::<T>(out.len().div_ceil(p)) {
        return all_reduce_rsag(alloc, index, op, out);
    }
    let epoch = alloc.coll_arrive(index);
    alloc.coll_wait_all_arrive(epoch);
    reduce_peers(alloc, |i| alloc.coll_peer_addr::<T>(i) as *const T, 0, out, op);
    alloc.coll_done(epoch);
    alloc.coll_wait_all_done(epoch);
}

/// Small all-reduce by recursive doubling: log2(P) rounds, each waiting on one partner's
/// subslot (flag and data share a line), instead of every PE reading all P peers. PEs past the
/// largest power of two fold into a partner first and get the result copied back. Each pair
/// combines the lower rank's data first, so every PE ends with the same bits.
fn all_reduce_rd<T: Remote>(alloc: &ShmemOptAlloc, index: usize, op: &ReduceOp, out: &mut [T]) {
    let p = alloc.num_pes();
    let me = alloc.my_alloc_pe;
    let p2 = 1 << p.ilog2();
    let n = out.len();
    let bytes = |s: &[T]| unsafe { std::slice::from_raw_parts(s.as_ptr() as *const u8, std::mem::size_of_val(s)) };
    let epoch = alloc.coll_rd_begin();
    let sub = |idx: usize, k: usize| unsafe { std::slice::from_raw_parts(alloc.coll_rd_get::<T>(idx, epoch, k), n) };
    out.copy_from_slice(unsafe { std::slice::from_raw_parts((alloc.data as *const T).add(index), n) });
    if me >= p2 {
        alloc.coll_rd_put(epoch, 0, bytes(out));
        out.copy_from_slice(sub(me - p2, alloc.coll_rd_post()));
        alloc.coll_rd_end(epoch);
        return;
    }
    if me + p2 < p {
        local_reduce(op, out, sub(me + p2, 0));
    }
    let (mut dist, mut k) = (1, 1);
    while dist < p2 {
        let partner = me ^ dist;
        alloc.coll_rd_put(epoch, k, bytes(out));
        if me < partner {
            local_reduce(op, out, sub(partner, k));
        } else {
            out.copy_from_slice(sub(partner, k));
            local_reduce(op, out, sub(me, k));
        }
        dist <<= 1;
        k += 1;
    }
    if me + p2 < p {
        alloc.coll_rd_put(epoch, alloc.coll_rd_post(), bytes(out));
    }
    alloc.coll_rd_end(epoch);
}

// Reduce-scatter + allgather pays off once each PE would otherwise read P full inputs.
const RSAG_MIN_PES: usize = 4;

/// Mid-size all-reduce: PE i reduces chunk i of every peer's input (read in place) into its own
/// eager slot, then everyone copies all chunks out of the slots. Each PE reads 2 * len instead
/// of P * len. `done` doubles as "my chunk is ready" and "I've finished reading your input".
///
/// Slot reuse: the slot for `epoch` was last read at `epoch - 2`; writing it only after every PE
/// arrived at `epoch` implies everyone has returned from that op.
fn all_reduce_rsag<T: Remote>(alloc: &ShmemOptAlloc, index: usize, op: &ReduceOp, out: &mut [T]) {
    let p = alloc.num_pes();
    let me = alloc.my_alloc_pe;
    let (q, r) = (out.len() / p, out.len() % p);
    let range = |i: usize| {
        let start = i * q + std::cmp::min(i, r);
        start..start + q + (i < r) as usize
    };
    let epoch = alloc.coll_arrive(index);
    alloc.coll_wait_all_arrive(epoch);
    let mine = range(me);
    let slot = unsafe {
        std::slice::from_raw_parts_mut(alloc.coll_eager_peer::<T>(me, epoch) as *mut T, mine.len())
    };
    reduce_peers(alloc, |i| alloc.coll_peer_addr::<T>(i) as *const T, mine.start, slot, op);
    alloc.coll_done(epoch);
    // start with our own chunk, then rotate so PEs don't all poll the same peer first
    for k in 0..p {
        let i = (me + k) % p;
        alloc.coll_wait_done(i, epoch);
        let ri = range(i);
        let src = unsafe { std::slice::from_raw_parts(alloc.coll_eager_peer::<T>(i, epoch), ri.len()) };
        out[ri].copy_from_slice(src);
    }
}

/// Only `root` reads, so the others only wait for it (zero copy) or not at all (eager).
fn reduce<T: Remote>(
    root: usize,
    alloc: &ShmemOptAlloc,
    index: usize,
    len: usize,
    op: &ReduceOp,
    out: Option<&mut [T]>,
) {
    if eager::<T>(len) {
        let epoch = alloc.coll_eager_arrive(my_bytes::<T>(alloc, index, len));
        if let Some(out) = out {
            alloc.coll_wait_all_arrive(epoch);
            reduce_peers(alloc, |i| alloc.coll_eager_peer::<T>(i, epoch), 0, out, op);
        }
        return;
    }
    let epoch = alloc.coll_arrive(index);
    if let Some(out) = out {
        alloc.coll_wait_all_arrive(epoch);
        reduce_peers(alloc, |i| alloc.coll_peer_addr::<T>(i) as *const T, 0, out, op);
        alloc.coll_done(epoch);
    } else {
        alloc.coll_done(epoch);
        alloc.coll_wait_done(root, epoch);
    }
}

/// PE i reduces chunk i of every peer's `len` elements.
fn reduce_scatter<T: Remote>(alloc: &ShmemOptAlloc, index: usize, len: usize, op: &ReduceOp, out: &mut [T]) {
    let chunk = len / alloc.num_pes();
    let off = alloc.my_alloc_pe * chunk;
    if eager::<T>(len) {
        let epoch = alloc.coll_eager_arrive(my_bytes::<T>(alloc, index, len));
        alloc.coll_wait_all_arrive(epoch);
        reduce_peers(alloc, |i| alloc.coll_eager_peer::<T>(i, epoch), off, &mut out[..chunk], op);
        return;
    }
    let epoch = alloc.coll_arrive(index);
    alloc.coll_wait_all_arrive(epoch);
    reduce_peers(alloc, |i| alloc.coll_peer_addr::<T>(i) as *const T, off, &mut out[..chunk], op);
    alloc.coll_done(epoch);
    alloc.coll_wait_all_done(epoch);
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptCollectiveAllReduceFuture<T: Remote> {
    pub(crate) alloc: ShmemOptAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> ShmemOptCollectiveAllReduceFuture<T> {
    fn exec_op(&mut self) {
        all_reduce(&self.alloc, self.index, &self.op, &mut self.result[..self.len]);
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        res
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for ShmemOptCollectiveAllReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a ShmemOptCollectiveAllReduceFuture").print();
        }
    }
}

impl<T: Remote> From<ShmemOptCollectiveAllReduceFuture<T>> for CollectiveAllReduceOpHandle<T> {
    fn from(f: ShmemOptCollectiveAllReduceFuture<T>) -> CollectiveAllReduceOpHandle<T> {
        CollectiveAllReduceOpHandle {
            future: CollectiveAllReduceOpFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote> Future for ShmemOptCollectiveAllReduceFuture<T> {
    type Output = Vec<T>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let mut res = Vec::new();
        std::mem::swap(&mut self.result, &mut res);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptCollectiveAllReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: ShmemOptAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> ShmemOptCollectiveAllReduceIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        all_reduce(&self.alloc, self.index, &self.op, &mut self.result.as_mut_slice()[..self.len]);
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for ShmemOptCollectiveAllReduceIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a ShmemOptCollectiveAllReduceIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<ShmemOptCollectiveAllReduceIntoBufferFuture<T, B>>
    for CollectiveAllReduceIntoBufferOpHandle<T, B>
{
    fn from(
        f: ShmemOptCollectiveAllReduceIntoBufferFuture<T, B>,
    ) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        CollectiveAllReduceIntoBufferOpHandle {
            future: CollectiveAllReduceIntoBufferOpFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for ShmemOptCollectiveAllReduceIntoBufferFuture<T, B> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptCollectiveAllReduceInPlaceFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: ShmemOptAlloc,
    pub(super) op: ReduceOp,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
    phantom: std::marker::PhantomData<T>,
}

impl<T: Remote, B: AsLamellarBuffer<T>> ShmemOptCollectiveAllReduceInPlaceFuture<T, B> {
    fn exec_op(&mut self) {
        let len = self.result.len();
        assert!(
            len * std::mem::size_of::<T>() <= self.alloc.num_bytes(),
            "reduce_all_in_place buffer ({} bytes) exceeds shmem collective scratch capacity ({} bytes)",
            len * std::mem::size_of::<T>(),
            self.alloc.num_bytes(),
        );
        // stage the caller's (possibly off-alloc) buffer in this alloc's own memory, the only
        // memory peers can read, and put the alloc's contents back once every peer is done
        let alloc_slice = unsafe { self.alloc.as_mut_slice() };
        let my_slice = &mut alloc_slice[0..len];
        let to_replace: Vec<T> = my_slice.to_vec();
        my_slice.copy_from_slice(self.result.as_slice());
        all_reduce(&self.alloc, 0, &self.op, self.result.as_mut_slice());
        my_slice.copy_from_slice(&to_replace);
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();
        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for ShmemOptCollectiveAllReduceInPlaceFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a ShmemOptCollectiveAllReduceInPlaceFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<ShmemOptCollectiveAllReduceInPlaceFuture<T, B>>
    for CollectiveAllReduceInPlaceOpHandle<T, B>
{
    fn from(
        f: ShmemOptCollectiveAllReduceInPlaceFuture<T, B>,
    ) -> CollectiveAllReduceInPlaceOpHandle<T, B> {
        CollectiveAllReduceInPlaceOpHandle {
            future: CollectiveAllReduceInPlaceOpFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for ShmemOptCollectiveAllReduceInPlaceFuture<T, B> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptCollectiveReduceFuture<T: Remote> {
    pub(crate) alloc: ShmemOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(super) op: ReduceOp,
    pub(crate) target: RootOrBuffer<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> ShmemOptCollectiveReduceFuture<T> {
    fn exec_op(&mut self) {
        match &mut self.target {
            RootOrBuffer::Root(result) => {
                let root = self.alloc.my_alloc_pe;
                reduce(root, &self.alloc, self.index, self.len, &self.op, Some(&mut result[..self.len]));
            }
            RootOrBuffer::NotRoot(root) => {
                reduce::<T>(*root, &self.alloc, self.index, self.len, &self.op, None);
            }
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        match &mut self.target {
            RootOrBuffer::Root(r) => {
                let mut res = Vec::new();
                std::mem::swap(&mut res, r);
                Some(res)
            }
            RootOrBuffer::NotRoot(_) => None,
        }
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Option<Vec<T>>> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for ShmemOptCollectiveReduceFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a ShmemOptCollectiveReduceFuture").print();
        }
    }
}

impl<T: Remote> From<ShmemOptCollectiveReduceFuture<T>> for CollectiveReduceOpHandle<T> {
    fn from(f: ShmemOptCollectiveReduceFuture<T>) -> CollectiveReduceOpHandle<T> {
        CollectiveReduceOpHandle {
            future: CollectiveReduceOpFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote> Future for ShmemOptCollectiveReduceFuture<T> {
    type Output = Option<Vec<T>>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match &mut self.target {
            RootOrBuffer::Root(r) => {
                let mut res = Vec::new();
                std::mem::swap(&mut res, r);
                Poll::Ready(Some(res))
            }
            RootOrBuffer::NotRoot(_) => Poll::Ready(None),
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptCollectiveReduceIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: ShmemOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(super) op: ReduceOp,
    pub(crate) target: RootOrLamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> ShmemOptCollectiveReduceIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        match &mut self.target {
            RootOrLamellarBuffer::Root(result) => {
                let root = self.alloc.my_alloc_pe;
                let out = &mut result.as_mut_slice()[..self.len];
                reduce(root, &self.alloc, self.index, self.len, &self.op, Some(out));
            }
            RootOrLamellarBuffer::NotRoot(root) => {
                reduce::<T>(*root, &self.alloc, self.index, self.len, &self.op, None);
            }
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for ShmemOptCollectiveReduceIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a ShmemOptCollectiveReduceIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<ShmemOptCollectiveReduceIntoBufferFuture<T, B>>
    for CollectiveReduceIntoBufferOpHandle<T, B>
{
    fn from(
        f: ShmemOptCollectiveReduceIntoBufferFuture<T, B>,
    ) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        CollectiveReduceIntoBufferOpHandle {
            future: CollectiveReduceIntoBufferOpFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for ShmemOptCollectiveReduceIntoBufferFuture<T, B> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}
/// PEs with a `result` copy every peer's `len` elements as soon as that peer arrives.
/// `all_read`: every PE has a result, so all wait for all; otherwise only `root` reads.
fn gather<T: Remote>(
    root: usize,
    alloc: &ShmemOptAlloc,
    result: Option<&mut [T]>,
    index: usize,
    len: usize,
    all_read: bool,
) {
    if eager::<T>(len) {
        let epoch = alloc.coll_eager_arrive(my_bytes::<T>(alloc, index, len));
        if let Some(result) = result {
            for pe in 0..alloc.num_pes() {
                alloc.coll_wait_arrive(pe, epoch);
                let src = unsafe { std::slice::from_raw_parts(alloc.coll_eager_peer::<T>(pe, epoch), len) };
                result[(pe * len)..((pe + 1) * len)].copy_from_slice(src);
            }
        }
        return;
    }
    let epoch = alloc.coll_arrive(index);
    if let Some(result) = result {
        for pe in 0..alloc.num_pes() {
            alloc.coll_wait_arrive(pe, epoch);
            let src =
                unsafe { std::slice::from_raw_parts(alloc.coll_peer_addr::<T>(pe) as *const T, len) };
            result[(pe * len)..((pe + 1) * len)].copy_from_slice(src);
        }
    }
    alloc.coll_done(epoch);
    if all_read {
        alloc.coll_wait_all_done(epoch);
    } else {
        alloc.coll_wait_done(root, epoch);
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptCollectiveAllGatherFuture<T: Remote> {
    pub(crate) alloc: ShmemOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> ShmemOptCollectiveAllGatherFuture<T> {
    fn exec_op(&mut self) {
        gather(0, &self.alloc, Some(&mut self.result), self.index, self.len, true);
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        let mut res = Vec::new();
        std::mem::swap(&mut res, &mut self.result);

        res
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for ShmemOptCollectiveAllGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a ShmemOptCollectiveAllGatherFuture").print();
        }
    }
}

impl<T: Remote> From<ShmemOptCollectiveAllGatherFuture<T>> for CollectiveAllGatherOpHandle<T> {
    fn from(f: ShmemOptCollectiveAllGatherFuture<T>) -> CollectiveAllGatherOpHandle<T> {
        CollectiveAllGatherOpHandle {
            future: CollectiveAllGatherOpFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote> Future for ShmemOptCollectiveAllGatherFuture<T> {
    type Output = Vec<T>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let mut res = Vec::new();
        std::mem::swap(&mut res, &mut self.result);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptCollectiveAllGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: ShmemOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> ShmemOptCollectiveAllGatherIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        gather(
            0,
            &self.alloc,
            Some(self.result.as_mut_slice()),
            self.index,
            self.len,
            true,
        );
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for ShmemOptCollectiveAllGatherIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a ShmemOptCollectiveAllGatherIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<ShmemOptCollectiveAllGatherIntoBufferFuture<T, B>>
    for CollectiveAllGatherIntoBufferOpHandle<T, B>
{
    fn from(
        f: ShmemOptCollectiveAllGatherIntoBufferFuture<T, B>,
    ) -> CollectiveAllGatherIntoBufferOpHandle<T, B> {
        CollectiveAllGatherIntoBufferOpHandle {
            future: CollectiveAllGatherIntoBufferOpFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for ShmemOptCollectiveAllGatherIntoBufferFuture<T, B> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptCollectiveGatherFuture<T: Remote> {
    pub(crate) alloc: ShmemOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) target: RootOrBuffer<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> ShmemOptCollectiveGatherFuture<T> {
    fn exec_op(&mut self) {
        match &mut self.target {
            RootOrBuffer::Root(result) => {
                gather(
                    self.alloc.my_alloc_pe,
                    &self.alloc,
                    Some(result.as_mut_slice()),
                    self.index,
                    self.len,
                    false,
                );
            }
            RootOrBuffer::NotRoot(root) => {
                gather::<T>(*root, &self.alloc, None, self.index, self.len, false);
            }
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        match &mut self.target {
            RootOrBuffer::Root(r) => {
                let mut res = Vec::new();
                std::mem::swap(&mut res, r);
                Some(res)
            }
            RootOrBuffer::NotRoot(_) => None,
        }
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Option<Vec<T>>> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for ShmemOptCollectiveGatherFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a ShmemOptCollectiveGatherFuture").print();
        }
    }
}

impl<T: Remote> From<ShmemOptCollectiveGatherFuture<T>> for CollectiveGatherOpHandle<T> {
    fn from(f: ShmemOptCollectiveGatherFuture<T>) -> CollectiveGatherOpHandle<T> {
        CollectiveGatherOpHandle {
            future: CollectiveGatherOpFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote> Future for ShmemOptCollectiveGatherFuture<T> {
    type Output = Option<Vec<T>>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match &mut self.target {
            RootOrBuffer::Root(r) => {
                let mut res = Vec::new();
                std::mem::swap(&mut res, r);
                Poll::Ready(Some(res))
            }
            RootOrBuffer::NotRoot(_) => Poll::Ready(None),
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptCollectiveGatherIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: ShmemOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) target: RootOrLamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> ShmemOptCollectiveGatherIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        match &mut self.target {
            RootOrLamellarBuffer::Root(result) => {
                gather(
                    self.alloc.my_alloc_pe,
                    &self.alloc,
                    Some(result.as_mut_slice()),
                    self.index,
                    self.len,
                    false,
                );
            }
            RootOrLamellarBuffer::NotRoot(root) => {
                gather::<T>(*root, &self.alloc, None, self.index, self.len, false);
            }
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop for ShmemOptCollectiveGatherIntoBufferFuture<T, B> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a ShmemOptCollectiveGatherIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<ShmemOptCollectiveGatherIntoBufferFuture<T, B>>
    for CollectiveGatherIntoBufferOpHandle<T, B>
{
    fn from(
        f: ShmemOptCollectiveGatherIntoBufferFuture<T, B>,
    ) -> CollectiveGatherIntoBufferOpHandle<T, B> {
        CollectiveGatherIntoBufferOpHandle {
            future: CollectiveGatherIntoBufferOpFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for ShmemOptCollectiveGatherIntoBufferFuture<T, B> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptCollectiveAllToAllFuture<T: Remote> {
    pub(crate) alloc: ShmemOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> ShmemOptCollectiveAllToAllFuture<T> {
    fn exec_op(&mut self) {
        gather(0, &self.alloc, Some(&mut self.result), self.index, self.len, true);
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        let mut res = Vec::new();
        std::mem::swap(&mut res, &mut self.result);
        res
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for ShmemOptCollectiveAllToAllFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a ShmemOptCollectiveAllToAllFuture").print();
        }
    }
}

impl<T: Remote> From<ShmemOptCollectiveAllToAllFuture<T>> for CollectiveAllToAllOpHandle<T> {
    fn from(f: ShmemOptCollectiveAllToAllFuture<T>) -> CollectiveAllToAllOpHandle<T> {
        CollectiveAllToAllOpHandle {
            future: CollectiveAllToAllOpFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote> Future for ShmemOptCollectiveAllToAllFuture<T> {
    type Output = Vec<T>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let mut res = Vec::new();
        std::mem::swap(&mut res, &mut self.result);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptCollectiveAllToAllIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: ShmemOptAlloc,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> ShmemOptCollectiveAllToAllIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        gather(
            0,
            &self.alloc,
            Some(self.result.as_mut_slice()),
            self.index,
            self.len,
            true,
        );
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for ShmemOptCollectiveAllToAllIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a ShmemOptCollectiveAllToAllIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<ShmemOptCollectiveAllToAllIntoBufferFuture<T, B>>
    for CollectiveAllToAllIntoBufferOpHandle<T, B>
{
    fn from(
        f: ShmemOptCollectiveAllToAllIntoBufferFuture<T, B>,
    ) -> CollectiveAllToAllIntoBufferOpHandle<T, B> {
        CollectiveAllToAllIntoBufferOpHandle {
            future: CollectiveAllToAllIntoBufferOpFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for ShmemOptCollectiveAllToAllIntoBufferFuture<T, B> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptCollectiveBroadcastFuture<T: Remote> {
    pub(crate) alloc: ShmemOptAlloc,
    pub(crate) target: RootSrcOrBuffer<T>,
    pub(crate) len: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

fn broadcast_root_side<T: Remote>(alloc: &ShmemOptAlloc, root_index: usize, len: usize) {
    if eager::<T>(len) {
        alloc.coll_eager_arrive(my_bytes::<T>(alloc, root_index, len));
        return;
    }
    let epoch = alloc.coll_arrive(root_index);
    alloc.coll_done(epoch);
    alloc.coll_wait_all_done(epoch);
}

fn broadcast_non_root_side<T: Remote>(
    alloc: &ShmemOptAlloc,
    root_index: usize,
    len: usize,
    res: &mut [T],
) {
    if eager::<T>(len) {
        let epoch = alloc.coll_eager_arrive(&[]);
        alloc.coll_wait_arrive(root_index, epoch);
        res.copy_from_slice(unsafe { std::slice::from_raw_parts(alloc.coll_eager_peer::<T>(root_index, epoch), len) });
        return;
    }
    let epoch = alloc.coll_arrive(0);
    alloc.coll_wait_arrive(root_index, epoch);
    let src = unsafe {
        std::slice::from_raw_parts(alloc.coll_peer_addr::<T>(root_index) as *const T, len)
    };
    res.copy_from_slice(src);
    alloc.coll_done(epoch);
}

impl<T: Remote> ShmemOptCollectiveBroadcastFuture<T> {
    fn exec_op(&mut self) {
        match self.target {
            RootSrcOrBuffer::Root(index) => {
                broadcast_root_side::<T>(&self.alloc, index, self.len);
            }
            RootSrcOrBuffer::NotRoot(ref mut res, root) => {
                broadcast_non_root_side(&self.alloc, root, self.len, res.as_mut_slice());
            }
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Option<Vec<T>> {
        self.exec_op();
        match &mut self.target {
            RootSrcOrBuffer::Root(_) => None,
            RootSrcOrBuffer::NotRoot(items, _) => {
                let mut res = Vec::new();
                std::mem::swap(items, &mut res);
                Some(res)
            }
        }
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Option<Vec<T>>> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for ShmemOptCollectiveBroadcastFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a ShmemOptCollectiveBroadcastFuture").print();
        }
    }
}

impl<T: Remote> From<ShmemOptCollectiveBroadcastFuture<T>> for CollectiveBroadcastOpHandle<T> {
    fn from(f: ShmemOptCollectiveBroadcastFuture<T>) -> CollectiveBroadcastOpHandle<T> {
        CollectiveBroadcastOpHandle {
            future: CollectiveBroadcastOpFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote> Future for ShmemOptCollectiveBroadcastFuture<T> {
    type Output = Option<Vec<T>>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        match &mut self.target {
            RootSrcOrBuffer::Root(_) => Poll::Ready(None),
            RootSrcOrBuffer::NotRoot(items, _) => {
                let mut res = Vec::new();
                std::mem::swap(items, &mut res);
                Poll::Ready(Some(res))
            }
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptCollectiveBroadcastIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: ShmemOptAlloc,
    pub(crate) target: RootSrcOrLamellarBufferInner<T, B>,
    pub(crate) len: usize,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> ShmemOptCollectiveBroadcastIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        match self.target {
            RootSrcOrLamellarBufferInner::Root(index) => {
                broadcast_root_side::<T>(&self.alloc, index, self.len);
            }
            RootSrcOrLamellarBufferInner::NotRoot(ref mut res, root) => {
                broadcast_non_root_side(&self.alloc, root, self.len, res.as_mut_slice());
            }
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for ShmemOptCollectiveBroadcastIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a ShmemOptCollectiveBroadcastIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<ShmemOptCollectiveBroadcastIntoBufferFuture<T, B>>
    for CollectiveBroadcastIntoBufferOpHandle<T, B>
{
    fn from(
        f: ShmemOptCollectiveBroadcastIntoBufferFuture<T, B>,
    ) -> CollectiveBroadcastIntoBufferOpHandle<T, B> {
        CollectiveBroadcastIntoBufferOpHandle {
            future: CollectiveBroadcastIntoBufferOpFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for ShmemOptCollectiveBroadcastIntoBufferFuture<T, B> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

fn scatter_root_side<T: Remote>(alloc: &ShmemOptAlloc, index: usize, len: usize, res: &mut [T]) {
    let alloc_slice = unsafe { alloc.as_mut_slice() };
    res.copy_from_slice(&alloc_slice[index..index + len]);
    if eager::<T>(len * alloc.num_pes()) {
        alloc.coll_eager_arrive(my_bytes::<T>(alloc, index, len * alloc.num_pes()));
        return;
    }
    let epoch = alloc.coll_arrive(index);
    alloc.coll_done(epoch);
    alloc.coll_wait_all_done(epoch);
}

fn scatter_non_root_side<T: Remote>(
    alloc: &ShmemOptAlloc,
    root_index: usize,
    len: usize,
    res: &mut [T],
) {
    if eager::<T>(len * alloc.num_pes()) {
        let epoch = alloc.coll_eager_arrive(&[]);
        alloc.coll_wait_arrive(root_index, epoch);
        let src = unsafe { alloc.coll_eager_peer::<T>(root_index, epoch).add(alloc.my_alloc_pe * len) };
        res.copy_from_slice(unsafe { std::slice::from_raw_parts(src, len) });
        return;
    }
    let epoch = alloc.coll_arrive(0);
    alloc.coll_wait_arrive(root_index, epoch);
    let src = unsafe {
        std::slice::from_raw_parts(
            (alloc.coll_peer_addr::<T>(root_index) as *const T).add(alloc.my_alloc_pe * len),
            len,
        )
    };
    res.copy_from_slice(src);
    alloc.coll_done(epoch);
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptCollectiveScatterFuture<T: Remote> {
    pub(crate) alloc: ShmemOptAlloc,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    src_or_root_pe: ScatterInputInner,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> ShmemOptCollectiveScatterFuture<T> {
    fn exec_op(&mut self) {
        match self.src_or_root_pe {
            ScatterInputInner::Root(index) => {
                scatter_root_side(&self.alloc, index, self.len, &mut self.result);
            }
            ScatterInputInner::NotRoot(root) => {
                scatter_non_root_side(&self.alloc, root, self.len, &mut self.result);
            }
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        let mut res = Vec::new();
        std::mem::swap(&mut res, &mut self.result);
        res
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for ShmemOptCollectiveScatterFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a ShmemOptCollectiveScatterFuture").print();
        }
    }
}

impl<T: Remote> From<ShmemOptCollectiveScatterFuture<T>> for CollectiveScatterOpHandle<T> {
    fn from(f: ShmemOptCollectiveScatterFuture<T>) -> CollectiveScatterOpHandle<T> {
        CollectiveScatterOpHandle {
            future: CollectiveScatterOpFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote> Future for ShmemOptCollectiveScatterFuture<T> {
    type Output = Vec<T>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let mut res = Vec::new();
        std::mem::swap(&mut res, &mut self.result);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptCollectiveScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: ShmemOptAlloc,
    pub(crate) len: usize,
    src_or_root_pe: ScatterInputInner,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> ShmemOptCollectiveScatterIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        match self.src_or_root_pe {
            ScatterInputInner::Root(index) => {
                scatter_root_side(&self.alloc, index, self.len, self.result.as_mut_slice());
            }
            ScatterInputInner::NotRoot(root) => {
                scatter_non_root_side(&self.alloc, root, self.len, self.result.as_mut_slice());
            }
        }
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for ShmemOptCollectiveScatterIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a ShmemOptCollectiveScatterIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<ShmemOptCollectiveScatterIntoBufferFuture<T, B>>
    for CollectiveScatterIntoBufferOpHandle<T, B>
{
    fn from(
        f: ShmemOptCollectiveScatterIntoBufferFuture<T, B>,
    ) -> CollectiveScatterIntoBufferOpHandle<T, B> {
        CollectiveScatterIntoBufferOpHandle {
            future: CollectiveScatterIntoBufferOpFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future for ShmemOptCollectiveScatterIntoBufferFuture<T, B> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptCollectiveReduceScatterFuture<T: Remote> {
    pub(crate) alloc: ShmemOptAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: Vec<T>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote> ShmemOptCollectiveReduceScatterFuture<T> {
    fn exec_op(&mut self) {
        reduce_scatter(&self.alloc, self.index, self.len, &self.op, &mut self.result);
        self.spawned = true;
    }

    pub(crate) fn block(mut self) -> Vec<T> {
        self.exec_op();
        let mut res = Vec::new();
        std::mem::swap(&mut res, &mut self.result);
        res
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote> PinnedDrop for ShmemOptCollectiveReduceScatterFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a ShmemOptCollectiveReduceScatterFuture").print();
        }
    }
}

impl<T: Remote> From<ShmemOptCollectiveReduceScatterFuture<T>> for CollectiveReduceScatterOpHandle<T> {
    fn from(f: ShmemOptCollectiveReduceScatterFuture<T>) -> CollectiveReduceScatterOpHandle<T> {
        CollectiveReduceScatterOpHandle {
            future: CollectiveReduceScatterOpFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote> Future for ShmemOptCollectiveReduceScatterFuture<T> {
    type Output = Vec<T>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        let mut res = Vec::new();
        std::mem::swap(&mut res, &mut self.result);
        Poll::Ready(res)
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct ShmemOptCollectiveReduceScatterIntoBufferFuture<T: Remote, B: AsLamellarBuffer<T>> {
    pub(crate) alloc: ShmemOptAlloc,
    pub(super) op: ReduceOp,
    pub(crate) index: usize,
    pub(crate) len: usize,
    pub(crate) result: LamellarBuffer<T, B>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) counters: Option<Arc<[Arc<AMCounters>]>>,
    pub(crate) spawned: bool,
}

impl<T: Remote, B: AsLamellarBuffer<T>> ShmemOptCollectiveReduceScatterIntoBufferFuture<T, B> {
    fn exec_op(&mut self) {
        reduce_scatter(&self.alloc, self.index, self.len, &self.op, self.result.as_mut_slice());
        self.spawned = true;
    }

    pub(crate) fn block(mut self) {
        self.exec_op();
    }

    pub(crate) fn spawn(mut self) -> LamellarTask<()> {
        self.exec_op();

        let counters = self.counters.clone();
        self.scheduler.clone().spawn_task(self, counters)
    }
}

#[pinned_drop]
impl<T: Remote, B: AsLamellarBuffer<T>> PinnedDrop
    for ShmemOptCollectiveReduceScatterIntoBufferFuture<T, B>
{
    fn drop(self: Pin<&mut Self>) {
        if !self.spawned {
            RuntimeWarning::DroppedHandle("a ShmemOptCollectiveReduceScatterIntoBufferFuture").print();
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> From<ShmemOptCollectiveReduceScatterIntoBufferFuture<T, B>>
    for CollectiveReduceScatterIntoBufferOpHandle<T, B>
{
    fn from(
        f: ShmemOptCollectiveReduceScatterIntoBufferFuture<T, B>,
    ) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        CollectiveReduceScatterIntoBufferOpHandle {
            future: CollectiveReduceScatterIntoBufferOpFuture::ShmemOpt(f),
        }
    }
}

impl<T: Remote, B: AsLamellarBuffer<T>> Future
    for ShmemOptCollectiveReduceScatterIntoBufferFuture<T, B>
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            self.exec_op();
        }
        Poll::Ready(())
    }
}

impl CommAllocCollectiveAllReduce for ShmemOptAlloc {
    fn reduce_all<T: crate::Remote>(
        &self,
        scheduler: &std::sync::Arc<crate::scheduler::Scheduler>,
        counters: Option<std::sync::Arc<[std::sync::Arc<crate::active_messaging::AMCounters>]>>,
        index: usize,
        len: usize,
        op: crate::lamellae::collective::ReduceOp,
    ) -> crate::lamellae::collective::CollectiveAllReduceOpHandle<T> {
        ShmemOptCollectiveAllReduceFuture {
            alloc: self.clone(),
            op,
            index,
            len,
            result: (0..len).map(|_| unsafe { std::mem::zeroed() }).collect(),
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }

    fn reduce_all_into_buffer<T: crate::Remote, B: crate::AsLamellarBuffer<T>>(
        &self,
        scheduler: &std::sync::Arc<crate::scheduler::Scheduler>,
        counters: Option<std::sync::Arc<[std::sync::Arc<crate::active_messaging::AMCounters>]>>,
        index: usize,
        len: usize,
        op: crate::lamellae::collective::ReduceOp,
        dst: crate::LamellarBuffer<T, B>,
    ) -> crate::lamellae::collective::CollectiveAllReduceIntoBufferOpHandle<T, B> {
        ShmemOptCollectiveAllReduceIntoBufferFuture {
            alloc: self.clone(),
            op,
            index,
            len,
            result: dst,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }

    fn reduce_all_in_place<T: crate::Remote, B: crate::AsLamellarBuffer<T>>(
        &self, // TODO: This should probably take a multiple reference to self.
        scheduler: &std::sync::Arc<crate::scheduler::Scheduler>,
        counters: Option<std::sync::Arc<[std::sync::Arc<crate::active_messaging::AMCounters>]>>,
        src_and_dst: crate::LamellarBuffer<T, B>,
        op: crate::lamellae::collective::ReduceOp,
    ) -> crate::lamellae::collective::CollectiveAllReduceInPlaceOpHandle<T, B> {
        ShmemOptCollectiveAllReduceInPlaceFuture {
            alloc: self.clone(),
            op,
            result: src_and_dst,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
            phantom: std::marker::PhantomData,
        }
        .into()
    }
}

impl CommAllocCollectiveReduce for ShmemOptAlloc {
    fn reduce<T: crate::Remote>(
        &self,
        scheduler: &std::sync::Arc<crate::scheduler::Scheduler>,
        counters: Option<std::sync::Arc<[std::sync::Arc<crate::active_messaging::AMCounters>]>>,
        op: crate::lamellae::collective::ReduceOp,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> crate::lamellae::collective::CollectiveReduceOpHandle<T> {
        let target = if root_pe != self.my_alloc_pe {
            RootOrBuffer::NotRoot(root_pe)
        } else {
            RootOrBuffer::Root((0..len).map(|_| unsafe { std::mem::zeroed() }).collect())
        };
        ShmemOptCollectiveReduceFuture {
            alloc: self.clone(),
            index,
            len,
            op,
            target,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }

    fn reduce_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: ReduceOp,
        index: usize,
        len: usize,
        root_or_buffer: crate::lamellae::collective::RootOrLamellarBuffer<T, B>,
    ) -> crate::lamellae::collective::CollectiveReduceIntoBufferOpHandle<T, B> {
        ShmemOptCollectiveReduceIntoBufferFuture {
            alloc: self.clone(),
            index,
            len,
            op,
            target: root_or_buffer,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }
}

impl CommAllocCollectiveBroadcast for ShmemOptAlloc {
    fn broadcast<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src_or_pe: BroadcastInput,
        len: usize,
    ) -> CollectiveBroadcastOpHandle<T> {
        let target = match src_or_pe {
            BroadcastInput::Root(index) => RootSrcOrBuffer::Root(index),
            BroadcastInput::NotRoot(root) => RootSrcOrBuffer::NotRoot(
                (0..len).map(|_| unsafe { std::mem::zeroed() }).collect(),
                root,
            ),
        };
        ShmemOptCollectiveBroadcastFuture {
            alloc: self.clone(),
            target,
            len,
            scheduler: scheduler.clone(),
            counters,
            spawned: false,
        }
        .into()
    }

    fn broadcast_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        root_or_buffer: RootSrcOrLamellarBuffer<T, B>,
        len: usize,
    ) -> CollectiveBroadcastIntoBufferOpHandle<T, B> {
        ShmemOptCollectiveBroadcastIntoBufferFuture {
            alloc: self.clone(),
            target: root_or_buffer.into(),
            len,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
}

impl CommAllocCollectiveGather for ShmemOptAlloc {
    fn gather<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        root_pe: usize,
    ) -> CollectiveGatherOpHandle<T> {
        let target = if root_pe != self.my_alloc_pe {
            RootOrBuffer::NotRoot(root_pe)
        } else {
            RootOrBuffer::Root(
                (0..len * self.num_pes())
                    .map(|_| unsafe { std::mem::zeroed() })
                    .collect(),
            )
        };
        ShmemOptCollectiveGatherFuture {
            alloc: self.clone(),
            index,
            len,
            target,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn gather_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        root_or_buffer: RootOrLamellarBuffer<T, B>,
    ) -> CollectiveGatherIntoBufferOpHandle<T, B> {
        ShmemOptCollectiveGatherIntoBufferFuture {
            alloc: self.clone(),
            index,
            len,
            target: root_or_buffer,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
}

impl CommAllocCollectiveAllGather for ShmemOptAlloc {
    fn gather_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllGatherOpHandle<T> {
        ShmemOptCollectiveAllGatherFuture {
            alloc: self.clone(),
            index,
            len,
            result: (0..len * self.num_pes())
                .map(|_| unsafe { std::mem::zeroed() })
                .collect(),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn gather_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllGatherIntoBufferOpHandle<T, B> {
        ShmemOptCollectiveAllGatherIntoBufferFuture {
            alloc: self.clone(),
            index,
            len,
            result: dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
}

impl CommAllocCollectiveAllToAll for ShmemOptAlloc {
    fn alltoall<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
    ) -> CollectiveAllToAllOpHandle<T> {
        ShmemOptCollectiveAllToAllFuture {
            alloc: self.clone(),
            index,
            len,
            result: (0..len * self.num_pes())
                .map(|_| unsafe { std::mem::zeroed() })
                .collect(),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn alltoall_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        index: usize,
        len: usize,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveAllToAllIntoBufferOpHandle<T, B> {
        ShmemOptCollectiveAllToAllIntoBufferFuture {
            alloc: self.clone(),
            index,
            len,
            result: dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
}

impl CommAllocCollectiveScatter for ShmemOptAlloc {
    fn scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterOpHandle<T> {
        ShmemOptCollectiveScatterFuture {
            alloc: self.clone(),
            len,
            src_or_root_pe: src_or_root_pe.into(),
            result: (0..len).map(|_| unsafe { std::mem::zeroed() }).collect(),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn scatter_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        result: LamellarBuffer<T, B>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> CollectiveScatterIntoBufferOpHandle<T, B> {
        ShmemOptCollectiveScatterIntoBufferFuture {
            alloc: self.clone(),
            len,
            result: result,
            src_or_root_pe: src_or_root_pe.into(),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
}

impl CommAllocCollectiveReduceScatter for ShmemOptAlloc {
    fn reduce_scatter<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: ReduceOp,
        index: usize,
        len: usize,
    ) -> CollectiveReduceScatterOpHandle<T> {
        ShmemOptCollectiveReduceScatterFuture {
            alloc: self.clone(),
            op: op,
            index,
            len,
            result: (0..len / self.num_pes())
                .map(|_| unsafe { std::mem::zeroed() })
                .collect(),
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
    fn reduce_scatter_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: ReduceOp,
        index: usize,
        len: usize,
        dst: LamellarBuffer<T, B>,
    ) -> CollectiveReduceScatterIntoBufferOpHandle<T, B> {
        ShmemOptCollectiveReduceScatterIntoBufferFuture {
            alloc: self.clone(),
            op: op,
            index,
            len,
            result: dst,
            spawned: false,
            scheduler: scheduler.clone(),
            counters,
        }
        .into()
    }
}

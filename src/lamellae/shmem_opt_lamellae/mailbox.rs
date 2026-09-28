//! Zero-copy mailbox AM transport for shmem-opt (replaces the CommandQueue).
//!
//! One collective symmetric segment holds, in every PE's portion (the receiving side):
//! ```text
//! [ctl line: futex sleep word, panic word (PE0's is authoritative)]
//! [doorbell: one bit per source PE, padded to a cache line]
//! [free ring: tail line + FREE_DEPTH x {seq, addr}]   (buffers this PE leaked for peers)
//! [ring src0: tail line + DEPTH x SLOT] [ring src1] ...   (MPSC per source)
//! [stream src0: {tail, commit} line, head line, STREAM bytes] [stream src1] ...
//! ```
//! Small messages are copied inline into a ring slot and consumed in place. Large messages
//! are published as a pointer into the sender's rt heap (universally mapped); the receiver
//! reads them in place and hands the buffer back through the owner's free ring on drop.
//!
//! Ring slots use a Vyukov sequence encoded relative to a zeroed segment: for position `p`
//! with `lap = p & !mask`, `seq == lap` is free, `lap + 1` published and `lap + depth`
//! released (the next lap's free value).
//!
//! Streams (StreamBatcher) are per-pair byte rings of packed wire.rs records. Producers
//! reserve with a CAS on `tail`, encode in place, then publish in reservation order by
//! advancing `commit` (a producer waits for its predecessors' commits; reserve-to-commit
//! never awaits). A record never wraps: the rest of the lap becomes a PAD record, and a
//! lap never ends with a 1-3 byte gap, so every lap boundary is a record boundary. The
//! consumer copies everything committed into one frame and frees it at once.

use super::comm::ShmemOptComm;
use super::fabric::ShmemOptAlloc;
use super::heap::SMALL_GRANULE;
use crate::lamellae::{
    comm::{CommInfo, CommMem},
    Comm, CommAlloc, Lamellae, SerializedData, SERIALIZE_HEADER_LEN,
};
use crate::scheduler::Scheduler;

use crate::active_messaging::batching::{adaptive_batcher::stream_header, wire};

use parking_lot::Mutex;
use std::sync::atomic::{fence, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use zerocopy::IntoBytes;
use std::sync::Arc;
use tracing::trace;

const LINE: usize = 64;
const SLOT_HDR: usize = 32;
const KIND_INLINE: u64 = 1;
const KIND_PTR: u64 = 2;
const KIND_SHIFT: u32 = 56;
const LEN_MASK: u64 = (1 << KIND_SHIFT) - 1;
const DRAIN_BATCH: usize = 64;
const PUSH_SPIN: usize = 256;
const FREE_BATCH: usize = 256;
const REC_HDR_LEN64: u64 = wire::REC_HDR_LEN as u64;

fn env_usize(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(val) => val
            .parse::<usize>()
            .unwrap_or_else(|_| panic!("{name} must be an integer, got {val:?}")),
        Err(_) => default,
    }
}

fn round_up(v: usize, to: usize) -> usize {
    (v + to - 1) / to * to
}

#[inline(always)]
fn lap(pos: u64, mask: u64) -> u64 {
    pos & !mask
}

#[repr(C)]
struct Ctl {
    sleeping: AtomicU32,
    _pad: u32,
    panic: AtomicU64,
}

#[repr(C)]
struct SlotHdr {
    seq: AtomicU64,
    meta: u64, // kind << KIND_SHIFT | len
    ptr: u64,
    free: u64,
}

#[repr(C)]
struct StreamCtl {
    tail: AtomicU64,   // reserved (producers)
    commit: AtomicU64, // published, in order (producers)
    _pad: [u64; 6],
    head: AtomicU64, // consumed (consumer)
}

#[repr(C)]
struct FreeSlot {
    seq: AtomicU64,
    addr: u64,
}

#[derive(Clone, Copy, Debug)]
struct Geom {
    num_pes: usize,
    slot: usize,
    depth: u64,
    mask: u64,
    free_depth: u64,
    free_mask: u64,
    bell_words: usize,
    off_bells: usize,
    off_free: usize,
    off_rings: usize,
    ring_stride: usize,
    stream_cap: usize, // 0: streams disabled
    stream_max: usize, // largest record sent over a stream
    off_streams: usize,
    stream_stride: usize,
    portion: usize,
}

impl Geom {
    fn new(num_pes: usize) -> Geom {
        let slot = env_usize("LAMELLAR_SHMEM_SLOT", 256);
        let depth = env_usize("LAMELLAR_SHMEM_RING", 4);
        let free_depth = env_usize("LAMELLAR_SHMEM_FREE_RING", 4096);
        assert!(
            slot >= 2 * LINE && slot % LINE == 0,
            "LAMELLAR_SHMEM_SLOT must be a multiple of {LINE} and >= {}",
            2 * LINE
        );
        assert!(
            depth >= 2 && depth.is_power_of_two(),
            "LAMELLAR_SHMEM_RING must be a power of two >= 2"
        );
        assert!(
            free_depth >= 2 && free_depth.is_power_of_two(),
            "LAMELLAR_SHMEM_FREE_RING must be a power of two >= 2"
        );
        let bell_words = (num_pes + 63) / 64;
        let off_bells = LINE;
        let off_free = off_bells + round_up(bell_words * 8, LINE);
        let off_rings = off_free + LINE + round_up(free_depth * 16, LINE);
        let ring_stride = LINE + depth * slot;
        // per-pair byte ring: 32MB total per receiver, clamped, power of two
        let dflt = (32usize << 20) / num_pes.max(1);
        let dflt = 1usize << dflt.clamp(16 << 10, 256 << 10).ilog2();
        let stream_cap = env_usize("LAMELLAR_SHMEM_STREAM", dflt);
        assert!(
            stream_cap == 0 || (stream_cap >= 4096 && stream_cap.is_power_of_two() && stream_cap <= 1 << 24),
            "LAMELLAR_SHMEM_STREAM must be 0 or a power of two in [4096, 16M]"
        );
        // past ~4K the ring's two copies lose to a staged frame sent by pointer
        let stream_max = env_usize("LAMELLAR_SHMEM_STREAM_MAX", (stream_cap / 4).min(4096));
        assert!(
            stream_max <= stream_cap / 2,
            "LAMELLAR_SHMEM_STREAM_MAX must be <= LAMELLAR_SHMEM_STREAM / 2"
        );
        let off_streams = off_rings + num_pes * ring_stride;
        let stream_stride = if stream_cap == 0 { 0 } else { 2 * LINE + stream_cap };
        let portion = off_streams + num_pes * stream_stride;
        Geom {
            num_pes,
            slot,
            depth: depth as u64,
            mask: depth as u64 - 1,
            free_depth: free_depth as u64,
            free_mask: free_depth as u64 - 1,
            bell_words,
            off_bells,
            off_free,
            off_rings,
            ring_stride,
            stream_cap,
            stream_max,
            off_streams,
            stream_stride,
            portion,
        }
    }
    fn inline_cap(&self) -> usize {
        self.slot - SLOT_HDR
    }
}

/// Producer side claim on an MPSC ring, returns (position, slot address)
#[inline(always)]
unsafe fn try_claim(tail: &AtomicU64, slots: usize, stride: usize, mask: u64) -> Option<(u64, usize)> {
    let mut pos = tail.load(Ordering::Relaxed);
    loop {
        let slot = slots + (pos & mask) as usize * stride;
        let seq = (*(slot as *const AtomicU64)).load(Ordering::Acquire);
        let diff = seq.wrapping_sub(lap(pos, mask)) as i64;
        if diff == 0 {
            match tail.compare_exchange_weak(pos, pos + 1, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return Some((pos, slot)),
                Err(cur) => pos = cur,
            }
        } else if diff < 0 {
            return None; // previous lap still in use: full
        } else {
            pos = tail.load(Ordering::Relaxed); // another producer published here
        }
    }
}

/// Shared by the mailbox and every outstanding zero-copy view (free ring pushes)
pub(crate) struct MailboxShared {
    geom: Geom,
    bases: Vec<usize>, // universal address of each PE's mailbox portion
    overflow: Mutex<Vec<(usize, u64)>>,
    overflow_len: AtomicUsize,
}

impl MailboxShared {
    #[inline(always)]
    fn ctl(&self, pe: usize) -> &Ctl {
        unsafe { &*(self.bases[pe] as *const Ctl) }
    }
    #[inline(always)]
    fn bell(&self, dst: usize, word: usize) -> &AtomicU64 {
        unsafe { &*((self.bases[dst] + self.geom.off_bells + word * 8) as *const AtomicU64) }
    }
    #[inline(always)]
    fn free_tail(&self, owner: usize) -> &AtomicU64 {
        unsafe { &*((self.bases[owner] + self.geom.off_free) as *const AtomicU64) }
    }
    #[inline(always)]
    fn free_slots(&self, owner: usize) -> usize {
        self.bases[owner] + self.geom.off_free + LINE
    }
    #[inline(always)]
    fn ring_tail(&self, dst: usize, src: usize) -> &AtomicU64 {
        unsafe {
            &*((self.bases[dst] + self.geom.off_rings + src * self.geom.ring_stride)
                as *const AtomicU64)
        }
    }
    #[inline(always)]
    fn ring_slots(&self, dst: usize, src: usize) -> usize {
        self.bases[dst] + self.geom.off_rings + src * self.geom.ring_stride + LINE
    }
    #[inline(always)]
    fn ring_slot(&self, dst: usize, src: usize, pos: u64) -> usize {
        self.ring_slots(dst, src) + (pos & self.geom.mask) as usize * self.geom.slot
    }

    #[inline(always)]
    fn stream(&self, dst: usize, src: usize) -> &StreamCtl {
        unsafe {
            &*((self.bases[dst] + self.geom.off_streams + src * self.geom.stream_stride)
                as *const StreamCtl)
        }
    }
    #[inline(always)]
    fn stream_data(&self, dst: usize, src: usize) -> usize {
        self.bases[dst] + self.geom.off_streams + src * self.geom.stream_stride + 2 * LINE
    }

    fn try_push_free(&self, owner: usize, addr: u64) -> bool {
        let mask = self.geom.free_mask;
        match unsafe { try_claim(self.free_tail(owner), self.free_slots(owner), 16, mask) } {
            Some((pos, slot)) => {
                let slot = unsafe { &mut *(slot as *mut FreeSlot) };
                slot.addr = addr;
                slot.seq.store(lap(pos, mask) + 1, Ordering::Release);
                true
            }
            None => false,
        }
    }

    fn push_free(&self, owner: usize, addr: u64) {
        if !self.try_push_free(owner, addr) {
            self.overflow.lock().push((owner, addr));
            self.overflow_len.fetch_add(1, Ordering::Release);
        }
    }

    fn retry_overflow(&self) {
        if self.overflow_len.load(Ordering::Acquire) == 0 {
            return;
        }
        if let Some(mut overflow) = self.overflow.try_lock() {
            overflow.retain(|(owner, addr)| !self.try_push_free(*owner, *addr));
            self.overflow_len.store(overflow.len(), Ordering::Release);
        }
    }
}

/// Release action attached to a received view, run when the last reference drops
pub(crate) enum MailboxRelease {
    /// inline message: hand the ring slot back to its producers
    Slot { seq: *const AtomicU64, val: u64 },
    /// zero-copy message: return the sender's buffer through its free ring
    Remote {
        shared: Arc<MailboxShared>,
        owner: usize,
        addr: u64,
    },
    /// inline message copied out of its slot (slot already handed back)
    Owned(Vec<u8>),
}

// the slot pointer targets the mailbox segment, which outlives every view
unsafe impl Send for MailboxRelease {}
unsafe impl Sync for MailboxRelease {}

impl Drop for MailboxRelease {
    fn drop(&mut self) {
        match self {
            MailboxRelease::Slot { seq, val } => unsafe { (**seq).store(*val, Ordering::Release) },
            MailboxRelease::Remote {
                shared,
                owner,
                addr,
            } => shared.push_free(*owner, *addr),
            MailboxRelease::Owned(_) => {}
        }
    }
}

struct Consumer {
    heads: Vec<u64>,
    stream_heads: Vec<u64>,
    pending: Vec<u64>, // rings that hit the drain batch limit
}

pub(crate) struct Mailbox {
    my_pe: usize,
    num_pes: usize,
    comm: Arc<Comm>,
    _seg: ShmemOptAlloc,
    tmpl: ShmemOptAlloc, // views are derived from the rt reservation
    shared: Arc<MailboxShared>,
    consumer: Mutex<Consumer>,
    free_head: Mutex<u64>,
    stream_head_cache: Vec<AtomicU64>, // last seen consumer head of our stream to each dst
    outstanding: AtomicUsize, // leaked buffers peers have not returned yet
    futex: bool,
    inline_copy: bool,
    push_spin: usize,
    idle_spin: usize,
    stats: Option<[AtomicUsize; 11]>, // inline msgs, inline bytes, ptr msgs, ptr bytes, claims that waited, claim yields, wait ns, stream recs, stream bytes, stream full, stream frames recvd
}

impl std::fmt::Debug for Mailbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Mailbox {{ my_pe: {}, num_pes: {}, geom: {:?}, outstanding: {} }}",
            self.my_pe,
            self.num_pes,
            self.shared.geom,
            self.outstanding.load(Ordering::Relaxed)
        )
    }
}

impl Mailbox {
    /// Collective: every PE must call this at the same point during init
    pub(crate) fn new(comm: Arc<Comm>) -> Mailbox {
        #[allow(unreachable_patterns)]
        let shmem: &ShmemOptComm = match &*comm {
            Comm::ShmemOpt(c) => c,
            _ => unreachable!("mailbox requires the shmem-opt comm"),
        };
        let my_pe = shmem.my_pe();
        let num_pes = shmem.num_pes();
        let geom = Geom::new(num_pes);
        // collective allocs come back zeroed, which is the initial state of every ring
        let seg = unsafe {
            shmem
                .allocator
                .alloc(geom.portion, LINE, &(0..num_pes).collect::<Vec<_>>())
        };
        let bases: Vec<usize> = (0..num_pes).map(|pe| seg.pe_base_offset(pe)).collect();
        // NUMA owner placement: force this PE's own mailbox pages to fault in now, on
        // this thread (already core/NUMA-bound by the launcher), rather than later on
        // whichever peer's send happens to be the first write into a given ring slot.
        // No peer can reach these pages before every PE clears this barrier, so a
        // pre-touch here fully decides the physical placement.
        unsafe {
            let base = bases[my_pe] as *mut u8;
            let mut off = 0usize;
            while off < geom.portion {
                std::ptr::write_volatile(base.add(off), 0u8);
                off += SMALL_GRANULE;
            }
        }
        unsafe { shmem.allocator.barrier() };
        trace!(target: "shmem", "mailbox geom {:?}", geom);
        Mailbox {
            my_pe,
            num_pes,
            tmpl: shmem.rt_fabric.clone(),
            comm: comm.clone(),
            _seg: seg,
            shared: Arc::new(MailboxShared {
                geom,
                bases,
                overflow: Mutex::new(Vec::new()),
                overflow_len: AtomicUsize::new(0),
            }),
            consumer: Mutex::new(Consumer {
                heads: vec![0; num_pes],
                stream_heads: vec![0; num_pes],
                pending: vec![0; geom.bell_words],
            }),
            free_head: Mutex::new(0),
            stream_head_cache: (0..num_pes).map(|_| AtomicU64::new(0)).collect(),
            outstanding: AtomicUsize::new(0),
            futex: env_usize("LAMELLAR_SHMEM_FUTEX", 0) != 0,
            inline_copy: env_usize("LAMELLAR_SHMEM_INLINE_COPY", 1) != 0,
            push_spin: env_usize("LAMELLAR_SHMEM_PUSH_SPIN", PUSH_SPIN),
            idle_spin: env_usize("LAMELLAR_SHMEM_SPIN", 1 << 14),
            stats: (env_usize("LAMELLAR_SHMEM_STATS", 0) != 0).then(Default::default),
        }
    }

    pub(crate) fn idle_spin(&self) -> usize {
        self.idle_spin
    }
    pub(crate) fn futex(&self) -> bool {
        self.futex
    }

    //------------------------------ producer ------------------------------

    pub(crate) fn available_to_send(&self, dst: usize) -> bool {
        let sh = &self.shared;
        let pos = sh.ring_tail(dst, self.my_pe).load(Ordering::Relaxed);
        let slot = sh.ring_slot(dst, self.my_pe, pos);
        unsafe { (*(slot as *const AtomicU64)).load(Ordering::Acquire) == lap(pos, sh.geom.mask) }
    }

    async fn claim(&self, dst: usize) -> (u64, usize) {
        let sh = &self.shared;
        let tail = sh.ring_tail(dst, self.my_pe);
        let slots = sh.ring_slots(dst, self.my_pe);
        let mut spins = 0;
        let mut waited: Option<std::time::Instant> = None;
        loop {
            if let Some(c) = unsafe { try_claim(tail, slots, sh.geom.slot, sh.geom.mask) } {
                if let (Some(st), Some(t)) = (&self.stats, waited) {
                    st[4].fetch_add(1, Ordering::Relaxed);
                    st[6].fetch_add(t.elapsed().as_nanos() as usize, Ordering::Relaxed);
                }
                return c;
            }
            if waited.is_none() && self.stats.is_some() {
                waited = Some(std::time::Instant::now());
            }
            if spins < self.push_spin {
                spins += 1;
                std::hint::spin_loop();
            } else {
                spins = 0;
                if let Some(st) = &self.stats {
                    st[5].fetch_add(1, Ordering::Relaxed);
                }
                self.drain_frees();
                async_std::task::yield_now().await;
            }
        }
    }

    fn publish(&self, dst: usize, pos: u64, slot: usize, kind: u64, len: usize, ptr: u64, free: u64) {
        let sh = &self.shared;
        let hdr = unsafe { &mut *(slot as *mut SlotHdr) };
        hdr.meta = (kind << KIND_SHIFT) | len as u64;
        hdr.ptr = ptr;
        hdr.free = free;
        hdr.seq.store(lap(pos, sh.geom.mask) + 1, Ordering::Release);
        self.ring_bell(dst);
    }

    #[inline(always)]
    fn ring_bell(&self, dst: usize) {
        let sh = &self.shared;
        // order the publish before the doorbell check (pairs with the consumer's swap)
        fence(Ordering::SeqCst);
        let bell = sh.bell(dst, self.my_pe / 64);
        let bit = 1u64 << (self.my_pe % 64);
        if bell.load(Ordering::Relaxed) & bit == 0 {
            bell.fetch_or(bit, Ordering::SeqCst);
        }
        if self.futex {
            let ctl = sh.ctl(dst);
            if ctl.sleeping.load(Ordering::SeqCst) != 0 && ctl.sleeping.swap(0, Ordering::SeqCst) != 0 {
                futex_wake(&ctl.sleeping);
            }
        }
    }

    /// Appends one `len`-byte wire record to our stream to `dst`; `fill` encodes it in
    /// place (every byte, synchronously). Hands `fill` back if the ring lacks room.
    #[inline]
    pub(crate) fn stream_write<F: FnOnce(&mut [u8])>(&self, dst: usize, len: usize, fill: F) -> Result<(), F> {
        let g = &self.shared.geom;
        if len > g.stream_max {
            return Err(fill);
        }
        let s = self.shared.stream(dst, self.my_pe);
        let cap = g.stream_cap as u64;
        let len64 = len as u64;
        let cache = &self.stream_head_cache[dst];
        let mut t = s.tail.load(Ordering::Relaxed);
        let (t, rem, fits) = loop {
            let rem = cap - (t & (cap - 1));
            // never leave a 1-3 byte gap at the lap end (no room for a PAD header there)
            let fits = len64 <= rem && !(1..REC_HDR_LEN64).contains(&(rem - len64));
            let need = if fits { len64 } else { rem + len64 };
            if (t + need).saturating_sub(cache.load(Ordering::Acquire)) > cap {
                let head = s.head.load(Ordering::Acquire);
                cache.store(head, Ordering::Release);
                if (t + need).saturating_sub(head) > cap {
                    if let Some(st) = &self.stats {
                        st[9].fetch_add(1, Ordering::Relaxed);
                    }
                    return Err(fill);
                }
            }
            match s.tail.compare_exchange_weak(t, t + need, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break (t, rem, fits),
                Err(cur) => t = cur,
            }
        };
        let data = self.shared.stream_data(dst, self.my_pe);
        let (start, end) = if fits {
            (t, t + len64)
        } else {
            let pad = wire::pad_hdr(rem as usize);
            unsafe {
                std::ptr::copy_nonoverlapping(
                    pad.as_ptr(),
                    (data + (t & (cap - 1)) as usize) as *mut u8,
                    REC_HDR_LEN64 as usize,
                )
            };
            (t + rem, t + rem + len64)
        };
        fill(unsafe {
            std::slice::from_raw_parts_mut((data + (start & (cap - 1)) as usize) as *mut u8, len)
        });
        // publish in reservation order: wait for the producers ahead of us
        let mut spins = 0usize;
        while s.commit.load(Ordering::Acquire) != t {
            spins += 1;
            if spins < self.push_spin {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
        s.commit.store(end, Ordering::Release);
        self.ring_bell(dst);
        if let Some(st) = &self.stats {
            st[7].fetch_add(1, Ordering::Relaxed);
            st[8].fetch_add(len, Ordering::Relaxed);
        }
        Ok(())
    }

    // src is an address (not a pointer) so the future stays Send; caller keeps it alive
    async fn send_inline(&self, src: usize, len: usize, dst: usize) {
        let (pos, slot) = self.claim(dst).await;
        unsafe { std::ptr::copy_nonoverlapping(src as *const u8, (slot + SLOT_HDR) as *mut u8, len) };
        self.publish(dst, pos, slot, KIND_INLINE, len, 0, 0);
        if let Some(st) = &self.stats {
            st[0].fetch_add(1, Ordering::Relaxed);
            st[1].fetch_add(len, Ordering::Relaxed);
        }
    }

    /// publish a buffer the peer will read in place; `leaked` is returned via our free ring
    async fn send_ptr(&self, ptr: usize, len: usize, leaked: usize, dst: usize) {
        self.outstanding.fetch_add(1, Ordering::Relaxed);
        let (pos, slot) = self.claim(dst).await;
        self.publish(dst, pos, slot, KIND_PTR, len, ptr as u64, leaked as u64);
        if let Some(st) = &self.stats {
            st[2].fetch_add(1, Ordering::Relaxed);
            st[3].fetch_add(len, Ordering::Relaxed);
        }
    }

    pub(crate) async fn send(&self, data: &SerializedData, dst: usize) {
        let len = data.ser_data_bytes.len();
        let ptr = data.ser_data_bytes.as_ptr() as usize;
        if len <= self.shared.geom.inline_cap() {
            self.send_inline(ptr, len, dst).await;
        } else {
            let leaked = data
                .alloc
                .clone()
                .leak()
                .expect("serialized data must live in the rt heap")
                .0;
            self.send_ptr(ptr, len, leaked, dst).await;
        }
    }

    pub(crate) async fn send_bytes(&self, bytes: Vec<u8>, dst: usize) {
        let len = bytes.len();
        if len <= self.shared.geom.inline_cap() {
            self.send_inline(bytes.as_ptr() as usize, len, dst).await;
            return;
        }
        let alloc: CommAlloc = loop {
            match self.comm.rt_alloc_uninit(len, std::mem::align_of::<usize>()) {
                Ok(alloc) => break alloc,
                Err(_) => {
                    self.drain_frees();
                    async_std::task::yield_now().await;
                }
            }
        };
        let ptr = alloc.as_comm_slice::<u8>().as_mut_ptr() as usize;
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr as *mut u8, len) };
        let leaked = alloc.leak().expect("rt alloc must be leakable").0;
        self.send_ptr(ptr, len, leaked, dst).await;
    }

    //------------------------------ consumer ------------------------------

    /// Return buffers peers are done with to the rt heap
    pub(crate) fn drain_frees(&self) -> usize {
        let Some(mut head) = self.free_head.try_lock() else {
            return 0;
        };
        let sh = &self.shared;
        let mask = sh.geom.free_mask;
        let slots = sh.free_slots(self.my_pe);
        let mut n = 0;
        while n < FREE_BATCH {
            let slot = unsafe { &*((slots + (*head & mask) as usize * 16) as *const FreeSlot) };
            if slot.seq.load(Ordering::Acquire) != lap(*head, mask) + 1 {
                break;
            }
            let addr = slot.addr as usize;
            slot.seq
                .store(lap(*head, mask) + sh.geom.free_depth, Ordering::Release);
            *head += 1;
            drop(
                self.comm
                    .local_rt_alloc_from_local_addr(addr)
                    .expect("returned mailbox buffer is not an rt alloc"),
            );
            self.outstanding.fetch_sub(1, Ordering::Relaxed);
            n += 1;
        }
        n
    }

    fn view(&self, data: usize, len: usize, release: MailboxRelease) -> SerializedData {
        let alloc: CommAlloc = self.tmpl.mailbox_view(data as *mut u8, len, release).into();
        let ser_data_bytes = alloc.as_comm_slice::<u8>();
        let header_bytes = ser_data_bytes.sub_slice(0..SERIALIZE_HEADER_LEN);
        let payload_bytes = ser_data_bytes.sub_slice(SERIALIZE_HEADER_LEN..len);
        SerializedData {
            alloc,
            ser_data_bytes,
            header_bytes,
            payload_bytes,
        }
    }

    /// returns (messages consumed, ring still has more)
    fn drain_ring(
        &self,
        c: &mut Consumer,
        src: usize,
        lamellae: &Arc<Lamellae>,
        scheduler: &Scheduler,
    ) -> (usize, bool) {
        let sh = &self.shared;
        let mask = sh.geom.mask;
        for n in 0..DRAIN_BATCH {
            let h = c.heads[src];
            let slot = sh.ring_slot(self.my_pe, src, h);
            let hdr = unsafe { &*(slot as *const SlotHdr) };
            if hdr.seq.load(Ordering::Acquire) != lap(h, mask) + 1 {
                return (n, false);
            }
            c.heads[src] = h + 1;
            let released = lap(h, mask) + sh.geom.depth;
            let kind = hdr.meta >> KIND_SHIFT;
            let len = (hdr.meta & LEN_MASK) as usize;
            let data = match kind {
                // copy out so the slot frees at drain, not after the AM executes: holding
                // it ties producer credit to consumer exec latency
                KIND_INLINE if self.inline_copy => {
                    let v =
                        unsafe { std::slice::from_raw_parts((slot + SLOT_HDR) as *const u8, len) }.to_vec();
                    hdr.seq.store(released, Ordering::Release);
                    self.view(v.as_ptr() as usize, len, MailboxRelease::Owned(v))
                }
                KIND_INLINE => self.view(
                    slot + SLOT_HDR,
                    len,
                    MailboxRelease::Slot {
                        seq: &hdr.seq,
                        val: released,
                    },
                ),
                KIND_PTR => {
                    let (ptr, free) = (hdr.ptr as usize, hdr.free);
                    hdr.seq.store(released, Ordering::Release);
                    self.view(
                        ptr,
                        len,
                        MailboxRelease::Remote {
                            shared: sh.clone(),
                            owner: src,
                            addr: free,
                        },
                    )
                }
                _ => panic!("corrupt mailbox slot from pe {src}: kind {kind}"),
            };
            scheduler.submit_remote_am(data, lamellae);
        }
        (DRAIN_BATCH, true)
    }

    /// Copies everything committed on the stream from `src` into one frame and frees the
    /// ring space immediately (credit is never tied to AM execution).
    fn drain_stream(
        &self,
        c: &mut Consumer,
        src: usize,
        lamellae: &Arc<Lamellae>,
        scheduler: &Scheduler,
    ) -> usize {
        let sh = &self.shared;
        let cap = sh.geom.stream_cap;
        if cap == 0 {
            return 0;
        }
        let s = sh.stream(self.my_pe, src);
        let h = c.stream_heads[src];
        let commit = s.commit.load(Ordering::Acquire);
        if commit == h {
            return 0;
        }
        let len = (commit - h) as usize;
        let off = (h as usize) & (cap - 1);
        let base = sh.stream_data(self.my_pe, src);
        let first = len.min(cap - off);
        let mut v = Vec::with_capacity(SERIALIZE_HEADER_LEN + len);
        v.extend_from_slice(stream_header(src).as_bytes());
        unsafe {
            v.extend_from_slice(std::slice::from_raw_parts((base + off) as *const u8, first));
            if first < len {
                v.extend_from_slice(std::slice::from_raw_parts(base as *const u8, len - first));
            }
        }
        c.stream_heads[src] = commit;
        s.head.store(commit, Ordering::Release);
        if let Some(st) = &self.stats {
            st[10].fetch_add(1, Ordering::Relaxed);
        }
        let n = v.len();
        let data = self.view(v.as_ptr() as usize, n, MailboxRelease::Owned(v));
        scheduler.submit_remote_am(data, lamellae);
        1
    }

    /// Consume incoming messages and recycle returned buffers; returns work done
    pub(crate) fn progress(&self, lamellae: &Arc<Lamellae>, scheduler: &Scheduler) -> usize {
        let mut n = self.drain_frees();
        self.shared.retry_overflow();
        let Some(mut c) = self.consumer.try_lock() else {
            return n;
        };
        let sh = &self.shared;
        for w in 0..sh.geom.bell_words {
            let bell = sh.bell(self.my_pe, w);
            let mut bits = std::mem::take(&mut c.pending[w]);
            if bell.load(Ordering::Relaxed) != 0 {
                bits |= bell.swap(0, Ordering::SeqCst);
            }
            while bits != 0 {
                let b = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let (got, more) = self.drain_ring(&mut c, w * 64 + b, lamellae, scheduler);
                n += got + self.drain_stream(&mut c, w * 64 + b, lamellae, scheduler);
                if more {
                    c.pending[w] |= 1 << b;
                }
            }
        }
        n
    }

    /// A producer has rung our doorbell since the last drain (cheap: loads only)
    pub(crate) fn bell_rung(&self) -> bool {
        let sh = &self.shared;
        (0..sh.geom.bell_words).any(|w| sh.bell(self.my_pe, w).load(Ordering::Relaxed) != 0)
    }

    /// Block (bounded) until a producer rings our doorbell
    pub(crate) fn sleep(&self) {
        let sh = &self.shared;
        let ctl = sh.ctl(self.my_pe);
        ctl.sleeping.store(1, Ordering::SeqCst);
        let idle = (0..sh.geom.bell_words)
            .all(|w| sh.bell(self.my_pe, w).load(Ordering::SeqCst) == 0)
            && self.consumer.lock().pending.iter().all(|p| *p == 0);
        if idle {
            futex_wait(&ctl.sleeping, 1, 1_000_000);
        }
        ctl.sleeping.store(0, Ordering::Relaxed);
    }

    /// Nothing left to receive and every buffer we lent out has come back
    pub(crate) fn quiescent(&self) -> bool {
        let sh = &self.shared;
        if self.outstanding.load(Ordering::Acquire) != 0
            || sh.overflow_len.load(Ordering::Acquire) != 0
        {
            return false;
        }
        {
            let c = self.consumer.lock();
            if c.pending.iter().any(|p| *p != 0) {
                return false;
            }
            for src in 0..self.num_pes {
                let h = c.heads[src];
                let hdr = unsafe { &*(sh.ring_slot(self.my_pe, src, h) as *const SlotHdr) };
                if hdr.seq.load(Ordering::Acquire) == lap(h, sh.geom.mask) + 1 {
                    return false;
                }
                if sh.geom.stream_cap != 0 {
                    let s = sh.stream(self.my_pe, src);
                    if s.commit.load(Ordering::Acquire) != c.stream_heads[src] {
                        return false;
                    }
                }
            }
        }
        let head = *self.free_head.lock();
        let mask = sh.geom.free_mask;
        let slot = unsafe {
            &*((sh.free_slots(self.my_pe) + (head & mask) as usize * 16) as *const FreeSlot)
        };
        slot.seq.load(Ordering::Acquire) != lap(head, mask) + 1
    }

    pub(crate) fn send_panic(&self) {
        self.shared.ctl(0).panic.store(1, Ordering::SeqCst);
    }

    pub(crate) fn check_panic(&self) -> bool {
        self.shared.ctl(0).panic.load(Ordering::Relaxed) != 0
    }

    pub(crate) fn print_status(&self) {
        let sh = &self.shared;
        let c = self.consumer.lock();
        println!(
            "[{}] mailbox outstanding: {} overflow: {} pending: {:?}",
            self.my_pe,
            self.outstanding.load(Ordering::SeqCst),
            sh.overflow_len.load(Ordering::SeqCst),
            c.pending
        );
        for pe in 0..self.num_pes {
            println!(
                "[{}] from {pe}: head {} tail {} | to {pe}: tail {}",
                self.my_pe,
                c.heads[pe],
                sh.ring_tail(self.my_pe, pe).load(Ordering::SeqCst),
                sh.ring_tail(pe, self.my_pe).load(Ordering::SeqCst),
            );
            if sh.geom.stream_cap != 0 {
                let (i, o) = (sh.stream(self.my_pe, pe), sh.stream(pe, self.my_pe));
                println!(
                    "[{}] stream from {pe}: head {} commit {} tail {} | to {pe}: head {} commit {} tail {}",
                    self.my_pe,
                    c.stream_heads[pe],
                    i.commit.load(Ordering::SeqCst),
                    i.tail.load(Ordering::SeqCst),
                    o.head.load(Ordering::SeqCst),
                    o.commit.load(Ordering::SeqCst),
                    o.tail.load(Ordering::SeqCst),
                );
            }
        }
    }
}

fn futex_wait(word: &AtomicU32, val: u32, timeout_ns: i64) {
    let ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: timeout_ns,
    };
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            word as *const AtomicU32,
            libc::FUTEX_WAIT,
            val,
            &ts as *const libc::timespec,
            std::ptr::null::<u32>(),
            0u32,
        );
    }
}

fn futex_wake(word: &AtomicU32) {
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            word as *const AtomicU32,
            libc::FUTEX_WAKE,
            1i32,
            std::ptr::null::<libc::timespec>(),
            std::ptr::null::<u32>(),
            0u32,
        );
    }
}

impl Drop for Mailbox {
    fn drop(&mut self) {
        if let Some(st) = &self.stats {
            let v: Vec<usize> = st.iter().map(|s| s.load(Ordering::Relaxed)).collect();
            println!(
                "[{}] mailbox sent inline: {} msgs {} B (avg {}) | ptr: {} msgs {} B (avg {}) | full-ring waits: {} yields: {} total wait {:.3}s | stream: {} recs {} B, full {}, frames recvd {}",
                self.my_pe,
                v[0],
                v[1],
                v[1] / v[0].max(1),
                v[2],
                v[3],
                v[3] / v[2].max(1),
                v[4],
                v[5],
                v[6] as f64 * 1e-9,
                v[7],
                v[8],
                v[9],
                v[10]
            );
        }
    }
}

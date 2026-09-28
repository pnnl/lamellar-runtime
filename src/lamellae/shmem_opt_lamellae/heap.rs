//! Symmetric heap at a universal virtual address (shmem-opt M1).
//!
//! Every PE maps every other PE's backing file at the *same* virtual address, so a
//! shared byte has one address in every process:
//!
//! ```text
//!   base + pe * REGION  ..  base + (pe + 1) * REGION   <- PE `pe`'s region (sparse tmpfs file)
//! ```
//!
//! Address translation between PEs is pure arithmetic (`pe_of`, `translate`), there are
//! no per-allocation segments and no lookup tables on the hot path.
//!
//! Collective allocations are symmetric: one offset, valid in every region, is handed out
//! by an offset allocator living in a small control segment (`/lamellar_{job}_ctl`)
//! behind a cross-process lock. The team leader allocates and publishes the offset to each
//! member through a per-(leader, member) slot.
//!
//! All shm names are unlinked right after init, so nothing is left in `/dev/shm` even if
//! a PE is killed.

use std::ffi::CString;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crossbeam::utils::CachePadded;
use tracing::{debug, trace};

// MAP_FIXED_NOREPLACE (linux >= 4.17). Older kernels treat it as a hint, so the returned
// address is always verified as well.
const MAP_FIXED_NOREPLACE: libc::c_int = 0x100000;

const CTL_MAGIC: u64 = 0x4c41_4d5f_5348_4f50; // "LAM_SHOP"
const MAX_MAP_ATTEMPTS: usize = 16;
// fixed-candidate scan range for the shared VA window (x86-64 47-bit user VA; the top TiBs hold
// the stack + randomized top-down mmap base, the bottom holds the binary/brk under legacy layout)
const VA_TOP: usize = 0x7e00_0000_0000;
const VA_BOTTOM: usize = 1 << 40;
const SCAN_STEP: usize = 64 << 30;
const MAX_FREE: usize = 1024;
const MAX_PENDING: usize = 1024;
const WINDOW_ALIGN: usize = 1 << 30; // 1 GiB
const MAX_WINDOW: usize = 64 << 40; // 64 TiB of the 128 TiB x86-64 user VA
const DEFAULT_REGION: usize = 1 << 40; // 1 TiB
const INIT_TIMEOUT: Duration = Duration::from_secs(120);
const SPIN_BEFORE_YIELD: usize = 1 << 10;

pub(crate) const SMALL_GRANULE: usize = 4096;
pub(crate) const LARGE_GRANULE: usize = 2 * 1024 * 1024;

#[inline(always)]
pub(crate) fn round_up(v: usize, align: usize) -> usize {
    (v + align - 1) / align * align
}

/// Spin on `cond`, falling back to `yield_now` after a short budget.
#[inline]
pub(crate) fn spin_until(mut cond: impl FnMut() -> bool) {
    let mut spins = 0usize;
    while !cond() {
        if spins < SPIN_BEFORE_YIELD {
            std::hint::spin_loop();
            spins += 1;
        } else {
            std::thread::yield_now();
        }
    }
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Range {
    off: usize,
    len: usize,
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Pending {
    off: usize,
    len: usize,
    released: usize,
    num_pes: usize,
}

/// Symmetric offset allocator state; only touched while holding `CtlHeader::lock`.
#[repr(C)]
struct SymState {
    next: usize,
    limit: usize,
    free_len: usize,
    free: [Range; MAX_FREE],
    pending_len: usize,
    pending: [Pending; MAX_PENDING],
}

#[repr(C)]
struct CtlHeader {
    magic: AtomicU64,
    num_pes: AtomicUsize,
    region_shift: AtomicUsize,
    base: AtomicUsize,
    map_fail: [AtomicUsize; MAX_MAP_ATTEMPTS],
    bar_count: CachePadded<AtomicUsize>,
    bar_gen: CachePadded<AtomicUsize>,
    lock: CachePadded<AtomicUsize>,
    sym: SymState,
}

/// Leader -> member handoff of a collective allocation's symmetric offset.
/// `seq` is written only by the leader, `ack` only by the member.
#[repr(C)]
struct AllocSlot {
    seq: AtomicU64,
    ack: AtomicU64,
    off: AtomicUsize,
    len: AtomicUsize,
}

fn ctl_len(num_pes: usize) -> usize {
    round_up(
        std::mem::size_of::<CtlHeader>()
            + num_pes * num_pes * std::mem::size_of::<CachePadded<AllocSlot>>(),
        SMALL_GRANULE,
    )
}

fn shm_name(job_id: usize, suffix: &str) -> CString {
    CString::new(format!("/lamellar_{job_id}_{suffix}")).unwrap()
}

fn errno_str() -> String {
    std::io::Error::last_os_error().to_string()
}

fn region_shift_for(num_pes: usize) -> u32 {
    let region = match std::env::var("LAMELLAR_SHMEM_REGION") {
        Ok(v) => v
            .parse::<usize>()
            .expect("LAMELLAR_SHMEM_REGION must be a size in bytes")
            .next_power_of_two(),
        Err(_) => {
            let fit = (MAX_WINDOW / num_pes.max(1)).max(WINDOW_ALIGN);
            // largest power of two <= fit
            let fit = 1usize << (usize::BITS - 1 - fit.leading_zeros());
            std::cmp::min(DEFAULT_REGION, fit)
        }
    };
    region.trailing_zeros()
}

pub(crate) struct ShmemOptHeap {
    base: usize,
    region_shift: u32,
    window_len: usize,
    my_pe: usize,
    num_pes: usize,
    ctl: *mut CtlHeader,
    ctl_len: usize,
    shutting_down: AtomicBool,
}

unsafe impl Send for ShmemOptHeap {}
unsafe impl Sync for ShmemOptHeap {}

impl std::fmt::Debug for ShmemOptHeap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShmemOptHeap")
            .field("base", &format_args!("{:#x}", self.base))
            .field("region", &format_args!("{:#x}", self.region_len()))
            .field("my_pe", &self.my_pe)
            .field("num_pes", &self.num_pes)
            .finish()
    }
}

impl ShmemOptHeap {
    pub(crate) fn new(num_pes: usize, my_pe: usize, job_id: usize) -> ShmemOptHeap {
        let region_shift = region_shift_for(num_pes);
        let region_len = 1usize << region_shift;
        let window_len = region_len * num_pes;
        let ctl_len = ctl_len(num_pes);

        let ctl = unsafe { Self::attach_ctl(num_pes, my_pe, job_id, ctl_len, region_shift) };
        let my_fd = unsafe { Self::create_region_file(job_id, my_pe, region_len) };

        let mut heap = ShmemOptHeap {
            base: 0,
            region_shift,
            window_len,
            my_pe,
            num_pes,
            ctl,
            ctl_len,
            shutting_down: AtomicBool::new(false),
        };
        // every region file exists after this barrier
        heap.barrier();

        heap.base = unsafe { heap.reserve_window() };
        unsafe { heap.map_regions(job_id, my_fd) };
        heap.barrier();

        // every PE has mapped every file: names are no longer needed
        unsafe {
            libc::shm_unlink(shm_name(job_id, &my_pe.to_string()).as_ptr());
            if my_pe == 0 {
                libc::shm_unlink(shm_name(job_id, "ctl").as_ptr());
            }
        }
        debug!(target: "shmem", "shmem-opt heap ready: {:?}", heap);
        heap
    }

    unsafe fn attach_ctl(
        num_pes: usize,
        my_pe: usize,
        job_id: usize,
        len: usize,
        region_shift: u32,
    ) -> *mut CtlHeader {
        let name = shm_name(job_id, "ctl");
        let fd = if my_pe == 0 {
            libc::shm_unlink(name.as_ptr()); // stale segment from a crashed run
            let fd = libc::shm_open(
                name.as_ptr(),
                libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
                0o600,
            );
            assert!(fd >= 0, "shm_open({name:?}) failed: {}", errno_str());
            assert!(
                libc::ftruncate(fd, len as libc::off_t) == 0,
                "ftruncate ctl failed: {}",
                errno_str()
            );
            fd
        } else {
            let start = Instant::now();
            loop {
                let fd = libc::shm_open(name.as_ptr(), libc::O_RDWR, 0o600);
                if fd >= 0 {
                    let mut st: libc::stat = std::mem::zeroed();
                    if libc::fstat(fd, &mut st) == 0 && st.st_size as usize >= len {
                        break fd;
                    }
                    libc::close(fd);
                }
                assert!(
                    start.elapsed() < INIT_TIMEOUT,
                    "timed out waiting for PE0 to create {name:?}"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
        };
        let ptr = libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        );
        assert!(ptr != libc::MAP_FAILED, "mmap ctl failed: {}", errno_str());
        libc::close(fd);
        let ctl = ptr as *mut CtlHeader;
        let hdr = &*ctl;
        if my_pe == 0 {
            // ftruncate zero-filled everything
            hdr.num_pes.store(num_pes, Ordering::Relaxed);
            hdr.region_shift
                .store(region_shift as usize, Ordering::Relaxed);
            (*ctl).sym.limit = 1usize << region_shift;
            hdr.magic.store(CTL_MAGIC, Ordering::Release);
        } else {
            spin_until(|| hdr.magic.load(Ordering::Acquire) == CTL_MAGIC);
            assert_eq!(
                hdr.num_pes.load(Ordering::Relaxed),
                num_pes,
                "shmem-opt: PE count mismatch with PE0"
            );
            assert_eq!(
                hdr.region_shift.load(Ordering::Relaxed),
                region_shift as usize,
                "shmem-opt: LAMELLAR_SHMEM_REGION must be identical on all PEs"
            );
        }
        ctl
    }

    unsafe fn create_region_file(job_id: usize, my_pe: usize, region_len: usize) -> libc::c_int {
        let name = shm_name(job_id, &my_pe.to_string());
        libc::shm_unlink(name.as_ptr()); // stale segment from a crashed run
        let fd = libc::shm_open(
            name.as_ptr(),
            libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
            0o600,
        );
        assert!(fd >= 0, "shm_open({name:?}) failed: {}", errno_str());
        // sparse: only touched pages consume memory
        assert!(
            libc::ftruncate(fd, region_len as libc::off_t) == 0,
            "ftruncate region failed: {}",
            errno_str()
        );
        fd
    }

    /// Agree on a window base that is free in every process, and reserve it (PROT_NONE).
    unsafe fn reserve_window(&self) -> usize {
        let hdr = &*self.ctl;
        let mut last_pick = 0usize;
        for attempt in 0..MAX_MAP_ATTEMPTS {
            if self.my_pe == 0 {
                let pick = self.pick_base(attempt, &mut last_pick);
                hdr.base.store(pick, Ordering::Release);
            }
            self.barrier();
            let base = hdr.base.load(Ordering::Acquire);
            let ok = base != 0 && Self::reserve_at(base, self.window_len);
            if !ok {
                hdr.map_fail[attempt].fetch_add(1, Ordering::AcqRel);
            }
            self.barrier();
            if hdr.map_fail[attempt].load(Ordering::Acquire) == 0 {
                trace!(target: "shmem", "reserved VA window {:#x} (+{:#x}) attempt {}", base, self.window_len, attempt);
                return base;
            }
            if ok {
                libc::munmap(base as *mut libc::c_void, self.window_len);
            }
        }
        panic!(
            "shmem-opt: could not find a {:#x}-byte VA window free in all {} PEs after {} attempts; \
             lower LAMELLAR_SHMEM_REGION",
            self.window_len, self.num_pes, MAX_MAP_ATTEMPTS
        );
    }

    unsafe fn pick_base(&self, attempt: usize, last_pick: &mut usize) -> usize {
        let len = self.window_len + WINDOW_ALIGN;
        if attempt == 0 {
            // let the kernel choose; usually free everywhere
            let p = libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_NONE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            );
            if p == libc::MAP_FAILED {
                return 0;
            }
            libc::munmap(p, len);
            *last_pick = round_up(p as usize, WINDOW_ALIGN);
            return *last_pick;
        }
        // After a collision, scan fixed candidates downward from the top of the user VA,
        // always starting a full window below the last collided pick (when it was in the
        // scan range). The kernel's own pick is useless here: under the legacy (bottom-up)
        // mmap layout it sits low, and the old "walk down from it" hint underflowed.
        let top = (VA_TOP - self.window_len) & !(WINDOW_ALIGN - 1);
        let mut cand = if attempt == 1 || *last_pick > top {
            top
        } else {
            last_pick.saturating_sub(self.window_len)
        };
        while cand >= VA_BOTTOM {
            if Self::reserve_at(cand, self.window_len) {
                libc::munmap(cand as *mut libc::c_void, self.window_len);
                *last_pick = cand;
                return cand;
            }
            cand -= SCAN_STEP;
        }
        0
    }

    unsafe fn reserve_at(base: usize, len: usize) -> bool {
        let p = libc::mmap(
            base as *mut libc::c_void,
            len,
            libc::PROT_NONE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE | MAP_FIXED_NOREPLACE,
            -1,
            0,
        );
        if p == libc::MAP_FAILED {
            return false;
        }
        if p as usize != base {
            libc::munmap(p, len);
            return false;
        }
        true
    }

    /// Map every PE's region file over our own reservation.
    unsafe fn map_regions(&self, job_id: usize, my_fd: libc::c_int) {
        let region_len = self.region_len();
        for pe in 0..self.num_pes {
            let fd = if pe == self.my_pe {
                my_fd
            } else {
                let name = shm_name(job_id, &pe.to_string());
                let fd = libc::shm_open(name.as_ptr(), libc::O_RDWR, 0o600);
                assert!(fd >= 0, "shm_open({name:?}) failed: {}", errno_str());
                fd
            };
            let addr = self.region_base(pe);
            // MAP_FIXED is safe here: the range is our own PROT_NONE reservation
            let p = libc::mmap(
                addr as *mut libc::c_void,
                region_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_FIXED | libc::MAP_NORESERVE,
                fd,
                0,
            );
            assert!(
                p as usize == addr,
                "mmap region of pe {pe} at {addr:#x} failed: {}",
                errno_str()
            );
            libc::close(fd);
        }
        // effective when /sys/kernel/mm/transparent_hugepage/shmem_enabled is advise/always
        libc::madvise(
            self.base as *mut libc::c_void,
            self.window_len,
            libc::MADV_HUGEPAGE,
        );
    }

    #[inline(always)]
    pub(crate) fn region_shift(&self) -> u32 {
        self.region_shift
    }

    #[inline(always)]
    pub(crate) fn region_len(&self) -> usize {
        1usize << self.region_shift
    }

    #[inline(always)]
    pub(crate) fn region_base(&self, pe: usize) -> usize {
        self.base + (pe << self.region_shift)
    }

    /// PE owning a universal address, if it lies in the heap window.
    #[inline(always)]
    pub(crate) fn pe_of(&self, addr: usize) -> Option<usize> {
        if addr >= self.base && addr < self.base + self.window_len {
            Some((addr - self.base) >> self.region_shift)
        } else {
            None
        }
    }

    /// Address of the byte at the same region offset as `addr` (in `from`'s region) in `to`'s region.
    #[inline(always)]
    pub(crate) fn translate(&self, addr: usize, from: usize, to: usize) -> usize {
        addr - (from << self.region_shift) + (to << self.region_shift)
    }

    pub(crate) fn set_shutting_down(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
    }

    fn hdr(&self) -> &CtlHeader {
        unsafe { &*self.ctl }
    }

    fn slot(&self, leader: usize, member: usize) -> &AllocSlot {
        unsafe {
            let slots = (self.ctl as *mut u8).add(std::mem::size_of::<CtlHeader>())
                as *const CachePadded<AllocSlot>;
            &*slots.add(leader * self.num_pes + member)
        }
    }

    /// Central sense-reversing (generation) barrier across all PEs.
    // TODO(M5): dissemination barrier
    pub(crate) fn barrier(&self) {
        let hdr = self.hdr();
        let gen = hdr.bar_gen.load(Ordering::Acquire);
        if hdr.bar_count.fetch_add(1, Ordering::AcqRel) + 1 == self.num_pes {
            hdr.bar_count.store(0, Ordering::Relaxed);
            hdr.bar_gen.store(gen.wrapping_add(1), Ordering::Release);
        } else {
            spin_until(|| hdr.bar_gen.load(Ordering::Acquire) != gen);
        }
    }

    fn lock(&self) -> CtlGuard<'_> {
        let lock = &self.hdr().lock;
        spin_until(|| {
            lock.load(Ordering::Relaxed) == 0
                && lock
                    .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
        });
        CtlGuard {
            heap: self,
            sym: unsafe { &mut (*self.ctl).sym },
        }
    }

    /// Obtain a symmetric offset for a collective allocation of `len` bytes per PE over `pes`
    /// (`pes[my_idx] == my_pe`). Collective: every PE in `pes` must call this, in the same
    /// order relative to other collective allocations sharing members.
    pub(crate) fn collective_offset(
        &self,
        pes: &[usize],
        my_idx: usize,
        len: usize,
        align: usize,
    ) -> usize {
        let leader = pes[0];
        if my_idx == 0 {
            let off = self.lock().alloc(len, align).unwrap_or_else(|| {
                panic!(
                    "shmem-opt: symmetric heap exhausted allocating {len:#x} bytes \
                     (region {:#x}); increase LAMELLAR_SHMEM_REGION",
                    self.region_len()
                )
            });
            for &member in &pes[1..] {
                let slot = self.slot(leader, member);
                let seq = slot.seq.load(Ordering::Relaxed) + 1;
                // member must have consumed the previous handoff
                spin_until(|| slot.ack.load(Ordering::Acquire) == seq - 1);
                slot.off.store(off, Ordering::Relaxed);
                slot.len.store(len, Ordering::Relaxed);
                slot.seq.store(seq, Ordering::Release);
            }
            off
        } else {
            let slot = self.slot(leader, self.my_pe);
            let seq = slot.ack.load(Ordering::Relaxed) + 1;
            spin_until(|| slot.seq.load(Ordering::Acquire) == seq);
            let off = slot.off.load(Ordering::Relaxed);
            let leader_len = slot.len.load(Ordering::Relaxed);
            slot.ack.store(seq, Ordering::Release);
            assert_eq!(
                leader_len, len,
                "shmem-opt: collective alloc size mismatch with leader pe {leader}"
            );
            off
        }
    }

    /// Called once per PE when its last reference to a collective allocation drops.
    /// The final releaser zeroes (hole-punches) every PE's portion and recycles the offset.
    pub(crate) fn release_offset(&self, off: usize, len: usize, pes: &[usize]) {
        if self.shutting_down.load(Ordering::Relaxed) {
            // peers may still be reading; the files die with the processes anyway
            return;
        }
        let done = self.lock().release(off, len, pes.len());
        if done {
            for &pe in pes {
                let addr = (self.region_base(pe) + off) as *mut libc::c_void;
                unsafe {
                    if libc::madvise(addr, len, libc::MADV_REMOVE) != 0 {
                        std::ptr::write_bytes(addr as *mut u8, 0, len);
                    }
                }
            }
            self.lock().free(off, len);
        }
    }
}

impl Drop for ShmemOptHeap {
    fn drop(&mut self) {
        unsafe {
            if self.base != 0 {
                libc::munmap(self.base as *mut libc::c_void, self.window_len);
            }
            libc::munmap(self.ctl as *mut libc::c_void, self.ctl_len);
        }
    }
}

struct CtlGuard<'a> {
    heap: &'a ShmemOptHeap,
    sym: &'a mut SymState,
}

impl Drop for CtlGuard<'_> {
    fn drop(&mut self) {
        self.heap.hdr().lock.store(0, Ordering::Release);
    }
}

impl CtlGuard<'_> {
    fn alloc(&mut self, len: usize, align: usize) -> Option<usize> {
        let s = &mut *self.sym;
        // first fit from the free list
        for i in 0..s.free_len {
            let r = s.free[i];
            let start = round_up(r.off, align);
            if start + len <= r.off + r.len {
                let head = Range {
                    off: r.off,
                    len: start - r.off,
                };
                let tail = Range {
                    off: start + len,
                    len: r.off + r.len - (start + len),
                };
                s.free_len -= 1;
                s.free[i] = s.free[s.free_len];
                for rem in [head, tail] {
                    // a remainder that doesn't fit in the table is leaked VA (no memory)
                    if rem.len > 0 && s.free_len < MAX_FREE {
                        s.free[s.free_len] = rem;
                        s.free_len += 1;
                    }
                }
                return Some(start);
            }
        }
        let start = round_up(s.next, align);
        if start + len > s.limit {
            return None;
        }
        s.next = start + len;
        Some(start)
    }

    fn free(&mut self, off: usize, len: usize) {
        let s = &mut *self.sym;
        let mut r = Range { off, len };
        // coalesce with neighbours
        let mut i = 0;
        while i < s.free_len {
            let f = s.free[i];
            if f.off + f.len == r.off || r.off + r.len == f.off {
                r = Range {
                    off: f.off.min(r.off),
                    len: f.len + r.len,
                };
                s.free_len -= 1;
                s.free[i] = s.free[s.free_len];
            } else {
                i += 1;
            }
        }
        if r.off + r.len == s.next {
            s.next = r.off;
        } else if s.free_len < MAX_FREE {
            s.free[s.free_len] = r;
            s.free_len += 1;
        }
    }

    /// Returns true when this was the last of `num_pes` releases for `off`.
    fn release(&mut self, off: usize, len: usize, num_pes: usize) -> bool {
        if num_pes == 1 {
            return true;
        }
        let s = &mut *self.sym;
        for i in 0..s.pending_len {
            if s.pending[i].off == off {
                s.pending[i].released += 1;
                if s.pending[i].released == s.pending[i].num_pes {
                    s.pending_len -= 1;
                    s.pending[i] = s.pending[s.pending_len];
                    return true;
                }
                return false;
            }
        }
        if s.pending_len < MAX_PENDING {
            s.pending[s.pending_len] = Pending {
                off,
                len,
                released: 1,
                num_pes,
            };
            s.pending_len += 1;
        }
        // table full: the range is leaked (never reused)
        false
    }
}

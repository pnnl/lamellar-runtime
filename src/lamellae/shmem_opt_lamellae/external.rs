//! Caller-owned memory (for example a program's `.data`/`.bss`) made reachable by the other PEs of a shmem-opt job.
//!
//! shmem-opt reaches a peer's memory by arithmetic over a shared window (see `heap.rs`), and a program's data
//! segment is not in that window. To make it reachable the range is turned into shared memory in place:
//!
//! 1. every PE creates a shared file (`/lamellar_{job}_ext{id}_{pe}`) as large as the range and copies the range's
//!    current contents into it;
//! 2. the file is mapped `MAP_SHARED | MAP_FIXED` over the original range, so the program keeps using the same
//!    addresses and sees the same bytes;
//! 3. after a barrier every PE maps every peer's file at an address of its own choosing;
//! 4. after a second barrier the names are unlinked.
//!
//! A put, get or atomic on a peer's range is then a plain load, store or CPU atomic at `peer_base[pe] + offset`. All
//! PEs touch the same physical pages, so CPU atomics are coherent. Only RMA and atomics are supported on these
//! allocations (no collectives).
//!
//! What this changes, besides the registration itself (see the module docs of the user-facing switch,
//! `LAMELLAR_SHMEM_OPT_EXTERNAL`): the range becomes shared memory. A child created by `fork()` shares it with its
//! parent; the range is backed by tmpfs pages (no transparent huge pages unless tmpfs is configured for them); it
//! counts as shared memory in cgroup accounting.
//!
//! The copy-and-remap step must not race with writers to the range: call it before the program starts threads that
//! use its globals. In a build that links the runtime statically into the executable, the runtime's own statics are
//! part of the range, so the copy and the remap are kept in one short section that touches only locals.

use std::ffi::CString;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::lamellae::comm::{
    error::{AllocError, AllocResult},
    CommAllocAddr,
};

use super::heap::ShmemOptHeap;

/// Registration ids; registrations are collective and ordered, so every PE draws the same sequence.
static EXT_ID: AtomicUsize = AtomicUsize::new(0);

/// Remapping the caller's memory is invasive (see the module docs), so it is opt-in at the runtime level.
pub(crate) fn enabled() -> bool {
    std::env::var("LAMELLAR_SHMEM_OPT_EXTERNAL")
        .map(|v| v == "1")
        .unwrap_or(false)
}

struct ExternInner {
    my_pe: usize,
    num_pes: usize,
    /// Addressable bytes of each PE's range (the registered length rounded up to whole pages).
    len: usize,
    /// Where each PE's range lives in THIS process's address space; our own entry is the original range.
    peer_base: Vec<usize>,
    /// Peer mappings created here, unmapped when the allocation is dropped.
    peer_maps: Vec<(usize, usize)>,
}

impl Drop for ExternInner {
    fn drop(&mut self) {
        for &(addr, len) in &self.peer_maps {
            unsafe { libc::munmap(addr as *mut libc::c_void, len) };
        }
    }
}

#[derive(Clone)]
pub(crate) struct ShmemOptExternAlloc {
    inner: Arc<ExternInner>,
}

unsafe impl Send for ShmemOptExternAlloc {}
unsafe impl Sync for ShmemOptExternAlloc {}

impl std::fmt::Debug for ShmemOptExternAlloc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShmemOptExternAlloc")
            .field("my_pe", &self.inner.my_pe)
            .field("num_pes", &self.inner.num_pes)
            .field("len", &self.inner.len)
            .field("local", &format_args!("{:#x}", self.inner.peer_base[self.inner.my_pe]))
            .finish()
    }
}

// The methods below are what the allocation dispatch (`comm/alloc.rs`) and the generated RDMA/atomic impls call.
impl ShmemOptExternAlloc {
    pub(crate) fn num_pes(&self) -> usize {
        self.inner.num_pes
    }
    pub(crate) fn num_bytes(&self) -> usize {
        self.inner.len
    }
    pub(crate) fn start(&self) -> usize {
        self.inner.peer_base[self.inner.my_pe]
    }
    /// Base of world PE `pe`'s range in this process's address space.
    pub(crate) fn pe_base_offset(&self, pe: usize) -> usize {
        self.inner.peer_base[pe]
    }
    /// Same as `pe_base_offset`: an external allocation always spans every PE, in world order.
    pub(crate) fn pe_base_offset_rel(&self, idx: usize) -> usize {
        self.inner.peer_base[idx]
    }
    /// The memory is not ours to hand out or free, so there is nothing to leak.
    pub(crate) fn leak(self) -> Option<CommAllocAddr> {
        None
    }
    /// Peers are addressed through `pe_base_offset`, never by translating a local address.
    pub(crate) fn peer_copy_addr(&self, _pe: usize, _addr: usize) -> Option<usize> {
        None
    }
    pub(crate) fn sub_alloc(&self, _offset: usize, _len: usize) -> AllocResult<Self> {
        Err(AllocError::Unsupported("sub-allocation of externally registered memory"))
    }
    /// Stores are complete when issued; nothing to wait for.
    pub(crate) fn wait(&self) {}
}

fn page_size() -> usize {
    unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
}

/// `[lo, hi)` of every private, readable and writable mapping of this process.
fn rw_private_mappings() -> Vec<(usize, usize)> {
    let text = std::fs::read_to_string("/proc/self/maps").unwrap_or_default();
    let mut maps = Vec::new();
    for line in text.lines() {
        let mut it = line.split_whitespace();
        let (Some(range), Some(perms)) = (it.next(), it.next()) else {
            continue;
        };
        let p = perms.as_bytes();
        if p.len() >= 4 && p[0] == b'r' && p[1] == b'w' && p[3] == b'p' {
            if let Some((lo, hi)) = range.split_once('-') {
                if let (Ok(lo), Ok(hi)) = (usize::from_str_radix(lo, 16), usize::from_str_radix(hi, 16)) {
                    maps.push((lo, hi));
                }
            }
        }
    }
    maps.sort();
    maps
}

/// End of the run of adjacent private writable mappings that begins at `addr`; `None` if `addr` is not in one.
fn covered_until(maps: &[(usize, usize)], addr: usize) -> Option<usize> {
    let mut cur = addr;
    let mut covered = false;
    for &(lo, hi) in maps {
        if lo <= cur && cur < hi {
            cur = hi;
            covered = true;
        }
    }
    covered.then_some(cur)
}

/// Collectively make `len` bytes at `ptr` reachable by every PE (see the module docs). Every PE must call this, in the
/// same order relative to other collective allocations. `ptr` may differ between PEs.
pub(super) fn register(
    heap: &ShmemOptHeap,
    my_pe: usize,
    num_pes: usize,
    job_id: usize,
    ptr: *mut u8,
    len: usize,
) -> AllocResult<ShmemOptExternAlloc> {
    if !enabled() {
        return Err(AllocError::Unsupported(
            "shmem-opt external registration is off (set LAMELLAR_SHMEM_OPT_EXTERNAL=1: it turns the range into shared memory)",
        ));
    }
    let page = page_size();
    let start = ptr as usize;
    if len == 0 || start % page != 0 {
        return Err(AllocError::Unsupported(
            "the external range is empty or does not start on a page boundary",
        ));
    }
    // Mappings are page-granular, so rounding the end up to the page boundary stays inside the mapping that holds the
    // range's last byte. Every byte of [start, end) must currently be private writable memory.
    let end = (start + len + page - 1) & !(page - 1);
    match covered_until(&rw_private_mappings(), start) {
        Some(reach) if reach >= end => {}
        _ => {
            return Err(AllocError::Unsupported(
                "the external range is not entirely private writable memory",
            ))
        }
    }
    let map_len = end - start;

    // Everything above returned an error identically on every PE (same binary, same layout). From here on a
    // failure on one PE would leave the others waiting at a barrier, so failures abort.
    let fatal = |what: &str| -> ! {
        eprintln!(
            "lamellar shmem-opt: register_external: {what}: {}",
            std::io::Error::last_os_error()
        );
        std::process::abort()
    };
    let id = EXT_ID.fetch_add(1, Ordering::SeqCst);
    let name = |pe: usize| CString::new(format!("/lamellar_{job_id}_ext{id}_{pe}")).unwrap();
    let my_name = name(my_pe);
    let rw = libc::PROT_READ | libc::PROT_WRITE;

    let fd = unsafe { libc::shm_open(my_name.as_ptr(), libc::O_CREAT | libc::O_EXCL | libc::O_RDWR, 0o600) };
    if fd < 0 {
        fatal("shm_open");
    }
    if unsafe { libc::ftruncate(fd, map_len as libc::off_t) } != 0 {
        fatal("ftruncate");
    }
    let staging = unsafe { libc::mmap(std::ptr::null_mut(), map_len, rw, libc::MAP_SHARED, fd, 0) };
    if staging == libc::MAP_FAILED {
        fatal("mmap (staging)");
    }
    // Copy the live range into the shared file and map the file over it. Only locals are touched in between, because
    // anything written to the range after the copy would be lost.
    unsafe {
        std::ptr::copy_nonoverlapping(start as *const u8, staging as *mut u8, map_len);
        let r = libc::mmap(
            start as *mut libc::c_void,
            map_len,
            rw,
            libc::MAP_SHARED | libc::MAP_FIXED,
            fd,
            0,
        );
        if r == libc::MAP_FAILED {
            fatal("mmap over the external range (the original mapping is gone)");
        }
        libc::munmap(staging, map_len);
    }

    heap.barrier(); // every PE has created, filled and remapped its file

    let mut peer_base = vec![0usize; num_pes];
    let mut peer_maps = Vec::with_capacity(num_pes.saturating_sub(1));
    peer_base[my_pe] = start;
    for pe in (0..num_pes).filter(|pe| *pe != my_pe) {
        let pfd = unsafe { libc::shm_open(name(pe).as_ptr(), libc::O_RDWR, 0) };
        if pfd < 0 {
            fatal("shm_open (peer)");
        }
        let m = unsafe { libc::mmap(std::ptr::null_mut(), map_len, rw, libc::MAP_SHARED, pfd, 0) };
        if m == libc::MAP_FAILED {
            fatal("mmap (peer)");
        }
        unsafe { libc::close(pfd) };
        peer_base[pe] = m as usize;
        peer_maps.push((m as usize, map_len));
    }

    heap.barrier(); // every PE has mapped every other: the names can go
    unsafe {
        libc::shm_unlink(my_name.as_ptr());
        libc::close(fd);
    }

    Ok(ShmemOptExternAlloc {
        inner: Arc::new(ExternInner {
            my_pe,
            num_pes,
            len: map_len,
            peer_base,
            peer_maps,
        }),
    })
}

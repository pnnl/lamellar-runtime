use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use parking_lot::{Mutex, RwLock};
use tracing::{debug, trace};

use super::heap::{round_up, spin_until, ShmemOptHeap, LARGE_GRANULE, SMALL_GRANULE};
use super::mailbox::MailboxRelease;
use crate::{
    lamellae::{
        calc_alloc_padding_size_align, decode_padding, decode_ref_count, decrement_ref_count,
        encode_ref_count_and_padding, increment_ref_count, AllocError, AllocResult, CommAlloc,
        CommAllocAddr, CommAllocInner,
    },
    lamellar_alloc::{BTreeAlloc, LamellarAlloc},
};

// allocations keyed by the start of this PE's portion (local universal address)
type AllocMap = Arc<RwLock<BTreeMap<usize, ShmemOptAlloc>>>;

#[derive(Clone)]
enum AllocTable {
    Fabric(AllocMap),
    Runtime(BTreeAlloc, usize, AllocMap),
    // received-AM view over a mailbox slot or a peer's rt buffer; not refcounted in shared
    // memory, the release runs when the last view drops the Arc
    Mailbox(Arc<MailboxRelease>),
}

/// Per collective allocation state shared by all local views of it. Dropped when the last
/// local view drops, which releases this PE's claim on the symmetric offset.
struct SymMeta {
    heap: Arc<ShmemOptHeap>,
    pes: Vec<usize>,
    sym_off: usize,
    portion_len: usize,
    sync: Vec<usize>, // universal address of each alloc PE's collective sync block
    seen_all: AtomicUsize, // highest epoch this PE has seen every alloc PE arrive at
    rd_subs: usize,        // recursive-doubling subslots per parity (0: RD unused)
}

impl Drop for SymMeta {
    fn drop(&mut self) {
        trace!(target: "shmem", "releasing symmetric offset {:#x} (+{:#x})", self.sym_off, self.portion_len);
        self.heap
            .release_offset(self.sym_off, self.portion_len, &self.pes);
    }
}

pub(crate) struct ShmemOptAlloc {
    pub(crate) data: *mut u8,
    pub(crate) data_num_bytes: usize,
    pub(crate) base_ptr: *mut u8, // start of this PE's portion
    pub(crate) base_data_len: usize,
    pub(crate) base_len: usize,   // per-PE portion length
    pub(crate) my_alloc_pe: usize, //pe id relative to the pes associated with the alloc
    my_pe: usize,
    region_shift: u32,
    fabric_ref_cnt_offset: usize,
    rt_ref_cnt_offset: usize,
    meta: Arc<SymMeta>,
    alloc_table: AllocTable,
}
impl std::fmt::Debug for ShmemOptAlloc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fabric_ref_count = unsafe {
            (&*(self.base_ptr.add(self.fabric_ref_cnt_offset) as *const AtomicUsize))
                .load(Ordering::SeqCst)
        };

        let mut temp = f.debug_struct("ShmemOptAlloc");
        temp.field(
            "addr",
            &format_args!("{:?} - {:?}", self.data, unsafe {
                self.data.add(self.data_num_bytes)
            },),
        )
        .field(
            "base_ptr",
            &format_args!(
                "{:?} - ({:?}) {:?}",
                self.base_ptr,
                unsafe { self.base_ptr.add(self.base_data_len) },
                unsafe { self.base_ptr.add(self.base_len) }
            ),
        )
        .field("sym_off", &format_args!("{:#x}", self.meta.sym_off))
        .field("data_num_bytes", &self.data_num_bytes)
        .field("my_pe", &self.my_alloc_pe)
        .field("num_pes", &self.num_pes())
        .field(
            "fabric_ref_cnt_offset",
            &format_args!(
                "{} ({:?}): {}",
                self.fabric_ref_cnt_offset,
                unsafe { self.base_ptr.add(self.fabric_ref_cnt_offset) as *const AtomicUsize },
                fabric_ref_count
            ),
        );
        if let AllocTable::Runtime(_, _, _) = &self.alloc_table {
            let rt_ref_count = unsafe {
                (&*(self.base_ptr.add(self.rt_ref_cnt_offset) as *const AtomicUsize))
                    .load(Ordering::SeqCst)
            };
            let padding = decode_padding(rt_ref_count);
            let rt_ref_count = decode_ref_count(rt_ref_count);
            temp.field(
                "rt_ref_cnt_offset",
                &format_args!(
                    "{} ({:?}): {}, {}",
                    self.rt_ref_cnt_offset,
                    unsafe { self.base_ptr.add(self.rt_ref_cnt_offset) as *const AtomicUsize },
                    rt_ref_count,
                    padding,
                ),
            );
        }
        temp.finish()
    }
}

impl Clone for ShmemOptAlloc {
    fn clone(&self) -> Self {
        self.retain();
        let alloc = Self {
            data: self.data,
            data_num_bytes: self.data_num_bytes,
            base_ptr: self.base_ptr,
            base_data_len: self.base_data_len,
            base_len: self.base_len,
            my_alloc_pe: self.my_alloc_pe,
            my_pe: self.my_pe,
            region_shift: self.region_shift,
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: self.rt_ref_cnt_offset,
            meta: self.meta.clone(),
            alloc_table: self.alloc_table.clone(),
        };
        debug!(target: "shmem", "Cloned ShmemOpt allocation: {:?}", alloc);
        alloc
    }
}

unsafe impl Sync for ShmemOptAlloc {}
unsafe impl Send for ShmemOptAlloc {}

impl ShmemOptAlloc {
    fn new(
        data: *mut u8,
        data_num_bytes: usize,
        padding: usize,
        base_len: usize,
        my_alloc_pe: usize, //pe id relative to the pes associated with the alloc
        my_pe: usize,
        meta: SymMeta,
        alloc_table: AllocMap,
    ) -> AllocResult<Self> {
        let ref_cnt_offset = data_num_bytes + padding;
        let encoded = encode_ref_count_and_padding(1, padding);
        let alloc = Self {
            data,
            data_num_bytes,
            base_ptr: data,
            base_data_len: data_num_bytes,
            base_len,
            my_alloc_pe,
            my_pe,
            region_shift: meta.heap.region_shift(),
            fabric_ref_cnt_offset: ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            meta: Arc::new(meta),
            alloc_table: AllocTable::Fabric(alloc_table),
        };
        unsafe {
            (&*(alloc.base_ptr.add(ref_cnt_offset) as *mut AtomicUsize))
                .store(encoded, Ordering::SeqCst);
        }
        debug!(target: "shmem", "Created ShmemOpt allocation: {:?}", alloc);
        Ok(alloc)
    }

    #[inline(always)]
    pub(crate) fn num_pes(&self) -> usize {
        self.meta.pes.len()
    }

    #[allow(dead_code)]
    pub(crate) unsafe fn as_mut_slice<T: Copy>(&self) -> &mut [T] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.start() as *mut T,
                self.num_bytes() / std::mem::size_of::<T>(),
            )
        }
    }

    #[allow(dead_code)]
    pub(crate) unsafe fn as_slice<T: Copy>(&self) -> &[T] {
        unsafe {
            std::slice::from_raw_parts(
                self.start() as *const T,
                self.num_bytes() / std::mem::size_of::<T>(),
            )
        }
    }

    #[inline(always)]
    pub(crate) fn start(&self) -> usize {
        self.data as usize
    }

    #[inline(always)]
    pub(crate) fn num_bytes(&self) -> usize {
        self.data_num_bytes
    }

    // one more reference for a clone or sub-allocation of this allocation
    #[inline(always)]
    fn retain(&self) {
        match &self.alloc_table {
            AllocTable::Fabric(_) => {
                self.increment_fabric_ref_count();
            }
            AllocTable::Runtime(_, _, _) => {
                self.increment_fabric_ref_count();
                self.increment_rt_ref_count();
            }
            AllocTable::Mailbox(_) => {}
        }
    }

    /// View of `len` bytes at `data` (a mailbox slot, or a peer's rt buffer at its universal
    /// address) for a received AM. `release` runs once the last view is dropped.
    pub(crate) fn mailbox_view(&self, data: *mut u8, len: usize, release: MailboxRelease) -> Self {
        self.derive(
            data,
            len,
            self.rt_ref_cnt_offset,
            AllocTable::Mailbox(Arc::new(release)),
        )
    }

    fn derive(&self, data: *mut u8, data_num_bytes: usize, rt_ref_cnt_offset: usize, alloc_table: AllocTable) -> Self {
        ShmemOptAlloc {
            data,
            data_num_bytes,
            base_ptr: self.base_ptr,
            base_data_len: self.base_data_len,
            base_len: self.base_len,
            my_alloc_pe: self.my_alloc_pe,
            my_pe: self.my_pe,
            region_shift: self.region_shift,
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset,
            meta: self.meta.clone(),
            alloc_table,
        }
    }

    pub(crate) fn sub_alloc(&self, offset: usize, len: usize) -> AllocResult<ShmemOptAlloc> {
        if offset + len > self.data_num_bytes {
            return Err(AllocError::InvalidSubAlloc(offset, len));
        }
        let new_data = unsafe { self.data.add(offset) };
        self.retain();
        //keep the same ref count offset as the parent allocation if this is actually a rt alloc, it will be updated when converted to a rt_alloc
        let alloc = self.derive(new_data, len, self.rt_ref_cnt_offset, self.alloc_table.clone());
        debug!(target: "shmem", "Created ShmemOpt sub-allocation: {:?}", alloc);
        Ok(alloc)
    }

    //we call this function to create a sub-allocation that is tracked as part of a runtime allocation
    pub(crate) fn rt_alloc(
        &self,
        alloc_table: BTreeAlloc,
        offset: usize,
        padding: usize,
        len: usize,
    ) -> AllocResult<Self> {
        if offset + len > self.data_num_bytes {
            return Err(AllocError::InvalidSubAlloc(offset, len));
        }
        let new_data_bytes = len - padding - std::mem::size_of::<AtomicUsize>();
        let new_data = unsafe { self.data.add(offset) };
        let allocs = match &self.alloc_table {
            AllocTable::Fabric(at) => at.clone(),
            AllocTable::Runtime(_, _, at) => at.clone(),
            AllocTable::Mailbox(_) => panic!("mailbox views cannot back rt allocations"),
        };
        self.increment_fabric_ref_count();
        // ref count location is relative to base_ptr
        let ref_cnt_offset =
            (new_data as usize - self.base_ptr as usize) + new_data_bytes + padding;
        let encoded = encode_ref_count_and_padding(1, padding);

        let alloc = self.derive(
            new_data,
            new_data_bytes,
            ref_cnt_offset,
            AllocTable::Runtime(alloc_table, new_data as usize, allocs),
        );
        unsafe {
            (&*(alloc.base_ptr.add(alloc.rt_ref_cnt_offset) as *mut AtomicUsize))
                .store(encoded, Ordering::SeqCst);
        }
        debug!(target: "shmem", "Created ShmemOpt rt-allocation: {:?}", alloc);
        Ok(alloc)
    }

    // This function is used to construct an rt_alloc from a raw sub-allocation
    // typically paired with a call to leak() we decrement the ref count as this instance recaptures the leaked instance
    pub(crate) fn as_rt_alloc(self, alloc_table: BTreeAlloc) -> AllocResult<Self> {
        let allocs = match &self.alloc_table {
            AllocTable::Fabric(allocs) => allocs.clone(),
            AllocTable::Runtime(_, _, allocs) => allocs.clone(),
            AllocTable::Mailbox(_) => panic!("mailbox views cannot back rt allocations"),
        };
        let ref_cnt_offset = ((self.start() - self.base_ptr as usize) + self.num_bytes())
            - std::mem::size_of::<AtomicUsize>();
        let encoded_ref_count = unsafe {
            (&*(self.base_ptr.add(ref_cnt_offset) as *const AtomicUsize)).load(Ordering::SeqCst)
        };
        let padding = decode_padding(encoded_ref_count);

        // the new instance takes over the leaked refs; dropping `self` releases the one it held
        let alloc = self.derive(
            self.data,
            self.data_num_bytes - padding - std::mem::size_of::<AtomicUsize>(),
            ref_cnt_offset,
            AllocTable::Runtime(alloc_table, self.data as usize, allocs),
        );
        debug!(target: "shmem", "Converted ShmemOpt alloc to rt-alloc: {:?}", alloc);
        Ok(alloc)
    }

    pub(crate) fn leak(self) -> Option<CommAllocAddr> {
        match self.alloc_table {
            AllocTable::Fabric(_) | AllocTable::Mailbox(_) => None, //only rt_allocs can be leaked
            AllocTable::Runtime(_, _, _) => {
                self.increment_fabric_ref_count(); //increment the ref count to account for the leaked instance
                self.increment_rt_ref_count(); //increment the ref count to account for the leaked instance
                debug!(target: "shmem", "Leaked ShmemOpt rt-allocation: {:?}", self);
                Some(CommAllocAddr(self.start()))
            }
        }
    }

    pub(crate) fn increment_fabric_ref_count(&self) -> usize {
        let ref_count =
            unsafe { &*(self.base_ptr.add(self.fabric_ref_cnt_offset) as *const AtomicUsize) };
        increment_ref_count(ref_count)
    }

    pub(crate) fn decrement_fabric_ref_count(&self) -> usize {
        let ref_count =
            unsafe { &*(self.base_ptr.add(self.fabric_ref_cnt_offset) as *const AtomicUsize) };
        decrement_ref_count(ref_count)
    }

    pub(crate) fn increment_rt_ref_count(&self) -> usize {
        let ref_count =
            unsafe { &*(self.base_ptr.add(self.rt_ref_cnt_offset) as *const AtomicUsize) };
        increment_ref_count(ref_count)
    }
    pub(crate) fn decrement_rt_ref_count(&self) -> usize {
        let ref_count =
            unsafe { &*(self.base_ptr.add(self.rt_ref_cnt_offset) as *const AtomicUsize) };
        decrement_ref_count(ref_count)
    }

    /// Universal address of this allocation's data on (world) PE `pe`.
    #[inline(always)]
    pub(crate) fn pe_base_offset(&self, pe: usize) -> usize {
        self.data as usize - (self.my_pe << self.region_shift) + (pe << self.region_shift)
    }

    /// Universal address of this allocation's data on the `idx`-th PE of the allocation.
    #[inline(always)]
    pub(crate) fn pe_base_offset_rel(&self, idx: usize) -> usize {
        self.pe_base_offset(self.meta.pes[idx])
    }

    pub(crate) unsafe fn zeroize_bytes(&self) {
        let u8_slice = std::slice::from_raw_parts_mut(self.data, self.data_num_bytes);
        u8_slice.fill(0);
    }

    pub(crate) fn wait(&self) {
        //shmem is always ready
    }

    // Collective sync blocks (see `SYNC_LEN`). Each PE writes only its own block; peers only
    // read it. Every PE calls `coll_arrive` exactly once per collective on this allocation,
    // so the epochs line up across PEs and never need a reset.
    #[inline(always)]
    fn sync_word(&self, idx: usize, word: usize) -> &AtomicUsize {
        unsafe { &*((self.meta.sync[idx] + word * 8) as *const AtomicUsize) }
    }

    /// Publishes this PE's element `index` for the next collective and marks it arrived.
    /// Returns the op's epoch.
    #[inline]
    pub(crate) fn coll_arrive(&self, index: usize) -> usize {
        let me = self.my_alloc_pe;
        let epoch = self.sync_word(me, SYNC_ARRIVE).load(Ordering::Relaxed) + 1;
        self.sync_word(me, SYNC_INDEX).store(index, Ordering::Relaxed);
        self.sync_word(me, SYNC_ARRIVE).store(epoch, Ordering::Release);
        epoch
    }

    /// Marks this PE as finished reading peers' data for `epoch`.
    #[inline]
    pub(crate) fn coll_done(&self, epoch: usize) {
        self.sync_word(self.my_alloc_pe, SYNC_DONE)
            .store(epoch, Ordering::Release);
    }

    #[inline]
    pub(crate) fn coll_wait_arrive(&self, idx: usize, epoch: usize) {
        let w = self.sync_word(idx, SYNC_ARRIVE);
        spin_until(|| w.load(Ordering::Acquire) >= epoch);
    }

    #[inline]
    pub(crate) fn coll_wait_done(&self, idx: usize, epoch: usize) {
        let w = self.sync_word(idx, SYNC_DONE);
        spin_until(|| w.load(Ordering::Acquire) >= epoch);
    }

    pub(crate) fn coll_wait_all_arrive(&self, epoch: usize) {
        if self.meta.seen_all.load(Ordering::Relaxed) >= epoch {
            return;
        }
        for idx in 0..self.num_pes() {
            self.coll_wait_arrive(idx, epoch);
        }
        self.meta.seen_all.fetch_max(epoch, Ordering::Relaxed);
    }

    #[inline(always)]
    fn eager_slot(&self, idx: usize, epoch: usize) -> usize {
        self.meta.sync[idx] + SYNC_LEN + (epoch & 1) * EAGER_SLOT
    }

    /// Eager arrive: copies `src` (at most `COLL_EAGER_BYTES`) into this PE's scratch slot for
    /// the op's epoch, then marks it arrived. Returns the epoch.
    ///
    /// The slot was last used two epochs ago. Every PE arrives at an epoch only after it has
    /// returned from the previous op, so once all PEs have arrived at `epoch - 1` nobody still
    /// reads the slot and it can be overwritten.
    #[inline]
    pub(crate) fn coll_eager_arrive(&self, src: &[u8]) -> usize {
        debug_assert!(src.len() <= EAGER_SLOT);
        let me = self.my_alloc_pe;
        let epoch = self.sync_word(me, SYNC_ARRIVE).load(Ordering::Relaxed) + 1;
        if !src.is_empty() {
            if epoch > 1 {
                self.coll_wait_all_arrive(epoch - 1);
            }
            unsafe {
                std::ptr::copy_nonoverlapping(src.as_ptr(), self.eager_slot(me, epoch) as *mut u8, src.len());
            }
        }
        self.sync_word(me, SYNC_ARRIVE).store(epoch, Ordering::Release);
        epoch
    }

    /// The scratch slot the `idx`-th alloc PE filled for `epoch`. Valid after
    /// `coll_wait_arrive(idx, epoch)`.
    #[inline]
    pub(crate) fn coll_eager_peer<T>(&self, idx: usize, epoch: usize) -> *const T {
        self.eager_slot(idx, epoch) as *const T
    }

    #[inline(always)]
    fn rd_sub(&self, idx: usize, epoch: usize, k: usize) -> usize {
        self.meta.sync[idx] + SYNC_LEN + 2 * EAGER_SLOT + ((epoch & 1) * self.meta.rd_subs + k) * RD_SUB
    }

    /// Whether this allocation has recursive-doubling scratch.
    #[inline(always)]
    pub(crate) fn coll_rd_avail(&self) -> bool {
        self.meta.rd_subs > 0
    }

    /// Subslot for the final copy back to PEs past the largest power of two.
    #[inline(always)]
    pub(crate) fn coll_rd_post(&self) -> usize {
        self.meta.rd_subs - 1
    }

    /// Starts a recursive-doubling op and returns its epoch. This epoch's subslots were last
    /// read two epochs ago, and every PE having arrived at `epoch - 1` means that read is over.
    #[inline]
    pub(crate) fn coll_rd_begin(&self) -> usize {
        let w = self.sync_word(self.my_alloc_pe, SYNC_ARRIVE);
        let epoch = w.load(Ordering::Relaxed) + 1;
        if epoch > 1 {
            self.coll_wait_all_arrive(epoch - 1);
        }
        w.store(epoch, Ordering::Release);
        epoch
    }

    /// Publishes `src` (at most `COLL_RD_BYTES`) in this PE's subslot `k` for `epoch`.
    #[inline]
    pub(crate) fn coll_rd_put(&self, epoch: usize, k: usize, src: &[u8]) {
        debug_assert!(src.len() <= COLL_RD_BYTES && k < self.meta.rd_subs);
        let sub = self.rd_sub(self.my_alloc_pe, epoch, k);
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), (sub + RD_HDR) as *mut u8, src.len());
            (*(sub as *const AtomicUsize)).store(epoch, Ordering::Release);
        }
    }

    /// Waits for the `idx`-th alloc PE to publish subslot `k` for `epoch` and returns its data.
    #[inline]
    pub(crate) fn coll_rd_get<T>(&self, idx: usize, epoch: usize, k: usize) -> *const T {
        let sub = self.rd_sub(idx, epoch, k);
        let f = unsafe { &*(sub as *const AtomicUsize) };
        spin_until(|| f.load(Ordering::Acquire) >= epoch);
        (sub + RD_HDR) as *const T
    }

    /// Ends a recursive-doubling op. The result depends on every PE's data, so every PE had
    /// arrived at `epoch`.
    #[inline]
    pub(crate) fn coll_rd_end(&self, epoch: usize) {
        self.meta.seen_all.fetch_max(epoch, Ordering::Relaxed);
    }

    /// Where world PE `pe`'s portion holds the byte at local address `addr` of this PE's
    /// portion, if `pe` is an alloc PE. Portions are laid out alike and every PE maps them all.
    pub(crate) fn peer_copy_addr(&self, pe: usize, addr: usize) -> Option<usize> {
        let idx = self.meta.pes.iter().position(|p| *p == pe)?;
        Some(
            addr.wrapping_add(self.meta.sync[idx])
                .wrapping_sub(self.meta.sync[self.my_alloc_pe]),
        )
    }

    pub(crate) fn coll_wait_all_done(&self, epoch: usize) {
        for idx in 0..self.num_pes() {
            self.coll_wait_done(idx, epoch);
        }
    }

    /// Universal address of element `index` of this allocation on the `idx`-th alloc PE,
    /// where `index` is what that PE published for the current collective. Valid after
    /// `coll_wait_arrive(idx, ..)`.
    #[inline]
    pub(crate) fn coll_peer_addr<T>(&self, idx: usize) -> usize {
        self.pe_base_offset_rel(idx)
            + self.sync_word(idx, SYNC_INDEX).load(Ordering::Relaxed) * std::mem::size_of::<T>()
    }
}

fn remove_from_table(alloc: &ShmemOptAlloc, allocs: &AllocMap) {
    let removed = allocs.write().remove(&(alloc.base_ptr as usize));
    if removed.is_none() {
        panic!("failed to free alloc: {:?}", alloc);
    }
    // dropped outside the table lock
    drop(removed);
}

impl Drop for ShmemOptAlloc {
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop ShmemOptAlloc");
        if let AllocTable::Mailbox(_) = &self.alloc_table {
            return;
        }
        let fabric_ref_count = self.decrement_fabric_ref_count();
        debug!(target: "shmem", "Dropping ShmemOptAlloc: {:?}" , self);
        match &self.alloc_table {
            AllocTable::Fabric(allocs) => {
                if fabric_ref_count == 2 {
                    debug!(target: "shmem", "Dropping fabric ShmemOptAlloc: {:?}", self);
                    remove_from_table(self, allocs);
                }
            }
            AllocTable::Runtime(rt_alloc_table, addr, allocs) => {
                let rt_ref_count = self.decrement_rt_ref_count();
                if rt_ref_count == 1 {
                    debug!(target: "shmem", "Dropping rt ShmemOptAlloc: {:?}", self);
                    rt_alloc_table.free(*addr).expect(&format!(
                        "[{:?}] Error removing from runtime alloc table {:x}",
                        std::thread::current().id(),
                        addr
                    ));
                }
                if fabric_ref_count == 2 {
                    debug!(target: "shmem", "Dropping fabric ShmemOptAlloc: {:?}", self);
                    remove_from_table(self, allocs);
                }
            }
            AllocTable::Mailbox(_) => unreachable!(),
        }
        trace!(target: "drop", "end drop ShmemOptAlloc");
    }
}

impl From<ShmemOptAlloc> for CommAlloc {
    fn from(alloc: ShmemOptAlloc) -> Self {
        CommAlloc {
            inner_alloc: Arc::new(CommAllocInner::ShmemOptAlloc(alloc)),
            // alloc_type: CommAllocType::Fabric,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OneSidedShmemOptAlloc {
    pub(crate) data: *mut u8, // this is the actual address to the data on the remote PE (since we are in shared memory this is directly accessible)
    pub(crate) data_num_bytes: usize,
    pub(crate) remote_pe: usize,
    // keeps the backing allocation's ref count bumped for the lifetime of this
    // one-sided view, so it can't be freed while a get/put against `data` is in flight
    pub(crate) alloc: ShmemOptAlloc,
}

//safety is managed via higher level abstractions or marked unsafe
unsafe impl Sync for OneSidedShmemOptAlloc {}
unsafe impl Send for OneSidedShmemOptAlloc {}

impl OneSidedShmemOptAlloc {
    pub(crate) fn num_bytes(&self) -> usize {
        self.data_num_bytes
    }
    pub(crate) fn start(&self) -> usize {
        self.data as usize
    }
    pub(crate) fn sub_alloc(&self, offset: usize, len: usize) -> AllocResult<OneSidedShmemOptAlloc> {
        if offset + len > self.data_num_bytes {
            return Err(AllocError::InvalidSubAlloc(offset, len));
        }
        let new_data = unsafe { self.data.add(offset) };
        let alloc = OneSidedShmemOptAlloc {
            data: new_data,
            data_num_bytes: len,
            remote_pe: self.remote_pe,
            alloc: self.alloc.clone(),
        };
        Ok(alloc)
    }
    pub(crate) fn wait(&self) {
        //shmem is always ready
    }
}

impl From<OneSidedShmemOptAlloc> for CommAlloc {
    fn from(alloc: OneSidedShmemOptAlloc) -> Self {
        CommAlloc {
            inner_alloc: Arc::new(CommAllocInner::OneSidedShmemOptAlloc(alloc)),
            // alloc_type: CommAllocType::Remote,
        }
    }
}

// Per-PE collective sync block at the end of each portion: line 0 holds the arrive epoch and
// the published element index (written together, read together), line 1 the done epoch, so
// arrive polling and done polling don't share a line.
const SYNC_LEN: usize = 128;
const SYNC_ARRIVE: usize = 0;
const SYNC_INDEX: usize = 1;
const SYNC_DONE: usize = 8;
// Two parity scratch slots follow the sync block, for small collectives that copy their input
// instead of exposing it (one sync per op instead of arrive + done).
const EAGER_SLOT: usize = 2048;
pub(crate) const COLL_EAGER_BYTES: usize = EAGER_SLOT;
// Recursive-doubling scratch after the eager slots: per parity, `rd_subs` subslots, each a flag
// epoch followed by the data (the first bytes share the flag's line). Kept apart from the eager
// slots, whose data would otherwise land on the flags. Subslot 0 is the fold-in from PEs past
// the largest power of two, 1..=log2 are the rounds, and the last is the copy back to them.
const RD_SUB: usize = 576;
const RD_HDR: usize = 16;
pub(crate) const COLL_RD_BYTES: usize = RD_SUB - RD_HDR;
/// Below this a flat eager read of every peer is as fast as log2(P) dependent rounds.
pub(crate) const COLL_RD_MIN_PES: usize = 4;

fn rd_subs(num_pes: usize) -> usize {
    if num_pes >= COLL_RD_MIN_PES {
        num_pes.ilog2() as usize + 2
    } else {
        0
    }
}

pub(crate) struct ShmemOptAllocator {
    heap: Arc<ShmemOptHeap>,
    // collective allocs from one process are serialized so the leader->member handoffs
    // are consumed in call order
    alloc_lock: Mutex<()>,
    my_pe: usize,
    num_pes: usize,
    job_id: usize,
    allocs: AllocMap,
}

unsafe impl Sync for ShmemOptAllocator {}
unsafe impl Send for ShmemOptAllocator {}

impl std::fmt::Debug for ShmemOptAllocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShmemOptAllocator")
            .field("my_pe", &self.my_pe)
            .field("num_pes", &self.num_pes)
            .field("job_id", &self.job_id)
            .field("heap", &self.heap)
            .finish()
    }
}

impl ShmemOptAllocator {
    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new(num_pes: usize, pe: usize, job_id: usize) -> Self {
        ShmemOptAllocator {
            heap: Arc::new(ShmemOptHeap::new(num_pes, pe, job_id)),
            alloc_lock: Mutex::new(()),
            my_pe: pe,
            num_pes: num_pes,
            job_id: job_id,
            allocs: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub(crate) fn heap(&self) -> &Arc<ShmemOptHeap> {
        &self.heap
    }

    pub(crate) unsafe fn barrier(&self) {
        self.heap.barrier();
    }

    /// Collective over `pes`. The returned memory is zero: fresh sparse pages, or a range
    /// whose previous owner hole-punched every portion before recycling it.
    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) unsafe fn alloc(&self, data_size: usize, align: usize, pes: &[usize]) -> ShmemOptAlloc {
        let my_idx = pes
            .iter()
            .position(|pe| *pe == self.my_pe)
            .expect("pe not in sub alloc list");
        let (padding, size, align) = calc_alloc_padding_size_align(data_size, align);

        let meta_off = round_up(size, 64);
        let portion = meta_off + SYNC_LEN + 2 * EAGER_SLOT + 2 * rd_subs(pes.len()) * RD_SUB;
        let granule = if portion >= LARGE_GRANULE {
            LARGE_GRANULE
        } else {
            SMALL_GRANULE
        };
        let granule = std::cmp::max(granule, align.next_power_of_two());
        let portion = round_up(portion, granule);

        let sym_off = {
            let _guard = self.alloc_lock.lock();
            self.heap.collective_offset(pes, my_idx, portion, granule)
        };
        let my_base = self.heap.region_base(self.my_pe) + sym_off;
        let meta = SymMeta {
            heap: self.heap.clone(),
            pes: pes.to_vec(),
            sym_off,
            portion_len: portion,
            sync: pes
                .iter()
                .map(|pe| self.heap.region_base(*pe) + sym_off + meta_off)
                .collect(),
            seen_all: AtomicUsize::new(0),
            rd_subs: rd_subs(pes.len()),
        };

        let alloc = ShmemOptAlloc::new(
            my_base as *mut u8,
            data_size,
            padding,
            portion,
            my_idx,
            self.my_pe,
            meta,
            self.allocs.clone(),
        )
        .expect("failed to create shmem alloc");
        self.allocs.write().insert(my_base, alloc.clone());
        trace!(target: "shmem", "new collective alloc {:?}", alloc);
        alloc
    }

    // allocation containing local address `addr`
    fn find(&self, addr: usize) -> Option<ShmemOptAlloc> {
        let allocs = self.allocs.read();
        let (_, alloc) = allocs.range(..=addr).next_back()?;
        if addr < alloc.start() + alloc.num_bytes() {
            Some(alloc.clone())
        } else {
            None
        }
    }

    #[inline(always)]
    fn to_local(&self, remote_pe: usize, remote_addr: usize) -> usize {
        debug_assert_eq!(self.heap.pe_of(remote_addr), Some(remote_pe));
        self.heap.translate(remote_addr, remote_pe, self.my_pe)
    }

    pub(crate) fn get_alloc_from_start_addr(
        &self,
        mem_addr: CommAllocAddr,
    ) -> AllocResult<ShmemOptAlloc> {
        self.allocs
            .read()
            .get(&mem_addr.0)
            .cloned()
            .ok_or(AllocError::LocalNotFound(mem_addr))
    }

    #[inline(always)]
    pub(crate) fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> Option<usize> {
        Some(self.to_local(remote_pe, remote_addr))
    }

    pub(crate) fn one_sided_alloc_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
        num_bytes: usize,
    ) -> CommAlloc {
        let local = self.to_local(remote_pe, remote_addr);
        match self.find(local) {
            Some(alloc) => OneSidedShmemOptAlloc {
                // universal VA: the remote address is directly usable
                data: remote_addr as *mut u8,
                data_num_bytes: num_bytes,
                remote_pe,
                alloc,
            }
            .into(),
            None => panic!(
                "failed to find remote addr {:x} on pe {}",
                remote_addr, remote_pe
            ),
        }
    }

    pub(crate) fn local_alloc_and_offset_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
    ) -> Option<(CommAlloc, usize)> {
        let local = self.to_local(remote_pe, remote_addr);
        let alloc = self.find(local)?;
        let offset = local - alloc.start();
        Some((alloc.into(), offset))
    }

    #[inline(always)]
    pub(crate) fn remote_addr(&self, remote_pe: usize, local_addr: usize) -> Option<usize> {
        debug_assert_eq!(self.heap.pe_of(local_addr), Some(self.my_pe));
        Some(self.heap.translate(local_addr, self.my_pe, remote_pe))
    }
}

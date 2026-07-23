use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicIsize, AtomicUsize, Ordering},
        Arc,
    },
};

use parking_lot::RwLock;
use shared_memory::*;
use tracing::{debug, trace};

use crate::{
    lamellae::{
        calc_alloc_padding_size_align, decode_padding, decode_ref_count, decrement_ref_count,
        encode_ref_count_and_padding, increment_ref_count, AllocError, AllocResult, CommAlloc,
        CommAllocAddr, CommAllocInner,
    },
    lamellar_alloc::{BTreeAlloc, LamellarAlloc},
};

pub(crate) struct ShmemHandle {
    base_addr: *mut u8,
    _shmem: Shmem,
}

impl ShmemHandle {
    pub(crate) fn base_ptr(&self) -> *mut u8 {
        self.base_addr
    }
}

#[derive(Clone)]
enum AllocTable {
    Fabric(Arc<RwLock<Vec<ShmemAlloc>>>),
    Runtime(BTreeAlloc, usize, Arc<RwLock<Vec<ShmemAlloc>>>),
}

pub(crate) struct ShmemAlloc {
    pub(crate) data: *mut u8,
    pub(crate) data_num_bytes: usize,
    pub(crate) base_ptr: *mut u8,
    pub(crate) base_data_len: usize,
    pub(crate) base_len: usize,
    pub(crate) global_base_ptr: *mut u8,
    pub(crate) global_base_len: usize,
    pub(crate) my_alloc_pe: usize, //pe id relative to the pes associated with the alloc
    fabric_ref_cnt_offset: usize,
    rt_ref_cnt_offset: usize,
    coll_meta_offset: usize, // offset (relative to shmem.base_ptr()) of this allocation's collective barrier/index registers
    pe_map: Arc<HashMap<usize, usize>>,
    remote_addrs: Arc<Vec<usize>>,
    alloc_table: AllocTable,
    shmem: Arc<ShmemHandle>,
}
impl std::fmt::Debug for ShmemAlloc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fabric_ref_count = unsafe {
            (&*(self.base_ptr.add(self.fabric_ref_cnt_offset) as *const AtomicUsize))
                .load(Ordering::SeqCst)
        };

        let mut temp = f.debug_struct("ShmemAlloc");
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
        .field(
            "global_base_ptr",
            &format_args!("{:?} - {:?}", self.global_base_ptr, unsafe {
                self.global_base_ptr.add(self.global_base_len)
            }),
        )
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

impl Clone for ShmemAlloc {
    fn clone(&self) -> Self {
        self.increment_fabric_ref_count();
        if let AllocTable::Runtime(_, _, _) = &self.alloc_table {
            self.increment_rt_ref_count();
        }
        let alloc = Self {
            data: self.data,
            data_num_bytes: self.data_num_bytes,
            base_ptr: self.base_ptr,
            base_data_len: self.base_data_len,
            base_len: self.base_len,
            global_base_ptr: self.global_base_ptr,
            global_base_len: self.global_base_len,
            my_alloc_pe: self.my_alloc_pe,
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: self.rt_ref_cnt_offset,
            coll_meta_offset: self.coll_meta_offset,
            pe_map: self.pe_map.clone(),
            remote_addrs: self.remote_addrs.clone(),
            alloc_table: self.alloc_table.clone(),
            shmem: self.shmem.clone(),
        };
        debug!(target: "shmem", "Cloned Shmem allocation: {:?}", alloc);
        alloc
    }
}

unsafe impl Sync for ShmemAlloc {}
unsafe impl Send for ShmemAlloc {}

impl ShmemAlloc {
    pub(crate) fn new(
        data: *mut u8,
        data_num_bytes: usize,
        padding: usize,
        base_ptr: *mut u8,
        base_data_len: usize,
        base_len: usize,
        global_base_ptr: *mut u8,
        global_base_len: usize,
        my_alloc_pe: usize, //pe id relative to the pes associated with the alloc
        pe_map: HashMap<usize, usize>,
        remote_addrs: Vec<usize>,
        alloc_table: Arc<RwLock<Vec<ShmemAlloc>>>,
        shmem: Arc<ShmemHandle>,
        coll_meta_offset: usize,
    ) -> AllocResult<Self> {
        let ref_cnt_offset = data_num_bytes + padding;
        let encoded = encode_ref_count_and_padding(1, padding);
        let alloc = Self {
            data,
            data_num_bytes,
            base_ptr,
            base_data_len,
            base_len,
            global_base_ptr,
            global_base_len,
            my_alloc_pe,
            fabric_ref_cnt_offset: ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            coll_meta_offset,
            pe_map: Arc::new(pe_map),
            remote_addrs: Arc::new(remote_addrs),
            alloc_table: AllocTable::Fabric(alloc_table),
            shmem,
        };
        unsafe {
            (&*(alloc.base_ptr.add(ref_cnt_offset) as *mut AtomicUsize))
                .store(encoded, Ordering::SeqCst);
        }
        debug!(target: "shmem", "Created Shmem allocation: {:?}", alloc);
        Ok(alloc)
    }
    pub(crate) fn num_pes(&self) -> usize {
        self.pe_map.len()
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

    pub(crate) fn start(&self) -> usize {
        self.data as usize
    }

    pub(crate) fn num_bytes(&self) -> usize {
        self.data_num_bytes
    }
    pub(crate) fn sub_alloc(&self, offset: usize, len: usize) -> AllocResult<ShmemAlloc> {
        if offset + len > self.data_num_bytes {
            return Err(AllocError::InvalidSubAlloc(offset, len));
        }
        let new_data = unsafe { self.data.add(offset) };

        let mut remote_addrs = Vec::with_capacity(self.remote_addrs.len());
        for addr in self.remote_addrs.iter() {
            remote_addrs.push(addr + offset);
        }
        self.increment_fabric_ref_count();
        if let AllocTable::Runtime(_, _, _) = &self.alloc_table {
            self.increment_rt_ref_count();
        }
        let alloc = ShmemAlloc {
            data: new_data,
            data_num_bytes: len,
            base_ptr: self.base_ptr,
            base_data_len: self.base_data_len,
            base_len: self.base_len,
            global_base_ptr: self.global_base_ptr,
            global_base_len: self.global_base_len,
            my_alloc_pe: self.my_alloc_pe,
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: self.rt_ref_cnt_offset, //keep the same ref count offset as the parent allocation if this is actually a rt alloc, it will be updated when converted to a rt_alloc
            coll_meta_offset: self.coll_meta_offset,
            pe_map: self.pe_map.clone(),
            remote_addrs: Arc::new(remote_addrs),
            alloc_table: self.alloc_table.clone(),
            shmem: self.shmem.clone(),
        };
        debug!(target: "shmem", "Created Shmem sub-allocation: {:?}", alloc);
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

        let mut remote_addrs = Vec::with_capacity(self.remote_addrs.len());
        for addr in self.remote_addrs.iter() {
            remote_addrs.push(addr + offset);
        }
        self.increment_fabric_ref_count();
        let ref_cnt_offset = offset + new_data_bytes + padding;
        let encoded = encode_ref_count_and_padding(1, padding);

        let allocs = match &self.alloc_table {
            AllocTable::Fabric(at) => at.clone(),
            AllocTable::Runtime(_, _, at) => at.clone(),
        };

        let alloc = ShmemAlloc {
            data: new_data,
            data_num_bytes: new_data_bytes,
            base_ptr: self.base_ptr,
            base_data_len: self.base_data_len,
            base_len: self.base_len,
            global_base_ptr: self.global_base_ptr,
            global_base_len: self.global_base_len,
            my_alloc_pe: self.my_alloc_pe,
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset, //keep the same ref count offset as the parent allocation if this is actually a rt alloc, it will be updated when converted to a rt_alloc
            coll_meta_offset: self.coll_meta_offset,
            pe_map: self.pe_map.clone(),
            remote_addrs: Arc::new(remote_addrs),
            alloc_table: AllocTable::Runtime(alloc_table, new_data as usize, allocs),
            shmem: self.shmem.clone(),
        };
        unsafe {
            (&*(alloc.base_ptr.add(alloc.rt_ref_cnt_offset) as *mut AtomicUsize))
                .store(encoded, Ordering::SeqCst);
        }
        debug!(target: "shmem", "Created Shmem rt-allocation: {:?}", alloc);
        Ok(alloc)
    }

    // This function is used to construct an rt_alloc from a raw sub-allocation
    // typically paired with a call to leak() we decrement the ref count as this instance recaptures the leaked instance
    pub(crate) fn as_rt_alloc(self, alloc_table: BTreeAlloc) -> AllocResult<Self> {
        let allocs = match &self.alloc_table {
            AllocTable::Fabric(allocs) => allocs.clone(),
            AllocTable::Runtime(_, _, allocs) => allocs.clone(),
        };
        let ref_cnt_offset = ((self.start() - self.base_ptr as usize) + self.num_bytes())
            - std::mem::size_of::<AtomicUsize>();
        let encoded_ref_count = unsafe {
            (&*(self.base_ptr.add(ref_cnt_offset) as *const AtomicUsize)).load(Ordering::SeqCst)
        };
        let padding = decode_padding(encoded_ref_count);

        let alloc = ShmemAlloc {
            data: self.data,
            data_num_bytes: self.data_num_bytes - padding - std::mem::size_of::<AtomicUsize>(),
            base_ptr: self.base_ptr,
            base_data_len: self.base_data_len,
            base_len: self.base_len,
            global_base_ptr: self.global_base_ptr,
            global_base_len: self.global_base_len,
            my_alloc_pe: self.my_alloc_pe,
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset, //keep the same ref count offset as the parent allocation if this is actually a rt alloc, it will be updated when converted to a rt_alloc
            coll_meta_offset: self.coll_meta_offset,
            pe_map: self.pe_map.clone(),
            remote_addrs: self.remote_addrs.clone(),
            alloc_table: AllocTable::Runtime(alloc_table, self.data as usize, allocs),
            shmem: self.shmem.clone(),
        };
        debug!(target: "shmem", "Converted Shmem alloc to rt-alloc: {:?}", alloc);
        Ok(alloc)
    }

    pub(crate) fn leak(self) -> Option<CommAllocAddr> {
        match self.alloc_table {
            AllocTable::Fabric(_) => None, //only rt_allocs can be leaked
            AllocTable::Runtime(_, _, _) => {
                self.increment_fabric_ref_count(); //increment the ref count to account for the leaked instance
                self.increment_rt_ref_count(); //increment the ref count to account for the leaked instance
                debug!(target: "shmem", "Leaked Shmem rt-allocation: {:?}", self);
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

    pub(crate) fn pe_base_offset(&self, pe: usize) -> usize {
        let offset = unsafe { self.data.offset_from_unsigned(self.base_ptr) };
        unsafe {
            self.shmem
                .base_ptr()
                .add(self.pe_map[&pe] * self.base_len + offset) as usize
        }
    }

    pub(crate) unsafe fn zeroize_bytes(&self) {
        let u8_slice = std::slice::from_raw_parts_mut(self.data, self.data_num_bytes);
        u8_slice.fill(0);
    }

    pub(crate) fn wait(&self) {
        //shmem is always ready
    }

    // Per-allocation collective barrier generation counter, used by the star
    // all-reduce algorithm to synchronize root/non-root PEs. Distinct from
    // ShmemAllocator::barrier(), which is a single global barrier shared across
    // all concurrent allocations and would cross-talk between unrelated collectives.
    fn coll_barrier_atomic(&self) -> &AtomicUsize {
        unsafe { &*(self.shmem.base_ptr().add(self.coll_meta_offset) as *const AtomicUsize) }
    }

    // Per-root index register: which offset (in elements) within the root's data
    // the current collective op is targeting.
    fn coll_index_atomic(&self, pe: usize) -> &AtomicUsize {
        unsafe {
            &*(self.shmem.base_ptr().add(
                self.coll_meta_offset
                    + std::mem::size_of::<AtomicUsize>()
                    + pe * std::mem::size_of::<AtomicUsize>(),
            ) as *const AtomicUsize)
        }
    }

    pub(crate) fn coll_inc_barrier(&self) -> usize {
        self.coll_barrier_atomic().fetch_add(1, Ordering::SeqCst)
    }

    pub(crate) fn coll_get_barrier(&self) -> usize {
        self.coll_barrier_atomic().load(Ordering::SeqCst)
    }

    pub(crate) fn coll_reset_barrier(&self) {
        self.coll_barrier_atomic().store(0, Ordering::SeqCst);
    }

    pub(crate) fn coll_set_index(&self, pe: usize, index: usize) {
        self.coll_index_atomic(pe).store(index, Ordering::SeqCst);
    }

    pub(crate) fn coll_index(&self, pe: usize) -> usize {
        self.coll_index_atomic(pe).load(Ordering::SeqCst)
    }
}

impl Drop for ShmemAlloc {
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop ShmemAlloc");
        let fabric_ref_count = self.decrement_fabric_ref_count();
        debug!(target: "shmem", "Dropping ShmemAlloc: {:?}" , self);
        match &self.alloc_table {
            AllocTable::Fabric(allocs) => {
                if fabric_ref_count == 2 {
                    debug!(target: "shmem", "Dropping fabric ShmemAlloc: {:?}", self);

                    let mut allocs = allocs.write();
                    let len = allocs.len();
                    allocs.retain(|a| a.base_ptr != self.base_ptr);
                    if len == allocs.len() {
                        panic!("failed to free alloc: {:?}", self);
                    }
                }
            }
            AllocTable::Runtime(rt_alloc_table, addr, allocs) => {
                let rt_ref_count = self.decrement_rt_ref_count();
                if rt_ref_count == 1 {
                    debug!(target: "shmem", "Dropping rt ShmemAlloc: {:?}", self);
                    rt_alloc_table.free(*addr).expect(&format!(
                        "[{:?}] Error removing from runtime alloc table {:x}",
                        std::thread::current().id(),
                        addr
                    ));
                }
                if fabric_ref_count == 2 {
                    debug!(target: "shmem", "Dropping fabric ShmemAlloc: {:?}", self);
                    let mut allocs = allocs.write();
                    let len = allocs.len();
                    allocs.retain(|a| a.base_ptr != self.base_ptr);
                    if len == allocs.len() {
                        panic!("failed to free alloc: {:?}", self);
                    }
                }
            }
        }
        trace!(target: "drop", "end drop ShmemAlloc");
    }
}

impl From<ShmemAlloc> for CommAlloc {
    fn from(alloc: ShmemAlloc) -> Self {
        CommAlloc {
            inner_alloc: Arc::new(CommAllocInner::ShmemAlloc(alloc)),
            // alloc_type: CommAllocType::Fabric,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OneSidedShmemAlloc {
    pub(crate) data: *mut u8, // this is the actual address to the data on the remote PE (since we are in shared memory this is directly accessible)
    pub(crate) data_num_bytes: usize,
    pub(crate) remote_pe: usize,
    // keeps the backing allocation's ref count bumped for the lifetime of this
    // one-sided view, so it can't be freed while a get/put against `data` is in flight
    pub(crate) alloc: ShmemAlloc,
}

//safety is managed via higher level abstractions or marked unsafe
unsafe impl Sync for OneSidedShmemAlloc {}
unsafe impl Send for OneSidedShmemAlloc {}

impl OneSidedShmemAlloc {
    pub(crate) fn num_bytes(&self) -> usize {
        self.data_num_bytes
    }
    pub(crate) fn start(&self) -> usize {
        self.data as usize
    }
    pub(crate) fn sub_alloc(&self, offset: usize, len: usize) -> AllocResult<OneSidedShmemAlloc> {
        if offset + len > self.data_num_bytes {
            return Err(AllocError::InvalidSubAlloc(offset, len));
        }
        let new_data = unsafe { self.data.add(offset) };
        let alloc = OneSidedShmemAlloc {
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

impl From<OneSidedShmemAlloc> for CommAlloc {
    fn from(alloc: OneSidedShmemAlloc) -> Self {
        CommAlloc {
            inner_alloc: Arc::new(CommAllocInner::OneSidedShmemAlloc(alloc)),
            // alloc_type: CommAllocType::Remote,
        }
    }
}

//#[tracing::instrument(skip_all, level = "debug")]
fn attach_to_shmem(
    _num_pes: usize,
    job_id: usize,
    size: usize,
    align: usize,
    id: &str,
    header: usize,
    create: bool,
) -> ShmemHandle {
    let padding = std::mem::size_of::<usize>() % align;
    let shmem_size = std::mem::size_of::<usize>() + padding + size;

    let shmem_id =
        "lamellar_".to_owned() + &(job_id.to_string()) + "_" + &(shmem_size.to_string()) + "_" + id;
    // let  m = if create {
    let mut retry = 0;
    let m = loop {
        match ShmemConf::new()
            .size(shmem_size)
            .os_id(shmem_id.clone())
            .create()
        {
            Ok(m) => {
                // println!("created {:?}", shmem_id);
                if create {
                    // let zeros = vec![0u8; size];
                    unsafe {
                        //     std::ptr::copy_nonoverlapping(
                        //         zeros.as_ptr() as *const u8,
                        //         m.as_ptr() as *mut u8,
                        //         size,
                        //     );
                        *(m.as_ptr() as *mut _ as *mut usize) = header;
                    }
                }
                break Ok(m);
            }
            Err(ShmemError::LinkExists)
            | Err(ShmemError::MappingIdExists)
            | Err(ShmemError::MapOpenFailed(_)) => {
                match ShmemConf::new().os_id(shmem_id.clone()).open() {
                    Ok(m) => {
                        // println!("attached {:?}", shmem_id);
                        if create {
                            // let zeros = vec![0u8; size];
                            unsafe {
                                // std::ptr::copy_nonoverlapping(
                                //     zeros.as_ptr() as *const u8,
                                //     m.as_ptr() as *mut u8,
                                //     size,
                                // );
                                *(m.as_ptr() as *mut _ as *mut usize) = header;
                            }
                            // unsafe {
                            //     println!(
                            //         "updated {:?} {:?}",
                            //         shmem_id,
                            //         *(m.as_ptr() as *const _ as *const usize)
                            //     );
                            // }
                        }
                        break Ok(m);
                    }
                    Err(ShmemError::MapOpenFailed(_)) if retry < 5 => {
                        retry += 1;
                        std::thread::sleep(std::time::Duration::from_millis(50));
                    }
                    Err(e) => break Err(e),
                }
            }
            Err(e) => break Err(e),
        }
    };
    let m = match m {
        Ok(m) => m,
        Err(e) => panic!("unable to create shared memory {:?} {:?}", shmem_id, e),
    };

    while (unsafe { *(m.as_ptr() as *const _ as *const usize) } != header) {
        std::thread::yield_now()
    }
    // let cnt = unsafe {
    //     (m.as_ptr().add(std::mem::size_of::<usize>()) as *mut AtomicUsize)
    //         .as_ref()
    //         .unwrap()
    // };
    // cnt.fetch_add(1, Ordering::SeqCst);
    // if create {
    //     while cnt.load(Ordering::SeqCst) != num_pes {
    //         std::thread::yield_now()
    //     }
    // }

    unsafe {
        trace!(
            "shmem inited {:?} {:?}",
            shmem_id,
            *(m.as_ptr() as *const _ as *const usize)
        );
    }

    // unsafe {
    //     MyShmem {
    //         // data: m.as_ptr().add(std::mem::size_of::<usize>()),
    //         // len: size,
    //         alloc: CommAlloc {
    //             info: CommAllocInner::Raw(
    //                 m.as_ptr().add(std::mem::size_of::<usize>() + padding) as usize,
    //                 size,
    //             ),
    //             alloc_type: CommAllocType::Fabric,
    //         },
    //         _shmem: m,
    //     }
    // }
    unsafe {
        ShmemHandle {
            base_addr: m.as_ptr().add(std::mem::size_of::<usize>() + padding),
            _shmem: m,
        }
    }
}

pub(crate) struct ShmemAllocator {
    _shmem: ShmemHandle,
    mutex: *mut AtomicIsize,
    id: *mut AtomicUsize,
    barrier1: *mut usize,
    barrier2: *mut usize,
    my_pe: usize,
    num_pes: usize,
    job_id: usize,
    allocs: Arc<RwLock<Vec<ShmemAlloc>>>,
}

unsafe impl Sync for ShmemAllocator {}
unsafe impl Send for ShmemAllocator {}

impl std::fmt::Debug for ShmemAllocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShmemAllocator")
            .field("my_pe", &self.my_pe)
            .field("num_pes", &self.num_pes)
            .field("job_id", &self.job_id)
            .finish()
    }
}

impl ShmemAllocator {
    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new(num_pes: usize, pe: usize, job_id: usize) -> Self {
        let size = std::mem::size_of::<AtomicUsize>()
            + std::mem::size_of::<usize>()
            + std::mem::size_of::<usize>() * num_pes * 2;
        let shmem = attach_to_shmem(
            num_pes,
            job_id,
            size,
            std::mem::align_of::<usize>(),
            "alloc",
            job_id,
            pe == 0,
        );
        let base_ptr = shmem.base_ptr();

        let data = unsafe { std::slice::from_raw_parts_mut(base_ptr, size) };
        if pe == 0 {
            for i in data {
                *i = 0;
            }
        }

        trace!("new shmem allocated! base_pointer{:?}", base_ptr);
        ShmemAllocator {
            _shmem: shmem,
            mutex: base_ptr as *mut AtomicIsize,
            id: unsafe { base_ptr.add(std::mem::size_of::<AtomicIsize>()) as *mut AtomicUsize },
            barrier1: unsafe {
                base_ptr
                    .add(std::mem::size_of::<AtomicIsize>() + std::mem::size_of::<AtomicUsize>())
                    as *mut usize
            },
            barrier2: unsafe {
                base_ptr.add(
                    std::mem::size_of::<AtomicIsize>()
                        + std::mem::size_of::<AtomicUsize>()
                        + std::mem::size_of::<usize>() * num_pes,
                ) as *mut usize
            },
            // barrier3: unsafe { base_ptr.add(std::mem::size_of::<AtomicUsize>() + std::mem::size_of::<usize>()) as *mut usize + std::mem::size_of::<usize>()*num_pes*2},
            my_pe: pe,
            num_pes: num_pes,
            job_id: job_id,
            allocs: Arc::new(RwLock::new(vec![])),
        }
    }

    //TODO update this to a dissemination barrier or someother optimized shmem barrier
    pub(crate) unsafe fn barrier(&self) {
        let _allocs = self.allocs.read();
        let barrier1 = std::slice::from_raw_parts_mut(self.barrier1, self.num_pes);
        let barrier2 = std::slice::from_raw_parts_mut(self.barrier2, self.num_pes);
        // Wait for all PEs to reach this point

        for i in 0..self.num_pes {
            while barrier2[i] != 0 {
                std::thread::yield_now();
            }
        }
        if self.my_pe == 0 {
            (*self.id).fetch_add(1, Ordering::SeqCst);
            barrier1[self.my_pe] = (*self.id).load(Ordering::SeqCst);
        }
        while barrier1[0] == 0 {
            std::thread::yield_now();
        }
        let id = (*self.id).load(Ordering::SeqCst);
        barrier1[self.my_pe] = id;
        for i in 1..self.num_pes {
            while barrier1[i] < id {
                std::thread::yield_now();
            }
        }
        barrier2[self.my_pe] = 1;
        if self.my_pe == 0 {
            self.mutex
                .as_ref()
                .unwrap()
                .fetch_add(self.num_pes as isize, Ordering::SeqCst);
        }
        self.mutex.as_ref().unwrap().fetch_sub(1, Ordering::SeqCst);
        while self.mutex.as_ref().unwrap().load(Ordering::SeqCst) != 0 {}
        // Reset barrier
        barrier1[self.my_pe] = 0;
        for i in 0..self.num_pes {
            while barrier1[i] != 0 {
                std::thread::yield_now();
            }
        }
        barrier2[self.my_pe] = 0;
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) unsafe fn alloc(&self, data_size: usize, align: usize, pes: &[usize]) -> ShmemAlloc {
        let mut allocs = self.allocs.write();
        let barrier1 = std::slice::from_raw_parts_mut(self.barrier1, self.num_pes);
        let barrier2 = std::slice::from_raw_parts_mut(self.barrier2, self.num_pes);
        // println!("trying to alloc! {:?} {:?} {:?}",self.my_pe, barrier1,barrier2);
        // let barrier3 = std::slice::from_raw_parts_mut(self.barrier1, self.num_pes) ;
        let mut sub_alloc_pe_id = None;
        let mut pe_map = HashMap::new();
        for (i, pe) in pes.iter().enumerate() {
            pe_map.insert(*pe, i);
            if *pe == self.my_pe {
                sub_alloc_pe_id = Some(i);
            }
            while barrier2[*pe] != 0 {
                std::thread::yield_now();
            }
        }
        let sub_alloc_pe_id = sub_alloc_pe_id.expect("pe not in sub alloc list");
        // let mut pes_clone = pes.clone();
        // let first_pe = pes_clone.next().unwrap();

        // let mut pes_len = 1;

        if sub_alloc_pe_id == 0 {
            while let Err(_) = self.mutex.as_ref().unwrap().compare_exchange(
                0,
                1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                std::thread::yield_now();
            }
            (*self.id).fetch_add(1, Ordering::SeqCst);
            let id = (*self.id).load(Ordering::SeqCst);
            barrier1[self.my_pe] = id;
            for pe in pes.iter() {
                while barrier1[*pe] != id {
                    std::thread::yield_now();
                }
            }
        } else {
            while barrier1[pes[0]] == 0 {
                std::thread::yield_now();
            }
            let id = (*self.id).load(Ordering::SeqCst);
            barrier1[self.my_pe] = id;

            for pe in pes.iter() {
                while barrier1[*pe] != id {
                    std::thread::yield_now();
                }
            }
        }
        let (padding, size, align) = calc_alloc_padding_size_align(data_size, align);

        // extra shmem-local (not shared with other allocations) space for this
        // allocation's own collective barrier generation counter + one index
        // register per potential root PE
        let coll_meta_offset = size * pes.len();
        let coll_meta_size =
            std::mem::size_of::<AtomicUsize>() + pes.len() * std::mem::size_of::<AtomicUsize>();

        // println!("going to attach to shmem {:?} {:?} {:?} {:?} {:?}",size*pes_len,*self.id,self.my_pe, barrier1,barrier2);
        let shmem = attach_to_shmem(
            pes.len(),
            self.job_id,
            size * pes.len() + coll_meta_size,
            align,
            &((*self.id).load(Ordering::SeqCst).to_string()),
            (*self.id).load(Ordering::SeqCst),
            sub_alloc_pe_id == 0,
        );
        // let base_ptr = shmem.base_ptr();
        let my_base_ptr = shmem.base_ptr().add(size * sub_alloc_pe_id);
        barrier2[self.my_pe] = my_base_ptr as usize; //save the start of my segment in my address space

        if sub_alloc_pe_id == 0 {
            let coll_meta = std::slice::from_raw_parts_mut(
                shmem.base_ptr().add(coll_meta_offset),
                coll_meta_size,
            );
            coll_meta.fill(0);
        }

        //temporarily use the first element of the shared segment as a counter barrier
        let cnt = shmem.base_ptr() as *mut AtomicIsize;
        if sub_alloc_pe_id == 0 {
            cnt.as_ref()
                .unwrap()
                .fetch_add(pes.len() as isize, Ordering::SeqCst);
        }
        cnt.as_ref().unwrap().fetch_sub(1, Ordering::SeqCst);
        while cnt.as_ref().unwrap().load(Ordering::SeqCst) != 0 {}
        let addrs = barrier2.to_vec();
        trace!(
            "attached {:?} {:?} my offset: {:?}",
            self.my_pe,
            shmem.base_ptr(),
            my_base_ptr
        );
        barrier1[self.my_pe] = 0;
        for pe in pes.into_iter() {
            while barrier1[*pe] != 0 {
                std::thread::yield_now();
            }
        }
        barrier2[self.my_pe] = 0;
        if sub_alloc_pe_id == 0 {
            self.mutex.as_ref().unwrap().store(0, Ordering::SeqCst);
        }

        let alloc = ShmemAlloc::new(
            my_base_ptr,
            data_size,
            padding,
            my_base_ptr,
            data_size,
            size,
            shmem.base_ptr(),
            size * pes.len(),
            sub_alloc_pe_id,
            pe_map,
            addrs,
            self.allocs.clone(),
            Arc::new(shmem),
            coll_meta_offset,
        )
        .expect("failed to create shmem alloc");
        allocs.push(alloc.clone());
        trace!(target: "shmem","current allocs: {:?}", allocs);
        alloc
    }

    pub(crate) fn get_alloc_from_start_addr(
        &self,
        mem_addr: CommAllocAddr,
    ) -> AllocResult<ShmemAlloc> {
        let allocs = self.allocs.read();
        for alloc in allocs.iter() {
            if alloc.start() == mem_addr.0 {
                return Ok(alloc.clone());
            }
        }
        Err(AllocError::LocalNotFound(mem_addr))
    }

    pub(crate) fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> Option<usize> {
        let allocs = self.allocs.read();
        for alloc in allocs.iter() {
            let remote_start = alloc.remote_addrs[remote_pe];
            if remote_start <= remote_addr && remote_addr < remote_start + alloc.data_num_bytes {
                return Some(alloc.data as usize + (remote_addr - remote_start));
            }
        }
        None
    }

    pub(crate) fn one_sided_alloc_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
        num_bytes: usize,
    ) -> CommAlloc {
        let allocs = self.allocs.read();
        for alloc in allocs.iter() {
            let remote_start = alloc.remote_addrs[remote_pe];
            if remote_start <= remote_addr && remote_addr < remote_start + alloc.data_num_bytes {
                let remote_src_addr =
                    alloc.pe_base_offset(remote_pe) + (remote_addr - remote_start);
                return OneSidedShmemAlloc {
                    data: (remote_src_addr) as *mut u8,
                    data_num_bytes: num_bytes,
                    remote_pe,
                    alloc: alloc.clone(),
                }
                .into();
            }
        }
        panic!(
            "failed to find remote addr {:x} on pe {}",
            remote_addr, remote_pe
        );
    }

    pub(crate) fn local_alloc_and_offset_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
    ) -> Option<(CommAlloc, usize)> {
        let allocs = self.allocs.read();
        for alloc in allocs.iter() {
            let remote_start = alloc.remote_addrs[remote_pe];
            if remote_start <= remote_addr && remote_addr < remote_start + alloc.data_num_bytes {
                return Some((alloc.clone().into(), remote_addr - remote_start));
            }
        }
        None
    }
    pub(crate) fn remote_addr(&self, remote_pe: usize, local_addr: usize) -> Option<usize> {
        let allocs = self.allocs.read();
        for alloc in allocs.iter() {
            if alloc.data as usize <= local_addr
                && local_addr < alloc.data as usize + alloc.data_num_bytes
            {
                return Some(alloc.remote_addrs[remote_pe] + (local_addr - alloc.data as usize));
            }
        }
        None
    }
}

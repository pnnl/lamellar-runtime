mod context;
mod endpoint;
mod error;
mod memory_region;
mod worker;

use context::Context;
use endpoint::Endpoint;
pub(crate) use endpoint::UcxRequest;
use endpoint::ATOMIC_PUT_TMP;
use memory_region::{MemoryHandle, MemoryHandleInner, RKey};
use worker::Worker;

use crate::{
    LAMELLAR_THREAD_ID, config, lamellae::{
        AllocError, AllocResult, AllocationType, AtomicOp, CollectiveOpKind, CommAlloc, CommAllocAddr, CommAllocInner, FabricError, collective::{AllReduceOp, RootOrSliceMut, RootSrcOrSliceMut, RootSrcSliceOrNone}, comm::alloc::*, ucx_lamellae_mt::ucc::{self, Error, UccContext, UccLib, UccRequest, UccTeam}
    }, lamellar_alloc::{BTreeAlloc, LamellarAlloc}
};

use pmi::{pmi::Pmi, PmiBuilder};
use lamellar_ucx_sys::ucp_atomic_op_t;

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tracing::{debug, trace};

#[derive(Clone)]
pub(crate) struct CommGroup {
    worker: Arc<Worker>,
    endpoints: Vec<Arc<Endpoint>>,
    ucc_world_team: Option<Arc<UccTeam>>,
    ucc_context: Option<Arc<UccContext>>,
    ucc_world_buffer: Option<Arc<UcxMtAlloc>>,
}

pub(crate) struct UcxWorld {
    pmi: Arc<dyn Pmi>,
    pub(crate) my_pe: usize,
    pub(crate) num_pes: usize,
    context: Arc<Context>,
    comm_groups: Vec<CommGroup>,
    utility_comm_group: CommGroup,
    mem_handles: Arc<Mutex<Vec<UcxMtAlloc>>>,
    remote_keys: Arc<Mutex<Vec<(UcxMtAlloc, Vec<(usize, Arc<RKey>)>)>>>,
    exchange_buffer: Option<UcxMtAlloc>,
}

impl std::fmt::Debug for UcxWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("UcxWorld")
            .field("my_pe", &self.my_pe)
            .field("num_pes", &self.num_pes)
            .finish()
    }
}

impl UcxWorld {
    pub(crate) fn new(num_threads: usize) -> Self {
        let my_pmi = Arc::new(
            PmiBuilder::init()
                .map_err(|e| {
                    eprintln!("Error initializing PMI: {:?}", e);
                    FabricError::InitError(1)
                })
                .unwrap(),
        );
        let context = Context::new(my_pmi.clone()).unwrap();
        let mut comm_groups = Vec::with_capacity(num_threads + 1);
        // let mut comm_groups = Vec::with_capacity(1);
        for tid in 0..num_threads {
            let worker = context.create_worker().unwrap();
            let addresses = worker.exchange_address(my_pmi.clone(),tid).unwrap();
            let endpoints = addresses
                .iter()
                .map(|a| Endpoint::new(worker.clone(), a).unwrap())
                .collect::<Vec<_>>();
                
            let cg = CommGroup { 
                worker, 
                endpoints, 
                ucc_context: None, 
                ucc_world_team: None, 
                ucc_world_buffer: None
            };
            comm_groups.push(cg);
        }
        let utility_comm_group = comm_groups.last().unwrap().clone();

        let my_pe = my_pmi.rank();
        let num_pes = my_pmi.ranks().len();
        let mem_handles = Arc::new(Mutex::new(Vec::new()));
        let remote_keys = Arc::new(Mutex::new(Vec::new()));
        let exchange_buffer = Self::initial_alloc(
            &context,
            &utility_comm_group.endpoints,
            &comm_groups,
            my_pmi.clone(),
            num_pes,
            my_pe,
            mem_handles.clone(),
            remote_keys.clone(),
        )
        .unwrap();
        Self::warmup_peer_puts(
            &utility_comm_group.worker,
            &exchange_buffer,
            my_pe,
            num_pes,
        );
        let mut world = UcxWorld {
            pmi: my_pmi.clone(),
            my_pe,
            num_pes,
            context,
            comm_groups,
            utility_comm_group,
            mem_handles,
            remote_keys,
            exchange_buffer: Some(exchange_buffer),
        };
        my_pmi.barrier(false).expect("Failed to perform barrier after initial allocations");
        for tid in 0..num_threads {

            let alloc = Arc::new(world.alloc(config().ucc_oob_init_buffer_size * num_pes, 8, AllocationType::Global));
            world.comm_groups[tid].ucc_world_buffer = Some(alloc.clone());

            unsafe { alloc.as_mut_slice().iter_mut().for_each(|x| *x = u8::MAX) };
            world.barrier();
            let ucc_lib = Arc::new(UccLib::new());
            let ucc_context = Arc::new(UccContext::new(ucc_lib.clone(), alloc.clone()).unwrap());
            world.barrier();
            unsafe { alloc.as_mut_slice().iter_mut().for_each(|x| *x = u8::MAX) };
            world.barrier();
            let ucc_world_team = UccTeam::new(my_pe, &(0..num_pes).collect::<Vec<_>>(), ucc_context.clone(), alloc.clone()).unwrap();
            world.comm_groups[tid].ucc_world_team = Some(Arc::new(ucc_world_team));
            world.comm_groups[tid].ucc_context = Some(ucc_context);

        }

        world
    }

    pub(crate) fn atomic_avail<T: 'static>(&self) -> bool {
        let id = std::any::TypeId::of::<T>();

        if id == std::any::TypeId::of::<u8>() {
            false
        } else if id == std::any::TypeId::of::<u16>() {
            false
        } else if id == std::any::TypeId::of::<u32>() {
            true
        } else if id == std::any::TypeId::of::<u64>() {
            true
        } else if id == std::any::TypeId::of::<i8>() {
            false
        } else if id == std::any::TypeId::of::<i16>() {
            false
        } else if id == std::any::TypeId::of::<i32>() {
            true
        } else if id == std::any::TypeId::of::<i64>() {
            true
        } else if id == std::any::TypeId::of::<usize>() {
            true
        } else if id == std::any::TypeId::of::<isize>() {
            true
        } else {
            false
        }
    }

    pub(crate) fn atomic_op_avail<T: 'static>(&self, op: &AtomicOp<T>) -> bool {
        if !self.atomic_avail::<T>() {
            return false;
        }
        matches!(
            op,
            AtomicOp::Read(_)
                | AtomicOp::Write(_)
                | AtomicOp::Cas
                | AtomicOp::Sum(_)
                | AtomicOp::Sub(_)
                | AtomicOp::BitOr(_)
                | AtomicOp::BitXor(_)
                | AtomicOp::BitAnd(_)
                | AtomicOp::FetchSum(_)
                | AtomicOp::FetchSub(_)
                | AtomicOp::FetchBitOr(_)
                | AtomicOp::FetchBitXor(_)
                | AtomicOp::FetchBitAnd(_)
        )
    }
    pub(crate) fn collective_avail<T: 'static>(&self, op: CollectiveOpKind) -> bool {
        if self.comm_groups[LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len()].ucc_context.is_none() {
            return false;
        }
        if std::any::TypeId::of::<T>() == std::any::TypeId::of::<()>() {
            assert!(matches!(op, CollectiveOpKind::Barrier));
            return true;
        }
        let id = std::any::TypeId::of::<T>();

        if id == std::any::TypeId::of::<u8>() {
            true
        } else if id == std::any::TypeId::of::<u16>() {
            true
        } else if id == std::any::TypeId::of::<u32>() {
            true
        } else if id == std::any::TypeId::of::<u64>() {
            true
        } else if id == std::any::TypeId::of::<u128>() {
            true
        } else if id == std::any::TypeId::of::<i8>() {
            true
        } else if id == std::any::TypeId::of::<i16>() {
            true
        } else if id == std::any::TypeId::of::<i32>() {
            true
        } else if id == std::any::TypeId::of::<i64>() {
            true
        } else if id == std::any::TypeId::of::<i128>() {
            true
        } else if id == std::any::TypeId::of::<usize>() {
            true
        } else if id == std::any::TypeId::of::<isize>() {
            true
        } else {
            false
        }
    }
    fn initial_alloc(
        context: &Arc<Context>,
        util_endpoints: &Vec<Arc<Endpoint>>,
        comm_groups: &Vec<CommGroup>,
        pmi: Arc<dyn Pmi>,
        num_pes: usize,
        my_pe: usize,
        mem_handles: Arc<Mutex<Vec<UcxMtAlloc>>>,
        remote_keys: Arc<Mutex<Vec<(UcxMtAlloc, Vec<(usize, Arc<RKey>)>)>>>,
    ) -> AllocResult<UcxMtAlloc> {
        let mem_handle = MemoryHandleInner::alloc(context, 1024); //dummy allocation to get the size of the exchange buffer
        let mut data_size = mem_handle.addr.to_ne_bytes().len();
        data_size += mem_handle.pack().as_ref().len();
        data_size = data_size * num_pes;
        drop(mem_handle);
        let (padding, size, _align) = calc_alloc_padding_size_align(data_size, 8);
        // debug!("Initial alloc size: {}", size);
        let mem_handle = MemoryHandleInner::alloc(context, size * num_pes);
        let buffer_keys = mem_handle
            .exchange_key_pmi(util_endpoints, &pmi, comm_groups.len())
            .unwrap();

        let mem = MemoryHandle {
            addr: mem_handle.addr,
            size: size,
            inner: mem_handle.clone(),
        };

        let alloc = UcxMtAlloc::new(
            mem,
            data_size,
            padding,
            my_pe,
            num_pes,
            context.clone(),
            comm_groups.clone(),
            buffer_keys.clone(),
            mem_handles.clone(),
            remote_keys.clone(),
        )?;
        mem_handles.lock().unwrap().push(alloc.clone());
        remote_keys
            .lock()
            .unwrap()
            .push((alloc.clone(), buffer_keys));
        Ok(alloc)
    }

    // Found this was necessary in the offchance that the first call to a intranode PE
    // happened simultaneously (in a MT environment) with other operations like progress or flush
    fn warmup_peer_puts(
        worker: &Arc<Worker>,
        buffer: &UcxMtAlloc,
        my_pe: usize,
        num_pes: usize,
    ) {
        for pe in 0..num_pes {
            if pe == my_pe {
                continue;
            }
            unsafe {
                buffer.put_inner(pe, 0, std::slice::from_ref(&my_pe), false, false);
            }
        }

        worker
            .wait_all()
            .expect("Failed final worker flush after UCX warm-up puts");
    }

    pub(crate) fn alloc(
        &self,
        data_size: usize,
        align: usize,
        _alloc_type: AllocationType,
    ) -> UcxMtAlloc {
        //add space for ref count and padding to align it
        let (padding, size, _align) = calc_alloc_padding_size_align(data_size, align);

        let mem_handle = MemoryHandleInner::alloc(&self.context, size);
        let buffer_keys = mem_handle
            .exchange_key_alloc(
                &self.utility_comm_group.endpoints,
                &self.pmi,
                &self.exchange_buffer.as_ref().unwrap(),
                self.comm_groups.len(),
            )
            .unwrap();

        let mem = MemoryHandle {
            addr: mem_handle.addr,
            size: size,
            inner: mem_handle.clone(),
        };

        let alloc = UcxMtAlloc::new(
            mem,
            data_size,
            padding,
            self.my_pe,
            self.num_pes,
            self.context.clone(),
            self.comm_groups.clone(),
            buffer_keys.clone(),
            self.mem_handles.clone(),
            self.remote_keys.clone(),
        )
        .expect("UcxMtAlloc::new failed");
        self.mem_handles.lock().unwrap().push(alloc.clone());
        self.remote_keys
            .lock()
            .unwrap()
            .push((alloc.clone(), buffer_keys));
        alloc
    }

    pub(crate) fn wait_all(&self) {
        for comm_group in &self.comm_groups {
            comm_group
                .worker
                .wait_all()
                .expect("UcxWorld::wait_all failed waiting on UCX requests");
        }
    }
    #[allow(dead_code)] // WIP: called via trait dispatch, lint false-positive
    pub(crate) fn thread_wait(&self) {
        self.comm_groups[LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len()]
            .worker
            .wait_all()
            .expect("UcxWorld::thread_wait failed waiting on UCX requests");
    }

    pub(crate) fn progress_all(&self) {
        for comm_group in &self.comm_groups {
            comm_group.worker.progress();
        }
    }

    pub(crate) fn thread_progress(&self) {
        self.comm_groups[LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len()]
            .worker
            .progress();
    }

    pub(crate) fn barrier(&self) {
        self.pmi.barrier(false).expect(" Failed to perform barrier");
    }

    pub(crate) fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> Option<usize> {
        let allocs = self.remote_keys.lock().unwrap();
        for (alloc, remote_addrs) in allocs.iter() {
            let remote_pe_addr = remote_addrs[remote_pe].0;
            if remote_pe_addr <= remote_addr && remote_addr < remote_pe_addr + alloc.data_num_bytes
            {
                let offset = remote_addr - remote_pe_addr;
                return Some(alloc.mem.inner.addr + offset);
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
        let allocs = self.remote_keys.lock().unwrap();
        for (alloc, remote_addrs) in allocs.iter() {
            let remote_pe_addr = remote_addrs[remote_pe].0;
            if remote_pe_addr <= remote_addr
                && remote_addr + num_bytes <= remote_pe_addr + alloc.data_num_bytes
            {
                let offset = remote_addr - remote_pe_addr;
                return OneSidedUcxMtAlloc {
                    alloc: alloc
                        .clone()
                        .sub_alloc(offset, num_bytes)
                        .expect("one_sided_alloc_from_remote_pe_and_addr failed"),
                    remote_pe,
                }
                .into();
            }
        }
        panic!(
            "unable to find allocation for remote pe: {} addr: {:x} num_bytes: {}",
            remote_pe, remote_addr, num_bytes
        );
    }

    pub(crate) fn local_alloc_and_offset_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
    ) -> Option<(CommAlloc, usize)> {
        let allocs = self.remote_keys.lock().unwrap();
        for (alloc, remote_addrs) in allocs.iter() {
            let remote_pe_addr = remote_addrs[remote_pe].0;
            if remote_pe_addr <= remote_addr && remote_addr < remote_pe_addr + alloc.data_num_bytes
            {
                let offset = remote_addr - remote_pe_addr;
                return Some((alloc.clone().into(), offset));
            }
        }
        None
    }

    pub(crate) fn remote_addr(&self, pe: usize, local_addr: usize) -> Option<usize> {
        let allocs = self.remote_keys.lock().unwrap();
        for (alloc, remote_addrs) in allocs.iter() {
            if alloc.mem.inner.addr <= local_addr
                && local_addr < alloc.mem.inner.addr + alloc.data_num_bytes
            {
                let offset = local_addr - alloc.mem.inner.addr;
                let remote_pe_addr = remote_addrs[pe].0;
                return Some(remote_pe_addr + offset);
            }
        }
        None
    }

    pub(crate) fn get_alloc_from_start_addr(
        &self,
        addr: CommAllocAddr,
    ) -> Result<UcxMtAlloc, String> {
        let allocs = self.mem_handles.lock().unwrap();
        for alloc in allocs.iter() {
            if alloc.mem.inner.addr == *addr {
                return Ok(alloc.clone());
            }
        }
        Err(format!("No allocation found for address {:x}", addr))
    }

    pub(crate) fn clear_allocs(&self) {
        let mut mem_handles = self.mem_handles.lock().unwrap();
        let temp_handles = mem_handles.drain(..).collect::<Vec<_>>();
        drop(mem_handles);
        let mut remote_keys = self.remote_keys.lock().unwrap();
        let temp_keys = remote_keys.drain(..).collect::<Vec<_>>();
        drop(remote_keys);
        for (_alloc, rkeys) in temp_keys.into_iter() {
            for (addr, rkey) in rkeys.into_iter() {
                let ref_cnt = Arc::strong_count(&rkey);
                debug!("Clearing rkey for addr {:x}, ref count: {}", addr, ref_cnt);
            }
        }
        debug!("Cleared {} allocations", temp_handles.len());
    }
}

impl Drop for UcxWorld {
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop UcxWorld");

        for comm_group in self.comm_groups.iter_mut() {
            comm_group.ucc_world_team.take();
            comm_group.ucc_context.take();
            comm_group.ucc_world_buffer.take();
        }

        debug!("dropping ucx world");
        self.barrier();
        self.exchange_buffer.take();
        self.clear_allocs();
        let mem_handles_cnt = Arc::strong_count(&self.mem_handles);
        let remote_keys_cnt = Arc::strong_count(&self.remote_keys);
        debug!(
            "mem handle count: {} remote keys cnt: {}",
            mem_handles_cnt, remote_keys_cnt
        );
        // self.remote_keys.lock().unwrap().clear();
        // self.mem_handles.lock().unwrap().clear();
        self.barrier();
        trace!(target: "drop", "end drop UcxWorld");
    }
}

#[derive(Clone)]
enum AllocTable {
    Fabric(
        Arc<Mutex<Vec<UcxMtAlloc>>>,
        Arc<Mutex<Vec<(UcxMtAlloc, Vec<(usize, Arc<RKey>)>)>>>,
    ),
    Runtime(
        BTreeAlloc,
        usize,
        Arc<Mutex<Vec<UcxMtAlloc>>>,
        Arc<Mutex<Vec<(UcxMtAlloc, Vec<(usize, Arc<RKey>)>)>>>,
    ), //the usize is the offset of the rt_alloc so that we can free it properly if a sub_alloc is the last reference
}

pub(crate) struct UcxMtAlloc {
    mem: MemoryHandle,
    data_num_bytes: usize,
    pub(crate) my_pe: usize,
    pub(crate) num_pes: usize,
    fabric_ref_cnt_offset: usize,
    rt_ref_cnt_offset: usize,
    context: Arc<Context>,
    comm_groups: Arc<Vec<CommGroup>>,
    remote_keys: Arc<Vec<(usize, Arc<RKey>)>>,
    alloc_table: AllocTable,
}

impl Clone for UcxMtAlloc {
    fn clone(&self) -> Self {
        self.increment_fabric_ref_count();
        if let AllocTable::Runtime(_, _, _, _) = &self.alloc_table {
            self.increment_rt_ref_count();
        }
        Self {
            mem: self.mem.clone(),
            data_num_bytes: self.data_num_bytes,
            my_pe: self.my_pe,
            num_pes: self.num_pes,
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: self.rt_ref_cnt_offset,
            context: self.context.clone(),
            comm_groups: self.comm_groups.clone(),
            remote_keys: self.remote_keys.clone(),
            alloc_table: self.alloc_table.clone(),
        }
    }
}

impl From<UcxMtAlloc> for CommAlloc {
    fn from(alloc: UcxMtAlloc) -> Self {
        CommAlloc {
            inner_alloc: Arc::new(CommAllocInner::UcxMtAlloc(alloc)),
            // alloc_type: CommAllocType::Fabric,
        }
    }
}

impl std::fmt::Debug for UcxMtAlloc {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let fabric_ref_count = unsafe {
            (&*(self.mem.inner.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize))
                .load(Ordering::SeqCst)
        };

        let mut temp = f.debug_struct("UcxMtAlloc");
        temp.field(
            "addr",
            &format_args!(
                "{:x} - ({:x}) {:x}",
                self.mem.addr,
                self.mem.addr + self.data_num_bytes,
                self.mem.addr + self.mem.size
            ),
        )
        .field("data_num_bytes", &self.data_num_bytes)
        .field("my_pe", &self.my_pe)
        .field("num_pes", &self.num_pes)
        .field(
            "fabric_ref_cnt_offset",
            &format_args!(
                "{} ({:?}): {}",
                self.fabric_ref_cnt_offset,
                unsafe {
                    self.mem.inner.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize
                },
                fabric_ref_count
            ),
        );
        if let AllocTable::Runtime(_, _, _, _) = &self.alloc_table {
            let rt_ref_count = unsafe {
                (&*(self.mem.inner.as_ptr().add(self.rt_ref_cnt_offset) as *const AtomicUsize))
                    .load(Ordering::SeqCst)
            };
            let padding = decode_padding(rt_ref_count);
            let rt_ref_count = decode_ref_count(rt_ref_count);
            temp.field(
                "rt_ref_cnt_offset",
                &format_args!(
                    "{} ({:?}): {}, {}",
                    self.rt_ref_cnt_offset,
                    unsafe {
                        self.mem.inner.as_ptr().add(self.rt_ref_cnt_offset) as *const AtomicUsize
                    },
                    rt_ref_count,
                    padding,
                ),
            );
        }
        temp.finish()
    }
}

impl UcxMtAlloc {
    unsafe fn negate_atomic_value<T>(value: *mut T) {
        let num_bytes = std::mem::size_of::<T>();
        let mut bytes = vec![0u8; num_bytes];
        std::ptr::copy(value.cast::<u8>(), bytes.as_mut_ptr(), num_bytes);
        for byte in bytes.iter_mut() {
            *byte = !*byte;
        }

        let mut carry: u16 = 1;
        #[cfg(target_endian = "little")]
        for byte in bytes.iter_mut() {
            let sum = *byte as u16 + carry;
            *byte = sum as u8;
            carry = sum >> 8;
            if carry == 0 {
                break;
            }
        }
        #[cfg(target_endian = "big")]
        for byte in bytes.iter_mut().rev() {
            let sum = *byte as u16 + carry;
            *byte = sum as u8;
            carry = sum >> 8;
            if carry == 0 {
                break;
            }
        }
        std::ptr::copy(bytes.as_ptr(), value.cast::<u8>(), num_bytes);
    }

    fn ucx_atomic_update<T>(op: &mut AtomicOp<T>) -> (ucp_atomic_op_t, *const T) {
        match op {
            AtomicOp::Write(val) => (ucp_atomic_op_t::UCP_ATOMIC_OP_SWAP, val.as_ref().get_ref()),
            AtomicOp::Sum(val) | AtomicOp::FetchSum(val) => {
                (ucp_atomic_op_t::UCP_ATOMIC_OP_ADD, val.as_ref().get_ref())
            }
            AtomicOp::Sub(val) => (ucp_atomic_op_t::UCP_ATOMIC_OP_ADD, unsafe {
                Self::negate_atomic_value(val.as_mut().get_unchecked_mut());
                val.as_ref().get_ref()
            }),
            AtomicOp::FetchSub(val) => (ucp_atomic_op_t::UCP_ATOMIC_OP_ADD, unsafe {
                Self::negate_atomic_value(val.as_mut().get_unchecked_mut());
                val.as_ref().get_ref()
            }),
            AtomicOp::BitAnd(val) | AtomicOp::FetchBitAnd(val) => {
                (ucp_atomic_op_t::UCP_ATOMIC_OP_AND, val.as_ref().get_ref())
            }
            AtomicOp::BitOr(val) | AtomicOp::FetchBitOr(val) => {
                (ucp_atomic_op_t::UCP_ATOMIC_OP_OR, val.as_ref().get_ref())
            }
            AtomicOp::BitXor(val) | AtomicOp::FetchBitXor(val) => {
                (ucp_atomic_op_t::UCP_ATOMIC_OP_XOR, val.as_ref().get_ref())
            }
            _ => panic!("Unsupported atomic operation"),
        }
    }

    pub(crate) fn new(
        mem: MemoryHandle,
        data_num_bytes: usize,
        padding: usize,
        my_pe: usize,
        num_pes: usize,
        context: Arc<Context>,
        comm_groups: Vec<CommGroup>,
        my_remote_keys: Vec<(usize, Arc<RKey>)>,
        mem_handles: Arc<Mutex<Vec<UcxMtAlloc>>>,
        remote_keys: Arc<Mutex<Vec<(UcxMtAlloc, Vec<(usize, Arc<RKey>)>)>>>,
    ) -> AllocResult<Self> {
        let ref_cnt_offset = data_num_bytes + padding;
        let fabric_ref_cnt_offset = data_num_bytes + padding;
        let encoded = encode_ref_count_and_padding(1, padding);
        let alloc = Self {
            mem,
            data_num_bytes,
            my_pe,
            num_pes,
            fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            context,
            comm_groups: Arc::new(comm_groups),
            remote_keys: Arc::new(my_remote_keys),
            alloc_table: AllocTable::Fabric(mem_handles.clone(), remote_keys.clone()),
        };
        unsafe {
            (&*(alloc.mem.inner.as_ptr().add(alloc.fabric_ref_cnt_offset) as *mut AtomicUsize))
                .store(encoded, Ordering::SeqCst);
        }
        debug!(target: "ucx", "Created UCX allocation: {:?}", alloc);
        Ok(alloc)
    }
    pub(crate) fn start(&self) -> usize {
        self.mem.addr.into()
    }
    pub(crate) fn num_bytes(&self) -> usize {
        self.data_num_bytes
    }
    pub(crate) fn sub_alloc(&self, offset: usize, size: usize) -> AllocResult<Self> {
        if offset + size > self.num_bytes() {
            return Err(AllocError::InvalidSubAlloc(offset, size));
        }
        let remote_keys = self
            .remote_keys
            .iter()
            .map(|(addr, rkey)| (addr + offset, rkey.clone()))
            .collect();
        self.increment_fabric_ref_count();
        if let AllocTable::Runtime(_, _, _, _) = &self.alloc_table {
            self.increment_rt_ref_count();
        }
        let alloc = UcxMtAlloc {
            mem: self.mem.sub_alloc(offset, size),
            data_num_bytes: size,
            my_pe: self.my_pe,
            num_pes: self.num_pes,
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: self.rt_ref_cnt_offset, //keep the same ref count offset as the parent allocation if this is actually a rt alloc, it will be updated when converted to a rt_alloc
            context: self.context.clone(),
            comm_groups: self.comm_groups.clone(),
            remote_keys: Arc::new(remote_keys),
            alloc_table: self.alloc_table.clone(),
        };
        debug!(target: "ucx", "Created UCX sub-allocation: {:?}", alloc);
        Ok(alloc)
    }

    //we call this function to create a sub-allocation that is tracked as part of a runtime allocation
    pub(crate) fn rt_alloc(
        &self,
        alloc_table: BTreeAlloc,
        offset: usize,
        padding: usize,
        size: usize,
    ) -> AllocResult<Self> {
        if offset + size > self.num_bytes() {
            return Err(AllocError::InvalidSubAlloc(offset, size));
        }
        let data_bytes = size - padding - std::mem::size_of::<AtomicUsize>();
        let my_remote_keys = self
            .remote_keys
            .iter()
            .map(|(addr, rkey)| (addr + offset, rkey.clone()))
            .collect();

        self.increment_fabric_ref_count();
        let ref_cnt_offset = offset + data_bytes + padding;
        let encoded = encode_ref_count_and_padding(1, padding);

        let (mem_handles, remote_keys) = match &self.alloc_table {
            AllocTable::Fabric(mem_handles, remote_keys) => {
                (mem_handles.clone(), remote_keys.clone())
            }
            AllocTable::Runtime(_, _, mem_handles, remote_keys) => {
                (mem_handles.clone(), remote_keys.clone())
            }
        };

        let mem = self.mem.sub_alloc(offset, size);
        let addr = mem.addr;

        let alloc = UcxMtAlloc {
            mem,
            data_num_bytes: size,
            my_pe: self.my_pe,
            num_pes: self.num_pes,
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            context: self.context.clone(),
            comm_groups: self.comm_groups.clone(),
            remote_keys: Arc::new(my_remote_keys),
            alloc_table: AllocTable::Runtime(
                alloc_table,
                addr,
                mem_handles.clone(),
                remote_keys.clone(),
            ),
        };

        unsafe {
            (&*(alloc.mem.inner.as_ptr().add(alloc.rt_ref_cnt_offset) as *mut AtomicUsize))
                .store(encoded, Ordering::SeqCst);
        }
        debug!(target: "ucx", "Created UCX rt-sub-allocation: {:?}", alloc);
        Ok(alloc)
    }

    // This function is used to construct an rt_alloc from a raw sub-allocation
    // typically paired with a call to leak() we decrement the ref count as this instance recaptures the leaked instance
    pub(crate) fn as_rt_alloc(self, alloc_table: BTreeAlloc) -> AllocResult<Self> {
        let (mem_handles, remote_keys) = match &self.alloc_table {
            AllocTable::Fabric(mem_handles, remote_keys) => {
                (mem_handles.clone(), remote_keys.clone())
            }
            AllocTable::Runtime(_, _, mem_handles, remote_keys) => {
                (mem_handles.clone(), remote_keys.clone())
            }
        };

        //since we are recapturing a leaked alloc, the non-rt sub-allocation we are converting should contain the appropriate ref count space at the end of the allocation
        let ref_cnt_offset = ((self.start() - self.mem.inner.as_ptr() as usize) + self.num_bytes())
            - std::mem::size_of::<AtomicUsize>();

        let encoded_ref_count = unsafe {
            (&*(self.mem.inner.as_ptr().add(ref_cnt_offset) as *const AtomicUsize))
                .load(Ordering::SeqCst)
        };

        let padding = decode_padding(encoded_ref_count);

        let alloc = Self {
            mem: self.mem.clone(),
            data_num_bytes: self.data_num_bytes - padding - std::mem::size_of::<AtomicUsize>(),
            my_pe: self.my_pe,
            num_pes: self.num_pes,
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            context: self.context.clone(),
            comm_groups: self.comm_groups.clone(),
            remote_keys: self.remote_keys.clone(),
            alloc_table: AllocTable::Runtime(
                alloc_table,
                self.mem.addr,
                mem_handles.clone(),
                remote_keys.clone(),
            ),
        };

        debug!(target: "ucx", "Converted UCX alloc to rt-alloc: {:?}", alloc);
        Ok(alloc)
    }

    pub(crate) fn leak(self) -> Option<CommAllocAddr> {
        match self.alloc_table {
            AllocTable::Fabric(_, _) => None, //only rt_allocs can be leaked
            AllocTable::Runtime(_, _, _, _) => {
                self.increment_fabric_ref_count(); //increment the ref count to account for the leaked instance
                self.increment_rt_ref_count(); //increment the ref count to account for the leaked instance
                debug!(target: "ucx", "Leaked UCX rt-alloc: {:?}", self);
                Some(CommAllocAddr(self.start()))
            }
        }
    }

    pub(crate) fn increment_fabric_ref_count(&self) -> usize {
        let ref_count = unsafe {
            &*(self.mem.inner.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize)
        };
        increment_ref_count(ref_count)
    }

    pub(crate) fn decrement_fabric_ref_count(&self) -> usize {
        let ref_count = unsafe {
            &*(self.mem.inner.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize)
        };
        decrement_ref_count(ref_count)
    }

    pub(crate) fn increment_rt_ref_count(&self) -> usize {
        let ref_count = unsafe {
            &*(self.mem.inner.as_ptr().add(self.rt_ref_cnt_offset) as *const AtomicUsize)
        };
        increment_ref_count(ref_count)
    }
    pub(crate) fn decrement_rt_ref_count(&self) -> usize {
        let ref_count = unsafe {
            &*(self.mem.inner.as_ptr().add(self.rt_ref_cnt_offset) as *const AtomicUsize)
        };
        decrement_ref_count(ref_count)
    }

    pub(crate) unsafe fn put_inner<T>(
        &self,
        pe: usize,
        offset: usize, //with respect to T
        src_addr: &[T],
        blocking: bool,
        managed: bool,
    ) -> Option<UcxRequest> {
        let offset = offset * std::mem::size_of::<T>();
        trace!(target: "ucx",
            "put_inner pe {} offset {} src_addr len {} * size_of T {} total bytes {}, alloc local size {}",
            pe,
            offset,
            src_addr.len(),
            std::mem::size_of::<T>(),
            src_addr.len() * std::mem::size_of::<T>(),
            self.num_bytes(),
        );
        assert!(offset + src_addr.len() * std::mem::size_of::<T>() <= self.num_bytes());
        if pe == self.my_pe {
            std::ptr::copy(
                src_addr.as_ptr() as *const u8,
                (self.start() + offset) as *mut u8,
                src_addr.len() * std::mem::size_of::<T>(),
            );
            return None;
        }
        let (remote_addr, rkey) = &self.remote_keys[pe];
        let comm_group_id = LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len();
        trace!(target: "ucx",
            "put to pe {} at remote addr {:x} + offset {:?}, final addr: {:x} comm group id {}",
            pe,
            remote_addr,
            offset,
            remote_addr + offset,
            comm_group_id
        );
        let req = self.comm_groups[comm_group_id].endpoints[pe].put(
            src_addr.as_ptr() as _,
            src_addr.len() * std::mem::size_of::<T>(),
            remote_addr + offset,
            &rkey,
            managed,
        );
        if blocking {
            if let Some(req) = req {
                req.wait().expect("blocking_put failed");
            }
            None
        } else {
            req
        }
    }

    pub(crate) unsafe fn inner_get<T: Copy>(
        &self,
        pe: usize,
        offset: usize,
        blocking: bool,
        dst_addr: &mut [T],
    ) -> Option<UcxRequest> {
        let offset = offset * std::mem::size_of::<T>();
        trace!(target: "ucx",
            "get_inner pe {} offset {} dst_addr len {} * size_of T {} total bytes {}, alloc local size {}",
            pe,
            offset,
            dst_addr.len(),
            std::mem::size_of::<T>(),
            dst_addr.len() * std::mem::size_of::<T>(),
            self.num_bytes(),
        );
        assert!(offset + dst_addr.len() * std::mem::size_of::<T>() <= self.num_bytes());
        if pe == self.my_pe {
            std::ptr::copy(
                (self.start() + offset) as *const u8,
                dst_addr.as_mut_ptr() as *mut u8,
                dst_addr.len() * std::mem::size_of::<T>(),
            );
            return None;
        }
        let (remote_addr, rkey) = &self.remote_keys[pe];
        let req = self.comm_groups[LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len()]
            .endpoints[pe]
            .get(
                dst_addr.as_mut_ptr() as _,
                dst_addr.len() * std::mem::size_of::<T>(),
                remote_addr + offset,
                &rkey,
            );
        if blocking {
            req.wait().expect("blocking_get failed");
            None
        } else {
            Some(req)
        }
    }

    pub(crate) fn inner_atomic_op<T: Copy + 'static>(
        &self,
        pe: usize,
        offset: usize,
        blocking: bool,
        op: &mut AtomicOp<T>,
        managed: bool,
    ) -> Option<UcxRequest> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let (remote_addr, rkey) = &self.remote_keys[pe];
        let req = match op {
            AtomicOp::Write(val) => self.comm_groups
                [LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len()]
            .endpoints[pe]
                .atomic_swap(
                    val.as_ref().get_ref(),
                    &ATOMIC_PUT_TMP as *const _ as *mut T,
                    remote_addr + offset,
                    &rkey,
                    managed,
                ),
            AtomicOp::Sum(_)
            | AtomicOp::Sub(_)
            | AtomicOp::BitAnd(_)
            | AtomicOp::BitOr(_)
            | AtomicOp::BitXor(_) => {
                let (ucx_op, val) = Self::ucx_atomic_update(op);
                self.comm_groups[LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len()]
                    .endpoints[pe]
                    .atomic_op(ucx_op, val, remote_addr + offset, &rkey, managed)
            }
            AtomicOp::FetchMin(_)
            | AtomicOp::FetchMax(_)
            | AtomicOp::FetchSum(_)
            | AtomicOp::FetchSub(_)
            | AtomicOp::FetchProd(_)
            | AtomicOp::FetchBitOr(_)
            | AtomicOp::FetchBitXor(_)
            | AtomicOp::FetchBitAnd(_) => {
                panic!("Fetch atomic ops must use the fetch path")
            }
            AtomicOp::Cas => {
                panic!("Compare atomic ops must use the compare path")
            }
            _ => panic!("Unsupported atomic operation"),
        };
        if blocking {
            let comm_group_id = LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len();
            self.comm_groups[comm_group_id].endpoints[pe]
                .ep_wait_all()
                .expect("blocking_atomic_op failed");
            None
        } else {
            req
        }
    }

    pub(crate) fn inner_atomic_fetch_op<T: Copy + 'static>(
        &self,
        pe: usize,
        offset: usize,
        blocking: bool,
        op: &mut AtomicOp<T>,
        result: &mut [T],
    ) -> Option<UcxRequest> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let req = match op {
            AtomicOp::Read(zero) => {
                let (remote_addr, rkey) = &self.remote_keys[pe];
                self.comm_groups[LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len()]
                    .endpoints[pe]
                    .atomic_get(
                        zero.as_ref().get_ref(),
                        result.as_mut_ptr(),
                        remote_addr + offset,
                        &rkey,
                    )
            }
            AtomicOp::Write(_)
            | AtomicOp::FetchSum(_)
            | AtomicOp::FetchSub(_)
            | AtomicOp::FetchBitAnd(_)
            | AtomicOp::FetchBitOr(_)
            | AtomicOp::FetchBitXor(_) => {
                let (ucx_op, val) = Self::ucx_atomic_update(op);
                let (remote_addr, rkey) = &self.remote_keys[pe];
                self.comm_groups[LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len()]
                    .endpoints[pe]
                    .atomic_fetch_op(
                        ucx_op,
                        val,
                        result.as_mut_ptr(),
                        remote_addr + offset,
                        &rkey,
                    )
            }
            AtomicOp::Min(_)
            | AtomicOp::Max(_)
            | AtomicOp::Sum(_)
            | AtomicOp::Sub(_)
            | AtomicOp::Prod(_)
            | AtomicOp::BitAnd(_)
            | AtomicOp::BitOr(_)
            | AtomicOp::BitXor(_) => {
                panic!("Non-fetch atomic ops must use the non-fetch path")
            }
            AtomicOp::Cas => {
                panic!("Compare atomic ops must use the compare path")
            }
            _ => panic!("Unsupported atomic operation"),
        };
        if blocking {
            req.wait().expect("atomic_fetch_op_blocking failed");
            None
        } else {
            Some(req)
        }
    }

    pub(crate) fn inner_atomic_compare_exchange_op<T: Copy + 'static>(
        &self,
        pe: usize,
        offset: usize,
        blocking: bool,
        compare: *const T,
        result: &mut [T],
    ) -> Option<UcxRequest> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let (remote_addr, rkey) = &self.remote_keys[pe];
        // result[0] must be pre-initialized to `new` (Z) before calling;
        // after completion it holds the original remote value (old Y).
        let req = self.comm_groups[LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len()]
            .endpoints[pe]
            .atomic_compare_swap(compare, result.as_mut_ptr(), remote_addr + offset, &rkey);
        if blocking {
            req.wait()
                .expect("atomic_compare_exchange_blocking_op failed");
            None
        } else {
            Some(req)
        }
    }

    pub(crate) fn allgather_inner<T: Copy + 'static>(
        &self,
        src: &[T],
        result: &mut [T],
        blocking: bool,
    ) -> Result<Option<UccRequest>, ucc::Error> {
        if let Some(ucc_team) = &self.comm_groups[LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len()].ucc_world_team {
            let req = ucc_team
                .allgather(src, result)?;

            if blocking {
                self.wait_ucc_request(&req)?;
                Ok(None)
            } else {
                Ok(Some(req))
            }
        } else {
            panic!("UCC team not initialized for allgather");
        }
    }


    pub(crate) fn allreduce_inner<T: Copy + 'static>(
        &self,
        op: &AllReduceOp,
        src: &[T],
        result: &mut [T],
        blocking: bool,
    ) -> Result<Option<UccRequest>, ucc::Error> {
        if let Some(ucc_team) = &self.comm_groups[LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len()].ucc_world_team {
            let req = ucc_team
                .allreduce(src, result, op.clone())?;
            if blocking {
                self.wait_ucc_request(&req)?;
                Ok(None)
            } else {
                Ok(Some(req))
            }
        } else {
            panic!("UCC team not initialized for allreduce");
        }
    }

    pub(crate) fn allreduce_inplace_inner<T: Copy + 'static>(
        &self,
        op: &AllReduceOp,
        src_and_result: &mut [T],
        blocking: bool,
    ) -> Result<Option<UccRequest>, ucc::Error> {
        let src = unsafe { std::slice::from_raw_parts(src_and_result.as_ptr(), src_and_result.len()) };
        self.allreduce_inner(op, src, src_and_result, blocking)
    }

    pub(crate) fn alltoall_inner<T: Copy + 'static>(
        &self,
        src: &[T],
        result: &mut [T],
        blocking: bool,
    ) -> Result<Option<UccRequest>, ucc::Error> {
        if let Some(ucc_team) = &self.comm_groups[LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len()].ucc_world_team {
            let req = ucc_team.alltoall(src, result)?;
            if blocking {
                self.wait_ucc_request(&req)?;
                Ok(None)
            } else {
                Ok(Some(req))
            }
        } else {
            panic!("UCC team not initialized for alltoall");
        }
    }

    pub(crate) fn reduce_inner<T: Copy + 'static>(
        &self,
        op: &AllReduceOp,
        src: &[T],
        slice_or_pe: RootOrSliceMut<'_, T>,
        blocking: bool,
    ) -> Result<Option<UccRequest>, ucc::Error> {
        if let Some(ucc_team) = &self.comm_groups[LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len()].ucc_world_team {
            let (result, root_pe) = match slice_or_pe {
                RootOrSliceMut::Root(result) => (Some(result), self.my_pe),
                RootOrSliceMut::NotRoot(root_pe) => (None, root_pe),
            };
            let res = match result {
                Some(res) => res,
                None => unsafe {
                    std::slice::from_raw_parts_mut(src.as_ptr() as *mut T, src.len())
                },
            };

            let req = ucc_team.reduce(src, res, root_pe, op.clone())?;
            if blocking {
                self.wait_ucc_request(&req)?;
                Ok(None)
            } else {
                Ok(Some(req))
            }
        } else {
            panic!("UCC team not initialized for reduce");
        }
    }

    pub(crate) fn gather_inner<T: Copy + 'static>(
        &self,
        src: &[T],
        slice_or_pe: RootOrSliceMut<'_, T>,
        blocking: bool,
    ) -> Result<Option<UccRequest>, ucc::Error> {
        if let Some(ucc_team) = &self.comm_groups[LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len()].ucc_world_team {
            let (result, root_pe) = match slice_or_pe {
                RootOrSliceMut::Root(result) => (Some(result), self.my_pe),
                RootOrSliceMut::NotRoot(root_pe) => (None, root_pe),
            };
            let res = match result {
                Some(res) => res,
                None => unsafe {
                    std::slice::from_raw_parts_mut(src.as_ptr() as *mut T, src.len())
                },
            };

            let req = ucc_team.gather(src, res, root_pe)?;
            if blocking {
                self.wait_ucc_request(&req)?;
                Ok(None)
            } else {
                Ok(Some(req))
            }
        } else {
            panic!("UCC team not initialized for gather");
        }
    }

    pub(crate) fn broadcast_inner<T: Copy + 'static>(
        &self,
        root_src: RootSrcOrSliceMut<'_, T>,
        blocking: bool,
    ) -> Result<Option<UccRequest>, ucc::Error> {
        if let Some(ucc_team) = &self.comm_groups[LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len()].ucc_world_team {
            let (res, root_pe) = match root_src {
                RootSrcOrSliceMut::Root(src) => {
                    (unsafe { std::slice::from_raw_parts_mut(src.as_ptr() as *mut T, src.len()) }, self.my_pe)
                }
                RootSrcOrSliceMut::NotRoot(result, root_pe) => (result, root_pe),
            };

            let src = unsafe { std::slice::from_raw_parts(res.as_ptr(), res.len()) };
            let req = ucc_team.broadcast(src, res, root_pe)?;
            if blocking {
                self.wait_ucc_request(&req)?;
                Ok(None)
            } else {
                Ok(Some(req))
            }
        } else {
            panic!("UCC team not initialized for broadcast");
        }
    }

    pub(crate) fn scatter_inner<T: Copy + 'static>(
        &self,
        res: &mut [T],
        src_or_root_pe: RootSrcSliceOrNone<'_, T>,
        blocking: bool,
    ) -> Result<Option<UccRequest>, ucc::Error> {
        if let Some(ucc_team) = &self.comm_groups[LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len()].ucc_world_team {
            let (src, root_pe) = match src_or_root_pe {
                RootSrcSliceOrNone::Root(src) => (src, self.my_pe),
                RootSrcSliceOrNone::NotRoot(root_pe) => {
                    (unsafe { std::slice::from_raw_parts(res.as_ptr(), res.len()) }, root_pe)
                }
            };

            let req = ucc_team.scatter(src, res, root_pe)?;
            if blocking {
                self.wait_ucc_request(&req)?;
                Ok(None)
            } else {
                Ok(Some(req))
            }
        } else {
            panic!("UCC team not initialized for scatter");
        }
    }

    pub(crate) fn reduce_scatter_inner<T: Copy + 'static>(
        &self,
        op: &AllReduceOp,
        src: &[T],
        result: &mut [T],
        blocking: bool,
    ) -> Result<Option<UccRequest>, ucc::Error> {
        if let Some(ucc_team) = &self.comm_groups[LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len()].ucc_world_team {
            let req = ucc_team.reduce_scatter(src, result, op.clone())?;
            if blocking {
                self.wait_ucc_request(&req)?;
                Ok(None)
            } else {
                Ok(Some(req))
            }
        } else {
            panic!("UCC team not initialized for reduce_scatter");
        }
    }

    pub(crate) unsafe fn as_mut_slice<T>(&self) -> &mut [T] {
        self.mem.as_mut_slice()
    }

    pub(crate) unsafe fn as_slice<T>(&self) -> &[T] {
        self.mem.as_slice()
    }

    pub(crate) fn wait_ucc_request(&self, req: &UccRequest) -> Result<(), ucc::Error> {
        if let Some(ucc_team) = &self.comm_groups[LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len()].ucc_world_team {
            while let Err(err) = req.test() {
                if !matches!(err, Error::Inprogress) {
                    return Err(err);
                } 
                ucc_team.context.progress()?;
            }
            Ok(())
        }
        else {
            panic!("UCC team not initialized for waiting on UCC request");
        }
    }

    pub(crate) fn wait_ucc_all(&self) {
        for comm_group in self.comm_groups.iter() {
            if let Some(ucc_team) = &comm_group.ucc_world_team {
                loop {
                    ucc_team.context.progress().unwrap();
                    let completed = ucc_team.req_completed.load(std::sync::atomic::Ordering::SeqCst);
                    let pending = ucc_team.req_pending.load(std::sync::atomic::Ordering::SeqCst);
                    if completed == pending {
                        break;
                    }
                    std::thread::yield_now();
                }
            }
        }
    }

    pub(crate) fn wait_all(&self) {
        for comm_group in self.comm_groups.iter() {
            comm_group
                .worker
                .wait_all()
                .expect("UcxMtAlloc::wait_all failed waiting on UCX requests");
        }

        self.wait_ucc_all();
    }

    #[allow(dead_code)] // WIP: called via trait dispatch, lint false-positive
    pub(crate) fn thread_wait(&self) {
        self.comm_groups[LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len()]
            .worker
            .wait_all()
            .expect("UcxMtAlloc::thread_wait failed waiting on UCX requests");

        if let Some(ucc_team) = &self.comm_groups[LAMELLAR_THREAD_ID.with(|id| *id) % self.comm_groups.len()].ucc_world_team {
            loop {
                ucc_team.context.progress().unwrap();
                let completed = ucc_team.req_completed.load(std::sync::atomic::Ordering::SeqCst);
                let pending = ucc_team.req_pending.load(std::sync::atomic::Ordering::SeqCst);
                if completed == pending {
                    break;
                }
                std::thread::yield_now();
            }
        }
    }

    pub(crate) fn wait(&self) {
        for comm_group in self.comm_groups.iter() {
            comm_group
                .worker
                .wait_all()
                .expect("UcxMtAlloc::wait_all failed waiting on UCX requests");
        }
        self.wait_ucc_all();
    }
}

impl Drop for UcxMtAlloc {
    fn drop(&mut self) {
        trace!(target: "ucx", "drop UcxMtAlloc mem: {:x} - ({:x}) {:x} {:?}",
                self.mem.addr,
                self.mem.addr + self.data_num_bytes,
                self.mem.addr + self.mem.size,
                self);
        let fabric_ref_count = self.decrement_fabric_ref_count();
        match &self.alloc_table {
            AllocTable::Fabric(mem_handles, remote_keys) => {
                if fabric_ref_count == 3 {
                    debug!(target: "ucx", "Dropping UCX alloc: {:?}", self);
                    //last reference, remove from world tracking
                    mem_handles
                        .lock()
                        .unwrap()
                        .retain(|a| a.mem.inner.addr != self.mem.inner.addr);
                    remote_keys
                        .lock()
                        .unwrap()
                        .retain(|(a, _)| a.mem.inner.addr != self.mem.inner.addr);
                }
                if fabric_ref_count == 1 {
                    let mem_handles_count = Arc::strong_count(&mem_handles);
                    let mem_handle_count = Arc::strong_count(&self.mem.inner);
                    let remote_keys_count = Arc::strong_count(&remote_keys);
                    debug!("Dropping UCX alloc:  mem_handles_count: {}, mem_handle_count: {}, remote_keys_count: {}",
                    mem_handles_count, mem_handle_count, remote_keys_count);
                }
            }
            AllocTable::Runtime(rt_alloc_table, addr, mem_handles, remote_keys) => {
                let rt_ref_cnt = self.decrement_rt_ref_count();
                if rt_ref_cnt == 1 {
                    debug!(target: "ucx", "Dropping UCX rt-alloc: {:?}", self);
                    //last rt reference, free from alloc table
                    rt_alloc_table.free(*addr).expect(&format!(
                        "[{:?}] Error removing from runtime alloc table {:x}",
                        std::thread::current().id(),
                        addr
                    ));
                }
                if fabric_ref_count == 3 {
                    debug!(target: "ucx", "Dropping UCX alloc (from RT): {:?}", self);
                    //last reference, remove from world tracking
                    mem_handles
                        .lock()
                        .unwrap()
                        .retain(|a| a.mem.inner.addr != self.mem.inner.addr);
                    remote_keys
                        .lock()
                        .unwrap()
                        .retain(|(a, _)| a.mem.inner.addr != self.mem.inner.addr);
                }
                // if fabric_ref_count == 1 {
                //     let mem_handles_count = Arc::strong_count(&mem_handles);
                //     let mem_handle_count = Arc::strong_count(&self.mem.inner);
                //     let remote_keys_count = Arc::strong_count(&remote_keys);
                //     debug!("Dropping UCX alloc:  mem_handles_count: {}, mem_handle_count: {}, remote_keys_count: {}",
                //     mem_handles_count, mem_handle_count, remote_keys_count);
                // }
            }
        }
        trace!(target: "drop", "end drop UcxMtAlloc");
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OneSidedUcxMtAlloc {
    pub(crate) remote_pe: usize,
    pub(crate) alloc: UcxMtAlloc,
}

impl OneSidedUcxMtAlloc {
    pub(crate) fn num_bytes(&self) -> usize {
        self.alloc.num_bytes()
    }
    pub(crate) fn start(&self) -> usize {
        self.alloc.start()
    }
    pub(crate) fn sub_alloc(&self, offset: usize, size: usize) -> AllocResult<Self> {
        Ok(OneSidedUcxMtAlloc {
            remote_pe: self.remote_pe,
            alloc: self.alloc.sub_alloc(offset, size)?,
        })
    }
}

impl From<OneSidedUcxMtAlloc> for CommAlloc {
    fn from(alloc: OneSidedUcxMtAlloc) -> Self {
        CommAlloc {
            inner_alloc: Arc::new(CommAllocInner::OneSidedUcxMtAlloc(alloc)),
            // alloc_type: CommAllocType::Remote,
        }
    }
}

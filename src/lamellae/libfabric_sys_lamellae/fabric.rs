use std::{
    collections::HashMap,
    mem::MaybeUninit,
    ops::{Range, RangeFrom, RangeFull, RangeTo},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use libfabric_sys;
use parking_lot::{Mutex, RwLock};
use pmi::{Pmi, PmiBuilder};
use tracing::{debug, trace};

use crate::{
    lamellae::{
        calc_alloc_padding_size_align,
        collective::{
            AllReduceOp, ReduceOp, RootOrSliceMut, RootSrcOrSliceMut, RootSrcSliceOrNone,
        },
        decode_padding, decode_ref_count, decode_ref_count_and_padding, decrement_ref_count,
        encode_ref_count_and_padding, get_ref_count, increment_ref_count, AllocError, AllocResult,
        AllocationType, AtomicOp, CollectiveOpKind, CommAlloc, CommAllocAddr, CommAllocInner,
        FabricError, FabricResult,
    },
    lamellar_alloc::{BTreeAlloc, LamellarAlloc},
};

enum BarrierImpl {
    Uninit,
    Collective(*mut libfabric_sys::fid_mc, u64),
    Manual(LibfabricSysAlloc, AtomicUsize),
    Pmi(Arc<dyn Pmi>),
}

#[derive(Clone, Copy)]
enum AtomicOpKind {
    Min,
    Max,
    Sum,
    Prod,
    BitOr,
    BitXor,
    BitAnd,
    Read,
    Write,
    Cas,
}

pub(crate) struct CommGroup {
    mapped_addresses: Vec<u64>,
    ep: *mut libfabric_sys::fid_ep,
    cq: *mut libfabric_sys::fid_cq,
    put_cntr: *mut libfabric_sys::fid_cntr,
    get_cntr: *mut libfabric_sys::fid_cntr,
    av: *mut libfabric_sys::fid_av,
    eq: *mut libfabric_sys::fid_eq,
    info_entry: *mut libfabric_sys::fi_info,
    put_cnt: AtomicU64,
    get_cnt: AtomicU64,
    coll_cnt_issued: AtomicU64,
    coll_cnt_completed: AtomicU64,
    barrier_impl: RwLock<BarrierImpl>,
    lock: Mutex<()>,
}

unsafe impl Send for CommGroup {}
unsafe impl Sync for CommGroup {}

impl CommGroup {
    fn wait_for_join_event(&self, ctx: *mut std::ffi::c_void) {
        let _lock = self.lock.lock();
        loop {
            let mut event_entry = MaybeUninit::<libfabric_sys::fi_eq_entry>::uninit();
            let mut event_type = 0;
            let ret = unsafe {
                libfabric_sys::inlined_fi_eq_read(
                    self.eq,
                    &mut event_type,
                    event_entry.as_mut_ptr() as *mut libc::c_void,
                    std::mem::size_of::<libfabric_sys::fi_eq_cm_entry>(),
                    0,
                )
            };

            if ret >= 0 {
                let entry = unsafe { event_entry.assume_init() };
                if event_type == libfabric_sys::FI_JOIN_COMPLETE && entry.context == ctx {
                    return;
                }
            } else if ret != -(libfabric_sys::FI_EAGAIN as isize) {
                panic!("Error reading EQ: {}", ret);
            }

            self.progress();
        }
    }

    fn progress(&self) {
        let mut cq_entry = MaybeUninit::<libfabric_sys::fi_cq_entry>::uninit();
        let ret = unsafe {
            libfabric_sys::inlined_fi_cq_read(
                self.cq,
                cq_entry.as_mut_ptr() as *mut libc::c_void,
                1,
            )
        };

        if ret >= 0 {
            self.coll_cnt_completed.fetch_add(1, Ordering::SeqCst);
        } else if ret != -(libfabric_sys::FI_EAGAIN as isize) {
            panic!("Error reading CQ: {}", ret);
        }
    }

    pub(crate) fn wait_all(&self) {
        trace!("wait_all");
        self.wait_for_tx_cntr();
        trace!("wait_all put done");
        self.wait_for_rx_cntr();
        trace!("wait_all get done");
        self.wait_for_collectives();
        trace!("wait_all collective done");
        trace!("wait_all done");
    }

    fn wait_for_tx_cntr(&self) {
        self.wait_for_cntr(&self.put_cnt, self.put_cntr, "tx");
    }

    fn wait_for_rx_cntr(&self) {
        self.wait_for_cntr(&self.get_cnt, self.get_cntr, "rx");
    }

    fn wait_for_collectives(&self) {
        let _lock = self.lock.lock();
        let mut prev_expected_cnt = self.coll_cnt_issued.load(Ordering::SeqCst);
        let mut old_cnt = self.coll_cnt_completed.load(Ordering::SeqCst);
        let mut expected_cnt = self.coll_cnt_issued.load(Ordering::SeqCst);
        let mut cur_cnt = self.coll_cnt_completed.load(Ordering::SeqCst);

        while cur_cnt < expected_cnt || prev_expected_cnt < expected_cnt || cur_cnt != old_cnt {
            prev_expected_cnt = expected_cnt;
            old_cnt = cur_cnt;
            self.progress();
            while self.coll_cnt_completed.load(Ordering::SeqCst)
                < self.coll_cnt_issued.load(Ordering::SeqCst)
            {
                self.progress();
                std::thread::yield_now();
            }

            cur_cnt = self.coll_cnt_completed.load(Ordering::SeqCst);
            expected_cnt = self.coll_cnt_issued.load(Ordering::SeqCst);
            std::thread::yield_now();
        }
    }

    fn wait_for_cntr(&self, pending: &AtomicU64, cntr: *mut libfabric_sys::fid_cntr, _dir: &str) {
        let _lock = self.lock.lock();
        let mut prev_expected_cnt = pending.load(Ordering::SeqCst);
        let mut old_cnt = unsafe { libfabric_sys::inlined_fi_cntr_read(cntr) as u64 };
        let mut expected_cnt = pending.load(Ordering::SeqCst);
        let mut cur_cnt = unsafe { libfabric_sys::inlined_fi_cntr_read(cntr) as u64 };

        while cur_cnt < expected_cnt || prev_expected_cnt < expected_cnt || cur_cnt != old_cnt {
            prev_expected_cnt = expected_cnt;
            old_cnt = cur_cnt;

            self.progress();
            unsafe {
                let ret = libfabric_sys::inlined_fi_cntr_wait(cntr, prev_expected_cnt as u64, -1);
                if ret != 0 {
                    panic!("Error waiting on {} counter: {}", _dir, ret);
                }
            }

            cur_cnt = unsafe { libfabric_sys::inlined_fi_cntr_read(cntr) as u64 };
            expected_cnt = pending.load(Ordering::SeqCst);
            std::thread::yield_now();
        }
    }

    fn post_collective<F>(&self, blocking: bool, mut fun: F)
    where
        F: FnMut() -> isize,
    {
        let _lock = self.lock.lock();
        let cnt = self.coll_cnt_issued.fetch_add(1, Ordering::SeqCst) + 1;
        loop {
            let ret = fun();
            if ret >= 0 {
                break;
            } else if ret == -(libfabric_sys::FI_EAGAIN as isize) {
                self.progress();
            } else {
                panic!("Error posting collective: {}", ret);
            }
        }
        if blocking {
            loop {
                if self.coll_cnt_completed.load(Ordering::SeqCst) >= cnt {
                    break;
                }
                self.progress();
            }
        }
    }

    fn post_put<F>(&self, blocking: bool, mut fun: F) -> u64
    where
        F: FnMut() -> isize,
    {
        let _lock = self.lock.lock();
        let old_put_cntr = unsafe { libfabric_sys::inlined_fi_cntr_read(self.put_cntr) as u64 };
        self.put_cnt.fetch_max(old_put_cntr, Ordering::SeqCst);
        loop {
            let ret = fun();
            if ret == 0 {
                break;
            } else if ret == -(libfabric_sys::FI_EAGAIN as isize) {
                self.progress();
            } else {
                panic!("Error posting put: {}", ret);
            }
        }
        let cnt = self.put_cnt.fetch_add(1, Ordering::SeqCst) + 1;
        if blocking {
            unsafe {
                let _ = libfabric_sys::inlined_fi_cntr_wait(self.put_cntr, cnt as u64, -1);
            }
        }
        cnt
    }

    fn post_get<F>(&self, blocking: bool, mut fun: F) -> u64
    where
        F: FnMut() -> isize,
    {
        let _lock = self.lock.lock();
        let old_get_cntr = unsafe { libfabric_sys::inlined_fi_cntr_read(self.get_cntr) as u64 };
        let old_cnt = self.get_cnt.fetch_max(old_get_cntr, Ordering::SeqCst);
        loop {
            let ret = fun();
            if ret >= 0 {
                break;
            } else if ret == -(libfabric_sys::FI_EAGAIN as isize) {
                self.progress();
            } else {
                panic!("Error posting get: {}", ret);
            }
        }
        let new_cnt = self.get_cnt.fetch_add(1, Ordering::SeqCst) + 1;
        trace!(target: "libfabric-sys", "done posting get {} {}", old_cnt, new_cnt);

        if blocking {
            unsafe {
                let _ = libfabric_sys::inlined_fi_cntr_wait(self.get_cntr, new_cnt as u64, -1);
            }
        }
        new_cnt
    }
}

#[derive(Clone)]
pub(crate) enum LibfabricSysMem {
    Mmap(Arc<memmap::MmapMut>),
    #[cfg(feature = "enable-on-node-shmem")]
    Shmem(Arc<ShmemSegment>),
}

impl LibfabricSysMem {
    fn as_ptr(&self) -> *mut u8 {
        match self {
            LibfabricSysMem::Mmap(mem) => mem.as_ptr() as *mut u8,
            #[cfg(feature = "enable-on-node-shmem")]
            LibfabricSysMem::Shmem(segment) => segment.base_ptr(),
        }
    }

    fn len(&self) -> usize {
        match self {
            LibfabricSysMem::Mmap(mem) => mem.len(),
            #[cfg(feature = "enable-on-node-shmem")]
            LibfabricSysMem::Shmem(segment) => segment.len(),
        }
    }

    #[allow(dead_code)]
    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }

    #[allow(dead_code)]
    fn as_mut_slice(&self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_ptr(), self.len()) }
    }
}

pub(crate) struct Ofi {
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    domain: *mut libfabric_sys::fid_domain,
    #[allow(dead_code)] // WIP: held for ownership/lifetime, not yet read
    fabric: *mut libfabric_sys::fid_fabric,
    _my_pmi: Arc<dyn Pmi>,
    alloc_manager: Arc<AllocInfoManager>,
    comm_group: CommGroup,
}

unsafe impl Send for Ofi {}
unsafe impl Sync for Ofi {}

impl std::fmt::Debug for Ofi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ofi")
            .field("num_pes", &self.num_pes)
            .field("my_pe", &self.my_pe)
            .finish()
    }
}

impl Ofi {
    pub(crate) fn new(provider: Option<&str>, domain: Option<&str>) -> FabricResult<Arc<Self>> {
        let my_pmi = Arc::new(PmiBuilder::init().map_err(|e| {
            eprintln!("Error initializing PMI: {:?}", e);
            FabricError::InitError(1)
        })?);

        let num_pes = my_pmi.ranks().len();

        let info = unsafe {
            let hints = libfabric_sys::inlined_fi_allocinfo();
            (*hints).caps = (libfabric_sys::FI_RMA
                | libfabric_sys::FI_ATOMIC
                | libfabric_sys::FI_COLLECTIVE) as u64;
            (*hints).mode = libfabric_sys::FI_CONTEXT;
            (*(*hints).ep_attr).type_ = libfabric_sys::fi_ep_type_FI_EP_RDM;
            (*(*hints).domain_attr).threading = libfabric_sys::fi_threading_FI_THREAD_SAFE;
            // (*(*hints).domain_attr).control_progress = libfabric_sys::fi_progress_FI_PROGRESS_AUTO as u8;
            (*(*hints).domain_attr).data_progress = libfabric_sys::fi_progress_FI_PROGRESS_MANUAL;
            (*(*hints).domain_attr).mr_mode = (libfabric_sys::FI_MR_PROV_KEY
                | libfabric_sys::FI_MR_VIRT_ADDR
                | libfabric_sys::FI_MR_ALLOCATED)
                as i32;
            (*(*hints).domain_attr).resource_mgmt = libfabric_sys::fi_resource_mgmt_FI_RM_ENABLED;
            (*(*hints).tx_attr).tclass = libfabric_sys::FI_TC_LOW_LATENCY;
            (*(*hints).tx_attr).op_flags = (libfabric_sys::FI_DELIVERY_COMPLETE) as u64;
            (*hints).addr_format = libfabric_sys::FI_FORMAT_UNSPEC;
            let version = 1 << 16 | 22;
            let mut c_info = MaybeUninit::<*mut libfabric_sys::fi_info>::uninit();
            libfabric_sys::fi_getinfo(
                version,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                0,
                hints,
                c_info.as_mut_ptr(),
            );
            let info = c_info.assume_init();
            let mut curr_info = info;
            while !curr_info.is_null() {
                if !provider.is_none()
                    && !std::ffi::CStr::from_ptr(
                        (*curr_info).fabric_attr.as_ref().unwrap().prov_name,
                    )
                    .to_str()
                    .unwrap()
                    .split(';')
                    .any(|p| p == provider.unwrap())
                {
                    curr_info = (*curr_info).next;
                    continue;
                }
                if !domain.is_none()
                    && !std::ffi::CStr::from_ptr((*curr_info).domain_attr.as_ref().unwrap().name)
                        .to_str()
                        .unwrap()
                        .split(';')
                        .any(|d| d == domain.unwrap())
                {
                    curr_info = (*curr_info).next;
                    continue;
                }
                break;
            }
            let ret: *mut libfabric_sys::fi_info = libfabric_sys::fi_dupinfo(curr_info);
            libfabric_sys::fi_freeinfo(hints);
            libfabric_sys::fi_freeinfo(info);
            ret
        };

        let fabric = unsafe {
            let mut fabric = MaybeUninit::<*mut libfabric_sys::fid_fabric>::uninit();
            let ret = libfabric_sys::fi_fabric(
                (*info).fabric_attr,
                fabric.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if ret != 0 {
                eprintln!("Error creating fabric: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            fabric.assume_init()
        };

        let domain = unsafe {
            let mut domain = MaybeUninit::<*mut libfabric_sys::fid_domain>::uninit();
            let ret = libfabric_sys::inlined_fi_domain(
                fabric,
                info,
                domain.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if ret != 0 {
                eprintln!("Error creating domain: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            domain.assume_init()
        };

        let eq = unsafe {
            let mut eq = MaybeUninit::<*mut libfabric_sys::fid_eq>::uninit();
            let mut eq_attr = libfabric_sys::fi_eq_attr {
                size: 0,
                flags: 0,
                wait_obj: libfabric_sys::fi_wait_obj_FI_WAIT_UNSPEC,
                signaling_vector: 0,
                wait_set: std::ptr::null_mut(),
            };

            let ret = libfabric_sys::inlined_fi_eq_open(
                fabric,
                &mut eq_attr,
                eq.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if ret != 0 {
                eprintln!("Error creating EQ: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            eq.assume_init()
        };

        let cq = unsafe {
            let mut cq = MaybeUninit::<*mut libfabric_sys::fid_cq>::uninit();
            let mut cq_attr = libfabric_sys::fi_cq_attr {
                size: (*(*info).rx_attr).size,
                flags: 0,
                format: libfabric_sys::fi_cq_format_FI_CQ_FORMAT_CONTEXT,
                wait_obj: libfabric_sys::fi_wait_obj_FI_WAIT_UNSPEC,
                signaling_vector: 0,
                wait_cond: libfabric_sys::fi_cq_wait_cond_FI_CQ_COND_NONE,
                wait_set: std::ptr::null_mut(),
            };
            let ret = libfabric_sys::inlined_fi_cq_open(
                domain,
                &mut cq_attr,
                cq.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if ret != 0 {
                eprintln!("Error creating CQ: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            cq.assume_init()
        };

        let av = unsafe {
            let mut av = MaybeUninit::<*mut libfabric_sys::fid_av>::uninit();
            let mut av_attr = libfabric_sys::fi_av_attr {
                type_: libfabric_sys::fi_av_type_FI_AV_UNSPEC,
                rx_ctx_bits: 0,
                count: 0,
                ep_per_node: 0,
                name: std::ptr::null(),
                map_addr: std::ptr::null_mut(),
                flags: 0,
            };
            let ret = libfabric_sys::inlined_fi_av_open(
                domain,
                &mut av_attr,
                av.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if ret != 0 {
                eprintln!("Error creating AV: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            av.assume_init()
        };

        let put_cntr = unsafe {
            let mut cntr = MaybeUninit::<*mut libfabric_sys::fid_cntr>::uninit();
            let mut cntr_attr = libfabric_sys::fi_cntr_attr {
                events: libfabric_sys::fi_cntr_events_FI_CNTR_EVENTS_COMP,
                wait_obj: libfabric_sys::fi_wait_obj_FI_WAIT_UNSPEC,
                wait_set: std::ptr::null_mut(),
                flags: 0,
            };
            let ret = libfabric_sys::inlined_fi_cntr_open(
                domain,
                &mut cntr_attr,
                cntr.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if ret != 0 {
                eprintln!("Error creating put counter: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            cntr.assume_init()
        };

        let get_cntr = unsafe {
            let mut cntr = MaybeUninit::<*mut libfabric_sys::fid_cntr>::uninit();
            let mut cntr_attr = libfabric_sys::fi_cntr_attr {
                events: libfabric_sys::fi_cntr_events_FI_CNTR_EVENTS_COMP,
                wait_obj: libfabric_sys::fi_wait_obj_FI_WAIT_UNSPEC,
                wait_set: std::ptr::null_mut(),
                flags: 0,
            };
            let ret = libfabric_sys::inlined_fi_cntr_open(
                domain,
                &mut cntr_attr,
                cntr.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if ret != 0 {
                eprintln!("Error creating put counter: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            cntr.assume_init()
        };

        let ep = unsafe {
            let mut ep = MaybeUninit::<*mut libfabric_sys::fid_ep>::uninit();
            let ret = libfabric_sys::inlined_fi_endpoint(
                domain,
                info,
                ep.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if ret != 0 {
                eprintln!("Error creating endpoint: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            ep.assume_init()
        };

        unsafe {
            let ret = libfabric_sys::inlined_fi_ep_bind(ep, &mut (*eq).fid, 0);
            if ret != 0 {
                eprintln!("Error binding EQ: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            let flags = (libfabric_sys::FI_TRANSMIT | libfabric_sys::FI_RECV) as u64
                | libfabric_sys::FI_SELECTIVE_COMPLETION;
            let ret = libfabric_sys::inlined_fi_ep_bind(ep, &mut (*cq).fid, flags);
            if ret != 0 {
                eprintln!("Error binding CQ: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            let ret = libfabric_sys::inlined_fi_ep_bind(ep, &mut (*av).fid, 0);
            if ret != 0 {
                eprintln!("Error binding AV: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            let ret = libfabric_sys::inlined_fi_ep_bind(
                ep,
                &mut (*put_cntr).fid,
                libfabric_sys::FI_WRITE as u64,
            );
            if ret != 0 {
                eprintln!("Error binding put counter: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            let ret = libfabric_sys::inlined_fi_ep_bind(
                ep,
                &mut (*get_cntr).fid,
                libfabric_sys::FI_READ as u64,
            );
            if ret != 0 {
                eprintln!("Error binding get counter: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
        }

        unsafe {
            let ret = libfabric_sys::inlined_fi_enable(ep);
            if ret != 0 {
                eprintln!("Error enabling endpoint: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
        }
        let address_bytes = unsafe {
            let mut len = 0;
            let ret =
                libfabric_sys::inlined_fi_getname(&mut (*ep).fid, std::ptr::null_mut(), &mut len);
            if ret != -(libfabric_sys::FI_ETOOSMALL as i32) {
                eprintln!("Error getting endpoint name: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            let mut addr = vec![0u8; len];
            let ret = libfabric_sys::inlined_fi_getname(
                &mut (*ep).fid,
                addr.as_mut_ptr().cast(),
                &mut len,
            );
            if ret != 0 {
                eprintln!("Error getting endpoint name: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            addr
        };

        my_pmi.put(&format!("epname"), &address_bytes).unwrap();
        my_pmi.exchange().unwrap();

        let unmapped_addresses: Vec<Vec<u8>> = my_pmi
            .ranks()
            .iter()
            .map(|r| my_pmi.get(&format!("epname"), &r).unwrap())
            .collect();

        let mapped_addresses = unsafe {
            let mut mapped_addresses = vec![0u64; unmapped_addresses.len()];
            let total_size = unmapped_addresses
                .iter()
                .fold(0, |acc, unmapped_addresses| acc + unmapped_addresses.len());
            let mut serialized: Vec<u8> = Vec::with_capacity(total_size);
            for a in unmapped_addresses {
                serialized.extend(a.iter())
            }
            let ret = libfabric_sys::inlined_fi_av_insert(
                av,
                serialized.as_mut_ptr().cast(),
                mapped_addresses.len(),
                mapped_addresses.as_mut_ptr(),
                0,
                std::ptr::null_mut(),
            );
            if ret < 0 {
                eprintln!("Error inserting addresses into AV: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            mapped_addresses
        };

        let comm_group = CommGroup {
            mapped_addresses,
            ep,
            cq,
            put_cntr,
            get_cntr,
            coll_cnt_completed: AtomicU64::new(0),
            coll_cnt_issued: AtomicU64::new(0),
            av,
            eq,
            info_entry: info,
            put_cnt: AtomicU64::new(0),
            get_cnt: AtomicU64::new(0),
            lock: Mutex::new(()),
            barrier_impl: RwLock::new(BarrierImpl::Pmi(my_pmi.clone())),
        };

        let alloc_manager = AllocInfoManager::new();
        let ofi = Arc::new(Self {
            num_pes,
            my_pe: my_pmi.rank(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_pes,
            #[cfg(feature = "enable-on-node-shmem")]
            disable_on_node_shmem,
            #[cfg(feature = "enable-on-node-shmem")]
            job_id,
            _my_pmi: my_pmi,
            // info_entry,
            fabric: fabric,
            domain,
            alloc_manager: Arc::new(alloc_manager),
            comm_group,
        });

        Ok(ofi)
    }

    fn atomic_avail_inner<T: 'static>(&self) -> bool {
        let cg = &self.comm_group;
        let data_type = if let Some(dt) = rust_type_to_fi_type::<T>() {
            dt
        } else {
            return false;
        };
        let mut count: usize = 0;
        let mut avail = true;
        let ret = unsafe {
            libfabric_sys::inlined_fi_atomicvalid(
                cg.ep,
                data_type,
                libfabric_sys::fi_op_FI_SUM,
                &mut count as *mut usize,
            )
        };
        avail &= if ret != 0 { false } else { true };

        let ret = unsafe {
            libfabric_sys::inlined_fi_fetch_atomicvalid(
                cg.ep,
                data_type,
                libfabric_sys::fi_op_FI_ATOMIC_READ,
                &mut count as *mut usize,
            )
        };
        avail &= if ret != 0 { false } else { true };

        let ret = unsafe {
            libfabric_sys::inlined_fi_compare_atomicvalid(
                cg.ep,
                data_type,
                libfabric_sys::fi_op_FI_CSWAP,
                &mut count as *mut usize,
            )
        };
        avail &= if ret != 0 { false } else { true };
        avail
    }

    fn atomic_op_avail_inner<T: 'static>(&self, op: AtomicOpKind) -> bool {
        let cg = &self.comm_group;
        let data_type = if let Some(dt) = rust_type_to_fi_type::<T>() {
            dt
        } else {
            return false;
        };
        let mut count: usize = 0;
        let fi_op = match op {
            AtomicOpKind::Min => libfabric_sys::fi_op_FI_MIN,
            AtomicOpKind::Max => libfabric_sys::fi_op_FI_MAX,
            AtomicOpKind::Sum => libfabric_sys::fi_op_FI_SUM,
            AtomicOpKind::Prod => libfabric_sys::fi_op_FI_PROD,
            AtomicOpKind::BitAnd => libfabric_sys::fi_op_FI_BAND,
            AtomicOpKind::BitOr => libfabric_sys::fi_op_FI_BOR,
            AtomicOpKind::BitXor => libfabric_sys::fi_op_FI_BXOR,
            AtomicOpKind::Read => libfabric_sys::fi_op_FI_ATOMIC_READ,
            AtomicOpKind::Write => libfabric_sys::fi_op_FI_ATOMIC_WRITE,
            AtomicOpKind::Cas => libfabric_sys::fi_op_FI_CSWAP,
        };

        let mut avail = true;
        let ret = unsafe {
            libfabric_sys::inlined_fi_atomicvalid(cg.ep, data_type, fi_op, &mut count as *mut usize)
        };
        avail &= if ret != 0 { false } else { true };

        let ret = unsafe {
            libfabric_sys::inlined_fi_fetch_atomicvalid(
                cg.ep,
                data_type,
                fi_op,
                &mut count as *mut usize,
            )
        };
        avail &= if ret != 0 { false } else { true };
        avail
    }

    pub(crate) fn atomic_avail<T: 'static>(&self) -> bool {
        let id = std::any::TypeId::of::<T>();
        if id == std::any::TypeId::of::<u8>() {
            self.atomic_avail_inner::<u8>()
        } else if id == std::any::TypeId::of::<u16>() {
            self.atomic_avail_inner::<u16>()
        } else if id == std::any::TypeId::of::<u32>() {
            self.atomic_avail_inner::<u32>()
        } else if id == std::any::TypeId::of::<u64>() {
            self.atomic_avail_inner::<u64>()
        } else if id == std::any::TypeId::of::<i8>() {
            self.atomic_avail_inner::<i8>()
        } else if id == std::any::TypeId::of::<i16>() {
            self.atomic_avail_inner::<i16>()
        } else if id == std::any::TypeId::of::<i32>() {
            self.atomic_avail_inner::<i32>()
        } else if id == std::any::TypeId::of::<i64>() {
            self.atomic_avail_inner::<i64>()
        } else if id == std::any::TypeId::of::<usize>() {
            self.atomic_avail_inner::<usize>()
        } else if id == std::any::TypeId::of::<isize>() {
            self.atomic_avail_inner::<isize>()
        } else {
            false
        }
    }

    pub(crate) fn atomic_op_avail<T: 'static>(&self, op: AtomicOp<T>) -> bool {
        let op_kind = match op {
            AtomicOp::Min(_) | AtomicOp::FetchMin(_) => AtomicOpKind::Min,
            AtomicOp::Max(_) | AtomicOp::FetchMax(_) => AtomicOpKind::Max,
            AtomicOp::Sum(_) | AtomicOp::FetchSum(_) => AtomicOpKind::Sum,
            AtomicOp::Sub(_) | AtomicOp::FetchSub(_) => AtomicOpKind::Sum, // Sub can be implemented as Add with negative value
            AtomicOp::Prod(_) | AtomicOp::FetchProd(_) => AtomicOpKind::Prod,
            AtomicOp::BitOr(_) | AtomicOp::FetchBitOr(_) => AtomicOpKind::BitOr,
            AtomicOp::BitXor(_) | AtomicOp::FetchBitXor(_) => AtomicOpKind::BitXor,
            AtomicOp::BitAnd(_) | AtomicOp::FetchBitAnd(_) => AtomicOpKind::BitAnd,
            AtomicOp::Read(_) => AtomicOpKind::Read,
            AtomicOp::Write(_) => AtomicOpKind::Write,
            AtomicOp::Cas => AtomicOpKind::Cas,
        };

        let id = std::any::TypeId::of::<T>();
        if id == std::any::TypeId::of::<u8>() {
            self.atomic_op_avail_inner::<u8>(op_kind)
        } else if id == std::any::TypeId::of::<u16>() {
            self.atomic_op_avail_inner::<u16>(op_kind)
        } else if id == std::any::TypeId::of::<u32>() {
            self.atomic_op_avail_inner::<u32>(op_kind)
        } else if id == std::any::TypeId::of::<u64>() {
            self.atomic_op_avail_inner::<u64>(op_kind)
        } else if id == std::any::TypeId::of::<i8>() {
            self.atomic_op_avail_inner::<i8>(op_kind)
        } else if id == std::any::TypeId::of::<i16>() {
            self.atomic_op_avail_inner::<i16>(op_kind)
        } else if id == std::any::TypeId::of::<i32>() {
            self.atomic_op_avail_inner::<i32>(op_kind)
        } else if id == std::any::TypeId::of::<i64>() {
            self.atomic_op_avail_inner::<i64>(op_kind)
        } else if id == std::any::TypeId::of::<usize>() {
            self.atomic_op_avail_inner::<usize>(op_kind)
        } else if id == std::any::TypeId::of::<isize>() {
            self.atomic_op_avail_inner::<isize>(op_kind)
        } else {
            false
        }
    }

    pub(crate) fn collective_avail<T: 'static>(&self, op: CollectiveOpKind) -> bool {
        let (fi_op, reduce_op) = match op {
            CollectiveOpKind::Barrier => (libfabric_sys::fi_collective_op_FI_BARRIER, None),
            CollectiveOpKind::Broadcast => (libfabric_sys::fi_collective_op_FI_BROADCAST, None),
            CollectiveOpKind::AllToAll => (libfabric_sys::fi_collective_op_FI_ALLTOALL, None),
            CollectiveOpKind::AllReduce(reduce_op) => (
                libfabric_sys::fi_collective_op_FI_ALLREDUCE,
                Some(reduce_op),
            ),
            CollectiveOpKind::AllGather => (libfabric_sys::fi_collective_op_FI_ALLGATHER, None),
            CollectiveOpKind::ReduceScatter(reduce_op) => (
                libfabric_sys::fi_collective_op_FI_REDUCE_SCATTER,
                Some(reduce_op),
            ),
            CollectiveOpKind::Reduce(reduce_op) => {
                (libfabric_sys::fi_collective_op_FI_REDUCE, Some(reduce_op))
            }
            CollectiveOpKind::Scatter => (libfabric_sys::fi_collective_op_FI_SCATTER, None),
            CollectiveOpKind::Gather => (libfabric_sys::fi_collective_op_FI_GATHER, None),
        };

        let data_type = if let Some(dt) = rust_type_to_fi_type::<T>() {
            dt
        } else {
            return false;
        };
        let mut attr = libfabric_sys::fi_collective_attr {
            op: 0,
            datatype: data_type,
            datatype_attr: libfabric_sys::fi_atomic_attr { count: 0, size: 0 },
            max_members: 0,
            mode: 0,
        };

        if let Some(reduce_op) = reduce_op {
            let reduce_fi_op = match reduce_op {
                ReduceOp::Min => libfabric_sys::fi_op_FI_MIN,
                ReduceOp::Max => libfabric_sys::fi_op_FI_MAX,
                ReduceOp::Sum => libfabric_sys::fi_op_FI_SUM,
                ReduceOp::Prod => libfabric_sys::fi_op_FI_PROD,
                ReduceOp::BitAnd => libfabric_sys::fi_op_FI_BAND,
                ReduceOp::BitOr => libfabric_sys::fi_op_FI_BOR,
                ReduceOp::BitXor => libfabric_sys::fi_op_FI_BXOR,
            };
            attr.op = reduce_fi_op;
        }
        return unsafe {
            libfabric_sys::inlined_fi_query_collective(self.domain, fi_op, &mut attr, 0)
        } == 0;
    }

    fn create_mc_group(&self, pes: &[usize]) -> (*mut libfabric_sys::fid_mc, u64) {
        let cg = &self.comm_group;
        let mut av_set_attr = libfabric_sys::fi_av_set_attr {
            flags: 0,
            count: pes.len(),
            start_addr: cg.mapped_addresses[pes[0]],
            end_addr: cg.mapped_addresses[pes[0]],
            stride: 1,
            comm_key_size: 0,
            comm_key: std::ptr::null_mut(),
        };
        let av_set = unsafe {
            let mut av_set = MaybeUninit::<*mut libfabric_sys::fid_av_set>::uninit();
            let res = libfabric_sys::inlined_fi_av_set(
                cg.av,
                &mut av_set_attr,
                av_set.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if res != 0 {
                panic!("Error creating AV set: {}", res);
            }
            av_set.assume_init()
        };

        for pe in pes.iter().skip(1) {
            let ret = unsafe {
                libfabric_sys::inlined_fi_av_set_insert(av_set, cg.mapped_addresses[*pe])
            };
            if ret < 0 {
                panic!("Error inserting address into AV set: {}", ret);
            }
        }

        let mut ctx = libfabric_sys::fi_context2 {
            internal: [std::ptr::null_mut(); 8],
        };

        let av_set_addr = unsafe {
            let mut addr: u64 = 0;
            let ret = libfabric_sys::inlined_fi_av_set_addr(av_set, &mut addr);
            if ret != 0 {
                panic!("Error getting AV set address: {}", ret);
            }
            addr
        };
        let mc = unsafe {
            let mut mc = MaybeUninit::<*mut libfabric_sys::fid_mc>::uninit();
            let ret = libfabric_sys::inlined_fi_join_collective(
                cg.ep,
                av_set_addr,
                av_set,
                0,
                mc.as_mut_ptr(),
                (&mut ctx) as *mut libfabric_sys::fi_context2 as *mut libc::c_void,
            );
            if ret != 0 {
                panic!("Error registering memory for MC group: {}", ret);
            }
            cg.wait_for_join_event(
                (&mut ctx) as *mut libfabric_sys::fi_context2 as *mut libc::c_void,
            );
            mc.assume_init()
        };
        (mc, av_set_addr)
    }

    fn collective_exchange_mr_info(
        &self,
        pes: &[usize],
        mem: &[u8],
        mr: *mut libfabric_sys::fid_mr,
    ) -> HashMap<usize, RemoteMemAddressInfo> {
        let (_mc, av_set_addr) = self.create_mc_group(pes);
        let cg = &self.comm_group;
        let addr = if unsafe { (*(*self.comm_group.info_entry).domain_attr).mr_mode }
            & (libfabric_sys::FI_MR_VIRT_ADDR | libfabric_sys::fi_mr_mode_FI_MR_BASIC) as i32
            != 0
        {
            mem.as_ptr() as u64
        } else {
            0u64
        };

        let addr_size = std::mem::size_of_val(mem);

        let mut key_bytes = unsafe {
            let mut base_addr = addr;
            let mut key_size = (*(*self.comm_group.info_entry).domain_attr).mr_key_size;
            let mut raw_key = vec![0u8; key_size + std::mem::size_of::<u64>()];
            if (*(*self.comm_group.info_entry).domain_attr).mr_mode
                & (libfabric_sys::FI_MR_RAW as i32)
                != 0
            {
                let err = libfabric_sys::inlined_fi_mr_raw_attr(
                    mr,
                    &mut base_addr,
                    raw_key.as_mut_ptr().cast(),
                    &mut key_size,
                    0,
                );

                if err != 0 {
                    panic!("Error getting raw MR key: {}", err);
                }

                let raw_key_len = raw_key.len();
                raw_key[raw_key_len - std::mem::size_of::<u64>()..].copy_from_slice(
                    std::slice::from_raw_parts(&base_addr as *const u64 as *const u8, 8),
                );
                raw_key
            } else {
                let key = libfabric_sys::inlined_fi_mr_key(mr);
                if key == u64::MAX {
                    panic!("Error getting MR key: {}", key);
                }

                let mut bytes = vec![0; std::mem::size_of::<u64>() + std::mem::size_of::<usize>()];
                bytes[..std::mem::size_of::<u64>()].copy_from_slice(std::slice::from_raw_parts(
                    &key as *const u64 as *const u8,
                    std::mem::size_of::<u64>(),
                ));
                bytes[std::mem::size_of::<u64>()..].copy_from_slice(std::slice::from_raw_parts(
                    &base_addr as *const u64 as *const u8,
                    std::mem::size_of::<usize>(),
                ));

                bytes
            }
        };

        key_bytes.extend(unsafe {
            std::slice::from_raw_parts(
                &addr_size as *const usize as *const u8,
                std::mem::size_of::<usize>(),
            )
        });

        let mut all_mem_info_bytes = vec![0u8; key_bytes.len() * pes.len()];
        cg.post_collective(true, || unsafe {
            libfabric_sys::inlined_fi_allgather(
                cg.ep,
                key_bytes.as_ptr().cast(),
                key_bytes.len(),
                std::ptr::null_mut(),
                all_mem_info_bytes.as_mut_ptr().cast(),
                std::ptr::null_mut(),
                av_set_addr,
                rust_type_to_fi_type::<u8>().unwrap(),
                0,
                std::ptr::null_mut(),
            )
        });

        let all_mem_info = all_mem_info_bytes
            .chunks_exact(key_bytes.len())
            .enumerate()
            .map(|(pe, chunk)| {
                let addr_len = unsafe {
                    *(chunk[chunk.len() - std::mem::size_of::<usize>()..].as_ptr() as *const u64)
                };
                let key_addr = unsafe {
                    *(chunk[chunk.len() - std::mem::size_of::<usize>() - std::mem::size_of::<u64>()
                        ..chunk.len() - std::mem::size_of::<usize>()]
                        .as_ptr() as *const u64)
                };
                let mut key = chunk
                    [..chunk.len() - std::mem::size_of::<usize>() - std::mem::size_of::<u64>()]
                    .to_vec();
                let mem_info = if unsafe { (*(*self.comm_group.info_entry).domain_attr).mr_mode }
                    & (libfabric_sys::FI_MR_RAW as i32)
                    != 0
                {
                    let mut mapped_key = 0u64;
                    let err = unsafe {
                        libfabric_sys::inlined_fi_mr_map_raw(
                            self.domain,
                            key_addr,
                            key.as_mut_ptr(),
                            key.len(),
                            &mut mapped_key,
                            0,
                        )
                    };
                    if err != 0 {
                        panic!("Error mapping raw MR key: {}", err);
                    }
                    RemoteMemAddressInfo {
                        mem_address: key_addr as *const u8,
                        key: mapped_key,
                        len: addr_len as usize,
                    }
                } else {
                    let mut key_mapped = 0u64;
                    unsafe {
                        std::slice::from_raw_parts_mut(&mut key_mapped as *mut u64 as *mut u8, 8)
                            .copy_from_slice(&key)
                    };
                    RemoteMemAddressInfo {
                        mem_address: key_addr as *const u8,
                        key: key_mapped,
                        len: addr_len as usize,
                    }
                };

                (pes[pe], mem_info)
            })
            .collect();

        all_mem_info
    }

    #[allow(dead_code)]
    fn init_barrier(self: &Arc<Ofi>) -> FabricResult<()> {
        let mut coll_attr = libfabric_sys::fi_collective_attr {
            op: 0,
            datatype: 0,
            datatype_attr: libfabric_sys::fi_atomic_attr { count: 0, size: 0 },
            max_members: 0,
            mode: 0,
        };

        let err = unsafe {
            libfabric_sys::inlined_fi_query_collective(
                self.domain,
                libfabric_sys::fi_collective_op_FI_BARRIER,
                &mut coll_attr,
                0,
            )
        };

        if err != 0 {
            let all_pes: Vec<_> = (0..self.num_pes).collect();
            let barrier_size = all_pes.len() * std::mem::size_of::<usize>();
            let barrier_addr = self
                .sub_alloc(&all_pes, barrier_size, std::mem::align_of::<usize>())
                .map_err(|e| {
                    if let AllocError::FabricAllocationError(err_no) = e {
                        FabricError::BarrierError(err_no as u32)
                    } else {
                        FabricError::BarrierError(u32::MAX)
                    }
                })?;

            *self.comm_group.barrier_impl.write() =
                BarrierImpl::Manual(barrier_addr, AtomicUsize::new(0));
            Ok(())
        } else {
            let all_pes: Vec<_> = (0..self.num_pes).collect();
            let (mc, av_set_addr) = self.create_mc_group(&all_pes);
            *self.comm_group.barrier_impl.write() = BarrierImpl::Collective(mc, av_set_addr);
            Ok(())
        }
    }

    pub(crate) fn clear_barrier(&self) {
        let mut barrier_impl = self.comm_group.barrier_impl.write();
        *barrier_impl = BarrierImpl::Uninit;
    }

    pub(crate) fn alloc(
        self: &Arc<Ofi>,
        size: usize,
        alloc: AllocationType,
        align: usize,
    ) -> AllocResult<LibfabricSysAlloc> {
        match alloc {
            AllocationType::Sub(pes) => self.sub_alloc(&pes, size, align),
            AllocationType::Global => self.full_alloc(size, align),
            _ => return Err(AllocError::UnexpectedAllocationType(alloc)),
        }
    }

    fn full_alloc(
        self: &Arc<Ofi>,
        data_size: usize,
        align: usize,
    ) -> AllocResult<LibfabricSysAlloc> {
        //add space for ref count and padding to align it
        let (padding, size, _align) = calc_alloc_padding_size_align(data_size, align);

        // Align to page boundaries
        let aligned_size = if (self.alloc_manager.page_size() - 1) & size != 0 {
            (size + self.alloc_manager.page_size()) & !(self.alloc_manager.page_size() - 1)
        } else {
            size
        };

        trace!(target: "libfabric-sys", "Full Allocating aligned size: {} aligned", aligned_size);
        #[cfg(not(feature = "enable-on-node-shmem"))]
        let (mem, mem_base_ptr) = {
            let mmap = memmap::MmapOptions::new()
                .len(aligned_size)
                .map_anon()
                .expect(&format!(
                    "Error in allocating aligned memory of size: {} {}",
                    aligned_size, size,
                ));
            unsafe {
                std::slice::from_raw_parts_mut(mmap.as_ptr() as *mut u8, aligned_size).fill(0);
            }
            let mem_base_ptr = mmap.as_ptr() as *mut u8;
            (LibfabricSysMem::Mmap(Arc::new(mmap)), mem_base_ptr)
        };

        #[cfg(feature = "enable-on-node-shmem")]
        let (mem, mem_base_ptr, same_node_bases, same_node_segments) = if self.disable_on_node_shmem
        {
            let mut mmap = memmap::MmapOptions::new()
                .len(aligned_size)
                .map_anon()
                .expect(&format!(
                    "Error in allocating aligned memory of size: {} {}",
                    aligned_size, size,
                ));
            unsafe {
                std::slice::from_raw_parts_mut(mmap.as_ptr() as *mut u8, aligned_size).fill(0);
            }
            let mem_base_ptr = mmap.as_ptr() as *mut u8;
            (
                LibfabricSysMem::Mmap(Arc::new(mmap)),
                mem_base_ptr,
                vec![None; self.num_pes],
                vec![None; self.num_pes],
            )
        } else {
            let alloc_id = LIBFABRIC_SHMEM_ALLOC_ID.fetch_add(1, Ordering::SeqCst);
            let shmem_id = format!("libfabric_alloc_{}_pe_{}", alloc_id, self.my_pe);
            let local_segment = Arc::new(attach_shmem_segment(
                self.job_id,
                aligned_size,
                align,
                &shmem_id,
                alloc_id,
                true,
            ));
            let mem_base_ptr = local_segment.base_ptr();
            let mem_slice = unsafe { std::slice::from_raw_parts_mut(mem_base_ptr, aligned_size) };
            mem_slice.fill(0);
            let (mut same_node_bases, same_node_segments) = build_same_node_segments(
                &(0..self.num_pes).collect::<Vec<_>>(),
                &self.same_node_pes,
                self.job_id,
                aligned_size,
                align,
                alloc_id,
                self.my_pe,
            );
            let mut same_node_segments = same_node_segments;
            same_node_bases[self.my_pe] = Some(mem_base_ptr as usize);
            same_node_segments[self.my_pe] = Some(local_segment.clone());
            (
                LibfabricSysMem::Shmem(local_segment),
                mem_base_ptr,
                same_node_bases,
                same_node_segments,
            )
        };
        let mem_slice = unsafe { std::slice::from_raw_parts_mut(mem_base_ptr, aligned_size) };

        let mr = unsafe {
            let mut mr = MaybeUninit::<*mut libfabric_sys::fid_mr>::uninit();
            let ret = libfabric_sys::inlined_fi_mr_reg(
                self.domain,
                mem_base_ptr as *mut libc::c_void,
                aligned_size,
                (libfabric_sys::FI_READ
                    | libfabric_sys::FI_WRITE
                    | libfabric_sys::FI_REMOTE_READ
                    | libfabric_sys::FI_REMOTE_WRITE) as u64,
                0,
                self.alloc_manager.next_key() as u64,
                0,
                mr.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if ret != 0 {
                eprintln!("Error registering memory region: {}", ret);
                Err(AllocError::FabricAllocationError(-ret as i32))?;
            }
            mr.assume_init()
        };

        // let mr = match mr {
        //     MaybeDisabledMemoryRegion::Disabled(mr) => {
        //         match mr {
        //             DisabledMemoryRegion::EpBind(mr) => {
        //                 // trace!("Binding memory region to endpoint");
        //                 mr.enable(&self.comm_group.ep)
        //                     .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?
        //             }
        //             DisabledMemoryRegion::RmaEvent(mr) => {
        //                 // trace!("Binding memory region to domain");
        //                 mr.enable()
        //                     .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?
        //                 // This will bind the memory region to the domain
        //             }
        //         }
        //     }
        //     MaybeDisabledMemoryRegion::Enabled(mr) => mr,
        // };

        // let remote_alloc_infos = self.pmi_exchange_mr_info(&mem, &mr);
        let remote_alloc_infos =
            self.collective_exchange_mr_info(&(0..self.num_pes).collect::<Vec<_>>(), mem_slice, mr);

        let mcast_group = self.create_mc_group(&(0..self.num_pes).collect::<Vec<_>>());

        let alloc = LibfabricSysAlloc::new(
            self.clone(),
            mem,
            #[cfg(feature = "enable-on-node-shmem")]
            Arc::new(same_node_bases),
            #[cfg(feature = "enable-on-node-shmem")]
            Arc::new(same_node_segments),
            mr,
            remote_alloc_infos,
            data_size,
            padding,
            self.alloc_manager.clone(),
            Some(mcast_group),
        )
        .map_err(|e| AllocError::FabricAllocationError(e))?;
        self.alloc_manager.insert(alloc.clone());
        Ok(alloc)
    }

    fn sub_alloc(
        self: &Arc<Ofi>,
        pes: &[usize],
        data_size: usize,
        align: usize,
    ) -> AllocResult<LibfabricSysAlloc> {
        //add space for ref count and padding to align it
        let (padding, size, _align) = calc_alloc_padding_size_align(data_size, align);
        // Align to page boundaries
        let aligned_size = if (self.alloc_manager.page_size() - 1) & size != 0 {
            (size + self.alloc_manager.page_size()) & !(self.alloc_manager.page_size() - 1)
        } else {
            size
        };

        trace!(target: "libfabric-sys", "Sub Allocating aligned size: {} aligned pes: {:?}", aligned_size, pes);

        #[cfg(not(feature = "enable-on-node-shmem"))]
        let (mem, mem_base_ptr) = {
            let mmap = memmap::MmapOptions::new()
                .len(aligned_size)
                .map_anon()
                .expect("Error in allocating aligned memory");
            unsafe {
                std::slice::from_raw_parts_mut(mmap.as_ptr() as *mut u8, aligned_size).fill(0);
            }
            let mem_base_ptr = mmap.as_ptr() as *mut u8;
            (LibfabricSysMem::Mmap(Arc::new(mmap)), mem_base_ptr)
        };
        #[cfg(feature = "enable-on-node-shmem")]
        let (mem, mem_base_ptr, same_node_bases, same_node_segments) = if self.disable_on_node_shmem
        {
            let mut mmap = memmap::MmapOptions::new()
                .len(aligned_size)
                .map_anon()
                .expect("Error in allocating aligned memory");
            unsafe {
                std::slice::from_raw_parts_mut(mmap.as_ptr() as *mut u8, aligned_size).fill(0);
            }
            let mem_base_ptr = mmap.as_ptr() as *mut u8;
            (
                LibfabricMem::Mmap(Arc::new(mmap)),
                mem_base_ptr,
                vec![None; self.num_pes],
                vec![None; self.num_pes],
            )
        } else {
            let alloc_id = LIBFABRIC_SHMEM_ALLOC_ID.fetch_add(1, Ordering::SeqCst);
            let shmem_id = format!("libfabric_sys_alloc_{}_pe_{}", alloc_id, self.my_pe);
            let local_segment = Arc::new(attach_shmem_segment(
                self.job_id,
                aligned_size,
                align,
                &shmem_id,
                alloc_id,
                true,
            ));
            let mem_base_ptr = local_segment.base_ptr();
            let mem_slice = unsafe { std::slice::from_raw_parts_mut(mem_base_ptr, aligned_size) };
            mem_slice.fill(0);
            let (mut same_node_bases, same_node_segments) = build_same_node_segments(
                pes,
                &self.same_node_pes,
                self.job_id,
                aligned_size,
                align,
                alloc_id,
                self.my_pe,
            );
            let mut same_node_segments = same_node_segments;
            same_node_bases[self.my_pe] = Some(mem_base_ptr as usize);
            same_node_segments[self.my_pe] = Some(local_segment.clone());
            (
                LibfabricMem::Shmem(local_segment),
                mem_base_ptr,
                same_node_bases,
                same_node_segments,
            )
        };
        let mem_slice = unsafe { std::slice::from_raw_parts_mut(mem_base_ptr, aligned_size) };

        let mr = unsafe {
            let mut mr = MaybeUninit::<*mut libfabric_sys::fid_mr>::uninit();
            let ret = libfabric_sys::inlined_fi_mr_reg(
                self.domain,
                mem_base_ptr as *mut libc::c_void,
                aligned_size,
                (libfabric_sys::FI_READ
                    | libfabric_sys::FI_WRITE
                    | libfabric_sys::FI_REMOTE_READ
                    | libfabric_sys::FI_REMOTE_WRITE) as u64,
                0,
                self.alloc_manager.next_key() as u64,
                0,
                mr.as_mut_ptr(),
                std::ptr::null_mut(),
            );
            if ret != 0 {
                eprintln!("Error registering memory region: {}", ret);
                Err(AllocError::FabricAllocationError(-ret as i32))?;
            }
            mr.assume_init()
        };

        // let mr = match mr {
        //     MaybeDisabledMemoryRegion::Disabled(mr) => {
        //         match mr {
        //             DisabledMemoryRegion::EpBind(mr) => {
        //                 // trace!("Binding memory region to endpoint");
        //                 mr.enable(&self.comm_group.ep)
        //                     .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?
        //             }
        //             DisabledMemoryRegion::RmaEvent(mr) => {
        //                 // trace!("Binding memory region to domain");
        //                 mr.enable()
        //                     .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?
        //                 // This will bind the memory region to the domain
        //             }
        //         }
        //     }
        //     MaybeDisabledMemoryRegion::Enabled(mr) => mr,
        // };

        let remote_alloc_infos = self.collective_exchange_mr_info(pes, mem_slice, mr);

        let mcast_group = self.create_mc_group(pes);

        let alloc = LibfabricSysAlloc::new(
            self.clone(),
            mem,
            #[cfg(feature = "enable-on-node-shmem")]
            Arc::new(same_node_bases),
            #[cfg(feature = "enable-on-node-shmem")]
            Arc::new(same_node_segments),
            mr,
            remote_alloc_infos,
            data_size,
            padding,
            self.alloc_manager.clone(),
            Some(mcast_group),
        )
        .map_err(|e| AllocError::FabricAllocationError(e))?;
        self.alloc_manager.insert(alloc.clone());
        Ok(alloc)
    }

    pub(crate) fn get_alloc_from_start_addr(
        &self,
        addr: CommAllocAddr,
    ) -> AllocResult<LibfabricSysAlloc> {
        self.alloc_manager.get_alloc_from_start_addr(addr)
    }

    pub(crate) fn clear_allocs(&self) -> Result<(), libfabric::error::Error> {
        self.alloc_manager.clear();
        Ok(())
    }

    pub(crate) fn barrier(&self) -> Result<(), libfabric::error::Error> {
        // trace!("Running barrier");
        match &*self.comm_group.barrier_impl.read() {
            BarrierImpl::Uninit => {
                panic!("Barrier is not initialized");
            }
            BarrierImpl::Collective(_mc, coll_addr) => {
                let cg = &self.comm_group;
                cg.post_collective(true, || unsafe {
                    libfabric_sys::inlined_fi_barrier(cg.ep, *coll_addr, std::ptr::null_mut())
                });
                // trace!("Done with barrier");
                Ok(())
            }
            BarrierImpl::Manual(barrier_alloc, barrier_id) => {
                let n = 2usize;
                let pes = (0..self.num_pes).collect::<Vec<_>>();
                let num_pes = pes.len();
                let num_rounds = ((num_pes as f64).log2() / (n as f64).log2()).ceil();
                let my_barrier = barrier_id.fetch_add(1, Ordering::SeqCst);
                for round in 0..num_rounds as usize {
                    for i in 1..=n {
                        let send_pe = (self.my_pe + i * (n + 1).pow(round as u32)) % num_pes;

                        // let dst = barrier_addr + 8 * self.my_pe;
                        unsafe {
                            barrier_alloc.inner_put::<usize>(
                                send_pe,
                                self.my_pe,
                                std::slice::from_ref(&my_barrier),
                                false,
                            );
                        }
                    }

                    let _lock = self.comm_group.lock.lock();
                    for i in 1..=n {
                        let recv_pe = (self.my_pe as i64
                            - i as i64 * (n as i64 + 1).pow(round as u32))
                        .rem_euclid(num_pes as i64);
                        // let barrier_vec = unsafe {
                        //     std::slice::from_raw_parts(barrier_addr as *const usize, num_pes)
                        // };
                        let barrier_vec = unsafe { barrier_alloc.as_mut_slice::<usize>() };

                        while my_barrier > barrier_vec[recv_pe as usize] {
                            self.comm_group.progress();
                            std::thread::yield_now();
                        }
                    }
                }

                Ok(())
            }
            BarrierImpl::Pmi(pmi) => {
                pmi.barrier(true).expect("PMI Barrier failed");
                Ok(())
            }
        }
    }

    pub(crate) fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize {
        self.alloc_manager
            .local_addr(remote_pe, remote_addr)
            .expect(&format!(
                "Local address not found from remote PE {}, remote addr: {:x}",
                remote_pe, remote_addr
            ))
    }

    pub(crate) fn one_sided_alloc_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
        num_bytes: usize,
    ) -> CommAlloc {
        self.alloc_manager.one_sided_alloc_from_remote_pe_and_addr(
            remote_pe,
            remote_addr,
            num_bytes,
        )
    }

    pub(crate) fn local_alloc_and_offset_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
    ) -> Option<(CommAlloc, usize)> {
        self.alloc_manager
            .local_alloc_and_offset_from_remote_pe_and_addr(remote_pe, remote_addr)
    }

    pub(crate) fn remote_addr(&self, pe: usize, local_addr: usize) -> usize {
        self.alloc_manager
            .remote_addr(pe, local_addr)
            .expect(&format!("Remote address not found for PE {}", pe))
    }

    pub(crate) fn wait_all(&self) {
        self.comm_group.wait_all()
    }

    pub(crate) fn thread_wait(&self) {
        self.comm_group.wait_all()
    }

    pub(crate) fn progress_all(&self) {
        let _lock = self.comm_group.lock.lock();
        self.comm_group.progress()
    }

    pub(crate) fn thread_progress(&self) {
        let _lock = self.comm_group.lock.lock();
        self.comm_group.progress()
    }
}

fn atomic_op_to_fi_atomic_op<T: 'static>(op: &AtomicOp<T>) -> u32 {
    match op {
        AtomicOp::Min(_) | AtomicOp::FetchMin(_) => libfabric_sys::fi_op_FI_MIN,
        AtomicOp::Max(_) | AtomicOp::FetchMax(_) => libfabric_sys::fi_op_FI_MAX,
        AtomicOp::Sum(_) | AtomicOp::FetchSum(_) => libfabric_sys::fi_op_FI_SUM,
        AtomicOp::Sub(_) | AtomicOp::FetchSub(_) => libfabric_sys::fi_op_FI_SUM, // Sub can be implemented as Add with negative value
        AtomicOp::Prod(_) | AtomicOp::FetchProd(_) => libfabric_sys::fi_op_FI_PROD,
        AtomicOp::BitOr(_) | AtomicOp::FetchBitOr(_) => libfabric_sys::fi_op_FI_BOR,
        AtomicOp::BitXor(_) | AtomicOp::FetchBitXor(_) => libfabric_sys::fi_op_FI_BXOR,
        AtomicOp::BitAnd(_) | AtomicOp::FetchBitAnd(_) => libfabric_sys::fi_op_FI_BAND,
        AtomicOp::Read(_) => libfabric_sys::fi_op_FI_ATOMIC_READ,
        AtomicOp::Write(_) => libfabric_sys::fi_op_FI_ATOMIC_WRITE,
        AtomicOp::Cas => libfabric_sys::fi_op_FI_CSWAP,
    }
}

fn rust_type_to_fi_type<T: 'static>() -> Option<u32> {
    let tid = std::any::TypeId::of::<T>();
    if tid == std::any::TypeId::of::<u8>() {
        Some(libfabric_sys::fi_datatype_FI_UINT8)
    } else if tid == std::any::TypeId::of::<u16>() {
        Some(libfabric_sys::fi_datatype_FI_UINT16)
    } else if tid == std::any::TypeId::of::<u32>() {
        Some(libfabric_sys::fi_datatype_FI_UINT32)
    } else if tid == std::any::TypeId::of::<u64>() {
        Some(libfabric_sys::fi_datatype_FI_UINT64)
    } else if tid == std::any::TypeId::of::<usize>() {
        #[cfg(target_pointer_width = "64")]
        {
            Some(libfabric_sys::fi_datatype_FI_UINT64)
        }
        #[cfg(target_pointer_width = "32")]
        {
            Some(libfabric_sys::fi_datatype_FI_UINT32)
        }
    } else if tid == std::any::TypeId::of::<u128>() {
        Some(libfabric_sys::fi_datatype_FI_UINT128)
    } else if tid == std::any::TypeId::of::<i8>() {
        Some(libfabric_sys::fi_datatype_FI_INT8)
    } else if tid == std::any::TypeId::of::<i16>() {
        Some(libfabric_sys::fi_datatype_FI_INT16)
    } else if tid == std::any::TypeId::of::<i32>() {
        Some(libfabric_sys::fi_datatype_FI_INT32)
    } else if tid == std::any::TypeId::of::<i64>() {
        Some(libfabric_sys::fi_datatype_FI_INT64)
    } else if tid == std::any::TypeId::of::<isize>() {
        #[cfg(target_pointer_width = "64")]
        {
            Some(libfabric_sys::fi_datatype_FI_INT64)
        }
        #[cfg(target_pointer_width = "32")]
        {
            Some(libfabric_sys::fi_datatype_FI_INT32)
        }
    } else if tid == std::any::TypeId::of::<i128>() {
        Some(libfabric_sys::fi_datatype_FI_INT128)
    } else if tid == std::any::TypeId::of::<f32>() {
        Some(libfabric_sys::fi_datatype_FI_FLOAT)
    } else if tid == std::any::TypeId::of::<f64>() {
        Some(libfabric_sys::fi_datatype_FI_DOUBLE)
    } else {
        None
    }
}

pub(crate) struct AllocInfoManager {
    pub(crate) mr_info_table: Arc<RwLock<Vec<LibfabricSysAlloc>>>,
    mr_next_key: AtomicUsize,
    page_size: usize,
}

impl AllocInfoManager {
    pub(crate) fn new() -> Self {
        Self {
            mr_info_table: Arc::new(RwLock::new(Vec::new())),
            mr_next_key: AtomicUsize::new(0),
            page_size: page_size::get(),
        }
    }

    pub(crate) fn insert(&self, alloc: LibfabricSysAlloc) {
        self.mr_info_table.write().push(alloc);
    }

    pub(crate) fn clear(&self) {
        let mut table = self.mr_info_table.write();
        let allocs = table.drain(..).collect::<Vec<_>>();
        drop(table); // we do this because when the allocs are dropped, they may call back into the AllocInfoManager to remove themselves thus deadlocking
        for alloc in allocs {
            trace!(target: "libfabric-sys", "Clearing alloc: {:?}", alloc);
        }
    }

    pub(crate) fn remove_from_alloc(&self, mem_addr: &LibfabricSysAlloc) {
        let mut table = self.mr_info_table.write();
        if !table.is_empty() {
            let idx = table
                .iter()
                .position(|e| e.mem.as_ptr() == mem_addr.mem.as_ptr())
                .expect("Error! Invalid memory address");
            table.remove(idx);
        }
    }

    pub(crate) fn get_alloc_from_start_addr(
        &self,
        mem_addr: CommAllocAddr,
    ) -> AllocResult<LibfabricSysAlloc> {
        let table = self.mr_info_table.read();
        table
            .iter()
            .find(|e| e.start() == mem_addr.0)
            .cloned()
            .ok_or(AllocError::LocalNotFound(mem_addr))
    }

    pub(crate) fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> Option<usize> {
        let table = self.mr_info_table.read();
        let alloc_info = table
            .iter()
            .find(|x| x.remote_contains(&remote_pe, &remote_addr))?;
        let remote_alloc_info = alloc_info.remote_allocs.get(&remote_pe)?;
        let remote_offset = remote_addr - remote_alloc_info.mem_address as usize;
        Some(alloc_info.start() + remote_offset)
    }

    pub(crate) fn one_sided_alloc_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
        num_bytes: usize,
    ) -> CommAlloc {
        trace!(target: "libfabric-sys",
            "looking for remote_addr: {:x} on pe {:x}",
            remote_pe,
            remote_addr
        );
        let table = self.mr_info_table.read();
        let alloc_info = table
            .iter()
            .find(|x| x.remote_contains(&remote_pe, &remote_addr))
            .expect("Remote address not found in any allocation");
        let remote_alloc_info = alloc_info
            .remote_allocs
            .get(&remote_pe)
            .expect("Remote PE not part of the allocation");
        let remote_offset = remote_addr - remote_alloc_info.mem_address as usize;
        let alloc = alloc_info
            .sub_alloc(remote_offset, num_bytes)
            .expect("Failed to create one-sided allocation from remote PE and address");
        OneSidedLibfabricSysAlloc { alloc, remote_pe }.into()
    }

    pub(crate) fn local_alloc_and_offset_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
    ) -> Option<(CommAlloc, usize)> {
        trace!(
            target: "libfabric-sys",
            "looking for remote_addr: {:x} on pr {:x}",
            remote_pe,
            remote_addr
        );
        let table = self.mr_info_table.read();
        let alloc_info = table
            .iter()
            .find(|x| x.remote_contains(&remote_pe, &remote_addr))?;
        let remote_alloc_info = alloc_info.remote_allocs.get(&remote_pe)?;
        let remote_offset = remote_addr - remote_alloc_info.mem_address as usize;
        Some((alloc_info.clone().into(), remote_offset))
    }

    pub(crate) fn remote_addr(&self, remote_pe: usize, local_addr: usize) -> Option<usize> {
        let table = self.mr_info_table.read();
        if let Some(alloc_info) = table.iter().find(|x| x.contains(&local_addr)) {
            if let Some(remote_alloc_info) = alloc_info.remote_allocs.get(&remote_pe) {
                let local_offset = local_addr - alloc_info.start();
                Some(unsafe { remote_alloc_info.mem_address.add(local_offset) as usize })
            } else {
                None
            }
        } else {
            None
        }
    }

    pub(crate) fn page_size(&self) -> usize {
        self.page_size
    }

    pub(crate) fn next_key(&self) -> usize {
        self.mr_next_key.fetch_add(1, Ordering::SeqCst)
    }
}

#[derive(Clone)]
enum AllocTable {
    Fabric(Arc<AllocInfoManager>),
    Runtime(BTreeAlloc, usize, Arc<AllocInfoManager>), //the usize is the offset of the rt_alloc so that we can free it properly if a sub_alloc is the last reference
}

pub(crate) trait MemoryRange {
    fn bounds(&self, len: usize) -> (usize, usize);
}

impl MemoryRange for Range<usize> {
    fn bounds(&self, _len: usize) -> (usize, usize) {
        (self.start, self.end)
    }
}

impl MemoryRange for RangeFrom<usize> {
    fn bounds(&self, len: usize) -> (usize, usize) {
        (self.start, len)
    }
}

impl MemoryRange for RangeFull {
    fn bounds(&self, len: usize) -> (usize, usize) {
        (0, len)
    }
}

impl MemoryRange for RangeTo<usize> {
    fn bounds(&self, _len: usize) -> (usize, usize) {
        (0, self.end)
    }
}

#[derive(Clone)]
pub(crate) struct RemoteMemAddressInfo {
    mem_address: *const u8,
    len: usize,
    key: u64,
}

impl RemoteMemAddressInfo {
    pub(crate) fn mem_address(&self) -> *const u8 {
        self.mem_address
    }

    pub(crate) fn key(&self) -> u64 {
        self.key
    }

    pub(crate) fn contains(&self, addr: &usize) -> bool {
        let start = self.mem_address as usize;
        let end = start + self.len;
        *addr >= start && *addr < end
    }

    pub(crate) unsafe fn sub_region(&self, range: impl MemoryRange) -> Self {
        let (start, end) = range.bounds(self.len);
        // We can create a zero-length sub-region.
        assert!(
            start <= end,
            "Invalid range for remote memory sub-region: start: {}, end: {}",
            start,
            end
        );
        let len = end - start;
        let (start, end) = (
            start * std::mem::size_of::<u8>(),
            end * std::mem::size_of::<u8>(),
        );

        // We can create a zero-length sub-region.
        assert!(
            start <= self.len,
            "Out of bounds access to remote memory sub-region start:{}  len: {}",
            start,
            self.len
        );
        assert!(
            end - 1 < self.len,
            "Out of bounds access to remote memory sub-region start:{}  len: {}",
            start,
            self.len
        );

        Self {
            mem_address: unsafe { self.mem_address.add(start) },
            len,
            key: self.key.clone(),
        }
    }
}

pub(crate) struct LibfabricSysAlloc {
    pub(crate) ofi: Arc<Ofi>,
    mem: LibfabricSysMem,
    #[cfg(feature = "enable-on-node-shmem")]
    same_node_bases: Arc<Vec<Option<usize>>>,
    #[cfg(feature = "enable-on-node-shmem")]
    same_node_segments: Arc<Vec<Option<Arc<ShmemSegment>>>>,
    mr: *mut libfabric_sys::fid_mr,
    mr_desc: *mut std::ffi::c_void,
    range: std::ops::Range<usize>,
    remote_allocs: HashMap<usize, RemoteMemAddressInfo>,
    fabric_ref_cnt_offset: usize,
    rt_ref_cnt_offset: usize,
    id: usize,
    alloc_table: AllocTable,
    mcast_group: Option<(*mut libfabric_sys::fid_mc, u64)>,
    pub(crate) print: bool,
}

unsafe impl Send for LibfabricSysAlloc {}
unsafe impl Sync for LibfabricSysAlloc {}

impl std::fmt::Debug for LibfabricSysAlloc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fabric_ref_count = unsafe {
            (&*(self.mem.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize))
                .load(Ordering::SeqCst)
        };

        let mut temp = f.debug_struct("LibfabricSysAlloc");
        temp.field("id", &self.id);
        temp.field(
            "addr",
            &format_args!("{:x} - {:x}", self.range.start, self.range.end),
        )
        .field(
            "mem",
            &format_args!("{:?}-{:?}", self.mem.as_ptr(), unsafe {
                self.mem.as_ptr().add(self.mem.len())
            }),
        )
        .field("data_num_bytes", &self.num_bytes())
        .field("my_pe", &self.ofi.my_pe)
        .field("num_pes", &self.ofi.num_pes)
        .field(
            "fabric_ref_cnt_offset",
            &format_args!(
                "{} ({:?}): {}",
                self.fabric_ref_cnt_offset,
                unsafe { self.mem.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize },
                fabric_ref_count
            ),
        );
        if let AllocTable::Runtime(_, _, _) = &self.alloc_table {
            let rt_ref_count = unsafe {
                (&*(self.mem.as_ptr().add(self.rt_ref_cnt_offset) as *const AtomicUsize))
                    .load(Ordering::SeqCst)
            };
            let padding = decode_padding(rt_ref_count);
            let rt_ref_count = decode_ref_count(rt_ref_count);
            temp.field(
                "rt_ref_cnt_offset",
                &format_args!(
                    "{} ({:?}): {}, {}",
                    self.rt_ref_cnt_offset,
                    unsafe { self.mem.as_ptr().add(self.rt_ref_cnt_offset) as *const AtomicUsize },
                    rt_ref_count,
                    padding,
                ),
            );
        }
        temp.finish()
    }
}

impl Clone for LibfabricSysAlloc {
    fn clone(&self) -> Self {
        self.increment_fabric_ref_count();
        if let AllocTable::Runtime(_, _, _) = &self.alloc_table {
            self.increment_rt_ref_count();
        }
        get_ref_count(unsafe {
            &*(self.mem.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize)
        });
        trace!(target: "libfabric-sys", "Cloned LibfabricSysAlloc: {:?}", self);
        Self {
            ofi: self.ofi.clone(),
            mem: self.mem.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_bases: self.same_node_bases.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_segments: self.same_node_segments.clone(),
            mr: self.mr.clone(),
            mr_desc: unsafe { libfabric_sys::inlined_fi_mr_desc(self.mr) },
            range: self.range.clone(),
            remote_allocs: self.remote_allocs.clone(),
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: self.rt_ref_cnt_offset,
            id: self.id,
            alloc_table: self.alloc_table.clone(),
            mcast_group: self.mcast_group.clone(),
            print: self.print,
        }
    }
}

impl From<LibfabricSysAlloc> for CommAlloc {
    fn from(alloc: LibfabricSysAlloc) -> Self {
        CommAlloc {
            inner_alloc: Arc::new(CommAllocInner::LibfabricSysAlloc(alloc)),
        }
    }
}

static ALLOC_ID: AtomicUsize = AtomicUsize::new(0);

impl LibfabricSysAlloc {
    unsafe fn negate_atomic_value<OFI>(value: *mut OFI) {
        let num_bytes = std::mem::size_of::<OFI>();
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

        // let mut result = std::mem::MaybeUninit::<OFI>::uninit();
        std::ptr::copy(bytes.as_ptr(), value.cast::<u8>(), num_bytes);
    }

    pub(crate) fn new(
        ofi: Arc<Ofi>,
        mem: LibfabricSysMem,
        #[cfg(feature = "enable-on-node-shmem")] same_node_bases: Arc<Vec<Option<usize>>>,
        #[cfg(feature = "enable-on-node-shmem")] same_node_segments: Arc<
            Vec<Option<Arc<ShmemSegment>>>,
        >,
        mr: *mut libfabric_sys::fid_mr,
        remote_allocs: HashMap<usize, RemoteMemAddressInfo>,
        num_bytes: usize,
        padding: usize,
        alloc_table: Arc<AllocInfoManager>,
        mcast_group: Option<(*mut libfabric_sys::fid_mc, u64)>,
    ) -> Result<Self, i32> {
        let start = mem.as_ptr() as usize;
        let end = start + num_bytes;
        let id = ALLOC_ID.fetch_add(1, Ordering::SeqCst);
        let ref_cnt_offset = num_bytes + padding;
        let fabric_ref_cnt_offset = num_bytes + padding;
        let encoded = encode_ref_count_and_padding(1, padding);

        let alloc = Self {
            ofi: ofi.clone(),
            mem: mem.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_bases,
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_segments,
            mr,
            mr_desc: unsafe { libfabric_sys::inlined_fi_mr_desc(mr) },
            range: std::ops::Range { start, end },
            remote_allocs: remote_allocs.clone(),
            fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            id,
            alloc_table: AllocTable::Fabric(alloc_table),
            mcast_group: mcast_group,
            print: false,
        };

        unsafe {
            (&*(alloc.mem.as_ptr().add(alloc.fabric_ref_cnt_offset) as *mut AtomicUsize))
                .store(encoded, Ordering::SeqCst);
        }
        debug!(target: "libfabric-sys", "Created LibfabricSys allocation: {:?}", alloc);

        Ok(alloc)
    }

    pub(crate) fn num_pes(&self) -> usize {
        self.remote_allocs.len()
    }

    // we call this function to create a sub-allocation that is tracked as part of a runtime allocation
    // 'len' should already contain the approriate padding and space for the ref count
    pub(crate) fn rt_alloc(
        &self,
        alloc_table: BTreeAlloc,
        offset: usize,
        padding: usize,
        len: usize, //data size + padding + ref count size
    ) -> AllocResult<Self> {
        if offset + len > self.num_bytes() {
            return Err(AllocError::InvalidSubAlloc(offset, len));
        }
        let data_bytes = len - padding - std::mem::size_of::<AtomicUsize>();
        let mut remote_allocs = HashMap::new();
        for (pe, remote_info) in self.remote_allocs.iter() {
            let new_remote_info = unsafe { remote_info.sub_region(offset..offset + len) };
            remote_allocs.insert(*pe, new_remote_info);
        }
        let id = ALLOC_ID.fetch_add(1, Ordering::SeqCst);
        self.increment_fabric_ref_count();
        let ref_cnt_offset = offset + data_bytes + padding;
        let encoded = encode_ref_count_and_padding(1, padding);

        let alloc_manager = match &self.alloc_table {
            AllocTable::Fabric(alloc_manager) => alloc_manager.clone(),
            AllocTable::Runtime(_, _, alloc_manager) => alloc_manager.clone(),
        };

        let alloc = Self {
            ofi: self.ofi.clone(),
            mem: self.mem.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_bases: self.shifted_same_node_bases(offset),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_segments: self.same_node_segments.clone(),
            mr: self.mr.clone(),
            mr_desc: unsafe { libfabric_sys::inlined_fi_mr_desc(self.mr) },
            range: self.range.start + offset..self.range.start + offset + data_bytes,
            remote_allocs: remote_allocs.clone(),
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            id,
            alloc_table: AllocTable::Runtime(alloc_table, self.range.start + offset, alloc_manager),
            mcast_group: self.mcast_group,
            print: self.print,
        };

        unsafe {
            (&*(alloc.mem.as_ptr().add(alloc.rt_ref_cnt_offset) as *mut AtomicUsize))
                .store(encoded, Ordering::SeqCst);
        }

        debug!(target: "libfabric-sys", "Created LibfabricSys rt-allocation: {:?}", alloc);
        Ok(alloc)
    }

    // This function is used to construct an rt_alloc from a raw sub-allocation
    // typically paired with a call to leak() we dont increment the ref counts as this instance recaptures the leaked instance
    pub(crate) fn as_rt_alloc(self, alloc_table: BTreeAlloc) -> AllocResult<Self> {
        let alloc_manager = match &self.alloc_table {
            AllocTable::Fabric(alloc_manager) => alloc_manager.clone(),
            AllocTable::Runtime(_, _, alloc_manager) => alloc_manager.clone(),
        };

        //since we are recapturing a leaked alloc, the non-rt sub-allocation we are converting should contain the appropriate ref count space at the end of the allocation
        let ref_cnt_offset = ((self.start() - self.mem.as_ptr() as usize) + self.num_bytes())
            - std::mem::size_of::<AtomicUsize>();

        let encoded_ref_count = unsafe {
            (&*(self.mem.as_ptr().add(ref_cnt_offset) as *const AtomicUsize)).load(Ordering::SeqCst)
        };
        let (_rt_ref_cnt, padding) = decode_ref_count_and_padding(encoded_ref_count);

        let alloc = Self {
            ofi: self.ofi.clone(),
            mem: self.mem.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_bases: self.same_node_bases.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_segments: self.same_node_segments.clone(),
            mr: self.mr.clone(),
            mr_desc: unsafe { libfabric_sys::inlined_fi_mr_desc(self.mr) },
            range: self.range.start..self.range.end - padding - std::mem::size_of::<AtomicUsize>(),
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            remote_allocs: self.remote_allocs.clone(),
            id: self.id,
            alloc_table: AllocTable::Runtime(alloc_table, self.range.start, alloc_manager),
            mcast_group: self.mcast_group,
            print: true,
        };
        get_ref_count(unsafe {
            &*(alloc.mem.as_ptr().add(alloc.fabric_ref_cnt_offset) as *const AtomicUsize)
        });
        debug!(target: "libfabric-sys", "Converted LibfabricSys alloc to rt-alloc: {:?}", alloc);
        Ok(alloc)
    }

    pub(crate) fn sub_alloc(&self, offset: usize, len: usize) -> AllocResult<Self> {
        if offset + len > self.num_bytes() {
            return Err(AllocError::InvalidSubAlloc(offset, len));
        }
        let mut remote_allocs = HashMap::new();
        for (pe, remote_info) in self.remote_allocs.iter() {
            let new_remote_info = unsafe { remote_info.sub_region(offset..offset + len) };
            remote_allocs.insert(*pe, new_remote_info);
        }
        let id = ALLOC_ID.fetch_add(1, Ordering::SeqCst);

        self.increment_fabric_ref_count();
        if let AllocTable::Runtime(_, _, _) = &self.alloc_table {
            self.increment_rt_ref_count();
        }

        let alloc = Self {
            ofi: self.ofi.clone(),
            mem: self.mem.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_bases: self.shifted_same_node_bases(offset),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_segments: self.same_node_segments.clone(),
            mr: self.mr.clone(),
            mr_desc: unsafe { libfabric_sys::inlined_fi_mr_desc(self.mr) },
            range: self.range.start + offset..self.range.start + offset + len,
            remote_allocs,
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: self.rt_ref_cnt_offset, //keep the same ref count offset as the parent allocation if this is actually a rt alloc, it will be updated when converted to a rt_alloc
            id,
            alloc_table: self.alloc_table.clone(),
            mcast_group: self.mcast_group,
            print: self.print,
        };
        debug!(target: "libfabric-sys", "Created LibfabricSys sub-allocation: {:?}", alloc);
        Ok(alloc)
    }

    pub(crate) fn leak(self) -> Option<CommAllocAddr> {
        match self.alloc_table {
            AllocTable::Fabric(_) => None, //only rt_allocs can be leaked
            AllocTable::Runtime(_, _, _) => {
                self.increment_fabric_ref_count(); //increment the ref count to account for the leaked instance
                let _cnt = self.increment_rt_ref_count(); //increment the ref count to account for the leaked instance
                debug!(target: "libfabric-sys", "Leaking Libfabric-sys rt-allocation: {:?}", self);
                // println!("Leaking allocation {:x} {:?}", self.start(), cnt);
                Some(CommAllocAddr(self.start()))
            }
        }
    }

    pub(crate) fn increment_fabric_ref_count(&self) -> usize {
        let ref_count =
            unsafe { &*(self.mem.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize) };
        increment_ref_count(ref_count)
    }

    pub(crate) fn decrement_fabric_ref_count(&self) -> usize {
        let ref_count =
            unsafe { &*(self.mem.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize) };
        decrement_ref_count(ref_count)
    }

    pub(crate) fn increment_rt_ref_count(&self) -> usize {
        let ref_count =
            unsafe { &*(self.mem.as_ptr().add(self.rt_ref_cnt_offset) as *const AtomicUsize) };
        increment_ref_count(ref_count)
    }
    pub(crate) fn decrement_rt_ref_count(&self) -> usize {
        let ref_count =
            unsafe { &*(self.mem.as_ptr().add(self.rt_ref_cnt_offset) as *const AtomicUsize) };
        decrement_ref_count(ref_count)
    }

    pub(crate) unsafe fn as_mut_slice<T: Copy>(&self) -> &mut [T] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.start() as *mut T,
                self.num_bytes() / std::mem::size_of::<T>(),
            )
        }
    }

    pub(crate) unsafe fn as_slice<T: Copy>(&self) -> &[T] {
        unsafe {
            std::slice::from_raw_parts(
                self.start() as *const T,
                self.num_bytes() / std::mem::size_of::<T>(),
            )
        }
    }

    pub(crate) fn start(&self) -> usize {
        self.range.start
    }
    pub(crate) fn num_bytes(&self) -> usize {
        self.range.end - self.range.start
    }

    #[cfg(feature = "enable-on-node-shmem")]
    fn shifted_same_node_bases(&self, offset: usize) -> Arc<Vec<Option<usize>>> {
        Arc::new(
            self.same_node_bases
                .iter()
                .map(|base| base.map(|addr| addr + offset))
                .collect(),
        )
    }

    #[cfg(feature = "enable-on-node-shmem")]
    fn same_node_addr(&self, pe: usize, offset_bytes: usize) -> Option<CommAllocAddr> {
        self.same_node_bases
            .get(pe)
            .and_then(|base| base.map(|addr| CommAllocAddr(addr + offset_bytes)))
    }

    pub(crate) fn contains(&self, addr: &usize) -> bool {
        trace!(target: "libfabric-sys",
            "Checking if address {:x} is contained in allocation range {:x}-{:x}",
            addr,
            self.range.start,
            self.range.end
        );
        self.range.contains(addr)
    }

    pub(crate) fn remote_contains(&self, remote_id: &usize, addr: &usize) -> bool {
        trace!(target: "libfabric-sys",
            "Checking if remote address {:x} on PE {} is contained in remote allocation for {:?}",
            addr,
            remote_id,
            self,
        );
        match self.remote_allocs.get(remote_id) {
            Some(remote_info) => {
                trace!(target: "libfabric-sys",
                    "Remote PE {} allocation info: {:?} {:?}",
                    remote_id,
                    remote_info.mem_address,
                    unsafe{remote_info.mem_address.add(remote_info.len)}
                );
                remote_info.contains(&addr)
            }
            None => {
                trace!(target: "libfabric-sys",
                    "Remote PE {} is not part of the sub allocation group",
                    remote_id
                );
                for (pe, remote_info) in self.remote_allocs.iter() {
                    trace!(target: "libfabric-sys",
                        "  PE {}: {:?} {:?}",
                        pe,
                        remote_info.mem_address,
                        remote_info.len
                    );
                }
                false
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn remote_info(&self, remote_pe: &usize) -> Option<RemoteMemAddressInfo> {
        self.remote_allocs.get(remote_pe).cloned()
    }

    #[allow(dead_code)]
    pub(crate) fn mr(&self) -> *mut libfabric_sys::fid_mr {
        self.mr.clone()
    }

    pub(crate) unsafe fn inner_put<T: Copy>(
        &self,
        pe: usize,
        offset: usize, //T-sized offset
        src_addr: &[T],
        blocking: bool,
    ) {
        let offset = offset * std::mem::size_of::<T>(); //we allocate memoryregions from libfabric as u8;
        assert!(offset + src_addr.len() * std::mem::size_of::<T>() <= self.num_bytes()); //we use num_bytes instead of mem.len() to allow for sub-allocations,
        #[cfg(feature = "enable-on-node-shmem")]
        if let Some(addr) = self.same_node_addr(pe, offset) {
            std::ptr::copy_nonoverlapping(
                src_addr.as_ptr() as *const u8,
                addr.as_ptr::<u8>() as *mut u8,
                src_addr.len() * std::mem::size_of::<T>(),
            );
            return Ok(());
        }
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));

        let mut remote_dst_addr = remote_alloc_info.mem_address().add(offset);
        debug!(
            "Inner Put: Remote destination address for PE {}: base_addr {:?} offset<T> {} size_of<T> {} len {} {:?}-{:?} {} bytes",
            pe,
            remote_alloc_info.mem_address(),
            offset,
            std::mem::size_of::<T>(),
            src_addr.len(),
            remote_dst_addr,
            remote_dst_addr.add(std::mem::size_of_val(src_addr)),
            std::mem::size_of_val(src_addr)
        );
        let remote_key = remote_alloc_info.key();
        let cg = &self.ofi.comm_group;
        if std::mem::size_of_val(src_addr)
            < (*(*self.ofi.comm_group.info_entry).tx_attr).inject_size
        {
            trace!(
                target: "libfabric-sys",
                "Injecting write to PE {} at address {:?}",
                pe,
                remote_dst_addr
            );
            cg.post_put(blocking, || unsafe {
                libfabric_sys::inlined_fi_inject_write(
                    cg.ep,
                    src_addr.as_ptr().cast(),
                    std::mem::size_of_val(src_addr),
                    cg.mapped_addresses[pe],
                    remote_dst_addr as u64,
                    remote_key,
                )
            });
        } else {
            let mut curr_idx = 0;
            while curr_idx < src_addr.len() {
                let msg_len = std::cmp::min(
                    src_addr.len() - curr_idx,
                    (*(*self.ofi.comm_group.info_entry).ep_attr).max_msg_size
                        / std::mem::size_of::<T>(),
                );

                trace!(
                    target: "libfabric-sys",
                    "Posting write to PE {} at address {:?}",
                    pe,
                    remote_dst_addr,
                );
                cg.post_put(blocking, || unsafe {
                    libfabric_sys::inlined_fi_write(
                        cg.ep,
                        src_addr[curr_idx..curr_idx + msg_len].as_ptr().cast(),
                        std::mem::size_of_val(&src_addr[curr_idx..curr_idx + msg_len]),
                        self.mr_desc,
                        cg.mapped_addresses[pe],
                        remote_dst_addr as u64,
                        remote_key,
                        std::ptr::null_mut(),
                    )
                });

                remote_dst_addr = remote_dst_addr.add(msg_len * std::mem::size_of::<T>());
                curr_idx += msg_len;
            }
        }
    }

    pub(crate) unsafe fn inner_get<T: Copy>(
        &self,
        pe: usize,
        offset: usize,
        dst_addr: &mut [T],
        blocking: bool,
    ) {
        let offset = offset * std::mem::size_of::<T>(); //we allocate memoryregions from libfabric as u8;
        assert!(offset + dst_addr.len() * std::mem::size_of::<T>() <= self.num_bytes()); //we use num_bytes instead of mem.len() to allow for sub-allocations,
        #[cfg(feature = "enable-on-node-shmem")]
        if let Some(addr) = self.same_node_addr(pe, offset) {
            std::ptr::copy_nonoverlapping(
                addr.as_ptr::<u8>(),
                dst_addr.as_mut_ptr() as *mut u8,
                dst_addr.len() * std::mem::size_of::<T>(),
            );
            return Ok(());
        }
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));

        let mut remote_src_addr = remote_alloc_info.mem_address.add(offset);
        let remote_key = remote_alloc_info.key;
        // trace!(
        //     "Inner Get: Remote destination address for PE {}: base_addr {:?} offset<T> {} size_of<T> {} len {} {:?}-{:?} {} bytes",
        //     pe,
        //     remote_alloc_info.mem_address(),
        //     offset,
        //     std::mem::size_of::<T>(),
        //     dst_addr.len(),
        //     remote_src_addr,
        //     remote_src_addr.add(std::mem::size_of_val(dst_addr)),
        //     std::mem::size_of_val(dst_addr)
        // );
        let cg = &self.ofi.comm_group;
        if dst_addr.len()
            < (*(*self.ofi.comm_group.info_entry).ep_attr).max_msg_size / std::mem::size_of::<T>()
        {
            // trace!(
            //     target: "libfabric-sys",
            //     "GET: from PE {} at addr {:?} to local addr {:?} len {} {}",
            //     pe,
            //     remote_src_addr,
            //     dst_addr.as_mut_ptr(),
            //     std::mem::size_of_val(dst_addr),
            //     dst_addr.len(),
            // );
            cg.post_get(blocking, || unsafe {
                libfabric_sys::inlined_fi_read(
                    cg.ep,
                    dst_addr.as_mut_ptr().cast(),
                    std::mem::size_of_val(dst_addr),
                    self.mr_desc,
                    cg.mapped_addresses[pe],
                    remote_src_addr as u64,
                    remote_key,
                    std::ptr::null_mut(),
                )
            });
        } else {
            let mut curr_idx = 0;

            while curr_idx < dst_addr.len() {
                let msg_len = std::cmp::min(
                    dst_addr.len() - curr_idx,
                    (*(*self.ofi.comm_group.info_entry).ep_attr).max_msg_size
                        / std::mem::size_of::<T>(),
                );
                cg.post_get(blocking, || unsafe {
                    trace!(
                        target: "libfabric-sys",
                        "GET: from PE {} at addr {:?} to local addr {:?} len {}",
                        pe,
                        remote_src_addr,
                        &mut dst_addr[curr_idx..curr_idx + msg_len] as *mut [T],
                        msg_len * std::mem::size_of::<T>()
                    );
                    libfabric_sys::inlined_fi_read(
                        cg.ep,
                        dst_addr[curr_idx..curr_idx + msg_len].as_mut_ptr().cast(),
                        std::mem::size_of_val(&dst_addr[curr_idx..curr_idx + msg_len]),
                        self.mr_desc,
                        cg.mapped_addresses[pe],
                        remote_src_addr as u64,
                        remote_key,
                        std::ptr::null_mut(),
                    )
                });
                remote_src_addr = remote_src_addr.add(msg_len * std::mem::size_of::<T>());
                curr_idx += msg_len;
            }
        }
    }

    #[inline(never)]
    pub(crate) unsafe fn inner_get_small<T: Copy>(
        &self,
        pe: usize,
        offset: usize,
        dst_addr: &mut [T],
        blocking: bool,
    ) {
        let offset = offset * std::mem::size_of::<T>(); //we allocate memoryregions from libfabric as u8;
        assert!(offset + dst_addr.len() * std::mem::size_of::<T>() <= self.num_bytes()); //we use num_bytes instead of mem.len() to allow for sub-allocations,
        #[cfg(feature = "enable-on-node-shmem")]
        if let Some(addr) = self.same_node_addr(pe, offset) {
            std::ptr::copy_nonoverlapping(
                addr.as_ptr::<u8>(),
                dst_addr.as_mut_ptr() as *mut u8,
                dst_addr.len() * std::mem::size_of::<T>(),
            );
            return;
        }
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));

        let remote_src_addr = remote_alloc_info.mem_address.add(offset);
        let remote_key = remote_alloc_info.key;
        let cg = &self.ofi.comm_group;
        cg.post_get(blocking, || unsafe {
            libfabric_sys::inlined_fi_read(
                cg.ep,
                dst_addr.as_mut_ptr().cast(),
                std::mem::size_of_val(dst_addr),
                self.mr_desc,
                cg.mapped_addresses[pe],
                remote_src_addr as u64,
                remote_key,
                std::ptr::null_mut(),
            )
        });
    }

    pub(crate) fn atomic_op_inner<T: 'static>(
        &self,
        pe: usize,
        offset: usize,
        op: &mut AtomicOp<T>,
        blocking: bool,
    ) {
        #[cfg(feature = "enable-on-node-shmem")]
        {
            let offset_bytes = offset * std::mem::size_of::<T>();
            if let Some(addr) = self.same_node_addr(pe, offset_bytes) {
                crate::lamellae::comm::atomic::net_atomic_op(op, &addr);
                return;
            }
        }
        let offset = offset * std::mem::size_of::<T>(); //we allocate memoryregions from libfabric as u8;
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes()); //we use num_bytes instead of mem.len() to allow for sub-allocations, offset + 1 because atomics operate on a single element and we verifying we arent missaligned
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));
        let remote_dst_addr = unsafe { remote_alloc_info.mem_address().add(offset) };
        let remote_key = remote_alloc_info.key();

        match op {
            AtomicOp::Sub(src) => unsafe {
                Self::negate_atomic_value(src.as_mut().get_unchecked_mut())
            },
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
            _ => {}
        };
        let src = op.src();
        let buf = unsafe { std::slice::from_raw_parts(src, 1) };
        // let buf = std::slice::from_ref(std::mem::transmute::<&T, &OFI>(&src));
        let cg = &self.ofi.comm_group;
        let data_type = rust_type_to_fi_type::<T>().expect("Unsupported type for atomic operation");
        let op = atomic_op_to_fi_atomic_op(op);
        cg.post_put(blocking, || unsafe {
            libfabric_sys::inlined_fi_inject_atomic(
                cg.ep,
                buf.as_ptr().cast(),
                buf.len(),
                cg.mapped_addresses[pe],
                remote_dst_addr as u64,
                remote_key,
                data_type,
                op,
            )
        });
    }

    pub(crate) fn atomic_fetch_op_inner<T: 'static>(
        &self,
        pe: usize,
        offset: usize,
        op: &mut AtomicOp<T>,
        result: &mut [T],
        blocking: bool,
    ) {
        #[cfg(feature = "enable-on-node-shmem")]
        {
            let offset_bytes = offset * std::mem::size_of::<T>();
            if let Some(addr) = self.same_node_addr(pe, offset_bytes) {
                crate::lamellae::comm::atomic::net_atomic_fetch_op(op, &addr, result.as_mut_ptr());
                return;
            }
        }

        let offset = offset * std::mem::size_of::<T>(); //we allocate memoryregions from libfabric as u8;
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes()); //we use num_bytes instead of mem.len() to allow for sub-allocations, offset + 1 because atomics operate on a single element and we verifying we arent missaligned
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));
        let remote_dst_addr = unsafe { remote_alloc_info.mem_address().add(offset) };
        let remote_key = remote_alloc_info.key();

        let cg = &self.ofi.comm_group;
        let data_type = rust_type_to_fi_type::<T>().expect("Unsupported type for atomic operation");
        match op {
            AtomicOp::FetchSub(src) => unsafe {
                Self::negate_atomic_value(src.as_mut().get_unchecked_mut())
            },
            AtomicOp::Min(_)
            | AtomicOp::Max(_)
            | AtomicOp::Sum(_)
            | AtomicOp::Sub(_)
            | AtomicOp::Prod(_)
            | AtomicOp::BitOr(_)
            | AtomicOp::BitXor(_)
            | AtomicOp::BitAnd(_) => {
                panic!("Non-fetch atomic ops must use the non-fetch path")
            }
            AtomicOp::Cas => {
                panic!("Compare atomic ops must use the compare path")
            }
            _ => {}
        };
        let src = op.src();
        let buf = unsafe { std::slice::from_raw_parts(src, 1) };
        cg.post_get(blocking, || unsafe {
            libfabric_sys::inlined_fi_fetch_atomic(
                cg.ep,
                buf.as_ptr().cast(),
                buf.len(),
                self.mr_desc,
                result.as_mut_ptr().cast(),
                std::ptr::null_mut(),
                cg.mapped_addresses[pe],
                remote_dst_addr as u64,
                remote_key,
                data_type,
                atomic_op_to_fi_atomic_op(op),
                std::ptr::null_mut(),
            )
        });
    }

    pub(crate) fn atomic_compare_exchange_op_inner<T: 'static + Copy>(
        &self,
        pe: usize,
        offset: usize,
        current: *const T,
        new: *const T,
        result: &mut [T],
        blocking: bool,
    ) {
        #[cfg(feature = "enable-on-node-shmem")]
        {
            let offset_bytes = offset * std::mem::size_of::<T>();
            if let Some(addr) = self.same_node_addr(pe, offset_bytes) {
                result[0] = match crate::lamellae::comm::atomic::net_atomic_compare_exchange(
                    current, new, &addr,
                ) {
                    Ok(old) | Err(old) => old,
                };
                return;
            }
        }

        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));
        let remote_dst_addr = unsafe { remote_alloc_info.mem_address().add(offset) };
        let remote_key = remote_alloc_info.key();

        // let new = *(&new as *const T as *const OFI);
        // let current = *(&current as *const T as *const OFI);
        // let res = &mut *(result as *mut [T] as *mut [OFI]);
        let cg = &self.ofi.comm_group;

        cg.post_get(blocking, || unsafe {
            libfabric_sys::inlined_fi_compare_atomic(
                cg.ep,
                new.cast(),
                1,
                std::ptr::null_mut(),
                current.cast(),
                std::ptr::null_mut(),
                result.as_mut_ptr().cast(),
                std::ptr::null_mut(),
                cg.mapped_addresses[pe],
                remote_dst_addr as u64,
                remote_key,
                rust_type_to_fi_type::<T>().expect("Unsupported type for atomic operation"),
                libfabric_sys::fi_op_FI_CSWAP,
                std::ptr::null_mut(),
            )
        });
    }

    pub(crate) fn allreduce_inplace_inner<T: 'static>(
        &self,
        op: &AllReduceOp,
        src_and_result: &mut [T],
        blocking: bool,
    ) {
        let dst = unsafe {
            std::slice::from_raw_parts_mut(src_and_result.as_mut_ptr(), src_and_result.len())
        };
        self.allreduce_inner(op, src_and_result, dst, blocking)
    }

    pub(crate) fn allreduce_inner<T: 'static>(
        &self,
        op: &AllReduceOp,
        src: &[T],
        result: &mut [T],
        blocking: bool,
    ) {
        // let res = unsafe {&mut *(result as *mut [T] as *mut [OFI])};
        // let buf = unsafe { std::mem::transmute::<&[T], &[OFI]>(src) };
        let cg = &self.ofi.comm_group;
        let (_, addr) = self
            .mcast_group
            .as_ref()
            .expect("No multicast group for allreduce");
        cg.post_collective(blocking, || unsafe {
            libfabric_sys::inlined_fi_allreduce(
                cg.ep,
                src.as_ptr().cast(),
                src.len(),
                std::ptr::null_mut(),
                result.as_mut_ptr().cast(),
                std::ptr::null_mut(),
                *addr,
                rust_type_to_fi_type::<T>().expect("Unsupported type for allreduce operation"),
                op.into(),
                0,
                std::ptr::null_mut(),
            )
        });
    }
    // pub(crate) fn reduce_inplace_inner<T: 'static>(
    //     &self,
    //     op: &ReduceOp,
    //     root_pe: Option<usize>,
    //     blocking: bool,
    // )  {
    //     let dst = unsafe {std::slice::from_raw_parts_mut(self.start() as *mut T, self.num_bytes()/std::mem::size_of::<T>())};
    //     // let slice_or_pe = if let Some(root) = root_pe {
    //     //     RootOrSliceMut::NotRoot(root)
    //     // }
    //     // else {
    //     //     RootOrSliceMut::Root(dst)
    //     // };

    //     self.reduce_inner(op, slice_or_pe, blocking)
    // }

    pub(crate) fn allgather_inner<T: 'static>(&self, src: &[T], result: &mut [T], blocking: bool) {
        // let res = unsafe {&mut *(result as *mut [T] as *mut [OFI])};
        // let buf = unsafe { std::mem::transmute::<&[T], &[OFI]>(src) };
        let cg = &self.ofi.comm_group;
        let (_, coll_addr) = self
            .mcast_group
            .as_ref()
            .expect("No multicast group for allgather");
        cg.post_collective(blocking, || unsafe {
            libfabric_sys::inlined_fi_allgather(
                cg.ep,
                src.as_ptr().cast(),
                src.len(),
                std::ptr::null_mut(),
                result.as_mut_ptr().cast(),
                std::ptr::null_mut(),
                *coll_addr,
                rust_type_to_fi_type::<T>().expect("Unsupported type for allgather operation"),
                0,
                std::ptr::null_mut(),
            )
        });
    }

    pub(crate) fn alltoall_inner<T: 'static>(&self, src: &[T], result: &mut [T], blocking: bool) {
        // let res = unsafe {&mut *(result as *mut [T] as *mut [OFI])};
        // let buf = unsafe { std::mem::transmute::<&[T], &[OFI]>(src) };
        let cg = &self.ofi.comm_group;
        let (_mc, coll_addr) = self
            .mcast_group
            .as_ref()
            .expect("No multicast group for alltoall");
        cg.post_collective(blocking, || unsafe {
            libfabric_sys::inlined_fi_alltoall(
                cg.ep,
                src.as_ptr().cast(),
                src.len(),
                std::ptr::null_mut(),
                result.as_mut_ptr().cast(),
                std::ptr::null_mut(),
                *coll_addr,
                rust_type_to_fi_type::<T>().expect("Unsupported type for alltoall operation"),
                0,
                std::ptr::null_mut(),
            )
        });
    }

    pub(crate) fn reduce_inner<T: 'static>(
        &self,
        op: &ReduceOp,
        src: &[T],
        slice_or_pe: RootOrSliceMut<T>,
        blocking: bool,
    ) {
        let cg = &self.ofi.comm_group;
        let (_mc, coll_addr) = self
            .mcast_group
            .as_ref()
            .expect("No multicast group for collective reduce");
        let (result, root_pe) = match slice_or_pe {
            RootOrSliceMut::Root(result) => (Some(result), self.ofi.my_pe),
            RootOrSliceMut::NotRoot(root_pe) => (None, root_pe),
        };

        // let buf = unsafe { std::mem::transmute::<&[T], &[OFI]>(src) };
        let res = match result {
            Some(res) => res,
            None => {
                let res_buf =
                    unsafe { std::slice::from_raw_parts_mut(src.as_ptr() as *mut T, src.len()) }; // if result is None, we are doing an in-place reduce or we reduce on a non-root PE, so we can reuse the source buffer as the destination buffer since it is either the destination or will be ignored by non-root PEs
                res_buf
            }
        };
        cg.post_collective(blocking, || unsafe {
            libfabric_sys::inlined_fi_reduce(
                cg.ep,
                src.as_ptr().cast(),
                src.len(),
                std::ptr::null_mut(),
                res.as_mut_ptr().cast(),
                std::ptr::null_mut(),
                *coll_addr,
                cg.mapped_addresses[root_pe],
                rust_type_to_fi_type::<T>().expect("Unsupported type for reduce operation"),
                op.into(),
                0,
                std::ptr::null_mut(),
            )
        });
    }

    pub(crate) fn gather_inner<T: 'static>(
        &self,
        src: &[T],
        slice_or_pe: RootOrSliceMut<T>,
        blocking: bool,
    ) {
        let cg = &self.ofi.comm_group;
        let (_mc, coll_addr) = self
            .mcast_group
            .as_ref()
            .expect("No multicast group for collective reduce");
        let (result, root_pe) = match slice_or_pe {
            RootOrSliceMut::Root(result) => (Some(result), self.ofi.my_pe),
            RootOrSliceMut::NotRoot(root_pe) => (None, root_pe),
        };
        let res = match result {
            Some(res) => res,
            None => {
                let res_buf =
                    unsafe { std::slice::from_raw_parts_mut(src.as_ptr() as *mut T, src.len()) }; // if result is None, we are a non-root PE, so we can reuse the source buffer as the destination buffer since it will be ignored.
                res_buf
            }
        };
        cg.post_collective(blocking, || unsafe {
            libfabric_sys::inlined_fi_gather(
                cg.ep,
                src.as_ptr().cast(),
                src.len(),
                std::ptr::null_mut(),
                res.as_mut_ptr().cast(),
                std::ptr::null_mut(),
                *coll_addr,
                cg.mapped_addresses[root_pe],
                rust_type_to_fi_type::<T>().expect("Unsupported type for gather operation"),
                0,
                std::ptr::null_mut(),
            )
        });
    }

    pub(crate) fn broadcast_inner<T: 'static>(
        &self,
        root_src: RootSrcOrSliceMut<T>,
        blocking: bool,
    ) {
        let cg = &self.ofi.comm_group;
        let (_mc, coll_addr) = self
            .mcast_group
            .as_ref()
            .expect("No multicast group for collective reduce");
        let (result, root_pe) = match root_src {
            RootSrcOrSliceMut::Root(src) => (src, self.ofi.my_pe),
            RootSrcOrSliceMut::NotRoot(result, root_pe) => (result, root_pe),
        };
        // let res = unsafe {std::mem::transmute::<&mut [T], &mut [OFI]>(result)};
        cg.post_collective(blocking, || unsafe {
            libfabric_sys::inlined_fi_broadcast(
                cg.ep,
                result.as_mut_ptr().cast(),
                result.len(),
                std::ptr::null_mut(),
                *coll_addr,
                cg.mapped_addresses[root_pe],
                rust_type_to_fi_type::<T>().expect("Unsupported type for broadcast operation"),
                0,
                std::ptr::null_mut(),
            )
        });
    }

    pub(crate) fn scatter_inner<T: 'static>(
        &self,
        res: &mut [T],
        src_or_root_pe: RootSrcSliceOrNone<'_, T>,
        blocking: bool,
    ) {
        let cg = &self.ofi.comm_group;
        let (_mc, coll_addr) = self
            .mcast_group
            .as_ref()
            .expect("No multicast group for collective reduce");

        let (src, root_pe) = match src_or_root_pe {
            RootSrcSliceOrNone::Root(src) => (src, self.ofi.my_pe),
            RootSrcSliceOrNone::NotRoot(root_pe) => (
                unsafe { std::slice::from_raw_parts(res.as_ptr(), res.len()) },
                root_pe,
            ),
        };

        cg.post_collective(blocking, || unsafe {
            libfabric_sys::inlined_fi_scatter(
                cg.ep,
                src.as_ptr().cast(),
                src.len(),
                std::ptr::null_mut(),
                res.as_mut_ptr().cast(),
                std::ptr::null_mut(),
                *coll_addr,
                cg.mapped_addresses[root_pe],
                rust_type_to_fi_type::<T>().expect("Unsupported type for scatter operation"),
                0,
                std::ptr::null_mut(),
            )
        });
    }

    pub(crate) fn reduce_scatter_inner<T: 'static>(
        &self,
        op: &AllReduceOp,
        src: &[T],
        result: &mut [T],
        blocking: bool,
    ) {
        let cg = &self.ofi.comm_group;
        let (_mc, coll_addr) = self
            .mcast_group
            .as_ref()
            .expect("No multicast group for allreduce");
        cg.post_collective(blocking, || unsafe {
            libfabric_sys::inlined_fi_reduce_scatter(
                cg.ep,
                src.as_ptr().cast(),
                src.len(),
                std::ptr::null_mut(),
                result.as_mut_ptr().cast(),
                std::ptr::null_mut(),
                *coll_addr,
                rust_type_to_fi_type::<T>().expect("Unsupported type for reduce_scatter operation"),
                op.into(),
                0,
                std::ptr::null_mut(),
            )
        });
    }

    pub(crate) fn wait(&self) {
        self.ofi.comm_group.wait_all()
    }
}

impl Drop for LibfabricSysAlloc {
    fn drop(&mut self) {
        let fabric_ref_count = self.decrement_fabric_ref_count();
        // if self.print {
        //     println!(
        //         "[{:?}] Dropping LibfabricSysAlloc: {:x} - {:x} ref_cnt(before drop) {}",
        //         std::thread::current().id(),
        //         self.range.start,
        //         self.range.end,
        //         fabric_ref_count
        //     );
        // }
        debug!(target: "libfabric-sys", "Dropping LibfabricSysAlloc: {:x} - {:x} ref_cnt(before drop) {}", self.range.start,self.range.end, fabric_ref_count);

        match &self.alloc_table {
            AllocTable::Fabric(alloc_table) => {
                if fabric_ref_count == 2 {
                    debug!(target: "libfabric-sys", "Dropping fabric LibfabricSysAlloc: {:?}", self);
                    alloc_table.remove_from_alloc(self);
                }
            }
            AllocTable::Runtime(rt_alloc_table, addr, fabric_alloc_table) => {
                let rt_ref_count = self.decrement_rt_ref_count();
                // if self.print {
                //     println!(
                //         "[{:?}, {:?}] Freeing runtime LibfabricSysAlloc: {:?}",
                //         std::time::Instant::now(),
                //         std::thread::current().id(),
                //         self
                //     );
                // }
                if rt_ref_count == 1 {
                    debug!(target: "libfabric-sys", "Freeing runtime LibfabricSysAlloc: {:?}",  self);

                    rt_alloc_table.free(*addr).expect(&format!(
                        "[{:?}] Error removing from runtime alloc table {:x}",
                        std::thread::current().id(),
                        addr
                    ));
                }
                if fabric_ref_count == 2 {
                    debug!(target: "libfabric-sys", "Dropping fabric LibfabricSysAlloc from rt LibfabricSysAlloc: {:?}", self);
                    fabric_alloc_table.remove_from_alloc(self);
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OneSidedLibfabricSysAlloc {
    pub(crate) remote_pe: usize,
    pub(crate) alloc: LibfabricSysAlloc,
}

unsafe impl Send for OneSidedLibfabricSysAlloc {}
unsafe impl Sync for OneSidedLibfabricSysAlloc {}

impl OneSidedLibfabricSysAlloc {
    pub(crate) fn num_bytes(&self) -> usize {
        self.alloc.num_bytes()
    }
    pub(crate) fn start(&self) -> usize {
        self.alloc.start()
    }
    pub(crate) fn sub_alloc(&self, offset: usize, len: usize) -> AllocResult<Self> {
        let sub_alloc = self.alloc.sub_alloc(offset, len)?;
        Ok(OneSidedLibfabricSysAlloc {
            remote_pe: self.remote_pe,
            alloc: sub_alloc,
        })
    }
}

impl From<OneSidedLibfabricSysAlloc> for CommAlloc {
    fn from(alloc: OneSidedLibfabricSysAlloc) -> Self {
        CommAlloc {
            inner_alloc: Arc::new(CommAllocInner::OneSidedLibfabricSysAlloc(alloc)),
        }
    }
}

impl From<&ReduceOp> for libfabric_sys::fi_op {
    fn from(op: &ReduceOp) -> Self {
        match op {
            ReduceOp::Min => libfabric_sys::fi_op_FI_MIN,
            ReduceOp::Max => libfabric_sys::fi_op_FI_MAX,
            ReduceOp::Sum => libfabric_sys::fi_op_FI_SUM,
            ReduceOp::Prod => libfabric_sys::fi_op_FI_PROD,
            // CollectiveReduceOp::LogicalOr => libfabric_sys::fi_op_FI_LOR,
            // CollectiveReduceOp::LogicalXor => libfabric_sys::fi_op_FI_LXOR,
            // CollectiveReduceOp::LogicalAnd => libfabric_sys::fi_op_FI_LAND,
            ReduceOp::BitOr => libfabric_sys::fi_op_FI_BOR,
            ReduceOp::BitXor => libfabric_sys::fi_op_FI_BXOR,
            ReduceOp::BitAnd => libfabric_sys::fi_op_FI_BAND,
        }
    }
}

impl From<&ReduceOp> for &libfabric_sys::fi_op {
    fn from(op: &ReduceOp) -> Self {
        match op {
            ReduceOp::Min => &libfabric_sys::fi_op_FI_MIN,
            ReduceOp::Max => &libfabric_sys::fi_op_FI_MAX,
            ReduceOp::Sum => &libfabric_sys::fi_op_FI_SUM,
            ReduceOp::Prod => &libfabric_sys::fi_op_FI_PROD,
            // CollectiveReduceOp::LogicalOr => &libfabric_sys::fi_op_FI_LOR,
            // CollectiveReduceOp::LogicalXor => &libfabric_sys::fi_op_FI_LXOR,
            // CollectiveReduceOp::LogicalAnd => &libfabric_sys::fi_op_FI_LAND,
            ReduceOp::BitOr => &libfabric_sys::fi_op_FI_BOR,
            ReduceOp::BitXor => &libfabric_sys::fi_op_FI_BXOR,
            ReduceOp::BitAnd => &libfabric_sys::fi_op_FI_BAND,
        }
    }
}

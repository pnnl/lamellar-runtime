use std::{collections::HashMap, mem::MaybeUninit, ops::{Range, RangeFrom, RangeFull, RangeTo}, sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering}}};

use libfabric_sys;
use parking_lot::{Mutex, RwLock};
use pmi::{Pmi, PmiBuilder};
use tracing::{debug, trace};

use crate::{lamellae::{AllocError, AllocResult, AtomicOp, CommAlloc, CommAllocAddr, CommAllocInner, CommAllocType, FabricError, FabricResult, decode_padding, decode_ref_count, decrement_ref_count, get_ref_count, increment_ref_count}, lamellar_alloc::BTreeAlloc};

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

pub(crate) struct CommGroup{
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
    lock: Mutex<()>,
}

unsafe impl Send for CommGroup {}
unsafe impl Sync for CommGroup {}

impl CommGroup{
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
                    0
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
                1
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
            while self.coll_cnt_completed.load(Ordering::SeqCst) < self.coll_cnt_issued.load(Ordering::SeqCst) {
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
                let _ = libfabric_sys::inlined_fi_cntr_wait(cntr, prev_expected_cnt as u64, -1);
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
            if ret >= 0 {
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

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }

    fn as_mut_slice(&self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_ptr(), self.len()) }
    }
}


pub(crate) struct Ofi {
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    domain: *mut libfabric_sys::fid_domain,
    fabric: *mut libfabric_sys::fid_fabric,
    _my_pmi: Arc<dyn Pmi>,
    alloc_manager: Arc<AllocInfoManager>,
    comm_group: CommGroup,
}

unsafe impl Send for Ofi {}
unsafe impl Sync for Ofi {}

impl Ofi {
    pub(crate) fn new(provider: Option<&str>, domain: Option<&str>) -> FabricResult<Arc<Self>> {
        let my_pmi = Arc::new(PmiBuilder::init().map_err(|e| {
            eprintln!("Error initializing PMI: {:?}", e);
            FabricError::InitError(1)
        })?);

        let num_pes = my_pmi.ranks().len();

        let info = unsafe{
            let hints = libfabric_sys::inlined_fi_allocinfo();
            (*hints).caps = (libfabric_sys::FI_RMA | libfabric_sys::FI_ATOMIC | libfabric_sys::FI_COLLECTIVE) as u64;
            (*hints).mode = libfabric_sys::FI_CONTEXT;
            (*(*hints).ep_attr).type_ = libfabric_sys::fi_ep_type_FI_EP_RDM;
            (*(*hints).domain_attr).threading = libfabric_sys::fi_threading_FI_THREAD_SAFE;
            // (*(*hints).domain_attr).control_progress = libfabric_sys::fi_progress_FI_PROGRESS_AUTO as u8;
            (*(*hints).domain_attr).data_progress = libfabric_sys::fi_progress_FI_PROGRESS_MANUAL;
            (*(*hints).domain_attr).mr_mode = (libfabric_sys::FI_MR_PROV_KEY | libfabric_sys::FI_MR_VIRT_ADDR | libfabric_sys::FI_MR_ALLOCATED) as i32;
            (*(*hints).domain_attr).resource_mgmt = libfabric_sys::fi_resource_mgmt_FI_RM_ENABLED;
            (*(*hints).tx_attr).tclass = libfabric_sys::FI_TC_LOW_LATENCY;
            (*(*hints).tx_attr).op_flags = (libfabric_sys::FI_DELIVERY_COMPLETE ) as u64;
            (*hints).addr_format = libfabric_sys::FI_FORMAT_UNSPEC;
            let version = 1 << 16 | 22;
            let mut c_info = MaybeUninit::<*mut libfabric_sys::fi_info>::uninit();
            libfabric_sys::fi_getinfo(version, std::ptr::null_mut(), std::ptr::null_mut(), 0, hints, c_info.as_mut_ptr());
            let info = c_info.assume_init();
            let mut curr_info = info;
            while !curr_info.is_null() {
                unsafe {
                    if !provider.is_none() && provider.unwrap() != std::ffi::CStr::from_ptr((*curr_info).fabric_attr.as_ref().unwrap().name).to_str().unwrap() {
                        curr_info = (*curr_info).next;
                        continue;
                    }
                    if !domain.is_none() && domain.unwrap() != std::ffi::CStr::from_ptr((*curr_info).domain_attr.as_ref().unwrap().name).to_str().unwrap() {
                        curr_info = (*curr_info).next;
                        continue;
                    }
                    break;
                }
            }
            libfabric_sys::fi_freeinfo(hints);
            libfabric_sys::fi_freeinfo(info);
            libfabric_sys::fi_dupinfo(curr_info)
        };
        
        let fabric =
            unsafe {
                let mut fabric =  MaybeUninit::<*mut libfabric_sys::fid_fabric>::uninit();
                let ret = libfabric_sys::fi_fabric((*info).fabric_attr, fabric.as_mut_ptr(), std::ptr::null_mut());
                if ret != 0 {
                    eprintln!("Error creating fabric: {}", ret);
                    Err(FabricError::InitError(-ret as u32))?;
                }
                fabric.assume_init()
            };
        
        let domain = unsafe {
            let mut domain = MaybeUninit::<*mut libfabric_sys::fid_domain>::uninit();
            let ret = libfabric_sys::inlined_fi_domain(fabric, info, domain.as_mut_ptr(), std::ptr::null_mut());
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

            let ret = libfabric_sys::inlined_fi_eq_open(fabric, &mut eq_attr, eq.as_mut_ptr(), std::ptr::null_mut());
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
            let ret = libfabric_sys::inlined_fi_cq_open(domain, &mut cq_attr, cq.as_mut_ptr(), std::ptr::null_mut());
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
            let ret = libfabric_sys::inlined_fi_av_open(domain, &mut av_attr, av.as_mut_ptr(), std::ptr::null_mut());
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
            let ret = libfabric_sys::inlined_fi_cntr_open(domain, &mut cntr_attr, cntr.as_mut_ptr(), std::ptr::null_mut());
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
            let ret = libfabric_sys::inlined_fi_cntr_open(domain, &mut cntr_attr, cntr.as_mut_ptr(), std::ptr::null_mut());
            if ret != 0 {
                eprintln!("Error creating put counter: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            cntr.assume_init()
        };

        let ep = unsafe {
            let mut ep = MaybeUninit::<*mut libfabric_sys::fid_ep>::uninit();
            let ret = libfabric_sys::inlined_fi_endpoint(domain, info, ep.as_mut_ptr(), std::ptr::null_mut());
            if ret != 0 {
                eprintln!("Error creating endpoint: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            ep.assume_init()
        };

        unsafe {
            let mut flags = (libfabric_sys::FI_TRANSMIT | libfabric_sys::FI_RECV)  as u64 | libfabric_sys::FI_SELECTIVE_COMPLETION;
            let ret = libfabric_sys::inlined_fi_ep_bind(ep, &mut (*eq).fid, flags);
            if ret != 0 {
                eprintln!("Error binding EQ: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            let ret = libfabric_sys::inlined_fi_ep_bind(ep, &mut (*cq).fid, 0);
            if ret != 0 {
                eprintln!("Error binding CQ: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            let ret = libfabric_sys::inlined_fi_ep_bind(ep, &mut (*av).fid, 0);
            if ret != 0 {
                eprintln!("Error binding AV: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            let ret = libfabric_sys::inlined_fi_ep_bind(ep, &mut (*put_cntr).fid, libfabric_sys::FI_WRITE as u64);
            if ret != 0 {
                eprintln!("Error binding put counter: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            let ret = libfabric_sys::inlined_fi_ep_bind(ep, &mut (*get_cntr).fid, libfabric_sys::FI_READ as u64);
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
            let ret = unsafe {libfabric_sys::inlined_fi_getname(&mut (*ep).fid, std::ptr::null_mut(), &mut len)};
            if ret != 0 {
                eprintln!("Error getting endpoint name: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            let mut addr = vec![0u8; len];
            let ret = unsafe {libfabric_sys::inlined_fi_getname(&mut (*ep).fid, addr.as_mut_ptr().cast(), &mut len)};
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
            .map(|r| {
                my_pmi
                    .get(&format!("epname"), &r)
                    .unwrap()
            })
            .collect();

        let mapped_addresses = unsafe {
            let mut mapped_addresses = vec![0u64; unmapped_addresses.len()];
            let total_size = unmapped_addresses.iter().fold(0, |acc, unmapped_addresses| acc + unmapped_addresses.len());
            let mut serialized: Vec<u8> = Vec::with_capacity(total_size);
            for a in unmapped_addresses {
                serialized.extend(a.iter())
            }
            let ret = libfabric_sys::inlined_fi_av_insert(av, serialized.as_mut_ptr().cast(), mapped_addresses.len(), mapped_addresses.as_mut_ptr(), 0, std::ptr::null_mut());
            if ret != 0 {
                eprintln!("Error inserting addresses into AV: {}", ret);
                Err(FabricError::InitError(-ret as u32))?;
            }
            mapped_addresses
        };

        let comm_group = CommGroup{
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
            _my_pmi: my_pmi.clone(),
            // info_entry,
            fabric: fabric,
            domain,
            alloc_manager: Arc::new(alloc_manager),
            // barrier_impl: RwLock::new(BarrierImpl::Pmi(my_pmi)),
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
        let ret = unsafe {libfabric_sys::inlined_fi_atomicvalid(
            cg.ep,
            data_type, 
            libfabric_sys::fi_op_FI_SUM, 
            &mut count as *mut usize)
        };
        avail &= if ret != 0 {
            false
        } else {
            count > 0
        };

        let ret = unsafe {libfabric_sys::inlined_fi_fetch_atomicvalid(
            cg.ep,
            data_type, 
            libfabric_sys::fi_op_FI_ATOMIC_READ, 
            &mut count as *mut usize)
        };
        avail &= if ret != 0 {
            false
        } else {
            count > 0
        };

        let ret = unsafe {libfabric_sys::inlined_fi_compare_atomicvalid(
            cg.ep,
            data_type, 
            libfabric_sys::fi_op_FI_CSWAP, 
            &mut count as *mut usize)
        };
        avail &= if ret != 0 {
            false
        } else {
            count > 0
        };
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

        let mut avail = false;
        let ret = unsafe {libfabric_sys::inlined_fi_atomicvalid(
            cg.ep,
            data_type, 
            fi_op, 
            &mut count as *mut usize)
        };
        avail &= if ret != 0 {
            false
        } else {
            count > 0
        };

        let ret = unsafe {libfabric_sys::inlined_fi_fetch_atomicvalid(
            cg.ep,
            data_type, 
            fi_op, 
            &mut count as *mut usize)
        };
        avail &= if ret != 0 {
            false        
        } 
        else {
            count > 0
        };
        avail
    }

    pub(crate) fn atomic_avail<T: 'static>(&self) -> bool {
        self.atomic_avail_inner::<T>()
    }

    pub(crate) fn atomic_op_avail<T: 'static>(&self, op: AtomicOp<T>) -> bool {
        let op_kind = match op {
            AtomicOp::Min(_) => AtomicOpKind::Min,
            AtomicOp::Max(_) => AtomicOpKind::Max,
            AtomicOp::Sum(_) => AtomicOpKind::Sum,
            AtomicOp::Sub(_) => AtomicOpKind::Sum, // Sub can be implemented as Add with negative value
            AtomicOp::Prod(_) => AtomicOpKind::Prod,
            AtomicOp::BitOr(_) => AtomicOpKind::BitOr,
            AtomicOp::BitXor(_) => AtomicOpKind::BitXor,
            AtomicOp::BitAnd(_) => AtomicOpKind::BitAnd,
            AtomicOp::Read => AtomicOpKind::Read,
            AtomicOp::Write(_) => AtomicOpKind::Write,
            AtomicOp::Cas(_, _) => AtomicOpKind::Cas,
        };

        self.atomic_op_avail_inner::<T>(op_kind)
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
            trace!(target: "libfabric", "Clearing alloc: {:?}", alloc);
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
        trace!(target: "libfabric",
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
            target: "libfabric",
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

pub trait MemoryRange {
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
pub struct RemoteMemAddressInfo {
    mem_address: *const u8,
    len: usize,
    key: u64,
}

impl RemoteMemAddressInfo {
    pub fn contains(&self, addr: &usize) -> bool {
        let start = self.mem_address as usize;
        let end = start + self.len;
        *addr >= start && *addr < end
    }

    pub unsafe fn sub_region(&self, range: impl MemoryRange) -> Self {
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
    // mcast_group: Option<MultiCastGroup>,
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
            // mcast_group: self.mcast_group.clone(),
            print: self.print,
        }
    }
}


impl From<LibfabricSysAlloc> for CommAlloc {
    fn from(alloc: LibfabricSysAlloc) -> Self {
        CommAlloc {
            inner_alloc: CommAllocInner::LibfabricSysAlloc(alloc),
            alloc_type: CommAllocType::Fabric,
        }
    }
}

static ALLOC_ID: AtomicUsize = AtomicUsize::new(0);


impl LibfabricSysAlloc {
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
            // mcast_group: self.mcast_group.clone(),
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
            // mcast_group: self.mcast_group.clone(),
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
            // mcast_group: self.mcast_group.clone(),
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

    pub(crate) fn remote_info(&self, remote_pe: &usize) -> Option<RemoteMemAddressInfo> {
        self.remote_allocs.get(remote_pe).cloned()
    }

    pub(crate) fn mr(&self) -> *mut libfabric_sys::fid_mr {
        self.mr.clone()
    }


    pub(crate) unsafe fn inner_put<T: Copy>(
        &self,
        pe: usize,
        offset: usize, //T-sized offset
        src_addr: &[T],
        blocking: bool,
    )  {
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
        if std::mem::size_of_val(src_addr) < self.ofi.info_entry.tx_attr().inject_size() {
            trace!(
                target: "libfabric",
                "Injecting write to PE {} at address {:?}",
                pe,
                remote_dst_addr.as_ptr()
            );
            cg.post_put(blocking, || unsafe {
                libfabric_sys::inlined_fi_inject_write(
                    cg.ep,
                    src_addr.as_ptr().cast(),
                    std::mem::size_of_val(src_addr),
                    cg.mapped_addresses[pe],
                    remote_dst_addr,
                    remote_key,
                )
            })?;
        } else {
            let mut curr_idx = 0;
            while curr_idx < src_addr.len() {
                let msg_len = std::cmp::min(
                    src_addr.len() - curr_idx,
                    self.ofi.info_entry.ep_attr().max_msg_size() / std::mem::size_of::<T>(),
                );

                cg.post_put(blocking, || unsafe {
                    libfabric_sys::inlined_fi_write(
                        cg.ep,
                        src_addr[curr_idx..curr_idx + msg_len].as_ptr().cast(),
                        std::mem::size_of_val(&src_addr[curr_idx..curr_idx + msg_len]),
                        self.mr_desc,
                        cg.mapped_addresses[pe],
                        remote_dst_addr,
                        remote_key,
                        std::ptr::null_mut(),
                    )
                })
                .expect("Error posting put");

                remote_dst_addr = remote_dst_addr.add(msg_len * std::mem::size_of::<T>());
                curr_idx += msg_len;
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
    // pub(crate) fn sub_alloc(&self, offset: usize, len: usize) -> AllocResult<Self> {
    //     let sub_alloc = self.alloc.sub_alloc(offset, len)?;
    //     Ok(OneSidedLibfabricSysAlloc {
    //         remote_pe: self.remote_pe,
    //         alloc: sub_alloc,
    //     })
    // }
}

impl From<OneSidedLibfabricSysAlloc> for CommAlloc {
    fn from(alloc: OneSidedLibfabricSysAlloc) -> Self {
        CommAlloc {
            inner_alloc: CommAllocInner::OneSidedLibfabricSysAlloc(alloc),
            alloc_type: CommAllocType::Remote,
        }
    }
}

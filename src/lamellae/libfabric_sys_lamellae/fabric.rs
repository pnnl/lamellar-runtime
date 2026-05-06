use std::{mem::MaybeUninit, sync::Arc};

use libfabric_sys;
use parking_lot::Mutex;
use pmi::{Pmi, PmiBuilder};

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

pub(crate) struct Ofi {
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    domain: *mut libfabric_sys::fid_domain,
    fabric: *mut libfabric_sys::fid_fabric,
    _my_pmi: Arc<dyn Pmi>,
    alloc_manager: Arc<AllocInfoManager>,
    comm_group: CommGroup,
}

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
                let fabric =  MaybeUninit::<*mut libfabric_sys::fid_fabric>::uninit();
                let ret = libfabric_sys::fi_fabric((*info).fabric_attr, fabric.as_mut_ptr(), std::ptr::null_mut());
                if ret != 0 {
                    eprintln!("Error creating fabric: {}", ret);
                    Err(FabricError::InitError(ret))?;
                }
                fabric.assume_init()
            };
        
        let domain = unsafe {
            let domain = MaybeUninit::<*mut libfabric_sys::fid_domain>::uninit();
            let ret = libfabric_sys::fi_domain(fabric, info, domain.as_mut_ptr(), std::ptr::null_mut());
            if ret != 0 {
                eprintln!("Error creating domain: {}", ret);
                Err(FabricError::InitError(ret))?;
            }
            domain.assume_init()
        };

        let eq = unsafe {
            let eq = MaybeUninit::<*mut libfabric_sys::fid_eq>::uninit();
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
                Err(FabricError::InitError(ret))?;
            }
            eq.assume_init()
        };

        let cq = unsafe {
            let cq = MaybeUninit::<*mut libfabric_sys::fid_cq>::uninit();
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
                Err(FabricError::InitError(ret))?;
            }
            cq.assume_init()
        };

        let av = unsafe {
            let av = MaybeUninit::<*mut libfabric_sys::fid_av>::uninit();
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
                Err(FabricError::InitError(ret))?;
            }
            av.assume_init()
        };

        let put_cntr = unsafe {
            let cntr = MaybeUninit::<*mut libfabric_sys::fid_cntr>::uninit();
            let mut cntr_attr = libfabric_sys::fi_cntr_attr {
                events: libfabric_sys::fi_cntr_events_FI_CNTR_EVENTS_COMP,
                wait_obj: libfabric_sys::fi_wait_obj_FI_WAIT_UNSPEC,
                wait_set: std::ptr::null_mut(),
                flags: 0,
            };
            let ret = libfabric_sys::inlined_fi_cntr_open(domain, &mut cntr_attr, cntr.as_mut_ptr(), std::ptr::null_mut());
            if ret != 0 {
                eprintln!("Error creating put counter: {}", ret);
                Err(FabricError::InitError(ret))?;
            }
            cntr.assume_init()
        };

        let get_cntr = unsafe {
            let cntr = MaybeUninit::<*mut libfabric_sys::fid_cntr>::uninit();
            let mut cntr_attr = libfabric_sys::fi_cntr_attr {
                events: libfabric_sys::fi_cntr_events_FI_CNTR_EVENTS_COMP,
                wait_obj: libfabric_sys::fi_wait_obj_FI_WAIT_UNSPEC,
                wait_set: std::ptr::null_mut(),
                flags: 0,
            };
            let ret = libfabric_sys::inlined_fi_cntr_open(domain, &mut cntr_attr, cntr.as_mut_ptr(), std::ptr::null_mut());
            if ret != 0 {
                eprintln!("Error creating put counter: {}", ret);
                Err(FabricError::InitError(ret))?;
            }
            cntr.assume_init()
        };

        let ep = unsafe {
            let ep = MaybeUninit::<*mut libfabric_sys::fid_ep>::uninit();
            let ret = libfabric_sys::inlined_fi_endpoint(domain, info, ep.as_mut_ptr(), std::ptr::null_mut());
            if ret != 0 {
                eprintln!("Error creating endpoint: {}", ret);
                Err(FabricError::InitError(ret))?;
            }
            ep.assume_init()
        };

        unsafe {
            let mut flags = (libfabric_sys::FI_TRANSMIT | libfabric_sys::FI_RECV)  as u64 | libfabric_sys::FI_SELECTIVE_COMPLETION;
            let ret = libfabric_sys::inlined_fi_ep_bind(ep, &mut (*eq).fid, flags);
            if ret != 0 {
                eprintln!("Error binding EQ: {}", ret);
                Err(FabricError::InitError(ret))?;
            }
            let ret = libfabric_sys::inlined_fi_ep_bind(ep, &mut (*cq).fid, 0);
            if ret != 0 {
                eprintln!("Error binding CQ: {}", ret);
                Err(FabricError::InitError(ret))?;
            }
            let ret = libfabric_sys::inlined_fi_ep_bind(ep, &mut (*av).fid, 0);
            if ret != 0 {
                eprintln!("Error binding AV: {}", ret);
                Err(FabricError::InitError(ret))?;
            }
            let ret = libfabric_sys::inlined_fi_ep_bind(ep, &mut (*put_cntr).fid, libfabric_sys::FI_WRITE as u64);
            if ret != 0 {
                eprintln!("Error binding put counter: {}", ret);
                Err(FabricError::InitError(ret))?;
            }
            let ret = libfabric_sys::inlined_fi_ep_bind(ep, &mut (*get_cntr).fid, libfabric_sys::FI_READ as u64);
            if ret != 0 {
                eprintln!("Error binding get counter: {}", ret);
                Err(FabricError::InitError(ret))?;
            }
        }

        unsafe {
            let ret = libfabric_sys::inlined_fi_enable(ep);
            if ret != 0 {
                eprintln!("Error enabling endpoint: {}", ret);
                Err(FabricError::InitError(ret))?;
            }
        }
        let address_bytes = unsafe {
            let mut len = 0;
            let ret = unsafe {libfabric_sys::inlined_fi_getname(&mut (*ep).fid, std::ptr::null_mut(), &mut len)};
            if ret != 0 {
                eprintln!("Error getting endpoint name: {}", ret);
                Err(FabricError::InitError(ret))?;
            }
            let mut addr = vec![0u8; len];
            let ret = unsafe {libfabric_sys::inlined_fi_getname(&mut (*ep).fid, addr.as_mut_ptr().cast(), &mut len)};
            if ret != 0 {
                eprintln!("Error getting endpoint name: {}", ret);
                Err(FabricError::InitError(ret))?;
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
            let mapped_addresses = vec![0u64; unmapped_addresses.len()];
            let total_size = unmapped_addresses.iter().fold(0, |acc, unmapped_addresses| acc + unmapped_addresses.as_bytes().len());
            let mut serialized: Vec<u8> = Vec::with_capacity(total_size);
            for a in unmapped_addresses {
                serialized.extend(a.as_bytes().iter())
            }
            let ret = libfabric_sys::inlined_fi_av_insert(av, serialized.as_mut_ptr().cast(), mapped_addresses.len(), mapped_addresses.as_mut_ptr(), 0, std::ptr::null_mut());
            if ret != 0 {
                eprintln!("Error inserting addresses into AV: {}", ret);
                Err(FabricError::InitError(ret))?;
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

    fn atomic_avail_inner<T>(&self) -> bool {
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

    fn atomic_op_avail_inner<T>(&self, op: AtomicOpKind) -> bool {
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

    pub(crate) fn atomic_op_avail<T: 'static>(&self, op: LamellarAtomicOp<T>) -> bool {
        let op_kind = match op {
            LamellarAtomicOp::Min(_) => AtomicOpKind::Min,
            LamellarAtomicOp::Max(_) => AtomicOpKind::Max,
            LamellarAtomicOp::Sum(_) => AtomicOpKind::Sum,
            LamellarAtomicOp::Sub(_) => AtomicOpKind::Sum, // Sub can be implemented as Add with negative value
            LamellarAtomicOp::Prod(_) => AtomicOpKind::Prod,
            LamellarAtomicOp::BitOr(_) => AtomicOpKind::BitOr,
            LamellarAtomicOp::BitXor(_) => AtomicOpKind::BitXor,
            LamellarAtomicOp::BitAnd(_) => AtomicOpKind::BitAnd,
            LamellarAtomicOp::Read => AtomicOpKind::Read,
            LamellarAtomicOp::Write(_) => AtomicOpKind::Write,
            LamellarAtomicOp::Cas(_, _) => AtomicOpKind::Cas,
        };

        self.atomic_op_avail_inner::<T>(op_kind)
    }
}

fn rust_type_to_fi_type<T>() -> Option<u32> {
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
    pub(crate) mr_info_table: Arc<RwLock<Vec<LibfabricAlloc>>>,
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

    pub(crate) fn insert(&self, alloc: LibfabricAlloc) {
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

    pub(crate) fn remove_from_alloc(&self, mem_addr: &LibfabricAlloc) {
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
    ) -> AllocResult<LibfabricAlloc> {
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
        let remote_offset = remote_addr - remote_alloc_info.mem_address().as_ptr() as usize;
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
        let remote_offset = remote_addr - remote_alloc_info.mem_address().as_ptr() as usize;
        let alloc = alloc_info
            .sub_alloc(remote_offset, num_bytes)
            .expect("Failed to create one-sided allocation from remote PE and address");
        OneSidedLibfabricAlloc { alloc, remote_pe }.into()
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
        let remote_offset = remote_addr - remote_alloc_info.mem_address().as_ptr() as usize;
        Some((alloc_info.clone().into(), remote_offset))
    }

    pub(crate) fn remote_addr(&self, remote_pe: usize, local_addr: usize) -> Option<usize> {
        let table = self.mr_info_table.read();
        if let Some(alloc_info) = table.iter().find(|x| x.contains(&local_addr)) {
            if let Some(remote_alloc_info) = alloc_info.remote_allocs.get(&remote_pe) {
                let local_offset = local_addr - alloc_info.start();
                Some(unsafe { remote_alloc_info.mem_address().add(local_offset).as_ptr() as usize })
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

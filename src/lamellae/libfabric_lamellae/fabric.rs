use libfabric::{
    av::{AddressVector, AddressVectorBuilder, AvInAddress},
    av_set::AddressVectorSetBuilder,
    cntr::{Counter, CounterBuilder, ReadCntr, WaitCntr},
    comm::{
        atomic::{AtomicCASEp, AtomicFetchEp, AtomicValidEp, AtomicWriteEp},
        collective::{CollectiveAttr, CollectiveEp},
        rma::{ReadEp, WriteEp},
    }, connless_ep::ConnectionlessEndpoint, cq::{Completion, CompletionQueue, CompletionQueueBuilder, ReadCq}, domain::{Domain, DomainBuilder}, enums::{
        AVOptions, AddressFormat, AtomicOp, CollectiveOp, CollectiveOptions, CompareAtomicOp, EndpointType, FetchAtomicOp, HmemIface, JoinOptions, Mode, MrMode, Progress, ReduceOp, ResourceMgmt, TrafficClass, TransferOptions
    }, ep::{Address, BaseEndpoint, Endpoint, EndpointBuilder}, eq::{Event, EventQueue, EventQueueBuilder, JoinCompleteEvent, ReadEq}, fabric::{Fabric, FabricBuilder}, info::{libfabric_version, Info, InfoEntry}, infocapsoptions::InfoCaps, mcast::{MultiCastGroup, MulticastGroupBuilder}, mr::{DisabledMemoryRegion, MaybeDisabledMemoryRegion, MemoryRegion, MemoryRegionBuilder}, *
};

use crate::{
    lamellae::{
        collective::{AllReduceOp, ReduceOp as LamellarReduceOp, RootOrSliceMut, RootSrcOrSliceMut}, comm::{alloc::*, error::{AllocError, AllocResult, FabricError, FabricResult}}, AllocationType, AtomicOp as LamellarAtomicOp
    },
    lamellar_alloc::{BTreeAlloc, LamellarAlloc},
};

#[cfg(feature = "enable-on-node-shmem")]
use crate::{
    config,
    lamellae::shmem_utils::{attach_shmem_segment, ShmemSegment},
};

use libc::{sysconf, _SC_PAGESIZE, _SC_PHYS_PAGES};
use parking_lot::{Mutex, RwLock};
use pmi::{pmi::Pmi, pmi::PmiBuilder};
use std::{
    collections::HashMap,
    env,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};
use tracing::{debug, trace};

type WaitableEq = libfabric::eq_caps_type!(EqCaps::WAIT);
type WaitableCq = libfabric::cq_caps_type!(CqCaps::WAIT);
type WaitableCntr = libfabric::cntr_caps_type!(CntrCaps::WAIT);
type RmaAtomicCollEp =
    libfabric::info_caps_type!(FabInfoCaps::ATOMIC, FabInfoCaps::RMA, FabInfoCaps::COLL);

fn node_total_memory_bytes() -> Option<u64> {
    let pages = unsafe { sysconf(_SC_PHYS_PAGES) };
    let page_size = unsafe { sysconf(_SC_PAGESIZE) };
    if pages <= 0 || page_size <= 0 {
        return None;
    }

    let pages = pages as u64;
    let page_size = page_size as u64;
    pages.checked_mul(page_size)
}

// #[derive(Debug)]
enum BarrierImpl {
    Uninit,
    Collective(MultiCastGroup),
    Manual(LibfabricAlloc, AtomicUsize),
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

struct CommGroup {
    mapped_addresses: Vec<MappedAddress>,
    ep: ConnectionlessEndpoint<RmaAtomicCollEp>,
    cq: CompletionQueue<WaitableCq>,
    put_cntr: Counter<WaitableCntr>,
    get_cntr: Counter<WaitableCntr>,
    av: AddressVector,
    eq: EventQueue<WaitableEq>,
    info_entry: Arc<InfoEntry<RmaAtomicCollEp>>,
    put_cnt: AtomicU64,
    get_cnt: AtomicU64,
    lock: Mutex<()>,
    contexts_cache: Arc<Mutex<Vec<CachedContext>>>,
    contexts_cache_size_per_thread: usize,
}


pub(crate) struct CachedContext{
    context: Option<libfabric::Context>,
    cache: Arc<Mutex<Vec<CachedContext>>>,
}

impl Drop for CachedContext{
    fn drop(&mut self) {
        let mut cache = self.cache.lock();
        cache.push(
            CachedContext{
                context: self.context.take(),
                cache: self.cache.clone(),
            }
        );
    }
}

#[derive(Clone)]
pub(crate) enum LibfabricMem {
    Mmap(Arc<memmap::MmapMut>),
    #[cfg(feature = "enable-on-node-shmem")]
    Shmem(Arc<ShmemSegment>),
}

impl LibfabricMem {
    fn as_ptr(&self) -> *mut u8 {
        match self {
            LibfabricMem::Mmap(mem) => mem.as_ptr() as *mut u8,
            #[cfg(feature = "enable-on-node-shmem")]
            LibfabricMem::Shmem(segment) => segment.base_ptr(),
        }
    }

    fn len(&self) -> usize {
        match self {
            LibfabricMem::Mmap(mem) => mem.len(),
            #[cfg(feature = "enable-on-node-shmem")]
            LibfabricMem::Shmem(segment) => segment.len(),
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
    #[cfg(feature = "enable-on-node-shmem")]
    same_node_pes: Vec<bool>,
    #[cfg(feature = "enable-on-node-shmem")]
    disable_on_node_shmem: bool,
    #[cfg(feature = "enable-on-node-shmem")]
    job_id: usize,

    barrier_impl: RwLock<BarrierImpl>,
    info_entry: Arc<InfoEntry<RmaAtomicCollEp>>,
    domain: Domain,
    _fabric: Fabric,
    _my_pmi: Arc<dyn Pmi>,
     alloc_manager: Arc<AllocInfoManager>,
    pub(crate)comm_group: CommGroup,
}

impl CommGroup{
    
    fn allocate_context(&self) -> CachedContext {
        let mut cache = self.contexts_cache.lock();
        if let Some(ctx) = cache.pop() {
            ctx
        } else {
            for _ in 0..self.contexts_cache_size_per_thread - 1 {
                cache.push(
                    CachedContext{
                        context: Some(self.info_entry.allocate_context()),
                        cache: self.contexts_cache.clone(),
                    }
                );
            }
            CachedContext{
                context: Some(self.info_entry.allocate_context()),
                cache: self.contexts_cache.clone(),
            }
        }
    }

    fn wait_for_join_event(&self, ctx: &Context) -> Result<JoinCompleteEvent, libfabric::error::Error> {
        let _lock = self.lock.lock();
        loop {
            let eq_res = self.eq.read();

            match eq_res {
                Ok(event) => {
                    if let Event::JoinComplete(entry) = event {
                        if entry.is_context_equal(ctx) {
                            return Ok(entry);
                        }
                    }
                }
                Err(err) => {
                    if !matches!(err.kind, libfabric::error::ErrorKind::TryAgain) {
                        return Err(err);
                    }
                }
            }
            self.progress()?;
        }
    }

    fn progress(&self) -> Result<(), libfabric::error::Error> {
        let cq_res = self.cq.read(0);

        match cq_res {
            Ok(_) => Ok(()),
            Err(err) => {
                if !matches!(err.kind, libfabric::error::ErrorKind::TryAgain) {
                    Err(err)
                } else {
                    Ok(())
                }
            }
        }
    }

    pub(crate) fn wait_for_completion(&self, ctx: &CachedContext) -> Result<(), libfabric::error::Error> {
        let _lock = self.lock.lock();
        self.wait_for_completion_inner(ctx.context.as_ref().unwrap())
    }

    fn wait_for_completion_inner(&self, ctx: &Context) -> Result<(), libfabric::error::Error> {
        loop {
            let cq_res = self.cq.read(1);
            match cq_res {
                Ok(completion) => match completion {
                    Completion::Ctx(entries) | Completion::Unspec(entries) => {
                        if entries[0].is_op_context_equal(ctx) {
                            return Ok(());
                        }
                        else {
                            panic!("got completion for context that does not match expected context");
                        }
                    }
                    Completion::Msg(entries) => {
                        if entries[0].is_op_context_equal(ctx) {
                            return Ok(());
                        }
                        else {
                            panic!("got completion for context that does not match expected context");
                        }
                    }
                    Completion::Data(entries) => {
                        if entries[0].is_op_context_equal(ctx) {
                            return Ok(());
                        }
                        else {
                            panic!("got completion for context that does not match expected context");
                        }
                    }
                    Completion::Tagged(entries) => {
                        if entries[0].is_op_context_equal(ctx) {
                            return Ok(());
                        }
                        else {
                            panic!("got completion for context that does not match expected context");
                        }
                    }
                },
                Err(err) => {
                    if !matches!(err.kind, libfabric::error::ErrorKind::TryAgain) {
                        return Err(err);
                    }
                }
            }
            std::thread::yield_now();
        }
    }

    pub(crate) fn wait_all(&self) -> Result<(), libfabric::error::Error> {
        trace!("wait_all");
        self.wait_for_tx_cntr()?;
        trace!("wait_all put done");
        self.wait_for_rx_cntr()?;
        trace!("wait_all done");
        Ok(())
    }

    fn wait_for_tx_cntr(&self) -> Result<(), libfabric::error::Error> {
        self.wait_for_cntr(&self.put_cnt, &self.put_cntr, "tx")
    }

    fn wait_for_rx_cntr(&self) -> Result<(), libfabric::error::Error> {
        self.wait_for_cntr(&self.get_cnt, &self.get_cntr, "rx")
    }

    fn wait_for_cntr(
        &self,
        pending: &AtomicU64,
        cntr: &Counter<WaitableCntr>,
        dir: &str,
    ) -> Result<(), libfabric::error::Error> {
        let _lock = self.lock.lock();
        let mut prev_expected_cnt = pending.load(Ordering::SeqCst);
        let mut old_cnt = cntr.read();
        let mut expected_cnt = pending.load(Ordering::SeqCst);
        let mut cur_cnt = old_cnt;
        trace!(
                "{dir} before.  expected_cnt {expected_cnt} prev_expected_cnt {prev_expected_cnt} cur_cnt {} old_cnt {old_cnt} ",
                cntr.read(),
            );
        // let mut timer = std::time::Instant::now();
        // drop(_guard);

        while cur_cnt < expected_cnt || prev_expected_cnt < expected_cnt || cur_cnt != old_cnt {
            prev_expected_cnt = expected_cnt;
            old_cnt = cur_cnt;
            let _ = self.progress();
            let wait_result = cntr.wait(prev_expected_cnt as u64, -1);

            if let Err(err) = wait_result {
                if let libfabric::error::ErrorKind::TimedOut = err.kind {
                    if let Err(err) = self.progress() {
                        match err.kind {
                            libfabric::error::ErrorKind::TryAgain => {}
                            _ => return Err(err),
                        }
                    }
                }
            }

            cur_cnt = cntr.read();
            expected_cnt = pending.load(Ordering::SeqCst);
            std::thread::yield_now();
        }
        trace!(
            target: "libfabric",
            "{dir} after.   expected_cnt {expected_cnt} prev_expected_cnt {prev_expected_cnt} cur_cnt {} old_cnt {old_cnt} ",
            cntr.read(),
        );
        // }
        Ok(())
    }

    fn post_collective(
        &self,
        blocking: bool,
        mut fun: impl FnMut(&mut Context) -> Result<(), libfabric::error::Error>,
    ) -> Result<CachedContext, libfabric::error::Error> {
        let _lock = self.lock.lock();
        let mut cached_ctx = self.allocate_context();
        let mut ctx = cached_ctx.context.as_mut().unwrap();
        loop {
            match fun(&mut ctx) {
                Ok(_) => break,
                Err(error) => {
                    if matches!(error.kind, libfabric::error::ErrorKind::TryAgain) {
                        self.progress()?;
                    } else {
                        return Err(error);
                    }
                }
            }
        }
        if blocking {
            self.wait_for_completion_inner(&ctx)?;
        }
        Ok(cached_ctx)
    }

    fn post_put(
        &self,
        blocking: bool,
        mut fun: impl FnMut() -> Result<(), libfabric::error::Error>,
    ) -> Result<u64, libfabric::error::Error> {
        let _lock = self.lock.lock();
        self.put_cnt
            .fetch_max(self.put_cntr.read(), Ordering::SeqCst);
        loop {
            match fun() {
                Ok(_) => break,
                Err(error) => {
                    if matches!(error.kind, libfabric::error::ErrorKind::TryAgain) {
                        // trace!("need to progress, retrying put");
                        self.progress()?;
                    } else {
                        return Err(error);
                    }
                }
            }
        }
        trace!("done posting put");
        let cnt = self.put_cnt.fetch_add(1, Ordering::SeqCst) + 1;
        if blocking {
            self.put_cntr.wait(cnt, -1)?;
        }
        Ok(cnt)
    }

    fn post_get(
        &self,
        blocking: bool,
        mut fun: impl FnMut() -> Result<(), libfabric::error::Error>,
    ) -> Result<u64, libfabric::error::Error> {
        let _lock = self.lock.lock();
        let old_cnt = self
            .get_cnt
            .fetch_max(self.get_cntr.read(), Ordering::SeqCst);
        // let old_cnt = self.get_cnt.load(Ordering::SeqCst);
        loop {
            match fun() {
                Ok(_) => break,
                Err(error) => {
                    if matches!(error.kind, libfabric::error::ErrorKind::TryAgain) {
                        self.progress()?;
                        // std::thread::yield_now();
                    } else {
                        return Err(error);
                    }
                }
            }
        }
        let new_cnt = self.get_cnt.fetch_add(1, Ordering::SeqCst) + 1;
        trace!(target: "libfabric", "done posting get {} {}", old_cnt, new_cnt);

        if blocking {
            self.get_cntr.wait(new_cnt, -1)?;
        }

        Ok(new_cnt)
    }
}

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
        if env::var_os("FI_UNIVERSE").is_none() {
            env::set_var("FI_UNIVERSE", num_pes.to_string());
        }

        if env::var_os("FI_MR_CACHE_MAX_SIZE").is_none() {
            if let Some(total_bytes) = node_total_memory_bytes() {
                env::set_var("FI_MR_CACHE_MAX_SIZE", total_bytes.to_string());
            } else {
                eprintln!(
                    "Warning: unable to determine total system memory for FI_MR_CACHE_MAX_SIZE"
                );
            }
        }

        #[cfg(feature = "enable-on-node-shmem")]
        let disable_on_node_shmem = config().disable_on_node_shmem.unwrap_or(false);
        #[cfg(feature = "enable-on-node-shmem")]
        let mut same_node_pes = vec![false; num_pes];
        #[cfg(feature = "enable-on-node-shmem")]
        if !disable_on_node_shmem {
            let ranks_on_node = my_pmi.ranks_on_node(my_pmi.node());
            if !ranks_on_node.is_empty() {
                for pe in ranks_on_node {
                    if pe < same_node_pes.len() {
                        same_node_pes[pe] = true;
                    }
                }
            }
        }
        #[cfg(feature = "enable-on-node-shmem")]
        let job_id = my_pmi.job_id();

        // trace!("Using PMI my_pe {} num_pes {}",my_pmi.rank(), num_pes);

        let info = Info::new(&libfabric_version())
            .enter_hints()
            .caps(InfoCaps::new().rma().atomic().collective())
            .mode(Mode::new().context())
            .enter_ep_attr()
            .type_(EndpointType::Rdm)
            .leave_ep_attr()
            .enter_domain_attr()
            .mr_mode(
                MrMode::new().prov_key().allocated().virt_addr(), // .local()
                                                                  // .endpoint()
                                                                  // .raw(),
            )
            .resource_mgmt(ResourceMgmt::Enabled)
            .data_progress(Progress::Manual)
            .leave_domain_attr()
            .enter_tx_attr()
            .traffic_class(TrafficClass::LowLatency)
            .op_flags(TransferOptions::new().delivery_complete())
            .leave_tx_attr()
            .addr_format(AddressFormat::Unspec)
            .leave_hints()
            .get()
            .map_err(|e| FabricError::InitError(e.c_err))?;

        // trace!("Found the following providers");
        let info_entry = info
            .into_iter()
            .find(|e| {
                if let Some(prov) = provider {
                    if let Some(dom) = domain {
                        e.fabric_attr().prov_name().split(';').any(|s| s == prov)
                            && e.domain_attr().name().split(';').any(|s| s == dom)
                    } else {
                        e.fabric_attr().prov_name().split(';').any(|s| s == prov)
                    }
                } else {
                    if let Some(dom) = domain {
                        e.domain_attr().name().split(';').any(|s| s == dom)
                    } else {
                        eprintln!("Warning: No provider/domain requested");
                        true
                    }
                }
            })
            .expect(&format!(
                "Error! No provider with name {:?} / domain {:?} was found",
                provider, domain
            ));
        //trace!("Using provider: {:?}", info_entry);

        let fabric = FabricBuilder::new()
            .build(&info_entry)
            .map_err(|e| FabricError::InitError(e.c_err))?;

        let domain = DomainBuilder::new(&fabric, &info_entry)
            .build()
            .map_err(|e| FabricError::InitError(e.c_err))?;
        let mut coll_attr = CollectiveAttr::<()>::new();
        domain
            .query_collective::<()>(CollectiveOp::AllGather, &mut coll_attr)
            .map_err(|e| FabricError::InitError(e.c_err))?;

        let info_entry = Arc::new(info_entry);

        let eq = EventQueueBuilder::new(&fabric)
            .build()
            .map_err(|e| FabricError::InitError(e.c_err))?;

        let cq = CompletionQueueBuilder::new()
            .format(libfabric::enums::CqFormat::Context)
            .size(info_entry.rx_attr().size())
            .build(&domain)
            .map_err(|e| FabricError::InitError(e.c_err))?;

        let av = AddressVectorBuilder::new()
            .build(&domain)
            .map_err(|e| FabricError::InitError(e.c_err))?;

        let put_cntr = CounterBuilder::new()
            .build(&domain)
            .map_err(|e| FabricError::InitError(e.c_err))?;
        let get_cntr = CounterBuilder::new()
            .build(&domain)
            .map_err(|e| FabricError::InitError(e.c_err))?; //

        let ep = EndpointBuilder::new(&info_entry)
            .build_with_shared_cq(&domain, &cq, true)
            // .build_scalable(&domain)
            .map_err(|e| FabricError::InitError(e.c_err))?;
        let ep = match ep {
            Endpoint::ConnectionOriented(_) => {
                panic!("Verbs should be connectionless, I think")
            }
            Endpoint::Connectionless(ep) => ep,
        };

        ep.bind_cntr()
            .write()
            .cntr(&put_cntr)
            .map_err(|e| FabricError::InitError(e.c_err))?;

        ep.bind_cntr()
            .read()
            .cntr(&get_cntr)
            .map_err(|e| FabricError::InitError(e.c_err))?;

        ep.bind_eq(&eq)
            .map_err(|e| FabricError::InitError(e.c_err))?;

        let ep = ep
            .enable(&av)
            .map_err(|e| FabricError::InitError(e.c_err))?;

        let address = ep.getname().map_err(|e| FabricError::InitError(e.c_err))?;
        let address_bytes = address.as_bytes();

        my_pmi.put(&format!("epname"), address_bytes).unwrap();
        my_pmi.exchange().unwrap();

        let unmapped_addresses: Vec<_> = my_pmi
            .ranks()
            .iter()
            .map(|r| {
                let addr = my_pmi
                    .get(&format!("epname"), &address_bytes.len(), &r)
                    .unwrap();
                unsafe { Address::from_bytes(&addr) }
            })
            .collect();

            let mapped_addresses = av
                .insert(AvInAddress::Encoded(&unmapped_addresses), AVOptions::new())
                .map_err(|e| FabricError::InitError(e.c_err))?;
            let mapped_addresses: Vec<MappedAddress> =
                mapped_addresses.into_iter().map(|a| a.unwrap()).collect();
                    
            
            let mut contexts = Arc::new(Mutex::new(Vec::with_capacity(10)));
            for _ in 0..10 {
                contexts.lock().push(
                    CachedContext{
                        context: Some(info_entry.allocate_context()),
                        cache: contexts.clone(),
                    }
                );
            }
            
            let comm_group = CommGroup{
                mapped_addresses,
                ep,
                cq,
                put_cntr,
                get_cntr,
                av,
                eq,
                info_entry: info_entry.clone(),
                put_cnt: AtomicU64::new(0),
                get_cnt: AtomicU64::new(0),
                lock: Mutex::new(()),
                contexts_cache: contexts,
                contexts_cache_size_per_thread,
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
            info_entry,
            _fabric: fabric,
            domain,
            alloc_manager: Arc::new(alloc_manager),
            barrier_impl: RwLock::new(BarrierImpl::Pmi(my_pmi)),
            comm_group,
        });

        // ofi.init_barrier()?;

        Ok(ofi)
    }

    fn atomic_avail_inner<T: AsFiType>(&self) -> bool {
        let cg = &self.comm_group;
        unsafe {
            cg.ep.atomicvalid::<T>(AtomicOp::Sum).is_ok()
                && cg
                    .ep
                    .fetch_atomicvalid::<T>(FetchAtomicOp::AtomicRead)
                    .is_ok()
                && cg
                    .ep
                    .compare_atomicvalid::<T>(CompareAtomicOp::Cswap)
                    .is_ok()
        }
    }

    fn atomic_op_avail_inner<T: AsFiType>(&self, op: AtomicOpKind) -> bool {
        let cg = &self.comm_group;
        unsafe {
            match op {
                AtomicOpKind::Min => {
                    cg.ep.atomicvalid::<T>(AtomicOp::Min).is_ok()
                        && cg.ep.fetch_atomicvalid::<T>(FetchAtomicOp::Min).is_ok()
                }
                AtomicOpKind::Max => {
                    cg.ep.atomicvalid::<T>(AtomicOp::Max).is_ok()
                        && cg.ep.fetch_atomicvalid::<T>(FetchAtomicOp::Max).is_ok()
                }
                AtomicOpKind::Sum => {
                    cg.ep.atomicvalid::<T>(AtomicOp::Sum).is_ok()
                        && cg.ep.fetch_atomicvalid::<T>(FetchAtomicOp::Sum).is_ok()
                }
                AtomicOpKind::Prod => {
                    cg.ep.atomicvalid::<T>(AtomicOp::Prod).is_ok()
                        && cg.ep.fetch_atomicvalid::<T>(FetchAtomicOp::Prod).is_ok()
                }
                AtomicOpKind::BitOr => {
                    cg.ep.atomicvalid::<T>(AtomicOp::Bor).is_ok()
                        && cg.ep.fetch_atomicvalid::<T>(FetchAtomicOp::Bor).is_ok()
                }
                AtomicOpKind::BitXor => {
                    cg.ep.atomicvalid::<T>(AtomicOp::Bxor).is_ok()
                        && cg.ep.fetch_atomicvalid::<T>(FetchAtomicOp::Bxor).is_ok()
                }
                AtomicOpKind::BitAnd => {
                    cg.ep.atomicvalid::<T>(AtomicOp::Band).is_ok()
                        && cg.ep.fetch_atomicvalid::<T>(FetchAtomicOp::Band).is_ok()
                }
                AtomicOpKind::Read => cg
                    .ep
                    .fetch_atomicvalid::<T>(FetchAtomicOp::AtomicRead)
                    .is_ok(),
                AtomicOpKind::Write => {
                    cg.ep.atomicvalid::<T>(AtomicOp::AtomicWrite).is_ok()
                        && cg
                            .ep
                            .fetch_atomicvalid::<T>(FetchAtomicOp::AtomicWrite)
                            .is_ok()
                }
                AtomicOpKind::Cas => cg
                    .ep
                    .compare_atomicvalid::<T>(CompareAtomicOp::Cswap)
                    .is_ok(),
            }
        }
    }

    fn allocate_context(&self) -> CachedContext {
        self.comm_group.allocate_context()
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

    pub(crate) fn atomic_op_avail<T: 'static>(&self, op: LamellarAtomicOp<T>) -> bool {
        let op_kind = match op {
            LamellarAtomicOp::Min(_) => AtomicOpKind::Min,
            LamellarAtomicOp::Max(_) => AtomicOpKind::Max,
            LamellarAtomicOp::Sum(_) | LamellarAtomicOp::Sub(_) => AtomicOpKind::Sum, // Sub can be implemented as Add with negative value
            LamellarAtomicOp::Prod(_) => AtomicOpKind::Prod,
            LamellarAtomicOp::BitOr(_) => AtomicOpKind::BitOr,
            LamellarAtomicOp::BitXor(_) => AtomicOpKind::BitXor,
            LamellarAtomicOp::BitAnd(_) => AtomicOpKind::BitAnd,
            LamellarAtomicOp::Read(_) => AtomicOpKind::Read,
            LamellarAtomicOp::Write(_) => AtomicOpKind::Write,
            LamellarAtomicOp::Cas(_, _) => AtomicOpKind::Cas,
            LamellarAtomicOp::FetchMin(_) => AtomicOpKind::Min,
            LamellarAtomicOp::FetchMax(_) => AtomicOpKind::Max,
            LamellarAtomicOp::FetchSum(_) | LamellarAtomicOp::FetchSub(_) => AtomicOpKind::Sum,
            LamellarAtomicOp::FetchProd(_) => AtomicOpKind::Prod,
            LamellarAtomicOp::FetchBitOr(_) => AtomicOpKind::BitOr,
            LamellarAtomicOp::FetchBitXor(_) => AtomicOpKind::BitXor,
            LamellarAtomicOp::FetchBitAnd(_) => AtomicOpKind::BitAnd,
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

    fn create_mc_group(&self, pes: &[usize]) -> Result<MultiCastGroup, libfabric::error::Error> {
        // trace!("Creating MC group of len: {}", pes.len());
        let cg = &self.comm_group;
        let mut av_set = AddressVectorSetBuilder::new_from_range(
            &cg.av,
            &cg.mapped_addresses[pes[0]],
            &cg.mapped_addresses[pes[0]],
            1,
        )
        .count(pes.len())
        .build()
        .unwrap();

        for pe in pes.iter().skip(1) {
            av_set.insert(&cg.mapped_addresses[*pe]).unwrap();
        }

        let mut cached_ctx = self.allocate_context();
        let mut ctx = cached_ctx.context.as_mut().unwrap();
        let mc = MulticastGroupBuilder::from_av_set(&av_set)
            .build()
            .join_collective_with_context(&cg.ep, JoinOptions::new(), &mut ctx)
            .unwrap();
        let join_event = cg.wait_for_join_event(&ctx).unwrap();
        let mc = mc.join_complete(join_event);
        // trace!("Done Creating MC group");

        Ok(mc)
    }

    fn collective_exchange_mr_info(
        &self,
        pes: &[usize],
        mem: &[u8],
        mr: &MemoryRegion,
    ) -> Result<HashMap<usize, RemoteMemAddressInfo>, libfabric::error::Error> {
        let mc = self.create_mc_group(&pes)?;
        let cg = &self.comm_group;

        let mut mem_info = MemAddressInfo::from_slice(mem, 0, &mr.key().unwrap(), &self.info_entry);

        let mut mem_info_bytes = mem_info.to_bytes_mut();
        let mut all_mem_info_bytes = vec![0u8; mem_info_bytes.len() * pes.len()];

        let ctx = cg.post_collective(
            true,
            |ctx|  {
                cg.ep.allgather_with_context(
                &mut mem_info_bytes,
                None,
                &mut all_mem_info_bytes,
                None,
                &mc,
                CollectiveOptions::new(),
                ctx,
                )
            }
        )?;
        let all_mem_info: HashMap<_, _> = all_mem_info_bytes
            .chunks_exact(std::mem::size_of::<MemAddressInfo>())
            .enumerate()
            .map(|(pe, chunk)| {
                let mem_info = unsafe { MemAddressInfo::from_bytes(chunk) };
                let rem_mem_info = mem_info
                    .into_remote_info(&self.domain)
                    .expect("Failed to convert MemAddressInfo to RemoteMemAddressInfo");
                trace!(target: "libfabric",
                    "PE {}: RemoteMemAddressInfo: {:?} {:?} ",
                    pes[pe],
                    rem_mem_info.mem_address(),
                    rem_mem_info.mem_len()
                );

                (pes[pe], rem_mem_info)
            })
            .collect();

        Ok(all_mem_info)
    }

    fn init_barrier(self: &Arc<Ofi>) -> FabricResult<()> {
        let mut coll_attr = CollectiveAttr::<()>::new();

        if self
            .domain
            .query_collective::<()>(CollectiveOp::Barrier, &mut coll_attr)
            .is_err()
            || true
        {
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

            *self.barrier_impl.write() = BarrierImpl::Manual(barrier_addr, AtomicUsize::new(0));
            Ok(())
        } else {
            let all_pes: Vec<_> = (0..self.num_pes).collect();
            *self.barrier_impl.write() = BarrierImpl::Collective(
                self.create_mc_group(&all_pes)
                    .map_err(|e| FabricError::BarrierError(e.c_err))?,
            );
            Ok(())
        }
    }
    pub(crate) fn clear_barrier(&self) {
        let mut barrier_impl = self.barrier_impl.write();
        *barrier_impl = BarrierImpl::Uninit;
    }
    pub(crate) fn alloc(
        self: &Arc<Ofi>,
        size: usize,
        alloc: AllocationType,
        align: usize,
    ) -> AllocResult<LibfabricAlloc> {
        match alloc {
            AllocationType::Sub(pes) => self.sub_alloc(&pes, size, align),
            AllocationType::Global => self.full_alloc(size, align),
            _ => return Err(AllocError::UnexpectedAllocationType(alloc)),
        }
    }

    fn full_alloc(self: &Arc<Ofi>, data_size: usize, align: usize) -> AllocResult<LibfabricAlloc> {
        //add space for ref count and padding to align it
        let (padding, size, _align) = calc_alloc_padding_size_align(data_size, align);

        // Align to page boundaries
        let aligned_size = if (self.alloc_manager.page_size() - 1) & size != 0 {
            (size + self.alloc_manager.page_size()) & !(self.alloc_manager.page_size() - 1)
        } else {
            size
        };

        trace!(target: "libfabric", "Full Allocating aligned size: {} aligned", aligned_size);
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
            (LibfabricMem::Mmap(Arc::new(mmap)), mem_base_ptr)
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
                LibfabricMem::Mmap(Arc::new(mmap)),
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
                LibfabricMem::Shmem(local_segment),
                mem_base_ptr,
                same_node_bases,
                same_node_segments,
            )
        };
        let mem_slice = unsafe { std::slice::from_raw_parts_mut(mem_base_ptr, aligned_size) };

        let mr = MemoryRegionBuilder::new(mem_slice, HmemIface::System)
            .requested_key(self.alloc_manager.next_key() as u64)
            .access_read()
            .access_write()
            .access_remote_read()
            .access_remote_write()
            .build(&self.domain)
            .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?;

        let mr = match mr {
            MaybeDisabledMemoryRegion::Disabled(mr) => {
                match mr {
                    DisabledMemoryRegion::EpBind(mr) => {
                        // trace!("Binding memory region to endpoint");
                        mr.enable(&self.comm_group.ep)
                            .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?
                    }
                    DisabledMemoryRegion::RmaEvent(mr) => {
                        // trace!("Binding memory region to domain");
                        mr.enable()
                            .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?
                        // This will bind the memory region to the domain
                    }
                }
            }
            MaybeDisabledMemoryRegion::Enabled(mr) => mr,
        };

        // let remote_alloc_infos = self.pmi_exchange_mr_info(&mem, &mr);
        let remote_alloc_infos = self
            .collective_exchange_mr_info(&(0..self.num_pes).collect::<Vec<_>>(), mem_slice, &mr)
            .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?;

        let mcast_group = self.create_mc_group(&(0..self.num_pes).collect::<Vec<_>>())
            .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?;
        let alloc = LibfabricAlloc::new(
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
        .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?;
        self.alloc_manager.insert(alloc.clone());
        Ok(alloc)
    }

    fn sub_alloc(
        self: &Arc<Ofi>,
        pes: &[usize],
        data_size: usize,
        align: usize,
    ) -> AllocResult<LibfabricAlloc> {
        //add space for ref count and padding to align it
        let (padding, size, _align) = calc_alloc_padding_size_align(data_size, align);
        // Align to page boundaries
        let aligned_size = if (self.alloc_manager.page_size() - 1) & size != 0 {
            (size + self.alloc_manager.page_size()) & !(self.alloc_manager.page_size() - 1)
        } else {
            size
        };

        trace!(target: "libfabric", "Sub Allocating aligned size: {} aligned pes: {:?}", aligned_size, pes);

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
            (LibfabricMem::Mmap(Arc::new(mmap)), mem_base_ptr)
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

        let mr = MemoryRegionBuilder::new(mem_slice, HmemIface::System)
            .requested_key(self.alloc_manager.next_key() as u64)
            .access_read()
            .access_write()
            .access_remote_read()
            .access_remote_write()
            .build(&self.domain)
            .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?;

        let mr = match mr {
            MaybeDisabledMemoryRegion::Disabled(mr) => {
                match mr {
                    DisabledMemoryRegion::EpBind(mr) => {
                        // trace!("Binding memory region to endpoint");
                        mr.enable(&self.comm_group.ep)
                            .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?
                    }
                    DisabledMemoryRegion::RmaEvent(mr) => {
                        // trace!("Binding memory region to domain");
                        mr.enable()
                            .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?
                        // This will bind the memory region to the domain
                    }
                }
            }
            MaybeDisabledMemoryRegion::Enabled(mr) => mr,
        };

        let remote_alloc_infos = self
            .collective_exchange_mr_info(pes, mem_slice, &mr)
            .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?;

        
        let mcast_group = self.create_mc_group(pes)
            .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?;

        
        let alloc = LibfabricAlloc::new(
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
        .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?;
        self.alloc_manager.insert(alloc.clone());
        Ok(alloc)
    }

    pub(crate) fn get_alloc_from_start_addr(
        &self,
        addr: CommAllocAddr,
    ) -> AllocResult<LibfabricAlloc> {
        self.alloc_manager.get_alloc_from_start_addr(addr)
    }

    pub(crate) fn clear_allocs(&self) -> Result<(), libfabric::error::Error> {
        self.alloc_manager.clear();
        Ok(())
    }

    pub(crate) fn barrier(&self) -> Result<(), libfabric::error::Error> {
        // trace!("Running barrier");
        match &*self.barrier_impl.read() {
            BarrierImpl::Uninit => {
                panic!("Barrier is not initialized");
            }
            BarrierImpl::Collective(mc) => {
                let cg = &self.comm_group;
                cg.post_collective(true, |ctx| {
                    cg.ep.barrier_with_context(mc, ctx)
                })?;
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
                            )?
                        };
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
                            self.comm_group.progress()?;
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

    pub(crate) fn wait_all(&self) -> Result<(), libfabric::error::Error> {
        self.comm_group.wait_all()
    }

    pub(crate) fn wait_for_completion(&self, ctx: &CachedContext) -> Result<(), libfabric::error::Error> {
        self.comm_group.wait_for_completion(ctx)
    }

    pub(crate) fn wait_for_completion(&self, ctx: &CachedContext) -> Result<(), libfabric::error::Error> {
        self.comm_group.wait_for_completion(ctx)
    }

    pub(crate) fn thread_wait(&self) -> Result<(), libfabric::error::Error> {
        self.comm_group.wait_all()
    }

    pub(crate) fn progress_all(&self) -> Result<(), libfabric::error::Error> {
        let _lock = self.comm_group.lock.lock();
        self.comm_group.progress()
    }

    pub(crate) fn thread_progress(&self) -> Result<(), libfabric::error::Error> {
        let _lock = self.comm_group.lock.lock();
        self.comm_group.progress()
    }
}

impl Drop for Ofi {
    fn drop(&mut self) {
        trace!(target: "libfabric", "Dropping OFI backend");
        let _ = self.comm_group.wait_all();

        self._my_pmi
            .barrier(false)
            .expect("PMI Barrier failed during OFI drop");
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

pub(crate) struct LibfabricAlloc {
    pub(crate) ofi: Arc<Ofi>,
    mem: LibfabricMem,
    #[cfg(feature = "enable-on-node-shmem")]
    same_node_bases: Arc<Vec<Option<usize>>>,
    #[cfg(feature = "enable-on-node-shmem")]
    same_node_segments: Arc<Vec<Option<Arc<ShmemSegment>>>>,
    mr: MemoryRegion,
    range: std::ops::Range<usize>,
    remote_allocs: HashMap<usize, RemoteMemAddressInfo>,
    fabric_ref_cnt_offset: usize,
    rt_ref_cnt_offset: usize,
    id: usize,
    alloc_table: AllocTable,
    mcast_group: Option<MultiCastGroup>,
    pub(crate) print: bool,
}

impl std::fmt::Debug for LibfabricAlloc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fabric_ref_count = unsafe {
            (&*(self.mem.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize))
                .load(Ordering::SeqCst)
        };

        let mut temp = f.debug_struct("LibfabricAlloc");
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

impl Clone for LibfabricAlloc {
    fn clone(&self) -> Self {
        self.increment_fabric_ref_count();
        if let AllocTable::Runtime(_, _, _) = &self.alloc_table {
            self.increment_rt_ref_count();
        }
        get_ref_count(unsafe {
            &*(self.mem.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize)
        });
        trace!(target: "libfabric", "Cloned LibfabricAlloc: {:?}", self);
        Self {
            ofi: self.ofi.clone(),
            mem: self.mem.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_bases: self.same_node_bases.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_segments: self.same_node_segments.clone(),
            mr: self.mr.clone(),
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

impl From<LibfabricAlloc> for CommAlloc {
    fn from(alloc: LibfabricAlloc) -> Self {
        CommAlloc {
            inner_alloc: CommAllocInner::LibfabricAlloc(alloc),
            alloc_type: CommAllocType::Fabric,
        }
    }
}

static ALLOC_ID: AtomicUsize = AtomicUsize::new(0);
#[cfg(feature = "enable-on-node-shmem")]
static LIBFABRIC_SHMEM_ALLOC_ID: AtomicUsize = AtomicUsize::new(0);

#[cfg(feature = "enable-on-node-shmem")]
fn build_same_node_segments(
    pes: &[usize],
    same_node_pes: &[bool],
    job_id: usize,
    size: usize,
    align: usize,
    alloc_id: usize,
    my_pe: usize,
) -> (Vec<Option<usize>>, Vec<Option<Arc<ShmemSegment>>>) {
    let mut bases = vec![None; same_node_pes.len()];
    let mut segments = vec![None; same_node_pes.len()];

    for pe in pes {
        if !same_node_pes.get(*pe).copied().unwrap_or(false) {
            continue;
        }
        if *pe == my_pe {
            continue;
        }
        let shmem_id = format!("libfabric_alloc_{}_pe_{}", alloc_id, pe);
        let segment = attach_shmem_segment(job_id, size, align, &shmem_id, alloc_id, false);
        bases[*pe] = Some(segment.base_ptr() as usize);
        segments[*pe] = Some(Arc::new(segment));
    }

    (bases, segments)
}
impl LibfabricAlloc {
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
        mem: LibfabricMem,
        #[cfg(feature = "enable-on-node-shmem")] same_node_bases: Arc<Vec<Option<usize>>>,
        #[cfg(feature = "enable-on-node-shmem")] same_node_segments: Arc<
            Vec<Option<Arc<ShmemSegment>>>,
        >,
        mr: MemoryRegion,
        remote_allocs: HashMap<usize, RemoteMemAddressInfo>,
        num_bytes: usize,
        padding: usize,
        alloc_table: Arc<AllocInfoManager>,
        mcast_group: Option<MultiCastGroup>,
    ) -> Result<Self, libfabric::error::Error> {
        let start = mem.as_ptr() as usize;
        let end = start + num_bytes; //mem.len();
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
            mr: mr.clone(),
            range: std::ops::Range { start, end },
            remote_allocs: remote_allocs.clone(),
            fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            id,
            alloc_table: AllocTable::Fabric(alloc_table),
            mcast_group: mcast_group,
            print: false,
        };
        //initialize ref count to 1
        // let encoded = encode_ref_count_and_padding(1, padding);
        unsafe {
            (&*(alloc.mem.as_ptr().add(alloc.fabric_ref_cnt_offset) as *mut AtomicUsize))
                .store(encoded, Ordering::SeqCst);
        }
        debug!(target: "libfabric", "Created Libfabric allocation: {:?}", alloc);

        Ok(alloc)
    }
    pub(crate) fn num_pes(&self) -> usize {
        self.remote_allocs.len()
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
            range: self.range.start + offset..self.range.start + offset + len,
            remote_allocs,
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: self.rt_ref_cnt_offset, //keep the same ref count offset as the parent allocation if this is actually a rt alloc, it will be updated when converted to a rt_alloc
            id,
            alloc_table: self.alloc_table.clone(),
            mcast_group: self.mcast_group.clone(),
            print: self.print,
        };
        debug!(target: "libfabric", "Created Libfabric sub-allocation: {:?}", alloc);
        Ok(alloc)
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
            range: self.range.start + offset..self.range.start + offset + data_bytes,
            remote_allocs: remote_allocs.clone(),
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            id,
            alloc_table: AllocTable::Runtime(alloc_table, self.range.start + offset, alloc_manager),
            mcast_group: self.mcast_group.clone(),
            print: self.print,
        };

        unsafe {
            (&*(alloc.mem.as_ptr().add(alloc.rt_ref_cnt_offset) as *mut AtomicUsize))
                .store(encoded, Ordering::SeqCst);
        }

        debug!(target: "libfabric", "Created Libfabric rt-allocation: {:?}", alloc);
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
            range: self.range.start..self.range.end - padding - std::mem::size_of::<AtomicUsize>(),
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            remote_allocs: self.remote_allocs.clone(),
            id: self.id,
            alloc_table: AllocTable::Runtime(alloc_table, self.range.start, alloc_manager),
            mcast_group: self.mcast_group.clone(),
            print: true,
        };
        get_ref_count(unsafe {
            &*(alloc.mem.as_ptr().add(alloc.fabric_ref_cnt_offset) as *const AtomicUsize)
        });
        debug!(target: "libfabric", "Converted Libfabric alloc to rt-alloc: {:?}", alloc);
        Ok(alloc)
    }

    pub(crate) fn leak(self) -> Option<CommAllocAddr> {
        match self.alloc_table {
            AllocTable::Fabric(_) => None, //only rt_allocs can be leaked
            AllocTable::Runtime(_, _, _) => {
                self.increment_fabric_ref_count(); //increment the ref count to account for the leaked instance
                let _cnt = self.increment_rt_ref_count(); //increment the ref count to account for the leaked instance
                debug!(target: "libfabric", "Leaking Libfabric rt-allocation: {:?}", self);
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
        trace!(target: "libfabric",
            "Checking if address {:x} is contained in allocation range {:x}-{:x}",
            addr,
            self.range.start,
            self.range.end
        );
        self.range.contains(addr)
    }

    pub(crate) fn remote_contains(&self, remote_id: &usize, addr: &usize) -> bool {
        trace!(target: "libfabric",
            "Checking if remote address {:x} on PE {} is contained in remote allocation for {:?}",
            addr,
            remote_id,
            self,
        );
        match self.remote_allocs.get(remote_id) {
            Some(remote_info) => {
                trace!(target: "libfabric",
                    "Remote PE {} allocation info: {:?} {:?}",
                    remote_id,
                    remote_info.mem_address(),
                    unsafe{remote_info.mem_address().add(remote_info.mem_len())}
                );
                remote_info.contains(&addr)
            }
            None => {
                trace!(target: "libfabric",
                    "Remote PE {} is not part of the sub allocation group",
                    remote_id
                );
                for (pe, remote_info) in self.remote_allocs.iter() {
                    trace!(target: "libfabric",
                        "  PE {}: {:?} {:?}",
                        pe,
                        remote_info.mem_address(),
                        remote_info.mem_len()
                    );
                }
                false
            }
        }
    }

    pub(crate) fn remote_info(&self, remote_pe: &usize) -> Option<RemoteMemAddressInfo> {
        self.remote_allocs.get(remote_pe).cloned()
    }

    pub(crate) fn mr(&self) -> MemoryRegion {
        self.mr.clone()
    }

    pub(crate) unsafe fn inner_put<T: Copy>(
        &self,
        pe: usize,
        offset: usize, //T-sized offset
        src_addr: &[T],
        blocking: bool,
    ) -> Result<(), libfabric::error::Error> {
        let offset = offset * std::mem::size_of::<T>(); //we allocate memoryregions from libfabric as u8;
        assert!(offset + src_addr.len() * std::mem::size_of::<T>() <= self.num_bytes()); //we use num_bytes instead of mem.len() to allow for sub-allocations,
        if pe == self.ofi.my_pe {
            std::ptr::copy(
                src_addr.as_ptr() as *const u8,
                (self.start() + offset) as *mut u8,
                src_addr.len() * std::mem::size_of::<T>(),
            );
            return Ok(());
        }
        #[cfg(feature = "enable-on-node-shmem")]
        if let Some(addr) = self.same_node_addr(pe, offset) {
            std::ptr::copy(
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
                cg.ep.inject_write_to(
                    src_addr,
                    &cg.mapped_addresses[pe],
                    remote_dst_addr,
                    &remote_key,
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
                    cg.ep.write_to(
                        &src_addr[curr_idx..curr_idx + msg_len],
                        Some(self.mr.descriptor()),
                        &cg.mapped_addresses[pe],
                        remote_dst_addr,
                        &remote_key,
                    )
                })
                .expect("Error posting put");

                remote_dst_addr = remote_dst_addr.add(msg_len * std::mem::size_of::<T>());
                curr_idx += msg_len;
            }
        }

        Ok(())
    }

    pub(crate) unsafe fn inner_get<T: Copy>(
        &self,
        pe: usize,
        offset: usize,
        dst_addr: &mut [T],
        blocking: bool,
    ) -> Result<(), libfabric::error::Error> {
        let offset = offset * std::mem::size_of::<T>(); //we allocate memoryregions from libfabric as u8;
        assert!(offset + dst_addr.len() * std::mem::size_of::<T>() <= self.num_bytes()); //we use num_bytes instead of mem.len() to allow for sub-allocations,
        if pe == self.ofi.my_pe {
            std::ptr::copy(
                (self.start() + offset) as *const u8,
                dst_addr.as_mut_ptr() as *mut u8,
                dst_addr.len() * std::mem::size_of::<T>(),
            );
            return Ok(());
        }
        #[cfg(feature = "enable-on-node-shmem")]
        if let Some(addr) = self.same_node_addr(pe, offset) {
            std::ptr::copy(
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

        let mut remote_src_addr = remote_alloc_info.mem_address().add(offset);
        let remote_key = remote_alloc_info.key();
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
        if dst_addr.len() < self.ofi.info_entry.ep_attr().max_msg_size() / std::mem::size_of::<T>()
        {
            // trace!(
            //     target: "libfabric",
            //     "GET: from PE {} at addr {:?} to local addr {:?} len {} {}",
            //     pe,
            //     remote_src_addr,
            //     dst_addr.as_mut_ptr(),
            //     std::mem::size_of_val(dst_addr),
            //     dst_addr.len(),
            // );
            cg.post_get(blocking, || unsafe {
                cg.ep.read_from(
                    dst_addr,
                    Some(self.mr.descriptor()),
                    &cg.mapped_addresses[pe],
                    remote_src_addr,
                    &remote_key,
                )
            })?;
        } else {
            let mut curr_idx = 0;

            while curr_idx < dst_addr.len() {
                let msg_len = std::cmp::min(
                    dst_addr.len() - curr_idx,
                    self.ofi.info_entry.ep_attr().max_msg_size() / std::mem::size_of::<T>(),
                );
                cg.post_get(blocking, || unsafe {
                    trace!(
                        target: "libfabric",
                        "GET: from PE {} at addr {:?} to local addr {:?} len {}",
                        pe,
                        remote_src_addr,
                        &mut dst_addr[curr_idx..curr_idx + msg_len] as *mut [T],
                        msg_len * std::mem::size_of::<T>()
                    );
                    cg.ep.read_from(
                        &mut dst_addr[curr_idx..curr_idx + msg_len],
                        Some(self.mr.descriptor()),
                        &cg.mapped_addresses[pe],
                        remote_src_addr,
                        &remote_key,
                    )
                })
                .expect("Error posting get");
                remote_src_addr = remote_src_addr.add(msg_len * std::mem::size_of::<T>());
                curr_idx += msg_len;
            }
        }

        Ok(())
    }

    #[inline(never)]
    pub(crate) unsafe fn inner_get_small<T: Copy>(
        &self,
        pe: usize,
        offset: usize,
        dst_addr: &mut [T],
        blocking: bool,
    ) -> Result<(), libfabric::error::Error> {
        let offset = offset * std::mem::size_of::<T>(); //we allocate memoryregions from libfabric as u8;
        assert!(offset + dst_addr.len() * std::mem::size_of::<T>() <= self.num_bytes()); //we use num_bytes instead of mem.len() to allow for sub-allocations,
        if pe == self.ofi.my_pe {
            std::ptr::copy(
                (self.start() + offset) as *const u8,
                dst_addr.as_mut_ptr() as *mut u8,
                dst_addr.len() * std::mem::size_of::<T>(),
            );
            return Ok(());
        }
        #[cfg(feature = "enable-on-node-shmem")]
        if let Some(addr) = self.same_node_addr(pe, offset) {
            std::ptr::copy(
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

        let remote_src_addr = remote_alloc_info.mem_address().add(offset);
        let remote_key = remote_alloc_info.key();
        let cg = &self.ofi.comm_group;
        cg.post_get(blocking, || unsafe {
            cg.ep.read_from(
                dst_addr,
                Some(self.mr.descriptor()),
                &cg.mapped_addresses[pe],
                remote_src_addr,
                &remote_key,
            )
        })?;

        Ok(())
    }

    pub(crate) fn atomic_op_inner<T: 'static>(
        &self,
        pe: usize,
        offset: usize,
        op: &mut LamellarAtomicOp<T>,
        blocking: bool,
    ) -> Result<(), libfabric::error::Error> {
        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_atomic_op::<T, u8>(pe, offset, op, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_atomic_op::<T, u16>(pe, offset, op, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_atomic_op::<T, u32>(pe, offset, op, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_atomic_op::<T, u64>(pe, offset, op, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_atomic_op::<T, usize>(pe, offset, op, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_atomic_op::<T, i8>(pe, offset, op, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_atomic_op::<T, i16>(pe, offset, op, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_atomic_op::<T, i32>(pe, offset, op, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_atomic_op::<T, i64>(pe, offset, op, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_atomic_op::<T, isize>(pe, offset, op, blocking)
            } else {
                panic!("Unsupported atomic operation type");
            }
        }
    }

    unsafe fn typed_atomic_op<T, OFI: AsFiType>(
        &self,
        pe: usize,
        offset: usize,
        op: &mut LamellarAtomicOp<T>,
        blocking: bool,
    ) -> Result<(), libfabric::error::Error> {
        let offset = offset * std::mem::size_of::<T>(); //we allocate memoryregions from libfabric as u8;
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes()); //we use num_bytes instead of mem.len() to allow for sub-allocations, offset + 1 because atomics operate on a single element and we verifying we arent missaligned
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));
        let remote_dst_addr = unsafe { remote_alloc_info.mem_address().add(offset) };
        let remote_key = remote_alloc_info.key();

        match op {
            LamellarAtomicOp::Sub(src) => {
                Self::negate_atomic_value(src.as_mut().get_unchecked_mut())
            }
            LamellarAtomicOp::FetchMin(_)
            | LamellarAtomicOp::FetchMax(_)
            | LamellarAtomicOp::FetchSum(_)
            | LamellarAtomicOp::FetchSub(_)
            | LamellarAtomicOp::FetchProd(_)
            | LamellarAtomicOp::FetchBitOr(_)
            | LamellarAtomicOp::FetchBitXor(_)
            | LamellarAtomicOp::FetchBitAnd(_) => {
                panic!("Fetch atomic ops must use the fetch path")
            }
            LamellarAtomicOp::Cas(_, _) => {
                panic!("Compare atomic ops must use the compare path")
            }
            _ => {}
        };
        let src = op.src() as *const OFI;
        let buf = std::slice::from_raw_parts(src, 1);
        let cg = &self.ofi.comm_group;
        cg.post_put(blocking, || {
            cg.ep.atomic_inject_to(
                buf,
                &cg.mapped_addresses[pe],
                remote_dst_addr,
                &remote_key,
                op.into(),
            )
        })?;
        Ok(())
    }

    pub(crate) fn atomic_fetch_op_inner<T: 'static>(
        &self,
        pe: usize,
        offset: usize,
        op: &mut LamellarAtomicOp<T>,
        result: &mut [T],
        blocking: bool,
    ) -> Result<(), libfabric::error::Error> {
        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_atomic_fetch_op::<T, u8>(pe, offset, op, result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_atomic_fetch_op::<T, u16>(pe, offset, op, result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_atomic_fetch_op::<T, u32>(pe, offset, op, result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_atomic_fetch_op::<T, u64>(pe, offset, op, result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_atomic_fetch_op::<T, usize>(pe, offset, op, result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_atomic_fetch_op::<T, i8>(pe, offset, op, result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_atomic_fetch_op::<T, i16>(pe, offset, op, result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_atomic_fetch_op::<T, i32>(pe, offset, op, result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_atomic_fetch_op::<T, i64>(pe, offset, op, result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_atomic_fetch_op::<T, isize>(pe, offset, op, result, blocking)
            } else {
                panic!("Unsupported atomic operation type");
            }
        }
    }

    unsafe fn typed_atomic_fetch_op<T, OFI: AsFiType>(
        &self,
        pe: usize,
        offset: usize,
        op: &mut LamellarAtomicOp<T>,
        result: &mut [T],
        blocking: bool,
    ) -> Result<(), libfabric::error::Error> {
        let offset = offset * std::mem::size_of::<T>(); //we allocate memoryregions from libfabric as u8;
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes()); //we use num_bytes instead of mem.len() to allow for sub-allocations, offset + 1 because atomics operate on a single element and we verifying we arent missaligned
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));
        let remote_dst_addr = unsafe { remote_alloc_info.mem_address().add(offset) };
        let remote_key = remote_alloc_info.key();

        // let res = std::mem::transmute::<&mut [T], &mut [OFI]>(result);
        let res = &mut *(result as *mut [T] as *mut [OFI]);
        let cg = &self.ofi.comm_group;

        match op {
            LamellarAtomicOp::FetchSub(src) => {
                Self::negate_atomic_value(src.as_mut().get_unchecked_mut())
            }
            LamellarAtomicOp::Min(_)
            | LamellarAtomicOp::Max(_)
            | LamellarAtomicOp::Sum(_)
            | LamellarAtomicOp::Sub(_)
            | LamellarAtomicOp::Prod(_)
            | LamellarAtomicOp::BitOr(_)
            | LamellarAtomicOp::BitXor(_)
            | LamellarAtomicOp::BitAnd(_) => {
                panic!("Non-fetch atomic ops must use the non-fetch path")
            }
            LamellarAtomicOp::Cas(_, _) => {
                panic!("Compare atomic ops must use the compare path")
            }
            _ => {}
        };
        let src = op.src() as *const OFI;
        let buf = std::slice::from_raw_parts(src, 1);
        cg.post_get(blocking, || {
            cg.ep.fetch_atomic_from(
                buf,
                None,
                res,
                None,
                &cg.mapped_addresses[pe],
                remote_dst_addr,
                &remote_key,
                op.into(),
            )
        })?;

        Ok(())
    }

    pub(crate) fn atomic_compare_exchange_op_inner<T: 'static + Copy>(
        &self,
        pe: usize,
        offset: usize,
        current: *const T,
        new: *const T,
        result: &mut [T],
        blocking: bool,
    ) -> Result<(), libfabric::error::Error> {
        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_atomic_compare_exchange_op::<T, u8>(
                    pe, offset, current, new, result, blocking,
                )
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_atomic_compare_exchange_op::<T, u16>(
                    pe, offset, current, new, result, blocking,
                )
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_atomic_compare_exchange_op::<T, u32>(
                    pe, offset, current, new, result, blocking,
                )
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_atomic_compare_exchange_op::<T, u64>(
                    pe, offset, current, new, result, blocking,
                )
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_atomic_compare_exchange_op::<T, usize>(
                    pe, offset, current, new, result, blocking,
                )
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_atomic_compare_exchange_op::<T, i8>(
                    pe, offset, current, new, result, blocking,
                )
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_atomic_compare_exchange_op::<T, i16>(
                    pe, offset, current, new, result, blocking,
                )
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_atomic_compare_exchange_op::<T, i32>(
                    pe, offset, current, new, result, blocking,
                )
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_atomic_compare_exchange_op::<T, i64>(
                    pe, offset, current, new, result, blocking,
                )
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_atomic_compare_exchange_op::<T, isize>(
                    pe, offset, current, new, result, blocking,
                )
            } else {
                panic!("Unsupported atomic operation type");
            }
        }
    }

    unsafe fn typed_atomic_compare_exchange_op<T, OFI: AsFiType>(
        &self,
        pe: usize,
        offset: usize,
        current: *const T,
        new: *const T,
        result: &mut [T],
        blocking: bool,
    ) -> Result<(), libfabric::error::Error> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));
        let remote_dst_addr = remote_alloc_info.mem_address().add(offset);
        let remote_key = remote_alloc_info.key();

        let new_slice = std::slice::from_raw_parts(new as *const OFI, 1);
        let current_slice = std::slice::from_raw_parts(current as *const OFI, 1);
        let res = &mut *(result as *mut [T] as *mut [OFI]);
        let cg = &self.ofi.comm_group;
        cg.post_get(blocking, || {
            cg.ep.compare_atomic_swap_to(
                new_slice,
                None,
                current_slice,
                None,
                res,
                None,
                &cg.mapped_addresses[pe],
                remote_dst_addr,
                &remote_key,
            )
        })?;

        Ok(())
    }

    pub(crate) fn allreduce_inplace_inner<T: 'static>(        
        &self,
        op: &AllReduceOp,
        blocking: bool,
    ) -> Result<CachedContext, libfabric::error::Error> {
        let dst = unsafe {std::slice::from_raw_parts_mut(self.start() as *mut T, self.num_bytes()/std::mem::size_of::<T>())};
        self.allreduce_inner(op, dst, blocking)
    }

    pub(crate) fn allreduce_inner<T: 'static>(
        &self,
        op: &AllReduceOp,
        result: &mut [T],
        blocking: bool,
    ) -> Result<CachedContext, libfabric::error::Error> {
        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_allreduce::<T, u8>(op, result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_allreduce::<T, u16>(op, result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_allreduce::<T, u32>(op, result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_allreduce::<T, u64>(op, result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_allreduce::<T, usize>(op, result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_allreduce::<T, i8>(op, result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_allreduce::<T, i16>(op, result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_allreduce::<T, i32>(op, result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_allreduce::<T, i64>(op, result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_allreduce::<T, isize>(op, result, blocking)
            } else {
                panic!("Unsupported allreduce operation type");
            }
        }
    }

    unsafe fn typed_allreduce<T, OFI: AsFiType>(
        &self,
        op: &AllReduceOp,
        result: &mut [T],
        blocking: bool,
    ) -> Result<CachedContext, libfabric::error::Error> {
        let res = unsafe {&mut *(result as *mut [T] as *mut [OFI])};
        let cg = &self.ofi.comm_group;
        let mc = self.mcast_group.as_ref().expect("No multicast group for allreduce");
        let src = unsafe {std::slice::from_raw_parts(self.start() as *const T, self.num_bytes()/std::mem::size_of::<T>())};
        let buf = unsafe { std::mem::transmute::<&[T], &[OFI]>(src) };
        let ctx = cg.post_collective(blocking, |ctx| {
                cg.ep.allreduce_with_context(
                    buf,
                    None,
                    res,
                    None,
                    mc,
                    op.into(),
                    CollectiveOptions::default(),
                    ctx,
                )
            }
        )?;

        Ok(ctx)
    }
    pub(crate) fn reduce_inplace_inner<T: 'static>(        
        &self,
        op: &LamellarReduceOp,
        root_pe: Option<usize>,
        blocking: bool,
    ) -> Result<CachedContext, libfabric::error::Error> {
        let dst = unsafe {std::slice::from_raw_parts_mut(self.start() as *mut T, self.num_bytes()/std::mem::size_of::<T>())};
        let slice_or_pe = if let Some(root) = root_pe {
            RootOrSliceMut::NotRoot(root)
        }
        else {
            RootOrSliceMut::Root(dst)
        };

        
        self.reduce_inner(op, slice_or_pe, blocking)
    }

    pub(crate) fn allgather_inner<T: 'static>(
        &self,
        result: &mut [T],
        blocking: bool,
    ) -> Result<CachedContext, libfabric::error::Error> {
        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_allgather::<T, u8>(result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_allgather::<T, u16>(result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_allgather::<T, u32>(result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_allgather::<T, u64>(result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_allgather::<T, usize>(result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_allgather::<T, i8>(result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_allgather::<T, i16>(result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_allgather::<T, i32>(result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_allgather::<T, i64>(result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_allgather::<T, isize>(result, blocking)
            } else {
                panic!("Unsupported allgather operation type");
            }
        }
    }

    unsafe fn typed_allgather<T, OFI: AsFiType>(
        &self,
        result: &mut [T],
        blocking: bool,
    ) -> Result<CachedContext, libfabric::error::Error> {
        let res = unsafe {&mut *(result as *mut [T] as *mut [OFI])};
        let cg = &self.ofi.comm_group;
        let mc = self.mcast_group.as_ref().expect("No multicast group for allgather");
        let src = unsafe {std::slice::from_raw_parts(self.start() as *const T, self.num_bytes()/std::mem::size_of::<T>())};
        let buf = unsafe { std::mem::transmute::<&[T], &[OFI]>(src) };
        let ctx = cg.post_collective(blocking, |ctx| {
                cg.ep.allgather_with_context(
                    buf,
                    None,
                    res,
                    None,
                    mc,
                    CollectiveOptions::default(),
                    ctx,
                )
            }
        )?;

        Ok(ctx)
    }

    pub(crate) fn alltoall_inner<T: 'static>(
        &self,
        result: &mut [T],
        blocking: bool,
    ) -> Result<CachedContext, libfabric::error::Error> {
        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_alltoall::<T, u8>(result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_alltoall::<T, u16>(result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_alltoall::<T, u32>(result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_alltoall::<T, u64>(result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_alltoall::<T, usize>(result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_alltoall::<T, i8>(result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_alltoall::<T, i16>(result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_alltoall::<T, i32>(result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_alltoall::<T, i64>(result, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_alltoall::<T, isize>(result, blocking)
            } else {
                panic!("Unsupported alltoall operation type");
            }
        }
    }

    unsafe fn typed_alltoall<T, OFI: AsFiType>(
        &self,
        result: &mut [T],
        blocking: bool,
    ) -> Result<CachedContext, libfabric::error::Error> {
        let res = unsafe {&mut *(result as *mut [T] as *mut [OFI])};
        let cg = &self.ofi.comm_group;
        let mc = self.mcast_group.as_ref().expect("No multicast group for alltoall");
        let src = unsafe {std::slice::from_raw_parts(self.start() as *const T, self.num_bytes()/std::mem::size_of::<T>())};
        let buf = unsafe { std::mem::transmute::<&[T], &[OFI]>(src) };
        let ctx = cg.post_collective(blocking, |ctx| {
                cg.ep.alltoall_with_context(
                    buf,
                    None,
                    res,
                    None,
                    mc,
                    CollectiveOptions::default(),
                    ctx,
                )
            }
        )?;

        Ok(ctx)
    }

    pub(crate) fn reduce_inner<T: 'static>(
        &self,
        op: &LamellarReduceOp,
        slice_or_pe: RootOrSliceMut<T>,
        blocking: bool,
    ) -> Result<CachedContext, libfabric::error::Error> {

        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_reduce::<T, u8>(op, slice_or_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_reduce::<T, u16>(op, slice_or_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_reduce::<T, u32>(op, slice_or_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_reduce::<T, u64>(op, slice_or_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_reduce::<T, usize>(op, slice_or_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_reduce::<T, i8>(op, slice_or_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_reduce::<T, i16>(op, slice_or_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_reduce::<T, i32>(op, slice_or_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_reduce::<T, i64>(op, slice_or_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_reduce::<T, isize>(op, slice_or_pe, blocking)
            } else {
                panic!("Unsupported allreduce operation type");
            }
        }
    }

    unsafe fn typed_reduce<T, OFI: AsFiType>(
        &self,
        op: &LamellarReduceOp,
        slice_or_pe: RootOrSliceMut<'_, T>,
        blocking: bool,
    ) -> Result<CachedContext, libfabric::error::Error> {
        let cg = &self.ofi.comm_group;
        let mc = self.mcast_group.as_ref().expect("No multicast group for collective reduce");
        let (result, root_pe) = match slice_or_pe {
            RootOrSliceMut::Root(result) => (Some(result), self.ofi.my_pe) ,
            RootOrSliceMut::NotRoot(root_pe) => (None, root_pe),
        };
        let src = unsafe {std::slice::from_raw_parts(self.start() as *const T, self.num_bytes()/std::mem::size_of::<T>())};
        let res = match result {
            Some(res) => unsafe {std::mem::transmute::<&mut [T], &mut [OFI]>(res)},
            None => {
                let res_buf = unsafe {std::slice::from_raw_parts_mut(self.start() as *mut T, self.num_bytes()/std::mem::size_of::<T>())}; // if result is None, we are doing an in-place reduce or we reduce on a non-root PE, so we can reuse the source buffer as the destination buffer since it is either the destination or will be ignored by non-root PEs
                unsafe { std::mem::transmute::<&mut [T], &mut [OFI]>(res_buf) }
            }
        };
        let buf = unsafe { std::mem::transmute::<&[T], &[OFI]>(src) };
        let ctx = cg.post_collective(blocking, |ctx| {
                cg.ep.reduce_with_context(
                    buf,
                    None,
                    res,
                    None,
                    mc,
                    &cg.mapped_addresses[root_pe],
                    op.into(),
                    CollectiveOptions::default(),
                    ctx,
                )
            }
        )?;

        Ok(ctx)
    }

    pub(crate) fn gather_inner<T: 'static>(
        &self,
        slice_or_pe: RootOrSliceMut<T>,
        blocking: bool,
    ) -> Result<CachedContext, libfabric::error::Error> {

        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_gather::<T, u8>(slice_or_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_gather::<T, u16>(slice_or_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_gather::<T, u32>(slice_or_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_gather::<T, u64>(slice_or_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_gather::<T, usize>(slice_or_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_gather::<T, i8>(slice_or_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_gather::<T, i16>(slice_or_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_gather::<T, i32>(slice_or_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_gather::<T, i64>(slice_or_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_gather::<T, isize>(slice_or_pe, blocking)
            } else {
                panic!("Unsupported allreduce operation type");
            }
        }
    }

    unsafe fn typed_gather<T, OFI: AsFiType>(
        &self,
        slice_or_pe: RootOrSliceMut<'_, T>,
        blocking: bool,
    ) -> Result<CachedContext, libfabric::error::Error> {
        let cg = &self.ofi.comm_group;
        let mc = self.mcast_group.as_ref().expect("No multicast group for collective reduce");
        let (result, root_pe) = match slice_or_pe {
            RootOrSliceMut::Root(result) => (Some(result), self.ofi.my_pe) ,
            RootOrSliceMut::NotRoot(root_pe) => (None, root_pe),
        };
        let src = unsafe {std::slice::from_raw_parts(self.start() as *const T, self.num_bytes()/std::mem::size_of::<T>())};
        let res = match result {
            Some(res) => unsafe {std::mem::transmute::<&mut [T], &mut [OFI]>(res)},
            None => {
                let res_buf = unsafe {std::slice::from_raw_parts_mut(self.start() as *mut T, self.num_bytes()/std::mem::size_of::<T>())}; // if result is None, we are a non-root PE, so we can reuse the source buffer as the destination buffer since it will be ignored.
                unsafe { std::mem::transmute::<&mut [T], &mut [OFI]>(res_buf) }
            }
        };
        let buf = unsafe { std::mem::transmute::<&[T], &[OFI]>(src) };
        let ctx = cg.post_collective(blocking, |ctx| {
                cg.ep.gather_with_context(
                    buf,
                    None,
                    res,
                    None,
                    mc,
                    &cg.mapped_addresses[root_pe],
                    CollectiveOptions::default(),
                    ctx,
                )
            }
        )?;

        Ok(ctx)
    }

    pub(crate) fn broadcast_inner<T: 'static>(
        &self,
        root_src: RootSrcOrSliceMut<T>,
        blocking: bool,
    ) -> Result<CachedContext, libfabric::error::Error> {

        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_broadcast::<T, u8>(root_src, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_broadcast::<T, u16>(root_src, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_broadcast::<T, u32>(root_src, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_broadcast::<T, u64>(root_src, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_broadcast::<T, usize>(root_src, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_broadcast::<T, i8>(root_src, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_broadcast::<T, i16>(root_src, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_broadcast::<T, i32>(root_src, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_broadcast::<T, i64>(root_src, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_broadcast::<T, isize>(root_src, blocking)
            } else {
                panic!("Unsupported allreduce operation type");
            }
        }
    }

    unsafe fn typed_broadcast<T, OFI: AsFiType>(
        &self,
        slice_or_pe: RootSrcOrSliceMut<'_, T>,
        blocking: bool,
    ) -> Result<CachedContext, libfabric::error::Error> {
        let cg = &self.ofi.comm_group;
        let mc = self.mcast_group.as_ref().expect("No multicast group for collective reduce");
        let (result, root_pe) = match slice_or_pe {
            RootSrcOrSliceMut::Root() => (None, self.ofi.my_pe) ,
            RootSrcOrSliceMut::NotRoot(result, root_pe) => (Some(result), root_pe),
        };
        let res = match result {
            Some(res) => unsafe {std::mem::transmute::<&mut [T], &mut [OFI]>(res)},
            None => {
                let res_buf = unsafe {std::slice::from_raw_parts_mut(self.start() as *mut T, self.num_bytes()/std::mem::size_of::<T>())}; // if result is None, we are a non-root PE, so we can reuse the source buffer as the destination buffer since it will be ignored.
                unsafe { std::mem::transmute::<&mut [T], &mut [OFI]>(res_buf) }
            }
        };
        let ctx = cg.post_collective(blocking, |ctx| {
                cg.ep.broadcast_with_context(
                    res,
                    None,
                    mc,
                    &cg.mapped_addresses[root_pe],
                    CollectiveOptions::default(),
                    ctx,
                )
            }
        )?;

        Ok(ctx)
    }

    pub(crate) fn scatter_inner<T: 'static>(
        &self,
        res: &mut [T],
        root_pe: usize,
        blocking: bool,
    ) -> Result<CachedContext, libfabric::error::Error> {

        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_scatter::<T, u8>(res, root_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_scatter::<T, u16>(res, root_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_scatter::<T, u32>(res, root_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_scatter::<T, u64>(res, root_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_scatter::<T, usize>(res, root_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_scatter::<T, i8>(res, root_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_scatter::<T, i16>(res, root_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_scatter::<T, i32>(res, root_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_scatter::<T, i64>(res, root_pe, blocking)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_scatter::<T, isize>(res, root_pe, blocking)
            } else {
                panic!("Unsupported allreduce operation type");
            }
        }
    }

    unsafe fn typed_scatter<T, OFI: AsFiType>(
        &self,
        res: &mut [T],
        root_pe: usize,
        blocking: bool,
    ) -> Result<CachedContext, libfabric::error::Error> {
        let cg = &self.ofi.comm_group;
        let mc = self.mcast_group.as_ref().expect("No multicast group for collective reduce");

        let res = unsafe {std::mem::transmute::<&mut [T], &mut [OFI]>(res)};
        
        let src = unsafe {std::slice::from_raw_parts(self.start() as *const T, self.num_bytes()/std::mem::size_of::<T>()/ self.ofi.num_pes)};
        let buf = unsafe { std::mem::transmute::<&[T], &[OFI]>(src) };

        let ctx = cg.post_collective(blocking, |ctx| {
                cg.ep.scatter_with_context(
                    buf,
                    None,
                    res,
                    None,
                    mc,
                    &cg.mapped_addresses[root_pe],
                    CollectiveOptions::default(),
                    ctx,
                )
            }
        )?;

        Ok(ctx)
    }

    pub(crate) fn wait(&self) -> Result<(), libfabric::error::Error> {
        self.ofi.comm_group.wait_all()
    }
}

impl Drop for LibfabricAlloc {
    fn drop(&mut self) {
        let fabric_ref_count = self.decrement_fabric_ref_count();
        // if self.print {
        //     println!(
        //         "[{:?}] Dropping LibfabricAlloc: {:x} - {:x} ref_cnt(before drop) {}",
        //         std::thread::current().id(),
        //         self.range.start,
        //         self.range.end,
        //         fabric_ref_count
        //     );
        // }
        debug!(target: "libfabric", "Dropping LibfabricAlloc: {:x} - {:x} ref_cnt(before drop) {}", self.range.start,self.range.end, fabric_ref_count);

        match &self.alloc_table {
            AllocTable::Fabric(alloc_table) => {
                if fabric_ref_count == 2 {
                    debug!(target: "libfabric", "Dropping fabric LibfabricAlloc: {:?}", self);
                    alloc_table.remove_from_alloc(self);
                }
            }
            AllocTable::Runtime(rt_alloc_table, addr, fabric_alloc_table) => {
                let rt_ref_count = self.decrement_rt_ref_count();
                // if self.print {
                //     println!(
                //         "[{:?}, {:?}] Freeing runtime LibfabricAlloc: {:?}",
                //         std::time::Instant::now(),
                //         std::thread::current().id(),
                //         self
                //     );
                // }
                if rt_ref_count == 1 {
                    debug!(target: "libfabric", "Freeing runtime LibfabricAlloc: {:?}",  self);

                    rt_alloc_table.free(*addr).expect(&format!(
                        "[{:?}] Error removing from runtime alloc table {:x}",
                        std::thread::current().id(),
                        addr
                    ));
                }
                if fabric_ref_count == 2 {
                    debug!(target: "libfabric", "Dropping fabric LibfabricAlloc from rt LibfabricAlloc: {:?}", self);
                    fabric_alloc_table.remove_from_alloc(self);
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OneSidedLibfabricAlloc {
    pub(crate) remote_pe: usize,
    pub(crate) alloc: LibfabricAlloc,
}

impl OneSidedLibfabricAlloc {
    pub(crate) fn num_bytes(&self) -> usize {
        self.alloc.num_bytes()
    }
    pub(crate) fn start(&self) -> usize {
        self.alloc.start()
    }
    pub(crate) fn sub_alloc(&self, offset: usize, len: usize) -> AllocResult<Self> {
        let sub_alloc = self.alloc.sub_alloc(offset, len)?;
        Ok(OneSidedLibfabricAlloc {
            remote_pe: self.remote_pe,
            alloc: sub_alloc,
        })
    }
}

impl From<OneSidedLibfabricAlloc> for CommAlloc {
    fn from(alloc: OneSidedLibfabricAlloc) -> Self {
        CommAlloc {
            inner_alloc: CommAllocInner::OneSidedLibfabricAlloc(alloc),
            alloc_type: CommAllocType::Remote,
        }
    }
}

impl<T> From<&LamellarAtomicOp<T>> for AtomicOp {
    fn from(op: &LamellarAtomicOp<T>) -> Self {
        match op {
            LamellarAtomicOp::Min(_) => AtomicOp::Min,
            LamellarAtomicOp::Max(_) => AtomicOp::Max,
            LamellarAtomicOp::Sum(_) => AtomicOp::Sum,
            LamellarAtomicOp::Sub(_) => AtomicOp::Sum, // Sub can be implemented as Add with negative value
            LamellarAtomicOp::Prod(_) => AtomicOp::Prod,
            LamellarAtomicOp::BitOr(_) => AtomicOp::Bor,
            LamellarAtomicOp::BitXor(_) => AtomicOp::Bxor,
            LamellarAtomicOp::BitAnd(_) => AtomicOp::Band,
            LamellarAtomicOp::Write(_) => AtomicOp::AtomicWrite,
            _ => panic!("unexpected atomic op"),
        }
    }
}

impl<T> From<&mut LamellarAtomicOp<T>> for AtomicOp {
    fn from(op: &mut LamellarAtomicOp<T>) -> Self {
        match op {
            LamellarAtomicOp::Min(_) => AtomicOp::Min,
            LamellarAtomicOp::Max(_) => AtomicOp::Max,
            LamellarAtomicOp::Sum(_) => AtomicOp::Sum,
            LamellarAtomicOp::Sub(_) => AtomicOp::Sum, // Sub can be implemented as Add with negative value
            LamellarAtomicOp::Prod(_) => AtomicOp::Prod,
            // LamellarAtomicOp::LogicalOr(_) => AtomicOp::Lor,
            // LamellarAtomicOp::LogicalXor(_) => AtomicOp::Lxor,
            // LamellarAtomicOp::LogicalAnd(_) => AtomicOp::Land,
            LamellarAtomicOp::BitOr(_) => AtomicOp::Bor,
            LamellarAtomicOp::BitXor(_) => AtomicOp::Bxor,
            LamellarAtomicOp::BitAnd(_) => AtomicOp::Band,
            LamellarAtomicOp::Write(_) => AtomicOp::AtomicWrite,
            _ => panic!("unexpected atomic op"),
        }
    }
}

impl<T> From<&LamellarAtomicOp<T>> for FetchAtomicOp {
    fn from(op: &LamellarAtomicOp<T>) -> Self {
        match op {
            LamellarAtomicOp::FetchMin(_) => FetchAtomicOp::Min,
            LamellarAtomicOp::FetchMax(_) => FetchAtomicOp::Max,
            LamellarAtomicOp::FetchSum(_) | LamellarAtomicOp::FetchSub(_) => FetchAtomicOp::Sum,
            LamellarAtomicOp::FetchProd(_) => FetchAtomicOp::Prod,
            LamellarAtomicOp::FetchBitOr(_) => FetchAtomicOp::Bor,
            LamellarAtomicOp::FetchBitXor(_) => FetchAtomicOp::Bxor,
            LamellarAtomicOp::FetchBitAnd(_) => FetchAtomicOp::Band,
            LamellarAtomicOp::Write(_) => FetchAtomicOp::AtomicWrite,
            LamellarAtomicOp::Read(_) => FetchAtomicOp::AtomicRead,
            LamellarAtomicOp::Cas(_, _) => panic!("Cas conversion to FetchAtomicOp not supported"),
            _ => panic!("Non-fetch atomic ops must use non-fetch path"),
        }
    }
}

impl<T> From<&mut LamellarAtomicOp<T>> for FetchAtomicOp {
    fn from(op: &mut LamellarAtomicOp<T>) -> Self {
        match op {
            LamellarAtomicOp::FetchMin(_) => FetchAtomicOp::Min,
            LamellarAtomicOp::FetchMax(_) => FetchAtomicOp::Max,
            LamellarAtomicOp::FetchSum(_) => FetchAtomicOp::Sum,
            LamellarAtomicOp::FetchSub(_) => FetchAtomicOp::Sum,
            LamellarAtomicOp::FetchProd(_) => FetchAtomicOp::Prod,
            LamellarAtomicOp::FetchBitOr(_) => FetchAtomicOp::Bor,
            LamellarAtomicOp::FetchBitXor(_) => FetchAtomicOp::Bxor,
            LamellarAtomicOp::FetchBitAnd(_) => FetchAtomicOp::Band,
            LamellarAtomicOp::Write(_) => FetchAtomicOp::AtomicWrite,
            LamellarAtomicOp::Read(_) => FetchAtomicOp::AtomicRead,
            _ => panic!("Non-fetch atomic ops must use non-fetch path"),
        }
    }
}

impl<T> From<LamellarAtomicOp<T>> for AtomicOp {
    fn from(op: LamellarAtomicOp<T>) -> Self {
        match op {
            LamellarAtomicOp::Min(_) => AtomicOp::Min,
            LamellarAtomicOp::Max(_) => AtomicOp::Max,
            LamellarAtomicOp::Sum(_) => AtomicOp::Sum,
            LamellarAtomicOp::Prod(_) => AtomicOp::Prod,
            LamellarAtomicOp::BitOr(_) => AtomicOp::Bor,
            LamellarAtomicOp::BitXor(_) => AtomicOp::Bxor,
            LamellarAtomicOp::BitAnd(_) => AtomicOp::Band,
            LamellarAtomicOp::Write(_) => AtomicOp::AtomicWrite,
            _ => panic!("unexpected atomic op"),
        }
    }
}

impl<T> From<LamellarAtomicOp<T>> for FetchAtomicOp {
    fn from(op: LamellarAtomicOp<T>) -> Self {
        match op {
            LamellarAtomicOp::FetchMin(_) => FetchAtomicOp::Min,
            LamellarAtomicOp::FetchMax(_) => FetchAtomicOp::Max,
            LamellarAtomicOp::FetchSum(_) | LamellarAtomicOp::FetchSub(_) => FetchAtomicOp::Sum,
            LamellarAtomicOp::FetchProd(_) => FetchAtomicOp::Prod,
            LamellarAtomicOp::FetchBitOr(_) => FetchAtomicOp::Bor,
            LamellarAtomicOp::FetchBitXor(_) => FetchAtomicOp::Bxor,
            LamellarAtomicOp::FetchBitAnd(_) => FetchAtomicOp::Band,
            LamellarAtomicOp::Write(_) => FetchAtomicOp::AtomicWrite,
            LamellarAtomicOp::Read => FetchAtomicOp::AtomicRead,
            _ => panic!("unexpected atomic op"),
        }
    }
}

impl From<LamellarReduceOp> for ReduceOp {
    fn from(op: LamellarReduceOp) -> Self {
        match op {
            LamellarReduceOp::Min => ReduceOp::Min,
            LamellarReduceOp::Max => ReduceOp::Max,
            LamellarReduceOp::Sum => ReduceOp::Sum,
            LamellarReduceOp::Prod => ReduceOp::Prod,
            // CollectiveReduceOp::LogicalOr => ReduceOp::Lor,
            // CollectiveReduceOp::LogicalXor => ReduceOp::Lxor,
            // CollectiveReduceOp::LogicalAnd => ReduceOp::Land,
            LamellarReduceOp::BitOr => ReduceOp::Bor,
            LamellarReduceOp::BitXor => ReduceOp::Bxor,
            LamellarReduceOp::BitAnd => ReduceOp::Band,
        }
    }
}

impl From<&LamellarReduceOp> for ReduceOp {
    fn from(op: &LamellarReduceOp) -> Self {
        match op {
            LamellarReduceOp::Min => ReduceOp::Min,
            LamellarReduceOp::Max => ReduceOp::Max,
            LamellarReduceOp::Sum => ReduceOp::Sum,
            LamellarReduceOp::Prod => ReduceOp::Prod,
            // CollectiveReduceOp::LogicalOr => ReduceOp::Lor,
            // CollectiveReduceOp::LogicalXor => ReduceOp::Lxor,
            // CollectiveReduceOp::LogicalAnd => ReduceOp::Land,
            LamellarReduceOp::BitOr => ReduceOp::Bor,
            LamellarReduceOp::BitXor => ReduceOp::Bxor,
            LamellarReduceOp::BitAnd => ReduceOp::Band,
        }
    }
}
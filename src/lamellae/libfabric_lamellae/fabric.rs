use libfabric::{
    av::{AddressVector, AddressVectorBuilder, AddressVectorSetBuilder, AvInAddress},
    cntr::{Counter, CounterBuilder, ReadCntr, WaitCntr},
    comm::{
        atomic::{AtomicCASEp, AtomicFetchEp, AtomicWriteEp},
        collective::{CollectiveAttr, CollectiveEp, MulticastGroupCollective},
        rma::{ReadEp, WriteEp},
    },
    connless_ep::ConnectionlessEndpoint,
    cq::{Completion, CompletionQueue, CompletionQueueBuilder, ReadCq},
    domain::{Domain, DomainBuilder},
    enums::{
        AVOptions, AddressFormat, AtomicOp, CollectiveOp, CollectiveOptions, CqFormat,
        EndpointType, FetchAtomicOp, HmemIface, JoinOptions, Mode, MrMode, Progress, ResourceMgmt,
        Threading, TrafficClass, TransferOptions,
    },
    ep::{Address, BaseEndpoint, Endpoint, EndpointBuilder},
    eq::{Event, EventQueue, EventQueueBuilder},
    fabric::{Fabric, FabricBuilder},
    info::{libfabric_version, Info, InfoEntry},
    infocapsoptions::InfoCaps,
    mr::{DisabledMemoryRegion, MaybeDisabledMemoryRegion, MemoryRegion, MemoryRegionBuilder},
    *,
};

use crate::lamellae::{
    comm::error::{AllocError, AllocResult, FabricError, FabricResult},
    AllocationType, CommAllocAddr, CommSlice,
};

use parking_lot::RwLock;
use pmi::pmi::Pmi;
use pmi::pmix::PmiX;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};
use tracing::{info, trace};

type WaitableEq = libfabric::eq_caps_type!(EqCaps::WAIT);
type WaitableCq = libfabric::cq_caps_type!(CqCaps::WAIT);
type WaitableCntr = libfabric::cntr_caps_type!(CntrCaps::WAIT);
type RmaAtomicCollEp =
    libfabric::info_caps_type!(FabInfoCaps::ATOMIC, FabInfoCaps::RMA, FabInfoCaps::COLL);

// #[derive(Debug)]
enum BarrierImpl {
    Uninit,
    Collective(MulticastGroupCollective),
    Manual(usize, AtomicUsize),
}

pub(crate) struct Ofi {
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    mapped_addresses: Vec<MappedAddress>,
    barrier_impl: BarrierImpl,
    ep: ConnectionlessEndpoint<RmaAtomicCollEp>,
    cq: CompletionQueue<WaitableCq>,
    put_cntr: Counter<WaitableCntr>,
    get_cntr: Counter<WaitableCntr>,
    av: AddressVector,
    eq: EventQueue<WaitableEq>,
    domain: Domain,
    _fabric: Fabric,
    info_entry: InfoEntry<RmaAtomicCollEp>,
    alloc_manager: AllocInfoManager,
    my_pmi: Arc<PmiX>,
    put_cnt: AtomicU64,
    get_cnt: AtomicU64,
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
        let my_pmi = Arc::new(PmiX::new().map_err(|e| {
            eprintln!("Error initializing PMI: {:?}", e);
            FabricError::InitError(1)
        })?);

        // println!("Using PMI my_pe {} num_pes {}",my_pmi.rank(),my_pmi.ranks().len());

        let info = Info::new(&libfabric_version())
            .enter_hints()
            .caps(InfoCaps::new().rma().atomic().collective())
            .mode(Mode::new().context())
            .enter_ep_attr()
            .type_(EndpointType::Rdm)
            .leave_ep_attr()
            .enter_domain_attr()
            .threading(Threading::Safe) //test different modes
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

        // println!("Found the following providers");
        // info.iter().for_each(|e| println!("{:?}", e));
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
        //println!("Using provider: {:?}", info_entry);

        let fabric = FabricBuilder::new()
            .build(&info_entry)
            .map_err(|e| FabricError::InitError(e.c_err))?;
        let eq = EventQueueBuilder::new(&fabric)
            .build()
            .map_err(|e| FabricError::InitError(e.c_err))?;

        let domain = DomainBuilder::new(&fabric, &info_entry)
            .build()
            .map_err(|e| FabricError::InitError(e.c_err))?;
        let mut coll_attr = CollectiveAttr::<()>::new();
        domain
            .query_collective::<()>(CollectiveOp::AllGather, &mut coll_attr)
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

        // ep.bind_shared_cq(&cq, true)?;

        // ep.bind_av(&av)?;
        ep.bind_eq(&eq)
            .map_err(|e| FabricError::InitError(e.c_err))?;

        let ep = ep
            .enable(&av)
            .map_err(|e| FabricError::InitError(e.c_err))?;

        let address = ep.getname().map_err(|e| FabricError::InitError(e.c_err))?;
        let address_bytes = address.as_bytes();

        my_pmi.put("epname", address_bytes).unwrap();
        my_pmi.exchange().unwrap();

        let unmapped_addresses: Vec<_> = my_pmi
            .ranks()
            .iter()
            .map(|r| {
                let addr = my_pmi.get("epname", &address_bytes.len(), &r).unwrap();
                unsafe { Address::from_bytes(&addr) }
            })
            .collect();

        let mapped_addresses = av
            .insert(AvInAddress::Encoded(&unmapped_addresses), AVOptions::new())
            .map_err(|e| FabricError::InitError(e.c_err))?;
        let mapped_addresses: Vec<MappedAddress> =
            mapped_addresses.into_iter().map(|a| a.unwrap()).collect();
        let alloc_manager = AllocInfoManager::new();

        let mut ofi = Self {
            num_pes: my_pmi.ranks().len(),
            my_pe: my_pmi.rank(),
            my_pmi,
            info_entry,
            _fabric: fabric,
            domain,
            av,
            eq,
            put_cntr,
            get_cntr,
            cq,
            ep,
            mapped_addresses,
            alloc_manager,
            barrier_impl: BarrierImpl::Uninit,
            put_cnt: AtomicU64::new(0),
            get_cnt: AtomicU64::new(0),
        };

        ofi.init_barrier()?;

        Ok(Arc::new(ofi))
    }

    fn create_mc_group(
        &self,
        pes: &[usize],
    ) -> Result<MulticastGroupCollective, libfabric::error::Error> {
        // println!("Creating MC group of len: {}", pes.len());
        let mut av_set = AddressVectorSetBuilder::new_from_range(
            &self.av,
            &self.mapped_addresses[pes[0]],
            &self.mapped_addresses[pes[0]],
            1,
        )
        .count(pes.len())
        .build()
        .unwrap();

        for pe in pes.iter().skip(1) {
            av_set.insert(&self.mapped_addresses[*pe]).unwrap();
        }

        let mut ctx = self.info_entry.allocate_context();
        let mc = MulticastGroupCollective::new(&av_set);
        mc.join_collective_with_context(&self.ep, JoinOptions::new(), &mut ctx)
            .unwrap();
        self.wait_for_join_event(&ctx).unwrap();
        // println!("Done Creating MC group");

        Ok(mc)
    }

    fn wait_for_join_event(&self, ctx: &Context) -> Result<(), libfabric::error::Error> {
        loop {
            let eq_res = self.eq.read();

            match eq_res {
                Ok(event) => {
                    if let Event::JoinComplete(entry) = event {
                        if entry.is_context_equal(ctx) {
                            return Ok(());
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

    pub(crate) fn progress(&self) -> Result<(), libfabric::error::Error> {
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

    fn wait_for_completion(&self, ctx: &Context) -> Result<(), libfabric::error::Error> {
        loop {
            let cq_res = self.cq.read(1);
            match cq_res {
                Ok(completion) => match completion {
                    Completion::Ctx(entries) | Completion::Unspec(entries) => {
                        if entries[0].is_op_context_equal(ctx) {
                            return Ok(());
                        }
                    }
                    Completion::Msg(entries) => {
                        if entries[0].is_op_context_equal(ctx) {
                            return Ok(());
                        }
                    }
                    Completion::Data(entries) => {
                        if entries[0].is_op_context_equal(ctx) {
                            return Ok(());
                        }
                    }
                    Completion::Tagged(entries) => {
                        if entries[0].is_op_context_equal(ctx) {
                            return Ok(());
                        }
                    }
                },
                Err(err) => {
                    if !matches!(err.kind, libfabric::error::ErrorKind::TryAgain) {
                        return Err(err);
                    }
                }
            }
        }
    }

    pub(crate) fn wait_all(&self) -> Result<(), libfabric::error::Error> {
        trace!("wait_all");
        // self.wait_all_put()?;
        // self.wait_all_get()
        self.wait_for_tx_cntr()?;
        trace!("wait_all put done");
        self.wait_for_rx_cntr()?;
        trace!("wait_all done");
        Ok(())
    }

    pub(crate) fn wait_all_put(&self) -> Result<(), libfabric::error::Error> {
        let mut cnt = self.put_cnt.load(Ordering::SeqCst);

        loop {
            let prev_cnt = cnt;
            match self.put_cntr.wait(prev_cnt as u64, -1) {
                Ok(_) => {}
                Err(e) => {
                    println!("put Error {e}");
                    let err = self.put_cntr.readerr();

                    continue;
                }
            }
            cnt = self.put_cnt.load(Ordering::SeqCst);

            if prev_cnt >= cnt {
                break;
            }
        }

        Ok(())
    }

    pub(crate) fn wait_all_get(&self) -> Result<(), libfabric::error::Error> {
        let mut cnt = self.get_cnt.load(Ordering::SeqCst);
        // println!("Waiting for all gets, current count: {}", cnt);
        loop {
            let prev_cnt = cnt;
            self.get_cntr.wait(prev_cnt as u64, -1)?;
            cnt = self.get_cnt.load(Ordering::SeqCst);

            if prev_cnt >= cnt {
                break;
            }
        }
        // println!("Done waiting for all gets, final count: {}", cnt);

        Ok(())
    }

    // fn wait_for_tx_cntr(&self, target: usize) -> Result<(), libfabric::error::Error> {
    //     self.put_cntr.wait(target as u64, -1)
    // }
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
        let mut expected_cnt = pending.load(Ordering::SeqCst);
        let mut prev_expected_cnt = expected_cnt;
        let mut cur_cnt = cntr.read();
        let mut old_cnt = cur_cnt;
        let mut err_cnt = cntr.readerr();
        let mut first = true;
        trace!(
            "{dir} before.  expected_cnt {expected_cnt} prev_expected_cnt {prev_expected_cnt} cur_cnt {} old_cnt {old_cnt} ",
            cntr.read(),
        );
        // let mut timer = std::time::Instant::now();

        while expected_cnt > cur_cnt
            || prev_expected_cnt < expected_cnt
            || cur_cnt != old_cnt
            || first
        {
            first = false;
            prev_expected_cnt = expected_cnt;
            old_cnt = cur_cnt;
            let _ = self.progress();
            let wait_result = cntr.wait(prev_expected_cnt as u64, -1);

            if let Err(err) = wait_result {
                if let libfabric::error::ErrorKind::TimedOut = err.kind {
                    // if timer.elapsed() > std::time::Duration::from_millis(1000) {
                    //     println!(
                    //         "1. cnt {cnt} prev_cnt {prev_cnt} cur_cnt {cur_cnt} old_cnt {old_cnt} {}",cnt > cur_cnt || prev_cnt < cnt || cur_cnt != old_cnt
                    //     );
                    //     timer = std::time::Instant::now();
                    // }
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

            // if timer.elapsed() > std::time::Duration::from_millis(1000) {
            //     println!(
            //         "2. cnt {cnt} prev_cnt {prev_cnt} cur_cnt {cur_cnt} old_cnt {old_cnt}  {}",
            //         cnt > cur_cnt || prev_cnt < cnt || cur_cnt != old_cnt
            //     );
            //     timer = std::time::Instant::now();
            // }
            // cnt = pending.load(Ordering::SeqCst);
        }
        trace!(
            "{dir} after.   expected_cnt {expected_cnt} prev_expected_cnt {prev_expected_cnt} cur_cnt {} old_cnt {old_cnt} ",
            cntr.read(),
        );
        Ok(())
        // self.get_cntr.wait(target as u64, -1)
    }

    fn pmi_exchange_mr_info(
        &self,
        mem: &[u8],
        mr: &MemoryRegion,
    ) -> HashMap<usize, RemoteMemAddressInfo> {
        let mem_info = MemAddressInfo::from_slice(mem, 0, &mr.key().unwrap(), &self.info_entry);

        let mut mem_info_bytes = mem_info.to_bytes();
        self.my_pmi.put("mr_info", &mem_info_bytes).unwrap();
        self.my_pmi.exchange().unwrap();
        // self.my_pmi.barrier(true).unwrap();
        let mut all_mem_info_bytes = vec![0u8; mem_info_bytes.len() * self.num_pes];
        for i in 0..self.num_pes {
            let mem_info_bytes = self
                .my_pmi
                .get("mr_info", &mem_info_bytes.len(), &i)
                .unwrap();
            all_mem_info_bytes[i * mem_info_bytes.len()..(i + 1) * mem_info_bytes.len()]
                .copy_from_slice(&mem_info_bytes);
        }

        all_mem_info_bytes
            .chunks_exact(std::mem::size_of::<MemAddressInfo>())
            .enumerate()
            .map(|(pe, chunk)| {
                let mut mem_info = unsafe { MemAddressInfo::from_bytes(chunk) };
                let rem_mem_info = mem_info
                    .into_remote_info(&self.domain)
                    .expect("Failed to convert MemAddressInfo to RemoteMemAddressInfo");
                trace!(
                    "PE {}: RemoteMemAddressInfo: {:?} {:?} ",
                    pe,
                    rem_mem_info.mem_address(),
                    rem_mem_info.mem_len()
                );

                (pe, rem_mem_info)
            })
            .collect::<HashMap<_, _>>()
    }

    //fixme
    fn collective_exchange_mr_info(
        &self,
        pes: &[usize],
        mem: &[u8],
        mr: &MemoryRegion,
    ) -> Result<HashMap<usize, RemoteMemAddressInfo>, libfabric::error::Error> {
        let mc = self.create_mc_group(&pes)?;

        let mut mem_info = MemAddressInfo::from_slice(mem, 0, &mr.key().unwrap(), &self.info_entry);

        let mut mem_info_bytes = mem_info.to_bytes_mut();
        let mut all_mem_info_bytes = vec![0u8; mem_info_bytes.len() * pes.len()];
        let mut ctx = self.info_entry.allocate_context();

        self.ep.allgather_with_context(
            &mut mem_info_bytes,
            None,
            &mut all_mem_info_bytes,
            None,
            &mc,
            CollectiveOptions::new(),
            &mut ctx,
        )?;

        self.wait_for_completion(&ctx)?;

        let all_mem_info: HashMap<_, _> = all_mem_info_bytes
            .chunks_exact(std::mem::size_of::<MemAddressInfo>())
            .enumerate()
            .map(|(pe, chunk)| {
                let mut mem_info = unsafe { MemAddressInfo::from_bytes(chunk) };
                let rem_mem_info = mem_info
                    .into_remote_info(&self.domain)
                    .expect("Failed to convert MemAddressInfo to RemoteMemAddressInfo");
                trace!(
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

    fn post_put(
        &self,
        mut fun: impl FnMut() -> Result<(), libfabric::error::Error>,
    ) -> Result<u64, libfabric::error::Error> {
        loop {
            match fun() {
                Ok(_) => break,
                Err(error) => {
                    if matches!(error.kind, libfabric::error::ErrorKind::TryAgain) {
                        trace!("need to progress, retrying put");
                        self.progress()?;
                    } else {
                        return Err(error);
                    }
                }
            }
        }
        trace!("done posting put");
        Ok(self.put_cnt.fetch_add(1, Ordering::SeqCst) + 1)
    }

    fn post_get(
        &self,
        mut fun: impl FnMut() -> Result<(), libfabric::error::Error>,
    ) -> Result<u64, libfabric::error::Error> {
        loop {
            match fun() {
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

        Ok(self.get_cnt.fetch_add(1, Ordering::SeqCst))
    }

    fn init_barrier(&mut self) -> FabricResult<()> {
        let mut coll_attr = CollectiveAttr::<()>::new();

        if self
            .domain
            .query_collective::<()>(CollectiveOp::Barrier, &mut coll_attr)
            .is_err()
        {
            let all_pes: Vec<_> = (0..self.num_pes).collect();
            let barrier_size = all_pes.len() * std::mem::size_of::<usize>();
            let barrier_addr = self.sub_alloc(&all_pes, barrier_size).map_err(|e| {
                if let AllocError::FabricAllocationError(err_no) = e {
                    FabricError::BarrierError(err_no as u32)
                } else {
                    FabricError::BarrierError(u32::MAX)
                }
            })?;

            self.barrier_impl = BarrierImpl::Manual(barrier_addr.start(), AtomicUsize::new(0));
            Ok(())
        } else {
            let all_pes: Vec<_> = (0..self.num_pes).collect();
            self.barrier_impl = BarrierImpl::Collective(
                self.create_mc_group(&all_pes)
                    .map_err(|e| FabricError::BarrierError(e.c_err))?,
            );
            Ok(())
        }
    }
    pub(crate) fn alloc(&self, size: usize, alloc: AllocationType) -> AllocResult<Arc<AllocInfo>> {
        match alloc {
            AllocationType::Sub(pes) => self.sub_alloc(&pes, size),
            AllocationType::Global => self.full_alloc(size),
            _ => return Err(AllocError::UnexpectedAllocationType(alloc)),
        }
    }

    fn full_alloc(&self, size: usize) -> AllocResult<Arc<AllocInfo>> {
        // Align to page boundaries
        let aligned_size = if (self.alloc_manager.page_size() - 1) & size != 0 {
            (size + self.alloc_manager.page_size()) & !(self.alloc_manager.page_size() - 1)
        } else {
            size
        };

        // Map memory of aligned size
        let mut mem = memmap::MmapOptions::new()
            .len(aligned_size)
            .map_anon()
            .expect("Error in allocating aligned memory");

        // Initialize mapped memory to zeros
        mem.iter_mut().map(|x| *x = 0).count();

        let mem_addr = mem.as_ptr() as usize;
        let mr = MemoryRegionBuilder::new(&mem, HmemIface::System)
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
                        // println!("Binding memory region to endpoint");
                        mr.enable(&self.ep)
                            .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?
                    }
                    DisabledMemoryRegion::RmaEvent(mr) => {
                        // println!("Binding memory region to domain");
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
            .collective_exchange_mr_info(&(0..self.num_pes).collect::<Vec<_>>(), &mem, &mr)
            .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?;
        let alloc = Arc::new(
            AllocInfo::new(Arc::new(mem), mr, remote_alloc_infos, size)
                .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?,
        );
        self.alloc_manager.insert(alloc.clone());
        Ok(alloc)
    }

    fn sub_alloc(&self, pes: &[usize], size: usize) -> AllocResult<Arc<AllocInfo>> {
        // Align to page boundaries
        let aligned_size = if (self.alloc_manager.page_size() - 1) & size != 0 {
            (size + self.alloc_manager.page_size()) & !(self.alloc_manager.page_size() - 1)
        } else {
            size
        };

        // Map memory of aligned size
        let mut mem = memmap::MmapOptions::new()
            .len(aligned_size)
            .map_anon()
            .expect("Error in allocating aligned memory");

        // Initialize mapped memory to zeros
        mem.iter_mut().map(|x| *x = 0).count();

        let mem_addr = mem.as_ptr() as usize;
        let mr = MemoryRegionBuilder::new(&mem, HmemIface::System)
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
                        // println!("Binding memory region to endpoint");
                        mr.enable(&self.ep)
                            .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?
                    }
                    DisabledMemoryRegion::RmaEvent(mr) => {
                        // println!("Binding memory region to domain");
                        mr.enable()
                            .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?
                        // This will bind the memory region to the domain
                    }
                }
            }
            MaybeDisabledMemoryRegion::Enabled(mr) => mr,
        };

        let remote_alloc_infos = self
            .collective_exchange_mr_info(pes, &mem, &mr)
            .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?;

        let alloc = Arc::new(
            AllocInfo::new(Arc::new(mem), mr, remote_alloc_infos, size)
                .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?,
        );
        self.alloc_manager.insert(alloc.clone());
        Ok(alloc)
    }

    pub(crate) fn get_alloc_from_start_addr(
        &self,
        addr: CommAllocAddr,
    ) -> AllocResult<Arc<AllocInfo>> {
        self.alloc_manager.get_alloc_from_start_addr(addr)
    }

    pub(crate) fn free_alloc(&self, alloc_info: &AllocInfo) -> Result<(), libfabric::error::Error> {
        self.alloc_manager.remove_from_alloc(alloc_info);
        Ok(())
    }

    pub(crate) fn free_addr(&self, addr: usize) -> Result<(), libfabric::error::Error> {
        self.alloc_manager.remove_from_addr(&addr);
        Ok(())
    }

    pub(crate) fn clear_allocs(&self) -> Result<(), libfabric::error::Error> {
        self.alloc_manager.clear();
        Ok(())
    }

    pub(crate) fn barrier(&self) -> Result<(), libfabric::error::Error> {
        // println!("Running barrier");
        match &self.barrier_impl {
            BarrierImpl::Uninit => {
                panic!("Barrier is not initialized");
            }
            BarrierImpl::Collective(mc) => {
                let mut ctx = self.info_entry.allocate_context();
                loop {
                    let ret = self.ep.barrier_with_context(mc, &mut ctx);
                    match &ret {
                        Ok(_) => break,
                        Err(err) => {
                            if !matches!(err.kind, libfabric::error::ErrorKind::TryAgain) {
                                return ret;
                            }
                        }
                    }
                }
                self.wait_for_completion(&ctx)?;
                // println!("Done with barrier");
                Ok(())
            }
            BarrierImpl::Manual(barrier_addr, barrier_id) => {
                let n = 2;
                let pes = (0..self.num_pes).collect::<Vec<_>>();
                let num_pes = pes.len();
                let num_rounds = ((num_pes as f64).log2() / (n as f64).log2()).ceil();
                let my_barrier = barrier_id.fetch_add(1, Ordering::SeqCst);

                for round in 0..num_rounds as usize {
                    for i in 1..=n {
                        let send_pe = euclid_rem(
                            self.my_pe as i64 + i as i64 * (n as i64 + 1).pow(round as u32),
                            num_pes as i64,
                        );

                        let dst = barrier_addr + 8 * self.my_pe;
                        unsafe {
                            self.inner_put(dst, std::slice::from_ref(&my_barrier), send_pe, false)?
                        };
                    }

                    for i in 1..=n {
                        let recv_pe = euclid_rem(
                            self.my_pe as i64 - i as i64 * (n as i64 + 1).pow(round as u32),
                            num_pes as i64,
                        );
                        let barrier_vec = unsafe {
                            std::slice::from_raw_parts(barrier_addr as *const usize, num_pes)
                        };

                        while my_barrier > barrier_vec[recv_pe] {
                            self.progress()?;
                            std::thread::yield_now();
                        }
                    }
                }

                Ok(())
            }
        }
    }

    pub(crate) unsafe fn put<T: Copy>(
        &self,
        pe: usize,
        src_addr: &CommSlice<T>,
        dst_addr: &CommAllocAddr,
        sync: bool,
    ) -> Result<(), libfabric::error::Error> {
        self.inner_put(pe, src_addr.as_ref(), *(dst_addr as &usize), sync)
    }

    unsafe fn inner_put<T: Copy>(
        &self,
        pe: usize,
        src_addr: &[T],
        dst_addr: usize,
        sync: bool,
    ) -> Result<(), libfabric::error::Error> {
        let (offset, mr, remote_alloc_info) = {
            let table = self.alloc_manager.mr_info_table.read();
            let alloc_info = table
                .iter()
                .find(|e| e.contains(&dst_addr))
                .expect("Invalid address");

            (
                alloc_info.start(),
                alloc_info.mr(),
                alloc_info.remote_info(&pe).expect(&format!(
                    "PE {} is not part of the sub allocation group",
                    pe
                )),
            )
        };

        let mut remote_dst_addr = remote_alloc_info.mem_address().add(dst_addr - offset);
        trace!(
            "Remote destination address for PE {}: {:?}",
            pe,
            remote_dst_addr
        );

        let remote_key = remote_alloc_info.key();
        if std::mem::size_of_val(src_addr) < self.info_entry.tx_attr().inject_size() {
            trace!(
                "Injecting write to PE {} at address {:?}",
                pe,
                remote_dst_addr
            );
            self.post_put(|| unsafe {
                self.ep.inject_write_to(
                    src_addr,
                    &self.mapped_addresses[pe],
                    remote_dst_addr,
                    &remote_key,
                )
            })?;
        } else {
            let mut curr_idx = 0;

            let mut cntr_order = 0;
            while curr_idx < src_addr.len() {
                let msg_len = std::cmp::min(
                    src_addr.len() - curr_idx,
                    self.info_entry.ep_attr().max_msg_size(),
                );

                let order = self.post_put(|| unsafe {
                    self.ep.write_to(
                        &src_addr[curr_idx..curr_idx + msg_len],
                        Some(&mr.descriptor()),
                        &self.mapped_addresses[pe],
                        remote_dst_addr,
                        &remote_key,
                    )
                })?;

                remote_dst_addr = remote_dst_addr.add(msg_len);
                curr_idx += msg_len;
            }
        }

        if sync {
            self.wait_for_tx_cntr()?;
        }
        // println!("Done putting");
        Ok(())
    }

    pub(crate) unsafe fn get<T: Copy>(
        &self,
        pe: usize,
        src_addr: &CommAllocAddr,
        dst_addr: &mut CommSlice<T>,
        sync: bool,
    ) -> Result<(), libfabric::error::Error> {
        self.inner_get(pe, *(src_addr as &usize), dst_addr, sync)
    }

    unsafe fn inner_get<T: Copy>(
        &self,
        pe: usize,
        src_addr: usize,
        dst_addr: &mut [T],
        sync: bool,
    ) -> Result<(), libfabric::error::Error> {
        let (offset, mr, remote_alloc_info) = {
            let table = self.alloc_manager.mr_info_table.read();
            let alloc_info = table
                .iter()
                .find(|e| e.contains(&src_addr))
                .expect("Invalid address");

            (
                alloc_info.start(),
                alloc_info.mr(),
                alloc_info.remote_info(&pe).expect(&format!(
                    "PE {} is not part of the sub allocation group",
                    pe
                )),
            )
        };

        let mut remote_src_addr = remote_alloc_info.mem_address().add(src_addr - offset);
        let remote_key = remote_alloc_info.key();

        let mut curr_idx = 0;

        while curr_idx < dst_addr.len() {
            let msg_len = std::cmp::min(
                dst_addr.len() - curr_idx,
                self.info_entry.ep_attr().max_msg_size(),
            );
            let order = self.post_get(|| unsafe {
                self.ep.read_from(
                    &mut dst_addr[curr_idx..curr_idx + msg_len],
                    Some(&mr.descriptor()),
                    &self.mapped_addresses[pe],
                    remote_src_addr,
                    &remote_key,
                )
            })?;
            remote_src_addr = remote_src_addr.add(msg_len);
            curr_idx += msg_len;
        }

        if sync {
            self.wait_for_rx_cntr()?;
        }

        Ok(())
    }

    pub(crate) fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize {
        self.alloc_manager
            .local_addr(remote_pe, remote_addr)
            .expect(&format!(
                "Local address not found from remote PE {}, remote addr: {:x}",
                remote_pe, remote_addr
            ))
    }

    pub(crate) fn remote_addr(&self, pe: usize, local_addr: usize) -> usize {
        self.alloc_manager
            .remote_addr(pe, local_addr)
            .expect(&format!("Remote address not found for PE {}", pe))
    }

    // pub(crate) fn release(&self, addr: &usize) {
    //     self.alloc_manager.remove(addr);
    // }
}

impl Drop for Ofi {
    fn drop(&mut self) {
        self.wait_all_get().unwrap();
        self.wait_all_put().unwrap();
    }
}

pub(crate) struct AllocInfoManager {
    pub(crate) mr_info_table: Arc<RwLock<Vec<Arc<AllocInfo>>>>,
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

    pub(crate) fn insert(&self, alloc: Arc<AllocInfo>) {
        self.mr_info_table.write().push(alloc);
    }

    pub(crate) fn clear(&self) {
        let mut table = self.mr_info_table.write();
        table.clear();
    }

    pub(crate) fn remove_from_addr(&self, mem_addr: &usize) {
        let mut table = self.mr_info_table.write();
        let idx = table
            .iter()
            .position(|e| e.start() == *mem_addr)
            .expect("Error! Invalid memory address");

        table.remove(idx);
    }
    pub(crate) fn remove_from_alloc(&self, mem_addr: &AllocInfo) {
        let mut table = self.mr_info_table.write();
        let idx = table
            .iter()
            .position(|e| e.start() == mem_addr.start())
            .expect("Error! Invalid memory address");

        table.remove(idx);
    }

    pub(crate) fn get_alloc(&self, mem_addr: CommAllocAddr) -> Option<Arc<AllocInfo>> {
        let table = self.mr_info_table.read();
        table.iter().find(|e| e.contains(&mem_addr.0)).cloned()
    }

    pub(crate) fn get_alloc_from_start_addr(
        &self,
        mem_addr: CommAllocAddr,
    ) -> AllocResult<Arc<AllocInfo>> {
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

// #[derive(Clone)]
// pub(crate) struct RemoteAllocInfo {
//     key: MappedMemoryRegionKey,
//     range: std::ops::Range<usize>,
// }

// impl RemoteAllocInfo {
//     pub(crate) fn from_rma_iov(
//         rma_iov: RmaIoVec,
//         key: MappedMemoryRegionKey,
//     ) -> Self {
//         let start = rma_iov.get_address() as usize;
//         let end = start + rma_iov.get_len();

//         Self {
//             key,
//             range: std::ops::Range { start, end },
//         }
//     }

// //     #[allow(dead_code)]
// //     pub(crate) fn new(
// //         range: std::ops::Range<usize>,
// //         key: MappedMemoryRegionKey,
// //     ) -> Self {
// //         Self { key, range }
// //     }

//     pub(crate) fn start(&self) -> usize {
//         self.range.start
//     }

//     pub(crate) fn end(&self) -> usize {
//         self.range.end
//     }

//     pub(crate) fn key(&self) -> &libfabric::mr::MappedMemoryRegionKey {
//         &self.key
//     }

//     pub(crate) fn contains(&self, addr: &usize) -> bool {
//         self.range.contains(addr)
//     }
// }

pub(crate) struct AllocInfo {
    mem: Arc<memmap::MmapMut>,
    mr: MemoryRegion,
    range: std::ops::Range<usize>,
    remote_allocs: HashMap<usize, RemoteMemAddressInfo>,
}

impl std::fmt::Debug for AllocInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AllocInfo")
            .field("range", &self.range)
            // .field("remote_allocs", &self.remote_allocs)
            .finish()
    }
}

impl AllocInfo {
    pub(crate) fn new(
        mem: Arc<memmap::MmapMut>,
        mr: MemoryRegion,
        remote_allocs: HashMap<usize, RemoteMemAddressInfo>,
        num_bytes: usize,
    ) -> Result<Self, libfabric::error::Error> {
        let start = mem.as_ptr() as usize;
        let end = start + num_bytes; //mem.len();

        Ok(Self {
            mem: mem,
            mr,
            range: std::ops::Range { start, end },
            remote_allocs,
        })
    }
    pub(crate) fn sub_alloc(&self, offset: usize, len: usize) -> AllocResult<Arc<Self>> {
        if offset + len > self.num_bytes() {
            return Err(AllocError::InvalidSubAlloc(offset, len));
        }
        // let start = offset;
        // let end = start + len;
        // let range = std::ops::Range { start, end };
        let mut remote_allocs = HashMap::new();
        for (pe, remote_info) in self.remote_allocs.iter() {
            let new_remote_info = unsafe { remote_info.sub_region(offset..offset + len) };
            remote_allocs.insert(*pe, new_remote_info);
        }

        Ok(Arc::new(Self {
            mem: self.mem.clone(),
            mr: self.mr.clone(),
            range: self.range.start + offset..self.range.start + offset + len,
            remote_allocs,
        }))
    }

    pub(crate) fn start(&self) -> usize {
        self.range.start
    }
    pub(crate) fn num_bytes(&self) -> usize {
        self.range.end - self.range.start
    }

    // #[allow(dead_code)]
    // pub(crate) fn remote_start(&self, remote_id: &usize) -> usize {
    //     self.remote_allocs
    //         .get(remote_id)
    //         .expect(&format!(
    //             "PE {} is not part of the sub allocation group",
    //             remote_id
    //         ))
    //         .start()
    // }

    //     #[allow(dead_code)]
    //     pub(crate) fn remote_end(&self, remote_id: &usize) -> usize {
    //         self.remote_allocs
    //             .get(remote_id)
    //             .expect(&format!(
    //                 "PE {} is not part of the sub allocation group",
    //                 remote_id
    //             ))
    //             .end()
    //     }

    //     #[allow(dead_code)]
    //     pub(crate) fn end(&self) -> usize {
    //         self.range.end
    //     }

    //     #[allow(dead_code)]
    //     pub(crate) fn key(&self) -> &libfabric::mr::MemoryRegionKey {
    //         &self.key
    //     }

    //     #[allow(dead_code)]
    //     pub(crate) fn remote_key(&self, remote_id: &usize) -> &libfabric::mr::MappedMemoryRegionKey {
    //         self.remote_allocs
    //             .get(remote_id)
    //             .expect(&format!(
    //                 "PE {} is not part of the sub allocation group",
    //                 remote_id
    //             ))
    //             .key()
    //     }

    pub(crate) fn contains(&self, addr: &usize) -> bool {
        self.range.contains(addr)
    }

    pub(crate) fn remote_contains(&self, remote_id: &usize, addr: &usize) -> bool {
        match self.remote_allocs.get(remote_id) {
            Some(remote_info) => remote_info.contains(&addr),
            None => {
                trace!(
                    "Remote PE {} is not part of the sub allocation group",
                    remote_id
                );
                for (pe, remote_info) in self.remote_allocs.iter() {
                    trace!(
                        "  PE {}: {:?} {:?}",
                        pe,
                        remote_info.mem_address(),
                        remote_info.mem_len()
                    );
                }
                false
                // panic!(" {:?}", self)
            }
        }
    }

    pub(crate) fn remote_info(&self, remote_pe: &usize) -> Option<RemoteMemAddressInfo> {
        self.remote_allocs.get(remote_pe).cloned()
    }

    pub(crate) fn mr_desc(&self) -> libfabric::mr::MemoryRegionDesc {
        self.mr.descriptor()
    }
    pub(crate) fn mr(&self) -> MemoryRegion {
        self.mr.clone()
    }

    //     #[allow(dead_code)]
    //     pub(crate) fn mr(&self) -> &libfabric::mr::MemoryRegion {
    //         &self.mr
    //     }
}

fn euclid_rem(a: i64, b: i64) -> usize {
    let r = a % b;

    if r >= 0 {
        r as usize
    } else {
        (r + b.abs()) as usize
    }
}

// pub struct LibfabricWorld {
//     ofi: Rc<Ofi>,
// }

// impl LibfabricWorld {
//     pub fn new() -> Result<Self, libfabric::error::Error> {
//         let ofi = Ofi::new(Some("verbs"), None)?;
//         Ok(Self { ofi })
//     }

//     pub fn mype(&self) -> usize {
//         self.ofi.my_pe
//     }
//     pub fn numpes(&self) -> usize {
//         self.ofi.num_pes
//     }
//     pub fn barrier(&self) {
//         self.ofi.barrier();
//     }
//     pub fn alloc<T: Copy>(&self, num_elems: usize) -> LibfabricArray<T> {
//         let size = num_elems * std::mem::size_of::<T>();
//         let pes = (0..self.ofi.num_pes).collect::<Vec<_>>();
//         LibfabricArray {
//             ofi: self.ofi.clone(),
//             alloc_info: self.ofi.alloc(size).unwrap(),
//             local_size: num_elems,
//             total_size: num_elems * self.ofi.num_pes,
//             num_pes: self.ofi.num_pes,
//             _phantom: std::marker::PhantomData,
//         }
//     }
// }

// pub struct LibfabricArray<T: Copy> {
//     ofi: Rc<Ofi>,
//     alloc_info: AllocInfo,
//     local_size: usize,
//     total_size: usize,
//     num_pes: usize,
//     _phantom: std::marker::PhantomData<T>,
// }

// impl<T: Copy> LibfabricArray<T> {
//     pub fn len(&self) -> usize {
//         self.total_size
//     }

//     pub fn as_mut_slice(&self) -> &mut [T] {
//         let start = self.alloc_info.start();
//         let end = start + self.local_size * std::mem::size_of::<T>();
//         unsafe { std::slice::from_raw_parts_mut(start as *mut T, self.local_size) }
//     }
//     pub fn as_slice(&self) -> &[T] {
//         let start = self.alloc_info.start();
//         let end = start + self.local_size * std::mem::size_of::<T>();
//         unsafe { std::slice::from_raw_parts(start as *const T, self.local_size) }
//     }
//     fn pe_and_offset(&self, index: usize) -> (usize, usize) {
//         if index >= self.total_size {
//             panic!("Index out of bounds");
//         }
//         let pe = index % self.num_pes;
//         let offset = index / self.num_pes;
//         (pe, offset)
//     }
//     pub fn put(&self, index: usize, val: T) {
//         let (pe, offset) = self.pe_and_offset(index);
//         let remote_alloc_info = self
//             .alloc_info
//             .remote_info(&pe)
//             .expect(&format!("PE {} is not part of the allocation group", pe));
//         let remote_addr = unsafe {
//             remote_alloc_info
//                 .mem_address()
//                 .add(offset * std::mem::size_of::<T>())
//         };
//         let remote_key = remote_alloc_info.key();
//         let cntr_order = self
//             .ofi
//             .post_put(|| unsafe {
//                 self.ofi.ep.inject_write_to(
//                     &[val],
//                     &self.ofi.mapped_addresses[pe],
//                     remote_addr,
//                     &remote_key,
//                 )
//             })
//             .expect("Failed to post put operation");
//         // self.ofi.wait_for_tx_cntr(cntr_order)
//         //     .expect("Failed to wait for put operation");
//     }

//     pub fn wait_all(&self) {
//         self.ofi
//             .wait_all()
//             .expect("Failed to wait for all operations");
//     }
// }
// impl<T: Copy + Default + std::fmt::Debug> LibfabricArray<T> {
//     pub fn get(&self, index: usize) -> T {
//         let (pe, offset) = self.pe_and_offset(index);
//         let remote_alloc_info = self
//             .alloc_info
//             .remote_info(&pe)
//             .expect(&format!("PE {} is not part of the allocation group", pe));
//         let remote_addr = unsafe {
//             remote_alloc_info
//                 .mem_address()
//                 .add(offset * std::mem::size_of::<T>())
//         };
//         let remote_key = remote_alloc_info.key();
//         // println!("Getting from PE {}, addr: {}, remote_addr: {:?}", pe, index, remote_addr);
//         let mut val = vec![T::default()];
//         let cntr_order = self
//             .ofi
//             .post_get(|| unsafe {
//                 self.ofi.ep.read_from(
//                     &mut val,
//                     Some(&self.alloc_info.mr_desc()),
//                     &self.ofi.mapped_addresses[pe],
//                     remote_addr,
//                     &remote_key,
//                 )
//             })
//             .expect("Failed to post get operation");
//         self.ofi
//             .wait_for_rx_cntr(cntr_order)
//             .expect("Failed to wait for get operation");
//         // println!("Got value: {:?}", val);
//         val[0]
//     }
// }

// impl<T: Copy + Default + libfabric::AsFiType> LibfabricArray<T> {
//     pub fn atomic_put(&self, index: usize, val: T) {
//         let (pe, offset) = self.pe_and_offset(index);
//         let remote_alloc_info = self
//             .alloc_info
//             .remote_info(&pe)
//             .expect(&format!("PE {} is not part of the allocation group", pe));
//         let remote_addr = unsafe {
//             remote_alloc_info
//                 .mem_address()
//                 .add(offset * std::mem::size_of::<T>())
//         };
//         let remote_key = remote_alloc_info.key();
//         let cntr_order = self
//             .ofi
//             .post_put(|| unsafe {
//                 self.ofi.ep.inject_atomic_to(
//                     &[val],
//                     &self.ofi.mapped_addresses[pe],
//                     remote_addr,
//                     &remote_key,
//                     AtomicOp::AtomicWrite,
//                 )
//             })
//             .expect("Failed to post atomic put operation");
//         // self.ofi.wait_for_tx_cntr(cntr_order)
//         //     .expect("Failed to wait for atomic put operation");
//     }

//     pub fn atomic_get(&self, index: usize) -> T {
//         let (pe, offset) = self.pe_and_offset(index);
//         let remote_alloc_info = self
//             .alloc_info
//             .remote_info(&pe)
//             .expect(&format!("PE {} is not part of the allocation group", pe));
//         let remote_addr = unsafe {
//             remote_alloc_info
//                 .mem_address()
//                 .add(offset * std::mem::size_of::<T>())
//         };
//         let remote_key = remote_alloc_info.key();
//         // println!("Getting from PE {}, addr: {}, remote_addr: {:?}", pe, index, remote_addr);
//         let mut val = vec![T::default()];
//         let cntr_order = self
//             .ofi
//             .post_get(|| unsafe {
//                 self.ofi.ep.fetch_atomic_from(
//                     &[T::default()],
//                     None,
//                     &mut val,
//                     None,
//                     &self.ofi.mapped_addresses[pe],
//                     remote_addr,
//                     &remote_key,
//                     FetchAtomicOp::AtomicRead,
//                 )
//             })
//             .expect("Failed to post atomic get operation");
//         self.ofi
//             .wait_for_rx_cntr(cntr_order)
//             .expect("Failed to wait for atomic get operation");
//         // println!("Got value: {:?}", val);
//         val[0]
//     }

//     pub fn swap(&self, index: usize, val: T) -> T {
//         let (pe, offset) = self.pe_and_offset(index);
//         let remote_alloc_info = self
//             .alloc_info
//             .remote_info(&pe)
//             .expect(&format!("PE {} is not part of the allocation group", pe));
//         let remote_addr = unsafe {
//             remote_alloc_info
//                 .mem_address()
//                 .add(offset * std::mem::size_of::<T>())
//         };
//         let remote_key = remote_alloc_info.key();
//         // println!("Swapping from PE {}, addr: {}, remote_addr: {:?}", pe, index, remote_addr);
//         let mut old_val = vec![val];
//         let mut new_val = vec![T::default()];
//         let cntr_order = self
//             .ofi
//             .post_get(|| unsafe {
//                 self.ofi.ep.fetch_atomic_from(
//                     &old_val,
//                     None,
//                     &mut new_val,
//                     None,
//                     &self.ofi.mapped_addresses[pe],
//                     remote_addr,
//                     &remote_key,
//                     FetchAtomicOp::AtomicWrite,
//                 )
//             })
//             .expect("Failed to post atomic swap operation");
//         self.ofi
//             .wait_for_rx_cntr(cntr_order)
//             .expect("Failed to wait for atomic swap operation");
//         // println!("Got value: {:?}", old_val);
//         new_val[0]
//     }
// }

// impl<T: Copy> Drop for LibfabricArray<T> {
//     fn drop(&mut self) {
//         // println!("Dropping LibfabricArray");
//         // self.ofi.alloc_manager.remove(&self.alloc_info.start());
//     }
// }

// impl LibfabricWorld {
//     fn init() -> Self {

//     }
// }

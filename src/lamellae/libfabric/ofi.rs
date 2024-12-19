use crate::lamellae::Remote;
use av::AvInAddress;
use comm::{
    collective::CollectiveEp,
    rma::{ReadEp, WriteEp},
};
use cq::ReadCq;
use ep::{ActiveEndpoint, BaseEndpoint};
use iovec::RmaIoVec;
use libfabric::cntr::WaitCntr;
use libfabric::info::Version;
use libfabric::mr::MaybeDisabledMemoryRegion;
use libfabric::*;
use parking_lot::RwLock;
use pmi::pmi::Pmi;
use std::sync::Arc;
use std::{
    collections::HashMap,
    rc::Rc,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
type WaitableEq = libfabric::eq_caps_type!(EqCaps::WAIT);
type WaitableCq = libfabric::cq_caps_type!(CqCaps::WAIT);
type WaitableCntr = libfabric::cntr_caps_type!(CntrCaps::WAIT);
type RmaAtomicCollEp =
    libfabric::info_caps_type!(FabInfoCaps::ATOMIC, FabInfoCaps::RMA, FabInfoCaps::COLL);

// #[derive(Debug)]
enum BarrierImpl {
    Uninit,
    Collective(libfabric::comm::collective::MulticastGroupCollective),
    Manual(usize, AtomicUsize),
}

pub(crate) struct Ofi {
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    mapped_addresses: Vec<libfabric::MappedAddress>,
    barrier_impl: BarrierImpl,
    ep: libfabric::connless_ep::ConnectionlessEndpoint<RmaAtomicCollEp>,
    cq: libfabric::cq::CompletionQueue<WaitableCq>,
    put_cntr: libfabric::cntr::Counter<WaitableCntr>,
    get_cntr: libfabric::cntr::Counter<WaitableCntr>,
    av: libfabric::av::AddressVector,
    eq: libfabric::eq::EventQueue<WaitableEq>,
    domain: libfabric::domain::Domain,
    fabric: libfabric::fabric::Fabric,
    info_entry: libfabric::info::InfoEntry<RmaAtomicCollEp>,
    alloc_manager: AllocInfoManager,
    my_pmi: Arc<dyn Pmi + Sync + Send>,
    put_cnt: AtomicUsize,
    get_cnt: AtomicUsize,
}

unsafe impl Sync for Ofi {}
unsafe impl Send for Ofi {}

impl Ofi {
    pub(crate) fn new(
        provider: Option<&str>,
        domain: Option<&str>,
    ) -> Result<Self, libfabric::error::Error> {
        let my_pmi = pmi::pmi::PmiBuilder::init().unwrap();

        // let info_caps = libfabric::infocapsoptions::InfoCaps::new().rma().atomic().collective();
        // let mut domain_conf = libfabric::domain::DomainAttr::new();
        // domain_conf.resource_mgmt= libfabric::enums::ResourceMgmt::Enabled;
        // domain_conf.threading = libfabric::enums::Threading::Domain;
        // domain_conf.mr_mode = libfabric::enums::MrMode::new().allocated().prov_key().virt_addr();
        // domain_conf.data_progress = libfabric::enums::Progress::Manual;

        // let mut endpoint_conf = libfabric::ep::EndpointAttr::new();
        //     endpoint_conf
        //     .ep_type(libfabric::enums::EndpointType::Rdm);

        // let info_hints = libfabric::info::InfoHints::new()
        //     .caps(info_caps)
        //     .domain_attr(domain_conf)
        //     .mode(libfabric::enums::Mode::new().context())
        //     .ep_attr(endpoint_conf);

        let info = libfabric::info::Info::new(&Version {
            major: 1,
            minor: 19,
        })
        .enter_hints()
        .caps(
            libfabric::infocapsoptions::InfoCaps::new()
                .rma()
                .atomic()
                .collective(),
        )
        .mode(libfabric::enums::Mode::new().context())
        .enter_domain_attr()
        .resource_mgmt(libfabric::enums::ResourceMgmt::Enabled)
        .threading(libfabric::enums::Threading::Domain)
        .mr_mode(
            libfabric::enums::MrMode::new()
                .allocated()
                .prov_key()
                .virt_addr(),
        )
        .data_progress(libfabric::enums::Progress::Manual)
        .leave_domain_attr()
        .enter_ep_attr()
        .type_(libfabric::enums::EndpointType::Rdm)
        .leave_ep_attr()
        .leave_hints()
        .get()?;

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

        let fabric = libfabric::fabric::FabricBuilder::new().build(&info_entry)?;
        let eq = libfabric::eq::EventQueueBuilder::new(&fabric).build()?;

        let domain = libfabric::domain::DomainBuilder::new(&fabric, &info_entry).build()?;
        let mut coll_attr = libfabric::comm::collective::CollectiveAttr::<()>::new();
        domain.query_collective::<()>(libfabric::enums::CollectiveOp::AllGather, &mut coll_attr)?;

        let cq = libfabric::cq::CompletionQueueBuilder::new()
            .format(libfabric::enums::CqFormat::Context)
            .size(info_entry.rx_attr().size())
            .build(&domain)?;

        let av = libfabric::av::AddressVectorBuilder::new().build(&domain)?;

        let put_cntr = libfabric::cntr::CounterBuilder::new().build(&domain)?;
        let get_cntr = libfabric::cntr::CounterBuilder::new().build(&domain)?; //

        let ep = libfabric::ep::EndpointBuilder::new(&info_entry).build(&domain)?;
        let ep = match ep {
            libfabric::ep::Endpoint::ConnectionOriented(_) => {
                panic!("Verbs should be connectionless, I think")
            }
            libfabric::ep::Endpoint::Connectionless(ep) => ep,
        };
        ep.bind_av(&av)?;
        ep.bind_cntr().write().cntr(&put_cntr)?;

        ep.bind_cntr().read().cntr(&get_cntr)?;

        ep.bind_shared_cq(&cq, true)?;

        ep.bind_eq(&eq)?;

        let ep = ep.enable()?;

        let address = ep.getname()?;
        let address_bytes = address.as_bytes();

        my_pmi.put("epname", address_bytes).unwrap();
        my_pmi.exchange().unwrap();

        let unmapped_addresses: Vec<_> = my_pmi
            .ranks()
            .iter()
            .map(|r| {
                let addr = my_pmi.get("epname", &address_bytes.len(), &r).unwrap();
                unsafe { ep::Address::from_bytes(&addr) }
            })
            .collect();

        let mapped_addresses = av.insert(
            AvInAddress::Encoded(&unmapped_addresses),
            libfabric::enums::AVOptions::new(),
        )?;
        let mapped_addresses: Vec<MappedAddress> =
            mapped_addresses.into_iter().map(|a| a.unwrap()).collect();
        let alloc_manager = AllocInfoManager::new();

        let mut ofi = Self {
            num_pes: my_pmi.ranks().len(),
            my_pe: my_pmi.rank(),
            my_pmi: Arc::new(my_pmi),
            info_entry,
            fabric,
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
            put_cnt: AtomicUsize::new(0),
            get_cnt: AtomicUsize::new(0),
        };

        ofi.init_barrier()?;

        Ok(ofi)
    }

    fn create_mc_group(
        &self,
        pes: &[usize],
    ) -> Result<libfabric::comm::collective::MulticastGroupCollective, libfabric::error::Error>
    {
        // let table_address = match &self.mapped_addresses[pes[0]] {
        //     _ => panic!("Address type is not table"),
        //     MappedAddress::Table(table_address) => table_address,
        // };
        // println!("Creating MC group of len: {}", pes.len());
        let mut av_set = libfabric::av::AddressVectorSetBuilder::new_from_range(
            &self.av,
            &self.mapped_addresses[pes[0]],
            &self.mapped_addresses[pes[0]],
            1,
        )
        .count(pes.len())
        // .start_addr(&self.mapped_addresses[pes[0]])
        // .end_addr(&self.mapped_addresses[pes[0]])
        // .stride(1)
        .build()
        .unwrap();

        for pe in pes.iter().skip(1) {
            av_set.insert(&self.mapped_addresses[*pe]).unwrap();
        }

        let mut ctx = self.info_entry.allocate_context();
        let mc = libfabric::comm::collective::MulticastGroupCollective::new(&av_set);
        mc.join_collective_with_context(&self.ep, libfabric::enums::JoinOptions::new(), &mut ctx)
            .unwrap();
        self.wait_for_join_event(&ctx).unwrap();
        // println!("Done Creating MC group");

        Ok(mc)
    }

    fn wait_for_join_event(&self, ctx: &libfabric::Context) -> Result<(), libfabric::error::Error> {
        loop {
            let eq_res = self.eq.read();

            match eq_res {
                Ok(event) => {
                    if let libfabric::eq::Event::JoinComplete(entry) = event {
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

    fn wait_for_completion(&self, ctx: &libfabric::Context) -> Result<(), libfabric::error::Error> {
        loop {
            let cq_res = self.cq.read(1);
            match cq_res {
                Ok(completion) => match completion {
                    libfabric::cq::Completion::Ctx(entries)
                    | libfabric::cq::Completion::Unspec(entries) => {
                        if entries[0].is_op_context_equal(ctx) {
                            return Ok(());
                        }
                    }
                    libfabric::cq::Completion::Msg(entries) => {
                        if entries[0].is_op_context_equal(ctx) {
                            return Ok(());
                        }
                    }
                    libfabric::cq::Completion::Data(entries) => {
                        if entries[0].is_op_context_equal(ctx) {
                            return Ok(());
                        }
                    }
                    libfabric::cq::Completion::Tagged(entries) => {
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
        self.wait_all_put()?;
        self.wait_all_get()
    }

    pub(crate) fn wait_all_put(&self) -> Result<(), libfabric::error::Error> {
        let mut cnt = self.put_cnt.load(Ordering::SeqCst);

        loop {
            let prev_cnt = cnt;
            self.put_cntr.wait(prev_cnt as u64, -1)?;
            cnt = self.put_cnt.load(Ordering::SeqCst);

            if prev_cnt >= cnt {
                break;
            }
        }

        Ok(())
    }

    pub(crate) fn wait_all_get(&self) -> Result<(), libfabric::error::Error> {
        let mut cnt = self.get_cnt.load(Ordering::SeqCst);

        loop {
            let prev_cnt = cnt;
            self.get_cntr.wait(prev_cnt as u64, -1)?;
            cnt = self.get_cnt.load(Ordering::SeqCst);

            if prev_cnt >= cnt {
                break;
            }
        }

        Ok(())
    }

    fn wait_for_tx_cntr(&self, target: usize) -> Result<(), libfabric::error::Error> {
        self.put_cntr.wait(target as u64, -1)
    }

    fn wait_for_rx_cntr(&self, target: usize) -> Result<(), libfabric::error::Error> {
        self.get_cntr.wait(target as u64, -1)
    }

    fn exchange_mr_info(
        &self,
        addr: usize,
        len: usize,
        key: &libfabric::mr::MemoryRegionKey,
        pes: &[usize],
    ) -> Result<Vec<libfabric::iovec::RmaIoVec>, libfabric::error::Error> {
        // println!("Exchaning mr info");
        let mc = self.create_mc_group(pes)?;
        let mut key = match key {
            libfabric::mr::MemoryRegionKey::Key(key) => *key,
            libfabric::mr::MemoryRegionKey::RawKey(_) => {
                panic!("Raw keys are not handled currently")
            }
        };

        // println!("PE {} sending : {}", self.my_pe, addr);
        let mut my_rma_iov = libfabric::iovec::RmaIoVec::new()
            .address(addr as u64)
            .len(len)
            .key(key);

        let mut all_rma_iovs = vec![libfabric::iovec::RmaIoVec::new(); pes.len()];

        let mut ctx = self.info_entry.allocate_context();

        let my_iov_bytes = unsafe {
            std::slice::from_raw_parts_mut(
                (&mut my_rma_iov) as *mut RmaIoVec as *mut u8,
                std::mem::size_of_val(&my_rma_iov),
            )
        };
        let all_iov_bytes = unsafe {
            std::slice::from_raw_parts_mut(
                all_rma_iovs.as_mut_ptr() as *mut u8,
                std::mem::size_of_val(&all_rma_iovs),
            )
        };

        self.ep.allgather_with_context(
            my_iov_bytes,
            &mut libfabric::mr::default_desc(),
            all_iov_bytes,
            &mut libfabric::mr::default_desc(),
            &mc,
            libfabric::enums::TferOptions::new(),
            &mut ctx,
        )?;

        self.wait_for_completion(&ctx)?;
        // println!("Recevied the following:");
        // for rma_iov in all_rma_iovs.iter() {
        //     println!("{}", rma_iov.get_address());
        // }
        // println!("Done Exchaning mr info");

        Ok(all_rma_iovs)
    }

    fn post_put(
        &self,
        mut fun: impl FnMut() -> Result<(), libfabric::error::Error>,
    ) -> Result<usize, libfabric::error::Error> {
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

        Ok(self.put_cnt.fetch_add(1, Ordering::SeqCst) + 1)
    }

    fn post_get(
        &self,
        mut fun: impl FnMut() -> Result<(), libfabric::error::Error>,
    ) -> Result<usize, libfabric::error::Error> {
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

        Ok(self.get_cnt.fetch_add(1, Ordering::SeqCst) + 1)
    }

    fn init_barrier(&mut self) -> Result<(), libfabric::error::Error> {
        let mut coll_attr = libfabric::comm::collective::CollectiveAttr::<()>::new();

        if self
            .domain
            .query_collective::<()>(libfabric::enums::CollectiveOp::Barrier, &mut coll_attr)
            .is_err()
        {
            let all_pes: Vec<_> = (0..self.num_pes).collect();
            let barrier_size = all_pes.len() * std::mem::size_of::<usize>();
            let barrier_addr = self.sub_alloc(&all_pes, barrier_size)?;

            self.barrier_impl = BarrierImpl::Manual(barrier_addr, AtomicUsize::new(0));
            Ok(())
        } else {
            let all_pes: Vec<_> = (0..self.num_pes).collect();
            self.barrier_impl = BarrierImpl::Collective(self.create_mc_group(&all_pes)?);
            Ok(())
        }
    }

    pub(crate) fn sub_alloc(
        &self,
        pes: &[usize],
        size: usize,
    ) -> Result<usize, libfabric::error::Error> {
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
        let mr = libfabric::mr::MemoryRegionBuilder::new(&mem, libfabric::enums::HmemIface::System)
            .requested_key(self.alloc_manager.next_key() as u64)
            .build(&self.domain)?;

        let mr = match mr {
            MaybeDisabledMemoryRegion::Disabled(mr) => {
                mr.bind_ep(&self.ep)?;
                mr.enable()?
            }
            MaybeDisabledMemoryRegion::Enabled(mr) => mr,
        };

        let rma_iovs = self.exchange_mr_info(mem.as_ptr() as usize, mem.len(), &mr.key()?, &pes)?;

        let remote_alloc_infos = pes
            .iter()
            .zip(rma_iovs)
            .map(|(pe, rma_iov)| {
                let mapped_key =
                    unsafe { libfabric::mr::MemoryRegionKey::from_u64(rma_iov.get_key()) }
                        .into_mapped(&self.domain)
                        .unwrap();
                (*pe, RemoteAllocInfo::from_rma_iov(rma_iov, mapped_key))
            })
            .collect();

        self.alloc_manager
            .insert(AllocInfo::new(mem, mr, remote_alloc_infos)?);

        Ok(mem_addr)
    }

    pub(crate) fn sub_barrier(&self, pes: &[usize]) -> Result<(), libfabric::error::Error> {
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
                            self.put(dst, std::slice::from_ref(&my_barrier), send_pe, false)?
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

    pub(crate) unsafe fn put<T: Remote>(
        &self,
        pe: usize,
        src_addr: &[T],
        dst_addr: usize,
        sync: bool,
    ) -> Result<(), libfabric::error::Error> {
        let (offset, mut desc, remote_alloc_info) = {
            let table = self.alloc_manager.mr_info_table.read();
            let alloc_info = table
                .iter()
                .find(|e| e.contains(&dst_addr))
                .expect("Invalid address");

            (
                alloc_info.start(),
                alloc_info.mr_desc(),
                alloc_info.remote_info(&pe).expect(&format!(
                    "PE {} is not part of the sub allocation group",
                    pe
                )),
            )
        };

        let mut remote_dst_addr = dst_addr - offset + remote_alloc_info.start();
        // println!(
        //     "Putting to PE {}, addr: {}, remote_addr: {}",
        //     pe, dst_addr, remote_dst_addr
        // );

        let remote_key = remote_alloc_info.key();
        let cntr_order =
            if std::mem::size_of_val(src_addr) < self.info_entry.tx_attr().inject_size() {
                self.post_put(|| unsafe {
                    self.ep.inject_write_to(
                        src_addr,
                        &self.mapped_addresses[pe],
                        remote_dst_addr as u64,
                        remote_key,
                    )
                })?
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
                            &mut desc,
                            &self.mapped_addresses[pe],
                            remote_dst_addr as u64,
                            remote_key,
                        )
                    })?;

                    remote_dst_addr += msg_len;
                    curr_idx += msg_len;
                    cntr_order = order;
                }

                cntr_order
            };

        if sync {
            self.wait_for_tx_cntr(cntr_order)?;
        }
        // println!("Done putting");
        Ok(())
    }

    pub(crate) unsafe fn get<T: Remote>(
        &self,
        pe: usize,
        src_addr: usize,
        dst_addr: &mut [T],
        sync: bool,
    ) -> Result<(), libfabric::error::Error> {
        let (offset, mut desc, remote_alloc_info) = {
            let table = self.alloc_manager.mr_info_table.read();
            let alloc_info = table
                .iter()
                .find(|e| e.contains(&src_addr))
                .expect("Invalid address");

            (
                alloc_info.start(),
                alloc_info.mr_desc(),
                alloc_info.remote_info(&pe).expect(&format!(
                    "PE {} is not part of the sub allocation group",
                    pe
                )),
            )
        };

        let mut remote_src_addr = src_addr - offset + remote_alloc_info.start();
        let remote_key = remote_alloc_info.key();
        // println!(
        //     "Getting from PE {}, addr: {}, remote_addr: {}",
        //     pe, src_addr, remote_src_addr
        // );

        let mut curr_idx = 0;

        let mut cntr_order = 0;
        while curr_idx < dst_addr.len() {
            let msg_len = std::cmp::min(
                dst_addr.len() - curr_idx,
                self.info_entry.ep_attr().max_msg_size(),
            );
            let order = self.post_get(|| unsafe {
                self.ep.read_from(
                    &mut dst_addr[curr_idx..curr_idx + msg_len],
                    &mut desc,
                    &self.mapped_addresses[pe],
                    remote_src_addr as u64,
                    remote_key,
                )
            })?;
            remote_src_addr += msg_len;
            curr_idx += msg_len;
            cntr_order = order;
        }

        if sync {
            self.wait_for_rx_cntr(cntr_order)?;
        }

        Ok(())
    }

    pub(crate) fn local_addr(&self, remote_pe: &usize, remote_addr: &usize) -> usize {
        self.alloc_manager
            .local_addr(remote_pe, remote_addr)
            .expect(&format!(
                "Local address not found from remote PE {}, remote addr: {}",
                remote_pe, remote_addr
            ))
    }

    pub(crate) fn remote_addr(&self, pe: &usize, local_addr: &usize) -> usize {
        self.alloc_manager
            .remote_addr(pe, local_addr)
            .expect(&format!("Remote address not found for PE {}", pe))
    }

    pub(crate) fn release(&self, addr: &usize) {
        self.alloc_manager.remove(addr);
    }
}

impl Drop for Ofi {
    fn drop(&mut self) {
        self.wait_all_get().unwrap();
        self.wait_all_put().unwrap();
    }
}

pub(crate) struct AllocInfoManager {
    pub(crate) mr_info_table: RwLock<Vec<AllocInfo>>,
    mr_next_key: AtomicUsize,
    page_size: usize,
}

impl AllocInfoManager {
    pub(crate) fn new() -> Self {
        Self {
            mr_info_table: RwLock::new(Vec::new()),
            mr_next_key: AtomicUsize::new(0),
            page_size: page_size::get(),
        }
    }

    pub(crate) fn insert(&self, alloc: AllocInfo) {
        self.mr_info_table.write().push(alloc)
    }

    pub(crate) fn remove(&self, mem_addr: &usize) {
        let mut table = self.mr_info_table.write();
        let idx = table
            .iter()
            .position(|e| e.start() == *mem_addr)
            .expect("Error! Invalid memory address");

        table.remove(idx);
    }

    pub(crate) fn local_addr(&self, remote_pe: &usize, remote_addr: &usize) -> Option<usize> {
        let table = self.mr_info_table.read();
        if let Some(alloc_info) = table
            .iter()
            .find(|x| x.remote_contains(remote_pe, remote_addr))
        {
            Some(remote_addr - alloc_info.remote_start(&remote_pe) + alloc_info.start())
        } else {
            None
        }
    }

    pub(crate) fn remote_addr(&self, remote_pe: &usize, local_addr: &usize) -> Option<usize> {
        let table = self.mr_info_table.read();
        if let Some(alloc_info) = table.iter().find(|x| x.contains(local_addr)) {
            if let Some(remote_alloc_info) = alloc_info.remote_allocs.get(remote_pe) {
                Some(local_addr - alloc_info.start() + remote_alloc_info.start())
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
pub(crate) struct RemoteAllocInfo {
    key: libfabric::mr::MappedMemoryRegionKey,
    range: std::ops::Range<usize>,
}

impl RemoteAllocInfo {
    pub(crate) fn from_rma_iov(
        rma_iov: libfabric::iovec::RmaIoVec,
        key: libfabric::mr::MappedMemoryRegionKey,
    ) -> Self {
        let start = rma_iov.get_address() as usize;
        let end = start + rma_iov.get_len();

        Self {
            key,
            range: std::ops::Range { start, end },
        }
    }

    #[allow(dead_code)]
    pub(crate) fn new(
        range: std::ops::Range<usize>,
        key: libfabric::mr::MappedMemoryRegionKey,
    ) -> Self {
        Self { key, range }
    }

    pub(crate) fn start(&self) -> usize {
        self.range.start
    }

    pub(crate) fn end(&self) -> usize {
        self.range.end
    }

    pub(crate) fn key(&self) -> &libfabric::mr::MappedMemoryRegionKey {
        &self.key
    }

    pub(crate) fn contains(&self, addr: &usize) -> bool {
        self.range.contains(addr)
    }
}

pub(crate) struct AllocInfo {
    _mem: memmap::MmapMut,
    mr: libfabric::mr::MemoryRegion,
    desc: libfabric::mr::MemoryRegionDesc,
    key: libfabric::mr::MemoryRegionKey,
    range: std::ops::Range<usize>,
    remote_allocs: HashMap<usize, RemoteAllocInfo>,
}

impl AllocInfo {
    pub(crate) fn new(
        mem: memmap::MmapMut,
        mr: libfabric::mr::MemoryRegion,
        remote_allocs: HashMap<usize, RemoteAllocInfo>,
    ) -> Result<Self, libfabric::error::Error> {
        let start = mem.as_ptr() as usize;
        let end = start + mem.len();
        let desc = mr.description();
        let key = mr.key()?;

        Ok(Self {
            _mem: mem,
            mr,
            desc,
            key,
            range: std::ops::Range { start, end },
            remote_allocs,
        })
    }

    pub(crate) fn start(&self) -> usize {
        self.range.start
    }

    #[allow(dead_code)]
    pub(crate) fn remote_start(&self, remote_id: &usize) -> usize {
        self.remote_allocs
            .get(remote_id)
            .expect(&format!(
                "PE {} is not part of the sub allocation group",
                remote_id
            ))
            .start()
    }

    #[allow(dead_code)]
    pub(crate) fn remote_end(&self, remote_id: &usize) -> usize {
        self.remote_allocs
            .get(remote_id)
            .expect(&format!(
                "PE {} is not part of the sub allocation group",
                remote_id
            ))
            .end()
    }

    #[allow(dead_code)]
    pub(crate) fn end(&self) -> usize {
        self.range.end
    }

    #[allow(dead_code)]
    pub(crate) fn key(&self) -> &libfabric::mr::MemoryRegionKey {
        &self.key
    }

    #[allow(dead_code)]
    pub(crate) fn remote_key(&self, remote_id: &usize) -> &libfabric::mr::MappedMemoryRegionKey {
        self.remote_allocs
            .get(remote_id)
            .expect(&format!(
                "PE {} is not part of the sub allocation group",
                remote_id
            ))
            .key()
    }

    pub(crate) fn contains(&self, addr: &usize) -> bool {
        self.range.contains(addr)
    }

    #[allow(dead_code)]
    pub(crate) fn remote_contains(&self, remote_id: &usize, addr: &usize) -> bool {
        self.remote_allocs
            .get(remote_id)
            .expect(&format!(
                "PE {} is not part of the sub allocation group",
                remote_id
            ))
            .contains(&addr)
    }

    pub(crate) fn remote_info(&self, remote_pe: &usize) -> Option<RemoteAllocInfo> {
        self.remote_allocs.get(remote_pe).cloned()
    }

    pub(crate) fn mr_desc(&self) -> libfabric::mr::MemoryRegionDesc {
        self.desc.clone()
    }

    #[allow(dead_code)]
    pub(crate) fn mr(&self) -> &libfabric::mr::MemoryRegion {
        &self.mr
    }
}

fn euclid_rem(a: i64, b: i64) -> usize {
    let r = a % b;

    if r >= 0 {
        r as usize
    } else {
        (r + b.abs()) as usize
    }
}

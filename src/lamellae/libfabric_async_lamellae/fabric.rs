use crate::lamellae::calc_alloc_padding_size_align;
use crate::lamellae::collective::AllReduceOp;
use crate::lamellae::collective::ReduceOp as LamellarReduceOp;
use crate::lamellae::collective::RootOrSliceMut;
use crate::lamellae::collective::RootSrcOrSliceMut;
use crate::lamellae::collective::RootSrcSliceOrNone;
use crate::lamellae::decode_padding;
use crate::lamellae::decode_ref_count;
use crate::lamellae::decode_ref_count_and_padding;
use crate::lamellae::decrement_ref_count;
use crate::lamellae::encode_ref_count_and_padding;
use crate::lamellae::get_ref_count;
use crate::lamellae::increment_ref_count;
use crate::lamellae::AllocError;
use crate::lamellae::AllocResult;
use crate::lamellae::AllocationType;
use crate::lamellae::AtomicOp as LamellarAtomicOp;
use crate::lamellae::CollectiveOpKind;
use crate::lamellae::CommAlloc;
use crate::lamellae::CommAllocAddr;
use crate::lamellae::CommAllocInner;
use crate::lamellae::FabricError;
use crate::lamellae::FabricResult;
use crate::lamellar_alloc::BTreeAlloc;
use crate::lamellar_alloc::LamellarAlloc;
use libfabric::async_::comm::atomic::AsyncAtomicCASEp;
use libfabric::async_::comm::atomic::AsyncAtomicFetchEp;
use libfabric::async_::comm::atomic::AsyncAtomicWriteEp;
use libfabric::async_::comm::collective::AsyncCollectiveEp;
use libfabric::async_::comm::rma::AsyncReadEp;
use libfabric::async_::comm::rma::AsyncWriteEp;
use libfabric::async_::connless_ep::ConnectionlessEndpoint;
use libfabric::async_::cq::AsyncWaitCq;
use libfabric::async_::cq::CompletionQueue;
use libfabric::async_::cq::CompletionQueueBuilder;
use libfabric::async_::ep::Endpoint;
use libfabric::async_::ep::EndpointBuilder;
use libfabric::async_::eq::EventQueue;
use libfabric::async_::eq::EventQueueBuilder;
use libfabric::av::AddressVector;
use libfabric::av::AddressVectorBuilder;
use libfabric::av::AvInAddress;
use libfabric::av_set::AddressVectorSetBuilder;
use libfabric::cntr::Counter;
use libfabric::cntr::CounterBuilder;
use libfabric::cntr::ReadCntr;
use libfabric::cntr::WaitCntr;
use libfabric::comm::atomic::AtomicFetchEp;
use libfabric::comm::atomic::AtomicValidEp;
use libfabric::comm::atomic::AtomicWriteEp;
use libfabric::comm::collective::CollectiveAttr;
use libfabric::comm::rma::WriteEp;
use libfabric::domain::Domain;
use libfabric::domain::DomainBuilder;
use libfabric::enums::AVOptions;
use libfabric::enums::AddressFormat;
use libfabric::enums::AtomicOp;
use libfabric::enums::CollectiveOp;
use libfabric::enums::CollectiveOptions;
use libfabric::enums::CompareAtomicOp;
use libfabric::enums::EndpointType;
use libfabric::enums::FetchAtomicOp;
use libfabric::enums::HmemIface;
use libfabric::enums::JoinOptions;
use libfabric::enums::Mode;
use libfabric::enums::MrMode;
use libfabric::enums::Progress;
use libfabric::enums::ResourceMgmt;
use libfabric::enums::TrafficClass;
use libfabric::enums::TransferOptions;
use libfabric::ep::Address;
use libfabric::ep::BaseEndpoint;
use libfabric::fabric::Fabric;
use libfabric::fabric::FabricBuilder;
use libfabric::info::libfabric_version;
use libfabric::info::Info;
use libfabric::info::InfoEntry;
use libfabric::infocapsoptions::InfoCaps;
use libfabric::mcast::MultiCastGroup;
use libfabric::mcast::MulticastGroupBuilder;
use libfabric::mr::DisabledMemoryRegion;
use libfabric::mr::MaybeDisabledMemoryRegion;
use libfabric::mr::MemoryRegion;
use libfabric::mr::MemoryRegionBuilder;
use libfabric::AsFiType;
use libfabric::CntrCaps;
use libfabric::FabInfoCaps;
use libfabric::MappedAddress;
use libfabric::MemAddressInfo;
use libfabric::RemoteMemAddressInfo;
use parking_lot::RwLock;
use pmi::pmi::Pmi;
use pmi::pmi::PmiBuilder;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{debug, trace};

enum BarrierImpl {
    Uninit,
    Collective(MultiCastGroup),
    Manual(usize, AtomicUsize),
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

type RmaAtomicCollEp =
    libfabric::info_caps_type!(FabInfoCaps::ATOMIC, FabInfoCaps::RMA, FabInfoCaps::COLL);
type SpinCq = libfabric::async_cq_caps_type!();
type SpinEq = libfabric::async_eq_caps_type!();
type WaitableCntr = libfabric::cntr_caps_type!(CntrCaps::WAIT);

pub(crate) struct OfiAsync {
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    mapped_addresses: Vec<MappedAddress>,
    barrier_impl: RwLock<BarrierImpl>,
    ep: ConnectionlessEndpoint<RmaAtomicCollEp>,
    cq: CompletionQueue<SpinCq>,
    put_cntr: Counter<WaitableCntr>,
    get_cntr: Counter<WaitableCntr>,
    av: AddressVector,
    _eq: EventQueue<SpinEq>,
    domain: Domain,
    _fabric: Fabric,
    info_entry: InfoEntry<RmaAtomicCollEp>,
    alloc_manager: Arc<AllocInfoManager>,
    _my_pmi: Arc<dyn Pmi>,
    put_cnt: AtomicU64,
    get_cnt: AtomicU64,
    completion_lock: RwLock<()>,
}

impl std::fmt::Debug for OfiAsync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OfiAsync")
            .field("num_pes", &self.num_pes)
            .field("my_pe", &self.my_pe)
            .finish()
    }
}

impl OfiAsync {
    pub(crate) fn new(provider: Option<&str>, domain: Option<&str>) -> FabricResult<Arc<Self>> {
        let my_pmi = Arc::new(PmiBuilder::init().map_err(|e| {
            eprintln!("Error initializing PMI: {:?}", e);
            FabricError::InitError(1)
        })?);

        // trace!("Using PMI my_pe {} num_pes {}",my_pmi.rank(),my_pmi.ranks().len());

        let info = Info::new(&libfabric_version())
            .enter_hints()
            .caps(InfoCaps::new().rma().atomic().collective())
            .mode(Mode::new().context())
            .enter_ep_attr()
            .type_(EndpointType::Rdm)
            .leave_ep_attr()
            .enter_domain_attr()
            // .threading(Threading::Safe) // different threading modes set by libfabric feature at compile time
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
        //trace!("Using provider: {:?}", info_entry);

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
            .build_with_shared_cq(&domain, &cq)
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
                let addr = my_pmi.get("epname", &r).unwrap();
                unsafe { Address::from_bytes(&addr) }
            })
            .collect();

        let mapped_addresses = av
            .insert(AvInAddress::Encoded(&unmapped_addresses), AVOptions::new())
            .map_err(|e| FabricError::InitError(e.c_err))?;
        let mapped_addresses: Vec<MappedAddress> =
            mapped_addresses.into_iter().map(|a| a.unwrap()).collect();
        let alloc_manager = AllocInfoManager::new();

        let ofi = Arc::new(Self {
            num_pes: my_pmi.ranks().len(),
            my_pe: my_pmi.rank(),
            _my_pmi: my_pmi,
            info_entry,
            _fabric: fabric,
            domain,
            av,
            _eq: eq,
            put_cntr,
            get_cntr,
            cq,
            ep,
            mapped_addresses,
            alloc_manager: Arc::new(alloc_manager),
            barrier_impl: RwLock::new(BarrierImpl::Uninit),
            put_cnt: AtomicU64::new(0),
            get_cnt: AtomicU64::new(0),
            completion_lock: RwLock::new(()),
        });

        ofi.init_barrier()?;

        Ok(ofi)
    }

    fn atomic_avail_inner<T: AsFiType>(&self) -> bool {
        unsafe {
            self.ep.atomicvalid::<T>(AtomicOp::Sum).is_ok()
                && self
                    .ep
                    .fetch_atomicvalid::<T>(FetchAtomicOp::AtomicRead)
                    .is_ok()
                && self
                    .ep
                    .compare_atomicvalid::<T>(CompareAtomicOp::Cswap)
                    .is_ok()
        }
    }

    fn atomic_op_avail_inner<T: AsFiType>(&self, op: AtomicOpKind) -> bool {
        unsafe {
            match op {
                AtomicOpKind::Min => {
                    self.ep.atomicvalid::<T>(AtomicOp::Min).is_ok()
                        && self.ep.fetch_atomicvalid::<T>(FetchAtomicOp::Min).is_ok()
                }
                AtomicOpKind::Max => {
                    self.ep.atomicvalid::<T>(AtomicOp::Max).is_ok()
                        && self.ep.fetch_atomicvalid::<T>(FetchAtomicOp::Max).is_ok()
                }
                AtomicOpKind::Sum => {
                    self.ep.atomicvalid::<T>(AtomicOp::Sum).is_ok()
                        && self.ep.fetch_atomicvalid::<T>(FetchAtomicOp::Sum).is_ok()
                }
                AtomicOpKind::Prod => {
                    self.ep.atomicvalid::<T>(AtomicOp::Prod).is_ok()
                        && self.ep.fetch_atomicvalid::<T>(FetchAtomicOp::Prod).is_ok()
                }
                AtomicOpKind::BitOr => {
                    self.ep.atomicvalid::<T>(AtomicOp::Bor).is_ok()
                        && self.ep.fetch_atomicvalid::<T>(FetchAtomicOp::Bor).is_ok()
                }
                AtomicOpKind::BitXor => {
                    self.ep.atomicvalid::<T>(AtomicOp::Bxor).is_ok()
                        && self.ep.fetch_atomicvalid::<T>(FetchAtomicOp::Bxor).is_ok()
                }
                AtomicOpKind::BitAnd => {
                    self.ep.atomicvalid::<T>(AtomicOp::Band).is_ok()
                        && self.ep.fetch_atomicvalid::<T>(FetchAtomicOp::Band).is_ok()
                }
                AtomicOpKind::Read => self
                    .ep
                    .fetch_atomicvalid::<T>(FetchAtomicOp::AtomicRead)
                    .is_ok(),
                AtomicOpKind::Write => {
                    self.ep.atomicvalid::<T>(AtomicOp::AtomicWrite).is_ok()
                        && self
                            .ep
                            .fetch_atomicvalid::<T>(FetchAtomicOp::AtomicWrite)
                            .is_ok()
                }
                AtomicOpKind::Cas => self
                    .ep
                    .compare_atomicvalid::<T>(CompareAtomicOp::Cswap)
                    .is_ok(),
            }
        }
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
            LamellarAtomicOp::Sum(_) => AtomicOpKind::Sum,
            LamellarAtomicOp::Sub(_) => AtomicOpKind::Sum, // Sub can be implemented as Add with negative value
            LamellarAtomicOp::Prod(_) => AtomicOpKind::Prod,
            LamellarAtomicOp::BitOr(_) => AtomicOpKind::BitOr,
            LamellarAtomicOp::BitXor(_) => AtomicOpKind::BitXor,
            LamellarAtomicOp::BitAnd(_) => AtomicOpKind::BitAnd,
            LamellarAtomicOp::Read(_) => AtomicOpKind::Read,
            LamellarAtomicOp::Write(_) => AtomicOpKind::Write,
            LamellarAtomicOp::Cas => AtomicOpKind::Cas,
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

    pub(crate) fn collective_avail<T: 'static>(&self, op: CollectiveOpKind) -> bool {
        match op {
            CollectiveOpKind::Barrier => self.barrier_avail(),
            CollectiveOpKind::AllToAll => self.collective_data_op_avail::<T>(CollectiveOp::AllToAll, None),
            CollectiveOpKind::Broadcast => self.collective_data_op_avail::<T>(CollectiveOp::Broadcast, None),
            CollectiveOpKind::AllGather => self.collective_data_op_avail::<T>(CollectiveOp::AllGather, None),
            CollectiveOpKind::Gather => self.collective_data_op_avail::<T>(CollectiveOp::Gather, None),
            CollectiveOpKind::AllReduce(reduce_op) => self.collective_data_op_avail::<T>(CollectiveOp::AllReduce, Some(reduce_op)),
            CollectiveOpKind::Reduce(reduce_op) => self.collective_data_op_avail::<T>(CollectiveOp::Reduce, Some(reduce_op)),
            CollectiveOpKind::ReduceScatter(reduce_op) => self.collective_data_op_avail::<T>(CollectiveOp::ReduceScatter, Some(reduce_op)),
            CollectiveOpKind::Scatter => self.collective_data_op_avail::<T>(CollectiveOp::Scatter, None),
        }
    }

    pub(crate) fn barrier_avail(&self) -> bool {
        self.domain
            .query_collective(CollectiveOp::Barrier, &mut CollectiveAttr::<()>::new())
            .is_ok()
    }

    fn collective_data_op_avail<T: 'static>(&self, data_op: CollectiveOp, reduce_op: Option<LamellarReduceOp>) -> bool {
        let id = std::any::TypeId::of::<T>();
        if id == std::any::TypeId::of::<u8>() {
            let mut attr = CollectiveAttr::<u8>::new();
            attr = if let Some(reduce_op) = reduce_op {
                attr.op((&reduce_op).into())
            }
            else {
                attr
            };
            self
                .domain.query_collective(data_op, &mut attr)
                .is_ok()
        }
        else if id == std::any::TypeId::of::<u16>() {
            let mut attr = CollectiveAttr::<u16>::new();
            attr = if let Some(reduce_op) = reduce_op {
                attr.op((&reduce_op).into())
            }
            else {
                attr
            };
            self
                .domain.query_collective(data_op, &mut attr)
                .is_ok()
        }
        else if id == std::any::TypeId::of::<u32>() {
            let mut attr = CollectiveAttr::<u32>::new();
            attr = if let Some(reduce_op) = reduce_op {
                attr.op((&reduce_op).into())
            }
            else {
                attr
            };
            self
                .domain.query_collective(data_op, &mut attr)
                .is_ok()
        }
        else if id == std::any::TypeId::of::<u64>() {
            let mut attr = CollectiveAttr::<u64>::new();
            attr = if let Some(reduce_op) = reduce_op {
                attr.op((&reduce_op).into())
            }
            else {
                attr
            };
            self
                .domain.query_collective(data_op, &mut attr)
                .is_ok()
        }
        else if id == std::any::TypeId::of::<u128>() {
            let mut attr = CollectiveAttr::<u128>::new();
            attr = if let Some(reduce_op) = reduce_op {
                attr.op((&reduce_op).into())
            }
            else {
                attr
            };
            self
                .domain.query_collective(data_op, &mut attr)
                .is_ok()
        }
        else if id == std::any::TypeId::of::<i8>() {
            let mut attr = CollectiveAttr::<i8>::new();
            attr = if let Some(reduce_op) = reduce_op {
                attr.op((&reduce_op).into())
            }
            else {
                attr
            };
            self
                .domain.query_collective(data_op, &mut attr)
                .is_ok()
        }
        else if id == std::any::TypeId::of::<i16>() {
            let mut attr = CollectiveAttr::<i16>::new();
            attr = if let Some(reduce_op) = reduce_op {
                attr.op((&reduce_op).into())
            }
            else {
                attr
            };
            self
                .domain.query_collective(data_op, &mut attr)
                .is_ok()
        }
        else if id == std::any::TypeId::of::<i32>() {
            let mut attr = CollectiveAttr::<i32>::new();
            attr = if let Some(reduce_op) = reduce_op {
                attr.op((&reduce_op).into())
            }
            else {
                attr
            };
            self
                .domain.query_collective(data_op, &mut attr)
                .is_ok()
        }
        else if id == std::any::TypeId::of::<i64>() {
            let mut attr = CollectiveAttr::<i64>::new();
            attr = if let Some(reduce_op) = reduce_op {
                attr.op((&reduce_op).into())
            }
            else {
                attr
            };
            self
                .domain.query_collective(data_op, &mut attr)
                .is_ok()
        }
        else if id == std::any::TypeId::of::<i128>() {
            let mut attr = CollectiveAttr::<i128>::new();
            attr = if let Some(reduce_op) = reduce_op {
                attr.op((&reduce_op).into())
            }
            else {
                attr
            };
            self
                .domain.query_collective(data_op, &mut attr)
                .is_ok()
        }
        else if id == std::any::TypeId::of::<usize>() {
            let mut attr = CollectiveAttr::<usize>::new();
            attr = if let Some(reduce_op) = reduce_op {
                attr.op((&reduce_op).into())
            }
            else {
                attr
            };
            self
                .domain.query_collective(data_op, &mut attr)
                .is_ok()
        }
        else if id == std::any::TypeId::of::<isize>() {
            let mut attr = CollectiveAttr::<isize>::new();
            attr = if let Some(reduce_op) = reduce_op {
                attr.op((&reduce_op).into())
            }
            else {
                attr
            };
            self
                .domain.query_collective(data_op, &mut attr)
                .is_ok()
        }
        else if id == std::any::TypeId::of::<f32>() {
            let mut attr = CollectiveAttr::<f32>::new();
            attr = if let Some(reduce_op) = reduce_op {
                attr.op((&reduce_op).into())
            }
            else {
                attr
            };
            self
                .domain.query_collective(data_op, &mut attr)
                .is_ok()
        }
        else if id == std::any::TypeId::of::<f64>() {
            let mut attr = CollectiveAttr::<f64>::new();
            attr = if let Some(reduce_op) = reduce_op {
                attr.op((&reduce_op).into())
            }
            else {
                attr
            };
            self
                .domain.query_collective(data_op, &mut attr)
                .is_ok()
        }
        else {
            false
        }
    }

    fn create_mc_group(&self, pes: &[usize]) -> Result<MultiCastGroup, libfabric::error::Error> {
        // trace!("Creating MC group of len: {}", pes.len());
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
        let mc = MulticastGroupBuilder::from_av_set(&av_set).build();
        let mc = async_std::task::block_on(async {
            mc.join_collective_async(&self.ep, JoinOptions::new(), &mut ctx)
                .await
        })?;
        // trace!("Done Creating MC group");

        Ok(mc.1)
    }

    pub(crate) fn progress(&self) -> Result<(), libfabric::error::Error> {
        self.cq.progress()
    }

    // fn wait_for_completion(&self, ctx: &Context) -> Result<(), libfabric::error::Error> {
    //     loop {
    //         let cq_res = self.cq.read(1);
    //         match cq_res {
    //             Ok(completion) => match completion {
    //                 Completion::Ctx(entries) | Completion::Unspec(entries) => {
    //                     if entries[0].is_op_context_equal(ctx) {
    //                         return Ok(());
    //                     }
    //                 }
    //                 Completion::Msg(entries) => {
    //                     if entries[0].is_op_context_equal(ctx) {
    //                         return Ok(());
    //                     }
    //                 }
    //                 Completion::Data(entries) => {
    //                     if entries[0].is_op_context_equal(ctx) {
    //                         return Ok(());
    //                     }
    //                 }
    //                 Completion::Tagged(entries) => {
    //                     if entries[0].is_op_context_equal(ctx) {
    //                         return Ok(());
    //                     }
    //                 }
    //             },
    //             Err(err) => {
    //                 if !matches!(err.kind, libfabric::error::ErrorKind::TryAgain) {
    //                     return Err(err);
    //                 }
    //             }
    //         }
    //     }
    // }

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
        let _guard = self.completion_lock.write();
        let mut prev_expected_cnt = pending.load(Ordering::SeqCst);
        let mut old_cnt = cntr.read();
        if prev_expected_cnt != old_cnt {
            // let _guard = self.completion_lock.read();
            let mut expected_cnt = pending.load(Ordering::SeqCst);
            // let mut prev_expected_cnt = expected_cnt;
            let mut cur_cnt = cntr.read();
            // let mut old_cnt = cur_cnt;
            // let  err_cnt = cntr.readerr();
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
            }
            trace!(
            "{dir} after.   expected_cnt {expected_cnt} prev_expected_cnt {prev_expected_cnt} cur_cnt {} old_cnt {old_cnt} ",
            cntr.read(),
        );
        }
        Ok(())
    }

    fn collective_exchange_mr_info(
        &self,
        pes: &[usize],
        mem: &[u8],
        mr: &MemoryRegion,
    ) -> Result<HashMap<usize, RemoteMemAddressInfo>, libfabric::error::Error> {
        let _guard = self.completion_lock.write();
        let mc = self.create_mc_group(&pes)?;

        let mut mem_info = MemAddressInfo::from_slice(mem, 0, &mr.key().unwrap(), &self.info_entry);

        let mut mem_info_bytes = mem_info.to_bytes_mut();
        let mut all_mem_info_bytes = vec![0u8; mem_info_bytes.len() * pes.len()];
        let mut ctx = self.info_entry.allocate_context();

        async_std::task::block_on(async {
            self.ep
                .allgather_async(
                    &mut mem_info_bytes,
                    None,
                    &mut all_mem_info_bytes,
                    None,
                    &mc,
                    CollectiveOptions::new(),
                    &mut ctx,
                )
                .await
        })?;

        // self.wait_for_completion(&ctx)?;

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

    fn post_put(
        &self,
        mut fun: impl FnMut() -> Result<(), libfabric::error::Error>,
    ) -> Result<u64, libfabric::error::Error> {
        // let _guard = self.completion_lock.read();
        let _guard = self.completion_lock.write();
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
        Ok(self.put_cnt.fetch_add(1, Ordering::SeqCst) + 1)
    }

    #[allow(dead_code)] // WIP: get path not yet implemented in libfabric-async
    fn post_get(
        &self,
        mut fun: impl FnMut() -> Result<(), libfabric::error::Error>,
    ) -> Result<u64, libfabric::error::Error> {
        // let _guard = self.completion_lock.read();
        let _guard = self.completion_lock.write();
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

    fn init_barrier(self: &Arc<OfiAsync>) -> FabricResult<()> {
        let mut coll_attr = CollectiveAttr::<()>::new();

        if self
            .domain
            .query_collective::<()>(CollectiveOp::Barrier, &mut coll_attr)
            .is_err()
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

            *self.barrier_impl.write() =
                BarrierImpl::Manual(barrier_addr.start(), AtomicUsize::new(0));
            Ok(())
        } else {
            let all_pes: Vec<_> = (0..self.num_pes).collect();
            let _guard = self.completion_lock.write();
            *self.barrier_impl.write() = BarrierImpl::Collective(
                self.create_mc_group(&all_pes)
                    .map_err(|e| FabricError::BarrierError(e.c_err))?,
            );
            Ok(())
        }
    }
    pub(crate) fn alloc(
        self: &Arc<OfiAsync>,
        size: usize,
        alloc: AllocationType,
        align: usize,
    ) -> AllocResult<LibfabricAsyncAlloc> {
        match alloc {
            AllocationType::Sub(pes) => self.sub_alloc(&pes, size, align),
            AllocationType::Global => self.full_alloc(size, align),
            _ => return Err(AllocError::UnexpectedAllocationType(alloc)),
        }
    }

    fn full_alloc(
        self: &Arc<OfiAsync>,
        data_size: usize,
        align: usize,
    ) -> AllocResult<LibfabricAsyncAlloc> {
        //add space for ref count and padding to align it
        let (padding, size, _align) = calc_alloc_padding_size_align(data_size, align);

        // Align to page boundaries
        let aligned_size = if (self.alloc_manager.page_size() - 1) & size != 0 {
            (size + self.alloc_manager.page_size()) & !(self.alloc_manager.page_size() - 1)
        } else {
            size
        };

        trace!(target: "libfabric", "Full Allocating aligned size: {} aligned", aligned_size);
        // println!("{:?}", std::backtrace::Backtrace::capture());

        // Map memory of aligned size
        let mut mem = memmap::MmapOptions::new()
            .len(aligned_size)
            .map_anon()
            .expect(&format!(
                "Error in allocating aligned memory of size: {} {}",
                aligned_size, size,
            ));

        // Initialize mapped memory to zeros
        mem.iter_mut().map(|x| *x = 0).count();

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
        let mcast_group = self.create_mc_group(&(0..self.num_pes).collect::<Vec<_>>())
            .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?;
        let alloc = LibfabricAsyncAlloc::new(
            self.clone(),
            Arc::new(mem),
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
        self: &Arc<OfiAsync>,
        pes: &[usize],
        data_size: usize,
        align: usize,
    ) -> AllocResult<LibfabricAsyncAlloc> {
        //add space for ref count and padding to align it
        let (padding, size, _align) = calc_alloc_padding_size_align(data_size, align);
        // Align to page boundaries
        let aligned_size = if (self.alloc_manager.page_size() - 1) & size != 0 {
            (size + self.alloc_manager.page_size()) & !(self.alloc_manager.page_size() - 1)
        } else {
            size
        };

        trace!(target: "libfabric", "Sub Allocating aligned size: {} aligned pes: {:?}", aligned_size, pes);

        // Map memory of aligned size
        let mut mem = memmap::MmapOptions::new()
            .len(aligned_size)
            .map_anon()
            .expect("Error in allocating aligned memory");

        // Initialize mapped memory to zeros
        mem.iter_mut().map(|x| *x = 0).count();

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
                        // trace!("Binding memory region to endpoint");
                        mr.enable(&self.ep)
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
            .collective_exchange_mr_info(pes, &mem, &mr)
            .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?;
        let mcast_group = self.create_mc_group(pes)
            .map_err(|e| AllocError::FabricAllocationError(e.c_err as i32))?;

        let alloc = LibfabricAsyncAlloc::new(
            self.clone(),
            Arc::new(mem),
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
    ) -> AllocResult<LibfabricAsyncAlloc> {
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
                let mut ctx = self.info_entry.allocate_context();
                let _guard = self.completion_lock.write();
                async_std::task::block_on(async { self.ep.barrier_async(mc, &mut ctx).await })?;
                // loop {
                //     let ret = self.ep.barrier_with_context(mc, &mut ctx);
                //     match &ret {
                //         Ok(_) => break,
                //         Err(err) => {
                //             if !matches!(err.kind, libfabric::error::ErrorKind::TryAgain) {
                //                 return ret;
                //             }
                //         }
                //     }
                // }
                // trace!("Done with barrier");
                Ok(())
            }
            BarrierImpl::Manual(barrier_addr, barrier_id) => {
                let n = 2;
                let pes = (0..self.num_pes).collect::<Vec<_>>();
                let num_pes = pes.len();
                let num_rounds = ((num_pes as f64).log2() / (n as f64).log2()).ceil();
                let my_barrier = barrier_id.fetch_add(1, Ordering::SeqCst);
                // let _guard = self.completion_lock.read();
                let _guard = self.completion_lock.write();
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

    pub(crate) unsafe fn inner_put<T: Copy>(
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
            while curr_idx < src_addr.len() {
                let msg_len = std::cmp::min(
                    src_addr.len() - curr_idx,
                    self.info_entry.ep_attr().max_msg_size(),
                );

                self.post_put(|| unsafe {
                    self.ep.write_to(
                        &src_addr[curr_idx..curr_idx + msg_len],
                        Some(mr.descriptor()),
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
        // trace!("Done putting");
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

    // pub(crate) fn release(&self, addr: &usize) {
    //     self.alloc_manager.remove(addr);
    // }
}

impl Drop for OfiAsync {
    fn drop(&mut self) {
        trace!(target: "drop", "drop OfiAsync");
        let _ = self.barrier();
        let _ = self.wait_for_tx_cntr();
        trace!("wait_all put done");
        let _ = self.wait_for_rx_cntr();
        trace!(target: "drop", "end drop OfiAsync");
    }
}

pub(crate) struct AllocInfoManager {
    pub(crate) mr_info_table: Arc<RwLock<Vec<LibfabricAsyncAlloc>>>,
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

    pub(crate) fn insert(&self, alloc: LibfabricAsyncAlloc) {
        self.mr_info_table.write().push(alloc);
    }

    pub(crate) fn clear(&self) {
        let mut table = self.mr_info_table.write();
        let allocs = table.drain(..).collect::<Vec<_>>();
        drop(table); // we do this because when the allocs are dropped, they may call back into the AllocInfoManager to remove themselves thus deadlocking
        for alloc in allocs {
            trace!("Clearing alloc: {:?}", alloc);
        }
    }

    pub(crate) fn remove_from_alloc(&self, mem_addr: &LibfabricAsyncAlloc) {
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
    ) -> AllocResult<LibfabricAsyncAlloc> {
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
            .clone()
            .sub_alloc(remote_offset, num_bytes)
            .expect("Failed to create one-sided allocation from remote PE and address");
        OneSidedLibfabricAsyncAlloc { alloc, remote_pe }.into()
    }

    pub(crate) fn local_alloc_and_offset_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
    ) -> Option<(CommAlloc, usize)> {
        trace!(
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

pub(crate) struct LibfabricAsyncAlloc {
    pub(crate) ofi: Arc<OfiAsync>,
    mem: Arc<memmap::MmapMut>,
    mr: MemoryRegion,
    range: std::ops::Range<usize>,
    remote_allocs: Arc<HashMap<usize, RemoteMemAddressInfo>>,
    fabric_ref_cnt_offset: usize,
    rt_ref_cnt_offset: usize,
    id: usize,
    alloc_table: AllocTable,
    mcast_group: Option<MultiCastGroup>,
}

impl std::fmt::Debug for LibfabricAsyncAlloc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fabric_ref_count = unsafe {
            (&*(self.mem.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize))
                .load(Ordering::SeqCst)
        };

        let mut temp = f.debug_struct("LibfabricAsyncAlloc");
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

impl Clone for LibfabricAsyncAlloc {
    fn clone(&self) -> Self {
        self.increment_fabric_ref_count();
        if let AllocTable::Runtime(_, _, _) = &self.alloc_table {
            self.increment_rt_ref_count();
        }
        get_ref_count(unsafe {
            &*(self.mem.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize)
        });
        trace!(target: "libfabric", "Cloned LibfabricAsyncAlloc: {:?}", self);
        Self {
            ofi: self.ofi.clone(),
            mem: self.mem.clone(),
            mr: self.mr.clone(),
            range: self.range.clone(),
            remote_allocs: self.remote_allocs.clone(),
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: self.rt_ref_cnt_offset,
            id: self.id,
            alloc_table: self.alloc_table.clone(),
            mcast_group: self.mcast_group.clone(),
        }
    }
}

impl From<LibfabricAsyncAlloc> for CommAlloc {
    fn from(alloc: LibfabricAsyncAlloc) -> Self {
        CommAlloc {
            inner_alloc: Arc::new(CommAllocInner::LibfabricAsyncAlloc(alloc)),
            // alloc_type: CommAllocType::Fabric,
        }
    }
}

static ALLOC_ID: AtomicUsize = AtomicUsize::new(0);
impl LibfabricAsyncAlloc {
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

        std::ptr::copy(bytes.as_ptr(), value.cast::<u8>(), num_bytes);
    }

    pub(crate) fn new(
        ofi: Arc<OfiAsync>,
        mem: Arc<memmap::MmapMut>,
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
            mr: mr.clone(),
            range: std::ops::Range { start, end },
            remote_allocs: Arc::new(remote_allocs),
            fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            id,
            alloc_table: AllocTable::Fabric(alloc_table),
            mcast_group,
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
            mr: self.mr.clone(),
            range: self.range.start + offset..self.range.start + offset + len,
            remote_allocs: Arc::new(remote_allocs),
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: self.rt_ref_cnt_offset, //keep the same ref count offset as the parent allocation if this is actually a rt alloc, it will be updated when converted to a rt_alloc
            id,
            alloc_table: self.alloc_table.clone(),
            mcast_group: self.mcast_group.clone(),
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
            mr: self.mr.clone(),
            range: self.range.start + offset..self.range.start + offset + data_bytes,
            remote_allocs: Arc::new(remote_allocs),
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            id,
            alloc_table: AllocTable::Runtime(alloc_table, self.range.start + offset, alloc_manager),
            mcast_group: self.mcast_group.clone(),
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
            mr: self.mr.clone(),
            range: self.range.start..self.range.end - padding - std::mem::size_of::<AtomicUsize>(),
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            remote_allocs: self.remote_allocs.clone(),
            id: self.id,
            alloc_table: AllocTable::Runtime(alloc_table, self.range.start, alloc_manager),
            mcast_group: self.mcast_group.clone(),
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
                self.increment_rt_ref_count(); //increment the ref count to account for the leaked instance
                debug!(target: "libfabric", "Leaking Libfabric rt-allocation: {:?}", self);
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

    pub(crate) unsafe fn inner_put_unmanaged<T: Copy>(
        &self,
        pe: usize,
        offset: usize, //T-sized offset
        src_addr: &[T],
        sync: bool,
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
        if std::mem::size_of_val(src_addr) < self.ofi.info_entry.tx_attr().inject_size() {
            trace!(
                "Injecting write to PE {} at address {:?}",
                pe,
                remote_dst_addr.as_ptr()
            );
            self.ofi.post_put(|| unsafe {
                self.ofi.ep.inject_write_to(
                    src_addr,
                    &self.ofi.mapped_addresses[pe],
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

                self.ofi
                    .post_put(|| unsafe {
                        self.ofi.ep.write_to(
                            &src_addr[curr_idx..curr_idx + msg_len],
                            Some(self.mr.descriptor()),
                            &self.ofi.mapped_addresses[pe],
                            remote_dst_addr,
                            &remote_key,
                        )
                    })
                    .expect("Error posting put");

                remote_dst_addr = remote_dst_addr.add(msg_len * std::mem::size_of::<T>());
                curr_idx += msg_len;
            }
        }

        if sync {
            self.ofi.wait_for_tx_cntr()?;
        }
        // trace!("Done putting");
        Ok(())
    }

    pub(crate) async unsafe fn inner_put<T: Copy>(
        &self,
        pe: usize,
        offset: usize, //T-sized offset
        src_addr: &[T],
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
        if std::mem::size_of_val(src_addr) < self.ofi.info_entry.tx_attr().inject_size() {
            // trace!(
            //     "Injecting write to PE {} at address {:?}",
            //     pe,
            //     remote_dst_addr.as_ptr()
            // );
            // self.ofi.post_put(|| unsafe {
            self.ofi
                .ep
                .inject_write_to_async(
                    src_addr,
                    &self.ofi.mapped_addresses[pe],
                    remote_dst_addr,
                    &remote_key,
                )
                .await?;
            // })?;
        } else {
            let mut curr_idx = 0;
            while curr_idx < src_addr.len() {
                let msg_len = std::cmp::min(
                    src_addr.len() - curr_idx,
                    self.ofi.info_entry.ep_attr().max_msg_size() / std::mem::size_of::<T>(),
                );
                let mut ctx = self.ofi.info_entry.allocate_context();

                // self.ofi
                //     .post_put(|| unsafe {
                self.ofi
                    .ep
                    .write_to_async(
                        &src_addr[curr_idx..curr_idx + msg_len],
                        Some(self.mr.descriptor()),
                        &self.ofi.mapped_addresses[pe],
                        remote_dst_addr,
                        &remote_key,
                        &mut ctx,
                    )
                    .await
                    // })
                    .expect("Error posting put");

                remote_dst_addr = remote_dst_addr.add(msg_len * std::mem::size_of::<T>());
                curr_idx += msg_len;
            }
        }

        // trace!("Done putting");
        Ok(())
    }

    pub(crate) async unsafe fn inner_get_unmanaged<T: Copy>(
        &self,
        pe: usize,
        offset: usize,
        dst_addr: &mut [T],
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
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));

        let mut remote_src_addr = remote_alloc_info.mem_address().add(offset);
        debug!(
            "Inner Get: Remote destination address for PE {}: base_addr {:?} offset<T> {} size_of<T> {} len {} {:?}-{:?} {} bytes",
            pe,
            remote_alloc_info.mem_address(),
            offset,
            std::mem::size_of::<T>(),
            dst_addr.len(),
            remote_src_addr,
            remote_src_addr.add(std::mem::size_of_val(dst_addr)),
            std::mem::size_of_val(dst_addr)
        );
        let remote_key = remote_alloc_info.key();

        let mut curr_idx = 0;

        while curr_idx < dst_addr.len() {
            let msg_len = std::cmp::min(
                dst_addr.len() - curr_idx,
                self.ofi.info_entry.ep_attr().max_msg_size() / std::mem::size_of::<T>(),
            );
            let mut ctx = self.ofi.info_entry.allocate_context();
            async_std::task::block_on(async {
                self.ofi.ep.read_from_async(
                    &mut dst_addr[curr_idx..curr_idx + msg_len],
                    Some(self.mr.descriptor()),
                    &self.ofi.mapped_addresses[pe],
                    remote_src_addr,
                    &remote_key,
                    &mut ctx,
                ).await
                .expect("Error posting get");
            });
            remote_src_addr = remote_src_addr.add(msg_len * std::mem::size_of::<T>());
            curr_idx += msg_len;
        }

        Ok(())
    }

    pub(crate) async unsafe fn inner_get<T: Copy>(
        &self,
        pe: usize,
        offset: usize,
        dst_addr: &mut [T],
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
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));

        let mut remote_src_addr = remote_alloc_info.mem_address().add(offset);
        debug!(
            "Inner Get: Remote destination address for PE {}: base_addr {:?} offset<T> {} size_of<T> {} len {} {:?}-{:?} {} bytes",
            pe,
            remote_alloc_info.mem_address(),
            offset,
            std::mem::size_of::<T>(),
            dst_addr.len(),
            remote_src_addr,
            remote_src_addr.add(std::mem::size_of_val(dst_addr)),
            std::mem::size_of_val(dst_addr)
        );
        let remote_key = remote_alloc_info.key();

        let mut curr_idx = 0;

        while curr_idx < dst_addr.len() {
            let mut ctx = self.ofi.info_entry.allocate_context();
            let msg_len = std::cmp::min(
                dst_addr.len() - curr_idx,
                self.ofi.info_entry.ep_attr().max_msg_size() / std::mem::size_of::<T>(),
            );
            // self.ofi
            //     .post_get(|| unsafe {
            trace!(
                "GET: from PE {} at addr {:?} to local addr {:?} len {}",
                pe,
                remote_src_addr,
                &mut dst_addr[curr_idx..curr_idx + msg_len] as *mut [T],
                msg_len * std::mem::size_of::<T>()
            );
            self.ofi
                .ep
                .read_from_async(
                    &mut dst_addr[curr_idx..curr_idx + msg_len],
                    Some(self.mr.descriptor()),
                    &self.ofi.mapped_addresses[pe],
                    remote_src_addr,
                    &remote_key,
                    &mut ctx,
                )
                .await
                // })
                .expect("Error posting get");
            remote_src_addr = remote_src_addr.add(msg_len * std::mem::size_of::<T>());
            curr_idx += msg_len;
        }

        Ok(())
    }

    #[allow(dead_code)] // WIP: small-get optimization path not yet wired up
    pub(crate) async unsafe fn inner_get_small<T: Copy>(
        &self,
        pe: usize,
        offset: usize,
        dst_addr: &mut [T],
    ) -> Result<(), libfabric::error::Error> {
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

        let remote_src_addr = remote_alloc_info.mem_address().add(offset);
        let remote_key = remote_alloc_info.key();
        let mut ctx = self.ofi.info_entry.allocate_context();

        self.ofi.ep.read_from_async(
            dst_addr,
            Some(self.mr.descriptor()),
            &self.ofi.mapped_addresses[pe],
            remote_src_addr,
            &remote_key,
            &mut ctx,
        ).await?;

        Ok(())
    }

    pub(crate) async fn atomic_op_inner<T: 'static>(
        &self,
        pe: usize,
        offset: usize,
        op: &mut LamellarAtomicOp<T>,
    ) -> Result<(), libfabric::error::Error> {
        // if pe == self.ofi.my_pe {
        //     let offset_bytes = offset * std::mem::size_of::<T>();
        //     let addr = CommAllocAddr(self.start() + offset_bytes);
        //     crate::lamellae::comm::atomic::net_atomic_op(op, &addr);
        //     return Ok(());
        // }
        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_atomic_op::<T, u8>(pe, offset, op).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_atomic_op::<T, u16>(pe, offset, op).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_atomic_op::<T, u32>(pe, offset, op).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_atomic_op::<T, u64>(pe, offset, op).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_atomic_op::<T, usize>(pe, offset, op).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_atomic_op::<T, i8>(pe, offset, op).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_atomic_op::<T, i16>(pe, offset, op).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_atomic_op::<T, i32>(pe, offset, op).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_atomic_op::<T, i64>(pe, offset, op).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_atomic_op::<T, isize>(pe, offset, op).await
            } else {
                panic!("Unsupported atomic operation type");
            }
        }
    }

    pub(crate) fn atomic_op_inner_unmanaged<T: 'static>(
        &self,
        pe: usize,
        offset: usize,
        op: &mut LamellarAtomicOp<T>,
    ) -> Result<(), libfabric::error::Error> {
        // if pe == self.ofi.my_pe {
        //     let offset_bytes = offset * std::mem::size_of::<T>();
        //     let addr = CommAllocAddr(self.start() + offset_bytes);
        //     crate::lamellae::comm::atomic::net_atomic_op(op, &addr);
        //     return Ok(());
        // }
        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_atomic_op_unmanaged::<T, u8>(pe, offset, op)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_atomic_op_unmanaged::<T, u16>(pe, offset, op)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_atomic_op_unmanaged::<T, u32>(pe, offset, op)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_atomic_op_unmanaged::<T, u64>(pe, offset, op)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_atomic_op_unmanaged::<T, usize>(pe, offset, op)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_atomic_op_unmanaged::<T, i8>(pe, offset, op)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_atomic_op_unmanaged::<T, i16>(pe, offset, op)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_atomic_op_unmanaged::<T, i32>(pe, offset, op)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_atomic_op_unmanaged::<T, i64>(pe, offset, op)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_atomic_op_unmanaged::<T, isize>(pe, offset, op)
            } else {
                panic!("Unsupported atomic operation type");
            }
        }
    }

    unsafe fn typed_atomic_op_unmanaged<T, OFI: AsFiType>(
        &self,
        pe: usize,
        offset: usize,
        op: &mut LamellarAtomicOp<T>,
    ) -> Result<(), libfabric::error::Error> {
        let offset = offset * std::mem::size_of::<T>(); //we allocate memoryregions from libfabric as u8;
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes()); //we use num_bytes instead of mem.len() to allow for sub-allocations, offset + 1 because atomics operate on a single element and we verifying we arent missaligned
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));
        let remote_dst_addr =
            unsafe { remote_alloc_info.mem_address().as_type::<OFI>().add(offset) };
        let remote_key = remote_alloc_info.key();

        match op {
            LamellarAtomicOp::Sub(src) => {
                Self::negate_atomic_value(src.as_mut().get_unchecked_mut() as *mut T as *mut OFI);
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
            LamellarAtomicOp::Cas => {
                panic!("Compare atomic ops must use the compare path")
            }
            _ => {}
        };
        let src = op.src() as *const OFI;
        let buf = std::slice::from_raw_parts(src, 1);
        self.ofi.post_put(|| {
            self.ofi.ep.atomic_inject_to(
                buf,
                &self.ofi.mapped_addresses[pe],
                remote_dst_addr,
                &remote_key,
                op.into(),
            )
        })?;
        Ok(())
    }

    async unsafe fn typed_atomic_op<T, OFI: AsFiType>(
        &self,
        pe: usize,
        offset: usize,
        op: &mut LamellarAtomicOp<T>,
    ) -> Result<(), libfabric::error::Error> {
        let offset = offset * std::mem::size_of::<T>(); //we allocate memoryregions from libfabric as u8;
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes()); //we use num_bytes instead of mem.len() to allow for sub-allocations, offset + 1 because atomics operate on a single element and we verifying we arent missaligned
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));
        let remote_dst_addr =
            unsafe { remote_alloc_info.mem_address().as_type::<OFI>().add(offset) };
        let remote_key = remote_alloc_info.key();

        match op {
            LamellarAtomicOp::Sub(src) => {
                Self::negate_atomic_value(src.as_mut().get_unchecked_mut() as *mut T as *mut OFI)
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
            LamellarAtomicOp::Cas => {
                panic!("Compare atomic ops must use the compare path")
            }
            _ => {}
        };
        let src = op.src() as *const OFI;
        let buf = std::slice::from_raw_parts(src, 1);
        // self.ofi.post_put(|| {
        self.ofi
            .ep
            .atomic_inject_to_async(
                buf,
                &self.ofi.mapped_addresses[pe],
                remote_dst_addr,
                &remote_key,
                op.into(),
            )
            .await?;
        // })?;
        Ok(())
    }

    pub(crate) async fn atomic_fetch_op_inner<T: 'static>(
        &self,
        pe: usize,
        offset: usize,
        op: &mut LamellarAtomicOp<T>,
        result: &mut [T],
    ) -> Result<(), libfabric::error::Error> {
        // if pe == self.ofi.my_pe {
        //     let offset_bytes = offset * std::mem::size_of::<T>();
        //     let addr = CommAllocAddr(self.start() + offset_bytes);
        //     crate::lamellae::comm::atomic::net_atomic_fetch_op(op, &addr, result.as_mut_ptr());
        //     return Ok(());
        // }
        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_atomic_fetch_op::<T, u8>(pe, offset, op, result)
                    .await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_atomic_fetch_op::<T, u16>(pe, offset, op, result)
                    .await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_atomic_fetch_op::<T, u32>(pe, offset, op, result)
                    .await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_atomic_fetch_op::<T, u64>(pe, offset, op, result)
                    .await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_atomic_fetch_op::<T, usize>(pe, offset, op, result)
                    .await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_atomic_fetch_op::<T, i8>(pe, offset, op, result)
                    .await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_atomic_fetch_op::<T, i16>(pe, offset, op, result)
                    .await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_atomic_fetch_op::<T, i32>(pe, offset, op, result)
                    .await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_atomic_fetch_op::<T, i64>(pe, offset, op, result)
                    .await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_atomic_fetch_op::<T, isize>(pe, offset, op, result)
                    .await
            } else {
                panic!("Unsupported atomic operation type");
            }
        }
    }

    pub(crate) async fn atomic_compare_exchange_op_inner<T: 'static + Copy>(
        &self,
        pe: usize,
        offset: usize,
        current: &T,
        new: &T,
        result: &mut [T],
    ) -> Result<(), libfabric::error::Error> {
        // if pe == self.ofi.my_pe {
        //     let offset_bytes = offset * std::mem::size_of::<T>();
        //     let addr = CommAllocAddr(self.start() + offset_bytes);
        //     result[0] = match crate::lamellae::comm::atomic::net_atomic_compare_exchange(
        //         *current,
        //         *new,
        //         &addr,
        //     ) {
        //         Ok(old) | Err(old) => old,
        //     };
        //     return Ok(());
        // }
        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_atomic_compare_exchange_op::<T, u8>(pe, offset, current, new, result)
                    .await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_atomic_compare_exchange_op::<T, u16>(pe, offset, current, new, result)
                    .await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_atomic_compare_exchange_op::<T, u32>(pe, offset, current, new, result)
                    .await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_atomic_compare_exchange_op::<T, u64>(pe, offset, current, new, result)
                    .await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_atomic_compare_exchange_op::<T, usize>(pe, offset, current, new, result)
                    .await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_atomic_compare_exchange_op::<T, i8>(pe, offset, current, new, result)
                    .await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_atomic_compare_exchange_op::<T, i16>(pe, offset, current, new, result)
                    .await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_atomic_compare_exchange_op::<T, i32>(pe, offset, current, new, result)
                    .await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_atomic_compare_exchange_op::<T, i64>(pe, offset, current, new, result)
                    .await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_atomic_compare_exchange_op::<T, isize>(pe, offset, current, new, result)
                    .await
            } else {
                panic!("Unsupported atomic operation type");
            }
        }
    }

    #[allow(dead_code)] // WIP: unmanaged atomic path not yet exposed
    pub(crate) fn atomic_fetch_op_inner_unmanaged<T: 'static>(
        &self,
        pe: usize,
        offset: usize,
        op: &mut LamellarAtomicOp<T>,
        result: &mut [T],
    ) -> Result<(), libfabric::error::Error> {
        // if pe == self.ofi.my_pe {
        //     let offset_bytes = offset * std::mem::size_of::<T>();
        //     let addr = CommAllocAddr(self.start() + offset_bytes);
        //     crate::lamellae::comm::atomic::net_atomic_fetch_op(op, &addr, result.as_mut_ptr());
        //     return Ok(());
        // }
        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_atomic_fetch_op_unmanaged::<T, u8>(pe, offset, op, result)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_atomic_fetch_op_unmanaged::<T, u16>(pe, offset, op, result)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_atomic_fetch_op_unmanaged::<T, u32>(pe, offset, op, result)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_atomic_fetch_op_unmanaged::<T, u64>(pe, offset, op, result)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_atomic_fetch_op_unmanaged::<T, usize>(pe, offset, op, result)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_atomic_fetch_op_unmanaged::<T, i8>(pe, offset, op, result)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_atomic_fetch_op_unmanaged::<T, i16>(pe, offset, op, result)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_atomic_fetch_op_unmanaged::<T, i32>(pe, offset, op, result)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_atomic_fetch_op_unmanaged::<T, i64>(pe, offset, op, result)
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_atomic_fetch_op_unmanaged::<T, isize>(pe, offset, op, result)
            } else {
                panic!("Unsupported atomic operation type");
            }
        }
    }

    unsafe fn typed_atomic_fetch_op_unmanaged<T, OFI: AsFiType>(
        &self,
        pe: usize,
        offset: usize,
        op: &mut LamellarAtomicOp<T>,
        result: &mut [T],
    ) -> Result<(), libfabric::error::Error> {
        let offset = offset * std::mem::size_of::<T>(); //we allocate memoryregions from libfabric as u8;
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes()); //we use num_bytes instead of mem.len() to allow for sub-allocations, offset + 1 because atomics operate on a single element and we verifying we arent missaligned
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));
        let remote_dst_addr = unsafe { remote_alloc_info.mem_address().add(offset) };
        let remote_key = remote_alloc_info.key();

        let res = std::mem::transmute::<&mut [T], &mut [OFI]>(result);
        match op {
            LamellarAtomicOp::FetchSub(src) => {
                Self::negate_atomic_value(src.as_mut().get_unchecked_mut() as *mut T as *mut OFI)
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
            LamellarAtomicOp::Cas => {
                panic!("Compare atomic ops must use the compare path")
            }
            _ => {}
        };
        let src = op.src() as *const OFI;
        let buf = std::slice::from_raw_parts(src, 1);
        self.ofi.post_get(|| {
            self.ofi.ep.fetch_atomic_from(
                buf,
                None,
                res,
                None,
                &self.ofi.mapped_addresses[pe],
                remote_dst_addr,
                &remote_key,
                op.into(),
            )
        })?;
        Ok(())
    }

    async unsafe fn typed_atomic_fetch_op<T, OFI: AsFiType>(
        &self,
        pe: usize,
        offset: usize,
        op: &mut LamellarAtomicOp<T>,
        result: &mut [T],
    ) -> Result<(), libfabric::error::Error> {
        let offset = offset * std::mem::size_of::<T>(); //we allocate memoryregions from libfabric as u8;
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes()); //we use num_bytes instead of mem.len() to allow for sub-allocations, offset + 1 because atomics operate on a single element and we verifying we arent missaligned
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));
        let remote_dst_addr = unsafe { remote_alloc_info.mem_address().add(offset) };
        let remote_key = remote_alloc_info.key();

        let res = std::mem::transmute::<&mut [T], &mut [OFI]>(result);
        match op {
            LamellarAtomicOp::FetchSub(src) => {
                Self::negate_atomic_value(src.as_mut().get_unchecked_mut() as *mut T as *mut OFI)
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
            LamellarAtomicOp::Cas => {
                panic!("Compare atomic ops must use the compare path")
            }
            _ => {}
        };

        let src = op.src() as *const OFI;
        let buf = std::slice::from_raw_parts(src, 1);
        let mut ctx = self.ofi.info_entry.allocate_context();
        self.ofi
            .ep
            .fetch_atomic_from_async(
                buf,
                None,
                res,
                None,
                &self.ofi.mapped_addresses[pe],
                remote_dst_addr,
                &remote_key,
                &mut ctx,
                op.into(),
            )
            .await?;

        Ok(())
    }

    async unsafe fn typed_atomic_compare_exchange_op<T, OFI: AsFiType>(
        &self,
        pe: usize,
        offset: usize,
        current: &T,
        new: &T,
        result: &mut [T],
    ) -> Result<(), libfabric::error::Error> {
        let offset = offset * std::mem::size_of::<T>();
        assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let remote_alloc_info = self.remote_allocs.get(&pe).expect(&format!(
            "PE {} is not part of the sub allocation group",
            pe
        ));
        let remote_dst_addr = remote_alloc_info.mem_address().add(offset);
        let remote_key = remote_alloc_info.key();

        let new_slice = std::slice::from_raw_parts(new as *const T as *const OFI, 1);
        let current_slice = std::slice::from_raw_parts(current as *const T as *const OFI, 1);
        let res = std::mem::transmute::<&mut [T], &mut [OFI]>(result);
        let mut ctx = self.ofi.info_entry.allocate_context();

        self.ofi
            .ep
            .compare_atomic_swap_to_async(
                new_slice,
                None,
                current_slice,
                None,
                res,
                None,
                &self.ofi.mapped_addresses[pe],
                remote_dst_addr,
                &remote_key,
                &mut ctx,
            )
            .await?;

        Ok(())
    }

    pub(crate) async fn allreduce_inplace_inner<T: 'static>(        
        &self,
        op: &AllReduceOp,
        src_and_result: &mut [T],
    ) -> Result<(), libfabric::error::Error> {
        let dst = unsafe {std::slice::from_raw_parts_mut(src_and_result.as_mut_ptr(), src_and_result.len())};
        self.allreduce_inner(op, src_and_result, dst).await
    }

    pub(crate) async fn allreduce_inner<T: 'static>(
        &self,
        op: &AllReduceOp,
        src: &[T],
        result: &mut [T],
    ) -> Result<(), libfabric::error::Error> {
        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_allreduce::<T, u8>(op, src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_allreduce::<T, u16>(op, src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_allreduce::<T, u32>(op, src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_allreduce::<T, u64>(op, src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_allreduce::<T, usize>(op, src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_allreduce::<T, i8>(op, src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_allreduce::<T, i16>(op, src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_allreduce::<T, i32>(op, src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_allreduce::<T, i64>(op, src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_allreduce::<T, isize>(op, src, result).await
            } else {
                panic!("Unsupported allreduce operation type");
            }
        }
    }

    async unsafe fn typed_allreduce<T, OFI: AsFiType>(
        &self,
        op: &AllReduceOp,
        src: &[T],
        result: &mut [T],
    ) -> Result<(), libfabric::error::Error> {
        let res = unsafe {&mut *(result as *mut [T] as *mut [OFI])};
        let buf = unsafe { std::mem::transmute::<&[T], &[OFI]>(src) };
        let mc = self.mcast_group.as_ref().expect("No multicast group for allreduce");
        let mut ctx = self.ofi.info_entry.allocate_context();
        self.ofi.ep.allreduce_async(
            buf,
            None,
            res,
            None,
            mc,
            op.into(),
            CollectiveOptions::default(),
            &mut ctx,
        ).await?;

        Ok(())
    }
    // pub(crate) fn reduce_inplace_inner<T: 'static>(        
    //     &self,
    //     op: &LamellarReduceOp,
    //     root_pe: Option<usize>,
    //     blocking: bool,
    // ) -> Result<(), libfabric::error::Error> {
    //     let dst = unsafe {std::slice::from_raw_parts_mut(self.start() as *mut T, self.num_bytes()/std::mem::size_of::<T>())};
    //     let slice_or_pe = if let Some(root) = root_pe {
    //         RootOrSliceMut::NotRoot(root)
    //     }
    //     else {
    //         RootOrSliceMut::Root(dst)
    //     };

        
    //     self.reduce_inner(op, slice_or_pe, blocking)
    // }

    pub(crate) async fn allgather_inner<T: 'static>(
        &self,
        src: &[T],
        result: &mut [T],
    ) -> Result<(), libfabric::error::Error> {
        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_allgather::<T, u8>(src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_allgather::<T, u16>(src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_allgather::<T, u32>(src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_allgather::<T, u64>(src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_allgather::<T, usize>(src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_allgather::<T, i8>(src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_allgather::<T, i16>(src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_allgather::<T, i32>(src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_allgather::<T, i64>(src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_allgather::<T, isize>(src, result).await
            } else {
                panic!("Unsupported allgather operation type");
            }
        }
    }

    async unsafe fn typed_allgather<T, OFI: AsFiType>(
        &self,
        src: &[T],
        result: &mut [T],
    ) -> Result<(), libfabric::error::Error> {
        let res = unsafe {&mut *(result as *mut [T] as *mut [OFI])};
        let buf = unsafe { std::mem::transmute::<&[T], &[OFI]>(src) };
        let mc = self.mcast_group.as_ref().expect("No multicast group for allgather");
        let mut ctx = self.ofi.info_entry.allocate_context();
        self.ofi.ep.allgather_async(
            buf,
            None,
            res,
            None,
            mc,
            CollectiveOptions::default(),
            &mut ctx,
        ).await?;

        Ok(())
    }

    pub(crate) async fn alltoall_inner<T: 'static>(
        &self,
        src: &[T],
        result: &mut [T],
    ) -> Result<(), libfabric::error::Error> {
        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_alltoall::<T, u8>(src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_alltoall::<T, u16>(src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_alltoall::<T, u32>(src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_alltoall::<T, u64>(src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_alltoall::<T, usize>(src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_alltoall::<T, i8>(src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_alltoall::<T, i16>(src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_alltoall::<T, i32>(src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_alltoall::<T, i64>(src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_alltoall::<T, isize>(src, result).await
            } else {
                panic!("Unsupported alltoall operation type");
            }
        }
    }

    async unsafe fn typed_alltoall<T, OFI: AsFiType>(
        &self,
        src: &[T],
        result: &mut [T],
    ) -> Result<(), libfabric::error::Error> {
        let res = unsafe {&mut *(result as *mut [T] as *mut [OFI])};
        let buf = unsafe { std::mem::transmute::<&[T], &[OFI]>(src) };
        let mc = self.mcast_group.as_ref().expect("No multicast group for alltoall");
        let mut ctx = self.ofi.info_entry.allocate_context();
        self.ofi.ep.alltoall_async(
            buf,
            None,
            res,
            None,
            mc,
            CollectiveOptions::default(),
            &mut ctx,
        ).await?;

        Ok(())
    }

    pub(crate) async fn reduce_inner<T: 'static>(
        &self,
        op: &LamellarReduceOp,
        src: &[T],
        slice_or_pe: RootOrSliceMut<'_, T>,
    ) -> Result<(), libfabric::error::Error> {

        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_reduce::<T, u8>(op, src, slice_or_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_reduce::<T, u16>(op, src, slice_or_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_reduce::<T, u32>(op, src, slice_or_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_reduce::<T, u64>(op, src, slice_or_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_reduce::<T, usize>(op, src, slice_or_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_reduce::<T, i8>(op, src, slice_or_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_reduce::<T, i16>(op, src, slice_or_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_reduce::<T, i32>(op, src, slice_or_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_reduce::<T, i64>(op, src, slice_or_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_reduce::<T, isize>(op, src, slice_or_pe).await
            } else {
                panic!("Unsupported allreduce operation type");
            }
        }
    }

    async unsafe fn typed_reduce<T, OFI: AsFiType>(
        &self,
        op: &LamellarReduceOp,
        src: &[T],
        slice_or_pe: RootOrSliceMut<'_, T>,
    ) -> Result<(), libfabric::error::Error> {
        let mc = self.mcast_group.as_ref().expect("No multicast group for collective reduce");
        let (result, root_pe) = match slice_or_pe {
            RootOrSliceMut::Root(result) => (Some(result), self.ofi.my_pe) ,
            RootOrSliceMut::NotRoot(root_pe) => (None, root_pe),
        };
        
        let buf = unsafe { std::mem::transmute::<&[T], &[OFI]>(src) };
        let res = match result {
            Some(res) => unsafe {std::mem::transmute::<&mut [T], &mut [OFI]>(res)},
            None => {
                let res_buf = unsafe {std::slice::from_raw_parts_mut(src.as_ptr() as *mut T, src.len())}; // if result is None, we are doing an in-place reduce or we reduce on a non-root PE, so we can reuse the source buffer as the destination buffer since it is either the destination or will be ignored by non-root PEs
                unsafe { std::mem::transmute::<&mut [T], &mut [OFI]>(res_buf) }
            }
        };
        let mut ctx = self.ofi.info_entry.allocate_context();
        self.ofi.ep.reduce_async(
            buf,
            None,
            res,
            None,
            mc,
            &self.ofi.mapped_addresses[root_pe],
            op.into(),
            CollectiveOptions::default(),
            &mut ctx,
        ).await?;
        Ok(())
    }

    pub(crate) async fn gather_inner<T: 'static>(
        &self,
        src: &[T],
        slice_or_pe: RootOrSliceMut<'_, T>,
    ) -> Result<(), libfabric::error::Error> {

        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_gather::<T, u8>(src, slice_or_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_gather::<T, u16>(src, slice_or_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_gather::<T, u32>(src, slice_or_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_gather::<T, u64>(src, slice_or_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_gather::<T, usize>(src, slice_or_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_gather::<T, i8>(src, slice_or_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_gather::<T, i16>(src, slice_or_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_gather::<T, i32>(src, slice_or_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_gather::<T, i64>(src, slice_or_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_gather::<T, isize>(src, slice_or_pe).await
            } else {
                panic!("Unsupported allreduce operation type");
            }
        }
    }

    async unsafe fn typed_gather<T, OFI: AsFiType>(
        &self,
        src: &[T],
        slice_or_pe: RootOrSliceMut<'_, T>,
    ) -> Result<(), libfabric::error::Error> {
        let mc = self.mcast_group.as_ref().expect("No multicast group for collective reduce");
        let (result, root_pe) = match slice_or_pe {
            RootOrSliceMut::Root(result) => (Some(result), self.ofi.my_pe) ,
            RootOrSliceMut::NotRoot(root_pe) => (None, root_pe),
        };
        let res = match result {
            Some(res) => unsafe {std::mem::transmute::<&mut [T], &mut [OFI]>(res)},
            None => {
                let res_buf = unsafe {std::slice::from_raw_parts_mut(src.as_ptr() as *mut T, src.len())}; // if result is None, we are a non-root PE, so we can reuse the source buffer as the destination buffer since it will be ignored.
                unsafe { std::mem::transmute::<&mut [T], &mut [OFI]>(res_buf) }
            }
        };
        let buf = unsafe { std::mem::transmute::<&[T], &[OFI]>(src) };
        let mut ctx = self.ofi.info_entry.allocate_context();
        self.ofi.ep.gather_async(
            buf,
            None,
            res,
            None,
            mc,
            &self.ofi.mapped_addresses[root_pe],
            CollectiveOptions::default(),
            &mut ctx,
        ).await?;

        Ok(())

    }

    pub(crate) async fn broadcast_inner<T: 'static>(
        &self,
        root_src: RootSrcOrSliceMut<'_, T>,
    ) -> Result<(), libfabric::error::Error> {

        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_broadcast::<T, u8>(root_src).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_broadcast::<T, u16>(root_src).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_broadcast::<T, u32>(root_src).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_broadcast::<T, u64>(root_src).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_broadcast::<T, usize>(root_src).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_broadcast::<T, i8>(root_src).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_broadcast::<T, i16>(root_src).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_broadcast::<T, i32>(root_src).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_broadcast::<T, i64>(root_src).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_broadcast::<T, isize>(root_src).await
            } else {
                panic!("Unsupported allreduce operation type");
            }
        }
    }

    async unsafe fn typed_broadcast<T, OFI: AsFiType>(
        &self,
        slice_or_pe: RootSrcOrSliceMut<'_, T>,
    ) -> Result<(), libfabric::error::Error> {
        let mc = self.mcast_group.as_ref().expect("No multicast group for collective reduce");
        let (result, root_pe) = match slice_or_pe {
            RootSrcOrSliceMut::Root(src) => (unsafe{std::slice::from_raw_parts_mut(src.as_ptr() as *mut T, src.len())}, self.ofi.my_pe) ,
            RootSrcOrSliceMut::NotRoot(result, root_pe) => (result, root_pe),
        };
        let res = unsafe {std::mem::transmute::<&mut [T], &mut [OFI]>(result)};
        let mut ctx = self.ofi.info_entry.allocate_context();
        self.ofi.ep.broadcast_async(
            res,
            None,
            mc,
            &self.ofi.mapped_addresses[root_pe],
            CollectiveOptions::default(),
            &mut ctx,
        ).await?;

        Ok(())
    }

    pub(crate) async fn scatter_inner<T: 'static>(
        &self,
        res: &mut [T],
        src_or_root_pe: RootSrcSliceOrNone<'_, T>,
    ) -> Result<(), libfabric::error::Error> {

        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_scatter::<T, u8>(res, src_or_root_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_scatter::<T, u16>(res, src_or_root_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_scatter::<T, u32>(res, src_or_root_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_scatter::<T, u64>(res, src_or_root_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_scatter::<T, usize>(res, src_or_root_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_scatter::<T, i8>(res, src_or_root_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_scatter::<T, i16>(res, src_or_root_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_scatter::<T, i32>(res, src_or_root_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_scatter::<T, i64>(res, src_or_root_pe).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_scatter::<T, isize>(res, src_or_root_pe).await
            } else {
                panic!("Unsupported allreduce operation type");
            }
        }

    }

    async unsafe fn typed_scatter<T, OFI: AsFiType>(
        &self,
        res: &mut [T],
        src_or_root_pe: RootSrcSliceOrNone<'_, T>,
    ) -> Result<(), libfabric::error::Error> {
        let mc = self.mcast_group.as_ref().expect("No multicast group for collective reduce");

        let res = unsafe {std::mem::transmute::<&mut [T], &mut [OFI]>(res)};
        
        let (src, root_pe) = match src_or_root_pe {
            RootSrcSliceOrNone::Root(src) => (unsafe { std::mem::transmute::<&[T], &[OFI]>(src) }, self.ofi.my_pe),
            RootSrcSliceOrNone::NotRoot(root_pe) => (unsafe {std::slice::from_raw_parts(res.as_ptr(), res.len())}, root_pe),
        };

        let mut ctx = self.ofi.info_entry.allocate_context();
        self.ofi.ep.scatter_async(
            src,
            None,
            res,
            None,
            mc,
            &self.ofi.mapped_addresses[root_pe],
            CollectiveOptions::default(),
            &mut ctx,
        ).await?;

        Ok(())
    }

    pub(crate) async fn reduce_scatter_inner<T: 'static>(
        &self,
        op: &AllReduceOp,
        src: &[T],
        result: &mut [T],
    ) -> Result<(), libfabric::error::Error> {
        unsafe {
            if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
                self.typed_reduce_scatter::<T, u8>(op, src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
                self.typed_reduce_scatter::<T, u16>(op, src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
                self.typed_reduce_scatter::<T, u32>(op, src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
                self.typed_reduce_scatter::<T, u64>(op, src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
                self.typed_reduce_scatter::<T, usize>(op, src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
                self.typed_reduce_scatter::<T, i8>(op, src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
                self.typed_reduce_scatter::<T, i16>(op, src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
                self.typed_reduce_scatter::<T, i32>(op, src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
                self.typed_reduce_scatter::<T, i64>(op, src, result).await
            } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
                self.typed_reduce_scatter::<T, isize>(op, src, result).await
            } else {
                panic!("Unsupported allreduce operation type");
            }
        }
    }

    async unsafe fn typed_reduce_scatter<T, OFI: AsFiType>(
        &self,
        op: &AllReduceOp,
        src: &[T],
        result: &mut [T],
    ) -> Result<(), libfabric::error::Error> {
        let res = unsafe {&mut *(result as *mut [T] as *mut [OFI])};
        let buf = unsafe { std::mem::transmute::<&[T], &[OFI]>(src) };
        let mc = self.mcast_group.as_ref().expect("No multicast group for allreduce");
        let mut ctx = self.ofi.info_entry.allocate_context();
        self.ofi.ep.reduce_scatter_async(
            buf,
            None,
            res,
            None,
            mc,
            op.into(),
            CollectiveOptions::default(),
            &mut ctx,
        ).await?;
        Ok(())
    }


    pub(crate) fn wait(&self) -> Result<(), libfabric::error::Error> {
        self.ofi.wait_for_tx_cntr()?;
        self.ofi.wait_for_rx_cntr()?;
        Ok(())
    }


}

impl Drop for LibfabricAsyncAlloc {
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop LibfabricAsyncAlloc");
        let fabric_ref_count = self.decrement_fabric_ref_count();
        debug!(target: "libfabric", "Dropping LibfabricAsyncAlloc: {:x} - {:x} ref_cnt(before drop) {}", self.range.start,self.range.end, fabric_ref_count);

        match &self.alloc_table {
            AllocTable::Fabric(alloc_table) => {
                if fabric_ref_count == 2 {
                    debug!(target: "libfabric", "Dropping fabric LibfabricAsyncAlloc: {:?}", self);
                    alloc_table.remove_from_alloc(self);
                }
            }
            AllocTable::Runtime(rt_alloc_table, addr, fabric_alloc_table) => {
                let rt_ref_count = self.decrement_rt_ref_count();
                if rt_ref_count == 1 {
                    debug!(target: "libfabric", "Freeing runtime LibfabricAsyncAlloc: {:?}",  self);
                    rt_alloc_table.free(*addr).expect(&format!(
                        "[{:?}] Error removing from runtime alloc table {:x}",
                        std::thread::current().id(),
                        addr
                    ));
                }
                if fabric_ref_count == 2 {
                    debug!(target: "libfabric", "Dropping fabric LibfabricAsyncAlloc from rt LibfabricAsyncAlloc: {:?}", self);
                    fabric_alloc_table.remove_from_alloc(self);
                }
            }
        }
        trace!(target: "drop", "end drop LibfabricAsyncAlloc");
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OneSidedLibfabricAsyncAlloc {
    pub(crate) remote_pe: usize,
    pub(crate) alloc: LibfabricAsyncAlloc,
}

impl OneSidedLibfabricAsyncAlloc {
    pub(crate) fn num_bytes(&self) -> usize {
        self.alloc.num_bytes()
    }
    pub(crate) fn start(&self) -> usize {
        self.alloc.start()
    }
    pub(crate) fn sub_alloc(&self, offset: usize, len: usize) -> AllocResult<Self> {
        let sub_alloc = self.alloc.sub_alloc(offset, len)?;
        Ok(OneSidedLibfabricAsyncAlloc {
            remote_pe: self.remote_pe,
            alloc: sub_alloc,
        })
    }
}

impl From<OneSidedLibfabricAsyncAlloc> for CommAlloc {
    fn from(alloc: OneSidedLibfabricAsyncAlloc) -> Self {
        CommAlloc {
            inner_alloc: Arc::new(CommAllocInner::OneSidedLibfabricAsyncAlloc(alloc)),
            // alloc_type: CommAllocType::Remote,
        }
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

#[cfg(not(feature = "enable-libfabric"))]
impl<T> From<&LamellarAtomicOp<T>> for AtomicOp {
    fn from(op: &LamellarAtomicOp<T>) -> Self {
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

#[cfg(not(feature = "enable-libfabric"))]
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

#[cfg(not(feature = "enable-libfabric"))]
impl<T> From<&LamellarAtomicOp<T>> for FetchAtomicOp {
    fn from(op: &LamellarAtomicOp<T>) -> Self {
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

#[cfg(not(feature = "enable-libfabric"))]
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

#[cfg(not(feature = "enable-libfabric"))]
impl<T> From<LamellarAtomicOp<T>> for AtomicOp {
    fn from(op: LamellarAtomicOp<T>) -> Self {
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

#[cfg(not(feature = "enable-libfabric"))]
impl<T> From<LamellarAtomicOp<T>> for FetchAtomicOp {
    fn from(op: LamellarAtomicOp<T>) -> Self {
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

pub(crate) mod comm;
pub(crate) mod command_queues;
pub(crate) mod local_lamellae;
pub(crate) mod shmem_lamellae;

#[cfg(feature = "enable-on-node-shmem")]
pub(crate) mod shmem_utils;

use crate::{active_messaging::Msg, config, lamellar_arch::LamellarArchRT, scheduler::Scheduler};
pub(crate) use comm::*;

pub use comm::atomic::{AtomicCompareExchangeOpHandle, AtomicFetchOpHandle, AtomicOpHandle};
pub use comm::rdma::RdmaHandle;
use local_lamellae::{Local, LocalBuilder};
use shmem_lamellae::{Shmem, ShmemBuilder};

#[cfg(feature = "enable-rofi-c")]
pub(crate) mod rofi_c_lamellae;
#[cfg(feature = "enable-rofi-c")]
use rofi_c_lamellae::{RofiC, RofiCBuilder};

#[cfg(feature = "enable-libfabric")]
pub(crate) mod libfabric_lamellae;
#[cfg(feature = "enable-libfabric-mt")]
pub(crate) mod libfabric_lamellae_mt;
#[cfg(feature = "enable-ucx-mt")]
pub(crate) mod ucx_lamellae_mt;

#[cfg(feature = "enable-libfabric-async")]
pub(crate) mod libfabric_async_lamellae;
#[cfg(feature = "enable-ucx")]
pub(crate) mod ucx_lamellae;

#[cfg(feature = "enable-libfabric-async")]
use libfabric_async_lamellae::{LibfabricAsync, LibfabricAsyncBuilder};
#[cfg(feature = "enable-libfabric")]
use libfabric_lamellae::{Libfabric, LibfabricBuilder};
#[cfg(feature = "enable-libfabric-mt")]
use libfabric_lamellae_mt::{LibfabricMt, LibfabricMtBuilder};
#[cfg(feature = "enable-ucx")]
use ucx_lamellae::{Ucx, UcxBuilder};
#[cfg(feature = "enable-ucx-mt")]
use ucx_lamellae_mt::{UcxMt, UcxMtBuilder};

use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
use std::sync::Arc;
use tracing::trace;

lazy_static! {
    static ref SERIALIZE_HEADER_LEN: usize =
        crate::serialized_size::<Option<SerializeHeader>>(&Some(Default::default()), false);
}

/// The list of available lamellae backends, used to specify how data is transferred between PEs
#[derive(
    serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone, Copy,
)]
pub enum Backend {
    #[cfg(feature = "enable-rofi-c")]
    #[cfg_attr(docsrs, doc(cfg(feature = "enable-rofi-c")))]
    RofiC,
    #[cfg(feature = "enable-libfabric")]
    #[cfg_attr(docsrs, doc(cfg(feature = "enable-libfabric")))]
    Libfabric,
    #[cfg(feature = "enable-libfabric-mt")]
    #[cfg_attr(docsrs, doc(cfg(feature = "enable-libfabric-mt")))]
    LibfabricMt, // Updated feature
    #[cfg(feature = "enable-libfabric-async")]
    #[cfg_attr(docsrs, doc(cfg(feature = "enable-libfabric-async")))]
    LibfabricAsync,
    #[cfg(feature = "enable-ucx")]
    #[cfg_attr(docsrs, doc(cfg(feature = "enable-ucx")))]
    Ucx,
    #[cfg(feature = "enable-ucx-mt")]
    #[cfg_attr(docsrs, doc(cfg(feature = "enable-ucx-mt")))]
    UcxMt, // Updated feature
    /// The Local backend -- intended for single process environments
    Local,
    /// The Shmem backend -- intended for multi process environments single node environments
    Shmem,
}

#[derive(Debug, Clone)]
pub(crate) enum AllocationType {
    Local,
    Global,
    Sub(Vec<usize>),
}

impl Default for Backend {
    fn default() -> Self {
        // println!("default backend: {}", config().backend);
        match config().backend.as_str() {
            "rofi_c" => {
                #[cfg(feature = "enable-rofi-c")]
                return Backend::RofiC;
                #[cfg(not(feature = "enable-rofi-c"))]
                panic!("unable to set rofi C backend, recompile with 'enable-rofi-c' feature")
            }
            "libfabric" => {
                #[cfg(feature = "enable-libfabric")]
                return Backend::Libfabric;
                #[cfg(not(feature = "enable-libfabric"))]
                panic!("unable to set libfabric backend, recompile with 'enable-libfabric' feature")
            }

            "libfabric-mt" => {
                #[cfg(feature = "enable-libfabric-mt")]
                return Backend::LibfabricMt;
                #[cfg(not(feature = "enable-libfabric-mt"))]
                panic!(
                    "unable to set libfabric-mt backend, recompile with 'enable-libfabric-mt' feature"
                )
            }

            "libfabric-async" => {
                #[cfg(feature = "enable-libfabric-async")]
                return Backend::LibfabricAsync;
                #[cfg(not(feature = "enable-libfabric-async"))]
                panic!("unable to set libfabric-async backend, recompile with 'enable-libfabric-async' feature")
            }
            "ucx" => {
                #[cfg(feature = "enable-ucx")]
                return Backend::Ucx;
                #[cfg(not(feature = "enable-ucx"))]
                panic!("unable to set ucx backend, recompile with 'enable-ucx' feature")
            }
            "ucx-mt" => {
                #[cfg(feature = "enable-ucx-mt")]
                return Backend::UcxMt;
                #[cfg(not(feature = "enable-ucx-mt"))]
                panic!("unable to set ucx-mt backend, recompile with 'enable-ucx-mt' feature")
            }
            "shmem" => {
                return Backend::Shmem;
            }
            "local" => {
                return Backend::Local;
            }
            _ => {
                panic!("unknown backend: {}", config().backend);
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub(crate) struct SerializeHeader {
    pub(crate) msg: Msg,
}

// #[derive(Debug)]
#[derive(Clone)]
pub(crate) struct SerializedData {
    pub(crate) alloc: CommAlloc,
    pub(crate) ser_data_bytes: CommSlice<u8>,
    pub(crate) header_bytes: CommSlice<u8>,
    pub(crate) payload_bytes: CommSlice<u8>,
}

// #[derive(Debug)]
pub(crate) struct SubSerializedData {
    pub(crate) alloc: CommAlloc,
    pub(crate) _ser_data_bytes: CommSlice<u8>,
    pub(crate) header_bytes: CommSlice<u8>,
    pub(crate) payload_bytes: CommSlice<u8>,
}

// we have allocated this memory out of fabric memory and thus are responsible for managing it,
// we will not move the underlying data, reallocate it, nor free it until all references are dropped
unsafe impl Send for SerializedData {}
unsafe impl Sync for SerializedData {}

unsafe impl Send for SubSerializedData {}
unsafe impl Sync for SubSerializedData {}

impl SerializedData {
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new(comm: Arc<Comm>, size: usize) -> Result<Self, anyhow::Error> {
        let alloc_size = size; //+ ser_data_size_size;
        let mut alloc = comm.rt_alloc(alloc_size, std::mem::align_of::<usize>())?;
        alloc.set_print(true);
        let ser_data_bytes = alloc.comm_slice_at_byte_offset(0, size);
        let header_bytes = ser_data_bytes.sub_slice(0..*SERIALIZE_HEADER_LEN);
        let payload_bytes = ser_data_bytes.sub_slice(*SERIALIZE_HEADER_LEN..size);

        // println!(
        //     "[{:?}, {:?}] creating new serialized data {:?} {:?} {:?} {:?}",
        //     std::time::Instant::now(),
        //     std::thread::current().id(),
        //     alloc,
        //     ser_data_bytes,
        //     header_bytes,
        //     payload_bytes
        // );

        Ok(SerializedData {
            alloc,
            ser_data_bytes,
            header_bytes,
            payload_bytes,
        })
    }
}

impl SerializedData {
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn header_as_bytes(&self) -> CommSlice<u8> {
        self.header_bytes.clone()
    }
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn header_as_bytes_mut(&mut self) -> CommSlice<u8> {
        self.header_bytes.clone()
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn data_as_bytes(&self) -> CommSlice<u8> {
        self.payload_bytes.clone()
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn data_as_bytes_mut(&mut self) -> CommSlice<u8> {
        self.payload_bytes.clone()
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn data_len(&self) -> usize {
        self.payload_bytes.len()
    }
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn header_and_data_as_bytes_mut(&mut self) -> CommSlice<u8> {
        self.ser_data_bytes.clone()
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn len(&self) -> usize {
        self.ser_data_bytes.len()
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn print(&self) {
        println!("{:?}", self);
    }

    pub(crate) fn leak_alloc(self) -> CommAlloc {
        // println!("Leaking allocation");
        self.alloc
    }
}

impl std::fmt::Debug for SerializedData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SeralizedData addr: {:x} relative addr {:?} len {:?} data {:?} data_len {:?} alloc_size {:?}",

            self.alloc.comm_addr(),
            self.ser_data_bytes.as_ptr(),
            self.ser_data_bytes.len(),
            self.payload_bytes.as_ptr(),
            self.payload_bytes.len(),
            self.alloc.num_bytes())
    }
}

impl Des for SerializedData {
    #[tracing::instrument(skip_all, level = "debug")]
    fn deserialize_header(&self) -> Option<SerializeHeader> {
        crate::deserialize(&self.header_as_bytes(), false).unwrap()
    }
    #[tracing::instrument(skip_all, level = "debug")]
    fn deserialize_data<T: serde::de::DeserializeOwned>(&self) -> Result<T, anyhow::Error> {
        Ok(crate::deserialize(&self.data_as_bytes(), true)?)
    }
}

// impl SubData for SubSerializedData {
impl SerializedData {
    // unsafe because user must ensure that multiple sub_data do not overlap if mutating the underlying data
    #[tracing::instrument(level = "debug")]
    pub(crate) fn sub_data(&mut self, start: usize, end: usize) -> SubSerializedData {
        trace!("sub_data start: {} end: {}", start, end);
        SubSerializedData {
            alloc: self.alloc.clone(),
            _ser_data_bytes: self.ser_data_bytes.clone(),
            header_bytes: self.header_bytes.clone(),
            payload_bytes: self.payload_bytes.sub_slice(start..end),
        }
    }
}

impl SubSerializedData {
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn header_as_bytes(&self) -> CommSlice<u8> {
        self.header_bytes.clone()
    }
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn data_as_bytes(&self) -> CommSlice<u8> {
        self.payload_bytes.clone()
    }
}

impl std::fmt::Debug for SubSerializedData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SubSeralizedData addr: {:x} relative addr {:?} len {:?} data {:?} data_len {:?} alloc_size {:?}",

            self.alloc.comm_addr(),
            self._ser_data_bytes.as_ptr(),
            self._ser_data_bytes.len(),
            self.payload_bytes.as_ptr(),
            self.payload_bytes.len(),
            self.alloc.num_bytes())
    }
}

impl Des for SubSerializedData {
    #[tracing::instrument(skip_all, level = "debug")]
    fn deserialize_header(&self) -> Option<SerializeHeader> {
        crate::deserialize(&self.header_as_bytes(), false).unwrap()
    }
    #[tracing::instrument(skip_all, level = "debug")]
    fn deserialize_data<T: serde::de::DeserializeOwned>(&self) -> Result<T, anyhow::Error> {
        Ok(crate::deserialize(&self.data_as_bytes(), true)?)
    }
}
#[enum_dispatch]
pub(crate) trait Des {
    fn deserialize_header(&self) -> Option<SerializeHeader>;
    fn deserialize_data<T: serde::de::DeserializeOwned>(&self) -> Result<T, anyhow::Error>;
}

#[enum_dispatch(LamellaeInit)]
pub(crate) enum LamellaeBuilder {
    #[cfg(feature = "enable-rofi-c")]
    RofiCBuilder,
    #[cfg(feature = "enable-libfabric")]
    LibfabricBuilder,
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMtBuilder, // Updated feature
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsyncBuilder,
    #[cfg(feature = "enable-ucx")]
    UcxBuilder,
    #[cfg(feature = "enable-ucx-mt")]
    UcxMtBuilder, // Updated feature
    ShmemBuilder,
    LocalBuilder,
}

#[async_trait]
#[enum_dispatch]
pub(crate) trait LamellaeInit {
    fn init_fabric(&mut self) -> (usize, usize); //(my_pe,num_pes)
    fn init_lamellae(&mut self, scheduler: Arc<Scheduler>) -> Arc<Lamellae>;
}

#[enum_dispatch]
pub(crate) trait LamellaeShutdown {
    fn shutdown(&self);
    fn force_shutdown(&self);
    fn force_deinit(&self);
}

// #[async_trait]
#[enum_dispatch]
pub(crate) trait Ser {
    fn serialize_header(
        &self,
        header: Option<SerializeHeader>,
        serialized_size: usize,
    ) -> Result<SerializedData, anyhow::Error>;
}

#[enum_dispatch(Ser, LamellaeUtil, LamellaeShutdown)]
#[derive(Debug)]
pub(crate) enum Lamellae {
    #[cfg(feature = "enable-rofi-c")]
    RofiC,
    #[cfg(feature = "enable-libfabric")]
    Libfabric,
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMt,
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsync,
    #[cfg(feature = "enable-ucx")]
    Ucx,
    #[cfg(feature = "enable-ucx-mt")]
    UcxMt,
    // #[cfg(feature = "enable-libfabric")]
    // LibfabricAsync,
    Shmem,
    Local,
}

impl Lamellae {
    pub(crate) fn comm(&self) -> &Comm {
        match self {
            #[cfg(feature = "enable-rofi-c")]
            Lamellae::RofiC(rofi_c) => rofi_c.comm(),
            #[cfg(feature = "enable-libfabric")]
            Lamellae::Libfabric(libfabric) => libfabric.comm(),
            #[cfg(feature = "enable-libfabric-mt")]
            Lamellae::LibfabricMt(libfabric_mt) => libfabric_mt.comm(),
            #[cfg(feature = "enable-libfabric-async")]
            Lamellae::LibfabricAsync(libfabric_async) => libfabric_async.comm(),
            #[cfg(feature = "enable-ucx")]
            Lamellae::Ucx(ucx) => ucx.comm(),
            #[cfg(feature = "enable-ucx-mt")]
            Lamellae::UcxMt(ucx_mt) => ucx_mt.comm(),
            Lamellae::Shmem(shmem) => shmem.comm(),
            Lamellae::Local(local) => local.comm(),
        }
    }

    pub(crate) fn wait_all_print(&self) {
        match self {
            #[cfg(feature = "enable-rofi-c")]
            Lamellae::RofiC(rofi_c) => rofi_c.wait_all_print(),
            #[cfg(feature = "enable-libfabric")]
            Lamellae::Libfabric(libfabric) => libfabric.wait_all_print(),
            #[cfg(feature = "enable-libfabric-mt")]
            Lamellae::LibfabricMt(libfabric_mt) => libfabric_mt.wait_all_print(),
            #[cfg(feature = "enable-libfabric-async")]
            Lamellae::LibfabricAsync(libfabric_async) => libfabric_async.wait_all_print(),
            #[cfg(feature = "enable-ucx")]
            Lamellae::Ucx(ucx) => ucx.wait_all_print(),
            #[cfg(feature = "enable-ucx-mt")]
            Lamellae::UcxMt(ucx_mt) => ucx_mt.wait_all_print(),
            // #[cfg(feature = "enable-libfabric")]
            // Lamellae::LibfabricAsync => println!("libfabric async - nothing to print"),
            Lamellae::Shmem(shmem) => shmem.wait_all_print(),
            Lamellae::Local(local) => local.wait_all_print(),
        }
    }
}

#[async_trait]
#[enum_dispatch]
pub(crate) trait LamellaeUtil: Send {
    async fn send_to_pes_async(
        &self,
        pe: Option<usize>,
        team: Arc<LamellarArchRT>,
        data: SerializedData,
    );

    async fn request_new_alloc(&self, min_size: usize);
}

#[tracing::instrument(skip_all, level = "debug")]
pub(crate) fn create_lamellae(backend: Backend, num_threads: usize) -> LamellaeBuilder {
    match backend {
        #[cfg(feature = "enable-rofi-c")]
        Backend::RofiC => {
            let provider = config().rofi_provider.clone();
            let domain = config().rofi_domain.clone();
            return LamellaeBuilder::RofiCBuilder(RofiCBuilder::new(&provider, &domain));
        }
        #[cfg(feature = "enable-libfabric")]
        Backend::Libfabric => {
            let provider = config().rofi_provider.clone();
            let domain = config().rofi_domain.clone();
            LamellaeBuilder::LibfabricBuilder(LibfabricBuilder::new(&provider, &domain))
        }
        #[cfg(feature = "enable-libfabric-mt")]
        Backend::LibfabricMt => {
            let provider = config().rofi_provider.clone();
            let domain = config().rofi_domain.clone();
            LamellaeBuilder::LibfabricMtBuilder(LibfabricMtBuilder::new(
                &provider,
                &domain,
                num_threads,
            ))
        }
        #[cfg(feature = "enable-libfabric-async")]
        Backend::LibfabricAsync => {
            let provider = config().rofi_provider.clone();
            let domain = config().rofi_domain.clone();
            LamellaeBuilder::LibfabricAsyncBuilder(LibfabricAsyncBuilder::new(&provider, &domain))
        }
        #[cfg(feature = "enable-ucx")]
        Backend::Ucx => LamellaeBuilder::UcxBuilder(UcxBuilder::new()),
        #[cfg(feature = "enable-ucx-mt")]
        Backend::UcxMt => LamellaeBuilder::UcxMtBuilder(UcxMtBuilder::new(num_threads)),
        Backend::Shmem => LamellaeBuilder::ShmemBuilder(ShmemBuilder::new()),
        Backend::Local => LamellaeBuilder::LocalBuilder(LocalBuilder::new()),
    }
}

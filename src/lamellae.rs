use crate::active_messaging::Msg;
use crate::lamellar_arch::LamellarArchRT;
use crate::scheduler::Scheduler;
use std::sync::Arc;

use async_trait::async_trait;
use enum_dispatch::enum_dispatch;

pub(crate) mod comm;
pub(crate) mod command_queues;
use comm::AllocResult;
use comm::Comm;

pub(crate) mod local_lamellae;
use local_lamellae::{Local, LocalData};
#[cfg(feature = "enable-rofi")]
mod rofi;
#[cfg(feature = "enable-rofi")]
pub(crate) mod rofi_lamellae;

#[cfg(feature = "enable-rofi")]
use rofi::rofi_comm::RofiData;
#[cfg(feature = "enable-rofi")]
use rofi_lamellae::{Rofi, RofiBuilder};

pub(crate) mod shmem_lamellae;
use shmem::shmem_comm::ShmemData;
use shmem_lamellae::{Shmem, ShmemBuilder};
mod shmem;

lazy_static! {
    static ref SERIALIZE_HEADER_LEN: usize =
        crate::serialized_size::<Option<SerializeHeader>>(&Some(Default::default()), false);
}

/// The list of available lamellae backends, used to specify how data is transfered between PEs
#[derive(
    serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone, Copy,
)]
pub enum Backend {
    #[doc(hidden)]
    #[cfg(feature = "enable-rofi")]
    Rofi,
    #[doc(hidden)]
    Local,
    #[doc(hidden)]
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
        default_backend()
    }
}
fn default_backend() -> Backend {
    match std::env::var("LAMELLAE_BACKEND") {
        Ok(p) => match p.as_str() {
            "rofi" => {
                #[cfg(feature = "enable-rofi")]
                return Backend::Rofi;
                #[cfg(not(feature = "enable-rofi"))]
                panic!("unable to set rofi backend, recompile with 'enable-rofi' feature")
            }
            "shmem" => {
                return Backend::Shmem;
            }
            _ => {
                return Backend::Local;
            }
        },
        Err(_) => {
            #[cfg(feature = "enable-rofi")]
            return Backend::Rofi;
            #[cfg(not(feature = "enable-rofi"))]
            return Backend::Local;
        }
    };
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub(crate) struct SerializeHeader {
    pub(crate) msg: Msg,
}

#[enum_dispatch(Des, SubData, SerializedDataOps)]
#[derive(Clone, Debug)]
pub(crate) enum SerializedData {
    #[cfg(feature = "enable-rofi")]
    RofiData,
    ShmemData,
    LocalData,
}

#[enum_dispatch]
pub(crate) trait SerializedDataOps {
    fn header_as_bytes(&self) -> &mut [u8];
    fn increment_cnt(&self);
    fn len(&self) -> usize;
}

#[enum_dispatch]
pub(crate) trait Des {
    fn deserialize_header(&self) -> Option<SerializeHeader>;
    fn deserialize_data<T: serde::de::DeserializeOwned>(&self) -> Result<T, anyhow::Error>;
    fn header_and_data_as_bytes(&self) -> &mut [u8];
    fn data_as_bytes(&self) -> &mut [u8];
    fn print(&self);
}

#[enum_dispatch]
pub(crate) trait SubData {
    fn sub_data(&self, start: usize, end: usize) -> SerializedData;
}

#[enum_dispatch(LamellaeInit)]
pub(crate) enum LamellaeBuilder {
    #[cfg(feature = "enable-rofi")]
    RofiBuilder,
    ShmemBuilder,
    Local,
}

#[async_trait]
#[enum_dispatch]
pub(crate) trait LamellaeInit {
    fn init_fabric(&mut self) -> (usize, usize); //(my_pe,num_pes)
    fn init_lamellae(&mut self, scheduler: Arc<Scheduler>) -> Arc<Lamellae>;
}

// #[async_trait]
#[enum_dispatch]
pub(crate) trait Ser {
    fn serialize<T: serde::Serialize + ?Sized>(
        &self,
        header: Option<SerializeHeader>,
        obj: &T,
    ) -> Result<SerializedData, anyhow::Error>;
    fn serialize_header(
        &self,
        header: Option<SerializeHeader>,
        serialized_size: usize,
    ) -> Result<SerializedData, anyhow::Error>;
}

#[enum_dispatch(LamellaeComm, LamellaeAM, LamellaeRDMA, Ser)]
#[derive(Debug)]
pub(crate) enum Lamellae {
    #[cfg(feature = "enable-rofi")]
    Rofi,
    Shmem,
    Local,
}

#[async_trait]
#[enum_dispatch]
pub(crate) trait LamellaeComm: LamellaeAM + LamellaeRDMA {
    // this is a global barrier (hopefully using hardware)
    fn my_pe(&self) -> usize;
    fn num_pes(&self) -> usize;
    fn barrier(&self);
    fn backend(&self) -> Backend;
    #[allow(non_snake_case)]
    fn MB_sent(&self) -> f64;
    fn print_stats(&self);
    fn shutdown(&self);
    fn force_shutdown(&self);
    fn force_deinit(&self);
}

#[async_trait]
#[enum_dispatch]
pub(crate) trait LamellaeAM: Send {
    async fn send_to_pe_async(&self, pe: usize, data: SerializedData); //should never send to self... this is short circuited before request is serialized in the active message layer
    async fn send_to_pes_async(
        &self,
        pe: Option<usize>,
        team: Arc<LamellarArchRT>,
        data: SerializedData,
    );
}

#[enum_dispatch]
pub(crate) trait LamellaeRDMA: Send + Sync {
    fn flush(&self);
    fn put(&self, pe: usize, src: &[u8], dst: usize);
    fn iput(&self, pe: usize, src: &[u8], dst: usize);
    fn put_all(&self, src: &[u8], dst: usize);
    fn get(&self, pe: usize, src: usize, dst: &mut [u8]);
    fn iget(&self, pe: usize, src: usize, dst: &mut [u8]);
    fn rt_alloc(&self, size: usize, align: usize) -> AllocResult<usize>;
    fn rt_check_alloc(&self, size: usize, align: usize) -> bool;
    fn rt_free(&self, addr: usize);
    fn alloc(&self, size: usize, alloc: AllocationType, align: usize) -> AllocResult<usize>;
    fn free(&self, addr: usize);
    fn base_addr(&self) -> usize;
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize;
    fn remote_addr(&self, remote_pe: usize, local_addr: usize) -> usize;
    fn occupied(&self) -> usize;
    fn num_pool_allocs(&self) -> usize;
    fn alloc_pool(&self, min_size: usize);
}

#[allow(unused_variables)]

pub(crate) fn create_lamellae(backend: Backend) -> LamellaeBuilder {
    match backend {
        #[cfg(feature = "enable-rofi")]
        Backend::Rofi => {
            let provider = match std::env::var("LAMELLAR_ROFI_PROVIDER") {
                Ok(p) => match p.as_str() {
                    "verbs" => "verbs",
                    "shm" => "shm",
                    _ => "verbs",
                },
                Err(_) => "verbs",
            };
            LamellaeBuilder::RofiBuilder(RofiBuilder::new(provider))
        }
        Backend::Shmem => LamellaeBuilder::ShmemBuilder(ShmemBuilder::new()),
        Backend::Local => LamellaeBuilder::Local(Local::new()),
    }
}

use crate::lamellar_arch::LamellarArchRT;
use crate::active_messaging::Msg;
use crate::active_messaging::registered_active_message::AmId;
use crate::scheduler::Scheduler;
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use std::sync::Arc;

use async_trait::async_trait;
use enum_dispatch::enum_dispatch;

pub(crate) mod local_lamellae;
use local_lamellae::{Local,LocalData};
#[cfg(feature = "enable-rofi")]
mod rofi;
#[cfg(feature = "enable-rofi")]
pub(crate) mod rofi_lamellae;

use rofi_lamellae::{RofiBuilder,Rofi};
use rofi::rofi_comm::RofiData;
// pub(crate) mod shmem_lamellae;

#[derive(
    serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone, Copy,
)]
pub enum Backend {
    #[cfg(feature = "enable-rofi")]
    Rofi,
    // #[cfg(feature = "enable-rofi")]
    // RofiShm,
    // #[cfg(feature = "enable-rofi")]
    // RofiVerbs,
    Local,
    // Shmem,
}

#[derive(Debug,Clone)]
pub(crate) enum  AllocationType{
    Local,
    Global,
    Sub(Vec<usize>),
}

//#[prof]
impl Default for Backend {
    fn default() -> Self {
        default_backend()
    }
}
fn default_backend() -> Backend {
    #[cfg(feature = "enable-rofi")]
    return Backend::Rofi;
    #[cfg(not(feature = "enable-rofi"))]
    return Backend::Local;
}


// #[derive(Clone)]
// pub(crate) struct SerializedData{
//     pub(crate) addr: usize,
//     pub(crate) len: usize,
//     pub(crate) rdma: Arc<Lamellae>
// }
#[derive(serde::Serialize,serde::Deserialize,Clone)]
pub(crate) struct SerializeHeader{
    pub(crate) msg: Msg,
    pub(crate) team_hash: u64,
    pub(crate) id: AmId,
}


#[enum_dispatch(Des)]
#[derive(Clone)]
pub(crate) enum SerializedData{
    RofiData,
    LocalData
}

#[enum_dispatch]
pub(crate) trait Des{
    fn deserialize_header(&self) -> Option<SerializeHeader>;
    fn deserialize_data<T: serde::de::DeserializeOwned>(& self) -> Result<T, anyhow::Error>;
    fn data_as_bytes(&self) -> &mut[u8];
}


// impl Drop for SerializedData{
//     fn drop(&mut self){
//         //println!("dropping {:?} {:?}",self.addr,self.len);
//         // self.rdma.rt_free(self.addr - rdma.base_addr());
//     }
// }

#[enum_dispatch(LamellaeInit)]
pub(crate) enum LamellaeBuilder{
    RofiBuilder,
    Local, 
}

#[async_trait]
#[enum_dispatch]
pub(crate) trait LamellaeInit{
    fn init_fabric(&mut self) -> (usize, usize);//(my_pe,num_pes)
    fn init_lamellae(&mut self, scheduler: Arc<Scheduler>) -> Arc<Lamellae>;
    
}

#[async_trait]
#[enum_dispatch]
pub(crate) trait Ser{
    async fn serialize<T: Send + Sync + serde::Serialize + ?Sized >(&self,header: Option<SerializeHeader>, obj: &T) -> Result<SerializedData,anyhow::Error>;
    async fn serialize_header(&self,header: Option<SerializeHeader>,serialized_size: usize) -> Result<SerializedData,anyhow::Error>;
}

#[enum_dispatch(LamellaeComm, LamellaeAM, LamellaeRDMA, Ser)]
pub(crate) enum Lamellae{
    // #[cfg(feature = "enable-rofi")]
    Rofi,
    Local,
}

#[async_trait]
#[enum_dispatch]
pub(crate) trait LamellaeComm: LamellaeAM + LamellaeRDMA + Send + Sync {
// this is a global barrier (hopefully using hardware)
    fn my_pe(&self) -> usize;
    fn num_pes(&self) ->usize;
    fn barrier(&self);
    fn backend(&self) -> Backend;
    #[allow(non_snake_case)]
    fn MB_sent(&self) -> f64;
    fn print_stats(&self);
    fn shutdown(&self);
}

#[async_trait]
#[enum_dispatch]
pub(crate) trait LamellaeAM: Send + Sync{
    async fn send_to_pe_async(&self, pe: usize ,data: SerializedData); //should never send to self... this is short circuited before request is serialized in the active message layer
    async fn send_to_pes_async(&self,pe: Option<usize>, team: Arc<LamellarArchRT>, data: SerializedData);
}

#[enum_dispatch]
pub(crate) trait LamellaeRDMA: Send + Sync {
    fn put(&self, pe: usize, src: &[u8], dst: usize);
    fn iput(&self, pe: usize, src: &[u8], dst: usize);
    fn put_all(&self, src: &[u8], dst: usize);
    fn get(&self, pe: usize, src: usize, dst: &mut [u8]);
    fn rt_alloc(&self, size: usize) -> Option<usize>;
    fn rt_free(&self, addr: usize);
    fn alloc(&self, size: usize, alloc: AllocationType) -> Option<usize>;
    fn free(&self, addr: usize);
    fn base_addr(&self) -> usize;
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize;
    fn remote_addr(&self, remote_pe: usize, local_addr: usize) -> usize;
}


#[allow(unused_variables)]
//#[prof]
pub(crate) fn create_lamellae(backend: Backend) -> LamellaeBuilder { 
    match backend {
        // #[cfg(feature = "enable-rofi")]
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
            // Box::new(rofi_lamellae::RofiLamellae::new(provider))
        }
        // #[cfg(feature = "enable-rofi")]
        // Backend::RofiShm => Box::new(rofi_lamellae::RofiLamellae::new("shm")),
        // #[cfg(feature = "enable-rofi")]
        // Backend::RofiVerbs => Box::new(rofi_lamellae::RofiLamellae::new("verbs")),
        // Backend::Shmem => Box::new(shmem_lamellae::ShmemLamellae::new()),
        Backend::Local => LamellaeBuilder::Local(Local::new()),
    }
}

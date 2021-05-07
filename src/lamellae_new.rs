use crate::lamellar_arch::LamellarArchRT;
use crate::scheduler::Scheduler;
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use std::sync::Arc;

use async_trait::async_trait;
use enum_dispatch::enum_dispatch;

pub(crate) mod local_lamellae;
use local_lamellae::Local;
#[cfg(feature = "enable-rofi")]
mod rofi;
#[cfg(feature = "enable-rofi")]
pub(crate) mod rofi_lamellae;

use rofi_lamellae::{RofiBuilder,Rofi};
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
pub(crate) struct SerializedData{
    pub(crate) addr: usize,
    pub(crate) len: usize,
    pub(crate) rdma: Arc<Lamellae>
}

impl Drop for SerializedData{
    fn drop(&mut self){
        self.rdma.rt_free(self.addr);
    }
}

pub(crate) async fn serialize<T: ?Sized>(obj: &T,rdma: Arc<Lamellae>) -> Result<SerializedData,anyhow::Error> 
where
    T: serde::Serialize {
    let size = bincode::serialized_size(obj)? as usize;
    let mut mem = rdma.rt_alloc(size+1);
    while mem.is_none(){
        async_std::task::yield_now().await;
        mem = rdma.rt_alloc(size);
    }
    let addr = mem.unwrap();
    let mem_slice = unsafe {std::slice::from_raw_parts_mut(addr as *mut u8, size)};
     mem_slice[size-1]=0;
    bincode::serialize_into(mem_slice,obj)?;
    Ok(SerializedData{
        addr: addr,
        len: size,
        rdma: rdma.clone()
    })
}

#[enum_dispatch(LamellaeInit)]
pub(crate) enum LamellaeBuilder{
    RofiBuilder,
    Local, 
}

#[enum_dispatch]
pub(crate) trait LamellaeInit{
    fn init_fabric(&mut self) -> (usize, usize);
    fn init_lamellae(&mut self, scheduler: Arc<Scheduler>) -> Lamellae;
}

#[enum_dispatch(LamellaeComm, LamellaeAM, LamellaeRDMA)]
pub(crate) enum Lamellae{
    // #[cfg(feature = "enable-rofi")]
    Rofi,
    Local,
}

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
}

#[async_trait]
#[enum_dispatch]
pub(crate) trait LamellaeAM: Send + Sync{
    fn send_to_pe(&self, pe: usize, data: std::vec::Vec<u8>); //should never send to self... this is short circuited before request is serialized in the active message layer
    fn send_to_pes(
        //should never send to self... this is short circuited before request is serialized in the active message layer
        &self,
        pe: Option<usize>,
        team: Arc<LamellarArchRT>,
        data: std::vec::Vec<u8>,
    );
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

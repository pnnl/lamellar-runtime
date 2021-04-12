use crate::lamellar_arch::LamellarArchRT;
use crate::schedulers::SchedulerQueue;
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use std::sync::Arc;

pub(crate) mod local_lamellae;
#[cfg(feature = "enable-rofi")]
mod rofi;
#[cfg(feature = "enable-rofi")]
pub(crate) mod rofi_lamellae;
pub(crate) mod shmem_lamellae;

#[derive(
    serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone, Copy,
)]
pub enum Backend {
    #[cfg(feature = "enable-rofi")]
    Rofi,
    #[cfg(feature = "enable-rofi")]
    RofiShm,
    #[cfg(feature = "enable-rofi")]
    RofiVerbs,
    Local,
    Shmem,
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

pub(crate) trait LamellaeAM: Send + Sync + std::fmt::Debug {
    fn send_to_pe(&self, pe: usize, data: std::vec::Vec<u8>); //should never send to self... this is short circuited before request is serialized in the active message layer
    fn send_to_all(&self, data: std::vec::Vec<u8>); //should never send to self... this is short circuited before request is serialized in the active message layer
    fn send_to_pes(
        //should never send to self... this is short circuited before request is serialized in the active message layer
        &self,
        pe: Option<usize>,
        team: Arc<LamellarArchRT>,
        data: std::vec::Vec<u8>,
    );
    //this probably has to be an active message based barrier (unless hardware supports barrier groups?)
    fn barrier(&self);
    fn backend(&self) -> Backend;
}

pub(crate) trait LamellaeRDMA: Send + Sync {
    fn put(&self, pe: usize, src: &[u8], dst: usize);
    fn iput(&self, pe: usize, src: &[u8], dst: usize);
    fn put_all(&self, src: &[u8], dst: usize);
    fn get(&self, pe: usize, src: usize, dst: &mut [u8]);
    fn rt_alloc(&self, size: usize) -> Option<usize>;
    fn rt_free(&self, addr: usize);
    fn alloc(&self, size: usize) -> Option<usize>;
    fn free(&self, addr: usize);
    fn base_addr(&self) -> usize;
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize;
    fn remote_addr(&self, remote_pe: usize, local_addr: usize) -> usize;
    fn mype(&self) -> usize;
}

pub(crate) trait Lamellae: Send + Sync {
    // fn new() -> Self;
    fn init_fabric(&mut self) -> (usize, usize);
    fn init_lamellae(&mut self, scheduler: Arc<dyn SchedulerQueue>);
    fn finit(&self);
    fn get_am(&self) -> Arc<dyn LamellaeAM>;
    // fn get_rdma(&self) -> &dyn LamellaeRDMA;
    fn get_rdma(&self) -> Arc<dyn LamellaeRDMA>;
    //this is a global barrier (hopefully using hardware)
    fn barrier(&self);
    fn backend(&self) -> Backend;
    #[allow(non_snake_case)]
    fn MB_sent(&self) -> f64;
    fn print_stats(&self);
}
//#[prof]
impl<T: Lamellae + ?Sized> Lamellae for Box<T> {
    fn init_fabric(&mut self) -> (usize, usize) {
        (**self).init_fabric()
    }
    fn init_lamellae(&mut self, scheduler: Arc<dyn SchedulerQueue>) {
        (**self).init_lamellae(scheduler)
    }
    fn finit(&self) {
        (**self).finit()
    }
    fn get_am(&self) -> Arc<dyn LamellaeAM> {
        (**self).get_am()
    }
    // fn get_rdma(&self) -> &dyn LamellaeRDMA {
    //     (**self).get_rdma()
    // }
    fn get_rdma(&self) -> Arc<dyn LamellaeRDMA> {
        (**self).get_rdma()
    }
    //this is a global barrier (hopefully using hardware)
    fn barrier(&self) {
        (**self).barrier()
    }
    fn backend(&self) -> Backend {
        (**self).backend()
    }
    #[allow(non_snake_case)]
    fn MB_sent(&self) -> f64 {
        (**self).MB_sent()
    }
    fn print_stats(&self) {
        (**self).print_stats()
    }
}
#[allow(unused_variables)]
//#[prof]
pub(crate) fn create_lamellae(backend: Backend) -> Box<dyn Lamellae> {
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
            Box::new(rofi_lamellae::RofiLamellae::new(provider))
        }
        #[cfg(feature = "enable-rofi")]
        Backend::RofiShm => Box::new(rofi_lamellae::RofiLamellae::new("shm")),
        #[cfg(feature = "enable-rofi")]
        Backend::RofiVerbs => Box::new(rofi_lamellae::RofiLamellae::new("verbs")),
        Backend::Shmem => Box::new(shmem_lamellae::ShmemLamellae::new()),
        Backend::Local => Box::new(local_lamellae::LocalLamellae::new()),
    }
}

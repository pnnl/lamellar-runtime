use crate::lamellar_team::LamellarArch;
use crate::schedulers::SchedulerQueue;
use std::sync::Arc;

mod rofi;
pub(crate) mod rofi_lamellae; //{rofi_api,rofi_comm};

#[derive(
    serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone, Copy,
)]
pub enum Backend {
    Rofi,
}

pub(crate) trait LamellaeAM: Send + Sync {
    fn send_to_pe(&self, pe: usize, data: std::vec::Vec<u8>);
    fn send_to_all(&self, data: std::vec::Vec<u8>);
    fn send_to_pes(
        &self,
        pe: Option<usize>,
        team: Arc<dyn LamellarArch + Sync + Send>,
        data: std::vec::Vec<u8>,
    );
    //this probably has to be an active message based barrier (unless hardware supports barrier groups?)
    fn barrier(&self);
    fn backend(&self) -> Backend;
}

pub(crate) trait LamellaeRDMA: Send + Sync 
{
    fn put(&self, pe: usize, src: &[u8], dst: usize);
    fn put_all(&self, src: &[u8], dst: usize);
    fn get(&self, pe: usize, src: usize, dst: &mut [u8]);
    fn alloc(&self, size: usize) -> Option<usize>;
    fn free(&self, addr: usize);
    fn base_addr(&self) -> usize;
}

pub(crate) trait Lamellae: Send + Sync{
    // fn new() -> Self;
    fn init(&mut self) -> (usize, usize);
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
impl<T: Lamellae + ?Sized> Lamellae for Box<T> {
    fn init(&mut self) -> (usize, usize) {
        (**self).init()
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
pub(crate) fn create_lamellae(
    backend: Backend,
    scheduler: Arc<dyn SchedulerQueue>,
) -> Box<dyn Lamellae> {
    Box::new(match backend {
        Backend::Rofi => rofi_lamellae::RofiLamellae::new(scheduler),
    })
}


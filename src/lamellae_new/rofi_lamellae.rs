use crate::lamellae_new::{AllocationType, Backend, Lamellae, LamellaeInit, LamellaeComm, LamellaeAM, LamellaeRDMA, SerializedData};
use crate::lamellae_new::rofi::rofi_comm::RofiComm;
use crate::lamellar_arch::LamellarArchRT;
use crate::scheduler::Scheduler;
use lamellar_prof::*;
use log::{error, trace};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

use async_trait::async_trait;

// mod rofi;

pub(crate) struct RofiBuilder{
    my_pe: usize,
    num_pes: usize,
    rofi_comm: Arc<RofiComm>
}

impl RofiBuilder{
    pub(crate) fn new(provider: &str)->RofiBuilder{
        let rofi_comm = Arc::new(RofiComm::new(provider));
        RofiBuilder{
            my_pe: rofi_comm.my_pe,
            num_pes: rofi_comm.num_pes,
            rofi_comm: rofi_comm
        }
    }
}


impl LamellaeInit for RofiBuilder{
    fn init_fabric(&mut self) -> (usize, usize){
        (self.my_pe, self.num_pes)
    }
    fn init_lamellae(&mut self, scheduler: Arc<Scheduler>) -> Lamellae{
        Lamellae::Rofi(Rofi::new(self.my_pe,self.num_pes,self.rofi_comm.clone(),scheduler.clone()))
    }
}


pub(crate) struct Rofi{
    my_pe: usize,
    num_pes: usize,
    rofi_comm: Arc<RofiComm>,
    scheduler: Arc<Scheduler>,
}
impl Rofi{
    fn new (my_pe: usize, num_pes: usize, rofi_comm: Arc<RofiComm>, scheduler: Arc<Scheduler>)->Rofi{
        Rofi{
            my_pe: my_pe,
            num_pes: num_pes,
            rofi_comm: rofi_comm,
            scheduler: scheduler
        }
    }
}




impl LamellaeComm for Rofi {
    // this is a global barrier (hopefully using hardware)
    fn my_pe(&self) -> usize {0}
    fn num_pes(&self) ->usize {0}
    fn barrier(&self) {}
    fn backend(&self) -> Backend {Backend::Rofi}
    #[allow(non_snake_case)]
    fn MB_sent(&self) -> f64 {0f64}
    fn print_stats(&self) {}
}

#[async_trait]
impl LamellaeAM for Rofi {
    fn send_to_pe(&self, pe: usize, data: std::vec::Vec<u8>) {} //should never send to self... this is short circuited before request is serialized in the active message layer
    fn send_to_pes(
        //should never send to self... this is short circuited before request is serialized in the active message layer
        &self,
        pe: Option<usize>,
        team: Arc<LamellarArchRT>,
        data: std::vec::Vec<u8>,
    ) {}
    async fn send_to_pes_async(&self,pe: Option<usize>, team: Arc<LamellarArchRT>, data: SerializedData) {}
}

impl LamellaeRDMA for Rofi {
    fn put(&self, pe: usize, src: &[u8], dst: usize) {}
    fn iput(&self, pe: usize, src: &[u8], dst: usize) {}
    fn put_all(&self, src: &[u8], dst: usize) {}
    fn get(&self, pe: usize, src: usize, dst: &mut [u8]) {}
    fn rt_alloc(&self, size: usize) -> Option<usize> {None}
    fn rt_free(&self, addr: usize) {}
    fn alloc(&self, size: usize, alloc: AllocationType) -> Option<usize> {None}
    fn free(&self, addr: usize) {}
    fn base_addr(&self) -> usize {0}
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize {0}
    fn remote_addr(&self, remote_pe: usize, local_addr: usize) -> usize {0}
}
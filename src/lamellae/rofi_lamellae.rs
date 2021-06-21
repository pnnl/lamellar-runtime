use crate::lamellae::{AllocationType, Backend, Lamellae, LamellaeInit, LamellaeComm, LamellaeAM, LamellaeRDMA, SerializedData,SerializeHeader,Ser,Des};
use crate::lamellae::rofi::rofi_comm::{RofiComm,RofiData};
use crate::lamellar_arch::LamellarArchRT;
use crate::scheduler::{Scheduler,SchedulerQueue};
use crate::lamellae::rofi::command_queues::RofiCommandQueue;

// use crate::active_messaging::Msg;

// use lamellar_prof::*;
// use log::{error, trace};
// use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool,  Ordering};
use std::sync::Arc;

use futures::stream::FuturesUnordered;
use futures::StreamExt;

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
    fn init_lamellae(&mut self, scheduler: Arc<Scheduler>) -> Arc<Lamellae>{
        let rofi = Rofi::new(self.my_pe,self.num_pes,self.rofi_comm.clone());
        let cq_clone = rofi.cq();
        let scheduler_clone = scheduler.clone();
        let active_clone = rofi.active();

        let rofi = Arc::new(Lamellae::Rofi(rofi));
        let rofi_clone = rofi.clone();
        scheduler.submit_task (async move {
            // cq_clone.recv_data(Arc::downgrade(&scheduler_clone),Arc::downgrade(&rofi_clone),active_clone).await;
            cq_clone.recv_data(scheduler_clone.clone(),rofi_clone.clone(),active_clone).await;
        });
        rofi
    }
    
}


pub(crate) struct Rofi{
    my_pe: usize,
    num_pes: usize,
    rofi_comm: Arc<RofiComm>,
    active: Arc<AtomicBool>,
    cq: Arc<RofiCommandQueue>,
}
impl Rofi{
    fn new (my_pe: usize, num_pes: usize, rofi_comm: Arc<RofiComm>)->Rofi{
        println!("my_pe {:?} num_pes {:?}",my_pe,num_pes);
         Rofi{
            my_pe: my_pe,
            num_pes: num_pes,
            rofi_comm: rofi_comm.clone(),
            active: Arc::new(AtomicBool::new(true)),
            cq: Arc::new(RofiCommandQueue::new(rofi_comm.clone(), my_pe, num_pes)),
        }
    }
    fn active(&self)->Arc<AtomicBool>{
        self.active.clone()
    }
    fn cq(&self) ->Arc<RofiCommandQueue>{
        self.cq.clone() 
    }
}

impl Drop for Rofi{
    fn drop(&mut self){
        println!("dropping rofi_lamellae");
        self.active.store(false, Ordering::SeqCst);
        println!("dropped rofi_lamellae");
        //rofi finit
    }
}




impl LamellaeComm for Rofi {
    // this is a global barrier (hopefully using hardware)
    fn my_pe(&self) -> usize {self.my_pe}
    fn num_pes(&self) ->usize {self.num_pes}
    fn barrier(&self) {self.rofi_comm.barrier()}
    fn backend(&self) -> Backend {Backend::Rofi}
    #[allow(non_snake_case)]
    fn MB_sent(&self) -> f64 {
        // println!("put: {:?} get: {:?}",self.rofi_comm.put_amt.load(Ordering::SeqCst),self.rofi_comm.get_amt.load(Ordering::SeqCst));
        // (self.rofi_comm.put_amt.load(Ordering::SeqCst) + self.rofi_comm.get_amt.load(Ordering::SeqCst)) as 
        self.cq.tx_amount() as f64 / 1_000_000.0 
    }
    fn print_stats(&self) {}
    fn shutdown(&self){
        // println!("Rofi Lamellae shuting down");
        self.active.store(false,Ordering::Relaxed);
        // println!("Rofi Lamellae shut down");
    }
}

#[async_trait]
impl LamellaeAM for Rofi {
    async fn send_to_pe_async(&self, pe: usize, data: SerializedData) {
        self.cq.send_data(data,pe).await;
    } //should never send to self... this is short circuited before request is serialized in the active message layer
    
    async fn send_to_pes_async(&self,pe: Option<usize>, team: Arc<LamellarArchRT>, data: SerializedData) {
        if let Some(pe) = pe {
            // self.cq.send_data(data,team.world_pe(pe).expect("invalid pe")).await;
            // println!("rofi_lamellae sending to {:?}",pe);
            self.cq.send_data(data,pe).await;
        }
        else{
            let mut futures = team.team_iter().filter(|pe| pe != &self.my_pe).map(|pe| self.cq.send_data(data.clone(),pe)).collect::<FuturesUnordered<_>>(); //in theory this launches all the futures before waiting...
            while let Some(_) = futures.next().await{}
        }
    }
}
#[async_trait]
impl Ser for Rofi{
    async fn serialize<T: Send + Sync + serde::Serialize + ?Sized>(&self,header: Option<SerializeHeader>, obj: &T) -> Result<SerializedData,anyhow::Error> {
        // let header_size_temp= if let Some(header) = &header {
        //     bincode::serialized_size(&header)? as usize //will this be standard for every header?
        // }
        // else{
        //     // println!("None! {:?}",bincode::serialized_size(&header)? as usize);
        //     // bincode::serialized_size(&header)? as usize
        //     0
        // };
        // let test: Option<SerializeHeader> = None;
        // let test_size = bincode::serialized_size(&test)? as usize;
        let header_size = std::mem::size_of::<Option<SerializeHeader>>();
        // println!("hst {:?} hs {:?} {:?} {:?} ",header_size_temp, header_size, test_size,std::mem::size_of::<SerializeHeader>()) ;
        let data_size = bincode::serialized_size(obj)? as usize;
        let ser_data = RofiData::new(self.rofi_comm.clone(),header_size+data_size).await;
        // println!("h+d size: {:?} hsize {:?} dsize {:?}",ser_data.alloc_size,ser_data.header_as_bytes().len(),ser_data.data_as_bytes().len());
        bincode::serialize_into(ser_data.header_as_bytes(),&header)?;
        bincode::serialize_into(ser_data.data_as_bytes(),obj)?;
        Ok(SerializedData::RofiData(ser_data))
    }
    async fn serialize_header(&self,header: Option<SerializeHeader>,serialized_size: usize) -> Result<SerializedData,anyhow::Error> {
        // let header_size_temp= if let Some(header) = &header {
        //     bincode::serialized_size(&header)? as usize //will this be standard for every header?
        // }
        // else{
        //     // println!("None! {:?}",bincode::serialized_size(&header)? as usize);
        //     // bincode::serialized_size(&header)? as usize
        //     0
        // };
        // let test: Option<SerializeHeader> = None;
        // let test_size = bincode::serialized_size(&test)? as usize;
        
        let header_size = std::mem::size_of::<Option<SerializeHeader>>();
        // println!("hs {:?}",header_size);
        // println!("hst {:?} hs {:?} {:?} {:?} ",header_size_temp, header_size, test_size,std::mem::size_of::<SerializeHeader>()) ;
        let ser_data = RofiData::new(self.rofi_comm.clone(),header_size+serialized_size).await;
        
        // println!("h+d size: {:?} hsize {:?} dsize {:?}",ser_data.alloc_size,ser_data.header_as_bytes().len(),ser_data.data_as_bytes().len());
        bincode::serialize_into(ser_data.header_as_bytes(),&header)?;
        // println!("header {:?} ({:?}) size {:?} occupied {:?}",header, ser_data.header_as_bytes(),header_size+serialized_size,self.rofi_comm.occupied());
        Ok(SerializedData::RofiData(ser_data))
    }
}



#[allow(dead_code,unused_variables)]
impl LamellaeRDMA for Rofi {
    fn put(&self, pe: usize, src: &[u8], dst: usize) {
        self.rofi_comm.put(pe, src, dst);
    }
    fn iput(&self, pe: usize, src: &[u8], dst: usize) {
        self.rofi_comm.iput(pe, src, dst);
    }
    fn put_all(&self, src: &[u8], dst: usize) {
        self.rofi_comm.put_all(src, dst);
    }
    fn get(&self, pe: usize, src: usize, dst: &mut [u8]) {
        self.rofi_comm.get(pe, src, dst)
    }
    fn rt_alloc(&self, size: usize) -> Option<usize> {
        self.rofi_comm.rt_alloc(size)
    }
    fn rt_free(&self, addr: usize) {
        self.rofi_comm.rt_free(addr)
    }
    fn alloc(&self, size: usize, alloc: AllocationType) -> Option<usize> {
        self.rofi_comm.alloc(size,alloc)
    }
    fn free(&self, addr: usize) {
        self.rofi_comm.free(addr)
    }
    fn base_addr(&self) -> usize {
        self.rofi_comm.base_addr()
    }
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize {
        self.rofi_comm.local_addr(remote_pe, remote_addr)
    }
    fn remote_addr(&self, remote_pe: usize, local_addr: usize) -> usize {
        self.rofi_comm.remote_addr(remote_pe, local_addr)
    }
}
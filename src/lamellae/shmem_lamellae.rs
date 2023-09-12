use crate::lamellae::comm::{AllocResult, CmdQStatus, CommOps};
use crate::lamellae::command_queues::CommandQueue;
use crate::lamellae::shmem::shmem_comm::*;

use crate::lamellae::{
    AllocationType, Backend, Comm, Des, Lamellae, LamellaeAM, LamellaeComm, LamellaeInit,
    LamellaeRDMA, Ser, SerializeHeader, SerializedData, SerializedDataOps, SERIALIZE_HEADER_LEN,
};
use crate::lamellar_arch::LamellarArchRT;
use crate::scheduler::{Scheduler, SchedulerQueue};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::StreamExt;

pub(crate) struct ShmemBuilder {
    my_pe: usize,
    num_pes: usize,
    shmem_comm: Arc<Comm>,
}

impl ShmemBuilder {
    pub(crate) fn new() -> ShmemBuilder {
        let shmem_comm: Arc<Comm> = Arc::new(ShmemComm::new().into());
        ShmemBuilder {
            my_pe: shmem_comm.my_pe(),
            num_pes: shmem_comm.num_pes(),
            shmem_comm: shmem_comm,
        }
    }
}

impl LamellaeInit for ShmemBuilder {
    fn init_fabric(&mut self) -> (usize, usize) {
        (self.my_pe, self.num_pes)
    }
    fn init_lamellae(&mut self, scheduler: Arc<Scheduler>) -> Arc<Lamellae> {
        let shmem = Shmem::new(self.my_pe, self.num_pes, self.shmem_comm.clone());
        let cq_clone = shmem.cq();
        let cq_clone2 = shmem.cq();
        let cq_clone3 = shmem.cq();
        let scheduler_clone = scheduler.clone();
        let scheduler_clone2 = scheduler.clone();
        let scheduler_clone3 = scheduler.clone();

        let shmem = Arc::new(Lamellae::Shmem(shmem));
        let shmem_clone = shmem.clone();
        scheduler.submit_task(async move {
            cq_clone
                .recv_data(scheduler_clone.clone(), shmem_clone.clone())
                .await;
        });

        scheduler.submit_task(async move {
            cq_clone2.alloc_task(scheduler_clone2.clone()).await;
        });

        scheduler.submit_task(async move {
            cq_clone3.panic_task(scheduler_clone3.clone()).await;
        });
        shmem
    }
}

pub(crate) struct Shmem {
    my_pe: usize,
    num_pes: usize,
    shmem_comm: Arc<Comm>,
    active: Arc<AtomicU8>,
    cq: Arc<CommandQueue>,
}

impl std::fmt::Debug for Shmem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Shmem {{ my_pe: {}, num_pes: {},  active: {:?} }}",
            self.my_pe, self.num_pes, self.active,
        )
    }
}

impl Shmem {
    fn new(my_pe: usize, num_pes: usize, shmem_comm: Arc<Comm>) -> Shmem {
        // println!("my_pe {:?} num_pes {:?}",my_pe,num_pes);
        let active = Arc::new(AtomicU8::new(CmdQStatus::Active as u8));
        Shmem {
            my_pe: my_pe,
            num_pes: num_pes,
            shmem_comm: shmem_comm.clone(),
            active: active.clone(),
            cq: Arc::new(CommandQueue::new(shmem_comm, my_pe, num_pes, active)),
        }
    }
    // fn active(&self) -> Arc<AtomicU8> {
    //     self.active.clone()
    // }
    fn cq(&self) -> Arc<CommandQueue> {
        self.cq.clone()
    }
}

// impl Drop for Shmem{
//     fn drop(&mut self){
//         // println!("dropping shmem_lamellae");
//         // self.active.store(0, Ordering::SeqCst);
//         // while self.active.load(Ordering::SeqCst) != 2 {
//         //     std::thread::yield_now();
//         // }
//         println!("dropped shmem_lamellae");
//         //shmem finit
//     }
// }

impl LamellaeComm for Shmem {
    // this is a global barrier (hopefully using hardware)
    fn my_pe(&self) -> usize {
        self.my_pe
    }
    fn num_pes(&self) -> usize {
        self.num_pes
    }
    fn barrier(&self) {
        self.shmem_comm.barrier()
    }
    fn backend(&self) -> Backend {
        Backend::Shmem
    }
    #[allow(non_snake_case)]
    fn MB_sent(&self) -> f64 {
        // println!("put: {:?} get: {:?}",self.shmem_comm.put_amt.load(Ordering::SeqCst),self.shmem_comm.get_amt.load(Ordering::SeqCst));
        // (self.shmem_comm.put_amt.load(Ordering::SeqCst) + self.shmem_comm.get_amt.load(Ordering::SeqCst)) as
        self.cq.tx_amount() as f64 / 1_000_000.0
    }
    fn print_stats(&self) {}
    fn shutdown(&self) {
        // println!("Shmem Lamellae shuting down");
        let _ = self.active.compare_exchange(
            CmdQStatus::Active as u8,
            CmdQStatus::ShuttingDown as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        // println!("set active to 0");
        while self.active.load(Ordering::SeqCst) != CmdQStatus::Finished as u8
            && self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8
        {
            std::thread::yield_now();
        }
        // println!("Shmem Lamellae shut down");
    }

    fn force_shutdown(&self) {
        self.cq.send_panic();
        self.active
            .store(CmdQStatus::Panic as u8, Ordering::Relaxed);
    }
    fn force_deinit(&self) {
        self.shmem_comm.force_shutdown();
    }
}

#[async_trait]
impl LamellaeAM for Shmem {
    async fn send_to_pe_async(&self, pe: usize, data: SerializedData) {
        self.cq.send_data(data, pe).await;
    } //should never send to self... this is short circuited before request is serialized in the active message layer

    async fn send_to_pes_async(
        &self,
        pe: Option<usize>,
        team: Arc<LamellarArchRT>,
        data: SerializedData,
    ) {
        if let Some(pe) = pe {
            self.cq.send_data(data, pe).await;
        } else {
            let mut futures = team
                .team_iter()
                .filter(|pe| pe != &self.my_pe)
                .map(|pe| self.cq.send_data(data.clone(), pe))
                .collect::<FuturesUnordered<_>>(); //in theory this launches all the futures before waiting...
            while let Some(_) = futures.next().await {}
        }
    }
}

impl Ser for Shmem {
    fn serialize<T: serde::Serialize + ?Sized>(
        &self,
        header: Option<SerializeHeader>,
        obj: &T,
    ) -> Result<SerializedData, anyhow::Error> {
        let header_size = *SERIALIZE_HEADER_LEN;
        let data_size = crate::serialized_size(obj, true) as usize;
        let ser_data = ShmemData::new(self.shmem_comm.clone(), header_size + data_size)?;
        crate::serialize_into(ser_data.header_as_bytes(), &header, false)?; //we want header to be a fixed size
        crate::serialize_into(ser_data.data_as_bytes(), obj, true)?;
        Ok(SerializedData::ShmemData(ser_data))
    }
    fn serialize_header(
        &self,
        header: Option<SerializeHeader>,
        serialized_size: usize,
    ) -> Result<SerializedData, anyhow::Error> {
        let header_size = *SERIALIZE_HEADER_LEN;
        let ser_data = ShmemData::new(self.shmem_comm.clone(), header_size + serialized_size)?;
        crate::serialize_into(ser_data.header_as_bytes(), &header, false)?; //we want header to be a fixed size
        Ok(SerializedData::ShmemData(ser_data))
    }
}

#[allow(dead_code, unused_variables)]
impl LamellaeRDMA for Shmem {
    fn put(&self, pe: usize, src: &[u8], dst: usize) {
        self.shmem_comm.put(pe, src, dst);
    }
    fn iput(&self, pe: usize, src: &[u8], dst: usize) {
        self.shmem_comm.iput(pe, src, dst);
    }
    fn put_all(&self, src: &[u8], dst: usize) {
        self.shmem_comm.put_all(src, dst);
    }
    fn get(&self, pe: usize, src: usize, dst: &mut [u8]) {
        self.shmem_comm.get(pe, src, dst);
    }
    fn iget(&self, pe: usize, src: usize, dst: &mut [u8]) {
        self.shmem_comm.get(pe, src, dst);
    }
    fn rt_alloc(&self, size: usize) -> AllocResult<usize> {
        self.shmem_comm.rt_alloc(size)
    }
    fn rt_check_alloc(&self, size: usize) -> bool {
        self.shmem_comm.rt_check_alloc(size)
    }
    fn rt_free(&self, addr: usize) {
        self.shmem_comm.rt_free(addr)
    }
    fn alloc(&self, size: usize, alloc: AllocationType) -> AllocResult<usize> {
        self.shmem_comm.alloc(size, alloc)
    }
    fn free(&self, addr: usize) {
        self.shmem_comm.free(addr)
    }
    fn base_addr(&self) -> usize {
        self.shmem_comm.base_addr()
    }
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize {
        self.shmem_comm.local_addr(remote_pe, remote_addr)
    }
    fn remote_addr(&self, remote_pe: usize, local_addr: usize) -> usize {
        self.shmem_comm.remote_addr(remote_pe, local_addr)
    }
    fn occupied(&self) -> usize {
        self.shmem_comm.occupied()
    }
    fn num_pool_allocs(&self) -> usize {
        self.shmem_comm.num_pool_allocs()
    }
    fn alloc_pool(&self, min_size: usize) {
        self.cq.send_alloc(min_size);
    }
}

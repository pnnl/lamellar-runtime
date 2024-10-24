use crate::env_var::{config, HeapMode};
use crate::lamellae::comm::{AllocResult, CmdQStatus, CommOps};
use crate::lamellae::command_queues::CommandQueue;
use crate::lamellae::rofi_rust_async::rofi_rust_async_comm::{
    RofiRustAsyncComm, RofiRustAsyncData,
};
use crate::lamellae::{
    AllocationType, Backend, Comm, Lamellae, LamellaeAM, LamellaeComm, LamellaeInit, LamellaeRDMA,
    Ser, SerializeHeader, SerializedData, SerializedDataOps, SERIALIZE_HEADER_LEN,
};
use crate::lamellar_arch::LamellarArchRT;
use crate::scheduler::Scheduler;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;

pub(crate) struct RofiRustAsyncBuilder {
    my_pe: usize,
    num_pes: usize,
    libfab_comm: Arc<Comm>,
}

impl RofiRustAsyncBuilder {
    pub(crate) fn new(provider: &str, domain: &str) -> RofiRustAsyncBuilder {
        let provider = if !provider.is_empty() {
            Some(provider)
        } else {
            None
        };
        let domain = if !domain.is_empty() {
            Some(domain)
        } else {
            None
        };
        let libfab: Arc<Comm> = Arc::new(RofiRustAsyncComm::new(provider, domain).unwrap().into());
        RofiRustAsyncBuilder {
            my_pe: libfab.my_pe(),
            num_pes: libfab.num_pes(),
            libfab_comm: libfab,
        }
    }
}

impl LamellaeInit for RofiRustAsyncBuilder {
    fn init_fabric(&mut self) -> (usize, usize) {
        (self.my_pe, self.num_pes)
    }
    fn init_lamellae(&mut self, scheduler: Arc<Scheduler>) -> Arc<Lamellae> {
        let libfab = RofiRustAsync::new(self.my_pe, self.num_pes, self.libfab_comm.clone());
        let cq_clone = libfab.cq();
        let cq_clone2 = libfab.cq();
        let cq_clone3 = libfab.cq();
        let scheduler_clone = scheduler.clone();
        let scheduler_clone2 = scheduler.clone();
        let scheduler_clone3 = scheduler.clone();

        let libfab = Arc::new(Lamellae::RofiRustAsync(libfab));
        let libfab_clone = libfab.clone();
        println!("Submitting Rofi Tasks");
        scheduler.submit_io_task(async move {
            println!("ROFI RECV DATA TASK");
            cq_clone
                .recv_data(scheduler_clone.clone(), libfab_clone.clone())
                .await;
            println!("ROFI RECV DATA DONE");
        });
        scheduler.submit_io_task(async move {
            println!("ROFI ALLOC TASK");
            cq_clone2.alloc_task(scheduler_clone2.clone()).await;
            println!("ROFI ALLOC DONE");
        });
        scheduler.submit_io_task(async move {
            println!("ROFI PANIC TASK");
            cq_clone3.panic_task(scheduler_clone3.clone()).await;
            println!("ROFI PANIC DONE");
        });
        libfab
    }
}

pub(crate) struct RofiRustAsync {
    my_pe: usize,
    num_pes: usize,
    libfab_comm: Arc<Comm>,
    active: Arc<AtomicU8>,
    cq: Arc<CommandQueue>,
}

impl std::fmt::Debug for RofiRustAsync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Rofi {{ my_pe: {}, num_pes: {},  active: {:?} }}",
            self.my_pe, self.num_pes, self.active,
        )
    }
}

impl RofiRustAsync {
    fn new(my_pe: usize, num_pes: usize, libfab_comm: Arc<Comm>) -> RofiRustAsync {
        // println!("my_pe {:?} num_pes {:?}",my_pe,num_pes);
        let active = Arc::new(AtomicU8::new(CmdQStatus::Active as u8));
        RofiRustAsync {
            my_pe: my_pe,
            num_pes: num_pes,
            libfab_comm: libfab_comm.clone(),
            active: active.clone(),
            cq: Arc::new(CommandQueue::new(libfab_comm, my_pe, num_pes, active)),
        }
    }
    // fn active(&self) -> Arc<AtomicU8> {
    //     self.active.clone()
    // }
    fn cq(&self) -> Arc<CommandQueue> {
        self.cq.clone()
    }
}

// impl Drop for Rofi {
//     fn drop(&mut self) {
//         println!("dropping rofi_lamellae");
//         // self.active.store(0, Ordering::SeqCst);
//         // while self.active.load(Ordering::SeqCst) != 2 {
//         //     std::thread::yield_now();
//         // }
//         println!("dropped rofi_lamellae");
//         //rofi finit
//     }
// }

impl LamellaeComm for RofiRustAsync {
    // this is a global barrier (hopefully using hardware)
    fn my_pe(&self) -> usize {
        self.my_pe
    }
    fn num_pes(&self) -> usize {
        self.num_pes
    }
    async fn barrier(&self) {
        self.libfab_comm.barrier().await
    }
    fn backend(&self) -> Backend {
        Backend::RofiRustAsync
    }
    #[allow(non_snake_case)]
    fn MB_sent(&self) -> f64 {
        // println!("put: {:?} get: {:?}",self.rofi_comm.put_amt.load(Ordering::SeqCst),self.rofi_comm.get_amt.load(Ordering::SeqCst));
        self.libfab_comm.MB_sent()
        //+ self.cq.tx_amount() as f64 / 1_000_000.0
    }
    // fn print_stats(&self) {}
    fn shutdown(&self) {
        // println!("Rofi Lamellae shuting down");
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
        // println!("Rofi Lamellae shut down");
    }

    fn force_shutdown(&self) {
        if self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8 {
            self.active
                .store(CmdQStatus::Panic as u8, Ordering::Relaxed);
            self.cq.send_panic();
        }
    }

    fn force_deinit(&self) {
        self.libfab_comm.force_shutdown();
    }
}

#[async_trait]
impl LamellaeAM for RofiRustAsync {
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

impl Ser for RofiRustAsync {
    // fn serialize<T: serde::Serialize + ?Sized>(
    //     &self,
    //     header: Option<SerializeHeader>,
    //     obj: &T,
    // ) -> Result<SerializedData, anyhow::Error> {
    //     let header_size = *SERIALIZE_HEADER_LEN;
    //     // let data_size = bincode::serialized_size(obj)? as usize;
    //     let data_size = crate::serialized_size(obj, true) as usize;
    //     let ser_data = RofiRustAsyncData::new(self.rofi_comm.clone(), header_size + data_size)?;
    //     crate::serialize_into(ser_data.header_as_bytes(), &header, false)?; //we want header to be a fixed size
    //     crate::serialize_into(ser_data.data_as_bytes(), obj, true)?;
    //     Ok(SerializedData::RofiRustAsyncData(ser_data))
    // }
    fn serialize_header(
        &self,
        header: Option<SerializeHeader>,
        serialized_size: usize,
    ) -> Result<SerializedData, anyhow::Error> {
        let header_size = *SERIALIZE_HEADER_LEN;
        let ser_data =
            RofiRustAsyncData::new(self.libfab_comm.clone(), header_size + serialized_size)?;
        // bincode::serialize_into(ser_data.header_as_bytes(), &header)?;
        crate::serialize_into(ser_data.header_as_bytes(), &header, false)?; //we want header to be a fixed size
        Ok(SerializedData::RofiRustAsyncData(ser_data))
    }
}

#[async_trait]
#[allow(dead_code, unused_variables)]
impl LamellaeRDMA for RofiRustAsync {
    fn flush(&self) {
        self.libfab_comm.flush();
    }
    async fn put(&self, pe: usize, src: &[u8], dst: usize) {
        self.libfab_comm.put(pe, src, dst).await;
    }
    fn iput(&self, pe: usize, src: &[u8], dst: usize) {
        self.libfab_comm.iput(pe, src, dst);
    }
    async fn put_all(&self, src: &[u8], dst: usize) {
        self.libfab_comm.put_all(src, dst).await;
    }
    async fn get(&self, pe: usize, src: usize, dst: &mut [u8]) {
        self.libfab_comm.get(pe, src, dst).await;
    }
    fn iget(&self, pe: usize, src: usize, dst: &mut [u8]) {
        self.libfab_comm.iget(pe, src, dst);
    }
    fn rt_alloc(&self, size: usize, align: usize) -> AllocResult<usize> {
        self.libfab_comm.rt_alloc(size, align)
    }
    // fn rt_check_alloc(&self, size: usize, align: usize) -> bool {
    //     self.rofi_comm.rt_check_alloc(size, align)
    // }
    fn rt_free(&self, addr: usize) {
        self.libfab_comm.rt_free(addr)
    }
    async fn alloc(&self, size: usize, alloc: AllocationType, align: usize) -> AllocResult<usize> {
        self.libfab_comm.alloc(size, alloc).await
    }
    fn free(&self, addr: usize) {
        self.libfab_comm.free(addr)
    }
    fn base_addr(&self) -> usize {
        self.libfab_comm.base_addr()
    }
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize {
        self.libfab_comm.local_addr(remote_pe, remote_addr)
    }
    fn remote_addr(&self, remote_pe: usize, local_addr: usize) -> usize {
        self.libfab_comm.remote_addr(remote_pe, local_addr)
    }
    // fn occupied(&self) -> usize {
    //     self.rofi_comm.occupied()
    // }
    // fn num_pool_allocs(&self) -> usize {
    //     self.rofi_comm.num_pool_allocs()
    // }
    fn alloc_pool(&self, min_size: usize) {
        // println!("trying to alloc pool {:?}",min_size);
        match config().heap_mode {
            HeapMode::Static => {
                panic!("[LAMELLAR ERROR] Heap out of memory, current heap size is {} bytes,set LAMELLAR_HEAP_SIZE envrionment variable to increase size, or set LAMELLAR_HEAP_MODE=dynamic to enable exprimental growable heaps",RofiRustAsyncComm::heap_size())
            }
            HeapMode::Dynamic => self.cq.send_alloc(min_size),
        }
    }
}
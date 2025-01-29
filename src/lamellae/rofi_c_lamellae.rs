pub(crate) mod atomic;
pub(crate) mod comm;
pub(crate) mod fabric;
pub(crate) mod mem;
pub(crate) mod rdma;

use super::{
    comm::{CmdQStatus, CommInfo, CommShutdown},
    command_queues::CommandQueue,
    Comm, Lamellae, LamellaeAM, LamellaeInit, LamellaeShutdown, Ser, SerializeHeader,
    SerializedData, SERIALIZE_HEADER_LEN,
};
use crate::{lamellar_arch::LamellarArchRT, scheduler::Scheduler};
use comm::RofiCComm;

use async_trait::async_trait;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

pub(crate) struct RofiCBuilder {
    my_pe: usize,
    num_pes: usize,
    rofi_c_comm: Arc<Comm>,
}

impl RofiCBuilder {
    pub(crate) fn new(provider: &str, domain: &str) -> RofiCBuilder {
        let rofi_c_comm: Arc<Comm> = Arc::new(RofiCComm::new(provider, domain).into());
        RofiCBuilder {
            my_pe: rofi_c_comm.my_pe(),
            num_pes: rofi_c_comm.num_pes(),
            rofi_c_comm: rofi_c_comm,
        }
    }
}

impl LamellaeInit for RofiCBuilder {
    fn init_fabric(&mut self) -> (usize, usize) {
        (self.my_pe, self.num_pes)
    }
    fn init_lamellae(&mut self, scheduler: Arc<Scheduler>) -> Arc<Lamellae> {
        let rofi_c = RofiC::new(self.my_pe, self.num_pes, self.rofi_c_comm.clone(),scheduler.clone());
        let cq = rofi_c.cq();
        let rofi_c = Arc::new(Lamellae::RofiC(rofi_c));
        let rofi_c_clone = rofi_c.clone();
        let cq_clone = cq.clone();
        scheduler.submit_task(async move {
            cq_clone.recv_data(rofi_c_clone.clone()).await;
        });

        let cq_clone = cq.clone();
        scheduler.submit_task(async move {
            cq_clone.alloc_task().await;
        });
        let cq_clone = cq.clone();
        scheduler.submit_task(async move {
            cq_clone.panic_task().await;
        });
        rofi_c
    }
}

pub(crate) struct RofiC {
    my_pe: usize,
    num_pes: usize,
    rofi_c_comm: Arc<Comm>,
    active: Arc<AtomicU8>,
    cq: Arc<CommandQueue>,
}

impl std::fmt::Debug for RofiC {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RofiC {{ my_pe: {}, num_pes: {},  active: {:?} }}",
            self.my_pe, self.num_pes, self.active,
        )
    }
}

impl RofiC {
    fn new(my_pe: usize, num_pes: usize, rofi_c_comm: Arc<Comm>, scheduler: Arc<Scheduler>,) -> RofiC {
        // println!("my_pe {:?} num_pes {:?}",my_pe,num_pes);
        let active = Arc::new(AtomicU8::new(CmdQStatus::Active as u8));
        RofiC {
            my_pe: my_pe,
            num_pes: num_pes,
            rofi_c_comm: rofi_c_comm.clone(),
            active: active.clone(),
            cq: Arc::new(CommandQueue::new(rofi_c_comm, scheduler,my_pe, num_pes, active)),
        }
    }
    fn cq(&self) -> Arc<CommandQueue> {
        self.cq.clone()
    }
    pub(crate) fn comm(&self) -> &Comm {
        &self.rofi_c_comm
    }
}

impl LamellaeShutdown for RofiC {
    fn shutdown(&self) {
        // println!("rofi_c Lamellae shuting down");
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
        // println!("rofi_c Lamellae shut down");
    }

    fn force_shutdown(&self) {
        self.cq.send_panic();
        self.active
            .store(CmdQStatus::Panic as u8, Ordering::Relaxed);
    }
    fn force_deinit(&self) {
        self.rofi_c_comm.force_shutdown();
    }
}


#[async_trait]
impl LamellaeAM for RofiC {
    async fn send_to_pes_async(
        &self,
        pe: Option<usize>,
        team: Arc<LamellarArchRT>,
        data: SerializedData,
    ) {
        let remote_data = data.into_remote();
        if let Some(pe) = pe {
            self.cq.send_data(remote_data, pe).await;
        } else {
            let mut futures = team
                .team_iter()
                .filter(|pe| pe != &self.my_pe)
                .map(|pe| self.cq.send_data(remote_data.clone(), pe))
                .collect::<FuturesUnordered<_>>(); //in theory this launches all the futures before waiting...
            while let Some(_) = futures.next().await {}
        }
    }
}

impl Ser for RofiC {
    fn serialize_header(
        &self,
        header: Option<SerializeHeader>,
        serialized_size: usize,
    ) -> Result<SerializedData, anyhow::Error> {
        let header_size = *SERIALIZE_HEADER_LEN;
        let mut ser_data =
            SerializedData::new(self.rofi_c_comm.clone(), header_size + serialized_size)?;
        crate::serialize_into(&mut ser_data.header_as_bytes_mut(), &header, false)?; //we want header to be a fixed size
        Ok(ser_data)
    }
}

pub(crate) mod atomic;
pub(crate) mod comm;
pub(crate) mod fabric;
pub(crate) mod mem;
pub(crate) mod rdma;

use super::{
    comm::{CmdQStatus, CommInfo, CommShutdown},
    command_queues::CommandQueue,
    Comm, Lamellae, LamellaeInit, LamellaeShutdown, LamellaeUtil, Ser, SerializeHeader,
    SerializedData, SERIALIZE_HEADER_LEN,
};
use crate::{config, env_var::HeapMode, lamellar_arch::LamellarArchRT, scheduler::Scheduler};
use comm::UcxComm;

use async_trait::async_trait;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use tracing::trace;

pub(crate) struct UcxBuilder {
    my_pe: usize,
    num_pes: usize,
    ucx_comm: Arc<Comm>,
}

impl UcxBuilder {
    pub(crate) fn new() -> UcxBuilder {
        let ucx_comm: Arc<Comm> = Arc::new(UcxComm::new().into());
        UcxBuilder {
            my_pe: ucx_comm.my_pe(),
            num_pes: ucx_comm.num_pes(),
            ucx_comm: ucx_comm,
        }
    }
}

impl LamellaeInit for UcxBuilder {
    fn init_fabric(&mut self) -> (usize, usize) {
        (self.my_pe, self.num_pes)
    }
    fn init_lamellae(&mut self, scheduler: Arc<Scheduler>) -> Arc<Lamellae> {
        let ucx = Ucx::new(
            self.my_pe,
            self.num_pes,
            self.ucx_comm.clone(),
            scheduler.clone(),
        );
        trace!("created new ucx instance");
        let cq = ucx.cq();
        trace!("created command queue for ucx");
        let ucx = Arc::new(Lamellae::Ucx(ucx));
        let ucx_clone = ucx.clone();
        let cq_clone = cq.clone();
        scheduler.submit_task(async move {
            cq_clone.recv_data(ucx_clone.clone()).await;
        });

        let cq_clone = cq.clone();
        scheduler.submit_task(async move {
            cq_clone.alloc_task().await;
        });
        let cq_clone = cq.clone();
        scheduler.submit_task(async move {
            cq_clone.panic_task().await;
        });
        ucx
    }
}

pub(crate) struct Ucx {
    my_pe: usize,
    num_pes: usize,
    ucx_comm: Arc<Comm>,
    active: Arc<AtomicU8>,
    cq: Arc<CommandQueue>,
}

impl std::fmt::Debug for Ucx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Ucx {{ my_pe: {}, num_pes: {},  active: {:?} }}",
            self.my_pe, self.num_pes, self.active,
        )
    }
}

impl Ucx {
    fn new(my_pe: usize, num_pes: usize, ucx_comm: Arc<Comm>, scheduler: Arc<Scheduler>) -> Ucx {
        // println!("my_pe {:?} num_pes {:?}",my_pe,num_pes);
        let active = Arc::new(AtomicU8::new(CmdQStatus::Active as u8));
        Ucx {
            my_pe: my_pe,
            num_pes: num_pes,
            ucx_comm: ucx_comm.clone(),
            active: active.clone(),
            cq: Arc::new(CommandQueue::new(
                ucx_comm, scheduler, my_pe, num_pes, active,
            )),
        }
    }
    fn cq(&self) -> Arc<CommandQueue> {
        self.cq.clone()
    }
    pub(crate) fn comm(&self) -> &Comm {
        &self.ucx_comm
    }
}

impl LamellaeShutdown for Ucx {
    fn shutdown(&self) {
        // println!("ucx Lamellae shuting down");
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
        // println!("ucx Lamellae shut down");
    }

    fn force_shutdown(&self) {
        self.cq.send_panic();
        self.active
            .store(CmdQStatus::Panic as u8, Ordering::Relaxed);
    }
    fn force_deinit(&self) {
        self.ucx_comm.force_shutdown();
    }
}

#[async_trait]
impl LamellaeUtil for Ucx {
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
    async fn request_new_alloc(&self, min_size: usize) {
        if config().heap_mode == HeapMode::Static {
            panic!("Error: request_new_alloc should not be called in static heap mode, please set LAMELLAR_HEAP_MODE=dynamic or increase the heap size with LAMELLAR_HEAP_SIZE environment variable");
        }
        // println!("Requesting new pool of size: {} bytes", min_size);
        self.cq.send_alloc(min_size).await;
    }
}

impl Ser for Ucx {
    fn serialize_header(
        &self,
        header: Option<SerializeHeader>,
        serialized_size: usize,
    ) -> Result<SerializedData, anyhow::Error> {
        let header_size = *SERIALIZE_HEADER_LEN;
        let mut ser_data =
            SerializedData::new(self.ucx_comm.clone(), header_size + serialized_size)?;
        crate::serialize_into(&mut ser_data.header_as_bytes_mut(), &header, false)?; //we want header to be a fixed size
        Ok(ser_data)
    }
}

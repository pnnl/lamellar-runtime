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
use comm::ShmemComm;

use async_trait::async_trait;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

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
        let shmem = Shmem::new(
            self.my_pe,
            self.num_pes,
            self.shmem_comm.clone(),
            scheduler.clone(),
        );
        let cq = shmem.cq();
        let shmem = Arc::new(Lamellae::Shmem(shmem));
        let shmem_clone = shmem.clone();
        let cq_clone = cq.clone();
        scheduler.submit_task(async move {
            cq_clone.recv_data(shmem_clone.clone()).await;
        });

        let cq_clone = cq.clone();
        scheduler.submit_task(async move {
            cq_clone.alloc_task().await;
        });
        let cq_clone = cq.clone();
        scheduler.submit_task(async move {
            cq_clone.panic_task().await;
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
    fn new(
        my_pe: usize,
        num_pes: usize,
        shmem_comm: Arc<Comm>,
        scheduler: Arc<Scheduler>,
    ) -> Shmem {
        // println!("my_pe {:?} num_pes {:?}",my_pe,num_pes);
        let active = Arc::new(AtomicU8::new(CmdQStatus::Active as u8));
        Shmem {
            my_pe: my_pe,
            num_pes: num_pes,
            shmem_comm: shmem_comm.clone(),
            active: active.clone(),
            cq: Arc::new(CommandQueue::new(
                shmem_comm, scheduler, my_pe, num_pes, active,
            )),
        }
    }
    fn cq(&self) -> Arc<CommandQueue> {
        self.cq.clone()
    }

    pub(crate) fn comm(&self) -> &Comm {
        &self.shmem_comm
    }
}

impl LamellaeShutdown for Shmem {
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
impl LamellaeUtil for Shmem {
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

impl Ser for Shmem {
    #[tracing::instrument(skip_all, level = "debug")]
    fn serialize_header(
        &self,
        header: Option<SerializeHeader>,
        serialized_size: usize,
    ) -> Result<SerializedData, anyhow::Error> {
        // trace!("serialize header");
        let header_size = *SERIALIZE_HEADER_LEN;
        let mut ser_data =
            SerializedData::new(self.shmem_comm.clone(), header_size + serialized_size)?;
        crate::serialize_into(&mut ser_data.header_as_bytes_mut(), &header, false)?; //we want header to be a fixed size
        Ok(ser_data)
    }
}

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
use comm::LibfabricMtComm;

use async_trait::async_trait;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use tracing::trace;

pub(crate) struct LibfabricMtBuilder {
    my_pe: usize,
    num_pes: usize,
    libfabric_comm: Arc<Comm>,
}

impl LibfabricMtBuilder {
    pub(crate) fn new(provider: &str, domain: &str, num_threads: usize) -> LibfabricMtBuilder {
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
        let libfabric_comm: Arc<Comm> =
            Arc::new(LibfabricMtComm::new(provider, domain, num_threads).into());
        LibfabricMtBuilder {
            my_pe: libfabric_comm.my_pe(),
            num_pes: libfabric_comm.num_pes(),
            libfabric_comm: libfabric_comm,
        }
    }
}

impl LamellaeInit for LibfabricMtBuilder {
    fn init_fabric(&mut self) -> (usize, usize) {
        (self.my_pe, self.num_pes)
    }
    fn init_lamellae(&mut self, scheduler: Arc<Scheduler>) -> Arc<Lamellae> {
        let libfabric = LibfabricMt::new(
            self.my_pe,
            self.num_pes,
            self.libfabric_comm.clone(),
            scheduler.clone(),
        );
        trace!("created new libfabric_mt instance");
        let cq = libfabric.cq();
        trace!("created command queue for libfabric_mt");
        let libfabric = Arc::new(Lamellae::LibfabricMt(libfabric));
        let libfabric_clone = libfabric.clone();
        let cq_clone = cq.clone();
        scheduler.submit_task(async move {
            cq_clone.recv_data(libfabric_clone.clone()).await;
        });

        let cq_clone = cq.clone();
        scheduler.submit_task(async move {
            cq_clone.alloc_task().await;
        });
        let cq_clone = cq.clone();
        scheduler.submit_task(async move {
            cq_clone.panic_task().await;
        });
        libfabric
    }
}

pub(crate) struct LibfabricMt {
    my_pe: usize,
    num_pes: usize,
    libfabric_comm: Arc<Comm>,
    active: Arc<AtomicU8>,
    cq: Arc<CommandQueue>,
}

impl std::fmt::Debug for LibfabricMt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LibfabricMt {{ my_pe: {}, num_pes: {},  active: {:?} }}",
            self.my_pe, self.num_pes, self.active,
        )
    }
}

impl LibfabricMt {
    fn new(
        my_pe: usize,
        num_pes: usize,
        libfabric_comm: Arc<Comm>,
        scheduler: Arc<Scheduler>,
    ) -> LibfabricMt {
        // println!("my_pe {:?} num_pes {:?}",my_pe,num_pes);
        let active = Arc::new(AtomicU8::new(CmdQStatus::Active as u8));
        LibfabricMt {
            my_pe: my_pe,
            num_pes: num_pes,
            libfabric_comm: libfabric_comm.clone(),
            active: active.clone(),
            cq: Arc::new(CommandQueue::new(
                libfabric_comm,
                scheduler,
                my_pe,
                num_pes,
                active,
            )),
        }
    }
    fn cq(&self) -> Arc<CommandQueue> {
        self.cq.clone()
    }
    pub(crate) fn wait_all_print(&self) {
        self.cq.wait_all_print();
    }
    pub(crate) fn comm(&self) -> &Comm {
        &self.libfabric_comm
    }
}

impl LamellaeShutdown for LibfabricMt {
    fn shutdown(&self) {
        // println!("libfabric Lamellae shuting down");
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
        // println!("libfabric Lamellae shut down");
    }

    fn force_shutdown(&self) {
        self.cq.send_panic();
        self.active
            .store(CmdQStatus::Panic as u8, Ordering::Relaxed);
    }
    fn force_deinit(&self) {
        self.libfabric_comm.force_shutdown();
    }
}

#[async_trait]
impl LamellaeUtil for LibfabricMt {
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

    async fn request_new_alloc(&self, min_size: usize) {
        if config().heap_mode == HeapMode::Static {
            panic!("Error: request_new_alloc should not be called in static heap mode, please set LAMELLAR_HEAP_MODE=dynamic or increase the heap size with LAMELLAR_HEAP_SIZE environment variable");
        }
        // println!("Requesting new pool of size: {} bytes", min_size);
        self.cq.send_alloc(min_size).await;
    }
}

impl Ser for LibfabricMt {
    fn serialize_header(
        &self,
        header: Option<SerializeHeader>,
        serialized_size: usize,
    ) -> Result<SerializedData, anyhow::Error> {
        let header_size = *SERIALIZE_HEADER_LEN;
        let mut ser_data =
            SerializedData::new(self.libfabric_comm.clone(), header_size + serialized_size)?;
        crate::serialize_into(&mut ser_data.header_as_bytes_mut(), &header, false)?; //we want header to be a fixed size
        Ok(ser_data)
    }
}

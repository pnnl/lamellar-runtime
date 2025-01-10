pub(crate) mod atomic;
pub(crate) mod comm;
pub(crate) mod mem;
pub(crate) mod rdma;

use super::{
    AllocationType, Comm, CommInfo, Lamellae, LamellaeAM, LamellaeInit, Ser, SerializeHeader,
    SerializedData,
};
use crate::{lamellar_arch::LamellarArchRT, scheduler::Scheduler};
use comm::LocalComm;

use async_trait::async_trait;
use std::sync::Arc;

pub(crate) struct LocalBuilder {
    my_pe: usize,
    num_pes: usize,
    local_comm: Arc<Comm>,
}

impl LocalBuilder {
    pub(crate) fn new() -> LocalBuilder {
        let local_comm: Arc<Comm> = Arc::new(LocalComm::new().into());
        LocalBuilder {
            my_pe: local_comm.my_pe(),
            num_pes: local_comm.num_pes(),
            local_comm,
        }
    }
}

impl LamellaeInit for LocalBuilder {
    fn init_fabric(&mut self) -> (usize, usize) {
        (self.my_pe, self.num_pes)
    }
    fn init_lamellae(&mut self, scheduler: Arc<Scheduler>) -> Arc<Lamellae> {
        Arc::new(Lamellae::Local(Local::new(self.local_comm.clone())))
    }
}

#[derive(Clone)]
pub(crate) struct Local {
    local_comm: Arc<Comm>,
}

impl std::fmt::Debug for Local {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Local")
    }
}

impl Local {
    pub(crate) fn new(local_comm: Arc<Comm>) -> Local {
        Local { local_comm }
    }

    pub(crate) fn comm(&self) -> &Comm{
        &self.local_comm
    }
}

// #[async_trait]
impl Ser for Local {
    fn serialize_header(
        &self,
        _header: Option<SerializeHeader>,
        _serialized_size: usize,
    ) -> Result<SerializedData, anyhow::Error> {
        panic!("should not be serializing in local")
    }
}

impl LamellaeInit for Local {
    fn init_fabric(&mut self) -> (usize, usize) {
        (0, 1)
    }
    fn init_lamellae(&mut self, _scheduler: Arc<Scheduler>) -> Arc<Lamellae> {
        Arc::new(Lamellae::Local(self.clone()))
    }
}

#[async_trait]
impl LamellaeAM for Local {
    async fn send_to_pes_async(
        &self,
        _pe: Option<usize>,
        _team: Arc<LamellarArchRT>,
        _data: SerializedData,
    ) {
    }
}

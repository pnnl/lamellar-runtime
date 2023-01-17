use crate::active_messaging::registered_active_message::AmId;
use crate::active_messaging::*;

pub(crate) mod simple_batcher;
use simple_batcher::SimpleBatcher;

pub(crate) mod team_am_batcher;
use team_am_batcher::TeamAmBatcher;

use async_trait::async_trait;

#[derive(Clone)]
enum LamellarData {
    Am(LamellarArcAm, AmId),
    Return(LamellarArcAm, AmId),
    Data(LamellarResultArc, Vec<RemotePtr>, usize),
    Unit,
}

impl std::fmt::Debug for LamellarData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LamellarData::Am(_, _) => write!(f, "Am"),
            LamellarData::Return(_, _) => write!(f, "Return"),
            LamellarData::Data(_, _, _) => write!(f, "Data"),
            LamellarData::Unit => write!(f, "Unit"),
        }
    }
}

#[async_trait]
pub(crate) trait Batcher {
    fn add_remote_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_size: usize,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
        stall_mark: usize,
    );
    fn add_return_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_size: usize,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
        stall_mark: usize,
    );
    fn add_data_am_to_batch(
        &self,
        req_data: ReqMetaData,
        data: LamellarResultArc,
        data_size: usize,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
        stall_mark: usize,
    );
    fn add_unit_am_to_batch(
        &self,
        req_data: ReqMetaData,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
        stall_mark: usize,
    );

    async fn exec_batched_msg(
        &self,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
        ame: &RegisteredActiveMessages,
    );
}

#[derive(Debug)]
pub(crate) enum BatcherType {
    Simple(SimpleBatcher),
    TeamAm(TeamAmBatcher),
}

#[async_trait]
impl Batcher for BatcherType {
    #[tracing::instrument(skip_all)]
    fn add_remote_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_size: usize,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
        stall_mark: usize,
    ) {
        match self {
            BatcherType::Simple(batcher) => {
                batcher.add_remote_am_to_batch(req_data, am, am_id, am_size, scheduler, stall_mark)
            }
            BatcherType::TeamAm(batcher) => {
                batcher.add_remote_am_to_batch(req_data, am, am_id, am_size, scheduler, stall_mark)
            }
        }
    }
    #[tracing::instrument(skip_all)]
    fn add_return_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_size: usize,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
        stall_mark: usize,
    ) {
        match self {
            BatcherType::Simple(batcher) => {
                batcher.add_return_am_to_batch(req_data, am, am_id, am_size, scheduler, stall_mark)
            }
            BatcherType::TeamAm(batcher) => {
                batcher.add_return_am_to_batch(req_data, am, am_id, am_size, scheduler, stall_mark)
            }
        }
    }
    #[tracing::instrument(skip_all)]
    fn add_data_am_to_batch(
        &self,
        req_data: ReqMetaData,
        data: LamellarResultArc,
        data_size: usize,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
        stall_mark: usize,
    ) {
        match self {
            BatcherType::Simple(batcher) => {
                batcher.add_data_am_to_batch(req_data, data, data_size, scheduler, stall_mark)
            }
            BatcherType::TeamAm(batcher) => {
                batcher.add_data_am_to_batch(req_data, data, data_size, scheduler, stall_mark)
            }
        }
    }
    #[tracing::instrument(skip_all)]
    fn add_unit_am_to_batch(
        &self,
        req_data: ReqMetaData,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
        stall_mark: usize,
    ) {
        match self {
            BatcherType::Simple(batcher) => {
                batcher.add_unit_am_to_batch(req_data, scheduler, stall_mark)
            }
            BatcherType::TeamAm(batcher) => {
                batcher.add_unit_am_to_batch(req_data, scheduler, stall_mark)
            }
        }
    }
    #[tracing::instrument(skip_all)]
    async fn exec_batched_msg(
        &self,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
        ame: &RegisteredActiveMessages,
    ) {
        match self {
            BatcherType::Simple(batcher) => {
                batcher
                    .exec_batched_msg(msg, ser_data, lamellae, scheduler, ame)
                    .await;
            }
            BatcherType::TeamAm(batcher) => {
                batcher
                    .exec_batched_msg(msg, ser_data, lamellae, scheduler, ame)
                    .await;
            }
        }
    }
}

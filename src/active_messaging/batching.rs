use crate::active_messaging::registered_active_message::AmId;
use crate::active_messaging::*;

pub(crate) mod simple_batcher;
use simple_batcher::SimpleBatcher;

use async_trait::async_trait;

#[async_trait]
pub(crate) trait Batcher {
    fn add_remote_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_size: usize,
        scheduler: &(impl AmeSchedulerQueue + Sync),
        stall_mark: usize,
    );
    fn add_return_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_size: usize,
        scheduler: &(impl AmeSchedulerQueue + Sync),
        stall_mark: usize,
    );
    fn add_data_am_to_batch(
        &self,
        req_data: ReqMetaData,
        data: LamellarResultArc,
        data_size: usize,
        scheduler: &(impl AmeSchedulerQueue + Sync),
        stall_mark: usize,
    );
    fn add_unit_am_to_batch(
        &self,
        req_data: ReqMetaData,
        scheduler: &(impl AmeSchedulerQueue + Sync),
        stall_mark: usize,
    );

    async fn exec_batched_msg(
        &self,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        scheduler: &(impl AmeSchedulerQueue + Sync),
        ame: &RegisteredActiveMessages,
    );
}

pub(crate) enum BatcherType {
    Simple(SimpleBatcher),
}

#[async_trait]
impl Batcher for BatcherType {
    fn add_remote_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_size: usize,
        scheduler: &(impl AmeSchedulerQueue + Sync),
        stall_mark: usize,
    ) {
        match self {
            BatcherType::Simple(batcher) => {
                batcher.add_remote_am_to_batch(req_data, am, am_id, am_size, scheduler, stall_mark)
            }
        }
    }
    fn add_return_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_size: usize,
        scheduler: &(impl AmeSchedulerQueue + Sync),
        stall_mark: usize,
    ) {
        match self {
            BatcherType::Simple(batcher) => {
                batcher.add_return_am_to_batch(req_data, am, am_id, am_size, scheduler, stall_mark)
            }
        }
    }
    fn add_data_am_to_batch(
        &self,
        req_data: ReqMetaData,
        data: LamellarResultArc,
        data_size: usize,
        scheduler: &(impl AmeSchedulerQueue + Sync),
        stall_mark: usize,
    ) {
        match self {
            BatcherType::Simple(batcher) => {
                batcher.add_data_am_to_batch(req_data, data, data_size, scheduler, stall_mark)
            }
        }
    }
    fn add_unit_am_to_batch(
        &self,
        req_data: ReqMetaData,
        scheduler: &(impl AmeSchedulerQueue + Sync),
        stall_mark: usize,
    ) {
        match self {
            BatcherType::Simple(batcher) => {
                batcher.add_unit_am_to_batch(req_data, scheduler, stall_mark)
            }
        }
    }

    async fn exec_batched_msg(
        &self,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        scheduler: &(impl AmeSchedulerQueue + Sync),
        ame: &RegisteredActiveMessages,
    ) {
        match self {
            BatcherType::Simple(batcher) => {
                batcher
                    .exec_batched_msg(msg, ser_data, lamellae, scheduler, ame)
                    .await;
            }
        }
    }
}

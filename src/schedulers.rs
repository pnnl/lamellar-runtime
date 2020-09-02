use crate::active_messaging::*;
use crate::lamellae::{Backend, LamellaeAM};
use crate::lamellar_request::InternalReq;
use crate::lamellar_team::LamellarArch;

use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) mod work_stealing_sched;

pub enum SchedulerType {
    WorkStealing,
}

#[derive(Debug)]
pub(crate) struct ReqData {
    pub(crate) team: Arc<dyn LamellarArch>, //<'a>,
    pub(crate) src: usize,
    pub(crate) pe: Option<usize>, //team based pe id
    pub(crate) msg: Msg,
    pub(crate) ireq: InternalReq,
    pub(crate) func: LamellarAny,
    pub(crate) backend: Backend,
    // lamellae: &'a dyn LamellaeAM
}

pub(crate) trait SchedulerQueue: Sync + Send {
    fn new() -> Self
    where
        Self: Sized;
    // fn init(&mut self) -> Vec<Box<dyn WorkerThread>>;
    fn submit_req( //unserialized request
        &self,
        src: usize,
        pe: Option<usize>,
        msg: Msg,
        ireq: InternalReq,
        func: LamellarAny,
        team: Arc<dyn LamellarArch>,
        backend: Backend,
    );
    // fn submit_req_all(&self, msg: Msg, ireq: InternalReq, func: LamellarAny);
    fn submit_work(&self, msg: std::vec::Vec<u8>, lamellae: Arc<dyn LamellaeAM>); //serialized active message
}

pub(crate) trait Scheduler {
    fn init(
        &mut self,
        num_pes: usize,
        my_pe: usize,
        lamellaes: BTreeMap<Backend, Arc<dyn LamellaeAM>>,
    );
    fn get_queue(&self) -> Arc<dyn SchedulerQueue>;
}
impl<T: Scheduler + ?Sized> Scheduler for Box<T> {
    fn init(
        &mut self,
        num_pes: usize,
        my_pe: usize,
        lamellaes: BTreeMap<Backend, Arc<dyn LamellaeAM>>,
    ) {
        (**self).init(num_pes, my_pe, lamellaes)
    }
    fn get_queue(&self) -> Arc<dyn SchedulerQueue> {
        (**self).get_queue()
    }
}
pub(crate) fn create_scheduler(sched: SchedulerType) -> Box<dyn Scheduler> {
    Box::new(match sched {
        SchedulerType::WorkStealing => work_stealing_sched::WorkStealingScheduler::new(),
    })
}

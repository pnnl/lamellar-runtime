use crate::active_messaging::*;
use crate::lamellae::{Backend, LamellaeAM};
use crate::lamellar_request::InternalReq;
use crate::lamellar_team::LamellarTeamRT;

#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

pub(crate) mod futures_work_stealing_sched;

pub enum SchedulerType {
    WorkStealing,
}

#[derive(Debug)]
pub(crate) struct ReqData {
    pub(crate) src: usize,
    pub(crate) pe: Option<usize>, //team based pe id
    pub(crate) msg: Msg,
    pub(crate) ireq: InternalReq,
    pub(crate) func: LamellarAny,
    pub(crate) lamellae: Arc<dyn LamellaeAM>,
    pub(crate) team_hash: u64,
}

pub(crate) trait SchedulerQueue: Sync + Send {
    fn new(
        num_pes: usize,
        my_pe: usize,
        teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
    ) -> Self
    where
        Self: Sized;
    // fn init(&mut self) -> Vec<Box<dyn WorkerThread>>;
    fn submit_req(
        //unserialized request
        &self,
        src: usize,
        pe: Option<usize>,
        msg: Msg,
        ireq: InternalReq,
        func: LamellarAny,
        // team_arch: Arc<dyn LamellarArch>,
        // backend: Backend,
        lamellae: Arc<dyn LamellaeAM>,
        team_hash: u64,
    );
    // fn submit_req_all(&self, msg: Msg, ireq: InternalReq, func: LamellarAny);
    fn submit_work(&self, msg: std::vec::Vec<u8>, lamellae: Arc<dyn LamellaeAM>); //serialized active message
    fn as_any(&self) -> &dyn std::any::Any;
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
//#[prof]
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
pub(crate) fn create_scheduler(
    sched: SchedulerType,
    num_pes: usize,
    my_pe: usize,
    teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
) -> Box<dyn Scheduler> {
    Box::new(match sched {
        SchedulerType::WorkStealing => {
            futures_work_stealing_sched::WorkStealingScheduler::new(num_pes, my_pe, teams)
        }
    })
}

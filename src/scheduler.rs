use crate::active_messaging::*;
use crate::lamellae::{Lamellae, SerializedData};
use crate::lamellar_request::InternalReq;
use crate::lamellar_team::LamellarTeamRT;

use core::pin::Pin;
use enum_dispatch::enum_dispatch;
use futures::Future;
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

pub(crate) mod work_stealing;
use work_stealing::{WorkStealing, WorkStealingInner};

pub(crate) struct ReqData {
    pub(crate) src: usize,
    pub(crate) dst: Option<usize>, //team based pe id
    pub(crate) cmd: ExecType,
    pub(crate) id: usize,
    pub(crate) batch_id: Option<usize>,
    pub(crate) func: LamellarFunc,
    pub(crate) lamellae: Arc<Lamellae>,
    pub(crate) world: Pin<Arc<LamellarTeamRT>>,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) team_hash: u64,
    // pub(crate) rt_req: bool,
}

impl ReqData {
    pub(crate) fn copy_with_func(self: Arc<Self>, am: LamellarArcAm) -> ReqData {
        ReqData {
            src: self.src,
            dst: self.dst,
            cmd: self.cmd,
            id: self.id,
            batch_id: self.batch_id,
            func: LamellarFunc::Am(am),
            lamellae: self.lamellae.clone(),
            world: self.world.clone(),
            team: self.team.clone(),
            team_hash: self.team_hash,
        }
    }
}

pub enum SchedulerType {
    WorkStealing,
}

#[enum_dispatch(AmeSchedulerQueue)]
pub(crate) enum AmeScheduler {
    WorkStealingInner,
}
#[enum_dispatch]
pub(crate) trait AmeSchedulerQueue: Sync + Send {
    fn submit_req(
        //unserialized request
        &self,
        ame: Arc<ActiveMessageEngine>,
        src: usize,
        dst: Option<usize>,
        cmd: ExecType,
        id: usize,
        func: LamellarFunc,
        lamellae: Arc<Lamellae>,
        world: Pin<Arc<LamellarTeamRT>>,
        team: Pin<Arc<LamellarTeamRT>>,
        team_hash: u64,
        ireq: Option<InternalReq>,
    );
    fn submit_work(
        &self,
        ame: Arc<ActiveMessageEngine>,
        msg: SerializedData,
        lamellae: Arc<Lamellae>,
    ); //serialized active message
    fn submit_task<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send;
    fn exec_task(&self);
    fn shutdown(&self);
    fn active(&self) -> bool;
}

#[enum_dispatch(SchedulerQueue)]
pub(crate) enum Scheduler {
    WorkStealing,
}
#[enum_dispatch]
pub(crate) trait SchedulerQueue: Sync + Send {
    fn submit_req(
        //unserialized request
        &self,
        src: usize,
        dst: Option<usize>,
        cmd: ExecType,
        id: usize,
        func: LamellarFunc,
        lamellae: Arc<Lamellae>,
        world: Pin<Arc<LamellarTeamRT>>,
        team: Pin<Arc<LamellarTeamRT>>,
        team_hash: u64,
        ireq: Option<InternalReq>,
    );
    fn submit_work(&self, msg: SerializedData, lamellae: Arc<Lamellae>); //serialized active message
    fn submit_task<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send;
    fn exec_task(&self);
    fn shutdown(&self);
    fn active(&self) -> bool;
}

pub(crate) fn create_scheduler(
    sched: SchedulerType,
    num_pes: usize,
    my_pe: usize,
    teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
) -> Scheduler {
    match sched {
        SchedulerType::WorkStealing => {
            Scheduler::WorkStealing(work_stealing::WorkStealing::new(num_pes, my_pe, teams))
        }
    }
}

use crate::active_messaging::*;
use crate::lamellae::{Lamellae,SerializedData};
use crate::lamellar_request::InternalReq;
use crate::lamellar_team::LamellarTeamRT;

#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use futures::Future;
use enum_dispatch::enum_dispatch;

pub(crate) mod work_stealing;
use work_stealing::{WorkStealing,WorkStealingInner};




// #[derive(Debug)]
// pub(crate) struct ReqData {
//     pub(crate) src: usize,
//     pub(crate) pe: Option<usize>, //team based pe id
//     pub(crate) msg: Msg,
//     pub(crate) ireq: InternalReq,
//     pub(crate) func: LamellarFunc,
//     pub(crate) lamellae: Arc<Lamellae>,
//     pub(crate) team_hash: u64,
//     pub(crate) rt_req: bool,
// }

pub(crate) struct NewReqData{
    pub(crate) src: usize,
    pub(crate) dst: Option<usize>, //team based pe id
    pub(crate) cmd: ExecType,
    pub(crate) id: usize,
    pub(crate) batch_id: Option<usize>,
    pub(crate) func: LamellarFunc,
    pub(crate) lamellae: Arc<Lamellae>,
    pub(crate) world: Arc<LamellarTeamRT>,
    pub(crate) team: Arc<LamellarTeamRT>,
    pub(crate) team_hash: u64,
    // pub(crate) rt_req: bool,
}

// impl Drop for ReqData{
//     fn drop(&mut self){
//         //println!("dropping ReqData");
//         println!("lamellae: {:?}",Arc::strong_count(&self.lamellae));
//         //println!("dropped ReqData");
//     }
// }

pub enum SchedulerType {
    WorkStealing,
}

#[enum_dispatch(AmeSchedulerQueue)]
pub(crate) enum AmeScheduler{
    WorkStealingInner
}
#[enum_dispatch]
pub(crate) trait AmeSchedulerQueue: Sync + Send {
    // fn submit_req(
    //     //unserialized request
    //     &self,
    //     ame:  Arc<ActiveMessageEngine>,
    //     src: usize,
    //     pe: Option<usize>,
    //     msg: Msg,
    //     ireq: InternalReq,
    //     func: LamellarFunc,
    //     lamellae: Arc<Lamellae>,
    //     team_hash: u64,
    // );
    fn submit_req_new(
        //unserialized request
        &self,
        ame:  Arc<ActiveMessageEngine>,
        src: usize,
        dst: Option<usize>,
        cmd: ExecType,
        id: usize,
        func: LamellarFunc,
        lamellae: Arc<Lamellae>,
        world: Arc<LamellarTeamRT>,
        team: Arc<LamellarTeamRT>,
        team_hash: u64,
        ireq: Option<InternalReq>,
    );
    fn submit_work(&self, ame:  Arc<ActiveMessageEngine>, msg: SerializedData, lamellae: Arc<Lamellae>,); //serialized active message
    fn submit_task<F>(&self,future: F )
    where 
        F: Future<Output = ()> + Send + 'static;

    fn shutdown(&self);
}

#[enum_dispatch(SchedulerQueue)]
pub(crate) enum Scheduler{
    WorkStealing
}
#[enum_dispatch]
pub(crate) trait SchedulerQueue: Sync + Send {
    // fn submit_req(
    //     //unserialized request
    //     &self,
    //     src: usize,
    //     pe: Option<usize>,
    //     msg: Msg,
    //     ireq: InternalReq,
    //     func: LamellarFunc,
    //     lamellae: Arc<Lamellae>,
    //     team_hash: u64,
    // );
    fn submit_req_new(
        //unserialized request
        &self,
        src: usize,
        dst: Option<usize>,
        cmd: ExecType,
        id: usize,
        func: LamellarFunc,
        lamellae: Arc<Lamellae>,
        world: Arc<LamellarTeamRT>,
        team: Arc<LamellarTeamRT>,
        team_hash: u64,
        ireq: Option<InternalReq>,
    );
    fn submit_work(&self, msg: SerializedData, lamellae: Arc<Lamellae>); //serialized active message
    fn submit_task<F>(&self,future: F )
    where 
        F: Future<Output = ()> + Send + 'static;
    fn shutdown(&self);
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

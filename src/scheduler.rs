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
use work_stealing::WorkStealing;




// #[derive(Debug)]
pub(crate) struct ReqData {
    pub(crate) src: usize,
    pub(crate) pe: Option<usize>, //team based pe id
    pub(crate) msg: Msg,
    pub(crate) ireq: InternalReq,
    pub(crate) func: LamellarAny,
    pub(crate) lamellae: Arc<Lamellae>,
    pub(crate) team_hash: u64,
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

#[enum_dispatch(SchedulerQueue)]
pub(crate) enum Scheduler{
    WorkStealing
}
#[enum_dispatch]
pub(crate) trait SchedulerQueue: Sync + Send {
    fn submit_req(
        //unserialized request
        &self,
        src: usize,
        pe: Option<usize>,
        msg: Msg,
        ireq: InternalReq,
        func: LamellarAny,
        lamellae: Arc<Lamellae>,
        team_hash: u64,
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

use crate::lamellae::{Lamellae, LamellaeRDMA, SerializedData};
use crate::lamellar_arch::IdError;
use crate::lamellar_request::{InternalResult, LamellarRequestResult};
use crate::lamellar_team::{LamellarTeam, LamellarTeamRT};
use crate::scheduler::{ReqId, SchedulerQueue};
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
// use log::trace;
use async_trait::async_trait;
use futures::Future;
use parking_lot::Mutex; //, RwLock};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc; //, Weak};

pub(crate) mod registered_active_message;
use registered_active_message::RegisteredActiveMessages; //, AMS_EXECS};

pub(crate) mod batching;

#[cfg(feature = "nightly")]
pub(crate) mod remote_closures;
#[cfg(feature = "nightly")]
pub(crate) use remote_closures::RemoteClosures;
#[cfg(feature = "nightly")]
use remote_closures::{exec_closure_cmd, process_closure_request};

const BATCH_AM_SIZE: usize = 100000;

pub trait AmLocal: Sync + Send + std::fmt::Debug {}

impl<T: Sync + Send + std::fmt::Debug> AmLocal for T {}

pub trait AmDist: serde::ser::Serialize + serde::de::DeserializeOwned + AmLocal + 'static {}

impl<T: serde::ser::Serialize + serde::de::DeserializeOwned + AmLocal + 'static> AmDist for T {}

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
pub(crate) enum ExecType {
    Am(Cmd),
    Runtime(Cmd),
}

pub trait DarcSerde {
    fn ser(&self, num_pes: usize);
    fn des(&self, cur_pe: Result<usize, IdError>);
}

impl<T> DarcSerde for &T {
    fn ser(&self, _num_pes: usize) {}
    fn des(&self, _cur_pe: Result<usize, IdError>) {}
}

pub trait LamellarSerde: AmLocal {
    fn serialized_size(&self) -> usize;
    fn serialize_into(&self, buf: &mut [u8]);
}
pub trait LamellarResultSerde: LamellarSerde {
    fn serialized_result_size(&self, result: &LamellarAny) -> usize;
    fn serialize_result_into(&self, buf: &mut [u8], result: &LamellarAny);
}

pub trait RemoteActiveMessage: LamellarActiveMessage + LamellarSerde + LamellarResultSerde {
    fn as_local(self: Arc<Self>) -> LamellarArcLocalAm;
}

pub trait LamellarActiveMessage: DarcSerde + std::fmt::Debug {
    fn exec(
        self: Arc<Self>,
        my_pe: usize,
        num_pes: usize,
        local: bool,
        world: Arc<LamellarTeam>,
        team: Arc<LamellarTeam>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = LamellarReturn> + Send>>;
    fn get_id(&self) -> String;
}

pub(crate) type LamellarArcLocalAm = Arc<dyn LamellarActiveMessage + Sync + Send>;
pub(crate) type LamellarArcAm = Arc<dyn RemoteActiveMessage + Sync + Send>;
pub(crate) type LamellarAny = Box<dyn std::any::Any + Sync + Send>;
pub(crate) type LamellarResultArc = Arc<dyn LamellarSerde + Sync + Send>;

pub trait Serde: serde::ser::Serialize + serde::de::DeserializeOwned {}

pub trait LocalAM: AmLocal {
    type Output: AmLocal;
}

pub trait LamellarAM {
    type Output: AmDist;
}

#[derive(Debug)]
pub enum LamellarReturn {
    LocalData(LamellarAny),
    LocalAm(LamellarArcAm),
    RemoteData(LamellarResultArc),
    RemoteAm(LamellarArcAm),
    Unit,
}

#[derive(Clone, Debug)]
pub(crate) struct ReqMetaData {
    pub(crate) src: usize,         //source pe
    pub(crate) dst: Option<usize>, // destination pe - team based pe id, none means all pes
    pub(crate) id: ReqId,          // id of the request
    pub(crate) lamellae: Arc<Lamellae>,
    pub(crate) world: Pin<Arc<LamellarTeamRT>>,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) team_addr: usize,
}

#[derive(Debug)]
pub(crate) enum Am {
    All(ReqMetaData, LamellarArcAm),
    Remote(ReqMetaData, LamellarArcAm), //req data, am to execute
    Local(ReqMetaData, LamellarArcLocalAm), //req data, am to execute
    Return(ReqMetaData, LamellarArcAm), //req data, am to return and execute
    Data(ReqMetaData, LamellarResultArc), //req data, data to return
    Unit(ReqMetaData),                  //req data
    BatchedReturn(ReqMetaData, LamellarArcAm, ReqId), //req data, am to return and execute, batch id
    BatchedData(ReqMetaData, LamellarResultArc, ReqId), //req data, data to return, batch id
    BatchedUnit(ReqMetaData, ReqId),    //req data, batch id
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Default,
)]
pub(crate) enum Cmd {
    #[default]
    Am, //a single am
    ReturnAm, //a single return am
    Data,     //a single data result
    Unit,     //a single unit result
    BatchedMsg, //a batched message, can contain a variety of am types
              // BatchedReturnAm, //a batched message, only containing return ams -- not sure this can happen
              // BatchedData, //a batched message, only containing data results
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy, Default)]
pub(crate) struct Msg {
    pub src: u16,
    pub cmd: Cmd,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub(crate) enum RetType {
    //maybe change to ReqType? ReturnRequestType?
    Unit,
    Closure,
    Am,
    Data,
    Barrier,
    NoHandle,
    Put,
    Get,
}

#[derive(Debug)]
pub(crate) struct AMCounters {
    pub(crate) outstanding_reqs: Arc<AtomicUsize>,
    pub(crate) send_req_cnt: AtomicUsize,
}

//#[prof]
impl AMCounters {
    pub(crate) fn new() -> AMCounters {
        AMCounters {
            outstanding_reqs: Arc::new(AtomicUsize::new(0)),
            send_req_cnt: AtomicUsize::new(0),
        }
    }
    pub(crate) fn add_send_req(&self, num: usize) {
        let _num_reqs = self.outstanding_reqs.fetch_add(num, Ordering::SeqCst);
        self.send_req_cnt.fetch_add(num, Ordering::SeqCst);
    }
}

pub trait ActiveMessaging {
    fn wait_all(&self);
    fn barrier(&self);
    fn exec_am_all<F>(&self, am: F) -> Pin<Box<dyn Future<Output = Vec<F::Output>> + Send>>
    //Box<dyn LamellarMultiRequest<Output = F::Output>>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist;
    fn exec_am_pe<F>(&self, pe: usize, am: F) -> Pin<Box<dyn Future<Output = F::Output> + Send>>
    //Box<dyn LamellarRequest<Output = F::Output>>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist;
    fn exec_am_local<F>(&self, am: F) -> Pin<Box<dyn Future<Output = F::Output> + Send>>
    where
        F: LamellarActiveMessage + LocalAM + 'static;
}

#[async_trait]
pub(crate) trait ActiveMessageEngine {
    async fn process_msg(
        &self,
        am: Am,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
        stall_mark: usize,
    );

    async fn exec_msg(
        &self,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
    );

    fn get_team_and_world(
        &self,
        pe: usize,
        team_addr: usize,
        lamellae: &Arc<Lamellae>,
    ) -> (Arc<LamellarTeam>, Arc<LamellarTeam>) {
        let local_team_addr = lamellae.local_addr(pe, team_addr);
        let team_rt = unsafe {
            let team_ptr = local_team_addr as *mut *const LamellarTeamRT;
            // println!("{:x} {:?} {:?} {:?}", team_hash,team_ptr, (team_hash as *mut (*const LamellarTeamRT)).as_ref(), (*(team_hash as *mut (*const LamellarTeamRT))).as_ref());
            Arc::increment_strong_count(*team_ptr);
            Pin::new_unchecked(Arc::from_raw(*team_ptr))
        };
        let world_rt = if let Some(world) = team_rt.world.clone() {
            world
        } else {
            team_rt.clone()
        };
        let world = LamellarTeam::new(None, world_rt, true);
        let team = LamellarTeam::new(Some(world.clone()), team_rt, true);
        (team, world)
    }

    fn send_data_to_user_handle(&self, req_id: ReqId, pe: usize, data: InternalResult) {
        // println!("returned req_id: {:?}", req_id);
        let req = unsafe { Arc::from_raw(req_id.id as *const LamellarRequestResult) };
        // println!("strong count recv: {:?} ", Arc::strong_count(&req));
        req.add_result(pe, req_id.sub_id, data);
    }
}

#[derive(Debug)]
pub(crate) enum ActiveMessageEngineType {
    RegisteredActiveMessages(RegisteredActiveMessages),
}

#[async_trait]
impl ActiveMessageEngine for ActiveMessageEngineType {
    async fn process_msg(
        &self,
        am: Am,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
        stall_mark: usize,
    ) {
        match self {
            ActiveMessageEngineType::RegisteredActiveMessages(remote_am) => {
                remote_am.process_msg(am, scheduler, stall_mark).await;
            }
        }
    }
    async fn exec_msg(
        &self,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
    ) {
        match self {
            ActiveMessageEngineType::RegisteredActiveMessages(remote_am) => {
                remote_am.exec_msg(msg, ser_data, lamellae, scheduler).await;
            }
        }
    }
}

// //maybe make this a struct then we could hold the pending counters...
// pub(crate) struct ActiveMessageEngineS {
//     // teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
//     // my_pe: usize,
//     batched_am: Arc<RegisteredActiveMessages>,
// }

// //#[prof]
// // impl Drop for ActiveMessageEngine {
// //     fn drop(&mut self) {
// //         trace!("[{:?}] AME dropping", self.my_pe);
// //     }
// // }

// //#[prof]
// impl ActiveMessageEngine {
//     pub(crate) fn new(
//         // my_pe: usize,
//         scheduler: Arc<AmeScheduler>,
//         // teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
//         stall_mark: Arc<AtomicUsize>,
//         batch_id_start: usize,
//         scheduler_mask: usize,
//     ) -> Self {
//         // trace!("registered funcs {:?}", AMS_EXECS.len(),);
//         ActiveMessageEngine {
//             // teams: teams,
//             // my_pe: my_pe,
//             batched_am: Arc::new(RegisteredActiveMessages::new(
//                 scheduler,
//                 stall_mark,
//                 batch_id_start,
//                 scheduler_mask,
//             )),
//         }
//     }

//     pub(crate) async fn process_msg_new(&self, req_data: ReqData) {
//         // trace!("[{:?}] process msg: {:?}",self.my_pe, &req_data);
//         // let addr = req_data.lamellae.local_addr(req_data.src,req_data)
//         // let (team, world) = self.get_team_and_world(req_data.team.team_hash);
//         let world = LamellarTeam::new(None, req_data.world.clone(), true);
//         let team = LamellarTeam::new(Some(world.clone()), req_data.team.clone(), true);

//         match req_data.cmd.clone() {
//             ExecType::Runtime(_cmd) => {}
//             ExecType::Am(_) => self.batched_am.process_am_req(req_data, world, team).await,
//         }
//     }

//     pub(crate) fn get_team_and_world(
//         &self,
//         team_hash: usize,
//     ) -> (Arc<LamellarTeam>, Arc<LamellarTeam>) {
//         let team_rt = unsafe {
//             let team_ptr = team_hash as *mut *const LamellarTeamRT;
//             // println!("{:x} {:?} {:?} {:?}", team_hash,team_ptr, (team_hash as *mut (*const LamellarTeamRT)).as_ref(), (*(team_hash as *mut (*const LamellarTeamRT))).as_ref());
//             Arc::increment_strong_count(*team_ptr);
//             Pin::new_unchecked(Arc::from_raw(*team_ptr))
//         };
//         let world_rt = if let Some(world) = team_rt.world.clone() {
//             world
//         } else {
//             team_rt.clone()
//         };
//         let world = LamellarTeam::new(None, world_rt, true);
//         let team = LamellarTeam::new(Some(world.clone()), team_rt, true);
//         (team, world)
//     }

//     pub(crate) async fn exec_msg(
//         &self,
//         ame: Arc<ActiveMessageEngine>,
//         msg: Msg,
//         ser_data: SerializedData,
//         lamellae: Arc<Lamellae>,
//         team_hash: usize,
//     ) {
//         let (team, world) = self.get_team_and_world(team_hash);
//         match msg.cmd.clone() {
//             ExecType::Am(cmd) => {
//                 self.batched_am
//                     .process_batched_am(ame, cmd, msg, ser_data, lamellae, world, team)
//                     .await;
//             } //execute a remote am
//             ExecType::Runtime(_cmd) => {}
//         }
//     }

//     fn send_data_to_user_handle(req_id: ReqId, pe: u16, data: InternalResult) {
//         // println!("returned req_id: {:?}", req_id);
//         let req = unsafe { Arc::from_raw(req_id.id as *const LamellarRequestResult) };
//         // println!("strong count recv: {:?} ",Arc::strong_count(&req));
//         req.add_result(pe as usize, req_id.sub_id, data);
//     }
// }

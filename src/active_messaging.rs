use crate::lamellae::{Lamellae, LamellaeRDMA, SerializedData};
use crate::lamellar_arch::IdError;
use crate::lamellar_request::{InternalReq, InternalResult, LamellarRequest};
use crate::lamellar_team::{LamellarTeam, LamellarTeamRT};
use crate::scheduler::{AmeScheduler, ReqData};
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use log::trace;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

pub(crate) mod registered_active_message;
use registered_active_message::{RegisteredActiveMessages, AMS_EXECS};

#[cfg(feature = "nightly")]
pub(crate) mod remote_closures;
#[cfg(feature = "nightly")]
pub(crate) use remote_closures::RemoteClosures;
#[cfg(feature = "nightly")]
use remote_closures::{exec_closure_cmd, process_closure_request};

//todo
//turn requests into a struct
//impl insert, send_to_user -- (get, remove)
lazy_static! {
    pub(crate) static ref REQUESTS: Mutex<HashMap<usize, InternalReq>> = { //any reason for this to be static... we can put it im AME
       Mutex::new(HashMap::new())
    };
}

pub trait AmDist:
    serde::ser::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
{
}

impl<T: serde::ser::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static> AmDist for T {}

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
pub(crate) enum ExecType {
    Am(Cmd),
    Runtime(Cmd),
}

pub trait DarcSerde {
    fn ser(&self, num_pes: usize, cur_pe: Result<usize, IdError>);
    fn des(&self, cur_pe: Result<usize, IdError>);
}

impl<T> DarcSerde for &T {
    fn ser(&self, _num_pes: usize, _cur_pe: Result<usize, IdError>) {}
    fn des(&self, _cur_pe: Result<usize, IdError>) {}
}

pub trait LamellarSerde: Sync + Send {
    fn serialized_size(&self) -> usize;
    fn serialize_into(&self, buf: &mut [u8]);
}
pub trait LamellarResultSerde: LamellarSerde {
    fn serialized_result_size(&self, result: &LamellarAny) -> usize;
    fn serialize_result_into(&self, buf: &mut [u8], result: &LamellarAny);
}

pub trait RemoteActiveMessage: LamellarActiveMessage + LamellarSerde + LamellarResultSerde {}

pub trait LamellarActiveMessage: DarcSerde {
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

#[derive(Clone)]
pub(crate) enum LamellarFunc {
    LocalAm(LamellarArcLocalAm),
    Am(LamellarArcAm),
    Result(LamellarResultArc),
    None,
}

pub(crate) type LamellarArcLocalAm = Arc<dyn LamellarActiveMessage + Send + Sync>;
pub(crate) type LamellarArcAm = Arc<dyn RemoteActiveMessage + Send + Sync>;
pub(crate) type LamellarAny = Box<dyn std::any::Any + Send + Sync>;
pub(crate) type LamellarResultArc = Arc<dyn LamellarSerde + Send + Sync>;

pub trait Serde: serde::ser::Serialize + serde::de::DeserializeOwned {}

pub trait LocalAM {
    type Output: AmDist;
}

pub trait LamellarAM {
    type Output: AmDist;
}

pub enum LamellarReturn {
    LocalData(LamellarAny),
    LocalAm(LamellarArcAm),
    RemoteData(LamellarResultArc),
    RemoteAm(LamellarArcAm),
    Unit,
}

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
pub(crate) enum Cmd {
    //inverse this so its the exec type holding the inner command...
    Exec,
    ExecReturn,
    LocalExec,
    ExecBatchUnitReturns,
    AmReturn,
    BatchedAmReturn,
    DataReturn,
    BatchedDataReturn,
    UnitReturn,
    BatchedUnitReturn,
    BatchedUnitReturnNew,
    BatchedMsg,
    ExecBatchMsgSend,
    None,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
pub(crate) struct Msg {
    pub req_id: usize,
    pub src: u16,
    pub cmd: ExecType,
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
    fn exec_am_all<F>(&self, am: F) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist;
    fn exec_am_pe<F>(
        &self,
        pe: usize,
        am: F,
    ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist;
    fn exec_am_local<F>(&self, am: F) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: LamellarActiveMessage + LocalAM + Send + Sync + 'static;
}

//maybe make this a struct then we could hold the pending counters...
pub(crate) struct ActiveMessageEngine {
    teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
    my_pe: usize,
    batched_am: Arc<RegisteredActiveMessages>,
}

//#[prof]
impl Drop for ActiveMessageEngine {
    fn drop(&mut self) {
        trace!("[{:?}] AME dropping", self.my_pe);
    }
}

//#[prof]
impl ActiveMessageEngine {
    pub(crate) fn new(
        my_pe: usize,
        scheduler: Arc<AmeScheduler>,
        teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
        stall_mark: Arc<AtomicUsize>,
    ) -> Self {
        trace!("registered funcs {:?}", AMS_EXECS.len(),);
        ActiveMessageEngine {
            teams: teams,
            my_pe: my_pe,
            batched_am: Arc::new(RegisteredActiveMessages::new(scheduler, stall_mark)),
        }
    }

    pub(crate) async fn process_msg_new(&self, req_data: ReqData, ireq: Option<InternalReq>) {
        // trace!("[{:?}] process msg: {:?}",self.my_pe, &req_data);
        if let Some(ireq) = ireq {
            REQUESTS.lock().insert(req_data.id, ireq.clone());
        }
        // let addr = req_data.lamellae.local_addr(req_data.src,req_data)
        // let (team, world) = self.get_team_and_world(req_data.team.team_hash);
        let world = LamellarTeam::new(None, req_data.world.clone(), self.teams.clone(), true);
        let team = LamellarTeam::new(
            Some(world.clone()),
            req_data.team.clone(),
            self.teams.clone(),
            true,
        );

        match req_data.cmd.clone() {
            ExecType::Runtime(_cmd) => {}
            ExecType::Am(_) => self.batched_am.process_am_req(req_data, world, team).await,
        }
    }

    pub(crate) fn get_team_and_world(
        &self,
        team_hash: usize,
    ) -> (Arc<LamellarTeam>, Arc<LamellarTeam>) {
        let team_rt = unsafe {
            let team_ptr = team_hash as *mut *const LamellarTeamRT;
            // println!("{:x} {:?} {:?} {:?}", team_hash,team_ptr, (team_hash as *mut (*const LamellarTeamRT)).as_ref(), (*(team_hash as *mut (*const LamellarTeamRT))).as_ref());
            Arc::increment_strong_count(*team_ptr);
            Pin::new_unchecked(Arc::from_raw(*team_ptr))
            // unsafe {(*(team_hash as  *const  Pin<Arc<LamellarTeamRT>>)).clone()}
        };

        // let team_rt = unsafe {(*(team_hash as *mut *const Arc<LamellarTeamRT>)).as_ref().clone()};
        // let team_rt = unsafe{ (*(team_hash as *mut (*const Arc<LamellarTeamRT>))).as_ref().unwrap().clone()};
        let world_rt = if let Some(world) = team_rt.world.clone() {
            world
        } else {
            team_rt.clone()
        };
        // let teams = self.teams.read();
        // let world_rt = teams
        //     .get(&0)
        //     .expect("invalid world hash")
        //     .upgrade()
        //     .expect("team no longer exists (world)");
        // let team_rt = teams
        //     .get(&team_hash)
        //     .expect("invalid team hash")
        //     .upgrade()
        //     .expect("team no longer exists {:?}");
        let world = LamellarTeam::new(None, world_rt, self.teams.clone(), true);
        let team = LamellarTeam::new(Some(world.clone()), team_rt, self.teams.clone(), true);
        (team, world)
    }

    pub(crate) async fn exec_msg(
        &self,
        ame: Arc<ActiveMessageEngine>,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        team_hash: usize,
    ) {
        let (team, world) = self.get_team_and_world(team_hash);
        match msg.cmd.clone() {
            ExecType::Am(cmd) => {
                self.batched_am
                    .process_batched_am(ame, cmd, msg, ser_data, lamellae, world, team)
                    .await;
            } //execute a remote am
            ExecType::Runtime(_cmd) => {}
        }
    }

    // make this an associated function... or maybe make a "REQUESTS struct which will have a send_data_to_user_handle"
    fn send_data_to_user_handle(
        req_id: usize,
        pe: u16,
        data: InternalResult,
        team: Pin<Arc<LamellarTeamRT>>,
    ) {
        let reqs = REQUESTS.lock();
        match reqs.get(&req_id) {
            Some(ireq) => {
                // println!("sending {:?} to user  handle",req_id);
                let ireq = ireq.clone();
                drop(reqs); //release lock in the hashmap
                let _num_reqs = ireq.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                let _num_reqs = ireq.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                if let Some(tg_outstanding_reqs) = ireq.tg_outstanding_reqs {
                    let _num_reqs = tg_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                }
                if let Ok(_) = ireq.data_tx.send((pe as usize, data)) {} //if this returns an error it means the user has dropped the handle
                let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
                if cnt == 1 {
                    REQUESTS.lock().remove(&req_id);
                }
            }
            None => {
                panic!(
                    "error id not found {:?} mem in use: {:?}",
                    req_id,
                    team.lamellae.occupied()
                )
            }
        }
    }
}

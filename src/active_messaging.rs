use crate::lamellae::{Lamellae, SerializedData};
use crate::lamellar_request::{InternalReq, LamellarRequest, InternalResult};
use crate::lamellar_team::LamellarTeam;
use crate::scheduler::{NewReqData,AmeScheduler};
// use async_trait::async_trait;
// use chashmap::CHashMap;
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use log::trace;
use parking_lot::{Mutex,RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

pub(crate) mod registered_active_message;
use registered_active_message::{AMS_EXECS,RegisteredActiveMessages};

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


#[derive(
    serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
pub(crate) enum ExecType {
    #[cfg(feature = "nightly")]
    Closure(Cmd),
    Am(Cmd),
    Runtime(Cmd),
}

pub trait DarcSerde {
    fn ser(&self,num_pes: usize);
    fn des(&self);
}

pub trait LamellarSerde:  Sync + Send {
    fn serialized_size(&self)->usize;
    fn serialize_into(&self,buf: &mut [u8]);
}
pub trait LamellarResultSerde: LamellarSerde {
    fn serialized_result_size(&self,result: &LamellarAny)->usize;
    fn serialize_result_into(&self,buf: &mut [u8],result: &LamellarAny);
}

pub trait RemoteActiveMessage: LamellarActiveMessage + LamellarSerde + LamellarResultSerde {}

pub trait LamellarActiveMessage: DarcSerde  {
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
pub (crate) enum LamellarFunc{
    LocalAm(LamellarArcLocalAm),
    Am(LamellarArcAm),
    // Closure(LamellarAny),
    Result(LamellarResultArc),
    None,
}

pub(crate) type LamellarArcLocalAm = Arc<dyn LamellarActiveMessage + Send + Sync>;
pub(crate) type LamellarArcAm = Arc<dyn RemoteActiveMessage + Send + Sync>;
// pub(crate) type LamellarBoxedAm = Box<dyn LamellarActiveMessage + Send + Sync>;
// pub(crate) type LamellarBoxedData = Box<dyn LamellarSerde>;
pub(crate) type LamellarAny = Box<dyn std::any::Any + Send + Sync>;
pub(crate) type LamellarResultArc =Arc<dyn LamellarSerde + Send + Sync>;

pub trait Serde:  serde::ser::Serialize + serde::de::DeserializeOwned {}

pub trait LocalAM{
    type Output: serde::ser::Serialize + serde::de::DeserializeOwned + Sync + Send;
}

pub trait LamellarAM {
    type Output: serde::ser::Serialize + serde::de::DeserializeOwned + Sync + Send;
}

pub enum LamellarReturn {
    LocalData(LamellarAny),
    LocalAm(LamellarArcAm),
    // RemoteData(LamellarAny,LamellarBoxedAm),
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
    BatchedMsg,
    ExecBatchMsgSend,
    None,
}


#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
pub(crate) struct Msg {
    pub req_id: usize,
    pub src: u16,
    pub cmd: ExecType,
    
    // pub team_id: usize,
    // pub return_data: bool,
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
            // am_exec_cnt: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub(crate) fn add_send_req(&self, num: usize) {
        let _num_reqs=self.outstanding_reqs.fetch_add(num, Ordering::SeqCst);
        // println!("reqs: {:?}",num_reqs+num);
        self.send_req_cnt.fetch_add(num, Ordering::SeqCst);
    }
}

pub trait ActiveMessaging {
    fn wait_all(&self);
    fn barrier(&self);
    fn exec_am_all<F>(&self, am: F) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: RemoteActiveMessage + LamellarAM +Serde + Send + Sync + 'static;
    fn exec_am_pe<F>(
        &self,
        pe: usize,
        am: F,
    ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + Send + Sync + 'static;
    fn exec_am_local<F>(
            &self,
            am: F,
        ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
        where
            F: LamellarActiveMessage + LocalAM  + Send + Sync + 'static;
}

//maybe make this a struct then we could hold the pending counters...
pub(crate) struct ActiveMessageEngine {
    teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeam>>>>,
    // pending_active: Arc<Mutex<HashMap<u16,usize>>>, //CHashMap<u16, usize>,
    // pending_resp: Arc<Mutex<HashMap<u16, crossbeam::queue::SegQueue<usize>>>>,// CHashMap<u16, crossbeam::queue::SegQueue<usize>>,
    // pending_msg_active: CHashMap<u64, usize>,
    // pending_msg: Arc<Mutex<HashMap<u64, crossbeam::queue::SegQueue<Vec<u8>>>>>,
    my_pe: usize,
    // num_pes: usize,
    // fake_ireq: InternalReq,
    // _fake_arch: Arc<StridedArch>,
    // pub(crate) scheduler: Weak<AmeScheduler>,
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
        // num_pes: usize,
        my_pe: usize,
        scheduler: Arc<AmeScheduler>,
        teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeam>>>>,
    ) -> Self {
        trace!("registered funcs {:?}", AMS_EXECS.len(),);
        // println!("sizes: {:?} {:?} {:?}",std::mem::size_of::<Msg>(),std::mem::size_of::<ExecType>(),std::mem::size_of::<Cmd>());
        
        // let world = teams.read()
        //         .get(&0)
        //         .expect("invalid hash id").clone();
        // let (dummy_s, _) = crossbeam::channel::unbounded();
        ActiveMessageEngine {
            teams: teams,
            // pending_active: Arc::new(Mutex::new(HashMap::new())),
            // pending_resp: Arc::new(Mutex::new(HashMap::new())),
            // pending_msg_active: CHashMap::new(),
            // pending_msg: Arc::new(Mutex::new(HashMap::new())),
            my_pe: my_pe,
            // num_pes: num_pes,
            // fake_ireq: InternalReq {
            //     data_tx: dummy_s,
            //     cnt: Arc::new(CachePadded::new(AtomicUsize::new(0usize))),
            //     team_outstanding_reqs: Arc::new(AtomicUsize::new(0usize)),
            //     world_outstanding_reqs: Arc::new(AtomicUsize::new(0usize)),
            //     team_hash: 0,
            //     team: world.clone(),
            // },
            // _fake_arch: Arc::new(StridedArch::new(my_pe, 1, 1)),
            // scheduler: scheduler.clone(),
            batched_am: Arc::new(RegisteredActiveMessages::new(scheduler)),
        }
    }

    

    pub(crate) async fn process_msg_new(&self, req_data: NewReqData, ireq: Option<InternalReq>)  {
        // trace!("[{:?}] process msg: {:?}",self.my_pe, &req_data);
        if let Some (ireq) = ireq{
            REQUESTS.lock().insert(req_data.id, ireq.clone());
        }

        match req_data.cmd.clone() {
            ExecType::Runtime(_cmd) => {
            
            }
            ExecType::Am(_) => self.batched_am.process_am_req(req_data).await,

            #[cfg(feature = "nightly")]
            ExecType::Closure(_) => process_closure_request(self, req_data, world, team.clone()),
        }
        
    }           

    pub(crate) async fn exec_msg(
        &self,
        ame: Arc<ActiveMessageEngine>,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        team_hash: u64,
    )  {
        // println!("[{:?}] exec_msg: {:?} team_hash {:?}", self.my_pe, msg,team_hash);
        
        
        let (world,team) = {
            let teams = self.teams.read();
            (teams.get(&0).expect("invalid world hash").upgrade().expect("team no longer exists"),
            teams.get(&team_hash).expect("invalid team hash").upgrade().expect("team no longer exists"))
        };
        // trace!("using team {:?}", team.team_hash);
        match msg.cmd.clone() {
            // ExecType::Am(cmd) => exec_am_cmd(self, cmd, msg, ser_data, lamellae, world, team).await, //execute a remote am
            ExecType::Am(cmd) => {
                self.batched_am.process_batched_am(ame, cmd, msg, ser_data, lamellae, world, team).await;
            }, //execute a remote am

            #[cfg(feature = "nightly")]
            ExecType::Closure(cmd) => {
                exec_closure_cmd(self, cmd, msg, ser_data, lamellae,world, team)
            }
            ExecType::Runtime(_cmd) => {
                // self.exec_runtime_cmd(cmd, msg, lamellae, Some(ser_data), team_hash, None, team).await;
            }
        }
    }

    

    // make this an associated function... or maybe make a "REQUESTS struct which will have a send_data_to_user_handle" 
    fn send_data_to_user_handle(req_id: usize, pe: u16, data: InternalResult) { 
        let reqs = REQUESTS.lock();
        // let res = REQUESTS[req_id % REQUESTS.len()].get(&req_id);
        // println!("finalize {:?}",req_id);
        match reqs.get(&req_id) {
            Some(ireq) => {
                let ireq = ireq.clone();
                drop(reqs); //release lock in the hashmap
                // println!(" send_data_to_user_handle {:?}",  ireq);
                let _num_reqs = ireq.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                // println!("team reqs: {:?}",num_reqs);
                let _num_reqs =ireq.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                // println!("world reqs: {:?}",num_reqs);
                if let Ok(_) = ireq.data_tx.send((pe as usize,data)) {} //if this returns an error it means the user has dropped the handle
                let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
                if cnt == 1 {
                    REQUESTS.lock().remove(&req_id);
                }
            }
            None => panic!("error id not found"),
        }
    }
}

//     
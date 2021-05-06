use crate::lamellae::LamellaeAM;
use crate::lamellar_arch::StridedArch;
use crate::lamellar_request::{InternalReq, LamellarRequest};
use crate::lamellar_team::LamellarTeamRT;
use crate::schedulers::ReqData;
// use async_trait::async_trait;
use chashmap::CHashMap;
use crossbeam::utils::CachePadded;
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use log::trace;
use parking_lot::RwLock;
// use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

pub(crate) mod registered_active_message;
use registered_active_message::{exec_am_cmd, process_am_request, AMS_EXECS};

#[cfg(feature = "nightly")]
pub(crate) mod remote_closures;
#[cfg(feature = "nightly")]
pub(crate) use remote_closures::RemoteClosures;
#[cfg(feature = "nightly")]
use remote_closures::{exec_closure_cmd, process_closure_request};

lazy_static! {
    pub(crate) static ref REQUESTS: Vec<CHashMap<usize, InternalReq>> = {
        let mut reqs = Vec::new();
        for _i in 0..100 {
            reqs.push(CHashMap::new());
        }
        reqs
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

pub trait LamellarSerde {
    fn ser(&self,num_pes: usize) -> Vec<u8>;
    fn des(&self);
}
// #[async_trait]
pub trait LamellarActiveMessage: LamellarSerde {
    fn exec(
        self: Box<Self>,
        my_pe: usize,
        num_pes: usize,
        local: bool,
        world: Arc<LamellarTeamRT>,
        team: Arc<LamellarTeamRT>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<LamellarReturn>> + Send>>;
    fn get_id(&self) -> String;
}

pub(crate) type LamellarBoxedAm = Box<dyn LamellarActiveMessage + Send + Sync>;

// #[async_trait]
pub trait LocalAM{
    type Output: serde::ser::Serialize + serde::de::DeserializeOwned + Sync + Send;
}
pub trait LamellarAM {
    type Output: serde::ser::Serialize + serde::de::DeserializeOwned + Sync + Send;
    // fn exec(self, world: Arc<LamellarTeamRT>, team: Arc<LamellarTeamRT>) -> Self::Output;
}


pub trait LamellarDataReturn: std::fmt::Debug + std::any::Any {
    fn as_any(&self) -> &dyn std::any::Any;
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any>;
}

pub enum LamellarReturn {
    LocalData(Vec<u8>),
    LocalAm(LamellarBoxedAm),
    RemoteData(Vec<u8>),
    RemoteAm(LamellarBoxedAm),
}

pub(crate) type LamellarAny = Box<dyn std::any::Any + Send>;

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
pub(crate) enum Cmd {
    //inverse this so its the exec type holding the inner command...
    Exec,
    ExecReturn,
    LocalExec,
    ExecBatchUnitReturns,
    DataReturn,
    UnitReturn,
    BatchedUnitReturn,
    BatchedMsg,
    ExecBatchMsgSend,
}

#[repr(u8)]
#[derive(
    serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
pub(crate) enum InnerCmd {
    ClosureReq,
    ClosureReqLocal,
    ClosureResp,
    AmReq,
    AmReqLocal,
    AmResp,
    DataReq,
    DataResp,
    BatchReq,
    BatchResp,
    CompResp,
    Barrier,
    BarrierResp,
    UnitResp,
    BatchedUnitResp,
    BuffReq,
    NoHandleResp,
    AggregationReq,
    PutReq,
    GetReq,
    GetResp,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
pub(crate) struct Msg {
    pub cmd: ExecType,
    pub src: u16,
    pub req_id: usize,
    pub team_id: usize,
    pub return_data: bool,
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
    // pub(crate) am_exec_cnt: Arc<AtomicUsize>,
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
        self.outstanding_reqs.fetch_add(num, Ordering::SeqCst);
        self.send_req_cnt.fetch_add(num, Ordering::SeqCst);
    }
}

pub trait ActiveMessaging {
    fn wait_all(&self);
    fn barrier(&self);

    //     pub fn exec_am_all(...) -> LamellarRequest; // this probably requires an updated interface in lamellae to accept
    //                                                 // a list of pes to send to (currently the list is static and contains all pes)

    fn exec_am_all<F>(&self, am: F) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: LamellarActiveMessage + LamellarAM + Send + Sync + 'static;
    fn exec_am_pe<F>(
        &self,
        pe: usize,
        am: F,
    ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: LamellarActiveMessage + LamellarAM + Send + Sync + 'static;
    fn exec_am_local<F>(
            &self,
            am: F,
        ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
        where
            F: LamellarActiveMessage + LocalAM + Send + Sync + 'static;
}

//maybe make this a struct then we could hold the pending counters...
pub(crate) struct ActiveMessageEngine {
    pending_active: CHashMap<u16, usize>,
    pending_resp: CHashMap<u16, crossbeam::queue::SegQueue<usize>>,
    pending_msg_active: CHashMap<u64, usize>,
    pending_msg: Arc<RwLock<HashMap<u64, crossbeam::queue::SegQueue<Vec<u8>>>>>,
    my_pe: usize,
    num_pes: usize,
    fake_ireq: InternalReq,
    _fake_arch: Arc<StridedArch>,
    teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
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
        num_pes: usize,
        my_pe: usize,
        teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
    ) -> Self {
        trace!("registered funcs {:?}", AMS_EXECS.len(),);
        let (dummy_s, _) = crossbeam::channel::unbounded();
        ActiveMessageEngine {
            pending_active: CHashMap::new(),
            pending_resp: CHashMap::new(),
            pending_msg_active: CHashMap::new(),
            pending_msg: Arc::new(RwLock::new(HashMap::new())),
            my_pe: my_pe,
            num_pes: num_pes,
            // scheduler: scheduler,
            fake_ireq: InternalReq {
                data_tx: dummy_s,
                cnt: Arc::new(CachePadded::new(AtomicUsize::new(0usize))),
                start: std::time::Instant::now(),
                size: 0,
                active: Arc::new(AtomicBool::new(false)),
                team_outstanding_reqs: Arc::new(AtomicUsize::new(0usize)),
                world_outstanding_reqs: Arc::new(AtomicUsize::new(0usize)),
            },
            _fake_arch: Arc::new(StridedArch::new(my_pe, 1, 1)),
            teams: teams,
        }
    }

    pub(crate) async fn process_msg(&self, req_data: ReqData) -> Option<ReqData> {
        trace!("[{:?}] process msg: {:?}",self.my_pe, &req_data);
        if req_data.msg.return_data {
            REQUESTS[req_data.msg.req_id % REQUESTS.len()]
                .insert_new(req_data.msg.req_id, req_data.ireq.clone());
        }

        //we can probably include references to world and team in the ReqData
        let (world, team) = {
            let teams = self.teams.read();
            // println!("{:?}",&req_data);
            let world = teams
                .get(&0)
                .expect("invalid hash id")
                .upgrade()
                .expect("world team no longer exists");
            let team = teams
                .get(&req_data.team_hash)
                .expect("invalid hash id")
                .upgrade()
                .expect("team no longer exists");
            (world, team)
        };

        let batch = match req_data.msg.cmd.clone() {
            ExecType::Runtime(cmd) => {
                self.exec_runtime_cmd(
                    cmd,
                    req_data.msg,
                    req_data.lamellae.clone(),
                    vec![],
                    req_data.team_hash,
                    req_data.pe,
                    team.clone(),
                );
                None
            }
            ExecType::Am(_) => process_am_request(self, req_data, world, team.clone()).await,

            #[cfg(feature = "nightly")]
            ExecType::Closure(_) => process_closure_request(self, req_data, world, team.clone()),
        };
        if let Some((data, req_data)) = batch {
            let id = if let Some(pe) = req_data.pe {
                team.arch.world_pe(pe).expect("invalid pe") as u64 // aggregate to same pe across team boundaries
            } else {
                req_data.team_hash //use this to represent we want to send to all pes in the team
            };
            let mut pending_msg = self.pending_msg.write();
            let q = pending_msg
                .entry(id)
                .or_insert_with(crossbeam::queue::SegQueue::new);
            q.push(data);
            drop(pending_msg);
            if let None = self.pending_msg_active.insert(id, 1) {
                // no currently pending messages to send
                // create the runtime task to send data
                // self.batches_init.fetch_add(1,Ordering::SeqCst);
                let my_any: LamellarAny = Box::new(0);
                let msg = Msg {
                    cmd: ExecType::Runtime(Cmd::ExecBatchMsgSend),
                    src: self.my_pe as u16, //fake that this is from the original sender
                    req_id: 0,
                    team_id: id as usize, // we use this as the lookup id int the hashmaps
                    return_data: false,
                };
                Some(ReqData {
                    // team_arch: req_data,
                    src: self.my_pe,
                    pe: req_data.pe,
                    msg: msg,
                    ireq: self.fake_ireq.clone(),
                    func: my_any,
                    // backend: lamellae.backend(),
                    lamellae: req_data.lamellae.clone(),
                    team_hash: req_data.team_hash, // fake hash,
                })
            } else {
                None
            }
        } else {
            None
        }
    }

    pub(crate) fn exec_runtime_cmd(
        &self,
        cmd: Cmd,
        msg: Msg,
        lamellae: Arc<dyn LamellaeAM>,
        ser_data: Vec<u8>,
        team_hash: u64,
        pe: Option<usize>,
        team: Arc<LamellarTeamRT>,
    ) {
        // let msg = req_data.msg;
        trace!("[{:?}] exec_runtime_cmd: {:?}", self.my_pe, msg);
        match cmd {
            Cmd::ExecBatchMsgSend => {
                self.pending_msg_active.remove(&(msg.team_id as u64));
                let pending_msg = self.pending_msg.read();
                if let Some(pends) = pending_msg.get(&(msg.team_id as u64)) {
                    let mut size = 0;
                    let mut agg_data: Vec<Vec<u8>> = Vec::new();
                    while !pends.is_empty() {
                        if let Ok(data) = pends.pop() {
                            size += data.len();
                            agg_data.push(data);
                        }
                        if size > 100000 {
                            let rmsg = Msg {
                                cmd: ExecType::Runtime(Cmd::BatchedMsg),
                                src: self.my_pe as u16,
                                req_id: 0,
                                team_id: 0,
                                return_data: false,
                            };
                            let data = crate::serialize(&agg_data).unwrap();
                            let payload = (rmsg, data, team_hash);
                            lamellae.send_to_pes(
                                pe,
                                team.arch.clone(),
                                crate::serialize(&payload).unwrap(),
                            );
                            size = 0;
                            agg_data.clear();
                        }
                    }
                    if size > 0 {
                        let rmsg = Msg {
                            cmd: ExecType::Runtime(Cmd::BatchedMsg),
                            src: self.my_pe as u16,
                            req_id: 0,
                            team_id: 0,
                            return_data: false,
                        };
                        let data = crate::serialize(&agg_data).unwrap();
                        let payload = (rmsg, data, team_hash);
                        lamellae.send_to_pes(
                            pe,
                            team.arch.clone(),
                            crate::serialize(&payload).unwrap(),
                        );
                    }
                }
                drop(pending_msg);
            }
            Cmd::ExecBatchUnitReturns => {
                self.pending_active.remove(&msg.src);
                if let Some(pends) = self.pending_resp.get(&msg.src) {
                    let mut i = 1;
                    let mut ids: Vec<usize> = Vec::new();
                    while !pends.is_empty() {
                        if let Ok(id) = pends.pop() {
                            ids.push(id);
                        }
                        i += 1;
                        if i > 100_000 {
                            let rmsg = Msg {
                                cmd: ExecType::Runtime(Cmd::BatchedUnitReturn),
                                src: self.my_pe as u16,
                                req_id: msg.req_id,
                                team_id: msg.team_id,
                                return_data: false,
                            };
                            let data = crate::serialize(&ids).unwrap();
                            let payload = (rmsg, data, team_hash);
                            lamellae
                                .send_to_pe(msg.src as usize, crate::serialize(&payload).unwrap());
                            i = 0;
                            ids.clear();
                        }
                    }
                    if i > 0 {
                        let rmsg = Msg {
                            cmd: ExecType::Runtime(Cmd::BatchedUnitReturn),
                            src: self.my_pe as u16,
                            req_id: msg.req_id,
                            team_id: msg.team_id,
                            return_data: false,
                        };
                        let data = crate::serialize(&ids).unwrap();
                        let payload = (rmsg, data, team_hash);
                        lamellae.send_to_pe(msg.src as usize, crate::serialize(&payload).unwrap());
                        ids.clear();
                    }
                }
                self.pending_resp.alter(msg.src, |q| match q {
                    Some(q) => {
                        if q.len() > 0 {
                            Some(q)
                        } else {
                            None
                        }
                    }
                    None => None,
                })
            }
            Cmd::DataReturn => {
                self.send_data_to_user_handle(msg.req_id, msg.src, Some(ser_data));
            }
            Cmd::BatchedUnitReturn => {
                let ids: std::vec::Vec<usize> = crate::deserialize(&ser_data).unwrap();
                for id in &ids {
                    self.send_data_to_user_handle(*id, msg.src, None);
                }
            }
            Cmd::UnitReturn => {
                self.send_data_to_user_handle(msg.req_id, msg.src, None);
            }
            _ => {
                panic!("invalid cmd time for runtime cmd: {:?}", msg.cmd);
            }
        }
    }

    pub(crate) async fn exec_msg(
        &self,
        msg: Msg,
        ser_data: Vec<u8>,
        lamellae: Arc<dyn LamellaeAM>,
        team_hash: u64,
    ) -> Option<ReqData> {
        trace!("[{:?}] exec_msg: {:?}", self.my_pe, msg);
        let (world, team) = {
            let teams = self.teams.read();
            let world = teams
                .get(&0)
                .expect("invalid hash id")
                .upgrade()
                .expect("world team no longer exists");
            let team = teams
                .get(&team_hash)
                .expect("invalid hash id")
                .upgrade()
                .expect("team no longer exists");
            (world, team)
        };
        trace!("using team {:?}", team.my_hash);
        match msg.cmd.clone() {
            ExecType::Am(cmd) => exec_am_cmd(self, cmd, msg, ser_data, lamellae, world, team).await,
            #[cfg(feature = "nightly")]
            ExecType::Closure(cmd) => {
                exec_closure_cmd(self, cmd, msg, ser_data, lamellae, world, team)
            }
            ExecType::Runtime(cmd) => {
                self.exec_runtime_cmd(cmd, msg, lamellae, ser_data, team_hash, None, team);
                None
            }
        }
    }

    fn send_data_to_user_handle(&self, req_id: usize, pe: u16, data: Option<std::vec::Vec<u8>>) {
        let res = REQUESTS[req_id % REQUESTS.len()].get(&req_id);
        match res {
            Some(v) => {
                let ireq = v.clone();
                drop(v); //release lock in the hashmap
                trace!("[{:?}] send_data_to_user_handle {:?}", self.my_pe, ireq);
                ireq.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                ireq.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                if let Ok(_) = ireq.data_tx.send((pe as usize, data)) {} //if this returns an error it means the user has dropped the handle
                let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
                if cnt == 1 {
                    REQUESTS[req_id % REQUESTS.len()].remove(&req_id);
                }
            }
            None => println!("error id not found"),
        }
    }

    fn send_response(
        &self,
        cmd: ExecType,
        data: Option<std::vec::Vec<u8>>,
        msg: Msg,
        lamellae: Arc<dyn LamellaeAM>,
        team_hash: u64,
    ) -> Option<ReqData> {
        // let s = Instant::now();
        trace!(
            "[{:?}] send response {:?} {:?} {:?}",
            self.my_pe,
            cmd,
            msg,
            lamellae.backend()
        );
        match cmd {
            ExecType::Runtime(Cmd::BatchedUnitReturn) => {
                // self.pending_resp[msg.src as usize].push(msg.req_id);
                self.pending_resp.upsert(
                    msg.src,
                    || {
                        let q = crossbeam::queue::SegQueue::new();
                        q.push(msg.req_id);
                        q
                    },
                    |q| {
                        q.push(msg.req_id);
                    },
                );
                if let None = self.pending_active.insert(msg.src, 1) {
                    // self.batched_return_init.fetch_add(1,Ordering::SeqCst);
                    let my_any: LamellarAny = Box::new(0);
                    let msg = Msg {
                        cmd: ExecType::Runtime(Cmd::ExecBatchUnitReturns),
                        src: msg.src as u16, //fake that this is from the original sender
                        req_id: 0,
                        team_id: 0,
                        return_data: false,
                    };
                    Some(ReqData {
                        // team_arch: self.fake_arch.clone(),
                        src: self.my_pe,
                        pe: Some(self.my_pe),
                        msg: msg,
                        ireq: self.fake_ireq.clone(),
                        func: my_any,
                        // backend: lamellae.backend(),
                        lamellae: lamellae,
                        team_hash: team_hash, // fake hash,
                    })
                } else {
                    None
                }
            }
            ExecType::Runtime(Cmd::UnitReturn) => {
                // self.returns.fetch_add(1,Ordering::SeqCst);
                let rmsg = Msg {
                    cmd: cmd,
                    src: self.my_pe as u16,
                    req_id: msg.req_id,
                    team_id: msg.team_id,
                    return_data: false,
                };
                let payload = (rmsg, crate::serialize(&()).unwrap(), team_hash);
                lamellae.send_to_pe(msg.src as usize, crate::serialize(&payload).unwrap());
                None
            }
            ExecType::Runtime(Cmd::DataReturn) | ExecType::Am(Cmd::ExecReturn) => {
                // self.returns.fetch_add(1,Ordering::SeqCst);
                let rmsg = Msg {
                    cmd: cmd,
                    src: self.my_pe as u16,
                    req_id: msg.req_id,
                    team_id: msg.team_id,
                    return_data: false,
                };
                let data = data.unwrap();
                let payload = (rmsg, data, team_hash);
                lamellae.send_to_pe(msg.src as usize, crate::serialize(&payload).unwrap());
                None
            }
            #[cfg(feature = "nightly")]
            ExecType::Closure(Cmd::ExecReturn) => {
                let rmsg = Msg {
                    cmd: cmd,
                    src: self.my_pe as u16,
                    req_id: msg.req_id,
                    team_id: msg.team_id,
                    return_data: false,
                };
                let data = data.unwrap();
                let payload = (rmsg, data, team_hash);
                lamellae.send_to_pe(msg.src as usize, crate::serialize(&payload).unwrap());
                None
            }
            _ => {
                trace!("send_resp unknown command {:#?}", cmd);
                None
            }
        }
        // let b = s.elapsed().as_millis() as usize;
        // (*self.timers.get(&msg.cmd).unwrap()).fetch_add(b, Ordering::Relaxed);
    }
}

use crate::lamellae::{Lamellae,LamellaeAM, SerializedData,SerializeHeader,Ser,Des};
use crate::lamellar_arch::StridedArch;
use crate::lamellar_request::{InternalReq, LamellarRequest, InternalResult};
use crate::lamellar_team::LamellarTeamRT;
use crate::scheduler::{NewReqData,AmeScheduler};
// use async_trait::async_trait;
use chashmap::CHashMap;
use crossbeam::utils::CachePadded;
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use log::trace;
use parking_lot::{Mutex,RwLock};
// use future_parking_lot::rwlock::{FutureReadable, FutureWriteable};
// use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

pub(crate) mod registered_active_message;
use registered_active_message::{AMS_EXECS,RegisteredActiveMessages};
// pub(crate) mod batched_registered_active_message;
// use batched_registered_active_message::BatchedActiveMessages;

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
    // pub(crate) static ref REQUESTS: Vec<CHashMap<usize, InternalReq>> = { //no reason for this to be static... we can put it im AME
    //     let mut reqs = Vec::new();
    //     for _i in 0..100 {
    //         reqs.push(CHashMap::new());
    //     }
    //     reqs
    // };
    pub(crate) static ref REQUESTS: Mutex<HashMap<usize, InternalReq>> = { //no reason for this to be static... we can put it im AME
       Mutex::new(HashMap::new())
        // for _i in 0..100 {
        //     reqs.push(CHashMap::new());
        // }
        // reqs
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



pub trait LamellarActiveMessage: DarcSerde + LamellarSerde + LamellarResultSerde {
    fn exec(
        self: Arc<Self>,
        my_pe: usize,
        num_pes: usize,
        local: bool,
        world: Arc<LamellarTeamRT>,
        team: Arc<LamellarTeamRT>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = LamellarReturn> + Send>>;
    fn get_id(&self) -> String;
}

#[derive(Clone)]
pub (crate) enum LamellarFunc{
    Am(LamellarArcAm),
    // Closure(LamellarAny),
    Result(LamellarResultArc),
    None,
}

pub(crate) type LamellarArcAm = Arc<dyn LamellarActiveMessage + Send + Sync>;
pub(crate) type LamellarBoxedAm = Box<dyn LamellarActiveMessage + Send + Sync>;
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
    LocalAm(LamellarBoxedAm),
    // RemoteData(LamellarAny,LamellarBoxedAm),
    RemoteData(LamellarResultArc),
    RemoteAm(LamellarBoxedAm),
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
        let num_reqs=self.outstanding_reqs.fetch_add(num, Ordering::SeqCst);
        println!("reqs: {:?}",num_reqs+num);
        self.send_req_cnt.fetch_add(num, Ordering::SeqCst);
    }
}

pub trait ActiveMessaging {
    fn wait_all(&self);
    fn barrier(&self);
    fn exec_am_all<F>(&self, am: F) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: LamellarActiveMessage + LamellarAM +Serde + Send + Sync + 'static;
    fn exec_am_pe<F>(
        &self,
        pe: usize,
        am: F,
    ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: LamellarActiveMessage + LamellarAM + Serde + Send + Sync + 'static;
    fn exec_am_local<F>(
            &self,
            am: F,
        ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
        where
            F: LamellarActiveMessage + LocalAM  + Send + Sync + 'static;
}

//maybe make this a struct then we could hold the pending counters...
pub(crate) struct ActiveMessageEngine {
    pending_active: Arc<Mutex<HashMap<u16,usize>>>, //CHashMap<u16, usize>,
    pending_resp: Arc<Mutex<HashMap<u16, crossbeam::queue::SegQueue<usize>>>>,// CHashMap<u16, crossbeam::queue::SegQueue<usize>>,
    pending_msg_active: CHashMap<u64, usize>,
    pending_msg: Arc<Mutex<HashMap<u64, crossbeam::queue::SegQueue<Vec<u8>>>>>,
    my_pe: usize,
    num_pes: usize,
    fake_ireq: InternalReq,
    _fake_arch: Arc<StridedArch>,
    teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
    pub(crate) scheduler: Weak<AmeScheduler>,
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
        num_pes: usize,
        my_pe: usize,
        teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
        scheduler: Weak<AmeScheduler>,
    ) -> Self {
        trace!("registered funcs {:?}", AMS_EXECS.len(),);
        // println!("sizes: {:?} {:?} {:?}",std::mem::size_of::<Msg>(),std::mem::size_of::<ExecType>(),std::mem::size_of::<Cmd>());
        let (dummy_s, _) = crossbeam::channel::unbounded();
        ActiveMessageEngine {
            pending_active: Arc::new(Mutex::new(HashMap::new())),
            pending_resp: Arc::new(Mutex::new(HashMap::new())),
            pending_msg_active: CHashMap::new(),
            pending_msg: Arc::new(Mutex::new(HashMap::new())),
            my_pe: my_pe,
            num_pes: num_pes,
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
            scheduler: scheduler.clone(),
            batched_am: Arc::new(RegisteredActiveMessages::new(scheduler.upgrade().unwrap())),
        }
    }

    // pub(crate) async fn process_msg(&self, req_data: ReqData) -> Option<ReqData> {
        // // trace!("[{:?}] process msg: {:?}",self.my_pe, &req_data);
        // if !req_data.rt_req {//msg.return_data {
        //     REQUESTS[req_data.msg.req_id % REQUESTS.len()]
        //         .insert_new(req_data.msg.req_id, req_data.ireq.clone());
        // }

        // //we can probably include references to world and team in the ReqData
        // let (world, team) = {
        //     let teams = self.teams.read();
        //     // println!("{:?}",&req_data);
        //     let world = teams
        //         .get(&0)
        //         .expect("invalid hash id")
        //         .upgrade()
        //         .expect("world team no longer exists");
        //     let team = teams
        //         .get(&req_data.team_hash)
        //         .expect("invalid hash id")
        //         .upgrade()
        //         .expect("team no longer exists");
        //     (world, team)
        // };

        // let batch: Option<(Vec<u8>, ReqData)> = match req_data.msg.cmd.clone() {
        //     ExecType::Runtime(cmd) => {
        //         self.exec_runtime_cmd(
        //             cmd,
        //             req_data.msg,
        //             req_data.lamellae.clone(),
        //             None,
        //             req_data.team_hash,
        //             req_data.pe,
        //             team.clone(),
        //         ).await;
        //         None
        //     }
        //     // ExecType::Am(_) => process_am_request(self, req_data, world, team.clone()).await,
        //     ExecType::Am(_) => { self.batched_am.process_am_req(req_data,world,team.clone()).await;
        //         None
        //     },

        //     #[cfg(feature = "nightly")]
        //     ExecType::Closure(_) => process_closure_request(self, req_data, world, team.clone()),
        // };
    //     None
        
    // }

    pub(crate) async fn process_msg_new(&self, req_data: NewReqData, ireq: Option<InternalReq>)  {
        // trace!("[{:?}] process msg: {:?}",self.my_pe, &req_data);
        if let Some (ireq) = ireq{
            REQUESTS.lock().insert(req_data.id, ireq.clone());
            // REQUESTS[req_data.id % REQUESTS.len()]
            //     .insert_new(req_data.id, ireq.clone());
        }

        match req_data.cmd.clone() {
            ExecType::Runtime(cmd) => {
            //     self.exec_runtime_cmd(
            //         cmd,
            //         req_data.msg,
            //         req_data.lamellae.clone(),
            //         None,
            //         req_data.team_hash,
            //         req_data.pe,
            //         team.clone(),
            //     ).await;
            //     None
            }
            // ExecType::Am(_) => process_am_request(self, req_data, world, team.clone()).await,
            ExecType::Am(_) => self.batched_am.process_am_req(req_data).await,

            #[cfg(feature = "nightly")]
            ExecType::Closure(_) => process_closure_request(self, req_data, world, team.clone()),
        }
        
    }

    pub(crate) async fn exec_runtime_cmd(
        &self,
        cmd: Cmd,
        msg: Msg,
        lamellae: Arc<Lamellae>,
        ser_data: Option<SerializedData>,
        team_hash: u64,
        pe: Option<usize>,
        team: Arc<LamellarTeamRT>,
    ) {
        // let msg = req_data.msg;
        trace!("[{:?}] exec_runtime_cmd: {:?}", self.my_pe, msg);
        match cmd {
            Cmd::ExecBatchMsgSend => {
                self.pending_msg_active.remove(&team_hash);
                let pends = {
                    let mut pending_msg = self.pending_msg.lock();
                    pending_msg.remove(&team_hash)
                };
                if let Some(pends) = pends {
                    let mut size = 0;
                    let mut agg_data: Vec<Vec<u8>> = Vec::new();
                    while !pends.is_empty() {
                        if let Some(data) = pends.pop() {
                            size += data.len();
                            agg_data.push(data);
                        }
                        if size > 100000 {
                            let rmsg = Msg {
                                cmd: ExecType::Runtime(Cmd::BatchedMsg),
                                src: self.my_pe as u16,
                                req_id: 0,
                                // team_id: 0,
                                // return_data: false,
                            };
                            let header = Some(SerializeHeader{msg: rmsg, team_hash: team_hash,id: 0});
                            let data = lamellae.serialize(header,&agg_data).await.unwrap();

                            lamellae.send_to_pes_async(
                                pe,
                                team.arch.clone(),
                                data,).await;
                            size = 0;
                            agg_data.clear();
                        }
                    }
                    if size > 0 {
                        let rmsg = Msg {
                            cmd: ExecType::Runtime(Cmd::BatchedMsg),
                            src: self.my_pe as u16,
                            req_id: 0,
                            // team_id: 0,
                            // return_data: false,
                        };
                        let header = Some(SerializeHeader{msg: rmsg, team_hash: team_hash,id: 0});
                        let data = lamellae.serialize(header,&agg_data).await.unwrap();
                        lamellae.send_to_pes_async(
                            pe,
                            team.arch.clone(),
                            data,).await;
                    }
                }
            }
            Cmd::ExecBatchUnitReturns => {
                let pends = {
                    let mut pending_resp = self.pending_resp.lock();
                    self.pending_active.lock().remove(&msg.src);
                    pending_resp.remove(&msg.src)
                };
                if let Some(pends) = pends {
                    
                    let mut i = 1usize;
                    let mut ids: Vec<usize> = Vec::new();
                    while !pends.is_empty() {
                        if let Some(id) = pends.pop() {
                            ids.push(id);
                        }
                        i += 1;
                        if i > 100_000 {
                            let rmsg = Msg {
                                cmd: ExecType::Runtime(Cmd::BatchedUnitReturn),
                                src: self.my_pe as u16,
                                req_id: msg.req_id,
                                // team_id: msg.team_id,
                                // return_data: false,
                            };
                            let header = Some(SerializeHeader{msg: rmsg, team_hash: team_hash, id: 0});
                            let data = lamellae.serialize(header,&ids).await.unwrap();
                            lamellae
                                .send_to_pe_async(msg.src as usize,  data).await;
                            i = 0;
                            ids.clear();
                        }
                    }
                    if i > 0 {
                        let rmsg = Msg {
                            cmd: ExecType::Runtime(Cmd::BatchedUnitReturn),
                            src: self.my_pe as u16,
                            req_id: msg.req_id,
                            // team_id: msg.team_id,
                            // return_data: false,
                        };
                        let header = Some(SerializeHeader{msg: rmsg, team_hash: team_hash, id: 0});
                        let data = lamellae.serialize(header,&ids).await.unwrap();
                        lamellae.send_to_pe_async(msg.src as usize, data).await;
                        ids.clear();
                    }
                }
            }
            Cmd::DataReturn => {
                if let Some(data) = ser_data{
                    ActiveMessageEngine::send_data_to_user_handle(msg.req_id, msg.src, InternalResult::Remote(data));
                }
                else {
                    panic!("should i be here?");
                }
            }
            Cmd::BatchedUnitReturn => {
                if let Some(data) = ser_data{
                    let ids: std::vec::Vec<usize> = data.deserialize_data().unwrap();//crate::deserialize_data(&ser_data).unwrap();
                    for id in &ids {
                        ActiveMessageEngine::send_data_to_user_handle(*id, msg.src, InternalResult::Unit);
                    }
                }
                else{
                    panic!("should i be here?");
                }
            }
            Cmd::UnitReturn => {
                ActiveMessageEngine::send_data_to_user_handle(msg.req_id, msg.src, InternalResult::Unit);
            }
            _ => {
                panic!("invalid cmd time for runtime cmd: {:?}", msg.cmd);
            }
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
            // ExecType::Am(cmd) => exec_am_cmd(self, cmd, msg, ser_data, lamellae, world, team).await, //execute a remote am
            ExecType::Am(cmd) => {
                self.batched_am.process_batched_am(ame, cmd, msg, ser_data, lamellae, world, team).await;
            }, //execute a remote am

            #[cfg(feature = "nightly")]
            ExecType::Closure(cmd) => {
                exec_closure_cmd(self, cmd, msg, ser_data, lamellae, world, team)
            }
            ExecType::Runtime(cmd) => {
                self.exec_runtime_cmd(cmd, msg, lamellae, Some(ser_data), team_hash, None, team).await;
            }
        }
    }

    // pub(crate) async fn exec_msg_old(
    //     &self,
    //     msg: Msg,
    //     ser_data: SerializedData,
    //     lamellae: Arc<Lamellae>,
    //     team_hash: u64,
    // ) -> Option<ReqData> {
    //     trace!("[{:?}] exec_msg: {:?}", self.my_pe, msg);
    //     let (world, team) = {
    //         let teams = self.teams.read();
    //         let world = teams
    //             .get(&0)
    //             .expect("invalid hash id")
    //             .upgrade()
    //             .expect("world team no longer exists");
    //         let team = teams
    //             .get(&team_hash)
    //             .expect("invalid hash id")
    //             .upgrade()
    //             .expect("team no longer exists");
    //         (world, team)
    //     };
    //     trace!("using team {:?}", team.my_hash);
    //     match msg.cmd.clone() {
    //         // ExecType::Am(cmd) => exec_am_cmd(self, cmd, msg, ser_data, lamellae, world, team).await, //execute a remote am
    //         ExecType::Am(cmd) => {
    //             self.batched_am.process_batched_am(cmd, msg, ser_data, lamellae, world, team).await;
    //             None
    //         }, //execute a remote am

    //         #[cfg(feature = "nightly")]
    //         ExecType::Closure(cmd) => {
    //             exec_closure_cmd(self, cmd, msg, ser_data, lamellae, world, team)
    //         }
    //         ExecType::Runtime(cmd) => {
    //             self.exec_runtime_cmd(cmd, msg, lamellae, Some(ser_data), team_hash, None, team).await;
    //             None
    //         }
    //     }
    // }

    

    // make this an associated function... or maybe make a "REQUESTS struct which will have a send_data_to_user_handle" 
    fn send_data_to_user_handle(req_id: usize, pe: u16, data: InternalResult) { 
        let reqs = REQUESTS.lock();
        // let res = REQUESTS[req_id % REQUESTS.len()].get(&req_id);
        // println!("finalize {:?}",req_id);
        match reqs.get(&req_id) {
            Some(ireq) => {
                let ireq = ireq.clone();
                drop(reqs); //release lock in the hashmap
                // trace!("[{:?}] send_data_to_user_handle {:?}", self.my_pe, ireq);
                let num_reqs = ireq.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                println!("team reqs: {:?}",num_reqs);
                let num_reqs =ireq.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                println!("world reqs: {:?}",num_reqs);
                if let Ok(_) = ireq.data_tx.send((pe as usize,data)) {} //if this returns an error it means the user has dropped the handle
                let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
                if cnt == 1 {
                    REQUESTS.lock().remove(&req_id);
                    // REQUESTS[req_id % REQUESTS.len()].remove(&req_id);
                }
            }
            None => panic!("error id not found"),
        }
    }

//     async fn send_response( //will always be to remote node
//         &self,
//         cmd: ExecType,
//         data: LamellarReturn, // this should be option<Box<dyn Ser/De>> LamellarReturn
//         msg: Msg,
//         lamellae: Arc<Lamellae>,
//         team_hash: u64,
//     ) -> Option<ReqData> {
//         // let s = Instant::now();
//         trace!(
//             "[{:?}] send response {:?} {:?} {:?}",
//             self.my_pe,
//             cmd,
//             msg,
//             lamellae.backend()
//         );
//         match cmd {
//             ExecType::Runtime(Cmd::BatchedUnitReturn) => {
//                 // let active = {
//                 //     self.pending_resp.lock().entry(msg.src)
//                 //     .and_modify(|e| { e.push(msg.req_id) })
//                 //     .or_insert_with(|| {
//                 //         let q = crossbeam::queue::SegQueue::new();
//                 //         q.push(msg.req_id);
//                 //         q
//                 //     });
//                 //     self.pending_active.lock().insert(msg.src, 1)
//                 // };
//                 // if let None =  active{
//                 //     let my_any: LamellarAny = Box::new(0);
//                 //     let msg = Msg {
//                 //         cmd: ExecType::Runtime(Cmd::ExecBatchUnitReturns),
//                 //         src: msg.src as u16, //fake that this is from the original sender
//                 //         req_id: 0,
//                 //     };
//                 //     Some(ReqData {
//                 //         src: self.my_pe,
//                 //         pe: Some(self.my_pe),
//                 //         msg: msg,
//                 //         ireq: self.fake_ireq.clone(),
//                 //         func: my_any,
//                 //         lamellae: lamellae,
//                 //         team_hash: team_hash, // fake hash,
//                 //         rt_req: true,
//                 //     })
//                 // } else {
//                 //     None
//                 // }
//                 None
//             }
//             ExecType::Runtime(Cmd::UnitReturn) => {
//                 let rmsg = Msg {
//                     cmd: cmd,
//                     src: self.my_pe as u16,
//                     req_id: msg.req_id,
//                 };
//                 let header = Some(SerializeHeader{msg: rmsg, team_hash: team_hash, id: 0});
//                 let data = lamellae.serialize(header,&()).await.unwrap();
//                 lamellae.send_to_pe_async(msg.src as usize,  data
//                 ).await;
//                 None
//             }
//             ExecType::Runtime(Cmd::DataReturn) =>{
//                 let rmsg = Msg {
//                     cmd: cmd,
//                     src: self.my_pe as u16,
//                     req_id: msg.req_id,
//                     // team_id: msg.team_id,
//                     // return_data: false,
//                 };
//                 if let LamellarReturn::RemoteData(result,func) = data{
//                     let result_size = func.serialized_result_size(&result);
//                     let header = Some(SerializeHeader{msg: rmsg, team_hash: team_hash, id: 0});
//                     let data = lamellae.serialize_header(header,result_size).await.unwrap();
//                     func.serialize_result_into(data.data_as_bytes(),&result);
//                     lamellae.send_to_pe_async(msg.src as usize,  data).await;
//                 }
//                 else{
//                     panic!("should i be here?");
//                 }
//                 None
//             }
//             ExecType::Am(Cmd::ExecReturn)=> {
//                 let rmsg = Msg {
//                     cmd: cmd,
//                     src: self.my_pe as u16,
//                     req_id: msg.req_id,
//                     // team_id: msg.team_id,
//                     // return_data: false,
//                 };
//                 if let LamellarReturn::RemoteAm(am) = data {
//                     let id = *AMS_IDS.get(&am.get_id()).unwrap();
//                     let header = Some(SerializeHeader{msg: rmsg, team_hash: team_hash, id: id});
//                     let serialized_size = am.serialized_size();
//                     let data = lamellae.serialize_header(header,serialized_size).await.unwrap();
//                     am.serialize_into(data.data_as_bytes());
//                     lamellae.send_to_pe_async(msg.src as usize,  data).await;
//                 }
//                 else{
//                     panic!("Should i be here?");
//                 }
//                 None
//             }
//             #[cfg(feature = "nightly")]
//             ExecType::Closure(Cmd::ExecReturn) => {
//                 let rmsg = Msg {
//                     cmd: cmd,
//                     src: self.my_pe as u16,
//                     req_id: msg.req_id,
//                     // team_id: msg.team_id,
//                     // return_data: false,
//                 };
//                 // let data = data.unwrap();
//                 // let payload = (rmsg, data, team_hash);
//                 let header = Some(SerializeHeader{msg: rmsg, team_hash: team_hash});
//                 let data = lamellae.serialize(header,data).await.unwrap();
//                 lamellae.send_to_pe_async(msg.src as usize, data,
//                     crate::lamellae::serialize(&payload,lamellae.clone()).await.unwrap()
//                 ).await;
//                 None
//             }
//             _ => {
//                 trace!("send_resp unknown command {:#?}", cmd);
//                 None
//             }
//         }
//         // let b = s.elapsed().as_millis() as usize;
//         // (*self.timers.get(&msg.cmd).unwrap()).fetch_add(b, Ordering::Relaxed);
//     }
// }
}

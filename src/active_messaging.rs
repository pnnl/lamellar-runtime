use crate::lamellae::{Backend, LamellaeAM};
use crate::lamellar_request::{InternalReq, LamellarRequest};
use crate::lamellar_team::StridedArch;
use crate::schedulers::{ReqData, SchedulerQueue};
use chashmap::CHashMap;
use log::trace;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

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

pub trait LamellarActiveMessage {
    fn exec(&self, my_pe: isize, num_pes: isize, local: bool) -> Option<LamellarReturn>;
    fn get_id(&self) -> String;
    fn ser(&self) -> Vec<u8>;
}
pub(crate) type LamellarBoxedAm = Box<dyn LamellarActiveMessage + Send>;

pub trait LamellarAM {
    type Output: serde::ser::Serialize + serde::de::DeserializeOwned;
    fn exec(&self) -> Self::Output;
}

pub trait LamellarDataReturn: std::fmt::Debug + std::any::Any {
    fn as_any(&self) -> &dyn std::any::Any;
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any>;
}

pub enum LamellarReturn {
    LocalData(Vec<u8>),
    LocalAm(Box<dyn LamellarActiveMessage>),
    RemoteData(Vec<u8>),
    RemoteAm(Box<dyn LamellarActiveMessage>),
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
}

impl AMCounters {
    pub(crate) fn new() -> AMCounters {
        AMCounters {
            outstanding_reqs: Arc::new(AtomicUsize::new(0)),
            send_req_cnt: AtomicUsize::new(0),
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

    fn exec_am_all<F>(&self, am: F) -> LamellarRequest<F::Output>
    where
        F: LamellarActiveMessage
            + LamellarAM
            + Send
            // + serde::de::DeserializeOwned
            // + std::clone::Clone
            + 'static;
    fn exec_am_pe<F>(&self, pe: usize, am: F) -> LamellarRequest<F::Output>
    where
        F: LamellarActiveMessage
            + LamellarAM
            + Send
            + serde::de::DeserializeOwned
            // + std::clone::Clone
            + 'static;
}

//maybe make this a struct then we could hold the pending counters...
pub(crate) struct ActiveMessageEngine {
    pending_active: CHashMap<u16, usize>,
    pending_resp: CHashMap<u16, crossbeam::queue::SegQueue<usize>>,
    my_pe: usize,
    num_pes: usize,
    scheduler: Arc<dyn SchedulerQueue>,
    fake_ireq: InternalReq,
    fake_arch: Arc<StridedArch>,
    timers: BTreeMap<Cmd, AtomicUsize>,
}

fn create_cmd_timers() -> BTreeMap<Cmd, AtomicUsize> {
    let timers: BTreeMap<Cmd, AtomicUsize> = BTreeMap::new();
    // timers.insert(Cmd::ClosureReq, AtomicUsize::new(0));
    // timers.insert(Cmd::ClosureReqLocal, AtomicUsize::new(0));
    // timers.insert(Cmd::ClosureResp, AtomicUsize::new(0));
    // timers.insert(Cmd::AmReq, AtomicUsize::new(0));
    // timers.insert(Cmd::AmReqLocal, AtomicUsize::new(0));
    // timers.insert(Cmd::AmResp, AtomicUsize::new(0));
    // timers.insert(Cmd::DataReq, AtomicUsize::new(0));
    // timers.insert(Cmd::DataResp, AtomicUsize::new(0));
    // timers.insert(Cmd::BatchReq, AtomicUsize::new(0));
    // timers.insert(Cmd::BatchResp, AtomicUsize::new(0));
    // timers.insert(Cmd::CompResp, AtomicUsize::new(0));
    // timers.insert(Cmd::Barrier, AtomicUsize::new(0));
    // timers.insert(Cmd::BarrierResp, AtomicUsize::new(0));
    // timers.insert(Cmd::UnitResp, AtomicUsize::new(0));
    // timers.insert(Cmd::BatchedUnitResp, AtomicUsize::new(0));
    // timers.insert(Cmd::BuffReq, AtomicUsize::new(0));
    // timers.insert(Cmd::NoHandleResp, AtomicUsize::new(0));
    // timers.insert(Cmd::AggregationReq, AtomicUsize::new(0));
    // timers.insert(Cmd::PutReq, AtomicUsize::new(0));
    // timers.insert(Cmd::GetReq, AtomicUsize::new(0));
    // timers.insert(Cmd::GetResp, AtomicUsize::new(0));
    timers
}

impl Drop for ActiveMessageEngine {
    fn drop(&mut self) {
        trace!("[{:?}] AME dropping", self.my_pe); 
        let mut string = String::new();
        for item in self.timers.iter() {
            string.push_str(&format!(
                "{:?} {:?} ",
                item.0,
                item.1.load(Ordering::SeqCst) as f64 / 1000.0 as f64
            ));
        }
        // println!("timers: {:?} {:?}", self.my_pe, string);
    }
}

impl ActiveMessageEngine {
    pub(crate) fn new(num_pes: usize, my_pe: usize, scheduler: Arc<dyn SchedulerQueue>) -> Self {
        trace!("registered funcs {:?}", AMS_EXECS.len(),);
        let (dummy_s, _) = crossbeam::channel::unbounded();
        ActiveMessageEngine {
            pending_active: CHashMap::new(),
            pending_resp: CHashMap::new(),
            my_pe: my_pe,
            num_pes: num_pes,
            scheduler: scheduler,
            fake_ireq: InternalReq {
                data_tx: dummy_s,
                cnt: Arc::new(AtomicUsize::new(0usize)),
                start: std::time::Instant::now(),
                size: 0,
                active: Arc::new(AtomicBool::new(false)),
                team_outstanding_reqs: Arc::new(AtomicUsize::new(0usize)),
                world_outstanding_reqs: Arc::new(AtomicUsize::new(0usize)),
            },
            fake_arch: Arc::new(StridedArch::new( my_pe, my_pe, 1, num_pes)),
            timers: create_cmd_timers(),
        }
    }

    pub(crate) fn process_msg(
        &self,
        req_data: ReqData,
        lamellaes: &Arc<BTreeMap<Backend, Arc<dyn LamellaeAM>>>,
    ) {
        if req_data.msg.return_data {
            REQUESTS[req_data.msg.req_id % REQUESTS.len()]
                .insert_new(req_data.msg.req_id, req_data.ireq.clone());
        }

        match req_data.msg.cmd.clone() {
            ExecType::Runtime(cmd) => {
                self.exec_runtime_cmd(
                    cmd,
                    req_data.msg,
                    lamellaes[&req_data.backend].clone(),
                    vec![],
                );
            }
            ExecType::Am(_) => {
                process_am_request(self, req_data, lamellaes);
            }

            #[cfg(feature = "nightly")]
            ExecType::Closure(_) => {
                process_closure_request(self, req_data, lamellaes);
            }
        }
    }

    pub(crate) fn exec_runtime_cmd(
        &self,
        cmd: Cmd,
        msg: Msg,
        lamellae: Arc<dyn LamellaeAM>,
        ser_data: Vec<u8>,
    ) {
        trace!("[{:?}] exec_runtime_cmd: {:?}", self.my_pe, msg);
        // let s = Instant::now();
        match cmd {
            Cmd::ExecBatchUnitReturns => {
                self.pending_active.remove(&msg.src);
                if let Some(pends) = self.pending_resp.get(&msg.src) {
                    let mut i = 1;
                    let mut ids: Vec<usize> = Vec::new();
                    while i < 100_000 && pends.len() > 0 {
                        if let Ok(id) = pends.pop() {
                            ids.push(id);
                        }
                        i += 1;
                    }
                    let rmsg = Msg {
                        cmd: ExecType::Runtime(Cmd::BatchedUnitReturn),
                        src: self.my_pe as u16,
                        req_id: msg.req_id,
                        team_id: msg.team_id,
                        return_data: false,
                    };
                    let data = bincode::serialize(&ids).unwrap();
                    let payload = (rmsg, data);
                    lamellae.send_to_pe(msg.src as usize, bincode::serialize(&payload).unwrap());
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
                // println!("{:?} received data resp from {:?} resp for req: {:?} {:?}",LAMELLAR_RT.arch.my_pe,msg.src, msg.id,LAMELLAR_RT.counters.outstanding_reqs.load(Ordering::SeqCst));
                self.send_data_to_user_handle(msg.req_id, msg.src, Some(ser_data));
            }
            Cmd::BatchedUnitReturn => {
                let ids: std::vec::Vec<usize> = bincode::deserialize(&ser_data).unwrap();
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
        // let b = s.elapsed().as_millis() as usize;
        // (*self.timers.get(&msg.cmd).unwrap()).fetch_add(b, Ordering::Relaxed);
    }

    pub(crate) fn exec_msg(&self, msg: Msg, ser_data: Vec<u8>, lamellae: Arc<dyn LamellaeAM>) {
        // let s = Instant::now();
        trace!("[{:?}] exec_msg: {:?}", self.my_pe, msg);
        match msg.cmd.clone() {
            ExecType::Am(cmd) => {
                exec_am_cmd(self, cmd, msg, ser_data, lamellae);
            }
            #[cfg(feature = "nightly")]
            ExecType::Closure(cmd) => {
                exec_closure_cmd(self, cmd, msg, ser_data, lamellae);
            }
            ExecType::Runtime(cmd) => {
                self.exec_runtime_cmd(cmd, msg, lamellae, ser_data);
            }
        };
        // let b = s.elapsed().as_millis() as usize;
        // (*self.timers.get(&cmd).unwrap()).fetch_add(b, Ordering::Relaxed);
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
                if let Ok(_) = ireq.data_tx.send((pe as usize, data)) {}
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
    ) {
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
                    let my_any: LamellarAny = Box::new(0);
                    let msg = Msg {
                        cmd: ExecType::Runtime(Cmd::ExecBatchUnitReturns),
                        src: msg.src as u16, //fake that this is from the original sender
                        req_id: 0,
                        team_id: 0,
                        return_data: false,
                    };
                    self.scheduler.submit_req(
                        self.my_pe,
                        Some(self.my_pe),
                        msg,
                        self.fake_ireq.clone(),
                        my_any,
                        self.fake_arch.clone(),
                        lamellae.backend(),
                    );
                    //no other request has initiated yet
                }
            }
            ExecType::Runtime(Cmd::UnitReturn) => {
                let rmsg = Msg {
                    cmd: cmd,
                    src: self.my_pe as u16,
                    req_id: msg.req_id,
                    team_id: msg.team_id,
                    return_data: false,
                };
                let payload = (rmsg, bincode::serialize(&()).unwrap());
                lamellae.send_to_pe(msg.src as usize, bincode::serialize(&payload).unwrap());
            }
            ExecType::Runtime(Cmd::DataReturn) | ExecType::Am(Cmd::ExecReturn) => {
                let rmsg = Msg {
                    cmd: cmd,
                    src: self.my_pe as u16,
                    req_id: msg.req_id,
                    team_id: msg.team_id,
                    return_data: false,
                };
                let data = data.unwrap();
                let payload = (rmsg, data);
                lamellae.send_to_pe(msg.src as usize, bincode::serialize(&payload).unwrap());
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
                let payload = (rmsg, data);
                lamellae.send_to_pe(msg.src as usize, bincode::serialize(&payload).unwrap());
            }
            _ => println!("send_resp unknown command {:#?}", cmd),
        }
        // let b = s.elapsed().as_millis() as usize;
        // (*self.timers.get(&msg.cmd).unwrap()).fetch_add(b, Ordering::Relaxed);
    }
}

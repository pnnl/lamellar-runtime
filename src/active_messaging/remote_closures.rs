use crate::active_messaging::{
    ActiveMessageEngine, Cmd, ExecType, LamellarAny, Msg, RetType, REQUESTS,
};
use crate::lamellae::LamellaeAM;
use crate::lamellar_request::*;
use crate::lamellar_team::LamellarTeamRT;
use crate::schedulers::ReqData;

use log::trace;
use std::sync::atomic::Ordering;
use std::sync::Arc;
// use std::time::Instant;

pub(crate) type LamellarLocal = Box<dyn FnOnce<(), Output = (RetType, Option<Vec<u8>>)> + Send>;
pub(crate) type LamellarClosure = Box<dyn FnOnce<(), Output = Vec<u8>> + Send>;
/// Special return type used by runtime to detect an automatically executed closure upon return of a request
///
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct ClosureRet {
    pub(crate) data: std::vec::Vec<u8>,
}

/// a remote closure returning a handle to capture any resulting data (used internally)
///
/// # Arguments
///
/// * `func` - a serializable closure to execute automatically when the parent request returns
///
pub(crate) fn lamellar_closure<
    F: std::clone::Clone
        + Send
        + Sync
        + FnOnce() -> T
        + Send
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone
        + 'static,
    T: std::any::Any
        + Send
        + Sync
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone,
>(
    func: F,
) -> LamellarClosure {
    Box::new(move || {
        let arg: Vec<u8> = crate::serialize(&func).unwrap();
        let start: serde_closure::FnOnce<(Vec<u8>,), fn((Vec<u8>,), ()) -> _> = FnOnce!([arg]move||{
            let arg: Vec<u8> = arg;
            let closure: F = crate::deserialize(&arg).unwrap();
            let ret: T=closure();
            let box_any_ret: Box<dyn std::any::Any> = Box::new(ret.clone());
            if let Some(_x) = box_any_ret.downcast_ref::<()>(){
                // println!("unit return");
                (RetType::Unit,None)
            }
            else if let Some(x) = box_any_ret.downcast_ref::<ClosureRet>(){
                // println!("returning a func");
                (RetType::Closure,Some(x.data.clone()))
            }
            else{
                // println!("returning some other type");
                (RetType::Data,Some(crate::serialize(&ret).unwrap()))
            }
        });
        crate::serialize(&start).unwrap()
    })
}

/// a "remote" closure where destination is the local pe (no need to serialize) (used intenally)
///
/// # Arguments
///
/// * `func` - a serializable closure to execute automatically when the parent request returns
///
pub(crate) fn lamellar_local<
    F: std::clone::Clone
        + Send
        + Sync
        + FnOnce() -> T
        + Send
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone
        + 'static,
    T: std::any::Any
        + Send
        + Sync
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone,
>(
    func: F,
) -> LamellarLocal {
    Box::new(move || {
        let ret: T = func();
        let box_any_ret: Box<dyn std::any::Any> = Box::new(ret.clone());
        if let Some(_x) = box_any_ret.downcast_ref::<()>() {
            // println!("unit return");
            (RetType::Unit, None)
        } else if let Some(x) = box_any_ret.downcast_ref::<ClosureRet>() {
            // println!("returning a func");
            (RetType::Closure, Some(x.data.clone()))
        } else {
            // println!("returning some other type");
            (RetType::Data, Some(crate::serialize(&ret).unwrap()))
        }
    })
}
pub trait RemoteClosures {
    fn exec_closure_all<
        F: FnOnce() -> T
            + Send
            + Sync
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + 'static,
        T: std::any::Any
            + Send
            + Sync
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone,
    >(
        &self,
        func: F,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;

    fn exec_closure_pe<
        F: FnOnce() -> T
            + Send
            + Sync
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + 'static,
        T: std::any::Any
            + Send
            + Sync
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone,
    >(
        &self,
        pe: usize,
        func: F,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;

    fn exec_closure_on_return<
        F: FnOnce() -> T + serde::ser::Serialize + serde::de::DeserializeOwned + 'static,
        T: std::any::Any + serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone,
    >(
        &self,
        func: F,
    ) -> ClosureRet;
}

pub(crate) fn exec_closure_cmd(
    ame: &ActiveMessageEngine,
    cmd: Cmd,
    msg: Msg,
    ser_data: Vec<u8>,
    lamellae: Arc<dyn LamellaeAM>,
    _world: Arc<LamellarTeamRT>,
    team: Arc<LamellarTeamRT>,
) -> Option<ReqData> {
    match cmd {
        Cmd::Exec => {
            // let t = std::time::Instant::now();
            let (ret_type, data) = exec_closure(ame, &ser_data);
            // ame.send_response(Cmd::BatchedUnitResp, None, msg, lamellae);

            if msg.return_data {
                match ret_type {
                    RetType::Closure => ame.send_response(
                        ExecType::Closure(Cmd::ExecReturn),
                        data,
                        msg,
                        lamellae,
                        team.my_hash,
                    ),
                    RetType::Data => ame.send_response(
                        ExecType::Runtime(Cmd::DataReturn),
                        data,
                        msg,
                        lamellae,
                        team.my_hash,
                    ),
                    RetType::Unit => ame.send_response(
                        ExecType::Runtime(Cmd::BatchedUnitReturn),
                        None,
                        msg,
                        lamellae,
                        team.my_hash,
                    ),
                    _ => {
                        println!("unexpected return type");
                        None
                    }
                }
            } else {
                // match ret_type {
                //     RetType::Closure => {
                //         ame.send_response(Cmd::ClosureResp, data, msg, lamellae)
                //     }
                //     RetType::Data => ame.send_response(Cmd::NoHandleResp, data, msg, lamellae),
                //     RetType::Unit => ame.send_response(Cmd::NoHandleResp, None, msg, lamellae),
                //     _ => println!("unexpected return type"),
                // }
                None
            }
        }

        Cmd::ExecReturn => {
            exec_closure(ame, &ser_data); //remote closures can't capture result of returning closure
            ame.send_data_to_user_handle(msg.req_id, msg.src, None);
            None
        }
        _ => {
            println!("unexpected cmd in closure request: {:?}", msg.cmd);
            None
        }
    }
}

pub(crate) fn exec_closure(
    ame: &ActiveMessageEngine,
    data: &[u8],
) -> (RetType, Option<std::vec::Vec<u8>>) {
    trace!("[{:?}] exec_closure", ame.my_pe);
    let closure: serde_closure::FnOnce<
        (std::vec::Vec<u8>,),
        fn((std::vec::Vec<u8>,), ()) -> (RetType, Option<std::vec::Vec<u8>>),
    > = crate::deserialize(&data).unwrap();
    closure()
}

pub(crate) fn process_closure_request(
    ame: &ActiveMessageEngine,
    req_data: ReqData,
    _world: Arc<LamellarTeamRT>,
    team: Arc<LamellarTeamRT>,
) -> Option<(Vec<u8>, ReqData)> {
    //,lamellaes: &Arc<BTreeMap<Backend, Arc<dyn LamellaeAM>>>,){
    let func = req_data.func;
    let my_pe = if let Ok(my_pe) = team.arch.team_pe(req_data.src) {
        Some(my_pe)
    } else {
        None
    };

    match func.downcast::<LamellarLocal>() {
        Ok(func) => {
            exec_local(ame, req_data.msg, func, req_data.ireq);
            None
        }
        Err(func) => {
            match func.downcast::<LamellarClosure>() {
                Ok(func) => {
                    let data = func();
                    let payload = (req_data.msg, data, req_data.team_hash);
                    let data = crate::serialize(&payload).unwrap();
                    //lamellaes[&req_data.backend].send_to_pes(
                    req_data
                        .lamellae
                        .send_to_pes(req_data.pe, team.arch.clone(), data);
                    None
                }
                Err(func) => {
                    match func.downcast::<(LamellarLocal, LamellarClosure)>() {
                        Ok(func) => {
                            let data = func.1();
                            let payload = (req_data.msg.clone(), data, req_data.team_hash);
                            let data = crate::serialize(&payload).unwrap();
                            // lamellaes[&req_data.backend].

                            if req_data.pe == None && my_pe != None {
                                exec_local(ame, req_data.msg, func.0, req_data.ireq.clone());
                            }
                            if data.len() > 10_000 {
                                req_data
                                    .lamellae
                                    .send_to_pes(req_data.pe, team.arch.clone(), data);
                                None
                            } else {
                                let id = if let Some(pe) = req_data.pe {
                                    team.arch.global_pe(pe).expect("invalid pe") as u64
                                // aggregate to same pe across team boundaries
                                } else {
                                    req_data.team_hash //use this to represent we want to send to all pes in the team
                                };

                                let my_any: LamellarAny = Box::new(0);
                                let msg = Msg {
                                    cmd: ExecType::Runtime(Cmd::ExecBatchMsgSend),
                                    src: req_data.src as u16, //fake that this is from the original sender
                                    req_id: 0,
                                    team_id: id as usize, // we use this as the lookup id int the hashmaps
                                    return_data: false,
                                };
                                let new_req = ReqData {
                                    // team_arch: req_data,
                                    src: req_data.src,
                                    pe: req_data.pe,
                                    msg: msg,
                                    ireq: req_data.ireq.clone(),
                                    func: my_any,
                                    // backend: lamellae.backend(),
                                    lamellae: req_data.lamellae.clone(),
                                    team_hash: req_data.team_hash, // fake hash,
                                };
                                Some((data, new_req))
                            }
                        }
                        Err(_) => panic!("error! unknown closure type"),
                    }
                }
            }
        }
    }

    // if let Some(pe) = req_data.pe {
    //     if pe == my_pe || req_data.team.num_pes() == 1 {
    //         trace!("[{:?}] local closure request ", my_pe);
    //     match func.downcast::<LamellarLocal>() {
    //         Ok(func) => {
    //             exec_local(ame,req_data.msg, func, req_data.ireq);
    //         }
    //         Err(func) => {
    //             let func = func
    //                 .downcast::<(LamellarLocal, LamellarClosure)>()
    //                 .expect("LAMELLAR RUNTIME ERROR: error in local closure downcast");
    //             exec_local(ame,req_data.msg, func.0, req_data.ireq);
    //         }
    //     }

    //     } else {
    //         // remote request
    //         trace!("[{:?}] remote closure request ", my_pe);

    //         let func = func
    //             .downcast::<LamellarClosure>()
    //             .expect("LAMELLAR RUNTIME ERROR: error in remote closure downcast");
    //         // let it = Instant::now();
    //         let data = func();
    //         // (timers.get("serde_1").unwrap())
    //         //     .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
    //         let payload = (req_data.msg, data);

    //         // let it = Instant::now();
    //         let data = crate::serialize(&payload).unwrap();
    //         // (timers.get("serde_2").unwrap())
    //         //     .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
    //         // (timers.get("network_cnt").unwrap()).fetch_add(1, Ordering::Relaxed);
    //         // (timers.get("network_size").unwrap()).fetch_add(data.len(), Ordering::Relaxed);
    //         // let it = Instant::now();
    //         lamellaes[&req_data.backend].send_to_pes(Some(pe), req_data.team.clone(), data);
    //         // (timers.get("network").unwrap())
    //         //     .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
    //     }
    // } else {
    //     //all pe request
    //     trace!("[{:?}] all closure request exec", my_pe);

    //     // let it = Instant::now();
    //     let funcs = func
    //         .downcast::<(LamellarLocal, LamellarClosure)>()
    //         .expect("LAMELLAR RUNTIME ERROR: error in all am downcast");
    //     let data = funcs.1();
    //     // (timers.get("serde_1").unwrap())
    //     //     .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
    //     // let it = Instant::now();
    //     let payload = (req_data.msg.clone(), data);
    //     let data = crate::serialize(&payload).unwrap();
    //     // (timers.get("serde_2").unwrap())
    //     //     .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
    //     // (timers.get("network_cnt").unwrap()).fetch_add(1, Ordering::Relaxed);
    //     // (timers.get("network_size").unwrap()).fetch_add(data.len(), Ordering::Relaxed);
    //     // let it = Instant::now();
    //     lamellaes[&req_data.backend].send_to_pes(None, req_data.team.clone(), data);
    //     // (timers.get("network").unwrap())
    //     //     .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
    //     // let it = Instant::now();
    //     exec_local(ame,req_data.msg, funcs.0, req_data.ireq);
    //     // (timers.get("local_closure").unwrap())
    //     //     .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);

    // }
}

fn exec_local(ame: &ActiveMessageEngine, msg: Msg, func: LamellarLocal, ireq: InternalReq) {
    // let s = Instant::now();
    trace!("[{:?}] exec_local: {:?}", ame.my_pe, msg);
    if let ExecType::Closure(cmd) = msg.cmd.clone() {
        match cmd {
            Cmd::Exec => {
                let (ret_type, data) = func();
                match ret_type {
                    RetType::Unit => {
                        if msg.return_data {
                            if let Ok(_) = ireq
                                .data_tx
                                .send((msg.src as usize, Some(crate::serialize(&()).unwrap())))
                            {
                            }
                            let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
                            if cnt == 1 {
                                REQUESTS[msg.req_id % REQUESTS.len()].remove(&msg.req_id);
                            }
                        }
                    }
                    RetType::Closure => {
                        exec_closure(ame, &data.expect("error executing return closure"));
                        ame.send_data_to_user_handle(msg.req_id, msg.src, None);
                    }
                    RetType::Data => {
                        if msg.return_data {
                            if let Ok(_) = ireq.data_tx.send((msg.src as usize, data)) {}
                            let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
                            if cnt == 1 {
                                REQUESTS[msg.req_id % REQUESTS.len()].remove(&msg.req_id);
                            }
                        }
                    }
                    _ => println!("unexpected return type"),
                };
                ireq.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                ireq.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
            }
            Cmd::ExecReturn => {
                func();
                ame.send_data_to_user_handle(msg.req_id, msg.src, None);
                ireq.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                ireq.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
            }
            _ => println!(
                "[LAMELLAR WARNING]local  am unknown(or unhandled) command {:?}",
                msg.cmd
            ),
        }
    } else {
        println!(
            "[LAMELLAR WARNING] shouldn't have an exectype of {:?} in remote closure",
            msg.cmd
        )
    }

    // let b = s.elapsed().as_millis() as usize;
    // (*ame.timers.get(&msg.cmd).unwrap()).fetch_add(b, Ordering::Relaxed);
}

use crate::lamellae::{Backend,LamellaeAM};
use crate::active_messaging::{ActiveMessageEngine,Cmd,RetType,Msg,REQUESTS,ExecType};
use crate::lamellar_request::*;
use crate::schedulers::ReqData;

use log::trace;
use std::collections::BTreeMap;
use std::sync::atomic::{Ordering};
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
pub (crate) fn lamellar_closure<
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
        let arg: Vec<u8> = bincode::serialize(&func).unwrap();
        let start: serde_closure::FnOnce<(Vec<u8>,), fn((Vec<u8>,), ()) -> _> = FnOnce!([arg]move||{
            let arg: Vec<u8> = arg;
            let closure: F = bincode::deserialize(&arg).unwrap();
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
                (RetType::Data,Some(bincode::serialize(&ret).unwrap()))
            }
        });
        bincode::serialize(&start).unwrap()
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
            (RetType::Data, Some(bincode::serialize(&ret).unwrap()))
        }
    })
}
pub trait RemoteClosures{
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
    ) -> LamellarRequest<T>;

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
    ) -> LamellarRequest<T>;

    fn exec_closure_on_return<
        F: FnOnce() -> T + serde::ser::Serialize + serde::de::DeserializeOwned + 'static,
        T: std::any::Any + serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone,
    >(
        &self,
        func: F,
    ) -> ClosureRet;
}

pub(crate) fn exec_closure_cmd(ame: &ActiveMessageEngine,cmd: Cmd, msg: Msg, ser_data: Vec<u8>, lamellae: Arc<dyn LamellaeAM>) {
    match cmd {
        Cmd::Exec => {
            // let t = std::time::Instant::now();
            let (ret_type, data) = exec_closure(ame,&ser_data);
            // ame.send_response(Cmd::BatchedUnitResp, None, msg, lamellae);

            if msg.return_data {
                match ret_type {
                    RetType::Closure => {
                        ame.send_response(ExecType::Closure(Cmd::ExecReturn), data, msg, lamellae)
                    }
                    RetType::Data => ame.send_response(ExecType::Runtime(Cmd::DataReturn), data, msg, lamellae),
                    RetType::Unit => {
                        ame.send_response(ExecType::Runtime(Cmd::BatchedUnitReturn), None, msg, lamellae)
                    }
                    _ => println!("unexpected return type"),
                };
            } else {
                // match ret_type {
                //     RetType::Closure => {
                //         ame.send_response(Cmd::ClosureResp, data, msg, lamellae)
                //     }
                //     RetType::Data => ame.send_response(Cmd::NoHandleResp, data, msg, lamellae),
                //     RetType::Unit => ame.send_response(Cmd::NoHandleResp, None, msg, lamellae),
                //     _ => println!("unexpected return type"),
                // }
            }
        }
    
        Cmd::ExecReturn => {
            exec_closure(ame,&ser_data); //remote closures can't capture result of returning closure
            ame.send_data_to_user_handle(msg.req_id, msg.src, None);
        }
        _ => {
            println!("unexpected cmd in closure request: {:?}",msg.cmd);
        }
    }
}

pub(crate) fn exec_closure(ame: &ActiveMessageEngine, data: &[u8]) -> (RetType, Option<std::vec::Vec<u8>>) {
    trace!("[{:?}] exec_closure", ame.my_pe);
    let closure: serde_closure::FnOnce<
        (std::vec::Vec<u8>,),
        fn((std::vec::Vec<u8>,), ()) -> (RetType, Option<std::vec::Vec<u8>>),
    > = bincode::deserialize(&data).unwrap();
    closure()
}


pub (crate) fn process_closure_request(ame: &ActiveMessageEngine, req_data: ReqData,lamellaes: &Arc<BTreeMap<Backend, Arc<dyn LamellaeAM>>>,){
    let func = req_data.func;
    let my_pe = req_data.team.my_pe();
    if let Some(pe) = req_data.pe {
        if pe == my_pe || req_data.team.num_pes() == 1 {
            trace!("[{:?}] local closure request ", my_pe);
            match func.downcast::<LamellarLocal>() {          
                Ok(func) => {
                    // let it = Instant::now();
                    exec_local(ame,req_data.msg, func, req_data.ireq);
                    // (timers.get("local_closure").unwrap())
                    //     .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
                }
                Err(func) => {
                    // let it = Instant::now();
                    let func = func
                        .downcast::<(LamellarLocal, LamellarClosure)>()
                        .expect("LAMELLAR RUNTIME ERROR: error in local closure downcast");
                    exec_local(ame,req_data.msg, func.0, req_data.ireq);
                    // (timers.get("local_closure").unwrap())
                    //     .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
                }
            }
        } else {
            // remote request
            trace!("[{:?}] remote closure request ", my_pe);
            
            let func = func
                .downcast::<LamellarClosure>()
                .expect("LAMELLAR RUNTIME ERROR: error in remote closure downcast");
            // let it = Instant::now();
            let data = func();
            // (timers.get("serde_1").unwrap())
            //     .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
            let payload = (req_data.msg, data);
                
            
            // let it = Instant::now();
            let data = bincode::serialize(&payload).unwrap();
            // (timers.get("serde_2").unwrap())
            //     .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
            // (timers.get("network_cnt").unwrap()).fetch_add(1, Ordering::Relaxed);
            // (timers.get("network_size").unwrap()).fetch_add(data.len(), Ordering::Relaxed);
            // let it = Instant::now();
            lamellaes[&req_data.backend].send_to_pes(Some(pe), req_data.team.clone(), data);
            // (timers.get("network").unwrap())
            //     .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
        }
    } else {
        //all pe request
        trace!("[{:?}] all closure request exec", my_pe);
        
        // let it = Instant::now();
        let funcs = func
            .downcast::<(LamellarLocal, LamellarClosure)>()
            .expect("LAMELLAR RUNTIME ERROR: error in all am downcast");
        let data = funcs.1();
        // (timers.get("serde_1").unwrap())
        //     .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
        // let it = Instant::now();
        let payload = (req_data.msg.clone(), data);
        let data = bincode::serialize(&payload).unwrap();
        // (timers.get("serde_2").unwrap())
        //     .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
        // (timers.get("network_cnt").unwrap()).fetch_add(1, Ordering::Relaxed);
        // (timers.get("network_size").unwrap()).fetch_add(data.len(), Ordering::Relaxed);
        // let it = Instant::now();
        lamellaes[&req_data.backend].send_to_pes(None, req_data.team.clone(), data);
        // (timers.get("network").unwrap())
        //     .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
        // let it = Instant::now();
        exec_local(ame,req_data.msg, funcs.0, req_data.ireq);
        // (timers.get("local_closure").unwrap())
        //     .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
            
    }

}




fn exec_local(ame: &ActiveMessageEngine , msg: Msg, func: LamellarLocal, ireq: InternalReq) {
    // let s = Instant::now();
    trace!("[{:?}] exec_local: {:?}", ame.my_pe, msg);
    if let ExecType::Closure(cmd) = msg.cmd.clone(){
        match cmd {
            Cmd::Exec => {
                let (ret_type, data) = func();
                match ret_type {
                    RetType::Unit => {
                        if msg.return_data {
                            if let Ok(_) = ireq
                                .data_tx
                                .send((msg.src as usize, Some(bincode::serialize(&()).unwrap())))
                            {
                            }
                            let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
                            if cnt == 1 {
                                REQUESTS[msg.req_id % REQUESTS.len()].remove(&msg.req_id);
                            }
                        }
                    }
                    RetType::Closure => {
                        exec_closure(ame,&data.expect("error executing return closure"));
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
            _ => println!("[LAMELLAR WARNING]local  am unknown(or unhandled) command {:?}", msg.cmd),
        }
    }
    else{
        println!("[LAMELLAR WARNING] shouldn't have an exectype of {:?} in remote closure", msg.cmd)
    }
    
    // let b = s.elapsed().as_millis() as usize;
    // (*ame.timers.get(&msg.cmd).unwrap()).fetch_add(b, Ordering::Relaxed);
}
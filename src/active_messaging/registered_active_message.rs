use crate::active_messaging::{
    ActiveMessageEngine, Cmd, ExecType, LamellarActiveMessage, LamellarBoxedAm, LamellarReturn,
    Msg, RetType, REQUESTS,
};
use crate::lamellae::{Backend, LamellaeAM};
use crate::lamellar_request::*;
use crate::schedulers::ReqData;

use log::trace;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

lazy_static! {
    pub(crate) static ref AMS_EXECS: BTreeMap<String, fn(Vec<u8>, isize, isize) -> Option<LamellarReturn>> = {
        let mut temp = BTreeMap::new();
        for exec in crate::inventory::iter::<RegisteredAm> {
            trace!("{:#?}", exec);
            temp.insert(exec.name.clone(), exec.exec);
        }
        temp
    };
}

#[derive(Debug)]
pub struct RegisteredAm {
    pub exec: fn(Vec<u8>, isize, isize) -> Option<LamellarReturn>,
    pub name: String,
}
crate::inventory::collect!(RegisteredAm);

pub(crate) fn exec_am_cmd(
    ame: &ActiveMessageEngine,
    cmd: Cmd,
    msg: Msg,
    ser_data: Vec<u8>,
    lamellae: Arc<dyn LamellaeAM>,
) {
    match cmd {
        Cmd::Exec => {
            // let t = std::time::Instant::now();
            let (ret_type, data) = exec_am(ame, &ser_data);
            // ame.send_response(Cmd::BatchedUnitResp, None, msg, lamellae)
            // let (ret_type, data) = ame.exec_am(&ser_data);
            if msg.return_data {
                match ret_type {
                    RetType::Am => {
                        ame.send_response(ExecType::Am(Cmd::ExecReturn), data, msg, lamellae)
                    }
                    RetType::Data => {
                        ame.send_response(ExecType::Runtime(Cmd::DataReturn), data, msg, lamellae)
                    }
                    RetType::Unit => ame.send_response(
                        ExecType::Runtime(Cmd::BatchedUnitReturn),
                        None,
                        msg,
                        lamellae,
                    ),
                    _ => println!("unexpected return type"),
                };
            }
            // else {
            //     match ret_type {
            //         RetType::Closure => {
            //             ame.send_response(Cmd::ClosureResp, data, msg, lamellae)
            //         }
            //         RetType::Data => ame.send_response(Cmd::NoHandleResp, data, msg, lamellae),
            //         RetType::Unit => ame.send_response(Cmd::NoHandleResp, None, msg, lamellae),
            //         _ => println!("unexpected return type"),
            //     }
            // }
        }
        Cmd::ExecReturn => {
            let (ret_type, data) = exec_am(ame, &ser_data);
            match ret_type {
                RetType::Am => {
                    println!("returning another AM from a returned AM not yet implemented")
                } //ame.send_response(Cmd::AmResp, data, msg, lamellae),
                RetType::Data => ame.send_data_to_user_handle(msg.req_id, msg.src, data), //ame.send_response(Cmd::DataResp, data, msg, lamellae),
                RetType::Unit => ame.send_data_to_user_handle(msg.req_id, msg.src, None), //ame.send_response(Cmd::UnitResp, None, msg, lamellae),
                _ => println!("unexpected return type"),
            };
        }
        _ => println!(
            "[LAMELLAR WARNING] am unknown(or unhandled) command {:?}",
            msg.cmd
        ),
    }
}

pub(crate) fn process_am_request(
    ame: &ActiveMessageEngine,
    req_data: ReqData,
    lamellaes: &Arc<BTreeMap<Backend, Arc<dyn LamellaeAM>>>,
) {
    let func = req_data
        .func
        .downcast::<LamellarBoxedAm>()
        .expect("LAMELLAR RUNTIME ERROR: error in remote am downcast");
    let my_pe = if let Ok(my_pe) = req_data.team.team_pe_id(&req_data.src){
        Some(my_pe)
    }
    else{
        None
    };

    if req_data.pe == my_pe && my_pe != None{
        trace!("[{:?}] single local request ", my_pe);
        exec_local(ame, req_data.msg, *func, req_data.ireq)
    } else {
        trace!("[{:?}] remote request ", my_pe);
        let id = func.get_id();
        let ser_func = func.ser();
        let ser_func = bincode::serialize(&(&id, ser_func)).unwrap();
        let payload = (req_data.msg, ser_func);
        let data = bincode::serialize(&payload).unwrap();
        lamellaes[&req_data.backend].send_to_pes(req_data.pe, req_data.team.clone(), data);
        if req_data.pe == None && my_pe != None {
            exec_local(ame, req_data.msg, *func, req_data.ireq);
        }
    }
}

fn exec_am(ame: &ActiveMessageEngine, data: &[u8]) -> (RetType, Option<std::vec::Vec<u8>>) {
    trace!("[{:?}] exec_am {:?}", ame.my_pe, data.len());
    let (name, ser_data): (String, Vec<u8>) = bincode::deserialize(&data).unwrap();

    if let Some(res) = AMS_EXECS.get(&name).unwrap()(ser_data, ame.my_pe as isize, ame.num_pes as isize) {
        match res {
            LamellarReturn::LocalData(_res) => {
                panic!("Should not be returning local data from remote  am");
            }
            LamellarReturn::LocalAm(_res) => {
                panic!("Should not be returning local am from remote  am");
            }
            LamellarReturn::RemoteData(res) => (RetType::Data, Some(res)),
            LamellarReturn::RemoteAm(am) => {
                let id = am.get_id();
                let data = am.ser();
                let data = bincode::serialize(&(&id, data)).unwrap();
                (RetType::Am, Some(data))
            }
        }
    } else {
        (RetType::Unit, None)
    }
}

pub(crate) fn exec_local(
    ame: &ActiveMessageEngine,
    msg: Msg,
    func: LamellarBoxedAm,
    ireq: InternalReq,
) {
    trace!("[{:?}] exec_local_am: {:?}", ame.my_pe, msg);
    if let ExecType::Am(cmd) = msg.cmd.clone() {
        match cmd {
            Cmd::Exec => {
                if let Some(result) = func.exec(ame.my_pe as isize, ame.num_pes as isize, true) {
                    match result {
                        LamellarReturn::LocalData(data) => {
                            trace!("local am data return");
                            if msg.return_data {
                                if let Ok(_) = ireq.data_tx.send((msg.src as usize, Some(data))) {}
                                let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
                                if cnt == 1 {
                                    REQUESTS[msg.req_id % REQUESTS.len()].remove(&msg.req_id);
                                }
                            }
                            ireq.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                            ireq.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                        }
                        LamellarReturn::LocalAm(am) => {
                            trace!("local am am return");
                            exec_return_am(ame, msg, am, ireq);
                        }
                        LamellarReturn::RemoteData(_data) => {
                            trace!("remote am data return");
                            panic!("should not be returning remote data from local am");
                        }
                        LamellarReturn::RemoteAm(_am) => {
                            trace!("remote am am return");
                            panic!("should not be returning remote am from local am");
                        }
                    }
                } else {
                    // nothing returned
                    if msg.return_data {
                        if let Ok(_) = ireq.data_tx.send((msg.src as usize, None)) {}
                        let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
                        if cnt == 1 {
                            REQUESTS[msg.req_id % REQUESTS.len()].remove(&msg.req_id);
                        }
                    }
                    ireq.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                    ireq.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                }
            }
            _ => println!(
                "[LAMELLAR WARNING]local  am unknown(or unhandled) command {:?}",
                msg.cmd
            ),
        }
    } else {
        println!(
            "[LAMELLAR WARNING] shouldn't have an exectype of {:?} in registered active message",
            msg.cmd
        )
    }
    // let b = s.elapsed().as_millis() as usize;
    // (*ame.timers.get(&cmd).unwrap()).fetch_add(b, Ordering::Relaxed);
}

pub(crate) fn exec_return_am(
    ame: &ActiveMessageEngine,
    msg: Msg,
    func: Box<dyn LamellarActiveMessage>,
    ireq: InternalReq,
) {
    // let s = Instant::now();
    trace!("[{:?}] exec_return_am: {:?}", ame.my_pe, msg);
    if let ExecType::Am(cmd) = msg.cmd.clone() {
        match cmd {
            Cmd::Exec => {
                if let Some(result) = func.exec(ame.my_pe as isize, ame.num_pes as isize, true) {
                    match result {
                        LamellarReturn::LocalData(data) => {
                            trace!("return am local am data");
                            if msg.return_data {
                                if let Ok(_) = ireq.data_tx.send((msg.src as usize, Some(data))) {}
                                let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
                                if cnt == 1 {
                                    REQUESTS[msg.req_id % REQUESTS.len()].remove(&msg.req_id);
                                }
                            }
                            ireq.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                            ireq.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                        }
                        LamellarReturn::LocalAm(_am) => {
                            trace!("return am local am return");
                            println!("returning an am from a returned am not currently valid");
                        }
                        LamellarReturn::RemoteData(data) => {
                            trace!("return am remote am data");
                            if msg.return_data {
                                if let Ok(_) = ireq.data_tx.send((
                                    msg.src as usize,
                                    Some(bincode::serialize(&data).unwrap()),
                                )) {}
                                let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
                                if cnt == 1 {
                                    REQUESTS[msg.req_id % REQUESTS.len()].remove(&msg.req_id);
                                }
                            }
                            ireq.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                            ireq.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                        }
                        LamellarReturn::RemoteAm(_am) => {
                            trace!("return am remote am return");
                            println!("returning an am from a returned am not currently valid");
                        }
                    }
                } else {
                    // nothing returned
                    // println!("am unit return");
                    if msg.return_data {
                        if let Ok(_) = ireq.data_tx.send((msg.src as usize, None)) {}
                        let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
                        if cnt == 1 {
                            REQUESTS[msg.req_id % REQUESTS.len()].remove(&msg.req_id);
                        }
                    }
                    ireq.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                    ireq.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                }
            }
            _ => println!("[LAMELLAR WARNING] local  am unknown command {:?}", msg.cmd),
        }
    } else {
        println!(
            "[LAMELLAR WARNING] shouldn't have an exectype of {:?} in registered active message",
            msg.cmd
        )
    }
    // let b = s.elapsed().as_millis() as usize;
    // (*ame.timers.get(&cmd).unwrap()).fetch_add(b, Ordering::Relaxed);
}

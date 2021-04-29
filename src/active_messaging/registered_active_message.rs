use crate::active_messaging::{
    ActiveMessageEngine, Cmd, ExecType, LamellarAny, LamellarBoxedAm, LamellarReturn, Msg, RetType,
    REQUESTS,
};
use crate::lamellae::LamellaeAM;
use crate::lamellar_request::*;
use crate::lamellar_team::LamellarTeamRT;
use crate::schedulers::ReqData;
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use log::trace;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub(crate) type ExecFn =
    fn(
        Vec<u8>,
        usize,
        usize,
        Arc<LamellarTeamRT>,
        Arc<LamellarTeamRT>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<LamellarReturn>> + Send>>;

lazy_static! {
    pub(crate) static ref AMS_EXECS: BTreeMap<String, ExecFn> = {
        let mut temp = BTreeMap::new();
        for exec in crate::inventory::iter::<RegisteredAm> {
            trace!("{:#?}", exec.name);
            temp.insert(exec.name.clone(), exec.exec);
        }
        temp
    };
}

// #[derive(Debug)]
pub struct RegisteredAm {
    pub exec: ExecFn,
    pub name: String,
}
crate::inventory::collect!(RegisteredAm);

//#[prof]
pub(crate) async fn exec_am_cmd(
    ame: &ActiveMessageEngine,
    cmd: Cmd,
    msg: Msg,
    ser_data: Vec<u8>,
    lamellae: Arc<dyn LamellaeAM>,
    world: Arc<LamellarTeamRT>,
    team: Arc<LamellarTeamRT>,
) -> Option<ReqData> {
    match cmd {
        Cmd::Exec => {
            // let t = std::time::Instant::now();
            let (ret_type, data) = exec_am(ame, &ser_data, world, team.clone()).await;
            if msg.return_data {
                match ret_type {
                    RetType::Am => ame.send_response(
                        ExecType::Am(Cmd::ExecReturn),
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
                None
            }
        }
        Cmd::ExecReturn => {
            let (ret_type, data) = exec_am(ame, &ser_data, world, team).await;
            match ret_type {
                RetType::Am => {
                    println!("returning another AM from a returned AM not yet implemented")
                }
                RetType::Data => ame.send_data_to_user_handle(msg.req_id, msg.src, data),
                RetType::Unit => ame.send_data_to_user_handle(msg.req_id, msg.src, None),
                _ => println!("unexpected return type"),
            };
            None
        }
        _ => {
            println!(
                "[LAMELLAR WARNING] am unknown(or unhandled) command {:?}",
                msg.cmd
            );
            None
        }
    }
}

//#[prof]
pub(crate) async fn process_am_request(
    ame: &ActiveMessageEngine,
    req_data: ReqData,
    world: Arc<LamellarTeamRT>,
    team: Arc<LamellarTeamRT>,
) -> Option<(Vec<u8>, ReqData)> {
    let func = req_data
        .func
        .downcast::<LamellarBoxedAm>()
        .expect("LAMELLAR RUNTIME ERROR: error in remote am downcast");
    let my_pe = if let Ok(my_pe) = team.arch.team_pe(req_data.src) {
        Some(my_pe)
    } else {
        None
    };

    if req_data.pe == my_pe && my_pe != None {
        trace!("[{:?}] single local request ", my_pe);
        exec_local(ame, req_data.msg, *func, req_data.ireq.clone(), world, team).await;
        None
    } else {
        // ame.msgs.fetch_add(1,Ordering::SeqCst);
        trace!("[{:?}] remote request ", my_pe);
        let id = func.get_id();
    
        let ser_func = if req_data.pe.is_none() {
            func.ser(team.num_pes())
        }
        else{
            func.ser(1)
        };

        let ser_func = crate::serialize(&(&id, ser_func)).unwrap();
        let payload = (req_data.msg, ser_func, req_data.team_hash);
        let data = crate::serialize(&payload).unwrap();

        if data.len() > 0 {
            //1000 { // need to see if there is a better way to figure out when to do aggregation
            req_data
                .lamellae
                .send_to_pes(req_data.pe, team.arch.clone(), data);
            if req_data.pe == None && my_pe != None {
                exec_local(
                    ame,
                    req_data.msg,
                    *func,
                    req_data.ireq.clone(),
                    world,
                    team.clone(),
                )
                .await;
            }
            None
        } else {
            if req_data.pe == None && my_pe != None {
                exec_local(
                    ame,
                    req_data.msg,
                    *func,
                    req_data.ireq.clone(),
                    world,
                    team.clone(),
                )
                .await;
            }
            let id = if let Some(pe) = req_data.pe {
                team.arch.world_pe(pe).expect("invalid pe") as u64 // aggregate to same pe across team boundaries
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
                src: req_data.src,
                pe: req_data.pe,
                msg: msg,
                ireq: req_data.ireq.clone(),
                func: my_any,
                lamellae: req_data.lamellae.clone(),
                team_hash: req_data.team_hash, // fake hash,
            };
            Some((data, new_req))
        }
    }
}

//#[prof]
async fn exec_am(
    ame: &ActiveMessageEngine,
    data: &[u8],
    world: Arc<LamellarTeamRT>,
    team: Arc<LamellarTeamRT>,
) -> (RetType, Option<std::vec::Vec<u8>>) {
    trace!("[{:?}] exec_am {:?}", ame.my_pe, data.len());
    let (name, ser_data): (String, Vec<u8>) = crate::deserialize(&data).unwrap();
    // ame.cmds.fetch_add(1,Ordering::SeqCst);
    if let Some(res) =
        AMS_EXECS.get(&name).unwrap()(ser_data, ame.my_pe, ame.num_pes, world.clone(), team).await
    {
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
                let data = am.ser(1);
                let data = crate::serialize(&(&id, data)).unwrap();
                (RetType::Am, Some(data))
            }
        }
    } else {
        (RetType::Unit, None)
    }
}

//#[prof]
pub(crate) async fn exec_local(
    ame: &ActiveMessageEngine,
    msg: Msg,
    func: LamellarBoxedAm,
    ireq: InternalReq,
    world: Arc<LamellarTeamRT>,
    team: Arc<LamellarTeamRT>,
) {
    trace!("[{:?}] exec_local_am: {:?}", ame.my_pe, msg);
    if let ExecType::Am(cmd) = msg.cmd.clone() {
        match cmd {
            Cmd::Exec => {
                let res = func
                    .exec(ame.my_pe, ame.num_pes, true, world.clone(), team.clone())
                    .await;
                if let Some(result) = res {
                    match result {
                        LamellarReturn::LocalData(data) => {
                            trace!("local am data return");
                            if msg.return_data {
                                if let Ok(_) = ireq.data_tx.send((msg.src as usize, Some(data))) {} //if this returns an error it means the user has dropped the handle
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
                            exec_return_am(ame, msg, am, ireq, world, team).await;
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
                        if let Ok(_) = ireq.data_tx.send((msg.src as usize, None)) {} //if this returns an error it means the user has dropped the handle
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
                "[LAMELLAR WARNING] local am unknown(or unhandled) command {:?}",
                msg.cmd
            ),
        }
    } else {
        println!(
            "[LAMELLAR WARNING] shouldn't have an exectype of {:?} in registered active message",
            msg.cmd
        )
    }
}

//#[prof]
pub(crate) async fn exec_return_am(
    ame: &ActiveMessageEngine,
    msg: Msg,
    func: LamellarBoxedAm,
    ireq: InternalReq,
    world: Arc<LamellarTeamRT>,
    team: Arc<LamellarTeamRT>,
) {
    // let s = Instant::now();
    trace!("[{:?}] exec_return_am: {:?}", ame.my_pe, msg);
    if let ExecType::Am(cmd) = msg.cmd.clone() {
        match cmd {
            Cmd::Exec => {
                if let Some(result) = func.exec(ame.my_pe, ame.num_pes, true, world, team).await {
                    match result {
                        LamellarReturn::LocalData(data) => {
                            trace!("return am local am data");
                            if msg.return_data {
                                ireq.data_tx
                                    .send((msg.src as usize, Some(data)))
                                    .expect("error returning local data");
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
                                ireq.data_tx
                                    .send((
                                        msg.src as usize,
                                        Some(crate::serialize(&data).unwrap()),
                                    ))
                                    .expect("error returning remote data");
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
                    if msg.return_data {
                        ireq.data_tx
                            .send((msg.src as usize, None))
                            .expect("error returning none data");
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
}

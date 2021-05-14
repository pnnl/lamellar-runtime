use crate::active_messaging::*;
//{
    // ActiveMessageEngine, Cmd, ExecType, LamellarBoxedAm, LamellarReturn, Msg, RetType,
    // REQUESTS,LamellarBoxedData,
// };
use crate::lamellae::{Lamellae,LamellaeAM,SerializeHeader,SerializedData,Ser,Des};
use crate::lamellar_request::*;
use crate::lamellar_team::LamellarTeamRT;
use crate::scheduler::ReqData;
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use log::trace;
use std::sync::atomic::Ordering;
use std::sync::Arc;

// struct 

pub (crate) type UnpackFn = fn(&[u8]) -> LamellarBoxedAm;
// pub(crate) type ExecFn =
//     fn(
//         &[u8],
//         usize,
//         usize,
//         Arc<LamellarTeamRT>,
//         Arc<LamellarTeamRT>,
//         bool,
//     ) -> std::pin::Pin<Box<dyn std::future::Future<Output = LamellarReturn> + Send>>;

lazy_static! {
    pub(crate) static ref AMS_IDS: HashMap<String, usize> = {
        
        let mut ams = vec![];
        for am in crate::inventory::iter::<RegisteredAm>{
            ams.push(am.name.clone());
        }
        ams.sort();
        let mut cnt = 0;
        let mut temp = HashMap::new();
        for am in ams{
            temp.insert(am.clone(),cnt);
            cnt+=1;
        }
        temp
    };
}
lazy_static!{
    pub(crate) static ref AMS_EXECS: HashMap<usize, UnpackFn> = {
        let mut temp = HashMap::new();
        for exec in crate::inventory::iter::<RegisteredAm> {
            // trace!("{:#?}", exec.name);
            let id = AMS_IDS.get(&exec.name).unwrap();
            temp.insert(*id, exec.exec);
        }
        temp
    };
}

// #[derive(Debug)]
pub struct RegisteredAm {
    pub exec: UnpackFn,
    pub name: String,
}
crate::inventory::collect!(RegisteredAm);

//#[prof]
pub(crate) async fn exec_am_cmd(
    ame: &ActiveMessageEngine,
    cmd: Cmd,
    msg: Msg,
    ser_data: SerializedData,
    lamellae: Arc<Lamellae>,
    world: Arc<LamellarTeamRT>,
    team: Arc<LamellarTeamRT>,
) -> Option<ReqData> {
    match cmd {
        Cmd::Exec => {
            let lam_return = exec_am(ame, &ser_data, world, team.clone(),false).await;
            if msg.return_data {
                let cmd = match &lam_return {
                    LamellarReturn::LocalData(_) | LamellarReturn::LocalAm(_) =>{
                        panic!("Should not be returning local data from remote  am");
                    }
                    LamellarReturn::RemoteAm(ref _d) => ExecType::Am(Cmd::ExecReturn),
                    LamellarReturn::RemoteData(ref _d,ref _f) => ExecType::Runtime(Cmd::DataReturn),
                    LamellarReturn::Unit => ExecType::Runtime(Cmd::UnitReturn),//need to fix batched unit returns
                };
                ame.send_response(cmd, lam_return, msg, lamellae,team.my_hash).await
            } else {
                None
            }
        }
        Cmd::ExecReturn => {
            match exec_am(ame, &ser_data, world, team,true).await {
                LamellarReturn::LocalData(data) => ame.send_data_to_user_handle(msg.req_id, msg.src, InternalResult::Local(data)),
                LamellarReturn::LocalAm(_) => panic!("Should not be returning local data from remote  am"),
                LamellarReturn::RemoteAm(_) => panic!("returning another AM from a returned AM not yet implemented"),
                LamellarReturn::RemoteData(_,_) => panic!("Should not be returning remote data from a returned am"),
                LamellarReturn::Unit => ame.send_data_to_user_handle(msg.req_id, msg.src, InternalResult::Unit)
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
        let id = AMS_IDS.get(&func.get_id()).unwrap();
    
        if req_data.pe.is_none() {
            func.ser(team.num_pes()-1);
        }
        else{
            func.ser(1);
        };
        let header = Some(SerializeHeader{msg: req_data.msg, team_hash: req_data.team_hash, id: Some(*id)});
        let serialize_size = func.serialized_size();
        // println!("func {:?} {:?}",func,serialize_size);
        let data = req_data.lamellae.serialize_header(header,serialize_size).await.unwrap();
        func.serialize_into(data.data_as_bytes());
        // println!("data {:?}",data.data_as_bytes());

        

        // if data.len > 0 {
            //1000 { // need to see if there is a better way to figure out when to do aggregation
            req_data
                .lamellae
                .send_to_pes_async(req_data.pe, team.arch.clone(), data).await;
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
        // } else {
        //     if req_data.pe == None && my_pe != None {
        //         exec_local(
        //             ame,
        //             req_data.msg,
        //             *func,
        //             req_data.ireq.clone(),
        //             world,
        //             team.clone(),
        //         )
        //         .await;
        //     }
        //     let id = if let Some(pe) = req_data.pe {
        //         team.arch.world_pe(pe).expect("invalid pe") as u64 // aggregate to same pe across team boundaries
        //     } else {
        //         req_data.team_hash //use this to represent we want to send to all pes in the team
        //     };

        //     let my_any: LamellarAny = Box::new(0);
        //     let msg = Msg {
        //         cmd: ExecType::Runtime(Cmd::ExecBatchMsgSend),
        //         src: req_data.src as u16, //fake that this is from the original sender
        //         req_id: 0,
        //         team_id: id as usize, // we use this as the lookup id int the hashmaps
        //         return_data: false,
        //     };
        //     let new_req = ReqData {
        //         src: req_data.src,
        //         pe: req_data.pe,
        //         msg: msg,
        //         ireq: req_data.ireq.clone(),
        //         func: my_any,
        //         lamellae: req_data.lamellae.clone(),
        //         team_hash: req_data.team_hash, // fake hash,
        //     };
        //     Some((data, new_req))
        // }
    }
}

//#[prof]
async fn exec_am(
    ame: &ActiveMessageEngine,
    data: &SerializedData,
    world: Arc<LamellarTeamRT>,
    team: Arc<LamellarTeamRT>,
    return_am: bool,
) ->  LamellarReturn {
    // trace!("[{:?}] exec_am {:?}", ame.my_pe, data.len);
    if let Some(header) = data.deserialize_header(){
        trace!("exec am {:?}",header.id);   
        let func = AMS_EXECS.get(&(header.id).unwrap()).unwrap()(data.data_as_bytes());
        func.exec( ame.my_pe, ame.num_pes,return_am , world.clone(), team).await
    }
    else{
        panic!{"should i be here?"};
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
                match func.exec(ame.my_pe, ame.num_pes, true, world.clone(), team.clone()).await {
                    LamellarReturn::LocalData(data) => {
                        trace!("local am data return");
                        if msg.return_data {
                            if let Ok(_) = ireq.data_tx.send((msg.src as usize, InternalResult::Local(data))) {} //if this returns an error it means the user has dropped the handle
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
                    LamellarReturn::Unit => {
                        if msg.return_data {
                            if let Ok(_) = ireq.data_tx.send((msg.src as usize, InternalResult::Unit)) {} //if this returns an error it means the user has dropped the handle
                            let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
                            if cnt == 1 {
                                REQUESTS[msg.req_id % REQUESTS.len()].remove(&msg.req_id);
                            }
                        }
                        ireq.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                        ireq.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                    }
                    LamellarReturn::RemoteData(_,_) => {
                        trace!("remote am data return");
                        panic!("should not be returning remote data from local am");
                    }
                    LamellarReturn::RemoteAm(_) => {
                        trace!("remote am am return");
                        panic!("should not be returning remote am from local am");
                    }
                    
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
                match func.exec(ame.my_pe, ame.num_pes, true, world, team.clone()).await {
                    LamellarReturn::LocalData(data) => {
                        trace!("return am local am data");
                        if msg.return_data {
                            ireq.data_tx
                                .send((msg.src as usize, InternalResult::Local(data)))
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
                    LamellarReturn::RemoteData(data,func) => { //is this even valid?
                        trace!("return am remote am data");
                        let result_size = func.serialized_result_size(&data);
                        let ser_data = team.lamellae.serialize_header(None,result_size).await.unwrap();
                        func.serialize_result_into(ser_data.data_as_bytes(),&data);
                        if msg.return_data {
                            ireq.data_tx
                                .send((
                                    msg.src as usize,
                                    InternalResult::Remote(ser_data), 
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
                    LamellarReturn::Unit =>{
                        if msg.return_data {
                            if let Ok(_) = ireq.data_tx.send((msg.src as usize, InternalResult::Unit)) {} //if this returns an error it means the user has dropped the handle
                            let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
                            if cnt == 1 {
                                REQUESTS[msg.req_id % REQUESTS.len()].remove(&msg.req_id);
                            }
                        }
                        ireq.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                        ireq.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
                    }
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

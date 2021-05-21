use crate::active_messaging::*;
//{
    // ActiveMessageEngine, Cmd, ExecType, LamellarBoxedAm, LamellarReturn, Msg, RetType,
    // REQUESTS,LamellarBoxedData,
// };
use crate::lamellae::{Lamellae,LamellaeAM,SerializeHeader,SerializedData,Ser,Des};
use crate::lamellar_request::*;
use crate::lamellar_team::LamellarTeamRT;
use crate::scheduler::{NewReqData,AmeSchedulerQueue};
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use log::trace;
use std::sync::atomic::Ordering;
use std::sync::Arc;

// enum BatchAmId{
const UnitId: AmId = 0;
const BatchedUnitId: AmId = 1;
const RemoteDataId: AmId = 2;
const RemoteAmId: AmId = 3;
const AmIdStart: AmId = 4;

pub (crate) type UnpackFn = fn(&[u8]) -> LamellarBoxedAm;
pub(crate) type AmId = u16;
lazy_static! {
    pub(crate) static ref AMS_IDS: HashMap<String, AmId> = {
        
        let mut ams = vec![];
        for am in crate::inventory::iter::<RegisteredAm>{
            ams.push(am.name.clone());
        }
        ams.sort();
        let mut cnt = AmIdStart; 
        let mut temp = HashMap::new();
        for am in ams{
            temp.insert(am.clone(),cnt);
            cnt+=1;
        }
        temp
    };
}
lazy_static!{
    pub(crate) static ref AMS_EXECS: HashMap<AmId, UnpackFn> = {
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

pub(crate) struct RegisteredActiveMessages{
    submitted_ams: Arc<Mutex<HashMap<Option<usize>,HashMap<u64,HashMap<AmId,(Vec<(usize,Option<LamellarArcAm>)>,usize)>>>>>, //pe, team hash, function id (req_id,function)
    txed_ams: Arc<Mutex<HashMap<usize,Vec<usize>>>>, //batched req id, actual ids
    cur_batch_id: Arc<AtomicUsize>,
    scheduler: Arc<AmeScheduler>,
}

impl RegisteredActiveMessages{
    pub(crate) fn new(scheduler: Arc<AmeScheduler>) -> RegisteredActiveMessages{
        RegisteredActiveMessages{
            submitted_ams:  Arc::new(Mutex::new(HashMap::new())),
            txed_ams: Arc::new(Mutex::new(HashMap::new())),
            cur_batch_id: Arc::new(AtomicUsize::new(0)),
            scheduler: scheduler,
        }
    }

    // right now we batch by (destination pe, team, func) triplets
    // need to analyze the benefits of different levels of batching (e.g. just batch based on pe)
    fn add_req_to_batch(&self,
        func: Option<LamellarArcAm>, 
        func_size: usize, 
        func_id: AmId, 
        req_data: Arc<NewReqData>,){
        let mut submit_tx_task = false;
        let mut map = self.submitted_ams.lock();
        let mut entry = map.entry(req_data.dst).or_insert(HashMap::new()) // pe
        .entry(req_data.team_hash).or_insert(HashMap::new()) // team
        .entry(func_id).or_insert_with(|| { // func 
            submit_tx_task = true;
            (Vec::new(),0)
        });          
        entry.0.push((req_data.id,func));
        entry.1 += func_size; 
        drop(map); 

        if submit_tx_task{
            let submitted_ams = self.submitted_ams.clone();
            let txed_ams = self.txed_ams.clone();
            let batch_id = self.cur_batch_id.fetch_add(1, Ordering::Relaxed);
            // println!{"submitting tx_task {:?} {:?}",batch_id,req_data.cmd};
            self.scheduler.submit_task( async move{
                let mut cnt: usize=0;                       // -- this is a poor mans rate limiter
                while cnt < 10000{                          // essentially we want to make sure we
                    async_std::task::yield_now().await;     // buffer enough requests to make the
                    cnt+=1;                                 // batching worth it but also quick response time
                }                                           // can definitely do better
                let team_map = { //all requests going to pe
                    let mut map = submitted_ams.lock();
                    map.remove(&req_data.dst)
                };    
                // println!{"in submit_tx_task {:?} {:?}",batch_id,req_data.cmd};
                if let Some(team_map) = team_map{
                    if let Some(func_map) = team_map.get(&req_data.team_hash){                
                        let (reqs,size) = func_map.get(&func_id).unwrap();
                        
                        let msg = Msg {
                            cmd: req_data.cmd, 
                            src: req_data.team.world_pe as u16, //this should always originate from me?
                            req_id: batch_id,
                        };

                        let header = Some(SerializeHeader{msg: msg, team_hash: req_data.team_hash, id: func_id});
                        let data = req_data.lamellae.serialize_header(header,*size).await.unwrap();
                        let data_slice = data.data_as_bytes();
                        let mut i =0;
                        let mut ids: Vec<usize> = Vec::new();
                        for (req_id,func) in reqs{
                            match func_id {
                                UnitId | BatchedUnitId  => {
                                    let serialize_size = crate::serialized_size(req_id);
                                    crate::serialize_into(&mut data_slice[i..i+serialize_size],req_id).unwrap();
                                    i+=serialize_size;
                                }
                                RemoteDataId => { }
                                RemoteAmId => { }
                                _ => {
                                    if let Some(func) = func {
                                        let serialize_size = func.serialized_size();
                                        func.serialize_into(&mut data_slice[i..(i+serialize_size)]);
                                        if req_data.dst.is_none() {
                                            func.ser(req_data.team.num_pes()-1);
                                        }
                                        else{
                                            func.ser(1);
                                        };
                                        ids.push(*req_id);
                                        i+=serialize_size;
                                    }
                                    else{
                                        panic!("should not be none"); //user registered function
                                    }
                                }
                            }
                        }
                        if ids.len() > 0 { //only when we are sending initial requests, not return requests
                            txed_ams.lock().insert(batch_id,ids); 
                        }
                        // println!("sending batch {:?}",batch_id);
                        req_data.lamellae.send_to_pes_async(req_data.dst, req_data.team.arch.clone(), data).await;
                    }
                }
            });
        }
        
    }

    
    async fn send_req(&self,
        func: Option<LamellarArcAm>, 
        func_size: usize, 
        func_id: AmId, 
        req_data: Arc<NewReqData>,){
        if let Some(func) = func {
            if req_data.dst.is_none() {
                func.ser(req_data.team.num_pes()-1);
            }
            else{
                func.ser(1);
            };
            let msg = Msg {
                cmd: req_data.cmd, 
                src: req_data.team.world_pe as u16, //this should always originate from me?
                req_id: req_data.id,
            };
            // println!("sending req {:?}",msg);
            let header = Some(SerializeHeader{msg: msg, team_hash: req_data.team_hash, id: func_id});
            let data = req_data.lamellae.serialize_header(header,func_size).await.unwrap();
            func.serialize_into(data.data_as_bytes());
            req_data.lamellae.send_to_pes_async(req_data.dst, req_data.team.arch.clone(), data).await;
        }
        else{
            panic!("should not be possible");
        }
    }

    pub (crate) async fn process_am_req (
        &self,
        mut req_data: NewReqData,
       ){ 
        let func = match &req_data.func{
            LamellarFunc::Am(ref func) => Some(func.clone()),
            LamellarFunc::None => None,
            _ => panic!("should only process AMS"),
        };
        

        let my_pe = if let Ok(my_pe) = req_data.team.arch.team_pe(req_data.src) {
            Some(my_pe)
        } else {
            None
        };

        if req_data.dst == my_pe && my_pe != None {
            trace!("[{:?}] single local request ", my_pe);
           
                    // exec_local( 
                    //     req_data.msg,
                    //     func,
                    //     req_data.ireq.clone(),
                    //     world,
                    //     team.clone(),
                    // ).await;
        }
        else{
            trace!("[{:?}] remote request ", my_pe);
            let (func_id, func_size, cmd) = match func{
                Some(ref func) => {
                    let func_id = AMS_IDS.get(&func.get_id()).unwrap();
                    let func_size = func.serialized_size();
                    (*func_id,func_size,ExecType::Am(Cmd::BatchedMsg))
                }
                None =>{
                    match req_data.cmd{
                        ExecType::Am(Cmd::BatchedUnitReturn) => (BatchedUnitId, crate::serialized_size(&req_data.id),ExecType::Am(Cmd::BatchedUnitReturn)),
                        ExecType::Am(Cmd::UnitReturn) => (UnitId, crate::serialized_size(&req_data.id),ExecType::Am(Cmd::UnitReturn)),
                        _ => panic!("not handled yet")
                    }
                }
            };            
            
            let req_data = if func_size <= 10000{
                req_data.cmd=cmd;
                let req_data = Arc::new(req_data);
                self.add_req_to_batch(func,func_size,func_id,req_data.clone());//,Cmd::BatchedMsg);
                req_data
            }
            else{
                let req_data = Arc::new(req_data);
                self.send_req(func,func_size,func_id,req_data.clone()).await;
                req_data
            };

            // if req_data.pe == None && my_pe != None {
            //     self.scheduler.submit_task(async move {
                    
            //         exec_local( //we could probably move this out of the async
            //             req_data.msg,
            //             func,
            //             req_data.ireq.clone(),
            //             world,
            //             team.clone(),
            //         ).await;
            //     });
            // }
        }
        
    }

    async fn exec_single_msg(&self,
        ame: Arc<ActiveMessageEngine>,
        msg: Msg, 
        ser_data: SerializedData, 
        lamellae: Arc<Lamellae>,
        world: Arc<LamellarTeamRT>,
        team: Arc<LamellarTeamRT>,
        return_am: bool,){
        if let Some(header) = ser_data.deserialize_header(){
            let func = AMS_EXECS.get(&(header.id)).unwrap()(ser_data.data_as_bytes());
            let lam_return = func.exec( team.world_pe, team.num_world_pes, return_am , world.clone(), team.clone()).await;
            match lam_return{
                LamellarReturn::Unit =>{  
                    let req_data = NewReqData{
                        src: team.world_pe ,
                        dst: Some(msg.src as usize),
                        cmd: ExecType::Am(Cmd::UnitReturn),
                        id: msg.req_id,
                        func:  LamellarFunc::None,
                        lamellae: lamellae,
                        world: world,
                        team: team,
                        team_hash: header.team_hash,
                    };
                    ame.process_msg_new(req_data, None).await;
                }
                LamellarReturn::LocalData(_) | LamellarReturn::LocalAm(_) =>{
                    panic!("Should not be returning local data from remote  am");
                }
                // LamellarReturn::RemoteAm(func) => {
                //     // let rmsg = Msg {
                //     //     cmd: ExecType::Am(Cmd::ExecReturn),
                //     //     src: team.world_pe as u16,
                //     //     req_id: msg.req_id,
                //     // };
                //     // let id = *AMS_ID.get(&func.get_id()).unwrap();
                // }
                // LamellarReturn::RemoteData(ref _d,ref _f) => {
                //     let rmsg = Msg {
                //         cmd: ExecType::Runtime(Cmd::DataReturn),
                //         src: team.world_pe as u16,
                //         req_id: msg.req_id,
                //     };
                    
                // },
                _ => {panic!("unandled return type ")}
            }
        }
    }

    fn exec_batched_msg(&self,
        ame: Arc<ActiveMessageEngine>,
        msg: Msg, 
        ser_data: SerializedData, 
        lamellae: Arc<Lamellae>,
        world: Arc<LamellarTeamRT>,
        team: Arc<LamellarTeamRT>) {
        if let Some(header) = ser_data.deserialize_header(){
            trace!("exec am {:?}",header.id);
            let data_slice=ser_data.data_as_bytes();    
            let batch_id = msg.req_id;
            // println!("execing batch_id {:?}",batch_id);
            let mut index = 0;
            let mut results: Arc<Mutex<(HashMap<usize,LamellarReturn>,usize)>>= Arc::new(Mutex::new((HashMap::new(),0)));
            let  req_cnt = Arc::new(AtomicUsize::new(0));
            let  exec_cnt = Arc::new(AtomicUsize::new(0));
            let  processed = Arc::new(AtomicBool::new(false));
            let mut req_id =0;
            let team_hash = header.team_hash;
            while index < data_slice.len(){
                req_cnt.fetch_add(1,Ordering::SeqCst);
                let func = AMS_EXECS.get(&(header.id)).unwrap()(&data_slice[index..]);
                index += func.serialized_size();
                let world = world.clone();
                let team = team.clone();
                let lamellae = lamellae.clone();
                let req_cnt = req_cnt.clone();
                let exec_cnt = exec_cnt.clone();
                let results = results.clone();
                let processed = processed.clone();
                let ame = ame.clone();
                self.scheduler.submit_task(async move {
                    let lam_return = func.exec( team.world_pe, team.num_world_pes, false , world.clone(), team.clone()).await;
                    
                    let (num_rets,num_unit_rets) = {
                        let mut entry = results.lock();
                        if let LamellarReturn::Unit  = &lam_return{
                            entry.1  += 1;
                        }
                        entry.0.insert(req_id,lam_return);
                        (entry.0.len(),entry.1)
                    };
                    let my_cnt = exec_cnt.fetch_add(1, Ordering::SeqCst) + 1;
                    while my_cnt == req_cnt.load(Ordering::SeqCst){
                        if  processed.load(Ordering::SeqCst) == true {
                            if my_cnt == req_cnt.load(Ordering::SeqCst) {                            
                                if num_rets == num_unit_rets { //every result was a unit --- we probably can determine this from the function...
                                    let req_data = NewReqData{
                                        src: team.world_pe ,
                                        dst: Some(msg.src as usize),
                                        cmd: ExecType::Am(Cmd::BatchedUnitReturn),
                                        id: batch_id,
                                        func:  LamellarFunc::None,
                                        lamellae: lamellae,
                                        world: world,
                                        team: team,
                                        team_hash: team_hash,
                                    };
                                    ame.process_msg_new(req_data, None).await;
                                    
                                }
                            }
                            break;
                        }
                        async_std::task::yield_now().await;
                    }
                });
                req_id +=1;
            }
            processed.store(true,Ordering::SeqCst);
        }
        else{
            panic!{"should i be here?"};
        }
    }

    fn process_batched_unit_return(&self, msg: Msg, ser_data: SerializedData,){

        let batch_id = msg.req_id;
        // println!("processing returns {:?}",batch_id);
        let data_slice=ser_data.data_as_bytes();  
        let mut index=0;
        let serialized_size = crate::serialized_size(&0usize);
        // println!("data_slice {:?}",data_slice);
        while index < data_slice.len(){
            let batch_id: usize = crate::deserialize(&data_slice[index..(index+serialized_size)]).unwrap();
            let reqs = self.txed_ams.lock().remove(&batch_id);
            
            if let Some(reqs) = reqs{
                // println!("reqs {:?}", &reqs[0..10]);
                for req in &reqs{
                    ActiveMessageEngine::send_data_to_user_handle(*req,msg.src,InternalResult::Unit);
                }
            }
            else{
                panic!("batch id {:?} not found",batch_id);
            }
            index+=serialized_size;
        }
    }

    fn process_unit_return(&self, msg: Msg, ser_data: SerializedData,){

        // println!("processing returns {:?}",batch_id);
        let data_slice=ser_data.data_as_bytes();  
        let mut index=0;
        let serialized_size = crate::serialized_size(&0usize);
        // println!("data_slice {:?}",data_slice);
        while index < data_slice.len(){
            let req_id: usize = crate::deserialize(&data_slice[index..(index+serialized_size)]).unwrap();
            ActiveMessageEngine::send_data_to_user_handle(req_id,msg.src,InternalResult::Unit);
            index+=serialized_size;
        }
    }

    pub(crate) async fn process_batched_am(&self,
        ame: Arc<ActiveMessageEngine>,
        cmd: Cmd,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        world: Arc<LamellarTeamRT>,
        team: Arc<LamellarTeamRT>) {
        match cmd{
            Cmd::Exec => self.exec_single_msg(ame, msg,ser_data,lamellae,world,team,false).await,
            Cmd::BatchedMsg => self.exec_batched_msg(ame,msg,ser_data,lamellae,world,team),
            Cmd::BatchedUnitReturn => self.process_batched_unit_return(msg,ser_data),
            Cmd::UnitReturn => self.process_unit_return(msg,ser_data),
            _ => println!("unhandled cmd {:?}",msg.cmd)
        }
    }
}

//     pub(crate) async fn exec_am_cmd(
//         ame: Arc<ActiveMessageEngine>,
//         cmd: Cmd,
//         msg: Msg,
//         ser_data: SerializedData,
//         lamellae: Arc<Lamellae>,
//         world: Arc<LamellarTeamRT>,
//         team: Arc<LamellarTeamRT>,
//     ) -> Option<ReqData> {
//         match cmd {
//             Cmd::Exec => {
//                 let lam_return = exec_am(ame.clone(), &ser_data, world, team.clone(),false).await;
//                 // if msg.return_data {
//                     let cmd = match &lam_return {
//                         LamellarReturn::LocalData(_) | LamellarReturn::LocalAm(_) =>{
//                             panic!("Should not be returning local data from remote  am");
//                         }
//                         LamellarReturn::RemoteAm(ref _d) => ExecType::Am(Cmd::ExecReturn),
//                         LamellarReturn::RemoteData(ref _d,ref _f) => ExecType::Runtime(Cmd::DataReturn),
//                         LamellarReturn::Unit => ExecType::Runtime(Cmd::BatchedUnitReturn),//need to fix batched unit returns
//                     };
//                     ame.send_response(cmd, lam_return, msg, lamellae,team.my_hash).await
//                 // } else {
//                 //     None
//                 // }
//             }
//             Cmd::ExecReturn => {
//                 match exec_am(ame.clone(), &ser_data, world, team,true).await {
//                     LamellarReturn::LocalData(data) => ActiveMessageEngine::send_data_to_user_handle(msg.req_id, msg.src, InternalResult::Local(data)),
//                     LamellarReturn::LocalAm(_) => panic!("Should not be returning local data from remote  am"),
//                     LamellarReturn::RemoteAm(_) => panic!("returning another AM from a returned AM not yet implemented"),
//                     LamellarReturn::RemoteData(_,_) => panic!("Should not be returning remote data from a returned am"),
//                     LamellarReturn::Unit => ActiveMessageEngine::send_data_to_user_handle(msg.req_id, msg.src, InternalResult::Unit)
//                 };
//                 None
//             }
//             _ => {
//                 println!(
//                     "[LAMELLAR WARNING] am unknown(or unhandled) command {:?}",
//                     msg.cmd
//                 );
//                 None
//             }
//         }
//     }
// }



// // #[prof]
// async fn exec_am(
//     ame: Arc<ActiveMessageEngine>,
//     data: &SerializedData,
//     world: Arc<LamellarTeamRT>,
//     team: Arc<LamellarTeamRT>,
//     return_am: bool,
// ) ->  LamellarReturn {
//     // trace!("[{:?}] exec_am {:?}", ame.my_pe, data.len);
//     if let Some(header) = data.deserialize_header(){
//         trace!("exec am {:?}",header.id);   
//         let func = AMS_EXECS.get(&(header.id)).unwrap()(data.data_as_bytes());
//         func.exec( ame.my_pe, ame.num_pes, return_am , world.clone(), team).await
//     }
//     else{
//         panic!{"should i be here?"};
//     }
// }

// //#[prof]
// pub(crate) async fn exec_local(
//     // ame: Arc<ActiveMessageEngine>,
//     msg: Msg,
//     func: &LamellarBoxedAm,
//     ireq: InternalReq,
//     world: Arc<LamellarTeamRT>,
//     team: Arc<LamellarTeamRT>,
// ) {
//     // trace!("[{:?}] exec_local_am: {:?}", ame.my_pe, msg);
//     // if let ExecType::Am(cmd) = msg.cmd.clone() {
//     //     match cmd {
//     //         Cmd::Exec => {
//     //             match func.exec(ame.my_pe, ame.num_pes, true, world.clone(), team.clone()).await {
//     //                 LamellarReturn::LocalData(data) => {
//     //                     trace!("local am data return");
//     //                     // if msg.return_data {
//     //                     if let Ok(_) = ireq.data_tx.send((msg.src as usize, InternalResult::Local(data))) {} //if this returns an error it means the user has dropped the handle
//     //                     let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
//     //                     if cnt == 1 {
//     //                         REQUESTS[msg.req_id % REQUESTS.len()].remove(&msg.req_id);
//     //                     }
//     //                     // }
//     //                     ireq.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
//     //                     ireq.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
//     //                 }
//     //                 LamellarReturn::LocalAm(am) => {
//     //                     trace!("local am am return");
//     //                     exec_return_am(ame, msg, am, ireq, world, team).await;
//     //                 }
//     //                 LamellarReturn::Unit => {
//     //                     // if msg.return_data {
//     //                     if let Ok(_) = ireq.data_tx.send((msg.src as usize, InternalResult::Unit)) {} //if this returns an error it means the user has dropped the handle
//     //                     let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
//     //                     if cnt == 1 {
//     //                         REQUESTS[msg.req_id % REQUESTS.len()].remove(&msg.req_id);
//     //                     }
//     //                     // }
//     //                     ireq.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
//     //                     ireq.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
//     //                 }
//     //                 LamellarReturn::RemoteData(_,_) => {
//     //                     trace!("remote am data return");
//     //                     panic!("should not be returning remote data from local am");
//     //                 }
//     //                 LamellarReturn::RemoteAm(_) => {
//     //                     trace!("remote am am return");
//     //                     panic!("should not be returning remote am from local am");
//     //                 }
                    
//     //             }
//     //         }
//     //         _ => println!(
//     //             "[LAMELLAR WARNING] local am unknown(or unhandled) command {:?}",
//     //             msg.cmd
//     //         ),
//     //     }
//     // } else {
//     //     println!(
//     //         "[LAMELLAR WARNING] shouldn't have an exectype of {:?} in registered active message",
//     //         msg.cmd
//     //     )
//     // }
// }

// //#[prof]
// pub(crate) async fn exec_return_am(
//     ame: Arc<ActiveMessageEngine>,
//     msg: Msg,
//     func: LamellarBoxedAm,
//     ireq: InternalReq,
//     world: Arc<LamellarTeamRT>,
//     team: Arc<LamellarTeamRT>,
// ) {
//     // let s = Instant::now();
//     trace!("[{:?}] exec_return_am: {:?}", ame.my_pe, msg);
//     if let ExecType::Am(cmd) = msg.cmd.clone() {
//         match cmd {
//             Cmd::Exec => {
//                 match func.exec(ame.my_pe, ame.num_pes, true, world, team.clone()).await {
//                     LamellarReturn::LocalData(data) => {
//                         trace!("return am local am data");
//                         // if msg.return_data {
//                         ireq.data_tx
//                             .send((msg.src as usize, InternalResult::Local(data)))
//                             .expect("error returning local data");
//                         let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
//                         if cnt == 1 {
//                             REQUESTS[msg.req_id % REQUESTS.len()].remove(&msg.req_id);
//                         }
//                         // }
//                         ireq.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
//                         ireq.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
//                     }
//                     LamellarReturn::LocalAm(_am) => {
//                         trace!("return am local am return");
//                         println!("returning an am from a returned am not currently valid");
//                     }
//                     LamellarReturn::RemoteData(data,func) => { //is this even valid?
//                         trace!("return am remote am data");
//                         let result_size = func.serialized_result_size(&data);
//                         let ser_data = team.lamellae.serialize_header(None,result_size).await.unwrap();
//                         func.serialize_result_into(ser_data.data_as_bytes(),&data);
//                         // if msg.return_data {
//                         ireq.data_tx
//                             .send((
//                                 msg.src as usize,
//                                 InternalResult::Remote(ser_data), 
//                             ))
//                             .expect("error returning remote data");
//                         let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
//                         if cnt == 1 {
//                             REQUESTS[msg.req_id % REQUESTS.len()].remove(&msg.req_id);
//                         }
//                         // }
//                         ireq.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
//                         ireq.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
//                     }
//                     LamellarReturn::RemoteAm(_am) => {
//                         trace!("return am remote am return");
//                         println!("returning an am from a returned am not currently valid");
//                     }
//                     LamellarReturn::Unit =>{
//                         // if msg.return_data {
//                         if let Ok(_) = ireq.data_tx.send((msg.src as usize, InternalResult::Unit)) {} //if this returns an error it means the user has dropped the handle
//                         let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
//                         if cnt == 1 {
//                             REQUESTS[msg.req_id % REQUESTS.len()].remove(&msg.req_id);
//                         }
//                         // }
//                         ireq.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
//                         ireq.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
//                     }
//                 }
//             }
//             _ => println!("[LAMELLAR WARNING] local  am unknown command {:?}", msg.cmd),
//         }
//     } else {
//         println!(
//             "[LAMELLAR WARNING] shouldn't have an exectype of {:?} in registered active message",
//             msg.cmd
//         )
//     }
// }

use crate::active_messaging::*;
//{
    // ActiveMessageEngine, Cmd, ExecType, LamellarBoxedAm, LamellarReturn, Msg, RetType,
    // REQUESTS,LamellarBoxedData,
// };
use crate::lamellae::{Lamellae,LamellaeAM,SerializeHeader,SerializedData,Ser,Des,SubData};
use crate::lamellar_request::*;
use crate::lamellar_team::LamellarTeamRT;
use crate::scheduler::{NewReqData,AmeSchedulerQueue};
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use log::trace;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::collections::HashSet;
use futures::future::join_all;

// enum BatchAmId{
const UNIT_ID: AmId = 0;
const BATCHED_UNIT_ID: AmId = UNIT_ID + 1;
const REMOTE_DATA_ID: AmId = BATCHED_UNIT_ID + 1;
const BATCHED_REMOTE_DATA_ID: AmId = REMOTE_DATA_ID + 1;
const REMOTE_AM_ID: AmId = BATCHED_REMOTE_DATA_ID + 1;
const AM_ID_START: AmId = REMOTE_AM_ID + 1;

pub (crate) type UnpackFn = fn(&[u8]) -> LamellarArcAm;
pub(crate) type AmId = u16;
lazy_static! {
    pub(crate) static ref AMS_IDS: HashMap<String, AmId> = {
        
        let mut ams = vec![];
        for am in crate::inventory::iter::<RegisteredAm>{
            ams.push(am.name.clone());
        }
        ams.sort();
        let mut cnt = AM_ID_START; 
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


//TODO: we actually do need to group by team as well,
// so need to add the team hash hashmap back and move total size into there instead.
pub(crate) struct RegisteredActiveMessages{
    submitted_ams:  Arc<Mutex<HashMap<
                                Option<usize>, //pe
                                // HashMap<
                                    // u64, //team hash
                                (
                                    HashMap<
                                        AmId, //func id
                                        (
                                            HashMap< 
                                                Option<usize>, //batch_id
                                                Vec<(usize,LamellarFunc)>, //req_id, am
                                            >
                                            ,usize//func size 
                                        )                     
                                    >,
                                    usize,//total size
                                )
                                // >
                    >>>, //pe, team hash, function id, batch id : (req_id,function), total data size // maybe we can remove the func id lookup if we enfore that a batch only contains the same function.. which I think we do...
    txed_ams: Arc<Mutex<HashMap<
                            usize, //batched req id
                            Mutex<HashSet<usize>>>>>, //actual ids
    cur_batch_id: Arc<AtomicUsize>,
    scheduler: Arc<AmeScheduler>,
}

impl RegisteredActiveMessages{
    pub(crate) fn new(scheduler: Arc<AmeScheduler>) -> RegisteredActiveMessages{
        RegisteredActiveMessages{
            submitted_ams:  Arc::new(Mutex::new(HashMap::new())),
            txed_ams: Arc::new(Mutex::new(HashMap::new())),
            cur_batch_id: Arc::new(AtomicUsize::new(1)),
            scheduler: scheduler,
        }
    }

    // right now we batch by (destination pe, team, func) triplets
    // need to analyze the benefits of different levels of batching (e.g. just batch based on pe)
    fn add_req_to_batch(&self,
        func: LamellarFunc, 
        func_size: usize, 
        func_id: AmId, 
        req_data: Arc<NewReqData>,){
        println!("adding req {:?} {:?}",func_id,func_size);
        // add request to a batch or create a new one
        let mut submit_tx_task = false;
        let mut new_func = false;
        let mut new_batch = false;
        let mut map = self.submitted_ams.lock();
        let mut pe_entry = map.entry(req_data.dst).or_insert_with(|| { 
            println!("going to submit tx task {:?}",func_id);
            submit_tx_task = true;
            (HashMap::new(),0)
        }); // pe
        pe_entry.1 += func_size;
        // let mut team_entry = pe_entry.entry(req_data.team_hash).or_insert(HashMap::new()); // team
        let mut func_entry= pe_entry.0.entry(func_id).or_insert_with(|| {
            (HashMap::new(),0)
        }); //func
        if func_entry.1 ==0 {
            pe_entry.1 += crate::serialized_size(&0usize);
        }
        func_entry.1 += func_size;

        let mut batch_entry = func_entry.0.entry(req_data.batch_id).or_insert_with( || {
            println!("going to submit new batch {:?}",req_data.batch_id);
            
            // new_batch = true;
            Vec::new()
        }); // batch id        
        if batch_entry.len() == 0{
            func_entry.1 += crate::serialized_size(&Some(0usize));
        }
        batch_entry.push((req_data.clone(),func));
        
        // if new_batch && req_data.batch_id.is_some(){
        //     func_entry.1 += crate::serialized_size(&Some(0usize)); 
        // }
        drop(map); 
        //--------------------------

        if submit_tx_task{
            let submitted_ams = self.submitted_ams.clone();
            let txed_ams = self.txed_ams.clone();
            let outgoing_batch_id = self.cur_batch_id.clone();//fetch_add(1, Ordering::Relaxed);
            println!{"submitting tx_task {:?} {:?}",outgoing_batch_id,req_data.cmd};
            self.scheduler.submit_task( async move{
                let mut cnt: usize=0;                       // -- this is a poor mans rate limiter
                while cnt < 10000{                          // essentially we want to make sure we
                    async_std::task::yield_now().await;     // buffer enough requests to make the
                    cnt+=1;                                 // batching worth it but also quick response time
                }                                           // ...can definitely do better
                let func_map = { //all requests going to pe
                    let mut map = submitted_ams.lock();
                    map.remove(&req_data.dst)
                };    
                println!{"in submit_tx_task {:?} {:?}",outgoing_batch_id,req_data.cmd};
                if let Some((func_map,total_size)) = func_map{
                    let header_size = std::mem::size_of::<Option<SerializeHeader>>();
                    let agg_size = total_size + (func_map.len()-1) * header_size;
                    let data = req_data.lamellae.serialize_header(None,agg_size).await.unwrap();
                    let data_slice = data.header_and_data_as_bytes();
                    let mut i = 0;
                    for  (func_id,(batch_map,size)) in func_map{
                        let msg = Msg {
                            cmd: req_data.cmd, 
                            src: req_data.team.world_pe as u16, //this should always originate from me?
                            req_id: outgoing_batch_id.fetch_add(1, Ordering::Relaxed),
                        };
                        let header = Some(SerializeHeader{msg: msg, team_hash: req_data.team_hash, id: func_id});
                        crate::serialize_into(&mut data_slice[i..i+header_size],&header).unwrap();
                        i+= header_size;
                    }
                    // if let Some(func_map) = team_map.get(&req_data.team_hash){                
                        
                        
                        // let header_size = crate::serialized_size(&Some(0usize));
                        
                        // let agg_size = *total_size + (batch_map.len()-1) * header_size;//-1 becauze serialize_header includes the space of the first header when allocating buffer
                        
                        // let msg = Msg {
                        //     cmd: req_data.cmd, 
                        //     src: req_data.team.world_pe as u16, //this should always originate from me?
                        //     req_id: outgoing_batch_id,
                        // };
                        // let header = Some(SerializeHeader{msg: msg, team_hash: req_data.team_hash, id: func_id});
                        
                        // let data_slice = data.data_as_bytes();
                        // println!("{:?} (data len {:?})",*size,data_slice.len());
                        // let mut i = 0;

                        // for (batch_id,reqs) in batch_map{
                        //     // let batch_id = if let Some(batch_id) = batch_id { // request is part of a batch already
                        //     //     *batch_id
                        //     // }
                        //     // else{ 
                        //     //     outgoing_batch_id
                        //     // };
                        //     // let msg = Msg {
                        //     //     cmd: req_data.cmd, 
                        //     //     src: req_data.team.world_pe as u16, //this should always originate from me?
                        //     //     req_id: outgoing_batch_id,
                        //     // };
                        //     // let header = Some(SerializeHeader{msg: msg, team_hash: req_data.team_hash, id: func_id});
                        //     // crate::serialize_into(&mut data_slice[i..i+header_size],&header).unwrap();
                        //     // i+= header_size;
                        //     // crate::serialize_into(&mut data_slice[i..i+header_size],&batch_id).unwrap();
                        //     // i+= header_size;
                        //     let mut first = true;
                            
                        //     let mut req_ids: HashSet<usize> = HashSet::new();
                        //     for (req_id,func) in reqs{
                        //         println!("req_id {:?} func_id {:?} batch_id {:?}",req_id,func_id,batch_id);
                        //         match func_id {
                        //             BATCHED_UNIT_ID  => {
                        //                 let serialize_size = crate::serialized_size(batch_id);
                        //                 crate::serialize_into(&mut data_slice[i..i+serialize_size],batch_id).unwrap();
                        //                 i+=serialize_size;
                        //             }
                        //             UNIT_ID => {
                        //                 let serialize_size = crate::serialized_size(req_id);
                        //                 crate::serialize_into(&mut data_slice[i..i+serialize_size],req_id).unwrap();
                        //                 i+=serialize_size;
                        //             }
                        //             REMOTE_DATA_ID => { 
                        //                 if let LamellarFunc::Result(func) = func{
                        //                     let result_header_size = crate::serialized_size(&(&req_id,0usize));
                        //                         let serialize_size = func.serialized_size();
                        //                         crate::serialize_into(&mut data_slice[i..i+serialize_size],&(req_id,serialize_size)).unwrap();
                        //                         i+=result_header_size;                                            
                        //                         func.serialize_into(&mut data_slice[i..(i+serialize_size)]);
                        //                         // if req_data.dst.is_none() {
                        //                         //     func.ser(req_data.team.num_pes()-1);
                        //                         // }
                        //                         // else{
                        //                         //     func.ser(1);
                        //                         // };
                        //                         // ids.push(*req_id);
                        //                         i+=serialize_size;
                        //                 }
                        //             }
                        //             BATCHED_REMOTE_DATA_ID => {
                        //                 if (first) {
                        //                     let serialize_size = crate::serialized_size(batch_id);
                        //                     crate::serialize_into(&mut data_slice[i..i+serialize_size],batch_id).unwrap();
                        //                     i+=serialize_size;
                        //                     first = false;
                        //                 }
                        //                 if let LamellarFunc::Result(func) = func{
                        //                     let result_header_size = crate::serialized_size(&(&req_id,0usize));
                        //                         let serialize_size = func.serialized_size();
                        //                         crate::serialize_into(&mut data_slice[i..i+serialize_size],&(req_id,serialize_size)).unwrap();
                        //                         i+=result_header_size;                                            
                        //                         func.serialize_into(&mut data_slice[i..(i+serialize_size)]);
                        //                         // if req_data.dst.is_none() {
                        //                         //     func.ser(req_data.team.num_pes()-1);
                        //                         // }
                        //                         // else{
                        //                         //     func.ser(1);
                        //                         // };
                        //                         // ids.push(*req_id);
                        //                         i+=serialize_size;
                        //                 }

                        //             }
                        //             REMOTE_AM_ID => panic! {"not handled yet {:?}",func_id},
                        //             _ => {
                        //                 match func{
                        //                     LamellarFunc::Am(func) => {
                        //                         let serialize_size = func.serialized_size();
                        //                         func.serialize_into(&mut data_slice[i..(i+serialize_size)]);
                        //                         if req_data.dst.is_none() {
                        //                             func.ser(req_data.team.num_pes()-1);
                        //                         }
                        //                         else{
                        //                             func.ser(1);
                        //                         };
                        //                         req_ids.insert(*req_id);
                        //                         i+=serialize_size;
                        //                     },
                                            
                        //                     LamellarFunc::None =>{
                        //                         panic!("should not be none"); //user registered function
                        //                     },
                        //                     _ =>{
                        //                         panic!("not handled yet");
                        //                     }

                        //                 }
                        //             } 
                        //         }
                        //     }
                        //     if req_ids.len() > 0 { //only when we are sending initial requests, not return requests
                        //         txed_ams.lock().insert(outgoing_batch_id,Mutex::new(req_ids)); 
                        //     }
                        // }                       
                        println!("sending batch {:?}",data.header_and_data_as_bytes());
                        req_data.lamellae.send_to_pes_async(req_data.dst, req_data.team.arch.clone(), data).await;
                    // }
                }
                println!("leaving tx task");
            });
        }
        
    }

    
    async fn send_req(&self,
        func: LamellarFunc, 
        func_size: usize, 
        func_id: AmId, 
        req_data: Arc<NewReqData>,){
        
        
        let msg = Msg {
            cmd: req_data.cmd, 
            src: req_data.team.world_pe as u16, //this should always originate from me?
            req_id: req_data.id,
        };
        // println!("sending req {:?}",msg);
        let header = Some(SerializeHeader{msg: msg, team_hash: req_data.team_hash, id: func_id});
        let data = req_data.lamellae.serialize_header(header,func_size).await.unwrap();
        match func{
            LamellarFunc::Am(func) => {
                if req_data.dst.is_none() {
                    func.ser(req_data.team.num_pes()-1);
                }
                else{
                    func.ser(1);
                };
                func.serialize_into(data.data_as_bytes())
            },
            LamellarFunc::Result(func) => {
                // if req_data.dst.is_none() {
                //     func.ser(req_data.team.num_pes()-1);
                // }
                // else{
                //     func.ser(1);
                // };
                func.serialize_into(data.data_as_bytes())},
            LamellarFunc::None => panic!("should not send none")
        }
        req_data.lamellae.send_to_pes_async(req_data.dst, req_data.team.arch.clone(), data).await;
    }

    pub (crate) async fn process_am_req (
        &self,
        mut req_data: NewReqData,
       ){ 
        let my_pe = if let Ok(my_pe) = req_data.team.arch.team_pe(req_data.src) {
            Some(my_pe)
        } else {
            None
        };

        if req_data.dst == my_pe && my_pe != None {
            trace!("[{:?}] single local request ", my_pe);
           
            RegisteredActiveMessages::exec_local(
                Arc::new(req_data)
            ).await;
        }
        else{
            let (func_id, func_size, cmd) = match &req_data.func{
                LamellarFunc::Am(ref func) => {
                    let func_id = AMS_IDS.get(&func.get_id()).unwrap();
                    let func_size = func.serialized_size();
                    (*func_id,func_size,ExecType::Am(Cmd::BatchedMsg))
                }
                LamellarFunc::Result(ref func) => {
                    let func_size = func.serialized_size()+crate::serialized_size(&(&req_data.id,0usize));
                    match req_data.cmd{
                        
                        ExecType::Am(Cmd::BatchedDataReturn)=>{
                            (BATCHED_REMOTE_DATA_ID,func_size,ExecType::Am(Cmd::BatchedDataReturn))
                        }
                        ExecType::Am(Cmd::DataReturn)=>{
                            (REMOTE_DATA_ID,func_size,ExecType::Am(Cmd::DataReturn))
                        }
                        _ => panic!("not handled yet")
                    }
                    
                },
                LamellarFunc::None => {
                    match req_data.cmd{
                        ExecType::Am(Cmd::UnitReturn) => (UNIT_ID, crate::serialized_size(&req_data.id),ExecType::Am(Cmd::UnitReturn)),
                        ExecType::Am(Cmd::BatchedUnitReturn) => (BATCHED_UNIT_ID, 0 ,ExecType::Am(Cmd::BatchedUnitReturn)),
                        _ => panic!("not handled yet")
                    }
                },
                _ => panic!("should only process AMS"),
            };               
            trace!("[{:?}] remote request ", my_pe);           
            
            let req_data = if func_size <= 10000{
                req_data.cmd=cmd;
                let req_data = Arc::new(req_data);
                self.add_req_to_batch(req_data.func.clone(),func_size,func_id,req_data.clone());//,Cmd::BatchedMsg);
                req_data
            }
            else{
                let req_data = Arc::new(req_data);
                self.send_req(req_data.func.clone(),func_size,func_id,req_data.clone()).await;
                req_data
            };

            if req_data.dst == None && my_pe != None {
                self.scheduler.submit_task(async move {
                    
                    RegisteredActiveMessages::exec_local(
                        req_data
                    ).await;
                });
            }
        }
        
    }

    async fn exec_local(
        req_data: Arc<NewReqData>,){
        if let  LamellarFunc::Am(func) = req_data.func.clone() {
            match func.exec(req_data.team.world_pe, req_data.team.num_world_pes, true, req_data.world.clone(), req_data.team.clone()).await {
                LamellarReturn::LocalData(data) => {
                    println!("local am data return");
                    ActiveMessageEngine::send_data_to_user_handle(req_data.id,req_data.src as u16,InternalResult::Local(data));
                }
                LamellarReturn::LocalAm(am) => {
                    println!("local am am return");
                    // exec_return_am(ame, msg, am, ireq, world, team).await;
                }
                LamellarReturn::Unit => {
                    println!("local am unit return");
                    ActiveMessageEngine::send_data_to_user_handle(req_data.id,req_data.src as u16,InternalResult::Unit);
                }
                LamellarReturn::RemoteData(_) => {
                    println!("remote am data return");
                    panic!("should not be returning remote data from local am");
                }
                LamellarReturn::RemoteAm(_) => {
                    println!("remote am am return");
                    panic!("should not be returning remote am from local am");
                }   
            }
        }
        else{
            panic!("should only exec local ams");
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
                        batch_id: None,
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
                LamellarReturn::RemoteData(d) => {
                    let req_data = NewReqData{
                        src: team.world_pe ,
                        dst: Some(msg.src as usize),
                        cmd: ExecType::Am(Cmd::DataReturn),
                        id: msg.req_id,
                        batch_id: None,
                        func:  LamellarFunc::Result(d),
                        lamellae: lamellae,
                        world: world,
                        team: team,
                        team_hash: header.team_hash,
                    };
                    ame.process_msg_new(req_data, None).await;
                },
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
            println!("execing batch_id {:?}",batch_id);
            let mut index = 0;
            let mut results: Arc<Mutex<(HashMap<usize,LamellarReturn>,usize)>>= Arc::new(Mutex::new((HashMap::new(),0)));
            let  req_cnt = Arc::new(AtomicUsize::new(0));
            let  exec_cnt = Arc::new(AtomicUsize::new(0));
            let  processed = Arc::new(AtomicBool::new(false));
            let mut req_id =0;
            let team_hash = header.team_hash;
            let func_id = header.id;
            while index < data_slice.len(){
                req_cnt.fetch_add(1,Ordering::SeqCst);
                let func = AMS_EXECS.get(&(func_id)).unwrap()(&data_slice[index..]);
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
                                    let  req_data = NewReqData{
                                        src: team.world_pe ,
                                        dst: Some(msg.src as usize),
                                        cmd: ExecType::Am(Cmd::BatchedUnitReturn),
                                        id: batch_id, //for this case where every result is a unit return we only submit a single message and the ids are generated automatically.
                                        batch_id: Some(batch_id),
                                        func:  LamellarFunc::None,
                                        lamellae: lamellae,
                                        world: world,
                                        team: team,
                                        team_hash: team_hash,
                                    }; 
                                    ame.process_msg_new(req_data, None).await;
                                }
                                else{
                                    
                                    join_all({
                                        let mut entry = results.lock();
                                        let mut msgs = vec![];
                                        for (req_id,lam_result) in entry.0.drain(){
                                            match lam_result{
                                                LamellarReturn::Unit =>{  
                                                    panic!{"should not be the case that unit returns are mixed with data/am returns for batched am"}
                                                }
                                                LamellarReturn::RemoteData(d) => {
                                                    let req_data = NewReqData{
                                                        src: team.world_pe,
                                                        dst: Some(msg.src as usize),
                                                        cmd: ExecType::Am(Cmd::BatchedDataReturn),
                                                        id: msg.req_id,
                                                        batch_id: Some(batch_id),
                                                        func:  LamellarFunc::Result(d),
                                                        lamellae: lamellae.clone(),
                                                        world: world.clone(),
                                                        team: team.clone(),
                                                        team_hash: team_hash,
                                                    };
                                                    msgs.push(ame.process_msg_new(req_data, None));
                                                },
                                                _ => {
                                                    panic!{"not handled yet"};
                                                }
                                            }
                                        }
                                        msgs
                                    }).await;
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

    // Arc<Mutex<HashMap<
    //                         usize, //batched req id
    //                         Mutex<HashSet<usize>>>>>, //actual ids
    fn process_batched_unit_return(&self, msg: Msg, ser_data: SerializedData,){

        let batch_id = msg.req_id;
        println!("processing returns {:?}",batch_id);
        let data_slice=ser_data.data_as_bytes();  
        let mut index=0;
        let serialized_size = crate::serialized_size(&Some(0usize));
        // println!("header_and_data_slice {:?}",ser_data.header_and_data_as_bytes());
        println!("data_slice {:?} {:}",data_slice,serialized_size);

        while index < data_slice.len(){
            let batch_id: Option<usize> = crate::deserialize(&data_slice[index..(index+serialized_size)]).unwrap();
            index+=serialized_size;
            if let Some(batch_id) = batch_id{
                let reqs = self.txed_ams.lock().remove(&batch_id);
                
                if let Some(reqs) = reqs{
                    let reqs = reqs.lock();
                    
                    for req in reqs.iter(){
                        println!("completed req {:?}",req);
                        ActiveMessageEngine::send_data_to_user_handle(*req,msg.src,InternalResult::Unit);
                    }
                }
                else{
                    panic!("batch id {:?} not found",batch_id);
                }
            }
            else{
                panic!("batch id should not be none");
            }    
            println!("index {:?}",index);        
        }
    }

    fn process_unit_return(&self, msg: Msg, ser_data: SerializedData,){

        // println!("processing returns {:?}",batch_id);
        let data_slice=ser_data.data_as_bytes();  
        let mut index=0;
        let serialized_size = crate::serialized_size(&0usize);
        println!("data_slice {:?}",data_slice);
        while index < data_slice.len(){
            let req_id: usize = crate::deserialize(&data_slice[index..(index+serialized_size)]).unwrap();
            println!("completed req {:?}",req_id);
            ActiveMessageEngine::send_data_to_user_handle(req_id,msg.src,InternalResult::Unit);
            index+=serialized_size;
        }
    }

    fn process_data_return(&self, msg: Msg, ser_data: SerializedData){
        // println!("processing returns {:?}",batch_id);
        let data_slice=ser_data.data_as_bytes();  
        let mut index=0;
        let serialized_size = crate::serialized_size(&(0usize,0usize));
        // println!("data_slice {:?}",data_slice);
        while index < data_slice.len(){
            let (req_id,data_size): (usize,usize) = crate::deserialize(&data_slice[index..(index+serialized_size)]).unwrap();
            index+=serialized_size;
            let sub_data = ser_data.sub_data(index,index+data_size);
            ActiveMessageEngine::send_data_to_user_handle(req_id,msg.src,InternalResult::Remote(sub_data));
            index+=data_size;
            
        }
    }

    fn process_batched_data_return(&self, msg: Msg, ser_data: SerializedData,){

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
                let reqs = reqs.lock();
                // println!("reqs {:?}", &reqs[0..10]);
                for req in reqs.iter(){
                    ActiveMessageEngine::send_data_to_user_handle(*req,msg.src,InternalResult::Unit);
                }
            }
            else{
                panic!("batch id {:?} not found",batch_id);
            }
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
            Cmd::DataReturn => self.process_data_return(msg,ser_data),
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

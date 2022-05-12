use crate::active_messaging::*;
use crate::lamellae::comm::AllocError;
use crate::lamellae::{Des, Lamellae, LamellaeAM, Ser, SerializeHeader, SerializedData, SubData};
use crate::lamellar_team::LamellarTeamRT;

use crate::scheduler::{AmeSchedulerQueue, ReqData};
use async_recursion::async_recursion;
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use log::trace;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;

// enum BatchAmId{
const UNIT_ID: AmId = 0;
const BATCHED_UNIT_NEW_ID: AmId = UNIT_ID + 1;
const BATCHED_UNIT_ID: AmId = BATCHED_UNIT_NEW_ID + 1;
const REMOTE_DATA_ID: AmId = BATCHED_UNIT_ID + 1;
const BATCHED_REMOTE_DATA_ID: AmId = REMOTE_DATA_ID + 1;
const REMOTE_AM_ID: AmId = BATCHED_REMOTE_DATA_ID + 1; //when returning an am as a result we pass the negative of its actual id
const AM_ID_START: AmId = REMOTE_AM_ID + 1;

pub(crate) type UnpackFn = fn(&[u8], Result<usize, IdError>) -> LamellarArcAm;
pub(crate) type AmId = i32;
lazy_static! {
    pub(crate) static ref AMS_IDS: HashMap<String, AmId> = {
        let mut ams = vec![];
        for am in crate::inventory::iter::<RegisteredAm> {
            ams.push(am.name.clone());
        }
        ams.sort();
        let mut cnt = AM_ID_START;
        let mut temp = HashMap::new();
        let mut duplicates = vec![];
        for am in ams {
            if !temp.contains_key(&am) {
                temp.insert(am.clone(), cnt);
                cnt += 1;
            } else {
                duplicates.push(am);
            }
        }
        if duplicates.len() > 0 {
            panic!(
                "duplicate registered active message {:?}, AMs must have unique names",
                duplicates
            );
        }
        temp
    };
}
lazy_static! {
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

pub(crate) struct RegisteredActiveMessages {
    submitted_ams: Arc<
        Mutex<
            HashMap<
                u64, //team hash
                // (
                HashMap<
                    Option<usize>, //pe
                    (
                        HashMap<
                            AmId, //func id
                            (
                                HashMap<
                                    Option<ReqId>, //batch_id
                                    (
                                        Vec<(ReqId, LamellarFunc)>, //req_id, am
                                        usize,                      //batch size
                                    ),
                                >,
                                usize, //func size
                            ),
                        >,
                        Arc<AtomicUsize>, //pe size (total buf size)
                    ),
                >,
                // usize,//total size
                // )
            >,
        >,
    >, //pe, team hash, function id, batch id : (req_id,function), total data size // maybe we can remove the func id lookup if we enfore that a batch only contains the same function.. which I think we do...
    txed_ams: Arc<
        Mutex<
            HashMap<
                ReqId, //batched req id
                Mutex<HashMap<ReqId, (ReqId, AtomicUsize)>>,
            >,
        >,
    >, //actual ids, outstanding reqs
    cur_batch_id: Arc<AtomicUsize>,
    scheduler: Arc<AmeScheduler>,
    stall_mark: Arc<AtomicUsize>,
}

// type TeamHeader = (usize,u64); //size, hash
type FuncHeader = (usize, AmId); //size, ID
type BatchHeader = (usize, usize, ReqId); //size,num_reqs, id

impl RegisteredActiveMessages {
    pub(crate) fn new(
        scheduler: Arc<AmeScheduler>,
        stall_mark: Arc<AtomicUsize>,
    ) -> RegisteredActiveMessages {
        RegisteredActiveMessages {
            submitted_ams: Arc::new(Mutex::new(HashMap::new())),
            txed_ams: Arc::new(Mutex::new(HashMap::new())),
            cur_batch_id: Arc::new(AtomicUsize::new(1)),
            scheduler: scheduler,
            stall_mark: stall_mark,
        }
    }

    // right now we batch by (destination pe, team, func) triplets
    // need to analyze the benefits of different levels of batching (e.g. just batch based on pe)
    fn add_req_to_batch(
        &self,
        func: LamellarFunc,
        func_size: usize,
        func_id: AmId,
        req_data: Arc<ReqData>,
        world: Arc<LamellarTeam>,
    ) {
        // println!("adding req {:?} {:?}",func_id,func_size);

        let func_header_len = crate::serialized_size::<FuncHeader>(&(func_size,func_id),false);
        // println!("func header len {} {}", func_header_len,crate::serialized_size::<FuncHeader>(&(500, 500),false));
        let batch_header_len = crate::serialized_size::<BatchHeader>(&(0, 0, ReqId{id: 0, sub_id: 0}),false);
        // println!("func header len {} batch header len {}", func_header_len, batch_header_len);

        // add request to a batch or create a new one
        let mut submit_tx_task = false;
        let mut map = self.submitted_ams.lock();

        let team_entry = map
            .entry(req_data.team_hash)
            .or_insert_with(|| HashMap::new());
        // println!("team: {:?} {:?}",req_data.team_hash, team_entry.len());

        let pe_entry = team_entry.entry(req_data.dst).or_insert_with(|| {
            // println!("going to submit tx task {:?}",func_id);
            submit_tx_task = true;
            (HashMap::new(), Arc::new(AtomicUsize::new(0)))
        });
        pe_entry.1.fetch_add(func_size, Ordering::Relaxed);
        let total_batch_size = pe_entry.1.clone();
        // println!("pe: {:?} {:?} size: {:?}",req_data.dst, pe_entry.0.len(),pe_entry.1);

        let mut func_entry = pe_entry.0.entry(func_id).or_insert_with(|| {
            // println!("going to submit new func {:?}",func_id);
            (HashMap::new(), 0)
        }); //func
        if func_entry.1 == 0 {
            pe_entry.1.fetch_add(func_header_len, Ordering::Relaxed);
        }
        func_entry.1 += func_size;
        // println!("func: {:?} {:?} size: {:?} ({:?})",func_id, func_entry.0.len(),func_entry.1,pe_entry.1);

        let mut batch_entry = func_entry.0.entry(req_data.batch_id).or_insert_with(|| {
            // println!("going to submit new batch {:?}",req_data.batch_id);
            (Vec::new(), 0)
        }); // batch id
        if batch_entry.0.len() == 0 {
            //the funcsize can be zero so can't do check based on size
            pe_entry.1.fetch_add(batch_header_len, Ordering::Relaxed);
            func_entry.1 += batch_header_len;
        }
        batch_entry.1 += func_size;
        batch_entry.0.push((req_data.id, func));
        // println!("batch: {:?} {:?} size: {:?} ({:?}, {:?})",req_data.batch_id, batch_entry.0.len(),batch_entry.1,func_entry.1,pe_entry.1);

        drop(map);
        //--------------------------
        let mut stall_mark = self.stall_mark.load(Ordering::Relaxed);
        let stall_mark_clone = self.stall_mark.clone();
        // let world_clone = world.clone();
        if submit_tx_task {
            let submitted_ams = self.submitted_ams.clone();
            let txed_ams = self.txed_ams.clone();
            let outgoing_batch_id = self.cur_batch_id.clone(); //fetch_add(1, Ordering::Relaxed);
                                                               // println!{"submitting tx_task {:?} {:?}",outgoing_batch_id,req_data.cmd};
            self.scheduler.submit_task(async move {
                while stall_mark != stall_mark_clone.load(Ordering::Relaxed)
                    && total_batch_size.load(Ordering::Relaxed) < 1000000
                {
                    stall_mark = stall_mark_clone.load(Ordering::Relaxed);
                    async_std::task::yield_now().await;
                }
                let func_map = {
                    //all requests belonging to a team goin to a specific PE
                    let mut team_map = submitted_ams.lock();
                    if let Some(pe_map) = team_map.get_mut(&req_data.team_hash) {
                        pe_map.remove(&req_data.dst)
                    } else {
                        None
                    }
                };

                // println!{"in submit_tx_task {:?} {:?}",outgoing_batch_id,req_data.cmd};
                if let Some((func_map, total_size)) = func_map {
                    let total_size = total_size.load(Ordering::SeqCst);
                    // println!("func_map len {:?} total size {:?}",func_map.len(), total_size);
                    let msg = Msg {
                        cmd: ExecType::Am(Cmd::BatchedMsg),
                        src: req_data.team.world_pe as u16, //this should always originate from me?
                        req_id: ReqId{id: 0, sub_id: 0}, //outgoing_batch_id.fetch_add(1, Ordering::Relaxed),
                    };
                    let header = Some(SerializeHeader {
                        msg: msg,
                        team_hash: req_data.team_hash,
                        id: 0,
                    });
                    let mut data = req_data
                        .lamellae
                        .serialize_header(header.clone(), total_size);
                    while let Err(err) = data {
                        // println!("ram 222 need to alloc memory");
                        async_std::task::yield_now().await;
                        match err.downcast_ref::<AllocError>() {
                            Some(AllocError::OutOfMemoryError(_)) => {
                                world.alloc_new_pool(total_size * 2).await
                            }
                            _ => panic!("unhanlded error!! {:?}", err),
                        }
                        data = req_data
                            .lamellae
                            .serialize_header(header.clone(), total_size);
                        // println!("ram 230 data: {:?}",data.is_ok());
                    }
                    let data = data.unwrap();
                    let data_slice = data.data_as_bytes();
                    let mut i = 0;
                    // let mut num_reqs = 0;
                    for (func_id, (batch_map, func_size)) in func_map {
                        let func_header: FuncHeader = (func_size, func_id);
                        // println!("func_header: {:?} bml: {:?} {:?} {:?}",func_header, batch_map.len(),crate::serialized_size(&func_header),func_header_len);
                        crate::serialize_into(
                            &mut data_slice[i..i + func_header_len],
                            &func_header,
                            false,
                        )
                        .unwrap();
                        i += func_header_len;
                        // println!("i: {:?}",i);
                        for (batch_id, (reqs, batch_size)) in batch_map {
                            // println!("bid: {:?} reqs {:?} bsize {:?}",batch_id, reqs.len(), batch_size);
                            let batch_id = if let Some(batch_id) = batch_id {
                                //batch exists
                                batch_id
                            } else {
                                if func_id > 0 {
                                    //create new batch id
                                    let id = outgoing_batch_id.fetch_add(1, Ordering::Relaxed);
                                    ReqId{id: id, sub_id: 0}
                                } else {
                                    //original message not part of a batch
                                    ReqId{id: 0, sub_id: 0}
                                }
                            };
                            let batch_header: BatchHeader = (batch_size, reqs.len(), batch_id);
                            // println!("batch_header: {:?}",batch_header);
                            crate::serialize_into(
                                &mut data_slice[i..i + batch_header_len],
                                &batch_header,
                                false,
                            )
                            .unwrap();
                            i += batch_header_len;
                            // println!("i: {:?}",i);

                            let mut req_ids: HashMap<ReqId, (ReqId, AtomicUsize)> = HashMap::new();
                            let mut batch_req_id = ReqId{id: 0, sub_id: 0 };
                            // println!("dst: {:?} func_id {:?} batch_id {:?} num_reqs {:?}",req_data.dst,func_id,batch_id,reqs.len());
                            // num_reqs += reqs.len();
                            for (req_id, func) in reqs {
                                // println!("dst: {:?} req_id {:?} func_id {:?} batch_id {:?}",req_data.dst,req_id,func_id,batch_id);
                                match func_id {
                                    BATCHED_UNIT_ID => {} //dont have to do anything special, as all we need to know is batch id
                                    BATCHED_UNIT_NEW_ID => {
                                        let serialize_size = crate::serialized_size(&req_id, false,);
                                        crate::serialize_into(
                                            &mut data_slice[i..i + serialize_size],
                                            &req_id,
                                            false,
                                        )
                                        .unwrap();
                                        i += serialize_size;
                                    }
                                    UNIT_ID => {
                                        let serialize_size = crate::serialized_size(&req_id, false,);
                                        crate::serialize_into(
                                            &mut data_slice[i..i + serialize_size],
                                            &req_id,
                                            false,
                                        )
                                        .unwrap();
                                        i += serialize_size;
                                    }
                                    REMOTE_DATA_ID => {
                                        if let LamellarFunc::Result(func) = func {
                                            let result_header_size =
                                                crate::serialized_size(&(&req_id, 0usize), false,);
                                            let serialize_size = func.serialized_size();
                                            crate::serialize_into(
                                                &mut data_slice[i..i + result_header_size],
                                                &(req_id, serialize_size),
                                                false,
                                            )
                                            .unwrap();
                                            i += result_header_size;
                                            func.serialize_into(
                                                &mut data_slice[i..(i + serialize_size)],
                                            );
                                            // println!("req_id {:?} serialize_size {:?} {:?}", req_id, serialize_size, &data_slice[i..(i + serialize_size)]);
                                            // if req_data.dst.is_none() {
                                            //     func.ser(req_data.team.num_pes()-1);
                                            // }
                                            // else{
                                            //     func.ser(1);
                                            // };
                                            // ids.push(*req_id);
                                            i += serialize_size;
                                        }
                                    }
                                    BATCHED_REMOTE_DATA_ID => {
                                        if let LamellarFunc::Result(func) = func {
                                            let result_header_size =
                                                crate::serialized_size(&(&req_id, 0usize), false,);
                                            let serialize_size = func.serialized_size();
                                            crate::serialize_into(
                                                &mut data_slice[i..(i + result_header_size)],
                                                &(req_id, serialize_size),
                                                false,
                                            )
                                            .unwrap();
                                            i += result_header_size;
                                            func.serialize_into(
                                                &mut data_slice[i..(i + serialize_size)],
                                            );
                                            // println!("req_id {:?} serialize_size {:?} {:?}", req_id, serialize_size, &data_slice[i..(i + serialize_size)]);
                                            // if req_data.dst.is_none() {
                                            //     func.ser(req_data.team.num_pes()-1);
                                            // }
                                            // else{
                                            //     func.ser(1);
                                            // };
                                            // ids.push(*req_id);
                                            i += serialize_size;
                                        }
                                    }
                                    _ => {
                                        match func {
                                            LamellarFunc::Am(func) => {
                                                if func_id > 0 {
                                                    let num_reqs = if req_data.dst.is_none() {
                                                        match req_data.team.team_pe_id() {
                                                            Ok(_) => req_data.team.num_pes() - 1,
                                                            Err(_) => req_data.team.num_pes(),
                                                        }
                                                    } else {
                                                        1
                                                    };
                                                    req_ids.insert(
                                                        batch_req_id,
                                                        (req_id, AtomicUsize::new(num_reqs)),
                                                    );

                                                    batch_req_id.id += 1;
                                                } else {
                                                    let serialized_size =
                                                        crate::serialized_size(&req_id, false,);
                                                    // println!("serialized_size {:?}", serialized_size);
                                                    crate::serialize_into(
                                                        &mut data_slice[i..(i + serialized_size)],
                                                        &req_id,
                                                        false,
                                                    )
                                                    .unwrap();
                                                    i += serialized_size;
                                                }
                                                let serialize_size = func.serialized_size();
                                                if req_data.dst.is_none() {
                                                    func.ser(
                                                        match req_data.team.team_pe_id() {
                                                            Ok(_) => req_data.team.num_pes() - 1,
                                                            Err(_) => req_data.team.num_pes(),
                                                        },
                                                        req_data.team.team_pe,
                                                    );
                                                } else {
                                                    func.ser(1, req_data.team.team_pe);
                                                };
                                                func.serialize_into(
                                                    &mut data_slice[i..(i + serialize_size)],
                                                );
                                                // println!("req_id {:?} serialize_size {:?} {:?}", batch_req_id-1, serialize_size, &data_slice[i..(i + serialize_size)]);
                                                i += serialize_size;
                                            }

                                            LamellarFunc::None => {
                                                panic!("should not be none"); //user registered function
                                            }
                                            _ => {
                                                panic!("not handled yet");
                                            }
                                        }
                                    }
                                }
                                // println!("i: {:?}",i);
                            }
                            if req_ids.len() > 0 {
                                //only when we are sending initial requests, not return requests
                                // println!("inserting batch_id {:?} {:?}",batch_id,req_ids);
                                txed_ams.lock().insert(batch_id, Mutex::new(req_ids));
                            }
                        }
                    }
                    // println!("sending to {:?} batch {:?} num_reqs {:?}",req_data.dst,data.header_and_data_as_bytes().len(),num_reqs);
                    req_data
                        .lamellae
                        .send_to_pes_async(req_data.dst, req_data.team.arch.clone(), data)
                        .await;
                }
                // println!("leaving tx task");
            });
        }
    }

    async fn send_req(
        &self,
        func: LamellarFunc,
        func_size: usize,
        func_id: AmId,
        req_data: Arc<ReqData>,
        world: Arc<LamellarTeam>,
    ) {
        let batch_id = if let Some(batch_id) = &req_data.batch_id {
            *batch_id
        } else {
            req_data.id
        };
        let msg = Msg {
            cmd: req_data.cmd,
            src: req_data.team.world_pe as u16, //this should always originate from me?
            req_id: batch_id,
        };
        // println!("sending req {:?} {:?} {:?}",req_data.id,func_id,func_size);
        let header = Some(SerializeHeader {
            msg: msg,
            team_hash: req_data.team_hash,
            id: func_id,
        });
        let mut data = req_data
            .lamellae
            .serialize_header(header.clone(), func_size);
        while let Err(err) = data {
            async_std::task::yield_now().await;
            // println!("ram 446 need to alloc memory");
            match err.downcast_ref::<AllocError>() {
                Some(AllocError::OutOfMemoryError(_)) => world.alloc_new_pool(func_size * 2).await,
                _ => panic!("unhanlded error!! {:?}", err),
            }
            data = req_data
                .lamellae
                .serialize_header(header.clone(), func_size);
            // println!("ram 454 data: {:?}",data.is_ok());
        }
        let data = data.unwrap();
        let data_slice = data.data_as_bytes();
        match func {
            LamellarFunc::Am(func) => {
                if req_data.dst.is_none() {
                    func.ser(
                        match req_data.team.team_pe_id() {
                            Ok(_) => req_data.team.num_pes() - 1,
                            Err(_) => req_data.team.num_pes(),
                        },
                        req_data.team.team_pe,
                    );
                } else {
                    func.ser(1, req_data.team.team_pe);
                };
                let mut i = 0;
                if func_id < 0 {
                    let serialized_size = crate::serialized_size(&req_data.id, false,);
                    crate::serialize_into(&mut data_slice[i..i + serialized_size], &req_data.id, false,)
                        .unwrap();
                    i += serialized_size;
                }
                func.serialize_into(&mut data_slice[i..])
            }
            LamellarFunc::Result(func) => {
                // if req_data.dst.is_none() {
                //     func.ser(req_data.team.num_pes()-1);
                // }
                // else{
                //     func.ser(1);
                // };
                let mut i = 0;
                let serialized_size = crate::serialized_size(&(req_data.id, 0usize), false,);
                crate::serialize_into(
                    &mut data_slice[i..i + serialized_size],
                    &(req_data.id, func_size - serialized_size),
                    false,
                )
                .unwrap();
                i += serialized_size;
                func.serialize_into(&mut data_slice[i..])
            }
            LamellarFunc::LocalAm(_) => panic!("should not send a local am"),
            LamellarFunc::None => panic!("should not send none"),
        }
        // println!("sending single message {:?} {:?} {:?}",req_data.dst,req_data.team.arch.clone(),data.header_and_data_as_bytes().len());
        req_data
            .lamellae
            .send_to_pes_async(req_data.dst, req_data.team.arch.clone(), data)
            .await;
    }

    pub(crate) async fn process_am_req(
        &self,
        mut req_data: ReqData,
        world: Arc<LamellarTeam>,
        team: Arc<LamellarTeam>,
    ) {
        let my_team_pe = if let Ok(my_pe) = req_data.team.arch.team_pe(req_data.src) {
            Some(my_pe)
        } else {
            //this happens when we initiate a request using a team im not a part of but have a handle to
            None
        };
        let my_world_pe = Some(req_data.src);

        // println!("my_team_pe = {:?} Req src {:?} dst {:?} cmd {:?} id {:?} batch_id {:?} team_hash {:?}",my_team_pe,req_data.src,req_data.dst,req_data.cmd,req_data.id,req_data.batch_id,req_data.team_hash);
        if req_data.dst == my_world_pe && my_team_pe != None || req_data.team.num_pes() == 1 {
            // println!("[{:?}] single local request ", my_team_pe);

            RegisteredActiveMessages::exec_local(Arc::new(req_data), world, team).await;
        } else {
            // println!("precessing req cmd {:?}",req_data.cmd);
            let (func_id, func_size, cmd) = match &req_data.func {
                LamellarFunc::Am(ref func) => {
                    let (func_id, func_size) = match &req_data.cmd {
                        ExecType::Am(Cmd::BatchedAmReturn) => (
                            -(*AMS_IDS.get(&func.get_id()).unwrap()),
                            func.serialized_size() + crate::serialized_size(&ReqId{id: 0, sub_id: 0}, false,)
                        ),
                        ExecType::Am(Cmd::AmReturn) => (
                            -(*AMS_IDS.get(&func.get_id()).unwrap()),
                            func.serialized_size() + crate::serialized_size(&ReqId{id: 0, sub_id: 0}, false,),
                        ),
                        _ => (
                            *(AMS_IDS.get(&func.get_id()).unwrap()),
                            func.serialized_size(),
                        ),
                    };
                    (func_id, func_size, ExecType::Am(Cmd::BatchedMsg))
                }
                LamellarFunc::Result(ref func) => {
                    let func_size =
                        func.serialized_size() + crate::serialized_size(&(&req_data.id, 0usize), false,);
                    match req_data.cmd {
                        ExecType::Am(Cmd::BatchedDataReturn) => (
                            BATCHED_REMOTE_DATA_ID,
                            func_size,
                            ExecType::Am(Cmd::BatchedDataReturn),
                        ),
                        ExecType::Am(Cmd::DataReturn) => {
                            (REMOTE_DATA_ID, func_size, ExecType::Am(Cmd::DataReturn))
                        }
                        _ => panic!("not handled yet"),
                    }
                }
                LamellarFunc::None => match req_data.cmd {
                    ExecType::Am(Cmd::UnitReturn) => (
                        UNIT_ID,
                        crate::serialized_size(&req_data.id, false,),
                        ExecType::Am(Cmd::UnitReturn),
                    ),
                    ExecType::Am(Cmd::BatchedUnitReturnNew) => (
                        BATCHED_UNIT_NEW_ID,
                        crate::serialized_size(&req_data.id, false,),
                        ExecType::Am(Cmd::BatchedUnitReturn),
                    ),
                    ExecType::Am(Cmd::BatchedUnitReturn) => {
                        (BATCHED_UNIT_ID, 0, ExecType::Am(Cmd::BatchedUnitReturn))
                    }
                    _ => panic!("not handled yet"),
                },
                LamellarFunc::LocalAm(_) => {
                    println!("my_team_pe = {:?} Req src {:?} dst {:?} cmd {:?} id {:?} batch_id {:?} team_hash {:?}",my_team_pe,req_data.src,req_data.dst,req_data.cmd,req_data.id,req_data.batch_id,req_data.team_hash);
                    panic!("should only process remote AMS")
                }
            };
            trace!("[{:?}] remote request ", my_team_pe);
            // println!("func_id {:?} func_size {:?} cmd {:?}",func_id,func_size,cmd);

            let req_data = if func_size <= 10000 {
                req_data.cmd = cmd;
                let req_data = Arc::new(req_data);
                self.add_req_to_batch(
                    req_data.func.clone(),
                    func_size,
                    func_id,
                    req_data.clone(),
                    world.clone(),
                ); //,Cmd::BatchedMsg);
                req_data
            } else {
                let req_data = Arc::new(req_data);
                self.send_req(
                    req_data.func.clone(),
                    func_size,
                    func_id,
                    req_data.clone(),
                    world.clone(),
                )
                .await;

                req_data
            };

            if req_data.dst == None && my_team_pe != None {
                self.scheduler.submit_task(async move {
                    RegisteredActiveMessages::exec_local(req_data, world, team).await;
                });
            }
        }
    }

    #[async_recursion]
    async fn exec_local(req_data: Arc<ReqData>, world: Arc<LamellarTeam>, team: Arc<LamellarTeam>) {
        match req_data.func.clone() {
            LamellarFunc::Am(func) => {
                // println!("execing remote am locally");
                match func
                    .exec(
                        req_data.team.world_pe,
                        req_data.team.num_world_pes,
                        true,
                        world.clone(),
                        team.clone(),
                    )
                    .await
                {
                    LamellarReturn::LocalData(data) => {
                        // println!("local am data return");
                        ActiveMessageEngine::send_data_to_user_handle(
                            req_data.id,
                            req_data.src as u16,
                            InternalResult::Local(data),
                            req_data.team.clone(),
                        );
                    }
                    LamellarReturn::LocalAm(am) => {
                        // println!("local am am return");
                        let req_data = Arc::new(req_data.copy_with_func(am));
                        RegisteredActiveMessages::exec_local(req_data, world, team).await;
                        // exec_return_am(ame, msg, am, ireq, world, team).await;
                    }
                    LamellarReturn::Unit => {
                        // println!("local am unit return");
                        ActiveMessageEngine::send_data_to_user_handle(
                            req_data.id,
                            req_data.src as u16,
                            InternalResult::Unit,
                            req_data.team.clone(),
                        );
                    }
                    LamellarReturn::RemoteData(_) => {
                        // println!("remote am data return");
                        panic!("should not be returning remote data from local am");
                    }
                    LamellarReturn::RemoteAm(_) => {
                        // println!("remote am am return");
                        panic!("should not be returning remote am from local am");
                    }
                }
            }
            LamellarFunc::LocalAm(func) => {
                // println!("execing local am");
                match func
                    .exec(
                        req_data.team.world_pe,
                        req_data.team.num_world_pes,
                        true,
                        world.clone(),
                        team.clone(),
                    )
                    .await
                {
                    LamellarReturn::LocalData(data) => {
                        // println!("local am data return");
                        ActiveMessageEngine::send_data_to_user_handle(
                            req_data.id,
                            req_data.src as u16,
                            InternalResult::Local(data),
                            req_data.team.clone(),
                        );
                    }
                    LamellarReturn::LocalAm(am) => {
                        // println!("local am am return");
                        let req_data = Arc::new(req_data.copy_with_func(am));
                        RegisteredActiveMessages::exec_local(req_data, world, team).await;
                        // exec_return_am(ame, msg, am, ireq, world, team).await;
                    }
                    LamellarReturn::Unit => {
                        // println!("local am unit return");
                        ActiveMessageEngine::send_data_to_user_handle(
                            req_data.id,
                            req_data.src as u16,
                            InternalResult::Unit,
                            req_data.team.clone(),
                        );
                    }
                    LamellarReturn::RemoteData(_) => {
                        // println!("remote am data return");
                        panic!("should not be returning remote data from local am");
                    }
                    LamellarReturn::RemoteAm(_) => {
                        // println!("remote am am return");
                        panic!("should not be returning remote am from local am");
                    }
                }
            }
            _ => panic!("should only exec local ams"),
        }
    }

    async fn exec_single_msg(
        &self,
        ame: Arc<ActiveMessageEngine>,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        world: Arc<LamellarTeam>,
        team: Arc<LamellarTeam>,
        return_am: bool,
    ) {
        if let Some(header) = ser_data.deserialize_header() {
            let func =
                AMS_EXECS.get(&(header.id)).unwrap()(ser_data.data_as_bytes(), team.team.team_pe);
            let lam_return = func
                .exec(
                    team.team.world_pe,
                    team.team.num_world_pes,
                    return_am,
                    world.clone(),
                    team.clone(),
                )
                .await;
            let team_hash = team.team.remote_ptr_addr as u64;
            match lam_return {
                LamellarReturn::Unit => {
                    let req_data = ReqData {
                        src: team.team.world_pe,
                        dst: Some(msg.src as usize), //Some(team.arch.team_pe(msg.src as usize).expect("invalid team member")),
                        cmd: ExecType::Am(Cmd::UnitReturn),
                        id: msg.req_id,
                        batch_id: None,
                        func: LamellarFunc::None,
                        lamellae: lamellae,
                        world: world.team.clone(),
                        team: team.team.clone(),
                        team_hash: team_hash,
                    };
                    ame.process_msg_new(req_data, None).await;
                }
                LamellarReturn::LocalData(_) | LamellarReturn::LocalAm(_) => {
                    panic!("Should not be returning local data from remote  am");
                }
                LamellarReturn::RemoteAm(func) => {
                    let req_data = ReqData {
                        src: team.team.world_pe,
                        dst: Some(msg.src as usize), //Some(team.arch.team_pe(msg.src as usize).expect("invalid team member")),
                        cmd: ExecType::Am(Cmd::AmReturn),
                        id: msg.req_id,
                        batch_id: None,
                        func: LamellarFunc::Am(func),
                        lamellae: lamellae,
                        world: world.team.clone(),
                        team: team.team.clone(),
                        team_hash: team_hash,
                    };
                    ame.process_msg_new(req_data, None).await;
                }
                LamellarReturn::RemoteData(d) => {
                    let req_data = ReqData {
                        src: team.team.world_pe,
                        dst: Some(msg.src as usize), //Some(team.arch.team_pe(msg.src as usize).expect("invalid team member")),
                        cmd: ExecType::Am(Cmd::DataReturn),
                        id: msg.req_id,
                        batch_id: None,
                        func: LamellarFunc::Result(d),
                        lamellae: lamellae,
                        world: world.team.clone(),
                        team: team.team.clone(),
                        team_hash: team_hash,
                    };
                    ame.process_msg_new(req_data, None).await;
                }
            }
        }
    }

    fn exec_batched_msg(
        &self,
        ame: Arc<ActiveMessageEngine>,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        world: Arc<LamellarTeam>,
        team: Arc<LamellarTeam>,
    ) {
        // println!("exec batched msg");
        if let Some(header) = ser_data.deserialize_header() {
            // trace!("exec batched message {:?}",header.id);
            let data_slice = ser_data.data_as_bytes();
            // let team_header_len = crate::serialized_size::<TeamHeader>(&(0,0));
            let func_header_len = crate::serialized_size::<FuncHeader>(&(0, 0), false,);
            let batch_header_len = crate::serialized_size::<BatchHeader>(&(0, 0, ReqId{id: 0, sub_id: 0}), false,);
            let mut i = 0;
            // println!("data slice len {:?}",data_slice.len());
            while i < data_slice.len() {
                let func_header: FuncHeader =
                    crate::deserialize(&data_slice[i..i + func_header_len], false,).unwrap();
                i += func_header_len;
                let func_start = i;
                let func_id = func_header.1;
                while i < func_start + func_header.0 {
                    let batch_header: BatchHeader =
                        crate::deserialize(&data_slice[i..i + batch_header_len], false,).unwrap();
                    // println!("src: {:?} fh {:?} bh {:?} ({:?} {:?})",msg.src,func_header,batch_header,i,func_start + func_header.0);

                    i += batch_header_len;
                    // let batch_start = i;
                    let batch_id = batch_header.2;
                    let num_reqs = batch_header.1;
                    // println!("num_reqs: {:?}",num_reqs);
                    let batched_data = &data_slice[i..i + batch_header.0];
                    // println!("batched_data {:?} {:?}",batched_data.len(),&batched_data);
                    let sub_data = ser_data.sub_data(i, i + batch_header.0);
                    i += batch_header.0;
                    match func_id {
                        BATCHED_UNIT_NEW_ID => self.process_batched_unit_return_new(
                            batch_id,
                            msg.src,
                            batched_data,
                            team.team.clone(),
                        ),
                        BATCHED_UNIT_ID => {
                            self.process_batched_unit_return(batch_id, msg.src, team.team.clone())
                        }
                        UNIT_ID => {
                            self.process_unit_return(msg.src, batched_data, team.team.clone())
                        }
                        REMOTE_DATA_ID => self.process_data_return(msg, sub_data, team.clone()),
                        BATCHED_REMOTE_DATA_ID => self.process_batched_data_return(
                            batch_id,
                            msg.src,
                            sub_data,
                            team.clone(),
                        ),
                        REMOTE_AM_ID => panic! {"not handled yet {:?}",func_id},
                        _ => {
                            if func_id > 0 {
                                self.exec_batched_am(
                                    ame.clone(),
                                    msg.src as usize,
                                    header.team_hash,
                                    func_id,
                                    batch_id,
                                    num_reqs,
                                    lamellae.clone(),
                                    world.clone(),
                                    team.clone(),
                                    batched_data,
                                );
                            } else {
                                self.exec_batched_return_am(
                                    msg.src as usize,
                                    header.team_hash,
                                    -func_id,
                                    batch_id,
                                    num_reqs,
                                    lamellae.clone(),
                                    world.clone(),
                                    team.clone(),
                                    batched_data,
                                )
                            }
                        }
                    }
                }
            }
        }
    }

    fn exec_batched_am(
        &self,
        ame: Arc<ActiveMessageEngine>,
        src: usize,
        _team_hash: u64,
        func_id: AmId,
        batch_id: ReqId,
        num_reqs: usize,
        lamellae: Arc<Lamellae>,
        world: Arc<LamellarTeam>,
        team: Arc<LamellarTeam>,
        data_slice: &[u8],
    ) {
        // println!("execing batch_id {:?} {:?}",batch_id,data_slice.len());
        let mut index = 0;
        let mut req_id = ReqId{id: 0, sub_id: 0};
        let team_hash = team.team.remote_ptr_addr as u64;
        while req_id.id < num_reqs {
            // println!("req_id {:?} {:?}",req_id, &data_slice[index..(index+10)]);
            let func = AMS_EXECS.get(&(func_id)).unwrap()(&data_slice[index..], team.team.team_pe);
            // println!("func bytes {:?} {:?}",func.serialized_size(), &data_slice[index..(index+func.serialized_size())]);
            index += func.serialized_size();
            let world = world.clone();
            let team = team.clone();
            let lamellae = lamellae.clone();
            let ame = ame.clone();
            self.scheduler.submit_task(async move {
                // println!("in exec batch from src {:?}",src);
                // team.print_arch();
                let lam_return = func
                    .exec(
                        team.team.world_pe,
                        team.team.num_world_pes,
                        false,
                        world.clone(),
                        team.clone(),
                    )
                    .await;
                let req_data = match lam_return {
                    LamellarReturn::Unit => {
                        ReqData {
                            src: team.team.world_pe,
                            dst: Some(src), //Some(team.arch.team_pe(src).expect("invalid team member")),
                            cmd: ExecType::Am(Cmd::BatchedUnitReturnNew),
                            id: req_id,
                            batch_id: Some(batch_id),
                            func: LamellarFunc::None,
                            lamellae: lamellae.clone(),
                            world: world.team.clone(),
                            team: team.team.clone(),
                            team_hash: team_hash,
                        }
                    }
                    LamellarReturn::RemoteData(d) => {
                        ReqData {
                            src: team.team.world_pe,
                            dst: Some(src), //Some(team.arch.team_pe(src).expect("invalid team member")),
                            cmd: ExecType::Am(Cmd::BatchedDataReturn),
                            id: req_id,
                            batch_id: Some(batch_id),
                            func: LamellarFunc::Result(d),
                            lamellae: lamellae.clone(),
                            world: world.team.clone(),
                            team: team.team.clone(),
                            team_hash: team_hash,
                        }
                    }
                    LamellarReturn::RemoteAm(am) => {
                        // println!("returnin remote am");
                        ReqData {
                            src: team.team.world_pe,
                            dst: Some(src), //Some(team.arch.team_pe(src).expect("invalid team member")),
                            cmd: ExecType::Am(Cmd::BatchedAmReturn),
                            id: req_id,
                            batch_id: Some(batch_id),
                            func: LamellarFunc::Am(am),
                            lamellae: lamellae.clone(),
                            world: world.team.clone(),
                            team: team.team.clone(),
                            team_hash: team_hash,
                        }
                    }
                    _ => {
                        panic! {"not handled yet"};
                    }
                };
                ame.process_msg_new(req_data, None).await;
            });
            req_id.id += 1;
        }
    }

    fn process_am_return(
        &self,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        world: Arc<LamellarTeam>,
        team: Arc<LamellarTeam>,
    ) {
        if let Some(header) = ser_data.deserialize_header() {
            let data_slice = ser_data.data_as_bytes();
            self.exec_batched_return_am(
                msg.src as usize,
                header.team_hash,
                -header.id,
                ReqId{id: 0, sub_id: 0},
                1,
                lamellae,
                world,
                team.clone(),
                data_slice,
            );
        }
    }

    fn process_batched_am_return(
        &self,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        world: Arc<LamellarTeam>,
        team: Arc<LamellarTeam>,
    ) {
        if let Some(header) = ser_data.deserialize_header() {
            let data_slice = ser_data.data_as_bytes();
            self.exec_batched_return_am(
                msg.src as usize,
                header.team_hash,
                -header.id,
                msg.req_id,
                1,
                lamellae,
                world,
                team,
                data_slice,
            );
        }
    }

    fn exec_batched_return_am(
        &self,
        src: usize,
        team_hash: u64,
        func_id: AmId,
        batch_id: ReqId,
        num_reqs: usize,
        lamellae: Arc<Lamellae>,
        world: Arc<LamellarTeam>,
        team: Arc<LamellarTeam>,
        data_slice: &[u8],
    ) {
        // println!("execing return am batch_id {:?} func_id {:?} num_reqs {:?}",batch_id,func_id,num_reqs);
        let mut index = 0;
        let serialized_size = crate::serialized_size(&ReqId{id: 0, sub_id: 0 }, false,);
        // let team_hash: team.team.remote_ptr_addr as u64;
        let cnt = if let Some(reqs) = self.txed_ams.lock().get_mut(&batch_id) {
            let mut reqs = reqs.lock();
            // for (b_id,r_id) in reqs.iter(){
            //     println!("{:?} {:?}",b_id,r_id);
            // }
            let mut cur_req = 0;
            while cur_req < num_reqs {
                // while index < data_slice.len(){
                // println!("index {:?} len {:?}",index,data_slice.len());
                let batch_req_id: ReqId =
                    crate::deserialize(&data_slice[index..(index + serialized_size)], false,).unwrap();
                index += serialized_size;
                let func =
                    AMS_EXECS.get(&(func_id)).unwrap()(&data_slice[index..], team.team.team_pe);
                index += func.serialized_size();
                let (req_id, cnt) = reqs.get(&batch_req_id).expect("id not found");
                let req_id = *req_id;
                let lamellae = lamellae.clone();
                // let ame = ame.clone();
                let world_clone = world.clone();
                let team_clone = team.clone();
                self.scheduler.submit_task(async move {
                    let req_data = Arc::new(ReqData {
                        src: src,
                        dst: Some(src), //Some(team.team_pe.unwrap()),
                        cmd: ExecType::Am(Cmd::Exec),
                        id: req_id,
                        batch_id: Some(batch_id),
                        func: LamellarFunc::Am(func),
                        lamellae: lamellae,
                        world: world_clone.team.clone(),
                        team: team_clone.team.clone(),
                        team_hash: team_hash,
                    });
                    RegisteredActiveMessages::exec_local(req_data, world_clone, team_clone).await;
                });
                if cnt.fetch_sub(1, Ordering::Relaxed) == 1 {
                    reqs.remove(&batch_req_id);
                }
                cur_req += 1;
            }
            reqs.len()
        } else if batch_id.id == 0 {
            //not part a an original batch
            let mut cur_req = 0;
            while cur_req < num_reqs {
                // while index < data_slice.len(){
                // println!("index {:?} len {:?}",index,data_slice.len());
                let req_id: ReqId =
                    crate::deserialize(&data_slice[index..(index + serialized_size)], false,).unwrap();
                index += serialized_size;
                let func =
                    AMS_EXECS.get(&(func_id)).unwrap()(&data_slice[index..], team.team.team_pe);
                index += func.serialized_size();
                let lamellae = lamellae.clone();
                // let ame = ame.clone();
                let world = world.clone();
                let team = team.clone();
                self.scheduler.submit_task(async move {
                    let req_data = Arc::new(ReqData {
                        src: src,
                        dst: Some(src), //Some(team.team_pe.unwrap()),
                        cmd: ExecType::Am(Cmd::Exec),
                        id: req_id,
                        batch_id: Some(batch_id),
                        func: LamellarFunc::Am(func),
                        lamellae: lamellae,
                        world: world.team.clone(),
                        team: team.team.clone(),
                        team_hash: team_hash,
                    });
                    RegisteredActiveMessages::exec_local(req_data, world, team).await;
                });
                cur_req += 1;
            }
            1
        } else {
            println!("batch id {:?} doesnt exist", batch_id);
            1
        };
        if cnt == 0 {
            self.txed_ams.lock().remove(&batch_id);
        }
    }

    fn process_batched_unit_return_new(
        &self,
        batch_id: ReqId,
        src: u16,
        data_slice: &[u8],
        team: Pin<Arc<LamellarTeamRT>>,
    ) {
        let mut index = 0;
        // println!("data_slice {:?}",data_slice);
        let serialized_size = crate::serialized_size(&ReqId{id: 0, sub_id: 0 }, false,);
        let cnt = if let Some(reqs) = self.txed_ams.lock().get_mut(&batch_id) {
            let mut reqs = reqs.lock();
            // println!("batch_id: {:?} reqs {:?} ds {:?}",batch_id,reqs,data_slice);
            while index < data_slice.len() {
                let batch_req_id: ReqId =
                    crate::deserialize(&data_slice[index..(index + serialized_size)], false,).unwrap();
                index += serialized_size;
                let (req_id, cnt) = reqs.get(&batch_req_id).expect("id not found");
                // println!("batch_req_id  {:?} req_id {:?} ",batch_req_id,req_id);
                ActiveMessageEngine::send_data_to_user_handle(
                    *req_id,
                    src,
                    InternalResult::Unit,
                    team.clone(),
                );
                if cnt.fetch_sub(1, Ordering::Relaxed) == 1 {
                    reqs.remove(&batch_req_id);
                }
            }
            reqs.len()
        } else {
            0
        };
        if cnt == 0 {
            self.txed_ams.lock().remove(&batch_id);
        }
    }

    fn process_batched_unit_return(
        &self,
        batch_id: ReqId,
        src: u16,
        team: Pin<Arc<LamellarTeamRT>>,
    ) {
        // println!("processing returns {:?}",batch_id);
        let cnt = if let Some(reqs) = self.txed_ams.lock().get(&batch_id) {
            let reqs = reqs.lock();
            let mut max_cnt = 0;
            for (_, (req_id, cnt)) in reqs.iter() {
                // println!("completed req {:?}",req_id);
                ActiveMessageEngine::send_data_to_user_handle(
                    *req_id,
                    src,
                    InternalResult::Unit,
                    team.clone(),
                );
                let cnt = cnt.fetch_sub(1, Ordering::SeqCst);
                if cnt > max_cnt {
                    max_cnt = cnt;
                }
            }
            max_cnt
        } else {
            panic!("batch id {:?} not found, src: {:?}", batch_id, src);
        };
        if cnt == 0 {
            self.txed_ams.lock().remove(&batch_id);
        }
    }

    fn process_unit_return(&self, src: u16, data_slice: &[u8], team: Pin<Arc<LamellarTeamRT>>) {
        // println!("processing returns {:?}",batch_id);
        let mut index = 0;
        let serialized_size = crate::serialized_size(&ReqId{id: 0, sub_id: 0}, false,);
        // println!("data_slice {:?}",data_slice);
        while index < data_slice.len() {
            let req_id: ReqId =
                crate::deserialize(&data_slice[index..(index + serialized_size)], false,).unwrap();
            // println!("completed req {:?}",req_id);
            ActiveMessageEngine::send_data_to_user_handle(
                req_id,
                src,
                InternalResult::Unit,
                team.clone(),
            );
            index += serialized_size;
        }
    }

    fn process_data_return(&self, msg: Msg, ser_data: SerializedData, team: Arc<LamellarTeam>) {
        // println!("processing returns {:?}",batch_id);
        let data_slice = ser_data.data_as_bytes();
        let mut index = 0;
        let serialized_size = crate::serialized_size(&(ReqId{id: 0, sub_id: 0 }, 0usize), false,);
        // println!("data_slice {:?}",data_slice);
        while index < data_slice.len() {
            let (req_id, data_size): (ReqId, usize) =
                crate::deserialize(&data_slice[index..(index + serialized_size)], false,).unwrap();
            index += serialized_size;
            let sub_data = ser_data.sub_data(index, index + data_size);
            ActiveMessageEngine::send_data_to_user_handle(
                req_id,
                msg.src,
                InternalResult::Remote(sub_data),
                team.team.clone(),
            );
            index += data_size;
        }
    }

    fn process_batched_data_return(
        &self,
        batch_id: ReqId,
        src: u16,
        ser_data: SerializedData,
        team: Arc<LamellarTeam>,
    ) {
        let data_slice = ser_data.data_as_bytes();
        let mut index = 0;
        // println!("data_slice {:?}",data_slice);
        let serialized_size = crate::serialized_size(&(ReqId{id: 0, sub_id: 0 }, 0usize), false,);
        let cnt = if let Some(reqs) = self.txed_ams.lock().get_mut(&batch_id) {
            let mut reqs = reqs.lock();
            while index < data_slice.len() {
                // println!("index {:?} len {:?} ss {:?}",index,data_slice.len(),serialized_size);
                let (batch_req_id, data_size): (ReqId, usize) =
                    crate::deserialize(&data_slice[index..(index + serialized_size)], false,).unwrap();
                // println!("batch_req_id data_size {:?}  {:?}",batch_req_id,data_size);
                index += serialized_size;
                let sub_data = ser_data.sub_data(index, index + data_size);
                index += data_size;
                let (req_id, cnt) = reqs.get(&batch_req_id).expect("id not found");
                // println!("batch_req_id req_id data_size {:?} {:?} {:?}",batch_req_id,req_id,data_size);
                ActiveMessageEngine::send_data_to_user_handle(
                    *req_id,
                    src,
                    InternalResult::Remote(sub_data),
                    team.team.clone(),
                );
                if cnt.fetch_sub(1, Ordering::Relaxed) == 1 {
                    reqs.remove(&batch_req_id);
                }
            }
            reqs.len()
        } else {
            0
        };
        if cnt == 0 {
            self.txed_ams.lock().remove(&batch_id);
        }
    }

    pub(crate) async fn process_batched_am(
        &self, //process_am
        ame: Arc<ActiveMessageEngine>,
        cmd: Cmd,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        world: Arc<LamellarTeam>,
        team: Arc<LamellarTeam>,
    ) {
        // println!("recived msg!! {:?}",msg);
        match cmd {
            Cmd::BatchedMsg => self.exec_batched_msg(ame, msg, ser_data, lamellae, world, team),
            Cmd::Exec => {
                self.exec_single_msg(ame, msg, ser_data, lamellae, world, team, false)
                    .await
            }
            Cmd::BatchedDataReturn => {
                self.process_batched_data_return(msg.req_id, msg.src, ser_data, team)
            }
            Cmd::DataReturn => self.process_data_return(msg, ser_data, team),
            Cmd::BatchedAmReturn => {
                self.process_batched_am_return(msg, ser_data, lamellae, world, team)
            }
            Cmd::AmReturn => self.process_am_return(msg, ser_data, lamellae, world, team),

            _ => println!("unhandled cmd {:?}", msg.cmd),
        }
    }
}

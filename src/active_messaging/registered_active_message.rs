use crate::{
    active_messaging::{
        batching::{
            Batcher, BatcherType, StatCmd, StatType, BATCHER_AM_PE_RECV_CNTS,
            BATCHER_AM_PE_SEND_CNTS,
        },
        *,
    },
    config,
    lamellae::{
        Backend, Lamellae, SerializedData,comm::CommInfo,LamellaeUtil
    },
    utils::stats,
};

use async_recursion::async_recursion;
// use log::trace;
use std::sync::{Arc, OnceLock};


pub(crate) const AM_ID_START: AmId = 1;

pub(crate) type UnpackFn = fn(&[u8], Result<usize, IdError>) -> LamellarArcAm;
pub(crate) type AmId = i32;
lazy_static! {
    pub(crate) static ref AMS_IDS: HashMap<&'static str, AmId> = {
        let mut ams = vec![];
        for am in crate::inventory::iter::<RegisteredAm> {
            ams.push(am.name);
        }
        ams.sort();
        let mut cnt = AM_ID_START;
        let mut temp = HashMap::new();
        let mut duplicates = vec![];
        for am in ams {
            if !temp.contains_key(&am) {
                // println!("{:?}", am);
                temp.insert(am, cnt);
                cnt += 1;
            } else {
                duplicates.push(am);
            }
        }
        if !duplicates.is_empty() {
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

#[doc(hidden)]
pub struct RegisteredAm {
    pub exec: UnpackFn,
    pub name: &'static str,
}
crate::inventory::collect!(RegisteredAm);

#[derive(Debug, Clone)]
pub(crate) struct RegisteredActiveMessages {
    pub(crate) batcher: BatcherType,
    pub(crate) executor: Arc<Executor>,
}

pub(crate) static AM_HEADER_LEN: OnceLock<usize> = OnceLock::new();
lazy_static! {
    pub(crate) static ref DATA_HEADER_LEN: usize =
        crate::serialized_size::<DataHeader>(&DataHeader::default(), false);
    pub(crate) static ref UNIT_HEADER_LEN: usize =
        crate::serialized_size::<UnitHeader>(&UnitHeader::default(), false);
    pub(crate) static ref CMD_LEN: usize = crate::serialized_size::<Cmd>(&Cmd::Am, false);
}

#[repr(C)]
#[derive(serde::Serialize, serde::Deserialize, Debug, Copy, Clone)]
pub(crate) struct AmHeader {
    pub(crate) req_id: ReqId,
    pub(crate) team_addr: usize,
    pub(crate) am_id: AmId,
}


#[derive(serde::Serialize, serde::Deserialize, Default, Debug)]
pub(crate) struct DataHeader {
    pub(crate) size: usize,
    pub(crate) req_id: ReqId,
    pub(crate) darc_list_size: usize,
}

#[derive(serde::Serialize, serde::Deserialize, Default, Debug)]
pub(crate) struct UnitHeader {
    pub(crate) req_id: ReqId,
}

 #[lamellar_prof::prof]
#[async_trait]
impl ActiveMessageEngine for RegisteredActiveMessages {
    //#[tracing::instrument(skip_all, level = "debug")]
    async fn process_msg(self, am: Am, stall_mark: usize, immediate: bool) {
        trace!("[{:?}] process_msg {am:?}", std::thread::current().id());

        match am {
            Am::All(req_data, am) => {
                // println!("{:?}",am.get_id());
                let am_id = *(AMS_IDS.get(am.get_id()).unwrap());
                let am_size = am.serialized_size();

                if req_data.team.lamellae.comm().backend() != Backend::Local
                    && (req_data.team.num_pes() > 1 || req_data.team.team_pe_id().is_err())
                {
                    let ame = self.clone();
                    let req_data_clone = req_data.clone();
                    let am_clone = am.clone();
                    self.executor.submit_io_task(async move {
                        //spawn a task so that we can the execute the local am immediately
                        // println!(" {} {} {}, {}, {}",req_data.team.lamellae.comm().backend() != Backend::Local,req_data.team.num_pes() > 1, req_data.team.team_pe_id().is_err(),(req_data.team.num_pes() > 1 || req_data.team.team_pe_id().is_err()),req_data.team.lamellae.comm().backend() != Backend::Local && (req_data.team.num_pes() > 1 || req_data.team.team_pe_id().is_err()) );
                        if am_size < config().am_size_threshold && !immediate || req_data_clone.team.arch.team_iter().any(|pe| pe != req_data_clone.src && !req_data_clone.lamellae.available_to_send(pe)) {
                            ame.batcher
                                .add_remote_am_to_batch(
                                    req_data_clone.clone(),
                                    am_clone.clone(),
                                    am_id,
                                    am_size,
                                    stall_mark,
                                )
                                .await;
                        } else {
                            stats!(BATCHER_AM_PE_SEND_CNTS.0[&StatType::Orig].iter().for_each(
                                |(pe, c)| {
                                    if pe < &req_data_clone.team.arch.num_pes()
                                        && pe != &req_data_clone.src
                                    {
                                        c[&StatCmd::Am].fetch_add(1, Ordering::Relaxed);
                                        c[&StatCmd::Single].fetch_add(1, Ordering::Relaxed);
                                    }
                                },
                            ));
                            // println!(
                            //     "[{:?}] {:?} all {:?}",
                            //     std::thread::current().id(),
                            //     am_id,
                            //     am_size
                            // );
                            ame.send_am(req_data_clone, am_clone, am_id, am_size, Cmd::Am)
                                .await;
                        }
                    });
                }
                let world = LamellarTeam::new(None, req_data.world.clone(), true);
                let team = LamellarTeam::new(Some(world.clone()), req_data.team.clone(), true);
                if req_data.team.arch.team_pe(req_data.src).is_ok() {
                    self.exec_local_am(req_data, am.as_local(), world, team)
                        .await;
                }
            }
            Am::Remote(req_data, am) => {
                if req_data.dst == Some(req_data.src) {
                    let world = LamellarTeam::new(None, req_data.world.clone(), true);
                    let team = LamellarTeam::new(Some(world.clone()), req_data.team.clone(), true);
                    self.exec_local_am(req_data, am.as_local(), world, team)
                        .await;
                } else {
                    let am_id = *(AMS_IDS.get(&am.get_id()).unwrap());
                    let am_size = am.serialized_size();
                    if am_size < config().am_size_threshold && !immediate || !req_data.lamellae.available_to_send(req_data.dst.unwrap()) {
                        self.batcher
                            .add_remote_am_to_batch(req_data, am, am_id, am_size, stall_mark)
                            .await;
                    } else {
                        stats!(
                            BATCHER_AM_PE_SEND_CNTS.0[&StatType::Orig][&req_data.dst.unwrap()]
                                [&StatCmd::Single]
                                .fetch_add(1, Ordering::Relaxed)
                        );
                        stats!(
                            BATCHER_AM_PE_SEND_CNTS.0[&StatType::Orig][&req_data.dst.unwrap()]
                                [&StatCmd::Am]
                                .fetch_add(1, Ordering::Relaxed)
                        );
                        // println!(
                        //     "[{:?}] {:?} pe {:?}",
                        //     std::thread::current().id(),
                        //     am_id,
                        //     am_size
                        // );
                        self.send_am(req_data, am, am_id, am_size, Cmd::Am).await;
                    }
                }
            }
            Am::Local(req_data, am) => {
                let world = LamellarTeam::new(None, req_data.world.clone(), true);
                let team = LamellarTeam::new(Some(world.clone()), req_data.team.clone(), true);
                self.exec_local_am(req_data, am, world, team).await;
            }
            Am::Return(req_data, am) => {
                // println!("Am::Return");
                let am_id = *(AMS_IDS.get(&am.get_id()).unwrap());
                let am_size = am.serialized_size();
                if am_size < config().am_size_threshold && !immediate  || !req_data.lamellae.available_to_send(req_data.dst.unwrap()) {
                    self.batcher
                        .add_return_am_to_batch(req_data, am, am_id, am_size, stall_mark)
                        .await;
                } else {
                    stats!(
                        BATCHER_AM_PE_SEND_CNTS.0[&StatType::Remote][&req_data.dst.unwrap()]
                            [&StatCmd::Single]
                            .fetch_add(1, Ordering::Relaxed)
                    );
                    stats!(
                        BATCHER_AM_PE_SEND_CNTS.0[&StatType::Remote][&req_data.dst.unwrap()]
                            [&StatCmd::Return]
                            .fetch_add(1, Ordering::Relaxed)
                    );
                    // println!(
                    //     "[{:?}] {:?} return {:?}",
                    //     std::thread::current().id(),
                    //     am_id,
                    //     am_size
                    // );
                    self.send_am(req_data, am, am_id, am_size, Cmd::ReturnAm)
                        .await;
                }
            }
            Am::Data(req_data, data) => {
                // println!("Am::Data");
                let data_size = data.serialized_size();
                if data_size < config().am_size_threshold && !immediate || !req_data.lamellae.available_to_send(req_data.dst.unwrap()) {
                    self.batcher
                        .add_data_am_to_batch(req_data, data, data_size, stall_mark)
                        .await;
                } else {
                    stats!(
                        BATCHER_AM_PE_SEND_CNTS.0[&StatType::Remote][&req_data.dst.unwrap()]
                            [&StatCmd::Single]
                            .fetch_add(1, Ordering::Relaxed)
                    );
                    stats!(
                        BATCHER_AM_PE_SEND_CNTS.0[&StatType::Remote][&req_data.dst.unwrap()]
                            [&StatCmd::Data]
                            .fetch_add(1, Ordering::Relaxed)
                    );
                    // println!("[{:?}] data {:?}", std::thread::current().id(), data_size);
                    self.send_data_am(req_data, data, data_size).await;
                }
            }
            Am::Unit(req_data) => {
                if *UNIT_HEADER_LEN < config().am_size_threshold && !immediate || !req_data.lamellae.available_to_send(req_data.dst.unwrap()) {
                    self.batcher
                        .add_unit_am_to_batch(req_data, stall_mark)
                        .await;
                } else {
                    stats!(
                        BATCHER_AM_PE_SEND_CNTS.0[&StatType::Remote][&req_data.dst.unwrap()]
                            [&StatCmd::Single]
                            .fetch_add(1, Ordering::Relaxed)
                    );
                    stats!(
                        BATCHER_AM_PE_SEND_CNTS.0[&StatType::Remote][&req_data.dst.unwrap()]
                            [&StatCmd::Unit]
                            .fetch_add(1, Ordering::Relaxed)
                    );
                    // println!(
                    //     "[{:?}]  unit {:?}",
                    //     std::thread::current().id(),
                    //     *UNIT_HEADER_LEN
                    // );
                    self.send_unit_am(req_data).await;
                }
            }
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    async fn exec_msg(self, msg: Msg, ser_data: SerializedData, lamellae: &Arc<Lamellae>) {
        trace!("[{:?}] exec_msg {:?}", std::thread::current().id(), msg.cmd);
        let mut i = 0;

        match msg.cmd {
            Cmd::Am => {
                let data_bytes = ser_data.data_as_bytes();
                self.batcher.exec_am(msg.src as usize, &data_bytes, &mut i, lamellae, &self, &self.executor).await;
                stats!(
                    BATCHER_AM_PE_RECV_CNTS.0[&StatType::Remote][&(msg.src as usize)][&StatCmd::Am]
                        .fetch_add(1, Ordering::Relaxed)
                );
                stats!(
                    BATCHER_AM_PE_RECV_CNTS.0[&StatType::Remote][&(msg.src as usize)]
                        [&StatCmd::Single]
                        .fetch_add(1, Ordering::Relaxed)
                );
            }
            Cmd::ReturnAm => {
                let data_bytes = ser_data.data_as_bytes();
                self.batcher.exec_return_am(msg.src as usize, &data_bytes, &mut i, lamellae, &self).await;
                stats!(
                    BATCHER_AM_PE_RECV_CNTS.0[&StatType::Orig][&(msg.src as usize)]
                        [&StatCmd::Return]
                        .fetch_add(1, Ordering::Relaxed)
                );
                stats!(
                    BATCHER_AM_PE_RECV_CNTS.0[&StatType::Orig][&(msg.src as usize)]
                        [&StatCmd::Single]
                        .fetch_add(1, Ordering::Relaxed)
                );
            }
            Cmd::Data => {
                let data_bytes = ser_data.data_as_bytes();
                self.batcher.exec_data_am(msg.src as usize, &data_bytes, &mut i, &self);
                stats!(
                    BATCHER_AM_PE_RECV_CNTS.0[&StatType::Orig][&(msg.src as usize)][&StatCmd::Data]
                        .fetch_add(1, Ordering::Relaxed)
                );
                stats!(
                    BATCHER_AM_PE_RECV_CNTS.0[&StatType::Orig][&(msg.src as usize)]
                        [&StatCmd::Single]
                        .fetch_add(1, Ordering::Relaxed)
                );
            }
            Cmd::Unit => {
                let data_bytes = ser_data.data_as_bytes();
                self.batcher.exec_unit_am(msg.src as usize, &data_bytes, &mut i, &self);
                stats!(
                    BATCHER_AM_PE_RECV_CNTS.0[&StatType::Orig][&(msg.src as usize)][&StatCmd::Unit]
                        .fetch_add(1, Ordering::Relaxed)
                );
                stats!(
                    BATCHER_AM_PE_RECV_CNTS.0[&StatType::Orig][&(msg.src as usize)]
                        [&StatCmd::Single]
                        .fetch_add(1, Ordering::Relaxed)
                );
            }
            Cmd::BatchedMsg => {
                stats!(
                    BATCHER_AM_PE_RECV_CNTS.0[&StatType::Remote][&(msg.src as usize)]
                        [&StatCmd::Batched]
                        .fetch_add(1, Ordering::Relaxed)
                );
                self.batcher
                    .exec_batched_msg(msg, ser_data, lamellae, &self)
                    .await;
                stats!(
                    BATCHER_AM_PE_RECV_CNTS.0[&StatType::Orig][&(msg.src as usize)]
                        [&StatCmd::Batched]
                        .fetch_add(1, Ordering::Relaxed)
                );
            }
        }
        trace!(target: "lamellae_debug", "[{:?}] finished exec_msg {:?}, lamellae cnt: {:?}", std::thread::current().id(), msg.cmd, Arc::strong_count(lamellae));
    }
}

 #[lamellar_prof::prof]
impl RegisteredActiveMessages {
    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new(batcher: BatcherType, executor: Arc<Executor>) -> RegisteredActiveMessages {
        RegisteredActiveMessages { batcher, executor }
    }

    async fn send_am(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_size: usize,
        cmd: Cmd,
    ) {
        self.batcher.send_am(req_data, am, am_id, am_size, cmd).await;
    }

    async fn send_data_am(
        &self,
        req_data: ReqMetaData,
        data: LamellarResultArc,
        data_size: usize,
    ) {
        self.batcher.send_data_am(req_data, data, data_size).await;
    }

    async fn send_unit_am(&self, req_data: ReqMetaData) {
        self.batcher.send_unit_am(req_data).await;
    }

    //we can remove this by cloning self and submitting to the executor
    #[async_recursion]
    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) async fn exec_local_am(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcLocalAm,
        world: Arc<LamellarTeam>,
        team: Arc<LamellarTeam>,
    ) {
        trace!("[{:?}] exec_local_am", std::thread::current().id());
        world.team.world_counters.inc_outstanding(1);
        team.team.team_counters.inc_outstanding(1);
        match am
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
                // println!("[{:?}] local am data return", std::thread::current().id());
                self.send_data_to_user_handle(
                    req_data.id,
                    req_data.src,
                    InternalResult::Local(data),
                );
            }
            LamellarReturn::LocalAm(am) => {
                // println!("[{:?}] local am am return", std::thread::current().id());
                self.exec_local_am(req_data, am.as_local(), world.clone(), team.clone())
                    .await;
            }
            LamellarReturn::Unit => {
                // println!("[{:?}] local am unit return", std::thread::current().id());
                self.send_data_to_user_handle(req_data.id, req_data.src, InternalResult::Unit);
            }
            LamellarReturn::RemoteData(_) | LamellarReturn::RemoteAm(_) => {
                panic!("should not be returning remote data or am from local am");
            }
        }
        world.team.world_counters.dec_outstanding(1);
        team.team.team_counters.dec_outstanding(1);
    }

}

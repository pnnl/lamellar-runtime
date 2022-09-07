use crate::active_messaging::batching::{Batcher, BatcherType};
use crate::active_messaging::*;
use crate::lamellae::comm::AllocError;
use crate::lamellae::{
    Des, Lamellae, LamellaeAM, LamellaeRDMA, Ser, SerializeHeader, SerializedData, SubData, Backend,LamellaeComm
};

use crate::scheduler::SchedulerQueue;
use async_recursion::async_recursion;
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
// use log::trace;
use std::sync::Arc;

const AM_ID_START: AmId = 1;

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

pub struct RegisteredAm {
    pub exec: UnpackFn,
    pub name: String,
}
crate::inventory::collect!(RegisteredAm);

#[derive(Debug)]
pub(crate) struct RegisteredActiveMessages {
    batcher: BatcherType,
}

lazy_static! {
    pub(crate) static ref AM_HEADER_LEN: usize =
        crate::serialized_size::<AmHeader>(&AmHeader::default(), false);
    pub(crate) static ref DATA_HEADER_LEN: usize =
        crate::serialized_size::<DataHeader>(&DataHeader::default(), false);
    pub(crate) static ref UNIT_HEADER_LEN: usize =
        crate::serialized_size::<UnitHeader>(&UnitHeader::default(), false);
    pub(crate) static ref CMD_LEN: usize = crate::serialized_size::<Cmd>(&Cmd::Am, false);
}

#[derive(serde::Serialize, serde::Deserialize, Default, Debug)]
pub(crate) struct AmHeader {
    pub(crate) am_id: AmId,
    pub(crate) team_addr: usize,
    pub(crate) req_id: ReqId,
}

#[derive(serde::Serialize, serde::Deserialize, Default, Debug)]
pub(crate) struct DataHeader {
    pub(crate) size: usize,
    pub(crate) req_id: ReqId,
}

#[derive(serde::Serialize, serde::Deserialize, Default, Debug)]
pub(crate) struct UnitHeader {
    pub(crate) req_id: ReqId,
}

#[async_trait]
impl ActiveMessageEngine for RegisteredActiveMessages {
    #[tracing::instrument(skip_all)]
    async fn process_msg(
        &self,
        am: Am,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
        stall_mark: usize,
    ) {
        match am {
            Am::All(req_data, am) => {
                let am_id = *(AMS_IDS.get(&am.get_id()).unwrap());
                let am_size = am.serialized_size();
                if false && am_size < crate::active_messaging::BATCH_AM_SIZE {
                    self.batcher.add_remote_am_to_batch(
                        req_data.clone(),
                        am.clone(),
                        am_id,
                        am_size,
                        scheduler,
                        stall_mark,
                    );
                } else if req_data.team.lamellae.backend() != Backend::Local{
                    self.send_am(req_data.clone(), am.clone(), am_id, am_size, Cmd::Am)
                        .await;
                }
                let world = LamellarTeam::new(None, req_data.world.clone(), true);
                let team = LamellarTeam::new(Some(world.clone()), req_data.team.clone(), true);
                self.exec_local_am(req_data, am.as_local(), world, team)
                    .await;
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
                    if am_size < crate::active_messaging::BATCH_AM_SIZE {
                        self.batcher.add_remote_am_to_batch(
                            req_data, am, am_id, am_size, scheduler, stall_mark,
                        );
                    } else {
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
                let am_id = *(AMS_IDS.get(&am.get_id()).unwrap());
                let am_size = am.serialized_size();
                if am_size < crate::active_messaging::BATCH_AM_SIZE {
                    self.batcher.add_return_am_to_batch(
                        req_data, am, am_id, am_size, scheduler, stall_mark,
                    );
                } else {
                    self.send_am(req_data, am, am_id, am_size, Cmd::ReturnAm)
                        .await;
                }
            }
            Am::Data(req_data, data) => {
                let data_size = data.serialized_size();
                if data_size < crate::active_messaging::BATCH_AM_SIZE {
                    self.batcher
                        .add_data_am_to_batch(req_data, data, data_size, scheduler, stall_mark);
                } else {
                    self.send_data_am(req_data, data, data_size).await;
                }
            }
            Am::Unit(req_data) => {
                if *UNIT_HEADER_LEN < crate::active_messaging::BATCH_AM_SIZE {
                    self.batcher
                        .add_unit_am_to_batch(req_data, scheduler, stall_mark);
                } else {
                    self.send_unit_am(req_data).await;
                }
            }
            Am::BatchedReturn(_req_data, _func, _batch_id) => {
                // let func_id = *(AMS_IDS.get(&func.get_id()).unwrap());
                // let func_size = func.serialized_size();
                // if func_size <= crate::active_messaging::BATCH_AM_SIZE {
                //     self.batcher
                //         .add_batched_return_am_to_batch(
                //             req_data, func, func_id, func_size, batch_id, scheduler,stall_mark
                //         )
                //         .await;
                // } else {
                //     self.send_batched_return_am(
                //         req_data, func, func_id, func_size, batch_id, scheduler,
                //     )
                //     .await;
                // }
            }
            Am::BatchedData(_req_data, _data, _batch_id) => {
                // let data_size = data.serialized_size();
                // if data_size <= crate::active_messaging::BATCH_AM_SIZE {
                //     self.add_batched_data_am_to_batch(
                //         req_data, data, data_size, batch_id, scheduler,stall_mark
                //     )
                //     .await;
                // } else {
                //     self.send_batched_data_am(req_data, data, data_size, batch_id, scheduler)
                //         .await;
                // }
            }
            Am::BatchedUnit(_req_data, _batch_id) => {
                // self.add_batched_unit_am_to_batch(req_data, batch_id, scheduler,stall_mark)
                //     .await;
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn exec_msg(
        &self,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
    ) {
        let data = ser_data.data_as_bytes();
        let mut i = 0;
        match msg.cmd {
            Cmd::Am => {
                self.exec_am(&msg, data, &mut i, &lamellae, scheduler).await;
            }
            Cmd::ReturnAm => {
                self.exec_return_am(&msg, data, &mut i, &lamellae).await;
            }
            Cmd::Data => {
                self.exec_data_am(&msg, data, &mut i, &ser_data).await;
            }
            Cmd::Unit => {
                self.exec_unit_am(&msg, data, &mut i).await;
            }
            Cmd::BatchedMsg => {
                self.batcher
                    .exec_batched_msg(msg, ser_data, lamellae, scheduler, self)
                    .await;
            }
        }
    }
}

impl RegisteredActiveMessages {
    #[tracing::instrument(skip_all)]
    pub(crate) fn new(batcher: BatcherType) -> RegisteredActiveMessages {
        RegisteredActiveMessages { batcher: batcher }
    }

    #[tracing::instrument(skip_all)]
    async fn send_am(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_size: usize,
        cmd: Cmd,
    ) {
        // println!(
        //     "send_am {:?} {:?} {:?} ({:?})",
        //     am_id, am_size, cmd, *AM_HEADER_LEN
        // );
        let header = self.create_header(&req_data, cmd);
        let data_buf = self
            .create_data_buf(header, am_size + *AM_HEADER_LEN, &req_data.lamellae)
            .await;
        let data_slice = data_buf.data_as_bytes();

        let am_header = AmHeader {
            am_id: am_id,
            req_id: req_data.id,
            team_addr: req_data.team_addr,
        };

        crate::serialize_into(&mut data_slice[0..*AM_HEADER_LEN], &am_header, false).unwrap();

        let i = *AM_HEADER_LEN;

        let darc_ser_cnt = match req_data.dst {
            Some(_) => 1,
            None => {
                match req_data.team.team_pe_id() {
                    Ok(_) => req_data.team.num_pes() - 1, //we dont send an am to ourself here
                    Err(_) => req_data.team.num_pes(), //this means we have a handle to a team but are not in the team
                }
            }
        };
        am.ser(darc_ser_cnt);
        am.serialize_into(&mut data_slice[i..]);
        req_data
            .lamellae
            .send_to_pes_async(req_data.dst, req_data.team.arch.clone(), data_buf)
            .await;
    }

    #[tracing::instrument(skip_all)]
    async fn send_data_am(&self, req_data: ReqMetaData, data: LamellarResultArc, data_size: usize) {
        let header = self.create_header(&req_data, Cmd::Data);
        let data_buf = self
            .create_data_buf(header, data_size + *DATA_HEADER_LEN, &req_data.lamellae)
            .await;
        let data_slice = data_buf.data_as_bytes();

        let data_header = DataHeader {
            size: data_size,
            req_id: req_data.id,
        };
        crate::serialize_into(&mut data_slice[0..*DATA_HEADER_LEN], &data_header, false).unwrap();
        let i = *DATA_HEADER_LEN;

        data.serialize_into(&mut data_slice[i..]);
        req_data
            .lamellae
            .send_to_pes_async(req_data.dst, req_data.team.arch.clone(), data_buf)
            .await;
    }

    #[tracing::instrument(skip_all)]
    async fn send_unit_am(&self, req_data: ReqMetaData) {
        let header = self.create_header(&req_data, Cmd::Unit);
        let data_buf = self
            .create_data_buf(header, *UNIT_HEADER_LEN, &req_data.lamellae)
            .await;
        let data_slice = data_buf.data_as_bytes();

        let unit_header = UnitHeader {
            req_id: req_data.id,
        };
        crate::serialize_into(&mut data_slice[0..*UNIT_HEADER_LEN], &unit_header, false).unwrap();
        req_data
            .lamellae
            .send_to_pes_async(req_data.dst, req_data.team.arch.clone(), data_buf)
            .await;
    }

    #[tracing::instrument(skip_all)]
    fn create_header(&self, req_data: &ReqMetaData, cmd: Cmd) -> SerializeHeader {
        let msg = Msg {
            src: req_data.team.world_pe as u16,
            cmd: cmd,
        };
        SerializeHeader { msg: msg }
    }

    #[tracing::instrument(skip_all)]
    async fn create_data_buf(
        &self,
        header: SerializeHeader,
        size: usize,
        lamellae: &Arc<Lamellae>,
    ) -> SerializedData {
        let header = Some(header);
        let mut data = lamellae.serialize_header(header.clone(), size);
        while let Err(err) = data {
            async_std::task::yield_now().await;
            match err.downcast_ref::<AllocError>() {
                Some(AllocError::OutOfMemoryError(_)) => {
                    lamellae.alloc_pool(size * 2);
                }
                _ => panic!("unhanlded error!! {:?}", err),
            }
            data = lamellae.serialize_header(header.clone(), size);
        }
        data.unwrap()
    }

    #[async_recursion]
    #[tracing::instrument(skip_all)]
    pub(crate) async fn exec_local_am(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcLocalAm,
        world: Arc<LamellarTeam>,
        team: Arc<LamellarTeam>,
    ) {
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
                // println!("local am data return");
                self.send_data_to_user_handle(
                    req_data.id,
                    req_data.src,
                    InternalResult::Local(data),
                );
            }
            LamellarReturn::LocalAm(am) => {
                // println!("local am am return");
                self.exec_local_am(req_data, am.as_local(), world, team)
                    .await;
            }
            LamellarReturn::Unit => {
                // println!("local am unit return");
                self.send_data_to_user_handle(req_data.id, req_data.src, InternalResult::Unit);
            }
            LamellarReturn::RemoteData(_) | LamellarReturn::RemoteAm(_) => {
                panic!("should not be returning remote data or am from local am");
            }
        }
    }

    #[tracing::instrument(skip_all)]
    pub(crate) async fn exec_am(
        &self,
        msg: &Msg,
        data: &[u8],
        i: &mut usize,
        lamellae: &Arc<Lamellae>,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
    ) {
        let am_header: AmHeader =
            crate::deserialize(&data[*i..*i + *AM_HEADER_LEN], false).unwrap();
        let (team, world) =
            self.get_team_and_world(msg.src as usize, am_header.team_addr, &lamellae);
        *i += *AM_HEADER_LEN;

        let am = AMS_EXECS.get(&am_header.am_id).unwrap()(&data[*i..], team.team.team_pe);
        *i += am.serialized_size();

        let req_data = ReqMetaData {
            src: team.team.world_pe,
            dst: Some(msg.src as usize),
            id: am_header.req_id,
            lamellae: lamellae.clone(),
            world: world.team.clone(),
            team: team.team.clone(),
            team_addr: team.team.remote_ptr_addr,
        };

        let am = match am
            .exec(
                team.team.world_pe,
                team.team.num_world_pes,
                false,
                world.clone(),
                team.clone(),
            )
            .await
        {
            LamellarReturn::Unit => Am::Unit(req_data),
            LamellarReturn::RemoteData(data) => Am::Data(req_data, data),
            LamellarReturn::RemoteAm(am) => Am::Return(req_data, am),
            LamellarReturn::LocalData(_) | LamellarReturn::LocalAm(_) => {
                panic!("Should not be returning local data or AM from remote  am");
            }
        };
        self.process_msg(am, scheduler, 0).await; //0 just means we will force a stall_count loop
                                                  // scheduler.submit_am(am);
                                                  //TODO: compare against: scheduler.submit_am(ame, am).await;
    }

    #[tracing::instrument(skip_all)]
    pub(crate) async fn exec_return_am(
        &self,
        msg: &Msg,
        data: &[u8],
        i: &mut usize,
        lamellae: &Arc<Lamellae>,
    ) {
        let am_header: AmHeader =
            crate::deserialize(&data[*i..*i + *AM_HEADER_LEN], false).unwrap();
        let (team, world) =
            self.get_team_and_world(msg.src as usize, am_header.team_addr, &lamellae);
        *i += *AM_HEADER_LEN;
        let am = AMS_EXECS.get(&am_header.am_id).unwrap()(&data[*i..], team.team.team_pe);
        *i += am.serialized_size();

        let req_data = ReqMetaData {
            src: msg.src as usize,
            dst: Some(team.team.world_pe),
            id: am_header.req_id,
            lamellae: lamellae.clone(),
            world: world.team.clone(),
            team: team.team.clone(),
            team_addr: team.team.remote_ptr_addr,
        };
        self.exec_local_am(req_data, am.as_local(), world, team)
            .await;
    }

    #[tracing::instrument(skip_all)]
    pub(crate) async fn exec_data_am(
        &self,
        msg: &Msg,
        data_buf: &[u8],
        i: &mut usize,
        ser_data: &SerializedData,
    ) {
        let data_header: DataHeader =
            crate::deserialize(&data_buf[*i..*i + *DATA_HEADER_LEN], false).unwrap();
        *i += *DATA_HEADER_LEN;
        let data = ser_data.sub_data(*i, *i + data_header.size);
        *i += data_header.size;
        self.send_data_to_user_handle(
            data_header.req_id,
            msg.src as usize,
            InternalResult::Remote(data),
        );
    }

    #[tracing::instrument(skip_all)]
    pub(crate) async fn exec_unit_am(&self, msg: &Msg, data: &[u8], i: &mut usize) {
        let unit_header: UnitHeader =
            crate::deserialize(&data[*i..*i + *UNIT_HEADER_LEN], false).unwrap();
        *i += *UNIT_HEADER_LEN;
        self.send_data_to_user_handle(unit_header.req_id, msg.src as usize, InternalResult::Unit);
    }
}

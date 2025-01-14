use crate::{
    active_messaging::{
        batching::{Batcher, BatcherType},
        *,
    },
    config,
    lamellae::{
        comm::error::AllocError, Backend, CommMem, CommInfo,Lamellae, LamellaeAM, Ser, SerializeHeader, SerializedData
    },
};

use async_recursion::async_recursion;
// use log::trace;
use std::sync::Arc;

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

#[doc(hidden)]
pub struct RegisteredAm {
    pub exec: UnpackFn,
    pub name: &'static str,
}
crate::inventory::collect!(RegisteredAm);

#[derive(Debug, Clone)]
pub(crate) struct RegisteredActiveMessages {
    batcher: BatcherType,
    executor: Arc<Executor>,
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
    pub(crate) darc_list_size: usize,
}

#[derive(serde::Serialize, serde::Deserialize, Default, Debug)]
pub(crate) struct UnitHeader {
    pub(crate) req_id: ReqId,
}

#[async_trait]
impl ActiveMessageEngine for RegisteredActiveMessages {
    // #[tracing::instrument(skip_all)]
    async fn process_msg(self, am: Am, stall_mark: usize, immediate: bool) {
        // println!("[{:?}] process_msg {am:?}", std::thread::current().id());

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
                        if am_size < config().am_size_threshold && !immediate {
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
                    if am_size < config().am_size_threshold && !immediate {
                        self.batcher
                            .add_remote_am_to_batch(req_data, am, am_id, am_size, stall_mark)
                            .await;
                    } else {
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
                if am_size < config().am_size_threshold && !immediate {
                    self.batcher
                        .add_return_am_to_batch(req_data, am, am_id, am_size, stall_mark)
                        .await;
                } else {
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
                if data_size < config().am_size_threshold && !immediate {
                    self.batcher
                        .add_data_am_to_batch(req_data, data, data_size, stall_mark)
                        .await;
                } else {
                    // println!("[{:?}] data {:?}", std::thread::current().id(), data_size);
                    self.send_data_am(req_data, data, data_size).await;
                }
            }
            Am::Unit(req_data) => {
                if *UNIT_HEADER_LEN < config().am_size_threshold && !immediate {
                    self.batcher
                        .add_unit_am_to_batch(req_data, stall_mark)
                        .await;
                } else {
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

    //#[tracing::instrument(skip_all)]
    async fn exec_msg(self, msg: Msg, mut ser_data:  SerializedData, lamellae: Arc<Lamellae>) {
        // println!("[{:?}] exec_msg {:?}", std::thread::current().id(), msg.cmd);
        // let data = ser_data.data_as_bytes();
        let mut i = 0;
        match msg.cmd {
            Cmd::Am => {
                self.exec_am(&msg, &ser_data, &mut i, &lamellae).await;
            }
            Cmd::ReturnAm => {
                self.exec_return_am(&msg, &ser_data, &mut i, &lamellae).await;
            }
            Cmd::Data => {
                self.exec_data_am(&msg, &mut i, &mut ser_data).await;
            }
            Cmd::Unit => {
                self.exec_unit_am(&msg, &ser_data, &mut i).await;
            }
            Cmd::BatchedMsg => {
                self.batcher
                    .exec_batched_msg(msg, ser_data, lamellae, &self)
                    .await;
            }
        }
    }
}

impl RegisteredActiveMessages {
    //#[tracing::instrument(skip_all)]
    pub(crate) fn new(batcher: BatcherType, executor: Arc<Executor>) -> RegisteredActiveMessages {
        RegisteredActiveMessages { batcher, executor }
    }

    //#[tracing::instrument(skip_all)]
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
        let mut data_buf = self
            .create_data_buf(header, am_size + *AM_HEADER_LEN, &req_data.lamellae)
            .await;
        let mut data_slice = data_buf.data_as_bytes_mut();

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
        let mut darcs = vec![];
        am.ser(darc_ser_cnt, &mut darcs);
        am.serialize_into(&mut data_slice[i..]);
        req_data
            .lamellae
            .send_to_pes_async(req_data.dst, req_data.team.arch.clone(), data_buf)
            .await;
    }

    // #[tracing::instrument(skip_all)]
    async fn send_data_am(&self, req_data: ReqMetaData, data: LamellarResultArc, data_size: usize) {
        // println!("send_data_am");
        let header = self.create_header(&req_data, Cmd::Data);
        let mut darcs = vec![];
        data.ser(1, &mut darcs); //1 because we are only sending back to the original PE
        let darc_list_size = crate::serialized_size(&darcs, false);
        let data_header = DataHeader {
            size: data_size,
            req_id: req_data.id,
            darc_list_size: darc_list_size,
        };

        let mut data_buf = self
            .create_data_buf(
                header,
                data_size + darc_list_size + *DATA_HEADER_LEN,
                &req_data.lamellae,
            )
            .await;
        let mut data_slice = data_buf.data_as_bytes_mut();

        crate::serialize_into(&mut data_slice[0..*DATA_HEADER_LEN], &data_header, false).unwrap();
        let mut i = *DATA_HEADER_LEN;

        crate::serialize_into(&mut data_slice[i..(i + darc_list_size)], &darcs, false).unwrap();
        i += darc_list_size;

        data.serialize_into(&mut data_slice[i..]);
        req_data
            .lamellae
            .send_to_pes_async(req_data.dst, req_data.team.arch.clone(), data_buf)
            .await;
    }

    // #[tracing::instrument(skip_all)]
    async fn send_unit_am(&self, req_data: ReqMetaData) {
        // println!("send_unit_am");

        let header = self.create_header(&req_data, Cmd::Unit);
        let mut data_buf = self
            .create_data_buf(header, *UNIT_HEADER_LEN, &req_data.lamellae)
            .await;
        let mut data_slice = data_buf.data_as_bytes_mut();

        let unit_header = UnitHeader {
            req_id: req_data.id,
        };
        crate::serialize_into(&mut data_slice[0..*UNIT_HEADER_LEN], &unit_header, false).unwrap();
        req_data
            .lamellae
            .send_to_pes_async(req_data.dst, req_data.team.arch.clone(), data_buf)
            .await;
    }

    // #[tracing::instrument(skip_all)]
    fn create_header(&self, req_data: &ReqMetaData, cmd: Cmd) -> SerializeHeader {
        let msg = Msg {
            src: req_data.team.world_pe as u16,
            cmd: cmd,
        };
        SerializeHeader { msg: msg }
    }

    //#[tracing::instrument(skip_all)]
    async fn create_data_buf(
        &self,
        header: SerializeHeader,
        size: usize,
        lamellae: &Arc<Lamellae>,
    ) -> SerializedData {
        // println!("create_data_buf");

        let header = Some(header);
        let mut data = lamellae.serialize_header(header.clone(), size);
        while let Err(err) = data {
            async_std::task::yield_now().await;
            match err.downcast_ref::<AllocError>() {
                Some(AllocError::OutOfMemoryError(_)) => {
                    lamellae.comm().alloc_pool(size * 2);
                }
                _ => panic!("unhanlded error!! {:?}", err),
            }
            data = lamellae.serialize_header(header.clone(), size);
        }
        data.unwrap()
    }

    //we can remove this by cloning self and submitting to the executor
    #[async_recursion]
    //#[tracing::instrument(skip_all)]
    pub(crate) async fn exec_local_am(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcLocalAm,
        world: Arc<LamellarTeam>,
        team: Arc<LamellarTeam>,
    ) {
        // println!("[{:?}] exec_local_am", std::thread::current().id());
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

    //#[tracing::instrument(skip_all)]
    pub(crate) async fn exec_am(
        &self,
        msg: &Msg,
        // data: &[u8],
        ser_data: &SerializedData,
        i: &mut usize,
        lamellae: &Arc<Lamellae>,
    ) {
        // println!("exec_am");
        let data =  ser_data.data_as_bytes();
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

        world.team.world_counters.inc_outstanding(1);
        team.team.team_counters.inc_outstanding(1);
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
        let ame = self.clone();
        self.executor.submit_task(async move {
            ame.process_msg(am, 0, false).await;
        });
        world.team.world_counters.dec_outstanding(1);
        team.team.team_counters.dec_outstanding(1);
        //compare against:
        // ame.process_msg(am, 0, true).await;
    }

    //#[tracing::instrument(skip_all)]
    pub(crate) async fn exec_return_am(
        &self,
        msg: &Msg,
        // data: &[u8],
        ser_data: &SerializedData,
        i: &mut usize,
        lamellae: &Arc<Lamellae>,
    ) {
        // println!("exec_return_am");
        let data =  ser_data.data_as_bytes();
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

    //#[tracing::instrument(skip_all)]
    pub(crate) async fn exec_data_am(
        &self,
        msg: &Msg,
        // data_buf: &[u8],
        i: &mut usize,
        ser_data:  &mut SerializedData,
    ) {
        // println!("exec_data_am");
        let data_buf = ser_data.data_as_bytes();
        let data_header: DataHeader =
            crate::deserialize(&data_buf[*i..*i + *DATA_HEADER_LEN], false).unwrap();
        *i += *DATA_HEADER_LEN;

        let darcs: Vec<RemotePtr> =
            crate::deserialize(&data_buf[*i..*i + data_header.darc_list_size], false).unwrap();
        *i += data_header.darc_list_size;

        let data =   ser_data.sub_data(*i, *i + data_header.size) ; // i is incermented preventing overlapping sub_data
        *i += data_header.size;

        self.send_data_to_user_handle(
            data_header.req_id,
            msg.src as usize,
            InternalResult::Remote(data, darcs),
        );
    }

    // #[tracing::instrument(skip_all)]
    pub(crate) async fn exec_unit_am(&self, msg: &Msg, 
        //data: &[u8], 
        ser_data:  &SerializedData, i: &mut usize) {
        // println!("exec_unit_am");
        let data = ser_data.data_as_bytes();
        let unit_header: UnitHeader =
            crate::deserialize(&data[*i..*i + *UNIT_HEADER_LEN], false).unwrap();
        *i += *UNIT_HEADER_LEN;
        self.send_data_to_user_handle(unit_header.req_id, msg.src as usize, InternalResult::Unit);
    }
}

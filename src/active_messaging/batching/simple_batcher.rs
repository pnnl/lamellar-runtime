use crate::{
    active_messaging::{registered_active_message::*, *},
    lamellae::{
        comm::error::AllocError, CommMem, CommSlice, Des, Lamellae, LamellaeAM, Ser,
        SerializeHeader,
    },
};
use batching::*;

use async_trait::async_trait;

const MAX_BATCH_SIZE: usize = 1_000_000;

#[derive(Clone, Debug)]
struct SimpleBatcherInner {
    batch: Arc<Mutex<Vec<(ReqMetaData, LamellarData, usize)>>>, //reqid,data,data size,team addr
    size: Arc<AtomicUsize>,
    batch_id: Arc<AtomicUsize>,
    pe: Option<usize>,
}

impl SimpleBatcherInner {
    //#[tracing::instrument(skip_all)]
    fn new(pe: Option<usize>) -> SimpleBatcherInner {
        SimpleBatcherInner {
            batch: Arc::new(Mutex::new(Vec::new())),
            size: Arc::new(AtomicUsize::new(0)),
            batch_id: Arc::new(AtomicUsize::new(0)),
            pe: pe,
        }
    }

    //#[tracing::instrument(skip_all)]
    fn add(
        &self,
        req_data: ReqMetaData,
        data: LamellarData,
        payload_size: usize,
        header_size: usize,
    ) -> usize {
        // println!("adding to batch");
        //return true if this is the first am in the batch
        let mut batch = self.batch.lock();
        let size = *CMD_LEN + payload_size + header_size;
        batch.push((req_data, data, size));
        // batch.len() == 1
        self.size.fetch_add(size, Ordering::Relaxed)
    }

    //#[tracing::instrument(skip_all)]
    fn swap(&self) -> (Vec<(ReqMetaData, LamellarData, usize)>, usize) {
        let mut batch = self.batch.lock();
        let size = self.size.load(Ordering::Relaxed);
        self.size.store(0, Ordering::Relaxed);
        let _batch_id = self.batch_id.fetch_add(1, Ordering::SeqCst);
        // println!("batch_id {_batch_id} swapped");
        let mut new_vec = Vec::new();
        std::mem::swap(&mut *batch, &mut new_vec);
        (new_vec, size)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SimpleBatcher {
    batched_ams: Arc<Vec<SimpleBatcherInner>>,
    stall_mark: Arc<AtomicUsize>,
    executor: Arc<Executor>,
}

#[async_trait]
impl Batcher for SimpleBatcher {
    // #[tracing::instrument(skip_all)]
    async fn add_remote_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_size: usize,
        mut stall_mark: usize,
    ) {
        // println!("add_remote_am_to_batch");
        //let dst =req_data.dst;
        let batch = match req_data.dst {
            Some(dst) => self.batched_ams[dst].clone(),
            None => self.batched_ams.last().unwrap().clone(),
        };
        if stall_mark == 0 {
            self.stall_mark.fetch_add(1, Ordering::Relaxed);
        }
        let size = batch.add(
            req_data,
            LamellarData::Am(am, am_id, am_size),
            am_size,
            *AM_HEADER_LEN,
        );
        if size == 0 {
            //first data in batch, schedule a transfer task
            let batch_id = batch.batch_id.load(Ordering::SeqCst);
            // println!("remote batch_id {batch_id} created ");
            let cur_stall_mark = self.stall_mark.clone();
            // println!(
            //     "[{:?}] add_remote_am_to_batch submit task",
            //     std::thread::current().id()
            // );
            self.executor.submit_io_task(async move {
                while stall_mark != cur_stall_mark.load(Ordering::SeqCst)
                    && batch.size.load(Ordering::SeqCst) < MAX_BATCH_SIZE
                    && batch_id == batch.batch_id.load(Ordering::SeqCst)
                {
                    stall_mark = cur_stall_mark.load(Ordering::Relaxed);
                    async_std::task::yield_now().await;
                }
                if batch_id == batch.batch_id.load(Ordering::SeqCst) {
                    //this batch is still valid
                    SimpleBatcher::create_tx_task(batch).await;
                }
            });
        } else if size >= MAX_BATCH_SIZE {
            // println!("remote size: {:?} ", size);
            // println!(
            //     "[{:?}] add_remote_am_to_batch submit imm task",
            //     std::thread::current().id()
            // );
            SimpleBatcher::create_tx_task(batch).await;
        }
    }

    // #[tracing::instrument(skip_all)]
    async fn add_return_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_size: usize,
        mut stall_mark: usize,
    ) {
        // println!("add_return_am_to_batch");
        //let dst =req_data.dst;
        let batch = match req_data.dst {
            Some(dst) => self.batched_ams[dst].clone(),
            None => self.batched_ams.last().unwrap().clone(),
        };
        if stall_mark == 0 {
            self.stall_mark.fetch_add(1, Ordering::Relaxed);
        }
        let size = batch.add(
            req_data,
            LamellarData::Return(am, am_id, am_size),
            am_size,
            *AM_HEADER_LEN,
        );
        if size == 0 {
            //first data in batch, schedule a transfer task
            let batch_id = batch.batch_id.load(Ordering::SeqCst);
            // println!("return batch_id {batch_id} created {dst:?}");
            let cur_stall_mark = self.stall_mark.clone();
            // println!(
            //     "[{:?}] add_rerturn_am_to_batch submit task",
            //     std::thread::current().id()
            // );
            self.executor.submit_io_task(async move {
                while stall_mark != cur_stall_mark.load(Ordering::SeqCst)
                    && batch.size.load(Ordering::SeqCst) < MAX_BATCH_SIZE
                    && batch_id == batch.batch_id.load(Ordering::SeqCst)
                {
                    stall_mark = cur_stall_mark.load(Ordering::Relaxed);
                    async_std::task::yield_now().await;
                }
                if batch_id == batch.batch_id.load(Ordering::SeqCst) {
                    //this batch is still valid
                    SimpleBatcher::create_tx_task(batch).await;
                }
            });
        } else if size >= MAX_BATCH_SIZE {
            // println!("return size: {:?} {dst:?}",size);
            // println!(
            //     "[{:?}] add_return_am_to_batch submit imm task",
            //     std::thread::current().id()
            // );
            SimpleBatcher::create_tx_task(batch).await;
        }
    }

    // #[tracing::instrument(skip_all)]
    async fn add_data_am_to_batch(
        &self,
        req_data: ReqMetaData,
        data: LamellarResultArc,
        data_size: usize,
        mut stall_mark: usize,
    ) {
        // println!("add_data_am_to_batch");
        //let dst =req_data.dst;
        let batch = match req_data.dst {
            Some(dst) => self.batched_ams[dst].clone(),
            None => self.batched_ams.last().unwrap().clone(),
        };
        if stall_mark == 0 {
            self.stall_mark.fetch_add(1, Ordering::Relaxed);
        }
        let mut darcs = vec![];
        data.ser(1, &mut darcs); //1 because we are only sending back to the original PE
        let darc_list_size = crate::serialized_size(&darcs, false);
        let size = batch.add(
            req_data,
            LamellarData::Data(data, darcs, data_size, darc_list_size),
            data_size,
            darc_list_size + *DATA_HEADER_LEN,
        );
        if size == 0 {
            //first data in batch, schedule a transfer task
            let batch_id = batch.batch_id.load(Ordering::SeqCst);
            // println!("data batch_id {batch_id} created {dst:?}");
            let cur_stall_mark = self.stall_mark.clone();
            // println!(
            //     "[{:?}] add_data_am_to_batch submit task",
            //     std::thread::current().id()
            // );
            self.executor.submit_io_task(async move {
                while stall_mark != cur_stall_mark.load(Ordering::SeqCst)
                    && batch.size.load(Ordering::SeqCst) < MAX_BATCH_SIZE
                    && batch_id == batch.batch_id.load(Ordering::SeqCst)
                {
                    stall_mark = cur_stall_mark.load(Ordering::Relaxed);
                    async_std::task::yield_now().await;
                }
                if batch_id == batch.batch_id.load(Ordering::SeqCst) {
                    //this batch is still valid
                    SimpleBatcher::create_tx_task(batch).await;
                }
            });
        } else if size >= MAX_BATCH_SIZE {
            // println!("data size: {:?} {dst:?}",size);
            // println!(
            //     "[{:?}] add_data_am_to_batch submit imm task",
            //     std::thread::current().id()
            // );
            SimpleBatcher::create_tx_task(batch).await;
        }
    }

    // #[tracing::instrument(skip_all)]
    async fn add_unit_am_to_batch(&self, req_data: ReqMetaData, mut stall_mark: usize) {
        // println!("add_unit_am_to_batch");
        //let dst =req_data.dst;
        let batch = match req_data.dst {
            Some(dst) => self.batched_ams[dst].clone(),
            None => self.batched_ams.last().unwrap().clone(),
        };
        if stall_mark == 0 {
            self.stall_mark.fetch_add(1, Ordering::Relaxed);
        }
        let size = batch.add(req_data, LamellarData::Unit, 0, *UNIT_HEADER_LEN);
        if size == 0 {
            //first data in batch, schedule a transfer task
            let batch_id = batch.batch_id.load(Ordering::SeqCst);
            // println!("unit batch_id {batch_id} created ");
            let cur_stall_mark = self.stall_mark.clone();
            // println!(
            //     "[{:?}] add_unit_am_to_batch submit task",
            //     std::thread::current().id()
            // );
            self.executor.submit_io_task(async move {
                while stall_mark != cur_stall_mark.load(Ordering::SeqCst)
                    && batch.size.load(Ordering::SeqCst) < MAX_BATCH_SIZE
                    && batch_id == batch.batch_id.load(Ordering::SeqCst)
                {
                    stall_mark = cur_stall_mark.load(Ordering::Relaxed);
                    async_std::task::yield_now().await;
                }
                if batch_id == batch.batch_id.load(Ordering::SeqCst) {
                    //this batch is still valid
                    SimpleBatcher::create_tx_task(batch).await;
                }
            });
        } else if size >= MAX_BATCH_SIZE {
            // println!("unit size: {:?} ", size);
            // println!(
            //     "[{:?}] add_unit_am_to_batch submit imm task",
            //     std::thread::current().id()
            // );
            SimpleBatcher::create_tx_task(batch).await;
        }
    }

    //#[tracing::instrument(skip_all)]
    async fn exec_batched_msg(
        &self,
        msg: Msg,
        mut ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
    ) {
        // let data = ser_data.data_as_bytes();
        let mut i = 0;
        let data_len = ser_data.len();
        // println!("executing batched msg {:?}", data.len());
        while i < data_len {
            //data.len() {
            // let cmd: Cmd = crate::deserialize(&data[i..i + *CMD_LEN], false).unwrap();
            let cmd: Cmd = ser_data.sub_data(i, *CMD_LEN).deserialize_data().unwrap();
            i += *CMD_LEN;
            // let temp_i = i;
            // println!("cmd {:?}", cmd);
            match cmd {
                Cmd::Am => self.exec_am(&msg, &ser_data, &mut i, &lamellae, ame),
                Cmd::ReturnAm => {
                    self.exec_return_am(&msg, &ser_data, &mut i, &lamellae, ame)
                        .await
                }
                Cmd::Data => ame.exec_data_am(&msg, &mut i, &mut ser_data).await,
                Cmd::Unit => ame.exec_unit_am(&msg, &ser_data, &mut i).await,
                Cmd::BatchedMsg => {
                    panic!("should not recieve a batched msg within a Simple Batcher batched msg")
                }
            }
        }
    }
}

impl SimpleBatcher {
    //#[tracing::instrument(skip_all)]
    pub(crate) fn new(
        num_pes: usize,
        stall_mark: Arc<AtomicUsize>,
        executor: Arc<Executor>,
    ) -> SimpleBatcher {
        let mut batched_ams = Vec::new();
        for pe in 0..num_pes {
            batched_ams.push(SimpleBatcherInner::new(Some(pe)));
        }
        batched_ams.push(SimpleBatcherInner::new(None));
        SimpleBatcher {
            batched_ams: Arc::new(batched_ams),
            stall_mark: stall_mark,
            executor: executor,
        }
    }

    //#[tracing::instrument(skip_all)]
    async fn create_tx_task(batch: SimpleBatcherInner) {
        // println!("[{:?}] create_tx_task", std::thread::current().id());
        let (buf, size) = batch.swap();

        if size > 0 {
            let lamellae = buf[0].0.lamellae.clone();
            let arch = buf[0].0.team.arch.clone();
            let header = SimpleBatcher::create_header(buf[0].0.team.world_pe);
            let mut data_buf = SimpleBatcher::create_data_buf(header, size, &lamellae).await;
            let data_slice = data_buf.data_as_bytes_mut();

            let mut cnts = HashMap::new();

            let mut i = 0;
            for (req_data, data, _size) in buf {
                let req_data_slice = data_slice.sub_slice(i..);
                match data {
                    LamellarData::Am(am, id, am_size) => {
                        i += SimpleBatcher::serialize_am(
                            req_data,
                            am_size,
                            am,
                            id,
                            req_data_slice,
                            Cmd::Am,
                        );
                        cnts.entry(Cmd::Am).and_modify(|e| *e += 1).or_insert(1);
                    }
                    LamellarData::Return(am, id, am_size) => {
                        i += SimpleBatcher::serialize_am(
                            req_data,
                            am_size,
                            am,
                            id,
                            req_data_slice,
                            Cmd::ReturnAm,
                        );
                        cnts.entry(Cmd::ReturnAm)
                            .and_modify(|e| *e += 1)
                            .or_insert(1);
                    }
                    LamellarData::Data(data, darcs, data_size, darc_list_size) => {
                        i += SimpleBatcher::serialize_data(
                            req_data,
                            data_size,
                            data,
                            req_data_slice,
                            darcs,
                            darc_list_size,
                        );
                        cnts.entry(Cmd::Data).and_modify(|e| *e += 1).or_insert(1);
                    }
                    LamellarData::Unit => {
                        i += SimpleBatcher::serialize_unit(req_data, req_data_slice);
                        cnts.entry(Cmd::Unit).and_modify(|e| *e += 1).or_insert(1);
                    }
                }
            }
            // println!(
            //     "[{:?}] sending batch of size {} {} to pe {:?} {:?}",
            //     std::thread::current().id(),
            //     i,
            //     data_buf.len(),
            //     batch.pe,
            //     cnts
            // );
            lamellae.send_to_pes_async(batch.pe, arch, data_buf).await;
        }
    }

    //#[tracing::instrument(skip_all)]
    fn serialize_am(
        req_data: ReqMetaData,
        am_size: usize,
        am: LamellarArcAm,
        am_id: AmId,
        mut data_buf: CommSlice<u8>,
        cmd: Cmd,
    ) -> usize {
        // println!("serialize_am");
        let mut i = 0;
        crate::serialize_into(&mut data_buf[i..i + *CMD_LEN], &cmd, false).unwrap();
        i += *CMD_LEN;

        let am_header = AmHeader {
            am_id: am_id,
            req_id: req_data.id,
            team_addr: req_data.team_addr,
        };
        crate::serialize_into(&mut data_buf[i..i + *AM_HEADER_LEN], &am_header, false).unwrap();
        i += *AM_HEADER_LEN;

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
        am.serialize_into(&mut data_buf[i..i + am_size]);
        i + am_size
    }

    //#[tracing::instrument(skip_all)]
    fn serialize_data(
        req_data: ReqMetaData,
        data_size: usize,
        data: LamellarResultArc,
        mut data_buf: CommSlice<u8>,
        darcs: Vec<RemotePtr>,
        darc_list_size: usize,
    ) -> usize {
        // println!("serialize_data");
        let mut i = 0;
        crate::serialize_into(&mut data_buf[i..i + *CMD_LEN], &Cmd::Data, false).unwrap();
        i += *CMD_LEN;
        let data_header = DataHeader {
            size: data_size,
            req_id: req_data.id,
            darc_list_size: darc_list_size,
        };
        crate::serialize_into(&mut data_buf[i..i + *DATA_HEADER_LEN], &data_header, false).unwrap();
        i += *DATA_HEADER_LEN;

        crate::serialize_into(&mut data_buf[i..(i + darc_list_size)], &darcs, false).unwrap();
        i += darc_list_size;

        data.serialize_into(&mut data_buf[i..i + data_size]);
        i + data_size
    }

    //#[tracing::instrument(skip_all)]
    fn serialize_unit(req_data: ReqMetaData, mut data_buf: CommSlice<u8>) -> usize {
        // println!("serialize_unit");
        let mut i = 0;
        crate::serialize_into(&mut data_buf[i..i + *CMD_LEN], &Cmd::Unit, false).unwrap();
        i += *CMD_LEN;

        let unit_header = UnitHeader {
            req_id: req_data.id,
        };
        crate::serialize_into(&mut data_buf[i..i + *UNIT_HEADER_LEN], &unit_header, false).unwrap();
        i + *UNIT_HEADER_LEN
    }

    //#[tracing::instrument(skip_all)]
    fn create_header(src: usize) -> SerializeHeader {
        // println!("create_header");
        let msg = Msg {
            src: src as u16,
            cmd: Cmd::BatchedMsg,
        };
        SerializeHeader { msg: msg }
    }

    //#[tracing::instrument(skip_all)]
    async fn create_data_buf(
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

    // #[tracing::instrument(skip_all)]
    // async
    fn exec_am(
        &self,
        msg: &Msg,
        // data: &[u8],
        ser_data: &SerializedData,
        i: &mut usize,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
    ) {
        // println!("exec_am");
        let data = ser_data.data_as_bytes();
        let am_header: AmHeader =
            crate::deserialize(&data[*i..*i + *AM_HEADER_LEN], false).unwrap();
        let (team, world) =
            ame.get_team_and_world(msg.src as usize, am_header.team_addr, &lamellae);
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
            team_addr: unsafe { *team.team.remote_ptr_alloc.as_ptr() },
        };
        // println!(
        //     "[{:?}] simple batcher exec_am submit task",
        //     std::thread::current().id()
        // );
        let ame = ame.clone();
        world.team.world_counters.inc_outstanding(1);
        team.team.team_counters.inc_outstanding(1);
        self.executor.submit_task(async move {
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
            world.team.world_counters.dec_outstanding(1);
            team.team.team_counters.dec_outstanding(1);
            ame.process_msg(am, 0, false).await;
        });
    }

    // #[tracing::instrument(skip_all)]
    async fn exec_return_am(
        &self,
        msg: &Msg,
        // data: &[u8],
        ser_data: &SerializedData,
        i: &mut usize,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
    ) {
        // println!("exec_return_am");
        let data = ser_data.data_as_bytes();
        let am_header: AmHeader =
            crate::deserialize(&data[*i..*i + *AM_HEADER_LEN], false).unwrap();
        let (team, world) =
            ame.get_team_and_world(msg.src as usize, am_header.team_addr, &lamellae);
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
            team_addr: unsafe { *team.team.remote_ptr_alloc.as_ptr() },
        };
        // println!(
        //     "[{:?}] exec_return_am submit task",
        //     std::thread::current().id()
        // );
        ame.clone()
            .exec_local_am(req_data, am.as_local(), world, team)
            .await;
    }
}

use std::collections::HashMap;

use crate::{
    active_messaging::{registered_active_message::*, *},
    lamellae::{
        comm::error::AllocError, CommSlice, Lamellae, LamellaeUtil, Ser, SerializeHeader,
    },
    lamellar_arch::LamellarArchRT,
    LamellarTeam,
};
use batching::*;

use tracing::trace;

use async_trait::async_trait;
use parking_lot::Mutex;
use zerocopy::{IntoBytes, TryFromBytes};

const MAX_BATCH_SIZE: usize = 1_000_000;

pub(crate) const TEAM_HEADER_LEN: usize = std::mem::size_of::<TeamHeader>();
pub(crate) const BATCH_HEADER_LEN: usize = std::mem::size_of::<BatchHeader>();
pub(crate) const BATCHED_AM_HEADER_LEN: usize = std::mem::size_of::<BatchedAmHeader>();
pub(crate) const REQ_ID_LEN: usize = std::mem::size_of::<ReqId>();
// Explicit length prefix for each grouped AM's body -- recomputing the length
// by re-serializing the just-decoded AM (as this used to do) is unsound under
// a variable-width body codec (postcard): a field whose value legitimately
// changes across the wire (e.g. an address translated on receipt) can
// re-encode to a different byte width than what the sender wrote.
pub(crate) const AM_LEN_LEN: usize = std::mem::size_of::<usize>();

// type TeamId = Darc<LamellarTeamRT>;
type TeamId = usize;
type AmIdMap = HashMap<AmId, Vec<(ReqMetaData, LamellarArcAm, Vec<u8>)>>;
type TeamMap = HashMap<TeamId, AmIdMap>;

// Fixed-size wire header: zerocopy raw-byte layout, not (de)serialized via
// crate::serialize/deserialize -- its length must be a true compile-time
// constant, independent of field values (see BATCH_HEADER_LEN above).
#[repr(C)]
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Default,
    Debug,
    Copy,
    Clone,
    zerocopy_derive::IntoBytes,
    zerocopy_derive::TryFromBytes,
    zerocopy_derive::KnownLayout,
    zerocopy_derive::Immutable,
)]
struct BatchHeader {
    cnt: usize,
    cmd: Cmd,
    _pad: [u8; 7], // explicit -- zerocopy rejects implicit trailing padding
}

#[repr(C)]
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Default,
    Debug,
    Copy,
    Clone,
    zerocopy_derive::IntoBytes,
    zerocopy_derive::TryFromBytes,
    zerocopy_derive::KnownLayout,
    zerocopy_derive::Immutable,
)]
pub(crate) struct TeamHeader {
    pub(crate) team: TeamId,
    pub(crate) am_batch_cnts: usize,
}

#[repr(C)]
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Default,
    Debug,
    Copy,
    Clone,
    zerocopy_derive::IntoBytes,
    zerocopy_derive::TryFromBytes,
    zerocopy_derive::KnownLayout,
    zerocopy_derive::Immutable,
)]
struct BatchedAmHeader {
    am_cnt: usize,
    am_id: AmId,
    cmd: Cmd,
    _pad: [u8; 3], // explicit -- zerocopy rejects implicit trailing padding
}

#[derive(Clone)]
struct TeamAmBatcherInner {
    batch: Arc<Mutex<(TeamMap, TeamMap, Vec<(ReqMetaData, LamellarData)>)>>,
    size: Arc<AtomicUsize>,
    batch_id: Arc<AtomicUsize>,
    pe: Option<usize>,
}

impl std::fmt::Debug for TeamAmBatcherInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TeamAmBatcherInner {:?} {:?}",
            self.size.load(Ordering::SeqCst),
            self.pe
        )
    }
}

impl TeamAmBatcherInner {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn new(pe: Option<usize>) -> TeamAmBatcherInner {
        TeamAmBatcherInner {
            batch: Arc::new(Mutex::new((HashMap::new(), HashMap::new(), Vec::new()))),
            size: Arc::new(AtomicUsize::new(0)),
            batch_id: Arc::new(AtomicUsize::new(0)),
            pe,
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn add_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        id: AmId,
        am_bytes: Vec<u8>,
        batch: &mut TeamMap,
    ) -> usize {
        let am_size = am_bytes.len();
        let mut temp_size = 0;
        let team_batch = batch
            .entry(req_data.team.darc_addr())
            // .entry(req_data.team.clone())
            .or_insert_with(|| HashMap::new());
        if team_batch.len() == 0 {
            temp_size += TEAM_HEADER_LEN;
            // println!(
            //     "[{:?}] adding team header {} {} {}",
            //     std::thread::current().id(),
            //     temp_size,
            //     TEAM_HEADER_LEN,
            //     self.size.load(Ordering::SeqCst)
            // );
        }
        let am_batch = team_batch.entry(id).or_insert_with(|| Vec::new());
        if am_batch.len() == 0 {
            temp_size += BATCHED_AM_HEADER_LEN;
            // println!(
            //     "[{:?}] adding batched header {} {} {}",
            //     std::thread::current().id(),
            //     temp_size,
            //     BATCHED_AM_HEADER_LEN,
            //     self.size.load(Ordering::SeqCst)
            // );
        }
        am_batch.push((req_data, am, am_bytes));
        temp_size += am_size + REQ_ID_LEN + AM_LEN_LEN;
        // println!(
        //     "[{:?}] adding req_id + size header {} {} {} {}",
        //     std::thread::current().id(),
        //     temp_size,
        //     *REQ_ID_LEN,
        //     size,
        //     self.size.load(Ordering::SeqCst)
        // );
        temp_size
        //println!("updated size: {:?}", self.size.load(Ordering::SeqCst));
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn add_am(&self, req_data: ReqMetaData, data: LamellarData) -> usize {
        match data {
            LamellarData::Am(am, id, am_bytes) => {
                let mut batch = self.batch.lock();
                let batch_size = self.add_am_to_batch(req_data, am, id, am_bytes, &mut batch.0);
                self.size.fetch_add(batch_size, Ordering::SeqCst)
            }
            LamellarData::Return(am, id, am_bytes) => {
                let mut batch = self.batch.lock();
                let batch_size = self.add_am_to_batch(req_data, am, id, am_bytes, &mut batch.1);
                self.size.fetch_add(batch_size, Ordering::SeqCst)
            }
            _ => {
                panic!("unexpected data type");
            }
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn add_non_am(&self, req_data: ReqMetaData, data: LamellarData, size: usize) -> usize {
        let mut batch = self.batch.lock();
        let size = size + BATCH_HEADER_LEN;
        batch.2.push((req_data, data));
        self.size.fetch_add(size, Ordering::SeqCst)
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn swap(&self) -> (TeamMap, TeamMap, Vec<(ReqMetaData, LamellarData)>, usize) {
        let mut batch = self.batch.lock();
        let mut new_batch = (HashMap::new(), HashMap::new(), Vec::new());
        std::mem::swap(&mut batch.0, &mut new_batch.0);
        std::mem::swap(&mut batch.1, &mut new_batch.1);
        std::mem::swap(&mut batch.2, &mut new_batch.2);
        let size = self.size.load(Ordering::SeqCst);
        self.size.store(0, Ordering::SeqCst);
        let _batch_id = self.batch_id.fetch_add(1, Ordering::SeqCst);
        // println!("batch_id {batch_id} swapped");
        (new_batch.0, new_batch.1, new_batch.2, size)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TeamAmBatcher {
    batched_ams: Arc<Vec<TeamAmBatcherInner>>,
    stall_mark: Arc<AtomicUsize>,
    executor: Arc<Executor>,
}

#[async_trait]
impl Batcher for TeamAmBatcher {
    // //#[tracing::instrument(skip_all)]
    async fn add_remote_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_bytes: Vec<u8>,
        mut stall_mark: usize,
    ) {
        // println!("[{:?}] add_remote_am_to_batch", std::thread::current().id());
        let batch = match req_data.dst {
            Some(dst) => self.batched_ams[dst].clone(),
            None => self.batched_ams.last().unwrap().clone(),
        };
        if stall_mark == 0 {
            self.stall_mark.fetch_add(1, Ordering::Relaxed);
        }
        let size = batch.add_am(req_data.clone(), LamellarData::Am(am, am_id, am_bytes));
        if size == 0 {
            //first data in batch, schedule a transfer task
            let batch_id = batch.batch_id.load(Ordering::SeqCst);
            // println!(
            //     "[{:?}] remote batch_id {batch_id} created",
            //     std::thread::current().id()
            // );
            let cur_stall_mark = self.stall_mark.clone();
            self.executor.submit_io_task(async move {
                while stall_mark != cur_stall_mark.load(Ordering::Acquire)
                    && batch.size.load(Ordering::SeqCst) < MAX_BATCH_SIZE
                    && batch_id == batch.batch_id.load(Ordering::SeqCst)
                {
                    stall_mark = cur_stall_mark.load(Ordering::Relaxed);
                    async_std::task::yield_now().await;
                }
                if batch_id == batch.batch_id.load(Ordering::SeqCst) {
                    //this batch is still valid
                    TeamAmBatcher::create_tx_task(
                        batch,
                        req_data.lamellae.clone(),
                        req_data.team.arch.clone(),
                        req_data.team.world_pe,
                    )
                    .await;
                }
            });
        } else if size >= MAX_BATCH_SIZE {
            //batch is full, transfer now
            // println!(
            //     "[{:?}] remote size: {:?}",
            //     std::thread::current().id(),
            //     size
            // );
            TeamAmBatcher::create_tx_task(
                batch,
                req_data.lamellae.clone(),
                req_data.team.arch.clone(),
                req_data.team.world_pe,
            )
            .await;
        }
    }

    // //#[tracing::instrument(skip_all)]
    async fn add_return_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_bytes: Vec<u8>,
        mut stall_mark: usize,
    ) {
        // println!("[{:?}] add_return_am_to_batch", std::thread::current().id(),);
        let batch = match req_data.dst {
            Some(dst) => self.batched_ams[dst].clone(),
            None => self.batched_ams.last().unwrap().clone(),
        };
        if stall_mark == 0 {
            self.stall_mark.fetch_add(1, Ordering::Relaxed);
        }
        let size = batch.add_am(req_data.clone(), LamellarData::Return(am, am_id, am_bytes));
        if size == 0 {
            //first data in batch, schedule a transfer task
            let batch_id = batch.batch_id.load(Ordering::SeqCst);
            // println!(
            //     "[{:?}] return batch_id {batch_id} created",
            //     std::thread::current().id()
            // );
            let cur_stall_mark = self.stall_mark.clone();
            self.executor.submit_io_task(async move {
                while stall_mark != cur_stall_mark.load(Ordering::Acquire)
                    && batch.size.load(Ordering::SeqCst) < MAX_BATCH_SIZE
                    && batch_id == batch.batch_id.load(Ordering::SeqCst)
                {
                    stall_mark = cur_stall_mark.load(Ordering::Relaxed);
                    async_std::task::yield_now().await;
                }
                if batch_id == batch.batch_id.load(Ordering::SeqCst) {
                    //this batch is still valid
                    TeamAmBatcher::create_tx_task(
                        batch,
                        req_data.lamellae.clone(),
                        req_data.team.arch.clone(),
                        req_data.team.world_pe,
                    )
                    .await;
                }
            });
        } else if size >= MAX_BATCH_SIZE {
            //batch is full, transfer now
            // println!(
            //     "[{:?}] return size: {:?}",
            //     std::thread::current().id(),
            //     size
            // );

            TeamAmBatcher::create_tx_task(
                batch,
                req_data.lamellae.clone(),
                req_data.team.arch.clone(),
                req_data.team.world_pe,
            )
            .await;
        }
    }

    // //#[tracing::instrument(skip_all)]
    async fn add_data_am_to_batch(
        &self,
        req_data: ReqMetaData,
        darc_bytes: Vec<u8>,
        data_bytes: Vec<u8>,
        mut stall_mark: usize,
    ) {
        // println!("[{:?}] add_data_am_to_batch", std::thread::current().id(),);
        let batch = match req_data.dst {
            Some(dst) => self.batched_ams[dst].clone(),
            None => self.batched_ams.last().unwrap().clone(),
        };
        if stall_mark == 0 {
            self.stall_mark.fetch_add(1, Ordering::Relaxed);
        }
        let darc_list_size = darc_bytes.len();
        let data_size = data_bytes.len();
        let size = batch.add_non_am(
            req_data.clone(),
            LamellarData::Data(darc_bytes, data_bytes),
            data_size + darc_list_size + DATA_HEADER_LEN,
        );
        if size == 0 {
            //first data in batch, schedule a transfer task
            let batch_id = batch.batch_id.load(Ordering::SeqCst);
            // println!(
            //     "[{:?}] data batch_id {batch_id} created",
            //     std::thread::current().id()
            // );
            let cur_stall_mark = self.stall_mark.clone();
            self.executor.submit_io_task(async move {
                while stall_mark != cur_stall_mark.load(Ordering::Acquire)
                    && batch.size.load(Ordering::SeqCst) < MAX_BATCH_SIZE
                    && batch_id == batch.batch_id.load(Ordering::SeqCst)
                {
                    stall_mark = cur_stall_mark.load(Ordering::Relaxed);
                    async_std::task::yield_now().await;
                }
                if batch_id == batch.batch_id.load(Ordering::SeqCst) {
                    //this batch is still valid
                    TeamAmBatcher::create_tx_task(
                        batch,
                        req_data.lamellae.clone(),
                        req_data.team.arch.clone(),
                        req_data.team.world_pe,
                    )
                    .await;
                }
            });
        } else if size >= MAX_BATCH_SIZE {
            //batch is full, transfer now
            // println!("[{:?}] data size: {:?}", std::thread::current().id(), size);
            TeamAmBatcher::create_tx_task(
                batch,
                req_data.lamellae.clone(),
                req_data.team.arch.clone(),
                req_data.team.world_pe,
            )
            .await;
        }
    }

    // //#[tracing::instrument(skip_all)]
    async fn add_unit_am_to_batch(&self, req_data: ReqMetaData, mut stall_mark: usize) {
        // println!("[{:?}] add_unit_am_to_batch", std::thread::current().id(),);
        let batch = match req_data.dst {
            Some(dst) => self.batched_ams[dst].clone(),
            None => self.batched_ams.last().unwrap().clone(),
        };
        if stall_mark == 0 {
            self.stall_mark.fetch_add(1, Ordering::Relaxed);
        }
        let size = batch.add_non_am(req_data.clone(), LamellarData::Unit, UNIT_HEADER_LEN);
        if size == 0 {
            //first data in batch, schedule a transfer task
            let batch_id = batch.batch_id.load(Ordering::SeqCst);
            // println!(
            //     "[{:?}] unit batch_id {batch_id} created",
            //     std::thread::current().id()
            // );
            let cur_stall_mark = self.stall_mark.clone();
            self.executor.submit_io_task(async move {
                while stall_mark != cur_stall_mark.load(Ordering::Acquire)
                    && batch.size.load(Ordering::SeqCst) < MAX_BATCH_SIZE
                    && batch_id == batch.batch_id.load(Ordering::SeqCst)
                {
                    stall_mark = cur_stall_mark.load(Ordering::Relaxed);
                    async_std::task::yield_now().await;
                }
                if batch_id == batch.batch_id.load(Ordering::SeqCst) {
                    //this batch is still valid
                    TeamAmBatcher::create_tx_task(
                        batch,
                        req_data.lamellae.clone(),
                        req_data.team.arch.clone(),
                        req_data.team.world_pe,
                    )
                    .await;
                }
            });
        } else if size >= MAX_BATCH_SIZE {
            //batch is full, transfer now
            // println!("[{:?}] unit size: {:?}", std::thread::current().id(), size);
            TeamAmBatcher::create_tx_task(
                batch,
                req_data.lamellae.clone(),
                req_data.team.arch.clone(),
                req_data.team.world_pe,
            )
            .await;
        }
    }

    // //#[tracing::instrument(skip_all, level = "debug")]
    async fn exec_batched_msg(
        &self,
        msg: Msg,
        mut ser_data: SerializedData,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
    ) {
        // println!("[{:?}] exec_batched_msg", std::thread::current().id());
        let mut i = 0;
        // println!("i: {:?} dl {:?} cl {:?}", i, data.len(), CMD_LEN);
        while i < ser_data.data_len() {
            // println!("\ti: {:?} dl {:?} cl {:?}", i, data.len(), CMD_LEN);
            let batch = BatchHeader::try_read_from_bytes(
                &ser_data.data_as_bytes()[i..i + BATCH_HEADER_LEN],
            )
            .expect("failed to parse BatchHeader");
            // println!("batch {:?} i: {} len: {}", batch, i, data.len());
            i += BATCH_HEADER_LEN;
            // println!("[{:?}] cmd {:?}", std::thread::current().id(), batch.cmd);
            match batch.cmd {
                Cmd::Am | Cmd::ReturnAm => {
                    panic!("should not encounter individual am cmds in TeamAmBatcher")
                }
                Cmd::Data => {
                    let data = ser_data.data_as_bytes();
                    exec_data_am_serde(msg.src as usize, &data, &mut i, ame);
                }
                Cmd::Unit => {
                    let data = ser_data.data_as_bytes();
                    exec_unit_am_serde(msg.src as usize, &data, &mut i, ame);
                }
                Cmd::BatchedMsg => {
                    self.exec_batched_am(&msg, batch.cnt, &mut ser_data, &mut i, lamellae, &ame)
                        .await;
                }
            }
        }
        trace!(target: "lamellae_debug", "finished exec_batched_msg  lamellae cnt: {:?}",Arc::strong_count(&lamellae));
    }

    async fn send_am(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_bytes: Vec<u8>,
        cmd: Cmd,
    ) {
        send_am_serde(req_data, am, am_id, am_bytes, cmd).await;
    }

    async fn send_data_am(&self, req_data: ReqMetaData, darc_bytes: Vec<u8>, data_bytes: Vec<u8>) {
        send_data_am_serde(req_data, darc_bytes, data_bytes).await;
    }

    async fn send_unit_am(&self, req_data: ReqMetaData) {
        send_unit_am_serde(req_data).await;
    }

    async fn exec_am(
        &self,
        src: usize,
        data: &[u8],
        i: &mut usize,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
        _executor: &Arc<Executor>,
    ) {
        exec_am_serde(src, data, i, lamellae, ame, &ame.executor);
    }

    async fn exec_return_am(
        &self,
        src: usize,
        data: &[u8],
        i: &mut usize,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
    ) {
        exec_return_am_serde(src, data, i, lamellae, ame).await;
    }

    fn exec_data_am(&self, src: usize, data: &[u8], i: &mut usize, ame: &RegisteredActiveMessages) {
        exec_data_am_serde(src, data, i, ame);
    }

    fn exec_unit_am(&self, src: usize, data: &[u8], i: &mut usize, ame: &RegisteredActiveMessages) {
        exec_unit_am_serde(src, data, i, ame);
    }
}

impl TeamAmBatcher {
    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new(
        num_pes: usize,
        stall_mark: Arc<AtomicUsize>,
        executor: Arc<Executor>,
    ) -> TeamAmBatcher {
        let mut batched_ams = Vec::new();
        for pe in 0..num_pes {
            batched_ams.push(TeamAmBatcherInner::new(Some(pe)));
        }
        batched_ams.push(TeamAmBatcherInner::new(None));
        TeamAmBatcher {
            batched_ams: Arc::new(batched_ams),
            stall_mark,
            executor,
        }
    }
    //#[tracing::instrument(skip_all, level = "debug")]
    async fn create_tx_task(
        batch: TeamAmBatcherInner,
        lamellae: Arc<Lamellae>,
        arch: Arc<LamellarArchRT>,
        my_pe: usize,
    ) {
        // println!("[{:?}] create_tx_task", std::thread::current().id());
        let (am_batch, return_am_batch, non_am_batch, mut size) = batch.swap();
        if size > 0 {
            if am_batch.len() > 0 {
                size += BATCH_HEADER_LEN
            }
            if return_am_batch.len() > 0 {
                size += BATCH_HEADER_LEN
            }

            let header = TeamAmBatcher::create_header(my_pe);
            trace!(target: "ucx","create_tx_task creating data_buf for {:?}",batch.pe);
            let mut data_buf = TeamAmBatcher::create_data_buf(header, size, &lamellae).await;
            trace!(target: "ucx","create_tx_task creating data_slice for {:?}",batch.pe);
            let data_slice = data_buf.data_as_bytes_mut();
            let data_slice_addr = data_slice.usize_addr();

            // println!(
            //     "[{:?}] total batch size: {}",
            //     std::thread::current().id(),
            //     size
            // );
            let mut i = 0;
            trace!(target: "ucx","create_tx_task creating sub_slice1 for {:?} addr: 0x{:x}",batch.pe, data_slice_addr);
            i += TeamAmBatcher::serialize_am_batch(
                am_batch,
                data_slice.sub_slice(i..),
                Cmd::Am,
                batch.pe,
            );
            trace!(target: "ucx","create_tx_task creating sub_slice2 for {:?} addr: 0x{:x}",batch.pe, data_slice_addr);
            i += TeamAmBatcher::serialize_am_batch(
                return_am_batch,
                data_slice.sub_slice(i..),
                Cmd::ReturnAm,
                batch.pe,
            );
            trace!(target: "ucx","create_tx_task creating sub_slice3 for {:?} addr: 0x{:x}",batch.pe, data_slice_addr);
            TeamAmBatcher::serialize_non_am_batch(non_am_batch, data_slice.sub_slice(i..));
            trace!(target: "ucx","create_tx_task sending to pes for {:?} addr: 0x{:x}",batch.pe, data_slice_addr);

            lamellae.send_to_pes_async(batch.pe, arch, data_buf).await;
            trace!(target: "ucx","create_tx_task done sending to pes for {:?} addr: 0x{:x}",batch.pe, data_slice_addr);
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn serialize_am_batch(
        am_batch: TeamMap,
        mut data_slice: CommSlice<u8>,
        cmd: Cmd,
        _pe: Option<usize>,
    ) -> usize {
        let mut i = 0;
        if am_batch.len() > 0 {
            let batch_header = BatchHeader {
                cmd: Cmd::BatchedMsg,
                cnt: am_batch.len(),
                _pad: [0; 7],
            };
            data_slice[i..i + BATCH_HEADER_LEN].copy_from_slice(batch_header.as_bytes());
            i += BATCH_HEADER_LEN;
            //println!("after batch cmd -- i: {}", i);

            for (team, am_map) in am_batch {
                // if pe.is_some() {
                //     team.ser(1, &mut vec![]); //ensure team is serialized for am header
                // } else {
                //     team.ser(team.num_pes(), &mut vec![]); //ensure team is serialized for am header
                // }
                let team_header = TeamHeader {
                    team: team,
                    am_batch_cnts: am_map.len(),
                };
                data_slice[i..i + TEAM_HEADER_LEN].copy_from_slice(team_header.as_bytes());
                i += TEAM_HEADER_LEN;
                //println!("after team header -- i: {}", i);

                for (am_id, ams) in am_map {
                    let batched_am_header = BatchedAmHeader {
                        am_id,
                        am_cnt: ams.len(),
                        cmd,
                        _pad: [0; 3],
                    };
                    data_slice[i..i + BATCHED_AM_HEADER_LEN]
                        .copy_from_slice(batched_am_header.as_bytes());
                    i += BATCHED_AM_HEADER_LEN;
                    //println!("after batched header -- i: {}", i);
                    for (req_data, am, am_bytes) in ams {
                        i += TeamAmBatcher::serialize_am(
                            req_data,
                            am_bytes,
                            am,
                            am_id,
                            data_slice.sub_slice(i..),
                        );
                    }
                }
            }
        }
        i
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn serialize_non_am_batch(
        non_am_batch: Vec<(ReqMetaData, LamellarData)>,
        data_slice: CommSlice<u8>,
    ) -> usize {
        let mut i = 0;
        for (req_data, data) in non_am_batch {
            match data {
                LamellarData::Am(_, _, _) | LamellarData::Return(_, _, _) => {
                    panic!("should not have non am batch with am or return data");
                }
                LamellarData::Data(darc_bytes, data_bytes) => {
                    i += TeamAmBatcher::serialize_data(
                        req_data,
                        data_bytes,
                        data_slice.sub_slice(i..),
                        darc_bytes,
                    );
                }
                LamellarData::Unit => {
                    i += TeamAmBatcher::serialize_unit(req_data, data_slice.sub_slice(i..));
                }
            }
        }
        i
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn serialize_am(
        req_data: ReqMetaData,
        am_bytes: Vec<u8>,
        am: LamellarArcAm,
        _am_id: AmId,
        mut data_buf: CommSlice<u8>,
    ) -> usize {
        let mut i = 0;
        data_buf[i..i + REQ_ID_LEN].copy_from_slice(req_data.id.as_bytes());
        i += REQ_ID_LEN;

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
        let am_size = am_bytes.len();
        data_buf[i..i + AM_LEN_LEN].copy_from_slice(am_size.as_bytes());
        i += AM_LEN_LEN;
        data_buf[i..i + am_size].copy_from_slice(&am_bytes);
        i += am_size;
        i
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn serialize_data(
        req_data: ReqMetaData,
        data_bytes: Vec<u8>,
        mut data_buf: CommSlice<u8>,
        darc_bytes: Vec<u8>,
    ) -> usize {
        let data_size = data_bytes.len();
        let darc_list_size = darc_bytes.len();
        let mut i = 0;
        let batch_header = BatchHeader {
            cmd: Cmd::Data,
            cnt: 1,
            _pad: [0; 7],
        };
        data_buf[i..i + BATCH_HEADER_LEN].copy_from_slice(batch_header.as_bytes());
        i += BATCH_HEADER_LEN;
        let data_header = DataHeader {
            size: data_size,
            req_id: req_data.id,
            darc_list_size,
        };
        data_buf[i..i + DATA_HEADER_LEN].copy_from_slice(data_header.as_bytes());
        i += DATA_HEADER_LEN;

        data_buf[i..i + darc_list_size].copy_from_slice(&darc_bytes);
        i += darc_list_size;

        data_buf[i..i + data_size].copy_from_slice(&data_bytes);
        i + data_size
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn serialize_unit(req_data: ReqMetaData, mut data_buf: CommSlice<u8>) -> usize {
        let mut i = 0;
        let batch_header = BatchHeader {
            cmd: Cmd::Unit,
            cnt: 1,
            _pad: [0; 7],
        };
        data_buf[i..i + BATCH_HEADER_LEN].copy_from_slice(batch_header.as_bytes());
        i += BATCH_HEADER_LEN;

        let unit_header = UnitHeader {
            req_id: req_data.id,
        };
        data_buf[i..i + UNIT_HEADER_LEN].copy_from_slice(unit_header.as_bytes());
        i + UNIT_HEADER_LEN
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn create_header(src: usize) -> SerializeHeader {
        let msg = Msg {
            src: src as u16,
            cmd: Cmd::BatchedMsg,
            padding: [0; 1],
        };
        SerializeHeader { msg }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    async fn create_data_buf(
        header: SerializeHeader,
        size: usize,
        lamellae: &Arc<Lamellae>,
    ) -> SerializedData {
        let mut data = lamellae.serialize_header(header, size);
        while let Err(err) = data {
            async_std::task::yield_now().await;
            match err.downcast_ref::<AllocError>() {
                Some(AllocError::OutOfMemoryError(_)) => {
                    lamellae.request_new_alloc(size * 2).await;
                }
                _ => panic!("unhanlded error!! {:?}", err),
            }
            data = lamellae.serialize_header(header, size);
        }
        data.unwrap()
    }

    // //#[tracing::instrument(skip_all)]
    async fn exec_batched_am(
        &self,
        msg: &Msg,
        batch_cnt: usize,
        ser_data: &mut SerializedData,
        i: &mut usize,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
    ) {
        // println!("exec_batched_am batch_cnt: {}", batch_cnt);

        for _team in 0..batch_cnt {
            let team_header = TeamHeader::try_read_from_bytes(
                &ser_data.data_as_bytes()[*i..*i + TEAM_HEADER_LEN],
            )
            .expect("failed to parse TeamHeader");
            // team_header
            //     .team
            //     .inner()
            //     .dec_pe_ref_count(msg.src as usize, 1);
            // println!("team header: {:?}", team_header);
            *i += TEAM_HEADER_LEN;

            let (team, world) =
                ame.get_team_and_world(msg.src as usize, team_header.team, &lamellae);
            // ame.get_team_and_world(&team_header.team);

            for _am_batchs in 0..team_header.am_batch_cnts {
                let batched_am_header = BatchedAmHeader::try_read_from_bytes(
                    &ser_data.data_as_bytes()[*i..*i + BATCHED_AM_HEADER_LEN],
                )
                .expect("failed to parse BatchedAmHeader");
                // println!("batched am header: {:?}", batched_am_header);
                *i += BATCHED_AM_HEADER_LEN;
                for _am in 0..batched_am_header.am_cnt {
                    // println!(
                    //     "[{:?}] am cmd: {:?}",
                    //     std::thread::current().id(),
                    //     batched_am_header.cmd
                    // );
                    match batched_am_header.cmd {
                        Cmd::Am => {
                            self.exec_am(
                                msg,
                                &ser_data,
                                i,
                                lamellae,
                                ame,
                                batched_am_header.am_id,
                                world.clone(),
                                team.clone(),
                            );
                        }
                        Cmd::ReturnAm => {
                            self.exec_return_am(
                                msg,
                                &ser_data,
                                i,
                                lamellae,
                                ame,
                                batched_am_header.am_id,
                                world.clone(),
                                team.clone(),
                            )
                            .await;
                        }
                        _ => panic!("unhandled cmd"),
                    }
                }
            }
        }
        // println!(
        //     "[{:?}] return_ams: {:?}",
        //     std::thread::current().id(),
        //     return_ams
        // );
    }

    // //#[tracing::instrument(skip_all)]
    fn exec_am(
        &self,
        msg: &Msg,
        // data: &[u8],
        ser_data: &SerializedData,
        i: &mut usize,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
        am_id: AmId,
        world: Arc<LamellarTeam>,
        team: Arc<LamellarTeam>,
    ) {
        let data = ser_data.data_as_bytes();
        let req_id = ReqId::try_read_from_bytes(&data[*i..*i + REQ_ID_LEN])
            .expect("failed to parse ReqId");
        *i += REQ_ID_LEN;
        let am_len = usize::try_read_from_bytes(&data[*i..*i + AM_LEN_LEN])
            .expect("failed to parse am len");
        *i += AM_LEN_LEN;
        let am = AMS_EXECS.get(&am_id).unwrap()(&data[*i..*i + am_len], team.team.team_pe);
        *i += am_len;
        // println!("Team Batcher exec am");

        let req_data = ReqMetaData {
            src: team.team.world_pe,
            dst: Some(msg.src as usize),
            id: req_id,
            lamellae: lamellae.clone(),
            world: world.team.clone(),
            team: team.team.clone(),
            // team_addr: Darc::into_raw_team(team.team.clone()).addr(),
        };

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

    // //#[tracing::instrument(skip_all)]
    async fn exec_return_am(
        &self,
        msg: &Msg,
        // data: &[u8],
        ser_data: &SerializedData,
        i: &mut usize,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
        am_id: AmId,
        world: Arc<LamellarTeam>,
        team: Arc<LamellarTeam>,
    ) {
        let data = ser_data.data_as_bytes();
        // println!("[{:?}] exec_return_am", std::thread::current().id());
        let req_id = ReqId::try_read_from_bytes(&data[*i..*i + REQ_ID_LEN])
            .expect("failed to parse ReqId");
        *i += REQ_ID_LEN;
        let am_len = usize::try_read_from_bytes(&data[*i..*i + AM_LEN_LEN])
            .expect("failed to parse am len");
        *i += AM_LEN_LEN;
        let am = AMS_EXECS.get(&am_id).unwrap()(&data[*i..*i + am_len], team.team.team_pe);
        *i += am_len;

        let req_data = ReqMetaData {
            src: msg.src as usize,
            dst: Some(team.team.world_pe),
            id: req_id,
            lamellae: lamellae.clone(),
            world: world.team.clone(),
            team: team.team.clone(),
            // team_addr: Darc::into_raw_team(team.team.clone()).addr(),
        };

        ame.clone()
            .exec_local_am(req_data, am.as_local(), world, team)
            .await;
    }
}

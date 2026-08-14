use crate::{
    active_messaging::{
        batching::direct_batcher::{MyDataHeader, MyUnitHeader},
        registered_active_message::*,
        *,
    },
    lamellae::{comm::CommInfo, Lamellae, LamellaeUtil, SerializeHeader, SerializedData},
};
use batching::*;

use async_trait::async_trait;
use std::collections::HashMap;
use tracing::debug;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

const MAX_BATCH_SIZE: usize = 1_000_000;

// ---------------------------------------------------------------------------
// Wire-format zerocopy headers (team-grouped AM section)
// ---------------------------------------------------------------------------
//
// Layout of a VecTeamAmBatcher batch Vec<u8> after the transport framing
// (serialized Option<SerializeHeader>):
//
//   [Cmd::Am bytes]
//     [MyAmGroupHeader { num_teams: U64 }]
//     for each team:
//       [MyTeamHeader { team_addr: U64, num_am_types: U64 }]
//       for each am_type:
//         [MyAmTypeHeader { am_id: I32, am_cnt: U32 }]
//         for each am:
//           [MyAmReqHeader { req_id: U64, req_sub_id: U64 }]
//           <self-delimiting am bytes>
//
//   [Cmd::ReturnAm bytes]  (same structure, if any return AMs)
//     ...
//
//   For Data/Unit records (pre-serialized at add-time):
//   [Cmd::Data bytes][MyDataHeader][darcs][data]
//   [Cmd::Unit bytes][MyUnitHeader]

#[repr(C)]
#[derive(Debug, Copy, Clone, IntoBytes, FromBytes, KnownLayout, Immutable, Unaligned)]
struct MyAmGroupHeader {
    num_teams: U64<NativeEndian>,
}

#[repr(C)]
#[derive(Debug, Copy, Clone, IntoBytes, FromBytes, KnownLayout, Immutable, Unaligned)]
struct MyTeamHeader {
    team_addr: U64<NativeEndian>,
    num_am_types: U64<NativeEndian>,
}

#[repr(C)]
#[derive(Debug, Copy, Clone, IntoBytes, FromBytes, KnownLayout, Immutable, Unaligned)]
struct MyAmTypeHeader {
    am_id: I32<NativeEndian>,
    am_cnt: U32<NativeEndian>,
}

#[repr(C)]
#[derive(Debug, Copy, Clone, IntoBytes, FromBytes, KnownLayout, Immutable, Unaligned)]
struct MyAmReqHeader {
    req_id: U64<NativeEndian>,
    req_sub_id: U64<NativeEndian>,
    am_len: U64<NativeEndian>,
}

// ---------------------------------------------------------------------------
// Accumulation types
// ---------------------------------------------------------------------------

type TeamId = usize;
// am_id → Vec<(req_id, req_sub_id, serialized_am_bytes)>
type AmIdMap = HashMap<AmId, Vec<(u64, u64, Vec<u8>)>>;
type TeamAmMap = HashMap<TeamId, AmIdMap>;

#[derive(Default)]
struct SlotData {
    am_map: TeamAmMap,
    return_am_map: TeamAmMap,
    non_am: Vec<u8>, // pre-serialized Data/Unit records
}

struct VecTeamAmBatcherSlot {
    data: parking_lot::Mutex<SlotData>,
    size: Arc<AtomicUsize>,
    batch_id: Arc<AtomicUsize>,
}

impl VecTeamAmBatcherSlot {
    fn new() -> Self {
        VecTeamAmBatcherSlot {
            data: parking_lot::Mutex::new(SlotData::default()),
            size: Arc::new(AtomicUsize::new(0)),
            batch_id: Arc::new(AtomicUsize::new(0)),
        }
    }

    // Add an AM (or ReturnAm) to the slot.  Returns the previous total size.
    fn add_am(
        &self,
        team_addr: usize,
        am_id: AmId,
        req_id: u64,
        req_sub_id: u64,
        am_bytes: Vec<u8>,
        is_return: bool,
    ) -> usize {
        let am_size = am_bytes.len();
        let mut data = self.data.lock();
        let map = if is_return {
            &mut data.return_am_map
        } else {
            &mut data.am_map
        };
        let am_id_map = map.entry(team_addr).or_insert_with(HashMap::new);
        am_id_map
            .entry(am_id)
            .or_insert_with(Vec::new)
            .push((req_id, req_sub_id, am_bytes));
        // approximate: req header + am bytes (headers amortized)
        let delta = std::mem::size_of::<MyAmReqHeader>() + am_size;
        self.size.fetch_add(delta, Ordering::SeqCst)
    }

    // Append a pre-serialized Data/Unit record.  Returns the previous total size.
    fn add_non_am(&self, bytes: &[u8]) -> usize {
        let mut data = self.data.lock();
        data.non_am.extend_from_slice(bytes);
        self.size.fetch_add(bytes.len(), Ordering::SeqCst)
    }

    // Swap out the accumulated data and reset the slot.
    fn swap(&self) -> Option<SlotData> {
        let mut data = self.data.lock();
        if data.am_map.is_empty() && data.return_am_map.is_empty() && data.non_am.is_empty() {
            return None;
        }
        self.size.store(0, Ordering::SeqCst);
        self.batch_id.fetch_add(1, Ordering::SeqCst);
        let mut fresh = SlotData::default();
        std::mem::swap(&mut *data, &mut fresh);
        Some(fresh)
    }
}

// ---------------------------------------------------------------------------
// Batcher
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct VecTeamAmBatcher {
    pe_batch: Arc<Vec<Arc<VecTeamAmBatcherSlot>>>,
    header_bytes: Arc<Vec<u8>>, // serialized Option<SerializeHeader> for BatchedMsg
    stall_mark: Arc<AtomicUsize>,
    executor: Arc<Executor>,
}

impl std::fmt::Debug for VecTeamAmBatcherSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "VecTeamAmBatcherSlot(size={})",
            self.size.load(Ordering::Relaxed)
        )
    }
}

impl VecTeamAmBatcher {
    pub(crate) fn new(
        num_pes: usize,
        my_pe: usize,
        stall_mark: Arc<AtomicUsize>,
        executor: Arc<Executor>,
    ) -> Self {
        let header = SerializeHeader {
            msg: Msg {
                src: my_pe as u16,
                cmd: Cmd::BatchedMsg,
                padding: [0; 1],
            },
        };
        let header_bytes = Arc::new(header.as_bytes().to_vec());
        let pe_batch = (0..num_pes)
            .map(|_| Arc::new(VecTeamAmBatcherSlot::new()))
            .collect();
        VecTeamAmBatcher {
            pe_batch: Arc::new(pe_batch),
            header_bytes,
            stall_mark,
            executor,
        }
    }

    // After an add returns the previous size, decide whether to schedule a
    // flush task (prev == 0) or do an immediate over-max flush.
    fn post_add(
        &self,
        slot: Arc<VecTeamAmBatcherSlot>,
        lamellae: Arc<Lamellae>,
        pe: usize,
        prev_size: usize,
        stall_mark: usize,
    ) {
        let batch_id = slot.batch_id.load(Ordering::SeqCst);
        if prev_size == 0 {
            self.schedule_flush(slot, lamellae, pe, batch_id, stall_mark);
        } else if prev_size >= MAX_BATCH_SIZE {
            self.flush_now(slot, lamellae, pe);
        }
    }

    fn schedule_flush(
        &self,
        slot: Arc<VecTeamAmBatcherSlot>,
        lamellae: Arc<Lamellae>,
        pe: usize,
        batch_id: usize,
        mut stall_mark: usize,
    ) {
        let cur_stall_mark = self.stall_mark.clone();
        let header_bytes = Arc::clone(&self.header_bytes);
        self.executor.submit_io_task(async move {
            let mut timer = std::time::Instant::now();
            loop {
                let cur = cur_stall_mark.load(Ordering::Acquire);
                let size = slot.size.load(Ordering::SeqCst);
                if slot.batch_id.load(Ordering::SeqCst) != batch_id {
                    debug!(
                        "vec_team_am_batcher: batch {} for pe {} already sent",
                        batch_id, pe
                    );
                    return;
                }
                if cur != stall_mark || size >= MAX_BATCH_SIZE {
                    break;
                }
                stall_mark = cur;
                if timer.elapsed().as_secs_f32() > 10.0 {
                    debug!(
                        "vec_team_am_batcher: waiting batch {} pe {} size {}",
                        batch_id, pe, size
                    );
                    timer = std::time::Instant::now();
                }
                async_std::task::yield_now().await;
            }
            if slot.batch_id.load(Ordering::SeqCst) != batch_id {
                return;
            }
            if let Some(slot_data) = slot.swap() {
                let buf = VecTeamAmBatcher::serialize_slot(slot_data, &header_bytes);
                debug!(
                    "vec_team_am_batcher: flushing batch {} pe {} {} bytes",
                    batch_id,
                    pe,
                    buf.len()
                );
                lamellae.send_vec_to_pe_async(pe, buf).await;
            }
        });
    }

    fn flush_now(&self, slot: Arc<VecTeamAmBatcherSlot>, lamellae: Arc<Lamellae>, pe: usize) {
        let header_bytes = Arc::clone(&self.header_bytes);
        if let Some(slot_data) = slot.swap() {
            let buf = VecTeamAmBatcher::serialize_slot(slot_data, &header_bytes);
            debug!(
                "vec_team_am_batcher: over-max flush pe {} {} bytes",
                pe,
                buf.len()
            );
            self.executor.submit_io_task(async move {
                lamellae.send_vec_to_pe_async(pe, buf).await;
            });
        }
    }

    // Serialize the accumulated SlotData into a send-ready Vec<u8>.
    fn serialize_slot(slot_data: SlotData, header_bytes: &[u8]) -> Vec<u8> {
        let mut buf = header_bytes.to_vec();

        // Am group
        if !slot_data.am_map.is_empty() {
            buf.extend_from_slice(Cmd::Am.as_bytes());
            let group_header = MyAmGroupHeader {
                num_teams: U64::new(slot_data.am_map.len() as u64),
            };
            buf.extend_from_slice(group_header.as_bytes());
            for (team_addr, am_id_map) in &slot_data.am_map {
                let team_header = MyTeamHeader {
                    team_addr: U64::new(*team_addr as u64),
                    num_am_types: U64::new(am_id_map.len() as u64),
                };
                buf.extend_from_slice(team_header.as_bytes());
                for (am_id, ams) in am_id_map {
                    let type_header = MyAmTypeHeader {
                        am_id: I32::new(*am_id),
                        am_cnt: U32::new(ams.len() as u32),
                    };
                    buf.extend_from_slice(type_header.as_bytes());
                    for (req_id, req_sub_id, am_bytes) in ams {
                        let req_header = MyAmReqHeader {
                            req_id: U64::new(*req_id),
                            req_sub_id: U64::new(*req_sub_id),
                            am_len: U64::new(am_bytes.len() as u64),
                        };
                        buf.extend_from_slice(req_header.as_bytes());
                        buf.extend_from_slice(am_bytes);
                    }
                }
            }
        }

        // ReturnAm group
        if !slot_data.return_am_map.is_empty() {
            buf.extend_from_slice(Cmd::ReturnAm.as_bytes());
            let group_header = MyAmGroupHeader {
                num_teams: U64::new(slot_data.return_am_map.len() as u64),
            };
            buf.extend_from_slice(group_header.as_bytes());
            for (team_addr, am_id_map) in &slot_data.return_am_map {
                let team_header = MyTeamHeader {
                    team_addr: U64::new(*team_addr as u64),
                    num_am_types: U64::new(am_id_map.len() as u64),
                };
                buf.extend_from_slice(team_header.as_bytes());
                for (am_id, ams) in am_id_map {
                    let type_header = MyAmTypeHeader {
                        am_id: I32::new(*am_id),
                        am_cnt: U32::new(ams.len() as u32),
                    };
                    buf.extend_from_slice(type_header.as_bytes());
                    for (req_id, req_sub_id, am_bytes) in ams {
                        let req_header = MyAmReqHeader {
                            req_id: U64::new(*req_id),
                            req_sub_id: U64::new(*req_sub_id),
                            am_len: U64::new(am_bytes.len() as u64),
                        };
                        buf.extend_from_slice(req_header.as_bytes());
                        buf.extend_from_slice(am_bytes);
                    }
                }
            }
        }

        // Data/Unit (already Cmd-prefixed)
        buf.extend_from_slice(&slot_data.non_am);
        buf
    }
}

#[async_trait]
impl Batcher for VecTeamAmBatcher {
    async fn add_remote_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_bytes: Vec<u8>,
        stall_mark: usize,
    ) {
        if stall_mark == 0 {
            self.stall_mark.fetch_add(1, Ordering::Relaxed);
        }
        let team_addr = req_data.team.darc_addr();
        let req_id = req_data.id.id as u64;
        let req_sub_id = req_data.id.sub_id as u64;

        match req_data.dst {
            Some(pe) => {
                let mut darcs = vec![];
                am.ser(1, &mut darcs);
                let slot = Arc::clone(&self.pe_batch[pe]);
                let prev = slot.add_am(team_addr, am_id, req_id, req_sub_id, am_bytes, false);
                self.post_add(slot, req_data.lamellae.clone(), pe, prev, stall_mark);
            }
            None => {
                let darc_ser_cnt = match req_data.team.team_pe_id() {
                    Ok(_) => req_data.team.num_pes() - 1,
                    Err(_) => req_data.team.num_pes(),
                };
                let mut darcs = vec![];
                am.ser(darc_ser_cnt, &mut darcs);
                for pe in req_data
                    .team
                    .arch
                    .team_iter()
                    .filter(|pe| pe != &req_data.team.lamellae.comm().my_pe())
                {
                    let slot = Arc::clone(&self.pe_batch[pe]);
                    let prev = slot.add_am(
                        team_addr,
                        am_id,
                        req_id,
                        req_sub_id,
                        am_bytes.clone(),
                        false,
                    );
                    self.post_add(slot, req_data.lamellae.clone(), pe, prev, stall_mark);
                }
            }
        }
    }

    async fn add_return_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_bytes: Vec<u8>,
        stall_mark: usize,
    ) {
        if stall_mark == 0 {
            self.stall_mark.fetch_add(1, Ordering::Relaxed);
        }
        let team_addr = req_data.team.darc_addr();
        let req_id = req_data.id.id as u64;
        let req_sub_id = req_data.id.sub_id as u64;

        match req_data.dst {
            Some(pe) => {
                let mut darcs = vec![];
                am.ser(1, &mut darcs);
                let slot = Arc::clone(&self.pe_batch[pe]);
                let prev = slot.add_am(team_addr, am_id, req_id, req_sub_id, am_bytes, true);
                self.post_add(slot, req_data.lamellae.clone(), pe, prev, stall_mark);
            }
            None => {
                let darc_ser_cnt = match req_data.team.team_pe_id() {
                    Ok(_) => req_data.team.num_pes() - 1,
                    Err(_) => req_data.team.num_pes(),
                };
                let mut darcs = vec![];
                am.ser(darc_ser_cnt, &mut darcs);
                for pe in req_data
                    .team
                    .arch
                    .team_iter()
                    .filter(|pe| pe != &req_data.team.lamellae.comm().my_pe())
                {
                    let slot = Arc::clone(&self.pe_batch[pe]);
                    let prev =
                        slot.add_am(team_addr, am_id, req_id, req_sub_id, am_bytes.clone(), true);
                    self.post_add(slot, req_data.lamellae.clone(), pe, prev, stall_mark);
                }
            }
        }
    }

    async fn add_data_am_to_batch(
        &self,
        req_data: ReqMetaData,
        darc_bytes: Vec<u8>,
        data_bytes: Vec<u8>,
        stall_mark: usize,
    ) {
        if stall_mark == 0 {
            self.stall_mark.fetch_add(1, Ordering::Relaxed);
        }
        let serialized_darcs = darc_bytes;
        let data_header = MyDataHeader {
            req_id: U64::new(req_data.id.id as u64),
            req_sub_id: U64::new(req_data.id.sub_id as u64),
            size: U64::new(data_bytes.len() as u64),
            darc_list_size: U64::new(serialized_darcs.len() as u64),
        };
        let mut record = Vec::with_capacity(
            std::mem::size_of::<Cmd>()
                + std::mem::size_of::<MyDataHeader>()
                + serialized_darcs.len()
                + data_bytes.len(),
        );
        record.extend_from_slice(Cmd::Data.as_bytes());
        record.extend_from_slice(data_header.as_bytes());
        record.extend_from_slice(&serialized_darcs);
        record.extend_from_slice(&data_bytes);

        let pe = req_data.dst.expect("add_data_am_to_batch always has a dst");
        let slot = Arc::clone(&self.pe_batch[pe]);
        let prev = slot.add_non_am(&record);
        self.post_add(slot, req_data.lamellae.clone(), pe, prev, stall_mark);
    }

    async fn add_unit_am_to_batch(&self, req_data: ReqMetaData, stall_mark: usize) {
        if stall_mark == 0 {
            self.stall_mark.fetch_add(1, Ordering::Relaxed);
        }
        let unit_header = MyUnitHeader {
            req_id: U64::new(req_data.id.id as u64),
            req_sub_id: U64::new(req_data.id.sub_id as u64),
        };
        let mut record =
            Vec::with_capacity(std::mem::size_of::<Cmd>() + std::mem::size_of::<MyUnitHeader>());
        record.extend_from_slice(Cmd::Unit.as_bytes());
        record.extend_from_slice(unit_header.as_bytes());

        match req_data.dst {
            Some(pe) => {
                let slot = Arc::clone(&self.pe_batch[pe]);
                let prev = slot.add_non_am(&record);
                self.post_add(slot, req_data.lamellae.clone(), pe, prev, stall_mark);
            }
            None => {
                for pe in req_data
                    .team
                    .arch
                    .team_iter()
                    .filter(|pe| pe != &req_data.team.lamellae.comm().my_pe())
                {
                    let slot = Arc::clone(&self.pe_batch[pe]);
                    let prev = slot.add_non_am(&record);
                    self.post_add(slot, req_data.lamellae.clone(), pe, prev, stall_mark);
                }
            }
        }
    }

    async fn exec_batched_msg(
        &self,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
    ) {
        let src = msg.src as usize;
        let data_bytes = ser_data.data_as_bytes();
        let cmd_size = std::mem::size_of::<Cmd>();
        let mut offset = 0;
        while offset < data_bytes.len() {
            let cmd = Cmd::try_ref_from_bytes(&data_bytes[offset..offset + cmd_size])
                .expect("VecTeamAmBatcher: failed to parse Cmd");
            offset += cmd_size;
            offset += match cmd {
                Cmd::Am => exec_am_group(&data_bytes[offset..], src, lamellae, ame, &self.executor),
                Cmd::ReturnAm => {
                    exec_return_am_group(&data_bytes[offset..], src, lamellae, ame).await
                }
                Cmd::Data => exec_data_am_inner(src, &data_bytes[offset..], ame),
                Cmd::Unit => exec_unit_am_inner(src, &data_bytes[offset..], ame),
                Cmd::BatchedMsg => {
                    unreachable!("VecTeamAmBatcher: unexpected BatchedMsg in payload")
                }
            };
        }
    }
}

// ---------------------------------------------------------------------------
// Exec helpers
// ---------------------------------------------------------------------------

fn exec_am_group(
    data: &[u8],
    src: usize,
    lamellae: &Arc<Lamellae>,
    ame: &RegisteredActiveMessages,
    executor: &Arc<Executor>,
) -> usize {
    let mut offset = 0;
    let group_header = MyAmGroupHeader::ref_from_bytes(
        &data[offset..offset + std::mem::size_of::<MyAmGroupHeader>()],
    )
    .expect("VecTeamAmBatcher: failed to parse MyAmGroupHeader");
    offset += std::mem::size_of::<MyAmGroupHeader>();
    let num_teams = group_header.num_teams.get() as usize;

    for _ in 0..num_teams {
        let team_header = MyTeamHeader::ref_from_bytes(
            &data[offset..offset + std::mem::size_of::<MyTeamHeader>()],
        )
        .expect("VecTeamAmBatcher: failed to parse MyTeamHeader");
        offset += std::mem::size_of::<MyTeamHeader>();
        let (team, world) =
            ame.get_team_and_world(src, team_header.team_addr.get() as usize, lamellae);
        let num_am_types = team_header.num_am_types.get() as usize;

        for _ in 0..num_am_types {
            let type_header = MyAmTypeHeader::ref_from_bytes(
                &data[offset..offset + std::mem::size_of::<MyAmTypeHeader>()],
            )
            .expect("VecTeamAmBatcher: failed to parse MyAmTypeHeader");
            offset += std::mem::size_of::<MyAmTypeHeader>();
            let am_id = type_header.am_id.get();
            let am_cnt = type_header.am_cnt.get() as usize;

            for _ in 0..am_cnt {
                let req_header = MyAmReqHeader::ref_from_bytes(
                    &data[offset..offset + std::mem::size_of::<MyAmReqHeader>()],
                )
                .expect("VecTeamAmBatcher: failed to parse MyAmReqHeader");
                offset += std::mem::size_of::<MyAmReqHeader>();
                let am_len = req_header.am_len.get() as usize;
                let am = AMS_EXECS.get(&am_id).unwrap()(
                    &data[offset..offset + am_len],
                    team.team.team_pe,
                );
                offset += am_len;

                let team_arc = team.clone();
                let world_arc = world.clone();
                let req_data = ReqMetaData {
                    src: team_arc.team.world_pe,
                    dst: Some(src),
                    id: ReqId {
                        id: req_header.req_id.get() as usize,
                        sub_id: req_header.req_sub_id.get() as usize,
                    },
                    lamellae: lamellae.clone(),
                    world: world_arc.team.clone(),
                    team: team_arc.team.clone(),
                };
                let ame = ame.clone();
                world_arc.team.world_counters.inc_outstanding(1);
                team_arc.team.team_counters.inc_outstanding(1);
                executor.submit_task(async move {
                    let am = match am
                        .exec(
                            team_arc.team.world_pe,
                            team_arc.team.num_world_pes,
                            false,
                            world_arc.clone(),
                            team_arc.clone(),
                        )
                        .await
                    {
                        LamellarReturn::Unit => Am::Unit(req_data),
                        LamellarReturn::RemoteData(data) => Am::Data(req_data, data),
                        LamellarReturn::RemoteAm(am) => Am::Return(req_data, am),
                        LamellarReturn::LocalData(_) | LamellarReturn::LocalAm(_) => {
                            panic!("Should not be returning local data or AM from remote am");
                        }
                    };
                    world_arc.team.world_counters.dec_outstanding(1);
                    team_arc.team.team_counters.dec_outstanding(1);
                    ame.process_msg(am, 0, false).await;
                });
            }
        }
    }
    offset
}

async fn exec_return_am_group(
    data: &[u8],
    src: usize,
    lamellae: &Arc<Lamellae>,
    ame: &RegisteredActiveMessages,
) -> usize {
    let mut offset = 0;
    let group_header = MyAmGroupHeader::ref_from_bytes(
        &data[offset..offset + std::mem::size_of::<MyAmGroupHeader>()],
    )
    .expect("VecTeamAmBatcher: failed to parse MyAmGroupHeader (return)");
    offset += std::mem::size_of::<MyAmGroupHeader>();
    let num_teams = group_header.num_teams.get() as usize;

    for _ in 0..num_teams {
        let team_header = MyTeamHeader::ref_from_bytes(
            &data[offset..offset + std::mem::size_of::<MyTeamHeader>()],
        )
        .expect("VecTeamAmBatcher: failed to parse MyTeamHeader (return)");
        offset += std::mem::size_of::<MyTeamHeader>();
        let (team, world) =
            ame.get_team_and_world(src, team_header.team_addr.get() as usize, lamellae);
        let num_am_types = team_header.num_am_types.get() as usize;

        for _ in 0..num_am_types {
            let type_header = MyAmTypeHeader::ref_from_bytes(
                &data[offset..offset + std::mem::size_of::<MyAmTypeHeader>()],
            )
            .expect("VecTeamAmBatcher: failed to parse MyAmTypeHeader (return)");
            offset += std::mem::size_of::<MyAmTypeHeader>();
            let am_id = type_header.am_id.get();
            let am_cnt = type_header.am_cnt.get() as usize;

            for _ in 0..am_cnt {
                let req_header = MyAmReqHeader::ref_from_bytes(
                    &data[offset..offset + std::mem::size_of::<MyAmReqHeader>()],
                )
                .expect("VecTeamAmBatcher: failed to parse MyAmReqHeader (return)");
                offset += std::mem::size_of::<MyAmReqHeader>();
                let am_len = req_header.am_len.get() as usize;
                let am = AMS_EXECS.get(&am_id).unwrap()(
                    &data[offset..offset + am_len],
                    team.team.team_pe,
                );
                offset += am_len;

                let req_data = ReqMetaData {
                    src,
                    dst: Some(team.team.world_pe),
                    id: ReqId {
                        id: req_header.req_id.get() as usize,
                        sub_id: req_header.req_sub_id.get() as usize,
                    },
                    lamellae: lamellae.clone(),
                    world: world.team.clone(),
                    team: team.team.clone(),
                };
                ame.clone()
                    .exec_local_am(req_data, am.as_local(), world.clone(), team.clone())
                    .await;
            }
        }
    }
    offset
}

fn exec_data_am_inner(src: usize, data_bytes: &[u8], ame: &RegisteredActiveMessages) -> usize {
    let mut offset = 0;
    let data_header = MyDataHeader::ref_from_bytes(
        &data_bytes[offset..offset + std::mem::size_of::<MyDataHeader>()],
    )
    .expect("VecTeamAmBatcher: failed to parse MyDataHeader");
    offset += std::mem::size_of::<MyDataHeader>();
    let darc_list_size = data_header.darc_list_size.get() as usize;
    let darc_list: Vec<RemotePtr> =
        crate::deserialize(&data_bytes[offset..offset + darc_list_size], false).unwrap();
    offset += darc_list_size;
    let data_size = data_header.size.get() as usize;
    let payload = &data_bytes[offset..offset + data_size];
    offset += data_size;
    let req_id = ReqId {
        id: data_header.req_id.get() as usize,
        sub_id: data_header.req_sub_id.get() as usize,
    };
    ame.send_data_to_user_handle(
        req_id,
        src,
        InternalResult::NewRemote(payload.to_vec(), darc_list),
    );
    offset
}

fn exec_unit_am_inner(src: usize, data_bytes: &[u8], ame: &RegisteredActiveMessages) -> usize {
    let unit_header =
        MyUnitHeader::ref_from_bytes(&data_bytes[..std::mem::size_of::<MyUnitHeader>()])
            .expect("VecTeamAmBatcher: failed to parse MyUnitHeader");
    let req_id = ReqId {
        id: unit_header.req_id.get() as usize,
        sub_id: unit_header.req_sub_id.get() as usize,
    };
    ame.send_data_to_user_handle(req_id, src, InternalResult::Unit);
    std::mem::size_of::<MyUnitHeader>()
}

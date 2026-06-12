use crate::{
    active_messaging::{
        batching::direct_batcher::{MyAmHeader, MyDataHeader, MyUnitHeader},
        registered_active_message::*,
        *,
    },
    lamellae::{Lamellae, SerializedData, SerializeHeader},
};
use batching::*;

use async_trait::async_trait;
use tracing::debug;

const MAX_BATCH_SIZE: usize = 1_000_000;

// Per-PE batch slot: (buffer, data_byte_count, batch_id).
// The buffer is always pre-seeded with the serialized Option<SerializeHeader>
// so send_vec_to_pe_async can parse the framing without extra allocation.
type BatchSlot = Arc<parking_lot::Mutex<(Vec<u8>, usize, usize)>>;

#[derive(Debug)]
struct VecSimpleBatcherInner {
    pe_batch: Vec<BatchSlot>,
    header_bytes: Arc<Vec<u8>>, // serialized Option<SerializeHeader> for BatchedMsg
    stall_mark: Arc<AtomicUsize>,
}

#[derive(Debug, Clone)]
pub(crate) struct VecSimpleBatcher {
    inner: Arc<VecSimpleBatcherInner>,
    executor: Arc<Executor>,
}

impl VecSimpleBatcher {
    pub(crate) fn new(
        num_pes: usize,
        my_pe: usize,
        stall_mark: Arc<AtomicUsize>,
        executor: Arc<Executor>,
    ) -> Self {
        let header = Some(SerializeHeader {
            msg: Msg {
                src: my_pe as u16,
                cmd: Cmd::BatchedMsg,
                padding: [0; 1],
            },
        });
        let header_bytes = Arc::new(crate::serialize(&header, false).unwrap());
        let mut pe_batch = Vec::with_capacity(num_pes);
        for _ in 0..num_pes {
            pe_batch.push(Arc::new(parking_lot::Mutex::new((
                (*header_bytes).clone(),
                0,
                0,
            ))));
        }
        VecSimpleBatcher {
            inner: Arc::new(VecSimpleBatcherInner {
                pe_batch,
                header_bytes,
                stall_mark,
            }),
            executor,
        }
    }

    // Append slices to the per-PE buffer.  Returns (prev_data_bytes, batch_id)
    // so the caller knows whether to schedule a new flush task.
    fn append(slot: &BatchSlot, payload: &[&[u8]], payload_len: usize) -> (usize, usize) {
        let mut guard = slot.lock();
        let (buf, data_bytes, batch_id) = &mut *guard;
        let prev = *data_bytes;
        for p in payload {
            buf.extend_from_slice(p);
        }
        *data_bytes += payload_len;
        (prev, *batch_id)
    }

    // Swap out the buffer for a fresh header-seeded one while holding the lock.
    // Returns None if the batch has already been swapped (batch_id mismatch or
    // empty).
    fn take_batch(
        slot: &BatchSlot,
        header_bytes: &Arc<Vec<u8>>,
        expected_batch_id: usize,
    ) -> Option<Vec<u8>> {
        let mut guard = slot.lock();
        let (buf, data_bytes, batch_id) = &mut *guard;
        if *batch_id != expected_batch_id || *data_bytes == 0 {
            return None;
        }
        let mut fresh = (**header_bytes).clone();
        std::mem::swap(buf, &mut fresh);
        *data_bytes = 0;
        *batch_id += 1;
        Some(fresh)
    }

    // Schedule an IO task that waits for a stall-mark change (or MAX_BATCH_SIZE)
    // then flushes the batch for the given PE.
    fn schedule_flush(
        &self,
        slot: BatchSlot,
        header_bytes: Arc<Vec<u8>>,
        lamellae: Arc<Lamellae>,
        pe: usize,
        batch_id: usize,
        mut stall_mark: usize,
    ) {
        let cur_stall_mark = self.inner.stall_mark.clone();
        self.executor.submit_io_task(async move {
            let mut timer = std::time::Instant::now();
            loop {
                let cur = cur_stall_mark.load(Ordering::Acquire);
                let batch_size = {
                    let guard = slot.lock();
                    let (_, data_bytes, current_batch_id) = &*guard;
                    if *current_batch_id != batch_id {
                        debug!("vec_simple_batcher: batch {} for pe {} already sent", batch_id, pe);
                        return;
                    }
                    *data_bytes
                };
                if cur != stall_mark || batch_size >= MAX_BATCH_SIZE {
                    break;
                }
                stall_mark = cur;
                if timer.elapsed().as_secs_f32() > 10.0 {
                    debug!(
                        "vec_simple_batcher: waiting to flush batch {} to pe {}, size {}",
                        batch_id, pe, batch_size
                    );
                    timer = std::time::Instant::now();
                }
                async_std::task::yield_now().await;
            }
            if let Some(ready) = VecSimpleBatcher::take_batch(&slot, &header_bytes, batch_id) {
                debug!("vec_simple_batcher: flushing batch {} to pe {}, {} bytes", batch_id, pe, ready.len());
                lamellae.send_vec_to_pe_async(pe, ready).await;
            }
        });
    }

    // If the batch just crossed MAX_BATCH_SIZE, flush immediately without
    // spawning an extra IO task — caller already holds the slot via the append.
    fn flush_if_over_max(
        &self,
        slot: BatchSlot,
        header_bytes: Arc<Vec<u8>>,
        lamellae: Arc<Lamellae>,
        pe: usize,
        batch_id: usize,
    ) {
        if let Some(ready) = VecSimpleBatcher::take_batch(&slot, &header_bytes, batch_id) {
            debug!("vec_simple_batcher: over-max flush batch {} to pe {}, {} bytes", batch_id, pe, ready.len());
            self.executor.submit_io_task(async move {
                lamellae.send_vec_to_pe_async(pe, ready).await;
            });
        }
    }

    // Common post-append dispatch: schedule a new IO task if this is the first
    // payload in a fresh batch, or flush immediately if MAX_BATCH_SIZE exceeded.
    fn post_append(
        &self,
        slot: BatchSlot,
        lamellae: &Arc<Lamellae>,
        pe: usize,
        prev_data_bytes: usize,
        new_data_bytes: usize,
        batch_id: usize,
        stall_mark: usize,
    ) {
        let header_arc = Arc::clone(&self.inner.header_bytes);
        if prev_data_bytes == 0 {
            self.schedule_flush(slot, header_arc, lamellae.clone(), pe, batch_id, stall_mark);
        } else if new_data_bytes >= MAX_BATCH_SIZE {
            self.flush_if_over_max(slot, header_arc, lamellae.clone(), pe, batch_id);
        }
    }
}

#[async_trait]
impl Batcher for VecSimpleBatcher {
    async fn add_remote_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        _am_size: usize,
        stall_mark: usize,
    ) {
        if stall_mark == 0 {
            self.inner.stall_mark.fetch_add(1, Ordering::Relaxed);
        }
        let am_bytes = am.serialize();
        let cmd_bytes = Cmd::Am.as_bytes();
        let am_header = MyAmHeader {
            am_id: I32::new(am_id),
            req_id: U64::new(req_data.id.id as u64),
            req_sub_id: U64::new(req_data.id.sub_id as u64),
            data_len: U64::new(am_bytes.len() as u64),
            team_addr: U64::new(req_data.team.darc_addr() as u64),
        };
        let am_header_bytes = am_header.as_bytes();
        let payload_len = cmd_bytes.len() + am_header_bytes.len() + am_bytes.len();

        match req_data.dst {
            Some(pe) => {
                let mut darcs = vec![];
                am.ser(1, &mut darcs);
                let slot = Arc::clone(&self.inner.pe_batch[pe]);
                let (prev, batch_id) =
                    VecSimpleBatcher::append(&slot, &[cmd_bytes, am_header_bytes, &am_bytes], payload_len);
                self.post_append(slot, &req_data.lamellae, pe, prev, prev + payload_len, batch_id, stall_mark);
            }
            None => {
                let darc_ser_cnt = match req_data.team.team_pe_id() {
                    Ok(_) => req_data.team.num_pes() - 1,
                    Err(_) => req_data.team.num_pes(),
                };
                let mut darcs = vec![];
                am.ser(darc_ser_cnt, &mut darcs);
                for pe in req_data.team.arch.team_iter()
                    .filter(|pe| pe != &req_data.team.lamellae.comm().my_pe())
                {
                    let slot = Arc::clone(&self.inner.pe_batch[pe]);
                    let (prev, batch_id) = VecSimpleBatcher::append(
                        &slot,
                        &[cmd_bytes, am_header_bytes, &am_bytes],
                        payload_len,
                    );
                    self.post_append(slot, &req_data.lamellae, pe, prev, prev + payload_len, batch_id, stall_mark);
                }
            }
        }
    }

    async fn add_return_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        _am_size: usize,
        stall_mark: usize,
    ) {
        if stall_mark == 0 {
            self.inner.stall_mark.fetch_add(1, Ordering::Relaxed);
        }
        let am_bytes = am.serialize();
        let cmd_bytes = Cmd::ReturnAm.as_bytes();
        let am_header = MyAmHeader {
            am_id: I32::new(am_id),
            req_id: U64::new(req_data.id.id as u64),
            req_sub_id: U64::new(req_data.id.sub_id as u64),
            data_len: U64::new(am_bytes.len() as u64),
            team_addr: U64::new(req_data.team.darc_addr() as u64),
        };
        let am_header_bytes = am_header.as_bytes();
        let payload_len = cmd_bytes.len() + am_header_bytes.len() + am_bytes.len();

        match req_data.dst {
            Some(pe) => {
                let mut darcs = vec![];
                am.ser(1, &mut darcs);
                let slot = Arc::clone(&self.inner.pe_batch[pe]);
                let (prev, batch_id) =
                    VecSimpleBatcher::append(&slot, &[cmd_bytes, am_header_bytes, &am_bytes], payload_len);
                self.post_append(slot, &req_data.lamellae, pe, prev, prev + payload_len, batch_id, stall_mark);
            }
            None => {
                let darc_ser_cnt = match req_data.team.team_pe_id() {
                    Ok(_) => req_data.team.num_pes() - 1,
                    Err(_) => req_data.team.num_pes(),
                };
                let mut darcs = vec![];
                am.ser(darc_ser_cnt, &mut darcs);
                for pe in req_data.team.arch.team_iter()
                    .filter(|pe| pe != &req_data.team.lamellae.comm().my_pe())
                {
                    let slot = Arc::clone(&self.inner.pe_batch[pe]);
                    let (prev, batch_id) = VecSimpleBatcher::append(
                        &slot,
                        &[cmd_bytes, am_header_bytes, &am_bytes],
                        payload_len,
                    );
                    self.post_append(slot, &req_data.lamellae, pe, prev, prev + payload_len, batch_id, stall_mark);
                }
            }
        }
    }

    async fn add_data_am_to_batch(
        &self,
        req_data: ReqMetaData,
        data: LamellarResultArc,
        _data_size: usize,
        stall_mark: usize,
    ) {
        if stall_mark == 0 {
            self.inner.stall_mark.fetch_add(1, Ordering::Relaxed);
        }
        let mut darcs = vec![];
        data.ser(1, &mut darcs);
        let serialized_darcs = crate::serialize(&darcs, false).unwrap();
        let data_bytes = data.serialize();
        let cmd_bytes = Cmd::Data.as_bytes();
        let data_header = MyDataHeader {
            req_id: U64::new(req_data.id.id as u64),
            req_sub_id: U64::new(req_data.id.sub_id as u64),
            size: U64::new(data_bytes.len() as u64),
            darc_list_size: U64::new(serialized_darcs.len() as u64),
        };
        let data_header_bytes = data_header.as_bytes();
        let payload_len =
            cmd_bytes.len() + data_header_bytes.len() + serialized_darcs.len() + data_bytes.len();

        let pe = req_data.dst.expect("add_data_am_to_batch always has a dst");
        let slot = Arc::clone(&self.inner.pe_batch[pe]);
        let (prev, batch_id) = VecSimpleBatcher::append(
            &slot,
            &[cmd_bytes, data_header_bytes, &serialized_darcs, &data_bytes],
            payload_len,
        );
        self.post_append(slot, &req_data.lamellae, pe, prev, prev + payload_len, batch_id, stall_mark);
    }

    async fn add_unit_am_to_batch(&self, req_data: ReqMetaData, stall_mark: usize) {
        if stall_mark == 0 {
            self.inner.stall_mark.fetch_add(1, Ordering::Relaxed);
        }
        let cmd_bytes = Cmd::Unit.as_bytes();
        let unit_header = MyUnitHeader {
            req_id: U64::new(req_data.id.id as u64),
            req_sub_id: U64::new(req_data.id.sub_id as u64),
        };
        let unit_header_bytes = unit_header.as_bytes();
        let payload_len = cmd_bytes.len() + unit_header_bytes.len();

        match req_data.dst {
            Some(pe) => {
                let slot = Arc::clone(&self.inner.pe_batch[pe]);
                let (prev, batch_id) =
                    VecSimpleBatcher::append(&slot, &[cmd_bytes, unit_header_bytes], payload_len);
                self.post_append(slot, &req_data.lamellae, pe, prev, prev + payload_len, batch_id, stall_mark);
            }
            None => {
                for pe in req_data.team.arch.team_iter()
                    .filter(|pe| pe != &req_data.team.lamellae.comm().my_pe())
                {
                    let slot = Arc::clone(&self.inner.pe_batch[pe]);
                    let (prev, batch_id) = VecSimpleBatcher::append(
                        &slot,
                        &[cmd_bytes, unit_header_bytes],
                        payload_len,
                    );
                    self.post_append(slot, &req_data.lamellae, pe, prev, prev + payload_len, batch_id, stall_mark);
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
        // Wire format is identical to DirectBatcher: [Cmd][My*Header][payload]...
        let src = msg.src as usize;
        let data_bytes = ser_data.data_as_bytes();
        let mut offset = 0;
        while offset < data_bytes.len() {
            let cmd_bytes = &data_bytes[offset..offset + std::mem::size_of::<Cmd>()];
            let cmd = Cmd::try_ref_from_bytes(cmd_bytes).expect("VecSimpleBatcher: failed to parse Cmd");
            offset += std::mem::size_of::<Cmd>();
            offset += match cmd {
                Cmd::Am => exec_am_inner(src, &data_bytes[offset..], lamellae, ame, &self.executor),
                Cmd::ReturnAm => exec_return_am_inner(src, &data_bytes[offset..], lamellae, ame).await,
                Cmd::Data => exec_data_am_inner(src, &data_bytes[offset..], ame),
                Cmd::Unit => exec_unit_am_inner(src, &data_bytes[offset..], ame),
                _ => unreachable!("VecSimpleBatcher: unexpected cmd in batched msg"),
            };
        }
    }
}

// ---------------------------------------------------------------------------
// Exec helpers — same logic as DirectBatcher's private methods but as free
// functions so VecSimpleBatcher can share them without depending on a
// DirectBatcher instance.
// ---------------------------------------------------------------------------

fn exec_am_inner(
    src: usize,
    data_bytes: &[u8],
    lamellae: &Arc<Lamellae>,
    ame: &RegisteredActiveMessages,
    executor: &Arc<Executor>,
) -> usize {
    let mut offset = 0;
    let header_bytes = &data_bytes[offset..offset + std::mem::size_of::<MyAmHeader>()];
    let am_header = MyAmHeader::ref_from_bytes(header_bytes)
        .expect("VecSimpleBatcher: failed to parse MyAmHeader");
    offset += std::mem::size_of::<MyAmHeader>();
    let data_len = am_header.data_len.get() as usize;
    let am_data_bytes = &data_bytes[offset..offset + data_len];
    offset += data_len;
    let (team, world) = ame.get_team_and_world(src, am_header.team_addr.get() as usize, lamellae);
    let am = AMS_EXECS.get(&am_header.am_id.get()).unwrap()(am_data_bytes, team.team.team_pe);
    let req_data = ReqMetaData {
        src: team.team.world_pe,
        dst: Some(src),
        id: ReqId {
            id: am_header.req_id.get() as usize,
            sub_id: am_header.req_sub_id.get() as usize,
        },
        lamellae: lamellae.clone(),
        world: world.team.clone(),
        team: team.team.clone(),
    };
    let ame = ame.clone();
    world.team.world_counters.inc_outstanding(1);
    team.team.team_counters.inc_outstanding(1);
    executor.submit_task(async move {
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
                panic!("Should not be returning local data or AM from remote am");
            }
        };
        world.team.world_counters.dec_outstanding(1);
        team.team.team_counters.dec_outstanding(1);
        ame.process_msg(am, 0, false).await;
    });
    offset
}

async fn exec_return_am_inner(
    src: usize,
    data_bytes: &[u8],
    lamellae: &Arc<Lamellae>,
    ame: &RegisteredActiveMessages,
) -> usize {
    let mut offset = 0;
    let header_bytes = &data_bytes[offset..offset + std::mem::size_of::<MyAmHeader>()];
    let am_header = MyAmHeader::ref_from_bytes(header_bytes)
        .expect("VecSimpleBatcher: failed to parse MyAmHeader for return am");
    offset += std::mem::size_of::<MyAmHeader>();
    let data_len = am_header.data_len.get() as usize;
    let am_data_bytes = &data_bytes[offset..offset + data_len];
    offset += data_len;
    let (team, world) = ame.get_team_and_world(src, am_header.team_addr.get() as usize, lamellae);
    let am = AMS_EXECS.get(&am_header.am_id.get()).unwrap()(am_data_bytes, team.team.team_pe);
    let req_data = ReqMetaData {
        src,
        dst: Some(team.team.world_pe),
        id: ReqId {
            id: am_header.req_id.get() as usize,
            sub_id: am_header.req_sub_id.get() as usize,
        },
        lamellae: lamellae.clone(),
        world: world.team.clone(),
        team: team.team.clone(),
    };
    ame.clone().exec_local_am(req_data, am.as_local(), world, team).await;
    offset
}

fn exec_data_am_inner(src: usize, data_bytes: &[u8], ame: &RegisteredActiveMessages) -> usize {
    let mut offset = 0;
    let header_bytes = &data_bytes[offset..offset + std::mem::size_of::<MyDataHeader>()];
    let data_header = MyDataHeader::ref_from_bytes(header_bytes)
        .expect("VecSimpleBatcher: failed to parse MyDataHeader");
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
    ame.send_data_to_user_handle(req_id, src, InternalResult::NewRemote(payload.to_vec(), darc_list));
    offset
}

fn exec_unit_am_inner(src: usize, data_bytes: &[u8], ame: &RegisteredActiveMessages) -> usize {
    let header_bytes = &data_bytes[..std::mem::size_of::<MyUnitHeader>()];
    let unit_header = MyUnitHeader::ref_from_bytes(header_bytes)
        .expect("VecSimpleBatcher: failed to parse MyUnitHeader");
    let req_id = ReqId {
        id: unit_header.req_id.get() as usize,
        sub_id: unit_header.req_sub_id.get() as usize,
    };
    ame.send_data_to_user_handle(req_id, src, InternalResult::Unit);
    std::mem::size_of::<MyUnitHeader>()
}

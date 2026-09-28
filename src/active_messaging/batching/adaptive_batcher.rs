// AdaptiveBatcher: backend-agnostic batcher that sends Cmd::Stream frames (wire.rs).
//
// Each destination has one staging buffer. A producer appends its record under a short
// lock; if no send to that destination is in flight it becomes the flusher and sends
// inline (no spawn, no added latency when idle). While a send is in flight, records
// coalesce and go out as the next frame. There is no stall_mark heuristic: aggregation
// comes purely from sends taking time.
//
// Stream mode (`LAMELLAR_BATCHER=stream`) first tries to encode each point-to-point
// record straight into the lamellae's per-pair byte ring (shmem-opt only), with no
// staging lock, frame copy, flush task or mailbox slot. A full ring, an oversized record
// or a backend without streams falls through to the staging path above, which doubles
// as the spill list.

use crate::{
    active_messaging::{registered_active_message::*, *},
    config,
    lamellae::{Lamellae, LamellaeUtil, SerializeHeader},
    lamellar_arch::LamellarArchRT,
};
use batching::*;

use async_trait::async_trait;
use futures_util::future::BoxFuture;
use parking_lot::Mutex;
use std::sync::atomic::AtomicBool;

use super::wire;

/// Staged bytes beyond which a producer sends the buffer itself instead of waiting on
/// the in-flight flusher (`LAMELLAR_ADAPTIVE_HIGH_WATER`). This also sets the typical
/// frame size under load. Network sends pay a large per-message cost, so they want big
/// frames: 4M is 1.2-3x 256K at 4K-64K AMs on libfabric/ucx. Plain shmem copies each
/// frame twice and is best at 256K. shmem-opt is best at 1M.
fn high_water(backend: crate::lamellae::Backend) -> usize {
    use crate::lamellae::Backend;
    if let Some(hw) = std::env::var("LAMELLAR_ADAPTIVE_HIGH_WATER")
        .ok()
        .and_then(|v| v.parse().ok())
    {
        return hw;
    }
    match backend {
        Backend::Shmem | Backend::Local => 256 * 1024,
        #[cfg(feature = "enable-shmem-opt")]
        Backend::ShmemOpt => 1024 * 1024,
        #[allow(unreachable_patterns)]
        _ => 4 * 1024 * 1024,
    }
}

/// Frames a flusher sends back-to-back before handing off to an io task, so a producer
/// is not held hostage by other producers' traffic.
const HANDOFF_ROUNDS: usize = 4;

#[derive(Default)]
struct Stage {
    buf: Vec<u8>,
    // AMs whose darcs were serialized into buf; kept alive until the frame is sent
    keep: Vec<LamellarArcAm>,
    // taken together with buf so the batcher never pins the lamellae while idle
    ctx: Option<(Arc<Lamellae>, Arc<LamellarArchRT>, usize)>,
    // previously sent buffers, reused so the staging Vecs do not regrow every frame
    spare: Vec<u8>,
    spare_keep: Vec<LamellarArcAm>,
}

struct Frame {
    buf: Vec<u8>,
    keep: Vec<LamellarArcAm>,
    ctx: (Arc<Lamellae>, Arc<LamellarArchRT>, usize),
}

impl Stage {
    #[inline]
    fn take_frame(&mut self) -> Option<Frame> {
        if self.buf.is_empty() {
            return None;
        }
        let spare = std::mem::take(&mut self.spare);
        let spare_keep = std::mem::take(&mut self.spare_keep);
        Some(Frame {
            buf: std::mem::replace(&mut self.buf, spare),
            keep: std::mem::replace(&mut self.keep, spare_keep),
            ctx: self.ctx.take().expect("staged records without a send context"),
        })
    }

    #[inline]
    fn set_ctx(&mut self, req_data: &ReqMetaData) {
        if self.ctx.is_none() {
            self.ctx = Some((
                req_data.lamellae.clone(),
                req_data.team.arch.clone(),
                req_data.team.world_pe,
            ));
        }
    }
}

#[derive(Default)]
struct Dest {
    stage: Mutex<Stage>,
    inflight: AtomicBool,
}

impl Dest {
    /// Called with the stage lock held right after an append: when the flusher is busy and
    /// the backlog reached HIGH_WATER, takes the whole backlog so the caller ships it. Taking
    /// it under the same lock as the check keeps every backpressure frame >= HIGH_WATER
    /// (a later take could find only the few records appended since another taker ran).
    #[inline]
    fn take_overflow(&self, s: &mut Stage, high_water: usize) -> Option<Frame> {
        if s.buf.len() >= high_water && self.inflight.load(Ordering::SeqCst) {
            s.take_frame()
        } else {
            None
        }
    }

    async fn send(&self, dst: usize, frame: Frame) {
        let Frame { mut buf, mut keep, ctx } = frame;
        send_frame(Some(dst), &buf, ctx).await;
        keep.clear();
        buf.clear();
        let mut s = self.stage.lock();
        if s.spare.capacity() == 0 {
            s.spare = buf;
        }
        if s.spare_keep.capacity() == 0 {
            s.spare_keep = keep;
        }
    }
}

#[derive(Clone)]
pub(crate) struct AdaptiveBatcher {
    dests: Arc<Vec<Dest>>,
    executor: Arc<Executor>,
    stream: bool,
    high_water: usize,
}

impl std::fmt::Debug for AdaptiveBatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AdaptiveBatcher {{ num_pes: {}, stream: {} }}",
            self.dests.len(),
            self.stream
        )
    }
}

/// A record whose body exceeds the 24-bit length field must go out as a serde single.
#[inline(always)]
fn too_big(rec_len: usize) -> bool {
    rec_len - wire::REC_HDR_LEN > wire::MAX_REC_BODY
}

#[inline]
pub(crate) fn stream_header(src: usize) -> SerializeHeader {
    SerializeHeader {
        msg: Msg {
            src: src as u16,
            cmd: Cmd::Stream,
            padding: [0; 1],
        },
    }
}

async fn send_frame(dst: Option<usize>, buf: &[u8], ctx: (Arc<Lamellae>, Arc<LamellarArchRT>, usize)) {
    let (lamellae, arch, src) = ctx;
    let mut data = create_serde_buf(stream_header(src), buf.len(), &lamellae).await;
    data.data_as_bytes_mut()[..buf.len()].copy_from_slice(buf);
    lamellae.send_to_pes_async(dst, arch, data).await;
}

impl AdaptiveBatcher {
    pub(crate) fn new(
        num_pes: usize,
        executor: Arc<Executor>,
        stream: bool,
        backend: crate::lamellae::Backend,
    ) -> AdaptiveBatcher {
        AdaptiveBatcher {
            dests: Arc::new((0..num_pes).map(|_| Dest::default()).collect()),
            executor,
            stream,
            high_water: high_water(backend),
        }
    }

    /// Stream mode: encodes the record straight into the ring to `dst`. Hands `encode`
    /// back when the record must be staged instead.
    #[inline(always)]
    fn try_stream<F: FnOnce(&mut [u8])>(
        &self,
        dst: usize,
        req_data: &ReqMetaData,
        rec_len: usize,
        encode: F,
    ) -> Result<(), F> {
        if self.stream {
            req_data.lamellae.stream_write(dst, rec_len, encode)
        } else {
            Err(encode)
        }
    }

    /// Appends one `rec_len`-byte record for `dst` and then sends or defers according to
    /// the flush rule.
    async fn push(
        &self,
        dst: usize,
        req_data: &ReqMetaData,
        am: Option<LamellarArcAm>,
        rec_len: usize,
        encode: impl FnOnce(&mut [u8]),
    ) {
        let Err(encode) = self.try_stream(dst, req_data, rec_len, encode) else {
            return;
        };
        let d = &self.dests[dst];
        let over = {
            let mut s = d.stage.lock();
            s.set_ctx(req_data);
            wire::append(&mut s.buf, rec_len, encode);
            if let Some(am) = am {
                s.keep.push(am);
            }
            d.take_overflow(&mut s, self.high_water)
        };
        self.kick(dst, over).await;
    }

    /// Applies the flush rule after an append to `dst`; `over` is the backlog taken by
    /// `Dest::take_overflow`.
    #[inline]
    async fn kick(&self, dst: usize, over: Option<Frame>) {
        let d = &self.dests[dst];
        if let Some(frame) = over {
            // flusher is busy and the backlog is large: ship it ourselves (backpressure)
            d.send(dst, frame).await;
        } else if !d.inflight.load(Ordering::SeqCst) && !d.inflight.swap(true, Ordering::SeqCst) {
            self.clone().flush(dst).await;
        }
    }

    /// Runs with `inflight` owned; sends staged frames until the buffer is empty.
    fn flush(self, dst: usize) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let d = &self.dests[dst];
            let mut rounds = 0;
            loop {
                let frame = d.stage.lock().take_frame();
                let Some(frame) = frame else {
                    d.inflight.store(false, Ordering::SeqCst);
                    // a producer may have appended after our empty check but seen inflight
                    if d.stage.lock().buf.is_empty() || d.inflight.swap(true, Ordering::SeqCst) {
                        return;
                    }
                    continue;
                };
                d.send(dst, frame).await;
                rounds += 1;
                if rounds >= HANDOFF_ROUNDS {
                    let this = self.clone();
                    self.executor.submit_io_task(this.flush(dst));
                    return;
                }
            }
        })
    }

    /// Sink for process_msg: takes the AM unserialized and encodes it straight into the
    /// destination's staging buffer (no intermediate Vec, no boxed trait futures).
    pub(crate) async fn sink_am(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        immediate: bool,
    ) {
        let Some(dst) = req_data.dst else {
            let darc_ser_cnt = match req_data.team.team_pe_id() {
                Ok(_) => req_data.team.num_pes() - 1,
                Err(_) => req_data.team.num_pes(),
            };
            let am_bytes = {
                let _mrg = crate::memregion::one_sided::MemRegionSendGuard::new(darc_ser_cnt);
                am.serialize()
            };
            return self
                .send_am_record(req_data, am, am_id, am_bytes, false)
                .await;
        };
        let size = am.serialized_size();
        if too_big(wire::am_rec_len(&req_data.id, size))
            || ((size >= config().am_size_threshold || immediate)
                && req_data.lamellae.available_to_send(dst))
        {
            let am_bytes = {
                let _mrg = crate::memregion::one_sided::MemRegionSendGuard::new(1);
                am.serialize()
            };
            return send_am_serde(req_data, am, am_id, am_bytes, Cmd::Am).await;
        }
        let mut darcs = vec![];
        am.ser(1, &mut darcs);
        let team_addr = req_data.team.darc_addr();
        let rec_len = wire::am_rec_len(&req_data.id, size);
        let encode = |b: &mut [u8]| {
            wire::enc_am(b, false, am_id, &req_data.id, team_addr, |p| {
                let _mrg = crate::memregion::one_sided::MemRegionSendGuard::new(1);
                am.serialize_into(p)
            })
        };
        let Err(encode) = self.try_stream(dst, &req_data, rec_len, encode) else {
            return;
        };
        let over = {
            let d = &self.dests[dst];
            let mut s = d.stage.lock();
            s.set_ctx(&req_data);
            wire::append(&mut s.buf, rec_len, encode);
            s.keep.push(am);
            d.take_overflow(&mut s, self.high_water)
        };
        self.kick(dst, over).await;
    }

    /// Synchronous sink used by `Scheduler::submit_am`: stages a small point-to-point AM
    /// on the caller's thread, skipping the per-AM task. A flush task is spawned only
    /// when no send to `dst` is in flight. Hands the AM back if it doesn't qualify.
    pub(crate) fn try_stage_am(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
    ) -> Result<(), (ReqMetaData, LamellarArcAm)> {
        let dst = match req_data.dst {
            Some(dst) if dst != req_data.src => dst,
            _ => return Err((req_data, am)),
        };
        let size = am.serialized_size();
        if too_big(wire::am_rec_len(&req_data.id, size))
            || (size >= config().am_size_threshold && req_data.lamellae.available_to_send(dst))
        {
            return Err((req_data, am));
        }
        let mut darcs = vec![];
        am.ser(1, &mut darcs);
        let team_addr = req_data.team.darc_addr();
        let rec_len = wire::am_rec_len(&req_data.id, size);
        let encode = |b: &mut [u8]| {
            wire::enc_am(b, false, am_id, &req_data.id, team_addr, |p| {
                let _mrg = crate::memregion::one_sided::MemRegionSendGuard::new(1);
                am.serialize_into(p)
            })
        };
        let Err(encode) = self.try_stream(dst, &req_data, rec_len, encode) else {
            return Ok(());
        };
        let d = &self.dests[dst];
        let over = {
            let mut s = d.stage.lock();
            s.set_ctx(&req_data);
            wire::append(&mut s.buf, rec_len, encode);
            s.keep.push(am);
            d.take_overflow(&mut s, self.high_water)
        };
        if let Some(frame) = over {
            let this = self.clone();
            self.executor.submit_io_task(async move {
                this.dests[dst].send(dst, frame).await;
            });
        } else if !d.inflight.load(Ordering::SeqCst) && !d.inflight.swap(true, Ordering::SeqCst) {
            self.executor.submit_io_task(self.clone().flush(dst));
        }
        Ok(())
    }

    async fn send_am_record(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_bytes: Vec<u8>,
        ret: bool,
    ) {
        if too_big(wire::am_rec_len(&req_data.id, am_bytes.len())) {
            let cmd = if ret { Cmd::ReturnAm } else { Cmd::Am };
            return send_am_serde(req_data, am, am_id, am_bytes, cmd).await;
        }
        let team_addr = req_data.team.darc_addr();
        match req_data.dst {
            Some(dst) => {
                let mut darcs = vec![];
                am.ser(1, &mut darcs);
                let rec_len = wire::am_rec_len(&req_data.id, am_bytes.len());
                self.push(dst, &req_data, Some(am), rec_len, |b| {
                    wire::enc_am(b, ret, am_id, &req_data.id, team_addr, |p| {
                        p.copy_from_slice(&am_bytes)
                    })
                })
                .await;
            }
            None => {
                // team broadcast: one single-record frame to every member
                let darc_ser_cnt = match req_data.team.team_pe_id() {
                    Ok(_) => req_data.team.num_pes() - 1,
                    Err(_) => req_data.team.num_pes(),
                };
                let mut darcs = vec![];
                am.ser(darc_ser_cnt, &mut darcs);
                let mut buf = Vec::with_capacity(wire::am_rec_len(&req_data.id, am_bytes.len()));
                wire::put_am(&mut buf, ret, am_id, &req_data.id, team_addr, &am_bytes);
                let ctx = (
                    req_data.lamellae.clone(),
                    req_data.team.arch.clone(),
                    req_data.team.world_pe,
                );
                send_frame(None, &buf, ctx).await;
            }
        }
    }
}

#[async_trait]
impl Batcher for AdaptiveBatcher {
    async fn add_remote_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_bytes: Vec<u8>,
        _stall_mark: usize,
    ) {
        self.send_am_record(req_data, am, am_id, am_bytes, false)
            .await;
    }

    async fn add_return_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_bytes: Vec<u8>,
        _stall_mark: usize,
    ) {
        self.send_am_record(req_data, am, am_id, am_bytes, true)
            .await;
    }

    async fn add_data_am_to_batch(
        &self,
        req_data: ReqMetaData,
        darc_bytes: Vec<u8>,
        data_bytes: Vec<u8>,
        _stall_mark: usize,
    ) {
        let rec_len = wire::data_rec_len(&req_data.id, darc_bytes.len(), data_bytes.len());
        if too_big(rec_len) {
            return send_data_am_serde(req_data, darc_bytes, data_bytes).await;
        }
        let dst = req_data.dst.expect("data return must have a destination");
        self.push(dst, &req_data, None, rec_len, |b| {
            wire::enc_data(b, &req_data.id, &darc_bytes, &data_bytes)
        })
        .await;
    }

    async fn add_unit_am_to_batch(&self, req_data: ReqMetaData, _stall_mark: usize) {
        let dst = req_data.dst.expect("unit return must have a destination");
        let ids = std::slice::from_ref(&req_data.id);
        self.push(dst, &req_data, None, wire::units_rec_len(ids.iter()), |b| {
            wire::enc_units(b, ids)
        })
        .await;
    }

    async fn add_units_to_batch(&self, reqs: Vec<ReqMetaData>, _stall_mark: usize) {
        // one UNITS record per run of equal destinations (a frame's replies share one)
        let mut ids = Vec::with_capacity(reqs.len());
        let mut start = 0;
        while start < reqs.len() {
            let dst = reqs[start].dst.expect("unit return must have a destination");
            let mut end = start;
            ids.clear();
            while end < reqs.len() && reqs[end].dst == Some(dst) {
                ids.push(reqs[end].id);
                end += 1;
            }
            let rec_len = wire::units_rec_len(ids.iter());
            self.push(dst, &reqs[start], None, rec_len, |b| wire::enc_units(b, &ids))
                .await;
            start = end;
        }
    }

    // over-threshold singles use the serde wire format, same as SimpleBatcher
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
        exec_am_serde(src, data, i, lamellae, ame, &self.executor);
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

    async fn exec_batched_msg(
        &self,
        _msg: Msg,
        _ser_data: SerializedData,
        _lamellae: &Arc<Lamellae>,
        _ame: &RegisteredActiveMessages,
    ) {
        panic!("[LAMELLAR ERROR] AdaptiveBatcher sends Cmd::Stream frames, not BatchedMsg");
    }
}

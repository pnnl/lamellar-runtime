// command_queues_put3.rs — PUT-based protocol with eager send path for small messages.
//
// Extends put2 with an eager (zero-handshake) path for messages ≤ EAGER_DATA_SIZE bytes:
//
//   Sender                           Receiver
//   ──────────────────────────────────────────
//   claim slot (CAS on send_head)
//   PUT data → recv_ring[my_pe][slot]
//   PUT size  → recv_ring[my_pe][slot].magic
//                                  poll magic != 0
//                                  copy slot data to new SerializedData
//                                  zero magic (recycle slot)
//                                  PUT ack counter → src.send_acks[my_pe]
//                                  submit_remote_am
//
// Flow control: sender checks that (send_head[dst] - acked[dst]) < RING_SIZE before claiming.
// For larger messages, falls through to the rendezvous protocol from put2.

use super::{
    comm::{CmdQStatus, CommAlloc, CommInfo, CommMem, CommProgress, CommSlice},
    Comm, Lamellae, SerializedData,
};
use crate::{
    env_var::config, lamellae::CommAllocRdma, print_stats, scheduler::Scheduler, stats,
};
use core::panic;
use parking_lot::RwLock;
use async_lock::Mutex;
use std::num::Wrapping;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::{debug, info, trace, warn};

// ── Eager protocol constants ────────────────────────────────────────────────
const EAGER_DATA_SIZE: usize = 4096;
const EAGER_RING_SIZE: usize = 8;
const EAGER_SLOT_SIZE: usize = EAGER_DATA_SIZE + std::mem::size_of::<u64>();

static MSG_ID: AtomicUsize = AtomicUsize::new(1);

lazy_static! {
    static ref PE_SENDS: Vec<Vec<AtomicUsize>> = {
        let mut v = Vec::with_capacity(2);
        for _ in 0..2 {
            let mut t = Vec::with_capacity(32);
            for _ in 0..32 {
                t.push(AtomicUsize::new(0));
            }
            v.push(t);
        }
        v
    };
    static ref PE_RECVS: Vec<Vec<AtomicUsize>> = {
        let mut v = Vec::with_capacity(2);
        for _ in 0..2 {
            let mut t = Vec::with_capacity(32);
            for _ in 0..32 {
                t.push(AtomicUsize::new(0));
            }
            v.push(t);
        }
        v
    };
}

#[repr(C)]
#[derive(Clone, Copy)]
struct CmdMsg {
    daddr: usize,
    dsize: usize,
    msg_hash: usize,
    cmd_hash: usize,
    cmd: Cmd,
}

#[lamellar_prof::prof]
impl Default for CmdMsg {
    fn default() -> Self {
        CmdMsg {
            daddr: 0,
            dsize: 0,
            cmd: Cmd::Clear,
            msg_hash: 0,
            cmd_hash: 0,
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Cmd {
    Clear = 1,
    Free,
    Release,
    Tx,
    Alloc,
    Panic,
    Ready,
}
#[lamellar_prof::prof]
impl Default for Cmd {
    fn default() -> Self {
        Cmd::Clear
    }
}

fn calc_hash(addr: usize, num_bytes: usize) -> usize {
    let num_usizes = num_bytes / std::mem::size_of::<usize>();
    let num_u8s = num_bytes % std::mem::size_of::<usize>();
    let u8_slice = unsafe {
        std::slice::from_raw_parts(
            (addr + num_usizes * std::mem::size_of::<usize>()) as *const u8,
            num_u8s,
        )
    };
    ((0..num_usizes)
        .map(|x| unsafe { Wrapping((addr as *const usize).offset(x as isize).read_unaligned()) })
        .sum::<Wrapping<usize>>()
        + u8_slice
            .iter()
            .map(|x| Wrapping(*x as usize))
            .sum::<Wrapping<usize>>())
    .0
}

#[lamellar_prof::prof]
impl CmdMsg {
    fn as_bytes(&self) -> &[u8] {
        let pointer = self as *const Self as *const u8;
        let size = std::mem::size_of::<Self>();
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(pointer, size) };
        slice
    }

    fn hash(&self) -> usize {
        let mut res = self
            .daddr
            .wrapping_add(self.dsize)
            .wrapping_add(self.cmd as usize)
            .wrapping_add(self.msg_hash);
        if res == 0 {
            res = 1
        }
        res
    }
    fn calc_hash(&mut self) {
        self.cmd_hash = self.hash()
    }
    fn check_hash(&self) -> bool {
        self.cmd_hash == self.hash() && self.cmd_hash != 0
    }
}

#[lamellar_prof::prof]
impl std::fmt::Debug for CmdMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "daddr {:#x}({:?}) dsize {:?}  cmd {:?} msg_hash {:?} cmd_hash {:?}",
            self.daddr, self.daddr, self.dsize, self.cmd, self.msg_hash, self.cmd_hash,
        )
    }
}

struct InnerCQ {
    send_buffer: Arc<Vec<RwLock<CommSlice<CmdMsg>>>>,
    recv_buffer: Arc<Vec<RwLock<CommSlice<CmdMsg>>>>,
    alloc_buffer: Arc<Vec<Mutex<CommSlice<CmdMsg>>>>,
    panic_buffer: Arc<Mutex<CommSlice<CmdMsg>>>,
    release_cmd: Arc<Box<CmdMsg>>,
    clear_cmd: Arc<Box<CmdMsg>>,
    free_cmd: Arc<Box<CmdMsg>>,
    comm: Arc<Comm>,
    scheduler: Arc<Scheduler>,
    my_pe: usize,
    num_pes: usize,
    pending_alloc: Arc<AtomicBool>,
    sent_cnt: Arc<AtomicUsize>,
    recv_cnt: Arc<AtomicUsize>,
    put_amt: Arc<AtomicUsize>,
    alloc_id: Arc<AtomicUsize>,
    active: Arc<AtomicU8>,
    // Eager path state
    eager_recv_alloc_addr: usize,       // local VA of our eager recv ring (for local polling)
    eager_send_acks_alloc_addr: usize,  // local VA of our send-ack counters (for local reads)
    eager_recv_comm_alloc: CommAlloc,   // CommAlloc for remote PUTs into any PE's recv ring
    eager_send_acks_comm_alloc: CommAlloc, // CommAlloc for remote PUTs into any PE's ack counters
    eager_recv_head: Arc<Vec<AtomicUsize>>,      // per-src: next ring slot index to poll
    eager_send_head: Arc<Vec<AtomicUsize>>,      // per-dst: next ring slot index to claim
    eager_recv_processed: Arc<Vec<AtomicUsize>>, // per-src: monotonic count of processed slots
}

#[lamellar_prof::prof]
impl InnerCQ {
    fn new(
        send_buffer_alloc: CommAlloc,
        recv_buffer_alloc: CommAlloc,
        alloc_buffer_alloc: CommAlloc,
        panic_buffer_alloc: CommAlloc,
        release_cmd_alloc: CommAlloc,
        clear_cmd_alloc: CommAlloc,
        free_cmd_alloc: CommAlloc,
        eager_recv_alloc: CommAlloc,
        eager_send_acks_alloc: CommAlloc,
        comm: Arc<Comm>,
        scheduler: Arc<Scheduler>,
        my_pe: usize,
        num_pes: usize,
        active: Arc<AtomicU8>,
    ) -> InnerCQ {
        let send_buffer: CommSlice<CmdMsg> = send_buffer_alloc.as_comm_slice();
        let mut send_buffers = vec![];
        for (i, cmd) in send_buffer.clone().iter_mut().enumerate() {
            (*cmd).daddr = 0;
            (*cmd).dsize = 0;
            (*cmd).cmd = Cmd::Clear;
            (*cmd).msg_hash = 0;
            (*cmd).calc_hash();
            send_buffers.push(RwLock::new(send_buffer.sub_slice(i..=i)));
        }
        trace!("send_buffer init {:?}", send_buffer);

        let recv_buffer: CommSlice<CmdMsg> = recv_buffer_alloc.as_comm_slice();
        let mut recv_buffers = vec![];
        for (i, cmd) in recv_buffer.clone().iter_mut().enumerate() {
            (*cmd).daddr = 0;
            (*cmd).dsize = 0;
            (*cmd).cmd = Cmd::Clear;
            (*cmd).msg_hash = 0;
            (*cmd).calc_hash();
            recv_buffers.push(RwLock::new(recv_buffer.sub_slice(i..=i)));
        }
        trace!("recv_buffer init {:?}", recv_buffer);

        let alloc_buffer: CommSlice<CmdMsg> = alloc_buffer_alloc.as_comm_slice();
        let mut alloc_buffers = vec![];
        for (i, cmd) in alloc_buffer.clone().iter_mut().enumerate() {
            (*cmd).daddr = 0;
            (*cmd).dsize = 0;
            (*cmd).cmd = Cmd::Clear;
            (*cmd).msg_hash = 0;
            (*cmd).calc_hash();
            alloc_buffers.push(Mutex::new(alloc_buffer.sub_slice(i..=i)));
        }
        trace!("alloc_buffer init {:?}", alloc_buffer);

        let panic_buffer: CommSlice<CmdMsg> = panic_buffer_alloc.as_comm_slice();
        for cmd in panic_buffer.clone().iter_mut() {
            (*cmd).daddr = 0;
            (*cmd).dsize = 0;
            (*cmd).cmd = Cmd::Clear;
            (*cmd).msg_hash = 0;
            (*cmd).calc_hash();
        }
        trace!("panic_buffer init {:?}", panic_buffer);

        let mut release_cmd = unsafe { Box::from_raw(release_cmd_alloc.as_mut_ptr::<CmdMsg>()) };
        release_cmd.daddr = 1;
        release_cmd.dsize = 1;
        release_cmd.cmd = Cmd::Release;
        release_cmd.msg_hash = 1;
        release_cmd.calc_hash();
        trace!("release_cmd init {:?}", release_cmd);

        let mut clear_cmd = unsafe { Box::from_raw(clear_cmd_alloc.as_mut_ptr::<CmdMsg>()) };
        clear_cmd.daddr = 0;
        clear_cmd.dsize = 0;
        clear_cmd.cmd = Cmd::Clear;
        clear_cmd.msg_hash = 0;
        clear_cmd.calc_hash();
        trace!("clear_cmd init {:?}", clear_cmd);

        let mut free_cmd = unsafe { Box::from_raw(free_cmd_alloc.as_mut_ptr::<CmdMsg>()) };
        free_cmd.daddr = 0;
        free_cmd.dsize = 0;
        free_cmd.cmd = Cmd::Free;
        free_cmd.msg_hash = 0;
        free_cmd.calc_hash();
        trace!("free_cmd init {:?}", free_cmd);

        // Zero-initialise the eager recv ring so no spurious magic values are seen.
        {
            let mut slice: CommSlice<u8> = eager_recv_alloc.as_comm_slice();
            for byte in slice.iter_mut() {
                *byte = 0;
            }
        }
        let eager_recv_alloc_addr = eager_recv_alloc.as_comm_slice::<u8>().usize_addr();
        let eager_recv_comm_alloc = eager_recv_alloc.clone();

        // Zero-initialise the send-ack counters.
        {
            let mut slice: CommSlice<u64> = eager_send_acks_alloc.as_comm_slice();
            for val in slice.iter_mut() {
                *val = 0;
            }
        }
        let eager_send_acks_alloc_addr = eager_send_acks_alloc.as_comm_slice::<u8>().usize_addr();
        let eager_send_acks_comm_alloc = eager_send_acks_alloc.clone();

        let mut eager_recv_head = Vec::with_capacity(num_pes);
        let mut eager_send_head = Vec::with_capacity(num_pes);
        let mut eager_recv_processed = Vec::with_capacity(num_pes);
        for _ in 0..num_pes {
            eager_recv_head.push(AtomicUsize::new(0));
            eager_send_head.push(AtomicUsize::new(0));
            eager_recv_processed.push(AtomicUsize::new(0));
        }

        InnerCQ {
            send_buffer: Arc::new(send_buffers),
            recv_buffer: Arc::new(recv_buffers),
            alloc_buffer: Arc::new(alloc_buffers),
            panic_buffer: Arc::new(Mutex::new(panic_buffer)),
            release_cmd: Arc::new(release_cmd),
            clear_cmd: Arc::new(clear_cmd),
            free_cmd: Arc::new(free_cmd),
            comm,
            scheduler,
            my_pe,
            num_pes,
            pending_alloc: Arc::new(AtomicBool::new(false)),
            sent_cnt: Arc::new(AtomicUsize::new(0)),
            recv_cnt: Arc::new(AtomicUsize::new(0)),
            put_amt: Arc::new(AtomicUsize::new(0)),
            alloc_id: Arc::new(AtomicUsize::new(0)),
            active,
            eager_recv_alloc_addr,
            eager_send_acks_alloc_addr,
            eager_recv_comm_alloc,
            eager_send_acks_comm_alloc,
            eager_recv_head: Arc::new(eager_recv_head),
            eager_send_head: Arc::new(eager_send_head),
            eager_recv_processed: Arc::new(eager_recv_processed),
        }
    }

    async fn empty(&self) -> bool {
        for dst in 0..self.num_pes {
            if dst == self.my_pe {
                continue;
            }
            let buf = self.send_buffer[dst].read();
            if buf[0].cmd == Cmd::Tx || buf[0].cmd == Cmd::Ready {
                return false;
            }
        }
        true
    }

    fn ready(&self, src: usize) -> Option<CmdMsg> {
        let mut recv_buffer = self.recv_buffer[src].write();
        let cmd = recv_buffer[0].clone();
        if cmd.check_hash() {
            match cmd.cmd {
                Cmd::Clear => None,
                Cmd::Tx => {
                    trace!("received cmd:{:?} from pe {} -- [{:?}]", cmd.cmd, src, cmd);
                    let res = Some(cmd.clone());
                    recv_buffer[0] = **self.clear_cmd;
                    res
                }
                Cmd::Free | Cmd::Release => panic!("should not see free/release in recv buffer"),
                Cmd::Alloc => panic!("should not encounter alloc here"),
                Cmd::Panic => panic!("should not encounter panic here"),
                Cmd::Ready => panic!("should not see Ready in recv buffer"),
            }
        } else {
            None
        }
    }

    fn available_to_send(&self, pe: usize) -> bool {
        let send_buf = self.send_buffer[pe].read();
        if send_buf[0].hash() == self.clear_cmd.hash() {
            return true;
        }
        false
    }

    fn check_alloc(&self, print: bool) {
        if let Ok(_) =
            self.pending_alloc
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        {
            if print {
                trace!("checking alloc buffer");
            }
            let mut do_alloc = false;
            let mut min_size = 0;
            for pe in 0..self.num_pes {
                if let Some(alloc_buf) = self.alloc_buffer[pe].try_lock() {
                    if alloc_buf[0].cmd == Cmd::Alloc && alloc_buf[0].check_hash() {
                        info!("pe {} (allegedly) needs alloc {:?}", pe, alloc_buf[0]);
                        do_alloc = true;
                        min_size = std::cmp::max(min_size, alloc_buf[0].dsize);
                    }
                }
            }
            if do_alloc {
                info!(
                    "need to alloc new pool {:?}",
                    std::backtrace::Backtrace::capture()
                );
                self.send_alloc_inner(min_size);
            }
            self.pending_alloc.store(false, Ordering::SeqCst);
        }
    }

    fn check_panic(&self) -> bool {
        if let Some(panic_buf) = self.panic_buffer.try_lock() {
            let mut paniced = false;
            for pe in 0..self.num_pes {
                if panic_buf[pe].cmd != Cmd::Clear {
                    trace!(
                        "pe {} panic_buf not clear {:?}",
                        pe, &panic_buf[pe] as *const CmdMsg
                    );
                    trace!("panic_buf {:?}", panic_buf[pe]);
                }
                if panic_buf[pe].check_hash() && panic_buf[pe].cmd == Cmd::Panic {
                    trace!("panic_buf passed hash check {:?}", panic_buf[pe]);
                    paniced = true;
                    break;
                }
            }
            if paniced {
                self.active.store(CmdQStatus::Panic as u8, Ordering::SeqCst);
                return true;
            }
        }
        false
    }

    // Try to claim an eager ring slot for dst using a CAS loop.
    // Returns the slot index (0..RING_SIZE) on success, or None if the ring is full.
    fn try_claim_eager_slot(&self, dst: usize) -> Option<usize> {
        loop {
            let head = self.eager_send_head[dst].load(Ordering::Acquire);
            // Read acks that dst has sent back to our send_acks array.
            let acked = unsafe {
                ((self.eager_send_acks_alloc_addr + dst * std::mem::size_of::<u64>())
                    as *const u64)
                    .read_volatile() as usize
            };
            if head.wrapping_sub(acked) >= EAGER_RING_SIZE {
                return None;
            }
            if self
                .eager_send_head[dst]
                .compare_exchange(head, head + 1, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return Some(head % EAGER_RING_SIZE);
            }
            // CAS lost to another thread; retry with updated head.
        }
    }

    // Eager send: PUT data directly into dst's ring slot, then PUT size as magic terminator.
    // Uses eager_recv_comm_alloc which has remote_keys[dst].addr = dst's actual recv ring VA.
    async fn send_eager(&self, data: CommSlice<u8>, dst: usize, slot_idx: usize) {
        let size = data.len();
        let byte_offset = (self.my_pe * EAGER_RING_SIZE + slot_idx) * EAGER_SLOT_SIZE;

        let dst_data = self.eager_recv_comm_alloc.comm_slice_at_byte_offset::<u8>(byte_offset, size);
        let dst_magic = self.eager_recv_comm_alloc.comm_slice_at_byte_offset::<u64>(byte_offset + EAGER_DATA_SIZE, 1);

        dst_data
            .put_buffer::<u8>(&self.scheduler, None, data, dst, 0)
            .await;
        // PUT magic after data so RDMA ordering ensures data arrives first.
        dst_magic.put_unmanaged::<u64>(size as u64, dst, 0);

        stats!(PE_SENDS[1][dst].fetch_add(1, Ordering::SeqCst));
        self.sent_cnt.fetch_add(1, Ordering::SeqCst);
    }

    // Eager send for Vec<u8>: same as send_eager but copies into registered memory first.
    async fn send_eager_vec(&self, data: Vec<u8>, dst: usize, slot_idx: usize) {
        let size = data.len();
        let byte_offset = (self.my_pe * EAGER_RING_SIZE + slot_idx) * EAGER_SLOT_SIZE;

        let dst_data = self.eager_recv_comm_alloc.comm_slice_at_byte_offset::<u8>(byte_offset, size);
        let dst_magic = self.eager_recv_comm_alloc.comm_slice_at_byte_offset::<u64>(byte_offset + EAGER_DATA_SIZE, 1);

        if let Ok(rt_data) = self.comm.rt_alloc(size, std::mem::align_of::<u8>()) {
            let mut data_slice = rt_data.as_comm_slice::<u8>();
            data_slice.copy_from_slice(&data);
            self.put_amt.fetch_add(size, Ordering::Relaxed);
            dst_data
                .put_buffer::<u8>(&self.scheduler, None, data_slice, dst, 0)
                .await;
        } else {
            dst_data
                .put_buffer::<u8>(&self.scheduler, None, data, dst, 0)
                .await;
        }
        dst_magic.put_unmanaged::<u64>(size as u64, dst, 0);

        stats!(PE_SENDS[1][dst].fetch_add(1, Ordering::SeqCst));
        self.sent_cnt.fetch_add(1, Ordering::SeqCst);
    }

    // Send an ack counter back to src. Uses eager_send_acks_comm_alloc which has
    // remote_keys[src].addr = src's actual ack counter VA. Slot my_pe is src's index for us.
    fn send_eager_ack(&self, src: usize, count: usize) {
        let ack_slot = self.eager_send_acks_comm_alloc
            .comm_slice_at_byte_offset::<u64>(self.my_pe * std::mem::size_of::<u64>(), 1);
        ack_slot.put_unmanaged::<u64>(count as u64, src, 0);
    }

    // Two-phase rendezvous send (same as put2).
    async fn send(&self, data: CommSlice<u8>, dst: usize, hash: usize) {
        stats!(PE_SENDS[0][dst].fetch_add(1, Ordering::SeqCst));
        debug!("want to send data {:?} {:x}", dst, hash);

        // Eager path: small messages that fit in a ring slot.
        if data.len() > 0 && data.len() <= EAGER_DATA_SIZE {
            if let Some(slot_idx) = self.try_claim_eager_slot(dst) {
                debug!("eager send to dst({dst}) size={} slot={slot_idx}", data.len());
                self.send_eager(data, dst, slot_idx).await;
                return;
            }
        }

        let mut printed = false;

        // Phase 1: wait for Clear, fill Tx, signal dst.
        loop {
            if self.active.load(Ordering::SeqCst) == CmdQStatus::Panic as u8 {
                return;
            }
            {
                let mut send_buf = self.send_buffer[dst].write();
                if send_buf[0].hash() == self.clear_cmd.hash() {
                    send_buf[0].daddr = data.usize_addr();
                    send_buf[0].dsize = data.len();
                    send_buf[0].msg_hash = hash;
                    send_buf[0].cmd = Cmd::Tx;
                    send_buf[0].calc_hash();

                    let recv_buffer = self.recv_buffer[self.my_pe].read();
                    stats!(PE_SENDS[1][dst].fetch_add(1, Ordering::SeqCst));
                    debug!("sending tx to dst({dst}) {:?}", send_buf[0]);
                    let send_cmd = send_buf[0].clone();
                    drop(send_buf);
                    let num_bytes = send_cmd.as_bytes().len();
                    let _ = recv_buffer.put_unmanaged::<CmdMsg>(send_cmd, dst, 0);
                    self.put_amt.fetch_add(num_bytes, Ordering::Relaxed);
                    break;
                } else {
                    if !printed {
                        printed = true;
                        debug!(
                            "send slot busy for dst({dst}), waiting... {:?}",
                            send_buf[0]
                        );
                    }
                }
            }
            self.comm.thread_flush();
            async_std::task::yield_now().await;
        }

        // Phase 2: spawn as a separate regular-priority task.
        let send_buffer = self.send_buffer.clone();
        let comm = self.comm.clone();
        let scheduler = self.scheduler.clone();
        let clear_cmd = self.clear_cmd.clone();
        let sent_cnt = self.sent_cnt.clone();
        let active = self.active.clone();
        self.scheduler.submit_task(async move {
            loop {
                if active.load(Ordering::SeqCst) == CmdQStatus::Panic as u8 {
                    return;
                }
                let maybe_ready = {
                    let send_buf = send_buffer[dst].read();
                    if send_buf[0].cmd == Cmd::Ready && send_buf[0].check_hash() {
                        Some(send_buf[0].clone())
                    } else {
                        None
                    }
                };
                if let Some(ready) = maybe_ready {
                    send_buffer[dst].write()[0] = **clear_cmd;
                    debug!("got ready from dst({dst}) {:?}", ready);

                    let dst_data_full = comm.one_sided_alloc_from_remote_pe_and_addr(
                        dst,
                        ready.daddr,
                        data.len() + std::mem::size_of::<u64>(),
                    );
                    let dst_data =
                        dst_data_full.comm_slice_at_byte_offset::<u8>(0, data.len());
                    let magic_data = dst_data_full
                        .comm_slice_at_byte_offset::<u64>(data.len(), 1);
                    dst_data
                        .put_buffer::<u8>(&scheduler, None, data.clone(), dst, 0)
                        .await;
                    magic_data.put_unmanaged::<u64>(ready.msg_hash as u64, dst, 0);

                    sent_cnt.fetch_add(1, Ordering::SeqCst);
                    break;
                }
                comm.thread_flush();
                async_std::task::yield_now().await;
            }
        });
    }

    async fn send_vec(&self, mut data: Vec<u8>, dst: usize, hash: usize) {
        stats!(PE_SENDS[0][dst].fetch_add(1, Ordering::SeqCst));
        debug!("want to send vec data {:?} {:x}", dst, hash);

        // Eager path: small Vec messages.
        if data.len() > 0 && data.len() <= EAGER_DATA_SIZE {
            if let Some(slot_idx) = self.try_claim_eager_slot(dst) {
                debug!(
                    "eager send_vec to dst({dst}) size={} slot={slot_idx}",
                    data.len()
                );
                self.send_eager_vec(data, dst, slot_idx).await;
                return;
            }
        }

        let mut printed = false;

        // Phase 1: wait for Clear, fill Tx, signal dst.
        loop {
            if self.active.load(Ordering::SeqCst) == CmdQStatus::Panic as u8 {
                return;
            }
            {
                let mut send_buf = self.send_buffer[dst].write();
                if send_buf[0].hash() == self.clear_cmd.hash() {
                    send_buf[0].daddr = 0;
                    send_buf[0].dsize = data.len();
                    send_buf[0].msg_hash = hash;
                    send_buf[0].cmd = Cmd::Tx;
                    send_buf[0].calc_hash();

                    let recv_buffer = self.recv_buffer[self.my_pe].read();
                    stats!(PE_SENDS[1][dst].fetch_add(1, Ordering::SeqCst));
                    debug!("sending tx to dst({dst}) {:?}", send_buf[0]);
                    let send_cmd = send_buf[0].clone();
                    drop(send_buf);
                    let num_bytes = send_cmd.as_bytes().len();
                    let _ = recv_buffer.put_unmanaged::<CmdMsg>(send_cmd, dst, 0);
                    debug!(
                        "put cmd msg to dst({dst}) recv buffer, cmd msg size {:?} bytes",
                        num_bytes
                    );
                    self.put_amt.fetch_add(num_bytes, Ordering::Relaxed);
                    break;
                } else {
                    if !printed {
                        printed = true;
                        debug!(
                            "send slot busy for dst({dst}), waiting... {:?}",
                            send_buf[0]
                        );
                    }
                }
            }
            self.comm.thread_flush();
            async_std::task::yield_now().await;
        }

        // Phase 2: spawn as a separate regular-priority task.
        let send_buffer = self.send_buffer.clone();
        let comm = self.comm.clone();
        let scheduler = self.scheduler.clone();
        let clear_cmd = self.clear_cmd.clone();
        let sent_cnt = self.sent_cnt.clone();
        let put_amt = self.put_amt.clone();
        let active = self.active.clone();
        self.scheduler.submit_task(async move {
            loop {
                if active.load(Ordering::SeqCst) == CmdQStatus::Panic as u8 {
                    return;
                }
                let maybe_ready = {
                    let send_buf = send_buffer[dst].read();
                    if send_buf[0].cmd == Cmd::Ready && send_buf[0].check_hash() {
                        Some(send_buf[0].clone())
                    } else {
                        None
                    }
                };
                if let Some(ready) = maybe_ready {
                    send_buffer[dst].write()[0] = **clear_cmd;
                    debug!("got ready from dst({dst}) {:?}", ready);
                    data.extend_from_slice(&ready.msg_hash.to_ne_bytes());

                    let dst_data_full = comm.one_sided_alloc_from_remote_pe_and_addr(
                        dst,
                        ready.daddr,
                        data.len(),
                    );

                    if let Ok(rt_data) = comm.rt_alloc(data.len(), std::mem::align_of::<u8>()) {
                        let mut data_slice = rt_data.as_comm_slice::<u8>();
                        data_slice.copy_from_slice(&data);
                        put_amt.fetch_add(data.len(), Ordering::Relaxed);
                        dst_data_full
                            .put_buffer::<u8>(&scheduler, None, data_slice, dst, 0)
                            .await;
                    } else {
                        dst_data_full
                            .put_buffer::<u8>(&scheduler, None, data, dst, 0)
                            .await;
                    }

                    sent_cnt.fetch_add(1, Ordering::SeqCst);
                    break;
                }
                comm.thread_flush();
                async_std::task::yield_now().await;
            }
        });
    }

    fn send_alloc(&self, min_size: usize) {
        if let Ok(_) = self.pending_alloc.compare_exchange_weak(
            false,
            true,
            Ordering::SeqCst,
            Ordering::Relaxed,
        ) {
            let prev_cnt = self.comm.num_pool_allocs();
            if !self
                .comm
                .rt_check_alloc(min_size, std::mem::align_of::<CmdMsg>())
            {
                trace!(" {:?} {:?}", prev_cnt, self.comm.num_pool_allocs());
                if prev_cnt == self.comm.num_pool_allocs() {
                    info!(
                        "im responsible for the new alloc of at least {:?}",
                        min_size
                    );
                    self.send_alloc_inner(min_size);
                }
            }
            self.pending_alloc.store(false, Ordering::SeqCst);
        } else {
            while self.pending_alloc.load(Ordering::Relaxed) {
                std::thread::yield_now();
            }
        }
    }

    fn send_panic(&self) {
        let mut panic_buf = self.panic_buffer.lock_blocking();
        self.send_panic_inner(&mut panic_buf);
    }

    fn send_alloc_inner(&self, min_size: usize) {
        debug!("in send_alloc_inner");
        let mut new_alloc = true;
        while new_alloc {
            new_alloc = false;
            let alloc_id = self.alloc_id.fetch_add(1, Ordering::SeqCst);

            let mut my_alloc_buf = self.alloc_buffer[self.my_pe].lock_blocking();
            if my_alloc_buf[0].hash() == self.clear_cmd.hash() {
                my_alloc_buf[0].daddr = alloc_id;
                my_alloc_buf[0].dsize = min_size;
                my_alloc_buf[0].cmd = Cmd::Alloc;
                my_alloc_buf[0].msg_hash = 0;
                my_alloc_buf[0].calc_hash();
                for pe in 0..self.num_pes {
                    if pe != self.my_pe {
                        info!("putting alloc cmd to pe {:?}", pe);
                        let _ = my_alloc_buf.put_unmanaged::<CmdMsg>(my_alloc_buf[0], pe, 0);
                    }
                }
            }

            let mut start = std::time::Instant::now();
            for pe in 0..self.num_pes {
                if pe != self.my_pe {
                    let alloc_buf = self.alloc_buffer[pe].lock_blocking();
                    while !alloc_buf[0].check_hash() || alloc_buf[0].cmd != Cmd::Alloc {
                        self.comm.thread_flush();
                        std::thread::yield_now();
                        if start.elapsed().as_secs_f64() > config().deadlock_warning_timeout {
                            info!(
                                "waiting to alloc from[{pe}]: {:?} {:?} {:?}",
                                alloc_buf[0].cmd,
                                alloc_buf.as_slice(),
                                alloc_id,
                            );
                            start = std::time::Instant::now();
                        }
                    }
                    info!(" pe {:?} ready to alloc {:?} {:?}", pe, alloc_id, min_size);
                }
            }
            info!("all pes ready to alloc");

            self.comm.alloc_pool(min_size);
            info!("allocated new pool of at least {:?}", min_size);

            my_alloc_buf[0].daddr = 0;
            my_alloc_buf[0].dsize = 0;
            my_alloc_buf[0].cmd = Cmd::Clear;
            my_alloc_buf[0].msg_hash = 0;
            my_alloc_buf[0].calc_hash();
            for pe in 0..self.num_pes {
                if pe != self.my_pe {
                    let _ = my_alloc_buf.put_unmanaged::<CmdMsg>(my_alloc_buf[0], pe, 0);
                }
            }
            info!("sent clear cmds");
            for pe in 0..self.num_pes {
                if pe == self.my_pe {
                    while !my_alloc_buf[0].check_hash() || my_alloc_buf[0].cmd != Cmd::Clear {
                        if my_alloc_buf[0].cmd == Cmd::Alloc {
                            if my_alloc_buf[0].daddr > alloc_id {
                                new_alloc = true;
                                break;
                            }
                        }
                    }
                } else {
                    let alloc_buf = self.alloc_buffer[pe].lock_blocking();
                    while !alloc_buf[0].check_hash() || alloc_buf[0].cmd != Cmd::Clear {
                        if alloc_buf[0].cmd == Cmd::Alloc {
                            if alloc_buf[0].daddr > alloc_id {
                                new_alloc = true;
                                break;
                            }
                        }
                    }
                }
                std::thread::yield_now();
            }
            info!("created new alloc pool");
        }
    }

    fn send_panic_inner(&self, panic_buf: &mut CommSlice<CmdMsg>) {
        if panic_buf[self.my_pe].hash() == self.clear_cmd.hash() {
            let cmd = &mut panic_buf[self.my_pe];
            cmd.daddr = 0;
            cmd.dsize = 0;
            cmd.cmd = Cmd::Panic;
            cmd.msg_hash = 0;
            cmd.calc_hash();
            for pe in 0..self.num_pes {
                if pe != self.my_pe {
                    let _ = panic_buf.put_unmanaged::<CmdMsg>(panic_buf[self.my_pe], pe, self.my_pe);
                }
            }
            self.comm.thread_wait();
        }
    }

    fn send_ready(&self, src: usize, cmd: CmdMsg, alloc_addr: usize) {
        debug!("send_ready to src[{src}]: alloc_addr={alloc_addr:#x}");
        let send_buf = self.send_buffer[self.my_pe].read();
        let mut ready = CmdMsg {
            daddr: alloc_addr,
            dsize: 0,
            msg_hash: cmd.msg_hash,
            cmd: Cmd::Ready,
            cmd_hash: 0,
        };
        ready.calc_hash();
        debug!("sending ready cmd to src[{src}]: {:?}", ready);
        let _ = send_buf.put_unmanaged::<CmdMsg>(ready, src, 0);
    }
}

#[lamellar_prof::prof]
impl Drop for InnerCQ {
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop InnerCQ");
        debug!("dropping InnerCQ");
        let old = std::mem::replace(
            Arc::get_mut(&mut self.release_cmd).unwrap(),
            Box::new(CmdMsg::default()),
        );
        let _ = Box::into_raw(old);
        let old = std::mem::replace(
            Arc::get_mut(&mut self.clear_cmd).unwrap(),
            Box::new(CmdMsg::default()),
        );
        let _ = Box::into_raw(old);
        let old = std::mem::replace(
            Arc::get_mut(&mut self.free_cmd).unwrap(),
            Box::new(CmdMsg::default()),
        );
        let _ = Box::into_raw(old);
        debug!("dropped InnerCQ");
        trace!(target: "drop", "end drop InnerCQ");
    }
}

pub(crate) struct CQPut3 {
    cq: Arc<InnerCQ>,
    _send_buffer: CommAlloc,
    _recv_buffer: CommAlloc,
    _alloc_buffer: CommAlloc,
    _panic_buffer: CommAlloc,
    _release_cmd: CommAlloc,
    _clear_cmd: CommAlloc,
    _free_cmd: CommAlloc,
    _eager_recv_alloc: CommAlloc,
    _eager_send_acks_alloc: CommAlloc,
    _comm: Arc<Comm>,
    pub(crate) scheduler: Arc<Scheduler>,
    active: Arc<AtomicU8>,
}

#[lamellar_prof::prof]
impl CQPut3 {
    pub(crate) fn new(
        comm: Arc<Comm>,
        scheduler: Arc<Scheduler>,
        my_pe: usize,
        num_pes: usize,
        active: Arc<AtomicU8>,
    ) -> CQPut3 {
        let send_buffer = comm
            .rt_alloc(
                num_pes * std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap();
        trace!("send_buffer {:?}", send_buffer);
        let recv_buffer = comm
            .rt_alloc(
                num_pes * std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap();
        trace!("recv_buffer {:?}", recv_buffer);
        let alloc_buffer = comm
            .rt_alloc(
                num_pes * std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap();
        trace!("alloc_buffer {:?}", alloc_buffer);
        let panic_buffer = comm
            .rt_alloc(
                num_pes * std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap();
        trace!("panic_buffer {:?}", panic_buffer);
        let release_cmd = comm
            .rt_alloc(
                std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap();
        trace!(
            "release_cmd {:?} {:?} {:?}",
            release_cmd,
            std::mem::size_of::<CmdMsg>(),
            std::mem::align_of::<CmdMsg>()
        );
        let clear_cmd = comm
            .rt_alloc(
                std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap();
        trace!("clear_cmd {:?}", clear_cmd);
        let free_cmd = comm
            .rt_alloc(
                std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap();
        trace!("free_cmd {:?}", free_cmd);

        // Eager recv ring: one row per source PE, RING_SIZE slots per row.
        let eager_recv_alloc = comm
            .rt_alloc(
                num_pes * EAGER_RING_SIZE * EAGER_SLOT_SIZE,
                std::mem::align_of::<u64>(),
            )
            .unwrap();
        trace!("eager_recv_alloc {:?}", eager_recv_alloc);

        // Send-ack counters: one u64 per destination PE.
        let eager_send_acks_alloc = comm
            .rt_alloc(
                num_pes * std::mem::size_of::<u64>(),
                std::mem::align_of::<u64>(),
            )
            .unwrap();
        trace!("eager_send_acks_alloc {:?}", eager_send_acks_alloc);

        let cq = InnerCQ::new(
            send_buffer.clone(),
            recv_buffer.clone(),
            alloc_buffer.clone(),
            panic_buffer.clone(),
            release_cmd.clone(),
            clear_cmd.clone(),
            free_cmd.clone(),
            eager_recv_alloc.clone(),
            eager_send_acks_alloc.clone(),
            comm.clone(),
            scheduler.clone(),
            my_pe,
            num_pes,
            active.clone(),
        );
        trace!("created InnerCQ");
        CQPut3 {
            cq: Arc::new(cq),
            _send_buffer: send_buffer,
            _recv_buffer: recv_buffer,
            _alloc_buffer: alloc_buffer,
            _panic_buffer: panic_buffer,
            _release_cmd: release_cmd,
            _clear_cmd: clear_cmd,
            _free_cmd: free_cmd,
            _eager_recv_alloc: eager_recv_alloc,
            _eager_send_acks_alloc: eager_send_acks_alloc,
            _comm: comm,
            scheduler,
            active,
        }
    }

    pub(crate) async fn send_alloc(&self, min_size: usize) {
        self.cq.send_alloc(min_size);
    }

    pub(crate) fn send_panic(&self) {
        self.cq.send_panic();
    }

    pub(crate) async fn send_data(&self, data: SerializedData, dst: usize) {
        let mut hash = calc_hash(data.ser_data_bytes.usize_addr(), data.len());
        if hash == 0 {
            hash = 1;
        }
        let data_slice = data.ser_data_bytes.clone();
        self.cq.send(data_slice, dst, hash).await;
    }

    pub(crate) async fn send_vec(&self, vec_data: Vec<u8>, dst: usize) {
        debug!("sending vec_data of len {:?} to dst {:?}", vec_data.len(), dst);
        let vec_data_addr = vec_data.as_ptr() as usize;
        let mut hash = calc_hash(vec_data_addr, vec_data.len());
        if hash == 0 {
            hash = 1;
        }
        self.cq.send_vec(vec_data, dst, hash).await;
    }

    pub(crate) fn wait_all_print(&self) {
        println!("command queue");
        println!(
            "sends {:?}",
            print_stats!(PE_SENDS
                .iter()
                .map(|x| x
                    .iter()
                    .map(|y| y.load(Ordering::SeqCst))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>())
        );
        println!(
            "recvs {:?}",
            print_stats!(PE_RECVS
                .iter()
                .map(|x| x
                    .iter()
                    .map(|y| y.load(Ordering::SeqCst))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>())
        );
        for pe in 0..self.cq.num_pes {
            let mut sends = Vec::new();
            stats!(
                sends = PE_SENDS
                    .iter()
                    .map(|x| x[pe].load(Ordering::SeqCst))
                    .collect::<Vec<_>>()
            );
            let mut recvs = Vec::new();
            stats!(
                recvs = PE_RECVS
                    .iter()
                    .map(|x| x[pe].load(Ordering::SeqCst))
                    .collect::<Vec<_>>()
            );
            println!("PE {pe} sends: {:?} recvs: {:?}", sends, recvs);
            let send_buffer = self.cq.send_buffer[pe].read();
            let recv_buffer = self.cq.recv_buffer[pe].read();
            println!(
                "recv_buffer ptr for pe {pe}: {:x}",
                recv_buffer.usize_addr()
            );
            println!(
                "send_buffer ptr for pe {pe}: {:x}",
                send_buffer.usize_addr()
            );
            println!("recv buffer: {:?}", recv_buffer[0]);
            println!("send buffer: {:?}", send_buffer[0]);
        }
        panic!("finished command queue wait_all_print");
    }

    pub(crate) async fn alloc_task(&self) {
        // let mut timer = std::time::Instant::now();
        let print = false;
        while self.scheduler.active(0)
            && self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8
        {
            // if timer.elapsed().as_secs_f64() > 10.0 {
            //     trace!("alloc_task still running");
            //     timer = std::time::Instant::now();
            //     print = true;
            // }
            self.cq.check_alloc(print);
            // print = false;
            // async_std::task::yield_now().await;
            async_std::task::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    pub(crate) async fn panic_task(&self) {
        let mut panic = false;
        while self.scheduler.active(0) && !panic {
            panic = self.cq.check_panic();
            async_std::task::sleep(std::time::Duration::from_millis(1000)).await;
        }
        if panic {
            warn!("received panic from other PE");
            panic!("received panic from other PE");
        }
    }

    pub(crate) async fn recv_data(&self, lamellae: Arc<Lamellae>) {
        let comm = lamellae.comm();
        let num_pes = comm.num_pes();
        let my_pe = comm.my_pe();
        let mut timer = std::time::Instant::now();
        while self.active.load(Ordering::SeqCst) == CmdQStatus::Active as u8
            || !self.cq.empty().await
            || self.scheduler.active(0)
        {
            if timer.elapsed().as_secs_f64() > 5.0 {
                debug!(
                    "recv_data still running -- cq empty? {:?}  scheduler active? {:?}",
                    self.cq.empty().await,
                    self.scheduler.active(0)
                );
                timer = std::time::Instant::now();
            }
            for src in 0..num_pes {
                if src == my_pe {
                    continue;
                }

                // ── Rendezvous receive ────────────────────────────────────────
                if let Some(cmd) = self.cq.ready(src) {
                    debug!("recv_data {:?}", cmd);
                    match cmd.cmd {
                        Cmd::Alloc => panic!("should not encounter alloc here"),
                        Cmd::Panic => panic!("should not encounter panic here"),
                        Cmd::Clear | Cmd::Release | Cmd::Free | Cmd::Ready => {
                            panic!("should not be possible to see cmd clear, release, free, or ready here")
                        }
                        Cmd::Tx => {
                            let msg_id = MSG_ID.fetch_add(1, Ordering::SeqCst);
                            let size = cmd.dsize;
                            debug!("got tx from {src} size={size} msg_id={msg_id}");

                            // INLINE: allocate recv buffer before spawning io_task so Ready
                            // is sent without depending on io_task scheduling.
                            let mut ser_data = loop {
                                match self.cq.comm.new_serialized_data(
                                    size + std::mem::size_of::<u64>(),
                                ) {
                                    Ok(sd) => break sd,
                                    Err(_) => {
                                        debug!("recv_data: waiting for alloc size={size} src={src} msg_id={msg_id}");
                                        self.cq.send_alloc(size + std::mem::size_of::<u64>());
                                        self.cq.comm.thread_flush();
                                        async_std::task::yield_now().await;
                                    }
                                }
                            };

                            let data_addr = ser_data.ser_data_bytes.usize_addr();
                            let done_flag_addr = data_addr + size;

                            debug!(
                                "recv_data: allocated for src[{src}] size={size} data_addr={data_addr:#x} done_flag_addr={done_flag_addr:#x} msg_id={msg_id}"
                            );

                            unsafe { (done_flag_addr as *mut u64).write_unaligned(0u64) };

                            // INLINE: fire Ready immediately.
                            self.cq.send_ready(src, cmd, data_addr);
                            debug!("recv_data: sent ready to src[{src}] msg_id={msg_id}");

                            // io_task: poll done flag, submit AM.
                            let cq = self.cq.clone();
                            let lamellae = lamellae.clone();
                            let scheduler1 = self.scheduler.clone();
                            let task = async move {
                                let magic = cmd.msg_hash as u64;
                                let mut timer = std::time::Instant::now();
                                loop {
                                    let val = unsafe {
                                        (done_flag_addr as *mut u64).read_unaligned()
                                    };
                                    if val == magic {
                                        break;
                                    }
                                    if timer.elapsed().as_secs_f64() > 10.0 {
                                        debug!(
                                            "msg_id: {msg_id} waiting for done flag from {src} magic={magic:#x} cur={val:#x}"
                                        );
                                        timer = std::time::Instant::now();
                                    }
                                    cq.comm.thread_flush();
                                    async_std::task::yield_now().await;
                                }

                                stats!(PE_RECVS[0][src].fetch_add(1, Ordering::SeqCst));
                                debug!(
                                    "msg_id: {msg_id} done flag received from {src}, submitting remote am"
                                );

                                cq.recv_cnt.fetch_add(1, Ordering::SeqCst);
                                scheduler1.submit_remote_am(
                                    ser_data.drop_payload_bytes(std::mem::size_of::<u64>()),
                                    &lamellae,
                                );
                            };
                            self.scheduler.submit_io_task(task);
                        }
                    }
                }

                // ── Eager receive: check the head slot of src's ring row ──────
                {
                    let head = self.cq.eager_recv_head[src].load(Ordering::Relaxed);
                    let slot_idx = head % EAGER_RING_SIZE;
                    let byte_offset = (src * EAGER_RING_SIZE + slot_idx) * EAGER_SLOT_SIZE;
                    let magic_addr =
                        self.cq.eager_recv_alloc_addr + byte_offset + EAGER_DATA_SIZE;
                    // Volatile read so the compiler doesn't hoist this out of the loop.
                    let magic =
                        unsafe { (magic_addr as *const u64).read_volatile() };

                    if magic != 0 {
                        let size = magic as usize;
                        debug!(
                            "eager recv from src={src} slot={slot_idx} size={size}"
                        );

                        let data_addr = self.cq.eager_recv_alloc_addr + byte_offset;

                        // Allocate output buffer; retry with alloc pressure until success.
                        let ser_data = loop {
                            match self.cq.comm.new_serialized_data(size) {
                                Ok(sd) => break sd,
                                Err(_) => {
                                    self.cq.send_alloc(size);
                                    self.cq.comm.thread_flush();
                                    async_std::task::yield_now().await;
                                }
                            }
                        };

                        // Copy from the ring slot into the fresh allocation.
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                data_addr as *const u8,
                                ser_data.ser_data_bytes.usize_addr() as *mut u8,
                                size,
                            );
                        }

                        // Zero magic to recycle the slot before sending the ack.
                        unsafe { (magic_addr as *mut u64).write_volatile(0u64) };

                        // Advance head and send ack.
                        self.cq.eager_recv_head[src].fetch_add(1, Ordering::SeqCst);
                        let processed =
                            self.cq.eager_recv_processed[src]
                                .fetch_add(1, Ordering::SeqCst)
                                + 1;
                        self.cq.send_eager_ack(src, processed);

                        stats!(PE_RECVS[0][src].fetch_add(1, Ordering::SeqCst));
                        self.cq.recv_cnt.fetch_add(1, Ordering::SeqCst);
                        self.scheduler.submit_remote_am(ser_data, &lamellae);
                    }
                }
            }

            comm.thread_flush();
            async_std::task::yield_now().await;
        }
        self.active
            .store(CmdQStatus::Finished as u8, Ordering::SeqCst);
    }

    pub(crate) fn mem_per_pe() -> usize {
        4 * std::mem::size_of::<CmdMsg>()
            + EAGER_RING_SIZE * EAGER_SLOT_SIZE
            + std::mem::size_of::<u64>()
    }

    pub(crate) fn available_to_send(&self, pe: usize) -> bool {
        self.cq.available_to_send(pe)
    }
}

#[lamellar_prof::prof]
impl Drop for CQPut3 {
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop CQPut3");
        debug!(
            "sends {:?}",
            print_stats!(PE_SENDS
                .iter()
                .map(|x| x
                    .iter()
                    .map(|y| y.load(Ordering::SeqCst))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>())
        );
        debug!(
            "recvs {:?}",
            print_stats!(PE_RECVS
                .iter()
                .map(|x| x
                    .iter()
                    .map(|y| y.load(Ordering::SeqCst))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>())
        );
        trace!(target: "drop", "end drop CQPut3");
    }
}

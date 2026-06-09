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

// N in-flight rendezvous slots per PE pair.
const N: usize = 4;

#[repr(C)]
#[derive(Clone, Copy)]
struct CmdMsg {
    daddr: usize,
    dsize: usize,
    msg_hash: usize,
    cmd_hash: usize,
    // Sender's slot index (0..N). Embedded in Tx and echoed in Ready so the receiver
    // can route Ready to the correct send_buffer[my_pe * N + slot] entry.
    slot: usize,
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
            slot: 0,
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
            .wrapping_add(self.msg_hash)
            .wrapping_add(self.slot);
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
            "daddr {:#x}({:?}) dsize {:?} slot {:?} cmd {:?} msg_hash {:?} cmd_hash {:?}",
            self.daddr, self.daddr, self.dsize, self.slot, self.cmd, self.msg_hash, self.cmd_hash,
        )
    }
}

struct InnerCQ {
    // Flat vec of num_pes * N slots; slot s for PE pe is at index pe * N + s.
    send_buffer: Arc<Vec<RwLock<CommSlice<CmdMsg>>>>,
    recv_buffer: Arc<Vec<RwLock<CommSlice<CmdMsg>>>>,
    // Full CommAllocs used with comm_slice_at_byte_offset to route messages to
    // specific (pe, slot) entries without assuming symmetric VAs.
    send_buffer_comm_alloc: CommAlloc, // receiver PUTs Ready to sender's specific slot
    recv_buffer_comm_alloc: CommAlloc, // sender PUTs Tx to receiver's specific slot
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
        send_buffer_comm_alloc: CommAlloc,
        recv_buffer_comm_alloc: CommAlloc,
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
            (*cmd).slot = 0;
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
            (*cmd).slot = 0;
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
            (*cmd).slot = 0;
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
            (*cmd).slot = 0;
            (*cmd).cmd = Cmd::Clear;
            (*cmd).msg_hash = 0;
            (*cmd).calc_hash();
        }
        trace!("panic_buffer init {:?}", panic_buffer);

        let mut release_cmd = unsafe { Box::from_raw(release_cmd_alloc.as_mut_ptr::<CmdMsg>()) };
        release_cmd.daddr = 1;
        release_cmd.dsize = 1;
        release_cmd.slot = 0;
        release_cmd.cmd = Cmd::Release;
        release_cmd.msg_hash = 1;
        release_cmd.calc_hash();

        let mut clear_cmd = unsafe { Box::from_raw(clear_cmd_alloc.as_mut_ptr::<CmdMsg>()) };
        clear_cmd.daddr = 0;
        clear_cmd.dsize = 0;
        clear_cmd.slot = 0;
        clear_cmd.cmd = Cmd::Clear;
        clear_cmd.msg_hash = 0;
        clear_cmd.calc_hash();

        let mut free_cmd = unsafe { Box::from_raw(free_cmd_alloc.as_mut_ptr::<CmdMsg>()) };
        free_cmd.daddr = 0;
        free_cmd.dsize = 0;
        free_cmd.slot = 0;
        free_cmd.cmd = Cmd::Free;
        free_cmd.msg_hash = 0;
        free_cmd.calc_hash();

        InnerCQ {
            send_buffer: Arc::new(send_buffers),
            recv_buffer: Arc::new(recv_buffers),
            send_buffer_comm_alloc,
            recv_buffer_comm_alloc,
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
        }
    }

    async fn empty(&self) -> bool {
        for dst in 0..self.num_pes {
            if dst == self.my_pe {
                continue;
            }
            for s in 0..N {
                let buf = self.send_buffer[dst * N + s].read();
                if buf[0].cmd == Cmd::Tx || buf[0].cmd == Cmd::Ready {
                    return false;
                }
            }
        }
        true
    }

    fn available_to_send(&self, pe: usize) -> bool {
        (0..N).any(|s| {
            let buf = self.send_buffer[pe * N + s].read();
            buf[0].hash() == self.clear_cmd.hash()
        })
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

    // Two-phase send (CommSlice<u8> variant):
    // Phase 1: scan N slots, claim first clear, embed slot index, PUT Tx to receiver.
    // Phase 2: spawn task that polls the exact claimed slot for Ready, then PUTs data.
    async fn send(&self, data: CommSlice<u8>, dst: usize, hash: usize) {
        stats!(PE_SENDS[0][dst].fetch_add(1, Ordering::SeqCst));
        debug!("want to send data {:?} {:x}", dst, hash);

        // Phase 1: claim a slot.
        let s = 'outer: loop {
            if self.active.load(Ordering::SeqCst) == CmdQStatus::Panic as u8 {
                return;
            }
            for s in 0..N {
                let mut send_buf = self.send_buffer[dst * N + s].write();
                if send_buf[0].hash() == self.clear_cmd.hash() {
                    send_buf[0].daddr = data.usize_addr();
                    send_buf[0].dsize = data.len();
                    send_buf[0].msg_hash = hash;
                    send_buf[0].slot = s;
                    send_buf[0].cmd = Cmd::Tx;
                    send_buf[0].calc_hash();

                    let send_cmd = send_buf[0].clone();
                    let num_bytes = send_cmd.as_bytes().len();
                    drop(send_buf);

                    // PUT Tx to dst's recv_buffer[my_pe * N + s] via CommAlloc remote_keys.
                    let byte_offset = (self.my_pe * N + s) * std::mem::size_of::<CmdMsg>();
                    self.recv_buffer_comm_alloc
                        .comm_slice_at_byte_offset::<CmdMsg>(byte_offset, 1)
                        .put_unmanaged::<CmdMsg>(send_cmd, dst, 0);

                    self.put_amt.fetch_add(num_bytes, Ordering::Relaxed);
                    stats!(PE_SENDS[1][dst].fetch_add(1, Ordering::SeqCst));
                    break 'outer s;
                }
            }
            self.comm.thread_flush();
            async_std::task::yield_now().await;
        };

        // Phase 2: spawn separate task to poll send_buffer[dst * N + s] for Ready.
        // Capturing s avoids scanning all N slots in the hot path.
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
                    let send_buf = send_buffer[dst * N + s].read();
                    if send_buf[0].cmd == Cmd::Ready && send_buf[0].check_hash() {
                        Some(send_buf[0].clone())
                    } else {
                        None
                    }
                };
                if let Some(ready) = maybe_ready {
                    send_buffer[dst * N + s].write()[0] = **clear_cmd;
                    debug!("got ready from dst({dst}) slot({s}) {:?}", ready);

                    let dst_data_full = comm.one_sided_alloc_from_remote_pe_and_addr(
                        dst, ready.daddr, data.len() + std::mem::size_of::<u64>());
                    let dst_data = dst_data_full.comm_slice_at_byte_offset::<u8>(0, data.len());
                    let magic_data = dst_data_full.comm_slice_at_byte_offset::<u64>(data.len(), 1);
                    dst_data.put_buffer::<u8>(&scheduler, None, data.clone(), dst, 0).await;
                    magic_data.put_unmanaged::<u64>(ready.msg_hash as u64, dst, 0);

                    sent_cnt.fetch_add(1, Ordering::SeqCst);
                    break;
                }
                comm.thread_flush();
                async_std::task::yield_now().await;
            }
        });
    }

    // Two-phase send (Vec<u8> variant): same slot-scanning logic as send().
    async fn send_vec(&self, mut data: Vec<u8>, dst: usize, hash: usize) {
        stats!(PE_SENDS[0][dst].fetch_add(1, Ordering::SeqCst));
        debug!("want to send vec data {:?} {:x}", dst, hash);

        // Phase 1: claim a slot.
        let s = 'outer: loop {
            if self.active.load(Ordering::SeqCst) == CmdQStatus::Panic as u8 {
                return;
            }
            for s in 0..N {
                let mut send_buf = self.send_buffer[dst * N + s].write();
                if send_buf[0].hash() == self.clear_cmd.hash() {
                    send_buf[0].daddr = 0;
                    send_buf[0].dsize = data.len();
                    send_buf[0].msg_hash = hash;
                    send_buf[0].slot = s;
                    send_buf[0].cmd = Cmd::Tx;
                    send_buf[0].calc_hash();

                    let send_cmd = send_buf[0].clone();
                    let num_bytes = send_cmd.as_bytes().len();
                    drop(send_buf);

                    let byte_offset = (self.my_pe * N + s) * std::mem::size_of::<CmdMsg>();
                    self.recv_buffer_comm_alloc
                        .comm_slice_at_byte_offset::<CmdMsg>(byte_offset, 1)
                        .put_unmanaged::<CmdMsg>(send_cmd, dst, 0);

                    self.put_amt.fetch_add(num_bytes, Ordering::Relaxed);
                    stats!(PE_SENDS[1][dst].fetch_add(1, Ordering::SeqCst));
                    break 'outer s;
                }
            }
            self.comm.thread_flush();
            async_std::task::yield_now().await;
        };

        // Phase 2: spawn task polling send_buffer[dst * N + s].
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
                    let send_buf = send_buffer[dst * N + s].read();
                    if send_buf[0].cmd == Cmd::Ready && send_buf[0].check_hash() {
                        Some(send_buf[0].clone())
                    } else {
                        None
                    }
                };
                if let Some(ready) = maybe_ready {
                    send_buffer[dst * N + s].write()[0] = **clear_cmd;
                    debug!("got ready from dst({dst}) slot({s}) {:?}", ready);
                    data.extend_from_slice(&ready.msg_hash.to_ne_bytes());

                    let dst_data_full = comm.one_sided_alloc_from_remote_pe_and_addr(
                        dst, ready.daddr, data.len());

                    if let Ok(rt_data) = comm.rt_alloc(data.len(), std::mem::align_of::<u8>()) {
                        let mut data_slice = rt_data.as_comm_slice::<u8>();
                        data_slice.copy_from_slice(&data);
                        put_amt.fetch_add(data.len(), Ordering::Relaxed);
                        dst_data_full.put_buffer::<u8>(&scheduler, None, data_slice, dst, 0).await;
                    } else {
                        dst_data_full.put_buffer::<u8>(&scheduler, None, data, dst, 0).await;
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
            cmd.slot = 0;
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

    // Receiver writes Ready to sender's send_buffer[sender_pe * N + cmd.slot].
    // Uses send_buffer_comm_alloc.comm_slice_at_byte_offset to target the correct slot
    // without symmetric-VA assumptions.
    fn send_ready(&self, src: usize, cmd: CmdMsg, alloc_addr: usize) {
        debug!(
            "send_ready to src[{src}]: alloc_addr={alloc_addr:#x} slot={}",
            cmd.slot
        );
        let byte_offset = (self.my_pe * N + cmd.slot) * std::mem::size_of::<CmdMsg>();
        let slot = self.send_buffer_comm_alloc
            .comm_slice_at_byte_offset::<CmdMsg>(byte_offset, 1);
        let mut ready = CmdMsg {
            daddr: alloc_addr,
            dsize: 0,
            msg_hash: cmd.msg_hash,
            slot: cmd.slot,
            cmd: Cmd::Ready,
            cmd_hash: 0,
        };
        ready.calc_hash();
        debug!("sending ready cmd to src[{src}]: {:?}", ready);
        let _ = slot.put_unmanaged::<CmdMsg>(ready, src, 0);
    }
}

#[lamellar_prof::prof]
impl Drop for InnerCQ {
    fn drop(&mut self) {
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
    }
}

pub(crate) struct CQPut2N {
    cq: Arc<InnerCQ>,
    _send_buffer: CommAlloc,
    _recv_buffer: CommAlloc,
    _alloc_buffer: CommAlloc,
    _panic_buffer: CommAlloc,
    _release_cmd: CommAlloc,
    _clear_cmd: CommAlloc,
    _free_cmd: CommAlloc,
    _comm: Arc<Comm>,
    pub(crate) scheduler: Arc<Scheduler>,
    active: Arc<AtomicU8>,
}

#[lamellar_prof::prof]
impl CQPut2N {
    pub(crate) fn new(
        comm: Arc<Comm>,
        scheduler: Arc<Scheduler>,
        my_pe: usize,
        num_pes: usize,
        active: Arc<AtomicU8>,
    ) -> CQPut2N {
        let send_buffer = comm
            .rt_alloc(
                num_pes * N * std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap();
        trace!("send_buffer {:?}", send_buffer);
        let send_buffer_comm_alloc = send_buffer.clone();

        let recv_buffer = comm
            .rt_alloc(
                num_pes * N * std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap();
        trace!("recv_buffer {:?}", recv_buffer);
        let recv_buffer_comm_alloc = recv_buffer.clone();

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

        let cq = InnerCQ::new(
            send_buffer.clone(),
            recv_buffer.clone(),
            alloc_buffer.clone(),
            panic_buffer.clone(),
            release_cmd.clone(),
            clear_cmd.clone(),
            free_cmd.clone(),
            send_buffer_comm_alloc,
            recv_buffer_comm_alloc,
            comm.clone(),
            scheduler.clone(),
            my_pe,
            num_pes,
            active.clone(),
        );
        trace!("created InnerCQ");
        CQPut2N {
            cq: Arc::new(cq),
            _send_buffer: send_buffer,
            _recv_buffer: recv_buffer,
            _alloc_buffer: alloc_buffer,
            _panic_buffer: panic_buffer,
            _release_cmd: release_cmd,
            _clear_cmd: clear_cmd,
            _free_cmd: free_cmd,
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
        if hash == 0 { hash = 1; }
        let data_slice = data.ser_data_bytes.clone();
        self.cq.send(data_slice, dst, hash).await;
    }

    pub(crate) async fn send_vec(&self, vec_data: Vec<u8>, dst: usize) {
        debug!("sending vec_data of len {:?} to dst {:?}", vec_data.len(), dst);
        let vec_data_addr = vec_data.as_ptr() as usize;
        let mut hash = calc_hash(vec_data_addr, vec_data.len());
        if hash == 0 { hash = 1; }
        self.cq.send_vec(vec_data, dst, hash).await;
    }

    pub(crate) fn wait_all_print(&self) {
        println!("command queue (Put2N, N={N})");
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
            for s in 0..N {
                let send_buffer = self.cq.send_buffer[pe * N + s].read();
                let recv_buffer = self.cq.recv_buffer[pe * N + s].read();
                println!("  slot {s}: send={:?} recv={:?}", send_buffer[0], recv_buffer[0]);
            }
        }
        // panic!("finished command queue wait_all_print");
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
                // Scan all N recv slots for this src.
                for slot_idx in 0..N {
                    let cmd_opt = {
                        let buf = self.cq.recv_buffer[src * N + slot_idx].read();
                        if buf[0].cmd == Cmd::Tx && buf[0].check_hash() {
                            Some(buf[0].clone())
                        } else {
                            None
                        }
                    };
                    if let Some(cmd) = cmd_opt {
                        // Clear the recv slot before allocating so the sender can reuse it.
                        self.cq.recv_buffer[src * N + slot_idx].write()[0] =
                            **self.cq.clear_cmd;

                        debug!("recv_data: Tx from {src} slot {slot_idx} size={}", cmd.dsize);
                        let msg_id = MSG_ID.fetch_add(1, Ordering::SeqCst);
                        let size = cmd.dsize;

                        let mut ser_data = loop {
                            match self.cq.comm.new_serialized_data(size + std::mem::size_of::<u64>()) {
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

                        self.cq.send_ready(src, cmd, data_addr);
                        debug!("recv_data: sent ready to src[{src}] slot {slot_idx} msg_id={msg_id}");

                        let cq = self.cq.clone();
                        let lamellae = lamellae.clone();
                        let scheduler1 = self.scheduler.clone();
                        let task = async move {
                            let magic = cmd.msg_hash as u64;
                            let mut timer = std::time::Instant::now();
                            loop {
                                let val = unsafe { (done_flag_addr as *mut u64).read_unaligned() };
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
                                lamellae,
                            );
                        };
                        self.scheduler.submit_io_task(task);
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
        // N send slots + N recv slots + 1 alloc + 1 panic (amortized).
        (2 * N + 2) * std::mem::size_of::<CmdMsg>()
    }

    pub(crate) fn available_to_send(&self, pe: usize) -> bool {
        self.cq.available_to_send(pe)
    }
}

#[lamellar_prof::prof]
impl Drop for CQPut2N {
    fn drop(&mut self) {
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
    }
}

use super::{
    comm::{CmdQStatus, CommAlloc, CommInfo, CommMem, CommProgress, CommSlice},
    Comm, Lamellae, SerializedData,
};
use crate::{
    active_messaging::AMCounters,
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
    //#[tracing::instrument(skip_all, level = "debug")]
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
    //#[tracing::instrument(skip_all, level = "debug")]
    fn default() -> Self {
        Cmd::Clear
    }
}

//#[tracing::instrument(skip_all, level = "debug")]
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
    //#[tracing::instrument(skip_all, level = "debug")]
    fn as_bytes(&self) -> &[u8] {
        let pointer = self as *const Self as *const u8;
        let size = std::mem::size_of::<Self>();
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(pointer, size) };
        slice
    }

    //#[tracing::instrument(skip_all, level = "debug")]
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
    //#[tracing::instrument(skip_all, level = "debug")]
    fn calc_hash(&mut self) {
        self.cmd_hash = self.hash()
    }
    //#[tracing::instrument(skip_all, level = "debug")]
    fn check_hash(&self) -> bool {
        self.cmd_hash == self.hash() && self.cmd_hash != 0
    }
}

#[lamellar_prof::prof]
impl std::fmt::Debug for CmdMsg {
    //#[tracing::instrument(skip_all, level = "debug")]
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
}

#[lamellar_prof::prof]
impl InnerCQ {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn new(
        send_buffer_alloc: CommAlloc,
        recv_buffer_alloc: CommAlloc,
        alloc_buffer_alloc: CommAlloc,
        panic_buffer_alloc: CommAlloc,
        release_cmd_alloc: CommAlloc,
        clear_cmd_alloc: CommAlloc,
        free_cmd_alloc: CommAlloc,
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
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
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

    //#[tracing::instrument(skip_all, level = "debug")]
    fn ready(&self, src: usize) -> Option<CmdMsg> {
        let mut recv_buffer = self.recv_buffer[src].write();
        let cmd = recv_buffer[0].clone();
        if cmd.check_hash() {
            match cmd.cmd {
                Cmd::Clear => None,
                Cmd::Tx => {
                    // if cmd.daddr == 0 {
                    //     return None;
                    // }
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

    //#[tracing::instrument(skip_all, level = "debug")]
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

    //#[tracing::instrument(skip_all, level = "debug")]
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


      // Two-phase send:
    // Phase 1: wait for Clear slot, fill Tx, PUT CmdMsg to dst's recv_buffer.
    // Phase 2: poll send_buffer[dst] for Ready (written by dst after allocating a receive buffer),
    //          then PUT data via put_buffer().await, free source, PUT magic terminator, reset slot.
    //#[tracing::instrument(skip_all, level = "debug")]
    async fn send(&self, data: CommSlice<u8>, dst: usize, hash: usize) {
        stats!(PE_SENDS[0][dst].fetch_add(1, Ordering::SeqCst));
        debug!("want to send data {:?} {:x}", dst, hash);
        let mut printed = false;

        // Phase 1: wait for Clear, fill Tx, signal dst
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
                    let _ = recv_buffer.put_unmanaged::<CmdMsg>( send_cmd, dst, 0);
                    self.put_amt
                        .fetch_add(num_bytes, Ordering::Relaxed);
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

        // Phase 2: spawn as a separate regular-priority task so it doesn't hold a
        // thread slot while polling for Ready.  This lets recv_data (and other tasks
        // in work_inj) be scheduled between polling iterations, breaking the circular
        // starvation where all threads are occupied by Phase-2 senders.
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
                        dst, ready.daddr, data.len() + std::mem::size_of::<u64>());
                    let dst_data = dst_data_full.comm_slice_at_byte_offset::<u8>(0, data.len());
                    let magic_data = dst_data_full.comm_slice_at_byte_offset::<u64>(data.len(), 1);
                    dst_data.put_buffer::<u8>(&scheduler, vec![], data.clone(), dst, 0).await;
                    magic_data.put_unmanaged::<u64>(ready.msg_hash as u64, dst, 0);

                    sent_cnt.fetch_add(1, Ordering::SeqCst);
                    break;
                }
                comm.thread_flush();
                async_std::task::yield_now().await;
            }
        });
    }

    // Two-phase send:
    // Phase 1: wait for Clear slot, fill Tx, PUT CmdMsg to dst's recv_buffer.
    // Phase 2: poll send_buffer[dst] for Ready (written by dst after allocating a receive buffer),
    //          then PUT data via put_buffer().await, free source, PUT magic terminator, reset slot.
    //#[tracing::instrument(skip_all, level = "debug")]
    async fn send_vec(&self, mut data: Vec<u8>, dst: usize, hash: usize) {
        stats!(PE_SENDS[0][dst].fetch_add(1, Ordering::SeqCst));
        debug!("want to send data {:?} {:x}", dst, hash);
        let mut printed = false;

        // Phase 1: wait for Clear, fill Tx, signal dst
        loop {
            if self.active.load(Ordering::SeqCst) == CmdQStatus::Panic as u8 {
                return;
            }
            {
                let mut send_buf = self.send_buffer[dst].write();
                if send_buf[0].hash() == self.clear_cmd.hash() {
                    send_buf[0].daddr = 0; // not used in Tx, but set to 0 for cleanliness
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
                    debug!("put cmd msg to dst({dst}) recv buffer, cmd msg size {:?} bytes", num_bytes);
                    self.put_amt
                        .fetch_add(num_bytes, Ordering::Relaxed);
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

        // Phase 2: spawn as a separate regular-priority task (same rationale as send()).
        let send_buffer = self.send_buffer.clone();
        let comm = self.comm.clone();
        let scheduler = self.scheduler.clone();
        let clear_cmd = self.clear_cmd.clone();
        let sent_cnt = self.sent_cnt.clone();
        let put_amt = self.put_amt.clone();
        let active = self.active.clone();
        self.scheduler.submit_io_task(async move {
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
                        dst, ready.daddr, data.len() );
                    // let dst_data = dst_data_full.comm_slice_at_byte_offset::<u8>(0, data.len());
                    // let magic_data = dst_data_full.comm_slice_at_byte_offset::<u64>(data.len(), 1);

                    if let Ok(rt_data) = comm.rt_alloc(data.len(), std::mem::align_of::<u8>()) {
                        let mut data_slice = rt_data.as_comm_slice::<u8>();
                        data_slice.copy_from_slice(&data);
                        put_amt.fetch_add(data.len(), Ordering::Relaxed);
                        dst_data_full.put_buffer::<u8>(&scheduler, vec![], data_slice, dst, 0).await;
                    } else {
                        dst_data_full.put_buffer::<u8>(&scheduler, vec![], data, dst, 0).await;
                    }

                    // magic_data.put_unmanaged::<u64>(ready.msg_hash as u64, dst, 0);

                    sent_cnt.fetch_add(1, Ordering::SeqCst);
                    return;
                }
                comm.thread_flush();
                async_std::task::yield_now().await;
            }
        });
    }

    //#[tracing::instrument(skip_all, level = "debug")]
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

    //#[tracing::instrument(skip_all, level = "debug")]
    fn send_panic(&self) {
        let mut panic_buf = self.panic_buffer.lock_blocking();
        self.send_panic_inner(&mut panic_buf);
    }

    //#[tracing::instrument(skip_all, level = "debug")]
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

    //#[tracing::instrument(skip_all, level = "debug")]
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

    // PE1 (receiver) writes a Ready CmdMsg to PE0's send_buffer[PE1] slot via ack_addr.
    // ready.daddr = allocated data buffer address on PE1
    // ready.ack_addr = done flag address on PE1 (PE0 will PUT magic here to signal completion)
    //#[tracing::instrument(skip_all, level = "debug")]
    fn send_ready(&self, src: usize, cmd: CmdMsg, alloc_addr: usize) {
        debug!(
            "send_ready to src[{src}]: alloc_addr={alloc_addr:#x} "
        );
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
    //#[tracing::instrument(skip_all, level = "debug")]
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

pub(crate) struct CQPut2 {
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
impl CQPut2 {
    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new(
        comm: Arc<Comm>,
        scheduler: Arc<Scheduler>,
        my_pe: usize,
        num_pes: usize,
        active: Arc<AtomicU8>,
    ) -> CQPut2 {
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

        let cq = InnerCQ::new(
            send_buffer.clone(),
            recv_buffer.clone(),
            alloc_buffer.clone(),
            panic_buffer.clone(),
            release_cmd.clone(),
            clear_cmd.clone(),
            free_cmd.clone(),
            comm.clone(),
            scheduler.clone(),
            my_pe,
            num_pes,
            active.clone(),
        );
        trace!("created InnerCQ");
        CQPut2 {
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

    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) async fn send_alloc(&self, min_size: usize) {
        self.cq.send_alloc(min_size);
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn send_panic(&self) {
        self.cq.send_panic();
    }

    // Leak alloc before send so memory stays registered during PUT; cq.send() frees it.
    //#[tracing::instrument(skip_all, level = "debug")]
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
        let mut print = false;
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

    //#[tracing::instrument(skip_all, level = "debug")]
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
                if src != my_pe {
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

                                // INLINE: allocate recv buffer on the recv_data thread so Ready
                                // is sent without depending on io_task scheduling.  This breaks
                                // the circular starvation where Phase-2 sender tasks occupy all
                                // threads while waiting for Ready messages that only io_tasks
                                // could send.
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

                                // Zero the done flag before sending Ready so sender can't see
                                // stale magic from a previous allocation.
                                unsafe { (done_flag_addr as *mut u64).write_unaligned(0u64) };

                                // INLINE: fire Ready immediately — sender's Phase-2 poll unblocks.
                                self.cq.send_ready(src, cmd, data_addr);
                                debug!("recv_data: sent ready to src[{src}] msg_id={msg_id}");

                                // Spawn io_task only for done-flag polling + AM submission.
                                // By the time this task runs the sender has usually already PUT
                                // both the data and the magic terminator.
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
                                    // No send_free needed: sender freed source after put_buffer().await
                                };
                                self.scheduler.submit_io_task(task);
                            }
                        }
                    }
                }
            }

            comm.thread_flush();
            async_std::task::yield_now().await;
        }
        self.active
            .store(CmdQStatus::Finished as u8, Ordering::SeqCst);
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn mem_per_pe() -> usize {
        4 * std::mem::size_of::<CmdMsg>()
    }

    pub(crate) fn available_to_send(&self, pe: usize) -> bool {
        self.cq.available_to_send(pe)
    }
}

#[lamellar_prof::prof]
impl Drop for CQPut2 {
    //#[tracing::instrument(skip_all, level = "debug")]
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

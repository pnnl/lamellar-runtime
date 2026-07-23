use super::{
    comm::{CmdQStatus, CommAlloc, CommInfo, CommMem, CommProgress, CommSlice},
    Comm, Lamellae, SerializedData,
};
use crate::{
    env_var::{config, CmdQueue}, lamellae::CommAllocRdma, print_stats, scheduler::Scheduler, stats,
    LamellarBuffer,
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
    ack_addr: usize, // RDMA addr of sender's send_buffer[dst] slot, used to write back Free
    msg_hash: usize,
    cmd_hash: usize,
    cmd: Cmd,
}

impl Default for CmdMsg {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn default() -> Self {
        CmdMsg {
            daddr: 0,
            dsize: 0,
            ack_addr: 0,
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
}
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
            .wrapping_add(self.ack_addr)
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

impl std::fmt::Debug for CmdMsg {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "daddr {:#x}({:?}) dsize {:?} ack_addr {:#x} cmd {:?} msg_hash {:?} cmd_hash {:?}",
            self.daddr, self.daddr, self.dsize, self.ack_addr, self.cmd, self.msg_hash, self.cmd_hash,
        )
    }
}

struct InnerCQ {
    send_buffer: Arc<Vec<Mutex<CommSlice<CmdMsg>>>>,
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
            (*cmd).ack_addr = 0;
            (*cmd).cmd = Cmd::Clear;
            (*cmd).msg_hash = 0;
            (*cmd).calc_hash();
            send_buffers.push(Mutex::new(send_buffer.sub_slice(i..=i)));
        }
        trace!("send_buffer init {:?}", send_buffer);

        let recv_buffer: CommSlice<CmdMsg> = recv_buffer_alloc.as_comm_slice();
        let mut recv_buffers = vec![];
        for (i, cmd) in recv_buffer.clone().iter_mut().enumerate() {
            (*cmd).daddr = 0;
            (*cmd).dsize = 0;
            (*cmd).ack_addr = 0;
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
            (*cmd).ack_addr = 0;
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
            (*cmd).ack_addr = 0;
            (*cmd).cmd = Cmd::Clear;
            (*cmd).msg_hash = 0;
            (*cmd).calc_hash();
        }
        trace!("panic_buffer init {:?}", panic_buffer);

        let mut release_cmd = unsafe { Box::from_raw(release_cmd_alloc.as_mut_ptr::<CmdMsg>()) };
        release_cmd.daddr = 1;
        release_cmd.dsize = 1;
        release_cmd.ack_addr = 0;
        release_cmd.cmd = Cmd::Release;
        release_cmd.msg_hash = 1;
        release_cmd.calc_hash();
        trace!("release_cmd init {:?}", release_cmd);

        let mut clear_cmd = unsafe { Box::from_raw(clear_cmd_alloc.as_mut_ptr::<CmdMsg>()) };
        clear_cmd.daddr = 0;
        clear_cmd.dsize = 0;
        clear_cmd.ack_addr = 0;
        clear_cmd.cmd = Cmd::Clear;
        clear_cmd.msg_hash = 0;
        clear_cmd.calc_hash();
        trace!("clear_cmd init {:?}", clear_cmd);

        let mut free_cmd = unsafe { Box::from_raw(free_cmd_alloc.as_mut_ptr::<CmdMsg>()) };
        free_cmd.daddr = 0;
        free_cmd.dsize = 0;
        free_cmd.ack_addr = 0;
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
            let buf = self.send_buffer[dst].lock().await;
            if buf[0].cmd == Cmd::Tx || buf[0].cmd == Cmd::Free {
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
                    if cmd.daddr == 0 {
                        return None;
                    }
                    trace!("received cmd:{:?} from pe {} -- [{:?}]", cmd.cmd, src, cmd);
                    let res = Some(cmd.clone());
                    recv_buffer[0] = **self.clear_cmd;
                    res
                }
                Cmd::Free | Cmd::Release => panic!("should not see free/release in recv buffer"),
                Cmd::Alloc => panic!("should not encounter alloc here"),
                Cmd::Panic => panic!("should not encounter panic here"),
            }
        } else {
            None
        }
    }

    fn available_to_send(&self, pe: usize) -> bool {
        // if let Some(send_buf) = self.send_buffer[pe].lcok() {
        let send_buf = self.send_buffer[pe].lock_blocking();
        if send_buf[0].hash() == self.clear_cmd.hash() {
            return true;
        }
        // trace!("send slot busy for dst({pe}), cannot send... {:?}", send_buf[0]);
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
                    debug!(
                        "pe {} panic_buf not clear {:?}",
                        pe, &panic_buf[pe] as *const CmdMsg
                    );
                    debug!("panic_buf {:?}", panic_buf[pe]);
                }
                if panic_buf[pe].check_hash() && panic_buf[pe].cmd == Cmd::Panic {
                    debug!("panic_buf passed hash check {:?}", panic_buf[pe]);
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

    // Check all send_buffer slots for Free acks and clean up. Called from recv_data loop
    // so cleanup happens even when no new sends are in flight.
    //#[tracing::instrument(skip_all, level = "debug")]
    fn cleanup_transfers(&self) {
        for dst in 0..self.num_pes {
            if dst == self.my_pe {
                continue;
            }
            if let Some(mut send_buf) = self.send_buffer[dst].try_lock() {
                if send_buf[0].cmd == Cmd::Free {
                    debug!("cleanup_transfers: freeing data for dst({dst})");
                    if send_buf[0].dsize > 0 {
                        let _alloc = self
                            .comm
                            .local_rt_alloc_from_local_addr(send_buf[0].daddr)
                            .expect("failed to find local alloc from addr");
                    }
                    send_buf[0] = **self.clear_cmd;
                }
            }
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    async fn send(&self, data: CommSlice<u8>, dst: usize, hash: usize) {
        stats!(PE_SENDS[0][dst].fetch_add(1, Ordering::SeqCst));
        debug!("want to send data {:?} {:?} {:x}", data, dst, hash);
        let mut printed = false;
        loop {
            if self.active.load(Ordering::SeqCst) == CmdQStatus::Panic as u8 {
                break;
            }
            {
                let mut send_buf = self.send_buffer[dst].lock_blocking();

                // Progress any pending Free → Clear transition inline
                if send_buf[0].cmd == Cmd::Free {
                    debug!("send: clearing free'd slot for dst({dst})");
                    if send_buf[0].dsize > 0 {
                        let _alloc = self
                            .comm
                            .local_rt_alloc_from_local_addr(send_buf[0].daddr)
                            .expect("failed to find local alloc from addr");
                    }
                    send_buf[0] = **self.clear_cmd;
                }

                if send_buf[0].hash() == self.clear_cmd.hash() {
                    let ack_addr = send_buf.usize_addr();
                    send_buf[0].daddr = data.usize_addr();
                    send_buf[0].dsize = data.len();
                    send_buf[0].ack_addr = ack_addr;
                    send_buf[0].msg_hash = hash;
                    send_buf[0].cmd = Cmd::Tx;
                    send_buf[0].calc_hash();

                    let recv_buffer = self.recv_buffer[self.my_pe].read();
                    stats!(PE_SENDS[1][dst].fetch_add(1, Ordering::SeqCst));
                    debug!("sending cmd to dst({dst}) {:?}", send_buf[0]);
                    let send_cmd = send_buf[0].clone();
                    let _ = recv_buffer.put_unmanaged::<CmdMsg>(send_cmd, dst, 0);
                    self.put_amt
                        .fetch_add(send_buf[0].as_bytes().len(), Ordering::Relaxed);
                    self.sent_cnt.fetch_add(1, Ordering::SeqCst);
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
            async_std::task::yield_now().await;
        }
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
                my_alloc_buf[0].ack_addr = 0;
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
            my_alloc_buf[0].ack_addr = 0;
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
            cmd.ack_addr = 0;
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

    // Receiver writes Cmd::Free to sender's send_buffer[dst][0].cmd via ack_addr.
    // This mirrors how the old design wrote Free to cmd_buffer[0].cmd.
    //#[tracing::instrument(skip_all, level = "debug")]
    fn send_free(&self, dst: usize, cmd: CmdMsg) {
        trace!(
            "sending free to dst[{dst}]: ack_addr {:#x} cmd: {:?}",
            cmd.ack_addr,
            cmd,
        );
        let (local_ack_alloc, offset) = self
            .comm
            .local_alloc_and_offset_from_remote_pe_and_addr(dst, cmd.ack_addr);
        let ack_slice = local_ack_alloc
            .comm_slice_at_byte_offset::<Cmd>(offset + offset_of!(CmdMsg, cmd), 1);
        let _ = ack_slice.put_unmanaged::<Cmd>(self.free_cmd.cmd, dst, 0);
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    async fn get_serialized_data(
        &self,
        src: usize,
        cmd: CmdMsg,
        ser_data: &mut SerializedData,
        msg_id: usize,
        lamellae: &Arc<Lamellae>,
    ) {
        let len = ser_data.len();
        let mut buffer = unsafe {
            LamellarBuffer::<u8, CommSlice<u8>>::from_comm_slice(
                ser_data.header_and_data_as_bytes_mut(),
                lamellae.clone(),
            )
        };
        trace!("get_serialized_data {:?} {:?} {:x}", src, cmd, cmd.daddr);
        let (local_daddr_alloc, offset) = self
            .comm
            .local_alloc_and_offset_from_remote_pe_and_addr(src, cmd.daddr);
        let task = local_daddr_alloc
            .get_into_buffer(&self.scheduler, None, src, offset, buffer.split_off(0))
            .spawn();
        let data_slice = ser_data.header_and_data_as_bytes_mut();

        let mut timer = std::time::Instant::now();
        trace!(
            "msg_id: {msg_id} tmp_data_addr: {:?} calced hash: {:x} cmd msg hash {:x} {:?} {:?}",
            data_slice.as_ptr(),
            calc_hash(data_slice.as_ptr() as usize, len),
            cmd.msg_hash,
            &data_slice.as_slice()[0..std::cmp::min(32, len)],
            &data_slice.as_slice()[len.saturating_sub(32)..len],
        );

        while calc_hash(data_slice.as_ptr() as usize, len) != cmd.msg_hash
            && self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8
        {
            if timer.elapsed().as_secs_f64() > 10.0 {
                println!(
                    "msg_id: {msg_id} data hash mismatch from {:?}!!! cmd: {:?} l: {:?} l(): {:?} -- calced hash {:x} expected {:x} [{:?}..{:?}] cur_thread: {:?}",
                    src,
                    cmd,
                    len,
                    data_slice.len(),
                    calc_hash(data_slice.as_ptr() as usize, len),
                    cmd.msg_hash,
                    &data_slice.as_slice()[0..32],
                    &data_slice.as_slice()[len.saturating_sub(32)..len],
                    std::thread::current().id(),
                );
                timer = std::time::Instant::now();
            }
            self.comm.thread_flush();
            async_std::task::yield_now().await;
        }
        task.await;
        trace!(
            "after msg_id: {msg_id} calced hash: {:x} cmd msg hash {:x}",
            calc_hash(data_slice.as_ptr() as usize, len),
            cmd.msg_hash,
        );
        stats!(PE_RECVS[0][src].fetch_add(1, Ordering::SeqCst));
        debug!(
            "got serialized data from {src} {:x} -- {} bytes",
            cmd.daddr,
            ser_data.len()
        );
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    async fn get_cmd(&self, src: usize, cmd: CmdMsg, msg_id: usize,lamellae: &Arc<Lamellae>,) -> SerializedData {
        trace!("getting cmd from {} of size {}", src, cmd.dsize);
        let mut ser_data = self.comm.new_serialized_data(cmd.dsize as usize);
        let mut print = true;

        while ser_data.is_err() && self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8 {
            if print {
                debug!("msg_id: {msg_id} get cmd buf stuck waiting for alloc");
                print = false;
            }
            async_std::task::yield_now().await;
            self.send_alloc(cmd.dsize);
            ser_data = self.comm.new_serialized_data(cmd.dsize as usize);
        }
        let mut ser_data = ser_data.unwrap();
        self.get_serialized_data(src, cmd, &mut ser_data, msg_id, lamellae)
            .await;
        self.recv_cnt.fetch_add(1, Ordering::SeqCst);
        ser_data
    }
}

impl Drop for InnerCQ {
    //#[tracing::instrument(skip_all, level = "debug")]
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

struct CQGet {
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

impl CQGet {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn new(
        comm: Arc<Comm>,
        scheduler: Arc<Scheduler>,
        my_pe: usize,
        num_pes: usize,
        active: Arc<AtomicU8>,
    ) -> CQGet {
        let send_buffer = comm
            .rt_alloc(
                num_pes * std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap();
        debug!("send_buffer {:?}", send_buffer);
        let recv_buffer = comm
            .rt_alloc(
                num_pes * std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap();
        debug!("recv_buffer {:?}", recv_buffer);
        let alloc_buffer = comm
            .rt_alloc(
                num_pes * std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap();
        debug!("alloc_buffer {:?}", alloc_buffer);
        let panic_buffer = comm
            .rt_alloc(
                num_pes * std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap();
        debug!("panic_buffer {:?}", panic_buffer);
        let release_cmd = comm
            .rt_alloc(
                std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap();
        debug!(
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
        debug!("clear_cmd {:?}", clear_cmd);
        let free_cmd = comm
            .rt_alloc(
                std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap();
        debug!("free_cmd {:?}", free_cmd);

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
        CQGet {
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

    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) async fn send_data(&self, data: SerializedData, dst: usize) {
        let hash = calc_hash(data.ser_data_bytes.usize_addr(), data.len());
        let data_slice = data.ser_data_bytes.clone();
        data.leak_alloc()
            .leak()
            .expect("failed to leak alloc in send_data");
        self.cq.send(data_slice, dst, hash).await;
    }

    async fn send_vec(&self, vec_data: Vec<u8>, dst: usize) {
        trace!("sending vec_data of len {:?} to dst {:?}", vec_data.len(), dst);
        let mut data = self.cq.comm.rt_alloc(vec_data.len(), std::mem::align_of::<u8>());
        while let Err(_) = data {
            async_std::task::yield_now().await;
            data = self.cq.comm.rt_alloc(vec_data.len(), std::mem::align_of::<u8>());
        }
        let data = data.unwrap();
        unsafe {
            std::ptr::copy_nonoverlapping(
                vec_data.as_ptr(),
                data.as_mut_ptr(),
                vec_data.len(),
            );
        }
        let data_slice = data.as_comm_slice().clone();
        let hash = calc_hash(data_slice.usize_addr(), data_slice.len());
        data.leak().expect("failed to leak alloc in send_vec");
        self.cq.send(data_slice, dst, hash).await;
    }

    fn wait_all_print(&self) {
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
            let send_buffer = self.cq.send_buffer[pe].lock_blocking();
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
        println!("finished command queue wait_all_print");
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    async fn alloc_task(&self) {
        let mut timer = std::time::Instant::now();
        let mut print = false;
        while self.scheduler.active(0)
            && self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8
        {
            if timer.elapsed().as_secs_f64() > 10.0 {
                trace!("alloc_task still running");
                timer = std::time::Instant::now();
                print = true;
            }
            self.cq.check_alloc(print);
            print = false;
            async_std::task::sleep(std::time::Duration::from_millis(10)).await;
        }
    } 

    #[tracing::instrument(skip_all, level = "debug")]
    async fn panic_task(&self) {
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
    async fn recv_data(&self, lamellae: Arc<Lamellae>) {
        let comm = lamellae.comm();
        let num_pes = comm.num_pes();
        let my_pe = comm.my_pe();
        let mut timer = std::time::Instant::now();
        while self.active.load(Ordering::SeqCst) == CmdQStatus::Active as u8
            || !self.cq.empty().await
            || self.scheduler.active(0)
        {
            if timer.elapsed().as_secs_f64() > 5.0 {
                trace!(
                    "recv_data still running -- cq empty? {:?}  scheduler active? {:?}",
                    self.cq.empty().await,
                    self.scheduler.active(0)
                );
                timer = std::time::Instant::now();
            }
            for src in 0..num_pes {
                if src != my_pe {
                    if let Some(cmd) = self.cq.ready(src) {
                        trace!("recv_data {:?}", cmd);
                        match cmd.cmd {
                            Cmd::Alloc => panic!("should not encounter alloc here"),
                            Cmd::Panic => panic!("should not encounter panic here"),
                            Cmd::Clear | Cmd::Release | Cmd::Free => {
                                panic!("should not be possible to see cmd clear, release, or free here")
                            }
                            Cmd::Tx => {
                                trace!("got tx from {src}");
                                let cq = self.cq.clone();
                                let lamellae = lamellae.clone();
                                let scheduler1 = self.scheduler.clone();
                                let task = async move {
                                    let msg_id = MSG_ID.fetch_add(1, Ordering::SeqCst);
                                    debug!("getting cmd from {src} {:?} msg_id: {msg_id}", cmd);
                                    let work_data = cq.get_cmd(src, cmd, msg_id,&lamellae).await;
                                    debug!("msg_id: {msg_id} submitting remote am from {src}");
                                    scheduler1.submit_remote_am(work_data, &lamellae);
                                    cq.send_free(src, cmd);
                                };
                                self.scheduler.submit_io_task(task);
                            }
                        }
                    }
                }
            }

            comm.thread_flush();
            self.cq.cleanup_transfers();
            async_std::task::yield_now().await;
        }
        self.active
            .store(CmdQStatus::Finished as u8, Ordering::SeqCst);
    }

    fn mem_per_pe() -> usize {
        match config().cmd_queue {
            CmdQueue::Batched => super::command_queues_batched::CQBatched::mem_per_pe(),
            CmdQueue::Get => 4 * std::mem::size_of::<CmdMsg>(),
            CmdQueue::GetEager => super::command_queues_get_eager::CQGetEager::mem_per_pe(),
            CmdQueue::GetSlots => super::command_queues_get_slots::CQGetSlots::mem_per_pe(),
            CmdQueue::Put => super::command_queues_put::CQPut::mem_per_pe(),
            CmdQueue::PutSlots => super::command_queues_put_slots::CQPutSlots::mem_per_pe(),
            CmdQueue::PutEager => super::command_queues_put_eager::CQPutEager::mem_per_pe(),
        } 
    }

    fn available_to_send(&self, pe: usize) -> bool {
        self.cq.available_to_send(pe)
    }
}

impl Drop for CQGet {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop CQGet");
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
        trace!(target: "drop", "end drop CQGet");
    }
}

enum CQVariant {
    Batched(super::command_queues_batched::CQBatched),
    Get(CQGet),
    GetEager(super::command_queues_get_eager::CQGetEager),
    GetSlots(super::command_queues_get_slots::CQGetSlots),
    Put(super::command_queues_put::CQPut),
    PutSlots(super::command_queues_put_slots::CQPutSlots),
    PutEager(super::command_queues_put_eager::CQPutEager),
}

pub(crate) struct CommandQueue {
    pub(crate) scheduler: Arc<Scheduler>,
    inner: CQVariant,
    background_done: Arc<AtomicUsize>, // counts down from 2 as alloc_task and panic_task exit
}

impl CommandQueue {
    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new(
        comm: Arc<Comm>,
        scheduler: Arc<Scheduler>,
        my_pe: usize,
        num_pes: usize,
        active: Arc<AtomicU8>,
    ) -> CommandQueue {
        let inner = match crate::config().cmd_queue {
            CmdQueue::Batched => CQVariant::Batched(super::command_queues_batched::CQBatched::new(
                comm, scheduler.clone(), my_pe, num_pes, active,
            )),
            CmdQueue::Get => CQVariant::Get(CQGet::new(
                comm, scheduler.clone(), my_pe, num_pes, active,
            )),
            CmdQueue::GetEager => CQVariant::GetEager(super::command_queues_get_eager::CQGetEager::new(
                comm, scheduler.clone(), my_pe, num_pes, active,
            )),
            CmdQueue::GetSlots => CQVariant::GetSlots(super::command_queues_get_slots::CQGetSlots::new(
                comm, scheduler.clone(), my_pe, num_pes, active,
            )),
            CmdQueue::Put => CQVariant::Put(super::command_queues_put::CQPut::new(
                comm, scheduler.clone(), my_pe, num_pes, active,
            )),
            CmdQueue::PutSlots => CQVariant::PutSlots(super::command_queues_put_slots::CQPutSlots::new(
                comm, scheduler.clone(), my_pe, num_pes, active,
            )),
            CmdQueue::PutEager => CQVariant::PutEager(super::command_queues_put_eager::CQPutEager::new(
                comm, scheduler.clone(), my_pe, num_pes, active,
            )),
        };
        CommandQueue { scheduler, inner, background_done: Arc::new(AtomicUsize::new(2)) }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) async fn send_alloc(&self, min_size: usize) {
        match &self.inner {
            CQVariant::Batched(cq) => cq.send_alloc(min_size).await,
            CQVariant::Get(cq) => cq.send_alloc(min_size).await,
            CQVariant::GetEager(cq) => cq.send_alloc(min_size).await,
            CQVariant::GetSlots(cq) => cq.send_alloc(min_size).await,
            CQVariant::Put(cq) => cq.send_alloc(min_size).await,
            CQVariant::PutSlots(cq) => cq.send_alloc(min_size).await,
            CQVariant::PutEager(cq) => cq.send_alloc(min_size).await,
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn send_panic(&self) {
        match &self.inner {
            CQVariant::Batched(cq) => cq.send_panic(),
            CQVariant::Get(cq) => cq.send_panic(),
            CQVariant::GetEager(cq) => cq.send_panic(),
            CQVariant::GetSlots(cq) => cq.send_panic(),
            CQVariant::Put(cq) => cq.send_panic(),
            CQVariant::PutSlots(cq) => cq.send_panic(),
            CQVariant::PutEager(cq) => cq.send_panic(),
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) async fn send_data(&self, data: SerializedData, dst: usize) {
        match &self.inner {
            CQVariant::Batched(cq) => cq.send_data(data, dst).await,
            CQVariant::Get(cq) => cq.send_data(data, dst).await,
            CQVariant::GetEager(cq) => cq.send_data(data, dst).await,
            CQVariant::GetSlots(cq) => cq.send_data(data, dst).await,
            CQVariant::Put(cq) => cq.send_data(data, dst).await,
            CQVariant::PutSlots(cq) => cq.send_data(data, dst).await,
            CQVariant::PutEager(cq) => cq.send_data(data, dst).await,
        }
    }

    pub(crate) async fn send_vec(&self, vec_data: Vec<u8>, dst: usize) {
        match &self.inner {
            CQVariant::Batched(cq) => cq.send_vec(vec_data, dst).await,
            CQVariant::Get(cq) => cq.send_vec(vec_data, dst).await,
            CQVariant::GetEager(cq) => cq.send_vec(vec_data, dst).await,
            CQVariant::GetSlots(cq) => cq.send_vec(vec_data, dst).await,
            CQVariant::Put(cq) => cq.send_vec(vec_data, dst).await,
            CQVariant::PutSlots(cq) => cq.send_vec(vec_data, dst).await,
            CQVariant::PutEager(cq) => cq.send_vec(vec_data, dst).await,
        }
    }

    pub(crate) fn wait_all_print(&self) {
        match &self.inner {
            CQVariant::Batched(cq) => cq.wait_all_print(),
            CQVariant::Get(cq) => cq.wait_all_print(),
            CQVariant::GetEager(cq) => cq.wait_all_print(),
            CQVariant::GetSlots(cq) => cq.wait_all_print(),
            CQVariant::Put(cq) => cq.wait_all_print(),
            CQVariant::PutSlots(cq) => cq.wait_all_print(),
            CQVariant::PutEager(cq) => cq.wait_all_print(),
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) async fn alloc_task(&self) {
        match &self.inner {
            CQVariant::Batched(cq) => cq.alloc_task().await,
            CQVariant::Get(cq) => cq.alloc_task().await,
            CQVariant::GetEager(cq) => cq.alloc_task().await,
            CQVariant::GetSlots(cq) => cq.alloc_task().await,
            CQVariant::Put(cq) => cq.alloc_task().await,
            CQVariant::PutSlots(cq) => cq.alloc_task().await,
            CQVariant::PutEager(cq) => cq.alloc_task().await,
        }
        self.background_done.fetch_sub(1, Ordering::Release);
        debug!(target: "drop","alloc_task exiting");
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) async fn panic_task(&self) {
        match &self.inner {
            CQVariant::Batched(cq) => cq.panic_task().await,
            CQVariant::Get(cq) => cq.panic_task().await,
            CQVariant::GetEager(cq) => cq.panic_task().await,
            CQVariant::GetSlots(cq) => cq.panic_task().await,
            CQVariant::Put(cq) => cq.panic_task().await,
            CQVariant::PutSlots(cq) => cq.panic_task().await,
            CQVariant::PutEager(cq) => cq.panic_task().await,
        }
        self.background_done.fetch_sub(1, Ordering::Release);
        debug!(target: "drop","panic_task exiting");
    }

    pub(crate) fn background_tasks_done(&self) -> bool {
        self.background_done.load(Ordering::Acquire) == 0
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) async fn recv_data(&self, lamellae: Arc<Lamellae>) {
        match &self.inner {
            CQVariant::Batched(cq) => cq.recv_data(lamellae).await,
            CQVariant::Get(cq) => cq.recv_data(lamellae).await,
            CQVariant::GetEager(cq) => cq.recv_data(lamellae).await,
            CQVariant::GetSlots(cq) => cq.recv_data(lamellae).await,
            CQVariant::Put(cq) => cq.recv_data(lamellae).await,
            CQVariant::PutSlots(cq) => cq.recv_data(lamellae).await,
            CQVariant::PutEager(cq) => cq.recv_data(lamellae).await,
        }
        debug!(target: "drop","recv_data exiting");
    }

    pub(crate) fn mem_per_pe() -> usize {
        match crate::config().cmd_queue {
            CmdQueue::GetEager => super::command_queues_get_eager::CQGetEager::mem_per_pe(),
            CmdQueue::GetSlots => super::command_queues_get_slots::CQGetSlots::mem_per_pe(),
            CmdQueue::PutSlots => super::command_queues_put_slots::CQPutSlots::mem_per_pe(),
            _ => CQGet::mem_per_pe(),
        }
    }

    pub(crate) fn available_to_send(&self, pe: usize) -> bool {
        match &self.inner {
            CQVariant::Batched(cq) => cq.available_to_send(pe),
            CQVariant::Get(cq) => cq.available_to_send(pe),
            CQVariant::GetEager(cq) => cq.available_to_send(pe),
            CQVariant::GetSlots(cq) => cq.available_to_send(pe),
            CQVariant::Put(cq) => cq.available_to_send(pe),
            CQVariant::PutSlots(cq) => cq.available_to_send(pe),
            CQVariant::PutEager(cq) => cq.available_to_send(pe),
        }
    }
}

impl Drop for CommandQueue {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop CommandQueue");
        debug!("dropping CommandQueue");
        while !self.background_tasks_done() {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        debug!("dropped CommandQueue");
        trace!(target: "drop", "end drop CommandQueue");
    }
}

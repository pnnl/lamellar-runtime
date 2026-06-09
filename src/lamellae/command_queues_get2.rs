use super::{
    comm::{CmdQStatus, CommAlloc, CommInfo, CommMem, CommProgress, CommSlice},
    Comm, Lamellae, SerializedData,
};
use crate::{
    env_var::config, lamellae::CommAllocRdma, print_stats, scheduler::Scheduler, stats,
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

// Eager path constants: messages ≤ EAGER_DATA_SIZE bytes bypass the GET rendezvous entirely.
// The sender claims a ring slot via CAS, PUTs the data, then PUTs the size as a magic terminator.
// The receiver polls the magic field locally and copies out the data without any RDMA GET.
const EAGER_DATA_SIZE: usize = 4096;
const EAGER_RING_SIZE: usize = 8;
const EAGER_SLOT_SIZE: usize = EAGER_DATA_SIZE + std::mem::size_of::<u64>();

#[repr(C)]
#[derive(Clone, Copy)]
struct CmdMsg {
    daddr: usize,
    dsize: usize,
    ack_addr: usize,
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
        unsafe { std::slice::from_raw_parts(pointer, size) }
    }

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
    // Eager path state
    eager_recv_alloc_addr: usize,          // local VA of our eager recv ring (for polling)
    eager_send_acks_alloc_addr: usize,     // local VA of our send-ack counters (for reading)
    eager_recv_comm_alloc: CommAlloc,      // CommAlloc for remote PUTs into any PE's recv ring
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
            let buf = self.send_buffer[dst].lock().await;
            if buf[0].cmd == Cmd::Tx || buf[0].cmd == Cmd::Free {
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
        let send_buf = self.send_buffer[pe].lock_blocking();
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

    // Rendezvous send: sender keeps data in place, sends pointer via CmdMsg;
    // receiver issues a GET, verifies the hash, then sends a Free ack.
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

    fn send_free(&self, dst: usize, cmd: CmdMsg) {
        trace!(
            "sending free to dst[{dst}]: ack_addr {:#x} cmd: {:?}",
            cmd.ack_addr,
            cmd,
        );
        let (local_ack_alloc, offset) = self
            .comm
            .local_alloc_and_offset_from_remote_pe_and_addr(dst, cmd.ack_addr);
        use memoffset::offset_of;
        let ack_slice = local_ack_alloc
            .comm_slice_at_byte_offset::<Cmd>(offset + offset_of!(CmdMsg, cmd), 1);
        let _ = ack_slice.put_unmanaged::<Cmd>(self.free_cmd.cmd, dst, 0);
    }

    async fn get_serialized_data(
        &self,
        src: usize,
        cmd: CmdMsg,
        ser_data: &mut SerializedData,
        msg_id: usize,
    ) {
        let len = ser_data.len();
        let mut buffer = unsafe {
            LamellarBuffer::<u8, CommSlice<u8>>::from_comm_slice(
                ser_data.header_and_data_as_bytes_mut(),
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

    async fn get_cmd(&self, src: usize, cmd: CmdMsg, msg_id: usize) -> SerializedData {
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
        self.get_serialized_data(src, cmd, &mut ser_data, msg_id)
            .await;
        self.recv_cnt.fetch_add(1, Ordering::SeqCst);
        ser_data
    }

    // Try to claim an eager ring slot for dst. Returns slot index on success, None if ring is full.
    fn try_claim_eager_slot(&self, dst: usize) -> Option<usize> {
        loop {
            let head = self.eager_send_head[dst].load(Ordering::Acquire);
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
        }
    }

    // Eager send: PUT data into dst's ring slot, then PUT size as magic terminator.
    // Uses eager_recv_comm_alloc whose remote_keys[dst].addr is dst's actual recv ring VA.
    async fn send_eager(&self, data: CommSlice<u8>, dst: usize, slot_idx: usize) {
        let size = data.len();
        let byte_offset = (self.my_pe * EAGER_RING_SIZE + slot_idx) * EAGER_SLOT_SIZE;

        let dst_data = self.eager_recv_comm_alloc.comm_slice_at_byte_offset::<u8>(byte_offset, size);
        let dst_magic = self.eager_recv_comm_alloc.comm_slice_at_byte_offset::<u64>(byte_offset + EAGER_DATA_SIZE, 1);

        dst_data
            .put_buffer::<u8>(&self.scheduler, None, data, dst, 0)
            .await;
        // PUT magic after data; RDMA ordering ensures data arrives before magic.
        dst_magic.put_unmanaged::<u64>(size as u64, dst, 0);

        stats!(PE_SENDS[1][dst].fetch_add(1, Ordering::SeqCst));
        self.sent_cnt.fetch_add(1, Ordering::SeqCst);
    }

    // Eager send for Vec<u8>: stage into registered memory then PUT, or PUT directly.
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

    // Send ack back to src after processing one of its eager slots.
    // Uses eager_send_acks_comm_alloc whose remote_keys[src].addr is src's actual ack buffer VA.
    fn send_eager_ack(&self, src: usize, count: usize) {
        let ack_slot = self.eager_send_acks_comm_alloc
            .comm_slice_at_byte_offset::<u64>(self.my_pe * std::mem::size_of::<u64>(), 1);
        ack_slot.put_unmanaged::<u64>(count as u64, src, 0);
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

pub(crate) struct CQGet2 {
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
impl CQGet2 {
    pub(crate) fn new(
        comm: Arc<Comm>,
        scheduler: Arc<Scheduler>,
        my_pe: usize,
        num_pes: usize,
        active: Arc<AtomicU8>,
    ) -> CQGet2 {
        let send_buffer = comm
            .rt_alloc(num_pes * std::mem::size_of::<CmdMsg>(), std::mem::align_of::<CmdMsg>())
            .unwrap();
        trace!("send_buffer {:?}", send_buffer);
        let recv_buffer = comm
            .rt_alloc(num_pes * std::mem::size_of::<CmdMsg>(), std::mem::align_of::<CmdMsg>())
            .unwrap();
        trace!("recv_buffer {:?}", recv_buffer);
        let alloc_buffer = comm
            .rt_alloc(num_pes * std::mem::size_of::<CmdMsg>(), std::mem::align_of::<CmdMsg>())
            .unwrap();
        trace!("alloc_buffer {:?}", alloc_buffer);
        let panic_buffer = comm
            .rt_alloc(num_pes * std::mem::size_of::<CmdMsg>(), std::mem::align_of::<CmdMsg>())
            .unwrap();
        trace!("panic_buffer {:?}", panic_buffer);
        let release_cmd = comm
            .rt_alloc(std::mem::size_of::<CmdMsg>(), std::mem::align_of::<CmdMsg>())
            .unwrap();
        trace!("release_cmd {:?}", release_cmd);
        let clear_cmd = comm
            .rt_alloc(std::mem::size_of::<CmdMsg>(), std::mem::align_of::<CmdMsg>())
            .unwrap();
        trace!("clear_cmd {:?}", clear_cmd);
        let free_cmd = comm
            .rt_alloc(std::mem::size_of::<CmdMsg>(), std::mem::align_of::<CmdMsg>())
            .unwrap();
        trace!("free_cmd {:?}", free_cmd);

        let eager_recv_alloc = comm
            .rt_alloc(num_pes * EAGER_RING_SIZE * EAGER_SLOT_SIZE, std::mem::align_of::<u64>())
            .unwrap();
        trace!("eager_recv_alloc {:?}", eager_recv_alloc);

        let eager_send_acks_alloc = comm
            .rt_alloc(num_pes * std::mem::size_of::<u64>(), std::mem::align_of::<u64>())
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
        CQGet2 {
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
        if data.len() > 0 && data.len() <= EAGER_DATA_SIZE {
            if let Some(slot_idx) = self.cq.try_claim_eager_slot(dst) {
                self.cq.send_eager(data.ser_data_bytes.clone(), dst, slot_idx).await;
                // send_eager awaits the PUT — data is safe to drop here, no Free ack needed
                return;
            }
        }
        let hash = calc_hash(data.ser_data_bytes.usize_addr(), data.len());
        let data_slice = data.ser_data_bytes.clone();
        data.leak_alloc().leak().expect("failed to leak alloc in send_data");
        self.cq.send(data_slice, dst, hash).await;
    }

    pub(crate) async fn send_vec(&self, vec_data: Vec<u8>, dst: usize) {
        trace!("sending vec_data of len {:?} to dst {:?}", vec_data.len(), dst);
        if vec_data.len() > 0 && vec_data.len() <= EAGER_DATA_SIZE {
            if let Some(slot_idx) = self.cq.try_claim_eager_slot(dst) {
                self.cq.send_eager_vec(vec_data, dst, slot_idx).await;
                return;
            }
        }
        // Rendezvous fallback: copy into registered memory and send via GET.
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
        let data_slice = data.as_comm_slice::<u8>().clone();
        let hash = calc_hash(data_slice.usize_addr(), data_slice.len());
        data.leak().expect("failed to leak alloc in send_vec");
        self.cq.send(data_slice, dst, hash).await;
    }

    pub(crate) fn wait_all_print(&self) {
        println!("command queue");
        println!(
            "sends {:?}",
            print_stats!(PE_SENDS
                .iter()
                .map(|x| x.iter().map(|y| y.load(Ordering::SeqCst)).collect::<Vec<_>>())
                .collect::<Vec<_>>())
        );
        println!(
            "recvs {:?}",
            print_stats!(PE_RECVS
                .iter()
                .map(|x| x.iter().map(|y| y.load(Ordering::SeqCst)).collect::<Vec<_>>())
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
            println!("recv_buffer ptr for pe {pe}: {:x}", recv_buffer.usize_addr());
            println!("send_buffer ptr for pe {pe}: {:x}", send_buffer.usize_addr());
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
                trace!(
                    "recv_data still running -- cq empty? {:?}  scheduler active? {:?}",
                    self.cq.empty().await,
                    self.scheduler.active(0)
                );
                timer = std::time::Instant::now();
            }
            for src in 0..num_pes {
                if src != my_pe {
                    // --- Eager path: poll ring slot before checking rendezvous buffer ---
                    {
                        let head = self.cq.eager_recv_head[src].load(Ordering::Relaxed);
                        let slot_idx = head % EAGER_RING_SIZE;
                        let byte_offset = (src * EAGER_RING_SIZE + slot_idx) * EAGER_SLOT_SIZE;
                        let magic_addr =
                            self.cq.eager_recv_alloc_addr + byte_offset + EAGER_DATA_SIZE;
                        let magic = unsafe { (magic_addr as *const u64).read_volatile() };
                        if magic != 0 {
                            let size = magic as usize;
                            let data_addr = self.cq.eager_recv_alloc_addr + byte_offset;

                            // Allocate output SerializedData and copy from ring slot.
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
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    data_addr as *const u8,
                                    ser_data.ser_data_bytes.usize_addr() as *mut u8,
                                    size,
                                );
                            }
                            // Zero the magic field so the slot can be reused.
                            unsafe { (magic_addr as *mut u64).write_volatile(0u64) };
                            self.cq.eager_recv_head[src].fetch_add(1, Ordering::SeqCst);
                            let processed =
                                self.cq.eager_recv_processed[src].fetch_add(1, Ordering::SeqCst) + 1;
                            self.cq.send_eager_ack(src, processed);
                            self.cq.recv_cnt.fetch_add(1, Ordering::SeqCst);
                            stats!(PE_RECVS[1][src].fetch_add(1, Ordering::SeqCst));
                            self.scheduler.submit_remote_am(ser_data, lamellae.clone());
                        }
                    }

                    // --- Rendezvous path: GET-based protocol for large messages ---
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
                                    let work_data = cq.get_cmd(src, cmd, msg_id).await;
                                    debug!("msg_id: {msg_id} submitting remote am from {src}");
                                    scheduler1.submit_remote_am(work_data, lamellae.clone());
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
impl Drop for CQGet2 {
    fn drop(&mut self) {
        debug!(
            "sends {:?}",
            print_stats!(PE_SENDS
                .iter()
                .map(|x| x.iter().map(|y| y.load(Ordering::SeqCst)).collect::<Vec<_>>())
                .collect::<Vec<_>>())
        );
        debug!(
            "recvs {:?}",
            print_stats!(PE_RECVS
                .iter()
                .map(|x| x.iter().map(|y| y.load(Ordering::SeqCst)).collect::<Vec<_>>())
                .collect::<Vec<_>>())
        );
    }
}

use super::{
    Comm, Lamellae, SerializedData,
    comm::{CmdQStatus, CommAlloc, CommInfo, CommMem, CommProgress, CommSlice},
};
use crate::{
    LamellarBuffer, env_var::config, lamellae::CommAllocRdma, print_stats, scheduler::Scheduler,
    stats,
};
use async_lock::Mutex; //, RwLock};
use core::panic;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::mem::offset_of;
use std::num::Wrapping;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use tracing::{debug, info, trace, warn};

static MSG_ID: AtomicUsize = AtomicUsize::new(1);

fn new_pe_counters() -> Vec<Vec<AtomicUsize>> {
    let mut v = Vec::with_capacity(2);
    for _ in 0..2 {
        let mut t = Vec::with_capacity(32);
        for _ in 0..32 {
            t.push(AtomicUsize::new(0));
        }
        v.push(t);
    }
    v
}

static PE_SENDS: LazyLock<Vec<Vec<AtomicUsize>>> = LazyLock::new(new_pe_counters);
static PE_RECVS: LazyLock<Vec<Vec<AtomicUsize>>> = LazyLock::new(new_pe_counters);

#[repr(C)]
#[derive(Clone, Copy)]
struct CmdMsg {
    // we send this to remote nodes
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
    // Print,
    Tx,
    Alloc,
    Panic,
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
    //we split into a u64 slice and a u8 slice as u64 seems to compute faster.
    let num_usizes = num_bytes / std::mem::size_of::<usize>();
    //let u64_slice = unsafe { std::slice::from_raw_parts(addr as *const u64, num_u64s) };
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

    // let u8_slice = unsafe {
    //     std::slice::from_raw_parts(
    //         (addr) as *const u8,
    //         num_u8s,
    //     )
    // };
    // u8_slice
    //     .iter()
    //     .map(|x| Wrapping(*x as usize))
    //     .sum::<Wrapping<usize>>()
    //     .0
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
        if self.cmd_hash == self.hash() && self.cmd_hash != 0 {
            true
        } else {
            false
        }
    }
}

#[lamellar_prof::prof]
impl std::fmt::Debug for CmdMsg {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "daddr {:#x}({:?}) dsize {:?} cmd {:?} msg_hash {:?} cmd_hash {:?}",
            self.daddr, self.daddr, self.dsize, self.cmd, self.msg_hash, self.cmd_hash,
        )
    }
}

struct CmdBuf {
    buf: CommSlice<CmdMsg>,
    addr: usize,
    index: usize,
    allocated_cnt: usize,
    max_size: usize,
}

impl CmdBuf {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn push(&mut self, data: CommSlice<u8>, hash: usize) {
        let daddr = data.usize_addr();
        let dsize = data.len();
        if daddr == 0 || dsize == 0 {
            panic!("this shouldnt happen! {:?} {:?}", daddr, dsize);
        }
        let cmd = unsafe { self.buf.get_unchecked_mut(self.index) };
        cmd.daddr = daddr;
        cmd.dsize = dsize;
        cmd.cmd = Cmd::Tx;
        cmd.msg_hash = hash;
        cmd.calc_hash();
        trace!("pushing cmd {:?} to index {}", cmd, self.index);
        self.index += 1;
        if dsize > 0 {
            self.allocated_cnt += 1;
        }
    }
    //#[tracing::instrument(skip_all, level = "debug")]
    fn full(&self) -> bool {
        self.index == self.max_size
    }
    //#[tracing::instrument(skip_all, level = "debug")]
    fn reset(&mut self) {
        self.index = 0;
        self.allocated_cnt = 0;
    }
    //#[tracing::instrument(skip_all, level = "debug")]
    fn addr(&self) -> usize {
        self.addr
    }
    //#[tracing::instrument(skip_all, level = "debug")]
    fn size(&self) -> usize {
        self.index * std::mem::size_of::<CmdMsg>()
    }
    //#[tracing::instrument(skip_all, level = "debug")]
    fn iter(&self) -> std::slice::Iter<'_, CmdMsg> {
        self.buf[0..self.index].iter()
    }
    //#[tracing::instrument(skip_all, level = "debug")]
    fn state(&self) -> Cmd {
        self.buf[0].cmd
    }
}

impl std::fmt::Debug for CmdBuf {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "addr {:#x}({:?}) hash {:?} index {:?} a_cnt {:?}",
            self.addr,
            self.addr,
            calc_hash(self.addr, self.size()),
            self.index,
            self.allocated_cnt
        )
    }
}

struct CmdMsgBuffer {
    empty_bufs: Vec<CmdBuf>,
    full_bufs: Vec<CmdBuf>,
    tx_bufs: HashMap<usize, CmdBuf>,
    waiting_bufs: HashMap<usize, CmdBuf>,
    cur_buf: Option<CmdBuf>,
    num_bufs: usize,
    pe: usize,
}

impl CmdMsgBuffer {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn new(addrs: Arc<Vec<CommAlloc>>, pe: usize) -> CmdMsgBuffer {
        let mut bufs = vec![];
        for alloc in addrs.iter() {
            bufs.push(CmdBuf {
                buf: alloc.as_comm_slice(),
                addr: alloc.comm_addr().into(),
                index: 0,
                allocated_cnt: 0,
                max_size: config().cmd_buf_len,
            });
        }
        CmdMsgBuffer {
            empty_bufs: bufs,
            full_bufs: Vec::new(),
            tx_bufs: HashMap::new(),
            waiting_bufs: HashMap::new(),
            cur_buf: None,
            num_bufs: addrs.len(),
            pe: pe,
        }
    }
    //#[tracing::instrument(skip_all, level = "debug")]
    fn try_push(&mut self, data: CommSlice<u8>, hash: usize) -> bool {
        // debug!("trying to push data {:?} to buffer", data);
        if self.cur_buf.is_none() {
            self.cur_buf = self.empty_bufs.pop();
        }
        if let Some(buf) = &mut self.cur_buf {
            buf.push(data, hash);
            if buf.full() {
                self.full_bufs
                    .push(std::mem::replace(&mut self.cur_buf, None).unwrap());
            }
            true
        } else {
            false
        }
    }
    //#[tracing::instrument(skip_all, level = "debug")]
    fn flush_buffer(&mut self, cmd: &mut CmdMsg) {
        trace!("flush buffer before: {:?}", self);
        if let Some(buf) = self.full_bufs.pop() {
            cmd.daddr = buf.addr();
            cmd.dsize = buf.size();
            cmd.cmd = Cmd::Tx;
            cmd.msg_hash = calc_hash(cmd.daddr, cmd.dsize);

            cmd.calc_hash();
            self.tx_bufs.insert(buf.addr(), buf);
        } else if let Some(buf) = std::mem::replace(&mut self.cur_buf, None) {
            cmd.daddr = buf.addr();
            cmd.dsize = buf.size(); // + std::mem::size_of::<usize>();
            cmd.cmd = Cmd::Tx;
            cmd.msg_hash = calc_hash(cmd.daddr, cmd.dsize);
            cmd.calc_hash();
            self.tx_bufs.insert(buf.addr(), buf);
        }
        // else {} all buffers are currently busy
        trace!("flush buffer after: {:?}", self);
    }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn check_transfer(&mut self, addr: usize, pe: usize) -> bool {
        if let Some(buf) = self.tx_bufs.remove(&addr) {
            match buf.state() {
                Cmd::Tx => {
                    // trace!("transfer in progress {:x} {:?} !!", addr, buf.state());
                    self.tx_bufs.insert(addr, buf);
                    false
                }
                Cmd::Release | Cmd::Free => {
                    debug!("transfer complete {:x} {:?} {pe}!!", addr, buf.state());
                    if buf.allocated_cnt > 0 {
                        self.waiting_bufs.insert(buf.addr(), buf);
                        true
                    } else {
                        panic!("am I ever here?");
                    }
                }
                Cmd::Clear => panic!("should not clear before release"),
                Cmd::Alloc => panic!("should not encounter alloc here"),
                Cmd::Panic => panic!("should not encounter panic here"),
            }
        } else {
            false
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn check_free_data(&mut self, comm: &Arc<Comm>) {
        let mut freed_bufs = vec![];

        for (addr, buf) in self.waiting_bufs.iter() {
            match buf.state() {
                Cmd::Release => {} //do nothing
                Cmd::Free => {
                    debug!("freeing buffer {:x} {:?}", addr, buf.state());
                    freed_bufs.push(*addr)
                }
                Cmd::Tx => panic! {"should not be transerring if in waiting bufs"},
                Cmd::Clear => panic!("should not clear before release"),
                // Cmd::Print => panic!("shoud not encounter Print here"),
                Cmd::Alloc => panic!("should not encounter alloc here"),
                Cmd::Panic => panic!("should not encounter panic here"),
            }
        }

        for buf_addr in freed_bufs {
            if let Some(mut buf) = self.waiting_bufs.remove(&buf_addr) {
                for cmd in buf.iter() {
                    if cmd.dsize > 0 {
                        // let ser_data_addr = cmd.daddr;
                        // debug!(
                        //     "need to decrement  data with ser_data addr: {:x} ",
                        //     ser_data_addr
                        // );

                        // unsafe { SerializedData::decrement_cnt_from_addr(comm, ser_data_addr) };
                        let _alloc = comm
                            .local_rt_alloc_from_local_addr(cmd.daddr)
                            .expect("failed to find local alloc from addr");
                        //creates and then drops to decrement the reference count
                    }
                }
                trace!("freeing buffer back to empty bufs {:x}", buf.addr());
                buf.reset();
                self.empty_bufs.push(buf);
            } else {
                warn!("free_data should this be possible?");
            }
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn empty(&self) -> bool {
        self.empty_bufs.len() == self.num_bufs
    }
}

impl std::fmt::Debug for CmdMsgBuffer {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "empty_bufs {:?}
            full_bufs: {:?}
            tx_bufs: {:?}
            waiting_bufs: {:?}
            cur_buf: {:?}
            num_bufs: {:?}
            pe: {:?}",
            self.empty_bufs,
            self.full_bufs,
            self.tx_bufs,
            self.waiting_bufs,
            self.cur_buf,
            self.num_bufs,
            self.pe,
        )
    }
}

impl Drop for CmdMsgBuffer {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop CmdMsgBuffer pe={:?} {:?}", self.pe, self);
        let mut start = std::time::Instant::now();
        while !self.empty() {
            std::thread::yield_now();
            if start.elapsed().as_secs_f64() > config().deadlock_warning_timeout {
                trace!(target: "drop", "stuck in drop CmdMsgBuffer pe={:?} {:?}", self.pe, self);
                start = std::time::Instant::now();
            }
        }
        trace!(target: "drop", "end drop CmdMsgBuffer pe={:?}", self.pe);
    }
}

struct InnerCQ {
    send_buffer: Arc<Vec<Mutex<CommSlice<CmdMsg>>>>,
    recv_buffer: Arc<Vec<RwLock<CommSlice<CmdMsg>>>>,
    // free_buffer: Arc<Vec<Mutex<CommSlice<CmdMsg>>>,
    alloc_buffer: Arc<Vec<Mutex<CommSlice<CmdMsg>>>>,
    panic_buffer: Arc<Mutex<CommSlice<CmdMsg>>>,
    cmd_buffers: Vec<Mutex<CmdMsgBuffer>>,
    release_cmd: Arc<Box<CmdMsg>>,
    clear_cmd: Arc<Box<CmdMsg>>,
    free_cmd: Arc<Box<CmdMsg>>,
    comm: Arc<Comm>,
    scheduler: Arc<Scheduler>,
    my_pe: usize,
    num_pes: usize,
    send_waiting: Vec<Arc<AtomicBool>>,
    pending_cmds: Arc<AtomicUsize>,
    pending_alloc: Arc<AtomicBool>,
    _active_cnt: Arc<AtomicUsize>,
    sent_cnt: Arc<AtomicUsize>,
    recv_cnt: Arc<AtomicUsize>,
    put_amt: Arc<AtomicUsize>,
    // get_amt: Arc<AtomicUsize>,
    alloc_id: Arc<AtomicUsize>,
    active: Arc<AtomicU8>,
}

#[lamellar_prof::prof]
impl InnerCQ {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn new(
        send_buffer_alloc: CommAlloc,
        recv_buffer_alloc: CommAlloc,
        // free_buffer_alloc: CommAlloc,
        alloc_buffer_alloc: CommAlloc,
        panic_buffer_alloc: CommAlloc,
        cmd_buffers_alloc: &Vec<Arc<Vec<CommAlloc>>>,
        release_cmd_alloc: CommAlloc,
        clear_cmd_alloc: CommAlloc,
        free_cmd_alloc: CommAlloc,
        comm: Arc<Comm>,
        scheduler: Arc<Scheduler>,
        my_pe: usize,
        num_pes: usize,
        active: Arc<AtomicU8>,
    ) -> InnerCQ {
        let mut cmd_buffers = vec![];
        let mut pe = 0;
        for addrs in cmd_buffers_alloc.iter() {
            cmd_buffers.push(Mutex::new(CmdMsgBuffer::new(addrs.clone(), pe)));
            pe += 1;
        }
        trace!("cmd_buffers {:?}", cmd_buffers);
        let send_buffer: CommSlice<CmdMsg> = send_buffer_alloc.as_comm_slice();
        let mut send_buffers = vec![];

        // here we split the send buffers by PE so each PE has its own send buffer to avoid locking
        // as we write locally into these buffers.
        for (i, cmd) in send_buffer.clone().iter_mut().enumerate() {
            (*cmd).daddr = 0;
            (*cmd).dsize = 0;
            (*cmd).cmd = Cmd::Clear;
            (*cmd).msg_hash = 0;
            (*cmd).calc_hash();
            send_buffers.push(Mutex::new(send_buffer.sub_slice(i..=i)));
        }
        trace!("send_buffer init {:?}", send_buffer);

        let recv_buffer: CommSlice<CmdMsg> = recv_buffer_alloc.as_comm_slice();
        let mut recv_buffers = vec![];

        //for recv buffers we lock each PE's buffer when we read from it, and we lock our own buffer when we are sending data to a remote PE
        for (i, cmd) in recv_buffer.clone().iter_mut().enumerate() {
            (*cmd).daddr = 0;
            (*cmd).dsize = 0;
            (*cmd).cmd = Cmd::Clear;
            (*cmd).msg_hash = 0;
            (*cmd).calc_hash();
            recv_buffers.push(RwLock::new(recv_buffer.sub_slice(i..=i)));
        }
        trace!("recv_buffer init {:?}", recv_buffer);

        // let mut free_buffer: CommSlice<CmdMsg> = free_buffer_alloc.as_comm_slice();
        // for cmd in free_buffer.iter_mut() {
        //     (*cmd).daddr = 0;
        //     (*cmd).dsize = 0;
        //     (*cmd).cmd = Cmd::Clear;
        //     (*cmd).msg_hash = 0;
        //     (*cmd).calc_hash();
        // }
        // trace!("free_buffer init {:?}", free_buffer);

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
        for (_, cmd) in panic_buffer.clone().iter_mut().enumerate() {
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

        let mut send_waiting = vec![];
        for _pe in 0..num_pes {
            send_waiting.push(Arc::new(AtomicBool::new(false)));
        }
        trace!("send_waiting init {:?}", send_waiting);
        InnerCQ {
            send_buffer: Arc::new(send_buffers),
            recv_buffer: Arc::new(recv_buffers),
            alloc_buffer: Arc::new(alloc_buffers),
            panic_buffer: Arc::new(Mutex::new(panic_buffer)),
            cmd_buffers,
            release_cmd: Arc::new(release_cmd),
            clear_cmd: Arc::new(clear_cmd),
            free_cmd: Arc::new(free_cmd),
            comm,
            scheduler,
            my_pe,
            num_pes,
            send_waiting,
            pending_cmds: Arc::new(AtomicUsize::new(0)),
            pending_alloc: Arc::new(AtomicBool::new(false)),
            _active_cnt: Arc::new(AtomicUsize::new(0)),
            sent_cnt: Arc::new(AtomicUsize::new(0)),
            recv_cnt: Arc::new(AtomicUsize::new(0)),
            put_amt: Arc::new(AtomicUsize::new(0)),
            // get_amt: Arc::new(AtomicUsize::new(0)),
            alloc_id: Arc::new(AtomicUsize::new(0)),
            active,
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    async fn empty(&self) -> bool {
        for buf in &self.cmd_buffers {
            let buf = buf.lock().await;
            if !buf.empty() {
                return false;
            }
        }
        true
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn ready(&self, src: usize) -> Option<CmdMsg> {
        // let mut recv_buffer = self.recv_buffer[src].write_blocking(); //.await;
        let mut recv_buffer = self.recv_buffer[src].write();
        let cmd = recv_buffer[0].clone();
        if cmd.check_hash() {
            match cmd.cmd {
                Cmd::Clear => None,
                // Cmd::Free => panic!("should not see free in recv buffer"),
                Cmd::Tx | Cmd::Free | Cmd::Release => {
                    if cmd.daddr == 0 {
                        return None;
                    }
                    trace!("received  cmd:{:?} from pe {} -- [{:?}]", cmd.cmd, src, cmd);
                    let res = Some(cmd.clone());
                    let cmd = &mut recv_buffer[0];
                    cmd.daddr = 0;
                    cmd.dsize = 0;
                    cmd.cmd = Cmd::Clear;
                    cmd.msg_hash = 0;
                    cmd.calc_hash();
                    res
                }
                Cmd::Alloc => panic!("should not encounter alloc here"),
                Cmd::Panic => panic!("should not encounter panic here"),
            }
        } else {
            None
        }
    }

    fn available_to_send(&self, pe: usize) -> bool {
        if let Some(send_buf) = self.send_buffer[pe].try_lock() {
            if send_buf[0].hash() == self.clear_cmd.hash() {
                return true;
            }
        }
        false
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    async fn check_alloc(&self, print: bool) {
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
                        // break;
                    }
                }
            }
            if do_alloc {
                info!(
                    "need to alloc new pool {:?}",
                    std::backtrace::Backtrace::capture()
                );
                self.send_alloc_inner(min_size).await;
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
                    debug!("panic_buf passed hash check{:?}", panic_buf[pe]);
                    paniced = true;
                    break;
                }
            }
            if paniced {
                self.active.store(CmdQStatus::Panic as u8, Ordering::SeqCst);
                return true;
                // shutdown
            }
        }
        return false;
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn try_sending_buffer(
        &self,
        dst: usize,
        cmd_buffer: &mut CmdMsgBuffer,
        printed: &mut bool,
    ) -> bool {
        if self.pending_cmds.load(Ordering::SeqCst) == 0 || cmd_buffer.full_bufs.len() > 0 {
            let mut send_buf = self.send_buffer[dst].lock_blocking();
            if send_buf[0].hash() == self.clear_cmd.hash() {
                cmd_buffer.flush_buffer(&mut send_buf[0]);
                if send_buf[0].dsize > 0 {
                    let recv_buffer = self.recv_buffer[self.my_pe].read();
                    // let recv_buffer = self.recv_buffer[self.my_pe].read_blocking(); //we can safely read here as the send_buffer lock prevents any other thread from writing to our recv buffer on the dst PE
                    // debug! {"sending data to dst({dst}) {:x}    sending cmd {:?} {:?} {:?}",
                    // recv_buffer.index_addr(0),send_buf, send_buf[0],send_buf[0].as_bytes()};
                    stats!(PE_SENDS[1][dst].fetch_add(1, Ordering::SeqCst));
                    debug!("sending cmd to dst({dst}) {:?}", send_buf[0]);

                    let send_cmd = send_buf[0].clone();
                    let _ = recv_buffer.put_unmanaged::<CmdMsg>(
                        // &self.scheduler,
                        // vec![],
                        send_cmd, //send_buf.sub_slice(dst..=dst),
                        dst, 0,
                    );
                    // .block();
                    // recv_buffer.wait();
                    // .block();
                    // .spawn();
                    self.put_amt
                        .fetch_add(send_buf[0].as_bytes().len(), Ordering::Relaxed);
                    // debug!("sent cmd to dst({dst})");
                }
                true
            } else {
                if !*printed {
                    *printed = true;
                    debug!(
                        "unable to send buffer to dst({dst}) hash not clear {:?} {:?} {:?} {:?}",
                        send_buf[0],
                        self.clear_cmd,
                        send_buf[0].hash(),
                        self.clear_cmd.hash()
                    );
                }
                false
            }
        } else {
            if !*printed {
                *printed = true;
                debug!("unable to send buffer to dst({dst}) pending cmds > 0 or no full bufs");
            }
            false
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn progress_transfers(&self, dst: usize, cmd_buffer: &mut CmdMsgBuffer) {
        let mut send_buf = self.send_buffer[dst].lock_blocking();
        if cmd_buffer.check_transfer(send_buf[0].daddr, dst) {
            send_buf[0].daddr = 0;
            send_buf[0].dsize = 0;
            send_buf[0].cmd = Cmd::Clear;
            send_buf[0].msg_hash = 0;
            send_buf[0].calc_hash();
        }
        cmd_buffer.check_free_data(&self.comm);
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    async fn send(&self, data: CommSlice<u8>, dst: usize, hash: usize) {
        stats!(PE_SENDS[0][dst].fetch_add(1, Ordering::SeqCst));
        debug!("want to send data {:?} {:?} {:x}", data, dst, hash);
        // if hash != calc_hash(data.usize_addr(), data.len())
        // {
        //     panic!(
        //         "[{:?}] 0. hash mismatch! {:?} {dst} {:x} {:x} ",
        //         std::thread::current().id(),
        //         data,
        //         hash,
        //         calc_hash(data.usize_addr(), data.len())
        //     );
        // }
        // let mut timer = std::time::Instant::now();
        self.pending_cmds.fetch_add(1, Ordering::SeqCst);
        // let mut had_to_wait = false;
        let mut printed = false;
        while self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8 {
            {
                //this is to tell the compiler we wont hold the mutex lock if we have to yield
                let mut cmd_buffer = self.cmd_buffers[dst].lock_blocking();
                if cmd_buffer.try_push(data.clone(), hash) {
                    stats!(self.sent_cnt.fetch_add(1, Ordering::SeqCst));
                    self.put_amt.fetch_add(data.len(), Ordering::Relaxed);
                    let _cnt = self.pending_cmds.fetch_sub(1, Ordering::SeqCst);
                    debug!(
                        "pushed data to cmd buffer for dst({dst}) hash {:x} pending cmds {:?} {:?}",
                        hash,
                        self.pending_cmds.load(Ordering::SeqCst),
                        data
                    );
                    break;
                }
                // had_to_wait = true;
                //while we are waiting to push our data might as well try to advance the buffers
                self.progress_transfers(dst, &mut cmd_buffer);
                self.try_sending_buffer(dst, &mut cmd_buffer, &mut printed);
            }
            async_std::task::yield_now().await;
        }

        // if hash != calc_hash(data.usize_addr(), data.len())
        // {
        //     panic!(
        //         "[{:?}] 1. hash mismatch! {:?} {dst} {:x} {:x} had to wait {had_to_wait}",
        //         std::thread::current().id(),
        //         data,
        //         hash,
        //         calc_hash(data.usize_addr(), data.len())
        //     );
        // }
        let mut im_waiting = false;
        printed = false;
        let mut timer = std::time::Instant::now();
        while self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8 {
            {
                //this is to tell the compiler we wont hold the mutex lock if we have to yield
                let mut cmd_buffer = self.cmd_buffers[dst].lock_blocking();
                if !cmd_buffer.empty() {
                    //data to send
                    self.progress_transfers(dst, &mut cmd_buffer);
                    if self.try_sending_buffer(dst, &mut cmd_buffer, &mut printed) {
                        self.send_waiting[dst].store(false, Ordering::SeqCst);
                        debug!("finished sending to dst({dst}) {:?}", data);
                        break;
                    }
                } else {
                    self.send_waiting[dst].store(false, Ordering::SeqCst);
                    debug!("no more data to send to dst({dst}) {:?}", data);
                    break;
                }
                if !im_waiting {
                    if let Ok(_) = self.send_waiting[dst].compare_exchange_weak(
                        false,
                        true,
                        Ordering::SeqCst,
                        Ordering::Relaxed,
                    ) {
                        debug!("im waiting to send to dst({dst}) {:?}", data);
                        //ensure only a single task is waiting per destination
                        im_waiting = true;
                    } else {
                        debug!(
                            "someone else is already waiting to send to dst({dst}) {:?}",
                            data
                        );
                        break;
                    }
                } else {
                    if timer.elapsed().as_secs_f32() > 10.0 {
                        timer = std::time::Instant::now();
                        debug!("im still waiting to send to dst({dst}) {:?}", data);
                    }
                }
            }
            async_std::task::yield_now().await;
        }
        // if hash != calc_hash(data.usize_addr(), data.len())
        // {
        //     panic!(
        //         "[{:?}] 2. hash mismatch! {:x} {:x} im_waiting {im_waiting} had_to_wait {had_to_wait}",
        //         std::thread::current().id(),
        //         hash,
        //         calc_hash(data.usize_addr(), data.len())
        //     );
        // }
    }

    //TODO make this async, and have an atomic variable to check if an alloc is already in progress
    // because we are probably deadlocking on this lock...
    //#[tracing::instrument(skip_all, level = "debug")]
    async fn send_alloc(&self, min_size: usize) {
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
                    self.send_alloc_inner(min_size).await;
                }
            }
            self.pending_alloc.store(false, Ordering::SeqCst);
        } else {
            // someone else is already doing an alloc...
            // wait for them to finish and then we will return so we can use the new pool
            while self.pending_alloc.load(Ordering::Relaxed) {
                async_std::task::yield_now().await;
            }
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn send_panic(&self) {
        let mut panic_buf = self.panic_buffer.lock_blocking();
        self.send_panic_inner(&mut panic_buf);
    }

    // need to include  a "barrier" count...
    //#[tracing::instrument(skip_all, level = "debug")]
    async fn send_alloc_inner(&self, min_size: usize) {
        debug!("in send_alloc_inner");
        let mut new_alloc = true;
        while new_alloc {
            new_alloc = false;
            let alloc_id = self.alloc_id.fetch_add(1, Ordering::SeqCst);
            //scoping for the lock

            let mut my_alloc_buf = self.alloc_buffer[self.my_pe].lock_blocking();
            // need to loop incase a new alloc comes in aftet this one, but recursion and mut& T dont play nicely...
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
                        // .spawn();
                    }
                }
            }

            // self.comm.wait();
            let mut start = std::time::Instant::now();
            for pe in 0..self.num_pes {
                if pe != self.my_pe {
                    let alloc_buf = self.alloc_buffer[pe].lock_blocking();
                    while !alloc_buf[0].check_hash() || alloc_buf[0].cmd != Cmd::Alloc {
                        self.comm.thread_flush();
                        async_std::task::yield_now().await;
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
            // panic!("exiting");
            info!("all pes ready to alloc");

            self.comm.alloc_pool(min_size);
            info!("allocated new pool of at least {:?}", min_size);
            // let cmd = &mut alloc_buf[self.my_pe];

            my_alloc_buf[0].daddr = 0;
            my_alloc_buf[0].dsize = 0;
            my_alloc_buf[0].cmd = Cmd::Clear;
            my_alloc_buf[0].msg_hash = 0;
            my_alloc_buf[0].calc_hash();
            for pe in 0..self.num_pes {
                if pe != self.my_pe {
                    let _ = my_alloc_buf.put_unmanaged::<CmdMsg>(my_alloc_buf[0], pe, 0);
                    // .spawn();
                }
            }
            info!("sent clear cmds");
            // self.comm.wait();
            for pe in 0..self.num_pes {
                if pe == self.my_pe {
                    while !my_alloc_buf[0].check_hash() || my_alloc_buf[0].cmd != Cmd::Clear {
                        if my_alloc_buf[0].cmd == Cmd::Alloc {
                            if my_alloc_buf[0].daddr > alloc_id {
                                new_alloc = true;
                                break;
                            }
                        }
                        async_std::task::yield_now().await;
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
                        async_std::task::yield_now().await;
                    }
                }
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
                    let _ = panic_buf.put_unmanaged::<CmdMsg>(
                        // &self.scheduler,
                        // vec![],
                        panic_buf[self.my_pe], //panic_buf.sub_slice(self.my_pe..=self.my_pe),
                        pe,
                        self.my_pe,
                    );
                    // .spawn();
                }
            }
            self.comm.thread_wait(); //only need to wait on puts issued by this thread
            // join_all(txs).await;
        }
    }

    // We can run into a race condition between send_release and send_free so lets only use send_free.
    // //#[tracing::instrument(skip_all, level = "debug")]
    // fn send_release(&self, dst: usize, local_daddr_alloc: CommAlloc) {
    //     // let cmd_buffer = self.cmd_buffers[dst].lock();
    //     // debug!(
    //     //     "sending release to dst[{dst}]: {:?} cmd: {:?} {:?} {:?} 0x{:x} 0x{:x}",
    //     //     self.release_cmd,
    //     //     cmd,
    //     //     self.release_cmd.cmd_as_bytes(),
    //     //     cmd.cmd_as_bytes(),
    //     //     self.release_cmd.as_addr(),
    //     //     cmd.daddr + offset_of!(CmdMsg, cmd)
    //     // );
    //     // let local_daddr = self.comm.local_addr(dst, cmd.daddr);

    //     // let (local_daddr_alloc, offset) =
    //     //     self.comm.local_alloc_and_offset_from_remote_pe_and_addr(dst, cmd.daddr);
    //     // let local_daddr_slice =
    //     //     local_daddr_alloc.comm_slice_at_byte_offset::<Cmd>(offset + offset_of!(CmdMsg, cmd), 1);
    //     // local_daddr_slice
    //     //     .put::<Cmd>(&self.scheduler, None, self.release_cmd.cmd, dst, 0)
    //     //     .spawn();

    //     let local_cmd_slice =
    //         local_daddr_alloc.comm_slice_at_byte_offset::<Cmd>(offset_of!(CmdMsg, cmd), 1);
    //     local_cmd_slice.put_unmanaged::<Cmd>(
    //         // &self.scheduler, None,
    //         self.release_cmd.cmd,
    //         dst,
    //         0,
    //     );
    // }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn send_free(&self, dst: usize, cmd: CmdMsg) {
        trace!(
            "sending free to dst[{dst}]: {:?} cmd: {:?} ",
            self.free_cmd, cmd,
        );
        let (local_daddr_alloc, offset) = self
            .comm
            .local_alloc_and_offset_from_remote_pe_and_addr(dst, cmd.daddr);
        let local_daddr_slice =
            local_daddr_alloc.comm_slice_at_byte_offset::<Cmd>(offset + offset_of!(CmdMsg, cmd), 1);
        let _ = local_daddr_slice.put_unmanaged::<Cmd>(self.free_cmd.cmd, dst, 0);
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn check_transfers(&self, src: usize) {
        let mut cmd_buffer = self.cmd_buffers[src].lock_blocking(); //.await;
        // trace!("checking transfers for {src}");
        self.progress_transfers(src, &mut cmd_buffer); //.await;
    }

    //update cmdbuffers to include a hash the wait on that here
    // //#[tracing::instrument(skip(self), level = "debug")]
    async fn get_data(
        &self,
        src: usize,
        cmd: CmdMsg,
        msg_id: usize,
        lamellae: &Arc<Lamellae>,
    ) -> Vec<CmdMsg> {
        trace!(target: "lamellae_debug", "entering get_data lamellae cnt: {:?}", Arc::strong_count(lamellae));
        let (local_daddr_alloc, offset) = self
            .comm
            .local_alloc_and_offset_from_remote_pe_and_addr(src, cmd.daddr);
        let num_cmds = cmd.dsize / std::mem::size_of::<CmdMsg>();
        let remote_cmd_buffer =
            local_daddr_alloc.comm_slice_at_byte_offset::<CmdMsg>(offset, num_cmds);
        trace!(
            "msg_id: {msg_id} command queue getting data from {src}, {:x} local_alloc: {:?} offset ({:?}, {:x}) {:?}",
            cmd.daddr, local_daddr_alloc, offset, offset, remote_cmd_buffer,
        );

        if let Ok(data) = self
            .comm
            .rt_alloc(cmd.dsize, std::mem::align_of::<CmdMsg>())
        {
            trace!(
                "msg_id: {msg_id} allocated local buffer for get_data at addr: {:?} for cmd from {src}",
                data
            );
            let mut buffer = unsafe {
                LamellarBuffer::<CmdMsg, CommSlice<CmdMsg>>::from_comm_slice(
                    data.as_comm_slice(),
                    lamellae.clone(),
                )
            };
            remote_cmd_buffer
                .get_into_buffer(&self.scheduler, None, src, 0, buffer.split_off(0))
                .await;
            // let _ = remote_cmd_buffer.get_into_buffer_unmanaged(src, 0, buffer.split_off(0));
            // let mut timer = std::time::Instant::now();
            // while calc_hash(
            //     unsafe { buffer.orig_as_ptr() } as usize,
            //     buffer.orig_num_bytes(),
            // ) != cmd.msg_hash
            // {
            //     if timer.elapsed().as_secs_f64() > config().deadlock_warning_timeout {
            //         trace!(
            //             "msg_id: {msg_id} data hash mismatch from {:?}!!! {:?} {:?} {:?} -- calced hash {:x} expected {:x}",
            //             src,
            //             cmd,
            //             buffer.orig_num_bytes(),
            //             unsafe{ buffer.orig_as_casted_slice::<u8>() },
            //             calc_hash(
            //             unsafe{buffer.orig_as_ptr() } as usize,
            //                 buffer.orig_num_bytes()
            //             ),
            //             cmd.msg_hash,

            //         );
            //         timer = std::time::Instant::now();
            //     }
            //     self.comm.thread_flush();
            //     async_std::task::yield_now().await;
            // }
            let data_temp = buffer.async_unwrap().await;
            let data_vec = data_temp.to_vec();
            stats!(PE_RECVS[1][src].fetch_add(1, Ordering::SeqCst));
            debug!("got data from {src} -- {:?} cmds", data_vec.len());
            data_vec
        } else {
            let data = vec![CmdMsg::default(); num_cmds];
            let mut buffer = LamellarBuffer::<CmdMsg, Vec<CmdMsg>>::from_vec_with_lamellae(
                data,
                lamellae.clone(),
            );

            remote_cmd_buffer
                .get_into_buffer(&self.scheduler, None, src, 0, buffer.split_off(0))
                .await;
            // remote_cmd_buffer
            //     .get_into_buffer_unmanaged(src, 0, buffer.split_off(0));
            // let mut timer = std::time::Instant::now();
            // while calc_hash(
            //     unsafe { buffer.orig_as_ptr() } as usize,
            //     buffer.orig_num_bytes(),
            // ) != cmd.msg_hash
            // {
            //     if timer.elapsed().as_secs_f64() > config().deadlock_warning_timeout {
            //         trace!(
            //             "msg_id: {msg_id} data hash mismatch from {:?}!!! {:?} {:?} {:?} -- calced hash {:x} expected {:x}",
            //             src,
            //             cmd,
            //             buffer.orig_num_bytes(),
            //             unsafe{ buffer.orig_as_casted_slice::<u8>() },
            //             calc_hash(
            //             unsafe{buffer.orig_as_ptr() } as usize,
            //                 buffer.orig_num_bytes()
            //             ),
            //             cmd.msg_hash,

            //         );
            //         timer = std::time::Instant::now();
            //     }
            //     self.comm.thread_flush();
            //     async_std::task::yield_now().await;
            // }
            // task.await;
            let data = buffer.async_unwrap().await;
            // .expect("Multiple copies of data still exist");

            stats!(PE_RECVS[1][src].fetch_add(1, Ordering::SeqCst));
            debug!("got data from {src} -- {:?} cmds", data.len());
            trace!(target: "lamellae_debug", "leaving get_data lamellae cnt: {:?}", Arc::strong_count(&lamellae));
            data
        }
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
        trace!(target: "lamellae_debug", "entering get_serialized_data lamellae cnt: {:?}", Arc::strong_count(lamellae));
        let len = ser_data.len();
        // let vec = vec![0u8; len];
        let mut buffer = unsafe {
            LamellarBuffer::<u8, CommSlice<u8>>::from_comm_slice(
                ser_data.header_and_data_as_bytes_mut(),
                lamellae.clone(),
            )
        };
        // let mut buffer = LamellarBuffer::<u8, Vec<u8>>::from_vec(vec);
        trace!("get_serialized_data {:?} {:?} {:x}", src, cmd, cmd.daddr);
        let (local_daddr_alloc, offset) = self
            .comm
            .local_alloc_and_offset_from_remote_pe_and_addr(src, cmd.daddr);
        let task = local_daddr_alloc
            .get_into_buffer(&self.scheduler, None, src, offset, buffer.split_off(0))
            .spawn();
        // let _task = local_daddr_alloc
        //     .get_into_buffer_unmanaged( src, offset, buffer.split_off(0));
        // let data_slice = buffer.try_unwrap().unwrap();
        // let data_slice = &tmp_data;
        // let orig_thread = std::thread::current().id();
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
                //config().deadlock_warning_timeout {
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
                    // orig_thread,
                );
                timer = std::time::Instant::now();
            }
            self.comm.thread_flush();
            async_std::task::yield_now().await;
        }
        task.await;
        // unsafe {
        //     std::ptr::copy_nonoverlapping(
        //         data_slice.as_ptr(),
        //         ser_data.header_and_data_as_bytes_mut().as_mut_ptr(),
        //         len,
        //     );
        // }
        trace!(
            "after msg_id: {msg_id} calced hash: {:x} cmd msg hash {:x} ",
            calc_hash(data_slice.as_ptr() as usize, len),
            cmd.msg_hash,
        );
        stats!(PE_RECVS[0][src].fetch_add(1, Ordering::SeqCst));
        debug!(
            "got serialized data from {src} {:x} -- {} bytes",
            cmd.daddr,
            ser_data.len()
        );
        trace!(target: "lamellae_debug", "leaving get_serialized_data lamellae cnt: {:?}", Arc::strong_count(lamellae));
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    async fn get_cmd(
        &self,
        src: usize,
        cmd: CmdMsg,
        msg_id: usize,
        lamellae: &Arc<Lamellae>,
    ) -> SerializedData {
        trace!(target: "lamellae_debug", "entering get_cmd from {} of size {} lamellae cnt: {:?}", src, cmd.dsize, Arc::strong_count(lamellae));
        let mut ser_data = self.comm.new_serialized_data(cmd.dsize as usize);
        let mut print = true;

        while ser_data.is_err() && self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8 {
            if print {
                debug!("msg_id: {msg_id} get cmd buf stuck waiting for alloc");
                print = false;
            }
            async_std::task::yield_now().await;
            self.send_alloc(cmd.dsize).await;
            ser_data = self.comm.new_serialized_data(cmd.dsize as usize);
        }
        let mut ser_data = ser_data.unwrap();
        self.get_serialized_data(src, cmd, &mut ser_data, msg_id, lamellae)
            .await;
        stats!(self.recv_cnt.fetch_add(1, Ordering::SeqCst));
        trace!(target: "lamellae_debug", "leaving get_cmd lamellae cnt: {:?}", Arc::strong_count(lamellae));
        ser_data
    }

    // //#[tracing::instrument(skip_all, level = "debug")]
    async fn get_cmd_buf(
        &self,
        src: usize,
        cmd: CmdMsg,
        msg_id: usize,
        lamellae: &Arc<Lamellae>,
    ) -> Vec<CmdMsg> {
        // trace!("getting cmd buf from {}", src);
        let data = self.get_data(src, cmd, msg_id, lamellae).await;
        data
    }
}

#[lamellar_prof::prof]
impl Drop for InnerCQ {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop InnerCQ");
        debug!("dropping InnerCQ");
        let old = std::mem::replace(
            Arc::get_mut(&mut self.release_cmd).unwrap(),
            Box::new(CmdMsg {
                daddr: 0,
                dsize: 0,
                cmd: Cmd::Clear,
                msg_hash: 0,
                cmd_hash: 0,
            }),
        );
        let _ = Box::into_raw(old);
        let old = std::mem::replace(
            Arc::get_mut(&mut self.clear_cmd).unwrap(),
            Box::new(CmdMsg {
                daddr: 0,
                dsize: 0,
                cmd: Cmd::Clear,
                msg_hash: 0,
                cmd_hash: 0,
            }),
        );
        let _ = Box::into_raw(old);
        let old = std::mem::replace(
            Arc::get_mut(&mut self.free_cmd).unwrap(),
            Box::new(CmdMsg {
                daddr: 0,
                dsize: 0,
                cmd: Cmd::Clear,
                msg_hash: 0,
                cmd_hash: 0,
            }),
        );
        let _ = Box::into_raw(old);
        self.cmd_buffers.clear();
        debug!("dropped InnerCQ");
        trace!(target: "drop", "end drop InnerCQ");
    }
}

pub(crate) struct CQBatched {
    cq: Arc<InnerCQ>,
    _send_buffer: CommAlloc,
    _recv_buffer: CommAlloc,
    _alloc_buffer: CommAlloc,
    _panic_buffer: CommAlloc,
    _release_cmd: CommAlloc,
    _clear_cmd: CommAlloc,
    _free_cmd: CommAlloc,
    _cmd_buffers: Vec<Arc<Vec<CommAlloc>>>,
    _comm: Arc<Comm>,
    pub(crate) scheduler: Arc<Scheduler>,
    active: Arc<AtomicU8>,
}

#[lamellar_prof::prof]
impl CQBatched {
    fn print_arc_cnts(&self) {
        trace!(target: "drop",
            "CQBatched Arc counts: cq: {:?}  cmd_buffers: {:?} comm: {:?}",
            Arc::strong_count(&self.cq),
            self._cmd_buffers.iter().map(|cb| Arc::strong_count(cb)).collect::<Vec<_>>(),
            Arc::strong_count(&self._comm),
        );
    }
    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new(
        comm: Arc<Comm>,
        scheduler: Arc<Scheduler>,
        my_pe: usize,
        num_pes: usize,
        active: Arc<AtomicU8>,
    ) -> CQBatched {
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

        let mut cmd_buffers = vec![];
        for _pe in 0..num_pes {
            let mut addrs = vec![];
            for _i in 0..config().cmd_buf_cnt {
                let addr = comm
                    .rt_alloc(
                        config().cmd_buf_len * std::mem::size_of::<CmdMsg>() + 1,
                        std::mem::align_of::<CmdMsg>(),
                    )
                    .unwrap();
                addrs.push(addr);
            }
            cmd_buffers.push(Arc::new(addrs));
        }
        trace!("cmd_buffers: {:?}", cmd_buffers);
        // let cmd_buffers=Arc::new(cmd_buffers);
        let cq = InnerCQ::new(
            send_buffer.clone(),
            recv_buffer.clone(),
            alloc_buffer.clone(),
            panic_buffer.clone(),
            &cmd_buffers.clone(),
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
        CQBatched {
            cq: Arc::new(cq),
            _send_buffer: send_buffer,
            _recv_buffer: recv_buffer,
            _alloc_buffer: alloc_buffer,
            _panic_buffer: panic_buffer,
            _release_cmd: release_cmd,
            _clear_cmd: clear_cmd,
            _free_cmd: free_cmd,
            _cmd_buffers: cmd_buffers,
            _comm: comm,
            scheduler,
            active,
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) async fn send_alloc(&self, min_size: usize) {
        self.cq.send_alloc(min_size).await;
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn send_panic(&self) {
        self.cq.send_panic();
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) async fn send_data(&self, data: SerializedData, dst: usize) {
        let hash = calc_hash(data.ser_data_bytes.usize_addr(), data.len());
        // debug!(
        //     "sending data: {:?} {:?} {} {:x} {:?}..{:?}",
        //     data,
        //     dst,
        //     hash,
        //     hash,
        //     data.ser_data_bytes.as_slice().get(0..32),
        //     data.ser_data_bytes
        //         .as_slice()
        //         .get(data.len().saturating_sub(32)..data.len())
        // );
        let data_slice = data.ser_data_bytes.clone();
        data.leak_alloc()
            .leak()
            .expect("failed to leak alloc in send_data");
        self.cq.send(data_slice, dst, hash).await;
    }

    pub(crate) async fn send_vec(&self, vec_data: Vec<u8>, dst: usize) {
        trace!(
            "sending vec_data of len {:?} to dst {:?}",
            vec_data.len(),
            dst
        );
        let mut data = self
            .cq
            .comm
            .rt_alloc(vec_data.len(), std::mem::align_of::<u8>());
        while let Err(_) = data {
            async_std::task::yield_now().await;
            data = self
                .cq
                .comm
                .rt_alloc(vec_data.len(), std::mem::align_of::<u8>());
        }
        let data = data.unwrap();
        unsafe {
            std::ptr::copy_nonoverlapping(vec_data.as_ptr(), data.as_mut_ptr(), vec_data.len());
        }
        let data_slice = data.as_comm_slice().clone();
        let hash = calc_hash(data_slice.usize_addr(), data_slice.len());
        data.leak().expect("failed to leak alloc in send_vec");
        self.cq.send(data_slice, dst, hash).await;
    }

    pub(crate) fn wait_all_print(&self) {
        self.print_arc_cnts();
        println!("command queue");
        println!(
            "sends {:?}",
            print_stats!(
                PE_SENDS
                    .iter()
                    .map(|x| x
                        .iter()
                        .map(|y| y.load(Ordering::SeqCst))
                        .collect::<Vec<_>>())
                    .collect::<Vec<_>>()
            )
        );
        println!(
            "recvs {:?}",
            print_stats!(
                PE_RECVS
                    .iter()
                    .map(|x| x
                        .iter()
                        .map(|y| y.load(Ordering::SeqCst))
                        .collect::<Vec<_>>())
                    .collect::<Vec<_>>()
            )
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
            let cmd_buffer = self.cq.cmd_buffers[pe].lock_blocking();
            println!("cmd buffer for pe {pe}: {:?}", cmd_buffer);
            // if !cmd_buffer.waiting_bufs.empty() {
            println!("tx bufs: {:?}", cmd_buffer.tx_bufs);
            for (addr, buf) in cmd_buffer.tx_bufs.iter() {
                println!("tx buf: {:?} {:?}", addr, buf);
                for cmd in buf.iter() {
                    println!("tx cmd: {:?} {:?}", cmd, cmd.dsize);
                    if cmd.dsize > 0 {
                        println!(
                            "active cmd: {:?} new calced hash: {:x} {:?} {:?}",
                            cmd,
                            calc_hash(cmd.daddr, cmd.dsize),
                            unsafe {
                                std::slice::from_raw_parts(
                                    cmd.daddr as *const u8,
                                    std::cmp::min(32, cmd.dsize),
                                )
                            },
                            unsafe {
                                std::slice::from_raw_parts(
                                    (cmd.daddr + cmd.dsize.saturating_sub(32)) as *const u8,
                                    std::cmp::min(32, cmd.dsize),
                                )
                            },
                        );
                    }
                }
            }
            for (_, buf) in cmd_buffer.waiting_bufs.iter() {
                // println!("waiting buf: {:?}", buf);
                for cmd in buf.iter() {
                    if cmd.dsize > 0 {
                        debug!(
                            "waiting cmd: {:?} new calced hash: {:x}",
                            cmd,
                            calc_hash(cmd.daddr, cmd.dsize)
                        );
                    }
                }
            }
            // }
            let send_buffer = self.cq.send_buffer[pe].lock_blocking();
            // let recv_buffer = self.cq.recv_buffer[pe].read_blocking();
            let recv_buffer = self.cq.recv_buffer[pe].read();
            println!(
                "recv_buffer outer ptr for pe {pe}: {:?}",
                &*recv_buffer as *const CommSlice<CmdMsg>
            );
            println!(
                "send_buffer outer ptr for pe {pe}: {:?}",
                &*send_buffer as *const CommSlice<CmdMsg>
            );
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
        // println!("finished command queue wait_all_print");
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
            self.cq.check_alloc(print).await;
            // print = false;
            // async_std::task::yield_now().await;
            async_std::task::sleep(std::time::Duration::from_millis(10)).await;
        }
        self.print_arc_cnts();
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
        self.print_arc_cnts();
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) async fn recv_data(&self, lamellae: Arc<Lamellae>) {
        let comm = lamellae.comm();
        let num_pes = comm.num_pes();
        let my_pe = comm.my_pe();
        let mut timer = std::time::Instant::now();
        trace!(target: "lamellae_debug", "entering recv_data lamellae cnt: {:?}", Arc::strong_count(&lamellae));
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
                    if let Some(cmd_buf_cmd) = self.cq.ready(src) {
                        // timer =  std::time::Instant::now();
                        let cmd_buf_cmd = cmd_buf_cmd;
                        trace!("recv_data {:?}", cmd_buf_cmd);
                        match cmd_buf_cmd.cmd {
                            Cmd::Alloc => panic!("should not encounter alloc here"),
                            Cmd::Panic => panic!("should not encounter panic here"),
                            Cmd::Clear | Cmd::Release => {
                                panic!("should not be possible to see cmd clear or release here")
                            }
                            Cmd::Free => panic!("should not be possible to see free here"),
                            Cmd::Tx => {
                                trace!("got tx");
                                let cq = self.cq.clone();
                                let lamellae = lamellae.clone();
                                trace!(target: "lamellae_debug", "[{:?}] recv_data got tx cmd_buf from {src} submitting get cmd buf task cq cloned {:?} lamellae cnt {:?}", std::thread::current().id(), Arc::strong_count(&cq), Arc::strong_count(&lamellae));

                                let scheduler1 = self.scheduler.clone();
                                let task = async move {
                                    trace!("going to get cmd_buf {:?} from {:?}", cmd_buf_cmd, src);
                                    let msg_id = MSG_ID.fetch_add(1, Ordering::SeqCst);
                                    let data =
                                        cq.get_cmd_buf(src, cmd_buf_cmd, msg_id, &lamellae).await;
                                    let mut i = 0;
                                    let len = data.len();
                                    let cmd_cnt: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(len));

                                    trace!("src: {:?} cmd_buf len {:?} msg_id: {msg_id}", src, len);
                                    // let scheduler2 = scheduler1.clone();
                                    // let task = async move {
                                    for cmd in data.into_iter() {
                                        if cmd.dsize != 0 {
                                            let cq = cq.clone();
                                            let lamellae_c = lamellae.clone();
                                            trace!(target: "lamellae_debug", "[{:?}] recv_data submitting get command task for cmd {:?} from {src} msg_id: {msg_id} [{:?}/{:?}] cmd_cnt: {:?} cq cloned {:?} lamellae cnt {:?}", std::thread::current().id(), cmd, i, len, cmd_cnt, Arc::strong_count(&cq), Arc::strong_count(&lamellae_c));

                                            let scheduler2 = scheduler1.clone();
                                            let cmd_cnt_clone = cmd_cnt.clone();
                                            let task = async move {
                                                let work_data =
                                                    cq.get_cmd(src, cmd, msg_id, &lamellae_c).await;
                                                debug!(
                                                    "msg_id: {msg_id} submitting remote am for cmd {:?} [{:?}/{:?}] from {src}",
                                                    cmd, i, len
                                                );
                                                scheduler2.submit_remote_am(work_data, &lamellae_c);
                                                trace!(target: "lamellae_debug", "submitted_remote_am lamellae cnt: {:?}", Arc::strong_count(&lamellae_c));
                                                if cmd_cnt_clone.fetch_sub(1, Ordering::SeqCst) == 1
                                                {
                                                    //     debug!(
                                                    //         "done with all cmds for msg_id: {msg_id}"
                                                    //     );
                                                    cq.send_free(src, cmd_buf_cmd);
                                                }
                                                trace!(target: "lamellae_debug", "finished processing cmd {:?} from {src} msg_id: {msg_id} [{:?}/{:?}] remaining cmds: {:?} lamellae cnt {:?}", cmd,  i, len, cmd_cnt_clone.load(Ordering::SeqCst), Arc::strong_count(&lamellae_c));
                                            };
                                            trace!(
                                                "[{:?}] recv_data submitting get command task",
                                                std::thread::current().id(),
                                            );
                                            scheduler1.submit_io_task(task);
                                            trace!(target: "lamellae_debug","submitteg scheduler1 io_task lamellae cnt: {:?}", Arc::strong_count(&lamellae));
                                            i += 1;
                                        } else {
                                            panic!(
                                                "should not be here! {:?} -- {:?} [{:?}/{:?}]",
                                                cmd_buf_cmd, cmd, i, len
                                            );
                                        }
                                    }
                                    // debug!("done with all cmds for msg_id: {msg_id}");
                                    // cq.send_free(src, cmd_buf_cmd);
                                    // };
                                    // scheduler1.submit_io_task(task);
                                    trace!(target: "lamellae_debug",
                                        "[{:?}] finished recv_data submitted get command task for cmd_buf {:?} from {src} msg_id: {msg_id} lamellae cnt {:?} cq cnt {:?}",
                                        std::thread::current().id(),
                                        cmd_buf_cmd,
                                        Arc::strong_count(&lamellae),
                                        Arc::strong_count(&cq)
                                    );
                                };
                                self.scheduler.submit_io_task(task);
                            }
                        }
                    }
                    self.cq.check_transfers(src);
                }
            }

            comm.thread_flush();
            async_std::task::yield_now().await;
        }
        self.active
            .store(CmdQStatus::Finished as u8, Ordering::SeqCst);
        self.print_arc_cnts();
        trace!(target: "drop", "recv_data finished, cq empty? {:?}  scheduler active? {:?} lamellae cnt {:?}", self.cq.empty().await, self.scheduler.active(0), Arc::strong_count(&lamellae));
    }

    // //#[tracing::instrument(skip_all, level = "debug")]
    // pub(crate) fn tx_amount(&self) -> usize {
    //     // println!("cq put: {:?} get {:?}",self.cq.put_amt.load(Ordering::SeqCst) ,self.cq.get_amt.load(Ordering::SeqCst));
    //     self.cq.put_amt.load(Ordering::SeqCst) + self.cq.get_amt.load(Ordering::SeqCst)
    // }

    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn mem_per_pe() -> usize {
        (config().cmd_buf_len * config().cmd_buf_cnt + 4) * std::mem::size_of::<CmdMsg>()
    }

    pub(crate) fn available_to_send(&self, pe: usize) -> bool {
        self.cq.available_to_send(pe)
    }
}

#[lamellar_prof::prof]
impl Drop for CQBatched {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop CQBatched");
        debug!(
            "sends {:?}",
            print_stats!(
                PE_SENDS
                    .iter()
                    .map(|x| x
                        .iter()
                        .map(|y| y.load(Ordering::SeqCst))
                        .collect::<Vec<_>>())
                    .collect::<Vec<_>>()
            )
        );
        debug!(
            "recvs {:?}",
            print_stats!(
                PE_RECVS
                    .iter()
                    .map(|x| x
                        .iter()
                        .map(|y| y.load(Ordering::SeqCst))
                        .collect::<Vec<_>>())
                    .collect::<Vec<_>>()
            )
        );
        trace!(target: "drop", "end drop CQBatched");
    }
}

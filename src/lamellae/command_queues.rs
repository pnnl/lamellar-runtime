use super::{
    comm::{CmdQStatus, CommAlloc, CommInfo, CommMem, CommProgress, CommSlice},
    Comm, Lamellae, RemoteSerializedData, SerializedData,
};
use crate::{
    env_var::config, lamellae::CommAllocRdma, memregion::LamellarBuffer, scheduler::Scheduler,
};
use core::panic;
// use parking_lot::Mutex;
use async_lock::Mutex;
use std::collections::HashMap;
use std::num::Wrapping;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::{debug, info, trace, warn};

#[repr(C)]
#[derive(Clone, Copy)]
struct CmdMsg {
    // we send this to remote nodes
    daddr: usize,
    dsize: usize,
    cmd: Cmd,
    msg_hash: usize,
    cmd_hash: usize,
}

impl Default for CmdMsg {
    #[tracing::instrument(skip_all, level = "debug")]
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

#[repr(usize)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Cmd {
    Clear = 1,
    Free,
    Release,
    Print,
    Tx,
    Alloc,
    Panic,
}

#[tracing::instrument(skip_all, level = "debug")]
fn calc_hash(addr: usize, len: usize) -> usize {
    //we split into a u64 slice and a u8 slice as u64 seems to compute faster.
    let num_usizes = len / std::mem::size_of::<usize>();
    //let u64_slice = unsafe { std::slice::from_raw_parts(addr as *const u64, num_u64s) };
    let num_u8s = len % std::mem::size_of::<usize>();
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

impl CmdMsg {
    #[tracing::instrument(skip_all, level = "debug")]
    fn as_bytes(&self) -> &[u8] {
        let pointer = self as *const Self as *const u8;
        let size = std::mem::size_of::<Self>();
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(pointer, size) };
        slice
    }
    #[tracing::instrument(skip_all, level = "debug")]
    fn as_addr(&self) -> usize {
        self as *const CmdMsg as usize
    }
    #[tracing::instrument(skip_all, level = "debug")]
    fn cmd_as_bytes(&self) -> &[u8] {
        let pointer = &self.cmd as *const Cmd as *const u8;
        let size = std::mem::size_of::<Cmd>();
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(pointer, size) };
        slice
    }
    fn cmd_as_comm_slice(&self) -> CommSlice<Cmd> {
        let pointer = &self.cmd as *const Cmd;
        let slice: CommSlice<Cmd> = unsafe { CommSlice::from_raw_parts(pointer, 1) };
        slice
    }
    #[tracing::instrument(skip_all, level = "debug")]
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
    #[tracing::instrument(skip_all, level = "debug")]
    fn calc_hash(&mut self) {
        self.cmd_hash = self.hash()
    }
    #[tracing::instrument(skip_all, level = "debug")]
    fn check_hash(&self) -> bool {
        if self.cmd_hash == self.hash() && self.cmd_hash != 0 {
            true
        } else {
            false
        }
    }
}

impl std::fmt::Debug for CmdMsg {
    #[tracing::instrument(skip_all, level = "debug")]
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
    #[tracing::instrument(skip_all, level = "debug")]
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
    #[tracing::instrument(skip_all, level = "debug")]
    fn full(&self) -> bool {
        self.index == self.max_size
    }
    #[tracing::instrument(skip_all, level = "debug")]
    fn reset(&mut self) {
        self.index = 0;
        self.allocated_cnt = 0;
    }
    #[tracing::instrument(skip_all, level = "debug")]
    fn addr(&self) -> usize {
        self.addr
    }
    #[tracing::instrument(skip_all, level = "debug")]
    fn size(&self) -> usize {
        self.index * std::mem::size_of::<CmdMsg>()
    }
    #[tracing::instrument(skip_all, level = "debug")]
    fn iter(&self) -> std::slice::Iter<CmdMsg> {
        self.buf[0..self.index].iter()
    }
    #[tracing::instrument(skip_all, level = "debug")]
    fn state(&self) -> Cmd {
        self.buf[0].cmd
    }
}

impl std::fmt::Debug for CmdBuf {
    #[tracing::instrument(skip_all, level = "debug")]
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

// impl Drop for CmdBuf {
//     #[tracing::instrument(skip_all, level = "debug")]
//     fn drop(&mut self) {
//         // println!("dropping cmd buf");
//         let old = std::mem::take(&mut self.buf);
//         let _ = Box::into_raw(old);
//         // println!("dropped cmd buf");
//     }
// }

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
    #[tracing::instrument(skip_all, level = "debug")]
    fn new(addrs: Arc<Vec<CommAlloc>>, pe: usize) -> CmdMsgBuffer {
        let mut bufs = vec![];
        for alloc in addrs.iter() {
            // println!("CmdMsgBuffer {:x} {:?}",addr,addr);
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
    #[tracing::instrument(skip_all, level = "debug")]
    fn try_push(&mut self, data: CommSlice<u8>, hash: usize) -> bool {
        trace!("trying to push data {:?} to buffer", data);
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
    #[tracing::instrument(skip_all, level = "debug")]
    fn flush_buffer(&mut self, cmd: &mut CmdMsg) {
        trace!("flush buffer before: {:?}", self);
        if let Some(buf) = self.full_bufs.pop() {
            // println!("flushing {:?} {:?}",buf.addr(),buf.size());
            cmd.daddr = buf.addr();
            cmd.dsize = buf.size();
            cmd.cmd = Cmd::Tx;
            cmd.msg_hash = calc_hash(cmd.daddr, cmd.dsize);

            cmd.calc_hash();
            self.tx_bufs.insert(buf.addr(), buf);
        } else if let Some(buf) = std::mem::replace(&mut self.cur_buf, None) {
            // println!("flushing {:?} {:?}",buf.addr(),buf.size());
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

    #[tracing::instrument(skip(self), level = "debug")]
    fn check_transfer(&mut self, addr: usize) -> bool {
        if let Some(buf) = self.tx_bufs.remove(&addr) {
            match buf.state() {
                Cmd::Tx => {
                    // trace!("transfer in progress {:x} {:?} !!", addr, buf.state());
                    self.tx_bufs.insert(addr, buf);
                    false
                }
                Cmd::Release | Cmd::Free => {
                    debug!("transfer complete {:x} {:?}!!", addr, buf.state());
                    if buf.allocated_cnt > 0 {
                        self.waiting_bufs.insert(buf.addr(), buf);
                        true
                    } else {
                        panic!("am I ever here?");
                    }
                }
                Cmd::Clear => panic!("should not clear before release"),
                // Cmd::Free => panic!("should not free before release"),
                Cmd::Print => panic!("shoud not encounter Print here"),
                Cmd::Alloc => panic!("should not encounter alloc here"),
                Cmd::Panic => panic!("should not encounter panic here"),
            }
        } else {
            false
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
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
                Cmd::Print => panic!("shoud not encounter Print here"),
                Cmd::Alloc => panic!("should not encounter alloc here"),
                Cmd::Panic => panic!("should not encounter panic here"),
            }
        }

        for buf_addr in freed_bufs {
            if let Some(mut buf) = self.waiting_bufs.remove(&buf_addr) {
                for cmd in buf.iter() {
                    if cmd.dsize > 0 {
                        let ser_data_addr = cmd.daddr;
                        // debug!(
                        //     "need to decrement  data with ser_data addr: {:x} ",
                        //     ser_data_addr
                        // );
                        unsafe { SerializedData::decrement_cnt_from_addr(comm, ser_data_addr) };
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

    #[tracing::instrument(skip_all, level = "debug")]
    fn empty(&self) -> bool {
        self.empty_bufs.len() == self.num_bufs
    }
}

impl std::fmt::Debug for CmdMsgBuffer {
    #[tracing::instrument(skip_all, level = "debug")]
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
    #[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
        while !self.empty() {
            std::thread::yield_now()
        }
    }
}

struct InnerCQ {
    send_buffer: Arc<Vec<Mutex<CommSlice<CmdMsg>>>>,
    recv_buffer: Arc<Vec<Mutex<CommSlice<CmdMsg>>>>,
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
    get_amt: Arc<AtomicUsize>,
    alloc_id: Arc<AtomicUsize>,
    active: Arc<AtomicU8>,
}

impl InnerCQ {
    #[tracing::instrument(skip_all, level = "debug")]
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
        let mut send_buffer: CommSlice<CmdMsg> = send_buffer_alloc.as_comm_slice();
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

        let mut recv_buffer: CommSlice<CmdMsg> = recv_buffer_alloc.as_comm_slice();
        let mut recv_buffers = vec![];

        //for recv buffers we lock each PE's buffer when we read from it, and we lock our own buffer when we are sending data to a remote PE
        for (i, cmd) in recv_buffer.clone().iter_mut().enumerate() {
            (*cmd).daddr = 0;
            (*cmd).dsize = 0;
            (*cmd).cmd = Cmd::Clear;
            (*cmd).msg_hash = 0;
            (*cmd).calc_hash();
            recv_buffers.push(Mutex::new(recv_buffer.sub_slice(i..=i)));
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

        let mut alloc_buffer: CommSlice<CmdMsg> = alloc_buffer_alloc.as_comm_slice();
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

        let mut panic_buffer: CommSlice<CmdMsg> = panic_buffer_alloc.as_comm_slice();
        for (i, cmd) in panic_buffer.clone().iter_mut().enumerate() {
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
            get_amt: Arc::new(AtomicUsize::new(0)),
            alloc_id: Arc::new(AtomicUsize::new(0)),
            active: active,
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    async fn empty(&self) -> bool {
        for buf in &self.cmd_buffers {
            let buf = buf.lock().await;
            if !buf.empty() {
                return false;
            }
        }
        true
    }

    fn blocking_empty(&self) -> bool {
        for buf in &self.cmd_buffers {
            let buf = buf.lock_blocking();
            if !buf.empty() {
                return false;
            }
        }
        true
    }

    #[tracing::instrument(skip_all, level = "debug")]
    async fn ready(&self, src: usize) -> Option<CmdMsg> {
        let mut recv_buffer = self.recv_buffer[src].lock().await;
        let cmd = recv_buffer[0].clone();
        if cmd.check_hash() {
            match cmd.cmd {
                Cmd::Clear => None,
                // Cmd::Free => panic!("should not see free in recv buffer"),
                Cmd::Tx | Cmd::Print | Cmd::Free | Cmd::Release => {
                    if cmd.daddr == 0 {
                        return None;
                    }
                    debug!("received  cmd:{:?} from pe {} -- [{:?}]", cmd.cmd, src, cmd);
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

    #[tracing::instrument(skip_all, level = "debug")]
    async fn check_alloc(&self, print: bool) {
        if let Ok(_) =
            self.pending_alloc
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        {
            if print {
                debug!("checking alloc buffer");
            }
            let mut do_alloc = false;
            let mut min_size = 0;
            for pe in 0..self.num_pes {
                if let Some(alloc_buf) = self.alloc_buffer[pe].try_lock() {
                    if alloc_buf[0].check_hash() && alloc_buf[0].cmd == Cmd::Alloc {
                        info!("pe {} (allegedly) needs alloc {:?}", pe, alloc_buf[0]);
                        do_alloc = true;
                        min_size = std::cmp::max(min_size, alloc_buf[0].dsize);
                        // break;
                    }
                }
            }
            if do_alloc {
                println!(
                    "need to alloc new pool {:?}",
                    std::backtrace::Backtrace::capture()
                );
                self.send_alloc_inner(min_size).await;
            }
            self.pending_alloc.store(false, Ordering::SeqCst);
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn check_panic(&self) -> bool {
        if let Some(panic_buf) = self.panic_buffer.try_lock() {
            let mut paniced = false;
            for pe in 0..self.num_pes {
                if panic_buf[pe].cmd != Cmd::Clear {
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

    #[tracing::instrument(skip_all, level = "debug")]
    async fn try_sending_buffer(&self, dst: usize, cmd_buffer: &mut CmdMsgBuffer) -> bool {
        if self.pending_cmds.load(Ordering::SeqCst) == 0 || cmd_buffer.full_bufs.len() > 0 {
            let mut send_buf = self.send_buffer[dst].lock().await;
            if send_buf[0].hash() == self.clear_cmd.hash() {
                cmd_buffer.flush_buffer(&mut send_buf[0]);
                if send_buf[0].dsize > 0 {
                    let recv_buffer = self.recv_buffer[self.my_pe].lock().await;
                    debug! {"sending data to dst({dst}) {:x}    sending cmd {:?} {:?} {:?}",
                    recv_buffer.index_addr(0),send_buf, send_buf[0],send_buf[0].as_bytes()};

                    let _ = recv_buffer.put_unmanaged::<CmdMsg>(
                        // &self.scheduler,
                        // vec![],
                        send_buf[0], //send_buf.sub_slice(dst..=dst),
                        dst,
                        0,
                    );
                    // .spawn();
                    self.put_amt
                        .fetch_add(send_buf[0].as_bytes().len(), Ordering::Relaxed);
                }
                true
            } else {
                // debug!(
                //     "unable to send buffer to dst({dst}) hash not clear {:?}",
                //     send_buf[dst]
                // );
                false
            }
        } else {
            // debug!("unable to send buffer to dst({dst}) pending cmds > 0 or no full bufs");
            false
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    async fn progress_transfers(&self, dst: usize, cmd_buffer: &mut CmdMsgBuffer) {
        let mut send_buf = self.send_buffer[dst].lock().await;
        if cmd_buffer.check_transfer(send_buf[0].daddr) {
            send_buf[0].daddr = 0;
            send_buf[0].dsize = 0;
            send_buf[0].cmd = Cmd::Clear;
            send_buf[0].msg_hash = 0;
            send_buf[0].calc_hash();
        }
        cmd_buffer.check_free_data(&self.comm);
    }

    //TODO maybe undo async functions
    #[tracing::instrument(skip_all, level = "debug")]
    async fn send(&self, data: CommSlice<u8>, dst: usize, hash: usize) {
        trace!("want to send data {:?} {:?} {:?}", data, dst, hash);
        // let mut timer = std::time::Instant::now();
        self.pending_cmds.fetch_add(1, Ordering::SeqCst);
        while self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8 {
            {
                //this is to tell the compiler we wont hold the mutex lock if we have to yield
                // let span = trace_span!("send loop 1");
                // let _guard = span.enter();

                // let mut cmd_buffer = trace_span!("lock").in_scope(|| self.cmd_buffers[dst].lock());
                let mut cmd_buffer = self.cmd_buffers[dst].lock().await;
                if cmd_buffer.try_push(data.clone(), hash) {
                    self.sent_cnt.fetch_add(1, Ordering::SeqCst);
                    self.put_amt.fetch_add(data.len(), Ordering::Relaxed);
                    let _cnt = self.pending_cmds.fetch_sub(1, Ordering::SeqCst);
                    break;
                }
                // let span1 = trace_span!("send loop 1.1");
                // let _guard1 = span1.enter();
                //while we are waiting to push our data might as well try to advance the buffers
                self.progress_transfers(dst, &mut cmd_buffer).await;
                self.try_sending_buffer(dst, &mut cmd_buffer).await;
            }
            async_std::task::yield_now().await;
        }
        let mut im_waiting = false;
        while self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8 {
            {
                // let span = trace_span!("send loop 2");
                // let _guard = span.enter();
                //this is to tell the compiler we wont hold the mutex lock if we have to yield
                let mut cmd_buffer = self.cmd_buffers[dst].lock().await;
                if !cmd_buffer.empty() {
                    //data to send
                    self.progress_transfers(dst, &mut cmd_buffer).await;
                    if self.try_sending_buffer(dst, &mut cmd_buffer).await {
                        self.send_waiting[dst].store(false, Ordering::SeqCst);
                        break;
                    }
                } else {
                    self.send_waiting[dst].store(false, Ordering::SeqCst);
                    break;
                }
                if !im_waiting {
                    if let Ok(_) = self.send_waiting[dst].compare_exchange_weak(
                        false,
                        true,
                        Ordering::SeqCst,
                        Ordering::Relaxed,
                    ) {
                        //ensure only a single task is waiting per destination
                        im_waiting = true;
                    } else {
                        break;
                    }
                }
            }
            async_std::task::yield_now().await;
        }
        // self.active_cnt.fetch_sub(1, Ordering::SeqCst);
    }

    //TODO make this async, and have an atomic variable to check if an alloc is already in progress
    // because we are probably deadlocking on this lock...
    #[tracing::instrument(skip_all, level = "debug")]
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

    #[tracing::instrument(skip_all, level = "debug")]
    fn send_panic(&self) {
        let mut panic_buf = self.panic_buffer.lock_blocking();
        self.send_panic_inner(&mut panic_buf);
    }

    // need to include  a "barrier" count...
    #[tracing::instrument(skip_all, level = "debug")]
    fn send_alloc_inner(&self, min_size: usize) {
        debug!("in send_alloc_inner");
        let mut new_alloc = true;
        while new_alloc {
            new_alloc = false;
            let alloc_id = self.alloc_id.fetch_add(1, Ordering::SeqCst);
            //scoping for the lock

            let mut my_alloc_buf = self.alloc_buffer[self.my_pe].lock().await;
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
                    let alloc_buf = self.alloc_buffer[pe].lock().await;
                    while !alloc_buf[0].check_hash() || alloc_buf[0].cmd != Cmd::Alloc {
                        self.comm.flush();
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
                    }
                } else {
                    let alloc_buf = self.alloc_buffer[pe].lock().await;
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
                // println!(" pe {:?} has alloced", pe);
            }
            info!("created new alloc pool");
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
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
            self.comm.wait();
            // join_all(txs).await;
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn send_release(&self, dst: usize, cmd: CmdMsg) {
        // let cmd_buffer = self.cmd_buffers[dst].lock();
        debug!(
            "sending release to dst[{dst}]: {:?} cmd: {:?} {:?} {:?} 0x{:x} 0x{:x}",
            self.release_cmd,
            cmd,
            self.release_cmd.cmd_as_bytes(),
            cmd.cmd_as_bytes(),
            self.release_cmd.as_addr(),
            cmd.daddr + offset_of!(CmdMsg, cmd)
        );
        // let local_daddr = self.comm.local_addr(dst, cmd.daddr);
        let (local_daddr_alloc, offset) =
            self.comm.local_alloc_and_offset_from_addr(dst, cmd.daddr);

        let local_daddr_slice =
            local_daddr_alloc.comm_slice_at_byte_offset::<Cmd>(offset + offset_of!(CmdMsg, cmd), 1);
        local_daddr_slice
            .put::<Cmd>(&self.scheduler, vec![], self.release_cmd.cmd, dst, 0)
            .block();
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn send_free(&self, dst: usize, cmd: CmdMsg) {
        debug!(
            "sending free to dst[{dst}]: {:?} cmd: {:?} free_cmd_bytes {:?} cmd.cmd_as_bytes: {:?} free_cmd.as_addr 0x{:x} cmd.daddr+offset of(cmf) 0x{:x}",
            self.free_cmd,
            cmd,
            self.free_cmd.cmd_as_bytes(),
            cmd.cmd_as_bytes(),
            self.free_cmd.as_addr(),
            cmd.daddr + offset_of!(CmdMsg, cmd)
        );
        // let local_daddr = self.comm.local_addr(dst, cmd.daddr);
        let (local_daddr_alloc, offset) =
            self.comm.local_alloc_and_offset_from_addr(dst, cmd.daddr);
        // let _ = self
        //     .comm
        //     .put(
        //         &self.scheduler,
        //         vec![],
        //         dst,
        //         self.free_cmd.cmd_as_comm_slice(),
        //         local_daddr + offset_of!(CmdMsg, cmd),
        //     )
        //     .spawn();
        let local_daddr_slice =
            local_daddr_alloc.comm_slice_at_byte_offset::<Cmd>(offset + offset_of!(CmdMsg, cmd), 1);
        let _ = local_daddr_slice.put_unmanaged::<Cmd>(
            // &self.scheduler, vec![],
            self.free_cmd.cmd,
            dst,
            0,
        );
        // .spawn();
    }

    #[tracing::instrument(skip_all, level = "debug")]
    async fn send_print(&self, dst: usize, cmd: CmdMsg) {
        let mut timer = std::time::Instant::now();
        while self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8 {
            {
                let mut send_buf = self.send_buffer[dst].lock().await;
                if send_buf[0].hash() == self.clear_cmd.hash() {
                    send_buf[0].daddr = cmd.daddr;
                    send_buf[0].dsize = cmd.dsize;
                    send_buf[0].cmd = Cmd::Print;
                    send_buf[0].msg_hash = cmd.msg_hash;
                    send_buf[0].calc_hash();
                    let recv_buffer = self.recv_buffer[self.my_pe].lock().await;
                    // trace!("sending cmd {:?}", send_buf);
                    debug!("sending print to {dst}");
                    recv_buffer
                        .put::<CmdMsg>(
                            &self.scheduler,
                            vec![],
                            send_buf[0], // send_buf.sub_slice(dst..=dst),
                            dst,
                            0,
                        )
                        .await;
                    break;
                }
            }
            async_std::task::yield_now().await;
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    async fn check_transfers(&self, src: usize) {
        let mut cmd_buffer = self.cmd_buffers[src].lock().await;
        self.progress_transfers(src, &mut cmd_buffer).await;
    }

    #[tracing::instrument(skip_all, level = "debug")]
    async fn print_data(&self, src: usize, cmd: CmdMsg) {
        let cmd_buffer = self.cmd_buffers[src].lock().await;
        println!("print data -- cmd {:?}", cmd);
        println!("print data -- cmd_buffer {:?}", cmd_buffer);
    }

    //update cmdbuffers to include a hash the wait on that here
    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_data(&self, src: usize, cmd: CmdMsg) -> Vec<CmdMsg> {
        let (local_daddr_alloc, offset) =
            self.comm.local_alloc_and_offset_from_addr(src, cmd.daddr);
        debug!(
            "command queue getting data from {src}, {:x} local_alloc: {:?} offset ({:?}, {:x})",
            cmd.daddr, local_daddr_alloc, offset, offset
        );

        // let mut data = self
        //     .comm
        //     .rt_alloc(cmd.dsize as usize, std::mem::align_of::<CmdMsg>());
        // let mut timer = std::time::Instant::now();
        // while data.is_err() && self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8 {
        //     async_std::task::yield_now().await;
        //     if timer.elapsed().as_secs_f64() > config().deadlock_warning_timeout {
        //         println!("get cmd buf stuck waiting for alloc");
        //         timer = std::time::Instant::now();
        //     }
        //     self.send_alloc(cmd.dsize).await;
        //     data = self
        //         .comm
        //         .rt_alloc(cmd.dsize as usize, std::mem::align_of::<CmdMsg>());
        // }
        // let data = data.expect("failed to alloc data in get_data");
        let mut data = Vec::<CmdMsg>::with_capacity(cmd.dsize / std::mem::size_of::<CmdMsg>());
        unsafe {
            data.set_len(cmd.dsize / std::mem::size_of::<CmdMsg>());
        }

        let remote_cmd_buffer = local_daddr_alloc
            .comm_slice_at_byte_offset::<CmdMsg>(offset, cmd.dsize / std::mem::size_of::<CmdMsg>());

        let data = remote_cmd_buffer
            .get_buffer::<CmdMsg>(
                &self.scheduler,
                vec![],
                src,
                0,
                cmd.dsize / std::mem::size_of::<CmdMsg>(),
            )
            .block(); //maybe we do a block here?
        let mut timer = std::time::Instant::now();
        while calc_hash(
            unsafe { data.as_ptr() } as usize,
            data.len() * std::mem::size_of::<CmdMsg>(),
        ) != cmd.msg_hash
        {
            if timer.elapsed().as_secs_f64() > config().deadlock_warning_timeout {
                debug!(
                    "data hash mismatch from {:?}!!! {:?} {:?} {:?} -- calced hash {:x} expected {:x}",
                    src,
                    cmd,
                    data.len()*std::mem::size_of::<CmdMsg>(),
                    unsafe{ std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len()*std::mem::size_of::<CmdMsg>()) },
                    calc_hash(
                        unsafe { data.as_ptr()} as usize,
                        data.len()*std::mem::size_of::<CmdMsg>()
                    ),
                    cmd.msg_hash
                );
                // self.send_print(src, cmd).await;
                timer = std::time::Instant::now();
            }
            async_std::task::yield_now().await;
        }

        self.send_release(src, cmd);
        // let msg_data = data.as_comm_slice().to_vec();
        // self.comm.rt_free(data);
        data
    }

    #[tracing::instrument(skip_all, level = "debug")]
    async fn get_serialized_data(&self, src: usize, cmd: CmdMsg, ser_data: &mut SerializedData) {
        let len = ser_data.len();
        let data_slice = ser_data.header_and_data_as_bytes_mut();
        trace!("get_serialized_data {:?} {:?} {:x}", src, cmd, cmd.daddr);
        let (local_daddr_alloc, offset) =
            self.comm.local_alloc_and_offset_from_addr(src, cmd.daddr);
        local_daddr_alloc.get_into_buffer_unmanaged(src, offset, unsafe {
            LamellarBuffer::<u8, CommSlice<u8>>::from_comm_slice(data_slice.clone())
        });
        self.comm.wait();
        let mut timer = std::time::Instant::now();
        trace!(
            "calced hash: {:x} cmd msg hash {:x}",
            calc_hash(data_slice.as_ptr() as usize, len),
            cmd.msg_hash
        );

        while calc_hash(data_slice.as_ptr() as usize, len) != cmd.msg_hash
            && self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8
        {
            async_std::task::yield_now().await;
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    async fn get_cmd(&self, src: usize, cmd: CmdMsg) -> SerializedData {
        trace!("getting cmd from {}", src);
        let mut ser_data = self.comm.new_serialized_data(cmd.dsize as usize);
        let mut timer = std::time::Instant::now();
        // let mut print = false;
        while ser_data.is_err() && self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8 {
            async_std::task::yield_now().await;
            self.send_alloc(cmd.dsize).await;
            ser_data = self.comm.new_serialized_data(cmd.dsize as usize);
        }
        let mut ser_data = ser_data.unwrap();
        self.get_serialized_data(src, cmd, &mut ser_data).await;
        self.recv_cnt.fetch_add(1, Ordering::SeqCst);
        ser_data
    }

    #[tracing::instrument(skip_all, level = "debug")]
    async fn get_cmd_buf(&self, src: usize, cmd: CmdMsg) -> Vec<CmdMsg> {
        trace!("getting cmd buf from {}", src);
        let data = self.get_data(src, cmd).await;
        data
    }
}

impl Drop for InnerCQ {
    #[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
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
        // println!("dropped InnerCQ");
    }
}

pub(crate) struct CommandQueue {
    cq: Arc<InnerCQ>,
    send_buffer: CommAlloc,
    recv_buffer: CommAlloc,
    alloc_buffer: CommAlloc,
    panic_buffer: CommAlloc,
    release_cmd: CommAlloc,
    clear_cmd: CommAlloc,
    free_cmd: CommAlloc,
    cmd_buffers: Vec<Arc<Vec<CommAlloc>>>,
    comm: Arc<Comm>,
    scheduler: Arc<Scheduler>,
    active: Arc<AtomicU8>,
}

impl CommandQueue {
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new(
        comm: Arc<Comm>,
        scheduler: Arc<Scheduler>,
        my_pe: usize,
        num_pes: usize,
        active: Arc<AtomicU8>,
    ) -> CommandQueue {
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
        trace!("release_cmd {:?}", release_cmd);

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
        CommandQueue {
            cq: Arc::new(cq),
            send_buffer,
            recv_buffer,
            alloc_buffer,
            panic_buffer,
            release_cmd,
            clear_cmd,
            free_cmd,
            cmd_buffers,
            comm,
            scheduler,
            active,
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) async fn send_alloc(&self, min_size: usize) {
        self.cq.send_alloc(min_size).await;
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn send_panic(&self) {
        self.cq.send_panic();
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) async fn send_data(&self, data: RemoteSerializedData, dst: usize) {
        let hash = calc_hash(data.ser_data_bytes.usize_addr(), data.len());
        trace!("sending data: {:?} {:?} {}", data, dst, hash);
        // trace!("bytes: {:?}", data.ser_data_bytes.as_slice());

        // println!("send_data: {:?} {:?} {:?}",data.relative_addr,data.len,hash);
        data.increment_cnt(); //or we could implement something like an into_raw here...
                              // println!("sending data {:?}",data.header_and_data_as_bytes());
        self.cq.send(data.ser_data_bytes.clone(), dst, hash).await;
        //     }
        //     _ => {}
        // }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) async fn alloc_task(&self) {
        let mut timer = std::time::Instant::now();
        let mut print = false;
        while self.scheduler.active()
            && self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8
        {
            if timer.elapsed().as_secs_f64() > 1.0 {
                debug!("alloc_task still running");
                timer = std::time::Instant::now();
                print = true;
            }
            self.cq.check_alloc(print).await;
            print = false;
            async_std::task::yield_now().await;
        }
        // println!("leaving alloc_task task {:?}", scheduler.active());
        // println!("sechduler_new: {:?}", Arc::strong_count(&scheduler));
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) async fn panic_task(&self) {
        let mut panic = false;
        while self.scheduler.active() && !panic {
            panic = self.cq.check_panic();
            async_std::task::yield_now().await;
        }

        if panic {
            warn!("received panic from other PE");
            // scheduler.force_shutdown();
            panic!("received panic from other PE");
        }
        // println!("leaving panic_task task {:?}", scheduler.active());
        // println!("sechduler_new: {:?}", Arc::strong_count(&scheduler));
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) async fn recv_data(&self, lamellae: Arc<Lamellae>) {
        let comm = lamellae.comm();
        let num_pes = comm.num_pes();
        let my_pe = comm.my_pe();
        // let mut timer= std::time::Instant::now();
        while self.active.load(Ordering::SeqCst) == CmdQStatus::Active as u8
            || !self.cq.empty().await
            || self.scheduler.active()
        {
            // CNTS.get_or(|| AtomicUsize::new(0))
            //     .fetch_add(1, Ordering::Relaxed);
            for src in 0..num_pes {
                if src != my_pe {
                    if let Some(cmd_buf_cmd) = self.cq.ready(src).await {
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
                            // self.cq.free_data(src,cmd_buf_cmd.daddr as usize),
                            Cmd::Print => {
                                self.cq.print_data(src, cmd_buf_cmd).await;
                                self.cq.send_free(src, cmd_buf_cmd);
                            }
                            Cmd::Tx => {
                                trace!("got tx");
                                let cq = self.cq.clone();
                                let lamellae = lamellae.clone();
                                let scheduler1 = self.scheduler.clone();
                                // let comm = self.comm.clone();
                                let task = async move {
                                    trace!("going to get cmd_buf {:?} from {:?}", cmd_buf_cmd, src);
                                    let data = cq.get_cmd_buf(src, cmd_buf_cmd).await;
                                    // let cmd_buf = data.as_slice();
                                    // let cmd_buf = unsafe {
                                    //     std::slice::from_raw_parts(
                                    //         data as *const CmdMsg,
                                    //         cmd_buf_cmd.dsize as usize
                                    //             / std::mem::size_of::<CmdMsg>(),
                                    //     )
                                    // };
                                    // println!("cmd_buf {:?}", cmd_buf);
                                    let mut i = 0;
                                    let len = data.len();
                                    let cmd_cnt = Arc::new(AtomicUsize::new(len));
                                    // println!("src: {:?} cmd_buf len {:?}", src, len);

                                    for cmd in data.into_iter() {
                                        if cmd.dsize != 0 {
                                            let cq = cq.clone();
                                            let lamellae = lamellae.clone();
                                            let scheduler2 = scheduler1.clone();
                                            // cmd_cnt.fetch_add(1,Ordering::SeqCst);
                                            let cmd_cnt_clone = cmd_cnt.clone();
                                            let task = async move {
                                                trace!("getting cmd {:?} [{:?}/{:?}]", cmd, i, len);
                                                let work_data = cq.get_cmd(src, cmd).await;

                                                // println!(
                                                //     "[{:?}] recv_data submitting work",
                                                //     std::thread::current().id(),
                                                // );
                                                scheduler2
                                                    .submit_remote_am(work_data, lamellae.clone());
                                                if cmd_cnt_clone.fetch_sub(1, Ordering::SeqCst) == 1
                                                {
                                                    cq.send_free(src, cmd_buf_cmd);
                                                    // println!(
                                                    //     "sending clear cmd {:?} [{:?}/{:?}]",
                                                    //     cmd_buf_cmd.daddr, i, len
                                                    // );
                                                }
                                            };
                                            // println!(
                                            //     "[{:?}] recv_data submitting get command task",
                                            //     std::thread::current().id(),
                                            // );
                                            scheduler1.submit_io_task(task);
                                            i += 1;
                                        } else {
                                            panic!(
                                                "should not be here! {:?} -- {:?} [{:?}/{:?}]",
                                                cmd_buf_cmd, cmd, i, len
                                            );
                                        }
                                    }

                                    // comm.rt_free(data);
                                };
                                // println!(
                                //     "[{:?}] recv_data submitting tx task",
                                //     std::thread::current().id()
                                // );
                                self.scheduler.submit_io_task(task);
                            }
                        }
                    }
                    self.cq.check_transfers(src).await;
                    // self.cq.check_buffers(src);//process any finished buffers
                }
            }
            // if timer.elapsed().as_secs_f64() > 30.0{
            //     println!("waiting for requests");
            //     self.cq.print_recv_buffers();
            //     timer = std::time::Instant::now();
            // }
            // self.comm.process_dropped_reqs();
            // if active.load(Ordering::SeqCst) != 1 {
            //     println!(
            //         "command queue sched active? {:?}  {:?}",
            //         scheduler.active(),
            //         self.cq.empty()
            //     );
            // }
            comm.flush();
            async_std::task::yield_now().await;
        }
        // println!(
        //     "[{:?}] leaving recv_data task {:?}",
        //     std::thread::current().id(),
        //     scheduler.active()
        // );
        self.active
            .store(CmdQStatus::Finished as u8, Ordering::SeqCst);
        // println!("recv_data thread shutting down");
        // for cnt in CNTS.iter() {
        //     print!("{:?} ", cnt.load(Ordering::Relaxed));
        // }
        // println!("");
        // println!("sechduler_new: {:?}", Arc::strong_count(&scheduler));
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn tx_amount(&self) -> usize {
        // println!("cq put: {:?} get {:?}",self.cq.put_amt.load(Ordering::SeqCst) ,self.cq.get_amt.load(Ordering::SeqCst));
        self.cq.put_amt.load(Ordering::SeqCst) + self.cq.get_amt.load(Ordering::SeqCst)
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn mem_per_pe() -> usize {
        (config().cmd_buf_len * config().cmd_buf_cnt + 4) * std::mem::size_of::<CmdMsg>()
    }
}

impl Drop for CommandQueue {
    #[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
        // println!("dropping rofi command queue");
        self.comm.rt_free(self.send_buffer.clone());
        self.comm.rt_free(self.recv_buffer.clone());
        self.comm.rt_free(self.alloc_buffer.clone());
        self.comm.rt_free(self.panic_buffer.clone());
        self.comm.rt_free(self.release_cmd.clone());
        self.comm.rt_free(self.clear_cmd.clone());
        self.comm.rt_free(self.free_cmd.clone());
        for bufs in self.cmd_buffers.iter() {
            for buf in bufs.iter() {
                self.comm.rt_free(buf.clone());
            }
        }
        // println!("rofi command queue dropped");
    }
}

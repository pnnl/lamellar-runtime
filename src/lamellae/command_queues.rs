use super::{
    comm::{CmdQStatus, CommMem, CommProgress, CommRdma, CommInfo},
    Comm, Lamellae, SerializedData,RemoteSerializedData,
};
use crate::{env_var::config, scheduler::Scheduler};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::num::Wrapping;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

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
    //#[tracing::instrument(skip_all)]
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
    Print,
    Tx,
    Alloc,
    Panic,
}

//#[tracing::instrument(skip_all)]
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
    //#[tracing::instrument(skip_all)]
    fn as_bytes(&self) -> &[u8] {
        let pointer = self as *const Self as *const u8;
        let size = std::mem::size_of::<Self>();
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(pointer, size) };
        slice
    }
    //#[tracing::instrument(skip_all)]
    fn as_addr(&self) -> usize {
        self as *const CmdMsg as usize
    }
    //#[tracing::instrument(skip_all)]
    fn cmd_as_bytes(&self) -> &[u8] {
        let pointer = &self.cmd as *const Cmd as *const u8;
        let size = std::mem::size_of::<Cmd>();
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(pointer, size) };
        slice
    }
    //#[tracing::instrument(skip_all)]
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
    //#[tracing::instrument(skip_all)]
    fn calc_hash(&mut self) {
        self.cmd_hash = self.hash()
    }
    //#[tracing::instrument(skip_all)]
    fn check_hash(&self) -> bool {
        if self.cmd_hash == self.hash() && self.cmd_hash != 0 {
            true
        } else {
            false
        }
    }
}

impl std::fmt::Debug for CmdMsg {
    //#[tracing::instrument(skip_all)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "daddr {:#x}({:?}) dsize {:?} cmd {:?} msg_hash {:?} cmd_hash {:?}",
            self.daddr, self.daddr, self.dsize, self.cmd, self.msg_hash, self.cmd_hash,
        )
    }
}

struct CmdBuf {
    buf: Box<[CmdMsg]>,
    addr: usize,
    // base_addr: usize,
    index: usize,
    allocated_cnt: usize,
    max_size: usize,
}

impl CmdBuf {
    //#[tracing::instrument(skip_all)]
    fn push(&mut self, daddr: usize, dsize: usize, hash: usize) {
        if daddr == 0 || dsize == 0 {
            panic!("this shouldnt happen! {:?} {:?}", daddr, dsize);
        }
        let cmd = unsafe { self.buf.get_unchecked_mut(self.index) };
        cmd.daddr = daddr;
        cmd.dsize = dsize;
        cmd.cmd = Cmd::Tx;
        cmd.msg_hash = hash;
        cmd.calc_hash();
        self.index += 1;
        if dsize > 0 {
            self.allocated_cnt += 1;
        }
    }
    //#[tracing::instrument(skip_all)]
    fn full(&self) -> bool {
        self.index == self.max_size
    }
    //#[tracing::instrument(skip_all)]
    fn reset(&mut self) {
        self.index = 0;
        self.allocated_cnt = 0;
    }
    //#[tracing::instrument(skip_all)]
    fn addr(&self) -> usize {
        self.addr
    }
    //#[tracing::instrument(skip_all)]
    fn size(&self) -> usize {
        self.index * std::mem::size_of::<CmdMsg>()
    }
    //#[tracing::instrument(skip_all)]
    fn iter(&self) -> std::slice::Iter<CmdMsg> {
        self.buf[0..self.index].iter()
    }
    //#[tracing::instrument(skip_all)]
    fn state(&self) -> Cmd {
        self.buf[0].cmd
    }
}

impl std::fmt::Debug for CmdBuf {
    //#[tracing::instrument(skip_all)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "addr {:#x}({:?}) hash {:?} index {:?} a_cnt {:?}",
            self.addr,
            self.addr,
            // calc_hash(self.addr + self.base_addr, self.size()),
            calc_hash(self.addr, self.size()),
            self.index,
            self.allocated_cnt
        )
    }
}

impl Drop for CmdBuf {
    //#[tracing::instrument(skip_all)]
    fn drop(&mut self) {
        // println!("dropping cmd buf");
        let old = std::mem::take(&mut self.buf);
        let _ = Box::into_raw(old);
        // println!("dropped cmd buf");
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
    // base_addr: usize,
}

impl CmdMsgBuffer {
    // fn new(addrs: Arc<Vec<usize>>, base_addr: usize, pe: usize) -> CmdMsgBuffer {
    //#[tracing::instrument(skip_all)]
    fn new(addrs: Arc<Vec<usize>>, pe: usize) -> CmdMsgBuffer {
        let mut bufs = vec![];
        for addr in addrs.iter() {
            // println!("CmdMsgBuffer {:x} {:?}",addr,addr);
            bufs.push(CmdBuf {
                buf: unsafe {
                    Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                        // (*addr + base_addr) as *mut CmdMsg,
                        *addr as *mut CmdMsg,
                        config().cmd_buf_len,
                    ))
                },
                addr: *addr,
                // base_addr: base_addr,
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
            // base_addr: base_addr,
        }
    }
    //#[tracing::instrument(skip_all)]
    fn try_push(&mut self, addr: usize, len: usize, hash: usize) -> bool {
        if self.cur_buf.is_none() {
            self.cur_buf = self.empty_bufs.pop();
        }
        if let Some(buf) = &mut self.cur_buf {
            buf.push(addr, len, hash);
            if buf.full() {
                self.full_bufs
                    .push(std::mem::replace(&mut self.cur_buf, None).unwrap());
            }
            true
        } else {
            false
        }
    }
    //#[tracing::instrument(skip_all)]
    fn flush_buffer(&mut self, cmd: &mut CmdMsg) {
        // println!("flush buffer before: {:?}",self);
        if let Some(buf) = self.full_bufs.pop() {
            // println!("flushing {:?} {:?}",buf.addr(),buf.size());
            cmd.daddr = buf.addr();
            cmd.dsize = buf.size();
            cmd.cmd = Cmd::Tx;
            // cmd.msg_hash = calc_hash(cmd.daddr + self.base_addr, cmd.dsize);
            cmd.msg_hash = calc_hash(cmd.daddr, cmd.dsize);

            cmd.calc_hash();
            self.tx_bufs.insert(buf.addr(), buf);
        } else if let Some(buf) = std::mem::replace(&mut self.cur_buf, None) {
            // println!("flushing {:?} {:?}",buf.addr(),buf.size());
            cmd.daddr = buf.addr();
            cmd.dsize = buf.size(); // + std::mem::size_of::<usize>();
            cmd.cmd = Cmd::Tx;
            // cmd.msg_hash = calc_hash(cmd.daddr + self.base_addr, cmd.dsize);
            cmd.msg_hash = calc_hash(cmd.daddr, cmd.dsize);
            cmd.calc_hash();
            self.tx_bufs.insert(buf.addr(), buf);
        }
        // else {} all buffers are currently busy
        // println!("flush buffer after: {:?}",self);
    }

    //#[tracing::instrument(skip_all)]
    fn check_transfer(&mut self, addr: usize) -> bool {
        if let Some(buf) = self.tx_bufs.remove(&addr) {
            match buf.state() {
                Cmd::Tx => {
                    self.tx_bufs.insert(addr, buf);
                    false
                }
                Cmd::Release | Cmd::Free => {
                    //free is valid still but we need to make sure we release the send_buffer
                    // println!("transfer complete!!");
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

    //#[tracing::instrument(skip_all)]
    fn check_free_data(&mut self, comm: Arc<Comm>) {
        let mut freed_bufs = vec![];

        for (addr, buf) in self.waiting_bufs.iter() {
            match buf.state() {
                Cmd::Release => {} //do nothing
                Cmd::Free => freed_bufs.push(*addr),
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
                        let ser_data_addr = cmd.daddr as usize - std::mem::size_of::<AtomicUsize>();
                        unsafe { SerializedData::from_raw(ser_data_addr) }; //creates and then drops to decrement the reference count
                    }
                }
                buf.reset();
                self.empty_bufs.push(buf);
            } else {
                println!("free_data should this be possible?");
            }
        }
    }

    //#[tracing::instrument(skip_all)]
    fn empty(&self) -> bool {
        self.empty_bufs.len() == self.num_bufs
    }
}

impl std::fmt::Debug for CmdMsgBuffer {
    //#[tracing::instrument(skip_all)]
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
    //#[tracing::instrument(skip_all)]
    fn drop(&mut self) {
        // println!("dropping CmdMsgBuffer");
        while !self.empty() {
            std::thread::yield_now()
        }
        self.empty_bufs.clear();
        // println!("dropped CmdMsgBuffer");
    }
}

struct InnerCQ {
    send_buffer: Arc<Mutex<Box<[CmdMsg]>>>,
    recv_buffer: Arc<Mutex<Box<[CmdMsg]>>>,
    free_buffer: Arc<Mutex<Box<[CmdMsg]>>>,
    alloc_buffer: Arc<Mutex<Box<[CmdMsg]>>>,
    panic_buffer: Arc<Mutex<Box<[CmdMsg]>>>,
    cmd_buffers: Vec<Mutex<CmdMsgBuffer>>,
    release_cmd: Arc<Box<CmdMsg>>,
    clear_cmd: Arc<Box<CmdMsg>>,
    free_cmd: Arc<Box<CmdMsg>>,
    comm: Arc<Comm>,
    my_pe: usize,
    num_pes: usize,
    send_waiting: Vec<Arc<AtomicBool>>,
    pending_cmds: Arc<AtomicUsize>,
    _active_cnt: Arc<AtomicUsize>,
    sent_cnt: Arc<AtomicUsize>,
    recv_cnt: Arc<AtomicUsize>,
    put_amt: Arc<AtomicUsize>,
    get_amt: Arc<AtomicUsize>,
    alloc_id: Arc<AtomicUsize>,
    active: Arc<AtomicU8>,
}

impl InnerCQ {
    //#[tracing::instrument(skip_all)]
    fn new(
        send_buffer_addr: usize,
        recv_buffer_addr: usize,
        free_buffer_addr: usize,
        alloc_buffer_addr: usize,
        panic_buffer_addr: usize,
        cmd_buffers_addrs: &Vec<Arc<Vec<usize>>>,
        release_cmd_addr: usize,
        clear_cmd_addr: usize,
        free_cmd_addr: usize,
        comm: Arc<Comm>,
        my_pe: usize,
        num_pes: usize,
        active: Arc<AtomicU8>,
    ) -> InnerCQ {
        let mut cmd_buffers = vec![];
        let mut pe = 0;
        for addrs in cmd_buffers_addrs.iter() {
            cmd_buffers.push(Mutex::new(CmdMsgBuffer::new(
                addrs.clone(),
                // comm.base_addr(),
                pe,
            )));
            pe += 1;
        }
        let mut send_buffer = unsafe {
            Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                send_buffer_addr as *mut CmdMsg,
                num_pes,
            ))
        };
        for cmd in send_buffer.iter_mut() {
            (*cmd).daddr = 0;
            (*cmd).dsize = 0;
            (*cmd).cmd = Cmd::Clear;
            (*cmd).msg_hash = 0;
            (*cmd).calc_hash();
        }

        let mut recv_buffer = unsafe {
            Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                recv_buffer_addr as *mut CmdMsg,
                num_pes,
            ))
        };
        for cmd in recv_buffer.iter_mut() {
            (*cmd).daddr = 0;
            (*cmd).dsize = 0;
            (*cmd).cmd = Cmd::Clear;
            (*cmd).msg_hash = 0;
            (*cmd).calc_hash();
        }

        let mut free_buffer = unsafe {
            Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                free_buffer_addr as *mut CmdMsg,
                num_pes,
            ))
        };
        for cmd in free_buffer.iter_mut() {
            (*cmd).daddr = 0;
            (*cmd).dsize = 0;
            (*cmd).cmd = Cmd::Clear;
            (*cmd).msg_hash = 0;
            (*cmd).calc_hash();
        }

        let mut alloc_buffer = unsafe {
            Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                alloc_buffer_addr as *mut CmdMsg,
                num_pes,
            ))
        };
        for cmd in alloc_buffer.iter_mut() {
            (*cmd).daddr = 0;
            (*cmd).dsize = 0;
            (*cmd).cmd = Cmd::Clear;
            (*cmd).msg_hash = 0;
            (*cmd).calc_hash();
        }

        let mut panic_buffer = unsafe {
            Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                panic_buffer_addr as *mut CmdMsg,
                num_pes,
            ))
        };
        for cmd in panic_buffer.iter_mut() {
            (*cmd).daddr = 0;
            (*cmd).dsize = 0;
            (*cmd).cmd = Cmd::Clear;
            (*cmd).msg_hash = 0;
            (*cmd).calc_hash();
        }

        let mut release_cmd = unsafe { Box::from_raw(release_cmd_addr as *mut CmdMsg) };
        release_cmd.daddr = 1;
        release_cmd.dsize = 1;
        release_cmd.cmd = Cmd::Release;
        release_cmd.msg_hash = 1;
        release_cmd.calc_hash();

        let mut clear_cmd = unsafe { Box::from_raw(clear_cmd_addr as *mut CmdMsg) };
        clear_cmd.daddr = 0;
        clear_cmd.dsize = 0;
        clear_cmd.cmd = Cmd::Clear;
        clear_cmd.msg_hash = 0;
        clear_cmd.calc_hash();

        let mut free_cmd = unsafe { Box::from_raw(free_cmd_addr as *mut CmdMsg) };
        free_cmd.daddr = 0;
        free_cmd.dsize = 0;
        free_cmd.cmd = Cmd::Free;
        free_cmd.msg_hash = 0;
        free_cmd.calc_hash();

        let mut send_waiting = vec![];
        for _pe in 0..num_pes {
            send_waiting.push(Arc::new(AtomicBool::new(false)));
        }
        InnerCQ {
            send_buffer: Arc::new(Mutex::new(send_buffer)),
            recv_buffer: Arc::new(Mutex::new(recv_buffer)),
            free_buffer: Arc::new(Mutex::new(free_buffer)),
            alloc_buffer: Arc::new(Mutex::new(alloc_buffer)),
            panic_buffer: Arc::new(Mutex::new(panic_buffer)),
            cmd_buffers: cmd_buffers,
            release_cmd: Arc::new(release_cmd),
            clear_cmd: Arc::new(clear_cmd),
            free_cmd: Arc::new(free_cmd),
            comm: comm,
            my_pe: my_pe,
            num_pes: num_pes,
            send_waiting: send_waiting,
            pending_cmds: Arc::new(AtomicUsize::new(0)),
            _active_cnt: Arc::new(AtomicUsize::new(0)),
            sent_cnt: Arc::new(AtomicUsize::new(0)),
            recv_cnt: Arc::new(AtomicUsize::new(0)),
            put_amt: Arc::new(AtomicUsize::new(0)),
            get_amt: Arc::new(AtomicUsize::new(0)),
            alloc_id: Arc::new(AtomicUsize::new(0)),
            active: active,
        }
    }

    //#[tracing::instrument(skip_all)]
    fn empty(&self) -> bool {
        for buf in &self.cmd_buffers {
            let buf = buf.lock();
            if !buf.empty() {
                return false;
            }
        }
        true
    }

    //#[tracing::instrument(skip_all)]
    fn ready(&self, src: usize) -> Option<CmdMsg> {
        let mut recv_buffer = self.recv_buffer.lock();
        let cmd = recv_buffer[src].clone();
        if cmd.check_hash() {
            match cmd.cmd {
                Cmd::Clear => None,
                // Cmd::Free => panic!("should not see free in recv buffer"),
                Cmd::Tx | Cmd::Print | Cmd::Free | Cmd::Release => {
                    // println!("Received something {:?}", cmd.cmd);
                    if cmd.daddr == 0 {
                        // println!("received cmd src {:?} {:?} ", src, cmd);
                        return None;
                    }
                    let res = Some(cmd.clone());
                    let cmd = &mut recv_buffer[src];
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

    //#[tracing::instrument(skip_all)]
    fn check_alloc(&self) {
        if let Some(mut alloc_buf) = self.alloc_buffer.try_lock() {
            let mut do_alloc = false;
            let mut min_size = 0;
            for pe in 0..self.num_pes {
                if alloc_buf[pe].check_hash() && alloc_buf[pe].cmd == Cmd::Alloc {
                    do_alloc = true;
                    min_size = alloc_buf[pe].dsize;
                    break;
                }
            }
            if do_alloc {
                // println!(
                //     "need to alloc new pool {:?}",
                //     std::backtrace::Backtrace::capture()
                // );
                self.send_alloc_inner(&mut alloc_buf, min_size);
            }
        }
    }

    //#[tracing::instrument(skip_all)]
    fn check_panic(&self) -> bool {
        if let Some(panic_buf) = self.panic_buffer.try_lock() {
            let mut paniced = false;
            for pe in 0..self.num_pes {
                if panic_buf[pe].cmd != Cmd::Clear {
                    println!("panic_buf {:?}", panic_buf[pe]);
                }
                if panic_buf[pe].check_hash() && panic_buf[pe].cmd == Cmd::Panic {
                    println!("panic_buf passed hash check{:?}", panic_buf[pe]);
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

    //#[tracing::instrument(skip_all)]
    fn try_sending_buffer(&self, dst: usize, cmd_buffer: &mut CmdMsgBuffer) -> bool {
        if self.pending_cmds.load(Ordering::SeqCst) == 0 || cmd_buffer.full_bufs.len() > 0 {
            let mut send_buf = self.send_buffer.lock();
            if send_buf[dst].hash() == self.clear_cmd.hash() {
                cmd_buffer.flush_buffer(&mut send_buf[dst]);
                // println!{"send_buf  after{:?}",send_buf[dst]};
                if send_buf[dst].dsize > 0 {
                    let recv_buffer = self.recv_buffer.lock();
                    // println! {"sending data to dst {:?} {:?} {:?} {:?}",recv_buffer[self.my_pe].as_addr()-self.comm.base_addr(),send_buf[dst],send_buf[dst].as_bytes(),send_buf};
                    // println!("sending cmd {:?}", send_buf);
                    // println!("Command Queue sending buffer");
                    self.comm.put(
                        dst,
                        send_buf[dst].as_bytes(),
                        recv_buffer[self.my_pe].as_addr(),
                    );
                    self.put_amt
                        .fetch_add(send_buf[dst].as_bytes().len(), Ordering::Relaxed);
                }
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    //#[tracing::instrument(skip_all)]
    fn progress_transfers(&self, dst: usize, cmd_buffer: &mut CmdMsgBuffer) {
        let mut send_buf = self.send_buffer.lock();
        if cmd_buffer.check_transfer(send_buf[dst].daddr) {
            send_buf[dst].daddr = 0;
            send_buf[dst].dsize = 0;
            send_buf[dst].cmd = Cmd::Clear;
            send_buf[dst].msg_hash = 0;
            send_buf[dst].calc_hash();
        }
        cmd_buffer.check_free_data(self.comm.clone());
    }

    //#[tracing::instrument(skip_all)]
    async fn send(&self, addr: usize, len: usize, dst: usize, hash: usize) {
        let mut timer = std::time::Instant::now();
        self.pending_cmds.fetch_add(1, Ordering::SeqCst);
        while self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8 {
            {
                //this is to tell the compiler we wont hold the mutex lock if we have to yield
                // let span = trace_span!("send loop 1");
                // let _guard = span.enter();

                // let mut cmd_buffer = trace_span!("lock").in_scope(|| self.cmd_buffers[dst].lock());
                let mut cmd_buffer = self.cmd_buffers[dst].lock();
                if cmd_buffer.try_push(addr, len, hash) {
                    self.sent_cnt.fetch_add(1, Ordering::SeqCst);
                    self.put_amt.fetch_add(len, Ordering::Relaxed);
                    let _cnt = self.pending_cmds.fetch_sub(1, Ordering::SeqCst);
                    break;
                }
                // let span1 = trace_span!("send loop 1.1");
                // let _guard1 = span1.enter();
                //while we are waiting to push our data might as well try to advance the buffers
                self.progress_transfers(dst, &mut cmd_buffer);
                self.try_sending_buffer(dst, &mut cmd_buffer);
                // if timer.elapsed().as_secs_f64() > config().deadlock_timeout {
                //     let send_buf = self.send_buffer.lock();
                //     println!("waiting to add cmd to cmd buffer {:?}", cmd_buffer);
                //     println!("send_buf: {:?}", send_buf);
                //     // drop(send_buf);
                //     let recv_buf = self.recv_buffer.lock();
                //     println!("recv_buf: {:?}", recv_buf);
                //     let free_buf = self.free_buffer.lock();
                //     println!("free_buf: {:?}", free_buf);
                //     timer = std::time::Instant::now();
                // }
            }
            async_std::task::yield_now().await;
        }
        let mut im_waiting = false;
        while self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8 {
            {
                // let span = trace_span!("send loop 2");
                // let _guard = span.enter();
                //this is to tell the compiler we wont hold the mutex lock if we have to yield
                let mut cmd_buffer = self.cmd_buffers[dst].lock();
                if !cmd_buffer.empty() {
                    //data to send
                    self.progress_transfers(dst, &mut cmd_buffer);
                    if self.try_sending_buffer(dst, &mut cmd_buffer) {
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
                // if timer.elapsed().as_secs_f64() > config().deadlock_timeout {
                //     println!("waiting to send cmd buffer {:?}", cmd_buffer);
                //     let send_buf = self.send_buffer.lock();
                //     println!("send_buf addr {:?}", send_buf.as_ptr());
                //     println!("send_buf: {:?}", send_buf);
                //     let recv_buf = self.recv_buffer.lock();
                //     println!("recv_buf: {:?}", recv_buf);
                //     let free_buf = self.free_buffer.lock();
                //     println!("free_buf: {:?}", free_buf);
                //     timer = std::time::Instant::now();
                // }
            }
            async_std::task::yield_now().await;
        }
        // self.active_cnt.fetch_sub(1, Ordering::SeqCst);
    }

    //#[tracing::instrument(skip_all)]
    fn send_alloc(&self, min_size: usize) {
        let prev_cnt = self.comm.num_pool_allocs();
        let mut alloc_buf = self.alloc_buffer.lock();
        if !self
            .comm
            .rt_check_alloc(min_size, std::mem::align_of::<CmdMsg>())
        {
            // println!(" {:?} {:?}",prev_cnt,self.comm.num_pool_allocs());
            if prev_cnt == self.comm.num_pool_allocs() {
                // println!("im responsible for the new alloc");
                self.send_alloc_inner(&mut alloc_buf, min_size);
            }
        }
    }

    //#[tracing::instrument(skip_all)]
    fn send_panic(&self) {
        let mut panic_buf = self.panic_buffer.lock();
        self.send_panic_inner(&mut panic_buf);
    }

    // need to include  a "barrier" count...
    //#[tracing::instrument(skip_all)]
    fn send_alloc_inner(&self, alloc_buf: &mut Box<[CmdMsg]>, min_size: usize) {
        let mut new_alloc = true;
        while new_alloc {
            new_alloc = false;
            let alloc_id = self.alloc_id.fetch_add(1, Ordering::SeqCst);
            // need to loop incase a new alloc comes in aftet this one, but recursion and mut& T dont play nicely...
            if alloc_buf[self.my_pe].hash() == self.clear_cmd.hash() {
                let cmd = &mut alloc_buf[self.my_pe];
                cmd.daddr = alloc_id;
                cmd.dsize = min_size;
                cmd.cmd = Cmd::Alloc;
                cmd.msg_hash = 0;
                cmd.calc_hash();
                for pe in 0..self.num_pes {
                    if pe != self.my_pe {
                        // println!("putting alloc cmd to pe {:?}", pe);
                        self.comm.put(pe, cmd.as_bytes(), cmd.as_addr());
                    }
                }
            }
            let mut start = std::time::Instant::now();
            for pe in 0..self.num_pes {
                while !alloc_buf[pe].check_hash() || alloc_buf[pe].cmd != Cmd::Alloc {
                    self.comm.flush();
                    std::thread::yield_now();
                    if start.elapsed().as_secs_f64() > config().deadlock_timeout {
                        // println!("waiting to alloc: {:?} {:?}", alloc_buf, alloc_id);
                        start = std::time::Instant::now();
                    }
                }
                // println!(" pe {:?} ready to alloc {:?} {:?}", pe, alloc_id, min_size);
            }
            // panic!("exiting");

            self.comm.alloc_pool(min_size);
            let cmd = &mut alloc_buf[self.my_pe];
            cmd.daddr = 0;
            cmd.dsize = 0;
            cmd.cmd = Cmd::Clear;
            cmd.msg_hash = 0;
            cmd.calc_hash();
            for pe in 0..self.num_pes {
                if pe != self.my_pe {
                    // println!("putting clear cmd to pe {:?}", pe);
                    self.comm.put(pe, cmd.as_bytes(), cmd.as_addr());
                }
            }
            for pe in 0..self.num_pes {
                while !alloc_buf[pe].check_hash() || alloc_buf[pe].cmd != Cmd::Clear {
                    if alloc_buf[pe].cmd == Cmd::Alloc {
                        if alloc_buf[pe].daddr > alloc_id {
                            new_alloc = true;
                            break;
                        }
                    }
                    std::thread::yield_now();
                }
                // println!(" pe {:?} has alloced", pe);
            }
            // println!("created new alloc pool");
        }
    }

    //#[tracing::instrument(skip_all)]
    fn send_panic_inner(&self, panic_buf: &mut Box<[CmdMsg]>) {
        if panic_buf[self.my_pe].hash() == self.clear_cmd.hash() {
            let cmd = &mut panic_buf[self.my_pe];
            cmd.daddr = 0;
            cmd.dsize = 0;
            cmd.cmd = Cmd::Panic;
            cmd.msg_hash = 0;
            cmd.calc_hash();
            for pe in 0..self.num_pes {
                if pe != self.my_pe {
                    // println!("putting panic cmd to pe {:?} {cmd:?}", pe);
                    self.comm.put(pe, cmd.as_bytes(), cmd.as_addr()); // not sure if we need to make this put incase the other PEs are already down
                }
            }
            self.comm.wait();
            // join_all(txs).await;
        }
    }

    //#[tracing::instrument(skip_all)]
    fn send_release(&self, dst: usize, cmd: CmdMsg) {
        // let cmd_buffer = self.cmd_buffers[dst].lock();
        // println!("sending release: {:?} cmd: {:?} {:?} {:?} 0x{:x} 0x{:x}",self.release_cmd,cmd,self.release_cmd.cmd_as_bytes(), cmd.cmd_as_bytes(),self.release_cmd.as_addr(),cmd.daddr + offset_of!(CmdMsg,cmd));
        let local_daddr = self.comm.local_addr(dst, cmd.daddr);
        // println!("sending release to {dst}");
        self.comm.put(
            dst,
            self.release_cmd.cmd_as_bytes(),
            local_daddr + offset_of!(CmdMsg, cmd),
        ); // + self.comm.base_addr(),);
    }

    //#[tracing::instrument(skip_all)]
    fn send_free(&self, dst: usize, cmd: CmdMsg) {
        // let cmd_buffer = self.cmd_buffers[dst].lock();
        // println!("sending release: {:?} cmd: {:?} {:?} {:?} 0x{:x} 0x{:x}",self.release_cmd,cmd,self.release_cmd.cmd_as_bytes(), cmd.cmd_as_bytes(),self.release_cmd.as_addr(),cmd.daddr + offset_of!(CmdMsg,cmd));
        let local_daddr = self.comm.local_addr(dst, cmd.daddr);
        // println!("sending free to {dst}");
        self.comm.put(
            dst,
            self.free_cmd.cmd_as_bytes(),
            local_daddr + offset_of!(CmdMsg, cmd),
        ); // + self.comm.base_addr(),);
    }

    //#[tracing::instrument(skip_all)]
    async fn send_print(&self, dst: usize, cmd: CmdMsg) {
        let mut timer = std::time::Instant::now();
        while self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8 {
            {
                // let mut cmd_buffer = self.cmd_buffers[dst].lock();
                let mut send_buf = self.send_buffer.lock();
                if send_buf[dst].hash() == self.clear_cmd.hash() {
                    send_buf[dst].daddr = cmd.daddr;
                    send_buf[dst].dsize = cmd.dsize;
                    send_buf[dst].cmd = Cmd::Print;
                    send_buf[dst].msg_hash = cmd.msg_hash;
                    send_buf[dst].calc_hash();
                    // println!("sending print {:?} (s: {:?} r: {:?})",addr,self.sent_cnt.load(Ordering::SeqCst),self.recv_cnt.load(Ordering::SeqCst));
                    let recv_buffer = self.recv_buffer.lock();
                    println!("sending cmd {:?}", send_buf);
                    println!("sending print to {dst}");
                    self.comm
                        .put(
                            dst,
                            send_buf[dst].as_bytes(),
                            recv_buffer[self.my_pe].as_addr(),
                        )
                        .await
                        .expect("put failed");
                    // self.put_amt.fetch_add(send_buf[dst].as_bytes().len(),Ordering::Relaxed);
                    break;
                }
                if timer.elapsed().as_secs_f64() > config().deadlock_timeout {
                    let send_buf = self.send_buffer.lock();
                    // println!("waiting to add cmd to cmd buffer {:?}",cmd_buffer);
                    println!("send_buf: {:?}", send_buf);
                    // drop(send_buf);
                    let recv_buf = self.recv_buffer.lock();
                    println!("recv_buf: {:?}", recv_buf);
                    let free_buf = self.free_buffer.lock();
                    println!("free_buf: {:?}", free_buf);
                    timer = std::time::Instant::now();
                }
            }
            async_std::task::yield_now().await;
        }
    }

    //#[tracing::instrument(skip_all)]
    fn check_transfers(&self, src: usize) {
        let mut cmd_buffer = self.cmd_buffers[src].lock();
        self.progress_transfers(src, &mut cmd_buffer);
    }

    //#[tracing::instrument(skip_all)]
    fn print_data(&self, src: usize, cmd: CmdMsg) {
        let cmd_buffer = self.cmd_buffers[src].lock();
        println!("print data -- cmd {:?}", cmd);
        println!("print data -- cmd_buffer {:?}", cmd_buffer);
    }

    //update cmdbuffers to include a hash the wait on that here
    //#[tracing::instrument(skip_all)]
    async fn get_data(&self, src: usize, cmd: CmdMsg, data_slice: &mut [u8]) {
        let local_daddr = self.comm.local_addr(src, cmd.daddr);
        // println!("command queue getting data from {src}, {:?}", cmd.daddr);
        self.comm
            .get(src, local_daddr as usize, data_slice)
            .await
            .expect("get failed");
        // self.get_amt.fetch_add(data_slice.len(),Ordering::Relaxed);
        let mut timer = std::time::Instant::now();
        while calc_hash(data_slice.as_ptr() as usize, data_slice.len()) != cmd.msg_hash
            && self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8
        {
            async_std::task::yield_now().await;
            if timer.elapsed().as_secs_f64() > config().deadlock_timeout {
                println!(
                    "stuck waiting for data from {:?}!!! {:?} {:?} {:?} {:?} -- calced hash {:?}",
                    src,
                    cmd,
                    data_slice.len(),
                    cmd.dsize,
                    &data_slice,
                    calc_hash(data_slice.as_ptr() as usize, data_slice.len())
                );
                self.send_print(src, cmd).await;
                timer = std::time::Instant::now();
            }
        }
        // println!("got data {:?}", data_slice);
    }

    //#[tracing::instrument(skip_all)]
    async fn get_serialized_data(&self, src: usize, cmd: CmdMsg, ser_data: &mut SerializedData) {
        let len = ser_data.len();
        let data_slice = ser_data.header_and_data_as_bytes_mut();
        let local_daddr = self.comm.local_addr(src, cmd.daddr);
        // println!("command queue getting serialized data from {src}");
        self.comm
            .get(src, local_daddr as usize, data_slice)
            .await
            .expect("get failed");
        // self.get_amt.fetch_add(data_slice.len(),Ordering::Relaxed);
        let mut timer = std::time::Instant::now();
        while calc_hash(data_slice.as_ptr() as usize, len) != cmd.msg_hash
            && self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8
        {
            async_std::task::yield_now().await;
            if timer.elapsed().as_secs_f64() > config().deadlock_timeout {
                // println!(
                //     "stuck waiting for serialized data from {:?} !!! {:?} {:?} {:?} {:?}",
                //     src,
                //     cmd,
                //     data_slice.len(),
                //     cmd.dsize,
                //     &data_slice
                // );
                self.send_print(src, cmd).await;
                timer = std::time::Instant::now();
            }
        }
    }

    //#[tracing::instrument(skip_all)]
    async fn get_cmd(&self, src: usize, cmd: CmdMsg) -> SerializedData {
        let mut ser_data = self.comm.new_serialized_data(cmd.dsize as usize);
        let mut timer = std::time::Instant::now();
        // let mut print = false;
        while ser_data.is_err() && self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8 {
            async_std::task::yield_now().await;
            // println!("cq 848 need to alloc memory {:?}",cmd.dsize);
            self.send_alloc(cmd.dsize);
            ser_data = self.comm.new_serialized_data(cmd.dsize as usize);
            // println!("cq 851 data {:?}",ser_data.is_ok());
            if timer.elapsed().as_secs_f64() > config().deadlock_timeout && ser_data.is_err() {
                // println!(
                //     "get cmd stuck waiting for alloc {:?} {:?}",
                //     cmd.dsize,
                //     self.comm
                //         .rt_check_alloc(cmd.dsize, std::mem::align_of::<u8>())
                // );
                self.comm.print_pools();
                timer = std::time::Instant::now();
            }
            // print = true;
            // ser_data = match ser_data{
            //     Ok(ser_data) => {
            //                         ser_data.print();
            //                         Ok(ser_data)
            //     },
            //     Err(err) => Err(err),

            // };
        }
        let mut ser_data = ser_data.unwrap();
        // if print{
        // ser_data.print();
        // }
        self.get_serialized_data(src, cmd, &mut ser_data).await;
        // println!(
        //     "received data {:?}",
        //     &ser_data.header_and_data_as_bytes()[0..10]
        // );
        self.recv_cnt.fetch_add(1, Ordering::SeqCst);
        // println!("received: {:?} {:?} cmd: {:?} {:?}",cmd.dsize,ser_data.len(),cmd,&ser_data.header_and_data_as_bytes()[0..20]);
        // SerializedData::RofiData(ser_data)
        ser_data
    }

    //#[tracing::instrument(skip_all)]
    async fn get_cmd_buf(&self, src: usize, cmd: CmdMsg) -> usize {
        let mut data = self
            .comm
            .rt_alloc(cmd.dsize as usize, std::mem::align_of::<CmdMsg>());
        let mut timer = std::time::Instant::now();
        while data.is_err() && self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8 {
            async_std::task::yield_now().await;
            // println!("cq 871 need to alloc memory {:?}",cmd.dsize);
            self.send_alloc(cmd.dsize);
            data = self
                .comm
                .rt_alloc(cmd.dsize as usize, std::mem::align_of::<CmdMsg>());
            // println!("cq 874 data {:?}",data.is_ok());
            if timer.elapsed().as_secs_f64() > config().deadlock_timeout {
                println!("get cmd buf stuck waiting for alloc");
                timer = std::time::Instant::now();
            }
        }
        // println!("getting into {:?} 0x{:x} ",data, data.unwrap() + self.comm.base_addr());
        let data = data.unwrap();
        let data_slice =
            unsafe { std::slice::from_raw_parts_mut(data as *mut u8, cmd.dsize as usize) };
        self.get_data(src, cmd, data_slice).await;
        // println!("received cmd_buf {:?}", data_slice);
        // println!(
        //     "sending release to src: {:?} {:?} (s: {:?} r: {:?})",
        //     src,
        //     cmd.daddr,
        //     self.sent_cnt.load(Ordering::SeqCst),
        //     self.recv_cnt.load(Ordering::SeqCst)
        // );
        self.send_release(src, cmd);
        data
    }
}

impl Drop for InnerCQ {
    //#[tracing::instrument(skip_all)]
    fn drop(&mut self) {
        // println!("dropping InnerCQ");
        let mut send_buf = self.send_buffer.lock();
        let old = std::mem::take(&mut *send_buf);
        let _ = Box::into_raw(old);
        let mut recv_buf = self.recv_buffer.lock();
        let old = std::mem::take(&mut *recv_buf);
        let _ = Box::into_raw(old);
        let mut free_buf = self.free_buffer.lock();
        let old = std::mem::take(&mut *free_buf);
        let _ = Box::into_raw(old);
        let mut alloc_buffer = self.alloc_buffer.lock();
        let old = std::mem::take(&mut *alloc_buffer);
        let _ = Box::into_raw(old);
        let mut panic_buffer = self.panic_buffer.lock();
        let old = std::mem::take(&mut *panic_buffer);
        let _ = Box::into_raw(old);
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
    send_buffer_addr: usize,
    recv_buffer_addr: usize,
    free_buffer_addr: usize,
    alloc_buffer_addr: usize,
    panic_buffer_addr: usize,
    release_cmd_addr: usize,
    clear_cmd_addr: usize,
    free_cmd_addr: usize,
    cmd_buffers_addrs: Vec<Arc<Vec<usize>>>,
    comm: Arc<Comm>,
    active: Arc<AtomicU8>,
}

impl CommandQueue {
    //#[tracing::instrument(skip_all)]
    pub(crate) fn new(
        comm: Arc<Comm>,
        my_pe: usize,
        num_pes: usize,
        active: Arc<AtomicU8>,
    ) -> CommandQueue {
        let send_buffer_addr = comm
            .rt_alloc(
                num_pes * std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap();
        // + comm.base_addr();
        // println!("send_buffer_addr{:x} {:x}",send_buffer_addr,send_buffer_addr-comm.base_addr());
        let recv_buffer_addr = comm
            .rt_alloc(
                num_pes * std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap();
        // + comm.base_addr();
        // println!("recv_buffer_addr {:x} {:x}",recv_buffer_addr,recv_buffer_addr-comm.base_addr());
        let free_buffer_addr = comm
            .rt_alloc(
                num_pes * std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap();
        // + comm.base_addr();
        // println!("free_buffer_addr {:x} {:x}",recv_buffer_addr,recv_buffer_addr-comm.base_addr());
        let alloc_buffer_addr = comm
            .rt_alloc(
                num_pes * std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap();
        // println!("alloc_buffer_addr {:x}",alloc_buffer_addr);
        let panic_buffer_addr = comm
            .rt_alloc(
                num_pes * std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap();
        // println!("panic_buffer_addr {:x}",panic_buffer_addr);
        let release_cmd_addr = comm
            .rt_alloc(
                std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap(); // + comm.base_addr();
                       // println!("release_cmd_addr {:x}",release_cmd_addr-comm.base_addr());

        let clear_cmd_addr = comm
            .rt_alloc(
                std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap(); // + comm.base_addr();
                       // println!("clear_cmd_addr {:x}",clear_cmd_addr-comm.base_addr());
        let free_cmd_addr = comm
            .rt_alloc(
                std::mem::size_of::<CmdMsg>(),
                std::mem::align_of::<CmdMsg>(),
            )
            .unwrap(); // + comm.base_addr();
                       // println!("free_cmd_addr {:x}",free_cmd_addr-comm.base_addr());

        let mut cmd_buffers_addrs = vec![];
        for _pe in 0..num_pes {
            let mut addrs = vec![];
            for _i in 0..config().cmd_buf_cnt {
                let addr = comm
                    .rt_alloc(
                        config().cmd_buf_len * std::mem::size_of::<CmdMsg>() + 1,
                        std::mem::align_of::<CmdMsg>(),
                    )
                    .unwrap(); //+ comm.base_addr();
                               // println!("{:x} {:x} {:x}",_pe,_i,addr,);
                addrs.push(addr);
            }
            cmd_buffers_addrs.push(Arc::new(addrs));
        }
        // let cmd_buffers_addrs=Arc::new(cmd_buffers_addrs);
        let cq = InnerCQ::new(
            send_buffer_addr,
            recv_buffer_addr,
            free_buffer_addr,
            alloc_buffer_addr,
            panic_buffer_addr,
            &cmd_buffers_addrs.clone(),
            release_cmd_addr,
            clear_cmd_addr,
            free_cmd_addr,
            comm.clone(),
            my_pe,
            num_pes,
            active.clone(),
        );
        CommandQueue {
            cq: Arc::new(cq),
            send_buffer_addr: send_buffer_addr,
            recv_buffer_addr: recv_buffer_addr,
            free_buffer_addr: free_buffer_addr,
            alloc_buffer_addr: alloc_buffer_addr,
            panic_buffer_addr: panic_buffer_addr,
            release_cmd_addr: release_cmd_addr,
            clear_cmd_addr: clear_cmd_addr,
            free_cmd_addr: free_cmd_addr,
            cmd_buffers_addrs: cmd_buffers_addrs,
            comm: comm,
            active: active,
        }
    }

    //#[tracing::instrument(skip_all)]
    pub(crate) fn send_alloc(&self, min_size: usize) {
        self.cq.send_alloc(min_size)
    }

    //#[tracing::instrument(skip_all)]
    pub(crate) fn send_panic(&self) {
        self.cq.send_panic()
    }

    //#[tracing::instrument(skip_all)]
    pub(crate) async fn send_data(&self, data: RemoteSerializedData, dst: usize) {
        // match data {
        //     #[cfg(feature = "rofi")]
        //     SerializedData::RofiData(ref data) => {
        //         // println!("sending: {:?} {:?}",data.relative_addr,data.len);
        //         // let hash = calc_hash(data.relative_addr + self.comm.base_addr(), data.len);
        //         let hash = calc_hash(data.relative_addr, data.len);

        //         // println!(
        //         //     "[{:?}] send_data: {:?} {:?} {:?} {:?}",
        //         //     std::thread::current().id(),
        //         //     data.relative_addr,
        //         //     data.len,
        //         //     hash,
        //         //     &data.header_and_data_as_bytes()[0..20]
        //         // );
        //         data.increment_cnt(); //or we could implement something like an into_raw here...
        //                               // println!("sending data {:?}", data.header_and_data_as_bytes());

        //         self.cq.send(data.relative_addr, data.len, dst, hash).await;
        //     }
        //     #[cfg(feature = "enable-rofi-rust")]
        //     SerializedData::RofiRustData(ref data) => {
        //         // println!("sending: {:?} {:?}",data.relative_addr,data.len);
        //         // let hash = calc_hash(data.relative_addr + self.comm.base_addr(), data.len);
        //         let hash = calc_hash(data.relative_addr, data.len);

        //         // println!(
        //         //     "[{:?}] send_data: {:?} {:?} {:?} {:?}",
        //         //     std::thread::current().id(),
        //         //     data.relative_addr,
        //         //     data.len,
        //         //     hash,
        //         //     &data.header_and_data_as_bytes()[0..20]
        //         // );
        //         data.increment_cnt(); //or we could implement something like an into_raw here...
        //                               // println!("sending data {:?}", data.header_and_data_as_bytes());

        //         self.cq.send(data.relative_addr, data.len, dst, hash).await;
        //     }
        //     #[cfg(feature = "enable-rofi-rust")]
        //     SerializedData::RofiRustAsyncData(ref data) => {
        //         // println!("sending: {:?} {:?}",data.relative_addr,data.len);
        //         // let hash = calc_hash(data.relative_addr + self.comm.base_addr(), data.len);
        //         let hash = calc_hash(data.relative_addr, data.len);

        //         // println!(
        //         //     "[{:?}] send_data: {:?} {:?} {:?} {:?}",
        //         //     std::thread::current().id(),
        //         //     data.relative_addr,
        //         //     data.len,
        //         //     hash,
        //         //     &data.header_and_data_as_bytes()[0..20]
        //         // );
        //         data.increment_cnt(); //or we could implement something like an into_raw here...
        //                               // println!("sending data {:?}", data.header_and_data_as_bytes());

        //         self.cq.send(data.relative_addr, data.len, dst, hash).await;
        //     }
        //     #[cfg(feature = "enable-libfabric")]
        //     SerializedData::LibFabData(ref data) => {
        //         // println!("sending: {:?} {:?}",data.relative_addr,data.len);
        //         // let hash = calc_hash(data.relative_addr + self.comm.base_addr(), data.len);
        //         let hash = calc_hash(data.relative_addr, data.len);

        //         // println!(
        //         //     "[{:?}] send_data: {:?} {:?} {:?} {:?}",
        //         //     std::thread::current().id(),
        //         //     data.relative_addr,
        //         //     data.len,
        //         //     hash,
        //         //     &data.header_and_data_as_bytes()[0..20]
        //         // );
        //         data.increment_cnt(); //or we could implement something like an into_raw here...
        //                               // println!("sending data {:?}", data.header_and_data_as_bytes());

        //         self.cq.send(data.relative_addr, data.len, dst, hash).await;
        //     }
        //     #[cfg(feature = "enable-libfabric")]
        //     SerializedData::LibFabAsyncData(ref data) => {
        //         // println!("sending: {:?} {:?}",data.relative_addr,data.len);
        //         // let hash = calc_hash(data.relative_addr + self.comm.base_addr(), data.len);
        //         let hash = calc_hash(data.relative_addr, data.len);

        //         // println!(
        //         //     "[{:?}] send_data: {:?} {:?} {:?} {:?}",
        //         //     std::thread::current().id(),
        //         //     data.relative_addr,
        //         //     data.len,
        //         //     hash,
        //         //     &data.header_and_data_as_bytes()[0..20]
        //         // );
        //         data.increment_cnt(); //or we could implement something like an into_raw here...
        //                               // println!("sending data {:?}", data.header_and_data_as_bytes());

        //         self.cq.send(data.relative_addr, data.len, dst, hash).await;
        //     }
        //     SerializedData::ShmemData(ref data) => {
        // println!("sending: {:?} {:?}",data.relative_addr,data.len);
        // let hash = calc_hash(data.relative_addr + self.comm.base_addr(), data.len);
        let hash = calc_hash(data.ser_data_addr, data.len());

        // println!("send_data: {:?} {:?} {:?}",data.relative_addr,data.len,hash);
        data.increment_cnt(); //or we could implement something like an into_raw here...
                              // println!("sending data {:?}",data.header_and_data_as_bytes());
        self.cq
            .send(data.ser_data_addr, data.len(), dst, hash)
            .await;
        //     }
        //     _ => {}
        // }
    }

    //#[tracing::instrument(skip_all)]
    pub(crate) async fn alloc_task(&self, scheduler: Arc<Scheduler>) {
        while scheduler.active() && self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8 {
            self.cq.check_alloc();
            async_std::task::yield_now().await;
        }
        // println!("leaving alloc_task task {:?}", scheduler.active());
        // println!("sechduler_new: {:?}", Arc::strong_count(&scheduler));
    }

    //#[tracing::instrument(skip_all)]
    pub(crate) async fn panic_task(&self, scheduler: Arc<Scheduler>) {
        let mut panic = false;
        while scheduler.active() && !panic {
            panic = self.cq.check_panic();
            async_std::task::yield_now().await;
        }

        if panic {
            println!("received panic from other PE");
            // scheduler.force_shutdown();
            panic!("received panic from other PE");
        }
        // println!("leaving panic_task task {:?}", scheduler.active());
        // println!("sechduler_new: {:?}", Arc::strong_count(&scheduler));
    }

    //#[tracing::instrument(skip_all)]
    pub(crate) async fn recv_data(&self, scheduler: Arc<Scheduler>, lamellae: Arc<Lamellae>) {
        let comm = lamellae.comm();
        let num_pes = comm.num_pes();
        let my_pe = comm.my_pe();
        // let mut timer= std::time::Instant::now();
        while self.active.load(Ordering::SeqCst) == CmdQStatus::Active as u8
            || !self.cq.empty()
            || scheduler.active()
        {
            // CNTS.get_or(|| AtomicUsize::new(0))
            //     .fetch_add(1, Ordering::Relaxed);
            for src in 0..num_pes {
                if src != my_pe {
                    if let Some(cmd_buf_cmd) = self.cq.ready(src) {
                        // timer =  std::time::Instant::now();
                        let cmd_buf_cmd = cmd_buf_cmd;
                        // println!("recv_data {:?}", cmd_buf_cmd);
                        match cmd_buf_cmd.cmd {
                            Cmd::Alloc => panic!("should not encounter alloc here"),
                            Cmd::Panic => panic!("should not encounter panic here"),
                            Cmd::Clear | Cmd::Release => {
                                panic!("should not be possible to see cmd clear or release here")
                            }
                            Cmd::Free => panic!("should not be possible to see free here"),
                            // self.cq.free_data(src,cmd_buf_cmd.daddr as usize),
                            Cmd::Print => self.cq.print_data(src, cmd_buf_cmd),
                            Cmd::Tx => {
                                let cq = self.cq.clone();
                                let lamellae = lamellae.clone();
                                let scheduler1 = scheduler.clone();
                                let comm = self.comm.clone();
                                let task = async move {
                                    // println!(
                                    //     "going to get cmd_buf {:?} from {:?}",
                                    //     cmd_buf_cmd, src
                                    // );
                                    let data = cq.get_cmd_buf(src, cmd_buf_cmd).await;
                                    let cmd_buf = unsafe {
                                        std::slice::from_raw_parts(
                                            data as *const CmdMsg,
                                            cmd_buf_cmd.dsize as usize
                                                / std::mem::size_of::<CmdMsg>(),
                                        )
                                    };
                                    // println!("cmd_buf {:?}", cmd_buf);
                                    let mut i = 0;
                                    let len = cmd_buf.len();
                                    let cmd_cnt = Arc::new(AtomicUsize::new(len));
                                    // println!("src: {:?} cmd_buf len {:?}", src, len);

                                    for cmd in cmd_buf {
                                        let cmd = *cmd;
                                        if cmd.dsize != 0 {
                                            let cq = cq.clone();
                                            let lamellae = lamellae.clone();
                                            let scheduler2 = scheduler1.clone();
                                            // cmd_cnt.fetch_add(1,Ordering::SeqCst);
                                            let cmd_cnt_clone = cmd_cnt.clone();
                                            let task = async move {
                                                // println!(
                                                //     "getting cmd {:?} [{:?}/{:?}]",
                                                //     cmd, i, len
                                                // );
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
                                    // comm.rt_free(data - comm.base_addr());
                                    comm.rt_free(data);
                                };
                                // println!(
                                //     "[{:?}] recv_data submitting tx task",
                                //     std::thread::current().id()
                                // );
                                scheduler.submit_io_task(task);
                            }
                        }
                    }
                    self.cq.check_transfers(src);
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

    //#[tracing::instrument(skip_all)]
    pub(crate) fn tx_amount(&self) -> usize {
        // println!("cq put: {:?} get {:?}",self.cq.put_amt.load(Ordering::SeqCst) ,self.cq.get_amt.load(Ordering::SeqCst));
        self.cq.put_amt.load(Ordering::SeqCst) + self.cq.get_amt.load(Ordering::SeqCst)
    }

    //#[tracing::instrument(skip_all)]
    pub(crate) fn mem_per_pe() -> usize {
        (config().cmd_buf_len * config().cmd_buf_cnt + 4) * std::mem::size_of::<CmdMsg>()
    }
}

impl Drop for CommandQueue {
    //#[tracing::instrument(skip_all)]
    fn drop(&mut self) {
        // println!("dropping rofi command queue");
        self.comm.rt_free(self.send_buffer_addr); // - self.comm.base_addr());
        self.comm.rt_free(self.recv_buffer_addr); // - self.comm.base_addr());
        self.comm.rt_free(self.free_buffer_addr); // - self.comm.base_addr());
        self.comm.rt_free(self.alloc_buffer_addr);
        self.comm.rt_free(self.panic_buffer_addr);
        self.comm.rt_free(self.release_cmd_addr); // - self.comm.base_addr());
        self.comm.rt_free(self.clear_cmd_addr); // - self.comm.base_addr());
        self.comm.rt_free(self.free_cmd_addr); // - self.comm.base_addr());
        for bufs in self.cmd_buffers_addrs.iter() {
            for buf in bufs.iter() {
                self.comm.rt_free(*buf);
            }
        }
        // println!("rofi command queue dropped");
    }
}

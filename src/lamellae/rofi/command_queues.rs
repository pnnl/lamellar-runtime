use crate::lamellae::rofi::rofi_comm::RofiComm;
use crate::lamellar_alloc::{BTreeAlloc, LamellarAlloc, ObjAlloc};
use log::trace;
use parking_lot::{Mutex, RwLock};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

const CMD_Q_LEN: usize = 10000;
const NUM_REQ_SLOTS: usize = 100000;
const DATA_SIZE: usize = 200_000_000;
const FINI_STATUS: RofiReqStatus = RofiReqStatus::Fini;

static GARBAGE_COLLECT_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(10000);

lazy_static! {
    static ref DATA_COUNTS: Vec<AtomicUsize> = {
        let mut temp = Vec::new();
        for _i in 0..NUM_REQ_SLOTS {
            temp.push(AtomicUsize::new(0));
        }
        temp
    };
}

#[repr(u8)]
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq)]
pub(crate) enum RofiReqStatus {
    None,
    Init,
    Tx,
    Fini,
}

impl RofiReqStatus {
    fn as_bytes(&self) -> &[u8] {
        let pointer = self as *const RofiReqStatus as *const u8;
        let size = std::mem::size_of::<RofiReqStatus>();
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(pointer, size) };
        slice
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct RofiCmd {
    daddr: usize, // std::ffi::c_void,
    dsize: usize,
    data_hash: usize,
    cmd_hash: usize,
    status_idx: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct RofiReqInfo {
    cmd: RofiCmd,
    cmd_idx: usize,
    data_cnt_idx: usize,
    dst: usize,
    id: usize,
}
unsafe impl Send for RofiReqInfo {}

#[derive(Clone, Debug)]
struct RofiReqBuffer<T: LamellarAlloc> {
    alloc: T,
    cmd_allocs: Vec<T>,
    req_info: Arc<Mutex<Box<[RofiReqInfo]>>>,
    rofi_base_addr: Arc<RwLock<usize>>,
    status_base_addr: usize,
    cmd_base_addr: usize,
}

impl RofiCmd {
    fn new(daddr: usize, dsize: usize, data_hash: usize, status_idx: u32) -> RofiCmd {
        let mut cmd = RofiCmd {
            daddr: daddr,
            dsize: dsize,
            data_hash: data_hash,
            cmd_hash: 0,
            status_idx: status_idx,
        };
        cmd.cmd_hash = cmd.calc_cmd_hash();
        cmd
    }
    fn as_bytes(&self) -> &[u8] {
        let pointer = self as *const RofiCmd as *const u8;
        let size = std::mem::size_of::<RofiCmd>();
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(pointer, size) };
        slice
    }

    fn update(&mut self, daddr: usize, dsize: usize, data_hash: usize, status_idx: u32) {
        self.daddr = daddr;
        self.dsize = dsize;
        self.data_hash = data_hash;
        self.status_idx = status_idx;
        self.cmd_hash = self.calc_cmd_hash();
    }
    fn clear(&mut self) {
        self.daddr = 0;
        self.dsize = 0;
        self.data_hash = 0;
        self.cmd_hash = 0;
        self.status_idx = 0;
    }
    fn calc_cmd_hash(&self) -> usize {
        self.daddr as usize
            + self.dsize as usize
            + self.data_hash as usize
            + self.status_idx as usize
    }
}

impl RofiReqInfo {
    fn new(id: usize) -> RofiReqInfo {
        RofiReqInfo {
            cmd: RofiCmd::new(0, 0, 0, 0),
            cmd_idx: 0, //remote index
            data_cnt_idx: 0,
            dst: 0,
            id: id,
        }
    }
    fn update(&mut self, cmd: RofiCmd, cmd_idx: usize, data_cnt_idx: usize, dst: usize, id: usize) {
        self.cmd
            .update(cmd.daddr, cmd.dsize, cmd.data_hash, cmd.status_idx);
        self.cmd_idx = cmd_idx;
        self.data_cnt_idx = data_cnt_idx;
        self.dst = dst;
        self.id = id;
    }
    fn clear(&mut self) {
        self.cmd.clear();
        self.cmd_idx = 0;
        self.data_cnt_idx = 0;
        self.dst = 0;
        self.id = 0;
    }
}

impl<T: LamellarAlloc> RofiReqBuffer<T> {
    fn new(rofi_base_addr: Arc<RwLock<usize>>) -> RofiReqBuffer<T> {
        trace!("new RofiReqBuffer");
        RofiReqBuffer {
            alloc: T::new("req_buffer".to_string()), //location of req_info locally...
            cmd_allocs: vec![],                      //location in remote nodes...
            req_info: Arc::new(Mutex::new(
                // vec![RofiReqInfo::new(0); NUM_REQ_SLOTS].into_boxed_slice(),
                Box::new([RofiReqInfo::new(0); NUM_REQ_SLOTS]),
            )),
            rofi_base_addr: rofi_base_addr,
            status_base_addr: 0,
            cmd_base_addr: 0,
        }
    }

    fn init(&mut self, rofi_comm: Arc<RofiComm>, my_pe: usize, num_pes: usize, num_entries: usize) {
        self.status_base_addr = rofi_comm
            .alloc(num_entries * std::mem::size_of::<RofiReqStatus>())
            .expect("error allocating request status buffers");
        self.cmd_base_addr = rofi_comm
            .alloc(CMD_Q_LEN * num_pes * std::mem::size_of::<RofiCmd>())
            .expect("error allocating cmd buffers");
        trace!(
            "RofiReqBuffer: status_base_addr: 0x{:x} cmd_base_addr 0x{:x}",
            self.status_base_addr,
            self.cmd_base_addr
        );
        self.alloc.init(0, num_entries);
        if num_pes > 1 {
            let num_cmds_slots_per_pe = CMD_Q_LEN; // / (num_pes - 1);
            for i in 0..num_pes {
                // !!! note we create an empty allocation for my_pe
                // we short circuit local requests so the allocation for my_pe is never used
                // this implies we need to do some
                let start_idx = if i < my_pe {
                    (my_pe - 1) * num_cmds_slots_per_pe
                } else {
                    my_pe * num_cmds_slots_per_pe
                };

                let mut alloc = T::new("cmd".to_owned() + &i.to_string());
                if i != my_pe {
                    alloc.init(start_idx, num_cmds_slots_per_pe);
                } else {
                    alloc.init(0, 0);
                }
                self.cmd_allocs.push(alloc);
            }
        }
    }

    fn add_request(
        &self,
        daddr: usize,
        dsize: usize,
        dhash: usize,
        data_cnt_idx: usize,
        dst: usize,
        id: usize,
        status: RofiReqStatus,
    ) -> (usize, *const RofiCmd, usize) {
        let idx = self.alloc.malloc(1); //location to store request locally
        let cmd_idx = self //location to write into remotely
            .cmd_allocs
            .get(dst)
            .expect("cmd allocs dst out of bounds")
            .malloc(1);
        let mut req_i = self.req_info.lock();
        let cmd = RofiCmd::new(daddr, dsize, dhash, idx as u32);
        req_i[idx].update(cmd, cmd_idx, data_cnt_idx, dst, id);
        let cmd_ptr = &(req_i[idx].cmd) as *const RofiCmd;
        drop(req_i);
        unsafe {
            *((*self.rofi_base_addr.read() + self.status_base_addr + idx) as *mut RofiReqStatus) =
                status
        };
        (self.get_cmd_addr(cmd_idx), cmd_ptr, cmd_idx)
    }

    fn get_status(&self, idx: usize) -> RofiReqStatus {
        unsafe { *(self.get_status_addr(idx) as *const RofiReqStatus) }
    }

    fn get_status_addr(&self, idx: usize) -> usize {
        *self.rofi_base_addr.read()
            + self.status_base_addr
            + idx * std::mem::size_of::<RofiReqStatus>()
    }

    fn get_cmd(&self, idx: usize) -> RofiCmd {
        unsafe { *(self.get_cmd_addr(idx) as *const RofiCmd) }
    }

    fn clear_cmd(&self, idx: usize) {
        unsafe { &(*(self.get_cmd_addr(idx) as *mut RofiCmd)).clear() };
    }

    fn get_cmd_addr(&self, idx: usize) -> usize {
        *self.rofi_base_addr.read() + self.cmd_base_addr + idx * std::mem::size_of::<RofiCmd>()
    }

    fn free_request(&self, idx: usize) -> RofiReqInfo {
        let mut req_info = self.req_info.lock();
        let req = req_info
            .get(idx)
            .expect("req info entry out of bounds")
            .clone();
        req_info[idx].clear(); //the local request
        drop(req_info);
        assert_eq!(idx, req.cmd.status_idx as usize);
        self.cmd_allocs
            .get(req.dst)
            .expect("cmd allocs dst out of bounds")
            .free(req.cmd_idx as usize); //free my slot on the remote node
        unsafe { *(self.get_status_addr(idx) as *mut RofiReqStatus) = RofiReqStatus::None };
        self.alloc.free(idx); //free my local slot
        req
    }
}

struct RecvThread {
    thread: Option<thread::JoinHandle<()>>,
    id: usize,
    num_threads: usize,
    cmd_queue: Arc<RofiCommandQueueInternal>,
    active: Arc<AtomicBool>,
}

impl RecvThread {
    fn run(&mut self) {
        let cmd_queue = self.cmd_queue.clone();
        let active = self.active.clone();
        let id = self.id;
        let num_threads = self.num_threads;
        self.thread = Some(thread::spawn(move || {
            // let mut garbage: VecDeque <(Instant, (usize,usize))> = VecDeque::new();
            let mut garbage: VecDeque<(Instant, MyPtr)> = VecDeque::new();

            let mut pending_data_map: HashMap<usize,MyPtr> = HashMap::new();
            

            trace!("[{:?}] command queue recv thread launched", cmd_queue.my_pe);
            while active.load(Ordering::SeqCst)
                || cmd_queue.outstanding_msgs.load(Ordering::SeqCst) > 0
            {
                cmd_queue.process_msgs(id,num_threads,&mut garbage,&mut pending_data_map,);
                cmd_queue.process_completed_msgs(id,num_threads); //do we place this in its own thread as well?
                cmd_queue.garbage_collect(&mut garbage);
            }
            trace!(
                "[{:?}] command queue recv thread shutting down",
                cmd_queue.my_pe
            );
        }))
    }
    fn shutdown(&mut self) {
        // trace!(
        //     "[{:?}] shutting down command queue recv thread",
        //     self.cmd_queue.my_pe
        // );
        self.active.store(false, Ordering::SeqCst);
        let _res = self.thread.take().expect("error joining thread").join();
        trace!(
            "[{:?}] shutting down command queue recv thread",
            self.cmd_queue.my_pe
        );
    }
}

struct MyPtr {
    ptr: *mut [u8],
}
unsafe impl Sync for MyPtr {}
unsafe impl Send for MyPtr {}

struct RofiCommandQueueInternal {
    num_pes: usize,
    my_pe: usize,
    rofi_comm: Arc<RofiComm>,
    data_allocs: BTreeAlloc, // manages memory space reserved for am buffers (in rofi memory)
    req_buffer: RofiReqBuffer<ObjAlloc>, // am status slots (in rofi memory)
    data_counts_alloc: ObjAlloc, // multi message counters (in lamellar memory)
    // pending_data_map: Arc<chashmap::CHashMap<usize, usize>>, //*mut [u8]>>,
    // pending_data_map: Arc<RwLock<HashMap<usize, MyPtr>>>,
    msg_tx: crossbeam::channel::Sender<Arc<Vec<u8>>>,
    outstanding_msgs: Arc<AtomicUsize>,
    sent_data_amt: Arc<AtomicUsize>,
    timers: BTreeMap<String, AtomicUsize>,
}

impl Drop for RofiCommandQueueInternal {
    fn drop(&mut self) {
        trace!("[{:?}] RofiCommandQueueInternal Dropping", self.my_pe);
        let mut string = String::new();
        for item in self.timers.iter() {
            string.push_str(&format!(
                "{:?} {:?} ",
                item.0,
                item.1.load(Ordering::SeqCst) as f64 / 1000.0 as f64
            ));
        }
        // println!("command_queue timers: {:?} {:?}", self.my_pe, string);
    }
}

impl RofiCommandQueueInternal {
    pub(crate) fn init(&mut self) {
        self.data_allocs.init(
            self.rofi_comm
                .alloc(DATA_SIZE)
                .expect("error allocating data buffer"),
            DATA_SIZE,
        );
        self.req_buffer.init(
            self.rofi_comm.clone(),
            self.my_pe,
            self.num_pes,
            NUM_REQ_SLOTS,
        );
        self.data_counts_alloc.init(0, NUM_REQ_SLOTS);
        self.timers.insert("put".to_string(), AtomicUsize::new(0));
        self.timers
            .insert("add_request".to_string(), AtomicUsize::new(0));
        self.timers
            .insert("data_counts_alloc".to_string(), AtomicUsize::new(0));
        self.timers
            .insert("data_copy".to_string(), AtomicUsize::new(0));
        self.timers
            .insert("data_alloc".to_string(), AtomicUsize::new(0));
    }
    pub(crate) fn send_data(
        &self,
        data: Vec<u8>,
        team_iter: Box<dyn Iterator<Item = usize> + Send>,
    ) -> Vec<usize> {
        // allocate memory in our local data segment
        // let mut timer = Instant::now();
        let it = Instant::now();
        let daddr = self.data_allocs.malloc(data.len() + 1);
        self.timers
            .get("data_alloc")
            .unwrap()
            .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
        let it = Instant::now();
        self.rofi_comm.local_store(&data, daddr);
        let dummy = [27u8];
        self.rofi_comm.local_store(&dummy, daddr + data.len());

        self.timers
            .get("data_copy")
            .unwrap()
            .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
        //-----------------
        // allocate a data_count object
        // timer = Instant::now();
        let it = Instant::now();
        let data_cnt_idx = self.data_counts_alloc.malloc(1);
        self.timers
            .get("data_counts_alloc")
            .unwrap()
            .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
        // DATA_COUNTS[data_cnt_idx].fetch_add(dsts.len(), Ordering::SeqCst);
        // timers.1 += timer.elapsed().as_secs_f64();
        //----------------------
        let dhash = data.iter().fold(0usize, |a, b| a + (*b as usize)) + dummy[0] as usize;
        let dsize = data.len() + 1;
        let mut status_indices = vec![];
        for dst in team_iter {
            if dst != self.my_pe {
                self.outstanding_msgs.fetch_add(1, Ordering::SeqCst);
                self.sent_data_amt.fetch_add(dsize, Ordering::SeqCst);
                DATA_COUNTS[data_cnt_idx].fetch_add(1, Ordering::SeqCst);
                let it = Instant::now();
                let (dst_cmd_addr, cmd_msg_ptr, _cmd_idx) = self.req_buffer.add_request(
                    daddr,
                    dsize,
                    dhash,
                    data_cnt_idx,
                    dst,
                    0, //id argument currently unused.
                    RofiReqStatus::Init,
                );
                self.timers
                    .get("add_request")
                    .unwrap()
                    .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
                trace!(
                    "[{:?}] sending to [{:?}] -- daddr: {:?} dst_cmd_addr: {:?} dhash{:?}",
                    self.my_pe, dst, daddr, dst_cmd_addr, dhash
                );
                unsafe {
                    status_indices.push((*cmd_msg_ptr).status_idx as usize);
                }
                let src_cmd_slice = unsafe { (*cmd_msg_ptr).as_bytes() };
                // let lock = put_mutex.lock();
                let it = Instant::now();
                self.rofi_comm.put(dst, src_cmd_slice, dst_cmd_addr);
                self.timers
                    .get("put")
                    .unwrap()
                    .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
            }
        }
        status_indices
    }

    fn process_msgs(&self, id: usize, num_threads: usize, garbage: &mut VecDeque<(Instant, MyPtr)>,pending_data_map:&mut HashMap<usize,MyPtr>) {
        let num_cmds_slots_per_pe = CMD_Q_LEN;
        for entry in (id..(CMD_Q_LEN * self.num_pes)).step_by(num_threads) {
            let cmd = self.req_buffer.get_cmd(entry);
            if cmd.dsize != 0 && cmd.cmd_hash == cmd.calc_cmd_hash() {
                let mut src = entry / num_cmds_slots_per_pe;
                //account for not allocating cmd slots for my_pe
                if src >= self.my_pe {
                    src += 1;
                }
                // let map =self.pending_data_map.read();
                if !pending_data_map.contains_key(&entry) {
                    // drop(map);
                    //we havent yet processed this request
                    // println!("size to get: {:?} {:?} {:?}", entry, cmd, cmd.dsize);
                    let mut data = vec![0_u8; cmd.dsize].into_boxed_slice();
                    // println!(
                    //     "size to get: {:?} {:?} {:?} {:?}",
                    //     entry,
                    //     cmd,
                    //     cmd.dsize,
                    //     data.as_ptr()
                    // );
                    self.rofi_comm.iget_relative(src, cmd.daddr, &mut data);
                    let data_ptr = Box::into_raw(data);
                        
                    // let mut map =self.pending_data_map.write();
                    pending_data_map.insert(entry, MyPtr { ptr: data_ptr });
                    // drop(map);
                // .insert(entry, data_ptr as *const u8 as usize);
                } else {
                    // hopefully  our get request has finished...
                    // let mut comp_dptr: usize=0;
                    // let mut map =self.pending_data_map.write();
                    if let Some(data_ptr) = pending_data_map.remove(&entry) {
                        // drop(map);
                        // println!("entry: {:?} {:?}", entry, data_ptr.ptr);
                        // comp_dptr = data_ptr;
                        let data = unsafe {
                            // Box::from_raw(slice::from_raw_parts_mut(
                            //     data_ptr as *const u8 as *mut u8,
                            //     cmd.dsize,
                            // ))
                            Box::from_raw(data_ptr.ptr)
                        };
                        // println!("data len: {:?}", data.len());
                        if cmd.data_hash == data.iter().fold(0usize, |a, b| a + (*b as usize)) {
                            self.req_buffer.clear_cmd(entry);
                            // let lock = put_mutex.lock();
                            self.rofi_comm.put(
                                src,
                                FINI_STATUS.as_bytes(),
                                self.req_buffer.get_status_addr(cmd.status_idx as usize),
                            );
                            let mut dvec = data.to_vec();
                            dvec.pop();
                            // println!(
                            //     "{:?} {:?}",
                            //     data_ptr.ptr as *const u8 as usize,
                            //     dvec.as_ptr() as *const u8 as usize
                            // );
                            let advec = Arc::new(dvec);
                            self.msg_tx
                                .send(advec.clone())
                                .expect("error in sending rofi msg");
                            let data_ptr_n = Box::into_raw(data);
                            if data_ptr_n != data_ptr.ptr {
                                println!(
                                    "UHHHHHH OHHHHHH {:x} {:x}",
                                    data_ptr.ptr as *const u8 as usize,
                                    data_ptr_n as *const u8 as usize
                                );
                            }
                            garbage.push_back((Instant::now(), data_ptr));
                        // drop(lock);
                        // timers.2 += timer.elapsed().as_secs_f64();
                        } else {
                            let data_ptr_n = Box::into_raw(data);
                            // if data_ptr_n as *const u8 as usize != data_ptr {
                            // if data_ptr_n != data_ptr.ptr {
                            // println!(
                            //     "UHHHHHH OHHHHHH {:x} {:x}",
                            //     data_ptr.ptr as *const u8 as usize,
                            //     data_ptr_n as *const u8 as usize
                            // );
                            // }
                            // self.pending_data_map
                            //     .write()
                            //     // .insert(entry, data_ptr as *const u8 as usize);
                            // .insert(entry, data_ptr);
                            // self.pending_data_map
                            //     .write()
                            //     .insert(entry, MyPtr { ptr: data_ptr_n });
                            pending_data_map.insert(entry, MyPtr { ptr: data_ptr_n });
                        }
                    } else {
                        println!("ERROR data buf not found for entry: {:?}", entry);
                    }
                }
            }
        }
    }
    fn process_completed_msgs(&self, id: usize, num_threads: usize, ) {
        for entry in (id..NUM_REQ_SLOTS).step_by(num_threads) {
            // let mut timer = Instant::now();
            let status = self.req_buffer.get_status(entry);
            // timers.3 += timer.elapsed().as_secs_f64();
            if status == RofiReqStatus::Fini {
                // timer = Instant::now();
                let req_info = self.req_buffer.free_request(entry);
                // timers.4 += timer.elapsed().as_secs_f64();

                let current_count =
                    DATA_COUNTS[req_info.data_cnt_idx].fetch_sub(1, Ordering::SeqCst);
                self.outstanding_msgs.fetch_sub(1, Ordering::SeqCst);
                if current_count == 1 {
                    //we can't free the data allocation of idx allocation until all requests accessing this data have completed
                    // timer = Instant::now();
                    self.data_allocs.free(req_info.cmd.daddr);
                    // timers.5 += timer.elapsed().as_secs_f64();
                    // timer = Instant::now();
                    self.data_counts_alloc.free(req_info.data_cnt_idx);
                    // timers.6 += timer.elapsed().as_secs_f64();
                }
            }
        }
    }
    fn garbage_collect(&self, garbage: &mut VecDeque<(Instant, MyPtr)>) {
        let mut max_dur = std::time::Duration::from_secs(0);
        while let Some(front) = garbage.front() {
            if Instant::now() - front.0 > GARBAGE_COLLECT_TIMEOUT {
                if Instant::now() - front.0 > max_dur {
                    max_dur = Instant::now() - front.0;
                }
                let data = unsafe {
                    // Box::from_raw(slice::from_raw_parts_mut(
                    //     (front.1).0 as *const u8 as *mut u8,
                    //     (front.1).1,
                    // ))
                    Box::from_raw(front.1.ptr)
                };
                data.into_vec();
                garbage.pop_front();
            } else {
                // println!(
                //     "{:?} {:?} {:?} {:?}",
                //     Instant::now() - front.0,
                //     GARBAGE_COLLECT_TIMEOUT,
                //     max_dur,
                //     garbage.len()
                // );
                // if max_dur > std::time::Duration::from_secs(0) {
                //     println!("timeout dur time: {:?} {:?}", max_dur, garbage.len());
                // }
                break;
            }
        }
    }
}

pub(crate) struct RofiCommandQueue {
    threads: Vec<RecvThread>,
    internal: Arc<RofiCommandQueueInternal>,
}

impl RofiCommandQueue {
    pub(crate) fn new(
        msg_tx: crossbeam::channel::Sender<Arc<Vec<u8>>>,
        rofi_comm: Arc<RofiComm>,
    ) -> RofiCommandQueue {
        let mut internal = RofiCommandQueueInternal {
            num_pes: rofi_comm.num_pes,
            my_pe: rofi_comm.my_pe,
            rofi_comm: rofi_comm.clone(),
            data_allocs: BTreeAlloc::new("data".to_string()),
            req_buffer: RofiReqBuffer::<ObjAlloc>::new(rofi_comm.rofi_base_address.clone()),
            data_counts_alloc: ObjAlloc::new("data_counts".to_string()),
            // pending_data_map: Arc::new(chashmap::CHashMap::new()),
            // pending_data_map: Arc::new(RwLock::new(HashMap::new())),
            msg_tx: msg_tx,
            outstanding_msgs: Arc::new(AtomicUsize::new(0)),
            sent_data_amt: Arc::new(AtomicUsize::new(0)),
            timers: BTreeMap::new(),
            // garbage_collector: ArrayQueue::new(100000),
        };
        internal.init();
        let mut cmd_queue = RofiCommandQueue {
            threads: Vec::new(),
            internal: Arc::new(internal),
        };
        let num_threads = 2;
        for i in 0..num_threads {
            let mut thread = RecvThread {
                thread: None,
                id: i,
                num_threads: num_threads,
                cmd_queue: cmd_queue.internal.clone(),
                active: Arc::new(AtomicBool::new(true)),
            };
            thread.run();
            cmd_queue.threads.push(thread)
        }
        cmd_queue
    }
    pub(crate) fn finit(&self) {
        self.internal.rofi_comm.finit();
    }

    pub(crate) fn send_data(
        &self,
        data: Vec<u8>,
        team_iter: Box<dyn Iterator<Item = usize> + Send>,
    ) -> Vec<usize> {
        self.internal.send_data(data, team_iter)
    }

    pub(crate) fn my_pe(&self) -> usize {
        self.internal.rofi_comm.my_pe
    }

    pub(crate) fn num_pes(&self) -> usize {
        self.internal.rofi_comm.num_pes
    }
    pub(crate) fn barrier(&self) {
        self.internal.rofi_comm.barrier();
    }
    pub(crate) fn data_sent(&self) -> usize {
        self.internal.sent_data_amt.load(Ordering::SeqCst)
    }
}

impl Drop for RofiCommandQueue {
    fn drop(&mut self) {
        while let Some(mut iothread) = self.threads.pop() {
            iothread.shutdown();
        }
        trace!(
            "[{:?}] RofiCommandQueue Dropping",
            self.internal.rofi_comm.my_pe
        );
    }
}

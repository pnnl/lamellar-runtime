use crate::lamellae_new::rofi::rofi_comm::RofiComm;
use crate::lamellae::SerializedData;
use crate::scheduler::{Scheduler,SchedulerQueue};
use crate::lamellar_alloc::{LamellarAlloc, ObjAlloc};
use lamellar_prof::*;
use log::trace;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, VecDeque};

use std::sync::atomic::{AtomicBool, AtomicUsize,AtomicU8, Ordering};
use std::sync::Arc;
use std::rc::Rc;
use std::thread;
use std::time::Instant;

const CMD_BUF_LEN: usize = 10000; // this is the number of slots for each PE
                                // const NUM_REQ_SLOTS: usize = CMD_Q_LEN; // max requests at any given time -- probably have this be a multiple of num PES
const CMD_BUFS_PER_PE: usize = 2;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct RofiCmd { // we send this to remote nodes
    daddr: usize, 
    dsize: usize,
}

impl RofiCmd{
    fn as_bytes(&self) -> &[u8]{
        let pointer = self as *const Self as *const u8;
        let size = std::mem::size_of::<Self>();
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(pointer, size) };
        slice
    }
    fn as_addr(&self)->usize{
        self as *const RofiCmd as usize
    }
}

struct RofiCmdBuffer<'a>{
    bufs: Vec<&'a mut [RofiCmd]>,
    addrs: Rc<Vec<usize>>,
    ready_buf: Option<usize>,
    cur_buf: usize,
    cur_ind: usize,
}

impl<'a> RofiCmdBuffer<'a> {
    fn new(addrs: Rc<Vec<usize>>) -> RofiCmdBuffer<'a>{
        let mut bufs = vec![];
        for addr in addrs.iter(){
            bufs.push(unsafe {std::slice::from_raw_parts_mut(*addr as *mut RofiCmd, CMD_BUF_LEN)});
        }
        RofiCmdBuffer{
            bufs: bufs,
            addrs: addrs,
            ready_buf: None,
            cur_buf: 0,
            cur_ind: 0,
        }
    }
    fn try_push(&mut self, data: &SerializedData)-> bool{
        if self.cur_ind + 1 < self.bufs[self.cur_buf].len(){ //space in the current buffer
            self.cur_ind += 1;
            let mut cmd = self.bufs[self.cur_buf][self.cur_ind];
            cmd.daddr = data.addr;
            cmd.dsize = data.len;
            true
        }else if self.ready_buf.is_none(){ //switch buffers and mark previous as ready
            self.ready_buf = Some(self.cur_buf);
            self.cur_buf = (self.cur_buf + 1) % self.bufs.len();
            self.cur_ind = 0;
            let mut cmd = self.bufs[self.cur_buf][self.cur_ind];
            cmd.daddr = data.addr;
            cmd.dsize = data.len;
            true
        }
        else{ // no buffers available
            false
        }
    }
    fn flush_buffer(&mut self,  cmd: &mut RofiCmd) {
        if let Some(ready_buf) = self.ready_buf{
            self.ready_buf = None;
            cmd.daddr=self.addrs[ready_buf];
            cmd.dsize=self.bufs[ready_buf].len()*std::mem::size_of::<RofiCmd>();
        }
        else {
            cmd.daddr = self.addrs[self.cur_buf];
            cmd.dsize = self.cur_ind * std::mem::size_of::<RofiCmd>();
            self.cur_ind = 0;
        }
    }
}

struct RofiCQ<'a>{
    send_buffer: Arc<Mutex<&'a mut [RofiCmd]>>,
    recv_buffer: &'a  mut[RofiCmd],
    cmd_buffers: Vec<Mutex<RofiCmdBuffer<'a>>>,
    release_cmd: &'a RofiCmd,
    rofi_comm: Arc<RofiComm>,
    my_pe: usize,
    num_pes: usize,
}



impl<'a> RofiCQ<'a>{
    fn new(send_buffer_addr: usize, recv_buffer_addr: usize,  cmd_buffers_addrs: &Vec<Rc<Vec<usize>>>, release_cmd_addr:usize, rofi_comm: Arc<RofiComm>, my_pe: usize, num_pes: usize) -> RofiCQ<'a> {
        let mut cmd_buffers = vec![];
        for addrs in cmd_buffers_addrs.iter(){
            cmd_buffers.push(Mutex::new(RofiCmdBuffer::new(addrs.clone())));
        }
        RofiCQ{
            send_buffer: Arc::new(Mutex::new(unsafe {std::slice::from_raw_parts_mut(send_buffer_addr as *mut RofiCmd,num_pes)})),
            recv_buffer: unsafe {std::slice::from_raw_parts_mut(recv_buffer_addr as *mut RofiCmd,num_pes)},
            cmd_buffers: cmd_buffers,
            release_cmd: unsafe {&*(release_cmd_addr as *const RofiCmd)},
            rofi_comm: rofi_comm,
            my_pe: my_pe,
            num_pes: num_pes
        }
    }
    async fn send_data(&self, data: SerializedData, dst: usize) {
        loop{
            let mut cmd_buffer = self.cmd_buffers[dst].lock();
            if cmd_buffer.try_push(&data){
                let mut send_buf = self.send_buffer.lock(); //probably should make mutex for each pe?
                if send_buf[dst].daddr == 0 && send_buf[dst].dsize == 0 { // no open space to transfer
                    cmd_buffer.flush_buffer(&mut send_buf[dst]);
                    if send_buf[dst].dsize > 0{
                        self.rofi_comm.put(dst,send_buf[dst].as_bytes(),self.recv_buffer[dst].as_addr());
                    }
                }
                break;
            }
            drop(cmd_buffer);
            async_std::task::yield_now().await;
        }
    }
    async fn recv_data(&self, scheduler: Arc<Scheduler>) {
        loop{
            // let data_addrs=vec![];
            for src in 0..self.num_pes{
                if src != self.my_pe{
                    let cmd = self.recv_buffer[src];
                    if cmd.daddr != 0 && cmd.dsize != 0{
                        let mut data = self.rofi_comm.rt_alloc(cmd.dsize);
                        while data.is_none(){
                            async_std::task::yield_now().await;
                            data = self.rofi_comm.rt_alloc(cmd.dsize);
                        }
                        let data = data.unwrap();
                        let data_slice = unsafe{std::slice::from_raw_parts_mut(data as *mut u8,cmd.dsize)};
                        data_slice[cmd.dsize-1]=255;
                        self.rofi_comm.get(src,cmd.daddr,data_slice);
                        let send_buffer = self.send_buffer.clone();
                        let rofi_comm = self.rofi_comm.clone();
                        let release_cmd = self.release_cmd.as_addr();
                        let my_pe = self.my_pe;
                        let future = async move{
                            while data_slice[cmd.dsize-1] == 255{
                                async_std::task::yield_now().await;
                            }
                            // let sb = send_buffer.lock();
                            let release_cmd = unsafe{*(release_cmd as *const RofiCmd)};
                            rofi_comm.put(src,release_cmd.as_bytes(),0);//sb[my_pe].as_addr());
                            println!("here");
                        };
                        scheduler.submit_task(future);
                        // future.await;
                    }
                }
            }

        }
    }
}

struct RofiCommandQueue{
    send_buffer_addr: usize,
    recv_buffer_addr: usize,
    cmd_buffers_addrs: Vec<Rc<Vec<usize>>>,
    cq: Arc<RofiCQ<'static>>,
}

impl RofiCommandQueue{
    fn new(rofi_comm: Arc<RofiComm>,my_pe: usize, num_pes: usize) -> RofiCommandQueue {
        let send_buffer_addr = rofi_comm.rt_alloc(num_pes * std::mem::size_of::<RofiCmd>()).unwrap();
        let recv_buffer_addr = rofi_comm.rt_alloc(num_pes * std::mem::size_of::<RofiCmd>()).unwrap();
        let release_cmd_addr = rofi_comm.rt_alloc(std::mem::size_of::<RofiCmd>()).unwrap();
        let mut cmd_buffers_addrs = vec![];
        for pe in 0..num_pes{
            let mut addrs = vec![];
            for i in 0..CMD_BUFS_PER_PE{  
                addrs.push(rofi_comm.rt_alloc(CMD_BUF_LEN * std::mem::size_of::<RofiCmd>()).unwrap());
            }
            cmd_buffers_addrs.push(Rc::new(addrs));
        }
        // let cmd_buffers_addrs=Arc::new(cmd_buffers_addrs);
        let cq = RofiCQ::new(send_buffer_addr, recv_buffer_addr,&cmd_buffers_addrs.clone(),release_cmd_addr,rofi_comm.clone(),my_pe,num_pes);
        RofiCommandQueue{
            send_buffer_addr: send_buffer_addr,
            recv_buffer_addr: recv_buffer_addr,
            cmd_buffers_addrs: cmd_buffers_addrs,
            cq: Arc::new(cq),
        }
    }
}

// ////#[prof]
// impl RofiReqStatus {
//     fn as_bytes(&self) -> &[u8] {
//         let pointer = self as *const RofiReqStatus as *const u8;
//         let size = std::mem::size_of::<RofiReqStatus>();
//         let slice: &[u8] = unsafe { std::slice::from_raw_parts(pointer, size) };
//         slice
//     }
// }



// #[repr(C)]
// #[derive(Clone, Copy, Debug)]
// struct RofiReqInfo {
//     cmd: RofiCmd,
//     cmd_idx: usize,
//     data_cnt_idx: usize,
//     dst: usize,
//     id: usize,
// }
// unsafe impl Send for RofiReqInfo {}

// #[derive(Clone, Debug)]
// struct RofiReqBuffer<T: LamellarAlloc> {
//     alloc: T,
//     cmd_allocs: Vec<T>,
//     req_info: Arc<Mutex<Box<[RofiReqInfo]>>>,
//     rofi_base_addr: Arc<RwLock<usize>>,
//     status_base_addr: usize,
//     cmd_base_addr: usize,
// }

// ////#[prof]
// impl RofiCmd {
//     fn new(daddr: usize, dsize: usize, data_hash: usize, status_idx: u32) -> RofiCmd {
//         let mut cmd = RofiCmd {
//             daddr: daddr,
//             dsize: dsize,
//             data_hash: data_hash,
//             cmd_hash: 0,
//             status_idx: status_idx,
//         };
//         cmd.cmd_hash = cmd.calc_cmd_hash();
//         cmd
//     }
//     fn as_bytes(&self) -> &[u8] {
//         let pointer = self as *const RofiCmd as *const u8;
//         let size = std::mem::size_of::<RofiCmd>();
//         let slice: &[u8] = unsafe { std::slice::from_raw_parts(pointer, size) };
//         slice
//     }

//     fn update(&mut self, daddr: usize, dsize: usize, data_hash: usize, status_idx: u32) {
//         self.daddr = daddr;
//         self.dsize = dsize;
//         self.data_hash = data_hash;
//         self.status_idx = status_idx;
//         self.cmd_hash = self.calc_cmd_hash();
//     }
//     fn clear(&mut self) {
//         self.daddr = 0;
//         self.dsize = 0;
//         self.data_hash = 0;
//         self.cmd_hash = 0;
//         self.status_idx = 0;
//     }
//     fn calc_cmd_hash(&self) -> usize {
//         self.daddr as usize
//             + self.dsize as usize
//             + self.data_hash as usize
//             + self.status_idx as usize
//     }
// }

// ////#[prof]
// impl RofiReqInfo {
//     fn new(id: usize) -> RofiReqInfo {
//         RofiReqInfo {
//             cmd: RofiCmd::new(0, 0, 0, 0),
//             cmd_idx: 0, //remote index
//             data_cnt_idx: 0,
//             dst: 0,
//             id: id,
//         }
//     }
//     fn update(&mut self, cmd: RofiCmd, cmd_idx: usize, data_cnt_idx: usize, dst: usize, id: usize) {
//         self.cmd
//             .update(cmd.daddr, cmd.dsize, cmd.data_hash, cmd.status_idx);
//         self.cmd_idx = cmd_idx;
//         self.data_cnt_idx = data_cnt_idx;
//         self.dst = dst;
//         self.id = id;
//     }
//     fn clear(&mut self) {
//         self.cmd.clear();
//         self.cmd_idx = 0;
//         self.data_cnt_idx = 0;
//         self.dst = 0;
//         self.id = 0;
//     }
// }

// ////#[prof]
// impl<T: LamellarAlloc> RofiReqBuffer<T> {
//     fn new(rofi_base_addr: Arc<RwLock<usize>>, num_pes: usize) -> RofiReqBuffer<T> {
//         trace!("new RofiReqBuffer");
//         RofiReqBuffer {
//             alloc: T::new("req_buffer".to_string()), //location of req_info locally...
//             cmd_allocs: vec![],                      //location in remote nodes...
//             req_info: Arc::new(Mutex::new(
//                 vec![RofiReqInfo::new(0); CMD_Q_LEN * num_pes].into_boxed_slice(),
//             )),
//             rofi_base_addr: rofi_base_addr,
//             status_base_addr: 0,
//             cmd_base_addr: 0,
//         }
//     }

//     fn init(&mut self, rofi_comm: Arc<RofiComm>, my_pe: usize, num_pes: usize, num_entries: usize) {
//         self.status_base_addr = rofi_comm
//             .rt_alloc(num_entries * std::mem::size_of::<RofiReqStatus>())
//             .expect("error allocating request status buffers");
//         self.cmd_base_addr = rofi_comm
//             .rt_alloc(CMD_Q_LEN * num_pes * std::mem::size_of::<RofiCmd>())
//             .expect("error allocating cmd buffers");
//         // println!(
//         //     "RofiReqBuffer: status_base_addr: 0x{:x} cmd_base_addr 0x{:x} size: {:?}",
//         //     self.status_base_addr,
//         //     self.cmd_base_addr,
//         //     CMD_Q_LEN * num_pes * std::mem::size_of::<RofiCmd>()
//         //         + num_entries * std::mem::size_of::<RofiReqStatus>()
//         // );
//         self.alloc.init(0, num_entries);
//         if num_pes > 1 {
//             let num_cmds_slots_per_pe = CMD_Q_LEN; // / (num_pes - 1);
//             for i in 0..num_pes {
//                 // !!! note we create an empty allocation for my_pe
//                 // we short circuit local requests so the allocation for my_pe is never used
//                 // this implies we need to do some
//                 let start_idx = if i < my_pe {
//                     (my_pe - 1) * num_cmds_slots_per_pe
//                 } else {
//                     my_pe * num_cmds_slots_per_pe
//                 };

//                 let mut alloc = T::new("cmd".to_owned() + &i.to_string());
//                 if i != my_pe {
//                     alloc.init(start_idx, num_cmds_slots_per_pe);
//                 } else {
//                     alloc.init(0, 0);
//                 }
//                 self.cmd_allocs.push(alloc); //one per pe
//             }
//         }
//     }

//     fn add_request(
//         &self,
//         daddr: usize,
//         dsize: usize,
//         dhash: usize,
//         data_cnt_idx: usize,
//         dst: usize,
//         id: usize,
//         status: RofiReqStatus,
//     ) -> (usize, *const RofiCmd, usize) {
//         let idx = self.alloc.malloc(1); //location to store request locally
//         let cmd_idx = self //location to write into remotely
//             .cmd_allocs
//             .get(dst)
//             .expect("cmd allocs dst out of bounds")
//             .malloc(1);
//         let mut req_i = self.req_info.lock();
//         let cmd = RofiCmd::new(daddr, dsize, dhash, idx as u32);
//         req_i[idx].update(cmd, cmd_idx, data_cnt_idx, dst, id);
//         let cmd_ptr = &(req_i[idx].cmd) as *const RofiCmd;
//         drop(req_i);
//         unsafe {
//             *((*self.rofi_base_addr.read() + self.status_base_addr + idx) as *mut RofiReqStatus) =
//                 status
//         };
//         (self.get_cmd_addr(cmd_idx), cmd_ptr, cmd_idx)
//     }

//     fn get_status(&self, idx: usize) -> RofiReqStatus {
//         unsafe { *(self.get_status_addr(idx) as *const RofiReqStatus) }
//     }

//     fn get_status_addr(&self, idx: usize) -> usize {
//         *self.rofi_base_addr.read()
//             + self.status_base_addr
//             + idx * std::mem::size_of::<RofiReqStatus>()
//     }

//     fn get_cmd(&self, idx: usize) -> RofiCmd {
//         unsafe { *(self.get_cmd_addr(idx) as *const RofiCmd) }
//     }

//     fn clear_cmd(&self, idx: usize) {
//         unsafe { &(*(self.get_cmd_addr(idx) as *mut RofiCmd)).clear() };
//     }

//     fn get_cmd_addr(&self, idx: usize) -> usize {
//         *self.rofi_base_addr.read() + self.cmd_base_addr + idx * std::mem::size_of::<RofiCmd>()
//     }

//     fn free_request(&self, idx: usize) -> RofiReqInfo {
//         let mut req_info = self.req_info.lock();
//         let req = req_info
//             .get(idx)
//             .expect("req info entry out of bounds")
//             .clone();
//         req_info[idx].clear(); //the local request
//         drop(req_info);
//         assert_eq!(idx, req.cmd.status_idx as usize);
//         self.cmd_allocs
//             .get(req.dst)
//             .expect("cmd allocs dst out of bounds")
//             .free(req.cmd_idx as usize); //free my slot on the remote node
//         unsafe { *(self.get_status_addr(idx) as *mut RofiReqStatus) = RofiReqStatus::None };
//         self.alloc.free(idx); //free my local slot
//         req
//     }
// }

// struct RecvThread {
//     thread: Option<thread::JoinHandle<()>>,
//     id: usize,
//     num_threads: usize,
//     cmd_queue: Arc<RofiCommandQueueInternal>,
//     active: Arc<AtomicBool>,
// }

// ////#[prof]
// impl RecvThread {
//     fn run(&mut self) {
//         let cmd_queue = self.cmd_queue.clone();
//         let active = self.active.clone();
//         let id = self.id;
//         let num_threads = self.num_threads;
//         self.thread = Some(thread::spawn(move || {
//             let mut garbage: VecDeque<(Instant, MyPtr)> = VecDeque::new();

//             let mut pending_data_map: HashMap<usize, MyPtr> = HashMap::new();

//             trace!("[{:?}] command queue recv thread launched", cmd_queue.my_pe);
//             while active.load(Ordering::SeqCst)
//                 || cmd_queue.outstanding_msgs.load(Ordering::SeqCst) > 0
//             {
//                 cmd_queue.process_msgs(id, num_threads, &mut garbage, &mut pending_data_map);
//                 cmd_queue.process_completed_msgs(id, num_threads); //do we place this in its own thread as well?
//                                                                    // if timer.elapsed().as_millis() > 5000{
//                                                                    //     println!("tid:{:?} c0: {:?} c1: {:?} c2: {:?} c3: {:?}",std::thread::current().id(),cmd_queue.cnt0.load(Ordering::SeqCst),cmd_queue.cnt1.load(Ordering::SeqCst),cmd_queue.cnt2.load(Ordering::SeqCst),cmd_queue.cnt3.load(Ordering::SeqCst));
//                                                                    //     timer = std::time::Instant::now();
//                                                                    // }
//                                                                    // cmd_queue.garbage_collect(&mut garbage);
//             }
//             trace!(
//                 "[{:?}] command queue recv thread shutting down",
//                 cmd_queue.my_pe
//             );
//         }))
//     }
//     fn shutdown(&mut self) {
//         // trace!(
//         //     "[{:?}] shutting down command queue recv thread",
//         //     self.cmd_queue.my_pe
//         // );
//         self.active.store(false, Ordering::SeqCst);
//         let _res = self.thread.take().expect("error joining thread").join();
//         trace!(
//             "[{:?}] shutting down command queue recv thread",
//             self.cmd_queue.my_pe
//         );
//     }
// }

// struct MyPtr {
//     // ptr: *mut [u8],
//     addr: usize,
//     len: usize,
// }
// unsafe impl Sync for MyPtr {}
// unsafe impl Send for MyPtr {}

// struct RofiCommandQueueInternal {
//     num_pes: usize,
//     my_pe: usize,
//     rofi_comm: Arc<RofiComm>,
//     req_buffer: RofiReqBuffer<ObjAlloc<u8>>, // am status slots (in rofi memory)
//     data_counts_alloc: ObjAlloc<u8>,         // multi message counters (in lamellar memory)
//     data_counts: Vec<AtomicUsize>,
//     msg_tx: crossbeam::channel::Sender<Arc<Vec<u8>>>,
//     outstanding_msgs: Arc<AtomicUsize>,
//     sent_data_amt: Arc<AtomicUsize>,
//     // cnt0: Arc<AtomicUsize>,
//     // cnt1: Arc<AtomicUsize>,
//     // cnt2: Arc<AtomicUsize>,
//     // cnt3: Arc<AtomicUsize>,
// }

// ////#[prof]
// impl Drop for RofiCommandQueueInternal {
//     fn drop(&mut self) {
//         trace!("[{:?}] RofiCommandQueueInternal Dropping", self.my_pe);
//         // let mut string = String::new();
//     }
// }

// enum DataType{
//     Vec(Vec<u8>),
//     Ser(SerializedData),
// }
// #[prof]
// impl RofiCommandQueueInternal {
//     pub(crate) fn init(&mut self) {
//         self.req_buffer.init(
//             self.rofi_comm.clone(),
//             self.my_pe,
//             self.num_pes,
//             CMD_Q_LEN * self.num_pes,
//         );
//         self.data_counts_alloc.init(0, CMD_Q_LEN * self.num_pes);
//         for _i in 0..CMD_Q_LEN * self.num_pes {
//             self.data_counts.push(AtomicUsize::new(0));
//         }
//     }
//     pub(crate) fn send_data(
//         &self,
//         data: DataType,
//         team_iter: Box<dyn Iterator<Item = usize> + Send>,
//     ) -> Vec<usize> {
//         // allocate memory in our local data segment
//         prof_start!(data_alloc);
//         let (daddr,dsize,dhash) = match data {
//             DataType::Vec(data) => {
//                 let daddr = self
//                 .rofi_comm
//                 .rt_alloc(data.len())// + 1)
//                 .expect("error allocating rt_memory");
//                 prof_end!(data_alloc);
//                 prof_start!(data_copy);
//                 self.rofi_comm.local_store(&data, daddr);
//                 // let dummy = [27u8];
//                 // self.rofi_comm.local_store(&dummy, daddr + data.len());
//                 prof_end!(data_copy);
//                 let dhash = data.iter().fold(0usize, |a, b| a + (*b as usize));// + dummy[0] as usize;
//                 (daddr,data.len(),dhash)
//             }
//             DataType::Ser(data) => {
//                 let dhash = unsafe{std::slice::from_raw_parts(data.addr as *const u8,data.len).iter().fold(0usize, |a, b| a + (*b as usize))};
//                 (data.addr,data.len,dhash)
//             }
//         };
        
//         // println!("past allocs");
//         //-----------------
//         // allocate a data_count object
//         let data_cnt_idx = self.data_counts_alloc.malloc(1);
//         //----------------------
        
//         // let dsize = data.len() + 1;
//         let mut status_indices = vec![];
//         prof_start!(data_send);
//         for dst in team_iter {
//             if dst != self.my_pe {
//                 prof_start!(prep1);
//                 self.outstanding_msgs.fetch_add(1, Ordering::SeqCst);
//                 self.sent_data_amt.fetch_add(dsize, Ordering::SeqCst);
//                 self.data_counts[data_cnt_idx].fetch_add(1, Ordering::SeqCst);
//                 prof_end!(prep1);
//                 prof_start!(prep2);
//                 let (dst_cmd_addr, cmd_msg_ptr, _cmd_idx) = self.req_buffer.add_request(
//                     daddr,
//                     dsize,
//                     dhash,
//                     data_cnt_idx,
//                     dst,
//                     0, //id argument currently unused.
//                     RofiReqStatus::Init,
//                 );
//                 prof_end!(prep2);
//                 prof_start!(prep3);
//                 trace!(
//                     "[{:?}] sending to [{:?}] -- daddr: {:?} dst_cmd_addr: {:?} dhash{:?}",
//                     self.my_pe,
//                     dst,
//                     daddr,
//                     dst_cmd_addr,
//                     dhash
//                 );
//                 unsafe {
//                     status_indices.push((*cmd_msg_ptr).status_idx as usize);
//                 }
//                 let src_cmd_slice = unsafe { (*cmd_msg_ptr).as_bytes() };
//                 prof_end!(prep3);
//                 prof_start!(put);
//                 self.rofi_comm.put(dst, src_cmd_slice, dst_cmd_addr);
//                 prof_end!(put);
//                 trace!("[{:?}] put data", self.my_pe);
//             }
//             // self.cnt0.fetch_add(1,Ordering::SeqCst);
//         }
//         prof_end!(data_send);
//         status_indices
//     }

//     fn process_msgs(
//         &self,
//         id: usize,
//         num_threads: usize,
//         _garbage: &mut VecDeque<(Instant, MyPtr)>,
//         pending_data_map: &mut HashMap<usize, MyPtr>,
//     ) {
//         let num_cmds_slots_per_pe = CMD_Q_LEN;
//         for entry in (id..(CMD_Q_LEN * self.num_pes)).step_by(num_threads) {
//             let cmd = self.req_buffer.get_cmd(entry);
//             if cmd.dsize != 0 && cmd.cmd_hash == cmd.calc_cmd_hash() {
//                 let mut src = entry / num_cmds_slots_per_pe;
//                 //account for not allocating cmd slots for my_pe
//                 if src >= self.my_pe {
//                     src += 1;
//                 }
//                 if !pending_data_map.contains_key(&entry) {
//                     let data_addr = loop {
//                         if let Some(data_addr) = self.rofi_comm.rt_alloc(cmd.dsize) {
//                             // this acutally needs to be a command_queue specific allocator...
//                             break data_addr + self.rofi_comm.base_addr();
//                         }
//                         std::thread::yield_now();
//                     };
//                     let mut data =
//                         unsafe { std::slice::from_raw_parts_mut(data_addr as *mut u8, cmd.dsize) };
//                     self.rofi_comm.iget_relative(src, cmd.daddr, &mut data);
//                     pending_data_map.insert(
//                         entry,
//                         MyPtr {
//                             addr: data_addr,
//                             len: cmd.dsize,
//                         },
//                     );
//                     // self.cnt1.fetch_add(1,Ordering::SeqCst);
//                 } else {
//                     // hopefully  our get request has finished...
//                     if let Some(data_ptr) = pending_data_map.remove(&entry) {
//                         let data = unsafe {
//                             // Box::from_raw(data_ptr.ptr)
//                             std::slice::from_raw_parts_mut(data_ptr.addr as *mut u8, data_ptr.len)
//                         };

//                         if cmd.data_hash == data.iter().fold(0usize, |a, b| a + (*b as usize)) {
//                             self.req_buffer.clear_cmd(entry);
//                             self.rofi_comm.put(
//                                 src,
//                                 FINI_STATUS.as_bytes(),
//                                 self.req_buffer.get_status_addr(cmd.status_idx as usize),
//                             );
//                             let mut dvec = data.to_vec();
//                             // dvec.pop();
//                             let advec = Arc::new(dvec);
//                             self.msg_tx.send(advec).expect("error in sending rofi msg");

//                             // let data_ptr_n = Box::into_raw(data);
//                             // if data_ptr_n != data_ptr.ptr {
//                             //     println!(
//                             //         "UHHHHHH OHHHHHH {:x} {:x}",
//                             //         data_ptr.ptr as *const u8 as usize,
//                             //         data_ptr_n as *const u8 as usize
//                             //     );
//                             // }
//                             self.rofi_comm
//                                 .rt_free(data_ptr.addr - self.rofi_comm.base_addr());
//                             // self.cnt2.fetch_add(1,Ordering::SeqCst);
//                             // garbage.push_back((Instant::now(), data_ptr));
//                         } else {
//                             // let data_ptr_n = Box::into_raw(data);
//                             // pending_data_map.insert(entry, MyPtr { ptr: data_ptr_n });
//                             pending_data_map.insert(entry, data_ptr);
//                         }
//                     } else {
//                         println!("ERROR data buf not found for entry: {:?}", entry);
//                     }
//                 }
//             }
//         }
//     }
//     fn process_completed_msgs(&self, id: usize, num_threads: usize) {
//         for entry in (id..CMD_Q_LEN * self.num_pes).step_by(num_threads) {
//             let status = self.req_buffer.get_status(entry);
//             if status == RofiReqStatus::Fini {
//                 let req_info = self.req_buffer.free_request(entry);

//                 let current_count =
//                     self.data_counts[req_info.data_cnt_idx].fetch_sub(1, Ordering::SeqCst);
//                 self.outstanding_msgs.fetch_sub(1, Ordering::SeqCst);
//                 if current_count == 1 {
//                     //we can't free the data allocation of idx allocation until all requests accessing this data have completed
//                     // self.data_allocs.free(req_info.cmd.daddr);
//                     self.rofi_comm.rt_free(req_info.cmd.daddr);
//                     self.data_counts_alloc.free(req_info.data_cnt_idx);
//                     // self.cnt3.fetch_add(1,Ordering::SeqCst);
//                 }
//             }
//         }
//     }
// }

// pub(crate) struct RofiCommandQueue {
//     threads: Vec<RecvThread>,
//     internal: Arc<RofiCommandQueueInternal>,
// }

// ////#[prof]
// impl RofiCommandQueue {
//     pub(crate) fn mem_per_pe() -> usize {
//         2 * CMD_Q_LEN * std::mem::size_of::<RofiCmd>()
//     }
//     pub(crate) fn new(
//         msg_tx: crossbeam::channel::Sender<Arc<Vec<u8>>>,
//         rofi_comm: Arc<RofiComm>,
//     ) -> RofiCommandQueue {
//         let mut internal = RofiCommandQueueInternal {
//             num_pes: rofi_comm.num_pes,
//             my_pe: rofi_comm.my_pe,
//             rofi_comm: rofi_comm.clone(),
//             // data_allocs: BTreeAlloc::new("data".to_string()),
//             req_buffer: RofiReqBuffer::<ObjAlloc<u8>>::new(
//                 rofi_comm.rofi_base_address.clone(),
//                 rofi_comm.num_pes,
//             ),
//             data_counts: Vec::new(),
//             data_counts_alloc: ObjAlloc::new("data_counts".to_string()),
//             // pending_data_map: Arc::new(chashmap::CHashMap::new()),
//             // pending_data_map: Arc::new(RwLock::new(HashMap::new())),
//             msg_tx: msg_tx,
//             outstanding_msgs: Arc::new(AtomicUsize::new(0)),
//             sent_data_amt: Arc::new(AtomicUsize::new(0)),
//             // cnt0:  Arc::new(AtomicUsize::new(0)),
//             // cnt1:  Arc::new(AtomicUsize::new(0)),
//             // cnt2:  Arc::new(AtomicUsize::new(0)),
//             // cnt3:  Arc::new(AtomicUsize::new(0)),
//             // garbage_collector: ArrayQueue::new(100000),
//         };
//         internal.init();
//         let mut cmd_queue = RofiCommandQueue {
//             threads: Vec::new(),
//             internal: Arc::new(internal),
//         };
//         let num_threads = 1;
//         for i in 0..num_threads {
//             let mut thread = RecvThread {
//                 thread: None,
//                 id: i,
//                 num_threads: num_threads,
//                 cmd_queue: cmd_queue.internal.clone(),
//                 active: Arc::new(AtomicBool::new(true)),
//             };
//             thread.run();
//             cmd_queue.threads.push(thread)
//         }
//         // let addr = rofi_comm.rt_alloc(1).expect("error allocating memory");
//         // println!(
//         //     "after init current address: {:x} {:x}",
//         //     addr,
//         //     addr + rofi_comm.base_addr()
//         // );
//         cmd_queue
//     }
//     pub(crate) fn finit(&self) {
//         self.internal.rofi_comm.finit();
//     }

//     pub(crate) fn send_data(
//         &self,
//         data: Vec<u8>,
//         team_iter: Box<dyn Iterator<Item = usize> + Send>,
//     ) -> Vec<usize> {
//         self.internal.send_data(DataType::Vec(data), team_iter)
//     }

//     pub(crate) fn send_data_2(
//         &self,
//         data: SerializedData,
//         team_iter: Box<dyn Iterator<Item = usize> + Send>,
//     ) -> Vec<usize> {
//         self.internal.send_data(DataType::Ser(data), team_iter)
//     }

//     pub(crate) fn my_pe(&self) -> usize {
//         self.internal.rofi_comm.my_pe
//     }

//     pub(crate) fn num_pes(&self) -> usize {
//         self.internal.rofi_comm.num_pes
//     }
//     pub(crate) fn barrier(&self) {
//         self.internal.rofi_comm.barrier();
//     }
//     pub(crate) fn data_sent(&self) -> usize {
//         self.internal.sent_data_amt.load(Ordering::SeqCst)
//     }
// }

// ////#[prof]
// impl Drop for RofiCommandQueue {
//     fn drop(&mut self) {
//         while let Some(mut iothread) = self.threads.pop() {
//             iothread.shutdown();
//         }
//         trace!(
//             "[{:?}] RofiCommandQueue Dropping",
//             self.internal.rofi_comm.my_pe
//         );
//     }
// }

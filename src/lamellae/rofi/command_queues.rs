

use crate::lamellae::rofi::{rofi_comm::{RofiData,RofiComm}};
// use crate::lamellae::rofi_lamellae::RofiData;
use crate::lamellae::{Lamellae, LamellaeComm,SerializedData,Des};
use crate::scheduler::{Scheduler,SchedulerQueue};
// use lamellar_prof::*;
// use log::trace;
use parking_lot::{Mutex};

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize,AtomicBool,  Ordering};
use std::sync::{Arc};

const CMD_BUF_LEN: usize = 50000; // this is the number of slots for each PE
                                // const NUM_REQ_SLOTS: usize = CMD_Q_LEN; // max requests at any given time -- probably have this be a multiple of num PES
const CMD_BUFS_PER_PE: usize = 2;

#[repr(C)]
#[derive(Clone, Copy)]
struct RofiCmd { // we send this to remote nodes
    daddr: usize, 
    dsize: usize,
    cmd: Cmd,
    msg_hash: usize,
    cmd_hash: usize,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug,Eq,PartialEq )]
enum Cmd{
    Clear = 1,
    Free,
    Release,
    Print,
    Tx,
}

fn calc_hash(addr: usize, len: usize) -> usize {
    //we split into a u64 slice and a u8 slice as u64 seems to compute faster.
    let num_u64s = len/std::mem::size_of::<u64>();
    let u64_slice =  unsafe {std::slice::from_raw_parts(addr as *const u64,num_u64s)};
    let num_u8s = len%std::mem::size_of::<u64>();
    let u8_slice =  unsafe {std::slice::from_raw_parts((addr+num_u64s*std::mem::size_of::<u64>()) as *const u8, num_u8s)};
    u64_slice.iter().map(|x| *x as usize).sum::<usize>() + u8_slice.iter().map(|x| *x as usize).sum::<usize>()
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
    fn cmd_as_bytes(&self) -> &[u8]{
        let pointer = &self.cmd as *const Cmd as *const u8;
        let size = std::mem::size_of::<Cmd>();
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(pointer,size) };
        slice
    }
    // fn cmd_as_addr(&self)->usize{
    //     &(self.cmd) as *const Cmd as usize 
    // }
    fn hash(&self) -> usize{
        self.daddr+self.dsize + self.cmd as usize + self.msg_hash
    }
    fn calc_hash(&mut self){
        self.cmd_hash = self.hash()
    }
    fn check_hash(&self) -> bool{
        if self.cmd_hash == self.hash(){
            true
        }
        else{
            false
        }
    }
}

impl std::fmt::Debug for RofiCmd{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "daddr {:#x}({:?}) dsize {:?} cmd {:?} msg_hash {:?} cmd_hash {:?}",
            self.daddr,
            self.daddr,
            self.dsize,
            self.cmd,
            self.msg_hash,
            self.cmd_hash,
        )
    }
}

struct CmdBuf{
    buf: Box<[RofiCmd]>,
    addr: usize,
    base_addr: usize,
    index: usize,
    allocated_cnt: usize,
    max_size: usize,
}

impl CmdBuf{
    fn push(&mut self, daddr: usize, dsize: usize, hash: usize){
        if daddr==0 || dsize ==0{
            panic!("this shouldnt happen! {:?} {:?}",daddr,dsize);
        }
        let mut cmd = unsafe {self.buf.get_unchecked_mut(self.index)};
        cmd.daddr=daddr;
        cmd.dsize=dsize;
        cmd.cmd=Cmd::Tx;
        cmd.msg_hash = hash;
        cmd.calc_hash();
        self.index +=1;
        if dsize > 0{
            self.allocated_cnt+=1;
        }
        // println!("pushed cmd {:?}",cmd);
        // self.hash += cmd.hash() + cmd.cmd_hash;
    }
    fn full(&self) -> bool{
        self.index==self.max_size
    }
    fn reset(&mut self){
        self.index = 0;
        self.allocated_cnt=0;
        // self.hash = 0;
    }
    fn addr(&self)->usize{
        self.addr
    }
    fn size(&self)->usize{
        self.index * std::mem::size_of::<RofiCmd>()
    }
    fn iter(&self) -> std::slice::Iter<RofiCmd>{
        self.buf[0..self.index].iter()
    }
    fn state(&self)-> Cmd{
        self.buf[0].cmd
    }
}

impl std::fmt::Debug for CmdBuf{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "addr {:#x}({:?}) hash {:?} index {:?} a_cnt {:?}",
            self.addr,
            self.addr,
            calc_hash(self.addr+self.base_addr,self.size()),
            self.index,
            self.allocated_cnt
        )
    }
}

impl Drop for CmdBuf{
    fn drop(&mut self){
        // println!("dropping cmd buf");
        let old=std::mem::take(&mut self.buf);
        Box::into_raw(old);
        // println!("dropped cmd buf");

    }
}

struct RofiCmdBuffer{
    empty_bufs: Vec<CmdBuf>,
    full_bufs: Vec<CmdBuf>,
    tx_bufs: HashMap<usize,CmdBuf>,
    waiting_bufs: HashMap<usize,CmdBuf>,
    cur_buf: Option<CmdBuf>,
    num_bufs: usize,
    pe: usize,
    base_addr: usize,
    
}

impl RofiCmdBuffer {
    fn new(addrs: Arc<Vec<usize>>,base_addr: usize,pe: usize) -> RofiCmdBuffer{
        let mut bufs = vec![];
        for addr in addrs.iter(){
            // println!("RofiCmdBuffer {:x} {:x} {:x}",addr,base_addr,*addr+base_addr);
            bufs.push(CmdBuf{ 
                    buf: unsafe {Box::from_raw(std::ptr::slice_from_raw_parts_mut((*addr + base_addr) as *mut RofiCmd, CMD_BUF_LEN))},
                    addr: *addr,
                    base_addr: base_addr,
                    index: 0,
                    allocated_cnt: 0,
                    max_size: CMD_BUF_LEN,
                }
            );
        }
        RofiCmdBuffer{
            empty_bufs: bufs,
            full_bufs: Vec::new(),
            tx_bufs: HashMap::new(),
            waiting_bufs: HashMap::new(),
            cur_buf: None,
            num_bufs: addrs.len(),
            pe: pe,
            base_addr: base_addr,
        }
    }
    fn try_push(&mut self, addr: usize, len: usize, hash: usize )-> bool{
        
            if self.cur_buf.is_none() {
                self.cur_buf= self.empty_bufs.pop();
            }
            if let Some(buf) = &mut self.cur_buf{
                buf.push(addr,len, hash);
                if buf.full(){

                    self.full_bufs.push(std::mem::replace(&mut self.cur_buf,None).unwrap());
                }
                true
            }
            else{
                false
            }  
    }
    fn flush_buffer(&mut self,  cmd: &mut RofiCmd) {
        // println!("flush buffer before: {:?}",self);
        if let Some(buf) = self.full_bufs.pop(){
            // println!("flushing {:?} {:?}",buf.addr(),buf.size());
            cmd.daddr = buf.addr();
            cmd.dsize = buf.size();
            cmd.cmd = Cmd::Tx;
            cmd.msg_hash = calc_hash(cmd.daddr+self.base_addr,cmd.dsize);
            cmd.calc_hash();
            self.tx_bufs.insert(buf.addr(),buf);
            
        }
        else if let Some(buf) = std::mem::replace(&mut self.cur_buf,None) {
            // println!("flushing {:?} {:?}",buf.addr(),buf.size());
            cmd.daddr = buf.addr();
            cmd.dsize = buf.size();// + std::mem::size_of::<usize>();
            cmd.cmd = Cmd::Tx;
            cmd.msg_hash = calc_hash(cmd.daddr+self.base_addr,cmd.dsize);
            cmd.calc_hash();
            self.tx_bufs.insert(buf.addr(),buf);
            
        }
        // else {} all buffers are currently busy
        // println!("flush buffer after: {:?}",self);
    }

    fn check_transfer(&mut self, addr: usize) -> bool{
        if let Some(buf) = self.tx_bufs.remove(&addr){
            match buf.state(){
                Cmd::Tx => {
                    self.tx_bufs.insert(addr, buf);
                    false
                }
                Cmd::Release | Cmd::Free => {//free is valid still but we need to make sure we release the send_buffer
                //     println!("transfer complete!!");
                    if buf.allocated_cnt > 0 {
                        self.waiting_bufs.insert(buf.addr(),buf);
                        true
                    }
                    else{
                        panic!("am I ever here?");
                    }
                }
                Cmd::Clear => panic!("should not clear before release"),
                // Cmd::Free => panic!("should not free before release"),
                Cmd::Print => panic!("shoud not encounter Print here"),
            }
        }else{
            false
        }
    }

    fn check_free_data(&mut self, rofi_comm: Arc<RofiComm>) {
        let mut freed_bufs = vec![];

        for (addr,buf) in self.waiting_bufs.iter(){
            match buf.state(){
                Cmd::Release => { }, //do nothing
                Cmd::Free => freed_bufs.push(*addr),
                Cmd::Tx => panic!{"should not be transerring if in waiting bufs"},
                Cmd::Clear => panic!("should not clear before release"),
                Cmd::Print => panic!("shoud not encounter Print here"),
            }
        }
        for buf_addr in freed_bufs {
            if let Some(mut buf) = self.waiting_bufs.remove(&buf_addr){
                for cmd in buf.iter() {
                    if cmd.dsize > 0{
                        let ref_cnt_addr = cmd.daddr as usize-std::mem::size_of::<AtomicUsize>()+rofi_comm.base_addr();
                        let cnt = unsafe{(*(ref_cnt_addr as *const AtomicUsize)).fetch_sub(1,Ordering::SeqCst)};
                        if cnt == 1 {
                            rofi_comm.rt_free(ref_cnt_addr-rofi_comm.base_addr());
                        }
                    }
                }
                buf.reset();
                self.empty_bufs.push(buf);
            }
            else{
                println!("free_data should this be possible?");
            }
        }
    }
    
    // fn free_data(&mut self, buf_addr: usize,rofi_comm: Arc<RofiComm>) -> bool {
    //     if let Some(mut buf) = self.waiting_bufs.remove(&buf_addr){
    //         for cmd in buf.iter() {
    //             if cmd.dsize > 0{
    //                 let ref_cnt_addr = cmd.daddr as usize-std::mem::size_of::<AtomicUsize>()+rofi_comm.base_addr();
    //                 let cnt = unsafe{(*(ref_cnt_addr as *const AtomicUsize)).fetch_sub(1,Ordering::SeqCst)};
    //                 if cnt == 1 {
    //                     rofi_comm.rt_free(ref_cnt_addr-rofi_comm.base_addr());
    //                 }
    //             }
    //         }
    //         buf.reset();
    //         self.empty_bufs.push(buf);
    //         true
    //     }
    //     else{
    //         println!("free_data should this be possible?");
    //         false
    //     }
    // }
    fn empty(&self) -> bool{
        self.empty_bufs.len() == self.num_bufs
    }
   
}

impl std::fmt::Debug for RofiCmdBuffer{
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

impl Drop for RofiCmdBuffer{
    fn drop(&mut self) {
        // println!("dropping RofiCmdBuffer");
        while !self.empty() {std::thread::yield_now()}
        self.empty_bufs.clear();
        // println!("dropped RofiCmdBuffer");
    }
}

struct RofiCQ{
    send_buffer: Arc<Mutex<Box<[RofiCmd]>>>,
    recv_buffer: Arc<Mutex<Box<[RofiCmd]>>>,
    free_buffer: Arc<Mutex<Box<[RofiCmd]>>>,
    cmd_buffers: Vec<Mutex<RofiCmdBuffer>>,
    release_cmd: Arc<Box<RofiCmd>>,
    clear_cmd: Arc<Box<RofiCmd>>,
    free_cmd: Arc<Box<RofiCmd>>,
    rofi_comm: Arc<RofiComm>,
    my_pe: usize,
    _num_pes: usize,
    send_waiting: Vec<Arc<AtomicBool>>,
    pending_cmds: Arc<AtomicUsize>,
    sent_cnt: Arc<AtomicUsize>,
    recv_cnt: Arc<AtomicUsize>,
    put_amt: Arc<AtomicUsize>,
    get_amt: Arc<AtomicUsize>,
}



impl RofiCQ{
    fn new(send_buffer_addr: usize, recv_buffer_addr: usize, free_buffer_addr: usize,  cmd_buffers_addrs: &Vec<Arc<Vec<usize>>>, release_cmd_addr:usize, clear_cmd_addr:usize, free_cmd_addr:usize,rofi_comm: Arc<RofiComm>, my_pe: usize, num_pes: usize) -> RofiCQ {
        let mut cmd_buffers = vec![];
        let mut pe =0;
        for addrs in cmd_buffers_addrs.iter(){
            cmd_buffers.push(Mutex::new(RofiCmdBuffer::new(addrs.clone(),rofi_comm.base_addr(),pe)));
            pe += 1;
        }
        let mut send_buffer = unsafe {Box::from_raw(std::ptr::slice_from_raw_parts_mut(send_buffer_addr as *mut RofiCmd,num_pes))};
        for cmd in send_buffer.iter_mut(){
            (*cmd).daddr=0;
            (*cmd).dsize=0;
            (*cmd).cmd=Cmd::Clear;
            (*cmd).msg_hash=0;
            (*cmd).calc_hash();
        }

        let mut recv_buffer = unsafe {Box::from_raw(std::ptr::slice_from_raw_parts_mut(recv_buffer_addr as *mut RofiCmd,num_pes))};
        for cmd in recv_buffer.iter_mut(){
            (*cmd).daddr=0;
            (*cmd).dsize=0;
            (*cmd).cmd=Cmd::Clear;
            (*cmd).msg_hash=0;
            (*cmd).calc_hash();
        }

        let mut free_buffer = unsafe {Box::from_raw(std::ptr::slice_from_raw_parts_mut(free_buffer_addr as *mut RofiCmd,num_pes))};
        for cmd in free_buffer.iter_mut(){
            (*cmd).daddr=0;
            (*cmd).dsize=0;
            (*cmd).cmd=Cmd::Clear;
            (*cmd).msg_hash=0;
            (*cmd).calc_hash();
        }

        let mut release_cmd = unsafe {Box::from_raw(release_cmd_addr as *mut RofiCmd)};
        release_cmd.daddr=1;
        release_cmd.dsize=1;
        release_cmd.cmd=Cmd::Release;
        release_cmd.msg_hash=1;
        release_cmd.calc_hash();

        let mut clear_cmd = unsafe {Box::from_raw(clear_cmd_addr as *mut RofiCmd)};
        clear_cmd.daddr=0;
        clear_cmd.dsize=0;
        clear_cmd.cmd=Cmd::Clear;
        clear_cmd.msg_hash=0;
        clear_cmd.calc_hash();

        let mut free_cmd = unsafe {Box::from_raw(free_cmd_addr as *mut RofiCmd)};
        free_cmd.daddr=0;
        free_cmd.dsize=0;
        free_cmd.cmd=Cmd::Free;
        free_cmd.msg_hash=0;
        free_cmd.calc_hash();

        let mut send_waiting = vec![];
        for _pe in 0..num_pes{
            send_waiting.push(Arc::new(AtomicBool::new(false)));
        }
        RofiCQ{
            send_buffer: Arc::new(Mutex::new(send_buffer)),
            recv_buffer: Arc::new(Mutex::new(recv_buffer)),
            free_buffer: Arc::new(Mutex::new(free_buffer)),
            cmd_buffers: cmd_buffers,
            release_cmd: Arc::new(release_cmd),
            clear_cmd: Arc::new(clear_cmd),
            free_cmd: Arc::new(free_cmd),
            rofi_comm: rofi_comm,
            my_pe: my_pe,
            _num_pes: num_pes,
            send_waiting: send_waiting,
            pending_cmds: Arc::new(AtomicUsize::new(0)),
            sent_cnt: Arc::new(AtomicUsize::new(0)),
            recv_cnt: Arc::new(AtomicUsize::new(0)),
            put_amt: Arc::new(AtomicUsize::new(0)),
            get_amt: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn empty(&self) -> bool{
        for buf in &self.cmd_buffers{
            let buf = buf.lock();
            if !buf.empty(){
                return false;
            }
        }
        true
    }

    fn ready(&self, src: usize) -> Option<RofiCmd> {
        let mut recv_buffer = self.recv_buffer.lock();
        let cmd = recv_buffer[src].clone();
        if cmd.check_hash(){
            match cmd.cmd{
                Cmd::Clear  => None,
                // Cmd::Free => panic!("should not see free in recv buffer"),
                Cmd::Tx | Cmd::Print | Cmd::Free | Cmd::Release => {
                    // println!("received cmd src {:?} {:?} ",src,cmd);
                    let res = Some(cmd);
                    let cmd = &mut recv_buffer[src];
                    cmd.daddr=0;
                    cmd.dsize=0;
                    cmd.cmd = Cmd::Clear;
                    cmd.msg_hash =0;
                    cmd.calc_hash();
                    res
                },
            }
        }
        else {
            None
        }
        //     let mut free_buffer = self.free_buffer.lock();
        //     let cmd = free_buffer[src].clone();
        //     if cmd.check_hash(){
        //         match cmd.cmd{
        //             Cmd::Tx | Cmd::Clear | Cmd::Print | Cmd::Release  => panic!("should not see anything but free"),
        //             Cmd::Free => {
        //                 // println!("received cmd src {:?} {:?} ",src,cmd);
        //                 let res = Some(cmd);
        //                 let cmd = &mut free_buffer[src];
        //                 cmd.daddr=0;
        //                 cmd.dsize=0;
        //                 cmd.cmd = Cmd::Clear;
        //                 cmd.msg_hash =0;
        //                 cmd.calc_hash();
        //                 res
        //             },
        //         }
        //     }
        //     else{
        //         None
        //     }
        // }
    }

    fn try_sending_buffer(&self,dst: usize, cmd_buffer: &mut RofiCmdBuffer) -> bool{
        if self.pending_cmds.load(Ordering::SeqCst) == 0 || cmd_buffer.full_bufs.len() > 0{
            let mut send_buf = self.send_buffer.lock();
            if send_buf[dst].hash() == self.clear_cmd.hash() {
                cmd_buffer.flush_buffer(&mut send_buf[dst]);
                // println!{"send_buf  after{:?}",send_buf};
                if send_buf[dst].dsize > 0{
                    let recv_buffer = self.recv_buffer.lock();
                    // println!{"sending data to dst {:?} {:?} {:?} {:?}",recv_buffer[self.my_pe].as_addr()-self.rofi_comm.base_addr(),send_buf[dst],send_buf[dst].as_bytes(),send_buf};
                    // println!("sending cmd {:?}",send_buf);
                    self.rofi_comm.put(dst,send_buf[dst].as_bytes(),recv_buffer[self.my_pe].as_addr());
                    self.put_amt.fetch_add(send_buf[dst].as_bytes().len(),Ordering::Relaxed);
                }
                true
            }
            else{
                false
            }
        }
        else{
            false
        }
    }

    fn progress_transfers(&self,dst: usize, cmd_buffer: &mut RofiCmdBuffer){
        let mut send_buf = self.send_buffer.lock();
        if cmd_buffer.check_transfer(send_buf[dst].daddr){
            send_buf[dst].daddr=0;
            send_buf[dst].dsize=0;
            send_buf[dst].cmd = Cmd::Clear;
            send_buf[dst].msg_hash =0;
            send_buf[dst].calc_hash();
        }
        cmd_buffer.check_free_data(self.rofi_comm.clone());
    }

    async fn send(&self, addr: usize, len: usize, dst: usize, hash: usize){
        let mut timer=std::time::Instant::now();
        self.pending_cmds.fetch_add(1,Ordering::SeqCst);
        loop{
            {//this is to tell the compiler we wont hold the mutex lock if we have to yield
                let mut cmd_buffer = self.cmd_buffers[dst].lock();
                if cmd_buffer.try_push(addr,len,hash){
                    // let data_slice = unsafe{ std::slice::from_raw_parts((addr + self.rofi_comm.base_addr()) as *const u8, len) };
                    self.sent_cnt.fetch_add(1,Ordering::SeqCst);
                    self.put_amt.fetch_add(len,Ordering::Relaxed);
                    let _cnt = self.pending_cmds.fetch_sub(1,Ordering::SeqCst);
                    // println!("pushed {:?} {:?} {:?} {:?}",addr,len,hash,_cnt);//, data_slice);
                    // println!("cmd_buffer {:?}",cmd_buffer);
                    break;
                }
                //while we are waiting to push our data might as well try to advance the buffers
                self.progress_transfers(dst,&mut cmd_buffer);
                self.try_sending_buffer(dst,&mut cmd_buffer);
                if timer.elapsed().as_secs_f64() > 60.0 {
                    let send_buf = self.send_buffer.lock();                    
                    println!("waiting to add cmd to cmd buffer {:?}",cmd_buffer);
                    println!("send_buf: {:?}",send_buf);
                    // drop(send_buf);
                    let recv_buf = self.recv_buffer.lock();                    
                    println!("recv_buf: {:?}",recv_buf);
                    let free_buf = self.free_buffer.lock();                    
                    println!("free_buf: {:?}",free_buf);
                    timer=std::time::Instant::now();
                }                
            }
            async_std::task::yield_now().await;
            
        }
        let mut im_waiting = false;
        loop{
            {//this is to tell the compiler we wont hold the mutex lock if we have to yield
                let mut cmd_buffer = self.cmd_buffers[dst].lock();
                if !cmd_buffer.empty(){ //data to send
                    self.progress_transfers(dst,&mut cmd_buffer);
                    if self.try_sending_buffer(dst,&mut cmd_buffer){
                        self.send_waiting[dst].store(false,Ordering::SeqCst); 
                        break;
                    }
                }
                else{
                    self.send_waiting[dst].store(false,Ordering::SeqCst);
                    break;
                }
                if !im_waiting{ 
                    if let Ok(_) = self.send_waiting[dst].compare_exchange_weak(false,true, Ordering::SeqCst, Ordering::Relaxed) { //ensure only a single task is waiting per destination
                        im_waiting = true;
                    }
                    else{
                        break;
                    }
                }
                if timer.elapsed().as_secs_f64() > 60.0 {
                    println!("waiting to send cmd buffer {:?}",cmd_buffer);
                    let send_buf = self.send_buffer.lock();
                    println!("send_buf addr {:?}",send_buf.as_ptr());
                    println!("send_buf: {:?}",send_buf);  
                    let recv_buf = self.recv_buffer.lock();                    
                    println!("recv_buf: {:?}",recv_buf);
                    let free_buf = self.free_buffer.lock();                    
                    println!("free_buf: {:?}",free_buf);
                    timer=std::time::Instant::now();
                } 
                
            }
            async_std::task::yield_now().await;
            
        }
    }

    fn send_release(&self, dst: usize, cmd: RofiCmd){
        // let cmd_buffer = self.cmd_buffers[dst].lock();
        // println!("sending release: {:?} cmd: {:?} {:?} {:?} 0x{:x} 0x{:x}",self.release_cmd,cmd,self.release_cmd.cmd_as_bytes(), cmd.cmd_as_bytes(),self.release_cmd.cmd_as_addr(),cmd.daddr + offset_of!(RofiCmd,cmd));
        self.rofi_comm.put(dst,self.release_cmd.cmd_as_bytes(),cmd.daddr + offset_of!(RofiCmd,cmd)+self.rofi_comm.base_addr());
    }

    fn send_free(&self, dst: usize, cmd: RofiCmd){
        // let cmd_buffer = self.cmd_buffers[dst].lock();
        // println!("sending release: {:?} cmd: {:?} {:?} {:?} 0x{:x} 0x{:x}",self.release_cmd,cmd,self.release_cmd.cmd_as_bytes(), cmd.cmd_as_bytes(),self.release_cmd.cmd_as_addr(),cmd.daddr + offset_of!(RofiCmd,cmd));
        self.rofi_comm.put(dst,self.free_cmd.cmd_as_bytes(),cmd.daddr + offset_of!(RofiCmd,cmd)+self.rofi_comm.base_addr());
    }

    //actually can we just do the same as send_release and then just check if any buffers can be freed at the end up every recieve loop?
    // async fn send_free(&self, dst: usize, cmd: RofiCmd){
    //     loop{
    //         {
    //             //change back to send_buf...
    //             let mut free_buf = self.free_buffer.lock();
    //             if free_buf[dst].hash() == self.clear_cmd.hash(){
    //                 free_buf[dst].daddr = cmd.daddr;
    //                 free_buf[dst].dsize = 0;
    //                 free_buf[dst].cmd = Cmd::Free;
    //                 free_buf[dst].msg_hash = 0;
    //                 free_buf[dst].calc_hash();
    //                 if !free_buf[dst].check_hash(){
    //                     panic!("wtf");
    //                 }
    //                 // println!("sending free to dst {:?} {:?} (s: {:?} r: {:?})",dst,addr,self.sent_cnt.load(Ordering::SeqCst),self.recv_cnt.load(Ordering::SeqCst));
    //                 let recv_buffer = self.recv_buffer.lock();
    //                 println!("sending free {:?}",free_buf);
    //                 self.rofi_comm.put(dst,free_buf[dst].as_bytes(),recv_buffer[self.my_pe].as_addr());
    //                 // self.put_amt.fetch_add(send_buf[dst].as_bytes().len(),Ordering::Relaxed);
    //                 break;
    //             }
    //         }
    //         async_std::task::yield_now().await;
    //     }
    // }
    async fn send_print(&self, dst: usize,  cmd: RofiCmd){
        let mut timer = std::time::Instant::now();
        loop{
            {
                // let mut cmd_buffer = self.cmd_buffers[dst].lock();
                let mut send_buf = self.send_buffer.lock();
                if send_buf[dst].hash() == self.clear_cmd.hash(){
                    send_buf[dst].daddr = cmd.daddr;
                    send_buf[dst].dsize = cmd.dsize;
                    send_buf[dst].cmd = Cmd::Print;
                    send_buf[dst].msg_hash = cmd.msg_hash;
                    send_buf[dst].calc_hash();
                    // println!("sending print {:?} (s: {:?} r: {:?})",addr,self.sent_cnt.load(Ordering::SeqCst),self.recv_cnt.load(Ordering::SeqCst));
                    let recv_buffer = self.recv_buffer.lock();
                    // println!("sending cmd {:?}",send_buf);
                    self.rofi_comm.put(dst,send_buf[dst].as_bytes(),recv_buffer[self.my_pe].as_addr());
                    // self.put_amt.fetch_add(send_buf[dst].as_bytes().len(),Ordering::Relaxed);
                    break;
                }
                if timer.elapsed().as_secs_f64() > 30.0 {
                    let send_buf = self.send_buffer.lock();                    
                    // println!("waiting to add cmd to cmd buffer {:?}",cmd_buffer);
                    println!("send_buf: {:?}",send_buf);
                    // drop(send_buf);
                    let recv_buf = self.recv_buffer.lock();                    
                    println!("recv_buf: {:?}",recv_buf);
                    let free_buf = self.free_buffer.lock();                    
                    println!("free_buf: {:?}",free_buf);
                    timer=std::time::Instant::now();
                } 
            }
            async_std::task::yield_now().await;
        }
    }

    fn check_transfers(&self, src: usize){
        let mut cmd_buffer = self.cmd_buffers[src].lock();
        self.progress_transfers(src,&mut cmd_buffer);
    } 


    // fn free_data(&self, src: usize,  buf_addr: usize){
    //     let fb = self.free_buffer.lock();
    //     // println!("sending clear to src {:?} {:?} {:?} (s: {:?} r: {:?})",src,buf_addr,sb[self.my_pe].as_addr()-self.rofi_comm.base_addr(),self.sent_cnt.load(Ordering::SeqCst),self.recv_cnt.load(Ordering::SeqCst));
    //     self.rofi_comm.put(src,self.clear_cmd.as_bytes(),fb[self.my_pe].as_addr());
    //     // self.put_amt.fetch_add(self.clear_cmd.as_bytes().len(),Ordering::Relaxed);
    //     drop(fb);
    //     let mut cmd_buffer = self.cmd_buffers[src].lock();
    //     self.progress_transfers(src,&mut cmd_buffer);
    //     // self.check_for_finished_tx(src,&mut cmd_buffer);
    //     if !cmd_buffer.free_data(buf_addr,self.rofi_comm.clone()){
    //         let send_buf = self.send_buffer.lock();                    
    //         println!("waiting to add cmd to cmd buffer {:?}",cmd_buffer);
    //         println!("send_buf: {:?}",send_buf);
    //         drop(send_buf);
    //         let recv_buf = self.recv_buffer.lock();                    
    //         println!("recv_buf: {:?}",recv_buf);
    //     }
    // }

    fn print_data(&self, src: usize, cmd: RofiCmd){
        let cmd_buffer = self.cmd_buffers[src].lock();
        println!("print data -- cmd {:?}",cmd);
        println!("print data -- cmd_buffer {:?}",cmd_buffer);
    }

    // fn print_recv_buffers(&self){
    //     let  recv_buffer = self.recv_buffer.lock();
    //     println!("recv_buffer {:?}",recv_buffer);
    // }

    //update cmdbuffers to include a hash the wait on that here
    async fn get_data(&self, src: usize,  cmd: RofiCmd, data_slice: &mut [u8]) {
        self.rofi_comm.iget_relative(src,cmd.daddr as usize,data_slice);
        // self.get_amt.fetch_add(data_slice.len(),Ordering::Relaxed);
        let mut timer = std::time::Instant::now();
        while calc_hash(data_slice.as_ptr() as usize,data_slice.len()) != cmd.msg_hash{
            async_std::task::yield_now().await;
            if timer.elapsed().as_secs_f64() > 15.0{
                println!("stuck waiting for data from {:?}!!! {:?} {:?} {:?} {:?}",src,cmd,data_slice.len(),cmd.dsize,&data_slice);
                self.send_print(src,cmd).await;
                // self.rofi_comm.iget_relative(src,cmd.daddr,data_slice);
                timer = std::time::Instant::now();
            }
        }
    }

    async fn get_serialized_data(&self, src: usize,  cmd: RofiCmd, ser_data: &RofiData) {
        let data_slice = ser_data.header_and_data_as_bytes();
        self.rofi_comm.iget_relative(src,cmd.daddr as usize,data_slice);
        // self.get_amt.fetch_add(data_slice.len(),Ordering::Relaxed);
        let mut timer = std::time::Instant::now();
        while calc_hash(data_slice.as_ptr() as usize,ser_data.len) != cmd.msg_hash{
            async_std::task::yield_now().await;
            if timer.elapsed().as_secs_f64() > 15.0{
                println!("stuck waiting for serialized data from {:?} !!! {:?} {:?} {:?} {:?}",src,cmd,data_slice.len(),cmd.dsize,&data_slice);
                self.send_print(src, cmd).await;
                timer = std::time::Instant::now();
            }
        }
    }


    async fn get_cmd(&self,src: usize, cmd: RofiCmd) -> SerializedData {
        let ser_data = RofiData::new(self.rofi_comm.clone(),cmd.dsize as usize).await;
        self.get_serialized_data(src,cmd,&ser_data).await;
        // println!("received data {:?}",ser_data.header_and_data_as_bytes());
        self.recv_cnt.fetch_add(1,Ordering::SeqCst);
        // println!("received: {:?} {:?} {:?} cmd: {:?}",ser_data.relative_addr,cmd.dsize,ser_data.len,cmd);//,ser_data.header_and_data_as_bytes());
        SerializedData::RofiData(ser_data)
    }

    async fn get_cmd_buf(&self, src: usize,  cmd: RofiCmd) -> usize {
        let mut data = self.rofi_comm.rt_alloc(cmd.dsize as usize);
        let mut timer = std::time::Instant::now();
        while data.is_none(){
            async_std::task::yield_now().await;
            data = self.rofi_comm.rt_alloc(cmd.dsize as usize);
            if timer.elapsed().as_secs_f64() > 15.0{
                println!("stuck waiting for alloc");
                timer = std::time::Instant::now();
            }
        }
        // println!("getting into {:?} 0x{:x} ",data, data.unwrap() + self.rofi_comm.base_addr());
        let data = data.unwrap() + self.rofi_comm.base_addr();
        let data_slice = unsafe{std::slice::from_raw_parts_mut(data as *mut u8,cmd.dsize as usize)};
        self.get_data(src,cmd,data_slice).await;
        // println!("received cmd_buf {:?}",data_slice);
        // println!("sending release to src: {:?} {:?} (s: {:?} r: {:?})",src,cmd.daddr,self.sent_cnt.load(Ordering::SeqCst),self.recv_cnt.load(Ordering::SeqCst));
        self.send_release(src,cmd);
        // self.put_amt.fetch_add(self.release_cmd.as_bytes().len(),Ordering::Relaxed);
        data
    }
}

impl Drop for RofiCQ{
    fn drop(&mut self){
        // println!("dropping RofiCQ");
        let mut send_buf =  self.send_buffer.lock();
        let old=std::mem::take(&mut *send_buf);
        Box::into_raw(old);
        let mut recv_buf = self.recv_buffer.lock();
        let old=std::mem::take(&mut *recv_buf);
        Box::into_raw(old);
        let mut free_buf = self.free_buffer.lock();
        let old=std::mem::take(&mut *free_buf);
        Box::into_raw(old);
        let old=std::mem::replace(Arc::get_mut(&mut self.release_cmd).unwrap(),Box::new(RofiCmd{daddr:0,dsize:0, cmd: Cmd::Clear,msg_hash: 0,cmd_hash: 0}));
        Box::into_raw(old);
        let old=std::mem::replace(Arc::get_mut(&mut self.clear_cmd).unwrap(),Box::new(RofiCmd{daddr:0,dsize:0, cmd: Cmd::Clear,msg_hash: 0,cmd_hash: 0}));
        Box::into_raw(old);
        let old=std::mem::replace(Arc::get_mut(&mut self.free_cmd).unwrap(),Box::new(RofiCmd{daddr:0,dsize:0, cmd: Cmd::Clear,msg_hash: 0,cmd_hash: 0}));
        Box::into_raw(old);
        self.cmd_buffers.clear();
        // println!("dropped RofiCQ");
    }
}

pub(crate) struct RofiCommandQueue{
    cq: Arc<RofiCQ>,
    send_buffer_addr: usize,
    recv_buffer_addr: usize,
    free_buffer_addr: usize,
    release_cmd_addr: usize,
    clear_cmd_addr: usize,
    free_cmd_addr: usize,
    cmd_buffers_addrs: Vec<Arc<Vec<usize>>>,
    rofi_comm: Arc<RofiComm>
}

impl RofiCommandQueue{
    pub fn new(rofi_comm: Arc<RofiComm>,my_pe: usize, num_pes: usize) -> RofiCommandQueue {
        let send_buffer_addr = rofi_comm.rt_alloc(num_pes * std::mem::size_of::<RofiCmd>()).unwrap() + rofi_comm.base_addr();
        // println!("send_buffer_addr{:x} {:x}",send_buffer_addr,send_buffer_addr-rofi_comm.base_addr());
        let recv_buffer_addr = rofi_comm.rt_alloc(num_pes * std::mem::size_of::<RofiCmd>()).unwrap() + rofi_comm.base_addr();
        // println!("recv_buffer_addr {:x} {:x}",recv_buffer_addr,recv_buffer_addr-rofi_comm.base_addr());
        let free_buffer_addr = rofi_comm.rt_alloc(num_pes * std::mem::size_of::<RofiCmd>()).unwrap() + rofi_comm.base_addr();
        // println!("free_buffer_addr {:x} {:x}",recv_buffer_addr,recv_buffer_addr-rofi_comm.base_addr());
        let release_cmd_addr = rofi_comm.rt_alloc(std::mem::size_of::<RofiCmd>()).unwrap()+ rofi_comm.base_addr();
        // println!("release_cmd_addr {:x}",release_cmd_addr-rofi_comm.base_addr());
        let clear_cmd_addr = rofi_comm.rt_alloc(std::mem::size_of::<RofiCmd>()).unwrap()+ rofi_comm.base_addr();
        // println!("clear_cmd_addr {:x}",clear_cmd_addr-rofi_comm.base_addr());
        let free_cmd_addr = rofi_comm.rt_alloc(std::mem::size_of::<RofiCmd>()).unwrap()+ rofi_comm.base_addr();
        // println!("free_cmd_addr {:x}",free_cmd_addr-rofi_comm.base_addr());

        let mut cmd_buffers_addrs = vec![];
        for _pe in 0..num_pes{
            let mut addrs = vec![];
            for _i in 0..CMD_BUFS_PER_PE{  
                let addr = rofi_comm.rt_alloc(CMD_BUF_LEN * std::mem::size_of::<RofiCmd>() + 1).unwrap();//+ rofi_comm.base_addr();
                // println!("{:x} {:x} {:x}",_pe,_i,addr,);
                addrs.push(addr);
                
            }
            cmd_buffers_addrs.push(Arc::new(addrs));
        }
        // let cmd_buffers_addrs=Arc::new(cmd_buffers_addrs);
        let cq = RofiCQ::new(send_buffer_addr, recv_buffer_addr,free_buffer_addr,&cmd_buffers_addrs.clone(),release_cmd_addr,clear_cmd_addr,free_cmd_addr,rofi_comm.clone(),my_pe,num_pes);
        RofiCommandQueue{
            cq: Arc::new(cq),
            send_buffer_addr: send_buffer_addr,
            recv_buffer_addr: recv_buffer_addr,
            free_buffer_addr: free_buffer_addr,
            release_cmd_addr: release_cmd_addr,
            clear_cmd_addr: clear_cmd_addr,
            free_cmd_addr: free_cmd_addr,
            cmd_buffers_addrs: cmd_buffers_addrs,
            rofi_comm: rofi_comm
        }
    }

    pub async fn send_data(&self, data: SerializedData, dst: usize){
        if let SerializedData::RofiData(ref data) = data {
            // println!("sending: {:?} {:?}",data.relative_addr,data.len);
            let hash = calc_hash(data.relative_addr+self.rofi_comm.base_addr(),data.len);
            // println!("send_data: {:?} {:?} {:?}",data.relative_addr,data.len,hash);
            data.increment_cnt(); //or we could implement something like an into_raw here...
            // println!("sending data {:?}",data.header_and_data_as_bytes());
            self.cq.send(data.relative_addr,data.len,dst,hash).await;
        }
    }

    

    pub async fn recv_data(&self, scheduler: Arc<Scheduler>, lamellae: Arc<Lamellae>, active: Arc<AtomicBool>) {
        let num_pes = lamellae.num_pes();
        let my_pe = lamellae.my_pe();
        // let mut timer= std::time::Instant::now();
        while active.load(Ordering::SeqCst) || !self.cq.empty(){
            for src in 0..num_pes{
                if src != my_pe{
                    if let Some(cmd_buf_cmd) = self.cq.ready(src){
                        // timer =  std::time::Instant::now();
                        let cmd_buf_cmd=cmd_buf_cmd;
                        // println!("recv_data {:?}",cmd_buf_cmd);
                        match cmd_buf_cmd.cmd{
                            Cmd::Clear| Cmd::Release=> panic!("should not be possible to see cmd clear or release here"),
                            Cmd::Free => panic!("should not be possible to see free here"),
                            // self.cq.free_data(src,cmd_buf_cmd.daddr as usize),
                            Cmd::Print => self.cq.print_data(src,cmd_buf_cmd),
                            Cmd::Tx => {
                                let cq = self.cq.clone();
                                let lamellae = lamellae.clone();
                                let scheduler1 = scheduler.clone();
                                let rofi_comm = self.rofi_comm.clone();
                                let task = async move{
                                    // println!("going to get cmd_buf {:?} from {:?}",cmd_buf_cmd, src);
                                    let data = cq.get_cmd_buf(src,cmd_buf_cmd).await;
                                    let cmd_buf = unsafe{std::slice::from_raw_parts(data as *const RofiCmd,cmd_buf_cmd.dsize as usize/std::mem::size_of::<RofiCmd>())};
                                    // println!("cmd_buf {:?}",cmd_buf);
                                    let mut i =0;
                                    let len = cmd_buf.len();
                                    let cmd_cnt= Arc::new(AtomicUsize::new(len));
                                    // println!("src: {:?} cmd_buf len {:?}",src ,len);
    
                                    for cmd in cmd_buf{
                                        let cmd = *cmd;
                                        if cmd.dsize !=0 {
                                            let cq = cq.clone();
                                            let lamellae=lamellae.clone();
                                            let scheduler2 = scheduler1.clone();
                                            // cmd_cnt.fetch_add(1,Ordering::SeqCst);
                                            let cmd_cnt_clone = cmd_cnt.clone();
                                            let task = async move{
                                                // println!("getting cmd {:?} [{:?}/{:?}]",cmd,i,len);
                                                let work_data = cq.get_cmd(src,cmd).await;
                                                
                                                scheduler2.submit_work(work_data,lamellae.clone());
                                                if cmd_cnt_clone.fetch_sub(1,Ordering::SeqCst) == 1{
                                                    cq.send_free(src,cmd_buf_cmd);
                                                    // println!("sending clear cmd {:?} [{:?}/{:?}]",cmd_buf_cmd.daddr,i,len);
                                                }
                                            };
                                            scheduler1.submit_task(task);
                                            i+=1;
                                        } 
                                        else{
                                            panic!("should not be here! {:?} -- {:?} [{:?}/{:?}]",cmd_buf_cmd,cmd,i,len);
                                        }
                                    }
                                    rofi_comm.rt_free(data-rofi_comm.base_addr());
                                };
                                scheduler.submit_task(task);
                            },
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
            self.rofi_comm.process_dropped_reqs();
            async_std::task::yield_now().await;
        }
        // println!("leaving recv_data task");
    }

    pub fn tx_amount(&self)->usize{
        // println!("cq put: {:?} get {:?}",self.cq.put_amt.load(Ordering::SeqCst) ,self.cq.get_amt.load(Ordering::SeqCst));
        self.cq.put_amt.load(Ordering::SeqCst) + self.cq.get_amt.load(Ordering::SeqCst)
    }

    pub fn mem_per_pe() -> usize{
        (CMD_BUF_LEN * CMD_BUFS_PER_PE + 4) * std::mem::size_of::<RofiCmd>()
    }

}

impl Drop for RofiCommandQueue{
    fn drop(&mut self){
        // println!("dropping rofi command queue");
        self.rofi_comm.rt_free(self.send_buffer_addr - self.rofi_comm.base_addr());
        self.rofi_comm.rt_free(self.recv_buffer_addr - self.rofi_comm.base_addr());
        self.rofi_comm.rt_free(self.free_buffer_addr - self.rofi_comm.base_addr());
        self.rofi_comm.rt_free(self.release_cmd_addr - self.rofi_comm.base_addr());
        self.rofi_comm.rt_free(self.clear_cmd_addr - self.rofi_comm.base_addr());
        self.rofi_comm.rt_free(self.free_cmd_addr - self.rofi_comm.base_addr());
        for bufs in self.cmd_buffers_addrs.iter(){
            for buf in bufs.iter(){
                self.rofi_comm.rt_free(*buf);
            }
        }
        // println!("rofi command queue dropped");
    }
}

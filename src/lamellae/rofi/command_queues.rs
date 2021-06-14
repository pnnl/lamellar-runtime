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
const CMD_BUFS_PER_PE: usize = 1;

#[repr(C)]
#[derive(Clone, Copy)]
struct RofiCmd { // we send this to remote nodes
    daddr: u32, 
    dsize: u32,
    tx_bit: u16,
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

impl std::fmt::Debug for RofiCmd{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "daddr {:#x}({:?}) dsize {:?} tx {:?}",
            self.daddr,
            self.daddr,
            self.dsize,
            self.tx_bit,
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
    fn push(&mut self, daddr: usize, dsize: usize){
        if daddr==0 || dsize ==0{
            panic!("this shouldnt happen! {:?} {:?}",daddr,dsize);
        }
        let mut cmd = unsafe {self.buf.get_unchecked_mut(self.index)};
        cmd.daddr=daddr as u32;
        cmd.dsize=dsize as u32;
        cmd.tx_bit=255 as u16;
        self.index +=1;
        if dsize > 0{
            self.allocated_cnt+=1;
        }
    }
    fn full(&self) -> bool{
        self.index==self.max_size
    }
    fn reset(&mut self){
        self.index = 0;
        self.allocated_cnt=0;
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
    fn set_tx_bit(&mut self){
        let slice = unsafe {std::slice::from_raw_parts_mut((self.addr + self.base_addr) as *mut u8, std::mem::size_of::<RofiCmd>() * self.index +1)};
        slice[slice.len()-1]=0;
    }
}

impl std::fmt::Debug for CmdBuf{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "addr {:#x}({:?}) index {:?} a_cnt {:?}",
            self.addr,
            self.addr,
            self.index,
            self.allocated_cnt
        )
    }
}

impl Drop for CmdBuf{
    fn drop(&mut self){
        //println!("dropping cmd buf");
        let old=std::mem::take(&mut self.buf);
        Box::into_raw(old);
        //println!("dropped cmd buf");

    }
}

struct RofiCmdBuffer{
    empty_bufs: Vec<CmdBuf>,
    full_bufs: Vec<CmdBuf>,
    tx_bufs: Vec<CmdBuf>,
    waiting_bufs: HashMap<usize,CmdBuf>,
    cur_buf: Option<CmdBuf>,
    num_bufs: usize,
    
}

impl RofiCmdBuffer {
    fn new(addrs: Arc<Vec<usize>>,base_addr: usize) -> RofiCmdBuffer{
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
            tx_bufs: Vec::new(),
            waiting_bufs: HashMap::new(),
            cur_buf: None,
            num_bufs: addrs.len(),
        }
    }
    fn try_push(&mut self, addr: usize, len: usize )-> bool{
        
            if self.cur_buf.is_none() {
                self.cur_buf= self.empty_bufs.pop();
            }
            if let Some(buf) = &mut self.cur_buf{
                buf.push(addr,len);
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
        if let Some(mut buf) = self.full_bufs.pop(){
            // println!("flushing {:?} {:?}",buf.addr(),buf.size());
            buf.set_tx_bit();
            cmd.daddr = buf.addr() as u32;
            cmd.dsize = buf.size() as u32+1;
            cmd.tx_bit = 255 as u16;
            self.tx_bufs.push(buf);
            
        }
        else if let Some(mut buf) = std::mem::replace(&mut self.cur_buf,None) {
            // println!("flushing {:?} {:?}",buf.addr(),buf.size());
            buf.set_tx_bit();
            cmd.daddr = buf.addr() as u32;
            cmd.dsize = buf.size() as u32+1;
            cmd.tx_bit = 255 as u16;
            self.tx_bufs.push(buf);
            
        }
        // else {} all buffers are currently busy
        // println!("flush buffer after: {:?}",self);
    }
    fn finish_transfer(&mut self,  cmd: &mut RofiCmd){
        // println!("finish transfer before: {:?}",self);
        if let Some(mut buf) = self.tx_bufs.pop(){
            // println!("tx complete {:?} {:?}",buf.addr(),buf.size());
            // buf.reset();
            if buf.allocated_cnt > 0 {
                self.waiting_bufs.insert(buf.addr(),buf);
            }
            else{
                buf.reset();
                self.empty_bufs.push(buf);
            }
            cmd.daddr = 0;
            cmd.dsize = 0;
            cmd.tx_bit = 0;
        }
        else{
            println!("finish_transfer should this be possible?");
        }
        // println!("finish transfer after: {:?}",self);
    }
    fn free_data(&mut self, buf_addr: usize,rofi_comm: Arc<RofiComm>){
        // println!("free data before [{:?}]: {:?}",buf_addr,self);
        // println!("free_data {:?} {:?}",buf_addr,self.waiting_bufs);
        if let Some(mut buf) = self.waiting_bufs.remove(&buf_addr){
            
            for cmd in buf.iter() {
                if cmd.dsize > 0{
                    let ref_cnt_addr = cmd.daddr as usize-std::mem::size_of::<AtomicUsize>()+rofi_comm.base_addr();
                    let cnt = unsafe{(*(ref_cnt_addr as *const AtomicUsize)).fetch_sub(1,Ordering::SeqCst)};
                    // println!("dereferencing addr: {:?} {:?}",cmd.daddr,cnt);
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
        // println!("free data after [{:?}]: {:?}",buf_addr,self);
    }
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
            num_bufs: {:?}",
            self.empty_bufs,
            self.full_bufs,
            self.tx_bufs,
            self.waiting_bufs,
            self.cur_buf,
            self.num_bufs
        )
    }
}

impl Drop for RofiCmdBuffer{
    fn drop(&mut self) {
        while!self.empty() {std::thread::yield_now()}
        //println!("dropping RofiCmdBuffer");
        self.empty_bufs.clear();
        // self.full_bufs.clear();
        // self.tx_bufs.clear();
        // std::mem::replace(&mut self.cur_buf,None);
        //println!("dropped RofiCmdBuffer");
    }
}

struct RofiCQ{
    send_buffer: Arc<Mutex<Box<[RofiCmd]>>>,
    recv_buffer: Arc<Mutex<Box<[RofiCmd]>>>,
    cmd_buffers: Vec<Mutex<RofiCmdBuffer>>,
    release_cmd: Arc<Box<RofiCmd>>,
    clear_cmd: Arc<Box<RofiCmd>>,
    rofi_comm: Arc<RofiComm>,
    my_pe: usize,
    _num_pes: usize,
    send_waiting: Vec<Arc<AtomicBool>>,
    pending_cmds: Arc<AtomicUsize>,
    sent_cnt: Arc<AtomicUsize>,
    recv_cnt: Arc<AtomicUsize>,
    put_amt: Arc<AtomicUsize>,
    get_amt: Arc<AtomicUsize>,
    // tx_data: Arc<Mutex<HashMap<usize,SerializedData>>>,
}



impl RofiCQ{
    fn new(send_buffer_addr: usize, recv_buffer_addr: usize,  cmd_buffers_addrs: &Vec<Arc<Vec<usize>>>, release_cmd_addr:usize, clear_cmd_addr:usize, rofi_comm: Arc<RofiComm>, my_pe: usize, num_pes: usize) -> RofiCQ {
        let mut cmd_buffers = vec![];
        for addrs in cmd_buffers_addrs.iter(){
            cmd_buffers.push(Mutex::new(RofiCmdBuffer::new(addrs.clone(),rofi_comm.base_addr())));
        }
        let mut recv_buffer = unsafe {Box::from_raw(std::ptr::slice_from_raw_parts_mut(recv_buffer_addr as *mut RofiCmd,num_pes))};
        for cmd in recv_buffer.iter_mut(){
            (*cmd).daddr=0;
            (*cmd).dsize=0;
            (*cmd).tx_bit=0;
        }
        let mut release_cmd = unsafe {Box::from_raw(release_cmd_addr as *mut RofiCmd)};
        release_cmd.daddr=1;
        release_cmd.dsize=1;
        release_cmd.tx_bit=1;

        let mut clear_cmd = unsafe {Box::from_raw(clear_cmd_addr as *mut RofiCmd)};
        clear_cmd.daddr=0;
        clear_cmd.dsize=0;
        clear_cmd.tx_bit=0;

        let mut send_waiting = vec![];
        for _pe in 0..num_pes{
            send_waiting.push(Arc::new(AtomicBool::new(false)));
        }
        RofiCQ{
            send_buffer: Arc::new(Mutex::new(unsafe {Box::from_raw(std::ptr::slice_from_raw_parts_mut(send_buffer_addr as *mut RofiCmd,num_pes))})),
            recv_buffer: Arc::new(Mutex::new(recv_buffer)),
            cmd_buffers: cmd_buffers,
            release_cmd: Arc::new(release_cmd),
            clear_cmd: Arc::new(clear_cmd),
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
        let cmd = &mut recv_buffer[src];
        if !(cmd.daddr == self.clear_cmd.daddr) &&
           !(cmd.daddr == self.clear_cmd.daddr && cmd.dsize == self.clear_cmd.dsize) &&
           !(cmd.daddr == self.release_cmd.daddr && cmd.dsize != self.release_cmd.dsize) &&
           cmd.tx_bit == 255{ 
            // println!("src {:?} {:?} ",src,&self.recv_buffer[src]);
            let res = Some(*cmd);
            cmd.daddr=0;
            cmd.dsize=0;
            cmd.tx_bit=0;
            // self.rofi_comm.put(self.my_pe,self.clear_cmd.as_bytes(),cmd.as_addr()); //reset command locally
            res
        }
        else{
            None
        }
    }
   
    fn check_for_finished_tx(&self, pe: usize,cmd_buffer: &mut RofiCmdBuffer){
        let mut send_buf = self.send_buffer.lock(); //probably should make mutex for each pe?
        if send_buf[pe].daddr == self.release_cmd.dsize && send_buf[pe].dsize == self.release_cmd.dsize && send_buf[pe].tx_bit == self.release_cmd.tx_bit{ //previous request as transfered
            cmd_buffer.finish_transfer(&mut send_buf[pe]);
        }

    }

    fn try_sending_buffer(&self,dst: usize,cmd_buffer: &mut RofiCmdBuffer) -> bool{
        if self.pending_cmds.load(Ordering::SeqCst) == 0 || cmd_buffer.full_bufs.len() > 0{
            let mut send_buf = self.send_buffer.lock();
            if send_buf[dst].daddr == self.clear_cmd.daddr && send_buf[dst].dsize == self.clear_cmd.dsize && send_buf[dst].tx_bit == self.clear_cmd.tx_bit{
                cmd_buffer.flush_buffer(&mut send_buf[dst]);
                // println!{"send_buf  after{:?}",send_buf};
                if send_buf[dst].dsize > 0{
                    let recv_buffer = self.recv_buffer.lock();
                    // println!{"sending data to dst {:?} {:?} ",recv_buffer[self.my_pe].as_addr()-self.rofi_comm.base_addr(),send_buf[dst]};
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

    async fn send(&self, addr: usize, len: usize, dst: usize){
    //    let mut timer=std::time::Instant::now();
        self.pending_cmds.fetch_add(1,Ordering::SeqCst);
        loop{
            {//this is to tell the compiler we wont hold the mutex lock if we have to yield
                let mut cmd_buffer = self.cmd_buffers[dst].lock();
                if cmd_buffer.try_push(addr,len){
                    let data_slice = unsafe{ std::slice::from_raw_parts((addr + self.rofi_comm.base_addr()) as *const u8, len) };
                    self.sent_cnt.fetch_add(1,Ordering::SeqCst);
                    self.put_amt.fetch_add(len,Ordering::Relaxed);
                    let cnt = self.pending_cmds.fetch_sub(1,Ordering::SeqCst);
                    // println!("pushed {:?} {:?} {:?} {:?}",addr,len,cnt, data_slice);
                    break;
                }
                //while we are waiting to push our data might as well try to advance the buffers
                self.check_for_finished_tx(dst,&mut cmd_buffer);
                self.try_sending_buffer(dst,&mut cmd_buffer);                
            }
            async_std::task::yield_now().await;
        }
        let mut im_waiting = false;
        loop{
            {//this is to tell the compiler we wont hold the mutex lock if we have to yield
                let mut cmd_buffer = self.cmd_buffers[dst].lock();
                if !cmd_buffer.empty(){ //data to send
                    self.check_for_finished_tx(dst,&mut cmd_buffer);
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
                        // let mut send_buf = self.send_buffer.lock();
                        // println!("im waiting {:?} {:?} {:?} {:?} (s: {:?} r: {:?})",addr,len,self.pending_cmds.load(Ordering::SeqCst),send_buf,self.sent_cnt.load(Ordering::SeqCst),self.recv_cnt.load(Ordering::SeqCst));
                    }
                    else{
                        break;
                    }
                }
                
            }
            async_std::task::yield_now().await;
            
        }
    }

    async fn send_free(&self, dst: usize, addr: usize){
        loop{
            {
                let mut cmd_buffer = self.cmd_buffers[dst].lock();
                self.check_for_finished_tx(dst,&mut cmd_buffer);
                let mut send_buf = self.send_buffer.lock();
                if send_buf[dst].daddr == self.clear_cmd.daddr && send_buf[dst].dsize == self.clear_cmd.dsize && send_buf[dst].tx_bit == self.clear_cmd.tx_bit {
                    send_buf[dst].daddr = addr as u32;
                    send_buf[dst].dsize = 0;
                    send_buf[dst].tx_bit = 255;
                    // println!("sending free to dst {:?} {:?} (s: {:?} r: {:?})",dst,addr,self.sent_cnt.load(Ordering::SeqCst),self.recv_cnt.load(Ordering::SeqCst));
                    let recv_buffer = self.recv_buffer.lock();
                    self.rofi_comm.put(dst,send_buf[dst].as_bytes(),recv_buffer[self.my_pe].as_addr());
                    // self.put_amt.fetch_add(send_buf[dst].as_bytes().len(),Ordering::Relaxed);
                    break;
                }
            }
            async_std::task::yield_now().await;
        }
    }
    async fn send_print(&self, dst: usize, addr: usize, size: usize){
        // let size = (size as f64).log2() as usize;
        loop{
            {
                let mut cmd_buffer = self.cmd_buffers[dst].lock();
                self.check_for_finished_tx(dst,&mut cmd_buffer);
                let mut send_buf = self.send_buffer.lock();
                if send_buf[dst].daddr == self.clear_cmd.daddr && send_buf[dst].dsize == self.clear_cmd.dsize && send_buf[dst].tx_bit == self.clear_cmd.tx_bit {
                    send_buf[dst].daddr = size as u32;
                    send_buf[dst].dsize = addr as u32;
                    send_buf[dst].tx_bit = 255 as u16;
                    // println!("sending print {:?} (s: {:?} r: {:?})",addr,self.sent_cnt.load(Ordering::SeqCst),self.recv_cnt.load(Ordering::SeqCst));
                    let recv_buffer = self.recv_buffer.lock();
                    self.rofi_comm.put(dst,send_buf[dst].as_bytes(),recv_buffer[self.my_pe].as_addr());
                    // self.put_amt.fetch_add(send_buf[dst].as_bytes().len(),Ordering::Relaxed);
                    break;
                }
            }
            async_std::task::yield_now().await;
        }
    }

    fn free_data(&self, src: usize, buf_addr: usize){
        let sb = self.send_buffer.lock();
        // println!("sending clear to src {:?} {:?} {:?} (s: {:?} r: {:?})",src,buf_addr,sb[self.my_pe].as_addr()-self.rofi_comm.base_addr(),self.sent_cnt.load(Ordering::SeqCst),self.recv_cnt.load(Ordering::SeqCst));
        self.rofi_comm.put(src,self.clear_cmd.as_bytes(),sb[self.my_pe].as_addr());
        // self.put_amt.fetch_add(self.clear_cmd.as_bytes().len(),Ordering::Relaxed);
        drop(sb);
        let mut cmd_buffer = self.cmd_buffers[src].lock();
        self.check_for_finished_tx(src,&mut cmd_buffer);
        cmd_buffer.free_data(buf_addr,self.rofi_comm.clone());
    }

    fn check_buffers(&self,src: usize){
        let mut cmd_buffer = self.cmd_buffers[src].lock();
        self.check_for_finished_tx(src,&mut cmd_buffer);
    }

    async fn get_data(&self, src: usize, cmd: RofiCmd, data_slice: &mut [u8]) {
        data_slice[cmd.dsize as usize - 1]=255;
        data_slice[cmd.dsize as usize - 2]=255;
        self.rofi_comm.iget_relative(src,cmd.daddr as usize,data_slice);
        // self.get_amt.fetch_add(data_slice.len(),Ordering::Relaxed);
        let mut timer = std::time::Instant::now();
        while data_slice[data_slice.len()-1] == 255{
            async_std::task::yield_now().await;
            if timer.elapsed().as_secs_f64() > 15.0{
                println!("stuck waiting for data!!! {:?} {:?} {:?} {:?}",cmd,data_slice.len(),cmd.dsize,&data_slice);
                self.send_print(src,cmd.daddr as usize,cmd.dsize as usize).await;
                // self.rofi_comm.iget_relative(src,cmd.daddr,data_slice);
                timer = std::time::Instant::now();
            }
        }
    }

    async fn get_cmd(&self,src: usize, cmd: RofiCmd) -> SerializedData {
        let ser_data = RofiData::new(self.rofi_comm.clone(),cmd.dsize as usize-1).await;
        let data_slice = ser_data.header_and_data_as_bytes();
        self.get_data(src,cmd,data_slice).await;
        self.recv_cnt.fetch_add(1,Ordering::SeqCst);
        // println!("received: {:?} {:?} {:?} {:?}",ser_data.relative_addr,cmd.dsize,ser_data.len,ser_data.header_and_data_as_bytes());
        SerializedData::RofiData(ser_data)
    }

    async fn get_cmd_buf(&self, src: usize, cmd: RofiCmd) -> usize {
        let mut data = self.rofi_comm.rt_alloc(cmd.dsize as usize);
        while data.is_none(){
            async_std::task::yield_now().await;
            data = self.rofi_comm.rt_alloc(cmd.dsize as usize);
        }
        // println!("getting into {:?} 0x{:x} ",data, data.unwrap() + self.rofi_comm.base_addr());
        let data = data.unwrap() + self.rofi_comm.base_addr();
        let data_slice = unsafe{std::slice::from_raw_parts_mut(data as *mut u8,cmd.dsize as usize)};
        self.get_data(src,cmd,data_slice).await;
        let sb = self.send_buffer.lock();
        // println!("sending release to src: {:?} {:?} (s: {:?} r: {:?})",src,cmd.daddr,self.sent_cnt.load(Ordering::SeqCst),self.recv_cnt.load(Ordering::SeqCst));
        self.rofi_comm.put(src,self.release_cmd.as_bytes(),sb[self.my_pe].as_addr()); //release remote command
        // self.put_amt.fetch_add(self.release_cmd.as_bytes().len(),Ordering::Relaxed);
        data
    }
}

impl Drop for RofiCQ{
    fn drop(&mut self){
        //println!("dropping RofiCQ");
        let mut send_buf =  self.send_buffer.lock();
        let old=std::mem::take(&mut *send_buf);
        Box::into_raw(old);
        let mut recv_buf = self.recv_buffer.lock();
        let old=std::mem::take(&mut *recv_buf);
        Box::into_raw(old);
        let old=std::mem::replace(Arc::get_mut(&mut self.release_cmd).unwrap(),Box::new(RofiCmd{daddr:0,dsize:0,tx_bit: 0}));
        Box::into_raw(old);
        let old=std::mem::replace(Arc::get_mut(&mut self.clear_cmd).unwrap(),Box::new(RofiCmd{daddr:0,dsize:0,tx_bit: 0}));
        Box::into_raw(old);
        self.cmd_buffers.clear();
        //println!("dropped RofiCQ");
    }
}

pub(crate) struct RofiCommandQueue{
    cq: Arc<RofiCQ>,
    send_buffer_addr: usize,
    recv_buffer_addr: usize,
    release_cmd_addr: usize,
    clear_cmd_addr: usize,
    cmd_buffers_addrs: Vec<Arc<Vec<usize>>>,
    rofi_comm: Arc<RofiComm>
}

impl RofiCommandQueue{
    pub fn new(rofi_comm: Arc<RofiComm>,my_pe: usize, num_pes: usize) -> RofiCommandQueue {
        let send_buffer_addr = rofi_comm.rt_alloc(num_pes * std::mem::size_of::<RofiCmd>()).unwrap() + rofi_comm.base_addr();
        // println!("send_buffer_addr {:x}",send_buffer_addr-rofi_comm.base_addr());
        let recv_buffer_addr = rofi_comm.rt_alloc(num_pes * std::mem::size_of::<RofiCmd>()).unwrap() + rofi_comm.base_addr();
        // println!("recv_buffer_addr {:x}",recv_buffer_addr-rofi_comm.base_addr());
        let release_cmd_addr = rofi_comm.rt_alloc(std::mem::size_of::<RofiCmd>()).unwrap()+ rofi_comm.base_addr();
        // println!("release_cmd_addr {:x}",release_cmd_addr-rofi_comm.base_addr());
        let clear_cmd_addr = rofi_comm.rt_alloc(std::mem::size_of::<RofiCmd>()).unwrap()+ rofi_comm.base_addr();
        // println!("clear_cmd_addr {:x}",clear_cmd_addr-rofi_comm.base_addr());

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
        let cq = RofiCQ::new(send_buffer_addr, recv_buffer_addr,&cmd_buffers_addrs.clone(),release_cmd_addr,clear_cmd_addr,rofi_comm.clone(),my_pe,num_pes);
        RofiCommandQueue{
            cq: Arc::new(cq),
            send_buffer_addr: send_buffer_addr,
            recv_buffer_addr: recv_buffer_addr,
            release_cmd_addr: release_cmd_addr,
            clear_cmd_addr: clear_cmd_addr,
            cmd_buffers_addrs: cmd_buffers_addrs,
            rofi_comm: rofi_comm
        }
    }

    pub async fn send_data(&self, data: SerializedData, dst: usize){
        if let SerializedData::RofiData(ref data) = data {
            // println!("sending: {:?} {:?}",data.relative_addr,data.len);
            data.increment_cnt(); //or we could implement something like an into_raw here...
            self.cq.send(data.relative_addr,data.len,dst).await;
        }
    }

    

    pub async fn recv_data(&self, scheduler: Arc<Scheduler>, lamellae: Arc<Lamellae>, active: Arc<AtomicBool>) {
        let num_pes = lamellae.num_pes();
        let my_pe = lamellae.my_pe();
        while active.load(Ordering::SeqCst) || !self.cq.empty(){
            for src in 0..num_pes{
                if src != my_pe{
                    if let Some(cmd_buf_cmd) = self.cq.ready(src){
                        let cmd_buf_cmd=cmd_buf_cmd;
                        // println!("processing {:?} ",cmd_buf_cmd);
                        if cmd_buf_cmd.dsize != 0{
                            // if cmd_buf_cmd.daddr <= 2000 && cmd_buf_cmd.dsize > 500000{
                            //     // let size = (cmd_buf_cmd.daddr  as f64).exp2() as usize;
                            //     let data_slice = unsafe{std::slice::from_raw_parts((cmd_buf_cmd.dsize + self.rofi_comm.base_addr()) as *const u8,cmd_buf_cmd.daddr)};
                            //     panic!("debug slice: {:?} {:?}",cmd_buf_cmd.dsize,data_slice);
                            // }
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
                                let cmd_cnt= Arc::new(AtomicUsize::new(0));

                                for cmd in cmd_buf{
                                    let cmd = *cmd;
                                    if cmd.dsize !=0 {
                                        let cq = cq.clone();
                                        let lamellae=lamellae.clone();
                                        let scheduler2 = scheduler1.clone();
                                        cmd_cnt.fetch_add(1,Ordering::SeqCst);
                                        let cmd_cnt_clone = cmd_cnt.clone();
                                        let task = async move{
                                            // println!("getting cmd {:?} [{:?}/{:?}]",cmd,i,len);
                                            let work_data = cq.get_cmd(src,cmd).await;
                                            
                                            scheduler2.submit_work(work_data,lamellae.clone());
                                            if cmd_cnt_clone.fetch_sub(1,Ordering::SeqCst) == 1{
                                                cq.send_free(src,cmd_buf_cmd.daddr as usize).await;
                                                // println!("sending clear cmd {:?} [{:?}/{:?}]",cmd_buf_cmd.daddr,i,len);
                                            }
                                        };
                                        scheduler1.submit_task(task);
                                        i+=1;
                                    } 
                                    else{
                                        panic!("should not be here! {:?} [{:?}/{:?}]",cmd,i,len);
                                    }
                                }
                                rofi_comm.rt_free(data-rofi_comm.base_addr());
                            };
                            scheduler.submit_task(task);
                        }
                        else{
                            self.cq.free_data(src,cmd_buf_cmd.daddr as usize);
                        }
                    }
                    // self.cq.check_buffers(src);//process any finished buffers
                }
            }
            async_std::task::yield_now().await;
        }
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
        //println!("dropping rofi command queue");
        self.rofi_comm.rt_free(self.send_buffer_addr - self.rofi_comm.base_addr());
        self.rofi_comm.rt_free(self.recv_buffer_addr - self.rofi_comm.base_addr());
        self.rofi_comm.rt_free(self.release_cmd_addr - self.rofi_comm.base_addr());
        self.rofi_comm.rt_free(self.clear_cmd_addr - self.rofi_comm.base_addr());
        for bufs in self.cmd_buffers_addrs.iter(){
            for buf in bufs.iter(){
                self.rofi_comm.rt_free(*buf);
            }
        }
        println!("rofi command queue dropped");
    }
}

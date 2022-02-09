use crate::lamellae::comm::*;
use crate::lamellae::command_queues::CommandQueue;
use crate::lamellae::rofi::rofi_api::*;
use crate::lamellae::{
    AllocationType, Des, SerializeHeader, SerializedData, SerializedDataOps, SubData,
};
use crate::lamellar_alloc::{BTreeAlloc, LamellarAlloc};
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use log::trace;
use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

// pub struct RofiReq{
//     txids: Vec<std::os::raw::c_ulong>,
//     _drop_set: Arc<Mutex<Vec<std::os::raw::c_ulong>>>,
//     _any_dropped: Arc<AtomicBool>,
// }

// impl Drop for RofiReq{
//     fn drop(&mut self) {
//         // let mut dropped = self.drop_set.lock();
//         // dropped.append(&mut self.txids);
//     }
// }

static ROFI_MAGIC_8: u64 = 0b1101001110111100011111001001100100111110011001100011110111001011;
static ROFI_MAGIC_4: u32 = 0b10001111100100110010011111001100;
static ROFI_MAGIC_2: u16 = 0b1100100110010011;
static ROFI_MAGIC_1: u8 = 0b10011001;

static ROFI_MEM: AtomicUsize = AtomicUsize::new(4 * 1024 * 1024 * 1024);
const RT_MEM: usize = 100 * 1024 * 1024; // we add this space for things like team barrier buffers, but will work towards having teams get memory from rofi allocs
pub(crate) struct RofiComm {
    pub(crate) rofi_base_address: Arc<RwLock<usize>>,
    alloc: RwLock<Vec<BTreeAlloc>>,
    _init: AtomicBool,
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    pub(crate) put_amt: Arc<AtomicUsize>,
    put_cnt: Arc<AtomicUsize>,
    pub(crate) get_amt: Arc<AtomicUsize>,
    get_cnt: Arc<AtomicUsize>,
    comm_mutex: Arc<Mutex<()>>,
    // drop_set: Arc<Mutex<Vec<std::os::raw::c_ulong>>>,
    // any_dropped: Arc<AtomicBool>,
    // alloc_mutex: Arc<Mutex<()>>,
}

//#[prof]
impl RofiComm {
    pub(crate) fn new(provider: &str) -> RofiComm {
        if let Ok(size) = std::env::var("LAMELLAR_MEM_SIZE") {
            let size = size
                .parse::<usize>()
                .expect("invalid memory size, please supply size in bytes");
            ROFI_MEM.store(size, Ordering::SeqCst);
        }
        rofi_init(provider).expect("error in rofi init");
        trace!("rofi initialized");
        rofi_barrier();
        let num_pes = rofi_get_size();
        let cmd_q_mem = CommandQueue::mem_per_pe() * num_pes;

        let total_mem = cmd_q_mem + RT_MEM + ROFI_MEM.load(Ordering::SeqCst);
        // println!("rofi comm total_mem {:?}",total_mem);
        let addr = rofi_alloc(total_mem, AllocationType::Global);
        let rofi = RofiComm {
            rofi_base_address: Arc::new(RwLock::new(addr as usize)),
            alloc: RwLock::new(vec![BTreeAlloc::new("rofi_mem".to_string())]),
            _init: AtomicBool::new(true),
            num_pes: num_pes,
            my_pe: rofi_get_id(),
            put_amt: Arc::new(AtomicUsize::new(0)),
            put_cnt: Arc::new(AtomicUsize::new(0)),
            get_amt: Arc::new(AtomicUsize::new(0)),
            get_cnt: Arc::new(AtomicUsize::new(0)),
            comm_mutex: Arc::new(Mutex::new(())),
            // drop_set: Arc::new(Mutex::new(Vec::new())),
            // any_dropped: Arc::new(AtomicBool::new(false)),
            // alloc_mutex: Arc::new(Mutex::new(())),
        };
        trace!(
            "[{:?}] Rofi base addr 0x{:x}",
            rofi.my_pe,
            *rofi.rofi_base_address.read()
        );
        rofi.alloc.write()[0].init(addr as usize, total_mem);
        rofi
    }

    unsafe fn fill_buffer<R: Copy, T>(&self, dst_addr: &mut [T], val: R) {
        // println!("{:?} {:?} {:?} {:?} {:?}",std::mem::size_of::<T>(),std::mem::size_of::<R>(),(dst_addr.len()*std::mem::size_of::<T>()),(dst_addr.len()*std::mem::size_of::<T>())/std::mem::size_of::<R>(),(dst_addr.len()*std::mem::size_of::<T>())%std::mem::size_of::<R>());
        let bytes = std::slice::from_raw_parts_mut(
            dst_addr.as_ptr() as *mut T as *mut R,
            (dst_addr.len() * std::mem::size_of::<T>()) / std::mem::size_of::<R>(),
        );
        for elem in bytes.iter_mut() {
            *elem = val;
        }
    }
    fn init_buffer<T>(&self, dst_addr: &mut [T]) {
        let bytes_len = dst_addr.len() * std::mem::size_of::<T>();
        // println!("{:?} {:?}",dst_addr.as_ptr(),bytes_len);
        unsafe {
            if bytes_len % std::mem::size_of::<u64>() == 0 {
                self.fill_buffer(dst_addr, ROFI_MAGIC_8);
            } else if bytes_len % std::mem::size_of::<u32>() == 0 {
                self.fill_buffer(dst_addr, ROFI_MAGIC_4);
            } else if bytes_len % std::mem::size_of::<u16>() == 0 {
                self.fill_buffer(dst_addr, ROFI_MAGIC_2);
            } else {
                self.fill_buffer(dst_addr, ROFI_MAGIC_1);
            }
        }
    }
    unsafe fn check_buffer_elems<R: std::cmp::PartialEq + std::fmt::Debug, T>(&self, dst_addr: &mut [T], val: R) -> TxResult<()>{
        let bytes = std::slice::from_raw_parts_mut(
            dst_addr.as_ptr() as *mut T as *mut R,
            (dst_addr.len() * std::mem::size_of::<T>()) / std::mem::size_of::<R>(),
        );
        let mut timer = std::time::Instant::now();
        for i in 0..(bytes.len() as isize - 2) {
            while bytes[i as usize] == val && bytes[i as usize + 1] == val {
                if timer.elapsed().as_secs_f64() > 1.0{
                    println!("{:?}: {:?} {:?} {:?}",i,bytes[i as usize],bytes[i as usize + 1],val);
                    return Err(TxError::GetError);
                }
                //hopefully magic number doesnt appear twice in a row
                std::thread::yield_now();
            }
        }
        timer = std::time::Instant::now();
        while bytes[bytes.len() - 1] == val {
            if timer.elapsed().as_secs_f64() > 1.0{
                println!("{:?}",bytes[bytes.len() - 1]);
                return Err(TxError::GetError);
            }
            //hopefully magic number isn't the last element
            std::thread::yield_now();
        }
        Ok(())
    }
    fn check_buffer<T>(&self, dst_addr: &mut [T]) -> TxResult<()>{
        let bytes_len = dst_addr.len() * std::mem::size_of::<T>();
        unsafe {
            if bytes_len % std::mem::size_of::<u64>() == 0 {
                self.check_buffer_elems(dst_addr, ROFI_MAGIC_8)?;
            } else if bytes_len % std::mem::size_of::<u32>() == 0 {
                self.check_buffer_elems(dst_addr, ROFI_MAGIC_4)?;
            } else if bytes_len % std::mem::size_of::<u16>() == 0 {
                self.check_buffer_elems(dst_addr, ROFI_MAGIC_2)?;
            } else {
                self.check_buffer_elems(dst_addr, ROFI_MAGIC_1)?;
            }
        }
        Ok(())
    }
    fn iget_data<T: Remote>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]) {
        let _lock = self.comm_mutex.lock();
        match rofi_iget(src_addr, dst_addr, pe) {
            //.expect("error in rofi get")
            Err(ret) => {
                println!(
                        "Error in get from {:?} src {:x} base_addr {:x} dst_addr {:p} size {:?} ret {:?}",
                        pe,
                        src_addr,
                        *self.rofi_base_address.read(),
                        dst_addr,
                        dst_addr.len(),
                        ret,
                    );
                panic!();
            }
            Ok(_ret) => {
                self.get_amt
                    .fetch_add(dst_addr.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
                self.get_cnt.fetch_add(1, Ordering::SeqCst);
                // if ret != 0{
                //     req.txids.push(ret);
                // }
            }
        }
    }
}

impl CommOps for RofiComm {
    fn my_pe(&self) -> usize {
        self.my_pe
    }
    fn num_pes(&self) -> usize {
        self.num_pes
    }
    fn barrier(&self) {
        rofi_barrier();
    }

    fn occupied(&self) -> usize {
        let mut occupied = 0;
        let allocs = self.alloc.read();
        for alloc in allocs.iter() {
            occupied += alloc.occupied();
        }
        occupied
    }
    fn num_pool_allocs(&self) -> usize {
        self.alloc.read().len()
    }
    fn print_pools(&self) {
        let allocs = self.alloc.read();
        println!("num_pools {:?}", allocs.len());
        for alloc in allocs.iter() {
            println!(
                "{:x} {:?} {:?} {:?}",
                alloc.start_addr,
                alloc.max_size,
                alloc.occupied(),
                alloc.space_avail()
            );
        }
    }
    fn alloc_pool(&self, min_size: usize) {
        let mut allocs = self.alloc.write();
        let size = std::cmp::max(min_size * 2 * self.num_pes, ROFI_MEM.load(Ordering::SeqCst));
        if let Ok(addr) = self.alloc(size, AllocationType::Global) {
            // println!("addr: {:x} - {:x}",addr, addr+size);
            let mut new_alloc = BTreeAlloc::new("rofi_mem".to_string());
            new_alloc.init(addr, size);
            allocs.push(new_alloc)
        } else {
            panic!("[Error] out of system memory");
        }
    }
    fn rt_alloc(&self, size: usize) -> AllocResult<usize> {
        // let size = size + size%8;
        let allocs = self.alloc.read();
        for alloc in allocs.iter() {
            if let Some(addr) = alloc.try_malloc(size) {
                return Ok(addr);
            }
            // println!("size: {:?} remaining {:?} occupied {:?} len {:?}",size, alloc.space_avail(),alloc.occupied(),allocs.len());
        }
        Err(AllocError::OutOfMemoryError(size))
    }
    fn rt_check_alloc(&self, size: usize) -> bool {
        let allocs = self.alloc.read();
        for alloc in allocs.iter() {
            if alloc.fake_malloc(size) {
                return true;
            }
        }
        false
    }

    fn rt_free(&self, addr: usize) {
        let allocs = self.alloc.read();
        for alloc in allocs.iter() {
            if let Ok(_) = alloc.free(addr) {
                return;
            }
        }
        panic!("Error invalid free! {:?}", addr);
    }
    fn alloc(&self, size: usize, alloc: AllocationType) -> AllocResult<usize> {
        // let size = size + size%8;
        let _lock = self.comm_mutex.lock();
        Ok(rofi_alloc(size, alloc) as usize)
    }

    fn free(&self, addr: usize) {
        let _lock = self.comm_mutex.lock();
        rofi_release(addr);
    }

    #[allow(dead_code)]
    fn base_addr(&self) -> usize {
        *self.rofi_base_address.read()
    }

    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize {
        rofi_local_addr(remote_pe, remote_addr)
    }

    fn remote_addr(&self, pe: usize, local_addr: usize) -> usize {
        rofi_remote_addr(pe, local_addr)
    }

    fn put<T: Remote>(&self, pe: usize, src_addr: &[T], dst_addr: usize) {
        //-> RofiReq {
        // let mut req = RofiReq{
        //     txids: Vec::new(),
        //     _drop_set: self.drop_set.clone(),
        //     _any_dropped: self.any_dropped.clone(),
        // };
        if pe != self.my_pe {
            let _lock = self.comm_mutex.lock();
            // println!("[{:?}]-({:?}) put [{:?}] entry",self.my_pe,0,pe);

            let _txid = unsafe { rofi_put(src_addr, dst_addr, pe).expect("error in rofi put") };
            self.put_amt
                .fetch_add(src_addr.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
            self.put_cnt.fetch_add(1, Ordering::SeqCst);
            // if txid != 0{
            //     req.txids.push(txid);
            // }
        } else {
            unsafe {
                // println!("[{:?}]-({:?}) memcopy {:?} into {:x}",pe,src_addr.as_ptr(),src_addr.len(),dst_addr);
                std::ptr::copy_nonoverlapping(
                    src_addr.as_ptr(),
                    dst_addr as *mut T,
                    src_addr.len(),
                );
            }
        }
        // req
        // println!("[{:?}]- gc: {:?} pc: {:?} put exit",self.my_pe,self.get_cnt.load(Ordering::SeqCst),self.put_cnt.load(Ordering::SeqCst));
        // println!("[{:?}]-({:?}) put [{:?}] exit",self.my_pe,thread::current().id(),pe);
    }

    fn iput<T: Remote>(&self, pe: usize, src_addr: &[T], dst_addr: usize) {
        //-> RofiReq{
        // println!("[{:?}]-({:?}) iput entry",self.my_pe,thread::current().id());
        // let mut req = RofiReq{
        //     txids: Vec::new(),
        //     _drop_set: self.drop_set.clone(),
        //     _any_dropped: self.any_dropped.clone(),
        // };
        if pe != self.my_pe {
            let _lock = self.comm_mutex.lock();
            let _txid = rofi_iput(src_addr, dst_addr, pe).expect("error in rofi put");
            self.put_amt
                .fetch_add(src_addr.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
            self.put_cnt.fetch_add(1, Ordering::SeqCst);
            // if txid != 0{
            //     req.txids.push(txid);
            // }
        } else {
            unsafe {
                // println!("[{:?}]-({:?}) memcopy {:?}",pe,src_addr.as_ptr());
                std::ptr::copy_nonoverlapping(
                    src_addr.as_ptr(),
                    dst_addr as *mut T,
                    src_addr.len(),
                );
            }
        }
        // req
        // println!("[{:?}]-({:?}) iput exit",self.my_pe,thread::current().id());

        // println!("[{:?}]- gc: {:?} pc: {:?} iput exit",self.my_pe,self.get_cnt.load(Ordering::SeqCst),self.put_cnt.load(Ordering::SeqCst));
    }

    fn put_all<T: Remote>(&self, src_addr: &[T], dst_addr: usize) {
        //-> RofiReq {
        // println!("[{:?}]-({:?}) put all entry",self.my_pe,thread::current().id());
        // let mut req = RofiReq{
        //     txids: Vec::new(),
        //     _drop_set: self.drop_set.clone(),
        //     _any_dropped: self.any_dropped.clone(),
        // };
        for pe in 0..self.my_pe {
            let _lock = self.comm_mutex.lock();
            let _txid = unsafe { rofi_put(src_addr, dst_addr, pe).expect("error in rofi put") };
            // if txid != 0 {
            //     req.txids.push(txid);
            // }
        }
        for pe in self.my_pe + 1..self.num_pes {
            let _lock = self.comm_mutex.lock();
            let _txid = unsafe { rofi_put(src_addr, dst_addr, pe).expect("error in rofi put") };
            // if txid != 0 {
            //     req.txids.push(txid);
            // }
        }
        unsafe {
            std::ptr::copy_nonoverlapping(src_addr.as_ptr(), dst_addr as *mut T, src_addr.len());
        }
        self.put_amt.fetch_add(
            src_addr.len() * (self.num_pes - 1) * std::mem::size_of::<T>(),
            Ordering::SeqCst,
        );
        self.put_cnt.fetch_add(1, Ordering::SeqCst);
        // req
        // println!("[{:?}]-({:?}) put all exit",self.my_pe,thread::current().id());
        // println!("[{:?}]- gc: {:?} pc: {:?} put_all exit",self.my_pe,self.get_cnt.load(Ordering::SeqCst),self.put_cnt.load(Ordering::SeqCst));
    }

    fn get<T: Remote>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]) {
        //-> RofiReq {
        // println!("[{:?}]-({:?}) get entry",self.my_pe,thread::current().id());
        // let mut req = RofiReq{
        //     txids: Vec::new(),
        //     _drop_set: self.drop_set.clone(),
        //     _any_dropped: self.any_dropped.clone(),
        // };
        if pe != self.my_pe {
            unsafe {
                let _lock = self.comm_mutex.lock();
                match rofi_get(src_addr, dst_addr, pe) {
                    //.expect("error in rofi get")
                    Err(ret) => {
                        println!(
                            "Error in get from {:?} src {:x} base_addr {:x} dst_addr {:p} size {:?} ret {:?}",
                            pe,
                            src_addr,
                            *self.rofi_base_address.read(),
                            dst_addr,
                            dst_addr.len(),
                            ret,
                        );
                        panic!();
                    }
                    Ok(_ret) => {
                        self.get_amt
                            .fetch_add(dst_addr.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
                        self.get_cnt.fetch_add(1, Ordering::SeqCst);
                        // if ret != 0{
                        //     req.txids.push(ret);
                        // }
                    }
                }
            }
        } else {
            // println!("[{:?}]-{:?} {:?} {:?}",self.my_pe,src_addr as *const T,dst_addr.as_mut_ptr(),dst_addr.len());
            unsafe {
                std::ptr::copy_nonoverlapping(
                    src_addr as *const T,
                    dst_addr.as_mut_ptr(),
                    dst_addr.len(),
                );
            }
        }
        // req
        // println!("[{:?}]- gc: {:?} pc: {:?} get exit",self.my_pe,self.get_cnt.load(Ordering::SeqCst),self.put_cnt.load(Ordering::SeqCst));
    }

    fn iget<T: Remote>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]) {
        if pe != self.my_pe {
            let bytes_len = dst_addr.len() * std::mem::size_of::<T>();
            let rem_bytes = bytes_len % std::mem::size_of::<u64>();
            // println!("{:x} {:?} {:?} {:?}",src_addr,dst_addr.as_ptr(),bytes_len,rem_bytes);
            if bytes_len >= std::mem::size_of::<u64>() {
                let temp_dst_addr = &mut dst_addr[rem_bytes..];
                self.init_buffer(temp_dst_addr);
                self.iget_data(pe, src_addr + rem_bytes, temp_dst_addr);
                while let Err(TxError::GetError) = self.check_buffer(temp_dst_addr){
                    self.iget_data(pe, src_addr + rem_bytes, temp_dst_addr);
                }
            }
            if rem_bytes > 0 {
                loop {
                    if let Ok(addr) = self.rt_alloc(rem_bytes) {
                        unsafe {
                            let temp_dst_addr = &mut dst_addr[0..rem_bytes];
                            let buf1 = std::slice::from_raw_parts_mut(
                                addr as *mut T as *mut u8,
                                rem_bytes,
                            );
                            let buf0 = std::slice::from_raw_parts(
                                temp_dst_addr.as_ptr() as *mut T as *mut u8,
                                rem_bytes,
                            );
                            self.fill_buffer(temp_dst_addr, 0u8);
                            self.fill_buffer(buf1, 1u8);

                            self.iget_data(pe, src_addr, temp_dst_addr);
                            self.iget_data(pe, src_addr, buf1);

                            let mut timer = std::time::Instant::now();
                            for i in 0..temp_dst_addr.len() {
                                while buf0[i] != buf1[i] {
                                    std::thread::yield_now();
                                    if timer.elapsed().as_secs_f64() > 1.0 {
                                        println!("iget {:?} {:?} {:?}",i,buf0[i],buf1[i]);
                                        self.iget_data(pe, src_addr, temp_dst_addr);
                                        self.iget_data(pe, src_addr, buf1);
                                        timer = std::time::Instant::now();
                                    }
                                }
                            }
                            // println!("{:?} {:?}",buf0,buf1);
                        }
                        self.rt_free(addr);
                        break;
                    }
                    std::thread::yield_now();
                }
            }
        } else {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    src_addr as *const T,
                    dst_addr.as_mut_ptr(),
                    dst_addr.len(),
                );
            }
        }
    }

    //src address is relative to rofi base addr
    fn iget_relative<T: Remote>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]) {
        //-> RofiReq {
        // let mut req = RofiReq{
        //     txids: Vec::new(),
        //     _drop_set: self.drop_set.clone(),
        //     _any_dropped: self.any_dropped.clone(),
        // };
        if pe != self.my_pe {
            // unsafe {
            let _lock = self.comm_mutex.lock();
            // println!("[{:?}]-({:?}) iget_relative [{:?}] entry",self.my_pe,thread::current().id(),pe);

            match rofi_iget(*self.rofi_base_address.read() + src_addr, dst_addr, pe) {
                //.expect("error in rofi get")
                Err(_ret) => {
                    println!(
                        "[{:?}] Error in iget_relative from {:?} src_addr {:x} ({:x}) dst_addr {:?} base_addr {:x} size {:?}",
                        self.my_pe,
                        pe,
                        src_addr,
                        src_addr+*self.rofi_base_address.read() ,
                        dst_addr.as_ptr(),
                        *self.rofi_base_address.read(),
                        dst_addr.len()
                    );
                    panic!();
                }
                Ok(_ret) => {
                    self.get_cnt.fetch_add(1, Ordering::SeqCst);
                    self.get_amt
                        .fetch_add(dst_addr.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
                    // if ret != 0{
                    //     req.txids.push(ret);
                    // }
                }
            }
            // };
        } else {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    src_addr as *const T,
                    dst_addr.as_mut_ptr(),
                    dst_addr.len(),
                );
            }
        }
        // req
        // println!("[{:?}]- gc: {:?} pc: {:?} iget_relative exit",self.my_pe,self.get_cnt.load(Ordering::SeqCst),self.put_cnt.load(Ordering::SeqCst));
        // println!("[{:?}]-({:?}) iget relative [{:?}] exit",self.my_pe,thread::current().id(),pe);
    }
}

//#[prof]
impl Drop for RofiComm {
    fn drop(&mut self) {
        // println!("[{:?}] in rofi comm drop", self.my_pe);
        // print!(""); //not sure why this prevents hanging....
        // rofi_barrier();
        // std::thread::sleep(std::time::Duration::from_millis(1000));

        if self.occupied() > 0 {
            println!("dropping rofi -- memory in use {:?}", self.occupied());
        }
        if self.alloc.read().len() > 1 {
            println!("[LAMELLAR INFO] {:?} additional rt memory pools were allocated, performance may be increased using a larger initial pool, set using the LAMELLAR_MEM_SIZE envrionment variable. Current initial size = {:?}",self.alloc.read().len()-1, ROFI_MEM.load(Ordering::SeqCst));
        }
        rofi_barrier();
        // std::thread::sleep(std::time::Duration::from_millis(1000));
        // //we can probably do a final "put" to each node where we specify we we are done, then once all nodes have done this no further communication amongst them occurs...
        let _res = rofi_finit();
        // std::thread::sleep(std::time::Duration::from_millis(1000));
        // println!("[{:?}] dropping rofi comm", self.my_pe);
    }
}

pub(crate) struct RofiData {
    pub(crate) addr: usize,          // process space address
    pub(crate) relative_addr: usize, //address allocated from rofi
    pub(crate) len: usize,
    pub(crate) data_start: usize,
    pub(crate) data_len: usize,
    pub(crate) rofi_comm: Arc<Comm>, //Comm instead of RofiComm because I can't figure out how to make work with Enum_distpatch....
    pub(crate) alloc_size: usize,
}

impl RofiData {
    pub fn new(rofi_comm: Arc<Comm>, size: usize) -> Result<RofiData, anyhow::Error> {
        let ref_cnt_size = std::mem::size_of::<AtomicUsize>();
        let alloc_size = size + ref_cnt_size; //+  std::mem::size_of::<u64>();
        let relative_addr = rofi_comm.rt_alloc(alloc_size)?;
        let addr = relative_addr; // + rofi_comm.base_addr();
        unsafe {
            let ref_cnt = addr as *const AtomicUsize;
            (*ref_cnt).store(1, Ordering::SeqCst)
        };
        Ok(RofiData {
            addr: addr,
            relative_addr: relative_addr + ref_cnt_size,
            data_start: addr
                + std::mem::size_of::<AtomicUsize>()
                + std::mem::size_of::<Option<SerializeHeader>>(),
            len: size, // + std::mem::size_of::<u64>(),
            data_len: size - std::mem::size_of::<Option<SerializeHeader>>(),
            rofi_comm: rofi_comm,
            alloc_size: alloc_size,
        })
    }
}
impl SerializedDataOps for RofiData {
    fn header_as_bytes(&self) -> &mut [u8] {
        let header_size = std::mem::size_of::<Option<SerializeHeader>>();
        unsafe {
            std::slice::from_raw_parts_mut(
                (self.addr + std::mem::size_of::<AtomicUsize>()) as *mut u8,
                header_size,
            )
        }
    }

    fn increment_cnt(&self) {
        unsafe { (*(self.addr as *const AtomicUsize)).fetch_add(1, Ordering::SeqCst) };
    }

    fn len(&self) -> usize {
        self.len
    }
}

impl Des for RofiData {
    fn deserialize_header(&self) -> Option<SerializeHeader> {
        bincode::deserialize(self.header_as_bytes()).unwrap()
    }
    fn deserialize_data<T: serde::de::DeserializeOwned>(&self) -> Result<T, anyhow::Error> {
        Ok(bincode::deserialize(self.data_as_bytes())?)
    }
    fn data_as_bytes(&self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut((self.data_start) as *mut u8, self.data_len) }
    }
    fn header_and_data_as_bytes(&self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                (self.addr + std::mem::size_of::<AtomicUsize>()) as *mut u8,
                self.len,
            )
        }
    }
    fn print(&self) {
        println!(
            "addr: {:x} relative addr {:x} len {:?} data_start {:x} data_len {:?} alloc_size {:?}",
            self.addr,
            self.relative_addr,
            self.len,
            self.data_start,
            self.data_len,
            self.alloc_size
        );
    }
}

impl SubData for RofiData {
    fn sub_data(&self, start: usize, end: usize) -> SerializedData {
        let mut sub = self.clone();
        sub.data_start += start;
        sub.data_len = end - start;
        SerializedData::RofiData(sub)
    }
}

impl Clone for RofiData {
    fn clone(&self) -> Self {
        unsafe {
            let ref_cnt = self.addr as *const AtomicUsize;
            (*ref_cnt).fetch_add(1, Ordering::SeqCst)
        };
        RofiData {
            addr: self.addr,
            relative_addr: self.relative_addr,
            len: self.len,
            data_start: self.data_start,
            data_len: self.data_len,
            rofi_comm: self.rofi_comm.clone(),
            alloc_size: self.alloc_size,
        }
    }
}

impl Drop for RofiData {
    fn drop(&mut self) {
        let cnt = unsafe { (*(self.addr as *const AtomicUsize)).fetch_sub(1, Ordering::SeqCst) };
        if cnt == 1 {
            self.rofi_comm.rt_free(self.addr);
        }
    }
}

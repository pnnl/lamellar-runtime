use crate::config;
use crate::lamellae::comm::{AllocError, AllocResult, Comm, CommOps, Remote};
use crate::lamellae::command_queues::CommandQueue;
use crate::lamellae::{
    AllocationType, Des, SerializeHeader, SerializedData, SerializedDataOps, SubData,
    SERIALIZE_HEADER_LEN,
};
use crate::lamellar_alloc::BTreeAlloc;
use crate::lamellar_alloc::LamellarAlloc;
use libfabric::*;
use parking_lot::{Mutex, RwLock};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use super::ofi::Ofi;

#[derive(Debug, Clone, Copy)]
pub(crate) enum TxError {
    GetError,
}
impl std::fmt::Display for TxError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TxError::GetError => {
                write!(f, "error performing get")
            }
        }
    }
}
impl std::error::Error for TxError {}
pub(crate) type TxResult<T> = Result<T, TxError>;
static LIBFAB_MAGIC_8: u64 = 0b1101001110111100011111001001100100111110011001100011110111001011;
static LIBFAB_MAGIC_4: u32 = 0b10001111100100110010011111001100;
static LIBFAB_MAGIC_2: u16 = 0b1100100110010011;
static LIBFAB_MAGIC_1: u8 = 0b10011001;

pub(crate) static LIBFAB_MEM: AtomicUsize = AtomicUsize::new(4 * 1024 * 1024 * 1024);
const RT_MEM: usize = 100 * 1024 * 1024; // we add this space for things like team barrier buffers, but will work towards having teams get memory from rofi allocs
                                         // #[derive(Debug)]
pub(crate) struct LibFabComm {
    pub(crate) base_address: Arc<RwLock<usize>>,
    alloc: RwLock<Vec<BTreeAlloc>>,
    _init: AtomicBool,
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    pub(crate) put_amt: Arc<AtomicUsize>,
    pub(crate) put_cnt: Arc<AtomicUsize>,
    pub(crate) get_amt: Arc<AtomicUsize>,
    pub(crate) get_cnt: Arc<AtomicUsize>,
    ofi: Arc<Mutex<Ofi>>,
}

impl std::fmt::Debug for LibFabComm {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "my_pes: {}", self.my_pe)?;
        write!(f, "num_pes: {}", self.num_pes)?;
        write!(f, "put_amt: {}", self.put_amt.load(Ordering::SeqCst))?;
        write!(f, "put_cnt: {}", self.put_cnt.load(Ordering::SeqCst))?;
        write!(f, "get_amt: {}", self.get_amt.load(Ordering::SeqCst))?;
        write!(f, "get_cnt: {}", self.get_cnt.load(Ordering::SeqCst))
    }
}

impl LibFabComm {
    pub(crate) fn new(
        provider: Option<&str>,
        domain: Option<&str>,
    ) -> Result<Self, libfabric::error::Error> {
        if let Some(size) = config().heap_size {
            // if let Ok(size) = std::env::var("LAMELLAR_MEM_SIZE") {
            // let size = size
            //     .parse::<usize>()
            //     .expect("invalid memory size, please supply size in bytes");
            LIBFAB_MEM.store(size, Ordering::SeqCst);
        }

        let ofi = Ofi::new(provider, domain)?;

        let cmd_q_mem = CommandQueue::mem_per_pe() * ofi.num_pes;

        let total_mem = cmd_q_mem + RT_MEM + LIBFAB_MEM.load(Ordering::SeqCst);
        // println!("rofi comm total_mem {:?}",total_mem);
        let all_pes: Vec<_> = (0..ofi.num_pes).collect();
        let addr = ofi.sub_alloc(&all_pes, total_mem)?;

        let libfab = Self {
            base_address: Arc::new(RwLock::new(addr as usize)),
            alloc: RwLock::new(vec![BTreeAlloc::new("libfab_mem".to_string())]),
            _init: AtomicBool::new(true),
            my_pe: ofi.my_pe,
            num_pes: ofi.num_pes,
            put_amt: Arc::new(AtomicUsize::new(0)),
            put_cnt: Arc::new(AtomicUsize::new(0)),
            get_amt: Arc::new(AtomicUsize::new(0)),
            get_cnt: Arc::new(AtomicUsize::new(0)),
            ofi: Arc::new(Mutex::new(ofi)),
        };

        libfab.alloc.write()[0].init(addr as usize, total_mem);
        println!("Libfab is ready");
        Ok(libfab)
    }

    //#[tracing::instrument(skip_all)]
    unsafe fn fill_buffer<R: Copy, T>(&self, dst_addr: &mut [T], val: R) {
        let num_r = (dst_addr.len() * std::mem::size_of::<T>()) / std::mem::size_of::<R>();
        let r_ptr = dst_addr.as_ptr() as *mut T as *mut R;
        for i in 0..num_r {
            r_ptr.offset(i as isize).write_unaligned(val);
        }
    }
    //#[tracing::instrument(skip_all)]
    fn init_buffer<T>(&self, dst_addr: &mut [T]) {
        let bytes_len = dst_addr.len() * std::mem::size_of::<T>();
        // println!("{:?} {:?}", dst_addr.as_ptr(), bytes_len);
        unsafe {
            if bytes_len % std::mem::size_of::<u64>() == 0 {
                self.fill_buffer(dst_addr, LIBFAB_MAGIC_8);
            } else if bytes_len % std::mem::size_of::<u32>() == 0 {
                self.fill_buffer(dst_addr, LIBFAB_MAGIC_4);
            } else if bytes_len % std::mem::size_of::<u16>() == 0 {
                self.fill_buffer(dst_addr, LIBFAB_MAGIC_2);
            } else {
                self.fill_buffer(dst_addr, LIBFAB_MAGIC_1);
            }
        }
    }
    //#[tracing::instrument(skip_all)]
    unsafe fn check_buffer_elems<R: std::cmp::PartialEq + std::fmt::Debug, T>(
        &self,
        dst_addr: &mut [T],
        val: R,
    ) -> TxResult<()> {
        let num_r = (dst_addr.len() * std::mem::size_of::<T>()) / std::mem::size_of::<R>();
        let r_ptr = dst_addr.as_ptr() as *mut T as *mut R;

        let mut timer = std::time::Instant::now();
        for i in 0..num_r - 2 {
            while r_ptr.offset(i as isize).read_unaligned() == val
                && r_ptr.offset(i as isize + 1).read_unaligned() == val
            {
                if timer.elapsed().as_secs_f64() > 1.0 {
                    // println!(
                    //     "{:?}/{:?}: {:?} {:?} {:?}",
                    //     i,
                    //     num_r,
                    //     r_ptr.offset(i as isize).read_unaligned(),
                    //     r_ptr.offset(i as isize + 1).read_unaligned(),
                    //     val
                    // );
                    return Err(TxError::GetError);
                }
                //hopefully magic number doesnt appear twice in a row
                std::thread::yield_now();
            }
            timer = std::time::Instant::now();
        }
        timer = std::time::Instant::now();
        while r_ptr.offset(num_r as isize - 1).read_unaligned() == val {
            if timer.elapsed().as_secs_f64() > 1.0 {
                // println!("{:?}", bytes[bytes.len() - 1]);
                return Err(TxError::GetError);
            }
            //hopefully magic number isn't the last element
            std::thread::yield_now();
        }
        Ok(())
    }
    //#[tracing::instrument(skip_all)]
    fn check_buffer<T>(&self, dst_addr: &mut [T]) -> TxResult<()> {
        let bytes_len = dst_addr.len() * std::mem::size_of::<T>();
        unsafe {
            if bytes_len % std::mem::size_of::<u64>() == 0 {
                self.check_buffer_elems(dst_addr, LIBFAB_MAGIC_8)?;
            } else if bytes_len % std::mem::size_of::<u32>() == 0 {
                self.check_buffer_elems(dst_addr, LIBFAB_MAGIC_4)?;
            } else if bytes_len % std::mem::size_of::<u16>() == 0 {
                self.check_buffer_elems(dst_addr, LIBFAB_MAGIC_2)?;
            } else {
                self.check_buffer_elems(dst_addr, LIBFAB_MAGIC_1)?;
            }
        }
        Ok(())
    }
    //#[tracing::instrument(skip_all)]
    fn iget_data<T: Remote>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]) {
        // println!("iget_data {:?} {:x} {:?}", pe, src_addr, dst_addr.as_ptr());
        // let _lock = self.comm_mutex.write();
        match unsafe { self.ofi.lock().get(pe, src_addr, dst_addr, true) } {
            //.expect("error in rofi get")
            Err(ret) => {
                println!(
                        "Error in get from {:?} src {:x} base_addr {:x} dst_addr {:p} size {:?} ret {:?}",
                        pe,
                        src_addr,
                        *self.base_address.read(),
                        dst_addr,
                        dst_addr.len(),
                        ret,
                    );
                panic!();
            }
            Ok(_) => {}
        }
    }
    pub(crate) fn heap_size() -> usize {
        LIBFAB_MEM.load(Ordering::SeqCst)
    }
}

impl CommOps for LibFabComm {
    fn my_pe(&self) -> usize {
        self.my_pe
    }

    fn num_pes(&self) -> usize {
        self.num_pes
    }

    async fn barrier(&self) {
        let all_pes: Vec<_> = (0..self.num_pes).collect();
        self.ofi.lock().sub_barrier(&all_pes).unwrap();
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

    async fn alloc_pool(&self, min_size: usize) {
        let mut allocs = self.alloc.write();
        let size = std::cmp::max(
            min_size * 2 * self.num_pes,
            LIBFAB_MEM.load(Ordering::SeqCst),
        );
        if let Ok(addr) = self.alloc(size, AllocationType::Global).await {
            // println!("addr: {:x} - {:x}",addr, addr+size);
            let mut new_alloc = BTreeAlloc::new("libfab_mem".to_string());
            new_alloc.init(addr, size);
            allocs.push(new_alloc)
        } else {
            panic!("[Error] out of system memory");
        }
    }

    fn rt_alloc(&self, size: usize, align: usize) -> AllocResult<usize> {
        // println!("rt_alloc size {size} align {align}");
        // let size = size + size%8;
        let allocs = self.alloc.read();
        for alloc in allocs.iter() {
            // println!("size: {:?} remaining {:?} occupied {:?} len {:?}",size, alloc.space_avail(),alloc.occupied(),allocs.len());

            if let Some(addr) = alloc.try_malloc(size, align) {
                return Ok(addr);
            }
            // println!("size: {:?} remaining {:?} occupied {:?} len {:?}",size, alloc.space_avail(),alloc.occupied(),allocs.len());
        }
        Err(AllocError::OutOfMemoryError(size))
    }

    fn rt_check_alloc(&self, size: usize, align: usize) -> bool {
        let allocs = self.alloc.read();
        for alloc in allocs.iter() {
            if alloc.fake_malloc(size, align) {
                // println!("fake alloc passes");
                return true;
            }
        }
        // println!("fake alloc fails");
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

    async fn alloc(&self, size: usize, alloc: AllocationType) -> AllocResult<usize> {
        match alloc {
            AllocationType::Local => todo!(),
            AllocationType::Global => {
                let pes: Vec<_> = (0..self.num_pes).collect();
                Ok(self.ofi.lock().sub_alloc(&pes, size).unwrap())
            }
            AllocationType::Sub(pes) => Ok(self.ofi.lock().sub_alloc(&pes, size).unwrap()),
        }
    }

    fn free(&self, addr: usize) {
        self.ofi.lock().release(&addr);
    }

    fn base_addr(&self) -> usize {
        *self.base_address.read()
    }

    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize {
        self.ofi.lock().local_addr(&remote_pe, &remote_addr)
    }

    fn remote_addr(&self, pe: usize, local_addr: usize) -> usize {
        self.ofi.lock().remote_addr(&pe, &local_addr)
    }

    fn flush(&self) {
        self.ofi.lock().progress().unwrap()
    }

    async fn put<T: Remote>(&self, pe: usize, src_addr: &[T], dst_addr: usize) {
        if pe != self.my_pe {
            unsafe { self.ofi.lock().put(pe, src_addr, dst_addr, false) }.unwrap();
            self.put_amt
                .fetch_add(src_addr.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
        } else {
            unsafe {
                // println!(
                //     "[{:?}]-({:?}) memcopy {:?} into {:x} src align {:?} dst align {:?}",
                //     pe,
                //     src_addr.as_ptr(),
                //     src_addr.len(),
                //     dst_addr,
                //     src_addr.as_ptr().align_offset(std::mem::align_of::<T>()),
                //     (dst_addr as *mut T).align_offset(std::mem::align_of::<T>()),
                // );
                std::ptr::copy(src_addr.as_ptr(), dst_addr as *mut T, src_addr.len());
            }
        }
    }

    fn iput<T: Remote>(&self, pe: usize, src_addr: &[T], dst_addr: usize) {
        if pe != self.my_pe {
            unsafe { self.ofi.lock().put(pe, src_addr, dst_addr, true) }.unwrap();
            self.put_amt
                .fetch_add(src_addr.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
        } else {
            unsafe {
                // println!("[{:?}]-({:?}) memcopy {:?}",pe,src_addr.as_ptr());
                std::ptr::copy(src_addr.as_ptr(), dst_addr as *mut T, src_addr.len());
            }
        }
    }

    async fn put_all<T: Remote>(&self, src_addr: &[T], dst_addr: usize) {
        for pe in 0..self.my_pe {
            unsafe { self.ofi.lock().put(pe, src_addr, dst_addr, false) }.unwrap()
        }

        for pe in self.my_pe..self.num_pes {
            unsafe { self.ofi.lock().put(pe, src_addr, dst_addr, false) }.unwrap()
        }

        unsafe {
            std::ptr::copy(src_addr.as_ptr(), dst_addr as *mut T, src_addr.len());
        }

        self.put_amt.fetch_add(
            src_addr.len() * (self.num_pes - 1) * std::mem::size_of::<T>(),
            Ordering::SeqCst,
        );
        self.put_cnt.fetch_add(self.num_pes - 1, Ordering::SeqCst);
    }

    async fn get<T: Remote>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]) {
        if pe != self.my_pe {
            unsafe { self.ofi.lock().get(pe, src_addr, dst_addr, true) }.unwrap();
            self.get_amt
                .fetch_add(dst_addr.len() * std::mem::size_of::<T>(), Ordering::SeqCst);
        } else {
            // println!("[{:?}]-{:?} {:?} {:?}",self.my_pe,src_addr as *const T,dst_addr.as_mut_ptr(),dst_addr.len());
            unsafe {
                std::ptr::copy(src_addr as *const T, dst_addr.as_mut_ptr(), dst_addr.len());
            }
        }
    }

    //#[tracing::instrument(skip_all)]
    fn iget<T: Remote>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]) {
        if pe != self.my_pe {
            let bytes_len = dst_addr.len() * std::mem::size_of::<T>();
            self.get_amt.fetch_add(bytes_len, Ordering::SeqCst);
            // println!(
            //     "rofi_comm iget {:?} {:?} {:?}",
            //     dst_addr.len() * std::mem::size_of::<T>(),
            //     bytes_len,
            //     self.get_amt.load(Ordering::SeqCst)
            // );
            self.get_cnt.fetch_add(1, Ordering::SeqCst);
            let rem_bytes = bytes_len % std::mem::size_of::<u64>();
            // println!(
            //     "{:x} {:?} {:?} {:?}",
            //     src_addr,
            //     dst_addr.as_ptr(),
            //     bytes_len,
            //     rem_bytes
            // );
            if bytes_len >= std::mem::size_of::<u64>() {
                let temp_dst_addr = &mut dst_addr[rem_bytes..];
                self.init_buffer(temp_dst_addr);
                self.iget_data(pe, src_addr + rem_bytes, temp_dst_addr);
                while let Err(TxError::GetError) = self.check_buffer(temp_dst_addr) {
                    self.iget_data(pe, src_addr + rem_bytes, temp_dst_addr);
                }
            }
            if rem_bytes > 0 {
                loop {
                    if let Ok(addr) = self.rt_alloc(rem_bytes, std::mem::size_of::<u8>()) {
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
                                        // println!("iget {:?} {:?} {:?}", i, buf0[i], buf1[i]);
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
                std::ptr::copy(src_addr as *const T, dst_addr.as_mut_ptr(), dst_addr.len());
            }
        }
    }

    #[allow(non_snake_case)]
    fn MB_sent(&self) -> f64 {
        (self.put_amt.load(Ordering::SeqCst) + self.get_amt.load(Ordering::SeqCst)) as f64
            / 1_000_000.0
    }

    fn force_shutdown(&self) {
        todo!()
    }
}

impl Drop for LibFabComm {
    fn drop(&mut self) {
        println!("[{:?}] in rofi comm drop", self.my_pe);
        // print!(""); //not sure why this prevents hanging....
        // rofi_barrier();
        // std::thread::sleep(std::time::Duration::from_millis(1000));

        if self.occupied() > 0 {
            println!("dropping rofi -- memory in use {:?}", self.occupied());
        }
        if self.alloc.read().len() > 1 {
            println!("[LAMELLAR INFO] {:?} additional rt memory pools were allocated, performance may be increased using a larger initial pool, set using the LAMELLAR_MEM_SIZE envrionment variable. Current initial size = {:?}",self.alloc.read().len()-1, LIBFAB_MEM.load(Ordering::SeqCst));
        }
        // let _lock = self.comm_mutex.write();
        self.barrier();
        // std::thread::sleep(std::time::Duration::from_millis(1000));
        // //we can probably do a final "put" to each node where we specify we we are done, then once all nodes have done this no further communication amongst them occurs...

        // println!(
        //     "Rofi Drop #Gets: {:?} #Puts: {:?}",
        //     self.get_cnt.load(Ordering::SeqCst),
        //     self.put_cnt.load(Ordering::SeqCst)
        // );
        // println!(
        //     "Rofi Drop #Get amt: {:?} #Put amt: {:?}",
        //     self.get_amt.load(Ordering::SeqCst),
        //     self.put_amt.load(Ordering::SeqCst)
        // );
        // std::thread::sleep(std::time::Duration::from_millis(1000));
        // println!("[{:?}] dropping rofi comm", self.my_pe);
    }
}

#[derive(Debug)]
pub(crate) struct LibFabData {
    pub(crate) addr: usize,          // process space address)
    pub(crate) relative_addr: usize, //address allocated from rofi
    pub(crate) len: usize,
    pub(crate) data_start: usize,
    pub(crate) data_len: usize,
    pub(crate) libfab_comm: Arc<Comm>, //Comm instead of RofiComm because I can't figure out how to make work with Enum_distpatch....
    pub(crate) alloc_size: usize,
}

impl LibFabData {
    //#[tracing::instrument(skip_all)]
    pub(crate) fn new(libfab_comm: Arc<Comm>, size: usize) -> Result<LibFabData, anyhow::Error> {
        let ref_cnt_size = std::mem::size_of::<AtomicUsize>();
        let alloc_size = size + ref_cnt_size; //+  std::mem::size_of::<u64>();
        let relative_addr =
            libfab_comm.rt_alloc(alloc_size, std::mem::align_of::<AtomicUsize>())?;
        let addr = relative_addr; // + libfab_comm.base_addr();
        unsafe {
            let ref_cnt = addr as *const AtomicUsize;
            (*ref_cnt).store(1, Ordering::SeqCst)
        };
        Ok(LibFabData {
            addr: addr,
            relative_addr: relative_addr + ref_cnt_size,
            data_start: addr + std::mem::size_of::<AtomicUsize>() + *SERIALIZE_HEADER_LEN,
            len: size, // + std::mem::size_of::<u64>(),
            data_len: size - *SERIALIZE_HEADER_LEN,
            libfab_comm: libfab_comm,
            alloc_size: alloc_size,
        })
    }
}

impl SerializedDataOps for LibFabData {
    //#[tracing::instrument(skip_all)]
    fn header_as_bytes(&self) -> &mut [u8] {
        let header_size = *SERIALIZE_HEADER_LEN;
        // println!("header_as_bytes header_size: {:?}", header_size);
        unsafe {
            std::slice::from_raw_parts_mut(
                (self.addr + std::mem::size_of::<AtomicUsize>()) as *mut u8,
                header_size,
            )
        }
    }

    //#[tracing::instrument(skip_all)]
    fn increment_cnt(&self) {
        unsafe { (*(self.addr as *const AtomicUsize)).fetch_add(1, Ordering::SeqCst) };
    }

    //#[tracing::instrument(skip_all)]
    fn len(&self) -> usize {
        self.len
    }
}

impl Des for LibFabData {
    //#[tracing::instrument(skip_all)]
    fn deserialize_header(&self) -> Option<SerializeHeader> {
        crate::deserialize(self.header_as_bytes(), false).unwrap()
    }
    //#[tracing::instrument(skip_all)]
    fn deserialize_data<T: serde::de::DeserializeOwned>(&self) -> Result<T, anyhow::Error> {
        Ok(crate::deserialize(self.data_as_bytes(), true)?)
    }
    //#[tracing::instrument(skip_all)]
    fn data_as_bytes(&self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut((self.data_start) as *mut u8, self.data_len) }
    }
    //#[tracing::instrument(skip_all)]
    fn header_and_data_as_bytes(&self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                (self.addr + std::mem::size_of::<AtomicUsize>()) as *mut u8,
                self.len,
            )
        }
    }
    //#[tracing::instrument(skip_all)]
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

impl SubData for LibFabData {
    //#[tracing::instrument(skip_all)]
    fn sub_data(&self, start: usize, end: usize) -> SerializedData {
        let mut sub = self.clone();
        sub.data_start += start;
        sub.data_len = end - start;
        SerializedData::LibFabData(sub)
    }
}

impl Clone for LibFabData {
    //#[tracing::instrument(skip_all)]
    fn clone(&self) -> Self {
        unsafe {
            let ref_cnt = self.addr as *const AtomicUsize;
            (*ref_cnt).fetch_add(1, Ordering::SeqCst)
        };
        LibFabData {
            addr: self.addr,
            relative_addr: self.relative_addr,
            len: self.len,
            data_start: self.data_start,
            data_len: self.data_len,
            libfab_comm: self.libfab_comm.clone(),
            alloc_size: self.alloc_size,
        }
    }
}

impl Drop for LibFabData {
    //#[tracing::instrument(skip_all)]
    fn drop(&mut self) {
        let cnt = unsafe { (*(self.addr as *const AtomicUsize)).fetch_sub(1, Ordering::SeqCst) };
        if cnt == 1 {
            self.libfab_comm.rt_free(self.addr);
        }
    }
}

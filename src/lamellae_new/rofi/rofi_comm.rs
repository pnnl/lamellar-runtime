// use crate::lamellae_new::rofi::command_queues::RofiCommandQueue;
use crate::lamellae_new::rofi::rofi_api::*;
use crate::lamellae_new::AllocationType;
use crate::lamellar_alloc::{BTreeAlloc, LamellarAlloc};
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use log::trace;
use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

static ROFI_MEM: AtomicUsize = AtomicUsize::new(1 * 1024 * 1024 * 1024);
const RT_MEM: usize = 100 * 1024 * 1024; // we add this space for things like team barrier buffers, but will work towards having teams get memory from rofi allocs
pub(crate) struct RofiComm {
    pub(crate) rofi_base_address: Arc<RwLock<usize>>,
    alloc: BTreeAlloc,
    _init: AtomicBool,
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    pub(crate) put_amt: Arc<AtomicUsize>,
    put_cnt: Arc<AtomicUsize>,
    pub(crate) get_amt: Arc<AtomicUsize>,
    get_cnt: Arc<AtomicUsize>,
    comm_mutex: Arc<Mutex<()>>,
    alloc_mutex: Arc<Mutex<()>>,
}

//#[prof]
impl RofiComm {
    pub(crate) fn new(provider: &str) -> RofiComm {
        if let Ok(size) = std::env::var("LAMELLAR_ROFI_MEM_SIZE") {
            let size = size
                .parse::<usize>()
                .expect("invalid memory size, please supply size in bytes");
            ROFI_MEM.store(size, Ordering::SeqCst);
        }
        rofi_init(provider).expect("error in rofi init");
        trace!("rofi initialized");
        rofi_barrier();
        let num_pes = rofi_get_size();
        let cmd_q_mem = 0;//RofiCommandQueue::mem_per_pe() * num_pes;

        let total_mem = cmd_q_mem + RT_MEM + ROFI_MEM.load(Ordering::SeqCst);
        let addr = rofi_alloc(total_mem, AllocationType::Global);
        let mut rofi = RofiComm {
            rofi_base_address: Arc::new(RwLock::new(addr as usize)),
            alloc: BTreeAlloc::new("rofi_mem".to_string()),
            _init: AtomicBool::new(true),
            num_pes: num_pes,
            my_pe: rofi_get_id(),
            put_amt: Arc::new(AtomicUsize::new(0)),
            put_cnt: Arc::new(AtomicUsize::new(0)),
            get_amt: Arc::new(AtomicUsize::new(0)),
            get_cnt:  Arc::new(AtomicUsize::new(0)),
            comm_mutex: Arc::new(Mutex::new(())),
            alloc_mutex: Arc::new(Mutex::new(())),
        };
        trace!(
            "[{:?}] Rofi base addr 0x{:x}",
            rofi.my_pe,
            *rofi.rofi_base_address.read()
        );
        rofi.alloc.init(0, total_mem);
        rofi
    }
    pub(crate) fn finit(&self) {
        rofi_barrier();
        let _res = rofi_finit();
    }
    pub(crate) fn mype(&self) -> usize {
        self.my_pe
    }
    pub(crate) fn barrier(&self) {
        // println!("[{:?}]-({:?}) barrier entry",self.my_pe,thread::current().id());
        rofi_barrier();
        // println!("[{:?}]-({:?}) barrier exit",self.my_pe,thread::current().id());
    }
    pub(crate) fn rt_alloc(&self, size: usize) -> Option<usize> {
        // println!("[{:?}]-({:?}) rt_alloc entry",self.my_pe,thread::current().id());
        // let b =self.alloc.space_avail();
        if let Some(addr) = self.alloc.try_malloc(size) {
            // println!("[{:?}]-({:?}) rt_alloc exit",self.my_pe,thread::current().id());
            // println!("alloc addr {:?} free space {:?} (before: {:?}   alloc size {:?} ({:?}))", addr, self.alloc.space_avail(), b, b - self.alloc.space_avail(), size);
            Some(addr)
        } else {
            println!("[WARNING] out of memory: (work in progress on a scalable solution, as a work around try setting the LAMELLAR_ROFI_MEM_SIZE envrionment variable (current size = {:?} -- Note: LamellarLocalArrays are currently allocated out of this pool",ROFI_MEM.load(Ordering::SeqCst));
            // println!("[{:?}]-({:?}) rt_alloc exit",self.my_pe,thread::current().id());
            None
        }
    }
    #[allow(dead_code)]
    pub(crate) fn rt_free(&self, addr: usize) {
        // println!("[{:?}]-({:?}) rt_free entry",self.my_pe,thread::current().id());
        // let b =self.alloc.space_avail();
        self.alloc.free(addr);
        // println!("[{:?}]-({:?}) rt_free exit",self.my_pe,thread::current().id());
        // println!("free addr {:?} free space {:?} (before: {:?}   {:?})", addr, self.alloc.space_avail(), b, self.alloc.space_avail()-b);
    }
    pub(crate) fn alloc(&self, size: usize, alloc: AllocationType) -> Option<usize> {
        // println!("[{:?}]-({:?}) alloc entry",self.my_pe,thread::current().id());
        // if let Some(addr) = self.rt_alloc(size) {
        //     // println!("[{:?}]-({:?}) alloc exit",self.my_pe,thread::current().id());
        //     Some(addr + self.base_addr())
        // } else {
        //     // println!("[{:?}]-({:?}) alloc exit",self.my_pe,thread::current().id());
        //     None
        // }
        let _lock = self.alloc_mutex.lock();
        Some(rofi_alloc(size,alloc) as usize)
    }
    #[allow(dead_code)]
    pub(crate) fn free(&self, addr: usize) {
        // println!("[{:?}]-({:?}) free entry",self.my_pe,thread::current().id());
        // self.rt_free(addr - self.base_addr());
        // println!("[{:?}]-({:?}) free exit",self.my_pe,thread::current().id());
        let _lock = self.alloc_mutex.lock();
        rofi_release(addr);
        // println!("free addr {:?} free space {:?} (before: {:?}   {:?})", addr, self.alloc.space_avail(), b, self.alloc.space_avail()-b);
    }

    #[allow(dead_code)]
    pub(crate) fn base_addr(&self) -> usize {
        *self.rofi_base_address.read()
    }

    pub(crate) fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize {
        rofi_local_addr(remote_pe, remote_addr)
    }

    pub(crate) fn remote_addr(&self, pe: usize, local_addr: usize) -> usize {
        rofi_remote_addr(pe, local_addr)
    }

    //dst_addr relative to rofi_base_addr, need to calculate the real address
    pub(crate) fn local_store(&self, data: &[u8], dst_addr: usize) {
        let base_addr = *self.rofi_base_address.read();
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                (base_addr + dst_addr) as *mut u8,
                data.len(),
            );
        }
        //read lock dropped here
    }

    pub(crate) fn put<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    >(
        &self,
        pe: usize,
        src_addr: &[T],
        dst_addr: usize,
    ) {
        if pe != self.my_pe {
            let _lock = self.comm_mutex.lock();
            // println!("[{:?}]-({:?}) put [{:?}] entry",self.my_pe,thread::current().id(),pe);

            unsafe { rofi_put(src_addr, dst_addr, pe).expect("error in rofi put") };
            self.put_amt.fetch_add(src_addr.len(), Ordering::SeqCst);
            self.put_cnt.fetch_add(1, Ordering::SeqCst);
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
        // println!("[{:?}]- gc: {:?} pc: {:?} put exit",self.my_pe,self.get_cnt.load(Ordering::SeqCst),self.put_cnt.load(Ordering::SeqCst));
        // println!("[{:?}]-({:?}) put [{:?}] exit",self.my_pe,thread::current().id(),pe);
    }

    pub(crate) fn iput<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    >(
        &self,
        pe: usize,
        src_addr: &[T],
        dst_addr: usize,
    ) {
        // println!("[{:?}]-({:?}) iput entry",self.my_pe,thread::current().id());
        if pe != self.my_pe {
            let _lock = self.comm_mutex.lock();
            rofi_iput(src_addr, dst_addr, pe).expect("error in rofi put");
            self.put_amt.fetch_add(src_addr.len(), Ordering::SeqCst);
            self.put_cnt.fetch_add(1, Ordering::SeqCst);
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
        // println!("[{:?}]-({:?}) iput exit",self.my_pe,thread::current().id());

        // println!("[{:?}]- gc: {:?} pc: {:?} iput exit",self.my_pe,self.get_cnt.load(Ordering::SeqCst),self.put_cnt.load(Ordering::SeqCst));
    }

    pub(crate) fn put_all<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    >(
        &self,
        src_addr: &[T],
        dst_addr: usize,
    ) {
        // println!("[{:?}]-({:?}) put all entry",self.my_pe,thread::current().id());
        for pe in 0..self.my_pe {
            let _lock = self.comm_mutex.lock();
            unsafe { rofi_put(src_addr, dst_addr, pe).expect("error in rofi put") };
        }
        for pe in self.my_pe + 1..self.num_pes {
            let _lock = self.comm_mutex.lock();
            unsafe { rofi_put(src_addr, dst_addr, pe).expect("error in rofi put") };
        }
        unsafe {
            std::ptr::copy_nonoverlapping(src_addr.as_ptr(), dst_addr as *mut T, src_addr.len());
        }
        self.put_amt
            .fetch_add(src_addr.len() * (self.num_pes - 1), Ordering::SeqCst);
        self.put_cnt
            .fetch_add(1, Ordering::SeqCst);
        // println!("[{:?}]-({:?}) put all exit",self.my_pe,thread::current().id());
        println!("[{:?}]- gc: {:?} pc: {:?} put_all exit",self.my_pe,self.get_cnt.load(Ordering::SeqCst),self.put_cnt.load(Ordering::SeqCst));
    }

    pub(crate) fn get<
        T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + 'static,
    >(
        &self,
        pe: usize,
        src_addr: usize,
        dst_addr: &mut [T],
    ) {
        // println!("[{:?}]-({:?}) get entry",self.my_pe,thread::current().id());
        if pe != self.my_pe {
            unsafe {
                let _lock = self.comm_mutex.lock();
                let ret = rofi_get(src_addr, dst_addr, pe); //.expect("error in rofi get")
                if let Err(_) = ret {
                    println!(
                        "Error in get from {:?} src {:x} base_addr {:x} dst_addr {:p} size {:?}",
                        pe,
                        src_addr,
                        *self.rofi_base_address.read(),
                        dst_addr,
                        dst_addr.len()
                    );
                    panic!();
                }
                self.get_amt.fetch_add(dst_addr.len(), Ordering::SeqCst);
                self.get_cnt.fetch_add(1, Ordering::SeqCst);
            };
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
        // println!("[{:?}]- gc: {:?} pc: {:?} get exit",self.my_pe,self.get_cnt.load(Ordering::SeqCst),self.put_cnt.load(Ordering::SeqCst));
    }
    #[allow(dead_code)]
    fn iget<
        T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + 'static,
    >(
        &self,
        pe: usize,
        src_addr: usize,
        dst_addr: &mut [T],
    ) {
        // println!("[{:?}]-({:?}) iget entry",self.my_pe,thread::current().id());
        if pe != self.my_pe {
            unsafe {
                let _lock = self.comm_mutex.lock();
                let ret = rofi_get(src_addr, dst_addr, pe); //.expect("error in rofi get")
                if let Err(_) = ret {
                    println!(
                        "Error in iget from {:?} src {:x} base_addr {:x} dst_addr {:p} size {:x}",
                        pe,
                        src_addr,
                        *self.rofi_base_address.read(),
                        dst_addr,
                        dst_addr.len()
                    );
                    panic!();
                }
                self.get_amt.fetch_add(dst_addr.len(), Ordering::SeqCst);
                self.get_cnt.fetch_add(1, Ordering::SeqCst);
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
        // println!("[{:?}]- gc: {:?} pc: {:?} iget exit",self.my_pe,self.get_cnt.load(Ordering::SeqCst),self.put_cnt.load(Ordering::SeqCst));
        // println!("[{:?}]-({:?}) iget exit",self.my_pe,thread::current().id());
    }
    //src address is relative to rofi base addr
    pub(crate) fn iget_relative<
        T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + 'static,
    >(
        &self,
        pe: usize,
        src_addr: usize,
        dst_addr: &mut [T],
    ) {
        if pe != self.my_pe {
            // unsafe {
            let _lock = self.comm_mutex.lock();
            // println!("[{:?}]-({:?}) iget_relative [{:?}] entry",self.my_pe,thread::current().id(),pe);

            let ret = rofi_iget(*self.rofi_base_address.read() + src_addr, dst_addr, pe); //.expect("error in rofi get")
            if let Err(_) = ret {
                println!(
                    "[{:?}] Error in iget_relative from {:?} src_addr {:?} ({:x}) dst_addr {:?} base_addr {:x} size {:?}",
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
            self.get_cnt.fetch_add(1, Ordering::SeqCst);
            // self.get_amt.fetch_add(dst_addr.len(),Ordering::SeqCst);
            // }
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
        // println!("[{:?}]- gc: {:?} pc: {:?} iget_relative exit",self.my_pe,self.get_cnt.load(Ordering::SeqCst),self.put_cnt.load(Ordering::SeqCst));
        // println!("[{:?}]-({:?}) iget relative [{:?}] exit",self.my_pe,thread::current().id(),pe);
    }
}

//#[prof]
impl Drop for RofiComm {
    fn drop(&mut self) {
        rofi_barrier();
        std::thread::sleep(std::time::Duration::from_millis(1000));
        let _res = rofi_finit();
        std::thread::sleep(std::time::Duration::from_millis(1000));
        trace!("[{:?}] dropping rofi comm", self.my_pe);
    }
}

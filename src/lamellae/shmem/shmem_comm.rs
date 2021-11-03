use crate::lamellae::comm::*;
use crate::lamellae::command_queues::CommandQueue;
use crate::lamellae::{
    AllocationType, Des, SerializeHeader, SerializedData, SerializedDataOps, SubData,
};
use crate::lamellar_alloc::{BTreeAlloc, LamellarAlloc};

use parking_lot::RwLock;
use shared_memory::*;
use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::sync::Arc;

struct MyShmem {
    data: *mut u8,
    len: usize,
    _shmem: Shmem,
}
unsafe impl Sync for MyShmem {}
unsafe impl Send for MyShmem {}

impl MyShmem {
    fn as_ptr(&self) -> *mut u8 {
        self.data
    }
    fn base_addr(&self) -> usize {
        self.as_ptr() as usize
    }
    fn len(&self) -> usize {
        self.len
    }
    fn contains(&self, addr: usize) -> bool {
        self.base_addr() <= addr && addr < self.base_addr() + self.len
    }
}

fn attach_to_shmem(size: usize, id: &str, header: usize, create: bool) -> MyShmem {
    let size = size + std::mem::size_of::<usize>();
    let shmem_id = "lamellar_".to_owned() + &(size.to_string()) + "_" + id;
    // let  m = if create {
    let mut retry = 0;
    let m = loop {
        match ShmemConf::new().size(size).os_id(shmem_id.clone()).create() {
            Ok(m) => {
                // println!("created {:?}", shmem_id);
                if create {
                    // let zeros = vec![0u8; size];
                    unsafe {
                        //     std::ptr::copy_nonoverlapping(
                        //         zeros.as_ptr() as *const u8,
                        //         m.as_ptr() as *mut u8,
                        //         size,
                        //     );
                        *(m.as_ptr() as *mut _ as *mut usize) = header;
                    }
                }
                break Ok(m);
            }
            Err(ShmemError::LinkExists)
            | Err(ShmemError::MappingIdExists)
            | Err(ShmemError::MapOpenFailed(_)) => {
                match ShmemConf::new().os_id(shmem_id.clone()).open() {
                    Ok(m) => {
                        // println!("attached {:?}", shmem_id);
                        if create {
                            // let zeros = vec![0u8; size];
                            unsafe {
                                // std::ptr::copy_nonoverlapping(
                                //     zeros.as_ptr() as *const u8,
                                //     m.as_ptr() as *mut u8,
                                //     size,
                                // );
                                *(m.as_ptr() as *mut _ as *mut usize) = header;
                            }
                            // unsafe {
                            //     println!(
                            //         "updated {:?} {:?}",
                            //         shmem_id,
                            //         *(m.as_ptr() as *const _ as *const usize)
                            //     );
                            // }
                        }
                        break Ok(m);
                    }
                    Err(ShmemError::MapOpenFailed(_)) if retry < 5 => {
                        retry += 1;
                        std::thread::sleep(std::time::Duration::from_millis(50));
                    }
                    Err(e) => break Err(e),
                }
            }
            Err(e) => break Err(e),
        }
    };
    let m = match m {
        Ok(m) => m,
        Err(e) => panic!("unable to create shared memory {:?} {:?}", shmem_id, e),
    };

    while (unsafe { *(m.as_ptr() as *const _ as *const usize) } != header) {
        std::thread::yield_now()
    }
    // unsafe {
    //     println!(
    //         "shmem inited {:?} {:?}",
    //         shmem_id,
    //         *(m.as_ptr() as *const _ as *const usize)
    //     );
    // }

    unsafe {
        MyShmem {
            data: m.as_ptr().add(std::mem::size_of::<usize>()),
            len: size,
            _shmem: m,
        }
    }
}

struct ShmemAlloc {
    _shmem: MyShmem,
    mutex: *mut AtomicUsize,
    id: *mut usize,
    barrier1: *mut usize,
    barrier2: *mut usize,
    // barrier3: *mut usize,
    my_pe: usize,
    num_pes: usize,
}

unsafe impl Sync for ShmemAlloc {}
unsafe impl Send for ShmemAlloc {}

impl ShmemAlloc {
    fn new(num_pes: usize, pe: usize, job_id: usize) -> Self {
        let size = std::mem::size_of::<AtomicUsize>()
            + std::mem::size_of::<usize>()
            + std::mem::size_of::<usize>() * num_pes * 2;
        let shmem = attach_to_shmem(size, "alloc", job_id, pe == 0);
        let data = unsafe { std::slice::from_raw_parts_mut(shmem.as_ptr(), size) };
        if pe == 0 {
            for i in data {
                *i = 0;
            }
        }
        let base_ptr = shmem.as_ptr();
        ShmemAlloc {
            _shmem: shmem,
            mutex: base_ptr as *mut AtomicUsize,
            id: unsafe { base_ptr.add(std::mem::size_of::<AtomicUsize>()) as *mut usize },
            barrier1: unsafe {
                base_ptr.add(std::mem::size_of::<AtomicUsize>() + std::mem::size_of::<usize>())
                    as *mut usize
            },
            barrier2: unsafe {
                base_ptr.add(
                    std::mem::size_of::<AtomicUsize>()
                        + std::mem::size_of::<usize>()
                        + std::mem::size_of::<usize>() * num_pes,
                ) as *mut usize
            },
            // barrier3: unsafe { base_ptr.add(std::mem::size_of::<AtomicUsize>() + std::mem::size_of::<usize>()) as *mut usize + std::mem::size_of::<usize>()*num_pes*2},
            my_pe: pe,
            num_pes: num_pes,
        }
    }
    unsafe fn alloc<I>(&self, size: usize, pes: I) -> (MyShmem, usize, Vec<usize>)
    where
        I: Iterator<Item = usize> + Clone,
    {
        let barrier1 = std::slice::from_raw_parts_mut(self.barrier1, self.num_pes);
        let barrier2 = std::slice::from_raw_parts_mut(self.barrier2, self.num_pes);
        // println!("trying to alloc! {:?} {:?} {:?}",self.my_pe, barrier1,barrier2);
        // let barrier3 = std::slice::from_raw_parts_mut(self.barrier1, self.num_pes) ;
        for pe in pes.clone() {
            while barrier2[pe] != 0 {
                std::thread::yield_now();
            }
        }
        let mut pes_clone = pes.clone();
        let first_pe = pes_clone.next().unwrap();
        let mut relative_pe = 0;
        let mut pes_len = 1;

        if self.my_pe == first_pe {
            while let Err(_) = self.mutex.as_ref().unwrap().compare_exchange(
                0,
                1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                std::thread::yield_now();
            }
            *self.id += 1;
            barrier1[self.my_pe] = *self.id;
            for pe in pes_clone {
                // println!("{:?} {:?} {:?}",pe ,barrier1[pe], *self.id );
                pes_len += 1;
                while barrier1[pe] != *self.id {
                    std::thread::yield_now();
                }
            }
        } else {
            while barrier1[first_pe] == 0 {
                std::thread::yield_now();
            }
            barrier1[self.my_pe] = *self.id;

            for pe in pes_clone {
                // println!("{:?} {:?} {:?}",pe ,barrier1[pe], *self.id );
                pes_len += 1;
                while barrier1[pe] != *self.id {
                    std::thread::yield_now();
                }
            }
        }

        // println!("going to attach to shmem {:?} {:?} {:?} {:?} {:?}",size*pes_len,*self.id,self.my_pe, barrier1,barrier2);
        let shmem = attach_to_shmem(
            size * pes_len,
            &((*self.id).to_string()),
            *self.id,
            self.my_pe == first_pe,
        );
        barrier2[self.my_pe] = shmem.as_ptr() as usize;
        let cnt = shmem.as_ptr() as *mut AtomicIsize;
        if self.my_pe == first_pe {
            cnt.as_ref()
                .unwrap()
                .fetch_add(pes_len as isize, Ordering::SeqCst);
        }
        cnt.as_ref().unwrap().fetch_sub(1, Ordering::SeqCst);
        while cnt.as_ref().unwrap().load(Ordering::SeqCst) != 0 {}
        let addrs = barrier2.to_vec();
        // println!("attached {:?} {:?}",self.my_pe,shmem.as_ptr());
        barrier1[self.my_pe] = 0;
        for pe in pes.into_iter() {
            // println!("{:?} pe {:?} {:?} {:?}",self.my_pe, pe, barrier1,barrier2);
            while barrier1[pe] != 0 {
                std::thread::yield_now();
            }
            if pe < self.my_pe {
                relative_pe += 1;
            }
        }
        barrier2[self.my_pe] = 0;
        if self.my_pe == first_pe {
            self.mutex.as_ref().unwrap().store(0, Ordering::SeqCst);
        }
        // println!("{:?} {:?} {:?}",self.my_pe, barrier1,barrier2);
        (shmem, relative_pe, addrs)
    }
}

pub(crate) struct ShmemComm {
    // _shmem: MyShmem, //the global handle
    pub(crate) base_address: Arc<RwLock<usize>>, //start address of my segment
    _size: usize,                                //size of my segment
    alloc: RwLock<Vec<BTreeAlloc>>,
    _init: AtomicBool,
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    alloc_lock: Arc<
        RwLock<(
            HashMap<
                usize, //local addr
                (
                    MyShmem,
                    usize,
                    HashMap<
                        //share mem segment, per pe size,
                        usize, //global pe id
                        (usize, usize),
                    >,
                ), // remote addr, relative index (for subteams)
            >,
            ShmemAlloc,
        )>,
    >,
}

static SHMEM_SIZE: AtomicUsize = AtomicUsize::new(4 * 1024 * 1024 * 1024);
const RT_MEM: usize = 100 * 1024 * 1024;
impl ShmemComm {
    pub(crate) fn new() -> ShmemComm {
        let num_pes = match env::var("LAMELLAR_NUM_PES") {
            Ok(val) => val.parse::<usize>().unwrap(),
            Err(_e) => 1,
        };
        let my_pe = match env::var("LAMELLAR_PE_ID") {
            Ok(val) => val.parse::<usize>().unwrap(),
            Err(_e) => 0,
        };
        let job_id = match env::var("LAMELLAR_JOB_ID") {
            Ok(val) => val.parse::<usize>().unwrap(),
            Err(_e) => 0,
        };
        if let Ok(size) = std::env::var("LAMELLAR_MEM_SIZE") {
            let size = size
                .parse::<usize>()
                .expect("invalid memory size, please supply size in bytes");
            SHMEM_SIZE.store(size, Ordering::SeqCst);
        }

        // let mem_per_pe = SHMEM_SIZE.load(Ordering::SeqCst)/num_pes;
        let cmd_q_mem = CommandQueue::mem_per_pe() * num_pes;
        let total_mem = cmd_q_mem + RT_MEM + SHMEM_SIZE.load(Ordering::SeqCst);
        let mem_per_pe = total_mem; // / num_pes;

        let alloc = ShmemAlloc::new(num_pes, my_pe, job_id);
        // let shmem = attach_to_shmem(SHMEM_SIZE.load(Ordering::SeqCst),"main",job_id,my_pe==0);

        // let (shmem,index) =unsafe {alloc.alloc(mem_per_pe,0..num_pes)};
        let (shmem, _index, addrs) = unsafe { alloc.alloc(mem_per_pe, 0..num_pes) };
        let addr = shmem.as_ptr() as usize + mem_per_pe * my_pe;

        let mut allocs_map = HashMap::new();
        let mut pe_map = HashMap::new();
        for pe in 0..num_pes {
            if addrs[pe] > 0 {
                pe_map.insert(pe, (addrs[pe], pe));
            }
        }
        allocs_map.insert(addr, (shmem, mem_per_pe, pe_map));
        // alloc.0.insert(addr,(ret,size))
        let shmem = ShmemComm {
            // _shmem: shmem,
            base_address: Arc::new(RwLock::new(addr)),
            _size: mem_per_pe,
            alloc: RwLock::new(vec![BTreeAlloc::new("shmem".to_string())]),
            _init: AtomicBool::new(true),
            num_pes: num_pes,
            my_pe: my_pe,
            alloc_lock: Arc::new(RwLock::new((allocs_map, alloc))),
        };
        shmem.alloc.write()[0].init(addr, mem_per_pe);
        shmem
    }
}

impl CommOps for ShmemComm {
    fn my_pe(&self) -> usize {
        self.my_pe
    }
    fn num_pes(&self) -> usize {
        self.num_pes
    }
    fn barrier(&self) {
        let alloc = self.alloc_lock.write();
        unsafe {
            alloc.1.alloc(1, 0..self.num_pes);
        }
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
        let size = std::cmp::max(
            min_size * 2 * self.num_pes,
            SHMEM_SIZE.load(Ordering::SeqCst),
        ) / self.num_pes;
        if let Ok(addr) = self.alloc(size, AllocationType::Global) {
            // println!("addr: {:x} - {:x}",addr, addr+size);
            let mut new_alloc = BTreeAlloc::new("shmem".to_string());
            new_alloc.init(addr, size);
            allocs.push(new_alloc)
        } else {
            panic!("[Error] out of system memory");
        }
    }

    fn rt_alloc(&self, size: usize) -> AllocResult<usize> {
        let allocs = self.alloc.read();
        for alloc in allocs.iter() {
            if let Some(addr) = alloc.try_malloc(size) {
                return Ok(addr);
            }
            // println!("size: {:?} remaining {:?} occupied {:?} len {:?}",size, alloc.space_avail(),alloc.occupied(),allocs.len());
        }
        Err(AllocError::OutOfMemoryError(size))
        // if let Some(addr) = self.alloc.try_malloc(size) {
        //     Some(addr)
        // } else {
        //     println!("[WARNING] out of memory: (work in progress on a scalable solution, as a work around try setting the LAMELLAR_MEM_SIZE envrionment variable (current size = {:?} -- Note: LamellarLocalArrays are currently allocated out of this pool",SHMEM_SIZE.load(Ordering::SeqCst));
        //     None
        // }
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

    fn alloc(&self, size: usize, alloc_type: AllocationType) -> AllocResult<usize> {
        let mut alloc = self.alloc_lock.write();
        let (ret, index, remote_addrs) = match alloc_type {
            AllocationType::Sub(pes) => {
                // println!("pes: {:?}",pes);
                if pes.contains(&self.my_pe) {
                    let ret = unsafe { alloc.1.alloc(size, pes.iter().cloned()) };
                    // println!("{:?}",ret.2);
                    ret
                } else {
                    return Err(AllocError::IdError(self.my_pe));
                }
            }
            AllocationType::Global => unsafe { alloc.1.alloc(size, 0..self.num_pes) },
            _ => panic!("unexpected allocation type {:?} in rofi_alloc", alloc_type),
        };
        let mut addr_map = HashMap::new();
        let mut relative_index = 0;
        for pe in 0..self.num_pes {
            if remote_addrs[pe] > 0 {
                // let local_addr = ret.as_ptr() as usize + size*relative_index;
                addr_map.insert(pe, (remote_addrs[pe], relative_index));
                relative_index += 1;
            }
        }
        let addr = ret.as_ptr() as usize + size * index;
        alloc.0.insert(addr, (ret, size, addr_map));
        Ok(addr)
    }

    fn free(&self, addr: usize) {
        //maybe need to do something more intelligent on the drop of the shmem_alloc
        let mut alloc = self.alloc_lock.write();
        alloc.0.remove(&addr);
    }

    fn base_addr(&self) -> usize {
        *self.base_address.read()
    }
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize {
        let alloc = self.alloc_lock.read();
        for (addr, (shmem, size, addrs)) in alloc.0.iter() {
            if let Some(data) = addrs.get(&remote_pe) {
                if data.0 <= remote_addr && remote_addr < data.0 + shmem.len() {
                    let remote_offset = remote_addr - (data.0 + size * data.1);
                    // println!("{:?} {:?} {:?}",addr,remote_offset,addr + remote_offset);
                    return addr + remote_offset;
                }
            }
            // println!("1-- addr {:?} remote_addr {:?} base_addr( {:?}, {:?} ) top_addr( {:?}, {:?} ) {:?} {:?}",addr,remote_addr,shmem.base_addr(),addrs[remote_pe],shmem.base_addr()+shmem.len(),addrs[remote_pe] + shmem.len(),addrs[remote_pe] <= remote_addr , remote_addr < addrs[remote_pe] + shmem.len());
        }
        // println!();
        // for (addr,(shmem,_,addrs)) in alloc.0.iter(){
        //     println!("2-- addr {:?} remote_addr {:?} base_addr( {:?}, {:?} ) top_addr( {:?}, {:?} ) {:?} {:?}",addr,remote_addr,shmem.base_addr(),addrs[remote_pe],shmem.base_addr()+shmem.len(),addrs[&remote_pe].0 + shmem.len(),addrs[&remote_pe] <= remote_addr , remote_addr < addrs[remote_pe] + shmem.len());
        // }
        panic!("not sure i should be here...means address not found");
    }
    fn remote_addr(&self, pe: usize, local_addr: usize) -> usize {
        let alloc = self.alloc_lock.read();
        for (addr, (shmem, size, addrs)) in alloc.0.iter() {
            if shmem.contains(local_addr) {
                let local_offset = local_addr - addr;
                return addrs[&pe].0 + size * addrs[&pe].1 + local_offset;
            }
        }
        panic!("not sure i should be here...means address not found");
    }
    fn put<T: Remote + 'static>(&self, pe: usize, src_addr: &[T], dst_addr: usize) {
        let alloc = self.alloc_lock.read();
        for (addr, (shmem, size, addrs)) in alloc.0.iter() {
            if shmem.contains(dst_addr) {
                let real_dst_base = shmem.base_addr() + size * addrs[&pe].1;
                let real_dst_addr = real_dst_base + (dst_addr - addr);
                // if self.alloc.read().len() > 1 {
                //     println!("put base: {:x} {:x} addr: {:x} src {:?} len {:?} pe {:?}",shmem.base_addr(),real_dst_base,real_dst_addr,src_addr.as_ptr(),alloc.0.len(),pe );
                // }
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        src_addr.as_ptr(),
                        real_dst_addr as *mut T,
                        src_addr.len(),
                    );
                }
                break;
            }
        }
    }
    fn iput<T: Remote + 'static>(&self, pe: usize, src_addr: &[T], dst_addr: usize) {
        self.put(pe, src_addr, dst_addr);
    }
    fn put_all<T: Remote + 'static>(&self, src_addr: &[T], dst_addr: usize) {
        for pe in 0..self.num_pes {
            self.put(pe, src_addr, dst_addr);
        }
    }
    fn get<T: Remote + 'static>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]) {
        let alloc = self.alloc_lock.read();
        for (addr, (shmem, size, addrs)) in alloc.0.iter() {
            if shmem.contains(src_addr) {
                let real_src_base = shmem.base_addr() + size * addrs[&pe].1;
                let real_src_addr = real_src_base + (src_addr - addr);
                // if self.alloc.read().len() > 1 {
                //     println!("get base: {:x} {:x} addr: {:x} dst: {:?} pe {:?}",shmem.base_addr(),real_src_base,real_src_addr, dst_addr.as_mut_ptr(),pe);
                // }
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        real_src_addr as *const T,
                        dst_addr.as_mut_ptr(),
                        dst_addr.len(),
                    );
                }
                break;
            }
        }
    }
    fn iget<T: Remote + 'static>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]) {
        // println!("iget s_addr {:?} d_addr {:?} b_addr {:?}",src_addr,dst_addr.as_ptr(),self.base_addr());
        self.get(pe, src_addr, dst_addr);
    }
    fn iget_relative<T: Remote + 'static>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]) {
        self.get(pe, src_addr + self.base_addr(), dst_addr);
    }
}

impl Drop for ShmemComm {
    fn drop(&mut self) {
        // let allocs = self.alloc.read();
        // for alloc in allocs.iter(){
        //     println!("dropping shmem -- memory in use {:?}", alloc.occupied());
        // }
        if self.occupied() > 0 {
            println!("dropping rofi -- memory in use {:?}", self.occupied());
        }
        if self.alloc.read().len() > 1 {
            println!("[LAMELLAR INFO] {:?} additional rt memory pools were allocated, performance may be increased using a larger initial pool, set using the LAMELLAR_MEM_SIZE envrionment variable. Current initial size = {:?}",self.alloc.read().len()-1, SHMEM_SIZE.load(Ordering::SeqCst));
        }
    }
}

pub(crate) struct ShmemData {
    pub(crate) addr: usize,          // process space address
    pub(crate) relative_addr: usize, //address allocated from shmem
    pub(crate) len: usize,
    pub(crate) data_start: usize,
    pub(crate) data_len: usize,
    pub(crate) shmem_comm: Arc<Comm>, //Comm instead of RofiComm because I can't figure out how to make work with Enum_distpatch....
    pub(crate) alloc_size: usize,
}

impl ShmemData {
    pub fn new(shmem_comm: Arc<Comm>, size: usize) -> Result<ShmemData, anyhow::Error> {
        let ref_cnt_size = std::mem::size_of::<AtomicUsize>();
        let alloc_size = size + ref_cnt_size; //+  std::mem::size_of::<u64>();
        let relative_addr = shmem_comm.rt_alloc(alloc_size)?;
        // println!("addr: {:?} rel_addr {:?} base{:?}",relative_addr + shmem_comm.base_addr(),relative_addr ,shmem_comm.base_addr());
        let addr = relative_addr; //+ shmem_comm.base_addr();
        unsafe {
            let ref_cnt = addr as *const AtomicUsize;
            (*ref_cnt).store(1, Ordering::SeqCst)
        };
        Ok(ShmemData {
            addr: addr,
            relative_addr: relative_addr + ref_cnt_size,
            data_start: addr
                + std::mem::size_of::<AtomicUsize>()
                + std::mem::size_of::<Option<SerializeHeader>>(),
            len: size, // + std::mem::size_of::<u64>(),
            data_len: size - std::mem::size_of::<Option<SerializeHeader>>(),
            shmem_comm: shmem_comm,
            alloc_size: alloc_size,
        })
    }
}
impl SerializedDataOps for ShmemData {
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

impl Des for ShmemData {
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

impl SubData for ShmemData {
    fn sub_data(&self, start: usize, end: usize) -> SerializedData {
        let mut sub = self.clone();
        sub.data_start += start;
        sub.data_len = end - start;
        SerializedData::ShmemData(sub)
    }
}

impl Clone for ShmemData {
    fn clone(&self) -> Self {
        unsafe {
            let ref_cnt = self.addr as *const AtomicUsize;
            (*ref_cnt).fetch_add(1, Ordering::SeqCst)
        };
        ShmemData {
            addr: self.addr,
            relative_addr: self.relative_addr,
            len: self.len,
            data_start: self.data_start,
            data_len: self.data_len,
            shmem_comm: self.shmem_comm.clone(),
            alloc_size: self.alloc_size,
        }
    }
}

impl Drop for ShmemData {
    fn drop(&mut self) {
        let cnt = unsafe { (*(self.addr as *const AtomicUsize)).fetch_sub(1, Ordering::SeqCst) };
        if cnt == 1 {
            self.shmem_comm.rt_free(self.addr);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shmem_alloc_test() {
        let num_pes = 3;
        for pe in 0..num_pes {
            std::thread::spawn(move || {
                let shmem_comm = ShmemComm::new(num_pes, pe);
                let addr = shmem_comm.alloc(1000, AllocationType::Global);
                // let addr = 0;
                let sub_addr = shmem_comm.alloc(500, AllocationType::Sub(vec![0, 2]));
                println!("{:?} addr {:?} sub_addr {:?}", pe, addr, sub_addr);
                std::thread::sleep(std::time::Duration::from_secs(30));
            });
        }
        std::thread::sleep(std::time::Duration::from_secs(45));
    }
}

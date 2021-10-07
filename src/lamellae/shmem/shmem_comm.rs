use crate::lamellae::{AllocationType, Des, SerializeHeader, SerializedData, SubData,SerializedDataOps};
use crate::lamellae::comm::*;
use crate::lamellar_alloc::{BTreeAlloc,LamellarAlloc};

use shared_memory::*;
use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::env;
use std::collections::HashMap;
use std::borrow::Borrow;


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
    let shmem_id = "lamellar_".to_owned() + &(size.to_string())+"_"+id;
    // let m = if create {
    let m = match ShmemConf::new().size(size).os_id(shmem_id.clone()).create() {
        Ok(m) => {
            println!("created {:?}", shmem_id);

            if create {
                let zeros = vec![0u8; size];
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        zeros.as_ptr() as *const u8,
                        m.as_ptr() as *mut u8,
                        size,
                    );
                    *(m.as_ptr() as *mut _ as *mut usize) = header;
                }
            }
            m
        }
        Err(ShmemError::LinkExists) | Err(ShmemError::MappingIdExists) => {
            match ShmemConf::new().os_id(shmem_id.clone()).open() {
                Ok(m) => {
                    println!("attached {:?}", shmem_id);
                    if create {
                        let zeros = vec![0u8; size];
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                zeros.as_ptr() as *const u8,
                                m.as_ptr() as *mut u8,
                                size,
                            );
                            *(m.as_ptr() as *mut _ as *mut usize) = header;
                        }
                        unsafe {
                            println!(
                                "updated {:?} {:?}",
                                shmem_id,
                                *(m.as_ptr() as *const _ as *const usize)
                            );
                        }
                    }
                    m
                }
                Err(r) => panic!("unable to attach to shared memory {:?} {:?}", shmem_id, r),
            }
        }
        Err(e) => panic!("unable to create shared memory {:?} {:?}", shmem_id, e),
    };
    while (unsafe { *(m.as_ptr() as *const _ as *const usize) } != header) {
        std::thread::yield_now()
    }
    unsafe {
        println!(
            "shmem inited {:?} {:?}",
            shmem_id,
            *(m.as_ptr() as *const _ as *const usize)
        );
    }

    unsafe {
        MyShmem {
            data: m.as_ptr().add(std::mem::size_of::<usize>()),
            len: size,
            _shmem: m,
        }
    }
}



struct ShmemAlloc{
    _shmem: MyShmem,
    mutex: *mut AtomicUsize,
    id: *mut usize,
    barrier: *mut usize,
    my_pe: usize,
    num_pes: usize,
}

unsafe impl Sync for ShmemAlloc {}
unsafe impl Send for ShmemAlloc {}

impl ShmemAlloc{
    fn new(num_pes: usize, pe: usize, job_id: usize) -> Self{
        let size = std::mem::size_of::<AtomicUsize>() + std::mem::size_of::<usize>()+ std::mem::size_of::<usize>()*num_pes;
        let shmem = attach_to_shmem(size,"alloc",job_id, pe == 0);
        let base_ptr = shmem.as_ptr();
        ShmemAlloc{
            _shmem: shmem,
            mutex: base_ptr as *mut AtomicUsize,
            id: unsafe { base_ptr.add(std::mem::size_of::<AtomicUsize>())as *mut usize },
            barrier: unsafe { base_ptr.add(std::mem::size_of::<AtomicUsize>() + std::mem::size_of::<usize>()) as *mut usize },
            my_pe: pe,
            num_pes: num_pes,
        }
    }
    unsafe fn alloc<I>(&self, size: usize, pes: I) -> (MyShmem,usize) 
    where 
    I: Iterator<Item=usize> + Clone{
        let barrier = std::slice::from_raw_parts_mut(self.barrier, self.num_pes) ;
        println!("b1 {:?}",barrier);
        let mut pes_clone = pes.clone();
        let first_pe = pes_clone.next().unwrap();
        let mut relative_pe = 0;
        let mut pes_len=1;
        if self.my_pe == first_pe{
            while let Err(_) = self.mutex.as_ref().unwrap().compare_exchange(0,1,Ordering::SeqCst,Ordering::SeqCst){
                std::thread::yield_now();
            }
            *self.id += 1;
            barrier[self.my_pe] = *self.id;
            println!("b2 {:?} {:?}",barrier,*self.id);
            for pe in pes_clone{
                pes_len+=1;
                while barrier[pe] != *self.id {
                    std::thread::yield_now();
                }
            }
        }
        else{
            println!("b2 {:?}",barrier);
            while barrier[first_pe] == 0{
                std::thread::yield_now();
            }
            barrier[self.my_pe] = *self.id;
            println!("b3 {:?}",barrier);
            for pe in pes_clone{
                pes_len+=1;
                println!("{:?} {:?}",first_pe, pe);
                while barrier[pe] != *self.id {
                    std::thread::yield_now();
                }
            }
        }
        println!("going to attach to shmem {:?} {:?} {:?}",size*pes_len,*self.id,self.my_pe);
        let shmem = attach_to_shmem(size*pes_len,&((*self.id).to_string()),*self.id,self.my_pe==first_pe);
        barrier[self.my_pe] = 0;
        for pe in pes.into_iter(){
            while barrier[pe] != 0 {
                std::thread::yield_now();
            }
            if pe < self.my_pe{
                relative_pe +=1;
            }
        }
        println!("b4 {:?}",barrier);
        if self.my_pe == first_pe{
            self.mutex.as_ref().unwrap().store(0,Ordering::SeqCst);
        }
        (shmem,relative_pe)
    }
}

pub(crate) struct ShmemComm{
    _shmem: MyShmem, //the global handle
    pub(crate) base_address: Arc<RwLock<usize>>, //start address of my segment
    size: usize, //size of my segment
    alloc: BTreeAlloc,
    _init: AtomicBool,
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    comm_mutex: Arc<Mutex<()>>,
    alloc_lock: Arc<RwLock<(HashMap<usize,(MyShmem,usize)>,ShmemAlloc)>>,
}

static SHMEM_SIZE: AtomicUsize = AtomicUsize::new(1 * 1024 * 1024 * 1024);
impl ShmemComm {
    pub(crate) fn new() -> ShmemComm {
        let num_pes = match env::var("LAMELLAR_NUM_PES") {
            Ok(val) => val.parse::<usize>().unwrap(),
            Err(_e) => 1
        };
        let my_pe = match env::var("LAMELLAR_PE_ID") {
            Ok(val) => val.parse::<usize>().unwrap(),
            Err(_e) => 0
        };
        let job_id = match env::var("LAMELLAR_JOB_ID") {
            Ok(val) => val.parse::<usize>().unwrap(),
            Err(_e) => 0
        };
        if let Ok(size) = std::env::var("LAMELLAR_SHMEM_SIZE") {
            let size = size
                .parse::<usize>()
                .expect("invalid memory size, please supply size in bytes");
            SHMEM_SIZE.store(size, Ordering::SeqCst);
        }
        
        let mem_per_pe = SHMEM_SIZE.load(Ordering::SeqCst)/num_pes;

        
        let mut shmem = ShmemComm{
            _shmem: attach_to_shmem(SHMEM_SIZE.load(Ordering::SeqCst),"main",job_id,my_pe==0),
            base_address: Arc::new(RwLock::new(my_pe * mem_per_pe)),
            size: mem_per_pe,
            alloc: BTreeAlloc::new("shmem".to_string()),
            _init: AtomicBool::new(true),
            num_pes: num_pes,
            my_pe: my_pe,
            comm_mutex: Arc::new(Mutex::new(())),
            alloc_lock: Arc::new(RwLock::new((HashMap::new(), ShmemAlloc::new(num_pes,my_pe,job_id)))),

        };
        shmem.alloc.init(0,mem_per_pe);
        shmem
    }
}

impl CommOps for ShmemComm{
    fn my_pe(&self) -> usize{
        self.my_pe
    }
    fn num_pes(&self) -> usize{
        self.num_pes
    }
    fn barrier(&self) {

    }
    fn occupied(&self) -> usize {
        self.alloc.occupied()
    }

   fn rt_alloc(&self, size: usize) -> Option<usize> {
        if let Some(addr) = self.alloc.try_malloc(size) {
            Some(addr)
        } else {
            println!("[WARNING] out of memory: (work in progress on a scalable solution, as a work around try setting the LAMELLAR_ROFI_MEM_SIZE envrionment variable (current size = {:?} -- Note: LamellarLocalArrays are currently allocated out of this pool",SHMEM_SIZE.load(Ordering::SeqCst));
            None
        }
    }
    fn rt_free(&self,addr: usize) {
        self.alloc.free(addr);
    }

    fn alloc(&self, size: usize, alloc_type: AllocationType) -> Option<usize> {
        let mut alloc = self.alloc_lock.write();
        let (ret,index) = match alloc_type {
            AllocationType::Sub(pes) => {
                println!("pes: {:?}",pes);
                if pes.contains(&self.my_pe){
                    unsafe {  alloc.1.alloc(size,pes.iter().cloned()) }
                }
                else{
                    return None;
                }
            },
            AllocationType::Global => unsafe { alloc.1.alloc(size,0..self.num_pes)},
            _ => panic!("unexpected allocation type {:?} in rofi_alloc", alloc_type),
        };
        let addr = ret.as_ptr() as usize + size * index;
        alloc.0.insert(addr,(ret,size));
        Some(addr)
    }
    
    fn free(&self, addr: usize){ //maybe need to do something more intelligent on the drop of the shmem_alloc
        let mut alloc = self.alloc_lock.write();
        alloc.0.remove(&addr); 
    }

    fn base_addr(&self) -> usize{
        *self.base_address.read()
    }
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize{
        let alloc = self.alloc_lock.read();
        for (addr,(shmem,_)) in alloc.0.iter(){
            if shmem.contains(remote_addr){
                let remote_offset = remote_addr - shmem.base_addr();
                return addr + remote_offset;
            }
            break;
        }
        panic!("not sure i should be here...means address not found");
    }
    fn remote_addr(&self, pe: usize, local_addr: usize) -> usize{
        let alloc = self.alloc_lock.read();
        for (addr,(shmem,size)) in alloc.0.iter(){
            if shmem.contains(local_addr){
                let local_offset = local_addr - addr;
                return shmem.base_addr()+size*pe + local_offset;
            }
        }
        panic!("not sure i should be here...means address not found");
    }
    fn put<T: Remote + 'static>(&self, pe: usize, src_addr: &[T], dst_addr: usize){
        let alloc = self.alloc_lock.read();
        for (addr,(shmem,size)) in alloc.0.iter(){
            if shmem.contains(dst_addr){
                let real_dst_base = shmem.base_addr()+size*pe;
                let real_dst_addr = real_dst_base+ (dst_addr-addr);
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
    fn iput<T: Remote + 'static>(&self,pe: usize,src_addr: &[T],dst_addr: usize){
        self.put(pe,src_addr,dst_addr);
    }
    fn put_all<T: Remote + 'static>(&self, src_addr: &[T], dst_addr: usize){
        for pe in 0..self.num_pes{
            self.put(pe,src_addr,dst_addr);
        }
    } 
    fn get<T: Remote + 'static>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]){
        let alloc = self.alloc_lock.read();
        for (addr,(shmem,size)) in alloc.0.iter(){
            if shmem.contains(src_addr){
                let real_src_base = shmem.base_addr() + size*pe;
                let real_src_addr = real_src_base + (src_addr - addr);
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
    fn iget<T: Remote + 'static>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]){
        self.get(pe,src_addr,dst_addr);
    }
    fn iget_relative<T: Remote + 'static>(&self,pe: usize,src_addr: usize,dst_addr: &mut [T]){
        self.get(pe, src_addr+self.base_addr(),dst_addr);
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
    pub async fn new(shmem_comm: Arc<Comm>, size: usize) -> ShmemData {
        let ref_cnt_size = std::mem::size_of::<AtomicUsize>();
        let alloc_size = size + ref_cnt_size; //+  std::mem::size_of::<u64>();
        let mut mem = shmem_comm.rt_alloc(alloc_size);
        let mut timer = std::time::Instant::now();
        while mem.is_none() {
            async_std::task::yield_now().await;
            mem = shmem_comm.rt_alloc(alloc_size);
            if timer.elapsed().as_secs_f64() > 15.0 {
                println!("stuck waiting for shmemdata alloc");
                timer = std::time::Instant::now();
            }
        }
        let relative_addr = mem.unwrap();
        let addr = relative_addr + shmem_comm.base_addr();
        unsafe {
            let ref_cnt = addr as *const AtomicUsize;
            (*ref_cnt).store(1, Ordering::SeqCst)
        };
        ShmemData {
            addr: addr,
            relative_addr: relative_addr + ref_cnt_size,
            data_start: addr
                + std::mem::size_of::<AtomicUsize>()
                + std::mem::size_of::<Option<SerializeHeader>>(),
            len: size, // + std::mem::size_of::<u64>(),
            data_len: size - std::mem::size_of::<Option<SerializeHeader>>(),
            shmem_comm: shmem_comm,
            alloc_size: alloc_size,
        }
    }
}
impl SerializedDataOps for ShmemData{
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
            self.shmem_comm
                .rt_free(self.addr - self.shmem_comm.base_addr());
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shmem_alloc_test() {
        let num_pes = 3;
        for pe in 0..num_pes{
            std::thread::spawn(move || {
                let shmem_comm = ShmemComm::new(num_pes,pe);
                let addr = shmem_comm.alloc(1000,AllocationType::Global);
                // let addr = 0;
                let sub_addr = shmem_comm.alloc(500,AllocationType::Sub(vec![0,2]));
                println!("{:?} addr {:?} sub_addr {:?}",pe,addr,sub_addr);
                std::thread::sleep(std::time::Duration::from_secs(30));
            });
        }
        std::thread::sleep(std::time::Duration::from_secs(45));

    }
}

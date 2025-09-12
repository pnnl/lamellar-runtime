use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicIsize, AtomicUsize, Ordering},
        Arc,
    },
};

use parking_lot::RwLock;
use shared_memory::*;
use tracing::trace;

use crate::lamellae::{
    AllocError, AllocResult, CommAlloc, CommAllocAddr, CommAllocInner, CommAllocType,
};

pub(crate) struct ShmemHandle {
    base_addr: *mut u8,
    num_bytes: usize,
    _shmem: Shmem,
}

impl ShmemHandle {
    pub(crate) fn base_ptr(&self) -> *mut u8 {
        self.base_addr
    }
    pub(crate) fn num_bytes(&self) -> usize {
        self.num_bytes
    }
}

pub(crate) struct ShmemAlloc {
    pub(crate) data: *mut u8,
    pub(crate) len: usize,
    pub(crate) base_ptr: *mut u8,
    pub(crate) base_len: usize,
    pub(crate) my_alloc_pe: usize, //pe id relative to the pes associated with the alloc
    pe_map: HashMap<usize, usize>,
    remote_addrs: Vec<usize>,
    _shmem: Arc<ShmemHandle>,
}
impl std::fmt::Debug for ShmemAlloc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShmemAlloc")
            .field("data", &self.data)
            .field("len", &self.len)
            .field("my_alloc_pe", &self.my_alloc_pe)
            .field("remote_addrs", &self.remote_addrs)
            .finish()
    }
}

unsafe impl Sync for ShmemAlloc {}
unsafe impl Send for ShmemAlloc {}

impl ShmemAlloc {
    pub(crate) fn num_pes(&self) -> usize {
        self.pe_map.len()
    }
    pub(crate) fn start(&self) -> usize {
        self.data as usize
    }

    pub(crate) fn num_bytes(&self) -> usize {
        self.len
    }
    pub(crate) fn sub_alloc(&self, offset: usize, len: usize) -> AllocResult<Arc<ShmemAlloc>> {
        if offset + len > self.len {
            return Err(AllocError::InvalidSubAlloc(offset, len));
        }
        let new_data = unsafe { self.data.add(offset) };

        let mut remote_addrs = Vec::with_capacity(self.remote_addrs.len());
        for addr in self.remote_addrs.iter() {
            remote_addrs.push(addr + offset);
        }
        Ok(Arc::new(ShmemAlloc {
            data: new_data,
            len,
            base_ptr: self.base_ptr,
            base_len: self.base_len,
            my_alloc_pe: self.my_alloc_pe,
            pe_map: self.pe_map.clone(),
            remote_addrs,
            _shmem: self._shmem.clone(),
        }))
    }

    pub(crate) fn pe_base_offset(&self, pe: usize) -> usize {
        let offset = unsafe { self.data.offset_from_unsigned(self.base_ptr) };
        unsafe {
            self._shmem
                .base_ptr()
                .add(self.pe_map[&pe] * self.base_len + offset) as usize
        }
    }
}

impl From<Arc<ShmemAlloc>> for CommAlloc {
    fn from(alloc: Arc<ShmemAlloc>) -> Self {
        CommAlloc {
            inner_alloc: CommAllocInner::ShmemAlloc(alloc),
            alloc_type: CommAllocType::Fabric,
        }
    }
}

#[tracing::instrument(skip_all, level = "debug")]
fn attach_to_shmem(
    _num_pes: usize,
    job_id: usize,
    size: usize,
    align: usize,
    id: &str,
    header: usize,
    create: bool,
) -> ShmemHandle {
    let padding = std::mem::size_of::<usize>() % align;
    let shmem_size = std::mem::size_of::<usize>() + padding + size;

    let shmem_id =
        "lamellar_".to_owned() + &(job_id.to_string()) + "_" + &(shmem_size.to_string()) + "_" + id;
    // let  m = if create {
    let mut retry = 0;
    let m = loop {
        match ShmemConf::new()
            .size(shmem_size)
            .os_id(shmem_id.clone())
            .create()
        {
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
    // let cnt = unsafe {
    //     (m.as_ptr().add(std::mem::size_of::<usize>()) as *mut AtomicUsize)
    //         .as_ref()
    //         .unwrap()
    // };
    // cnt.fetch_add(1, Ordering::SeqCst);
    // if create {
    //     while cnt.load(Ordering::SeqCst) != num_pes {
    //         std::thread::yield_now()
    //     }
    // }

    unsafe {
        trace!(
            "shmem inited {:?} {:?}",
            shmem_id,
            *(m.as_ptr() as *const _ as *const usize)
        );
    }

    // unsafe {
    //     MyShmem {
    //         // data: m.as_ptr().add(std::mem::size_of::<usize>()),
    //         // len: size,
    //         alloc: CommAlloc {
    //             info: CommAllocInner::Raw(
    //                 m.as_ptr().add(std::mem::size_of::<usize>() + padding) as usize,
    //                 size,
    //             ),
    //             alloc_type: CommAllocType::Fabric,
    //         },
    //         _shmem: m,
    //     }
    // }
    unsafe {
        ShmemHandle {
            base_addr: m.as_ptr().add(std::mem::size_of::<usize>() + padding),
            num_bytes: size,
            _shmem: m,
        }
    }
}

pub(crate) struct ShmemAllocator {
    _shmem: ShmemHandle,
    mutex: *mut AtomicIsize,
    id: *mut AtomicUsize,
    barrier1: *mut usize,
    barrier2: *mut usize,
    my_pe: usize,
    num_pes: usize,
    job_id: usize,
    allocs: RwLock<Vec<Arc<ShmemAlloc>>>,
}

unsafe impl Sync for ShmemAllocator {}
unsafe impl Send for ShmemAllocator {}

impl std::fmt::Debug for ShmemAllocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShmemAllocator")
            .field("my_pe", &self.my_pe)
            .field("num_pes", &self.num_pes)
            .field("job_id", &self.job_id)
            .finish()
    }
}

impl ShmemAllocator {
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new(num_pes: usize, pe: usize, job_id: usize) -> Self {
        let size = std::mem::size_of::<AtomicUsize>()
            + std::mem::size_of::<usize>()
            + std::mem::size_of::<usize>() * num_pes * 2;
        let shmem = attach_to_shmem(
            num_pes,
            job_id,
            size,
            std::mem::align_of::<usize>(),
            "alloc",
            job_id,
            pe == 0,
        );
        let base_ptr = shmem.base_ptr();

        let data = unsafe { std::slice::from_raw_parts_mut(base_ptr, size) };
        if pe == 0 {
            for i in data {
                *i = 0;
            }
        }

        trace!("new shmem allocated! base_pointer{:?}", base_ptr);
        ShmemAllocator {
            _shmem: shmem,
            mutex: base_ptr as *mut AtomicIsize,
            id: unsafe { base_ptr.add(std::mem::size_of::<AtomicIsize>()) as *mut AtomicUsize },
            barrier1: unsafe {
                base_ptr
                    .add(std::mem::size_of::<AtomicIsize>() + std::mem::size_of::<AtomicUsize>())
                    as *mut usize
            },
            barrier2: unsafe {
                base_ptr.add(
                    std::mem::size_of::<AtomicIsize>()
                        + std::mem::size_of::<AtomicUsize>()
                        + std::mem::size_of::<usize>() * num_pes,
                ) as *mut usize
            },
            // barrier3: unsafe { base_ptr.add(std::mem::size_of::<AtomicUsize>() + std::mem::size_of::<usize>()) as *mut usize + std::mem::size_of::<usize>()*num_pes*2},
            my_pe: pe,
            num_pes: num_pes,
            job_id: job_id,
            allocs: RwLock::new(vec![]),
        }
    }

    //TODO update this to a dissemination barrier or someother optimized shmem barrier
    pub(crate) unsafe fn barrier(&self) {
        let _allocs = self.allocs.read();
        let barrier1 = std::slice::from_raw_parts_mut(self.barrier1, self.num_pes);
        let barrier2 = std::slice::from_raw_parts_mut(self.barrier2, self.num_pes);
        // Wait for all PEs to reach this point

        for i in 0..self.num_pes {
            while barrier2[i] != 0 {
                std::thread::yield_now();
            }
        }
        if self.my_pe == 0 {
            (*self.id).fetch_add(1, Ordering::SeqCst);
            barrier1[self.my_pe] = (*self.id).load(Ordering::SeqCst);
        }
        while barrier1[0] == 0 {
            std::thread::yield_now();
        }
        let id = (*self.id).load(Ordering::SeqCst);
        barrier1[self.my_pe] = id;
        for i in 1..self.num_pes {
            while barrier1[i] < id {
                std::thread::yield_now();
            }
        }
        barrier2[self.my_pe] = 1;
        if self.my_pe == 0 {
            self.mutex
                .as_ref()
                .unwrap()
                .fetch_add(self.num_pes as isize, Ordering::SeqCst);
        }
        self.mutex.as_ref().unwrap().fetch_sub(1, Ordering::SeqCst);
        while self.mutex.as_ref().unwrap().load(Ordering::SeqCst) != 0 {}
        // Reset barrier
        barrier1[self.my_pe] = 0;
        for i in 0..self.num_pes {
            while barrier1[i] != 0 {
                std::thread::yield_now();
            }
        }
        barrier2[self.my_pe] = 0;
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) unsafe fn alloc(&self, size: usize, align: usize, pes: &[usize]) -> Arc<ShmemAlloc> {
        let mut allocs = self.allocs.write();
        let barrier1 = std::slice::from_raw_parts_mut(self.barrier1, self.num_pes);
        let barrier2 = std::slice::from_raw_parts_mut(self.barrier2, self.num_pes);
        // println!("trying to alloc! {:?} {:?} {:?}",self.my_pe, barrier1,barrier2);
        // let barrier3 = std::slice::from_raw_parts_mut(self.barrier1, self.num_pes) ;
        let mut sub_alloc_pe_id = None;
        let mut pe_map = HashMap::new();
        for (i, pe) in pes.iter().enumerate() {
            pe_map.insert(*pe, i);
            if *pe == self.my_pe {
                sub_alloc_pe_id = Some(i);
            }
            while barrier2[*pe] != 0 {
                std::thread::yield_now();
            }
        }
        let sub_alloc_pe_id = sub_alloc_pe_id.expect("pe not in sub alloc list");
        // let mut pes_clone = pes.clone();
        // let first_pe = pes_clone.next().unwrap();

        // let mut pes_len = 1;

        if sub_alloc_pe_id == 0 {
            while let Err(_) = self.mutex.as_ref().unwrap().compare_exchange(
                0,
                1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                std::thread::yield_now();
            }
            (*self.id).fetch_add(1, Ordering::SeqCst);
            let id = (*self.id).load(Ordering::SeqCst);
            barrier1[self.my_pe] = id;
            for pe in pes.iter() {
                while barrier1[*pe] != id {
                    std::thread::yield_now();
                }
            }
        } else {
            while barrier1[pes[0]] == 0 {
                std::thread::yield_now();
            }
            let id = (*self.id).load(Ordering::SeqCst);
            barrier1[self.my_pe] = id;

            for pe in pes.iter() {
                while barrier1[*pe] != id {
                    std::thread::yield_now();
                }
            }
        }

        // println!("going to attach to shmem {:?} {:?} {:?} {:?} {:?}",size*pes_len,*self.id,self.my_pe, barrier1,barrier2);
        let shmem = attach_to_shmem(
            pes.len(),
            self.job_id,
            size * pes.len(),
            align,
            &((*self.id).load(Ordering::SeqCst).to_string()),
            (*self.id).load(Ordering::SeqCst),
            sub_alloc_pe_id == 0,
        );
        // let base_ptr = shmem.base_ptr();
        let my_base_ptr = shmem.base_ptr().add(size * sub_alloc_pe_id);
        barrier2[self.my_pe] = my_base_ptr as usize; //save the start of my segment in my address space

        //temporarily use the first element of the shared segment as a counter barrier
        let cnt = shmem.base_ptr() as *mut AtomicIsize;
        if sub_alloc_pe_id == 0 {
            cnt.as_ref()
                .unwrap()
                .fetch_add(pes.len() as isize, Ordering::SeqCst);
        }
        cnt.as_ref().unwrap().fetch_sub(1, Ordering::SeqCst);
        while cnt.as_ref().unwrap().load(Ordering::SeqCst) != 0 {}
        let addrs = barrier2.to_vec();
        trace!(
            "attached {:?} {:?} my offset: {:?}",
            self.my_pe,
            shmem.base_ptr(),
            my_base_ptr
        );
        barrier1[self.my_pe] = 0;
        for pe in pes.into_iter() {
            while barrier1[*pe] != 0 {
                std::thread::yield_now();
            }
        }
        barrier2[self.my_pe] = 0;
        if sub_alloc_pe_id == 0 {
            self.mutex.as_ref().unwrap().store(0, Ordering::SeqCst);
        }

        let alloc = Arc::new(ShmemAlloc {
            data: my_base_ptr,
            len: size,
            base_ptr: my_base_ptr,
            base_len: size,
            my_alloc_pe: sub_alloc_pe_id,
            pe_map,
            remote_addrs: addrs,
            _shmem: Arc::new(shmem),
        });
        allocs.push(alloc.clone());
        alloc
        // println!("{:?} {:?} {:?}",self./my_pe, barrier1,barrier2);
        // (shmem, sub_alloc_pe_id, addrs)
    }

    pub(crate) fn free_addr(&self, addr: usize) {
        let mut allocs = self.allocs.write();
        let len = allocs.len();
        allocs.retain(|alloc| alloc.data as usize != addr);
        if len == allocs.len() {
            panic!("failed to free addr: {:x}", addr);
        }
    }

    pub(crate) fn free_alloc(&self, alloc: &Arc<ShmemAlloc>) {
        let mut allocs = self.allocs.write();
        let len = allocs.len();
        allocs.retain(|a| !Arc::ptr_eq(a, alloc));
        if len == allocs.len() {
            panic!("failed to free alloc: {:?}", alloc);
        }
    }

    pub(crate) fn get_alloc_from_start_addr(
        &self,
        mem_addr: CommAllocAddr,
    ) -> AllocResult<Arc<ShmemAlloc>> {
        let allocs = self.allocs.read();
        for alloc in allocs.iter() {
            if alloc.start() == mem_addr.0 {
                return Ok(alloc.clone());
            }
        }
        Err(AllocError::LocalNotFound(mem_addr))
    }

    pub(crate) fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> Option<usize> {
        let allocs = self.allocs.read();
        for alloc in allocs.iter() {
            let remote_start = alloc.remote_addrs[remote_pe];
            if remote_start <= remote_addr && remote_addr < remote_start + alloc.len {
                return Some(alloc.data as usize + (remote_addr - remote_start));
            }
        }
        None
    }
    pub(crate) fn local_alloc_and_offset_from_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
    ) -> Option<(CommAlloc, usize)> {
        let allocs = self.allocs.read();
        for alloc in allocs.iter() {
            let remote_start = alloc.remote_addrs[remote_pe];
            if remote_start <= remote_addr && remote_addr < remote_start + alloc.len {
                return Some((alloc.clone().into(), remote_addr - remote_start));
            }
        }
        None
    }
    pub(crate) fn remote_addr(&self, remote_pe: usize, local_addr: usize) -> Option<usize> {
        let allocs = self.allocs.read();
        for alloc in allocs.iter() {
            if alloc.data as usize <= local_addr && local_addr < alloc.data as usize + alloc.len {
                return Some(alloc.remote_addrs[remote_pe] + (local_addr - alloc.data as usize));
            }
        }
        None
    }
}

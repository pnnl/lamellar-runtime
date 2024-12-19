use shared_memory::*;

struct MyShmem {
    data: *mut u8,
    len: usize,
    _shmem: Shmem,
}

impl std::fmt::Debug for MyShmem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MyShmem{{ data: {:?}, len: {:?} }}", self.data, self.len)
    }
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

fn attach_to_shmem(job_id: usize, size: usize, id: &str, header: usize, create: bool) -> MyShmem {
    let size = size + std::mem::size_of::<usize>();

    let shmem_id =
        "lamellar_".to_owned() + &(job_id.to_string()) + "_" + &(size.to_string()) + "_" + id;
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

#[derive(Debug)]
struct ShmemAlloc {
    _shmem: MyShmem,
    mutex: *mut AtomicUsize,
    id: *mut usize,
    barrier1: *mut usize,
    barrier2: *mut usize,
    // barrier3: *mut usize,
    my_pe: usize,
    num_pes: usize,
    job_id: usize,
}

unsafe impl Sync for ShmemAlloc {}
unsafe impl Send for ShmemAlloc {}

impl ShmemAlloc {
    fn new(num_pes: usize, pe: usize, job_id: usize) -> Self {
        let size = std::mem::size_of::<AtomicUsize>()
            + std::mem::size_of::<usize>()
            + std::mem::size_of::<usize>() * num_pes * 2;
        let shmem = attach_to_shmem(job_id, size, "alloc", job_id, pe == 0);
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
            job_id: job_id,
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
            self.job_id,
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

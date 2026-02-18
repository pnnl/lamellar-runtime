use shared_memory::{Shmem, ShmemConf, ShmemError};

pub(crate) struct ShmemSegment {
    base_addr: *mut u8,
    shmem: Shmem,
    len: usize,
}

unsafe impl Send for ShmemSegment {}
unsafe impl Sync for ShmemSegment {}

impl ShmemSegment {
    pub(crate) fn base_ptr(&self) -> *mut u8 {
        self.base_addr
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

pub(crate) fn attach_shmem_segment(
    job_id: usize,
    size: usize,
    align: usize,
    id: &str,
    header: usize,
    create: bool,
) -> ShmemSegment {
    let padding = std::mem::size_of::<usize>() % align;
    let shmem_size = std::mem::size_of::<usize>() + padding + size;
    let shmem_id =
        "lamellar_".to_owned() + &job_id.to_string() + "_" + &shmem_size.to_string() + "_" + id;

    let mut retry = 0;
    let shmem = loop {
        match ShmemConf::new()
            .size(shmem_size)
            .os_id(shmem_id.clone())
            .create()
        {
            Ok(m) => {
                if create {
                    unsafe {
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
                        if create {
                            unsafe {
                                *(m.as_ptr() as *mut _ as *mut usize) = header;
                            }
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

    let shmem = match shmem {
        Ok(m) => m,
        Err(e) => panic!("unable to create shared memory {:?} {:?}", shmem_id, e),
    };

    while unsafe { *(shmem.as_ptr() as *const _ as *const usize) } != header {
        std::thread::yield_now();
    }

    let base_addr = unsafe { shmem.as_ptr().add(std::mem::size_of::<usize>() + padding) };

    ShmemSegment {
        base_addr,
        shmem,
        len: size,
    }
}

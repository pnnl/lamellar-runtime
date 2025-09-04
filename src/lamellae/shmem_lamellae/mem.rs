use std::{sync::atomic::Ordering};

use tracing::trace;

use crate::{
    lamellae::{
        comm::{
            error::{AllocError, AllocResult},
            CommAlloc, CommAllocAddr, CommAllocInner, CommAllocType, CommMem,
        },
        AllocationType,
    },
    lamellar_alloc::{BTreeAlloc, LamellarAlloc},
};

use super::comm::{ShmemComm, SHMEM_SIZE};

impl CommMem for ShmemComm {
    #[tracing::instrument(skip(self), level = "debug")]
    fn alloc(
        &self,
        size: usize,
        alloc_type: AllocationType,
        align: usize,
    ) -> AllocResult<CommAlloc> {
        //shared memory segments are aligned on page boundaries so no need to pass in alignment constraint
        let inner_alloc = match alloc_type {
            AllocationType::Sub(pes) => {
                if pes.contains(&self.my_pe) {
                    let ret = unsafe { self.allocator.alloc(size, align, &pes) };
                    ret
                } else {
                    return Err(AllocError::IdError(self.my_pe));
                }
            }
            AllocationType::Global => unsafe {
                self.allocator
                    .alloc(size, align, &(0..self.num_pes).collect::<Vec<_>>())
            },
            _ => panic!("unexpected allocation type {:?} in rofi_alloc", alloc_type),
        };

        Ok(CommAlloc {
            inner_alloc: CommAllocInner::ShmemAlloc(inner_alloc),
            alloc_type: CommAllocType::Fabric,
        })
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn free(&self, alloc: CommAlloc) {
        //maybe need to do something more intelligent on the drop of the shmem_alloc
        debug_assert!(alloc.alloc_type == CommAllocType::Fabric);
        match alloc.inner_alloc {
            CommAllocInner::Raw(addr, _) => {
                println!("freeing raw alloc: {:x} should we ever be here?", addr);
                self.allocator.free_addr(addr);
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                trace!("freeing inner_alloc: {:?}", inner_alloc);
                self.allocator.free_alloc(&inner_alloc);
            }
            _ => {
                panic!("free should only be called with ShmemAlloc or Raw addr");
            }
        }
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn rt_alloc(&self, size: usize, align: usize) -> AllocResult<CommAlloc> {
        let allocs = self.runtime_allocs.read();
        for (inner_alloc, alloc) in allocs.iter() {
            if let Some(addr) = alloc.try_malloc(size, align) {
                trace!(
                    "new rt alloc: {:x} {} {}",
                    addr,
                    addr - inner_alloc.start(),
                    size
                );

                return Ok(CommAlloc {
                    inner_alloc: CommAllocInner::ShmemAlloc(
                        inner_alloc.sub_alloc(addr - inner_alloc.start(), size)?,
                    ),
                    alloc_type: CommAllocType::RtHeap,
                });
            }
        }
        Err(AllocError::OutOfMemoryError(size))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn rt_check_alloc(&self, size: usize, align: usize) -> bool {
        let allocs = self.runtime_allocs.read();
        for (_, alloc) in allocs.iter() {
            if alloc.fake_malloc(size, align) {
                return true;
            }
        }
        false
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn rt_free(&self, alloc: CommAlloc) {
        debug_assert!(alloc.alloc_type == CommAllocType::RtHeap);
        match alloc.inner_alloc {
            CommAllocInner::Raw(addr, _) => {
                trace!("should i be here, rt_free should only be called with AllocInfo");

                trace!("freeing rt alloc: {:x}", addr);
                let allocs = self.runtime_allocs.read();
                for (_, alloc) in allocs.iter() {
                    if let Ok(_) = alloc.free(addr) {
                        return;
                    }
                }
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                trace!("freeing rt alloc: {:?}", inner_alloc);
                let allocs = self.runtime_allocs.read();
                for (_, alloc) in allocs.iter() {
                    if let Ok(_) = alloc.free(inner_alloc.start()) {
                        return;
                    }
                }
                panic!("Error invalid free! {:?}", inner_alloc);
            }
            _ => panic!(
                "unexpected allocation type {:?} in rt_free",
                alloc.inner_alloc
            ),
        }
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn mem_occupied(&self) -> usize {
        let mut occupied = 0;
        let allocs = self.runtime_allocs.read();
        for alloc in allocs.iter() {
            occupied += alloc.1.occupied();
        }
        occupied
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn alloc_pool(&self, min_size: usize) {
        let size = std::cmp::max(
            min_size * 2 * self.num_pes,
            SHMEM_SIZE.load(Ordering::SeqCst),
        ) / self.num_pes;
        if let Ok(alloc) = self.alloc(size, AllocationType::Global, 0) {
            // println!("addr: {:x} - {:x}",addr, addr+size);
            if let CommAllocInner::ShmemAlloc(inner_alloc) = alloc.inner_alloc {
                let mut new_alloc = BTreeAlloc::new("libfabric_c_mem".to_string());
                new_alloc.init(inner_alloc.start(), size);
                self.runtime_allocs
                    .write()
                    .push((inner_alloc.clone(), new_alloc));
            } else {
                panic!("libfabric alloc pool should only be called with AllocInfo addr");
            }
        } else {
            panic!("[Error] out of system memory");
        }
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn num_pool_allocs(&self) -> usize {
        self.runtime_allocs.read().len()
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn print_pools(&self) {
        let allocs = self.runtime_allocs.read();
        println!("num_pools {:?}", allocs.len());
        for (_, alloc) in allocs.iter() {
            println!(
                // "{:x} {:?} {:?} {:?}",
                "{:x} {:?}",
                alloc.start_addr,
                alloc.max_size,
                // alloc.occupied(),
                // alloc.space_avail()
            );
        }
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> CommAllocAddr {
        self.allocator
            .local_addr(remote_pe, remote_addr)
            .expect("failed to get remote addr")
            .into()
    }

    fn local_alloc_and_offset_from_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
    ) -> (CommAlloc, usize) {
        self.allocator
            .local_alloc_and_offset_from_addr(remote_pe, remote_addr)
            .expect("remote addr doesnt correspnd to local alloc")
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn remote_addr(&self, pe: usize, local_addr: usize) -> CommAllocAddr {
        self.allocator
            .remote_addr(pe, local_addr)
            .expect("failed to get remote addr")
            .into()
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn get_alloc(&self, addr: CommAllocAddr) -> AllocResult<CommAlloc> {
        trace!("get_alloc: {:?}", addr);
        if let Ok(inner_alloc) = self.allocator.get_alloc_from_start_addr(addr) {
            return Ok(CommAlloc {
                inner_alloc: CommAllocInner::ShmemAlloc(inner_alloc),
                alloc_type: CommAllocType::Fabric,
            });
        }

        let allocs = self.runtime_allocs.read();
        for (inner_alloc, alloc) in allocs.iter() {
            if let Some(size) = alloc.find(addr.0) {
                return Ok(CommAlloc {
                    inner_alloc: CommAllocInner::ShmemAlloc(inner_alloc.sub_alloc(addr.0, size)?),
                    alloc_type: CommAllocType::RtHeap,
                });
            }
        }

        Err(AllocError::LocalNotFound(addr))
    }
}

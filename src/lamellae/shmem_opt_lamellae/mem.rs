use std::sync::Arc;

use tracing::trace;

use crate::{
    config,
    env_var::HeapMode,
    lamellae::{
        calc_alloc_padding_size_align,
        comm::{
            error::{AllocError, AllocResult},
            CommAlloc, CommAllocAddr, CommAllocInner, CommMem,
        },
        AllocationType,
    },
    lamellar_alloc::LamellarAlloc,
};

use super::comm::ShmemOptComm;

impl CommMem for ShmemOptComm {
    //#[tracing::instrument(skip(self), level = "debug")]
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
        // no zeroize: symmetric ranges are fresh sparse pages or were hole-punched on release

        Ok(CommAlloc {
            inner_alloc: Arc::new(CommAllocInner::ShmemOptAlloc(inner_alloc)),
            // alloc_type: CommAllocType::Fabric,
        })
    }

    // //#[tracing::instrument(skip(self), level = "debug")]
    // fn free(&self, alloc: CommAlloc) {
    //     //maybe need to do something more intelligent on the drop of the shmem_alloc
    //     assert!(alloc.alloc_type == CommAllocType::Fabric);
    //     match alloc.inner_alloc {
    //         CommAllocInner::Raw(addr, _) => {
    //             println!("freeing raw alloc: {:x} should we ever be here?", addr);
    //             self.allocator.free_addr(addr);
    //         }
    //         CommAllocInner::ShmemOptAlloc(inner_alloc) => {
    //             trace!("freeing inner_alloc: {:?}", inner_alloc);
    //             self.allocator.free_alloc(&inner_alloc);
    //         }
    //         _ => {
    //             panic!("free should only be called with ShmemOptAlloc or Raw addr");
    //         }
    //     }
    // }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn rt_alloc(&self, size: usize, align: usize) -> AllocResult<CommAlloc> {
        self.rt_alloc_inner(size, align, true)
    }

    fn rt_alloc_uninit(&self, size: usize, align: usize) -> AllocResult<CommAlloc> {
        self.rt_alloc_inner(size, align, false)
    }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn rt_check_alloc(&self, size: usize, align: usize) -> bool {
        let (_padding, size, align) = calc_alloc_padding_size_align(size, align);
        let allocs = self.runtime_allocs.read();
        for (_, alloc) in allocs.iter() {
            if alloc.fake_malloc(size, align) {
                return true;
            }
        }
        self.rt_room(size + align)
    }

    // //#[tracing::instrument(skip(self), level = "debug")]
    // fn rt_free(&self, alloc: CommAlloc) {
    //     assert!(alloc.alloc_type == CommAllocType::RtHeap);
    //     match alloc.inner_alloc {
    //         CommAllocInner::Raw(addr, _) => {
    //             trace!("freeing rt alloc: {:x}", addr);
    //             let allocs = self.runtime_allocs.read();
    //             for (_, alloc) in allocs.iter() {
    //                 if let Ok(_) = alloc.free(addr) {
    //                     return;
    //                 }
    //             }
    //         }
    //         CommAllocInner::ShmemOptAlloc(inner_alloc) => {
    //             trace!("freeing rt alloc: {:?}", inner_alloc);
    //             let allocs = self.runtime_allocs.read();
    //             for (_, alloc) in allocs.iter() {
    //                 if let Ok(_) = alloc.free(inner_alloc.start()) {
    //                     return;
    //                 }
    //             }
    //             panic!("Error invalid free! {:?}", inner_alloc);
    //         }
    //         _ => panic!(
    //             "unexpected allocation type {:?} in rt_free",
    //             alloc.inner_alloc
    //         ),
    //     }
    // }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn mem_occupied(&self) -> usize {
        let mut occupied = 0;
        let allocs = self.runtime_allocs.read();
        for alloc in allocs.iter() {
            occupied += alloc.1.occupied();
        }
        occupied
    }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn alloc_pool(&self, min_size: usize) {
        if config().heap_mode == HeapMode::Static {
            panic!("Error: alloc_pool should not be called in static heap mode, please set LAMELLAR_HEAP_MODE=dynamic or increase the heap size with LAMELLAR_HEAP_SIZE environment variable");
        }
        // local: pools are carved from the symmetric rt reservation, no collective needed
        if !self.grow_rt_pool(min_size) {
            panic!("[Error] out of shmem rt memory, increase LAMELLAR_SHMEM_REGION");
        }
    }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn num_pool_allocs(&self) -> usize {
        self.runtime_allocs.read().len()
    }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn print_pools(&self) {
        let allocs = self.runtime_allocs.read();
        println!("num_pools {:?}", allocs.len());
        for (_, alloc) in allocs.iter() {
            println!("{:x} {:?}", alloc.start_addr, alloc.max_size,);
        }
    }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> CommAllocAddr {
        self.allocator
            .local_addr(remote_pe, remote_addr)
            .expect("failed to get remote addr")
            .into()
    }

    fn one_sided_alloc_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
        num_bytes: usize,
    ) -> CommAlloc {
        self.allocator
            .one_sided_alloc_from_remote_pe_and_addr(remote_pe, remote_addr, num_bytes)
    }

    fn local_alloc_and_offset_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
    ) -> (CommAlloc, usize) {
        self.allocator
            .local_alloc_and_offset_from_remote_pe_and_addr(remote_pe, remote_addr)
            .expect("remote addr doesnt correspnd to local alloc")
    }

    fn local_rt_alloc_from_local_addr(&self, addr: usize) -> AllocResult<CommAlloc> {
        trace!("local_rt_alloc_from_local_addr: {:x}", addr);
        let allocs = self.runtime_allocs.read();
        for (inner_alloc, alloc) in allocs.iter() {
            if let Some(size) = alloc.find(addr) {
                return Ok(CommAlloc {
                    inner_alloc: Arc::new(CommAllocInner::ShmemOptAlloc(
                        inner_alloc
                            .sub_alloc(addr - inner_alloc.start(), size)?
                            .as_rt_alloc(alloc.clone())?,
                    )),
                    // alloc_type: CommAllocType::RtHeap,
                });
            }
        }
        Err(AllocError::LocalNotFound(CommAllocAddr(addr)))
    }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn remote_addr(&self, pe: usize, local_addr: usize) -> CommAllocAddr {
        self.allocator
            .remote_addr(pe, local_addr)
            .expect("failed to get remote addr")
            .into()
    }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn get_alloc_cloned(&self, addr: CommAllocAddr) -> AllocResult<CommAlloc> {
        trace!("get_alloc_cloned: {:?}", addr);
        if let Ok(inner_alloc) = self.allocator.get_alloc_from_start_addr(addr) {
            return Ok(CommAlloc {
                inner_alloc: Arc::new(CommAllocInner::ShmemOptAlloc(inner_alloc)),
                // alloc_type: CommAllocType::Fabric,
            });
        }

        let allocs = self.runtime_allocs.read();
        for (inner_alloc, alloc) in allocs.iter() {
            if let Some(size) = alloc.find(addr.0) {
                return Ok(CommAlloc {
                    inner_alloc: Arc::new(CommAllocInner::ShmemOptAlloc(
                        inner_alloc.sub_alloc(addr.0 - inner_alloc.start(), size)?,
                    )),
                    // alloc_type: CommAllocType::RtHeap,
                });
            }
        }

        Err(AllocError::LocalNotFound(addr))
    }
}

impl ShmemOptComm {
    fn rt_alloc_inner(&self, size: usize, align: usize, zero: bool) -> AllocResult<CommAlloc> {
        // add space for ref count
        let (padding, size, align) = calc_alloc_padding_size_align(size, align);

        loop {
            {
                let allocs = self.runtime_allocs.read();
                // newest pool first: older pools are the likeliest to be full
                for (inner_alloc, alloc) in allocs.iter().rev() {
                    if let Some(addr) = alloc.try_malloc(size, align) {
                        trace!(
                            "new rt alloc: {:x} {} {}",
                            addr,
                            addr - inner_alloc.start(),
                            size
                        );
                        let alloc = inner_alloc.rt_alloc(
                            alloc.clone(),
                            addr - inner_alloc.start(),
                            padding,
                            size,
                        )?;
                        if zero {
                            unsafe {
                                alloc.zeroize_bytes();
                            }
                        }

                        return Ok(CommAlloc {
                            inner_alloc: Arc::new(CommAllocInner::ShmemOptAlloc(alloc)),
                            // alloc_type: CommAllocType::RtHeap,
                        });
                    }
                }
            }
            // grow locally inside the reserved rt range
            if !self.rt_room(size + align) || !self.grow_rt_pool(size + align) {
                return Err(AllocError::OutOfMemoryError(size));
            }
        }
    }
}

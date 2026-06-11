use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::{
    config,
    env_var::HeapMode,
    lamellae::{
        comm::{
            calc_alloc_padding_size_align,
            error::{AllocError, AllocResult},
            CommAlloc, CommAllocAddr, CommAllocInner, CommMem,
        },
        AllocationType,
    },
    lamellar_alloc::{BTreeAlloc, LamellarAlloc},
};

use super::comm::{LibfabricMtComm, HEAP_SIZE};

use tracing::{debug, trace};

impl CommMem for LibfabricMtComm {
    //#[tracing::instrument(skip(self), level = "debug")]
    fn alloc(
        &self,
        size: usize,
        alloc_type: AllocationType,
        align: usize,
    ) -> AllocResult<CommAlloc> {
        let inner_alloc = self.ofi.alloc(size, alloc_type, align)?;
        // unsafe {
        //     inner_alloc.zeroize_bytes();
        // }
        let comm_alloc = CommAlloc {
            inner_alloc: Arc::new(CommAllocInner::LibfabricMtAlloc(inner_alloc)),
            // alloc_type: CommAllocType::Fabric,
        };
        // self.fabric_allocs.write().insert(addr,comm_alloc.clone());
        Ok(comm_alloc)
    }

    // //#[tracing::instrument(skip(self), level = "debug")]
    // fn free(&self, alloc: CommAlloc) {
    //     assert!(alloc.alloc_type == CommAllocType::Fabric);
    //     match alloc.inner_alloc {
    //         CommAllocInner::Raw(addr, _) => {
    //             println!("freeing raw alloc: {:x} should we ever be here?", addr);
    //             self.ofi.free_addr(addr).unwrap();
    //         }
    //         CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
    //             trace!("freeing  inner_alloc: {:?}", inner_alloc);
    //             self.ofi.free_alloc(&inner_alloc).unwrap();
    //         }
    //         _ => {
    //             panic!("free should only be called with LibfabricMtAlloc or Raw addr");
    //         }
    //     }
    // }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn rt_alloc(&self, size: usize, align: usize) -> AllocResult<CommAlloc> {
        // add space for ref count
        let (padding, size, align) = calc_alloc_padding_size_align(size, align);

        let allocs = self.runtime_allocs.read();
        for (inner_alloc, alloc) in allocs.iter() {
            if let Some(addr) = alloc.try_malloc(size, align) {
                let alloc = inner_alloc.rt_alloc(
                    alloc.clone(),
                    addr - inner_alloc.start(),
                    padding,
                    size,
                )?;
                trace!(
                    "new rt alloc: 0x{:x}-0x{:x} {} {} {:?}", // {:?}",
                    addr,
                    addr + size,
                    addr - inner_alloc.start(),
                    size,
                    alloc,
                    // std::backtrace::Backtrace::capture()
                );
                // unsafe {
                //     alloc.zeroize_bytes();
                // }
                return Ok(CommAlloc {
                    inner_alloc: Arc::new(CommAllocInner::LibfabricMtAlloc(alloc)),
                    // alloc_type: CommAllocType::RtHeap,
                });
            }
        }
        Err(AllocError::OutOfMemoryError(size))
    }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn rt_check_alloc(&self, size: usize, align: usize) -> bool {
        // add space for ref count
        let (_padding, size, align) = calc_alloc_padding_size_align(size, align);

        let allocs = self.runtime_allocs.read();
        for (_, alloc) in allocs.iter() {
            if alloc.fake_malloc(size, align) {
                return true;
            }
        }
        false
    }

    // //#[tracing::instrument(skip(self), level = "debug")]
    // fn rt_free(&self, alloc: CommAlloc) {
    //     assert!(alloc.alloc_type == CommAllocType::RtHeap);
    //     debug!("rt_free: {:?}", alloc);
    //     match alloc.inner_alloc {
    //         CommAllocInner::Raw(addr, _) => {
    //             info!(
    //                 "freeing rt alloc: {:x} {:?}",
    //                 addr,
    //                 std::backtrace::Backtrace::capture()
    //             );
    //             let allocs = self.runtime_allocs.read();
    //             for (_, alloc) in allocs.iter() {
    //                 if let Ok(_) = alloc.free(addr) {
    //                     info!(
    //                         "freed rt alloc: {:x} {:?}",
    //                         addr,
    //                         std::backtrace::Backtrace::capture()
    //                     );
    //                     return;
    //                 }
    //             }
    //             error!(
    //                 "Error invalid free from addr! {:x} {:?}",
    //                 addr,
    //                 std::backtrace::Backtrace::capture()
    //             );
    //             for (_, alloc) in allocs.iter() {
    //                 error!("{:?}", alloc);
    //             }
    //             panic!("Error invalid free from addr! {:x}", addr);
    //         }
    //         CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
    //             info!(
    //                 "freeing rt alloc: {:?} cnt: {} {:?}",
    //                 inner_alloc,
    //                 std::sync::Arc::strong_count(&inner_alloc),
    //                 std::backtrace::Backtrace::capture()
    //             );
    //             let allocs = self.runtime_allocs.read();
    //             for (_, alloc) in allocs.iter() {
    //                 if let Ok(_) = alloc.free(inner_alloc.start()) {
    //                     info!(
    //                         "freed rt alloc: {:?} cnt: {} {:?}",
    //                         inner_alloc,
    //                         std::sync::Arc::strong_count(&inner_alloc),
    //                         std::backtrace::Backtrace::capture()
    //                     );
    //                     return;
    //                 }
    //             }
    //             error!(
    //                 "Error invalid free from alloc! {:?} {} {:?}",
    //                 inner_alloc,
    //                 std::sync::Arc::strong_count(&inner_alloc),
    //                 std::backtrace::Backtrace::capture()
    //             );
    //             for (_, alloc) in allocs.iter() {
    //                 error!("{:?}", alloc);
    //             }
    //             panic!("Error invalid free from alloc! {:?}", inner_alloc);
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
            let tmp = alloc.1.occupied();
            if tmp > 0 {
                debug!("alloc {:?} {:?} occupied: {tmp}", alloc.0, alloc.1);
            }
            occupied += tmp;
        }
        occupied
    }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn alloc_pool(&self, min_size: usize) {
        if config().heap_mode == HeapMode::Static {
            panic!("Error: alloc_pool should not be called in static heap mode, please set LAMELLAR_HEAP_MODE=dynamic or increase the heap size with LAMELLAR_HEAP_SIZE environment variable");
        }
        let size = std::cmp::max(
            min_size * 2 * self.num_pes,
            HEAP_SIZE.load(Ordering::SeqCst),
        );
        println!("Allocating new pool of size: {} bytes", size);
        if let Ok(alloc) = self.alloc(size, AllocationType::Global, 0) {
            // println!("addr: {:x} - {:x}",addr, addr+size);

            if let CommAllocInner::LibfabricMtAlloc(inner_alloc) = alloc.inner_alloc.as_ref() {
                println!("Allocated new LibfabricMt alloc pool: {:?}", inner_alloc);
                let mut new_alloc = BTreeAlloc::new("LibfabricMt_mem".to_string());
                new_alloc.init(inner_alloc.start(), size);
                self.runtime_allocs
                    .write()
                    .push((inner_alloc.clone(), new_alloc));
            } else {
                panic!("LibfabricMt alloc pool should only be called with AllocInfo addr");
            }
        } else {
            panic!("[Error] out of system memory");
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
        for (_info, alloc) in allocs.iter() {
            println!("{:x} {:?}", alloc.start_addr, alloc.max_size,);
        }
    }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> CommAllocAddr {
        self.ofi.local_addr(remote_pe, remote_addr).into()
    }

    fn one_sided_alloc_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
        num_bytes: usize,
    ) -> CommAlloc {
        self.ofi
            .one_sided_alloc_from_remote_pe_and_addr(remote_pe, remote_addr, num_bytes)
    }

    fn local_alloc_and_offset_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
    ) -> (CommAlloc, usize) {
        self.ofi
            .local_alloc_and_offset_from_remote_pe_and_addr(remote_pe, remote_addr)
            .expect(&format!(
                "local_alloc_and_offset_from_remote_pe_and_addr failed for pe: {} addr: {:x}",
                remote_pe, remote_addr
            ))
    }

    fn local_rt_alloc_from_local_addr(&self, addr: usize) -> AllocResult<CommAlloc> {
        for (inner_alloc, alloc) in self.runtime_allocs.read().iter() {
            if let Some(size) = alloc.find(addr) {
                let comm_alloc = CommAlloc {
                    inner_alloc: Arc::new(CommAllocInner::LibfabricMtAlloc(
                        inner_alloc
                            .sub_alloc(addr - inner_alloc.start(), size)?
                            .as_rt_alloc(alloc.clone())?,
                    )),
                    // alloc_type: CommAllocType::RtHeap,
                };
                return Ok(comm_alloc);
            }
        }
        Err(AllocError::LocalNotFound(CommAllocAddr(addr.into())))
    }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn remote_addr(&self, pe: usize, local_addr: usize) -> CommAllocAddr {
        self.ofi.remote_addr(pe, local_addr).into()
    }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn get_alloc_cloned(&self, addr: CommAllocAddr) -> AllocResult<CommAlloc> {
        trace!("get_alloc cloned: {:?}", addr);
        if let Ok(alloc) = self.ofi.get_alloc_from_start_addr(addr) {
            return Ok(CommAlloc {
                inner_alloc: Arc::new(CommAllocInner::LibfabricMtAlloc(alloc)),
                // alloc_type: CommAllocType::Fabric,
            });
        }

        let allocs = self.runtime_allocs.read();
        for (inner_alloc, alloc) in allocs.iter() {
            if let Some(size) = alloc.find(addr.0) {
                return Ok(CommAlloc {
                    inner_alloc: Arc::new(CommAllocInner::LibfabricMtAlloc(
                        inner_alloc.sub_alloc(addr.0, size)?,
                    )),
                    // alloc_type: CommAllocType::RtHeap,
                });
            }
        }

        Err(AllocError::LocalNotFound(addr))
    }
}

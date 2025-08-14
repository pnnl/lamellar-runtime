use std::sync::atomic::Ordering;

use tracing::trace;

use crate::{
    lamellae::{
        comm::{
            error::{AllocError, AllocResult},
            CommAlloc, CommAllocAddr, CommAllocInfo, CommAllocType, CommMem,
        },
        AllocationType,
    },
    lamellar_alloc::{BTreeAlloc, LamellarAlloc},
};

use super::comm::{LibfabricComm, HEAP_SIZE};

impl CommMem for LibfabricComm {
    #[tracing::instrument(skip(self), level = "debug")]
    fn alloc(
        &self,
        size: usize,
        alloc_type: AllocationType,
        align: usize,
    ) -> AllocResult<CommAlloc> {
        // rofi_c allocs are aligned on page boundaries so no need to pass in alignment constraint
        let alloc_info = self.ofi.alloc(size, alloc_type)?;
        let comm_alloc = CommAlloc {
            info: CommAllocInfo::AllocInfo(alloc_info),
            alloc_type: CommAllocType::Fabric,
        };
        // self.fabric_allocs.write().insert(addr,comm_alloc.clone());
        Ok(comm_alloc)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn free(&self, alloc: CommAlloc) {
        debug_assert!(alloc.alloc_type == CommAllocType::Fabric);
        match alloc.info {
            CommAllocInfo::Raw(addr, _) => {
                println!("freeing raw alloc: {:x} should we ever be here?", addr);
                self.ofi.free_addr(addr);
            }
            CommAllocInfo::AllocInfo(alloc_info) => {
                trace!("freeing alloc info: {:?}", alloc_info);
                self.ofi.free_alloc(&alloc_info);
            }
        }
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn rt_alloc(&self, size: usize, align: usize) -> AllocResult<CommAlloc> {
        let allocs = self.runtime_allocs.read();
        for (info, alloc) in allocs.iter() {
            if let Some(addr) = alloc.try_malloc(size, align) {
                trace!("new rt alloc: {:x} {} {}", addr,addr-info.start(), size);

                return Ok(CommAlloc {
                    info: CommAllocInfo::AllocInfo(info.sub_alloc(addr-info.start(), size)?),
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
        match alloc.info {
            CommAllocInfo::Raw(addr, _) => {
                trace!("should i be here, rt_free should only be called with AllocInfo");
                
                trace!("freeing rt alloc: {:x}", addr);
                let allocs = self.runtime_allocs.read();
                for (_, alloc) in allocs.iter() {
                    if let Ok(_) = alloc.free(addr) {
                        return;
                    }
                }
            }
            CommAllocInfo::AllocInfo(info) => {
                trace!("freeing rt alloc: {:?}", info);
                let allocs = self.runtime_allocs.read();
                for (_, alloc) in allocs.iter() {
                    if let Ok(_) = alloc.free(info.start()) {
                        return;
                    }
                }
                panic!("Error invalid free! {:?}", info);
            }
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
            HEAP_SIZE.load(Ordering::SeqCst),
        );
        if let Ok(alloc) = self.alloc(size, AllocationType::Global, 0) {
            // println!("addr: {:x} - {:x}",addr, addr+size);

            if let CommAllocInfo::AllocInfo(info) = alloc.info {
                let mut new_alloc = BTreeAlloc::new("libfabric_c_mem".to_string());
                new_alloc.init(info.start(), size);
                self.runtime_allocs.write().push((info.clone(), new_alloc));
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
        for (info, alloc) in allocs.iter() {
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
        self.ofi.local_addr(remote_pe, remote_addr).into()
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn remote_addr(&self, pe: usize, local_addr: usize) -> CommAllocAddr {
        self.ofi.remote_addr(pe, local_addr).into()
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn get_alloc(&self, addr: CommAllocAddr) -> AllocResult<CommAlloc> {
        trace!("get_alloc: {:?}", addr);
        if let Ok(alloc) = self.ofi.get_alloc_from_start_addr(addr) {
            return Ok(CommAlloc {
                info: CommAllocInfo::AllocInfo(alloc),
                alloc_type: CommAllocType::Fabric,
            });
        }

        let allocs = self.runtime_allocs.read();
        for (info, alloc) in allocs.iter() {
            if let Some(size) = alloc.find(addr.0) {
                return Ok(CommAlloc {
                    info: CommAllocInfo::AllocInfo(info.sub_alloc(addr.0, size)?),
                    alloc_type: CommAllocType::RtHeap,
                });
            }
        }

        Err(AllocError::LocalNotFound(addr))
    }
}

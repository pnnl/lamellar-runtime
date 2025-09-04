use std::{collections::HashMap, sync::atomic::Ordering};

use tracing::{debug, trace};

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

use super::{
    comm::{RofiCComm, ROFI_SIZE},
    fabric::*,
};

impl CommMem for RofiCComm {
    #[tracing::instrument(skip(self), level = "debug")]
    fn alloc(
        &self,
        size: usize,
        alloc_type: AllocationType,
        align: usize,
    ) -> AllocResult<CommAlloc> {
        // rofi_c allocs are aligned on page boundaries so no need to pass in alignment constraint
        let addr = rofi_c_alloc(size, alloc_type)? as usize;
        let comm_alloc = CommAlloc {
            inner_alloc: CommAllocInner::Raw(addr, size),
            alloc_type: CommAllocType::Fabric,
        };
        self.fabric_allocs.write().insert(addr, comm_alloc.clone());
        Ok(comm_alloc)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn free(&self, alloc: CommAlloc) {
        //maybe need to do something more intelligent on the drop of the rofi_c_alloc
        debug_assert!(alloc.alloc_type == CommAllocType::Fabric);
        let addr = alloc.comm_addr().into();
        rofi_c_release(addr);
        self.fabric_allocs.write().remove(&addr);
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn rt_alloc(&self, size: usize, align: usize) -> AllocResult<CommAlloc> {
        let allocs = self.runtime_allocs.read();
        for alloc in allocs.iter() {
            if let Some(addr) = alloc.try_malloc(size, align) {
                trace!("new rt alloc: {:x} {}", addr, size);
                return Ok(CommAlloc {
                    inner_alloc: CommAllocInner::Raw(addr, size),
                    alloc_type: CommAllocType::RtHeap,
                });
            }
        }
        Err(AllocError::OutOfMemoryError(size))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn rt_check_alloc(&self, size: usize, align: usize) -> bool {
        let allocs = self.runtime_allocs.read();
        for alloc in allocs.iter() {
            if alloc.fake_malloc(size, align) {
                return true;
            }
        }
        false
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn rt_free(&self, alloc: CommAlloc) {
        trace!("freeing rt alloc: {:x}", alloc.comm_addr());
        debug_assert!(alloc.alloc_type == CommAllocType::RtHeap);
        if let CommAllocInner::Raw(addr, _) = alloc.info {
            trace!("freeing raw alloc: {:x}", addr);
            let allocs = self.runtime_allocs.read();
            for alloc in allocs.iter() {
                if let Ok(_) = alloc.free(addr) {
                    return;
                }
            }
            panic!("Error invalid free! {:?}", addr);
        } else {
            unreachable!("rt_free should only be called with Raw allocs, not AllocInfo");
        }
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn mem_occupied(&self) -> usize {
        let mut occupied = 0;
        let allocs = self.runtime_allocs.read();
        for alloc in allocs.iter() {
            occupied += alloc.occupied();
        }
        occupied
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn alloc_pool(&self, min_size: usize) {
        let size = std::cmp::max(
            min_size * 2 * self.num_pes,
            ROFI_SIZE.load(Ordering::SeqCst),
        );
        if let Ok(alloc) = self.alloc(size, AllocationType::Global, 0) {
            // println!("addr: {:x} - {:x}",addr, addr+size);
            if let CommAllocInner::Raw(addr, _) = alloc.info {
                let mut new_alloc = BTreeAlloc::new("rofi_c_mem".to_string());
                new_alloc.init(addr, size);
                self.runtime_allocs.write().push(new_alloc);
            } else {
                panic!("rofi_c alloc_pool should only be called with Raw allocs, not AllocInfo");
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
        for alloc in allocs.iter() {
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
        rofi_c_local_addr(remote_pe, remote_addr)
            .expect("unable to locate local memory addr for remote addr")
            .into()
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn remote_addr(&self, pe: usize, local_addr: usize) -> CommAllocAddr {
        rofi_c_remote_addr(pe, local_addr)
            .expect("unable to locate remote memory addr for local addr")
            .into()
    }

    #[tracing::instrument(skip(self), level = "debug")]
    fn get_alloc(&self, addr: CommAllocAddr) -> AllocResult<CommAlloc> {
        trace!("get_alloc: {:?}", addr);
        let allocs = self.fabric_allocs.read();
        if let Some(alloc) = allocs.get(&addr.0) {
            return Ok(alloc.clone());
        }
        let allocs = self.runtime_allocs.read();
        for alloc in allocs.iter() {
            if let Some(size) = alloc.find(addr.0) {
                return Ok(CommAlloc {
                    inner_alloc: CommAllocInner::Raw(addr.0, size),
                    alloc_type: CommAllocType::RtHeap,
                });
            }
        }
        Err(AllocError::LocalNotFound(addr))
    }
}

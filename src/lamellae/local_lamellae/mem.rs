use core::panic;
use std::sync::Arc;

use tracing::trace;

use crate::lamellae::{
    comm::{error::AllocResult, CommMem},
    local_lamellae::comm::LocalAlloc,
    AllocError, CommAlloc, CommAllocAddr, CommAllocInner, CommAllocType,
};

use super::{comm::LocalComm, AllocationType};

impl CommMem for LocalComm {
    #[tracing::instrument(skip_all, level = "debug")]
    fn alloc(
        &self,
        size: usize,
        _alloc_type: AllocationType,
        align: usize,
    ) -> AllocResult<CommAlloc> {
        let layout = std::alloc::Layout::from_size_align(size, align).unwrap();
        let data_ptr = unsafe { std::alloc::alloc(layout) };
        let data_addr = data_ptr as usize;
        let alloc = Arc::new(LocalAlloc {
            ptr: data_ptr as *mut u8,
            layout: layout,
        });
        let mut allocs = self.allocs.lock();
        allocs.insert(data_addr, alloc.clone());
        trace!("new alloc: {:?} {}", alloc.ptr, alloc.layout.size());
        Ok(CommAlloc {
            inner_alloc: CommAllocInner::LocalAlloc(alloc),
            alloc_type: CommAllocType::Fabric,
        })
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn free(&self, alloc: CommAlloc) {
        assert!(alloc.alloc_type == CommAllocType::Fabric);
        let mut allocs = self.allocs.lock();
        if let Some(data_ptr) = allocs.remove(&alloc.comm_addr().into()) {
            trace!("freeing alloc: {:x}", alloc.comm_addr());
            unsafe {
                std::alloc::dealloc(data_ptr.ptr, data_ptr.layout);
            };
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn rt_alloc(&self, size: usize, align: usize) -> AllocResult<CommAlloc> {
        let layout = std::alloc::Layout::from_size_align(size, align).unwrap();
        let data_ptr = unsafe { std::alloc::alloc(layout) };
        let data_addr = data_ptr as usize;
        let alloc = Arc::new(LocalAlloc {
            ptr: data_ptr,
            layout,
        });
        let mut allocs = self.heap_allocs.lock();
        allocs.insert(data_addr, alloc.clone());
        trace!("new rt alloc: {:x} {}", data_addr, size);
        Ok(CommAlloc {
            inner_alloc: CommAllocInner::LocalAlloc(alloc),
            alloc_type: CommAllocType::RtHeap,
        })
    }

    fn rt_check_alloc(&self, _size: usize, _align: usize) -> bool {
        true
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn rt_free(&self, alloc: CommAlloc) {
        assert!(alloc.alloc_type == CommAllocType::RtHeap);
        let mut allocs = self.heap_allocs.lock();
        if let Some(data_ptr) = allocs.remove(&alloc.comm_addr().into()) {
            trace!("freeing alloc: {:x}", alloc.comm_addr());
            unsafe {
                std::alloc::dealloc(data_ptr.ptr, data_ptr.layout);
            };
        }
    }

    fn mem_occupied(&self) -> usize {
        let mut occupied = 0;
        let allocs = self.allocs.lock();
        for alloc in allocs.iter() {
            occupied += alloc.1.layout.size();
        }
        occupied
    }

    fn alloc_pool(&self, _min_size: usize) {
        panic!("should never alloc a pool in local")
    }
    fn num_pool_allocs(&self) -> usize {
        1
    }

    fn print_pools(&self) {
        println!("no pools in local")
    }

    fn local_addr(&self, _remote_pe: usize, remote_addr: usize) -> CommAllocAddr {
        CommAllocAddr(remote_addr)
    }

    fn local_alloc_and_offset_from_addr(
        &self,
        _remote_pe: usize,
        remote_addr: usize,
    ) -> (CommAlloc, usize) {
        let allocs = self.allocs.lock();
        for (_addr, alloc) in allocs.iter() {
            if alloc.start() <= remote_addr && remote_addr < alloc.start() + alloc.num_bytes() {
                return (alloc.clone().into(), remote_addr - alloc.start());
            }
        }
        let allocs = self.heap_allocs.lock();
        for (_addr, alloc) in allocs.iter() {
            if alloc.start() <= remote_addr && remote_addr < alloc.start() + alloc.num_bytes() {
                return (alloc.clone().into(), remote_addr - alloc.start());
            }
        }
        panic!(
            "failed to find local alloc for remote addr: {}",
            remote_addr
        );
    }
    fn remote_addr(&self, _pe: usize, local_addr: usize) -> CommAllocAddr {
        CommAllocAddr(local_addr)
    }

    fn get_alloc(&self, addr: CommAllocAddr) -> AllocResult<CommAlloc> {
        let allocs = self.allocs.lock();
        if let Some(alloc) = allocs.get(&addr.0) {
            return Ok(CommAlloc {
                inner_alloc: CommAllocInner::LocalAlloc(alloc.clone()),
                alloc_type: CommAllocType::Fabric,
            });
        }
        let allocs = self.heap_allocs.lock();
        if let Some(alloc) = allocs.get(&addr.0) {
            return Ok(CommAlloc {
                inner_alloc: CommAllocInner::LocalAlloc(alloc.clone()),
                alloc_type: CommAllocType::RtHeap,
            });
        }
        Err(AllocError::LocalNotFound(addr))
    }
}

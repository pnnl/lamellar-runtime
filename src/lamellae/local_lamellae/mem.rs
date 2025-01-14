use crate::{env_var::Alloc, lamellae::{comm::{error::AllocResult, CommMem}, AllocError, CommAlloc, CommAllocAddr, CommAllocType}};

use super::{
    comm::{LocalComm, MyPtr},
    AllocationType,
};

impl CommMem for LocalComm {
    fn alloc(&self, size: usize, _alloc_type: AllocationType, align: usize) -> AllocResult<CommAlloc> {
        let layout = std::alloc::Layout::from_size_align(size, align).unwrap();
        let data_ptr = unsafe { std::alloc::alloc(layout) };
        let data_addr = data_ptr as usize;
        let mut allocs = self.allocs.lock();
        allocs.insert(
            data_addr,
            MyPtr {
                ptr: data_ptr as *mut u8,
                layout: layout,
            },
        );
        Ok(CommAlloc {
            addr: data_addr,
            size,
            alloc_type: CommAllocType::Fabric,
        })
    }

    fn free(&self, alloc: CommAlloc) {
        debug_assert!(alloc.alloc_type == CommAllocType::Fabric);
        let mut allocs = self.allocs.lock();
        if let Some(data_ptr) = allocs.remove(&alloc.addr) {
            unsafe {
                std::alloc::dealloc(data_ptr.ptr, data_ptr.layout);
            };
        }
    }

    fn rt_alloc(&self, size: usize, align: usize) -> AllocResult<CommAlloc> {
        let layout = std::alloc::Layout::from_size_align(size, align).unwrap();
        let data_ptr = unsafe { std::alloc::alloc(layout) };
        let data_addr = data_ptr as usize;
        let mut allocs = self.heap_allocs.lock();
        allocs.insert(
            data_addr,
            MyPtr {
                ptr: data_ptr,
                layout,
            },
        );
        Ok(CommAlloc {
            addr: data_addr,
            size,
            alloc_type: CommAllocType::RtHeap,
        })
    }

    fn rt_check_alloc(&self, _size: usize, _align: usize) -> bool {
        true
    }

    fn rt_free(&self, alloc: CommAlloc) {
        debug_assert!(alloc.alloc_type == CommAllocType::RtHeap);
        let mut allocs = self.heap_allocs.lock();
        if let Some(data_ptr) = allocs.remove(&alloc.addr) {
            unsafe {
                std::alloc::dealloc(data_ptr.ptr, data_ptr.layout);
                // let _ = Box::from_raw(data_ptr.ptr);
            }; //it will free when dropping from scope
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

    fn base_addr(&self) -> CommAllocAddr {
        0
    }
    fn local_addr(&self, _remote_pe: usize, remote_addr: usize) -> CommAllocAddr {
        remote_addr
    }
    fn remote_addr(&self, _pe: usize, local_addr: usize) -> CommAllocAddr {
        local_addr
    }
    fn local_alloc(&self, _remote_pe: usize, remote_addr: CommAllocAddr) -> AllocResult<CommAlloc> {
        let  allocs: parking_lot::lock_api::MutexGuard<'_, parking_lot::RawMutex, std::collections::HashMap<usize, MyPtr>> = self.allocs.lock();
        if let Some(alloc) = allocs.get(remote_addr) {
            return Ok(CommAlloc {
                addr: remote_addr,
                size: alloc.layout.size(),
                alloc_type: CommAllocType::Fabric,
            })
        }
        let  allocs = self.heap_allocs.lock();
        if  let Some(alloc) = allocs.get(remote_addr) {
            return Ok(CommAlloc {
                addr: remote_addr,
                size: alloc.layout.size(),
                alloc_type: CommAllocType::RtHeap,
            })
        }
        Err(AllocError::LocalNotFound(remote_addr))

    }
}

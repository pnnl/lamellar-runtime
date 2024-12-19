use crate::lamellae::comm::error::AllocResult;

use super::{comm::LocalComm, AllocationType};

impl CommMem for LocalComm {
    fn alloc(&self, size: usize, alloc_type: AllocationType) -> AllocResult<usize> {
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
        Ok(data_addr)
    }

    fn free(&self, addr: usize) {
        let mut allocs = self.allocs.lock();
        if let Some(data_ptr) = allocs.remove(&addr) {
            unsafe {
                std::alloc::dealloc(data_ptr.ptr, data_ptr.layout);
            };
        }
    }

    fn rt_alloc(&self, size: usize, align: usize) -> AllocResult<usize> {
        let layout = std::alloc::Layout::from_size_align(size, align).unwrap();
        let data_ptr = unsafe { std::alloc::alloc(layout) };
        let data_addr = data_ptr as usize;
        let mut allocs = self.allocs.lock();
        allocs.insert(
            data_addr,
            MyPtr {
                ptr: data_ptr,
                layout,
            },
        );
        Ok(data_addr)
    }

    fn rt_check_alloc(&self, size: usize, align: usize) -> bool {
        let allocs = self.alloc.read();
        for alloc in allocs.iter() {
            if alloc.fake_malloc(size, align) {
                return true;
            }
        }
        false
    }

    fn rt_free(&self, addr: usize) {
        let mut allocs = self.allocs.lock();
        if let Some(data_ptr) = allocs.remove(&addr) {
            unsafe {
                std::alloc::dealloc(data_ptr.ptr, data_ptr.layout);
                // let _ = Box::from_raw(data_ptr.ptr);
            }; //it will free when dropping from scope
        }
    }

    fn occupied(&self) -> usize {
        let mut occupied = 0;
        let allocs = self.alloc.read();
        for alloc in allocs.iter() {
            occupied += alloc.occupied();
        }
        occupied
    }

    fn alloc_pool(&self, min_size: usize) {
        panic!("should never alloc a pool in local")
    }
    fn num_pool_allocs(&self) -> usize {
        1
    }

    fn print_pools(&self) {
        println!("no pools in local")
    }

    fn base_addr(&self) -> usize {
        0
    }
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize {
        remote_addr
    }
    fn remote_addr(&self, pe: usize, local_addr: usize) -> usize {
        local_addr
    }
}

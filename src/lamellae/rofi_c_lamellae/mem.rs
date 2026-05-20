use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, sync::atomic::Ordering};

use tracing::{debug, trace};

use crate::{
    config,
    env_var::HeapMode,
    lamellae::{
        comm::{
            alloc::calc_alloc_padding_size_align,
            error::{AllocError, AllocResult},
            CommAlloc, CommAllocAddr, CommAllocInner, CommAllocType, CommMem,
        },
        AllocationType,
    },
    lamellar_alloc::{BTreeAlloc, LamellarAlloc},
};

use super::{
    comm::{RofiCComm, HEAP_SIZE},
    fabric::*,
    rofi::*,
};

impl CommMem for RofiCComm {
    //#[tracing::instrument(skip(self), level = "debug")]
    fn alloc(
        &self,
        size: usize,
        alloc_type: AllocationType,
        align: usize,
    ) -> AllocResult<CommAlloc> {
        let inner_alloc = self.rofi_c.alloc(size, alloc_type, align)?;
        // unsafe {
        //     inner_alloc.zeroize_bytes();
        // }
        // println!("new fabric alloc: {:?}", inner_alloc);
        let comm_alloc = CommAlloc {
            inner_alloc: Arc::new(CommAllocInner::RofiCAlloc(inner_alloc)),
        };

        // self.fabric_allocs.write().insert(addr,comm_alloc.clone());
        Ok(comm_alloc)
    }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn rt_alloc(&self, size: usize, align: usize) -> AllocResult<CommAlloc> {
        // add space for ref count
        let (padding, size, align) = calc_alloc_padding_size_align(size, align);

        let allocs = self.runtime_allocs.read();
        for (inner_alloc, alloc) in allocs.iter() {
            if let Some(addr) = alloc.try_malloc(size, align) {
                // trace!(
                //     "new rt alloc: {:x} {} {}",
                //     addr,
                //     addr - inner_alloc.start(),
                //     size
                // );
                let alloc = inner_alloc.rt_alloc(
                    alloc.clone(),
                    addr - inner_alloc.start(),
                    padding,
                    size,
                )?;
                let comm_alloc = CommAlloc {
                    inner_alloc: Arc::new(CommAllocInner::RofiCAlloc(alloc)),
                };
                return Ok(comm_alloc);
            }
        }
        Err(AllocError::OutOfMemoryError(size))
    }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn rt_check_alloc(&self, size: usize, align: usize) -> bool {
        // add space for ref count
        let (_padding, size, align) = calc_alloc_padding_size_align(size, align);

        let allocs = self.runtime_allocs.read();
        for (_inner_alloc, alloc) in allocs.iter() {
            if let Some(_addr) = alloc.try_malloc(size, align) {
                return true;
            }
        }
        false
    }

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
        let size = std::cmp::max(
            min_size * 2 * self.num_pes,
            HEAP_SIZE.load(Ordering::SeqCst),
        );
        if let Ok(alloc) = self.alloc(size, AllocationType::Global, 0) {
            // println!("addr: {:x} - {:x}",addr, addr+size);

            if let CommAllocInner::RofiCAlloc(inner_alloc) = alloc.inner_alloc.as_ref() {
                let mut new_alloc = BTreeAlloc::new("rofi_c_mem".to_string());
                new_alloc.init(inner_alloc.start(), size);
                self.runtime_allocs
                    .write()
                    .push((inner_alloc.clone(), new_alloc));
            } else {
                panic!("rofi_c alloc pool should only be called with RofiCAlloc addr");
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
        for (_inner_alloc, alloc) in allocs.iter() {
            println!("{:x} {:?}", alloc.start_addr, alloc.max_size,);
        }
    }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> CommAllocAddr {
        self.rofi_c
            .local_addr(remote_pe, remote_addr)
            .expect("local_addr failed")
            .into()
    }

    fn one_sided_alloc_from_remote_pe_and_addr(
        &self,
        _remote_pe: usize,
        _remote_addr: usize,
        _num_bytes: usize,
    ) -> CommAlloc {
        self.rofi_c
            .one_sided_alloc_from_remote_pe_and_addr(_remote_pe, _remote_addr, _num_bytes)
    }
    fn local_alloc_and_offset_from_remote_pe_and_addr(
        &self,
        _remote_pe: usize,
        _remote_addr: usize,
    ) -> (CommAlloc, usize) {
        self.rofi_c
            .local_alloc_and_offset_from_remote_pe_and_addr(_remote_pe, _remote_addr)
            .expect("local_alloc_and_offset_from_remote_pe_and_addr failed")
    }

    fn local_rt_alloc_from_local_addr(&self, addr: usize) -> AllocResult<CommAlloc> {
        for (inner_alloc, alloc) in self.runtime_allocs.read().iter() {
            if let Some(size) = alloc.find(addr) {
                let comm_alloc = CommAlloc {
                    inner_alloc: Arc::new(CommAllocInner::RofiCAlloc(
                        inner_alloc
                            .sub_alloc(addr - inner_alloc.start(), size)?
                            .as_rt_alloc(alloc.clone())?,
                    )),
                };
                return Ok(comm_alloc);
            }
        }
        Err(AllocError::LocalNotFound(CommAllocAddr(addr)))
    }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn remote_addr(&self, pe: usize, local_addr: usize) -> CommAllocAddr {
        self.rofi_c
            .remote_addr(pe, local_addr)
            .expect("remote_addr failed")
            .into()
    }

    //#[tracing::instrument(skip(self), level = "debug")]
    fn get_alloc_cloned(&self, addr: CommAllocAddr) -> AllocResult<CommAlloc> {
        trace!("get_alloc: {:?}", addr);
        if let Ok(inner_alloc) = self.rofi_c.get_alloc_from_start_addr(addr) {
            return Ok(CommAlloc {
                inner_alloc: Arc::new(CommAllocInner::RofiCAlloc(inner_alloc)),
            });
        }

        let allocs = self.runtime_allocs.read();
        for (inner_alloc, alloc) in allocs.iter() {
            if let Some(size) = alloc.find(addr.0) {
                return Ok(CommAlloc {
                    inner_alloc: Arc::new(CommAllocInner::RofiCAlloc(inner_alloc.sub_alloc(addr.0, size)?)),
                });
            }
        }

        Err(AllocError::LocalNotFound(addr))
    }
}

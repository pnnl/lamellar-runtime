use crate::lamellae::rofi::rofi_api::*;
use crate::lamellar_alloc::{BTreeAlloc, LamellarAlloc};
use log::trace;
use parking_lot::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

const MEM_SIZE: usize = 10 * 1024 * 1024 * 1024;

pub(crate) struct RofiComm {
    pub(crate) rofi_base_address: Arc<RwLock<usize>>,
    alloc: BTreeAlloc,
    _init: AtomicBool,
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
}

impl RofiComm {
    pub(crate) fn new() -> RofiComm {
        rofi_init().expect("error in rofi init");
        trace!("rofi initialized");
        rofi_barrier();
        let mut rofi = RofiComm {
            rofi_base_address: Arc::new(RwLock::new(rofi_alloc(MEM_SIZE) as usize)),
            alloc: BTreeAlloc::new("rofi_mem".to_string()),
            _init: AtomicBool::new(true),
            num_pes: rofi_get_size(),
            my_pe: rofi_get_id(),
        };
        trace!(
            "[{:?}] Rofi base addr 0x{:x}",
            rofi.my_pe,
            *rofi.rofi_base_address.read()
        );
        rofi.alloc.init(0, MEM_SIZE);
        rofi
    }
    pub(crate) fn finit(&self) {
        rofi_barrier();
        let _res = rofi_finit();
    }
    pub(crate) fn mype(&self) -> usize{
        self.my_pe
    }
    pub(crate) fn barrier(&self) {
        rofi_barrier();
    }
    pub(crate) fn alloc(&self, size: usize) -> Option<usize> {
        if let Some(addr) = self.alloc.try_malloc(size) {
            Some(addr)
        } else {
            None
        }
    }
    #[allow(dead_code)]
    pub(crate) fn free(&self, addr: usize) {
        self.alloc.free(addr)
    }
    #[allow(dead_code)]
    pub(crate) fn base_addr(&self) -> usize {
        *self.rofi_base_address.read()
    }

    //dst_addr relative to rofi_base_addr, need to calculate the real address
    pub(crate) fn local_store(&self, data: &[u8], dst_addr: usize) {
        let base_addr = *self.rofi_base_address.read();
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                (base_addr + dst_addr) as *mut u8,
                data.len(),
            );
        }
        //read lock dropped here
    }

    pub(crate) fn put<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    >(
        &self,
        pe: usize,
        src_addr: &[T],
        dst_addr: usize,
    ) {
        if pe != self.my_pe {
            unsafe { rofi_put(src_addr, dst_addr, pe).expect("error in rofi put") };
        } else {
            unsafe {
                // println!("[{:?}] memcopy {:?}",pe,src_addr.as_ptr());
                std::ptr::copy_nonoverlapping(
                    src_addr.as_ptr(),
                    dst_addr as *mut T,
                    src_addr.len(),
                );
            }
        }
    }

    pub(crate) fn iput<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    >(
        &self,
        pe: usize,
        src_addr: &[T],
        dst_addr: usize,
    ) {
        if pe != self.my_pe {
            rofi_iput(src_addr, dst_addr, pe).expect("error in rofi put");
        } else {
            unsafe {
                // println!("[{:?}] memcopy {:?}",pe,src_addr.as_ptr());
                std::ptr::copy_nonoverlapping(
                    src_addr.as_ptr(),
                    dst_addr as *mut T,
                    src_addr.len(),
                );
            }
        }
    }

    pub(crate) fn put_all<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    >(
        &self,
        src_addr: &[T],
        dst_addr: usize,
    ) {
        for pe in 0..self.my_pe {
            unsafe { rofi_put(src_addr, dst_addr, pe).expect("error in rofi put") };
        }
        for pe in self.my_pe + 1..self.num_pes {
            unsafe { rofi_put(src_addr, dst_addr, pe).expect("error in rofi put") };
        }
        unsafe {
            std::ptr::copy_nonoverlapping(src_addr.as_ptr(), dst_addr as *mut T, src_addr.len());
        }
    }

    pub(crate) fn get<
        T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + 'static,
    >(
        &self,
        pe: usize,
        src_addr: usize,
        dst_addr: &mut [T],
    ) {
        if pe != self.my_pe {
            unsafe {
                let ret = rofi_get(src_addr, dst_addr, pe); //.expect("error in rofi get")
                if let Err(_) = ret {
                    println!(
                        "Error in get from {:?} src {:x} base_addr{:x} size{:x}",
                        pe,
                        src_addr,
                        *self.rofi_base_address.read(),
                        dst_addr.len()
                    );
                    panic!();
                }
            };
        } else {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    src_addr as *const T,
                    dst_addr.as_mut_ptr(),
                    dst_addr.len(),
                );
            }
        }
    }
    #[allow(dead_code)]
    fn iget<
        T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + 'static,
    >(
        &self,
        pe: usize,
        src_addr: usize,
        dst_addr: &mut [T],
    ) {
        if pe != self.my_pe {
            // unsafe {
            let ret = rofi_iget(src_addr, dst_addr, pe); //.expect("error in rofi get")
            if let Err(_) = ret {
                println!(
                    "Error in get from {:?} src {:x} base_addr{:x} size{:x}",
                    pe,
                    src_addr,
                    *self.rofi_base_address.read(),
                    dst_addr.len()
                );
                panic!();
            }
        // };
        } else {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    src_addr as *const T,
                    dst_addr.as_mut_ptr(),
                    dst_addr.len(),
                );
            }
        }
    }
    //src address is relative to rofi base addr
    pub(crate) fn iget_relative<
        T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + 'static,
    >(
        &self,
        pe: usize,
        src_addr: usize,
        dst_addr: &mut [T],
    ) {
        if pe != self.my_pe {
            // unsafe {
            let ret = rofi_iget(*self.rofi_base_address.read() + src_addr, dst_addr, pe); //.expect("error in rofi get")
            if let Err(_) = ret {
                println!(
                    "[{:?}] Error in get from {:?} src_addr {:x} dst_addr {:?} base_addr {:x} size {:x}",
                    self.my_pe,
                    pe,
                    src_addr,
                    dst_addr.as_ptr(),
                    *self.rofi_base_address.read(),
                    dst_addr.len()
                );
                panic!();
            }
        // }
        // };
        } else {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    src_addr as *const T,
                    dst_addr.as_mut_ptr(),
                    dst_addr.len(),
                );
            }
        }
    }
}

impl Drop for RofiComm {
    fn drop(&mut self) {
        rofi_barrier();
        let _res = rofi_finit();
        trace!("[{:?}] dropping rofi comm", self.my_pe);
    }
}

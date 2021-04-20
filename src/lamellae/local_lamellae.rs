use crate::lamellae::{AllocationType, Backend, Lamellae, LamellaeAM, LamellaeRDMA};
use crate::lamellar_arch::LamellarArchRT;
use crate::schedulers::SchedulerQueue;
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use log::{error, trace};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) struct LocalLamellae {
    am: Arc<LocalLamellaeAM>,
    rdma: Arc<LocalLamellaeRDMA>,
}

//#[prof]
impl LocalLamellae {
    pub(crate) fn new() -> LocalLamellae {
        let am = Arc::new(LocalLamellaeAM {});

        let rdma = Arc::new(LocalLamellaeRDMA {
            allocs: Arc::new(Mutex::new(HashMap::new())),
        });
        LocalLamellae { am: am, rdma: rdma }
    }
}

//#[prof]
impl Lamellae for LocalLamellae {
    fn init_fabric(&mut self) -> (usize, usize) {
        (1, 0)
    }
    fn init_lamellae(&mut self, _scheduler: Arc<dyn SchedulerQueue>) {}
    fn finit(&self) {}
    fn barrier(&self) {}
    fn backend(&self) -> Backend {
        Backend::Local
    }
    fn get_am(&self) -> Arc<dyn LamellaeAM> {
        self.am.clone()
    }
    fn get_rdma(&self) -> Arc<dyn LamellaeRDMA> {
        self.rdma.clone()
    }
    #[allow(non_snake_case)]
    fn MB_sent(&self) -> f64 {
        0.0f64
    }
    fn print_stats(&self) {}
}

#[derive(Debug)]
pub(crate) struct LocalLamellaeAM {}

//#[prof]
impl LamellaeAM for LocalLamellaeAM {
    fn send_to_pe(&self, _pe: usize, _data: std::vec::Vec<u8>) {}
    fn send_to_all(&self, _data: std::vec::Vec<u8>) {}
    fn send_to_pes(
        &self,
        _pe: Option<usize>,
        _team: Arc<LamellarArchRT>,
        _data: std::vec::Vec<u8>,
    ) {
    }
    fn barrier(&self) {
        error!(
            "need to //#[prof]
implement an active message version of barrier"
        );
    }
    fn backend(&self) -> Backend {
        Backend::Local
    }
}

struct MyPtr {
    ptr: *mut [u8],
}
unsafe impl Sync for MyPtr {}
unsafe impl Send for MyPtr {}

pub(crate) struct LocalLamellaeRDMA {
    allocs: Arc<Mutex<HashMap<usize, MyPtr>>>,
}

//#[prof]
impl LamellaeRDMA for LocalLamellaeRDMA {
    fn put(&self, _pe: usize, src: &[u8], dst: usize) {
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), dst as *mut u8, src.len());
        }
    }
    fn iput(&self, _pe: usize, src: &[u8], dst: usize) {
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), dst as *mut u8, src.len());
        }
    }
    fn put_all(&self, src: &[u8], dst: usize) {
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), dst as *mut u8, src.len());
        }
    }
    fn get(&self, _pe: usize, src: usize, dst: &mut [u8]) {
        unsafe {
            std::ptr::copy_nonoverlapping(src as *mut u8, dst.as_mut_ptr(), dst.len());
        }
    }

    fn rt_alloc(&self, size: usize) -> Option<usize> {
        let data = vec![0u8; size].into_boxed_slice();
        let data_ptr = Box::into_raw(data);
        let data_addr = data_ptr as *const u8 as usize;
        let mut allocs = self.allocs.lock();
        allocs.insert(data_addr, MyPtr { ptr: data_ptr });
        Some(data_addr)
    }
    fn rt_free(&self, addr: usize) {
        let mut allocs = self.allocs.lock();
        if let Some(data_ptr) = allocs.remove(&addr) {
            unsafe { Box::from_raw(data_ptr.ptr) }; //it will free when dropping from scope
        }
    }
    fn alloc(&self, size: usize, _alloc: AllocationType) -> Option<usize> {
        let data = vec![0u8; size].into_boxed_slice();
        let data_ptr = Box::into_raw(data);
        let data_addr = data_ptr as *const u8 as usize;
        let mut allocs = self.allocs.lock();
        allocs.insert(data_addr, MyPtr { ptr: data_ptr });
        Some(data_addr)
    }
    fn free(&self, addr: usize) {
        let mut allocs = self.allocs.lock();
        if let Some(data_ptr) = allocs.remove(&addr) {
            unsafe { Box::from_raw(data_ptr.ptr) }; //it will free when dropping from scope
        }
    }
    fn base_addr(&self) -> usize {
        0
    }
    fn local_addr(&self, _pe: usize, remote_addr: usize) -> usize {
        remote_addr
    }
    fn remote_addr(&self, _pe: usize, local_addr: usize) -> usize {
        local_addr
    }
    fn mype(&self) -> usize {
        0
    }
}

//#[prof]
impl Drop for LocalLamellae {
    fn drop(&mut self) {
        trace!("[{:?}] RofiLamellae Dropping", 0);
    }
}

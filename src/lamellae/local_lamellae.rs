use crate::lamellae::{Backend, Lamellae, LamellaeAM, LamellaeRDMA};
use crate::lamellar_team::LamellarArch;
use log::{error, trace};
use std::sync::Arc;
use parking_lot::Mutex;
use std::collections::HashMap;


pub(crate) struct LocalLamellae {
    am: Arc<LocalLamellaeAM>,
    rdma: Arc<LocalLamellaeRDMA>,
}

impl LocalLamellae{
    pub(crate) fn new() -> LocalLamellae{
        let am = Arc::new(LocalLamellaeAM {});

        let rdma = Arc::new(LocalLamellaeRDMA {
            allocs: Arc::new(Mutex::new(HashMap::new()))
        });
        LocalLamellae{
            am: am,
            rdma: rdma,
        }
    }
}

impl Lamellae for LocalLamellae {
    fn init(&mut self) -> (usize, usize) {
        (1, 0)
    }
    fn finit(&self) { }
    fn barrier(&self) {  }
    fn backend(&self) -> Backend {
        Backend::Local
    }
    fn get_am(&self) -> Arc<dyn LamellaeAM> {
        self.am.clone()
    }
    fn get_rdma(&self) -> Arc<dyn LamellaeRDMA>{
        self.rdma.clone()
    }
    #[allow(non_snake_case)]
    fn MB_sent(&self) -> f64 {
        0.0f64
    }
    fn print_stats(&self) {}
}

pub(crate) struct LocalLamellaeAM { }

impl LamellaeAM for LocalLamellaeAM {
    fn send_to_pe(&self, _pe: usize, _data: std::vec::Vec<u8>) {
        
    }
    fn send_to_all(&self, _data: std::vec::Vec<u8>) {
       
    }
    fn send_to_pes(
        &self,
        _pe: Option<usize>,
        _team: Arc<dyn LamellarArch>,
        _data: std::vec::Vec<u8>,
    ) {
       
    }
    fn barrier(&self) {
        error!("need to implement an active message version of barrier");
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
    allocs: Arc<Mutex<HashMap<usize,MyPtr>>>,
}

impl LamellaeRDMA for LocalLamellaeRDMA{
    fn put(&self, _pe: usize, src: &[u8], dst: usize) {
        unsafe {
            std::ptr::copy_nonoverlapping(
                src.as_ptr(),
                dst as *mut u8,
                src.len(),
            );
        }
    }
    fn iput(&self, _pe: usize, src: &[u8], dst: usize) {
        unsafe {
            std::ptr::copy_nonoverlapping(
                src.as_ptr(),
                dst as *mut u8,
                src.len(),
            );
        }
    }
    fn put_all(&self, src: &[u8], dst: usize) {
        unsafe {
            std::ptr::copy_nonoverlapping(
                src.as_ptr(),
                dst as *mut u8,
                src.len(),
            );
        }
    }
    fn get(&self, _pe: usize, src: usize, dst: &mut [u8]) {
        unsafe {
            std::ptr::copy_nonoverlapping(
                src as *mut u8,
                dst.as_mut_ptr() ,
                dst.len(),
            );
        }
    }
    fn alloc(&self, size: usize) -> Option<usize> {
        let data = vec![0u8;size].into_boxed_slice();
        let data_ptr = Box::into_raw(data);
        let data_addr = data_ptr as *const u8 as usize;
        let mut allocs = self.allocs.lock();
        allocs.insert(data_addr,MyPtr{ ptr: data_ptr} );
        Some(data_addr)
    }
    fn free(&self, addr: usize) {
        let mut allocs = self.allocs.lock();
        if let Some(data_ptr) = allocs.remove(&addr){
            unsafe {Box::from_raw(data_ptr.ptr) }; //it will free when dropping from scope
        }
        // else{
        //     assert!{"error unknown address"};
        // }
    }
    fn base_addr(&self) -> usize {
        0
    }
    fn mype(&self) -> usize {
        0
    }
}

impl Drop for LocalLamellae {
    fn drop(&mut self) {
        trace!("[{:?}] RofiLamellae Dropping", 0);
    }
}

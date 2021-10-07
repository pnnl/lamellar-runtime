use crate::lamellae::{
    AllocationType, Backend, Des, Lamellae, LamellaeAM, LamellaeComm, LamellaeInit, LamellaeRDMA,
    Ser, SerializeHeader, SerializedData, SubData,SerializedDataOps,
};
use crate::lamellar_arch::LamellarArchRT;
use crate::scheduler::Scheduler;
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use log::trace;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

#[derive(Clone)]
pub(crate) struct Local {
    // am: Arc<LocalLamellaeAM>,
    // rdma: Arc<LocalLamellaeRDMA>,
    allocs: Arc<Mutex<HashMap<usize, MyPtr>>>,
}

#[derive(Clone)]
pub(crate) struct LocalData {}
impl Des for LocalData {
    fn deserialize_header(&self) -> Option<SerializeHeader> {
        panic!("should not be deserializing in local");
    }
    fn deserialize_data<T: serde::de::DeserializeOwned>(&self) -> Result<T, anyhow::Error> {
        panic!("should not be deserializing in local");
    }
    fn data_as_bytes(&self) -> &mut [u8] {
        panic!("should not be deserializing in local");
    }
    fn header_and_data_as_bytes(&self) -> &mut [u8] {
        &mut []
    }
    fn print(&self) {}
}

impl SubData for LocalData {
    fn sub_data(&self, _start: usize, _end: usize) -> SerializedData {
        SerializedData::LocalData(self.clone())
    }
}

impl SerializedDataOps for LocalData{
    fn header_as_bytes(&self) -> &mut [u8]{
        panic!("should not be deserializing in local");
    }
    fn increment_cnt(&self){ }
    fn len(&self) -> usize{
        0
    }
}

//#[prof]
impl Local {
    pub(crate) fn new() -> Local {
        Local {
            allocs: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl Ser for Local {
    async fn serialize<T: Send + Sync + serde::Serialize + ?Sized>(
        &self,
        _header: Option<SerializeHeader>,
        _obj: &T,
    ) -> Result<SerializedData, anyhow::Error> {
        panic!("should not be serializing in local");
    }
    async fn serialize_header(
        &self,
        _header: Option<SerializeHeader>,
        _serialized_size: usize,
    ) -> Result<SerializedData, anyhow::Error> {
        panic!("should not be serializing in local")
    }
}

impl LamellaeInit for Local {
    fn init_fabric(&mut self) -> (usize, usize) {
        (0, 1)
    }
    fn init_lamellae(&mut self, _scheduler: Arc<Scheduler>) -> Arc<Lamellae> {
        Arc::new(Lamellae::Local(self.clone()))
    }
}

//#[prof]
impl LamellaeComm for Local {
    fn my_pe(&self) -> usize {
        0
    }
    fn num_pes(&self) -> usize {
        1
    }
    fn barrier(&self) {}
    fn backend(&self) -> Backend {
        Backend::Local
    }
    #[allow(non_snake_case)]
    fn MB_sent(&self) -> f64 {
        0.0f64
    }
    fn print_stats(&self) {}
    fn shutdown(&self) {}
}

//#[prof]
#[async_trait]
impl LamellaeAM for Local {
    async fn send_to_pe_async(&self, _pe: usize, _data: SerializedData) {}
    async fn send_to_pes_async(
        &self,
        _pe: Option<usize>,
        _team: Arc<LamellarArchRT>,
        _data: SerializedData,
    ) {
    }
}

struct MyPtr {
    ptr: *mut [u8],
}
unsafe impl Sync for MyPtr {}
unsafe impl Send for MyPtr {}

//#[prof]
impl LamellaeRDMA for Local {
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
    //todo make this return a real value
    fn occupied(&self) -> usize {
        0
    }
}

//#[prof]
impl Drop for Local {
    fn drop(&mut self) {
        trace!("[{:?}] RofiLamellae Dropping", 0);
    }
}

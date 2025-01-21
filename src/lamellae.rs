pub(crate) mod comm;
pub(crate) mod command_queues;
#[cfg(feature = "enable-libfabric")]
pub(crate) mod libfab_lamellae;
#[cfg(feature = "enable-libfabric")]
pub(crate) mod libfabasync_lamellae;
pub(crate) mod local_lamellae;
#[cfg(feature = "rofi")]
mod rofi_lamellae;
#[cfg(feature = "enable-rofi-rust")]
pub(crate) mod rofi_rust_async_lamellae;
#[cfg(feature = "enable-rofi-rust")]
pub(crate) mod rofi_rust_lamellae;
pub(crate) mod shmem_lamellae;

use crate::{active_messaging::Msg, config, lamellar_arch::LamellarArchRT, scheduler::Scheduler};
pub(crate) use comm::*;
use local_lamellae::{Local, LocalBuilder};
#[cfg(feature = "enable-rofi")]
use rofi_lamellae::{Rofi, RofiBuilder};
use shmem_lamellae::{Shmem, ShmemBuilder};
#[cfg(feature = "enable-libfabric")]
use {
    libfab_lamellae::{LibFab, LibFabBuilder},
    libfabasync_lamellae::{LibFabAsync, LibFabAsyncBuilder},
};
#[cfg(feature = "enable-rofi-rust")]
use {
    rofi_rust_async_lamellae::{RofiRustAsync, RofiRustAsyncBuilder},
    rofi_rust_lamellae::{RofiRust, RofiRustBuilder},
};

use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

lazy_static! {
    static ref SERIALIZE_HEADER_LEN: usize =
        crate::serialized_size::<Option<SerializeHeader>>(&Some(Default::default()), false);
}

/// The list of available lamellae backends, used to specify how data is transfered between PEs
#[derive(
    serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone, Copy,
)]
pub enum Backend {
    #[cfg(feature = "rofi")]
    /// The Rofi (Rust-OFI) backend -- intended for multi process and distributed environments
    Rofi,
    #[cfg(feature = "enable-rofi-rust")]
    RofiRust,
    #[cfg(feature = "enable-rofi-rust")]
    RofiRustAsync,
    #[cfg(feature = "enable-libfabric")]
    LibFab,
    #[cfg(feature = "enable-libfabric")]
    LibFabAsync,
    /// The Local backend -- intended for single process environments
    Local,
    /// The Shmem backend -- intended for multi process environments single node environments
    Shmem,
}

#[derive(Debug, Clone)]
pub(crate) enum AllocationType {
    Local,
    Global,
    Sub(Vec<usize>),
}

impl Default for Backend {
    fn default() -> Self {
        match config().backend.as_str() {
            "rofi" => {
                #[cfg(feature = "rofi")]
                return Backend::Rofi;
                #[cfg(not(feature = "rofi"))]
                panic!("unable to set rofi backend, recompile with 'enable-rofi' feature")
            }
            "rofi_rust" => {
                #[cfg(feature = "enable-rofi-rust")]
                return Backend::RofiRust;
                #[cfg(not(feature = "enable-rofi-rust"))]
                panic!("unable to set rofi-rust backend, recompile with 'enable-rofi-rust' feature")
            }
            "rofi_rust_async" => {
                #[cfg(feature = "enable-rofi-rust")]
                return Backend::RofiRustAsync;
                #[cfg(not(feature = "enable-rofi-rust"))]
                panic!("unable to set rofi-rust backend, recompile with 'enable-rofi-rust' feature")
            }

            "libfab" => {
                #[cfg(feature = "enable-libfabric")]
                return Backend::LibFab;
                #[cfg(not(feature = "enable-libfabric"))]
                panic!("unable to set libfabric backend, recompile with 'enable-libfabric' feature")
            }
            "libfabasync" => {
                #[cfg(feature = "enable-libfabric")]
                return Backend::LibFabAsync;
                #[cfg(not(feature = "enable-libfabric"))]
                panic!("unable to set libfabric backend, recompile with 'enable-libfabric' feature")
            }
            "shmem" => {
                return Backend::Shmem;
            }
            _ => {
                return Backend::Local;
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub(crate) struct SerializeHeader {
    pub(crate) msg: Msg,
}

#[derive(Debug)]
pub(crate) struct SerializedData {
    // pub(crate) addr: usize, // process space address)
    // pub(crate) alloc_size: usize,
    // pub(crate) data: NonNull<u8>,
    // pub(crate) data_len: usize,
    // pub(crate) ser_data_addr: usize, //address allocated from Comm
    pub(crate) alloc: CommAlloc,
    pub(crate) ref_cnt: *const AtomicUsize,
    pub(crate) ser_data_bytes: CommSlice<u8>,
    pub(crate) header_bytes: CommSlice<u8>,
    pub(crate) payload_bytes: CommSlice<u8>,
    pub(crate) comm: Arc<Comm>, //Comm instead of RofiComm because I can't figure out how to make work with Enum_distpatch....
}


#[derive(Debug)]
pub(crate) struct SubSerializedData {
    pub(crate) alloc: CommAlloc,
    pub(crate) ref_cnt: *const AtomicUsize,
    pub(crate) ser_data_bytes: CommSlice<u8>,
    pub(crate) header_bytes: CommSlice<u8>,
    pub(crate) payload_bytes: CommSlice<u8>,
    pub(crate) comm: Arc<Comm>, //Comm instead of RofiComm because I can't figure out how to make work with Enum_distpatch....
}


#[derive(Debug)]
pub(crate) struct RemoteSerializedData {
    pub(crate) alloc: CommAlloc,
    pub(crate) ref_cnt: *const AtomicUsize,
    pub(crate) ser_data_bytes: CommSlice<u8>,
    pub(crate) header_bytes: CommSlice<u8>,
    pub(crate) payload_bytes: CommSlice<u8>,
    pub(crate) comm: Arc<Comm>, //Comm instead of RofiComm because I can't figure out how to make work with Enum_distpatch....
}



// we have allocated this memory out of fabric memory and thus are responsible for managing it,
// we will not move the underlying data, reallocate it, nor free it until all references are dropped
unsafe impl Send for SerializedData {} 
unsafe impl Sync for SerializedData {}

unsafe impl Send for SubSerializedData {} 
unsafe impl Sync for SubSerializedData {}

unsafe impl Send for RemoteSerializedData {} 
unsafe impl Sync for RemoteSerializedData {}

impl SerializedData {
    pub(crate) fn new(comm: Arc<Comm>, size: usize) -> Result<Self, anyhow::Error> {
        let ref_cnt_size = std::mem::size_of::<AtomicUsize>();
        let alloc_size = size + ref_cnt_size;
        let alloc = comm.rt_alloc(alloc_size, std::mem::align_of::<AtomicUsize>())?;
        let ref_cnt = alloc.addr as *const AtomicUsize;
        let mut ser_data_bytes = alloc.slice_at_offset(ref_cnt_size, size);
        let header_bytes = ser_data_bytes.sub_slice(0.. *SERIALIZE_HEADER_LEN);
        let payload_bytes = ser_data_bytes.sub_slice(*SERIALIZE_HEADER_LEN..size);
        // let ser_data_addr = addr + ref_cnt_size;
        // let raw_data_addr = ser_data_addr + *SERIALIZE_HEADER_LEN;

        Ok(SerializedData {
            // addr,
            // alloc_size,
            // data: unsafe { NonNull::new_unchecked(raw_data_addr as *mut u8) },
            // data_len: size,
            // ser_data_addr,
            alloc,
            ref_cnt,
            ser_data_bytes,
            header_bytes,
            payload_bytes,
            comm,
        })
    }

    pub(crate) unsafe fn from_raw(addr: usize) -> Self {
        let data = (addr as *const Self)
            .as_ref()
            .expect("valid serialized data");
        SerializedData {
            alloc: data.alloc.clone(),
            ref_cnt: data.ref_cnt,
            ser_data_bytes: data.ser_data_bytes,
            header_bytes: data.header_bytes,
            payload_bytes: data.payload_bytes,
            comm: data.comm.clone(),
        }
    }

    pub(crate) fn into_remote(self) -> RemoteSerializedData {
        self.increment_cnt();
        RemoteSerializedData {
            alloc: self.alloc.clone(),
            ref_cnt: self.ref_cnt,
            ser_data_bytes: self.ser_data_bytes,
            header_bytes: self.header_bytes,
            payload_bytes: self.payload_bytes,
            comm: self.comm.clone(),
        }
    }
}

// impl SerializedDataOps for SerializedData {
impl SerializedData {
    pub(crate) fn header_as_bytes(&self) -> CommSlice<u8> {
        // let header_size = *SERIALIZE_HEADER_LEN;
        // unsafe { std::slice::from_raw_parts((self.ser_data_addr) as *mut u8, header_size) }
        self.header_bytes
    }
    pub(crate) fn header_as_bytes_mut(&mut self) -> CommSlice<u8> {
        // let header_size = *SERIALIZE_HEADER_LEN;
        // unsafe { std::slice::from_raw_parts_mut((self.ser_data_addr) as *mut u8, header_size) }
        self.header_bytes
    }

    pub(crate) fn data_as_bytes(&self) ->  CommSlice<u8> {
        // unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.data_len) }
        self.payload_bytes
    }
    pub(crate) fn data_as_bytes_mut(&mut self) -> CommSlice<u8> {
        // unsafe { std::slice::from_raw_parts_mut(self.data.as_ptr(), self.data_len) }
        self.payload_bytes
    }

    pub(crate) fn header_and_data_as_bytes(&self) -> CommSlice<u8> {
        // unsafe { std::slice::from_raw_parts((self.ser_data_addr) as *mut u8, self.len()) }
        self.ser_data_bytes
    }
    pub(crate) fn header_and_data_as_bytes_mut(&mut self) ->  CommSlice<u8> {
        // unsafe {
        //     std::slice::from_raw_parts_mut(
        //         (self.addr + std::mem::size_of::<AtomicUsize>()) as *mut u8,
        //         self.len(),
        //     )
        // }
        self.ser_data_bytes
    }

    //#[tracing::instrument(skip_all)]
    pub(crate) fn increment_cnt(&self) {
        unsafe { self.ref_cnt.as_ref().expect("valid serialized data").fetch_add(1, Ordering::SeqCst) };
    }

    //#[tracing::instrument(skip_all)]
    pub(crate) fn len(&self) -> usize {
        self.alloc.size - std::mem::size_of::<AtomicUsize>()
    }

    pub(crate) fn print(&self) {
        println!(
            "addr: {:x} relative addr {:?} len {:?} data {:?} data_len {:?} alloc_size {:?}",
            self.alloc.addr,
            self.ser_data_bytes.as_ptr(),
            self.ser_data_bytes.len(),
            self.payload_bytes.as_ptr(),
            self.payload_bytes.len(),
            self.alloc.size
        );
    }
}

impl Des for SerializedData {
    fn deserialize_header(&self) -> Option<SerializeHeader> {
        crate::deserialize(&self.header_as_bytes(), false).unwrap()
    }
    fn deserialize_data<T: serde::de::DeserializeOwned>(&self) -> Result<T, anyhow::Error> {
        Ok(crate::deserialize(&self.data_as_bytes(), true)?)
    }
}

// impl SubData for SubSerializedData {
    impl SerializedData {

    // unsafe because user must ensure that multiple sub_data do not overlap if mutating the underlying data
    pub(crate)  fn sub_data(&mut self, start: usize, end: usize) -> SubSerializedData {
        // let mut sub = self.clone();
        self.increment_cnt();
        SubSerializedData {
            alloc: self.alloc.clone(),
            ref_cnt: self.ref_cnt,
            ser_data_bytes: self.ser_data_bytes,
            header_bytes: self.header_bytes,
            payload_bytes: self.payload_bytes,
            comm: self.comm.clone(),
        }
    }
}

impl Drop for SerializedData {
    fn drop(&mut self) {
        unsafe {
            if self.ref_cnt.as_ref().expect("valid serialized data").fetch_sub(1, Ordering::SeqCst) == 1 {
                self.comm.rt_free(self.alloc.clone());
            }
        }
    }
}

impl SubSerializedData{
    pub(crate) fn header_as_bytes(&self) ->  CommSlice<u8> {
        // let header_size = *SERIALIZE_HEADER_LEN;
        // unsafe { std::slice::from_raw_parts((self.ser_data_addr) as *mut u8, header_size) }
        self.header_bytes
    }
    
    pub(crate) fn data_as_bytes(&self) ->  CommSlice<u8> {
        // unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.data_len) }
        self.payload_bytes
    }
}

impl Des for SubSerializedData {
    fn deserialize_header(&self) -> Option<SerializeHeader> {
        crate::deserialize(&self.header_as_bytes(), false).unwrap()
    }
    fn deserialize_data<T: serde::de::DeserializeOwned>(&self) -> Result<T, anyhow::Error> {
        Ok(crate::deserialize(&self.data_as_bytes(), true)?)
    }
}

impl Drop for SubSerializedData {
    fn drop(&mut self) {
        unsafe {
            if self.ref_cnt.as_ref().expect("valid serialized data").fetch_sub(1, Ordering::SeqCst) == 1 {
                self.comm.rt_free(self.alloc.clone());
            }
        }
    }
}

impl RemoteSerializedData {
    pub(crate) fn increment_cnt(&self) {
        unsafe { self.ref_cnt.as_ref().expect("valid serialized data").fetch_add(1, Ordering::SeqCst) };
    }

    pub(crate)  fn len(&self) -> usize {
        self.alloc.size - std::mem::size_of::<AtomicUsize>()
    }
}

impl Clone for RemoteSerializedData {
    fn clone(&self) -> Self {
        self.increment_cnt();
        RemoteSerializedData {
            alloc: self.alloc.clone(),
            ref_cnt: self.ref_cnt,
            ser_data_bytes: self.ser_data_bytes,
            header_bytes: self.header_bytes,
            payload_bytes: self.payload_bytes,
            comm: self.comm.clone(),
        }
    }
}

impl Drop for RemoteSerializedData{
    fn drop(&mut self) {
        unsafe {
            if self.ref_cnt.as_ref().expect("valid serialized data").fetch_sub(1, Ordering::SeqCst) == 1 {
                self.comm.rt_free(self.alloc.clone());
            }
        }
    }
}


#[enum_dispatch]
pub(crate) trait Des {
    fn deserialize_header(&self) -> Option<SerializeHeader>;
    fn deserialize_data<T: serde::de::DeserializeOwned>(&self) -> Result<T, anyhow::Error>;
}

#[enum_dispatch(LamellaeInit)]
pub(crate) enum LamellaeBuilder {
    #[cfg(feature = "rofi")]
    RofiBuilder,
    #[cfg(feature = "enable-rofi-rust")]
    RofiRustBuilder,
    #[cfg(feature = "enable-rofi-rust")]
    RofiRustAsyncBuilder,
    #[cfg(feature = "enable-libfabric")]
    LibFabBuilder,
    #[cfg(feature = "enable-libfabric")]
    LibFabAsyncBuilder,
    ShmemBuilder,
    LocalBuilder,
}

#[async_trait]
#[enum_dispatch]
pub(crate) trait LamellaeInit {
    fn init_fabric(&mut self) -> (usize, usize); //(my_pe,num_pes)
    fn init_lamellae(&mut self, scheduler: Arc<Scheduler>) -> Arc<Lamellae>;
}

#[enum_dispatch]
pub(crate) trait LamellaeShutdown {
    fn shutdown(&self);
    fn force_shutdown(&self);
    fn force_deinit(&self);
}

// #[async_trait]
#[enum_dispatch]
pub(crate) trait Ser {
    fn serialize_header(
        &self,
        header: Option<SerializeHeader>,
        serialized_size: usize,
    ) -> Result<SerializedData, anyhow::Error>;
}

#[enum_dispatch(Ser, LamellaeAM, LamellaeShutdown)]
#[derive(Debug)]
pub(crate) enum Lamellae {
    #[cfg(feature = "rofi")]
    Rofi,
    #[cfg(feature = "enable-rofi-rust")]
    RofiRust,
    #[cfg(feature = "enable-rofi-rust")]
    RofiRustAsync,
    #[cfg(feature = "enable-libfabric")]
    LibFab,
    #[cfg(feature = "enable-libfabric")]
    LibFabAsync,
    Shmem,
    Local,
}

impl Lamellae {
    pub(crate) fn comm(&self) -> &Comm {
        match self {
            #[cfg(feature = "rofi")]
            Lamellae::Rofi => self.comm(),
            #[cfg(feature = "enable-rofi-rust")]
            Lamellae::RofiRust => self.comm(),
            #[cfg(feature = "enable-rofi-rust")]
            Lamellae::RofiRustAsync => self.comm(),
            #[cfg(feature = "enable-libfabric")]
            Lamellae::LibFab => self.comm(),
            #[cfg(feature = "enable-libfabric")]
            Lamellae::LibFabAsync => self.comm(),
            Lamellae::Shmem(shmem) => shmem.comm(),
            Lamellae::Local(local) => local.comm(),
        }
    }
}

// // #[async_trait]
// #[enum_dispatch]
// pub(crate) trait LamellaeComm: LamellaeAM + LamellaeRDMA {
//     fn my_pe(&self) -> usize;
//     fn num_pes(&self) -> usize;
//     fn barrier(&self);
//     fn backend(&self) -> Backend;
//     #[allow(non_snake_case)]
//     fn MB_sent(&self) -> f64;
//     // fn print_stats(&self);
// }

#[async_trait]
#[enum_dispatch]
pub(crate) trait LamellaeAM: Send {
    async fn send_to_pes_async(
        &self,
        pe: Option<usize>,
        team: Arc<LamellarArchRT>,
        data: SerializedData,
    );
}

#[allow(unused_variables)]
pub(crate) fn create_lamellae(backend: Backend, ) -> LamellaeBuilder {
    match backend {
        #[cfg(feature = "rofi")]
        Backend::Rofi => {
            let provider = config().rofi_provider.clone();
            let domain = config().rofi_domain.clone();
            LamellaeBuilder::RofiBuilder(RofiBuilder::new(&provider, &domain))
        }
        #[cfg(feature = "enable-rofi-rust")]
        Backend::RofiRust => {
            let provider = config().rofi_provider.clone();
            let domain = config().rofi_domain.clone();
            LamellaeBuilder::RofiRustBuilder(RofiRustBuilder::new(&provider, &domain))
        }
        #[cfg(feature = "enable-rofi-rust")]
        Backend::RofiRustAsync => {
            let provider = config().rofi_provider.clone();
            let domain = config().rofi_domain.clone();
            LamellaeBuilder::RofiRustAsyncBuilder(RofiRustAsyncBuilder::new(&provider, &domain))
        }
        #[cfg(feature = "enable-libfabric")]
        Backend::LibFab => {
            let provider = config().rofi_provider.clone();
            let domain = config().rofi_domain.clone();
            LamellaeBuilder::LibFabBuilder(LibFabBuilder::new(&provider, &domain))
        }
        #[cfg(feature = "enable-libfabric")]
        Backend::LibFabAsync => {
            let provider = config().rofi_provider.clone();
            let domain = config().rofi_domain.clone();
            LamellaeBuilder::LibFabAsyncBuilder(LibFabAsyncBuilder::new(&provider, &domain))
        }
        Backend::Shmem => LamellaeBuilder::ShmemBuilder(ShmemBuilder::new()),
        Backend::Local => LamellaeBuilder::LocalBuilder(LocalBuilder::new()),
    }
}

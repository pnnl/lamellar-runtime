use crate::active_messaging::Msg;
use crate::config;
#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfab_lamellae::LibFab;
#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfab_lamellae::LibFabBuilder;
#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabasync_lamellae::LibFabAsync;
#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabasync_lamellae::LibFabAsyncBuilder;
#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabric::libfabric_comm::LibFabData;
#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabric_async::libfabric_async_comm::LibFabAsyncData;
use crate::lamellar_arch::LamellarArchRT;
use crate::scheduler::Scheduler;
use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
use std::sync::Arc;
use std::pin::Pin;
pub(crate) mod comm;
pub(crate) mod command_queues;
use comm::AllocResult;
use comm::Comm;
use comm::CommOpHandle;

pub(crate) mod local_lamellae;
use local_lamellae::{Local, LocalData};
#[cfg(feature = "enable-rofi")]
mod rofi;
#[cfg(feature = "enable-rofi")]
pub(crate) mod rofi_lamellae;

#[cfg(feature = "enable-rofi")]
use rofi::rofi_comm::RofiData;
#[cfg(feature = "enable-rofi-rust")]
use rofi_rust::rofi_rust_comm::RofiRustData;
#[cfg(feature = "enable-rofi-rust")]
use rofi_rust_async::rofi_rust_async_comm::RofiRustAsyncData;

#[cfg(feature = "enable-rofi")]
use rofi_lamellae::{Rofi, RofiBuilder};
#[cfg(feature = "enable-rofi-rust")]
use rofi_rust_async_lamellae::{RofiRustAsync, RofiRustAsyncBuilder};
#[cfg(feature = "enable-rofi-rust")]
use rofi_rust_lamellae::{RofiRust, RofiRustBuilder};

#[cfg(feature = "enable-libfabric")]
pub(crate) mod libfab_lamellae;
#[cfg(feature = "enable-libfabric")]
pub(crate) mod libfabasync_lamellae;
#[cfg(feature = "enable-rofi-rust")]
pub(crate) mod rofi_rust_async_lamellae;
#[cfg(feature = "enable-rofi-rust")]
pub(crate) mod rofi_rust_lamellae;

#[cfg(feature = "enable-libfabric")]
mod libfabric;
#[cfg(feature = "enable-libfabric")]
pub(crate) mod libfabric_async;
#[cfg(feature = "enable-rofi-rust")]
mod rofi_rust;
#[cfg(feature = "enable-rofi-rust")]
mod rofi_rust_async;

pub(crate) mod shmem_lamellae;
use shmem::shmem_comm::ShmemData;
use shmem_lamellae::{Shmem, ShmemBuilder};
mod shmem;

lazy_static! {
    static ref SERIALIZE_HEADER_LEN: usize =
        crate::serialized_size::<Option<SerializeHeader>>(&Some(Default::default()), false);
}

/// The list of available lamellae backends, used to specify how data is transfered between PEs
#[derive(
    serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Clone, Copy,
)]
pub enum Backend {
    #[cfg(feature = "enable-rofi")]
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
                #[cfg(feature = "enable-rofi")]
                return Backend::Rofi;
                #[cfg(not(feature = "enable-rofi"))]
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
// fn default_backend() -> Backend {
//     match std::env::var("LAMELLAE_BACKEND") {
//         Ok(p) => match p.as_str() {
//             "rofi" => {
//                 #[cfg(feature = "enable-rofi")]
//                 return Backend::Rofi;
//                 #[cfg(not(feature = "enable-rofi"))]
//                 panic!("unable to set rofi backend, recompile with 'enable-rofi' feature")
//             }
//             "shmem" => {
//                 return Backend::Shmem;
//             }
//             _ => {
//                 return Backend::Local;
//             }
//         },
//         Err(_) => {
//             #[cfg(feature = "enable-rofi")]
//             return Backend::Rofi;
//             #[cfg(not(feature = "enable-rofi"))]
//             return Backend::Local;
//         }
//     };
// }

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub(crate) struct SerializeHeader {
    pub(crate) msg: Msg,
}

#[enum_dispatch(Des, SubData, SerializedDataOps)]
#[derive(Clone, Debug)]
pub(crate) enum SerializedData {
    #[cfg(feature = "enable-rofi")]
    RofiData,
    #[cfg(feature = "enable-rofi-rust")]
    RofiRustData,
    #[cfg(feature = "enable-rofi-rust")]
    RofiRustAsyncData,
    #[cfg(feature = "enable-libfabric")]
    LibFabData,
    #[cfg(feature = "enable-libfabric")]
    LibFabAsyncData,
    ShmemData,
    LocalData,
}

#[enum_dispatch]
pub(crate) trait SerializedDataOps {
    fn header_as_bytes(&self) -> &mut [u8];
    fn increment_cnt(&self);
    fn len(&self) -> usize;
}

#[enum_dispatch]
pub(crate) trait Des {
    fn deserialize_header(&self) -> Option<SerializeHeader>;
    fn deserialize_data<T: serde::de::DeserializeOwned>(&self) -> Result<T, anyhow::Error>;
    fn header_and_data_as_bytes(&self) -> &mut [u8];
    fn data_as_bytes(&self) -> &mut [u8];
    fn print(&self);
}

#[enum_dispatch]
pub(crate) trait SubData {
    fn sub_data(&self, start: usize, end: usize) -> SerializedData;
}

#[enum_dispatch(LamellaeInit)]
pub(crate) enum LamellaeBuilder {
    #[cfg(feature = "enable-rofi")]
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
    Local,
}

#[async_trait]
#[enum_dispatch]
pub(crate) trait LamellaeInit {
    fn init_fabric(&mut self) -> (usize, usize); //(my_pe,num_pes)
    fn init_lamellae(&mut self, scheduler: Arc<Scheduler>) -> Arc<Lamellae>;
}

// #[async_trait]
#[enum_dispatch]
pub(crate) trait Ser {
    // fn serialize<T: serde::Serialize + ?Sized>(
    //     &self,
    //     header: Option<SerializeHeader>,
    //     obj: &T,
    // ) -> Result<SerializedData, anyhow::Error>;
    fn serialize_header(
        &self,
        header: Option<SerializeHeader>,
        serialized_size: usize,
    ) -> Result<SerializedData, anyhow::Error>;
}

#[enum_dispatch(LamellaeComm, LamellaeAM, LamellaeRDMA, Ser)]
#[derive(Debug)]
pub(crate) enum Lamellae {
    #[cfg(feature = "enable-rofi")]
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

// #[async_trait]
#[enum_dispatch]
pub(crate) trait LamellaeComm: LamellaeAM + LamellaeRDMA {
    // this is a global barrier (hopefully using hardware)
    fn my_pe(&self) -> usize;
    fn num_pes(&self) -> usize;

    #[must_use="You must .block() or .await the returned handle in order for this function to execute"]
    fn barrier<'a>(&'a self) -> CommOpHandle<'a>;
    fn backend(&self) -> Backend;
    #[allow(non_snake_case)]
    fn MB_sent(&self) -> f64;
    // fn print_stats(&self);
    fn shutdown(&self);
    fn force_shutdown(&self);
    fn force_deinit(&self);
}

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
#[enum_dispatch]
pub(crate) trait LamellaeRDMA: Send + Sync {
    fn flush(&self);
    fn put(&self, pe: usize, src: &[u8], dst: usize);
    #[must_use="You must .block() or .await the returned handle in order for this function to execute"]
    fn iput<'a>(&'a self, pe: usize, src: &'a [u8], dst: usize) -> CommOpHandle<'a>;
    fn put_all(&self, src: &[u8], dst: usize);
    fn get(&self, pe: usize, src: usize, dst: &mut [u8]);
    #[must_use="You must .block() or .await the returned handle in order for this function to execute"]
    fn iget<'a>(&'a self, pe: usize, src: usize, dst: &'a mut [u8]) -> CommOpHandle<'a>;
    fn rt_alloc(&self, size: usize, align: usize) -> AllocResult<usize>;
    // fn rt_check_alloc(&self, size: usize, align: usize) -> bool;
    fn rt_free(&self, addr: usize);
    #[must_use="You must .block() or .await the returned handle in order for this function to execute"]
    fn alloc<'a>(&'a self, size: usize, alloc: AllocationType, align: usize) -> CommOpHandle<'a, AllocResult<usize>>;
    fn free(&self, addr: usize);
    fn base_addr(&self) -> usize;
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize;
    fn remote_addr(&self, remote_pe: usize, local_addr: usize) -> usize;
    // fn occupied(&self) -> usize;
    // fn num_pool_allocs(&self) -> usize;
    fn alloc_pool(&self, min_size: usize);
}

#[allow(unused_variables)]

pub(crate) fn create_lamellae(backend: Backend) -> LamellaeBuilder {
    match backend {
        #[cfg(feature = "enable-rofi")]
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
        Backend::Local => LamellaeBuilder::Local(Local::new()),
    }
}


// #[pin_project(project = StateProj)]
// enum State<'a> {
//     Init,
//     TryingPut(#[pin] Pin<Box<dyn Future<Output =()> + Send + 'a> >),
// }

// // #[pin_project]
// pub(crate) struct CommPutHandle<'a> {
//     fut: Pin<Box<dyn Future<Output =()> + Send + 'a> >
//     // pe: usize, 
//     // src: &'a [u8],
//     // dst: usize,
//     // comm: Option<Arc<Comm>>,
//     // #[pin]
//     // state: State<'a>,
//     // put_future: Pin<Box<dyn Future<Output = ()>>>,
// }

// impl<'a> CommPutHandle<'a> {
//     pub(crate) fn new(fut: impl Future<Output =()> + Send + 'a) -> Self {
//         Self {
//             fut: Box::pin(fut)
//         }
//     }
//     // pub(crate) fn new(comm: Arc<Comm>, pe: usize, src: &'a [u8], dst: usize) -> Self {
//     //     Self{
//     //         pe,
//     //         src,
//     //         dst,
//     //         state: State::Init,
//     //         comm: Some(comm),
//     //     }
//     // }
    
//     // pub(crate) fn no_comm(pe: usize, src: &'a [u8], dst: usize) -> Self {
//     //     Self{
//     //         pe,
//     //         src,
//     //         dst,
//     //         state: State::Init,
//     //         comm: None,
//     //     }
//     // }

//     pub(crate) fn block(self) {
//         // match self.comm {
//             // Some(comm) => {
//                 async_std::task::block_on(async {self.fut.await});
//         //     }
//         //     None => {}
//         // }
//     }
// }

// impl<'a> Future for CommPutHandle<'a> {
//         type Output = ();
//     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         let mut this = self.get_mut();
//         let _guard = ready!(this.fut.as_mut().poll(cx));
//         Poll::Ready(())
//     }
// }
// impl<'a> Future for CommPutHandle<'a> {
//     type Output = ();
//     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         // let comm = self.comm.clone();
//         let mut this = self.project();
//         match this.comm {
//             None => return Poll::Ready(()),
//             Some(comm) => {

//                 match this.state.as_mut().project() {
//                     StateProj::Init => {
//                         let fut = Box::pin(comm.iput(*this.pe, this.src, *this.dst));
//                         *this.state = State::TryingPut(fut);
//                         cx.waker().wake_by_ref();
//                         Poll::Pending
//                     }
//                     StateProj::TryingPut(fut) => {
//                         let _guard = ready!(fut.poll(cx));
//                         Poll::Ready(())
//                     }
//                     _ => unreachable!(),
//                 }
//             }
//         }

//     }
// }
#[cfg(feature = "enable-rofi")]
use crate::lamellae::rofi::rofi_comm::*;
use crate::lamellae::shmem::shmem_comm::*;
use crate::lamellae::{AllocationType, SerializedData};
// use crate::lamellae::shmem::ShmemComm;

use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
use std::sync::Arc;

pub(crate) trait Remote:
    serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + Send + Sync
{
}
impl<T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + Send + Sync>
    Remote for T
{
}

#[enum_dispatch(CommOps)]
pub(crate) enum Comm {
    #[cfg(feature = "enable-rofi")]
    Rofi(RofiComm),
    Shmem(ShmemComm),
}

impl Comm {
    pub(crate) async fn new_serialized_data(self: &Arc<Comm>, size: usize) -> SerializedData {
        match self.as_ref() {
            #[cfg(feature = "enable-rofi")]
            Comm::Rofi(_) => RofiData::new(self.clone(), size).await.into(),
            Comm::Shmem(_) => ShmemData::new(self.clone(), size).await.into(),
        }
    }
}

#[async_trait]
#[enum_dispatch]
pub(crate) trait CommOps {
    fn my_pe(&self) -> usize;
    fn num_pes(&self) -> usize;
    fn barrier(&self);
    fn occupied(&self) -> usize;
    fn rt_alloc(&self, size: usize) -> Option<usize>;
    fn rt_free(&self, addr: usize);
    fn alloc(&self, size: usize, alloc: AllocationType) -> Option<usize>;
    fn free(&self, addr: usize);
    fn base_addr(&self) -> usize;
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize;
    fn remote_addr(&self, pe: usize, local_addr: usize) -> usize;
    fn put<T: Remote + 'static>(&self, pe: usize, src_addr: &[T], dst_addr: usize);
    fn iput<T: Remote + 'static>(&self, pe: usize, src_addr: &[T], dst_addr: usize);
    fn put_all<T: Remote + 'static>(&self, src_addr: &[T], dst_addr: usize);
    fn get<T: Remote + 'static>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]);
    fn iget<T: Remote + 'static>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]);
    fn iget_relative<T: Remote + 'static>(&self, pe: usize, src_addr: usize, dst_addr: &mut [T]);
}

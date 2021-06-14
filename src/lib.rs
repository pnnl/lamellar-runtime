#![cfg_attr(feature = "nightly", feature(unboxed_closures))]

#[cfg(feature = "nightly")]
#[macro_use]
extern crate serde_closure;
#[cfg(feature = "nightly")]
pub use serde_closure::FnOnce;

#[macro_use]
extern crate lazy_static;



mod active_messaging;
mod lamellae;
mod lamellar_alloc;
mod lamellar_arch;
#[cfg(feature = "experimental")]
mod lamellar_array;
#[cfg(feature = "experimental")]
mod lamellar_darc;

mod barrier;

mod lamellar_memregion;
mod lamellar_request;
mod lamellar_team;
mod lamellar_world;
mod scheduler;
mod utils;
pub use utils::*;

#[doc(hidden)]
use lamellar_prof::init_prof;
init_prof!();

#[doc(hidden)]
pub use crate::active_messaging::{registered_active_message::RegisteredAm, LamellarReturn,LamellarActiveMessage,RemoteActiveMessage,DarcSerde,LamellarSerde,LamellarResultSerde,Serde};
#[cfg(feature = "nightly")]
pub use crate::active_messaging::remote_closures::RemoteClosures;
pub use crate::active_messaging::{ActiveMessaging, LamellarAM,LocalAM};

#[cfg(feature = "experimental")]
pub use crate::lamellar_array::{LamellarArray, ReduceKey};
pub use crate::lamellar_memregion::{
    LamellarLocalMemoryRegion, LamellarMemoryRegion, RegisteredMemoryRegion, RemoteMemoryRegion,
};

#[cfg(feature = "experimental")]
pub use crate::lamellar_darc::{Darc,LocalRwDarc};
#[cfg(feature = "experimental")]
#[doc(hidden)]
pub use crate::lamellar_darc::{darc_serialize,darc_from_ndarc,localrw_serialize,localrw_from_ndarc};

// #[doc(hidden)]
// pub use crate::lamellae::{Lamellae,SerializedData,SerializeHeader};
pub use crate::lamellae::Backend;
pub use crate::scheduler::SchedulerType;
pub use crate::lamellar_world::*;
pub use crate::lamellar_arch::{BlockedArch, IdError, LamellarArch, StridedArch};

#[doc(hidden)]
pub use crate::lamellar_team::LamellarTeamRT;

pub use crate::lamellar_team::LamellarTeam;



extern crate lamellar_impl;
pub use lamellar_impl::{am, local_am, generate_reductions_for_type,  register_reduction,AmData,AmLocalData};

#[doc(hidden)]
pub use inventory;

#[doc(hidden)]
pub use bincode;

#[doc(hidden)]
pub fn serialize<T: ?Sized>(obj: &T) -> Result<Vec<u8>, anyhow::Error>
where
    T: serde::Serialize,
{
    // let mut buf = Vec::new();
    // obj.serialize(&mut rmp_serde::Serializer::new(&mut buf)).unwrap()
    Ok(bincode::serialize(obj)?)
    // Ok(postcard::to_stdvec(obj)?)
    // Ok(rmp_serde::to_vec(obj)?)
}

#[doc(hidden)]
pub fn serialized_size<T: ?Sized>(obj: &T) -> usize 
where
    T: serde::Serialize,{
    bincode::serialized_size(obj).unwrap() as usize
}
#[doc(hidden)]
pub fn serialize_into<T: ?Sized>(buf: &mut [u8], obj: &T) ->Result<(), anyhow::Error>
where
    T: serde::Serialize,
    {
        bincode::serialize_into(buf,obj)?;
        Ok(())
    }

#[doc(hidden)]
pub fn deserialize<'a, T>(bytes: &'a [u8]) -> Result<T, anyhow::Error>
where
    T: serde::Deserialize<'a>,
{
    Ok(bincode::deserialize(bytes)?)
    // Ok(postcard::from_bytes(bytes)?)
    // Ok(rmp_serde::from_read_ref(bytes)?)
}
#[doc(hidden)]
pub use async_std;

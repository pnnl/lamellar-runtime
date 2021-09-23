#![cfg_attr(feature = "nightly", feature(unboxed_closures))]

#[cfg(feature = "nightly")]
#[macro_use]
extern crate serde_closure;
#[cfg(feature = "nightly")]
pub use serde_closure::FnOnce;

#[macro_use]
extern crate lazy_static;

#[cfg(feature = "enable-rofi")]
#[macro_use]
extern crate memoffset;

// #[macro_use]
pub extern crate serde;
pub use serde::*;

// pub use serde::{Deserialize, Serialize};

mod active_messaging;
pub mod array;
//#[cfg(feature = "experimental")]
mod darc;
mod lamellae;
mod lamellar_alloc;
mod lamellar_arch;
mod memregion;

mod barrier;

// mod lamellar_memregion;
mod lamellar_request;
mod lamellar_team;
mod lamellar_world;
mod scheduler;
mod utils;
pub use utils::*;

#[doc(hidden)]
use lamellar_prof::init_prof;
init_prof!();

#[cfg(feature = "nightly")]
pub use crate::active_messaging::remote_closures::RemoteClosures;
#[doc(hidden)]
pub use crate::active_messaging::{
    registered_active_message::RegisteredAm, DarcSerde, LamellarActiveMessage, LamellarResultSerde,
    LamellarReturn, LamellarSerde, RemoteActiveMessage, Serde,
};
pub use crate::active_messaging::{ActiveMessaging, LamellarAM, LocalAM};
pub use crate::lamellar_request::LamellarRequest;

// //#[cfg(feature = "experimental")]
pub use crate::array::{LamellarArray, ReduceKey};//,AddKey};
pub use crate::memregion::{
    local::LocalMemoryRegion, shared::SharedMemoryRegion, RemoteMemoryRegion,
};

//#[cfg(feature = "experimental")]
#[doc(hidden)]
pub use crate::darc::global_rw_darc::{globalrw_from_ndarc, globalrw_serialize};
//#[cfg(feature = "experimental")]
#[doc(hidden)]
// pub use crate::darc::{darc_from_ndarc, darc_serialize};
//#[cfg(feature = "experimental")]
pub use crate::darc::local_rw_darc::LocalRwDarc;
//#[cfg(feature = "experimental")]
#[doc(hidden)]
pub use crate::darc::local_rw_darc::{localrw_from_ndarc, localrw_serialize};
//#[cfg(feature = "experimental")]
pub use crate::darc::Darc;

//#[cfg(feature = "experimental")]
pub use crate::darc::global_rw_darc::GlobalRwDarc;

// #[doc(hidden)]
// pub use crate::lamellae::{Lamellae,SerializedData,SerializeHeader};
pub use crate::lamellae::Backend;
pub use crate::lamellar_arch::{BlockedArch, IdError, LamellarArch, StridedArch};
pub use crate::lamellar_world::*;
pub use crate::scheduler::SchedulerType;

#[doc(hidden)]
pub use crate::lamellar_team::LamellarTeamRT;

pub use crate::lamellar_team::LamellarTeam;

extern crate lamellar_impl;
pub use lamellar_impl::{
    am, generate_reductions_for_type, local_am, register_reduction, AmData, AmLocalData,
    DarcSerdeRT,
};

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
    T: serde::Serialize,
{
    bincode::serialized_size(obj).unwrap() as usize
}
#[doc(hidden)]
pub fn serialize_into<T: ?Sized>(buf: &mut [u8], obj: &T) -> Result<(), anyhow::Error>
where
    T: serde::Serialize,
{
    bincode::serialize_into(buf, obj)?;
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

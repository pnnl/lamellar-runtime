#![cfg_attr(feature = "nightly", feature(unboxed_closures))]

#[cfg(feature = "nightly")]
#[macro_use]
extern crate serde_closure;
#[cfg(feature = "nightly")]
pub use serde_closure::FnOnce;

#[macro_use]
extern crate lazy_static;

// #[macro_use]
// extern crate crossbeam;

mod active_messaging;
mod lamellae;
mod lamellar_alloc;
mod lamellar_arch;
#[cfg(feature = "experimental")]
mod lamellar_array;

mod lamellar_memregion;
mod lamellar_request;
mod lamellar_team;
mod lamellar_world;
mod schedulers;
mod utils;
pub use utils::*;

#[doc(hidden)]
use lamellar_prof::init_prof;
init_prof!();

#[doc(hidden)]
pub use crate::active_messaging::{registered_active_message::RegisteredAm, LamellarReturn};

#[doc(hidden)]
pub use crate::active_messaging::LamellarActiveMessage;

#[cfg(feature = "nightly")]
pub use crate::active_messaging::remote_closures::RemoteClosures;
pub use crate::active_messaging::{ActiveMessaging, LamellarAM};

#[cfg(feature = "experimental")]
pub use crate::lamellar_array::{LamellarArray, ReduceKey};
pub use crate::lamellar_memregion::{
    LamellarLocalMemoryRegion, LamellarMemoryRegion, RegisteredMemoryRegion, RemoteMemoryRegion,
};

pub use crate::lamellae::Backend;
pub use crate::schedulers::SchedulerType;

pub use crate::lamellar_world::*;

pub use crate::lamellar_arch::{BlockedArch, IdError, LamellarArch, StridedArch};

#[doc(hidden)]
pub use crate::lamellar_team::LamellarTeamRT;

pub use crate::lamellar_team::LamellarTeam;

extern crate lamellar_impl;
pub use lamellar_impl::{am, generate_reductions_for_type, reduction, register_reduction};

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

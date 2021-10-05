// #![cfg_attr(feature = "nightly", feature(unboxed_closures))]

// #[cfg(feature = "nightly")]
// #[macro_use]
// extern crate serde_closure;
// #[cfg(feature = "nightly")]
// pub use serde_closure::FnOnce;

#[macro_use]
extern crate lazy_static;

#[cfg(feature = "enable-rofi")]
#[macro_use]
extern crate memoffset;

// #[macro_use]
pub extern crate serde;
pub use serde::*;

mod active_messaging;
pub mod array;
mod barrier;
mod darc;
mod lamellae;
mod lamellar_alloc;
mod lamellar_arch;
mod lamellar_request;
mod lamellar_team;
mod lamellar_world;
mod memregion;
mod scheduler;
mod utils;
pub use utils::*;

#[doc(hidden)]
use lamellar_prof::init_prof;
init_prof!();

// #[cfg(feature = "nightly")]
// pub use crate::active_messaging::remote_closures::RemoteClosures;

#[doc(hidden)]
pub use crate::active_messaging::{
    registered_active_message::RegisteredAm, DarcSerde, LamellarActiveMessage, LamellarResultSerde,
    LamellarReturn, LamellarSerde, RemoteActiveMessage, Serde,
};
pub use crate::active_messaging::{ActiveMessaging, LamellarAM, LocalAM};
pub use crate::lamellar_request::LamellarRequest;

pub use crate::array::{LamellarArray, ReduceKey};
pub use crate::memregion::{
    local::LocalMemoryRegion, shared::SharedMemoryRegion, LamellarMemoryRegion, RemoteMemoryRegion,
};

pub use crate::darc::global_rw_darc::GlobalRwDarc;
#[doc(hidden)]
pub use crate::darc::global_rw_darc::{globalrw_from_ndarc, globalrw_serialize};
pub use crate::darc::local_rw_darc::LocalRwDarc;
#[doc(hidden)]
pub use crate::darc::local_rw_darc::{localrw_from_ndarc, localrw_serialize};
pub use crate::darc::Darc;

pub use crate::lamellae::Backend;
pub use crate::lamellar_arch::{BlockedArch, IdError, LamellarArch, StridedArch};
pub use crate::lamellar_world::*;
pub use crate::scheduler::SchedulerType;

pub use crate::lamellar_team::LamellarTeam;
#[doc(hidden)]
pub use crate::lamellar_team::LamellarTeamRT;

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
    Ok(bincode::serialize(obj)?)
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
}
#[doc(hidden)]
pub use async_std;

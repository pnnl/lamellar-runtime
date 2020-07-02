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
mod lamellar_memregion;
mod lamellar_request;
mod lamellar_team;
mod lamellar_world;
mod schedulers;
mod utils;
pub use utils::*;

pub use crate::active_messaging::{
    registered_active_message::RegisteredAm, ActiveMessaging, LamellarAM, LamellarActiveMessage,
    LamellarReturn,
};

pub use crate::lamellar_memregion::{RemoteMemoryRegion,LamellarMemoryRegion};

#[cfg(feature = "nightly")]
pub use crate::active_messaging::remote_closures::RemoteClosures;

pub use crate::lamellae::Backend;
pub use crate::schedulers::SchedulerType;

pub use crate::lamellar_world::*;


extern crate lamellar_impl;
pub use lamellar_impl::am;

#[doc(hidden)]
pub use inventory;

#[doc(hidden)] 
pub use bincode;


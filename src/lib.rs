#![feature(unboxed_closures)]
#![feature(vec_into_raw_parts)]

#[macro_use]
extern crate serde_closure;

pub use serde_closure::FnOnce;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate crossbeam;

mod core;
mod lamellae;
mod lamellar_alloc;
mod lamellar_memregion;
pub use lamellar_memregion::LamellarMemoryRegion;
mod lamellar_request;
#[cfg(feature = "RofiBackend")]
mod rofi_api;
mod runtime;
mod schedulers;
mod utils;
pub use crate::core::*;

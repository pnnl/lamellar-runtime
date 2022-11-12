//! Lamellar is an investigation of the applicability of the Rust systems programming language for HPC as an alternative to C and C++, with a focus on PGAS approaches.
//!
//! Lamellar provides several different communication patterns to distributed applications.
//! First, Lamellar allows for sending and executing user defined active messages on remote nodes in a distributed environments.
//! User first implement runtime exported trait (LamellarAM) for their data structures and then call a procedural macro (#\[lamellar::am\]) on the implementation.
//! The procedural macro procudes all the nescessary code to enable remote execution of the active message.
//!
//! Lamellar also provides PGAS capabilities through multiple interfaces.
//! The first is a low level interface for constructing memory regions which are readable and writable from remote pes (nodes).
//!
//! The second is a high-level abstraction of distributed arrays, allowing for distributed iteration and data parallel processing of elements.
//!
//! Lamellar relies on network providers called Lamellae to perform the transfer of data throughout the system.
//! Currently three such Lamellae exist, one used for single node (single process) development ("local"), , one used for single node (multi-process) development ("shmem") useful for emulating distributed environments,and another based on the Rust OpenFabrics Interface Transport Layer (ROFI) (<https://github.com/pnnl/rofi>).
//!
//! EXAMPLES
//! --------
//!
//! # Selecting a Lamellae and constructing a lamellar world instance
//! ```
//! use lamellar::Backend;
//! fn main(){
//!  let mut world = lamellar::LamellarWorldBuilder::new()
//!         .with_lamellae( Default::default() ) //if "enable-rofi" feature is active default is rofi, otherwise  default is Shmem
//!         //.with_lamellae( Backend::Rofi ) //explicity set the lamellae backend to rofi, using the provider specified by the LAMELLAR_ROFI_PROVIDER env var ("verbs" or "shm")
//!         //.with_lamellae( Backend::RofiVerbs ) //explicity set the lamellae backend to rofi, specifying the verbs provider
//!         //.with_lamellae( Backend::Shmem ) //explicity set the lamellae backend to rofi, specifying the shm provider
//!         .build();
//! }
//! ```
//!
//! # Creating and executing a Registered Active Message
//! ```
//! use lamellar::ActiveMessaging;
//!
//! #[lamellar::AmData(Debug, Clone)]
//! struct HelloWorld { //the "input data" we are sending with our active message
//!     my_pe: usize, // "pe" is processing element == a node
//! }
//!
//! #[lamellar::am]
//! impl LamellarAM for HelloWorld {
//!     async fn exec(&self) {
//!         println!(
//!             "Hello pe {:?} of {:?}, I'm pe {:?}",
//!             lamellar::current_pe,
//!             lamellar::num_pes,
//!             self.my_pe
//!         );
//!     }
//! }
//!
//! fn main(){
//!     let mut world = lamellar::LamellarWorldBuilder::new().build();
//!     let my_pe = world.my_pe();
//!     let num_pes = world.num_pes();
//!     let am = HelloWorld { my_pe: my_pe };
//!     for pe in 0..num_pes{
//!         world.exec_am_pe(pe,am.clone()); // explicitly launch on each PE
//!     }
//!     world.wait_all(); // wait for all active messages to finish
//!     world.barrier();  // synchronize with other pes
//!     let handle = world.exec_all(am.clone()); //also possible to execute on every PE with a single call
//!     handle.get(); //both exec_all and exec_am_pe return request handles that can be used to access any returned result
//! }
//! ```
//!
//! # Creating, initializing, and iterating through a distributed array
//! ```
//! use lamellar::array::{DistributedIterator,DistributedIterator, Distribution, OneSidedIterator, UnsafeArray};
//!
//! fn main(){
//!     let world = lamellar::LamellarWorldBuilder::new().build();
//!     let my_pe = world.my_pe();
//!     let block_array = UnsafeArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Block); //we also support Cyclic distribution.
//!     block_array.dist_iter_mut().enumerate().for_each(move |elem| *elem = my_pe); //simultaneosuly initialize array accross all pes, each pe only updates its local data
//!     block_array.wait_all();
//!     block_array.barrier();
//!     if my_pe == 0{
//!         for (i,elem) in block_array.onesided_iter().into_iter().enumerate(){ //iterate through entire array on pe 0 (automatically transfering remote data)
//!             println!("i: {} = {})",i,elem);
//!         }
//!     }
//! }
//! ```

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate memoffset;
#[doc(hidden)]
pub extern crate serde;
#[doc(hidden)]
pub use serde::*;

#[doc(hidden)]
pub extern crate tracing;
#[doc(hidden)]
pub use tracing::*;

pub mod active_messaging;
pub use active_messaging::prelude::*;
pub mod array;
pub use array::prelude::*;
mod barrier;
pub mod darc;
pub use darc::prelude::*;
mod lamellae;
mod lamellar_alloc;
mod lamellar_arch;
mod lamellar_request;
mod lamellar_task_group;
mod lamellar_team;
mod lamellar_world;
pub mod memregion;
pub use memregion::prelude::*;
mod scheduler;
mod utils;
pub use utils::*;

#[doc(hidden)]
use lamellar_prof::init_prof;
init_prof!();

pub use crate::lamellar_request::LamellarRequest;

pub use crate::lamellar_arch::{BlockedArch, IdError, LamellarArch, StridedArch};
pub use crate::lamellae::Backend;
pub use crate::scheduler::SchedulerType;
pub use crate::lamellar_team::LamellarTeam;
#[doc(hidden)]
pub use crate::lamellar_team::LamellarTeamRT;
pub use crate::lamellar_task_group::LamellarTaskGroup;
pub use crate::lamellar_world::*;


extern crate lamellar_impl;
pub use lamellar_impl::{
    am, generate_ops_for_type, generate_reductions_for_type, local_am, register_reduction, AmData,
    AmLocalData, ArithmeticOps, Dist,
};

#[doc(hidden)]
pub use inventory;

#[doc(hidden)]
pub use bincode;
use bincode::Options;

// #[macro_use]
// pub extern crate custom_derive;
#[doc(hidden)]
pub use custom_derive;

// #[macro_use]
// pub extern crate newtype_derive;
#[doc(hidden)]
pub use newtype_derive;

lazy_static! {
    pub(crate) static ref BINCODE: bincode::config::WithOtherTrailing<bincode::DefaultOptions, bincode::config::AllowTrailing> =
        bincode::DefaultOptions::new().allow_trailing_bytes();
}

#[doc(hidden)]
pub fn serialize<T: ?Sized>(obj: &T, var: bool) -> Result<Vec<u8>, anyhow::Error>
where
    T: serde::Serialize,
{
    if var {
        // Ok(BINCODE.serialize(obj)?)
        Ok(bincode::serialize(obj)?)
    } else {
        Ok(bincode::serialize(obj)?)
    }
}

#[doc(hidden)]
pub fn serialized_size<T: ?Sized>(obj: &T, var: bool) -> usize
where
    T: serde::Serialize,
{
    if var {
        // BINCODE.serialized_size(obj).unwrap() as usize
        bincode::serialized_size(obj).unwrap() as usize
    } else {
        bincode::serialized_size(obj).unwrap() as usize
    }
}
#[doc(hidden)]
pub fn serialize_into<T: ?Sized>(buf: &mut [u8], obj: &T, var: bool) -> Result<(), anyhow::Error>
where
    T: serde::Serialize,
{
    if var {
        // BINCODE.serialize_into(buf, obj)?;
        bincode::serialize_into(buf, obj)?;
    } else {
        bincode::serialize_into(buf, obj)?;
    }
    Ok(())
}

#[doc(hidden)]
pub fn deserialize<'a, T>(bytes: &'a [u8], var: bool) -> Result<T, anyhow::Error>
where
    T: serde::Deserialize<'a>,
{
    if var {
        // Ok(BINCODE.deserialize(bytes)?)
        Ok(bincode::deserialize(bytes)?)
    } else {
        Ok(bincode::deserialize(bytes)?)
    }
}
#[doc(hidden)]
pub use async_std;

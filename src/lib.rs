#![warn(missing_docs)]
#![warn(unreachable_pub)]
#![doc(test(attr(deny(unused_must_use))))]
//! Lamellar is an investigation of the applicability of the Rust systems programming language for HPC as an alternative to C and C++, with a focus on PGAS approaches.
//!
//! # Some Nomenclature
//! Throughout this documentation and APIs there are a few terms we end up reusing a lot, those terms and brief descriptions are provided below:
//! - `PE` - a processing element, typically a multi threaded process, for those familiar with MPI, it corresponds to a Rank.
//!     - Commonly you will create 1 PE per psychical CPU socket on your system, but it is just as valid to have multiple PE's per CPU
//!     - There may be some instances where `Node` (meaning a compute node) is used instead of `PE` in these cases they are interchangeable
//! - `World` - an abstraction representing your distributed computing system
//!     - consists of N PEs all capable of communicating with one another
//! - `Team` - A subset of the PEs that exist in the world
//! - `AM` - short for [Active Message][crate::active_messaging]
//! - `Collective Operation` - Generally means that all PEs (associated with a given distributed object) must explicitly participate in the operation, otherwise deadlock will occur.
//!     - e.g. barriers, construction of new distributed objects
//! - `One-sided Operation` - Generally means that only the calling PE is required for the operation to successfully complete.
//!     - e.g. accessing local data, waiting for local work to complete
//!
//! # Features
//!
//! Lamellar provides several different communication patterns and programming models to distributed applications, briefly highlighted below
//! ## Active Messages
//! Lamellar allows for sending and executing user defined active messages on remote PEs in a distributed environment.
//! User first implement runtime exported trait (LamellarAM) for their data structures and then call a procedural macro [#\[lamellar::am\]][crate::active_messaging::am] on the implementation.
//! The procedural macro produces all the necessary code to enable remote execution of the active message.
//! More details can be found in the [Active Messaging][crate::active_messaging] module documentation.
//!
//! ## Darcs (Distributed Arcs)
//! Lamellar provides a distributed extension of an [`Arc`][std::sync::Arc] called a [Darc][crate::darc].
//! Darcs provide safe shared access to inner objects in a distributed environment, ensuring lifetimes and read/write accesses are enforced properly.
//! More details can be found in the [Darc][crate::darc] module documentation.
//!
//! ## PGAS abstractions
//!
//! Lamellar also provides PGAS capabilities through multiple interfaces.
//!
//! ### LamellarArrays (Distributed Arrays)
//!
//! The first is a high-level abstraction of distributed arrays, allowing for distributed iteration and data parallel processing of elements.
//! More details can be found in the [LamellarArray][crate::array] module documentation.
//!
//! ### Low-level Memory Regions
//!
//! The second is a low level (unsafe) interface for constructing memory regions which are readable and writable from remote PEs.
//! Note that unless you are very comfortable/confident in low level distributed memory (and even then) it is highly recommended you use the LamellarArrays interface
//! More details can be found in the [Memory Region][crate::memregion] module documentation.
//!
//! # Network Backends
//!
//! Lamellar relies on network providers called Lamellae to perform the transfer of data throughout the system.
//! Currently three such Lamellae exist:
//! - `local` -  used for single-PE (single system, single process) development (this is the default),
//! - `shmem` -  used for multi-PE (single system, multi-process) development, useful for emulating distributed environments (communicates through shared memory)
//! - `rofi` - used for multi-PE (multi system, multi-process) distributed development, based on the Rust OpenFabrics Interface Transport Layer (ROFI) (<https://github.com/pnnl/rofi>).
//!     - By default support for Rofi is disabled as using it relies on both the Rofi C-library and the libfabrics library, which may not be installed on your system.
//!     - It can be enabled by adding ```features = ["enable-rofi"] or `features = ["enable-rofi-shared"]``` to the lamellar entry in your `Cargo.toml` file
//!
//! The long term goal for lamellar is that you can develop using the `local` backend and then when you are ready to run distributed switch to the `rofi` backend with no changes to your code.
//! Currently the inverse is true, if it compiles and runs using `rofi` it will compile and run when using `local` and `shmem` with no changes.
//!
//! Additional information on using each of the lamellae backends can be found below in the `Running Lamellar Applications` section
//!
//! Environment Variables
//! ---------------------
//! Lamellar has a number of environment variables that can be used to configure the runtime.
//! please see the [Environment Variables][crate::env_var] module documentation for more details
//!
//! Examples
//! --------
//! Our repository also provides numerous examples highlighting various features of the runtime: <https://github.com/pnnl/lamellar-runtime/tree/master/examples>
//!
//! Additionally, we are compiling a set of benchmarks (some with multiple implementations) that may be helpful to look at as well: <https://github.com/pnnl/lamellar-benchmarks/>
//!
//! Below are a few small examples highlighting some of the features of lamellar, more in-depth examples can be found in the documentation for the various features.
//! # Selecting a Lamellae and constructing a lamellar world instance
//! You can select which backend to use at runtime as shown below:
//! ```
//! use lamellar::Backend;
//! fn main(){
//!  let mut world = lamellar::LamellarWorldBuilder::new()
//!         .with_lamellae( Default::default() ) //if "enable-rofi" feature is active default is rofi, otherwise  default is `Local`
//!         //.with_lamellae( Backend::Rofi ) //explicity set the lamellae backend to rofi,
//!         //.with_lamellae( Backend::Local ) //explicity set the lamellae backend to local
//!         //.with_lamellae( Backend::Shmem ) //explicity set the lamellae backend to use shared memory
//!         .build();
//! }
//! ```
//! or by setting the following envrionment variable:
//!```LAMELLAE_BACKEND="lamellae"``` where lamellae is one of `local`, `shmem`, or `rofi`.
//!
//! # Creating and executing a Registered Active Message
//! Please refer to the [Active Messaging][crate::active_messaging] documentation for more details and examples
//! ```
//! use lamellar::active_messaging::prelude::*;
//!
//! #[AmData(Debug, Clone)] // `AmData` is a macro used in place of `derive`
//! struct HelloWorld { //the "input data" we are sending with our active message
//!     my_pe: usize, // "pe" is processing element == a node
//! }
//!
//! #[lamellar::am] // at a highlevel registers this LamellarAM implemenatation with the runtime for remote execution
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
//!         let _ = world.exec_am_pe(pe,am.clone()).spawn(); // explicitly launch on each PE
//!     }
//!     world.wait_all(); // wait for all active messages to finish
//!     world.barrier();  // synchronize with other PEs
//!     let request = world.exec_am_all(am.clone()); //also possible to execute on every PE with a single call
//!     request.block(); //both exec_am_all and exec_am_pe return futures that can be used to wait for completion and access any returned result
//! }
//! ```
//!
//! # Creating, initializing, and iterating through a distributed array
//! Please refer to the [LamellarArray][crate::array] documentation for more details and examples
//! ```
//! use lamellar::array::prelude::*;
//!
//! fn main(){
//!     let world = lamellar::LamellarWorldBuilder::new().build();
//!     let my_pe = world.my_pe();
//!     let block_array = AtomicArray::<usize>::new(&world, 1000, Distribution::Block).block(); //we also support Cyclic distribution.
//!     let _ =block_array.dist_iter_mut().enumerate().for_each(move |(i,elem)| elem.store(i)).block(); //simultaneosuly initialize array accross all PEs, each pe only updates its local data
//!     block_array.barrier();
//!     if my_pe == 0{
//!         for (i,elem) in block_array.onesided_iter().into_iter().enumerate(){ //iterate through entire array on pe 0 (automatically transfering remote data)
//!             println!("i: {} = {})",i,elem);
//!         }
//!     }
//! }
//! ```
//!
//! # Utilizing a Darc within an active message
//! Please refer to the [Darc][crate::darc] documentation for more details and examples
//!```
//! use lamellar::active_messaging::prelude::*;
//! use lamellar::darc::prelude::*;
//! use std::sync::atomic::{AtomicUsize,Ordering};
//!
//! #[AmData(Debug, Clone)] // `AmData` is a macro used in place of `derive`
//! struct DarcAm { //the "input data" we are sending with our active message
//!     cnt: Darc<AtomicUsize>, // count how many times each PE executes an active message
//! }
//!
//! #[lamellar::am] // at a highlevel registers this LamellarAM implemenatation with the runtime for remote execution
//! impl LamellarAM for DarcAm {
//!     async fn exec(&self) {
//!         self.cnt.fetch_add(1,Ordering::SeqCst);
//!     }
//! }
//!
//! fn main(){
//!     let mut world = lamellar::LamellarWorldBuilder::new().build();
//!     let my_pe = world.my_pe();
//!     let num_pes = world.num_pes();
//!     let cnt = Darc::new(&world, AtomicUsize::new(0)).block().expect("Current PE is in world team");
//!     for pe in 0..num_pes{
//!         let _ = world.exec_am_pe(pe,DarcAm{cnt: cnt.clone()}).spawn(); // explicitly launch on each PE
//!     }
//!     let _ = world.exec_am_all(DarcAm{cnt: cnt.clone()}).spawn(); //also possible to execute on every PE with a single call
//!     cnt.fetch_add(1,Ordering::SeqCst); //this is valid as well!
//!     world.wait_all(); // wait for all active messages to finish
//!     world.barrier();  // synchronize with other PEs
//!     assert_eq!(cnt.load(Ordering::SeqCst),num_pes*2 + 1);
//! }
//!```
//! # Using Lamellar
//! Lamellar is capable of running on single node workstations as well as distributed HPC systems.
//! For a workstation, simply copy the following to the dependency section of you Cargo.toml file:
//!
//!``` lamellar = "0.7.0-rc.1" ```
//!
//! If planning to use within a distributed HPC system copy the following to your Cargo.toml file:
//!
//! ``` lamellar = { version = "0.7.0-rc.1", features = ["enable-rofi"]}```
//!
//! NOTE: as of Lamellar 0.6.1 It is no longer necessary to manually install Libfabric, the build process will now try to automatically build libfabric for you.
//! If this process fails, it is still possible to pass in a manual libfabric installation via the OFI_DIR envrionment variable.
//!
//!
//! For both environments, build your application as normal
//!
//! ```cargo build (--release)```
//! # Running Lamellar Applications
//! There are a number of ways to run Lamellar applications, mostly dictated by the lamellae you want to use.
//! ## local (single-process, single system)
//! 1. directly launch the executable
//!     - ```cargo run --release```
//! ## shmem (multi-process, single system)
//! 1. grab the [lamellar_run.sh](https://github.com/pnnl/lamellar-runtime/blob/master/lamellar_run.sh)
//! 2. Use `lamellar_run.sh` to launch your application
//!     - ```./lamellar_run -N=2 -T=10 <appname>```
//!         - `N` number of PEs (processes) to launch (Default=1)
//!         - `T` number of threads Per PE (Default = number of cores/ number of PEs)
//!         - assumes `<appname>` executable is located at `./target/release/<appname>`
//! ## rofi (multi-process, multi-system)
//! 1. allocate compute nodes on the cluster:
//!     - ```salloc -N 2```
//! 2. launch application using cluster launcher
//!     - ```srun -N 2 -mpi=pmi2 ./target/release/<appname>```
//!         - `pmi2` library is required to grab info about the allocated nodes and helps set up initial handshakes
//!

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate memoffset;
//#[doc(hidden)]
pub extern crate serde;
//#[doc(hidden)]
pub use serde::*;

// //#[doc(hidden)]
pub extern crate serde_with;
// pub use serde_with::*;

// //#[doc(hidden)]
// pub extern crate tracing;
//#[doc(hidden)]
pub use parking_lot;
// //#[doc(hidden)]
// pub use tracing::*;

//#[doc(hidden)]
pub use async_trait;

//#[doc(hidden)]
pub use futures_util;

pub mod active_messaging;
// //#[doc(hidden)]
pub use active_messaging::prelude::*;
pub mod array;
// //#[doc(hidden)]
pub use array::prelude::*;
mod barrier;
pub mod darc;
// //#[doc(hidden)]
pub use darc::prelude::*;
mod lamellae;
mod lamellar_alloc;
mod lamellar_arch;
pub mod lamellar_env;
pub use lamellar_env::LamellarEnv;
mod lamellar_request;
mod lamellar_task_group;
mod lamellar_team;
mod lamellar_world;
pub mod memregion;
// //#[doc(hidden)]
pub use memregion::prelude::*;
mod scheduler;
mod utils;
//#[doc(hidden)]
pub use utils::*;

pub(crate) mod warnings;

pub mod env_var;
pub use env_var::config;

pub use crate::lamellae::Backend;
pub use crate::lamellar_arch::{BlockedArch, IdError, LamellarArch, StridedArch};
// //#[doc(hidden)]
pub use crate::lamellar_task_group::{
    AmGroup, AmGroupResult, BaseAmGroupReq, LamellarTaskGroup, TypedAmGroupBatchReq,
    TypedAmGroupBatchResult, TypedAmGroupResult,
};
pub use crate::lamellar_team::LamellarTeam;
// //#[doc(hidden)]
pub use crate::lamellar_team::ArcLamellarTeam;
pub(crate) use crate::lamellar_team::LamellarTeamRT;
pub use crate::lamellar_world::*;
pub use crate::scheduler::ExecutorType;
pub use crate::scheduler::LamellarTask;

extern crate lamellar_impl;
// //#[doc(hidden)]
pub use lamellar_impl::Dist;
// use lamellar_impl;

//#[doc(hidden)]
pub use inventory;

//#[doc(hidden)]
pub use bincode;
use bincode::Options;

// #[macro_use]
// pub extern crate custom_derive;
//#[doc(hidden)]
pub use custom_derive;

// #[macro_use]
// pub extern crate newtype_derive;
//#[doc(hidden)]
pub use newtype_derive;

lazy_static! {
    pub(crate) static ref BINCODE: bincode::config::WithOtherTrailing<bincode::DefaultOptions, bincode::config::AllowTrailing> =
        bincode::DefaultOptions::new().allow_trailing_bytes();
}
// use std::sync::atomic::AtomicUsize;
// use std::sync::atomic::Ordering::SeqCst;
// use std::sync::Arc;
// lazy_static! {
//     pub(crate) static ref SERIALIZE_TIMER: thread_local::ThreadLocal<Arc<AtomicUsize>> =
//         thread_local::ThreadLocal::new();
//     pub(crate) static ref DESERIALIZE_TIMER: thread_local::ThreadLocal<Arc<AtomicUsize>> =
//         thread_local::ThreadLocal::new();
//     pub(crate) static ref SERIALIZE_SIZE_TIMER: thread_local::ThreadLocal<Arc<AtomicUsize>> =
//         thread_local::ThreadLocal::new();
// }

/// Wrapper function for serializing data
pub fn serialize<T: ?Sized>(obj: &T, var: bool) -> Result<Vec<u8>, anyhow::Error>
where
    T: serde::Serialize,
{
    // let start = std::time::Instant::now();
    let res = if var {
        // Ok(BINCODE.serialize(obj)?)
        Ok(bincode::serialize(obj)?)
    } else {
        Ok(bincode::serialize(obj)?)
    };
    // unsafe {
    //     SERIALIZE_TIMER
    //         .get_or(|| Arc::new(AtomicUsize::new(0)))
    //         .fetch_add(start.elapsed().as_micros() as usize, SeqCst);
    // }
    res
}

/// Wrapper function for getting the size of serialized data
pub fn serialized_size<T: ?Sized>(obj: &T, var: bool) -> usize
where
    T: serde::Serialize,
{
    // let start = std::time::Instant::now();
    let res = if var {
        // BINCODE.serialized_size(obj).unwrap() as usize
        bincode::serialized_size(obj).unwrap() as usize
    } else {
        bincode::serialized_size(obj).unwrap() as usize
    };
    // unsafe {
    //     SERIALIZE_SIZE_TIMER
    //         .get_or(|| Arc::new(AtomicUsize::new(0)))
    //         .fetch_add(start.elapsed().as_micros() as usize, SeqCst);
    // }
    res
}

/// Wrapper function for serializing an object into a buffer
pub fn serialize_into<T: ?Sized>(buf: &mut [u8], obj: &T, var: bool) -> Result<(), anyhow::Error>
where
    T: serde::Serialize,
{
    // let start = std::time::Instant::now();
    if var {
        // BINCODE.serialize_into(buf, obj)?;
        bincode::serialize_into(buf, obj)?;
    } else {
        bincode::serialize_into(buf, obj)?;
    }
    // unsafe {
    //     SERIALIZE_TIMER
    //         .get_or(|| Arc::new(AtomicUsize::new(0)))
    //         .fetch_add(start.elapsed().as_micros() as usize, SeqCst);
    // }
    Ok(())
}

/// Wrapper function for deserializing data
pub fn deserialize<'a, T>(bytes: &'a [u8], var: bool) -> Result<T, anyhow::Error>
where
    T: serde::Deserialize<'a>,
{
    // let start = std::time::Instant::now();
    let res = if var {
        // Ok(BINCODE.deserialize(bytes)?)
        Ok(bincode::deserialize(bytes)?)
    } else {
        Ok(bincode::deserialize(bytes)?)
    };
    // unsafe {
    //     DESERIALIZE_TIMER
    //         .get_or(|| Arc::new(AtomicUsize::new(0)))
    //         .fetch_add(start.elapsed().as_micros() as usize, SeqCst);
    // }
    res
}
//#[doc(hidden)]
pub use async_std;

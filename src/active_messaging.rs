//! Active Messages are a computing model where messages contain both data (that you want to compute something with) and metadata
//! that tells the message how to process its data when it arrives at its destination, e.g. a function pointer. The Wikipedia Page<https://en.wikipedia.org/wiki/Active_message>
//! provides a short overview.
//!
//! Lamellar is built upon asynchronous active messages, and provides users with an interface to construct their own active messages.
//! 
//! This interface is exposed through multiple Rust procedural macros.
//! - [AmData]
//! - [am]
//! - [AmLocalData]
//! - [local_am]
//!
//! Further details are provided in the documentation for each macro but at a high level to implement and active message we need to
//! define the data to be transfered in a message and then define what to do with that data when we arrive at the destination.
//!
//! # Example
//! Let's implement a simple active message below:
//!
//! First lets define the data we would like to transfer
//!```
//!#[derive(Debug,Clone)]
//! struct HelloWorld {
//!    originial_pe: usize, //this will contain the ID of the PE this data originated from
//! } 
//!```
//! This looks like a pretty normal (if simple) struct, we next have to let the runtime know we would like this data
//! to be used in an active message, so we need to apply the [AmData] macro, this is done by replacing the `derive` macro:
//!```
//! use lamellar::active_messaging::prelude::*;
//!#[AmData(Debug,Clone)]
//! struct HelloWorld {
//!    originial_pe: usize, //this will contain the ID of the PE this data originated from
//! } 
//!```
//! This change allows the compiler to implement the proper traits (related to Serialization and Deserialization) that will let this data type
//! be used in an active message.
//! 
//! Next we now need to define the processing that we would like to take place when a message arrives at another PE
//!
//! For this we use the [am] macro on an implementation of the [LamellarAM] trait
//!```
//! #[lamellar::am]
//! impl LamellarAM for HelloWorld {
//!     async fn exec(self) {
//!         println!(
//!             "Hello World, I'm from PE {:?}",
//!             self.originial_pe,
//!         );
//!     }
//! }
//!```
//! The [am] macro parses the provided implementation and performs a number of transformations to the code to enable execution of the active message.
//! This macro is responsible for generating the code which will perform serialization/deserialization of both the active message data and any returned data.
//!
//! Each active message implementation is assigned a unique ID at runtime initialization, these IDs are then used as the key
//! to a Map containing specialized deserialization functions that convert a slice of bytes into the appropriate data type on the remote PE.
//! 
//! The final step is to actually launch an active message and await its result
//!```
//! fn main(){
//!     let world = lamellar::LamellarWorldBuilder::new().build();
//!     let my_pe = world.my_pe();
//!     //Send a Hello World Active Message to all pes
//!     let request = world.exec_am_all(
//!         HelloWorld {
//!             originial_pe: Arc::new(Mutex::new(my_pe)),
//!         }
//!     );
//!     //wait for the request to complete
//!     world.block_on(request);
//! }
//!```
//! In this example we simply send a `HelloWorld` from every PE to every other PE using `exec_am_all` (please see the [ActiveMessaging] trait documentation for further details).
//! `exec_am_all` returns a `Future` which we can use to await the completion of our operation.
//!
//! Sample output for the above example on a 2 PE system may look something like (exact ordering is nondeterministic due to asynchronous behavior)
//!```text
//! Hello World, I'm from PE 0
//! Hello World, I'm from PE 1
//! Hello World, I'm from PE 0
//! Hello World, I'm from PE 1
//!```
//!
//! What if we wanted to actuall know where we are currently executing?

//! # Lamellar AM DSL
//! This lamellar [am] macro also parses the provided code block for the presence of keywords from a small DSL, specifically searching for the following token streams:
//! - ```lamellar::current_pe``` - return the world id of the PE this active message is executing on
//! - ```lamellar::num_pes``` - return the number of PEs in the world
//! - ```lamellar::world``` - return a reference to the instantiated LamellarWorld
//! - ```lamellar::team``` - return a reference to the LamellarTeam responsible for launching this AM
//!
//! Given this functionality, we can adapt the above active message body to this:
//!```
//! #[lamellar::am]
//! impl LamellarAM for HelloWorld {
//!     async fn exec(self) {
//!         println!(
//!             "Hello World on PE {:?} of {:?}, I'm from PE {:?}",
//!             lamellar::current_pe,
//!             lamellar::num_pes,
//!             self.originial_pe,
//!         );
//!     }
//! }
//!```
//! the new Sample output for the above example on a 2 PE system may look something like (exact ordering is nondeterministic due to asynchronous behavior)
//!```text
//! Hello World on PE 0 of 2, I'm from PE 0
//! Hello World on PE 0 of 2, I'm from PE 1
//! Hello World on PE 1 of 2, I'm from PE 0
//! Hello World on PE 1 of 2, I'm from PE 1
//!```
//! # Active Messages with return data
//! ## normal data
//! ## another active message
//! # Nested Active Messages
use crate::lamellae::{Lamellae, LamellaeRDMA, SerializedData};
use crate::lamellar_arch::IdError;
use crate::lamellar_request::{InternalResult, LamellarRequestResult};
use crate::lamellar_team::{LamellarTeam, LamellarTeamRT};
use crate::scheduler::{ReqId, SchedulerQueue};
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
// use log::trace;
use async_trait::async_trait;
use futures::Future;
use parking_lot::Mutex; //, RwLock};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc; //, Weak};

pub mod prelude;

pub(crate) mod registered_active_message;
use registered_active_message::RegisteredActiveMessages; //, AMS_EXECS};
#[doc(hidden)]
pub use registered_active_message::RegisteredAm;

pub(crate) mod batching;

const BATCH_AM_SIZE: usize = 100000;

/// This macro is used to setup the attributed type so that it can be used within remote active messages.
///
/// For this derivation to succeed all members of the data structure must impl [AmDist] (which it self is a blanket impl)
///
///```
/// AmDist: serde::ser::Serialize + serde::de::DeserializeOwned + Sync + Send + 'static {}
/// impl<T: serde::ser::Serialize + serde::de::DeserializeOwned + Sync + Send + 'static> AmDist for T {}
///```
///
/// Typically you will use this macro in place of `#[derive()]`, as it will manage deriving both the traits 
/// that are provided as well as those require by Lamellar for active messaging.
///
/// Generally this is paired with the [lamellar::am][am] macro on an implementation of the [LamellarAM], to associate a remote function with this data.
/// (if you simply want this type to able to be included in other active messages, implementing [LamellarAM] can be omitted )
///
pub use lamellar_impl::AmData;

/// This macro is used to setup the attributed type so that it can be used within local active messages.
///
/// Typically you will use this macro in place of `#[derive()]`, as it will manage deriving both the traits 
/// that are provided as well as those require by Lamellar for active messaging.
///
/// This macro relaxes the Serialize/Deserialize trait bounds required by the [AmData] macro
///
/// Generally this is paired with the [lamellar::local_am][local_am] macro on an implementation of the [LamellarAM], to associate a local function with this data.
/// (if you simply want this type to able to be included in other active messages, implementing [LamellarAM] can be omitted )
///
pub use lamellar_impl::AmLocalData;

/// This macro is used to associate an implemenation of [LamellarAM] for type that has used the [AmData] attribute macro
///
/// This essentially constructs and registers the Active Message with the runtime. It is responsible for ensuring all data
/// within the active message is properly serialize and deserialized, including any returned results.
///
/// Each active message implementation is assigned a unique ID at runtime initialization, these IDs are then used as the key
/// to a Map containing specialized deserialization functions that convert a slice of bytes into the appropriate data type on the remote PE.
/// Finally, a worker thread will call that deserialized objects `exec()` function to execute the actual active message.
///
/// # Lamellar AM DSL
/// This macro also parses the provided code block for the presence of keywords from a small DSL, specifically searching for the following token streams:
/// - ```lamellar::current_pe``` - return the world id of the PE this active message is executing on
/// - ```lamellar::num_pes``` - return the number of PEs in the world
/// - ```lamellar::world``` - return a reference to the instantiated LamellarWorld
/// - ```lamellar::team``` - return a reference to the LamellarTeam responsible for launching this AM
///
pub use lamellar_impl::am;

/// This macro is used to associate an implemenation of [LamellarAM] for a data structure that has used the [AmLocalData] attribute macro
///
/// This essentially constructs and registers the Active Message with the runtime. (LocalAms *do not* perform any serialization/deserialization)
///
/// # Lamellar AM DSL
/// This macro also parses the provided code block for the presence of keywords from a small DSL, specifically searching for the following token streams:
/// - ```lamellar::current_pe``` - return the world id of the PE this active message is executing on
/// - ```lamellar::num_pes``` - return the number of PEs in the world
/// - ```lamellar::world``` - return a reference to the instantiated LamellarWorld
/// - ```lamellar::team``` - return a reference to the LamellarTeam responsible for launching this AM
///
pub use lamellar_impl::local_am;


/// Supertrait specifying `Sync` + `Send`
pub trait SyncSend: Sync + Send {}

impl<T: Sync + Send> SyncSend for T {}

/// Supertrait specifying a Type can be used in (remote)ActiveMessages
///
/// Types must impl [Serialize][serde::ser::Serialize], [Deserialize][serde::de::DeserializeOwned], and [SyncSend]
pub trait AmDist: serde::ser::Serialize + serde::de::DeserializeOwned + SyncSend + 'static {}

impl<T: serde::ser::Serialize + serde::de::DeserializeOwned + SyncSend + 'static> AmDist for T {}

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
pub(crate) enum ExecType {
    Am(Cmd),
    Runtime(Cmd),
}

#[doc(hidden)]
pub trait DarcSerde {
    fn ser(&self, num_pes: usize);
    fn des(&self, cur_pe: Result<usize, IdError>);
}

impl<T> DarcSerde for &T {
    fn ser(&self, _num_pes: usize) {}
    fn des(&self, _cur_pe: Result<usize, IdError>) {}
}

#[doc(hidden)]
pub trait LamellarSerde: SyncSend {
    fn serialized_size(&self) -> usize;
    fn serialize_into(&self, buf: &mut [u8]);
}

#[doc(hidden)]
pub trait LamellarResultSerde: LamellarSerde {
    fn serialized_result_size(&self, result: &LamellarAny) -> usize;
    fn serialize_result_into(&self, buf: &mut [u8], result: &LamellarAny);
}

#[doc(hidden)]
pub trait RemoteActiveMessage: LamellarActiveMessage + LamellarSerde + LamellarResultSerde {
    fn as_local(self: Arc<Self>) -> LamellarArcLocalAm;
}

#[doc(hidden)]
pub trait LamellarActiveMessage: DarcSerde {
    fn exec(
        self: Arc<Self>,
        my_pe: usize,
        num_pes: usize,
        local: bool,
        world: Arc<LamellarTeam>,
        team: Arc<LamellarTeam>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = LamellarReturn> + Send>>;
    fn get_id(&self) -> &'static str;
}

pub(crate) type LamellarArcLocalAm = Arc<dyn LamellarActiveMessage + Sync + Send>;
pub(crate) type LamellarArcAm = Arc<dyn RemoteActiveMessage + Sync + Send>;
pub(crate) type LamellarAny = Box<dyn std::any::Any + Sync + Send>;
pub(crate) type LamellarResultArc = Arc<dyn LamellarSerde + Sync + Send>;

/// Supertrait specifying `serde::ser::Serialize` + `serde::de::DeserializeOwned`
pub trait Serde: serde::ser::Serialize + serde::de::DeserializeOwned {}

/// The trait representing an active message that can only be executed locally, i.e. from the PE that initiated it
/// (SyncSend is a blanket impl for Sync + Send)
pub trait LocalAM: SyncSend {
    /// The type of the output returned by the active message
    type Output: SyncSend;
}

/// The trait representing an active message that can be executed remotely
/// (AmDist is a blanket impl for serde::Serialize + serde::Deserialize + Sync + Send + 'static)
#[async_trait]
pub trait LamellarAM {
    /// The type of the output returned by the active message
    type Output: AmDist;
    async fn exec(self) -> Self::Output;
}

#[doc(hidden)]
pub enum LamellarReturn {
    LocalData(LamellarAny),
    LocalAm(LamellarArcAm),
    RemoteData(LamellarResultArc),
    RemoteAm(LamellarArcAm),
    Unit,
}

impl std::fmt::Debug for LamellarReturn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LamellarReturn::LocalData(_) => write!(f, "LocalData"),
            LamellarReturn::LocalAm(_) => write!(f, "LocalAm"),
            LamellarReturn::RemoteData(_) => write!(f, "RemoteData"),
            LamellarReturn::RemoteAm(_) => write!(f, "RemoteAm"),
            LamellarReturn::Unit => write!(f, "Unit"),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ReqMetaData {
    pub(crate) src: usize,         //source pe
    pub(crate) dst: Option<usize>, // destination pe - team based pe id, none means all pes
    pub(crate) id: ReqId,          // id of the request
    pub(crate) lamellae: Arc<Lamellae>,
    pub(crate) world: Pin<Arc<LamellarTeamRT>>,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) team_addr: usize,
}

pub(crate) enum Am {
    All(ReqMetaData, LamellarArcAm),
    Remote(ReqMetaData, LamellarArcAm), //req data, am to execute
    Local(ReqMetaData, LamellarArcLocalAm), //req data, am to execute
    Return(ReqMetaData, LamellarArcAm), //req data, am to return and execute
    Data(ReqMetaData, LamellarResultArc), //req data, data to return
    Unit(ReqMetaData),                  //req data
    _BatchedReturn(ReqMetaData, LamellarArcAm, ReqId), //req data, am to return and execute, batch id
    _BatchedData(ReqMetaData, LamellarResultArc, ReqId), //req data, data to return, batch id
    _BatchedUnit(ReqMetaData, ReqId),                  //req data, batch id
}

impl std::fmt::Debug for Am {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Am::All(_, _) => write!(f, "All"),
            Am::Remote(_, _) => write!(f, "Remote"),
            Am::Local(_, _) => write!(f, "Local"),
            Am::Return(_, _) => write!(f, "Return"),
            Am::Data(_, _) => write!(f, "Data"),
            Am::Unit(_) => write!(f, "Unit"),
            Am::_BatchedReturn(_, _, _) => write!(f, "BatchedReturn"),
            Am::_BatchedData(_, _, _) => write!(f, "BatchedData"),
            Am::_BatchedUnit(_, _) => write!(f, "BatchedUnit"),
        }
    }
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Default,
)]
pub(crate) enum Cmd {
    #[default]
    Am, //a single am
    ReturnAm, //a single return am
    Data,     //a single data result
    Unit,     //a single unit result
    BatchedMsg, //a batched message, can contain a variety of am types
              // BatchedReturnAm, //a batched message, only containing return ams -- not sure this can happen
              // BatchedData, //a batched message, only containing data results
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy, Default)]
pub(crate) struct Msg {
    pub src: u16,
    pub cmd: Cmd,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub(crate) enum RetType {
    //maybe change to ReqType? ReturnRequestType?
    Unit,
    Closure,
    Am,
    Data,
    Barrier,
    NoHandle,
    Put,
    Get,
}

#[derive(Debug)]
pub(crate) struct AMCounters {
    pub(crate) outstanding_reqs: Arc<AtomicUsize>,
    pub(crate) send_req_cnt: AtomicUsize,
}

//#[prof]
impl AMCounters {
    pub(crate) fn new() -> AMCounters {
        AMCounters {
            outstanding_reqs: Arc::new(AtomicUsize::new(0)),
            send_req_cnt: AtomicUsize::new(0),
        }
    }
    pub(crate) fn add_send_req(&self, num: usize) {
        let _num_reqs = self.outstanding_reqs.fetch_add(num, Ordering::SeqCst);
        // println!("add_send_req {}",_num_reqs+1);
        self.send_req_cnt.fetch_add(num, Ordering::SeqCst);
    }
}

pub trait ActiveMessaging {
    /// launch and execute an active message on every PE (including originating PE).
    ///
    /// Expects as input an instance of a struct thats been defined using the lamellar::am procedural macros.
    ///
    /// Returns a future allow the user to poll for complete and retrive the result of the Active Message stored within a vector,
    /// each index in the vector corresponds to the data returned by the corresponding PE
    ///
    /// NOTE: lamellar active messages are not lazy, i.e. you do not need to drive the returned future to launch the computation,
    /// the future is only used to check for completeion and/or retrieving any returned data
    /// # Examples
    ///```
    /// use lamellar::ActiveMessaging;
    ///
    /// #[lamellar::AmData(/*derivable traits*/)]
    /// struct Am{
    /// // can contain anything that impls Serialize, Deserialize, Sync, Send   
    /// }
    ///
    /// #[lamellar::am]
    /// impl LamellarAM for Am{
    ///     async fn exec(self) -> usize { //can return nothing or any type that impls Serialize, Deserialize, Sync, Send
    ///         //do some remote computation
    ///         lamellar::current_pe //return the executing pe
    ///     }
    /// }
    /// //----------------
    ///
    /// let world =  let world = lamellar::LamellarWorldBuilder::new().build();
    /// let request = world.exec_am_all(Am{...}); //launch am on all pes
    /// let results = world.block_on(request); //block until am has executed and retrieve the data
    /// for i in 0..world.num_pes(){
    ///     assert_eq!(i,results[i]);
    /// }
    ///```
    fn exec_am_all<F>(&self, am: F) -> Pin<Box<dyn Future<Output = Vec<F::Output>> + Send>>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist;

    /// launch and execute an active message on a specifc PE.
    ///
    /// Expects as input the PE to execute on and an instance of a struct thats been defined using the lamellar::am procedural macros.
    ///
    /// Returns a future allow the user to poll for complete and retrive the result of the Active Message
    ///
    ///
    /// NOTE: lamellar active messages are not lazy, i.e. you do not need to drive the returned future to launch the computation,
    /// the future is only used to check for completeion and/or retrieving any returned data
    /// # Examples
    ///```
    /// use lamellar::ActiveMessaging;
    ///
    /// #[lamellar::AmData(/*derivable traits*/)]
    /// struct Am{
    /// // can contain anything that impls Serialize, Deserialize, Sync, Send   
    /// }
    ///
    /// #[lamellar::am]
    /// impl LamellarAM for Am{
    ///     async fn exec(self) -> usize { //can return nothing or any type that impls Serialize, Deserialize, Sync, Send
    ///         //do some remote computation
    ///         lamellar::current_pe //return the executing pe
    ///     }
    /// }
    /// //----------------
    ///
    /// let world =  let world = lamellar::LamellarWorldBuilder::new().build();
    /// let request = world.exec_am_pe(world.num_pes()-1, Am{...}); //launch am on all pes
    /// let result = world.block_on(request); //block until am has executed
    /// assert_eq!(world.num_pes()-1,result);
    ///```
    fn exec_am_pe<F>(&self, pe: usize, am: F) -> Pin<Box<dyn Future<Output = F::Output> + Send>>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist;

    /// launch and execute an active message on the calling PE.
    ///
    /// Expects as input an instance of a struct thats been defined using the lamellar::local_am procedural macros.
    ///
    /// Returns a future allow the user to poll for complete and retrive the result of the Active Message.
    ///
    ///
    /// NOTE: lamellar active messages are not lazy, i.e. you do not need to drive the returned future to launch the computation,
    /// the future is only used to check for completeion and/or retrieving any returned data.
    /// # Examples
    ///```
    /// use lamellar::ActiveMessaging;
    ///
    /// #[lamellar::AmLocalData(/*derivable traits*/)]
    /// struct Am{
    /// // can contain anything that impls Sync, Send   
    /// }
    ///
    /// #[lamellar::local_am]
    /// impl LamellarAM for Am{
    ///     async fn exec(self) -> usize { //can return nothing or any type that impls Serialize, Deserialize, Sync, Send
    ///         //do some remote computation
    ///         lamellar::current_pe //return the executing pe
    ///     }
    /// }
    /// //----------------
    ///
    /// let world =  let world = lamellar::LamellarWorldBuilder::new().build();
    /// let request = world.exec_am_local(Am{...}); //launch am on all pes
    /// let result = world.block_on(request); //block until am has executed
    /// assert_eq!(world.my_pe(),result);
    ///```
    fn exec_am_local<F>(&self, am: F) -> Pin<Box<dyn Future<Output = F::Output> + Send>>
    where
        F: LamellarActiveMessage + LocalAM + 'static;

    /// blocks calling thread until all remote tasks (e.g. active mesages, array operations)
    /// initiated by the calling PE have completed.
    ///
    /// Note: this is not a distributed synchronization primitive (i.e. it has no knowledge of a Remote PEs tasks)
    /// # Examples
    ///```
    /// use lamellar::ActiveMessaging;
    ///
    /// let world =  let world = lamellar::LamellarWorldBuilder::new().build();
    /// world.exec_am_all(...);
    /// world.wait_all(); //block until the previous am has finished
    ///```
    fn wait_all(&self);

    /// Global synchronization method which blocks calling thread until all PEs in the barrier group (e.g. World, Team, Array) have entered
    ///
    /// # Examples
    ///```
    /// use lamellar::ActiveMessaging;
    ///
    /// let world =  let world = lamellar::LamellarWorldBuilder::new().build();
    /// //do some work
    /// world.barrier(); //block until all PEs have entered the barrier
    ///```
    fn barrier(&self);

    /// Run a future to completion on the current thread
    ///
    /// This function will block the caller until the given future has completed, the future is executed within the Lamellar threadpool
    ///
    /// Users can await any future, including those returned from lamellar remote operations
    ///
    /// # Examples
    ///```
    /// use lamellar::ActiveMessaging;
    ///
    /// let request = world.exec_am_local(Am{...}); //launch am on all pes
    /// let result = world.block_on(request); //block until am has executed
    /// // you can also directly pass an async block
    /// world.block_on(async move {
    ///     let file = async_std::fs::File.open(...);.await;
    ///     for pe in 0..num_pes{
    ///         let data = file.read(...).await;
    ///         world.exec_am_pe(pe,DataAm{data}).await;
    ///     }
    ///     world.exec_am_all(...).await;
    /// });
    ///```
    fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future;
}

#[async_trait]
pub(crate) trait ActiveMessageEngine {
    async fn process_msg(
        &self,
        am: Am,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
        stall_mark: usize,
    );

    async fn exec_msg(
        &self,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
    );

    fn get_team_and_world(
        &self,
        pe: usize,
        team_addr: usize,
        lamellae: &Arc<Lamellae>,
    ) -> (Arc<LamellarTeam>, Arc<LamellarTeam>) {
        let local_team_addr = lamellae.local_addr(pe, team_addr);
        let team_rt = unsafe {
            let team_ptr = local_team_addr as *mut *const LamellarTeamRT;
            // println!("{:x} {:?} {:?} {:?}", team_hash,team_ptr, (team_hash as *mut (*const LamellarTeamRT)).as_ref(), (*(team_hash as *mut (*const LamellarTeamRT))).as_ref());
            Arc::increment_strong_count(*team_ptr);
            Pin::new_unchecked(Arc::from_raw(*team_ptr))
        };
        let world_rt = if let Some(world) = team_rt.world.clone() {
            world
        } else {
            team_rt.clone()
        };
        let world = LamellarTeam::new(None, world_rt, true);
        let team = LamellarTeam::new(Some(world.clone()), team_rt, true);
        (team, world)
    }

    fn send_data_to_user_handle(&self, req_id: ReqId, pe: usize, data: InternalResult) {
        // println!("returned req_id: {:?}", req_id);
        let req = unsafe { Arc::from_raw(req_id.id as *const LamellarRequestResult) };
        // println!("strong count recv: {:?} ", Arc::strong_count(&req));
        req.add_result(pe, req_id.sub_id, data);
    }
}

#[derive(Debug)]
pub(crate) enum ActiveMessageEngineType {
    RegisteredActiveMessages(RegisteredActiveMessages),
}

#[async_trait]
impl ActiveMessageEngine for ActiveMessageEngineType {
    async fn process_msg(
        &self,
        am: Am,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
        stall_mark: usize,
    ) {
        match self {
            ActiveMessageEngineType::RegisteredActiveMessages(remote_am) => {
                remote_am.process_msg(am, scheduler, stall_mark).await;
            }
        }
    }
    async fn exec_msg(
        &self,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
    ) {
        match self {
            ActiveMessageEngineType::RegisteredActiveMessages(remote_am) => {
                remote_am.exec_msg(msg, ser_data, lamellae, scheduler).await;
            }
        }
    }
}

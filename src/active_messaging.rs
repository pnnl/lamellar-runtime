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

pub(crate) mod registered_active_message;
use registered_active_message::RegisteredActiveMessages; //, AMS_EXECS};

pub(crate) mod batching;

const BATCH_AM_SIZE: usize = 100000;

pub trait SyncSend: Sync + Send {}

impl<T: Sync + Send> SyncSend for T {}

pub trait AmDist: serde::ser::Serialize + serde::de::DeserializeOwned + SyncSend + 'static {}

impl<T: serde::ser::Serialize + serde::de::DeserializeOwned + SyncSend + 'static> AmDist for T {}

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
pub(crate) enum ExecType {
    Am(Cmd),
    Runtime(Cmd),
}


pub trait DarcSerde {
    fn ser(&self, num_pes: usize);
    fn des(&self, cur_pe: Result<usize, IdError>);
}

impl<T> DarcSerde for &T {
    fn ser(&self, _num_pes: usize) {}
    fn des(&self, _cur_pe: Result<usize, IdError>) {}
}

pub trait LamellarSerde: SyncSend {
    fn serialized_size(&self) -> usize;
    fn serialize_into(&self, buf: &mut [u8]);
}
pub trait LamellarResultSerde: LamellarSerde {
    fn serialized_result_size(&self, result: &LamellarAny) -> usize;
    fn serialize_result_into(&self, buf: &mut [u8], result: &LamellarAny);
}

pub trait RemoteActiveMessage: LamellarActiveMessage + LamellarSerde + LamellarResultSerde {
    fn as_local(self: Arc<Self>) -> LamellarArcLocalAm;
}

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

pub trait Serde: serde::ser::Serialize + serde::de::DeserializeOwned {}

/// The trait representing an active message that can only be executed locally, i.e. from the PE that initiated it
/// (SyncSend is a blanket impl for Sync + Send)
pub trait LocalAM: SyncSend {
    /// The type of the output returned by the active message
    type Output: SyncSend;
}

/// The trait representing an active message that can be executed remotely
/// (AmDist is a blanket impl for serde::Serialize + serde::Deserialize + Sync + Send + 'static)
pub trait LamellarAM {
    /// The type of the output returned by the active message
    type Output: AmDist;
}

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
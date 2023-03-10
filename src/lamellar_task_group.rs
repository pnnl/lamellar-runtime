use crate::active_messaging::*;
use crate::lamellae::Des;
use crate::lamellar_arch::LamellarArchRT;
use crate::lamellar_request::*;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT, LamellarTeam};
use crate::memregion::one_sided::MemRegionHandleInner;
use crate::scheduler::{ReqId, SchedulerQueue};
use crate::Darc;


use crate::active_messaging::registered_active_message::{AmId, AM_ID_START, AMS_IDS, AMS_EXECS};


use async_trait::async_trait;

// use crossbeam::utils::CachePadded;
use futures::Future;
use futures::StreamExt;
use parking_lot::Mutex;
use std::collections::{HashMap,BTreeMap};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub(crate) struct TaskGroupRequestHandleInner {
    cnt: Arc<AtomicUsize>,
    data: Mutex<HashMap<usize, InternalResult>>, //<sub_id, result>
    team_outstanding_reqs: Arc<AtomicUsize>,
    world_outstanding_reqs: Arc<AtomicUsize>,
    tg_outstanding_reqs: Option<Arc<AtomicUsize>>,
}

#[doc(hidden)]
#[derive(Debug)]
pub struct TaskGroupRequestHandle<T: AmDist> {
    inner: Arc<TaskGroupRequestHandleInner>,
    sub_id: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: AmDist> Drop for TaskGroupRequestHandle<T> {
    fn drop(&mut self) {
        self.inner.cnt.fetch_sub(1, Ordering::SeqCst);
    }
}

impl LamellarRequestAddResult for TaskGroupRequestHandleInner {
    fn user_held(&self) -> bool {
        self.cnt.load(Ordering::SeqCst) > 0
    }
    fn add_result(&self, _pe: usize, sub_id: usize, data: InternalResult) {
        self.data.lock().insert(sub_id, data);
    }
    fn update_counters(&self) {
        let _team_reqs = self.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        let _world_req = self.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        // println!("tg update counter team {} world {}",_team_reqs-1,_world_req-1);
        if let Some(tg_outstanding_reqs) = self.tg_outstanding_reqs.clone() {
            tg_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl<T: AmDist> TaskGroupRequestHandle<T> {
    fn process_result(&self, data: InternalResult) -> T {
        match data {
            InternalResult::Local(x) => {
                if let Ok(result) = x.downcast::<T>() {
                    *result
                } else {
                    panic!("unexpected result type");
                }
            }
            InternalResult::Remote(result, darcs) => {
                if let Ok(result) = result.deserialize_data::<T>() {
                    // we need to appropraiately set the reference counts if the returned data contains any Darcs
                    // we "cheat" in that we dont actually care what the Darc wraps (hence the cast to ()) we just care
                    // that the reference count is updated.
                    for darc in darcs {
                        match darc {
                            RemotePtr::NetworkDarc(darc) => {
                                let temp: Darc<()> = darc.into();
                                temp.des(Ok(0));
                                temp.inc_local_cnt(1); //we drop temp decreasing local count, but need to account for the actual real darc (and we unfourtunately cannot enforce the T: DarcSerde bound, or at least I havent figured out how to yet)
                            }
                            RemotePtr::NetMemRegionHandle(mr) => {
                                let temp: Arc<MemRegionHandleInner> = mr.into();
                                temp.local_ref.fetch_add(2, Ordering::SeqCst); // Need to increase by two, 1 for temp, 1 for result
                            }
                        }
                    }
                    result
                } else {
                    panic!("unexpected result type");
                }
            }
            InternalResult::Unit => {
                if let Ok(result) = (Box::new(()) as Box<dyn std::any::Any>).downcast::<T>() {
                    *result
                } else {
                    panic!("unexpected unit result  of type ");
                }
            }
        }
    }
}

#[async_trait]
impl<T: AmDist> LamellarRequest for TaskGroupRequestHandle<T> {
    type Output = T;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        let mut res = self.inner.data.lock().remove(&self.sub_id);
        while res.is_none() {
            async_std::task::yield_now().await;
            res = self.inner.data.lock().remove(&self.sub_id);
        }
        self.process_result(res.unwrap())
    }

    fn get(&self) -> Self::Output {
        let mut res = self.inner.data.lock().remove(&self.sub_id);
        while res.is_none() {
            std::thread::yield_now();
            res = self.inner.data.lock().remove(&self.sub_id);
        }
        self.process_result(res.unwrap())
    }
}

#[derive(Debug)]
pub(crate) struct TaskGroupMultiRequestHandleInner {
    cnt: Arc<AtomicUsize>,
    arch: Arc<LamellarArchRT>,
    data: Mutex<HashMap<usize, HashMap<usize, InternalResult>>>, //<sub_id, <pe, result>>
    team_outstanding_reqs: Arc<AtomicUsize>,
    world_outstanding_reqs: Arc<AtomicUsize>,
    tg_outstanding_reqs: Option<Arc<AtomicUsize>>,
}

#[doc(hidden)]
#[derive(Debug)]
pub struct TaskGroupMultiRequestHandle<T: AmDist> {
    inner: Arc<TaskGroupMultiRequestHandleInner>,
    sub_id: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: AmDist> Drop for TaskGroupMultiRequestHandle<T> {
    fn drop(&mut self) {
        self.inner.cnt.fetch_sub(1, Ordering::SeqCst);
    }
}

impl LamellarRequestAddResult for TaskGroupMultiRequestHandleInner {
    fn user_held(&self) -> bool {
        self.cnt.load(Ordering::SeqCst) > 0
    }
    fn add_result(&self, pe: usize, sub_id: usize, data: InternalResult) {
        let pe = self.arch.team_pe(pe).expect("pe does not exist on team");
        let mut map = self.data.lock(); //.insert(pe, data);
        map.entry(sub_id)
            .or_insert_with(|| HashMap::new())
            .insert(pe, data);
    }
    fn update_counters(&self) {
        let _team_reqs = self.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        let _world_req = self.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        // println!("tg update counter team {} world {}",_team_reqs-1,_world_req-1);
        if let Some(tg_outstanding_reqs) = self.tg_outstanding_reqs.clone() {
            tg_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl<T: AmDist> TaskGroupMultiRequestHandle<T> {
    fn process_result(&self, data: InternalResult) -> T {
        match data {
            InternalResult::Local(x) => {
                if let Ok(result) = x.downcast::<T>() {
                    *result
                } else {
                    panic!("unexpected result type");
                }
            }
            InternalResult::Remote(result, darcs) => {
                if let Ok(result) = result.deserialize_data::<T>() {
                    // we need to appropraiately set the reference counts if the returned data contains any Darcs
                    // we "cheat" in that we dont actually care what the Darc wraps (hence the cast to ()) we just care
                    // that the reference count is updated.
                    for darc in darcs {
                        match darc {
                            RemotePtr::NetworkDarc(darc) => {
                                let temp: Darc<()> = darc.into();
                                temp.des(Ok(0));
                                temp.inc_local_cnt(1); //we drop temp decreasing local count, but need to account for the actual real darc (and we unfourtunately cannot enforce the T: DarcSerde bound, or at least I havent figured out how to yet)
                            }
                            RemotePtr::NetMemRegionHandle(mr) => {
                                let temp: Arc<MemRegionHandleInner> = mr.into();
                                temp.local_ref.fetch_add(2, Ordering::SeqCst); // Need to increase by two, 1 for temp, 1 for result
                            }
                        }
                    }
                    result
                } else {
                    panic!("unexpected result type");
                }
            }
            InternalResult::Unit => {
                if let Ok(result) = (Box::new(()) as Box<dyn std::any::Any>).downcast::<T>() {
                    *result
                } else {
                    panic!("unexpected unit result  of type ");
                }
            }
        }
    }
}

#[async_trait]
impl<T: AmDist> LamellarMultiRequest for TaskGroupMultiRequestHandle<T> {
    type Output = T;
    async fn into_future(mut self: Box<Self>) -> Vec<Self::Output> {
        while !self.inner.data.lock().contains_key(&self.sub_id) {
            async_std::task::yield_now().await;
        }
        while self.inner.data.lock().get(&self.sub_id).unwrap().len() < self.inner.arch.num_pes() {
            async_std::task::yield_now().await;
        }
        let mut sub_id_map = self.inner.data.lock().remove(&self.sub_id).unwrap();
        let mut res = Vec::new();
        for pe in 0..sub_id_map.len() {
            res.push(self.process_result(sub_id_map.remove(&pe).unwrap()));
        }
        res
    }

    fn get(&self) -> Vec<Self::Output> {
        while !self.inner.data.lock().contains_key(&self.sub_id) {
            std::thread::yield_now();
        }
        while self.inner.data.lock().get(&self.sub_id).unwrap().len() < self.inner.arch.num_pes() {
            std::thread::yield_now();
        }
        let mut sub_id_map = self.inner.data.lock().remove(&self.sub_id).unwrap();
        let mut res = Vec::new();
        for pe in 0..sub_id_map.len() {
            res.push(self.process_result(sub_id_map.remove(&pe).unwrap()));
        }
        res
    }
}

#[derive(Debug)]
pub(crate) struct TaskGroupLocalRequestHandleInner {
    cnt: Arc<AtomicUsize>,
    data: Mutex<HashMap<usize, LamellarAny>>, //<sub_id, result>
    team_outstanding_reqs: Arc<AtomicUsize>,
    world_outstanding_reqs: Arc<AtomicUsize>,
    tg_outstanding_reqs: Option<Arc<AtomicUsize>>,
}

#[doc(hidden)]
#[derive(Debug)]
pub struct TaskGroupLocalRequestHandle<T> {
    inner: Arc<TaskGroupLocalRequestHandleInner>,
    sub_id: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Drop for TaskGroupLocalRequestHandle<T> {
    fn drop(&mut self) {
        self.inner.cnt.fetch_sub(1, Ordering::SeqCst);
    }
}

impl LamellarRequestAddResult for TaskGroupLocalRequestHandleInner {
    fn user_held(&self) -> bool {
        self.cnt.load(Ordering::SeqCst) > 0
    }
    fn add_result(&self, _pe: usize, sub_id: usize, data: InternalResult) {
        match data {
            InternalResult::Local(x) => self.data.lock().insert(sub_id, x),
            InternalResult::Remote(_, _) => panic!("unexpected result type"),
            InternalResult::Unit => self.data.lock().insert(sub_id, Box::new(()) as LamellarAny),
        };
    }
    fn update_counters(&self) {
        let _team_reqs = self.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        let _world_req = self.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        // println!("tg update counter team {} world {}",_team_reqs-1,_world_req-1);
        if let Some(tg_outstanding_reqs) = self.tg_outstanding_reqs.clone() {
            tg_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl<T: 'static> TaskGroupLocalRequestHandle<T> {
    fn process_result(&self, data: LamellarAny) -> T {
        if let Ok(result) = data.downcast::<T>() {
            *result
        } else {
            panic!("unexpected result type");
        }
    }
}

#[async_trait]
impl<T: SyncSend + 'static> LamellarRequest for TaskGroupLocalRequestHandle<T> {
    type Output = T;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        let mut res = self.inner.data.lock().remove(&self.sub_id);
        while res.is_none() {
            async_std::task::yield_now().await;
            res = self.inner.data.lock().remove(&self.sub_id);
        }
        self.process_result(res.unwrap())
    }

    fn get(&self) -> Self::Output {
        let mut res = self.inner.data.lock().remove(&self.sub_id);
        while res.is_none() {
            std::thread::yield_now();
            res = self.inner.data.lock().remove(&self.sub_id);
        }
        self.process_result(res.unwrap())
    }
}

/// An abstraction for representing a set of active messages as single group.
///
/// This allows a user to wait on all the tasks in this group to finish executing.
/// This is in contrast to either waiting for a single request to finish, or to waiting for all tasks launched by a team to finish.
///
/// A given team can construct multiple independent Task Groups at a time
///
/// # Examples
///
///```
/// use lamellar::active_messaging::prelude::*;
///
/// #[AmData(Debug,Clone)]
/// struct Am{
///     world_pe: usize,
///     team_pe: Option<usize>,
/// }
///
/// #[lamellar::am]
/// impl LamellarAm for Am{
///     async fn exec(self) {
///         println!("Hello from world PE{:?}, team PE{:?}",self.world_pe, self.team_pe);
///     }
/// }
/// let world = LamellarWorldBuilder::new().build();
/// let num_pes = world.num_pes();
/// let world_pe = world.my_pe();
///
/// //create a team consisting of the "even" PEs in the world
/// let even_pes = world.create_team_from_arch(StridedArch::new(
///    0,                                      // start pe
///    2,                                      // stride
///    (num_pes as f64 / 2.0).ceil() as usize, //num_pes in team
/// )).expect("PE in world team");
/// let team_pe = match even_pes.team_pe_id(){
///     Ok(pe) => Some(pe),
///     Err(_) => None,
/// };
/// let task_group_1 = LamellarTaskGroup::new(&world); //associate the task group with the world
/// let task_group_2 = LamellarTaskGroup::new(&even_pes); //we can also associate the task group with a team/sub_team
/// task_group_1.exec_am_all(Am{world_pe,team_pe});
/// for pe in 0..even_pes.num_pes(){
///    task_group_2.exec_am_pe(pe,Am{world_pe,team_pe});
/// }
/// task_group_1.wait_all(); //only need to wait for active messages launched with task_group_1 to finish
/// //do interesting work
/// task_group_2.wait_all(); //only need to wait for active messages launched with task_group_2 to finish
/// ```
#[derive(Debug)]
pub struct LamellarTaskGroup {
    team: Pin<Arc<LamellarTeamRT>>,
    id: usize, //for exec_pe requests -- is actually the pointer to the rt_req (but *const are not sync so we use usize)
    multi_id: usize, //for exec_all requests -- is actually the pointer to the rt_multi_req  (but *const are not sync so we use usize)
    local_id: usize, //for exec_local requests -- is actually the pointer to the rt_local_req  (but *const are not sync so we use usize)
    sub_id_counter: AtomicUsize,
    cnt: Arc<AtomicUsize>, // handle reference count, so that we don't need to worry about storing results if all handles are dropped
    pub(crate) counters: AMCounters,
    //these are cloned and returned to user for each request
    req: Arc<TaskGroupRequestHandleInner>,
    multi_req: Arc<TaskGroupMultiRequestHandleInner>,
    local_req: Arc<TaskGroupLocalRequestHandleInner>,
    //these are cloned and passed to RT for each request (they wrap the above requests)
    rt_req: Arc<LamellarRequestResult>, //for exec_pe requests
    rt_multi_req: Arc<LamellarRequestResult>, //for exec_all requests
    rt_local_req: Arc<LamellarRequestResult>, //for exec_local requests
}

impl ActiveMessaging for LamellarTaskGroup {
    #[tracing::instrument(skip_all)]
    fn wait_all(&self) {
        self.wait_all();
    }

    #[tracing::instrument(skip_all)]
    fn barrier(&self) {
        self.team.barrier();
    }

    #[tracing::instrument(skip_all)]
    fn exec_am_all<F>(&self, am: F) -> Pin<Box<dyn Future<Output = Vec<F::Output>> + Send>>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        // trace!("[{:?}] team exec am all request", self.team.world_pe);
        self.exec_am_all_inner(am).into_future()
    }

    #[tracing::instrument(skip_all)]
    fn exec_am_pe<F>(&self, pe: usize, am: F) -> Pin<Box<dyn Future<Output = F::Output> + Send>>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        self.exec_am_pe_inner(pe, am).into_future()
    }

    #[tracing::instrument(skip_all)]
    fn exec_am_local<F>(&self, am: F) -> Pin<Box<dyn Future<Output = F::Output> + Send>>
    where
        F: LamellarActiveMessage + LocalAM + 'static,
    {
        self.exec_am_local_inner(am).into_future()
    }

    fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future,
    {
        tracing::trace_span!("block_on").in_scope(|| self.team.scheduler.block_on(f))
    }
}

impl LamellarTaskGroup {
    /// Construct a new Task group associated with the provided team
    ///
    /// # Examples
    ///
    ///```
    /// use lamellar::active_messaging::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let task_group = LamellarTaskGroup::new(&world); //associate the task group with the world
    ///```
    pub fn new<U: Into<IntoLamellarTeam>>(team: U) -> LamellarTaskGroup {
        let team = team.into().team.clone();
        let counters = AMCounters::new();
        let cnt = Arc::new(AtomicUsize::new(1)); //this lamellarTaskGroup instance represents 1 handle (even though we maintain a single and multi req handle)
        let req = Arc::new(TaskGroupRequestHandleInner {
            cnt: cnt.clone(),
            data: Mutex::new(HashMap::new()),
            team_outstanding_reqs: team.team_counters.outstanding_reqs.clone(),
            world_outstanding_reqs: team.world_counters.outstanding_reqs.clone(),
            tg_outstanding_reqs: Some(counters.outstanding_reqs.clone()),
        });
        let rt_req = Arc::new(LamellarRequestResult { req: req.clone() });
        let multi_req = Arc::new(TaskGroupMultiRequestHandleInner {
            cnt: cnt.clone(),
            arch: team.arch.clone(),
            data: Mutex::new(HashMap::new()),
            team_outstanding_reqs: team.team_counters.outstanding_reqs.clone(),
            world_outstanding_reqs: team.world_counters.outstanding_reqs.clone(),
            tg_outstanding_reqs: Some(counters.outstanding_reqs.clone()),
        });
        let rt_multi_req = Arc::new(LamellarRequestResult {
            req: multi_req.clone(),
        });
        let local_req = Arc::new(TaskGroupLocalRequestHandleInner {
            cnt: cnt.clone(),
            data: Mutex::new(HashMap::new()),
            team_outstanding_reqs: team.team_counters.outstanding_reqs.clone(),
            world_outstanding_reqs: team.world_counters.outstanding_reqs.clone(),
            tg_outstanding_reqs: Some(counters.outstanding_reqs.clone()),
        });
        let rt_local_req = Arc::new(LamellarRequestResult {
            req: local_req.clone(),
        });
        LamellarTaskGroup {
            team: team.clone(),
            id: Arc::as_ptr(&rt_req) as usize,
            multi_id: Arc::as_ptr(&rt_multi_req) as usize,
            local_id: Arc::as_ptr(&rt_local_req) as usize,
            sub_id_counter: AtomicUsize::new(0),
            cnt: cnt,
            counters: counters,
            req: req,
            multi_req: multi_req,
            local_req: local_req,
            rt_req: rt_req,
            rt_multi_req: rt_multi_req,
            rt_local_req: rt_local_req,
        }
    }

    fn wait_all(&self) {
        let mut temp_now = Instant::now();
        while self.counters.outstanding_reqs.load(Ordering::SeqCst) > 0 {
            self.team.scheduler.exec_task();
            if temp_now.elapsed() > Duration::new(600, 0) {
                println!(
                    "in task group wait_all mype: {:?} cnt: {:?} {:?}",
                    self.team.world_pe,
                    self.team.team_counters.send_req_cnt.load(Ordering::SeqCst),
                    self.team
                        .team_counters
                        .outstanding_reqs
                        .load(Ordering::SeqCst),
                );
                temp_now = Instant::now();
            }
        }
    }

    pub(crate) fn exec_am_all_inner<F>(
        &self,
        am: F,
    ) -> Box<dyn LamellarMultiRequest<Output = F::Output>>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        // println!("task group exec am all");
        self.team.team_counters.add_send_req(self.team.num_pes);
        self.team.world_counters.add_send_req(self.team.num_pes);
        self.counters.add_send_req(self.team.num_pes);
        // println!("cnts: t: {} w: {} self: {:?}",self.team.team_counters.outstanding_reqs.load(Ordering::Relaxed),self.team.world_counters.outstanding_reqs.load(Ordering::Relaxed), self.counters.outstanding_reqs.load(Ordering::Relaxed));

        self.cnt.fetch_add(1, Ordering::SeqCst);
        let func: LamellarArcAm = Arc::new(am);
        let world = if let Some(world) = &self.team.world {
            world.clone()
        } else {
            self.team.clone()
        };
        for _ in 0..(self.team.num_pes) {
            unsafe { Arc::increment_strong_count(Arc::as_ptr(&self.rt_multi_req)) }
            //each pe will return a result (which we turn back into an arc)
        }
        let req_id = ReqId {
            id: self.multi_id,
            sub_id: self.sub_id_counter.fetch_add(1, Ordering::SeqCst),
        };

        let req_data = ReqMetaData {
            src: self.team.world_pe,
            dst: None,
            id: req_id,
            lamellae: self.team.lamellae.clone(),
            world: world,
            team: self.team.clone(),
            team_addr: self.team.remote_ptr_addr,
        };
        self.team.scheduler.submit_am(Am::All(req_data, func));
        Box::new(TaskGroupMultiRequestHandle {
            inner: self.multi_req.clone(),
            sub_id: req_id.sub_id,
            _phantom: PhantomData,
        })
    }

    pub(crate) fn exec_am_pe_inner<F>(
        &self,
        pe: usize,
        am: F,
    ) -> Box<dyn LamellarRequest<Output = F::Output>>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        // println!("task group exec am pe");
        self.team.team_counters.add_send_req(1);
        self.team.world_counters.add_send_req(1);
        self.counters.add_send_req(1);
        // println!("cnts: t: {} w: {} self: {:?}",self.team.team_counters.outstanding_reqs.load(Ordering::Relaxed),self.team.world_counters.outstanding_reqs.load(Ordering::Relaxed), self.counters.outstanding_reqs.load(Ordering::Relaxed));

        self.cnt.fetch_add(1, Ordering::SeqCst);
        let func: LamellarArcAm = Arc::new(am);
        let world = if let Some(world) = &self.team.world {
            world.clone()
        } else {
            self.team.clone()
        };
        unsafe { Arc::increment_strong_count(Arc::as_ptr(&self.rt_req)) }
        let req_id = ReqId {
            id: self.id,
            sub_id: self.sub_id_counter.fetch_add(1, Ordering::SeqCst),
        };
        let req_data = ReqMetaData {
            src: self.team.world_pe,
            dst: Some(self.team.arch.world_pe(pe).expect("pe not member of team")),
            id: req_id,
            lamellae: self.team.lamellae.clone(),
            world: world,
            team: self.team.clone(),
            team_addr: self.team.remote_ptr_addr,
        };
        self.team.scheduler.submit_am(Am::Remote(req_data, func));
        Box::new(TaskGroupRequestHandle {
            inner: self.req.clone(),
            sub_id: req_id.sub_id,
            _phantom: PhantomData,
        })
    }

    pub(crate) fn exec_am_local_inner<F>(
        &self,
        am: F,
    ) -> Box<dyn LamellarRequest<Output = F::Output>>
    where
        F: LamellarActiveMessage + LocalAM + 'static,
    {
        // println!("task group exec am local");
        self.team.team_counters.add_send_req(1);
        self.team.world_counters.add_send_req(1);
        self.counters.add_send_req(1);
        // println!("cnts: t: {} w: {} self: {:?}",self.team.team_counters.outstanding_reqs.load(Ordering::Relaxed),self.team.world_counters.outstanding_reqs.load(Ordering::Relaxed), self.counters.outstanding_reqs.load(Ordering::Relaxed));

        self.cnt.fetch_add(1, Ordering::SeqCst);
        let func: LamellarArcLocalAm = Arc::new(am);
        let world = if let Some(world) = &self.team.world {
            world.clone()
        } else {
            self.team.clone()
        };
        unsafe { Arc::increment_strong_count(Arc::as_ptr(&self.rt_local_req)) }
        let req_id = ReqId {
            id: self.local_id,
            sub_id: self.sub_id_counter.fetch_add(1, Ordering::SeqCst),
        };
        let req_data = ReqMetaData {
            src: self.team.world_pe,
            dst: Some(self.team.world_pe),
            id: req_id,
            lamellae: self.team.lamellae.clone(),
            world: world,
            team: self.team.clone(),
            team_addr: self.team.remote_ptr_addr,
        };
        self.team.scheduler.submit_am(Am::Local(req_data, func));
        Box::new(TaskGroupLocalRequestHandle {
            inner: self.local_req.clone(),
            sub_id: req_id.sub_id,
            _phantom: PhantomData,
        })
    }
}

impl Drop for LamellarTaskGroup {
    fn drop(&mut self) {
        self.cnt.fetch_sub(1, Ordering::SeqCst);
    }
}


use std::borrow::Cow;
// LocalAM LamellarAM LamellarSerde LamellarResultSerde LamellarResultDarcSerde LamellarActiveMessage RemoteActiveMessage
// this is a special AM that embeds other AMs, we want to do some optimizations to avoid extra mem copies, and to satisfy various trait
// bounds related to serialization and deserialization so we must implement all the required active message traits manually...
struct TaskGroupAm<'a>{
    ams: Cow<'a,[LamellarArcAm]>
}

impl<'a> DarcSerde for TaskGroupAm<'a> {
    fn ser(&self,  num_pes: usize, darcs: &mut Vec<RemotePtr>){
        for am in self.ams.iter(){
            am.ser(num_pes,darcs);
        }
    }

    fn des(&self,_cur_pe: Result<usize,  crate::IdError>){
        // we dont actually do anything here, as each individual am will call its 
        // own des funcion during the deserialization of the TaskGroupAm 
    }
}


// impl Serde for TaskGroupAm {}

impl<'a> LocalAM for TaskGroupAm<'a> {
    type Output = Vec<Vec<u8>>;
}

#[async_trait::async_trait]
impl<'a> LamellarAM for TaskGroupAm<'a> {
    type Output =  Vec<Vec<u8>>;
    async fn exec(self) -> Self::Output{
        panic!("this should never be called")
    }
}

impl<'a> LamellarSerde for TaskGroupAm<'a> {
    fn serialized_size(&self)->usize{
        let mut size = 0;
        let id_size = crate::serialized_size(&AM_ID_START,true);
        for am in self.ams.iter(){
            size += id_size;
            size += am.serialized_size();
        }
        size
    }
    fn serialize_into(&self,buf: &mut [u8]){
        let mut i = 0;
        for am in self.ams.iter(){
            let id =  *(AMS_IDS.get(am.get_id()).unwrap());
            crate::serialize_into(&mut buf[i..],&id,true).unwrap();
            i += crate::serialized_size(&id,true);
            am.serialize_into(&mut buf[i..]);
            i += am.serialized_size();
        }
    }
    fn serialize(&self)->Vec<u8>{
        let ser_size = self.serialized_size();
        let mut data = vec![0;ser_size];
        self.serialize_into(&mut data);
        data
    }
}

impl<'a> LamellarResultSerde for TaskGroupAm<'a> {
    fn serialized_result_size(&self,result: & Box<dyn std::any::Any + Sync + Send>)->usize{
        let result  = result.downcast_ref::<Vec<Vec<u8>>>().unwrap();
        crate::serialized_size(result,true)
    }
    fn serialize_result_into(&self,buf: &mut [u8],result: & Box<dyn std::any::Any + Sync + Send>){
        let result  = result.downcast_ref::<Vec<Vec<u8>>>().unwrap();
        crate::serialize_into(buf,result,true).unwrap();
    }
}

impl<'a> LamellarActiveMessage for TaskGroupAm<'a> {
    fn exec(self: Arc<Self>,__lamellar_current_pe: usize, __lamellar_num_pes: usize, __local: bool, __lamellar_world: Arc<LamellarTeam>, __lamellar_team: Arc<LamellarTeam>) -> std::pin::Pin<Box<dyn std::future::Future<Output=LamellarReturn> + Send >>{
        Box::pin( async move {
            self.ams.iter().map(|e| {e.clone().exec(__lamellar_current_pe,__lamellar_num_pes,__local,__lamellar_world.clone(),__lamellar_team.clone())}).collect::<futures::stream::FuturesOrdered<_>>().collect::<Vec<_>>().await;
            LamellarReturn::RemoteData(Arc::new(TaskGroupAmReturn{val: vec![]}))
        })
    }

    fn get_id(&self) -> &'static str{
        "TaskGroupAm"
    }
}

impl<'a> RemoteActiveMessage for TaskGroupAm<'a> {
    fn as_local(self: Arc<Self>) -> Arc<dyn LamellarActiveMessage + Send + Sync>{
        self
    }
}


fn task_group_am_unpack(bytes: &[u8], cur_pe: Result<usize,crate::IdError>) -> std::sync::Arc<dyn RemoteActiveMessage + Sync + Send> {
    let mut i = 0;

    let id_size = crate::serialized_size(&AM_ID_START,true);
    let mut ams = Vec::new();
    
    while i < bytes.len() {
        let id: AmId = crate::deserialize(&bytes[i..i+id_size],true).unwrap();
        i += id_size;
        let am = AMS_EXECS.get(&id).unwrap()(&bytes[i..], cur_pe);
        i += am.serialized_size();
        ams.push(am);
    }
    let tg_am = TaskGroupAm{
        ams: Cow::Owned(ams)
    };
    <TaskGroupAm as DarcSerde>::des(&tg_am,cur_pe);
    Arc::new(tg_am)
}

crate::inventory::submit!{
    RegisteredAm{
        exec: task_group_am_unpack,
        name: "TaskGroupAm"
    }
}

#[lamellar_impl::AmDataRT]
struct TaskGroupAmReturn{
    val: Vec<Vec<u8>>
}

impl LamellarSerde for TaskGroupAmReturn {
    fn serialized_size(&self)->usize{
        crate::serialized_size(&self.val,true)
    }
    fn serialize_into(&self,buf: &mut [u8]){
        crate::serialize_into(buf,&self.val,true).unwrap();
    }
    fn serialize(&self)->Vec<u8>{
        crate::serialize(self,true).unwrap()
    }
}

impl LamellarResultDarcSerde for TaskGroupAmReturn{}



#[doc(hidden)]
pub struct TaskGroupFutures<'a> {
    team: Pin<Arc<LamellarTeamRT>>,
    cnt: usize,
    reqs: BTreeMap<usize,(Vec<usize>,Vec<LamellarArcAm>,usize)>,
    _lifetime: PhantomData<&'a ()>
}

impl<'a> TaskGroupFutures<'a>{
    pub fn new<U: Into<IntoLamellarTeam>>(team: U) -> TaskGroupFutures<'a> {
        TaskGroupFutures {
            team: team.into().team.clone(),
            cnt: 0,
            reqs: BTreeMap::new(),
        }

    }
    pub fn add_am_all<F>(&mut self, am: F) 
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        let req_queue = self.reqs.entry(self.team.num_pes).or_insert((Vec::new(),Vec::new(),0));
        req_queue.2 += am.serialized_size();
        req_queue.0.push(self.cnt);
        req_queue.1.push(Arc::new(am));
        
        self.cnt+=1;
    }

    pub fn add_am_pe<F>(&mut self, pe: usize, am: F)
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        let req_queue = self.reqs.entry(pe).or_insert((Vec::new(),Vec::new(),0));
        req_queue.2 += am.serialized_size();
        req_queue.0.push(self.cnt);
        req_queue.1.push(Arc::new(am));
        self.cnt+=1;
    }

    pub async fn exec(&self) {
        let mut reqs = vec![];
        // let mut all_req = None;
        let mut reqs_pes = vec![];
        for (pe,the_ams) in self.reqs.iter() {
            // let mut ams = vec![];
            // std::mem::swap(&mut ams,&mut the_ams.1);
            
            if *pe == self.team.num_pes{
                // all_req = Some(self.exec_am_all_inner(tg_am).into_future());
            }
            else{
                if the_ams.2 > 100_000_000{
                    let num_reqs = (the_ams.2 / 100_000_000) + 1;
                    let req_size = the_ams.2/num_reqs;
                    let mut temp_size = 0;
                    let mut i = 0;
                    let mut start_i = 0;
                    let mut send = false;
                    while i < the_ams.1.len() {
                        let am_size = the_ams.1[i].serialized_size();
                        if temp_size + am_size < 500_000_000 { //hard size limit
                            temp_size += am_size;
                            i+=1;
                            if temp_size > 100_000_000{
                                send = true
                            }
                        }
                        else {
                            send = true;
                        }
                        if send{
                            let tg_am = TaskGroupAm{ams: Cow::Borrowed(&the_ams.1[start_i..i])};
                            reqs.push(self.team.exec_arc_am_pe::<Vec<Vec<u8>>>(*pe,Arc::new(tg_am),None).into_future());
                            send = false;
                            start_i = i;
                            temp_size = 0;
                        }
                    }
                    if temp_size > 0 {
                        let tg_am = TaskGroupAm{ams: Cow::Borrowed(&the_ams.1[start_i..i])};
                        reqs.push(self.team.exec_arc_am_pe::<Vec<Vec<u8>>>(*pe,Arc::new(tg_am),None).into_future());
                    }
                }
                else{
                    let tg_am = TaskGroupAm{ams: Cow::Borrowed(&the_ams.1)};
                    reqs.push(self.team.exec_arc_am_pe::<Vec<Vec<u8>>>(*pe,Arc::new(tg_am),None).into_future());
                }
            }
            reqs_pes.push(pe);
        }
        futures::future::join_all(reqs).await;
        // if let Some(req) = all_req{
        //     req.await;
        // }

    }


}

//TODO create a typed task group which will require the exact am type

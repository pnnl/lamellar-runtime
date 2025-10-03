use crate::active_messaging::registered_active_message::{AmId, AMS_EXECS, AMS_IDS, AM_ID_START};
use crate::active_messaging::*;
use crate::barrier::BarrierHandle;
use crate::env_var::config;
use crate::lamellae::Des;
use crate::lamellar_arch::LamellarArchRT;
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_request::*;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeam, LamellarTeamRT};
use crate::memregion::one_sided::MemRegionHandleInner;
use crate::scheduler::{LamellarTask, ReqId, Scheduler};
use crate::warnings::RuntimeWarning;
use crate::Darc;

// use crossbeam::utils::CachePadded;
// use futures_util::StreamExt;

use futures_util::future::join_all;
use futures_util::{Future, StreamExt};
use parking_lot::Mutex;
use pin_project::{pin_project, pinned_drop};
use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

#[derive(Debug)]

pub(crate) struct TaskGroupAmHandleInner {
    cnt: Arc<AtomicUsize>,
    data: Mutex<HashMap<usize, InternalResult>>, //<sub_id, result>
    wakers: Mutex<HashMap<usize, Waker>>,
    team_counters: Arc<AMCounters>,
    world_counters: Arc<AMCounters>,
    tg_counters: Option<Arc<AMCounters>>,
    pub(crate) scheduler: Arc<Scheduler>,
    // pending_reqs: Arc<Mutex<HashSet<usize>>>,
}

//#[doc(hidden)]
#[derive(Debug)]
#[pin_project(PinnedDrop)]
pub struct TaskGroupAmHandle<T: AmDist> {
    inner: Arc<TaskGroupAmHandleInner>,
    am: Option<(Am, usize)>,
    sub_id: usize,
    _phantom: std::marker::PhantomData<T>,
}

#[pinned_drop]
impl<T: AmDist> PinnedDrop for TaskGroupAmHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if self.am.is_some() {
            RuntimeWarning::DroppedHandle("an TaskGroupAmHandle").print();
        }
        self.inner.cnt.fetch_sub(1, Ordering::SeqCst);
    }
}

impl LamellarRequestAddResult for TaskGroupAmHandleInner {
    fn user_held(&self) -> bool {
        self.cnt.load(Ordering::SeqCst) > 0
    }
    fn add_result(&self, _pe: usize, sub_id: usize, data: InternalResult) {
        self.data.lock().insert(sub_id, data);
        if let Some(waker) = self.wakers.lock().remove(&sub_id) {
            // println!("waker found for sub_id {}", sub_id);
            waker.wake();
        }
    }
    fn update_counters(&self, _sub_id: usize) {
        self.team_counters.dec_outstanding(1);
        self.world_counters.dec_outstanding(1);
        if let Some(tg_counters) = self.tg_counters.clone() {
            tg_counters.dec_outstanding(1);
        }
    }
}

impl<T: AmDist> TaskGroupAmHandle<T> {
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

    fn launch_am_if_needed(&mut self) {
        if let Some((am, num_pes)) = self.am.take() {
            self.inner.team_counters.inc_outstanding(num_pes);
            self.inner.team_counters.inc_launched(num_pes);
            self.inner.world_counters.inc_outstanding(num_pes);
            self.inner.world_counters.inc_launched(num_pes);
            if let Some(tg_counters) = self.inner.tg_counters.clone() {
                tg_counters.inc_outstanding(num_pes);
                tg_counters.inc_launched(num_pes);
            }

            self.inner.scheduler.submit_am(am);
        }
    }

    /// This method will spawn the associated Active Message on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. If ignored/dropped the only way to ensure completion is calling 'wait_all()' on the world or array"]
    pub fn spawn(mut self) -> LamellarTask<T> {
        self.launch_am_if_needed();
        self.inner.scheduler.clone().spawn_task(self, Vec::new()) //counters managed by AM
    }
    /// This method will block the calling thread until the associated Array Operation completes
    pub fn block(mut self) -> T {
        RuntimeWarning::BlockingCall(
            "TaskGroupAmHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.launch_am_if_needed();
        self.inner.scheduler.clone().block_on(self)
    }
}

impl<T: AmDist> LamellarRequest for TaskGroupAmHandle<T> {
    fn launch(&mut self) {
        self.launch_am_if_needed();
    }
    fn blocking_wait(mut self) -> Self::Output {
        self.launch_am_if_needed();
        let mut res = self.inner.data.lock().remove(&self.sub_id);
        while res.is_none() {
            self.inner.scheduler.exec_task();
            res = self.inner.data.lock().remove(&self.sub_id);
        }
        self.process_result(res.expect("result should exist"))
    }

    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        self.launch_am_if_needed();
        let data = self.inner.data.lock();
        if data.contains_key(&self.sub_id) {
            true
        } else {
            self.inner.wakers.lock().insert(self.sub_id, waker.clone());
            self.inner
                .wakers
                .lock()
                .entry(self.sub_id)
                .and_modify(|w| {
                    if !w.will_wake(waker) {
                        println!("WARNING: overwriting waker {:?}", w);
                        w.wake_by_ref();
                    }
                    w.clone_from(waker);
                })
                .or_insert(waker.clone());
            false
        }
    }

    fn val(&self) -> Self::Output {
        let res = self
            .inner
            .data
            .lock()
            .remove(&self.sub_id)
            .expect("result should exist");
        self.process_result(res)
    }
}

impl<T: AmDist> Future for TaskGroupAmHandle<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launch_am_if_needed();
        let mut this = self.as_mut();
        if this.ready_or_set_waker(cx.waker()) {
            Poll::Ready(
                this.process_result(
                    this.inner
                        .data
                        .lock()
                        .remove(&this.sub_id)
                        .expect("result should exist"),
                ),
            )
        } else {
            Poll::Pending
        }
    }
}

#[derive(Debug)]
pub(crate) struct TaskGroupMultiAmHandleInner {
    cnt: Arc<AtomicUsize>,
    arch: Arc<LamellarArchRT>,
    data: Mutex<HashMap<usize, HashMap<usize, InternalResult>>>, //<sub_id, <pe, result>>
    wakers: Mutex<HashMap<usize, Waker>>,
    team_counters: Arc<AMCounters>,
    world_counters: Arc<AMCounters>,
    tg_counters: Option<Arc<AMCounters>>,
    pub(crate) scheduler: Arc<Scheduler>,
    // pending_reqs: Arc<Mutex<HashSet<usize>>>,
}

//#[doc(hidden)]
#[derive(Debug)]
#[pin_project(PinnedDrop)]
pub struct TaskGroupMultiAmHandle<T: AmDist> {
    inner: Arc<TaskGroupMultiAmHandleInner>,
    am: Option<(Am, usize)>,
    sub_id: usize,
    _phantom: std::marker::PhantomData<T>,
}

#[pinned_drop]
impl<T: AmDist> PinnedDrop for TaskGroupMultiAmHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if self.am.is_some() {
            RuntimeWarning::DroppedHandle("an TaskGroupMultiAmHandle").print();
        }
        self.inner.cnt.fetch_sub(1, Ordering::SeqCst);
    }
}

impl LamellarRequestAddResult for TaskGroupMultiAmHandleInner {
    fn user_held(&self) -> bool {
        self.cnt.load(Ordering::SeqCst) > 0
    }
    fn add_result(&self, pe: usize, sub_id: usize, data: InternalResult) {
        let pe = self.arch.team_pe(pe).expect("pe does not exist on team");
        let mut map = self.data.lock(); //.insert(pe, data);

        let reqs = map.entry(sub_id).or_insert_with(|| HashMap::new());
        reqs.insert(pe, data);

        if reqs.len() == self.arch.num_pes() {
            if let Some(waker) = self.wakers.lock().remove(&sub_id) {
                // println!("0. waker found for sub_id {}", sub_id);
                waker.wake();
            }
            // else {
            //     println!("0. no waker found for sub_id {}", sub_id);
            // }
        } else {
            if let Some(waker) = self.wakers.lock().get(&sub_id) {
                // println!("1. waker found for sub_id {}", sub_id);
                waker.wake_by_ref();
            }
            //  else {
            //     println!("1. no waker found for sub_id {}", sub_id);
            // }
        }
    }
    fn update_counters(&self, _sub_id: usize) {
        self.team_counters.dec_outstanding(1);
        self.world_counters.dec_outstanding(1);
        if let Some(tg_counters) = self.tg_counters.clone() {
            tg_counters.dec_outstanding(1);
        }
    }
}

impl<T: AmDist> TaskGroupMultiAmHandle<T> {
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

    fn launch_am_if_needed(&mut self) {
        if let Some((am, num_pes)) = self.am.take() {
            self.inner.team_counters.inc_outstanding(num_pes);
            self.inner.team_counters.inc_launched(num_pes);
            self.inner.world_counters.inc_outstanding(num_pes);
            self.inner.world_counters.inc_launched(num_pes);
            if let Some(tg_counters) = self.inner.tg_counters.clone() {
                tg_counters.inc_outstanding(num_pes);
                tg_counters.inc_launched(num_pes);
            }
            self.inner.scheduler.submit_am(am);
        }
    }

    /// This method will spawn the associated Active Message on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. If ignored/dropped the only way to ensure completion is calling 'wait_all()' on the world or array"]
    pub fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.launch_am_if_needed();
        self.inner.scheduler.clone().spawn_task(self, Vec::new()) //counters managed by AM
    }
    /// This method will block the calling thread until the associated Array Operation completes
    pub fn block(mut self) -> Vec<T> {
        RuntimeWarning::BlockingCall(
            "TaskGroupMultiAmHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.launch_am_if_needed();
        self.inner.scheduler.clone().block_on(self)
    }
}

impl<T: AmDist> LamellarRequest for TaskGroupMultiAmHandle<T> {
    fn launch(&mut self) {
        self.launch_am_if_needed();
    }
    fn blocking_wait(mut self) -> Self::Output {
        self.launch_am_if_needed();
        while !self.inner.data.lock().contains_key(&self.sub_id) {
            self.inner.scheduler.exec_task();
        }
        while self
            .inner
            .data
            .lock()
            .get(&self.sub_id)
            .expect("req sub id should exist")
            .len()
            < self.inner.arch.num_pes()
        {
            self.inner.scheduler.exec_task();
        }
        let mut sub_id_map = self
            .inner
            .data
            .lock()
            .remove(&self.sub_id)
            .expect("req sub id should exist");
        let mut res = Vec::new();
        for pe in 0..sub_id_map.len() {
            res.push(
                self.process_result(sub_id_map.remove(&pe).expect("pe id should exist still")),
            );
        }
        res
    }

    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        self.launch_am_if_needed();
        let data = self.inner.data.lock();
        let mut ready = false;
        if let Some(req) = data.get(&self.sub_id) {
            ready = req.len() == self.inner.arch.num_pes();
        }
        if !ready {
            // println!("setting waker for sub_id {}", self.sub_id);
            self.inner.wakers.lock().insert(self.sub_id, waker.clone());
            self.inner
                .wakers
                .lock()
                .entry(self.sub_id)
                .and_modify(|w| {
                    if !w.will_wake(waker) {
                        println!("WARNING: overwriting waker {:?}", w);
                        w.wake_by_ref();
                    }
                    w.clone_from(waker);
                })
                .or_insert(waker.clone());
        }
        ready
    }

    fn val(&self) -> Self::Output {
        let mut sub_id_map = self
            .inner
            .data
            .lock()
            .remove(&self.sub_id)
            .expect("req sub id should exist");
        let mut res = Vec::new();
        for pe in 0..sub_id_map.len() {
            res.push(
                self.process_result(sub_id_map.remove(&pe).expect("pe id should exist still")),
            );
        }
        res
    }
}

impl<T: AmDist> Future for TaskGroupMultiAmHandle<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launch_am_if_needed();
        let mut this = self.as_mut();
        if this.ready_or_set_waker(cx.waker()) {
            let mut sub_id_map = this
                .inner
                .data
                .lock()
                .remove(&this.sub_id)
                .expect("req sub id should exist");
            let mut res = Vec::new();
            for pe in 0..sub_id_map.len() {
                res.push(
                    this.process_result(sub_id_map.remove(&pe).expect("pe id should exist still")),
                );
            }
            Poll::Ready(res)
        } else {
            Poll::Pending
        }
    }
}

//#[doc(hidden)]
#[derive(Debug)]
#[pin_project(PinnedDrop)]
pub struct TaskGroupLocalAmHandle<T> {
    inner: Arc<TaskGroupAmHandleInner>,
    am: Option<(Am, usize)>,
    sub_id: usize,
    _phantom: std::marker::PhantomData<T>,
}

#[pinned_drop]
impl<T> PinnedDrop for TaskGroupLocalAmHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if self.am.is_some() {
            RuntimeWarning::DroppedHandle("an TaskGroupLocalAmHandle").print();
        }
        self.inner.cnt.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<T: 'static> TaskGroupLocalAmHandle<T> {
    fn process_result(&self, data: InternalResult) -> T {
        match data {
            InternalResult::Local(x) => {
                if let Ok(result) = x.downcast::<T>() {
                    *result
                } else {
                    panic!("unexpected result type");
                }
            }
            InternalResult::Remote(_result, _darcs) => {
                panic!("unexpected remote result  of type within local am handle");
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

    fn launch_am_if_needed(&mut self) {
        if let Some((am, num_pes)) = self.am.take() {
            self.inner.team_counters.inc_outstanding(num_pes);
            self.inner.team_counters.inc_launched(num_pes);
            self.inner.world_counters.inc_outstanding(num_pes);
            self.inner.world_counters.inc_launched(num_pes);
            if let Some(tg_counters) = self.inner.tg_counters.clone() {
                tg_counters.inc_outstanding(num_pes);
                tg_counters.inc_launched(num_pes);
            }
            self.inner.scheduler.submit_am(am);
        }
    }
}

impl<T: Send + 'static> TaskGroupLocalAmHandle<T> {
    /// This method will spawn the associated Active Message on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. If ignored/dropped the only way to ensure completion is calling 'wait_all()' on the world or array"]
    pub fn spawn(mut self) -> LamellarTask<T> {
        self.launch_am_if_needed();
        self.inner.scheduler.clone().spawn_task(self, Vec::new()) //counters managed by AM
    }
    /// This method will block the calling thread until the associated Array Operation completes
    pub fn block(mut self) -> T {
        RuntimeWarning::BlockingCall(
            "TaskGroupLocalAmHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.launch_am_if_needed();
        self.inner.scheduler.clone().block_on(self)
    }
}

impl<T: 'static> LamellarRequest for TaskGroupLocalAmHandle<T> {
    fn launch(&mut self) {
        self.launch_am_if_needed();
    }
    fn blocking_wait(mut self) -> Self::Output {
        self.launch_am_if_needed();
        let mut res = self.inner.data.lock().remove(&self.sub_id);
        while res.is_none() {
            self.inner.scheduler.exec_task();
            res = self.inner.data.lock().remove(&self.sub_id);
        }
        self.process_result(res.expect("result should exist"))
    }

    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        self.launch_am_if_needed();
        let data = self.inner.data.lock();
        if data.contains_key(&self.sub_id) {
            // println!("request ready {:?}", self.sub_id);
            true
        } else {
            // println!("request not ready setting waker {:?}", self.sub_id);
            //this can probably be optimized similar to set_waker of MultiAmHandle
            // where we check if the waker already exists and if it wakes to same task
            self.inner.wakers.lock().insert(self.sub_id, waker.clone());
            false
        }
    }

    fn val(&self) -> Self::Output {
        let res = self
            .inner
            .data
            .lock()
            .remove(&self.sub_id)
            .expect("result should exist");
        self.process_result(res)
    }
}

impl<T: 'static> Future for TaskGroupLocalAmHandle<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launch_am_if_needed();
        let mut this = self.as_mut();
        if this.ready_or_set_waker(cx.waker()) {
            Poll::Ready(
                this.process_result(
                    this.inner
                        .data
                        .lock()
                        .remove(&this.sub_id)
                        .expect("result should exist"),
                ),
            )
        } else {
            Poll::Pending
        }
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
/// struct MyAm{
///     world_pe: usize,
///     team_pe: Option<usize>,
/// }
///
/// #[lamellar::am]
/// impl LamellarAm for MyAm{
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
/// let _ = task_group_1.exec_am_all(MyAm{world_pe,team_pe}).spawn();
/// for pe in 0..even_pes.num_pes(){
///    let _ = task_group_2.exec_am_pe(pe,MyAm{world_pe,team_pe}).spawn();
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
    pub(crate) counters: Arc<AMCounters>,
    //these are cloned and returned to user for each request
    req: Arc<TaskGroupAmHandleInner>,
    multi_req: Arc<TaskGroupMultiAmHandleInner>,
    local_req: Arc<TaskGroupAmHandleInner>,
    //these are cloned and passed to RT for each request (they wrap the above requests)
    rt_req: Arc<LamellarRequestResult>, //for exec_pe requests
    rt_multi_req: Arc<LamellarRequestResult>, //for exec_all requests
    rt_local_req: Arc<LamellarRequestResult>, //for exec_local requests

                                        // pub(crate) pending_reqs: Arc<Mutex<HashSet<usize>>>,
}

impl ActiveMessaging for LamellarTaskGroup {
    type SinglePeAmHandle<R: AmDist> = TaskGroupAmHandle<R>;
    type MultiAmHandle<R: AmDist> = TaskGroupMultiAmHandle<R>;
    type LocalAmHandle<L> = TaskGroupLocalAmHandle<L>;

    //#[tracing::instrument(skip_all)]
    fn wait_all(&self) {
        self.wait_all();
    }

    fn await_all(&self) -> impl std::future::Future<Output = ()> + Send {
        self.await_all()
    }

    //#[tracing::instrument(skip_all)]
    fn barrier(&self) {
        self.team.barrier();
    }

    fn async_barrier(&self) -> BarrierHandle {
        self.team.async_barrier()
    }

    //#[tracing::instrument(skip_all)]
    fn exec_am_all<F>(&self, am: F) -> Self::MultiAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        // trace!("[{:?}] team exec am all request", self.team.world_pe);
        self.exec_am_all_inner(am)
    }

    //#[tracing::instrument(skip_all)]
    fn exec_am_pe<F>(&self, pe: usize, am: F) -> Self::SinglePeAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        self.exec_am_pe_inner(pe, am)
    }

    //#[tracing::instrument(skip_all)]
    fn exec_am_local<F>(&self, am: F) -> Self::LocalAmHandle<F::Output>
    where
        F: LamellarActiveMessage + LocalAM + 'static,
    {
        self.exec_am_local_inner(am)
    }

    fn spawn<F>(&self, task: F) -> LamellarTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        self.team.scheduler.spawn_task(
            task,
            vec![
                self.team.world_counters.clone(),
                self.team.team_counters.clone(),
                self.counters.clone(),
            ],
        )
    }
    fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future,
    {
        // tracing::trace_span!("block_on").in_scope(||
        self.team.scheduler.block_on(f)
        // )
    }
    fn block_on_all<I>(&self, iter: I) -> Vec<<<I as IntoIterator>::Item as Future>::Output>
    where
        I: IntoIterator,
        <I as IntoIterator>::Item: Future + Send + 'static,
        <<I as IntoIterator>::Item as Future>::Output: Send,
    {
        self.team
            .scheduler
            .block_on(join_all(iter.into_iter().map(|task| {
                self.team.scheduler.spawn_task(
                    task,
                    vec![
                        self.team.world_counters.clone(),
                        self.team.team_counters.clone(),
                        self.counters.clone(),
                    ],
                )
            })))
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
        let counters = Arc::new(AMCounters::new());
        let cnt = Arc::new(AtomicUsize::new(1)); //this lamellarTaskGroup instance represents 1 handle (even though we maintain a single and multi req handle)
                                                 // let pending_reqs = Arc::new(Mutex::new(HashSet::new()));
        let req = Arc::new(TaskGroupAmHandleInner {
            cnt: cnt.clone(),
            data: Mutex::new(HashMap::new()),
            wakers: Mutex::new(HashMap::new()),
            team_counters: team.team_counters.clone(),
            world_counters: team.world_counters.clone(),
            tg_counters: Some(counters.clone()),
            scheduler: team.scheduler.clone(),
            // pending_reqs: pending_reqs.clone(),
        });
        let rt_req = Arc::new(LamellarRequestResult::TgAm(req.clone()));
        let multi_req = Arc::new(TaskGroupMultiAmHandleInner {
            cnt: cnt.clone(),
            arch: team.arch.clone(),
            data: Mutex::new(HashMap::new()),
            wakers: Mutex::new(HashMap::new()),
            team_counters: team.team_counters.clone(),
            world_counters: team.world_counters.clone(),
            tg_counters: Some(counters.clone()),
            scheduler: team.scheduler.clone(),
            // pending_reqs: pending_reqs.clone(),
        });
        let rt_multi_req = Arc::new(LamellarRequestResult::TgMultiAm(multi_req.clone()));
        let local_req = Arc::new(TaskGroupAmHandleInner {
            cnt: cnt.clone(),
            data: Mutex::new(HashMap::new()),
            wakers: Mutex::new(HashMap::new()),
            team_counters: team.team_counters.clone(),
            world_counters: team.world_counters.clone(),
            tg_counters: Some(counters.clone()),
            scheduler: team.scheduler.clone(),
            // pending_reqs: pending_reqs.clone(),
        });
        let rt_local_req = Arc::new(LamellarRequestResult::TgAm(local_req.clone()));
        LamellarTaskGroup {
            team: team.clone(),
            id: Arc::as_ptr(&rt_req) as usize,
            multi_id: Arc::as_ptr(&rt_multi_req) as usize,
            local_id: Arc::as_ptr(&rt_local_req) as usize,
            sub_id_counter: AtomicUsize::new(0),
            cnt,
            counters,
            req,
            multi_req,
            local_req,
            rt_req,
            rt_multi_req,
            rt_local_req,
            // pending_reqs: pending_reqs,
        }
    }

    fn wait_all(&self) {
        RuntimeWarning::BlockingCall("wait_all", "await_all().await").print();
        // println!(
        //     "in task group wait_all mype: {:?} cnt: {:?} {:?} {:?}",
        //     self.team.world_pe,
        //     self.counters.send_req_cnt.load(Ordering::SeqCst),
        //     self.counters.outstanding_reqs.load(Ordering::SeqCst),
        //     self.counters.launched_req_cnt.load(Ordering::SeqCst)
        // );
        let mut temp_now = Instant::now();
        let mut orig_reqs = self.counters.send_req_cnt.load(Ordering::SeqCst);
        let mut orig_launched = self.counters.launched_req_cnt.load(Ordering::SeqCst);
        let mut done = false;
        while !done {
            while self.team.panic.load(Ordering::SeqCst) == 0
                && ((self.counters.outstanding_reqs.load(Ordering::SeqCst) > 0)
                    || orig_reqs != self.counters.send_req_cnt.load(Ordering::SeqCst)
                    || orig_launched != self.counters.launched_req_cnt.load(Ordering::SeqCst))
            {
                orig_reqs = self.counters.send_req_cnt.load(Ordering::SeqCst);
                orig_launched = self.counters.launched_req_cnt.load(Ordering::SeqCst);
                // self.team.flush();
                if std::thread::current().id() == *crate::MAIN_THREAD {
                    self.team.scheduler.exec_task();
                }
                if temp_now.elapsed().as_secs_f64() > config().deadlock_timeout {
                    println!(
                    "in task group wait_all mype: {:?} cnt: team {:?} team {:?} tg {:?} tg {:?}",
                    self.team.world_pe,
                    self.team.team_counters.send_req_cnt.load(Ordering::SeqCst),
                    self.team
                        .team_counters
                        .outstanding_reqs
                        .load(Ordering::SeqCst),
                    self.counters.send_req_cnt.load(Ordering::SeqCst),
                    self.counters.outstanding_reqs.load(Ordering::SeqCst),
                    // self.pending_reqs.lock()
                );
                    self.team.scheduler.print_status();
                    temp_now = Instant::now();
                }
            }
            if self.counters.send_req_cnt.load(Ordering::SeqCst)
                != self.counters.launched_req_cnt.load(Ordering::SeqCst)
            {
                if self.counters.outstanding_reqs.load(Ordering::SeqCst) > 0
                    || orig_reqs != self.counters.send_req_cnt.load(Ordering::SeqCst)
                    || orig_launched != self.counters.launched_req_cnt.load(Ordering::SeqCst)
                {
                    continue;
                }
                println!(
                    "in task group wait_all mype: {:?} cnt: {:?} {:?} {:?}",
                    self.team.world_pe,
                    self.counters.send_req_cnt.load(Ordering::SeqCst),
                    self.counters.outstanding_reqs.load(Ordering::SeqCst),
                    self.counters.launched_req_cnt.load(Ordering::SeqCst)
                );
                RuntimeWarning::UnspawnedTask(
                            "`wait_all` on an active message group before all tasks/active messages create by the group have been spawned",
                        )
                        .print();
            }
            done = true;
        }
    }

    async fn await_all(&self) {
        let mut temp_now = Instant::now();
        let mut orig_reqs = self.counters.send_req_cnt.load(Ordering::SeqCst);
        let mut orig_launched = self.counters.launched_req_cnt.load(Ordering::SeqCst);
        let mut done = false;
        while !done {
            while self.team.panic.load(Ordering::SeqCst) == 0
                && ((self.counters.outstanding_reqs.load(Ordering::SeqCst) > 0)
                    || orig_reqs != self.counters.send_req_cnt.load(Ordering::SeqCst)
                    || orig_launched != self.counters.launched_req_cnt.load(Ordering::SeqCst))
            {
                orig_reqs = self.counters.send_req_cnt.load(Ordering::SeqCst);
                orig_launched = self.counters.launched_req_cnt.load(Ordering::SeqCst);
                // self.team.flush();
                async_std::task::yield_now().await;
                if temp_now.elapsed().as_secs_f64() > config().deadlock_timeout {
                    println!(
                    "in task group wait_all mype: {:?} cnt: team {:?} team {:?} tg {:?} tg {:?}",
                    self.team.world_pe,
                    self.team.team_counters.send_req_cnt.load(Ordering::SeqCst),
                    self.team
                        .team_counters
                        .outstanding_reqs
                        .load(Ordering::SeqCst),
                    self.counters.send_req_cnt.load(Ordering::SeqCst),
                    self.counters.outstanding_reqs.load(Ordering::SeqCst),
                    // self.pending_reqs.lock()
                );
                    self.team.scheduler.print_status();
                    temp_now = Instant::now();
                }
            }
            if self.counters.send_req_cnt.load(Ordering::SeqCst)
                != self.counters.launched_req_cnt.load(Ordering::SeqCst)
            {
                if self.counters.outstanding_reqs.load(Ordering::SeqCst) > 0
                    || orig_reqs != self.counters.send_req_cnt.load(Ordering::SeqCst)
                    || orig_launched != self.counters.launched_req_cnt.load(Ordering::SeqCst)
                {
                    continue;
                }
                println!(
                    "in task group wait_all mype: {:?} cnt: {:?} {:?} {:?}",
                    self.team.world_pe,
                    self.counters.send_req_cnt.load(Ordering::SeqCst),
                    self.counters.outstanding_reqs.load(Ordering::SeqCst),
                    self.counters.launched_req_cnt.load(Ordering::SeqCst)
                );
                RuntimeWarning::UnspawnedTask(
                            "`wait_all` on an active message group before all tasks/active messages create by the group have been spawned",
                        )
                        .print();
            }
            done = true;
        }
    }

    pub(crate) fn exec_am_all_inner<F>(&self, am: F) -> TaskGroupMultiAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        // println!("task group exec am all");
        self.team.team_counters.inc_send_req(self.team.num_pes);
        self.team.world_counters.inc_send_req(self.team.num_pes);
        self.counters.inc_send_req(self.team.num_pes);
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
        // self.pending_reqs.lock().insert(req_id.sub_id);

        let req_data = ReqMetaData {
            src: self.team.world_pe,
            dst: None,
            id: req_id,
            lamellae: self.team.lamellae.clone(),
            world,
            team: self.team.clone(),
            team_addr: self.team.remote_ptr_addr,
        };
        // println!("[{:?}] task group am all", std::thread::current().id());
        // self.team.scheduler.submit_am();
        TaskGroupMultiAmHandle {
            inner: self.multi_req.clone(),
            am: Some((Am::All(req_data, func), self.team.num_pes)),
            sub_id: req_id.sub_id,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn exec_am_pe_inner<F>(&self, pe: usize, am: F) -> TaskGroupAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        // println!("task group exec am pe");
        self.team.team_counters.inc_send_req(1);
        self.team.world_counters.inc_send_req(1);
        self.counters.inc_send_req(1);
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
        // self.pending_reqs.lock().insert(req_id.sub_id);
        let req_data = ReqMetaData {
            src: self.team.world_pe,
            dst: Some(self.team.arch.world_pe(pe).expect("pe not member of team")),
            id: req_id,
            lamellae: self.team.lamellae.clone(),
            world,
            team: self.team.clone(),
            team_addr: self.team.remote_ptr_addr,
        };
        // println!("[{:?}] task group am pe", std::thread::current().id());
        // self.team.scheduler.submit_am(Am::Remote(req_data, func));
        TaskGroupAmHandle {
            inner: self.req.clone(),
            am: Some((Am::Remote(req_data, func), 1)),
            sub_id: req_id.sub_id,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn exec_am_local_inner<F>(&self, am: F) -> TaskGroupLocalAmHandle<F::Output>
    where
        F: LamellarActiveMessage + LocalAM + 'static,
    {
        self.exec_arc_am_local_inner(Arc::new(am))
    }

    pub(crate) fn exec_arc_am_local_inner<O: SyncSend + 'static>(
        &self,
        func: LamellarArcLocalAm,
    ) -> TaskGroupLocalAmHandle<O> {
        // println!("task group exec am local");
        self.team.team_counters.inc_send_req(1);
        self.team.world_counters.inc_send_req(1);
        self.counters.inc_send_req(1);
        // println!("cnts: t: {} w: {} self: {:?}",self.team.team_counters.outstanding_reqs.load(Ordering::Relaxed),self.team.world_counters.outstanding_reqs.load(Ordering::Relaxed), self.counters.outstanding_reqs.load(Ordering::Relaxed));

        self.cnt.fetch_add(1, Ordering::SeqCst);
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
        // self.pending_reqs.lock().insert(req_id.sub_id);
        let req_data = ReqMetaData {
            src: self.team.world_pe,
            dst: Some(self.team.world_pe),
            id: req_id,
            lamellae: self.team.lamellae.clone(),
            world,
            team: self.team.clone(),
            team_addr: self.team.remote_ptr_addr,
        };
        // println!("[{:?}] task group am local", std::thread::current().id());
        // self.team.scheduler.submit_am(Am::Local(req_data, func));
        // Box::new(TaskGroupLocalAmHandle {
        //     inner: self.local_req.clone(),
        //     sub_id: req_id.sub_id,
        //     _phantom: PhantomData,
        // })
        TaskGroupLocalAmHandle {
            inner: self.local_req.clone(),
            am: Some((Am::Local(req_data, func), 1)),
            sub_id: req_id.sub_id,
            _phantom: PhantomData,
        }
    }
}

impl Drop for LamellarTaskGroup {
    fn drop(&mut self) {
        self.cnt.fetch_sub(1, Ordering::SeqCst);
    }
}

// LocalAM LamellarAM LamellarSerde LamellarResultSerde LamellarResultDarcSerde LamellarActiveMessage RemoteActiveMessage
// this is a special AM that embeds other AMs, we want to do some optimizations to avoid extra mem copies, and to satisfy various trait
// bounds related to serialization and deserialization so we must implement all the required active message traits manually...
struct AmGroupAm {
    ams: Arc<Vec<LamellarArcAm>>,
    si: usize,
    ei: usize,
}

impl DarcSerde for AmGroupAm {
    fn ser(&self, num_pes: usize, darcs: &mut Vec<RemotePtr>) {
        // println!("task group ser");
        for am in &self.ams[self.si..self.ei] {
            // println!("task group am ser");
            (*am).ser(num_pes, darcs);
        }
    }

    fn des(&self, _cur_pe: Result<usize, crate::IdError>) {
        // println!("task group des");
        // we dont actually do anything here, as each individual am will call its
        // own des function during the deserialization of the AmGroupAm
    }
}

impl LocalAM for AmGroupAm {
    type Output = Vec<Vec<u8>>;
}

#[async_trait::async_trait]
impl LamellarAM for AmGroupAm {
    type Output = Vec<Vec<u8>>;
    async fn exec(self) -> Self::Output {
        panic!("this should never be called")
    }
}

impl LamellarSerde for AmGroupAm {
    fn serialized_size(&self) -> usize {
        let mut size = 0;
        size += crate::serialized_size(&0usize, true);
        let id_size = crate::serialized_size(&AM_ID_START, true);
        for am in &self.ams[self.si..self.ei] {
            size += id_size;
            size += am.serialized_size();
        }
        size
    }
    fn serialize_into(&self, buf: &mut [u8]) {
        let mut i = 0;
        // let timer = std::time::Instant::now();
        let num = self.ei - self.si;
        crate::serialize_into(&mut buf[i..], &num, true).unwrap();
        i += crate::serialized_size(&num, true);
        for am in &self.ams[self.si..self.ei] {
            let id = *(AMS_IDS.get(am.get_id()).unwrap());
            crate::serialize_into(&mut buf[i..], &id, true).unwrap();
            i += crate::serialized_size(&id, true);
            am.serialize_into(&mut buf[i..]);
            i += am.serialized_size();
        }
        // println!("serialize time: {:?} elem cnt {:?} ({}-{})", timer.elapsed().as_secs_f64(),self.ei-self.si,self.ei,self.si);
    }
    fn serialize(&self) -> Vec<u8> {
        let ser_size = self.serialized_size();
        let mut data = vec![0; ser_size];
        self.serialize_into(&mut data);
        data
    }
}

impl LamellarResultSerde for AmGroupAm {
    fn serialized_result_size(&self, result: &Box<dyn std::any::Any + Sync + Send>) -> usize {
        let result = result.downcast_ref::<Vec<Vec<u8>>>().unwrap();
        crate::serialized_size(result, true)
    }
    fn serialize_result_into(&self, buf: &mut [u8], result: &Box<dyn std::any::Any + Sync + Send>) {
        let result = result.downcast_ref::<Vec<Vec<u8>>>().unwrap();
        crate::serialize_into(buf, result, true).unwrap();
    }
}

impl LamellarActiveMessage for AmGroupAm {
    fn exec(
        self: Arc<Self>,
        __lamellar_current_pe: usize,
        __lamellar_num_pes: usize,
        __local: bool,
        __lamellar_world: Arc<LamellarTeam>,
        __lamellar_team: Arc<LamellarTeam>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = LamellarReturn> + Send>> {
        Box::pin(async move {
            // let timer = std::time::Instant::now();
            self.ams[self.si..self.ei]
                .iter()
                .map(|e| {
                    e.clone().exec(
                        __lamellar_current_pe,
                        __lamellar_num_pes,
                        __local,
                        __lamellar_world.clone(),
                        __lamellar_team.clone(),
                    )
                })
                .collect::<futures_util::stream::FuturesOrdered<_>>()
                .collect::<Vec<_>>()
                .await;
            // for am in self.ams[self.si..self.ei].iter() {
            //     am.clone().exec(__lamellar_current_pe,__lamellar_num_pes,__local,__lamellar_world.clone(),__lamellar_team.clone()).await;
            // }
            // println!("exec time: {:?}", timer.elapsed().as_secs_f64());
            match __local {
                false => LamellarReturn::RemoteData(Arc::new(AmGroupAmReturn { val: vec![] })),
                true => LamellarReturn::LocalData(Box::<Vec<Vec<u8>>>::new(vec![])),
            }
        })
    }

    fn get_id(&self) -> &'static str {
        "AmGroupAm"
    }
}

impl RemoteActiveMessage for AmGroupAm {
    fn as_local(self: Arc<Self>) -> Arc<dyn LamellarActiveMessage + Send + Sync> {
        self
    }
}

fn am_group_am_unpack(
    bytes: &[u8],
    cur_pe: Result<usize, crate::IdError>,
) -> std::sync::Arc<dyn RemoteActiveMessage + Sync + Send> {
    let mut i = 0;

    let id_size = crate::serialized_size(&AM_ID_START, true);
    let mut ams = Vec::new();
    let mut num: usize = crate::deserialize(&bytes[i..], true).unwrap();
    i += crate::serialized_size(&num, true);
    // println!("task group unpack");
    while num > 0 {
        let id: AmId = crate::deserialize(&bytes[i..], true).unwrap();
        i += id_size;
        // println!("task group unpack am");
        let am = AMS_EXECS.get(&id).unwrap()(&bytes[i..], cur_pe);
        i += am.serialized_size();
        ams.push(am);
        num -= 1;
    }
    let ei = ams.len();
    let tg_am = AmGroupAm {
        ams: Arc::new(ams),
        si: 0,
        ei,
    };
    <AmGroupAm as DarcSerde>::des(&tg_am, cur_pe);
    Arc::new(tg_am)
}

crate::inventory::submit! {
    RegisteredAm{
        exec: am_group_am_unpack,
        name: "AmGroupAm"
    }
}

#[lamellar_impl::AmDataRT]
struct AmGroupAmReturn {
    val: Vec<Vec<u8>>,
}

impl LamellarSerde for AmGroupAmReturn {
    fn serialized_size(&self) -> usize {
        crate::serialized_size(&self.val, true)
    }
    fn serialize_into(&self, buf: &mut [u8]) {
        crate::serialize_into(buf, &self.val, true).unwrap();
    }
    fn serialize(&self) -> Vec<u8> {
        crate::serialize(self, true).unwrap()
    }
}

impl LamellarResultDarcSerde for AmGroupAmReturn {}

/// A group of active messages that can be executed in parallel
/// the active messages do not need to be the same type, but they must all return the unit type i.e. `()`
/// Future implementations will relax this restriction, so that they only need to return the same type.
/// ```
/// use lamellar::active_messaging::prelude::*;
/// #[AmData(Debug,Clone)]
/// struct Am1 {
///    foo: usize,
/// }
/// #[lamellar::am]
/// impl LamellarAm for Am1{
///     async fn exec(self) {
///         println!("in am1 {:?} on PE{:?}",self.foo,  lamellar::current_pe);
///     }
/// }
///
/// #[AmData(Debug,Clone)]
/// struct Am2 {
///    bar: String,
/// }
/// #[lamellar::am]
/// impl LamellarAm for Am2{
///     async fn exec(self) {
///         println!("in am2 {:?} on PE{:?}",self.bar,lamellar::current_pe);
///     }
/// }
///
/// fn main(){
///     let world = lamellar::LamellarWorldBuilder::new().build();
///     let my_pe = world.my_pe();
///     let num_pes = world.num_pes();
///
///     let am1 = Am1{foo: 1};
///     let am2 = Am2{bar: "hello".to_string()};
///     //create a new AmGroup
///     let mut am_group = AmGroup::new(&world);
///     // add the AMs to the group
///     // we can specify individual PEs to execute on or all PEs
///     am_group.add_am_pe(0,am1.clone());
///     am_group.add_am_pe(1,am1.clone());
///     am_group.add_am_pe(0,am2.clone());
///     am_group.add_am_pe(1,am2.clone());
///     am_group.add_am_all(am1.clone());
///     am_group.add_am_all(am2.clone());
///
///     //execute and await the completion of all AMs in the group
///     world.block_on(am_group.exec());
/// }
///```
/// Expected output on each PE:
/// ```text
/// in am1 1 on PE0
/// in am2 hello on PE0
/// in am1 1 on PE0
/// in am2 hello on PE0
/// in am1 1 on PE1
/// in am2 hello on PE1
/// in am1 1 on PE1
/// in am2 hello on PE1
/// ```
pub struct AmGroup {
    team: Pin<Arc<LamellarTeamRT>>,
    cnt: usize,
    reqs: BTreeMap<usize, (Vec<usize>, Vec<LamellarArcAm>, usize)>,
}

impl AmGroup {
    /// create a new AmGroup associated with the given team
    /// # Example
    /// ```
    /// use lamellar::active_messaging::prelude::*;
    /// fn main(){
    ///     let world = lamellar::LamellarWorldBuilder::new().build();
    ///     let my_pe = world.my_pe();
    ///     let num_pes = world.num_pes();
    ///     let mut am_group = AmGroup::new(&world);
    /// }
    /// ```
    pub fn new<U: Into<IntoLamellarTeam>>(team: U) -> AmGroup {
        AmGroup {
            team: team.into().team.clone(),
            cnt: 0,
            reqs: BTreeMap::new(),
        }
    }

    /// add an active message to the group which will execute on all PEs
    /// # Example
    /// ```
    /// use lamellar::active_messaging::prelude::*;
    /// #[AmData(Debug,Clone)]
    /// struct Am1 {
    ///    foo: usize,
    /// }
    /// #[lamellar::am]
    /// impl LamellarAm for Am1{
    ///     async fn exec(self) {
    ///         println!("in am1 {:?} on PE{:?}",self.foo,  lamellar::current_pe);
    ///     }
    /// }
    ///
    /// #[AmData(Debug,Clone)]
    /// struct Am2 {
    ///    bar: String,
    /// }
    /// #[lamellar::am]
    /// impl LamellarAm for Am2{
    ///     async fn exec(self) {
    ///         println!("in am2 {:?} on PE{:?}",self.bar,lamellar::current_pe);
    ///     }
    /// }
    ///
    /// fn main(){
    ///     let world = lamellar::LamellarWorldBuilder::new().build();
    ///     let my_pe = world.my_pe();
    ///     let num_pes = world.num_pes();
    ///
    ///     let am1 = Am1{foo: 1};
    ///     let am2 = Am2{bar: "hello".to_string()};
    ///     //create a new AmGroup
    ///     let mut am_group = AmGroup::new(&world);
    ///     // add the AMs to the group
    ///     // we can specify individual PEs to execute on or all PEs
    ///     am_group.add_am_all(am1.clone());
    ///     am_group.add_am_all(am2.clone());
    /// }
    /// ```
    pub fn add_am_all<F>(&mut self, am: F)
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        let req_queue = self
            .reqs
            .entry(self.team.num_pes)
            .or_insert((Vec::new(), Vec::new(), 0));
        req_queue.2 += am.serialized_size();
        req_queue.0.push(self.cnt);
        req_queue.1.push(Arc::new(am));

        self.cnt += 1;
    }

    /// add an active message to the group which will execute on the given PE
    /// # Example
    /// ```
    /// use lamellar::active_messaging::prelude::*;
    /// #[AmData(Debug,Clone)]
    /// struct Am1 {
    ///    foo: usize,
    /// }
    /// #[lamellar::am]
    /// impl LamellarAm for Am1{
    ///     async fn exec(self){
    ///         println!("in am1 {:?} on PE{:?}",self.foo,  lamellar::current_pe);
    ///     }
    /// }
    ///
    /// #[AmData(Debug,Clone)]
    /// struct Am2 {
    ///    bar: String,
    /// }
    /// #[lamellar::am]
    /// impl LamellarAm for Am2{
    ///     async fn exec(self) {
    ///         println!("in am2 {:?} on PE{:?}",self.bar,lamellar::current_pe);
    ///     }
    /// }
    ///
    /// fn main(){
    ///     let world = lamellar::LamellarWorldBuilder::new().build();
    ///     let my_pe = world.my_pe();
    ///     let num_pes = world.num_pes();
    ///
    ///     let am1 = Am1{foo: 1};
    ///     let am2 = Am2{bar: "hello".to_string()};
    ///     //create a new AmGroup
    ///     let mut am_group = AmGroup::new(&world);
    ///     // add the AMs to the group
    ///     // we can specify individual PEs to execute on or all PEs
    ///     am_group.add_am_pe(0,am1.clone());
    ///     am_group.add_am_pe(1,am2.clone());
    /// }
    /// ```
    pub fn add_am_pe<F>(&mut self, pe: usize, am: F)
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        let req_queue = self.reqs.entry(pe).or_insert((Vec::new(), Vec::new(), 0));
        req_queue.2 += am.serialized_size();
        req_queue.0.push(self.cnt);
        req_queue.1.push(Arc::new(am));
        self.cnt += 1;
    }

    /// execute the active messages that have been added to the group
    /// # Example
    /// ```
    /// use lamellar::active_messaging::prelude::*;
    /// #[AmData(Debug,Clone)]
    /// struct Am1 {
    ///    foo: usize,
    /// }
    /// #[lamellar::am]
    /// impl LamellarAm for Am1{
    ///     async fn exec(self) {
    ///         println!("in am1 {:?} on PE{:?}",self.foo,  lamellar::current_pe);
    ///     }
    /// }
    ///
    /// #[AmData(Debug,Clone)]
    /// struct Am2 {
    ///    bar: String,
    /// }
    /// #[lamellar::am]
    /// impl LamellarAm for Am2{
    ///     async fn exec(self){
    ///         println!("in am2 {:?} on PE{:?}",self.bar,lamellar::current_pe);
    ///     }
    /// }
    ///
    /// fn main(){
    ///     let world = lamellar::LamellarWorldBuilder::new().build();
    ///     let my_pe = world.my_pe();
    ///     let num_pes = world.num_pes();
    ///
    ///     let am1 = Am1{foo: 1};
    ///     let am2 = Am2{bar: "hello".to_string()};
    ///     //create a new AmGroup
    ///     let mut am_group = AmGroup::new(&world);
    ///     // add the AMs to the group
    ///     // we can specify individual PEs to execute on or all PEs
    ///     am_group.add_am_pe(0,am1.clone());
    ///     am_group.add_am_pe(1,am1.clone());
    ///     am_group.add_am_all(am2.clone());
    ///     //execute and await the completion of all AMs in the group
    ///     world.block_on(am_group.exec());
    /// }
    /// ```
    pub async fn exec(&mut self) {
        // let _timer = std::time::Instant::now();
        let mut reqs = vec![];
        let mut reqs_all = vec![];
        // let mut all_req = None;
        let mut reqs_pes = vec![];
        for (pe, the_ams) in self.reqs.iter_mut() {
            let mut ams = vec![];
            std::mem::swap(&mut ams, &mut the_ams.1);
            let ams = Arc::new(ams);

            if the_ams.2 > 1_000_000 {
                let num_reqs = (the_ams.2 / 1_000_000) + 1;
                let req_size = the_ams.2 / num_reqs;
                let mut temp_size = 0;
                let mut i = 0;
                let mut start_i = 0;
                let mut send = false;
                while i < ams.len() {
                    let am_size = ams[i].serialized_size();
                    if temp_size + am_size < 100_000_000 {
                        //hard size limit
                        temp_size += am_size;
                        i += 1;
                        if temp_size > req_size {
                            send = true
                        }
                    } else {
                        send = true;
                    }
                    if send {
                        let tg_am = AmGroupAm {
                            ams: ams.clone(),
                            si: start_i,
                            ei: i,
                        };
                        // println!("tg_am len {:?}",i-start_i);
                        if *pe == self.team.num_pes {
                            reqs_all.push(
                                self.team
                                    .exec_arc_am_all::<Vec<Vec<u8>>>(Arc::new(tg_am), None),
                            );
                        } else {
                            reqs.push(self.team.exec_arc_am_pe::<Vec<Vec<u8>>>(
                                *pe,
                                Arc::new(tg_am),
                                None,
                            ));
                        }
                        send = false;
                        start_i = i;
                        temp_size = 0;
                    }
                }
                if temp_size > 0 {
                    let tg_am = AmGroupAm {
                        ams: ams.clone(),
                        si: start_i,
                        ei: i,
                    };
                    // println!("tg_am len {:?}",i-start_i);
                    if *pe == self.team.num_pes {
                        reqs_all.push(
                            self.team
                                .exec_arc_am_all::<Vec<Vec<u8>>>(Arc::new(tg_am), None),
                        );
                    } else {
                        reqs.push(self.team.exec_arc_am_pe::<Vec<Vec<u8>>>(
                            *pe,
                            Arc::new(tg_am),
                            None,
                        ));
                    }
                }
            } else {
                let tg_am = AmGroupAm {
                    ams: ams.clone(),
                    si: 0,
                    ei: ams.len(),
                };
                // println!("tg_am len {:?}",ams.len());
                if *pe == self.team.num_pes {
                    reqs_all.push(
                        self.team
                            .exec_arc_am_all::<Vec<Vec<u8>>>(Arc::new(tg_am), None),
                    );
                } else {
                    reqs.push(
                        self.team
                            .exec_arc_am_pe::<Vec<Vec<u8>>>(*pe, Arc::new(tg_am), None),
                    );
                }
            }

            reqs_pes.push(pe);
        }
        // println!(
        //     "launch time: {:?} cnt: {:?} {:?}",
        //     timer.elapsed().as_secs_f64(),
        //     reqs.len(),
        //     reqs_all.len()
        // );
        futures_util::future::join_all(reqs).await;
        futures_util::future::join_all(reqs_all).await;
        // if let Some(req) = all_req{
        //     req.await;
        // }
    }
}

/// Contains the Result of an AM Group Request
#[derive(Clone)]
pub enum AmGroupResult<'a, T> {
    /// Contains the result from a single PE
    Pe(usize, &'a T),
    /// Contains the result from all PEs
    All(TypedAmAllIter<'a, T>),
}

impl<'a, T: std::fmt::Debug> std::fmt::Debug for AmGroupResult<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AmGroupResult::Pe(pe, req) => write!(f, "Pe({:?}, {:?})", pe, req),
            AmGroupResult::All(req) => write!(f, "All({:?})", req),
        }
    }
}

/// An iterator over the results of an individual AM in a Typed AM Group that executed on all PEs
#[derive(Clone)]
pub enum TypedAmAllIter<'a, T> {
    Unit(TypedAmAllUnitIter<'a, T>),
    Val(TypedAmAllValIter<'a, T>),
}

impl<'a, T: std::fmt::Debug> std::fmt::Debug for TypedAmAllIter<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TypedAmAllIter::Unit(iter) => write!(f, "{:?}", iter),
            TypedAmAllIter::Val(iter) => write!(f, "{:?}", iter),
        }
    }
}

impl<'a, T> Iterator for TypedAmAllIter<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            TypedAmAllIter::Unit(iter) => iter.next(),
            TypedAmAllIter::Val(iter) => iter.next(),
        }
    }
}

/// An iterator over the unit values of an individual AM in a Typed AM group that executed on all PEs
#[derive(Clone)]
pub struct TypedAmAllUnitIter<'a, T> {
    all: &'a Vec<T>,
    cur_pe: usize,
    num_pes: usize,
}

impl<'a, T: std::fmt::Debug> std::fmt::Debug for TypedAmAllUnitIter<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for i in 0..self.num_pes {
            write!(f, "{:?},", self.all[i])?;
        }
        write!(f, "]")
    }
}

impl<'a, T> Iterator for TypedAmAllUnitIter<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_pe < self.num_pes {
            let cur_pe = self.cur_pe;
            self.cur_pe += 1;
            Some(&self.all[cur_pe])
        } else {
            None
        }
    }
}

/// An iterator over the values of an individual AM in a Typed AM group that executed on all PEs
#[derive(Clone)]
pub struct TypedAmAllValIter<'a, T> {
    all: &'a Vec<Vec<T>>,
    req: usize,
    cur_pe: usize,
    num_pes: usize,
}

impl<'a, T: std::fmt::Debug> std::fmt::Debug for TypedAmAllValIter<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for i in 0..self.num_pes {
            write!(f, "{:?},", self.all[i][self.req])?;
        }
        write!(f, "]")
    }
}

impl<'a, T> Iterator for TypedAmAllValIter<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_pe < self.num_pes {
            let cur_pe = self.cur_pe;
            self.cur_pe += 1;
            Some(&self.all[cur_pe][self.req])
        } else {
            None
        }
    }
}

/// Hold the results from a Typed AM group request
#[derive(Clone)]
pub enum TypedAmGroupResult<T> {
    /// the Ams within the group return unit values
    Unit(TypedAmGroupUnitResult<T>),
    /// the Ams within the group return values of type T
    Val(TypedAmGroupValResult<T>),
}

impl<T> TypedAmGroupResult<T> {
    /// creates a new Unit result
    pub fn unit(reqs: Vec<TypedAmGroupBatchResult<T>>, cnt: usize, num_pes: usize) -> Self {
        TypedAmGroupResult::Unit(TypedAmGroupUnitResult::new(reqs, cnt, num_pes))
    }

    /// creates a new Val result
    pub fn val(reqs: Vec<TypedAmGroupBatchResult<T>>, cnt: usize, num_pes: usize) -> Self {
        TypedAmGroupResult::Val(TypedAmGroupValResult::new(reqs, cnt, num_pes))
    }

    /// returns the result at index i
    pub fn at(&self, i: usize) -> AmGroupResult<'_, T> {
        match self {
            TypedAmGroupResult::Unit(res) => res.at(i),
            TypedAmGroupResult::Val(res) => res.at(i),
        }
    }

    /// returns the number of results in the AM group request
    pub fn len(&self) -> usize {
        match self {
            TypedAmGroupResult::Unit(res) => res.len(),
            TypedAmGroupResult::Val(res) => res.len(),
        }
    }

    /// returns an iterator over the results of the Typed Am group
    pub fn iter(&self) -> TypedAmGroupResultIter<'_, T> {
        TypedAmGroupResultIter {
            index: 0,
            results: self,
        }
    }
}

/// An iterator over the results of an AM group request
#[derive(Clone)]
pub struct TypedAmGroupResultIter<'a, T> {
    index: usize,
    results: &'a TypedAmGroupResult<T>,
}

impl<'a, T> Iterator for TypedAmGroupResultIter<'a, T> {
    type Item = AmGroupResult<'a, T>;
    fn next(&mut self) -> Option<Self::Item> {
        // if self.index % 10000 == 0 {
        //     println!("TypedAmGroupResultIter index: {}", self.index);
        // }
        if self.index < self.results.len() {
            let index = self.index;
            self.index += 1;
            Some(self.results.at(index))
        } else {
            None
        }
    }
}

/// This enum is used to specify the type of AmGroup request
pub enum BaseAmGroupReq<T> {
    /// This request will execute on a single PE  and return the unit value
    SinglePeUnit(AmHandle<T>),
    /// This request will return a single value of type T from a single PE
    SinglePeVal(AmHandle<Vec<T>>),
    /// This request will execute on all PEs and return a vec of unit values
    AllPeUnit(MultiAmHandle<T>),
    /// This request will execute on all PEs and return a vec of values of type T for each PE
    AllPeVal(MultiAmHandle<Vec<T>>),
}

impl<T: AmDist> BaseAmGroupReq<T> {
    async fn into_result(self) -> BaseAmGroupResult<T> {
        match self {
            BaseAmGroupReq::SinglePeUnit(reqs) => BaseAmGroupResult::SinglePeUnit(reqs.await),
            BaseAmGroupReq::SinglePeVal(reqs) => BaseAmGroupResult::SinglePeVal(reqs.await),
            BaseAmGroupReq::AllPeUnit(reqs) => BaseAmGroupResult::AllPeUnit(reqs.await),
            BaseAmGroupReq::AllPeVal(reqs) => BaseAmGroupResult::AllPeVal(reqs.await),
        }
    }
}

/// This enum is used to hold the results of a TypedAmGroup request
#[derive(Clone)]
pub(crate) enum BaseAmGroupResult<T> {
    // T here should be the inner most return type
    /// AmGroup executed on a single PE, and does not return any value
    SinglePeUnit(T),
    /// AmGroup executed on a single PE, and returns a Vec of T
    SinglePeVal(Vec<T>),
    /// AmGroup executed on all PEs, and does not return any value
    AllPeUnit(Vec<T>),
    /// AmGroup executed on all PEs, and returns a vec of T for each PE
    AllPeVal(Vec<Vec<T>>),
}

/// A struct to hold the requests for a TypedAmGroup request corresponding to a single PE
pub struct TypedAmGroupBatchReq<T> {
    pe: usize,
    ids: Vec<usize>,
    reqs: BaseAmGroupReq<T>,
}

/// A struct to hold the results of a TypedAmGroup request corresponding to a single PE
#[derive(Clone)]
pub struct TypedAmGroupBatchResult<T> {
    pe: usize,
    ids: Vec<usize>,
    reqs: BaseAmGroupResult<T>,
}

impl<T: AmDist> TypedAmGroupBatchReq<T> {
    /// Create a new TypedAmGroupBatchReq for PE with the assoicated IDs and individual Requests
    pub fn new(pe: usize, ids: Vec<usize>, reqs: BaseAmGroupReq<T>) -> Self {
        Self { pe, ids, reqs }
    }

    /// Convert this TypedAmGroupBatchReq into a TypedAmGroupBatchResult
    pub async fn into_result(self) -> TypedAmGroupBatchResult<T> {
        TypedAmGroupBatchResult {
            pe: self.pe,
            ids: self.ids,
            reqs: self.reqs.into_result().await,
        }
    }
}

#[derive(Clone)]
pub struct TypedAmGroupValResult<T> {
    reqs: Vec<TypedAmGroupBatchResult<T>>,
    cnt: usize,
    num_pes: usize,
}

impl<T> TypedAmGroupValResult<T> {
    pub fn new(reqs: Vec<TypedAmGroupBatchResult<T>>, cnt: usize, num_pes: usize) -> Self {
        TypedAmGroupValResult { reqs, cnt, num_pes }
    }
    pub fn at(&self, index: usize) -> AmGroupResult<'_, T> {
        assert!(
            index < self.cnt,
            "AmGroupResult index out of bounds index = {}  len = {}",
            index,
            self.cnt
        );
        for req in self.reqs.iter() {
            if let Ok(idx) = req.ids.binary_search(&index) {
                match &req.reqs {
                    BaseAmGroupResult::SinglePeVal(res) => {
                        return AmGroupResult::Pe(req.pe, &res[idx])
                    }
                    BaseAmGroupResult::AllPeVal(res) => {
                        return AmGroupResult::All(TypedAmAllIter::Val(TypedAmAllValIter {
                            all: res,
                            cur_pe: 0,
                            req: idx,
                            num_pes: self.num_pes,
                        }))
                    }
                    _ => unreachable!(),
                }
            }
        }
        panic!("AmGroupResult index out of bounds");
    }
    pub fn len(&self) -> usize {
        self.cnt
    }
}

#[derive(Clone)]
pub struct TypedAmGroupUnitResult<T> {
    reqs: Vec<TypedAmGroupBatchResult<T>>,
    cnt: usize,
    num_pes: usize,
}

impl<T> TypedAmGroupUnitResult<T> {
    pub fn new(reqs: Vec<TypedAmGroupBatchResult<T>>, cnt: usize, num_pes: usize) -> Self {
        TypedAmGroupUnitResult { reqs, cnt, num_pes }
    }
    pub fn at(&self, index: usize) -> AmGroupResult<'_, T> {
        assert!(
            index < self.cnt,
            "AmGroupResult index out of bounds index = {}  len = {}",
            index,
            self.cnt
        );
        for req in self.reqs.iter() {
            if let Ok(_idx) = req.ids.binary_search(&index) {
                match &req.reqs {
                    BaseAmGroupResult::SinglePeUnit(res) => return AmGroupResult::Pe(req.pe, &res),
                    BaseAmGroupResult::AllPeUnit(res) => {
                        return AmGroupResult::All(TypedAmAllIter::Unit(TypedAmAllUnitIter {
                            all: res,
                            cur_pe: 0,
                            num_pes: self.num_pes,
                        }))
                    }
                    _ => unreachable!(),
                }
            }
        }
        panic!("AmGroupResult index out of bounds");
    }
    pub fn len(&self) -> usize {
        self.cnt
    }
}

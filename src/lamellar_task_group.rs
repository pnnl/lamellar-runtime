use crate::active_messaging::*;
use crate::lamellae::Des;
use crate::lamellar_arch::LamellarArchRT;
use crate::lamellar_request::*;
use crate::lamellar_team::{IntoLamellarTeam, LamellarTeamRT};
use crate::scheduler::{ReqId, SchedulerQueue};

use async_trait::async_trait;

// use crossbeam::utils::CachePadded;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) struct TaskGroupRequestHandleInner {
    cnt: Arc<AtomicUsize>,
    data: Mutex<HashMap<usize, InternalResult>>, //<sub_id, result>
    team_outstanding_reqs: Arc<AtomicUsize>,
    world_outstanding_reqs: Arc<AtomicUsize>,
    tg_outstanding_reqs: Option<Arc<AtomicUsize>>,
}

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
        self.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        self.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
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
            InternalResult::Remote(x) => {
                if let Ok(result) = x.deserialize_data() {
                    //crate::deserialize(&x) {
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

pub(crate) struct TaskGroupMultiRequestHandleInner {
    cnt: Arc<AtomicUsize>,
    arch: Arc<LamellarArchRT>,
    data: Mutex<HashMap<usize, HashMap<usize, InternalResult>>>, //<sub_id, <pe, result>>
    team_outstanding_reqs: Arc<AtomicUsize>,
    world_outstanding_reqs: Arc<AtomicUsize>,
    tg_outstanding_reqs: Option<Arc<AtomicUsize>>,
}

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
        self.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        self.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
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
            InternalResult::Remote(x) => {
                if let Ok(result) = x.deserialize_data() {
                    //crate::deserialize(&x) {
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

pub(crate) struct TaskGroupLocalRequestHandleInner {
    cnt: Arc<AtomicUsize>,
    data: Mutex<HashMap<usize, LamellarAny>>, //<sub_id, result>
    team_outstanding_reqs: Arc<AtomicUsize>,
    world_outstanding_reqs: Arc<AtomicUsize>,
    tg_outstanding_reqs: Option<Arc<AtomicUsize>>,
}

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
            InternalResult::Remote(_) => panic!("unexpected result type"),
            InternalResult::Unit => self.data.lock().insert(sub_id, Box::new(()) as LamellarAny),
        };
    }
    fn update_counters(&self) {
        self.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        self.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
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
impl<T: AmLocal + 'static> LamellarRequest for TaskGroupLocalRequestHandle<T> {
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

impl LamellarTaskGroup {
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

    pub fn wait_all(&self) {
        let mut temp_now = Instant::now();
        while self.counters.outstanding_reqs.load(Ordering::SeqCst) > 0 {
            self.team.scheduler.exec_task();
            if temp_now.elapsed() > Duration::new(60, 0) {
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

    pub fn exec_am_all<F>(&self, am: F) -> Box<dyn LamellarMultiRequest<Output = F::Output>>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        // println!("task group exec am all");
        self.team.team_counters.add_send_req(self.team.num_pes);
        self.team.world_counters.add_send_req(self.team.num_pes);
        self.counters.add_send_req(self.team.num_pes);
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
        self.team.scheduler.submit_req(
            self.team.world_pe,
            None,
            ExecType::Am(Cmd::Exec),
            req_id,
            LamellarFunc::Am(func),
            self.team.lamellae.clone(),
            world,
            self.team.clone(),
            self.team.remote_ptr_addr as u64,
        );
        Box::new(TaskGroupMultiRequestHandle {
            inner: self.multi_req.clone(),
            sub_id: req_id.sub_id,
            _phantom: PhantomData,
        })
    }

    pub fn exec_am_pe<F>(&self, pe: usize, am: F) -> Box<dyn LamellarRequest<Output = F::Output>>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        // println!("task group exec am pe");
        self.team.team_counters.add_send_req(1);
        self.team.world_counters.add_send_req(1);
        self.counters.add_send_req(1);
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
        self.team.scheduler.submit_req(
            self.team.world_pe,
            Some(self.team.arch.world_pe(pe).expect("pe not member of team")),
            ExecType::Am(Cmd::Exec),
            req_id,
            LamellarFunc::Am(func),
            self.team.lamellae.clone(),
            world,
            self.team.clone(),
            self.team.remote_ptr_addr as u64,
        );
        Box::new(TaskGroupRequestHandle {
            inner: self.req.clone(),
            sub_id: req_id.sub_id,
            _phantom: PhantomData,
        })
    }

    pub fn exec_am_local<F>(&self, am: F) -> Box<dyn LamellarRequest<Output = F::Output>>
    where
        F: LamellarActiveMessage + LocalAM + 'static,
    {
        // println!("task group exec am local");
        self.team.team_counters.add_send_req(1);
        self.team.world_counters.add_send_req(1);
        self.counters.add_send_req(1);
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
        self.team.scheduler.submit_req(
            self.team.world_pe,
            Some(self.team.world_pe),
            ExecType::Am(Cmd::Exec),
            req_id,
            LamellarFunc::LocalAm(func),
            self.team.lamellae.clone(),
            world,
            self.team.clone(),
            self.team.remote_ptr_addr as u64,
        );
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

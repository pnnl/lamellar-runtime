use std::{
    cell::Cell,
    collections::HashMap,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Waker},
};

use futures_util::Future;
use parking_lot::Mutex;
use pin_project::{pin_project, pinned_drop};
use tracing::{trace, warn};

use crate::{
    lamellae::Des,
    lamellar_request::{InternalResult, LamellarRequest, LamellarRequestAddResult},
    memregion::one_sided::MemRegionHandleInner,
    scheduler::{LamellarTask, Scheduler},
    warnings::RuntimeWarning,
    Darc, LamellarArchRT,
};

use super::{AMCounters, Am, AmDist, DarcSerde, RemotePtr};

pub(crate) struct AmHandleInner {
    pub(crate) ready: AtomicBool,
    pub(crate) waker: Mutex<Option<Waker>>,
    pub(crate) data: Cell<Option<InternalResult>>, //we only issue a single request, which the runtime will update, but the user also has a handle so we need a way to mutate
    pub(crate) team_counters: Arc<AMCounters>,
    pub(crate) world_counters: Arc<AMCounters>,
    pub(crate) tg_counters: Option<Arc<AMCounters>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) user_handle: AtomicU8,
}

impl std::fmt::Debug for AmHandleInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AmHandleInner {{ ready: {:?}, team_outstanding_reqs: {:?}  world_outstanding_reqs {:?} tg_outstanding_reqs {:?} user_handle{:?} }}", self.ready.load(Ordering::Relaxed),  
        self.team_counters.outstanding_reqs.load(Ordering::Relaxed), self.world_counters.outstanding_reqs.load(Ordering::Relaxed), self.tg_counters.as_ref().map(|x| x.outstanding_reqs.load(Ordering::Relaxed)), self.user_handle.load(Ordering::Relaxed))
    }
}

// we use the ready bool to protect access to the data field
unsafe impl Sync for AmHandleInner {}

impl LamellarRequestAddResult for AmHandleInner {
    fn user_held(&self) -> bool {
        self.user_handle.load(Ordering::SeqCst) > 0
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn add_result(&self, _pe: usize, _sub_id: usize, data: InternalResult) {
        // for a single request this is only called one time by a single runtime thread so use of the cell is safe
        self.data.set(Some(data));
        self.ready.store(true, Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().take() {
            trace!("notifying waker");
            waker.wake();
        }
        trace!("request complete");
    }
    fn update_counters(&self, _sub_id: usize) {
        self.team_counters.dec_outstanding(1);
        self.world_counters.dec_outstanding(1);
        if let Some(tg_counters) = self.tg_counters.clone() {
            tg_counters.dec_outstanding(1);
        }
    }
}
/// A handle to an active messaging request that executes on a singe PE
#[derive(Debug)]
#[pin_project(PinnedDrop)]
#[must_use = "active messaging handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
pub struct AmHandle<T> {
    pub(crate) inner: Arc<AmHandleInner>,
    pub(crate) am: Option<(Am, usize)>,
    pub(crate) _phantom: std::marker::PhantomData<T>,
}

#[pinned_drop]
impl<T> PinnedDrop for AmHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if self.am.is_some() {
            RuntimeWarning::DroppedHandle("an AmHandle").print();
        }
        self.inner.user_handle.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<T: AmDist> AmHandle<T> {
    fn process_result(&self, data: InternalResult) -> T {
        match data {
            InternalResult::Local(x) => {
                if let Ok(result) = x.downcast::<T>() {
                    *result
                } else {
                    panic!("unexpected local result  of type ");
                }
            }
            InternalResult::Remote(x, darcs) => {
                if let Ok(result) = x.deserialize_data::<T>() {
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
                    panic!("unexpected remote result  of type ");
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

    #[tracing::instrument(skip_all, level = "debug")]
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
            trace!("am spawned");
        }
    }
    /// This method will spawn the associated Active Message on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<T> {
        self.launch_am_if_needed();
        self.inner.scheduler.clone().spawn_task(self, Vec::new()) //AM handles counters
    }
    /// This method will block the calling thread until the associated Array Operation completes
    pub fn block(mut self) -> T {
        RuntimeWarning::BlockingCall("AmHandle::block", "<handle>.spawn() or <handle>.await")
            .print();
        self.launch_am_if_needed();
        self.inner.scheduler.clone().block_on(self)
    }
}

impl<T: AmDist> LamellarRequest for AmHandle<T> {
    fn launch(&mut self) {
        self.launch_am_if_needed();
    }
    fn blocking_wait(mut self) -> T {
        self.launch_am_if_needed();
        while !self.inner.ready.load(Ordering::SeqCst) {
            self.inner.scheduler.exec_task();
        }
        self.process_result(self.inner.data.replace(None).expect("result should exist"))
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        self.launch_am_if_needed();
        let mut cur_waker = self.inner.waker.lock();

        if self.inner.ready.load(Ordering::SeqCst) {
            trace!("request read");
            true
        } else {
            trace!("request not ready");
            match &mut *cur_waker {
                Some(cur_waker) => {
                    if !cur_waker.will_wake(waker) {
                        warn!("WARNING: overwriting waker {:?}", cur_waker);
                        cur_waker.wake_by_ref();
                    }
                    cur_waker.clone_from(waker);
                }
                None => {
                    *cur_waker = Some(waker.clone());
                }
            }
            false
        }
    }

    fn val(&self) -> Self::Output {
        self.process_result(self.inner.data.replace(None).expect("result should exist"))
    }
}

impl<T: AmDist> Future for AmHandle<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launch_am_if_needed();
        let mut this = self.as_mut();
        if this.ready_or_set_waker(cx.waker()) {
            Poll::Ready(
                this.process_result(this.inner.data.replace(None).expect("result should exist")),
            )
        } else {
            Poll::Pending
        }
    }
}

/// A handle to an active messaging request that executes on the local (originating) PE
#[derive(Debug)]
#[pin_project(PinnedDrop)]
#[must_use = "active messaging handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
pub struct LocalAmHandle<T> {
    pub(crate) inner: Arc<AmHandleInner>,
    pub(crate) am: Option<(Am, usize)>,
    pub(crate) _phantom: std::marker::PhantomData<T>,
    pub(crate) thread: Option<usize>,
}

#[pinned_drop]
impl<T> PinnedDrop for LocalAmHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if self.am.is_some() {
            RuntimeWarning::DroppedHandle("a LocalAmHandle").print();
        }
        self.inner.user_handle.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<T: 'static> LocalAmHandle<T> {
    fn process_result(&self, data: InternalResult) -> T {
        match data {
            InternalResult::Local(x) => {
                if let Ok(result) = x.downcast::<T>() {
                    *result
                } else {
                    panic!("unexpected local result  of type ");
                }
            }
            InternalResult::Remote(_x, _darcs) => {
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
            if let Some(thread) = self.thread {
                self.inner.scheduler.submit_am_thread(am, thread);
            } else {
                self.inner.scheduler.submit_am(am);
            }
        }
    }
}

impl<T: Send + 'static> LocalAmHandle<T> {
    /// This method will spawn the associated Active Message on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<T> {
        self.launch_am_if_needed();
        self.inner.scheduler.clone().spawn_task(self, Vec::new()) //AM handles counters)
    }
    /// This method will block the calling thread until the associated Array Operation completes
    pub fn block(mut self) -> T {
        RuntimeWarning::BlockingCall("LocalAmHandle::block", "<handle>.spawn() or <handle>.await")
            .print();
        self.launch_am_if_needed();
        self.inner.scheduler.clone().block_on(self)
    }
}

impl<T: AmDist> From<LocalAmHandle<T>> for AmHandle<T> {
    fn from(mut x: LocalAmHandle<T>) -> Self {
        x.inner.user_handle.fetch_add(1, Ordering::SeqCst);
        Self {
            inner: x.inner.clone(),
            am: x.am.take(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: 'static> LamellarRequest for LocalAmHandle<T> {
    fn launch(&mut self) {
        self.launch_am_if_needed();
    }
    fn blocking_wait(mut self) -> T {
        self.launch_am_if_needed();
        while !self.inner.ready.load(Ordering::SeqCst) {
            self.inner.scheduler.exec_task();
        }
        let data = self.inner.data.replace(None).expect("result should exist");
        self.process_result(data)
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        self.launch_am_if_needed();
        let mut cur_waker = self.inner.waker.lock();
        if self.inner.ready.load(Ordering::SeqCst) {
            true
        } else {
            match &mut *cur_waker {
                Some(cur_waker) => {
                    if !cur_waker.will_wake(waker) {
                        warn!("WARNING: overwriting waker {:?}", cur_waker);
                        cur_waker.wake_by_ref();
                    }
                    cur_waker.clone_from(waker);
                }
                None => {
                    *cur_waker = Some(waker.clone());
                }
            }
            false
        }
    }

    fn val(&self) -> Self::Output {
        let data = self.inner.data.replace(None).expect("result should exist");
        self.process_result(data)
    }
}

impl<T: 'static> Future for LocalAmHandle<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launch_am_if_needed();
        let mut this = self.as_mut();
        if this.ready_or_set_waker(cx.waker()) {
            Poll::Ready(
                this.process_result(this.inner.data.replace(None).expect("result should exist")),
            )
        } else {
            Poll::Pending
        }
    }
}

#[derive(Debug)]
pub(crate) struct MultiAmHandleInner {
    pub(crate) cnt: AtomicUsize,
    pub(crate) arch: Arc<LamellarArchRT>,
    pub(crate) data: Mutex<HashMap<usize, InternalResult>>,
    pub(crate) waker: Mutex<Option<Waker>>,
    pub(crate) team_counters: Arc<AMCounters>,
    pub(crate) world_counters: Arc<AMCounters>,
    pub(crate) tg_counters: Option<Arc<AMCounters>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) user_handle: AtomicU8, //we can use this flag to optimize what happens when the request returns
}

/// A handle to an active messaging request that executes on multiple PEs, returned from a call to [exec_am_all][crate::ActiveMessaging::exec_am_all]
#[derive(Debug)]
#[pin_project(PinnedDrop)]
#[must_use = "active messaging handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
pub struct MultiAmHandle<T> {
    pub(crate) inner: Arc<MultiAmHandleInner>,
    pub(crate) am: Option<(Am, usize)>,
    pub(crate) _phantom: std::marker::PhantomData<T>,
}

#[pinned_drop]
impl<T> PinnedDrop for MultiAmHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if self.am.is_some() {
            RuntimeWarning::DroppedHandle("a MultiAmHandle").print();
        }
        self.inner.user_handle.fetch_sub(1, Ordering::SeqCst);
    }
}

impl LamellarRequestAddResult for MultiAmHandleInner {
    fn user_held(&self) -> bool {
        self.user_handle.load(Ordering::SeqCst) > 0
    }
    fn add_result(&self, pe: usize, _sub_id: usize, data: InternalResult) {
        let pe = self.arch.team_pe(pe).expect("pe does not exist on team");
        self.data.lock().insert(pe, data);
        self.cnt.fetch_sub(1, Ordering::SeqCst);
        if self.cnt.load(Ordering::SeqCst) == 0 {
            if let Some(waker) = self.waker.lock().take() {
                waker.wake();
            }
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

impl<T: AmDist> MultiAmHandle<T> {
    fn process_result(&self, data: InternalResult) -> T {
        match data {
            InternalResult::Local(x) => {
                if let Ok(result) = x.downcast::<T>() {
                    *result
                } else {
                    panic!("unexpected local result  of type ");
                }
            }
            InternalResult::Remote(x, darcs) => {
                if let Ok(result) = x.deserialize_data::<T>() {
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
                    panic!("unexpected remote result  of type ");
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
    #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Vec<T>> {
        self.launch_am_if_needed();
        self.inner.scheduler.clone().spawn_task(self, Vec::new()) //AM handles counters
    }
    /// This method will block the calling thread until the associated Array Operation completes
    pub fn block(mut self) -> Vec<T> {
        RuntimeWarning::BlockingCall("MultiAmHandle::block", "<handle>.spawn() or <handle>.await")
            .print();
        self.launch_am_if_needed();
        self.inner.scheduler.clone().block_on(self)
    }
}

impl<T: AmDist> LamellarRequest for MultiAmHandle<T> {
    fn launch(&mut self) {
        self.launch_am_if_needed();
    }
    fn blocking_wait(mut self) -> Self::Output {
        self.launch_am_if_needed();
        while self.inner.cnt.load(Ordering::SeqCst) > 0 {
            self.inner.scheduler.exec_task();
        }
        let mut res = vec![];
        let mut data = self.inner.data.lock();
        // println!("data len{:?}", data.len());
        for pe in 0..data.len() {
            res.push(self.process_result(data.remove(&pe).expect("result should exist")));
        }
        res
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        self.launch_am_if_needed();
        let mut cur_waker = self.inner.waker.lock();
        if self.inner.cnt.load(Ordering::SeqCst) == 0 {
            true
        } else {
            match &mut *cur_waker {
                Some(cur_waker) => {
                    if !cur_waker.will_wake(waker) {
                        warn!("WARNING: overwriting waker {:?}", cur_waker);
                        cur_waker.wake_by_ref();
                    }
                    cur_waker.clone_from(waker);
                }
                None => {
                    *cur_waker = Some(waker.clone());
                }
            }
            false
        }
    }

    fn val(&self) -> Self::Output {
        let mut res = vec![];
        let mut data = self.inner.data.lock();
        for pe in 0..data.len() {
            res.push(self.process_result(data.remove(&pe).expect("result should exist")));
        }
        res
    }
}

impl<T: AmDist> Future for MultiAmHandle<T> {
    type Output = Vec<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launch_am_if_needed();
        let mut this = self.as_mut();
        if this.ready_or_set_waker(cx.waker()) {
            let mut res = vec![];
            let mut data = this.inner.data.lock();
            // println!("data len{:?}", data.len());
            for pe in 0..data.len() {
                res.push(this.process_result(data.remove(&pe).expect("result should exist")));
            }
            Poll::Ready(res)
        } else {
            Poll::Pending
        }
    }
}

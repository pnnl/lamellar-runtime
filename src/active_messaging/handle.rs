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

use crate::{
    lamellae::Des,
    lamellar_request::{InternalResult, LamellarRequest, LamellarRequestAddResult},
    memregion::one_sided::MemRegionHandleInner,
    scheduler::Scheduler,
    Darc, LamellarArchRT,
};

use super::{AmDist, DarcSerde, RemotePtr};

pub(crate) struct AmHandleInner {
    pub(crate) ready: AtomicBool,
    pub(crate) waker: Mutex<Option<Waker>>,
    pub(crate) data: Cell<Option<InternalResult>>, //we only issue a single request, which the runtime will update, but the user also has a handle so we need a way to mutate
    pub(crate) team_outstanding_reqs: Arc<AtomicUsize>,
    pub(crate) world_outstanding_reqs: Arc<AtomicUsize>,
    pub(crate) tg_outstanding_reqs: Option<Arc<AtomicUsize>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) user_handle: AtomicU8,
}

impl std::fmt::Debug for AmHandleInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AmHandleInner {{ ready: {:?}, team_outstanding_reqs: {:?}  world_outstanding_reqs {:?} tg_outstanding_reqs {:?} user_handle{:?} }}", self.ready.load(Ordering::Relaxed),  self.team_outstanding_reqs.load(Ordering::Relaxed), self.world_outstanding_reqs.load(Ordering::Relaxed), self.tg_outstanding_reqs.as_ref().map(|x| x.load(Ordering::Relaxed)), self.user_handle.load(Ordering::Relaxed))
    }
}

// we use the ready bool to protect access to the data field
unsafe impl Sync for AmHandleInner {}

impl LamellarRequestAddResult for AmHandleInner {
    fn user_held(&self) -> bool {
        self.user_handle.load(Ordering::SeqCst) > 0
    }
    fn add_result(&self, _pe: usize, _sub_id: usize, data: InternalResult) {
        // for a single request this is only called one time by a single runtime thread so use of the cell is safe
        self.data.set(Some(data));
        self.ready.store(true, Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().take() {
            waker.wake();
        }
    }
    fn update_counters(&self) {
        let _team_reqs = self.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        let _world_req = self.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        if let Some(tg_outstanding_reqs) = self.tg_outstanding_reqs.clone() {
            tg_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

/// A handle to an active messaging request that executes on a singe PE
#[derive(Debug)]
#[pin_project(PinnedDrop)]
pub struct AmHandle<T> {
    pub(crate) inner: Arc<AmHandleInner>,
    pub(crate) _phantom: std::marker::PhantomData<T>,
}

#[pinned_drop]
impl<T> PinnedDrop for AmHandle<T> {
    fn drop(self: Pin<&mut Self>) {
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
}

impl<T: AmDist> LamellarRequest for AmHandle<T> {
    fn blocking_wait(self) -> T {
        while !self.inner.ready.load(Ordering::SeqCst) {
            self.inner.scheduler.exec_task();
        }
        self.process_result(self.inner.data.replace(None).expect("result should exist"))
    }

    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        let mut cur_waker = self.inner.waker.lock();
        if self.inner.ready.load(Ordering::SeqCst) {
            true
        } else {
            match &mut *cur_waker {
                Some(cur_waker) => {
                    if !cur_waker.will_wake(waker) {
                        println!("WARNING: overwriting waker {:?}", cur_waker);
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
pub struct LocalAmHandle<T> {
    pub(crate) inner: Arc<AmHandleInner>,
    pub(crate) _phantom: std::marker::PhantomData<T>,
}

#[pinned_drop]
impl<T> PinnedDrop for LocalAmHandle<T> {
    fn drop(self: Pin<&mut Self>) {
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
}

impl<T: AmDist> From<LocalAmHandle<T>> for AmHandle<T> {
    fn from(x: LocalAmHandle<T>) -> Self {
        x.inner.user_handle.fetch_add(1, Ordering::SeqCst);
        Self {
            inner: x.inner.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: 'static> LamellarRequest for LocalAmHandle<T> {
    fn blocking_wait(self) -> T {
        while !self.inner.ready.load(Ordering::SeqCst) {
            self.inner.scheduler.exec_task();
        }
        let data = self.inner.data.replace(None).expect("result should exist");
        self.process_result(data)
    }

    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        let mut cur_waker = self.inner.waker.lock();
        if self.inner.ready.load(Ordering::SeqCst) {
            true
        } else {
            match &mut *cur_waker {
                Some(cur_waker) => {
                    if !cur_waker.will_wake(waker) {
                        println!("WARNING: overwriting waker {:?}", cur_waker);
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
    pub(crate) team_outstanding_reqs: Arc<AtomicUsize>,
    pub(crate) world_outstanding_reqs: Arc<AtomicUsize>,
    pub(crate) tg_outstanding_reqs: Option<Arc<AtomicUsize>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) user_handle: AtomicU8, //we can use this flag to optimize what happens when the request returns
}

/// A handle to an active messaging request that executes on multiple PEs, returned from a call to [exec_am_all][crate::ActiveMessaging::exec_am_all]
#[derive(Debug)]
#[pin_project(PinnedDrop)]
pub struct MultiAmHandle<T> {
    pub(crate) inner: Arc<MultiAmHandleInner>,
    pub(crate) _phantom: std::marker::PhantomData<T>,
}

#[pinned_drop]
impl<T> PinnedDrop for MultiAmHandle<T> {
    fn drop(self: Pin<&mut Self>) {
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
    fn update_counters(&self) {
        let _team_reqs = self.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        let _world_req = self.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        if let Some(tg_outstanding_reqs) = self.tg_outstanding_reqs.clone() {
            tg_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
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
}

impl<T: AmDist> LamellarRequest for MultiAmHandle<T> {
    fn blocking_wait(self) -> Self::Output {
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

    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        let mut cur_waker = self.inner.waker.lock();
        if self.inner.cnt.load(Ordering::SeqCst) == 0 {
            true
        } else {
            match &mut *cur_waker {
                Some(cur_waker) => {
                    if !cur_waker.will_wake(waker) {
                        println!("WARNING: overwriting waker {:?}", cur_waker);
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

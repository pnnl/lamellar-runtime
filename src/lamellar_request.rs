use crate::active_messaging::{AmDist, AmLocal, LamellarAny};
use crate::lamellae::{Des, SerializedData};
use crate::lamellar_arch::LamellarArchRT;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use std::cell::Cell;
use std::collections::HashMap;

pub(crate) enum InternalResult {
    Local(LamellarAny),     // a local result from a local am (possibly a returned one)
    Remote(SerializedData), // a remte result from a remote am
    Unit,
}

#[async_trait]
pub trait LamellarRequest: Sync + Send {
    type Output;
    async fn into_future(mut self: Box<Self>) -> Self::Output;
    fn get(&self) -> Self::Output;
}

#[async_trait]
pub trait LamellarMultiRequest: Sync + Send {
    type Output;
    async fn into_future(mut self: Box<Self>) -> Vec<Self::Output>;
    fn get(&self) -> Vec<Self::Output>;
}

pub(crate) trait LamellarRequestAddResult: Sync + Send {
    fn user_held(&self) -> bool;
    fn add_result(&self, pe: usize, sub_id: usize, data: InternalResult);
    fn update_counters(&self);
}

//todo make this an enum instead...
// will need to include the task group requests as well...
pub(crate) struct LamellarRequestResult {
    pub(crate) req: Arc<dyn LamellarRequestAddResult>,
}

impl LamellarRequestResult {
    pub(crate) fn add_result(&self, pe: usize, sub_id: usize, data: InternalResult) {
        if self.req.user_held() {
            //if the user has dropped the handle, no need to actually do anything with the returned data
            self.req.add_result(pe as usize, sub_id, data);
        }
        self.req.update_counters();
    }
}

pub(crate) struct LamellarRequestHandleInner {
    pub(crate) ready: AtomicBool,
    pub(crate) data: Cell<Option<InternalResult>>, //we only issue a single request, which the runtime will update, but the user also has a handle so we need a way to mutate
    pub(crate) team_outstanding_reqs: Arc<AtomicUsize>,
    pub(crate) world_outstanding_reqs: Arc<AtomicUsize>,
    pub(crate) tg_outstanding_reqs: Option<Arc<AtomicUsize>>,
    pub(crate) user_handle: AtomicBool, //we can use this flag to optimize what happens when the request returns
}
// we use the ready bool to protect access to the data field
unsafe impl Sync for LamellarRequestHandleInner {}

pub struct LamellarRequestHandle<T: AmDist> {
    pub(crate) inner: Arc<LamellarRequestHandleInner>,
    pub(crate) _phantom: std::marker::PhantomData<T>,
}

impl<T: AmDist> Drop for LamellarRequestHandle<T> {
    fn drop(&mut self) {
        self.inner.user_handle.store(false, Ordering::SeqCst);
    }
}

impl LamellarRequestAddResult for LamellarRequestHandleInner {
    fn user_held(&self) -> bool {
        self.user_handle.load(Ordering::SeqCst)
    }
    fn add_result(&self, _pe: usize, _sub_id: usize, data: InternalResult) {
        // for a single request this is only called one time by a single runtime thread so use of the cell is safe
        self.data.set(Some(data));
        self.ready.store(true, Ordering::SeqCst);
    }
    fn update_counters(&self) {
        self.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        self.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        if let Some(tg_outstanding_reqs) = self.tg_outstanding_reqs.clone() {
            tg_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl<T: AmDist> LamellarRequestHandle<T> {
    fn process_result(&self, data: InternalResult) -> T {
        match data {
            InternalResult::Local(x) => {
                if let Ok(result) = x.downcast::<T>() {
                    *result
                } else {
                    panic!("unexpected local result  of type ");
                }
            }
            InternalResult::Remote(x) => {
                if let Ok(result) = x.deserialize_data() {
                    //crate::deserialize(&x) {
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

#[async_trait]
impl<T: AmDist> LamellarRequest for LamellarRequestHandle<T> {
    type Output = T;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        while !self.inner.ready.load(Ordering::SeqCst) {
            async_std::task::yield_now().await;
        }
        self.process_result(self.inner.data.replace(None).unwrap())
    }
    fn get(&self) -> T {
        while !self.inner.ready.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        self.process_result(self.inner.data.replace(None).unwrap())
    }
}

pub(crate) struct LamellarMultiRequestHandleInner {
    pub(crate) cnt: AtomicUsize,
    pub(crate) arch: Arc<LamellarArchRT>,
    pub(crate) data: Mutex<HashMap<usize, InternalResult>>,
    pub(crate) team_outstanding_reqs: Arc<AtomicUsize>,
    pub(crate) world_outstanding_reqs: Arc<AtomicUsize>,
    pub(crate) tg_outstanding_reqs: Option<Arc<AtomicUsize>>,
    pub(crate) user_handle: AtomicBool, //we can use this flag to optimize what happens when the request returns
}

pub struct LamellarMultiRequestHandle<T: AmDist> {
    pub(crate) inner: Arc<LamellarMultiRequestHandleInner>,
    pub(crate) _phantom: std::marker::PhantomData<T>,
}

impl<T: AmDist> Drop for LamellarMultiRequestHandle<T> {
    fn drop(&mut self) {
        self.inner.user_handle.store(false, Ordering::SeqCst);
    }
}

impl LamellarRequestAddResult for LamellarMultiRequestHandleInner {
    fn user_held(&self) -> bool {
        self.user_handle.load(Ordering::SeqCst)
    }
    fn add_result(&self, pe: usize, _sub_id: usize, data: InternalResult) {
        let pe = self.arch.team_pe(pe).expect("pe does not exist on team");
        self.data.lock().insert(pe, data);
        self.cnt.fetch_sub(1, Ordering::SeqCst);
    }
    fn update_counters(&self) {
        self.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        self.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        if let Some(tg_outstanding_reqs) = self.tg_outstanding_reqs.clone() {
            tg_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl<T: AmDist> LamellarMultiRequestHandle<T> {
    fn process_result(&self, data: InternalResult) -> T {
        match data {
            InternalResult::Local(x) => {
                if let Ok(result) = x.downcast::<T>() {
                    *result
                } else {
                    panic!("unexpected local result  of type ");
                }
            }
            InternalResult::Remote(x) => {
                if let Ok(result) = x.deserialize_data() {
                    //crate::deserialize(&x) {
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

#[async_trait]
impl<T: AmDist> LamellarMultiRequest for LamellarMultiRequestHandle<T> {
    type Output = T;
    async fn into_future(mut self: Box<Self>) -> Vec<Self::Output> {
        while self.inner.cnt.load(Ordering::SeqCst) > 0 {
            async_std::task::yield_now().await;
        }
        let mut res = vec![];
        let mut data = self.inner.data.lock();
        for pe in 0..data.len() {
            res.push(self.process_result(data.remove(&pe).unwrap()));
        }
        res
    }
    fn get(&self) -> Vec<T> {
        while self.inner.cnt.load(Ordering::SeqCst) > 0 {
            std::thread::yield_now();
        }
        let mut res = vec![];
        let mut data = self.inner.data.lock();
        for pe in 0..data.len() {
            res.push(self.process_result(data.remove(&pe).unwrap()));
        }
        res
    }
}

pub(crate) struct LamellarLocalRequestHandleInner {
    pub(crate) ready: AtomicBool,
    pub(crate) data: Cell<Option<LamellarAny>>, //we only issue a single request, which the runtime will update, but the user also has a handle so we need a way to mutate
    pub(crate) team_outstanding_reqs: Arc<AtomicUsize>,
    pub(crate) world_outstanding_reqs: Arc<AtomicUsize>,
    pub(crate) tg_outstanding_reqs: Option<Arc<AtomicUsize>>,
    pub(crate) user_handle: AtomicBool, //we can use this flag to optimize what happens when the request returns
}
// we use the ready bool to protect access to the data field
unsafe impl Sync for LamellarLocalRequestHandleInner {}

pub struct LamellarLocalRequestHandle<T> {
    pub(crate) inner: Arc<LamellarLocalRequestHandleInner>,
    pub(crate) _phantom: std::marker::PhantomData<T>,
}

impl<T> Drop for LamellarLocalRequestHandle<T> {
    fn drop(&mut self) {
        self.inner.user_handle.store(false, Ordering::SeqCst);
    }
}

impl LamellarRequestAddResult for LamellarLocalRequestHandleInner {
    fn user_held(&self) -> bool {
        self.user_handle.load(Ordering::SeqCst)
    }
    fn add_result(&self, _pe: usize, _sub_id: usize, data: InternalResult) {
        // for a single request this is only called one time by a single runtime thread so use of the cell is safe
        match data {
            InternalResult::Local(x) => self.data.set(Some(x)),
            InternalResult::Remote(_) => panic!("unexpected local result  of type "),
            InternalResult::Unit => self.data.set(Some(Box::new(()) as LamellarAny)),
        }

        self.ready.store(true, Ordering::SeqCst);
    }
    fn update_counters(&self) {
        self.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        self.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        if let Some(tg_outstanding_reqs) = self.tg_outstanding_reqs.clone() {
            tg_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl<T: 'static> LamellarLocalRequestHandle<T> {
    fn process_result(&self, data: LamellarAny) -> T {
        if let Ok(result) = data.downcast::<T>() {
            *result
        } else {
            panic!("unexpected local result  of type ");
        }
    }
}

#[async_trait]
impl<T: AmLocal + 'static> LamellarRequest for LamellarLocalRequestHandle<T> {
    type Output = T;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        while !self.inner.ready.load(Ordering::SeqCst) {
            async_std::task::yield_now().await;
        }
        self.process_result(self.inner.data.replace(None).unwrap())
    }
    fn get(&self) -> T {
        while !self.inner.ready.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        self.process_result(self.inner.data.replace(None).unwrap())
    }
}

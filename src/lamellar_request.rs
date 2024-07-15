use crate::active_messaging::{
    AmHandleInner, DarcSerde, LamellarAny, MultiAmHandleInner, RemotePtr,
};
use crate::darc::Darc;
use crate::lamellae::SerializedData;
use crate::lamellar_task_group::{TaskGroupAmHandleInner, TaskGroupMultiAmHandleInner};
use crate::memregion::one_sided::MemRegionHandleInner;

use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Waker;

#[derive(Debug)]
pub(crate) enum InternalResult {
    Local(LamellarAny), // a local result from a local am (possibly a returned one)
    Remote(SerializedData, Vec<RemotePtr>), // a remte result from a remote am
    Unit,
}

// #[enum_dispatch(Future, LamellarRequest)]
// pub(crate) enum LamellarHandle<T> {
//     SinglePeAm(AmHandle<T>),
// }
// impl<T: AmDist> LamellarHandle<T> {
//     pub fn blocking_wait(&self) -> T {
//         match self {
//             LamellarHandle::SinglePeAm(h) => h.blocking_wait(),
//         }
//     }

//     pub fn ready(&self) -> bool {
//         match self {
//             LamellarHandle::SinglePeAm(h) => h.ready(),
//         }
//     }
// }

// impl<T> Future for LamellarHandle<T> {
//     type Output = T;
//     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         match self {
//             LamellarHandle::SinglePeAm(h) => h.poll(cx),
//         }
//     }
// }

// pub(crate) mod private {
//     use crate::active_messaging::handle::AmHandle;
//     use crate::lamellar_request::LamellarHandle;
//     use enum_dispatch::enum_dispatch;
//     use futures_util::Future;
//     use std::task::Waker;

//     #[enum_dispatch(LamellarHandle<T>)]
//     pub trait LamellarRequestSealed:  {

//     }
// }

//#[doc(hidden)]
// #[enum_dispatch]
pub(crate) trait LamellarRequest: Future {
    fn blocking_wait(self) -> Self::Output;
    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool;
    fn val(&self) -> Self::Output;
}

// //#[doc(hidden)]
// #[async_trait]
// pub trait LamellarMultiRequest: Sync + Send {
//     type Output;
//     async fn into_future(mut self: Box<Self>) -> Vec<Self::Output>;
//     fn blocking_wait(&self) -> Vec<Self::Output>;
// }

pub(crate) trait LamellarRequestAddResult: Sync + Send {
    fn user_held(&self) -> bool;
    fn add_result(&self, pe: usize, sub_id: usize, data: InternalResult);
    fn update_counters(&self);
}

pub(crate) enum LamellarRequestResult {
    Am(Arc<AmHandleInner>),
    MultiAm(Arc<MultiAmHandleInner>),
    TgAm(Arc<TaskGroupAmHandleInner>),
    TgMultiAm(Arc<TaskGroupMultiAmHandleInner>),
}
//todo make this an enum instead...
// will need to include the task group requests as well...
// pub(crate) struct LamellarRequestResult {
//     pub(crate) req: Arc<dyn LamellarRequestAddResult>,
// }
impl std::fmt::Debug for LamellarRequestResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Am(_) => write!(f, "Am"),
            Self::MultiAm(_) => write!(f, "MultiAm"),
            Self::TgAm(_) => write!(f, "TgAm"),
            Self::TgMultiAm(_) => write!(f, "TgMultiAm"),
        }
    }
}

impl LamellarRequestResult {
    //#[tracing::instrument(skip_all)]
    pub(crate) fn add_result_inner<T: LamellarRequestAddResult>(
        req: &Arc<T>,
        pe: usize,
        sub_id: usize,
        data: InternalResult,
    ) -> bool {
        let mut added = false;

        if req.user_held() {
            req.add_result(pe as usize, sub_id, data);
            added = true;
        } else {
            // if the user dopped the handle we still need to handle if Darcs are returned
            if let InternalResult::Remote(_, darcs) = data {
                // we need to appropraiately set the reference counts if the returned data contains any Darcs
                // we "cheat" in that we dont actually care what the Darc wraps (hence the cast to ()) we just care
                // that the reference count is updated.
                for darc in darcs {
                    match darc {
                        RemotePtr::NetworkDarc(darc) => {
                            let temp: Darc<()> = darc.into();
                            temp.des(Ok(0));
                        }
                        RemotePtr::NetMemRegionHandle(mr) => {
                            let temp: Arc<MemRegionHandleInner> = mr.into();
                            temp.local_ref.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            }
        }
        req.update_counters();
        added
    }

    pub(crate) fn add_result(&self, pe: usize, sub_id: usize, data: InternalResult) -> bool {
        match self {
            Self::Am(req) => Self::add_result_inner(req, pe, sub_id, data),
            Self::MultiAm(req) => Self::add_result_inner(req, pe, sub_id, data),
            Self::TgAm(req) => Self::add_result_inner(req, pe, sub_id, data),
            Self::TgMultiAm(req) => Self::add_result_inner(req, pe, sub_id, data),
        }
    }
}

// #[derive(Debug)]
// pub struct LamellarHandle<T: AmDist> {
//     pub(crate) inner: Arc<LamellarRequestType<T>>,
// }

// impl<T: AmDist> Drop for LamellarHandle<T> {
//     fn drop(&mut self) {
//         self.inner.user_handle.store(false, Ordering::SeqCst);
//     }
// }

// impl<T: AmDist> LamellarHandle<T> {
//     fn process_result(&self, data: InternalResult) -> T {
//         match data {
//             InternalResult::Local(x) => {
//                 if let Ok(result) = x.downcast::<T>() {
//                     *result
//                 } else {
//                     panic!("unexpected local result  of type ");
//                 }
//             }
//             InternalResult::Remote(x, darcs) => {
//                 if let Ok(result) = x.deserialize_data::<T>() {
//                     // we need to appropraiately set the reference counts if the returned data contains any Darcs
//                     // we "cheat" in that we dont actually care what the Darc wraps (hence the cast to ()) we just care
//                     // that the reference count is updated.
//                     for darc in darcs {
//                         match darc {
//                             RemotePtr::NetworkDarc(darc) => {
//                                 let temp: Darc<()> = darc.into();
//                                 temp.des(Ok(0));
//                                 temp.inc_local_cnt(1); //we drop temp decreasing local count, but need to account for the actual real darc (and we unfourtunately cannot enforce the T: DarcSerde bound, or at least I havent figured out how to yet)
//                             }
//                             RemotePtr::NetMemRegionHandle(mr) => {
//                                 let temp: Arc<MemRegionHandleInner> = mr.into();
//                                 temp.local_ref.fetch_add(2, Ordering::SeqCst); // Need to increase by two, 1 for temp, 1 for result
//                             }
//                         }
//                     }

//                     result
//                 } else {
//                     panic!("unexpected remote result  of type ");
//                 }
//             }
//             InternalResult::Unit => {
//                 if let Ok(result) = (Box::new(()) as Box<dyn std::any::Any>).downcast::<T>() {
//                     *result
//                 } else {
//                     panic!("unexpected unit result  of type ");
//                 }
//             }
//         }
//     }
// }

// impl<T: AmDist> private::LamellarRequestSealed for LamellarHandle<T> {
//     fn set_waker(&mut self, waker: &Waker) {
//         self.inner.set_waker(waker);
//     }

//     fn val(&self) -> Self::Output {
//         self.inner.val();
//     }
// }

// impl<T: AmDist> LamellarRequest for LamellarHandle<T> {
//     fn blocking_wait(&self) -> T {
//         self.inner.blocking_wait()
//     }

//     fn ready(&self) -> bool {
//         self.inner.read()
//     }
// }

// impl<T: AmDist> Future for LamellarHandle<T> {
//     type Output = T;
//     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         self.inner.poll(cx)
//     }
// }

// pub(crate) struct LamellarRequestHandleInner {
//     pub(crate) ready: AtomicBool,
//     pub(crate) waker: Mutex<Option<Waker>>,
//     pub(crate) data: Cell<Option<InternalResult>>, //we only issue a single request, which the runtime will update, but the user also has a handle so we need a way to mutate
//     pub(crate) team_outstanding_reqs: Arc<AtomicUsize>,
//     pub(crate) world_outstanding_reqs: Arc<AtomicUsize>,
//     pub(crate) tg_outstanding_reqs: Option<Arc<AtomicUsize>>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) user_handle: AtomicBool, //we can use this flag to optimize what happens when the request returns
// }
// impl std::fmt::Debug for LamellarRequestHandleInner {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "LamellarRequestHandleInner {{ ready: {:?}, team_outstanding_reqs: {:?}  world_outstanding_reqs {:?} tg_outstanding_reqs {:?} user_handle{:?} }}", self.ready.load(Ordering::Relaxed),  self.team_outstanding_reqs.load(Ordering::Relaxed), self.world_outstanding_reqs.load(Ordering::Relaxed), self.tg_outstanding_reqs.as_ref().map(|x| x.load(Ordering::Relaxed)), self.user_handle.load(Ordering::Relaxed))
//     }
// }
// // we use the ready bool to protect access to the data field
// unsafe impl Sync for LamellarRequestHandleInner {}

// //#[doc(hidden)]
// #[derive(Debug)]
// pub struct LamellarRequestHandle<T: AmDist> {
//     pub(crate) inner: Arc<LamellarRequestHandleInner>,
//     pub(crate) _phantom: std::marker::PhantomData<T>,
// }

// impl<T: AmDist> Drop for LamellarRequestHandle<T> {
//     //#[tracing::instrument(skip_all)]
//     fn drop(&mut self) {
//         self.inner.user_handle.store(false, Ordering::SeqCst);
//     }
// }

// impl LamellarRequestAddResult for LamellarRequestHandleInner {
//     //#[tracing::instrument(skip_all)]
//     fn user_held(&self) -> bool {
//         self.user_handle.load(Ordering::SeqCst)
//     }
//     //#[tracing::instrument(skip_all)]
//     fn add_result(&self, _pe: usize, _sub_id: usize, data: InternalResult) {
//         // for a single request this is only called one time by a single runtime thread so use of the cell is safe
//         self.data.set(Some(data));
//         self.ready.store(true, Ordering::SeqCst);
//         if let Some(waker) = self.waker.lock().take() {
//             waker.wake();
//         }
//     }
//     //#[tracing::instrument(skip_all)]
//     fn update_counters(&self) {
//         let _team_reqs = self.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
//         let _world_req = self.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
//         // println!(
//         //     "[{:?}] update counter team {} world {}",
//         //     std::thread::current().id(),
//         //     _team_reqs - 1,
//         //     _world_req - 1
//         // );
//         if let Some(tg_outstanding_reqs) = self.tg_outstanding_reqs.clone() {
//             tg_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
//         }
//     }
// }

// impl<T: AmDist> LamellarRequestHandle<T> {
//     //#[tracing::instrument(skip_all)]
//     fn process_result(&self, data: InternalResult) -> T {
//         match data {
//             InternalResult::Local(x) => {
//                 if let Ok(result) = x.downcast::<T>() {
//                     *result
//                 } else {
//                     panic!("unexpected local result  of type ");
//                 }
//             }
//             InternalResult::Remote(x, darcs) => {
//                 if let Ok(result) = x.deserialize_data::<T>() {
//                     // we need to appropraiately set the reference counts if the returned data contains any Darcs
//                     // we "cheat" in that we dont actually care what the Darc wraps (hence the cast to ()) we just care
//                     // that the reference count is updated.
//                     for darc in darcs {
//                         match darc {
//                             RemotePtr::NetworkDarc(darc) => {
//                                 let temp: Darc<()> = darc.into();
//                                 temp.des(Ok(0));
//                                 temp.inc_local_cnt(1); //we drop temp decreasing local count, but need to account for the actual real darc (and we unfourtunately cannot enforce the T: DarcSerde bound, or at least I havent figured out how to yet)
//                             }
//                             RemotePtr::NetMemRegionHandle(mr) => {
//                                 let temp: Arc<MemRegionHandleInner> = mr.into();
//                                 temp.local_ref.fetch_add(2, Ordering::SeqCst); // Need to increase by two, 1 for temp, 1 for result
//                             }
//                         }
//                     }

//                     result
//                 } else {
//                     panic!("unexpected remote result  of type ");
//                 }
//             }
//             InternalResult::Unit => {
//                 if let Ok(result) = (Box::new(()) as Box<dyn std::any::Any>).downcast::<T>() {
//                     *result
//                 } else {
//                     panic!("unexpected unit result  of type ");
//                 }
//             }
//         }
//     }
// }

// #[async_trait]
// impl<T: AmDist> LamellarRequest for LamellarRequestHandle<T> {
//     type Output = T;
//     //#[tracing::instrument(skip_all)]
//     async fn into_future(mut self: Box<Self>) -> Self::Output {
//         while !self.inner.ready.load(Ordering::SeqCst) {
//             async_std::task::yield_now().await;
//         }
//         self.process_result(self.inner.data.replace(None).expect("result should exist"))
//     }
//     //#[tracing::instrument(skip_all)]
//     fn blocking_wait(&self) -> T {
//         while !self.inner.ready.load(Ordering::SeqCst) {
//             // std::thread::yield_now();
//             self.inner.scheduler.exec_task();
//         }
//         self.process_result(self.inner.data.replace(None).expect("result should exist"))
//     }

//     fn ready(&self) -> bool {
//         self.inner.ready.load(Ordering::SeqCst)
//     }

//     fn set_waker(&mut self, waker: &Waker) {
//         *self.inner.waker.lock() = Some(waker);
//     }
// }

// #[derive(Debug)]
// pub(crate) struct LamellarMultiRequestHandleInner {
//     pub(crate) cnt: AtomicUsize,
//     pub(crate) arch: Arc<LamellarArchRT>,
//     pub(crate) data: Mutex<HashMap<usize, InternalResult>>,
//     pub(crate) waker: Mutex<Option<Waker>>,
//     pub(crate) team_outstanding_reqs: Arc<AtomicUsize>,
//     pub(crate) world_outstanding_reqs: Arc<AtomicUsize>,
//     pub(crate) tg_outstanding_reqs: Option<Arc<AtomicUsize>>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) user_handle: AtomicBool, //we can use this flag to optimize what happens when the request returns
// }

// //#[doc(hidden)]
// #[derive(Debug)]
// pub struct LamellarMultiRequestHandle<T: AmDist> {
//     pub(crate) inner: Arc<LamellarMultiRequestHandleInner>,
//     pub(crate) _phantom: std::marker::PhantomData<T>,
// }

// impl<T: AmDist> Drop for LamellarMultiRequestHandle<T> {
//     //#[tracing::instrument(skip_all)]
//     fn drop(&mut self) {
//         self.inner.user_handle.store(false, Ordering::SeqCst);
//     }
// }

// impl LamellarRequestAddResult for LamellarMultiRequestHandleInner {
//     //#[tracing::instrument(skip_all)]
//     fn user_held(&self) -> bool {
//         self.user_handle.load(Ordering::SeqCst)
//     }
//     //#[tracing::instrument(skip_all)]
//     fn add_result(&self, pe: usize, _sub_id: usize, data: InternalResult) {
//         let pe = self.arch.team_pe(pe).expect("pe does not exist on team");
//         self.data.lock().insert(pe, data);
//         self.cnt.fetch_sub(1, Ordering::SeqCst);
//         if self.cnt.load(Ordering::SeqCst) == 0 {
//             if let Some(waker) = self.waker.lock().take() {
//                 waker.wake();
//             }
//         }
//     }
//     //#[tracing::instrument(skip_all)]
//     fn update_counters(&self) {
//         // println!(
//         //     "update counter {:?} {:?}",
//         //     self.team_outstanding_reqs.load(Ordering::SeqCst),
//         //     self.world_outstanding_reqs.load(Ordering::SeqCst)
//         // );
//         let _team_reqs = self.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
//         let _world_req = self.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
//         // println!(
//         //     "[{:?}] multi update counter team {} world {}",
//         //     std::thread::current().id(),
//         //     _team_reqs - 1,
//         //     _world_req - 1
//         // );
//         if let Some(tg_outstanding_reqs) = self.tg_outstanding_reqs.clone() {
//             tg_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
//         }
//     }
// }

// impl<T: AmDist> LamellarMultiRequestHandle<T> {
//     //#[tracing::instrument(skip_all)]
//     fn process_result(&self, data: InternalResult) -> T {
//         match data {
//             InternalResult::Local(x) => {
//                 if let Ok(result) = x.downcast::<T>() {
//                     *result
//                 } else {
//                     panic!("unexpected local result  of type ");
//                 }
//             }
//             InternalResult::Remote(x, darcs) => {
//                 if let Ok(result) = x.deserialize_data::<T>() {
//                     // we need to appropraiately set the reference counts if the returned data contains any Darcs
//                     // we "cheat" in that we dont actually care what the Darc wraps (hence the cast to ()) we just care
//                     // that the reference count is updated.
//                     for darc in darcs {
//                         match darc {
//                             RemotePtr::NetworkDarc(darc) => {
//                                 let temp: Darc<()> = darc.into();
//                                 temp.des(Ok(0));
//                                 temp.inc_local_cnt(1); //we drop temp decreasing local count, but need to account for the actual real darc (and we unfourtunately cannot enforce the T: DarcSerde bound, or at least I havent figured out how to yet)
//                             }
//                             RemotePtr::NetMemRegionHandle(mr) => {
//                                 let temp: Arc<MemRegionHandleInner> = mr.into();
//                                 temp.local_ref.fetch_add(2, Ordering::SeqCst); // Need to increase by two, 1 for temp, 1 for result
//                             }
//                         }
//                     }
//                     result
//                 } else {
//                     panic!("unexpected remote result  of type ");
//                 }
//             }
//             InternalResult::Unit => {
//                 if let Ok(result) = (Box::new(()) as Box<dyn std::any::Any>).downcast::<T>() {
//                     *result
//                 } else {
//                     panic!("unexpected unit result  of type ");
//                 }
//             }
//         }
//     }
// }

// #[async_trait]
// impl<T: AmDist> LamellarMultiRequest for LamellarMultiRequestHandle<T> {
//     type Output = T;
//     //#[tracing::instrument(skip_all)]
//     async fn into_future(mut self: Box<Self>) -> Vec<Self::Output> {
//         while self.inner.cnt.load(Ordering::SeqCst) > 0 {
//             async_std::task::yield_now().await;
//         }
//         let mut res = vec![];
//         let mut data = self.inner.data.lock();
//         // println!("data len{:?}", data.len());
//         for pe in 0..data.len() {
//             res.push(self.process_result(data.remove(&pe).expect("result should exist")));
//         }
//         res
//     }
//     //#[tracing::instrument(skip_all)]
//     fn blocking_wait(&self) -> Vec<T> {
//         while self.inner.cnt.load(Ordering::SeqCst) > 0 {
//             // std::thread::yield_now();
//             self.inner.scheduler.exec_task();
//         }
//         let mut res = vec![];
//         let mut data = self.inner.data.lock();
//         for pe in 0..data.len() {
//             res.push(self.process_result(data.remove(&pe).expect("result should exist")));
//         }
//         res
//     }
// }

// pub(crate) struct LamellarLocalRequestHandleInner {
//     // pub(crate) ready: AtomicBool,
//     pub(crate) ready: (Mutex<bool>, Condvar),
//     pub(crate) waker: Mutex<Option<Waker>>,
//     pub(crate) data: Cell<Option<LamellarAny>>, //we only issue a single request, which the runtime will update, but the user also has a handle so we need a way to mutate
//     pub(crate) team_outstanding_reqs: Arc<AtomicUsize>,
//     pub(crate) world_outstanding_reqs: Arc<AtomicUsize>,
//     pub(crate) tg_outstanding_reqs: Option<Arc<AtomicUsize>>,
//     pub(crate) scheduler: Arc<Scheduler>,
//     pub(crate) user_handle: AtomicBool, //we can use this flag to optimize what happens when the request returns
// }

// impl std::fmt::Debug for LamellarLocalRequestHandleInner {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "LamellarLocalRequestHandleInner {{ ready: {:?}, team_outstanding_reqs {:?}, world_outstanding_reqs {:?}, tg_outstanding_reqs{:?}, user_handle {:?}}}", self.ready.0.lock(), self.team_outstanding_reqs.load(Ordering::SeqCst), self.world_outstanding_reqs.load(Ordering::SeqCst), self.tg_outstanding_reqs.as_ref().map(|x| x.load(Ordering::SeqCst)), self.user_handle.load(Ordering::SeqCst))
//     }
// }

// // we use the ready bool to protect access to the data field
// unsafe impl Sync for LamellarLocalRequestHandleInner {}

// //#[doc(hidden)]
// #[derive(Debug)]
// pub struct LamellarLocalRequestHandle<T> {
//     pub(crate) inner: Arc<LamellarLocalRequestHandleInner>,
//     pub(crate) _phantom: std::marker::PhantomData<T>,
// }

// impl<T> Drop for LamellarLocalRequestHandle<T> {
//     //#[tracing::instrument(skip_all)]
//     fn drop(&mut self) {
//         self.inner.user_handle.store(false, Ordering::SeqCst);
//     }
// }

// impl LamellarRequestAddResult for LamellarLocalRequestHandleInner {
//     //#[tracing::instrument(skip_all)]
//     fn user_held(&self) -> bool {
//         self.user_handle.load(Ordering::SeqCst)
//     }
//     //#[tracing::instrument(skip_all)]
//     fn add_result(&self, _pe: usize, _sub_id: usize, data: InternalResult) {
//         // for a single request this is only called one time by a single runtime thread so use of the cell is safe
//         match data {
//             InternalResult::Local(x) => self.data.set(Some(x)),
//             InternalResult::Remote(_, _) => panic!("unexpected local result  of type "),
//             InternalResult::Unit => self.data.set(Some(Box::new(()) as LamellarAny)),
//         }

//         // self.ready.store(true, Ordering::SeqCst);
//         *self.ready.0.lock() = true;
//         self.ready.1.notify_one();
//         if let Some(waker) = self.waker.lock().take() {
//             waker.wake();
//         }
//     }
//     //#[tracing::instrument(skip_all)]
//     fn update_counters(&self) {
//         let _team_reqs = self.team_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
//         let _world_req = self.world_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
//         // println!(
//         //     "[{:?}] local update counter team {} world {}",
//         //     std::thread::current().id(),
//         //     _team_reqs - 1,
//         //     _world_req - 1
//         // );
//         if let Some(tg_outstanding_reqs) = self.tg_outstanding_reqs.clone() {
//             tg_outstanding_reqs.fetch_sub(1, Ordering::SeqCst);
//         }
//     }
// }

// impl<T: 'static> LamellarLocalRequestHandle<T> {
//     //#[tracing::instrument(skip_all)]
//     fn process_result(&self, data: LamellarAny) -> T {
//         if let Ok(result) = data.downcast::<T>() {
//             *result
//         } else {
//             panic!("unexpected local result  of type ");
//         }
//     }
// }

// #[async_trait]
// impl<T: SyncSend + 'static> LamellarRequest for LamellarLocalRequestHandle<T> {
//     type Output = T;
//     //#[tracing::instrument(skip_all)]
//     async fn into_future(mut self: Box<Self>) -> Self::Output {
//         while !*self.inner.ready.0.lock() {
//             async_std::task::yield_now().await;
//         }
//         self.process_result(self.inner.data.replace(None).expect("result should exist"))
//     }
//     //#[tracing::instrument(skip_all)]
//     fn blocking_wait(&self) -> T {
//         // let mut ready_lock = self.inner.ready.0.lock();
//         // while !*ready_lock {
//         while !*self.inner.ready.0.lock() {
//             // std::thread::yield_now();
//             // self.inner.ready.1.wait(&mut ready_lock);
//             self.inner.scheduler.exec_task();
//         }
//         self.process_result(self.inner.data.replace(None).expect("result should exist"))
//     }

//     fn ready(&self) -> bool {
//         let ready = *self.inner.ready.0.lock();
//         // println!("ready: {}", ready);
//         ready
//     }

//     fn set_waker(&mut self, waker: &Waker) {
//         *self.inner.waker.lock() = Some(waker);
//     }
// }

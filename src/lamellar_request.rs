use crate::lamellar_arch::LamellarArchRT;
use async_trait::async_trait;
use crossbeam::utils::CachePadded;
use lamellar_prof::*;
use log::trace;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
// use futures::Future;

static CUR_REQ_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Debug)]
pub(crate) struct InternalReq {
    pub(crate) data_tx: crossbeam::channel::Sender<(usize, Option<std::vec::Vec<u8>>)>, //what if we create an enum for either bytes or the raw data?
    pub(crate) start: std::time::Instant,
    pub(crate) size: usize,
    pub(crate) cnt: Arc<CachePadded<AtomicUsize>>,
    pub(crate) active: Arc<AtomicBool>,
    pub(crate) team_outstanding_reqs: Arc<AtomicUsize>,
    pub(crate) world_outstanding_reqs: Arc<AtomicUsize>,
}

#[async_trait]
pub trait LamellarRequest {
    type Output: serde::ser::Serialize + serde::de::DeserializeOwned;
    async fn into_future(self: Box<Self>) -> Option<Self::Output>;
    // fn future(&self) -> impl Future<Output=Option<Self::Output>>;
    fn get(&self) -> Option<Self::Output>;
    fn get_all(&self) -> Vec<Option<Self::Output>>;
    fn as_any(self) -> Box<dyn std::any::Any>;
}

pub struct LamellarRequestHandle<
    T: serde::ser::Serialize + serde::de::DeserializeOwned + Sync + Send,
> {
    pub(crate) id: usize,
    pub(crate) cnt: usize,
    pub(crate) data_rx: crossbeam::channel::Receiver<(usize, Option<std::vec::Vec<u8>>)>,
    pub(crate) active: Arc<AtomicBool>,
    pub(crate) arch: Arc<LamellarArchRT>,
    pub(crate) am_type: AmType,
    pub(crate) _phantom: std::marker::PhantomData<T>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum AmType {
    RegisteredFunction,
    #[allow(dead_code)]
    RemoteClosure,
}

//#[prof]
impl<T: 'static + serde::ser::Serialize + serde::de::DeserializeOwned + Sync + Send>
    LamellarRequestHandle<T>
{
    pub(crate) fn new<'a>(
        num_pes: usize,
        am_type: AmType,
        arch: Arc<LamellarArchRT>,
        team_reqs: Arc<AtomicUsize>,
        world_reqs: Arc<AtomicUsize>,
    ) -> (LamellarRequestHandle<T>, InternalReq) {
        prof_start!(active);
        let active = Arc::new(AtomicBool::new(true));
        prof_end!(active);
        prof_start!(channel);
        let (s, r) = crossbeam::channel::unbounded();
        prof_end!(channel);
        prof_start!(id);
        let id = CUR_REQ_ID.fetch_add(1, Ordering::SeqCst);
        prof_end!(id);
        prof_start!(ireq);
        let ireq = InternalReq {
            data_tx: s,
            cnt: Arc::new(CachePadded::new(AtomicUsize::new(num_pes))),
            start: Instant::now(),
            size: 0,
            active: active.clone(),
            team_outstanding_reqs: team_reqs,
            world_outstanding_reqs: world_reqs,
        };
        prof_end!(ireq);
        (
            LamellarRequestHandle {
                id: id,
                cnt: num_pes,
                data_rx: r,
                active: active.clone(),
                arch: arch.clone(),
                am_type: am_type,
                _phantom: std::marker::PhantomData,
            },
            ireq,
        )
    }

    // fn as_type<S: 'static + serde::ser::Serialize + serde::de::DeserializeOwned + Sync + Send>(self) -> LamellarRequestHandle<T>{
    //     LamellarRequestHandle {
    //         id: self.id,
    //         cnt: self.cnt,
    //         data_rx: self.data_rx,
    //         active: self.active.clone(),
    //         arch: self.arch.clone(),
    //         am_type: self.am_type,
    //         _phantom: std::marker::PhantomData,
    //     }
    // }

    fn am_get(&self) -> Option<T> {
        let (_pe, data) = self.data_rx.recv().expect("result recv");
        match data {
            Some(x) => {
                // let result:LamellarReturn =
                //     crate::deserialize(&x).unwrap();
                // if let LamellarReturn::NewData(raw_result) = result {
                if let Ok(result) = crate::deserialize(&x) {
                    Some(result)
                } else {
                    None
                }
                // } else {
                //     None
                // }
            }
            None => None,
        }
    }

    fn am_get_all(&self) -> Vec<Option<T>> {
        let mut res: std::vec::Vec<Option<T>> = Vec::new(); //= vec![&None; self.cnt];
        for _i in 0..self.cnt {
            res.push(None);
        }
        if self.cnt > 1 {
            let mut cnt = self.cnt;
            while cnt > 0 {
                let (pe, data) = self.data_rx.recv().expect("result recv");
                if let Ok(pe) = self.arch.team_pe(pe) {
                    match data {
                        Some(x) => {
                            if let Ok(result) = crate::deserialize(&x) {
                                res[pe] = Some(result);
                            } else {
                                res[pe] = None;
                            }
                        }
                        None => {
                            res[pe] = None;
                        }
                    }
                    cnt -= 1;
                }
            }
        } else {
            res[0] = self.am_get();
        }
        res
    }

    fn closure_get(&self) -> Option<T> {
        let (_pe, data) = self.data_rx.recv().expect("result recv");
        match data {
            Some(x) => {
                let result: T = crate::deserialize(&x).unwrap();
                Some(result)
            }
            None => None,
        }
    }

    fn closure_get_all(&self) -> std::vec::Vec<Option<T>> {
        let mut res: std::vec::Vec<Option<T>> = Vec::new(); //= vec![&None; self.cnt];
        for _i in 0..self.cnt {
            res.push(None);
        }
        let mut cnt = self.cnt;
        while cnt > 0 {
            let (pe, data) = self.data_rx.recv().expect("result recv");
            match data {
                Some(x) => {
                    let result: T = crate::deserialize(&x).unwrap();
                    res[pe] = Some(result);
                }
                None => res[pe] = None,
            }
            cnt -= 1;
        }
        res
    }
}

#[async_trait]
impl<T: 'static + serde::ser::Serialize + serde::de::DeserializeOwned + Sync + Send> LamellarRequest
    for LamellarRequestHandle<T>
{
    type Output = T;
    async fn into_future(self: Box<Self>) -> Option<Self::Output> {
        let mut res = self.data_rx.try_recv();
        while res.is_err() {
            res = self.data_rx.try_recv();
            async_std::task::yield_now().await;
        }
        if let Ok((_pe, data)) = res {
            match data {
                Some(x) => {
                    if let Ok(result) = crate::deserialize(&x) {
                        Some(result)
                    } else {
                        None
                    }
                }
                None => None,
            }
        } else {
            None
        }
    }

    // fn future(&self) -> impl Future<Output = Option<Self::Output>>{
    //     self.am_await().await
    // }

    fn get(&self) -> Option<Self::Output> {
        match self.am_type {
            AmType::RegisteredFunction => self.am_get(),
            AmType::RemoteClosure => self.closure_get(),
        }
    }

    fn get_all(&self) -> Vec<Option<Self::Output>> {
        match self.am_type {
            AmType::RegisteredFunction => self.am_get_all(),
            AmType::RemoteClosure => self.closure_get_all(),
        }
    }
    fn as_any(self) -> Box<dyn std::any::Any> {
        Box::new(self)
    }
}

//#[prof]
impl<T: serde::ser::Serialize + serde::de::DeserializeOwned + Sync + Send> Drop
    for LamellarRequestHandle<T>
{
    fn drop(&mut self) {
        trace!("Request dropping {:?} {:?}", self.id, self.am_type);
        self.active.store(false, Ordering::SeqCst);
    }
}

use crate::active_messaging::{AmDist, LamellarAny};
use crate::lamellae::{Des, SerializedData};
use crate::lamellar_arch::LamellarArchRT;
use crate::lamellar_team::LamellarTeamRT;
use async_trait::async_trait;
use crossbeam::utils::CachePadded;
use lamellar_prof::*;
use log::trace;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

static CUR_REQ_ID: AtomicUsize = AtomicUsize::new(0);
pub(crate) enum InternalResult {
    Local(LamellarAny),     // a local result from a local am (possibly a returned one)
    Remote(SerializedData), // a remte result from a remote am
    Unit,
}

#[derive(Clone, Debug)]
pub(crate) struct InternalReq {
    pub(crate) data_tx: crossbeam::channel::Sender<(usize, InternalResult)>, //what if we create an enum for either bytes or the raw data?
    pub(crate) cnt: Arc<CachePadded<AtomicUsize>>,
    pub(crate) team_outstanding_reqs: Arc<AtomicUsize>,
    pub(crate) world_outstanding_reqs: Arc<AtomicUsize>,
    pub(crate) tg_outstanding_reqs: Option<Arc<AtomicUsize>>,
    pub(crate) team_hash: u64,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
}

#[async_trait]
pub trait LamellarRequest {
    type Output;
    async fn into_future(self: Box<Self>) -> Option<Self::Output>;
    fn get(&self) -> Option<Self::Output>;
    fn get_all(&self) -> Vec<Option<Self::Output>>;
    // fn as_any(self) -> Box<dyn std::any::Any>;
}

pub struct LamellarRequestHandle<T: AmDist> {
    pub(crate) id: usize,
    pub(crate) cnt: usize,
    pub(crate) data_rx: crossbeam::channel::Receiver<(usize, InternalResult)>,
    pub(crate) active: Arc<AtomicBool>,
    pub(crate) arch: Arc<LamellarArchRT>,
    pub(crate) am_type: AmType,
    pub(crate) _phantom: std::marker::PhantomData<T>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum AmType {
    RegisteredFunction,
    // #[allow(dead_code)]
    // RemoteClosure,
}

//#[prof]
// impl<T: AmDist + 'static> LamellarRequestHandle<T> {
impl<T: AmDist> LamellarRequestHandle<T> {
    pub(crate) fn new<'a>(
        num_pes: usize,
        am_type: AmType,
        arch: Arc<LamellarArchRT>,
        team_reqs: Arc<AtomicUsize>,
        world_reqs: Arc<AtomicUsize>,
        tg_reqs: Option<Arc<AtomicUsize>>,
        team_hash: u64,
        team: Pin<Arc<LamellarTeamRT>>,
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
            team_outstanding_reqs: team_reqs,
            world_outstanding_reqs: world_reqs,
            tg_outstanding_reqs: tg_reqs,
            team_hash: team_hash,
            team: team,
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

    fn process_result(&self, data: InternalResult) -> Option<T> {
        match data {
            InternalResult::Local(x) => {
                if let Ok(result) = x.downcast::<T>() {
                    Some(*result)
                } else {
                    None
                }
            }
            InternalResult::Remote(x) => {
                if let Ok(result) = x.deserialize_data() {
                    //crate::deserialize(&x) {
                    Some(result)
                } else {
                    None
                }
            }
            InternalResult::Unit => None,
        }
    }

    fn am_get(&self) -> Option<T> {
        let (_pe, data) = self.data_rx.recv().expect("result recv");
        self.process_result(data)
    }

    fn am_get_all(&self) -> Vec<Option<T>> {
        let mut res = vec![];
        for _i in 0..self.cnt {
            res.push(None);
        }
        if self.cnt > 1 {
            let mut cnt = self.cnt;
            while cnt > 0 {
                let (pe, data) = self.data_rx.recv().expect("result recv");
                if let Ok(pe) = self.arch.team_pe(pe) {
                    res[pe] = self.process_result(data);
                    cnt -= 1;
                }
            }
        } else {
            res[0] = self.am_get();
        }
        res
    }

    // fn closure_get(&self) -> Option<T> {
    //     let (_pe, data) = self.data_rx.recv().expect("result recv");
    //     self.process_result(data)
    // }

    // fn closure_get_all(&self) -> std::vec::Vec<Option<T>> {
    //     let mut res = vec![]; //= vec![&None; self.cnt];
    //     for _i in 0..self.cnt {
    //         res.push(None);
    //     }
    //     let mut cnt = self.cnt;
    //     while cnt > 0 {
    //         let (pe, data) = self.data_rx.recv().expect("result recv");
    //         res[pe]=self.process_result(data);
    //         cnt -= 1;
    //     }
    //     res
    // }
}

#[async_trait]
impl<T: AmDist> LamellarRequest for LamellarRequestHandle<T> {
    type Output = T;
    async fn into_future(self: Box<Self>) -> Option<Self::Output> {
        let mut res = self.data_rx.try_recv();
        while res.is_err() {
            res = self.data_rx.try_recv();
            async_std::task::yield_now().await;
        }
        if let Ok((_pe, data)) = res {
            self.process_result(data)
        } else {
            None
        }
    }

    fn get(&self) -> Option<Self::Output> {
        match self.am_type {
            AmType::RegisteredFunction => self.am_get(),
            // AmType::RemoteClosure => self.closure_get(),
        }
    }

    fn get_all(&self) -> Vec<Option<Self::Output>> {
        match self.am_type {
            AmType::RegisteredFunction => self.am_get_all(),
            // AmType::RemoteClosure => self.closure_get_all(),
        }
    }
    // fn as_any(self) -> Box<dyn std::any::Any> {
    //     Box::new(self)
    // }
}

//#[prof]
impl<T: AmDist> Drop for LamellarRequestHandle<T> {
    fn drop(&mut self) {
        trace!("Request dropping {:?} {:?}", self.id, self.am_type);
        self.active.store(false, Ordering::SeqCst);
    }
}

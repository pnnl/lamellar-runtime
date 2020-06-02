use crate::runtime::LAMELLAR_RT;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Debug)]
pub(crate) struct InternalReq {
    pub(crate) data_tx: crossbeam::channel::Sender<(usize, Option<std::vec::Vec<u8>>)>,
    pub(crate) cnt: Arc<AtomicUsize>,
    pub(crate) start: std::time::Instant,
    pub(crate) size: usize,
    pub(crate) active: Arc<AtomicBool>,
}

pub struct LamellarRequest<T: serde::de::DeserializeOwned + std::clone::Clone> {
    pub(crate) id: usize,
    pub(crate) cnt: usize,
    pub(crate) data_rx: crossbeam::channel::Receiver<(usize, Option<std::vec::Vec<u8>>)>,
    pub(crate) active: Arc<AtomicBool>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: serde::de::DeserializeOwned + std::clone::Clone> LamellarRequest<T> {
    // #[flame]
    pub(crate) fn new(num_pes: usize) -> (LamellarRequest<T>, InternalReq) {
        let active = Arc::new(AtomicBool::new(true));
        let (s, r) = crossbeam::channel::unbounded();
        let id = LAMELLAR_RT
            .counters
            .cur_req_id
            .fetch_add(1, Ordering::SeqCst);
        let ireq = InternalReq {
            data_tx: s,
            cnt: Arc::new(AtomicUsize::new(num_pes)),
            start: Instant::now(),
            size: 0,
            active: active.clone(),
        };
        (
            LamellarRequest {
                id: id,
                cnt: num_pes,
                data_rx: r,
                active: active.clone(),
                _phantom: std::marker::PhantomData,
            },
            ireq,
        )
    }

    pub fn get(&self) -> Option<T> {
        let (_pe, data) = self.data_rx.recv().expect("result recv");
        match data {
            Some(x) => {
                let result: T = bincode::deserialize(&x).expect("result deserialize");
                Some(result)
            }
            None => None,
        }
    }

    pub fn get_all(&self) -> std::vec::Vec<Option<T>> {
        let mut res: std::vec::Vec<Option<T>> = vec![None; self.cnt];
        let mut cnt = self.cnt;
        while cnt > 0 {
            let (pe, data) = self.data_rx.recv().expect("result recv");
            match data {
                Some(x) => {
                    let result: T = bincode::deserialize(&x).expect("result deserialize");
                    res[pe] = Some(result);
                }
                None => res[pe] = None,
            }
            cnt -= 1;
        }
        res
    }
}

impl<T: serde::de::DeserializeOwned + std::clone::Clone> Drop for LamellarRequest<T> {
    fn drop(&mut self) {
        self.active.store(false, Ordering::SeqCst);
    }
}

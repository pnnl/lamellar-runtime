use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

static CUR_REQ_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Debug)]
pub(crate) struct InternalReq {
    pub(crate) data_tx: crossbeam::channel::Sender<(usize, Option<std::vec::Vec<u8>>)>, //what if we create an enum for either bytes or the raw data?
    pub(crate) cnt: Arc<AtomicUsize>,
    pub(crate) start: std::time::Instant,
    pub(crate) size: usize,
    pub(crate) active: Arc<AtomicBool>,
    pub(crate) team_outstanding_reqs: Arc<AtomicUsize>,
    pub(crate) world_outstanding_reqs: Arc<AtomicUsize>,
}

pub struct LamellarRequest<T: serde::de::DeserializeOwned> {
    pub(crate) id: usize,
    pub(crate) cnt: usize,
    pub(crate) data_rx: crossbeam::channel::Receiver<(usize, Option<std::vec::Vec<u8>>)>,
    pub(crate) active: Arc<AtomicBool>,
    // pub(crate) type: ExecType,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: 'static + serde::de::DeserializeOwned> LamellarRequest<T> {
    pub(crate) fn new<'a>(
        num_pes: usize,
        team_reqs: Arc<AtomicUsize>,
        world_reqs: Arc<AtomicUsize>,
    ) -> (LamellarRequest<T>, InternalReq) {
        let active = Arc::new(AtomicBool::new(true));
        let (s, r) = crossbeam::channel::unbounded();
        let id = CUR_REQ_ID.fetch_add(1, Ordering::SeqCst);
        let ireq = InternalReq {
            data_tx: s,
            cnt: Arc::new(AtomicUsize::new(num_pes)),
            start: Instant::now(),
            size: 0,
            active: active.clone(),
            team_outstanding_reqs: team_reqs,
            world_outstanding_reqs: world_reqs,
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
    #[allow(dead_code)]
    fn am_get(&self) -> Option<T> {
        // let (_pe, data) = self.data_rx.recv().expect("result recv");
        // match data {
        //     Some(x) => {
        //         let result: Box<dyn LamellarDataReturn> =
        //             bincode::deserialize(&x).expect("result deserialize");
        //         let result = result.into_any();
        //         if let Ok(result) = result.downcast::<T>() {
        //             Some(*result)
        //         } else {
        //             println!("result was not a LamellarDataReturn");
        //             None
        //         }
        //     }
        //     None => None,
        // }
        None
    }

    pub fn am_get_new(&self) -> Option<T> {
        let (_pe, data) = self.data_rx.recv().expect("result recv");
        match data {
            Some(x) => {
                // let result:LamellarReturn =
                //     bincode::deserialize(&x).expect("result deserialize");
                // if let LamellarReturn::NewData(raw_result) = result {
                if let Ok(result) = bincode::deserialize(&x) {
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

    pub fn am_get_all(&self) -> Vec<Option<T>> {
        let mut res: std::vec::Vec<Option<T>> = Vec::new(); //= vec![&None; self.cnt];
        for _i in 0..self.cnt {
            res.push(None);
        }
        if self.cnt > 1 {
            let mut cnt = self.cnt;
            while cnt > 0 {
                let (pe, data) = self.data_rx.recv().expect("result recv");
                match data {
                    Some(x) => {
                        // let result: Box<dyn LamellarDataReturn> =
                        //     bincode::deserialize(&x).expect("result deserialize");
                        // let result = result.into_any();
                        // if let Ok(result) = result.downcast::<T>() {
                        //     res[pe] = Some(*result);
                        // } else {
                        //     println!("result was not a LamellarDataReturn");
                        //     res[pe] = None;
                        // }
                        if let Ok(result) = bincode::deserialize(&x) {
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
        } else {
            res[0] = self.am_get_new();
        }
        res
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
        let mut res: std::vec::Vec<Option<T>> = Vec::new(); //= vec![&None; self.cnt];
        for _i in 0..self.cnt {
            res.push(None);
        }
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

impl<T: serde::de::DeserializeOwned> Drop for LamellarRequest<T> {
    fn drop(&mut self) {
        self.active.store(false, Ordering::SeqCst);
    }
}

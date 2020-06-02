use crate::lamellar_request::InternalReq;
use crate::runtime::LAMELLAR_RT;
use crate::runtime::*;
use crate::schedulers::Scheduler;
use rand::prelude::*;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

pub(crate) struct WorkStealing {
    threads: Vec<std::thread::JoinHandle<()>>,
    work_inj: Arc<crossbeam::deque::Injector<Vec<u8>>>,
    work_stealers: Vec<crossbeam::deque::Stealer<Vec<u8>>>,
    msg_inj: Arc<crossbeam::deque::Injector<(usize, Msg, InternalReq, LamellarAny)>>,
    msg_stealers: Vec<crossbeam::deque::Stealer<(usize, Msg, InternalReq, LamellarAny)>>,
    num_threads: usize,
    num_pes: usize,
}

struct WorkStealingWorker {
    _id: core_affinity::CoreId,
    work_inj: Arc<crossbeam::deque::Injector<Vec<u8>>>,
    work_stealers: Vec<crossbeam::deque::Stealer<Vec<u8>>>,
    work_q: crossbeam::deque::Worker<Vec<u8>>,
    work_flag: Arc<AtomicU8>,
    msg_inj: Arc<crossbeam::deque::Injector<(usize, Msg, InternalReq, LamellarAny)>>,
    msg_stealers: Vec<crossbeam::deque::Stealer<(usize, Msg, InternalReq, LamellarAny)>>,
    msg_q: crossbeam::deque::Worker<(usize, Msg, InternalReq, LamellarAny)>,
    msg_flag: Arc<AtomicU8>,
}

impl Scheduler for WorkStealing {
    fn new() -> WorkStealing {
        // println!("new work stealling");
        let mut scheduler = WorkStealing {
            threads: Vec::new(),
            work_inj: Arc::new(crossbeam::deque::Injector::new()),
            work_stealers: Vec::new(),
            msg_inj: Arc::new(crossbeam::deque::Injector::new()),
            msg_stealers: Vec::new(),
            num_threads: 1,
            num_pes: LAMELLAR_RT.arch.num_pes,
        };
        scheduler.num_threads = match std::env::var("LAMELLAR_THREADS") {
            Ok(n) => n.parse::<usize>().unwrap(),
            Err(_) => 4,
        };
        // println!("num threads: {:?}", scheduler.num_threads);
        let core_ids = core_affinity::get_core_ids().unwrap();
        let mut msg_workers: std::vec::Vec<
            crossbeam::deque::Worker<(usize, Msg, InternalReq, LamellarAny)>,
        > = vec![];
        let mut work_workers: std::vec::Vec<crossbeam::deque::Worker<Vec<u8>>> = vec![];

        for _i in 0..scheduler.num_threads {
            let msg_worker: crossbeam::deque::Worker<(usize, Msg, InternalReq, LamellarAny)> =
                crossbeam::deque::Worker::new_fifo();
            let work_worker: crossbeam::deque::Worker<Vec<u8>> =
                crossbeam::deque::Worker::new_fifo();
            scheduler.msg_stealers.push(msg_worker.stealer());
            scheduler.work_stealers.push(work_worker.stealer());
            msg_workers.push(msg_worker);
            work_workers.push(work_worker);
        }
        let work_flag = Arc::new(AtomicU8::new(0));
        let msg_flag = Arc::new(AtomicU8::new(0));
        for tid in 0..scheduler.num_threads {
            let msg_worker = msg_workers.pop().unwrap();
            let work_worker = work_workers.pop().unwrap();
            let worker = WorkStealingWorker {
                _id: core_ids[(tid + 2) % core_ids.len()].clone(),
                work_inj: scheduler.work_inj.clone(),
                work_stealers: scheduler.work_stealers.clone(),
                work_q: work_worker,
                work_flag: work_flag.clone(),
                msg_inj: scheduler.msg_inj.clone(),
                msg_stealers: scheduler.msg_stealers.clone(),
                msg_q: msg_worker,
                msg_flag: msg_flag.clone(),
            };
            scheduler.threads.push(std::thread::spawn(move || {
                WorkStealing::init_threads(worker)
            }));
        }
        scheduler
    }

    fn submit_req(&self, pe: usize, msg: Msg, ireq: InternalReq, func: LamellarAny) {
        self.msg_inj.push((pe, msg, ireq, func));
    }

    fn submit_req_all(&self, msg: Msg, ireq: InternalReq, func: LamellarAny) {
        self.msg_inj.push((self.num_pes, msg, ireq, func));
    }

    fn submit_work(&self, msg: std::vec::Vec<u8>) {
        self.work_inj.push(msg);
    }
}

impl WorkStealing {
    fn init_threads(worker: WorkStealingWorker) {
        let work_inj = worker.work_inj;
        let work_stealers = worker.work_stealers;
        let work_q = worker.work_q;
        let work_flag = worker.work_flag;
        let msg_inj = worker.msg_inj;
        let msg_stealers = worker.msg_stealers;
        let msg_q = worker.msg_q;
        let msg_flag = worker.msg_flag;
        let my_pe = LAMELLAR_RT.arch.my_pe;
        let num_pes = LAMELLAR_RT.arch.num_pes;
        let mut rng = rand::thread_rng();
        let t = rand::distributions::Uniform::from(0..msg_stealers.len());
        loop {
            let mut prog = true;
            while prog {
                let omsg = msg_q.pop().or_else(|| {
                    if msg_flag.compare_and_swap(0, 1, Ordering::SeqCst) == 0 {
                        let ret = msg_inj.steal_batch_and_pop(&msg_q).success();
                        msg_flag.store(0, Ordering::SeqCst);
                        ret
                    } else {
                        msg_stealers[t.sample(&mut rng)].steal().success()
                    }
                });

                prog = match omsg {
                    Some(msg) => {
                        WorkStealing::process_msg(msg, my_pe, num_pes);
                        true
                    }
                    None => false,
                };
            }

            let omsg = work_q.pop().or_else(|| {
                if work_flag.compare_and_swap(0, 1, Ordering::SeqCst) == 0 {
                    let ret = work_inj.steal_batch_and_pop(&work_q).success();
                    work_flag.store(0, Ordering::SeqCst);
                    ret
                } else {
                    work_stealers[t.sample(&mut rng)].steal().success()
                }
                // work_inj
                //     .steal_batch_and_pop(&work_q)
                //     .or_else(|| work_stealers[t.sample(&mut rng)].steal())
                //     .success()
            });
            if let Some(msg) = omsg {
                let (msg, ser_data): (Msg, Vec<u8>) = bincode::deserialize(&msg).unwrap();
                exec_msg(msg, ser_data);
            }
        }
    }
}

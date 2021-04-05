use crate::active_messaging::{ActiveMessageEngine, LamellarAny, Msg};
use crate::lamellae::{Backend, LamellaeAM};
use crate::lamellar_request::InternalReq;
use crate::lamellar_team::LamellarArch;
use crate::schedulers::{ReqData, Scheduler, SchedulerQueue};

use log::{debug, trace};
use rand::prelude::*;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

pub(crate) struct WorkStealingThread {
    work_inj: Arc<crossbeam::deque::Injector<(Vec<u8>, Arc<dyn LamellaeAM>)>>,
    work_stealers: Vec<crossbeam::deque::Stealer<(Vec<u8>, Arc<dyn LamellaeAM>)>>,
    work_q: crossbeam::deque::Worker<(Vec<u8>, Arc<dyn LamellaeAM>)>,
    work_flag: Arc<AtomicU8>,

    msg_inj: Arc<crossbeam::deque::Injector<ReqData>>,
    msg_stealers: Vec<crossbeam::deque::Stealer<ReqData>>,
    msg_q: crossbeam::deque::Worker<ReqData>,
    msg_flag: Arc<AtomicU8>,

    active: Arc<AtomicBool>,
    timers: BTreeMap<String, AtomicUsize>,
}

impl WorkStealingThread {
    fn run(
        worker: WorkStealingThread,
        ame: Arc<ActiveMessageEngine>,
        _um_pe: usize,
        my_pe: usize,
        lamellaes: Arc<BTreeMap<Backend, Arc<dyn LamellaeAM>>>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            trace!("[{:?}] TestSchdulerWorker thread running", my_pe);

            let mut rng = rand::thread_rng();
            let t = rand::distributions::Uniform::from(0..worker.msg_stealers.len());
            while worker.active.load(Ordering::SeqCst)
                || !(worker.msg_q.is_empty()
                    && worker.msg_inj.is_empty()
                    && worker.work_q.is_empty()
                    && worker.work_inj.is_empty())
            {
                let mut prog = true;
                let ot = Instant::now();
                while prog {
                    let omsg = worker.msg_q.pop().or_else(|| {
                        if worker.msg_flag.compare_and_swap(0, 1, Ordering::SeqCst) == 0 {
                            //only let one thread steal a batch from main injector at a time
                            let ret = worker.msg_inj.steal_batch_and_pop(&worker.msg_q).success();
                            worker.msg_flag.store(0, Ordering::SeqCst);
                            ret
                        } else {
                            worker.msg_stealers[t.sample(&mut rng)].steal().success()
                        }
                    });
                    prog = match omsg {
                        Some(msg) => {
                            let it = Instant::now();
                            ame.process_msg(msg, &lamellaes);
                            // WorkStealingThread::process_msg(
                            //     msg,
                            //     my_pe,
                            //     num_pe,
                            //     &ame,
                            //     &lamellaes,
                            //     &worker.timers,
                            // );
                            (*worker.timers.get("process_msg_inner").unwrap())
                                .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
                            true
                        }
                        None => false,
                    };
                }
                (*worker.timers.get("process_msg_outer").unwrap())
                    .fetch_add(ot.elapsed().as_millis() as usize, Ordering::Relaxed);

                let ot = Instant::now();
                let omsg = worker.work_q.pop().or_else(|| {
                    if worker.work_flag.compare_and_swap(0, 1, Ordering::SeqCst) == 0 {
                        let ret = worker
                            .work_inj
                            .steal_batch_and_pop(&worker.work_q)
                            .success();
                        worker.work_flag.store(0, Ordering::SeqCst);
                        ret
                    } else {
                        worker.work_stealers[t.sample(&mut rng)].steal().success()
                    }
                });
                if let Some((msg, lamellae)) = omsg {
                    let it = Instant::now();
                    debug!("msg len: {:?}", msg.len());
                    let (msg, ser_data): (Msg, Vec<u8>) = crate::deserialize(&msg).unwrap();
                    debug!("ser_Data: {:?}", ser_data.len());
                    ame.exec_msg(msg, ser_data, lamellae);
                    (*worker.timers.get("exec_msg_inner").unwrap())
                        .fetch_add(it.elapsed().as_millis() as usize, Ordering::Relaxed);
                }
                (*worker.timers.get("exec_msg_outer").unwrap())
                    .fetch_add(ot.elapsed().as_millis() as usize, Ordering::Relaxed);
            }
            let mut string = String::new();
            for item in worker.timers.iter() {
                string.push_str(&format!(
                    "{:?} {:?} ",
                    item.0,
                    item.1.load(Ordering::SeqCst) as f64 / 1000.0 as f64
                ));
            }
            // println!("thread timers: {:?} {:?}", my_pe, string);
            trace!("[{:?}] TestSchdulerWorker thread shutting down", my_pe);
        })
    }
}

pub(crate) struct WorkStealingQueue {
    work_inj: Arc<crossbeam::deque::Injector<(Vec<u8>, Arc<dyn LamellaeAM>)>>,
    work_stealers: Vec<crossbeam::deque::Stealer<(Vec<u8>, Arc<dyn LamellaeAM>)>>,

    msg_inj: Arc<crossbeam::deque::Injector<ReqData>>,
    msg_stealers: Vec<crossbeam::deque::Stealer<ReqData>>,
}

impl SchedulerQueue for WorkStealingQueue {
    fn new() -> WorkStealingQueue {
        trace!("new work stealing queue");
        let queue = WorkStealingQueue {
            work_inj: Arc::new(crossbeam::deque::Injector::new()),
            work_stealers: Vec::new(),
            msg_inj: Arc::new(crossbeam::deque::Injector::new()),
            msg_stealers: Vec::new(),
        };
        queue
    }
    fn submit_req(
        &self,
        src: usize,
        pe: Option<usize>,
        msg: Msg,
        ireq: InternalReq,
        func: LamellarAny,
        team: Arc<dyn LamellarArch>,
        backend: Backend,
    ) {
        let req_data = ReqData {
            team: team,
            src: src,
            pe: pe,
            msg: msg,
            ireq: ireq,
            func: func,
            backend: backend,
        };
        self.msg_inj.push(req_data);
    }
    fn submit_work(&self, msg: std::vec::Vec<u8>, lamellae: Arc<dyn LamellaeAM>) {
        self.work_inj.push((msg, lamellae));
    }
}

impl WorkStealingQueue {
    fn init(&mut self, active: Arc<AtomicBool>) -> Vec<WorkStealingThread> {
        let mut work_workers: std::vec::Vec<
            crossbeam::deque::Worker<(Vec<u8>, Arc<dyn LamellaeAM>)>,
        > = vec![];
        let mut msg_workers: std::vec::Vec<crossbeam::deque::Worker<ReqData>> = vec![];
        let num_workers = match std::env::var("LAMELLAR_THREADS") {
            Ok(n) => n.parse::<usize>().unwrap(),
            Err(_) => 4,
        };
        for _i in 0..num_workers {
            let work_worker: crossbeam::deque::Worker<(Vec<u8>, Arc<dyn LamellaeAM>)> =
                crossbeam::deque::Worker::new_fifo();
            self.work_stealers.push(work_worker.stealer());
            work_workers.push(work_worker);

            let msg_worker: crossbeam::deque::Worker<ReqData> =
                crossbeam::deque::Worker::new_fifo();
            self.msg_stealers.push(msg_worker.stealer());
            msg_workers.push(msg_worker);
        }
        let mut workers: Vec<WorkStealingThread> = vec![];
        let work_flag = Arc::new(AtomicU8::new(0));
        let msg_flag = Arc::new(AtomicU8::new(0));
        for _i in 0..num_workers {
            let work_worker = work_workers.pop().unwrap();
            let msg_worker = msg_workers.pop().unwrap();
            let mut worker = WorkStealingThread {
                work_inj: self.work_inj.clone(),
                work_stealers: self.work_stealers.clone(),
                work_q: work_worker,
                work_flag: work_flag.clone(),

                msg_inj: self.msg_inj.clone(),
                msg_stealers: self.msg_stealers.clone(),
                msg_q: msg_worker,
                msg_flag: msg_flag.clone(),

                active: active.clone(),
                timers: BTreeMap::new(),
            };
            worker
                .timers
                .insert("process_msg_outer".to_string(), AtomicUsize::new(0));
            worker
                .timers
                .insert("process_msg_inner".to_string(), AtomicUsize::new(0));
            worker
                .timers
                .insert("exec_msg_outer".to_string(), AtomicUsize::new(0));
            worker
                .timers
                .insert("exec_msg_inner".to_string(), AtomicUsize::new(0));
            worker
                .timers
                .insert("local_am".to_string(), AtomicUsize::new(0));
            worker
                .timers
                .insert("local_closure".to_string(), AtomicUsize::new(0));
            worker
                .timers
                .insert("network".to_string(), AtomicUsize::new(0));
            worker
                .timers
                .insert("network_cnt".to_string(), AtomicUsize::new(0));
            worker
                .timers
                .insert("network_size".to_string(), AtomicUsize::new(0));
            worker
                .timers
                .insert("serde_1".to_string(), AtomicUsize::new(0));
            worker
                .timers
                .insert("serde_2".to_string(), AtomicUsize::new(0));
            workers.push(worker);
        }
        workers
    }
}

pub(crate) struct WorkStealingScheduler {
    threads: Vec<thread::JoinHandle<()>>,
    workers: Vec<WorkStealingThread>,
    queue: Arc<dyn SchedulerQueue>,
    active: Arc<AtomicBool>,
}

impl WorkStealingScheduler {
    pub(crate) fn new() -> WorkStealingScheduler {
        trace!("new work stealing scheduler");
        let active = Arc::new(AtomicBool::new(true));
        let mut queue = WorkStealingQueue::new();
        let workers = queue.init(active.clone());
        let sched = WorkStealingScheduler {
            workers: workers,
            threads: Vec::new(),
            queue: Arc::new(queue),
            active: active,
        };
        sched
    }
}

impl Scheduler for WorkStealingScheduler {
    fn init(
        &mut self,
        num_pes: usize,
        my_pe: usize,
        lamellaes: BTreeMap<Backend, Arc<dyn LamellaeAM>>,
    ) {
        let lamellaes = Arc::new(lamellaes);
        let ame = Arc::new(ActiveMessageEngine::new(num_pes, my_pe, self.queue.clone()));
        for worker in self.workers.drain(..) {
            self.threads.push(WorkStealingThread::run(
                worker,
                ame.clone(),
                num_pes,
                my_pe,
                lamellaes.clone(),
            ));
        }
    }
    fn get_queue(&self) -> Arc<dyn SchedulerQueue> {
        self.queue.clone()
    }
}

impl Drop for WorkStealingScheduler {
    //when is this called with respect to world?
    fn drop(&mut self) {
        self.active.store(false, Ordering::SeqCst);
        while let Some(thread) = self.threads.pop() {
            let _res = thread.join();
        }
        trace!("WorkStealingScheduler Dropping");
    }
}

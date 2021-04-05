use crate::active_messaging::{ActiveMessageEngine, Cmd, ExecType, LamellarAny, Msg};
use crate::lamellae::{Backend, LamellaeAM};
use crate::lamellar_request::InternalReq;
// use crate::lamellar_arch::LamellarArchRT;
use crate::lamellar_team::LamellarTeamRT;
use crate::schedulers::{ReqData, Scheduler, SchedulerQueue};
use lamellar_prof::*;
use log::trace;
use parking_lot::RwLock;
use rand::prelude::*;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::thread;
// use std::time::Instant;

pub(crate) struct WorkStealingThread {
    work_inj: Arc<crossbeam::deque::Injector<async_task::Runnable>>,
    work_stealers: Vec<crossbeam::deque::Stealer<async_task::Runnable>>,
    work_q: crossbeam::deque::Worker<async_task::Runnable>,
    work_flag: Arc<AtomicU8>,
    // ame: Arc<ActiveMessageEngine>,
    active: Arc<AtomicBool>,
    timers: BTreeMap<String, AtomicUsize>,
}

//#[prof]
impl WorkStealingThread {
    fn run(
        worker: WorkStealingThread,
        _num_pe: usize,
        my_pe: usize,
        _lamellaes: Arc<BTreeMap<Backend, Arc<dyn LamellaeAM>>>,
        active_cnt: Arc<AtomicUsize>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            trace!("[{:?}] TestSchdulerWorker thread running", my_pe);
            active_cnt.fetch_add(1, Ordering::SeqCst);
            let mut rng = rand::thread_rng();
            let t = rand::distributions::Uniform::from(0..worker.work_stealers.len());
            // let mut temptime = Instant::now();
            while worker.active.load(Ordering::SeqCst)
                || !(worker.work_q.is_empty() && worker.work_inj.is_empty())
            {
                // let ot = Instant::now();
                let omsg = worker.work_q.pop().or_else(|| {
                    if worker
                        .work_flag
                        .compare_exchange(0, 1, Ordering::SeqCst, Ordering::Relaxed)
                        == Ok(0)
                    {
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
                if let Some(runnable) = omsg {
                    runnable.run();
                }
                // (*worker.timers.get("exec_msg_outer").unwrap())
                //     .fetch_add(ot.elapsed().as_millis() as usize, Ordering::Relaxed);

                // if temptime.elapsed().as_millis() > 5*1000{
                //     println!("tid:{:?} wq: {:?} wi {:?} msgs: {:?} bi {:?} bp {:?} bs {:?} br {:?} c: {:?} r {:?} rp{:?} bri {:?} brp {:?} brs{:?} brr {:?}"
                //     ,std::thread::current().id(),worker.work_q.is_empty(), worker.work_inj.is_empty(),
                //     worker.ame.msgs.load(Ordering::SeqCst),worker.ame.batches_init.load(Ordering::SeqCst),
                //     worker.ame.batches_proc.load(Ordering::SeqCst),worker.ame.batches_sent.load(Ordering::SeqCst),
                //     worker.ame.batches_recv.load(Ordering::SeqCst), worker.ame.cmds.load(Ordering::SeqCst),
                //     worker.ame.returns.load(Ordering::SeqCst),worker.ame.returns_proc.load(Ordering::SeqCst),
                //     worker.ame.batched_return_init.load(Ordering::SeqCst),worker.ame.batched_return_proc.load(Ordering::SeqCst),
                //     worker.ame.batched_return_sent.load(Ordering::SeqCst), worker.ame.batched_return_recv.load(Ordering::SeqCst),);

                //     temptime = Instant::now();
                // }
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

            fini_prof!();
            trace!("[{:?}] TestSchdulerWorker thread shutting down", my_pe);
        })
    }
}

pub(crate) struct WorkStealingQueue {
    work_inj: Arc<crossbeam::deque::Injector<async_task::Runnable>>,
    work_stealers: Vec<crossbeam::deque::Stealer<async_task::Runnable>>,
    ame: Arc<ActiveMessageEngine>,
}

//#[prof]
impl SchedulerQueue for WorkStealingQueue {
    fn new(
        num_pes: usize,
        my_pe: usize,
        teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
    ) -> WorkStealingQueue {
        trace!("new work stealing queue");
        let ame = ActiveMessageEngine::new(num_pes, my_pe, teams);
        let queue = WorkStealingQueue {
            work_inj: Arc::new(crossbeam::deque::Injector::new()),
            work_stealers: Vec::new(),
            ame: Arc::new(ame),
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
        // team_arch: Arc<dyn LamellarArch>,
        // backend: Backend,
        lamellae: Arc<dyn LamellaeAM>,
        team_hash: u64,
    ) {
        let req_data = ReqData {
            // team_arch: team_arch,
            src: src,
            pe: pe,
            msg: msg,
            ireq: ireq,
            func: func,
            // backend: backend,
            lamellae: lamellae.clone(),
            team_hash: team_hash,
        };
        let ame = self.ame.clone();
        let work_inj = self.work_inj.clone();
        // println!("submitting_req");
        let future = async move {
            // println!("in submit_req");
            if let Some(req_data) = ame.process_msg(req_data).await {
                let future = async move {
                    ame.process_msg(req_data).await;
                };
                let schedule = move |runnable| work_inj.push(runnable);
                let (runnable, task) = async_task::spawn(future, schedule);
                runnable.schedule();
                task.detach();
            }
        };
        let work_inj = self.work_inj.clone();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = async_task::spawn(future, schedule);
        runnable.schedule();
        task.detach();
    }

    fn submit_work(&self, msg: std::vec::Vec<u8>, lamellae: Arc<dyn LamellaeAM>) {
        let ame = self.ame.clone();
        let work_inj = self.work_inj.clone();
        let future = async move {
            let msg = msg;
            let (msg, ser_data, team_hash): (Msg, Vec<u8>, u64) = crate::deserialize(&msg).unwrap();

            if msg.cmd == ExecType::Runtime(Cmd::BatchedMsg) {
                // ame.batches_recv.fetch_add(1,Ordering::SeqCst);
                let mut reqs: Vec<Vec<u8>> = crate::deserialize(&ser_data).unwrap();
                for req in reqs.drain(..) {
                    WorkStealingQueue::submit_work_batch(
                        req,
                        lamellae.clone(),
                        ame.clone(),
                        work_inj.clone(),
                    );
                }
            } else if let Some(req_data) = ame.exec_msg(msg, ser_data, lamellae, team_hash).await {
                let future = async move {
                    ame.process_msg(req_data).await;
                };
                let schedule = move |runnable| work_inj.push(runnable);
                let (runnable, task) = async_task::spawn(future, schedule);
                runnable.schedule();
                task.detach();
            }
        };
        let work_inj = self.work_inj.clone();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = async_task::spawn(future, schedule);
        runnable.schedule();
        task.detach();
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

//#[prof]
impl WorkStealingQueue {
    fn init(&mut self, active: Arc<AtomicBool>) -> Vec<WorkStealingThread> {
        // let mut work_workers: std::vec::Vec<
        //     crossbeam::deque::Worker<(Vec<u8>, Arc<dyn LamellaeAM>)>,
        // > = vec![];
        let mut work_workers: std::vec::Vec<crossbeam::deque::Worker<async_task::Runnable>> =
            vec![];
        let num_workers = match std::env::var("LAMELLAR_THREADS") {
            Ok(n) => n.parse::<usize>().unwrap(),
            Err(_) => 4,
        };
        for _i in 0..num_workers {
            // let work_worker: crossbeam::deque::Worker<(Vec<u8>, Arc<dyn LamellaeAM>)> =
            //     crossbeam::deque::Worker::new_fifo();
            let work_worker: crossbeam::deque::Worker<async_task::Runnable> =
                crossbeam::deque::Worker::new_fifo();
            self.work_stealers.push(work_worker.stealer());
            work_workers.push(work_worker);
        }
        let mut workers: Vec<WorkStealingThread> = vec![];
        let work_flag = Arc::new(AtomicU8::new(0));
        for _i in 0..num_workers {
            let work_worker = work_workers.pop().unwrap();
            let mut worker = WorkStealingThread {
                work_inj: self.work_inj.clone(),
                work_stealers: self.work_stealers.clone(),
                work_q: work_worker,
                work_flag: work_flag.clone(),

                // ame: self.ame.clone(),
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
    fn submit_work_batch(
        msg: std::vec::Vec<u8>,
        lamellae: Arc<dyn LamellaeAM>,
        ame: Arc<ActiveMessageEngine>,
        work_inj: Arc<crossbeam::deque::Injector<async_task::Runnable>>,
    ) {
        let ame = ame.clone();
        let work_inj_clone = work_inj.clone();
        let future = async move {
            let msg = msg;
            let (msg, ser_data, team_hash): (Msg, Vec<u8>, u64) = crate::deserialize(&msg).unwrap();
            if let Some(req_data) = ame.exec_msg(msg, ser_data, lamellae, team_hash).await {
                let future = async move {
                    ame.process_msg(req_data).await;
                };
                let schedule = move |runnable| work_inj_clone.push(runnable);
                let (runnable, task) = async_task::spawn(future, schedule);
                runnable.schedule();
                task.detach();
            }
        };
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = async_task::spawn(future, schedule);
        runnable.schedule();
        task.detach();
    }
}

pub(crate) struct WorkStealingScheduler {
    threads: Vec<thread::JoinHandle<()>>,
    workers: Vec<WorkStealingThread>,
    queue: Arc<dyn SchedulerQueue>,
    active: Arc<AtomicBool>,
}

//#[prof]
impl WorkStealingScheduler {
    pub(crate) fn new(
        num_pes: usize,
        my_pe: usize,
        teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
    ) -> WorkStealingScheduler {
        trace!("new work stealing scheduler");
        let active = Arc::new(AtomicBool::new(true));
        let mut queue = WorkStealingQueue::new(num_pes, my_pe, teams);
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

//#[prof]
impl Scheduler for WorkStealingScheduler {
    fn init(
        &mut self,
        num_pes: usize,
        my_pe: usize,
        lamellaes: BTreeMap<Backend, Arc<dyn LamellaeAM>>,
    ) {
        let lamellaes = Arc::new(lamellaes);
        // let ame = Arc::new(ActiveMessageEngine::new(num_pes, my_pe, self.queue.clone()));
        let active_cnt = Arc::new(AtomicUsize::new(0));
        for worker in self.workers.drain(..) {
            self.threads.push(WorkStealingThread::run(
                worker,
                // ame.clone(),
                num_pes,
                my_pe,
                lamellaes.clone(),
                active_cnt.clone(),
            ));
        }
        while active_cnt.load(Ordering::SeqCst) != self.threads.len() {
            std::thread::yield_now();
        }
    }
    fn get_queue(&self) -> Arc<dyn SchedulerQueue> {
        self.queue.clone()
    }
}

//#[prof]
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

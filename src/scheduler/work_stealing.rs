use crate::active_messaging::{ActiveMessageEngine, Cmd, ExecType, LamellarAny, Msg};
use crate::lamellae_new::{Backend, Lamellae};
use crate::lamellar_request::InternalReq;
use crate::lamellar_team::LamellarTeamRT;
use crate::scheduler::{ReqData, Scheduler, SchedulerQueue};
use lamellar_prof::*;
use log::trace;
use parking_lot::RwLock;
use rand::prelude::*;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::thread;
use futures::Future;
// use std::time::Instant;

pub(crate) struct WorkStealingThread {
    work_inj: Arc<crossbeam::deque::Injector<async_task::Runnable>>,
    work_stealers: Vec<crossbeam::deque::Stealer<async_task::Runnable>>,
    work_q: crossbeam::deque::Worker<async_task::Runnable>,
    work_flag: Arc<AtomicU8>,
    active: Arc<AtomicBool>,
}

//#[prof]
impl WorkStealingThread {
    fn run(
        worker: WorkStealingThread,
        active_cnt: Arc<AtomicUsize>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            trace!("TestSchdulerWorker thread running");
            active_cnt.fetch_add(1, Ordering::SeqCst);
            let mut rng = rand::thread_rng();
            let t = rand::distributions::Uniform::from(0..worker.work_stealers.len());
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
            }
            fini_prof!();
            trace!("TestSchdulerWorker thread shutting down");
        })
    }
}

pub(crate) struct WorkStealing {
    threads: Vec<thread::JoinHandle<()>>,
    work_inj: Arc<crossbeam::deque::Injector<async_task::Runnable>>,
    work_stealers: Vec<crossbeam::deque::Stealer<async_task::Runnable>>,
    ame: Arc<ActiveMessageEngine>,
    active: Arc<AtomicBool>,
}

//#[prof]
impl SchedulerQueue for WorkStealing {
    fn submit_req(
        &self,
        src: usize,
        pe: Option<usize>,
        msg: Msg,
        ireq: InternalReq,
        func: LamellarAny,
        lamellae: Arc<Lamellae>,
        team_hash: u64,
    ) {
        let req_data = ReqData {
            src: src,
            pe: pe,
            msg: msg,
            ireq: ireq,
            func: func,
            lamellae: lamellae.clone(),
            team_hash: team_hash,
        };
        let ame = self.ame.clone();
        let work_inj = self.work_inj.clone();
        // println!("submitting_req");
        let future = async move {
            trace!("in submit_req {:?} {:?} {:?} ", pe.clone(), req_data.src, req_data.pe);
            // if let Some(req_data) = ame.process_msg(req_data).await { //need to update ame for new req_data
            //     let future = async move {
            //         ame.process_msg(req_data).await;
            //     };
            //     let schedule = move |runnable| work_inj.push(runnable);
            //     let (runnable, task) = async_task::spawn(future, schedule);
            //     runnable.schedule();
            //     task.detach();
            // }
        };
        let work_inj = self.work_inj.clone();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = async_task::spawn(future, schedule);
        runnable.schedule();
        task.detach();
    }

    fn submit_work(&self, msg: std::vec::Vec<u8>, lamellae: Arc<Lamellae>) {
        let ame = self.ame.clone();
        let work_inj = self.work_inj.clone();
        let future = async move {
            let msg = msg;
            let (msg, ser_data, team_hash): (Msg, Vec<u8>, u64) = crate::deserialize(&msg).unwrap();

            if msg.cmd == ExecType::Runtime(Cmd::BatchedMsg) {
                let mut reqs: Vec<Vec<u8>> = crate::deserialize(&ser_data).unwrap();
                for req in reqs.drain(..) {
                    WorkStealing::submit_work_batch(
                        req,
                        lamellae.clone(),
                        ame.clone(),
                        work_inj.clone(),
                    );
                }
            } 
            // else if let Some(req_data) = ame.exec_msg(msg, ser_data, lamellae, team_hash).await { //need to update for Lamellae
            //     let future = async move {
            //         ame.process_msg(req_data).await;
            //     };
            //     let schedule = move |runnable| work_inj.push(runnable);
            //     let (runnable, task) = async_task::spawn(future, schedule);
            //     runnable.schedule();
            //     task.detach();
            // }
        };
        let work_inj = self.work_inj.clone();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = async_task::spawn(future, schedule);
        runnable.schedule();
        task.detach();
    }

    fn submit_task<F>(&self,future: F )
    where 
        F: Future<Output = ()> + Send + 'static {
        let work_inj = self.work_inj.clone();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = async_task::spawn(future, schedule);
        runnable.schedule();
        task.detach();
    }

}


//#[prof]
impl WorkStealing {
    pub(crate) fn new(
        num_pes: usize,
        my_pe: usize,
        teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
    ) -> WorkStealing {
        trace!("new work stealing queue");
        
        let mut sched = WorkStealing {
            threads: Vec::new(),
            work_inj: Arc::new(crossbeam::deque::Injector::new()),
            work_stealers: Vec::new(),
            ame: Arc::new(ActiveMessageEngine::new(num_pes, my_pe, teams)),
            active: Arc::new(AtomicBool::new(true)),
        };
        sched.init();
        sched
    }

    fn init(&mut self){
        let mut work_workers: std::vec::Vec<crossbeam::deque::Worker<async_task::Runnable>> =
            vec![];
        let num_workers = match std::env::var("LAMELLAR_THREADS") {
            Ok(n) => n.parse::<usize>().unwrap(),
            Err(_) => 4,
        };
        for _i in 0..num_workers {
            let work_worker: crossbeam::deque::Worker<async_task::Runnable> =
                crossbeam::deque::Worker::new_fifo();
            self.work_stealers.push(work_worker.stealer());
            work_workers.push(work_worker);
        }
        
        let work_flag = Arc::new(AtomicU8::new(0));

        let threads_started = Arc::new(AtomicUsize::new(0));
        for _i in 0..num_workers {
            let work_worker = work_workers.pop().unwrap();
            let mut worker = WorkStealingThread {
                work_inj: self.work_inj.clone(),
                work_stealers: self.work_stealers.clone(),
                work_q: work_worker,
                work_flag: work_flag.clone(),
                active: self.active.clone(),
            };
            self.threads.push(WorkStealingThread::run(
                worker,
                threads_started.clone()
            ));
        }
        while threads_started.load(Ordering::SeqCst) != self.threads.len() {
            std::thread::yield_now();
        }
    }

    fn submit_work_batch(
        msg: std::vec::Vec<u8>,
        lamellae: Arc<Lamellae>,
        ame: Arc<ActiveMessageEngine>,
        work_inj: Arc<crossbeam::deque::Injector<async_task::Runnable>>,
    ) {
        let ame = ame.clone();
        let work_inj_clone = work_inj.clone();
        let future = async move {
            let msg = msg;
            let (msg, ser_data, team_hash): (Msg, Vec<u8>, u64) = crate::deserialize(&msg).unwrap();
            // if let Some(req_data) = ame.exec_msg(msg, ser_data, lamellae, team_hash).await { //need to update for req data
            //     let future = async move {
            //         ame.process_msg(req_data).await;
            //     };
            //     let schedule = move |runnable| work_inj_clone.push(runnable);
            //     let (runnable, task) = async_task::spawn(future, schedule);
            //     runnable.schedule();
            //     task.detach();
            // }
        };
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = async_task::spawn(future, schedule);
        runnable.schedule();
        task.detach();
    }
}

//#[prof]
impl Drop for WorkStealing {
    //when is this called with respect to world?
    fn drop(&mut self) {
        self.active.store(false, Ordering::SeqCst);
        while let Some(thread) = self.threads.pop() {
            let _res = thread.join();
        }
        trace!("WorkStealing Scheduler Dropping");
    }
}

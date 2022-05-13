use crate::active_messaging::{ActiveMessageEngine, ExecType, LamellarFunc};
use crate::lamellae::{Des, Lamellae, LamellaeRDMA, SerializedData};
use crate::lamellar_team::LamellarTeamRT;
use crate::scheduler::{AmeScheduler, AmeSchedulerQueue, ReqData, ReqId,SchedulerQueue};
use lamellar_prof::*;
// use log::trace;
use core_affinity::CoreId;
use crossbeam::deque::Worker;
use futures::Future;
use parking_lot::RwLock;
use rand::prelude::*;
use std::collections::HashMap;
use std::panic;
use std::pin::Pin;
use std::process;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::thread;
// use std::time::Instant;

pub(crate) struct WorkStealingThread {
    work_inj: Arc<crossbeam::deque::Injector<async_task::Runnable>>,
    work_stealers: Vec<crossbeam::deque::Stealer<async_task::Runnable>>,
    work_q: Worker<async_task::Runnable>,
    work_flag: Arc<AtomicU8>,
    active: Arc<AtomicBool>,
}

//#[prof]
impl WorkStealingThread {
    fn run(
        worker: WorkStealingThread,
        active_cnt: Arc<AtomicUsize>,
        num_tasks: Arc<AtomicUsize>,
        id: CoreId,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            // println!("TestSchdulerWorker thread running");
            core_affinity::set_for_current(id);
            active_cnt.fetch_add(1, Ordering::SeqCst);
            let mut rng = rand::thread_rng();
            let t = rand::distributions::Uniform::from(0..worker.work_stealers.len());
            let mut timer = std::time::Instant::now();
            // let mut cur_tasks = num_tasks.load(Ordering::SeqCst);
            while worker.active.load(Ordering::SeqCst)
                || !(worker.work_q.is_empty() && worker.work_inj.is_empty())
                || num_tasks.load(Ordering::SeqCst) > 1
            {
                // let ot = Instant::now();
                // if cur_tasks != num_tasks.load(Ordering::SeqCst){
                //     println!(
                //         "work_q size {:?} work inj size {:?} num_tasks {:?}",
                //         worker.work_q.len(),
                //         worker.work_inj.len(),
                //         num_tasks.load(Ordering::SeqCst)
                //     );
                //     cur_tasks = num_tasks.load(Ordering::SeqCst);

                // }
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
                    if !worker.active.load(Ordering::SeqCst) && timer.elapsed().as_secs_f64() > 60.0
                    {
                        println!("runnable {:?}", runnable);
                        println!(
                            "work_q size {:?} work inj size {:?} num_tasks {:?}",
                            worker.work_q.len(),
                            worker.work_inj.len(),
                            num_tasks.load(Ordering::SeqCst)
                        );
                        timer = std::time::Instant::now();
                    }
                    runnable.run();
                }
                if !worker.active.load(Ordering::SeqCst)
                    && timer.elapsed().as_secs_f64() > 60.0
                    && (worker.work_q.len() > 0 || worker.work_inj.len() > 0)
                {
                    println!(
                        "work_q size {:?} work inj size {:?} num_tasks {:?}",
                        worker.work_q.len(),
                        worker.work_inj.len(),
                        num_tasks.load(Ordering::SeqCst)
                    );
                    timer = std::time::Instant::now();
                }
                if timer.elapsed().as_secs_f64() > 60.0 {
                    println!(
                        "work_q size {:?} work inj size {:?} num_tasks {:?}",
                        worker.work_q.len(),
                        worker.work_inj.len(),
                        num_tasks.load(Ordering::SeqCst)
                    );
                    timer = std::time::Instant::now()
                }
            }
            fini_prof!();
            active_cnt.fetch_sub(1, Ordering::SeqCst);
            // println!("TestSchdulerWorker thread shutting down");
        })
    }
}

pub(crate) struct WorkStealingInner {
    threads: Vec<thread::JoinHandle<()>>,
    work_inj: Arc<crossbeam::deque::Injector<async_task::Runnable>>,
    work_stealers: Vec<crossbeam::deque::Stealer<async_task::Runnable>>,
    work_flag: Arc<AtomicU8>,
    active: Arc<AtomicBool>,
    active_cnt: Arc<AtomicUsize>,
    num_tasks: Arc<AtomicUsize>,
    stall_mark: Arc<AtomicUsize>,
}

impl AmeSchedulerQueue for WorkStealingInner {
    fn submit_req(
        //unserialized request
        &self,
        ame: Arc<ActiveMessageEngine>,
        src: usize,
        dst: Option<usize>,
        cmd: ExecType,
        id: ReqId,
        func: LamellarFunc,
        lamellae: Arc<Lamellae>,
        world: Pin<Arc<LamellarTeamRT>>,
        team: Pin<Arc<LamellarTeamRT>>,
        team_hash: u64,
    ) {
        let req_data = ReqData {
            src: src,
            dst: dst,
            cmd: cmd,
            id: id,
            batch_id: None,
            func: func,
            lamellae: lamellae,
            world: world,
            team: team,
            team_hash: team_hash,
            // rt_req: false,
        };
        // println!("submitting_req");
        // println!("submit req {:?}",self.num_tasks.load(Ordering::Relaxed)+1);
        let num_tasks = self.num_tasks.clone();
        self.stall_mark.fetch_add(1, Ordering::Relaxed);
        let future = async move {
            // println!("exec req {:?}",num_tasks.load(Ordering::Relaxed));
            num_tasks.fetch_add(1, Ordering::Relaxed);
            // println!("in submit_req {:?} {:?} {:?} ", pe.clone(), req_data.src, req_data.pe);
            ame.process_msg_new(req_data).await;
            // println!("num tasks: {:?}",);
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            // println!("done req {:?}",num_tasks.load(Ordering::Relaxed));
        };
        let work_inj = self.work_inj.clone();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = unsafe { async_task::spawn_unchecked(future, schedule) }; //safe as contents are sync+send, and no borrowed variables
        runnable.schedule();
        task.detach();
    }

    //this is a serialized request
    fn submit_work(
        &self,
        ame: Arc<ActiveMessageEngine>,
        data: SerializedData,
        lamellae: Arc<Lamellae>,
    ) {
        // let work_inj = self.work_inj.clone();
        // println!("submit work {:?}",self.num_tasks.load(Ordering::Relaxed));
        let num_tasks = self.num_tasks.clone();
        let future = async move {
            // println!("exec work {:?}",num_tasks.load(Ordering::Relaxed)+1);
            num_tasks.fetch_add(1, Ordering::Relaxed);
            if let Some(header) = data.deserialize_header() {
                let msg = header.msg;
                // println!("msg recieved: {:?}",msg);
                let addr = lamellae.local_addr(msg.src as usize, header.team_hash as usize);
                // println!("from pe {:?} remote addr {:x} local addr {:x}",msg.src,header.team_hash,addr);
                ame.exec_msg(ame.clone(), msg, data, lamellae, addr).await;
            } else {
                data.print();
                panic!("should i be here?");
            }
            // println!("num tasks: {:?}",);
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            // println!("done work {:?}",num_tasks.load(Ordering::Relaxed));
        };
        let work_inj = self.work_inj.clone();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = unsafe { async_task::spawn_unchecked(future, schedule) }; //safe as contents are sync+send, and no borrowed variables
        runnable.schedule();
        task.detach();
    }

    fn submit_task<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send,
    {
        // println!("submit task {:?}",self.num_tasks.load(Ordering::Relaxed));
        let num_tasks = self.num_tasks.clone();
        let future2 = async move {
            // println!("exec task {:?}",num_tasks.load(Ordering::Relaxed)+1);
            num_tasks.fetch_add(1, Ordering::Relaxed);
            future.await;
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            // println!("done task {:?}",num_tasks.load(Ordering::Relaxed));
        };
        let work_inj = self.work_inj.clone();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = unsafe { async_task::spawn_unchecked(future2, schedule) }; //safe //safe as contents are sync+send... may need to do something to enforce lifetime bounds
        runnable.schedule();
        task.detach();
    }
    fn shutdown(&self) {
        // println!("work stealing shuting down {:?}",self.active());
        self.active.store(false, Ordering::SeqCst);
        // println!("work stealing shuting down {:?}",self.active());
        while self.active_cnt.load(Ordering::Relaxed) > 2
            || self.num_tasks.load(Ordering::Relaxed) > 2
        {
            //this should be the recvtask, and alloc_task
            std::thread::yield_now()
        }
        // println!("work stealing shut down {:?}",self.active());
    }

    fn exec_task(&self) {
        let mut rng = rand::thread_rng();
        let t = rand::distributions::Uniform::from(0..self.work_stealers.len());
        let ret = if self
            .work_flag
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::Relaxed)
            == Ok(0)
        {
            let ret = self.work_inj.steal().success();
            self.work_flag.store(0, Ordering::SeqCst);
            ret
        } else {
            self.work_stealers[t.sample(&mut rng)].steal().success()
        };
        if let Some(runnable) = ret {
            runnable.run();
        }
    }

    fn active(&self) -> bool {
        // println!("sched active {:?} {:?}",self.active.load(Ordering::SeqCst) , self.num_tasks.load(Ordering::SeqCst));
        self.active.load(Ordering::SeqCst) || self.num_tasks.load(Ordering::SeqCst) > 2
    }
}

//#[prof]
impl SchedulerQueue for WorkStealing {
    fn submit_req(
        //unserialized request
        &self,
        src: usize,
        dst: Option<usize>,
        cmd: ExecType,
        id: ReqId,
        func: LamellarFunc,
        lamellae: Arc<Lamellae>,
        world: Pin<Arc<LamellarTeamRT>>,
        team: Pin<Arc<LamellarTeamRT>>,
        team_hash: u64,
    ) {
        self.inner.submit_req(
            self.ame.clone(),
            src,
            dst,
            cmd,
            id,
            func,
            lamellae,
            world,
            team,
            team_hash,
        );
    }

    // fn submit_return(&self, src, pe)

    fn submit_work(&self, data: SerializedData, lamellae: Arc<Lamellae>) {
        self.inner.submit_work(self.ame.clone(), data, lamellae);
    }

    fn submit_task<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send,
    {
        self.inner.submit_task(future);
    }

    fn exec_task(&self) {
        self.inner.exec_task();
    }

    fn shutdown(&self) {
        self.inner.shutdown();
    }
    fn active(&self) -> bool {
        self.inner.active()
    }
}

//#[prof]
impl WorkStealingInner {
    pub(crate) fn new(stall_mark: Arc<AtomicUsize>) -> WorkStealingInner {
        // println!("new work stealing queue");

        let mut sched = WorkStealingInner {
            threads: Vec::new(),
            work_inj: Arc::new(crossbeam::deque::Injector::new()),
            work_stealers: Vec::new(),
            work_flag: Arc::new(AtomicU8::new(0)),
            active: Arc::new(AtomicBool::new(true)),
            active_cnt: Arc::new(AtomicUsize::new(0)),
            num_tasks: Arc::new(AtomicUsize::new(0)),
            stall_mark: stall_mark,
        };
        sched.init();
        sched
    }

    fn init(&mut self) {
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

        let orig_hook = panic::take_hook();
        panic::set_hook(Box::new(move |panic_info| {
            // invoke the default handler and exit the process
            orig_hook(panic_info);
            process::exit(1);
        }));
        let core_ids = core_affinity::get_core_ids().unwrap();
        // println!("core_ids: {:?}",core_ids);
        for i in 0..num_workers {
            let work_worker = work_workers.pop().unwrap();
            let worker = WorkStealingThread {
                work_inj: self.work_inj.clone(),
                work_stealers: self.work_stealers.clone(),
                work_q: work_worker,
                work_flag: self.work_flag.clone(),
                active: self.active.clone(),
                // num_tasks: self.num_tasks.clone(),
            };
            self.threads.push(WorkStealingThread::run(
                worker,
                self.active_cnt.clone(),
                self.num_tasks.clone(),
                core_ids[i % core_ids.len()],
            ));
        }
        while self.active_cnt.load(Ordering::SeqCst) != self.threads.len() {
            std::thread::yield_now();
        }
    }
}

pub(crate) struct WorkStealing {
    inner: Arc<AmeScheduler>,
    ame: Arc<ActiveMessageEngine>,
}
impl WorkStealing {
    pub(crate) fn new(
        _num_pes: usize,
        my_pe: usize,
        teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
    ) -> WorkStealing {
        // println!("new work stealing queue");
        let stall_mark = Arc::new(AtomicUsize::new(0));
        let inner = Arc::new(AmeScheduler::WorkStealingInner(WorkStealingInner::new(
            stall_mark.clone(),
        )));
        let sched = WorkStealing {
            inner: inner.clone(),
            ame: Arc::new(ActiveMessageEngine::new(
                my_pe,
                inner.clone(),
                teams,
                stall_mark.clone(),
            )),
        };
        sched
    }
}

//#[prof]
impl Drop for WorkStealingInner {
    //when is this called with respect to world?
    fn drop(&mut self) {
        // println!("dropping work stealing");
        while let Some(thread) = self.threads.pop() {
            if thread.thread().id() != std::thread::current().id() {
                let _res = thread.join();
            }
        }
        // println!("WorkStealing Scheduler Dropped");
    }
}

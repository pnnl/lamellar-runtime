use crate::active_messaging::{ActiveMessageEngine, ActiveMessageEngineType, Am};
use crate::lamellae::{Des, Lamellae, SerializedData};
use crate::scheduler::batching::simple_batcher::SimpleBatcher;
use crate::scheduler::batching::team_am_batcher::TeamAmBatcher;
use crate::scheduler::batching::BatcherType;
use crate::scheduler::registered_active_message::RegisteredActiveMessages;
use crate::scheduler::{AmeScheduler, AmeSchedulerQueue, SchedulerQueue};

//use tracing::*;

use async_task::{Builder, Runnable};
use core_affinity::CoreId;
use crossbeam::deque::Worker;
use futures::Future;
use futures_lite::FutureExt;
// use parking_lot::Mutex;
use rand::prelude::*;
// use std::collections::BTreeMap;
use std::panic;
use std::process;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc; //, Weak};
use std::thread;
// use std::time::Instant;
// use std::time::Instant;

const ACTIVE: u8 = 0;
const FINISHED: u8 = 1;
const PANIC: u8 = 2;

static TASK_ID: AtomicUsize = AtomicUsize::new(0);

// static LAST_PRINTED_TASKS: AtomicUsize = AtomicUsize::new(0);

// static OUTSTANDING_REQS:  Mutex<HashMap<usize,usize>> = parking_lot::const_mutex(HashMap::new());
// lazy_static!{ static ref OUTSTANDING_REQS: Mutex<BTreeMap<usize,usize>> = Mutex::new(BTreeMap::new()); }

#[derive(Debug)]
pub(crate) struct WorkStealingThread {
    imm_inj: Arc<crossbeam::deque::Injector<Runnable<usize>>>,
    work_inj: Arc<crossbeam::deque::Injector<Runnable<usize>>>,
    work_stealers: Vec<crossbeam::deque::Stealer<Runnable<usize>>>,
    work_q: Worker<Runnable<usize>>,
    work_flag: Arc<AtomicU8>,
    active: Arc<AtomicU8>,
    panic: Arc<AtomicU8>,
}

impl WorkStealingThread {
    //#[tracing::instrument(skip_all)]
    fn run(
        worker: WorkStealingThread,
        active_cnt: Arc<AtomicUsize>,
        num_tasks: Arc<AtomicUsize>,
        _max_tasks: Arc<AtomicUsize>,
        id: CoreId,
        _my_pe: usize,
    ) -> thread::JoinHandle<()> {
        let builder = thread::Builder::new().name("worker_thread".into());
        builder
            .spawn(move || {
                // println!("TestSchdulerWorker thread running {:?} core: {:?}", std::thread::current().id(), id);
                // let mut num_task_executed = 0;
                // let _span = trace_span!("WorkStealingThread::run");
                core_affinity::set_for_current(id);
                active_cnt.fetch_add(1, Ordering::SeqCst);
                let mut rng = rand::thread_rng();
                let t = rand::distributions::Uniform::from(0..worker.work_stealers.len());
                let mut timer = std::time::Instant::now();
                // let mut cur_tasks = num_tasks.load(Ordering::SeqCst);
                while worker.panic.load(Ordering::SeqCst) == 0
                    && (worker.active.load(Ordering::SeqCst) == ACTIVE
                        || !(worker.work_q.is_empty()
                            && worker.work_inj.is_empty()
                            && worker.imm_inj.is_empty())
                        || num_tasks.load(Ordering::SeqCst) > 1)
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
                    let omsg = if !worker.imm_inj.is_empty() {
                        worker.imm_inj.steal().success()
                    } else {
                        worker.work_q.pop().or_else(|| {
                            if worker.work_flag.compare_exchange(
                                0,
                                1,
                                Ordering::SeqCst,
                                Ordering::Relaxed,
                            ) == Ok(0)
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
                        })
                    };

                    if let Some(runnable) = omsg {
                        if worker.active.load(Ordering::SeqCst) == FINISHED
                            && timer.elapsed().as_secs_f64() > *crate::DEADLOCK_TIMEOUT
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
                        // *OUTSTANDING_REQS.lock().entry(*runnable.metadata()).or_insert(0) += 1;
                        // if LAST_PRINTED_TASKS.load(Ordering::Relaxed) != num_tasks.load(Ordering::Relaxed) {
                        //     LAST_PRINTED_TASKS.store(num_tasks.load(Ordering::Relaxed), Ordering::Relaxed);
                        //     let work_stealers_lens = worker.work_stealers.iter().map(|x| x.len()).collect::<Vec<_>>();
                        //     println!("[{:?}] (worker thread) Executing task {:?}, num_tasks: {:?} {:?} {:?} {work_stealers_lens:?} {:?}", std::thread::current().id(), runnable.metadata(),num_tasks.load(Ordering::Relaxed), worker.imm_inj.len(),worker.work_inj.len(), OUTSTANDING_REQS.lock());
                        // }
                        runnable.run();
                    }
                    if worker.active.load(Ordering::SeqCst) == FINISHED
                        && timer.elapsed().as_secs_f64() > *crate::DEADLOCK_TIMEOUT
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
                    // if timer.elapsed().as_secs_f64() > 10.0 {
                    //     println!(
                    //         "[{:?}] work_q size {:?} work inj size {:?} num_tasks {:?} {:?} {:?}",
                    //         std::thread::current().id(),
                    //         worker.work_q.len(),
                    //         worker.work_inj.len(),
                    //         num_tasks.load(Ordering::SeqCst),
                    //         worker.active.load(Ordering::SeqCst) == FINISHED,
                    //         OUTSTANDING_REQS.lock()
                    //     );
                    //     timer = std::time::Instant::now()
                    // }
                    std::thread::yield_now();
                }
                active_cnt.fetch_sub(1, Ordering::SeqCst);
                // println!("TestSchdulerWorker thread shutting down");
            })
            .unwrap()
    }
}

#[derive(Debug)]
pub(crate) struct WorkStealingInner {
    threads: Vec<thread::JoinHandle<()>>,
    imm_inj: Arc<crossbeam::deque::Injector<Runnable<usize>>>,
    work_inj: Arc<crossbeam::deque::Injector<Runnable<usize>>>,
    work_stealers: Vec<crossbeam::deque::Stealer<Runnable<usize>>>,
    work_flag: Arc<AtomicU8>,
    active: Arc<AtomicU8>,
    active_cnt: Arc<AtomicUsize>,
    num_tasks: Arc<AtomicUsize>,
    max_tasks: Arc<AtomicUsize>,
    stall_mark: Arc<AtomicUsize>,
    panic: Arc<AtomicU8>,
}

impl AmeSchedulerQueue for WorkStealingInner {
    //#[tracing::instrument(skip_all)]
    fn submit_am(
        //unserialized request
        &self,
        scheduler: impl SchedulerQueue + Sync + Send + Clone + std::fmt::Debug + 'static,
        ame: Arc<ActiveMessageEngineType>,
        am: Am,
    ) {
        // println!("[{:?}] submitting_req", std::thread::current().id());
        // println!("submit req {:?}",self.num_tasks.load(Ordering::Relaxed)+1);
        let num_tasks = self.num_tasks.clone();
        let max_tasks = self.max_tasks.clone();
        let stall_mark = self.stall_mark.fetch_add(1, Ordering::Relaxed);
        let future = move |_cur_task| async move {
            num_tasks.fetch_add(1, Ordering::Relaxed);
            max_tasks.fetch_add(1, Ordering::Relaxed);
            // println!("[{:?}] submit am exec req {:?} {:?} TaskId: {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),_cur_task);
            ame.process_msg(am, scheduler, stall_mark, false).await;
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            // let mut reqs = OUTSTANDING_REQS.lock();
            // reqs.remove(cur_task);
            // println!("[{:?}] submit am done {:?} {:?} TaskId: {:?} ", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),_cur_task);
        };
        let work_inj = self.work_inj.clone();
        // let schedule = move |runnable| work_inj.push(runnable);
        let schedule = move |runnable| work_inj.push(runnable);
        // let (runnable, task) = unsafe { async_task::spawn(future, schedule) }; //safe as contents are sync+send, and no borrowed variables
        let (runnable, task) = unsafe {
            Builder::new()
                .metadata(TASK_ID.fetch_add(1, Ordering::Relaxed))
                .spawn_unchecked(future, schedule)
        };
        // println!("[{:?}] submit am schedule task {:?} {:?} {:?}", std::thread::current().id(),runnable.metadata(),self.num_tasks.load(Ordering::Relaxed),self.max_tasks.load(Ordering::Relaxed));
        runnable.schedule();
        task.detach();
    }

    //#[tracing::instrument(skip_all)]
    fn submit_am_immediate(
        //unserialized request
        &self,
        scheduler: impl SchedulerQueue + Sync + Send + Clone + std::fmt::Debug + 'static,
        ame: Arc<ActiveMessageEngineType>,
        am: Am,
    ) {
        // println!("submitting_req");
        // println!("submit req {:?}",self.num_tasks.load(Ordering::Relaxed)+1);
        let num_tasks = self.num_tasks.clone();
        let max_tasks = self.max_tasks.clone();
        let stall_mark = self.stall_mark.fetch_add(1, Ordering::Relaxed);
        let future = move |_cur_task| async move {
            num_tasks.fetch_add(1, Ordering::Relaxed);
            max_tasks.fetch_add(1, Ordering::Relaxed);
            // println!("[{:?}] submit am imm exec req {:?} {:?} TaskId: {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task);
            ame.process_msg(am, scheduler, stall_mark, true).await;
            num_tasks.fetch_sub(1, Ordering::Relaxed);

            // let mut reqs = OUTSTANDING_REQS.lock();
            // reqs.remove(cur_task);
            // println!("[{:?}] submit am imm done {:?} {:?} TaskId: {:?} {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task, reqs);
        };
        let work_inj = self.work_inj.clone();
        // let schedule = move |runnable| work_inj.push(runnable);
        let schedule = move |runnable| work_inj.push(runnable);
        // let (runnable, task) = unsafe { async_task::spawn(future, schedule) }; //safe as contents are sync+send, and no borrowed variables
        let (runnable, task) = unsafe {
            Builder::new()
                .metadata(TASK_ID.fetch_add(1, Ordering::Relaxed))
                .spawn_unchecked(future, schedule)
        };
        // println!("[{:?}] submit am imm running task {:?} {:?} {:?}", std::thread::current().id(),runnable.metadata(),self.num_tasks.load(Ordering::Relaxed),self.max_tasks.load(Ordering::Relaxed));
        // *OUTSTANDING_REQS.lock().entry(*runnable.metadata()).or_insert(0) += 1;
        runnable.run();
        task.detach();
    }

    //this is a serialized request
    //#[tracing::instrument(skip_all)]
    fn submit_work(
        &self,
        scheduler: impl SchedulerQueue + Sync + Send + Clone + std::fmt::Debug + 'static,
        ame: Arc<ActiveMessageEngineType>,
        data: SerializedData,
        lamellae: Arc<Lamellae>,
    ) {
        // let work_inj = self.work_inj.clone();
        // println!("submit work {:?}", self.num_tasks.load(Ordering::Relaxed));
        let num_tasks = self.num_tasks.clone();
        let max_tasks = self.max_tasks.clone();
        let future = move |_cur_task| async move {
            num_tasks.fetch_add(1, Ordering::Relaxed);
            max_tasks.fetch_add(1, Ordering::Relaxed);
            // println!("[{:?}] submit work exec req {:?} {:?} TaskId: {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task);
            if let Some(header) = data.deserialize_header() {
                let msg = header.msg;
                ame.exec_msg(msg, data, lamellae, scheduler).await;
            } else {
                data.print();
                panic!("should i be here?");
            }
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            // let mut reqs = OUTSTANDING_REQS.lock();
            // reqs.remove(cur_task);
            // println!("[{:?}] submit work done req {:?} {:?} TaskId: {:?} {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task,reqs);
        };
        let work_inj = self.work_inj.clone();
        // let schedule = move |runnable| work_inj.push(runnable);
        let schedule = move |runnable| work_inj.push(runnable);
        // let (runnable, task) = unsafe { async_task::spawn(future, schedule) }; //safe as contents are sync+send, and no borrowed variables
        let (runnable, task) = unsafe {
            Builder::new()
                .metadata(TASK_ID.fetch_add(1, Ordering::Relaxed))
                .spawn_unchecked(future, schedule)
        };
        // println!("[{:?}] submit work schedule task {:?} {:?} {:?}", std::thread::current().id(),runnable.metadata(),self.num_tasks.load(Ordering::Relaxed),self.max_tasks.load(Ordering::Relaxed));

        runnable.schedule();
        task.detach();
    }

    fn submit_task<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // trace_span!("submit_task").in_scope(|| {
        let num_tasks = self.num_tasks.clone();
        let max_tasks = self.max_tasks.clone();
        let future2 = move |_cur_task: &_| async move {
            num_tasks.fetch_add(1, Ordering::Relaxed);
            max_tasks.fetch_add(1, Ordering::Relaxed);
            // println!("[{:?}] submit task exec req {:?} {:?} TaskId: {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task);
            future.await;
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            // let mut reqs = OUTSTANDING_REQS.lock();
            // reqs.remove(cur_task);
            // println!("[{:?}] submit task done {:?} {:?} TaskId: {:?} {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task,reqs);
        };
        let work_inj = self.work_inj.clone();
        // let schedule = move |runnable| work_inj.push(runnable);
        let schedule = move |runnable| work_inj.push(runnable);
        // let (runnable, task) = unsafe { async_task::spawn(future2, schedule) }; //safe //safe as contents are sync+send... may need to do something to enforce lifetime bounds
        let (runnable, task) = Builder::new()
            .metadata(TASK_ID.fetch_add(1, Ordering::Relaxed))
            .spawn(future2, schedule);
        // println!("[{:?}] submit task schedule task {:?} {:?} {:?}", std::thread::current().id(),runnable.metadata(),self.num_tasks.load(Ordering::Relaxed),self.max_tasks.load(Ordering::Relaxed));

        runnable.schedule();
        task.detach();
        // });
    }

    fn submit_immediate_task<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // trace_span!("submit_task").in_scope(|| {
        let num_tasks = self.num_tasks.clone();
        let max_tasks = self.max_tasks.clone();
        let future2 = move |_cur_task: &_| async move {
            // println!("exec task {:?}",num_tasks.load(Ordering::Relaxed)+1);
            num_tasks.fetch_add(1, Ordering::Relaxed);
            max_tasks.fetch_add(1, Ordering::Relaxed);
            // println!("[{:?}] submit imm task exec req {:?} {:?} TaskId: {:?} ", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task);

            future.await;
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            // let mut reqs = OUTSTANDING_REQS.lock();
            // reqs.remove(cur_task);
            // println!("[{:?}] submit imm task exec done {:?} {:?} TaskId: {:?} {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task,reqs);
        };
        let work_inj = self.work_inj.clone();
        // let schedule = move |runnable| work_inj.push(runnable);
        let schedule = move |runnable| work_inj.push(runnable);
        // let (runnable, task) = unsafe { async_task::spawn(future2, schedule) }; //safe //safe as contents are sync+send... may need to do something to enforce lifetime bounds
        let (runnable, task) = Builder::new()
            .metadata(TASK_ID.fetch_add(1, Ordering::Relaxed))
            .spawn(future2, schedule);
        // println!("[{:?}] submit imm task schedule task {:?} {:?} {:?}", std::thread::current().id(),runnable.metadata(),self.num_tasks.load(Ordering::Relaxed),self.max_tasks.load(Ordering::Relaxed));
        // *OUTSTANDING_REQS.lock().entry(*runnable.metadata()).or_insert(0) += 1;
        runnable.run(); //try to run immediately
        task.detach();
        // });
    }

    fn submit_immediate_task2<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // trace_span!("submit_task").in_scope(|| {
        let num_tasks = self.num_tasks.clone();
        let max_tasks = self.max_tasks.clone();
        let future2 = move |_cur_task: &_| async move {
            num_tasks.fetch_add(1, Ordering::Relaxed);
            max_tasks.fetch_add(1, Ordering::Relaxed);
            // println!("[{:?}] submit imm2 task exec req {:?} {:?} TaskId: {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task);

            future.await;
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            //     let mut reqs = OUTSTANDING_REQS.lock();
            // reqs.remove(cur_task);
            // println!("[{:?}] submit imm2 task exec done {:?} {:?} TaskId: {:?} {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task, reqs);
        };
        let imm_inj = self.imm_inj.clone();
        // let schedule = move |runnable| imm_inj.push(runnable);
        let schedule = move |runnable| imm_inj.push(runnable);
        // let (runnable, task) = unsafe { async_task::spawn(future2, schedule) }; //safe //safe as contents are sync+send... may need to do something to enforce lifetime bounds
        let (runnable, task) = Builder::new()
            .metadata(TASK_ID.fetch_add(1, Ordering::Relaxed))
            .spawn(future2, schedule);
        // println!("[{:?}] submit imm2 task schedule task {:?} {:?} {:?}", std::thread::current().id(),runnable.metadata(),self.num_tasks.load(Ordering::Relaxed),self.max_tasks.load(Ordering::Relaxed));

        runnable.schedule(); //try to run immediately
        task.detach();
        // });
    }

    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        // trace_span!("block_on").in_scope(|| {
        // println!(
        //     "[{:?}] work stealing block on -- num tasks {:?} max tasks {:?}  tasks executed {:?}",
        //     std::thread::current().id(),
        //     self.num_tasks.load(Ordering::Relaxed),
        //     self.max_tasks.load(Ordering::Relaxed),
        //     0
        // );
        let num_tasks = self.num_tasks.clone();
        let max_tasks = self.max_tasks.clone();
        let future2 = move |_cur_task| async move {
            num_tasks.fetch_add(1, Ordering::Relaxed);
            max_tasks.fetch_add(1, Ordering::Relaxed);
            // println!("[{:?}] block on task exec req {:?} {:?} TaskId: {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task);

            let res = future.await;
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            // let mut reqs = OUTSTANDING_REQS.lock();
            // reqs.remove(cur_task);
            // println!("[{:?}] block on task exec done {:?} {:?} TaskId: {:?} {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task, reqs);
            res
        };
        let work_inj = self.work_inj.clone();
        // let schedule = move |runnable| work_inj.push(runnable);
        let schedule = move |runnable| work_inj.push(runnable);

        // let (runnable, mut task) = unsafe { async_task::spawn(future, schedule) }; //safe //safe as contents are sync+send... may need to do something to enforce lifetime bounds
        let (runnable, mut task) = unsafe {
            Builder::new()
                .metadata(TASK_ID.fetch_add(1, Ordering::Relaxed))
                .spawn_unchecked(future2, schedule)
        };
        let waker = runnable.waker();
        // *OUTSTANDING_REQS.lock().entry(*runnable.metadata()).or_insert(0) += 1;
        runnable.run(); //try to run immediately
                        // let mut s = std::time::Instant::now();
                        // let mut cnt = 0;
        while !task.is_finished() {
            self.exec_task();
            // if s.elapsed().as_secs() > 10 {
            //     println!(
            //         "[{:?}] work stealing block on timeout -- num tasks {:?} max tasks {:?}  tasks executed {:?} task id{:?} {:?}",
            //         std::thread::current().id(),
            //         self.num_tasks.load(Ordering::Relaxed),
            //         self.max_tasks.load(Ordering::Relaxed),
            //         cnt,
            //         task.metadata(),
            //         OUTSTANDING_REQS.lock(),
            //     );
            //     s = std::time::Instant::now();
            //     break;
            // }
            // cnt += 1;
            // std::thread::yield_now();
        }
        let cx = &mut async_std::task::Context::from_waker(&waker);
        if let async_std::task::Poll::Ready(output) = task.poll(cx) {
            // println!(
            //     "[{:?}] work stealing block on done -- num tasks {:?} max tasks {:?}  tasks executed {:?} task id{:?}",
            //     std::thread::current().id(),
            //     self.num_tasks.load(Ordering::Relaxed),
            //     self.max_tasks.load(Ordering::Relaxed),
            //     cnt,
            //     task.metadata()
            // );
            output
        } else {
            println!(
                    "[{:?}] work stealing block on failed -- num tasks {:?} max tasks {:?}   task id{:?}",
                    std::thread::current().id(),
                    self.num_tasks.load(Ordering::Relaxed),
                    self.max_tasks.load(Ordering::Relaxed),
                    task.metadata()
                );
            panic!("task not ready");
        }

        // })
    }

    //#[tracing::instrument(skip_all)]
    fn shutdown(&self) {
        // println!("work stealing shuting down {:?}", self.active());
        self.active.store(FINISHED, Ordering::SeqCst);
        // println!("work stealing shuting down {:?}",self.active());
        while self.panic.load(Ordering::SeqCst) == 0
            && (self.active_cnt.load(Ordering::Relaxed) > 0 //num active threads
            || self.num_tasks.load(Ordering::Relaxed) > 2)
        {
            //this should be the recvtask, and alloc_task
            std::thread::yield_now()
        }
        // println!(
        //     "work stealing shut down {:?} {:?} {:?}",
        //     self.active(),
        //     self.active_cnt.load(Ordering::Relaxed),
        //     self.active_cnt.load(Ordering::Relaxed)
        // );
    }

    //#[tracing::instrument(skip_all)]
    fn shutdown_threads(&self) {
        self.active.store(FINISHED, Ordering::SeqCst);
    }

    //#[tracing::instrument(skip_all)]
    fn force_shutdown(&self) {
        // println!("work stealing shuting down {:?}", self.active());
        self.active.store(PANIC, Ordering::SeqCst);
        // println!("work stealing shuting down {:?}",self.active());
        let my_id = std::thread::current().id();
        if self.threads.iter().any(|e| e.thread().id() == my_id) {
            // while self.active_cnt.load(Ordering::Relaxed) > 1 {//num active threads -- wait for all but myself
            //     std::thread::yield_now()
            // }
            self.active_cnt.fetch_sub(1, Ordering::SeqCst); // I paniced so I wont actually decrement
        } else {
            while self.active_cnt.load(Ordering::Relaxed) > 0 {
                //num active threads
                self.exec_task();
                std::thread::yield_now()
            }
        }
        // println!(
        //     "work stealing shut down {:?} {:?} {:?}",
        //     self.active(),
        //     self.active_cnt.load(Ordering::Relaxed),
        //     self.active_cnt.load(Ordering::Relaxed)
        // );
    }

    //#[tracing::instrument(skip_all)]
    fn exec_task(&self) {
        let mut rng = rand::thread_rng();
        let t = rand::distributions::Uniform::from(0..self.work_stealers.len());
        let ret = if !self.imm_inj.is_empty() {
            self.imm_inj.steal().success()
        } else {
            if self
                .work_flag
                .compare_exchange(0, 1, Ordering::SeqCst, Ordering::Relaxed)
                == Ok(0)
            {
                let ret = self.work_inj.steal().success();
                self.work_flag.store(0, Ordering::SeqCst);
                ret
            } else {
                self.work_stealers[t.sample(&mut rng)].steal().success()
            }
        };
        if let Some(runnable) = ret {
            // if LAST_PRINTED_TASKS.load(Ordering::Relaxed) != self.num_tasks.load(Ordering::Relaxed) {
            //     LAST_PRINTED_TASKS.store(self.num_tasks.load(Ordering::Relaxed), Ordering::Relaxed);
            //     let work_stealers_lens = self.work_stealers.iter().map(|x| x.len()).collect::<Vec<_>>();
            //     // println!("[{:?}] (exec_task) Executing task {:?}, num_tasks: {:?} {:?} {:?} {work_stealers_lens:?} {:?}", std::thread::current().id(), runnable.metadata(),self.num_tasks.load(Ordering::Relaxed), self.imm_inj.len(),self.work_inj.len(), OUTSTANDING_REQS.lock());
            // }
            // *OUTSTANDING_REQS.lock().entry(*runnable.metadata()).or_insert(0) += 1;
            runnable.run();
        }
    }

    //#[tracing::instrument(skip_all)]
    fn active(&self) -> bool {
        // println!("sched active {:?} {:?}",self.active.load(Ordering::SeqCst) , self.num_tasks.load(Ordering::SeqCst));
        self.active.load(Ordering::SeqCst) == ACTIVE || self.num_tasks.load(Ordering::SeqCst) > 3
    }
}

impl SchedulerQueue for Arc<WorkStealing> {
    fn submit_am(
        //unserialized request
        &self,
        am: Am,
    ) {
        self.inner.submit_am(self.clone(), self.ame.clone(), am);
    }

    fn submit_am_immediate(
        //unserialized request
        &self,
        am: Am,
    ) {
        self.inner
            .submit_am_immediate(self.clone(), self.ame.clone(), am);
    }

    // fn submit_return(&self, src, pe)

    fn submit_work(&self, data: SerializedData, lamellae: Arc<Lamellae>) {
        self.inner
            .submit_work(self.clone(), self.ame.clone(), data, lamellae);
    }

    fn submit_task<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.inner.submit_task(future);
    }

    fn submit_immediate_task<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.inner.submit_immediate_task(future);
    }

    fn submit_immediate_task2<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.inner.submit_immediate_task2(future);
    }

    fn exec_task(&self) {
        self.inner.exec_task();
        std::thread::yield_now();
    }

    fn submit_task_node<F>(&self, future: F, _node: usize)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.inner.submit_task(future);
    }

    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        self.inner.block_on(future)
    }

    fn shutdown(&self) {
        self.inner.shutdown();
    }

    fn shutdown_threads(&self) {
        self.inner.shutdown_threads();
    }

    fn force_shutdown(&self) {
        self.inner.force_shutdown();
    }
    fn active(&self) -> bool {
        self.inner.active()
    }
    fn num_workers(&self) -> usize {
        self.max_num_threads
    }
}

impl WorkStealingInner {
    //#[tracing::instrument(skip_all)]
    pub(crate) fn new(
        stall_mark: Arc<AtomicUsize>,
        num_workers: usize,
        panic: Arc<AtomicU8>,
        my_pe: usize,
    ) -> WorkStealingInner {
        // println!("new work stealing queue");

        let mut sched = WorkStealingInner {
            threads: Vec::new(),
            imm_inj: Arc::new(crossbeam::deque::Injector::new()),
            work_inj: Arc::new(crossbeam::deque::Injector::new()),
            work_stealers: Vec::new(),
            work_flag: Arc::new(AtomicU8::new(0)),
            active: Arc::new(AtomicU8::new(ACTIVE)),
            active_cnt: Arc::new(AtomicUsize::new(0)),
            num_tasks: Arc::new(AtomicUsize::new(0)),
            max_tasks: Arc::new(AtomicUsize::new(0)),
            stall_mark: stall_mark,
            panic: panic,
        };
        sched.init(num_workers, my_pe);
        sched
    }

    //#[tracing::instrument(skip_all)]
    fn init(&mut self, num_workers: usize, my_pe: usize) {
        let mut work_workers: std::vec::Vec<crossbeam::deque::Worker<Runnable<usize>>> = vec![];
        for _i in 0..num_workers {
            let work_worker: crossbeam::deque::Worker<Runnable<usize>> =
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
        let core_ids = match core_affinity::get_core_ids() {
            Some(core_ids) => core_ids,
            None => {
                vec![core_affinity::CoreId { id: 0 }]
            }
        };
        // println!("core_ids: {:?}",core_ids);
        for i in 0..num_workers {
            let work_worker = work_workers.pop().unwrap();
            let worker = WorkStealingThread {
                imm_inj: self.imm_inj.clone(),
                work_inj: self.work_inj.clone(),
                work_stealers: self.work_stealers.clone(),
                work_q: work_worker,
                work_flag: self.work_flag.clone(),
                active: self.active.clone(),
                panic: self.panic.clone(),
                // num_tasks: self.num_tasks.clone(),
            };
            self.threads.push(WorkStealingThread::run(
                worker,
                self.active_cnt.clone(),
                self.num_tasks.clone(),
                self.max_tasks.clone(),
                core_ids[i % core_ids.len()],
                my_pe,
            ));
        }
        while self.active_cnt.load(Ordering::SeqCst) != self.threads.len() {
            std::thread::yield_now();
        }
    }
}

#[derive(Debug)]
pub(crate) struct WorkStealing {
    inner: Arc<AmeScheduler>,
    ame: Arc<ActiveMessageEngineType>,
    max_num_threads: usize, //including the main thread
}
impl WorkStealing {
    //#[tracing::instrument(skip_all)]
    pub(crate) fn new(
        num_pes: usize,
        num_workers: usize,
        panic: Arc<AtomicU8>,
        my_pe: usize,
        // teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
    ) -> WorkStealing {
        // println!("new work stealing queue");
        let stall_mark = Arc::new(AtomicUsize::new(0));
        let inner = Arc::new(AmeScheduler::WorkStealingInner(WorkStealingInner::new(
            stall_mark.clone(),
            num_workers,
            panic.clone(),
            my_pe,
        )));

        let batcher = match std::env::var("LAMELLAR_BATCHER") {
            Ok(n) => {
                let n = n.parse::<usize>().unwrap();
                if n == 1 {
                    BatcherType::Simple(SimpleBatcher::new(num_pes, stall_mark.clone()))
                } else {
                    BatcherType::TeamAm(TeamAmBatcher::new(num_pes, stall_mark.clone()))
                }
            }
            Err(_) => BatcherType::TeamAm(TeamAmBatcher::new(num_pes, stall_mark.clone())),
        };

        let sched = WorkStealing {
            inner: inner.clone(),
            ame: Arc::new(ActiveMessageEngineType::RegisteredActiveMessages(Arc::new(
                RegisteredActiveMessages::new(batcher),
            ))),
            max_num_threads: num_workers,
        };
        sched
    }
}

impl Drop for WorkStealingInner {
    //when is this called with respect to world?
    //#[tracing::instrument(skip_all)]
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

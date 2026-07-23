use crate::active_messaging::batching::simple_batcher::io_task_stats;
use crate::env_var::config;
use crate::scheduler::{
    Executor, LamellarExecutor, LamellarTask, LamellarTaskInner, SchedulerStatus,
};
use crate::LAMELLAR_THREAD_ID;

//use tracing::*;

use async_task::{Builder, Runnable};
use core_affinity::CoreId;
use crossbeam::deque::Worker;
use futures_util::Future;
use rand::prelude::*;
use std::collections::BTreeMap;
use std::panic;
use std::pin::Pin;
use std::process;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use tracing::{debug, trace, trace_span, Instrument};
//, Weak};
use std::thread;

static TASK_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub(crate) enum TaskType {
    Spawn,
    Submit,
    LongSubmit,
    IO,
    Immediate,
    BlockOn,
    AmSubmit,
    AmImmediate,
    AmExec,
    AmRemote,
    TaskSpawn,
    TaskSubmit,
    TaskLongSubmit,
    TaskImmediate,
    TaskIo,
    TaskExec,
    SchedBlockOn,
}

lazy_static! {
    pub(crate) static ref TASKS_LAUNCHED: BTreeMap<TaskType, AtomicUsize> = {
        let mut m = BTreeMap::new();
        m.insert(TaskType::Spawn, AtomicUsize::new(0));
        m.insert(TaskType::Submit, AtomicUsize::new(0));
        m.insert(TaskType::LongSubmit, AtomicUsize::new(0));
        m.insert(TaskType::IO, AtomicUsize::new(0));
        m.insert(TaskType::Immediate, AtomicUsize::new(0));
        m.insert(TaskType::BlockOn, AtomicUsize::new(0));
        m.insert(TaskType::AmSubmit, AtomicUsize::new(0));
        m.insert(TaskType::AmImmediate, AtomicUsize::new(0));
        m.insert(TaskType::AmExec, AtomicUsize::new(0));
        m.insert(TaskType::AmRemote, AtomicUsize::new(0));
        m.insert(TaskType::TaskSpawn, AtomicUsize::new(0));
        m.insert(TaskType::TaskSubmit, AtomicUsize::new(0));
        m.insert(TaskType::TaskLongSubmit, AtomicUsize::new(0));
        m.insert(TaskType::TaskImmediate, AtomicUsize::new(0));
        m.insert(TaskType::TaskIo, AtomicUsize::new(0));
        m.insert(TaskType::TaskExec, AtomicUsize::new(0));
        m.insert(TaskType::SchedBlockOn, AtomicUsize::new(0));
        m
    };
    pub(crate) static ref TASKS_FINISHED: BTreeMap<TaskType, AtomicUsize> = {
        let mut m = BTreeMap::new();
        m.insert(TaskType::Spawn, AtomicUsize::new(0));
        m.insert(TaskType::Submit, AtomicUsize::new(0));
        m.insert(TaskType::LongSubmit, AtomicUsize::new(0));
        m.insert(TaskType::IO, AtomicUsize::new(0));
        m.insert(TaskType::Immediate, AtomicUsize::new(0));
        m.insert(TaskType::BlockOn, AtomicUsize::new(0));
        m.insert(TaskType::AmSubmit, AtomicUsize::new(0));
        m.insert(TaskType::AmImmediate, AtomicUsize::new(0));
        m.insert(TaskType::AmExec, AtomicUsize::new(0));
        m.insert(TaskType::AmRemote, AtomicUsize::new(0));
        m.insert(TaskType::TaskSpawn, AtomicUsize::new(0));
        m.insert(TaskType::TaskSubmit, AtomicUsize::new(0));
        m.insert(TaskType::TaskLongSubmit, AtomicUsize::new(0));
        m.insert(TaskType::TaskImmediate, AtomicUsize::new(0));
        m.insert(TaskType::TaskIo, AtomicUsize::new(0));
        m.insert(TaskType::TaskExec, AtomicUsize::new(0));
        m.insert(TaskType::SchedBlockOn, AtomicUsize::new(0));
        m
    };
}

pub(crate) fn task_launched_to_string() -> String {
    let mut s = String::new();
    for (task_type, counter) in TASKS_LAUNCHED.iter() {
        let task_type_str = match task_type {
            TaskType::Spawn => "Spawn",
            TaskType::Submit => "Submit",
            TaskType::LongSubmit => "LongSubmit",
            TaskType::IO => "IO",
            TaskType::Immediate => "Immediate",
            TaskType::BlockOn => "BlockOn",
            TaskType::AmSubmit => "AmSubmit",
            TaskType::AmImmediate => "AmImmediate",
            TaskType::AmExec => "AmExec",
            TaskType::AmRemote => "AmRemote",
            TaskType::TaskSpawn => "TaskSpawn",
            TaskType::TaskSubmit => "TaskSubmit",
            TaskType::TaskLongSubmit => "TaskLongSubmit",
            TaskType::TaskImmediate => "TaskImmediate",
            TaskType::TaskIo => "TaskIo",
            TaskType::TaskExec => "TaskExec",
            TaskType::SchedBlockOn => "SchedBlockOn",
        };
        s.push_str(&format!(
            "{}: {}, ",
            task_type_str,
            counter.load(Ordering::Relaxed)
        ));
    }
    s
}

pub(crate) fn task_finished_to_string() -> String {
    let mut s = String::new();
    for (task_type, counter) in TASKS_FINISHED.iter() {
        let task_type_str = match task_type {
            TaskType::Spawn => "Spawn",
            TaskType::Submit => "Submit",
            TaskType::LongSubmit => "LongSubmit",
            TaskType::IO => "IO",
            TaskType::Immediate => "Immediate",
            TaskType::BlockOn => "BlockOn",
            TaskType::AmSubmit => "AmSubmit",
            TaskType::AmImmediate => "AmImmediate",
            TaskType::AmExec => "AmExec",
            TaskType::AmRemote => "AmRemote",
            TaskType::TaskSpawn => "TaskSpawn",
            TaskType::TaskSubmit => "TaskSubmit",
            TaskType::TaskLongSubmit => "TaskLongSubmit",
            TaskType::TaskImmediate => "TaskImmediate",
            TaskType::TaskIo => "TaskIo",
            TaskType::TaskExec => "TaskExec",
            TaskType::SchedBlockOn => "SchedBlockOn",
        };
        s.push_str(&format!(
            "{}: {}, ",
            task_type_str,
            counter.load(Ordering::Relaxed)
        ));
    }
    s
}

#[derive(Debug)]
pub(crate) struct WorkStealingThread {
    thread_only_inj: Arc<crossbeam::deque::Injector<Runnable<usize>>>,
    imm_inj: Arc<crossbeam::deque::Injector<Runnable<usize>>>,
    work_inj: Arc<crossbeam::deque::Injector<Runnable<usize>>>,
    work_stealers: Vec<crossbeam::deque::Stealer<Runnable<usize>>>,
    work_q: Worker<Runnable<usize>>,
    work_flag: Arc<AtomicU8>,
    status: Arc<AtomicU8>,
    panic: Arc<AtomicU8>,
    
}

impl WorkStealingThread {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn run(
        worker: WorkStealingThread,
        active_cnt: Arc<AtomicUsize>,
        // num_tasks: Arc<AtomicUsize>,
        ids: Arc<Vec<CoreId>>,
        _my_pe: usize,

    ) -> thread::JoinHandle<()> {
        let builder = thread::Builder::new().name("worker_thread".into());
        builder
            .spawn(move || {
                
                let tid = LAMELLAR_THREAD_ID.with(|tid| *tid);
                // let log_name= format!("lamellar_log-pe-{}-thread-{}-", my_pe, tid);
                // file_per_thread_logger::initialize(&log_name);
                let id = ids[tid % ids.len()];
                trace!(
                    "WorkStealing Worker thread running {:?} core: {:?} tid: {:?}",
                    std::thread::current().id(),
                    id,
                    tid
                );
                
                let _span = trace_span!("WorkStealingThread::run");
                core_affinity::set_for_current(id);
                active_cnt.fetch_add(1, Ordering::SeqCst);
                let mut rng = rand::rng();
                let t = rand::distr::Uniform::try_from(0..worker.work_stealers.len()).expect("error getting uniform distribution");
                let mut timer = std::time::Instant::now();
                while worker.panic.load(Ordering::SeqCst) == 0
                    && (
                        worker.status.load(Ordering::SeqCst) == SchedulerStatus::Active as u8
                            || !(worker.work_q.is_empty()
                                && worker.work_inj.is_empty()
                                && worker.imm_inj.is_empty()
                                && worker.thread_only_inj.is_empty())
                        // || num_tasks.load(Ordering::SeqCst) > 1
                    )
                {
                    let omsg = if !worker.imm_inj.is_empty() {
                        worker.imm_inj.steal().success()
                    } else {
                        match worker
                            .thread_only_inj
                            .steal_batch_and_pop(&worker.work_q)
                            .success()
                        {
                            Some(runnable) => Some(runnable),
                            None => worker.work_q.pop().or_else(|| {
                                if worker.work_flag.compare_exchange(
                                    0,
                                    1,
                                    Ordering::SeqCst,
                                    Ordering::Relaxed,
                                ) == Ok(0)
                                {
                                    let ret = if worker.work_inj.len() < worker.work_stealers.len() * 5 {
                                        worker.work_inj.steal_batch_and_pop(&worker.work_q).success()
                                    } else {
                                        worker.work_inj.steal().success()
                                    };
                                    worker.work_flag.store(0, Ordering::SeqCst);
                                    ret
                                } else {
                                    worker.work_stealers[t.sample(&mut rng)].steal().success()
                                }
                            }),
                        }
                    };

                    if let Some(runnable) = omsg {
                        if worker.status.load(Ordering::SeqCst) == SchedulerStatus::Finished as u8
                            && timer.elapsed().as_secs_f64() > config().deadlock_warning_timeout
                        {
                            println!("runnable {:?}", runnable);
                            println!(
                                "work_q size {:?} work inj size {:?} imm_inj size {:?} launched_tasks {:?} finished_tasks {:?} {:?}",
                                worker.work_q.len(),
                                worker.work_inj.len(),
                                worker.imm_inj.len(),
                                task_launched_to_string(),
                                task_finished_to_string(),
                                io_task_stats(),
                                // num_tasks.load(Ordering::SeqCst)
                            );
                            timer = std::time::Instant::now();
                        }
                        runnable.run();
                    }
                    if worker.status.load(Ordering::SeqCst) == SchedulerStatus::Finished as u8
                        && timer.elapsed().as_secs_f64() > config().deadlock_warning_timeout
                        && (worker.work_q.len() > 0 || worker.work_inj.len() > 0)
                    {
                        println!(
                            "work_q size {:?} work inj size {:?} imm_inj size {:?} launched_tasks {:?} finished_tasks {:?}",
                            worker.work_q.len(),
                            worker.work_inj.len(),
                            worker.imm_inj.len(),
                            task_launched_to_string(),
                            task_finished_to_string(),
                            // num_tasks.load(Ordering::SeqCst)
                        );
                        timer = std::time::Instant::now();
                    }
                    std::thread::yield_now();
                }
                active_cnt.fetch_sub(1, Ordering::SeqCst);
                trace!("TestSchdulerWorker thread shutting down");
                // #[cfg(feature = "enable-prof")]
                // lamellar_prof::fini_prof!();
            })
            .unwrap()
    }
}

#[derive(Debug)]
pub(crate) struct WorkStealing {
    orig_num_threads: usize,
    max_num_threads: usize,
    threads: Vec<thread::JoinHandle<()>>,
    thread_injs: Vec<Arc<crossbeam::deque::Injector<Runnable<usize>>>>,
    imm_inj: Arc<crossbeam::deque::Injector<Runnable<usize>>>,
    work_inj: Arc<crossbeam::deque::Injector<Runnable<usize>>>,
    work_stealers: Vec<crossbeam::deque::Stealer<Runnable<usize>>>,
    work_flag: Arc<AtomicU8>,
    status: Arc<AtomicU8>,
    active_cnt: Arc<AtomicUsize>,
    panic: Arc<AtomicU8>,
}

impl LamellarExecutor for WorkStealing {
    fn spawn_task<F>(&self, task: F, executor: Arc<Executor>) -> LamellarTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        TASKS_LAUNCHED
            .get(&TaskType::Spawn)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
        let task_id = TASK_ID.fetch_add(1, Ordering::Relaxed);
        trace!(target: "collective", "executor spawn task id: {:?}", task_id);
        // trace_span!("spawn_task").in_scope(|| {
        let work_inj = self.work_inj.clone();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = Builder::new().metadata(task_id).spawn(
            move |_task_id| {
                async move {
                    trace!(target: "collective", "starting spawn task id: {:?} ", task_id);
                    let res = task.await;
                    TASKS_FINISHED
                        .get(&TaskType::Spawn)
                        .unwrap()
                        .fetch_add(1, Ordering::Relaxed);
                        trace!(target: "collective", "finished spawn task id: {:?} ", task_id);
                    res
                }
                .instrument(trace_span!("Spawned Task", task_id = task_id))
            },
            schedule,
        );

        runnable.schedule();
        LamellarTask {
            task: LamellarTaskInner::LamellarTask(Some(task)),
            executor,
            task_id,
        }
        // })
    }

    fn submit_long_task<F>(&self, task: F)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        TASKS_LAUNCHED
            .get(&TaskType::LongSubmit)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
        let task_id = TASK_ID.fetch_add(1, Ordering::Relaxed);
        // trace!("submit long task id: {:?}", task_id);
        // trace_span!("submit_task").in_scope(|| {
        let work_inj = self.work_inj.clone();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = Builder::new().metadata(task_id).spawn(
            move |_task_id| {
                async move {
                    // trace!("starting long submit task id: {:?} ", task_id);
                    let res = task.await;
                    TASKS_FINISHED
                        .get(&TaskType::LongSubmit)
                        .unwrap()
                        .fetch_add(1, Ordering::Relaxed);
                        // trace!("finished long submit task id: {:?} ", task_id);
                    res
                }
                .instrument(trace_span!("Submitted Task", task_id = task_id))
            },
            schedule,
        );

        runnable.schedule();
        task.detach();
        // });
    }
    fn submit_task<F>(&self, task: F)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        TASKS_LAUNCHED
            .get(&TaskType::Submit)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
        let task_id = TASK_ID.fetch_add(1, Ordering::Relaxed);
        trace!(target: "collective", " submit task id: {:?}", task_id);
        // trace_span!("submit_task").in_scope(|| {
        let work_inj = self.work_inj.clone();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = Builder::new().metadata(task_id).spawn(
            move |_task_id| {
                async move {
                    trace!(target: "collective", "starting submit task id: {:?} ", task_id);
                    let res = task.await;
                    TASKS_FINISHED
                        .get(&TaskType::Submit)
                        .unwrap()
                        .fetch_add(1, Ordering::Relaxed);
                        trace!(target: "collective", "finished submit task id: {:?} ", task_id);
                    res
                }
                .instrument(trace_span!("Submitted Task", task_id = task_id))
            },
            schedule,
        );

        runnable.schedule();
        task.detach();
        // });
    }

    fn submit_task_thread<F>(&self, task: F, tid: usize)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        TASKS_LAUNCHED
            .get(&TaskType::Submit)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
        let task_id = TASK_ID.fetch_add(1, Ordering::Relaxed);
        trace!(target: "collective", "submit task thread id: {:?} task id: {:?}", tid, task_id);
        // trace_span!("submit_task_thread").in_scope(|| {
        let work_inj = self.thread_injs[tid].clone();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = Builder::new().metadata(task_id).spawn(
            move |_task_id| {
                async move {
                    trace!(target: "collective", "starting thread submit task id: {:?} ", task_id);
                    let res = task.await;
                    TASKS_FINISHED
                        .get(&TaskType::Submit)
                        .unwrap()
                        .fetch_add(1, Ordering::Relaxed);
                        trace!(target: "collective", "finished thread submit task id: {:?} ", task_id);
                    res
                }
                .instrument(trace_span!("Submitted Task", task_id = task_id))
            },
            schedule,
        );

        runnable.schedule();
        task.detach();
        // });
    }

    fn submit_io_task<F>(&self, task: F)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        TASKS_LAUNCHED
            .get(&TaskType::IO)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
        let task_id = TASK_ID.fetch_add(1, Ordering::Relaxed);
        trace!("submitting IO task id: {:?}", task_id);
        // trace!("submit IO task id: {:?}", task_id);
        // trace_span!("submit_io_task:").in_scope(|| {
        let work_inj = self.work_inj.clone();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = Builder::new().metadata(task_id).spawn(
            move |_task_id| {
                async move {
                    // trace!("starting IO task id: {:?} ", task_id);
                    let res = task.await;
                    TASKS_FINISHED
                        .get(&TaskType::IO)
                        .unwrap()
                        .fetch_add(1, Ordering::Relaxed);
                        // trace!("finished IO task id: {:?} ", task_id);
                    res
                }
                .instrument(trace_span!("IO Task", task_id = task_id))
            },
            schedule,
        );

        runnable.schedule();
        task.detach();
        // });
    }

    fn submit_immediate_task<F>(&self, task: F)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        TASKS_LAUNCHED
            .get(&TaskType::Immediate)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
        let task_id = TASK_ID.fetch_add(1, Ordering::Relaxed);
        // trace!("submit immediate task id: {:?}", task_id);
        // trace_span!("submit_immediate_task").in_scope(|| {
        let imm_inj = self.imm_inj.clone();
        let schedule = move |runnable| imm_inj.push(runnable);
        let (runnable, task) = Builder::new().metadata(task_id).spawn(
            move |_task_id| {
                async move {
                    // trace!("starting immediate task id: {:?} ", task_id);
                    let res = task.await;
                    TASKS_FINISHED
                        .get(&TaskType::Immediate)
                        .unwrap()
                        .fetch_add(1, Ordering::Relaxed);
                        // trace!("finished immediate task id: {:?} ", task_id);
                    res
                }
                .instrument(trace_span!("Immediate Task", task_id = task_id))
            },
            schedule,
        );

        runnable.schedule();
        // runnable.run(); //try to run immediately
        task.detach();
        // });
    }

    fn block_on<F: Future>(&self, fut: F) -> F::Output {
        TASKS_LAUNCHED
            .get(&TaskType::BlockOn)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
        let task_id = TASK_ID.fetch_add(1, Ordering::Relaxed);
        trace!(target: "collective", "executor block_on task id: {:?}", task_id);
        // trace!("block on task id: {:?}", task_id);
        // trace_span!("block_on").in_scope(|| {
        let work_inj = self.work_inj.clone();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, mut task) = unsafe {
            Builder::new().metadata(task_id).spawn_unchecked(
                move |_task_id| {
                    async move {
                        trace!(target: "collective","starting block on task id: {:?} ", task_id);
                        let res = fut.await;
                        TASKS_FINISHED
                            .get(&TaskType::BlockOn)
                            .unwrap()
                            .fetch_add(1, Ordering::Relaxed);
                        trace!(target: "collective", "finished block on task id: {:?} ", task_id);
                        res
                    }
                    .instrument(trace_span!("Block OnTask", task_id = task_id))
                },
                schedule,
            )
        };
        let waker = runnable.waker();
        runnable.run(); //try to run immediately
        while !task.is_finished() {
            self.exec_task(); //try to execute another task while this one is not ready
        }
        let cx = &mut Context::from_waker(&waker);
        if let Poll::Ready(output) = Pin::new(&mut task).poll(cx) {
            trace!(target: "collective", "executor block_on task id: {:?} finished", task_id);
            output
        } else {
            println!(
                "[{:?}] work stealing block on failed --  task id{:?}",
                std::thread::current().id(),
                task.metadata()
            );
            panic!("task not ready");
        }
        // })
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn shutdown(&self) {
        trace!(target: "drop", "entering shutdown");
        while self.panic.load(Ordering::SeqCst) == 0 && self.active_cnt.load(Ordering::Relaxed) > 0
        {
            //num active threads
            self.exec_task();
            std::thread::yield_now()
        }
        trace!(target: "drop", "leaving shutdown");
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn force_shutdown(&self) {
        trace!(target: "drop", "entering force_shutdown");

        // trace!("work stealing shutting down {:?}",self.status());
        let my_id = std::thread::current().id();
        if self.threads.iter().any(|e| e.thread().id() == my_id) {
            self.active_cnt.fetch_sub(1, Ordering::SeqCst); // I paniced so I wont actually decrement
        } else {
            while self.active_cnt.load(Ordering::Relaxed) > 0 {
                //num active threads
                self.exec_task();
                std::thread::yield_now()
            }
        }
        // trace!(
        //    "work stealing shut down {:?} {:?} {:?}",
        //     self.status(),
        //     self.active_cnt.load(Ordering::Relaxed),
        //     self.active_cnt.load(Ordering::Relaxed)
        // );
        trace!(target: "drop", "leaving force_shutdown");
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn exec_task(&self) {
        let mut rng = rand::rng();
        let t = rand::distr::Uniform::try_from(0..self.work_stealers.len())
            .expect("error getting uniform distribution");
        let ret = if !self.imm_inj.is_empty() {
            self.imm_inj.steal().success()
        } else {
            match self.thread_injs[0].steal().success() {
                Some(runnable) => Some(runnable),
                None => {
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
                }
            }
        };
        if let Some(runnable) = ret {
            runnable.run();
        }
    }

    fn num_workers(&self) -> usize {
        self.orig_num_threads
    }

    // fn active(&self) -> bool {
    //     self.status.load(Ordering::SeqCst) == SchedulerStatus::Active as u8
    //         || self.active_cnt.load(Ordering::Relaxed) > 0
    // }
}

impl WorkStealing {
    pub(crate) fn new(
        num_workers: usize,
        status: Arc<AtomicU8>,
        panic: Arc<AtomicU8>,
        my_pe: usize,
    ) -> WorkStealing {
        let core_ids = match core_affinity::get_core_ids() {
            Some(core_ids) => core_ids,
            None => {
                vec![core_affinity::CoreId { id: 0 }]
            }
        };
        let tid = LAMELLAR_THREAD_ID.with(|tid| *tid);
        let id = core_ids[tid % core_ids.len()];
        core_affinity::set_for_current(id);
        trace!(
            "WorkStealing Main thread running {:?} core: {:?} tid: {:?}",
            std::thread::current().id(),
            id,
            tid
        );
        // trace!("new work stealing queue");
        let mut ws = WorkStealing {
            orig_num_threads: num_workers,
            max_num_threads: std::cmp::max(1, num_workers - 1), // the main thread does work during blocking_ons and wait_alls
            threads: Vec::new(),
            thread_injs: Vec::new(),
            imm_inj: Arc::new(crossbeam::deque::Injector::new()),
            work_inj: Arc::new(crossbeam::deque::Injector::new()),
            work_stealers: Vec::new(),
            work_flag: Arc::new(AtomicU8::new(0)),
            status,
            active_cnt: Arc::new(AtomicUsize::new(0)),
            panic,
        };
        ws.init(Arc::new(core_ids),my_pe);
        ws
    }
    // //#[tracing::instrument(skip_all)]
    fn init(&mut self, core_ids: Arc<Vec<CoreId>>,my_pe: usize,) {
        let mut work_workers: std::vec::Vec<crossbeam::deque::Worker<Runnable<usize>>> = vec![];
        for _i in 0..self.max_num_threads {
            let work_worker: crossbeam::deque::Worker<Runnable<usize>> =
                crossbeam::deque::Worker::new_fifo();
            self.work_stealers.push(work_worker.stealer());
            work_workers.push(work_worker);
            let thread_inj = Arc::new(crossbeam::deque::Injector::new());
            self.thread_injs.push(thread_inj);
        }
        self.thread_injs
            .push(Arc::new(crossbeam::deque::Injector::new()));

        let orig_hook = panic::take_hook();
        panic::set_hook(Box::new(move |panic_info| {
            // invoke the default handler and exit the process
            orig_hook(panic_info);
            process::exit(1);
        }));
        // let core_ids = match core_affinity::get_core_ids() {
        //     Some(core_ids) => core_ids,
        //     None => {
        //         vec![core_affinity::CoreId { id: 0 }]
        //     }
        // };
        // trace!("core_ids: {:?}",core_ids);
        for i in 0..self.max_num_threads {
            let work_worker = work_workers.pop().unwrap();
            let worker: WorkStealingThread = WorkStealingThread {
                thread_only_inj: self.thread_injs[i + 1].clone(),
                imm_inj: self.imm_inj.clone(),
                work_inj: self.work_inj.clone(),
                work_stealers: self.work_stealers.clone(),
                work_q: work_worker,
                work_flag: self.work_flag.clone(),
                status: self.status.clone(),
                panic: self.panic.clone(),
            };
            self.threads.push(WorkStealingThread::run(
                worker,
                self.active_cnt.clone(),
                // self.num_tasks.clone(),
                core_ids.clone(),
                my_pe,
            ));
        }
        while self.active_cnt.load(Ordering::SeqCst) != self.threads.len() {
            std::thread::yield_now();
        }
    }
}

impl Drop for WorkStealing {
    //when is this called with respect to world?
    //#[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop WorkStealing");
        debug!("dropping work stealing");
        while let Some(thread) = self.threads.pop() {
            if thread.thread().id() != std::thread::current().id() {
                let _res = thread.join();
            }
        }
        debug!("WorkStealing Scheduler Dropped");
        trace!(target: "drop", "end drop WorkStealing");
    }
}

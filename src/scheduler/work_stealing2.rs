use crate::env_var::config;
use crate::scheduler::{
    Executor, LamellarExecutor, LamellarTask, LamellarTaskInner, SchedulerStatus,
};
use crate::MAIN_THREAD;

//use tracing::*;

use async_task::{Builder, Runnable};
use core_affinity::CoreId;
use crossbeam::deque::{Injector, Stealer, Worker};
use futures_util::Future;
use rand::distributions::Uniform;
use rand::prelude::*;
use std::collections::HashMap;
use std::panic;
use std::pin::Pin;
use std::process;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
//, Weak};
use std::thread::{self, ThreadId};

static TASK_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
struct TaskQueue {
    injector: Arc<Injector<Runnable<usize>>>,
    stealers: Vec<Stealer<Runnable<usize>>>,
    tasks: Worker<Runnable<usize>>,
    work_flag: Arc<AtomicU8>,
}

impl TaskQueue {
    fn get_task(&self, t: &Uniform<usize>, rng: &mut ThreadRng) -> Option<Runnable<usize>> {
        self.tasks.pop().or_else(|| {
            if self
                .work_flag
                .compare_exchange(0, 1, Ordering::SeqCst, Ordering::Relaxed)
                == Ok(0)
            {
                let ret = self.injector.steal_batch_and_pop(&self.tasks).success();
                self.work_flag.store(0, Ordering::SeqCst);
                ret
            } else {
                self.stealers[t.sample(rng)].steal().success()
            }
        })
    }

    fn is_empty(&self) -> bool {
        self.tasks.is_empty() && self.injector.is_empty()
    }
}

#[derive(Debug)]
pub(crate) struct WorkStealingThread {
    imm_inj: Arc<Injector<Runnable<usize>>>,
    group_queue: TaskQueue,
    global_injs: Vec<Arc<Injector<Runnable<usize>>>>,
    status: Arc<AtomicU8>,
    panic: Arc<AtomicU8>,
}

impl WorkStealingThread {
    //#[tracing::instrument(skip_all)]
    fn run(
        worker: WorkStealingThread,
        active_cnt: Arc<AtomicUsize>,
        // num_tasks: Arc<AtomicUsize>,
        id: CoreId,
    ) -> thread::JoinHandle<()> {
        let builder = thread::Builder::new().name("worker_thread".into());
        builder
            .spawn(move || {
                // println!("TestSchdulerWorker thread running {:?} core: {:?}", std::thread::current().id(), id);
                // let _span = trace_span!("WorkStealingThread::run");
                // println!(
                //     "Woker Thread {:?} core: {:?}, global_injs: {:?}, group_queue.stealers: {:?}",
                //     std::thread::current().id(),
                //     id,
                //     worker.global_injs.len(),
                //     worker.group_queue.stealers.len()
                // );
                core_affinity::set_for_current(id);
                active_cnt.fetch_add(1, Ordering::SeqCst);
                let mut rng = rand::thread_rng();
                let global_inj_dist = Uniform::new(0, worker.global_injs.len());
                let group_dist = Uniform::new(0, worker.group_queue.stealers.len());
                let mut timer = std::time::Instant::now();
                while worker.panic.load(Ordering::SeqCst) == 0
                    && (worker.status.load(Ordering::SeqCst) == SchedulerStatus::Active as u8
                        || !(worker.group_queue.is_empty()
                            && worker.imm_inj.is_empty()
                            && worker.global_injs.iter().all(|i| i.is_empty())))
                {
                    let omsg = if !worker.imm_inj.is_empty() {
                        worker.imm_inj.steal().success()
                    } else {
                        worker
                            .group_queue
                            .get_task(&group_dist, &mut rng)
                            .or_else(|| {
                                let i = global_inj_dist.sample(&mut rng);
                                worker.global_injs[i]
                                    .steal_batch_and_pop(&(worker.group_queue.tasks))
                                    .success()
                            })
                    };

                    if let Some(runnable) = omsg {
                        if worker.status.load(Ordering::SeqCst) == SchedulerStatus::Finished as u8
                            && timer.elapsed().as_secs_f64() > config().deadlock_timeout
                        {
                            println!("runnable {:?}", runnable);
                            println!(
                                "work_q size {:?} work inj size {:?}", // num_tasks {:?}",
                                worker.group_queue.tasks.len(),
                                worker.group_queue.injector.len(),
                                // num_tasks.load(Ordering::SeqCst)
                            );
                            timer = std::time::Instant::now();
                        }
                        runnable.run();
                    }
                    if worker.status.load(Ordering::SeqCst) == SchedulerStatus::Finished as u8
                        && timer.elapsed().as_secs_f64() > config().deadlock_timeout
                        && !worker.group_queue.is_empty()
                    {
                        println!(
                            "work_q size {:?} work inj size {:?} ", // num_tasks {:?}",
                            worker.group_queue.tasks.len(),
                            worker.group_queue.injector.len(),
                            // num_tasks.load(Ordering::SeqCst)
                        );
                        timer = std::time::Instant::now();
                    }
                    std::thread::yield_now();
                }
                active_cnt.fetch_sub(1, Ordering::SeqCst);
                // println!("TestSchdulerWorker thread shutting down");
            })
            .unwrap()
    }
}

// #[derive(Debug)]
// pub(crate) struct IoThread {
//     io_inj: Arc<crossbeam::deque::Injector<Runnable<usize>>>,
//     io_q: Worker<Runnable<usize>>,
//     status: Arc<AtomicU8>,
//     panic: Arc<AtomicU8>,
// }

// impl IoThread {
//     //#[tracing::instrument(skip_all)]
//     fn run(worker: IoThread, active_cnt: Arc<AtomicUsize>, id: CoreId) -> thread::JoinHandle<()> {
//         let builder = thread::Builder::new().name("io_thread".into());
//         builder
//             .spawn(move || {
//                 core_affinity::set_for_current(id);
//                 active_cnt.fetch_add(1, Ordering::SeqCst);
//                 let mut timer = std::time::Instant::now();
//                 while worker.panic.load(Ordering::SeqCst) == 0
//                     && (worker.status.load(Ordering::SeqCst) == SchedulerStatus::Active as u8
//                         || !(worker.io_q.is_empty() && worker.io_inj.is_empty()))
//                 {
//                     let io_task = worker
//                         .io_q
//                         .pop()
//                         .or_else(|| worker.io_inj.steal_batch_and_pop(&worker.io_q).success());
//                     if let Some(runnable) = io_task {
//                         if worker.status.load(Ordering::SeqCst) == SchedulerStatus::Finished as u8
//                             && timer.elapsed().as_secs_f64() > config().deadlock_timeout
//                         {
//                             println!(
//                                 "io_q size {:?} io inj size {:?} ", // num_tasks {:?}",
//                                 worker.io_q.len(),
//                                 worker.io_inj.len(),
//                                 // num_tasks.load(Ordering::SeqCst)
//                             );
//                             timer = std::time::Instant::now();
//                         }
//                         runnable.run();
//                     }

//                     if worker.status.load(Ordering::SeqCst) == SchedulerStatus::Finished as u8
//                         && timer.elapsed().as_secs_f64() > config().deadlock_timeout
//                         && (worker.io_q.len() > 0 || worker.io_inj.len() > 0)
//                     {
//                         println!(
//                             "io_q size {:?} io inj size {:?} ", // num_tasks {:?}",
//                             worker.io_q.len(),
//                             worker.io_inj.len(),
//                             // num_tasks.load(Ordering::SeqCst)
//                         );
//                         timer = std::time::Instant::now();
//                     }
//                     std::thread::yield_now();
//                 }
//                 active_cnt.fetch_sub(1, Ordering::SeqCst);
//             })
//             .unwrap()
//     }
// }

#[derive(Debug)]
pub(crate) struct WorkStealing2 {
    max_num_threads: usize,
    threads: Vec<thread::JoinHandle<()>>,
    imm_inj: Arc<Injector<Runnable<usize>>>,
    // io_inj: Arc<Injector<Runnable<usize>>>,
    work_injs: Vec<Arc<Injector<Runnable<usize>>>>,
    work_stealers: Vec<Stealer<Runnable<usize>>>,
    work_flag: Arc<AtomicU8>,
    status: Arc<AtomicU8>,
    active_cnt: Arc<AtomicUsize>,
    panic: Arc<AtomicU8>,
    num_threads_per_group: usize,
    cur_inj: Arc<AtomicU8>,
    inj_map: HashMap<ThreadId, usize>,
}

impl LamellarExecutor for WorkStealing2 {
    fn spawn_task<F>(&self, task: F, executor: Arc<Executor>) -> LamellarTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let work_inj = self.get_injector();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = Builder::new()
            .metadata(TASK_ID.fetch_add(1, Ordering::Relaxed))
            .spawn(move |_task_id| async move { task.await }, schedule);

        runnable.schedule();
        LamellarTask {
            task: LamellarTaskInner::LamellarTask(Some(task)),
            executor,
        }
    }
    fn submit_task<F>(&self, task: F)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        // trace_span!("submit_task").in_scope(|| {
        let work_inj = self.get_injector();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = Builder::new()
            .metadata(TASK_ID.fetch_add(1, Ordering::Relaxed))
            .spawn(move |_task_id| async move { task.await }, schedule);

        runnable.schedule();
        task.detach();
        // });
    }

    fn submit_io_task<F>(&self, task: F)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        // trace_span!("submit_task").in_scope(|| {
        let io_inj = self.get_injector();
        let schedule = move |runnable| io_inj.push(runnable);
        let (runnable, task) = Builder::new()
            .metadata(TASK_ID.fetch_add(1, Ordering::Relaxed))
            .spawn(move |_task_id| async move { task.await }, schedule);

        runnable.schedule();
        task.detach();
        // });
    }

    fn submit_immediate_task<F>(&self, task: F)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        // trace_span!("submit_task").in_scope(|| {
        let imm_inj = self.imm_inj.clone();
        let schedule = move |runnable| imm_inj.push(runnable);
        let (runnable, task) = Builder::new()
            .metadata(TASK_ID.fetch_add(1, Ordering::Relaxed))
            .spawn(move |_task_id| async move { task.await }, schedule);

        runnable.run(); //try to run immediately
        task.detach();
        // });
    }

    fn block_on<F: Future>(&self, fut: F) -> F::Output {
        // trace_span!("block_on").in_scope(|| {
        let work_inj = self.get_injector();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, mut task) = unsafe {
            Builder::new()
                .metadata(TASK_ID.fetch_add(1, Ordering::Relaxed))
                .spawn_unchecked(move |_task_id| async move { fut.await }, schedule)
        };
        let waker = runnable.waker();
        runnable.run(); //try to run immediately
        while !task.is_finished() {
            self.exec_task(); //try to execute another task while this one is not ready
        }
        let cx = &mut Context::from_waker(&waker);
        if let Poll::Ready(output) = Pin::new(&mut task).poll(cx) {
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

    //#[tracing::instrument(skip_all)]
    fn shutdown(&self) {
        while self.panic.load(Ordering::SeqCst) == 0 && self.active_cnt.load(Ordering::Relaxed) > 0
        {
            //num active threads
            self.exec_task();
            std::thread::yield_now()
        }
    }

    //#[tracing::instrument(skip_all)]
    fn force_shutdown(&self) {
        // println!("work stealing shuting down {:?}", self.status());

        // println!("work stealing shuting down {:?}",self.status());
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
        // println!(
        //     "work stealing shut down {:?} {:?} {:?}",
        //     self.status(),
        //     self.active_cnt.load(Ordering::Relaxed),
        //     self.active_cnt.load(Ordering::Relaxed)
        // );
    }

    //#[tracing::instrument(skip_all)]
    fn exec_task(&self) {
        let mut rng = rand::thread_rng();
        let t = rand::distributions::Uniform::new(0, self.work_stealers.len());
        let ret = if !self.imm_inj.is_empty() {
            self.imm_inj.steal().success()
        } else {
            self.get_injector()
                .steal()
                .success()
                .or_else(|| self.work_stealers[t.sample(&mut rng)].steal().success())
        };
        if let Some(runnable) = ret {
            runnable.run();
        }
    }

    // fn set_max_workers(&mut self, num_workers: usize) {
    //     self.max_num_threads = num_workers;
    // }

    fn num_workers(&self) -> usize {
        self.max_num_threads
    }
}

impl WorkStealing2 {
    pub(crate) fn new(
        num_workers: usize,
        status: Arc<AtomicU8>,
        panic: Arc<AtomicU8>,
    ) -> WorkStealing2 {
        // println!("new work stealing queue");
        let num_workers = std::cmp::max(1, num_workers - 1);
        let mut num_threads_per_group = match std::env::var("LAMELLAR_WS2_THREADS") {
            Ok(s) => {
                if let Ok(num) = s.parse::<usize>() {
                    num
                } else {
                    4
                }
            }
            _ => 4,
        };
        if num_threads_per_group > num_workers {
            num_threads_per_group = num_workers
        }

        let mut ws = WorkStealing2 {
            max_num_threads: num_workers,
            threads: Vec::new(),
            imm_inj: Arc::new(Injector::new()),
            // io_inj: Arc::new(Injector::new()),
            work_injs: Vec::new(),
            work_stealers: Vec::new(),
            work_flag: Arc::new(AtomicU8::new(0)),
            status: status,
            active_cnt: Arc::new(AtomicUsize::new(0)),
            panic: panic,
            num_threads_per_group: num_threads_per_group,
            cur_inj: Arc::new(AtomicU8::new(0)),
            inj_map: HashMap::new(),
        };
        ws.init();
        ws
    }
    // #[tracing::instrument(skip_all)]
    fn init(&mut self) {
        let mut num_groups = self.max_num_threads / self.num_threads_per_group;
        if self.max_num_threads % self.num_threads_per_group != 0 {
            num_groups += 1;
        }

        for _i in 0..num_groups {
            self.work_injs.push(Arc::new(Injector::new()));
        }

        let mut work_workers = vec![];
        for _i in 0..self.max_num_threads {
            let work_worker: Worker<Runnable<usize>> = Worker::new_fifo();
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

        // println!(
        //     "num threads: {} {} num_groups: {}",
        //     self.max_num_threads,
        //     core_ids.len(),
        //     num_groups
        // );

        let mut thread_cnt = 0;
        for (group_id, group_stealers) in self
            .work_stealers
            .chunks(self.num_threads_per_group)
            .enumerate()
        {
            // println!("init group {} {:?}", group_id, group_stealers.len());
            let work_flag = self.work_flag.clone();
            for _ in 0..group_stealers.len() {
                let group_queue = TaskQueue {
                    tasks: work_workers.pop().unwrap(),
                    injector: self.work_injs[group_id].clone(),
                    stealers: group_stealers.to_vec(),
                    work_flag: work_flag.clone(),
                };
                let mut work_injs = vec![];
                for (i, inj) in self.work_injs.iter().enumerate() {
                    if i != group_id || num_groups == 1 {
                        work_injs.push(inj.clone());
                    }
                }

                let worker = WorkStealingThread {
                    imm_inj: self.imm_inj.clone(),
                    group_queue: group_queue,
                    global_injs: work_injs,
                    status: self.status.clone(),
                    panic: self.panic.clone(),
                };
                let thread = WorkStealingThread::run(
                    worker,
                    self.active_cnt.clone(),
                    core_ids[thread_cnt % core_ids.len()],
                );
                thread_cnt += 1;
                self.inj_map.insert(thread.thread().id(), group_id);
                self.threads.push(thread);
            }
        }

        // let io_thread = IoThread {
        //     io_inj: self.io_inj.clone(),
        //     io_q: crossbeam::deque::Worker::new_fifo(),
        //     status: self.status.clone(),
        //     panic: self.panic.clone(),
        // };
        // self.threads.push(IoThread::run(
        //     io_thread,
        //     self.active_cnt.clone(),
        //     core_ids[self.max_num_threads % core_ids.len()],
        // ));
        while self.active_cnt.load(Ordering::SeqCst) != self.threads.len() {
            std::thread::yield_now();
        }
    }

    fn get_injector(&self) -> Arc<Injector<Runnable<usize>>> {
        let tid = thread::current().id();
        if tid == *MAIN_THREAD {
            self.work_injs
                [self.cur_inj.fetch_add(1, Ordering::Relaxed) as usize % self.work_injs.len()]
            .clone()
        } else {
            self.work_injs[*self
                .inj_map
                .get(&tid)
                .expect("Thread ID Should be registered")]
            .clone()
        }
    }
}

impl Drop for WorkStealing2 {
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

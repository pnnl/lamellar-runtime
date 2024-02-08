use crate::active_messaging::batching::simple_batcher::SimpleBatcher;
use crate::active_messaging::batching::team_am_batcher::TeamAmBatcher;
use crate::active_messaging::batching::BatcherType;
use crate::active_messaging::registered_active_message::RegisteredActiveMessages;
use crate::active_messaging::*;
use crate::lamellae::{Des, Lamellae, SerializedData};

use enum_dispatch::enum_dispatch;
use futures::Future;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

pub(crate) mod work_stealing;
use work_stealing::WorkStealing;

#[cfg(feature = "tokio-executor")]
pub(crate) mod tokio_executor;
#[cfg(feature = "tokio-executor")]
use tokio_executor::TokioRt;

// ACTIVE ENUM
// since atomic enums would be another dependecy

#[repr(u8)]
#[derive(Copy, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) enum SchedulerStatus {
    Active,
    Finished,
    Panic,
}

// pub(crate) mod numa_work_stealing;
// use numa_work_stealing::{NumaWorkStealing, NumaWorkStealingInner};

// pub(crate) mod numa_work_stealing2;
// use numa_work_stealing2::{NumaWorkStealing2, NumaWorkStealing2Inner};

#[derive(
    Copy,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    std::cmp::Eq,
    std::cmp::PartialEq,
    Hash,
    Default,
)]
pub(crate) struct ReqId {
    pub(crate) id: usize,
    pub(crate) sub_id: usize,
}

#[derive(Debug)]
pub enum ExecutorType {
    LamellarWorkStealing,
    #[cfg(feature = "tokio-executor")]
    Tokio,
    // Dyn(impl LamellarExecutor),
}

#[enum_dispatch]
pub(crate) trait LamellarExecutor {
    fn submit_task<F>(&self, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send;

    fn submit_immediate_task<F>(&self, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        Self::submit_task(self, future)
    }

    fn exec_task(&self) {
        std::thread::yield_now();
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output;

    fn set_max_workers(&mut self, num_workers: usize);
    fn num_workers(&self) -> usize;
    fn shutdown(&self);
    fn force_shutdown(&self);
}

#[enum_dispatch(LamellarExecutor)]
#[derive(Debug)]
pub(crate) enum Executor {
    WorkStealing(WorkStealing),
    #[cfg(feature = "tokio-executor")]
    Tokio(TokioRt),
}

#[derive(Debug)]
pub(crate) struct Scheduler {
    executor: Arc<Executor>,
    active_message_engine: RegisteredActiveMessages, //we can eventually abstract this around the ActiveMessageEngine trait but no need currently
    num_ams: Arc<AtomicUsize>,
    max_ams: Arc<AtomicUsize>,
    num_tasks: Arc<AtomicUsize>,
    max_tasks: Arc<AtomicUsize>,
    am_stall_mark: Arc<AtomicUsize>,
    status: Arc<AtomicU8>,
    panic: Arc<AtomicU8>,
}

impl Scheduler {
    pub(crate) fn new(
        executor: Executor,
        active_message_engine: RegisteredActiveMessages,
        am_stall_mark: Arc<AtomicUsize>,
        status: Arc<AtomicU8>,
        panic: Arc<AtomicU8>,
    ) -> Self {
        Self {
            executor: Arc::new(executor),
            active_message_engine,
            num_ams: Arc::new(AtomicUsize::new(0)),
            max_ams: Arc::new(AtomicUsize::new(0)),
            num_tasks: Arc::new(AtomicUsize::new(0)),
            max_tasks: Arc::new(AtomicUsize::new(0)),
            am_stall_mark,
            status,
            panic,
        }
    }
    pub(crate) fn submit_am(&self, am: Am) {
        let num_ams = self.num_ams.clone();
        let max_ams = self.max_ams.clone();
        let am_stall_mark = self.am_stall_mark.fetch_add(1, Ordering::Relaxed);
        let ame = self.active_message_engine.clone();
        let executor = self.executor.clone();
        let am_future = async move {
            num_ams.fetch_add(1, Ordering::Relaxed);
            max_ams.fetch_add(1, Ordering::Relaxed);
            // println!("[{:?}] submit work exec req {:?} {:?} TaskId: {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task);
            ame.process_msg(am, executor, am_stall_mark, false).await;
            num_ams.fetch_sub(1, Ordering::Relaxed);
            // println!("[{:?}] submit work done req {:?} {:?} TaskId: {:?} {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task,reqs);
        };
        self.executor.submit_task(am_future);
    }

    #[allow(dead_code)]
    pub(crate) fn submit_am_immediate(&self, am: Am) {
        let num_ams = self.num_ams.clone();
        let max_ams = self.max_ams.clone();
        let am_stall_mark = self.am_stall_mark.fetch_add(1, Ordering::Relaxed);
        let ame = self.active_message_engine.clone();
        let executor = self.executor.clone();
        let am_future = async move {
            num_ams.fetch_add(1, Ordering::Relaxed);
            max_ams.fetch_add(1, Ordering::Relaxed);
            // println!("[{:?}] submit work exec req {:?} {:?} TaskId: {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task);
            ame.process_msg(am, executor, am_stall_mark, false).await;
            num_ams.fetch_sub(1, Ordering::Relaxed);
            // println!("[{:?}] submit work done req {:?} {:?} TaskId: {:?} {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task,reqs);
        };
        self.executor.submit_immediate_task(am_future);
    }

    pub(crate) fn submit_remote_am(&self, data: SerializedData, lamellae: Arc<Lamellae>) {
        let num_ams = self.num_ams.clone();
        let max_ams = self.max_ams.clone();
        let ame = self.active_message_engine.clone();
        let executor = self.executor.clone();
        let am_future = async move {
            num_ams.fetch_add(1, Ordering::Relaxed);
            max_ams.fetch_add(1, Ordering::Relaxed);
            // println!("[{:?}] submit work exec req {:?} {:?} TaskId: {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task);
            if let Some(header) = data.deserialize_header() {
                let msg = header.msg;
                ame.exec_msg(msg, data, lamellae, executor).await;
            } else {
                data.print();
                panic!("should i be here?");
            }
            num_ams.fetch_sub(1, Ordering::Relaxed);
            // println!("[{:?}] submit work done req {:?} {:?} TaskId: {:?} {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task,reqs);
        };
        self.executor.submit_task(am_future);
    }

    pub(crate) fn submit_task<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let num_tasks = self.num_tasks.clone();
        let max_tasks = self.max_tasks.clone();
        let future = async move {
            num_tasks.fetch_add(1, Ordering::Relaxed);
            max_tasks.fetch_add(1, Ordering::Relaxed);
            task.await;
            num_tasks.fetch_sub(1, Ordering::Relaxed);
        };
        self.executor.submit_task(future);
    }

    pub(crate) fn submit_immediate_task<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let num_tasks = self.num_tasks.clone();
        let max_tasks = self.max_tasks.clone();
        let future = async move {
            num_tasks.fetch_add(1, Ordering::Relaxed);
            max_tasks.fetch_add(1, Ordering::Relaxed);
            task.await;
            num_tasks.fetch_sub(1, Ordering::Relaxed);
        };
        self.executor.submit_immediate_task(future);
    }

    pub(crate) fn exec_task(&self) {
        if std::thread::current().id() == *crate::MAIN_THREAD {
            self.executor.exec_task();
        } else {
            std::thread::yield_now();
        }
    }

    pub(crate) fn block_on<F: Future>(&self, task: F) -> F::Output {
        if std::thread::current().id() != *crate::MAIN_THREAD {
            println!(
                "trying to call block on within a worker thread {:?}",
                std::backtrace::Backtrace::capture()
            )
        }
        self.executor.block_on(task)
    }

    #[allow(dead_code)]
    pub(crate) fn get_executor(&self) -> Arc<Executor> {
        self.executor.clone()
    }

    pub(crate) fn active(&self) -> bool {
        self.status.load(Ordering::SeqCst) == SchedulerStatus::Active as u8
            || self.num_tasks.load(Ordering::SeqCst) > 3 // the Lamellae Comm Task, Lamellae Alloc Task, Lamellar Error Task
    }
    pub(crate) fn num_workers(&self) -> usize {
        self.executor.num_workers()
    }
    pub(crate) fn begin_shutdown(&self) {
        self.status
            .store(SchedulerStatus::Finished as u8, Ordering::SeqCst);
    }
    pub(crate) fn shutdown(&self) {
        let mut timer = std::time::Instant::now();
        while self.panic.load(Ordering::SeqCst) == 0 && self.num_tasks.load(Ordering::Relaxed) > 3
        //TODO maybe this should be > 2
        {
            //the Lamellae Comm Task, Lamellae Alloc Task, Lamellar Error Task
            if timer.elapsed().as_secs_f64() > *crate::DEADLOCK_TIMEOUT {
                println!(
                    "shurtdown timeout, tasks remaining: {:?} panic: {:?}",
                    self.num_tasks.load(Ordering::Relaxed),
                    self.panic.load(Ordering::SeqCst),
                );
                timer = std::time::Instant::now();
            }
            std::thread::yield_now()
        }
        self.executor.shutdown();
    }
    pub(crate) fn force_shutdown(&self) {
        self.status
            .store(SchedulerStatus::Panic as u8, Ordering::SeqCst);
        self.executor.force_shutdown();
    }
}

pub(crate) fn create_scheduler(
    executor: ExecutorType,
    num_pes: usize,
    num_workers: usize,
    panic: Arc<AtomicU8>,
) -> Scheduler {
    let am_stall_mark = Arc::new(AtomicUsize::new(0));
    let status = Arc::new(AtomicU8::new(SchedulerStatus::Active as u8));
    let executor = match executor {
        ExecutorType::LamellarWorkStealing => {
            WorkStealing::new(num_workers, status.clone(), panic.clone()).into()
        }
        #[cfg(feature = "tokio-executor")]
        ExecutorType::Tokio => TokioRt::new(num_workers).into(),
    };

    let batcher = match std::env::var("LAMELLAR_BATCHER") {
        Ok(n) => {
            let n = n.parse::<usize>().unwrap();
            if n == 1 {
                BatcherType::Simple(SimpleBatcher::new(num_pes, am_stall_mark.clone()))
            } else {
                BatcherType::TeamAm(TeamAmBatcher::new(num_pes, am_stall_mark.clone()))
            }
        }
        Err(_) => BatcherType::TeamAm(TeamAmBatcher::new(num_pes, am_stall_mark.clone())),
    };
    Scheduler::new(
        executor,
        RegisteredActiveMessages::new(batcher),
        am_stall_mark,
        status,
        panic,
    )
}

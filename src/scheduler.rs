use crate::active_messaging::batching::simple_batcher::SimpleBatcher;
use crate::active_messaging::batching::team_am_batcher::TeamAmBatcher;
use crate::active_messaging::batching::BatcherType;
use crate::active_messaging::registered_active_message::RegisteredActiveMessages;
use crate::active_messaging::*;
use crate::env_var::config;
use crate::lamellae::{Des, Lamellae, SerializedData};
use crate::warnings::RuntimeWarning;

use enum_dispatch::enum_dispatch;
use futures_util::Future;
use pin_project::pin_project;
use std::pin::{pin, Pin};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

pub(crate) mod work_stealing;
use work_stealing::WorkStealing;

pub(crate) mod work_stealing2;
use work_stealing2::WorkStealing2;

pub(crate) mod work_stealing3;
use work_stealing3::WorkStealing3;

pub(crate) mod async_std_executor;
use async_std_executor::AsyncStdRt;

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

// static AM_SAME_THREAD: AtomicUsize = AtomicUsize::new(0);
// static AM_DIFF_THREAD: AtomicUsize = AtomicUsize::new(0);

// static TASK_SAME_THREAD: AtomicUsize = AtomicUsize::new(0);
// static TASK_DIFF_THREAD: AtomicUsize = AtomicUsize::new(0);

// static IO_SAME_THREAD: AtomicUsize = AtomicUsize::new(0);
// static IO_DIFF_THREAD: AtomicUsize = AtomicUsize::new(0);

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

/// Indicates the executor backend
/// Default is a work stealing executor
/// If the "tokio-executor" feature is enabled,the tokio executor can also be used
/// allowing seemless integration with tokio based applications
#[derive(Debug)]
pub enum ExecutorType {
    /// The default work stealing executor
    LamellarWorkStealing,
    /// Experimental numa-aware(ish) work stealing executor
    LamellarWorkStealing2,
    /// Experimental numa-aware(ish) work stealing executor
    LamellarWorkStealing3,
    /// executor provided by the AsyncStd crate
    AsyncStd,
    #[cfg(feature = "tokio-executor")]
    /// The tokio executor
    Tokio,
    // Dyn(impl LamellarExecutor),
}

#[derive(Debug)]
#[pin_project]
pub struct LamellarTask<T> {
    #[pin]
    task: LamellarTaskInner<T>,
    executor: Arc<Executor>,
}

impl<T> LamellarTask<T> {
    pub fn block(self) -> T {
        RuntimeWarning::BlockingCall("LamellarTask::block", "<task>.await").print();
        self.executor.clone().block_on(self)
    }
}

impl<T> Future for LamellarTask<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().task.poll(cx)
    }
}

#[derive(Debug)]
pub(crate) enum LamellarTaskInner<T> {
    LamellarTask(Option<async_task::Task<T, usize>>),
    AsyncStdTask(async_std::task::JoinHandle<T>),
    #[cfg(feature = "tokio-executor")]
    TokioTask(tokio::task::JoinHandle<T>),
}

impl<T> Drop for LamellarTaskInner<T> {
    fn drop(self: &mut Self) {
        // let mut dropped = LamellarTaskInner::Dropped;

        // std::mem::swap(&mut dropped, self);
        match self {
            LamellarTaskInner::LamellarTask(task) => {
                task.take().expect("task already taken").detach();
            }
            LamellarTaskInner::AsyncStdTask(_task) => {}
            #[cfg(feature = "tokio-executor")]
            LamellarTaskInner::TokioTask(_task) => {}
        }
    }
}

impl<T> Future for LamellarTaskInner<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe {
            match self.get_unchecked_mut() {
                LamellarTaskInner::LamellarTask(task) => {
                    if let Some(task) = task {
                        Pin::new_unchecked(task).poll(cx)
                    } else {
                        unreachable!()
                    }
                }
                LamellarTaskInner::AsyncStdTask(task) => Pin::new_unchecked(task).poll(cx),
                #[cfg(feature = "tokio-executor")]
                LamellarTaskInner::TokioTask(task) => match Pin::new_unchecked(task).poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(res) => Poll::Ready(res.expect("tokio task failed")),
                },
            }
        }
    }
}

#[enum_dispatch]
pub(crate) trait LamellarExecutor {
    fn spawn_task<F>(&self, future: F, executor: Arc<Executor>) -> LamellarTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send;

    fn submit_task<F>(&self, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send;

    fn submit_io_task<F>(&self, future: F)
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

    // fn set_max_workers(&mut self, num_workers: usize);
    fn num_workers(&self) -> usize;
    fn shutdown(&self);
    fn force_shutdown(&self);
}

#[enum_dispatch(LamellarExecutor)]
#[derive(Debug)]
pub(crate) enum Executor {
    WorkStealing(WorkStealing),
    WorkStealing2(WorkStealing2),
    WorkStealing3(WorkStealing3),
    AsyncStd(AsyncStdRt),
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
        executor: Arc<Executor>,
        active_message_engine: RegisteredActiveMessages,
        am_stall_mark: Arc<AtomicUsize>,
        status: Arc<AtomicU8>,
        panic: Arc<AtomicU8>,
    ) -> Self {
        Self {
            executor: executor,
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
        num_ams.fetch_add(1, Ordering::Relaxed);
        let _am_id = max_ams.fetch_add(1, Ordering::Relaxed);
        // println!("am ptr {:p} ", &am);
        let am_future = async move {
            // let start_tid = thread::current().id();

            // println!("[{:?}] submit work exec req {:?} {:?} TaskId: {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task);
            // println!("[{:?}] submit_am {:?}", std::thread::current().id(), am_id);
            ame.process_msg(am, am_stall_mark, false).await;
            num_ams.fetch_sub(1, Ordering::Relaxed);
            // if thread::current().id() != start_tid {
            //     AM_DIFF_THREAD.fetch_add(1, Ordering::Relaxed);
            // } else {
            //     AM_SAME_THREAD.fetch_add(1, Ordering::Relaxed);
            // }
            // println!(
            //     "[{:?}] submit_am_done {:?}",
            //     std::thread::current().id(),
            //     am_id
            // );
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
        num_ams.fetch_add(1, Ordering::Relaxed);
        let _am_id = max_ams.fetch_add(1, Ordering::Relaxed);
        let am_future = async move {
            // let start_tid = thread::current().id();

            // println!("[{:?}] submit work exec req {:?} {:?} TaskId: {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task);
            // println!(
            //     "[{:?}] submit_am_immediate {:?}",
            //     std::thread::current().id(),
            //     am_id
            // );
            ame.process_msg(am, am_stall_mark, false).await;
            num_ams.fetch_sub(1, Ordering::Relaxed);
            // if thread::current().id() != start_tid {
            //     AM_DIFF_THREAD.fetch_add(1, Ordering::Relaxed);
            // } else {
            //     AM_SAME_THREAD.fetch_add(1, Ordering::Relaxed);
            // }
            // println!(
            //     "[{:?}] submit_am_immediate done {:?}",
            //     std::thread::current().id(),
            //     am_id
            // );
            // println!("[{:?}] submit work done req {:?} {:?} TaskId: {:?} {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task,reqs);
        };
        self.executor.submit_immediate_task(am_future);
    }

    #[allow(dead_code)]
    pub(crate) async fn exec_am(&self, am: Am) {
        let num_ams = self.num_ams.clone();
        let max_ams = self.max_ams.clone();
        let am_stall_mark = self.am_stall_mark.fetch_add(1, Ordering::Relaxed);
        let ame = self.active_message_engine.clone();
        // let am_future = async move {
        // let start_tid = thread::current().id();
        num_ams.fetch_add(1, Ordering::Relaxed);
        let _am_id = max_ams.fetch_add(1, Ordering::Relaxed);
        // println!("[{:?}] submit work exec req {:?} {:?} TaskId: {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task);
        // println!(
        //     "[{:?}] submit_am_immediate {:?}",
        //     std::thread::current().id(),
        //     am_id
        // );
        ame.process_msg(am, am_stall_mark, false).await;
        num_ams.fetch_sub(1, Ordering::Relaxed);
        // if thread::current().id() != start_tid {
        //     AM_DIFF_THREAD.fetch_add(1, Ordering::Relaxed);
        // } else {
        //     AM_SAME_THREAD.fetch_add(1, Ordering::Relaxed);
        // }
        // println!(
        //     "[{:?}] submit_am_immediate done {:?}",
        //     std::thread::current().id(),
        //     am_id
        // );
        // println!("[{:?}] submit work done req {:?} {:?} TaskId: {:?} {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task,reqs);
        // };
        // self.executor.submit_immediate_task(am_future);
    }

    pub(crate) fn submit_remote_am(&self, data: SerializedData, lamellae: Arc<Lamellae>) {
        let num_ams = self.num_ams.clone();
        let max_ams = self.max_ams.clone();
        let ame = self.active_message_engine.clone();
        num_ams.fetch_add(1, Ordering::Relaxed);
        let _am_id = max_ams.fetch_add(1, Ordering::Relaxed);
        let am_future = async move {
            // let start_tid = std::thread::current().id();

            // println!("[{:?}] submit work exec req {:?} {:?} TaskId: {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task);
            // println!(
            //     "[{:?}] submit_remote_am {:?}",
            //     std::thread::current().id(),
            //     am_id
            // );
            if let Some(header) = data.deserialize_header() {
                let msg = header.msg;
                ame.exec_msg(msg, data, lamellae).await;
            } else {
                data.print();
                panic!("should i be here?");
            }
            num_ams.fetch_sub(1, Ordering::Relaxed);
            // if start_tid == std::thread::current().id() {
            //     AM_SAME_THREAD.fetch_add(1, Ordering::Relaxed);
            // } else {
            //     AM_DIFF_THREAD.fetch_add(1, Ordering::Relaxed);
            // }
            // println!(
            //     "[{:?}] submit_remote_am done {:?}",
            //     std::thread::current().id(),
            //     am_id
            // );
            // println!("[{:?}] submit work done req {:?} {:?} TaskId: {:?} {:?}", std::thread::current().id(),num_tasks.load(Ordering::Relaxed),max_tasks.load(Ordering::Relaxed),cur_task,reqs);
        };
        self.executor.submit_task(am_future);
    }

    pub(crate) fn spawn_task<F>(&self, task: F) -> LamellarTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let num_tasks = self.num_tasks.clone();
        let max_tasks = self.max_tasks.clone();
        num_tasks.fetch_add(1, Ordering::Relaxed);
        let _task_id = max_tasks.fetch_add(1, Ordering::Relaxed);
        let future = async move {
            let result = task.await;
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            result
        };
        self.executor.spawn_task(future, self.executor.clone())
    }

    pub(crate) fn submit_task<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let num_tasks = self.num_tasks.clone();
        let max_tasks = self.max_tasks.clone();
        num_tasks.fetch_add(1, Ordering::Relaxed);
        let _task_id = max_tasks.fetch_add(1, Ordering::Relaxed);
        let future = async move {
            // let start_tid = std::thread::current().id();

            // println!(
            //     "[{:?}] execing new task {:?}",
            //     std::thread::current().id(),
            //     task_id
            // );
            task.await;
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            // println!(
            //     "[{:?}] done new task {:?} ",
            //     std::thread::current().id(),
            //     task_id
            // );
            // if start_tid == std::thread::current().id() {
            //     TASK_SAME_THREAD.fetch_add(1, Ordering::Relaxed);
            // } else {
            //     TASK_DIFF_THREAD.fetch_add(1, Ordering::Relaxed);
            // }
        };
        self.executor.submit_task(future);
    }

    pub(crate) fn submit_immediate_task<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let num_tasks = self.num_tasks.clone();
        let max_tasks = self.max_tasks.clone();
        num_tasks.fetch_add(1, Ordering::Relaxed);
        let _task_id = max_tasks.fetch_add(1, Ordering::Relaxed);
        let future = async move {
            // let start_tid = std::thread::current().id();

            // println!(
            //     "[{:?}] execing new task immediate {:?}",
            //     std::thread::current().id(),
            //     _task_id
            // );
            task.await;
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            // println!(
            //     "[{:?}] done new task immediate {:?} ",
            //     std::thread::current().id(),
            //     task_id
            // );
            // if start_tid == std::thread::current().id() {
            //     TASK_SAME_THREAD.fetch_add(1, Ordering::Relaxed);
            // } else {
            //     TASK_DIFF_THREAD.fetch_add(1, Ordering::Relaxed);
            // }
        };
        self.executor.submit_immediate_task(future);
    }

    pub(crate) fn submit_io_task<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let num_tasks = self.num_tasks.clone();
        let max_tasks = self.max_tasks.clone();
        num_tasks.fetch_add(1, Ordering::Relaxed);
        let _task_id = max_tasks.fetch_add(1, Ordering::Relaxed);
        let future = async move {
            // let start_tid = std::thread::current().id();

            // println!(
            //     "[{:?}] execing new task {:?}",
            //     std::thread::current().id(),
            //     task_id
            // );
            task.await;
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            // println!(
            //     "[{:?}] done new task {:?} ",
            //     std::thread::current().id(),
            //     task_id
            // );
            // if start_tid == std::thread::current().id() {
            //     IO_SAME_THREAD.fetch_add(1, Ordering::Relaxed);
            // } else {
            //     IO_DIFF_THREAD.fetch_add(1, Ordering::Relaxed);
            // }
        };

        self.executor.submit_io_task(future);
    }

    pub(crate) fn exec_task(&self) {
        // if std::thread::current().id() == *crate::MAIN_THREAD {
        self.executor.exec_task();
        // } else {
        //     std::thread::yield_now();
        // }
    }

    pub(crate) fn block_on<F: Future>(&self, task: F) -> F::Output {
        RuntimeWarning::BlockOn.print();
        self.executor.block_on(task)
    }

    #[allow(dead_code)]
    pub(crate) fn get_executor(&self) -> Arc<Executor> {
        self.executor.clone()
    }

    pub(crate) fn print_status(&self) {
        println!(
            "status: {:?} num tasks: {:?} max tasks: {:?} num ams  {:?} max ams {:?}",
            self.status.load(Ordering::SeqCst),
            self.num_tasks.load(Ordering::SeqCst),
            self.max_tasks.load(Ordering::SeqCst),
            self.num_ams.load(Ordering::SeqCst),
            self.max_ams.load(Ordering::SeqCst)
        );
    }

    pub(crate) fn active(&self) -> bool {
        // if self.status.load(Ordering::SeqCst) == SchedulerStatus::Finished as u8 {
        //     println!(
        //         "active: {:?} {:?}",
        //         self.status.load(Ordering::SeqCst),
        //         self.num_tasks.load(Ordering::SeqCst)
        //     );
        // }

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
        // println!(
        //     "shutting down executor panic {:?} num_tasks {:?} max_tasks {:?} num_ams {:?} max_ams {:?}",
        //     self.panic.load(Ordering::SeqCst),
        //     self.num_tasks.load(Ordering::Relaxed),
        //     self.max_tasks.load(Ordering::Relaxed),
        //     self.num_ams.load(Ordering::Relaxed),
        //     self.max_ams.load(Ordering::Relaxed),
        // );
        while self.panic.load(Ordering::SeqCst) == 0
            && self.num_tasks.load(Ordering::Relaxed) > 3
            && self.num_ams.load(Ordering::Relaxed) > 0
        //TODO maybe this should be > 2
        {
            //the Lamellae Comm Task, Lamellae Alloc Task, Lamellar Error Task
            if timer.elapsed().as_secs_f64() > config().deadlock_timeout {
                println!(
                    "shutdown timeout, tasks remaining: {:?} panic: {:?}",
                    self.num_tasks.load(Ordering::Relaxed),
                    self.panic.load(Ordering::SeqCst),
                );
                timer = std::time::Instant::now();
            }
            std::thread::yield_now()
        }
        self.executor.shutdown();
        // println!(
        //     "AM_SAME: {:?} AM_DIFF: {:?}",
        //     AM_SAME_THREAD.load(Ordering::Relaxed),
        //     AM_DIFF_THREAD.load(Ordering::Relaxed)
        // );
        // println!(
        //     "TASK_SAME: {:?} TASK_DIFF: {:?}",
        //     TASK_SAME_THREAD.load(Ordering::Relaxed),
        //     TASK_DIFF_THREAD.load(Ordering::Relaxed)
        // );
        // println!(
        //     "IO_SAME: {:?} IO_DIFF: {:?}",
        //     IO_SAME_THREAD.load(Ordering::Relaxed),
        //     IO_DIFF_THREAD.load(Ordering::Relaxed)
        // );
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
    let executor: Arc<Executor> = Arc::new(match executor {
        ExecutorType::LamellarWorkStealing => {
            WorkStealing::new(num_workers, status.clone(), panic.clone()).into()
        }
        ExecutorType::LamellarWorkStealing2 => {
            WorkStealing2::new(num_workers, status.clone(), panic.clone()).into()
        }
        ExecutorType::LamellarWorkStealing3 => {
            WorkStealing3::new(num_workers, status.clone(), panic.clone()).into()
        }
        ExecutorType::AsyncStd => AsyncStdRt::new(num_workers).into(),

        #[cfg(feature = "tokio-executor")]
        ExecutorType::Tokio => TokioRt::new(num_workers).into(),
    });

    let batcher = match config().batcher.as_str() {
        "simple" => BatcherType::Simple(SimpleBatcher::new(
            num_pes,
            am_stall_mark.clone(),
            executor.clone(),
        )),
        "team_am" => BatcherType::TeamAm(TeamAmBatcher::new(
            num_pes,
            am_stall_mark.clone(),
            executor.clone(),
        )),
        _ => panic!("[LAMELLAR ERROR] unexpected batcher type please set LAMELLAR_BATCHER to one of 'simple' or 'team_am'")
    };

    Scheduler::new(
        executor.clone(),
        RegisteredActiveMessages::new(batcher, executor),
        am_stall_mark,
        status,
        panic,
    )
}

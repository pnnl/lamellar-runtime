use crate::active_messaging::batching::simple_batcher::SimpleBatcher;
use crate::active_messaging::batching::direct_batcher::DirectBatcher;
use crate::active_messaging::batching::vec_simple_batcher::VecSimpleBatcher;
use crate::active_messaging::batching::team_am_batcher::TeamAmBatcher;
use crate::active_messaging::batching::vec_team_am_batcher::VecTeamAmBatcher;
use crate::active_messaging::batching::BatcherType;
use crate::active_messaging::registered_active_message::RegisteredActiveMessages;
use crate::active_messaging::*;
use crate::env_var::config;
use crate::lamellae::{Des, Lamellae, SerializedData};
use crate::warnings::RuntimeWarning;
 use crate::scheduler::work_stealing::{TASKS_LAUNCHED, TASKS_FINISHED, TaskType};

use enum_dispatch::enum_dispatch;
use futures_util::Future;
use pin_project::pin_project;
use std::pin::{pin, Pin};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use zerocopy_derive::*;
use tracing::trace;


static LAMELLAR_THREAD_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);
thread_local! {
    /// A thread-local unique identifier for the current Lamellar worker thread.
    ///
    /// Each thread that accesses this value is assigned a monotonically increasing `usize`
    /// ID on first access. IDs are assigned globally across all threads in the process.
    ///
    /// This is useful for fine-grained per-thread diagnostics, pinning work to specific
    /// threads (e.g., via [`LamellarTeam::exec_am_local_thread`][crate::LamellarTeam::exec_am_local_thread]),
    /// and low-level scheduler introspection.
    ///
    /// # Examples
    ///```
    /// use lamellar::LAMELLAR_THREAD_ID;
    ///
    /// LAMELLAR_THREAD_ID.with(|id| {
    ///     println!("current thread id: {}", id);
    /// });
    ///```
    pub static LAMELLAR_THREAD_ID: usize = LAMELLAR_THREAD_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
}

pub(crate) mod work_stealing;
use work_stealing::WorkStealing;

pub(crate) mod work_stealing2;
use work_stealing2::WorkStealing2;

pub(crate) mod work_stealing3;
use work_stealing3::WorkStealing3;

pub(crate) mod async_std_executor;
use async_std_executor::AsyncStdRt;

pub(crate) mod single_thread;
use single_thread::SingleThread;

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

#[repr(C)]
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
    // Pod,
    // Zeroable,
    FromBytes,
    IntoBytes,
    KnownLayout,
    Immutable,

)]
pub(crate) struct ReqId {
    pub(crate) id: usize,
    pub(crate) sub_id: usize,
}

/// Indicates the executor backend
/// Default is a work stealing executor
/// If the "tokio-executor" feature is enabled,the tokio executor can also be used
/// allowing seamless integration with tokio based applications
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
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio-executor")))]
    /// The tokio executor
    Tokio,
    /// Run every submitted future inline on the calling thread (no worker threads).
    SingleThread,
    // Dyn(impl LamellarExecutor),
}

#[derive(Debug)]
#[pin_project]
/// A LamellarTask is a wrapper around a future that is being executed by the Lamellar scheduler.
/// LamellarTasks can be either awaited or blocked on.
pub struct LamellarTask<T> {
    #[pin]
    pub(crate) task: LamellarTaskInner<T>,
    pub(crate) executor: Arc<Executor>,
}

unsafe impl<T: Send> Send for LamellarTask<T> {}
unsafe impl<T: Sync> Sync for LamellarTask<T> {}

impl<T> LamellarTask<T> {
    /// Calls the underlying scheduler block_on method to block the current thread until the task is completed.
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
    // Finished(Option<T>),
    LamellarTask(Option<async_task::Task<T, usize>>),
    AsyncStdTask(async_std::task::JoinHandle<T>),
    #[cfg(feature = "tokio-executor")]
    TokioTask(tokio::task::JoinHandle<T>),
}

unsafe impl<T: Send> Send for LamellarTaskInner<T> {}
unsafe impl<T: Sync> Sync for LamellarTaskInner<T> {}

impl<T> Drop for LamellarTaskInner<T> {
    fn drop(self: &mut Self) {
        // let mut dropped = LamellarTaskInner::Dropped;

        // std::mem::swap(&mut dropped, self);
        match self {
            // LamellarTaskInner::Finished(_) => {}
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
                // LamellarTaskInner::Finished(val) => Poll::Ready(val.take().unwrap()),
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

    fn submit_task_thread<F>(&self, future: F, tid: usize)
    where
        F: Future + Send + 'static,
        F::Output: Send;
    
    fn submit_long_task<F>(&self, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send{
            self.submit_task(future)
        }

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
    fn active(&self) -> bool;
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
    SingleThread(SingleThread),
}

#[derive(Debug)]
pub(crate) struct Scheduler {
    pub(crate) executor: Arc<Executor>,
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
            executor,
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

    pub(crate) fn increment_stall_mark(&self) -> usize {
        self.am_stall_mark.fetch_add(1, Ordering::Release)
    }
    pub(crate) fn submit_am(&self, am: Am) {
        let num_ams = self.num_ams.clone();
        let am_stall_mark = self.increment_stall_mark();
        let ame = self.active_message_engine.clone();
        num_ams.fetch_add(1, Ordering::Relaxed);
        let _am_id = self.max_ams.fetch_add(1, Ordering::Relaxed);
        TASKS_LAUNCHED
            .get(&TaskType::AmSubmit)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
        // println!("am ptr {:p} ", &am);
        let am_future = async move {
            ame.process_msg(am, am_stall_mark, false).await;
            num_ams.fetch_sub(1, Ordering::Relaxed);
            TASKS_FINISHED
                .get(&TaskType::AmSubmit)
                .unwrap()
                .fetch_add(1, Ordering::Relaxed);
        };
        self.executor.submit_task(am_future);
    }

    pub(crate) fn submit_am_thread(&self, am: Am, tid: usize) {
        let num_ams = self.num_ams.clone();
        let am_stall_mark = self.increment_stall_mark();
        let ame = self.active_message_engine.clone();
        num_ams.fetch_add(1, Ordering::Relaxed);
        TASKS_LAUNCHED
            .get(&TaskType::AmSubmit)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
        let _am_id = self.max_ams.fetch_add(1, Ordering::Relaxed);
        let am_future = async move {
            ame.process_msg(am, am_stall_mark, false).await;
            num_ams.fetch_sub(1, Ordering::Relaxed);
            TASKS_FINISHED
                .get(&TaskType::AmSubmit)
                .unwrap()
                .fetch_add(1, Ordering::Relaxed);
        };
        self.executor.submit_task_thread(am_future, tid);
    }

    #[allow(dead_code)]
    pub(crate) fn submit_am_immediate(&self, am: Am) {
        let num_ams = self.num_ams.clone();
        let am_stall_mark = self.increment_stall_mark();
        let ame = self.active_message_engine.clone();
        num_ams.fetch_add(1, Ordering::Relaxed);
        TASKS_LAUNCHED
            .get(&TaskType::AmImmediate)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
        let _am_id = self.max_ams.fetch_add(1, Ordering::Relaxed);
        let am_future = async move {
            ame.process_msg(am, am_stall_mark, false).await;
            num_ams.fetch_sub(1, Ordering::Relaxed);
            TASKS_FINISHED
                .get(&TaskType::AmImmediate)
                .unwrap()
                .fetch_add(1, Ordering::Relaxed);
        };
        self.executor.submit_immediate_task(am_future);
    }

    #[allow(dead_code)]
    pub(crate) async fn exec_am(&self, am: Am) {
        let am_stall_mark = self.increment_stall_mark();
        let ame = self.active_message_engine.clone();
        TASKS_LAUNCHED
            .get(&TaskType::AmExec)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
        self.num_ams.fetch_add(1, Ordering::Relaxed);
        let _am_id = self.max_ams.fetch_add(1, Ordering::Relaxed);
        ame.process_msg(am, am_stall_mark, false).await;
        self.num_ams.fetch_sub(1, Ordering::Relaxed);
        TASKS_FINISHED
            .get(&TaskType::AmExec)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn submit_remote_am(&self, data: SerializedData, lamellae: Arc<Lamellae>) {
        let num_ams = self.num_ams.clone();
        let ame = self.active_message_engine.clone();
        num_ams.fetch_add(1, Ordering::Relaxed);
        let _am_id = self.max_ams.fetch_add(1, Ordering::Relaxed);
        TASKS_LAUNCHED
            .get(&TaskType::AmRemote)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
        let am_future = async move {
            if let Some(header) = data.deserialize_header() {
                let msg = header.msg;
                ame.exec_msg(msg, data, lamellae).await;
            } else {
                data.print();
                panic!("should i be here?");
            }
            num_ams.fetch_sub(1, Ordering::Relaxed);
            TASKS_FINISHED
                .get(&TaskType::AmRemote)
                .unwrap()
                .fetch_add(1, Ordering::Relaxed);
        };
        self.executor.submit_task(am_future);
    }

    pub(crate) fn spawn_task<F>(
        &self,
        task: F,
        outstanding_reqs: Option<Arc<[Arc<AMCounters>]>>,
    ) -> LamellarTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let num_tasks = self.num_tasks.clone();
        num_tasks.fetch_add(1, Ordering::Relaxed);
        if let Some(reqs) = &outstanding_reqs {
            for cntr in reqs.iter() {
                cntr.inc_outstanding(1);
            }
        }
        let _task_id = self.max_tasks.fetch_add(1, Ordering::Relaxed);
        TASKS_LAUNCHED
            .get(&TaskType::TaskSpawn)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
        let future = async move {
            let result = task.await;
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            if let Some(reqs) = &outstanding_reqs {
                for cntr in reqs.iter() {
                    cntr.dec_outstanding(1);
                }
            }
            TASKS_FINISHED
                .get(&TaskType::TaskSpawn)
                .unwrap()
                .fetch_add(1, Ordering::Relaxed);
            result
        };
        self.executor.spawn_task(future, self.executor.clone())
    }



    pub(crate) fn submit_task<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let num_tasks = self.num_tasks.clone();
        num_tasks.fetch_add(1, Ordering::Relaxed);
        let _task_id = self.max_tasks.fetch_add(1, Ordering::Relaxed);
        TASKS_LAUNCHED
            .get(&TaskType::TaskSubmit)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
        let future = async move {
            task.await;
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            TASKS_FINISHED
                .get(&TaskType::TaskSubmit)
                .unwrap()
                .fetch_add(1, Ordering::Relaxed);
        };
        self.executor.submit_task(future);
    }

     pub(crate) fn submit_long_task<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let num_tasks = self.num_tasks.clone();
        num_tasks.fetch_add(1, Ordering::Relaxed);
        let _task_id = self.max_tasks.fetch_add(1, Ordering::Relaxed);
        TASKS_LAUNCHED
            .get(&TaskType::TaskLongSubmit)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
        let future = async move {
            task.await;
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            TASKS_FINISHED
                .get(&TaskType::TaskLongSubmit)
                .unwrap()
                .fetch_add(1, Ordering::Relaxed);
        };
        self.executor.submit_long_task(future);
    }

    pub(crate) fn submit_immediate_task<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let num_tasks = self.num_tasks.clone();
        num_tasks.fetch_add(1, Ordering::Relaxed);
        let _task_id = self.max_tasks.fetch_add(1, Ordering::Relaxed);
        TASKS_LAUNCHED
            .get(&TaskType::TaskImmediate)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
        let future = async move {
            task.await;
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            TASKS_FINISHED
                .get(&TaskType::TaskImmediate)
                .unwrap()
                .fetch_add(1, Ordering::Relaxed);
        };
        self.executor.submit_immediate_task(future);
    }

    pub(crate) fn submit_io_task<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let num_tasks = self.num_tasks.clone();
        num_tasks.fetch_add(1, Ordering::Relaxed);
        let _task_id = self.max_tasks.fetch_add(1, Ordering::Relaxed);
        TASKS_LAUNCHED
            .get(&TaskType::TaskIo)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
        let future = async move {
            task.await;
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            TASKS_FINISHED
                .get(&TaskType::TaskIo)
                .unwrap()
                .fetch_add(1, Ordering::Relaxed);
        };

        self.executor.submit_io_task(future);
    }

    pub(crate) fn exec_task(&self) {
        TASKS_LAUNCHED
            .get(&TaskType::TaskExec)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
        self.executor.exec_task();
        TASKS_FINISHED
            .get(&TaskType::TaskExec)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn block_on<F: Future>(&self, task: F) -> F::Output {
        RuntimeWarning::BlockOn.print();
        TASKS_LAUNCHED
            .get(&TaskType::SchedBlockOn)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
        let res = self.executor.block_on(task);
        TASKS_FINISHED
            .get(&TaskType::SchedBlockOn)
            .unwrap()
            .fetch_add(1, Ordering::Relaxed);
        res
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

    pub(crate) fn active(&self,additional: usize) -> bool {

        self.status.load(Ordering::SeqCst) == SchedulerStatus::Active as u8
            || self.num_tasks.load(Ordering::SeqCst) > 3 + additional // the Lamellae Comm Task, Lamellae Alloc Task, Lamellar Error Task, additional represents a long running task that we dont want to consider when determining if the scheduler is active
    }
    pub(crate) fn num_workers(&self) -> usize {
        self.executor.num_workers()
    }
    pub(crate) fn begin_shutdown(&self) {
        trace!("beginning scheduler shutdown");
        self.status
            .store(SchedulerStatus::Finished as u8, Ordering::SeqCst);
    }
    pub(crate) fn shutdown(&self) {
        let mut timer = std::time::Instant::now();
        while self.panic.load(Ordering::SeqCst) == 0
            && self.num_tasks.load(Ordering::Relaxed) > 3
            && self.num_ams.load(Ordering::Relaxed) > 0
        {
            //the Lamellae Comm Task, Lamellae Alloc Task, Lamellar Error Task
            if timer.elapsed().as_secs_f64() > config().deadlock_warning_timeout {
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
    }
    pub(crate) fn force_shutdown(&self) {
        self.status
            .store(SchedulerStatus::Panic as u8, Ordering::SeqCst);
        self.executor.force_shutdown();
    }

    pub(crate) fn max_threads(executor: &ExecutorType, num_workers: usize) -> usize {
        match executor {
            ExecutorType::LamellarWorkStealing
            | ExecutorType::LamellarWorkStealing2
            | ExecutorType::LamellarWorkStealing3 => std::cmp::max(2, num_workers), // at least one worker + main thread, for more than one worker, the main thread is considered a worker.
            ExecutorType::AsyncStd => num_workers + 1, // the main thread + workers
            #[cfg(feature = "tokio-executor")]
            ExecutorType::Tokio => num_workers + 1, //the main thread + workers
            ExecutorType::SingleThread => 1,
        }
    }
    pub(crate) fn create_scheduler(
        executor: ExecutorType,
        num_pes: usize,
        my_pe: usize,
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
            ExecutorType::AsyncStd => AsyncStdRt::new(num_workers, status.clone()).into(),

            #[cfg(feature = "tokio-executor")]
            ExecutorType::Tokio => TokioRt::new(num_workers, status.clone()).into(),
            ExecutorType::SingleThread => SingleThread::new(status.clone()).into(),
        });

        let batcher = match config().batcher.as_str() {
            "simple" => BatcherType::Simple(SimpleBatcher::new(
                num_pes,
                am_stall_mark.clone(),
                executor.clone(),
            )),
            "direct" => BatcherType::Direct(DirectBatcher::new(
                num_pes,
                my_pe,
                am_stall_mark.clone(),
                executor.clone(),
            )),
            "vec_simple" => BatcherType::VecSimple(VecSimpleBatcher::new(
                num_pes,
                my_pe,
                am_stall_mark.clone(),
                executor.clone(),
            )),
            "team_am" => BatcherType::TeamAm(TeamAmBatcher::new(
                num_pes,
                am_stall_mark.clone(),
                executor.clone(),
            )),
            "vec_team_am" => BatcherType::VecTeamAm(VecTeamAmBatcher::new(
                num_pes,
                my_pe,
                am_stall_mark.clone(),
                executor.clone(),
            )),
            _ => panic!("[LAMELLAR ERROR] unexpected batcher type please set LAMELLAR_BATCHER to one of 'simple', 'direct', 'vec_simple', 'team_am', or 'vec_team_am'")
        };

        Scheduler::new(
            executor.clone(),
            RegisteredActiveMessages::new(batcher, executor),
            am_stall_mark,
            status,
            panic,
        )
    }

    pub(crate) fn init_batcher_task(&self,scheduler: Arc<Scheduler>, lamellae: &Arc<Lamellae>) {
        if let BatcherType::Direct(batcher) = &self.active_message_engine.batcher {
            batcher.init_batcher_task(scheduler,lamellae);
        }
    }
}

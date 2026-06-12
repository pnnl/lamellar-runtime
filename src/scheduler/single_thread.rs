use async_task::Builder;
use futures_util::Future;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use crate::scheduler::{Executor, LamellarExecutor, LamellarTask, LamellarTaskInner};

use tracing::{debug, trace};

static TASK_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
pub(crate) struct SingleThread {
    queue: Arc<Mutex<VecDeque<async_task::Runnable<usize>>>>,
    #[allow(dead_code)]
    status: Arc<AtomicU8>,
}

impl SingleThread {
    pub(crate) fn new(status: Arc<AtomicU8>) -> Self {
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        Self { queue, status }
    }

    // fn schedule<F>(&self, task_id: usize, future: F) -> async_task::Task<F::Output,usize>
    // where
    //     F: Future + Send + 'static,
    //     F::Output: Send,
    // {
    //     let queue = self.queue.clone();
    //     let schedule = move |runnable| {

    //         let mut guard = queue.lock().unwrap();
    //         guard.push_back(runnable);
    //          trace!("Scheduled task {:?} on single thread executor", task_id);
    //     };
    //     let (runnable, task) = Builder::new()
    //         .metadata(task_id)
    //         .spawn(move |task_id| async move {
    //             trace!(
    //                 "[{:?}] Running task {:?} on single thread executor",
    //                 std::thread::current().id(),
    //                 task_id
    //             );
    //             future.await
    //         }, schedule);
    //     runnable.schedule();
    //     task
    // }

    fn run_one_task(&self) -> bool {
        let task = {
            let mut guard = self.queue.lock().unwrap();
            guard.pop_front()
        };
        if let Some(run) = task {
            run.run();
            true
        } else {
            false
        }
    }
}

impl LamellarExecutor for SingleThread {
    fn spawn_task<F>(&self, task: F, executor: Arc<Executor>) -> LamellarTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let task_id = TASK_ID.fetch_add(1, Ordering::Relaxed);
        let queue = self.queue.clone();
        let schedule = move |runnable| {
            let mut guard = queue.lock().unwrap();
            guard.push_back(runnable);
            // trace!("Scheduled task {:?} on single thread executor", task_id);
        };
        let _id = task_id;
        let (runnable, task) = Builder::new().metadata(task_id).spawn(
            move |_task_id| async move {
                // trace!(
                //     "[{:?}] Running task {id} on single thread executor",
                //     std::thread::current().id(),
                // );
                let res = task.await;
                // trace!(
                //     "[{:?}] Completed task {id} on single thread executor",
                //     std::thread::current().id(),
                // );
                res
            },
            schedule,
        );
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
        let task_id = TASK_ID.fetch_add(1, Ordering::Relaxed);
        let queue = self.queue.clone();
        let schedule = move |runnable| {
            let mut guard = queue.lock().unwrap();
            guard.push_back(runnable);
            // trace!("Scheduled task {:?} on single thread executor", task_id);
        };
        let _id = task_id;
        let (runnable, task) = Builder::new().metadata(task_id).spawn(
            move |_task_id| async move {
                // trace!(
                //     "[{:?}] Running task {id} on single thread executor",
                //     std::thread::current().id(),
                // );
                let res = task.await;
                // trace!(
                //     "[{:?}] Completed task {id} on single thread executor",
                //     std::thread::current().id(),
                // );
                res
            },
            schedule,
        );
        runnable.schedule();
        task.detach();
        // Self::detach_task(&mut task_inner);
    }

    fn submit_task_thread<F>(&self, task: F, _tid: usize)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        self.submit_task(task);
    }

    fn submit_io_task<F>(&self, task: F)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        self.submit_task(task);
    }

    fn submit_immediate_task<F>(&self, task: F)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        self.submit_task(task);
    }

    fn exec_task(&self) {
        self.run_one_task();
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        let task_id = TASK_ID.fetch_add(1, Ordering::Relaxed);
        let queue = self.queue.clone();
        let schedule = move |runnable| {
            let mut guard = queue.lock().unwrap();
            guard.push_back(runnable);
            // trace!(
            //     "block on Scheduled task {:?} on single thread executor",
            //     task_id
            // );
        };
        let (runnable, mut task) = unsafe {
            Builder::new().metadata(task_id).spawn_unchecked(
                move |_task_id| async move {
                    // trace!(
                    //     "[{:?}] block on Running task {:?} on single thread executor",
                    //     std::thread::current().id(),
                    //     task_id
                    // );
                    let res = future.await;
                    // trace!(
                    //     "[{:?}] block on Completed task {:?} on single thread executor",
                    //     std::thread::current().id(),
                    //     task_id
                    // );
                    res
                },
                schedule,
            )
        };

        let waker = runnable.waker();
        runnable.run();
        while !task.is_finished() {
            self.run_one_task();
        }
        let cx = &mut Context::from_waker(&waker);
        if let Poll::Ready(output) = Pin::new(&mut task).poll(cx) {
            output
        } else {
            println!(
                "[{:?}] single_thread block on failed --  task id{:?}",
                std::thread::current().id(),
                task.metadata()
            );
            panic!("Task should be ready");
        }
    }

    fn num_workers(&self) -> usize {
        1
    }

    // fn active(&self) -> bool {
    //     self.status.load(Ordering::SeqCst) == SchedulerStatus::Active as u8
    // }

    fn shutdown(&self) {
        debug!("Shutting down SingleThread executor");
        while self.run_one_task() {}
        debug!("SingleThread executor shutdown complete");
    }

    fn force_shutdown(&self) {
        self.shutdown();
    }
}

impl Drop for SingleThread {
    fn drop(&mut self) {
        trace!(target: "drop", "Dropped SingleThread Scheduler");
    }
}

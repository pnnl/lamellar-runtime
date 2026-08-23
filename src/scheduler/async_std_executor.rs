use std::sync::Arc;

use crate::scheduler::{Executor, LamellarExecutor, LamellarTask, LamellarTaskInner};

use async_std::task;

use futures_util::Future;

#[derive(Debug)]
pub(crate) struct AsyncStdRt {
    max_num_threads: usize,
}

impl LamellarExecutor for AsyncStdRt {
    fn spawn_task<F>(&self, task: F, executor: Arc<Executor>) -> LamellarTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        // trace_span!("spawn_task").in_scope(|| {
        let task = task::spawn(task);
        LamellarTask {
            task: LamellarTaskInner::AsyncStdTask(task),
            executor,
            task_id: 0,
        }
        // })
    }
    fn submit_task<F>(&self, task: F)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        // trace_span!("submit_task").in_scope(|| {
        task::spawn(async move { task.await });
        // });
    }

    fn submit_task_thread<F>(&self, task: F, _: usize)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        // trace_span!("submit_task").in_scope(|| {
        task::spawn(async move { task.await });
        // });
    }
    fn submit_io_task<F>(&self, task: F)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        // trace_span!("submit_task").in_scope(|| {
        task::spawn(async move { task.await });
        // });
    }

    fn submit_immediate_task<F>(&self, task: F)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        // trace_span!("submit_task").in_scope(|| {
        task::spawn(async move { task.await });
        // });
    }

    fn block_on<F: Future>(&self, task: F) -> F::Output {
        // trace_span!("block_on").in_scope(||
        task::block_on(task)
        // )
    }

    // //#[tracing::instrument(skip_all)]
    fn shutdown(&self) {
        // i think we just let tokio do this on drop
    }

    // //#[tracing::instrument(skip_all)]
    fn force_shutdown(&self) {
        // i think we just let tokio do this on drop
    }

    // //#[tracing::instrument(skip_all)]
    fn exec_task(&self) {
        // I dont think tokio has a way to do this
    }

    // async-global-executor's pool is a fixed size unless given headroom (see
    // `with_max_threads` in `new` below). Grow it by one thread while we block here
    // so other tasks keep making progress, then ask to shrink back down afterward.
    fn block_in_place<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let _ = task::block_on(async_global_executor::spawn_more_threads(1));
        let result = f();
        async_global_executor::stop_thread().detach();
        result
    }

    // fn set_max_workers(&mut self, num_workers: usize) {
    //     self.max_num_threads = num_workers;
    // }

    fn num_workers(&self) -> usize {
        self.max_num_threads
    }

    // fn active(&self) -> bool {
    //     self.status.load(Ordering::SeqCst) == SchedulerStatus::Active as u8
    // }
}

impl AsyncStdRt {
    pub(crate) fn new(num_workers: usize) -> AsyncStdRt {
        // println!("New TokioRT with {} workers", num_workers);
        async_global_executor::init_with_config(
            async_global_executor::GlobalExecutorConfig::default()
                .with_min_threads(num_workers)
                .with_max_threads(num_workers + 1) // headroom for block_in_place
                .with_thread_name_fn(Box::new(|| "lamellar_worker".to_string())),
        );
        Self {
            max_num_threads: num_workers,
        }
    }
}

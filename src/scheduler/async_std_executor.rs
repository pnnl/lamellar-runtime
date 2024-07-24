use crate::scheduler::{LamellarExecutor, LamellarTask, LamellarTaskInner};

use async_std::task;

use futures_util::Future;

#[derive(Debug)]
pub(crate) struct AsyncStdRt {
    max_num_threads: usize,
}

impl LamellarExecutor for AsyncStdRt {
    fn spawn_task<F>(&self, task: F) -> LamellarTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        // trace_span!("spawn_task").in_scope(|| {
        let task = task::spawn(task);
        LamellarTask {
            task: LamellarTaskInner::AsyncStdTask(task),
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

    // #[tracing::instrument(skip_all)]
    fn shutdown(&self) {
        // i think we just let tokio do this on drop
    }

    // #[tracing::instrument(skip_all)]
    fn force_shutdown(&self) {
        // i think we just let tokio do this on drop
    }

    // #[tracing::instrument(skip_all)]
    fn exec_task(&self) {
        // I dont think tokio has a way to do this
    }

    // fn set_max_workers(&mut self, num_workers: usize) {
    //     self.max_num_threads = num_workers;
    // }

    fn num_workers(&self) -> usize {
        self.max_num_threads
    }
}

impl AsyncStdRt {
    pub(crate) fn new(num_workers: usize) -> AsyncStdRt {
        // println!("New TokioRT with {} workers", num_workers);
        async_global_executor::init_with_config(
            async_global_executor::GlobalExecutorConfig::default()
                .with_min_threads(num_workers)
                .with_max_threads(num_workers)
                .with_thread_name_fn(Box::new(|| "lamellar_worker".to_string())),
        );
        Self {
            max_num_threads: num_workers,
        }
    }
}

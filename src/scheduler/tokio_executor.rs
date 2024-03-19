use crate::scheduler::LamellarExecutor;

use tokio::runtime::Runtime;

use futures_util::Future;

#[derive(Debug)]
pub(crate) struct TokioRt {
    max_num_threads: usize,
    rt: Runtime,
}

impl LamellarExecutor for TokioRt {
    fn submit_task<F>(&self, task: F)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        // trace_span!("submit_task").in_scope(|| {
        self.rt.spawn(async move { task.await });
        // });
    }

    fn submit_immediate_task<F>(&self, task: F)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        // trace_span!("submit_task").in_scope(|| {
        self.rt.spawn(async move { task.await });
        // });
    }

    fn block_on<F: Future>(&self, task: F) -> F::Output {
        // trace_span!("block_on").in_scope(||
        self.rt.block_on(task)
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

    fn set_max_workers(&mut self, num_workers: usize) {
        self.max_num_threads = num_workers;
    }

    fn num_workers(&self) -> usize {
        self.max_num_threads
    }
}

impl TokioRt {
    pub(crate) fn new(num_workers: usize) -> TokioRt {
        // println!("New TokioRT with {} workers", num_workers);
        TokioRt {
            max_num_threads: num_workers + 1, //LAMELLAR_THREADS = num_workers + 1, so for tokio runtime, we actually want num_workers + 1 worker threads as block_on will not do anywork on the main thread (i think)...
            rt: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(num_workers + 1)
                .enable_all()
                .build()
                .unwrap(),
        }
    }
}

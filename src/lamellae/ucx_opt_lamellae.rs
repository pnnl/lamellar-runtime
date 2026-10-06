pub(crate) mod atomic;
pub(crate) mod collective;
pub(crate) mod comm;
pub(crate) mod fabric;
pub(crate) mod mem;
pub(crate) mod rdma;
pub(crate) mod ucc;

use super::{
    comm::{CmdQStatus, CommInfo, CommShutdown},
    command_queues::CommandQueue,
    Comm, Lamellae, LamellaeInit, LamellaeShutdown, LamellaeUtil, Ser, SerializeHeader,
    SerializedData, SERIALIZE_HEADER_LEN,
};
use crate::{config, env_var::HeapMode, lamellar_arch::LamellarArchRT, scheduler::Scheduler};
use comm::UcxOptComm;

use async_trait::async_trait;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use tracing::trace;
use zerocopy::IntoBytes;

pub(crate) struct UcxOptBuilder {
    my_pe: usize,
    num_pes: usize,
    ucx_comm: Arc<Comm>,
}

impl UcxOptBuilder {
    pub(crate) fn new() -> UcxOptBuilder {
        let ucx_comm: Arc<Comm> = Arc::new(UcxOptComm::new().into());
        UcxOptBuilder {
            my_pe: ucx_comm.my_pe(),
            num_pes: ucx_comm.num_pes(),
            ucx_comm: ucx_comm,
        }
    }
}

impl LamellaeInit for UcxOptBuilder {
    fn init_fabric(&mut self) -> (usize, usize) {
        (self.my_pe, self.num_pes)
    }
    fn init_lamellae(&mut self, scheduler: Arc<Scheduler>) -> Arc<Lamellae> {
        // D5: without this, progress only ever happens as a side effect of some future's own
        // poll() call -- nothing drives `ucp_worker_progress` from the executor's idle loop.
        // That is fine while some task is still actively polling, but once the last live op on
        // this PE takes the progress try-lock, runs a capped pass that doesn't finish, and
        // trusts its registered completion-callback waker (see
        // `UcxOptRequest::poll_local`'s doc comment), nothing is left to ever call progress
        // again -- a completion already sitting on the wire is never noticed, so the waker
        // never fires. Confirmed via gdb on a real hang (`put_buffer_test` UnsafeArray np=2):
        // every worker thread idling in the work-stealing steal loop, none in any UCX
        // progress/wait code. Weak ref: this hook must not keep the comm (or the scheduler
        // that owns the hook slot) alive past shutdown.
        let workers = match &*self.ucx_comm {
            Comm::UcxOpt(c) => c.ucx.workers(),
            _ => unreachable!("UcxOptBuilder constructed a non-UcxOpt Comm"),
        };
        let workers_weak = workers.iter().map(Arc::downgrade).collect::<Vec<_>>();
        drop(workers);
        // One worker per call, round-robin: every worker still gets progressed, but an idle
        // thread no longer pays K progress attempts (and K lock probes) per iteration.
        let next = std::sync::atomic::AtomicUsize::new(0);
        // Optional rate limit (LAMELLAR_UCX_HOOK_MIN_US): at most one hook progress call per that
        // many microseconds across all idle threads.
        let min_ns = crate::config().ucx_hook_min_us * 1000;
        let start = std::time::Instant::now();
        let last_ns = std::sync::atomic::AtomicU64::new(0);
        scheduler.set_progress_hook(Box::new(move || {
            if min_ns > 0 {
                let now = start.elapsed().as_nanos() as u64;
                let prev = last_ns.load(std::sync::atomic::Ordering::Relaxed);
                if now.saturating_sub(prev) < min_ns
                    || last_ns
                        .compare_exchange(
                            prev,
                            now,
                            std::sync::atomic::Ordering::Relaxed,
                            std::sync::atomic::Ordering::Relaxed,
                        )
                        .is_err()
                {
                    return 0;
                }
            }
            let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % workers_weak.len();
            match workers_weak[i].upgrade() {
                Some(worker) => {
                    if worker.try_progress(32) {
                        1
                    } else {
                        0
                    }
                }
                None => 0,
            }
        }));

        let ucx = UcxOpt::new(
            self.my_pe,
            self.num_pes,
            self.ucx_comm.clone(),
            scheduler.clone(),
        );
        trace!("created new ucx instance");
        let cq = ucx.cq();
        trace!("created command queue for ucx");
        let ucx = Arc::new(Lamellae::UcxOpt(ucx));
        trace!(target: "lamellae_debug", "created Arc<Lamellae::UcxOpt> instance lamellae cnt: {:?}", Arc::strong_count(&ucx));
        let ucx_clone = ucx.clone();
        let cq_clone = cq.clone();
        scheduler.submit_long_task(async move {
            trace!(target: "lamellae_debug", "starting recv_data task for ucx lamellae cnt: {:?}", Arc::strong_count(&ucx_clone));
            cq_clone.recv_data(ucx_clone.clone()).await;
            trace!(target: "lamellae_debug", "finished recv_data task for ucx lamellae cnt: {:?}", Arc::strong_count(&ucx_clone));
        });

        let cq_clone = cq.clone();
        scheduler.submit_long_task(async move {
            cq_clone.alloc_task().await;
        });
        let cq_clone = cq.clone();
        scheduler.submit_long_task(async move {
            cq_clone.panic_task().await;
        });
        trace!(target: "lamellae_debug", "finished submitting long tasks for ucx lamellae cnt: {:?}", Arc::strong_count(&ucx));
        ucx
    }
}

pub(crate) struct UcxOpt {
    my_pe: usize,
    num_pes: usize,
    ucx_comm: Arc<Comm>,
    active: Arc<AtomicU8>,
    cq: Arc<CommandQueue>,
}

impl std::fmt::Debug for UcxOpt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UcxOpt {{ my_pe: {}, num_pes: {},  active: {:?} }}",
            self.my_pe, self.num_pes, self.active,
        )
    }
}

impl UcxOpt {
    fn new(my_pe: usize, num_pes: usize, ucx_comm: Arc<Comm>, scheduler: Arc<Scheduler>) -> UcxOpt {
        // println!("my_pe {:?} num_pes {:?}",my_pe,num_pes);
        let active = Arc::new(AtomicU8::new(CmdQStatus::Active as u8));
        UcxOpt {
            my_pe: my_pe,
            num_pes: num_pes,
            ucx_comm: ucx_comm.clone(),
            active: active.clone(),
            cq: Arc::new(CommandQueue::new(
                ucx_comm, scheduler, my_pe, num_pes, active,
            )),
        }
    }
    fn cq(&self) -> Arc<CommandQueue> {
        self.cq.clone()
    }
    pub(crate) fn wait_all_print(&self) {
        self.cq.wait_all_print();
    }
    pub(crate) fn comm(&self) -> &Comm {
        &self.ucx_comm
    }
}

impl LamellaeShutdown for UcxOpt {
    fn shutdown(&self) {
        // println!("ucx Lamellae shuting down");
        let _ = self.active.compare_exchange(
            CmdQStatus::Active as u8,
            CmdQStatus::ShuttingDown as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        // println!("set active to 0");
        while (self.active.load(Ordering::SeqCst) != CmdQStatus::Finished as u8
            && self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8)
            || !self.cq.background_tasks_done()
        {
            self.cq.scheduler.exec_task();
        }
        // println!("ucx Lamellae shut down");
    }

    fn force_shutdown(&self) {
        self.cq.send_panic();
        self.active
            .store(CmdQStatus::Panic as u8, Ordering::Relaxed);
    }
    fn force_deinit(&self) {
        self.ucx_comm.force_shutdown();
    }
}

#[async_trait]
impl LamellaeUtil for UcxOpt {
    async fn send_to_pes_async(
        &self,
        pe: Option<usize>,
        team: Arc<LamellarArchRT>,
        data: SerializedData,
    ) {
        // let remote_data = data.into_remote();
        if let Some(pe) = pe {
            self.cq.send_data(data, pe).await;
        } else {
            let mut futures = team
                .team_iter()
                .filter(|pe| pe != &self.my_pe)
                .map(|pe| self.cq.send_data(data.clone(), pe))
                .collect::<FuturesUnordered<_>>(); //in theory this launches all the futures before waiting...
            while let Some(_) = futures.next().await {}
        }
    }
    async fn request_new_alloc(&self, min_size: usize) {
        if config().heap_mode == HeapMode::Static {
            panic!("Error: request_new_alloc should not be called in static heap mode, please set LAMELLAR_HEAP_MODE=dynamic or increase the heap size with LAMELLAR_HEAP_SIZE environment variable");
        }
        // println!("Requesting new pool of size: {} bytes", min_size);
        self.cq.send_alloc(min_size).await;
    }

    async fn send_vec_to_pe_async(&self, pe: usize, vec_data: Vec<u8>) {
        self.cq.send_vec(vec_data, pe).await;
    }

    fn available_to_send(&self, pe: usize) -> bool {
        self.cq.available_to_send(pe)
    }
}

impl Ser for UcxOpt {
    fn serialize_header(
        &self,
        header: SerializeHeader,
        serialized_size: usize,
    ) -> Result<SerializedData, anyhow::Error> {
        let header_size = SERIALIZE_HEADER_LEN;
        let mut ser_data =
            SerializedData::new(self.ucx_comm.clone(), header_size + serialized_size)?;
        ser_data
            .header_as_bytes_mut()
            .copy_from_slice(header.as_bytes()); //fixed-size zerocopy header
        Ok(ser_data)
    }
}

pub(crate) mod atomic;
pub(crate) mod collective;
pub(crate) mod comm;
pub(crate) mod fabric;
pub(crate) mod heap;
pub(crate) mod mailbox;
pub(crate) mod mem;
pub(crate) mod rdma;

use super::{
    comm::{CmdQStatus, CommInfo, CommMem, CommShutdown},
    Comm, Lamellae, LamellaeInit, LamellaeShutdown, LamellaeUtil, Ser, SerializeHeader,
    SerializedData, SERIALIZE_HEADER_LEN,
};
use crate::{config, env_var::HeapMode, lamellar_arch::LamellarArchRT, scheduler::Scheduler};
use comm::ShmemOptComm;
use mailbox::Mailbox;

use async_trait::async_trait;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use zerocopy::IntoBytes;

pub(crate) struct ShmemOptBuilder {
    my_pe: usize,
    num_pes: usize,
    shmem_comm: Arc<Comm>,
}

impl ShmemOptBuilder {
    pub(crate) fn new() -> ShmemOptBuilder {
        let shmem_comm: Arc<Comm> = Arc::new(ShmemOptComm::new().into());
        ShmemOptBuilder {
            my_pe: shmem_comm.my_pe(),
            num_pes: shmem_comm.num_pes(),
            shmem_comm,
        }
    }
}

impl LamellaeInit for ShmemOptBuilder {
    fn init_fabric(&mut self) -> (usize, usize) {
        (self.my_pe, self.num_pes)
    }
    fn init_lamellae(&mut self, scheduler: Arc<Scheduler>) -> Arc<Lamellae> {
        let shmem = ShmemOpt::new(
            self.my_pe,
            self.num_pes,
            self.shmem_comm.clone(),
            scheduler.clone(),
        );
        let mailbox = shmem.mailbox.clone();
        let active = shmem.active.clone();
        let recv_done = shmem.recv_done.clone();
        let shmem = Arc::new(Lamellae::ShmemOpt(shmem));
        // a single recv task drives the mailbox (no alloc or panic tasks)
        scheduler.set_lamellae_tasks(1);
        let lamellae = shmem.clone();
        let sched = scheduler.clone();
        // threads spinning in block_on/wait_all drain the mailbox themselves instead of waiting
        // for the recv task's next turn. Weak refs: the executor owning the hook must not keep
        // the lamellae (or the scheduler that owns it) alive
        if std::env::var("LAMELLAR_SHMEM_NESTED").map_or(true, |v| v != "0") {
            let (mb, la, sc) = (
                Arc::downgrade(&mailbox),
                Arc::downgrade(&shmem),
                Arc::downgrade(&scheduler),
            );
            let done = recv_done.clone();
            scheduler.set_progress_hook(Box::new(move || {
                if done.load(Ordering::Acquire) {
                    return 0;
                }
                // only touch the (hot, shared) lamellae/scheduler refcounts when there is mail
                let Some(mb) = mb.upgrade() else { return 0 };
                if !mb.bell_rung() {
                    return 0;
                }
                match (la.upgrade(), sc.upgrade()) {
                    (Some(la), Some(sc)) => mb.progress(&la, &sc),
                    _ => 0,
                }
            }));
        }
        let task = async move {
            ShmemOpt::recv_task(mailbox, lamellae, sched, active, recv_done).await;
        };
        // opt-in immediate queue: re-queues ahead of AM work on every yield. Off by default:
        // prompt draining defeats batcher aggregation (measured worse on am_bw small sizes)
        if std::env::var("LAMELLAR_SHMEM_RECV_IMM").map_or(false, |v| v != "0") {
            scheduler.submit_immediate_task(task);
        } else {
            scheduler.submit_task(task);
        }
        shmem
    }
}

pub(crate) struct ShmemOpt {
    my_pe: usize,
    num_pes: usize,
    shmem_comm: Arc<Comm>,
    active: Arc<AtomicU8>,
    mailbox: Arc<Mailbox>,
    scheduler: Arc<Scheduler>,
    recv_done: Arc<AtomicBool>,
}

impl std::fmt::Debug for ShmemOpt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ShmemOpt {{ my_pe: {}, num_pes: {},  active: {:?} }}",
            self.my_pe, self.num_pes, self.active,
        )
    }
}

impl ShmemOpt {
    fn new(
        my_pe: usize,
        num_pes: usize,
        shmem_comm: Arc<Comm>,
        scheduler: Arc<Scheduler>,
    ) -> ShmemOpt {
        // println!("my_pe {:?} num_pes {:?}",my_pe,num_pes);
        let active = Arc::new(AtomicU8::new(CmdQStatus::Active as u8));
        ShmemOpt {
            my_pe,
            num_pes,
            mailbox: Arc::new(Mailbox::new(shmem_comm.clone())),
            shmem_comm,
            active,
            scheduler,
            recv_done: Arc::new(AtomicBool::new(false)),
        }
    }

    // Takes its handles by value and drops them before returning so the last
    // Arc<Comm> is never released from a worker thread after shutdown returns.
    async fn recv_task(
        mailbox: Arc<Mailbox>,
        lamellae: Arc<Lamellae>,
        scheduler: Arc<Scheduler>,
        active: Arc<AtomicU8>,
        recv_done: Arc<AtomicBool>,
    ) {
        let idle_spin = mailbox.idle_spin();
        let futex = mailbox.futex();
        let mut idle = 0;
        while active.load(Ordering::SeqCst) == CmdQStatus::Active as u8
            || scheduler.active(0)
            || !mailbox.quiescent()
        {
            if mailbox.check_panic() {
                active.store(CmdQStatus::Panic as u8, Ordering::SeqCst);
                drop(lamellae);
                drop(mailbox);
                drop(scheduler);
                recv_done.store(true, Ordering::Release);
                tracing::warn!("received panic from other PE");
                panic!("received panic from other PE");
            }
            if active.load(Ordering::Relaxed) == CmdQStatus::Panic as u8 {
                break;
            }
            if mailbox.progress(&lamellae, &scheduler) > 0 {
                idle = 0;
            } else {
                idle += 1;
                if futex && idle > idle_spin {
                    mailbox.sleep();
                    idle = 0;
                }
            }
            async_std::task::yield_now().await;
        }
        let _ = active.compare_exchange(
            CmdQStatus::ShuttingDown as u8,
            CmdQStatus::Finished as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        drop(lamellae);
        drop(mailbox);
        drop(scheduler);
        recv_done.store(true, Ordering::Release);
    }

    pub(crate) fn wait_all_print(&self) {
        self.mailbox.print_status();
    }

    pub(crate) fn comm(&self) -> &Comm {
        &self.shmem_comm
    }

    #[inline(always)]
    pub(crate) fn stream_write<F: FnOnce(&mut [u8])>(&self, dst: usize, len: usize, fill: F) -> Result<(), F> {
        self.mailbox.stream_write(dst, len, fill)
    }
}

impl LamellaeShutdown for ShmemOpt {
    fn shutdown(&self) {
        // println!("ShmemOpt Lamellae shuting down");
        let _ = self.active.compare_exchange(
            CmdQStatus::Active as u8,
            CmdQStatus::ShuttingDown as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        // println!("set active to 0");
        while (self.active.load(Ordering::SeqCst) != CmdQStatus::Finished as u8
            && self.active.load(Ordering::SeqCst) != CmdQStatus::Panic as u8)
            || !self.recv_done.load(Ordering::Acquire)
        {
            self.scheduler.exec_task();
        }
        // println!("ShmemOpt Lamellae shut down");
    }

    fn force_shutdown(&self) {
        self.mailbox.send_panic();
        self.active
            .store(CmdQStatus::Panic as u8, Ordering::Relaxed);
    }
    fn force_deinit(&self) {
        self.shmem_comm.force_shutdown();
    }
}

#[async_trait]
impl LamellaeUtil for ShmemOpt {
    async fn send_to_pes_async(
        &self,
        pe: Option<usize>,
        team: Arc<LamellarArchRT>,
        data: SerializedData,
    ) {
        // let remote_data = data.into_remote();
        if let Some(pe) = pe {
            self.mailbox.send(&data, pe).await;
        } else {
            let mut futures = team
                .team_iter()
                .filter(|pe| pe != &self.my_pe)
                .map(|pe| self.mailbox.send(&data, pe))
                .collect::<FuturesUnordered<_>>(); //in theory this launches all the futures before waiting...
            while let Some(_) = futures.next().await {}
        }
    }

    async fn request_new_alloc(&self, min_size: usize) {
        if config().heap_mode == HeapMode::Static {
            panic!("Error: request_new_alloc should not be called in static heap mode, please set LAMELLAR_HEAP_MODE=dynamic or increase the heap size with LAMELLAR_HEAP_SIZE environment variable");
        }
        // pools grow locally from the symmetric rt reservation, no CommandQueue round trip
        if !self.shmem_comm.rt_check_alloc(min_size, std::mem::align_of::<u8>()) {
            self.shmem_comm.alloc_pool(min_size);
        }
    }

    async fn send_vec_to_pe_async(&self, pe: usize, vec_data: Vec<u8>) {
        self.mailbox.send_bytes(vec_data, pe).await;
    }

    fn available_to_send(&self, pe: usize) -> bool {
        self.mailbox.available_to_send(pe)
    }
}

impl Ser for ShmemOpt {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn serialize_header(
        &self,
        header: SerializeHeader,
        serialized_size: usize,
    ) -> Result<SerializedData, anyhow::Error> {
        // trace!("serialize header");
        let header_size = SERIALIZE_HEADER_LEN;
        let mut ser_data =
            SerializedData::new(self.shmem_comm.clone(), header_size + serialized_size)?;
        ser_data
            .header_as_bytes_mut()
            .copy_from_slice(header.as_bytes()); //fixed-size zerocopy header
        Ok(ser_data)
    }
}

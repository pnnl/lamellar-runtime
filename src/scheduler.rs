use crate::active_messaging::*;
use crate::lamellae::{Lamellae, SerializedData};

use enum_dispatch::enum_dispatch;
use futures::Future;
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;

pub(crate) mod work_stealing;
use work_stealing::{WorkStealing, WorkStealingInner};

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

/// The available worker thread scheduling algorithms
#[derive(Debug)]
pub enum SchedulerType {
    WorkStealing,
    // NumaWorkStealing,
    // NumaWorkStealing2,
}

#[enum_dispatch(AmeSchedulerQueue)]
#[derive(Debug)]
pub(crate) enum AmeScheduler {
    WorkStealingInner,
    // NumaWorkStealingInner,
    // NumaWorkStealing2Inner,
}
#[enum_dispatch]
pub(crate) trait AmeSchedulerQueue {
    fn submit_am(
        &self,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
        ame: Arc<ActiveMessageEngineType>,
        am: Am,
    );
    fn submit_am_immediate(
        &self,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
        ame: Arc<ActiveMessageEngineType>,
        am: Am,
    );
    fn submit_work(
        &self,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
        ame: Arc<ActiveMessageEngineType>,
        msg: SerializedData,
        lamellae: Arc<Lamellae>,
    ); //serialized active message
    fn submit_task<F>(&self, future: F)
    where
        F: Future<Output = ()>;
    fn submit_immediate_task<F>(&self, future: F)
    where
        F: Future<Output = ()>;
    fn submit_immediate_task2<F>(&self, future: F)
    where
        F: Future<Output = ()>;
    fn exec_task(&self);

    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future;

    fn shutdown(&self);
    fn shutdown_threads(&self);
    fn force_shutdown(&self);
    fn active(&self) -> bool;
}

#[enum_dispatch(SchedulerQueue)]
#[derive(Debug)]
pub(crate) enum Scheduler {
    WorkStealing,
    // NumaWorkStealing,
    // NumaWorkStealing2,
}
#[enum_dispatch]
pub(crate) trait SchedulerQueue {
    fn submit_am(&self, am: Am); //serialized active message
    fn submit_am_immediate(&self, am: Am); //serialized active message
    fn submit_work(&self, msg: SerializedData, lamellae: Arc<Lamellae>); //serialized active message
    fn submit_task<F>(&self, future: F)
    where
        F: Future<Output = ()>;
    fn submit_immediate_task<F>(&self, future: F)
    where
        F: Future<Output = ()>;
    fn submit_immediate_task2<F>(&self, future: F)
    where
        F: Future<Output = ()>;
    fn submit_task_node<F>(&self, future: F, node: usize)
    where
        F: Future<Output = ()>;
    fn exec_task(&self);
    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future;
    fn shutdown(&self);
    fn shutdown_threads(&self);
    fn force_shutdown(&self);
    fn active(&self) -> bool;
    fn num_workers(&self) -> usize;
}

pub(crate) fn create_scheduler(
    sched: SchedulerType,
    num_pes: usize,
    num_workers: usize,
    panic: Arc<AtomicU8>,
    my_pe: usize,
    // teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
) -> Scheduler {
    match sched {
        SchedulerType::WorkStealing => Scheduler::WorkStealing(work_stealing::WorkStealing::new(
            num_pes,
            num_workers,
            panic
            my_pe,
        )), // SchedulerType::NumaWorkStealing => {
            //     Scheduler::NumaWorkStealing(numa_work_stealing::NumaWorkStealing::new(num_pes))
            // }
            // SchedulerType::NumaWorkStealing2 => {
            //     Scheduler::NumaWorkStealing2(numa_work_stealing2::NumaWorkStealing2::new(num_pes))
            // }
    }
}

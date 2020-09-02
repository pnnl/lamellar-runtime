use crate::active_messaging::*;
use crate::lamellae::{create_lamellae, Backend, Lamellae, LamellaeAM};
use crate::lamellar_memregion::{LamellarMemoryRegion, RemoteMemoryRegion};
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_team::{LamellarArch,LamellarTeam};
use crate::schedulers::{create_scheduler, Scheduler, SchedulerType};
use log::trace;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct LamellarWorld {
    team: Arc<LamellarTeam>,
    counters: Arc<AMCounters>,
    _scheduler: Arc<dyn Scheduler>,
    lamellaes: BTreeMap<Backend, Arc<dyn Lamellae>>,
    my_pe: usize,
    num_pes: usize,
}

impl ActiveMessaging for LamellarWorld {
    fn wait_all(&self) {
        let mut temp_now = Instant::now();
        while self.counters.outstanding_reqs.load(Ordering::SeqCst) > 0 {
            std::thread::yield_now();
            if temp_now.elapsed() > Duration::new(60, 0) {
                println!(
                    "in world wait_all mype: {:?} cnt: {:?} {:?}",
                    self.my_pe,
                    self.counters.send_req_cnt.load(Ordering::SeqCst),
                    self.counters.outstanding_reqs.load(Ordering::SeqCst),
                    // self.counters.recv_req_cnt.load(Ordering::SeqCst),
                );
                // self.lamellae.print_stats();
                temp_now = Instant::now();
            }
        }
    }
    fn barrier(&self) {
        trace!("[{:?}] world barrier", self.my_pe);
        for (backend, lamellae) in &self.lamellaes {
            trace!("[{:?}] {:?} barrier", self.my_pe, backend);
            lamellae.barrier();
        }
    }
    fn exec_am_all<F>(&self, am: F) -> LamellarRequest<F::Output>
    where
        F: LamellarActiveMessage
            + LamellarAM
            + Send
            // + serde::de::DeserializeOwned
            // + std::clone::Clone
            + 'static,
    {
        self.team.exec_am_all(am)
    }
    fn exec_am_pe<F>(&self, pe: usize, am: F) -> LamellarRequest<F::Output>
    where
        F: LamellarActiveMessage
            + LamellarAM
            + Send
            + serde::de::DeserializeOwned
            // + std::clone::Clone
            + 'static,
    {
        assert!(pe < self.num_pes(), "invalid pe: {:?}", pe);
        self.team.exec_am_pe(pe, am)
    }
}

//#feature gated closures for those with nightly
#[cfg(feature = "nightly")]
use crate::active_messaging::remote_closures::ClosureRet;
#[cfg(feature = "nightly")]
impl RemoteClosures for LamellarWorld {
    fn exec_closure_all<
        F: FnOnce() -> T
            + Send
            + Sync
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + 'static,
        T: std::any::Any
            + Send
            + Sync
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone,
    >(
        &self,
        func: F,
    ) -> LamellarRequest<T> {
        trace!("[{:?}] exec closure all", self.my_pe);
        self.team.exec_closure_all(func)
    }

    #[cfg(feature = "nightly")]
    fn exec_closure_pe<
        F: FnOnce() -> T
            + Send
            + Sync
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + 'static,
        T: std::any::Any
            + Send
            + Sync
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone,
    >(
        &self,
        pe: usize,
        func: F,
    ) -> LamellarRequest<T> {
        assert!(pe < self.num_pes(), "invalid pe: {:?}", pe);
        trace!("[{:?}] world exec_closure_pe: [{:?}]", self.my_pe, pe);
        self.team.exec_closure_pe(pe, func)
    }
    #[cfg(feature = "nightly")]
    fn exec_closure_on_return<
        F: FnOnce() -> T + serde::ser::Serialize + serde::de::DeserializeOwned + 'static,
        T: std::any::Any + serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone,
    >(
        &self,
        func: F,
    ) -> ClosureRet {
        self.team.exec_closure_on_return(func)
    }
}

impl RemoteMemoryRegion for LamellarWorld {
    /// allocate a remote memory region from the symmetric heap
    ///
    /// # Arguments
    ///
    /// * `size` - number of elements of T to allocate a memory region for -- (not size in bytes)
    ///
    fn alloc_mem_region<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + std::fmt::Debug
            + 'static,
    >(
        &self,
        size: usize,
    ) -> LamellarMemoryRegion<T> {
        self.barrier();
        self.team.alloc_mem_region::<T>(size)
    }

    /// release a remote memory region from the symmetric heap
    ///
    /// # Arguments
    ///
    /// * `region` - the region to free
    ///
    fn free_memory_region<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + std::fmt::Debug
            + 'static,
    >(
        &self,
        region: LamellarMemoryRegion<T>,
    ) {
        self.team.free_memory_region::<T>(region)
    }
}

impl  LamellarWorld {
    pub fn my_pe(&self) -> usize {
        self.my_pe
    }
    pub fn num_pes(&self) -> usize {
        self.num_pes
    }
    #[allow(non_snake_case)]
    pub fn MB_sent(&self) -> Vec<f64> {
        let mut sent = vec![];
        for (_backend, lamellae) in &self.lamellaes {
            sent.push(lamellae.MB_sent());
        }
        sent
    }

    pub fn team_barrier(&self){
        self.team.barrier();
    }

    pub fn create_team_from_arch<L>(&self, arch: L ) -> Arc<LamellarTeam>
    where L: LamellarArch + 'static{
        LamellarTeam::create_subteam_from_arch(self.team.clone(),arch)
    }
}

impl Drop for LamellarWorld {
    fn drop(&mut self) {
        trace!("[{:?}] world dropping", self.my_pe);
        self.wait_all();
        self.barrier();
        self.team.destroy();
        self.lamellaes.clear();

        // im not sure we want to explicitly call lamellae.finit(). instead we should let
        // have the lamellae instances do that in their own drop implementations.
        // partly this is because we currently call wait_all() and barrier() in the lamellar_team drop impl as well
        // the issue is that the world team wont call drop until after this (LamellarWord) drop is executed...
        // for (backend,lamellae) in &self.lamellaes{
        //     lamellae.finit();
        // }
    }
}

pub struct LamellarWorldBuilder {
    primary_lamellae: Backend,
    secondary_lamellae: HashSet<Backend>,
    scheduler: SchedulerType,
}

impl LamellarWorldBuilder {
    pub fn new() -> LamellarWorldBuilder {
        // simple_logger::init().unwrap();
        trace!("New world builder");
        LamellarWorldBuilder {
            primary_lamellae: Default::default(),
            secondary_lamellae: HashSet::new(),
            scheduler: SchedulerType::WorkStealing,
        }
    }
    pub fn with_lamellae(mut self, lamellae: Backend) -> LamellarWorldBuilder {
        self.primary_lamellae = lamellae;
        self
    }
    pub fn add_lamellae(mut self, lamellae: Backend) -> LamellarWorldBuilder {
        self.secondary_lamellae.insert(lamellae);
        self
    }
    pub fn with_scheduler(mut self, sched: SchedulerType) -> LamellarWorldBuilder {
        self.scheduler = sched;
        self
    }
    pub fn build(self) -> LamellarWorld {
        // for exec in inventory::iter::<TestAm> {
        //     println!("{:#?}", exec);
        // }
        // let mut sched = WorkStealingScheduler::new();
        let mut sched = create_scheduler(self.scheduler);
        let mut lamellae = create_lamellae(self.primary_lamellae, sched.get_queue().clone());
        // let mut lamellae = RofiLamellae::new(sched.get_queue().clone());
        let (num_pes, my_pe) = lamellae.init();
        trace!(
            "[{:?}] lamellae initialized: num_pes {:?} my_pe {:?}",
            my_pe,
            num_pes,
            my_pe
        );
        let lamellae = Arc::new(lamellae);

        let mut lamellaes: BTreeMap<Backend, Arc<dyn LamellaeAM>> = BTreeMap::new();
        lamellaes.insert(lamellae.backend(), lamellae.get_am());
        sched.init(num_pes, my_pe, lamellaes);
        let counters = Arc::new(AMCounters::new());
        let mut world = LamellarWorld {
            team: Arc::new(LamellarTeam::new(
                num_pes,
                my_pe,
                sched.get_queue().clone(),
                counters.clone(),
                lamellae.clone(),
            )),
            counters: counters,
            _scheduler: Arc::new(sched),
            lamellaes: BTreeMap::new(),
            my_pe: my_pe,
            num_pes: num_pes,
        };
        world.lamellaes.insert(lamellae.backend(), lamellae.clone());
        // println!("Lamellar world created with {:?}",lamellae.backend());
        world
    }
}

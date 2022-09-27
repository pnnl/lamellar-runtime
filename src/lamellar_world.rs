use crate::active_messaging::*;
use crate::lamellae::{create_lamellae, Backend, Lamellae, LamellaeComm, LamellaeInit};
use crate::lamellar_arch::LamellarArch;
use crate::lamellar_team::{LamellarTeam, LamellarTeamRT};
use crate::memregion::{
    local::LocalMemoryRegion, shared::SharedMemoryRegion, Dist, RemoteMemoryRegion,
};
use crate::scheduler::{create_scheduler, SchedulerQueue, SchedulerType};
use lamellar_prof::*;
// use log::trace;

use tracing::*;

use futures::Future;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc; //, Weak};

lazy_static! {
    pub(crate) static ref LAMELLAES: RwLock<HashMap<Backend, Arc<Lamellae>>> =
        RwLock::new(HashMap::new());
}

/// Represents all the pe's (processing elements) within a given distributed execution
#[derive(Debug)]
pub struct LamellarWorld {
    team: Arc<LamellarTeam>,
    pub(crate) team_rt: std::pin::Pin<Arc<LamellarTeamRT>>,
    _counters: Arc<AMCounters>,
    my_pe: usize,
    num_pes: usize,
    ref_cnt: Arc<AtomicUsize>,
}

//#[prof]
impl ActiveMessaging for LamellarWorld {
    #[tracing::instrument(skip_all)]
    fn wait_all(&self) {
        self.team.wait_all();
    }
    #[tracing::instrument(skip_all)]
    fn barrier(&self) {
        self.team.barrier();
    }
    #[tracing::instrument(skip_all)]
    fn exec_am_all<F>(&self, am: F) -> Pin<Box<dyn Future<Output = Vec<F::Output>> + Send>>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        self.team.exec_am_all(am)
    }
    #[tracing::instrument(skip_all)]
    fn exec_am_pe<F>(&self, pe: usize, am: F) -> Pin<Box<dyn Future<Output = F::Output> + Send>>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        assert!(pe < self.num_pes(), "invalid pe: {:?}", pe);
        self.team.exec_am_pe(pe, am)
    }
    #[tracing::instrument(skip_all)]
    fn exec_am_local<F>(&self, am: F) -> Pin<Box<dyn Future<Output = F::Output> + Send>>
    where
        F: LamellarActiveMessage + LocalAM + 'static,
    {
        self.team.exec_am_local(am)
    }
}

//#[prof]
impl RemoteMemoryRegion for LamellarWorld {
    // /// allocate a shared memory region from the asymmetric heap
    // ///
    // /// # Arguments
    // ///
    // /// * `size` - number of elements of T to allocate a memory region for -- (not size in bytes)
    // ///
    #[tracing::instrument(skip_all)]
    fn alloc_shared_mem_region<T: Dist>(&self, size: usize) -> SharedMemoryRegion<T> {
        self.barrier();
        self.team.alloc_shared_mem_region::<T>(size)
    }

    // /// allocate a local memory region from the asymmetric heap
    // ///
    // /// # Arguments
    // ///
    // /// * `size` - number of elements of T to allocate a memory region for -- (not size in bytes)
    // ///
    #[tracing::instrument(skip_all)]
    fn alloc_local_mem_region<T: Dist>(&self, size: usize) -> LocalMemoryRegion<T> {
        self.team.alloc_local_mem_region::<T>(size)
    }

    // /// release a remote memory region from the asymmetric heap
    // ///
    // /// # Arguments
    // ///
    // /// * `region` - the region to free
    // ///
    // fn free_shared_memory_region<T: AmDist+ 'static>(&self, region: SharedMemoryRegion<T>) {
    //     self.team.free_shared_memory_region(region)
    // }

    // /// release a remote memory region from the asymmetric heap
    // ///
    // /// # Arguments
    // ///
    // /// * `region` - the region to free
    // ///
    // fn free_local_memory_region<T: AmDist+ 'static>(&self, region: LocalMemoryRegion<T>) {
    //     self.team.free_local_memory_region(region)
    // }
}

//#[prof]
impl LamellarWorld {
    /// gets the id of this pe (roughly equivalent to MPI Rank)
    #[tracing::instrument(skip_all)]
    pub fn my_pe(&self) -> usize {
        self.my_pe
    }
    /// returns nummber of pe's in this execution
    #[tracing::instrument(skip_all)]
    pub fn num_pes(&self) -> usize {
        self.num_pes
    }

    pub fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future,
    {
        trace_span!("block_on").in_scope(|| self.team_rt.scheduler.block_on(f))
    }

    /// return the number of megabytes sent
    #[allow(non_snake_case)]
    #[tracing::instrument(skip_all)]
    pub fn MB_sent(&self) -> f64 {
        let mut sent = vec![];
        for (_backend, lamellae) in LAMELLAES.read().iter() {
            sent.push(lamellae.MB_sent());
        }
        sent[0]
    }

    // pub fn team_barrier(&self) {
    //     self.team.barrier();
    // }

    /// create a team containing any number of pe's from the world using the provided LamellarArch (layout)
    #[tracing::instrument(skip_all)]
    pub fn create_team_from_arch<L>(&self, arch: L) -> Option<Arc<LamellarTeam>>
    where
        L: LamellarArch + std::hash::Hash + 'static,
    {
        if let Some(team) = LamellarTeam::create_subteam_from_arch(self.team.clone(), arch) {
            // self.teams
            //     .write()
            //     .insert(team.team.team_hash, Arc::downgrade(&team.team));
            Some(team)
        } else {
            None
        }
    }

    /// return the team encompassing all pe's in the world
    #[tracing::instrument(skip_all)]
    pub fn team(&self) -> Arc<LamellarTeam> {
        self.team.clone()
    }
    #[tracing::instrument(skip_all)]
    pub fn barrier(&self) {
        self.team.barrier();
    }
    #[tracing::instrument(skip_all)]
    pub fn wait_all(&self) {
        self.team.wait_all();
    }
}

impl Clone for LamellarWorld {
    #[tracing::instrument(skip_all)]
    fn clone(&self) -> Self {
        self.ref_cnt.fetch_add(1, Ordering::SeqCst);
        LamellarWorld {
            team: self.team.clone(),
            team_rt: self.team_rt.clone(),
            // teams: self.teams.clone(),
            _counters: self._counters.clone(),
            my_pe: self.my_pe.clone(),
            num_pes: self.num_pes.clone(),
            ref_cnt: self.ref_cnt.clone(),
        }
    }
}
//#[prof]
impl Drop for LamellarWorld {
    #[tracing::instrument(skip_all)]
    fn drop(&mut self) {
        let cnt = self.ref_cnt.fetch_sub(1, Ordering::SeqCst);
        if cnt == 1 {
            // println!("[{:?}] world dropping", self.my_pe);
            self.wait_all();
            self.barrier();

            self.team_rt.destroy();
            LAMELLAES.write().clear();

            // println!("team: {:?} ", Arc::strong_count(&self.team));
            // println!(
            //     "team_rt: {:?} {:?}",
            //     &self.team_rt.team_hash,
            //     Arc::strong_count(&self.team_rt)
            // );
            // let teams = self.teams.read();
            // for (k, team) in teams.iter() {
            // println!("team map: {:?} {:?}", k, Weak::strong_count(&team));
            // }
            // println!("counters: {:?}", Arc::strong_count(&self._counters));

            fini_prof!();
        }
        // println!("[{:?}] world dropped", self.my_pe);
    }
}

/// Lamellar World Builder used for customization
#[derive(Debug)]
pub struct LamellarWorldBuilder {
    primary_lamellae: Backend,
    // secondary_lamellae: HashSet<Backend>,
    scheduler: SchedulerType,
}

//#[prof]
impl LamellarWorldBuilder {
    #[tracing::instrument(skip_all)]
    pub fn new() -> LamellarWorldBuilder {
        // simple_logger::init().unwrap();
        // trace!("New world builder");
        let scheduler = match std::env::var("LAMELLAR_SCHEDULER") {
            Ok(val) => {
                let scheduler = val.parse::<usize>().unwrap();
                if scheduler == 0 {
                    SchedulerType::WorkStealing
                } else if scheduler == 1 {
                    SchedulerType::NumaWorkStealing
                } else if scheduler == 2 {
                    SchedulerType::NumaWorkStealing2
                } else {
                    SchedulerType::WorkStealing
                }
            }
            Err(_) => SchedulerType::WorkStealing,
        };
        LamellarWorldBuilder {
            primary_lamellae: Default::default(),
            // secondary_lamellae: HashSet::new(),
            scheduler: scheduler,
        }
    }

    /// specify the lamellae backend to use for this execution
    #[tracing::instrument(skip_all)]
    pub fn with_lamellae(mut self, lamellae: Backend) -> LamellarWorldBuilder {
        self.primary_lamellae = lamellae;
        self
    }

    // pub fn add_lamellae(mut self, lamellae: Backend) -> LamellarWorldBuilder {
    //     self.secondary_lamellae.insert(lamellae);
    //     self
    // }

    /// specify the scheduler to use for this execution
    #[tracing::instrument(skip_all)]
    pub fn with_scheduler(mut self, sched: SchedulerType) -> LamellarWorldBuilder {
        self.scheduler = sched;
        self
    }

    /// Instantiate a world handle
    #[tracing::instrument(skip_all)]
    pub fn build(self) -> LamellarWorld {
        // let teams = Arc::new(RwLock::new(HashMap::new()));
        let mut lamellae_builder = create_lamellae(self.primary_lamellae);
        let (my_pe, num_pes) = lamellae_builder.init_fabric();
        let sched_new = Arc::new(create_scheduler(
            self.scheduler,
            num_pes,
            // my_pe,
            // teams.clone(),
        ));
        let lamellae = lamellae_builder.init_lamellae(sched_new.clone());
        let counters = Arc::new(AMCounters::new());
        lamellae.barrier();
        let team_rt = LamellarTeamRT::new(
            num_pes,
            my_pe,
            sched_new.clone(),
            counters.clone(),
            lamellae.clone(),
            // teams.clone(),
        );

        let world = LamellarWorld {
            team: LamellarTeam::new(None, team_rt.clone(), false),
            team_rt: team_rt.clone(),
            // teams: teams.clone(),
            _counters: counters,
            my_pe: my_pe,
            num_pes: num_pes,
            ref_cnt: Arc::new(AtomicUsize::new(1)),
        };
        // world
        //     .teams
        //     .write()
        //     .insert(world.team_rt.team_hash, Arc::downgrade(&team_rt));
        LAMELLAES
            .write()
            .insert(lamellae.backend(), lamellae.clone());

        world.barrier();
        world
    }
}

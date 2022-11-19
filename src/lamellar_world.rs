use crate::active_messaging::*;
use crate::lamellae::{create_lamellae, Backend, Lamellae, LamellaeComm, LamellaeInit};
use crate::lamellar_arch::LamellarArch;
use crate::lamellar_team::{LamellarTeam, LamellarTeamRT};
use crate::memregion::{
    one_sided::OneSidedMemoryRegion, shared::SharedMemoryRegion, Dist, RemoteMemoryRegion,
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

/// An abstraction representing all the PE's (processing elements) within a given distributed execution.
///
/// Constructing a LamellarWorld is necessesary to perform any remote operations or distributed communications.
///
/// A LamellarWorld instance can launch and await the result of [active messages][ActiveMessaging],
/// can create distributed [memory regions][RemoteMemoryRegion] and [LamellarArrays][array],
/// create [sub teams][LamellarWorld::create_team_from_arch] of PEs, and be used to construct [LamellarTaskGroups][crate::lamellar_task_group::LamellarTaskGroup].
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
    #[tracing::instrument(skip_all)]
    fn wait_all(&self) {
        self.team.wait_all();
    }
    #[tracing::instrument(skip_all)]
    fn barrier(&self) {
        self.team.barrier();
    }

    fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future,
    {
        trace_span!("block_on").in_scope(|| self.team_rt.scheduler.block_on(f))
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
    fn alloc_one_sided_mem_region<T: Dist>(&self, size: usize) -> OneSidedMemoryRegion<T> {
        self.team.alloc_one_sided_mem_region::<T>(size)
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
    // fn free_local_memory_region<T: AmDist+ 'static>(&self, region: OneSidedMemoryRegion<T>) {
    //     self.team.free_local_memory_region(region)
    // }
}

//#[prof]
impl LamellarWorld {
    /// Returns the id of this PE (roughly equivalent to MPI Rank)
    ///
    /// # Examples
    ///```
    /// let my_pe = world.my_pe();
    ///```
    #[tracing::instrument(skip_all)]
    pub fn my_pe(&self) -> usize {
        self.my_pe
    }
    /// Returns nummber of PE's in this execution
    ///
    /// # Examples
    ///```
    /// let num_pes = world.num_pes();
    ///```
    #[tracing::instrument(skip_all)]
    pub fn num_pes(&self) -> usize {
        self.num_pes
    }

    /// Run a future to completion on the current thread
    ///
    /// This function will block the caller until the given future has completed, the future is executed within the Lamellar threadpool
    ///
    /// Users can await any future, including those returned from lamellar remote operations
    ///
    /// # Examples
    ///```
    /// use lamellar::ActiveMessaging;
    ///
    /// let request = world.exec_am_local(Am{...}); //launch am on all pes
    /// let result = world.block_on(request); //block until am has executed
    /// // you can also directly pass an async block
    /// world.block_on(async move {
    ///     let file = async_std::fs::File.open(...);.await;
    ///     for pe in 0..num_pes{
    ///         let data = file.read(...).await;
    ///         world.exec_am_pe(pe,DataAm{data}).await;
    ///     }
    ///     world.exec_am_all(...).await;
    /// });
    ///```
    pub fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future,
    {
        trace_span!("block_on").in_scope(|| self.team_rt.scheduler.block_on(f))
    }

    #[doc(hidden)]
    #[allow(non_snake_case)]
    #[tracing::instrument(skip_all)]
    pub fn MB_sent(&self) -> f64 {
        let mut sent = vec![];
        for (_backend, lamellae) in LAMELLAES.read().iter() {
            sent.push(lamellae.MB_sent());
        }
        sent[0]
    }

    /// create a team containing any number of pe's from the world using the provided LamellarArch (layout)
    ///
    /// # Examples
    ///```
    ///
    /// let even_team = world.create_team_from_arch(StridedArch::new(
    ///                                            0, // start pe
    ///                                            2,// stride
    ///                                            (world.num_pes() / 2.0), //num pes in team
    /// ));
    ///```
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

    #[doc(hidden)]
    #[tracing::instrument(skip_all)]
    pub fn team(&self) -> Arc<LamellarTeam> {
        self.team.clone()
    }

    #[doc(hidden)]
    #[tracing::instrument(skip_all)]
    pub fn barrier(&self) {
        self.team.barrier();
    }

    #[doc(hidden)]
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

/// An implementation of the Builder design pattern, used to construct an instance of a LamellarWorld.
///
/// Allows for customizing the way the world is built.
///
/// Currently this includes being able to specify the lamellae [Backend][crate::Backend] and workpool scheduler type.
///
/// # Examples
///
///```
/// use lamellar::{LamellarWorldBuilder,Backend,SchedulerType};
///
/// let world = LamellarWorldBuilder::new()
///                             .with_lamellae(Backend::Rofi)
///                             .with_scheduler(SchedulerType::WorkStealing)
///                             .build();
///```
#[derive(Debug)]
pub struct LamellarWorldBuilder {
    primary_lamellae: Backend,
    // secondary_lamellae: HashSet<Backend>,
    scheduler: SchedulerType,
}

//#[prof]
impl LamellarWorldBuilder {
    /// Construct a new lamellar world builder
    #[tracing::instrument(skip_all)]
    pub fn new() -> LamellarWorldBuilder {
        // simple_logger::init().unwrap();
        // trace!("New world builder");
        let scheduler = match std::env::var("LAMELLAR_SCHEDULER") {
            Ok(val) => {
                let scheduler = val.parse::<usize>().unwrap();
                if scheduler == 0 {
                    SchedulerType::WorkStealing
                }
                // else if scheduler == 1 {
                //     SchedulerType::NumaWorkStealing
                // } else if scheduler == 2 {
                //     SchedulerType::NumaWorkStealing2
                // }
                else {
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

    /// Specify the lamellae backend to use for this execution
    /// # Examples
    ///
    ///```
    /// use lamellar::{LamellarWorldBuilder,Backend};
    ///
    /// let builder = LamellarWorldBuilder::new()
    ///                             .with_lamellae(Backend::Local);
    ///```
    #[tracing::instrument(skip_all)]
    pub fn with_lamellae(mut self, lamellae: Backend) -> LamellarWorldBuilder {
        self.primary_lamellae = lamellae;
        self
    }

    // pub fn add_lamellae(mut self, lamellae: Backend) -> LamellarWorldBuilder {
    //     self.secondary_lamellae.insert(lamellae);
    //     self
    // }

    /// Specify the scheduler to use for this execution
    /// # Examples
    ///
    ///```
    /// use lamellar::{LamellarWorldBuilder,SchedulerType};
    ///
    /// let builder = LamellarWorldBuilder::new()
    ///                             .with_scheduler(SchedulerType::WorkStealing);
    ///```
    #[tracing::instrument(skip_all)]
    pub fn with_scheduler(mut self, sched: SchedulerType) -> LamellarWorldBuilder {
        self.scheduler = sched;
        self
    }

    /// Instantiate a LamellarWorld object
    /// # Examples
    ///
    ///```
    /// use lamellar::{LamellarWorldBuilder,Backend,SchedulerType};
    ///
    /// let world = LamellarWorldBuilder::new()
    ///                             .with_lamellae(Backend::Rofi)
    ///                             .with_scheduler(SchedulerType::WorkStealing)
    ///                             .build();
    ///```
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

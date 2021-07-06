use crate::active_messaging::*;
// use crate::lamellae::{create_lamellae, Backend, Lamellae, LamellaeAM};
use crate::lamellae::{create_lamellae, Backend, Lamellae, LamellaeComm, LamellaeInit};
use crate::lamellar_arch::LamellarArch;
#[cfg(feature = "experimental")]
use crate::lamellar_array::LamellarArray;
use crate::lamellar_memregion::{
    LamellarLocalMemoryRegion, LamellarMemoryRegion, RemoteMemoryRegion,
};
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_team::{LamellarTeam, LamellarTeamRT};
use crate::scheduler::{create_scheduler, SchedulerType};
use lamellar_prof::*;
use log::trace;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::collections::{BTreeMap, HashSet};
// use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};

lazy_static! {
    pub(crate) static ref LAMELLAES: RwLock<HashMap<Backend, Arc<Lamellae>>> =
        RwLock::new(HashMap::new());
}

pub struct LamellarWorld {
    team: Arc<LamellarTeam>,
    team_rt: Arc<LamellarTeamRT>,
    teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeam>>>>,
    counters: Arc<AMCounters>,
    lamellaes_new: BTreeMap<crate::lamellae::Backend, Arc<Lamellae>>,
    my_pe: usize,
    num_pes: usize,
}

//#[prof]
impl ActiveMessaging for LamellarWorld {
    fn wait_all(&self) {
        self.team.wait_all();
        // let mut temp_now = Instant::now();
        // while self.counters.outstanding_reqs.load(Ordering::SeqCst) > 0 {
        //     std::thread::yield_now();
        //     if temp_now.elapsed() > Duration::new(60, 0) {
        //         println!(
        //             "in world wait_all mype: {:?} cnt: {:?} {:?}",
        //             self.my_pe,
        //             self.counters.send_req_cnt.load(Ordering::SeqCst),
        //             self.counters.outstanding_reqs.load(Ordering::SeqCst),
        //             // self.counters.recv_req_cnt.load(Ordering::SeqCst),
        //         );
        //         // self.lamellae.print_stats();
        //         temp_now = Instant::now();
        //     }
        // }
    }
    fn barrier(&self) {
        // println!("[{:?}] world barrier", self.my_pe);
        // for (backend, lamellae) in &self.lamellaes_new {
        //     trace!("[{:?}] {:?} barrier", self.my_pe, backend);
        //     lamellae.barrier();
        //     // println!("world barrier!!!!!!!!!!!!");
        // }
        self.team.barrier();
    }
    fn exec_am_all<F>(&self, am: F) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + Send + Sync + 'static,
    {
        self.team.exec_am_all(am)
    }
    fn exec_am_pe<F>(
        &self,
        pe: usize,
        am: F,
    ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + Send + Sync + 'static,
    {
        assert!(pe < self.num_pes(), "invalid pe: {:?}", pe);
        self.team.exec_am_pe(pe, am)
    }
    fn exec_am_local<F>(&self, am: F) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: LamellarActiveMessage + LocalAM + Send + Sync + 'static,
    {
        self.team.exec_am_local(am)
    }
}

//#feature gated closures for those with nightly
#[cfg(feature = "nightly")]
use crate::active_messaging::remote_closures::ClosureRet;
#[cfg(feature = "nightly")]
//#[prof]
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
    ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync> {
        trace!("[{:?}] exec closure all", self.my_pe);
        self.team_rt.exec_closure_all(func)
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
    ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync> {
        assert!(pe < self.num_pes(), "invalid pe: {:?}", pe);
        trace!("[{:?}] world exec_closure_pe: [{:?}]", self.my_pe, pe);
        self.team_rt.exec_closure_pe(pe, func)
    }
    #[cfg(feature = "nightly")]
    fn exec_closure_on_return<
        F: FnOnce() -> T + serde::ser::Serialize + serde::de::DeserializeOwned + 'static,
        T: std::any::Any + serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone,
    >(
        &self,
        func: F,
    ) -> ClosureRet {
        self.team_rt.exec_closure_on_return(func)
    }
}

//#[prof]
impl RemoteMemoryRegion for LamellarWorld {
    /// allocate a shared memory region from the asymmetric heap
    ///
    /// # Arguments
    ///
    /// * `size` - number of elements of T to allocate a memory region for -- (not size in bytes)
    ///
    fn alloc_shared_mem_region<
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
        self.team_rt.alloc_shared_mem_region::<T>(size)
    }

    /// allocate a local memory region from the asymmetric heap
    ///
    /// # Arguments
    ///
    /// * `size` - number of elements of T to allocate a memory region for -- (not size in bytes)
    ///
    fn alloc_local_mem_region<
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
    ) -> LamellarLocalMemoryRegion<T> {
        self.team_rt.alloc_local_mem_region::<T>(size)
    }

    /// release a remote memory region from the asymmetric heap
    ///
    /// # Arguments
    ///
    /// * `region` - the region to free
    ///
    fn free_shared_memory_region<
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
        self.team_rt.free_shared_memory_region(region)
    }

    /// release a remote memory region from the asymmetric heap
    ///
    /// # Arguments
    ///
    /// * `region` - the region to free
    ///
    fn free_local_memory_region<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + std::fmt::Debug
            + 'static,
    >(
        &self,
        region: LamellarLocalMemoryRegion<T>,
    ) {
        self.team_rt.free_local_memory_region(region)
    }
}

//#[prof]
impl LamellarWorld {
    pub fn my_pe(&self) -> usize {
        self.my_pe
    }
    pub fn num_pes(&self) -> usize {
        self.num_pes
    }
    #[allow(non_snake_case)]
    pub fn MB_sent(&self) -> Vec<f64> {
        let mut sent = vec![];
        for (_backend, lamellae) in &self.lamellaes_new {
            sent.push(lamellae.MB_sent());
        }
        sent
    }

    pub fn team_barrier(&self) {
        self.team.barrier();
        println!("team barrier!!!!!!!!!!!!");
    }

    pub fn create_team_from_arch<L>(&self, arch: L) -> Option<Arc<LamellarTeam>>
    where
        L: LamellarArch + std::hash::Hash + 'static,
    {
        if let Some(team) = LamellarTeam::create_subteam_from_arch(self.team.clone(), arch) {
            self.teams
                .write()
                .insert(team.team.team_hash, Arc::downgrade(&team));
            Some(team)
        } else {
            None
        }
    }

    pub fn team(&self) -> Arc<LamellarTeam> {
        self.team.clone()
    }

    #[cfg(feature = "experimental")]
    pub fn new_array<
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
    ) -> LamellarArray<T> {
        self.barrier();
        LamellarArray::new(self.team.clone(), size, self.counters.clone())
    }

    // #[cfg(feature = "experimental")]
    // pub fn local_array<
    //     T: serde::ser::Serialize
    //         + serde::de::DeserializeOwned
    //         + std::clone::Clone
    //         + Send
    //         + Sync
    //         + 'static,
    // >(
    //     &self,
    //     size: usize,
    //     init: T,
    // ) -> LamellarLocalArray<T> {
    //     LamellarLocalArray::new(size, init, self.team_rt.lamellae.get_rdma().clone())
    // }
}

//#[prof]
impl Drop for LamellarWorld {
    fn drop(&mut self) {
        // println!("[{:?}] world dropping", self.my_pe);
        self.wait_all();
        // self.team_rt.barrier();
        self.barrier();
        self.team_rt.destroy();
        // self.lamellaes.clear();
        // for (backend,lamellae) in self.lamellaes_new.iter(){
        //     println!("lamellae: {:?} {:?}",backend,Arc::strong_count(&lamellae))
        // }
        self.lamellaes_new.clear();
        LAMELLAES.write().clear();

        // println!("team: {:?} ",Arc::strong_count(&self.team));
        // println!("team_rt: {:?} {:?}",&self.team_rt.team_hash,Arc::strong_count(&self.team_rt));
        // let teams = self.teams.read();
        //     for (k,team) in teams.iter(){
        //         println!("team map: {:?} {:?}",k,Weak::strong_count(&team));
        //     }

        // println!("counters: {:?}",Arc::strong_count(&self.counters));

        fini_prof!();

        // im not sure we want to explicitly call lamellae.finit(). instead we should let
        // have the lamellae instances do that in their own drop implementations.
        // partly this is because we currently call wait_all() and barrier() in the lamellar_team drop impl as well
        // the issue is that the world team wont call drop until after this (LamellarWord) drop is executed...
        // for (backend,lamellae) in &self.lamellaes{
        //     lamellae.finit();
        // }
        // println!("[{:?}] world dropped", self.my_pe);
    }
}

pub struct LamellarWorldBuilder {
    primary_lamellae: Backend,
    secondary_lamellae: HashSet<Backend>,
    scheduler: SchedulerType,
}

//#[prof]
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
        // println!("building world");
        // for exec in inventory::iter::<TestAm> {
        //     println!("{:#?}", exec);
        // }
        // let mut sched = WorkStealingScheduler::new();
        let teams = Arc::new(RwLock::new(HashMap::new()));
        let mut lamellae_builder = create_lamellae(self.primary_lamellae);
        let (my_pe, num_pes) = lamellae_builder.init_fabric();
        let sched_new = Arc::new(create_scheduler(
            self.scheduler,
            num_pes,
            my_pe,
            teams.clone(),
        ));
        let lamellae = lamellae_builder.init_lamellae(sched_new.clone());
        let counters = Arc::new(AMCounters::new());
        lamellae.barrier();
        let team_rt = Arc::new(LamellarTeamRT::new(
            num_pes,
            my_pe,
            sched_new.clone(),
            counters.clone(),
            lamellae.clone(),
        ));
        // println!("world gen barrier!!!!!!!!!!!!");
        let mut world = LamellarWorld {
            team: Arc::new(LamellarTeam {
                world: None,
                team: team_rt.clone(),
                teams: teams.clone(),
            }),
            team_rt: team_rt,
            teams: teams.clone(),
            counters: counters,
            lamellaes_new: BTreeMap::new(),
            my_pe: my_pe,
            num_pes: num_pes,
        };
        world
            .teams
            .write()
            .insert(world.team_rt.team_hash, Arc::downgrade(&world.team));
        // world.lamellaes.insert(lamellae.backend(), lamellae.clone());
        LAMELLAES
            .write()
            .insert(lamellae.backend(), lamellae.clone());
        world
            .lamellaes_new
            .insert(lamellae.backend(), lamellae.clone());
        // println!("Lamellar world created with {:?}", lamellae.backend());
        world.barrier();
        world
    }
}

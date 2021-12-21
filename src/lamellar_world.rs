use crate::active_messaging::*;
use crate::lamellae::{create_lamellae, Backend, Lamellae, LamellaeComm, LamellaeInit};
use crate::lamellar_arch::LamellarArch;
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_team::{LamellarTeam, LamellarTeamRT};
use crate::memregion::{
    local::LocalMemoryRegion, shared::SharedMemoryRegion, Dist, RemoteMemoryRegion,
};
use crate::scheduler::{create_scheduler, SchedulerType};
use lamellar_prof::*;
use log::trace;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

lazy_static! {
    pub(crate) static ref LAMELLAES: RwLock<HashMap<Backend, Arc<Lamellae>>> =
        RwLock::new(HashMap::new());
}

/// Represents all the pe's (processing elements) within a given distributed execution
pub struct LamellarWorld {
    team: Arc<LamellarTeam>,
    pub(crate) team_rt: std::pin::Pin<Arc<LamellarTeamRT>>,
    teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
    _counters: Arc<AMCounters>,
    my_pe: usize,
    num_pes: usize,
    ref_cnt: Arc<AtomicUsize>,
}

//#[prof]
impl ActiveMessaging for LamellarWorld {
    fn wait_all(&self) {
        self.team.wait_all();
    }
    fn barrier(&self) {
        self.team.barrier();
    }
    fn exec_am_all<F>(&self, am: F) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        self.team.exec_am_all(am)
    }
    fn exec_am_pe<F>(
        &self,
        pe: usize,
        am: F,
    ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
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
// #[cfg(feature = "nightly")]
// use crate::active_messaging::remote_closures::ClosureRet;
// #[cfg(feature = "nightly")]
// //#[prof]
// impl RemoteClosures for LamellarWorld {
//     fn exec_closure_all<
//         F: FnOnce() -> T
//             + Send
//             + Sync
//             + serde::ser::Serialize
//             + serde::de::DeserializeOwned
//             + std::clone::Clone
//             + 'static,
//         T: std::any::Any
//             + Send
//             + Sync
//             + serde::ser::Serialize
//             + serde::de::DeserializeOwned
//             + std::clone::Clone,
//     >(
//         &self,
//         func: F,
//     ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync> {
//         trace!("[{:?}] exec closure all", self.my_pe);
//         self.team_rt.exec_closure_all(func)
//     }

//     #[cfg(feature = "nightly")]
//     fn exec_closure_pe<
//         F: FnOnce() -> T
//             + Send
//             + Sync
//             + serde::ser::Serialize
//             + serde::de::DeserializeOwned
//             + std::clone::Clone
//             + 'static,
//         T: std::any::Any
//             + Send
//             + Sync
//             + serde::ser::Serialize
//             + serde::de::DeserializeOwned
//             + std::clone::Clone,
//     >(
//         &self,
//         pe: usize,
//         func: F,
//     ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync> {
//         assert!(pe < self.num_pes(), "invalid pe: {:?}", pe);
//         trace!("[{:?}] world exec_closure_pe: [{:?}]", self.my_pe, pe);
//         self.team_rt.exec_closure_pe(pe, func)
//     }
//     #[cfg(feature = "nightly")]
//     fn exec_closure_on_return<
//         F: FnOnce() -> T + serde::ser::Serialize + serde::de::DeserializeOwned + 'static,
//         T: std::any::Any + serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone,
//     >(
//         &self,
//         func: F,
//     ) -> ClosureRet {
//         self.team_rt.exec_closure_on_return(func)
//     }
// }

//#[prof]
impl RemoteMemoryRegion for LamellarWorld {
    // /// allocate a shared memory region from the asymmetric heap
    // ///
    // /// # Arguments
    // ///
    // /// * `size` - number of elements of T to allocate a memory region for -- (not size in bytes)
    // ///
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
    pub fn my_pe(&self) -> usize {
        self.my_pe
    }
    /// returns nummber of pe's in this execution
    pub fn num_pes(&self) -> usize {
        self.num_pes
    }

    /// return the number of megabytes sent
    #[allow(non_snake_case)]
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
    pub fn team(&self) -> Arc<LamellarTeam> {
        self.team.clone()
    }
}

impl Clone for LamellarWorld {
    fn clone(&self) -> Self {
        self.ref_cnt.fetch_add(1, Ordering::SeqCst);
        LamellarWorld {
            team: self.team.clone(),
            team_rt: self.team_rt.clone(),
            teams: self.teams.clone(),
            _counters: self._counters.clone(),
            my_pe: self.my_pe.clone(),
            num_pes: self.num_pes.clone(),
            ref_cnt: self.ref_cnt.clone(),
        }
    }
}
//#[prof]
impl Drop for LamellarWorld {
    fn drop(&mut self) {
        let cnt = self.ref_cnt.fetch_sub(1, Ordering::SeqCst);
        if cnt == 1 {
            // println!("[{:?}] world dropping", self.my_pe);
            self.wait_all();
            self.barrier();

            self.team_rt.destroy();
            LAMELLAES.write().clear();

            // println!("team: {:?} ",Arc::strong_count(&self.team));
            // println!("team_rt: {:?} {:?}",&self.team_rt.team_hash,Arc::strong_count(&self.team_rt));
            // let teams = self.teams.read();
            //     for (k,team) in teams.iter(){
            //         println!("team map: {:?} {:?}",k,Weak::strong_count(&team));
            //     }
            // println!("counters: {:?}",Arc::strong_count(&self.counters));

            fini_prof!();
        }
        // println!("[{:?}] world dropped", self.my_pe);
    }
}

/// Lamellar World Builder used for customization
pub struct LamellarWorldBuilder {
    primary_lamellae: Backend,
    // secondary_lamellae: HashSet<Backend>,
    scheduler: SchedulerType,
}

//#[prof]
impl LamellarWorldBuilder {
    pub fn new() -> LamellarWorldBuilder {
        // simple_logger::init().unwrap();
        trace!("New world builder");
        LamellarWorldBuilder {
            primary_lamellae: Default::default(),
            // secondary_lamellae: HashSet::new(),
            scheduler: SchedulerType::WorkStealing,
        }
    }

    /// specify the lamellae backend to use for this execution
    pub fn with_lamellae(mut self, lamellae: Backend) -> LamellarWorldBuilder {
        self.primary_lamellae = lamellae;
        self
    }

    // pub fn add_lamellae(mut self, lamellae: Backend) -> LamellarWorldBuilder {
    //     self.secondary_lamellae.insert(lamellae);
    //     self
    // }

    /// specify the scheduler to use for this execution
    pub fn with_scheduler(mut self, sched: SchedulerType) -> LamellarWorldBuilder {
        self.scheduler = sched;
        self
    }

    /// Instantiate a world handle
    pub fn build(self) -> LamellarWorld {
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
        let team_rt = LamellarTeamRT::new(
            num_pes,
            my_pe,
            sched_new.clone(),
            counters.clone(),
            lamellae.clone(),
            teams.clone(),
        );

        let world = LamellarWorld {
            team: LamellarTeam::new(None, team_rt.clone(), teams.clone(), false),
            team_rt: team_rt.clone(),
            teams: teams.clone(),
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

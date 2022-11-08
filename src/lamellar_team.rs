use crate::active_messaging::*;
use crate::barrier::Barrier;
use crate::lamellae::{AllocationType, Lamellae, LamellaeComm, LamellaeRDMA};
use crate::lamellar_arch::{GlobalArch, IdError, LamellarArch, LamellarArchEnum, LamellarArchRT};
use crate::lamellar_request::*;
use crate::lamellar_world::LamellarWorld;
use crate::memregion::{
    one_sided::OneSidedMemoryRegion, shared::SharedMemoryRegion, Dist, LamellarMemoryRegion, MemoryRegion,
    RemoteMemoryRegion,
};
use crate::scheduler::{ReqId, Scheduler, SchedulerQueue};
#[cfg(feature = "nightly")]
use crate::utils::ser_closure;

// use log::trace;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
// use std::any;
use core::pin::Pin;
use futures::Future;
use lamellar_prof::*;
use parking_lot::Mutex;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::marker::PhantomPinned;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc; //, Weak};
use std::time::{Duration, Instant};

use std::cell::Cell;
use std::marker::PhantomData;

use tracing::*;

// to manage team lifetimes properly we need a seperate user facing handle that contains a strong link to the inner team.
// this outer handle has a lifetime completely tied to whatever the user wants
// when the outer handle is dropped, we do the appropriate barriers and then remove the inner team from the runtime data structures
// this should allow for the inner team to persist while at least one user handle exists in the world.

/// An abstraction used to group PEs into distributed computational units.
///
/// The [ActiveMessaging] trait is implemented for `Arc<LamellarTeam>`, new LamellarTeams will always be returned as `Arc<LamellarTeam>`.
///
/// Actions taking place on a team, only execute on members of the team.
/// # Examples
///```
/// use lamellar::ActiveMessaging;
/// use lamellar::array::AtomicArray;
///
/// // we can launch and await the results of active messages on a given team
/// let req = team.exec_am_all(...);
/// let result = team.block_on(req);
/// // we can also create a distributed array so that its data only resides on the members of the team.
/// let even_pes = team.create_subteam_from_arch(...);
/// let array: AtomicArray<usize> = AtomicArray::new(even_pes, ...);
/// ```
pub struct LamellarTeam {
    pub(crate) world: Option<Arc<LamellarTeam>>,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) am_team: bool, 
}

//#[prof]
impl LamellarTeam {
    #[tracing::instrument(skip_all)]
    pub(crate) fn new(
        world: Option<Arc<LamellarTeam>>,
        team: Pin<Arc<LamellarTeamRT>>,
        // teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
        am_team: bool,
    ) -> Arc<LamellarTeam> {
        // unsafe{
        //     let pinned_team = Pin::into_inner_unchecked(team.clone()).clone();
        //     let team_ptr = Arc::into_raw(pinned_team);
        //     println!{"new lam team: {:?} {:?} {:?} {:?}",&team_ptr,team_ptr, (team.remote_ptr_addr as *mut (*const LamellarTeamRT)).as_ref(), (*(team.remote_ptr_addr as *mut (*const LamellarTeamRT))).as_ref()};

        // }
        let the_team = Arc::new(LamellarTeam {
            world,
            team,
            // teams,
            am_team,
        });
        // unsafe{
        //     let pinned_team = Pin::into_inner_unchecked(the_team.team.clone()).clone();
        //     let team_ptr = Arc::into_raw(pinned_team);
        //     println!{"new lam team: {:?} {:?} {:?} {:?}",&team_ptr,team_ptr, (the_team.team.remote_ptr_addr as *mut (*const LamellarTeamRT)).as_ref(), (*(the_team.team.remote_ptr_addr as *mut (*const LamellarTeamRT))).as_ref()};

        // }
        the_team
    }

    /// return a list of (world-based) pe ids representing the members of the team
    #[allow(dead_code)]
    #[tracing::instrument(skip_all)]
    pub fn get_pes(&self) -> Vec<usize> {
        self.team.arch.team_iter().collect::<Vec<usize>>()
    }

    /// return number of pes in team
    #[tracing::instrument(skip_all)]
    pub fn num_pes(&self) -> usize {
        self.team.arch.num_pes()
    }

    /// return the world-based id of this pe
    #[tracing::instrument(skip_all)]
    pub fn world_pe_id(&self) -> usize {
        self.team.world_pe
    }

    /// return the team-based id of this pe
    #[tracing::instrument(skip_all)]
    pub fn team_pe_id(&self) -> Result<usize, IdError> {
        self.team.arch.team_pe(self.team.world_pe)
    }

    /// create a subteam containing any number of pe's from this team using the provided LamellarArch (layout)
    ///
    /// # Examples
    ///```
    /// //assume we have a parent team "team"
    /// let even_team = team.create_team_from_arch(StridedArch::new(
    ///                                            0, // start pe
    ///                                            2,// stride
    ///                                            (team.num_pes() / 2.0), //num pes in team
    /// ));
    ///```
    #[tracing::instrument(skip_all)]
    pub fn create_subteam_from_arch<L>(
        parent: Arc<LamellarTeam>,
        arch: L,
    ) -> Option<Arc<LamellarTeam>>
    where
        L: LamellarArch + std::hash::Hash + 'static,
    {
        let world = if let Some(world) = &parent.world {
            world.clone()
        } else {
            parent.clone()
        };
        if let Some(team) =
            LamellarTeamRT::create_subteam_from_arch(world.team.clone(), parent.team.clone(), arch)
        {
            let team = LamellarTeam::new(Some(world), team.clone(), parent.am_team);
            Some(team)
        } else {
            None
        }
    }

    /// text based representation of the team
    #[tracing::instrument(skip_all)]
    pub fn print_arch(&self) {
        self.team.print_arch()
    }

    /// team wide synchronization method which blocks calling thread until all PEs in the team have entered
    ///
    /// # Examples
    ///```
    /// //do some work
    /// team.barrier(); //block until all PEs have entered the barrier
    ///```
    #[tracing::instrument(skip_all)]
    pub fn barrier(&self) {
        self.team.barrier()
    }
}

impl std::fmt::Debug for LamellarTeam {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "pes: {:?}",
            self.team.arch.team_iter().collect::<Vec<usize>>()
        )
    }
}

// #[prof]
impl ActiveMessaging for Arc<LamellarTeam> {
    #[tracing::instrument(skip_all)]
    fn exec_am_all<F>(&self, am: F) -> Pin<Box<dyn Future<Output = Vec<F::Output>> + Send>>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        // trace!("[{:?}] team exec am all request", self.team.world_pe);
        self.team.exec_am_all_tg(am, None).into_future()
    }

    #[tracing::instrument(skip_all)]
    fn exec_am_pe<F>(&self, pe: usize, am: F) -> Pin<Box<dyn Future<Output = F::Output> + Send>>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        self.team.exec_am_pe_tg(pe, am, None).into_future()
    }

    #[tracing::instrument(skip_all)]
    fn exec_am_local<F>(&self, am: F) -> Pin<Box<dyn Future<Output = F::Output> + Send>>
    where
        F: LamellarActiveMessage + LocalAM + 'static,
    {
        self.team.exec_am_local_tg(am, None).into_future()
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
        trace_span!("block_on").in_scope(|| self.team.scheduler.block_on(f))
    }
}

impl RemoteMemoryRegion for Arc<LamellarTeam> {
    #[tracing::instrument(skip_all)]
    fn alloc_shared_mem_region<T: Dist>(&self, size: usize) -> SharedMemoryRegion<T> {
        self.team.barrier.barrier();
        let mr: SharedMemoryRegion<T> = if self.team.num_world_pes == self.team.num_pes {
            SharedMemoryRegion::new(size, self.team.clone(), AllocationType::Global)
        } else {
            SharedMemoryRegion::new(
                size,
                self.team.clone(),
                AllocationType::Sub(self.team.arch.team_iter().collect::<Vec<usize>>()),
            )
        };
        self.team.barrier.barrier();
        mr
    }

    #[tracing::instrument(skip_all)]
    fn alloc_one_sided_mem_region<T: Dist>(&self, size: usize) -> OneSidedMemoryRegion<T> {
        let mut lmr = OneSidedMemoryRegion::try_new(size, &self.team, self.team.lamellae.clone());
        while let Err(_err) = lmr {
            std::thread::yield_now();
            self.team
                .lamellae
                .alloc_pool(size * std::mem::size_of::<T>());
            lmr = OneSidedMemoryRegion::try_new(size, &self.team, self.team.lamellae.clone());
        }
        lmr.expect("out of memory")
    }
}

#[derive(Debug)]
pub struct IntoLamellarTeam {
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
}

impl From<Pin<Arc<LamellarTeamRT>>> for IntoLamellarTeam {
    #[tracing::instrument(skip_all)]
    fn from(team: Pin<Arc<LamellarTeamRT>>) -> Self {
        IntoLamellarTeam { team: team.clone() }
    }
}
impl From<Arc<LamellarTeam>> for IntoLamellarTeam {
    #[tracing::instrument(skip_all)]
    fn from(team: Arc<LamellarTeam>) -> Self {
        IntoLamellarTeam {
            team: team.team.clone(),
        }
    }
}

impl From<&LamellarWorld> for IntoLamellarTeam {
    #[tracing::instrument(skip_all)]
    fn from(world: &LamellarWorld) -> Self {
        IntoLamellarTeam {
            team: world.team_rt.clone(),
        }
    }
}

impl From<LamellarWorld> for IntoLamellarTeam {
    #[tracing::instrument(skip_all)]
    fn from(world: LamellarWorld) -> Self {
        IntoLamellarTeam {
            team: world.team_rt.clone(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub(crate) struct LamellarTeamRemotePtr {
    pub(crate) addr: usize,
    pub(crate) pe: usize,
    pub(crate) backend: crate::Backend,
}

impl From<LamellarTeamRemotePtr> for Pin<Arc<LamellarTeamRT>> {
    #[tracing::instrument(skip_all)]
    fn from(remote_ptr: LamellarTeamRemotePtr) -> Self {
        let lamellae = if let Some(lamellae) = crate::LAMELLAES.read().get(&remote_ptr.backend) {
            lamellae.clone()
        } else {
            panic!("unexepected lamellae backend {:?}", &remote_ptr.backend);
        };
        let local_team_addr = lamellae.local_addr(remote_ptr.pe, remote_ptr.addr);
        let team_ptr = local_team_addr as *mut *const LamellarTeamRT;

        unsafe {
            Arc::increment_strong_count(*team_ptr);
            Pin::new_unchecked(Arc::from_raw(*team_ptr))
        }
    }
}

impl From<Pin<Arc<LamellarTeamRT>>> for LamellarTeamRemotePtr {
    #[tracing::instrument(skip_all)]
    fn from(team: Pin<Arc<LamellarTeamRT>>) -> Self {
        LamellarTeamRemotePtr {
            addr: team.remote_ptr_addr,
            pe: team.world_pe,
            backend: team.lamellae.backend(),
        }
    }
}

#[doc(hidden)]
pub struct LamellarTeamRT {
    #[allow(dead_code)]
    pub(crate) world: Option<Pin<Arc<LamellarTeamRT>>>,
    parent: Option<Pin<Arc<LamellarTeamRT>>>,
    // teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
    sub_teams: RwLock<HashMap<usize, Pin<Arc<LamellarTeamRT>>>>,
    mem_regions: RwLock<HashMap<usize, Box<LamellarMemoryRegion<u8>>>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) lamellae: Arc<Lamellae>,
    pub(crate) arch: Arc<LamellarArchRT>,
    pub(crate) world_pe: usize,
    pub(crate) num_world_pes: usize,
    pub(crate) team_pe: Result<usize, IdError>,
    pub(crate) num_pes: usize,
    pub(crate) team_counters: AMCounters,
    pub(crate) world_counters: Arc<AMCounters>, // can probably remove this?
    pub(crate) id: usize,
    sub_team_id_cnt: AtomicUsize,
    barrier: Barrier,
    dropped: MemoryRegion<usize>,
    pub(crate) remote_ptr_addr: usize,
    pub(crate) team_hash: u64,
    _pin: std::marker::PhantomPinned,
}

impl std::fmt::Debug for LamellarTeamRT {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "pes: {:?}",
            self.arch.team_iter().collect::<Vec<usize>>()
        )
    }
}

//#[prof]
impl Hash for LamellarTeamRT {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.team_hash.hash(state);
    }
}

//#[prof]
impl LamellarTeamRT {
    #[tracing::instrument(skip_all)]
    pub(crate) fn new(
        //creates a new root team
        num_pes: usize,
        world_pe: usize,
        scheduler: Arc<Scheduler>,
        world_counters: Arc<AMCounters>,
        lamellae: Arc<Lamellae>,
        // teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
    ) -> Pin<Arc<LamellarTeamRT>> {
        let arch = Arc::new(LamellarArchRT {
            parent: None,
            arch: LamellarArchEnum::GlobalArch(GlobalArch::new(num_pes)),
            num_pes: num_pes,
        });
        lamellae.barrier();

        let alloc = AllocationType::Global;
        let team = LamellarTeamRT {
            world: None,
            parent: None,
            // teams: teams,
            sub_teams: RwLock::new(HashMap::new()),
            mem_regions: RwLock::new(HashMap::new()),
            scheduler: scheduler.clone(),
            lamellae: lamellae.clone(),
            arch: arch.clone(),
            world_pe: world_pe,
            team_pe: Ok(world_pe),
            num_world_pes: num_pes,
            num_pes: num_pes,
            team_counters: AMCounters::new(),
            world_counters: world_counters,
            id: 0,
            team_hash: 0, //easy id to look up for global
            sub_team_id_cnt: AtomicUsize::new(0),
            barrier: Barrier::new(
                world_pe,
                num_pes,
                lamellae.clone(),
                arch.clone(),
                scheduler.clone(),
            ),
            dropped: MemoryRegion::new(num_pes, lamellae.clone(), alloc.clone()),
            remote_ptr_addr: lamellae
                .alloc(std::mem::size_of::<*const LamellarTeamRT>(), alloc)
                .unwrap(),
            _pin: PhantomPinned,
        };

        // println!("team addr {:x}",team.remote_ptr_addr);
        unsafe {
            for e in team.dropped.as_mut_slice().unwrap().iter_mut() {
                *e = 0;
            }
        }
        // lamellae.get_am().barrier(); //this is a noop currently
        lamellae.barrier();
        let team = Arc::pin(team);
        // let team_ptr = Arc::into_raw(team.clone()); //*const Arc<LamellarTeamRT>
        // let team_ptr = &team as *const Pin<Arc<LamellarTeamRT>>;
        // unsafe {
        //     std::ptr::copy_nonoverlapping(&team_ptr, team.remote_ptr_addr as *mut (*const Pin<Arc<LamellarTeamRT>>), 1);

        // }
        unsafe {
            let pinned_team = Pin::into_inner_unchecked(team.clone()).clone(); //get access to inner arc (it will stay pinned)
            let team_ptr = Arc::into_raw(pinned_team); //we consume the arc to get access to raw ptr (this ensures we wont drop the team until destroy is called)
                                                       // std::ptr::copy_nonoverlapping(&(&team as *const Pin<Arc<LamellarTeamRT>>), team.remote_ptr_addr as *mut (*const Pin<Arc<LamellarTeamRT>>), 1);
                                                       // we are copying the address of the team into the remote ptr address, because of the two previous calls, we ensure its location is pinned and its lifetime will live until the team is destroyed.
            std::ptr::copy_nonoverlapping(
                &team_ptr,
                team.remote_ptr_addr as *mut *const LamellarTeamRT,
                1,
            );
            // println!{"{:?} {:?} {:?} {:?}",&team_ptr,team_ptr, (team.remote_ptr_addr as *mut (*const LamellarTeamRT)).as_ref(), (*(team.remote_ptr_addr as *mut (*const LamellarTeamRT))).as_ref()};
        }

        // println!("sechduler_new: {:?}", Arc::strong_count(&team.scheduler));
        // println!("lamellae: {:?}", Arc::strong_count(&team.lamellae));
        // println!("arch: {:?}", Arc::strong_count(&team.arch));
        // println!(
        //     "world_counters: {:?}",
        //     Arc::strong_count(&team.world_counters)
        // );

        team.barrier.barrier();
        team
    }

    #[tracing::instrument(skip_all)]
    pub(crate) fn destroy(&self) {
        // println!("destroying team? {:?}", self.mem_regions.read().len());
        // println!(
        //     "in team destroy mype: {:?} cnt: {:?} {:?}",
        //     self.world_pe,
        //     self.team_counters.send_req_cnt.load(Ordering::SeqCst),
        //     self.team_counters.outstanding_reqs.load(Ordering::SeqCst),
        // );
        // for _lmr in self.mem_regions.read().iter() {
        //     // println!("lmr {:?}",_lmr);
        //     //TODO: i have a gut feeling we might have an issue if a mem region was destroyed on one node, but not another
        //     // add a barrier method that takes a message so if we are stuck in the barrier for a long time we can say that
        //     // this is probably mismatched frees.
        //     self.barrier.barrier();
        // }
        // println!("sechduler_new: {:?}", Arc::strong_count(&self.scheduler));
        // println!("lamellae: {:?}", Arc::strong_count(&self.lamellae));
        // println!("arch: {:?}", Arc::strong_count(&self.arch));
        // println!(
        //     "world_counters: {:?}",
        //     Arc::strong_count(&self.world_counters)
        // );
        self.wait_all();
        self.mem_regions.write().clear();
        self.sub_teams.write().clear(); // not sure this is necessary or should be allowed? sub teams delete themselves from this map when dropped...
                                        // what does it mean if we drop a parent team while a sub_team is valid?
        if let None = &self.parent {
            // println!("shutdown lamellae, going to shutdown scheduler");
            self.scheduler.shutdown();
            self.put_dropped();
            self.drop_barrier();
            self.lamellae.shutdown();
        }
        // println!("sechduler_new: {:?}", Arc::strong_count(&self.scheduler));
        // println!("lamellae: {:?}", Arc::strong_count(&self.lamellae));
        // println!("arch: {:?}", Arc::strong_count(&self.arch));
        // println!(
        //     "world_counters: {:?}",
        //     Arc::strong_count(&self.world_counters)
        // );
        // println!(
        //     "in team destroy mype: {:?} cnt: {:?} {:?}",
        //     self.world_pe,
        //     self.team_counters.send_req_cnt.load(Ordering::SeqCst),
        //     self.team_counters.outstanding_reqs.load(Ordering::SeqCst),
        // );
        // println!("team destroyed")
    }
    #[allow(dead_code)]
    #[tracing::instrument(skip_all)]
    pub fn get_pes(&self) -> Vec<usize> {
        self.arch.team_iter().collect::<Vec<usize>>()
    }
    #[tracing::instrument(skip_all)]
    pub fn world_pe_id(&self) -> usize {
        self.world_pe
    }
    #[tracing::instrument(skip_all)]
    pub fn team_pe_id(&self) -> Result<usize, IdError> {
        self.arch.team_pe(self.world_pe)
    }

    #[tracing::instrument(skip_all)]
    pub fn create_subteam_from_arch<L>(
        world: Pin<Arc<LamellarTeamRT>>,
        parent: Pin<Arc<LamellarTeamRT>>,
        arch: L,
    ) -> Option<Pin<Arc<LamellarTeamRT>>>
    where
        L: LamellarArch + std::hash::Hash + 'static,
    {
        if parent.team_pe_id().is_ok() {
            //make sure everyone in parent is ready
            let id = parent.sub_team_id_cnt.fetch_add(1, Ordering::SeqCst);
            let mut hasher = DefaultHasher::new();
            parent.hash(&mut hasher);
            id.hash(&mut hasher);
            arch.hash(&mut hasher);
            let hash = hasher.finish();
            let archrt = Arc::new(LamellarArchRT::new(parent.arch.clone(), arch));
            // println!("arch: {:?}",archrt);
            parent.barrier();
            // ------ ensure team is being constructed synchronously and in order across all pes in parent ------ //
            let parent_alloc = AllocationType::Sub(parent.arch.team_iter().collect::<Vec<usize>>());
            let temp_buf = MemoryRegion::<usize>::new(
                parent.num_pes,
                parent.lamellae.clone(),
                parent_alloc.clone(),
            );

            let temp_array = MemoryRegion::<usize>::new(
                parent.num_pes,
                parent.lamellae.clone(),
                AllocationType::Local,
            );
            let temp_array_slice = unsafe { temp_array.as_mut_slice().unwrap() };
            for e in temp_array_slice.iter_mut() {
                *e = 0;
            }
            unsafe {
                temp_buf.put_slice(parent.world_pe, 0, &temp_array_slice[..parent.num_pes]);
            }
            let s = Instant::now();
            parent.barrier();
            let timeout = Instant::now() - s;
            temp_array_slice[0] = hash as usize;
            if let Ok(parent_world_pe) = parent.arch.team_pe(parent.world_pe) {
                for world_pe in parent.arch.team_iter() {
                    unsafe {
                        temp_buf.put_slice(world_pe, parent_world_pe, &temp_array_slice[0..1]);
                    }
                }
            }
            parent.check_hash_vals(hash as usize, &temp_buf, timeout);

            let remote_ptr_addr = parent
                .lamellae
                .alloc(std::mem::size_of::<*const LamellarTeamRT>(), parent_alloc)
                .unwrap();
            // ------------------------------------------------------------------------------------------------- //

            for e in temp_array_slice.iter_mut() {
                *e = 0;
            }
            let num_pes = archrt.num_pes();
            parent.barrier();
            let team = LamellarTeamRT {
                world: Some(world.clone()),
                parent: Some(parent.clone()),
                // teams: parent.teams.clone(),
                sub_teams: RwLock::new(HashMap::new()),
                mem_regions: RwLock::new(HashMap::new()),
                scheduler: parent.scheduler.clone(),
                lamellae: parent.lamellae.clone(),
                arch: archrt.clone(),
                world_pe: parent.world_pe,
                num_world_pes: parent.num_world_pes,
                team_pe: archrt.team_pe(parent.world_pe),
                num_pes: num_pes,
                team_counters: AMCounters::new(),
                world_counters: parent.world_counters.clone(),
                id: id,
                sub_team_id_cnt: AtomicUsize::new(0),
                barrier: Barrier::new(
                    parent.world_pe,
                    parent.num_world_pes,
                    parent.lamellae.clone(),
                    archrt,
                    parent.scheduler.clone(),
                ),
                team_hash: hash,
                dropped: temp_buf,
                remote_ptr_addr: remote_ptr_addr,
                _pin: PhantomPinned,
            };
            unsafe {
                for e in team.dropped.as_mut_slice().unwrap().iter_mut() {
                    *e = 0;
                }
            }
            let team = Arc::pin(team);
            unsafe {
                let pinned_team = Pin::into_inner_unchecked(team.clone()).clone();
                let team_ptr = Arc::into_raw(pinned_team);
                // std::ptr::copy_nonoverlapping(&(&team as *const Pin<Arc<LamellarTeamRT>>), team.remote_ptr_addr as *mut (*const Pin<Arc<LamellarTeamRT>>), 1);
                std::ptr::copy_nonoverlapping(
                    &team_ptr,
                    team.remote_ptr_addr as *mut *const LamellarTeamRT,
                    1,
                );
            }

            let mut sub_teams = parent.sub_teams.write();
            sub_teams.insert(team.id, team.clone());
            parent.barrier();
            team.barrier();
            parent.barrier();
            Some(team)
        } else {
            None
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn num_pes(&self) -> usize {
        self.arch.num_pes()
    }

    #[cfg_attr(test, allow(unreachable_code), allow(unused_variables))]
    #[tracing::instrument(skip_all)]
    fn check_hash_vals(&self, hash: usize, hash_buf: &MemoryRegion<usize>, timeout: Duration) {
        #[cfg(test)]
        return;

        let mut s = Instant::now();
        let mut cnt = 0;
        for pe in hash_buf.as_slice().unwrap() {
            while *pe == 0 {
                std::thread::yield_now();
                if s.elapsed().as_secs_f64() > 5.0 {
                    println!(
                        "[{:?}] ({:?})  hash: {:?}",
                        self.world_pe,
                        hash,
                        hash_buf.as_slice().unwrap()
                    );
                    s = Instant::now();
                }
            }
            if *pe != hash && Instant::now() - s > timeout {
                println!(
                    "[{:?}] ({:?})  hash: {:?}",
                    self.world_pe,
                    hash,
                    hash_buf.as_slice().unwrap()
                );
                panic!("team creating mismatch! Ensure teams are constructed in same order on every pe");
            } else {
                std::thread::yield_now();
                cnt = cnt + 1;
            }
        }
        // println!("{:?} {:?}", hash,hash_buf.as_slice().unwrap());
    }

    #[tracing::instrument(skip_all)]
    fn put_dropped(&self) {
        if let Some(parent) = &self.parent {
            let temp_slice = unsafe { self.dropped.as_mut_slice().unwrap() };

            let my_index = parent
                .arch
                .team_pe(self.world_pe)
                .expect("invalid parent pe");
            temp_slice[my_index] = 1;
            for world_pe in self.arch.team_iter() {
                if world_pe != self.world_pe {
                    unsafe {
                        self.dropped.put_slice(
                            world_pe,
                            my_index,
                            &temp_slice[my_index..=my_index],
                        );
                    }
                }
            }
        } else {
            let temp_slice = unsafe { self.dropped.as_mut_slice().unwrap() };
            temp_slice[self.world_pe] = 1;
            for world_pe in self.arch.team_iter() {
                if world_pe != self.world_pe {
                    unsafe {
                        self.dropped.put_slice(
                            world_pe,
                            self.world_pe,
                            &temp_slice[self.world_pe..=self.world_pe],
                        );
                    }
                }
            }
        }
    }

    #[tracing::instrument(skip_all)]
    fn drop_barrier(&self) {
        let mut s = Instant::now();
        for pe in self.dropped.as_slice().unwrap() {
            while *pe != 1 {
                std::thread::yield_now();
                if s.elapsed().as_secs_f64() > 5.0 {
                    println!("[{:?}] ({:?})  ", self.world_pe, self.dropped.as_slice());
                    s = Instant::now();
                }
            }
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn print_arch(&self) {
        println!("-----mapping of team pe ids to parent pe ids-----");
        let mut parent = format!("");
        let mut team = format!("");
        for i in 0..self.arch.num_pes() {
            let mut width = (i as f64).log10() as usize + 1;
            if let Ok(id) = self.arch.world_pe(i) {
                width = std::cmp::max(width, (id as f64).log10() as usize + 1);
                parent = format!("{} {:width$}", parent, id, width = width);
            }
            team = format!("{} {:width$}", team, i, width = width);
        }
        println!("  team pes: {}", team);
        println!("global pes: {}", parent);
        println!("-----mapping of parent pe ids to team pe ids-----");
        parent = format!("");
        team = format!("");
        for i in 0..self.num_world_pes {
            let mut width = (i as f64).log10() as usize + 1;
            if let Ok(id) = self.arch.team_pe(i) {
                width = std::cmp::max(width, (id as f64).log10() as usize + 1);
                team = format!("{} {:width$}", team, id, width = width);
            } else {
                team = format!("{} {:width$}", team, "", width = width);
            }
            parent = format!("{} {:width$}", parent, i, width = width);
        }
        println!("global pes: {}", parent);
        println!("  team pes: {}", team);
        println!("-------------------------------------------------");
    }
    // }

    // // #[prof]
    #[tracing::instrument(skip_all)]
    fn wait_all(&self) {
        let mut temp_now = Instant::now();
        while self.team_counters.outstanding_reqs.load(Ordering::SeqCst) > 0
            || (self.parent.is_none()
                && self.world_counters.outstanding_reqs.load(Ordering::SeqCst) > 0)
        {
            // std::thread::yield_now();
            self.scheduler.exec_task(); //mmight as well do useful work while we wait
            if temp_now.elapsed() > Duration::new(600, 0) {
                println!(
                    "in team wait_all mype: {:?} cnt: {:?} {:?}",
                    self.world_pe,
                    self.team_counters.send_req_cnt.load(Ordering::SeqCst),
                    self.team_counters.outstanding_reqs.load(Ordering::SeqCst),
                );
                temp_now = Instant::now();
            }
        }
    }

    #[tracing::instrument(skip_all)]
    pub(crate) fn barrier(&self) {
        self.barrier.barrier();
    }

    #[tracing::instrument(skip_all)]
    pub fn exec_am_all<F>(
        self: &Pin<Arc<LamellarTeamRT>>,
        am: F,
    ) -> Box<dyn LamellarMultiRequest<Output = F::Output>>
    where
        F: RemoteActiveMessage + LamellarAM + AmDist,
    {
        self.exec_am_all_tg(am, None)
    }

    #[tracing::instrument(skip_all)]
    pub(crate) fn exec_am_all_tg<F>(
        self: &Pin<Arc<LamellarTeamRT>>,
        am: F,
        task_group_cnts: Option<Arc<AMCounters>>,
    ) -> Box<dyn LamellarMultiRequest<Output = F::Output>>
    where
        F: RemoteActiveMessage + LamellarAM + AmDist,
    {
        // println!("team exec am all num_pes {:?}", self.num_pes);
        // trace!("[{:?}] team exec am all request", self.world_pe);
        // event!(Level::TRACE, "team exec am all request");
        let tg_outstanding_reqs = match task_group_cnts {
            Some(task_group_cnts) => {
                task_group_cnts.add_send_req(self.num_pes);
                Some(task_group_cnts.outstanding_reqs.clone())
            }
            None => None,
        };
        let req = Arc::new(LamellarMultiRequestHandleInner {
            cnt: AtomicUsize::new(self.num_pes),
            arch: self.arch.clone(),
            data: Mutex::new(HashMap::new()),
            team_outstanding_reqs: self.team_counters.outstanding_reqs.clone(),
            world_outstanding_reqs: self.world_counters.outstanding_reqs.clone(),
            tg_outstanding_reqs: tg_outstanding_reqs.clone(),
            user_handle: AtomicBool::new(true),
        });
        let req_result = Arc::new(LamellarRequestResult { req: req.clone() });
        let req_ptr = Arc::into_raw(req_result);
        for _ in 0..(self.num_pes - 1) {
            // -1 because of the arc we turned into raw
            unsafe { Arc::increment_strong_count(req_ptr) } //each pe will return a result (which we turn back into an arc)
        }
        // println!("strong count recv: {:?} ",Arc::strong_count(&req));
        let id = ReqId {
            id: req_ptr as usize,
            sub_id: 0,
        };

        self.world_counters.add_send_req(self.num_pes);
        self.team_counters.add_send_req(self.num_pes);
        // println!("cnts: t: {} w: {} tg: {:?}",self.team_counters.outstanding_reqs.load(Ordering::Relaxed),self.world_counters.outstanding_reqs.load(Ordering::Relaxed), tg_outstanding_reqs.as_ref().map(|x| x.load(Ordering::Relaxed)));

        // println!("team counter: {:?}", self.team_counters.outstanding_reqs);
        let func: LamellarArcAm = Arc::new(am);
        let world = if let Some(world) = &self.world {
            world.clone()
        } else {
            self.clone()
        };
        let req_data = ReqMetaData {
            src: self.world_pe,
            dst: None,
            id: id,
            lamellae: self.lamellae.clone(),
            world: world,
            team: self.clone(),
            team_addr: self.remote_ptr_addr,
        };
        // event!(Level::TRACE, "submitting request to scheduler");
        self.scheduler.submit_am(Am::All(req_data, func));
        Box::new(LamellarMultiRequestHandle {
            inner: req,
            _phantom: PhantomData,
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn exec_am_pe<F>(
        self: &Pin<Arc<LamellarTeamRT>>,
        pe: usize,
        am: F,
    ) -> Box<dyn LamellarRequest<Output = F::Output>>
    where
        F: RemoteActiveMessage + LamellarAM + AmDist,
    {
        self.exec_am_pe_tg(pe, am, None)
    }

    #[tracing::instrument(skip_all)]
    pub(crate) fn exec_am_pe_tg<F>(
        self: &Pin<Arc<LamellarTeamRT>>,
        pe: usize,
        am: F,
        task_group_cnts: Option<Arc<AMCounters>>,
    ) -> Box<dyn LamellarRequest<Output = F::Output>>
    where
        F: RemoteActiveMessage + LamellarAM + AmDist,
    {
        // println!("team exec am pe tg");
        prof_start!(pre);
        let tg_outstanding_reqs = match task_group_cnts {
            Some(task_group_cnts) => {
                task_group_cnts.add_send_req(1);
                Some(task_group_cnts.outstanding_reqs.clone())
            }
            None => None,
        };
        assert!(pe < self.arch.num_pes());

        let req = Arc::new(LamellarRequestHandleInner {
            ready: AtomicBool::new(false),
            data: Cell::new(None),
            team_outstanding_reqs: self.team_counters.outstanding_reqs.clone(),
            world_outstanding_reqs: self.world_counters.outstanding_reqs.clone(),
            tg_outstanding_reqs: tg_outstanding_reqs.clone(),
            user_handle: AtomicBool::new(true),
        });
        let req_result = Arc::new(LamellarRequestResult { req: req.clone() });
        let req_ptr = Arc::into_raw(req_result);
        // Arc::increment_strong_count(req_ptr); //we would need to do this for the exec_all command
        let id = ReqId {
            id: req_ptr as usize,
            sub_id: 0,
        };
        self.world_counters.add_send_req(1);
        self.team_counters.add_send_req(1);
        // println!("cnts: t: {} w: {} tg: {:?}",self.team_counters.outstanding_reqs.load(Ordering::Relaxed),self.world_counters.outstanding_reqs.load(Ordering::Relaxed), tg_outstanding_reqs.as_ref().map(|x| x.load(Ordering::Relaxed)));

        // println!("req_id: {:?}", id);
        let world = if let Some(world) = &self.world {
            world.clone()
        } else {
            self.clone()
        };
        let func: LamellarArcAm = Arc::new(am);
        let req_data = ReqMetaData {
            src: self.world_pe,
            dst: Some(self.arch.world_pe(pe).expect("pe not member of team")),
            id: id,
            lamellae: self.lamellae.clone(),
            world: world,
            team: self.clone(),
            team_addr: self.remote_ptr_addr,
        };
        self.scheduler.submit_am(Am::Remote(req_data, func));

        Box::new(LamellarRequestHandle {
            inner: req,
            _phantom: PhantomData,
        })
    }

    #[tracing::instrument(skip_all)]
    pub(crate) fn exec_arc_am_pe<F>(
        self: &Pin<Arc<LamellarTeamRT>>,
        pe: usize,
        am: LamellarArcAm,
        task_group_cnts: Option<Arc<AMCounters>>,
    ) -> Box<dyn LamellarRequest<Output = F>>
    where
        F: AmDist,
    {
        // println!("team exec arc am pe");
        let tg_outstanding_reqs = match task_group_cnts {
            Some(task_group_cnts) => {
                task_group_cnts.add_send_req(1);
                Some(task_group_cnts.outstanding_reqs.clone())
            }
            None => None,
        };
        assert!(pe < self.arch.num_pes());
        let req = Arc::new(LamellarRequestHandleInner {
            ready: AtomicBool::new(false),
            data: Cell::new(None),
            team_outstanding_reqs: self.team_counters.outstanding_reqs.clone(),
            world_outstanding_reqs: self.world_counters.outstanding_reqs.clone(),
            tg_outstanding_reqs: tg_outstanding_reqs.clone(),
            user_handle: AtomicBool::new(true),
        });
        let req_result = Arc::new(LamellarRequestResult { req: req.clone() });
        let req_ptr = Arc::into_raw(req_result);
        let id = ReqId {
            id: req_ptr as usize,
            sub_id: 0,
        };
        self.world_counters.add_send_req(1);
        self.team_counters.add_send_req(1);
        // println!("cnts: t: {} w: {} tg: {:?}",self.team_counters.outstanding_reqs.load(Ordering::Relaxed),self.world_counters.outstanding_reqs.load(Ordering::Relaxed), tg_outstanding_reqs.as_ref().map(|x| x.load(Ordering::Relaxed)));

        let world = if let Some(world) = &self.world {
            world.clone()
        } else {
            self.clone()
        };
        let req_data = ReqMetaData {
            src: self.world_pe,
            dst: Some(self.arch.world_pe(pe).expect("pe not member of team")),
            id: id,
            lamellae: self.lamellae.clone(),
            world: world,
            team: self.clone(),
            team_addr: self.remote_ptr_addr,
        };
        self.scheduler.submit_am(Am::Remote(req_data, am));
        prof_end!(sub);
        Box::new(LamellarRequestHandle {
            inner: req,
            _phantom: PhantomData,
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn exec_am_local<F>(
        self: &Pin<Arc<LamellarTeamRT>>,
        am: F,
    ) -> Box<dyn LamellarRequest<Output = F::Output>>
    where
        F: LamellarActiveMessage + LocalAM + 'static,
    {
        self.exec_am_local_tg(am, None)
    }

    #[tracing::instrument(skip_all)]
    pub(crate) fn exec_am_local_tg<F>(
        self: &Pin<Arc<LamellarTeamRT>>,
        am: F,
        task_group_cnts: Option<Arc<AMCounters>>,
    ) -> Box<dyn LamellarRequest<Output = F::Output>>
    where
        F: LamellarActiveMessage + LocalAM + 'static,
    {
        // println!("team exec am local");
        prof_start!(pre);
        prof_end!(pre);
        prof_start!(req);
        let tg_outstanding_reqs = match task_group_cnts {
            Some(task_group_cnts) => {
                task_group_cnts.add_send_req(1);
                Some(task_group_cnts.outstanding_reqs.clone())
            }
            None => None,
        };
        let req = Arc::new(LamellarLocalRequestHandleInner {
            ready: AtomicBool::new(false),
            data: Cell::new(None),
            team_outstanding_reqs: self.team_counters.outstanding_reqs.clone(),
            world_outstanding_reqs: self.world_counters.outstanding_reqs.clone(),
            tg_outstanding_reqs: tg_outstanding_reqs.clone(),
            user_handle: AtomicBool::new(true),
        });
        let req_result = Arc::new(LamellarRequestResult { req: req.clone() });
        let req_ptr = Arc::into_raw(req_result);
        let id = ReqId {
            id: req_ptr as usize,
            sub_id: 0,
        };
        prof_end!(req);

        prof_start!(counters);
        self.world_counters.add_send_req(1);
        self.team_counters.add_send_req(1);
        // println!("cnts: t: {} w: {} tg: {:?}",self.team_counters.outstanding_reqs.load(Ordering::Relaxed),self.world_counters.outstanding_reqs.load(Ordering::Relaxed), tg_outstanding_reqs.as_ref().map(|x| x.load(Ordering::Relaxed)));
        prof_end!(counters);
        prof_start!(any);
        let func: LamellarArcLocalAm = Arc::new(am);
        prof_end!(any);
        prof_start!(sub);
        let world = if let Some(world) = &self.world {
            world.clone()
        } else {
            self.clone()
        };
        let req_data = ReqMetaData {
            src: self.world_pe,
            dst: Some(self.world_pe),
            id: id,
            lamellae: self.lamellae.clone(),
            world: world,
            team: self.clone(),
            team_addr: self.remote_ptr_addr,
        };
        self.scheduler.submit_am(Am::Local(req_data, func));
        prof_end!(sub);
        Box::new(LamellarLocalRequestHandle {
            inner: req,
            _phantom: PhantomData,
        })
    }
    /// allocate a shared memory region from the asymmetric heap
    ///
    /// # Arguments
    ///
    /// * `size` - number of elements of T to allocate a memory region for -- (not size in bytes)
    ///
    // pub(crate) fn alloc_shared_mem_region<T: AmDist+ 'static>(self:   &Pin<Arc<LamellarTeamRT>>, size: usize) -> SharedMemoryRegion<T> {
    //     self.barrier.barrier();
    //     let mr: SharedMemoryRegion<T> = if self.num_world_pes == self.num_pes {
    //         SharedMemoryRegion::new(size, self.clone(), AllocationType::Global)
    //     } else {
    //         SharedMemoryRegion::new(
    //             size,
    //             self.clone(),
    //             AllocationType::Sub(self.arch.team_iter().collect::<Vec<usize>>()),
    //         )
    //     };
    //     self.barrier.barrier();
    //     mr
    // }

    /// allocate a local memory region from the asymmetric heap
    ///
    /// # Arguments
    ///
    /// * `size` - number of elements of T to allocate a memory region for -- (not size in bytes)
    ///
    #[tracing::instrument(skip_all)]
    pub fn alloc_one_sided_mem_region<T: Dist>(
        self: &Pin<Arc<LamellarTeamRT>>,
        size: usize,
    ) -> OneSidedMemoryRegion<T> {
        // let lmr: OneSidedMemoryRegion<T> =
        //     OneSidedMemoryRegion::new(size, self, self.lamellae.clone()).into();
        // lmr
        let mut lmr = OneSidedMemoryRegion::try_new(size, self, self.lamellae.clone());
        while let Err(_err) = lmr {
            std::thread::yield_now();
            self.lamellae.alloc_pool(size * std::mem::size_of::<T>());
            lmr = OneSidedMemoryRegion::try_new(size, self, self.lamellae.clone());
        }
        lmr.expect("out of memory")
    }
}

//#[prof]
impl Drop for LamellarTeamRT {
    #[tracing::instrument(skip_all)]
    fn drop(&mut self) {
        // println!("sechduler_new: {:?}", Arc::strong_count(&self.scheduler));
        // println!("lamellae: {:?}", Arc::strong_count(&self.lamellae));
        // println!("arch: {:?}", Arc::strong_count(&self.arch));
        // println!(
        //     "world_counters: {:?}",
        //     Arc::strong_count(&self.world_counters)
        // );
        // println!("removing {:?} ", self.team_hash);
        // self.teams.write().remove(&(self.remote_ptr_addr as u64));
        self.lamellae.free(self.remote_ptr_addr);
        // println!("LamellarTeamRT dropped {:?}", self.team_hash);
    }
}

//#[prof]
impl Drop for LamellarTeam {
    #[tracing::instrument(skip_all)]
    fn drop(&mut self) {
        // println!("team handle dropping {:?}", self.team.team_hash);
        if !self.am_team {
            //we only care about when the user handle gets dropped (not the team handles that are created for use in an active message)
            if let Some(parent) = &self.team.parent {
                // println!("not world?");
                // println!(
                //     "[{:?}] {:?} team handle dropping {:?} {:?}",
                //     self.team.world_pe,
                //     self.team.team_hash,
                //     self.team.get_pes(),
                //     self.team.dropped.as_slice()
                // );
                self.team.wait_all();
                // println!("after wait all");
                self.team.barrier();
                // println!("after barrier");
                self.team.put_dropped();
                // println!("after put dropped");
                // if let Ok(_my_index) = self.team.arch.team_pe(self.team.world_pe) {
                if self.team.team_pe.is_ok() {
                    self.team.drop_barrier();
                }
                // println!("after drop barrier");
                // println!("removing {:?} ",self.team.id);
                parent.sub_teams.write().remove(&self.team.id);
            }

            // else {
            // println!("world team");
            // println!(
            //     "sechduler_new: {:?}",
            //     Arc::strong_count(&self.team.scheduler)
            // );
            // println!("lamellae: {:?}", Arc::strong_count(&self.team.lamellae));
            // println!("arch: {:?}", Arc::strong_count(&self.team.arch));
            // println!(
            //     "world_counters: {:?}",
            //     Arc::strong_count(&self.team.world_counters)
            // );
            // println!("removing {:?} ", self.team.team_hash);
            let team_ptr = self.team.remote_ptr_addr as *mut *const LamellarTeamRT;
            unsafe {
                let arc_team = Arc::from_raw(*team_ptr);
                // println!("arc_team: {:?}", Arc::strong_count(&arc_team));
                Pin::new_unchecked(arc_team); //allows us to drop the inner team
            }
        }
        // else{
        //     println!("in world team!!!");
        //     self.alloc_barrier=None;
        //     self.alloc_mutex=None;
        //     self.team.wait_all();
        //     self.team.barrier();
        //     self.team.destroy();
        // }
        // println!("how am i here...");

        // println!("team handle dropped");

        // if let Some(world) = &self.world{
        //     println!("world: {:?}", Arc::strong_count(world));
        // }
        // println!("team: {:?}",Arc::strong_count(&self.team));
        // for (k,tes"team handle dropped");
        // println!(
        //     "[{:?}] {:?} team handle dropped {:?} {:?}",
        //     self.team.world_pe,
        //     self.team.team_hash,
        //     self.team.get_pes(),
        //     self.team.dropped.as_slice()
        // );
        // std::thread::sleep(Duration::from_secs(1));
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::lamellae::{create_lamellae, Backend, Lamellae, LamellaeAM};
//     use crate::lamellar_arch::StridedArch;
//     use crate::schedulers::{create_scheduler, Scheduler, SchedulerType};
//     use std::collections::BTreeMap;
//     #[test]
//     fn multi_team_unique_hash() {
//         let num_pes = 10;
//         let world_pe = 0;
//         let mut lamellae = create_lamellae(Backend::Local);
//         let teams = Arc::new(RwLock::new(HashMap::new()));

//         let mut sched = create_scheduler(
//             SchedulerType::WorkStealing,
//             num_pes,
//             world_pe,
//             teams.clone(),
//         );
//         lamellae.init_lamellae(sched.get_queue().clone());
//         let lamellae = Arc::new(lamellae);
//         let mut lamellaes: BTreeMap<Backend, Arc<dyn LamellaeAM>> = BTreeMap::new();
//         lamellaes.insert(lamellae.backend(), lamellae.get_am());
//         sched.init(num_pes, world_pe, lamellaes);
//         let counters = Arc::new(AMCounters::new());
//         let root_team = Arc::new(LamellarTeamRT::new(
//             num_pes,
//             world_pe,
//             sched.get_queue().clone(),
//             counters.clone(),
//             lamellae.clone(),
//         ));
//         let child_arch1 = StridedArch::new(0, 1, num_pes);
//         let team1 =
//             LamellarTeamRT::create_subteam_from_arch(root_team.clone(), child_arch1.clone())
//                 .unwrap();
//         let team2 =
//             LamellarTeamRT::create_subteam_from_arch(root_team.clone(), child_arch1.clone())
//                 .unwrap();
//         let team1_1 =
//             LamellarTeamRT::create_subteam_from_arch(team1.clone(), child_arch1.clone()).unwrap();
//         let team1_2 =
//             LamellarTeamRT::create_subteam_from_arch(team1.clone(), child_arch1.clone()).unwrap();
//         let team2_1 =
//             LamellarTeamRT::create_subteam_from_arch(team2.clone(), child_arch1.clone()).unwrap();
//         let team2_2 =
//             LamellarTeamRT::create_subteam_from_arch(team2.clone(), child_arch1.clone()).unwrap();

//         println!("{:?}", root_team.team_hash);
//         println!("{:?} -- {:?}", team1.team_hash, team2.team_hash);
//         println!(
//             "{:?} {:?} -- {:?} {:?}",
//             team1_1.team_hash, team1_2.team_hash, team2_1.team_hash, team2_2.team_hash
//         );

//         assert_ne!(root_team.team_hash, team1.team_hash);
//         assert_ne!(root_team.team_hash, team2.team_hash);
//         assert_ne!(team1.team_hash, team2.team_hash);
//         assert_ne!(team1.team_hash, team1_1.team_hash);
//         assert_ne!(team1.team_hash, team1_2.team_hash);
//         assert_ne!(team2.team_hash, team2_1.team_hash);
//         assert_ne!(team2.team_hash, team2_2.team_hash);
//         assert_ne!(team2.team_hash, team1_1.team_hash);
//         assert_ne!(team2.team_hash, team1_2.team_hash);
//         assert_ne!(team1.team_hash, team2_1.team_hash);
//         assert_ne!(team1.team_hash, team2_2.team_hash);

//         assert_ne!(team1_1.team_hash, team1_2.team_hash);
//         assert_ne!(team1_1.team_hash, team2_1.team_hash);
//         assert_ne!(team1_1.team_hash, team2_2.team_hash);

//         assert_ne!(team1_2.team_hash, team2_1.team_hash);
//         assert_ne!(team1_2.team_hash, team2_2.team_hash);

//         assert_ne!(team2_1.team_hash, team2_2.team_hash);
//     }
//     // #[test]
//     // fn asymetric_teams(){

//     // }

//     #[test]
//     fn multi_team_unique_hash2() {
//         let num_pes = 10;
//         let world_pe = 0;
//         let mut lamellae = create_lamellae(Backend::Local);
//         let teams = Arc::new(RwLock::new(HashMap::new()));

//         let mut sched = create_scheduler(
//             SchedulerType::WorkStealing,
//             num_pes,
//             world_pe,
//             teams.clone(),
//         );
//         lamellae.init_lamellae(sched.get_queue().clone());
//         let lamellae = Arc::new(lamellae);
//         let mut lamellaes: BTreeMap<Backend, Arc<dyn LamellaeAM>> = BTreeMap::new();
//         lamellaes.insert(lamellae.backend(), lamellae.get_am());
//         sched.init(num_pes, world_pe, lamellaes);
//         let counters = Arc::new(AMCounters::new());

//         let mut root_teams = Vec::new();
//         for pe in 0..num_pes {
//             root_teams.push(Arc::new(LamellarTeamRT::new(
//                 num_pes,
//                 pe,
//                 sched.get_queue().clone(),
//                 counters.clone(),
//                 lamellae.clone(),
//             )));
//         }
//         let root_hash = root_teams[0].team_hash;
//         for root_team in &root_teams {
//             assert_eq!(root_hash, root_team.team_hash);
//         }

//         let odds_arch = StridedArch::new(1, 2, num_pes / 2);
//         let odd_childs: Vec<_> = root_teams
//             .iter()
//             .map(|team| {
//                 LamellarTeamRT::create_subteam_from_arch(team.clone(), odds_arch.clone()).unwrap()
//             })
//             .collect();

//         let evens_arch = StridedArch::new(0, 2, num_pes / 2);
//         let even_childs: Vec<_> = root_teams
//             .iter()
//             .step_by(2)
//             .map(|team| {
//                 LamellarTeamRT::create_subteam_from_arch(team.clone(), evens_arch.clone()).unwrap()
//             })
//             .collect();

//         let hash = odd_childs[0].team_hash;
//         for child in odd_childs {
//             assert_eq!(hash, child.team_hash);
//         }

//         let hash = even_childs[0].team_hash;
//         for child in even_childs {
//             assert_eq!(hash, child.team_hash);
//         }

//         // let odds_odds_arch = StridedArch::new(1,2,num_pes);

//         // println!("{:?}", root_team.team_hash);
//         // println!("{:?} -- {:?}", team1.team_hash, team2.team_hash);
//         // println!(
//         //     "{:?} {:?} -- {:?} {:?}",
//         //     team1_1.team_hash, team1_2.team_hash, team2_1.team_hash, team2_2.team_hash
//         // );

//         // assert_ne!(root_team.team_hash, team1.team_hash);
//         // assert_ne!(root_team.team_hash, team2.team_hash);
//         // assert_ne!(team1.team_hash, team2.team_hash);
//         // assert_ne!(team1.team_hash, team1_1.team_hash);
//         // assert_ne!(team1.team_hash, team1_2.team_hash);
//         // assert_ne!(team2.team_hash, team2_1.team_hash);
//         // assert_ne!(team2.team_hash, team2_2.team_hash);
//         // assert_ne!(team2.team_hash, team1_1.team_hash);
//         // assert_ne!(team2.team_hash, team1_2.team_hash);
//         // assert_ne!(team1.team_hash, team2_1.team_hash);
//         // assert_ne!(team1.team_hash, team2_2.team_hash);

//         // assert_ne!(team1_1.team_hash, team1_2.team_hash);
//         // assert_ne!(team1_1.team_hash, team2_1.team_hash);
//         // assert_ne!(team1_1.team_hash, team2_2.team_hash);

//         // assert_ne!(team1_2.team_hash, team2_1.team_hash);
//         // assert_ne!(team1_2.team_hash, team2_2.team_hash);

//         // assert_ne!(team2_1.team_hash, team2_2.team_hash);
//     }
// }

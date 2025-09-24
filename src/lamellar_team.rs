use crate::{
    active_messaging::{handle::AmHandleInner, *},
    barrier::{Barrier, BarrierHandle},
    env_var::config,
    lamellae::{
        AllocationType, CommAlloc, CommAllocInner, CommAllocType, CommInfo, CommMem, CommProgress,
        Lamellae, LamellaeShutdown, Remote,
    },
    lamellar_arch::{GlobalArch, IdError, LamellarArch, LamellarArchEnum, LamellarArchRT},
    lamellar_env::LamellarEnv,
    lamellar_request::*,
    lamellar_world::LamellarWorld,
    memregion::{
        handle::{FallibleSharedMemoryRegionHandle, SharedMemoryRegionHandle},
        one_sided::OneSidedMemoryRegion,
        shared::SharedMemoryRegion,
        Dist, LamellarMemoryRegion, MemoryRegion, RemoteMemoryRegion,
    },
    scheduler::{LamellarTask, ReqId, Scheduler},
    warnings::RuntimeWarning,
};

// #[cfg(feature = "nightly")]
// use crate::utils::ser_closure;

// use log::trace;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
// use std::any;
use core::pin::Pin;
use futures_util::future::join_all;
use futures_util::Future;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::marker::PhantomPinned;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc; //, Weak};
use std::time::{Duration, Instant};
use tracing::trace;

use std::cell::Cell;
use std::marker::PhantomData;

//use tracing::*;

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
/// use lamellar::active_messaging::prelude::*;
/// use lamellar::array::prelude::*;
///
/// #[AmData(Debug,Clone)]
/// struct MyAm{
///     world_pe: usize,
///     team_pe: Option<usize>,
/// }
///
/// #[lamellar::am]
/// impl LamellarAm for MyAm{
///     async fn exec(self) {
///         println!("Hello from world PE{:?}, team PE{:?}",self.world_pe, self.team_pe);
///     }
/// }
///
/// let world = LamellarWorldBuilder::new().build();
/// let num_pes = world.num_pes();
/// let world_pe = world.my_pe();
///
/// //create a team consisting of the "even" PEs in the world
/// let even_pes = world.create_team_from_arch(StridedArch::new(
///    0,                                      // start pe
///    2,                                      // stride
///    (num_pes as f64 / 2.0).ceil() as usize, //num_pes in team
/// )).expect("PE in world team");
/// let team_pe = match even_pes.team_pe_id(){
///     Ok(pe) => Some(pe),
///     Err(_) => None,
/// };
/// // we can launch and await the results of active messages on a given team
/// let req = even_pes.exec_am_all(MyAm{world_pe,team_pe});
/// let result = req.block();
/// // we can also create a distributed array so that its data only resides on the members of the team.
/// let array: AtomicArray<usize> = AtomicArray::new(&even_pes, 100,Distribution::Block).block();
/// ```
pub struct LamellarTeam {
    pub(crate) world: Option<Arc<LamellarTeam>>,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) am_team: bool,
    pub(crate) panic: Arc<AtomicU8>,
}

impl LamellarTeam {
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new(
        world: Option<Arc<LamellarTeam>>,
        team: Pin<Arc<LamellarTeamRT>>,
        // teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
        am_team: bool,
    ) -> Arc<LamellarTeam> {
        // unsafe{
        //     let pinned_team = Pin::into_inner_unchecked(team.clone()).clone();
        //     let team_ptr = Arc::into_raw(pinned_team);
        //     println!{"new lam team: {:?} {:?} {:?} {:?}",&team_ptr,team_ptr, (team.remote_ptr_alloc as *mut (*const LamellarTeamRT)).as_ref(), (*(team.remote_ptr_alloc as *mut (*const LamellarTeamRT))).as_ref()};

        // }
        // team.print_cnt();
        let panic = team.panic.clone();
        let the_team = Arc::new(LamellarTeam {
            world,
            team,
            // teams,
            am_team,
            panic,
        });
        // the_team.print_cnt();
        // unsafe{
        //     let pinned_team = Pin::into_inner_unchecked(the_team.team.clone()).clone();
        //     let team_ptr = Arc::into_raw(pinned_team);
        //     println!{"new lam team: {:?} {:?} {:?} {:?}",&team_ptr,team_ptr, (the_team.team.remote_ptr_alloc as *mut (*const LamellarTeamRT)).as_ref(), (*(the_team.team.remote_ptr_alloc as *mut (*const LamellarTeamRT))).as_ref()};

        // }
        the_team
    }

    // pub fn print_cnt(&self) {
    //     self.team.print_cnt();
    // }

    #[doc(alias("One-sided", "onesided"))]
    /// Return a list of (world-based) pe ids representing the members of the team
    ///
    /// # One-sided Operation
    /// The result is returned only on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::active_messaging::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    ///
    /// //create a team consisting of the "even" PEs in the world
    /// let even_pes = world.create_team_from_arch(StridedArch::new(
    ///    0,                                      // start pe
    ///    2,                                      // stride
    ///    (num_pes as f64 / 2.0).ceil() as usize, //num_pes in team
    /// )).expect("PE in world team");
    /// let pes = even_pes.get_pes();
    ///
    /// for (a,b) in (0..num_pes).step_by(2).zip(pes.iter()){
    ///     assert_eq!(a,*b);
    /// }
    ///```
    #[allow(dead_code)]
    #[tracing::instrument(skip_all, level = "debug")]
    pub fn get_pes(&self) -> Vec<usize> {
        assert!(self.panic.load(Ordering::SeqCst) == 0);

        self.team.arch.team_iter().collect::<Vec<usize>>()
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return number of pes in team
    ///
    /// # One-sided Operation
    /// The result is returned only on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::active_messaging::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    ///
    /// //create a team consisting of the "even" PEs in the world
    /// let even_pes = world.create_team_from_arch(StridedArch::new(
    ///    0,                                      // start pe
    ///    2,                                      // stride
    ///    (num_pes as f64 / 2.0).ceil() as usize, //num_pes in team
    /// )).expect("PE in world team");
    /// let pes = even_pes.get_pes();
    ///
    /// assert_eq!((num_pes as f64 / 2.0).ceil() as usize,even_pes.num_pes());
    ///```
    #[tracing::instrument(skip_all, level = "debug")]
    pub fn num_pes(&self) -> usize {
        assert!(self.panic.load(Ordering::SeqCst) == 0);

        self.team.arch.num_pes()
    }

    // #[doc(alias("One-sided", "onesided"))]
    /// Returns nummber of threads on this PE (including the main thread)
    ///
    /// # One-sided Operation
    /// The result is returned only on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::active_messaging::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    ///
    /// //create a team consisting of the "even" PEs in the world
    /// let even_pes = world.create_team_from_arch(StridedArch::new(
    ///    0,                                      // start pe
    ///    2,                                      // stride
    ///    (num_pes as f64 / 2.0).ceil() as usize, //num_pes in team
    /// )).expect("PE in world team");
    /// let pes = even_pes.get_pes();
    ///
    /// assert_eq!((num_pes as f64 / 2.0).ceil() as usize,even_pes.num_pes());
    ///```
    #[tracing::instrument(skip_all, level = "debug")]
    pub fn num_threads_per_pe(&self) -> usize {
        self.team.scheduler.num_workers()
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the world-based id of this pe
    ///
    /// # One-sided Operation
    /// The result is returned only on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::active_messaging::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let my_pe = world.my_pe();
    ///
    /// //create a team consisting of the "even" PEs in the world
    /// let even_pes = world.create_team_from_arch(StridedArch::new(
    ///    0,                                      // start pe
    ///    2,                                      // stride
    ///    (num_pes as f64 / 2.0).ceil() as usize, //num_pes in team
    /// )).expect("PE in world team");
    /// let pes = even_pes.get_pes();
    ///
    /// assert_eq!(my_pe,even_pes.world_pe_id());
    ///```
    #[tracing::instrument(skip_all, level = "debug")]
    pub fn world_pe_id(&self) -> usize {
        assert!(self.panic.load(Ordering::SeqCst) == 0);

        self.team.world_pe
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Return the team-based id of this pe
    ///
    /// # One-sided Operation
    /// The result is returned only on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::active_messaging::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    /// let my_pe = world.my_pe();
    ///
    /// //create a team consisting of the "even" PEs in the world
    /// let even_pes = world.create_team_from_arch(StridedArch::new(
    ///    0,                                      // start pe
    ///    2,                                      // stride
    ///    (num_pes as f64 / 2.0).ceil() as usize, //num_pes in team
    /// )).expect("PE in world team");
    /// let pes = even_pes.get_pes();
    ///
    /// if let Ok(team_pe) = even_pes.team_pe_id(){
    ///    assert!(my_pe %2 == 0);
    /// }
    ///```
    #[tracing::instrument(skip_all, level = "debug")]
    pub fn team_pe_id(&self) -> Result<usize, IdError> {
        assert!(self.panic.load(Ordering::SeqCst) == 0);

        self.team.arch.team_pe(self.team.world_pe)
    }

    #[doc(alias = "Collective")]
    /// create a subteam containing any number of pe's from this team using the provided LamellarArch (layout)
    ///
    /// # Collective Operation
    /// Requrires all PEs present within `parent` to enter the call otherwise deadlock will occur.
    /// Note that this *does* include the PEs that will not exist within the new subteam.
    ///
    /// # Examples
    ///```
    /// use lamellar::active_messaging::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    ///
    /// //create a team consisting of the "even" PEs in the world
    /// let even_pes = world.create_team_from_arch(StridedArch::new(
    ///    0,                                      // start pe
    ///    2,                                      // stride
    ///    (num_pes as f64 / 2.0).ceil() as usize, //num_pes in team
    /// )).expect("PE in world team");
    ///```
    #[tracing::instrument(skip_all, level = "debug")]
    pub fn create_subteam_from_arch<L>(
        parent: Arc<LamellarTeam>,
        arch: L,
    ) -> Option<Arc<LamellarTeam>>
    where
        L: LamellarArch + std::hash::Hash + 'static,
    {
        assert!(parent.panic.load(Ordering::SeqCst) == 0);
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

    #[doc(alias("One-sided", "onesided"))]
    /// Text based representation of the team
    ///
    /// # One-sided Operation
    /// the team architecture will only be printed on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::active_messaging::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    ///
    /// //create a team consisting of the "even" PEs in the world
    /// let even_pes = world.create_team_from_arch(StridedArch::new(
    ///    0,                                      // start pe
    ///    2,                                      // stride
    ///    (num_pes as f64 / 2.0).ceil() as usize, //num_pes in team
    /// )).expect("PE in world team");
    /// even_pes.print_arch();
    ///```
    #[tracing::instrument(skip_all, level = "debug")]
    pub fn print_arch(&self) {
        assert!(self.panic.load(Ordering::SeqCst) == 0);

        self.team.print_arch()
    }

    #[doc(alias = "Collective")]
    /// team wide synchronization method which blocks calling thread until all PEs in the team have entered
    /// Generally this is intended to be called from the main thread, if a barrier is needed within an active message or async context please see [async_barrier](Self::async_barrier)
    ///
    /// # Collective Operation
    /// Requrires all PEs present within the team to enter the barrier otherwise deadlock will occur.
    ///
    /// # Examples
    ///```
    /// use lamellar::active_messaging::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    ///
    /// //create a team consisting of the "even" PEs in the world
    /// let even_pes = world.create_team_from_arch(StridedArch::new(
    ///    0,                                      // start pe
    ///    2,                                      // stride
    ///    (num_pes as f64 / 2.0).ceil() as usize, //num_pes in team
    /// )).expect("PE in world team");
    /// //do some work
    /// even_pes.barrier(); //block until all PEs have entered the barrier
    ///```
    #[tracing::instrument(skip_all, level = "debug")]
    pub fn barrier(&self) {
        assert!(self.panic.load(Ordering::SeqCst) == 0);

        self.team.barrier()
    }

    #[doc(alias = "Collective")]
    /// EXPERIMENTAL: team wide synchronization method which blocks the calling task until all PEs in team have entered.
    /// This function allows for calling barrier in an async context without blocking the worker thread.
    /// Care should be taken when using this function to avoid deadlocks,as it is easy to mismatch barrier calls accross threads and PEs.
    ///
    /// # Collective Operation
    /// Requrires all PEs present within the team to enter the barrier otherwise deadlock will occur.
    ///
    /// # Examples
    ///```
    /// use lamellar::active_messaging::prelude::*;
    ///
    /// let world = lamellar::LamellarWorldBuilder::new().build();
    /// //do some work
    /// world.barrier(); //block until all PEs have entered the barrier
    ///```
    #[tracing::instrument(skip_all, level = "debug")]
    pub fn async_barrier(&self) -> BarrierHandle {
        assert!(self.panic.load(Ordering::SeqCst) == 0);

        self.team.async_barrier()
    }

    //used by proc macro
    #[doc(hidden)]
    #[tracing::instrument(skip_all, level = "debug")]
    pub fn exec_am_group_pe<F, O>(&self, pe: usize, am: F) -> AmHandle<O>
    where
        F: RemoteActiveMessage + LamellarAM + crate::Serialize + 'static,
        O: AmDist + 'static,
    {
        self.team.am_group_exec_am_pe_tg(pe, am, None)
    }

    //used by proc macro
    #[doc(hidden)]
    #[tracing::instrument(skip_all, level = "debug")]
    pub fn exec_am_group_all<F, O>(&self, am: F) -> MultiAmHandle<O>
    where
        F: RemoteActiveMessage + LamellarAM + crate::Serialize + 'static,
        O: AmDist + 'static,
    {
        self.team.am_group_exec_am_all_tg(am, None)
    }

    pub fn exec_am_local_thread<F>(&self, am: F, thread: usize) -> LocalAmHandle<F::Output>
    where
        F: LamellarActiveMessage + LocalAM + 'static,
    {
        self.team.exec_am_local_tg(am, None, Some(thread))
    }
}

impl LamellarEnv for Arc<LamellarTeam> {
    fn my_pe(&self) -> usize {
        self.team
            .arch
            .team_pe(self.team.world_pe)
            .expect("PE is apart of team")
    }
    fn num_pes(&self) -> usize {
        self.team.num_pes()
    }
    fn num_threads_per_pe(&self) -> usize {
        self.team.num_threads()
    }
    fn world(&self) -> Arc<LamellarTeam> {
        // println!("LamellarTeam world");
        if let Some(world) = &self.world {
            world.clone()
        } else {
            self.clone()
        }
    }
    fn team(&self) -> Arc<LamellarTeam> {
        // println!("LamellarTeam team");
        self.clone()
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

impl ActiveMessaging for Arc<LamellarTeam> {
    type SinglePeAmHandle<R: AmDist> = AmHandle<R>;
    type MultiAmHandle<R: AmDist> = MultiAmHandle<R>;
    type LocalAmHandle<L> = LocalAmHandle<L>;
    #[tracing::instrument(skip_all, level = "debug")]
    fn exec_am_all<F>(&self, am: F) -> Self::MultiAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        assert!(self.panic.load(Ordering::SeqCst) == 0);

        // trace!("[{:?}] team exec am all request", self.team.world_pe);
        self.team.exec_am_all_tg(am, None)
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn exec_am_pe<F>(&self, pe: usize, am: F) -> AmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        assert!(self.panic.load(Ordering::SeqCst) == 0);

        self.team.exec_am_pe_tg(pe, am, None)
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn exec_am_local<F>(&self, am: F) -> LocalAmHandle<F::Output>
    where
        F: LamellarActiveMessage + LocalAM + 'static,
    {
        assert!(self.panic.load(Ordering::SeqCst) == 0);

        self.team.exec_am_local_tg(am, None, None)
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn wait_all(&self) {
        assert!(self.panic.load(Ordering::SeqCst) == 0);

        self.team.wait_all();
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn await_all(&self) -> impl Future<Output = ()> + Send {
        assert!(self.panic.load(Ordering::SeqCst) == 0);

        self.team.await_all()
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn barrier(&self) {
        assert!(self.panic.load(Ordering::SeqCst) == 0);

        self.team.barrier();
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn async_barrier(&self) -> BarrierHandle {
        assert!(self.panic.load(Ordering::SeqCst) == 0);

        self.team.async_barrier()
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn spawn<F>(&self, task: F) -> LamellarTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        assert!(self.panic.load(Ordering::SeqCst) == 0);
        self.team.scheduler.spawn_task(
            task,
            vec![
                self.team.world_counters.clone(),
                self.team.team_counters.clone(),
            ],
        )
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn block_on<F: Future>(&self, f: F) -> F::Output {
        assert!(self.panic.load(Ordering::SeqCst) == 0);

        // trace_span!("block_on").in_scope(||
        self.team.scheduler.block_on(f)
        // )
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn block_on_all<I>(&self, iter: I) -> Vec<<<I as IntoIterator>::Item as Future>::Output>
    where
        I: IntoIterator,
        <I as IntoIterator>::Item: Future + Send + 'static,
        <<I as IntoIterator>::Item as Future>::Output: Send,
    {
        assert!(self.panic.load(Ordering::SeqCst) == 0);
        self.team
            .scheduler
            .block_on(join_all(iter.into_iter().map(|task| {
                self.team.scheduler.spawn_task(
                    task,
                    vec![
                        self.team.world_counters.clone(),
                        self.team.team_counters.clone(),
                    ],
                )
            })))
    }
}

impl RemoteMemoryRegion for Arc<LamellarTeam> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn try_alloc_shared_mem_region<T: Remote>(
        &self,
        size: usize,
    ) -> FallibleSharedMemoryRegionHandle<T> {
        assert!(self.panic.load(Ordering::SeqCst) == 0);

        // self.team.barrier.barrier();
        let mr = if self.team.num_world_pes == self.team.num_pes {
            SharedMemoryRegion::try_new(size, self.team.clone(), AllocationType::Global)
        } else {
            SharedMemoryRegion::try_new(
                size,
                self.team.clone(),
                AllocationType::Sub(self.team.arch.team_iter().collect::<Vec<usize>>()),
            )
        };
        // self.team.barrier.barrier();
        mr
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn alloc_shared_mem_region<T: Remote>(&self, size: usize) -> SharedMemoryRegionHandle<T> {
        assert!(self.panic.load(Ordering::SeqCst) == 0);

        // self.team.barrier.barrier();
        let mr = if self.team.num_world_pes == self.team.num_pes {
            SharedMemoryRegion::new(size, self.team.clone(), AllocationType::Global)
        } else {
            SharedMemoryRegion::new(
                size,
                self.team.clone(),
                AllocationType::Sub(self.team.arch.team_iter().collect::<Vec<usize>>()),
            )
        };
        // self.team.barrier.barrier();
        mr
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn try_alloc_one_sided_mem_region<T: Remote>(
        &self,
        size: usize,
    ) -> Result<OneSidedMemoryRegion<T>, anyhow::Error> {
        assert!(self.panic.load(Ordering::SeqCst) == 0);

        OneSidedMemoryRegion::try_new(size, &self.team)
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn alloc_one_sided_mem_region<T: Remote>(&self, size: usize) -> OneSidedMemoryRegion<T> {
        assert!(self.panic.load(Ordering::SeqCst) == 0);

        let mut lmr = OneSidedMemoryRegion::try_new(size, &self.team);
        while let Err(_err) = lmr {
            std::thread::yield_now();
            // println!(
            //     "out of Lamellar mem trying to alloc new pool {:?} {:?}",
            //     size,
            //     std::mem::size_of::<T>()
            // );
            self.team
                .lamellae
                .comm()
                .alloc_pool(size * std::mem::size_of::<T>());
            lmr = OneSidedMemoryRegion::try_new(size, &self.team);
        }
        lmr.expect("out of memory")
    }
}

/// wrapper struct around an internal runtime handle to a lamellar team
#[derive(Debug)]
pub struct IntoLamellarTeam {
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
}

impl From<Pin<Arc<LamellarTeamRT>>> for IntoLamellarTeam {
    #[tracing::instrument(skip_all, level = "debug")]
    fn from(team: Pin<Arc<LamellarTeamRT>>) -> Self {
        IntoLamellarTeam { team: team.clone() }
    }
}
impl From<&Pin<Arc<LamellarTeamRT>>> for IntoLamellarTeam {
    #[tracing::instrument(skip_all, level = "debug")]
    fn from(team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        IntoLamellarTeam { team: team.clone() }
    }
}

impl From<Arc<LamellarTeam>> for IntoLamellarTeam {
    #[tracing::instrument(skip_all, level = "debug")]
    fn from(team: Arc<LamellarTeam>) -> Self {
        IntoLamellarTeam {
            team: team.team.clone(),
        }
    }
}

impl From<&Arc<LamellarTeam>> for IntoLamellarTeam {
    #[tracing::instrument(skip_all, level = "debug")]
    fn from(team: &Arc<LamellarTeam>) -> Self {
        IntoLamellarTeam {
            team: team.team.clone(),
        }
    }
}

impl From<&LamellarWorld> for IntoLamellarTeam {
    #[tracing::instrument(skip_all, level = "debug")]
    fn from(world: &LamellarWorld) -> Self {
        IntoLamellarTeam {
            team: world.team_rt.clone(),
        }
    }
}

impl From<LamellarWorld> for IntoLamellarTeam {
    #[tracing::instrument(skip_all, level = "debug")]
    fn from(world: LamellarWorld) -> Self {
        IntoLamellarTeam {
            team: world.team_rt.clone(),
        }
    }
}

/// Intenal Runtime handle to a lamellar team
/// users generally don't need to use this
#[doc(hidden)]
pub struct ArcLamellarTeam {
    pub team: Arc<LamellarTeam>,
}

impl From<Arc<LamellarTeam>> for ArcLamellarTeam {
    #[tracing::instrument(skip_all, level = "debug")]
    fn from(team: Arc<LamellarTeam>) -> Self {
        ArcLamellarTeam { team }
    }
}

impl From<&Arc<LamellarTeam>> for ArcLamellarTeam {
    #[tracing::instrument(skip_all, level = "debug")]
    fn from(team: &Arc<LamellarTeam>) -> Self {
        ArcLamellarTeam { team: team.clone() }
    }
}

impl From<&LamellarWorld> for ArcLamellarTeam {
    #[tracing::instrument(skip_all, level = "debug")]
    fn from(world: &LamellarWorld) -> Self {
        ArcLamellarTeam { team: world.team() }
    }
}

impl From<LamellarWorld> for ArcLamellarTeam {
    #[tracing::instrument(skip_all, level = "debug")]
    fn from(world: LamellarWorld) -> Self {
        ArcLamellarTeam { team: world.team() }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub(crate) struct LamellarTeamRemotePtr {
    pub(crate) addr: usize,
    pub(crate) pe: usize,
    pub(crate) backend: crate::Backend,
}

impl From<LamellarTeamRemotePtr> for Pin<Arc<LamellarTeamRT>> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn from(remote_ptr: LamellarTeamRemotePtr) -> Self {
        let lamellae = if let Some(lamellae) = crate::LAMELLAES.read().get(&remote_ptr.backend) {
            lamellae.clone()
        } else {
            panic!("unexepected lamellae backend {:?}", &remote_ptr.backend);
        };
        let local_team_addr = lamellae.comm().local_addr(remote_ptr.pe, remote_ptr.addr);

        unsafe {
            let team_ptr = *local_team_addr.as_ptr::<*const LamellarTeamRT>();
            trace!("team_ptr from local_team_addr {:?}", team_ptr);
            Arc::increment_strong_count(team_ptr);
            Pin::new_unchecked(Arc::from_raw(team_ptr))
        }
    }
}

impl From<Pin<Arc<LamellarTeamRT>>> for LamellarTeamRemotePtr {
    #[tracing::instrument(skip_all, level = "debug")]
    fn from(team: Pin<Arc<LamellarTeamRT>>) -> Self {
        LamellarTeamRemotePtr {
            addr: team.remote_ptr_alloc.comm_addr().into(),
            pe: team.world_pe,
            backend: team.lamellae.comm().backend(),
        }
    }
}

/// Internal Runtime handle to a lamellar team
/// this is typically used by proc macros (hence why it is public)
/// end users should never use this directly and should instead use the [LamellarTeam] and/or [LamellarWorld] struct
pub(crate) struct LamellarTeamRT {
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
    pub(crate) team_counters: Arc<AMCounters>,
    pub(crate) world_counters: Arc<AMCounters>, // can probably remove this?
    pub(crate) id: usize,
    sub_team_id_cnt: AtomicUsize,
    pub(crate) barrier: Barrier,
    dropped: MemoryRegion<usize>,
    pub(crate) remote_ptr_alloc: CommAlloc,
    pub(crate) team_hash: u64,
    pub(crate) panic: Arc<AtomicU8>,
    pub(crate) tid: std::thread::ThreadId,
    // pub(crate) main_panic: Arc<AtomicBool>,
    _pin: std::marker::PhantomPinned,
}

impl LamellarEnv for Pin<Arc<LamellarTeamRT>> {
    fn my_pe(&self) -> usize {
        self.arch
            .team_pe(self.world_pe)
            .expect("PE is apart of team")
    }
    fn num_pes(&self) -> usize {
        self.num_pes
    }
    fn num_threads_per_pe(&self) -> usize {
        self.num_threads()
    }
    fn world(&self) -> Arc<LamellarTeam> {
        // println!("LamellarTeamRT world");
        // self.print_cnt();
        let world = if let Some(world) = self.world.clone() {
            world
        } else {
            // self.print_cnt();
            self.clone()
        };
        let world = LamellarTeam::new(None, world, false);
        // self.print_cnt();
        world
    }
    fn team(&self) -> Arc<LamellarTeam> {
        // println!("LamellarTeamRT team");
        // self.print_cnt();
        let world = if self.world.is_some() {
            Some(self.world())
        } else {
            None
        };
        let team = LamellarTeam::new(world, self.clone(), false);
        // self.print_cnt();
        team
    }
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

impl Hash for LamellarTeamRT {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.team_hash.hash(state);
    }
}

impl LamellarTeamRT {
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new(
        //creates a new root team
        num_pes: usize,
        world_pe: usize,
        scheduler: Arc<Scheduler>,
        world_counters: Arc<AMCounters>,
        lamellae: Arc<Lamellae>,
        panic: Arc<AtomicU8>,
        // teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
    ) -> Pin<Arc<LamellarTeamRT>> {
        let arch = Arc::new(LamellarArchRT {
            parent: None,
            arch: LamellarArchEnum::GlobalArch(GlobalArch::new(num_pes)),
            num_pes: num_pes,
        });
        lamellae.comm().barrier();

        let barrier = Barrier::new(
            world_pe,
            num_pes,
            lamellae.clone(),
            arch.clone(),
            scheduler.clone(),
            panic.clone(),
        );
        // println!("barrier created");

        let alloc = AllocationType::Global;
        let team_counters = Arc::new(AMCounters::new());

        let dropped = MemoryRegion::new(
            num_pes,
            &scheduler,
            vec![team_counters.clone(), world_counters.clone()],
            &lamellae,
            alloc.clone(),
        );

        let remote_ptr_alloc = lamellae
            .comm()
            .alloc(
                std::mem::size_of::<*const LamellarTeamRT>(),
                alloc,
                std::mem::align_of::<*const LamellarTeamRT>(),
            )
            .expect("unable to allocate remote_ptr_alloc");
        // println!("remote_ptr_alloc created {:?}", remote_ptr_alloc);

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
            team_counters,
            world_counters: world_counters,
            id: 0,
            team_hash: 0, //easy id to look up for global
            sub_team_id_cnt: AtomicUsize::new(0),
            barrier: barrier,
            dropped: dropped,
            remote_ptr_alloc: remote_ptr_alloc,
            panic: panic.clone(),
            tid: std::thread::current().id(),
            // panic_info: Arc::new(Mutex::new(Vec::new())),
            _pin: PhantomPinned,
        };

        // trace!("team addr {:?}", team.remote_ptr_alloc);
        unsafe {
            for e in team.dropped.as_mut_slice().iter_mut() {
                *e = 0;
            }
        }

        let team = Arc::pin(team);
        unsafe {
            let pinned_team = Pin::into_inner_unchecked(team.clone()).clone(); //get access to inner arc (it will stay pinned)
            let team_ptr = Arc::into_raw(pinned_team); //we consume the arc to get access to raw ptr (this ensures we wont drop the team until destroy is called)
                                                       // std::ptr::copy_nonoverlapping(&(&team as *const Pin<Arc<LamellarTeamRT>>), team.remote_ptr_alloc as *mut (*const Pin<Arc<LamellarTeamRT>>), 1);
                                                       // we are copying the address of the team into the remote ptr address, because of the two previous calls, we ensure its location is pinned and its lifetime will live until the team is destroyed.
                                                       // std::ptr::copy_nonoverlapping(&team_ptr, team.remote_ptr_alloc.as_mut_ptr(), 1);
            *team.remote_ptr_alloc.as_mut_ptr() = team_ptr;
            trace!(
                "team_ptr: {:?} remote_ptr_alloc {:?} {:?} {:?}",
                team_ptr,
                team.remote_ptr_alloc,
                team.remote_ptr_alloc.as_ref::<*const LamellarTeamRT>(),
                team.remote_ptr_alloc.as_ref::<usize>()
            );

            // println!{"{:?} {:?} {:?} {:?}",&team_ptr,team_ptr, (team.remote_ptr_alloc as *mut (*const LamellarTeamRT)).as_ref(), (*(team.remote_ptr_alloc as *mut (*const LamellarTeamRT))).as_ref()};
        }

        // println!("sechduler_new: {:?}", Arc::strong_count(&team.scheduler));
        // println!("lamellae: {:?}", Arc::strong_count(&team.lamellae));
        // println!("arch: {:?}", Arc::strong_count(&team.arch));
        // println!(
        //     "world_counters: {:?}",
        //     Arc::strong_count(&team.world_counters)
        // );

        // println!("entering lamellae barrier");
        lamellae.comm().barrier(); // need to make sure barriers are done before we return
                                   // println!("entering team barrier");
                                   // team.barrier.barrier();
        team
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn force_shutdown(&self) {
        // println!("force_shutdown {:?} {:?}",self.tid, std::thread::current().id());
        let first = self
            .panic
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok();
        if self.tid == std::thread::current().id() {
            self.panic.store(2, Ordering::SeqCst);
        }
        self.scheduler.force_shutdown();
        if first {
            self.lamellae.force_shutdown();
        }
        // if self.panic.compare_exchange(false,true,Ordering::SeqCst,Ordering::SeqCst).is_ok(){
        // println!("Im first! {:?} {:?}",self.tid, std::thread::current().id());

        if self.tid == std::thread::current().id() {
            // println!("Im main thread!");
            self.mem_regions.write().clear();
            self.sub_teams.write().clear();
            // std::process::exit(1);
            self.lamellae.force_deinit();
            // std::process::exit(1);
        }
        // // }
        // else if self.tid == std::thread::current().id() {
        //     println!("Im main thread!");
        //     self.scheduler.force_shutdown();
        //     self.lamellae.force_shutdown();
        //     self.mem_regions.write().clear();
        //     self.sub_teams.write().clear();
        //     //
        //     self.lamellae.force_deinit();
        //     std::process::exit(1);
        // }
        // println!("force_shutdown done {:?} {:?}",self.tid, std::thread::current().id());
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn destroy(&self) {
        // println!("destroying team? {:?}", self.mem_regions.read().len());
        if self.panic.load(Ordering::SeqCst) == 0 {
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
        }
        self.mem_regions.write().clear();
        self.sub_teams.write().clear(); // not sure this is necessary or should be allowed? sub teams delete themselves from this map when dropped...
        self.lamellae.comm().wait();
        self.lamellae.comm().barrier();
        if self.panic.load(Ordering::SeqCst) == 0 {
            // what does it mean if we drop a parent team while a sub_team is valid?
            if let None = &self.parent {
                // println!("shutdown lamellae, going to shutdown scheduler");
                self.scheduler.begin_shutdown();
                self.put_dropped();
                self.drop_barrier();
                self.lamellae.shutdown();
                self.scheduler.shutdown();
            }
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
        unsafe {
            let team_ptr: *mut LamellarTeamRT = *self.remote_ptr_alloc.as_ptr();
            self.lamellae.comm().free(self.remote_ptr_alloc.clone());
            (team_ptr).as_mut().unwrap().remote_ptr_alloc = CommAlloc {
                inner_alloc: CommAllocInner::Raw(0, 0),
                alloc_type: CommAllocType::Fabric,
            };
            trace!("dropping team_ptr: {:?}", team_ptr);

            let arc_team = Arc::from_raw(team_ptr);

            // println!("arc_team: {:?}", Arc::strong_count(&arc_team));
            Pin::new_unchecked(arc_team); //allows us to get rid of the extra reference created in new
        }

        // println!("team destroyed")
    }
    #[allow(dead_code)]
    pub(crate) fn get_pes(&self) -> Vec<usize> {
        self.arch.team_iter().collect::<Vec<usize>>()
    }

    pub(crate) fn team_pe_id(&self) -> Result<usize, IdError> {
        self.arch.team_pe(self.world_pe)
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn counters(&self) -> Vec<Arc<AMCounters>> {
        vec![self.world_counters.clone(), self.team_counters.clone()]
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn create_subteam_from_arch<L>(
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
            // println!("arch: {:?}", archrt);
            parent.barrier();
            // ------ ensure team is being constructed synchronously and in order across all pes in parent ------ //
            let parent_alloc = AllocationType::Sub(parent.arch.team_iter().collect::<Vec<usize>>());

            let team_counters = Arc::new(AMCounters::new());
            // println!("allocating hash_buf");
            let dropped = MemoryRegion::<usize>::new(
                parent.num_pes,
                &parent.scheduler,
                vec![team_counters.clone(), parent.world_counters.clone()],
                &parent.lamellae,
                parent_alloc.clone(),
            );
            unsafe { dropped.as_mut_slice().fill(0) };

            // println!("allocating temp_array");
            // let temp_array = MemoryRegion::<usize>::new(
            //     parent.num_pes,
            //     &parent.scheduler,
            //     vec![],
            //     &parent.lamellae,
            //     AllocationType::Local,
            // );
            // let mut temp_array_slice =
            //     unsafe { temp_array.as_comm_slice().expect("data should exist on pe") };
            // for e in temp_array_slice.iter_mut() {
            //     *e = 0;
            // }
            // unsafe {
            //     hash_buf.put_buffer(parent.world_pe, 0, temp_array_slice.sub_slice(..parent.num_pes)).block();
            // }
            // let mut temp_array = vec![0; parent.num_pes];
            // temp_array[0] = hash as usize;
            let s = Instant::now();
            parent.barrier();
            // println!("barrier done in {:?}", s.elapsed());
            let timeout = Instant::now() - s;
            // temp_array_slice[0] = hash as usize;

            // println!("putting hash vals");
            if let Ok(parent_world_pe) = parent.arch.team_pe(parent.world_pe) {
                for world_pe in parent.arch.team_iter() {
                    if world_pe != parent.world_pe {
                        unsafe {
                            dropped
                                .put(world_pe, parent_world_pe, hash as usize)
                                .block();
                        }
                    }
                }
            }
            // println!("done putting hash, now gonna check hash vals");
            parent.check_hash_vals(hash as usize, &dropped, timeout);
            // println!("passed check hash vals");

            let remote_ptr_alloc = parent
                .lamellae
                .comm()
                .alloc(
                    std::mem::size_of::<*const LamellarTeamRT>(),
                    parent_alloc,
                    std::mem::align_of::<*const LamellarTeamRT>(),
                )
                .expect("alloc failed creating LamellarTeam");
            // ------------------------------------------------------------------------------------------------- //
            // println!("passed remote_ptr_alloc");
            // for e in temp_array_slice.iter_mut() {
            //     *e = 0;
            // }
            let num_pes = archrt.num_pes();
            parent.barrier();
            // println!("passed barrier, creating RT team");
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
                team_counters,
                world_counters: parent.world_counters.clone(),
                id: id,
                sub_team_id_cnt: AtomicUsize::new(0),
                barrier: Barrier::new(
                    parent.world_pe,
                    parent.num_world_pes,
                    parent.lamellae.clone(),
                    archrt,
                    parent.scheduler.clone(),
                    parent.panic.clone(),
                ),
                team_hash: hash,
                dropped,
                remote_ptr_alloc: remote_ptr_alloc,
                panic: parent.panic.clone(),
                tid: parent.tid,
                // panic_info: parent.panic_info.clone(),
                _pin: PhantomPinned,
            };
            unsafe {
                for e in team.dropped.as_mut_slice().iter_mut() {
                    *e = 0;
                }
            }
            let team = Arc::pin(team);
            unsafe {
                let pinned_team = Pin::into_inner_unchecked(team.clone()).clone();
                let team_ptr = Arc::into_raw(pinned_team);
                // std::ptr::copy_nonoverlapping(&(&team as *const Pin<Arc<LamellarTeamRT>>), team.remote_ptr_alloc as *mut (*const Pin<Arc<LamellarTeamRT>>), 1);
                // std::ptr::copy_nonoverlapping(
                //     &team_ptr,
                //     unsafe { team.remote_ptr_alloc.as_mut_ptr() },
                //     1,
                // );
                *team.remote_ptr_alloc.as_mut_ptr() = team_ptr;
                trace!("team_ptr {:?} {:?}", team_ptr, team_ptr.as_ref());
            }

            // println!("team created in {:?}", s.elapsed());

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

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn num_pes(&self) -> usize {
        self.arch.num_pes()
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn num_threads(&self) -> usize {
        self.scheduler.num_workers()
    }

    #[cfg_attr(test, allow(unreachable_code), allow(unused_variables))]
    #[tracing::instrument(skip_all, level = "debug")]
    fn check_hash_vals(&self, hash: usize, hash_buf: &MemoryRegion<usize>, timeout: Duration) {
        #[cfg(test)]
        return;

        let mut s = Instant::now();
        let mut cnt = 0;

        for (pe, hash_val) in hash_buf.as_slice().iter().enumerate() {
            if pe != self.team_pe.unwrap() {
                while *hash_val == 0 {
                    self.flush();
                    std::thread::yield_now();
                    if s.elapsed().as_secs_f64() > config().deadlock_warning_timeout {
                        let status = hash_buf
                            .as_slice()
                            .iter()
                            .enumerate()
                            .map(|(i, elem)| (i, *elem == hash))
                            .collect::<Vec<_>>();
                        println!("[WARNING]  Potential deadlock detected when trying construct a new LamellarTeam.\n\
                        Creating a team is a collective operation requiring all PEs associated with the Parent Team (or LamellarWorld) to enter the call, not just the PEs that will be part of the new team.\n\
                        The following indicates which PEs have not entered the call: {:?}\n\
                        The deadlock timeout can be set via the LAMELLAR_DEADLOCK_WARNING_TIMEOUT environment variable, the current timeout is {} seconds\n\
                        To view backtrace set RUST_LIB_BACKTRACE=1\n\
                        {}",status,config().deadlock_warning_timeout,std::backtrace::Backtrace::capture()
                    );
                        // println!(
                        //     "[{:?}] ({:?})  hash: {:?}",
                        //     self.world_pe,
                        //     hash,
                        //     hash_buf.as_slice().unwrap()
                        // );
                        s = Instant::now();
                    }
                }
                if *hash_val != hash && Instant::now() - s > timeout {
                    println!(
                        "[{:?}] ({:?})  hash: {:?}",
                        self.world_pe,
                        hash,
                        hash_buf.as_slice()
                    );
                    panic!("team creating mismatch! Ensure teams are constructed in same order on every pe");
                } else {
                    std::thread::yield_now();
                    cnt = cnt + 1;
                }
            }
        }
        // println!("{:?} {:?}", hash,hash_buf.as_slice().unwrap());
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn put_dropped(&self) {
        if self.panic.load(Ordering::SeqCst) == 0 {
            if let Some(parent) = &self.parent {
                // let mut temp_slice = unsafe {
                //     self.dropped
                //         .as_comm_slice()
                //         .expect("data should exist on pe")
                // };

                let my_index = parent
                    .arch
                    .team_pe(self.world_pe)
                    .expect("invalid parent pe");
                // temp_slice[my_index] = 1;
                for world_pe in self.arch.team_iter() {
                    // if world_pe != self.world_pe {
                    unsafe {
                        let _ = self.dropped.put(world_pe, my_index, 1).spawn();
                    }
                    // }
                }
            } else {
                // let mut temp_slice = unsafe {
                //     self.dropped
                //         .as_comm_slice()
                //         .expect("data should exist on pe")
                // };
                // temp_slice[self.world_pe] = 1;
                for world_pe in self.arch.team_iter() {
                    if world_pe != self.world_pe {
                        unsafe {
                            let _ = self.dropped.put(world_pe, self.world_pe, 1).spawn();
                        }
                    }
                }
            }
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn drop_barrier(&self) {
        let mut s = Instant::now();
        if self.panic.load(Ordering::SeqCst) == 0 {
            for pe in self.dropped.as_slice().iter() {
                while *pe != 1 {
                    // std::thread::yield_now();
                    if s.elapsed().as_secs_f64() > config().deadlock_warning_timeout {
                        println!("[WARNING]  Potential deadlock detected when trying to drop a LamellarTeam.\n\
                            The following indicates the dropped status on each PE: {:?}\n\
                            The deadlock timeout can be set via the LAMELLAR_DEADLOCK_WARNING_TIMEOUT environment variable, the current timeout is {} seconds\n\
                            To view backtrace set RUST_LIB_BACKTRACE=1\n\
                            {}",
                            self.dropped.as_slice(),
                            config().deadlock_warning_timeout,
                            std::backtrace::Backtrace::capture());
                        s = Instant::now();
                    }
                }
            }
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn print_arch(&self) {
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

    pub(crate) fn inc_outstanding(&self, cnt: usize) {
        self.team_counters.inc_outstanding(cnt);
        self.world_counters.inc_outstanding(cnt);
    }

    pub(crate) fn dec_outstanding(&self, cnt: usize) {
        self.team_counters.dec_outstanding(cnt);
        self.world_counters.dec_outstanding(cnt);
    }

    pub(crate) fn spawn<F>(&self, task: F) -> LamellarTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        assert!(self.panic.load(Ordering::SeqCst) == 0);
        self.scheduler.spawn_task(
            task,
            vec![self.world_counters.clone(), self.team_counters.clone()],
        )
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn wait_all(&self) {
        // println!("wait_all called on pe: {}", self.world_pe);
        RuntimeWarning::BlockingCall("wait_all", "await_all().await").print();

        self.lamellae.comm().wait();

        let mut temp_now = Instant::now();
        let mut orig_reqs = self.team_counters.send_req_cnt.load(Ordering::SeqCst);
        let mut orig_launched = self.team_counters.launched_req_cnt.load(Ordering::SeqCst);
        let mut world_orig_reqs = self.world_counters.send_req_cnt.load(Ordering::SeqCst);
        let mut world_orig_launched = self.world_counters.launched_req_cnt.load(Ordering::SeqCst);

        // println!(
        //     "in team wait_all mype: {:?} cnt: {:?} {:?}",
        //     self.world_pe,
        //     self.team_counters.send_req_cnt.load(Ordering::SeqCst),
        //     self.team_counters.outstanding_reqs.load(Ordering::SeqCst),
        // );
        let mut done = false;
        while !done {
            while self.panic.load(Ordering::SeqCst) == 0
                && ((self.team_counters.outstanding_reqs.load(Ordering::SeqCst) > 0
                    || orig_reqs != self.team_counters.send_req_cnt.load(Ordering::SeqCst)
                    || orig_launched != self.team_counters.launched_req_cnt.load(Ordering::SeqCst))
                    || (self.parent.is_none()
                        && (self.world_counters.outstanding_reqs.load(Ordering::SeqCst) > 0
                            || world_orig_reqs
                                != self.world_counters.send_req_cnt.load(Ordering::SeqCst)
                            || world_orig_launched
                                != self.world_counters.launched_req_cnt.load(Ordering::SeqCst))))
            {
                orig_reqs = self.team_counters.send_req_cnt.load(Ordering::SeqCst);
                orig_launched = self.team_counters.launched_req_cnt.load(Ordering::SeqCst);
                world_orig_reqs = self.world_counters.send_req_cnt.load(Ordering::SeqCst);
                world_orig_launched = self.world_counters.launched_req_cnt.load(Ordering::SeqCst);
                // std::thread::yield_now();
                // self.flush();
                if std::thread::current().id() == *crate::MAIN_THREAD {
                    self.scheduler.exec_task()
                }; //mmight as well do useful work while we wait }
                if temp_now.elapsed().as_secs_f64() > config().deadlock_warning_timeout {
                    println!(
                        "in team wait_all mype: {:?} cnt: {:?} {:?} {:?}",
                        self.world_pe,
                        self.team_counters.send_req_cnt.load(Ordering::SeqCst),
                        self.team_counters.outstanding_reqs.load(Ordering::SeqCst),
                        self.team_counters.launched_req_cnt.load(Ordering::SeqCst)
                    );
                    temp_now = Instant::now();
                }
            }
            if self.team_counters.send_req_cnt.load(Ordering::SeqCst)
                != self.team_counters.launched_req_cnt.load(Ordering::SeqCst)
                || (self.parent.is_none()
                    && self.world_counters.send_req_cnt.load(Ordering::SeqCst)
                        != self.world_counters.launched_req_cnt.load(Ordering::SeqCst))
            {
                if (self.team_counters.outstanding_reqs.load(Ordering::SeqCst) > 0
                    || orig_reqs != self.team_counters.send_req_cnt.load(Ordering::SeqCst)
                    || orig_launched != self.team_counters.launched_req_cnt.load(Ordering::SeqCst))
                    || (self.parent.is_none()
                        && (self.world_counters.outstanding_reqs.load(Ordering::SeqCst) > 0
                            || world_orig_reqs
                                != self.world_counters.send_req_cnt.load(Ordering::SeqCst)
                            || world_orig_launched
                                != self.world_counters.launched_req_cnt.load(Ordering::SeqCst)))
                {
                    continue;
                }
                println!(
                    "in team wait_all mype: {:?} cnt: {:?} {:?} {:?}",
                    self.world_pe,
                    self.team_counters.send_req_cnt.load(Ordering::SeqCst),
                    self.team_counters.outstanding_reqs.load(Ordering::SeqCst),
                    self.team_counters.launched_req_cnt.load(Ordering::SeqCst)
                );
                RuntimeWarning::UnspawnedTask(
                    "`wait_all` before all tasks/active messages have been spawned",
                )
                .print();
            }
            done = true;
        }
        // println!(
        //     "in team wait_all mype: {:?} cnt: {:?} {:?}",
        //     self.world_pe,
        //     self.team_counters.send_req_cnt.load(Ordering::SeqCst),
        //     self.team_counters.outstanding_reqs.load(Ordering::SeqCst),
        // );
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) async fn await_all(&self) {
        // println!("await_all called on pe: {}", self.world_pe);
        self.lamellae.comm().wait();
        let mut temp_now = Instant::now();
        let mut orig_reqs = self.team_counters.send_req_cnt.load(Ordering::SeqCst);
        let mut orig_launched = self.team_counters.launched_req_cnt.load(Ordering::SeqCst);
        let mut world_orig_reqs = self.world_counters.send_req_cnt.load(Ordering::SeqCst);
        let mut world_orig_launched = self.world_counters.launched_req_cnt.load(Ordering::SeqCst);

        // println!(
        //     "in team await_all mype: {:?} cnt: {:?} {:?}",
        //     self.world_pe,
        //     self.team_counters.send_req_cnt.load(Ordering::SeqCst),
        //     self.team_counters.outstanding_reqs.load(Ordering::SeqCst),
        // );
        let mut done = false;
        while !done {
            while self.panic.load(Ordering::SeqCst) == 0
                && ((self.team_counters.outstanding_reqs.load(Ordering::SeqCst) > 0
                    || orig_reqs != self.team_counters.send_req_cnt.load(Ordering::SeqCst)
                    || orig_launched != self.team_counters.launched_req_cnt.load(Ordering::SeqCst))
                    || (self.parent.is_none()
                        && (self.world_counters.outstanding_reqs.load(Ordering::SeqCst) > 0
                            || world_orig_reqs
                                != self.world_counters.send_req_cnt.load(Ordering::SeqCst)
                            || world_orig_launched
                                != self.world_counters.launched_req_cnt.load(Ordering::SeqCst))))
            {
                orig_reqs = self.team_counters.send_req_cnt.load(Ordering::SeqCst);
                orig_launched = self.team_counters.launched_req_cnt.load(Ordering::SeqCst);
                world_orig_reqs = self.world_counters.send_req_cnt.load(Ordering::SeqCst);
                world_orig_launched = self.world_counters.launched_req_cnt.load(Ordering::SeqCst);
                async_std::task::yield_now().await;
                if temp_now.elapsed().as_secs_f64() > config().deadlock_warning_timeout {
                    println!(
                        "in team wait_all mype: {:?} cnt: {:?} {:?}",
                        self.world_pe,
                        self.team_counters.send_req_cnt.load(Ordering::SeqCst),
                        self.team_counters.outstanding_reqs.load(Ordering::SeqCst),
                    );
                    temp_now = Instant::now();
                }
            }
            if self.team_counters.send_req_cnt.load(Ordering::SeqCst)
                != self.team_counters.launched_req_cnt.load(Ordering::SeqCst)
                || (self.parent.is_none()
                    && self.world_counters.send_req_cnt.load(Ordering::SeqCst)
                        != self.world_counters.launched_req_cnt.load(Ordering::SeqCst))
            {
                if (self.team_counters.outstanding_reqs.load(Ordering::SeqCst) > 0
                    || orig_reqs != self.team_counters.send_req_cnt.load(Ordering::SeqCst)
                    || orig_launched != self.team_counters.launched_req_cnt.load(Ordering::SeqCst))
                    || (self.parent.is_none()
                        && (self.world_counters.outstanding_reqs.load(Ordering::SeqCst) > 0
                            || world_orig_reqs
                                != self.world_counters.send_req_cnt.load(Ordering::SeqCst)
                            || world_orig_launched
                                != self.world_counters.launched_req_cnt.load(Ordering::SeqCst)))
                {
                    continue;
                }
                println!(
                    "in team await_all mype: {:?} cnt: {:?} {:?} {:?}",
                    self.world_pe,
                    self.team_counters.send_req_cnt.load(Ordering::SeqCst),
                    self.team_counters.outstanding_reqs.load(Ordering::SeqCst),
                    self.team_counters.launched_req_cnt.load(Ordering::SeqCst)
                );
                RuntimeWarning::UnspawnedTask(
                    "`await_all` before all tasks/active messages have been spawned",
                )
                .print();
            }
            done = true;
        }
        // println!(
        //     "in team wait_all mype: {:?} cnt: {:?} {:?}",
        //     self.world_pe,
        //     self.team_counters.send_req_cnt.load(Ordering::SeqCst),
        //     self.team_counters.outstanding_reqs.load(Ordering::SeqCst),
        // );
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        assert!(self.panic.load(Ordering::SeqCst) == 0);

        self.scheduler.block_on(f)
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn block_on_all<I>(
        &self,
        iter: I,
    ) -> Vec<<<I as IntoIterator>::Item as Future>::Output>
    where
        I: IntoIterator,
        <I as IntoIterator>::Item: Future + Send + 'static,
        <<I as IntoIterator>::Item as Future>::Output: Send,
    {
        assert!(self.panic.load(Ordering::SeqCst) == 0);
        self.scheduler
            .block_on(join_all(iter.into_iter().map(|task| {
                self.scheduler.spawn_task(
                    task,
                    vec![self.world_counters.clone(), self.team_counters.clone()],
                )
            })))
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn barrier(&self) {
        self.barrier.barrier();
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn tasking_barrier(&self) {
        self.barrier.tasking_barrier();
    }
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn async_barrier(&self) -> BarrierHandle {
        self.barrier.barrier_handle()
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn flush(&self) {
        self.lamellae.comm().flush();
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn exec_am_all<F>(self: &Pin<Arc<LamellarTeamRT>>, am: F) -> MultiAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + AmDist,
    {
        self.exec_am_all_tg(am, None)
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn exec_am_all_tg<F>(
        self: &Pin<Arc<LamellarTeamRT>>,
        am: F,
        task_group_cnts: Option<Arc<AMCounters>>,
    ) -> MultiAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + crate::Serialize + 'static,
    {
        // println!("team exec am all num_pes {:?}", self.num_pes);
        // trace!("[{:?}] team exec am all request", self.world_pe);
        // event!(Level::TRACE, "team exec am all request");
        if let Some(task_group_cnts) = task_group_cnts.as_ref() {
            task_group_cnts.inc_send_req(1);
        }
        let req = Arc::new(MultiAmHandleInner {
            cnt: AtomicUsize::new(self.num_pes),
            arch: self.arch.clone(),
            data: Mutex::new(HashMap::new()),
            waker: Mutex::new(None),
            team_counters: self.team_counters.clone(),
            world_counters: self.world_counters.clone(),
            tg_counters: task_group_cnts,
            user_handle: AtomicU8::new(1),
            scheduler: self.scheduler.clone(),
        });
        let req_result = Arc::new(LamellarRequestResult::MultiAm(req.clone()));
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

        self.world_counters.inc_send_req(self.num_pes);
        self.team_counters.inc_send_req(self.num_pes);
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
            team_addr: self.remote_ptr_alloc.comm_addr(),
        };
        // event!(Level::TRACE, "submitting request to scheduler");
        // println!("[{:?}] team exec all", std::thread::current().id());
        // self.scheduler.submit_am(Am::All(req_data, func));
        MultiAmHandle {
            inner: req,
            am: Some((Am::All(req_data, func), self.num_pes)),
            _phantom: PhantomData,
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn am_group_exec_am_all_tg<F, O>(
        self: &Pin<Arc<LamellarTeamRT>>,
        am: F,
        task_group_cnts: Option<Arc<AMCounters>>,
    ) -> MultiAmHandle<O>
    where
        F: RemoteActiveMessage + LamellarAM + crate::Serialize + 'static,
        O: AmDist + 'static,
    {
        // println!("team exec am all num_pes {:?}", self.num_pes);
        // trace!("[{:?}] team exec am all request", self.world_pe);
        // event!(Level::TRACE, "team exec am all request");
        if let Some(task_group_cnts) = task_group_cnts.as_ref() {
            task_group_cnts.inc_send_req(1);
        }

        let req = Arc::new(MultiAmHandleInner {
            cnt: AtomicUsize::new(self.num_pes),
            arch: self.arch.clone(),
            data: Mutex::new(HashMap::new()),
            waker: Mutex::new(None),
            team_counters: self.team_counters.clone(),
            world_counters: self.world_counters.clone(),
            tg_counters: task_group_cnts,
            user_handle: AtomicU8::new(1),
            scheduler: self.scheduler.clone(),
        });
        let req_result = Arc::new(LamellarRequestResult::MultiAm(req.clone()));
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

        self.world_counters.inc_send_req(self.num_pes);
        self.team_counters.inc_send_req(self.num_pes);
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
            team_addr: self.remote_ptr_alloc.comm_addr(),
        };
        // event!(Level::TRACE, "submitting request to scheduler");
        // println!("[{:?}] team am group exec all", std::thread::current().id());
        // self.scheduler.submit_am(Am::All(req_data, func));
        MultiAmHandle {
            inner: req,
            am: Some((Am::All(req_data, func), self.num_pes)),
            _phantom: PhantomData,
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn exec_am_pe<F>(
        self: &Pin<Arc<LamellarTeamRT>>,
        pe: usize,
        am: F,
    ) -> AmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + AmDist,
    {
        self.exec_am_pe_tg(pe, am, None)
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn exec_am_pe_tg<F>(
        self: &Pin<Arc<LamellarTeamRT>>,
        pe: usize,
        am: F,
        task_group_cnts: Option<Arc<AMCounters>>,
    ) -> AmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + crate::Serialize + 'static,
    {
        // println!("team exec am pe tg");
        if let Some(task_group_cnts) = task_group_cnts.as_ref() {
            task_group_cnts.inc_send_req(1);
        }
        assert!(pe < self.arch.num_pes());

        let req = Arc::new(AmHandleInner {
            ready: AtomicBool::new(false),
            data: Cell::new(None),
            waker: Mutex::new(None),
            team_counters: self.team_counters.clone(),
            world_counters: self.world_counters.clone(),
            tg_counters: task_group_cnts,
            user_handle: AtomicU8::new(1),
            scheduler: self.scheduler.clone(),
        });
        let req_result = Arc::new(LamellarRequestResult::Am(req.clone()));
        let req_ptr = Arc::into_raw(req_result);
        // Arc::increment_strong_count(req_ptr); //we would need to do this for the exec_all command
        let id = ReqId {
            id: req_ptr as usize,
            sub_id: 0,
        };
        self.world_counters.inc_send_req(1);
        self.team_counters.inc_send_req(1);

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
            team_addr: self.remote_ptr_alloc.comm_addr(),
        };

        AmHandle {
            inner: req,
            am: Some((Am::Remote(req_data, func), 1)),
            _phantom: PhantomData,
        }
        .into()
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn spawn_am_pe_tg<F>(
        self: &Pin<Arc<LamellarTeamRT>>,
        pe: usize,
        am: F,
        task_group_cnts: Option<Arc<AMCounters>>,
    ) -> AmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + crate::Serialize + 'static,
    {
        // println!("team exec am pe tg");
        if let Some(task_group_cnts) = task_group_cnts.as_ref() {
            task_group_cnts.inc_outstanding(1);
            task_group_cnts.inc_launched(1);
            task_group_cnts.inc_send_req(1);
        }
        assert!(pe < self.arch.num_pes());

        let req = Arc::new(AmHandleInner {
            ready: AtomicBool::new(false),
            data: Cell::new(None),
            waker: Mutex::new(None),
            team_counters: self.team_counters.clone(),
            world_counters: self.world_counters.clone(),
            tg_counters: task_group_cnts,
            user_handle: AtomicU8::new(1),
            scheduler: self.scheduler.clone(),
        });
        let req_result = Arc::new(LamellarRequestResult::Am(req.clone()));
        let req_ptr = Arc::into_raw(req_result);
        // Arc::increment_strong_count(req_ptr); //we would need to do this for the exec_all command
        let id = ReqId {
            id: req_ptr as usize,
            sub_id: 0,
        };

        self.world_counters.inc_outstanding(1);
        self.world_counters.inc_launched(1);
        self.world_counters.inc_send_req(1);
        self.team_counters.inc_outstanding(1);
        self.team_counters.inc_launched(1);
        self.team_counters.inc_send_req(1);

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
            team_addr: self.remote_ptr_alloc.comm_addr(),
        };

        self.scheduler.submit_am(Am::Remote(req_data, func));

        AmHandle {
            inner: req,
            am: None,
            _phantom: PhantomData,
        }
        .into()
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn am_group_exec_am_pe_tg<F, O>(
        self: &Pin<Arc<LamellarTeamRT>>,
        pe: usize,
        am: F,
        task_group_cnts: Option<Arc<AMCounters>>,
    ) -> AmHandle<O>
    where
        F: RemoteActiveMessage + LamellarAM + crate::Serialize + 'static,
        O: AmDist + 'static,
    {
        // println!("team exec am pe tg");
        if let Some(task_group_cnts) = task_group_cnts.as_ref() {
            task_group_cnts.inc_send_req(1);
        }
        assert!(pe < self.arch.num_pes());

        let req = Arc::new(AmHandleInner {
            ready: AtomicBool::new(false),
            data: Cell::new(None),
            waker: Mutex::new(None),
            team_counters: self.team_counters.clone(),
            world_counters: self.world_counters.clone(),
            tg_counters: task_group_cnts,
            user_handle: AtomicU8::new(1),
            scheduler: self.scheduler.clone(),
        });
        let req_result = Arc::new(LamellarRequestResult::Am(req.clone()));
        let req_ptr = Arc::into_raw(req_result);
        // Arc::increment_strong_count(req_ptr); //we would need to do this for the exec_all command
        let id = ReqId {
            id: req_ptr as usize,
            sub_id: 0,
        };
        self.world_counters.inc_send_req(1);
        self.team_counters.inc_send_req(1);
        // println!(
        //     "req_id: {:?} tc: {:?} wc: {:?}",
        //     id,
        //     self.team_counters.outstanding_reqs.load(Ordering::Relaxed),
        //     self.world_counters.outstanding_reqs.load(Ordering::Relaxed)
        // );
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
            team_addr: self.remote_ptr_alloc.comm_addr(),
        };

        // println!(
        //     "[{:?}] team am group exec am pe tg",
        //     std::thread::current().id()
        // );
        // self.scheduler.submit_am(Am::Remote(req_data, func));

        // Box::new(LamellarRequestHandle {
        //     inner: req,
        //     _phantom: PhantomData,
        // })
        AmHandle {
            inner: req,
            am: Some((Am::Remote(req_data, func), 1)),
            _phantom: PhantomData,
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn exec_arc_am_all<F>(
        self: &Pin<Arc<LamellarTeamRT>>,
        am: LamellarArcAm,
        task_group_cnts: Option<Arc<AMCounters>>,
    ) -> MultiAmHandle<F>
    where
        F: AmDist,
    {
        // println!("team exec arc am pe");
        if let Some(task_group_cnts) = task_group_cnts.as_ref() {
            task_group_cnts.inc_send_req(1);
        }
        let req = Arc::new(MultiAmHandleInner {
            cnt: AtomicUsize::new(self.num_pes),
            arch: self.arch.clone(),
            waker: Mutex::new(None),
            data: Mutex::new(HashMap::new()),
            team_counters: self.team_counters.clone(),
            world_counters: self.world_counters.clone(),
            tg_counters: task_group_cnts,
            user_handle: AtomicU8::new(1),
            scheduler: self.scheduler.clone(),
        });
        let req_result = Arc::new(LamellarRequestResult::MultiAm(req.clone()));
        let req_ptr = Arc::into_raw(req_result);
        for _ in 0..(self.num_pes - 1) {
            // -1 because of the arc we turned into raw
            unsafe { Arc::increment_strong_count(req_ptr) } //each pe will return a result (which we turn back into an arc)
        }
        let id = ReqId {
            id: req_ptr as usize,
            sub_id: 0,
        };
        self.world_counters.inc_send_req(self.num_pes);
        self.team_counters.inc_send_req(self.num_pes);
        // println!("cnts: t: {} w: {} tg: {:?}",self.team_counters.outstanding_reqs.load(Ordering::Relaxed),self.world_counters.outstanding_reqs.load(Ordering::Relaxed), tg_outstanding_reqs.as_ref().map(|x| x.load(Ordering::Relaxed)));

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
            team_addr: self.remote_ptr_alloc.comm_addr(),
        };

        // println!(
        //     "[{:?}] team arc exec am all tg",
        //     std::thread::current().id()
        // );
        // self.scheduler.submit_am(Am::All(req_data, am));

        MultiAmHandle {
            inner: req,
            am: Some((Am::All(req_data, am), self.num_pes)),
            _phantom: PhantomData,
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn exec_arc_am_pe<F>(
        self: &Pin<Arc<LamellarTeamRT>>,
        pe: usize,
        am: LamellarArcAm,
        task_group_cnts: Option<Arc<AMCounters>>,
    ) -> AmHandle<F>
    where
        F: AmDist,
    {
        // println!("team exec arc am pe");
        if let Some(task_group_cnts) = task_group_cnts.as_ref() {
            task_group_cnts.inc_send_req(1);
        }
        assert!(pe < self.arch.num_pes());
        let req = Arc::new(AmHandleInner {
            ready: AtomicBool::new(false),
            data: Cell::new(None),
            waker: Mutex::new(None),
            team_counters: self.team_counters.clone(),
            world_counters: self.world_counters.clone(),
            tg_counters: task_group_cnts,
            user_handle: AtomicU8::new(1),
            scheduler: self.scheduler.clone(),
        });
        let req_result = Arc::new(LamellarRequestResult::Am(req.clone()));
        let req_ptr = Arc::into_raw(req_result);
        let id = ReqId {
            id: req_ptr as usize,
            sub_id: 0,
        };
        self.world_counters.inc_send_req(1);
        self.team_counters.inc_send_req(1);
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
            team_addr: self.remote_ptr_alloc.comm_addr(),
        };

        // println!("[{:?}] team arc exec am pe", std::thread::current().id());
        // self.scheduler.submit_am(Am::Remote(req_data, am));

        // Box::new(LamellarRequestHandle {
        //     inner: req,
        //     _phantom: PhantomData,
        // })
        AmHandle {
            inner: req,
            am: Some((Am::Remote(req_data, am), 1)),
            _phantom: PhantomData,
        }
        .into()
    }

    #[allow(dead_code)]
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) async fn exec_arc_am_pe_immediately<F>(
        self: &Pin<Arc<LamellarTeamRT>>,
        pe: usize,
        am: LamellarArcAm,
        task_group_cnts: Option<Arc<AMCounters>>,
    ) -> AmHandle<F>
    where
        F: AmDist,
    {
        // println!("team exec arc am pe");
        if let Some(task_group_cnts) = task_group_cnts.as_ref() {
            task_group_cnts.inc_send_req(1);
        }
        assert!(pe < self.arch.num_pes());
        let req = Arc::new(AmHandleInner {
            ready: AtomicBool::new(false),
            data: Cell::new(None),
            waker: Mutex::new(None),
            team_counters: self.team_counters.clone(),
            world_counters: self.world_counters.clone(),
            tg_counters: task_group_cnts,
            user_handle: AtomicU8::new(1),
            scheduler: self.scheduler.clone(),
        });
        let req_result = Arc::new(LamellarRequestResult::Am(req.clone()));
        let req_ptr = Arc::into_raw(req_result);
        let id = ReqId {
            id: req_ptr as usize,
            sub_id: 0,
        };
        self.world_counters.inc_send_req(1);
        self.team_counters.inc_send_req(1);
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
            team_addr: self.remote_ptr_alloc.comm_addr(),
        };

        // println!("[{:?}] team arc exec am pe", std::thread::current().id());
        self.scheduler.exec_am(Am::Remote(req_data, am)).await;

        // Box::new(LamellarRequestHandle {
        //     inner: req,
        //     _phantom: PhantomData,
        // })
        AmHandle {
            inner: req,
            am: None,
            _phantom: PhantomData,
        }
        .into()
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn exec_am_local<F>(
        self: &Pin<Arc<LamellarTeamRT>>,
        am: F,
    ) -> LocalAmHandle<F::Output>
    where
        F: LamellarActiveMessage + LocalAM + 'static,
    {
        self.exec_am_local_tg(am, None, None)
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn exec_am_local_tg<F>(
        self: &Pin<Arc<LamellarTeamRT>>,
        am: F,
        task_group_cnts: Option<Arc<AMCounters>>,
        thread: Option<usize>,
    ) -> LocalAmHandle<F::Output>
    where
        F: LamellarActiveMessage + LocalAM + 'static,
    {
        // println!("team exec am local");
        if let Some(task_group_cnts) = task_group_cnts.as_ref() {
            task_group_cnts.inc_send_req(1);
        }
        let req = Arc::new(AmHandleInner {
            ready: AtomicBool::new(false),
            data: Cell::new(None),
            waker: Mutex::new(None),
            team_counters: self.team_counters.clone(),
            world_counters: self.world_counters.clone(),
            tg_counters: task_group_cnts,
            user_handle: AtomicU8::new(1),
            scheduler: self.scheduler.clone(),
        });
        let req_result = Arc::new(LamellarRequestResult::Am(req.clone()));
        let req_ptr = Arc::into_raw(req_result);
        let id = ReqId {
            id: req_ptr as usize,
            sub_id: 0,
        };

        self.world_counters.inc_send_req(1);
        self.team_counters.inc_send_req(1);
        // println!("cnts: t: {} w: {} tg: {:?}",self.team_counters.outstanding_reqs.load(Ordering::Relaxed),self.world_counters.outstanding_reqs.load(Ordering::Relaxed), tg_outstanding_reqs.as_ref().map(|x| x.load(Ordering::Relaxed)));

        let func: LamellarArcLocalAm = Arc::new(am);

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
            team_addr: self.remote_ptr_alloc.comm_addr(),
        };
        // println!("[{:?}] team exec am local", std::thread::current().id());
        // self.scheduler.submit_am(Am::Local(req_data, func));

        // Box::new(LamellarLocalRequestHandle {
        //     inner: req,
        //     _phantom: PhantomData,
        // })
        LocalAmHandle {
            inner: req,
            am: Some((Am::Local(req_data, func), 1)),
            _phantom: PhantomData,
            thread: thread,
        }
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
    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn alloc_one_sided_mem_region<T: Dist>(
        self: &Pin<Arc<LamellarTeamRT>>,
        size: usize,
    ) -> OneSidedMemoryRegion<T> {
        let mut lmr = OneSidedMemoryRegion::try_new(size, self);
        while let Err(_err) = lmr {
            std::thread::yield_now();
            // println!(
            //     "out of Lamellar mem trying to alloc new pool {:?} {:?}",
            //     size,
            //     std::mem::size_of::<T>()
            // );
            self.lamellae
                .comm()
                .alloc_pool(size * std::mem::size_of::<T>());
            lmr = OneSidedMemoryRegion::try_new(size, self);
        }
        lmr.expect("out of memory")
    }

    // pub(crate) fn print_cnt(self: &Pin<Arc<LamellarTeamRT>>) {
    //     let team_rt = unsafe { Pin::into_inner_unchecked(self.clone()) };
    //     println!("team_rt: {:?}", Arc::strong_count(&team_rt));
    // }
}

impl Drop for LamellarTeamRT {
    #[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
        // println!("LamellarTeamRT Drop");
        // println!("sechduler_new: {:?}", Arc::strong_count(&self.scheduler));
        // println!("lamellae: {:?}", Arc::strong_count(&self.lamellae));
        // println!("arch: {:?}", Arc::strong_count(&self.arch));
        // println!(
        //     "world_counters: {:?}",
        //     Arc::strong_count(&self.world_counters)
        // );
        // println!("removing {:?} ", self.team_hash);
        // self.lamellae.comm().free(self.remote_ptr_alloc.clone());
        // println!("Lamellae Cnt: {:?}", Arc::strong_count(&self.lamellae));
        // println!("scheduler Cnt: {:?}", Arc::strong_count(&self.scheduler));
        // println!("LamellarTeamRT dropped {:?}", self.team_hash);
        // unsafe {
        //     for duration in crate::SERIALIZE_TIMER.iter() {
        //         println!("Serialize: {:?}", duration.load(Ordering::SeqCst));
        //     }
        //     for duration in crate::SERIALIZE_SIZE_TIMER.iter() {
        //         println!("Serialize Time: {:?}", duration.load(Ordering::SeqCst));
        //     }
        //     for duration in crate::DESERIALIZE_TIMER.iter() {
        //         println!("Deserialize: {:?}", duration.load(Ordering::SeqCst));
        //     }
        // }
        // println!("LamellarTeamRT dropped");
    }
}

impl Drop for LamellarTeam {
    #[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
        // println!("team handle dropping {:?}", self.team.team_hash);
        // println!("arch: {:?}", Arc::strong_count(&self.team.arch));
        // self.team.print_cnt();
        if self.panic.load(Ordering::SeqCst) == 0 {
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
                    // println!("removing {:?} ", self.team.id);
                    parent.sub_teams.write().remove(&self.team.id);
                    let team_ptr: *const LamellarTeamRT =
                        unsafe { *self.team.remote_ptr_alloc.as_ptr() };
                    unsafe {
                        let arc_team = Arc::from_raw(team_ptr);
                        // println!("arc_team: {:?}", Arc::strong_count(&arc_team));
                        Pin::new_unchecked(arc_team); //allows us to drop the inner team
                    }
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
                // self.team.print_cnt();
            }
        } else {
            assert!(self.panic.load(Ordering::SeqCst) == 2);
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
        // println!("team: {:?}", Arc::strong_count(&self.team));
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
//         lamellaes.insert(lamellae.comm().backend(), lamellae.get_am());
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
//         lamellaes.insert(lamellae.comm().backend(), lamellae.get_am());
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

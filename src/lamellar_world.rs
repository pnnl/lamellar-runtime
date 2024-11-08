use crate::barrier::BarrierHandle;
use crate::lamellae::{create_lamellae, Backend, Lamellae, LamellaeComm, LamellaeInit};
use crate::lamellar_arch::LamellarArch;
use crate::lamellar_env::LamellarEnv;
use crate::lamellar_team::{LamellarTeam, LamellarTeamRT};
use crate::memregion::handle::{FallibleSharedMemoryRegionHandle,SharedMemoryRegionHandle};
use crate::memregion::{
    one_sided::OneSidedMemoryRegion,  Dist, RemoteMemoryRegion,
};
use crate::scheduler::{create_scheduler, ExecutorType, LamellarTask};
use crate::{active_messaging::*, config};
// use log::trace;

//use tracing::*;

use futures_util::future::join_all;
use futures_util::Future;
use parking_lot::RwLock;
use pin_weak::sync::PinWeak;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

lazy_static! {
    pub(crate) static ref LAMELLAES: RwLock<HashMap<Backend, Arc<Lamellae>>> =
        RwLock::new(HashMap::new());
    pub(crate) static ref INIT: AtomicBool = AtomicBool::new(false);
    pub(crate) static ref MAIN_THREAD: std::thread::ThreadId = std::thread::current().id();
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

impl ActiveMessaging for LamellarWorld {
    type SinglePeAmHandle<R: AmDist> = AmHandle<R>;
    type MultiAmHandle<R: AmDist> = MultiAmHandle<R>;
    type LocalAmHandle<L> = LocalAmHandle<L>;
    //#[tracing::instrument(skip_all)]
    fn exec_am_all<F>(&self, am: F) -> Self::MultiAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        self.team.exec_am_all(am)
    }
    //#[tracing::instrument(skip_all)]
    fn exec_am_pe<F>(&self, pe: usize, am: F) -> Self::SinglePeAmHandle<F::Output>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        assert!(pe < self.num_pes(), "invalid pe: {:?}", pe);
        self.team.exec_am_pe(pe, am)
    }
    //#[tracing::instrument(skip_all)]
    fn exec_am_local<F>(&self, am: F) -> Self::LocalAmHandle<F::Output>
    where
        F: LamellarActiveMessage + LocalAM + 'static,
    {
        self.team.exec_am_local(am)
    }
    //#[tracing::instrument(skip_all)]
    fn wait_all(&self) {
        self.team.wait_all();
    }
    fn await_all(&self) -> impl std::future::Future<Output = ()> + Send {
        self.team.await_all()
    }
    //#[tracing::instrument(skip_all)]
    fn barrier(&self) {
        self.team.barrier();
    }

    fn async_barrier(&self) -> BarrierHandle {
        self.team.async_barrier()
    }

    fn spawn<F>(&self, f: F) -> LamellarTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        self.team_rt.scheduler.spawn_task(
            f,
            vec![
                self.team_rt.world_counters.clone(),
                self.team_rt.team_counters.clone(),
            ],
        )
    }

    fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future,
    {
        // trace_span!("block_on").in_scope(||
        self.team_rt.scheduler.block_on(f)
        // )
    }

    fn block_on_all<I>(&self, iter: I) -> Vec<<<I as IntoIterator>::Item as Future>::Output>
    where
        I: IntoIterator,
        <I as IntoIterator>::Item: Future + Send + 'static,
        <<I as IntoIterator>::Item as Future>::Output: Send,
    {
        // trace_span!("block_on_all").in_scope(||
        self.team_rt
            .scheduler
            .block_on(join_all(iter.into_iter().map(|task| {
                self.team_rt.scheduler.spawn_task(
                    task,
                    vec![
                        self.team_rt.world_counters.clone(),
                        self.team_rt.team_counters.clone(),
                    ],
                )
            })))
        // )
    }
}

impl RemoteMemoryRegion for LamellarWorld {
    //#[tracing::instrument(skip_all)]
    fn try_alloc_shared_mem_region<T: Dist>(&self, size: usize) -> FallibleSharedMemoryRegionHandle<T> {
        self.team.try_alloc_shared_mem_region::<T>(size)
    }

    //#[tracing::instrument(skip_all)]
    fn alloc_shared_mem_region<T: Dist>(&self, size: usize) -> SharedMemoryRegionHandle<T> {
        self.team.alloc_shared_mem_region::<T>(size)
    }

    //#[tracing::instrument(skip_all)]
    fn try_alloc_one_sided_mem_region<T: Dist>(
        &self,
        size: usize,
    ) -> Result<OneSidedMemoryRegion<T>, anyhow::Error> {
        self.team.try_alloc_one_sided_mem_region::<T>(size)
    }

    //#[tracing::instrument(skip_all)]
    fn alloc_one_sided_mem_region<T: Dist>(
        &self,
        size: usize,
    ) -> OneSidedMemoryRegion<T> {
        self.team.alloc_one_sided_mem_region::<T>(size)
    }
}

impl LamellarWorld {
    #[doc(alias("One-sided", "onesided"))]
    /// Returns the id of this PE (roughly equivalent to MPI Rank)
    ///
    /// # One-sided Operation
    /// The result is returned only on the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::active_messaging::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    ///```
    //#[tracing::instrument(skip_all)]
    pub fn my_pe(&self) -> usize {
        self.my_pe
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Returns nummber of PE's in this execution
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
    ///```
    //#[tracing::instrument(skip_all)]
    pub fn num_pes(&self) -> usize {
        self.num_pes
    }

    #[doc(hidden)]
    #[allow(non_snake_case)]
    //#[tracing::instrument(skip_all)]
    pub fn MB_sent(&self) -> f64 {
        let mut sent = vec![];
        for (_backend, lamellae) in LAMELLAES.read().iter() {
            sent.push(lamellae.MB_sent());
        }
        sent[0]
    }

    #[doc(alias = "Collective")]
    /// Create a team containing any number of pe's from the world using the provided LamellarArch (layout)
    ///
    /// # Collective Operation
    /// Requrires all PEs present within the world to enter the call otherwise deadlock will occur.
    /// Note that this *does* include the PEs that will not exist within the new team.
    ///
    /// # Examples
    ///```
    /// use lamellar::active_messaging::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let num_pes = world.num_pes();
    ///
    /// let even_pes = world.create_team_from_arch(StridedArch::new(
    ///    0,                                      // start pe
    ///    2,                                      // stride
    ///    (num_pes as f64 / 2.0).ceil() as usize, //num_pes in team
    /// )).expect("PE in world team");
    ///```
    //#[tracing::instrument(skip_all)]
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

    #[doc(alias("One-sided", "onesided"))] //#[tracing::instrument(skip_all)]
    /// Returns the underlying [LamellarTeam] for this world
    /// # Examples
    ///```
    /// use lamellar::active_messaging::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let team = world.team();
    ///```
    pub fn team(&self) -> Arc<LamellarTeam> {
        self.team.clone()
    }

    #[doc(alias("One-sided", "onesided"))]
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
    /// let num_threads = world.num_threads_per_pe();
    ///```
    pub fn num_threads_per_pe(&self) -> usize {
        self.team.num_threads_per_pe()
    }

    // pub fn flush(&self) {
    //     self.team_rt.flush();
    // }
}

impl LamellarEnv for LamellarWorld {
    fn my_pe(&self) -> usize {
        self.my_pe
    }
    fn num_pes(&self) -> usize {
        self.num_pes
    }
    fn num_threads_per_pe(&self) -> usize {
        self.team.num_threads_per_pe()
    }
    fn world(&self) -> Arc<LamellarTeam> {
        // println!("LamellarWorld world");
        self.team.clone()
    }
    fn team(&self) -> Arc<LamellarTeam> {
        // println!("LamellarWorld team");
        self.team.clone()
    }
}

impl Clone for LamellarWorld {
    //#[tracing::instrument(skip_all)]
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

impl Drop for LamellarWorld {
    //#[tracing::instrument(skip_all)]
    fn drop(&mut self) {
        let cnt = self.ref_cnt.fetch_sub(1, Ordering::SeqCst);
        if cnt == 1 {
            // println!("[{:?}] world dropping", self.my_pe);
            // println!(
            //     "lamellae cnt {:?} sched cnt {:?}",
            //     Arc::strong_count(&self.team_rt.lamellae),
            //     Arc::strong_count(&self.team_rt.scheduler)
            // );
            // println!(
            //     "in team destroy mype: {:?} cnt: {:?} {:?}",
            //     self.my_pe,
            //     self._counters.send_req_cnt.load(Ordering::SeqCst),
            //     self._counters.outstanding_reqs.load(Ordering::SeqCst),
            // );
            // let team_rt = unsafe {Pin::into_inner_unchecked(self.team_rt.clone())};
            // println!(
            //     "team: {:?} team_rt: {:?}",
            //     Arc::strong_count(&self.team),
            //     unsafe { Arc::strong_count(&team_rt) }
            // );
            // let team_rt = unsafe {Pin::into_inner_unchecked(self.team_rt.clone())};
            // println!(
            //     "team: {:?} team_rt: {:?}",
            //     Arc::strong_count(&self.team),
            //     unsafe { Arc::strong_count(&team_rt) }
            // );
            if self.team.panic.load(Ordering::SeqCst) < 2 {
                // println!("wait for all ams to complete");
                // println!(
                //     "in team destroy mype: {:?} cnt: {:?} {:?}",
                //     self.my_pe,
                //     self._counters.send_req_cnt.load(Ordering::SeqCst),
                //     self._counters.outstanding_reqs.load(Ordering::SeqCst),
                // );
                self.barrier();
                self.wait_all();
                // println!(
                //     "in team destroy mype: {:?} cnt: {:?} {:?}",
                //     self.my_pe,
                //     self._counters.send_req_cnt.load(Ordering::SeqCst),
                //     self._counters.outstanding_reqs.load(Ordering::SeqCst),
                // );
                // println!("team_rt {:?}",Arc::strong_count(&team_rt));
                // println!("wait for everyone else to finish");
                self.barrier();
                // println!(
                //     "in team destroy mype: {:?} cnt: {:?} {:?}",
                //     self.my_pe,
                //     self._counters.send_req_cnt.load(Ordering::SeqCst),
                //     self._counters.outstanding_reqs.load(Ordering::SeqCst),
                // );
            }
            // println!(
            //     "in team destroy mype: {:?} cnt: {:?} {:?}",
            //     self.my_pe,
            //     self._counters.send_req_cnt.load(Ordering::SeqCst),
            //     self._counters.outstanding_reqs.load(Ordering::SeqCst),
            // );
            // println!("destroy team");
            self.team_rt.destroy();
            LAMELLAES.write().clear();
            // println!(
            //     "in team destroy mype: {:?} cnt: {:?} {:?}",
            //     self.my_pe,
            //     self._counters.send_req_cnt.load(Ordering::SeqCst),
            //     self._counters.outstanding_reqs.load(Ordering::SeqCst),
            // );

            // println!(
            //     "team: {:?} team_rt: {:?}",
            //     Arc::strong_count(&self.team),
            //     unsafe { Arc::strong_count(&self.team_rt) }
            // );

            // println!("counters: {:?}", Arc::strong_count(&self._counters));
            // println!(
            //     "sechduler_new: {:?}",
            //     Arc::strong_count(&self.team_rt.scheduler)
            // );
            // println!("[{:?}] world dropped", self.my_pe);
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
/// use lamellar::{LamellarWorldBuilder,Backend,ExecutorType};
/// // can also use and of the module preludes
/// // use lamellar::active_messaging::prelude::*;
/// // use lamellar::array::prelude::*;
/// // use lamellar::darc::prelude::*;
/// // use lamellar::memregion::prelude::*;
///
/// let world = LamellarWorldBuilder::new()
///                             .with_lamellae(Backend::Local)
///                             .with_executor(ExecutorType::LamellarWorkStealing)
///                             .build();
///```
#[derive(Debug)]
pub struct LamellarWorldBuilder {
    primary_lamellae: Backend,
    // secondary_lamellae: HashSet<Backend>,
    executor: ExecutorType,
    num_threads: usize,
}

impl LamellarWorldBuilder {
    #[doc(alias = "Collective")]
    /// Construct a new lamellar world builder
    ///
    /// # Collective Operation
    /// While simply calling `new` is not collective by itself (i.e. there is no internal barrier that would deadlock,
    /// as the remote fabric is not initiated until after a call to `build`), it is necessary that the same
    /// parameters are used by all PEs that will exist in the world.
    ///
    /// # Examples
    ///
    ///```
    /// use lamellar::{LamellarWorldBuilder,Backend,ExecutorType};
    /// // can also use and of the module preludes
    /// // use lamellar::active_messaging::prelude::*;
    /// // use lamellar::array::prelude::*;
    /// // use lamellar::darc::prelude::*;
    /// // use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new()
    ///                             .with_lamellae(Backend::Local)
    ///                             .with_executor(ExecutorType::LamellarWorkStealing)
    ///                             .build();
    ///```
    //#[tracing::instrument(skip_all)]
    pub fn new() -> LamellarWorldBuilder {
        // simple_logger::init().unwrap();
        // trace!("New world builder");
        let executor = match config().executor.as_str(){
            "tokio" => {
                #[cfg(not(feature = "tokio-executor"))]
                {
                    panic!("[LAMELLAR WARNING]: tokio-executor selected but it is not enabled, either recompile lamellar with --features tokio-executor, or set LAMELLAR_EXECUTOR to one of 'lamellar' or 'async_std'");
                }
                #[cfg(feature = "tokio-executor")]
                ExecutorType::Tokio
            }
            "async_std" => ExecutorType::AsyncStd,
            "lamellar" => ExecutorType::LamellarWorkStealing,
            "lamellar2" => ExecutorType::LamellarWorkStealing2,
            "lamellar3" => ExecutorType::LamellarWorkStealing3,
            _ => panic!("[LAMELLAR WARNING]: unexpected executor type, please set LAMELLAR_EXECUTOR to one of the following 'lamellar', 'async_std', or (if tokio-executor feature is enabled, 'tokio'.")
        };

        let num_threads = config().threads;
        LamellarWorldBuilder {
            primary_lamellae: Default::default(),
            // secondary_lamellae: HashSet::new(),
            executor: executor,
            num_threads: num_threads,
        }
    }

    #[doc(alias = "Collective")]
    /// Specify the lamellae backend to use for this execution
    ///
    /// # Collective Operation
    /// While simply calling `with_lamellae` is not collective by itself (i.e. there is no internal barrier that would deadlock,
    /// as the remote fabric is not initiated until after a call to `build`), it is necessary that the same
    /// parameters are used by all PEs that will exist in the world.
    ///
    /// # Examples
    ///
    ///```
    /// use lamellar::{LamellarWorldBuilder,Backend};
    ///
    /// let builder = LamellarWorldBuilder::new()
    ///                             .with_lamellae(Backend::Local);
    ///```
    //#[tracing::instrument(skip_all)]
    pub fn with_lamellae(mut self, lamellae: Backend) -> LamellarWorldBuilder {
        self.primary_lamellae = lamellae;
        self
    }

    // pub fn add_lamellae(mut self, lamellae: Backend) -> LamellarWorldBuilder {
    //     self.secondary_lamellae.insert(lamellae);
    //     self
    // }

    #[doc(alias = "Collective")]
    /// Specify the executor to use for this execution
    ///
    /// # Collective Operation
    /// While simply calling `with_executor` is not collective by itself (i.e. there is no internal barrier that would deadlock,
    /// as the remote fabric is not initiated until after a call to `build`), it is necessary that the same
    /// parameters are used by all PEs that will exist in the world.
    ///
    /// # Examples
    ///
    ///```
    /// use lamellar::{LamellarWorldBuilder,ExecutorType};
    ///
    /// let builder = LamellarWorldBuilder::new()
    ///                             .with_executor(ExecutorType::LamellarWorkStealing);
    ///```
    // #[tracing::instrument(skip_all)]
    pub fn with_executor(mut self, sched: ExecutorType) -> LamellarWorldBuilder {
        self.executor = sched;
        self
    }

    #[doc(alias = "One-sided")]
    /// Specify the number of threads per PE to use for this execution (the Main thread is included in this count)
    ///
    /// # Onesided Operation
    /// While calling `with_num_workers` is onesided and will not cause any issues if different PEs use different numbers of
    /// workers(threads), performance is likely to be inconsistent from PE to PE depending on the number of threads
    ///
    /// # Examples
    ///
    ///```
    /// use lamellar::{LamellarWorldBuilder,ExecutorType};
    ///
    /// let builder = LamellarWorldBuilder::new()
    ///                             .set_num_threads(10);
    ///```
    //#[tracing::instrument(skip_all)]
    pub fn set_num_threads(mut self, num_threads: usize) -> LamellarWorldBuilder {
        self.num_threads = num_threads;
        self
    }

    #[doc(alias = "Collective")]
    /// Instantiate a LamellarWorld object
    ///
    /// # Collective Operation
    /// Requires all PEs that will be in the world to enter the call otherwise deadlock will occur (i.e. internal barriers are called)
    ///
    /// # Examples
    ///
    ///```
    /// use lamellar::{LamellarWorldBuilder,Backend,ExecutorType};
    ///
    /// let world = LamellarWorldBuilder::new()
    ///                             .with_lamellae(Backend::Local)
    ///                             .with_executor(ExecutorType::LamellarWorkStealing)
    ///                             .build();
    ///```
    //#[tracing::instrument(skip_all)]
    pub fn build(self) -> LamellarWorld {
        // let mut timer = std::time::Instant::now();
        assert_eq!(INIT.fetch_or(true, Ordering::SeqCst), false, "ERROR: Building more than one world is not allowed, you may want to consider cloning or creating a reference to first instance");
        // let teams = Arc::new(RwLock::new(HashMap::new()));
        // println!("{:?}: INIT", timer.elapsed());

        assert!(std::thread::current().id() == *MAIN_THREAD);

        // timer = std::time::Instant::now();
        let mut lamellae_builder = create_lamellae(self.primary_lamellae);
        // println!("{:?}: init_lamellae", timer.elapsed());

        // timer = std::time::Instant::now();
        let (my_pe, num_pes) = lamellae_builder.init_fabric();
        // println!("{:?}: init_fabric", timer.elapsed());

        // timer = std::time::Instant::now();

        // we delay building the scheduler until we know the number of PEs (which is used for message aggregation)
        // this could be lazyily provided but this is easy enough to do here
        let panic = Arc::new(AtomicU8::new(0));
        let sched_new = Arc::new(create_scheduler(
            self.executor,
            num_pes,
            self.num_threads,
            panic.clone(),
        ));
        // println!(
        //     " create_scheduler  cnt {:?}",
        //     // timer.elapsed(),
        //     Arc::strong_count(&sched_new)
        // );

        // timer = std::time::Instant::now();
        let lamellae = lamellae_builder.init_lamellae(sched_new.clone());
        // println!("{:?}: init_lamellae", timer.elapsed());

        // timer = std::time::Instant::now();
        let counters = Arc::new(AMCounters::new());
        // println!("{:?}: init_counters", timer.elapsed());

        // timer = std::time::Instant::now();
        lamellae.barrier();
        // println!("{:?}: lamellae barrier", timer.elapsed());

        // timer = std::time::Instant::now();
        let team_rt = LamellarTeamRT::new(
            num_pes,
            my_pe,
            sched_new.clone(),
            counters.clone(),
            lamellae.clone(),
            panic.clone(),
            // teams.clone(),
        );
        // println!("{:?}: init_team_rt", timer.elapsed());

        // timer = std::time::Instant::now();
        let world = LamellarWorld {
            team: LamellarTeam::new(None, team_rt.clone(), false),
            team_rt: team_rt.clone(),
            // teams: teams.clone(),
            _counters: counters,
            my_pe: my_pe,
            num_pes: num_pes,
            ref_cnt: Arc::new(AtomicUsize::new(1)),
        };
        // println!("{:?}: init_world", timer.elapsed());

        // timer = std::time::Instant::now();
        LAMELLAES
            .write()
            .insert(lamellae.backend(), lamellae.clone());
        // println!("{:?}: insert lamellae", timer.elapsed());

        // timer = std::time::Instant::now();
        let weak_rt = PinWeak::downgrade(team_rt.clone());
        // println!("{:?}: weak_rt", timer.elapsed());

        // timer = std::time::Instant::now();
        std::panic::set_hook(Box::new(move |panic_info| {
            let backtrace = std::backtrace::Backtrace::capture();
            // panics.lock().push(format!("{panic_info}"));
            // for p in panics.lock().iter(){
            //     println!("{p}");
            // }
            println!("{panic_info}");
            println!("{backtrace:#?}");

            if let Some(rt) = weak_rt.upgrade() {
                println!("trying to shutdown Lamellar Runtime");
                rt.force_shutdown();
            } else {
                println!("unable to shutdown Lamellar Runtime gracefully");
            }
            // std::process::exit(1);
        }));
        // println!("{:?}: set_hook", timer.elapsed());

        // timer = std::time::Instant::now();
        world.barrier();
        // println!("{:?}: world barrier", timer.elapsed());
        world
    }
}

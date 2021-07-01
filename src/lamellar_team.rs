// use crate::active_messaging::*; //{ActiveMessaging,AMCounters,Cmd,Msg,LamellarAny,LamellarLocal};
use crate::active_messaging::*;
// use crate::lamellae::{AllocationType,Lamellae};
use crate::barrier::Barrier;
use crate::lamellae::{AllocationType, Lamellae, LamellaeComm};
use crate::lamellar_arch::{GlobalArch, IdError, LamellarArch, LamellarArchEnum, LamellarArchRT};
// #[cfg(feature = "experimental")]
// use crate::lamellar_array::{LamellarArray, LamellarSubArray};
use crate::lamellar_memregion::{
    LamellarLocalMemoryRegion, LamellarMemoryRegion, MemoryRegion, RegisteredMemoryRegion,
    RemoteMemoryRegion,
};
use crate::lamellar_request::{AmType, LamellarRequest, LamellarRequestHandle};
// use crate::schedulers::SchedulerQueue;
use crate::scheduler::{Scheduler, SchedulerQueue};
#[cfg(feature = "nightly")]
use crate::utils::ser_closure;

use log::trace;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
// use std::any;
use lamellar_prof::*;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

// to manage team lifetimes properly we need a seperate user facing handle that contains a strong link to the inner team.
// this outer handle has a lifetime completely tied to whatever the user wants
// when the outer handle is dropped, we do the appropriate barriers and then remove the inner team from the runtime data structures
// this should allow for the inner team to persist while at least one user handle exists in the world.

pub struct LamellarTeam {
    pub(crate) world: Option<Arc<LamellarTeam>>,
    pub(crate) team: Arc<LamellarTeamRT>,
    pub(crate) teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeam>>>>, //need a reference to this so we can clean up after dropping the team
}

//#[prof]
impl LamellarTeam {
    #[allow(dead_code)]
    pub fn get_pes(&self) -> Vec<usize> {
        self.team.arch.team_iter().collect::<Vec<usize>>()
    }
    pub fn num_pes(&self) -> usize {
        self.team.arch.num_pes()
    }
    pub fn world_pe_id(&self) -> usize {
        self.team.world_pe
    }
    pub fn team_pe_id(&self) -> Result<usize, IdError> {
        self.team.arch.team_pe(self.team.world_pe)
    }
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
            let team = Arc::new(LamellarTeam {
                world: Some(world),
                team: team,
                teams: parent.teams.clone(),
            });
            parent
                .teams
                .write()
                .insert(team.team.team_hash, Arc::downgrade(&team));
            Some(team)
        } else {
            None
        }
    }
    pub fn print_arch(&self) {
        self.team.print_arch()
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
    fn wait_all(&self) {
        self.team.wait_all();
    }

    fn barrier(&self) {
        self.team.barrier();
    }

    fn exec_am_all<F>(&self, am: F) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + Send + Sync + 'static,
    {
        trace!("[{:?}] team exec am all request", self.team.world_pe);
        self.team.exec_am_all(self.clone(), am)
    }

    fn exec_am_pe<F>(
        &self,
        pe: usize,
        am: F,
    ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + Send + Sync + 'static,
    {
        self.team.exec_am_pe(self.clone(), pe, am)
    }

    fn exec_am_local<F>(&self, am: F) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: LamellarActiveMessage + LocalAM + Send + Sync + 'static,
    {
        self.team.exec_am_local(self.clone(), am)
    }
}

pub struct LamellarTeamRT {
    #[allow(dead_code)]
    world: Option<Arc<LamellarTeamRT>>,
    parent: Option<Arc<LamellarTeamRT>>,
    sub_teams: RwLock<HashMap<usize, Arc<LamellarTeamRT>>>,
    mem_regions: RwLock<HashMap<usize, Box<dyn MemoryRegion + Sync + Send>>>,
    pub(crate) scheduler: Arc<Scheduler>,
    pub(crate) lamellae: Arc<Lamellae>,
    pub(crate) arch: Arc<LamellarArchRT>,
    pub(crate) world_pe: usize,
    pub(crate) num_world_pes: usize,
    pub(crate) team_pe: Result<usize, IdError>,
    pub(crate) num_pes: usize,
    team_counters: AMCounters,
    pub(crate) world_counters: Arc<AMCounters>, // can probably remove this?
    id: usize,
    sub_team_id_cnt: AtomicUsize,
    barrier: Barrier,
    dropped: LamellarMemoryRegion<usize>,
    pub(crate) team_hash: u64,
}

//#[prof]
impl Hash for LamellarTeamRT {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.team_hash.hash(state);
    }
}

//#[prof]
impl LamellarTeamRT {
    pub(crate) fn new(
        //creates a new root team
        num_pes: usize,
        world_pe: usize,
        scheduler: Arc<Scheduler>,
        world_counters: Arc<AMCounters>,
        lamellae: Arc<Lamellae>,
    ) -> LamellarTeamRT {
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
            sub_teams: RwLock::new(HashMap::new()),
            mem_regions: RwLock::new(HashMap::new()),
            scheduler: scheduler,
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
            barrier: Barrier::new(world_pe, num_pes, lamellae.clone(), arch.clone()),
            dropped: LamellarMemoryRegion::new(num_pes, lamellae.clone(), alloc.clone()),
        };
        unsafe {
            for e in team.dropped.as_mut_slice().unwrap().iter_mut() {
                *e = 0;
            }
        }
        // lamellae.get_am().barrier(); //this is a noop currently
        lamellae.barrier();
        team.barrier.barrier();
        team
    }

    pub(crate) fn destroy(&self) {
        // println!("destroying team? {:?}",self.mem_regions.read().len());
        for _lmr in self.mem_regions.read().iter() {
            // println!("lmr {:?}",_lmr);
            //TODO: i have a gut feeling we might have an issue if a mem region was destroyed on one node, but not another
            // add a barrier method that takes a message so if we are stuck in the barrier for a long time we can say that
            // this is probably mismatched frees.
            self.barrier.barrier();
        }
        self.mem_regions.write().clear();
        self.sub_teams.write().clear(); // not sure this is necessary or should be allowed? sub teams delete themselves from this map when dropped...
                                        // what does it mean if we drop a parent team while a sub_team is valid?
    }
    #[allow(dead_code)]
    pub fn get_pes(&self) -> Vec<usize> {
        self.arch.team_iter().collect::<Vec<usize>>()
    }
    pub fn world_pe_id(&self) -> usize {
        self.world_pe
    }
    pub fn team_pe_id(&self) -> Result<usize, IdError> {
        self.arch.team_pe(self.world_pe)
    }

    pub fn create_subteam_from_arch<L>(
        world: Arc<LamellarTeamRT>,
        parent: Arc<LamellarTeamRT>,
        arch: L,
    ) -> Option<Arc<LamellarTeamRT>>
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
            // println!("subteam: hash: {:?} arch{:?}",hash,archrt);
            parent.barrier();
            // println!("[{:?}][LAMELLAR] creating team: {:?}",parent.world_pe,archrt.team_iter().collect::<Vec<usize>>());

            // ------ ensure team is being constructed synchronously and in order across all pes in parent ------ //
            let parent_alloc = AllocationType::Sub(parent.arch.team_iter().collect::<Vec<usize>>());
            let temp_buf = LamellarMemoryRegion::<usize>::new(
                parent.num_pes,
                parent.lamellae.clone(),
                parent_alloc,
            );

            let temp_array =
                LamellarLocalMemoryRegion::<usize>::new(parent.num_pes, parent.lamellae.clone());
            let temp_array_slice = unsafe { temp_array.as_mut_slice().unwrap() };
            for e in temp_array_slice.iter_mut() {
                *e = 0;
            }
            unsafe {
                temp_buf.put_slice(parent.world_pe, 0, &temp_array_slice[..parent.num_pes]);
            }
            // println!("new team hash: {:?}",hash);
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
            // ------------------------------------------------------------------------------------------------- //

            for e in temp_array_slice.iter_mut() {
                *e = 0;
            }
            let num_pes = archrt.num_pes();
            let team = LamellarTeamRT {
                world: Some(world.clone()),
                parent: Some(parent.clone()),
                sub_teams: RwLock::new(HashMap::new()),
                mem_regions: RwLock::new(HashMap::new()),
                // scheduler: parent.scheduler.clone(),
                scheduler: parent.scheduler.clone(),
                // lamellae: parent.lamellae.clone(),
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
                ),
                team_hash: hash,
                dropped: temp_buf,
            };
            unsafe {
                for e in team.dropped.as_mut_slice().unwrap().iter_mut() {
                    *e = 0;
                }
            }
            let team = Arc::new(team);
            // println!("[LAMELLAR] new  team 1 {:?} {:?} {:?}",hash, team.dropped.as_slice(),temp_array_slice);
            let mut sub_teams = parent.sub_teams.write();
            sub_teams.insert(team.id, team.clone());
            parent.barrier();

            // println!("[{:?}][LAMELLAR] entering team barrier ", team.world_pe);
            team.barrier();
            // println!("[LAMELLAR] new  team 2 {:?} {:?}",hash, team.dropped.as_slice());
            parent.barrier();
            Some(team)
        } else {
            None
        }
    }

    pub fn num_pes(&self) -> usize {
        self.arch.num_pes()
    }

    #[cfg_attr(test, allow(unreachable_code), allow(unused_variables))]
    fn check_hash_vals(
        &self,
        hash: usize,
        hash_buf: &LamellarMemoryRegion<usize>,
        timeout: Duration,
    ) {
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
                        hash_buf.as_slice()
                    );
                    s = Instant::now();
                }
            }
            if *pe != hash && Instant::now() - s > timeout {
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

    fn put_dropped(&self) {
        if let Some(parent) = &self.parent {
            let temp_slice = unsafe { self.dropped.as_mut_slice().unwrap() };
            // println!("temp_slice {:?}",temp_slice);

            let my_index = parent
                .arch
                .team_pe(self.world_pe)
                .expect("invalid parent pe");
            // println!("my_index {:?}",my_index);
            temp_slice[my_index] = 1;
            for world_pe in self.arch.team_iter() {
                // println!("world_pe: {:?}",world_pe);
                if world_pe != self.world_pe {
                    // println!("putting into dropped {:?} {:?} {:?}",world_pe, my_index, & temp_slice[my_index..=my_index]);
                    unsafe {
                        self.dropped.put_slice(
                            world_pe,
                            my_index,
                            &temp_slice[my_index..=my_index],
                        );
                    }
                }
            }
        }
    }

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
    //     LamellarLocalArray::new(size, init, self.lamellae.get_rdma().clone())
    // }

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
    // impl ActiveMessaging for Arc<LamellarTeamRT> {
    fn wait_all(&self) {
        // println!(
        //     "in team wait_all team_hash: {:?} cnt: {:?} {:?}",
        //     self.team_hash,
        //     self.team_counters.send_req_cnt.load(Ordering::SeqCst),
        //     self.team_counters.outstanding_reqs.load(Ordering::SeqCst),
        //     // self.counters.recv_req_cnt.load(Ordering::SeqCst),
        // );
        let mut temp_now = Instant::now();
        while self.team_counters.outstanding_reqs.load(Ordering::SeqCst) > 0 {
            std::thread::yield_now();
            if temp_now.elapsed() > Duration::new(10, 0) {
                println!(
                    "in world wait_all mype: {:?} cnt: {:?} {:?}",
                    self.world_pe,
                    self.team_counters.send_req_cnt.load(Ordering::SeqCst),
                    self.team_counters.outstanding_reqs.load(Ordering::SeqCst),
                    // self.counters.recv_req_cnt.load(Ordering::SeqCst),
                );
                // self.lamellae.print_stats();
                temp_now = Instant::now();
            }
        }
    }

    fn barrier(&self) {
        self.barrier.barrier();
    }

    fn exec_am_all<F>(
        &self,
        team: Arc<LamellarTeam>,
        am: F,
    ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: RemoteActiveMessage + LamellarAM + Send + Sync + 'static,
    {
        trace!("[{:?}] team exec am all request", self.world_pe);
        let (my_req, ireq) = LamellarRequestHandle::new(
            self.num_pes,
            AmType::RegisteredFunction,
            self.arch.clone(),
            self.team_counters.outstanding_reqs.clone(),
            self.world_counters.outstanding_reqs.clone(),
            self.team_hash,
            team.clone(),
        );

        self.world_counters.add_send_req(self.num_pes);
        self.team_counters.add_send_req(self.num_pes);
        // let my_any: LamellarAny =Box::new(Box::new(am) as LamellarBoxedAm);
        let func: LamellarArcAm = Arc::new(am);
        let world = if let Some(world) = &team.world {
            world.clone()
        } else {
            team.clone()
        };
        self.scheduler.submit_req_new(
            self.world_pe,
            None,
            ExecType::Am(Cmd::Exec),
            my_req.id,
            LamellarFunc::Am(func),
            self.lamellae.clone(),
            world,
            team.clone(),
            self.team_hash,
            Some(ireq),
        );
        Box::new(my_req)
    }

    fn exec_am_pe<F>(
        &self,
        team: Arc<LamellarTeam>,
        pe: usize,
        am: F,
    ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: RemoteActiveMessage + LamellarAM + Send + Sync + 'static,
    {
        prof_start!(pre);
        // println!("[{:?}] team exec am pe request {:?}", self.world_pe,self.team_hash);
        assert!(pe < self.arch.num_pes());
        prof_end!(pre);
        prof_start!(req);
        let (my_req, ireq) = LamellarRequestHandle::new(
            1,
            AmType::RegisteredFunction,
            self.arch.clone(),
            self.team_counters.outstanding_reqs.clone(),
            self.world_counters.outstanding_reqs.clone(),
            self.team_hash,
            team.clone(),
        );
        prof_end!(req);
        prof_start!(counters);
        // println!("world_cnt");
        self.world_counters.add_send_req(1);
        // println!("team_cnt");
        self.team_counters.add_send_req(1);
        prof_end!(counters);
        prof_start!(any);
        let func: LamellarArcAm = Arc::new(am);
        prof_end!(any);
        prof_start!(sub);
        let world = if let Some(world) = &team.world {
            world.clone()
        } else {
            team.clone()
        };
        self.scheduler.submit_req_new(
            self.world_pe,
            Some(self.arch.world_pe(pe).expect("pe not member of team")),
            ExecType::Am(Cmd::Exec),
            my_req.id,
            LamellarFunc::Am(func),
            self.lamellae.clone(),
            world,
            team.clone(),
            self.team_hash,
            Some(ireq),
        );
        prof_end!(sub);
        // println!(
        //     "in team exec_am_pe team_hash: {:?} cnt: {:?} {:?}",
        //     self.team_hash,
        //     self.team_counters.send_req_cnt.load(Ordering::SeqCst),
        //     self.team_counters.outstanding_reqs.load(Ordering::SeqCst),
        //     // self.counters.recv_req_cnt.load(Ordering::SeqCst),
        // );
        Box::new(my_req)
    }

    fn exec_am_local<F>(
        &self,
        team: Arc<LamellarTeam>,
        am: F,
    ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: LamellarActiveMessage + LocalAM + Send + Sync + 'static,
    {
        prof_start!(pre);
        // println!("[{:?}] team exec am local request", self.world_pe);
        prof_end!(pre);
        prof_start!(req);
        let (my_req, ireq) = LamellarRequestHandle::new(
            1,
            AmType::RegisteredFunction,
            self.arch.clone(),
            self.team_counters.outstanding_reqs.clone(),
            self.world_counters.outstanding_reqs.clone(),
            self.team_hash,
            team.clone(),
        );
        prof_end!(req);
        prof_start!(counters);
        self.world_counters.add_send_req(1);
        self.team_counters.add_send_req(1);
        prof_end!(counters);
        prof_start!(any);
        let func: LamellarArcLocalAm = Arc::new(am);
        prof_end!(any);
        prof_start!(sub);
        let world = if let Some(world) = &team.world {
            world.clone()
        } else {
            team.clone()
        };
        self.scheduler.submit_req_new(
            self.world_pe,
            Some(self.world_pe),
            ExecType::Am(Cmd::Exec),
            my_req.id,
            LamellarFunc::LocalAm(func),
            self.lamellae.clone(),
            world,
            team.clone(),
            self.team_hash,
            Some(ireq),
        );
        prof_end!(sub);
        Box::new(my_req)
    }
}

//#feature gated closures for those with nightly
#[cfg(feature = "nightly")]
use crate::active_messaging::remote_closures::{lamellar_closure, lamellar_local, ClosureRet};
#[cfg(feature = "nightly")]
//#[prof]
impl RemoteClosures for LamellarTeamRT {
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
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        trace!("[{:?}] team exec closure all request", self.world_pe);
        let (my_req, ireq) = LamellarRequestHandle::new(
            self.num_pes,
            AmType::RemoteClosure,
            self.arch.clone(),
            self.team_counters.outstanding_reqs.clone(),
            self.world_counters.outstanding_reqs.clone(),
        );
        let msg = Msg {
            cmd: ExecType::Closure(Cmd::Exec),
            src: self.world_pe as u16,
            req_id: my_req.id,
            team_id: self.id,
            return_data: true,
        };
        self.world_counters.add_send_req(self.num_pes);
        self.team_counters.add_send_req(self.num_pes);
        let my_any: LamellarAny = Box::new((lamellar_local(func.clone()), lamellar_closure(func)));
        self.scheduler.submit_req(
            self.world_pe,
            None,
            msg,
            ireq,
            my_any,
            self.lamellae.get_am(),
            self.team_hash,
        );
        Box::new(my_req)
    }

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
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        trace!("[{:?}] team exec_closure_pe [{:?}]", self.world_pe, pe);
        assert!(pe < self.arch.num_pes());
        let (my_req, mut ireq) = LamellarRequestHandle::new(
            1,
            AmType::RemoteClosure,
            self.arch.clone(),
            self.team_counters.outstanding_reqs.clone(),
            self.world_counters.outstanding_reqs.clone(),
        );
        let msg = Msg {
            cmd: ExecType::Closure(Cmd::Exec),
            src: self.world_pe as u16,
            req_id: my_req.id,
            team_id: self.id,
            return_data: true,
        };
        self.world_counters.add_send_req(1);
        self.team_counters.add_send_req(1);
        ireq.start = Instant::now();
        let my_any: LamellarAny = if self.world_pe == pe {
            Box::new(lamellar_local(func))
        } else {
            Box::new(lamellar_closure(func))
        };
        self.scheduler.submit_req(
            self.world_pe,
            Some(pe),
            msg,
            ireq,
            my_any,
            self.lamellae.get_am(),
            self.team_hash,
        );
        Box::new(my_req)
    }

    fn exec_closure_on_return<
        F: FnOnce() -> T + serde::ser::Serialize + serde::de::DeserializeOwned + 'static,
        T: std::any::Any + serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone,
    >(
        &self,
        func: F,
    ) -> ClosureRet {
        ClosureRet {
            data: ser_closure(
                FnOnce!([func] move||-> (RetType,Option<std::vec::Vec<u8>>) {
                    let closure: F = func;
                    let ret: T = closure();
                    let box_any_ret: Box<dyn std::any::Any> = Box::new(ret.clone());
                    if let Some(_x) = box_any_ret.downcast_ref::<()>(){
                        // println!("unit return");
                        (RetType::Unit,None)
                    }
                    else{
                        println!("warning: return data from exec_on_return not yet supported -- data not reachable");
                        (RetType::Unit,None)
                    }
                }),
            ),
        }
    }
}

//#[prof]
impl RemoteMemoryRegion for LamellarTeam {
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
        // println!("alloc_mem_reg: {:?}",self.world_pe);
        self.team.alloc_shared_mem_region(size)
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
        // println!("alloc_mem_reg: {:?}",self.world_pe);
        self.team.alloc_local_mem_region(size)
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
        self.team.free_shared_memory_region(region)
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
        self.team.free_local_memory_region(region)
    }
}

//#[prof]
impl RemoteMemoryRegion for LamellarTeamRT {
    /// allocate a remote memory region from the asymmetric heap
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
        self.barrier.barrier();
        let lmr = if self.num_world_pes == self.num_pes {
            LamellarMemoryRegion::new(size, self.lamellae.clone(), AllocationType::Global)
        } else {
            LamellarMemoryRegion::new(
                size,
                self.lamellae.clone(),
                AllocationType::Sub(self.arch.team_iter().collect::<Vec<usize>>()),
            )
        };
        self.mem_regions
            .write()
            .insert(lmr.id(), Box::new(lmr.clone()));
        self.barrier.barrier();
        lmr
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
        let llmr = LamellarLocalMemoryRegion::new(size, self.lamellae.clone());
        self.mem_regions
            .write()
            .insert(llmr.id(), Box::new(llmr.clone()));
        // println!("alloc_mem_reg: {:?}",llmr);
        llmr
    }

    /// release a remote memory region from the symmetric heap
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
        self.barrier.barrier();
        self.mem_regions.write().remove(&region.id());
    }

    /// release a remote memory region from the symmetric heap
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
        // println!("free_mem_reg: {:?}",region);
        self.mem_regions.write().remove(&region.id());
    }
}

//#[prof]
impl Drop for LamellarTeamRT {
    fn drop(&mut self) {
        // println!("LamellarTeamRT dropping");
        // if let Some(parent) = &self.parent{
        //     println!("parent: {:?}",Arc::strong_count(&parent));
        // }
        // let sub_teams = self.sub_teams.read();
        // for (k,team) in sub_teams.iter(){
        //     println!("subteam: {:?} {:?}",k,Arc::strong_count(&team));
        // }

        // println!("sechduler_new: {:?}",Arc::strong_count(&self.scheduler));
        // println!("lamellae: {:?}",Arc::strong_count(&self.lamellae));
        // println!("arch: {:?}",Arc::strong_count(&self.arch));
        // println!("world_counters: {:?}",Arc::strong_count(&self.world_counters));
        // println!("LamellarTeamRT dropped");
        if let None = &self.parent {
            self.lamellae.shutdown(); //do we want to shutdown the scheduler first?
            self.scheduler.shutdown();
        }

        // println!("LamellarTeamRT dropped");
    }
}
//         // println!("[{:?}] team dropping {:?}", self.world_pe, self.get_pes());
//         // self.wait_all();
//         // self.barrier();
//         // self.put_dropped();
//         // if let Ok(my_index) = self.arch.team_pe_id(&self.world_pe){
//         //     self.drop_barrier();
//         // }
//         // if let Some(parent) = &self.parent{
//         //     parent.sub_teams.write().remove(&self.id);
//         // }
//         // println!("[{:?}] team dropped {:?}", self.world_pe, self.get_pes());
//         // self.barrier1.delete(self.lamellae.clone());
//         // self.barrier2.delete(self.lamellae.clone());
//         // self.barrier3.delete(self.lamellae.clone());
//         // if let Some(parent) = &self.parent{
//         //     parent.sub_teams.write().remove(&self.id);
//         // }
//         //TODO: what happends when we have sub_teams present?
//         // does the recursive drop manage it properly for us or
//         // do we need to manually remove those teams from the map?
//     }
// }

//#[prof]
impl Drop for LamellarTeam {
    fn drop(&mut self) {
        // println!("team handle dropping");
        if let Some(parent) = &self.team.parent {
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

            parent.sub_teams.write().remove(&self.team.id);
        }
        self.teams.write().remove(&self.team.team_hash);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lamellae::{create_lamellae, Backend, Lamellae, LamellaeAM};
    use crate::lamellar_arch::StridedArch;
    use crate::schedulers::{create_scheduler, Scheduler, SchedulerType};
    use std::collections::BTreeMap;
    #[test]
    fn multi_team_unique_hash() {
        let num_pes = 10;
        let world_pe = 0;
        let mut lamellae = create_lamellae(Backend::Local);
        let teams = Arc::new(RwLock::new(HashMap::new()));

        let mut sched = create_scheduler(
            SchedulerType::WorkStealing,
            num_pes,
            world_pe,
            teams.clone(),
        );
        lamellae.init_lamellae(sched.get_queue().clone());
        let lamellae = Arc::new(lamellae);
        let mut lamellaes: BTreeMap<Backend, Arc<dyn LamellaeAM>> = BTreeMap::new();
        lamellaes.insert(lamellae.backend(), lamellae.get_am());
        sched.init(num_pes, world_pe, lamellaes);
        let counters = Arc::new(AMCounters::new());
        let root_team = Arc::new(LamellarTeamRT::new(
            num_pes,
            world_pe,
            sched.get_queue().clone(),
            counters.clone(),
            lamellae.clone(),
        ));
        let child_arch1 = StridedArch::new(0, 1, num_pes);
        let team1 =
            LamellarTeamRT::create_subteam_from_arch(root_team.clone(), child_arch1.clone())
                .unwrap();
        let team2 =
            LamellarTeamRT::create_subteam_from_arch(root_team.clone(), child_arch1.clone())
                .unwrap();
        let team1_1 =
            LamellarTeamRT::create_subteam_from_arch(team1.clone(), child_arch1.clone()).unwrap();
        let team1_2 =
            LamellarTeamRT::create_subteam_from_arch(team1.clone(), child_arch1.clone()).unwrap();
        let team2_1 =
            LamellarTeamRT::create_subteam_from_arch(team2.clone(), child_arch1.clone()).unwrap();
        let team2_2 =
            LamellarTeamRT::create_subteam_from_arch(team2.clone(), child_arch1.clone()).unwrap();

        println!("{:?}", root_team.team_hash);
        println!("{:?} -- {:?}", team1.team_hash, team2.team_hash);
        println!(
            "{:?} {:?} -- {:?} {:?}",
            team1_1.team_hash, team1_2.team_hash, team2_1.team_hash, team2_2.team_hash
        );

        assert_ne!(root_team.team_hash, team1.team_hash);
        assert_ne!(root_team.team_hash, team2.team_hash);
        assert_ne!(team1.team_hash, team2.team_hash);
        assert_ne!(team1.team_hash, team1_1.team_hash);
        assert_ne!(team1.team_hash, team1_2.team_hash);
        assert_ne!(team2.team_hash, team2_1.team_hash);
        assert_ne!(team2.team_hash, team2_2.team_hash);
        assert_ne!(team2.team_hash, team1_1.team_hash);
        assert_ne!(team2.team_hash, team1_2.team_hash);
        assert_ne!(team1.team_hash, team2_1.team_hash);
        assert_ne!(team1.team_hash, team2_2.team_hash);

        assert_ne!(team1_1.team_hash, team1_2.team_hash);
        assert_ne!(team1_1.team_hash, team2_1.team_hash);
        assert_ne!(team1_1.team_hash, team2_2.team_hash);

        assert_ne!(team1_2.team_hash, team2_1.team_hash);
        assert_ne!(team1_2.team_hash, team2_2.team_hash);

        assert_ne!(team2_1.team_hash, team2_2.team_hash);
    }
    // #[test]
    // fn asymetric_teams(){

    // }

    #[test]
    fn multi_team_unique_hash2() {
        let num_pes = 10;
        let world_pe = 0;
        let mut lamellae = create_lamellae(Backend::Local);
        let teams = Arc::new(RwLock::new(HashMap::new()));

        let mut sched = create_scheduler(
            SchedulerType::WorkStealing,
            num_pes,
            world_pe,
            teams.clone(),
        );
        lamellae.init_lamellae(sched.get_queue().clone());
        let lamellae = Arc::new(lamellae);
        let mut lamellaes: BTreeMap<Backend, Arc<dyn LamellaeAM>> = BTreeMap::new();
        lamellaes.insert(lamellae.backend(), lamellae.get_am());
        sched.init(num_pes, world_pe, lamellaes);
        let counters = Arc::new(AMCounters::new());

        let mut root_teams = Vec::new();
        for pe in 0..num_pes {
            root_teams.push(Arc::new(LamellarTeamRT::new(
                num_pes,
                pe,
                sched.get_queue().clone(),
                counters.clone(),
                lamellae.clone(),
            )));
        }
        let root_hash = root_teams[0].team_hash;
        for root_team in &root_teams {
            assert_eq!(root_hash, root_team.team_hash);
        }

        let odds_arch = StridedArch::new(1, 2, num_pes / 2);
        let odd_childs: Vec<_> = root_teams
            .iter()
            .map(|team| {
                LamellarTeamRT::create_subteam_from_arch(team.clone(), odds_arch.clone()).unwrap()
            })
            .collect();

        let evens_arch = StridedArch::new(0, 2, num_pes / 2);
        let even_childs: Vec<_> = root_teams
            .iter()
            .step_by(2)
            .map(|team| {
                LamellarTeamRT::create_subteam_from_arch(team.clone(), evens_arch.clone()).unwrap()
            })
            .collect();

        let hash = odd_childs[0].team_hash;
        for child in odd_childs {
            assert_eq!(hash, child.team_hash);
        }

        let hash = even_childs[0].team_hash;
        for child in even_childs {
            assert_eq!(hash, child.team_hash);
        }

        // let odds_odds_arch = StridedArch::new(1,2,num_pes);

        // println!("{:?}", root_team.team_hash);
        // println!("{:?} -- {:?}", team1.team_hash, team2.team_hash);
        // println!(
        //     "{:?} {:?} -- {:?} {:?}",
        //     team1_1.team_hash, team1_2.team_hash, team2_1.team_hash, team2_2.team_hash
        // );

        // assert_ne!(root_team.team_hash, team1.team_hash);
        // assert_ne!(root_team.team_hash, team2.team_hash);
        // assert_ne!(team1.team_hash, team2.team_hash);
        // assert_ne!(team1.team_hash, team1_1.team_hash);
        // assert_ne!(team1.team_hash, team1_2.team_hash);
        // assert_ne!(team2.team_hash, team2_1.team_hash);
        // assert_ne!(team2.team_hash, team2_2.team_hash);
        // assert_ne!(team2.team_hash, team1_1.team_hash);
        // assert_ne!(team2.team_hash, team1_2.team_hash);
        // assert_ne!(team1.team_hash, team2_1.team_hash);
        // assert_ne!(team1.team_hash, team2_2.team_hash);

        // assert_ne!(team1_1.team_hash, team1_2.team_hash);
        // assert_ne!(team1_1.team_hash, team2_1.team_hash);
        // assert_ne!(team1_1.team_hash, team2_2.team_hash);

        // assert_ne!(team1_2.team_hash, team2_1.team_hash);
        // assert_ne!(team1_2.team_hash, team2_2.team_hash);

        // assert_ne!(team2_1.team_hash, team2_2.team_hash);
    }
}

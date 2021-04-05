use crate::active_messaging::*; //{ActiveMessaging,AMCounters,Cmd,Msg,LamellarAny,LamellarLocal};
use crate::lamellae::Lamellae;
use crate::lamellar_arch::{GlobalArch, IdError, LamellarArch, LamellarArchEnum, LamellarArchRT};
// #[cfg(feature = "experimental")]
// use crate::lamellar_array::{LamellarArray, LamellarSubArray};
use crate::lamellar_memregion::{
    LamellarLocalMemoryRegion, LamellarMemoryRegion, MemoryRegion, RegisteredMemoryRegion,
    RemoteMemoryRegion,
};
use crate::lamellar_request::{AmType, LamellarRequest, LamellarRequestHandle};
use crate::schedulers::SchedulerQueue;
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
    pub(crate) team: Arc<LamellarTeamRT>,
    pub(crate) teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>, //need a reference to this so we can clean up after dropping the team
}

//#[prof]
impl LamellarTeam {
    #[allow(dead_code)]
    pub(crate) fn new(
        num_pes: usize,
        my_pe: usize,
        scheduler: Arc<dyn SchedulerQueue>,
        world_counters: Arc<AMCounters>,
        lamellae: Arc<dyn Lamellae + Sync + Send>,
        teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
    ) -> LamellarTeam {
        LamellarTeam {
            team: Arc::new(LamellarTeamRT::new(
                num_pes,
                my_pe,
                scheduler,
                world_counters,
                lamellae,
            )),
            teams: teams,
        }
    }
    #[allow(dead_code)]
    pub fn get_pes(&self) -> Vec<usize> {
        self.team.arch.team_iter().collect::<Vec<usize>>()
    }
    pub fn num_pes(&self) -> usize {
        self.team.arch.num_pes()
    }
    pub fn global_pe_id(&self) -> usize {
        self.team.my_pe
    }
    pub fn team_pe_id(&self) -> Result<usize, IdError> {
        self.team.arch.team_pe(self.team.my_pe)
    }
    pub fn create_team_from_arch<L>(&self, arch: L) -> Option<Arc<LamellarTeam>>
    where
        L: LamellarArch + std::hash::Hash + 'static,
    {
        if let Some(team) = LamellarTeamRT::create_subteam_from_arch(self.team.clone(), arch) {
            self.teams
                .write()
                .insert(team.my_hash, Arc::downgrade(&team));
            Some(Arc::new(LamellarTeam {
                team: team,
                teams: self.teams.clone(),
            }))
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
impl ActiveMessaging for LamellarTeam {
    fn wait_all(&self) {
        self.team.wait_all();
    }

    fn barrier(&self) {
        self.team.barrier();
    }

    fn exec_am_all<F>(&self, am: F) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: LamellarActiveMessage + LamellarAM + Send + Sync + 'static, // R: LamellarDataReturn + serde::de::DeserializeOwned
    {
        trace!("[{:?}] team exec am all request", self.team.my_pe);
        self.team.exec_am_all(am)
    }

    fn exec_am_pe<F>(
        &self,
        pe: usize,
        am: F,
    ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: LamellarActiveMessage + LamellarAM + Send + Sync + 'static,
    {
        self.team.exec_am_pe(pe, am)
    }
}

pub struct LamellarTeamRT {
    #[allow(dead_code)]
    parent: Option<Arc<LamellarTeamRT>>,
    sub_teams: RwLock<HashMap<usize, Arc<LamellarTeamRT>>>,
    mem_regions: RwLock<HashMap<usize, Box<dyn MemoryRegion + Sync + Send>>>,
    pub(crate) scheduler: Arc<dyn SchedulerQueue>,
    pub(crate) lamellae: Arc<dyn Lamellae + Send + Sync>,
    pub(crate) arch: Arc<LamellarArchRT>,
    pub(crate) my_pe: usize,
    pub(crate) num_pes: usize,
    world_pes: usize,
    team_counters: AMCounters,
    pub(crate) world_counters: Arc<AMCounters>, // can probably remove this?
    id: usize,
    sub_team_id_cnt: AtomicUsize,
    barrier_cnt: AtomicUsize,
    barrier1: LamellarMemoryRegion<usize>,
    barrier2: LamellarMemoryRegion<usize>,
    barrier3: LamellarMemoryRegion<usize>,
    pub(crate) my_hash: u64,
    dropped: LamellarMemoryRegion<usize>,
    // barrier_buf0: RwLock<Vec<usize>>,
    // barrier_buf1: RwLock<Vec<usize>>,
}

//#[prof]
impl Hash for LamellarTeamRT {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.my_hash.hash(state);
    }
}

//#[prof]
impl LamellarTeamRT {
    pub(crate) fn new(
        //creates a new root team
        num_pes: usize,
        my_pe: usize,
        scheduler: Arc<dyn SchedulerQueue>,
        world_counters: Arc<AMCounters>,
        lamellae: Arc<dyn Lamellae + Sync + Send>,
    ) -> LamellarTeamRT {
        // let backend = lamellae.backend();
        let arch = Arc::new(LamellarArchRT {
            parent: None,
            arch: LamellarArchEnum::GlobalArch(GlobalArch::new(num_pes)),
            num_pes: num_pes,
        });

        let team = LamellarTeamRT {
            parent: None,
            sub_teams: RwLock::new(HashMap::new()),
            mem_regions: RwLock::new(HashMap::new()),
            scheduler: scheduler,
            lamellae: lamellae.clone(),
            // backend: backend,
            arch: arch,
            my_pe: my_pe,
            num_pes: num_pes,
            world_pes: num_pes,
            team_counters: AMCounters::new(),
            world_counters: world_counters,
            id: 0,
            sub_team_id_cnt: AtomicUsize::new(0),
            barrier_cnt: AtomicUsize::new(0),
            barrier1: LamellarMemoryRegion::new(num_pes, lamellae.clone()),
            barrier2: LamellarMemoryRegion::new(num_pes, lamellae.clone()),
            barrier3: LamellarMemoryRegion::new(3, lamellae.clone()),
            my_hash: 0, //easy id to look up for global
            dropped: LamellarMemoryRegion::new(num_pes, lamellae.clone()),
            // barrier_buf0: RwLock::new(vec![10;num_pes]),
            // barrier_buf1: RwLock::new(vec![10;num_pes]),
        };
        unsafe {
            // let mut temp_array = LamellarLocalArray::new(std::cmp::max(team.num_pes,2),0usize,lamellae.get_rdma().clone());
            let temp_array = LamellarLocalMemoryRegion::<usize>::new(
                std::cmp::max(team.num_pes, 2),
                lamellae.clone(),
            );
            let temp_array_slice = temp_array.as_mut_slice().unwrap();
            team.barrier1
                .put_slice(team.my_pe, 0, &temp_array_slice[..team.num_pes]);
            team.barrier2
                .put_slice(team.my_pe, 0, &temp_array_slice[..team.num_pes]);
            temp_array_slice[0] = 10;
            team.barrier3
                .put_slice(team.my_pe, 0, &temp_array_slice[0..1]);
            team.barrier3
                .put_slice(team.my_pe, 1, &temp_array_slice[1..2]);
            team.barrier3
                .put_slice(team.my_pe, 2, &temp_array_slice[1..2]);
            for i in 0..team.num_pes {
                temp_array_slice[i] = 1;
            }
            team.dropped
                .put_slice(team.my_pe, 0, &temp_array_slice[..team.num_pes]);
        }
        lamellae.get_am().barrier(); //this is a noop currently
                                     // println!(
                                     //     "[{:?}] new lamellar team: {:?} {:?} {:?} {:?} {:?}",
                                     //     my_pe,
                                     //     team.id,
                                     //     team.arch.team_iter().collect::<Vec<usize>>(),
                                     //     team.barrier1.as_slice(),
                                     //     team.barrier2.as_slice(),
                                     //     team.barrier3.as_slice(),
                                     // );
                                     // team.bar_print("init".to_string());
        team
    }

    // pub(crate) fn bar_print(&self, tag: String) {
    // println!(
    //     "[{:?}] {:?} ({:?}) {:?} ({:?}) {:?} ({:?}) {:?}",
    //     self.my_pe,
    //     tag,
    //     self.barrier1.as_slice().as_ptr(),
    //     self.barrier1.as_slice(),
    //     self.barrier2.as_slice().as_ptr(),
    //     self.barrier2.as_slice(),
    //     self.barrier3.as_slice().as_ptr(),
    //     self.barrier3.as_slice(),
    // );
    // }

    pub(crate) fn destroy(&self) {
        // println!("destroying team?");
        for _lmr in self.mem_regions.read().iter() {
            //TODO: i have a gut feeling we might have an issue if a mem region was destroyed on one node, but not another
            // add a barrier method that takes a message so if we are stuck in the barrier for a long time we can say that
            // this is probably mismatched frees.
            self.barrier();
        }
        self.mem_regions.write().clear();
        self.sub_teams.write().clear(); // not sure this is necessary or should be allowed? sub teams delete themselves from this map when dropped...
                                        // what does it mean if we drop a parent team while a sub_team is valid?
    }
    #[allow(dead_code)]
    pub fn get_pes(&self) -> Vec<usize> {
        self.arch.team_iter().collect::<Vec<usize>>()
    }
    pub fn global_pe_id(&self) -> usize {
        self.my_pe
    }
    pub fn team_pe_id(&self) -> Result<usize, IdError> {
        self.arch.team_pe(self.my_pe)
    }

    pub fn create_subteam_from_arch<L>(
        parent: Arc<LamellarTeamRT>,
        arch: L,
    ) -> Option<Arc<LamellarTeamRT>>
    where
        L: LamellarArch + std::hash::Hash + 'static,
    {
        if parent.team_pe_id().is_ok() {
            let id = parent.sub_team_id_cnt.fetch_add(1, Ordering::SeqCst);
            let mut hasher = DefaultHasher::new();
            parent.hash(&mut hasher);
            id.hash(&mut hasher);
            arch.hash(&mut hasher);
            let hash = hasher.finish();
            let archrt = Arc::new(LamellarArchRT::new(parent.arch.clone(), arch));
            // println!("subteam: hash: {:?} arch{:?}",hash,archrt);

            // ------ ensure team is being constructed synchronously and in order across all pes in parent ------ //
            let temp_buf = LamellarMemoryRegion::new(parent.num_pes, parent.lamellae.clone());
            // let temp_array = LamellarLocalArray::new(parent.num_pes,0usize,parent.lamellae.get_rdma().clone());
            // let temp_array = parent.local_array(parent.num_pes,0usize);
            let temp_array = LamellarLocalMemoryRegion::<usize>::new(
                std::cmp::max(parent.num_pes, 3),
                parent.lamellae.clone(),
            );
            let temp_array_slice = temp_array.as_slice().unwrap();
            unsafe {
                temp_buf.put_slice(parent.my_pe, 0, &temp_array_slice[..parent.num_pes]);
            }
            // println!("new team hash: {:?}",hash);
            let s = Instant::now();
            parent.barrier();
            let timeout = Instant::now() - s;
            let hash_slice = unsafe { parent.barrier3.as_mut_slice().unwrap() };
            hash_slice[2] = hash as usize;
            // unsafe { parent.barrier3.as_mut_slice().unwrap()[2]  };

            // let hash_slice = parent.local_array(1,parent.barrier3.as_slice()[2]);
            parent.put_barrier_val(parent.my_pe, &hash_slice[2..], &temp_buf);

            parent.check_hash_vals(hash as usize, &temp_buf, timeout);
            // ------------------------------------------------------------------------------------------------- //

            // parent.barrier();
            // let backend = parent.lamellae.backend();
            let num_pes = archrt.num_pes();

            let team = LamellarTeamRT {
                parent: Some(parent.clone()),
                sub_teams: RwLock::new(HashMap::new()),
                mem_regions: RwLock::new(HashMap::new()),
                scheduler: parent.scheduler.clone(),
                lamellae: parent.lamellae.clone(),
                // backend: backend,
                arch: archrt,
                my_pe: parent.my_pe,
                num_pes: num_pes,
                world_pes: parent.world_pes,
                team_counters: AMCounters::new(),
                world_counters: parent.world_counters.clone(),
                id: id,
                sub_team_id_cnt: AtomicUsize::new(0),
                barrier_cnt: AtomicUsize::new(0),
                barrier1: LamellarMemoryRegion::new(num_pes, parent.lamellae.clone()),
                barrier2: LamellarMemoryRegion::new(num_pes, parent.lamellae.clone()),
                barrier3: LamellarMemoryRegion::new(3, parent.lamellae.clone()),
                my_hash: hash,
                dropped: LamellarMemoryRegion::new(parent.num_pes, parent.lamellae.clone()),
                // barrier_buf0: RwLock::new(vec![10;num_pes]),
                // barrier_buf1: RwLock::new(vec![10;num_pes]),
            };
            unsafe {
                team.barrier1
                    .put_slice(team.my_pe, 0, &temp_array_slice[0..team.num_pes]);
                team.barrier2
                    .put_slice(team.my_pe, 0, &temp_array_slice[0..team.num_pes]);
                team.barrier3
                    .put_slice(team.my_pe, 0, &temp_array_slice[0..3]);
                team.dropped
                    .put_slice(team.my_pe, 0, &temp_array_slice[0..team.num_pes]);
            }
            let team = Arc::new(team);
            let mut sub_teams = parent.sub_teams.write();
            // sub_teams.insert(team.id,Arc::downgrade(&team));
            sub_teams.insert(team.id, team.clone());
            parent.barrier();
            // println!("[{:?}] entering team barrier ", team.my_pe);
            team.barrier();
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
                        self.my_pe,
                        hash,
                        hash_buf.as_slice()
                    );
                    s = Instant::now();
                }
            }
            if *pe != hash && Instant::now() - s > timeout {
                println!(
                    "[{:?}] ({:?})  hash: {:?}",
                    self.my_pe,
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

    fn check_barrier_vals(&self, barrier_id: usize, barrier_buf: &LamellarMemoryRegion<usize>) {
        let mut s = Instant::now();
        for pe in barrier_buf.as_slice().unwrap() {
            while *pe != barrier_id {
                std::thread::yield_now();
                if s.elapsed().as_secs_f64() > 60.0 {
                    println!(
                        "[{:?}] ({:?})  b before: {:?} {:?}",
                        self.my_pe,
                        barrier_id,
                        self.barrier1.as_slice(),
                        self.barrier2.as_slice()
                    );
                    s = Instant::now();
                }
            }
        }
    }

    fn put_barrier_val(
        &self,
        my_index: usize,
        barrier_id: &[usize],
        // barrier_id: &LamellarLocalMemoryRegion<usize>,
        barrier_buf: &LamellarMemoryRegion<usize>,
    ) {
        for world_pe in self.arch.team_iter() {
            unsafe {
                barrier_buf.put_slice(world_pe, my_index, barrier_id);
            }
            // println!(
            //     "[{:?}] (bid: {:?}) putting b: {:?} {:?} {:?} {:?} {:?}",
            //     self.my_pe,
            //     barrier_id,
            //     world_pe,
            //     my_index,
            //     &[barrier_id],
            //     self.barrier1.as_slice(),
            //     self.barrier2.as_slice()
            // );
        }
    }

    fn put_dropped(&self) {
        if let Some(parent) = &self.parent {
            let temp_slice = unsafe { self.barrier3.as_mut_slice().unwrap() };
            temp_slice[2] = 1 as usize;
            let my_index = parent.arch.team_pe(self.my_pe).expect("invalid parent pe");
            // let  temp_array = parent.local_array(1,self.barrier3.as_slice().unwrap()[2]);
            for world_pe in parent.arch.team_iter() {
                unsafe {
                    self.dropped
                        .put_slice(world_pe, my_index, &temp_slice[2..=2]);
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
                    println!("[{:?}] ({:?})  ", self.my_pe, self.dropped.as_slice());
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
            if let Ok(id) = self.arch.global_pe(i) {
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
        for i in 0..self.world_pes {
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
}

// #[prof]
impl ActiveMessaging for LamellarTeamRT {
    fn wait_all(&self) {
        let mut temp_now = Instant::now();
        while self.team_counters.outstanding_reqs.load(Ordering::SeqCst) > 0 {
            std::thread::yield_now();
            if temp_now.elapsed() > Duration::new(60, 0) {
                println!(
                    "in world wait_all mype: {:?} cnt: {:?} {:?}",
                    self.my_pe,
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
        #[cfg(test)]
        if self.lamellae.backend() == crate::lamellae::Backend::Local {
            return;
        }
        // println!("[{:?}] in team barrier",self.my_pe);
        if let Ok(my_index) = self.arch.team_pe(self.my_pe) {
            // self.bar_print("bar_init".to_string());
            let mut barrier_id = self.barrier_cnt.fetch_add(1, Ordering::SeqCst);
            // println!("[{:?}] checking barrier entry ({:?}) {:?}",self.my_pe,barrier_id,&[barrier_id].as_ptr());
            // self.bar_print("0".to_string());

            self.check_barrier_vals(barrier_id, &self.barrier2);
            barrier_id += 1;
            let barrier3_slice = unsafe { self.barrier3.as_mut_slice().unwrap() };
            barrier3_slice[0] = barrier_id;
            let barrier_slice = &[barrier_id];
            // self.bar_print("1".to_string());
            // println!("[{:?}] putting new barrier val ({:?}) {:?}",self.my_pe,barrier_id,barrier_slice.as_ptr());

            self.put_barrier_val(my_index, barrier_slice, &self.barrier1);
            // self.bar_print("2".to_string());
            // println!("[{:?}] checking new barrier val ({:?})",self.my_pe,barrier_id);
            self.check_barrier_vals(barrier_id, &self.barrier1);
            // self.bar_print("3".to_string());
            // println!("[{:?}] setting barrier exit val ({:?})",self.my_pe,barrier_id);
            barrier3_slice[1] = barrier_id;
            // self.bar_print("4".to_string());

            let barrier_slice = &barrier3_slice[1..2];

            self.put_barrier_val(my_index, barrier_slice, &self.barrier2);
            // self.bar_print("5".to_string());
            // println!("[{:?}] checking barrier exit ({:?})",self.my_pe,barrier_id);
        }
    }

    fn exec_am_all<F>(&self, am: F) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: LamellarActiveMessage + LamellarAM + Send + Sync + 'static,
        // R: LamellarRequest
        // R: LamellarDataReturn + serde::de::DeserializeOwned
    {
        trace!("[{:?}] team exec am all request", self.my_pe);
        let (my_req, ireq) = LamellarRequestHandle::new(
            self.num_pes,
            AmType::RegisteredFunction,
            self.arch.clone(),
            self.team_counters.outstanding_reqs.clone(),
            self.world_counters.outstanding_reqs.clone(),
        );
        let msg = Msg {
            cmd: ExecType::Am(Cmd::Exec),
            src: self.my_pe as u16,
            req_id: my_req.id,
            team_id: self.id,
            return_data: true,
        };
        self.world_counters.add_send_req(self.num_pes);
        self.team_counters.add_send_req(self.num_pes);
        let my_any: LamellarAny = Box::new(Box::new(am) as LamellarBoxedAm);
        self.scheduler.submit_req(
            self.my_pe,
            None,
            msg,
            ireq,
            my_any,
            self.lamellae.get_am(),
            self.my_hash,
        );
        Box::new(my_req)
    }

    fn exec_am_pe<F>(
        &self,
        pe: usize,
        am: F,
    ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
    where
        F: LamellarActiveMessage + LamellarAM + Send + Sync + 'static,
    {
        prof_start!(pre);
        trace!("[{:?}] team exec am pe request", self.my_pe);
        assert!(pe < self.arch.num_pes());
        prof_end!(pre);
        prof_start!(req);
        let (my_req, ireq) = LamellarRequestHandle::new(
            1,
            AmType::RegisteredFunction,
            self.arch.clone(),
            self.team_counters.outstanding_reqs.clone(),
            self.world_counters.outstanding_reqs.clone(),
        );
        prof_end!(req);
        prof_start!(msg);
        let msg = Msg {
            cmd: ExecType::Am(Cmd::Exec),
            src: self.my_pe as u16,
            req_id: my_req.id,
            team_id: self.id,
            return_data: true,
        };
        prof_end!(msg);
        prof_start!(counters);
        self.world_counters.add_send_req(1);
        self.team_counters.add_send_req(1);
        prof_end!(counters);
        prof_start!(any);
        let my_any: LamellarAny = Box::new(Box::new(am) as LamellarBoxedAm);
        prof_end!(any);
        prof_start!(sub);
        self.scheduler.submit_req(
            self.my_pe,
            Some(pe),
            msg,
            ireq,
            my_any,
            self.lamellae.get_am(),
            self.my_hash,
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
    ) -> LamellarRequest<T> {
        trace!("[{:?}] team exec closure all request", self.my_pe);
        let (my_req, ireq) = LamellarRequest::new(
            self.num_pes,
            AmType::RemoteClosure,
            self.arch.clone(),
            self.team_counters.outstanding_reqs.clone(),
            self.world_counters.outstanding_reqs.clone(),
        );
        let msg = Msg {
            cmd: ExecType::Closure(Cmd::Exec),
            src: self.my_pe as u16,
            req_id: my_req.id,
            team_id: self.id,
            return_data: true,
        };
        self.world_counters.add_send_req(self.num_pes);
        self.team_counters.add_send_req(self.num_pes);
        let my_any: LamellarAny = Box::new((lamellar_local(func.clone()), lamellar_closure(func)));
        self.scheduler.submit_req(
            self.my_pe,
            None,
            msg,
            ireq,
            my_any,
            self.lamellae.get_am(),
            self.my_hash,
        );
        my_req
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
    ) -> LamellarRequest<T> {
        trace!("[{:?}] team exec_closure_pe [{:?}]", self.my_pe, pe);
        assert!(pe < self.arch.num_pes());
        let (my_req, mut ireq) = LamellarRequest::new(
            1,
            AmType::RemoteClosure,
            self.arch.clone(),
            self.team_counters.outstanding_reqs.clone(),
            self.world_counters.outstanding_reqs.clone(),
        );
        let msg = Msg {
            cmd: ExecType::Closure(Cmd::Exec),
            src: self.my_pe as u16,
            req_id: my_req.id,
            team_id: self.id,
            return_data: true,
        };
        self.world_counters.add_send_req(1);
        self.team_counters.add_send_req(1);
        ireq.start = Instant::now();
        let my_any: LamellarAny = if self.my_pe == pe {
            Box::new(lamellar_local(func))
        } else {
            Box::new(lamellar_closure(func))
        };
        self.scheduler.submit_req(
            self.my_pe,
            Some(pe),
            msg,
            ireq,
            my_any,
            self.lamellae.get_am(),
            self.my_hash,
        );
        my_req
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
        // println!("alloc_mem_reg: {:?}",self.my_pe);
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
        // println!("alloc_mem_reg: {:?}",self.my_pe);
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
        // println!("alloc_mem_reg: {:?}",self.my_pe);
        self.barrier();
        let lmr = LamellarMemoryRegion::new(size, self.lamellae.clone());
        // println!("adding region {:?} {:?}",lmr,lmr.id());
        self.mem_regions
            .write()
            .insert(lmr.id(), Box::new(lmr.clone()));
        self.barrier();
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
        // println!("alloc_mem_reg: {:?}",self.my_pe);
        let llmr = LamellarLocalMemoryRegion::new(size, self.lamellae.clone());
        // println!("adding region {:?} {:?}",lmr,lmr.id());
        self.mem_regions
            .write()
            .insert(llmr.id(), Box::new(llmr.clone()));
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
        self.barrier();
        self.mem_regions.write().remove(&region.id());
        // println!("freed region {:?} {:?}",region, self.mem_regions.read().len());
        // region.delete(self.lamellae.clone());
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
        self.mem_regions.write().remove(&region.id());
        // println!("freed region {:?} {:?}",region, self.mem_regions.read().len());
        // region.delete(self.lamellae.clone());
    }
}

//#[prof]
impl Drop for LamellarTeamRT {
    fn drop(&mut self) {
        // println!("[{:?}] team dropping {:?}", self.my_pe, self.get_pes());
        // self.wait_all();
        // self.barrier();
        // self.put_dropped();
        // if let Ok(my_index) = self.arch.team_pe_id(&self.my_pe){
        //     self.drop_barrier();
        // }
        // if let Some(parent) = &self.parent{
        //     parent.sub_teams.write().remove(&self.id);
        // }
        // println!("[{:?}] team dropped {:?}", self.my_pe, self.get_pes());
        // self.barrier1.delete(self.lamellae.clone());
        // self.barrier2.delete(self.lamellae.clone());
        // self.barrier3.delete(self.lamellae.clone());
        // if let Some(parent) = &self.parent{
        //     parent.sub_teams.write().remove(&self.id);
        // }
        //TODO: what happends when we have sub_teams present?
        // does the recursive drop manage it properly for us or
        // do we need to manually remove those teams from the map?
    }
}

//#[prof]
impl Drop for LamellarTeam {
    fn drop(&mut self) {
        // println!(
        //     "[{:?}] team handle dropping {:?}",
        //     self.team.my_pe,
        //     self.team.get_pes()
        // );
        self.team.wait_all();
        self.team.barrier();
        self.team.put_dropped();
        if let Ok(_my_index) = self.team.arch.team_pe(self.team.my_pe) {
            self.team.drop_barrier();
        }
        if let Some(parent) = &self.team.parent {
            parent.sub_teams.write().remove(&self.team.id);
        }
        self.teams.write().remove(&self.team.my_hash);
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
        let my_pe = 0;
        let mut lamellae = create_lamellae(Backend::Local);
        let teams = Arc::new(RwLock::new(HashMap::new()));

        let mut sched =
            create_scheduler(SchedulerType::WorkStealing, num_pes, my_pe, teams.clone());
        lamellae.init_lamellae(sched.get_queue().clone());
        let lamellae = Arc::new(lamellae);
        let mut lamellaes: BTreeMap<Backend, Arc<dyn LamellaeAM>> = BTreeMap::new();
        lamellaes.insert(lamellae.backend(), lamellae.get_am());
        sched.init(num_pes, my_pe, lamellaes);
        let counters = Arc::new(AMCounters::new());
        let root_team = Arc::new(LamellarTeamRT::new(
            num_pes,
            my_pe,
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

        println!("{:?}", root_team.my_hash);
        println!("{:?} -- {:?}", team1.my_hash, team2.my_hash);
        println!(
            "{:?} {:?} -- {:?} {:?}",
            team1_1.my_hash, team1_2.my_hash, team2_1.my_hash, team2_2.my_hash
        );

        assert_ne!(root_team.my_hash, team1.my_hash);
        assert_ne!(root_team.my_hash, team2.my_hash);
        assert_ne!(team1.my_hash, team2.my_hash);
        assert_ne!(team1.my_hash, team1_1.my_hash);
        assert_ne!(team1.my_hash, team1_2.my_hash);
        assert_ne!(team2.my_hash, team2_1.my_hash);
        assert_ne!(team2.my_hash, team2_2.my_hash);
        assert_ne!(team2.my_hash, team1_1.my_hash);
        assert_ne!(team2.my_hash, team1_2.my_hash);
        assert_ne!(team1.my_hash, team2_1.my_hash);
        assert_ne!(team1.my_hash, team2_2.my_hash);

        assert_ne!(team1_1.my_hash, team1_2.my_hash);
        assert_ne!(team1_1.my_hash, team2_1.my_hash);
        assert_ne!(team1_1.my_hash, team2_2.my_hash);

        assert_ne!(team1_2.my_hash, team2_1.my_hash);
        assert_ne!(team1_2.my_hash, team2_2.my_hash);

        assert_ne!(team2_1.my_hash, team2_2.my_hash);
    }
    // #[test]
    // fn asymetric_teams(){

    // }

    #[test]
    fn multi_team_unique_hash2() {
        let num_pes = 10;
        let my_pe = 0;
        let mut lamellae = create_lamellae(Backend::Local);
        let teams = Arc::new(RwLock::new(HashMap::new()));

        let mut sched =
            create_scheduler(SchedulerType::WorkStealing, num_pes, my_pe, teams.clone());
        lamellae.init_lamellae(sched.get_queue().clone());
        let lamellae = Arc::new(lamellae);
        let mut lamellaes: BTreeMap<Backend, Arc<dyn LamellaeAM>> = BTreeMap::new();
        lamellaes.insert(lamellae.backend(), lamellae.get_am());
        sched.init(num_pes, my_pe, lamellaes);
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
        let root_hash = root_teams[0].my_hash;
        for root_team in &root_teams {
            assert_eq!(root_hash, root_team.my_hash);
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

        let hash = odd_childs[0].my_hash;
        for child in odd_childs {
            assert_eq!(hash, child.my_hash);
        }

        let hash = even_childs[0].my_hash;
        for child in even_childs {
            assert_eq!(hash, child.my_hash);
        }

        // let odds_odds_arch = StridedArch::new(1,2,num_pes);

        // println!("{:?}", root_team.my_hash);
        // println!("{:?} -- {:?}", team1.my_hash, team2.my_hash);
        // println!(
        //     "{:?} {:?} -- {:?} {:?}",
        //     team1_1.my_hash, team1_2.my_hash, team2_1.my_hash, team2_2.my_hash
        // );

        // assert_ne!(root_team.my_hash, team1.my_hash);
        // assert_ne!(root_team.my_hash, team2.my_hash);
        // assert_ne!(team1.my_hash, team2.my_hash);
        // assert_ne!(team1.my_hash, team1_1.my_hash);
        // assert_ne!(team1.my_hash, team1_2.my_hash);
        // assert_ne!(team2.my_hash, team2_1.my_hash);
        // assert_ne!(team2.my_hash, team2_2.my_hash);
        // assert_ne!(team2.my_hash, team1_1.my_hash);
        // assert_ne!(team2.my_hash, team1_2.my_hash);
        // assert_ne!(team1.my_hash, team2_1.my_hash);
        // assert_ne!(team1.my_hash, team2_2.my_hash);

        // assert_ne!(team1_1.my_hash, team1_2.my_hash);
        // assert_ne!(team1_1.my_hash, team2_1.my_hash);
        // assert_ne!(team1_1.my_hash, team2_2.my_hash);

        // assert_ne!(team1_2.my_hash, team2_1.my_hash);
        // assert_ne!(team1_2.my_hash, team2_2.my_hash);

        // assert_ne!(team2_1.my_hash, team2_2.my_hash);
    }
}

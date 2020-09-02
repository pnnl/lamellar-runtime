use crate::active_messaging::*; //{ActiveMessaging,AMCounters,Cmd,Msg,LamellarAny,LamellarLocal};
use crate::lamellae::{Backend, Lamellae};
use crate::lamellar_memregion::{MemoryRegion, LamellarMemoryRegion, RemoteMemoryRegion};
use crate::lamellar_request::{LamellarRequest,AmType};
use crate::schedulers::SchedulerQueue;
#[cfg(feature = "nightly")]
use crate::utils::ser_closure;

use log::{trace};
// use std::any;
use std::collections::{HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;

static CUR_TEAM_ID: AtomicUsize = AtomicUsize::new(0);

pub trait LamellarArch:  Sync + Send + std::fmt::Debug {
    fn num_pes(&self) -> usize;
    fn world_pe_id(&self, team_pe: &usize) -> Result<usize, IdError>; // need global id so the lamellae knows who to communicate this
    fn team_pe_id(&self, world_pe: &usize) -> Result<usize, IdError>; // team id is for user convenience, ids == 0..num_pes-1
    fn team_iter(&self) -> Box<dyn Iterator<Item = usize> + Send>; //return an iterator of the teams global pe ids
    fn single_iter(&self, pe: usize) -> Box<dyn Iterator<Item = usize> + Send>; //a single element iterator returning the global id of pe
}

impl<T: LamellarArch> LamellarArch for & T {
    fn num_pes(&self) -> usize{
        (*self).num_pes()
    }
    fn world_pe_id(&self, team_pe: &usize) -> Result<usize, IdError>{
        (*self).world_pe_id(team_pe)
    } // need global id so the lamellae knows who to communicate this
    fn team_pe_id(&self, world_pe: &usize) -> Result<usize, IdError>{
        (*self).team_pe_id(world_pe)
    } // team id is for user convenience, ids == 0..num_pes-1
                                                                    
    //return an iterator of the teams global pe ids
    fn team_iter(&self) -> Box<dyn Iterator<Item = usize> + Send>{
        (*self).team_iter()
    }
    //a single element iterator returning the global id of pe
    fn single_iter(&self, pe: usize) -> Box<dyn Iterator<Item = usize> + Send>{
        (*self).single_iter(pe)
    }   
}

impl<T: LamellarArch + ?Sized> LamellarArch for Box<T> {
    fn num_pes(&self) -> usize{
        (**self).num_pes()
    }
    fn world_pe_id(&self, team_pe: &usize) -> Result<usize, IdError>{
        (**self).world_pe_id(team_pe)
    } // need global id so the lamellae knows who to communicate this
    fn team_pe_id(&self, world_pe: &usize) -> Result<usize, IdError>{
        (**self).team_pe_id(world_pe)
    } // team id is for user convenience, ids == 0..num_pes-1
                                                                    
    //return an iterator of the teams global pe ids
    fn team_iter(&self) -> Box<dyn Iterator<Item = usize> + Send>{
        (**self).team_iter()
    }
    //a single element iterator returning the global id of pe
    fn single_iter(&self, pe: usize) -> Box<dyn Iterator<Item = usize> + Send>{
        (**self).single_iter(pe)
    }
    
}

// pub(crate) trait LamellarArchIter: Iterator + Sync + Send {}

// pub (crate) //have a function that creates an iterator for either the whole team or a single pe.

#[derive(Debug, Clone)]
pub struct IdError {
    world_pe: usize,
    team_pe: usize,
}
impl std::fmt::Display for IdError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Invalid Id => global_pe:{} team_pe => {}",
            self.world_pe, self.team_pe
        )
    }
}

impl std::error::Error for IdError {}

#[derive(Copy, Clone, Debug)]
pub struct StridedArch {
    pub(crate) num_pes: usize,
    pub(crate) start_pe: usize, //global id
    pub(crate) end_pe: usize,   //global id
    pub(crate) stride: usize,
    pub(crate) global_pes: usize,
}

impl StridedArch {
    pub fn new(
        start_pe: usize,
        end_pe: usize,
        stride: usize,
        global_pes: usize,
    ) -> StridedArch {
        // trace!("[{:?}] new strided arch", my_global_pe);
        if start_pe >= global_pes {
            panic!(
                "invalid start_pe {:?}, needs to be between 0-{:?}",
                start_pe,
                global_pes - 1
            );
        }
        if end_pe < start_pe || end_pe >= global_pes {
            panic!(
                "invalid end_pe {:?}, needs to be between {:?}-{:?}",
                end_pe,
                start_pe,
                global_pes - 1
            );
        }
        let num_pes = ((1 + end_pe - start_pe) as f32 / stride as f32).ceil() as usize;
        
        StridedArch {
            num_pes: num_pes,
            start_pe: start_pe,
            end_pe: end_pe,
            stride: stride,
            global_pes: global_pes,
        }
    }
}

impl LamellarArch for StridedArch {
    // fn my_pe(&self) -> usize {
    //     self.my_pe
    // }
    fn num_pes(&self) -> usize {
        self.num_pes
    }
    fn world_pe_id(&self, team_pe: &usize) -> Result<usize, IdError> {
        let id = self.start_pe + team_pe * self.stride;
        if id >= self.start_pe && id < self.global_pes {
            Ok(id)
        } else {
            Err(IdError {
                world_pe: id,
                team_pe: *team_pe,
            })
        }
    }
    fn team_pe_id(&self, world_pe: &usize) -> Result<usize, IdError> {
        if (world_pe - self.start_pe) % self.stride == 0 {
            let id = (world_pe - self.start_pe) / self.stride;
            if id >= self.start_pe && id <= self.end_pe {
                Ok(id)
            } else {
                Err(IdError {
                    world_pe: *world_pe,
                    team_pe: id,
                })
            }
        } else {
            Err(IdError {
                world_pe: *world_pe,
                team_pe: 0,
            })
        }
    }
    fn team_iter(&self) -> Box<dyn Iterator<Item = usize> + Send> {
        Box::new(StridedArchIter {
            arch: *self,
            cur_pe: 0,
            single: false,
        })
    }

    fn single_iter(&self, pe: usize) -> Box<dyn Iterator<Item = usize> + Send> {
        Box::new(StridedArchIter {
            arch: *self,
            cur_pe: pe,
            single: true,
        })
    }
}

pub(crate) struct StridedArchIter {
    arch: StridedArch,
    cur_pe: usize, //pe in team based ids
    single: bool,
}

// impl LamellarArchIter for StridedArchIter {}
impl Iterator for StridedArchIter {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        let res = if self.cur_pe < self.arch.num_pes {
            if let Ok(pe) = self.arch.world_pe_id(&self.cur_pe) {
                Some(pe)
            } else {
                None
            }
        } else {
            return None;
        };
        if self.single {
            self.cur_pe = self.arch.num_pes;
        } else {
            self.cur_pe += 1;
        }
        res
    }
}

pub struct LamellarTeam {
    #[allow(dead_code)]
    parent: Option<Arc<LamellarTeam>>,
    sub_teams: RwLock<HashMap<usize, Arc<LamellarTeam>>>,
    mem_regions: RwLock<HashMap<usize, Box<dyn MemoryRegion>>>,
    scheduler: Arc<dyn SchedulerQueue>,
    lamellae: Arc<dyn Lamellae + Send + Sync>,
    backend: Backend,
    pub(crate) arch: Arc<dyn LamellarArch>,
    #[allow(dead_code)]
    pes: Vec<usize>,
    my_pe: usize,
    num_pes: usize,
    team_counters: AMCounters,
    world_counters: Arc<AMCounters>,
    id: usize,
    barrier_cnt: AtomicUsize,
    barrier1: LamellarMemoryRegion<usize> ,
    barrier2: LamellarMemoryRegion<usize> ,
    barrier3: LamellarMemoryRegion<usize> ,
    // barrier_buf0: RwLock<Vec<usize>>,
    // barrier_buf1: RwLock<Vec<usize>>,
}

impl LamellarTeam {
    pub(crate) fn new(
        num_pes: usize,
        my_pe: usize,
        scheduler: Arc<dyn SchedulerQueue>,
        world_counters: Arc<AMCounters>,
        lamellae: Arc<dyn Lamellae + Sync + Send>,
    ) -> LamellarTeam {
        let backend = lamellae.backend();
        let team = LamellarTeam {
            parent: None,
            sub_teams: RwLock::new(HashMap::new()),
            mem_regions: RwLock::new(HashMap::new()),
            scheduler: scheduler,
            lamellae: lamellae.clone(),
            backend: backend,
            pes: Vec::new(),
            arch: Arc::new(StridedArch::new( 0, num_pes - 1, 1, num_pes)),
            my_pe: my_pe,
            num_pes: num_pes,
            team_counters: AMCounters::new(),
            world_counters: world_counters,
            id: CUR_TEAM_ID.fetch_add(1, Ordering::SeqCst),
            barrier_cnt: AtomicUsize::new(0),
            barrier1: LamellarMemoryRegion::new(num_pes,lamellae.clone()),
            barrier2: LamellarMemoryRegion::new(num_pes,lamellae.clone()),
            barrier3: LamellarMemoryRegion::new(2,lamellae.clone()),
            // barrier_buf0: RwLock::new(vec![10;num_pes]),
            // barrier_buf1: RwLock::new(vec![10;num_pes]),
        };
        unsafe{
            team.barrier1.put(team.my_pe,0,&(0..team.num_pes).map(|_x| 0).collect::<Vec<usize>>());
            team.barrier2.put(team.my_pe,0,&(0..team.num_pes).map(|_x| 0).collect::<Vec<usize>>());
            team.barrier3.put(team.my_pe,0,&[10]);
            team.barrier3.put(team.my_pe,1,&[0]);
        }
        trace!(
            "[{:?}] new lamellar team: {:?} {:?} {:?}",
            my_pe,
            team.id,
            team.arch.team_iter().collect::<Vec<usize>>(),
            team.barrier3
        );
        team
    }

    pub(crate) fn destroy(&self){
        for _lmr in self.mem_regions.read().iter(){
            //TODO: i have a gut feeling we might have an issue if a mem region was destroyed on one node, but not another
            // add a barrier method that takes a message so if we are stuck in the barrier for a long time we can say that
            // this is probably mismatched frees.
            self.barrier();
        }
        self.mem_regions.write().clear();
        self.sub_teams.write().clear();
    }
    #[allow(dead_code)]
    pub fn get_pes(&self) -> Vec<usize> {
        self.arch.team_iter().collect::<Vec<usize>>()
    }

    pub fn create_subteam_from_arch<L>(parent: Arc<LamellarTeam>, arch: L ) -> Arc<LamellarTeam>
        where L: LamellarArch  + 'static {
        parent.barrier();
        let backend = parent.lamellae.backend();
        // let newarch = Box::new(arch.clone());
        let num_pes = arch.num_pes();
        let team = LamellarTeam {
            parent: Some(parent.clone()),
            sub_teams: RwLock::new(HashMap::new()),
            mem_regions: RwLock::new(HashMap::new()),
            scheduler: parent.scheduler.clone(),
            lamellae: parent.lamellae.clone(),
            backend: backend,
            pes: Vec::new(),
            arch: Arc::new(arch),
            my_pe: parent.my_pe,
            num_pes: num_pes,
            team_counters: AMCounters::new(),
            world_counters: parent.world_counters.clone(),
            id: CUR_TEAM_ID.fetch_add(1, Ordering::SeqCst),
            barrier_cnt: AtomicUsize::new(0),
            barrier1: LamellarMemoryRegion::new(num_pes,parent.lamellae.clone()),
            barrier2: LamellarMemoryRegion::new(num_pes,parent.lamellae.clone()),
            barrier3: LamellarMemoryRegion::new(2,parent.lamellae.clone()),
            // barrier_buf0: RwLock::new(vec![10;num_pes]),
            // barrier_buf1: RwLock::new(vec![10;num_pes]),

        };
        unsafe{
            team.barrier1.put(team.my_pe,0,&(0..team.num_pes).map(|_x| 0).collect::<Vec<usize>>());
            team.barrier2.put(team.my_pe,0,&(0..team.num_pes).map(|_x| 0).collect::<Vec<usize>>());
            team.barrier3.put(team.my_pe,0,&(0..2).map(|_x| 0).collect::<Vec<usize>>());
        }
        let team = Arc::new(team);
        let mut sub_teams = parent.sub_teams.write();
        sub_teams.insert(team.id,team.clone());
        parent.barrier();
        team.barrier();
        team
    }

    pub fn num_pes(&self) -> usize {
        self.arch.num_pes()
    }

    fn check_barrier_vals(&self, barrier_id: usize, barrier_buf: &LamellarMemoryRegion<usize>){
        let mut s = Instant::now();
        for pe in barrier_buf.as_slice() {
            while *pe != barrier_id {
                std::thread::yield_now();
                if s.elapsed().as_secs_f64() > 5.0{
                    println!("[{:?}] ({:?})  b before: {:?} {:?}",self.my_pe,barrier_id,self.barrier1.as_slice(),self.barrier2.as_slice());
                    s = Instant::now();
                }
            }
        }
    }

    fn put_barrier_val(&self, my_index: usize, barrier_id: &[usize], barrier_buf: &LamellarMemoryRegion<usize>){
        for world_pe in self.arch.team_iter(){
            
            barrier_buf.iput(world_pe,my_index,barrier_id);
            // println!("[{:?}] (bid: {:?}) putting b: {:?} {:?} {:?} {:?} {:?}",self.my_pe,barrier_id,world_pe,my_index,&[barrier_id],self.barrier1.as_slice(),self.barrier2.as_slice());
        }
    }

    // fn check_barrier_vals(&self,index: usize,barrier_id: usize,barrier_buf: &RwLock<std::vec::Vec<usize>>){
    //     // let mut barrier_val: std::vec::Vec<usize> = vec![barrier_id+10;self.arch.num_pes()];
    //     let mut barrier_buf= barrier_buf.write();
    //     let mut s = Instant::now();
    //     let mut done = false;
    //     for pe in (0..self.arch.num_pes()){
    //         barrier_buf[pe]=barrier_id+10;
    //         unsafe{self.barrier3.get(self.arch.world_pe_id(&pe).expect("invalid pe"),index,&mut barrier_buf[pe..(pe+1)])};
    //     }
    //     while !done {
    //         done = true;
    //         for pe in (0..self.arch.num_pes()){
    //             if barrier_buf[pe] != barrier_id {
    //                 done = false;
    //                 if barrier_buf[pe] != barrier_id+10 {
    //                     barrier_buf[pe]=barrier_id+10;
    //                     // println!("[{:?}] ({:?}) barrierissue! b{:?} {:?} {:?} {:?} {:?}",self.my_pe,barrier_id,index,pe,self.arch.world_pe_id(&pe),&barrier_buf[pe..(pe+1)],barrier_buf);
    //                     unsafe{self.barrier3.get(self.arch.world_pe_id(&pe).expect("invalid pe"),index,&mut barrier_buf[pe..(pe+1)])};
    //                 }
    //             }
    //         }
    //         std::thread::yield_now();
    //         if s.elapsed().as_secs_f64() > 5.0{
    //             debug!("[{:?}] ({:?})  b{:?}: {:?} {:?}",self.my_pe,barrier_id,index,self.barrier3.as_slice(),barrier_buf);
    //             s = Instant::now();
    //         }
    //     }
    // }
}

impl ActiveMessaging for LamellarTeam {
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

    

    // fn barrier(&self) {
    //     trace!("[{:?}] team {:?} barrier", self.my_pe, self.id);
    //     // self.lamellae.barrier();
        
    //     // let mut barrier_val: std::vec::Vec<usize> = vec![barrier_id+10;self.arch.num_pes()];
    //     if let Ok(my_index) = self.arch.team_pe_id(&self.my_pe){
    //         let mut barrier_id = self.barrier_cnt.fetch_add(1,Ordering::SeqCst);
    //         debug!("[{:?}] ({:?}) {:?} ",self.my_pe,barrier_id,self.barrier3.as_slice());
            
    //         self.check_barrier_vals(1,barrier_id, &self.barrier_buf1);
            
    //         barrier_id += 1;
    //         unsafe { self.barrier3.iput(self.my_pe,0,&[barrier_id])} ;
    //         // println!("[{:?}] ({:?}) here {:?}",self.my_pe,barrier_id,self.barrier3.as_slice());
            
    //         self.check_barrier_vals(0,barrier_id, &self.barrier_buf0);
            
            
    //         unsafe { self.barrier3.iput(self.my_pe,1,&[barrier_id])} ;
    //         // println!("[{:?}] ({:?}) here2 {:?}",self.my_pe,barrier_id,self.barrier3.as_slice());
            
    //         self.check_barrier_vals(1,barrier_id, &self.barrier_buf1);

    //         debug!("[{:?}] ({:?})  leaving barrier",self.my_pe,barrier_id)
    //     }
    // }

    fn barrier(&self){
        if let Ok(my_index) = self.arch.team_pe_id(&self.my_pe){
            let mut barrier_id = self.barrier_cnt.fetch_add(1,Ordering::SeqCst);
            // println!("[{:?}] checking barrier entry ({:?}) {:?}",self.my_pe,barrier_id,&[barrier_id].as_ptr());
            self.check_barrier_vals(barrier_id,&self.barrier2);
            barrier_id += 1;
            unsafe{self.barrier3.as_mut_slice()[0] = barrier_id};
            let barrier_slice = &self.barrier3.as_slice()[0..1];
            
            // println!("[{:?}] putting new barrier val ({:?}) {:?}",self.my_pe,barrier_id,barrier_slice.as_ptr());
            self.put_barrier_val(my_index, barrier_slice,&self.barrier1);
            // println!("[{:?}] checking new barrier val ({:?})",self.my_pe,barrier_id);
            self.check_barrier_vals(barrier_id,&self.barrier1);
            // println!("[{:?}] setting barrier exit val ({:?})",self.my_pe,barrier_id);
            unsafe{self.barrier3.as_mut_slice()[1] = barrier_id};
            let barrier_slice = &self.barrier3.as_slice()[1..];
            self.put_barrier_val(my_index, barrier_slice,&self.barrier2);
            // println!("[{:?}] checking barrier exit ({:?})",self.my_pe,barrier_id);
            // self.check_barrier_vals(barrier_id,&self.barrier2);
        }
    
    }

    // fn barrier_old(&self) {
    //     trace!("[{:?}] team {:?} barrier", self.my_pe, self.id);
    //     // self.lamellae.barrier();
    //     let mut barrier_id = self.barrier_cnt.fetch_add(1,Ordering::SeqCst);
    //     println!("[{:?}] ({:?}) {:?} {:?}",self.my_pe,barrier_id,self.barrier1.as_slice(),self.barrier2.as_slice());
        
    //     if let Ok(my_index) = self.arch.team_pe_id(&self.my_pe){
    //         let mut s = Instant::now();
    //         for pe in self.barrier2.as_slice(){
    //             while *pe != barrier_id{
    //                 std::thread::yield_now();
    //                 if s.elapsed().as_secs_f64() > 5.0{
    //                     println!("[{:?}] ({:?})  b2 before: {:?}",self.my_pe,barrier_id,self.barrier2.as_slice());
    //                     s = Instant::now();
    //                 }
    //             }
                
    //         }
    //         barrier_id += 1;
    //         for world_pe in self.arch.team_iter(){
    //             println!("[{:?}] ({:?}) putting b1: {:?} {:?} {:?}",self.my_pe,barrier_id,world_pe,my_index,&[barrier_id]);
    //             unsafe { self.barrier1.iput(world_pe,my_index,&[barrier_id])} ;
    //         }
            
    //         for pe in self.barrier1.as_slice(){
    //             while *pe != barrier_id{
    //                 std::thread::yield_now();
    //                 if s.elapsed().as_secs_f64() > 5.0{
    //                     println!("[{:?}] ({:?}) b1 waiting: {:?}",self.my_pe,barrier_id,self.barrier1.as_slice());
    //                     s = Instant::now();
    //                 }
    //             }
                
    //         }
            
    //         for world_pe in self.arch.team_iter(){
    //             println!("[{:?}] ({:?}) putting b2: {:?} {:?} {:?}",self.my_pe,barrier_id,world_pe,my_index,&[barrier_id]);
    //             unsafe { self.barrier2.iput(world_pe,my_index,&[barrier_id])} ;
    //         }
    //         for pe in self.barrier2.as_slice(){
    //             while *pe != barrier_id{
    //                 std::thread::yield_now();
    //                 if s.elapsed().as_secs_f64() > 5.0{
    //                     println!("[{:?}] ({:?}) b2 after: {:?}",self.my_pe,barrier_id,self.barrier2.as_slice());
    //                     s = Instant::now();
    //                 }
    //             }
                
    //         }
    //         println!("[{:?}] ({:?})  leaving barrier",self.my_pe,barrier_id)
    //     }
    // }

    fn exec_am_all<F>(&self, am: F) -> LamellarRequest<F::Output>
    where
        F: LamellarActiveMessage
            + LamellarAM
            + Send
            // + serde::de::DeserializeOwned
            // + std::clone::Clone
            + 'static,
        // R: LamellarDataReturn + serde::de::DeserializeOwned
    {
        trace!("[{:?}] team exec am all request", self.my_pe);
        let (my_req, ireq) = LamellarRequest::new(
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
        self.scheduler
            .submit_req(self.my_pe,None, msg, ireq, my_any, self.arch.clone(), self.backend);
        my_req
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
        trace!("[{:?}] team exec am pe request", self.my_pe);
        assert!(pe < self.arch.num_pes());
        let (my_req, ireq) = LamellarRequest::new(
            1,
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
        self.world_counters.add_send_req(1);
        self.team_counters.add_send_req(1);
        let my_any: LamellarAny = Box::new(Box::new(am) as LamellarBoxedAm);
        self.scheduler
            .submit_req(self.my_pe, Some(pe), msg, ireq, my_any, self.arch.clone(), self.backend);
        my_req
    }
}

//#feature gated closures for those with nightly
#[cfg(feature = "nightly")]
use crate::active_messaging::remote_closures::{lamellar_closure, lamellar_local, ClosureRet};
#[cfg(feature = "nightly")]
impl RemoteClosures for LamellarTeam{
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
        self.scheduler
            .submit_req(self.my_pe, None, msg, ireq, my_any, self.arch.clone(), self.backend);
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
        self.scheduler
            .submit_req(self.my_pe, Some(pe), msg, ireq, my_any, self.arch.clone(), self.backend);
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

impl RemoteMemoryRegion for LamellarTeam {
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
        // println!("alloc_mem_reg: {:?}",self.my_pe);
        self.barrier();
        let lmr = LamellarMemoryRegion::new(size, self.lamellae.clone());
        // println!("adding region {:?} {:?}",lmr,lmr.id());
        self.mem_regions.write().insert(lmr.id(),Box::new(lmr.clone()));
        self.barrier();
        lmr
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
        self.barrier();
        self.mem_regions.write().remove(&region.id());
        println!("freed reqgion {:?} {:?}",region, self.mem_regions.read().len());
        // region.delete(self.lamellae.clone());
    }
}

impl Drop for LamellarTeam {
    fn drop(&mut self) {
        trace!("[{:?}] team dropping {:?}", self.my_pe,self.get_pes()); 
        self.wait_all();
        self.barrier();
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn global_arch() {
        let arch = StridedArch::new(0, 0, 9, 1, 10);
        // assert_eq!(0, arch.my_pe());
        assert_eq!(10, arch.num_pes());
        assert_eq!(vec![0], arch.single_iter(0).collect::<Vec<usize>>());
        assert_eq!(vec![3], arch.single_iter(3).collect::<Vec<usize>>());
        assert_eq!(
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            arch.team_iter().collect::<Vec<usize>>()
        );
    }
    #[test]
    fn sub_arch_stride_1() {
        let arch = StridedArch::new(0, 0, 4, 1, 10);
        // assert_eq!(0, arch.my_pe());
        assert_eq!(5, arch.num_pes());
        assert_eq!(vec![0], arch.single_iter(0).collect::<Vec<usize>>());
        assert_eq!(vec![3], arch.single_iter(3).collect::<Vec<usize>>());
        assert_eq!(
            Vec::<usize>::new(),
            arch.single_iter(7).collect::<Vec<usize>>()
        );
        assert_eq!(
            vec![0, 1, 2, 3, 4],
            arch.team_iter().collect::<Vec<usize>>()
        );
    }
    #[test]
    fn sub_arch_stride_2() {
        let arch = StridedArch::new(0, 0, 9, 2, 10);
        // assert_eq!(0, arch.my_pe());
        assert_eq!(5, arch.num_pes());
        assert_eq!(vec![0], arch.single_iter(0).collect::<Vec<usize>>());
        assert_eq!(vec![4], arch.single_iter(2).collect::<Vec<usize>>());
        assert_eq!(
            Vec::<usize>::new(),
            arch.single_iter(7).collect::<Vec<usize>>()
        );
        assert_eq!(
            vec![0, 2, 4, 6, 8],
            arch.team_iter().collect::<Vec<usize>>()
        );

        let arch = StridedArch::new(1, 1, 9, 2, 10);
        // assert_eq!(1, arch.my_pe());
        assert_eq!(5, arch.num_pes());
        assert_eq!(vec![1], arch.single_iter(0).collect::<Vec<usize>>());
        assert_eq!(vec![5], arch.single_iter(2).collect::<Vec<usize>>());
        assert_eq!(
            Vec::<usize>::new(),
            arch.single_iter(7).collect::<Vec<usize>>()
        );
        assert_eq!(
            vec![1, 3, 5, 7, 9],
            arch.team_iter().collect::<Vec<usize>>()
        );
    }
    #[test]
    fn sub_arch_stride_3() {
        let arch = StridedArch::new(0, 0, 10, 3, 11);
        // assert_eq!(0, arch.my_pe());
        assert_eq!(4, arch.num_pes());
        assert_eq!(vec![0], arch.single_iter(0).collect::<Vec<usize>>());
        assert_eq!(vec![6], arch.single_iter(2).collect::<Vec<usize>>());
        assert_eq!(
            Vec::<usize>::new(),
            arch.single_iter(7).collect::<Vec<usize>>()
        );
        assert_eq!(vec![0, 3, 6, 9], arch.team_iter().collect::<Vec<usize>>());

        let arch = StridedArch::new(1, 1, 10, 3, 11);
        // assert_eq!(1, arch.my_pe());
        assert_eq!(4, arch.num_pes());
        assert_eq!(vec![1], arch.single_iter(0).collect::<Vec<usize>>());
        assert_eq!(vec![7], arch.single_iter(2).collect::<Vec<usize>>());
        assert_eq!(
            Vec::<usize>::new(),
            arch.single_iter(7).collect::<Vec<usize>>()
        );
        assert_eq!(vec![1, 4, 7, 10], arch.team_iter().collect::<Vec<usize>>());

        let arch = StridedArch::new(1, 2, 10, 3, 11);
        // assert_eq!(1, arch.my_pe());
        assert_eq!(3, arch.num_pes());
        assert_eq!(vec![2], arch.single_iter(0).collect::<Vec<usize>>());
        assert_eq!(vec![8], arch.single_iter(2).collect::<Vec<usize>>());
        assert_eq!(
            Vec::<usize>::new(),
            arch.single_iter(7).collect::<Vec<usize>>()
        );
        assert_eq!(vec![2, 5, 8], arch.team_iter().collect::<Vec<usize>>());
    }
}



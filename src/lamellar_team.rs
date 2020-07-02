use crate::active_messaging::*; //{ActiveMessaging,AMCounters,Cmd,Msg,LamellarAny,LamellarLocal};
use crate::lamellae::{Backend, Lamellae};
use crate::lamellar_memregion::{LamellarMemoryRegion, RemoteMemoryRegion};
use crate::lamellar_request::LamellarRequest;
use crate::schedulers::SchedulerQueue;
#[cfg(feature = "nightly")]
use crate::utils::ser_closure;

use log::trace;
// use std::any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

static CUR_TEAM_ID: AtomicUsize = AtomicUsize::new(0);

pub(crate) trait LamellarArch: Sync + Send + std::fmt::Debug {
    fn my_pe(&self) -> usize;
    fn num_pes(&self) -> usize;
    fn global_pe_id(&self, team_pe: &usize) -> Result<usize, IdError>; // need global id so the lamellae knows who to communicate this
    fn team_pe_id(&self, global_pe: &usize) -> Result<usize, IdError>; // team id is for user convenience, ids == 0..num_pes-1
                                                                       //return an iterator of the teams global pe ids
    fn team_iter(&self) -> Box<dyn Iterator<Item = usize> + Send>;
    //a single element iterator returning the global id of pe
    fn single_iter(&self, pe: usize) -> Box<dyn Iterator<Item = usize> + Send>;
}

pub(crate) trait LamellarArchIter: Iterator + Sync + Send {}

// pub (crate) //have a function that creates an iterator for either the whole team or a single pe.

#[derive(Debug, Clone)]
pub struct IdError {
    global_pe: usize,
    team_pe: usize,
}
impl std::fmt::Display for IdError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Invalid Id => global_pe:{} team_pe => {}",
            self.global_pe, self.team_pe
        )
    }
}

impl std::error::Error for IdError {}

#[derive(Copy, Clone, Debug)]
pub(crate) struct StridedArch {
    pub(crate) my_pe: usize,
    pub(crate) num_pes: usize,
    pub(crate) start_pe: usize, //global id
    pub(crate) end_pe: usize,   //global id
    pub(crate) stride: usize,
    pub(crate) global_pes: usize,
}

impl StridedArch {
    pub(crate) fn new(
        my_pe: usize,
        start_pe: usize,
        end_pe: usize,
        stride: usize,
        global_pes: usize,
    ) -> StridedArch {
        trace!("[{:?}] new strided arch", my_pe);
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
            my_pe: my_pe,
            num_pes: num_pes,
            start_pe: start_pe,
            end_pe: end_pe,
            stride: stride,
            global_pes: global_pes,
        }
    }
}

impl LamellarArch for StridedArch {
    fn my_pe(&self) -> usize {
        self.my_pe
    }
    fn num_pes(&self) -> usize {
        self.num_pes
    }
    fn global_pe_id(&self, team_pe: &usize) -> Result<usize, IdError> {
        let id = self.start_pe + team_pe * self.stride;
        if id >= self.start_pe && id < self.global_pes {
            Ok(id)
        } else {
            Err(IdError {
                global_pe: id,
                team_pe: *team_pe,
            })
        }
    }
    fn team_pe_id(&self, global_pe: &usize) -> Result<usize, IdError> {
        if (global_pe - self.start_pe) % self.stride == 0 {
            let id = (global_pe - self.start_pe) / self.stride;
            if id >= self.start_pe && id <= self.end_pe {
                Ok(id)
            } else {
                Err(IdError {
                    global_pe: *global_pe,
                    team_pe: id,
                })
            }
        } else {
            Err(IdError {
                global_pe: *global_pe,
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
    cur_pe: usize,
    single: bool,
}

impl LamellarArchIter for StridedArchIter {}
impl Iterator for StridedArchIter {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        let res = if self.cur_pe < self.arch.num_pes {
            if let Ok(pe) = self.arch.global_pe_id(&self.cur_pe) {
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

pub struct LamellarTeam<'a> {
    #[allow(dead_code)]
    sub_teams: HashMap<usize, &'a LamellarTeam<'a>>,
    scheduler: Arc<dyn SchedulerQueue>,
    lamellae: Arc<dyn Lamellae + Send + Sync>,
    backend: Backend,
    pub(crate) arch: Arc<dyn LamellarArch + Sync + Send>,
    #[allow(dead_code)]
    pes: Vec<usize>,
    my_pe: usize,
    num_pes: usize,
    team_counters: AMCounters,
    world_counters: Arc<AMCounters>,
    id: usize,
}

impl<'a> LamellarTeam<'a> {
    pub(crate) fn new(
        num_pes: usize,
        my_pe: usize,
        scheduler: Arc<dyn SchedulerQueue>,
        world_counters: Arc<AMCounters>,
        lamellae: Arc<dyn Lamellae + Sync + Send>,
    ) -> LamellarTeam<'a> {
        let backend = lamellae.backend();
        let team = LamellarTeam {
            sub_teams: HashMap::new(),
            scheduler: scheduler,
            lamellae: lamellae,
            backend: backend,
            pes: Vec::new(),
            arch: Arc::new(StridedArch::new(my_pe, 0, num_pes - 1, 1, num_pes)),
            my_pe: my_pe,
            num_pes: num_pes,
            team_counters: AMCounters::new(),
            world_counters: world_counters,
            id: CUR_TEAM_ID.fetch_add(1, Ordering::SeqCst),
        };
        trace!(
            "[{:?}] new lamellar team: {:?} {:?}",
            my_pe,
            team.id,
            team.arch.team_iter().collect::<Vec<usize>>()
        );
        team
    }
    #[allow(dead_code)]
    pub(crate) fn get_pes(&self) -> &[usize] {
        &self.pes
    }
}

impl ActiveMessaging for LamellarTeam<'_> {
    fn wait_all(&self) {
        let mut temp_now = Instant::now();
        while self.team_counters.outstanding_reqs.load(Ordering::SeqCst) > 0 {
            std::thread::yield_now();
            if temp_now.elapsed() > Duration::new(60, 0) {
                println!(
                    "in world wait_all mype: {:?} cnt: {:?} {:?}",
                    self.arch.my_pe(),
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
        trace!("[{:?}] team {:?} barrier", self.my_pe, self.id);
        self.lamellae.barrier();
    }
    fn exec_am_all<F>(&self, am: F) -> LamellarRequest<F::Output>
    where
        F: LamellarActiveMessage
            + LamellarAM
            + Send
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + 'static,
        // R: LamellarDataReturn + serde::de::DeserializeOwned
    {
        trace!("[{:?}] team exec am all request", self.my_pe);
        let (my_req, ireq) = LamellarRequest::new(
            self.num_pes,
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
            .submit_req(None, msg, ireq, my_any, self.arch.clone(), self.backend);
        my_req
    }

    fn exec_am_pe<F>(&self, pe: usize, am: F) -> LamellarRequest<F::Output>
    where
        F: LamellarActiveMessage
            + LamellarAM
            + Send
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + 'static,
    {
        trace!("[{:?}] team exec am pe request", self.my_pe);
        let (my_req, ireq) = LamellarRequest::new(
            1,
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
            .submit_req(Some(pe), msg, ireq, my_any, self.arch.clone(), self.backend);
        my_req
    }
}

//#feature gated closures for those with nightly
#[cfg(feature = "nightly")]
use crate::active_messaging::remote_closures::{lamellar_closure, lamellar_local, ClosureRet};
#[cfg(feature = "nightly")]
impl RemoteClosures for LamellarTeam<'_> {
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
            .submit_req(None, msg, ireq, my_any, self.arch.clone(), self.backend);
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
        let (my_req, mut ireq) = LamellarRequest::new(
            1,
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
            .submit_req(Some(pe), msg, ireq, my_any, self.arch.clone(), self.backend);
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

impl RemoteMemoryRegion for LamellarTeam<'_> {
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
        region.delete(self.lamellae.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn global_arch() {
        let arch = StridedArch::new(0, 0, 9, 1, 10);
        assert_eq!(0, arch.my_pe());
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
        assert_eq!(0, arch.my_pe());
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
        assert_eq!(0, arch.my_pe());
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
        assert_eq!(1, arch.my_pe());
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
        assert_eq!(0, arch.my_pe());
        assert_eq!(4, arch.num_pes());
        assert_eq!(vec![0], arch.single_iter(0).collect::<Vec<usize>>());
        assert_eq!(vec![6], arch.single_iter(2).collect::<Vec<usize>>());
        assert_eq!(
            Vec::<usize>::new(),
            arch.single_iter(7).collect::<Vec<usize>>()
        );
        assert_eq!(vec![0, 3, 6, 9], arch.team_iter().collect::<Vec<usize>>());

        let arch = StridedArch::new(1, 1, 10, 3, 11);
        assert_eq!(1, arch.my_pe());
        assert_eq!(4, arch.num_pes());
        assert_eq!(vec![1], arch.single_iter(0).collect::<Vec<usize>>());
        assert_eq!(vec![7], arch.single_iter(2).collect::<Vec<usize>>());
        assert_eq!(
            Vec::<usize>::new(),
            arch.single_iter(7).collect::<Vec<usize>>()
        );
        assert_eq!(vec![1, 4, 7, 10], arch.team_iter().collect::<Vec<usize>>());

        let arch = StridedArch::new(1, 2, 10, 3, 11);
        assert_eq!(1, arch.my_pe());
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

impl Drop for LamellarTeam<'_> {
    fn drop(&mut self) {
        trace!("[{:?}] team dropping", self.my_pe);
        self.wait_all();
        self.barrier();
        //TODO: what happends when we have sub_teams present?
        // does the recursive drop manage it properly for us or
        // do we need to manually remove those teams from the map?
    }
}


use crate::active_messaging::*;
use crate::LamellarTeamRT;
use crate::LamellarTeam;
use crate::lamellar_request::{InternalReq,CUR_REQ_ID};
use crate::scheduler::SchedulerQueue;

use crossbeam::utils::CachePadded;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize,Ordering};
use std::time::{Instant,Duration};

pub struct LamellarTaskGroup{
    team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) counters: AMCounters,
    pub(crate) ireq: InternalReq,
    pub(crate) req_id: usize,
}   

impl LamellarTaskGroup{
    pub fn new(team: Arc<LamellarTeam>) -> LamellarTaskGroup{
        let team = team.team.clone();
        let (s, _r) = crossbeam::channel::unbounded();
        let counters = AMCounters::new();
        let ireq = InternalReq {
            data_tx: s,
            cnt: Arc::new(CachePadded::new(AtomicUsize::new(1))),
            team_outstanding_reqs: team.team_counters.outstanding_reqs.clone(),
            world_outstanding_reqs: team.world_counters.outstanding_reqs.clone(),
            tg_outstanding_reqs: Some(counters.outstanding_reqs.clone()),
            // team_hash: team_hash,
            // team: team,
        };
        let req_id = CUR_REQ_ID.fetch_add(1,Ordering::SeqCst);
        REQUESTS.lock().insert(req_id,ireq.clone());
        LamellarTaskGroup{
            team: team.clone(),
            counters: counters,
            ireq: ireq,
            req_id: req_id,
        }
    }

    pub fn wait_all(&self) {
        let mut temp_now = Instant::now();
        while self.counters.outstanding_reqs.load(Ordering::SeqCst) > 0 {
            self.team.scheduler.exec_task(); 
            if temp_now.elapsed() > Duration::new(60, 0) {
                println!(
                    "in task group wait_all mype: {:?} cnt: {:?} {:?}",
                    self.team.world_pe,
                    self.team.team_counters.send_req_cnt.load(Ordering::SeqCst),
                    self.team.team_counters.outstanding_reqs.load(Ordering::SeqCst),
                );
                temp_now = Instant::now();
            }
        }
    }


    pub fn exec_am_all<F>(&self, am: F)
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        self.team.team_counters.add_send_req(self.team.num_pes);
        self.team.world_counters.add_send_req(self.team.num_pes);
        self.counters.add_send_req(self.team.num_pes);
        self.ireq.cnt.fetch_add(self.team.num_pes,Ordering::SeqCst);
        let func: LamellarArcAm = Arc::new(am);
        let world = if let Some(world) = &self.team.world {
            world.clone()
        } else {
            self.team.clone()
        };
        self.team.scheduler.submit_req(
            self.team.world_pe,
            None,
            ExecType::Am(Cmd::Exec),
            self.req_id,
            LamellarFunc::Am(func),
            self.team.lamellae.clone(),
            world,
            self.team.clone(),
            self.team.remote_ptr_addr as u64,
            None,
        )
    }

    pub fn exec_am_pe<F>(
        &self,
        pe: usize,
        am: F,
    ) 
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist,
    {
        self.team.team_counters.add_send_req(1);
        self.team.world_counters.add_send_req(1);
        self.counters.add_send_req(1);
        self.ireq.cnt.fetch_add(1,Ordering::SeqCst);
        let func: LamellarArcAm = Arc::new(am);
        let world = if let Some(world) = &self.team.world {
            world.clone()
        } else {
            self.team.clone()
        };
        self.team.scheduler.submit_req(
            self.team.world_pe,
            Some(self.team.arch.world_pe(pe).expect("pe not member of team")),
            ExecType::Am(Cmd::Exec),
            self.req_id,
            LamellarFunc::Am(func),
            self.team.lamellae.clone(),
            world,
            self.team.clone(),
            self.team.remote_ptr_addr as u64,
            None,
        )
    }
}

impl Drop for LamellarTaskGroup{
    fn drop(&mut self){
        let cnt = self.ireq.cnt.fetch_sub(1, Ordering::SeqCst);
        if cnt == 1 {
            REQUESTS.lock().remove(&self.req_id);
        }
    }
}
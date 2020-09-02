use crate::lamellae::rofi::command_queues::RofiCommandQueue;
use crate::lamellae::rofi::rofi_comm::RofiComm;
use crate::lamellae::{Backend, Lamellae, LamellaeAM, LamellaeRDMA};
use crate::lamellar_team::LamellarArch;
use crate::schedulers::SchedulerQueue;
use log::{error, trace};
// use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

struct IOThread {
    thread: Option<thread::JoinHandle<()>>,
    lamellar_msg_q_rx:
        crossbeam::channel::Receiver<(Vec<u8>, Box<dyn Iterator<Item = usize> + Send>)>,
    cmd_q_rx: crossbeam::channel::Receiver<Arc<Vec<u8>>>,
    scheduler: Arc<dyn SchedulerQueue>,
    am: Arc<RofiLamellaeAM>,
    cmd_q: Arc<RofiCommandQueue>,
    // active: Arc<AtomicBool>,
    active_notify: (
        crossbeam::channel::Sender<bool>,
        crossbeam::channel::Receiver<bool>,
    ),
}

impl IOThread {
    fn run(&mut self) {
        trace!("[{:?}] Test Lamellae IO Thread running", self.am.my_pe);
        let scheduler = self.scheduler.clone();
        let am = self.am.clone();
        let lamellar_msg_q_rx = self.lamellar_msg_q_rx.clone();
        let cmd_q_rx = self.cmd_q_rx.clone();
        let cmd_q = self.cmd_q.clone();
        let active_notify = self.active_notify.1.clone();
        let mut active = true;
        self.thread = Some(thread::spawn(move || {
            let mut sel = crossbeam::Select::new();
            let q1 = sel.recv(&lamellar_msg_q_rx);
            let q2 = sel.recv(&cmd_q_rx);
            let q3 = sel.recv(&active_notify);
            while active || !(lamellar_msg_q_rx.is_empty() && cmd_q_rx.is_empty()) {
                let oper = sel.select();
                let q = oper.index();
                match q {
                    q if q == q1 => {
                        if let Ok((data, team_iter)) = oper.recv(&lamellar_msg_q_rx) {
                            trace!("[{:?}] sending data (len = {:?})", am.my_pe, data.len());
                            cmd_q.send_data(data, team_iter);
                            trace!("[{:?}] data sent", am.my_pe);
                        }
                    }
                    q if q == q2 => {
                        if let Ok(data) = oper.recv(&cmd_q_rx) {
                            trace!("[{:?}] received data", am.my_pe);
                            scheduler.submit_work(data.to_vec(), am.clone());
                        }
                    }
                    q if q == q3 => {
                        if let Ok(notify) = oper.recv(&active_notify) {
                            trace!("[{:?}] received notify", am.my_pe);
                            active = notify;
                        }
                    }
                    _ => unreachable!(),
                }
            }

            trace!("[{:?}] Test Lamellae IO Thread finished", am.my_pe,);
        }));
    }
    fn shutdown(&mut self) {
        trace!("[{:}] shutting down io thread", self.am.my_pe);
        let _res = self.active_notify.0.send(false);
        while !(self.lamellar_msg_q_rx.is_empty() && self.cmd_q_rx.is_empty()) {
            let _res = self.active_notify.0.send(false); //keep sending notifications so we can break out of the select
        }
        self.thread
            .take()
            .expect("error joining io thread")
            .join()
            .expect("error joinging io thread");
        trace!("[{:}] shut down io thread", self.am.my_pe);
    }
}

pub(crate) struct RofiLamellae {
    _scheduler: Arc<dyn SchedulerQueue>,
    threads: Vec<IOThread>,
    am: Arc<RofiLamellaeAM>,
    rdma: Arc<RofiLamellaeRDMA>,
    cq: Arc<RofiCommandQueue>,
    // rofi_comm: Arc<RofiComm>,
    my_pe: usize,
    num_pes: usize,
}

impl RofiLamellae {
    pub(crate) fn new(scheduler: Arc<dyn SchedulerQueue>) -> RofiLamellae {
        let (lamellar_msg_q_tx, lamellar_msg_q_rx) = crossbeam::channel::unbounded();
        let (cmd_q_tx, cmd_q_rx) = crossbeam::channel::unbounded();
        let rofi_comm = Arc::new(RofiComm::new());
        let cq = RofiCommandQueue::new(cmd_q_tx, rofi_comm.clone());
        let num_pes = cq.num_pes();
        let my_pe = cq.my_pe();
        trace!("cq initialized my_pe: {:} num_pes {:?}", my_pe, num_pes);

        let am = Arc::new(RofiLamellaeAM {
            lamellar_msg_q_tx: lamellar_msg_q_tx,
            my_pe: my_pe,
        });
        trace!("[{:}] new RofiLamellaeAM", my_pe);

        let rdma = Arc::new(RofiLamellaeRDMA {
            rofi_comm: rofi_comm.clone(),
        });

        let mut lamellae = RofiLamellae {
            _scheduler: scheduler.clone(),
            threads: vec![],
            am: am.clone(),
            rdma: rdma.clone(),
            cq: Arc::new(cq),
            // rofi_comm: rofi_comm,
            my_pe: my_pe,
            num_pes: num_pes,
        };
        trace!("[{:}] new RofiLamellae", my_pe);

        for _i in 0..1 {
            let thread = IOThread {
                lamellar_msg_q_rx: lamellar_msg_q_rx.clone(),
                cmd_q_rx: cmd_q_rx.clone(),
                thread: None,
                scheduler: scheduler.clone(),
                am: am.clone(),
                cmd_q: lamellae.cq.clone(),
                active_notify: crossbeam::channel::bounded(1),
            };
            lamellae.threads.push(thread);
        }
        for thread in &mut lamellae.threads {
            thread.run();
        }
        trace!("[{:}] threads launched", my_pe);
        lamellae
    }
}

impl Lamellae for RofiLamellae {
    fn init(&mut self) -> (usize, usize) {
        (self.num_pes, self.my_pe)
    }
    fn finit(&self) {
        self.cq.finit();
    }
    fn barrier(&self) {
        self.cq.barrier();
    }
    fn backend(&self) -> Backend {
        Backend::Rofi
    }
    fn get_am(&self) -> Arc<dyn LamellaeAM> {
        self.am.clone()
    }
    // fn get_rdma<'a>(&self) -> &'a dyn LamellaeRDMA {
    //     self
    // }
    fn get_rdma(&self) -> Arc<dyn LamellaeRDMA>{
        self.rdma.clone()
    }
    #[allow(non_snake_case)]
    fn MB_sent(&self) -> f64 {
        self.cq.data_sent() as f64 / 1_000_000.0f64
    }
    fn print_stats(&self) {}
}

pub(crate) struct RofiLamellaeAM {
    lamellar_msg_q_tx:
        crossbeam::channel::Sender<(Vec<u8>, Box<dyn Iterator<Item = usize> + Send>)>,
    my_pe: usize,
}

impl LamellaeAM for RofiLamellaeAM {
    fn send_to_pe(&self, pe: usize, data: std::vec::Vec<u8>) {
        // self.single_q_tx.send((data, pe));
        let v = vec![pe];
        self.lamellar_msg_q_tx
            .send((data, Box::new(v.into_iter())))
            .expect("error in send to pe");
    }
    fn send_to_all(&self, _data: std::vec::Vec<u8>) {}
    fn send_to_pes(
        &self,
        pe: Option<usize>,
        team: Arc<dyn LamellarArch>,
        data: std::vec::Vec<u8>,
    ) {
        if let Some(pe) = pe {
            self.lamellar_msg_q_tx
                .send((data, team.single_iter(pe)))
                .expect("error in send to pes");
        } else {
            self.lamellar_msg_q_tx
                .send((data, team.team_iter()))
                .expect("error in send to pes");
        }
    }
    fn barrier(&self) {
        error!("need to implement an active message version of barrier");
    }
    fn backend(&self) -> Backend {
        Backend::Rofi
    }
}

pub(crate) struct RofiLamellaeRDMA { 
    rofi_comm: Arc<RofiComm>,
}

impl LamellaeRDMA for RofiLamellaeRDMA{
    fn put(&self, pe: usize, src: &[u8], dst: usize) {
        self.rofi_comm.put(pe, src, dst);
    }
    fn iput(&self, pe: usize, src: &[u8], dst: usize) {
        self.rofi_comm.iput(pe, src, dst);
    }
    fn put_all(&self, src: &[u8], dst: usize) {
        self.rofi_comm.put_all(src, dst);
    }
    fn get(&self, pe: usize, src: usize, dst: &mut [u8]) {
        self.rofi_comm.get(pe, src, dst)
    }
    fn alloc(&self, size: usize) -> Option<usize> {
        self.rofi_comm.alloc(size)
    }
    fn free(&self, addr: usize) {
        self.rofi_comm.free(addr)
    }
    fn base_addr(&self) -> usize {
        self.rofi_comm.base_addr()
    }
    fn mype(&self) -> usize {
        self.rofi_comm.mype()
    }
}

impl Drop for RofiLamellae {
    fn drop(&mut self) {
        
        while let Some(mut iothread) = self.threads.pop() {
            iothread.shutdown();
        }
        trace!("[{:?}] RofiLamellae Dropping", self.my_pe);
        
    }
}

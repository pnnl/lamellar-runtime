use crate::active_messaging::{ActiveMessageEngine, ActiveMessageEngineType, Am};
use crate::lamellae::{Des, Lamellae, SerializedData};
use crate::scheduler::batching::simple_batcher::SimpleBatcher;
use crate::scheduler::batching::team_am_batcher::TeamAmBatcher;
use crate::scheduler::batching::BatcherType;
use crate::scheduler::registered_active_message::RegisteredActiveMessages;
use crate::scheduler::{AmeScheduler, AmeSchedulerQueue, SchedulerQueue};
use lamellar_prof::*;
// use log::trace;
use core_affinity::CoreId;
use crossbeam::deque::Worker;
use futures::Future;
use futures_lite::FutureExt;
// use parking_lot::RwLock;
use rand::prelude::*;
use std::collections::HashMap;
use std::panic;
use std::process;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc; //, Weak};
use std::thread;
// use thread_local::ThreadLocal;
// use std::time::Instant;

#[derive(Debug)]
pub(crate) struct NumaWorkStealing2Thread {
    work_inj: Arc<crossbeam::deque::Injector<async_task::Runnable>>,
    work_stealers: Vec<crossbeam::deque::Stealer<async_task::Runnable>>,
    work_q: Worker<async_task::Runnable>,
    work_flag: Arc<AtomicU8>,
    active: Arc<AtomicBool>,
}

//#[prof]
impl NumaWorkStealing2Thread {
    fn run(
        worker: NumaWorkStealing2Thread,
        active_cnt: Arc<AtomicUsize>,
        num_tasks: Arc<AtomicUsize>,
        id: CoreId,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            // println!("TestSchdulerWorker thread running");
            core_affinity::set_for_current(id);
            active_cnt.fetch_add(1, Ordering::SeqCst);
            let mut rng = rand::thread_rng();
            let t = rand::distributions::Uniform::from(0..worker.work_stealers.len());
            let mut timer = std::time::Instant::now();
            // let mut cur_tasks = num_tasks.load(Ordering::SeqCst);
            while worker.active.load(Ordering::SeqCst)
                || !(worker.work_q.is_empty() && worker.work_inj.is_empty())
                || num_tasks.load(Ordering::SeqCst) > 1
            {
                // let ot = Instant::now();
                // if cur_tasks != num_tasks.load(Ordering::SeqCst){
                //     println!(
                //         "work_q size {:?} work inj size {:?} num_tasks {:?}",
                //         worker.work_q.len(),
                //         worker.work_inj.len(),
                //         num_tasks.load(Ordering::SeqCst)
                //     );
                //     cur_tasks = num_tasks.load(Ordering::SeqCst);

                // }
                let omsg = worker.work_q.pop().or_else(|| {
                    if worker
                        .work_flag
                        .compare_exchange(0, 1, Ordering::SeqCst, Ordering::Relaxed)
                        == Ok(0)
                    {
                        let ret = worker
                            .work_inj
                            .steal_batch_and_pop(&worker.work_q)
                            .success();
                        worker.work_flag.store(0, Ordering::SeqCst);
                        ret
                    } else {
                        worker.work_stealers[t.sample(&mut rng)].steal().success()
                    }
                });
                if let Some(runnable) = omsg {
                    if !worker.active.load(Ordering::SeqCst) && timer.elapsed().as_secs_f64() > 60.0
                    {
                        println!("runnable {:?}", runnable);
                        println!(
                            "work_q size {:?} work inj size {:?} num_tasks {:?}",
                            worker.work_q.len(),
                            worker.work_inj.len(),
                            num_tasks.load(Ordering::SeqCst)
                        );
                        timer = std::time::Instant::now();
                    }
                    runnable.run();
                }
                if !worker.active.load(Ordering::SeqCst)
                    && timer.elapsed().as_secs_f64() > 60.0
                    && (worker.work_q.len() > 0 || worker.work_inj.len() > 0)
                {
                    println!(
                        "work_q size {:?} work inj size {:?} num_tasks {:?}",
                        worker.work_q.len(),
                        worker.work_inj.len(),
                        num_tasks.load(Ordering::SeqCst)
                    );
                    timer = std::time::Instant::now();
                }
                // if timer.elapsed().as_secs_f64() > 60.0 {
                //     println!(
                //         "work_q size {:?} work inj size {:?} num_tasks {:?}",
                //         worker.work_q.len(),
                //         worker.work_inj.len(),
                //         num_tasks.load(Ordering::SeqCst)
                //     );
                //     timer = std::time::Instant::now()
                // }
            }
            fini_prof!();
            active_cnt.fetch_sub(1, Ordering::SeqCst);
            // println!("TestSchdulerWorker thread shutting down");
        })
    }
}

#[derive(Debug)]
pub(crate) struct NumaWorkStealing2Inner {
    threads: Vec<thread::JoinHandle<()>>,
    work_inj: Arc<crossbeam::deque::Injector<async_task::Runnable>>,
    work_stealers: Vec<crossbeam::deque::Stealer<async_task::Runnable>>,
    work_flag: Arc<AtomicU8>,
    active: Arc<AtomicBool>,
    active_cnt: Arc<AtomicUsize>,
    num_tasks: Arc<AtomicUsize>,
    stall_mark: Arc<AtomicUsize>,
}

impl AmeSchedulerQueue for NumaWorkStealing2Inner {
    fn submit_am(
        //unserialized request
        &self,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
        ame: Arc<ActiveMessageEngineType>,
        am: Am,
    ) {
        let num_tasks = self.num_tasks.clone();
        let stall_mark = self.stall_mark.fetch_add(1, Ordering::Relaxed);
        let future = async move {
            // println!("exec req {:?}",num_tasks.load(Ordering::Relaxed));
            num_tasks.fetch_add(1, Ordering::Relaxed);
            // println!("in submit_req {:?} {:?} {:?} ", pe.clone(), req_data.src, req_data.pe);
            ame.process_msg(am, scheduler, stall_mark).await;
            // println!("num tasks: {:?}",);
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            // println!("done req {:?}",num_tasks.load(Ordering::Relaxed));
        };
        let work_inj = self.work_inj.clone();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = unsafe { async_task::spawn_unchecked(future, schedule) }; //safe as contents are sync+send, and no borrowed variables
        runnable.schedule();
        task.detach();
    }

    //this is a serialized request
    fn submit_work(
        &self,
        scheduler: &(impl SchedulerQueue + Sync + std::fmt::Debug),
        ame: Arc<ActiveMessageEngineType>,
        data: SerializedData,
        lamellae: Arc<Lamellae>,
    ) {
        // let work_inj = self.work_inj.clone();
        // println!("submit work {:?}",self.num_tasks.load(Ordering::Relaxed));
        let num_tasks = self.num_tasks.clone();
        let future = async move {
            // println!("exec work {:?}",num_tasks.load(Ordering::Relaxed)+1);
            num_tasks.fetch_add(1, Ordering::Relaxed);
            if let Some(header) = data.deserialize_header() {
                let msg = header.msg;
                ame.exec_msg(msg, data, lamellae, scheduler).await;
            } else {
                data.print();
                panic!("should i be here?");
            }
            // println!("num tasks: {:?}",);
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            // println!("done work {:?}",num_tasks.load(Ordering::Relaxed));
        };
        let work_inj = self.work_inj.clone();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = unsafe { async_task::spawn_unchecked(future, schedule) }; //safe as contents are sync+send, and no borrowed variables
        runnable.schedule();
        task.detach();
    }

    fn submit_task<F>(&self, future: F)
    where
        F: Future<Output = ()>,
    {
        // println!("submit task {:?}",self.num_tasks.load(Ordering::Relaxed));
        let num_tasks = self.num_tasks.clone();
        let future2 = async move {
            // println!("exec task {:?}",num_tasks.load(Ordering::Relaxed)+1);
            num_tasks.fetch_add(1, Ordering::Relaxed);
            future.await;
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            // println!("done task {:?}",num_tasks.load(Ordering::Relaxed));
        };
        let work_inj = self.work_inj.clone();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = unsafe { async_task::spawn_unchecked(future2, schedule) }; //safe //safe as contents are sync+send... may need to do something to enforce lifetime bounds
        runnable.schedule();
        task.detach();
    }

    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        let work_inj = self.work_inj.clone();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, mut task) = unsafe { async_task::spawn_unchecked(future, schedule) }; //safe //safe as contents are sync+send... may need to do something to enforce lifetime bounds
        let waker = runnable.waker();
        runnable.schedule();
        while !task.is_finished() {
            self.exec_task();
        }
        let cx = &mut async_std::task::Context::from_waker(&waker);
        if let async_std::task::Poll::Ready(output) = task.poll(cx) {
            output
        } else {
            panic!("task not ready");
        }
    }

    fn shutdown(&self) {
        // println!("work stealing shuting down {:?}", self.active());
        self.active.store(false, Ordering::SeqCst);
        // println!("work stealing shuting down {:?}",self.active());
        while self.active_cnt.load(Ordering::Relaxed) > 2
            || self.num_tasks.load(Ordering::Relaxed) > 2
        {
            //this should be the recvtask, and alloc_task
            std::thread::yield_now()
        }
        // println!(
        //     "work stealing shut down {:?} {:?} {:?}",
        //     self.active(),
        //     self.active_cnt.load(Ordering::Relaxed),
        //     self.active_cnt.load(Ordering::Relaxed)
        // );
    }

    fn exec_task(&self) {
        let mut rng = rand::thread_rng();
        let t = rand::distributions::Uniform::from(0..self.work_stealers.len());
        let ret = if self
            .work_flag
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::Relaxed)
            == Ok(0)
        {
            let ret = self.work_inj.steal().success();
            self.work_flag.store(0, Ordering::SeqCst);
            ret
        } else {
            self.work_stealers[t.sample(&mut rng)].steal().success()
        };
        if let Some(runnable) = ret {
            runnable.run();
        }
    }

    fn active(&self) -> bool {
        // println!("sched active {:?} {:?}",self.active.load(Ordering::SeqCst) , self.num_tasks.load(Ordering::SeqCst));
        self.active.load(Ordering::SeqCst) || self.num_tasks.load(Ordering::SeqCst) > 2
    }
}

//#[prof]
impl SchedulerQueue for NumaWorkStealing2 {
    fn submit_am(
        //unserialized request
        &self,
        am: Am,
    ) {
        let node =
            CUR_NODE.with(|cur_node| cur_node.fetch_add(1, Ordering::Relaxed) & self.node_mask);

        self.inners[node].submit_am(self, self.ames[node].clone(), am);
    }

    // fn submit_return(&self, src, pe)

    fn submit_work(&self, data: SerializedData, lamellae: Arc<Lamellae>) {
        // let node = if let Some(header) = data.deserialize_header() {
        //     let msg = header.msg;
        //     if let ExecType::Am(cmd) = msg.cmd.clone() {
        //         match cmd {
        //             Cmd::BatchedDataReturn | Cmd::BatchedAmReturn => {
        //                 println!(
        //                     "got batched return {:x} {:x}",
        //                     msg.req_id.id,
        //                     msg.req_id.id & self.node_mask
        //                 );
        //                 msg.req_id.id & self.node_mask
        //             }
        //             _ => CUR_NODE
        //                 .with(|cur_node| cur_node.fetch_add(1, Ordering::Relaxed) & self.node_mask),
        //         }
        //     } else {
        //         CUR_NODE.with(|cur_node| cur_node.fetch_add(1, Ordering::Relaxed) & self.node_mask)
        //     }
        // } else {
        //     CUR_NODE.with(|cur_node| cur_node.fetch_add(1, Ordering::Relaxed) & self.node_mask)
        // };
        // println!("submit work {:?}", node);
        let node =
            CUR_NODE.with(|cur_node| cur_node.fetch_add(1, Ordering::Relaxed) & self.node_mask);
        self.inners[node].submit_work(self, self.ames[node].clone(), data, lamellae);
    }

    fn submit_task<F>(&self, future: F)
    where
        F: Future<Output = ()>,
    {
        let node =
            CUR_NODE.with(|cur_node| cur_node.fetch_add(1, Ordering::Relaxed) & self.node_mask);
        self.inners[node].submit_task(future);
    }

    fn exec_task(&self) {
        let node =
            CUR_NODE.with(|cur_node| cur_node.fetch_add(1, Ordering::Relaxed) & self.node_mask);
        self.inners[node].exec_task();
    }

    fn submit_task_node<F>(&self, future: F, node: usize)
    where
        F: Future<Output = ()>,
    {
        self.inners[node].submit_task(future);
    }

    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        let node =
            CUR_NODE.with(|cur_node| cur_node.fetch_add(1, Ordering::Relaxed) & self.node_mask);
        self.inners[node].block_on(future)
    }

    fn shutdown(&self) {
        for inner in self.inners.iter() {
            inner.shutdown();
        }
    }
    fn active(&self) -> bool {
        for inner in self.inners.iter() {
            if inner.active() {
                return true;
            }
        }
        return false;
    }
}

//#[prof]
impl NumaWorkStealing2Inner {
    pub(crate) fn new(
        stall_mark: Arc<AtomicUsize>,
        core_ids: Vec<CoreId>,
    ) -> NumaWorkStealing2Inner {
        // println!("new work stealing queue");

        let mut sched = NumaWorkStealing2Inner {
            threads: Vec::new(),
            work_inj: Arc::new(crossbeam::deque::Injector::new()),
            work_stealers: Vec::new(),
            work_flag: Arc::new(AtomicU8::new(0)),
            active: Arc::new(AtomicBool::new(true)),
            active_cnt: Arc::new(AtomicUsize::new(0)),
            num_tasks: Arc::new(AtomicUsize::new(0)),
            stall_mark: stall_mark,
        };
        sched.init(core_ids);
        sched
    }

    fn init(&mut self, core_ids: Vec<CoreId>) {
        let mut work_workers: std::vec::Vec<crossbeam::deque::Worker<async_task::Runnable>> =
            vec![];
        // let num_workers = match std::env::var("LAMELLAR_THREADS") {
        //     Ok(n) => n.parse::<usize>().unwrap(),
        //     Err(_) => 4,
        // };
        for _i in 0..core_ids.len() {
            let work_worker: crossbeam::deque::Worker<async_task::Runnable> =
                crossbeam::deque::Worker::new_fifo();
            self.work_stealers.push(work_worker.stealer());
            work_workers.push(work_worker);
        }

        let orig_hook = panic::take_hook();
        panic::set_hook(Box::new(move |panic_info| {
            // invoke the default handler and exit the process
            orig_hook(panic_info);
            process::exit(1);
        }));
        // let core_ids = core_affinity::get_core_ids().unwrap();
        // println!("core_ids: {:?}",core_ids);
        for i in 0..core_ids.len() {
            let work_worker = work_workers.pop().unwrap();
            let worker = NumaWorkStealing2Thread {
                work_inj: self.work_inj.clone(),
                work_stealers: self.work_stealers.clone(),
                work_q: work_worker,
                work_flag: self.work_flag.clone(),
                active: self.active.clone(),
                // num_tasks: self.num_tasks.clone(),
            };
            self.threads.push(NumaWorkStealing2Thread::run(
                worker,
                self.active_cnt.clone(),
                self.num_tasks.clone(),
                core_ids[i % core_ids.len()],
            ));
        }
        while self.active_cnt.load(Ordering::SeqCst) != self.threads.len() {
            std::thread::yield_now();
        }
    }
}

thread_local! {
    static CUR_NODE: AtomicUsize = AtomicUsize::new(0);
}

#[derive(Debug)]
pub(crate) struct NumaWorkStealing2 {
    inners: Vec<Arc<AmeScheduler>>,
    ames: Vec<Arc<ActiveMessageEngineType>>,
    node_mask: usize,
}
impl NumaWorkStealing2 {
    pub(crate) fn new(
        num_pes: usize,
        // my_pe: usize,
        // teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
    ) -> NumaWorkStealing2 {
        // println!("new work stealing queue");

        let num_workers = match std::env::var("LAMELLAR_THREADS") {
            Ok(n) => n.parse::<usize>().unwrap(),
            Err(_) => 4,
        };
        let core_ids = core_affinity::get_core_ids().unwrap();
        println!("core_ids: {:?}", core_ids);
        let mut node_to_cores: HashMap<usize, Vec<usize>> = HashMap::new();
        let mut core_to_node: HashMap<usize, usize> = HashMap::new();

        let mut cur_worker_cnt = 0;

        if let Ok(nodes) = glob::glob("/sys/devices/system/node/node*") {
            for node in nodes {
                if let Ok(node_path) = node {
                    if let Some(node) = format!("{}", node_path.display()).split("/").last() {
                        if let Some(node) = node.strip_prefix("node") {
                            if let Ok(node) = node.parse::<usize>() {
                                if let Ok(cpus) =
                                    glob::glob(&format!("{}/cpu*", node_path.display()))
                                {
                                    let mut cores = Vec::new();
                                    for cpu in cpus {
                                        if let Ok(cpu) = cpu {
                                            if let Some(cpu) =
                                                format!("{}", cpu.display()).split("/").last()
                                            {
                                                if let Some(cpu) = cpu.strip_prefix("cpu") {
                                                    if let Ok(cpu) = cpu.parse::<usize>() {
                                                        for core_id in core_ids.iter() {
                                                            if core_id.id == cpu {
                                                                core_to_node.insert(cpu, node);
                                                                cores.push(cpu);
                                                                cur_worker_cnt += 1;
                                                            }
                                                            if cur_worker_cnt >= num_workers {
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    if cores.len() > 0 {
                                        node_to_cores.insert(node, cores);
                                    }
                                    if cur_worker_cnt >= num_workers {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        println!("node_to_cores {:?}", node_to_cores);
        println!("core_to_node {:?}", core_to_node);

        let mut inners = vec![];
        let mut ames = vec![];

        let mut node_mask = node_to_cores.len() - 1;
        node_mask |= node_mask >> 1;
        node_mask |= node_mask >> 2;
        node_mask |= node_mask >> 4;
        node_mask |= node_mask >> 8;
        node_mask |= node_mask >> 16;
        node_mask |= node_mask >> 32;

        // let mut node_i = 0;
        let stall_mark = Arc::new(AtomicUsize::new(0));
        for (_node, cores) in node_to_cores.iter() {
            let mut core_ids = vec![];
            for core in cores {
                core_ids.push(CoreId { id: *core });
            }
            let inner = Arc::new(AmeScheduler::NumaWorkStealing2Inner(
                NumaWorkStealing2Inner::new(stall_mark.clone(), core_ids),
            ));
            let batcher = match std::env::var("LAMELLAR_BATCHER") {
                Ok(n) => {
                    let n = n.parse::<usize>().unwrap();
                    if n == 1 {
                        BatcherType::Simple(SimpleBatcher::new(num_pes, stall_mark.clone()))
                    } else {
                        BatcherType::TeamAm(TeamAmBatcher::new(num_pes, stall_mark.clone()))
                    }
                }
                Err(_) => BatcherType::TeamAm(TeamAmBatcher::new(num_pes, stall_mark.clone())),
            };
            ames.push(Arc::new(ActiveMessageEngineType::RegisteredActiveMessages(
                RegisteredActiveMessages::new(batcher),
            )));
            inners.push(inner);
            // node_i += 1;
        }

        println!("numa node mask: {:x}", node_mask);

        let sched = NumaWorkStealing2 {
            inners: inners,
            ames: ames,
            node_mask: node_mask,
        };
        sched
    }
}

//#[prof]
impl Drop for NumaWorkStealing2Inner {
    //when is this called with respect to world?
    fn drop(&mut self) {
        // println!("dropping work stealing");
        while let Some(thread) = self.threads.pop() {
            if thread.thread().id() != std::thread::current().id() {
                let _res = thread.join();
            }
        }
        // for val in self.local_work_inj.iter_mut() {
        //     println!("local_work_inj {:?}", val.load(Ordering::SeqCst));
        // }
        // println!("NumaWorkStealing2 Scheduler Dropped");
    }
}

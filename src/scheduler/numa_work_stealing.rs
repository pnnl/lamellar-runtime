use crate::active_messaging::{ActiveMessageEngine, ActiveMessageEngineType, Am};
use crate::lamellae::{Des, Lamellae, SerializedData};
use crate::scheduler::batching::simple_batcher::SimpleBatcher;
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
// use std::collections::HashMap;
use std::collections::HashMap;
use std::panic;
use std::process;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc; //, Weak};
use std::thread;
use thread_local::ThreadLocal;
// use std::time::Instant;

pub(crate) struct NumaWorkStealingThread {
    node_work_inj: Arc<crossbeam::deque::Injector<async_task::Runnable>>,
    sys_work_inj: Vec<Arc<crossbeam::deque::Injector<async_task::Runnable>>>,
    node_work_stealers: Vec<crossbeam::deque::Stealer<async_task::Runnable>>,
    sys_work_stealers: HashMap<usize, Vec<crossbeam::deque::Stealer<async_task::Runnable>>>,
    work_q: Worker<async_task::Runnable>,
    work_flag: Arc<AtomicU8>,
    active: Arc<AtomicBool>,
}

//#[prof]
impl NumaWorkStealingThread {
    fn run(
        worker: NumaWorkStealingThread,
        active_cnt: Arc<AtomicUsize>,
        num_tasks: Arc<AtomicUsize>,
        id: CoreId,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            // println!("TestSchdulerWorker thread running");
            core_affinity::set_for_current(id);
            active_cnt.fetch_add(1, Ordering::SeqCst);
            let mut rng = rand::thread_rng();
            let t = rand::distributions::Uniform::from(0..worker.node_work_stealers.len());
            let mut timer = std::time::Instant::now();
            // let mut cur_tasks = num_tasks.load(Ordering::SeqCst);
            while worker.active.load(Ordering::SeqCst)
                || !(worker.work_q.is_empty() && worker.node_work_inj.is_empty())
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
                            .node_work_inj
                            .steal_batch_and_pop(&worker.work_q)
                            .success();
                        worker.work_flag.store(0, Ordering::SeqCst);
                        ret
                    } else {
                        worker.node_work_stealers[t.sample(&mut rng)]
                            .steal()
                            .success()
                    }
                });
                if let Some(runnable) = omsg {
                    if !worker.active.load(Ordering::SeqCst) && timer.elapsed().as_secs_f64() > 60.0
                    {
                        println!("runnable {:?}", runnable);
                        println!(
                            "work_q size {:?} work inj size {:?} num_tasks {:?}",
                            worker.work_q.len(),
                            worker.node_work_inj.len(),
                            num_tasks.load(Ordering::SeqCst)
                        );
                        timer = std::time::Instant::now();
                    }
                    runnable.run();
                }
                if !worker.active.load(Ordering::SeqCst)
                    && timer.elapsed().as_secs_f64() > 60.0
                    && (worker.work_q.len() > 0 || worker.node_work_inj.len() > 0)
                {
                    println!(
                        "work_q size {:?} work inj size {:?} num_tasks {:?}",
                        worker.work_q.len(),
                        worker.node_work_inj.len(),
                        num_tasks.load(Ordering::SeqCst)
                    );
                    timer = std::time::Instant::now();
                }
                if timer.elapsed().as_secs_f64() > 60.0 {
                    println!(
                        "work_q size {:?} work inj size {:?} num_tasks {:?}",
                        worker.work_q.len(),
                        worker.node_work_inj.len(),
                        num_tasks.load(Ordering::SeqCst)
                    );
                    timer = std::time::Instant::now()
                }
            }
            fini_prof!();
            active_cnt.fetch_sub(1, Ordering::SeqCst);
            // println!("TestSchdulerWorker thread shutting down");
        })
    }
}

/*
create a work injector and stealer for each numa node,
additionally create a threadlocal counter that each thread will use to index
into the to appropriate work injector when submitting work
*/
pub(crate) struct NumaWorkStealingInner {
    threads: Vec<thread::JoinHandle<()>>,
    work_inj: Vec<Arc<crossbeam::deque::Injector<async_task::Runnable>>>,
    work_stealers: HashMap<usize, Vec<crossbeam::deque::Stealer<async_task::Runnable>>>,
    work_flag: Arc<AtomicU8>,
    active: Arc<AtomicBool>,
    active_cnt: Arc<AtomicUsize>,
    num_tasks: Arc<AtomicUsize>,
    stall_mark: Arc<AtomicUsize>,
    local_work_inj: ThreadLocal<AtomicUsize>,
    nodes: Vec<usize>,
}

impl AmeSchedulerQueue for NumaWorkStealingInner {
    fn submit_am(
        //unserialized request
        &self,
        ame: Arc<ActiveMessageEngineType>,
        am: Am,
    ) {
        let num_tasks = self.num_tasks.clone();
        let stall_mark = self.stall_mark.fetch_add(1, Ordering::Relaxed);
        let future = async move {
            // println!("exec req {:?}",num_tasks.load(Ordering::Relaxed));
            num_tasks.fetch_add(1, Ordering::Relaxed);
            // println!("in submit_req {:?} {:?} {:?} ", pe.clone(), req_data.src, req_data.pe);
            ame.process_msg(am, self, stall_mark).await;
            // println!("num tasks: {:?}",);
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            // println!("done req {:?}",num_tasks.load(Ordering::Relaxed));
        };
        let work_inj = self.work_inj[self
            .local_work_inj
            .get_or(|| AtomicUsize::new(0))
            .fetch_add(1, Ordering::SeqCst)
            % self.work_inj.len()]
        .clone();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = unsafe { async_task::spawn_unchecked(future, schedule) }; //safe as contents are sync+send, and no borrowed variables
        runnable.schedule();
        task.detach();
    }

    //this is a serialized request
    fn submit_work(
        &self,
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
                ame.exec_msg(msg, data, lamellae, self).await;
            } else {
                data.print();
                panic!("should i be here?");
            }
            // println!("num tasks: {:?}",);
            num_tasks.fetch_sub(1, Ordering::Relaxed);
            // println!("done work {:?}",num_tasks.load(Ordering::Relaxed));
        };
        let work_inj = self.work_inj[self
            .local_work_inj
            .get_or(|| AtomicUsize::new(0))
            .fetch_add(1, Ordering::SeqCst)
            % self.work_inj.len()]
        .clone();
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
        let work_inj = self.work_inj[self
            .local_work_inj
            .get_or(|| AtomicUsize::new(0))
            .fetch_add(1, Ordering::SeqCst)
            % self.work_inj.len()]
        .clone();
        let schedule = move |runnable| work_inj.push(runnable);
        let (runnable, task) = unsafe { async_task::spawn_unchecked(future2, schedule) }; //safe //safe as contents are sync+send... may need to do something to enforce lifetime bounds
        runnable.schedule();
        task.detach();
    }

    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        let work_inj = self.work_inj[self
            .local_work_inj
            .get_or(|| AtomicUsize::new(0))
            .fetch_add(1, Ordering::SeqCst)
            % self.work_inj.len()]
        .clone();
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
        // let c = rand::distributions::Uniform::from(0..self.work_stealers.len());
        // let c = rand::distributions::Uniform::from
        let ret = if self
            .work_flag
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::Relaxed)
            == Ok(0)
        {
            let ret = self
                .nodes
                .choose_multiple(&mut rng, self.nodes.len())
                .find_map(|node| self.work_inj[*node % self.nodes.len()].steal().success());
            self.work_flag.store(0, Ordering::SeqCst);
            ret
        } else {
            self.nodes
                .choose_multiple(&mut rng, self.nodes.len())
                .find_map(|node| {
                    self.work_stealers[node]
                        .choose(&mut rng)
                        .unwrap()
                        .steal()
                        .success()
                })
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
impl SchedulerQueue for NumaWorkStealing {
    fn submit_am(
        //unserialized request
        &self,
        am: Am,
    ) {
        self.inner.submit_am(self.ame.clone(), am);
    }

    // fn submit_return(&self, src, pe)

    fn submit_work(&self, data: SerializedData, lamellae: Arc<Lamellae>) {
        self.inner.submit_work(self.ame.clone(), data, lamellae);
    }

    fn submit_task<F>(&self, future: F)
    where
        F: Future<Output = ()>,
    {
        self.inner.submit_task(future);
    }

    fn exec_task(&self) {
        self.inner.exec_task();
    }

    fn submit_task_node<F>(&self, future: F, _node: usize)
    where
        F: Future<Output = ()>,
    {
        self.inner.submit_task(future);
    }

    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        self.inner.block_on(future)
    }

    fn shutdown(&self) {
        self.inner.shutdown();
    }
    fn active(&self) -> bool {
        self.inner.active()
    }
}

//#[prof]
impl NumaWorkStealingInner {
    pub(crate) fn new(stall_mark: Arc<AtomicUsize>) -> NumaWorkStealingInner {
        // println!("new work stealing queue");

        let mut sched = NumaWorkStealingInner {
            threads: Vec::new(),
            work_inj: Vec::new(), //Arc::new(crossbeam::deque::Injector::new()),
            work_stealers: HashMap::new(), //Vec::new(),
            work_flag: Arc::new(AtomicU8::new(0)),
            active: Arc::new(AtomicBool::new(true)),
            active_cnt: Arc::new(AtomicUsize::new(0)),
            num_tasks: Arc::new(AtomicUsize::new(0)),
            stall_mark: stall_mark,
            local_work_inj: ThreadLocal::new(),
            nodes: Vec::new(),
        };
        sched.local_work_inj.get_or(|| AtomicUsize::new(0));
        sched.init();
        sched
    }

    fn init(&mut self) {
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

        let mut work_workers = HashMap::new();
        for (node, cores) in &node_to_cores {
            let mut node_work_workers: std::vec::Vec<
                crossbeam::deque::Worker<async_task::Runnable>,
            > = vec![];
            let mut node_work_stealers = vec![];
            for core in cores {
                let core_work_worker: crossbeam::deque::Worker<async_task::Runnable> =
                    crossbeam::deque::Worker::new_fifo();
                node_work_stealers.push(core_work_worker.stealer());
                node_work_workers.push(core_work_worker);
            }
            self.work_inj
                .push(Arc::new(crossbeam::deque::Injector::new()));
            self.work_stealers.insert(*node, node_work_stealers);
            work_workers.insert(node, node_work_workers);
            self.nodes.push(*node);
        }

        let orig_hook = panic::take_hook();
        panic::set_hook(Box::new(move |panic_info| {
            // invoke the default handler and exit the process
            orig_hook(panic_info);
            process::exit(1);
        }));

        let mut inj = 0;
        for (node, cores) in &node_to_cores {
            let node_work_workers = work_workers.get_mut(&node).unwrap();
            for core in cores {
                let core_work_worker = node_work_workers.pop().unwrap();
                let worker = NumaWorkStealingThread {
                    node_work_inj: self.work_inj[inj].clone(),
                    sys_work_inj: self.work_inj.clone(),
                    node_work_stealers: self.work_stealers.get(&node).unwrap().clone(),
                    sys_work_stealers: self.work_stealers.clone(),
                    work_q: core_work_worker,
                    work_flag: self.work_flag.clone(),
                    active: self.active.clone(),
                };
                self.threads.push(NumaWorkStealingThread::run(
                    worker,
                    self.active_cnt.clone(),
                    self.num_tasks.clone(),
                    CoreId { id: *core },
                ));
            }
            inj += 1;
        }

        while self.active_cnt.load(Ordering::SeqCst) != self.threads.len() {
            std::thread::yield_now();
        }
    }
}

pub(crate) struct NumaWorkStealing {
    inner: Arc<AmeScheduler>,
    ame: Arc<ActiveMessageEngineType>,
}
impl NumaWorkStealing {
    pub(crate) fn new(
        num_pes: usize,
        // my_pe: usize,
        // teams: Arc<RwLock<HashMap<u64, Weak<LamellarTeamRT>>>>,
    ) -> NumaWorkStealing {
        // println!("new work stealing queue");
        let stall_mark = Arc::new(AtomicUsize::new(0));
        let inner = Arc::new(AmeScheduler::NumaWorkStealingInner(
            NumaWorkStealingInner::new(stall_mark.clone()),
        ));
        let sched = NumaWorkStealing {
            inner: inner.clone(),
            ame: Arc::new(ActiveMessageEngineType::RegisteredActiveMessages(
                RegisteredActiveMessages::new(BatcherType::Simple(SimpleBatcher::new(
                    num_pes,
                    stall_mark.clone(),
                ))),
            )),
        };
        sched
    }
}

//#[prof]
impl Drop for NumaWorkStealingInner {
    //when is this called with respect to world?
    fn drop(&mut self) {
        // println!("dropping work stealing");
        while let Some(thread) = self.threads.pop() {
            if thread.thread().id() != std::thread::current().id() {
                let _res = thread.join();
            }
        }
        for val in self.local_work_inj.iter_mut() {
            println!("local_work_inj {:?}", val.load(Ordering::SeqCst));
        }
        // println!("NumaWorkStealing Scheduler Dropped");
    }
}

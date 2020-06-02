use crate::lamellae;
use crate::lamellae::Lamellae;
use crate::lamellar_request::*;
use crate::schedulers;
use crate::schedulers::Scheduler;
use crate::utils::ser_closure;

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

lazy_static! {
    pub(crate) static ref LAMELLAR_RT: Runtime = {
        let mut rt = Runtime::new();
        rt.init();
        rt
    };
}

#[cfg(feature = "WorkStealingSched")]
lazy_static! {
    pub(crate) static ref SCHEDULER: schedulers::WorkStealing = {
        let sched = schedulers::WorkStealing::new();
        sched
    };
}

#[cfg(feature = "SharedChannelSched")]
lazy_static! {
    pub(crate) static ref SCHEDULER: schedulers::SharedChannels = {
        let sched = schedulers::SharedChannels::new();
        sched
    };
}

#[cfg(feature = "PrivateChannelSched")]
lazy_static! {
    pub(crate) static ref SCHEDULER: schedulers::PrivateChannels = {
        let sched = schedulers::PrivateChannels::new();
        sched
    };
}

lazy_static! {
    pub(crate) static ref REQUESTS: Vec<chashmap::CHashMap<usize, InternalReq>> = {
        let mut reqs = Vec::new();
        for _i in 0..100 {
            reqs.push(chashmap::CHashMap::new());
        }
        reqs
    };
}

pub(crate) type LamellarLocal = Box<dyn FnOnce<(), Output = (RetType, Option<Vec<u8>>)> + Send>;
pub(crate) type LamellarClosure = Box<dyn FnOnce<(), Output = Vec<u8>> + Send>;
pub(crate) type LamellarAny = Box<dyn std::any::Any + Send>;

#[repr(u8)]
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
pub(crate) enum Cmd {
    ClosureReq,
    ClosureReqLocal,
    ClosureResp,
    DataReq,
    DataResp,
    BatchReq,
    BatchResp,
    CompResp,
    Barrier,
    BarrierResp,
    UnitResp,
    BuffReq,
    NoHandleResp,
    AggregationReq,
    PutReq,
    GetReq,
    GetResp,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub(crate) struct Msg {
    pub cmd: Cmd,
    pub src: u16,
    pub id: usize,
    pub return_data: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub(crate) enum RetType {
    //maybe change to ReqType? ReturnRequestType?
    Unit,
    Closure,
    Data,
    Barrier,
    NoHandle,
    Put,
    Get,
}

#[derive(Clone, Debug)]
pub(crate) struct Arch {
    pub(crate) my_pe: usize,
    pub(crate) num_pes: usize,
    pub(crate) pe_addrs: Vec<std::string::String>,
    pub(crate) job_id: usize,
}

#[derive(Clone)]
pub(crate) struct Counters {
    pub(crate) outstanding_reqs: Arc<AtomicUsize>,
    pub(crate) send_req_cnt: Arc<AtomicUsize>,
    pub(crate) recv_req_cnt: Arc<AtomicUsize>,
    pub(crate) pending_ret_cnts: Vec<Arc<AtomicUsize>>,
    pub(crate) pending_ret_active: Vec<Arc<AtomicUsize>>,
    pub(crate) pending_nohandle_cnts: Vec<Arc<AtomicUsize>>,
    pub(crate) pending_nohandle_active: Vec<Arc<AtomicUsize>>,
    pub(crate) cur_req_id: Arc<AtomicUsize>,
    pub(crate) time1: Arc<AtomicU64>,
    pub(crate) time2: Arc<AtomicU64>,
    pub(crate) time3: Arc<AtomicU64>,
    pub(crate) time4: Arc<AtomicU64>,
}

pub(crate) struct Runtime {
    pub(crate) arch: Arch,
    pub(crate) counters: Counters,
    pending_resp: Vec<crossbeam::queue::SegQueue<usize>>,

    #[cfg(feature = "SocketsBackend")]
    pub(crate) lamellae: lamellae::sockets_lamellae::Runtime, //can we used procedural macros to detect the backend at compile time?
    #[cfg(feature = "SocketsBackend")]
    pub(crate) lamellae2: lamellae::sockets_lamellae::Runtime, //can we used procedural macros to detect the backend at compile time?

    #[cfg(feature = "RofiBackend")]
    pub(crate) lamellae: lamellae::rofi_lamellae::Runtime,
    #[cfg(feature = "RofiBackend")]
    pub(crate) lamellae2: lamellae::rofi_lamellae::Runtime,

    pub(crate) nohandle_ireq: Arc<InternalReq>, //a dummy internal req to be reused when we dont need to return request handles
}

impl Runtime {
    pub fn new() -> Runtime {
        let (dummy_s, _) = crossbeam::channel::unbounded();
        Runtime {
            arch: Arch {
                my_pe: 0,
                num_pes: 0,
                pe_addrs: Vec::new(),
                job_id: 0,
            },
            counters: Counters {
                outstanding_reqs: Arc::new(AtomicUsize::new(0)),
                send_req_cnt: Arc::new(AtomicUsize::new(0)),
                recv_req_cnt: Arc::new(AtomicUsize::new(0)),
                pending_ret_cnts: Vec::new(),
                pending_ret_active: Vec::new(),
                pending_nohandle_cnts: Vec::new(),
                pending_nohandle_active: Vec::new(),
                cur_req_id: Arc::new(AtomicUsize::new(1)),
                time1: Arc::new(AtomicU64::new(0)),
                time2: Arc::new(AtomicU64::new(0)),
                time3: Arc::new(AtomicU64::new(0)),
                time4: Arc::new(AtomicU64::new(0)),
            },
            pending_resp: Vec::new(),
            #[cfg(feature = "SocketsBackend")]
            lamellae: lamellae::sockets_lamellae::Runtime::new(50000, 0),
            #[cfg(feature = "SocketsBackend")]
            lamellae2: lamellae::sockets_lamellae::Runtime::new(50000, 2),
            #[cfg(feature = "RofiBackend")]
            lamellae: lamellae::rofi_lamellae::Runtime::new(),
            #[cfg(feature = "RofiBackend")]
            lamellae2: lamellae::rofi_lamellae::Runtime::new(),

            nohandle_ireq: Arc::new(InternalReq {
                data_tx: dummy_s,
                cnt: Arc::new(AtomicUsize::new(0usize)),
                start: Instant::now(),
                size: 0,
                active: Arc::new(AtomicBool::new(false)),
            }),
        }
    }

    pub fn init(&mut self) {
        self.arch = self.lamellae.init();

        for _pe in 0..self.arch.num_pes {
            self.counters
                .pending_ret_cnts
                .push(Arc::new(AtomicUsize::new(0)));
            self.counters
                .pending_ret_active
                .push(Arc::new(AtomicUsize::new(0)));
            self.counters
                .pending_nohandle_cnts
                .push(Arc::new(AtomicUsize::new(0)));
            self.counters
                .pending_nohandle_active
                .push(Arc::new(AtomicUsize::new(0)));
            self.pending_resp.push(crossbeam::queue::SegQueue::new());
        }
    }

    pub fn finit(&self) {
        self.lamellae.finit();
    }

    pub fn get_arch(&self) -> (usize, usize) {
        (self.arch.my_pe, self.arch.num_pes)
    }

    pub fn wait_all(&self) {
        let mut temp_now = Instant::now();
        while self.counters.outstanding_reqs.load(Ordering::SeqCst) > 0 {
            std::thread::yield_now();
            if temp_now.elapsed() > Duration::new(60, 0) {
                println!(
                    "in wait_all mype: {:?} cnt: {:?} {:?} {:?}",
                    self.arch.my_pe,
                    self.counters.send_req_cnt.load(Ordering::SeqCst),
                    self.counters.outstanding_reqs.load(Ordering::SeqCst),
                    self.counters.recv_req_cnt.load(Ordering::SeqCst),
                );
                self.lamellae.print_stats();
                temp_now = Instant::now();
            }
        }
    }
}

fn send_response(cmd: Cmd, data: Option<std::vec::Vec<u8>>, msg: Msg) {
    match cmd {
        // Cmd::CompResp => {
        Cmd::NoHandleResp => {
            // println!("no handle resp");
            LAMELLAR_RT.counters.pending_nohandle_cnts[msg.src as usize]
                .fetch_add(1, Ordering::SeqCst);
            if LAMELLAR_RT.counters.pending_nohandle_active[msg.src as usize].compare_and_swap(
                0,
                1,
                Ordering::SeqCst,
            ) == 0
            {
                //means no other return request initiated yet
                let src = msg.src;
                let data = ser_closure(FnOnce!(
                    [src] move || {
                        LAMELLAR_RT.counters.pending_nohandle_active[src  as usize].store(0,Ordering::SeqCst);
                        let cnt = LAMELLAR_RT.counters.pending_nohandle_cnts[src  as usize].load(Ordering::SeqCst);
                        LAMELLAR_RT.counters.pending_nohandle_cnts[src  as usize].fetch_sub(cnt,Ordering::SeqCst);

                        let rmsg = Msg {
                        cmd: Cmd::NoHandleResp,
                        src: LAMELLAR_RT.arch.my_pe as u16,
                        id: 0,
                        return_data: false,
                        };
                        let data = bincode::serialize(&cnt).unwrap();
                        let payload = (rmsg,data);
                        LAMELLAR_RT.lamellae.send_to_pe(src as usize,bincode::serialize(&payload).unwrap());
                        (RetType::Unit,None::<Vec<u8>>)
                }));
                let rmsg = Msg {
                    cmd: Cmd::BatchReq,
                    src: src as u16,
                    id: msg.id,
                    return_data: false,
                };
                let payload = (rmsg, data);
                SCHEDULER.submit_work(bincode::serialize(&payload).unwrap());
            }
        }
        Cmd::UnitResp => {
            LAMELLAR_RT.pending_resp[msg.src as usize].push(msg.id);
            if LAMELLAR_RT.counters.pending_ret_active[msg.src as usize].compare_and_swap(
                0,
                1,
                Ordering::SeqCst,
            ) == 0
            {
                //means no other return request initiated yet
                let src = msg.src;
                let data = ser_closure(FnOnce!(
                [src,cmd] move || ->(RetType, Option<std::vec::Vec<u8>>){
                    LAMELLAR_RT.counters.pending_ret_active[src  as usize].store(0,Ordering::SeqCst);
                    let mut cnt = LAMELLAR_RT.pending_resp[src as usize].len();

                    while cnt > 0{
                        let mut i =0;
                        let mut ids: std::vec::Vec<usize> = std::vec::Vec::new();
                        while i < 100_000 && cnt > 0{
                            if let Ok(id) = LAMELLAR_RT.pending_resp[src as usize].pop(){
                                ids.push(id);
                            }
                            i+=1;
                            cnt-=1;
                        }
                        // println!("sending {:?} responses",i);
                        let rmsg = Msg {
                        cmd: cmd.clone(),
                        src: LAMELLAR_RT.arch.my_pe as u16,
                        id: 0,
                        return_data: false,
                        };
                        let data = bincode::serialize(&ids).unwrap();
                        let payload = (rmsg,data);
                        LAMELLAR_RT.lamellae.send_to_pe(src as usize,bincode::serialize(&payload).unwrap());
                    }
                    (RetType::Unit,None)
                }));
                let rmsg = Msg {
                    cmd: Cmd::BatchReq,
                    src: src as u16,
                    id: msg.id,
                    return_data: false,
                };
                let payload = (rmsg, data);
                SCHEDULER.submit_work(bincode::serialize(&payload).unwrap());
            }
        }
        Cmd::BarrierResp | Cmd::ClosureResp | Cmd::DataResp | Cmd::BatchResp | Cmd::GetResp => {
            let rmsg = Msg {
                cmd: cmd,
                src: LAMELLAR_RT.arch.my_pe as u16,
                id: msg.id,
                return_data: false,
            };
            let data = data.unwrap();
            let payload = (rmsg, data);
            LAMELLAR_RT
                .lamellae
                .send_to_pe(msg.src as usize, bincode::serialize(&payload).unwrap());
        }
        _ => println!("unknown command {:#?}", cmd),
    }
}

// #[flame]
fn exec_closure(data: &[u8]) -> (RetType, Option<std::vec::Vec<u8>>) {
    let closure: serde_closure::FnOnce<
        (std::vec::Vec<u8>,),
        fn((std::vec::Vec<u8>,), ()) -> (RetType, Option<std::vec::Vec<u8>>),
    > = bincode::deserialize(&data).unwrap();
    closure()
}

// #[flame]
fn send_data_to_user_handle(req_id: usize, pe: u16, data: Option<std::vec::Vec<u8>>) {
    let res = REQUESTS[req_id % REQUESTS.len()].get(&req_id);
    match res {
        Some(v) => {
            let ireq = v.clone();
            drop(v); //release lock in the hashmap
            if let Ok(_) = ireq.data_tx.send((pe as usize, data)) {}
            let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
            if cnt == 1 {
                REQUESTS[req_id % REQUESTS.len()].remove(&req_id);
            }
        }
        None => println!("error id not found"),
    }
}

pub(crate) fn exec_msg(msg: Msg, ser_data: Vec<u8>) {
    match msg.cmd {
        Cmd::ClosureReq => {
            // println!("received req: {:?}", msg.id);
            let t = std::time::Instant::now();
            LAMELLAR_RT
                .counters
                .recv_req_cnt
                .fetch_add(1, Ordering::SeqCst);
            let (ret_type, data) = exec_closure(&ser_data);
            let mut msec = t.elapsed().as_secs() * 1000;
            msec += t.elapsed().subsec_millis() as u64;
            LAMELLAR_RT.counters.time1.fetch_add(msec, Ordering::SeqCst);
            let t = std::time::Instant::now();
            if msg.return_data {
                match ret_type {
                    RetType::Closure => send_response(Cmd::ClosureResp, data, msg),
                    RetType::Data => send_response(Cmd::DataResp, data, msg),
                    RetType::Unit => send_response(Cmd::UnitResp, None, msg),
                    _ => println!("unexpected return type"),
                };
            } else {
                match ret_type {
                    RetType::Closure => send_response(Cmd::ClosureResp, data, msg),
                    RetType::Data => send_response(Cmd::NoHandleResp, data, msg),
                    RetType::Unit => send_response(Cmd::NoHandleResp, None, msg),
                    _ => println!("unexpected return type"),
                }
            }
            let mut msec = t.elapsed().as_secs() * 1000;
            msec += t.elapsed().subsec_millis() as u64;
            LAMELLAR_RT.counters.time2.fetch_add(msec, Ordering::SeqCst);
        }
        Cmd::ClosureResp => {
            // println!(
            //     "{:?} received closure resp from {:?} resp for req: {:?} {:?}",
            //     LAMELLAR_RT.arch.my_pe,
            //     msg.src,
            //     msg.id,
            //     LAMELLAR_RT.counters.outstanding_reqs.load(Ordering::SeqCst)
            // );
            exec_closure(&ser_data);
            send_data_to_user_handle(msg.id, msg.src, None);
            LAMELLAR_RT
                .counters
                .outstanding_reqs
                .fetch_sub(1, Ordering::SeqCst);
        }
        Cmd::DataResp => {
            // println!("{:?} received data resp from {:?} resp for req: {:?} {:?}",LAMELLAR_RT.arch.my_pe,msg.src, msg.id,LAMELLAR_RT.counters.outstanding_reqs.load(Ordering::SeqCst));

            send_data_to_user_handle(msg.id, msg.src, Some(ser_data));
            LAMELLAR_RT
                .counters
                .outstanding_reqs
                .fetch_sub(1, Ordering::SeqCst);
        }
        Cmd::Barrier => {
            // println!("received barrier");
            let (ret_type, data) = exec_closure(&ser_data);
            match ret_type {
                RetType::Closure => send_response(Cmd::BarrierResp, data, msg),
                RetType::Barrier => (),
                _ => println!("unexpected return type {:?}", ret_type),
            }
        }
        Cmd::BarrierResp => {
            // println!("received barrier resp");
            exec_closure(&ser_data);
        }
        Cmd::PutReq => {
            // println!("received put");
            exec_closure(&ser_data);
        }
        Cmd::GetReq => {
            // println!("received get");
            let (ret_type, data) = exec_closure(&ser_data);
            match ret_type {
                RetType::Closure => send_response(Cmd::GetResp, data, msg),
                _ => println!("unexpected return type {:?}", ret_type),
            }
        }
        Cmd::GetResp => {
            // println!("received GetResp");
            exec_closure(&ser_data);
        }
        Cmd::BatchReq => {
            // println!("batch request");
            if LAMELLAR_RT.counters.pending_ret_active[msg.src as usize]
                .fetch_add(1, Ordering::SeqCst)
                < 10
            {
                let payload = (msg, ser_data);
                SCHEDULER.submit_work(bincode::serialize(&payload).unwrap());
            } else {
                exec_closure(&ser_data);
            }
        }
        Cmd::BatchResp => {
            //println!("batch request");
            exec_closure(&ser_data);
        }
        Cmd::UnitResp => {
            let t = std::time::Instant::now();
            let ids: std::vec::Vec<usize> = bincode::deserialize(&ser_data).unwrap();
            // println!("{:?} received unit resp from {:?} resp for req: {:?} {:?} {:?}",LAMELLAR_RT.arch.my_pe,msg.src, msg.id,LAMELLAR_RT.counters.outstanding_reqs.load(Ordering::SeqCst),ids.len());

            let mut msec = t.elapsed().as_secs() * 1000;
            msec += t.elapsed().subsec_millis() as u64;
            LAMELLAR_RT.counters.time3.fetch_add(msec, Ordering::SeqCst);
            let t = std::time::Instant::now();
            for id in &ids {
                send_data_to_user_handle(*id, msg.src, Some(bincode::serialize(&()).unwrap()));
            }
            LAMELLAR_RT
                .counters
                .outstanding_reqs
                .fetch_sub(ids.len(), Ordering::SeqCst);
            let mut msec = t.elapsed().as_secs() * 1000;
            msec += t.elapsed().subsec_millis() as u64;
            LAMELLAR_RT.counters.time4.fetch_add(msec, Ordering::SeqCst);
        }
        Cmd::NoHandleResp => {
            let cnt: usize = bincode::deserialize(&ser_data).unwrap();
            // println!("{:?} received nohandle resp from {:?} resp for req: {:?} {:?} {:?}",LAMELLAR_RT.arch.my_pe,msg.src, msg.id,LAMELLAR_RT.counters.outstanding_reqs.load(Ordering::SeqCst),cnt);
            LAMELLAR_RT
                .counters
                .outstanding_reqs
                .fetch_sub(cnt, Ordering::SeqCst);
        }
        Cmd::AggregationReq => {
            exec_closure(&ser_data);
        }
        _ => println!("unknown command"),
    };
}

pub(crate) fn exec_local(msg: Msg, func: LamellarLocal, ireq: InternalReq) {
    match msg.cmd {
        Cmd::ClosureReqLocal => {}
        Cmd::ClosureReq => {
            // println!("local received req: {:?}", msg.id);
            LAMELLAR_RT
                .counters
                .recv_req_cnt
                .fetch_add(1, Ordering::SeqCst);
            let (ret_type, data) = func();
            match ret_type {
                RetType::Unit => {
                    if msg.return_data {
                        if let Ok(_) = ireq
                            .data_tx
                            .send((msg.src as usize, Some(bincode::serialize(&()).unwrap())))
                        {
                        }
                        let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
                        if cnt == 1 {
                            REQUESTS[msg.id % REQUESTS.len()].remove(&msg.id);
                        }
                    }
                }
                RetType::Closure => {
                    // exec_msg(msg, data.expect("error executing return closure"));
                    exec_closure(&data.expect("error executing return closure"));
                    send_data_to_user_handle(msg.id, msg.src, None);
                }
                RetType::Data => {
                    if msg.return_data {
                        if let Ok(_) = ireq.data_tx.send((msg.src as usize, data)) {}
                        let cnt = ireq.cnt.fetch_sub(1, Ordering::SeqCst);
                        if cnt == 1 {
                            REQUESTS[msg.id % REQUESTS.len()].remove(&msg.id);
                        }
                    }
                }
                _ => println!("unexpected return type"),
            };
            LAMELLAR_RT
                .counters
                .outstanding_reqs
                .fetch_sub(1, Ordering::SeqCst);
        }
        Cmd::ClosureResp => {
            // println!(
            //     "local {:?} received closure resp from {:?} resp for req: {:?} {:?}",
            //     LAMELLAR_RT.arch.my_pe,
            //     msg.src,
            //     msg.id,
            //     LAMELLAR_RT.counters.outstanding_reqs.load(Ordering::SeqCst)
            // );
            func();
            send_data_to_user_handle(msg.id, msg.src, None);
            LAMELLAR_RT
                .counters
                .outstanding_reqs
                .fetch_sub(1, Ordering::SeqCst);
        }
        Cmd::Barrier => {
            // println!("received barrier");
            let (ret_type, data) = func(); //exec_closure(&ser_data);
            match ret_type {
                RetType::Closure => send_response(Cmd::BarrierResp, data, msg),
                RetType::Barrier => (),
                _ => println!("unexpected return type {:?}", ret_type),
            }
        }
        Cmd::BarrierResp => {
            // println!("received barrier resp");
            func();
        }
        Cmd::BatchReq => {
            println!("batch request");
            func();
        }
        Cmd::BatchResp => {
            //println!("batch request");
            func();
        }
        Cmd::DataResp => {
            //should not happen (handled in Cmd::ClosureReq)
            println!(
                "{:?} received data resp from {:?} resp for req: {:?} {:?}",
                LAMELLAR_RT.arch.my_pe,
                msg.src,
                msg.id,
                LAMELLAR_RT.counters.outstanding_reqs.load(Ordering::SeqCst)
            );
        }
        Cmd::UnitResp => {
            //should not happen (handled in Cmd::ClosureReq)
            println!(
                "{:?} received unit resp from {:?} resp for req: {:?} {:?}",
                LAMELLAR_RT.arch.my_pe,
                msg.src,
                msg.id,
                LAMELLAR_RT.counters.outstanding_reqs.load(Ordering::SeqCst)
            );
        }
        _ => println!("unknown command"),
    };
}

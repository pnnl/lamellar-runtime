use crate::lamellar_request::InternalReq;
use crate::runtime::LAMELLAR_RT;
use crate::runtime::*;
use crate::schedulers::Scheduler;

pub(crate) struct SharedChannels {
    threads: Vec<std::thread::JoinHandle<()>>,
    work_tx: crossbeam::channel::Sender<Vec<u8>>,
    msg_tx: crossbeam::channel::Sender<(usize, Msg, InternalReq, LamellarAny)>,
    num_threads: usize,
    num_pes: usize,
}

impl Scheduler for SharedChannels {
    fn new() -> SharedChannels {
        let (work_s, work_r) = crossbeam::channel::unbounded();
        let (msg_s, msg_r) = crossbeam::channel::unbounded();
        // let (msg_s, msg_r) = crossbeam::channel::bounded(100);

        let mut scheduler = SharedChannels {
            threads: Vec::new(),
            work_tx: work_s,
            msg_tx: msg_s,
            num_threads: 1,
            num_pes: LAMELLAR_RT.arch.num_pes,
        };
        scheduler.num_threads = match std::env::var("LAMELLAR_THREADS") {
            Ok(n) => n.parse::<usize>().unwrap(),
            Err(_) => 4,
        };
        // println!("num threads: {:?}", scheduler.num_threads);
        for tid in 0..scheduler.num_threads {
            let cloned_work_r = work_r.clone();
            let cloned_msg_r = msg_r.clone();
            scheduler.threads.push(std::thread::spawn(move || {
                SharedChannels::init_threads(tid, cloned_work_r, cloned_msg_r);
            }));
        }
        scheduler
    }

    fn submit_req(&self, pe: usize, msg: Msg, ireq: InternalReq, func: LamellarAny) {
        self.msg_tx
            .send((pe, msg, ireq, func))
            .expect("error in sending to msg q");
    }

    fn submit_req_all(&self, msg: Msg, ireq: InternalReq, func: LamellarAny) {
        self.msg_tx
            .send((self.num_pes, msg, ireq, func))
            .expect("error in sending to msg q");
    }

    fn submit_work(&self, msg: std::vec::Vec<u8>) {
        self.work_tx.send(msg).expect("error in sending to work q");
    }
}

impl SharedChannels {
    fn init_threads(
        _id: usize,
        work_rx: crossbeam::channel::Receiver<Vec<u8>>,
        msg_rx: crossbeam::channel::Receiver<(usize, Msg, InternalReq, LamellarAny)>,
    ) {
        let my_pe = LAMELLAR_RT.arch.my_pe;
        let num_pes = LAMELLAR_RT.arch.num_pes;
        loop {
            select! {
                recv(work_rx) -> msg => {
                    let m = msg.expect("error in exec_work recv");
                    // let t = std::time::Instant::now();
                    let (msg,ser_data): (Msg,Vec<u8>) = bincode::deserialize(&m).unwrap();
                    // let mut msec = t.elapsed().as_secs()*1000;
                    // msec += t.elapsed().subsec_millis() as u64;
                    // LAMELLAR_RT.counters.time1.fetch_add(msec,Ordering::SeqCst);
                    // let t = std::time::Instant::now();
                    exec_msg(msg,ser_data);
                    // let mut msec = t.elapsed().as_secs()*1000;
                    // msec += t.elapsed().subsec_millis() as u64;
                    // LAMELLAR_RT.counters.time4.fetch_add(msec,Ordering::SeqCst);
                }
                recv(msg_rx) -> msg => SharedChannels::process_msg(msg.expect("error in issue req"),my_pe,num_pes),
            }
            while !msg_rx.is_empty() {
                //&& id < 3 {
                select! {
                    recv(msg_rx) -> msg => SharedChannels::process_msg(msg.expect("error in issue req"),my_pe,num_pes),
                    default => ()
                }
            }
        }
    }
}

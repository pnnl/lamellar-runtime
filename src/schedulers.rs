use crate::lamellae::Lamellae;
use crate::lamellar_request::InternalReq;
use crate::runtime::LAMELLAR_RT;
use crate::runtime::*;

// mod private_channels;
mod shared_channels;
mod work_stealing_sched;

// #[cfg(feature = "PrivateChannelSched")]
// pub(crate) use private_channels::PrivateChannels;
#[cfg(feature = "SharedChannelSched")]
pub(crate) use shared_channels::SharedChannels;
#[cfg(feature = "WorkStealingSched")]
pub(crate) use work_stealing_sched::WorkStealing;

pub(crate) trait Scheduler {
    fn new() -> Self;
    fn first(&self) {
        // println!("init");
    }
    fn submit_req(&self, pe: usize, msg: Msg, ireq: InternalReq, func: LamellarAny);
    fn submit_req_all(&self, msg: Msg, ireq: InternalReq, func: LamellarAny);
    fn submit_work(&self, msg: std::vec::Vec<u8>);

    //process user initiated requests (either execute it locally or serialize and send to remote node)
    fn process_msg(
        //maybe better to call this process request?
        (pe, msg, ireq, func): (usize, Msg, InternalReq, LamellarAny),
        my_pe: usize,
        num_pes: usize,
    ) {
        //process remote
        // msg.return_data = ireq.active.load(Ordering::SeqCst);
        // msg.return_data = false;
        if msg.return_data {
            REQUESTS[msg.id % REQUESTS.len()].insert_new(msg.id, ireq.clone());
        }
        // let t = std::time::Instant::now();
        if pe == my_pe || num_pes == 1 {
            match func.downcast::<LamellarLocal>() {
                Ok(func) => {
                    exec_local(msg, func, ireq);
                }
                Err(func) => {
                    let func = func
                        .downcast::<(LamellarLocal, LamellarClosure)>()
                        .expect("error in local downcast");
                    exec_local(msg, func.0, ireq);
                }
            }
        } else if pe == num_pes {
            // let t = std::time::Instant::now();
            if let Ok(funcs) = func.downcast::<(LamellarLocal, LamellarClosure)>() {
                let data = funcs.1();
                let payload = (msg.clone(), data);
                let data = bincode::serialize(&payload).unwrap();
                LAMELLAR_RT.lamellae.send_to_all(data);
                exec_local(msg, funcs.0, ireq);
            } else {
                println!("error in all downcast");
            }
        } else {
            // println!("PE: {:?} {:?} {:?}",pe,my_pe,num_pes);
            if let Ok(func) = func.downcast::<LamellarClosure>() {
                let data = func();
                let payload = (msg, data);
                let data = bincode::serialize(&payload).unwrap();
                LAMELLAR_RT.lamellae.send_to_pe(pe, data);
            } else {
                println!("error in remote downcast");
            }
        }
    }
}

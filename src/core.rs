use crate::lamellae::Lamellae;
use crate::lamellar_memregion::*;
use crate::lamellar_request::*;
use crate::runtime::LAMELLAR_RT;
use crate::runtime::*;
use crate::schedulers::Scheduler;
use crate::utils::ser_closure;

use std::any;
use std::sync::atomic::Ordering;
use std::time::Instant;


/// Global Synchronization 
///
pub fn barrier() {
    LAMELLAR_RT.lamellae.barrier();
}

/// Wait for outstanding remote closures to complete
/// *Note* does not apply to remote memory region requests
pub fn wait_all() {
    LAMELLAR_RT.wait_all();
}

/// allocate a remote memory region from the symmetric heap
///
/// # Arguments
///
/// * `size` - number of elements of T to allocate a memory region for -- (not size in bytes)
///
pub fn alloc_mem_region<
    T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + Send + Sync + std::fmt::Debug + 'static,
>(
    size: usize,
) -> LamellarMemoryRegion<T> {
    LamellarMemoryRegion::new(size)
}

/// release a remote memory region from the symmetric heap
///
/// # Arguments
///
/// * `region` - the region to free
///
pub fn free_memory_region<
    T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + Send + Sync + std::fmt::Debug + 'static,
>(
    region: LamellarMemoryRegion<T>,
) {
    drop(region)
}


/// Special return type used by runtime to detect an automatically executed closure upon return of a request
///
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct ClosureRet {
    data: std::vec::Vec<u8>,
}

/// use as the return value of a remote reqeust to execute the provided closure upon return of the request
///
/// # Arguments
///
/// * `func` - a serializable closure to execute automatically when the parent request returns
///
pub fn exec_on_return<
    F: FnOnce() -> T + serde::ser::Serialize + serde::de::DeserializeOwned + 'static,
    T: any::Any + serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone,
>(
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

/// a remote closure returning a handle to capture any resulting data (used internally)
///
/// # Arguments
///
/// * `func` - a serializable closure to execute automatically when the parent request returns
///
fn lamellar_closure<
    F: std::clone::Clone
        + Send
        + Sync
        + FnOnce() -> T
        + Send
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone
        + 'static,
    T: any::Any
        + Send
        + Sync
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone,
>(
    func: F,
) -> LamellarClosure {
    Box::new(move || {
        let arg: Vec<u8> = bincode::serialize(&func).unwrap();
        let start: serde_closure::FnOnce<(Vec<u8>,), fn((Vec<u8>,), ()) -> _> = FnOnce!([arg]move||{
            let arg: Vec<u8> = arg;
            let closure: F = bincode::deserialize(&arg).unwrap();
            let ret: T=closure();
            let box_any_ret: Box<dyn std::any::Any> = Box::new(ret.clone());
            if let Some(_x) = box_any_ret.downcast_ref::<()>(){
                // println!("unit return");
                (RetType::Unit,None)
            }
            else if let Some(x) = box_any_ret.downcast_ref::<ClosureRet>(){
                // println!("returning a func");
                (RetType::Closure,Some(x.data.clone()))
            }
            else{
                // println!("returning some other type");
                (RetType::Data,Some(bincode::serialize(&ret).unwrap()))
            }
        });
        bincode::serialize(&start).unwrap()
    })
}

/// a remote closure that doesn't capture any return data (thus no need for a user facing handle)
/// slightly faster than creating and maintaining the user handle. (used internally)
///
/// # Arguments
///
/// * `func` - a serializable closure to execute automatically when the parent request returns
///
fn lamellar_closure_nohandle<
    F: std::clone::Clone
        + Send
        + Sync
        + FnOnce() -> T
        + Send
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone
        + 'static,
    T: any::Any
        + Send
        + Sync
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone,
>(
    func: F,
) -> LamellarClosure {
    Box::new(move || {
        let arg: Vec<u8> = bincode::serialize(&func).unwrap();
        let start: serde_closure::FnOnce<(Vec<u8>,), fn((Vec<u8>,), ()) -> _> = FnOnce!([arg]move||{
            let arg: Vec<u8> = arg;
            let closure: F = bincode::deserialize(&arg).unwrap();
            closure();
            (RetType::NoHandle,None::<Vec<u8>>)
        });
        bincode::serialize(&start).unwrap()
    })
}

//a "remote" closure where destination is the local pe (no need to serialize) (used intenally)
///
/// # Arguments
///
/// * `func` - a serializable closure to execute automatically when the parent request returns
///
fn lamellar_local<
    F: std::clone::Clone
        + Send
        + Sync
        + FnOnce() -> T
        + Send
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone
        + 'static,
    T: any::Any
        + Send
        + Sync
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone,
>(
    func: F,
) -> LamellarLocal {
    Box::new(move || {
        let ret: T = func();
        let box_any_ret: Box<dyn std::any::Any> = Box::new(ret.clone());
        if let Some(_x) = box_any_ret.downcast_ref::<()>() {
            // println!("unit return");
            (RetType::Unit, None)
        } else if let Some(x) = box_any_ret.downcast_ref::<ClosureRet>() {
            // println!("returning a func");
            (RetType::Closure, Some(x.data.clone()))
        } else {
            // println!("returning some other type");
            (RetType::Data, Some(bincode::serialize(&ret).unwrap()))
        }
    })
}

/// a local closure that doesn't capture any return data (thus no need for a user facing handle)
/// slightly faster than creating and maintaining the user handle. (used internally)
///
/// # Arguments
///
/// * `func` - a serializable closure to execute automatically when the parent request returns
///
fn lamellar_local_nohandle<
    F: std::clone::Clone
        + Send
        + Sync
        + FnOnce() -> T
        + Send
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone
        + 'static,
    T: any::Any
        + Send
        + Sync
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone,
>(
    func: F,
) -> LamellarLocal {
    Box::new(move || {
        func();
        (RetType::NoHandle, None)
    })
}

/// user callable -- execute provided closure on specified pe without returning any data.
///
/// # Arguments
///
/// * `pe` - the pe to execute on
/// * `func` - a serializable closure to execute automatically when the parent request returns
///
pub fn exec_on_pe_nohandle<
    F: std::clone::Clone
        + Send
        + Sync
        + FnOnce() -> T
        + Send
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone
        + 'static,
    T: any::Any
        + Send
        + Sync
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone,
>(
    pe: usize,
    func: F,
) {
    let ireq = InternalReq {
        data_tx: LAMELLAR_RT.nohandle_ireq.data_tx.clone(),
        cnt: LAMELLAR_RT.nohandle_ireq.cnt.clone(),
        start: LAMELLAR_RT.nohandle_ireq.start.clone(),
        size: 0,
        active: LAMELLAR_RT.nohandle_ireq.active.clone(),
    };
    let msg = Msg {
        cmd: Cmd::ClosureReq,
        src: LAMELLAR_RT.arch.my_pe as u16,
        id: 0,
        return_data: false,
    };
    LAMELLAR_RT
        .counters
        .outstanding_reqs
        .fetch_add(1, Ordering::SeqCst);
    LAMELLAR_RT
        .counters
        .send_req_cnt
        .fetch_add(1, Ordering::SeqCst);
    let my_any: LamellarAny = if LAMELLAR_RT.arch.my_pe == pe {
        Box::new(lamellar_local_nohandle(func))
    } else {
        Box::new(lamellar_closure_nohandle(func))
    };

    SCHEDULER.submit_req(pe, msg, ireq, my_any);
}

/// user callable -- execute provided closure on specified pe 
///
/// # Arguments
///
/// * `pe` - the pe to execute on
/// * `func` - a serializable closure to execute automatically when the parent request returns
///
pub fn exec_on_pe<
    F: std::clone::Clone
        + Send
        + Sync
        + FnOnce() -> T
        + Send
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone
        + 'static,
    T: any::Any
        + Send
        + Sync
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone,
>(
    pe: usize,
    func: F,
) -> LamellarRequest<T> {
    let (my_req, mut ireq) = LamellarRequest::new(1);
    let msg = Msg {
        cmd: Cmd::ClosureReq,
        src: LAMELLAR_RT.arch.my_pe as u16,
        id: my_req.id,
        return_data: true,
    };
    LAMELLAR_RT
        .counters
        .outstanding_reqs
        .fetch_add(1, Ordering::SeqCst);
    LAMELLAR_RT
        .counters
        .send_req_cnt
        .fetch_add(1, Ordering::SeqCst);
    // ireq.size = msg.data.len();
    ireq.start = Instant::now();
    // println!("{:?} {:?} {:?}",msg.src,msg.src as usize ,pe);
    let my_any: LamellarAny = if LAMELLAR_RT.arch.my_pe == pe {
        Box::new(lamellar_local(func))
    } else {
        Box::new(lamellar_closure(func))
    };
    SCHEDULER.submit_req(pe, msg, ireq, my_any);
    my_req
}

/// user callable -- execute provided closure on all pes
///
/// # Arguments
///
/// * `func` - a serializable closure to execute automatically when the parent request returns
///
pub fn exec_all<
    F: std::clone::Clone
        + Send
        + Sync
        + FnOnce() -> T
        + Send
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone
        + 'static,
    T: any::Any
        + Send
        + Sync
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone,
>(
    func: F,
) -> LamellarRequest<T> {
    let (my_req, ireq) = LamellarRequest::new(LAMELLAR_RT.arch.num_pes);
    let msg = Msg {
        cmd: Cmd::ClosureReq,
        src: LAMELLAR_RT.arch.my_pe as u16,
        id: my_req.id,
        return_data: true,
    };
    LAMELLAR_RT
        .counters
        .outstanding_reqs
        .fetch_add(LAMELLAR_RT.arch.num_pes, Ordering::SeqCst);
    LAMELLAR_RT
        .counters
        .send_req_cnt
        .fetch_add(LAMELLAR_RT.arch.num_pes, Ordering::SeqCst);
    let my_any: LamellarAny = Box::new((lamellar_local(func.clone()), lamellar_closure(func)));
    SCHEDULER.submit_req_all(msg, ireq, my_any);
    my_req
}


pub fn init() -> (usize, usize) {
    let arch = LAMELLAR_RT.get_arch();
    SCHEDULER.first();
    arch
}

/// get id of current PE 
///
pub fn local_pe() -> usize {
    LAMELLAR_RT.arch.my_pe
}

/// return amount of data transfered
#[allow(non_snake_case)]
pub fn MB_sent() -> Vec<f64> {
    vec![
        LAMELLAR_RT.lamellae.MB_sent(),
        LAMELLAR_RT.lamellae2.MB_sent(),
    ]
}


pub fn finit() {
    LAMELLAR_RT.finit();
}


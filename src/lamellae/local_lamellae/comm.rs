use crate::config;

use crate::lamellar_alloc::{BTreeAlloc, LamellarAlloc};

use futures_util::Future;
use parking_lot::{Mutex, RwLock};

use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::sync::Arc;

struct MyPtr {
    ptr: *mut u8,
    layout: std::alloc::Layout,
}
unsafe impl Send for MyPtr {}

#[derive(Debug)]
pub(crate) struct LocalComm {
    num_pes: usize,
    my_pe: usize,
    allocs: Arc<Mutex<HashMap<usize, MyPtr>>>,
}

impl LocalComm {
    pub(crate) fn new() -> LocalComm {
        LocalComm {
            num_pes: 1,
            my_pe: 0,
            allocs: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl CommShutdown for LocalComm {
    //TODO perform cleanups of the shared memory if possible
    fn force_shutdown(&self) {}
}

impl CommProgress for LocalComm {
    fn flush(&self) {}
    fn wait(&self) {}
    fn barrier(&self) {}
}

impl CommInfo for LocalComm {
    fn my_pe(&self) -> usize {
        self.my_pe
    }
    fn num_pes(&self) -> usize {
        self.num_pes
    }
    fn backend(&self) -> Backend {
        Backend::Local
    }
}

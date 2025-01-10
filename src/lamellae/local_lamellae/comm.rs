use crate::{
    lamellae::comm::{CommInfo, CommProgress, CommShutdown},
    lamellar_alloc::LamellarAlloc,
    Backend,
};

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct MyPtr {
    pub(crate) ptr: *mut u8,
    pub(crate) layout: std::alloc::Layout,
}
unsafe impl Send for MyPtr {}

#[derive(Debug)]
pub(crate) struct LocalComm {
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    pub(crate) allocs: Arc<Mutex<HashMap<usize, MyPtr>>>,
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

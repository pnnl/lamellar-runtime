use crate::{
    lamellae::{
        comm::{CommInfo, CommProgress, CommShutdown},
        AllocError, AllocResult, CommAlloc, CommAllocInner, CommAllocType,
    },
    Backend,
};

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug)]
pub(crate) struct LocalAlloc {
    pub(crate) ptr: *mut u8,
    pub(crate) layout: std::alloc::Layout,
}
unsafe impl Send for LocalAlloc {}
unsafe impl Sync for LocalAlloc {}

impl LocalAlloc {
    pub(crate) fn start(&self) -> usize {
        self.ptr as usize
    }

    pub(crate) fn as_mut_ptr<T>(&self) -> *mut T {
        self.ptr as *mut T
    }
    pub(crate) fn as_mut_slice<T>(&self) -> &mut [T] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.as_mut_ptr::<T>(),
                self.num_bytes() / std::mem::size_of::<T>(),
            )
        }
    }

    pub(crate) fn num_bytes(&self) -> usize {
        self.layout.size()
    }
    pub(crate) fn sub_alloc(&self, offset: usize, len: usize) -> AllocResult<Arc<LocalAlloc>> {
        if offset + len > self.layout.size() {
            return Err(AllocError::InvalidSubAlloc(offset, len));
        }
        let new_data = unsafe { self.ptr.add(offset) };
        Ok(Arc::new(LocalAlloc {
            ptr: new_data,
            layout: std::alloc::Layout::from_size_align(len, self.layout.align()).unwrap(),
        }))
    }

    pub(crate) fn wait(&self) {
        // Local memory is always ready
    }
}

impl From<Arc<LocalAlloc>> for CommAlloc {
    fn from(alloc: Arc<LocalAlloc>) -> Self {
        CommAlloc {
            inner_alloc: CommAllocInner::LocalAlloc(alloc),
            alloc_type: CommAllocType::Fabric,
        }
    }
}

#[derive(Debug)]
pub(crate) struct LocalComm {
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    pub(crate) allocs: Arc<Mutex<HashMap<usize, Arc<LocalAlloc>>>>,
    pub(crate) heap_allocs: Arc<Mutex<HashMap<usize, Arc<LocalAlloc>>>>,
    pub(crate) put_amt: Arc<AtomicUsize>,
    pub(crate) get_amt: Arc<AtomicUsize>,
}

impl LocalComm {
    pub(crate) fn new() -> LocalComm {
        LocalComm {
            num_pes: 1,
            my_pe: 0,
            allocs: Arc::new(Mutex::new(HashMap::new())),
            heap_allocs: Arc::new(Mutex::new(HashMap::new())),
            put_amt: Arc::new(AtomicUsize::new(0)),
            get_amt: Arc::new(AtomicUsize::new(0)),
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
    fn atomic_avail<T: 'static>(&self) -> bool {
        false
    }
    fn MB_sent(&self) -> f64 {
        (self.put_amt.load(Ordering::SeqCst) + self.get_amt.load(Ordering::SeqCst)) as f64
            / 1_000_000.0
    }
}

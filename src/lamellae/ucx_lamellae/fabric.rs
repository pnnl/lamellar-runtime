mod context;
mod endpoint;
mod error;
mod memory_region;
mod worker;

use context::Context;
use endpoint::Endpoint;
pub(crate) use endpoint::UcxRequest;
use memory_region::{MemoryHandle, MemoryHandleInner, RKey};
use worker::Worker;

use crate::lamellae::{
    AllocResult, AllocationType, AtomicOp, CommAlloc, CommAllocAddr, CommAllocInner, CommAllocType,
    CommSlice, FabricError,
};

use pmi::{pmi::Pmi, pmix::PmiX};

use std::sync::{Arc, Mutex};
use tracing::trace;

pub(crate) struct UcxWorld {
    pmi: Arc<PmiX>,
    pub(crate) my_pe: usize,
    pub(crate) num_pes: usize,
    context: Arc<Context>,
    worker: Arc<Worker>,
    endpoints: Vec<Arc<Endpoint>>,
    mem_handles: Arc<Mutex<Vec<Arc<UcxAlloc>>>>,
    remote_keys: Arc<Mutex<Vec<(Arc<UcxAlloc>, Vec<(usize, Arc<RKey>)>)>>>,
    exchange_buffer: Option<Arc<UcxAlloc>>,
}

/* need to implement...
X - alloc(size,alloc_type) need to handle sub_alloc eventually
X - free_addr(addr)
X - free_alloc(alloc_info)
X - local_addr(remote_pe,remote_addr)
X - remote_addr(pe, local_addr)
X - get_alloc_from_start_addr(addr)
X - progress()
X - wait_all()
X - barrier()
X - clear_allocs()
X - atomic_avail()
atomic_op()
atomic_fetch_op()
put()
inner_put()
get()
inner_get()
 */

impl std::fmt::Debug for UcxWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("UcxWorld")
            .field("my_pe", &self.my_pe)
            .field("num_pes", &self.num_pes)
            .finish()
    }
}

impl UcxWorld {
    pub(crate) fn new() -> Self {
        let my_pmi = Arc::new(
            PmiX::new()
                .map_err(|e| {
                    eprintln!("Error initializing PMI: {:?}", e);
                    FabricError::InitError(1)
                })
                .unwrap(),
        );
        let context = Context::new(my_pmi.clone()).unwrap();
        let worker = context.create_worker().unwrap();

        let addresses = worker.exchange_address(&my_pmi).unwrap();

        let endpoints = addresses
            .iter()
            .map(|a| Endpoint::new(worker.clone(), a).unwrap())
            .collect::<Vec<_>>();

        let my_pe = my_pmi.rank();
        let num_pes = my_pmi.ranks().len();
        let exchange_buffer =
            Self::initial_alloc(&context, &endpoints, &worker, &my_pmi, num_pes, my_pe).unwrap();
        UcxWorld {
            pmi: my_pmi,
            my_pe,
            num_pes,
            context,
            worker,
            endpoints,
            mem_handles: Arc::new(Mutex::new(Vec::new())),
            remote_keys: Arc::new(Mutex::new(Vec::new())),
            exchange_buffer: Some(exchange_buffer),
        }
    }

    pub(crate) fn my_pe(&self) -> usize {
        self.my_pe
    }

    pub(crate) fn num_pes(&self) -> usize {
        self.num_pes
    }

    pub(crate) fn atomic_avail<T: 'static>(&self) -> bool {
        let id = std::any::TypeId::of::<T>();

        if id == std::any::TypeId::of::<u8>() {
            false
        } else if id == std::any::TypeId::of::<u16>() {
            false
        } else if id == std::any::TypeId::of::<u32>() {
            true
        } else if id == std::any::TypeId::of::<u64>() {
            true
        } else if id == std::any::TypeId::of::<i8>() {
            false
        } else if id == std::any::TypeId::of::<i16>() {
            false
        } else if id == std::any::TypeId::of::<i32>() {
            true
        } else if id == std::any::TypeId::of::<i64>() {
            true
        } else if id == std::any::TypeId::of::<usize>() {
            true
        } else if id == std::any::TypeId::of::<isize>() {
            true
        } else {
            false
        }
    }

    fn initial_alloc(
        context: &Arc<Context>,
        endpoints: &Vec<Arc<Endpoint>>,
        worker: &Arc<Worker>,
        pmi: &Arc<PmiX>,
        num_pes: usize,
        my_pe: usize,
    ) -> AllocResult<Arc<UcxAlloc>> {
        let mem_handle = MemoryHandleInner::alloc(context, 1024); //dummy allocation to get the size of the exchange buffer
        let mut size = mem_handle.addr.to_ne_bytes().len();
        size += mem_handle.pack().as_ref().len();
        size = size * num_pes;
        drop(mem_handle);
        // println!("Initial alloc size: {}", size);
        let mem_handle = MemoryHandleInner::alloc(context, size * num_pes);
        let buffer_keys = mem_handle.exchange_key_pmi(endpoints, pmi).unwrap();

        let mem = MemoryHandle {
            addr: mem_handle.addr,
            size: size,
            inner: mem_handle.clone(),
        };

        let alloc = Arc::new(UcxAlloc {
            mem,
            total_size: size * num_pes,
            local_size: size,
            my_pe: my_pe,
            num_pes: num_pes,
            context: context.clone(),
            worker: worker.clone(),
            endpoints: endpoints.clone(),
            remote_keys: buffer_keys.clone(),
        });
        Ok(alloc)
    }

    pub(crate) fn alloc(&self, size: usize, _alloc_type: AllocationType) -> Arc<UcxAlloc> {
        let mem_handle = MemoryHandleInner::alloc(&self.context, size);
        let buffer_keys = mem_handle
            .exchange_key_alloc(
                &self.endpoints,
                &self.pmi,
                &self.exchange_buffer.as_ref().unwrap(),
            )
            .unwrap();

        let mem = MemoryHandle {
            addr: mem_handle.addr,
            size: size,
            inner: mem_handle.clone(),
        };

        // for (i, (addr, key)) in buffer_keys.iter().enumerate() {
        //     println!("Buffer key for endpoint {}: {:x} {:?}", i, addr, key);
        // }
        let alloc = Arc::new(UcxAlloc {
            mem,
            total_size: size * self.num_pes,
            local_size: size,
            my_pe: self.my_pe,
            num_pes: self.num_pes,
            context: self.context.clone(),
            worker: self.worker.clone(),
            endpoints: self.endpoints.clone(),
            remote_keys: buffer_keys.clone(),
        });
        self.mem_handles.lock().unwrap().push(alloc.clone());
        self.remote_keys
            .lock()
            .unwrap()
            .push((alloc.clone(), buffer_keys));
        alloc
    }

    pub(crate) fn free_alloc(&self, alloc: &UcxAlloc) {
        self.mem_handles
            .lock()
            .unwrap()
            .retain(|a| a.mem.inner.addr != alloc.mem.inner.addr);
        self.remote_keys
            .lock()
            .unwrap()
            .retain(|(a, _)| a.mem.inner.addr != alloc.mem.inner.addr);
    }

    pub(crate) fn free_addr(&self, addr: usize) {
        if let Some(alloc) = self
            .mem_handles
            .lock()
            .unwrap()
            .iter()
            .find(|a| a.mem.inner.addr == addr)
            .clone()
        {
            self.free_alloc(alloc);
        }
    }

    pub(crate) fn wait_all(&self) {
        self.worker.wait_all();
    }

    pub(crate) fn progress(&self) {
        self.worker.progress();
    }

    pub(crate) fn flush(&self) {
        self.worker.progress();
    }

    pub(crate) fn barrier(&self) {
        self.pmi.barrier(false).expect(" Failed to perform barrier");
    }

    pub(crate) fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> Option<usize> {
        let allocs = self.remote_keys.lock().unwrap();
        for (alloc, remote_addrs) in allocs.iter() {
            let remote_pe_addr = remote_addrs[remote_pe].0;
            if remote_pe_addr <= remote_addr && remote_addr < remote_pe_addr + alloc.local_size {
                let offset = remote_addr - remote_pe_addr;
                return Some(alloc.mem.inner.addr + offset);
            }
        }
        None
    }

    pub(crate) fn local_alloc_and_offset_from_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
    ) -> Option<(CommAlloc, usize)> {
        let allocs = self.remote_keys.lock().unwrap();
        for (alloc, remote_addrs) in allocs.iter() {
            let remote_pe_addr = remote_addrs[remote_pe].0;
            if remote_pe_addr <= remote_addr && remote_addr < remote_pe_addr + alloc.local_size {
                let offset = remote_addr - remote_pe_addr;
                return Some((alloc.clone().into(), offset));
            }
        }
        None
    }

    pub(crate) fn remote_addr(&self, pe: usize, local_addr: usize) -> Option<usize> {
        let allocs = self.remote_keys.lock().unwrap();
        for (alloc, remote_addrs) in allocs.iter() {
            if alloc.mem.inner.addr <= local_addr
                && local_addr < alloc.mem.inner.addr + alloc.local_size
            {
                let offset = local_addr - alloc.mem.inner.addr;
                let remote_pe_addr = remote_addrs[pe].0;
                return Some(remote_pe_addr + offset);
            }
        }
        None
    }

    pub(crate) fn get_alloc_from_start_addr(
        &self,
        addr: CommAllocAddr,
    ) -> Result<Arc<UcxAlloc>, String> {
        let allocs = self.mem_handles.lock().unwrap();
        for alloc in allocs.iter() {
            if alloc.mem.inner.addr == *addr {
                return Ok(alloc.clone());
            }
        }
        Err(format!("No allocation found for address {:x}", addr))
    }

    pub(crate) fn clear_allocs(&self) {
        self.mem_handles.lock().unwrap().clear();
        self.remote_keys.lock().unwrap().clear();
    }

    // pub(crate) unsafe fn put<T>(
    //     &self,
    //     pe: usize,
    //     src_addr: &CommSlice<T>,
    //     dst_addr: &CommAllocAddr,
    //     managed: bool,
    // ) -> Option<UcxRequest> {
    //     self.inner_put(pe, src_addr.as_ref(), *(dst_addr as &usize), managed)
    // }
    // pub(crate) unsafe fn inner_put<T>(
    //     &self,
    //     pe: usize,
    //     src_addr: &[T],
    //     dst_addr: usize,
    //     managed: bool,
    // ) -> Option<UcxRequest> {
    //     // println!("ucx put: remote_pe: {pe} dst_addr: {dst_addr:x}");
    //     let allocs = self.remote_keys.lock().unwrap();
    //     for (alloc, remote_addrs) in allocs.iter() {
    //         if alloc.contains(dst_addr) {
    //             let (remote_addr, rkey) = &remote_addrs[pe];
    //             let offset = dst_addr - alloc.start();
    //             let remote_dst_addr = remote_addr + offset;
    //             // println!("found remote_dst_addr: {remote_dst_addr:x} = remote_addr {remote_addr:x} + offset {offset:x}");
    //             return self.endpoints[pe].put(
    //                 src_addr.as_ptr() as _,
    //                 src_addr.len() * std::mem::size_of::<T>(),
    //                 remote_dst_addr,
    //                 &rkey,
    //                 managed,
    //             );
    //         }
    //     }
    //     panic!("Failed to find remote address");
    // }

    // pub(crate) unsafe fn get<T: Copy>(
    //     &self,
    //     pe: usize,
    //     src_addr: &CommAllocAddr,
    //     dst_addr: &mut CommSlice<T>,
    //     sync: bool,
    // ) -> UcxRequest {
    //     self.inner_get(pe, *(src_addr as &usize), dst_addr, sync)
    // }

    // pub(crate) unsafe fn inner_get<T: Copy>(
    //     &self,
    //     pe: usize,
    //     src_addr: usize,
    //     dst_addr: &mut [T],
    //     sync: bool,
    // ) -> UcxRequest {
    //     let allocs = self.remote_keys.lock().unwrap();
    //     for (alloc, remote_addrs) in allocs.iter() {
    //         if alloc.contains(src_addr) {
    //             let (remote_addr, rkey) = &remote_addrs[pe];
    //             let remote_src_addr = remote_addr + (src_addr - alloc.start());
    //             return self.endpoints[pe].get(
    //                 dst_addr.as_mut_ptr() as _,
    //                 dst_addr.len() * std::mem::size_of::<T>(),
    //                 remote_src_addr,
    //                 &rkey,
    //             );
    //         }
    //     }
    //     panic!("Failed to find remote address");
    // }

    // pub fn atomic_op<T: Copy>(
    //     &self,
    //     pe: usize,
    //     op: &AtomicOp<T>,
    //     dst_addr: &CommAllocAddr,
    // ) -> Option<UcxRequest> {
    //     let dst_addr = *(dst_addr as &usize);
    //     match op {
    //         AtomicOp::Write(val) => {
    //             let allocs = self.remote_keys.lock().unwrap();
    //             for (alloc, remote_addrs) in allocs.iter() {
    //                 if alloc.contains(dst_addr) {
    //                     let (remote_addr, rkey) = &remote_addrs[pe];
    //                     let remote_dst_addr = remote_addr + (dst_addr - alloc.start());
    //                     return self.endpoints[pe].atomic_put(*val, remote_dst_addr, &rkey, true);
    //                 }
    //             }
    //         }
    //         _ => panic!("Unsupported atomic operation"),
    //     }
    //     panic!("Failed to find remote address");
    // }

    // pub fn atomic_fetch_op<T: Copy>(
    //     &self,
    //     pe: usize,
    //     op: &AtomicOp<T>,
    //     dst_addr: &CommAllocAddr,
    //     result: &mut [T],
    // ) -> UcxRequest {
    //     let dst_addr = *(dst_addr as &usize);
    //     match op {
    //         AtomicOp::Read => {
    //             let allocs = self.remote_keys.lock().unwrap();
    //             for (alloc, remote_addrs) in allocs.iter() {
    //                 if alloc.contains(dst_addr) {
    //                     let (remote_addr, rkey) = &remote_addrs[pe];
    //                     let remote_dst_addr = remote_addr + (dst_addr - alloc.start());
    //                     return self.endpoints[pe].atomic_get(
    //                         result.as_mut_ptr(),
    //                         remote_dst_addr,
    //                         &rkey,
    //                     );
    //                 }
    //             }
    //         }
    //         AtomicOp::Write(val) => {
    //             let allocs = self.remote_keys.lock().unwrap();
    //             for (alloc, remote_addrs) in allocs.iter() {
    //                 if alloc.contains(dst_addr) {
    //                     let (remote_addr, rkey) = &remote_addrs[pe];
    //                     let remote_dst_addr = remote_addr + (dst_addr - alloc.start());
    //                     return self.endpoints[pe].atomic_swap(
    //                         *val,
    //                         result.as_mut_ptr(),
    //                         remote_dst_addr,
    //                         &rkey,
    //                     );
    //                 }
    //             }
    //         }
    //         _ => panic!("Unsupported atomic operation"),
    //     }
    //     panic!("Failed to find remote address");
    // }
}

impl Drop for UcxWorld {
    fn drop(&mut self) {
        trace!("dropping ucx world");
        self.barrier();
        self.exchange_buffer.take();
        self.remote_keys.lock().unwrap().clear();
        self.mem_handles.lock().unwrap().clear();
        self.barrier();
    }
}

pub struct UcxAlloc {
    mem: MemoryHandle,
    total_size: usize,
    local_size: usize,
    pub(crate) my_pe: usize,
    pub(crate) num_pes: usize,
    context: Arc<Context>,
    worker: Arc<Worker>,
    endpoints: Vec<Arc<Endpoint>>,
    remote_keys: Vec<(usize, Arc<RKey>)>,
}

impl From<Arc<UcxAlloc>> for CommAlloc {
    fn from(alloc: Arc<UcxAlloc>) -> Self {
        CommAlloc {
            inner_alloc: CommAllocInner::UcxAlloc(alloc),
            alloc_type: CommAllocType::Fabric,
        }
    }
}
// impl Hash for UcxAlloc {
//     fn hash<H: Hasher>(&self, state: &mut H) {
//         self.mem.hash(state);
//     }
// }
// impl PartialEq for UcxAlloc {
//     fn eq(&self, other: &Self) -> bool {
//         self.mem == other.mem
//     }
// }
// impl Eq for UcxAlloc {}

impl std::fmt::Debug for UcxAlloc {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("UcxAlloc")
            .field("addr", &format_args!("{:x}", self.mem.addr))
            .field("total_size", &self.total_size)
            .field("local_size", &self.local_size)
            .field("my_pe", &self.my_pe)
            .field("num_pes", &self.num_pes)
            .finish()
    }
}

impl UcxAlloc {
    pub(crate) fn start(&self) -> usize {
        self.mem.addr.into()
    }
    pub(crate) fn num_bytes(&self) -> usize {
        self.local_size
    }
    pub(crate) fn sub_alloc(&self, offset: usize, size: usize) -> AllocResult<Arc<Self>> {
        let remote_keys = self
            .remote_keys
            .iter()
            .map(|(addr, rkey)| (addr + offset, rkey.clone()))
            .collect();
        Ok(Arc::new(UcxAlloc {
            mem: self.mem.sub_alloc(offset, size),
            total_size: size * self.num_pes,
            local_size: size,
            my_pe: self.my_pe,
            num_pes: self.num_pes,
            context: self.context.clone(),
            worker: self.worker.clone(),
            endpoints: self.endpoints.clone(),
            remote_keys,
        }))
    }

    pub(crate) unsafe fn put<T>(
        &self,
        pe: usize,
        offset: usize, //with respect to T
        src_addr: &CommSlice<T>,
        managed: bool,
    ) -> Option<UcxRequest> {
        self.put_inner(pe, offset, src_addr.as_ref(), managed)
    }
    pub(crate) unsafe fn put_inner<T>(
        &self,
        pe: usize,
        offset: usize, //with respect to T
        src_addr: &[T],
        managed: bool,
    ) -> Option<UcxRequest> {
        let offset = offset * std::mem::size_of::<T>();
        let (remote_addr, rkey) = &self.remote_keys[pe];
        trace!(
            "put to pe {} at remote addr {:x} + offset {:?}, final addr: {:x}",
            pe,
            remote_addr,
            offset,
            remote_addr + offset
        );
        self.endpoints[pe].put(
            src_addr.as_ptr() as _,
            src_addr.len() * std::mem::size_of::<T>(),
            remote_addr + offset,
            &rkey,
            managed,
        )
    }

    pub(crate) unsafe fn get<T: Copy>(
        &self,
        pe: usize,
        offset: usize,
        dst_addr: &mut CommSlice<T>,
    ) -> UcxRequest {
        self.inner_get(pe, offset, dst_addr)
    }

    pub(crate) unsafe fn inner_get<T: Copy>(
        &self,
        pe: usize,
        offset: usize,
        dst_addr: &mut [T],
    ) -> UcxRequest {
        let offset = offset * std::mem::size_of::<T>();
        let (remote_addr, rkey) = &self.remote_keys[pe];
        self.endpoints[pe].get(
            dst_addr.as_mut_ptr() as _,
            dst_addr.len() * std::mem::size_of::<T>(),
            remote_addr + offset,
            &rkey,
        )
    }

    pub(crate) fn atomic_op<T: Copy>(
        &self,
        pe: usize,
        offset: usize,
        op: &AtomicOp<T>,
        managed: bool,
    ) -> Option<UcxRequest> {
        match op {
            AtomicOp::Write(val) => {
                let (remote_addr, rkey) = &self.remote_keys[pe];
                self.endpoints[pe].atomic_put(*val, remote_addr + offset, &rkey, managed)
            }
            _ => panic!("Unsupported atomic operation"),
        }
    }

    pub(crate) fn atomic_fetch_op<T: Copy>(
        &self,
        pe: usize,
        offset: usize,
        op: &AtomicOp<T>,
        result: &mut [T],
    ) -> UcxRequest {
        match op {
            AtomicOp::Read => {
                let (remote_addr, rkey) = &self.remote_keys[pe];
                self.endpoints[pe].atomic_get(result.as_mut_ptr(), remote_addr + offset, &rkey)
            }
            AtomicOp::Write(val) => {
                let (remote_addr, rkey) = &self.remote_keys[pe];
                self.endpoints[pe].atomic_swap(
                    *val,
                    result.as_mut_ptr(),
                    remote_addr + offset,
                    &rkey,
                )
            }
            _ => panic!("Unsupported atomic operation"),
        }
    }

    pub(crate) fn as_mut_slice<T>(&self) -> &mut [T] {
        self.mem.as_mut_slice()
    }

    pub(crate) fn wait_all(&self) {
        self.worker.wait_all();
    }

    pub(crate) fn wait(&self) {
        self.worker.wait_all();
    }

    pub(crate) fn contains(&self, addr: usize) -> bool {
        self.mem.inner.addr <= addr && addr < self.mem.inner.addr + self.local_size
    }
}

impl Drop for UcxAlloc {
    fn drop(&mut self) {
        // println!("Dropping UcxArray");
        // self.mem_handles.lock().unwrap().remove(&self.mem.inner);
        // self.remote_keys.lock().unwrap().remove(&self.mem.inner);
    }
}

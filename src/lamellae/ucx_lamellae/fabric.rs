mod context;
mod endpoint;
mod error;
mod memory_region;
mod worker;

use context::Context;
use endpoint::Endpoint;
pub(crate) use endpoint::UcxRequest;
use endpoint::ATOMIC_PUT_TMP;
use memory_region::{MemoryHandle, MemoryHandleInner, RemoteAddressInfo};
use worker::Worker;
use crate::config;
use crate::lamellae::CollectiveOpKind;
use super::ucc::UccLib;

#[cfg(feature = "enable-on-node-shmem")]
use crate::config;
use crate::{
    lamellae::{
        comm::alloc::*, AllocError, AllocResult, AllocationType, AtomicOp, CommAlloc,
        collective::{AllReduceOp, RootOrSliceMut, RootSrcOrSliceMut, RootSrcSliceOrNone},  ucx_lamellae::ucc::{self, Error, UccContext, UccRequest, UccTeam}, CommAllocAddr, CommAllocInner, FabricError
    },
    lamellar_alloc::{BTreeAlloc, LamellarAlloc},
};

#[cfg(feature = "enable-on-node-shmem")]
use crate::lamellae::shmem_utils::{attach_shmem_segment, ShmemSegment};

use pmi::pmi::{Pmi, PmiBuilder};
#[cfg(feature = "enable-on-node-shmem")]
use std::ffi::c_void;

use lamellar_ucx_sys::ucp_atomic_op_t;

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tracing::{debug, trace};

pub(crate) struct UcxBarrier {
    counter: AtomicUsize,
    sub_counter: AtomicU32,
    num_pes: usize,
    my_pe: usize,
    buffer: UcxAlloc,
    sub_buffer: UcxAlloc,
    sub_last_seen: Mutex<Vec<u32>>,
}

impl UcxBarrier {
    fn new(num_pes: usize, my_pe: usize, buffer: UcxAlloc) -> Self {
        trace!(target: "ucx", "PE {} creating barrier with num_pes: {}", my_pe, num_pes);
        let sub_buffer_size = std::mem::size_of::<u32>() * num_pes * 2;

        let full_buffer_size = buffer.data_num_bytes - sub_buffer_size;
        trace!(target: "ucx", "PE {} barrier buffer size: {} sub_buffer_size: {} full_buffer_size: {}", my_pe, buffer.data_num_bytes, sub_buffer_size, full_buffer_size);
        let full_buffer = buffer
            .sub_alloc(0, full_buffer_size)
            .expect("Failed to create full buffer for barrier");
        let sub_buffer = buffer
            .sub_alloc(full_buffer_size, sub_buffer_size)
            .expect("Failed to create sub buffer for barrier");
        UcxBarrier {
            counter: AtomicUsize::new(1),
            sub_counter: AtomicU32::new(1),
            num_pes,
            my_pe,
            buffer: full_buffer,
            sub_buffer: sub_buffer,
            sub_last_seen: Mutex::new(vec![0u32; num_pes * 2]),
        }
    }

    fn sub_barrier(&self, pes: &[usize]) {
        let group_size = pes.len();
        let num_rounds = (group_size as f64).log2().ceil() as usize;
        let my_group_pe = pes.iter().position(|p| *p == self.my_pe).unwrap();
        let my_barrier = self.sub_counter.fetch_add(1, Ordering::SeqCst);
        let phase_offset = (my_barrier as usize & 1) * self.num_pes;
        let barrier_alloc = &self.sub_buffer;
        let barrier_vec = unsafe { barrier_alloc.as_mut_slice::<u32>() };
        let mut last_seen_guard = self.sub_last_seen.lock().unwrap();
        let last_seen_vec = last_seen_guard.as_mut_slice();
        trace!(target: "ucx", "PE {} entering sub barrier id: {my_barrier} with pes: {:?} ", self.my_pe, pes);

        //just do dissemination instead of 2-way dissemination, as its simpler to implement and sub_barrier is a place holder until we get UCC up and working
        for round in 0..num_rounds as usize {
            let send_pe = pes[(my_group_pe + (1 << round)) % group_size];
            let recv_pe = pes
                [(my_group_pe as i64 - (1 << round) as i64).rem_euclid(group_size as i64) as usize];
            trace!(target: "ucx", "PE {} sending sub barrier to PE {} in round {} with barrier value {} {:?} {:?}", self.my_pe, send_pe, round, my_barrier, barrier_vec, last_seen_vec);
            unsafe {
                barrier_alloc.put_inner(
                    send_pe,
                    phase_offset + self.my_pe,
                    std::slice::from_ref(&my_barrier),
                    false,
                    false,
                );
            };
            trace!(target: "ucx", "PE {} waiting for sub barrier from PE {} in round {} with barrier value {} {:?} {:?}", self.my_pe, recv_pe, round, my_barrier, barrier_vec, last_seen_vec);
            let recv_idx_0 = recv_pe;
            let recv_idx_1 = self.num_pes + recv_pe;
            loop {
                let cur_0 =
                    unsafe { std::ptr::read_volatile(barrier_vec.as_ptr().add(recv_idx_0)) };
                let cur_1 =
                    unsafe { std::ptr::read_volatile(barrier_vec.as_ptr().add(recv_idx_1)) };

                let ready_0 = cur_0 > last_seen_vec[recv_idx_0];
                let ready_1 = cur_1 > last_seen_vec[recv_idx_1];

                if ready_0 || ready_1 {
                    if ready_1 && (!ready_0 || cur_1 >= cur_0) {
                        last_seen_vec[recv_idx_1] = cur_1;
                    } else {
                        last_seen_vec[recv_idx_0] = cur_0;
                    }
                    break;
                }

                barrier_alloc.worker.progress();
                std::thread::yield_now();
            }
        }
        barrier_alloc.wait_all();
        trace!(target: "ucx", "PE {} exiting sub barrier id: {my_barrier}  with pes: {:?}", self.my_pe, pes);
    }

    //n-way dissemnation barrier
    fn barrier(&self) {
        trace!(target: "ucx", "PE {} entering barrier", self.my_pe);
        let num_pes = self.num_pes;
        if num_pes <= 1 {
            return;
        }
        let (n, num_rounds) = if num_pes == 2 {
            (1usize, 1usize)
        } else {
            (
                2usize,
                ((num_pes as f64).log2() / (2 as f64).log2()).ceil() as usize,
            )
        };
        let my_pe = self.my_pe;
        let my_barrier = self.counter.fetch_add(1, Ordering::SeqCst);

        let barrier_alloc = &self.buffer;

        for round in 0..num_rounds as usize {
            trace!(target: "ucx", "PE {} starting round {}/{} of barrier", self.my_pe, round, num_rounds);
            for i in 1..=n {
                let send_pe = (my_pe + i * (n + 1).pow(round as u32)) % num_pes;
                unsafe {
                    barrier_alloc.put_inner(
                        send_pe,
                        round * n + i - 1,
                        std::slice::from_ref(&my_barrier),
                        false,
                        false,
                    );
                };
            }

            for i in 1..=n {
                // let _recv_pe = (my_pe as i64
                //     - i as i64 * (n as i64 + 1).pow(round as u32))
                // .rem_euclid(num_pes as i64);
                let barrier_vec = unsafe { barrier_alloc.as_mut_slice::<usize>() };

                while my_barrier > barrier_vec[round * n + i - 1] {
                    barrier_alloc.worker.progress();
                    std::thread::yield_now();
                }
            }

            trace!(target: "ucx", "PE {} completed round {}/{} of barrier", self.my_pe, round, num_rounds);
        }
        barrier_alloc.wait_all();
        trace!(target: "ucx","PE {} exiting barrier", self.my_pe);
    }
}

pub(crate) struct UcxWorld {
    pmi: Arc<dyn Pmi>,
    pub(crate) my_pe: usize,
    pub(crate) num_pes: usize,
    #[cfg(feature = "enable-on-node-shmem")]
    same_node_pes: Vec<bool>,
    #[cfg(feature = "enable-on-node-shmem")]
    disable_on_node_shmem: bool,
    #[cfg(feature = "enable-on-node-shmem")]
    job_id: usize,
    context: Arc<Context>,
    worker: Arc<Worker>,
    endpoints: Vec<Arc<Endpoint>>,
    mem_handles: Arc<Mutex<Vec<UcxAlloc>>>,
    remote_keys: Arc<Mutex<Vec<(UcxAlloc, HashMap<usize, RemoteAddressInfo>)>>>,
    ucc_context: Option<Arc<UccContext>>,
    ucc_world_team: Option<Arc<UccTeam>>,
    exchange_buffer: Option<UcxAlloc>,
    barrier: Option<Arc<Mutex<UcxBarrier>>>,
    ucc_world_buffer: Option<Arc<UcxAlloc>>,
}

#[cfg(feature = "enable-on-node-shmem")]
static UCX_SHMEM_ALLOC_ID: AtomicUsize = AtomicUsize::new(0);

impl std::fmt::Debug for UcxWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("UcxWorld")
            .field("my_pe", &self.my_pe)
            .field("num_pes", &self.num_pes)
            .finish()
    }
}

#[lamellar_prof::prof]
impl UcxWorld {
    pub(crate) fn new() -> Self {
        let my_pmi = Arc::new(
            PmiBuilder::init()
                .map_err(|e| {
                    eprintln!("Error initializing PMI: {:?}", e);
                    FabricError::InitError(1)
                })
                .unwrap(),
        );
        let context = Context::new(my_pmi.clone()).unwrap();
        let worker = context.create_worker().unwrap();

        let addresses = worker.exchange_address(my_pmi.clone()).unwrap();

        let endpoints = addresses
            .iter()
            .map(|a| Endpoint::new(worker.clone(), a).unwrap())
            .collect::<Vec<_>>();
        
        let my_pe = my_pmi.rank();
        let num_pes = my_pmi.ranks().len();
        #[cfg(feature = "enable-on-node-shmem")]
        let disable_on_node_shmem = config().disable_on_node_shmem.unwrap_or(false);
        #[cfg(feature = "enable-on-node-shmem")]
        let mut same_node_pes = vec![false; num_pes];
        #[cfg(feature = "enable-on-node-shmem")]
        if !disable_on_node_shmem {
            let pes_on_node = my_pmi.ranks_on_node(my_pmi.node());
            if !pes_on_node.is_empty() {
                for pe in pes_on_node {
                    if pe < num_pes {
                        same_node_pes[pe] = true;
                    }
                }
            }
        }
        #[cfg(feature = "enable-on-node-shmem")]
        let job_id = my_pmi.job_id();
        let mem_handles = Arc::new(Mutex::new(Vec::new()));
        let remote_keys = Arc::new(Mutex::new(Vec::new()));
        let exchange_buffer = Self::initial_alloc(
            true,
            &context,
            &endpoints,
            &worker,
            my_pmi.clone(),
            num_pes,
            my_pe,
            #[cfg(feature = "enable-on-node-shmem")]
            &same_node_pes,
            #[cfg(feature = "enable-on-node-shmem")]
            disable_on_node_shmem,
            #[cfg(feature = "enable-on-node-shmem")]
            job_id,
            mem_handles.clone(),
            remote_keys.clone(),
        )
        .unwrap();

        Self::warmup_peer_puts( &worker, &exchange_buffer, my_pe, num_pes);


        let barrier_buffer = Self::initial_alloc(
            false,
            &context,
            &endpoints,
            &worker,
            my_pmi.clone(),
            num_pes,
            my_pe,
            #[cfg(feature = "enable-on-node-shmem")]
            &same_node_pes,
            #[cfg(feature = "enable-on-node-shmem")]
            disable_on_node_shmem,
            #[cfg(feature = "enable-on-node-shmem")]
            job_id,
            mem_handles.clone(),
            remote_keys.clone(),
        )
        .unwrap();

        // Self::warmup_peer_puts( &worker, &barrier_buffer, my_pe, num_pes);
    
        let mut world = UcxWorld {
            pmi: my_pmi.clone(),
            my_pe,
            num_pes,
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_pes,
            #[cfg(feature = "enable-on-node-shmem")]
            disable_on_node_shmem,
            #[cfg(feature = "enable-on-node-shmem")]
            job_id,
            context,
            worker,
            endpoints,
            mem_handles,
            remote_keys,
            ucc_context: None,
            ucc_world_team: None,
            exchange_buffer: Some(exchange_buffer),
            barrier: Some(Arc::new(Mutex::new(UcxBarrier::new(
                num_pes,
                my_pe,
                barrier_buffer,
            )))),
            ucc_world_buffer: None,
        };
        my_pmi.barrier(false).expect("Failed to perform barrier after initial allocations");
    
        let alloc = Arc::new(world.alloc(config().ucc_oob_init_buffer_size * world.num_pes, 8, AllocationType::Global));
        world.ucc_world_buffer = Some(alloc.clone());

        unsafe { alloc.as_mut_slice().iter_mut().for_each(|x| *x = u8::MAX) };
        world.barrier();
        // my_pmi.barrier(false);
        let ucc_lib = Arc::new(UccLib::new());
        let ucc_context = Arc::new(UccContext::new(ucc_lib.clone(), alloc.clone()).unwrap());
        world.barrier();
        // my_pmi.barrier(false);
        unsafe { alloc.as_mut_slice().iter_mut().for_each(|x| *x = u8::MAX) };
        world.barrier();
        // my_pmi.barrier(false);
        let ucc_world_team = UccTeam::new(my_pe, &(0..num_pes).collect::<Vec<_>>(), ucc_context.clone(), alloc.clone()).unwrap();

        world.ucc_context = Some(ucc_context);
        world.ucc_world_team = Some(Arc::new(ucc_world_team));
        world
    }

    // Found this was necessary in the offchance that the first call to a intranode PE
    // happened simultaneously (in a MT environment) with other operations like progress or flush
    // if this becomes a bottleneck it may be sufficient to just do a put to each node instead of each PE, but for now we will do it to each PE to be safe
    fn warmup_peer_puts(
        worker: &Arc<Worker>,
        buffer: &UcxAlloc,
        my_pe: usize,
        num_pes: usize,
    ) {
        for pe in 0..num_pes {
            if pe == my_pe {
                continue;
            }
            unsafe {
                buffer.put_inner(pe, 0, std::slice::from_ref(&my_pe), false, false);
            }
        }

        worker
            .wait_all()
            .expect("Failed final worker flush after UCX warm-up puts");
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

    pub(crate) fn atomic_op_avail<T: 'static>(&self, op: &AtomicOp<T>) -> bool {
        if !self.atomic_avail::<T>() {
            return false;
        }
        matches!(
            op,
            AtomicOp::Read(_)
                | AtomicOp::Write(_)
                | AtomicOp::Cas
                | AtomicOp::Sum(_)
                | AtomicOp::Sub(_)
                | AtomicOp::BitOr(_)
                | AtomicOp::BitXor(_)
                | AtomicOp::BitAnd(_)
                | AtomicOp::FetchSum(_)
                | AtomicOp::FetchSub(_)
                | AtomicOp::FetchBitOr(_)
                | AtomicOp::FetchBitXor(_)
                | AtomicOp::FetchBitAnd(_)
        )
    }

    pub(crate) fn collective_avail<T: 'static>(&self, op: CollectiveOpKind) -> bool {
        if self.ucc_context.is_none() {
            print!("UCC context not initialized, collective operations are not available");
            return false;
        }
        if std::any::TypeId::of::<T>() == std::any::TypeId::of::<()>() {
            assert!(matches!(op, CollectiveOpKind::Barrier));
            return true;
        }
        let id = std::any::TypeId::of::<T>();

        if id == std::any::TypeId::of::<u8>() {
            true
        } else if id == std::any::TypeId::of::<u16>() {
            true
        } else if id == std::any::TypeId::of::<u32>() {
            true
        } else if id == std::any::TypeId::of::<u64>() {
            true
        } else if id == std::any::TypeId::of::<u128>() {
            true
        } else if id == std::any::TypeId::of::<i8>() {
            true
        } else if id == std::any::TypeId::of::<i16>() {
            true
        } else if id == std::any::TypeId::of::<i32>() {
            true
        } else if id == std::any::TypeId::of::<i64>() {
            true
        } else if id == std::any::TypeId::of::<i128>() {
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
        exchange_buffer: bool,
        context: &Arc<Context>,
        endpoints: &Vec<Arc<Endpoint>>,
        worker: &Arc<Worker>,
        pmi: Arc<dyn Pmi>,
        num_pes: usize,
        my_pe: usize,
        #[cfg(feature = "enable-on-node-shmem")] same_node_pes: &Vec<bool>,
        #[cfg(feature = "enable-on-node-shmem")] disable_on_node_shmem: bool,
        #[cfg(feature = "enable-on-node-shmem")] job_id: usize,
        mem_handles: Arc<Mutex<Vec<UcxAlloc>>>,
        remote_keys: Arc<Mutex<Vec<(UcxAlloc, HashMap<usize, RemoteAddressInfo>)>>>,
    ) -> AllocResult<UcxAlloc> {
        let data_size = if exchange_buffer {
            let mem_handle = MemoryHandleInner::alloc(context, 1024); //dummy allocation to get the size of the exchange buffer
            let mut data_size = mem_handle.addr.to_ne_bytes().len();
            data_size += mem_handle.pack().as_ref().len();
            data_size * num_pes
        } else {
            //we are the barrier buffer
            let mut n = 2;
            let num_rounds = if n > 1 && num_pes > 2 {
                ((num_pes as f64).log2() / (2 as f64).log2()).ceil() as usize
            } else {
                n = 1;
                1
            };
            let full_barrier_size = std::mem::size_of::<usize>() * num_rounds * n;
            let sub_barrier_size = std::mem::size_of::<u32>() * num_pes * 2;
            trace!(target: "ucx", "PE {}: Calculated barrier buffer size: full_barrier_size: {} sub_barrier_size: {} num_rounds: {} n: {}", my_pe, full_barrier_size, sub_barrier_size, num_rounds, n);
            full_barrier_size + sub_barrier_size
        };
        let (padding, size, _align) = calc_alloc_padding_size_align(data_size, 8);
        // debug!("Initial alloc size: {}", size);
        #[cfg(not(feature = "enable-on-node-shmem"))]
        let mem_handle = MemoryHandleInner::alloc(context, size);
        #[cfg(feature = "enable-on-node-shmem")]
        let (mem_handle, same_node_bases, same_node_segments) = if disable_on_node_shmem {
            (
                MemoryHandleInner::alloc(context, size * num_pes),
                vec![None; num_pes],
                vec![None; num_pes],
            )
        } else {
            let alloc_id = UCX_SHMEM_ALLOC_ID.fetch_add(1, Ordering::SeqCst);
            let shmem_id = format!("ucx_alloc_{}_pe_{}", alloc_id, my_pe);
            let local_segment =
                attach_shmem_segment(job_id, size * num_pes, 8, &shmem_id, alloc_id, true);
            let mem_handle = MemoryHandleInner::map_existing(
                context,
                local_segment.base_ptr() as *mut c_void,
                size * num_pes,
            );
            let (mut same_node_bases, same_node_segments) =
                build_same_node_segments(same_node_pes, job_id, size * num_pes, 8, alloc_id, my_pe);
            let mut same_node_segments = same_node_segments;
            same_node_bases[my_pe] = Some(local_segment.base_ptr() as usize);
            same_node_segments[my_pe] = Some(Arc::new(local_segment));
            (mem_handle, same_node_bases, same_node_segments)
        };
        let buffer_keys = mem_handle.exchange_key_pmi(endpoints, &pmi).unwrap();

        let mem = MemoryHandle {
            addr: mem_handle.addr,
            size: size,
            inner: mem_handle.clone(),
        };
        mem.as_mut_slice::<u8>().fill(0);

        let alloc = UcxAlloc::new(
            mem,
            data_size,
            padding,
            my_pe,
            num_pes,
            #[cfg(feature = "enable-on-node-shmem")]
            Arc::new(same_node_pes.clone()),
            #[cfg(feature = "enable-on-node-shmem")]
            Arc::new(same_node_bases),
            #[cfg(feature = "enable-on-node-shmem")]
            Arc::new(same_node_segments),
            context.clone(),
            worker.clone(),
            endpoints.clone(),
            buffer_keys.clone(),
            mem_handles.clone(),
            remote_keys.clone(),
            None,
        )?;
        mem_handles.lock().unwrap().push(alloc.clone());
        remote_keys
            .lock()
            .unwrap()
            .push((alloc.clone(), buffer_keys));
        Ok(alloc)
    }

    pub(crate) fn alloc(
        &self,
        data_size: usize,
        align: usize,
        alloc_type: AllocationType,
    ) -> UcxAlloc {
        match alloc_type {
            AllocationType::Sub(pes) => self.sub_alloc(&pes, data_size, align),
            AllocationType::Global => self.full_alloc(data_size, align),
            _ => panic!("Unexpected allocation type: {:?}", alloc_type),
        }
    }

    pub(crate) fn full_alloc(&self, data_size: usize, align: usize) -> UcxAlloc {
        //add space for ref count and padding to align it
        let (padding, size, _align) = calc_alloc_padding_size_align(data_size, align);
        #[cfg(not(feature = "enable-on-node-shmem"))]
        let mem_handle = MemoryHandleInner::alloc(&self.context, size);

        #[cfg(feature = "enable-on-node-shmem")]
        let (mem_handle, same_node_bases, same_node_segments) = if self.disable_on_node_shmem {
            (
                MemoryHandleInner::alloc(&self.context, size),
                vec![None; self.num_pes],
                vec![None; self.num_pes],
            )
        } else {
            let alloc_id = UCX_SHMEM_ALLOC_ID.fetch_add(1, Ordering::SeqCst);
            let shmem_id = format!("ucx_alloc_{}_pe_{}", alloc_id, self.my_pe);
            let local_segment =
                attach_shmem_segment(self.job_id, size, align, &shmem_id, alloc_id, true);
            let mem_handle = MemoryHandleInner::map_existing(
                &self.context,
                local_segment.base_ptr() as *mut c_void,
                size,
            );
            let (mut same_node_bases, same_node_segments) = build_same_node_segments(
                &self.same_node_pes,
                self.job_id,
                size,
                align,
                alloc_id,
                self.my_pe,
            );
            let mut same_node_segments = same_node_segments;
            same_node_bases[self.my_pe] = Some(local_segment.base_ptr() as usize);
            same_node_segments[self.my_pe] = Some(Arc::new(local_segment));
            (mem_handle, same_node_bases, same_node_segments)
        };
        let buffer_keys = mem_handle
            .exchange_key_alloc(
                &self.endpoints,
                // &self.pmi,
                &self.barrier.as_ref().unwrap().lock().unwrap(),
                &self.exchange_buffer.as_ref().unwrap(),
            )
            .unwrap();

        let mem = MemoryHandle {
            addr: mem_handle.addr,
            size: size,
            inner: mem_handle.clone(),
        };

        let alloc = UcxAlloc::new(
            mem,
            data_size,
            padding,
            self.my_pe,
            self.num_pes,
            #[cfg(feature = "enable-on-node-shmem")]
            Arc::new(self.same_node_pes.clone()),
            #[cfg(feature = "enable-on-node-shmem")]
            Arc::new(same_node_bases),
            #[cfg(feature = "enable-on-node-shmem")]
            Arc::new(same_node_segments),
            self.context.clone(),
            self.worker.clone(),
            self.endpoints.clone(),
            buffer_keys.clone(),
            self.mem_handles.clone(),
            self.remote_keys.clone(),
            self.ucc_world_team.clone(),
        )
        .expect("UcxAlloc::new failed");
        self.mem_handles.lock().unwrap().push(alloc.clone());
        self.remote_keys
            .lock()
            .unwrap()
            .push((alloc.clone(), buffer_keys));
        alloc
    }

    pub(crate) fn sub_alloc(&self, pes: &[usize], data_size: usize, align: usize) -> UcxAlloc {
        //add space for ref count and padding to align it
        let (padding, size, _align) = calc_alloc_padding_size_align(data_size, align);
        let mem_handle = MemoryHandleInner::alloc(&self.context, size);
        let buffer_keys_map = mem_handle
            .exchange_key_sub_alloc(
                &self.endpoints,
                pes,
                &self.barrier.as_ref().unwrap().lock().unwrap(),
                &self.exchange_buffer.as_ref().unwrap(),
            )
            .unwrap();

        let mem = MemoryHandle {
            addr: mem_handle.addr,
            size: size,
            inner: mem_handle.clone(),
        };

        #[cfg(feature = "enable-on-node-shmem")]
        let (same_node_bases, same_node_segments) = if self.disable_on_node_shmem {
            (vec![None; pes.len()], vec![None; pes.len()])
        } else {
            // For sub_alloc we don't create new shared segments here; use placeholders sized to `pes`.
            (vec![None; pes.len()], vec![None; pes.len()])
        };
        let ucc_team = self.ucc_context.as_ref().and_then(|ctx| {
            UccTeam::new_sub_team(self.my_pe, pes, ctx.clone())
                .map_err(|e| eprintln!("Failed to create UCC sub-team, falling back to manual collectives: {:?}", e))
                .ok()
        });
        let alloc = UcxAlloc::new(
            mem,
            data_size,
            padding,
            self.my_pe,
            pes.len(),
            #[cfg(feature = "enable-on-node-shmem")]
            Arc::new(self.same_node_pes.clone()),
            #[cfg(feature = "enable-on-node-shmem")]
            Arc::new(same_node_bases),
            #[cfg(feature = "enable-on-node-shmem")]
            Arc::new(same_node_segments),
            self.context.clone(),
            self.worker.clone(),
            self.endpoints.clone(),
            buffer_keys_map.clone(),
            self.mem_handles.clone(),
            self.remote_keys.clone(),
            ucc_team.map(Arc::new)
        )
        .expect("UcxAlloc::new failed");
        self.mem_handles.lock().unwrap().push(alloc.clone());
        self.remote_keys
            .lock()
            .unwrap()
            .push((alloc.clone(), buffer_keys_map));
        alloc
    }

    pub(crate) fn wait_all(&self) {
        self.worker
            .wait_all()
            .expect("UcxWorld::wait_all failed waiting on UCX requests");
    }

    pub(crate) fn progress(&self) {
        self.worker.progress();

        if let Some(ucc_team) = &self.ucc_world_team {
            ucc_team.context.progress().expect("Failed to progress UCC context");
        }
    }

    pub(crate) fn barrier(&self) {
        self.pmi.barrier(false).expect(" Failed to perform barrier");
    }

    pub(crate) fn pmi_barrier(&self) {
        self.pmi.barrier(false).expect(" Failed to perform barrier");
    }

    pub(crate) fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> Option<usize> {
        let allocs = self.remote_keys.lock().unwrap();
        for (alloc, remote_addrs) in allocs.iter() {
            if let Some(remote_info) = remote_addrs.get(&remote_pe) {
                let remote_pe_addr = remote_info.addr;
                if remote_pe_addr <= remote_addr
                    && remote_addr < remote_pe_addr + alloc.data_num_bytes
                {
                    let offset = remote_addr - remote_pe_addr;
                    return Some(alloc.mem.inner.addr + offset);
                }
            }
        }
        None
    }

    pub(crate) fn one_sided_alloc_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
        num_bytes: usize,
    ) -> CommAlloc {
        let allocs = self.remote_keys.lock().unwrap();
        for (alloc, remote_addrs) in allocs.iter() {
            if let Some(remote_info) = remote_addrs.get(&remote_pe) {
                let remote_pe_addr = remote_info.addr;
                if remote_pe_addr <= remote_addr
                    && remote_addr + num_bytes <= remote_pe_addr + alloc.data_num_bytes
                {
                    let offset = remote_addr - remote_pe_addr;
                    return OneSidedUcxAlloc {
                        alloc: alloc
                            .sub_alloc(offset, num_bytes)
                            .expect("one_sided_alloc_from_remote_pe_and_addr failed"),
                        remote_pe,
                    }
                    .into();
                }
            }
        }
        panic!(
            "unable to find allocation for remote pe: {} addr: {:x} num_bytes: {}",
            remote_pe, remote_addr, num_bytes
        );
    }

    pub(crate) fn local_alloc_and_offset_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
    ) -> Option<(CommAlloc, usize)> {
        let allocs = self.remote_keys.lock().unwrap();
        for (alloc, remote_addrs) in allocs.iter() {
            if let Some(remote_info) = remote_addrs.get(&remote_pe) {
                let remote_pe_addr = remote_info.addr;
                if remote_pe_addr <= remote_addr
                    && remote_addr < remote_pe_addr + alloc.data_num_bytes
                {
                    let offset = remote_addr - remote_pe_addr;
                    return Some((alloc.clone().into(), offset));
                }
            }
        }
        None
    }

    pub(crate) fn remote_addr(&self, pe: usize, local_addr: usize) -> Option<usize> {
        let allocs = self.remote_keys.lock().unwrap();
        for (alloc, remote_addrs) in allocs.iter() {
            if alloc.mem.inner.addr <= local_addr
                && local_addr < alloc.mem.inner.addr + alloc.data_num_bytes
            {
                let offset = local_addr - alloc.mem.inner.addr;
                if let Some(remote_info) = remote_addrs.get(&pe) {
                    let remote_pe_addr = remote_info.addr;
                    return Some(remote_pe_addr + offset);
                }
            }
        }
        None
    }

    pub(crate) fn get_alloc_from_start_addr(
        &self,
        addr: CommAllocAddr,
    ) -> Result<UcxAlloc, String> {
        let allocs = self.mem_handles.lock().unwrap();
        for alloc in allocs.iter() {
            if alloc.mem.inner.addr == *addr {
                return Ok(alloc.clone());
            }
        }
        Err(format!("No allocation found for address {:x}", addr))
    }

    pub(crate) fn clear_allocs(&self) {
        let mut mem_handles = self.mem_handles.lock().unwrap();
        let temp_handles = mem_handles.drain(..).collect::<Vec<_>>();
        drop(mem_handles);
        let mut remote_keys = self.remote_keys.lock().unwrap();
        let temp_keys = remote_keys.drain(..).collect::<Vec<_>>();
        drop(remote_keys);
        for (_alloc, rkeys) in temp_keys.into_iter() {
            for remote_info in rkeys.values() {
                let ref_cnt = Arc::strong_count(&remote_info.rkey);
                debug!(
                    "Clearing rkey for addr {:x}, ref count: {}",
                    remote_info.addr, ref_cnt
                );
            }
        }
        debug!("Cleared {} allocations", temp_handles.len());
    }
}

#[cfg(feature = "enable-on-node-shmem")]
fn build_same_node_segments(
    same_node_pes: &Vec<bool>,
    job_id: usize,
    size: usize,
    align: usize,
    alloc_id: usize,
    my_pe: usize,
) -> (Vec<Option<usize>>, Vec<Option<Arc<ShmemSegment>>>) {
    let mut bases = vec![None; same_node_pes.len()];
    let mut segments = vec![None; same_node_pes.len()];
    if same_node_pes.is_empty() {
        return (bases, segments);
    }

    for (pe, is_same_node) in same_node_pes.iter().enumerate() {
        trace!("PE {} same node with PE {}: {}", my_pe, pe, is_same_node);
        if !*is_same_node {
            continue;
        }
        if pe == my_pe {
            continue;
        }
        let shmem_id = format!("ucx_alloc_{}_pe_{}", alloc_id, pe);
        let segment = attach_shmem_segment(job_id, size, align, &shmem_id, alloc_id, false);
        bases[pe] = Some(segment.base_ptr() as usize);
        segments[pe] = Some(Arc::new(segment));
    }

    (bases, segments)
}

impl Drop for UcxWorld {
    fn drop(&mut self) {
        trace!(target: "drop", "begin drop UcxWorld");
        debug!("dropping ucx world");
        self.pmi_barrier();

        // Tear down UCC objects first so they release any UCX-side resources
        // before we clear allocations and drop UCX context state.
        self.ucc_world_team.take();
        self.ucc_context.take();
        self.ucc_world_buffer.take();

        self.exchange_buffer.take();
        self.barrier.take();
        self.clear_allocs();
        let mem_handles_cnt = Arc::strong_count(&self.mem_handles);
        let remote_keys_cnt = Arc::strong_count(&self.remote_keys);
        debug!(
            "mem handle count: {} remote keys cnt: {}",
            mem_handles_cnt, remote_keys_cnt
        );
        // self.remote_keys.lock().unwrap().clear();
        // self.mem_handles.lock().unwrap().clear();
        self.pmi_barrier();
        debug!("dropped ucx world");
        trace!(target: "drop", "end drop UcxWorld");
    }
}

#[derive(Clone)]
enum AllocTable {
    Fabric(
        Arc<Mutex<Vec<UcxAlloc>>>,
        Arc<Mutex<Vec<(UcxAlloc, HashMap<usize, RemoteAddressInfo>)>>>,
    ),
    Runtime(
        BTreeAlloc,
        usize,
        Arc<Mutex<Vec<UcxAlloc>>>,
        Arc<Mutex<Vec<(UcxAlloc, HashMap<usize, RemoteAddressInfo>)>>>,
    ), //the usize is the offset of the rt_alloc so that we can free it properly if a sub_alloc is the last reference
}

pub(crate) struct UcxAlloc {
    mem: MemoryHandle,
    data_num_bytes: usize,
    pub(crate) my_pe: usize,
    pub(crate) num_pes: usize,
    #[cfg(feature = "enable-on-node-shmem")]
    same_node_pes: Arc<Vec<bool>>,
    #[cfg(feature = "enable-on-node-shmem")]
    same_node_bases: Arc<Vec<Option<usize>>>,
    #[cfg(feature = "enable-on-node-shmem")]
    same_node_segments: Arc<Vec<Option<Arc<ShmemSegment>>>>,
    fabric_ref_cnt_offset: usize,
    rt_ref_cnt_offset: usize,
    context: Arc<Context>,
    worker: Arc<Worker>,
    endpoints: Arc<Vec<Arc<Endpoint>>>,
    remote_keys: Arc<HashMap<usize, RemoteAddressInfo>>,
    alloc_table: AllocTable,
    ucc_team: Option<Arc<UccTeam>>,
}

#[lamellar_prof::prof]
impl Clone for UcxAlloc {
    fn clone(&self) -> Self {
        self.increment_fabric_ref_count();
        if let AllocTable::Runtime(_, _, _, _) = &self.alloc_table {
            self.increment_rt_ref_count();
        }
        Self {
            mem: self.mem.clone(),
            data_num_bytes: self.data_num_bytes,
            my_pe: self.my_pe,
            num_pes: self.num_pes,
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_pes: self.same_node_pes.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_bases: self.same_node_bases.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_segments: self.same_node_segments.clone(),
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: self.rt_ref_cnt_offset,
            context: self.context.clone(),
            worker: self.worker.clone(),
            endpoints: self.endpoints.clone(),
            remote_keys: self.remote_keys.clone(),
            alloc_table: self.alloc_table.clone(),
            ucc_team: self.ucc_team.clone(),
        }
    }
}

impl From<UcxAlloc> for CommAlloc {
    fn from(alloc: UcxAlloc) -> Self {
        CommAlloc {
            inner_alloc: Arc::new(CommAllocInner::UcxAlloc(alloc)),
            // alloc_type: CommAllocType::Fabric,
        }
    }
}

impl std::fmt::Debug for UcxAlloc {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let fabric_ref_count = unsafe {
            (&*(self.mem.inner.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize))
                .load(Ordering::SeqCst)
        };

        let mut temp = f.debug_struct("UcxAlloc");
        temp.field(
            "addr",
            &format_args!(
                "{:x} - ({:x}) {:x}",
                self.mem.addr,
                self.mem.addr + self.data_num_bytes,
                self.mem.addr + self.mem.size
            ),
        )
        .field("data_num_bytes", &self.data_num_bytes)
        .field("my_pe", &self.my_pe)
        .field("num_pes", &self.num_pes)
        .field(
            "fabric_ref_cnt_offset",
            &format_args!(
                "{} ({:?}) cnt: {}",
                self.fabric_ref_cnt_offset,
                unsafe {
                    self.mem.inner.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize
                },
                fabric_ref_count
            ),
        );
        if let AllocTable::Runtime(_, _, _, _) = &self.alloc_table {
            let rt_ref_count = unsafe {
                (&*(self.mem.inner.as_ptr().add(self.rt_ref_cnt_offset) as *const AtomicUsize))
                    .load(Ordering::SeqCst)
            };
            let padding = decode_padding(rt_ref_count);
            let rt_ref_count = decode_ref_count(rt_ref_count);
            temp.field(
                "rt_ref_cnt_offset",
                &format_args!(
                    "{} ({:?}) cnt: {}, padding: {}",
                    self.rt_ref_cnt_offset,
                    unsafe {
                        self.mem.inner.as_ptr().add(self.rt_ref_cnt_offset) as *const AtomicUsize
                    },
                    rt_ref_count,
                    padding,
                ),
            );
        }
        temp.finish()
    }
}

#[lamellar_prof::prof]
impl UcxAlloc {
    unsafe fn negate_atomic_value<T>(value: *mut T) {
        let num_bytes = std::mem::size_of::<T>();
        let mut bytes = vec![0u8; num_bytes];
        std::ptr::copy(value.cast::<u8>(), bytes.as_mut_ptr(), num_bytes);
        for byte in bytes.iter_mut() {
            *byte = !*byte;
        }

        let mut carry: u16 = 1;
        #[cfg(target_endian = "little")]
        for byte in bytes.iter_mut() {
            let sum = *byte as u16 + carry;
            *byte = sum as u8;
            carry = sum >> 8;
            if carry == 0 {
                break;
            }
        }
        #[cfg(target_endian = "big")]
        for byte in bytes.iter_mut().rev() {
            let sum = *byte as u16 + carry;
            *byte = sum as u8;
            carry = sum >> 8;
            if carry == 0 {
                break;
            }
        }

        std::ptr::copy(bytes.as_ptr(), value.cast::<u8>(), num_bytes);
    }

    fn ucx_atomic_update<T: Copy>(op: &mut AtomicOp<T>) -> (ucp_atomic_op_t, *const T) {
        match op {
            AtomicOp::Write(val) => (ucp_atomic_op_t::UCP_ATOMIC_OP_SWAP, val.as_ref().get_ref()),
            AtomicOp::Sum(val) | AtomicOp::FetchSum(val) => {
                (ucp_atomic_op_t::UCP_ATOMIC_OP_ADD, val.as_ref().get_ref())
            }
            AtomicOp::Sub(val) => (ucp_atomic_op_t::UCP_ATOMIC_OP_ADD, unsafe {
                Self::negate_atomic_value(val.as_mut().get_unchecked_mut());
                val.as_ref().get_ref()
            }),
            AtomicOp::FetchSub(val) => (ucp_atomic_op_t::UCP_ATOMIC_OP_ADD, unsafe {
                Self::negate_atomic_value(val.as_mut().get_unchecked_mut());
                val.as_ref().get_ref()
            }),
            AtomicOp::BitAnd(val) | AtomicOp::FetchBitAnd(val) => {
                (ucp_atomic_op_t::UCP_ATOMIC_OP_AND, val.as_ref().get_ref())
            }
            AtomicOp::BitOr(val) | AtomicOp::FetchBitOr(val) => {
                (ucp_atomic_op_t::UCP_ATOMIC_OP_OR, val.as_ref().get_ref())
            }
            AtomicOp::BitXor(val) | AtomicOp::FetchBitXor(val) => {
                (ucp_atomic_op_t::UCP_ATOMIC_OP_XOR, val.as_ref().get_ref())
            }
            _ => panic!("Unsupported atomic operation"),
        }
    }

    pub(crate) fn new(
        mem: MemoryHandle,
        data_num_bytes: usize,
        padding: usize,
        my_pe: usize,
        num_pes: usize,
        #[cfg(feature = "enable-on-node-shmem")] same_node_pes: Arc<Vec<bool>>,
        #[cfg(feature = "enable-on-node-shmem")] same_node_bases: Arc<Vec<Option<usize>>>,
        #[cfg(feature = "enable-on-node-shmem")] same_node_segments: Arc<
            Vec<Option<Arc<ShmemSegment>>>,
        >,
        context: Arc<Context>,
        worker: Arc<Worker>,
        endpoints: Vec<Arc<Endpoint>>,
        my_remote_keys: HashMap<usize, RemoteAddressInfo>,
        mem_handles: Arc<Mutex<Vec<UcxAlloc>>>,
        remote_keys: Arc<Mutex<Vec<(UcxAlloc, HashMap<usize, RemoteAddressInfo>)>>>,
        ucc_team: Option<Arc<UccTeam>>,
    ) -> AllocResult<Self> {
        let ref_cnt_offset = data_num_bytes + padding;
        let fabric_ref_cnt_offset = data_num_bytes + padding;
        let encoded = encode_ref_count_and_padding(1, padding);
        let alloc = Self {
            mem,
            data_num_bytes,
            my_pe,
            num_pes,
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_pes,
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_bases,
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_segments,
            fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            context,
            worker,
            endpoints: Arc::new(endpoints),
            remote_keys: Arc::new(my_remote_keys),
            alloc_table: AllocTable::Fabric(mem_handles.clone(), remote_keys.clone()),
            ucc_team,
        };


        unsafe {
            (&*(alloc.mem.inner.as_ptr().add(alloc.fabric_ref_cnt_offset) as *mut AtomicUsize))
                .store(encoded, Ordering::SeqCst);
        }
        debug!(target: "ucx", "Created UCX allocation: {:?}", alloc);
        Ok(alloc)
    }
    pub(crate) fn start(&self) -> usize {
        self.mem.addr.into()
    }
    pub(crate) fn num_bytes(&self) -> usize {
        self.data_num_bytes
    }
    #[cfg(feature = "enable-on-node-shmem")]
    pub(crate) fn same_node_addr(&self, pe: usize, offset_bytes: usize) -> Option<CommAllocAddr> {
        self.same_node_bases
            .get(pe)
            .and_then(|base| base.map(|addr| CommAllocAddr(addr + offset_bytes)))
    }
    #[cfg(feature = "enable-on-node-shmem")]
    fn shifted_same_node_bases(&self, offset: usize) -> Arc<Vec<Option<usize>>> {
        Arc::new(
            self.same_node_bases
                .iter()
                .map(|base| base.map(|addr| addr + offset))
                .collect(),
        )
    }
    pub(crate) fn sub_alloc(&self, offset: usize, size: usize) -> AllocResult<Self> {
        if offset + size > self.num_bytes() {
            return Err(AllocError::InvalidSubAlloc(offset, size));
        }
        let remote_keys = self
            .remote_keys
            .iter()
            .map(|(pe, remote)| {
                (
                    *pe,
                    RemoteAddressInfo {
                        addr: remote.addr + offset,
                        rkey: remote.rkey.clone(),
                    },
                )
            })
            .collect::<HashMap<usize, RemoteAddressInfo>>();
        self.increment_fabric_ref_count();
        if let AllocTable::Runtime(_, _, _, _) = &self.alloc_table {
            self.increment_rt_ref_count();
        }
        let alloc = UcxAlloc {
            mem: self.mem.sub_alloc(offset, size),
            data_num_bytes: size,
            my_pe: self.my_pe,
            num_pes: self.num_pes,
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_pes: self.same_node_pes.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_bases: self.shifted_same_node_bases(offset),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_segments: self.same_node_segments.clone(),
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: self.rt_ref_cnt_offset, //keep the same ref count offset as the parent allocation if this is actually a rt alloc, it will be updated when converted to a rt_alloc
            context: self.context.clone(),
            worker: self.worker.clone(),
            endpoints: self.endpoints.clone(),
            remote_keys: Arc::new(remote_keys),
            alloc_table: self.alloc_table.clone(),
            ucc_team: self.ucc_team.clone(),
        };


        debug!(target: "ucx", "Created UCX sub-allocation: {:?}", alloc);
        Ok(alloc)
    }

    //we call this function to create a sub-allocation that is tracked as part of a runtime allocation
    pub(crate) fn rt_alloc(
        &self,
        alloc_table: BTreeAlloc,
        offset: usize,
        padding: usize,
        size: usize,
    ) -> AllocResult<Self> {
        if offset + size > self.num_bytes() {
            return Err(AllocError::InvalidSubAlloc(offset, size));
        }
        let data_bytes = size - padding - std::mem::size_of::<AtomicUsize>();
        let my_remote_keys = self
            .remote_keys
            .iter()
            .map(|(pe, remote)| {
                (
                    *pe,
                    RemoteAddressInfo {
                        addr: remote.addr + offset,
                        rkey: remote.rkey.clone(),
                    },
                )
            })
            .collect::<HashMap<usize, RemoteAddressInfo>>();

        self.increment_fabric_ref_count();
        let ref_cnt_offset = offset + data_bytes + padding;
        let encoded = encode_ref_count_and_padding(1, padding);

        let (mem_handles, remote_keys) = match &self.alloc_table {
            AllocTable::Fabric(mem_handles, remote_keys) => {
                (mem_handles.clone(), remote_keys.clone())
            }
            AllocTable::Runtime(_, _, mem_handles, remote_keys) => {
                (mem_handles.clone(), remote_keys.clone())
            }
        };

        let mem = self.mem.sub_alloc(offset, size);
        let addr = mem.addr;

        let alloc = UcxAlloc {
            mem,
            data_num_bytes: data_bytes,
            my_pe: self.my_pe,
            num_pes: self.num_pes,
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_pes: self.same_node_pes.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_bases: self.shifted_same_node_bases(offset),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_segments: self.same_node_segments.clone(),
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            context: self.context.clone(),
            worker: self.worker.clone(),
            endpoints: self.endpoints.clone(),
            remote_keys: Arc::new(my_remote_keys),
            alloc_table: AllocTable::Runtime(
                alloc_table,
                addr,
                mem_handles.clone(),
                remote_keys.clone(),
            ),
            ucc_team: self.ucc_team.clone(),
        };

        unsafe {
            (&*(alloc.mem.inner.as_ptr().add(alloc.rt_ref_cnt_offset) as *mut AtomicUsize))
                .store(encoded, Ordering::SeqCst);
        }
        debug!(target: "ucx", "Created UCX rt-sub-allocation: {:?}", alloc);
        Ok(alloc)
    }

    // This function is used to construct an rt_alloc from a raw sub-allocation
    // typically paired with a call to leak() we decrement the ref count as this instance recaptures the leaked instance
    pub(crate) fn as_rt_alloc(self, alloc_table: BTreeAlloc) -> AllocResult<Self> {
        let (mem_handles, remote_keys) = match &self.alloc_table {
            AllocTable::Fabric(mem_handles, remote_keys) => {
                (mem_handles.clone(), remote_keys.clone())
            }
            AllocTable::Runtime(_, _, mem_handles, remote_keys) => {
                (mem_handles.clone(), remote_keys.clone())
            }
        };

        //since we are recapturing a leaked alloc, the non-rt sub-allocation we are converting should contain the appropriate ref count space at the end of the allocation
        let ref_cnt_offset = ((self.start() - self.mem.inner.as_ptr() as usize) + self.num_bytes())
            - std::mem::size_of::<AtomicUsize>();

        let encoded_ref_count = unsafe {
            (&*(self.mem.inner.as_ptr().add(ref_cnt_offset) as *const AtomicUsize))
                .load(Ordering::SeqCst)
        };

        let padding = decode_padding(encoded_ref_count);

        let alloc = Self {
            mem: self.mem.clone(),
            data_num_bytes: self.data_num_bytes - padding - std::mem::size_of::<AtomicUsize>(),
            my_pe: self.my_pe,
            num_pes: self.num_pes,
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_pes: self.same_node_pes.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_bases: self.same_node_bases.clone(),
            #[cfg(feature = "enable-on-node-shmem")]
            same_node_segments: self.same_node_segments.clone(),
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            context: self.context.clone(),
            worker: self.worker.clone(),
            endpoints: self.endpoints.clone(),
            remote_keys: self.remote_keys.clone(),
            alloc_table: AllocTable::Runtime(
                alloc_table,
                self.mem.addr,
                mem_handles.clone(),
                remote_keys.clone(),
            ),
            ucc_team: self.ucc_team.clone(),
        };


        debug!(target: "ucx", "Converted UCX alloc to rt-alloc: {:?}", alloc);
        Ok(alloc)
    }

    pub(crate) fn leak(self) -> Option<CommAllocAddr> {
        match self.alloc_table {
            AllocTable::Fabric(_, _) => None, //only rt_allocs can be leaked
            AllocTable::Runtime(_, _, _, _) => {
                self.increment_fabric_ref_count(); //increment the ref count to account for the leaked instance
                self.increment_rt_ref_count(); //increment the ref count to account for the leaked instance
                debug!(target: "ucx", "Leaked UCX rt-alloc: {:?}", self);
                Some(CommAllocAddr(self.start()))
            }
        }
    }

    pub(crate) fn increment_fabric_ref_count(&self) -> usize {
        let ref_count = unsafe {
            &*(self.mem.inner.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize)
        };
        increment_ref_count(ref_count)
    }

    pub(crate) fn decrement_fabric_ref_count(&self) -> usize {
        let ref_count = unsafe {
            &*(self.mem.inner.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize)
        };
        decrement_ref_count(ref_count)
    }

    pub(crate) fn increment_rt_ref_count(&self) -> usize {
        let ref_count = unsafe {
            &*(self.mem.inner.as_ptr().add(self.rt_ref_cnt_offset) as *const AtomicUsize)
        };
        increment_ref_count(ref_count)
    }
    pub(crate) fn decrement_rt_ref_count(&self) -> usize {
        let ref_count = unsafe {
            &*(self.mem.inner.as_ptr().add(self.rt_ref_cnt_offset) as *const AtomicUsize)
        };
        decrement_ref_count(ref_count)
    }

    pub(crate) unsafe fn put_inner<T: Copy>(
        &self,
        pe: usize,
        offset: usize, //with respect to T
        src_addr: &[T],
        blocking: bool,
        managed: bool,
    ) -> Option<UcxRequest> {
        let offset = offset * std::mem::size_of::<T>();
        trace!(target: "ucx",
            "put_inner my_pe {} pe {} offset {} src_addr len {} * size_of T {} total bytes {}, alloc local size {}",
            self.my_pe,
            pe,
            offset,
            src_addr.len(),
            std::mem::size_of::<T>(),
            src_addr.len() * std::mem::size_of::<T>(),
            self.num_bytes(),
        );
        assert!(offset + src_addr.len() * std::mem::size_of::<T>() <= self.num_bytes());
        trace!(target: "ucx", "put_inner pe {} offset {} src_addr {:p} local addr {:p}", pe, offset, src_addr.as_ptr(), (self.start() + offset) as *mut u8);
        if pe == self.my_pe {
            std::ptr::copy(
                src_addr.as_ptr() as *const u8,
                (self.start() + offset) as *mut u8,
                src_addr.len() * std::mem::size_of::<T>(),
            );
            trace!(target: "ucx", "Completed local put to pe {} at offset {}", pe, offset);
            return None;
        }
        #[cfg(feature = "enable-on-node-shmem")]
        if let Some(addr) = self.same_node_addr(pe, offset) {
            std::ptr::copy(
                src_addr.as_ptr() as *const u8,
                addr.as_ptr::<u8>() as *mut u8,
                src_addr.len() * std::mem::size_of::<T>(),
            );
            trace!(target: "ucx", "Completed same-node put to pe {} at offset {}", pe, offset);
            return None;
        }
        let (remote_addr, rkey) = if let Some(remote_info) = self.remote_keys.get(&pe) {
            trace!(target: "ucx", "Found remote key for pe {}: addr {:x} rkey {:?}", pe, remote_info.addr, remote_info.rkey);
            (remote_info.addr, &remote_info.rkey)
        } else {
            trace!(target: "ucx", "Missing remote key for pe {}", pe);
            panic!("put_inner missing remote key for pe {}", pe);
        };
        trace!(target: "ucx",
            "put to pe {} at remote addr {:x} + offset {:?}, final addr: {:x} rkey: {:?}",
            pe,
            remote_addr,
            offset,
            remote_addr + offset,
            rkey
        );
        let req = self.endpoints[pe].put(
            src_addr.as_ptr() as _,
            src_addr.len() * std::mem::size_of::<T>(),
            remote_addr + offset,
            rkey,
            managed,
        );
        if blocking {
            if let Some(req) = req {
                req.wait().expect("blocking_put failed");
            }
            None
        } else {
            req
        }
    }

    pub(crate) unsafe fn inner_get<T: Copy>(
        &self,
        pe: usize,
        offset: usize,
        blocking: bool,
        dst_addr: &mut [T],
    ) -> Option<UcxRequest> {
        let offset = offset * std::mem::size_of::<T>();
        trace!(target: "ucx",
            "get_inner pe {} offset {} dst_addr len {} * size_of T {} total bytes {}, alloc local size {}",
            pe,
            offset,
            dst_addr.len(),
            std::mem::size_of::<T>(),
            dst_addr.len() * std::mem::size_of::<T>(),
            self.num_bytes(),
        );
        assert!(offset + dst_addr.len() * std::mem::size_of::<T>() <= self.num_bytes());
        if pe == self.my_pe {
            std::ptr::copy(
                (self.start() + offset) as *const u8,
                dst_addr.as_mut_ptr() as *mut u8,
                dst_addr.len() * std::mem::size_of::<T>(),
            );
            return None;
        }
        #[cfg(feature = "enable-on-node-shmem")]
        if let Some(addr) = self.same_node_addr(pe, offset) {
            std::ptr::copy(
                addr.as_ptr::<u8>(),
                dst_addr.as_mut_ptr() as *mut u8,
                dst_addr.len() * std::mem::size_of::<T>(),
            );
            return None;
        }
        let (remote_addr, rkey) = if let Some(remote_info) = self.remote_keys.get(&pe) {
            (remote_info.addr, &remote_info.rkey)
        } else {
            panic!("inner_get missing remote key for pe {}", pe);
        };
        let req = self.endpoints[pe].get(
            dst_addr.as_mut_ptr() as _,
            dst_addr.len() * std::mem::size_of::<T>(),
            remote_addr + offset,
            rkey,
        );
        if blocking {
            req.wait().expect("blocking_get failed");
            None
        } else {
            Some(req)
        }
    }

    pub(crate) fn inner_atomic_op<T: Copy + 'static>(
        &self,
        pe: usize,
        offset: usize,
        blocking: bool,
        op: &mut AtomicOp<T>,
        managed: bool,
    ) -> Option<UcxRequest> {
        let offset = offset * std::mem::size_of::<T>();
        debug_assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let (remote_addr, rkey) = if let Some(remote_info) = self.remote_keys.get(&pe) {
            (remote_info.addr, &remote_info.rkey)
        } else {
            panic!("inner_atomic_op missing remote key for pe {}", pe);
        };
        let req = match op {
            AtomicOp::Write(val) => self.endpoints[pe].atomic_swap(
                val.as_ref().get_ref(),
                &ATOMIC_PUT_TMP as *const _ as *mut T,
                remote_addr + offset,
                rkey,
                managed,
            ),
            AtomicOp::Sum(_)
            | AtomicOp::Sub(_)
            | AtomicOp::BitAnd(_)
            | AtomicOp::BitOr(_)
            | AtomicOp::BitXor(_) => {
                let (ucx_op, val) = Self::ucx_atomic_update(op);
                self.endpoints[pe].atomic_op(ucx_op, val, remote_addr + offset, rkey, managed)
            }
            AtomicOp::FetchMin(_)
            | AtomicOp::FetchMax(_)
            | AtomicOp::FetchSum(_)
            | AtomicOp::FetchSub(_)
            | AtomicOp::FetchProd(_)
            | AtomicOp::FetchBitOr(_)
            | AtomicOp::FetchBitXor(_)
            | AtomicOp::FetchBitAnd(_) => {
                panic!("Fetch atomic ops must use the fetch path")
            }
            AtomicOp::Cas => {
                panic!("Compare atomic ops must use the compare path")
            }
            _ => panic!("Unsupported atomic operation"),
        };
        if blocking {
            self.endpoints[pe]
                .ep_wait_all()
                .expect("blocking_atomic_op failed");
            None
        } else {
            req
        }
    }

    pub(crate) fn inner_atomic_fetch_op<T: Copy + 'static>(
        &self,
        pe: usize,
        offset: usize,
        blocking: bool,
        op: &mut AtomicOp<T>,
        result: &mut [T],
    ) -> Option<UcxRequest> {
        let offset = offset * std::mem::size_of::<T>();
        debug_assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let req = match op {
            AtomicOp::Read(zero) => {
                let (remote_addr, rkey) = if let Some(remote_info) = self.remote_keys.get(&pe) {
                    (remote_info.addr, &remote_info.rkey)
                } else {
                    panic!(
                        "inner_atomic_fetch_op missing remote key for pe {} (read)",
                        pe
                    );
                };
                self.endpoints[pe].atomic_get(
                    &**zero,
                    result.as_mut_ptr(),
                    remote_addr + offset,
                    rkey,
                )
            }
            AtomicOp::Write(_)
            | AtomicOp::FetchSum(_)
            | AtomicOp::FetchSub(_)
            | AtomicOp::FetchBitAnd(_)
            | AtomicOp::FetchBitOr(_)
            | AtomicOp::FetchBitXor(_) => {
                let (ucx_op, val) = Self::ucx_atomic_update(op);
                let (remote_addr, rkey) = if let Some(remote_info) = self.remote_keys.get(&pe) {
                    (remote_info.addr, &remote_info.rkey)
                } else {
                    panic!("inner_atomic_fetch_op missing remote key for pe {}", pe);
                };
                self.endpoints[pe].atomic_fetch_op(
                    ucx_op,
                    val,
                    result.as_mut_ptr(),
                    remote_addr + offset,
                    rkey,
                )
            }
            AtomicOp::Min(_)
            | AtomicOp::Max(_)
            | AtomicOp::Sum(_)
            | AtomicOp::Sub(_)
            | AtomicOp::Prod(_)
            | AtomicOp::BitAnd(_)
            | AtomicOp::BitOr(_)
            | AtomicOp::BitXor(_) => {
                panic!("Non-fetch atomic ops must use the non-fetch path")
            }
            AtomicOp::Cas => {
                panic!("Compare atomic ops must use the compare path")
            }
            _ => panic!("Unsupported atomic operation"),
        };
        if blocking {
            req.wait().expect("atomic_fetch_op_blocking failed");
            None
        } else {
            Some(req)
        }
    }

    pub(crate) fn inner_atomic_compare_exchange_op<T: Copy + 'static>(
        &self,
        pe: usize,
        offset: usize,
        blocking: bool,
        compare: *const T,
        result: &mut [T],
    ) -> Option<UcxRequest> {
        let offset = offset * std::mem::size_of::<T>();
        debug_assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let (remote_addr, rkey) = if let Some(remote_info) = self.remote_keys.get(&pe) {
            (remote_info.addr, &remote_info.rkey)
        } else {
            panic!(
                "inner_atomic_compare_exchange_op missing remote key for pe {}",
                pe
            );
        };
        // result[0] must be pre-initialized to `new` (Z) before calling;
        // after completion it holds the original remote value (old Y).
        let req = self.endpoints[pe].atomic_compare_swap(
            compare,
            result.as_mut_ptr(),
            remote_addr + offset,
            rkey,
        );
        if blocking {
            req.wait()
                .expect("atomic_compare_exchange_blocking_op failed");
            None
        } else {
            Some(req)
        }
    }

    pub(crate) fn allgather_inner<T: Copy + 'static>(
        &self,
        src: &[T],
        result: &mut [T],
        blocking: bool,
    ) -> Result<Option<UccRequest>, ucc::Error> {
        if let Some(ucc_team) = &self.ucc_team {
            let req = ucc_team
                .allgather(src, result)?;

            if blocking {
                self.wait_ucc_request(&req)?;
                Ok(None)
            } else {
                Ok(Some(req))
            }
        } else {
            panic!("UCC team not initialized for allgather");
        }
    }

    pub(crate) fn allreduce_inner<T: Copy + 'static>(
        &self,
        op: &AllReduceOp,
        src: &[T],
        result: &mut [T],
        blocking: bool,
    ) -> Result<Option<UccRequest>, ucc::Error> {
        if let Some(ucc_team) = &self.ucc_team {
            let req = ucc_team
                .allreduce(src, result, op.clone())?;
            if blocking {
                self.wait_ucc_request(&req)?;
                Ok(None)
            } else {
                Ok(Some(req))
            }
        } else {
            panic!("UCC team not initialized for allreduce");
        }
    }

    pub(crate) fn allreduce_inplace_inner<T: Copy + 'static>(
        &self,
        op: &AllReduceOp,
        src_and_result: &mut [T],
        blocking: bool,
    ) -> Result<Option<UccRequest>, ucc::Error> {
        let src = unsafe { std::slice::from_raw_parts(src_and_result.as_ptr(), src_and_result.len()) };
        self.allreduce_inner(op, src, src_and_result, blocking)
    }

    pub(crate) fn alltoall_inner<T: Copy + 'static>(
        &self,
        src: &[T],
        result: &mut [T],
        blocking: bool,
    ) -> Result<Option<UccRequest>, ucc::Error> {
        if let Some(ucc_team) = &self.ucc_team {
            let req = ucc_team.alltoall(src, result)?;
            if blocking {
                self.wait_ucc_request(&req)?;
                Ok(None)
            } else {
                Ok(Some(req))
            }
        } else {
            panic!("UCC team not initialized for alltoall");
        }
    }

    pub(crate) fn reduce_inner<T: Copy + 'static>(
        &self,
        op: &AllReduceOp,
        src: &[T],
        slice_or_pe: RootOrSliceMut<'_, T>,
        blocking: bool,
    ) -> Result<Option<UccRequest>, ucc::Error> {
        if let Some(ucc_team) = &self.ucc_team {
            let (result, root_pe) = match slice_or_pe {
                RootOrSliceMut::Root(result) => (Some(result), self.my_pe),
                RootOrSliceMut::NotRoot(root_pe) => (None, root_pe),
            };
            let res = match result {
                Some(res) => res,
                None => unsafe {
                    std::slice::from_raw_parts_mut(src.as_ptr() as *mut T, src.len())
                },
            };

            let req = ucc_team.reduce(src, res, root_pe, op.clone())?;
            if blocking {
                self.wait_ucc_request(&req)?;
                Ok(None)
            } else {
                Ok(Some(req))
            }
        } else {
            panic!("UCC team not initialized for reduce");
        }
    }

    pub(crate) fn gather_inner<T: Copy + 'static>(
        &self,
        src: &[T],
        slice_or_pe: RootOrSliceMut<'_, T>,
        blocking: bool,
    ) -> Result<Option<UccRequest>, ucc::Error> {
        if let Some(ucc_team) = &self.ucc_team {
            let (result, root_pe) = match slice_or_pe {
                RootOrSliceMut::Root(result) => (Some(result), self.my_pe),
                RootOrSliceMut::NotRoot(root_pe) => (None, root_pe),
            };
            let res = match result {
                Some(res) => res,
                None => unsafe {
                    std::slice::from_raw_parts_mut(src.as_ptr() as *mut T, src.len())
                },
            };

            let req = ucc_team.gather(src, res, root_pe)?;
            if blocking {
                self.wait_ucc_request(&req)?;
                Ok(None)
            } else {
                Ok(Some(req))
            }
        } else {
            panic!("UCC team not initialized for gather");
        }
    }

    pub(crate) fn broadcast_inner<T: Copy + 'static>(
        &self,
        root_src: RootSrcOrSliceMut<'_, T>,
        blocking: bool,
    ) -> Result<Option<UccRequest>, ucc::Error> {
        if let Some(ucc_team) = &self.ucc_team {
            let (res, root_pe) = match root_src {
                RootSrcOrSliceMut::Root(src) => {
                    (unsafe { std::slice::from_raw_parts_mut(src.as_ptr() as *mut T, src.len()) }, self.my_pe)
                }
                RootSrcOrSliceMut::NotRoot(result, root_pe) => (result, root_pe),
            };

            let src = unsafe { std::slice::from_raw_parts(res.as_ptr(), res.len()) };
            let req = ucc_team.broadcast(src, res, root_pe)?;
            if blocking {
                self.wait_ucc_request(&req)?;
                Ok(None)
            } else {
                Ok(Some(req))
            }
        } else {
            panic!("UCC team not initialized for broadcast");
        }
    }

    pub(crate) fn scatter_inner<T: Copy + 'static>(
        &self,
        res: &mut [T],
        src_or_root_pe: RootSrcSliceOrNone<'_, T>,
        blocking: bool,
    ) -> Result<Option<UccRequest>, ucc::Error> {
        if let Some(ucc_team) = &self.ucc_team {
            let (src, root_pe) = match src_or_root_pe {
                RootSrcSliceOrNone::Root(src) => (src, self.my_pe),
                RootSrcSliceOrNone::NotRoot(root_pe) => {
                    (unsafe { std::slice::from_raw_parts(res.as_ptr(), res.len()) }, root_pe)
                }
            };

            let req = ucc_team.scatter(src, res, root_pe)?;
            if blocking {
                self.wait_ucc_request(&req)?;
                Ok(None)
            } else {
                Ok(Some(req))
            }
        } else {
            panic!("UCC team not initialized for scatter");
        }
    }

    pub(crate) fn reduce_scatter_inner<T: Copy + 'static>(
        &self,
        op: &AllReduceOp,
        src: &[T],
        result: &mut [T],
        blocking: bool,
    ) -> Result<Option<UccRequest>, ucc::Error> {
        if let Some(ucc_team) = &self.ucc_team {
            let req = ucc_team.reduce_scatter(src, result, op.clone())?;
            if blocking {
                self.wait_ucc_request(&req)?;
                Ok(None)
            } else {
                Ok(Some(req))
            }
        } else {
            panic!("UCC team not initialized for reduce_scatter");
        }
    }

    pub(crate) fn wait_ucc_request(&self, req: &UccRequest) -> Result<(), ucc::Error> {
        if let Some(ucc_team) = &self.ucc_team {
            while let Err(err) = req.test() {
                if !matches!(err, Error::Inprogress) {
                    return Err(err);
                } 
                ucc_team.context.progress()?;
            }
            Ok(())
        }
        else {
            panic!("UCC team not initialized for waiting on UCC request");
        }
    }

    pub(crate) unsafe fn as_mut_slice<T>(&self) -> &mut [T] {
        self.mem.as_mut_slice()
    }

    pub(crate) unsafe fn as_slice<T>(&self) -> &[T] {
        self.mem.as_slice()
    }

    pub(crate) fn wait_ucc_all(&self) {
        if let Some(ucc_team) = &self.ucc_team {
            loop {
                ucc_team.context.progress().unwrap();
                let completed = ucc_team.req_completed.load(std::sync::atomic::Ordering::SeqCst);
                let pending = ucc_team.req_pending.load(std::sync::atomic::Ordering::SeqCst);
                if completed == pending {
                    break;
                }
                std::thread::yield_now();
            }
        }
        // else {
        //     panic!("UCC team not initialized for waiting on UCC requests");
        // }
    }

    pub(crate) fn wait_all(&self) {
        self.worker
            .wait_all()
            .expect("UcxAlloc::wait_all failed waiting on UCX requests");

        self.wait_ucc_all();
    }



    pub(crate) fn wait(&self) {
        self.worker
            .wait_all()
            .expect("UcxAlloc::wait failed waiting on UCX requests");
        self.wait_ucc_all();
    }
}

#[lamellar_prof::prof]
impl Drop for UcxAlloc {
    fn drop(&mut self) {
        trace!(target: "ucx", "drop UcxAlloc mem: {:x} - ({:x}) {:x} {:?}",
                self.mem.addr,
                self.mem.addr + self.data_num_bytes,
                self.mem.addr + self.mem.size,
                self);
        let fabric_ref_count = self.decrement_fabric_ref_count();
        match &self.alloc_table {
            AllocTable::Fabric(mem_handles, remote_keys) => {
                if fabric_ref_count == 3 {
                    debug!(target: "ucx", "Dropping UCX alloc: {:?}", self);
                    //last reference, remove from world tracking
                    mem_handles
                        .lock()
                        .unwrap()
                        .retain(|a| a.mem.inner.addr != self.mem.inner.addr);
                    remote_keys
                        .lock()
                        .unwrap()
                        .retain(|(a, _)| a.mem.inner.addr != self.mem.inner.addr);
                }
                if fabric_ref_count == 1 {
                    let mem_handles_count = Arc::strong_count(&mem_handles);
                    let mem_handle_count = Arc::strong_count(&self.mem.inner);
                    let remote_keys_count = Arc::strong_count(&remote_keys);
                    debug!("Dropping UCX alloc:  mem_handles_count: {}, mem_handle_count: {}, remote_keys_count: {}",
                    mem_handles_count, mem_handle_count, remote_keys_count);
                }
            }
            AllocTable::Runtime(rt_alloc_table, addr, mem_handles, remote_keys) => {
                let rt_ref_cnt = self.decrement_rt_ref_count();
                if rt_ref_cnt == 1 {
                    debug!(target: "ucx", "Dropping UCX rt-alloc: {:?}", self);
                    //last rt reference, free from alloc table
                    rt_alloc_table.free(*addr).expect(&format!(
                        "[{:?}] Error removing from runtime alloc table {:x}",
                        std::thread::current().id(),
                        addr
                    ));
                }
                if fabric_ref_count == 3 {
                    debug!(target: "ucx", "Dropping UCX alloc (from RT): {:?}", self);
                    //last reference, remove from world tracking
                    mem_handles
                        .lock()
                        .unwrap()
                        .retain(|a| a.mem.inner.addr != self.mem.inner.addr);
                    remote_keys
                        .lock()
                        .unwrap()
                        .retain(|(a, _)| a.mem.inner.addr != self.mem.inner.addr);
                }
                // if fabric_ref_count == 1 {
                //     let mem_handles_count = Arc::strong_count(&mem_handles);
                //     let mem_handle_count = Arc::strong_count(&self.mem.inner);
                //     let remote_keys_count = Arc::strong_count(&remote_keys);
                //     debug!("Dropping UCX alloc:  mem_handles_count: {}, mem_handle_count: {}, remote_keys_count: {}",
                //     mem_handles_count, mem_handle_count, remote_keys_count);
                // }
            }
        }
        trace!(target: "drop", "end drop UcxAlloc");
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OneSidedUcxAlloc {
    pub(crate) remote_pe: usize,
    pub(crate) alloc: UcxAlloc,
}

impl OneSidedUcxAlloc {
    pub(crate) fn num_bytes(&self) -> usize {
        self.alloc.num_bytes()
    }
    pub(crate) fn start(&self) -> usize {
        self.alloc.start()
    }
    pub(crate) fn sub_alloc(&self, offset: usize, size: usize) -> AllocResult<Self> {
        Ok(OneSidedUcxAlloc {
            remote_pe: self.remote_pe,
            alloc: self.alloc.sub_alloc(offset, size)?,
        })
    }
}

impl From<OneSidedUcxAlloc> for CommAlloc {
    fn from(alloc: OneSidedUcxAlloc) -> Self {
        CommAlloc {
            inner_alloc: Arc::new(CommAllocInner::OneSidedUcxAlloc(alloc)),
            // alloc_type: CommAllocType::Remote,
        }
    }
}

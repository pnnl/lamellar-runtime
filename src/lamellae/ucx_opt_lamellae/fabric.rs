mod context;
mod endpoint;
mod error;
mod memory_region;
mod worker;

use super::ucc::UccLib;
use crate::config;
use crate::lamellae::CollectiveOpKind;
use context::Context;
use endpoint::Endpoint;
pub(crate) use endpoint::UcxOptRequest;
use memory_region::{MemoryHandle, MemoryHandleInner, RKey, RemoteAddressInfo};
pub(crate) use worker::Worker;
use worker::FlushState;

#[cfg(feature = "enable-on-node-shmem")]
use crate::config;
use crate::{
    lamellae::{
        collective::{AllReduceOp, RootOrSliceMut, RootSrcOrSliceMut, RootSrcSliceOrNone},
        comm::alloc::*,
        ucx_opt_lamellae::ucc::{self, Error, UccContext, UccRequest, UccTeam},
        AllocError, AllocResult, AllocationType, AtomicOp, CommAlloc, CommAllocAddr,
        CommAllocInner, FabricError,
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
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::Poll,
};
use tracing::{debug, trace};

/// Worker index for a target PE. A plain `pe % shards` degenerates when PE ids have structure
/// (e.g. round-robin placement puts every remote PE at an odd id, so an even shard count would
/// leave half the workers idle); a multiplicative hash spreads them regardless of layout.
#[inline]
fn pe_shard(pe: usize, shards: usize) -> usize {
    (((pe as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 33) as usize) % shards
}

/// Upper bound on `LAMELLAR_UCX_WORKERS`; keeps `AllocFlushState` a small `Copy` type.
pub(crate) const MAX_SHARDS: usize = 16;

/// One UCX worker plus its endpoints to every PE's matching worker (`endpoints[pe]`).
#[derive(Clone)]
pub(crate) struct Shard {
    pub(crate) worker: Arc<Worker>,
    pub(crate) endpoints: Vec<Arc<Endpoint>>,
}

pub(crate) struct UcxOptBarrier {
    counter: AtomicUsize,
    sub_counter: AtomicU32,
    num_pes: usize,
    my_pe: usize,
    buffer: UcxOptAlloc,
    sub_buffer: UcxOptAlloc,
    sub_last_seen: Mutex<Vec<u32>>,
}

impl UcxOptBarrier {
    fn new(num_pes: usize, my_pe: usize, buffer: UcxOptAlloc) -> Self {
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
        UcxOptBarrier {
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

                barrier_alloc.progress_all();
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
                    barrier_alloc.progress_all();
                    std::thread::yield_now();
                }
            }

            trace!(target: "ucx", "PE {} completed round {}/{} of barrier", self.my_pe, round, num_rounds);
        }
        barrier_alloc.wait_all();
        trace!(target: "ucx","PE {} exiting barrier", self.my_pe);
    }
}

pub(crate) struct UcxOptWorld {
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
    /// Shards 1..K (empty with one worker); shard 0 is `worker`/`endpoints` above.
    extra_shards: Arc<Vec<Shard>>,
    mem_handles: Arc<Mutex<Vec<UcxOptAlloc>>>,
    remote_keys: Arc<Mutex<Vec<(UcxOptAlloc, Vec<Option<RemoteAddressInfo>>)>>>,
    ucc_context: Option<Arc<UccContext>>,
    ucc_world_team: Option<Arc<UccTeam>>,
    exchange_buffer: Option<UcxOptAlloc>,
    barrier: Option<Arc<Mutex<UcxOptBarrier>>>,
    ucc_world_buffer: Option<Arc<UcxOptAlloc>>,
    /// Guards `final_teardown` so it runs exactly once no matter who calls
    /// it: UcxOptComm::drop calls it explicitly (on a known-good thread), and
    /// Drop::drop below calls it too as a fallback. PMI barrier calls are
    /// not safe from an arbitrary OS thread, and Rust's implicit Arc-drop
    /// can run on any thread.
    teardown_done: AtomicBool,
}

#[cfg(feature = "enable-on-node-shmem")]
static UCX_SHMEM_ALLOC_ID: AtomicUsize = AtomicUsize::new(0);

impl std::fmt::Debug for UcxOptWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("UcxOptWorld")
            .field("my_pe", &self.my_pe)
            .field("num_pes", &self.num_pes)
            .finish()
    }
}

#[lamellar_prof::prof]
impl UcxOptWorld {
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
        let num_workers = config()
            .ucx_workers
            .unwrap_or_else(|| config().threads.clamp(4, MAX_SHARDS))
            .clamp(1, MAX_SHARDS);
        let workers = (0..num_workers)
            .map(|_| context.create_worker().unwrap())
            .collect::<Vec<_>>();
        let worker = workers[0].clone();

        let all_addresses = Worker::exchange_addresses(&workers, my_pmi.clone()).unwrap();
        let mut shards = workers
            .iter()
            .zip(all_addresses.iter())
            .map(|(w, addrs)| Shard {
                worker: w.clone(),
                endpoints: addrs
                    .iter()
                    .map(|a| Endpoint::new(w.clone(), a).unwrap())
                    .collect::<Vec<_>>(),
            })
            .collect::<Vec<_>>();
        drop(workers);
        let shard0 = shards.remove(0);
        let endpoints = shard0.endpoints;
        let extra_shards = Arc::new(shards);

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
            &extra_shards,
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

        Self::warmup_peer_puts(&worker, &extra_shards, &exchange_buffer, my_pe, num_pes);
        Self::warmup_peer_atomics(&worker, &extra_shards, &exchange_buffer, my_pe, num_pes);

        let barrier_buffer = Self::initial_alloc(
            false,
            &context,
            &endpoints,
            &extra_shards,
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

        let mut world = UcxOptWorld {
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
            extra_shards,
            mem_handles,
            remote_keys,
            ucc_context: None,
            ucc_world_team: None,
            exchange_buffer: Some(exchange_buffer),
            barrier: Some(Arc::new(Mutex::new(UcxOptBarrier::new(
                num_pes,
                my_pe,
                barrier_buffer,
            )))),
            ucc_world_buffer: None,
            teardown_done: AtomicBool::new(false),
        };
        my_pmi
            .barrier(false)
            .expect("Failed to perform barrier after initial allocations");

        let mut ucc_alloc = world.alloc(
            config().ucc_oob_init_buffer_size * world.num_pes,
            8,
            AllocationType::Global,
        );
        // UCC's OOB allgather puts data then a flag to the same peer and relies on their order,
        // so it must stay on a single endpoint.
        ucc_alloc.pinned = true;
        let alloc = Arc::new(ucc_alloc);
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
        let ucc_world_team = UccTeam::new(
            my_pe,
            &(0..num_pes).collect::<Vec<_>>(),
            ucc_context.clone(),
            alloc.clone(),
        )
        .unwrap();

        world.ucc_context = Some(ucc_context);
        world.ucc_world_team = Some(Arc::new(ucc_world_team));
        world
    }

    // Found this was necessary in the offchance that the first call to a intranode PE
    // happened simultaneously (in a MT environment) with other operations like progress or flush
    // if this becomes a bottleneck it may be sufficient to just do a put to each node instead of each PE, but for now we will do it to each PE to be safe
    // With K workers every shard is warmed separately (each has its own endpoints and lanes).
    fn warmup_flush_all(worker: &Arc<Worker>, extra: &Arc<Vec<Shard>>, shard: usize) {
        // Flush the shard being warmed while progressing EVERY shard: a peer that is still
        // warming shard j needs this PE to keep progressing shard j even after this PE has
        // moved on to a later shard.
        let target = if shard == 0 { worker } else { &extra[shard - 1].worker };
        let mut state = FlushState::default();
        loop {
            worker.try_progress(32);
            for s in extra.iter() {
                s.worker.try_progress(32);
            }
            match target.poll_wait_all(&mut state) {
                Poll::Ready(res) => {
                    res.expect("Failed final worker flush after UCX warm-up");
                    break;
                }
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    fn warmup_peer_puts(
        worker: &Arc<Worker>,
        extra: &Arc<Vec<Shard>>,
        buffer: &UcxOptAlloc,
        my_pe: usize,
        num_pes: usize,
    ) {
        for shard in 0..=extra.len() {
            for pe in 0..num_pes {
                if pe == my_pe {
                    continue;
                }
                unsafe {
                    buffer.put_inner_on(shard, pe, 0, std::slice::from_ref(&my_pe), false, false, false);
                }
            }
            Self::warmup_flush_all(worker, extra, shard);
        }
    }

    // Mirrors warmup_peer_puts above, but for the AMO path: UCX wires up a lane the first
    // time it's actually used, independent of whether other lanes on the same endpoint are
    // already wired up. warmup_peer_puts only ever forces PUT (RMA) wireup, so the first real
    // atomic op on an endpoint -- e.g. AtomicArray's "Write" path firing a batch of concurrent
    // atomic_swap calls from many worker threads during wait_all -- was still that same unsafe
    // "first call raced with concurrent progress/flush" scenario, just for the AMO lane instead.
    fn warmup_peer_atomics(
        worker: &Arc<Worker>,
        extra: &Arc<Vec<Shard>>,
        buffer: &UcxOptAlloc,
        my_pe: usize,
        num_pes: usize,
    ) {
        for shard in 0..=extra.len() {
            for pe in 0..num_pes {
                if pe == my_pe {
                    continue;
                }
                let mut op = AtomicOp::Write(Box::pin(my_pe));
                buffer.inner_atomic_op_on::<usize>(shard, pe, 0, false, &mut op, false);
            }
            Self::warmup_flush_all(worker, extra, shard);
        }
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

        let is_int = id == std::any::TypeId::of::<u8>()
            || id == std::any::TypeId::of::<u16>()
            || id == std::any::TypeId::of::<u32>()
            || id == std::any::TypeId::of::<u64>()
            || id == std::any::TypeId::of::<u128>()
            || id == std::any::TypeId::of::<i8>()
            || id == std::any::TypeId::of::<i16>()
            || id == std::any::TypeId::of::<i32>()
            || id == std::any::TypeId::of::<i64>()
            || id == std::any::TypeId::of::<i128>()
            || id == std::any::TypeId::of::<usize>()
            || id == std::any::TypeId::of::<isize>();
        // ucc.rs's rust_type_to_ucc_dtype maps f32/f64 to UCC_DT_FLOAT32/64, so the UCC binding
        // layer already supports them; only bitwise reduce ops are semantically invalid on floats.
        let is_float =
            id == std::any::TypeId::of::<f32>() || id == std::any::TypeId::of::<f64>();

        if !is_int && !is_float {
            return false;
        }

        let is_bitwise_reduce = matches!(
            op,
            CollectiveOpKind::AllReduce(AllReduceOp::BitOr)
                | CollectiveOpKind::AllReduce(AllReduceOp::BitXor)
                | CollectiveOpKind::AllReduce(AllReduceOp::BitAnd)
                | CollectiveOpKind::ReduceScatter(AllReduceOp::BitOr)
                | CollectiveOpKind::ReduceScatter(AllReduceOp::BitXor)
                | CollectiveOpKind::ReduceScatter(AllReduceOp::BitAnd)
                | CollectiveOpKind::Reduce(AllReduceOp::BitOr)
                | CollectiveOpKind::Reduce(AllReduceOp::BitXor)
                | CollectiveOpKind::Reduce(AllReduceOp::BitAnd)
        );

        // UCC's 128-bit reductions return wrong results (or/prod/and failed on every u128 case run), while
        // 128-bit data movement (gather, broadcast, alltoall) is correct. Reductions on 128-bit types
        // take the manual fallback instead.
        let is_128 = id == std::any::TypeId::of::<u128>() || id == std::any::TypeId::of::<i128>();
        let is_reduction = matches!(
            op,
            CollectiveOpKind::AllReduce(_)
                | CollectiveOpKind::Reduce(_)
                | CollectiveOpKind::ReduceScatter(_)
        );
        if is_128 && is_reduction {
            return false;
        }

        if is_bitwise_reduce {
            is_int
        } else {
            true
        }
    }
    fn initial_alloc(
        exchange_buffer: bool,
        context: &Arc<Context>,
        endpoints: &Vec<Arc<Endpoint>>,
        extra_shards: &Arc<Vec<Shard>>,
        worker: &Arc<Worker>,
        pmi: Arc<dyn Pmi>,
        num_pes: usize,
        my_pe: usize,
        #[cfg(feature = "enable-on-node-shmem")] same_node_pes: &Vec<bool>,
        #[cfg(feature = "enable-on-node-shmem")] disable_on_node_shmem: bool,
        #[cfg(feature = "enable-on-node-shmem")] job_id: usize,
        mem_handles: Arc<Mutex<Vec<UcxOptAlloc>>>,
        remote_keys: Arc<Mutex<Vec<(UcxOptAlloc, Vec<Option<RemoteAddressInfo>>)>>>,
    ) -> AllocResult<UcxOptAlloc> {
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
        let buffer_keys = mem_handle
            .exchange_key_pmi(endpoints, extra_shards, &pmi)
            .unwrap();

        let mem = MemoryHandle {
            addr: mem_handle.addr,
            size: size,
            inner: mem_handle.clone(),
        };
        mem.as_mut_slice::<u8>().fill(0);

        let alloc = UcxOptAlloc::new(
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
            extra_shards.clone(),
            true, // exchange/barrier buffers are ordering-critical: pinned to shard 0
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
    ) -> UcxOptAlloc {
        match alloc_type {
            AllocationType::Sub(pes) => self.sub_alloc(&pes, data_size, align),
            AllocationType::Global => self.full_alloc(data_size, align),
            _ => panic!("Unexpected allocation type: {:?}", alloc_type),
        }
    }

    pub(crate) fn full_alloc(&self, data_size: usize, align: usize) -> UcxOptAlloc {
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
                &self.extra_shards,
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

        let alloc = UcxOptAlloc::new(
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
            self.extra_shards.clone(),
            false,
            buffer_keys.clone(),
            self.mem_handles.clone(),
            self.remote_keys.clone(),
            self.ucc_world_team.clone(),
        )
        .expect("UcxOptAlloc::new failed");
        self.mem_handles.lock().unwrap().push(alloc.clone());
        self.remote_keys
            .lock()
            .unwrap()
            .push((alloc.clone(), buffer_keys));
        alloc
    }

    pub(crate) fn sub_alloc(&self, pes: &[usize], data_size: usize, align: usize) -> UcxOptAlloc {
        //add space for ref count and padding to align it
        let (padding, size, _align) = calc_alloc_padding_size_align(data_size, align);
        let mem_handle = MemoryHandleInner::alloc(&self.context, size);
        let buffer_keys_map = mem_handle
            .exchange_key_sub_alloc(
                &self.endpoints,
                &self.extra_shards,
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
                .map_err(|e| {
                    eprintln!(
                        "Failed to create UCC sub-team, falling back to manual collectives: {:?}",
                        e
                    )
                })
                .ok()
        });
        let alloc = UcxOptAlloc::new(
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
            self.extra_shards.clone(),
            false,
            buffer_keys_map.clone(),
            self.mem_handles.clone(),
            self.remote_keys.clone(),
            ucc_team.map(Arc::new),
        )
        .expect("UcxOptAlloc::new failed");
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
            .expect("UcxOptWorld::wait_all failed waiting on UCX requests");
        for shard in self.extra_shards.iter() {
            shard
                .worker
                .wait_all()
                .expect("UcxOptWorld::wait_all failed waiting on UCX requests");
        }
    }

    pub(crate) fn progress(&self) {
        // D5: single-shot, no retry loop needed since this caller doesn't block.
        self.worker.try_progress(32);
        for shard in self.extra_shards.iter() {
            shard.worker.try_progress(32);
        }

        // Only progress the UCC context when a collective is actually in
        // flight (D7/U8): otherwise every progress() call pays a UCC
        // progress cost for nothing. Safe because req_pending/req_completed
        // are only ever equal when there is truly no outstanding request,
        // matching the same check used by wait_ucc_all/wait_ucc above.
        if let Some(ucc_team) = &self.ucc_world_team {
            let pending = ucc_team.req_pending.load(Ordering::SeqCst);
            let completed = ucc_team.req_completed.load(Ordering::SeqCst);
            if pending != completed {
                ucc_team
                    .context
                    .progress()
                    .expect("Failed to progress UCC context");
            }
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
            if let Some(remote_info) = remote_addrs.get(remote_pe).and_then(|o| o.as_ref()) {
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
            if let Some(remote_info) = remote_addrs.get(remote_pe).and_then(|o| o.as_ref()) {
                let remote_pe_addr = remote_info.addr;
                if remote_pe_addr <= remote_addr
                    && remote_addr + num_bytes <= remote_pe_addr + alloc.data_num_bytes
                {
                    let offset = remote_addr - remote_pe_addr;
                    return OneSidedUcxOptAlloc {
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
            if let Some(remote_info) = remote_addrs.get(remote_pe).and_then(|o| o.as_ref()) {
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
                if let Some(remote_info) = remote_addrs.get(pe).and_then(|o| o.as_ref()) {
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
    ) -> Result<UcxOptAlloc, String> {
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
            for remote_info in rkeys.iter().flatten() {
                let ref_cnt = Arc::strong_count(&remote_info.rkeys);
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

impl UcxOptWorld {
    /// Explicit, idempotent teardown -- called directly from UcxOptComm::drop
    /// on its own (known-good) thread rather than left to fire implicitly
    /// whenever the last Arc<UcxOptWorld> clone happens to drop. PMI barrier
    /// calls are not safe from an arbitrary OS thread; see
    /// libfabric_sys_opt_lamellae for the confirmed hang this caused there.
    /// Drop::drop below calls this too as a fallback.
    ///
    /// Takes `&mut self`: the `ucc_*`/`exchange_buffer`/`barrier` fields are
    /// plain `Option<T>` (no interior mutability), so the caller must obtain
    /// exclusive access via `Arc::get_mut` (comm.rs spin-waits for
    /// `strong_count == 1` before calling this, same as `Drop::drop` itself
    /// naturally gets once the last `Arc<UcxOptWorld>` clone is gone).
    pub(crate) fn final_teardown(&mut self) {
        if self.teardown_done.swap(true, Ordering::SeqCst) {
            return;
        }
        trace!(target: "drop", "begin drop UcxOptWorld");
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
        trace!(target: "drop", "end drop UcxOptWorld");
    }
}

impl Drop for UcxOptWorld {
    fn drop(&mut self) {
        self.final_teardown();
    }
}

#[derive(Clone)]
enum AllocTable {
    Fabric(
        Arc<Mutex<Vec<UcxOptAlloc>>>,
        Arc<Mutex<Vec<(UcxOptAlloc, Vec<Option<RemoteAddressInfo>>)>>>,
    ),
    Runtime(
        BTreeAlloc,
        usize,
        Arc<Mutex<Vec<UcxOptAlloc>>>,
        Arc<Mutex<Vec<(UcxOptAlloc, Vec<Option<RemoteAddressInfo>>)>>>,
    ), //the usize is the offset of the rt_alloc so that we can free it properly if a sub_alloc is the last reference
}

#[derive(Debug, Clone, Default)]
pub(crate) struct AllocFlushState {
    worker: FlushState,
    /// Flush state for shards 1..K. Starts empty (no allocation) and is sized on first use, so a
    /// single-worker run pays nothing per future for the sharding support.
    extra: Vec<FlushState>,
}

pub(crate) struct UcxOptAlloc {
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
    /// Shards 1..K (empty with one worker). Data-path ops choose a shard per thread.
    extra_shards: Arc<Vec<Shard>>,
    /// Ordering-critical allocs (barrier, key exchange, UCC OOB) always use shard 0.
    pub(crate) pinned: bool,
    // Flat table indexed by (global) PE id instead of a HashMap: PE ids are dense
    // 0..num_pes, so this avoids a hash per RDMA/atomic op on the hot path (D6/U5).
    remote_keys: Arc<Vec<Option<RemoteAddressInfo>>>,
    alloc_table: AllocTable,
    ucc_team: Option<Arc<UccTeam>>,
}

#[lamellar_prof::prof]
impl Clone for UcxOptAlloc {
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
            extra_shards: self.extra_shards.clone(),
            pinned: self.pinned,
            remote_keys: self.remote_keys.clone(),
            alloc_table: self.alloc_table.clone(),
            ucc_team: self.ucc_team.clone(),
        }
    }
}

impl From<UcxOptAlloc> for CommAlloc {
    fn from(alloc: UcxOptAlloc) -> Self {
        CommAlloc {
            inner_alloc: Arc::new(CommAllocInner::UcxOptAlloc(alloc)),
            // alloc_type: CommAllocType::Fabric,
        }
    }
}

impl std::fmt::Debug for UcxOptAlloc {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let fabric_ref_count = unsafe {
            (&*(self.mem.inner.as_ptr().add(self.fabric_ref_cnt_offset) as *const AtomicUsize))
                .load(Ordering::SeqCst)
        };

        let mut temp = f.debug_struct("UcxOptAlloc");
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
impl UcxOptAlloc {
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
        extra_shards: Arc<Vec<Shard>>,
        pinned: bool,
        my_remote_keys: Vec<Option<RemoteAddressInfo>>,
        mem_handles: Arc<Mutex<Vec<UcxOptAlloc>>>,
        remote_keys: Arc<Mutex<Vec<(UcxOptAlloc, Vec<Option<RemoteAddressInfo>>)>>>,
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
            extra_shards,
            pinned,
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
            .map(|remote| {
                remote.as_ref().map(|remote| RemoteAddressInfo {
                    addr: remote.addr + offset,
                    rkeys: remote.rkeys.clone(),
                })
            })
            .collect::<Vec<Option<RemoteAddressInfo>>>();
        self.increment_fabric_ref_count();
        if let AllocTable::Runtime(_, _, _, _) = &self.alloc_table {
            self.increment_rt_ref_count();
        }
        let alloc = UcxOptAlloc {
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
            extra_shards: self.extra_shards.clone(),
            pinned: self.pinned,
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
            .map(|remote| {
                remote.as_ref().map(|remote| RemoteAddressInfo {
                    addr: remote.addr + offset,
                    rkeys: remote.rkeys.clone(),
                })
            })
            .collect::<Vec<Option<RemoteAddressInfo>>>();

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

        let alloc = UcxOptAlloc {
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
            extra_shards: self.extra_shards.clone(),
            pinned: self.pinned,
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
            extra_shards: self.extra_shards.clone(),
            pinned: self.pinned,
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

    /// Shard used for data-path ops to `pe`.
    ///
    /// Chosen by TARGET PE, not by issuing thread: the runtime's protocols (the command
    /// queue's per-peer header/ack slots, flag-after-data patterns) repeatedly write the
    /// same remote address from arbitrary threads and rely on those writes arriving in
    /// order. One endpoint per peer gives exactly that (RC delivery order), identical to a
    /// single worker, while traffic to different peers still spreads across workers.
    #[inline]
    fn shard_idx(&self, pe: usize) -> usize {
        let n = self.extra_shards.len();
        if n == 0 || self.pinned {
            0
        } else {
            pe_shard(pe, n + 1)
        }
    }

    /// Like `shard_idx`, but in the thread-sharded modes ("bulk" = gets and puts, "bulkget",
    /// "bulkput") transfers of at least `ucx_shard_bulk_bytes` are sharded by issuing thread
    /// instead of target PE. Large transfers are where one peer's traffic can usefully spread
    /// over several workers; the small writes that runtime protocols order (headers, acks,
    /// flags) stay on the per-peer endpoint.
    #[inline]
    fn shard_idx_sized(&self, pe: usize, bytes: usize, is_get: bool) -> usize {
        let n = self.extra_shards.len();
        if n == 0 || self.pinned {
            return 0;
        }
        // (shard gets by thread, shard puts by thread, size threshold)
        static CFG: std::sync::OnceLock<(bool, bool, usize)> = std::sync::OnceLock::new();
        let (gets, puts, thresh) = *CFG.get_or_init(|| {
            let mode = config().ucx_shard_mode.as_str();
            (
                mode == "bulk" || mode == "bulkget",
                mode == "bulk" || mode == "bulkput",
                config().ucx_shard_bulk_bytes,
            )
        });
        if bytes >= thresh && ((is_get && gets) || (!is_get && puts)) {
            crate::LAMELLAR_THREAD_ID.with(|id| *id) % (n + 1)
        } else {
            pe_shard(pe, n + 1)
        }
    }

    #[inline]
    fn ep(&self, shard: usize, pe: usize) -> &Arc<Endpoint> {
        if shard == 0 {
            &self.endpoints[pe]
        } else {
            &self.extra_shards[shard - 1].endpoints[pe]
        }
    }

    /// Progress every shard's worker (raw, unguarded), for spin loops that must keep all
    /// of this PE's workers serviceable for peers.
    pub(crate) fn progress_all(&self) {
        self.worker.progress();
        for s in self.extra_shards.iter() {
            s.worker.progress();
        }
    }

    pub(crate) unsafe fn put_inner<T: Copy>(
        &self,
        pe: usize,
        offset: usize, //with respect to T
        src_addr: &[T],
        blocking: bool,
        managed: bool,
    ) -> Option<UcxOptRequest> {
        self.put_inner_on(
            self.shard_idx_sized(pe, src_addr.len() * std::mem::size_of::<T>(), false),
            pe,
            offset,
            src_addr,
            blocking,
            managed,
            false,
        )
    }

    /// Unmanaged put whose source may be registered memory. A put of at least
    /// [`endpoint::UNMANAGED_NOWAIT_MIN_BYTES`] from a registered source returns without waiting for local
    /// completion: the source must then stay valid until the next `wait_all`, which drains the put.
    pub(crate) unsafe fn put_inner_unmanaged<T: Copy>(
        &self,
        pe: usize,
        offset: usize,
        src_addr: &[T],
        registered: bool,
    ) -> Option<UcxOptRequest> {
        let bytes = std::mem::size_of_val(src_addr);
        let nowait = registered && bytes >= endpoint::UNMANAGED_NOWAIT_MIN_BYTES;
        self.put_inner_on(
            self.shard_idx_sized(pe, bytes, false),
            pe,
            offset,
            src_addr,
            false,
            false,
            nowait,
        )
    }

    pub(crate) unsafe fn put_inner_on<T: Copy>(
        &self,
        shard: usize,
        pe: usize,
        offset: usize, //with respect to T
        src_addr: &[T],
        blocking: bool,
        managed: bool,
        nowait: bool,
    ) -> Option<UcxOptRequest> {
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
        let (remote_addr, rkey) = if let Some(remote_info) = self.remote_keys.get(pe).and_then(|o| o.as_ref()) {
            trace!(target: "ucx", "Found remote key for pe {}: addr {:x} rkey {:?}", pe, remote_info.addr, remote_info.rkeys[0]);
            (remote_info.addr, remote_info.rkey_for(shard))
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
        if nowait {
            self.ep(shard, pe).put_nowait(
                src_addr.as_ptr() as _,
                src_addr.len() * std::mem::size_of::<T>(),
                remote_addr + offset,
                rkey,
            );
            return None;
        }
        let req = self.ep(shard, pe).put(
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

    /// The part of a get that doesn't depend on how its completion is tracked: picks the shard,
    /// bounds-checks, satisfies local and same-node targets with a copy, and otherwise returns the
    /// shard, remote address and rkey for the remote get. `None` means the get is already done.
    unsafe fn prepare_get<T: Copy>(
        &self,
        pe: usize,
        offset: usize,
        dst_addr: &mut [T],
    ) -> Option<(usize, usize, &RKey)> {
        let shard = self.shard_idx_sized(pe, dst_addr.len() * std::mem::size_of::<T>(), true);
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
        let (remote_addr, rkey) = if let Some(remote_info) = self.remote_keys.get(pe).and_then(|o| o.as_ref()) {
            (remote_info.addr, remote_info.rkey_for(shard))
        } else {
            panic!("inner_get missing remote key for pe {}", pe);
        };
        Some((shard, remote_addr + offset, rkey))
    }

    pub(crate) unsafe fn inner_get<T: Copy>(
        &self,
        pe: usize,
        offset: usize,
        blocking: bool,
        dst_addr: &mut [T],
    ) -> Option<UcxOptRequest> {
        let (shard, remote_addr, rkey) = self.prepare_get(pe, offset, dst_addr)?;
        let req = self.ep(shard, pe).get(
            dst_addr.as_mut_ptr() as _,
            dst_addr.len() * std::mem::size_of::<T>(),
            remote_addr,
            rkey,
        );
        if blocking {
            req.wait().expect("blocking_get failed");
            None
        } else {
            Some(req)
        }
    }

    /// Fire-and-forget get for the unmanaged API: completion is observed only through `wait_all`.
    pub(crate) unsafe fn inner_get_unmanaged<T: Copy>(&self, pe: usize, offset: usize, dst_addr: &mut [T]) {
        if let Some((shard, remote_addr, rkey)) = self.prepare_get(pe, offset, dst_addr) {
            self.ep(shard, pe)
                .get_untracked(
                    dst_addr.as_mut_ptr() as _,
                    dst_addr.len() * std::mem::size_of::<T>(),
                    remote_addr,
                    rkey,
                )
                .expect("get_into_buffer_unmanaged failed");
        }
    }

    pub(crate) fn inner_atomic_op<T: Copy + 'static>(
        &self,
        pe: usize,
        offset: usize,
        blocking: bool,
        op: &mut AtomicOp<T>,
        managed: bool,
    ) -> Option<UcxOptRequest> {
        self.inner_atomic_op_on(self.shard_idx(pe), pe, offset, blocking, op, managed)
    }

    pub(crate) fn inner_atomic_op_on<T: Copy + 'static>(
        &self,
        shard: usize,
        pe: usize,
        offset: usize,
        blocking: bool,
        op: &mut AtomicOp<T>,
        managed: bool,
    ) -> Option<UcxOptRequest> {
        let offset = offset * std::mem::size_of::<T>();
        debug_assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let (remote_addr, rkey) = if let Some(remote_info) = self.remote_keys.get(pe).and_then(|o| o.as_ref()) {
            (remote_info.addr, remote_info.rkey_for(shard))
        } else {
            panic!("inner_atomic_op missing remote key for pe {}", pe);
        };
        let req = match op {
            AtomicOp::Write(val) => self.ep(shard, pe).atomic_swap(
                val.as_ref().get_ref(),
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
                self.ep(shard, pe).atomic_op(ucx_op, val, remote_addr + offset, rkey, managed)
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
            req.expect("blocking atomic_op always managed")
                .wait()
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
    ) -> Option<UcxOptRequest> {
        let shard = self.shard_idx(pe);
        let offset = offset * std::mem::size_of::<T>();
        debug_assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let req = match op {
            AtomicOp::Read(zero) => {
                let (remote_addr, rkey) = if let Some(remote_info) = self.remote_keys.get(pe).and_then(|o| o.as_ref()) {
                    (remote_info.addr, remote_info.rkey_for(shard))
                } else {
                    panic!(
                        "inner_atomic_fetch_op missing remote key for pe {} (read)",
                        pe
                    );
                };
                self.ep(shard, pe).atomic_get(
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
                let (remote_addr, rkey) = if let Some(remote_info) = self.remote_keys.get(pe).and_then(|o| o.as_ref()) {
                    (remote_info.addr, remote_info.rkey_for(shard))
                } else {
                    panic!("inner_atomic_fetch_op missing remote key for pe {}", pe);
                };
                self.ep(shard, pe).atomic_fetch_op(
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
    ) -> Option<UcxOptRequest> {
        let shard = self.shard_idx(pe);
        let offset = offset * std::mem::size_of::<T>();
        debug_assert!(offset + std::mem::size_of::<T>() <= self.num_bytes());
        let (remote_addr, rkey) = if let Some(remote_info) = self.remote_keys.get(pe).and_then(|o| o.as_ref()) {
            (remote_info.addr, remote_info.rkey_for(shard))
        } else {
            panic!(
                "inner_atomic_compare_exchange_op missing remote key for pe {}",
                pe
            );
        };
        // result[0] must be pre-initialized to `new` (Z) before calling;
        // after completion it holds the original remote value (old Y).
        let req = self.ep(shard, pe).atomic_compare_swap(
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
            let req = ucc_team.allgather(src, result)?;

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
            let req = ucc_team.allreduce(src, result, op.clone())?;
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
        let src =
            unsafe { std::slice::from_raw_parts(src_and_result.as_ptr(), src_and_result.len()) };
        self.allreduce_inner(op, src, src_and_result, blocking)
    }

    pub(crate) fn alltoall_inner<T: Copy + 'static>(
        &self,
        src: &[T],
        result: &mut [T],
        blocking: bool,
    ) -> Result<Option<UccRequest>, ucc::Error> {
        if let Some(ucc_team) = &self.ucc_team {
            let req = ucc_team.allgather(src, result)?;
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
                RootSrcOrSliceMut::Root(src) => (
                    unsafe { std::slice::from_raw_parts_mut(src.as_ptr() as *mut T, src.len()) },
                    self.my_pe,
                ),
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
                RootSrcSliceOrNone::NotRoot(root_pe) => (
                    unsafe { std::slice::from_raw_parts(res.as_ptr(), res.len()) },
                    root_pe,
                ),
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
        } else {
            panic!("UCC team not initialized for waiting on UCC request");
        }
    }

    /// Non-blocking, single-shot-per-call version of `wait_ucc_request`. Caller
    /// must keep polling (e.g. via a waker) until this returns `Poll::Ready`.
    pub(crate) fn poll_wait_ucc_request(&self, req: &UccRequest) -> Poll<Result<(), ucc::Error>> {
        if let Some(ucc_team) = &self.ucc_team {
            match req.test() {
                Ok(()) => Poll::Ready(Ok(())),
                Err(Error::Inprogress) => {
                    let _ = ucc_team.context.progress();
                    Poll::Pending
                }
                Err(e) => Poll::Ready(Err(e)),
            }
        } else {
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
                let completed = ucc_team
                    .req_completed
                    .load(std::sync::atomic::Ordering::SeqCst);
                let pending = ucc_team
                    .req_pending
                    .load(std::sync::atomic::Ordering::SeqCst);
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
            .expect("UcxOptAlloc::wait_all failed waiting on UCX requests");
        for s in self.extra_shards.iter() {
            s.worker
                .wait_all()
                .expect("UcxOptAlloc::wait_all failed waiting on UCX requests");
        }

        self.wait_ucc_all();
    }

    /// Non-blocking, single-shot-per-call version of `wait_all`. Caller owns
    /// `state` across polls and must keep polling (e.g. via a waker) until
    /// this returns `Poll::Ready`.
    pub(crate) fn poll_wait_all(
        &self,
        state: &mut AllocFlushState,
    ) -> Poll<Result<(), error::Error>> {
        let mut pending = false;
        if state.extra.len() != self.extra_shards.len() {
            state.extra.resize(self.extra_shards.len(), FlushState::default());
        }
        match self.worker.poll_wait_all(&mut state.worker) {
            Poll::Pending => pending = true,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }
        // Keep polling every shard even when an earlier one is pending, so all K flushes
        // make progress together instead of one after another.
        for (i, s) in self.extra_shards.iter().enumerate() {
            match s.worker.poll_wait_all(&mut state.extra[i]) {
                Poll::Pending => pending = true,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {}
            }
        }
        if pending {
            return Poll::Pending;
        }
        if let Some(ucc_team) = &self.ucc_team {
            ucc_team.context.progress().unwrap();
            let completed = ucc_team.req_completed.load(Ordering::SeqCst);
            let pending = ucc_team.req_pending.load(Ordering::SeqCst);
            if completed != pending {
                return Poll::Pending;
            }
        }
        Poll::Ready(Ok(()))
    }

    pub(crate) fn wait(&self) {
        self.worker
            .wait_all()
            .expect("UcxOptAlloc::wait failed waiting on UCX requests");
        for s in self.extra_shards.iter() {
            s.worker
                .wait_all()
                .expect("UcxOptAlloc::wait failed waiting on UCX requests");
        }
        self.wait_ucc_all();
    }
}

#[lamellar_prof::prof]
impl Drop for UcxOptAlloc {
    fn drop(&mut self) {
        trace!(target: "ucx", "drop UcxOptAlloc mem: {:x} - ({:x}) {:x} {:?}",
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
        trace!(target: "drop", "end drop UcxOptAlloc");
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OneSidedUcxOptAlloc {
    pub(crate) remote_pe: usize,
    pub(crate) alloc: UcxOptAlloc,
}

impl OneSidedUcxOptAlloc {
    pub(crate) fn num_bytes(&self) -> usize {
        self.alloc.num_bytes()
    }
    pub(crate) fn start(&self) -> usize {
        self.alloc.start()
    }
    pub(crate) fn sub_alloc(&self, offset: usize, size: usize) -> AllocResult<Self> {
        Ok(OneSidedUcxOptAlloc {
            remote_pe: self.remote_pe,
            alloc: self.alloc.sub_alloc(offset, size)?,
        })
    }
}

impl From<OneSidedUcxOptAlloc> for CommAlloc {
    fn from(alloc: OneSidedUcxOptAlloc) -> Self {
        CommAlloc {
            inner_alloc: Arc::new(CommAllocInner::OneSidedUcxOptAlloc(alloc)),
            // alloc_type: CommAllocType::Remote,
        }
    }
}

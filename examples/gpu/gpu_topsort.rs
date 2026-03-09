//! GPU Topological Sort benchmark — distributed Kahn's algorithm on GPUs.
//!
//! # Algorithm (Kahn's BFS topological sort)
//!
//! Each PE owns `local_n` vertices in the global space
//! `[my_pe * local_n, (my_pe + 1) * local_n)`.
//!
//! A `DistGpuArray<u32>` per PE stores the **in-degree** of each local vertex.
//!
//! Each BFS wave:
//!
//! 1. **`topsort_find_frontier` kernel** — scans in-degrees, marks every
//!    zero-degree vertex `PROCESSED_MARKER`, appends its global ID to
//!    `FRONTIER_OUT` (host static for stub; device→host copy for CUDA).
//! 2. For each frontier vertex, fan out its out-edges:
//!    - **local** successor → `PENDING_DECREMENTS`
//!    - **remote** successor → `DecrementAm` to target PE
//! 3. `wait_all()` + `barrier()` so all remote decrements have arrived.
//! 4. **`topsort_apply_decrements` kernel** — drains `PENDING_DECREMENTS`,
//!    uses `atomicSub` on in-degrees.
//! 5. `barrier()`, then repeat until no more frontier vertices.
//!
//! # CUDA vs stub
//!
//! * **Stub** (no cuda feature): `find_frontier` and `apply_decrements` run
//!   sequentially on the host using `DeviceBuffer<u32>` backed by `Vec<u32>`.
//! * **CUDA** (`--features cuda`): real `atomicAdd`/`atomicSub` kernels in
//!   `cuda/topsort.cu`.  The design avoids changing `FfiKernelFn<T>` by
//!   uploading extra data (frontier buffer, decrement list) into CUDA
//!   device-side globals via small C helper functions before each dispatch.
//!
//! # Running
//! ```sh
//! # Stub:
//! cargo run --example gpu_topsort -- 8192 4
//! # CUDA:
//! cargo run --features cuda --example gpu_topsort -- 8192 4
//! # args: <vertices_per_pe> <edges_per_vertex>
//! ```
use std::sync::Mutex;
use std::time::Instant;

use lamellar::ActiveMessaging;
use lamellar::LamellarEnv;
use lamellar::gpu::{
    array::{next_array_id, register_kernel, DistGpuArray, GpuArrayEntry},
    kernel::{FfiCudaKernel, LaunchConfig},
};

// ── Constants ─────────────────────────────────────────────────────────────────

const PROCESSED_MARKER: u32 = u32::MAX;

// ── Process-local queues ──────────────────────────────────────────────────────

static FRONTIER_OUT: std::sync::LazyLock<Mutex<Vec<u64>>> =
    std::sync::LazyLock::new(|| Mutex::new(Vec::new()));

static PENDING_DECREMENTS: std::sync::LazyLock<Mutex<Vec<u64>>> =
    std::sync::LazyLock::new(|| Mutex::new(Vec::new()));

fn take_frontier()   -> Vec<u64> { std::mem::take(&mut *FRONTIER_OUT.lock().unwrap()) }
fn take_decrements() -> Vec<u64> { std::mem::take(&mut *PENDING_DECREMENTS.lock().unwrap()) }
fn push_decrements(v: &[u64])    { PENDING_DECREMENTS.lock().unwrap().extend_from_slice(v); }

// ── CUDA FFI declarations ─────────────────────────────────────────────────────

#[cfg(feature = "cuda")]
extern "C" {
    fn topsort_set_init_data(host_ptr: *const u32, n: usize) -> i32;
    fn topsort_set_base_offset(base: u64) -> i32;
    fn topsort_reset_frontier(dev_ptr: *mut u64, cap: usize) -> i32;
    fn topsort_get_frontier_count(out: *mut u32) -> i32;
    fn topsort_set_decrements(dev_ptr: *mut u64, n: usize) -> i32;

    fn topsort_init_ffi(
        ptr: *mut u32, len: usize,
        gx: u32, gy: u32, gz: u32,
        bx: u32, by: u32, bz: u32,
        shared: u32, stream: *mut core::ffi::c_void,
    ) -> i32;
    fn topsort_find_frontier_ffi(
        ptr: *mut u32, len: usize,
        gx: u32, gy: u32, gz: u32,
        bx: u32, by: u32, bz: u32,
        shared: u32, stream: *mut core::ffi::c_void,
    ) -> i32;
    fn topsort_apply_decrements_ffi(
        ptr: *mut u32, len: usize,
        gx: u32, gy: u32, gz: u32,
        bx: u32, by: u32, bz: u32,
        shared: u32, stream: *mut core::ffi::c_void,
    ) -> i32;
}

// ── CUDA device buffer statics (per-PE process) ───────────────────────────────

/// Device buffer for frontier output — pre-allocated once, reused each wave.
#[cfg(feature = "cuda")]
static FRONTIER_DEV_BUF: std::sync::LazyLock<Mutex<Option<cust::memory::DeviceBuffer<u64>>>> =
    std::sync::LazyLock::new(|| Mutex::new(None));

/// Device buffer for decrement indices — reallocated each wave.
#[cfg(feature = "cuda")]
static DECREMENTS_DEV_BUF: std::sync::LazyLock<Mutex<Option<cust::memory::DeviceBuffer<u64>>>> =
    std::sync::LazyLock::new(|| Mutex::new(None));

// ── Stub kernels (host sequential, no CUDA feature) ───────────────────────────

/// Init kernel — copies `INIT_DATA` thread-local into the (host-backed) buffer.
#[cfg(not(feature = "cuda"))]
unsafe extern "C" fn topsort_init_stub(
    ptr: *mut u32, len: usize,
    _gx: u32, _gy: u32, _gz: u32,
    _bx: u32, _by: u32, _bz: u32,
    _shared: u32, _stream: *mut core::ffi::c_void,
) -> i32 {
    INIT_DATA.with(|data| {
        let data = data.borrow();
        let n = data.len().min(len);
        std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, n);
    });
    0
}

/// Find-frontier stub — scans host buffer, emits zero-degree entries.
#[cfg(not(feature = "cuda"))]
unsafe extern "C" fn topsort_find_frontier_stub(
    ptr: *mut u32, len: usize,
    _gx: u32, _gy: u32, _gz: u32,
    _bx: u32, _by: u32, _bz: u32,
    _shared: u32, _stream: *mut core::ffi::c_void,
) -> i32 {
    let in_degrees = std::slice::from_raw_parts_mut(ptr, len);
    let base = KERNEL_BASE_OFFSET.with(|c| c.get());
    let mut out = FRONTIER_OUT.lock().unwrap();
    for (i, deg) in in_degrees.iter_mut().enumerate() {
        if *deg == 0 {
            *deg = PROCESSED_MARKER;
            out.push(base + i as u64);
        }
    }
    0
}

/// Apply-decrements stub — drains `PENDING_DECREMENTS`.
#[cfg(not(feature = "cuda"))]
unsafe extern "C" fn topsort_apply_decrements_stub(
    ptr: *mut u32, len: usize,
    _gx: u32, _gy: u32, _gz: u32,
    _bx: u32, _by: u32, _bz: u32,
    _shared: u32, _stream: *mut core::ffi::c_void,
) -> i32 {
    let in_degrees = std::slice::from_raw_parts_mut(ptr, len);
    for local_idx in take_decrements() {
        let i = local_idx as usize;
        if i < len && in_degrees[i] != PROCESSED_MARKER {
            in_degrees[i] = in_degrees[i].saturating_sub(1);
        }
    }
    0
}

// ── Thread-locals for stub path ───────────────────────────────────────────────

thread_local! {
    /// Global vertex base written by FindFrontierAm before the stub kernel runs.
    static KERNEL_BASE_OFFSET: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// In-degree data for the init stub.
    static INIT_DATA: std::cell::RefCell<Vec<u32>> = const { std::cell::RefCell::new(Vec::new()) };
}

fn set_base_offset(offset: u64) {
    KERNEL_BASE_OFFSET.with(|c| c.set(offset));
}

// ── Active Messages ───────────────────────────────────────────────────────────

/// Sent by a PE to decrement a remote vertex's in-degree.
#[lamellar::AmData(Debug, Clone)]
struct DecrementAm {
    local_vertex: u64,
}

#[lamellar::am]
impl LamellarAM for DecrementAm {
    async fn exec(self) {
        push_decrements(&[self.local_vertex]);
    }
}

/// Runs the find-frontier kernel on a single PE with the correct base offset.
///
/// We dispatch this per-PE (rather than using exec_kernel_all) so we can set
/// both the thread-local (stub path) and the CUDA device globals (CUDA path)
/// immediately before the kernel fires, on the correct PE's worker thread.
#[lamellar::AmData(Debug, Clone)]
struct FindFrontierAm {
    array_id: u64,
    base_offset: u64,
    config: LaunchConfig,
}

#[lamellar::am]
impl LamellarAM for FindFrontierAm {
    async fn exec(self) -> lamellar::gpu::array::GpuKernelResult {
        use lamellar::gpu::array::{local::get_array_entry, GpuKernelResult};
        let pe = lamellar::current_pe;

        let result: Result<(), String> = async {
            let entry = get_array_entry(self.array_id).map_err(|e| format!("{e}"))?;

            // ── Stub path: set thread-local base offset ───────────────────────
            #[cfg(not(feature = "cuda"))]
            set_base_offset(self.base_offset);

            // ── CUDA path: set device globals before kernel dispatch ───────────
            #[cfg(feature = "cuda")]
            unsafe {
                // (1) Base offset for emitting global vertex IDs.
                let rc = topsort_set_base_offset(self.base_offset);
                if rc != 0 { return Err(format!("topsort_set_base_offset cuda err {rc}")); }

                // (2) Reset frontier counter and set output buffer.
                let guard = FRONTIER_DEV_BUF.lock().unwrap();
                let buf = guard.as_ref()
                    .ok_or("frontier device buffer not allocated")?;
                let rc = topsort_reset_frontier(
                    buf.as_device_ptr().as_mut_ptr(),
                    buf.len(),
                );
                if rc != 0 { return Err(format!("topsort_reset_frontier cuda err {rc}")); }
            }

            // ── Run kernel (launch only — no sync inside the AM) ─────────────
            // Calling stream.synchronize() inside an async AM blocks the executor
            // thread and can deadlock .block() on the caller.  The sync and the
            // CUDA frontier readback are done in main() after all FindFrontierAm
            // tasks have returned (via sync_local + explicit CUDA calls there).
            entry.write()
                .launch_kernel("topsort_find_frontier", &self.config)
                .map_err(|e| format!("{e}"))?;

            // ── Stub path: sync is a no-op, so read back here ────────────────
            // For the stub (no-CUDA), entry.read().sync() is instant (no GPU),
            // and the frontier was written directly by the stub kernel above.
            #[cfg(not(feature = "cuda"))]
            {
                entry.read().sync().map_err(|e| format!("{e}"))?;
            }

            Ok(())
        }
        .await;

        match result {
            Ok(()) => GpuKernelResult::ok(pe),
            Err(msg) => GpuKernelResult::err(pe, msg),
        }
    }
}

// ── Graph representation ──────────────────────────────────────────────────────

struct LocalGraph {
    out_edges: Vec<Vec<(usize, u64)>>,
    local_n: usize,
}

impl LocalGraph {
    fn random(
        my_pe: usize,
        num_pes: usize,
        local_n: usize,
        edges_per_vertex: usize,
        rng: &mut impl rand::Rng,
    ) -> Self {
        use rand::Rng;
        let global_n = local_n * num_pes;
        let mut out_edges = vec![Vec::new(); local_n];
        for local_v in 0..local_n {
            let global_v = my_pe * local_n + local_v;
            if global_v + 1 >= global_n { continue; }
            let mut targets = std::collections::BTreeSet::new();
            for _ in 0..(edges_per_vertex * 4) {
                if targets.len() >= edges_per_vertex { break; }
                let t: u64 = rng.gen_range((global_v + 1) as u64..global_n as u64);
                targets.insert(t);
            }
            out_edges[local_v] = targets
                .into_iter()
                .map(|t| (t as usize / local_n, t % local_n as u64))
                .collect();
        }
        Self { out_edges, local_n }
    }

    /// Replay all PEs' random graphs to compute in-degrees for `my_pe`.
    fn compute_in_degrees(
        my_pe: usize,
        num_pes: usize,
        local_n: usize,
        edges_per_vertex: usize,
    ) -> Vec<u32> {
        use rand::{Rng, SeedableRng};
        let global_n = local_n * num_pes;
        let mut in_deg = vec![0u32; local_n];
        for src_pe in 0..num_pes {
            let mut rng: rand::rngs::StdRng = rand::rngs::StdRng::seed_from_u64(src_pe as u64 + 42);
            for local_src in 0..local_n {
                let global_src = src_pe * local_n + local_src;
                if global_src + 1 >= global_n { continue; }
                let mut targets = std::collections::BTreeSet::new();
                for _ in 0..(edges_per_vertex * 4) {
                    if targets.len() >= edges_per_vertex { break; }
                    let t: u64 = rng.gen_range((global_src + 1) as u64..global_n as u64);
                    targets.insert(t);
                }
                for t in targets {
                    let tpe = t as usize / local_n;
                    let tli = (t % local_n as u64) as usize;
                    if tpe == my_pe { in_deg[tli] += 1; }
                }
            }
        }
        in_deg
    }
}

// ── main ──────────────────────────────────────────────────────────────────────

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let local_n: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(8192);
    let edges_per_vertex: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(4);

    // ── CUDA initialisation ──────────────────────────────────────────────────
    // init_cuda_context stores the context globally so Lamellar worker
    // threads can push it before running CUDA operations in AM exec bodies.
    #[cfg(feature = "cuda")]
    let _cuda_ctx = lamellar::gpu::init_cuda_context(
        cust::quick_init().expect("CUDA initialisation failed"),
    );

    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let global_n = local_n * num_pes;
    let backend = if cfg!(feature = "cuda") { "CUDA" } else { "stub" };

    world.barrier();
    println!(
        "PE {my_pe}/{num_pes}: topsort [{backend}] — {global_n} vertices \
         ({local_n}/PE), {edges_per_vertex} edges/vertex"
    );

    // ── 1. Register kernels ───────────────────────────────────────────────────
    #[cfg(feature = "cuda")]
    {
        register_kernel(FfiCudaKernel::<u32>::new("topsort_init", topsort_init_ffi));
        register_kernel(FfiCudaKernel::<u32>::new("topsort_find_frontier", topsort_find_frontier_ffi));
        register_kernel(FfiCudaKernel::<u32>::new("topsort_apply_decrements", topsort_apply_decrements_ffi));
    }
    #[cfg(not(feature = "cuda"))]
    {
        register_kernel(FfiCudaKernel::<u32>::new("topsort_init", topsort_init_stub));
        register_kernel(FfiCudaKernel::<u32>::new("topsort_find_frontier", topsort_find_frontier_stub));
        register_kernel(FfiCudaKernel::<u32>::new("topsort_apply_decrements", topsort_apply_decrements_stub));
    }

    // ── 2. Build local graph ──────────────────────────────────────────────────
    use rand::SeedableRng;
    let mut rng: rand::rngs::StdRng = rand::rngs::StdRng::seed_from_u64(my_pe as u64 + 42);
    let graph = LocalGraph::random(my_pe, num_pes, local_n, edges_per_vertex, &mut rng);

    // ── 3. Compute in-degrees ────────────────────────────────────────────────
    let in_degrees = LocalGraph::compute_in_degrees(my_pe, num_pes, local_n, edges_per_vertex);

    // ── 4. Allocate DistGpuArray and initialise in-degrees on device ─────────
    let array_id = next_array_id();
    let gpu_array = DistGpuArray::<u32>::new(&world, array_id, local_n, 0)
        .expect("DistGpuArray::new failed");

    // ── CUDA: pre-allocate frontier and decrement device buffers ─────────────
    #[cfg(feature = "cuda")]
    {
        use cust::memory::DeviceBuffer;
        // Frontier: worst case all vertices in one wave.
        let f_buf: DeviceBuffer<u64> = DeviceBuffer::zeroed(local_n)
            .expect("frontier DeviceBuffer alloc");
        *FRONTIER_DEV_BUF.lock().unwrap() = Some(f_buf);
    }

    // ── Stub: set INIT_DATA thread-local (used by topsort_init_stub) ─────────
    #[cfg(not(feature = "cuda"))]
    INIT_DATA.with(|c| *c.borrow_mut() = in_degrees.clone());

    // ── CUDA: call topsort_set_init_data so the CUDA init kernel can do H→D ──
    #[cfg(feature = "cuda")]
    unsafe {
        let rc = topsort_set_init_data(in_degrees.as_ptr(), in_degrees.len());
        assert_eq!(rc, 0, "topsort_set_init_data failed");
    }

    // Run the init kernel (copies in-degrees to device, stub or CUDA).
    {
        use lamellar::gpu::array::local::get_array_entry;
        let entry = get_array_entry(array_id).expect("array entry not found");
        let mut guard = entry.write();
        guard
            .launch_kernel("topsort_init", &LaunchConfig::simple_1d(local_n as u32, 256))
            .expect("topsort_init failed");
        guard.sync().expect("stream sync failed");
    }

    world.barrier();
    println!("PE {my_pe}: in-degrees ready — starting BFS waves");
    let now = Instant::now();

    // ── 5. BFS wave loop ──────────────────────────────────────────────────────
    let base_offset = (my_pe * local_n) as u64;
    let vertex_config = LaunchConfig::simple_1d(local_n as u32, 256);
    let mut wave = 0usize;
    let mut total_processed = 0usize;

    loop {
        // ── Wave A: find frontier on all PEs ──────────────────────────────────
        let frontier_tasks: Vec<_> = (0..num_pes)
            .map(|pe| {
                world.exec_am_pe(
                    pe,
                    FindFrontierAm {
                        array_id,
                        base_offset: (pe * local_n) as u64,
                        config: vertex_config.clone(),
                    },
                )
            })
            .collect();

        // Block on all FindFrontierAm results.
        let frontier_results: Vec<_> = frontier_tasks.into_iter().map(|t| t.block()).collect();

        // CUDA: sync the local stream (kernel launch is async; must wait for GPU).
        // Then read back the frontier count and data from device to host.
        #[cfg(feature = "cuda")]
        {
            gpu_array.sync_local().expect("GPU stream sync (find_frontier) failed");
            unsafe {
                let mut h_count: u32 = 0;
                let rc = topsort_get_frontier_count(&mut h_count);
                assert_eq!(rc, 0, "topsort_get_frontier_count cuda err {rc}");
                let h_count = h_count as usize;
                if h_count > 0 {
                    use cust::memory::CopyDestination;
                    let guard = FRONTIER_DEV_BUF.lock().unwrap();
                    let buf = guard.as_ref().unwrap();
                    let mut h_frontier = vec![0u64; h_count];
                    buf.index(0..h_count)
                        .copy_to(&mut h_frontier)
                        .expect("frontier copy_to failed");
                    FRONTIER_OUT.lock().unwrap().extend_from_slice(&h_frontier);
                }
            }
        }

        // Collect this PE's frontier (written by kernel/stub into FRONTIER_OUT).
        let frontier = take_frontier();
        let frontier_size = frontier.len();
        total_processed += frontier_size;

        if frontier_size == 0 {
            break;
        }

        for r in &frontier_results {
            if !r.is_ok() {
                eprintln!("PE {my_pe}: wave {wave} frontier kernel error: {:?}", r.error);
            }
        }

        // ── Wave B: fan out from frontier vertices ─────────────────────────────
        for &global_v in &frontier {
            let local_v = (global_v - base_offset) as usize;
            if local_v >= graph.local_n { continue; }
            for &(target_pe, target_local) in &graph.out_edges[local_v] {
                if target_pe == my_pe {
                    push_decrements(&[target_local]);
                } else {
                    let _ = world
                        .exec_am_pe(target_pe, DecrementAm { local_vertex: target_local })
                        .spawn();
                }
            }
        }

        world.wait_all();
        world.barrier();

        // ── Wave C: apply decrements on all PEs ───────────────────────────────
        //
        // CUDA: collect host decrements, upload to device, set CUDA global,
        //   then exec_kernel_all fires the atomicSub kernel.
        // Stub: exec_kernel_all calls the stub which drains PENDING_DECREMENTS.

        #[cfg(feature = "cuda")]
        {
            use cust::memory::DeviceBuffer;
            let host_dec = take_decrements();
            let num_dec = host_dec.len();
            if num_dec > 0 {
                let d_dec: DeviceBuffer<u64> = DeviceBuffer::from_slice(&host_dec)
                    .expect("decrements DeviceBuffer alloc");
                unsafe {
                    let rc = topsort_set_decrements(d_dec.as_device_ptr().as_mut_ptr(), num_dec);
                    assert_eq!(rc, 0, "topsort_set_decrements cuda err {rc}");
                }
                *DECREMENTS_DEV_BUF.lock().unwrap() = Some(d_dec);
            } else {
                // Zero decrements: point to null / set count 0.
                unsafe { topsort_set_decrements(std::ptr::null_mut(), 0); }
                *DECREMENTS_DEV_BUF.lock().unwrap() = None;
            }
        }

        // Grid for apply_decrements covers the decrement count (CUDA) or
        // local_n (stub — stub ignores grid and just drains the static queue).
        #[cfg(feature = "cuda")]
        let dec_count = {
            let g = DECREMENTS_DEV_BUF.lock().unwrap();
            g.as_ref().map_or(0, |b| b.len())
        };
        #[cfg(feature = "cuda")]
        let dec_config = LaunchConfig::simple_1d(dec_count.max(1) as u32, 256);
        #[cfg(not(feature = "cuda"))]
        let dec_config = vertex_config.clone();

        let results = gpu_array.exec_kernel_all("topsort_apply_decrements", dec_config).block();

        // Sync local GPU stream before releasing the decrement device buffer.
        #[cfg(feature = "cuda")]
        gpu_array.sync_local().expect("GPU stream sync (apply_decrements) failed");

        #[cfg(feature = "cuda")]
        { *DECREMENTS_DEV_BUF.lock().unwrap() = None; }

        for r in &results {
            if !r.is_ok() {
                eprintln!("PE {my_pe}: wave {wave} decrement kernel error: {:?}", r.error);
            }
        }
        world.barrier();

        wave += 1;
        if my_pe == 0 {
            println!("  wave {wave:3}: frontier={frontier_size:6}  processed={total_processed}");
        }
    }

    // ── Clean up CUDA buffers ─────────────────────────────────────────────────
    #[cfg(feature = "cuda")]
    { *FRONTIER_DEV_BUF.lock().unwrap() = None; }

    let elapsed = now.elapsed().as_secs_f64();
    world.barrier();

    if my_pe == 0 {
        let success = total_processed == global_n;
        println!("\n── GPU Topsort [{backend}] ────────────────────────────────────");
        println!("  Vertices:        {global_n}  ({local_n}/PE)");
        println!("  Edges/vertex:    {edges_per_vertex}");
        println!("  BFS waves:       {wave}");
        println!("  Processed:       {total_processed}/{global_n}");
        println!("  Time:            {elapsed:.4} s");
        println!("  MVPS:            {:.2}", (global_n as f64 / 1e6) / elapsed);
        println!(
            "  Status:          {}",
            if success { "SUCCESS (all vertices sorted)" } else { "FAILURE (cycle or bug)" }
        );
        println!("─────────────────────────────────────────────────────────────");
    }
}

//! GPU Histogram benchmark — GPU port of the Lamellar `histo` benchmark.
//!
//! # Algorithm
//!
//! Each PE owns `COUNTS_LOCAL_LEN` histogram buckets stored in a
//! `DistGpuArray<u64>`.  Each PE generates `L_NUM_UPDATES` random global
//! indices and:
//!
//! 1. **Classifies** each index by target PE:
//!    `rank = idx % num_pes`,  `offset = idx / num_pes`
//! 2. **Local** updates are pushed into a process-local `PENDING_UPDATES`
//!    queue (one per PE process).
//! 3. **Remote** updates are batched and sent via `BatchHistoAm` to the target
//!    PE's process, where `exec` pushes them into that PE's queue.
//! 4. After `wait_all()` + `barrier()` all pending updates are in each PE's
//!    local `PENDING_UPDATES` queue.
//! 5. The histogram kernel is dispatched on every PE:
//!    - **With `--features cuda`**: pending updates are copied to a
//!      `cust::DeviceBuffer<u64>`, `histo_set_updates` uploads the device
//!      pointer into a CUDA device-side global, and the `histo_u64` kernel
//!      runs `atomicAdd` — one thread per update.
//!    - **Without**: the stub kernel drains `PENDING_UPDATES` sequentially
//!      on the host (useful for correctness testing without a GPU).
//!
//! # Running
//! ```sh
//! # Stub (no GPU):
//! cargo run --example gpu_histo -- 1000000
//! # Real CUDA (requires CUDA toolkit):
//! cargo run --features cuda --example gpu_histo -- 1000000
//! ```
use std::sync::Mutex;
use std::time::Instant;

use lamellar::ActiveMessaging;
use lamellar::LamellarEnv;
use lamellar::gpu::{
    array::{next_array_id, register_kernel, DistGpuArray},
    kernel::{FfiCudaKernel, LaunchConfig},
};

// ── Constants ────────────────────────────────────────────────────────────────

/// Histogram bucket count per PE.
const COUNTS_LOCAL_LEN: usize = 1_000_000;

// ── Per-PE pending update queue (host memory) ─────────────────────────────────
//
// Lamellar SMP runs as separate OS processes (one per PE), so a process-global
// Mutex<Vec<u64>> is safely per-PE — no cross-PE aliasing.

static PENDING_UPDATES: std::sync::LazyLock<Mutex<Vec<u64>>> =
    std::sync::LazyLock::new(|| Mutex::new(Vec::new()));

fn push_updates(offsets: &[u64]) {
    PENDING_UPDATES.lock().unwrap().extend_from_slice(offsets);
}

fn take_updates() -> Vec<u64> {
    std::mem::take(&mut *PENDING_UPDATES.lock().unwrap())
}

// ── CUDA FFI declarations ─────────────────────────────────────────────────────
//
// histo_set_updates: write the device pointer + count into CUDA device-side
//   globals (d_updates, d_num_updates) via cudaMemcpyToSymbol.
// histo_kernel_u64_ffi: FfiKernelFn<u64>-compatible wrapper that launches
//   the histo_increment kernel using the pre-set globals.

#[cfg(feature = "cuda")]
extern "C" {
    /// Upload the device-side update buffer.  Must be called before the
    /// `histo_u64` kernel on this PE.  Returns 0 on success (cudaSuccess).
    fn histo_set_updates(device_ptr: *mut u64, num_updates: usize) -> i32;

    /// FfiKernelFn-compatible launcher (defined in cuda/histo.cu).
    fn histo_kernel_u64_ffi(
        ptr: *mut u64,
        len: usize,
        gx: u32, gy: u32, gz: u32,
        bx: u32, by: u32, bz: u32,
        shared: u32,
        stream: *mut core::ffi::c_void,
    ) -> i32;
}

// ── Stub kernel (no CUDA) ─────────────────────────────────────────────────────

/// Stub histogram kernel — runs on the host, sequential.
///
/// DeviceBuffer<u64> without the `cuda` feature is backed by a Vec<u64>,
/// so `ptr` is a valid host pointer.
#[cfg(not(feature = "cuda"))]
unsafe extern "C" fn histo_kernel_u64_stub(
    ptr: *mut u64,
    len: usize,
    _gx: u32, _gy: u32, _gz: u32,
    _bx: u32, _by: u32, _bz: u32,
    _shared: u32,
    _stream: *mut core::ffi::c_void,
) -> i32 {
    let counts = std::slice::from_raw_parts_mut(ptr, len);
    for &offset in take_updates().iter() {
        if (offset as usize) < len {
            counts[offset as usize] += 1;
        }
    }
    0
}

// ── Per-PE device buffer for pending updates (CUDA path only) ─────────────────

#[cfg(feature = "cuda")]
static UPDATES_DEVICE_BUF: std::sync::LazyLock<
    Mutex<Option<cust::memory::DeviceBuffer<u64>>>,
> = std::sync::LazyLock::new(|| Mutex::new(None));

// ── Remote-update Active Message ──────────────────────────────────────────────

/// Batch of histogram update offsets destined for a single remote PE.
///
/// `exec` pushes the offsets into the receiving PE's `PENDING_UPDATES` queue.
/// The GPU increment happens later when `exec_kernel_all` fires.
#[lamellar::AmData(Debug, Clone)]
struct BatchHistoAm {
    offsets: Vec<u64>,
}

#[lamellar::am]
impl LamellarAM for BatchHistoAm {
    async fn exec(self) {
        push_updates(&self.offsets);
    }
}

// ── main ──────────────────────────────────────────────────────────────────────

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let l_num_updates: usize = args
        .get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);

    // ── CUDA initialisation (no-op without cuda feature) ─────────────────────
    // init_cuda_context stores the context globally so Lamellar worker
    // threads can push it before running CUDA operations in AM exec bodies.
    #[cfg(feature = "cuda")]
    let _cuda_ctx = lamellar::gpu::init_cuda_context(
        cust::quick_init().expect("CUDA initialisation failed"),
    );

    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let global_count = COUNTS_LOCAL_LEN * num_pes;

    world.barrier();
    let backend = if cfg!(feature = "cuda") { "CUDA" } else { "stub" };
    println!("PE {my_pe}/{num_pes}: histo [{backend}] — {l_num_updates} updates/PE");

    // ── 1. Register kernel ────────────────────────────────────────────────────
    #[cfg(feature = "cuda")]
    register_kernel(FfiCudaKernel::<u64>::new("histo_u64", histo_kernel_u64_ffi));

    #[cfg(not(feature = "cuda"))]
    register_kernel(FfiCudaKernel::<u64>::new("histo_u64", histo_kernel_u64_stub));

    // ── 2. Allocate distributed GPU array (counts) ───────────────────────────
    let array_id = next_array_id();
    let gpu_array = DistGpuArray::<u64>::new(&world, array_id, COUNTS_LOCAL_LEN, 0)
        .expect("DistGpuArray::new failed");

    // ── 3. Generate random updates and classify local/remote ─────────────────
    use rand::prelude::*;
    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);
    let mut remote_batches: Vec<Vec<u64>> = vec![Vec::new(); num_pes];
    let mut local_updates: Vec<u64> = Vec::new();

    for _ in 0..l_num_updates {
        let idx: u64 = rng.gen_range(0..global_count as u64);
        let rank = (idx % num_pes as u64) as usize;
        let offset = idx / num_pes as u64;
        if rank == my_pe {
            local_updates.push(offset);
        } else {
            remote_batches[rank].push(offset);
        }
    }
    push_updates(&local_updates);

    world.barrier();
    let now = Instant::now();

    // ── 4. Send remote update batches ─────────────────────────────────────────
    for (target_pe, offsets) in remote_batches.into_iter().enumerate() {
        if !offsets.is_empty() {
            let _ = world.exec_am_pe(target_pe, BatchHistoAm { offsets }).spawn();
        }
    }
    let issue_time = now.elapsed().as_secs_f64();

    // ── 5. Wait for all remote AMs ────────────────────────────────────────────
    world.wait_all();
    world.barrier();
    let comm_time = now.elapsed().as_secs_f64();

    // ── 6a. CUDA path: upload pending updates to device memory ───────────────
    //
    // We collect the host queue, copy it to a per-PE device buffer, then call
    // histo_set_updates() so the CUDA kernel knows where the device data lives.
    // The device buffer guard is dropped EXPLICITLY after sync_local() below —
    // _updates_guard holds the UPDATES_DEVICE_BUF mutex, so we must NOT try to
    // lock it again before calling drop(_updates_guard).

    #[cfg(feature = "cuda")]
    let _updates_guard = {
        use cust::memory::DeviceBuffer;
        let host_updates = take_updates();
        let num_updates = host_updates.len();

        // Upload to device.
        let d_buf = DeviceBuffer::from_slice(&host_updates)
            .expect("DeviceBuffer::from_slice failed");

        // Set device globals in the CUDA module.
        let rc = unsafe {
            histo_set_updates(
                d_buf.as_device_ptr().as_mut_ptr(),
                num_updates,
            )
        };
        assert_eq!(rc, 0, "histo_set_updates failed with CUDA error {rc}");

        // Store in static so the buffer stays alive while the kernel runs.
        let mut guard = UPDATES_DEVICE_BUF.lock().unwrap();
        *guard = Some(d_buf);
        guard // returned; holds the lock until explicit drop below
    };

    // ── 6b. Stub path: nothing extra needed (take_updates is called by kernel) ─
    // (PENDING_UPDATES already has all updates from steps 3 and the AM exec)

    // ── 7. Dispatch histogram kernel on all PEs ───────────────────────────────
    //
    // CUDA: grid covers l_num_updates (upper bound — each PE may have fewer,
    //   extra threads are no-ops due to the bounds check in the kernel).
    // Stub: grid is irrelevant (kernel ignores it); covers COUNTS_LOCAL_LEN.
    #[cfg(feature = "cuda")]
    let config = LaunchConfig::simple_1d(l_num_updates as u32, 256);
    #[cfg(not(feature = "cuda"))]
    let config = LaunchConfig::simple_1d(COUNTS_LOCAL_LEN as u32, 256);

    // exec_kernel_all only *launches* the kernel (async GPU dispatch);
    // it does NOT sync the stream inside the AM to avoid deadlocking the executor.
    let results = gpu_array.exec_kernel_all("histo_u64", config).block();

    // Block the calling thread until the GPU finishes all kernel work.
    // Must happen BEFORE dropping _updates_guard (which frees the device buffer).
    #[cfg(feature = "cuda")]
    gpu_array.sync_local().expect("GPU stream sync failed");

    let kernel_time = now.elapsed().as_secs_f64();

    // Drop the device buffer and release the mutex.
    // IMPORTANT: _updates_guard holds UPDATES_DEVICE_BUF; drop it here so the
    // mutex is free for future use.  Never lock UPDATES_DEVICE_BUF while
    // _updates_guard is still alive.
    #[cfg(feature = "cuda")]
    drop(_updates_guard);

    // ── 8. Validate ───────────────────────────────────────────────────────────
    let all_ok = results.iter().all(|r| r.is_ok());
    if !all_ok {
        for r in &results {
            if !r.is_ok() {
                eprintln!("PE {my_pe}: PE {} kernel FAILED: {:?}", r.pe, r.error);
            }
        }
    }

    world.barrier();
    let global_time = now.elapsed().as_secs_f64();
    let mb_sent = world.MB_sent();

    // ── 9. Print results (PE 0 only) ──────────────────────────────────────────
    if my_pe == 0 {
        let global_updates = l_num_updates * num_pes;
        println!("\n── GPU Histogram [{backend}] ─────────────────────────────────");
        println!("  PEs:             {num_pes}");
        println!("  Updates/PE:      {l_num_updates}");
        println!("  Global updates:  {global_updates}");
        println!("  Buckets/PE:      {COUNTS_LOCAL_LEN}");
        println!("  Issue time:      {issue_time:.4} s");
        println!("  Comm time:       {comm_time:.4} s");
        println!("  Kernel time:     {kernel_time:.4} s");
        println!("  Global time:     {global_time:.4} s");
        println!(
            "  MUPS:            {:.2}",
            (global_updates as f64 / 1e6) / global_time
        );
        println!("  MB sent:         {mb_sent:.3}");
        println!(
            "  Status:          {}",
            if all_ok { "SUCCESS" } else { "FAILURE" }
        );
        println!("─────────────────────────────────────────────────────────────");
    }
}

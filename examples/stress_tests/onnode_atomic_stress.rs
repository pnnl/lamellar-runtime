/// ------------Lamellar stress test: concurrent atomic adds to one element -------------------
/// Every PE adds 1 to element 0 of an `AtomicArray<usize>` (owned by PE 0) `STRESS_ITERS` times, then the final value
/// is compared with the number of adds issued. Updates are kept only if every mechanism that touches that word is
/// coherent with the others, so this finds mixed CPU/NIC atomic problems and lost updates under contention.
///
/// `STRESS_MODE` selects how each PE issues its adds:
///   single (default): single-element adds, which go through the backend's own atomics (NIC atomics on `ucx_opt`;
///                     message-based atomics executed by the target CPU on `libfabric_sys_opt`; CPU atomics on the
///                     shared-memory backends and for same-node targets with `enable-on-node-shmem`);
///   batch:            `batch_add`, executed on the owner PE by an active message using CPU atomics;
///   mixed:            even PEs use single adds, odd PEs use batch adds, so the backend's atomics and owner-side CPU
///                     atomics hit the same word. This is the case that loses updates when the NIC's atomics are not
///                     coherent with CPU atomics.
///
/// Environment variables:
///   STRESS_MODE         single | batch | mixed (default single)
///   STRESS_ITERS        adds per PE for single-add PEs (default 200000)
///   STRESS_BATCH_ITERS  adds per PE for batch-add PEs (default STRESS_ITERS); different counts show which half of a
///                       mixed run was lost
///
/// Result: PE 0 prints one line, `onnode_atomic_stress: mode=.. array=.. total=.. expected=.. lost=.. time=..`, where
/// `array` is the AtomicArray variant that was created. On a mismatch it prints `LOST UPDATES` to stderr and exits
/// with status 1; otherwise it exits 0. Needs at least 2 PEs (mixed mode needs both an even and an odd PE).
///
/// Expected results (measured unless noted):
///   * `ucx_opt` (device atomics, `UCX_ATOMIC_MODE=guess` selects them when an RC or DC transport is active):
///     single and batch pass; mixed LOSES updates, because the NIC's atomics are not coherent with CPU atomics.
///     That is the known hazard recorded in ATOMIC_ARRAY_DESIGN.md, not a new failure.
///   * `libfabric_sys_opt` (`verbs;ofi_rxm`): all three modes pass; its atomics are executed by the target CPU.
///   * other backends: not measured with this program.
///
/// Run (example, one node with 4 PEs):
///   cargo build --release --example onnode_atomic_stress \
///       --features with-salloc,enable-ucx-opt,enable-libfabric-sys-opt,enable-on-node-shmem
///   LAMELLAR_BACKEND=ucx-opt STRESS_MODE=mixed ./target/release/examples/onnode_atomic_stress -- \
///       --nodes 1 --pes 4 --pes-per-node 4 --threads-per-pe 1
/// --------------------------------------------------------------------
use lamellar::array::prelude::*;
use lamellar::LamellarEnv;

#[lamellar::main]
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let iters: usize = std::env::var("STRESS_ITERS").ok().and_then(|v| v.parse().ok()).unwrap_or(200_000);
    let array: AtomicArray<usize> = AtomicArray::new(&world, num_pes, Distribution::Block).block();
    world.barrier();
    // element 0 lives on PE 0
    let t = std::time::Instant::now();
    let mode = std::env::var("STRESS_MODE").unwrap_or_else(|_| "single".to_string());
    // iterations of the batch-add PEs (defaults to `iters`); different counts reveal which half of a mixed run is lost
    let biters: usize = std::env::var("STRESS_BATCH_ITERS").ok().and_then(|v| v.parse().ok()).unwrap_or(iters);
    let uses_batch = |pe: usize| match mode.as_str() {
        "batch" => true,
        "mixed" => pe % 2 == 1,
        _ => false,
    };
    if uses_batch(my_pe) {
        const BATCH: usize = 1000;
        let idx = vec![0usize; BATCH];
        let mut left = biters;
        while left > 0 {
            let n = left.min(BATCH);
            array.batch_add(idx[..n].to_vec(), 1usize).block();
            left -= n;
        }
    } else {
        for _ in 0..iters {
            array.add_unmanaged(0, 1);
        }
    }
    array.wait_all();
    world.barrier();
    let total = array.load(0).block();
    if my_pe == 0 {
        let expect: usize = (0..num_pes).map(|pe| if uses_batch(pe) { biters } else { iters }).sum();
        let kind = match &array {
            AtomicArray::NetworkAtomicArray(_) => "network",
            AtomicArray::NativeAtomicArray(_) => "native",
            AtomicArray::GenericAtomicArray(_) => "generic",
        };
        println!(
            "onnode_atomic_stress: mode={mode} array={kind} total={total} expected={expect} lost={} time={:.2}s",
            expect as i64 - total as i64,
            t.elapsed().as_secs_f64()
        );
        if total != expect {
            eprintln!("LOST UPDATES");
            std::process::exit(1);
        }
    }
}

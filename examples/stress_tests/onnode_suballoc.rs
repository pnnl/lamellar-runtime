/// ------------Lamellar stress test: sub-team allocation followed by a world allocation -------------------
/// Creates a subset team (all PEs except the last), allocates a shared memory region in it, then allocates a region
/// world-wide, `SUBALLOC_ROUNDS` times. A sub-allocation is collective over the subset only. With the on-node
/// shared-memory feature, an allocation that names its shared-memory segment after a per-process counter and bumps it
/// on only the PEs in the subset leaves the counters out of step, so the next world-wide allocation hangs: the PEs
/// open segments with different names.
///
/// `SUBALLOC_STAGE` limits how far each round goes, to find where a hang is:
///   0: create the subset team only;
///   1: also allocate in the subset team (members only);
///   2 (default): also allocate world-wide afterwards.
/// Every PE prints where it is to stderr (`PE n round r: ...`), so the last line per PE shows the stage that hung.
/// Only the PEs that are really in the subset (`team_pe_id()` is Ok) may allocate in it: `create_team_from_arch`
/// returns Some on every PE of the parent team, so check membership, not the Option.
///
/// Environment variables:
///   SUBALLOC_STAGE   0 | 1 | 2 (default 2)
///   SUBALLOC_ROUNDS  number of rounds (default 5)
///
/// Result: PE 0 prints `onnode_suballoc: done` and the program exits 0 when every round finishes. A failure is a hang
/// (run it under `timeout`, e.g. 120 s) or a panic. Needs at least 3 PEs, so at least one PE is outside the subset and
/// two are inside. Without `enable-on-node-shmem` it exercises ordinary sub-team allocation only and should pass
/// everywhere.
///
/// Expected results: the bug existed in the three libfabric backends (`libfabric`, `libfabric_sys`,
/// `libfabric_sys_opt`) and is fixed by allocating sub-allocations from anonymous memory. Measured: hung before the
/// fix and passes after it on `libfabric_sys_opt`; the same change was applied to `libfabric` and `libfabric_sys`
/// and compile-checked only. From reading the code, `ucx`, `ucx_opt` and the shared-memory backends do not allocate
/// shared memory in `sub_alloc`, so they are not expected to hang; not measured with this program.
///
/// Run (example, one node with 4 PEs):
///   cargo build --release --example onnode_suballoc \
///       --features with-salloc,enable-libfabric-sys-opt,enable-on-node-shmem
///   LAMELLAR_BACKEND=libfabric-sys-opt timeout 120 ./target/release/examples/onnode_suballoc -- \
///       --nodes 1 --pes 4 --pes-per-node 4 --threads-per-pe 1
/// --------------------------------------------------------------------
use lamellar::memregion::prelude::*;
use lamellar::{BlockedArch, LamellarEnv};

#[lamellar::main]
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    assert!(num_pes >= 3, "needs at least 3 PEs");
    let stage: usize = std::env::var("SUBALLOC_STAGE").ok().and_then(|v| v.parse().ok()).unwrap_or(2);
    let rounds: usize = std::env::var("SUBALLOC_ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(5);
    for round in 0..rounds {
        eprintln!("PE {my_pe} round {round}: creating subset team");
        // `create_team_from_arch` returns Some on every PE of the parent team; real membership is `team_pe_id()`
        let team = world.create_team_from_arch(BlockedArch::new(0, num_pes - 1));
        let member = team.as_ref().map_or(false, |t| t.team_pe_id().is_ok());
        eprintln!("PE {my_pe} round {round}: team created (member: {member})");
        if stage >= 1 && member {
            let team = team.as_ref().unwrap();
            let sub: SharedMemoryRegion<u8> = team.alloc_shared_mem_region::<u8>(4096).block();
            eprintln!("PE {my_pe} round {round}: allocated in the subset team");
            drop(sub);
        }
        drop(team);
        world.barrier();
        eprintln!("PE {my_pe} round {round}: world barrier passed");
        if stage >= 2 {
            let all: SharedMemoryRegion<u8> = world.alloc_shared_mem_region::<u8>(4096).block();
            world.barrier();
            eprintln!("PE {my_pe} round {round}: world allocation done");
            drop(all);
        }
    }
    if my_pe == 0 {
        println!("onnode_suballoc: done");
    }
}

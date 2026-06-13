/// ------------Lamellar Example: GlobalRwDarc Collective Write-------------------------
/// Demonstrates and times the difference between one-sided write() and collective_write()
/// on a GlobalRwDarc.
///
/// One-sided write():
///   Each PE acquires the global lock independently — acquisitions serialize.
///   Total wall time = num_pes * lock_overhead.
///
/// collective_write():
///   All PEs acquire the lock together — each PE updates its local data in parallel.
///   Total wall time = lock_overhead.
///
/// Both versions perform the same total number of updates across all PEs.
/// --------------------------------------------------------------------
use lamellar::darc::prelude::*;
use std::time::Instant;

#[lamellar::main]
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    // -------------------------------------------------------
    // One-sided write: PEs serialize on the global lock
    // -------------------------------------------------------
    let counter = GlobalRwDarc::new(&world, 0usize).block().unwrap();

    world.barrier();
    let onesided_start = Instant::now();

    let mut guard = counter.write().block();
    *guard += my_pe + 1;
    drop(guard);

    world.barrier();
    let onesided_elapsed = onesided_start.elapsed();

    if my_pe == 0 {
        let val = *counter.read().block();
        let expected = (num_pes * (num_pes + 1)) / 2;
        println!(
            "[one-sided write]  elapsed: {:.6}s  counter = {val}  expected = {expected}",
            onesided_elapsed.as_secs_f64()
        );
        assert_eq!(val, expected);
    }
    world.barrier();

    // -------------------------------------------------------
    // Collective write: all PEs acquire the lock together
    // -------------------------------------------------------
    let shared = GlobalRwDarc::new(&world, 0usize).block().unwrap();

    world.barrier();
    let collective_start = Instant::now();

    let mut guard = shared.collective_write().block();
    *guard += my_pe + 1;
    drop(guard);

    world.barrier();
    let collective_elapsed = collective_start.elapsed();

    if my_pe == 0 {
        let val = *shared.read().block();
        let expected = (num_pes * (num_pes + 1)) / 2;
        println!(
            "[collective write] elapsed: {:.6}s  shared = {val}  expected = {expected}",
            collective_elapsed.as_secs_f64()
        );
        assert_eq!(val, expected);
        println!(
            "\ncollective_write {:.1}x faster for {num_pes} PEs",
            onesided_elapsed.as_secs_f64() / collective_elapsed.as_secs_f64()
        );
    }
    world.barrier();
    println!("PE {my_pe} done");
}

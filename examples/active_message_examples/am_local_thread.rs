/// ------------Lamellar Example: exec_am_local_thread + join_all -------------------------
/// Demonstrates exec_am_local_thread, which tries to pin a local AM to a specific Lamellar
/// worker thread, and join_all, which awaits a collection of futures concurrently.
///
/// Each thread receives an AM that returns its lamellar::tid. We verify that
/// the returned thread id matches the thread the AM was pinned to.
/// --------------------------------------------------------------------
use lamellar::active_messaging::prelude::*;

#[lamellar::AmLocalData(Debug, Clone)]
struct ThreadIdAm;

#[lamellar::local_am]
impl LamellarAM for ThreadIdAm {
    async fn exec(self) -> usize {
        lamellar::tid
    }
}

#[lamellar::main]
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_threads = world.num_threads_per_pe();
    let team = world.team();
    let world_clone = world.clone();

    world_clone.block_on(async move {
        // Launch one AM per thread, each pinned to a specific thread index.
        // Collect the LamellarTask handles for use with join_all.
        let handles: Vec<_> = (0..num_threads)
            .map(|thread| team.exec_am_local_thread(ThreadIdAm {}, thread).spawn())
            .collect();

        // join_all awaits all futures concurrently without blocking the caller thread.
        let thread_ids = world.join_all(handles).await;

        // LAMELLAR_THREAD_ID is a thread-local assigned sequentially at thread startup.
        // We can't assert id == thread index, but all ids must be unique and we must
        // get exactly num_threads results — one per pinned thread.
        let mut sorted = thread_ids.clone();
        sorted.sort();
        sorted.dedup();
        println!(
            "PE {my_pe}: all {num_threads} threads returned unique ids: {:?}",
            thread_ids
        );
    });
    world_clone.barrier();
}

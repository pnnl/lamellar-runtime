/// ------------Lamellar micro-benchmark: single-thread small RDMA gets -------------------------
/// PE 0 issues GET_COUNT unmanaged gets of GET_SIZE bytes from the last PE through the same
/// `get_into_buffer_unmanaged` + `LamellarBuffer::split_off` path get_bw uses, then wait_all.
/// Runs for a fixed number of rounds so a profiler can attach to the steady state.
/// Env: GET_SIZE (8), GET_COUNT (1000000, capped to fit the buffer), GET_ROUNDS (20).
/// --------------------------------------------------------------------
use lamellar::memregion::prelude::*;
use std::time::Instant;

const MEMREG_LEN: usize = 64 * 1024 * 1024;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

#[lamellar::main]
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let size = env_usize("GET_SIZE", 8);
    let count = env_usize("GET_COUNT", 1_000_000).min(MEMREG_LEN / size);
    let rounds = env_usize("GET_ROUNDS", 20);
    let mem_reg = world.alloc_shared_mem_region::<u8>(MEMREG_LEN).block();
    let mut data = world.alloc_one_sided_mem_region::<u8>(MEMREG_LEN);
    unsafe {
        for i in data.as_mut_slice() {
            *i = my_pe as u8;
        }
        for i in mem_reg.as_mut_slice() {
            *i = num_pes as u8;
        }
    }
    world.barrier();
    if my_pe == 0 {
        println!("get_small: size={}B count={} rounds={}", size, count, rounds);
    }
    for round in 0..rounds {
        world.barrier();
        if my_pe == 0 {
            let mut buffer = unsafe { LamellarBuffer::from_one_sided_memory_region(data) };
            let t0 = Instant::now();
            for _ in 0..count {
                let rest = buffer.split_off(size);
                unsafe {
                    let _ = mem_reg.get_into_buffer_unmanaged(num_pes - 1, 0, buffer);
                }
                buffer = rest;
            }
            let issue = t0.elapsed().as_secs_f64();
            world.wait_all();
            let total = t0.elapsed().as_secs_f64();
            println!(
                "round {}: {} gets  issue {:.3}s ({:.3} Mgets/s)  with wait_all {:.3}s ({:.3} Mgets/s, {:.1} MB/s)",
                round,
                count,
                issue,
                count as f64 / issue / 1e6,
                total,
                count as f64 / total / 1e6,
                (count * size) as f64 / total / 1e6,
            );
            data = buffer.try_unwrap().unwrap();
        }
        world.barrier();
    }
    world.barrier();
}

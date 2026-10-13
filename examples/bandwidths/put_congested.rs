/// ------------Lamellar Bandwidth: RDMA Put under load  -------------------
/// Every PE issues unmanaged RDMA puts at the same time, so the network is
/// congested, unlike `put_bw` where a single PE sends to a single peer.
/// Assumes the PEs are mapped round-robin over two nodes (`--map-by node`), so
/// PEs of different parity are on different nodes. Patterns:
///   shift  - PE i puts to PE (i + 1) % N: all traffic crosses the network
///   incast - every PE puts to PE 0: one receiver is the bottleneck
///   random - every put goes to a random PE on the other node
/// With `ONNODE=1` all PEs are on one node and the targets are any other PE:
/// random picks any PE other than itself (shift and incast are unchanged).
/// Reports the aggregate bandwidth over all PEs.
/// --------------------------------------------------------------------
use lamellar::memregion::prelude::*;
use std::time::Instant;

const ARRAY_LEN: usize = 64 * 1024 * 1024;
const BYTES_PER_PE: usize = 256 * 1024 * 1024;

#[lamellar::main]
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let half = (num_pes / 2).max(1);
    let onnode = std::env::var("ONNODE").map_or(false, |v| v == "1");
    assert!(onnode || num_pes % 2 == 0, "needs an even PE count over two nodes");
    let array = world.alloc_shared_mem_region::<u8>(ARRAY_LEN).block();
    let data = world.alloc_one_sided_mem_region::<u8>(2 * 1024 * 1024);
    unsafe {
        for i in data.as_mut_slice() {
            *i = my_pe as u8;
        }
    }
    world.barrier();
    if my_pe == 0 {
        println!("==================Congested put test, {num_pes} PEs===========================");
    }
    for pattern in ["shift", "incast", "random"] {
        for exp in [12u32, 14, 16, 18, 20] {
            let size = 1usize << exp;
            let count = BYTES_PER_PE / size;
            let mut rng = 0x9E3779B97F4A7C15u64 ^ (my_pe as u64 + 1);
            world.barrier();
            let timer = Instant::now();
            for j in 0..count {
                let target = match pattern {
                    "shift" => (my_pe + 1) % num_pes,
                    "incast" => 0,
                    _ => {
                        rng ^= rng << 13;
                        rng ^= rng >> 7;
                        rng ^= rng << 17;
                        if onnode {
                            (my_pe + 1 + rng as usize % (num_pes - 1)) % num_pes
                        } else {
                            2 * (rng as usize % half) + (1 - my_pe % 2)
                        }
                    }
                };
                let offset = (j * size) % (ARRAY_LEN - size);
                unsafe {
                    let _ = array.put_buffer_unmanaged(target, offset, data.sub_region(..size));
                }
            }
            array.wait_all();
            world.barrier();
            let t = timer.elapsed().as_secs_f64();
            if my_pe == 0 {
                let total = (BYTES_PER_PE * num_pes) as f64 / 1048576.0;
                println!(
                    "pattern: {pattern} tx_size: {size}B num_tx_per_pe: {count} time: {t:.4}s aggregate: {:.0}MB/s per_pe: {:.0}MB/s",
                    total / t,
                    total / t / num_pes as f64
                );
            }
        }
    }
    world.barrier();
}

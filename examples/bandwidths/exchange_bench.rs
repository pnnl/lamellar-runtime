/// ------------Lamellar Bandwidth: random exchange -------------------
/// Every PE sends `size` bytes to a randomly chosen PE with an unmanaged put, followed (unless
/// `EXCH_ATOMIC=0`) by an atomic add on a counter at the same PE, `num_pes` times per round, then
/// `wait_all` and a barrier. This is the put-plus-atomic mix to the same peer, to random targets,
/// that the point-to-point benchmarks do not exercise.
/// Targets come from a per-PE xorshift stream with a fixed seed, so every PE can replay every other
/// PE's stream and compute exactly how many messages it must have received.
/// Env: `EXCH_SIZES` (comma list of bytes, default 1024,4096,8192,32768), `EXCH_MB` (MiB sent per PE
/// per size, default 64), `EXCH_ATOMIC` (0 disables the counter updates), `EXCH_PUT` (0 sends only the
/// counter updates: an atomics-only run; use a small `EXCH_SIZES` and `EXCH_MB`), `EXCH_NODES` (only used to
/// print per-node bandwidth), `EXCH_SELF` (0 excludes the sending PE from its own targets and sends
/// `num_pes - 1` times per round, as earlier versions did; default 1 includes it), `EXCH_TASKS` (number of tasks issuing the sends on the executor threads,
/// default 1 = the main thread; with more than one, leave an executor thread free for the runtime's own
/// progress, e.g. 3 tasks for 4 threads). The counters and one payload slot per source are checked after
/// every size.
/// --------------------------------------------------------------------
use lamellar::memregion::prelude::*;
use lamellar::ActiveMessaging;
use std::time::Instant;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn seed(pe: usize, task: usize, size: usize) -> u64 {
    0x9E37_79B9_7F4A_7C15u64 ^ ((pe as u64 + 1) << 20) ^ ((task as u64 + 1) << 44) ^ (size as u64)
}

/// Sends handled by task `t` when `total` sends are split over `tasks` tasks.
fn share(total: usize, tasks: usize, t: usize) -> usize {
    total / tasks + usize::from(t < total % tasks)
}

/// One task's sends: `n` puts (optional) each followed by a counter update (optional), random targets.
fn issue(
    recv: &SharedMemoryRegion<u8>,
    counters: &SharedMemoryRegion<usize>,
    data: &OneSidedMemoryRegion<u8>,
    (my_pe, num_pes, size): (usize, usize, usize),
    (n, mut rng): (usize, u64),
    (use_put, use_atomic, self_ok): (bool, bool, bool),
) {
    for _ in 0..n {
        let peer = target(&mut rng, my_pe, num_pes, self_ok);
        unsafe {
            if use_put {
                let _ = recv.put_buffer_unmanaged(peer, my_pe * size, data.sub_region(..size));
            }
            if use_atomic {
                counters.atomic_add_unmanaged(peer, 0, 1);
            }
        }
    }
}

fn next(rng: &mut u64) -> u64 {
    *rng ^= *rng << 13;
    *rng ^= *rng >> 7;
    *rng ^= *rng << 17;
    *rng
}

/// Uniform choice among all PEs, or among the PEs other than `me` when `self_ok` is false.
fn target(rng: &mut u64, me: usize, num_pes: usize, self_ok: bool) -> usize {
    if self_ok {
        return (next(rng) % num_pes as u64) as usize;
    }
    let t = (next(rng) % (num_pes as u64 - 1)) as usize;
    if t >= me {
        t + 1
    } else {
        t
    }
}

#[lamellar::main]
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let self_ok = env_usize("EXCH_SELF", 1) != 0;
    assert!(self_ok || num_pes > 1, "EXCH_SELF=0 needs at least 2 PEs");
    let sizes: Vec<usize> = std::env::var("EXCH_SIZES")
        .unwrap_or_else(|_| "1024,4096,8192,32768".to_owned())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let max_size = *sizes.iter().max().expect("EXCH_SIZES is empty");
    let mb_per_pe = env_usize("EXCH_MB", 64);
    let use_atomic = env_usize("EXCH_ATOMIC", 1) != 0;
    let use_put = env_usize("EXCH_PUT", 1) != 0;
    assert!(use_put || use_atomic, "EXCH_PUT=0 needs EXCH_ATOMIC=1");
    let nodes = env_usize("EXCH_NODES", 0);
    let tasks = env_usize("EXCH_TASKS", 1).max(1);

    // slot `src` of the receive region holds a payload from PE `src`
    let recv = world
        .alloc_shared_mem_region::<u8>(num_pes * max_size)
        .block();
    let counters = world.alloc_shared_mem_region::<usize>(1).block();
    let data = world.alloc_one_sided_mem_region::<u8>(max_size);
    unsafe {
        for i in data.as_mut_slice() {
            *i = my_pe as u8;
        }
        counters.as_mut_slice()[0] = 0;
    }
    world.barrier();
    if my_pe == 0 {
        println!(
            "==================Random exchange test, {num_pes} PEs, atomic={use_atomic}==========================="
        );
    }
    let mut expected_count = 0usize;
    for &size in &sizes {
        let sends = if self_ok { num_pes } else { num_pes - 1 };
        let rounds = ((mb_per_pe << 20) / (sends * size)).max(1);
        // wipe the slots so a stale payload from a previous size cannot satisfy the check
        unsafe {
            for b in recv.as_mut_slice().iter_mut() {
                *b = 0xFF;
            }
        }
        world.barrier();
        let total = rounds * sends;
        let timer = Instant::now();
        if tasks == 1 {
            issue(
                &recv,
                &counters,
                &data,
                (my_pe, num_pes, size),
                (total, seed(my_pe, 0, size)),
                (use_put, use_atomic, self_ok),
            );
        } else {
            let handles: Vec<_> = (0..tasks)
                .map(|t| {
                    let (recv, counters, data) = (recv.clone(), counters.clone(), data.clone());
                    let n = share(total, tasks, t);
                    let sd = seed(my_pe, t, size);
                    world.spawn(async move {
                        issue(
                            &recv,
                            &counters,
                            &data,
                            (my_pe, num_pes, size),
                            (n, sd),
                            (use_put, use_atomic, self_ok),
                        );
                    })
                })
                .collect();
            for h in handles {
                h.block();
            }
        }
        world.wait_all();
        world.barrier();
        let t = timer.elapsed().as_secs_f64();
        // replay every PE's stream: how many messages should have reached this PE, and from whom
        let mut from = vec![0usize; num_pes];
        for src in 0..num_pes {
            if src == my_pe && !self_ok {
                continue;
            }
            for t in 0..tasks {
                let mut r = seed(src, t, size);
                for _ in 0..share(rounds * sends, tasks, t) {
                    if target(&mut r, src, num_pes, self_ok) == my_pe {
                        from[src] += 1;
                    }
                }
            }
        }
        if use_atomic {
            expected_count += from.iter().sum::<usize>();
            let got = unsafe { counters.as_slice()[0] };
            assert_eq!(
                got, expected_count,
                "PE {my_pe}: counter {got} != expected {expected_count} after size {size}"
            );
        }
        let slots = unsafe { recv.as_slice() };
        for src in 0..num_pes {
            let slot = &slots[src * size..(src + 1) * size];
            let want = if use_put && from[src] > 0 { src as u8 } else { 0xFF };
            assert!(
                slot.iter().all(|&b| b == want),
                "PE {my_pe}: slot for PE {src} is not {want:#x} at size {size}"
            );
        }
        if my_pe == 0 {
            let total_mib = (rounds * sends * size * num_pes) as f64 / 1048576.0;
            let per_node = if nodes > 0 {
                format!(" per_node: {:.0}MB/s", total_mib / t / nodes as f64)
            } else {
                String::new()
            };
            println!(
                "exchange size: {size}B rounds: {rounds} time: {t:.4}s aggregate: {:.0}MB/s per_pe: {:.0}MB/s{per_node} msgs: {:.3}M/s",
                total_mib / t,
                total_mib / t / num_pes as f64,
                (rounds * sends * num_pes) as f64 / t / 1e6
            );
        }
    }
    world.barrier();
    if my_pe == 0 {
        println!("exchange_bench: VERIFIED");
    }
}

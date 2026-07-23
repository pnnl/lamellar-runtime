/// AM Latency benchmark — sequential P2P ping (PE 0 → PE N-1)
///
/// Measures true per-message latency by sending one AM at a time and
/// blocking until completion. Reports min/p50/p95/p99/max per message size.
///
/// Two dispatch APIs are compared:
///   exec_am_pe().block()  — lazy: AM submitted inline at block() time from calling thread
///   spawn_am_pe().block() — eager: AM submitted to scheduler immediately, worker thread sends
///
/// Useful env vars to compare variants:
///   LAMELLAR_CMD_QUEUE=batched|get|geteager|getslots|put|putslots|puteager
///   LAMELLAR_AM_SIZE_THRESHOLD=0    (disable aggregation batching)
///   LAMELLAR_BATCHER=simple|direct|team_am
use lamellar::ActiveMessaging;
use std::time::Instant;

#[lamellar::AmData(Clone, Debug)]
struct LatencyAM {
    data: Vec<u8>,
}

#[lamellar::am]
impl LamellarAM for LatencyAM {
    async fn exec(self) {}
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 * p / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx] as f64 / 1000.0
}

#[lamellar::main]
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    if num_pes < 2 {
        eprintln!("Need at least 2 PEs");
        return;
    }

    let dst = num_pes - 1;

    // sizes: 0B through 32MB in powers of 4, plus a few intermediate points
    let sizes: &[usize] = &[
        0, 8, 64, 512, 4_096, 16_384, 65_536, 131_072, 524_288,
        1_048_576,
        // 4_194_304,
        // 16_777_216,
        // 33_554_432,
    ];

    const WARMUP: usize = 200;
    const ITERS: usize = 1000;

    world.barrier();

    if my_pe == 0 {
        let cq_variant = std::env::var("LAMELLAR_CMD_QUEUE").unwrap_or_else(|_| "get".to_owned());
        let threshold =
            std::env::var("LAMELLAR_AM_SIZE_THRESHOLD").unwrap_or_else(|_| "100000".to_owned());
        println!(
            "# AM latency  cmd_queue={cq_variant}  am_size_threshold={threshold}  \
             pe0->pe{dst}  iters={ITERS}"
        );
    }

    // ---- P2P: exec_am_pe().block() ----
    world.barrier();
    if my_pe == 0 {
        println!();
        println!("# P2P exec_am_pe().block()  (lazy: AM submitted inline at block() time)");
        println!(
            "# {:>12}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}",
            "size_B", "min_us", "p50_us", "p95_us", "p99_us", "max_us"
        );
    }
    world.barrier();

    for &size in sizes {
        let data = vec![42u8; size];

        if my_pe == 0 {
            for _ in 0..WARMUP {
                world
                    .exec_am_pe(dst, LatencyAM { data: data.clone() })
                    .block();
            }
        }
        world.barrier();

        let mut latencies: Vec<u64> = Vec::with_capacity(ITERS);
        if my_pe == 0 {
            for _ in 0..ITERS {
                let t = Instant::now();
                world
                    .exec_am_pe(dst, LatencyAM { data: data.clone() })
                    .block();
                latencies.push(t.elapsed().as_nanos() as u64);
            }
        }
        world.barrier();

        if my_pe == 0 {
            latencies.sort_unstable();
            println!(
                "  {:>12}  {:>9.2}  {:>9.2}  {:>9.2}  {:>9.2}  {:>9.2}",
                size,
                percentile(&latencies, 0.0),
                percentile(&latencies, 50.0),
                percentile(&latencies, 95.0),
                percentile(&latencies, 99.0),
                percentile(&latencies, 100.0),
            );
        }
        world.barrier();
    }

    // ---- P2P: spawn_am_pe().block() ----
    world.barrier();
    if my_pe == 0 {
        println!();
        println!("# P2P spawn_am_pe().block()  (eager: AM submitted to scheduler immediately, worker thread sends)");
        println!(
            "# {:>12}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}",
            "size_B", "min_us", "p50_us", "p95_us", "p99_us", "max_us"
        );
    }
    world.barrier();

    for &size in sizes {
        let data = vec![42u8; size];

        if my_pe == 0 {
            for _ in 0..WARMUP {
                world
                    .spawn_am_pe(dst, LatencyAM { data: data.clone() })
                    .block();
            }
        }
        world.barrier();

        let mut latencies: Vec<u64> = Vec::with_capacity(ITERS);
        if my_pe == 0 {
            for _ in 0..ITERS {
                let t = Instant::now();
                world
                    .spawn_am_pe(dst, LatencyAM { data: data.clone() })
                    .block();
                latencies.push(t.elapsed().as_nanos() as u64);
            }
        }
        world.barrier();

        if my_pe == 0 {
            latencies.sort_unstable();
            println!(
                "  {:>12}  {:>9.2}  {:>9.2}  {:>9.2}  {:>9.2}  {:>9.2}",
                size,
                percentile(&latencies, 0.0),
                percentile(&latencies, 50.0),
                percentile(&latencies, 95.0),
                percentile(&latencies, 99.0),
                percentile(&latencies, 100.0),
            );
        }
        world.barrier();
    }

    // ---- all-to-neighbor: exec_am_all().block() ----
    world.barrier();
    if my_pe == 0 {
        println!();
        println!("# All-to-neighbor exec_am_all().block()  (PE i -> PE (i+1)%N, concurrent)");
        println!(
            "# {:>12}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}",
            "size_B", "min_us", "p50_us", "p95_us", "p99_us", "max_us"
        );
    }
    world.barrier();

    let _neighbor = (my_pe + 1) % num_pes;

    for &size in &sizes[..8] {
        let data = vec![42u8; size];

        for _ in 0..WARMUP {
            world.exec_am_all(LatencyAM { data: data.clone() }).block();
        }
        world.barrier();

        let mut latencies: Vec<u64> = Vec::with_capacity(ITERS);
        for _ in 0..ITERS {
            let t = Instant::now();
            world.exec_am_all(LatencyAM { data: data.clone() }).block();
            latencies.push(t.elapsed().as_nanos() as u64);
        }
        world.barrier();

        if my_pe == 0 {
            latencies.sort_unstable();
            println!(
                "  {:>12}  {:>9.2}  {:>9.2}  {:>9.2}  {:>9.2}  {:>9.2}",
                size,
                percentile(&latencies, 0.0),
                percentile(&latencies, 50.0),
                percentile(&latencies, 95.0),
                percentile(&latencies, 99.0),
                percentile(&latencies, 100.0),
            );
        }
        world.barrier();
    }

    // ---- all-to-neighbor: spawn_am_all().block() ----
    world.barrier();
    if my_pe == 0 {
        println!();
        println!("# All-to-neighbor spawn_am_all().block()  (PE i -> PE (i+1)%N, concurrent)");
        println!(
            "# {:>12}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}",
            "size_B", "min_us", "p50_us", "p95_us", "p99_us", "max_us"
        );
    }
    world.barrier();

    for &size in &sizes[..8] {
        let data = vec![42u8; size];

        for _ in 0..WARMUP {
            world.spawn_am_all(LatencyAM { data: data.clone() }).block();
        }
        world.barrier();

        let mut latencies: Vec<u64> = Vec::with_capacity(ITERS);
        for _ in 0..ITERS {
            let t = Instant::now();
            world.spawn_am_all(LatencyAM { data: data.clone() }).block();
            latencies.push(t.elapsed().as_nanos() as u64);
        }
        world.barrier();

        if my_pe == 0 {
            latencies.sort_unstable();
            println!(
                "  {:>12}  {:>9.2}  {:>9.2}  {:>9.2}  {:>9.2}  {:>9.2}",
                size,
                percentile(&latencies, 0.0),
                percentile(&latencies, 50.0),
                percentile(&latencies, 95.0),
                percentile(&latencies, 99.0),
                percentile(&latencies, 100.0),
            );
        }
        world.barrier();
    }

    //test exec_*.spawn() + wait_all vs spawn_*() + wait_all for correctness and latency
    // ---- P2P: exec_am_pe().spawn() ----
    world.barrier();
    if my_pe == 0 {
        println!();
        println!("# P2P exec_am_pe().spawn()  (lazy: AM submitted inline at spawn() time)");
        println!(
            "# {:>12} {:>9}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}",
            "size_B", "wait_us", "min_us", "p50_us", "p95_us", "p99_us", "max_us"
        );
    }
    world.barrier();

    for &size in sizes {
        let data = vec![42u8; size];

        world.barrier();

        let mut latencies: Vec<u64> = Vec::with_capacity(ITERS + 1);
        if my_pe == 0 {
            let timer = Instant::now();
            for _ in 0..ITERS {
                let t = Instant::now();
                let _ = world
                    .exec_am_pe(dst, LatencyAM { data: data.clone() })
                    .spawn();
                latencies.push(t.elapsed().as_nanos() as u64);
            }
            world.wait_all();
            latencies.push(timer.elapsed().as_nanos() as u64);
        }
        world.barrier();

        if my_pe == 0 {
            let wait_all_time = latencies.pop().unwrap_or(0);
            latencies.sort_unstable();
            println!(
                "  {:>12} {:>9.2} {:>9.2}  {:>9.2}  {:>9.2}  {:>9.2}  {:>9.2}",
                size,
                wait_all_time as f64 / 1_000.0,
                percentile(&latencies, 0.0),
                percentile(&latencies, 50.0),
                percentile(&latencies, 95.0),
                percentile(&latencies, 99.0),
                percentile(&latencies, 100.0),
            );
        }
        world.barrier();
    }

    // ---- P2P: spawn_am_pe() ----
    world.barrier();
    if my_pe == 0 {
        println!();
        println!("# P2P spawn_am_pe() (eager: AM submitted to scheduler immediately, worker thread sends)");
        println!(
            "# {:>12} {:>9}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}",
            "size_B", "wait_us", "min_us", "p50_us", "p95_us", "p99_us", "max_us"
        );
    }
    world.barrier();

    for &size in sizes {
        let data = vec![42u8; size];

        world.barrier();

        let mut latencies: Vec<u64> = Vec::with_capacity(ITERS + 1);
        if my_pe == 0 {
            let timer = Instant::now();
            for _ in 0..ITERS {
                let t = Instant::now();
                let _ = world.spawn_am_pe(dst, LatencyAM { data: data.clone() });
                latencies.push(t.elapsed().as_nanos() as u64);
            }
            world.wait_all();
            latencies.push(timer.elapsed().as_nanos() as u64);
        }
        world.barrier();

        if my_pe == 0 {
            let wait_all_time = latencies.pop().unwrap_or(0);
            latencies.sort_unstable();
            println!(
                "  {:>12} {:>9.2} {:>9.2}  {:>9.2}  {:>9.2}  {:>9.2}  {:>9.2}",
                size,
                wait_all_time as f64 / 1_000.0,
                percentile(&latencies, 0.0),
                percentile(&latencies, 50.0),
                percentile(&latencies, 95.0),
                percentile(&latencies, 99.0),
                percentile(&latencies, 100.0),
            );
        }
        world.barrier();
    }

    // ---- all-to-neighbor: exec_am_all().spawn() ----
    world.barrier();
    if my_pe == 0 {
        println!();
        println!("# All-to-neighbor exec_am_all().spawn()  (PE i -> PE (i+1)%N, concurrent)");
        println!(
            "# {:>12} {:>9}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}",
            "size_B", "wait_us", "min_us", "p50_us", "p95_us", "p99_us", "max_us"
        );
    }
    world.barrier();

    let _neighbor = (my_pe + 1) % num_pes;

    for &size in &sizes[..8] {
        let data = vec![42u8; size];

        world.barrier();

        let mut latencies: Vec<u64> = Vec::with_capacity(ITERS);
        let timer = Instant::now();
        for _ in 0..ITERS {
            let t = Instant::now();
            let _ = world.exec_am_all(LatencyAM { data: data.clone() }).spawn();
            latencies.push(t.elapsed().as_nanos() as u64);
        }
        world.wait_all();
        let wait_all_time = timer.elapsed().as_nanos() as u64;
        world.barrier();

        if my_pe == 0 {
            latencies.sort_unstable();
            println!(
                "  {:>12} {:>9.2} {:>9.2}  {:>9.2}  {:>9.2}  {:>9.2}  {:>9.2}",
                size,
                wait_all_time as f64 / 1_000.0,
                percentile(&latencies, 0.0),
                percentile(&latencies, 50.0),
                percentile(&latencies, 95.0),
                percentile(&latencies, 99.0),
                percentile(&latencies, 100.0),
            );
        }
        world.barrier();
    }

    // ---- all-to-neighbor: spawn_am_all() ----
    world.barrier();
    if my_pe == 0 {
        println!();
        println!("# All-to-neighbor spawn_am_all() (PE i -> PE (i+1)%N, concurrent)");
        println!(
            "# {:>12} {:>9}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}",
            "size_B", "wait_us", "min_us", "p50_us", "p95_us", "p99_us", "max_us"
        );
    }
    world.barrier();

    for &size in &sizes[..8] {
        let data = vec![42u8; size];

        world.barrier();

        let mut latencies: Vec<u64> = Vec::with_capacity(ITERS);
        let timer = Instant::now();
        for _ in 0..ITERS {
            let t = Instant::now();
            let _ = world.spawn_am_all(LatencyAM { data: data.clone() });
            latencies.push(t.elapsed().as_nanos() as u64);
        }
        world.wait_all();
        let wait_all_time = timer.elapsed().as_nanos() as u64;
        world.barrier();

        if my_pe == 0 {
            latencies.sort_unstable();
            println!(
                "  {:>12} {:>9.2} {:>9.2}  {:>9.2}  {:>9.2}  {:>9.2}  {:>9.2}",
                size,
                wait_all_time as f64 / 1_000.0,
                percentile(&latencies, 0.0),
                percentile(&latencies, 50.0),
                percentile(&latencies, 95.0),
                percentile(&latencies, 99.0),
                percentile(&latencies, 100.0),
            );
        }
        world.barrier();
    }
}

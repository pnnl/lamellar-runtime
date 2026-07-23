/// Array operation latency benchmark — sequential P2P ops (PE 0 → PE N-1)
///
/// Part 1: RDMA put_buffer  — size sweep 0–1 MB (same as am_latency), per array type
/// Part 2: AM batch_store   — size sweep via index-list length, per array type
/// Part 3: AM batch_fetch_add — same size sweep, per array type
///
/// For batch_* ops the "size" is n_ops × sizeof(usize) bytes of index+value payload.
/// n_ops = size_B / sizeof(usize), matching the BUF_SIZES from am_latency.
///
/// Useful env vars:
///   LAMELLAR_CMD_QUEUE=batched|get|geteager|getslots|put|putslots|puteager
///   LAMELLAR_AM_SIZE_THRESHOLD=0    (disable aggregation batching)
use lamellar::array::prelude::*;
use lamellar::memregion::prelude::*;
use std::time::Instant;

const WARMUP: usize = 100;
const ITERS: usize = 200;

// Same sizes as am_latency (bytes).
const BUF_SIZES: &[usize] = &[
    0,
    8,
    64,
    512,
    4_096,
    // 16_384,
    // 65_536,
    // 131_072,
    // 524_288,
    // 1_048_576,
];
const MAX_SIZE: usize = 1_048_576;

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 * p / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx] as f64 / 1000.0
}

fn print_put_header(label: &str) {
    println!();
    println!("# RDMA put_buffer  {label}");
    println!(
        "# {:>12}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}",
        "size_B", "min_us", "p50_us", "p95_us", "p99_us", "max_us"
    );
}

fn print_am_header(label: &str) {
    println!();
    println!("# {label}");
    println!(
        "# {:>12}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}",
        "size_B", "min_us", "p50_us", "p95_us", "p99_us", "max_us"
    );
}

fn print_row(size: usize, latencies: &mut Vec<u64>) {
    latencies.sort_unstable();
    println!(
        "  {:>12}  {:>9.2}  {:>9.2}  {:>9.2}  {:>9.2}  {:>9.2}",
        size,
        percentile(latencies, 0.0),
        percentile(latencies, 50.0),
        percentile(latencies, 95.0),
        percentile(latencies, 99.0),
        percentile(latencies, 100.0),
    );
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

    // ---- u8 arrays for put_buffer size sweep (MAX_SIZE u8 elements per PE) ----
    let total_u8 = num_pes * MAX_SIZE;
    let remote_u8_base = dst * MAX_SIZE;

    let unsafe_u8 = UnsafeArray::<u8>::new(&world, total_u8, Distribution::Block).block();
    let atomic_u8 = AtomicArray::<u8>::new(&world, total_u8, Distribution::Block).block();
    let local_lock_u8 =
        LocalLockArray::<u8>::new(&world, total_u8, Distribution::Block).block();
    let global_lock_u8 =
        GlobalLockArray::<u8>::new(&world, total_u8, Distribution::Block).block();

    let src_buf = world.alloc_one_sided_mem_region::<u8>(MAX_SIZE);
    unsafe {
        for b in src_buf.as_mut_slice() {
            *b = 42;
        }
    }

    // ---- usize arrays for batch_store/batch_fetch_add size sweep ----
    // n_ops = MAX_SIZE / sizeof(usize) = 131072 elements per PE
    let usize_size = std::mem::size_of::<usize>();
    let max_n_ops = MAX_SIZE / usize_size;
    let total_usize = num_pes * max_n_ops;
    let remote_usize_base = dst * max_n_ops;

    let unsafe_usize =
        UnsafeArray::<usize>::new(&world, total_usize, Distribution::Block).block();
    let atomic_usize =
        AtomicArray::<usize>::new(&world, total_usize, Distribution::Block).block();
    let local_lock_usize =
        LocalLockArray::<usize>::new(&world, total_usize, Distribution::Block).block();
    let global_lock_usize =
        GlobalLockArray::<usize>::new(&world, total_usize, Distribution::Block).block();

    // Pre-build index lists for each size (reused across all batch ops)
    let indices_by_size: Vec<Vec<usize>> = BUF_SIZES
        .iter()
        .map(|&s| {
            let n = s / usize_size;
            (remote_usize_base..remote_usize_base + n).collect()
        })
        .collect();

    world.barrier();

    if my_pe == 0 {
        let cq_variant =
            std::env::var("LAMELLAR_CMD_QUEUE").unwrap_or_else(|_| "get".to_owned());
        let threshold = std::env::var("LAMELLAR_AM_SIZE_THRESHOLD")
            .unwrap_or_else(|_| "100000".to_owned());
        println!(
            "# Array operation latency  cmd_queue={cq_variant}  am_size_threshold={threshold}  \
             pe0->pe{dst}  iters={ITERS}"
        );
    }

    // =====================================================================
    // Part 1: RDMA put_buffer — size sweep per array type
    // =====================================================================

    // ---- UnsafeArray<u8> ----
    world.barrier();
    if my_pe == 0 {
        print_put_header("UnsafeArray<u8>.block()  — one-sided write");
    }
    world.barrier();
    for &size in BUF_SIZES {
        if my_pe == 0 {
            for _ in 0..WARMUP {
                unsafe {
                    unsafe_u8
                        .put_buffer(remote_u8_base, src_buf.sub_region(..size))
                        .block()
                };
            }
        }
        world.barrier();
        let mut lat = Vec::with_capacity(ITERS);
        if my_pe == 0 {
            for _ in 0..ITERS {
                let t = Instant::now();
                unsafe {
                    unsafe_u8
                        .put_buffer(remote_u8_base, src_buf.sub_region(..size))
                        .block()
                };
                lat.push(t.elapsed().as_nanos() as u64);
            }
            print_row(size, &mut lat);
        }
        world.barrier();
    }

    // ---- AtomicArray<u8> ----
    world.barrier();
    if my_pe == 0 {
        print_put_header("AtomicArray<u8>.block()  — one-sided write");
    }
    world.barrier();
    for &size in BUF_SIZES {
        if my_pe == 0 {
            for _ in 0..WARMUP {
                unsafe {
                    atomic_u8
                        .put_buffer(remote_u8_base, src_buf.sub_region(..size))
                        .block()
                };
            }
        }
        world.barrier();
        let mut lat = Vec::with_capacity(ITERS);
        if my_pe == 0 {
            for _ in 0..ITERS {
                let t = Instant::now();
                unsafe {
                    atomic_u8
                        .put_buffer(remote_u8_base, src_buf.sub_region(..size))
                        .block()
                };
                lat.push(t.elapsed().as_nanos() as u64);
            }
            print_row(size, &mut lat);
        }
        world.barrier();
    }

    // ---- LocalLockArray<u8> ----
    world.barrier();
    if my_pe == 0 {
        print_put_header("LocalLockArray<u8>.block()  — one-sided write");
    }
    world.barrier();
    for &size in BUF_SIZES {
        if my_pe == 0 {
            for _ in 0..WARMUP {
                local_lock_u8
                    .put_buffer(remote_u8_base, src_buf.sub_region(..size))
                    .block();
            }
        }
        world.barrier();
        let mut lat = Vec::with_capacity(ITERS);
        if my_pe == 0 {
            for _ in 0..ITERS {
                let t = Instant::now();
                local_lock_u8
                    .put_buffer(remote_u8_base, src_buf.sub_region(..size))
                    .block();
                lat.push(t.elapsed().as_nanos() as u64);
            }
            print_row(size, &mut lat);
        }
        world.barrier();
    }

    // ---- GlobalLockArray<u8> ----
    world.barrier();
    if my_pe == 0 {
        print_put_header("GlobalLockArray<u8>.block()  — one-sided write");
    }
    world.barrier();
    for &size in BUF_SIZES {
        if my_pe == 0 {
            for _ in 0..WARMUP {
                global_lock_u8
                    .put_buffer(remote_u8_base, src_buf.sub_region(..size))
                    .block();
            }
        }
        world.barrier();
        let mut lat = Vec::with_capacity(ITERS);
        if my_pe == 0 {
            for _ in 0..ITERS {
                let t = Instant::now();
                global_lock_u8
                    .put_buffer(remote_u8_base, src_buf.sub_region(..size))
                    .block();
                lat.push(t.elapsed().as_nanos() as u64);
            }
            print_row(size, &mut lat);
        }
        world.barrier();
    }

    // =====================================================================
    // Part 2: AM batch_store — size sweep via index-list length
    // =====================================================================

    // ---- UnsafeArray<usize> batch_store ----
    world.barrier();
    if my_pe == 0 {
        print_am_header(
            "AM batch_store(indices, 1).block()  UnsafeArray<usize>  — two-sided write",
        );
    }
    world.barrier();
    for (i, &size) in BUF_SIZES.iter().enumerate() {
        let idx = &indices_by_size[i];
        if my_pe == 0 {
            for _ in 0..WARMUP {
                unsafe { unsafe_usize.batch_store(idx.as_slice(), 1usize).block() };
            }
        }
        world.barrier();
        let mut lat = Vec::with_capacity(ITERS);
        if my_pe == 0 {
            for _ in 0..ITERS {
                let t = Instant::now();
                unsafe { unsafe_usize.batch_store(idx.as_slice(), 1usize).block() };
                lat.push(t.elapsed().as_nanos() as u64);
            }
            print_row(size, &mut lat);
        }
        world.barrier();
    }

    // ---- AtomicArray<usize> batch_store ----
    world.barrier();
    if my_pe == 0 {
        print_am_header(
            "AM batch_store(indices, 1).block()  AtomicArray<usize>  — two-sided atomic write",
        );
    }
    world.barrier();
    for (i, &size) in BUF_SIZES.iter().enumerate() {
        let idx = &indices_by_size[i];
        if my_pe == 0 {
            for _ in 0..WARMUP {
                atomic_usize.batch_store(idx.as_slice(), 1usize).block();
            }
        }
        world.barrier();
        let mut lat = Vec::with_capacity(ITERS);
        if my_pe == 0 {
            for _ in 0..ITERS {
                let t = Instant::now();
                atomic_usize.batch_store(idx.as_slice(), 1usize).block();
                lat.push(t.elapsed().as_nanos() as u64);
            }
            print_row(size, &mut lat);
        }
        world.barrier();
    }

    // ---- LocalLockArray<usize> batch_store ----
    world.barrier();
    if my_pe == 0 {
        print_am_header(
            "AM batch_store(indices, 1).block()  LocalLockArray<usize>  — two-sided write",
        );
    }
    world.barrier();
    for (i, &size) in BUF_SIZES.iter().enumerate() {
        let idx = &indices_by_size[i];
        if my_pe == 0 {
            for _ in 0..WARMUP {
                local_lock_usize.batch_store(idx.as_slice(), 1usize).block();
            }
        }
        world.barrier();
        let mut lat = Vec::with_capacity(ITERS);
        if my_pe == 0 {
            for _ in 0..ITERS {
                let t = Instant::now();
                local_lock_usize.batch_store(idx.as_slice(), 1usize).block();
                lat.push(t.elapsed().as_nanos() as u64);
            }
            print_row(size, &mut lat);
        }
        world.barrier();
    }

    // ---- GlobalLockArray<usize> batch_store ----
    world.barrier();
    if my_pe == 0 {
        print_am_header(
            "AM batch_store(indices, 1).block()  GlobalLockArray<usize>  — two-sided write",
        );
    }
    world.barrier();
    for (i, &size) in BUF_SIZES.iter().enumerate() {
        let idx = &indices_by_size[i];
        if my_pe == 0 {
            for _ in 0..WARMUP {
                global_lock_usize.batch_store(idx.as_slice(), 1usize).block();
            }
        }
        world.barrier();
        let mut lat = Vec::with_capacity(ITERS);
        if my_pe == 0 {
            for _ in 0..ITERS {
                let t = Instant::now();
                global_lock_usize.batch_store(idx.as_slice(), 1usize).block();
                lat.push(t.elapsed().as_nanos() as u64);
            }
            print_row(size, &mut lat);
        }
        world.barrier();
    }

    // =====================================================================
    // Part 3: AM batch_fetch_add — size sweep via index-list length
    // =====================================================================

    // ---- UnsafeArray<usize> batch_fetch_add ----
    world.barrier();
    if my_pe == 0 {
        print_am_header(
            "AM batch_fetch_add(indices, 1).block()  UnsafeArray<usize>  — two-sided RMW",
        );
    }
    world.barrier();
    for (i, &size) in BUF_SIZES.iter().enumerate() {
        let idx = &indices_by_size[i];
        if my_pe == 0 {
            for _ in 0..WARMUP {
                unsafe { unsafe_usize.batch_fetch_add(idx.as_slice(), 1usize).block() };
            }
        }
        world.barrier();
        let mut lat = Vec::with_capacity(ITERS);
        if my_pe == 0 {
            for _ in 0..ITERS {
                let t = Instant::now();
                unsafe { unsafe_usize.batch_fetch_add(idx.as_slice(), 1usize).block() };
                lat.push(t.elapsed().as_nanos() as u64);
            }
            print_row(size, &mut lat);
        }
        world.barrier();
    }

    // ---- AtomicArray<usize> batch_fetch_add ----
    world.barrier();
    if my_pe == 0 {
        print_am_header(
            "AM batch_fetch_add(indices, 1).block()  AtomicArray<usize>  — two-sided atomic RMW",
        );
    }
    world.barrier();
    for (i, &size) in BUF_SIZES.iter().enumerate() {
        let idx = &indices_by_size[i];
        if my_pe == 0 {
            for _ in 0..WARMUP {
                atomic_usize.batch_fetch_add(idx.as_slice(), 1usize).block();
            }
        }
        world.barrier();
        let mut lat = Vec::with_capacity(ITERS);
        if my_pe == 0 {
            for _ in 0..ITERS {
                let t = Instant::now();
                atomic_usize.batch_fetch_add(idx.as_slice(), 1usize).block();
                lat.push(t.elapsed().as_nanos() as u64);
            }
            print_row(size, &mut lat);
        }
        world.barrier();
    }

    // ---- LocalLockArray<usize> batch_fetch_add ----
    world.barrier();
    if my_pe == 0 {
        print_am_header(
            "AM batch_fetch_add(indices, 1).block()  LocalLockArray<usize>  — two-sided RMW",
        );
    }
    world.barrier();
    for (i, &size) in BUF_SIZES.iter().enumerate() {
        let idx = &indices_by_size[i];
        if my_pe == 0 {
            for _ in 0..WARMUP {
                local_lock_usize
                    .batch_fetch_add(idx.as_slice(), 1usize)
                    .block();
            }
        }
        world.barrier();
        let mut lat = Vec::with_capacity(ITERS);
        if my_pe == 0 {
            for _ in 0..ITERS {
                let t = Instant::now();
                local_lock_usize
                    .batch_fetch_add(idx.as_slice(), 1usize)
                    .block();
                lat.push(t.elapsed().as_nanos() as u64);
            }
            print_row(size, &mut lat);
        }
        world.barrier();
    }

    // ---- GlobalLockArray<usize> batch_fetch_add ----
    world.barrier();
    if my_pe == 0 {
        print_am_header(
            "AM batch_fetch_add(indices, 1).block()  GlobalLockArray<usize>  — two-sided RMW",
        );
    }
    world.barrier();
    for (i, &size) in BUF_SIZES.iter().enumerate() {
        let idx = &indices_by_size[i];
        if my_pe == 0 {
            for _ in 0..WARMUP {
                global_lock_usize
                    .batch_fetch_add(idx.as_slice(), 1usize)
                    .block();
            }
        }
        world.barrier();
        let mut lat = Vec::with_capacity(ITERS);
        if my_pe == 0 {
            for _ in 0..ITERS {
                let t = Instant::now();
                global_lock_usize
                    .batch_fetch_add(idx.as_slice(), 1usize)
                    .block();
                lat.push(t.elapsed().as_nanos() as u64);
            }
            print_row(size, &mut lat);
        }
        world.barrier();
    }
}

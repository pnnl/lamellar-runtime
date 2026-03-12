/// Benchmark comparing backend-native atomic execution vs active-messaging fallback.
///
/// Uses `usize` for likely hardware-assisted atomics and `UsizeWrap` for AM-path atomics,
/// reporting timings for each operation side by side.
///
/// Run with different backends:
/// - Local (single process):   cargo run --release --example network_atomics
/// - Shared memory (4 PEs):    ./lamellar_run.sh -N=4 ./target/release/examples/network_atomics
/// - Distributed (UCX):        srun -N 2 --mpi=pmi2 ./target/release/examples/network_atomics
use lamellar::array::prelude::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::time::Instant;

#[lamellar::AmData(
    Default,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    ArrayOps(Arithmetic, CompExEps, CompEx, Bitwise)
)]
struct UsizeWrap {
    val: usize,
}

impl std::ops::AddAssign for UsizeWrap {
    fn add_assign(&mut self, other: Self) {
        self.val += other.val;
    }
}

impl std::ops::SubAssign for UsizeWrap {
    fn sub_assign(&mut self, other: Self) {
        self.val -= other.val;
    }
}

impl std::ops::Sub for UsizeWrap {
    type Output = Self;
    fn sub(self, other: Self) -> Self {
        Self {
            val: self.val - other.val,
        }
    }
}

impl std::ops::MulAssign for UsizeWrap {
    fn mul_assign(&mut self, other: Self) {
        self.val *= other.val;
    }
}

impl std::ops::DivAssign for UsizeWrap {
    fn div_assign(&mut self, other: Self) {
        self.val /= other.val;
    }
}

impl std::ops::RemAssign for UsizeWrap {
    fn rem_assign(&mut self, other: Self) {
        self.val %= other.val;
    }
}

impl std::ops::BitAndAssign for UsizeWrap {
    fn bitand_assign(&mut self, other: Self) {
        self.val &= other.val;
    }
}

impl std::ops::BitOrAssign for UsizeWrap {
    fn bitor_assign(&mut self, other: Self) {
        self.val |= other.val;
    }
}

impl std::ops::BitXorAssign for UsizeWrap {
    fn bitxor_assign(&mut self, other: Self) {
        self.val ^= other.val;
    }
}

fn print_cmp_row(op: &str, hw: std::time::Duration, am: std::time::Duration) {
    let ratio = if hw.as_nanos() == 0 {
        0.0
    } else {
        am.as_secs_f64() / hw.as_secs_f64()
    };
    println!("{op:<24} hw={hw:?}  am={am:?}  am/hw={ratio:.2}x");
}

#[lamellar::main]
fn main() {
    let world = LamellarWorldBuilder::new().build();
    let num_pes = world.num_pes();
    let my_pe = world.my_pe();
    println!("=== Network Atomic Benchmark ===");
    println!("PE {}/{}: comparing likely HW atomics vs AM fallback\n", my_pe, num_pes);

    // ========== Hardware vs AM Timing ==========
    println!("\n--- Hardware vs AM Timing (usize vs UsizeWrap) ---");
    let bench_len = num_pes * 2048;
    let iters = 2048 * 8;
    let bench_usize = AtomicArray::<usize>::new(&world, bench_len, Distribution::Block).block();
    let bench_wrap = AtomicArray::<UsizeWrap>::new(&world, bench_len, Distribution::Block).block();

    if my_pe == 0 {
        bench_usize.print_network_atomic_avail();
        bench_wrap.print_network_atomic_avail();
    }

    bench_usize
        .dist_iter_mut()
        .for_each(move |elem| elem.store(0))
        .block();
    bench_wrap
        .dist_iter_mut()
        .for_each(move |elem| elem.store(UsizeWrap { val: 0 }))
        .block();
    world.barrier();

    let mut rng = StdRng::seed_from_u64(0x1234);
    let indices: Vec<usize> = (0..iters).map(|_| rng.random_range(0..bench_len)).collect();

    if my_pe == 0 {
        println!("Iterations: {iters}, array_len: {bench_len}");

        // load
        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.load(indices[i]).spawn();
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap.load(indices[i]).spawn();
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("load", hw, am);

        // store
        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.store(indices[i], (i as usize) + 1).spawn();
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap
                .store(indices[i], UsizeWrap { val: (i as usize) + 1 })
                .spawn();
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("store", hw, am);

        // swap
        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.swap(indices[i], (i as usize) + 7).spawn();
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap
                .swap(indices[i], UsizeWrap { val: (i as usize) + 7 })
                .spawn();
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("swap", hw, am);

        // add
        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.add(indices[i], 1).spawn();
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap.add(indices[i], UsizeWrap { val: 1 }).spawn();
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("add", hw, am);

        // fetch_add
        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.fetch_add(indices[i], 1).spawn();
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap.fetch_add(indices[i], UsizeWrap { val: 1 }).spawn();
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("fetch_add", hw, am);
    }
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(100)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 100 }))
        .block();
    if my_pe == 0 {
        // sub
        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.sub(indices[i], 1).spawn();
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap.sub(indices[i], UsizeWrap { val: 1 }).spawn();
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("sub", hw, am);

        // fetch_sub
        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.fetch_sub(indices[i], 1).spawn();
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap.fetch_sub(indices[i], UsizeWrap { val: 1 }).spawn();
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("fetch_sub", hw, am);
    }

    // mul
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(2)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 2 }))
        .block();

    if my_pe == 0 {

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.mul(indices[i], 3).spawn();
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap.mul(indices[i], UsizeWrap { val: 3 }).spawn();
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("mul", hw, am);
    }

    // fetch_mul
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(2)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 2 }))
        .block();

    if my_pe == 0 {

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.fetch_mul(indices[i], 3).spawn();
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap.fetch_mul(indices[i], UsizeWrap { val: 3 }).spawn();
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("fetch_mul", hw, am);
    }

    // bit_and
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(0xFFFF)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 0xFFFF }))
        .block();

    if my_pe == 0 {

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.bit_and(indices[i], 0x0FFF).spawn();
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap
                .bit_and(indices[i], UsizeWrap { val: 0x0FFF })
                .spawn();
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("bit_and", hw, am);
    }

    // fetch_bit_and
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(0xFFFF)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 0xFFFF }))
        .block();

    if my_pe == 0 {

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.fetch_bit_and(indices[i], 0x0FFF).spawn();
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap
                .fetch_bit_and(indices[i], UsizeWrap { val: 0x0FFF })
                .spawn();
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("fetch_bit_and", hw, am);
    }

    // bit_or
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(0)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 0 }))
        .block();

    if my_pe == 0 {

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.bit_or(indices[i], 0x0100).spawn();
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap
                .bit_or(indices[i], UsizeWrap { val: 0x0100 })
                .spawn();
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("bit_or", hw, am);
    }

    // fetch_bit_or
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(0)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 0 }))
        .block();

    if my_pe == 0 {

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.fetch_bit_or(indices[i], 0x0100).spawn();
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap
                .fetch_bit_or(indices[i], UsizeWrap { val: 0x0100 })
                .spawn();
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("fetch_bit_or", hw, am);
    }

    // bit_xor
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(0xAAAA)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 0xAAAA }))
        .block();

    if my_pe == 0 {

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.bit_xor(indices[i], 0x5555).spawn();
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap
                .bit_xor(indices[i], UsizeWrap { val: 0x5555 })
                .spawn();
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("bit_xor", hw, am);
    }

    // fetch_bit_xor
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(0xAAAA)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 0xAAAA }))
        .block();

    if my_pe == 0 {

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.fetch_bit_xor(indices[i], 0x5555).spawn();
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap
                .fetch_bit_xor(indices[i], UsizeWrap { val: 0x5555 })
                .spawn();
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("fetch_bit_xor", hw, am);
    }

    // compare_exchange
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(0)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 0 }))
        .block();

    if my_pe == 0 {

        let start = Instant::now();
        let mut expected_usize = vec![0usize; bench_len];
        for i in 0..iters {
            let idx = indices[i];
            let current = expected_usize[idx];
            let new_val = current + 1;
            let _ = bench_usize
                .compare_exchange(idx, current, new_val)
                .spawn()
                .block();
            expected_usize[idx] = new_val;
        }
        let hw = start.elapsed();

        let start = Instant::now();
        let mut expected_wrap = vec![UsizeWrap { val: 0 }; bench_len];
        for i in 0..iters {
            let idx = indices[i];
            let current = expected_wrap[idx];
            let new_val = UsizeWrap {
                val: current.val + 1,
            };
            let _ = bench_wrap
                .compare_exchange(idx, current, new_val)
                .spawn()
                .block();
            expected_wrap[idx] = new_val;
        }
        let am = start.elapsed();
        print_cmp_row("compare_exchange", hw, am);

        println!("\n--- Blocking API Timing (usize vs UsizeWrap) ---");

        // blocking_load
        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.blocking_load(indices[i]);
        }
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap.blocking_load(indices[i]);
        }
        let am = start.elapsed();
        print_cmp_row("blocking_load", hw, am);

        // blocking_store
        let start = Instant::now();
        for i in 0..iters {
            bench_usize.blocking_store(indices[i], (i as usize) + 1);
        }
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            bench_wrap.blocking_store(indices[i], UsizeWrap { val: (i as usize) + 1 });
        }
        let am = start.elapsed();
        print_cmp_row("blocking_store", hw, am);

        // blocking_swap
        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.blocking_swap(indices[i], (i as usize) + 7);
        }
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap.blocking_swap(indices[i], UsizeWrap { val: (i as usize) + 7 });
        }
        let am = start.elapsed();
        print_cmp_row("blocking_swap", hw, am);

        // blocking_add
        let start = Instant::now();
        for i in 0..iters {
            bench_usize.blocking_add(indices[i], 1);
        }
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            bench_wrap.blocking_add(indices[i], UsizeWrap { val: 1 });
        }
        let am = start.elapsed();
        print_cmp_row("blocking_add", hw, am);

        // blocking_fetch_add
        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.blocking_fetch_add(indices[i], 1);
        }
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap.blocking_fetch_add(indices[i], UsizeWrap { val: 1 });
        }
        let am = start.elapsed();
        print_cmp_row("blocking_fetch_add", hw, am);

        // blocking_sub
        let start = Instant::now();
        for i in 0..iters {
            bench_usize.blocking_sub(indices[i], 1);
        }
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            bench_wrap.blocking_sub(indices[i], UsizeWrap { val: 1 });
        }
        let am = start.elapsed();
        print_cmp_row("blocking_sub", hw, am);

        // blocking_fetch_sub
        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.blocking_fetch_sub(indices[i], 1);
        }
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap.blocking_fetch_sub(indices[i], UsizeWrap { val: 1 });
        }
        let am = start.elapsed();
        print_cmp_row("blocking_fetch_sub", hw, am);
    }

    // blocking_mul
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(2)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 2 }))
        .block();

    if my_pe == 0 {

        let start = Instant::now();
        for i in 0..iters {
            bench_usize.blocking_mul(indices[i], 3);
        }
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            bench_wrap.blocking_mul(indices[i], UsizeWrap { val: 3 });
        }
        let am = start.elapsed();
        print_cmp_row("blocking_mul", hw, am);
    }

    // blocking_fetch_mul
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(2)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 2 }))
        .block();

    if my_pe == 0 {

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.blocking_fetch_mul(indices[i], 3);
        }
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap.blocking_fetch_mul(indices[i], UsizeWrap { val: 3 });
        }
        let am = start.elapsed();
        print_cmp_row("blocking_fetch_mul", hw, am);
    }

    // blocking_bit_and
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(0xFFFF)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 0xFFFF }))
        .block();

    if my_pe == 0 {
        let start = Instant::now();
        for i in 0..iters {
            bench_usize.blocking_bit_and(indices[i], 0x0FFF);
        }
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            bench_wrap.blocking_bit_and(indices[i], UsizeWrap { val: 0x0FFF });
        }
        let am = start.elapsed();
        print_cmp_row("blocking_bit_and", hw, am);
    }

    // blocking_fetch_bit_and
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(0xFFFF)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 0xFFFF }))
        .block();

    if my_pe == 0 {
        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.blocking_fetch_bit_and(indices[i], 0x0FFF);
        }
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap.blocking_fetch_bit_and(indices[i], UsizeWrap { val: 0x0FFF });
        }
        let am = start.elapsed();
        print_cmp_row("blocking_fetch_bit_and", hw, am);
    }

    // blocking_bit_or
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(0)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 0 }))
        .block();

    if my_pe == 0 {
        let start = Instant::now();
        for i in 0..iters {
            bench_usize.blocking_bit_or(indices[i], 0x0100);
        }
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            bench_wrap.blocking_bit_or(indices[i], UsizeWrap { val: 0x0100 });
        }
        let am = start.elapsed();
        print_cmp_row("blocking_bit_or", hw, am);
    }

    // blocking_fetch_bit_or
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(0)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 0 }))
        .block();

    if my_pe == 0 {
        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.blocking_fetch_bit_or(indices[i], 0x0100);
        }
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap.blocking_fetch_bit_or(indices[i], UsizeWrap { val: 0x0100 });
        }
        let am = start.elapsed();
        print_cmp_row("blocking_fetch_bit_or", hw, am);
    }

    // blocking_bit_xor
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(0xAAAA)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 0xAAAA }))
        .block();

    if my_pe == 0 {
        let start = Instant::now();
        for i in 0..iters {
            bench_usize.blocking_bit_xor(indices[i], 0x5555);
        }
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            bench_wrap.blocking_bit_xor(indices[i], UsizeWrap { val: 0x5555 });
        }
        let am = start.elapsed();
        print_cmp_row("blocking_bit_xor", hw, am);
    }

    // blocking_fetch_bit_xor
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(0xAAAA)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 0xAAAA }))
        .block();

    if my_pe == 0 {
        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_usize.blocking_fetch_bit_xor(indices[i], 0x5555);
        }
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            let _ = bench_wrap.blocking_fetch_bit_xor(indices[i], UsizeWrap { val: 0x5555 });
        }
        let am = start.elapsed();
        print_cmp_row("blocking_fetch_bit_xor", hw, am);
    }

    // blocking_compare_exchange
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(0)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 0 }))
        .block();

    if my_pe == 0 {
        let start = Instant::now();
        let mut expected_usize = vec![0usize; bench_len];
        for i in 0..iters {
            let idx = indices[i];
            let current = expected_usize[idx];
            let new_val = current + 1;
            let _ = bench_usize.blocking_compare_exchange(idx, current, new_val);
            expected_usize[idx] = new_val;
        }
        let hw = start.elapsed();

        let start = Instant::now();
        let mut expected_wrap = vec![UsizeWrap { val: 0 }; bench_len];
        for i in 0..iters {
            let idx = indices[i];
            let current = expected_wrap[idx];
            let new_val = UsizeWrap {
                val: current.val + 1,
            };
            let _ = bench_wrap.blocking_compare_exchange(idx, current, new_val);
            expected_wrap[idx] = new_val;
        }
        let am = start.elapsed();
        print_cmp_row("blocking_compare_exchange", hw, am);

        println!("\n--- Unmanaged API Timing (non-returning ops only) ---");

        let start = Instant::now();
        for i in 0..iters {
            bench_usize.store_unmanaged(indices[i], (i as usize) + 1);
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            bench_wrap.store_unmanaged(indices[i], UsizeWrap { val: (i as usize) + 1 });
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("store_unmanaged", hw, am);

        let start = Instant::now();
        for i in 0..iters {
            bench_usize.add_unmanaged(indices[i], 1);
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            bench_wrap.add_unmanaged(indices[i], UsizeWrap { val: 1 });
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("add_unmanaged", hw, am);

        let start = Instant::now();
        for i in 0..iters {
            bench_usize.sub_unmanaged(indices[i], 1);
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            bench_wrap.sub_unmanaged(indices[i], UsizeWrap { val: 1 });
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("sub_unmanaged", hw, am);
    }

    // mul_unmanaged
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(2)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 2 }))
        .block();

    if my_pe == 0 {
        let start = Instant::now();
        for i in 0..iters {
            bench_usize.mul_unmanaged(indices[i], 3);
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            bench_wrap.mul_unmanaged(indices[i], UsizeWrap { val: 3 });
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("mul_unmanaged", hw, am);
    }

    // bit_and_unmanaged
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(0xFFFF)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 0xFFFF }))
        .block();

    if my_pe == 0 {
        let start = Instant::now();
        for i in 0..iters {
            bench_usize.bit_and_unmanaged(indices[i], 0x0FFF);
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            bench_wrap.bit_and_unmanaged(indices[i], UsizeWrap { val: 0x0FFF });
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("bit_and_unmanaged", hw, am);
    }

    // bit_or_unmanaged
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(0)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 0 }))
        .block();

    if my_pe == 0 {
        let start = Instant::now();
        for i in 0..iters {
            bench_usize.bit_or_unmanaged(indices[i], 0x0100);
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            bench_wrap.bit_or_unmanaged(indices[i], UsizeWrap { val: 0x0100 });
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("bit_or_unmanaged", hw, am);
    }

    // bit_xor_unmanaged
    let _ = bench_usize.dist_iter_mut().for_each(|e| e.store(0xAAAA)).block();
    let _ = bench_wrap
        .dist_iter_mut()
        .for_each(|e| e.store(UsizeWrap { val: 0xAAAA }))
        .block();

    if my_pe == 0 {
        let start = Instant::now();
        for i in 0..iters {
            bench_usize.bit_xor_unmanaged(indices[i], 0x5555);
        }
        bench_usize.wait_all();
        let hw = start.elapsed();

        let start = Instant::now();
        for i in 0..iters {
            bench_wrap.bit_xor_unmanaged(indices[i], UsizeWrap { val: 0x5555 });
        }
        bench_wrap.wait_all();
        let am = start.elapsed();
        print_cmp_row("bit_xor_unmanaged", hw, am);
    }

    world.barrier();

    println!("\n=== All network atomic operations completed ===");
}

/// Collective latency benchmark: world barrier, sum_all (all-reduce), gather_all (all-gather)
/// on a SharedMemoryRegion<u64>. Reports avg/min/max per op in microseconds (PE 0 prints).
/// Results are checked on every PE.
use lamellar::memregion::prelude::*;
use std::time::Instant;

fn iters_for(bytes: usize) -> usize {
    (((64 << 20) / bytes.max(64)).clamp(20, 2000)) as usize
}

#[lamellar::main]
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let max_elems = 1 << 17; // 1 MiB of u64
    let mr: SharedMemoryRegion<u64> =
        world.alloc_shared_mem_region(max_elems * num_pes).block();

    let report = |name: &str, bytes: usize, t: &mut Vec<f64>| {
        let n = t.len() as f64;
        let avg = t.iter().sum::<f64>() / n;
        let min = t.iter().cloned().fold(f64::MAX, f64::min);
        let max = t.iter().cloned().fold(0.0, f64::max);
        if my_pe == 0 {
            println!("{name:<10} {bytes:>9} avg {avg:>10.2} min {min:>10.2} max {max:>10.2}");
        }
    };

    world.barrier();
    let mut t = Vec::new();
    for _ in 0..5000 {
        let s = Instant::now();
        world.barrier();
        t.push(s.elapsed().as_secs_f64() * 1e6);
    }
    report("barrier", 0, &mut t);

    let mut elems = 1;
    while elems <= max_elems {
        let bytes = elems * 8;
        let iters = iters_for(bytes);
        let mut t = Vec::with_capacity(iters);
        for it in 0..iters {
            unsafe {
                for (i, x) in mr.as_mut_slice()[..elems].iter_mut().enumerate() {
                    *x = (my_pe + i + it) as u64;
                }
            }
            let s = Instant::now();
            let res = unsafe { mr.sum_all(0, elems).block() };
            t.push(s.elapsed().as_secs_f64() * 1e6);
            if it == 0 || it + 1 == iters {
                let base = (num_pes * (num_pes - 1) / 2) as u64;
                for (i, r) in res.iter().enumerate() {
                    let exp = base + (num_pes * (i + it)) as u64;
                    assert_eq!(*r, exp, "sum_all mismatch pe {my_pe} elems {elems} i {i}");
                }
            }
        }
        report("sum_all", bytes, &mut t);

        let mut t = Vec::with_capacity(iters);
        for it in 0..iters {
            unsafe {
                for (i, x) in mr.as_mut_slice()[..elems].iter_mut().enumerate() {
                    *x = (my_pe * 1_000_000 + i + it) as u64;
                }
            }
            let s = Instant::now();
            let res = unsafe { mr.gather_all(0, elems).block() };
            t.push(s.elapsed().as_secs_f64() * 1e6);
            if it == 0 || it + 1 == iters {
                assert_eq!(res.len(), elems * num_pes);
                for pe in 0..num_pes {
                    for i in 0..elems {
                        let exp = (pe * 1_000_000 + i + it) as u64;
                        assert_eq!(res[pe * elems + i], exp, "gather_all mismatch pe {my_pe} src {pe} i {i}");
                    }
                }
            }
        }
        report("gather_all", bytes, &mut t);
        elems *= 8;
    }
    world.barrier();
    if my_pe == 0 {
        println!("done");
    }
}

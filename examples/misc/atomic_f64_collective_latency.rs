/// F2 verification: latency of GenericAtomicArray<f64>::sum_all/min_all/max_all, before/after
/// collective_avail<T>() gates floats through the native UCC all-reduce path instead of the
/// manual fallback. Reports avg/min/max per op in microseconds (PE 0 prints).
use lamellar::array::prelude::*;
use std::time::Instant;

fn iters_for() -> usize {
    500
}

#[lamellar::main]
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let len = 16;

    let array: AtomicArray<f64> = AtomicArray::<f64>::new(world.team(), len * num_pes, Distribution::Block)
        .block();
    array
        .dist_iter()
        .for_each(move |x| x.store(my_pe as f64))
        .block();
    array.wait_all();
    array.barrier();

    let report = |name: &str, t: &mut Vec<f64>| {
        let n = t.len() as f64;
        let avg = t.iter().sum::<f64>() / n;
        let min = t.iter().cloned().fold(f64::MAX, f64::min);
        let max = t.iter().cloned().fold(0.0, f64::max);
        if my_pe == 0 {
            println!("{name:<10} avg {avg:>10.2}us min {min:>10.2}us max {max:>10.2}us");
        }
    };

    let iters = iters_for();

    let mut t = Vec::with_capacity(iters);
    for _ in 0..iters {
        let s = Instant::now();
        let _res = unsafe { array.sum_all(0, len).block() };
        t.push(s.elapsed().as_secs_f64() * 1e6);
    }
    report("sum_all", &mut t);

    let mut t = Vec::with_capacity(iters);
    for _ in 0..iters {
        let s = Instant::now();
        let _res = unsafe { array.min_all(0, len).block() };
        t.push(s.elapsed().as_secs_f64() * 1e6);
    }
    report("min_all", &mut t);

    let mut t = Vec::with_capacity(iters);
    for _ in 0..iters {
        let s = Instant::now();
        let _res = unsafe { array.max_all(0, len).block() };
        t.push(s.elapsed().as_secs_f64() * 1e6);
    }
    report("max_all", &mut t);

    world.barrier();
    if my_pe == 0 {
        println!("done");
    }
}

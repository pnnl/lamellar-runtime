/// ------------Lamellar Test: SharedMemoryRegion subregion collective reduce -------------------
/// Verifies that sum_all/min_all/max_all/gather_all/gather_at_pe/broadcast_all/broadcast_from_pe
/// on a SharedMemoryRegion sub_region() correctly apply the subregion's offset (regression test
/// for the missing self.sub_region_offset bug).
/// -----------------------------------------------------------------------------------------------
use lamellar::memregion::prelude::*;

macro_rules! close_enough {
    ($expected:expr, $got:expr) => {
        (($expected as f64) - ($got as f64)).abs() <= 0.0001
    };
}

macro_rules! subregion_test {
    ($t:ty, $len:expr, $start:expr, $end:expr) => {{
        let world = lamellar::LamellarWorldBuilder::new().build();
        let num_pes = world.num_pes();
        let my_pe = world.my_pe();
        let len: usize = $len;
        let start: usize = $start;
        let end: usize = $end;
        let mut success = true;

        let mem_region: SharedMemoryRegion<$t> = world.alloc_shared_mem_region(len).block();
        {
            let slice = unsafe { mem_region.as_mut_slice() };
            for i in 0..len {
                slice[i] = (i as $t) * (1000 as $t) + (my_pe as $t);
            }
        }
        world.barrier();

        let sub = mem_region.sub_region(start..end);
        let sub_len = end - start;

        let sum_buf = unsafe { sub.sum_all(0, sub_len).block() };
        let min_buf = unsafe { sub.min_all(0, sub_len).block() };
        let max_buf = unsafe { sub.max_all(0, sub_len).block() };

        for j in 0..sub_len {
            let i = start + j;
            let base = (i as $t) * (1000 as $t);
            let expected_sum: $t = (0..num_pes).map(|p| base + (p as $t)).sum();
            let expected_min = base;
            let expected_max = base + ((num_pes - 1) as $t);

            let s = sum_buf.as_slice()[j];
            let mn = min_buf.as_slice()[j];
            let mx = max_buf.as_slice()[j];
            if !close_enough!(expected_sum, s) {
                eprintln!(
                    "sum_all subregion mismatch at j={:?} i={:?}: got {:?} expected {:?}",
                    j, i, s, expected_sum
                );
                success = false;
            }
            if !close_enough!(expected_min, mn) {
                eprintln!(
                    "min_all subregion mismatch at j={:?} i={:?}: got {:?} expected {:?}",
                    j, i, mn, expected_min
                );
                success = false;
            }
            if !close_enough!(expected_max, mx) {
                eprintln!(
                    "max_all subregion mismatch at j={:?} i={:?}: got {:?} expected {:?}",
                    j, i, mx, expected_max
                );
                success = false;
            }
        }

        // exercise a second, compounding offset: index into the subregion itself
        if sub_len > 2 {
            let inner_len = sub_len - 2;
            let sum_buf2 = unsafe { sub.sum_all(2, inner_len).block() };
            for j in 0..inner_len {
                let i = start + 2 + j;
                let base = (i as $t) * (1000 as $t);
                let expected_sum: $t = (0..num_pes).map(|p| base + (p as $t)).sum();
                let s = sum_buf2.as_slice()[j];
                if !close_enough!(expected_sum, s) {
                    eprintln!(
                        "sum_all subregion (inner offset) mismatch at j={:?} i={:?}: got {:?} expected {:?}",
                        j, i, s, expected_sum
                    );
                    success = false;
                }
            }
        }

        // gather_all: every PE should see every PE's sub_len elements, in PE order
        world.barrier();
        let gather_all_buf = unsafe { sub.gather_all(0, sub_len).block() };
        for pe in 0..num_pes {
            for j in 0..sub_len {
                let i = start + j;
                let expected = (i as $t) * (1000 as $t) + (pe as $t);
                let got = gather_all_buf[pe * sub_len + j];
                if !close_enough!(expected, got) {
                    eprintln!(
                        "gather_all subregion mismatch at pe={:?} j={:?} i={:?}: got {:?} expected {:?}",
                        pe, j, i, got, expected
                    );
                    success = false;
                }
            }
        }

        // gather_at_pe: only the root PE should receive the gathered data
        world.barrier();
        let root_pe = 0;
        let gather_root_buf = unsafe { sub.gather_at_pe(0, sub_len, root_pe).block() };
        if my_pe == root_pe {
            match gather_root_buf {
                Some(buf) => {
                    for pe in 0..num_pes {
                        for j in 0..sub_len {
                            let i = start + j;
                            let expected = (i as $t) * (1000 as $t) + (pe as $t);
                            let got = buf[pe * sub_len + j];
                            if !close_enough!(expected, got) {
                                eprintln!(
                                    "gather_at_pe subregion mismatch at pe={:?} j={:?} i={:?}: got {:?} expected {:?}",
                                    pe, j, i, got, expected
                                );
                                success = false;
                            }
                        }
                    }
                }
                None => {
                    eprintln!("gather_at_pe: root PE received None");
                    success = false;
                }
            }
        } else if gather_root_buf.is_some() {
            eprintln!("gather_at_pe: non-root PE unexpectedly received Some(_)");
            success = false;
        }

        // broadcast_from_pe: every non-root PE should receive the root's sub_len elements
        world.barrier();
        let bcast_root_pe = 0;
        let bcast_input = if my_pe == bcast_root_pe {
            lamellar::BroadcastInput::root(0)
        } else {
            lamellar::BroadcastInput::not_root(bcast_root_pe)
        };
        let bcast_buf = unsafe { sub.broadcast_from_pe(bcast_input, sub_len).block() };
        if my_pe == bcast_root_pe {
            if bcast_buf.is_some() {
                eprintln!("broadcast_from_pe: root PE unexpectedly received Some(_)");
                success = false;
            }
        } else {
            match bcast_buf {
                Some(buf) => {
                    for j in 0..sub_len {
                        let i = start + j;
                        let expected = (i as $t) * (1000 as $t) + (bcast_root_pe as $t);
                        let got = buf[j];
                        if !close_enough!(expected, got) {
                            eprintln!(
                                "broadcast_from_pe subregion mismatch at j={:?} i={:?}: got {:?} expected {:?}",
                                j, i, got, expected
                            );
                            success = false;
                        }
                    }
                }
                None => {
                    eprintln!("broadcast_from_pe: non-root PE received None");
                    success = false;
                }
            }
        }

        // confirm elements outside the subregion were untouched
        world.barrier();
        {
            let slice = unsafe { mem_region.as_slice() };
            for i in 0..len {
                if i >= start && i < end {
                    continue;
                }
                let expected = (i as $t) * (1000 as $t) + (my_pe as $t);
                if !close_enough!(expected, slice[i]) {
                    eprintln!(
                        "data outside subregion corrupted at i={:?}: got {:?} expected {:?}",
                        i, slice[i], expected
                    );
                    success = false;
                }
            }
        }

        world.barrier();
        if !success {
            eprintln!("failed");
        }
    }};
}

#[lamellar::main]
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let elem = args[1].clone();
    let len = args[2].parse::<usize>().unwrap();
    let start = args[3].parse::<usize>().unwrap();
    let end = args[4].parse::<usize>().unwrap();

    match elem.as_str() {
        "i32" => subregion_test!(i32, len, start, end),
        "i64" => subregion_test!(i64, len, start, end),
        "u32" => subregion_test!(u32, len, start, end),
        "u64" => subregion_test!(u64, len, start, end),
        "f32" => subregion_test!(f32, len, start, end),
        "f64" => subregion_test!(f64, len, start, end),
        _ => eprintln!("unsupported element type"),
    }
}

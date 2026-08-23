/// ------------Lamellar Bandwidth: unsafearray store (custom 1-byte type) ---
/// Same as unsafe_array_store_bw.rs but uses a 1-byte user-defined type
/// instead of u8. TypeId::of::<MyByte>() never matches ScalarType::get_type,
/// so MultiValSingleIndex::into_am always takes the "user defined type"
/// branch (MULTI_VAL_SINGLE_IDX_OPS dispatch table / old codegen path)
/// instead of the newer scalar_impls.rs fast path -- used to isolate
/// whether the scalar-fast-path's idx_vals() (chunk.to_vec() per byte)
/// is responsible for the flat ~70-80MB/s ceiling seen with plain u8.
/// --------------------------------------------------------------------
use lamellar::array::prelude::*;
use lamellar::memregion::prelude::*;
use std::time::Instant;

const ARRAY_LEN: usize = 1024 * 1024 * 1024;

#[lamellar::AmData(Default, Debug, ArrayOps(Arithmetic), PartialEq, PartialOrd)]
struct MyByte {
    v: u8,
}

impl std::ops::AddAssign for MyByte {
    fn add_assign(&mut self, other: MyByte) {
        self.v = self.v.wrapping_add(other.v);
    }
}
impl std::ops::SubAssign for MyByte {
    fn sub_assign(&mut self, other: MyByte) {
        self.v = self.v.wrapping_sub(other.v);
    }
}
impl std::ops::MulAssign for MyByte {
    fn mul_assign(&mut self, other: MyByte) {
        self.v = self.v.wrapping_mul(other.v);
    }
}
impl std::ops::DivAssign for MyByte {
    fn div_assign(&mut self, other: MyByte) {
        self.v = self.v.wrapping_div(other.v.max(1));
    }
}
impl std::ops::RemAssign for MyByte {
    fn rem_assign(&mut self, other: MyByte) {
        self.v = self.v.wrapping_rem(other.v.max(1));
    }
}

#[lamellar::main]
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let array: UnsafeArray<MyByte> =
        UnsafeArray::new(&world, ARRAY_LEN * num_pes, Distribution::Block).block();
    let data = world.alloc_one_sided_mem_region::<MyByte>(ARRAY_LEN);
    unsafe {
        for i in data.as_mut_slice() {
            *i = MyByte { v: my_pe as u8 };
        }
        array
            .dist_iter_mut()
            .for_each(move |elem| *elem = MyByte { v: num_pes as u8 })
            .block();
    }
    array.wait_all();
    array.barrier();

    world.barrier();
    let s = Instant::now();
    world.barrier();
    let b = s.elapsed().as_secs_f64();
    println!("Barrier latency: {:?}s {:?}us", b, b * 1_000_000 as f64);

    if my_pe == 0 {
        println!("==================Bandwidth test===========================");
    }
    let mut bws = vec![];
    // focus sweep on sizes at/above am_size_threshold (default 100000B, ~2^17)
    // where multi_val_one_index chunking kicks in -- skip tiny latency-bound sizes
    for i in 0..30 {
        let num_bytes = 2_u64.pow(i);
        let old: f64 = world.MB_sent();
        let mbs_o = world.MB_sent();
        let mut sum = 0;
        let mut cnt = 0;
        let mut exp = 20;
        if num_bytes <= 2048 {
            exp = 18 + i;
        } else if num_bytes >= 4096 {
            exp = 30;
        }
        let timer = Instant::now();
        let mut sub_time = 0f64;
        if my_pe == 0 {
            for j in (0..2_u64.pow(exp) as usize).step_by(num_bytes as usize) {
                let sub_timer = Instant::now();
                let sub_reg = data.sub_region(j..(j + num_bytes as usize));

                let _ = unsafe {
                    array
                        .batch_store(ARRAY_LEN * (num_pes - 1), sub_reg.as_slice())
                        .spawn()
                };
                sub_time += sub_timer.elapsed().as_secs_f64();
                sum += num_bytes * 1 as u64;
                cnt += 1;
            }
            println!("issue time: {:?}", timer.elapsed());
            world.wait_all();
        }
        world.barrier();
        let cur_t = timer.elapsed().as_secs_f64();
        let cur: f64 = world.MB_sent();
        let mbs_c = world.MB_sent();
        if my_pe == 0 {
            println!(
            "tx_size: {:?}B num_tx: {:?} num_bytes: {:?}MB time: {:?} ({:?}) throughput (avg): {:?}MB/s (cuml): {:?}MB/s total_bytes (w/ overhead){:?}MB throughput (w/ overhead){:?} ({:?}) latency: {:?}us",
            num_bytes, //transfer size
            cnt,  //num transfers
            sum as f64/ 1048576.0,
            cur_t, //transfer time
            sub_time,
            (sum as f64 / 1048576.0) / cur_t, // throughput of user payload
            ((sum*(num_pes-1) as u64) as f64 / 1048576.0) / cur_t,
            cur - old, //total bytes sent including overhead
            (cur - old) as f64 / cur_t, //throughput including overhead
            (mbs_c -mbs_o )/ cur_t,
            (cur_t/cnt as f64) * 1_000_000 as f64 ,
        );
        }
        bws.push((sum as f64 / 1048576.0) / cur_t);
        unsafe {
            for j in data.as_mut_slice().iter_mut() {
                *j = MyByte { v: my_pe as u8 };
            }
        }
        world.barrier();
    }
    if my_pe == 0 {
        println!(
            "bandwidths: {}",
            bws.iter()
                .fold(String::new(), |acc, &num| acc + &num.to_string() + ", ")
        );
    }
    world.barrier();
}

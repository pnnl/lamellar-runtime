/// ------------Lamellar Bandwidth: RDMA Put  -------------------------
/// Test the bandwidth between two PEs using an RDMA Put of N bytes
/// from a local array into a remote PE.
/// --------------------------------------------------------------------
use lamellar::memregion::prelude::*;
use std::time::Instant;

const ARRAY_LEN: usize = 2 * 1024 * 1024 * 1024;

#[lamellar::main]
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let array = world.alloc_shared_mem_region::<u8>(ARRAY_LEN).block();
    let data = world.alloc_one_sided_mem_region::<u8>(ARRAY_LEN);
    unsafe {
        for i in data.as_mut_slice() {
            *i = my_pe as u8;
        }
        for i in array.as_mut_slice() {
            *i = 255 as u8;
        }
    }

    world.barrier();
    let s = Instant::now();
    world.barrier();
    let b = s.elapsed().as_secs_f64();
    println!("Barrier latency: {:?}s {:?}us", b, b * 1_000_000 as f64);

    if my_pe == 0 {
        println!("==================Bandwidth test===========================");
    }
    let mut bws = vec![];
    // The per-put clock reads serialize the CPU and dominate small-put loops; PUT_NO_SUBTIME=1 skips them (the second time in each result line then reads 0).
    let sub_timing = std::env::var_os("PUT_NO_SUBTIME").is_none();
    // PUT_SIZE=<bytes> [PUT_ROUNDS=<n>] repeats one size instead of sweeping 1 B..512 MiB (and skips the array reset), for profiling.
    let fixed_size: Option<u64> = std::env::var("PUT_SIZE").ok().and_then(|v| v.parse().ok());
    let rounds: u32 = match fixed_size {
        Some(_) => std::env::var("PUT_ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(20),
        None => 30,
    };
    for i in 0..rounds {
        let num_bytes = fixed_size.unwrap_or(2_u64.pow(i));
        let old: f64 = world.MB_sent();
        let mbs_o = world.MB_sent();
        let mut sum = 0;
        let mut cnt = 0;
        let mut exp = 20;
        if num_bytes <= 2048 {
            exp = 18 + if fixed_size.is_some() { num_bytes.ilog2() } else { i };
        } else if num_bytes >= 4096 {
            exp = 30;
        }
        let timer = Instant::now();
        let mut sub_time = 0f64;
        if my_pe == 0 {
            for j in (0..2_u64.pow(exp) as usize).step_by(num_bytes as usize) {
                let sub_timer = sub_timing.then(Instant::now);
                unsafe {
                    let _ = array.put_buffer_unmanaged(
                        num_pes - 1,
                        j,
                        data.sub_region(..num_bytes as usize),
                    );
                }

                // println!("j: {:?}",j);
                // unsafe { array.put_slice(num_pes - 1, j, &data[..num_bytes as usize]) };
                if let Some(t) = sub_timer {
                    sub_time += t.elapsed().as_secs_f64();
                }
                sum += num_bytes * 1 as u64;
                cnt += 1;
            }
            println!("issue time: {:?}", timer.elapsed());
            array.wait_all();
        }
        // if my_pe == num_pes - 1 {
        //     let array_slice = unsafe { array.as_slice() };
        //     // TODO: Not Needed
        //     for j in (0..2_u64.pow(exp) as usize).step_by(num_bytes as usize) {
        //         while *(&array_slice[(j + num_bytes as usize) - 1]) != 0 as u8 {
        //             std::thread::yield_now()
        //         }
        //     }
        // }
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
        if fixed_size.is_none() {
            unsafe {
                for i in array.as_mut_slice() {
                    *i = 255 as u8;
                }
            };
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

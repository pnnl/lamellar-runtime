/// ------------Lamellar Bandwidth: RDMA Get  -------------------------
/// Test the bandwidth between two PEs using an RDMA get of N bytes
/// from a remote PE to a local mem_reg.
/// --------------------------------------------------------------------
use lamellar::memregion::prelude::*;
use std::time::Instant;

const MEMREG_LEN: usize = 2 * 1024 * 1024 * 1024;

fn main() {
    let mut s = Instant::now();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let mem_reg = world.alloc_shared_mem_region::<u8>(MEMREG_LEN).block();
    let data = world.alloc_one_sided_mem_region::<u8>(MEMREG_LEN);
    for j in 0..MEMREG_LEN as usize {
        unsafe {
            data.as_mut_slice().unwrap()[j] = my_pe as u8;
            mem_reg.as_mut_slice().unwrap()[j] = num_pes as u8;
        }
    }

    println!(
        "init: {:?}s {:?}us",
        s.elapsed().as_secs_f64(),
        s.elapsed().as_secs_f64() * 1_000_000 as f64
    );

    world.barrier();
    s = Instant::now();
    world.barrier();
    let b = s.elapsed().as_secs_f64();
    println!("Barrier latency: {:?}s {:?}us", b, b * 1_000_000 as f64);
    world.barrier();
    if my_pe == 0 {
        println!("==================Bandwidth test===========================");
    }
    let mut bws = vec![];

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
                // if j % 1024 ==0 {
                // println!("[{:?}] j: {:?}",my_pe, j);
                // }
                let sub_timer = Instant::now();
                unsafe {
                    mem_reg.get_unchecked(
                        num_pes - 1,
                        0,
                        data.sub_region(j..(j + num_bytes as usize)),
                    )
                };
                sub_time += sub_timer.elapsed().as_secs_f64();
                sum += num_bytes * 1 as u64;
                cnt += 1;
            }
            println!("issue time: {:?}", timer.elapsed());
            world.wait_all();
        }
        let data_slice = unsafe { data.as_slice().unwrap() };
        if my_pe == 0 {
            for j in (0..2_u64.pow(exp) as usize).step_by(num_bytes as usize) {
                while data_slice[(j + num_bytes as usize) - 1] == my_pe as u8 {
                    std::thread::yield_now()
                }
            }
        }
        let cur_t = timer.elapsed().as_secs_f64();
        world.barrier();
        // let cur_t = timer.elapsed().as_secs_f64();
        let cur: f64 = world.MB_sent();
        let mbs_c = world.MB_sent();
        if my_pe == 0 {
            for j in 0..2_u64.pow(exp) as usize {
                if data_slice[j] == my_pe as u8 {
                    println!("Error: {:?} {:?}", j, data_slice[j]);
                    break;
                }
            }
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
            for j in data.as_mut_slice().unwrap().iter_mut() {
                *j = my_pe as u8;
            }
        }

        world.barrier();
        // println!(
        //     "cleanup: {:?}s {:?}us",
        //     s.elapsed().as_secs_f64(),
        //     s.elapsed().as_secs_f64() * 1_000_000 as f64
        // );
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

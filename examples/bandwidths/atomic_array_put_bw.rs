/// ------------Lamellar Bandwidth: RDMA Put  -------------------------
/// Test the bandwidth between two PEs using an RDMA Put of N bytes
/// from a local array into a remote PE.
/// --------------------------------------------------------------------
use lamellar::array::prelude::*;
use lamellar::memregion::prelude::*;
use std::time::Instant;

const ARRAY_LEN: usize = 1024 * 1024 * 1024;

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let array: LocalLockArray<u8> =
        LocalLockArray::new(&world, ARRAY_LEN * num_pes, Distribution::Block);
    let data = world.alloc_one_sided_mem_region::<u8>(ARRAY_LEN);
    unsafe {
        for i in data.as_mut_slice().unwrap() {
            *i = my_pe as u8;
        }
    }
    array
        .dist_iter_mut()
        .for_each(move |elem| *elem = 255 as u8)
        .block(); //this is can be pretty slow for atomic arrays as we perform an atomic store for 2^30 elements, local lock tends to perform better
    let mut array = array.into_atomic(); //so we simply convert the LocalLockArray array to atomic after initalization

    world.barrier();
    let s = Instant::now();
    world.barrier();
    let b = s.elapsed().as_secs_f64();
    println!("Barrier latency: {:?}s {:?}us", b, b * 1_000_000 as f64);

    if my_pe == 0 {
        println!("==================Bandwidth test===========================");
    }
    let mut bws = vec![];
    for i in 0..27 {
        let num_bytes = 2_u64.pow(i);
        let old: f64 = world.MB_sent();
        let mbs_o = world.MB_sent();
        let mut sum = 0;
        let mut cnt = 0;
        let timer = Instant::now();
        let mut sub_time = 0f64;
        let mut exp = 20;
        if num_bytes <= 2048 {
            exp = 18 + i;
        } else if num_bytes >= 4096 {
            exp = 30;
        }
        if my_pe == 0 {
            for j in (0..2_u64.pow(exp) as usize).step_by(num_bytes as usize) {
                let sub_timer = Instant::now();
                let sub_reg = data.sub_region(..num_bytes as usize);
                let _ = unsafe { array.put(ARRAY_LEN * (num_pes - 1) + j, sub_reg) };
                // println!("j: {:?}",j);
                // unsafe { array.put_slice(num_pes - 1, j, &data[..num_bytes as usize]) };
                sub_time += sub_timer.elapsed().as_secs_f64();
                sum += num_bytes * 1 as u64;
                cnt += 1;
            }
            println!("issue time: {:?}", timer.elapsed());
            array.wait_all();
        }
        array.barrier();
        let cur_t = timer.elapsed().as_secs_f64();
        if my_pe == num_pes - 1 {
            let array_data = array.mut_local_data();
            for j in (0..2_u64.pow(exp) as usize).step_by(num_bytes as usize) {
                while array_data.at((j + num_bytes as usize) - 1).load() == 255 as u8 {
                    println!(
                        "this should not happen {:?}",
                        array_data.at((j + num_bytes as usize) - 1).load()
                    );
                    std::thread::yield_now()
                }
            }
        }
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

        // unsafe {
        //     for i in array.mut_local_data() {
        //         i.store(255 as u8);
        //     }
        // };
        let temp = array.into_local_lock();
        temp.dist_iter_mut()
            .for_each(move |elem| *elem = 255 as u8)
            .block(); //this is pretty slow for atomic arrays as we perform an atomic store for 2^30 elements
        array = temp.into_atomic();
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

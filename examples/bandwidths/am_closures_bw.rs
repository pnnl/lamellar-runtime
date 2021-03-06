/// ------------Lamellar Bandwidth: Remote Closures  -------------------------
/// Test the bandwidth between two PEs using remote closures which
/// contains a vector of N bytes
/// this example requires nightly rust
/// --------------------------------------------------------------------
use lamellar::{ActiveMessaging, RemoteClosures};
use std::time::Instant;

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    world.barrier();
    let s = Instant::now();
    world.barrier();
    let b = s.elapsed().as_secs_f64();
    println!("Barrier latency: {:?}s {:?}us", b, b * 1_000_000 as f64);

    if my_pe == 0 {
        println!("==================Bandwidth test===========================");
    }
    let mut bws = vec![];
    for i in 0..26 {
        let num_bytes = 2_u64.pow(i);
        let mut _data: std::vec::Vec<u8> = Vec::new(); //vec![; num_bytes as usize];
        for i in 0..num_bytes {
            _data.push(i as u8);
        }

        let old: f64 = world.MB_sent().iter().sum();
        let mut sum = 0;
        let mut cnt = 0;
        let mut exp = 20;
        if num_bytes <= 8 {
            exp = 18;
        } else if num_bytes >= 4096 {
            exp = 30;
        }

        let timer = Instant::now();
        let mut sub_time = 0f64;
        if my_pe == 0 {
            for _j in (num_bytes..(2_u64.pow(exp))).step_by(num_bytes as usize) {
                let _d = _data.clone();
                let sub_timer = Instant::now();
                world.exec_closure_pe(num_pes - 1, lamellar::FnOnce!([_d] move || {} )); //we explicity are capturing _data and transfer it even though we do nothing with it
                sub_time += sub_timer.elapsed().as_secs_f64();
                sum += num_bytes * 1 as u64;
                cnt += 1;
            }
            println!("issue time: {:?}", timer.elapsed());
            world.wait_all();
        }
        world.barrier();
        let cur_t = timer.elapsed().as_secs_f64();
        let cur: f64 = world.MB_sent().iter().sum();
        if my_pe == 0 {
            println!(
                "tx_size: {:?}B num_tx: {:?} num_bytes: {:?}MB time: {:?} (issue time: {:?})
                throughput (avg): {:?}MB/s (cuml): {:?}MB/s total_bytes (w/ overhead) {:?}MB throughput (w/ overhead){:?} latency: {:?}us",
                num_bytes, //transfer size
                cnt,  //num transfers
                sum as f64/ 1048576.0,
                cur_t, //transfer time
                sub_time,
                (sum as f64 / 1048576.0) / cur_t, // throughput of user payload
                ((sum*(num_pes-1) as u64) as f64 / 1048576.0) / cur_t,
                cur - old, //total bytes sent including overhead
                (cur - old) as f64 / cur_t, //throughput including overhead
                (cur_t/cnt as f64) * 1_000_000 as f64 ,
            );
            bws.push((sum as f64 / 1048576.0) / cur_t);
        }
    }
    if my_pe == 0 {
        println!(
            "bandwidths: {}",
            bws.iter()
                .fold(String::new(), |acc, &num| acc + &num.to_string() + ", ")
        );
    }
}

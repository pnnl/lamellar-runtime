/// ------------Lamellar Bandwidth: AM  -------------------------
/// Test the bandwidth between two PEs using an active message which
/// contains a vector of N bytes
/// the active message simply returns immediately.
/// --------------------------------------------------------------------
use lamellar::ActiveMessaging;
use std::time::Instant;

#[lamellar::AmData(Clone, Debug)]
struct DataAM {
    data: Vec<u8>,
}

#[lamellar::am]
impl LamellarAM for DataAM {
    async fn exec() {}
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new()
        // .with_scheduler(lamellar::SchedulerType::WorkStealing)
        .build();
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
    for i in 0..27 {
        let num_bytes = 2_u64.pow(i);
        let mut _data: std::vec::Vec<u8> = Vec::new(); //vec![; num_bytes as usize];
        for i in 0..num_bytes {
            _data.push(i as u8);
        }

        let old: f64 = world.MB_sent();
        let mut sum = 0;
        let mut cnt = 0;
        let mut exp = 20;
        if num_bytes <= 2048 {
            exp = 18 + i;
        } else if num_bytes >= 4096 {
            exp = 30;
        }
        // exp=10;

        let timer = Instant::now();
        let mut sub_time = 0f64;
        // println!("starting next round");
        if my_pe == 0 {
            for _j in (num_bytes..(2_u64.pow(exp))).step_by(num_bytes as usize) {
                let sub_timer = Instant::now();
                let d = _data.clone();
                sub_time += sub_timer.elapsed().as_secs_f64();
                world.exec_am_pe(num_pes - 1, DataAM { data: d }); //we explicity  captured _data and transfer it even though we do nothing with it
                
                sum += num_bytes * 1 as u64;
                cnt += 1;
            }
            println!("issue time: {:?}", timer.elapsed().as_secs_f64()-sub_time);
        }
        world.wait_all();
        world.barrier();
        let cur_t = timer.elapsed().as_secs_f64();
        let cur: f64 = world.MB_sent();
        if my_pe == 0 {
            println!(
                "tx_size: {:?}B num_tx: {:?} num_bytes: {:?}MB time: {:?} (issue time: {:?})
                throughput (avg): {:?}MB/s (cuml): {:?}MB/s total_bytes (w/ overhead) {:?}MB throughput (w/ overhead){:?} latency: {:?}us",
                num_bytes, //transfer size
                cnt,  //num transfers
                sum as f64/ 1048576.0,
                cur_t-sub_time, //transfer time
                sub_time,
                (sum as f64 / 1048576.0) / cur_t, // throughput of user payload
                ((sum*(num_pes-1) as u64) as f64 / 1048576.0) / cur_t,
                cur - old, //total bytes sent including overhead
                (cur - old) as f64 / cur_t, //throughput including overhead
                (cur_t/cnt as f64) * 1_000_000 as f64 ,
            );
            bws.push((sum as f64 / 1048576.0) / cur_t);
        }
        println!("finished round");
        println!("======================================================================================");
    }
    if my_pe == 0 {
        println!(
            "bandwidths: {}",
            bws.iter()
                .fold(String::new(), |acc, &num| acc + &num.to_string() + ", ")
        );
    }
}

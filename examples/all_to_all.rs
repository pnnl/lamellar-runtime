use std::time::Instant;

fn main() {
    let (my_pe, num_pes) = lamellar::init();
    lamellar::barrier();
    let s = Instant::now();
    lamellar::barrier();
    let b = s.elapsed().as_secs_f64();
    println!("Barrier latency: {:?}s {:?}us", b, b * 1_000_000 as f64);

    if my_pe == 0 {
        println!("==================Bandwidth test===========================");
    }
    let mut bws = vec![];
    for i in 0..23 {
        let num_bytes = 2_u64.pow(i);
        let _data: std::vec::Vec<u8> = vec![0; num_bytes as usize];

        let old: f64 = lamellar::MB_sent().iter().sum();
        let mut sum = 0;
        let mut cnt = 0;
        let mut exp = 20;
        if num_bytes <= 8 {
            exp = 16;
        } else if num_bytes > 4096 && num_bytes < 65536 {
            exp = 26;
        } else if num_bytes >= 65536 {
            exp = 30;
        }
	//exp -= (num_pes as f64).log2() as u32;
        
	let timer = Instant::now();
        let mut sub_time = 0f64;
        for _j in (num_bytes..(2_u64.pow(exp))).step_by(num_bytes as usize) {
            let _d = _data.clone();
            let sub_timer = Instant::now();
            lamellar::exec_all(lamellar::FnOnce!([_d] move || {})); //we explicity are captured _data and transfer it even though we do nothing with it
                                                                    // lamellar::exec_on_pe(1, f.clone()); //we explicity are captured _data and transfer it even though we do nothing with it
                                                                    // lamellar::exec_all(FnOnce!([_d] move || {}));
            sub_time += sub_timer.elapsed().as_secs_f64();
            sum += num_bytes * num_pes as u64;
            cnt += 1;
        }
        lamellar::barrier();
        if my_pe == 0 {
            println!("issue time: {:?}", timer.elapsed());
        }
        lamellar::wait_all();
        lamellar::barrier();
        let cur_t = timer.elapsed().as_secs_f64();
        let cur: f64 = lamellar::MB_sent().iter().sum();
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
                ((sum*(num_pes) as u64) as f64 / 1048576.0) / cur_t,
                cur - old, //total bytes sent including overhead
                (cur - old) as f64 / cur_t, //throughput including overhead 
                (cur_t/cnt as f64) * 1_000_000 as f64 ,
            );
            bws.push(((sum * (num_pes - 1) as u64) as f64 / 1048576.0) / cur_t);
        }
    }

    println!(
        "PE: {:?} -> Total data sent per node: {:?}",
        my_pe,
        lamellar::MB_sent()
    );
    if my_pe == 0 {
        println!(
            "bandwidths: {}",
            bws.iter()
                .fold(String::new(), |acc, &num| acc + &num.to_string() + ", ")
        );
    }
    lamellar::finit();
}

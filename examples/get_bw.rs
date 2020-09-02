use lamellar::RemoteMemoryRegion;
use lamellar::ActiveMessaging;
use std::time::Instant;

const ARRAY_LEN: usize = 512 * 1024 * 1024;

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let array = world.alloc_mem_region::<u8>(ARRAY_LEN);
    let init = vec![num_pes as u8; ARRAY_LEN];
    unsafe { array.put(my_pe, 0, &init) };
    world.barrier();
    let s = Instant::now();
    world.barrier();
    let b = s.elapsed().as_secs_f64();
    println!("Barrier latency: {:?}s {:?}us", b, b * 1_000_000 as f64);

    if my_pe == 0 {
        println!("==================Bandwidth test===========================");
    }
    let mut bws = vec![];
    let mut data: std::vec::Vec<u8> = vec![my_pe as u8; ARRAY_LEN as usize];
    for i in 0..27 {
        let num_bytes = 2_u64.pow(i);
        // println!("data ptr {:p}",data.as_ptr());
        let old: f64 = world.MB_sent().iter().sum();
        let mbs_o = world.MB_sent();
        let mut sum = 0;
        let mut cnt = 0;
        let mut exp = 20;
        if num_bytes <= 8 {
            exp = 18;
        } else if num_bytes >= 4096 {
            exp = 29;
        }
        let timer = Instant::now();
        let mut sub_time = 0f64;
        if my_pe == 0 {
            for j in (0..2_u64.pow(exp) as usize).step_by(num_bytes as usize) {
                let sub_timer = Instant::now();
                unsafe { array.get(num_pes-1, j, &mut data[j..(j + num_bytes as usize)]) };
                sub_time += sub_timer.elapsed().as_secs_f64();
                sum += num_bytes * 1 as u64;
                cnt += 1;
            }
            println!("issue time: {:?}", timer.elapsed());
            world.wait_all();
        }
        if my_pe == 0 {
            for j in (0..2_u64.pow(exp) as usize).step_by(num_bytes as usize) {
                while data[(j + num_bytes as usize) - 1] == my_pe as u8 {
                    std::thread::yield_now()
                }
            }
        }
        world.barrier();
        let cur_t = timer.elapsed().as_secs_f64();
        let cur: f64 = world.MB_sent().iter().sum();
        let mbs_c = world.MB_sent();
        if my_pe == 0 {
            for j in 0..ARRAY_LEN as usize {
                if data[j] == my_pe as u8 {
                    println!("Error: {:?} {:?}", j, data[j]);
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
            (mbs_c[0] -mbs_o[0] )/ cur_t,
            (cur_t/cnt as f64) * 1_000_000 as f64 ,
        );
        }
        bws.push((sum as f64 / 1048576.0) / cur_t);
        for j in 0..ARRAY_LEN as usize {
            data[j] = my_pe as u8;
        }
        world.barrier();
    }
    world.free_memory_region(array);
    if my_pe == 0 {
        println!(
            "bandwidths: {}",
            bws.iter()
                .fold(String::new(), |acc, &num| acc + &num.to_string() + ", ")
        );
    }
    world.barrier();
    
}

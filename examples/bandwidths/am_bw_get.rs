/// ------------Lamellar Bandwidth: AM +RDMA -------------------------
/// Test the bandwidth between two PEs using an active message which
/// contians a handle to a SharedMemoryRegion, the active message
/// then "gets" N bytes into a local array.
/// This allows us to have multiple data transfers occuring in parallel
/// and reduces the need to copy + serialize/deserialize larges amounts
/// of data (on the critical path)
/// --------------------------------------------------------------------
use lamellar::{ActiveMessaging, RemoteMemoryRegion, SharedMemoryRegion};
use std::time::Instant;

const ARRAY_LEN: usize = 1 * 1024 * 1024 * 1024;

#[lamellar::AmData(Clone, Debug)]
struct DataAM {
    array: SharedMemoryRegion<u8>,
    index: usize,
    length: usize,
    src: usize,
}

#[lamellar::am]
impl LamellarAM for DataAM {
    fn exec(&self) {
        unsafe {
            // let local = lamellar::team.local_array::<u8>(self.length, 255u8);
            let local = lamellar::team.alloc_local_mem_region::<u8>(self.length);
            let local_slice = local.as_mut_slice().unwrap();
            local_slice[self.length - 1] = 255u8;
            self.array.get(self.src, self.index, local.clone());

            while local_slice[self.length - 1] == 255u8 {
                async_std::task::yield_now().await;
            }
        }
    }
}

fn main() {
    if let Ok(size) = std::env::var("LAMELLAR_MEM_SIZE") {
        let size = size
            .parse::<usize>()
            .expect("invalid memory size, please supply size in bytes");
        if size < 8 * 1024 * 1024 * 1024 {
            println!("This example requires 8GB of 'local' space, please set LAMELLAR_MEM_SIZE env var to at least 8589934592 (8GB)");
            std::process::exit(1);
        }
    } else {
        println!("This example requires 8GB of 'local' space, please set LAMELLAR_MEM_SIZE env var to at least 8589934592 (8GB)");
        std::process::exit(1);
    }
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let array = world.alloc_shared_mem_region::<u8>(ARRAY_LEN);
    let data = world.alloc_local_mem_region::<u8>(ARRAY_LEN);
    unsafe {
        for i in data.as_mut_slice().unwrap() {
            *i = my_pe as u8;
        }
    }
    unsafe { array.put(my_pe, 0, data.clone()) };
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
        let old: f64 = world.MB_sent().iter().sum();
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
        if my_pe == num_pes - 1 {
            for _j in (0..(2_u64.pow(exp))).step_by(num_bytes as usize) {
                let sub_timer = Instant::now();
                world.exec_am_pe(
                    my_pe,
                    DataAM {
                        array: array.clone(),
                        index: 0 as usize,
                        length: num_bytes as usize,
                        src: 0,
                    },
                );
                sub_time += sub_timer.elapsed().as_secs_f64();
                sum += num_bytes * 1 as u64;
                cnt += 1;
            }
            println!("issue time: {:?}", timer.elapsed().as_secs_f64());
            world.wait_all();
        }
        world.barrier();
        let cur_t = timer.elapsed().as_secs_f64();
        let cur: f64 = world.MB_sent().iter().sum();
        if my_pe == num_pes - 1 {
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
        unsafe {
            let data = array.as_mut_slice().unwrap();
            for j in 0..ARRAY_LEN as usize {
                data[j] = my_pe as u8;
            }
        }
        world.barrier();
    }
    if my_pe == num_pes - 1 {
        println!(
            "bandwidths: {}",
            bws.iter()
                .fold(String::new(), |acc, &num| acc + &num.to_string() + ", ")
        );
    }
}

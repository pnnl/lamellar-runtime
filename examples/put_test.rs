use lamellar::RemoteMemoryRegion;
use lamellar::ActiveMessaging;
use std::time::Instant;

const ARRAY_LEN: usize = 512;

//need a trait for like "RDMA able" meaning anything that is allocated on the stack
#[derive(serde::Serialize, serde::Deserialize,Debug,Clone,)]
struct TestData{
    arg1: u8,
    arg2: isize,
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    if num_pes == 1 {
        println!("WARNING: This example is intended for 2 PEs");
    } else {
        println!("Size of TestData: {:?}",std::mem::size_of::<TestData>());
        let array = world.alloc_mem_region::<TestData>(ARRAY_LEN);
        let init = vec![TestData{
            arg1: my_pe as u8,
            arg2: 0,
        }; ARRAY_LEN];
        unsafe { array.put(my_pe, 0, &init) }; //copy local array to memory region

        

        unsafe { array.put(my_pe, 0, &init) }; //copy local array to memory region
        world.barrier();
        let s = Instant::now();
        world.barrier();
        let b = s.elapsed().as_secs_f64();
        println!("Barrier latency: {:?}s {:?}us", b, b * 1_000_000 as f64);

        if my_pe == 0 {
            println!("==================Bandwidth test===========================");
        }
        let mut bws = vec![];
        let mut data: std::vec::Vec<TestData> = vec![TestData{
            arg1: my_pe as u8,
            arg2: 0,
        }; ARRAY_LEN as usize];
        for i in 0..ARRAY_LEN {
            data[i].arg2 = - (i as isize);
        }

        for i in 0..7 {
            let num_elems = 2_u64.pow(i);
            let num_bytes = num_elems*std::mem::size_of::<TestData>() as u64;
            let old: f64 = world.MB_sent().iter().sum();
            let mbs_o = world.MB_sent();
            let mut sum = 0;
            let mut cnt = 0;
            println!("before array[0] {:?} array[511] {:?}",array.as_slice()[0],array.as_slice()[ARRAY_LEN-1]);
            world.barrier();
            let timer = Instant::now();
            let mut sub_time = 0f64;
            if my_pe == 0 && num_pes > 1 {
                for j in (0..512).step_by(num_elems as usize) {
                    let sub_timer = Instant::now();
                    unsafe { array.put(1, j, &data[j..(j+num_elems as usize)]) };
                    sub_time += sub_timer.elapsed().as_secs_f64();
                    sum += num_bytes * 1 as u64;
                    cnt += 1;
                }
                println!("issue time: {:?}", timer.elapsed());
                world.wait_all();
            }
            if my_pe != 0 {
                for j in 0..512 as usize{
                    let temp =  &array.as_slice()[j];
                    while temp.arg1 == my_pe as u8 || temp.arg2 != -(j as isize) {
                        std::thread::yield_now()
                    }
                }
            }
            world.barrier();
            let cur_t = timer.elapsed().as_secs_f64();
            

            let cur: f64 = world.MB_sent().iter().sum();
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
                (mbs_c[0] -mbs_o[0] )/ cur_t,
                (cur_t/cnt as f64) * 1_000_000 as f64 ,
            );
            }
            println!("after array[0] {:?} array[511] {:?}",&array.as_slice()[0],&array.as_slice()[ARRAY_LEN-1]);
            bws.push((sum as f64 / 1048576.0) / cur_t);
            unsafe { array.put(my_pe, 0, &init) };
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
}

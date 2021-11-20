/// ------------Lamellar Example: Distributed Array Reductions -------------------------
/// this is an experimental exmaple testing our initial design for reductions on distributed array
/// we expect that the syntax and APIs illustrated are highly likey to change in future release.
///
/// This example shows how to create a user defined reduction operation
/// it is very similar to our registered active message interface.
///----------------------------------------------------------------
use lamellar::array::{DistributedIterator, Distribution, UnsafeArray};
use lamellar::{ActiveMessaging, RemoteMemoryRegion};
use std::time::Instant;

lamellar::register_reduction!(
    min,
    |a, b| {
        if a < *b {
            a
        } else {
            *b
        }
    },
    usize,
    u8
);

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let num_pes = world.num_pes();
    let my_pe = world.my_pe();
    let total_len = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 100);

    // let total_len = len_per_pe * num_pes;
    let len_per_pe = total_len as f32 / num_pes as f32;
    let my_local_size = len_per_pe.round() as usize; //((len_per_pe * (my_pe+1) as f32).round() - (len_per_pe * my_pe as f32).round()) as usize;
    println!("my local size {:?}", my_local_size);
    let block_array = UnsafeArray::<usize>::new(world.team(), total_len, Distribution::Block);
    let cyclic_array = UnsafeArray::<usize>::new(world.team(), total_len, Distribution::Cyclic);
    let local_mem_region = world.alloc_local_mem_region(total_len);
    world.barrier();
    if my_pe == 0 {
        unsafe {
            let mut i = 0; //(len_per_pe * my_pe as f32).round() as usize;
            for elem in local_mem_region.as_mut_slice().unwrap() {
                *elem = i;
                i += 1
            }
        }
        println!("{:?}", local_mem_region.as_slice().unwrap());
        // let index = ((len_per_pe * (my_pe) as f32).round() as usize) % total_len;

        block_array.put(0, &local_mem_region);
        cyclic_array.put(0, &local_mem_region);
    }
    world.barrier();
    std::thread::sleep(std::time::Duration::from_secs(1));
    println!("Block Array");
    block_array.print();
    println!();
    println!("Cyclic Array");
    std::thread::sleep(std::time::Duration::from_secs(1));
    cyclic_array.print();
    println!();
    unsafe {
        for elem in local_mem_region.as_mut_slice().unwrap() {
            *elem = 0;
        }
    }
    block_array.iget(0, &local_mem_region);
    world.barrier();
    std::thread::sleep(std::time::Duration::from_secs(1));
    if my_pe == 0 {
        println!(
            "[{:?}] get from block array {:?}",
            my_pe,
            local_mem_region.as_slice()
        );
    }

    unsafe {
        for elem in local_mem_region.as_mut_slice().unwrap() {
            *elem = 0;
        }
    }
    cyclic_array.iget(0, &local_mem_region);
    world.barrier();
    std::thread::sleep(std::time::Duration::from_secs(1));
    if my_pe == 0 {
        println!(
            "[{:?}] get from cyclic array {:?}",
            my_pe,
            local_mem_region.as_slice()
        );
    }

    world.barrier();
    if my_pe == 0 {
        println!("starting dist");
        let mut timer = Instant::now();
        let cyclic_sum = cyclic_array.sum().get();
        let cyclic_dist_time = timer.elapsed().as_secs_f64();
        timer = Instant::now();
        let block_sum = block_array.sum().get(); //need to figure out why this calculation is wrong...
        let block_dist_time = timer.elapsed().as_secs_f64();
        let calculated_sum = (total_len / 2) * (0 + 99);
        println!(
            "cyclic_sum {:?} cyclic time {:?}, block_sum {:?} block time {:?}, calculated sum {:?}",
            cyclic_sum, cyclic_dist_time, block_sum, block_dist_time, calculated_sum
        );

        let block_min = block_array.reduce("min").get();
        let cyclic_min = block_array.reduce("min").get();
        println!("block min: {:?} cyclic min: {:?}", block_min, cyclic_min);
    }
    // for i in 0..total_len {
    //     block_array.add(i, 10);
    // }
    // block_array.for_each_mut(|x| *x += *x);
    cyclic_array.dist_iter_mut().for_each(|x| *x += *x);
    cyclic_array
        .dist_iter()
        .enumerate()
        .for_each(|x| println!("x: {:?}", x));
    // cyclic_array.dist_iter().for_each(|x| println!("x: {:?}", x));

    block_array
        .dist_iter()
        .enumerate()
        .for_each(|x| println!("x: {:?}", x));
    // block_array.dist_iter().for_each(|x| println!("x: {:?}", x));
    world.wait_all();
    // block_array.for_each(|x| println!("x: {:?}", x));
    // cyclic_array.for_each_mut(|x| *x += *x);

    world.barrier();
}

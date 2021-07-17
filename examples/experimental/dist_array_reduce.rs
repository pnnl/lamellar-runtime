/// ------------Lamellar Example: Distributed Array Reductions -------------------------
/// this is an experimental exmaple testing our initial design for reductions on distributed array
/// we expect that the syntax and APIs illustrated are highly likey to change in future release.
///
/// This example shows how to create a user defined reduction operation
/// it is very similar to our registered active message interface.
///----------------------------------------------------------------
use lamellar::{
    ActiveMessaging, Distribution, LamellarAM, LamellarArray, LocalAM, RemoteMemoryRegion,
    UnsafeArray,
};
use std::time::Instant;

// lamellar::register_reduction!(
//     min,
//     |a, b| {
//         if a < *b {
//             a
//         } else {
//             *b
//         }
//     },
//     usize,
//     u8
// );

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
    // unsafe {
    //     for elem in local_mem_region.as_mut_slice().unwrap(){
    //         *elem = num_pes;
    //     }
    // }
    std::thread::sleep(std::time::Duration::from_secs(1));
    block_array.print();
    println!();
    // if my_pe  == 0{
    //     usize_array.put(index/2,&local_mem_region);
    // }
    std::thread::sleep(std::time::Duration::from_secs(1));
    cyclic_array.print();
    println!();
    unsafe {
        for elem in local_mem_region.as_mut_slice().unwrap() {
            *elem = 0;
        }
    }
    block_array.get(0, &local_mem_region);
    world.barrier();
    std::thread::sleep(std::time::Duration::from_secs(1));
    println!("[{:?}] {:?}", my_pe, local_mem_region.as_slice());
    unsafe {
        for elem in local_mem_region.as_mut_slice().unwrap() {
            *elem = 0;
        }
    }
    cyclic_array.get(0, &local_mem_region);
    world.barrier();
    std::thread::sleep(std::time::Duration::from_secs(1));
    println!("[{:?}] {:?}", my_pe, local_mem_region.as_slice());

    // let usize_array: LamellarArray<usize> = world.new_array(len_per_pe); //100 elements per pe
    // let mut local = vec![0; total_len];
    // println!("initializing data");
    // unsafe {
    //     //this will NOT be the standard way to update a LamellarArray
    //     let rmr_slice = usize_array.get_raw_mem_region().as_mut_slice().unwrap();
    //     for i in 0..len_per_pe {
    //         rmr_slice[i] = my_pe * len_per_pe + i + 1;
    //         local[i] = i + 1;
    //     }
    //     for i in len_per_pe..total_len {
    //         //finish filling out the local buf
    //         local[i] = i + 1;
    //     }
    //     //hope to end up with something like:
    //     // for (i,elem) in usize_array.lamellar_iter_mut().enumerate(){
    //     //    *elem = my_pe * len_per_pe + i;
    //     //}
    //     // next step will likely be something like:
    //     // for i in usize_array.local_indices(){
    //     //    usize_array[i] = i;
    //     //}
    //     // for i in 0..num_pes{
    //     //     if i == my_pe{
    //     //         println!("{:?}",&rmr_slice);
    //     //     }
    //     //     world.barrier();
    //     // }
    // }
    // world.barrier();
    // if my_pe == 0 {
    //     println!("starting dist");
    //     let timer = Instant::now();
    //     let sum = usize_array.sum().get();
    //     let dist_time = timer.elapsed().as_secs_f64();
    //     println!("starting local");
    //     let timer = Instant::now();
    //     let local_sum = local.iter().fold(0, |acc, val| acc + val);
    //     let local_time = timer.elapsed().as_secs_f64();

    //     println!(
    //         "sum: {:?} ({:?}) {:?} ({:?})",
    //         sum, dist_time, local_sum, local_time
    //     );

    //     let min = usize_array.reduce("min").get();
    //     println!("min: {:?} {:?}", min, 1);
    // }
    world.barrier();
    world.free_local_memory_region(local_mem_region);
}

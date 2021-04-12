use lamellar::{ActiveMessaging, LamellarAM, LamellarArray, RegisteredMemoryRegion};
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
    let len_per_pe = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 100);

    let total_len = len_per_pe * num_pes;

    let usize_array: LamellarArray<usize> = world.new_array(len_per_pe); //100 elements per pe
    let mut local = vec![0; total_len];
    println!("initializing data");
    unsafe {
        //this will NOT be the standard way to update a LamellarArray
        let rmr_slice = usize_array.get_raw_mem_region().as_mut_slice().unwrap();
        for i in 0..len_per_pe {
            rmr_slice[i] = my_pe * len_per_pe + i + 1;
            local[i] = i + 1;
        }
        for i in len_per_pe..total_len {
            //finish filling out the local buf
            local[i] = i + 1;
        }
        //hope to end up with something like:
        // for (i,elem) in usize_array.lamellar_iter_mut().enumerate(){
        //    *elem = my_pe * len_per_pe + i;
        //}
        // next step will likely be something like:
        // for i in usize_array.local_indices(){
        //    usize_array[i] = i;
        //}
        // for i in 0..num_pes{
        //     if i == my_pe{
        //         println!("{:?}",&rmr_slice);
        //     }
        //     world.barrier();
        // }
    }
    world.barrier();
    if my_pe == 0 {
        println!("starting dist");
        let timer = Instant::now();
        let sum = usize_array.sum().get();
        let dist_time = timer.elapsed().as_secs_f64();
        println!("starting local");
        let timer = Instant::now();
        let local_sum = local.iter().fold(0, |acc, val| acc + val);
        let local_time = timer.elapsed().as_secs_f64();

        println!(
            "sum: {:?} ({:?}) {:?} ({:?})",
            sum, dist_time, local_sum, local_time
        );

        let min = usize_array.reduce("min").get();
        println!("min: {:?} {:?}", min, 1);
    }
    world.barrier();
}

/// ------------Lamellar Example: Hello World-------------------------
/// This example highlights how to create a Lamellar Array
/// as then perform simple add operations on its elements
/// --------------------------------------------------------------------
use lamellar::array::prelude::*;
use lamellar::darc::prelude::*;
use lamellar::memregion::prelude::*;

fn main() {
    let timer = std::time::Instant::now();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let num_pes = world.num_pes();
    let _my_pe = world.my_pe();

    let local_length = 1_000_000_000; //if you want to ensure each thread processes data make this >= LAMELLAR_THREADS environment variable
    let global_length = num_pes * local_length;
    let init_time = timer.elapsed();
    println!("init_time: {:?}", init_time);
    let mut times = vec![0f32; 5];

    for i in 0..10 {
        world.wait_all();
        world.barrier();
        let timer = std::time::Instant::now();
        let local_vec = vec![10usize; local_length];
        let local_vec_time = timer.elapsed();
        std::hint::black_box(&local_vec);
        println!("local_vec_time: {:?} {:?}", local_vec_time, local_vec.len());
        times[0] += local_vec_time.as_secs_f32();
        world.wait_all();
        world.barrier();
        let timer = std::time::Instant::now();
        let darc_vec = vec![10usize; local_length];
        let vec_time = timer.elapsed();
        let darc_vec = Darc::new(world.team(), darc_vec).unwrap();
        let darc_vec_time = timer.elapsed();
        std::hint::black_box(&darc_vec);
        println!(
            "vec_time {:?} darc_vec_time: {:?} {:?}",
            vec_time,
            darc_vec_time,
            darc_vec.len()
        );
        times[1] += darc_vec_time.as_secs_f32();
        world.wait_all();
        world.barrier();
        let timer = std::time::Instant::now();
        let array = AtomicArray::<usize>::new(world.team(), global_length, Distribution::Block);
        let array_time = timer.elapsed();
        std::hint::black_box(&array);
        println!("array_time: {:?} {:?}", array_time, array.len());
        times[2] += array_time.as_secs_f32();
        world.wait_all();
        world.barrier();
        let timer = std::time::Instant::now();
        let one_sided = world.alloc_one_sided_mem_region::<usize>(local_length);
        let one_sided_time = timer.elapsed();
        std::hint::black_box(&one_sided);
        println!("one_sided_time: {:?} {:?}", one_sided_time, one_sided.len());
        times[3] += one_sided_time.as_secs_f32();
        drop(one_sided);
        world.wait_all();
        world.barrier();

        let timer = std::time::Instant::now();
        let shared = world.alloc_shared_mem_region::<usize>(local_length);
        let shared_time = timer.elapsed();
        std::hint::black_box(&shared);
        println!("shared_time: {:?} {:?}", shared_time, shared.len());
        times[4] += shared_time.as_secs_f32();
        println!("");
    }

    println!("local vec avg: {:?}", times[0] / 10.0);
    println!("darc vec avg: {:?}", times[1] / 10.0);
    println!("array avg: {:?}", times[2] / 10.0);
    println!("one_sided avg: {:?}", times[3] / 10.0);
    println!("shared avg: {:?}", times[4] / 10.0);

    //print local data on each pe
    // array.print();
    // println!("");

    // let timer = std::time::Instant::now();
    // //add 1 to each element of array
    // // for i in 0..global_length {
    // let _ = array.batch_add(0, &local_vec[0..100]).spawn();
    // // }
    // //wait for all the local add operations to finish
    // array.wait_all();
    // //wait for all the PEs to finish
    // array.barrier();

    // let add_time = timer.elapsed();
    // println!("add_time: {:?}", add_time);

    //print local data on each PE (should now be equal to num_pes)
    // array.print();
}

/// ------------Lamellar Example: Hello World-------------------------
/// This example highlights how to create a Lamellar Array
/// as then perform simple add operations on its elements
/// --------------------------------------------------------------------
use lamellar::array::prelude::*;

fn main() {
    let timer = std::time::Instant::now();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let num_pes = world.num_pes();
    let _my_pe = world.my_pe();
    let local_length = 1_000_000_000; //if you want to ensure each thread processes data make this >= LAMELLAR_THREADS environment variable
    let global_length = num_pes * local_length;
    let init_time = timer.elapsed();
    println!("init_time: {:?}", init_time);

    let timer = std::time::Instant::now();
    let local_vec = vec![0usize; local_length];
    let local_vec_time = timer.elapsed();
    println!("local_vec_time: {:?}", local_vec_time);

    let timer = std::time::Instant::now();
    let array = AtomicArray::<usize>::new(world.team(), global_length, Distribution::Block);
    let array_time = timer.elapsed();
    println!("array_time: {:?}", array_time);

    let timer = std::time::Instant::now();
    // let _one_sided = world.alloc_one_sided_mem_region::<usize>(local_length);
    let one_sided_time = timer.elapsed();
    println!("one_sided_time: {:?}", one_sided_time);

    //print local data on each pe
    // array.print();
    // println!("");

    let timer = std::time::Instant::now();
    //add 1 to each element of array
    // for i in 0..global_length {
    let _ = array.batch_add(0, &local_vec[0..100]);
    // }
    //wait for all the local add operations to finish
    array.wait_all();
    //wait for all the PEs to finish
    array.barrier();

    let add_time = timer.elapsed();
    println!("add_time: {:?}", add_time);

    //print local data on each PE (should now be equal to num_pes)
    // array.print();
}

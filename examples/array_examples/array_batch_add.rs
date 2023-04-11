use lamellar::array::prelude::*;

use rand::Rng;

fn main() {
    unsafe {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let num_pes = world.num_pes();
    let my_pe = world.my_pe();
    let array = UnsafeArray::<usize>::new(world.clone(), 1000000, Distribution::Block); //non intrinsic atomic, non bitwise
    //create vec of random indices between 0 & 1000000
    let mut rng = rand::thread_rng();
    let mut indices = (0..10_000_000).map(|_| rng.gen_range(0..num_pes)).collect::<Vec<_>>();
    let vals = vec![1;10_000_000];

    array.barrier();
    let mut timer = std::time::Instant::now();
    array.batch_add(indices.clone(),&vals);
    if my_pe == 0 {
        println!("{:?}", timer.elapsed());
    }
    world.wait_all();
    world.barrier();
    if my_pe == 0 {
        println!("{:?}", timer.elapsed());
    }
    println!("{:?}", world.block_on(array.sum()));


    world.barrier();
    // let iter = vals.into_iter();
    timer = std::time::Instant::now();
    array.new_add(indices.iter(),(0..10_000_000).map(|_| 1));
    if my_pe == 0 {
        println!("{:?}", timer.elapsed());
    }
    world.wait_all();
    world.barrier();
    if my_pe == 0 {
        println!("{:?}", timer.elapsed());
    }
    println!("{:?}", world.block_on(array.sum()));
    }
}
/// ------------Lamellar Example: Hello World-------------------------
/// This example highlights how to create a Lamellar Array
/// as then perform simple add operations on its elements
/// --------------------------------------------------------------------
use lamellar::array::prelude::*;

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let num_pes = world.num_pes();
    let _my_pe = world.my_pe();
    let local_length = 10; //if you want to ensure each thread processes data make this >= LAMELLAR_THREADS environment variable
    let global_length = num_pes * local_length;

    let array = AtomicArray::<usize>::new(world.team(), global_length, Distribution::Block);

    //print local data on each pe
    array.print();
    println!("");

    //add 1 to each element of array
    for i in 0..global_length {
        let _ = array.add(i, 1);
    }
    //wait for all the local add operations to finish
    array.wait_all();
    //wait for all the PEs to finish
    array.barrier();

    //print local data on each PE (should now be equal to num_pes)
    array.print();
}

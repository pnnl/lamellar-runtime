/// ------------Lamellar Example: Hello World-------------------------
/// This example highlights how to create a Lamellar Array
/// as well as use various iterators to print and modifiy its data
/// --------------------------------------------------------------------
use lamellar::array::{ //using the array submodule
    DistributedIterator, // needed for dist_iter_mut()
    SerialIterator, // needed for ser_iter()
    Distribution, // needed Distribution::{Block,Cyclic} 
    AtomicArray // needed to construct AtomicArrays
};


fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let num_pes = world.num_pes();
    let my_pe = world.my_pe();
    let local_length = 10;  //if you want to ensure each thread processes data make this >= LAMELLAR_THREADS environment variable
    let global_length = num_pes * local_length;

    let array = AtomicArray::<usize>::new(world.team(), global_length, Distribution::Block); //Compare with Distribution::Cyclic

    //examine array before initialization
    if my_pe == 0 { //we only want to print the array on pe 0
        array.ser_iter() // create a one sided iterator (manages data movement)
            .into_iter() // turn into a normal Rust iterator
            .for_each(|elem| print!("{elem} "));
        println!("");
    }
    world.barrier(); //wait for PE 0 to finish printing
    

    //initialize array, each PE will set its local elements equal to its ID
    let request = array.dist_iter_mut() //create a mutable distributed iterator (i.e. data parallel iteration, similar to Rayon par_iter())
                       .enumerate() // enumeration with respect to the global array
                       .for_each(move |(i,elem)| {
        println!("PE {:?} setting  array[{:?}]={:?} using thread {:?}", my_pe, i, my_pe, std::thread::current().id());
        elem.store(my_pe); //"store" because this is an AtomicArray
    });

    //wait for local iteration to complete
    world.block_on(request);

    //wait for all pes to finish
    world.barrier();

    //examine array after initialization
    if my_pe == 0 { //we only want to print the array on pe 0
        array.ser_iter() // create a one sided iterator (manages data movement)
            .into_iter() // turn into a normal Rust iterator
            .for_each(|elem| print!("{elem} "));
        println!("");
    }
    world.barrier(); //wait for PE 0 to finish printing
    array.print(); // now print the local data of each PE
}

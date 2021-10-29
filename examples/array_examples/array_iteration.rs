use lamellar::array::{DistributedIterator,SerialIterator, Distribution, UnsafeArray};
use lamellar::{ActiveMessaging, LamellarWorld};

const ARRAY_LEN: usize = 100;

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let block_array = UnsafeArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Block);
    let cyclic_array = UnsafeArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Cyclic);

    // We expose multiple ways to iterate over a lamellar array
    // the first approach introduces what we call a distributed iterator (inspired by Rayon's parallel iterators).
    // When using a distributed iterator, each PE (that is a part of the array) will only iterate over the data
    // local to that pe, thus instantiating a distributed iterator introduces a synchronoization point.
    // distributed iterators are created by calling dist_iter() or dist_iter_mut() on a LamellarArray;

    let block_dist_iter = block_array.dist_iter_mut();
    let cyclic_dist_iter = cyclic_array.dist_iter_mut();

    // similar to normal iterators, distributed iterators must have a consumer to extract elements from the iterator
    // we currently provide the "for_each" driver which will execute a closure on every element in the distributed array (concurrently)

    //for example lets initialize our arrays, where we store the value of my_pe to each local element a pe owns
    block_dist_iter.for_each(move |elem| *elem = my_pe);
    cyclic_dist_iter.for_each(move |elem| *elem = my_pe);
    //for_each is asynchronous so we must wait on the array for the operations to complete
    // we are working on providing a request handle which can be used to check for completion
    block_array.wait_all();
    block_array.barrier();
    cyclic_array.wait_all();
    cyclic_array.barrier();

    block_array.print();
    cyclic_array.print();

    // our plan is to support a number of iterator extenders/operators similar to tradition rust iters
    // currently we offer Enumurator,

    block_array
        .dist_iter()
        .enumerate()
        .for_each(move |(i, elem)| {
            println!(
                "[pe({:?})-{:?}] i: {:?} {:?}",
                my_pe,
                std::thread::current().id(),
                i,
                elem
            )
        });
    block_array.wait_all();
    block_array.barrier();

    println!("--------------------------------------------------------");

    cyclic_array
        .dist_iter()
        .enumerate()
        .for_each(move |(i, elem)| {
            println!(
                "[pe({:?})-{:?}] i: {:?} {:?}",
                my_pe,
                std::thread::current().id(),
                i,
                elem
            )
        });
    cyclic_array.wait_all();
    cyclic_array.barrier();

    println!("--------------------------------------------------------");

    block_array
        .dist_iter()
        .chunks(7)
        .enumerate()
        .for_each(move |(i,chunk)| {
            let data = chunk.collect::<Vec<_>>();
            println!(
                "[pe({:?})-{:?}] chunk {:?} {:?}",
                my_pe,
                std::thread::current().id(),
                i,
                data
            )
        });
    block_array.wait_all();
    block_array.barrier();

    println!("--------------------------------------------------------");

    // the second approach is to iterate over the entire array on a single pe
    // this is accomplished calling ".iter()" on a Lamellar Array
    // underneath, the runtime will transfer copies of any non local data to the pe.
    // For UnsafeArrays, there are no guarantees that remote data has not changed
    // by the time the copy has arrived at the calling node.
    // the returned iterator actualls impls the Rust Iterator trait, so operations
    // that work on rust iterations should work here. Because data is being copied
    // we do not provide an iter_mut() method for lamellararrays.

    if my_pe == 0 {
        for elem in block_array.iter() {
            print!("{:?} ", elem);
        }
        println!("");

        for elem in cyclic_array.iter() {
            print!("{:?} ", elem);
        }
        println!("");
    }

    println!("--------------------------------------------------------");

    // The lamellar array iterator used above is lazy, meaning that it only accesses and returns a value as its used,
    // while this is generally efficent and results in low overhead, because an elem may actually exists on a remote node
    // latencies to retrieve the next value in the iterator are dependent on the location of the data, as a result of
    // the need to get the data. Further impacting performance is that typically the transfer of a single element will
    // likely be small, thus inefficiently utilizing network resources.
    // to address these issues, we have provided a buffered iterator, which will transfer "get" and store a block of data
    // into a buffer, from with the iterated values are returned. More effectively using network resources. From the users
    // standpoint the only thing that changes is the instatiation of the iterator.

    if my_pe == 0 {
        for elem in block_array.buffered_iter(10) {
            print!("{:?} ", elem);
        }
        println!("");

        for elem in cyclic_array.buffered_iter(10) {
            print!("{:?} ", elem);
        }
        println!("");
    }

    println!("--------------------------------------------------------");

    // in addition to the buffered iters we also provide a method to iterate over chunks of a lamellar array, via
    // the copied_chunks() method. Called on a LamellarArrayIterator this creates a chunk sized LocalMemoryRegion,
    // and then puts the appropriate date based on the iteration index into that region

    if my_pe == 0 {
        for chunk in block_array.iter().copied_chunks(10) {
            println!("{:?}", chunk.as_slice());
        }
        println!("-----");
        for chunk in cyclic_array.iter().copied_chunks(10) {
            println!("{:?}", chunk.as_slice());
        }
    }

    println!("--------------------------------------------------------");
}

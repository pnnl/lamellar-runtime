use lamellar::array::prelude::*;
const ARRAY_LEN: usize = 100;

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let _num_pes = world.num_pes();
    let block_array = AtomicArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Block);
    let cyclic_array = AtomicArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Cyclic);

    //we are going to initialize the data on each PE by directly accessing its local data

    block_array
        .mut_local_data()
        .iter()
        .for_each(|e| e.store(my_pe));
    cyclic_array
        .mut_local_data()
        .iter()
        .for_each(|e| e.store(my_pe));

    // In this example we will make use of a onesided iterator which
    // enables us to iterate over the entire array on a single PE.
    // The runtime will manage transferring data from remote PEs.
    // Note that for UnsafeArrays, AtomicArrays, and LocalLockArrays,
    // there is no guarantee that by the time the transferred data
    // as arrived to the calling PE it has remained the same on the remote PE.
    // we do not currently provide a mutable one sided iterator.

    if my_pe == 0 {
        println!("Here");
        for elem in block_array.onesided_iter().into_iter() {
            //we can convert from a oneside iterator into a rust iterator
            print!("{:?} ", elem);
        }
        println!("");
        println!("Here2");
        for elem in cyclic_array.onesided_iter().into_iter() {
            print!("{:?} ", elem);
        }
        println!("");
    }
    println!("Here3");
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
        for elem in block_array.buffered_onesided_iter(10).into_iter() {
            print!("{:?} ", elem);
        }
        println!("");

        for elem in cyclic_array.buffered_onesided_iter(10).into_iter() {
            print!("{:?} ", elem);
        }
        println!("");
    }

    println!("--------------------------------------------------------");

    // in addition to the buffered iters we also provide a method to iterate over chunks of a lamellar array, via
    // the chunks() method. Called on a OneSidedIterator this creates a chunk sized OneSidedMemoryRegion,
    // and then puts the appropriate date based on the iteration index into that region

    if my_pe == 0 {
        for chunk in block_array.onesided_iter().chunks(10).skip(4).into_iter() {
            println!("{:?}", unsafe { chunk.as_slice() });
        }
        println!("-----");
        for chunk in cyclic_array.onesided_iter().chunks(10).into_iter() {
            println!("{:?}", unsafe { chunk.as_slice() });
        }

        println!("-----");
        for (i, (a, b)) in cyclic_array
            .onesided_iter()
            .zip(block_array.onesided_iter())
            .into_iter()
            .enumerate()
        {
            println!("{:?}: {:?} {:?}", i, a, b);
        }
        println!("-----");
        for (a, b) in cyclic_array
            .onesided_iter()
            .chunks(10)
            .zip(block_array.onesided_iter().chunks(10))
            .into_iter()
        {
            unsafe {
                println!("{:?} {:?}", a.as_slice(), b.as_slice());
            }
        }
    }

    println!("--------------------------------------------------------");

    // let block_array = UnsafeArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Block);
    // for elem in block_onesided_iter!($array,array).into_iter().step_by(4) {...}
    // for elem in block_array.buffered_onesided_iter(10) {...}

    // //rust step_by pseudo code
    // fn step_by(&mut self, n: usize) -> Result<T>{
    //     let val = self.next(); //grab val based on index
    //     self.index += n;
    //     val
    // }

    // //--------------
    // for elem in block_array.onesided_iter().step_by(4).into_iter() {...}
}

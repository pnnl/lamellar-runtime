use futures_util::stream::StreamExt;
use lamellar::array::prelude::*;

use tracing_subscriber::prelude::*;

const ARRAY_LEN: usize = 100;

#[lamellar::main]
fn main() {
    let _subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(
            tracing_subscriber::fmt::layer()
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .with_level(true),
        )
        .init();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let block_array =
        AtomicArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Block).block();
    let cyclic_array =
        AtomicArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Cyclic).block();

    //we are going to initialize the data on each PE by directly accessing its local data
    block_array.print();
    block_array
        .mut_local_data()
        .iter()
        .for_each(|e| e.store(my_pe));
    cyclic_array.print();
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
        let mut count = 0;
        for elem in block_array.onesided_iter().into_iter() {
            //we can convert from a oneside iterator into a rust iterator
            print!("{:?} ", elem);
            count += 1;
        }
        println!("");
        assert_eq!(count, ARRAY_LEN, "onesided_iter into_iter count: got {count} expected {ARRAY_LEN}");

        println!("Here2");
        let mut cyclic_count = 0;
        for elem in cyclic_array.onesided_iter().into_iter() {
            print!("{:?} ", elem);
            cyclic_count += 1;
        }
        println!("");
        assert_eq!(cyclic_count, ARRAY_LEN, "cyclic onesided_iter count: got {cyclic_count} expected {ARRAY_LEN}");
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
            println!("{:?}", chunk.as_slice());
        }
        println!("-----");
        for chunk in cyclic_array.onesided_iter().chunks(10).into_iter() {
            println!("{:?}", chunk.as_slice());
        }

        println!("-----");
        // cyclic[i] = i%num_pes, block[i] = owning PE under block distribution
        let chunk = ARRAY_LEN / num_pes;
        let rem = ARRAY_LEN % num_pes;
        let block_pe_for = |i: usize| -> usize {
            let boundary = rem * (chunk + 1);
            if i < boundary { i / (chunk + 1) } else { rem + (i - boundary) / chunk }
        };
        let zipped: Vec<_> = cyclic_array
            .onesided_iter()
            .zip(block_array.onesided_iter())
            .into_iter()
            .enumerate()
            .collect();
        assert_eq!(zipped.len(), ARRAY_LEN, "zip count: got {} expected {ARRAY_LEN}", zipped.len());
        for (i, (a, b)) in &zipped {
            println!("{:?}: {:?} {:?}", i, a, b);
            assert_eq!(*a, i % num_pes, "zip cyclic value mismatch at {i}: got {a} expected {}", i % num_pes);
            assert_eq!(*b, block_pe_for(*i), "zip block value mismatch at {i}: got {b} expected {}", block_pe_for(*i));
        }
        println!("-----");
        let chunk_pairs: Vec<_> = cyclic_array
            .onesided_iter()
            .chunks(10)
            .zip(block_array.onesided_iter().chunks(10))
            .into_iter()
            .collect();
        assert_eq!(chunk_pairs.len(), ARRAY_LEN / 10, "chunks zip count: got {} expected {}", chunk_pairs.len(), ARRAY_LEN / 10);
        for (chunk_idx, (a, b)) in chunk_pairs.iter().enumerate() {
            println!("{:?} {:?}", a.as_slice(), b.as_slice());
            for j in 0..a.as_slice().len() {
                let i = chunk_idx * 10 + j;
                assert_eq!(a.as_slice()[j], i % num_pes, "chunk cyclic mismatch at global {i}");
                assert_eq!(b.as_slice()[j], block_pe_for(i), "chunk block mismatch at global {i}");
            }
        }
    }

    println!("--------------------------------------------------------");

    // let block_array = UnsafeArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Block).block();
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
    // }

    // fn main() {
    //     let world = LamellarWorldBuilder::new().build();
    //     let array = LocalLockArray::<usize>::new(&world, 8, Distribution::Block).block();
    //     let my_pe = world.my_pe();
    //     let num_pes = world.num_pes();
    let block_array = block_array.into_local_lock().block();
    block_array
        .dist_iter_mut()
        .for_each(move |e| *e = my_pe)
        .block(); //initialize array using a distributed iterator

    world.block_on(async move {
        if my_pe == 0 {
            // block_array[i] = my_pe; all values < num_pes by definition
            let result = block_array
                .onesided_iter()
                .into_stream()
                .take(4)
                .map(|elem| elem as f64)
                .all(|elem| async move { elem < num_pes as f64 });
            assert!(result.await, "onesided_iter all: expected all elems < num_pes");
        }
    });
}

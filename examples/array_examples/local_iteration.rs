use lamellar::array::prelude::*;
const ARRAY_LEN: usize = 100;

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let _num_pes = world.num_pes();
    let block_array = AtomicArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Block);
    let cyclic_array = AtomicArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Cyclic);

    // We expose multiple ways to iterate over a lamellar array
    // the first approach introduces what we call a distributed iterator (inspired by Rayon's parallel iterators).
    // When using a distributed iterator, each PE (that is a part of the array) will only iterate over the data
    // local to that pe, thus instantiating a distributed iterator introduces a synchronoization point.
    // distributed iterators are created by calling local_iter() or local_iter_mut() on a LamellarArray;

    let block_local_iter = block_array.local_iter_mut();
    let cyclic_local_iter = cyclic_array.local_iter_mut();

    // similar to normal iterators, distributed iterators must have a consumer to extract elements from the iterator
    // we currently provide the "for_each" driver which will execute a closure on every element in the distributed array (concurrently)

    //for example lets initialize our arrays, where we store the value of my_pe to each local element a pe owns
    block_local_iter
        .enumerate()
        .for_each(move |(i, elem)| elem.store(i));
    cyclic_local_iter.for_each(move |elem| elem.store(my_pe));
    //for_each is asynchronous so we must wait on the array for the operations to complete
    // we are working on providing a request handle which can be used to check for completion
    block_array.wait_all();
    block_array.barrier();
    cyclic_array.wait_all();
    cyclic_array.barrier();

    // let block_array = block_array.into_read_only();
    block_array.print();
    cyclic_array.print();

    // our plan is to support a number of iterator extenders/operators similar to tradition rust iters
    // currently we offer Enumurator,

    println!("--------------------------------------------------------");
    println!("block skip enumerate step_by");
    block_array
        .local_iter()
        .skip(2)
        .enumerate()
        .step_by(3)
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

    // println!("zip ");
    // block_array
    //     .local_iter()
    //     .zip(cyclic_array.local_iter())
    //     .skip(2)
    //     .enumerate()
    //     .chunks(4)
    //     .step_by(3)
    //     .for_each(move |chunk| {
    //         println!("[pe({:?})-{:?}]", my_pe, std::thread::current().id(),);
    //         for (i, elem) in chunk {
    //             println!("i: {:?} {:?}", i, elem)
    //         }
    //     });
    // block_array.wait_all();
    // block_array.barrier();

    println!("--------------------------------------------------------");
    println!("cyclic skip enumerate");

    cyclic_array
        .local_iter()
        .enumerate()
        .skip(2)
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

    // block_array
    //     .local_iter()
    //     .chunks(7)
    //     .enumerate()
    //     .for_each(move |(i, chunk)| {
    //         let data = chunk.collect::<Vec<_>>();
    //         println!(
    //             "[pe({:?})-{:?}] chunk {:?} {:?}",
    //             my_pe,
    //             std::thread::current().id(),
    //             i,
    //             data
    //         )
    //     });
    // block_array.wait_all();
    // block_array.barrier();

    println!("--------------------------------------------------------");
    println!("cyclic enumerate map async for each");
    cyclic_array.print();
    let barray = block_array.clone();
    cyclic_array
        .local_iter()
        .enumerate()
        .map(move |(i, elem)| {
            let barray = barray.clone();
            println!(
                "[pe({:?})-{:?}] i: {:?} {:?}",
                my_pe,
                std::thread::current().id(),
                i,
                elem
            );
            async move { (i, elem.load(), barray.load(i).await + elem.load()) }
        })
        .for_each_async(move |i| async move {
            println!(
                "[pe({:?})-{:?}] {:?}",
                my_pe,
                std::thread::current().id(),
                i.await
            );
        });
    cyclic_array.wait_all();
    cyclic_array.barrier();
    block_array.print();

    // println!("--------------------------------------------------------");
    // println!("cyclic enumerate map async collect");
    // let barray = block_array.clone();
    // let new_array = world.block_on(
    //     cyclic_array
    //         .local_iter()
    //         .enumerate()
    //         .map(move |(i, elem)| {
    //             let barray = barray.clone();
    //             async move {
    //                 barray.add(i, *elem).await;
    //                 barray.fetch_sub(i, *elem).await
    //             }
    //         })
    //         .collect_async::<ReadOnlyArray<usize>, _>(Distribution::Block),
    // );
    // cyclic_array.barrier();
    // new_array.print();
    // block_array.print();

    println!("--------------------------------------------------------");
    println!("block enumerate filter");
    block_array
        .local_iter()
        .enumerate()
        .filter(|(_, elem)| {
            println!(
                "{:?} filter op {} {}",
                std::thread::current().id(),
                elem.load(),
                elem.load() % 4 == 0
            );
            elem.load() % 4 == 0
        })
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
    println!("block enumerate filter_map");
    block_array
        .local_iter()
        .enumerate()
        .filter_map(|(i, elem)| {
            if elem.load() % 4 == 0 {
                Some((i, elem.load() as f32))
            } else {
                None
            }
        })
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
    // println!("--------------------------------------------------------");
    // println!("filter_map collect");
    // let new_block_array = block_array.local_iter().filter_map(| elem| {
    //     if *elem % 8 == 0 {
    //         Some(*elem as f32)
    //     }
    //     else{
    //         None
    //     }
    // }).collect::<ReadOnlyArray<f32>>(Distribution::Block).wait(); //todo fix me
    // new_block_array.print();

    println!("--------------------------------------------------------");
    println!("block skip enumerate");
    block_array
        .local_iter()
        .skip(10)
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
    println!("block skip  step_by enumerate");
    block_array
        .local_iter()
        .skip(10)
        .step_by(3)
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
    println!("block take skip enumerate");
    block_array
        .local_iter()
        .take(60)
        .skip(10)
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
    println!("block take skip take enumerate");
    block_array
        .local_iter()
        .take(60)
        .skip(10)
        .take(30)
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

    println!("--------------------------------------------------------");
    block_array.block_on(
        block_array
            .dist_iter_mut()
            .enumerate()
            .for_each(|(i, elem)| {
                elem.store(i);
            }),
    );
    println!("block map reduce");
    let req = block_array
        .local_iter()
        .map(|elem| elem.load())
        .reduce(|acc, elem| acc + elem);

    let sum = block_array.block_on(req);

    println!("{my_pe} reduce sum: {:?}", sum);
    block_array.barrier();

    println!("--------------------------------------------------------");
}

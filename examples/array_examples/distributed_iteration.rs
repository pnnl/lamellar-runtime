use lamellar::array::prelude::*;
const ARRAY_LEN: usize = 100;

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let _num_pes = world.num_pes();
    let block_array = LocalLockArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Block).block();
    let cyclic_array = AtomicArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Cyclic).block();

    // We expose multiple ways to iterate over a lamellar array
    // the first approach introduces what we call a distributed iterator (inspired by Rayon's parallel iterators).
    // When using a distributed iterator, each PE (that is a part of the array) will only iterate over the data
    // local to that pe, thus instantiating a distributed iterator introduces a synchronoization point.
    // distributed iterators are created by calling dist_iter() or dist_iter_mut() on a LamellarArray;

    let lock = block_array.write_lock().block();
    let block_dist_iter = block_array.dist_iter_mut();
    let cyclic_dist_iter = cyclic_array.dist_iter_mut();

    // similar to normal iterators, distributed iterators must have a consumer to extract elements from the iterator
    // we currently provide the "for_each" driver which will execute a closure on every element in the distributed array (concurrently)

    //for example lets initialize our arrays, where we store the value of my_pe to each local element a pe owns
    let iter = block_dist_iter
        .enumerate()
        .for_each(move |(i, elem)| *elem = i)
        .spawn();
    std::thread::sleep(std::time::Duration::from_secs(1));
    drop(lock);
    iter.block();
    cyclic_dist_iter
        .for_each(move |elem| elem.store(my_pe))
        .block();

    // let block_array = block_array.into_read_only();
    block_array.print();
    cyclic_array.print();

    println!("--------------------------------------------------------");
    println!("block sum");
    let sum = block_array.block_on(block_array.dist_iter().map(|e| *e).sum());
    println!("result: {sum}");
    world.barrier();
    println!("--------------------------------------------------------");
    println!("--------------------------------------------------------");
    println!("cyclic sum");
    let sum = cyclic_array.block_on(cyclic_array.dist_iter().map(|e| e.load()).sum());
    println!("result: {sum}");
    world.barrier();
    println!("--------------------------------------------------------");

    // our plan is to support a number of iterator extenders/operators similar to tradition rust iters
    // currently we offer Enumurator,

    println!("--------------------------------------------------------");
    println!("block skip enumerate step_by");
    block_array
        .dist_iter()
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
        })
        .block();

    println!("--------------------------------------------------------");
    println!("cyclic skip enumerate");

    cyclic_array
        .dist_iter()
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
        })
        .block();

    println!("--------------------------------------------------------");

    println!("--------------------------------------------------------");
    println!("cyclic enumerate map async for each");
    cyclic_array.print();
    let barray = block_array.clone();
    cyclic_array
        .dist_iter()
        .enumerate()
        .map(move |(i, elem)| {
            let barray = barray.clone();
            println!(
                "[pe({:?})-{:?}] map i: {:?} {:?}",
                my_pe,
                std::thread::current().id(),
                i,
                elem
            );
            async move { (i, elem, barray.load(i).await) }
        })
        .for_each_async(move |i| async move {
            println!(
                "[pe({:?})-{:?}] for each {:?}",
                my_pe,
                std::thread::current().id(),
                i.await
            );
        })
        .block();
    block_array.print();

    println!("--------------------------------------------------------");
    println!("cyclic enumerate map async collect");
    let barray = block_array.clone();
    let new_array = world.block_on(
        cyclic_array
            .dist_iter()
            .enumerate()
            .map(move |(i, elem)| {
                let barray = barray.clone();
                async move {
                    barray.add(i, elem.load()).await;
                    barray.fetch_sub(i, elem.load()).await
                }
            })
            .collect_async::<ReadOnlyArray<usize>, _>(Distribution::Block),
    );
    new_array.print();
    block_array.print();

    println!("--------------------------------------------------------");
    println!("block enumerate filter");
    block_array
        .dist_iter()
        .enumerate()
        .filter(|(_, elem)| *elem % 4 == 0)
        .for_each(move |(i, elem)| {
            println!(
                "[pe({:?})-{:?}] i: {:?} {:?}",
                my_pe,
                std::thread::current().id(),
                i,
                elem
            )
        })
        .block();

    println!("--------------------------------------------------------");
    println!("block enumerate filter_map");
    block_array
        .dist_iter()
        .enumerate()
        .filter_map(|(i, elem)| {
            if *elem % 4 == 0 {
                Some((i, *elem as f32))
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
        })
        .block();
    println!("--------------------------------------------------------");
    println!("filter_map collect");
    let new_block_array = block_array.block_on(
        block_array
            .dist_iter()
            .filter_map(|elem| {
                let e = *elem;
                if e % 8 == 0 {
                    println!("e: {:?}", e);
                    Some(e as u8)
                } else {
                    None
                }
            })
            .collect::<ReadOnlyArray<u8>>(Distribution::Block),
    );

    new_block_array.print();

    println!("--------------------------------------------------------");
    println!("block skip enumerate");
    block_array
        .dist_iter()
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
        })
        .block();

    println!("--------------------------------------------------------");
    println!("block skip  step_by enumerate");
    block_array
        .dist_iter()
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
        })
        .block();

    println!("--------------------------------------------------------");
    println!("block take skip enumerate");
    block_array
        .dist_iter()
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
        })
        .block();

    println!("--------------------------------------------------------");
    println!("block take skip take enumerate");
    block_array
        .dist_iter()
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
        })
        .block();

    println!("--------------------------------------------------------");
    println!("block filter count");
    let count = block_array.block_on(block_array.dist_iter().filter(|e| *e % 2 == 0).count());
    println!("result: {count}");
}

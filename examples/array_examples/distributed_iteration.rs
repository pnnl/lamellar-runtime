use lamellar::array::prelude::*;
const ARRAY_LEN: usize = 100;

#[lamellar::main]
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let block_array =
        LocalLockArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Block).block();
    let cyclic_array =
        AtomicArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Cyclic).block();

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

    // let block_array = block_array.into_read_only().block();
    block_array.print();
    cyclic_array.print();

    println!("--------------------------------------------------------");
    println!("block sum");
    // block_array[i] = i after init above, so sum = 0+1+...+(ARRAY_LEN-1)
    let sum = block_array.dist_iter().map(|e| *e).sum().block();
    println!("result: {sum}");
    world.barrier();
    let expected_sum = ARRAY_LEN * (ARRAY_LEN - 1) / 2;
    assert_eq!(sum, expected_sum, "block dist_iter map+sum: got {sum} expected {expected_sum}");
    println!("--------------------------------------------------------");
    println!("--------------------------------------------------------");
    println!("cyclic sum");
    // cyclic_array[i] = my_pe for local elems; global sum = sum of all pe ids * elems_per_pe
    let sum = cyclic_array.dist_iter().map(|e| e.load()).sum().block();
    println!("result: {sum}");
    world.barrier();
    // each pe owns ARRAY_LEN/num_pes elems (cyclic), all set to my_pe
    let expected_cyclic_sum = (0..num_pes).sum::<usize>() * (ARRAY_LEN / num_pes);
    assert_eq!(sum, expected_cyclic_sum, "cyclic dist_iter map+sum: got {sum} expected {expected_cyclic_sum}");
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
    let new_array = cyclic_array
        .dist_iter()
        .enumerate()
        .map(move |(i, elem)| {
            let barray = barray.clone();
            async move {
                barray.add(i, elem.load()).await;
                barray.fetch_sub(i, elem.load()).await
            }
        })
        .collect_async::<ReadOnlyArray<usize>, _>(Distribution::Block)
        .block();
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
    let new_block_array = block_array
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
        .collect::<ReadOnlyArray<u8>>(Distribution::Block)
        .block();

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
    // block_array[i] = i; even indices: 0,2,4,...,98 → 50 elements
    let count = block_array
        .dist_iter()
        .filter(|e| *e % 2 == 0)
        .count()
        .block();
    println!("result: {count}");
    assert_eq!(count, ARRAY_LEN / 2, "dist_iter filter+count: got {count} expected {}", ARRAY_LEN / 2);

    println!("--------------------------------------------------------");
    println!("block filter_map collect correctness");
    // filter_map: keep elems divisible by 8, cast to u8 — values 0,8,16,...,96 → 13 elements
    let filtered = block_array
        .dist_iter()
        .filter_map(|elem| {
            let e = *elem;
            if e % 8 == 0 { Some(e as u8) } else { None }
        })
        .collect::<ReadOnlyArray<u8>>(Distribution::Block)
        .block();
    let expected_count = (0..ARRAY_LEN).filter(|e| e % 8 == 0).count();
    let actual_count = filtered.onesided_iter().into_iter().count();
    assert_eq!(actual_count, expected_count, "dist_iter filter_map+collect: got {actual_count} expected {expected_count}");
    println!("filter_map collect count: {actual_count} (expected {expected_count})");
}

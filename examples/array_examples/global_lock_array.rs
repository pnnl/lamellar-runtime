use lamellar::array::prelude::*;
use std::time::{Duration, Instant};

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let _num_pes = world.num_pes();

    let array = GlobalLockArray::<usize>::new(&world, 100, Distribution::Block).block();

    let s = Instant::now();
    let local_data = array.read_local_data().block();
    println!(
        "0. PE{my_pe} time: {:?} {:?}",
        s.elapsed().as_secs_f64(),
        local_data
    );
    std::thread::sleep(Duration::from_secs(my_pe as u64));
    drop(local_data); //release the lock

    world.barrier();
    let mut local_data = array.write_local_data().block();
    println!(
        "1. PE{my_pe} time: {:?} got write lock",
        s.elapsed().as_secs_f64()
    );
    local_data.iter_mut().for_each(|elem| *elem = my_pe);
    std::thread::sleep(Duration::from_secs(1));
    drop(local_data);

    array.print();
    println!("2 .PE{my_pe} time: {:?} done", s.elapsed().as_secs_f64());

    let mut local_data = array.collective_write_local_data().block();
    println!(
        "3. PE{my_pe} time: {:?} got collective write lock",
        s.elapsed().as_secs_f64()
    );
    local_data.iter_mut().for_each(|elem| *elem += my_pe);
    std::thread::sleep(Duration::from_secs(1));
    drop(local_data);
    println!(
        "4. PE{my_pe} time: {:?} dropped collective write lock",
        s.elapsed().as_secs_f64()
    );

    array.print();
    println!("5. PE{my_pe} time: {:?} done", s.elapsed().as_secs_f64());

    array
        .read_lock()
        .block()
        .dist_iter()
        .enumerate()
        .for_each(move |(i, elem)| {
            println!(
                "{my_pe}, {:?}: {i} {:?}",
                std::thread::current().id(),
                *elem
            )
        })
        .block();
    world.barrier();
    println!("6. PE{my_pe} time: {:?} done", s.elapsed().as_secs_f64());

    let task = array
        .dist_iter_mut()
        .enumerate()
        .for_each(|(i, elem)| *elem += i);
    task.block();
    world.barrier();
    println!("7. PE{my_pe} time: {:?} done", s.elapsed().as_secs_f64());

    array.print();

    println!("8. PE{my_pe} time: {:?} done", s.elapsed().as_secs_f64());
}

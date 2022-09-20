use lamellar::array::{
    iterator::distributed_iterator::Schedule, DistributedIterator, Distribution, SerialIterator,
    UnsafeArray,
};

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread::ThreadId;
use std::time::{Duration, Instant};
const ARRAY_LEN: usize = 1000;

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let _my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let block_array = UnsafeArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Block);
    block_array
        .dist_iter_mut()
        .enumerate()
        .for_each(move |(i, e)| *e = i % (ARRAY_LEN / num_pes));
    world.wait_all();

    let thread_cnts: Arc<Mutex<HashMap<ThreadId, usize>>> = Arc::new(Mutex::new(HashMap::new()));

    let timer = Instant::now();
    let tc = thread_cnts.clone();
    block_array.dist_iter().for_each(move |e| {
        std::thread::sleep(Duration::from_millis((e * 1) as u64));
        *tc.lock().entry(std::thread::current().id()).or_insert(0) += e * 1;
    });
    block_array.wait_all();
    block_array.barrier();
    println!("elapsed time {:?}", timer.elapsed().as_secs_f64());
    println!("counts {:?}", thread_cnts.lock());

    thread_cnts.lock().clear();
    let tc = thread_cnts.clone();

    let timer = Instant::now();
    block_array.dist_iter().for_each_with_schedule(
        move |e| {
            std::thread::sleep(Duration::from_millis((e * 1) as u64));
            *tc.lock().entry(std::thread::current().id()).or_insert(0) += e * 1;
        },
        Schedule::WorkStealing,
    );
    block_array.wait_all();
    block_array.barrier();
    println!("elapsed time {:?}", timer.elapsed().as_secs_f64());
    println!("counts {:?}", thread_cnts.lock());

    thread_cnts.lock().clear();
    let tc = thread_cnts.clone();

    let timer = Instant::now();
    block_array.dist_iter().for_each_with_schedule(
        move |e| {
            std::thread::sleep(Duration::from_millis((e * 1) as u64));
            *tc.lock().entry(std::thread::current().id()).or_insert(0) += e * 1;
        },
        Schedule::Guided,
    );
    block_array.wait_all();
    block_array.barrier();
    println!("elapsed time {:?}", timer.elapsed().as_secs_f64());
    println!("counts {:?}", thread_cnts.lock());

    thread_cnts.lock().clear();
    let tc = thread_cnts.clone();

    let timer = Instant::now();
    block_array.dist_iter().for_each_with_schedule(
        move |e| {
            std::thread::sleep(Duration::from_millis((e * 1) as u64));
            *tc.lock().entry(std::thread::current().id()).or_insert(0) += e * 1;
        },
        Schedule::Dynamic,
    );
    block_array.wait_all();
    block_array.barrier();
    println!("elapsed time {:?}", timer.elapsed().as_secs_f64());
    println!("counts {:?}", thread_cnts.lock());

    thread_cnts.lock().clear();
    let tc = thread_cnts.clone();

    let timer = Instant::now();
    block_array.dist_iter().for_each_with_schedule(
        move |e| {
            std::thread::sleep(Duration::from_millis((e * 1) as u64));
            *tc.lock().entry(std::thread::current().id()).or_insert(0) += e * 1;
        },
        Schedule::Chunk(10),
    );
    block_array.wait_all();
    block_array.barrier();
    println!("elapsed time {:?}", timer.elapsed().as_secs_f64());
    println!("counts {:?}", thread_cnts.lock());
}

use lamellar::array::prelude::*;

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
    let block_array = AtomicArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Block);
    block_array
        .local_iter_mut()
        .enumerate()
        .for_each(move |(i, e)| e.store(i % (ARRAY_LEN / num_pes)));
    world.wait_all();

    let thread_cnts: Arc<Mutex<HashMap<ThreadId, usize>>> = Arc::new(Mutex::new(HashMap::new()));

    let timer = Instant::now();
    let tc = thread_cnts.clone();
    block_array.local_iter().for_each(move |e| {
        std::thread::sleep(Duration::from_millis((e.load() * 1) as u64));
        *tc.lock().entry(std::thread::current().id()).or_insert(0) += e.load() * 1;
    });
    block_array.wait_all();
    block_array.barrier();
    println!("elapsed time {:?}", timer.elapsed().as_secs_f64());
    println!("counts {:?}", thread_cnts.lock());

    thread_cnts.lock().clear();
    let tc = thread_cnts.clone();

    let timer = Instant::now();
    block_array
        .local_iter()
        .for_each_with_schedule2(Schedule::WorkStealing, move |e| {
            std::thread::sleep(Duration::from_millis((e.load() * 1) as u64));
            *tc.lock().entry(std::thread::current().id()).or_insert(0) += e.load() * 1;
        });
    block_array.wait_all();
    block_array.barrier();
    println!("elapsed time {:?}", timer.elapsed().as_secs_f64());
    println!("counts {:?}", thread_cnts.lock());

    thread_cnts.lock().clear();
    let tc = thread_cnts.clone();

    let timer = Instant::now();
    block_array
        .local_iter()
        .for_each_with_schedule2(Schedule::Guided, move |e| {
            std::thread::sleep(Duration::from_millis((e.load() * 1) as u64));
            *tc.lock().entry(std::thread::current().id()).or_insert(0) += e.load() * 1;
        });
    block_array.wait_all();
    block_array.barrier();
    println!("elapsed time {:?}", timer.elapsed().as_secs_f64());
    println!("counts {:?}", thread_cnts.lock());

    thread_cnts.lock().clear();
    let tc = thread_cnts.clone();

    let timer = Instant::now();
    block_array
        .local_iter()
        .for_each_with_schedule2(Schedule::Dynamic, move |e| {
            std::thread::sleep(Duration::from_millis((e.load() * 1) as u64));
            *tc.lock().entry(std::thread::current().id()).or_insert(0) += e.load() * 1;
        });
    block_array.wait_all();
    block_array.barrier();
    println!("elapsed time {:?}", timer.elapsed().as_secs_f64());
    println!("counts {:?}", thread_cnts.lock());

    thread_cnts.lock().clear();
    let tc = thread_cnts.clone();

    let timer = Instant::now();
    block_array
        .local_iter()
        .for_each_with_schedule2(Schedule::Chunk(10), move |e| {
            std::thread::sleep(Duration::from_millis((e.load() * 1) as u64));
            *tc.lock().entry(std::thread::current().id()).or_insert(0) += e.load() * 1;
        });
    block_array.wait_all();
    block_array.barrier();
    println!("elapsed time {:?}", timer.elapsed().as_secs_f64());
    println!("counts {:?}", thread_cnts.lock());
}

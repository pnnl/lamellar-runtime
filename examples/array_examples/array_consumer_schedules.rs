use lamellar::array::prelude::*;

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread::ThreadId;
use std::time::{Duration, Instant};
const ARRAY_LEN: usize = 50;

fn for_each_with_schedule(
    schedule: Schedule,
    array: &AtomicArray<usize>,
    thread_cnts: Arc<Mutex<HashMap<ThreadId, usize>>>,
) {
    let timer = Instant::now();
    let tc = thread_cnts.clone();
    array
        .local_iter()
        .filter(|e| e.load() % 2 == 0)
        .for_each_with_schedule(schedule, move |e| {
            std::thread::sleep(Duration::from_millis((e.load() * 1) as u64));
            *tc.lock().entry(std::thread::current().id()).or_insert(0) += 1;
        });
    array.wait_all();
    array.barrier();
    println!("elapsed time {:?}", timer.elapsed().as_secs_f64());
    println!("counts {:?}", thread_cnts.lock());

    thread_cnts.lock().clear();
    array.barrier();
}

fn reduce_with_schedule(
    schedule: Schedule,
    array: &AtomicArray<usize>,
    thread_cnts: Arc<Mutex<HashMap<ThreadId, usize>>>,
) {
    let timer = Instant::now();
    let _tc = thread_cnts.clone();
    let result = array.block_on(
        array
            .local_iter()
            .filter(|e| e.load() % 2 == 0)
            .map(|e| e.load())
            .reduce_with_schedule(schedule, |e1, e2| e1 + e2),
    );
    array.barrier();
    println!("elapsed time {:?}", timer.elapsed().as_secs_f64());
    println!("reduced {:?}", result);
}

fn collect_with_schedule(
    schedule: Schedule,
    array: &AtomicArray<usize>,
    thread_cnts: Arc<Mutex<HashMap<ThreadId, usize>>>,
) {
    let timer = Instant::now();
    let _tc = thread_cnts.clone();
    let result = array.block_on(
        array
            .local_iter()
            .filter(|e| e.load() % 2 == 0)
            .map(|e| e.load())
            .collect_with_schedule::<Vec<_>>(schedule, Distribution::Block),
    );
    array.barrier();
    println!("elapsed time {:?}", timer.elapsed().as_secs_f64());
    println!("collect {:?}", result);
}

fn count_with_schedule(
    schedule: Schedule,
    array: &AtomicArray<usize>,
    thread_cnts: Arc<Mutex<HashMap<ThreadId, usize>>>,
) {
    let timer = Instant::now();
    let _tc = thread_cnts.clone();
    let result = array.block_on(
        array
            .local_iter()
            .filter(|e| e.load() % 2 == 0)
            .count_with_schedule(schedule),
    );
    array.barrier();
    println!("elapsed time {:?}", timer.elapsed().as_secs_f64());
    println!("count {:?}", result);
}

fn sum_with_schedule(
    schedule: Schedule,
    array: &AtomicArray<usize>,
    thread_cnts: Arc<Mutex<HashMap<ThreadId, usize>>>,
) {
    let timer = Instant::now();
    let _tc = thread_cnts.clone();
    let result = array.block_on(
        array
            .local_iter()
            .filter(|e| e.load() % 2 == 0)
            .sum_with_schedule(schedule),
    );
    array.barrier();
    println!("elapsed time {:?}", timer.elapsed().as_secs_f64());
    println!("sum {:?}", result);
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let _my_pe = world.my_pe();
    let _num_pes = world.num_pes();
    let block_array = AtomicArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Block);
    block_array
        .dist_iter_mut()
        .enumerate()
        .for_each(move |(i, e)| e.store(i));
    world.wait_all();
    block_array.print();

    let thread_cnts: Arc<Mutex<HashMap<ThreadId, usize>>> = Arc::new(Mutex::new(HashMap::new()));

    println!("starting");
    for_each_with_schedule(Schedule::Static, &block_array, thread_cnts.clone());
    for_each_with_schedule(Schedule::Dynamic, &block_array, thread_cnts.clone());
    for_each_with_schedule(Schedule::Chunk(10), &block_array, thread_cnts.clone());
    for_each_with_schedule(Schedule::Guided, &block_array, thread_cnts.clone());
    for_each_with_schedule(Schedule::WorkStealing, &block_array, thread_cnts.clone());

    reduce_with_schedule(Schedule::Static, &block_array, thread_cnts.clone());
    reduce_with_schedule(Schedule::Dynamic, &block_array, thread_cnts.clone());
    reduce_with_schedule(Schedule::Chunk(10), &block_array, thread_cnts.clone());
    reduce_with_schedule(Schedule::Guided, &block_array, thread_cnts.clone());
    reduce_with_schedule(Schedule::WorkStealing, &block_array, thread_cnts.clone());

    collect_with_schedule(Schedule::Static, &block_array, thread_cnts.clone());
    collect_with_schedule(Schedule::Dynamic, &block_array, thread_cnts.clone());
    collect_with_schedule(Schedule::Chunk(10), &block_array, thread_cnts.clone());
    collect_with_schedule(Schedule::Guided, &block_array, thread_cnts.clone());
    collect_with_schedule(Schedule::WorkStealing, &block_array, thread_cnts.clone());

    count_with_schedule(Schedule::Static, &block_array, thread_cnts.clone());
    count_with_schedule(Schedule::Dynamic, &block_array, thread_cnts.clone());
    count_with_schedule(Schedule::Chunk(10), &block_array, thread_cnts.clone());
    count_with_schedule(Schedule::Guided, &block_array, thread_cnts.clone());
    count_with_schedule(Schedule::WorkStealing, &block_array, thread_cnts.clone());

    sum_with_schedule(Schedule::Static, &block_array, thread_cnts.clone());
    sum_with_schedule(Schedule::Dynamic, &block_array, thread_cnts.clone());
    sum_with_schedule(Schedule::Chunk(10), &block_array, thread_cnts.clone());
    sum_with_schedule(Schedule::Guided, &block_array, thread_cnts.clone());
    sum_with_schedule(Schedule::WorkStealing, &block_array, thread_cnts.clone());
}

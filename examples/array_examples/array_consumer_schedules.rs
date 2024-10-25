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
        .for_each_with_schedule(schedule.clone(), move |e| {
            std::thread::sleep(Duration::from_millis((e.load() * 1) as u64));
            *tc.lock().entry(std::thread::current().id()).or_insert(0) += 1;
        })
        .block();
    array.barrier();
    println!(
        "for_each {schedule:?} block elapsed time {:?}",
        timer.elapsed().as_secs_f64()
    );
    println!("counts {:?}", thread_cnts.lock());

    thread_cnts.lock().clear();
    array.barrier();

    let timer = Instant::now();
    let tc = thread_cnts.clone();
    let _handle = array
        .local_iter()
        .filter(|e| e.load() % 2 == 0)
        .for_each_with_schedule(schedule.clone(), move |e| {
            std::thread::sleep(Duration::from_millis((e.load() * 1) as u64));
            *tc.lock().entry(std::thread::current().id()).or_insert(0) += 1;
        })
        .spawn();
    array.wait_all();
    array.barrier();
    println!(
        "for_each {schedule:?} spawn elapsed time {:?}",
        timer.elapsed().as_secs_f64()
    );
    println!("counts {:?}", thread_cnts.lock());

    thread_cnts.lock().clear();
    array.barrier();

    array.block_on(async move {
        let timer = Instant::now();
        let tc = thread_cnts.clone();
        let _ = array
            .local_iter()
            .filter(|e| e.load() % 2 == 0)
            .for_each_with_schedule(schedule.clone(), move |e| {
                std::thread::sleep(Duration::from_millis((e.load() * 1) as u64));
                *tc.lock().entry(std::thread::current().id()).or_insert(0) += 1;
            })
            .await;
        array.async_barrier().await;
        println!(
            "for_each {schedule:?} await elapsed await {:?}",
            timer.elapsed().as_secs_f64()
        );
        println!("counts {:?}", thread_cnts.lock());
    });
}

fn reduce_with_schedule(
    schedule: Schedule,
    array: &AtomicArray<usize>,
    thread_cnts: Arc<Mutex<HashMap<ThreadId, usize>>>,
) {
    let timer = Instant::now();
    let _tc = thread_cnts.clone();
    let result = array
        .local_iter()
        .filter(|e| e.load() % 2 == 0)
        .map(|e| e.load())
        .reduce_with_schedule(schedule.clone(), |e1, e2| e1 + e2)
        .block();
    array.barrier();
    println!(
        "reduce {schedule:?} block elapsed time {:?}",
        timer.elapsed().as_secs_f64()
    );
    println!("reduced {:?}", result);

    let timer = Instant::now();
    let _tc = thread_cnts.clone();
    let result_handle = array
        .local_iter()
        .filter(|e| e.load() % 2 == 0)
        .map(|e| e.load())
        .reduce_with_schedule(schedule.clone(), |e1, e2| e1 + e2)
        .spawn();

    println!("about to wait all");
    array.wait_all();
    array.barrier();
    let result = result_handle.block();
    println!(
        "reduce {schedule:?} spawn elapsed time {:?}",
        timer.elapsed().as_secs_f64()
    );
    println!("reduced {:?}", result);

    array.block_on(async move {
        let timer = Instant::now();
        let _tc = thread_cnts.clone();
        let result = array
            .local_iter()
            .filter(|e| e.load() % 2 == 0)
            .map(|e| e.load())
            .reduce_with_schedule(schedule.clone(), |e1, e2| e1 + e2)
            .await;
        array.async_barrier().await;
        println!(
            "reduce {schedule:?} await elapsed time {:?}",
            timer.elapsed().as_secs_f64()
        );
        println!("reduced {:?}", result);
    });
}

fn collect_with_schedule(
    schedule: Schedule,
    array: &AtomicArray<usize>,
    thread_cnts: Arc<Mutex<HashMap<ThreadId, usize>>>,
) {
    let timer = Instant::now();
    let _tc = thread_cnts.clone();
    let result = array
        .local_iter()
        .filter(|e| e.load() % 2 == 0)
        .map(|e| e.load())
        .collect_with_schedule::<Vec<_>>(schedule.clone(), Distribution::Block)
        .block();
    array.barrier();
    println!(
        "collect {schedule:?} block elapsed time {:?}",
        timer.elapsed().as_secs_f64()
    );
    println!("collect {:?}", result);

    let timer = Instant::now();
    let _tc = thread_cnts.clone();
    let result_handle = array
        .local_iter()
        .filter(|e| e.load() % 2 == 0)
        .map(|e| e.load())
        .collect_with_schedule::<Vec<_>>(schedule.clone(), Distribution::Block)
        .spawn();
    array.wait_all();
    array.barrier();
    println!(
        "collect {schedule:?} spawn elapsed time {:?}",
        timer.elapsed().as_secs_f64()
    );
    println!("collect {:?}", result_handle.block());

    array.block_on(async move {
        let timer = Instant::now();
        let _tc = thread_cnts.clone();
        let result = array
            .local_iter()
            .filter(|e| e.load() % 2 == 0)
            .map(|e| e.load())
            .collect_with_schedule::<Vec<_>>(schedule.clone(), Distribution::Block)
            .await;
        array.async_barrier().await;
        println!(
            "collect {schedule:?} await elapsed time {:?}",
            timer.elapsed().as_secs_f64()
        );
        println!("collect {:?}", result);
    });
}

fn count_with_schedule(
    schedule: Schedule,
    array: &AtomicArray<usize>,
    thread_cnts: Arc<Mutex<HashMap<ThreadId, usize>>>,
) {
    let timer = Instant::now();
    let _tc = thread_cnts.clone();
    let result = array
        .local_iter()
        .filter(|e| e.load() % 2 == 0)
        .count_with_schedule(schedule.clone())
        .block();
    array.barrier();
    println!(
        "count {schedule:?} block elapsed time {:?}",
        timer.elapsed().as_secs_f64()
    );
    println!("count {:?}", result);

    let timer = Instant::now();
    let _tc = thread_cnts.clone();
    let result_handle = array
        .local_iter()
        .filter(|e| e.load() % 2 == 0)
        .count_with_schedule(schedule.clone())
        .spawn();
    array.wait_all();
    array.barrier();
    println!(
        "count {schedule:?} spawn elapsed time {:?}",
        timer.elapsed().as_secs_f64()
    );
    println!("count {:?}", result_handle.block());

    array.block_on(async move {
        let timer = Instant::now();
        let _tc = thread_cnts.clone();
        let result = array
            .local_iter()
            .filter(|e| e.load() % 2 == 0)
            .count_with_schedule(schedule.clone())
            .await;
        array.async_barrier().await;
        println!(
            "count {schedule:?} await elapsed time {:?}",
            timer.elapsed().as_secs_f64()
        );
        println!("count {:?}", result);
    });
}

fn sum_with_schedule(
    schedule: Schedule,
    array: &AtomicArray<usize>,
    thread_cnts: Arc<Mutex<HashMap<ThreadId, usize>>>,
) {
    let timer = Instant::now();
    let _tc = thread_cnts.clone();
    let result = array
        .local_iter()
        .map(|e| e.load())
        .filter(|e| e % 2 == 0)
        .sum_with_schedule(schedule.clone())
        .block();
    array.barrier();
    println!(
        "sum {schedule:?} block elapsed time {:?}",
        timer.elapsed().as_secs_f64()
    );
    println!("sum {:?}", result);

    let timer = Instant::now();
    let _tc = thread_cnts.clone();
    let result_handle = array
        .local_iter()
        .map(|e| e.load())
        .filter(|e| e % 2 == 0)
        .sum_with_schedule(schedule.clone())
        .spawn();
    array.wait_all();
    array.barrier();
    println!(
        "sum {schedule:?} spawn elapsed time {:?}",
        timer.elapsed().as_secs_f64()
    );
    println!("sum {:?}", result_handle.block());

    array.block_on(async move {
        let timer = Instant::now();
        let _tc = thread_cnts.clone();
        let result = array
            .local_iter()
            .map(|e| e.load())
            .filter(|e| e % 2 == 0)
            .sum_with_schedule(schedule.clone())
            .await;
        array.async_barrier().await;
        println!(
            "sum {schedule:?} await elapsed time {:?}",
            timer.elapsed().as_secs_f64()
        );
        println!("sum {:?}", result);
    });
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    println!("world created");
    let _my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let block_array =
        AtomicArray::<usize>::new(world.team(), ARRAY_LEN * num_pes, Distribution::Block);
    println!("array created");
    block_array.print();
    let _ = block_array
        .dist_iter_mut()
        .enumerate()
        .for_each(move |(i, e)| {
            println!("setting {i} to {i}");
            e.store(i)
        })
        .spawn();
    world.wait_all();
    println!("Done");
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

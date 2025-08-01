// use itertools::Itertools;
use lamellar::active_messaging::prelude::*;
use lamellar::array::prelude::*;
use rand::{distributions::Distribution, rngs::StdRng, SeedableRng};
use std::time::Instant;

use tracing_subscriber::fmt::{self, SubscriberBuilder};

const ARRAY_SIZE: usize = 100000000;
const NUM_UPDATES_PER_PE: usize = 1000000;

fn main() {
    let subscriber = fmt::init();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let array =
        AtomicArray::<usize>::new(&world, ARRAY_SIZE, lamellar::Distribution::Block).block();

    let mut rng: StdRng = SeedableRng::seed_from_u64(world.my_pe() as u64);
    let range = rand::distributions::Uniform::new(0, ARRAY_SIZE);

    // let sum = array.sum().block().expect("array len > 0");
    array.barrier();
    // println!("PE{} sum: {}", world.my_pe(), sum);
    let start = Instant::now();
    let histo = array.batch_add(
        &mut range.sample_iter(&mut rng).take(NUM_UPDATES_PER_PE)
            as &mut dyn Iterator<Item = usize>,
        1,
    );
    histo.block();
    // if world.my_pe() == 0 {
    //     let histo = array.batch_add(
    //         &[ARRAY_SIZE-2, ARRAY_SIZE-1] as &[usize],
    //         1,
    //     );
    //     histo.block();
    //     println!("local done: {:?}", start.elapsed().as_secs_f64());
    // }

    world.barrier();
    println!(
        "PE{} time: {:?} done",
        world.my_pe(),
        start.elapsed().as_secs_f64()
    );
    // let sum = array.sum().block().expect("array len > 0");
    // println!("PE{} sum: {}", world.my_pe(), sum);
}

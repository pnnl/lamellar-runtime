// use itertools::Itertools;
use lamellar::active_messaging::prelude::*;
use lamellar::array::prelude::*;
use rand::{distr::Distribution, rngs::StdRng, SeedableRng};
use std::time::Instant;

const ARRAY_SIZE: usize = 1000000000;
const NUM_UPDATES_PER_PE: usize = 100000;

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let array =
        AtomicArray::<usize>::new(&world, ARRAY_SIZE, lamellar::Distribution::Block).block();

    let mut rng: StdRng = SeedableRng::seed_from_u64(world.my_pe() as u64);
    let range = rand::distr::Uniform::new(0, ARRAY_SIZE).unwrap();

    let start = Instant::now();
    let histo = array.batch_add(
        &mut range.sample_iter(&mut rng).take(NUM_UPDATES_PER_PE)
            as &mut dyn Iterator<Item = usize>,
        1,
    );

    histo.block();
    world.barrier();
    println!(
        "PE{} time: {:?} done",
        world.my_pe(),
        start.elapsed().as_secs_f64()
    );
}

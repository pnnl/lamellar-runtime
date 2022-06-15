use lamellar::array::{DistributedIterator,Distribution, LocalOnlyArray};
const ARRAY_LEN: usize = 100;
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let mut block_array = LocalOnlyArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Block);
    let mut cloned_block_array = block_array.clone();
}
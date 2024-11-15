use lamellar::array::prelude::*;
const ARRAY_LEN: usize = 100;
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let mut block_array = LocalOnlyArray::<usize>::new(world.team(), ARRAY_LEN, Distribution::Block).block();

    let block_slice = block_array.as_slice();
    let _mut_block_slice = block_array.as_mut_slice();
    let _task = async {
        println!("vec {:?}",block_slice);
    };

}
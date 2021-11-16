use lamellar::array::{Distribution, UnsafeArray};
use lamellar::{Dist, LamellarWorld};

struct ArrayWrapper<T> {
    array: UnsafeArray<T>,
}

impl<T: Dist + Clone> ArrayWrapper<T> {
    fn new(world: &LamellarWorld, len: usize) -> Self {
        ArrayWrapper {
            array: UnsafeArray::<T>::new(world, len, Distribution::Block),
        }
    }
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let _num_pes = world.num_pes();
    let wrapped_array_f32 = ArrayWrapper::<f32>::new(&world, 10);
    let wrapped_array_usize = ArrayWrapper::<f32>::new(&world, 10);
}

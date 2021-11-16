use lamellar::array::{Distribution, UnsafeArray};
use lamellar::{Dist, LamellarWorld};

struct ArrayWrapper<T> {
    array: UnsafeArray<T>,
}

impl<T: Dist> ArrayWrapper<T> {
    fn new(world: LamellarWorld, len: usize) -> Self {
        ArrayWrapper {
            array: UnsafeArray::<T>::new(world, len, Distribution::Block),
        }
    }
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let _my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let wrapped_array_f32 = ArrayWrapper::<f32>::new(world.clone(), 10*num_pes);
    let wrapped_array_usize = ArrayWrapper::<usize>::new(world.clone(), 10*num_pes);
    wrapped_array_f32.array.print();
    wrapped_array_usize.array.print();

}

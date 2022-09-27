use lamellar::array::{AtomicArray, Distribution};
use lamellar::{Dist, LamellarWorld};

struct ArrayWrapper<T: Dist> {
    _array: AtomicArray<T>,
}

impl<T: Dist + Default> ArrayWrapper<T> {
    fn new(world: LamellarWorld, len: usize) -> Self {
        ArrayWrapper {
            _array: AtomicArray::<T>::new(world, len, Distribution::Block),
        }
    }
}

// lamellar::generate_ops_for_type!(false, Option<u8>); //todo get this to work... which Im not sure is actually possible?

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let _my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let _wrapped_array_f32 = ArrayWrapper::<f32>::new(world.clone(), 10 * num_pes);
    let _wrapped_array_usize = ArrayWrapper::<usize>::new(world.clone(), 10 * num_pes);
    // let wrapped_array_option_u8 = ArrayWrapper::<Option<u8>>::new(world.clone(), 10 * num_pes); //todo get this to work...
    // wrapped_array_f32.array.print();
    // wrapped_array_usize.array.print();
}
//     wrapped_array_option_u8.array.compare_exchange(0,None,Some(1));
//     wrapped_array_option_u8.array.compare_exchange(7,None,Some(7));
//     wrapped_array_option_u8.array.wait_all();
//     wrapped_array_option_u8.array.barrier();
//     wrapped_array_option_u8.array.print();
// }

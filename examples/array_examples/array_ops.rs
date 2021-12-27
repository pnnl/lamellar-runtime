use lamellar::array::{ArrayOps, AtomicArray, Distribution, LamellarArray, ElementOps};
use lamellar::Dist;

#[lamellar::AmData(Default, Debug, Dist)]
struct Custom {
    int: usize,
    float: f32,
}

impl std::ops::AddAssign for Custom {
    fn add_assign(&mut self, other: Self) {
        *self = Self {
            int: self.int + other.int,
            float: self.float + other.float,
        }
    }
}

impl std::ops::SubAssign for Custom {
    fn sub_assign(&mut self, other: Self) {
        *self = Self {
            int: self.int - other.int,
            float: self.float - other.float,
        }
    }
}

fn test_add<T: Dist + std::fmt::Debug + ElementOps + 'static>(
    array: AtomicArray<T>,
    init_val: T,
    add_val: T,
) {
    array.dist_iter_mut().for_each(move |elem| *elem = init_val);
    array.wait_all();
    array.barrier();
    array.print();
    for i in 0..array.len() {
        array.add(i, add_val);
    }
    array.wait_all();
    array.barrier();
    array.print();
}


fn test_sub<T: Dist + std::fmt::Debug + ElementOps + 'static>(
    array: AtomicArray<T>,
    init_val: T,
    sub_val: T,
) {
    array.dist_iter_mut().for_each(move |elem| *elem = init_val);
    array.wait_all();
    array.barrier();
    array.print();
    for i in 0..array.len() {
        array.sub(i, sub_val);
    }
    array.wait_all();
    array.barrier();
    array.print();
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let num_pes = world.num_pes();
    let array_f64 = AtomicArray::<f64>::new(world.clone(), num_pes * 10, Distribution::Block);
    let array_u8 = AtomicArray::<u8>::new(world.clone(), num_pes * 10, Distribution::Block);
    let array_custom = AtomicArray::<Custom>::new(world.clone(), num_pes * 10, Distribution::Block);

    test_add(array_f64.clone(), 0.0, 1.0);
    test_add(array_u8.clone(), 0, 1);
    test_add(
        array_custom.clone(),
        Custom { int: 0, float: 0.0 },
        Custom { int: 1, float: 1.0 },
    );
    array_u8.add(3, 1);
    array_u8.wait_all();
    array_u8.barrier();
    array_u8.print();

    array_f64.add(3, 1.0);
    array_f64.wait_all();
    array_f64.barrier();
    array_f64.print();

    array_custom.add(3, Custom { int: 1, float: 1.0 });
    array_custom.wait_all();
    array_custom.barrier();
    array_custom.print();
    println!("====================================================================");
    
    test_sub(array_f64.clone(), 10.0, 1.0);
    test_sub(array_u8.clone(), 10, 1);
    test_sub(
        array_custom.clone(),
        Custom { int: 10, float: 10.0 },
        Custom { int: 1, float: 1.0 },
    );
    array_u8.sub(3, 1);
    array_u8.wait_all();
    array_u8.barrier();
    array_u8.print();

    array_f64.sub(3, 1.0);
    array_f64.wait_all();
    array_f64.barrier();
    array_f64.print();

    array_custom.sub(3, Custom { int: 1, float: 1.0 });
    array_custom.wait_all();
    array_custom.barrier();
    array_custom.print();
    println!("====================================================================");
}

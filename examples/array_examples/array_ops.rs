use lamellar::array::{
    AccessOps, ReadOnlyOps, ArithmeticOps, AtomicArray, BitWiseOps, DistributedIterator, Distribution,
    ElementArithmeticOps, ElementBitWiseOps, ElementOps,
};

#[lamellar::AmData(Default, Debug, ArithmeticOps, PartialEq, PartialOrd)]
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

impl std::ops::Sub for Custom {
    type Output = Self;
    fn sub(self, other: Self) -> Self {
        Self {
            int: self.int - other.int,
            float: self.float - other.float,
        }
    }
}

impl std::ops::MulAssign for Custom {
    fn mul_assign(&mut self, other: Self) {
        *self = Self {
            int: self.int * other.int,
            float: self.float * other.float,
        }
    }
}

impl std::ops::DivAssign for Custom {
    fn div_assign(&mut self, other: Self) {
        *self = Self {
            int: self.int / other.int,
            float: self.float / other.float,
        }
    }
}

fn test_add<T: std::fmt::Debug + ElementArithmeticOps + 'static>(
    array: AtomicArray<T>,
    init_val: T,
    add_val: T,
) {
    array
        .dist_iter_mut()
        .for_each(move |elem| elem.store(init_val));
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    for i in 0..array.len() {
        array.add(i, add_val);
    }
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    let mut reqs = vec![];
    for i in 0..array.len() {
        reqs.push(array.fetch_add(i, add_val));
    }
    for (i, req) in reqs.drain(0..).enumerate() {
        println!("i: {:?} {:?}", i, array.block_on(req));
    }
    array.barrier();
    array.print();
    array.barrier();
}

fn test_sub<T: std::fmt::Debug + ElementArithmeticOps + 'static>(
    array: AtomicArray<T>,
    init_val: T,
    sub_val: T,
) {
    array
        .dist_iter_mut()
        .for_each(move |elem| elem.store(init_val));
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    for i in 0..array.len() {
        array.sub(i, sub_val);
    }
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    let mut reqs = vec![];
    for i in 0..array.len() {
        reqs.push(array.fetch_sub(i, sub_val));
    }
    for (i, req) in reqs.drain(0..).enumerate() {
        println!("i: {:?} {:?}", i, array.block_on(req));
    }
    array.barrier();
    array.print();
    array.barrier();
}

fn test_mul<T: std::fmt::Debug + ElementArithmeticOps + 'static>(
    array: AtomicArray<T>,
    init_val: T,
    mul_val: T,
) {
    array
        .dist_iter_mut()
        .for_each(move |elem| elem.store(init_val));
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    for i in 0..array.len() {
        array.mul(i, mul_val);
    }
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    let mut reqs = vec![];
    for i in 0..array.len() {
        reqs.push(array.fetch_mul(i, mul_val));
    }
    for (i, req) in reqs.drain(0..).enumerate() {
        println!("i: {:?} {:?}", i, array.block_on(req));
    }
    array.barrier();
    array.print();
    array.barrier();
}

fn test_div<T: std::fmt::Debug + ElementArithmeticOps + 'static>(
    array: AtomicArray<T>,
    init_val: T,
    div_val: T,
) {
    array
        .dist_iter_mut()
        .for_each(move |elem| elem.store(init_val));
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    for i in 0..array.len() {
        array.div(i, div_val);
    }
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    let mut reqs = vec![];
    for i in 0..array.len() {
        reqs.push(array.fetch_div(i, div_val));
    }
    for (i, req) in reqs.drain(0..).enumerate() {
        println!("i: {:?} {:?}", i, array.block_on(req));
    }
    array.barrier();
    array.print();
    array.barrier();
}

fn test_and<T: std::fmt::Debug + ElementArithmeticOps + ElementBitWiseOps + 'static>(
    array: AtomicArray<T>,
    init_val: T,
    and_val: T,
) {
    array
        .dist_iter_mut()
        .for_each(move |elem| elem.store(init_val));
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    for i in 0..array.len() {
        array.bit_and(i, and_val);
    }
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    array
        .dist_iter_mut()
        .for_each(move |elem| elem.store(init_val));
    array.wait_all();
    array.barrier();
    let mut reqs = vec![];
    for i in 0..array.len() {
        reqs.push(array.fetch_bit_and(i, and_val));
    }
    for (i, req) in reqs.drain(0..).enumerate() {
        println!("i: {:?} {:?}", i, array.block_on(req));
    }
    array.barrier();
    array.print();
    array.barrier();
}

fn test_or<T: std::fmt::Debug + ElementBitWiseOps + 'static>(
    array: AtomicArray<T>,
    init_val: T,
    or_val: T,
) {
    array
        .dist_iter_mut()
        .for_each(move |elem| elem.store(init_val));
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    for i in 0..array.len() {
        array.bit_or(i, or_val);
    }
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    array
        .dist_iter_mut()
        .for_each(move |elem| elem.store(init_val));
    array.wait_all();
    array.barrier();
    let mut reqs = vec![];
    for i in 0..array.len() {
        reqs.push(array.fetch_bit_or(i, or_val));
    }
    for (i, req) in reqs.drain(0..).enumerate() {
        println!("i: {:?} {:?}", i, array.block_on(req));
    }
    array.barrier();
    array.print();
    array.barrier();
}

fn test_store_load<T: std::fmt::Debug + ElementOps + 'static>(
    array: AtomicArray<T>,
    init_val: T,
    store_val: T,
    my_pe: usize,
    num_pes: usize,
) {
    array
        .dist_iter_mut()
        .for_each(move |elem| elem.store(init_val));
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    for i in (my_pe..array.len()).step_by(num_pes) {
        array.store(i, store_val);
    }
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();

    let mut reqs = vec![];
    for i in 0..array.len() {
        reqs.push(array.load(i));
    }
    for (i, req) in reqs.drain(0..).enumerate() {
        println!("i: {:?} {:?}", i, array.block_on(req));
    }
    array.barrier();
    array.print();
    array.barrier();
}

fn main() {
    // let args: Vec<String> = std::env::args().collect();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let num_pes = world.num_pes();
    let my_pe = world.my_pe();
    let array_f64 = AtomicArray::<f64>::new(world.clone(), num_pes * 10, Distribution::Block); //non intrinsic atomic, non bitwise
    let array_u8 = AtomicArray::<u8>::new(world.clone(), num_pes * 10, Distribution::Block); //intrinsic atomic,  bitwise
    let array_i128 = AtomicArray::<i128>::new(world.clone(), num_pes * 10, Distribution::Block); //non intrinsic atomic,  bitwise
    let array_custom = AtomicArray::<Custom>::new(world.clone(), num_pes * 10, Distribution::Block); //non intrinsic atomic, non bitwise
    let array_bool = AtomicArray::<bool>::new(world.clone(), num_pes * 10, Distribution::Block);

    test_add(array_f64.clone(), 0.0, 1.0);
    test_add(array_u8.clone(), 0, 1);
    test_add(array_i128.clone(), 0, -1);
    test_add(
        array_custom.clone(),
        Custom { int: 0, float: 0.0 },
        Custom { int: 1, float: 1.0 },
    );
    (&array_u8).add(3, 1);
    array_u8.wait_all();
    array_u8.barrier();
    array_u8.print();
    array_u8.barrier();

    (&array_i128).add(3, 1);
    array_i128.wait_all();
    array_i128.barrier();
    array_i128.print();
    array_i128.barrier();

    (&array_f64).add(3, 1.0);
    array_f64.wait_all();
    array_f64.barrier();
    array_f64.print();
    array_f64.barrier();

    (&array_custom).add(3, Custom { int: 1, float: 1.0 });
    array_custom.wait_all();
    array_custom.barrier();
    array_custom.print();
    array_custom.barrier();
    println!("====================================================================");

    test_sub(array_f64.clone(), 10.0, 1.0);
    test_sub(array_u8.clone(), 10, 1);
    test_sub(array_i128.clone(), -10, 1);
    test_sub(
        array_custom.clone(),
        Custom {
            int: 10,
            float: 10.0,
        },
        Custom { int: 1, float: 1.0 },
    );
    (&array_u8).sub(3, 1);
    array_u8.wait_all();
    array_u8.barrier();
    array_u8.print();
    array_u8.barrier();

    (&array_i128).sub(3, -1);
    array_i128.wait_all();
    array_i128.barrier();
    array_i128.print();
    array_i128.barrier();

    (&array_f64).sub(3, 1.0);
    array_f64.wait_all();
    array_f64.barrier();
    array_f64.print();
    array_f64.barrier();

    (&array_custom).sub(3, Custom { int: 1, float: 1.0 });
    array_custom.wait_all();
    array_custom.barrier();
    array_custom.print();
    array_custom.barrier();
    println!("====================================================================");

    test_mul(array_f64.clone(), 1.0, 2.5);
    test_mul(array_u8.clone(), 1, 2);
    test_mul(array_i128.clone(), 1, -2);
    test_mul(
        array_custom.clone(),
        Custom { int: 1, float: 1.0 },
        Custom { int: 2, float: 2.5 },
    );
    (&array_u8).mul(3, 2);
    array_u8.wait_all();
    array_u8.barrier();
    array_u8.print();
    array_u8.barrier();

    (&array_i128).mul(3, -2);
    array_i128.wait_all();
    array_i128.barrier();
    array_i128.print();
    array_i128.barrier();

    (&array_f64).mul(3, 2.5);
    array_f64.wait_all();
    array_f64.barrier();
    array_f64.print();
    array_f64.barrier();

    (&array_custom).mul(3, Custom { int: 1, float: 2.5 });
    array_custom.wait_all();
    array_custom.barrier();
    array_custom.print();
    array_custom.barrier();
    println!("====================================================================");

    test_div(array_f64.clone(), 1000.0, 2.5);
    test_div(array_u8.clone(), 255, 2);
    test_div(array_i128.clone(), 100000000, -2);
    test_div(
        array_custom.clone(),
        Custom {
            int: 1000,
            float: 1000.0,
        },
        Custom { int: 2, float: 2.5 },
    );
    (&array_u8).div(3, 2);
    array_u8.wait_all();
    array_u8.barrier();
    array_u8.print();
    array_u8.barrier();

    (&array_i128).div(3, 2);
    array_i128.wait_all();
    array_i128.barrier();
    array_i128.print();
    array_i128.barrier();

    (&array_f64).div(3, 2.5);
    array_f64.wait_all();
    array_f64.barrier();
    array_f64.print();
    array_f64.barrier();

    (&array_custom).div(3, Custom { int: 1, float: 2.5 });
    array_custom.wait_all();
    array_custom.barrier();
    array_custom.print();
    array_custom.barrier();
    println!("====================================================================");
    let and_val = 1 << my_pe;
    println!("and_val:  {:?}", and_val);
    test_and(array_u8.clone(), 255, and_val);
    test_and(array_i128.clone(), 1023, and_val.into());

    (&array_u8).bit_and(3, 1 << num_pes);
    array_u8.wait_all();
    array_u8.barrier();
    array_u8.print();
    array_u8.barrier();

    (&array_i128).bit_and(3, 1 << num_pes);
    array_i128.wait_all();
    array_i128.barrier();
    array_i128.print();
    array_i128.barrier();

    println!("====================================================================");
    let or_val = 1 << my_pe;
    test_or(array_u8.clone(), 0, or_val);
    test_or(array_i128.clone(), 0, or_val.into());
    (&array_u8).bit_or(3, 1 << num_pes);
    array_u8.wait_all();
    array_u8.barrier();
    array_u8.print();
    array_u8.barrier();
    (&array_i128).bit_or(3, 1 << num_pes);
    array_i128.wait_all();
    array_i128.barrier();
    array_i128.print();
    array_i128.barrier();

    println!("====================================================================");

    test_store_load(array_f64.clone(), 0.0, my_pe as f64, my_pe, num_pes);
    test_store_load(array_u8.clone(), 0, my_pe as u8, my_pe, num_pes);
    test_store_load(array_i128.clone(), 0, -(my_pe as i128), my_pe, num_pes);
    test_store_load(
        array_custom.clone(),
        Custom { int: 0, float: 0.0 },
        Custom {
            int: my_pe,
            float: -(my_pe as f32),
        },
        my_pe,
        num_pes,
    );
    (&array_u8).store(3, num_pes as u8);
    array_u8.wait_all();
    array_u8.barrier();
    array_u8.print();
    array_u8.barrier();

    (&array_i128).store(3, num_pes as i128);
    array_i128.wait_all();
    array_i128.barrier();
    array_i128.print();
    array_i128.barrier();

    (&array_f64).store(3, num_pes as f64);
    array_f64.wait_all();
    array_f64.barrier();
    array_f64.print();
    array_f64.barrier();

    (&array_custom).store(
        3,
        Custom {
            int: num_pes as usize,
            float: -(num_pes as f32),
        },
    );
    array_custom.wait_all();
    array_custom.barrier();
    array_custom.print();
    array_custom.barrier();
}

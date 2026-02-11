use lamellar::array::prelude::*;
use tracing_subscriber::fmt;

#[lamellar::AmData(
    Default,
    Debug,
    ArrayOps(Arithmetic, CompExEps, Shift),
    PartialEq,
    PartialOrd
)]
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

impl std::ops::RemAssign for Custom {
    fn rem_assign(&mut self, other: Self) {
        *self = Self {
            int: self.int % other.int,
            float: self.float % other.float,
        }
    }
}

impl std::ops::ShlAssign for Custom {
    fn shl_assign(&mut self, other: Custom) {
        self.int <<= other.int;
    }
}

impl std::ops::ShrAssign for Custom {
    fn shr_assign(&mut self, other: Custom) {
        self.int >>= other.int;
    }
}

fn test_add<T: std::fmt::Debug + ElementArithmeticOps + 'static>(
    array: AtomicArray<T>,
    init_val: T,
    add_val: T,
) {
    array
        .dist_iter_mut()
        .for_each(move |elem| elem.store(init_val))
        .block();
    array.barrier();
    array.print();
    array.barrier();
    for i in 0..array.len() {
        let _ = array.add(i, add_val).spawn();
    }
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    let mut reqs = vec![];
    for i in 0..array.len() {
        reqs.push(array.fetch_add(i, add_val).spawn());
    }
    for (i, req) in reqs.drain(0..).enumerate() {
        println!(
            "{:?} i: {:?}/{:?} {:?}",
            std::thread::current().id(),
            i,
            array.len(),
            req.block()
        );
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
        .for_each(move |elem| elem.store(init_val))
        .block();
    array.barrier();
    array.print();
    array.barrier();
    for i in 0..array.len() {
        let _ = array.sub(i, sub_val).spawn();
    }
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    let mut reqs = vec![];
    for i in 0..array.len() {
        reqs.push(array.fetch_sub(i, sub_val).spawn());
    }
    for (i, req) in reqs.drain(0..).enumerate() {
        println!("i: {:?} {:?}", i, req.block());
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
        .for_each(move |elem| elem.store(init_val))
        .block();
    array.barrier();
    array.print();
    array.barrier();
    for i in 0..array.len() {
        let _ = array.mul(i, mul_val).spawn();
    }
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    let mut reqs = vec![];
    for i in 0..array.len() {
        reqs.push(array.fetch_mul(i, mul_val).spawn());
    }
    for (i, req) in reqs.drain(0..).enumerate() {
        println!("i: {:?} {:?}", i, req.block());
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
        .for_each(move |elem| elem.store(init_val))
        .block();

    array.barrier();
    array.print();
    array.barrier();
    for i in 0..array.len() {
        let _ = array.div(i, div_val).spawn();
    }
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    let mut reqs = vec![];
    for i in 0..array.len() {
        reqs.push(array.fetch_div(i, div_val).spawn());
    }
    for (i, req) in reqs.drain(0..).enumerate() {
        println!("i: {:?} {:?}", i, req.block());
    }
    array.barrier();
    array.print();
    array.barrier();
}

fn test_rem<T: std::fmt::Debug + ElementArithmeticOps + 'static>(
    array: AtomicArray<T>,
    init_val: T,
    rem_val: T,
) {
    array
        .dist_iter_mut()
        .for_each(move |elem| elem.store(init_val))
        .block();

    array.barrier();
    array.print();
    array.barrier();
    for i in 0..array.len() {
        let _ = array.rem(i, rem_val).spawn();
    }
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    let mut reqs = vec![];
    for i in 0..array.len() {
        reqs.push(array.fetch_rem(i, rem_val).spawn());
    }
    for (i, req) in reqs.drain(0..).enumerate() {
        println!("i: {:?} {:?}", i, req.block());
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
        .for_each(move |elem| elem.store(init_val))
        .block();

    array.barrier();
    array.print();
    array.barrier();
    for i in 0..array.len() {
        let _ = array.bit_and(i, and_val).spawn();
    }
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    array
        .dist_iter_mut()
        .for_each(move |elem| elem.store(init_val))
        .block();

    array.barrier();
    let mut reqs = vec![];
    for i in 0..array.len() {
        reqs.push(array.fetch_bit_and(i, and_val).spawn());
    }
    for (i, req) in reqs.drain(0..).enumerate() {
        println!("i: {:?} {:?}", i, req.block());
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
        .for_each(move |elem| elem.store(init_val))
        .block();

    array.barrier();
    array.print();
    array.barrier();
    for i in 0..array.len() {
        let _ = array.bit_or(i, or_val).spawn();
    }
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    array
        .dist_iter_mut()
        .for_each(move |elem| elem.store(init_val))
        .block();

    array.barrier();
    let mut reqs = vec![];
    for i in 0..array.len() {
        reqs.push(array.fetch_bit_or(i, or_val).spawn());
    }
    for (i, req) in reqs.drain(0..).enumerate() {
        println!("i: {:?} {:?}", i, req.block());
    }
    array.barrier();
    array.print();
    array.barrier();
}

fn test_xor<T: std::fmt::Debug + ElementBitWiseOps + 'static>(
    array: AtomicArray<T>,
    init_val: T,
    xor_val: T,
) {
    array
        .dist_iter_mut()
        .for_each(move |elem| elem.store(init_val))
        .block();

    array.barrier();
    array.print();
    array.barrier();
    for i in 0..array.len() {
        let _ = array.bit_xor(i, xor_val).spawn();
    }
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    array
        .dist_iter_mut()
        .for_each(move |elem| elem.store(init_val))
        .block();

    array.barrier();
    let mut reqs = vec![];
    for i in 0..array.len() {
        reqs.push(array.fetch_bit_xor(i, xor_val).spawn());
    }
    for (i, req) in reqs.drain(0..).enumerate() {
        println!("i: {:?} {:?}", i, req.block());
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
        .for_each(move |elem| elem.store(init_val))
        .block();

    array.barrier();
    array.print();
    array.barrier();
    for i in (my_pe..array.len()).step_by(num_pes) {
        let _ = array.store(i, store_val).spawn();
    }
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();

    let mut reqs = vec![];
    for i in 0..array.len() {
        reqs.push(array.load(i).spawn());
    }
    for (i, req) in reqs.drain(0..).enumerate() {
        println!("i: {:?} {:?}", i, req.block());
    }
    array.barrier();
    array.print();
    array.barrier();
}

fn test_shl<T: std::fmt::Debug + ElementShiftOps + 'static>(
    array: AtomicArray<T>,
    init_val: T,
    shl_val: T,
) {
    array
        .dist_iter_mut()
        .for_each(move |elem| elem.store(init_val))
        .block();

    array.barrier();
    array.print();
    array.barrier();
    for i in 0..array.len() {
        let _ = array.shl(i, shl_val).spawn();
    }
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    let mut reqs = vec![];
    for i in 0..array.len() {
        reqs.push(array.fetch_shl(i, shl_val).spawn());
    }
    for (i, req) in reqs.drain(0..).enumerate() {
        println!("i: {:?} {:?}", i, req.block());
    }
    array.barrier();
    array.print();
    array.barrier();
}

fn test_shr<T: std::fmt::Debug + ElementShiftOps + 'static>(
    array: AtomicArray<T>,
    init_val: T,
    shr_val: T,
) {
    array
        .dist_iter_mut()
        .for_each(move |elem| elem.store(init_val))
        .block();

    array.barrier();
    array.print();
    array.barrier();
    for i in 0..array.len() {
        let _ = array.shr(i, shr_val).spawn();
    }
    array.wait_all();
    array.barrier();
    array.print();
    array.barrier();
    let mut reqs = vec![];
    for i in 0..array.len() {
        reqs.push(array.fetch_shr(i, shr_val).spawn());
    }
    for (i, req) in reqs.drain(0..).enumerate() {
        println!("i: {:?} {:?}", i, req.block());
    }
    array.barrier();
    array.print();
    array.barrier();
}

fn main() {
    // let args: Vec<String> = std::env::args().collect();
    let subscriber = fmt::init();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let num_pes = world.num_pes();
    let my_pe = world.my_pe();
    let array_f64 =
        AtomicArray::<f64>::new(world.clone(), num_pes * 10, Distribution::Block).block(); //non intrinsic atomic, non bitwise
    let array_u8 = AtomicArray::<u8>::new(world.clone(), num_pes * 10, Distribution::Block).block(); //intrinsic atomic,  bitwise
    let array_i128 =
        AtomicArray::<i128>::new(world.clone(), num_pes * 10, Distribution::Block).block(); //non intrinsic atomic,  bitwise
    let array_custom =
        AtomicArray::<Custom>::new(world.clone(), num_pes * 10, Distribution::Block).block(); //non intrinsic atomic, non bitwise
    let _array_bool =
        AtomicArray::<bool>::new(world.clone(), num_pes * 10, Distribution::Block).block();

    println!("ADD-----------------------");
    test_add(array_f64.clone(), 0.0, 1.0);
    test_add(array_u8.clone(), 0, 1);
    test_add(array_i128.clone(), 0, -1);
    test_add(
        array_custom.clone(),
        Custom { int: 0, float: 0.0 },
        Custom { int: 1, float: 1.0 },
    );
    let _ = (&array_u8).add(3, 1).spawn();
    array_u8.wait_all();
    array_u8.barrier();
    array_u8.print();
    array_u8.barrier();

    let _ = (&array_i128).add(3, 1).spawn();
    array_i128.wait_all();
    array_i128.barrier();
    array_i128.print();
    array_i128.barrier();

    let _ = (&array_f64).add(3, 1.0).spawn();
    array_f64.wait_all();
    array_f64.barrier();
    array_f64.print();
    array_f64.barrier();

    let _ = (&array_custom)
        .add(3, Custom { int: 1, float: 1.0 })
        .spawn();
    array_custom.wait_all();
    array_custom.barrier();
    array_custom.print();
    array_custom.barrier();
    println!("====================================================================");
    println!("SUB-----------------------");
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
    let _ = (&array_u8).sub(3, 1).spawn();
    array_u8.wait_all();
    array_u8.barrier();
    array_u8.print();
    array_u8.barrier();

    let _ = (&array_i128).sub(3, -1).spawn();
    array_i128.wait_all();
    array_i128.barrier();
    array_i128.print();
    array_i128.barrier();

    let _ = (&array_f64).sub(3, 1.0).spawn();
    array_f64.wait_all();
    array_f64.barrier();
    array_f64.print();
    array_f64.barrier();

    let _ = (&array_custom)
        .sub(3, Custom { int: 1, float: 1.0 })
        .spawn();
    array_custom.wait_all();
    array_custom.barrier();
    array_custom.print();
    array_custom.barrier();
    println!("====================================================================");

    println!("MUL-----------------------");
    test_mul(array_f64.clone(), 1.0, 2.5);
    test_mul(array_u8.clone(), 1, 2);
    test_mul(array_i128.clone(), 1, -2);
    test_mul(
        array_custom.clone(),
        Custom { int: 1, float: 1.0 },
        Custom { int: 2, float: 2.5 },
    );
    let _ = (&array_u8).mul(3, 2).spawn();
    array_u8.wait_all();
    array_u8.barrier();
    array_u8.print();
    array_u8.barrier();

    let _ = (&array_i128).mul(3, -2).spawn();
    array_i128.wait_all();
    array_i128.barrier();
    array_i128.print();
    array_i128.barrier();

    let _ = (&array_f64).mul(3, 2.5).spawn();
    array_f64.wait_all();
    array_f64.barrier();
    array_f64.print();
    array_f64.barrier();

    let _ = (&array_custom)
        .mul(3, Custom { int: 1, float: 2.5 })
        .spawn();
    array_custom.wait_all();
    array_custom.barrier();
    array_custom.print();
    array_custom.barrier();
    println!("====================================================================");

    println!("DIV-----------------------");
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
    let _ = (&array_u8).div(3, 2).spawn();
    array_u8.wait_all();
    array_u8.barrier();
    array_u8.print();
    array_u8.barrier();

    let _ = (&array_i128).div(3, 2).spawn();
    array_i128.wait_all();
    array_i128.barrier();
    array_i128.print();
    array_i128.barrier();

    let _ = (&array_f64).div(3, 2.5).spawn();
    array_f64.wait_all();
    array_f64.barrier();
    array_f64.print();
    array_f64.barrier();

    let _ = (&array_custom)
        .div(3, Custom { int: 1, float: 2.5 })
        .spawn();
    array_custom.wait_all();
    array_custom.barrier();
    array_custom.print();
    array_custom.barrier();
    println!("====================================================================");

    println!("REM-----------------------");
    test_rem(array_f64.clone(), 1000.0, 2.5);
    test_rem(array_u8.clone(), 255, 2);
    test_rem(array_i128.clone(), 100000000, -2);
    test_rem(
        array_custom.clone(),
        Custom {
            int: 1000,
            float: 1000.0,
        },
        Custom { int: 2, float: 2.5 },
    );
    let _ = (&array_u8).rem(3, 2).spawn();
    array_u8.wait_all();
    array_u8.barrier();
    array_u8.print();
    array_u8.barrier();

    let _ = (&array_i128).rem(3, 2).spawn();
    array_i128.wait_all();
    array_i128.barrier();
    array_i128.print();
    array_i128.barrier();

    let _ = (&array_f64).rem(3, 2.5).spawn();
    array_f64.wait_all();
    array_f64.barrier();
    array_f64.print();
    array_f64.barrier();

    let _ = (&array_custom)
        .rem(3, Custom { int: 1, float: 2.5 })
        .spawn();
    array_custom.wait_all();
    array_custom.barrier();
    array_custom.print();
    array_custom.barrier();
    println!("====================================================================");

    println!("AND-----------------------");
    let and_val = 1 << my_pe;
    println!("and_val:  {:?}", and_val);
    test_and(array_u8.clone(), 255, and_val);
    test_and(array_i128.clone(), 1023, and_val.into());

    let _ = (&array_u8).bit_and(3, 1 << num_pes).spawn();
    array_u8.wait_all();
    array_u8.barrier();
    array_u8.print();
    array_u8.barrier();

    let _ = (&array_i128).bit_and(3, 1 << num_pes).spawn();
    array_i128.wait_all();
    array_i128.barrier();
    array_i128.print();
    array_i128.barrier();

    println!("====================================================================");
    println!("OR-----------------------");
    let or_val = 1 << my_pe;
    test_or(array_u8.clone(), 0, or_val);
    test_or(array_i128.clone(), 0, or_val.into());
    let _ = (&array_u8).bit_or(3, 1 << num_pes).spawn();
    array_u8.wait_all();
    array_u8.barrier();
    array_u8.print();
    array_u8.barrier();
    let _ = (&array_i128).bit_or(3, 1 << num_pes).spawn();
    array_i128.wait_all();
    array_i128.barrier();
    array_i128.print();
    array_i128.barrier();

    println!("====================================================================");
    println!("XOR-----------------------");
    let xor_val = 1 << my_pe;
    test_xor(array_u8.clone(), 0, xor_val);
    test_xor(array_i128.clone(), 0, xor_val.into());
    let _ = (&array_u8).bit_xor(3, 1 << num_pes).spawn();
    array_u8.wait_all();
    array_u8.barrier();
    array_u8.print();
    array_u8.barrier();
    let _ = (&array_i128).bit_xor(3, 1 << num_pes).spawn();
    array_i128.wait_all();
    array_i128.barrier();
    array_i128.print();
    array_i128.barrier();

    println!("====================================================================");

    println!("STORE LOAD-----------------------");
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
    let _ = (&array_u8).store(3, num_pes as u8).spawn();
    array_u8.wait_all();
    array_u8.barrier();
    array_u8.print();
    array_u8.barrier();

    let _ = (&array_i128).store(3, num_pes as i128).spawn();
    array_i128.wait_all();
    array_i128.barrier();
    array_i128.print();
    array_i128.barrier();

    let _ = (&array_f64).store(3, num_pes as f64).spawn();
    array_f64.wait_all();
    array_f64.barrier();
    array_f64.print();
    array_f64.barrier();

    let _ = (&array_custom)
        .store(
            3,
            Custom {
                int: num_pes as usize,
                float: -(num_pes as f32),
            },
        )
        .spawn();
    array_custom.wait_all();
    array_custom.barrier();
    array_custom.print();
    array_custom.barrier();

    println!("====================================================================");
    println!("SHL-----------------------");
    test_shl(array_u8.clone(), 1, 3);
    test_shl(array_i128.clone(), 1, 63);
    test_shl(
        array_custom.clone(),
        Custom {
            int: 1,
            float: 1000.0,
        },
        Custom {
            int: 15,
            float: 0.0,
        },
    );
    let _ = (&array_u8).shl(1, 3).spawn();
    array_u8.wait_all();
    array_u8.barrier();
    array_u8.print();
    array_u8.barrier();

    let _ = (&array_i128).shl(1, 63).spawn();
    array_i128.wait_all();
    array_i128.barrier();
    array_i128.print();
    array_i128.barrier();

    let _ = (&array_custom)
        .shl(
            1,
            Custom {
                int: 15,
                float: 0.0,
            },
        )
        .spawn();
    array_custom.wait_all();
    array_custom.barrier();
    array_custom.print();
    array_custom.barrier();
    println!("====================================================================");
    println!("SHR-----------------------");
    test_shr(array_u8.clone(), !0, 3);
    test_shr(array_i128.clone(), i128::MAX, 63);
    test_shr(
        array_custom.clone(),
        Custom {
            int: !0,
            float: 1000.0,
        },
        Custom {
            int: 15,
            float: 0.0,
        },
    );
    let _ = (&array_u8).shr(1, 3).spawn();
    array_u8.wait_all();
    array_u8.barrier();
    array_u8.print();
    array_u8.barrier();

    let _ = (&array_i128).shr(1, 63).spawn();
    array_i128.wait_all();
    array_i128.barrier();
    array_i128.print();
    array_i128.barrier();

    let _ = (&array_custom)
        .shr(
            1,
            Custom {
                int: 15,
                float: 0.0,
            },
        )
        .spawn();
    array_custom.wait_all();
    array_custom.barrier();
    array_custom.print();
    array_custom.barrier();
    println!("====================================================================");
}

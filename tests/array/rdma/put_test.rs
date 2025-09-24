use lamellar::array::prelude::*;

macro_rules! initialize_array {
    (UnsafeArray,$array:ident,$init_val:ident) => {
        unsafe {
            $array
                .dist_iter_mut()
                .for_each(move |x| *x = $init_val)
                .block();
        }
    };
    (AtomicArray,$array:ident,$init_val:ident) => {
        $array
            .dist_iter()
            .for_each(move |x| x.store($init_val))
            .block();
    };
    (LocalLockArray,$array:ident,$init_val:ident) => {
        $array
            .dist_iter_mut()
            .for_each(move |x| *x = $init_val)
            .block();
    };
    (GlobalLockArray,$array:ident,$init_val:ident) => {
        $array
            .dist_iter_mut()
            .for_each(move |x| *x = $init_val)
            .block();
    };
}

macro_rules! onesided_iter {
    (GlobalLockArray,$array:ident) => {
        $array.read_lock().block().onesided_iter()
    };
    ($arraytype:ident,$array:ident) => {
        $array.onesided_iter()
    };
}

macro_rules! put_test {
    ($array:ident, $t:ty, $len:expr, $dist:ident) => {{
        let world = lamellar::LamellarWorldBuilder::new().build();
        let num_pes = world.num_pes();
        let my_pe = world.my_pe();
        let array_total_len = $len;
        let mut success = true;
        let array: $array<$t> = $array::<$t>::new(world.team(), array_total_len, $dist)
            .block()
            .into(); //convert into abstract LamellarArray, distributed len is total_len

        //initialize array
        let init_val = 0 as $t;
        initialize_array!($array, array, init_val);
        array.wait_all();
        array.barrier();
        // world.barrier();

        for idx in (my_pe..array_total_len).step_by(num_pes) {
            #[allow(unused_unsafe)]
            unsafe {
                let _ = array.put(idx, my_pe as $t).spawn();
            }
        }
        array.wait_all();
        array.barrier();
        #[allow(unused_unsafe)]
        for (i, elem) in unsafe { onesided_iter!($array, array).into_iter().enumerate() } {
            if (((i % num_pes) as $t - elem) as f32).abs() > 0.0001 {
                eprintln!(
                    "{:?} {:?} {:?}",
                    i as $t,
                    elem,
                    ((i as $t - elem) as f32).abs()
                );
                success = false;
            }
        }

        if !success {
            eprintln!("failed");
        }
    }};
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let array = args[1].clone();
    let dist = args[2].clone();
    let elem = args[3].clone();
    let len = args[4].parse::<usize>().unwrap();

    let dist_type = match dist.as_str() {
        "Block" => Distribution::Block,
        "Cyclic" => Distribution::Cyclic,
        _ => panic!("unsupported dist type"),
    };

    match array.as_str() {
        "UnsafeArray" => match elem.as_str() {
            "u8" => put_test!(UnsafeArray, u8, len, dist_type),
            "u16" => put_test!(UnsafeArray, u16, len, dist_type),
            "u32" => put_test!(UnsafeArray, u32, len, dist_type),
            "u64" => put_test!(UnsafeArray, u64, len, dist_type),
            "u128" => put_test!(UnsafeArray, u128, len, dist_type),
            "usize" => put_test!(UnsafeArray, usize, len, dist_type),
            "i8" => put_test!(UnsafeArray, i8, len, dist_type),
            "i16" => put_test!(UnsafeArray, i16, len, dist_type),
            "i32" => put_test!(UnsafeArray, i32, len, dist_type),
            "i64" => put_test!(UnsafeArray, i64, len, dist_type),
            "i128" => put_test!(UnsafeArray, i128, len, dist_type),
            "isize" => put_test!(UnsafeArray, isize, len, dist_type),
            "f32" => put_test!(UnsafeArray, f32, len, dist_type),
            "f64" => put_test!(UnsafeArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "AtomicArray" => match elem.as_str() {
            "u8" => put_test!(AtomicArray, u8, len, dist_type),
            "u16" => put_test!(AtomicArray, u16, len, dist_type),
            "u32" => put_test!(AtomicArray, u32, len, dist_type),
            "u64" => put_test!(AtomicArray, u64, len, dist_type),
            "u128" => put_test!(AtomicArray, u128, len, dist_type),
            "usize" => put_test!(AtomicArray, usize, len, dist_type),
            "i8" => put_test!(AtomicArray, i8, len, dist_type),
            "i16" => put_test!(AtomicArray, i16, len, dist_type),
            "i32" => put_test!(AtomicArray, i32, len, dist_type),
            "i64" => put_test!(AtomicArray, i64, len, dist_type),
            "i128" => put_test!(AtomicArray, i128, len, dist_type),
            "isize" => put_test!(AtomicArray, isize, len, dist_type),
            "f32" => put_test!(AtomicArray, f32, len, dist_type),
            "f64" => put_test!(AtomicArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "LocalLockArray" => match elem.as_str() {
            "u8" => put_test!(LocalLockArray, u8, len, dist_type),
            "u16" => put_test!(LocalLockArray, u16, len, dist_type),
            "u32" => put_test!(LocalLockArray, u32, len, dist_type),
            "u64" => put_test!(LocalLockArray, u64, len, dist_type),
            "u128" => put_test!(LocalLockArray, u128, len, dist_type),
            "usize" => put_test!(LocalLockArray, usize, len, dist_type),
            "i8" => put_test!(LocalLockArray, i8, len, dist_type),
            "i16" => put_test!(LocalLockArray, i16, len, dist_type),
            "i32" => put_test!(LocalLockArray, i32, len, dist_type),
            "i64" => put_test!(LocalLockArray, i64, len, dist_type),
            "i128" => put_test!(LocalLockArray, i128, len, dist_type),
            "isize" => put_test!(LocalLockArray, isize, len, dist_type),
            "f32" => put_test!(LocalLockArray, f32, len, dist_type),
            "f64" => put_test!(LocalLockArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "GlobalLockArray" => match elem.as_str() {
            "u8" => put_test!(GlobalLockArray, u8, len, dist_type),
            "u16" => put_test!(GlobalLockArray, u16, len, dist_type),
            "u32" => put_test!(GlobalLockArray, u32, len, dist_type),
            "u64" => put_test!(GlobalLockArray, u64, len, dist_type),
            "u128" => put_test!(GlobalLockArray, u128, len, dist_type),
            "usize" => put_test!(GlobalLockArray, usize, len, dist_type),
            "i8" => put_test!(GlobalLockArray, i8, len, dist_type),
            "i16" => put_test!(GlobalLockArray, i16, len, dist_type),
            "i32" => put_test!(GlobalLockArray, i32, len, dist_type),
            "i64" => put_test!(GlobalLockArray, i64, len, dist_type),
            "i128" => put_test!(GlobalLockArray, i128, len, dist_type),
            "isize" => put_test!(GlobalLockArray, isize, len, dist_type),
            "f32" => put_test!(GlobalLockArray, f32, len, dist_type),
            "f64" => put_test!(GlobalLockArray, f64, len, dist_type),
            _ => {} //eprintln!("unsupported element type"),
        },
        _ => eprintln!("unsupported array type"),
    }
}

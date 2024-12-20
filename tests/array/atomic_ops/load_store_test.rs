use lamellar::array::prelude::*;

macro_rules! initialize_array {
    (UnsafeArray,$array:ident,$init_val:ident) => {
        $array
            .dist_iter_mut()
            .for_each(move |x| *x = $init_val)
            .block();
        $array.barrier();
    };
    (AtomicArray,$array:ident,$init_val:ident) => {
        $array
            .dist_iter()
            .for_each(move |x| x.store($init_val))
            .block();
        $array.barrier();
    };
    (LocalLockArray,$array:ident,$init_val:ident) => {
        $array
            .dist_iter_mut()
            .for_each(move |x| *x = $init_val)
            .block();
        $array.barrier();
    };
    (GlobalLockArray,$array:ident,$init_val:ident) => {
        $array
            .dist_iter_mut()
            .for_each(move |x| *x = $init_val)
            .block();
        $array.barrier();
    };
}

macro_rules! check_val {
    (UnsafeArray,$val:ident,$max_val:ident,$valid:ident) => {
        // UnsafeArray updates will be nondeterminstic so should not ever be considered safe/valid so for testing sake we just say they are
    };
    (AtomicArray,$val:ident,$max_val:ident,$valid:ident) => {
        if (($val - $max_val) as f32).abs() > 0.0001 {
            //all updates should be preserved
            $valid = false;
        }
    };
    (LocalLockArray,$val:ident,$max_val:ident,$valid:ident) => {
        if (($val - $max_val) as f32).abs() > 0.0001 {
            //all updates should be preserved
            $valid = false;
        }
    };
    (GlobalLockArray,$val:ident,$max_val:ident,$valid:ident) => {
        if (($val - $max_val) as f32).abs() > 0.0001 {
            //all updates should be preserved
            $valid = false;
        }
    };
}

// macro_rules! onesided_iter{
//     (GlobalLockArray,$array:ident) => {
//         $array.read_lock().block().onesided_iter()
//     };
//     ($arraytype:ident,$array:ident) => {
//        $array.onesided_iter()
//     };
// }

macro_rules! load_store_test {
    ($array:ident, $t:ty, $len:expr, $dist:ident) => {{
        let world = lamellar::LamellarWorldBuilder::new().build();
        let num_pes = world.num_pes();
        let my_pe = world.my_pe();
        let array_total_len = $len;
        #[allow(unused_mut)]
        let mut success = true;
        let array: $array<$t> = $array::<$t>::new(world.team(), array_total_len, $dist)
            .block()
            .into(); //convert into abstract LamellarArray, distributed len is total_len

        let init_val = (num_pes as $t);
        initialize_array!($array, array, init_val);
        array.wait_all();
        array.barrier();
        for idx in 0..array.len() {
            if idx % num_pes == my_pe {
                #[allow(unused_unsafe)]
                let _ = unsafe { array.store(idx, my_pe as $t).spawn() };
            }
        }
        array.wait_all();
        array.barrier();
        let mut reqs = vec![];
        for idx in 0..array.len() {
            #[allow(unused_unsafe)]
            reqs.push((unsafe { array.load(idx) }, idx));
        }
        for (req, idx) in reqs {
            let val = world.block_on(req);
            let check_val = (idx % num_pes) as $t;
            let val = val;
            check_val!($array, val, check_val, success);
            if !success {
                eprintln!("{:?} {:?} {:?}", idx, val, check_val);
            }
        }

        array.barrier();
        initialize_array!($array, array, init_val);
        array.wait_all();
        array.barrier();

        let half_len = array_total_len / 2;
        let start_i = half_len / 2;
        let end_i = start_i + half_len;
        let sub_array = array.sub_array(start_i..end_i);
        sub_array.barrier();
        for idx in 0..sub_array.len() {
            if idx % num_pes == my_pe {
                #[allow(unused_unsafe)]
                let _ = unsafe { sub_array.store(idx, my_pe as $t).spawn() };
            }
        }
        sub_array.wait_all();
        sub_array.barrier();

        let mut reqs = vec![];
        for idx in 0..sub_array.len() {
            #[allow(unused_unsafe)]
            reqs.push((unsafe { sub_array.load(idx) }, idx));
        }
        for (req, idx) in reqs {
            let val = world.block_on(req);
            let check_val = (idx % num_pes) as $t;
            let val = val;
            check_val!($array, val, check_val, success);
            if !success {
                eprintln!("{:?} {:?} {:?}", idx, val, check_val);
            }
        }

        sub_array.barrier();
        initialize_array!($array, array, init_val);
        sub_array.wait_all();
        sub_array.barrier();

        let pe_len = array_total_len / num_pes;
        for pe in 0..num_pes {
            let len = std::cmp::max(pe_len / 2, 1);
            let start_i = (pe * pe_len) + len / 2;
            let end_i = start_i + len;
            let sub_array = array.sub_array(start_i..end_i);
            sub_array.barrier();
            for idx in 0..sub_array.len() {
                if idx % num_pes == my_pe {
                    #[allow(unused_unsafe)]
                    let _ = unsafe { sub_array.store(idx, my_pe as $t).spawn() };
                }
            }
            sub_array.wait_all();
            sub_array.barrier();

            let mut reqs = vec![];
            for idx in 0..sub_array.len() {
                #[allow(unused_unsafe)]
                reqs.push((unsafe { sub_array.load(idx) }, idx));
            }
            for (req, idx) in reqs {
                let val = world.block_on(req);
                let check_val = (idx % num_pes) as $t;
                let val = val;
                check_val!($array, val, check_val, success);
                if !success {
                    eprintln!("{:?} {:?} {:?}", idx, val, check_val);
                }
            }

            sub_array.barrier();
            initialize_array!($array, array, init_val);
            sub_array.wait_all();
            sub_array.barrier();
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
        "Block" => lamellar::array::Distribution::Block,
        "Cyclic" => lamellar::array::Distribution::Cyclic,
        _ => panic!("unsupported dist type"),
    };

    match array.as_str() {
        "AtomicArray" => match elem.as_str() {
            "u8" => load_store_test!(AtomicArray, u8, len, dist_type),
            "u16" => load_store_test!(AtomicArray, u16, len, dist_type),
            "u32" => load_store_test!(AtomicArray, u32, len, dist_type),
            "u64" => load_store_test!(AtomicArray, u64, len, dist_type),
            "u128" => load_store_test!(AtomicArray, u128, len, dist_type),
            "usize" => load_store_test!(AtomicArray, usize, len, dist_type),
            "i8" => load_store_test!(AtomicArray, i8, len, dist_type),
            "i16" => load_store_test!(AtomicArray, i16, len, dist_type),
            "i32" => load_store_test!(AtomicArray, i32, len, dist_type),
            "i64" => load_store_test!(AtomicArray, i64, len, dist_type),
            "i128" => load_store_test!(AtomicArray, i128, len, dist_type),
            "isize" => load_store_test!(AtomicArray, isize, len, dist_type),
            "f32" => load_store_test!(AtomicArray, f32, len, dist_type),
            "f64" => load_store_test!(AtomicArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "LocalLockArray" => match elem.as_str() {
            "u8" => load_store_test!(LocalLockArray, u8, len, dist_type),
            "u16" => load_store_test!(LocalLockArray, u16, len, dist_type),
            "u32" => load_store_test!(LocalLockArray, u32, len, dist_type),
            "u64" => load_store_test!(LocalLockArray, u64, len, dist_type),
            "u128" => load_store_test!(LocalLockArray, u128, len, dist_type),
            "usize" => load_store_test!(LocalLockArray, usize, len, dist_type),
            "i8" => load_store_test!(LocalLockArray, i8, len, dist_type),
            "i16" => load_store_test!(LocalLockArray, i16, len, dist_type),
            "i32" => load_store_test!(LocalLockArray, i32, len, dist_type),
            "i64" => load_store_test!(LocalLockArray, i64, len, dist_type),
            "i128" => load_store_test!(LocalLockArray, i128, len, dist_type),
            "isize" => load_store_test!(LocalLockArray, isize, len, dist_type),
            "f32" => load_store_test!(LocalLockArray, f32, len, dist_type),
            "f64" => load_store_test!(LocalLockArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "GlobalLockArray" => match elem.as_str() {
            "u8" => load_store_test!(GlobalLockArray, u8, len, dist_type),
            "u16" => load_store_test!(GlobalLockArray, u16, len, dist_type),
            "u32" => load_store_test!(GlobalLockArray, u32, len, dist_type),
            "u64" => load_store_test!(GlobalLockArray, u64, len, dist_type),
            "u128" => load_store_test!(GlobalLockArray, u128, len, dist_type),
            "usize" => load_store_test!(GlobalLockArray, usize, len, dist_type),
            "i8" => load_store_test!(GlobalLockArray, i8, len, dist_type),
            "i16" => load_store_test!(GlobalLockArray, i16, len, dist_type),
            "i32" => load_store_test!(GlobalLockArray, i32, len, dist_type),
            "i64" => load_store_test!(GlobalLockArray, i64, len, dist_type),
            "i128" => load_store_test!(GlobalLockArray, i128, len, dist_type),
            "isize" => load_store_test!(GlobalLockArray, isize, len, dist_type),
            "f32" => load_store_test!(GlobalLockArray, f32, len, dist_type),
            "f64" => load_store_test!(GlobalLockArray, f64, len, dist_type),
            _ => {} //eprintln!("unsupported element type"),
        },
        _ => eprintln!("unsupported array type"),
    }
}

use lamellar::array::prelude::*;

use rand::distributions::Distribution;
use rand::distributions::Uniform;

macro_rules! initialize_array {
    (UnsafeArray,$array:ident,$init_val:ident) => {
        unsafe {
            $array
                .dist_iter_mut()
                .for_each(move |x| *x = $init_val)
                .block();
        }
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
    (UnsafeArray,$val:ident,$min_val:ident,$valid:ident) => {
        // UnsafeArray updates will be nondeterminstic so should not ever be considered safe/valid so for testing sake we just say they are
    };
    (AtomicArray,$val:ident,$min_val:ident,$valid:ident) => {
        if (($val - $min_val) as f32).abs() > 0.0001 {
            //all updates should be preserved
            $valid = false;
        }
    };
    (LocalLockArray,$val:ident,$min_val:ident,$valid:ident) => {
        if (($val - $min_val) as f32).abs() > 0.0001 {
            //all updates should be preserved
            $valid = false;
        }
    };
    (GlobalLockArray,$val:ident,$min_val:ident,$valid:ident) => {
        if (($val - $min_val) as f32).abs() > 0.0001 {
            //all updates should be preserved
            $valid = false;
        }
    };
}

macro_rules! insert_prev{
    (UnsafeArray,$val:ident,$prevs:ident) => {
       // UnsafeArray updates will be nondeterminstic so should not ever be considered safe/valid so for testing sake we just say they are
       true
    };
    (AtomicArray,$val:ident,$prevs:ident) => {
        $prevs.insert($val)
    };
    (LocalLockArray,$val:ident,$prevs:ident) => {
        $prevs.insert($val)
    };
    (GlobalLockArray,$val:ident,$prevs:ident) => {
        $prevs.insert($val)
    };
}

macro_rules! max_updates {
    ($t:ty,$num_pes:ident) => {
        if <$t>::MAX as u128 > 1000 as u128 {
            1000 / $num_pes
        } else {
            <$t>::MAX as usize / $num_pes
        }
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

macro_rules! fetch_sub_test {
    ($array:ident, $t:ty, $len:expr, $dist:ident) => {{
        let world = lamellar::LamellarWorldBuilder::new().build();
        let num_pes = world.num_pes();
        let _my_pe = world.my_pe();
        let array_total_len = $len;

        let mut rng = rand::thread_rng();
        let rand_idx = Uniform::from(0..array_total_len);
        let mut success = true;
        let array: $array<$t> = $array::<$t>::new(world.team(), array_total_len, $dist)
            .block()
            .into(); //convert into abstract LamellarArray, distributed len is total_len

        let pe_max_val: $t = 10 as $t;
        let max_val = pe_max_val * num_pes as $t;
        let init_val = max_val as $t;
        #[allow(unused)]
        let zero = 0 as $t;
        initialize_array!($array, array, init_val);
        array.wait_all();
        array.barrier();
        for idx in 0..array.len() {
            let mut reqs = vec![];
            for _i in 0..(pe_max_val as usize) {
                #[allow(unused_unsafe)]
                reqs.push(unsafe { array.fetch_sub(idx, 1 as $t) });
            }
            #[allow(unused_mut)]
            let mut prevs: std::collections::HashSet<u128> = std::collections::HashSet::new();
            for req in reqs {
                let val = world.block_on(req) as u128;
                if !insert_prev!($array, val, prevs) {
                    eprintln!("full 1: {:?} {:?} {:?}", init_val, val, prevs);
                    success = false;
                    break;
                }
            }
        }
        array.barrier();
        #[allow(unused_unsafe)]
        for (i, elem) in unsafe { onesided_iter!($array, array).into_iter().enumerate() } {
            let val = *elem;
            check_val!($array, val, zero, success);
            if !success {
                eprintln!("{:?} {:?} {:?}", i, val, max_val);
            }
        }
        array.barrier();
        let num_updates = max_updates!($t, num_pes);
        let tot_updates = (num_updates * num_pes) as $t;
        initialize_array!($array, array, tot_updates);
        array.wait_all();
        array.barrier();
        // let mut prev_vals = vec![tot_updates as $t;array.len()];

        let mut reqs = vec![];
        // println!("2------------");
        for _i in 0..num_updates {
            let idx = rand_idx.sample(&mut rng);
            #[allow(unused_unsafe)]
            reqs.push((unsafe { array.fetch_sub(idx, 1 as $t) }, idx))
        }
        for (req, _idx) in reqs {
            let _val = world.block_on(req);
        }

        array.barrier();
        #[allow(unused_unsafe)]
        let sum = unsafe {
            onesided_iter!($array, array)
                .into_iter()
                .fold(0, |acc, x| acc + *x as usize)
        };
        let calced_sum = tot_updates as usize * (array.len() - 1);
        check_val!($array, sum, calced_sum, success);
        if !success {
            eprintln!("{:?} {:?} {:?}", sum, calced_sum, (array.len() - 1));
        }
        world.wait_all();
        world.barrier();
        initialize_array!($array, array, init_val);

        let half_len = array_total_len / 2;
        let start_i = half_len / 2;
        let end_i = start_i + half_len;
        let rand_idx = Uniform::from(0..half_len);
        let sub_array = array.sub_array(start_i..end_i);
        sub_array.barrier();
        for idx in 0..sub_array.len() {
            let mut reqs = vec![];
            for _i in 0..(pe_max_val as usize) {
                #[allow(unused_unsafe)]
                reqs.push(unsafe { sub_array.fetch_sub(idx, 1 as $t) });
            }
            #[allow(unused_mut)]
            let mut prevs: std::collections::HashSet<u128> = std::collections::HashSet::new();
            for req in reqs {
                let val = world.block_on(req) as u128;
                if !insert_prev!($array, val, prevs) {
                    eprintln!("half 1: {:?} {:?}", val, prevs);
                    success = false;
                    break;
                }
            }
        }
        sub_array.barrier();
        #[allow(unused_unsafe)]
        for (i, elem) in unsafe { onesided_iter!($array, sub_array).into_iter().enumerate() } {
            let val = *elem;
            check_val!($array, val, zero, success);
            if !success {
                eprintln!("{:?} {:?} {:?}", i, val, max_val);
            }
        }
        sub_array.barrier();
        let num_updates = max_updates!($t, num_pes);
        let tot_updates = (num_updates * num_pes) as $t;
        initialize_array!($array, array, tot_updates);
        sub_array.wait_all();
        sub_array.barrier();
        // let mut prev_vals = vec![tot_updates ;sub_array.len()];
        let mut reqs = vec![];
        // println!("2------------");
        for _i in 0..num_updates {
            let idx = rand_idx.sample(&mut rng);
            #[allow(unused_unsafe)]
            reqs.push((unsafe { sub_array.fetch_sub(idx, 1 as $t) }, idx))
        }
        for (req, _idx) in reqs {
            let _val = world.block_on(req);
        }
        sub_array.barrier();
        #[allow(unused_unsafe)]
        let sum = unsafe {
            onesided_iter!($array, sub_array)
                .into_iter()
                .fold(0, |acc, x| acc + *x as usize)
        };
        let calced_sum = tot_updates as usize * (sub_array.len() - 1);
        check_val!($array, sum, calced_sum, success);
        if !success {
            eprintln!("{:?} {:?} {:?}", sum, calced_sum, (sub_array.len() - 1));
        }
        sub_array.wait_all();
        sub_array.barrier();
        initialize_array!($array, array, init_val);

        let pe_len = array_total_len / num_pes;
        for pe in 0..num_pes {
            let len = std::cmp::max(pe_len / 2, 1);
            let start_i = (pe * pe_len) + len / 2;
            let end_i = start_i + len;
            let rand_idx = Uniform::from(0..len);
            let sub_array = array.sub_array(start_i..end_i);
            sub_array.barrier();
            for idx in 0..sub_array.len() {
                let mut reqs = vec![];
                for _i in 0..(pe_max_val as usize) {
                    #[allow(unused_unsafe)]
                    reqs.push(unsafe { sub_array.fetch_sub(idx, 1 as $t) });
                }
                #[allow(unused_mut)]
                let mut prevs: std::collections::HashSet<u128> = std::collections::HashSet::new();
                for req in reqs {
                    let val = world.block_on(req) as u128;
                    if !insert_prev!($array, val, prevs) {
                        eprintln!("pe 1: {:?} {:?}", val, prevs);
                        success = false;
                        break;
                    }
                }
            }
            sub_array.barrier();
            #[allow(unused_unsafe)]
            for (i, elem) in unsafe { onesided_iter!($array, sub_array).into_iter().enumerate() } {
                let val = *elem;
                check_val!($array, val, zero, success);
                if !success {
                    eprintln!("{:?} {:?} {:?}", i, val, max_val);
                }
            }
            sub_array.barrier();
            let num_updates = max_updates!($t, num_pes);
            let tot_updates = (num_updates * num_pes) as $t;
            initialize_array!($array, array, tot_updates);
            sub_array.wait_all();
            sub_array.barrier();
            let mut reqs = vec![];
            // println!("2------------");
            for _i in 0..num_updates {
                let idx = rand_idx.sample(&mut rng);
                #[allow(unused_unsafe)]
                reqs.push((unsafe { sub_array.fetch_sub(idx, 1 as $t) }, idx))
            }
            for (req, _idx) in reqs {
                let _val = world.block_on(req);
            }

            sub_array.barrier();
            #[allow(unused_unsafe)]
            let sum = unsafe {
                onesided_iter!($array, sub_array)
                    .into_iter()
                    .fold(0, |acc, x| acc + *x as usize)
            };
            let calced_sum = tot_updates as usize * (sub_array.len() - 1);
            check_val!($array, sum, calced_sum, success);
            if !success {
                eprintln!("{:?} {:?} {:?}", sum, calced_sum, (sub_array.len() - 1));
            }
            sub_array.wait_all();
            sub_array.barrier();
            initialize_array!($array, array, init_val);
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
        "UnsafeArray" => match elem.as_str() {
            "u8" => fetch_sub_test!(UnsafeArray, u8, len, dist_type),
            "u16" => fetch_sub_test!(UnsafeArray, u16, len, dist_type),
            "u32" => fetch_sub_test!(UnsafeArray, u32, len, dist_type),
            "u64" => fetch_sub_test!(UnsafeArray, u64, len, dist_type),
            "u128" => fetch_sub_test!(UnsafeArray, u128, len, dist_type),
            "usize" => fetch_sub_test!(UnsafeArray, usize, len, dist_type),
            "i8" => fetch_sub_test!(UnsafeArray, i8, len, dist_type),
            "i16" => fetch_sub_test!(UnsafeArray, i16, len, dist_type),
            "i32" => fetch_sub_test!(UnsafeArray, i32, len, dist_type),
            "i64" => fetch_sub_test!(UnsafeArray, i64, len, dist_type),
            "i128" => fetch_sub_test!(UnsafeArray, i128, len, dist_type),
            "isize" => fetch_sub_test!(UnsafeArray, isize, len, dist_type),
            "f32" => fetch_sub_test!(UnsafeArray, f32, len, dist_type),
            "f64" => fetch_sub_test!(UnsafeArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "AtomicArray" => match elem.as_str() {
            "u8" => fetch_sub_test!(AtomicArray, u8, len, dist_type),
            "u16" => fetch_sub_test!(AtomicArray, u16, len, dist_type),
            "u32" => fetch_sub_test!(AtomicArray, u32, len, dist_type),
            "u64" => fetch_sub_test!(AtomicArray, u64, len, dist_type),
            "u128" => fetch_sub_test!(AtomicArray, u128, len, dist_type),
            "usize" => fetch_sub_test!(AtomicArray, usize, len, dist_type),
            "i8" => fetch_sub_test!(AtomicArray, i8, len, dist_type),
            "i16" => fetch_sub_test!(AtomicArray, i16, len, dist_type),
            "i32" => fetch_sub_test!(AtomicArray, i32, len, dist_type),
            "i64" => fetch_sub_test!(AtomicArray, i64, len, dist_type),
            "i128" => fetch_sub_test!(AtomicArray, i128, len, dist_type),
            "isize" => fetch_sub_test!(AtomicArray, isize, len, dist_type),
            "f32" => fetch_sub_test!(AtomicArray, f32, len, dist_type),
            "f64" => fetch_sub_test!(AtomicArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "LocalLockArray" => match elem.as_str() {
            "u8" => fetch_sub_test!(LocalLockArray, u8, len, dist_type),
            "u16" => fetch_sub_test!(LocalLockArray, u16, len, dist_type),
            "u32" => fetch_sub_test!(LocalLockArray, u32, len, dist_type),
            "u64" => fetch_sub_test!(LocalLockArray, u64, len, dist_type),
            "u128" => fetch_sub_test!(LocalLockArray, u128, len, dist_type),
            "usize" => fetch_sub_test!(LocalLockArray, usize, len, dist_type),
            "i8" => fetch_sub_test!(LocalLockArray, i8, len, dist_type),
            "i16" => fetch_sub_test!(LocalLockArray, i16, len, dist_type),
            "i32" => fetch_sub_test!(LocalLockArray, i32, len, dist_type),
            "i64" => fetch_sub_test!(LocalLockArray, i64, len, dist_type),
            "i128" => fetch_sub_test!(LocalLockArray, i128, len, dist_type),
            "isize" => fetch_sub_test!(LocalLockArray, isize, len, dist_type),
            "f32" => fetch_sub_test!(LocalLockArray, f32, len, dist_type),
            "f64" => fetch_sub_test!(LocalLockArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "GlobalLockArray" => match elem.as_str() {
            "u8" => fetch_sub_test!(GlobalLockArray, u8, len, dist_type),
            "u16" => fetch_sub_test!(GlobalLockArray, u16, len, dist_type),
            "u32" => fetch_sub_test!(GlobalLockArray, u32, len, dist_type),
            "u64" => fetch_sub_test!(GlobalLockArray, u64, len, dist_type),
            "u128" => fetch_sub_test!(GlobalLockArray, u128, len, dist_type),
            "usize" => fetch_sub_test!(GlobalLockArray, usize, len, dist_type),
            "i8" => fetch_sub_test!(GlobalLockArray, i8, len, dist_type),
            "i16" => fetch_sub_test!(GlobalLockArray, i16, len, dist_type),
            "i32" => fetch_sub_test!(GlobalLockArray, i32, len, dist_type),
            "i64" => fetch_sub_test!(GlobalLockArray, i64, len, dist_type),
            "i128" => fetch_sub_test!(GlobalLockArray, i128, len, dist_type),
            "isize" => fetch_sub_test!(GlobalLockArray, isize, len, dist_type),
            "f32" => fetch_sub_test!(GlobalLockArray, f32, len, dist_type),
            "f64" => fetch_sub_test!(GlobalLockArray, f64, len, dist_type),
            _ => {} //eprintln!("unsupported element type"),
        },
        _ => eprintln!("unsupported array type"),
    }
}

use lamellar::array::prelude::*;
use lamellar::memregion::prelude::*;

use rand::distributions::Distribution;
use rand::distributions::Uniform;

// use std::ops::Deref;

macro_rules! initialize_array {
    (UnsafeArray,$array:ident,$init_val:ident) => {
        unsafe {
            $array
                .dist_iter_mut()
                .for_each(move |x| *x = $init_val)
                .block()
        };
        $array.barrier();
    };
    (AtomicArray,$array:ident,$init_val:ident) => {
        $array
            .dist_iter()
            .enumerate()
            .for_each(move |(_i, x)| x.store($init_val))
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
        if (($val as f64 - $max_val as f64) as f32).abs() > 0.0001 {
            //all updates should be preserved
            $valid = false;
        }
    };
    (LocalLockArray,$val:ident,$max_val:ident,$valid:ident) => {
        if (($val as f64 - $max_val as f64) as f32).abs() > 0.0001 {
            //all updates should be preserved
            $valid = false;
        }
    };
    (GlobalLockArray,$val:ident,$max_val:ident,$valid:ident) => {
        if (($val as f64 - $max_val as f64) as f32).abs() > 0.0001 {
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
        if <$t>::MAX as u128 > (1000 * $num_pes) as u128 {
            1000
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

macro_rules! buffered_onesided_iter {
    (GlobalLockArray,$array:ident) => {
        $array
            .read_lock()
            .block()
            .buffered_onesided_iter($array.len())
    };
    ($arraytype:ident,$array:ident) => {
        $array.buffered_onesided_iter($array.len())
    };
}

macro_rules! fetch_add_test {
    ($array:ident, $t:ty, $len:expr, $dist:ident) => {{
        let world = lamellar::LamellarWorldBuilder::new().build();
        let num_pes = world.num_pes();
        let _my_pe = world.my_pe();
        let array_total_len = $len;

        let mut rng = rand::thread_rng();
        let rand_idx = Uniform::from(0..array_total_len);
        #[allow(unused_mut)]
        let mut success = true;
        let array: $array<$t> = $array::<$t>::new(world.team(), array_total_len, $dist)
            .block()
            .into(); //convert into abstract LamellarArray, distributed len is total_len

        let pe_max_val: $t = 10 as $t;
        let max_val = pe_max_val * num_pes as $t;
        let init_val = 0 as $t;
        initialize_array!($array, array, init_val);
        array.wait_all();
        array.barrier();
        for idx in 0..array.len() {
            let mut reqs = vec![];
            for _i in 0..(pe_max_val as usize) {
                #[allow(unused_unsafe)]
                reqs.push(unsafe { array.fetch_add(idx, 1 as $t).spawn() });
            }
            #[allow(unused_mut)]
            let mut prevs: std::collections::HashSet<u128> = std::collections::HashSet::new();
            for req in reqs {
                let val = world.block_on(req) as u128;
                if !insert_prev!($array, val, prevs) {
                    eprintln!("full 1: {:?} {:?}", val, prevs);
                    success = false;
                }
            }
        }
        // array.wait_all();
        array.barrier();
        #[allow(unused_unsafe)]
        for (i, elem) in unsafe { onesided_iter!($array, array).into_iter().enumerate() } {
            let val = *elem;
            check_val!($array, val, max_val, success);
            if !success {
                eprintln!("full 2: {:?} {:?} {:?}", i, val, max_val);
            }
        }
        array.barrier();
        // println!("1------------");
        initialize_array!($array, array, init_val);
        array.wait_all();
        array.barrier();
        let num_updates = max_updates!($t, num_pes);
        let mut reqs = vec![];
        // println!("2------------");
        for _i in 0..num_updates {
            let idx = rand_idx.sample(&mut rng);
            #[allow(unused_unsafe)]
            reqs.push((unsafe { array.fetch_add(idx, 1 as $t) }, idx))
        }
        for (req, _idx) in reqs {
            let _val = world.block_on(req) as usize;
        }
        array.barrier();
        #[allow(unused_unsafe)]
        let sum = unsafe {
            onesided_iter!($array, array)
                .into_iter()
                .fold(0, |acc, x| acc + *x as usize)
        };
        let tot_updates = num_updates * num_pes;
        check_val!($array, sum, tot_updates, success);
        if !success {
            eprintln!("full 4: {:?} {:?}", sum, tot_updates);
        }
        world.wait_all();
        world.barrier();
        // println!("2------------");
        initialize_array!($array, array, init_val);

        let half_len = array_total_len / 2;
        let start_i = half_len / 2;
        let end_i = start_i + half_len;
        let rand_idx = Uniform::from(0..half_len);
        let sub_array = array.sub_array(start_i..end_i);
        array.barrier();
        for idx in 0..sub_array.len() {
            let mut reqs = vec![];
            for _i in 0..(pe_max_val as usize) {
                #[allow(unused_unsafe)]
                reqs.push(unsafe { sub_array.fetch_add(idx, 1 as $t) });
            }
            #[allow(unused_mut)]
            let mut prevs: std::collections::HashSet<u128> = std::collections::HashSet::new();
            for req in reqs {
                let val = world.block_on(req) as u128;
                if !insert_prev!($array, val, prevs) {
                    eprintln!("half 1: {:?} {:?}", val, prevs);
                    success = false;
                }
            }
        }
        array.barrier();
        #[allow(unused_unsafe)]
        for (i, elem) in unsafe { onesided_iter!($array, sub_array).into_iter().enumerate() } {
            let val = *elem;
            check_val!($array, val, max_val, success);
            if !success {
                eprintln!("half 2: {:?} {:?} {:?}", i, val, max_val);
            }
        }
        array.barrier();
        // println!("3------------");
        initialize_array!($array, array, init_val);
        array.wait_all();
        array.barrier();
        let num_updates = max_updates!($t, num_pes);
        let mut reqs = vec![];
        for _i in 0..num_updates {
            let idx = rand_idx.sample(&mut rng);
            #[allow(unused_unsafe)]
            reqs.push((unsafe { sub_array.fetch_add(idx, 1 as $t) }, idx))
        }
        for (req, _idx) in reqs {
            let _val = world.block_on(req) as usize;
        }
        array.barrier();
        #[allow(unused_unsafe)]
        let sum = unsafe {
            onesided_iter!($array, sub_array)
                .into_iter()
                .fold(0, |acc, x| acc + *x as usize)
        };
        let tot_updates = num_updates * num_pes;
        check_val!($array, sum, tot_updates, success);
        if !success {
            eprintln!("half 4: {:?} {:?}", sum, tot_updates);
        }
        array.wait_all();
        array.barrier();
        // println!("4------------");
        initialize_array!($array, array, init_val);
        array.wait_all();
        array.barrier();

        let pe_len = array_total_len / num_pes;
        for pe in 0..num_pes {
            let len = std::cmp::max(pe_len / 2, 1);
            let start_i = (pe * pe_len) + len / 2;
            let end_i = start_i + len;
            let rand_idx = Uniform::from(0..len);
            let sub_array = array.sub_array(start_i..end_i);
            array.barrier();
            for idx in 0..sub_array.len() {
                let mut reqs = vec![];
                for _i in 0..(pe_max_val as usize) {
                    #[allow(unused_unsafe)]
                    reqs.push(unsafe { sub_array.fetch_add(idx, 1 as $t) });
                }
                #[allow(unused_mut)]
                let mut prevs: std::collections::HashSet<u128> = std::collections::HashSet::new();
                for req in reqs {
                    let val = world.block_on(req) as u128;
                    if !insert_prev!($array, val, prevs) {
                        eprintln!("pe 1: {:?} {:?}", val, prevs);
                        success = false;
                    }
                }
            }
            sub_array.barrier();
            #[allow(unused_unsafe)]
            for (i, elem) in unsafe { onesided_iter!($array, sub_array).into_iter().enumerate() } {
                let val = *elem;
                check_val!($array, val, max_val, success);
                if !success {
                    eprintln!("pe 2 {:?} {:?} {:?}", i, val, max_val);
                }
            }
            sub_array.barrier();
            // println!("5------------");
            initialize_array!($array, array, init_val);
            sub_array.wait_all();
            sub_array.barrier();
            let num_updates = max_updates!($t, num_pes);
            let mut reqs = vec![];
            for _i in 0..num_updates {
                let idx = rand_idx.sample(&mut rng);
                #[allow(unused_unsafe)]
                reqs.push((unsafe { sub_array.fetch_add(idx, 1 as $t) }, idx))
            }
            for (req, _idx) in reqs {
                let _val = world.block_on(req) as usize;
            }
            sub_array.barrier();
            #[allow(unused_unsafe)]
            let sum = unsafe {
                onesided_iter!($array, sub_array)
                    .into_iter()
                    .fold(0, |acc, x| acc + *x as usize)
            };
            let tot_updates = num_updates * num_pes;
            check_val!($array, sum, tot_updates, success);
            if !success {
                eprintln!("pe 4 {:?} {:?}", sum, tot_updates);
            }
            sub_array.wait_all();
            sub_array.barrier();
            // println!("6------------");
            initialize_array!($array, array, init_val);
        }

        if !success {
            eprintln!("failed");
        }
    }};
}

macro_rules! initialize_array2 {
    (UnsafeArray,$array:ident,$init_val:ident) => {
        #[allow(unused_unsafe)]
        unsafe {
            $array
                .dist_iter_mut()
                .enumerate()
                .for_each(move |(i, x)| *x = i)
                .block()
        };
        $array.barrier();
    };
    (AtomicArray,$array:ident,$init_val:ident) => {
        $array
            .dist_iter()
            .enumerate()
            .for_each(move |(i, x)| {
                // println!("{:?} {:?}", i, x.load());
                x.store(i)
            })
            .block();
        $array.barrier();
    };
    (LocalLockArray,$array:ident,$init_val:ident) => {
        $array
            .dist_iter_mut()
            .enumerate()
            .for_each(move |(i, x)| *x = i)
            .block();
        $array.barrier();
    };
    (GlobalLockArray,$array:ident,$init_val:ident) => {
        $array
            .dist_iter_mut()
            .enumerate()
            .for_each(move |(i, x)| *x = i)
            .block();
        $array.barrier();
    };
}

macro_rules! check_results {
    ($array_ty:ident, $array:ident, $num_pes:ident, $reqs:ident, $test:expr) => {
        let real_val = 0;
        check_results!($array_ty, $array, $num_pes,real_val,$reqs, $test);
    };
    ($array_ty:ident, $array:ident, $num_pes:ident, $real_val: ident, $reqs:ident, $test:expr) => {
        // println!("++++++++++++++++++++++++++++++++++++++++++++++");
        #[allow(unused_mut)]
        let mut success = true;
        $array.wait_all();
        $array.barrier();
        // println!("test {:?} reqs len {:?}", $test, $reqs.len());
        let mut req_cnt=0;
        for (i, req) in $reqs.drain(0..).enumerate() {
            let req =  $array.block_on(req);

            for (j, res) in req.iter().enumerate() {
                let check_val = if $real_val == 0  {
                    req_cnt + $num_pes
                }
                else{
                    $num_pes + $real_val
                };

                if !(res >= &0 && res < &check_val) {
                    success = false;
                    eprintln!("return i: {:?} j: {:?} check_val: {:?} val: {:?}, real_val: {:?}", i, j, check_val, res, $real_val);
                    // break;
                }
                req_cnt+=1;
            }
        }
        #[allow(unused_unsafe)]
        for (i, elem) in unsafe { buffered_onesided_iter!($array_ty,$array).into_iter().enumerate() }{
            let val = *elem;
            let real_val = if $real_val == 0  {
                i + $num_pes
            }
            else{
                if i >= $num_pes {
                    i
                }
                else{
                    i + $real_val
                }

            };
            // println!("val {:?} real_val {:?}", val, real_val);
            check_val!($array_ty, real_val, val, success);
            if !success {
                eprintln!("input {:?}: {:?} {:?} {:?}", $test, i, val, real_val);
                break;
            }
        }
        $array.barrier();
        // let init_val = 0;
        initialize_array2!($array_ty, $array, init_val);
        $array.wait_all();
        $array.barrier();
        // println!("--------------------------------------------------------------------------------");
    };
}

macro_rules! input_test {
    ($array:ident,  $len:expr, $dist:ident) => {{
        std::env::set_var("LAMELLAR_BATCH_OP_SIZE", "10");
        let world = lamellar::LamellarWorldBuilder::new().build();
        let num_pes = world.num_pes();
        let _my_pe = world.my_pe();
        let array_total_len = $len;

        // let mut success = true;
        let array: $array<usize> = $array::<usize>::new(world.team(), array_total_len, $dist)
            .block()
            .into(); //convert into abstract LamellarArray, distributed len is total_len
        let input_array: UnsafeArray<usize> =
            UnsafeArray::<usize>::new(world.team(), array_total_len * num_pes, $dist)
                .block()
                .into(); //convert into abstract LamellarArray, distributed len is total_len
                         // let init_val=0;
        initialize_array2!($array, array, init_val);
        if $dist == lamellar::array::Distribution::Block {
            #[allow(unused_unsafe)]
            unsafe {
                input_array
                    .dist_iter_mut()
                    .enumerate()
                    .for_each(move |(i, x)| {
                        /*println!("i: {:?}",i);*/
                        *x = i % array_total_len
                    })
                    .block()
            };
        } else {
            #[allow(unused_unsafe)]
            unsafe {
                input_array
                    .dist_iter_mut()
                    .enumerate()
                    .for_each(move |(i, x)| {
                        /*println!("i: {:?}",i);*/
                        *x = i / num_pes
                    })
                    .block()
            };
        }
        array.barrier();
        //individual T------------------------------
        let mut reqs = vec![];
        for i in 0..array.len() {
            #[allow(unused_unsafe)]
            reqs.push(unsafe { array.batch_fetch_add(i, 1).spawn() });
        }
        check_results!($array, array, num_pes, reqs, "T");
        //individual T------------------------------
        let mut reqs = vec![];
        for i in 0..array.len() {
            #[allow(unused_unsafe)]
            reqs.push(unsafe { array.batch_fetch_add(&i, 1).spawn() });
        }
        check_results!($array, array, num_pes, reqs, "&T");
        //&[T]------------------------------
        // multi_idx single val
        let idx = (0..array.len()).collect::<Vec<usize>>();
        let idx_slice = &idx[..];
        let vals = vec![1; array.len()];
        let vals_slice = &vals[..];

        let mut reqs = vec![];
        #[allow(unused_unsafe)]
        reqs.push(unsafe { array.batch_fetch_add(idx_slice, 1).spawn() });
        check_results!($array, array, num_pes, reqs, "&[T]");
        // single_idx multi_ val
        #[allow(unused_unsafe)]
        reqs.push(unsafe { array.batch_fetch_add(_my_pe, &vals).spawn() });
        let real_val = array.len();
        check_results!($array, array, num_pes, real_val, reqs, "&[T]");
        // multi_idx multi_ val
        #[allow(unused_unsafe)]
        reqs.push(unsafe { array.batch_fetch_add(idx_slice, vals_slice).spawn() });

        check_results!($array, array, num_pes, reqs, "&[T]");
        //scoped &[T]------------------------------
        let mut reqs = vec![];
        {
            let vec = (0..array.len()).collect::<Vec<usize>>();
            let slice = &vec[..];
            #[allow(unused_unsafe)]
            reqs.push(unsafe { array.batch_fetch_add(slice, 1).spawn() });
        }
        check_results!($array, array, num_pes, reqs, "scoped &[T]");
        // Vec<T>------------------------------
        let vec = (0..array.len()).collect::<Vec<usize>>();
        let mut reqs = vec![];
        #[allow(unused_unsafe)]
        reqs.push(unsafe { array.batch_fetch_add(vec, 1).spawn() });
        check_results!($array, array, num_pes, reqs, "Vec<T>");
        // &Vec<T>------------------------------
        let mut reqs = vec![];
        let vec = (0..array.len()).collect::<Vec<usize>>();
        #[allow(unused_unsafe)]
        reqs.push(unsafe { array.batch_fetch_add(&vec, 1).spawn() });
        check_results!($array, array, num_pes, reqs, "&Vec<T>");
        // Scoped Vec<T>------------------------------
        let mut reqs = vec![];
        {
            let vec = (0..array.len()).collect::<Vec<usize>>();
            #[allow(unused_unsafe)]
            reqs.push(unsafe { array.batch_fetch_add(vec, 1).spawn() });
        }
        check_results!($array, array, num_pes, reqs, "scoped Vec<T>");
        // Scoped &Vec<T>------------------------------
        let mut reqs = vec![];
        {
            let vec = (0..array.len()).collect::<Vec<usize>>();
            #[allow(unused_unsafe)]
            reqs.push(unsafe { array.batch_fetch_add(&vec, 1).spawn() });
        }
        check_results!($array, array, num_pes, reqs, "scoped &Vec<T>");

        // scoped &LMR<T>------------------------------
        let mut reqs = vec![];
        unsafe {
            let lmr = world.alloc_one_sided_mem_region(array.len());
            let slice = lmr.as_mut_slice().unwrap();
            for i in 0..array.len() {
                slice[i] = i;
            }
            reqs.push(array.batch_fetch_add(slice, 1).spawn());
            check_results!($array, array, num_pes, reqs, "scoped &LMR<T>");
        }

        // scoped SMR<T>------------------------------
        let mut reqs = vec![];
        unsafe {
            let smr = world.alloc_shared_mem_region(array.len()).block();
            let slice = smr.as_mut_slice().unwrap();
            for i in 0..array.len() {
                slice[i] = i;
            }

            reqs.push(array.batch_fetch_add(slice, 1).spawn());
            check_results!($array, array, num_pes, reqs, "scoped SMR<T>");
        }
        // UnsafeArray<T>------------------------------
        // let mut reqs = vec![];
        // reqs.push(array.fetch_add(input_array.clone(),1));
        // check_results!($array,array,num_pes,reqs,"UnsafeArray<T>");
        // UnsafeArray<T>------------------------------
        let mut reqs = vec![];
        #[allow(unused_unsafe)]
        reqs.push(unsafe { array.batch_fetch_add(input_array.local_data(), 1).spawn() });
        check_results!($array, array, num_pes, reqs, "&UnsafeArray<T>");

        // ReadOnlyArray<T>------------------------------
        // let mut reqs = vec![];
        let input_array = input_array.into_read_only().block();
        // println!("read only array len: {:?}", input_array.len());
        // reqs.push(array.fetch_add(input_array.clone(),1));
        // check_results!($array,array,num_pes,reqs,"ReadOnlyArray<T>");
        // ReadOnlyArray<T>------------------------------
        let mut reqs = vec![];
        #[allow(unused_unsafe)]
        reqs.push(unsafe { array.batch_fetch_add(input_array.local_data(), 1).spawn() });
        check_results!($array, array, num_pes, reqs, "&ReadOnlyArray<T>");

        // AtomicArray<T>------------------------------
        // let mut reqs = vec![];
        let input_array = input_array.into_atomic().block();
        // println!("atomic array len: {:?}", input_array.len());
        // reqs.push(array.fetch_add(input_array.clone(),1));
        // check_results!($array,array,num_pes,reqs,"AtomicArray<T>");
        // AtomicArray<T>------------------------------
        let mut reqs = vec![];
        #[allow(unused_unsafe)]
        reqs.push(unsafe { array.batch_fetch_add(&input_array.local_data(), 1).spawn() });
        check_results!($array, array, num_pes, reqs, "&AtomicArray<T>");

        // LocalLockArray<T>------------------------------
        //  let mut reqs = vec![];
        let input_array = input_array.into_local_lock().block();
        //  println!("local lock array len: {:?}", input_array.len());
        //  reqs.push(array.fetch_add(input_array.clone(),1));
        //  check_results!($array,array,num_pes,reqs,"LocalLockArray<T>");
        // LocalLockArray<T>------------------------------
        let mut reqs = vec![];
        let local_data = input_array.read_local_data().block();
        // println!("local lock array len: {:?}", local_data.deref());
        #[allow(unused_unsafe)]
        reqs.push(unsafe { array.batch_fetch_add(&local_data, 1).spawn() });
        drop(local_data);
        check_results!($array, array, num_pes, reqs, "&LocalLockArray<T>");

        // GlobalLockArray<T>------------------------------
        //  let mut reqs = vec![];
        let input_array = input_array.into_global_lock().block();
        // println!("global lock array len: {:?}", input_array.len());
        //  reqs.push(array.fetch_add(input_array.clone(),1));
        //  check_results!($array,array,num_pes,reqs,"GlobalLockArray<T>");
        // GlobalLockArray<T>------------------------------
        let mut reqs = vec![];
        #[allow(unused_unsafe)]
        reqs.push(unsafe {
            array
                .batch_fetch_add(&input_array.read_local_data().block(), 1)
                .spawn()
        });
        check_results!($array, array, num_pes, reqs, "&GlobalLockArray<T>");
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
            "u8" => fetch_add_test!(UnsafeArray, u8, len, dist_type),
            "u16" => fetch_add_test!(UnsafeArray, u16, len, dist_type),
            "u32" => fetch_add_test!(UnsafeArray, u32, len, dist_type),
            "u64" => fetch_add_test!(UnsafeArray, u64, len, dist_type),
            "u128" => fetch_add_test!(UnsafeArray, u128, len, dist_type),
            "usize" => fetch_add_test!(UnsafeArray, usize, len, dist_type),
            "i8" => fetch_add_test!(UnsafeArray, i8, len, dist_type),
            "i16" => fetch_add_test!(UnsafeArray, i16, len, dist_type),
            "i32" => fetch_add_test!(UnsafeArray, i32, len, dist_type),
            "i64" => fetch_add_test!(UnsafeArray, i64, len, dist_type),
            "i128" => fetch_add_test!(UnsafeArray, i128, len, dist_type),
            "isize" => fetch_add_test!(UnsafeArray, isize, len, dist_type),
            "f32" => fetch_add_test!(UnsafeArray, f32, len, dist_type),
            "f64" => fetch_add_test!(UnsafeArray, f64, len, dist_type),
            "input" => input_test!(UnsafeArray, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "AtomicArray" => match elem.as_str() {
            "u8" => fetch_add_test!(AtomicArray, u8, len, dist_type),
            "u16" => fetch_add_test!(AtomicArray, u16, len, dist_type),
            "u32" => fetch_add_test!(AtomicArray, u32, len, dist_type),
            "u64" => fetch_add_test!(AtomicArray, u64, len, dist_type),
            "u128" => fetch_add_test!(AtomicArray, u128, len, dist_type),
            "usize" => fetch_add_test!(AtomicArray, usize, len, dist_type),
            "i8" => fetch_add_test!(AtomicArray, i8, len, dist_type),
            "i16" => fetch_add_test!(AtomicArray, i16, len, dist_type),
            "i32" => fetch_add_test!(AtomicArray, i32, len, dist_type),
            "i64" => fetch_add_test!(AtomicArray, i64, len, dist_type),
            "i128" => fetch_add_test!(AtomicArray, i128, len, dist_type),
            "isize" => fetch_add_test!(AtomicArray, isize, len, dist_type),
            "f32" => fetch_add_test!(AtomicArray, f32, len, dist_type),
            "f64" => fetch_add_test!(AtomicArray, f64, len, dist_type),
            "input" => input_test!(AtomicArray, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "LocalLockArray" => match elem.as_str() {
            "u8" => fetch_add_test!(LocalLockArray, u8, len, dist_type),
            "u16" => fetch_add_test!(LocalLockArray, u16, len, dist_type),
            "u32" => fetch_add_test!(LocalLockArray, u32, len, dist_type),
            "u64" => fetch_add_test!(LocalLockArray, u64, len, dist_type),
            "u128" => fetch_add_test!(LocalLockArray, u128, len, dist_type),
            "usize" => fetch_add_test!(LocalLockArray, usize, len, dist_type),
            "i8" => fetch_add_test!(LocalLockArray, i8, len, dist_type),
            "i16" => fetch_add_test!(LocalLockArray, i16, len, dist_type),
            "i32" => fetch_add_test!(LocalLockArray, i32, len, dist_type),
            "i64" => fetch_add_test!(LocalLockArray, i64, len, dist_type),
            "i128" => fetch_add_test!(LocalLockArray, i128, len, dist_type),
            "isize" => fetch_add_test!(LocalLockArray, isize, len, dist_type),
            "f32" => fetch_add_test!(LocalLockArray, f32, len, dist_type),
            "f64" => fetch_add_test!(LocalLockArray, f64, len, dist_type),
            "input" => input_test!(LocalLockArray, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "GlobalLockArray" => match elem.as_str() {
            "u8" => fetch_add_test!(GlobalLockArray, u8, len, dist_type),
            "u16" => fetch_add_test!(GlobalLockArray, u16, len, dist_type),
            "u32" => fetch_add_test!(GlobalLockArray, u32, len, dist_type),
            "u64" => fetch_add_test!(GlobalLockArray, u64, len, dist_type),
            "u128" => fetch_add_test!(GlobalLockArray, u128, len, dist_type),
            "usize" => fetch_add_test!(GlobalLockArray, usize, len, dist_type),
            "i8" => fetch_add_test!(GlobalLockArray, i8, len, dist_type),
            "i16" => fetch_add_test!(GlobalLockArray, i16, len, dist_type),
            "i32" => fetch_add_test!(GlobalLockArray, i32, len, dist_type),
            "i64" => fetch_add_test!(GlobalLockArray, i64, len, dist_type),
            "i128" => fetch_add_test!(GlobalLockArray, i128, len, dist_type),
            "isize" => fetch_add_test!(GlobalLockArray, isize, len, dist_type),
            "f32" => fetch_add_test!(GlobalLockArray, f32, len, dist_type),
            "f64" => fetch_add_test!(GlobalLockArray, f64, len, dist_type),
            "input" => input_test!(GlobalLockArray, len, dist_type),
            _ => {} //eprintln!("unsupported element type"),
        },
        _ => eprintln!("unsupported array type"),
    }
}

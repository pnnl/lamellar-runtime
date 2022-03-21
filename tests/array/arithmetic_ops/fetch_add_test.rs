use lamellar::array::{
    ArithmeticOps, AtomicArray, LocalLockAtomicArray, SerialIterator, UnsafeArray,
};

use rand::distributions::Distribution;
use rand::distributions::Uniform;

macro_rules! initialize_array {
    (UnsafeArray,$array:ident,$init_val:ident) => {
        $array.dist_iter_mut().for_each(move |x| *x = $init_val);
        $array.wait_all();
        $array.barrier();
    };
    (AtomicArray,$array:ident,$init_val:ident) => {
        $array.dist_iter().for_each(move |x| x.store($init_val));
        $array.wait_all();
        $array.barrier();
    };
    (LocalLockAtomicArray,$array:ident,$init_val:ident) => {
        $array.dist_iter_mut().for_each(move |x| *x = $init_val);
        $array.wait_all();
        $array.barrier();
    };
}

macro_rules! check_val{
    (UnsafeArray,$val:ident,$max_val:ident,$valid:ident) => {
       if $val > $max_val{//because unsafe we might lose some updates, but val should never be greater than max_val
           $valid = false;
       }
    };
    (AtomicArray,$val:ident,$max_val:ident,$valid:ident) => {
        if (($val - $max_val)as f32).abs() > 0.0001{//all updates should be preserved
            $valid = false;
        }
    };
    (LocalLockAtomicArray,$val:ident,$max_val:ident,$valid:ident) => {
        if (($val - $max_val)as f32).abs()  > 0.0001{//all updates should be preserved
            $valid = false;
        }
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

macro_rules! add_test{
    ($array:ident, $t:ty, $len:expr, $dist:ident) =>{
       {
            let world = lamellar::LamellarWorldBuilder::new().build();
            let num_pes = world.num_pes();
            let _my_pe = world.my_pe();
            let array_total_len = $len;

            let mut rng = rand::thread_rng();
            let rand_idx = Uniform::from(0..array_total_len);
            let mut success = true;
            let array: $array::<$t> = $array::<$t>::new(world.team(), array_total_len, $dist).into(); //convert into abstract LamellarArray, distributed len is total_len

            let pe_max_val: $t = 10 as $t;
            let max_val = pe_max_val * num_pes as $t;
            let init_val = 0 as $t;
            initialize_array!($array, array, init_val);
            array.wait_all();
            array.barrier();
            for idx in 0..array.len(){
                let mut prev = init_val;
                #[cfg(feature="non-buffered-array-ops")]
                {
                    for i in 0..(pe_max_val as usize){
                        let val = array.fetch_add(idx,1 as $t).get().unwrap();
                        if val < prev{
                            println!("full 1: {:?} {:?} {:?}",i,val,prev);
                            success = false;
                        }
                        prev = val;
                    }
                }
                #[cfg(not(feature="non-buffered-array-ops"))]
                {
                    let mut reqs = vec![];
                    for _i in 0..(pe_max_val as usize){
                        reqs.push(array.fetch_add(idx,1 as $t));
                    }
                    for req in reqs{
                        let val = req.get().unwrap()[0];
                        if val < prev{
                            println!("full 1: {:?} {:?}",val,prev);
                            success = false;
                        }
                        prev = val;
                    }
                }
            }
            // array.wait_all();
            array.barrier();
            for (i,elem) in array.ser_iter().into_iter().enumerate(){
                let val = *elem;
                check_val!($array,val,max_val,success);
                if !success{
                    println!("full 2: {:?} {:?} {:?}",i,val,max_val);
                }
            }
            array.barrier();
            // println!("1------------");
            initialize_array!($array, array, init_val);
            array.wait_all();
            array.barrier();
            let num_updates=max_updates!($t,num_pes);
            // let mut prev_vals = vec![init_val;array.len()];
            #[cfg(feature="non-buffered-array-ops")]
            {
                for i in 0..num_updates{
                    let idx = rand_idx.sample(&mut rng);
                        let val = array.fetch_add(idx,1 as $t).get().unwrap();
                        if val < prev_vals[idx]{
                            println!("full 3: {:?} {:?} {:?}",i,val,prev_vals[idx]);
                            success = false;
                        }
                        prev_vals[idx]=val;
                }

            }
            #[cfg(not(feature="non-buffered-array-ops"))]
            {
                let mut reqs = vec![];
                // println!("2------------");
                for _i in 0..num_updates{
                    let idx = rand_idx.sample(&mut rng);
                    reqs.push((array.fetch_add(idx,1 as $t),idx))
                }
                for (req,_idx) in reqs{
                    let _val = req.get().unwrap();
                    // if val < prev_vals[idx]{
                    //     println!("full 3:  {:?} {:?}",val,prev_vals[idx]);
                    //     success = false;
                    // }
                    // prev_vals[idx]=val;
                }
            }
            array.barrier();
            let sum = array.ser_iter().into_iter().fold(0,|acc,x| acc+ *x as usize);
            let tot_updates = num_updates * num_pes;
            check_val!($array,sum,tot_updates,success);
            if !success{
                println!("full 4: {:?} {:?}",sum,tot_updates);
            }
            world.wait_all();
            world.barrier();
            // println!("2------------");
            initialize_array!($array, array, init_val);



            let half_len = array_total_len/2;
            let start_i = half_len/2;
            let end_i = start_i + half_len;
            let rand_idx = Uniform::from(0..half_len);
            let sub_array = array.sub_array(start_i..end_i);
            array.barrier();
            for idx in 0..sub_array.len(){
                let mut prev = init_val;
                #[cfg(feature="non-buffered-array-ops")]
                {
                    for i in 0..(pe_max_val as usize){
                        let val = sub_array.fetch_add(idx,1 as $t).get().unwrap();
                        if val < prev{
                            println!("half 1: {:?} {:?} {:?}",i,val,prev);
                            success = false;
                        }
                        prev = val;
                    }
                }
                #[cfg(not(feature="non-buffered-array-ops"))]
                {
                    let mut reqs = vec![];
                    for _i in 0..(pe_max_val as usize){
                        reqs.push(sub_array.fetch_add(idx,1 as $t));
                    }
                    for req in reqs{
                        let val = req.get().unwrap()[0];
                        if val < prev{
                            println!("half 1: {:?} {:?}",val,prev);
                            success = false;
                        }
                        prev = val;
                    }
                }
            }
            array.barrier();
            for (i,elem) in sub_array.ser_iter().into_iter().enumerate(){
                let val = *elem;
                check_val!($array,val,max_val,success);
                if !success{
                    println!("half 2: {:?} {:?} {:?}",i,val,max_val);
                }
            }
            array.barrier();
            // println!("3------------");
            initialize_array!($array, array, init_val);
            array.wait_all();
            array.barrier();
            let num_updates=max_updates!($t,num_pes);
            // let mut prev_vals = vec![init_val;sub_array.len()];
            #[cfg(feature="non-buffered-array-ops")]
            {
                for i in 0..num_updates{
                    let idx = rand_idx.sample(&mut rng);
                    let val = sub_array.fetch_add(idx,1 as $t).get().unwrap();
                        if val < prev_vals[idx]{
                            println!("half 3: {:?} {:?} {:?}",i,val,prev_vals[idx]);
                            success = false;
                        }
                        prev_vals[idx]=val;
                }

            }
            #[cfg(not(feature="non-buffered-array-ops"))]
            {
                let mut reqs = vec![];
                for _i in 0..num_updates{
                    let idx = rand_idx.sample(&mut rng);
                    reqs.push((sub_array.fetch_add(idx,1 as $t),idx))
                }
                for (req,_idx) in reqs{
                    let _val = req.get().unwrap();
                    // if val < prev_vals[idx]{
                    //     println!("half 3:  {:?} {:?}",val,prev_vals[idx]);
                    //     success = false;
                    // }
                    // prev_vals[idx]=val;
                }
            }
            array.barrier();
            let sum = sub_array.ser_iter().into_iter().fold(0,|acc,x| acc+ *x as usize);
            let tot_updates = num_updates * num_pes;
            check_val!($array,sum,tot_updates,success);
            if !success{
                println!("half 4: {:?} {:?}",sum,tot_updates);
            }
            array.wait_all();
            array.barrier();
            // println!("4------------");
            initialize_array!($array, array, init_val);
            array.wait_all();
            array.barrier();


            let pe_len = array_total_len/num_pes;
            for pe in 0..num_pes{
                let len = std::cmp::max(pe_len/2,1);
                let start_i = (pe*pe_len)+ len/2;
                let end_i = start_i+len;
                let rand_idx = Uniform::from(0..len);
                let sub_array = array.sub_array(start_i..end_i);
                array.barrier();
                for idx in 0..sub_array.len(){
                    let mut prev = init_val;
                    #[cfg(feature="non-buffered-array-ops")]
                    {
                        for i in 0..(pe_max_val as usize){
                            let val = sub_array.fetch_add(idx,1 as $t).get().unwrap();
                            if val < prev{
                                println!("pe 1: {:?} {:?} {:?}",i,val,prev);
                                success = false;
                            }
                            prev = val;
                        }
                    }
                    #[cfg(not(feature="non-buffered-array-ops"))]
                    {
                        let mut reqs = vec![];
                        for _i in 0..(pe_max_val as usize){
                            reqs.push(sub_array.fetch_add(idx,1 as $t));
                        }
                        for req in reqs{
                            let val = req.get().unwrap()[0];
                            if val < prev{
                                println!("pe 1: {:?} {:?}",val,prev);
                                success = false;
                            }
                            prev = val;
                        }
                    }
                }
                sub_array.barrier();
                for (i,elem) in sub_array.ser_iter().into_iter().enumerate(){
                    let val = *elem;
                    check_val!($array,val,max_val,success);
                    if !success{
                        println!("pe 2 {:?} {:?} {:?}",i,val,max_val);
                    }
                }
                sub_array.barrier();
                // println!("5------------");
                initialize_array!($array, array, init_val);
                sub_array.wait_all();
                sub_array.barrier();
                let num_updates=max_updates!($t,num_pes);
                // let mut prev_vals = vec![init_val;sub_array.len()];
                #[cfg(feature="non-buffered-array-ops")]
                {
                    for i in 0..num_updates{
                        let idx = rand_idx.sample(&mut rng);
                        let val = sub_array.fetch_add(idx,1 as $t).get().unwrap();
                            if val < prev_vals[idx]{
                                println!("pe 3: {:?} {:?} {:?}",i,val,prev_vals[idx]);
                                success = false;
                            }
                            prev_vals[idx]=val;
                    }

                }
                #[cfg(not(feature="non-buffered-array-ops"))]
                {
                    let mut reqs = vec![];
                    for _i in 0..num_updates{
                        let idx = rand_idx.sample(&mut rng);
                        reqs.push((sub_array.fetch_add(idx,1 as $t),idx))
                    }
                    for (req,_idx) in reqs{
                        let _val = req.get().unwrap();
                        // if val < prev_vals[idx]{
                        //     println!("pe 3:  {:?} {:?}",val,prev_vals[idx]);
                        //     success = false;
                        // }
                        // prev_vals[idx]=val;
                    }
                }
                sub_array.barrier();
                let sum = sub_array.ser_iter().into_iter().fold(0,|acc,x| acc+ *x as usize);
                let tot_updates = num_updates * num_pes;
                check_val!($array,sum,tot_updates,success);
                if !success{
                    println!("pe 4 {:?} {:?}",sum,tot_updates);
                }
                sub_array.wait_all();
                sub_array.barrier();
                // println!("6------------");
                initialize_array!($array, array, init_val);
            }

            if !success{
                eprintln!("failed");
            }
        }
    }
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
            "u8" => add_test!(UnsafeArray, u8, len, dist_type),
            "u16" => add_test!(UnsafeArray, u16, len, dist_type),
            "u32" => add_test!(UnsafeArray, u32, len, dist_type),
            "u64" => add_test!(UnsafeArray, u64, len, dist_type),
            "u128" => add_test!(UnsafeArray, u128, len, dist_type),
            "usize" => add_test!(UnsafeArray, usize, len, dist_type),
            "i8" => add_test!(UnsafeArray, i8, len, dist_type),
            "i16" => add_test!(UnsafeArray, i16, len, dist_type),
            "i32" => add_test!(UnsafeArray, i32, len, dist_type),
            "i64" => add_test!(UnsafeArray, i64, len, dist_type),
            "i128" => add_test!(UnsafeArray, i128, len, dist_type),
            "isize" => add_test!(UnsafeArray, isize, len, dist_type),
            "f32" => add_test!(UnsafeArray, f32, len, dist_type),
            "f64" => add_test!(UnsafeArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "AtomicArray" => match elem.as_str() {
            "u8" => add_test!(AtomicArray, u8, len, dist_type),
            "u16" => add_test!(AtomicArray, u16, len, dist_type),
            "u32" => add_test!(AtomicArray, u32, len, dist_type),
            "u64" => add_test!(AtomicArray, u64, len, dist_type),
            "u128" => add_test!(AtomicArray, u128, len, dist_type),
            "usize" => add_test!(AtomicArray, usize, len, dist_type),
            "i8" => add_test!(AtomicArray, i8, len, dist_type),
            "i16" => add_test!(AtomicArray, i16, len, dist_type),
            "i32" => add_test!(AtomicArray, i32, len, dist_type),
            "i64" => add_test!(AtomicArray, i64, len, dist_type),
            "i128" => add_test!(AtomicArray, i128, len, dist_type),
            "isize" => add_test!(AtomicArray, isize, len, dist_type),
            "f32" => add_test!(AtomicArray, f32, len, dist_type),
            "f64" => add_test!(AtomicArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "LocalLockAtomicArray" => match elem.as_str() {
            "u8" => add_test!(LocalLockAtomicArray, u8, len, dist_type),
            "u16" => add_test!(LocalLockAtomicArray, u16, len, dist_type),
            "u32" => add_test!(LocalLockAtomicArray, u32, len, dist_type),
            "u64" => add_test!(LocalLockAtomicArray, u64, len, dist_type),
            "u128" => add_test!(LocalLockAtomicArray, u128, len, dist_type),
            "usize" => add_test!(LocalLockAtomicArray, usize, len, dist_type),
            "i8" => add_test!(LocalLockAtomicArray, i8, len, dist_type),
            "i16" => add_test!(LocalLockAtomicArray, i16, len, dist_type),
            "i32" => add_test!(LocalLockAtomicArray, i32, len, dist_type),
            "i64" => add_test!(LocalLockAtomicArray, i64, len, dist_type),
            "i128" => add_test!(LocalLockAtomicArray, i128, len, dist_type),
            "isize" => add_test!(LocalLockAtomicArray, isize, len, dist_type),
            "f32" => add_test!(LocalLockAtomicArray, f32, len, dist_type),
            "f64" => add_test!(LocalLockAtomicArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        _ => eprintln!("unsupported array type"),
    }
}

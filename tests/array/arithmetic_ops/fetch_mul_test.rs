use lamellar::array::{
    ArithmeticOps, AtomicArray,Atomic2Array LocalLockAtomicArray, SerialIterator, UnsafeArray,
};

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
    (Atomic2Array,$array:ident,$init_val:ident) => {
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
    (Atomic2Array,$val:ident,$max_val:ident,$valid:ident) => {
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
        // let temp: u128 = (<$t>::MAX as u128/$num_pes as u128);
        // (temp as f64).log(2.0) as usize

        (std::mem::size_of::<u128>() * 8
            - (<$t>::MAX as u128 / $num_pes as u128).leading_zeros() as usize
            - 1 as usize)
            / $num_pes
        // if <$t>::MAX as u128 > (10000 * $num_pes) as u128{
        //     10000
        // }
        // else{
        //     <$t>::MAX as usize / $num_pes
        // }
    };
}

macro_rules! add_test{
    ($array:ident, $t:ty, $len:expr, $dist:ident) =>{
       {
            let world = lamellar::LamellarWorldBuilder::new().build();
            let num_pes = world.num_pes();
            let _my_pe = world.my_pe();
            let array_total_len = $len;

            let mut success = true;
            let array: $array::<$t> = $array::<$t>::new(world.team(), array_total_len, $dist).into(); //convert into abstract LamellarArray, distributed len is total_len

            let max_updates = max_updates!($t,num_pes);
            let max_val =  2u128.pow((max_updates*num_pes) as u32) as $t;
            let init_val = 1 as $t;
            initialize_array!($array, array, init_val);
            array.wait_all();
            array.barrier();
            // array.print();
            for idx in 0..array.len(){
                let mut prev = init_val;
                #[cfg(feature="non-buffered-array-ops")]
                {
                    for i in 0..(max_updates as usize){
                        let val = array.fetch_mul(idx,2 as $t).get().unwrap();
                        if val < prev || (prev as u128)%2 != 0{
                            if prev > 1 as $t{
                                println!("full 1: {:?} {:?} {:?}",i,val,prev);
                                success = false;
                            }
                        }
                        prev = val;
                    }
                }
                #[cfg(not(feature="non-buffered-array-ops"))]
                {
                    let mut reqs = vec![];
                    for _i in 0..(max_updates as usize){
                        reqs.push(array.fetch_mul(idx,2 as $t));
                    }
                    for req in reqs{
                        let val = req.get().unwrap();
                        if val < prev || (prev as u128)%2 != 0{
                            if prev > 1 as $t{
                                println!("full 1: {:?} {:?} ",val,prev);
                                success = false;
                            }
                        }
                        prev = val;
                    }
                }
            }
            array.wait_all();
            array.barrier();
            // array.print();
            for (i,elem) in array.ser_iter().into_iter().enumerate(){
                let val = *elem;
                check_val!($array,val,max_val,success);
                if !success{
                    println!("{:?} {:?} {:?}",i,val,max_val);
                }
            }

            array.barrier();
            initialize_array!($array, array, init_val);


            let half_len = array_total_len/2;
            let start_i = half_len/2;
            let end_i = start_i + half_len;
            let sub_array = array.sub_array(start_i..end_i);
            sub_array.barrier();
            // // sub_array.print();
            for idx in 0..sub_array.len(){
                let mut prev = init_val;
                #[cfg(feature="non-buffered-array-ops")]
                {
                    for i in 0..(max_updates as usize){
                        let val = sub_array.fetch_mul(idx,2 as $t).get().unwrap();
                        if val < prev || (prev as u128)%2 != 0{
                            if prev > 1 as $t{
                                println!("full 1: {:?} {:?} {:?}",i,val,prev);
                                success = false;
                            }
                        }
                        prev = val;
                    }
                }
                #[cfg(not(feature="non-buffered-array-ops"))]
                {
                    let mut reqs = vec![];
                    for _i in 0..(max_updates as usize){
                        reqs.push(sub_array.fetch_mul(idx,2 as $t));
                    }
                    for req in reqs{
                        let val = req.get().unwrap();
                        if val < prev || (prev as u128)%2 != 0{
                            if prev > 1 as $t{
                                println!("full 1:  {:?} {:?}",val,prev);
                                success = false;
                            }
                        }
                        prev = val;
                    }
                }
            }
            sub_array.wait_all();
            sub_array.barrier();
            for (i,elem) in sub_array.ser_iter().into_iter().enumerate(){
                let val = *elem;
                check_val!($array,val,max_val,success);
                if !success{
                    println!("{:?} {:?} {:?}",i,val,max_val);
                }
            }
            sub_array.barrier();
            initialize_array!($array, array, init_val);


            let pe_len = array_total_len/num_pes;
            for pe in 0..num_pes{
                let len = std::cmp::max(pe_len/2,1);
                let start_i = (pe*pe_len)+ len/2;
                let end_i = start_i+len;
                let sub_array = array.sub_array(start_i..end_i);
                sub_array.barrier();
                for idx in 0..sub_array.len(){
                    let mut prev = init_val;
                    #[cfg(feature="non-buffered-array-ops")]
                    {
                        for i in 0..(max_updates as usize){
                            let val = sub_array.fetch_mul(idx,2 as $t).get().unwrap();
                            if val < prev || (prev as u128)%2 != 0{
                                if prev > 1 as $t{
                                    println!("full 1: {:?} {:?} {:?}",i,val,prev);
                                    success = false;
                                }
                            }
                            prev = val;
                        }
                    }
                    #[cfg(not(feature="non-buffered-array-ops"))]
                    {
                        let mut reqs = vec![];
                        for _i in 0..(max_updates as usize){
                            reqs.push(sub_array.fetch_mul(idx,2 as $t));
                        }
                        for req in reqs{
                            let val = req.get().unwrap();
                            if val < prev || (prev as u128)%2 != 0{
                                if prev > 1 as $t{
                                    println!("full 1: {:?} {:?} ",val,prev);
                                    success = false;
                                }
                            }
                            prev = val;
                        }
                    }
                }
                sub_array.wait_all();
                sub_array.barrier();
                for (i,elem) in sub_array.ser_iter().into_iter().enumerate(){
                    let val = *elem;
                    check_val!($array,val,max_val,success);
                    if !success{
                        println!("{:?} {:?} {:?}",i,val,max_val);
                    }
                }
                sub_array.barrier();
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
        "Atomic2Array" => match elem.as_str() {
            "u8" => add_test!(Atomic2Array, u8, len, dist_type),
            "u16" => add_test!(Atomic2Array, u16, len, dist_type),
            "u32" => add_test!(Atomic2Array, u32, len, dist_type),
            "u64" => add_test!(Atomic2Array, u64, len, dist_type),
            "u128" => add_test!(Atomic2Array, u128, len, dist_type),
            "usize" => add_test!(Atomic2Array, usize, len, dist_type),
            "i8" => add_test!(Atomic2Array, i8, len, dist_type),
            "i16" => add_test!(Atomic2Array, i16, len, dist_type),
            "i32" => add_test!(Atomic2Array, i32, len, dist_type),
            "i64" => add_test!(Atomic2Array, i64, len, dist_type),
            "i128" => add_test!(Atomic2Array, i128, len, dist_type),
            "isize" => add_test!(Atomic2Array, isize, len, dist_type),
            "f32" => add_test!(Atomic2Array, f32, len, dist_type),
            "f64" => add_test!(Atomic2Array, f64, len, dist_type),
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

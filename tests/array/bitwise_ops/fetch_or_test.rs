use lamellar::array::{
    AtomicArray, BitWiseOps, DistributedIterator, LocalLockAtomicArray, SerialIterator, UnsafeArray,
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
    (LocalLockAtomicArray,$array:ident,$init_val:ident) => {
        $array.dist_iter_mut().for_each(move |x| *x = $init_val);
        $array.wait_all();
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
    (LocalLockAtomicArray,$val:ident,$max_val:ident,$valid:ident) => {
        if (($val - $max_val) as f32).abs() > 0.0001 {
            //all updates should be preserved
            $valid = false;
        }
    };
}

macro_rules! fetch_or_test{
    ($array:ident, $t:ty, $len:expr, $dist:ident) =>{
       {
            let world = lamellar::LamellarWorldBuilder::new().build();
            let num_pes = world.num_pes();
            let my_pe = world.my_pe();
            let array_total_len = $len;
            #[allow(unused_mut)]
            let mut success = true;
            let array: $array::<$t> = $array::<$t>::new(world.team(), array_total_len, $dist).into(); //convert into abstract LamellarArray, distributed len is total_len

            let init_val =0 as $t;
            let final_val = !(!init_val << num_pes);
            initialize_array!($array, array, init_val);
            array.wait_all();
            array.barrier();
            let my_val = 1 as $t << my_pe;
            #[cfg(feature="non-buffered-array-ops")]
            {
                for idx in 0..array.len(){
                    let val =  world.block_on(array.fetch_bit_or(idx,my_val));
                    if (val & my_val) != 0 {
                        println!("{:?} {:x} {:x} ",idx,my_val,val);
                        success = false;
                    }

                }
            }
            #[cfg(not(feature="non-buffered-array-ops"))]
            {
                let mut reqs = vec![];
                for idx in 0..array.len(){
                    reqs.push((array.fetch_bit_or(idx,my_val),idx));
                }
                for (req,idx) in reqs{
                    let val =  world.block_on(req);
                    if (val & my_val) != 0 {
                        println!("{:?} {:x} {:x} ",idx,my_val,val);
                        success = false;
                    }
                }
            }
            array.wait_all();
            array.barrier();
            // array.print();
            for (i,elem) in array.ser_iter().into_iter().enumerate(){
                let val = *elem;
                check_val!($array,val,final_val,success);
                if !success{
                    println!("{:?} {:x} {:x} {:x}",i,my_val,val,final_val);
                }
            }
            array.barrier();
            initialize_array!($array, array, init_val);
            array.wait_all();
            array.barrier();



            let half_len = array_total_len/2;
            let start_i = half_len/2;
            let end_i = start_i + half_len;
            let sub_array = array.sub_array(start_i..end_i);
            sub_array.barrier();
            // sub_array.print();
            #[cfg(feature="non-buffered-array-ops")]
            {
                for idx in 0..sub_array.len(){
                    let val =  world.block_on(sub_array.fetch_bit_or(idx,my_val));
                    if (val & my_val) != 0 {
                        println!("{:?} {:x} {:x} ",idx,my_val,val);
                        success = false;
                    }
                }
            }
            #[cfg(not(feature="non-buffered-array-ops"))]
            {
                let mut reqs = vec![];
                for idx in 0..sub_array.len(){
                    reqs.push((sub_array.fetch_bit_or(idx,my_val),idx));
                }
                for (req,idx)  in reqs{
                    let val =  world.block_on(req);
                    if (val & my_val) != 0 {
                        println!("{:?} {:x} {:x} ",idx,my_val,val);
                        success = false;
                    }
                }
            }
            sub_array.wait_all();
            sub_array.barrier();
            // sub_array.print();
            for (i,elem) in sub_array.ser_iter().into_iter().enumerate(){
                let val = *elem;
                check_val!($array,val,final_val,success);
                if !success{
                    println!("{:?} {:x} {:x} {:x}",i,my_val,val,final_val);
                }
            }
            sub_array.barrier();
            initialize_array!($array, array, init_val);
            sub_array.wait_all();
            sub_array.barrier();



            let pe_len = array_total_len/num_pes;
            for pe in 0..num_pes{
                let len = std::cmp::max(pe_len/2,1);
                let start_i = (pe*pe_len)+ len/2;
                let end_i = start_i+len;
                let sub_array = array.sub_array(start_i..end_i);
                sub_array.barrier();
                #[cfg(feature="non-buffered-array-ops")]
                {
                    for idx in 0..sub_array.len(){
                        let val =  world.block_on(sub_array.fetch_bit_or(idx,my_val));
                        if (val & my_val) != 0 {
                            println!("{:?} {:x} {:x} ",idx,my_val,val);
                            success = false;
                        }
                    }
                }
                #[cfg(not(feature="non-buffered-array-ops"))]
                {
                    let mut reqs = vec![];
                    for idx in 0..sub_array.len(){
                        reqs.push((sub_array.fetch_bit_or(idx,my_val),idx));
                    }
                    for (req,idx)  in reqs{
                        let val =  world.block_on(req);
                        if (val & my_val) != 0 {
                            println!("{:?} {:x} {:x} ",idx,my_val,val);
                            success = false;
                        }
                    }
                }
                sub_array.wait_all();
                sub_array.barrier();
                // sub_array.print();
                for (i,elem) in sub_array.ser_iter().into_iter().enumerate(){
                    let val = *elem;
                    check_val!($array,val,final_val,success);
                    if !success{
                        println!("{:?} {:x} {:x} {:x}",i,my_val,val,final_val);
                    }
                }
                sub_array.barrier();
                initialize_array!($array, array, init_val);
                sub_array.wait_all();
                sub_array.barrier();
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
            "u8" => fetch_or_test!(UnsafeArray, u8, len, dist_type),
            "u16" => fetch_or_test!(UnsafeArray, u16, len, dist_type),
            "u32" => fetch_or_test!(UnsafeArray, u32, len, dist_type),
            "u64" => fetch_or_test!(UnsafeArray, u64, len, dist_type),
            "u128" => fetch_or_test!(UnsafeArray, u128, len, dist_type),
            "usize" => fetch_or_test!(UnsafeArray, usize, len, dist_type),
            "i8" => fetch_or_test!(UnsafeArray, i8, len, dist_type),
            "i16" => fetch_or_test!(UnsafeArray, i16, len, dist_type),
            "i32" => fetch_or_test!(UnsafeArray, i32, len, dist_type),
            "i64" => fetch_or_test!(UnsafeArray, i64, len, dist_type),
            "i128" => fetch_or_test!(UnsafeArray, i128, len, dist_type),
            "isize" => fetch_or_test!(UnsafeArray, isize, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "AtomicArray" => match elem.as_str() {
            "u8" => fetch_or_test!(AtomicArray, u8, len, dist_type),
            "u16" => fetch_or_test!(AtomicArray, u16, len, dist_type),
            "u32" => fetch_or_test!(AtomicArray, u32, len, dist_type),
            "u64" => fetch_or_test!(AtomicArray, u64, len, dist_type),
            "u128" => fetch_or_test!(AtomicArray, u128, len, dist_type),
            "usize" => fetch_or_test!(AtomicArray, usize, len, dist_type),
            "i8" => fetch_or_test!(AtomicArray, i8, len, dist_type),
            "i16" => fetch_or_test!(AtomicArray, i16, len, dist_type),
            "i32" => fetch_or_test!(AtomicArray, i32, len, dist_type),
            "i64" => fetch_or_test!(AtomicArray, i64, len, dist_type),
            "i128" => fetch_or_test!(AtomicArray, i128, len, dist_type),
            "isize" => fetch_or_test!(AtomicArray, isize, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "LocalLockAtomicArray" => match elem.as_str() {
            "u8" => fetch_or_test!(LocalLockAtomicArray, u8, len, dist_type),
            "u16" => fetch_or_test!(LocalLockAtomicArray, u16, len, dist_type),
            "u32" => fetch_or_test!(LocalLockAtomicArray, u32, len, dist_type),
            "u64" => fetch_or_test!(LocalLockAtomicArray, u64, len, dist_type),
            "u128" => fetch_or_test!(LocalLockAtomicArray, u128, len, dist_type),
            "usize" => fetch_or_test!(LocalLockAtomicArray, usize, len, dist_type),
            "i8" => fetch_or_test!(LocalLockAtomicArray, i8, len, dist_type),
            "i16" => fetch_or_test!(LocalLockAtomicArray, i16, len, dist_type),
            "i32" => fetch_or_test!(LocalLockAtomicArray, i32, len, dist_type),
            "i64" => fetch_or_test!(LocalLockAtomicArray, i64, len, dist_type),
            "i128" => fetch_or_test!(LocalLockAtomicArray, i128, len, dist_type),
            "isize" => fetch_or_test!(LocalLockAtomicArray, isize, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        _ => eprintln!("unsupported array type"),
    }
}

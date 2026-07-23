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

macro_rules! array_or_lock {
    (GlobalLockArray, $array: ident, $lock:ident) => {
        $lock
    };
    ($arraytype:ident,$array:ident, $lock:ident) => {
        $array
    };
}

macro_rules! lock_if_needed {
    (GlobalLockArray, $array: ident) => {
        $array.collective_write_local_data().block()
    };
    ($arraytype:ident,$array:ident) => {
        0usize
    };
}

macro_rules! bit_xor_all_test{
    ($array:ident, $t:ty, $len:expr, $dist:ident) =>{
       {
            let world = lamellar::LamellarWorldBuilder::new().build();
            let num_pes = world.num_pes();
            let my_pe = world.my_pe();
            let array_total_len = $len;
            let mem_seg_len = array_total_len;
            let mut success = true;
            let array: $array::<$t> = $array::<$t>::new(world.team(), array_total_len * num_pes, $dist).block().into(); //convert into abstract LamellarArray, distributed len is total_len

            // let shared_mem_region: LamellarMemoryRegion<$t> = world.alloc_shared_mem_region(mem_seg_len).block().into(); //Convert into abstract LamellarMemoryRegion, each local segment is total_len
            //initialize array
            let init_val = (1  << my_pe) as $t;
            initialize_array!($array, array, init_val);
            array.wait_all();
            array.barrier();
            let final_val = !(!0 << num_pes);
            let _lock = lock_if_needed!($array, array);

            // initialize_mem_region(&shared_mem_region, (1  << my_pe) as $t,0 as $t);
            // world.barrier();

            for tx_size in 1..=mem_seg_len{
                let num_txs = mem_seg_len/tx_size;
                let mut reqs = vec![];
                for tx in (0..num_txs){
                    #[allow(unused_unsafe)]
                    reqs.push(unsafe { array_or_lock!($array, array, _lock).bit_xor_all(tx * tx_size, std::cmp::min(mem_seg_len,(tx+1)*tx_size) - tx * tx_size).spawn()});
                }
                for req in reqs.drain(..){
                    let buf =req.block();
                    for (i, elem) in buf.as_slice().iter().enumerate(){
                        if ((final_val as $t  - elem) as f32).abs() > 0.0001 {
                            eprintln!("{:?} {:?} {:?}",i as $t,elem,((final_val as $t - elem) as f32).abs());
                            success = false;
                        }
                    }
                }
                array.barrier();
                // array.print();
                // initialize_array!($array, array, init_val);
                array.wait_all();
                array.barrier();
            }
            array.barrier();
            world.wait_all();
            world.barrier();



            // let half_len = array_total_len/2;
            // let start_i = half_len/2;
            // let end_i = start_i + half_len;
            // let sub_array = array.sub_array(start_i..end_i);
            // world.barrier();
            // // sub_array.print();
            // for tx_size in 1..=half_len{
            //     let num_txs = half_len/tx_size;
            //     let mut reqs = vec![];
            //     for tx in (0..num_txs){
            //         // unsafe{println!("tx_size {:?} tx {:?} sindex: {:?} eindex: {:?} {:?}",tx_size,tx, tx*tx_size,std::cmp::min(half_len,(tx+1)*tx_size),&shared_mem_region.sub_region(tx*tx_size..std::cmp::min(half_len,(tx+1)*tx_size)).as_slice());}
            //         #[allow(unused_unsafe)]
            //         reqs.push(unsafe { array.bit_xor_all(tx * tx_size, std::cmp::min(half_len,(tx+1)*tx_size) - tx * tx_size).spawn()});
            //     }

            //     for req in reqs.drain(..){
            //         let buf =req.block();
            //         for (i, elem) in buf.as_slice().iter().enumerate(){
            //             if ((final_val as $t  - elem) as f32).abs() > 0.0001 {
            //                 eprintln!("{:?} {:?} {:?}",i as $t,elem,((final_val as $t - elem) as f32).abs());
            //                 success = false;
            //             }
            //         }
            //     }
            //     array.wait_all();
            //     sub_array.barrier();
            //     // sub_array.print();
            //     // initialize_array!($array, array, init_val);
            //     sub_array.wait_all();
            //     sub_array.barrier();
            //     // sub_array.print();
            // }
            // array.barrier();
            // world.wait_all();
            // world.barrier();

            // let pe_len = array_total_len/num_pes;

            // for pe in 0..num_pes{
            //     let len = pe_len/2;
            //     let start_i = (pe*pe_len)+ len/2;

            //     let end_i = start_i+len;
            //     let sub_array = array.sub_array(start_i..end_i);
            //     world.barrier();

            //     for tx_size in 1..len{
            //         let num_txs = len/tx_size;
            //         let mut reqs = vec![];
            //         for tx in (0..num_txs){
            //             // unsafe{println!("tx_size {:?} tx {:?} sindex: {:?} eindex: {:?} {:?}",tx_size,tx, tx*tx_size,std::cmp::min(len,(tx+1)*tx_size),&shared_mem_region.sub_region(tx*tx_size..std::cmp::min(len,(tx+1)*tx_size)).as_slice());}
            //             #[allow(unused_unsafe)]
            //             reqs.push(unsafe { sub_array.bit_xor_all(tx * tx_size, std::cmp::min(len,(tx+1)*tx_size) - tx * tx_size).spawn()});
            //         }
            //         // array.wait_all();
            //         // sub_array.barrier();
            //         for req in reqs.drain(..){
            //             let buf =req.block();
            //             for (i, elem) in buf.as_slice().iter().enumerate(){
            //                 if ((final_val as $t  - elem) as f32).abs() > 0.0001 {
            //                     eprintln!("{:?} {:?} {:?}",i as $t,elem,((final_val as $t - elem) as f32).abs());
            //                     success = false;
            //                 }
            //             }
            //         }
            //         array.wait_all();
            //         sub_array.barrier();
            //         // sub_array.print();
            //         // initialize_array!($array, array, init_val);
            //         sub_array.wait_all();
            //         sub_array.barrier();
            //     }
            //     array.barrier();
            //     world.wait_all();
            //     world.barrier();
            // }

            if !success{
                eprintln!("failed");
            }
        }
    }
}

#[lamellar::main]
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
            "u8" => bit_xor_all_test!(UnsafeArray, u8, len, dist_type),
            "u16" => bit_xor_all_test!(UnsafeArray, u16, len, dist_type),
            "u32" => bit_xor_all_test!(UnsafeArray, u32, len, dist_type),
            "u64" => bit_xor_all_test!(UnsafeArray, u64, len, dist_type),
            "u128" => bit_xor_all_test!(UnsafeArray, u128, len, dist_type),
            "usize" => bit_xor_all_test!(UnsafeArray, usize, len, dist_type),
            "i8" => bit_xor_all_test!(UnsafeArray, i8, len, dist_type),
            "i16" => bit_xor_all_test!(UnsafeArray, i16, len, dist_type),
            "i32" => bit_xor_all_test!(UnsafeArray, i32, len, dist_type),
            "i64" => bit_xor_all_test!(UnsafeArray, i64, len, dist_type),
            "i128" => bit_xor_all_test!(UnsafeArray, i128, len, dist_type),
            "isize" => bit_xor_all_test!(UnsafeArray, isize, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "AtomicArray" => match elem.as_str() {
            "u8" => bit_xor_all_test!(AtomicArray, u8, len, dist_type),
            "u16" => bit_xor_all_test!(AtomicArray, u16, len, dist_type),
            "u32" => bit_xor_all_test!(AtomicArray, u32, len, dist_type),
            "u64" => bit_xor_all_test!(AtomicArray, u64, len, dist_type),
            "u128" => bit_xor_all_test!(AtomicArray, u128, len, dist_type),
            "usize" => bit_xor_all_test!(AtomicArray, usize, len, dist_type),
            "i8" => bit_xor_all_test!(AtomicArray, i8, len, dist_type),
            "i16" => bit_xor_all_test!(AtomicArray, i16, len, dist_type),
            "i32" => bit_xor_all_test!(AtomicArray, i32, len, dist_type),
            "i64" => bit_xor_all_test!(AtomicArray, i64, len, dist_type),
            "i128" => bit_xor_all_test!(AtomicArray, i128, len, dist_type),
            "isize" => bit_xor_all_test!(AtomicArray, isize, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "GlobalLockArray" => match elem.as_str() {
            "u8" => bit_xor_all_test!(GlobalLockArray, u8, len, dist_type),
            "u16" => bit_xor_all_test!(GlobalLockArray, u16, len, dist_type),
            "u32" => bit_xor_all_test!(GlobalLockArray, u32, len, dist_type),
            "u64" => bit_xor_all_test!(GlobalLockArray, u64, len, dist_type),
            "u128" => bit_xor_all_test!(GlobalLockArray, u128, len, dist_type),
            "usize" => bit_xor_all_test!(GlobalLockArray, usize, len, dist_type),
            "i8" => bit_xor_all_test!(GlobalLockArray, i8, len, dist_type),
            "i16" => bit_xor_all_test!(GlobalLockArray, i16, len, dist_type),
            "i32" => bit_xor_all_test!(GlobalLockArray, i32, len, dist_type),
            "i64" => bit_xor_all_test!(GlobalLockArray, i64, len, dist_type),
            "i128" => bit_xor_all_test!(GlobalLockArray, i128, len, dist_type),
            "isize" => bit_xor_all_test!(GlobalLockArray, isize, len, dist_type),
            _ => {} //eprintln!("unsupported element type"),
        },
        _ => eprintln!("unsupported array type"),
    }
}

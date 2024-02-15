use lamellar::array::prelude::*;
use lamellar::memregion::prelude::*;

fn initialize_mem_region<T: Dist + std::ops::AddAssign>(
    memregion: &LamellarMemoryRegion<T>,
    init_val: T,
    inc_val: T,
) {
    unsafe {
        let mut i = init_val; //(len_per_pe * my_pe as f32).round() as usize;
        for elem in memregion.as_mut_slice().unwrap() {
            *elem = i;
            i += inc_val;
        }
    }
}

macro_rules! initialize_array {
    (UnsafeArray,$array:ident,$t:ty) => {
        unsafe {
            let _ = $array
                .dist_iter_mut()
                .enumerate()
                .for_each(move |(i, x)| *x = i as $t);
            $array.wait_all();
        }
    };
    (AtomicArray,$array:ident,$t:ty) => {
        let _ = $array
            .dist_iter()
            .enumerate()
            .for_each(move |(i, x)| x.store(i as $t));
        $array.wait_all();
    };
    (LocalLockArray,$array:ident,$t:ty) => {
        let _ = $array
            .dist_iter_mut()
            .enumerate()
            .for_each(move |(i, x)| *x = i as $t);
        $array.wait_all();
    };
    (GlobalLockArray,$array:ident,$t:ty) => {
        let _ = $array
            .dist_iter_mut()
            .enumerate()
            .for_each(move |(i, x)| *x = i as $t);
        $array.wait_all();
    };
    (ReadOnlyArray,$array:ident,$t:ty) => {
        // println!("into unsafe");
        let temp = $array.into_unsafe();
        // println!("unsafe");
        unsafe {
            let _ = temp
                .dist_iter_mut()
                .enumerate()
                .for_each(move |(i, x)| *x = i as $t);
            temp.wait_all();
            $array = temp.into_read_only();
        }
    };
}

macro_rules! initialize_array_range {
    (UnsafeArray,$array:ident,$t:ty,$range:expr) => {{
        unsafe {
            let subarray = $array.sub_array($range);
            let _ = subarray
                .dist_iter_mut()
                .enumerate()
                .for_each(move |(i, x)| *x = i as $t);
            subarray.wait_all();
        }
    }};
    (AtomicArray,$array:ident,$t:ty,$range:expr) => {{
        let subarray = $array.sub_array($range);
        let _ = subarray
            .dist_iter()
            .enumerate()
            .for_each(move |(i, x)| x.store(i as $t));
        subarray.wait_all();
    }};
    (LocalLockArray,$array:ident,$t:ty,$range:expr) => {{
        let subarray = $array.sub_array($range);
        let _ = subarray
            .dist_iter_mut()
            .enumerate()
            .for_each(move |(i, x)| *x = i as $t);
        subarray.wait_all();
    }};
    (GlobalLockArray,$array:ident,$t:ty,$range:expr) => {{
        let subarray = $array.sub_array($range);
        let _ = subarray
            .dist_iter_mut()
            .enumerate()
            .for_each(move |(i, x)| *x = i as $t);
        subarray.wait_all();
    }};
    (ReadOnlyArray,$array:ident,$t:ty,$range:expr) => {{
        // println!("into unsafe");
        let temp = $array.into_unsafe();
        // println!("unsafe");
        unsafe {
            let subarray = temp.sub_array($range);
            let _ = subarray
                .dist_iter_mut()
                .enumerate()
                .for_each(move |(i, x)| *x = i as $t);

            subarray.wait_all();
            drop(subarray);
        }
        println!("into read only");
        $array = temp.into_read_only();
        println!("read only");
    }};
}

macro_rules! get_test{
    ($array:ident, $t:ty, $len:expr, $dist:ident) =>{
       {
            let world = lamellar::LamellarWorldBuilder::new().build();
            let num_pes = world.num_pes();
            let _my_pe = world.my_pe();
            let array_total_len = $len;
            let mem_seg_len = array_total_len;
            #[allow(unused_mut)]
            let mut success = true;
            #[allow(unused_mut)]
            let mut array: $array::<$t> = $array::<$t>::new(world.team(), array_total_len, $dist).into(); //convert into abstract LamellarArray, distributed len is total_len
            // println!("bout to initialize");
            initialize_array!($array, array, $t);
            let shared_mem_region: LamellarMemoryRegion<$t> = world.alloc_shared_mem_region(mem_seg_len).into(); //Convert into abstract LamellarMemoryRegion, each local segment is total_len
            //initialize array

            array.wait_all();
            array.barrier();
            initialize_mem_region(&shared_mem_region,num_pes as $t,0 as $t);
            // world.barrier();

            for tx_size in 1..=mem_seg_len{
                let num_txs = mem_seg_len/tx_size;
                for tx in (0..num_txs){
                    // unsafe{println!("tx_size {:?} tx {:?} sindex: {:?} eindex: {:?} {:?}",tx_size,tx, tx*tx_size,std::cmp::min(mem_seg_len,(tx+1)*tx_size),&shared_mem_region.sub_region(tx*tx_size..std::cmp::min(mem_seg_len,(tx+1)*tx_size)).as_slice());}
                    unsafe {let _ = array.get(tx*tx_size,&shared_mem_region.sub_region(tx*tx_size..std::cmp::min(mem_seg_len,(tx+1)*tx_size)));}
                }
                array.wait_all();
                array.barrier();
                // unsafe{println!("{:?}",shared_mem_region.as_slice());}
                unsafe{
                    for (i,elem) in shared_mem_region.as_slice().unwrap().iter().enumerate().take( num_txs * tx_size){
                        if ((i as $t - *elem) as f32).abs() > 0.0001 {
                            println!("{:?} {:?} {:?}",i as $t,*elem,((i as $t - *elem) as f32).abs());
                            success = false;
                        }
                    }
                }
                array.barrier();
                // array.print();
                initialize_mem_region(&shared_mem_region,num_pes as $t,0 as $t);
                array.wait_all();
                array.barrier();
            }
            array.barrier();
            world.wait_all();
            world.barrier();
            if !success{
                eprintln!("failed 1");
            }


            let half_len = array_total_len/2;
            let start_i = half_len/2;
            let end_i = start_i + half_len;
            initialize_array_range!($array, array, $t,(start_i..end_i));
            let sub_array = array.sub_array(start_i..end_i);
            world.barrier();

            sub_array.barrier();
            // sub_array.print();
            for tx_size in 1..=half_len{
                let num_txs = half_len/tx_size;
                for tx in (0..num_txs){
                    // unsafe{println!("tx_size {:?} tx {:?} sindex: {:?} eindex: {:?} {:?}",tx_size,tx, tx*tx_size,std::cmp::min(half_len,(tx+1)*tx_size),&shared_mem_region.sub_region(tx*tx_size..std::cmp::min(half_len,(tx+1)*tx_size)).as_slice());}
                    unsafe {let _ = sub_array.get(tx*tx_size,&shared_mem_region.sub_region(tx*tx_size..std::cmp::min(half_len,(tx+1)*tx_size)));}
                }
                sub_array.wait_all();
                sub_array.barrier();
                // unsafe{println!("{:?}",shared_mem_region.as_slice());}
                unsafe{
                    for (i,elem) in shared_mem_region.as_slice().unwrap().iter().enumerate().take( num_txs * tx_size){
                        if ((i as $t - *elem) as f32).abs() > 0.0001 {
                            println!("{:?} {:?} {:?}",i as $t,*elem,((i as $t - *elem) as f32).abs());
                            success = false;
                        }
                    }
                }
                sub_array.barrier();
                // sub_array.print();
                initialize_mem_region(&shared_mem_region,num_pes as $t,0 as $t);
                sub_array.wait_all();
                sub_array.barrier();
                // sub_array.print();
            }
            array.barrier();
            world.wait_all();
            world.barrier();
            if !success{
                eprintln!("failed 2");
            }
            drop(sub_array); //needed for if we are using a ReadOnlyArray, as we will switch to an unsafe array to re-initialize

            let pe_len = array_total_len/num_pes;

            for pe in 0..num_pes{
                let len = pe_len/2;
                let start_i = (pe*pe_len)+ len/2;

                let end_i = start_i+len;
                initialize_array_range!($array, array, $t,(start_i..end_i));
                let sub_array = array.sub_array(start_i..end_i);
                world.barrier();

                sub_array.barrier();

                for tx_size in 1..len{
                    let num_txs = len/tx_size;
                    for tx in (0..num_txs){
                        // unsafe{println!("tx_size {:?} tx {:?} sindex: {:?} eindex: {:?} {:?}",tx_size,tx, tx*tx_size,std::cmp::min(len,(tx+1)*tx_size),&shared_mem_region.sub_region(tx*tx_size..std::cmp::min(mem_seg_len,(tx+1)*tx_size)).as_slice());}
                        unsafe {let _ = sub_array.get(tx*tx_size,&shared_mem_region.sub_region(tx*tx_size..std::cmp::min(len,(tx+1)*tx_size))); }
                    }
                    sub_array.wait_all();
                    sub_array.barrier();
                    unsafe{
                        for (i,elem) in shared_mem_region.as_slice().unwrap().iter().enumerate().take( num_txs * tx_size){
                            if ((i as $t - *elem) as f32).abs() > 0.0001 {
                                println!("{:?} {:?} {:?}",i as $t,*elem,((i as $t - *elem) as f32).abs());
                                success = false;
                            }
                        }
                    }

                    sub_array.barrier();
                    // sub_array.print();
                    initialize_mem_region(&shared_mem_region,num_pes as $t,0 as $t);
                    sub_array.wait_all();
                    sub_array.barrier();
                }
                array.barrier();
                world.wait_all();
                world.barrier();
            }

            if !success{
                eprintln!("failed 3");
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
        "Block" => Distribution::Block,
        "Cyclic" => Distribution::Cyclic,
        _ => panic!("unsupported dist type"),
    };

    match array.as_str() {
        "UnsafeArray" => match elem.as_str() {
            "u8" => get_test!(UnsafeArray, u8, len, dist_type),
            "u16" => get_test!(UnsafeArray, u16, len, dist_type),
            "u32" => get_test!(UnsafeArray, u32, len, dist_type),
            "u64" => get_test!(UnsafeArray, u64, len, dist_type),
            "u128" => get_test!(UnsafeArray, u128, len, dist_type),
            "usize" => get_test!(UnsafeArray, usize, len, dist_type),
            "i8" => get_test!(UnsafeArray, i8, len, dist_type),
            "i16" => get_test!(UnsafeArray, i16, len, dist_type),
            "i32" => get_test!(UnsafeArray, i32, len, dist_type),
            "i64" => get_test!(UnsafeArray, i64, len, dist_type),
            "i128" => get_test!(UnsafeArray, i128, len, dist_type),
            "isize" => get_test!(UnsafeArray, isize, len, dist_type),
            "f32" => get_test!(UnsafeArray, f32, len, dist_type),
            "f64" => get_test!(UnsafeArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "AtomicArray" => match elem.as_str() {
            "u8" => get_test!(AtomicArray, u8, len, dist_type),
            "u16" => get_test!(AtomicArray, u16, len, dist_type),
            "u32" => get_test!(AtomicArray, u32, len, dist_type),
            "u64" => get_test!(AtomicArray, u64, len, dist_type),
            "u128" => get_test!(AtomicArray, u128, len, dist_type),
            "usize" => get_test!(AtomicArray, usize, len, dist_type),
            "i8" => get_test!(AtomicArray, i8, len, dist_type),
            "i16" => get_test!(AtomicArray, i16, len, dist_type),
            "i32" => get_test!(AtomicArray, i32, len, dist_type),
            "i64" => get_test!(AtomicArray, i64, len, dist_type),
            "i128" => get_test!(AtomicArray, i128, len, dist_type),
            "isize" => get_test!(AtomicArray, isize, len, dist_type),
            "f32" => get_test!(AtomicArray, f32, len, dist_type),
            "f64" => get_test!(AtomicArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "LocalLockArray" => match elem.as_str() {
            "u8" => get_test!(LocalLockArray, u8, len, dist_type),
            "u16" => get_test!(LocalLockArray, u16, len, dist_type),
            "u32" => get_test!(LocalLockArray, u32, len, dist_type),
            "u64" => get_test!(LocalLockArray, u64, len, dist_type),
            "u128" => get_test!(LocalLockArray, u128, len, dist_type),
            "usize" => get_test!(LocalLockArray, usize, len, dist_type),
            "i8" => get_test!(LocalLockArray, i8, len, dist_type),
            "i16" => get_test!(LocalLockArray, i16, len, dist_type),
            "i32" => get_test!(LocalLockArray, i32, len, dist_type),
            "i64" => get_test!(LocalLockArray, i64, len, dist_type),
            "i128" => get_test!(LocalLockArray, i128, len, dist_type),
            "isize" => get_test!(LocalLockArray, isize, len, dist_type),
            "f32" => get_test!(LocalLockArray, f32, len, dist_type),
            "f64" => get_test!(LocalLockArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "GlobalLockArray" => match elem.as_str() {
            "u8" => get_test!(GlobalLockArray, u8, len, dist_type),
            "u16" => get_test!(GlobalLockArray, u16, len, dist_type),
            "u32" => get_test!(GlobalLockArray, u32, len, dist_type),
            "u64" => get_test!(GlobalLockArray, u64, len, dist_type),
            "u128" => get_test!(GlobalLockArray, u128, len, dist_type),
            "usize" => get_test!(GlobalLockArray, usize, len, dist_type),
            "i8" => get_test!(GlobalLockArray, i8, len, dist_type),
            "i16" => get_test!(GlobalLockArray, i16, len, dist_type),
            "i32" => get_test!(GlobalLockArray, i32, len, dist_type),
            "i64" => get_test!(GlobalLockArray, i64, len, dist_type),
            "i128" => get_test!(GlobalLockArray, i128, len, dist_type),
            "isize" => get_test!(GlobalLockArray, isize, len, dist_type),
            "f32" => get_test!(GlobalLockArray, f32, len, dist_type),
            "f64" => get_test!(GlobalLockArray, f64, len, dist_type),
            _ => {} //eprintln!("unsupported element type"),
        },
        "ReadOnlyArray" => match elem.as_str() {
            "u8" => get_test!(ReadOnlyArray, u8, len, dist_type),
            "u16" => get_test!(ReadOnlyArray, u16, len, dist_type),
            "u32" => get_test!(ReadOnlyArray, u32, len, dist_type),
            "u64" => get_test!(ReadOnlyArray, u64, len, dist_type),
            "u128" => get_test!(ReadOnlyArray, u128, len, dist_type),
            "usize" => get_test!(ReadOnlyArray, usize, len, dist_type),
            "i8" => get_test!(ReadOnlyArray, i8, len, dist_type),
            "i16" => get_test!(ReadOnlyArray, i16, len, dist_type),
            "i32" => get_test!(ReadOnlyArray, i32, len, dist_type),
            "i64" => get_test!(ReadOnlyArray, i64, len, dist_type),
            "i128" => get_test!(ReadOnlyArray, i128, len, dist_type),
            "isize" => get_test!(ReadOnlyArray, isize, len, dist_type),
            "f32" => get_test!(ReadOnlyArray, f32, len, dist_type),
            "f64" => get_test!(ReadOnlyArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        _ => eprintln!("unsupported array type"),
    }
}

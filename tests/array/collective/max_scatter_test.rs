use lamellar::array::prelude::*;
use lamellar::memregion::prelude::*;

fn initialize_mem_region<T: Dist + std::ops::AddAssign>(
    memregion: &LamellarMemoryRegion<T>,
    init_val: T,
    inc_val: T,
) {
    unsafe {
        let mut i = init_val; //(len_per_pe * my_pe as f32).round() as usize;
        for elem in memregion.as_mut_slice() {
            *elem = i;
            i += inc_val;
        }
    }
}

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

macro_rules! max_scatter_test{
    ($array:ident, $t:ty, $len:expr, $dist:ident) =>{
       {
            let world = lamellar::LamellarWorldBuilder::new().build();
            let num_pes = world.num_pes();
            let my_pe = world.my_pe();
            let array_total_len = $len;
            let mem_seg_len = array_total_len;
            let mut success = true;
            let array: $array::<$t> = $array::<$t>::new(world.team(), array_total_len, $dist).block().into(); //convert into abstract LamellarArray, distributed len is total_len

            let shared_mem_region: LamellarMemoryRegion<$t> = world.alloc_shared_mem_region(mem_seg_len).block().into(); //Convert into abstract LamellarMemoryRegion, each local segment is total_len
            //initialize array
            let init_val = my_pe as $t;
            initialize_array!($array, array, init_val);
            array.wait_all();
            array.barrier();
            initialize_mem_region(&shared_mem_region,0 as $t,1 as $t);
            // world.barrier();

            for tx_size in (1..=mem_seg_len).step_by(num_pes){
                let num_txs = mem_seg_len/tx_size;
                let mut reqs = vec![];
                for tx in (0..num_txs){
                    let chunk_size = (std::cmp::min(mem_seg_len,(tx+1)*tx_size) - tx*tx_size)/num_pes;
                    #[allow(unused_unsafe)]
                    reqs.push((unsafe { array.max_scatter(&shared_mem_region.sub_region(tx*tx_size..std::cmp::min(mem_seg_len,(tx+1)*tx_size)), chunk_size).spawn()}, chunk_size));
                }
                let mut i = 0;
                for req in reqs.drain(..){
                    let buf =req.0.block();
                    for elem in buf.as_slice().iter(){
                        let g = ((i/ req.1 * num_pes + my_pe) * req.1 + i % req.1);
                        let expected: $t = (g as $t) * num_pes as $t;
                        if ((expected  - *elem) as f32).abs() > 0.0001 {
                            eprintln!("expected {:?} got {:?}", expected, elem);
                            success = false;
                        }
                        i+=1;
                    }
                }
                array.barrier();
                // array.print();
                initialize_array!($array, array, init_val);
                array.wait_all();
                array.barrier();
            }
            array.barrier();
            world.wait_all();
            world.barrier();



            let half_len = array_total_len/2;
            let start_i = half_len/2;
            let end_i = start_i + half_len;
            let sub_array = array.sub_array(start_i..end_i);
            world.barrier();
            // sub_array.print();
            for tx_size in 1..=half_len{
                let num_txs = half_len/tx_size;
                let mut reqs = vec![];
                for tx in (0..num_txs){
                    let chunk_size = (std::cmp::min(half_len,(tx+1)*tx_size) - tx*tx_size)/num_pes;
                    #[allow(unused_unsafe)]
                    reqs.push((unsafe { array.max_scatter(&shared_mem_region.sub_region(tx*tx_size..std::cmp::min(half_len,(tx+1)*tx_size)), chunk_size).spawn()}, chunk_size));
                }

                let mut i = 0;
                for req in reqs.drain(..){
                    let buf =req.0.block();
                    for elem in buf.as_slice().iter(){
                        let g = ((i/ req.1 * num_pes + my_pe) * req.1 + i % req.1);
                        let expected: $t = (g as $t) * num_pes as $t;
                        if ((expected  - *elem) as f32).abs() > 0.0001 {
                            eprintln!("expected {:?} got {:?}", expected, elem);
                            success = false;
                        }
                        i+=1;
                    }
                }
                array.wait_all();
                sub_array.barrier();
                // sub_array.print();
                initialize_array!($array, array, init_val);
                sub_array.wait_all();
                sub_array.barrier();
                // sub_array.print();
            }
            array.barrier();
            world.wait_all();
            world.barrier();

            let pe_len = array_total_len/num_pes;

            for pe in 0..num_pes{
                let len = pe_len/2;
                let start_i = (pe*pe_len)+ len/2;

                let end_i = start_i+len;
                let sub_array = array.sub_array(start_i..end_i);
                world.barrier();

                for tx_size in 1..len{
                    let num_txs = len/tx_size;
                    let mut reqs = vec![];
                    for tx in (0..num_txs){
                        let chunk_size = (std::cmp::min(len,(tx+1)*tx_size) - tx*tx_size)/num_pes;
                        #[allow(unused_unsafe)]
                        reqs.push((unsafe { sub_array.max_scatter(&shared_mem_region.sub_region(tx*tx_size..std::cmp::min(len,(tx+1)*tx_size)), chunk_size).spawn()}, chunk_size));
                    }
                    // array.wait_all();
                    // sub_array.barrier();
                    let mut i = 0;
                    for req in reqs.drain(..){
                        let buf =req.0.block();
                        for elem in buf.as_slice().iter(){
                            let g = ((i/ req.1 * num_pes + my_pe) * req.1 + i % req.1);
                            let expected: $t = (g as $t) * num_pes as $t;
                            if ((expected  - *elem) as f32).abs() > 0.0001 {
                                eprintln!("expected {:?} got {:?}", expected, elem);
                                success = false;
                            }
                            i+=1;
                        }
                    }
                    array.wait_all();
                    sub_array.barrier();
                    // sub_array.print();
                    initialize_array!($array, array, init_val);
                    sub_array.wait_all();
                    sub_array.barrier();
                }
                array.barrier();
                world.wait_all();
                world.barrier();
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
        "Block" => Distribution::Block,
        "Cyclic" => Distribution::Cyclic,
        _ => panic!("unsupported dist type"),
    };

    match array.as_str() {
        "UnsafeArray" => match elem.as_str() {
            "u8" => max_scatter_test!(UnsafeArray, u8, len, dist_type),
            "u16" => max_scatter_test!(UnsafeArray, u16, len, dist_type),
            "u32" => max_scatter_test!(UnsafeArray, u32, len, dist_type),
            "u64" => max_scatter_test!(UnsafeArray, u64, len, dist_type),
            "u128" => max_scatter_test!(UnsafeArray, u128, len, dist_type),
            "usize" => max_scatter_test!(UnsafeArray, usize, len, dist_type),
            "i8" => max_scatter_test!(UnsafeArray, i8, len, dist_type),
            "i16" => max_scatter_test!(UnsafeArray, i16, len, dist_type),
            "i32" => max_scatter_test!(UnsafeArray, i32, len, dist_type),
            "i64" => max_scatter_test!(UnsafeArray, i64, len, dist_type),
            "i128" => max_scatter_test!(UnsafeArray, i128, len, dist_type),
            "isize" => max_scatter_test!(UnsafeArray, isize, len, dist_type),
            "f32" => max_scatter_test!(UnsafeArray, f32, len, dist_type),
            "f64" => max_scatter_test!(UnsafeArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "AtomicArray" => match elem.as_str() {
            "u8" => max_scatter_test!(AtomicArray, u8, len, dist_type),
            "u16" => max_scatter_test!(AtomicArray, u16, len, dist_type),
            "u32" => max_scatter_test!(AtomicArray, u32, len, dist_type),
            "u64" => max_scatter_test!(AtomicArray, u64, len, dist_type),
            "u128" => max_scatter_test!(AtomicArray, u128, len, dist_type),
            "usize" => max_scatter_test!(AtomicArray, usize, len, dist_type),
            "i8" => max_scatter_test!(AtomicArray, i8, len, dist_type),
            "i16" => max_scatter_test!(AtomicArray, i16, len, dist_type),
            "i32" => max_scatter_test!(AtomicArray, i32, len, dist_type),
            "i64" => max_scatter_test!(AtomicArray, i64, len, dist_type),
            "i128" => max_scatter_test!(AtomicArray, i128, len, dist_type),
            "isize" => max_scatter_test!(AtomicArray, isize, len, dist_type),
            "f32" => max_scatter_test!(AtomicArray, f32, len, dist_type),
            "f64" => max_scatter_test!(AtomicArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        _ => eprintln!("unsupported array type"),
    }
}

use lamellar::array::prelude::*;
use lamellar::memregion::prelude::*;

fn initialize_mem_region<T: Dist + std::ops::AddAssign>(
    memregion: &LamellarMemoryRegion<T>,
    init_val: T,
    inc_val: T,
) {
    unsafe {
        let mut i = init_val;
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

macro_rules! put_all_buffer_test {
    ($array:ident, $t:ty, $len:expr, $dist:ident) => {{
        let world = lamellar::LamellarWorldBuilder::new().build();
        let num_pes = world.num_pes();
        let my_pe = world.my_pe();
        let array_total_len = $len;
        let mut success = true;
        let array: $array<$t> = $array::<$t>::new(world.team(), array_total_len, $dist)
            .block()
            .into();

        let pe_len = array_total_len / num_pes;
        let shared_mem_region: LamellarMemoryRegion<$t> =
            world.alloc_shared_mem_region(pe_len).block().into();

        let init_val = 0 as $t;
        initialize_array!($array, array, init_val);
        array.wait_all();
        array.barrier();
        initialize_mem_region(&shared_mem_region, 0 as $t, 1 as $t);
        world.barrier();

        if my_pe == 0 {
            for tx_size in 1..=pe_len {
                println!("tx_size: {:?}", tx_size);
                let num_txs = pe_len / tx_size;
                for tx in 0..num_txs {
                    #[allow(unused_unsafe)]
                    unsafe {
                        let _ = array
                            .put_all_buffer(
                                tx * tx_size,
                                &shared_mem_region.sub_region(
                                    tx * tx_size..std::cmp::min(pe_len, (tx + 1) * tx_size),
                                ),
                            )
                            .spawn();
                    }
                }
                array.wait_all();
                array.barrier();

                for target_pe in 0..num_pes {
                    for i in 0..(num_txs * tx_size) {
                        #[allow(unused_unsafe)]
                        let elem = unsafe { array.blocking_get_pe(target_pe, i) };
                        if ((i as $t - elem) as f32).abs() > 0.0001 {
                            eprintln!(
                                "{:?} {:?} {:?}",
                                i as $t,
                                elem,
                                ((i as $t - elem) as f32).abs()
                            );
                            success = false;
                        }
                    }
                }
                array.barrier();
                initialize_array!($array, array, init_val);
                array.wait_all();
                array.barrier();
            }
        } else {
            for tx_size in 1..=pe_len {
                println!("tx_size: {:?}", tx_size);
                array.wait_all();
                array.barrier();
                let num_txs = pe_len / tx_size;
                for target_pe in 0..num_pes {
                    for i in 0..(num_txs * tx_size) {
                        #[allow(unused_unsafe)]
                        let elem = unsafe { array.blocking_get_pe(target_pe, i) };
                        if ((i as $t - elem) as f32).abs() > 0.0001 {
                            eprintln!(
                                "{:?} {:?} {:?}",
                                i as $t,
                                elem,
                                ((i as $t - elem) as f32).abs()
                            );
                            success = false;
                        }
                    }
                }
                array.barrier();
                initialize_array!($array, array, init_val);
                array.wait_all();
                array.barrier();
            }
        }
        println!("done");
        array.barrier();
        world.wait_all();
        world.barrier();

        if !success {
            eprintln!("failed");
        }
    }};
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
            "u8" => put_all_buffer_test!(UnsafeArray, u8, len, dist_type),
            "u16" => put_all_buffer_test!(UnsafeArray, u16, len, dist_type),
            "u32" => put_all_buffer_test!(UnsafeArray, u32, len, dist_type),
            "u64" => put_all_buffer_test!(UnsafeArray, u64, len, dist_type),
            "u128" => put_all_buffer_test!(UnsafeArray, u128, len, dist_type),
            "usize" => put_all_buffer_test!(UnsafeArray, usize, len, dist_type),
            "i8" => put_all_buffer_test!(UnsafeArray, i8, len, dist_type),
            "i16" => put_all_buffer_test!(UnsafeArray, i16, len, dist_type),
            "i32" => put_all_buffer_test!(UnsafeArray, i32, len, dist_type),
            "i64" => put_all_buffer_test!(UnsafeArray, i64, len, dist_type),
            "i128" => put_all_buffer_test!(UnsafeArray, i128, len, dist_type),
            "isize" => put_all_buffer_test!(UnsafeArray, isize, len, dist_type),
            "f32" => put_all_buffer_test!(UnsafeArray, f32, len, dist_type),
            "f64" => put_all_buffer_test!(UnsafeArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "AtomicArray" => match elem.as_str() {
            "u8" => put_all_buffer_test!(AtomicArray, u8, len, dist_type),
            "u16" => put_all_buffer_test!(AtomicArray, u16, len, dist_type),
            "u32" => put_all_buffer_test!(AtomicArray, u32, len, dist_type),
            "u64" => put_all_buffer_test!(AtomicArray, u64, len, dist_type),
            "u128" => put_all_buffer_test!(AtomicArray, u128, len, dist_type),
            "usize" => put_all_buffer_test!(AtomicArray, usize, len, dist_type),
            "i8" => put_all_buffer_test!(AtomicArray, i8, len, dist_type),
            "i16" => put_all_buffer_test!(AtomicArray, i16, len, dist_type),
            "i32" => put_all_buffer_test!(AtomicArray, i32, len, dist_type),
            "i64" => put_all_buffer_test!(AtomicArray, i64, len, dist_type),
            "i128" => put_all_buffer_test!(AtomicArray, i128, len, dist_type),
            "isize" => put_all_buffer_test!(AtomicArray, isize, len, dist_type),
            "f32" => put_all_buffer_test!(AtomicArray, f32, len, dist_type),
            "f64" => put_all_buffer_test!(AtomicArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "LocalLockArray" => match elem.as_str() {
            "u8" => put_all_buffer_test!(LocalLockArray, u8, len, dist_type),
            "u16" => put_all_buffer_test!(LocalLockArray, u16, len, dist_type),
            "u32" => put_all_buffer_test!(LocalLockArray, u32, len, dist_type),
            "u64" => put_all_buffer_test!(LocalLockArray, u64, len, dist_type),
            "u128" => put_all_buffer_test!(LocalLockArray, u128, len, dist_type),
            "usize" => put_all_buffer_test!(LocalLockArray, usize, len, dist_type),
            "i8" => put_all_buffer_test!(LocalLockArray, i8, len, dist_type),
            "i16" => put_all_buffer_test!(LocalLockArray, i16, len, dist_type),
            "i32" => put_all_buffer_test!(LocalLockArray, i32, len, dist_type),
            "i64" => put_all_buffer_test!(LocalLockArray, i64, len, dist_type),
            "i128" => put_all_buffer_test!(LocalLockArray, i128, len, dist_type),
            "isize" => put_all_buffer_test!(LocalLockArray, isize, len, dist_type),
            "f32" => put_all_buffer_test!(LocalLockArray, f32, len, dist_type),
            "f64" => put_all_buffer_test!(LocalLockArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "GlobalLockArray" => match elem.as_str() {
            "u8" => put_all_buffer_test!(GlobalLockArray, u8, len, dist_type),
            "u16" => put_all_buffer_test!(GlobalLockArray, u16, len, dist_type),
            "u32" => put_all_buffer_test!(GlobalLockArray, u32, len, dist_type),
            "u64" => put_all_buffer_test!(GlobalLockArray, u64, len, dist_type),
            "u128" => put_all_buffer_test!(GlobalLockArray, u128, len, dist_type),
            "usize" => put_all_buffer_test!(GlobalLockArray, usize, len, dist_type),
            "i8" => put_all_buffer_test!(GlobalLockArray, i8, len, dist_type),
            "i16" => put_all_buffer_test!(GlobalLockArray, i16, len, dist_type),
            "i32" => put_all_buffer_test!(GlobalLockArray, i32, len, dist_type),
            "i64" => put_all_buffer_test!(GlobalLockArray, i64, len, dist_type),
            "i128" => put_all_buffer_test!(GlobalLockArray, i128, len, dist_type),
            "isize" => put_all_buffer_test!(GlobalLockArray, isize, len, dist_type),
            "f32" => put_all_buffer_test!(GlobalLockArray, f32, len, dist_type),
            "f64" => put_all_buffer_test!(GlobalLockArray, f64, len, dist_type),
            _ => {} //eprintln!("unsupported element type"),
        },
        _ => eprintln!("unsupported array type"),
    }
}

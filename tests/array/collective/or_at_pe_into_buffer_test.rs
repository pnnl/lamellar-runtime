use lamellar::array::prelude::*;
use lamellar::memregion::prelude::*;

fn initialize_mem_region<T: Dist + std::ops::AddAssign>(
    memregion: &SharedMemoryRegion<T>,
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

macro_rules! bit_or_at_pe_into_buffer_test {
    ($array:ident, $t:ty, $len:expr, $dist:ident) => {{
        let world = lamellar::LamellarWorldBuilder::new().build();
        let num_pes = world.num_pes();
        let my_pe = world.my_pe();
        let array_total_len = $len;
        let mem_seg_len = array_total_len;
        let mut success = true;
        let array: $array<$t> = $array::<$t>::new(world.team(), array_total_len * num_pes, $dist)
            .block()
            .into();

        let mut shared_mem_region = world.alloc_shared_mem_region(mem_seg_len).block();

        let init_val = (1 << my_pe) as $t;
        initialize_array!($array, array, init_val);
        array.wait_all();
        array.barrier();
        let final_val = !(!0 << num_pes);
        let root = 1usize;
        initialize_mem_region(&shared_mem_region, num_pes as $t, 1 as $t);
        let _lock = lock_if_needed!($array, array);

        for tx_size in 1..=mem_seg_len {
            let mut buffer =
                unsafe { LamellarBuffer::from_shared_memory_region(shared_mem_region) };
            let num_txs = mem_seg_len / tx_size;
            for tx in 0..num_txs {
                let buf =
                    buffer.split_off(std::cmp::min(mem_seg_len, (tx + 1) * tx_size) - tx * tx_size);
                let target = if my_pe == root {
                    RootOrLamellarBuffer::Root(buffer)
                } else {
                    RootOrLamellarBuffer::NotRoot(root)
                };
                #[allow(unused_unsafe)]
                unsafe {
                    array_or_lock!($array, array, _lock)
                        .bit_or_at_pe_into_buffer(
                            tx * tx_size,
                            std::cmp::min(mem_seg_len, (tx + 1) * tx_size) - tx * tx_size,
                            target,
                        )
                        .block();
                }
                buffer = buf;
            }
            array.barrier();
            shared_mem_region = buffer
                .try_unwrap()
                .expect("could not unwrap buffer into mem_region");
            if my_pe == root {
                unsafe {
                    for elem in shared_mem_region.as_slice().iter().take(num_txs * tx_size) {
                        if ((final_val as $t - elem) as f32).abs() > 0.0001 {
                            eprintln!(
                                "[{:?}] {:?} {:?} {:?}",
                                my_pe,
                                final_val,
                                elem,
                                ((final_val as $t - elem) as f32).abs()
                            );
                            success = false;
                        }
                    }
                }
                initialize_mem_region(&shared_mem_region, num_pes as $t, 1 as $t);
            }
            array.barrier();
            array.wait_all();
            array.barrier();
        }
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
            "u8" => bit_or_at_pe_into_buffer_test!(UnsafeArray, u8, len, dist_type),
            "u16" => bit_or_at_pe_into_buffer_test!(UnsafeArray, u16, len, dist_type),
            "u32" => bit_or_at_pe_into_buffer_test!(UnsafeArray, u32, len, dist_type),
            "u64" => bit_or_at_pe_into_buffer_test!(UnsafeArray, u64, len, dist_type),
            "u128" => bit_or_at_pe_into_buffer_test!(UnsafeArray, u128, len, dist_type),
            "usize" => bit_or_at_pe_into_buffer_test!(UnsafeArray, usize, len, dist_type),
            "i8" => bit_or_at_pe_into_buffer_test!(UnsafeArray, i8, len, dist_type),
            "i16" => bit_or_at_pe_into_buffer_test!(UnsafeArray, i16, len, dist_type),
            "i32" => bit_or_at_pe_into_buffer_test!(UnsafeArray, i32, len, dist_type),
            "i64" => bit_or_at_pe_into_buffer_test!(UnsafeArray, i64, len, dist_type),
            "i128" => bit_or_at_pe_into_buffer_test!(UnsafeArray, i128, len, dist_type),
            "isize" => bit_or_at_pe_into_buffer_test!(UnsafeArray, isize, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "AtomicArray" => match elem.as_str() {
            "u8" => bit_or_at_pe_into_buffer_test!(AtomicArray, u8, len, dist_type),
            "u16" => bit_or_at_pe_into_buffer_test!(AtomicArray, u16, len, dist_type),
            "u32" => bit_or_at_pe_into_buffer_test!(AtomicArray, u32, len, dist_type),
            "u64" => bit_or_at_pe_into_buffer_test!(AtomicArray, u64, len, dist_type),
            "u128" => bit_or_at_pe_into_buffer_test!(AtomicArray, u128, len, dist_type),
            "usize" => bit_or_at_pe_into_buffer_test!(AtomicArray, usize, len, dist_type),
            "i8" => bit_or_at_pe_into_buffer_test!(AtomicArray, i8, len, dist_type),
            "i16" => bit_or_at_pe_into_buffer_test!(AtomicArray, i16, len, dist_type),
            "i32" => bit_or_at_pe_into_buffer_test!(AtomicArray, i32, len, dist_type),
            "i64" => bit_or_at_pe_into_buffer_test!(AtomicArray, i64, len, dist_type),
            "i128" => bit_or_at_pe_into_buffer_test!(AtomicArray, i128, len, dist_type),
            "isize" => bit_or_at_pe_into_buffer_test!(AtomicArray, isize, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "GlobalLockArray" => match elem.as_str() {
            "u8" => bit_or_at_pe_into_buffer_test!(GlobalLockArray, u8, len, dist_type),
            "u16" => bit_or_at_pe_into_buffer_test!(GlobalLockArray, u16, len, dist_type),
            "u32" => bit_or_at_pe_into_buffer_test!(GlobalLockArray, u32, len, dist_type),
            "u64" => bit_or_at_pe_into_buffer_test!(GlobalLockArray, u64, len, dist_type),
            "u128" => bit_or_at_pe_into_buffer_test!(GlobalLockArray, u128, len, dist_type),
            "usize" => bit_or_at_pe_into_buffer_test!(GlobalLockArray, usize, len, dist_type),
            "i8" => bit_or_at_pe_into_buffer_test!(GlobalLockArray, i8, len, dist_type),
            "i16" => bit_or_at_pe_into_buffer_test!(GlobalLockArray, i16, len, dist_type),
            "i32" => bit_or_at_pe_into_buffer_test!(GlobalLockArray, i32, len, dist_type),
            "i64" => bit_or_at_pe_into_buffer_test!(GlobalLockArray, i64, len, dist_type),
            "i128" => bit_or_at_pe_into_buffer_test!(GlobalLockArray, i128, len, dist_type),
            "isize" => bit_or_at_pe_into_buffer_test!(GlobalLockArray, isize, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        _ => eprintln!("unsupported array type"),
    }
}

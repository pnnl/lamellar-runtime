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

macro_rules! initialize_array_local {
    (UnsafeArray,$array:ident,$init_val:ident) => {
        unsafe {
            $array
                .local_iter_mut()
                .for_each(move |x| *x = $init_val)
                .block();
        }
    };
    (AtomicArray,$array:ident,$init_val:ident) => {
        $array
            .local_iter_mut()
            .for_each(move |x| x.store($init_val))
            .block();
    };
    (LocalLockArray,$array:ident,$init_val:ident) => {
        $array
            .local_iter_mut()
            .for_each(move |x| *x = $init_val)
            .block();
    };
    (GlobalLockArray,$array:ident,$init_val:ident) => {
        $array
            .local_iter_mut()
            .for_each(move |x| *x = $init_val)
            .block();
    };
    (ReadOnlyArray,$array:ident,$init_val:ident) => {
        let temp = $array.into_unsafe().block();
        unsafe {
            temp.local_iter_mut()
                .for_each(move |x| *x = $init_val)
                .block();
        }
        $array = temp.into_read_only().block();
    };
}

macro_rules! blocking_get_into_buffer_pe_test{
    ($array:ident, $t:ty, $len:expr, $dist:ident) =>{{
        let world = lamellar::LamellarWorldBuilder::new().build();
        let num_pes = world.num_pes();
        let my_pe = world.my_pe();
        let array_total_len = $len;
        #[allow(unused_mut)]
        let mut success = true;
        #[allow(unused_mut)]
        let mut array: $array::<$t> = $array::<$t>::new(world.team(), array_total_len, $dist).block().into();

        let init_val = my_pe as $t;
        initialize_array_local!($array, array, init_val);
        array.wait_all();
        array.barrier();

        let pe_len = array_total_len/num_pes;
        let mut shared_mem_region = world.alloc_shared_mem_region(pe_len).block();

        for pe in 0..num_pes {
            initialize_mem_region(&shared_mem_region,num_pes as $t,0 as $t);
            for tx_size in 1..=pe_len {
                let mut buffer = unsafe{LamellarBuffer::from_shared_memory_region(shared_mem_region)};
                let num_txs = pe_len/tx_size;
                for tx in 0..num_txs{
                    let buf = buffer.split_off(std::cmp::min(pe_len,(tx+1)*tx_size)- tx*tx_size);
                    #[allow(unused_unsafe)]
                    unsafe { array.blocking_get_into_buffer_pe(pe,tx*tx_size,buffer); }
                    buffer = buf;
                }
                array.barrier();
                shared_mem_region = buffer.try_unwrap().expect("could not unwrap buffer into mem_region");
                unsafe{
                    for elem in shared_mem_region.as_slice().iter().take( num_txs * tx_size){
                        if ((pe as $t - elem) as f32).abs() > 0.0001 {
                            eprintln!("{:?} {:?} {:?}",pe as $t,elem,((pe as $t - elem) as f32).abs());
                            success = false;
                        }
                    }
                }
                array.barrier();
                initialize_mem_region(&shared_mem_region,num_pes as $t,0 as $t);
                array.wait_all();
                array.barrier();
            }
        }

        array.barrier();
        world.wait_all();
        world.barrier();
        if !success{
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
            "u8" => blocking_get_into_buffer_pe_test!(UnsafeArray, u8, len, dist_type),
            "u16" => blocking_get_into_buffer_pe_test!(UnsafeArray, u16, len, dist_type),
            "u32" => blocking_get_into_buffer_pe_test!(UnsafeArray, u32, len, dist_type),
            "u64" => blocking_get_into_buffer_pe_test!(UnsafeArray, u64, len, dist_type),
            "u128" => blocking_get_into_buffer_pe_test!(UnsafeArray, u128, len, dist_type),
            "usize" => blocking_get_into_buffer_pe_test!(UnsafeArray, usize, len, dist_type),
            "i8" => blocking_get_into_buffer_pe_test!(UnsafeArray, i8, len, dist_type),
            "i16" => blocking_get_into_buffer_pe_test!(UnsafeArray, i16, len, dist_type),
            "i32" => blocking_get_into_buffer_pe_test!(UnsafeArray, i32, len, dist_type),
            "i64" => blocking_get_into_buffer_pe_test!(UnsafeArray, i64, len, dist_type),
            "i128" => blocking_get_into_buffer_pe_test!(UnsafeArray, i128, len, dist_type),
            "isize" => blocking_get_into_buffer_pe_test!(UnsafeArray, isize, len, dist_type),
            "f32" => blocking_get_into_buffer_pe_test!(UnsafeArray, f32, len, dist_type),
            "f64" => blocking_get_into_buffer_pe_test!(UnsafeArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "AtomicArray" => match elem.as_str() {
            "u8" => blocking_get_into_buffer_pe_test!(AtomicArray, u8, len, dist_type),
            "u16" => blocking_get_into_buffer_pe_test!(AtomicArray, u16, len, dist_type),
            "u32" => blocking_get_into_buffer_pe_test!(AtomicArray, u32, len, dist_type),
            "u64" => blocking_get_into_buffer_pe_test!(AtomicArray, u64, len, dist_type),
            "u128" => blocking_get_into_buffer_pe_test!(AtomicArray, u128, len, dist_type),
            "usize" => blocking_get_into_buffer_pe_test!(AtomicArray, usize, len, dist_type),
            "i8" => blocking_get_into_buffer_pe_test!(AtomicArray, i8, len, dist_type),
            "i16" => blocking_get_into_buffer_pe_test!(AtomicArray, i16, len, dist_type),
            "i32" => blocking_get_into_buffer_pe_test!(AtomicArray, i32, len, dist_type),
            "i64" => blocking_get_into_buffer_pe_test!(AtomicArray, i64, len, dist_type),
            "i128" => blocking_get_into_buffer_pe_test!(AtomicArray, i128, len, dist_type),
            "isize" => blocking_get_into_buffer_pe_test!(AtomicArray, isize, len, dist_type),
            "f32" => blocking_get_into_buffer_pe_test!(AtomicArray, f32, len, dist_type),
            "f64" => blocking_get_into_buffer_pe_test!(AtomicArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "LocalLockArray" => match elem.as_str() {
            "u8" => blocking_get_into_buffer_pe_test!(LocalLockArray, u8, len, dist_type),
            "u16" => blocking_get_into_buffer_pe_test!(LocalLockArray, u16, len, dist_type),
            "u32" => blocking_get_into_buffer_pe_test!(LocalLockArray, u32, len, dist_type),
            "u64" => blocking_get_into_buffer_pe_test!(LocalLockArray, u64, len, dist_type),
            "u128" => blocking_get_into_buffer_pe_test!(LocalLockArray, u128, len, dist_type),
            "usize" => blocking_get_into_buffer_pe_test!(LocalLockArray, usize, len, dist_type),
            "i8" => blocking_get_into_buffer_pe_test!(LocalLockArray, i8, len, dist_type),
            "i16" => blocking_get_into_buffer_pe_test!(LocalLockArray, i16, len, dist_type),
            "i32" => blocking_get_into_buffer_pe_test!(LocalLockArray, i32, len, dist_type),
            "i64" => blocking_get_into_buffer_pe_test!(LocalLockArray, i64, len, dist_type),
            "i128" => blocking_get_into_buffer_pe_test!(LocalLockArray, i128, len, dist_type),
            "isize" => blocking_get_into_buffer_pe_test!(LocalLockArray, isize, len, dist_type),
            "f32" => blocking_get_into_buffer_pe_test!(LocalLockArray, f32, len, dist_type),
            "f64" => blocking_get_into_buffer_pe_test!(LocalLockArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "GlobalLockArray" => match elem.as_str() {
            "u8" => blocking_get_into_buffer_pe_test!(GlobalLockArray, u8, len, dist_type),
            "u16" => blocking_get_into_buffer_pe_test!(GlobalLockArray, u16, len, dist_type),
            "u32" => blocking_get_into_buffer_pe_test!(GlobalLockArray, u32, len, dist_type),
            "u64" => blocking_get_into_buffer_pe_test!(GlobalLockArray, u64, len, dist_type),
            "u128" => blocking_get_into_buffer_pe_test!(GlobalLockArray, u128, len, dist_type),
            "usize" => blocking_get_into_buffer_pe_test!(GlobalLockArray, usize, len, dist_type),
            "i8" => blocking_get_into_buffer_pe_test!(GlobalLockArray, i8, len, dist_type),
            "i16" => blocking_get_into_buffer_pe_test!(GlobalLockArray, i16, len, dist_type),
            "i32" => blocking_get_into_buffer_pe_test!(GlobalLockArray, i32, len, dist_type),
            "i64" => blocking_get_into_buffer_pe_test!(GlobalLockArray, i64, len, dist_type),
            "i128" => blocking_get_into_buffer_pe_test!(GlobalLockArray, i128, len, dist_type),
            "isize" => blocking_get_into_buffer_pe_test!(GlobalLockArray, isize, len, dist_type),
            "f32" => blocking_get_into_buffer_pe_test!(GlobalLockArray, f32, len, dist_type),
            "f64" => blocking_get_into_buffer_pe_test!(GlobalLockArray, f64, len, dist_type),
            _ => {} //eprintln!("unsupported element type"),
        },
        "ReadOnlyArray" => match elem.as_str() {
            "u8" => blocking_get_into_buffer_pe_test!(ReadOnlyArray, u8, len, dist_type),
            "u16" => blocking_get_into_buffer_pe_test!(ReadOnlyArray, u16, len, dist_type),
            "u32" => blocking_get_into_buffer_pe_test!(ReadOnlyArray, u32, len, dist_type),
            "u64" => blocking_get_into_buffer_pe_test!(ReadOnlyArray, u64, len, dist_type),
            "u128" => blocking_get_into_buffer_pe_test!(ReadOnlyArray, u128, len, dist_type),
            "usize" => blocking_get_into_buffer_pe_test!(ReadOnlyArray, usize, len, dist_type),
            "i8" => blocking_get_into_buffer_pe_test!(ReadOnlyArray, i8, len, dist_type),
            "i16" => blocking_get_into_buffer_pe_test!(ReadOnlyArray, i16, len, dist_type),
            "i32" => blocking_get_into_buffer_pe_test!(ReadOnlyArray, i32, len, dist_type),
            "i64" => blocking_get_into_buffer_pe_test!(ReadOnlyArray, i64, len, dist_type),
            "i128" => blocking_get_into_buffer_pe_test!(ReadOnlyArray, i128, len, dist_type),
            "isize" => blocking_get_into_buffer_pe_test!(ReadOnlyArray, isize, len, dist_type),
            "f32" => blocking_get_into_buffer_pe_test!(ReadOnlyArray, f32, len, dist_type),
            "f64" => blocking_get_into_buffer_pe_test!(ReadOnlyArray, f64, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        _ => eprintln!("unsupported array type"),
    }
}

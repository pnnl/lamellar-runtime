use lamellar::array::prelude::*;

macro_rules! into_test_identity {
    ($array1:ident) => {{
        let world = lamellar::LamellarWorldBuilder::new().build();
        let _num_pes = world.num_pes();
        let _my_pe = world.my_pe();

        let array = $array1::<u32>::new(world.clone(), 1000, Distribution::Block).block();
        let _array2: $array1<u32> = array;
    }};
}

macro_rules! into_test_conv {
    ($array1:ident, $array2:ident, $method:ident) => {{
        let world = lamellar::LamellarWorldBuilder::new().build();
        let _num_pes = world.num_pes();
        let _my_pe = world.my_pe();

        let array = $array1::<u32>::new(world.clone(), 1000, Distribution::Block).block();
        let _array2: $array2<u32> = array.$method().block();
    }};
}

macro_rules! into_test {
    (UnsafeArray, UnsafeArray) => { into_test_identity!(UnsafeArray) };
    (ReadOnlyArray, ReadOnlyArray) => { into_test_identity!(ReadOnlyArray) };
    (AtomicArray, AtomicArray) => { into_test_identity!(AtomicArray) };
    (LocalLockArray, LocalLockArray) => { into_test_identity!(LocalLockArray) };
    (GlobalLockArray, GlobalLockArray) => { into_test_identity!(GlobalLockArray) };

    (UnsafeArray, ReadOnlyArray) => { into_test_conv!(UnsafeArray, ReadOnlyArray, into_read_only) };
    (UnsafeArray, AtomicArray) => { into_test_conv!(UnsafeArray, AtomicArray, into_atomic) };
    (UnsafeArray, LocalLockArray) => { into_test_conv!(UnsafeArray, LocalLockArray, into_local_lock) };
    (UnsafeArray, GlobalLockArray) => { into_test_conv!(UnsafeArray, GlobalLockArray, into_global_lock) };

    (ReadOnlyArray, UnsafeArray) => { into_test_conv!(ReadOnlyArray, UnsafeArray, into_unsafe) };
    (ReadOnlyArray, AtomicArray) => { into_test_conv!(ReadOnlyArray, AtomicArray, into_atomic) };
    (ReadOnlyArray, LocalLockArray) => { into_test_conv!(ReadOnlyArray, LocalLockArray, into_local_lock) };
    (ReadOnlyArray, GlobalLockArray) => { into_test_conv!(ReadOnlyArray, GlobalLockArray, into_global_lock) };

    (AtomicArray, UnsafeArray) => { into_test_conv!(AtomicArray, UnsafeArray, into_unsafe) };
    (AtomicArray, ReadOnlyArray) => { into_test_conv!(AtomicArray, ReadOnlyArray, into_read_only) };
    (AtomicArray, LocalLockArray) => { into_test_conv!(AtomicArray, LocalLockArray, into_local_lock) };
    (AtomicArray, GlobalLockArray) => { into_test_conv!(AtomicArray, GlobalLockArray, into_global_lock) };

    (LocalLockArray, UnsafeArray) => { into_test_conv!(LocalLockArray, UnsafeArray, into_unsafe) };
    (LocalLockArray, ReadOnlyArray) => { into_test_conv!(LocalLockArray, ReadOnlyArray, into_read_only) };
    (LocalLockArray, AtomicArray) => { into_test_conv!(LocalLockArray, AtomicArray, into_atomic) };
    (LocalLockArray, GlobalLockArray) => { into_test_conv!(LocalLockArray, GlobalLockArray, into_global_lock) };

    (GlobalLockArray, UnsafeArray) => { into_test_conv!(GlobalLockArray, UnsafeArray, into_unsafe) };
    (GlobalLockArray, ReadOnlyArray) => { into_test_conv!(GlobalLockArray, ReadOnlyArray, into_read_only) };
    (GlobalLockArray, AtomicArray) => { into_test_conv!(GlobalLockArray, AtomicArray, into_atomic) };
    (GlobalLockArray, LocalLockArray) => { into_test_conv!(GlobalLockArray, LocalLockArray, into_local_lock) };
}

macro_rules! match_array2 {
    ($array_ty:ident, $array2_str:tt) => {
        match $array2_str.as_str() {
            "UnsafeArray" => into_test!($array_ty, UnsafeArray),
            // "LocalOnlyArray" => into_test!($array_ty, LocalOnlyArray),
            "ReadOnlyArray" => into_test!($array_ty, ReadOnlyArray),
            "AtomicArray" => into_test!($array_ty, AtomicArray),
            "LocalLockArray" => into_test!($array_ty, LocalLockArray),
            "GlobalLockArray" => into_test!($array_ty, GlobalLockArray),
            _ => panic!("Unknown array type: {}", $array2_str),
        }
    };
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let array1 = args[1].clone();
    let array2 = args[2].clone();

    match array1.as_str() {
        "UnsafeArray" => match_array2!(UnsafeArray, array2),
        // "LocalOnlyArray" => match_array2!(LocalOnlyArray, array2),
        "ReadOnlyArray" => match_array2!(ReadOnlyArray, array2),
        "AtomicArray" => match_array2!(AtomicArray, array2),
        "LocalLockArray" => match_array2!(LocalLockArray, array2),
        "GlobalLockArray" => match_array2!(GlobalLockArray, array2),
        _ => panic!("Unknown array type: {}", array1),
    }
}

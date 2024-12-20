use lamellar::array::prelude::*;
macro_rules! into_test {
    ($array1:ident, $array2:ident) => {{
        let world = lamellar::LamellarWorldBuilder::new().build();
        let _num_pes = world.num_pes();
        let _my_pe = world.my_pe();

        let array = $array1::<u32>::new(world.clone(), 1000, Distribution::Block).block();
        let _array2: $array2<u32> = array.into();
    }};
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

use assert_cmd::Command;
use serial_test::serial;

macro_rules! create_test {
    ($array1:ty,$array2:ty) => {
        paste::paste! {
            #[test]
            #[serial]
            #[allow(non_snake_case)]
            fn [<$array1 _ into _ $array2>](){
                let profile = std::env::var("LAMELLAR_TEST_PROFILE").unwrap_or_else(|_| "release".to_string());
                let result = Command::new(format!("./target/{}/examples/array_into_test",profile))
                    .arg(stringify!($array1))
                    .arg(stringify!($array2))
                    .arg("--")
                    .arg("--map-by")
                    .arg("node:PE=4")
                    .arg("--np")
                    .arg("2")
                    .assert();
                println!("{:?}",result);
                result.stderr("").success();
            }
        }
    };
}

macro_rules! into_types {
    ($array1:ty, ($($array2:ty),*)) =>{
        $(create_test!($array1,$array2);)*
    }
}

macro_rules! loop_type {
    ( ($($array1:ty),*), $array2:tt) =>{
        // $(
            // println!("{:?} {:?} {:?} {:?}",stringify!($array),stringify!($elem),stringify!($num_pes),stringify!($len));
            $(into_types!($array1,$array2);)*
        // )*
    }
}

macro_rules! create_into_tests {
    ( $array:tt) => {
        loop_type!($array, $array);
    };
}

create_into_tests!((
    UnsafeArray,
    // LocalOnlyArray,
    ReadOnlyArray,
    AtomicArray,
    LocalLockArray,
    GlobalLockArray
));

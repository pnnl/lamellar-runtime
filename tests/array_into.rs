use assert_cmd::Command;
use serial_test::serial;
use std::path::PathBuf;

macro_rules! create_test {
    ($array1:ty,$array2:ty) => {
        paste::paste! {
            #[test]
            #[serial]
            #[allow(non_snake_case)]
            fn [<$array1 _ into _ $array2>](){
                let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
                d.push("lamellar_run.sh");
                let result = Command::new(d.into_os_string())
                    .arg("-N=2")
                    .arg("-T=4")
                    .arg("./target/release/examples/array_into_test")
                    .arg(stringify!($array1))
                    .arg(stringify!($array2))
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
    LocalOnlyArray,
    ReadOnlyArray,
    AtomicArray,
    LocalLockAtomicArray
));

use assert_cmd::Command;
use serial_test::serial;
use std::path::PathBuf;

macro_rules! create_test {
    ( $array:ty, $dist:expr, $elem:ty, $num_pes:expr, $len:expr) => {
        paste::paste! {
            #[test]
            #[serial]
            #[allow(non_snake_case)]
            #[cfg(not(feature="slurm-test"))]
            fn [<$array _ $dist _ $elem _ $num_pes _ $len _ fetch_add>](){
                let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
                d.push("lamellar_run.sh");
                let result = Command::new(d.into_os_string())
                    .arg(format!("-N={}",$num_pes))
                    .arg("-T=4")
                    .arg("./target/release/examples/fetch_add_test")
                    .arg(stringify!($array))
                    .arg($dist)
                    .arg(stringify!($elem))
                    .arg(stringify!($len))
                    .assert();
                println!("{:?}",result);
                result.stderr("").success();
            }

            #[test]
            #[allow(non_snake_case)]
            #[cfg(feature="slurm-test")]
            fn [<$array _ $dist _ $elem _ $num_pes _ $len _ fetch_add>](){
                // let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
                // d.push("lamellar_run.sh");
                let result = Command::new("srun")
                    .env("LAMELLAE_BACKEND","rofi")
                    .env("LAMELLLAR_THREADS","10")
                    .arg(format!("-N {}",$num_pes))
                    .arg("--partition=all")
                    .arg("--mpi=pmi2")
                    .arg("./target/release/examples/fetch_add_test")
                    .arg(stringify!($array))
                    .arg($dist)
                    .arg(stringify!($elem))
                    .arg(stringify!($len))
                    .assert();
                println!("{:?}",result);
                result.stderr("").success();
            }
        }
    };
}

macro_rules! iter_lens{
    ( $array:ty, $dist:expr, $elem:ty, $num_pes:expr, ($($len:expr),*)) =>{
        $(
            // println!("{:?} {:?} {:?} {:?}",stringify!($array),stringify!($elem),stringify!($num_pes),stringify!($len));
            create_test!($array,$dist,$elem,$num_pes,$len);
        )*
    }
}

macro_rules! iter_num_pes {
    ( $array:ty, $dist:expr, $elem:ty, ($($num_pes:expr),*), $len:tt) =>{
        $(
            // println!("{:?} {:?} {:?} {:?}",stringify!($array),stringify!($elem),stringify!($num_pes),stringify!($len));
            iter_lens!($array,$dist,$elem,$num_pes,$len);
        )*
    }
}

macro_rules! iter_elem_types {
    ( $array:ty, $dist:expr, ($($elem:ty),*), $num_pes:tt, $len:tt) =>{
        $(
            iter_num_pes!($array,$dist,$elem,$num_pes,$len);
        )*
    }
}

macro_rules! iter_dist_types {
    ( $array:ty, ($($dist:expr),*),  $elem:tt, $num_pes:tt, $len:tt) =>{
        $(
            iter_elem_types!($array,$dist,$elem,$num_pes,$len);
        )*
    }
}

macro_rules! create_fetch_add_tests {
    ( ($($array:ty),*), $dist:tt, $elem:tt, $num_pes:tt, $len:tt) =>{
        $(iter_dist_types!($array,$dist,$elem,$num_pes,$len);)*
    }
}

create_fetch_add_tests!(
    (LocalLockAtomicArray), // (UnsafeArray, AtomicArray, Atomic2Array, LocalLockAtomicArray),
    ("Block", "Cyclic"),
    (u8, u16, u32, u128, usize, i8, i16, i32, i128, isize, f32, f64),
    (2, 3, 4),
    (4, 19, 128)
);

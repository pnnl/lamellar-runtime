use assert_cmd::Command;
use serial_test::serial;

macro_rules! create_test {
    ( $array:ty, $dist:expr, $elem:ty, $num_pes:expr, $len:expr) => {
        paste::paste! {
            #[test]
            #[serial]
            #[allow(non_snake_case)]
            fn [<$array _ $dist _ $elem _ $num_pes _ $len __broadcast>](){
                let profile = std::env::var("LAMELLAR_TEST_PROFILE").unwrap_or_else(|_| "release".to_string());
                let result = Command::new(format!("./target/{}/examples/broadcast_test",profile))
                    .arg(stringify!($array))
                    .arg($dist)
                    .arg(stringify!($elem))
                    .arg(stringify!($len))
                    .arg("--")
                    .arg("--map-by")
                    .arg("node:PE=4")
                    .arg("--np")
                    .arg(format!("{}", $num_pes))
                    .arg("--timeout")
                    .arg("30")
                    .assert();
                println!("Result:  {:?}",result);
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

macro_rules! create_sum_all_tests {
    ( ($($array:ty),*), $dist:tt, $elem:tt, $num_pes:tt, $len:tt) =>{
        $(iter_dist_types!($array,$dist,$elem,$num_pes,$len);)*
    }
}

create_sum_all_tests!(
    (AtomicArray, GlobalLockArray),
    ("Block", "Cyclic"),
    (u8),
    (2, 4),
    (4, 20, 128)
);

// create_sum_all_tests!(
//     (GlobalLockArray),
//     ("Block", "Cyclic"),
//     (u8, f64),
//     (4),
//     (4, 9)
// );
// create_iput_tests!((UnsafeArray,AtomicArray,LocalLockArray),("Block"),(u8,u16,f64),(2),(4));

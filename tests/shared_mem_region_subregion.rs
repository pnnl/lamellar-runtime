use assert_cmd::Command;
use serial_test::serial;

macro_rules! create_test {
    ( $elem:ty, $num_pes:expr, $len:expr, $start:expr, $end:expr) => {
        paste::paste! {
            #[test]
            #[serial]
            #[allow(non_snake_case)]
            fn [<shared_mem_region_subregion_ $elem _ $num_pes _ $len _ $start _ $end>](){
                let profile = std::env::var("LAMELLAR_TEST_PROFILE").unwrap_or_else(|_| "release".to_string());
                let result = Command::new(format!("./target/{}/examples/shared_mem_region_subregion_test",profile))
                    .arg(stringify!($elem))
                    .arg(stringify!($len))
                    .arg(stringify!($start))
                    .arg(stringify!($end))
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

macro_rules! iter_num_pes {
    ( $elem:ty, ($($num_pes:expr),*), $len:expr, $start:expr, $end:expr) =>{
        $(
            create_test!($elem,$num_pes,$len,$start,$end);
        )*
    }
}

macro_rules! iter_elem_types {
    ( ($($elem:ty),*), $num_pes:tt, $len:expr, $start:expr, $end:expr) =>{
        $(
            iter_num_pes!($elem,$num_pes,$len,$start,$end);
        )*
    }
}

iter_elem_types!((i32, i64, u32, u64, f32, f64), (2, 3, 4), 20, 5, 15);

// #[test]
// fn array() {
//     let t = trybuild::TestCases::new();
//     t.compile_fail("tests/array/local_only/immutable_borrow.rs");
//     t.compile_fail("tests/array/local_only/clone.rs");
// }

// #[test]
// fn unsafe_array(){

// }
// mod array{
//     mod r#unsafe{ 
//         mod block{
//             mod put;
//         }
//     }
// }

use assert_cmd::Command;
use std::path::PathBuf;
use serial_test::serial;

macro_rules! create_test {
    ( $array:ty, $elem:ty, $num_pes:expr, $len:expr) =>{
        paste::paste!{
            #[test]
            #[serial]
            #[allow(non_snake_case)]
            fn [<$array _ $elem _ $num_pes _ $len _ iput>](){
                let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
                d.push("lamellar_run.sh");
                let result = Command::new(d.into_os_string())
                    .arg(format!("-N={}",$num_pes))
                    .arg("-T=4")
                    .arg("./target/release/examples/iput")
                    .arg(stringify!($array))
                    .arg(stringify!($elem))
                    .arg(stringify!($len))
                    .assert();
                println!("{:?}",result);
                result.stderr("").success();
            }
        }
    }
}

macro_rules! iter_lens{
    ( $array:ty, $elem:ty, $num_pes:expr, ($($len:expr),*)) =>{
        $(
            // println!("{:?} {:?} {:?} {:?}",stringify!($array),stringify!($elem),stringify!($num_pes),stringify!($len));
            create_test!($array,$elem,$num_pes,$len);
        )*
    }
}

macro_rules! iter_num_pes {
    ( $array:ty, $elem:ty, ($($num_pes:expr),*), $len:tt) =>{
        $(
            // println!("{:?} {:?} {:?} {:?}",stringify!($array),stringify!($elem),stringify!($num_pes),stringify!($len));
            iter_lens!($array,$elem,$num_pes,$len);
        )*
    }
}

macro_rules! iter_elem_types {
    ( $array:ty, ($($elem:ty),*), $num_pes:tt, $len:tt) =>{
        $(
            iter_num_pes!($array,$elem,$num_pes,$len);
        )*
    }
}

macro_rules! create_iput_tests {
    ( ($($array:ty),*), $elem:tt, $num_pes:tt, $len:tt) =>{
        $(iter_elem_types!($array,$elem,$num_pes,$len);)*
    }
}



create_iput_tests!((UnsafeArray,AtomicArray,CollectiveAtomicArray),(u8,u16,u32,u128,usize,i8,i16,i32,i128,isize,f32,f64),(2,3,4),(4,19,1031));

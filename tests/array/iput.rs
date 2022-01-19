use nix::{sys::wait::wait,unistd::{fork, ForkResult::{Child}}};

use lamellar::array::{LamellarReadArray, Distribution, UnsafeArray,ReadOnlyArray,AtomicArray,AtomicOps,CollectiveAtomicArray, SerialIterator};
use lamellar::{Dist,ActiveMessaging, LamellarMemoryRegion, RemoteMemoryRegion};

// fn initialize_array<T: Dist>(array: &UnsafeArray<T>,init_val: T) {
//     array.dist_iter_mut().for_each(move |x| *x = init_val);
//     array.wait_all();
//     array.barrier();
// }

fn initialize_mem_region<T: Dist + std::ops::AddAssign>(memregion: &LamellarMemoryRegion<T>,init_val: T,inc_val: T) {
    unsafe {
        let mut i = init_val; //(len_per_pe * my_pe as f32).round() as usize;
        for elem in memregion.as_mut_slice().unwrap() {
            *elem = i;
            i += inc_val;
        }
    }
}

macro_rules! initialize_array{
    (UnsafeArray,$array:ident,$init_val:ident) => {
        $array.dist_iter_mut().for_each(move |x| *x = $init_val);
    };
    (AtomicArray,$array:ident,$init_val:ident) => {
        $array.dist_iter().for_each(move |x| x.store($init_val));
    };
    (CollectiveAtomicArray,$array:ident,$init_val:ident) => {
        $array.dist_iter_mut().for_each(move |x| *x = $init_val);
    };
}

macro_rules! iput_test{
    ($array:ident, $t:ty, $len:expr) =>{
       {
            let world = lamellar::LamellarWorldBuilder::new().build();
            let num_pes = world.num_pes();
            let my_pe = world.my_pe();
            let array_total_len = $len;
            let mem_seg_len = array_total_len;
            let mut success = true;
            let block_array: $array::<$t> = $array::<$t>::new(world.team(), array_total_len, Distribution::Block).into(); //convert into abstract LamellarArray, distributed len is total_len
         
            let shared_mem_region: LamellarMemoryRegion<$t> = world.alloc_shared_mem_region(mem_seg_len).into(); //Convert into abstract LamellarMemoryRegion, each local segment is total_len
            //initialize array
            let init_val = my_pe as $t;
            initialize_array!($array, block_array, init_val);
            block_array.wait_all();
            block_array.barrier();
            initialize_mem_region(&shared_mem_region,0 as $t,1 as $t);
            world.barrier();
            if my_pe == 0{
                block_array.iput(0, &shared_mem_region); //uses the local data of the shared memregion
            }
            block_array.barrier();
            // block_array.print();
            for _pe in 0..num_pes{
                for (i,elem) in block_array.ser_iter().into_iter().enumerate(){
                    if ((i as $t - *elem) as f32).abs() > 0.0001 {
                        println!("{:?} {:?} {:?}",i as $t,*elem,((i as $t - *elem) as f32).abs());
                        success = false;
                    }
                }
            }
            world.wait_all();
            world.barrier();
            if !success{
                eprintln!("failed");
            }
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let array = args[1].clone();
    let elem = args[2].clone();
    let len = args[3].parse::<usize>().unwrap();
    
    match array.as_str(){
        "UnsafeArray" => {
            match elem.as_str() {
                "u8" => iput_test!(UnsafeArray,u8,len),
                "u16" => iput_test!(UnsafeArray,u16,len),
                "u32" => iput_test!(UnsafeArray,u32,len),
                "u64" => iput_test!(UnsafeArray,u64,len),
                "u128" => iput_test!(UnsafeArray,u128,len),
                "usize" => iput_test!(UnsafeArray,usize,len),
                "i8" => iput_test!(UnsafeArray,u8,len),
                "i16" => iput_test!(UnsafeArray,u16,len),
                "i32" => iput_test!(UnsafeArray,u32,len),
                "i64" => iput_test!(UnsafeArray,u64,len),
                "i128" => iput_test!(UnsafeArray,u128,len),
                "isize" => iput_test!(UnsafeArray,usize,len),
                "f32" => iput_test!(UnsafeArray,f32,len),
                "f64" => iput_test!(UnsafeArray,f64,len),
                _ =>  eprintln!("unsupported element type"),
            }
        }
        "AtomicArray" => {
            match elem.as_str() {
                "u8" => iput_test!(AtomicArray,u8,len),
                "u16" => iput_test!(AtomicArray,u16,len),
                "u32" => iput_test!(AtomicArray,u32,len),
                "u64" => iput_test!(AtomicArray,u64,len),
                "u128" => iput_test!(AtomicArray,u128,len),
                "usize" => iput_test!(AtomicArray,usize,len),
                "i8" => iput_test!(AtomicArray,u8,len),
                "i16" => iput_test!(AtomicArray,u16,len),
                "i32" => iput_test!(AtomicArray,u32,len),
                "i64" => iput_test!(AtomicArray,u64,len),
                "i128" => iput_test!(AtomicArray,u128,len),
                "isize" => iput_test!(AtomicArray,usize,len),
                "f32" => iput_test!(AtomicArray,f32,len),
                "f64" => iput_test!(AtomicArray,f64,len),
                _ =>  eprintln!("unsupported element type"),
            }
        }
        "CollectiveAtomicArray" => {
            match elem.as_str() {
                "u8" => iput_test!(CollectiveAtomicArray,u8,len),
                "u16" => iput_test!(CollectiveAtomicArray,u16,len),
                "u32" => iput_test!(CollectiveAtomicArray,u32,len),
                "u64" => iput_test!(CollectiveAtomicArray,u64,len),
                "u128" => iput_test!(CollectiveAtomicArray,u128,len),
                "usize" => iput_test!(CollectiveAtomicArray,usize,len),
                "i8" => iput_test!(CollectiveAtomicArray,u8,len),
                "i16" => iput_test!(CollectiveAtomicArray,u16,len),
                "i32" => iput_test!(CollectiveAtomicArray,u32,len),
                "i64" => iput_test!(CollectiveAtomicArray,u64,len),
                "i128" => iput_test!(CollectiveAtomicArray,u128,len),
                "isize" => iput_test!(CollectiveAtomicArray,usize,len),
                "f32" => iput_test!(CollectiveAtomicArray,f32,len),
                "f64" => iput_test!(CollectiveAtomicArray,f64,len),
                _ =>  eprintln!("unsupported element type"),
            }
        }
        _ => eprintln!("unsupported array type")
    }    
}
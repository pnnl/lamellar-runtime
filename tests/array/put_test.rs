use nix::{sys::wait::wait,unistd::{fork, ForkResult::{Child}}};

use lamellar::array::{LamellarReadArray, Distribution, UnsafeArray,ReadOnlyArray,AtomicArray,AtomicOps,CollectiveAtomicArray, SerialIterator};
use lamellar::{Dist,ActiveMessaging, LamellarMemoryRegion, RemoteMemoryRegion};

// fn initialize_array<T: Dist>(array: &UnsafeArray<T>,init_val: T) {
//     array.dist_iter_mut().for_each(move |x| *x = init_val);
//     array.wait_all();
//     array.barrier();
// }

// fn initialize_mem_region<T: Dist + std::ops::AddAssign>(memregion: &LamellarMemoryRegion<T>,init_val: T) {
//     unsafe {
//         for elem in memregion.as_mut_slice().unwrap() {
//             *elem = init_val;
//         }
//     }
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

// macro_rules! iput_test{
//     ($array:ident, $t:ty, $len:expr) =>{
//        {
//             let world = lamellar::LamellarWorldBuilder::new().build();
//             let num_pes = world.num_pes();
//             let my_pe = world.my_pe();
//             let array_total_len = $len;
//             let mem_seg_len = array_total_len;
//             let mut success = true;
//             let block_array: $array::<$t> = $array::<$t>::new(world.team(), array_total_len, Distribution::Block).into(); //convert into abstract LamellarArray, distributed len is total_len
         
//             let local_mem_region: LamellarMemoryRegion<$t> = world.alloc_local_mem_region(1).into(); //Convert into abstract LamellarMemoryRegion, each local segment is total_len
//             //initialize array
//             let init_val = my_pe as $t;
//             initialize_array!($array, block_array, init_val);
//             block_array.wait_all();
//             block_array.barrier();
//             initialize_mem_region(&local_mem_region,my_pe as $t);
            
//             for i in (my_pe..array_total_len).step_by(num_pes){
//                 block_array.put(i, &local_mem_region); //uses the local data of the shared memregion
//             }
//             block_array.wait_all();
//             block_array.barrier();
//             // block_array.print();
            
//             for (i,elem) in block_array.ser_iter().into_iter().enumerate(){
//                 if (( ((i as usize)%num_pes) as $t - *elem) as f32).abs() > 0.0001 {
//                     println!("{:?} {:?} {:?}",i as $t,*elem,((i as $t - *elem) as f32).abs());
//                     success = false;
//                 }
//             }
            
//             world.wait_all();
//             world.barrier();
//             if !success{
//                 eprintln!("failed");
//             }
//         }
//     }
// }

macro_rules! put_test{
    ($array:ident, $t:ty, $len:expr, $dist:ident) =>{
       {
            let world = lamellar::LamellarWorldBuilder::new().build();
            let num_pes = world.num_pes();
            let my_pe = world.my_pe();
            let array_total_len = $len;
            let mem_seg_len = array_total_len;
            let mut success = true;
            let array: $array::<$t> = $array::<$t>::new(world.team(), array_total_len, $dist).into(); //convert into abstract LamellarArray, distributed len is total_len
         
            let shared_mem_region: LamellarMemoryRegion<$t> = world.alloc_shared_mem_region(mem_seg_len).into(); //Convert into abstract LamellarMemoryRegion, each local segment is total_len
            //initialize array
            let init_val = my_pe as $t;
            initialize_array!($array, array, init_val);
            array.wait_all();
            array.barrier();
            initialize_mem_region(&shared_mem_region,0 as $t,1 as $t);
            // world.barrier();
            
            for tx_size in 1..=mem_seg_len{
                let num_txs = mem_seg_len/tx_size;
                for tx in (my_pe..num_txs).step_by(num_pes){
                    // unsafe{println!("tx_size {:?} tx {:?} sindex: {:?} eindex: {:?} {:?}",tx_size,tx, tx*tx_size,std::cmp::min(mem_seg_len,(tx+1)*tx_size),&shared_mem_region.sub_region(tx*tx_size..std::cmp::min(mem_seg_len,(tx+1)*tx_size)).as_slice());}
                    array.put(tx*tx_size,&shared_mem_region.sub_region(tx*tx_size..std::cmp::min(mem_seg_len,(tx+1)*tx_size)));
                }
                array.wait_all();
                array.barrier();
                for (i,elem) in array.ser_iter().into_iter().enumerate().take( num_txs * tx_size){
                    if ((i as $t - *elem) as f32).abs() > 0.0001 {
                        println!("{:?} {:?} {:?}",i as $t,*elem,((i as $t - *elem) as f32).abs());
                        success = false;
                    }
                }
                array.barrier();
                // array.print();
                initialize_array!($array, array, init_val);
                array.wait_all();
                array.barrier();
            }
            array.barrier();
            world.wait_all();
            world.barrier();

            

            let half_len = array_total_len/2;
            let start_i = half_len/2;
            let end_i = start_i + half_len;
            let sub_array = array.sub_array(start_i..end_i);
            world.barrier();
            // sub_array.print();
            for tx_size in 1..=half_len{
                let num_txs = half_len/tx_size;
                for tx in (my_pe..num_txs).step_by(num_pes){
                    // unsafe{println!("tx_size {:?} tx {:?} sindex: {:?} eindex: {:?} {:?}",tx_size,tx, tx*tx_size,std::cmp::min(half_len,(tx+1)*tx_size),&shared_mem_region.sub_region(tx*tx_size..std::cmp::min(half_len,(tx+1)*tx_size)).as_slice());}
                    sub_array.put(tx*tx_size,&shared_mem_region.sub_region(tx*tx_size..std::cmp::min(half_len,(tx+1)*tx_size)));
                }
                array.wait_all();
                sub_array.barrier();
                for (i,elem) in sub_array.ser_iter().into_iter().enumerate().take( num_txs * tx_size){
                    if ((i as $t - *elem) as f32).abs() > 0.0001 {
                        println!("{:?} {:?} {:?}",i as $t,*elem,((i as $t - *elem) as f32).abs());
                        success = false;
                    }
                }
                sub_array.barrier();
                // sub_array.print();
                initialize_array!($array, array, init_val);
                sub_array.wait_all();
                sub_array.barrier();
                // sub_array.print();
            }
            array.barrier();
            world.wait_all();
            world.barrier();
            
            let pe_len = array_total_len/num_pes;

            for pe in 0..num_pes{
                let len = pe_len/2;
                let start_i = (pe*pe_len)+ len/2;
              
                let end_i = start_i+len;
                let sub_array = array.sub_array(start_i..end_i);
                world.barrier();
                
                for tx_size in 1..len{
                    let num_txs = len/tx_size;
                    for tx in (my_pe..num_txs).step_by(num_pes){
                        // unsafe{println!("tx_size {:?} tx {:?} sindex: {:?} eindex: {:?} {:?}",tx_size,tx, tx*tx_size,std::cmp::min(len,(tx+1)*tx_size),&shared_mem_region.sub_region(tx*tx_size..std::cmp::min(mem_seg_len,(tx+1)*tx_size)).as_slice());}
                        sub_array.put(tx*tx_size,&shared_mem_region.sub_region(tx*tx_size..std::cmp::min(len,(tx+1)*tx_size)));
                    }
                    array.wait_all();
                    sub_array.barrier();
                    for (i,elem) in sub_array.ser_iter().into_iter().enumerate().take( num_txs * tx_size){
                        if ((i as $t - *elem) as f32).abs() > 0.0001 {
                            println!("{:?} {:?} {:?}",i as $t,*elem,((i as $t - *elem) as f32).abs());
                            success = false;
                        }
                    }
                    sub_array.barrier();
                    // sub_array.print();
                    initialize_array!($array, array, init_val);
                    sub_array.wait_all();
                    sub_array.barrier();
                }
                array.barrier();
                world.wait_all();
                world.barrier();
            }

            if !success{
                eprintln!("failed");
            }
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let array = args[1].clone();
    let dist = args[2].clone();
    let elem = args[3].clone();
    let len = args[4].parse::<usize>().unwrap();

    let dist_type = match dist.as_str(){
        "Block" => Distribution::Block,
        "Cyclic" => Distribution::Cyclic,
        _ =>  panic!("unsupported dist type"),
    };
    
    match array.as_str(){
        "UnsafeArray" => {
            match elem.as_str() {
                "u8" => put_test!(UnsafeArray,u8,len,dist_type),
                "u16" => put_test!(UnsafeArray,u16,len,dist_type),
                "u32" => put_test!(UnsafeArray,u32,len,dist_type),
                "u64" => put_test!(UnsafeArray,u64,len,dist_type),
                "u128" => put_test!(UnsafeArray,u128,len,dist_type),
                "usize" => put_test!(UnsafeArray,usize,len,dist_type),
                "i8" => put_test!(UnsafeArray,u8,len,dist_type),
                "i16" => put_test!(UnsafeArray,u16,len,dist_type),
                "i32" => put_test!(UnsafeArray,u32,len,dist_type),
                "i64" => put_test!(UnsafeArray,u64,len,dist_type),
                "i128" => put_test!(UnsafeArray,u128,len,dist_type),
                "isize" => put_test!(UnsafeArray,usize,len,dist_type),
                "f32" => put_test!(UnsafeArray,f32,len,dist_type),
                "f64" => put_test!(UnsafeArray,f64,len,dist_type),
                _ =>  eprintln!("unsupported element type"),
            }
        }
        "AtomicArray" => {
            match elem.as_str() {
                "u8" => put_test!(AtomicArray,u8,len,dist_type),
                "u16" => put_test!(AtomicArray,u16,len,dist_type),
                "u32" => put_test!(AtomicArray,u32,len,dist_type),
                "u64" => put_test!(AtomicArray,u64,len,dist_type),
                "u128" => put_test!(AtomicArray,u128,len,dist_type),
                "usize" => put_test!(AtomicArray,usize,len,dist_type),
                "i8" => put_test!(AtomicArray,u8,len,dist_type),
                "i16" => put_test!(AtomicArray,u16,len,dist_type),
                "i32" => put_test!(AtomicArray,u32,len,dist_type),
                "i64" => put_test!(AtomicArray,u64,len,dist_type),
                "i128" => put_test!(AtomicArray,u128,len,dist_type),
                "isize" => put_test!(AtomicArray,usize,len,dist_type),
                "f32" => put_test!(AtomicArray,f32,len,dist_type),
                "f64" => put_test!(AtomicArray,f64,len,dist_type),
                _ =>  eprintln!("unsupported element type"),
            }
        }
        "CollectiveAtomicArray" => {
            match elem.as_str() {
                "u8" => put_test!(CollectiveAtomicArray,u8,len,dist_type),
                "u16" => put_test!(CollectiveAtomicArray,u16,len,dist_type),
                "u32" => put_test!(CollectiveAtomicArray,u32,len,dist_type),
                "u64" => put_test!(CollectiveAtomicArray,u64,len,dist_type),
                "u128" => put_test!(CollectiveAtomicArray,u128,len,dist_type),
                "usize" => put_test!(CollectiveAtomicArray,usize,len,dist_type),
                "i8" => put_test!(CollectiveAtomicArray,u8,len,dist_type),
                "i16" => put_test!(CollectiveAtomicArray,u16,len,dist_type),
                "i32" => put_test!(CollectiveAtomicArray,u32,len,dist_type),
                "i64" => put_test!(CollectiveAtomicArray,u64,len,dist_type),
                "i128" => put_test!(CollectiveAtomicArray,u128,len,dist_type),
                "isize" => put_test!(CollectiveAtomicArray,usize,len,dist_type),
                "f32" => put_test!(CollectiveAtomicArray,f32,len,dist_type),
                "f64" => put_test!(CollectiveAtomicArray,f64,len,dist_type),
                _ =>  eprintln!("unsupported element type"),
            }
        }
        _ => eprintln!("unsupported array type")
    }    
}
use lamellar::array::{AtomicArray,CollectiveAtomicArray};




macro_rules! initialize_array{
    (UnsafeArray,$array:ident,$init_val:ident) => {
        $array.dist_iter_mut().for_each(move |x| *x = $init_val);
        $array.wait_all();
        $array.barrier();
    };
    (AtomicArray,$array:ident,$init_val:ident) => {
        $array.dist_iter().for_each(move |x| x.store($init_val));
        $array.wait_all();
        $array.barrier();
    };
    (CollectiveAtomicArray,$array:ident,$init_val:ident) => {
        $array.dist_iter_mut().for_each(move |x| *x = $init_val);
        $array.wait_all();
        $array.barrier();
    };
}

macro_rules! check_val{
    (UnsafeArray,$val:ident,$max_val:ident,$valid:ident) => {
       if (($val - $max_val)as f32).abs() > 0.0001{//because unsafe we might lose some updates, but val should never be greater than max_val
           $valid = false;
       }
    };
    (AtomicArray,$val:ident,$max_val:ident,$valid:ident) => {
        if (($val - $max_val)as f32).abs() > 0.0001{//all updates should be preserved
            $valid = false;
        }
    };
    (CollectiveAtomicArray,$val:ident,$max_val:ident,$valid:ident) => {
        if (($val - $max_val)as f32).abs()  > 0.0001{//all updates should be preserved
            $valid = false;
        }
    };
}

macro_rules! and_test{
    ($array:ident, $t:ty, $len:expr, $dist:ident) =>{
       {
            let world = lamellar::LamellarWorldBuilder::new().build();
            let num_pes = world.num_pes();
            let my_pe = world.my_pe();
            let array_total_len = $len;

            let mut success = true;
            let array: $array::<$t> = $array::<$t>::new(world.team(), array_total_len, $dist).into(); //convert into abstract LamellarArray, distributed len is total_len
         
            let init_val =(num_pes as $t);
            initialize_array!($array, array, init_val);
            array.wait_all();
            array.barrier();
            for idx in 0..array.len(){
                if idx%num_pes == my_pe{
                    let val = array.swap(idx,my_pe as $t).get().unwrap();
                    check_val!($array,val,init_val,success);
                    if !success{
                        println!("{:?} {:?} {:?}",idx,val,init_val);
                    }
                }                
            }
            array.wait_all();
            array.barrier();
            for idx in 0..array.len(){
                let val = array.load(idx).get().unwrap();
                let check_val = (idx%num_pes) as $t;
                check_val!($array,val,check_val,success);
                if !success{
                    println!("{:?} {:?} {:?}",idx,val,check_val);
                }
                
            }
            array.barrier();
            initialize_array!($array, array, init_val);
            array.wait_all();
            array.barrier();
            


            let half_len = array_total_len/2;
            let start_i = half_len/2;
            let end_i = start_i + half_len;
            let sub_array = array.sub_array(start_i..end_i);
            sub_array.barrier();
            for idx in 0..sub_array.len(){
                if idx%num_pes == my_pe{
                    let val = sub_array.swap(idx,my_pe as $t).get().unwrap();
                    check_val!($array,val,init_val,success);
                    if !success{
                        println!("{:?} {:?} {:?}",idx,val,init_val);
                    }
                }                
            }
            sub_array.wait_all();
            sub_array.barrier();
            for idx in 0..sub_array.len(){
                let val = sub_array.load(idx).get().unwrap();
                let check_val = (idx%num_pes) as $t;
                check_val!($array,val,check_val,success);
                if !success{
                    println!("{:?} {:?} {:?}",idx,val,check_val);
                }
            }
            sub_array.barrier();
            initialize_array!($array, array, init_val);
            sub_array.wait_all();
            sub_array.barrier();
            


            let pe_len = array_total_len/num_pes;
            for pe in 0..num_pes{
                let len = std::cmp::max(pe_len/2,1);
                let start_i = (pe*pe_len)+ len/2;
                let end_i = start_i+len;
                let sub_array = array.sub_array(start_i..end_i);
                sub_array.barrier();
                for idx in 0..sub_array.len(){
                    if idx%num_pes == my_pe{
                        let val = sub_array.swap(idx,my_pe as $t).get().unwrap();
                        check_val!($array,val,init_val,success);
                        if !success{
                            println!("{:?} {:?} {:?}",idx,val,init_val);
                        }
                    }                
                }
                sub_array.wait_all();
                sub_array.barrier();
                for idx in 0..sub_array.len(){
                    let val = sub_array.load(idx).get().unwrap();
                    let check_val = (idx%num_pes) as $t;
                    check_val!($array,val,check_val,success);
                    if !success{
                        println!("{:?} {:?} {:?}",idx,val,check_val);
                    }
                }
                sub_array.barrier();
                initialize_array!($array, array, init_val);
                sub_array.wait_all();
                sub_array.barrier();
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
        "Block" => lamellar::array::Distribution::Block,
        "Cyclic" => lamellar::array::Distribution::Cyclic,
        _ =>  panic!("unsupported dist type"),
    };
    
    match array.as_str(){
        "AtomicArray" => {
            match elem.as_str() {
                "u8" => and_test!(AtomicArray,u8,len,dist_type),
                "u16" => and_test!(AtomicArray,u16,len,dist_type),
                "u32" => and_test!(AtomicArray,u32,len,dist_type),
                "u64" => and_test!(AtomicArray,u64,len,dist_type),
                "u128" => and_test!(AtomicArray,u128,len,dist_type),
                "usize" => and_test!(AtomicArray,usize,len,dist_type),
                "i8" => and_test!(AtomicArray,i8,len,dist_type),
                "i16" => and_test!(AtomicArray,i16,len,dist_type),
                "i32" => and_test!(AtomicArray,i32,len,dist_type),
                "i64" => and_test!(AtomicArray,i64,len,dist_type),
                "i128" => and_test!(AtomicArray,i128,len,dist_type),
                "isize" => and_test!(AtomicArray,isize,len,dist_type),
                "f32" => and_test!(AtomicArray,f32,len,dist_type),
                "f64" => and_test!(AtomicArray,f64,len,dist_type),
                _ =>  eprintln!("unsupported element type"),
            }
        }
        "CollectiveAtomicArray" => {
            match elem.as_str() {
                "u8" => and_test!(CollectiveAtomicArray,u8,len,dist_type),
                "u16" => and_test!(CollectiveAtomicArray,u16,len,dist_type),
                "u32" => and_test!(CollectiveAtomicArray,u32,len,dist_type),
                "u64" => and_test!(CollectiveAtomicArray,u64,len,dist_type),
                "u128" => and_test!(CollectiveAtomicArray,u128,len,dist_type),
                "usize" => and_test!(CollectiveAtomicArray,usize,len,dist_type),
                "i8" => and_test!(CollectiveAtomicArray,i8,len,dist_type),
                "i16" => and_test!(CollectiveAtomicArray,i16,len,dist_type),
                "i32" => and_test!(CollectiveAtomicArray,i32,len,dist_type),
                "i64" => and_test!(CollectiveAtomicArray,i64,len,dist_type),
                "i128" => and_test!(CollectiveAtomicArray,i128,len,dist_type),
                "isize" => and_test!(CollectiveAtomicArray,isize,len,dist_type),
                "f32" => and_test!(CollectiveAtomicArray,f32,len,dist_type),
                "f64" => and_test!(CollectiveAtomicArray,f64,len,dist_type),
                _ =>  eprintln!("unsupported element type"),
            }
        }
        _ => eprintln!("unsupported array type")
    }    
}
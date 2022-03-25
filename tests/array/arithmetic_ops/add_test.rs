use lamellar::array::{
    ArithmeticOps, AtomicArray, DistributedIterator, LocalLockAtomicArray, SerialIterator,
    UnsafeArray,
};
use lamellar::RemoteMemoryRegion;

use rand::distributions::Uniform;
use rand::seq::SliceRandom;

macro_rules! initialize_array {
    (UnsafeArray,$array:ident,$init_val:ident) => {
        $array.dist_iter_mut().for_each(move |x| *x = $init_val);
        $array.wait_all();
        $array.barrier();
    };
    (AtomicArray,$array:ident,$init_val:ident) => {
        $array.dist_iter().for_each(move |x| x.store($init_val));
        $array.wait_all();
        $array.barrier();
        // println!("----------------------------------------------");
    };
    (LocalLockAtomicArray,$array:ident,$init_val:ident) => {
        $array.dist_iter_mut().for_each(move |x| *x = $init_val);
        $array.wait_all();
        $array.barrier();
    };
}

macro_rules! check_val{
    (UnsafeArray,$val:ident,$max_val:ident,$valid:ident) => {
       if $val > $max_val || $val as usize == 0{//because unsafe we might lose some updates, but val should never be greater than max_val
           $valid = false;
       }
    };
    (AtomicArray,$val:ident,$max_val:ident,$valid:ident) => {
        if (($val - $max_val)as f64).abs() > 0.0001{//all updates should be preserved
            $valid = false;
        }
    };
    (LocalLockAtomicArray,$val:ident,$max_val:ident,$valid:ident) => {
        if (($val - $max_val)as f64).abs()  > 0.0001{//all updates should be preserved
            $valid = false;
        }
    };
}

macro_rules! add_test{
    ($array:ident, $t:ty, $len:expr, $dist:ident) =>{
       {
            let world = lamellar::LamellarWorldBuilder::new().build();
            let num_pes = world.num_pes();
            let my_pe = world.my_pe();
            let array_total_len = $len;

            let mut rng = rand::thread_rng();
            let _rand_idx = Uniform::from(0..array_total_len);
            let mut success = true;
            let array: $array::<$t> = $array::<$t>::new(world.team(), array_total_len, $dist).into(); //convert into abstract LamellarArray, distributed len is total_len

            let pe_max_val: $t = if std::any::TypeId::of::<$t>() == std::any::TypeId::of::<f32>(){
                9 as $t
            }
            else{
                50 as $t
            };

            // let max_val = pe_max_val * num_pes as $t;
            let mut max_val = 0 as $t;
            for pe in 0..num_pes{
                max_val += (10_usize.pow((pe*2)as u32) as $t * pe_max_val);
                // println!("max_val {:?} {:?}",pe,max_val);
            }
            let init_val = 0 as $t;
            initialize_array!($array, array, init_val);
            array.wait_all();
            array.barrier();

            for idx in 0..array.len(){
                for _i in 0..(pe_max_val as usize){
                    array.add(idx,(10_usize.pow((my_pe*2)as u32)) as $t);
                }
            }
            array.wait_all();
            array.barrier();
            for (i,elem) in array.ser_iter().into_iter().enumerate(){
                let val = *elem;
                check_val!($array,val,max_val,success);
                if !success{
                    println!("full_0 {:?} {:?} {:?}",i,val,max_val);
                }
            }
            if !success{
                array.print()
            }
            array.wait_all();
            array.barrier();
            initialize_array!($array, array, init_val);
            array.wait_all();
            array.barrier();
            // let num_updates=max_updates!($t,num_pes);
            let mut indices = (0..array_total_len).collect::<Vec<usize>>();
            for _i in (0..(pe_max_val as usize -1)){
                indices.extend( (0..array_total_len).collect::<Vec<usize>>());
            }
            indices.shuffle(&mut rng);
            for idx in indices.iter() {//0..num_updates{
                // let idx = rand_idx.sample(&mut rng);
                array.add(idx,(10_usize.pow((my_pe*2)as u32)) as $t);
            }
            array.wait_all();
            array.barrier();
            for (i,elem) in array.ser_iter().into_iter().enumerate(){
                let val = *elem;
                check_val!($array,val,max_val,success);
                if !success{
                    println!("full_1 {:?} {:?} {:?}",i,val,max_val);
                }
            }
            if !success{
                array.print()
            }
            // let sum = array.ser_iter().into_iter().fold(0,|acc,x| acc+ *x as usize);
            // let tot_updates = indices.len()/10 * max_val as usize;
            // check_val!($array,sum,tot_updates,success);
            // if !success{
            //     println!("full_1 {:?} {:?}",sum,tot_updates);
            // }
            world.wait_all();
            world.barrier();
            initialize_array!($array, array, init_val);
            // println!("-----------------");


            let half_len = array_total_len/2;
            let start_i = half_len/2;
            let end_i = start_i + half_len;
            let _rand_idx = Uniform::from(0..half_len);
            let sub_array = array.sub_array(start_i..end_i);
            sub_array.barrier();
            for idx in 0..sub_array.len(){
                for _i in 0..(pe_max_val as usize){
                    sub_array.add(idx,(10_usize.pow((my_pe*2)as u32)) as $t);
                }
            }
            sub_array.wait_all();
            sub_array.barrier();
            for (i,elem) in sub_array.ser_iter().into_iter().enumerate(){
                let val = *elem;
                check_val!($array,val,max_val,success);
                if !success{
                    println!("half_0 {:?} {:?} {:?}",i,val,max_val);
                }
            }
            array.wait_all();
            sub_array.barrier();
            initialize_array!($array, array, init_val);
            sub_array.wait_all();
            sub_array.barrier();
            // let num_updates=max_updates!($t,num_pes);
            let mut indices = (0..half_len).collect::<Vec<usize>>();
            for _i in (0..(pe_max_val as usize-1)){
                indices.extend( (0..half_len).collect::<Vec<usize>>());
            }
            indices.shuffle(&mut rng);
            for idx in indices.iter(){ // in 0..num_updates{
                // let idx = rand_idx.sample(&mut rng);
                sub_array.add(idx,(10_usize.pow((my_pe*2)as u32)) as $t);
            }
            sub_array.wait_all();
            sub_array.barrier();
            for (i,elem) in sub_array.ser_iter().into_iter().enumerate(){
                let val = *elem;
                check_val!($array,val,max_val,success);
                if !success{
                    println!("half_1 {:?} {:?} {:?}",i,val,max_val);
                }
            }
            if !success{
                array.print()
            }
            // let sum = sub_array.ser_iter().into_iter().fold(0,|acc,x| acc+ *x as usize);
            // let tot_updates = indices.len()/10 * max_val as usize;
            // check_val!($array,sum,tot_updates,success);
            // if !success{
            //     println!("half_1 {:?} {:?}",sum,tot_updates);
            // }
            sub_array.wait_all();
            sub_array.barrier();
            initialize_array!($array, array, init_val);

            // println!("-------------------");
            let pe_len = array_total_len/num_pes;
            for pe in 0..num_pes{
                let len = std::cmp::max(pe_len/2,1);
                let start_i = (pe*pe_len)+ len/2;
                let end_i = start_i+len;
                let _rand_idx = Uniform::from(0..len);
                let sub_array = array.sub_array(start_i..end_i);
                sub_array.barrier();
                for idx in 0..sub_array.len(){
                    for _i in 0..(pe_max_val as usize){
                        sub_array.add(idx,(10_usize.pow((my_pe*2)as u32)) as $t);
                    }
                }
                sub_array.wait_all();
                sub_array.barrier();
                for (i,elem) in sub_array.ser_iter().into_iter().enumerate(){
                    let val = *elem;
                    check_val!($array,val,max_val,success);
                    if !success{
                        println!("small_0 {:?} {:?} {:?}",i,val,max_val);
                    }
                }
                array.wait_all();
                sub_array.barrier();
                initialize_array!($array, array, init_val);
                sub_array.wait_all();
                sub_array.barrier();
                // let num_updates=max_updates!($t,num_pes);
                let mut indices = (0..len).collect::<Vec<usize>>();
                for _i in (0..(pe_max_val as usize-1)){
                    indices.extend( (0..len).collect::<Vec<usize>>());
                }
                indices.shuffle(&mut rng);
                for idx in indices.iter() {//0..num_updates{
                    // let idx = rand_idx.sample(&mut rng);
                    sub_array.add(idx,(10_usize.pow((my_pe*2)as u32)) as $t);
                }
                sub_array.wait_all();
                sub_array.barrier();
                for (i,elem) in sub_array.ser_iter().into_iter().enumerate(){
                    let val = *elem;
                    check_val!($array,val,max_val,success);
                    if !success{
                        println!("small_1 {:?} {:?} {:?}",i,val,max_val);
                    }
                }
                if !success{
                    array.print()
                }
                // let sum = sub_array.ser_iter().into_iter().fold(0,|acc,x| acc+ *x as usize);
                // let tot_updates = indices.len()/10 * max_val as usize;
                // check_val!($array,sum,tot_updates,success);
                // if !success{
                //     println!("small_1 {:?} {:?}",sum,tot_updates);
                // }
                sub_array.wait_all();
                sub_array.barrier();
                initialize_array!($array, array, init_val);
            }

            if !success{
                eprintln!("failed");
            }
        }
    }
}

macro_rules! check_results {
    ($array_ty:ident, $array:ident, $num_pes:ident, $test:expr) => {
        // println!("test {:?}",$test);
        let mut success = true;
        $array.wait_all();
        $array.barrier();
        for (i, elem) in $array.ser_iter().into_iter().enumerate() {
            let val = *elem;
            check_val!($array_ty, val, $num_pes, success);
            if !success {
                println!("input {:?}: {:?} {:?} {:?}", $test, i, val, $num_pes);
            }
        }
        if !success {
            $array.print();
        }
        $array.barrier();
        let init_val = 0;
        initialize_array!($array_ty, $array, init_val);
        $array.wait_all();
        $array.barrier();
    };
}

macro_rules! input_test{
    ($array:ident,  $len:expr, $dist:ident) =>{
       {
            let world = lamellar::LamellarWorldBuilder::new().build();
            let num_pes = world.num_pes();
            let _my_pe = world.my_pe();
            let array_total_len = $len;

            // let mut success = true;
            let array: $array::<usize> = $array::<usize>::new(world.team(), array_total_len, $dist).into(); //convert into abstract LamellarArray, distributed len is total_len
            let input_array: UnsafeArray::<usize> = UnsafeArray::<usize>::new(world.team(), array_total_len*num_pes, $dist).into(); //convert into abstract LamellarArray, distributed len is total_len
            let init_val=0;
            initialize_array!($array, array, init_val);
            if $dist == lamellar::array::Distribution::Block{
                input_array.dist_iter_mut().enumerate().for_each(move |(i,x)| {println!("i: {:?}",i);*x = i%array_total_len});
            }
            else{
                input_array.dist_iter_mut().enumerate().for_each(move |(i,x)| {println!("i: {:?}",i);*x = i/num_pes});
            }
            input_array.wait_all();
            input_array.barrier();
            input_array.print();
            //individual T------------------------------
            for i in 0..array.len(){
                array.add(i,1);
            }
            check_results!($array,array,num_pes,"T");
            //individual T------------------------------
            for i in 0..array.len(){
                array.add(&i,1);
            }
            check_results!($array,array,num_pes,"&T");
            //&[T]------------------------------
            let vec=(0..array.len()).collect::<Vec<usize>>();
            let slice = &vec[..];
            array.add(slice,1);
            check_results!($array,array,num_pes,"&[T]");
            //scoped &[T]------------------------------
            {
                let vec=(0..array.len()).collect::<Vec<usize>>();
                let slice = &vec[..];
                array.add(slice,1);
            }
            check_results!($array,array,num_pes,"scoped &[T]");
            // Vec<T>------------------------------
            let vec=(0..array.len()).collect::<Vec<usize>>();
            array.add(vec,1);
            check_results!($array,array,num_pes,"Vec<T>");
            // &Vec<T>------------------------------
            let vec=(0..array.len()).collect::<Vec<usize>>();
            array.add(&vec,1);
            check_results!($array,array,num_pes,"&Vec<T>");
            // Scoped Vec<T>------------------------------
            {
                let vec=(0..array.len()).collect::<Vec<usize>>();
                array.add(vec,1);
            }
            check_results!($array,array,num_pes,"scoped Vec<T>");
            // Scoped &Vec<T>------------------------------
            {
                let vec=(0..array.len()).collect::<Vec<usize>>();
                array.add(&vec,1);
            }
            check_results!($array,array,num_pes,"scoped &Vec<T>");

            // LMR<T>------------------------------
            let lmr=world.alloc_local_mem_region(array.len());
            unsafe{
                let slice = lmr.as_mut_slice().unwrap();
                for i in 0..array.len(){
                    slice[i]=i;
                }
            }
            array.add(lmr.clone(),1);
            check_results!($array,array,num_pes,"LMR<T>");
            // &LMR<T>------------------------------
            array.add(&lmr,1);
            check_results!($array,array,num_pes,"&LMR<T>");
            drop(lmr);
            // scoped LMR<T>------------------------------
            {
                let lmr=world.alloc_local_mem_region(array.len());
                unsafe{
                    let slice = lmr.as_mut_slice().unwrap();
                    for i in 0..array.len(){
                        slice[i]=i;
                    }
                }
                array.add(lmr.clone(),1);
                check_results!($array,array,num_pes,"scoped LMR<T>");
            }
            // scoped &LMR<T>------------------------------
            {
                let lmr=world.alloc_local_mem_region(array.len());
                unsafe{
                    let slice = lmr.as_mut_slice().unwrap();
                    for i in 0..array.len(){
                        slice[i]=i;
                    }
                }
                array.add(&lmr,1);
                check_results!($array,array,num_pes,"scoped &LMR<T>");
            }

            // SMR<T>------------------------------
            let smr=world.alloc_shared_mem_region(array.len());
            unsafe{
                let slice = smr.as_mut_slice().unwrap();
                for i in 0..array.len(){
                    slice[i]=i;
                }
            }
            array.add(smr.clone(),1);
            check_results!($array,array,num_pes,"SMR<T>");
            // &SMR<T>------------------------------
            array.add(&smr,1);
            check_results!($array,array,num_pes,"&SMR<T>");
            drop(smr);
            // scoped SMR<T>------------------------------
            {
                let smr=world.alloc_shared_mem_region(array.len());
                unsafe{
                    let slice = smr.as_mut_slice().unwrap();
                    for i in 0..array.len(){
                        slice[i]=i;
                    }
                }
                array.add(smr,1);
                check_results!($array,array,num_pes,"scoped SMR<T>");
            }
            // scoped &SMR<T>------------------------------
            {
                let smr=world.alloc_shared_mem_region(array.len());
                unsafe{
                    let slice = smr.as_mut_slice().unwrap();
                    for i in 0..array.len(){
                        slice[i]=i;
                    }
                }
                array.add(&smr,1);
                check_results!($array,array,num_pes,"scoped &SMR<T>");
            }

            // UnsafeArray<T>------------------------------
            // array.add(input_array.clone(),1);
            // check_results!($array,array,num_pes,"UnsafeArray<T>");
            // UnsafeArray<T>------------------------------
            array.add(&input_array,1);
            check_results!($array,array,num_pes,"&UnsafeArray<T>");

            // ReadOnlyArray<T>------------------------------
            let input_array = input_array.into_read_only();
            // array.add(input_array.clone(),1);
            // check_results!($array,array,num_pes,"ReadOnlyArray<T>");
            // ReadOnlyArray<T>------------------------------
            array.add(&input_array,1);
            check_results!($array,array,num_pes,"&ReadOnlyArray<T>");

            // AtomicArray<T>------------------------------
            let input_array = input_array.into_atomic();
            // array.add(input_array.clone(),1);
            // check_results!($array,array,num_pes,"AtomicArray<T>");
            // AtomicArray<T>------------------------------
            array.add(&input_array,1);
            check_results!($array,array,num_pes,"&AtomicArray<T>");

             // LocalLockAtomicArray<T>------------------------------
             let input_array = input_array.into_local_lock_atomic();
            //  array.add(input_array.clone(),1);
            //  check_results!($array,array,num_pes,"LocalLockAtomicArray<T>");
             // LocalLockAtomicArray<T>------------------------------
             array.add(&input_array,1);
             check_results!($array,array,num_pes,"&LocalLockAtomicArray<T>");
       }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let array = args[1].clone();
    let dist = args[2].clone();
    let elem = args[3].clone();
    let len = args[4].parse::<usize>().unwrap();

    let dist_type = match dist.as_str() {
        "Block" => lamellar::array::Distribution::Block,
        "Cyclic" => lamellar::array::Distribution::Cyclic,
        _ => panic!("unsupported dist type"),
    };

    match array.as_str() {
        "UnsafeArray" => match elem.as_str() {
            "u8" => add_test!(UnsafeArray, u8, len, dist_type),
            "u16" => add_test!(UnsafeArray, u16, len, dist_type),
            "u32" => add_test!(UnsafeArray, u32, len, dist_type),
            "u64" => add_test!(UnsafeArray, u64, len, dist_type),
            "u128" => add_test!(UnsafeArray, u128, len, dist_type),
            "usize" => add_test!(UnsafeArray, usize, len, dist_type),
            "i8" => add_test!(UnsafeArray, i8, len, dist_type),
            "i16" => add_test!(UnsafeArray, i16, len, dist_type),
            "i32" => add_test!(UnsafeArray, i32, len, dist_type),
            "i64" => add_test!(UnsafeArray, i64, len, dist_type),
            "i128" => add_test!(UnsafeArray, i128, len, dist_type),
            "isize" => add_test!(UnsafeArray, isize, len, dist_type),
            "f32" => add_test!(UnsafeArray, f32, len, dist_type),
            "f64" => add_test!(UnsafeArray, f64, len, dist_type),
            "input" => input_test!(UnsafeArray, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "AtomicArray" => match elem.as_str() {
            "u8" => add_test!(AtomicArray, u8, len, dist_type),
            "u16" => add_test!(AtomicArray, u16, len, dist_type),
            "u32" => add_test!(AtomicArray, u32, len, dist_type),
            "u64" => add_test!(AtomicArray, u64, len, dist_type),
            "u128" => add_test!(AtomicArray, u128, len, dist_type),
            "usize" => add_test!(AtomicArray, usize, len, dist_type),
            "i8" => add_test!(AtomicArray, i8, len, dist_type),
            "i16" => add_test!(AtomicArray, i16, len, dist_type),
            "i32" => add_test!(AtomicArray, i32, len, dist_type),
            "i64" => add_test!(AtomicArray, i64, len, dist_type),
            "i128" => add_test!(AtomicArray, i128, len, dist_type),
            "isize" => add_test!(AtomicArray, isize, len, dist_type),
            "f32" => add_test!(AtomicArray, f32, len, dist_type),
            "f64" => add_test!(AtomicArray, f64, len, dist_type),
            "input" => input_test!(AtomicArray, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        "LocalLockAtomicArray" => match elem.as_str() {
            "u8" => add_test!(LocalLockAtomicArray, u8, len, dist_type),
            "u16" => add_test!(LocalLockAtomicArray, u16, len, dist_type),
            "u32" => add_test!(LocalLockAtomicArray, u32, len, dist_type),
            "u64" => add_test!(LocalLockAtomicArray, u64, len, dist_type),
            "u128" => add_test!(LocalLockAtomicArray, u128, len, dist_type),
            "usize" => add_test!(LocalLockAtomicArray, usize, len, dist_type),
            "i8" => add_test!(LocalLockAtomicArray, i8, len, dist_type),
            "i16" => add_test!(LocalLockAtomicArray, i16, len, dist_type),
            "i32" => add_test!(LocalLockAtomicArray, i32, len, dist_type),
            "i64" => add_test!(LocalLockAtomicArray, i64, len, dist_type),
            "i128" => add_test!(LocalLockAtomicArray, i128, len, dist_type),
            "isize" => add_test!(LocalLockAtomicArray, isize, len, dist_type),
            "f32" => add_test!(LocalLockAtomicArray, f32, len, dist_type),
            "f64" => add_test!(LocalLockAtomicArray, f64, len, dist_type),
            "input" => input_test!(LocalLockAtomicArray, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        _ => eprintln!("unsupported array type"),
    }
}

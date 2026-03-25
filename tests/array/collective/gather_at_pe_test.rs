use lamellar::array::prelude::*;
use lamellar::memregion::prelude::*;

fn initialize_mem_region<T: Dist + std::ops::AddAssign + std::ops::Mul<Output = T>>(
    memregion: &LamellarMemoryRegion<T>,
    init_val: T,
    inc_val: T,
) {
    unsafe {
        let mut i = init_val; //(len_per_pe * my_pe as f32).round() as usize;
        for elem in memregion.as_mut_slice() {
            *elem = i;
            i += inc_val;
        }
    }
}

macro_rules! initialize_array {
    (UnsafeArray,$array:ident,$init_val:ident) => {
        unsafe {
            $array
                .dist_iter_mut()
                .for_each(move |x| *x = $init_val)
                .block();
        }
    };
    (AtomicArray,$array:ident,$init_val:ident) => {
        $array
            .dist_iter()
            .for_each(move |x| x.store($init_val))
            .block();
    };
    (LocalLockArray,$array:ident,$init_val:ident) => {
        $array
            .dist_iter_mut()
            .for_each(move |x| *x = $init_val)
            .block();
    };
    (GlobalLockArray,$array:ident,$init_val:ident) => {
        $array
            .dist_iter_mut()
            .for_each(move |x| *x = $init_val)
            .block();
    };
}

macro_rules! gather_to_pe_test{
    ($array:ident, $t:ty, $len:expr, $dist:ident) =>{
       {
            let world = lamellar::LamellarWorldBuilder::new().build();
            let num_pes = world.num_pes();
            let my_pe = world.my_pe();
            let array_total_len = $len;
            let mem_seg_len = array_total_len;
            let mut success = true;
            let array: $array::<$t> = $array::<$t>::new(world.team(), array_total_len, $dist).block().into();

            let shared_mem_region: LamellarMemoryRegion<$t> = world.alloc_shared_mem_region(mem_seg_len).block().into();
            //initialize array
            let init_val = my_pe as $t;
            initialize_array!($array, array, init_val);
            array.wait_all();
            array.barrier();
            initialize_mem_region(&shared_mem_region, my_pe as $t, 0 as $t);

            // gather to PE 0
            let root_pe = 0usize;

            for tx_size in (1..=mem_seg_len).step_by(num_pes){
                let num_txs = mem_seg_len/tx_size;
                let mut reqs = vec![];
                for tx in (0..num_txs){
                    #[allow(unused_unsafe)]
                    reqs.push((unsafe { array.gather_at_pe(tx * tx_size, std::cmp::min(mem_seg_len,(tx+1)*tx_size) - tx * tx_size, root_pe).spawn()}, std::cmp::min(mem_seg_len,(tx+1)*tx_size - tx*tx_size)));
                }
                for req in reqs.drain(..){
                    let maybe_buf = req.0.block();
                    if let Some(buf) = maybe_buf {
                        let mut i = 0;
                        for elem in buf.as_slice().iter(){
                            // expected source PE for element is (i / chunk_size)
                            if ( ( (i/req.1) as $t  - elem) as f32).abs() > 0.0001 {
                                eprintln!("[{:?}] {:?} {:?} {:?} {:?}",my_pe, i as $t, (i/req.1) as $t, elem,( ( (i/req.1) as $t  - elem) as f32).abs());
                                success = false;
                            }
                            i+=1;
                        }
                    }
                }
                array.barrier();
                initialize_array!($array, array, init_val);
                array.wait_all();
                array.barrier();
            }

            // half_len section
            let half_len = array_total_len/2;
            let start_i = half_len/2;
            let end_i = start_i + half_len;
            let sub_array = array.sub_array(start_i..end_i);
            world.barrier();
            for tx_size in (1..=half_len).step_by(num_pes){
                let num_txs = half_len/tx_size;
                let mut reqs = vec![];
                for tx in (0..num_txs){
                    #[allow(unused_unsafe)]
                    reqs.push((unsafe { array.gather_at_pe(tx * tx_size, std::cmp::min(half_len,(tx+1)*tx_size) - tx * tx_size, root_pe).spawn()}, std::cmp::min(half_len,(tx+1)*tx_size - tx*tx_size)));
                }
                for req in reqs.drain(..){
                    let maybe_buf = req.0.block();
                    if let Some(buf) = maybe_buf {
                        let mut i = 0;
                        for elem in buf.as_slice().iter(){
                            if ( ( (i/req.1) as $t  - elem) as f32).abs() > 0.0001 {
                                eprintln!("{:?} {:?} {:?}",i as $t,elem,( ( (i/req.1) as $t  - elem) as f32).abs());
                                success = false;
                            }
                            i+=1;
                        }
                    }
                }
                array.wait_all();
                sub_array.barrier();
                initialize_array!($array, array, init_val);
                sub_array.wait_all();
                sub_array.barrier();
            }

            let pe_len = array_total_len/num_pes;
            for pe in 0..num_pes{
                let len = pe_len/2;
                let start_i = (pe*pe_len)+ len/2;
                let end_i = start_i+len;
                let sub_array = array.sub_array(start_i..end_i);
                world.barrier();

                for tx_size in (1..len).step_by(num_pes){
                    let num_txs = len/tx_size;
                    let mut reqs = vec![];
                    for tx in (0..num_txs){
                        #[allow(unused_unsafe)]
                        reqs.push((unsafe { sub_array.gather_at_pe(tx * tx_size, std::cmp::min(len,(tx+1)*tx_size) - tx * tx_size, root_pe).spawn()}, std::cmp::min(len,(tx+1)*tx_size - tx*tx_size)));
                    }
                    for req in reqs.drain(..){
                        let maybe_buf = req.0.block();
                        if let Some(buf) = maybe_buf {
                            let mut i = 0;
                            for elem in buf.as_slice().iter(){
                                if ( ( (i/req.1) as $t  - elem) as f32).abs() > 0.0001 {
                                    eprintln!("{:?} {:?} {:?}",i as $t,elem,( ( (i/req.1) as $t  - elem) as f32).abs());
                                    success = false;
                                }
                                i+=1;
                            }
                        }
                    }
                    array.wait_all();
                    sub_array.barrier();
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

    let dist_type = match dist.as_str() {
        "Block" => Distribution::Block,
        "Cyclic" => Distribution::Cyclic,
        _ => panic!("unsupported dist type"),
    };

    match array.as_str() {
        "UnsafeArray" => match elem.as_str() {
            "u8" => gather_to_pe_test!(UnsafeArray, u8, len, dist_type),
            // other types omitted for brevity
            _ => eprintln!("unsupported element type"),
        },
        "AtomicArray" => match elem.as_str() {
            "u8" => gather_to_pe_test!(AtomicArray, u8, len, dist_type),
            _ => eprintln!("unsupported element type"),
        },
        _ => eprintln!("unsupported array type"),
    }
}

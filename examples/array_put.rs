use lamellar::RemoteMemoryRegion;
use lamellar::ActiveMessaging;

const ARRAY_LEN: usize = 100;

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    println!("my_pe {:?} num_pes {:?}", my_pe, num_pes);
    let array = world.alloc_mem_region(ARRAY_LEN);
    let init = [255u8; ARRAY_LEN];
    unsafe { array.put(my_pe, 0, &init) };
    println!(" Before {:?}",array.as_slice());
    world.barrier();
    let mut index = my_pe;
    while index < ARRAY_LEN {
        let cur_index = index;
        unsafe { array.put_all(cur_index, &[my_pe as u8]) };
        index += num_pes;
    }
    let mut done = false;
    while !done {
        done = true;
        for i in array.as_slice() {
            if *i == 255_u8 {
                done = false;
                break;
            }
        }
    }

    world.barrier();
    println!("pe: {:?} {:?}", my_pe, array);
    println!("After {:?}",array.as_slice());
    world.free_memory_region(array);
}

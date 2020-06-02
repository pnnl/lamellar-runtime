const ARRAY_LEN: usize = 100;

fn main() {
    let (my_pe, num_pes) = lamellar::init();
    println!("my_pe {:?} num_pes {:?}", my_pe, num_pes);
    let array = lamellar::alloc_mem_region(ARRAY_LEN);
    let init = [255u8; ARRAY_LEN];
    unsafe { array.put(my_pe, 0, &init) };
    lamellar::barrier();
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

    lamellar::barrier();
    println!("pe: {:?} {:?}", my_pe, array);
    lamellar::free_memory_region(array);
    lamellar::finit();
}

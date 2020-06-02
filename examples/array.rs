const ARRAY_LEN: usize = 100;

fn main() {
    let (my_pe, num_pes) = lamellar::init();
    println!("my_pe {:?} num_pes {:?}", my_pe, num_pes);
    let array = lamellar::alloc_mem_region(ARRAY_LEN);
    let init = [255u8; ARRAY_LEN];
    unsafe { array.put(my_pe, 0, &init) };
    println!("pe: {:?} {:?}", my_pe, array);

    lamellar::barrier();
    let mut index = my_pe;
    while index < ARRAY_LEN {
        let cur_index = index;
        lamellar::exec_all(lamellar::FnOnce!([ my_pe,cur_index,array ] move || {
            unsafe { array.as_mut_slice()[cur_index]=my_pe as u8 } ;
        }));
        index += num_pes;
    }
    lamellar::wait_all();
    lamellar::barrier();
    array.delete();
    println!("pe: {:?} {:?}", my_pe, array);
    lamellar::finit();
}

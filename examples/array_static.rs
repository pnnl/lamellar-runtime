#[macro_use]
extern crate lazy_static;

use std::sync::Mutex;

const ARRAY_LEN: usize = 100;
lazy_static! {
    static ref ARRAY: Mutex<Vec<u8>> = Mutex::new(vec![0; ARRAY_LEN]);
}

fn main() {
    let (my_pe, num_pes) = lamellar::init();
    println!("my_pe {:?} num_pes {:?}", my_pe, num_pes);

    lamellar::barrier();
    let mut index = my_pe;
    while index < ARRAY_LEN {
        let cur_index = index;
        lamellar::exec_all(lamellar::FnOnce!([my_pe,cur_index ] move || {
        ARRAY.lock().unwrap()[cur_index]=my_pe as u8;
        }));
        index += num_pes;
    }
    lamellar::wait_all();
    lamellar::barrier();
    println!("pe: {:?} {:?}", my_pe, ARRAY.lock().unwrap());
    lamellar::finit();
}

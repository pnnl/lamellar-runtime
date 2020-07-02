#[macro_use]
extern crate lazy_static;

use lamellar::{ActiveMessaging,LamellarAM};

use std::sync::Mutex;

const ARRAY_LEN: usize = 100;
lazy_static! {
    static ref ARRAY: Mutex<Vec<u8>> = Mutex::new(vec![0; ARRAY_LEN]);
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct DataAM {
    index: usize,
    pe: u8,
}

#[lamellar::am]
impl LamellarAM for DataAM {
    fn exec() {
        ARRAY.lock().unwrap()[self.index]=self.pe;
    }
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    println!("my_pe {:?} num_pes {:?}", my_pe, num_pes);

    world.barrier();
    let mut index = my_pe;
    while index < ARRAY_LEN {
        world.exec_am_all(DataAM{
            index: index,
            pe: my_pe as u8,
        });
        index += num_pes;
    }
    world.wait_all();
    world.barrier();
    println!("pe: {:?} {:?}", my_pe, ARRAY.lock().unwrap());
}

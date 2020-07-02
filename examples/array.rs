use lamellar::{RemoteMemoryRegion,LamellarMemoryRegion};
use lamellar::{ActiveMessaging,LamellarAM};

const ARRAY_LEN: usize = 100;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct DataAM {
    index: usize,
    pe: u8,
    array: LamellarMemoryRegion<u8>,
}

#[lamellar::am]
impl LamellarAM for DataAM {
    fn exec() {
        unsafe { self.array.as_mut_slice()[self.index]=self.pe} ;
    }
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    println!("my_pe {:?} num_pes {:?}", my_pe, num_pes);
    let array = world.alloc_mem_region(ARRAY_LEN);
    let init = [255u8; ARRAY_LEN];
    unsafe { array.put(my_pe, 0, &init) };
    println!("pe: {:?} {:?}", my_pe, array);
    println!(" Before {:?}",array.as_slice());
    world.barrier();
    let mut index = my_pe;
    while index < ARRAY_LEN {
        world.exec_am_all(DataAM{
            index: index,
            pe: my_pe as u8,
            array: array.clone(),
        });
        index += num_pes;
    }
    world.wait_all();
    world.barrier();
    
    println!("pe: {:?} {:?}", my_pe, array.clone());
    println!("After {:?}",array.as_slice());
    world.free_memory_region(array);
}

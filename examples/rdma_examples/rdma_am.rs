use lamellar::{
    ActiveMessaging, LamellarAM, LamellarMemoryRegion, RegisteredMemoryRegion, RemoteMemoryRegion,
};

const ARRAY_LEN: usize = 100;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct RdmaAM {
    array: LamellarMemoryRegion<u8>,
    orig_pe: usize,
    index: usize,
}

#[lamellar::am]
impl LamellarAM for RdmaAM {
    fn exec(&self) {
        println!("\t in RdmaAM on pe {:?}, originating from pe {:?}\n\tlocal segement of array: {:?}..{:?}",lamellar::current_pe, self.orig_pe,  &self.array.as_slice().unwrap()[0..10], &self.array.as_slice().unwrap()[ARRAY_LEN-10..]);

        //get the original nodes data
        let local = lamellar::world.alloc_local_mem_region::<u8>(ARRAY_LEN);
        let local_slice = unsafe { local.as_mut_slice().unwrap() };
        local_slice[ARRAY_LEN - 1] = lamellar::num_pes as u8;
        unsafe {
            self.array.get(self.orig_pe, 0, &local);
        }
        while local_slice[ARRAY_LEN - 1] == lamellar::num_pes as u8 {
            async_std::task::yield_now().await;
        }

        let my_index = self.index * lamellar::num_pes + lamellar::current_pe;
        println!("\tcurrent view of remote segment on pe {:?}: {:?}..{:?}\n\tpe: {:?} updating index {:?} on pe  {:?}", self.orig_pe, &local_slice[0..10], &local_slice[ARRAY_LEN-10..],lamellar::current_pe, my_index, self.orig_pe);

        //update an element on the original node
        local_slice[0] = lamellar::current_pe as u8;
        if my_index < ARRAY_LEN {
            unsafe {
                self.array
                    .put(self.orig_pe, my_index, &local.sub_region(0..=0));
            }
        }
        lamellar::world.free_local_memory_region(local);
    }
}

// memory regions are low level (unsafe) abstractions
// upon which we will build safer PGAS abstractions
// we provide APIs for these memory regions but they
// are intended mostly for internal use in the runtime
//
// LamellarMemoryRegions are serializable and can be transfered
// as part of a LamellarAM
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let array = world.alloc_shared_mem_region::<u8>(ARRAY_LEN);
    unsafe {
        for i in array.as_mut_slice().unwrap() {
            *i = 255_u8;
        }
    }
    if my_pe == 0 {
        println!("------------------------------------------------------------");
    }
    world.barrier();
    println!("[{:?}] Before {:?}", my_pe, array.as_slice());
    world.barrier();
    if my_pe == 0 {
        println!("------------------------------------------------------------");
    }
    world.barrier();
    let mut index = 0;
    while index * num_pes < ARRAY_LEN {
        world.exec_am_all(RdmaAM {
            array: array.clone(),
            orig_pe: my_pe,
            index: index,
        });
        index += 1;
    }

    world.wait_all();
    world.barrier();
    println!("[{:?}] after {:?}", my_pe, array.as_slice());
    world.barrier();
    if my_pe == 0 {
        println!("------------------------------------------------------------");
    }
    world.free_shared_memory_region(array);
}

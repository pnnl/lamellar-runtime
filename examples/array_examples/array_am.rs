/// ------------Lamellar Example: RDMA get -------------------------
/// this example highlights constructing a lamellar memory region
/// an then embedding and using it within a Lamellar Active message.
/// Within the AM, the memory handle can be used to get data from the
/// a remote pes or put data into a remote pes. In the example,
/// the handles are used on remote pes to access data on the PE which launched the AM
///----------------------------------------------------------------
use lamellar::array::{Distribution, UnsafeArray};
use lamellar::{ActiveMessaging, RemoteMemoryRegion};

const ARRAY_LEN: usize = 100;

#[lamellar::AmData(Clone)]
struct RdmaAM {
    array: UnsafeArray<u8>,
    orig_pe: usize,
    index: usize,
}

#[lamellar::am]
impl LamellarAM for RdmaAM {
    fn exec(&self) {
        let num_pes = lamellar::num_pes;
        let max_i = unsafe { std::cmp::min(self.array.local_as_slice().len(), num_pes) };
        println!(
            "\t in RdmaAM on pe {:?}, originating from pe {:?}",
            lamellar::current_pe,
            self.orig_pe,
        );
        println!("\tlocal segement of array: {:?}..", unsafe {
            &self.array.local_as_slice()[0..max_i]
        });

        //get the original nodes data
        let local = lamellar::world.alloc_local_mem_region::<u8>(ARRAY_LEN);
        let local_slice = unsafe { local.as_mut_slice().unwrap() };
        local_slice[ARRAY_LEN - 1] = num_pes as u8;
        self.array.get(0, &local).await;
        // while local_slice[ARRAY_LEN - 1] == num_pes as u8 {
        //     async_std::task::yield_now().await;
        // }

        let my_index = self.index * num_pes + lamellar::current_pe;
        println!("\tcurrent view of remote segment on pe {:?}: {:?}..{:?}\n\tpe: {:?} updating index {:?} on pe  {:?}", self.orig_pe, &local_slice[0..max_i], &local_slice[local_slice.len()-max_i..],lamellar::current_pe, my_index, self.orig_pe);

        //update an element on the original node
        local_slice[0] = lamellar::current_pe as u8;
        self.array.put(my_index, &local.sub_region(0..=0)).await;
    }
}

// memory regions are low level (unsafe) abstractions
// upon which we will build safer PGAS abstractions
// we provide APIs for these memory regions but they
// are intended mostly for internal use in the runtime
//
// SharedMemoryRegions are serializable and can be transfered
// as part of a LamellarAM
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    println!("creating array");
    let array = UnsafeArray::<u8>::new(world.team(), ARRAY_LEN, Distribution::Block);
    println!("creating memregion");
    let local_mem_region = world.alloc_local_mem_region::<u8>(ARRAY_LEN);
    println!("about to initialize array");
    array.print();
    if my_pe == 0 {
        unsafe {
            for i in local_mem_region.as_mut_slice().unwrap() {
                *i = 255_u8;
            }
        }
        world.block_on(array.put(0, &local_mem_region));
    }
    println!("here!!! {:?}", my_pe);
    array.print();
    for i in unsafe { array.local_as_slice() } {
        while *i != 255_u8 {
            std::thread::yield_now();
        }
    }
    if my_pe == 0 {
        println!("------------------------------------------------------------");
    }
    world.barrier();
    drop(local_mem_region);
    println!("freed mem region");
    println!("[{:?}] Before {:?}", my_pe, unsafe {
        array.local_as_slice()
    });
    world.barrier();
    if my_pe == 0 {
        println!("------------------------------------------------------------");
    }
    world.barrier();
    let mut index = 0;
    while index < ARRAY_LEN / num_pes {
        world.exec_am_all(RdmaAM {
            array: array.clone(),
            orig_pe: my_pe,
            index: index,
        });
        index += 1;
    }

    world.wait_all();
    world.barrier();
    println!("[{:?}] after {:?}", my_pe, unsafe {
        array.local_as_slice()
    });
    world.barrier();
    array.print();
    if my_pe == 0 {
        println!("------------------------------------------------------------");
    }
    world.barrier();

    if my_pe == 0 {
        let sum = world.block_on(array.sum());
        println!("sum: {:?}", sum);
        println!("------------------------------------------------------------");
    }
}

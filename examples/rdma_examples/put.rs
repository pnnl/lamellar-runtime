/// ------------Lamellar Example: RDMA put -------------------------
/// this example highlights constructing a lamellar memory region
/// and performing an RDMA put of local data into the region located
/// on a remote PE.
///----------------------------------------------------------------
use lamellar::ActiveMessaging; //for barrier
use lamellar::RemoteMemoryRegion;

const ARRAY_LEN: usize = 100;

// memory regions are low level (unsafe) abstractions
// upon which we will build safer PGAS abstractions
// we provide APIs for these memory regions but they
// are intended mostly for internal use in the runtime
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    if num_pes > 1 {
        // instatiates a shared memory region on every PE in world
        // all other pes can put/get into this region
        let array = world.alloc_shared_mem_region::<u8>(ARRAY_LEN);
        let array_slice = array.as_slice().unwrap(); //we can unwrap because we know array is local

        // instatiates a local array whos memory is registered with
        // the underlying network device, so that it can be used
        // as the src buffer in a put or as the dst buffer in a get
        let data = world.alloc_local_mem_region::<u8>(ARRAY_LEN);
        let data_slice = unsafe { data.as_mut_slice().unwrap() }; //we can unwrap because we know data is local
        for elem in data_slice {
            *elem = my_pe as u8;
        }

        // we can use the local_array to initialize our local portion a shared memory region
        unsafe { array.put(my_pe, 0, data.clone()) };

        //we can "put" from our segment of a shared mem region into another nodes shared mem region
        world.barrier();
        if my_pe == 0 {
            println!(
                "-------------- testing shared mem region put to shared mem region --------------"
            );
            world.barrier();
            unsafe {
                array.put(num_pes - 1, 0, array.clone());
            }
        } else if my_pe == num_pes - 1 {
            println!("[{:?}] Before {:?}", my_pe, array_slice);
            world.barrier();
            while array_slice[ARRAY_LEN - 1] == my_pe as u8 {
                std::thread::yield_now();
            } // wait for put to show up
            println!("[{:?}] After {:?}", my_pe, array_slice);
            unsafe { array.put(my_pe, 0, data.clone()) };
            println!(
                "-------------------------------------------------------------------------------"
            );
        } else {
            world.barrier();
        }
        world.barrier();

        //we can "put" from our local_array into another nodes shared mem region
        if my_pe == 0 {
            println!(
                "----------------- testing local_array put to shared mem region ----------------"
            );
            world.barrier();
            unsafe {
                array.put(num_pes - 1, 0, data.clone());
            }
        } else if my_pe == num_pes - 1 {
            println!("[{:?}] Before {:?}", my_pe, array_slice);
            world.barrier();
            while array_slice[ARRAY_LEN - 1] == my_pe as u8 {
                std::thread::yield_now();
            } // wait for put to show up
            println!("[{:?}] After {:?}", my_pe, array_slice);
            unsafe { array.put(my_pe, 0, data.clone()) };
            println!(
                "-------------------------------------------------------------------------------"
            );
        } else {
            world.barrier();
        }
        world.barrier();
        if my_pe == 0 {
            println!(
                "--------------- testing local_array put_all to shared mem region --------------"
            );
        }
        world.barrier();
        println!("[{:?}] Before {:?}", my_pe, array_slice);
        world.barrier();
        let mut index = my_pe;
        //stripe pe ids accross all shared mem regions

        // let data  = world.alloc_local_mem_region::<u8>(1);
        // unsafe{data.as_mut_slice().unwrap()[0]=my_pe as u8;}
        while index < ARRAY_LEN {
            let cur_index = index;
            unsafe { array.put_all(cur_index, data.sub_region(0..=0)) };
            index += num_pes;
        }

        for i in 0..ARRAY_LEN {
            while (i % num_pes) as u8 != array_slice[i] {
                std::thread::yield_now();
            }
        }

        world.barrier();
        if my_pe == 0 {
            println!("----------------------------------------");
        }
        println!("[{:?}] After {:?}", my_pe, array_slice);
        world.barrier();
        if my_pe == 0 {
            println!(
                "-------------------------------------------------------------------------------"
            );
        }
    } else {
        println!("this example is intended for multi pe executions");
    }
}
